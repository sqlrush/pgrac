/*-------------------------------------------------------------------------
 *
 * cluster_undo_smgr.h
 *	  pgrac undo segment file I/O abstraction layer (spec-3.7 D7 carryover,
 *	  spec-3.8 真 ship).
 *
 *	  Wraps block-level read/write/create/fsync of per-instance undo
 *	  segment files at $PGDATA/pg_undo/instance_<N>/seg_<id>.dat.
 *	  Provides a single I/O surface for:
 *	    - spec-3.7 record-level allocator(cluster_undo_record.c — currently
 *	      inline I/O,  refactor to use this 推 Hardening v1.0.X)
 *	    - spec-3.8 lifecycle state machine + autoextend(cluster_undo_alloc.c
 *	      mark_active / mark_full / tail_block / bitmap helpers)
 *	    - spec-3.9 CR block construction(future)
 *	    - spec-3.10 block-level CR cache(future hook above this layer)
 *
 *	  Concurrency:
 *	    - Block-level read/write are stateless;  caller manages locking
 *	    - File create + fsync are lifecycle-level work,  caller holds
 *	      cluster_undo_record lifecycle_lock per spec-3.8 §3.2
 *
 *	  NOT critical-section safe(does file I/O).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.8-undo-segment-lifecycle-autoextend.md (FROZEN v0.3 +
 *       Hardening v1.0.1;  D7 carryover from spec-3.7 Hardening v1.0.3)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_smgr.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SMGR_H
#define CLUSTER_UNDO_SMGR_H

#ifndef FRONTEND

#include "postgres.h"
#include "storage/block.h"

#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoPathIntent (D2-2) */


/*
 * spec-5.22b D2-2: the block / header I/O entries take an explicit
 * ClusterUndoPathIntent so the shared-vs-local physical root is chosen per
 * call (RUNTIME_SHARED own undo -> shared cluster_fs root under coherence;
 * MATERIALIZED_LOCAL foreign dead-origin copy -> local DataDir).  Callers
 * pass cluster_undo_intent_for_owner(owner_instance).
 */

/*
 * cluster_undo_smgr_read_block -- read one 8KB block from undo segment file.
 *
 *	Args:
 *	  intent                      -- shared-vs-local root selector (D2-2)
 *	  segment_id, owner_instance -- per-instance segment identity
 *	  block_no                    -- 0..UNDO_BLOCKS_PER_SEGMENT-1
 *	  buf                         -- caller-provided 8KB buffer (BLCKSZ)
 *
 *	Returns: true on success, false on I/O error or short read.
 */
extern bool cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment_id,
										 uint8 owner_instance, uint32 block_no, char *buf);


/*
 * cluster_undo_smgr_write_block -- write one 8KB block to undo segment file.
 *
 *	Args:
 *	  segment_id, owner_instance -- per-instance segment identity
 *	  block_no                    -- 0..UNDO_BLOCKS_PER_SEGMENT-1
 *	  buf                         -- 8KB source buffer
 *	  do_fsync                    -- if true, fsync after write
 *
 *	Returns: true on success, false on I/O error.
 *
 *	NOT critical-section safe.
 */
extern bool cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment_id,
										  uint8 owner_instance, uint32 block_no, const char *buf,
										  bool do_fsync);

/* Register the per-backend fd-cache cleanup before temporary exit hooks. */
extern void cluster_undo_smgr_ensure_exit_hook(void);


/*
 * cluster_undo_smgr_read_header_bytes / _write_header_bytes (spec-3.11 D2)
 *
 *	Targeted read/write of a byte range within segment header block 0 (e.g. one
 *	32-byte TTSlot at offset 112 + slot*32).  Lock-free per-slot durable TT
 *	writes (each xact owns a distinct slot = non-overlapping range).  The write
 *	does NOT fsync (WAL-protected by XLOG_UNDO_TT_SLOT_COMMIT; torn write
 *	recovered by redo -- spec-3.11 C10).  offset+len must be within BLCKSZ.
 *	Returns true on success, false on bad args / I/O error / short transfer.
 *	NOT critical-section safe.
 */
extern bool cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
												uint8 owner_instance, uint32 offset, char *buf,
												uint32 len);
extern bool cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
												 uint8 owner_instance, uint32 offset,
												 const char *buf, uint32 len);


/*
 * Spec 8.4A P3 first-publication seam.  A final-path probe never creates or
 * repairs.  EXACT publishes block0 only after full-size and identity checks;
 * all other outcomes leave the caller buffer unchanged.
 */
typedef enum ClusterUndoSmgrFinalState {
	CLUSTER_UNDO_SMGR_FINAL_ABSENT = 0,
	CLUSTER_UNDO_SMGR_FINAL_EXACT,
	CLUSTER_UNDO_SMGR_FINAL_INVALID,
	CLUSTER_UNDO_SMGR_FINAL_IO_ERROR
} ClusterUndoSmgrFinalState;

typedef enum ClusterUndoSmgrPublishResult {
	CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED = 0,
	CLUSTER_UNDO_SMGR_PUBLISH_EXISTS,
	CLUSTER_UNDO_SMGR_PUBLISH_INVALID,
	CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR
} ClusterUndoSmgrPublishResult;

extern ClusterUndoSmgrFinalState cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent,
																 uint32 segment_id,
																 uint8 owner_instance,
																 char block0[BLCKSZ]);
extern bool cluster_undo_smgr_provision_temp_create(ClusterUndoPathIntent intent, uint32 segment_id,
													uint8 owner_instance,
													char temp_path[MAXPGPATH]);
extern ClusterUndoSmgrPublishResult
cluster_undo_smgr_provision_temp_publish(ClusterUndoPathIntent intent, uint32 segment_id,
										 uint8 owner_instance, const char *temp_path,
										 const char block0[BLCKSZ]);
extern bool cluster_undo_smgr_provision_temp_cleanup(ClusterUndoPathIntent intent,
													 uint32 segment_id, uint8 owner_instance,
													 const char *temp_path);
extern bool cluster_undo_smgr_cleanup_boot_foreign_temps(ClusterUndoPathIntent intent,
														 uint8 owner_instance);

/*
 * A-prime immutable root-descriptor applicability mirror.  root_directory is
 * an already resolved undo root; it locates pgrac_undo_root.control but never
 * contributes identity.  Probe publishes observed bytes only on EXACT.
 */
typedef enum ClusterUndoSmgrRootMirrorState {
	CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT = 0,
	CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT,
	CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED,
	CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD,
	CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR
} ClusterUndoSmgrRootMirrorState;

extern ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_probe(const char *root_directory,
										const uint8 expected[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
										uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
extern ClusterUndoSmgrRootMirrorState cluster_undo_smgr_root_descriptor_read_candidate(
	const char *root_directory, uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
extern ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_publish(const char *root_directory,
										  const uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);


/*
 * cluster_undo_smgr_create_segment_file -- create + WAL-protect a new
 *	segment file for owner_instance.
 *
 *	Idempotent:  if file already exists with valid header, returns 0
 *	without rewrite.  Mostly a thin wrapper around cluster_undo_segment_allocate
 *	(spec-3.4b/spec-1.22 ship).
 *
 *	Args:
 *	  segment_id, owner_instance -- per-instance segment identity
 *
 *	Returns: 0 on success;  -1 on FS error;  -2 on argument out of range.
 *
 *	NOT critical-section safe.  Caller MUST hold cluster_undo_record
 *	lifecycle_lock per spec-3.8 §3.2 lock contract.
 */
extern int cluster_undo_smgr_create_segment_file(uint32 segment_id, uint8 owner_instance);


/*
 * cluster_undo_smgr_fsync_segment_file -- fsync a segment file end-to-end.
 *
 *	Used after batched writes when caller wants explicit durability
 *	barrier without per-block do_fsync overhead.
 *
 *	Returns: true on success, false on I/O error.
 */
extern bool cluster_undo_smgr_fsync_segment_file(uint32 segment_id, uint8 owner_instance);

/*
 * cluster_undo_smgr_fd_cache_reset -- close the per-backend cached undo
 *	segment fd during PREPARE/ABORT full teardown or explicit invalidation.
 *	Normal COMMIT preserves the cache.
 */
extern void cluster_undo_smgr_fd_cache_reset(void);


#endif /* !FRONTEND */

#endif /* CLUSTER_UNDO_SMGR_H */
