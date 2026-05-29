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


/*
 * cluster_undo_smgr_read_block -- read one 8KB block from undo segment file.
 *
 *	Args:
 *	  segment_id, owner_instance -- per-instance segment identity
 *	  block_no                    -- 0..UNDO_BLOCKS_PER_SEGMENT-1
 *	  buf                         -- caller-provided 8KB buffer (BLCKSZ)
 *
 *	Returns: true on success, false on I/O error or short read.
 */
extern bool cluster_undo_smgr_read_block(uint32 segment_id, uint8 owner_instance, uint32 block_no,
										 char *buf);


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
extern bool cluster_undo_smgr_write_block(uint32 segment_id, uint8 owner_instance, uint32 block_no,
										  const char *buf, bool do_fsync);


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


#endif /* !FRONTEND */

#endif /* CLUSTER_UNDO_SMGR_H */
