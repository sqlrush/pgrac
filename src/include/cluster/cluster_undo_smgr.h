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
#include "cluster/cluster_recovery_tt_record.h"
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

/* Private recovery observation, not persisted authority. All entries require
 * the active qualified RECOVERY_SHARED scope. Short files are distinct from
 * I/O errors; the full-image caller may restore size before publishing block0. */
typedef struct ClusterUndoSmgrRecoveryFileV1 {
	uint64 device;
	uint64 inode;
	uint64 size;
	bool exists;
} ClusterUndoSmgrRecoveryFileV1;

extern bool cluster_undo_smgr_recovery_probe_v1(uint32 segment, uint8 instance,
												ClusterUndoSmgrRecoveryFileV1 *file,
												char block0[BLCKSZ]);
extern bool cluster_undo_smgr_recovery_read_block_v1(uint32 segment, uint8 instance, uint32 block,
													 const ClusterUndoSmgrRecoveryFileV1 *expected,
													 char out[BLCKSZ]);
/* Only a source-proved INIT/REUSE owner may call materialize. It preserves all
 * existing bytes through the target size and does not publish a new header. */
extern bool cluster_undo_smgr_recovery_materialize_v1(uint32 segment, uint8 instance,
													  const ClusterUndoSmgrRecoveryFileV1 *expected,
													  const char block0[BLCKSZ],
													  const char final_header[BLCKSZ]);


/*
 * cluster_undo_smgr_read_header_bytes / _write_header_bytes (spec-3.11 D2)
 *
 *	Targeted read/write of a byte range within segment header block 0 (e.g. one
 *	32-byte TTSlot at offset 112 + slot*32).  Lock-free per-slot durable TT
 *	writes (each xact owns a distinct slot = non-overlapping range).  The write
 *	does not fsync a path the checkpointer resolves itself (own runtime undo,
 *	local materialized copy): the caller's WAL record protects it until the
 *	next checkpoint fsyncs it (cluster_undo_buf_flush_all).  A recovery-scoped
 *	path is fsynced before returning, inside the caller's scope.  The caller must have
 *	inserted that record either while holding the segment's exclusive
 *	block-zero content lock or inside a DELAY_CHKPT_START window.  offset+len
 *	must be within BLCKSZ.  Returns true on success, false on bad args / I/O
 *	error / short transfer.  NOT critical-section safe.
 */
extern bool cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
												uint8 owner_instance, uint32 offset, char *buf,
												uint32 len);
extern bool cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
												 uint8 owner_instance, uint32 offset,
												 const char *buf, uint32 len);

/*
 * Header writer for a critical section: _open resolves and opens a private
 * descriptor beforehand (returns -1 on failure); _write performs the same
 * write and checkpoint bookkeeping as cluster_undo_smgr_write_header_bytes
 * without path resolution, allocation or ERROR; _close never throws.
 */
extern int cluster_undo_smgr_header_writer_open(ClusterUndoPathIntent intent, uint32 segment_id,
												uint8 owner_instance);
extern bool cluster_undo_smgr_header_writer_write(int fd, ClusterUndoPathIntent intent,
												  uint32 segment_id, uint8 owner_instance,
												  uint32 offset, const char *buf, uint32 len);
extern void cluster_undo_smgr_header_writer_close(int fd);

/* Storage-only C0 adapter. The caller keeps the exact target reservation and
 * recovery authority throughout, and flushes the record before write. Prepare
 * may resolve the qualified recovery path; write only uses the held raw FD.
 * A token carries no transaction, WAL, checkpoint or completion authority. */
typedef struct ClusterUndoSmgrRecoveryStampV1 {
	uint64 token;
} ClusterUndoSmgrRecoveryStampV1;

extern bool cluster_undo_smgr_recovery_abort_prepare_v1(const ClusterRecoveryTTAbortV1 *record,
														ClusterUndoSmgrRecoveryStampV1 *out);
struct ClusterWalInputsV1;
/* Post-EOR variant: exact original TX_CLOSURE candidate/source, same held-FD
 * storage checks. No redo-role fallback or WAL/write permission is added. */
extern bool cluster_undo_smgr_recovery_abort_prepare_tx_v1(struct ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, ClusterUndoSmgrRecoveryStampV1 *out);
extern bool cluster_undo_smgr_recovery_abort_write_v1(ClusterUndoSmgrRecoveryStampV1 stamp);
/* Exact held descriptor has completed write/sync/readback in this owner.
 * This is a storage fact, not proof of WAL flush or recovery authority. */
extern bool cluster_undo_smgr_recovery_abort_durable_v1(ClusterUndoSmgrRecoveryStampV1 stamp,
	const ClusterRecoveryTTAbortV1 *record);
/* Also released with the owning ResourceOwner. Cleanup never clears a
 * recovery duty/window or turns a failed write into completion. */
extern bool cluster_undo_smgr_recovery_abort_close_v1(ClusterUndoSmgrRecoveryStampV1 stamp);

#ifdef USE_PGRAC_CLUSTER
/* Original dormant D0 storage adapter. Shares the ResourceOwner stage with
 * C0 using a distinct kind; no cross-kind write/close/durability observation.
 * Caller keeps source/graph/target authority, reservation and flushed WAL.
 * Inputs/output must not overlap. Prepare alone is not completion.
 * Write uses the held FD and block0 lock;
 * fsync errors use the original data_sync_elevel(ERROR) after unlocking and
 * retaining sticky failure. Cleanup never clears a recovery duty/window. */
extern bool cluster_undo_smgr_recovery_ctrc_prepare_v2(const uint8 *payload, size_t length,
	uint64 system_identifier, ClusterUndoSmgrRecoveryStampV1 *out);
extern bool cluster_undo_smgr_recovery_ctrc_matches_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const uint8 *payload, size_t length);
extern bool cluster_undo_smgr_recovery_ctrc_write_v2(ClusterUndoSmgrRecoveryStampV1 stamp);
extern bool cluster_undo_smgr_recovery_ctrc_write_bound_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const uint8 *payload, size_t length);
/* Re-read the completed stage's entire expected-after block0 through its
 * same held descriptor and original header lock. Requires the exact D0
 * payload, live ResourceOwner and successful write/sync/postread first.
 * No reopen, mutation, flush, allocation or release. Entered refusal/ERROR
 * stays failed and keeps the FD until original cleanup. This is a storage
 * observation only; original graph/source/current authority, retention,
 * window and joint completion proofs remain the caller's obligations. */
extern bool cluster_undo_smgr_recovery_ctrc_readback_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const uint8 *payload, size_t length);
extern bool cluster_undo_smgr_recovery_ctrc_close_v2(ClusterUndoSmgrRecoveryStampV1 stamp);
#endif


#ifdef USE_PGRAC_CLUSTER
/* Storage-only D0 adapter. Prepare canonical bytes and the exact complete
 * native before/after pages before entering the recovery stamp window.
 * The caller retains original graph/source/terminal/target authority and
 * reservation, inserts and flushes WAL before write, and completes/clears
 * its own window only after successful direct sync and exact readback.
 * A token supplies none of those proofs. Inputs must not overlap out.
 *
 * Shares the single ResourceOwner stage with C0, with distinct token kinds.
 * No live caller. Write uses only the held FD and original block0 lock;
 * failures are sticky. D0 fsync failure uses data_sync_elevel(ERROR).
 * Close/owner cleanup never clears the recovery duty or window. */
extern bool cluster_undo_smgr_recovery_ctrc_prepare_v2(const uint8 *payload, size_t length,
													   uint64 system_identifier,
													   ClusterUndoSmgrRecoveryStampV1 *out);
/* Read-only pre-insert check of the held D0 stage and all prepared bytes.
 * No IO, lock, codec/hash/allocation, state change or completion authority.
 * Refusal does not poison the stage; post-insert write_bound remains required. */
struct XLogReaderState;
struct ClusterRecoveryCTRCReferenceV2;
/* Dormant recovery preparation from the original completed native D0 reader
 * and its immutable full raw snapshot. First bind the exact reference using
 * the existing reader/CRC/SHA, then prepare the original held-FD target from
 * those same payload bytes. No target write, WAL insertion or live dispatch.
 * Caller retains qualified ROOT/source/cut/graph/owner/pins and reservation;
 * byte identity does not prove those prerequisites or typed idempotency.
 * raw==NULL and raw_length==0 borrows the original current native reader's
 * complete D0 snapshot; other pointer/length pairs require explicit full raw
 * bytes. This does not relax the completed-reader or qualification contracts.
 * Private token/reference outputs must be disjoint from reader-owned storage,
 * input bytes and each other. Both stay unchanged on refusal; original ERROR
 * and ResourceOwner behavior is preserved. Only exact-before is admitted. */
extern bool cluster_undo_smgr_recovery_ctrc_prepare_record_v2(
	const struct XLogReaderState *reader, uint32 source_index,
	const uint8 source_identity_digest[32], const uint8 *raw, size_t raw_length,
	ClusterUndoSmgrRecoveryStampV1 *out, struct ClusterRecoveryCTRCReferenceV2 *reference);
extern bool cluster_undo_smgr_recovery_ctrc_matches_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
													 const uint8 *payload, size_t length);
extern bool cluster_undo_smgr_recovery_ctrc_write_v2(ClusterUndoSmgrRecoveryStampV1 stamp);
/* The durability coordinator must also bind the complete prepared canonical
 * payload, not just its target before/after page. An exact owned token with
 * mismatched payload fails sticky before target IO; unrelated tokens/owners
 * cannot poison this stage. No codec/hash/allocation is performed here. */
extern bool cluster_undo_smgr_recovery_ctrc_write_bound_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
														   const uint8 *payload, size_t length);
extern bool cluster_undo_smgr_recovery_ctrc_close_v2(ClusterUndoSmgrRecoveryStampV1 stamp);
/* Dormant recovery finish for a caller-owned prepared D0 token. Rebind the
 * original completed reader/full raw record and compare the exact reference
 * returned at preparation, then reuse write_bound/direct sync/readback/close.
 * Caller retains qualified source/cut/ROOT/graph/owner/pins/reservation and
 * original recovery-window authority. The reference is structural, not a
 * completion or source qualification. Only a successful write AND close
 * returns true; this does not clear a window or release any retention pin.
 * Binding refusal leaves the prepared stage intact. Target failures retain
 * the original sticky/ERROR behavior; close failure may already close the FD.
 * Caller still owns the token on false; never infer rollback or retry safety.
 * NULL/0 raw reacquires the current native snapshot, with the same exact R
 * recheck; advancing/resetting the reader is not a completion receipt.
 * No new target preparation, WAL insertion/flush or live redo dispatch. */
extern bool cluster_undo_smgr_recovery_ctrc_finish_record_v2(
	ClusterUndoSmgrRecoveryStampV1 stamp, const struct XLogReaderState *reader,
	uint32 source_index, const uint8 source_identity_digest[32], const uint8 *raw,
	size_t raw_length, const struct ClusterRecoveryCTRCReferenceV2 *prepared_reference);

#endif


/*
 * Redo: block zero already shows a replayed TT record's effect (or a newer
 * state), but those bytes may still be only in the page cache of a crashed
 * predecessor while shared memory -- and the checkpoint's record of unsynced
 * header writes -- was rebuilt.  Record them for the next checkpoint like a
 * fresh write, or fsync them now.  Returns false on failure.
 */
extern bool cluster_undo_smgr_header_unchanged_durable(ClusterUndoPathIntent intent,
													   uint32 segment_id, uint8 owner_instance);

/* Checkpoint: fsync one segment file through the intent its header used. */
extern bool cluster_undo_smgr_fsync_header(ClusterUndoPathIntent intent, uint32 segment_id,
										   uint8 owner_instance);


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
