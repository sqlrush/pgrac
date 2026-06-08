/*-------------------------------------------------------------------------
 *
 * cluster_undo_buf.h
 *	  pgrac spec-3.18 D1 — per-instance undo block buffer pool (interface).
 *
 *	  AD-014 (undo durability/cache form) restores the 专项 #3 §17.3 form:
 *	  active undo blocks live in an in-memory pool, not direct-written per
 *	  record.  This header declares the pool API locked by
 *	  pgrac:docs/spec-3.18-d1d2-interface-lock.md (v1.1).
 *
 *	  D1 SCOPE (pre-D2, no durability change — interface-lock §3):
 *	    - read-through cache + write-through writes (do_fsync=false; fsync
 *	      stays at cluster_undo_xact_precommit_flush, NOT per-unpin).
 *	    - buffered dirty (write-back) is FORBIDDEN until the D2 WAL-protection
 *	      alpha:  cluster.undo_buffer_writeback defaults off AND a GUC
 *	      check_hook hard-rejects turning it on (Assert is stripped in
 *	      production — L214/L218).
 *	    - DATA blocks only (block_no >= 1) are poolable;  block 0 (segment
 *	      header + durable TT slots, modified via byte-targeted
 *	      cluster_undo_smgr_{read,write}_header_bytes) is NOT pooled — pooling
 *	      it would go stale vs the header-byte path (interface-lock §3 pt 4).
 *
 *	  Pin discipline (interface-lock §2, review pt 1/2):  the content-lock
 *	  mode is chosen AT PIN TIME — there is no SHARED->EXCLUSIVE in-place
 *	  upgrade (LWLock upgrade self-deadlocks).  Write order is
 *	    pin(EXCLUSIVE) -> mutate image -> XLogInsert -> mark_dirty(lsn) -> unpin.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_buf.h
 *
 * NOTES
 *	  This is a pgrac-original file.  All symbols use the cluster_undo_buf_
 *	  namespace.
 *	  Spec: spec-3.18-write-path-performance-overhaul.md (FROZEN v0.6 RE-SCOPE)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_BUF_H
#define CLUSTER_UNDO_BUF_H

#include "access/xlogdefs.h"
#include "storage/block.h"

/*
 * The first poolable block.  Block 0 holds the segment header + durable TT
 * slots and is modified by the byte-targeted header path, so it is NOT cached
 * here (interface-lock §3 pt 4);  cluster_undo_buf_pin() rejects block_no 0.
 */
#define CLUSTER_UNDO_BUF_FIRST_DATA_BLOCK 1

/* Content-lock mode, fixed at pin time (no in-place upgrade — review pt 1). */
typedef enum ClusterUndoBufMode {
	CLUSTER_UNDO_BUF_SHARED,   /* read-only access */
	CLUSTER_UNDO_BUF_EXCLUSIVE /* intend to modify */
} ClusterUndoBufMode;

/*
 * A caller-held pin token.  Opaque-ish: callers pass it back to mark_dirty /
 * unpin.  slot < 0 means "not pooled" (e.g. pool disabled) and callers must
 * fall back to the direct smgr path.
 */
typedef struct ClusterUndoBufPin {
	int slot; /* pool slot index;  -1 = not pooled */
	uint32 segment_id;
	uint8 owner;
	uint32 block_no;
	ClusterUndoBufMode mode; /* lock mode chosen at pin time */
} ClusterUndoBufPin;

/* ----- shmem region (registered via cluster_shmem_register_region) ----- */
extern Size cluster_undo_buf_shmem_size(void);
extern void cluster_undo_buf_shmem_init(void);
extern void cluster_undo_buf_register_region(void);

/*
 * cluster_undo_buf_pin -- pin + content-lock an undo DATA block image in the
 *	chosen mode;  reads from disk on miss.  Returns a pointer to the BLCKSZ
 *	image (valid until unpin), or NULL when the pool is disabled / the block
 *	is not poolable (block_no < FIRST_DATA_BLOCK) — caller then uses the direct
 *	smgr path.  *pin is filled in on a successful pin.
 */
extern char *cluster_undo_buf_pin(uint32 segment_id, uint8 owner, uint32 block_no,
								  ClusterUndoBufMode mode, ClusterUndoBufPin *pin);

/*
 * cluster_undo_buf_mark_dirty -- the EXCLUSIVE-pinned block was modified,
 *	protected by the XLOG_UNDO_BLOCK_WRITE at wal_lsn (D2).  In D1
 *	(writeback off) wal_lsn is InvalidXLogRecPtr and this performs the
 *	write-through pwrite (do_fsync=false).  Caller MUST already hold an
 *	EXCLUSIVE pin and have finished mutating + (D2) XLogInsert.
 */
extern void cluster_undo_buf_mark_dirty(ClusterUndoBufPin *pin, XLogRecPtr wal_lsn);

/* Release a pin (drops the content lock). */
extern void cluster_undo_buf_unpin(ClusterUndoBufPin *pin);

/* checkpoint / shutdown flush hook.  is_checkpoint=true on checkpoint. */
extern void cluster_undo_buf_flush_all(bool is_checkpoint);

/*
 * spec-3.18 D3.2 (review finding 3):  drop all pool slots for a segment being
 * reborn in place (reuse-in-place, generation+1).  The pool key carries no
 * generation, so old-generation cached blocks must be invalidated on reuse to
 * avoid serving stale undo under the same (segment, block) key.  Caller holds
 * lifecycle_lock;  the segment is RECYCLABLE + WAL-protected so slots are
 * dropped without flushing.
 */
extern void cluster_undo_buf_invalidate_segment(uint32 segment_id, uint8 owner);

/*
 * cluster_undo_buf_writeback_allowed -- the alpha gate (interface-lock §5).
 *	Returns false during the D1 phase (buffered write-back has no WAL
 *	protection yet).  The cluster.undo_buffer_writeback GUC check_hook (in
 *	cluster_guc.c) hard-rejects setting the GUC on when this returns false —
 *	a real runtime guard, not Assert (stripped in production) nor default.
 *	Flips to true only after the D2 crash-restart redo proof.
 */
extern bool cluster_undo_buf_writeback_allowed(void);

/* ----- observability counters (dump_undo / pg_stat_cluster_undo) ----- */
extern uint64 cluster_undo_buf_get_hit_count(void);
extern uint64 cluster_undo_buf_get_miss_count(void);
extern uint64 cluster_undo_buf_get_writethrough_count(void);
extern uint64 cluster_undo_buf_get_evict_count(void);
extern uint64 cluster_undo_buf_get_writeback_count(void); /* spec-3.18 D7 */

#endif /* CLUSTER_UNDO_BUF_H */
