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
 *	  D1/D2 SCOPE (interface-lock §3, then D2b):
 *	    - with cluster.undo_buffer_writeback off, this is a read-through cache
 *	      with write-through pwrite(do_fsync=false); fsync stays at the legacy
 *	      precommit path, not per-unpin.
 *	    - with cluster.undo_buffer_writeback on, dirty DATA blocks are protected
 *	      by XLOG_UNDO_BLOCK_WRITE and flushed by checkpoint / eviction.  The
 *	      runtime latch in cluster_undo_buf_writeback_allowed() disables this
 *	      path while peers exist;  the GUC check hook only warns.
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
extern void cluster_undo_buf_mark_dirty(const ClusterUndoBufPin *pin, XLogRecPtr wal_lsn);

/* Release a pin (drops the content lock). */
extern void cluster_undo_buf_unpin(ClusterUndoBufPin *pin);

/* spec-3.25 D1b: deferred-merge window residency reference (see .c). */
extern void cluster_undo_buf_addref(const ClusterUndoBufPin *pin);
extern void cluster_undo_buf_unref_slot(int slot);

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
 * cluster_undo_buf_writeback_allowed -- runtime write-back latch.
 *	Returns true only when the pool exists, the GUC is on, and the node has no
 *	peers.  That last condition is the production safety guard: D2b durability
 *	is local WAL + local checkpoint flush, so multi-node write-back remains off
 *	until Cache-Fusion-aware undo shipping/recovery exists.
 */
extern bool cluster_undo_buf_writeback_allowed(void);

/*
 * spec-4.8ab D5 -- explicit checkpoint-writeback boundary verdict (three-state).
 *	A NEW, per-block decision predicate distinct from the global write-back-mode
 *	latch above (cluster_undo_buf_writeback_allowed, which answers "is the mode
 *	on?").  This answers "may THIS dirty block be written back / evicted right
 *	now?", and is consulted at the flush / eviction moment:
 *	  CLUSTER_WB_OK            -- WAL durable AND no live evidence window
 *	                              (or it is a checkpoint flush): write back, and
 *	                              evict when not a checkpoint.
 *	  CLUSTER_WB_HOLD_WAL      -- the block's XLOG_UNDO_BLOCK_WRITE is not durable
 *	                              yet (block_lsn > flushed_lsn): hold.
 *	  CLUSTER_WB_HOLD_EVIDENCE -- a non-checkpoint eviction would drop an undo
 *	                              image still consumable by a reader / recovery:
 *	                              flush is fine, eviction is held.
 *	cluster_undo_buf_writeback_decide is PURE (LSN + flags, no globals) so the
 *	three-state logic is unit-testable in isolation (mirrors the D1 predicates).
 */
typedef enum ClusterUndoWritebackVerdict {
	CLUSTER_WB_OK,			  /* may write back (and evict if not checkpoint) */
	CLUSTER_WB_HOLD_WAL,	  /* WAL not durable yet -> hold */
	CLUSTER_WB_HOLD_EVIDENCE, /* live evidence window -> flush ok, evict held */
} ClusterUndoWritebackVerdict;

extern ClusterUndoWritebackVerdict cluster_undo_buf_writeback_decide(XLogRecPtr block_lsn,
																	 XLogRecPtr flushed_lsn,
																	 bool evidence_live,
																	 bool is_checkpoint);

/*
 * spec-4.8ab D5 -- cluster.undo_writeback_boundary_check mode (two-layer model,
 * §3.1).  This GUC controls ONLY the advisory / diagnostic layer (verdict
 * accounting + an extra CI invariant check).  The hard corruption-path
 * fail-closed (cluster_undo_boundary_violation: WAL-before-data / checkpoint-
 * coverage / evidence-eviction) is UNCONDITIONAL and runs under every value,
 * including off -- 8.A correctness is never gated by a GUC.  Registered in
 * cluster_guc.c (DefineCustomEnumVariable).
 */
typedef enum ClusterUndoWritebackBoundaryCheck {
	CLUSTER_UNDO_WB_CHECK_OFF,	  /* skip advisory verdict accounting (save hot-path cost) */
	CLUSTER_UNDO_WB_CHECK_ON,	  /* default: verdict accounting + counters */
	CLUSTER_UNDO_WB_CHECK_STRICT, /* + ERROR on a broken advisory invariant (CI/test) */
} ClusterUndoWritebackBoundaryCheck;
extern int cluster_undo_writeback_boundary_check;

/*
 * spec-4.8ab D1 -- checkpoint-writeback boundary predicates + fail-closed
 * helper.  The two predicates are pure (LSN comparisons only) so the boundary
 * logic is unit-testable;  the violation helper fail-closes (8.A) and never
 * returns.  See cluster_undo_buf.c for the full contract.
 *
 *	wal_before_data_holds(block_lsn, flushed_lsn)
 *	    true iff block_lsn is durable (<= flushed_lsn) -- safe to write back.
 *	checkpoint_coverage_violation(block_lsn, checkpoint_redo)
 *	    true iff a leftover dirty block is PRE-redo (lost on recovery).  NB:
 *	    this is NOT "block_lsn > redo" -- a post-redo block is replayed.
 *	boundary_violation(what, block_lsn, flushed_or_redo)
 *	    fail-closed:  PANIC in a critical section, else ERROR 53R9N.
 */
extern bool cluster_undo_wal_before_data_holds(XLogRecPtr block_lsn, XLogRecPtr flushed_lsn);
extern bool cluster_undo_checkpoint_coverage_violation(XLogRecPtr block_lsn,
													   XLogRecPtr checkpoint_redo);
extern void cluster_undo_boundary_violation(const char *what, XLogRecPtr block_lsn,
											XLogRecPtr flushed_or_redo) pg_attribute_noreturn();

/* ----- observability counters (dump_undo / pg_stat_cluster_undo) ----- */
extern uint64 cluster_undo_buf_get_hit_count(void);
extern uint64 cluster_undo_buf_get_miss_count(void);
extern uint64 cluster_undo_buf_get_writethrough_count(void);
extern uint64 cluster_undo_buf_get_evict_count(void);
extern uint64 cluster_undo_buf_get_writeback_count(void); /* spec-3.18 D7 */

/*
 * spec-4.8ab D7 -- checkpoint-writeback boundary observability.  These four
 * counters are grown into the existing undo buffer pool region (NO new shmem
 * region -- D0 finding-3), so there is no region-count baseline ripple.  When
 * the pool is disabled (cluster.undo_buffers = 0) they read 0;  the boundary
 * fail-closed guards themselves never depend on the counters.
 */
extern uint64 cluster_undo_buf_get_writeback_held_wal_count(void);
extern uint64 cluster_undo_buf_get_writeback_held_evidence_count(void);
extern uint64 cluster_undo_buf_get_boundary_violation_count(void);
extern uint64 cluster_undo_buf_get_remote_evidence_hold_count(void); /* 4.8b D6 */

#endif /* CLUSTER_UNDO_BUF_H */
