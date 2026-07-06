/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_apply.h
 *	  pgrac online thread-recovery page apply matrix (spec-4.11 D1, Q10-B).
 *
 *	  When a survivor online-replays a dead WAL thread's data to shared storage
 *	  within the reconfig freeze window (spec-4.11), it applies the dead
 *	  thread's WAL records, one block at a time, to a LIVE page read from shared
 *	  storage -- NOT a clean detached FPI-base reconstruction (that is
 *	  spec-4.10).  The corruption-critical difference is idempotence on a live
 *	  page: a page that already reflects a record must be left untouched, or a
 *	  retry / cold redo after a partial-apply crash would double-apply deltas
 *	  (spec-4.11 v0.3 partial-apply, 8.A).
 *
 *	  cluster_thread_apply_record_to_page() therefore wraps the t/256-proven
 *	  single-block apply core (cluster_block_apply_one) with PG redo's LSN-gate
 *	  (mirror XLogReadBufferForRedoExtended BLK_DONE): the whole NEW correctness
 *	  surface this increment adds is that one gate.  The FPI-vs-per-rmgr-delta
 *	  routing, and the byte-for-byte matrix itself, stay inside
 *	  cluster_block_apply_one untouched.
 *
 *	  cluster_thread_apply_decide() is the PURE, unit-testable gate decision
 *	  (NOOP / DONE / APPLY), depending only on the record's block-reference fact
 *	  and the record-end vs page LSNs -- no shared state, no smgr, no I/O.
 *
 *	  SCOPE (increment boundary): this is a pure page mutation + result.  It
 *	  does NOT touch smgr, does NOT write to shared storage, does NOT iterate a
 *	  record's blocks, does NOT publish authority and does NOT touch global
 *	  redo side effects (nextXid / TLI / XLogRecoveryCtl).  The shared-storage
 *	  read-modify-write driver that streams WAL, loops a record's blocks, and
 *	  handles partial-apply / authority / episode is a later increment (D2).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_thread_recovery_apply.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_THREAD_RECOVERY_APPLY_H
#define CLUSTER_THREAD_RECOVERY_APPLY_H

#include "c.h"
#include "access/xlogdefs.h"

#include "cluster/cluster_pi_shadow.h" /* SCN + the D-h3a recovery-boundary gate */

struct XLogReaderState;

/*
 * Outcome of applying one record to one block of a live (shared-storage) page.
 *
 *	APPLIED  FPI restored / per-rmgr delta applied; *applied_lsn = record end.
 *	DONE     LSN-gate: the page already reflects this record -> page untouched,
 *	         *applied_lsn = the page's (>= record) version.  Idempotent skip.
 *	NOOP     the record does not reference this block -> page untouched.
 *	BLOCKED  off-matrix rmgr / unusable image-or-delta -> fail-closed (8.A,
 *	         53RA4); *applied_lsn = Invalid and the caller MUST discard the
 *	         (possibly partially-mutated) page rather than install it.
 */
typedef enum ClusterThreadApplyResult {
	CLUSTER_THREADAPPLY_APPLIED = 0,
	CLUSTER_THREADAPPLY_DONE,
	CLUSTER_THREADAPPLY_NOOP,
	CLUSTER_THREADAPPLY_BLOCKED,
} ClusterThreadApplyResult;

/*
 * Pure LSN-gate routing decision for one (record, block) pair.  Depends only
 * on the block-reference fact and the record-end vs page LSNs; no shared state.
 */
typedef enum ClusterThreadApplyAction {
	CLUSTER_THREADAPPLY_ACT_NOOP,  /* record does not reference this block */
	CLUSTER_THREADAPPLY_ACT_DONE,  /* LSN-gate: page already >= record -> skip */
	CLUSTER_THREADAPPLY_ACT_APPLY, /* hand to cluster_block_apply_one (FPI/delta) */
} ClusterThreadApplyAction;

/*
 * cluster_thread_apply_decide -- pure LSN-gate routing decision.
 *
 *	A page with no reference for this block is a NOOP.  Otherwise the redo
 *	LSN-gate (mirror XLogReadBufferForRedoExtended: a record whose end LSN is
 *	at or before the page version is already reflected -> BLK_DONE) yields DONE
 *	for a POPULATED page already at/past the record.  A new (all-zero) page is
 *	never gated -- it carries no version and must be (re)built.  Anything else
 *	routes to the single-block apply core.
 */
static inline ClusterThreadApplyAction
cluster_thread_apply_decide(bool has_block_ref, bool page_is_new, XLogRecPtr record_end,
							XLogRecPtr page_lsn)
{
	if (!has_block_ref)
		return CLUSTER_THREADAPPLY_ACT_NOOP;

	/*
	 * Redo LSN-gate: a POPULATED page whose version is at or past this
	 * record's end LSN already reflects it -> skip (mirror
	 * XLogReadBufferForRedoExtended: lsn <= PageGetLSN(page) -> BLK_DONE).
	 * This is the idempotence that lets a retry / cold redo re-apply from a
	 * validated lower bound without double-applying deltas (spec-4.11 v0.3
	 * partial-apply, 8.A).  A new (all-zero) page carries no version and is
	 * never gated.
	 */
	if (!page_is_new && record_end <= page_lsn)
		return CLUSTER_THREADAPPLY_ACT_DONE;

	return CLUSTER_THREADAPPLY_ACT_APPLY;
}

extern ClusterThreadApplyResult cluster_thread_apply_record_to_page(struct XLogReaderState *record,
																	uint8 block_id, char *page,
																	XLogRecPtr *applied_lsn);

/*
 * PGRAC: spec-6.12h D-h3b -- ship-SCN-gated variant for a PAST IMAGE base.
 *
 *	When the rebuild base is a D-h1 Past Image instead of a live shared-storage
 *	page, the LSN-gate above is the WRONG unit: the PI's pd_lsn is whatever
 *	stream last wrote the bytes before they were shipped away (possibly another
 *	node's), and per-thread WAL (spec-4.1) makes LSNs from different streams
 *	numerically incomparable -- gating the first dead-thread record against a
 *	foreign pd_lsn could silently skip a lost update or re-apply lineage.  The
 *	provable boundary is the conversion's ship-SCN stamp (cluster_pi_shadow.h):
 *	a record whose Lamport counter is at or below the stamp is already in the
 *	PI bytes (DONE), strictly above must be applied (APPLIED), and an
 *	unprovable operand poisons the whole PI (BLOCKED -- the caller must
 *	abandon the PI base and fall back to storage + full redo, never keep
 *	replaying onto it).
 */
extern ClusterThreadApplyResult
cluster_pi_thread_apply_record_to_page(struct XLogReaderState *record, uint8 block_id, char *page,
									   SCN ship_scn, XLogRecPtr *applied_lsn);

#endif /* CLUSTER_THREAD_RECOVERY_APPLY_H */
