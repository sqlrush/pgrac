/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_apply.c
 *	  pgrac online thread-recovery page apply matrix (spec-4.11 D1, Q10-B).
 *
 *	  cluster_thread_apply_record_to_page() applies ONE dead-thread WAL record's
 *	  effect on ONE block to a live page, for a survivor online-replaying the
 *	  dead thread's data to shared storage within the reconfig freeze window.
 *
 *	  It is the t/256-proven single-block apply core (cluster_block_apply_one)
 *	  wrapped with PG redo's LSN-gate.  The gate is the whole new
 *	  corruption-critical surface (8.A): unlike spec-4.10, which rebuilds a
 *	  block from a clean detached FPI base (each record applied exactly once),
 *	  online thread recovery applies the dead thread's stream to a LIVE page
 *	  that may already reflect a record.  A page already at or past the record's
 *	  end LSN is left untouched (DONE), mirroring XLogReadBufferForRedoExtended's
 *	  BLK_DONE -- which is exactly what makes stream apply-through idempotent
 *	  when a retry / cold redo re-applies from a validated lower bound after a
 *	  partial-apply crash (spec-4.11 v0.3 partial-apply).
 *
 *	  Global-side-effect suppression: this function NEVER advances nextXid,
 *	  switches timelines, or writes XLogRecoveryCtl -- it only mutates the
 *	  caller's page (the D0 STOP GATE A reason ApplyWalRecord is not online-safe
 *	  for a foreign segment's records, spec-4.11 Impl note v0.1).
 *
 *	  fail-closed (8.A): cluster_block_apply_one returning anything but OK (an
 *	  off-matrix rmgr -> UNSUPPORTED, or an unusable image/delta -> FAILED) maps
 *	  to BLOCKED with *applied_lsn left Invalid; the driver discards the
 *	  (possibly partially-mutated) page rather than install a wrong block.  This
 *	  is how multi-block / unclassifiable records (btree/gin split, heap
 *	  cross-page update) fail closed: they are off the heap matrix, so a block
 *	  reference to them returns UNSUPPORTED -> BLOCKED (forward Stage 5).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_apply.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/bufpage.h"

#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_thread_recovery_apply.h"

/*
 * cluster_thread_apply_record_to_page -- LSN-gated single-block apply-through.
 *
 * Inputs:
 *	record:      a decoded, CRC-validated WAL record (read-only) from the dead
 *	             thread's stream; the caller (D2 driver) feeds records in LSN
 *	             order and treats them as foreign.
 *	block_id:    the record's block-reference id for the target block.
 *	page:        BLCKSZ live page (read from shared storage); mutated only on
 *	             APPLIED.  On BLOCKED the caller MUST discard it.
 *	applied_lsn: out; the page's resulting version (record end on APPLIED, the
 *	             page's existing LSN on DONE/NOOP, Invalid on BLOCKED).
 *
 * Returns: ClusterThreadApplyResult (see header).
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadApplyResult
cluster_thread_apply_record_to_page(XLogReaderState *record, uint8 block_id, char *page,
									XLogRecPtr *applied_lsn)
{
	bool has_ref = XLogRecHasBlockRef(record, block_id);
	bool page_is_new = PageIsNew(page);
	XLogRecPtr page_lsn = page_is_new ? InvalidXLogRecPtr : PageGetLSN(page);
	XLogRecPtr record_end = record->EndRecPtr;
	ClusterThreadApplyAction action;
	ClusterBlkApplyResult res;

	action = cluster_thread_apply_decide(has_ref, page_is_new, record_end, page_lsn);

	switch (action) {
	case CLUSTER_THREADAPPLY_ACT_NOOP:
		if (applied_lsn)
			*applied_lsn = page_lsn;
		return CLUSTER_THREADAPPLY_NOOP;

	case CLUSTER_THREADAPPLY_ACT_DONE:
		/* Idempotent skip: the page already reflects this record. */
		if (applied_lsn)
			*applied_lsn = page_lsn;
		return CLUSTER_THREADAPPLY_DONE;

	case CLUSTER_THREADAPPLY_ACT_APPLY:

		/*
		 * Hand the (record, block) to the verified single-block apply core.
		 * It re-decides FPI vs per-rmgr delta and applies byte-for-byte
		 * (t/256).  Any non-OK outcome is fail-closed (8.A): off-matrix rmgr
		 * (UNSUPPORTED) or unusable image/delta (FAILED) -> BLOCKED; NOOP is
		 * impossible here (has_ref is true).
		 */
		res = cluster_block_apply_one(record, block_id, page);
		if (res == CLUSTER_BLKAPPLY_OK) {
			if (applied_lsn)
				*applied_lsn = record_end;
			return CLUSTER_THREADAPPLY_APPLIED;
		}
		if (applied_lsn)
			*applied_lsn = InvalidXLogRecPtr;
		return CLUSTER_THREADAPPLY_BLOCKED;
	}

	/* cluster_thread_apply_decide returns one of the cases above. */
	if (applied_lsn)
		*applied_lsn = InvalidXLogRecPtr;
	return CLUSTER_THREADAPPLY_BLOCKED;
}

/*
 * cluster_pi_thread_apply_record_to_page -- ship-SCN-gated single-block
 *	apply-through over a Past Image base (spec-6.12h D-h3b).
 *
 * Mirrors cluster_thread_apply_record_to_page above with ONE substitution:
 * the per-record gate is the D-h3a recovery-boundary judge over (xl_scn,
 * ship_scn) instead of the record-end vs page-LSN compare, which is the
 * wrong unit over a PI base (see the header note).  The page evolves in the
 * caller's PRIVATE copy (cluster_bufmgr_snapshot_pi_block); the resident PI
 * buffer is never mutated, so a retry re-snapshots a fresh base and
 * idempotence needs no page-version gate.  DONE here means "lineage: the
 * conversion-frozen bytes already contain this record's effect".
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterThreadApplyResult
cluster_pi_thread_apply_record_to_page(XLogReaderState *record, uint8 block_id, char *page,
									   SCN ship_scn, XLogRecPtr *applied_lsn)
{
	bool has_ref = XLogRecHasBlockRef(record, block_id);
	XLogRecPtr page_lsn = PageIsNew(page) ? InvalidXLogRecPtr : PageGetLSN(page);
	ClusterBlkApplyResult res;

	if (!has_ref) {
		if (applied_lsn)
			*applied_lsn = page_lsn;
		return CLUSTER_THREADAPPLY_NOOP;
	}

	switch (cluster_pi_recovery_gate((SCN)XLogRecGetScn(record), ship_scn)) {
	case CLUSTER_PI_GATE_UNUSABLE:
		/* Boundary unprovable -> the WHOLE PI base is unusable (8.A): the
		 * caller must stop replaying onto this copy and discard it. */
		if (applied_lsn)
			*applied_lsn = InvalidXLogRecPtr;
		return CLUSTER_THREADAPPLY_BLOCKED;

	case CLUSTER_PI_GATE_SKIP:
		/* Lineage: already reflected in the conversion-frozen bytes. */
		if (applied_lsn)
			*applied_lsn = page_lsn;
		return CLUSTER_THREADAPPLY_DONE;

	case CLUSTER_PI_GATE_APPLY:
		/* Post-ship: hand to the verified single-block apply core (same
		 * fail-closed mapping as the LSN-gated wrapper above). */
		res = cluster_block_apply_one(record, block_id, page);
		if (res == CLUSTER_BLKAPPLY_OK) {
			if (applied_lsn)
				*applied_lsn = record->EndRecPtr;
			return CLUSTER_THREADAPPLY_APPLIED;
		}
		if (applied_lsn)
			*applied_lsn = InvalidXLogRecPtr;
		return CLUSTER_THREADAPPLY_BLOCKED;
	}

	/* cluster_pi_recovery_gate returns one of the cases above. */
	if (applied_lsn)
		*applied_lsn = InvalidXLogRecPtr;
	return CLUSTER_THREADAPPLY_BLOCKED;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
