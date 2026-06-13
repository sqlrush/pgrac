/*-------------------------------------------------------------------------
 *
 * cluster_tt_recovery.c
 *	  pgrac undo/TT recovery -- crash-left ACTIVE slot resolution (spec-4.8 D1).
 *
 *	  After StartupXLOG redo + RecoverPreparedTransactions(), any durable TT
 *	  slot still in TT_SLOT_ACTIVE belongs to a transaction that was in flight
 *	  at the crash.  The by-xid resolver (cluster_tt_durable.c) only matches
 *	  TT_SLOT_COMMITTED, so an unresolved crash-left ACTIVE slot yields a
 *	  0-match that the spec-3.22 retention theorem can mis-attribute as
 *	  "recycled committed-below-horizon" rather than the truth ("aborted").
 *
 *	  cluster_tt_recovery_resolve_active_slots() scans this instance's undo
 *	  segment headers and durably transitions each crash-left ACTIVE slot to
 *	  TT_SLOT_ABORTED (WAL-logged via cluster_tt_slot_durable_abort -> 0x60)
 *	  UNLESS its owning xact is LIVE: committed per CLOG (AD-006: CLOG is the
 *	  "did it commit" authority), or a resurrected prepared 2PC xact still in
 *	  the proc array.  An xact we cannot prove live is aborted (fail-closed,
 *	  规则 8.A): cluster visibility must never treat an in-flight-at-crash
 *	  transaction as committed.
 *
 *	  The pure liveness classifier (cluster_tt_recovery_classify_liveness)
 *	  lives in cluster_tt_durable.c so it links into cluster_unit; this file
 *	  holds the backend wrapper + scanner that consult PG's CLOG / proc array
 *	  and the undo smgr, so it is intentionally NOT linked into cluster_unit.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.8-undo-tt-recovery.md (D1)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_recovery.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  spec-4.8 D0 measure-first finding (Option A reframe): the on-disk TT
 *	  header slots are NEVER written TT_SLOT_ACTIVE.  The only durable header
 *	  writes are durable_commit (-> COMMITTED) and durable_abort (-> ABORTED),
 *	  each a targeted 32-byte RMW; the in-flight ("active") binding lives only
 *	  in the in-memory allocator (CTS_ACTIVE, ClusterTTSlotAllocState) and is
 *	  lost on crash ("BIND is not WAL'd").  So a simple in-flight crash leaves
 *	  the on-disk slot UNUSED (fresh) or a stale prior COMMITTED/ABORTED state
 *	  (reused, below the retention horizon) -- never ACTIVE.  Single-node
 *	  correctness for a crashed in-flight xact is therefore already provided by
 *	  PG-native CLOG (the xact is not committed -> MVCC-invisible).
 *
 *	  cluster_tt_recovery_resolve_active_slots() is consequently a FAIL-CLOSED
 *	  DEFENSIVE NET, not the load-bearing single-node correctness fix: it
 *	  normally resolves 0 slots, but would safely abort any on-disk ACTIVE slot
 *	  that ever appeared (a torn write, a future durable-bind, or corruption),
 *	  never treating it as committed (规则 8.A).  The crash-left in-flight xact
 *	  handling that DOES carry weight lives elsewhere: cross-node TT authority
 *	  (D2) and physical heap rollback apply (D7, which identifies crashed xacts
 *	  via undo records + cluster_tt_recovery_xact_liveness, not on-disk ACTIVE
 *	  TT slots).  The pure classifier + xact_liveness here are reused by both.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"	   /* TransactionIdDidCommit */
#include "storage/procarray.h" /* TransactionIdIsInProgress */
#include "utils/elog.h"

#include "cluster/cluster_guc.h" /* cluster_enabled / cluster_node_id / GUC */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"			/* TTSlot, TT_SLOT_ACTIVE, TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_undo_segment.h"		/* UndoSegmentHeaderData */
#include "cluster/cluster_undo_smgr.h"			/* cluster_undo_smgr_read_block */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */

/*
 * cluster_tt_recovery_xact_liveness -- spec-4.8 D1.
 *
 *	Backend wrapper over the pure classifier: consult CLOG (did it commit)
 *	and the proc array (is it a resurrected prepared 2PC xact still in flight)
 *	for xid, then classify.  An xid that is neither normal nor valid is
 *	AMBIGUOUS (fail-closed -> the slot is aborted).
 *
 *	Note: CLOG alone cannot distinguish a crash-left non-prepared xact from a
 *	resurrected prepared xact -- both read as "not committed, not aborted" in
 *	CLOG.  Only TransactionIdIsInProgress (true for prepared xacts after
 *	RecoverPreparedTransactions, false for a crashed in-flight xact whose proc
 *	is gone) tells them apart, so it is required here.
 */
ClusterTtRecoveryLiveness
cluster_tt_recovery_xact_liveness(TransactionId xid)
{
	bool did_commit;
	bool is_in_progress;

	if (!TransactionIdIsValid(xid) || !TransactionIdIsNormal(xid))
		return CLUSTER_TT_RECOVERY_AMBIGUOUS;

	did_commit = TransactionIdDidCommit(xid);
	is_in_progress = TransactionIdIsInProgress(xid);

	return cluster_tt_recovery_classify_liveness(true, did_commit, is_in_progress);
}

/*
 * cluster_tt_recovery_resolve_active_slots -- spec-4.8 D1.
 *
 *	Scan this instance's undo segment headers; durably resolve every crash-
 *	left TT_SLOT_ACTIVE slot to TT_SLOT_ABORTED unless its owning xact is LIVE.
 *	Returns the number of slots resolved to ABORTED.
 *
 *	Called once from StartupXLOG after recovery completes (WAL writes enabled,
 *	prepared set loaded, RecoveryInProgress() false).  Gated by cluster.enabled
 *	+ cluster.tt_recovery_resolve_active.
 *
 *	Fail-safe on read failure: an existing-but-unreadable or absent segment is
 *	skipped (we never abort a slot we cannot read).  Leaving a slot ACTIVE is no
 *	worse than the pre-D1 behaviour (the by-xid 0-match path); aborting a slot we
 *	cannot read would risk a false-abort.  durable_abort does its own targeted
 *	32-byte RMW per slot, so the in-memory header snapshot used to iterate stays
 *	valid across slot resolutions within a segment.
 */
int
cluster_tt_recovery_resolve_active_slots(void)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	int resolved = 0;

	if (!cluster_enabled || !cluster_tt_recovery_resolve_active)
		return 0;
	if (cluster_node_id < 0)
		return 0;

	node = cluster_node_id;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
			continue; /* absent / unreadable -> skip (never false-abort) */

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];
			ClusterTtRecoveryLiveness verdict;

			if (s->status != (uint8)TT_SLOT_ACTIVE)
				continue;
			if (!TransactionIdIsValid(s->xid))
				continue;

			verdict = cluster_tt_recovery_xact_liveness(s->xid);
			if (verdict == CLUSTER_TT_RECOVERY_LIVE)
				continue; /* committed / resurrected prepared -> keep ACTIVE */

			/* DEAD or AMBIGUOUS -> fail-closed ABORTED (durable, WAL 0x60). */
			cluster_tt_slot_durable_abort(segment_id, i, s->xid, s->wrap);
			cluster_tt_recovery_count_active_resolved_aborted();
			resolved++;
		}
	}

	if (resolved > 0)
		ereport(
			LOG,
			(errmsg("cluster undo/TT recovery: resolved %d crash-left ACTIVE TT slot(s) to ABORTED",
					resolved)));

	return resolved;
}
