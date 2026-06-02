/*-------------------------------------------------------------------------
 *
 * cluster_tt_durable.h
 *	  pgrac durable Transaction Table (TT) slot commit_scn -- write / lookup
 *	  / by-xid scan (spec-3.11, own-instance).
 *
 *	  The durable TT slot lives in the undo segment header block 0
 *	  (UndoSegmentHeaderData.tt_slots[slot_offset], 32 bytes @ file offset
 *	  112 + slot_offset*32; commit_scn @ slot+8).  spec-3.4b reserved the
 *	  on-disk format; spec-3.11 activates the durable write + crash-recovery
 *	  (XLOG_UNDO_TT_SLOT_COMMIT, cluster_undo_xlog.c) + lookup so visibility /
 *	  CR can resolve commit_scn after overlay eviction or restart, retiring the
 *	  spec-3.10 watermark fail-closed for the still-bound case.
 *
 *	  Durable writes are per-slot 32-byte targeted pwrites: each committing
 *	  xact owns a distinct slot (non-overlapping byte range), so writes are
 *	  lock-free, and they are WAL-protected (no data-file fsync; redo recovers
 *	  a torn write -- spec-3.11 §2.2 / C10 / Q10).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§2.2, D2)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tt_durable.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_DURABLE_H
#define CLUSTER_TT_DURABLE_H

#include "cluster/cluster_scn.h" /* SCN */

/*
 * Pure decision predicates (no I/O) -- shared by the runtime lookup/redo paths
 * and exercised directly by cluster_unit (file I/O behavior is e2e in t/219).
 */

/* Redo wrap-comparison decision (spec-3.11 §2.3 table). */
typedef enum ClusterTTRedoDecision {
	CLUSTER_TT_REDO_APPLY,	  /* rec.wrap > slot.wrap, or == with same xid (idempotent) */
	CLUSTER_TT_REDO_SKIP,	  /* rec.wrap < slot.wrap (stale; newer commit durable) */
	CLUSTER_TT_REDO_CORRUPT,  /* rec.wrap == slot.wrap, different xid (8.A; PANIC) */
	CLUSTER_TT_REDO_BADSTATUS /* slot.status out of [0,4] (corruption; PANIC) */
} ClusterTTRedoDecision;

/*
 * cluster_tt_durable_redo_decide -- decide what an XLOG_UNDO_TT_SLOT_COMMIT
 *	redo should do to the on-disk slot, given the slot's current
 *	(status, xid, wrap) and the record's (xid, wrap).  Pure; no I/O.
 */
extern ClusterTTRedoDecision cluster_tt_durable_redo_decide(uint8 slot_status,
															TransactionId slot_xid,
															uint16 slot_wrap, TransactionId rec_xid,
															uint16 rec_wrap);

/*
 * cluster_tt_durable_slot_match -- true iff a durable slot with the given
 *	(status, xid, commit_scn) is a valid still-bound COMMITTED match for the
 *	wanted xid (spec-3.11 C5).  Pure; no I/O.
 *
 *	Matches on xid (not wrap): slot reuse stamps a new owner xid, so an xid
 *	mismatch is the recycle detector (the lookup key -- ClusterTTStatusKey --
 *	carries local_xid, not wrap; wrap is the WAL/redo ordering field only).
 */
extern bool cluster_tt_durable_slot_match(uint8 slot_status, TransactionId slot_xid,
										  SCN slot_commit_scn, TransactionId want_xid);

/*
 * cluster_tt_slot_durable_commit -- durably stamp commit_scn (status COMMITTED)
 *	on the own-instance TT slot (segment_id, slot_offset) owned by `xid`/`wrap`.
 *	Emits XLOG_UNDO_TT_SLOT_COMMIT (before the commit record -- caller is in the
 *	pre-commit hook), then per-slot 32-byte targeted write of the TTSlot.  No
 *	fsync (WAL-protected).  ereport(ERROR) on I/O failure.
 */
extern void cluster_tt_slot_durable_commit(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										   uint16 wrap, SCN commit_scn);

/*
 * cluster_tt_slot_durable_lookup -- read the durable TT slot (segment_id,
 *	slot_offset) and return its commit_scn iff the slot is still bound to
 *	`xid` and COMMITTED with a valid commit_scn.  xid mismatch (slot recycled
 *	by a later owner) or UNUSED -> false (never returns another owner's
 *	commit_scn -- spec-3.11 C5).  Returns true + *commit_scn on hit.  Callers
 *	must apply the C1b CLOG cross-check (the slot is stamped at pre-commit).
 */
extern bool cluster_tt_slot_durable_lookup(uint32 segment_id, uint16 slot_offset, TransactionId xid,
										   SCN *commit_scn);

/*
 * cluster_tt_slot_durable_lookup_by_xid -- scan the local node's undo segment
 *	headers for a COMMITTED TT slot owned by `xid`.  Used by the spec-3.10
 *	watermark gate (the ITL slot was recycled, so ITL->UBA->slot is unavailable;
 *	resolve by xid).  Exactly one match -> true + *commit_scn; zero matches
 *	(recycled / overwritten) or more than one (xid wrap residue ambiguity) ->
 *	false -> caller fail-closes (53R9F; spec-3.11 §2.2 / C4 / 规则 8.A).
 *	Cost O(local_segments * TT_SLOTS_PER_SEGMENT); bounded by spec-3.12 retention.
 */
extern bool cluster_tt_slot_durable_lookup_by_xid(TransactionId xid, SCN *commit_scn);

#endif /* CLUSTER_TT_DURABLE_H */
