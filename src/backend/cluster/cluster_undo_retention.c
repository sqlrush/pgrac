/*-------------------------------------------------------------------------
 *
 * cluster_undo_retention.c
 *	  pgrac own-instance undo / TT-slot retention predicates (spec-3.12 D2/D3).
 *
 *	  This file holds the two PURE judgement helpers that the lazy retention
 *	  gate consults at TT-slot / undo-segment allocation time:
 *
 *	    cluster_tt_slot_recyclable()      -- per-slot allocator-status gate
 *	    cluster_undo_segment_recyclable() -- per-segment header gate
 *
 *	  Neither takes a lock, touches shmem, or does I/O: they are functions of
 *	  (status, commit_scn, horizon) only, so cluster_unit can exercise every
 *	  branch without a live postmaster (test_cluster_retention).  The horizon
 *	  itself is produced by cluster_undo_retention_horizon() in procarray.c
 *	  (it scans the ProcArray under ProcArrayLock; see C17 lock ordering).
 *
 *	  Correctness contract (CLAUDE.md rule 8.A — MVCC/visibility must be sound
 *	  or fail-closed within this spec):
 *	    - A reader at read_scn == commit_scn still needs the pre-image of that
 *	      version, so the comparison is STRICT '<' (equality => retained).
 *	    - An UNKNOWN (InvalidScn) commit_scn on a COMMITTED slot cannot be
 *	      proven below the horizon, so it is RETAINED (never recycled on a
 *	      guess).
 *	    - ABORTED versions are invisible to every read_scn (abort already
 *	      rolled the row back in place; CR rebuilds committed history only), so
 *	      no reader needs their undo => immediately recyclable (C7).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.12-retention-horizon.md (§2, §3 C3/C5/C7, D2/D3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_retention.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_undo_segment.h"


/*
 * cluster_tt_slot_recyclable
 *
 *	Decide whether a TT-slot allocator entry may be recycled under the current
 *	retention horizon.  See header / file-banner contract.
 */
bool
cluster_tt_slot_recyclable(uint8 cts_status, SCN commit_scn, SCN horizon)
{
	/* C7: aborted versions are invisible to any read_scn -> always recyclable. */
	if (cts_status == CTS_ABORTED)
		return true;

	/* Only COMMITTED slots are gated; ACTIVE (in-flight) / FREE are not. */
	if (cts_status != CTS_COMMITTED)
		return false;

	/*
	 * InvalidScn horizon == cluster disabled (no live cluster reader can
	 * exist) -> no retention constraint, recycle freely.
	 */
	if (!SCN_VALID(horizon))
		return true;

	/*
	 * rule 8.A: a COMMITTED slot whose commit_scn is unresolved cannot be
	 * shown to be below the horizon -> fail-closed (retain).
	 */
	if (!SCN_VALID(commit_scn))
		return false;

	/* Strict '<': commit_scn == horizon is still needed by the oldest reader. */
	return scn_time_cmp(commit_scn, horizon) < 0;
}


/*
 * cluster_undo_segment_recyclable
 *
 *	Decide whether an undo segment may be recycled under the current horizon.
 *	Precondition: SEGMENT_COMMITTED (C5).  The segment's retention watermark is
 *	the max commit_scn over the COMMITTED on-disk TT slots in its header; the
 *	segment is recyclable only when that watermark is strictly below horizon.
 */
bool
cluster_undo_segment_recyclable(const struct UndoSegmentHeaderData *hdr, SCN horizon)
{
	SCN watermark = InvalidScn;
	bool saw_unresolved_committed = false;
	int i;

	if (hdr == NULL)
		return false;

	/*
	 * C5: only a SEGMENT_COMMITTED segment participates.  ALLOCATED / ACTIVE /
	 * FULL-but-ACTIVE (fullness is a flag, state stays ACTIVE) / RECYCLABLE are
	 * never selected here -- an "empty" ACTIVE segment is one being written,
	 * not a recycle candidate.
	 */
	if (hdr->segment_state != SEGMENT_COMMITTED)
		return false;

	/* InvalidScn horizon == cluster disabled -> no retention constraint. */
	if (!SCN_VALID(horizon))
		return true;

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const TTSlot *s = &hdr->tt_slots[i];

		if (s->status != TT_SLOT_COMMITTED)
			continue;
		if (!SCN_VALID(s->commit_scn)) {
			/* rule 8.A: unresolved committed slot -> retain whole segment. */
			saw_unresolved_committed = true;
			continue;
		}
		if (!SCN_VALID(watermark) || scn_time_cmp(s->commit_scn, watermark) > 0)
			watermark = s->commit_scn;
	}

	if (saw_unresolved_committed)
		return false;

	/* SEGMENT_COMMITTED with no live committed slot -> nothing to retain. */
	if (!SCN_VALID(watermark))
		return true;

	return scn_time_cmp(watermark, horizon) < 0;
}


/*
 * cluster_undo_record_segment_drainable -- spec-4.12a D1.
 *
 *	Decide whether a record segment may advance SEGMENT_ACTIVE -> COMMITTED.
 *	Pure: a function of the on-disk header plus the caller-supplied active-write
 *	boundary, the unresolved-prepared flag, and the three excluded segment ids
 *	(every shmem read is performed by the caller in cluster_undo_record.c).
 *	Once COMMITTED, cluster_undo_segment_recyclable() above still gates the
 *	actual reclaim under the retention horizon, so this is purely a liveness
 *	(reclaim-eligibility) gate, never a reader-visibility gate.
 *
 *	Six 8.A hard gates (spec-4.12a §0), every one fail-closed toward "retain":
 *	  - candidate state: only SEGMENT_ACTIVE (FULL is a flag, not a state, so a
 *	    FULL-but-ACTIVE segment IS a candidate).  ALLOCATED / COMMITTED /
 *	    RECYCLABLE / INVALID / NULL -> retain (硬门 4: unexpected state is safe).
 *	  - guard 2: never the spec-3.4b fixed first segment (shared with the
 *	    record write cursor's start; reclaiming it would corrupt the cursor).
 *	  - guard 3: never the record cursor's or the TT cursor's current segment.
 *	  - guard 6: any unresolved cluster-TT prepared xact -> retain ALL advances
 *	    (its undo may still be consumed by ROLLBACK PREPARED; Q11-A minimal-safe).
 *	  - guard 1: the segment's sealed upper bound (record_seal_upper_scn) must be
 *	    strictly below the oldest active-write boundary.  Each in-flight writer
 *	    registered first_undo_scn BEFORE claiming an extent in (hence sealing)
 *	    this segment, so boundary <= first_undo_scn <= seal whenever an in-flight
 *	    writer has undo here; strict '<' therefore retains exactly while such a
 *	    writer exists.  An unsealed/unknown upper bound (InvalidScn) cannot be
 *	    proven safe -> retain.  boundary.infinite (no in-flight writer) -> drain.
 */
bool
cluster_undo_record_segment_drainable(const struct UndoSegmentHeaderData *hdr,
									  ClusterUndoActiveBoundary boundary,
									  bool any_unresolved_prepared, uint32 fixed_first_segment_id,
									  uint32 active_record_segment_id, uint32 active_tt_segment_id,
									  bool recovery_in_progress)
{
	SCN seal_upper_scn;

	if (hdr == NULL)
		return false;

	/*
	 * guard 0 (spec-4.12a D3, crash-recovery, 硬门 4): never drain while the
	 * server is replaying WAL.  A crash empties the in-memory active-write
	 * registry, so the caller's boundary degrades to {infinite}; that cannot
	 * prove the absence of prepared / in-flight undo until
	 * RecoverPreparedTransactions has rebuilt the protected-slot view.  Draining
	 * during recovery is therefore never provably safe -> retain (fail-closed),
	 * overriding every per-segment gate below.  This is the single auditable
	 * recovery decision point; the imperative callers reach it only post-PM_RUN
	 * (so it is also belt-and-suspenders), but the gate keeps the invariant
	 * local and robust to future recovery-time callers.
	 */
	if (recovery_in_progress)
		return false;

	/* Only an ACTIVE record segment is a drain candidate (硬门 4). */
	if (hdr->segment_state != SEGMENT_ACTIVE)
		return false;

	/* guard 2: the spec-3.4b fixed first segment is never drained. */
	if (hdr->segment_id == fixed_first_segment_id)
		return false;

	/* guard 3: neither the record nor the TT cursor's current segment. */
	if (hdr->segment_id == active_record_segment_id)
		return false;
	if (hdr->segment_id == active_tt_segment_id)
		return false;

	/* guard 6: conservatively retain all while any prepared xact is unresolved. */
	if (any_unresolved_prepared)
		return false;

	/* guard 1 (in-flight): an unsealed/unknown upper bound cannot be proven
	 * safe -> retain (fail-closed). */
	seal_upper_scn = UndoSegmentHeader_record_seal_upper_scn(hdr);
	if (!SCN_VALID(seal_upper_scn))
		return false;

	/* No in-flight writer at all -> every sealed segment drains (quiesce). */
	if (boundary.infinite)
		return true;

	/* Strict '<': boundary == seal means an in-flight writer may have begun
	 * exactly at the seal point -> retain (equality is not safe). */
	return scn_time_cmp(seal_upper_scn, boundary.scn) < 0;
}
