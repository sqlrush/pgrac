/*-------------------------------------------------------------------------
 *
 * cluster_tt_durable.c
 *	  pgrac durable Transaction Table (TT) slot commit_scn (spec-3.11 D2).
 *
 *	  Activates the durable write + lookup of the undo-segment-header TT slot
 *	  reserved by spec-3.4b (UndoSegmentHeaderData.tt_slots[], 32B each @ file
 *	  offset 112 + slot_offset*32).  See cluster_tt_durable.h for the per-API
 *	  contract and cluster_undo_xlog.c for the WAL/redo half
 *	  (XLOG_UNDO_TT_SLOT_COMMIT).
 *
 *	  Concurrency: per-slot 32B targeted writes are lock-free -- each committing
 *	  xact owns a distinct slot (non-overlapping byte range; spec-3.11 §2.2 /
 *	  Q10) and lifecycle writes the header prefix (offset 32-111), also
 *	  disjoint.  Writes are WAL-protected (no data-file fsync; a torn write is
 *	  recovered by redo -- C10).
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
 *	  src/backend/cluster/cluster_tt_durable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "utils/elog.h"

#include "cluster/cluster_guc.h"		  /* cluster_node_id */
#include "cluster/cluster_scn.h"		  /* SCN, SCN_VALID, InvalidScn */
#include "cluster/cluster_undo_cleaner.h" /* spec-3.13 D2-B scan-only pass */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"	  /* TTSlot, TT_SLOT_COMMITTED, TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_undo_segment.h" /* UndoSegmentHeaderData */
#include "cluster/cluster_undo_smgr.h"	  /* header-bytes + block I/O */
#include "cluster/storage/cluster_undo_alloc.h" /* CLUSTER_UNDO_SEGS_PER_INSTANCE */
#include "cluster/storage/cluster_undo_xlog.h"	/* cluster_undo_emit_tt_slot_commit */


/* Absolute byte offset of TTSlot[slot_offset] within segment header block 0. */
static inline uint32
tt_slot_file_offset(uint16 slot_offset)
{
	return (uint32)offsetof(UndoSegmentHeaderData, tt_slots)
		   + (uint32)slot_offset * (uint32)sizeof(TTSlot);
}

/*
 * Own-instance owner derivation from segment_id (mirrors cluster_undo_alloc.c
 * encoding: segment_id = (owner_instance-1)*SEGS + slot + 1).
 */
static inline uint8
tt_owner_instance_for_segment(uint32 segment_id)
{
	return (uint8)(((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1);
}


/* ============================================================
 *	Pure decision predicates (no I/O; cluster_unit-tested)
 * ============================================================ */

ClusterTTRedoDecision
cluster_tt_durable_redo_decide(uint8 slot_status, TransactionId slot_xid, uint16 slot_wrap,
							   TransactionId rec_xid, uint16 rec_wrap)
{
	/*
	 * spec-3.11 §2.3 redo: XLOG_UNDO_TT_SLOT_COMMIT records are authoritative and
	 * replay in LSN order, so the last commit per (segment, slot) wins.  APPLY
	 * unless the on-disk slot already shows a strictly newer generation
	 * (slot_wrap > rec_wrap) -- a later commit already made durable -- in which
	 * case SKIP so an older record does not regress it.
	 *
	 * A zero-init (UNUSED) slot or a FREE-path slot reuse keeps wrap unchanged
	 * while the xid differs: BIND is not WAL'd, so the on-disk slot lags the
	 * record, and the allocator only bumps wrap on recycle (COMMITTED/ABORTED ->
	 * ACTIVE), NOT on FREE -> ACTIVE reuse (cluster_tt_slot.c).  Hence "same
	 * wrap, different xid" is the normal first-write / sequential-reuse case
	 * during redo, NOT corruption -- it must APPLY.  规则 8.A: a crash after a
	 * committed slot reuse must replay cleanly, never PANIC.  slot_xid therefore
	 * does not affect the decision; xid identity is enforced at lookup time (C5),
	 * and WAL CRC -- not this predicate -- guards record authenticity.
	 */
	(void)slot_xid;
	if (slot_status > (uint8)TT_SLOT_RECYCLABLE)
		return CLUSTER_TT_REDO_BADSTATUS; /* on-disk status byte out of [0,4] = garbage */
	if (rec_wrap >= slot_wrap)
		return CLUSTER_TT_REDO_APPLY; /* fresh / reuse / recycle / idempotent */
	return CLUSTER_TT_REDO_SKIP;	  /* rec_wrap < slot_wrap: stale; newer commit durable */
}

bool
cluster_tt_durable_slot_match(uint8 slot_status, TransactionId slot_xid, SCN slot_commit_scn,
							  TransactionId want_xid)
{
	/* spec-3.11 C5: COMMITTED + exact xid + valid commit_scn.  xid mismatch is
	 * the recycle detector (reuse stamps a new owner xid). */
	return slot_status == (uint8)TT_SLOT_COMMITTED && slot_xid == want_xid
		   && SCN_VALID(slot_commit_scn);
}


void
cluster_tt_slot_durable_commit(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   uint16 wrap, SCN commit_scn)
{
	uint8 owner = tt_owner_instance_for_segment(segment_id);
	uint32 off = tt_slot_file_offset(slot_offset);
	TTSlot slot;

	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);
	Assert(TransactionIdIsValid(xid));
	Assert(SCN_VALID(commit_scn));

	/*
	 * spec-3.11 C1: WAL BEFORE the commit record (caller is in the pre-commit
	 * hook).  The commit record's XLogFlush / group commit makes it durable;
	 * the data-file write below is NOT fsync'd (C10) -- a crash before the
	 * commit record means neither is durable; after, redo replays this WAL.
	 */
	(void)cluster_undo_emit_tt_slot_commit(owner, segment_id, slot_offset, wrap, xid, commit_scn);

	/*
	 * Per-slot 32B targeted RMW: read the slot (preserve flags /
	 * first_undo_block), set the commit fields, write 32B back.  Lock-free --
	 * this xact is the sole owner of this slot (spec-3.11 §2.2).
	 */
	cluster_tt_durable_io_wait_start();

	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_COMMITTED;
	slot.commit_scn = commit_scn;

	if (!cluster_undo_smgr_write_header_bytes(segment_id, owner, off, (const char *)&slot,
											  sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
							   slot_offset, segment_id)));
	}

	cluster_tt_durable_io_wait_end();
	cluster_tt_durable_count_commit();
}


bool
cluster_tt_slot_durable_lookup(uint32 segment_id, uint16 slot_offset, TransactionId xid,
							   SCN *commit_scn)
{
	uint8 owner;
	uint32 off;
	TTSlot slot;

	if (commit_scn == NULL || slot_offset >= TT_SLOTS_PER_SEGMENT)
		return false;

	owner = tt_owner_instance_for_segment(segment_id);
	off = tt_slot_file_offset(slot_offset);

	cluster_tt_durable_io_wait_start();
	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot))) {
		cluster_tt_durable_io_wait_end();
		cluster_tt_durable_count_lookup(false);
		return false; /* segment absent / I/O -> miss (caller fail-closes) */
	}
	cluster_tt_durable_io_wait_end();

	/*
	 * spec-3.11 C5 (规则 8.A): the slot must still be bound to this xid and be
	 * COMMITTED with a valid commit_scn.  xid mismatch = the slot was recycled
	 * by a later owner; never return that owner's commit_scn.
	 */
	if (!cluster_tt_durable_slot_match(slot.status, slot.xid, slot.commit_scn, xid)) {
		cluster_tt_durable_count_lookup(false);
		return false;
	}

	*commit_scn = slot.commit_scn;
	cluster_tt_durable_count_lookup(true);
	return true;
}


bool
cluster_tt_slot_durable_lookup_by_xid(TransactionId xid, SCN *commit_scn)
{
	int node;
	uint8 owner;
	uint32 seg_lo;
	uint32 seg_hi;
	uint32 segment_id;
	PGAlignedBlock blockbuf;
	int matches = 0;
	SCN found = InvalidScn;

	if (commit_scn == NULL || !TransactionIdIsNormal(xid))
		return false;
	if (cluster_node_id < 0)
		return false; /* single-node degraded: no durable TT scan */

	node = cluster_node_id;
	owner = (uint8)(node + 1);
	seg_lo = (uint32)node * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	seg_hi = seg_lo + CLUSTER_UNDO_SEGS_PER_INSTANCE - 1;

	cluster_tt_durable_count_by_xid_scan();

	/*
	 * spec-3.11 §2.2 / C4 / 规则 8.A: scan the local node's segment headers for a
	 * COMMITTED slot owned by xid.  Exactly one match resolves precisely; zero
	 * (recycled / overwritten) or >1 (raw-xid wrap residue) fails closed.
	 *
	 * Single-match soundness under spec-3.12 retention: an xid commits exactly
	 * once, so it stamps at most one durable COMMITTED slot across all segments.
	 * Recycle (D2) only OVERWRITES a slot with a new owner xid (the old xid then
	 * resolves to 0 matches -> fail-closed), and rollover (D2b) keeps a rolled-
	 * away segment's durable slots intact while never reusing their offsets, so
	 * recycle can never raise the count to >1.  C4: this lookup is reached only
	 * for a writer newer than read_scn (its ITL was recycled with a watermark
	 * newer than read_scn, so the writer's commit_scn is at or newer than the
	 * horizon), whose slot retention keeps alive -> the match is guaranteed to
	 * hit.  spec-3.13 xid index is a scan-cost optimization (§6 risk), not a
	 * correctness change.
	 */
	cluster_tt_durable_io_wait_start();
	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
			continue; /* segment file never allocated -> skip */

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			const TTSlot *s = &hdr->tt_slots[i];

			if (s->status == (uint8)TT_SLOT_COMMITTED && s->xid == xid
				&& SCN_VALID(s->commit_scn)) {
				matches++;
				found = s->commit_scn;
				if (matches > 1) {
					cluster_tt_durable_io_wait_end();
					return false; /* ambiguous -> fail-closed (规则 8.A) */
				}
			}
		}
	}
	cluster_tt_durable_io_wait_end();

	if (matches != 1)
		return false; /* 0 = not found (recycled/overwritten) */

	*commit_scn = found;
	return true;
}


/*
 * cluster_undo_segment_tt_header_scan_pass -- spec-3.13 D2-B (v0.3
 * scan-only).
 *
 *	READ-ONLY classification of one segment's durable TTSlot[] (block 0
 *	@ offset 112, 48 x 32B).  Produces inventory counts for the segment-
 *	level evaluation (D3) and observability (D6); deliberately writes
 *	NOTHING:
 *	  - rewriting COMMITTED -> RECYCLABLE has zero effect on the segment
 *	    predicate (it only watermarks TT_SLOT_COMMITTED), and
 *	  - it would break cluster_tt_durable_slot_match (COMMITTED-only),
 *	    degrading old unresolved-ITL readers' by-xid resolve to 53R97 —
 *	    worse than the 3.12 lazy status quo.  (spec-3.13 v0.3 ③)
 *
 *	Classification mirror of the shmem predicate, typed for on-disk
 *	TT_SLOT_* (C-R1: shared comparison semantics — strict < horizon,
 *	UNKNOWN retains — without mixing the CTS_* / TT_SLOT_* enums):
 *	  TT_SLOT_COMMITTED + valid scn < horizon  -> below_horizon++
 *	  TT_SLOT_COMMITTED + invalid scn          -> unresolved++ (8.A retain)
 *	  TT_SLOT_ACTIVE                           -> stale_active_skipped++ (HC6)
 *	  UNUSED / ABORTED / RECYCLABLE            -> no inventory impact
 */
bool
cluster_undo_segment_tt_header_scan_pass(uint32 segment_id, uint8 owner_instance, SCN horizon,
										 ClusterUndoCleanerPassStats *stats)
{
	PGAlignedBlock block;
	const UndoSegmentHeaderData *hdr;
	int i;

	Assert(stats != NULL);

	/* Whole-block read mirrors the by-xid scan shape (one smgr surface). */
	cluster_undo_cleaner_scan_wait_start();
	if (!cluster_undo_smgr_read_block(segment_id, owner_instance, 0, block.data)) {
		cluster_undo_cleaner_scan_wait_end();
		return false; /* absent / I/O: caller counts and moves on */
	}
	hdr = (const UndoSegmentHeaderData *)block.data;
	cluster_undo_cleaner_scan_wait_end();

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const TTSlot *s = &hdr->tt_slots[i];

		switch (s->status) {
		case TT_SLOT_COMMITTED:
			if (!SCN_VALID(s->commit_scn))
				stats->header_unresolved_committed++;
			else if (SCN_VALID(horizon) && scn_time_cmp(s->commit_scn, horizon) < 0)
				stats->header_tt_slots_below_horizon++;
			else
				stats->header_retained_committed++; /* at/above horizon: pinned signal */
			break;
		case TT_SLOT_ACTIVE:
			stats->stale_active_skipped++; /* HC6: never judged, only counted */
			break;
		default:
			break; /* UNUSED / ABORTED / RECYCLABLE: no inventory impact */
		}
	}

	stats->segments_scanned++;
	return true;
}
