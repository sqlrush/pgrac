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

#include "cluster/cluster_guc.h" /* cluster_node_id */
#include "cluster/cluster_scn.h" /* SCN, SCN_VALID, InvalidScn */
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
	/* spec-3.11 §2.3 wrap-comparison table. */
	if (slot_status > (uint8)TT_SLOT_RECYCLABLE)
		return CLUSTER_TT_REDO_BADSTATUS; /* status out of [0,4] = corruption */
	if (rec_wrap > slot_wrap)
		return CLUSTER_TT_REDO_APPLY; /* recycle-then-commit (normal) / fresh slot */
	if (rec_wrap == slot_wrap) {
		if (slot_xid == rec_xid)
			return CLUSTER_TT_REDO_APPLY; /* idempotent same-owner */
		return CLUSTER_TT_REDO_CORRUPT;	  /* same generation, different owner (8.A) */
	}
	return CLUSTER_TT_REDO_SKIP; /* rec_wrap < slot_wrap: stale; newer commit durable */
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
	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot)))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot read slot %u of undo segment %u",
							   slot_offset, segment_id)));

	slot.xid = xid;
	slot.wrap = wrap;
	slot.status = (uint8)TT_SLOT_COMMITTED;
	slot.commit_scn = commit_scn;

	if (!cluster_undo_smgr_write_header_bytes(segment_id, owner, off, (const char *)&slot,
											  sizeof(slot)))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster durable TT: cannot write slot %u of undo segment %u",
							   slot_offset, segment_id)));
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

	if (!cluster_undo_smgr_read_header_bytes(segment_id, owner, off, (char *)&slot, sizeof(slot)))
		return false; /* segment absent / I/O -> miss (caller fail-closes) */

	/*
	 * spec-3.11 C5 (规则 8.A): the slot must still be bound to this xid and be
	 * COMMITTED with a valid commit_scn.  xid mismatch = the slot was recycled
	 * by a later owner; never return that owner's commit_scn.
	 */
	if (!cluster_tt_durable_slot_match(slot.status, slot.xid, slot.commit_scn, xid))
		return false;

	*commit_scn = slot.commit_scn;
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

	/*
	 * spec-3.11 §2.2 / C4 / 规则 8.A: scan the local node's segment headers for a
	 * COMMITTED slot owned by xid.  Exactly one match resolves precisely; zero
	 * (recycled / overwritten) or >1 (raw-xid wrap residue) fails closed.
	 *
	 * Single-match soundness (pre-spec-3.12): no retention/recycle yet means a
	 * committed xid's slot is not reused, so at most one COMMITTED slot per live
	 * xid; xid wraparound is bounded by PG freeze well before reuse.  spec-3.12
	 * retention + spec-3.13 xid index tighten this (§6 risk).
	 */
	for (segment_id = seg_lo; segment_id <= seg_hi; segment_id++) {
		UndoSegmentHeaderData *hdr;
		uint16 i;

		if (!cluster_undo_smgr_read_block(segment_id, owner, 0, blockbuf.data))
			continue; /* segment file never allocated -> skip */

		hdr = (UndoSegmentHeaderData *)blockbuf.data;
		for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			TTSlot *s = &hdr->tt_slots[i];

			if (s->status == (uint8)TT_SLOT_COMMITTED && s->xid == xid
				&& SCN_VALID(s->commit_scn)) {
				matches++;
				found = s->commit_scn;
				if (matches > 1)
					return false; /* ambiguous -> fail-closed (规则 8.A) */
			}
		}
	}

	if (matches != 1)
		return false; /* 0 = not found (recycled/overwritten) */

	*commit_scn = found;
	return true;
}
