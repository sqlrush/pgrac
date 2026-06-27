/*-------------------------------------------------------------------------
 *
 * cluster_cr_tuple.c
 *	  pgrac tuple-level / verdict-only CR read fast path (spec-5.54 D1/D2/D4).
 *
 *	  Layered IN FRONT OF the spec-3.9 full-block CR construct: when a snapshot
 *	  needs the visibility verdict of ONE post-read_scn tuple on a block with
 *	  exactly one candidate transaction (nchains == 1), reconstruct ONLY the
 *	  queried offnum on a backend-local scratch via a single-chain target-offnum
 *	  walk and run the SAME per-tuple verdict the full-block gate runs.  A DECIDED
 *	  verdict is bit-equivalent to full-block (INV-T1); any uncertainty falls back
 *	  to the authoritative full-block path (INV-T2/T3).  See cluster_cr_tuple.h
 *	  for the six invariants.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_tuple.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "storage/itemid.h"

#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_cr_tuple.h"
#include "cluster/cluster_undo_record.h"

/*
 * cluster_cr_tuple_eligible_page -- pure static eligibility predicate (D1).
 * No undo image is read (lineage identity is a WALK-time check, INV-T4), so this
 * is side-effect-free over the page + scalar inputs.  Order mirrors spec §3.2.
 */
bool
cluster_cr_tuple_eligible_page(Page page, OffsetNumber offnum, SCN read_scn, int nchains,
							   int32 tuple_origin_node_id, int32 self_node_id,
							   ClusterCRTupleOutcome *reason)
{
	SCN recycle_wm;
	OffsetNumber maxoff;

	/* 1. own-instance only (F0-4): a materialized-remote tuple falls back. */
	if (tuple_origin_node_id != self_node_id) {
		if (reason != NULL)
			*reason = CR_TUPLE_OUTCOME_FALLBACK_REMOTE;
		return false;
	}

	/*
	 * 2. recycle watermark <= read_scn (F0-16): a completed post-read_scn DATA
	 * slot recycled out of this block would make the per-page candidate set
	 * incomplete -> the single-chain walk could miss a post-read_scn version.
	 */
	recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;
	if (SCN_VALID(recycle_wm) && SCN_VALID(read_scn) && scn_time_cmp(recycle_wm, read_scn) > 0) {
		if (reason != NULL)
			*reason = CR_TUPLE_OUTCOME_FALLBACK_RECYCLE_WM;
		return false;
	}

	/*
	 * 3. nchains == 1 (INV-T6): a single post-read_scn transaction means the one
	 * chain IS the whole candidate set (no cross-transaction multi-update miss).
	 * nchains > 1 falls back (perf forward v2); nchains < 1 is degenerate (the
	 * tier-2 gate proved a post-read_scn writer exists) -> not in scope.
	 */
	if (nchains != 1) {
		if (reason != NULL)
			*reason = (nchains > 1) ? CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN
									: CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return false;
	}

	/* 4. queried offnum must be in the page (basic form). */
	maxoff = PageGetMaxOffsetNumber(page);
	if (offnum < FirstOffsetNumber || offnum > maxoff) {
		if (reason != NULL)
			*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return false;
	}

	if (reason != NULL)
		*reason = CR_TUPLE_OUTCOME_VERDICT;
	return true;
}

/*
 * cluster_cr_tuple_prune_target -- per-step target-only prune (D2, §3.3).
 * Mirrors full-block's whole-page cluster_cr_prune_post_snapshot_versions
 * restricted to the queried offnum: a tuple whose raw xmin is the single
 * candidate transaction did not exist at read_scn (F0-25), so mark it LP_UNUSED.
 */
void
cluster_cr_tuple_prune_target(char *scratch_page, OffsetNumber queried_offnum,
							  TransactionId candidate_xid)
{
	Page page = (Page)scratch_page;
	ItemId iid;
	HeapTupleHeader htup;
	TransactionId xmin;

	if (scratch_page == NULL || queried_offnum < FirstOffsetNumber
		|| queried_offnum > PageGetMaxOffsetNumber(page))
		return;
	iid = PageGetItemId(page, queried_offnum);
	if (!ItemIdIsNormal(iid))
		return;
	htup = (HeapTupleHeader)PageGetItem(page, iid);
	xmin = HeapTupleHeaderGetRawXmin(htup);
	if (TransactionIdIsValid(xmin) && xmin == candidate_xid)
		ItemIdSetUnused(iid); /* created after read_scn -> not present at read_scn */
}

/*
 * cluster_cr_tuple_apply_record -- apply ONE undo record to the queried offnum
 * per TargetTupleCrState (D2, §3.3).  Mirrors cr_walk_chain's per-record switch
 * (cluster_cr.c) but (a) target-filters INSERT/UPDATE/DELETE to the queried
 * offnum and (b) applies EVERY ITL record's slot restoration (the verdict reads
 * the scratch ITL slot on the recycled-deleter path, D0b §B).  Reuses the
 * authoritative cluster_cr_apply.c primitives + their walk-time identity guard
 * (INV-T4) so a successful apply is bit-identical to full-block.
 *
 * Precondition (driver-enforced): the record is post-read_scn AND targets THIS
 * block.  Any apply failure (foreign identity / no space / malformed payload /
 * unknown type) returns CR_TUPLE_APPLY_FALLBACK so the driver defers to the
 * authoritative full-block path (INV-T2/T3) rather than guessing or fail-closing
 * from the narrower target-only view.
 */
ClusterCRTupleApplyResult
cluster_cr_tuple_apply_record(char *scratch_page, OffsetNumber queried_offnum,
							  const char *record_buf, size_t len, ClusterCRTupleOutcome *reason)
{
	const UndoRecordHeader *hdr = (const UndoRecordHeader *)record_buf;

	if (record_buf == NULL || len < sizeof(UndoRecordHeader)) {
		if (reason != NULL)
			*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return CR_TUPLE_APPLY_FALLBACK;
	}

	switch (hdr->record_type) {
	case UNDO_RECORD_INSERT: {
		const UndoInsertPayload *p
			= (const UndoInsertPayload *)(record_buf + sizeof(UndoRecordHeader));

		if (hdr->target_offset != queried_offnum)
			return CR_TUPLE_APPLY_OK; /* non-target: verdict never reads it */
		if (len < sizeof(UndoRecordHeader) + sizeof(UndoInsertPayload)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		if (!cluster_cr_apply_insert_inverse(scratch_page, hdr, p)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		return CR_TUPLE_APPLY_OK;
	}
	case UNDO_RECORD_UPDATE: {
		const UndoUpdatePayload *p
			= (const UndoUpdatePayload *)(record_buf + sizeof(UndoRecordHeader));
		const char *old_bytes = (const char *)p + p->old_tuple_offset;

		if (hdr->target_offset != queried_offnum)
			return CR_TUPLE_APPLY_OK;
		if (sizeof(UndoRecordHeader) + (size_t)p->old_tuple_offset + p->old_tuple_length > len) {
			if (reason != NULL)
				*reason
					= CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN; /* malformed -> full-block authority */
			return CR_TUPLE_APPLY_FALLBACK;
		}
		/* walk-time lineage identity guard lives inside cr_restore_full_image
			 * (occ.xmin == img.xmin || occ.xmin == img.xmax, INV-T4); a foreign /
			 * different-length occupant returns false -> fallback (NOT fail-close,
			 * P2#2: the target-only view is narrower than full-block). */
		if (!cluster_cr_apply_update_inverse(scratch_page, hdr, p, old_bytes,
											 p->old_tuple_length)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_IDENTITY;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		return CR_TUPLE_APPLY_OK;
	}
	case UNDO_RECORD_DELETE: {
		const UndoDeletePayload *p
			= (const UndoDeletePayload *)(record_buf + sizeof(UndoRecordHeader));
		const char *full_bytes = (const char *)p + p->full_tuple_offset;

		if (hdr->target_offset != queried_offnum)
			return CR_TUPLE_APPLY_OK;
		if (sizeof(UndoRecordHeader) + (size_t)p->full_tuple_offset + p->full_tuple_length > len) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		if (!cluster_cr_apply_delete_inverse(scratch_page, hdr, p, full_bytes,
											 p->full_tuple_length)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_IDENTITY;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		return CR_TUPLE_APPLY_OK;
	}
	case UNDO_RECORD_ITL: {
		const UndoItlPayload *p = (const UndoItlPayload *)(record_buf + sizeof(UndoRecordHeader));

		/* Apply EVERY ITL record (not just target-offnum ones): the slot
			 * restoration is read by the recycled-deleter verdict path, so the
			 * scratch ITL slot array must match full-block (D0b §B / v0.4 P0). */
		if (len < sizeof(UndoRecordHeader) + sizeof(UndoItlPayload)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		if (!cluster_cr_apply_itl_inverse(scratch_page, hdr, p)) {
			if (reason != NULL)
				*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			return CR_TUPLE_APPLY_FALLBACK;
		}
		return CR_TUPLE_APPLY_OK;
	}
	default:
		/* unknown type: full-block would ereport data_corrupted; fall back so
			 * the authoritative re-walk produces that SQLSTATE (never swallowed). */
		if (reason != NULL)
			*reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return CR_TUPLE_APPLY_FALLBACK;
	}
}
