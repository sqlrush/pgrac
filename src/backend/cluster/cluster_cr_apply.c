/*-------------------------------------------------------------------------
 *
 * cluster_cr_apply.c
 *	  pgrac CR inverse-apply helpers (spec-3.9 D4).
 *
 *	  Four pure-logic helpers that inverse-apply one undo record onto a
 *	  backend-local CR scratch page.  Driven by the chain walker in
 *	  cluster_cr.c.  See cluster_cr_apply.h for the per-helper contract.
 *
 *	  All four mutate the scratch page in place and return false on any
 *	  inconsistency (caller ereports data_corrupted — spec-3.9 I-fail-4).
 *	  None of them touch shared buffers, WAL, or hint bits beyond the
 *	  scratch page (spec-3.9 I-lock-4).
 *
 *	  Heap update/delete in PostgreSQL keep the old tuple physically in
 *	  place (only the header xmax / ctid / infomask change); the new
 *	  version goes elsewhere.  So inverse-update / inverse-delete restore
 *	  the full pre-change tuple image captured in the undo record over the
 *	  tuple bytes at target_offset — a length-preserving overwrite.
 *	  Inverse-insert removes the inserted line pointer (LP_UNUSED).
 *	  Inverse-ITL restores the lock-only tuple-header fields + ITL slot
 *	  fields captured in UndoItlPayload.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_apply.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"

#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_itl_slot.h"


/*
 * Resolve and range-check the line pointer at target_offset on the scratch
 * page.  Returns the ItemId on success, NULL if the offset is out of range.
 */
static ItemId
cr_target_itemid(char *scratch_page, OffsetNumber off)
{
	Page page = (Page)scratch_page;

	if (off < FirstOffsetNumber || off > PageGetMaxOffsetNumber(page))
		return NULL;
	return PageGetItemId(page, off);
}


/* ============================================================
 * spec-3.10 D1: full-block candidate chain collection + ordering
 * ============================================================ */

/*
 * cluster_cr_collect_candidate_chains -- see header.  Captures the chain head +
 * write_scn of every ITL slot with a valid write_scn newer than read_scn,
 * BEFORE any inverse-apply mutates the page's ITL slots.  FREE / lock-only
 * slots carry InvalidScn write_scn and are skipped by the SCN_VALID guard.
 */
int
cluster_cr_collect_candidate_chains(const ClusterItlSlotData *slots, SCN read_scn,
									ClusterCRCandidateChain *out, int max_out)
{
	int n = 0;
	int i;

	if (slots == NULL || out == NULL)
		return 0;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT && n < max_out; i++) {
		SCN ws = slots[i].write_scn;

		if (!SCN_VALID(ws))
			continue;
		if (!SCN_VALID(read_scn))
			continue;
		if (scn_time_cmp(ws, read_scn) <= 0)
			continue; /* this slot's change is already in the snapshot */

		out[n].slot_idx = (uint8)i;
		out[n].xid = slots[i].xid;
		out[n].write_scn = ws;
		out[n].undo_segment_head = slots[i].undo_segment_head;
		n++;
	}
	return n;
}

/*
 * cluster_cr_chain_cmp_by_write_scn_desc -- qsort comparator, write_scn DESC
 * (newest transaction first; spec-3.10 Q10).  Deterministic slot_idx tie-break
 * (own-instance write_scns are distinct, but keep the order reproducible).
 */
int
cluster_cr_chain_cmp_by_write_scn_desc(const void *a, const void *b)
{
	const ClusterCRCandidateChain *ca = (const ClusterCRCandidateChain *)a;
	const ClusterCRCandidateChain *cb = (const ClusterCRCandidateChain *)b;
	int c;

	/* DESC: scn_time_cmp(cb, ca) is negative when ca is newer -> ca first. */
	c = scn_time_cmp(cb->write_scn, ca->write_scn);
	if (c != 0)
		return c;
	return (int)ca->slot_idx - (int)cb->slot_idx;
}

/*
 * cluster_cr_prune_post_snapshot_versions -- see header.  Mark LP_UNUSED every
 * normal-line-pointer tuple whose raw xmin matches one of the candidate
 * (post-read_scn) transaction xids.
 */
int
cluster_cr_prune_post_snapshot_versions(char *scratch_page, const ClusterCRCandidateChain *chains,
										int nchains)
{
	Page page = (Page)scratch_page;
	OffsetNumber off;
	OffsetNumber maxoff;
	int pruned = 0;

	if (scratch_page == NULL || (chains == NULL && nchains > 0))
		return 0;

	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++) {
		ItemId iid = PageGetItemId(page, off);
		HeapTupleHeader htup;
		TransactionId xmin;
		int j;

		if (!ItemIdIsNormal(iid))
			continue;
		htup = (HeapTupleHeader)PageGetItem(page, iid);
		xmin = HeapTupleHeaderGetRawXmin(htup);
		if (!TransactionIdIsValid(xmin))
			continue;
		for (j = 0; j < nchains; j++) {
			if (chains[j].xid == xmin) {
				ItemIdSetUnused(iid); /* created after read_scn */
				pruned++;
				break;
			}
		}
	}
	return pruned;
}


/*
 * cluster_cr_apply_insert_inverse -- undo an INSERT: remove the inserted
 *	tuple by marking its line pointer LP_UNUSED.  A seqscan / index fetch
 *	skips LP_UNUSED, so the row correctly disappears from the CR image.
 *	(We do not rewind pd_lower: a trailing LP_UNUSED slot is harmless on a
 *	transient read-only image and keeps subsequent offsets stable.)
 */
bool
cluster_cr_apply_insert_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoInsertPayload *payload)
{
	ItemId itemid = cr_target_itemid(scratch_page, hdr->target_offset);

	if (itemid == NULL || !ItemIdIsNormal(itemid))
		return false;

	/* Optional sanity: the recorded inserted length matches the live tuple. */
	if (payload->inserted_tuple_len != 0 && ItemIdGetLength(itemid) != payload->inserted_tuple_len)
		return false;

	ItemIdSetUnused(itemid);
	return true;
}


/*
 * Shared body for inverse-update / inverse-delete: overwrite the tuple at
 * target_offset with the captured pre-change full image.  The overwrite is
 * length-preserving (PG keeps the old tuple physically in place across an
 * update/delete), so the captured length must equal the live item length.
 */
static bool
cr_restore_full_image(char *scratch_page, OffsetNumber off, const char *image_bytes,
					  uint16 image_length)
{
	ItemId itemid = cr_target_itemid(scratch_page, off);
	char *item;

	if (itemid == NULL || !ItemIdIsNormal(itemid))
		return false;
	if (image_bytes == NULL || image_length == 0)
		return false;
	if (ItemIdGetLength(itemid) != image_length)
		return false; /* in-place overwrite must be length-preserving */

	item = (char *)PageGetItem((Page)scratch_page, itemid);
	memcpy(item, image_bytes, image_length);
	return true;
}


/*
 * cluster_cr_apply_update_inverse -- undo an UPDATE: restore the old tuple
 *	image (which had xmax invalid + original ctid) over the tuple bytes at
 *	target_offset.
 */
bool
cluster_cr_apply_update_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoUpdatePayload *payload, const char *old_tuple_bytes,
								uint16 old_tuple_length)
{
	(void)payload; /* new_block/new_offset are not needed for the in-place restore */
	return cr_restore_full_image(scratch_page, hdr->target_offset, old_tuple_bytes,
								 old_tuple_length);
}


/*
 * cluster_cr_apply_delete_inverse -- undo a DELETE: restore the full
 *	pre-delete tuple image over the tuple bytes at target_offset (clearing
 *	the xmax / HEAP_XMAX_* the delete set, since the captured image predates
 *	the delete).
 */
bool
cluster_cr_apply_delete_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoDeletePayload *payload, const char *full_tuple_bytes,
								uint16 full_tuple_length)
{
	(void)payload;
	return cr_restore_full_image(scratch_page, hdr->target_offset, full_tuple_bytes,
								 full_tuple_length);
}


/*
 * cluster_cr_apply_itl_inverse -- undo a lock-only ITL transition: restore
 *	the tuple-header fields the lock set (xmax / infomask / infomask2) and
 *	the affected ITL slot's fields (flags / commit_scn / undo_segment_head)
 *	from the UndoItlPayload captured prior state.
 */
bool
cluster_cr_apply_itl_inverse(char *scratch_page, const UndoRecordHeader *hdr,
							 const UndoItlPayload *payload)
{
	Page page = (Page)scratch_page;
	ItemId itemid;
	HeapTupleHeader htup;
	ClusterItlSlotData *slots;

	if (payload == NULL)
		return false;
	if (payload->itl_slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	if (!PageHasItl(page))
		return false;

	/* Restore the tuple header the lock-only change touched. */
	itemid = cr_target_itemid(scratch_page, hdr->target_offset);
	if (itemid == NULL || !ItemIdIsNormal(itemid))
		return false;
	htup = (HeapTupleHeader)PageGetItem(page, itemid);
	HeapTupleHeaderSetXmax(htup, payload->prev_xmax);
	htup->t_infomask = payload->prev_infomask;
	htup->t_infomask2 = payload->prev_infomask2;

	/* Restore the ITL slot's captured prior state. */
	slots = ClusterPageGetItlSlots(page);
	slots[payload->itl_slot_idx].flags = payload->prev_flags;
	slots[payload->itl_slot_idx].commit_scn = payload->prev_commit_scn;
	slots[payload->itl_slot_idx].undo_segment_head = payload->prev_undo_segment_head;
	return true;
}
