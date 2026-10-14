/*-------------------------------------------------------------------------
 * cluster_cr_history.h
 *   Pure retained-history inverse shared by CR and cold page enumeration.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_HISTORY_H
#define CLUSTER_CR_HISTORY_H

#include "access/htup_details.h"
#include "cluster/cluster_cr_apply.h"
#include "storage/buf_internals.h"

#ifdef USE_PGRAC_CLUSTER
/* Callers validate an immutable page layout and exact record identity first.
 * This helper changes only their private scratch copy. A refusal may leave
 * that scratch partly modified; it must never be published after refusal. */
/* A page-history inverse removes one exact creation, not every tuple with the
 * same xid (earlier statements may already belong to the reader's snapshot). */
static inline bool
cluster_cr_history_remove_creation(char *page_bytes, OffsetNumber offset, TransactionId xid,
								   uint16 expected_length)
{
	Page page = (Page)page_bytes;
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;

	if (offset < FirstOffsetNumber || offset > MaxHeapTuplesPerPage)
		return false;
	if (offset > PageGetMaxOffsetNumber(page))
		return true;
	item = PageGetItemId(page, offset);
	if (!ItemIdIsNormal(item))
		return true;
	if (ItemIdGetLength(item) < SizeofHeapTupleHeader || ItemIdGetOffset(item) < header->pd_upper
		|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special
		|| (expected_length != 0 && ItemIdGetLength(item) != expected_length))
		return false;
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > ItemIdGetLength(item)
		|| HeapTupleHeaderGetRawXmin(tuple) != xid)
		return false;
	ItemIdSetUnused(item);
	return true;
}

/* Inverses leave removed tuple storage behind until it is actually needed.
 * Repack only the private image, and only for a missing target that cannot be
 * re-added yet. Never prune by xid or change live heap/pruning horizons. */
static inline void
cluster_cr_history_make_restore_space(Page page, OffsetNumber offset, uint16 length)
{
	PageHeader header = (PageHeader)page;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	Size required;

	if (offset <= maxoff && ItemIdIsNormal(PageGetItemId(page, offset)))
		return;
	required = MAXALIGN(length);
	if (offset > maxoff)
		required += (offset - maxoff) * sizeof(ItemIdData);
	if (required > (Size)(header->pd_upper - header->pd_lower))
		PageRepairFragmentation(page);
}

static inline bool
cluster_cr_history_inverse(char result_page[BLCKSZ], const BufferTag *tag,
						   const ClusterTxLocator *locator, UBA current_uba,
						   const UndoRecordHeader *record, const char *body, size_t payload_length)
{
	UndoItlHistoryTrailer history;
	const UndoItlHistoryEntry *entry = NULL;
	ClusterItlSlotData *slot;
	uint16 body_length;
	uint16 expected_wrap;
	unsigned i;
	bool target_page;
	bool same_role;

	if (!cluster_undo_record_decode_payload(record, body, payload_length, &body_length, &history)
		|| (record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) == 0
		|| record->target_locator.spcOid != tag->spcOid
		|| record->target_locator.dbOid != tag->dbOid
		|| record->target_locator.relNumber != tag->relNumber || record->target_fork != tag->forkNum
		|| cluster_undo_record_uba_equal(record->prev_uba, current_uba))
		return false;
	for (i = 0; i < history.count; i++) {
		if (history.entries[i].block == tag->blockNum)
			entry = &history.entries[i];
	}
	if (entry == NULL || entry->slot_index != locator->itl_slot_index)
		return false;
	slot = &ClusterPageGetItlSlots((Page)result_page)[entry->slot_index];
	same_role = entry->after_kind == ITL_FLAG_ACTIVE
					? (slot->flags >= ITL_FLAG_ACTIVE && slot->flags <= ITL_FLAG_NEEDS_CLEANOUT)
					: ITL_FLAG_IS_LOCK_ONLY(slot->flags);
	expected_wrap = entry->prior.wrap;
	if (entry->prior.flags != ITL_FLAG_FREE
		&& !(entry->prior.flags == entry->after_kind && entry->prior.xid == record->xid))
		expected_wrap++;
	if (!same_role || slot->xid != record->xid || slot->wrap != expected_wrap
		|| slot->write_scn != entry->after_write_scn
		|| !cluster_undo_record_uba_equal(slot->undo_segment_head, current_uba))
		return false;
	target_page = tag->blockNum == record->target_block;
	switch (record->record_type) {
	case UNDO_RECORD_INSERT: {
		UndoInsertPayload insert;

		memcpy(&insert, body, sizeof(insert));
		if (!target_page
			|| !cluster_cr_history_remove_creation(result_page, record->target_offset, record->xid,
												   insert.inserted_tuple_len))
			return false;
		break;
	}
	case UNDO_RECORD_UPDATE: {
		UndoUpdatePayload update;
		const char *old_tuple;
		const HeapTupleHeaderData *old_header;

		memcpy(&update, body, sizeof(update));
		old_tuple = body + update.old_tuple_offset;
		old_header = (const HeapTupleHeaderData *)old_tuple;
		if (update.old_tuple_length < SizeofHeapTupleHeader
			|| old_header->t_hoff < SizeofHeapTupleHeader
			|| old_header->t_hoff > update.old_tuple_length
			|| update.new_offset > MaxHeapTuplesPerPage)
			return false;
		if (update.new_block == tag->blockNum
			&& !cluster_cr_history_remove_creation(result_page, update.new_offset, record->xid, 0))
			return false;
		if (target_page)
			cluster_cr_history_make_restore_space((Page)result_page, record->target_offset,
												  update.old_tuple_length);
		if (target_page
			&& !cluster_cr_apply_update_inverse(result_page, record, &update, old_tuple,
												update.old_tuple_length))
			return false;
		break;
	}
	case UNDO_RECORD_DELETE: {
		UndoDeletePayload deleted;
		const char *old_tuple;
		const HeapTupleHeaderData *old_header;

		memcpy(&deleted, body, sizeof(deleted));
		old_tuple = body + deleted.full_tuple_offset;
		old_header = (const HeapTupleHeaderData *)old_tuple;
		if (!target_page || deleted.full_tuple_length < SizeofHeapTupleHeader
			|| old_header->t_hoff < SizeofHeapTupleHeader
			|| old_header->t_hoff > deleted.full_tuple_length)
			return false;
		cluster_cr_history_make_restore_space((Page)result_page, record->target_offset,
											  deleted.full_tuple_length);
		if (!cluster_cr_apply_delete_inverse(result_page, record, &deleted, old_tuple,
											 deleted.full_tuple_length))
			return false;
		break;
	}
	case UNDO_RECORD_ITL: {
		UndoItlPayload itl;
		ItemId item;

		memcpy(&itl, body, sizeof(itl));
		if (!target_page || record->target_offset > MaxHeapTuplesPerPage)
			return false;
		if (record->target_offset <= PageGetMaxOffsetNumber((Page)result_page)) {
			item = PageGetItemId((Page)result_page, record->target_offset);
			if (ItemIdIsNormal(item)) {
				HeapTupleHeader tuple;
				PageHeader page = (PageHeader)result_page;

				if (ItemIdGetLength(item) < SizeofHeapTupleHeader
					|| ItemIdGetOffset(item) < page->pd_upper
					|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > page->pd_special)
					return false;
				tuple = (HeapTupleHeader)PageGetItem((Page)result_page, item);
				if (HeapTupleHeaderGetRawXmax(tuple) != record->xid
					|| !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
					return false;
			}
		}
		if (!cluster_cr_apply_itl_inverse(result_page, record, &itl))
			return false;
		break;
	}
	default:
		return false;
	}
	/* No reference to live buffers: this restores only the private image. */
	*slot = entry->prior;
	return true;
}

#endif
#endif
