/*-------------------------------------------------------------------------
 *
 * cluster_cr_history.h
 *   Pure retained-history inverse shared by CR and cold page enumeration.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/include/cluster/cluster_cr_history.h
 *
 * NOTES
 *   Pgrac-original scratch inverse; acquisition belongs to original callers.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_HISTORY_H
#define CLUSTER_CR_HISTORY_H

#include "access/htup_details.h"
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_savepoint_reader.h"
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

/* Page history records the shared ITL owner separately from the raw tuple
 * writer. A different prior owner or role consumed a wrap, even when that
 * prior xid happens to equal this operation's raw child xid. */
static inline const UndoItlHistoryEntry *
cluster_cr_history_entry(char result_page[BLCKSZ], const BufferTag *tag,
						 const ClusterTxLocator *locator, UBA current_uba,
						 const UndoRecordHeader *record, const UndoItlHistoryTrailer *history,
						 TransactionId owner_xid)
{
	const UndoItlHistoryEntry *entry = NULL;
	ClusterItlSlotData *slot;
	uint16 expected_wrap;
	unsigned i;
	bool same_role;

	if (record->target_locator.spcOid != tag->spcOid || record->target_locator.dbOid != tag->dbOid
		|| record->target_locator.relNumber != tag->relNumber || record->target_fork != tag->forkNum
		|| cluster_undo_record_uba_equal(record->prev_uba, current_uba))
		return NULL;
	for (i = 0; i < history->count; i++) {
		if (history->entries[i].block == tag->blockNum)
			entry = &history->entries[i];
	}
	if (entry == NULL || entry->slot_index != locator->itl_slot_index)
		return NULL;
	slot = &ClusterPageGetItlSlots((Page)result_page)[entry->slot_index];
	same_role = entry->after_kind == ITL_FLAG_ACTIVE
					? (slot->flags >= ITL_FLAG_ACTIVE && slot->flags <= ITL_FLAG_NEEDS_CLEANOUT)
					: ITL_FLAG_IS_LOCK_ONLY(slot->flags);
	expected_wrap = entry->prior.wrap;
	if (entry->prior.flags != ITL_FLAG_FREE
		&& !(entry->prior.flags == entry->after_kind && entry->prior.xid == owner_xid))
		expected_wrap++;
	if (!same_role || slot->xid != owner_xid || slot->wrap != expected_wrap
		|| slot->write_scn != entry->after_write_scn
		|| !cluster_undo_record_uba_equal(slot->undo_segment_head, current_uba))
		return NULL;
	return entry;
}

static inline bool
cluster_cr_history_update(char result_page[BLCKSZ], const BufferTag *tag,
						  const UndoRecordHeader *record, const char *body)
{
	UndoUpdatePayload update;
	const char *old_tuple;
	const HeapTupleHeaderData *old_header;
	bool target_page = tag->blockNum == record->target_block;

	memcpy(&update, body, sizeof(update));
	old_tuple = body + update.old_tuple_offset;
	old_header = (const HeapTupleHeaderData *)old_tuple;
	if (update.old_tuple_length < SizeofHeapTupleHeader
		|| old_header->t_hoff < SizeofHeapTupleHeader
		|| old_header->t_hoff > update.old_tuple_length || update.new_offset > MaxHeapTuplesPerPage)
		return false;
	if (update.new_block == tag->blockNum
		&& !cluster_cr_history_remove_creation(result_page, update.new_offset, record->xid, 0))
		return false;
	if (target_page)
		cluster_cr_history_make_restore_space((Page)result_page, record->target_offset,
											  update.old_tuple_length);
	return !target_page
		   || cluster_cr_apply_update_inverse(result_page, record, &update, old_tuple,
											  update.old_tuple_length);
}

static inline bool
cluster_cr_history_delete(char result_page[BLCKSZ], const UndoRecordHeader *record,
						  const char *body)
{
	UndoDeletePayload deleted;
	const char *old_tuple;
	const HeapTupleHeaderData *old_header;

	memcpy(&deleted, body, sizeof(deleted));
	old_tuple = body + deleted.full_tuple_offset;
	old_header = (const HeapTupleHeaderData *)old_tuple;
	if (deleted.full_tuple_length < SizeofHeapTupleHeader
		|| old_header->t_hoff < SizeofHeapTupleHeader
		|| old_header->t_hoff > deleted.full_tuple_length)
		return false;
	cluster_cr_history_make_restore_space((Page)result_page, record->target_offset,
										  deleted.full_tuple_length);
	return cluster_cr_apply_delete_inverse(result_page, record, &deleted, old_tuple,
										   deleted.full_tuple_length);
}

static inline bool
cluster_cr_history_lock(char result_page[BLCKSZ], const UndoRecordHeader *record, const char *body)
{
	UndoItlPayload itl;
	ItemId item;

	memcpy(&itl, body, sizeof(itl));
	if (record->target_offset > MaxHeapTuplesPerPage)
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
	return cluster_cr_apply_itl_inverse(result_page, record, &itl);
}

/* Both decoders feed the same inverse. Only ITL ownership changes; payload
 * offsets, tuple raw xids and full prior slot restoration remain unchanged. */
static inline bool
cluster_cr_history_inverse_decoded(char result_page[BLCKSZ], const BufferTag *tag,
								   const ClusterTxLocator *locator, UBA current_uba,
								   const UndoRecordHeader *record, const char *body,
								   const UndoItlHistoryTrailer *history, TransactionId owner_xid)
{
	const UndoItlHistoryEntry *entry = cluster_cr_history_entry(
		result_page, tag, locator, current_uba, record, history, owner_xid);
	bool target_page = tag->blockNum == record->target_block;

	if (entry == NULL)
		return false;
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
	case UNDO_RECORD_UPDATE:
		if (!cluster_cr_history_update(result_page, tag, record, body))
			return false;
		break;
	case UNDO_RECORD_DELETE:
		if (!target_page || !cluster_cr_history_delete(result_page, record, body))
			return false;
		break;
	case UNDO_RECORD_ITL:
		if (!target_page || !cluster_cr_history_lock(result_page, record, body))
			return false;
		break;
	default:
		return false;
	}
	/* No reference to live buffers: this restores only the private image. */
	ClusterPageGetItlSlots((Page)result_page)[entry->slot_index] = entry->prior;
	return true;
}

/* The legacy entry continues rejecting operation trailers. */
static inline bool
cluster_cr_history_inverse(char result_page[BLCKSZ], const BufferTag *tag,
						   const ClusterTxLocator *locator, UBA current_uba,
						   const UndoRecordHeader *record, const char *body, size_t payload_length)
{
	UndoItlHistoryTrailer history;
	uint16 body_length;

	if (!cluster_undo_record_decode_payload(record, body, payload_length, &body_length, &history)
		|| (record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) == 0)
		return false;
	return cluster_cr_history_inverse_decoded(result_page, tag, locator, current_uba, record, body,
											  &history, record->xid);
}

/* An existing UPDATE/DELETE target must still be this raw child's mutation
 * of the captured tuple. Do this before removing a replacement or repacking.
 * Missing targets keep the original full-image restore; a MultiXact needs
 * its separately qualified member/recomposition path and is refused here. */
static inline bool
cluster_cr_history_operation_modify_identity(char result_page[BLCKSZ], const BufferTag *tag,
											 const UndoRecordHeader *record, const char *body)
{
	uint16 old_length;
	uint16 old_offset;
	HeapTupleHeaderData old_header;
	HeapTupleHeader tuple;
	Page page = (Page)result_page;
	PageHeader header = (PageHeader)page;
	ItemId item;

	if (tag->blockNum != record->target_block
		|| (record->record_type != UNDO_RECORD_UPDATE && record->record_type != UNDO_RECORD_DELETE))
		return true;
	if (record->record_type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update;

		memcpy(&update, body, sizeof(update));
		old_length = update.old_tuple_length;
		old_offset = update.old_tuple_offset;
	} else {
		UndoDeletePayload deleted;

		memcpy(&deleted, body, sizeof(deleted));
		old_length = deleted.full_tuple_length;
		old_offset = deleted.full_tuple_offset;
	}
	if (old_length < SizeofHeapTupleHeader || record->target_offset > MaxHeapTuplesPerPage)
		return false;
	memcpy(&old_header, body + old_offset, SizeofHeapTupleHeader);
	if (old_header.t_hoff < SizeofHeapTupleHeader || old_header.t_hoff > old_length)
		return false;
	if (record->target_offset > PageGetMaxOffsetNumber(page))
		return true;
	item = PageGetItemId(page, record->target_offset);
	if (!ItemIdIsNormal(item))
		return true;
	if (ItemIdGetLength(item) < SizeofHeapTupleHeader || ItemIdGetOffset(item) < header->pd_upper
		|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special)
		return false;
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	return tuple->t_hoff >= SizeofHeapTupleHeader && tuple->t_hoff <= ItemIdGetLength(item)
		   && (tuple->t_infomask & (HEAP_XMAX_IS_MULTI | HEAP_XMAX_INVALID)) == 0
		   && !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)
		   && HeapTupleHeaderGetRawXmax(tuple) == record->xid
		   && HeapTupleHeaderGetRawXmin(tuple) == HeapTupleHeaderGetRawXmin(&old_header);
}

/* Caller decides whether to inverse after original top/current qualification.
 * A known LIVE or DISCARDED state alone never grants transaction visibility.
 * The existing scratch-discard-on-refusal rule also applies to this entry. */
static inline bool
cluster_cr_history_inverse_operation_identity(char result_page[BLCKSZ], const BufferTag *tag,
											  const ClusterTxLocator *locator, UBA current_uba,
											  const UndoRecordHeader *record, const char *body,
											  size_t payload_length,
											  const ClusterSavepointReaderIdentity *expected)
{
	ClusterSavepointReaderRecord decoded;

	if (!cluster_savepoint_reader_decode_identity(expected, locator, current_uba, record, body,
												  payload_length, &decoded)
		|| !cluster_cr_history_operation_modify_identity(result_page, tag, record, body))
		return false;
	return cluster_cr_history_inverse_decoded(result_page, tag, locator, current_uba, record, body,
											  &decoded.history, expected->canonical.xid);
}

static inline bool
cluster_cr_history_inverse_operation(char result_page[BLCKSZ], const BufferTag *tag,
									 const ClusterTxLocator *locator, UBA current_uba,
									 const UndoRecordHeader *record, const char *body,
									 size_t payload_length,
									 const ClusterSavepointOperation *expected)
{
	ClusterSavepointReaderIdentity identity;

	return cluster_savepoint_reader_identity_from_operation(expected, locator, &identity)
		   && cluster_cr_history_inverse_operation_identity(
			   result_page, tag, locator, current_uba, record, body, payload_length, &identity);
}

#endif
#endif
