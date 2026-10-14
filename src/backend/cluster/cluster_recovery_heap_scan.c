/*-------------------------------------------------------------------------
 * cluster_recovery_heap_scan.c
 *   Read-only heap-page candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup_details.h"
#include "cluster/cluster_recovery_heap_scan.h"

#ifdef USE_PGRAC_CLUSTER

struct ClusterRecoveryHeapScan {
	union {
		uint64 alignment;
		char bytes[BLCKSZ];
	} page;
	uint16 item_count;
	uint16 item_next;
	uint8 itl_next;
};

static ClusterRecoveryHeapResult
recovery_heap_layout(ClusterRecoveryHeapScan *scan)
{
	Page page = scan->page.bytes;
	PageHeader header = (PageHeader)page;
	uint8 occupied[BLCKSZ / 8] = { 0 };

	if (PageGetPageSize(page) != BLCKSZ || PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION)
		return CLUSTER_RECOVERY_HEAP_UNSUPPORTED;
	if ((header->pd_flags & ~PD_VALID_FLAG_BITS) != 0 || header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special
		|| header->pd_special > BLCKSZ || header->pd_special != MAXALIGN(header->pd_special)
		|| (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0)
		return CLUSTER_RECOVERY_HEAP_CORRUPT;
	if (!PageHasItl(page))
		return CLUSTER_RECOVERY_HEAP_UNSUPPORTED;
	if ((header->pd_flags & (PD_UNDO_SEG_HEADER | PD_SPACE_METADATA)) != 0
		|| BLCKSZ - header->pd_special != MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE))
		return CLUSTER_RECOVERY_HEAP_CORRUPT;
	for (unsigned i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		if (ClusterPageGetItlSlots(page)[i].flags > ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI)
			return CLUSTER_RECOVERY_HEAP_CORRUPT;
	scan->item_count = (header->pd_lower - SizeOfPageHeaderData) / sizeof(ItemIdData);
	for (unsigned i = 0; i < scan->item_count; i++) {
		ItemId item = &header->pd_linp[i];
		unsigned offset = ItemIdGetOffset(item);
		unsigned length = ItemIdGetLength(item);
		HeapTupleHeader tuple;
		unsigned header_length;

		if (ItemIdIsRedirected(item)) {
			if (length != 0 || offset == 0 || offset > scan->item_count
				|| !ItemIdIsNormal(&header->pd_linp[offset - 1]))
				return CLUSTER_RECOVERY_HEAP_CORRUPT;
			continue;
		}
		if (!ItemIdIsUsed(item)) {
			if (length != 0)
				return CLUSTER_RECOVERY_HEAP_CORRUPT;
			continue;
		}
		if (!ItemIdHasStorage(item)) {
			if (!ItemIdIsDead(item))
				return CLUSTER_RECOVERY_HEAP_CORRUPT;
			continue;
		}
		/* DEAD may still own storage. Its bytes cannot disappear from the
		 * candidate census just because a line-pointer hint changed. */
		if (length < SizeofHeapTupleHeader || offset < header->pd_upper
			|| offset != MAXALIGN(offset) || offset > header->pd_special
			|| length > header->pd_special - offset)
			return CLUSTER_RECOVERY_HEAP_CORRUPT;
		for (unsigned byte = offset; byte < offset + length; byte++) {
			uint8 bit = UINT8_C(1) << (byte % 8);

			if ((occupied[byte / 8] & bit) != 0)
				return CLUSTER_RECOVERY_HEAP_CORRUPT;
			occupied[byte / 8] |= bit;
		}
		tuple = (HeapTupleHeader)(page + offset);
		header_length = SizeofHeapTupleHeader;
		if ((tuple->t_infomask & HEAP_HASNULL) != 0)
			header_length += BITMAPLEN(HeapTupleHeaderGetNatts(tuple));
		if (tuple->t_hoff < header_length || tuple->t_hoff > length
			|| tuple->t_hoff != MAXALIGN(tuple->t_hoff)
			|| (tuple->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
				&& tuple->t_itl_slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT))
			return CLUSTER_RECOVERY_HEAP_CORRUPT;
	}
	return CLUSTER_RECOVERY_HEAP_MORE;
}

ClusterRecoveryHeapResult
cluster_recovery_heap_scan_begin(const void *page, size_t length, ClusterRecoveryHeapScan **out)
{
	ClusterRecoveryHeapScan *scan;
	ClusterRecoveryHeapResult result;

	if (out != NULL)
		*out = NULL;
	if (page == NULL || length != BLCKSZ || out == NULL)
		return CLUSTER_RECOVERY_HEAP_INVALID;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		return CLUSTER_RECOVERY_HEAP_CAPACITY;
	memcpy(scan->page.bytes, page, BLCKSZ);
	result = recovery_heap_layout(scan);
	if (result != CLUSTER_RECOVERY_HEAP_MORE) {
		free(scan);
		return result;
	}
	scan->item_next = FirstOffsetNumber;
	*out = scan;
	return CLUSTER_RECOVERY_HEAP_MORE;
}

ClusterRecoveryHeapResult
cluster_recovery_heap_scan_next(ClusterRecoveryHeapScan *scan, ClusterRecoveryHeapEntry *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (scan == NULL || out == NULL)
		return CLUSTER_RECOVERY_HEAP_INVALID;
	if (scan->itl_next < CLUSTER_ITL_INITRANS_DEFAULT) {
		out->kind = CLUSTER_RECOVERY_HEAP_ITL;
		out->index = scan->itl_next;
		out->itl = ClusterPageGetItlSlots(scan->page.bytes)[scan->itl_next++];
		return CLUSTER_RECOVERY_HEAP_ENTRY;
	}
	if (scan->item_next <= scan->item_count) {
		ItemId item = PageGetItemId(scan->page.bytes, scan->item_next);

		out->kind = CLUSTER_RECOVERY_HEAP_ITEM;
		out->index = scan->item_next++;
		out->item = *item;
		if (ItemIdHasStorage(item)) {
			out->tuple_bytes = scan->page.bytes + ItemIdGetOffset(item);
			out->tuple_length = ItemIdGetLength(item);
		}
		return CLUSTER_RECOVERY_HEAP_ENTRY;
	}
	return CLUSTER_RECOVERY_HEAP_END;
}

void
cluster_recovery_heap_scan_destroy(ClusterRecoveryHeapScan *scan)
{
	free(scan);
}

#endif
