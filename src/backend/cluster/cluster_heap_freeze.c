/*-------------------------------------------------------------------------
 * cluster_heap_freeze.c
 *    Bounded physical input capture for shared heap freeze preparation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "cluster/cluster_heap_freeze.h"

/* Shared validation keeps both public outputs untouched on any failure. */
static bool
freeze_page_layout_valid(Page page, OffsetNumber *max_offset,
						 uint16 *normal_tuples, uint16 *multi_tuples)
{
	PageHeader header;
	OffsetNumber maxoff;
	uint16 normal = 0;
	uint16 multi = 0;

	header = (PageHeader) page;
	if (PageGetPageSize(page) != BLCKSZ
		|| PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper
		|| header->pd_upper > header->pd_special
		|| header->pd_special > BLCKSZ
		|| (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0
		|| header->pd_special != MAXALIGN(header->pd_special))
		return false;

	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff > MaxHeapTuplesPerPage
		|| (PageHasItl(page) && PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE))
		return false;

	/* Validate every dereference before publishing any part of the output. */
	for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId item = PageGetItemId(page, offnum);
		unsigned offset;
		unsigned length;
		HeapTupleHeader tuple;

		if (!ItemIdIsNormal(item))
			continue;
		offset = ItemIdGetOffset(item);
		length = ItemIdGetLength(item);
		if (offset < header->pd_upper || offset != MAXALIGN(offset)
			|| length < SizeofHeapTupleHeader
			|| offset + length > header->pd_special)
			return false;
		tuple = (HeapTupleHeader) (page + offset);
		if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > length)
			return false;
		normal++;
		if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			multi++;
	}

	*max_offset = maxoff;
	*normal_tuples = normal;
	*multi_tuples = multi;
	return true;
}

bool
cluster_heap_freeze_capture_page_v1(const ClusterSpaceIdentity *identity,
								  BlockNumber block, Page page,
								  ClusterHeapFreezePageInputV1 *out)
{
	OffsetNumber maxoff;
	uint16 normal;
	uint16 multi;

	if (identity == NULL || identity->state != CLUSTER_SPACE_IDENTITY_LIVE
		|| block == InvalidBlockNumber || page == NULL || out == NULL
		|| !freeze_page_layout_valid(page, &maxoff, &normal, &multi))
		return false;

	/* The image preserves physical identity; counts never establish proof. */
	out->identity = *identity;
	out->block = block;
	out->max_offset = maxoff;
	out->normal_tuples = normal;
	out->multi_tuples = multi;
	memcpy(out->image.data, page, BLCKSZ);
	return true;
}

bool
cluster_heap_freeze_prepare_page_v1(const ClusterHeapFreezePageInputV1 *input,
								  ClusterHeapFreezePageBatchV1 *out)
{
	Page page;
	OffsetNumber maxoff;
	uint16 normal;
	uint16 multi;
	uint16 index = 0;
	ClusterItlSlotData *slots = NULL;
	uint8 request_index_by_slot[CLUSTER_ITL_INITRANS_DEFAULT];
	bool request_scanned[CLUSTER_ITL_INITRANS_DEFAULT] = { false };

	if (input == NULL || out == NULL || input->identity.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| input->block == InvalidBlockNumber)
		return false;
	page = (Page)input->image.data;
	if (!freeze_page_layout_valid(page, &maxoff, &normal, &multi) || input->max_offset != maxoff
		|| input->normal_tuples != normal || input->multi_tuples != multi)
		return false;

	/* All remaining accesses are bounded by the validated immutable image. */
	if (PageHasItl(page))
		slots = ClusterPageGetItlSlots(page);
	memset(request_index_by_slot, CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST,
		   sizeof(request_index_by_slot));
	memset(out, 0, sizeof(*out));
	out->identity = input->identity;
	out->block = input->block;
	out->page_lsn = PageGetLSN(page);
	out->page_version = ((PageHeader)page)->pd_block_scn;
	out->tuple_count = normal;
	out->multi_tuples = multi;
	for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum)) {
		ItemId item = PageGetItemId(page, offnum);
		HeapTupleHeader tuple;
		ClusterHeapFreezeTupleInputV1 *row;

		if (!ItemIdIsNormal(item))
			continue;
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		row = &out->tuples[index++];
		row->offset = offnum;
		row->tuple_offset = ItemIdGetOffset(item);
		row->tuple_length = ItemIdGetLength(item);
		row->raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
		row->raw_xmax = HeapTupleHeaderGetRawXmax(tuple);
		row->ctid = tuple->t_ctid;
		row->infomask = tuple->t_infomask;
		row->infomask2 = tuple->t_infomask2;
		row->header_length = tuple->t_hoff;
		row->itl_slot_index = tuple->t_itl_slot_idx;
		row->carrier_request_index = CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST;
		/* No raw-XID search: this carrier need not belong to the creator. */
		if (slots != NULL && row->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT) {
			uint8 slot_index = row->itl_slot_index;

			row->has_carrier = true;
			row->carrier = slots[slot_index];
			if (!request_scanned[slot_index]) {
				ClusterTxLocator locator;

				request_scanned[slot_index] = true;
				/*
				 * Only prepare an origin request. The page wrap stays in the
				 * physical witness; exact undo-record identity supplies the
				 * canonical wrap later, outside all blocking locks/pins.
				 */
				if (cluster_tx_locator_from_itl_terminal_census(page, slot_index, &locator, NULL)) {
					ClusterHeapFreezeCarrierRequestV1 *request;
					uint8 request_index = out->carrier_request_count;

					Assert(request_index < lengthof(out->carrier_requests));
					request = &out->carrier_requests[request_index];
					request->locator = locator;
					request->carrier = slots[slot_index];
					request_index_by_slot[slot_index] = request_index;
					out->carrier_request_count++;
				}
			}
			/* Equal raw XIDs in different slots remain separate requests. */
			row->carrier_request_index = request_index_by_slot[slot_index];
		}
	}
	return true;
}
