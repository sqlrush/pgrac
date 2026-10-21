/*-------------------------------------------------------------------------
 * cluster_heap_freeze.c
 *    Bounded physical input capture and validation for shared heap freezing.
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

ClusterHeapFreezePageMatchV1
cluster_heap_freeze_validate_page_v1(const ClusterHeapFreezePageInputV1 *input,
									 const ClusterSpaceIdentity *identity, BlockNumber block,
									 Page page)
{
	OffsetNumber maxoff;
	uint16 normal;
	uint16 multi;

	if (input == NULL || identity == NULL || page == NULL
		|| input->identity.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| (identity->state != CLUSTER_SPACE_IDENTITY_LIVE
			&& identity->state != CLUSTER_SPACE_IDENTITY_TOMBSTONED)
		|| input->block == InvalidBlockNumber || block == InvalidBlockNumber
		|| !freeze_page_layout_valid((Page)input->image.data, &maxoff, &normal, &multi)
		|| input->max_offset != maxoff || input->normal_tuples != normal
		|| input->multi_tuples != multi
		|| !freeze_page_layout_valid(page, &maxoff, &normal, &multi))
		return CLUSTER_HEAP_FREEZE_PAGE_INVALID;

	/* SPACE is an in-memory value with padding, not a serialized witness. */
	if (input->block != block
		|| input->identity.key.system_identifier != identity->key.system_identifier
		|| input->identity.key.database_incarnation != identity->key.database_incarnation
		|| memcmp(input->identity.key.storage_uuid, identity->key.storage_uuid,
				  sizeof(identity->key.storage_uuid))
			   != 0
		|| !RelFileLocatorEquals(input->identity.key.locator, identity->key.locator)
		|| memcmp(input->identity.incarnation, identity->incarnation, sizeof(identity->incarnation))
			   != 0
		|| input->identity.sequence != identity->sequence
		|| input->identity.operation != identity->operation
		|| input->identity.state != identity->state || memcmp(input->image.data, page, BLCKSZ) != 0)
		return CLUSTER_HEAP_FREEZE_PAGE_CHANGED;

	return CLUSTER_HEAP_FREEZE_PAGE_MATCHED;
}

/* Small private plan: no partial public output if a runtime bound rejects. */
typedef struct FreezeCarrierPlan
{
	uint8 request_count;
	uint8 group_count;
	uint8 request_index_by_slot[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterHeapFreezeCarrierRequestV1 requests[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterHeapFreezeDataGroupV1 groups[CLUSTER_ITL_INITRANS_DEFAULT];
} FreezeCarrierPlan;

static bool
freeze_prepare_carriers(Page page, OffsetNumber maxoff, ClusterItlSlotData *slots,
						FreezeCarrierPlan *plan)
{
	bool scanned[CLUSTER_ITL_INITRANS_DEFAULT] = { false };

	memset(plan, 0, sizeof(*plan));
	memset(plan->request_index_by_slot, CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST,
		   sizeof(plan->request_index_by_slot));
	if (slots == NULL)
		return true;

	for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum)) {
		ItemId item = PageGetItemId(page, offnum);
		HeapTupleHeader tuple;
		uint8 slot_index;
		uint8 request_index;
		uint8 group_index;
		ClusterTxLocator locator;
		ClusterHeapFreezeCarrierRequestV1 *request;
		ClusterHeapFreezeDataGroupV1 *group;
		uint32 segment_id;
		uint32 block_no;
		uint16 tt_slot_offset;
		uint16 row_offset;

		if (!ItemIdIsNormal(item))
			continue;
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		slot_index = tuple->t_itl_slot_idx;
		if (slot_index >= lengthof(scanned) || scanned[slot_index])
			continue;
		scanned[slot_index] = true;
		if (!cluster_tx_locator_from_itl_terminal_census(page, slot_index, &locator, NULL))
			continue;

		request_index = plan->request_count;
		if (request_index >= lengthof(plan->requests))
			return false;
		request = &plan->requests[request_index];
		request->locator = locator;
		request->carrier = slots[slot_index];
		request->data_group_index = CLUSTER_HEAP_FREEZE_NO_DATA_GROUP;
		plan->request_index_by_slot[slot_index] = request_index;
		plan->request_count++;

		/*
		 * Keep legacy locator/witness semantics, but only plan DATA reads for
		 * the existing record-address domain. Neither wrap nor TT authority
		 * is inferred here; the page wrap remains in the physical witness.
		 */
		if (!uba_decode_record(locator.uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
			continue;
		for (group_index = 0; group_index < plan->group_count; group_index++)
			if (plan->groups[group_index].segment_id == segment_id
				&& plan->groups[group_index].block_no == block_no)
				break;
		if (group_index == plan->group_count) {
			if (group_index >= lengthof(plan->groups))
				return false;
			plan->groups[group_index].segment_id = segment_id;
			plan->groups[group_index].block_no = block_no;
			plan->group_count++;
		}
		group = &plan->groups[group_index];
		if (group->request_count >= lengthof(group->request_indexes))
			return false;
		group->request_indexes[group->request_count++] = request_index;
		request->data_group_index = group_index;
	}
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
	FreezeCarrierPlan plan;

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
	if (!freeze_prepare_carriers(page, maxoff, slots, &plan))
		return false;
	memset(out, 0, sizeof(*out));
	out->identity = input->identity;
	out->block = input->block;
	out->page_lsn = PageGetLSN(page);
	out->page_version = ((PageHeader)page)->pd_block_scn;
	out->tuple_count = normal;
	out->multi_tuples = multi;
	out->carrier_request_count = plan.request_count;
	out->data_group_count = plan.group_count;
	memcpy(out->carrier_requests, plan.requests, sizeof(plan.requests));
	memcpy(out->data_groups, plan.groups, sizeof(plan.groups));
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
		/*
		 * KEEP until canonical proof and conditional apply exist. Native
		 * FROZEN encoding exempts only xmin; it says nothing about xmax.
		 * Raw special XIDs, commit hints and carrier equality are not proof.
		 * All MX fields remain opaque, including stale INVALID/MULTI bits.
		 */
		if ((row->infomask & HEAP_XMIN_FROZEN) != HEAP_XMIN_FROZEN)
			out->unproven_xmin_tuples++;
		if (!(row->infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI)))
			out->unproven_xmax_tuples++;
		row->header_length = tuple->t_hoff;
		row->itl_slot_index = tuple->t_itl_slot_idx;
		row->carrier_request_index = CLUSTER_HEAP_FREEZE_NO_CARRIER_REQUEST;
		/* No raw-XID search: this carrier need not belong to the creator. */
		if (slots != NULL && row->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT) {
			uint8 slot_index = row->itl_slot_index;

			row->has_carrier = true;
			row->carrier = slots[slot_index];
			/* Equal raw XIDs in different slots remain separate requests. */
			row->carrier_request_index = plan.request_index_by_slot[slot_index];
		}
	}
	return true;
}
