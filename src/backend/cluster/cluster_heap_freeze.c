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

/*
 * The proof consumer compares SPACE identities field by field.  The in-memory
 * value contains padding and is not a serialized witness, so memcmp would
 * make a byte that is outside the identity contract part of admission.
 */
static bool
freeze_space_identity_equal(const ClusterSpaceIdentity *left,
							const ClusterSpaceIdentity *right)
{
	if (left == NULL || right == NULL)
		return false;
	return left->key.system_identifier == right->key.system_identifier
		&& left->key.database_incarnation == right->key.database_incarnation
		&& memcmp(left->key.storage_uuid, right->key.storage_uuid,
				  sizeof(left->key.storage_uuid)) == 0
		&& RelFileLocatorEquals(left->key.locator, right->key.locator)
		&& memcmp(left->incarnation, right->incarnation,
				  sizeof(left->incarnation)) == 0
		&& left->sequence == right->sequence
		&& left->operation == right->operation
		&& left->state == right->state;
}

static bool
freeze_final_fence_shape_valid(const ClusterHeapFreezeFinalFenceV1 *fence)
{
	return fence != NULL
		&& fence->identity.state == CLUSTER_SPACE_IDENTITY_LIVE
		&& fence->block != InvalidBlockNumber
		&& !XLogRecPtrIsInvalid(fence->page_lsn)
		&& SCN_VALID(fence->page_version)
		&& fence->holder_sequence != 0
		&& fence->master_sequence != 0
		&& fence->admission_generation != 0;
}

static bool
freeze_final_fence_equal(const ClusterHeapFreezeFinalFenceV1 *left,
						 const ClusterHeapFreezeFinalFenceV1 *right)
{
	return freeze_space_identity_equal(&left->identity, &right->identity)
		&& left->block == right->block
		&& left->page_lsn == right->page_lsn
		&& left->page_version == right->page_version
		&& left->holder_sequence == right->holder_sequence
		&& left->master_sequence == right->master_sequence
		&& left->admission_generation == right->admission_generation;
}

static bool
freeze_canonical_locator_shape_valid(const ClusterTxLocator *request,
									 const ClusterTxResolution *resolution)
{
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;

	if (request == NULL || resolution == NULL
		|| !TransactionIdIsNormal(request->xid)
		|| request->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| (request->tt_wrap != TT_WRAP_INVALID && request->tt_wrap > TT_WRAP_MAX)
		|| !uba_decode_record(request->uba, &segment_id, &block_no,
							 &tt_slot_offset, &row_offset)
		|| segment_id == 0 || block_no == 0
		|| !cluster_tx_locator_reply_matches(request, &resolution->locator_echo)
		|| resolution->locator_echo.tt_wrap == TT_WRAP_INVALID
		|| resolution->locator_echo.tt_wrap > TT_WRAP_MAX
		|| !resolution->local_canonical_valid
		|| resolution->local_canonical_generation == 0
		|| resolution->local_canonical.reserved32 != 0
		|| resolution->local_canonical.segment_id == 0
		|| resolution->local_canonical.segment_id > UINT16_MAX
		|| resolution->local_canonical.xid != request->xid
		|| resolution->local_canonical.slot_offset >= TT_SLOTS_PER_SEGMENT
		|| resolution->local_canonical.wrap == TT_WRAP_INVALID
		|| resolution->local_canonical.wrap > TT_WRAP_MAX
		|| resolution->local_canonical.wrap != resolution->locator_echo.tt_wrap)
		return false;

	return true;
}

ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_canonical_proof_v1(
	const ClusterHeapFreezeCanonicalProofV1 *proof)
{
	const ClusterTxResolution *resolution;

	if (proof == NULL || !freeze_final_fence_shape_valid(&proof->expected_fence))
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	if (!freeze_final_fence_shape_valid(&proof->current_fence)
		|| !freeze_final_fence_equal(&proof->expected_fence, &proof->current_fence))
		return CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED;

	/* C1b and admission are explicit gates; a missing gate is not proof. */
	if (!proof->c1b_finalized || !proof->authority_current || !proof->admission_current)
		return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;

	resolution = &proof->resolution;
	if (resolution->authority.origin_epoch == 0
		|| XLogRecPtrIsInvalid(resolution->authority.live_hwm_lsn)
		|| resolution->authority.tt_generation == 0
		|| !SCN_VALID(resolution->authority.authority_scn)
		|| !freeze_canonical_locator_shape_valid(&proof->requested_locator, resolution))
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;

	/* Non-terminal or unknown outcomes never admit a freeze. */
	switch (resolution->outcome) {
		case CLUSTER_TX_UNKNOWN:
		case CLUSTER_TX_IN_PROGRESS:
		case CLUSTER_TX_PREPARED:
			return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
		case CLUSTER_TX_COMMITTED:
		case CLUSTER_TX_ABORTED:
			break;
		default:
			return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	}

	if (!cluster_tx_outcome_proof_is_valid(resolution->outcome,
											 resolution->proof_kind))
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	if (resolution->outcome == CLUSTER_TX_COMMITTED
		&& !SCN_VALID(resolution->commit_scn))
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	if (resolution->outcome == CLUSTER_TX_ABORTED
		&& SCN_VALID(resolution->commit_scn))
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;

	return CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
}

ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_proof_batch_v1(
	const ClusterHeapFreezeProofBatchV1 *batch)
{
	ClusterHeapFreezeProofStatusV1 status;
	bool saw_retry = false;
	bool saw_unproven = false;
	const ClusterHeapFreezeCanonicalProofV1 *first;

	if (batch == NULL)
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	if (batch->proof_count == 0)
		return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	if (batch->proof_count > CLUSTER_HEAP_FREEZE_MAX_PROOF_BATCH)
		return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;

	/* Validate every member before comparing the common fence. */
	for (uint8 index = 0; index < batch->proof_count; index++)
	{
		status = cluster_heap_freeze_validate_canonical_proof_v1(&batch->proofs[index]);
		if (status == CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR)
			return status;
		if (status == CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED)
			saw_retry = true;
		if (status == CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN)
			saw_unproven = true;
	}
	if (saw_retry)
		return CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED;
	if (saw_unproven)
		return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;

	first = &batch->proofs[0];
	for (uint8 index = 1; index < batch->proof_count; index++)
	{
		const ClusterHeapFreezeCanonicalProofV1 *proof = &batch->proofs[index];

		/* Distinct final fences make a one-shot apply ambiguous. */
		if (!freeze_final_fence_equal(&first->expected_fence, &proof->expected_fence)
			|| !freeze_final_fence_equal(&first->current_fence, &proof->current_fence))
			return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	}

	return CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
}
