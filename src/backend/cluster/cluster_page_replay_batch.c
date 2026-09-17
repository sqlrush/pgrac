/*-------------------------------------------------------------------------
 *
 * cluster_page_replay_batch.c
 *    STOP-06 detached-plan to canonical-install production bridge.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_replay_batch.h"
#include "storage/bufpage.h"

static bool
page_all_zero(const char page[BLCKSZ])
{
	const uint8 *bytes = (const uint8 *)page;
	uint8 value = 0;
	int i;

	for (i = 0; i < BLCKSZ; i++)
		value |= bytes[i];
	return value == 0;
}

static bool
page_layout_valid(const char page[BLCKSZ])
{
	const PageHeader header = (const PageHeader)page;

	return !PageIsNew((Page)page) && PageGetPageSize((Page)page) == BLCKSZ
		   && PageGetPageLayoutVersion((Page)page) == PG_PAGE_LAYOUT_VERSION
		   && header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper
		   && header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ;
}

static int
identity_compare(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	int cmp;

	if (left->system_identifier != right->system_identifier)
		return left->system_identifier < right->system_identifier ? -1 : 1;
	cmp = memcmp(left->storage_uuid, right->storage_uuid, 16);
	if (cmp != 0)
		return cmp;
	if (left->locator.spcOid != right->locator.spcOid)
		return left->locator.spcOid < right->locator.spcOid ? -1 : 1;
	if (left->locator.dbOid != right->locator.dbOid)
		return left->locator.dbOid < right->locator.dbOid ? -1 : 1;
	if (left->locator.relNumber != right->locator.relNumber)
		return left->locator.relNumber < right->locator.relNumber ? -1 : 1;
	if (left->forknum != right->forknum)
		return left->forknum < right->forknum ? -1 : 1;
	if (left->blockno != right->blockno)
		return left->blockno < right->blockno ? -1 : 1;
	return 0;
}

static bool
version_zero(const RfPageVersionV1 *version)
{
	static const RfPageVersionV1 zero;

	return memcmp(version, &zero, sizeof(*version)) == 0;
}

static bool
bytes_nonzero(const uint8 *bytes, Size size)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < size; i++)
		value |= bytes[i];
	return value != 0;
}

static bool
target_versions_valid(const RfPageReplayTargetV1 *target)
{
	static const uint8 zero_reserved[7];

	if (!rf_page_identity_valid_v1(&target->page_identity)
		|| !rf_page_version_present_v1(&target->expected_result) || target->base_page == NULL
		|| memcmp(target->reserved_zero, zero_reserved, sizeof(zero_reserved)) != 0)
		return false;
	switch (target->before_kind) {
	case RF_PAGE_STATE_PRESENT:
		return rf_page_version_present_v1(&target->expected_before)
			   && memcmp(target->expected_before.segment_incarnation,
						 target->expected_result.segment_incarnation, 16)
					  == 0;
	case RF_PAGE_STATE_UNFORMATTED:
		return target->expected_before.mutation_token == 0
			   && memcmp(target->expected_before.segment_incarnation,
						 target->expected_result.segment_incarnation, 16)
					  == 0;
	case RF_PAGE_STATE_ABSENT:
		return version_zero(&target->expected_before);
	default:
		return false;
	}
}

static bool
step_matches_target(const RfPageReplayBatchRequestV1 *request, const RfPageReplayTargetV1 *target,
					const RfPageReplayStepV1 *step, const RfPageVersionV1 *expected_before,
					uint8 expected_before_kind)
{
	const RfDetachedRecordPlanV1 *plan;
	const RfDetachedComponentPlanV1 *component;
	const DecodedBkpBlock *block;

	if (step == NULL || step->record_index >= request->record_count)
		return false;
	plan = request->records[step->record_index].record_plan;
	if (!plan->preflight_complete || plan->source_record == NULL
		|| plan->source_record->record == NULL || step->component_index >= plan->component_count)
		return false;
	component = &plan->components[step->component_index];
	if (component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
		|| component->page_class != RF_PAGE_CLASS_ORDINARY
		|| component->result_kind != RF_PAGE_STATE_PRESENT
		|| component->before.mutation_token != expected_before->mutation_token
		|| component->before_kind != expected_before_kind
		|| !rf_page_version_equal_v1(&component->before, expected_before)
		|| !rf_page_version_present_v1(&component->result)
		|| component->result.mutation_token != plan->result_token
		|| component->block_id > plan->source_record->record->max_block_id)
		return false;
	block = &plan->source_record->record->blocks[component->block_id];
	return block->in_use && RelFileLocatorEquals(block->rlocator, target->page_identity.locator)
		   && (uint32)block->forknum == target->page_identity.forknum
		   && block->blkno == target->page_identity.blockno;
}

static bool
first_step_is_full_anchor(const RfPageReplayBatchRequestV1 *request,
						  const RfPageReplayTargetV1 *target)
{
	const RfDetachedRecordPlanV1 *plan;
	const RfDetachedComponentPlanV1 *component;

	if (target->step_count == 0)
		return false;
	plan = request->records[target->steps[0].record_index].record_plan;
	component = &plan->components[target->steps[0].component_index];
	return (component->edge_flags & RF_PAGE_EDGE_FULL_COVERAGE) != 0
		   && (component->edge_flags & (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_WILL_INIT))
				  != 0;
}

static RfPageProofDetailV1
validate_target(const RfPageReplayBatchRequestV1 *request, const RfPageReplayTargetV1 *target,
				uint32 *step_total)
{
	RfPageVersionV1 expected;
	uint8 expected_kind;
	uint32 i;

	if (!target_versions_valid(target))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (target->step_count == 0) {
		if (target->steps != NULL || target->before_kind != RF_PAGE_STATE_PRESENT
			|| !rf_page_version_equal_v1(&target->expected_before, &target->expected_result)
			|| page_all_zero(target->base_page) || !page_layout_valid(target->base_page)
			|| ((const PageHeader)target->base_page)->pd_block_scn
				   != target->expected_result.mutation_token)
			return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (target->steps == NULL || target->step_count > RF_PAGE_STABLE_MAX_EDGES
		|| *step_total > RF_PAGE_STABLE_MAX_EDGES - target->step_count)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	expected = target->expected_before;
	expected_kind = target->before_kind;
	for (i = 0; i < target->step_count; i++) {
		const RfPageReplayStepV1 *step = &target->steps[i];
		const RfDetachedComponentPlanV1 *component;

		if (!step_matches_target(request, target, step, &expected, expected_kind))
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		if (request->record_component_seen[(Size)step->record_index * RF_PAGE_STABLE_MAX_COMPONENTS
										   + step->component_index]
			!= 0)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		request->record_component_seen[(Size)step->record_index * RF_PAGE_STABLE_MAX_COMPONENTS
									   + step->component_index]
			= 1;
		component
			= &request->records[step->record_index].record_plan->components[step->component_index];
		expected = component->result;
		expected_kind = RF_PAGE_STATE_PRESENT;
	}
	if (!rf_page_version_equal_v1(&expected, &target->expected_result))
		return RF_PAGE_PROOF_DETAIL_EDGE_GAP;
	if (!first_step_is_full_anchor(request, target)
		&& (target->before_kind == RF_PAGE_STATE_ABSENT || page_all_zero(target->base_page)
			|| !page_layout_valid(target->base_page)
			|| ((const PageHeader)target->base_page)->pd_block_scn
				   != target->expected_before.mutation_token))
		return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
	*step_total += target->step_count;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
validate_record_table(const RfPageReplayBatchRequestV1 *request)
{
	uint32 component_counts[RF_PAGE_STABLE_MAX_PARTICIPANTS];
	uint32 page_counts[RF_PAGE_STABLE_MAX_PARTICIPANTS];
	uint32 i;

	if (request->system_identifier == 0
		|| !bytes_nonzero(request->storage_uuid, sizeof(request->storage_uuid))
		|| request->participants == NULL || request->participant_count == 0
		|| request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	memset(component_counts, 0, sizeof(component_counts));
	memset(page_counts, 0, sizeof(page_counts));
	for (i = 0; i < request->participant_count; i++) {
		const RfContributorStreamCutV1 *cut = &request->participants[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (cut->failed_thread == 0 || cut->timeline_id == 0
			|| (cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) == 0
			|| (cut->flags & ~RF_CONTRIBUTOR_CUT_KNOWN_MASK) != 0
			|| (empty
				&& (cut->flags != RF_CONTRIBUTOR_CUT_KNOWN_MASK
					|| cut->scan_begin_inclusive != cut->scan_end_exclusive
					|| cut->contributor_count != 0 || cut->component_count != 0))
			|| (!empty
				&& (cut->flags != RF_CONTRIBUTOR_CUT_COMPLETE
					|| cut->scan_begin_inclusive >= cut->scan_end_exclusive))
			|| (i > 0
				&& (request->participants[i - 1].failed_thread > cut->failed_thread
					|| (request->participants[i - 1].failed_thread == cut->failed_thread
						&& request->participants[i - 1].timeline_id >= cut->timeline_id))))
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	}

	if (request->record_count == 0) {
		if (request->records != NULL || request->record_component_seen != NULL
			|| request->record_component_seen_capacity != 0)
			return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
		for (i = 0; i < request->participant_count; i++)
			if ((request->participants[i].flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) == 0)
				return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (request->record_count > RF_PAGE_STABLE_MAX_EDGES || request->records == NULL
		|| request->record_component_seen == NULL
		|| request->record_component_seen_capacity
			   < (Size)request->record_count * RF_PAGE_STABLE_MAX_COMPONENTS)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	memset(request->record_component_seen, 0,
		   (Size)request->record_count * RF_PAGE_STABLE_MAX_COMPONENTS);
	for (i = 0; i < request->record_count; i++) {
		const RfPageReplayRecordV1 *record = &request->records[i];
		const RfDetachedRecordPlanV1 *plan = record->record_plan;
		const RfPageReplayRecordIdentityV1 *identity = &record->identity;
		const RfContributorStreamCutV1 *cut;

		if (plan == NULL || !plan->preflight_complete || plan->source_record == NULL
			|| plan->source_record->record == NULL
			|| plan->component_count > RF_PAGE_STABLE_MAX_COMPONENTS || record->reserved_zero != 0
			|| record->participant_index >= request->participant_count)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		cut = &request->participants[record->participant_index];
		if (identity->system_identifier != request->system_identifier
			|| memcmp(identity->storage_uuid, request->storage_uuid, 16) != 0
			|| identity->origin_thread != cut->failed_thread
			|| identity->timeline_id != cut->timeline_id || identity->reserved_zero != 0
			|| identity->reserved_zero2 != 0 || XLogRecPtrIsInvalid(identity->read_rec_ptr)
			|| XLogRecPtrIsInvalid(identity->end_rec_ptr)
			|| identity->read_rec_ptr >= identity->end_rec_ptr
			|| identity->read_rec_ptr != plan->source_record->ReadRecPtr
			|| identity->end_rec_ptr != plan->source_record->EndRecPtr
			|| identity->record_crc != plan->source_record->record->header.xl_crc
			|| identity->rmid != plan->source_record->record->header.xl_rmid
			|| identity->info != plan->source_record->record->header.xl_info
			|| (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0
			|| identity->read_rec_ptr < cut->scan_begin_inclusive
			|| identity->end_rec_ptr > cut->scan_end_exclusive)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		if (i > 0
			&& (request->records[i - 1].participant_index > record->participant_index
				|| (request->records[i - 1].participant_index == record->participant_index
					&& (request->records[i - 1].identity.read_rec_ptr > identity->read_rec_ptr
						|| (request->records[i - 1].identity.read_rec_ptr == identity->read_rec_ptr
							&& request->records[i - 1].identity.end_rec_ptr
								   >= identity->end_rec_ptr)))))
			return RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
		component_counts[record->participant_index] += plan->component_count;
		{
			uint32 j;

			for (j = 0; j < plan->component_count; j++)
				if (plan->components[j].owner == RF_DETACHED_COMPONENT_PAGE_CODEC)
					page_counts[record->participant_index]++;
		}
	}
	for (i = 0; i < request->participant_count; i++)
		if (component_counts[i] != request->participants[i].component_count
			|| page_counts[i] != request->participants[i].contributor_count)
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
validate_record_component_closure(const RfPageReplayBatchRequestV1 *request)
{
	uint32 i;

	for (i = 0; i < request->record_count; i++) {
		const RfDetachedRecordPlanV1 *plan = request->records[i].record_plan;
		uint32 j;

		for (j = 0; j < plan->component_count; j++) {
			bool seen
				= request->record_component_seen[(Size)i * RF_PAGE_STABLE_MAX_COMPONENTS + j] != 0;

			if ((plan->components[j].owner == RF_DETACHED_COMPONENT_PAGE_CODEC) != seen)
				return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		}
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
rf_page_replay_batch_run(const RfPageReplayBatchRequestV1 *request, RfPageReplayBatchProofV1 *proof,
						 bool use_smgr)
{
	RfPageStorageInstallComponentV1 components[RF_PAGE_STABLE_MAX_COMPONENTS];
	RfPageStorageInstallRequestV1 install_request;
	RfPageReplayBatchProofV1 completed;
	uint32 step_total = 0;
	uint32 i;
	RfPageProofDetailV1 detail;

	if (request == NULL || proof == NULL || request->targets == NULL
		|| request->canonical_pages == NULL || request->install_prepared_pages == NULL
		|| request->install_io_pages == NULL || request->authority == NULL
		|| request->target_count == 0 || request->target_count > RF_PAGE_STABLE_MAX_COMPONENTS
		|| request->canonical_capacity < (Size)request->target_count * BLCKSZ
		|| request->install_prepared_capacity < (Size)request->target_count * BLCKSZ
		|| request->install_io_capacity < (Size)request->target_count * BLCKSZ
		|| (!use_smgr && request->storage == NULL) || (use_smgr && request->storage != NULL))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (!request->global_preflight_ok)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	detail = validate_record_table(request);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;

	for (i = 0; i < request->target_count; i++) {
		if (request->targets[i].page_identity.system_identifier != request->system_identifier
			|| memcmp(request->targets[i].page_identity.storage_uuid, request->storage_uuid, 16)
				   != 0)
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (i > 0
			&& identity_compare(&request->targets[i - 1].page_identity,
								&request->targets[i].page_identity)
				   >= 0)
			return RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
		detail = validate_target(request, &request->targets[i], &step_total);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			return detail;
	}
	detail = validate_record_component_closure(request);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;

	memset(components, 0, sizeof(components));
	for (i = 0; i < request->target_count; i++) {
		const RfPageReplayTargetV1 *target = &request->targets[i];
		char *canonical = request->canonical_pages + (Size)i * BLCKSZ;
		uint32 j;

		memcpy(canonical, target->base_page, BLCKSZ);
		for (j = 0; j < target->step_count; j++) {
			const RfDetachedRecordPlanV1 *plan
				= request->records[target->steps[j].record_index].record_plan;

			detail = rf_page_detached_apply_v1(plan, target->steps[j].component_index, canonical,
											   canonical);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				return detail;
		}
		if (page_all_zero(canonical) || !page_layout_valid(canonical)
			|| ((PageHeader)canonical)->pd_block_scn != target->expected_result.mutation_token)
			return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
		components[i].page_identity = target->page_identity;
		components[i].before_kind = target->before_kind;
		components[i].expected_before = target->expected_before;
		components[i].expected_result = target->expected_result;
		components[i].canonical_page = canonical;
	}

	memset(&install_request, 0, sizeof(install_request));
	install_request.components = components;
	install_request.component_count = request->target_count;
	install_request.prepared_pages = request->install_prepared_pages;
	install_request.prepared_capacity = request->install_prepared_capacity;
	install_request.io_pages = request->install_io_pages;
	install_request.io_capacity = request->install_io_capacity;
	install_request.storage = request->storage;
	install_request.authority = request->authority;
	install_request.global_preflight_ok = true;
	memset(&completed, 0, sizeof(completed));
#ifndef USE_CLUSTER_UNIT
	if (use_smgr)
		detail = rf_page_storage_install_smgr_v1(&install_request, &completed.install);
	else
#endif
		detail = rf_page_storage_install_execute_v1(&install_request, &completed.install);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	completed.target_count = request->target_count;
	completed.step_count = step_total;
	completed.detached_apply_complete = true;
	*proof = completed;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_replay_batch_execute_v1(const RfPageReplayBatchRequestV1 *request,
								RfPageReplayBatchProofV1 *proof)
{
	return rf_page_replay_batch_run(request, proof, false);
}

#ifndef USE_CLUSTER_UNIT
RfPageProofDetailV1
rf_page_replay_batch_smgr_v1(const RfPageReplayBatchRequestV1 *request,
							 RfPageReplayBatchProofV1 *proof)
{
	return rf_page_replay_batch_run(request, proof, true);
}
#endif
