/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric_apply.c
 *    Apply immutable PAGE/SIDE plans under one fresh ROOT authority.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_page_authority.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"

#ifdef USE_CLUSTER_UNIT
#define fabric_apply_alloc0(size_) calloc(1, (size_))
#define fabric_apply_free(pointer_) free((pointer_))
#else
#define fabric_apply_alloc0(size_) palloc0(size_)
#define fabric_apply_free(pointer_) pfree(pointer_)
#endif

typedef struct ClusterThreadRecoveryFabricApplyStateV1 {
	RfPageOnlineTargetViewV1 *views;
	RfPageStorageInstallComponentV1 *components;
	RfPageAuthorityTargetV1 *authority_targets;
	RfPageStableBaseProofV1 **proofs;
	uint32 *chain_indices;
	char *prepared_pages;
	char *io_pages;
	RfPageAuthorityPreflightV1 *page_preflight;
	RfPageSmgrPreopenV1 *smgr_preopen;
	ClusterSpaceRecoveryBatchV1 *space;
} ClusterThreadRecoveryFabricApplyStateV1;

static bool
fabric_apply_authority_fresh(void *arg)
{
	const ClusterThreadRecoveryAuthorityV1 *authority
		= (const ClusterThreadRecoveryAuthorityV1 *)arg;

	return cluster_thread_recovery_authority_revalidate_nowait_v1(authority)
		   == CLUSTER_THREAD_AUTHORITY_OK;
}

static bool
fabric_apply_sources_fresh(const ClusterThreadRecoveryAuthorityV1 *authorities, uint32 count)
{
	for (uint32 i = 0; i < count; i++)
		if (!fabric_apply_authority_fresh((void *)&authorities[i]))
			return false;
	return true;
}

static RfPageProofDetailV1
fabric_apply_side_origin(const RfSideOnlinePlanV1 *plan,
						 const ClusterThreadRecoveryAuthorityV1 *authority, uint32 epoch,
						 ClusterSpaceRecoveryBatchV1 *space, bool preflight_only)
{
	RfSideOnlineProductionOwnerV1 owner;

	if (!rf_side_online_production_owner_init_v1(&owner, (void *)authority,
												 fabric_apply_authority_fresh, epoch, true)
		|| !rf_side_online_production_bind_undo_v1(&owner, authority))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	if (space != NULL) {
		owner.space_arg = space;
		owner.preflight_space = cluster_space_recovery_preflight_operation_v1;
		owner.apply_space = cluster_space_recovery_applied_operation_v1;
		owner.borrowed_scratch_bytes = cluster_space_recovery_scratch_held_v1(space);
	}
	return preflight_only ? rf_side_online_production_preflight_v1(plan, &owner)
						  : rf_side_online_production_apply_v1(plan, &owner);
}

static RfPageProofDetailV1
fabric_apply_map_authority(RfPageAuthorityVerdictV1 verdict)
{
	switch (verdict) {
	case RF_PAGE_AUTHORITY_OK:
		return RF_PAGE_PROOF_DETAIL_OK;
	case RF_PAGE_AUTHORITY_INVALID_ARGUMENT:
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	case RF_PAGE_AUTHORITY_ROOT_STALE:
	case RF_PAGE_AUTHORITY_GENERATION_STALE:
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	case RF_PAGE_AUTHORITY_FENCE_STALE:
	case RF_PAGE_AUTHORITY_SERIAL_NOT_HELD:
		return RF_PAGE_PROOF_DETAIL_FENCE_STALE;
	case RF_PAGE_AUTHORITY_RETENTION_STALE:
		return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
	case RF_PAGE_AUTHORITY_SOURCE_GAP:
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	case RF_PAGE_AUTHORITY_NO_STABLE_BASE:
		return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
	case RF_PAGE_AUTHORITY_CLASS_UNKNOWN:
		return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
	case RF_PAGE_AUTHORITY_IDENTITY_MISMATCH:
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	case RF_PAGE_AUTHORITY_INCARNATION_MISMATCH:
		return RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH;
	case RF_PAGE_AUTHORITY_VERSION_RULE_MISSING:
		return RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH;
	case RF_PAGE_AUTHORITY_CONTRIBUTOR_INCOMPLETE:
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	case RF_PAGE_AUTHORITY_SIDE_INCOMPLETE:
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	case RF_PAGE_AUTHORITY_WOULD_BLOCK:
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	case RF_PAGE_AUTHORITY_CANCELLED:
		return RF_PAGE_PROOF_DETAIL_CANCELLED;
	case RF_PAGE_AUTHORITY_OOM:
		return RF_PAGE_PROOF_DETAIL_OOM;
	case RF_PAGE_AUTHORITY_INTERNAL:
	default:
		return RF_PAGE_PROOF_DETAIL_INTERNAL;
	}
}

static void
fabric_apply_state_free(ClusterThreadRecoveryFabricApplyStateV1 *state, uint32 target_count)
{
	uint32 i;

	cluster_space_recovery_destroy_v1(&state->space);
	if (state->smgr_preopen != NULL)
		rf_page_storage_smgr_preopen_destroy_v1(&state->smgr_preopen);
	if (state->page_preflight != NULL)
		rf_page_authority_preflight_destroy_v1(&state->page_preflight);
	if (state->proofs != NULL)
		for (i = 0; i < target_count; i++)
			if (state->proofs[i] != NULL)
				rf_page_stable_base_proof_destroy_v1(&state->proofs[i]);
	if (state->io_pages != NULL)
		fabric_apply_free(state->io_pages);
	if (state->prepared_pages != NULL)
		fabric_apply_free(state->prepared_pages);
	if (state->chain_indices != NULL)
		fabric_apply_free(state->chain_indices);
	if (state->proofs != NULL)
		fabric_apply_free(state->proofs);
	if (state->authority_targets != NULL)
		fabric_apply_free(state->authority_targets);
	if (state->components != NULL)
		fabric_apply_free(state->components);
	if (state->views != NULL)
		fabric_apply_free(state->views);
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_apply_sources_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												const ClusterThreadRecoveryAuthorityV1 *authority,
												uint32 count,
												ClusterThreadRecoveryFabricApplyResultV1 *result)
{
	const RfPageOnlinePlanV1 *page_plan;
	const RfSideOnlinePlanV1 *side_plan;
	ClusterThreadRecoveryFabricApplyStateV1 *state;
	ClusterThreadRecoveryFabricApplyResultV1 completed;
	RfPageInstallAuthorityAdapterV1 page_adapter;
	RfPageStorageInstallRequestV1 install_request;
	RfPageStorageInstallProofV1 install_proof;
	RfPageAuthorityBatchRequestV1 authority_request;
	RfContributorStreamCutV1 cut;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
	uint64 current_epoch;
	uint32 participant_count;
	uint32 target_count;
	uint32 retained_target_count;
	uint32 max_chain_count = 0;
	uint32 i;

	if (result == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	if (authority == NULL || count == 0 || count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	page_plan = cluster_thread_recovery_fabric_page_plan_v1(plan);
	side_plan = cluster_thread_recovery_fabric_side_plan_v1(plan);
	if (page_plan == NULL || side_plan == NULL || !fabric_apply_sources_fresh(authority, count))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	participant_count = cluster_thread_recovery_fabric_participant_count_v1(plan);
	if (participant_count != count)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	for (i = 0; i < participant_count; i++) {
		const ClusterThreadRecoveryAuthorityV1 *source = &authority[i];

		if (source->duty == NULL || source->root_snapshot == NULL
			|| !cluster_thread_recovery_fabric_identity_matches_v1(
				plan, source->duty->system_identifier, source->duty->storage_uuid)
			|| !cluster_thread_recovery_fabric_identity_matches_v1(
				plan, source->root_snapshot->identity.system_identifier,
				source->root_snapshot->identity.storage_uuid))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (source->retention_pin == NULL || source->retention_pin != authority[0].retention_pin)
			return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
		if (!cluster_thread_recovery_fabric_cut_v1(plan, i, &cut))
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		if (cut.failed_thread == 0 || cut.failed_thread != source->duty->origin_thread_id
			|| cut.failed_thread != source->root_snapshot->identity.origin_thread_id
			|| (i > 0 && source->duty->origin_thread_id <= authority[i - 1].duty->origin_thread_id)
			|| cut.timeline_id == 0 || cut.timeline_id != source->root_snapshot->checkpoint_tli
			|| cut.timeline_id != source->root_snapshot->tail_tli
			|| cut.flags != RF_CONTRIBUTOR_CUT_COMPLETE
			|| cut.scan_begin_inclusive == InvalidXLogRecPtr
			|| cut.scan_end_exclusive <= cut.scan_begin_inclusive
			|| cut.scan_begin_inclusive != source->root_snapshot->checkpoint_lower_lsn
			|| cut.scan_end_exclusive != source->root_snapshot->validated_tail_lsn_exclusive)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		if (!cluster_thread_recovery_authority_covers_window_v1(
				source, cut.failed_thread, cut.scan_begin_inclusive, cut.scan_end_exclusive))
			return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
	}
	current_epoch = cluster_epoch_get_current();
	if (current_epoch == 0 || current_epoch > UINT32_MAX)
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	retained_target_count = rf_page_online_plan_target_count_v1(page_plan);
	if (retained_target_count > RF_PAGE_STABLE_MAX_EDGES)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	target_count = 0;
	for (i = 0; i < retained_target_count; i++) {
		RfPageOnlineTargetViewV1 view;
		if (!rf_page_online_plan_target_v1(page_plan, i, &view))
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		if (!view.history_only)
			target_count++;
	}
	state = fabric_apply_alloc0(sizeof(*state));
	if (state == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	memset(&completed, 0, sizeof(completed));
	completed.page_target_count = target_count;
	completed.side_operation_count = rf_side_online_plan_origin_operation_count_v1(side_plan, 0);

	/* Heap-owned state survives PG ERROR longjmp at any preparation/apply
	 * stage. The caller's plan and original authorities are only borrowed. */
	PG_TRY();
	{
		if (rf_side_online_plan_space_target_count_v1(side_plan) > 0
			&& !cluster_space_recovery_preflight_v1(plan, authority, count, &state->space)) {
			detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			goto done;
		}
		if (target_count > 0) {
			Size page_bytes = (Size)target_count * BLCKSZ;
			uint32 next = 0;

			state->views = (RfPageOnlineTargetViewV1 *)fabric_apply_alloc0((Size)target_count
																		   * sizeof(*state->views));
			state->components = (RfPageStorageInstallComponentV1 *)fabric_apply_alloc0(
				(Size)target_count * sizeof(*state->components));
			state->authority_targets = (RfPageAuthorityTargetV1 *)fabric_apply_alloc0(
				(Size)target_count * sizeof(*state->authority_targets));
			state->proofs = (RfPageStableBaseProofV1 **)fabric_apply_alloc0(
				(Size)target_count * sizeof(*state->proofs));
			state->prepared_pages = (char *)fabric_apply_alloc0(page_bytes);
			state->io_pages = (char *)fabric_apply_alloc0(page_bytes);
			if (state->views == NULL || state->components == NULL
				|| state->authority_targets == NULL || state->proofs == NULL
				|| state->prepared_pages == NULL || state->io_pages == NULL) {
				detail = RF_PAGE_PROOF_DETAIL_OOM;
				goto done;
			}
			for (i = 0; i < retained_target_count; i++) {
				RfPageOnlineTargetViewV1 view;
				if (!rf_page_online_plan_target_v1(page_plan, i, &view)) {
					detail = RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
					goto done;
				}
				if (view.history_only)
					continue;
				if (view.source == NULL || view.contributors == NULL || view.graph == NULL
					|| view.canonical_page == NULL || view.contributors->edge_count == 0
					|| next >= target_count) {
					detail = RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
					goto done;
				}
				state->views[next++] = view;
				max_chain_count = Max(max_chain_count, view.contributors->edge_count);
			}
			state->chain_indices = (uint32 *)fabric_apply_alloc0((Size)max_chain_count
																 * sizeof(*state->chain_indices));
			if (state->chain_indices == NULL) {
				detail = RF_PAGE_PROOF_DETAIL_OOM;
				goto done;
			}
			for (i = 0; i < target_count; i++) {
				detail = rf_page_stable_base_proof_build_sources_v1(
					state->views[i].graph, authority, participant_count, state->chain_indices,
					max_chain_count, &state->proofs[i]);
				if (detail != RF_PAGE_PROOF_DETAIL_OK)
					goto done;
				state->authority_targets[i].page_identity = state->views[i].page_identity;
				state->authority_targets[i].expected_result = state->views[i].expected_result;
				state->authority_targets[i].stable_base = state->proofs[i];
				state->authority_targets[i].source = state->views[i].source;
				state->authority_targets[i].contributors = state->views[i].contributors;
				state->components[i].page_identity = state->views[i].page_identity;
				state->components[i].before_kind = state->views[i].before_kind;
				state->components[i].expected_before = state->views[i].expected_before;
				state->components[i].expected_result = state->views[i].expected_result;
				state->components[i].canonical_page = state->views[i].canonical_page;
			}
			memset(&authority_request, 0, sizeof(authority_request));
			authority_request.targets = state->authority_targets;
			authority_request.target_count = target_count;
			authority_request.source_authorities = authority;
			authority_request.participant_count = participant_count;
			detail = fabric_apply_map_authority(rf_page_authority_batch_preflight_wait_v1(
				&authority_request, 1000, &state->page_preflight));
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				goto done;
			if (!rf_page_install_authority_adapter_init_v1(
					state->page_preflight, authority->serial_guard, &page_adapter)) {
				detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
				goto done;
			}
			memset(&install_request, 0, sizeof(install_request));
			install_request.components = state->components;
			install_request.component_count = target_count;
			install_request.prepared_pages = state->prepared_pages;
			install_request.prepared_capacity = page_bytes;
			install_request.io_pages = state->io_pages;
			install_request.io_capacity = page_bytes;
			install_request.authority = &page_adapter.ops;
			install_request.global_preflight_ok = true;
			detail = rf_page_storage_smgr_preopen_v1(&install_request, &state->smgr_preopen);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				goto done;
		}

		for (i = 0; i < participant_count; i++) {
			if (!fabric_apply_sources_fresh(authority, count)) {
				detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
				goto done;
			}
			detail = fabric_apply_side_origin(side_plan, &authority[i], (uint32)current_epoch,
											  state->space, true);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				goto done;
		}
		if (!fabric_apply_sources_fresh(authority, count)) {
			detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
			goto done;
		}
		if (state->space != NULL && !cluster_space_recovery_apply_v1(state->space)) {
			detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			goto done;
		}
		if (target_count > 0) {
			memset(&install_proof, 0, sizeof(install_proof));
			detail = rf_page_storage_install_smgr_preopened_v1(&install_request,
															   state->smgr_preopen, &install_proof);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				goto done;
			completed.page_write_count = install_proof.write_count;
			completed.page_result_skip_count = install_proof.result_skip_count;
			completed.page_durability_complete = install_proof.durability_complete;
			completed.page_postread_complete = install_proof.postread_complete;
			if (!install_proof.proof_published || !install_proof.authority_released
				|| !completed.page_durability_complete || !completed.page_postread_complete) {
				detail = RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED;
				goto done;
			}
		}
		for (i = 0; i < participant_count; i++) {
			if (!fabric_apply_sources_fresh(authority, count)) {
				detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
				goto done;
			}
			detail = fabric_apply_side_origin(side_plan, &authority[i], (uint32)current_epoch,
											  state->space, false);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				goto done;
		}
		completed.side_apply_complete = true;
		if (!fabric_apply_sources_fresh(authority, count)) {
			detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
			goto done;
		}
		detail = RF_PAGE_PROOF_DETAIL_OK;

	done:;
	}
	PG_FINALLY();
	{
		fabric_apply_state_free(state, target_count);
		fabric_apply_free(state);
	}
	PG_END_TRY();
	if (detail == RF_PAGE_PROOF_DETAIL_OK)
		*result = completed;
	return detail;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_apply_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
										const ClusterThreadRecoveryAuthorityV1 *authority,
										ClusterThreadRecoveryFabricApplyResultV1 *result)
{
	return cluster_thread_recovery_fabric_apply_sources_v1(plan, authority, 1, result);
}

#endif
