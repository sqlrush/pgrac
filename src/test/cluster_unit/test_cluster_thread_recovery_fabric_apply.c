/*-------------------------------------------------------------------------
 * test_cluster_thread_recovery_fabric_apply.c
 *    RF ROOT all-target preflight and PAGE-before-SIDE apply ordering.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_page_authority.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static char fabric_object;
static char page_plan_object;
static char side_plan_object;
static char proof_objects[2];
static char preflight_object;
static char preopen_object;
static RfContributorVectorV1 contributors[2];
static RfPagePinnedSourceV1 sources[2];
static RfPageStableGraphRequestV1 graphs[2];
static RfPageOnlineTargetViewV1 targets[2];
static int target_count;
static int step;
static int proof_step;
static int page_preflight_step;
static int preopen_step;
static int side_preflight_step;
static int page_install_step;
static int side_apply_step;
static bool side_preflight_ok;
static bool authority_current;
static bool retained_coverage_current;
static uint32 participant_count;
static uint32 side_preflight_calls, side_apply_calls;
static uint16 refuse_thread, stale_thread;
static bool stale_after_page, stale_after_side;

const RfPageOnlinePlanV1 *
cluster_thread_recovery_fabric_page_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan == (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object
			   ? (const RfPageOnlinePlanV1 *)&page_plan_object
			   : NULL;
}

const RfSideOnlinePlanV1 *
cluster_thread_recovery_fabric_side_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan == (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object
			   ? (const RfSideOnlinePlanV1 *)&side_plan_object
			   : NULL;
}

uint32
cluster_thread_recovery_fabric_participant_count_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan == (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object ? participant_count
																			 : 0;
}

bool
cluster_thread_recovery_fabric_identity_matches_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												   uint64 system_identifier,
												   const uint8 storage_uuid[16])
{
	return plan == (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object
		   && system_identifier == 99 && storage_uuid != NULL && storage_uuid[0] == 3;
}

bool
cluster_thread_recovery_fabric_cut_v1(const ClusterThreadRecoveryFabricPlanV1 *plan, uint32 index,
									  RfContributorStreamCutV1 *out_cut)
{
	if (plan != (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object
		|| index >= participant_count || out_cut == NULL)
		return false;
	memset(out_cut, 0, sizeof(*out_cut));
	out_cut->failed_thread = index + 2;
	out_cut->flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	out_cut->timeline_id = 7;
	out_cut->scan_begin_inclusive = 0x100;
	out_cut->scan_end_exclusive = 0x200;
	return true;
}

uint32
rf_page_online_plan_target_count_v1(const RfPageOnlinePlanV1 *plan)
{
	return plan == (const RfPageOnlinePlanV1 *)&page_plan_object ? (uint32)target_count : 0;
}

bool
rf_page_online_plan_target_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
							  RfPageOnlineTargetViewV1 *out_target)
{
	if (plan != (const RfPageOnlinePlanV1 *)&page_plan_object || index >= (uint32)target_count)
		return false;
	*out_target = targets[index];
	return true;
}

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	return plan == (const RfSideOnlinePlanV1 *)&side_plan_object ? 3 : 0;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	return authority != NULL && authority_current
				   && authority->duty->origin_thread_id != stale_thread
			   ? CLUSTER_THREAD_AUTHORITY_OK
			   : CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 dead_tid, XLogRecPtr scan_lower,
	XLogRecPtr scan_upper)
{
	return authority != NULL && retained_coverage_current
		   && dead_tid == authority->duty->origin_thread_id && scan_lower == 0x100
		   && scan_upper == 0x200;
}

uint64
cluster_epoch_get_current(void)
{
	return 9;
}

RfPageProofDetailV1
rf_page_stable_base_proof_build_sources_v1(const RfPageStableGraphRequestV1 *graph,
										   const ClusterThreadRecoveryAuthorityV1 *authorities,
										   uint32 count, uint32 *chain_indices,
										   uint32 chain_capacity,
										   RfPageStableBaseProofV1 **out_proof)
{
	int index;

	UT_ASSERT(graph != NULL && chain_indices != NULL && chain_capacity != 0 && authorities != NULL
			  && authorities->duty->origin_thread_id == 2 && count == participant_count);
	index = graph == &graphs[0] ? 0 : (graph == &graphs[1] ? 1 : -1);
	UT_ASSERT(index >= 0);
	*out_proof = (RfPageStableBaseProofV1 *)&proof_objects[index];
	proof_step = ++step;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_page_stable_base_proof_destroy_v1(RfPageStableBaseProofV1 **proof)
{
	*proof = NULL;
}

RfPageAuthorityVerdictV1
rf_page_authority_batch_preflight_wait_v1(const RfPageAuthorityBatchRequestV1 *request,
										  int timeout_ms,
										  RfPageAuthorityPreflightV1 **out_preflight)
{
	UT_ASSERT(request != NULL && request->target_count == (uint32)target_count
			  && timeout_ms == 1000);
	UT_ASSERT(request->source_authorities != NULL && request->participant_count == participant_count
			  && request->formation == NULL && request->duties == NULL
			  && request->retention_pin == NULL);
	if (target_count == 2) {
		UT_ASSERT(request->targets[0].contributors == &contributors[0]);
		UT_ASSERT(request->targets[1].contributors == &contributors[1]);
	}
	page_preflight_step = ++step;
	*out_preflight = (RfPageAuthorityPreflightV1 *)&preflight_object;
	return RF_PAGE_AUTHORITY_OK;
}

void
rf_page_authority_preflight_destroy_v1(RfPageAuthorityPreflightV1 **preflight)
{
	*preflight = NULL;
}

bool
rf_page_install_authority_adapter_init_v1(RfPageAuthorityPreflightV1 *preflight,
										  ClusterRecoverySerialGuard *serial_guard,
										  RfPageInstallAuthorityAdapterV1 *adapter)
{
	UT_ASSERT(preflight == (RfPageAuthorityPreflightV1 *)&preflight_object && serial_guard != NULL
			  && adapter != NULL);
	memset(adapter, 0, sizeof(*adapter));
	adapter->ops.arg = adapter;
	return true;
}

RfPageProofDetailV1
rf_page_storage_smgr_preopen_v1(const RfPageStorageInstallRequestV1 *request,
								RfPageSmgrPreopenV1 **out_preopen)
{
	UT_ASSERT(request != NULL && request->component_count == (uint32)target_count
			  && request->storage == NULL);
	preopen_step = ++step;
	*out_preopen = (RfPageSmgrPreopenV1 *)&preopen_object;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_page_storage_smgr_preopen_destroy_v1(RfPageSmgrPreopenV1 **preopen)
{
	*preopen = NULL;
}

RfPageProofDetailV1
rf_page_storage_install_smgr_preopened_v1(const RfPageStorageInstallRequestV1 *request,
										  RfPageSmgrPreopenV1 *preopen,
										  RfPageStorageInstallProofV1 *proof)
{
	UT_ASSERT(request != NULL && preopen == (RfPageSmgrPreopenV1 *)&preopen_object
			  && side_preflight_step != 0 && side_preflight_step < step + 1);
	page_install_step = ++step;
	if (stale_after_page)
		stale_thread = 2;
	memset(proof, 0, sizeof(*proof));
	proof->component_count = request->component_count;
	proof->write_count = request->component_count;
	proof->durability_complete = true;
	proof->postread_complete = true;
	proof->proof_published = true;
	proof->authority_released = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_side_online_production_owner_init_v1(RfSideOnlineProductionOwnerV1 *owner, void *authority_arg,
										RfSideOnlineFreshAuthorityV1 revalidate_authority,
										uint32 cluster_epoch, bool failed_origin_redo_retained)
{
	UT_ASSERT(owner != NULL && authority_arg != NULL && revalidate_authority != NULL
			  && revalidate_authority(authority_arg) && cluster_epoch == 9
			  && failed_origin_redo_retained);
	memset(owner, 0, sizeof(*owner));
	owner->authority_arg = authority_arg;
	return true;
}

bool
rf_side_online_production_bind_undo_v1(RfSideOnlineProductionOwnerV1 *owner,
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	UT_ASSERT(owner->authority_arg == authority);
	owner->undo_authority = authority;
	return true;
}

RfPageProofDetailV1
rf_side_online_production_preflight_v1(const RfSideOnlinePlanV1 *plan,
									   RfSideOnlineProductionOwnerV1 *owner)
{
	UT_ASSERT(plan == (const RfSideOnlinePlanV1 *)&side_plan_object && owner != NULL);
	UT_ASSERT(owner->undo_authority != NULL && owner->undo_authority == owner->authority_arg);
	side_preflight_step = ++step;
	side_preflight_calls++;
	return side_preflight_ok && owner->undo_authority->duty->origin_thread_id != refuse_thread
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

RfPageProofDetailV1
rf_side_online_production_apply_v1(const RfSideOnlinePlanV1 *plan,
								   RfSideOnlineProductionOwnerV1 *owner)
{
	UT_ASSERT(plan == (const RfSideOnlinePlanV1 *)&side_plan_object && owner != NULL);
	side_apply_step = ++step;
	side_apply_calls++;
	if (stale_after_side)
		stale_thread = 2;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
init_case(ClusterThreadRecoveryAuthorityV1 *authority)
{
	static ClusterRecoveryDutyKey duty;
	static ClusterControlRootSnapshot root;
	static ClusterControlRootReadToken token;
	static ClusterRecoverySerialGuard serial;
	int i;

	memset(authority, 0, sizeof(*authority));
	memset(&duty, 0, sizeof(duty));
	memset(&root, 0, sizeof(root));
	duty.system_identifier = 99;
	memset(duty.storage_uuid, 3, 16);
	duty.origin_thread_id = 2;
	root.identity = duty;
	root.checkpoint_tli = 7;
	root.tail_tli = 7;
	root.checkpoint_lower_lsn = 0x100;
	root.validated_tail_lsn_exclusive = 0x200;
	authority->duty = &duty;
	authority->root_snapshot = &root;
	authority->root_token = &token;
	authority->formation = (const ClusterFormationWitnessV1 *)&fabric_object;
	authority->fence_need_set = (const PgracExternalFenceNeedSetV1 *)&page_plan_object;
	authority->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)&side_plan_object;
	authority->retention_pin = (ClusterWalRetentionPin *)&proof_objects[0];
	authority->serial_guard = &serial;
	memset(targets, 0, sizeof(targets));
	memset(graphs, 0, sizeof(graphs));
	for (i = 0; i < 2; i++) {
		targets[i].page_identity.system_identifier = 99;
		memset(targets[i].page_identity.storage_uuid, 3, 16);
		targets[i].page_identity.locator.spcOid = 1;
		targets[i].page_identity.locator.dbOid = 2;
		targets[i].page_identity.locator.relNumber = 3;
		targets[i].page_identity.forknum = MAIN_FORKNUM;
		targets[i].page_identity.blockno = i + 1;
		memset(targets[i].expected_before.segment_incarnation, 7, 16);
		memset(targets[i].expected_result.segment_incarnation, 7, 16);
		targets[i].expected_before.mutation_token = 10 + i;
		targets[i].expected_result.mutation_token = 20 + i;
		targets[i].before_kind = RF_PAGE_STATE_PRESENT;
		targets[i].canonical_page = (const char *)&fabric_object;
		targets[i].source = &sources[i];
		targets[i].contributors = &contributors[i];
		graphs[i].contributors = &contributors[i];
		targets[i].graph = &graphs[i];
		contributors[i].edge_count = 1;
	}
	target_count = 2;
	step = proof_step = page_preflight_step = preopen_step = 0;
	side_preflight_step = page_install_step = side_apply_step = 0;
	side_preflight_ok = true;
	authority_current = true;
	retained_coverage_current = true;
	participant_count = 1;
	side_preflight_calls = side_apply_calls = 0;
	refuse_thread = stale_thread = 0;
	stale_after_page = stale_after_side = false;
}

typedef struct FabricSources {
	ClusterThreadRecoveryAuthorityV1 authorities[3];
	ClusterRecoveryDutyKey duties[3];
	ClusterControlRootSnapshot roots[3];
	ClusterControlRootReadToken tokens[3];
	ClusterRecoverySerialGuard serials[3];
} FabricSources;

static void
init_sources(FabricSources *f)
{
	memset(f, 0, sizeof(*f));
	init_case(&f->authorities[0]);
	for (uint32 i = 0; i < 3; i++) {
		f->authorities[i] = f->authorities[0];
		f->duties[i] = *f->authorities[0].duty;
		f->duties[i].origin_thread_id = i + 2;
		f->roots[i] = *f->authorities[0].root_snapshot;
		f->roots[i].identity = f->duties[i];
		f->authorities[i].duty = &f->duties[i];
		f->authorities[i].root_snapshot = &f->roots[i];
		f->authorities[i].root_token = &f->tokens[i];
		f->authorities[i].serial_guard = &f->serials[i];
	}
	participant_count = 3;
}

UT_TEST(test_multi_source_fabric_preflights_every_origin_before_page_and_side)
{
	FabricSources f;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_sources(&f);
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_sources_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, f.authorities, 3, &result),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(side_preflight_calls, 3);
	UT_ASSERT_EQ(side_apply_calls, 3);
	UT_ASSERT(side_preflight_step < page_install_step && page_install_step < side_apply_step);
	UT_ASSERT(result.side_apply_complete && result.page_durability_complete);
}

UT_TEST(test_multi_source_fabric_late_preflight_refusal_leaves_all_targets_untouched)
{
	FabricSources f;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_sources(&f);
	refuse_thread = 4;
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_sources_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, f.authorities, 3, &result),
		RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(side_preflight_calls, 3);
	UT_ASSERT_EQ(page_install_step, 0);
	UT_ASSERT_EQ(side_apply_calls, 0);
	UT_ASSERT(!result.side_apply_complete);
}

UT_TEST(test_multi_source_fabric_rechecks_prior_sources_between_owners)
{
	FabricSources f;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	for (int side = 0; side < 2; side++) {
		init_sources(&f);
		stale_after_page = !side;
		stale_after_side = side;
		UT_ASSERT_EQ(cluster_thread_recovery_fabric_apply_sources_v1(
						 (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, f.authorities,
						 3, &result),
					 RF_PAGE_PROOF_DETAIL_ROOT_STALE);
		UT_ASSERT_EQ(side_preflight_calls, 3);
		UT_ASSERT_EQ(side_apply_calls, side);
		UT_ASSERT(!result.side_apply_complete);
	}
}

UT_TEST(test_multi_source_fabric_rejects_missing_duplicate_or_unretained_origin)
{
	FabricSources f;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	for (int fault = 0; fault < 5; fault++) {
		init_sources(&f);
		if (fault == 1)
			f.duties[2].origin_thread_id = 3;
		if (fault == 2)
			f.roots[2].validated_tail_lsn_exclusive++;
		if (fault == 3)
			f.authorities[2].retention_pin = (ClusterWalRetentionPin *)&f;
		if (fault == 4)
			f.duties[2].storage_uuid[0]++;
		UT_ASSERT(cluster_thread_recovery_fabric_apply_sources_v1(
					  (const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, f.authorities,
					  fault == 0 ? 2 : 3, &result)
				  != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(proof_step, 0);
		UT_ASSERT_EQ(side_preflight_calls, 0);
		UT_ASSERT_EQ(page_install_step, 0);
	}
}

UT_TEST(test_all_page_and_side_targets_preflight_before_first_mutation)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_case(&authority);
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, &authority, &result),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(proof_step < page_preflight_step && page_preflight_step < preopen_step
			  && preopen_step < side_preflight_step && side_preflight_step < page_install_step
			  && page_install_step < side_apply_step);
	UT_ASSERT_EQ(result.page_target_count, 2);
	UT_ASSERT_EQ(result.side_operation_count, 3);
	UT_ASSERT(result.page_durability_complete && result.page_postread_complete
			  && result.side_apply_complete);
}

UT_TEST(test_side_preflight_failure_leaves_page_unmodified)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_case(&authority);
	side_preflight_ok = false;
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, &authority, &result),
		RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(page_install_step, 0);
	UT_ASSERT_EQ(side_apply_step, 0);
}

UT_TEST(test_side_only_plan_skips_page_authority_and_install)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_case(&authority);
	target_count = 0;
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, &authority, &result),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(proof_step, 0);
	UT_ASSERT_EQ(page_preflight_step, 0);
	UT_ASSERT_EQ(page_install_step, 0);
	UT_ASSERT(side_preflight_step != 0 && side_apply_step > side_preflight_step);
	UT_ASSERT(result.side_apply_complete);
}

UT_TEST(test_retained_cut_mismatch_blocks_before_any_target_preflight)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterThreadRecoveryFabricApplyResultV1 result;

	init_case(&authority);
	retained_coverage_current = false;
	UT_ASSERT_EQ(
		cluster_thread_recovery_fabric_apply_v1(
			(const ClusterThreadRecoveryFabricPlanV1 *)&fabric_object, &authority, &result),
		RF_PAGE_PROOF_DETAIL_RETENTION_STALE);
	UT_ASSERT_EQ(proof_step, 0);
	UT_ASSERT_EQ(page_preflight_step, 0);
	UT_ASSERT_EQ(side_preflight_step, 0);
	UT_ASSERT_EQ(page_install_step, 0);
	UT_ASSERT_EQ(side_apply_step, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_multi_source_fabric_preflights_every_origin_before_page_and_side);
	UT_RUN(test_multi_source_fabric_late_preflight_refusal_leaves_all_targets_untouched);
	UT_RUN(test_multi_source_fabric_rechecks_prior_sources_between_owners);
	UT_RUN(test_multi_source_fabric_rejects_missing_duplicate_or_unretained_origin);
	UT_RUN(test_all_page_and_side_targets_preflight_before_first_mutation);
	UT_RUN(test_side_preflight_failure_leaves_page_unmodified);
	UT_RUN(test_side_only_plan_skips_page_authority_and_install);
	UT_RUN(test_retained_cut_mismatch_blocks_before_any_target_preflight);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
