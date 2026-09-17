/*-------------------------------------------------------------------------
 * test_cluster_side_online_owner.c
 *    RF-SIDE real production callback owner and freshness ordering.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_online_owner.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

typedef struct OwnerCapture {
	uint32 plan_operation_count;
	uint32 authority_calls;
	uint32 authority_fail_call;
	uint32 pushes;
	uint32 pops;
	uint32 xact_preflights;
	uint32 undo_preflights;
	uint32 projection_preflights;
	uint32 xact_applies;
	uint32 undo_applies;
	uint32 projection_applies;
} OwnerCapture;

static OwnerCapture capture;

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(plan != NULL);
	return capture.plan_operation_count;
}

static bool
fresh_authority(void *arg)
{
	OwnerCapture *state = (OwnerCapture *)arg;

	state->authority_calls++;
	return state->authority_fail_call == 0 || state->authority_calls != state->authority_fail_call;
}

void
cluster_remote_xact_online_writer_push(void)
{
	capture.pushes++;
}

void
cluster_remote_xact_online_writer_pop(void)
{
	capture.pops++;
}

bool
rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner, uint32 cluster_epoch,
										bool failed_origin_redo_retained)
{
	memset(owner, 0, sizeof(*owner));
	owner->cluster_epoch = cluster_epoch;
	owner->failed_origin_redo_retained = failed_origin_redo_retained;
	return cluster_epoch != 0;
}

bool
rf_side_online_projection_preflight_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *projection = (RfSideOnlineProjectionOwnerV1 *)arg;

	capture.projection_preflights++;
	return projection->cluster_epoch == 19 && projection->failed_origin_redo_retained
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

bool
rf_side_online_projection_apply_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *projection = (RfSideOnlineProjectionOwnerV1 *)arg;

	capture.projection_applies++;
	return projection->cluster_epoch == 19 && operation != NULL
		   && operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

RfSideXactApplyResultV1
rf_side_xact_target_preflight_owned_v1(const RfSideXactOperationV1 *operation,
									   const uint8 *owned_payload, uint32 owned_payload_length)
{
	capture.xact_preflights++;
	return operation != NULL && owned_payload == NULL && owned_payload_length == 0
			   ? RF_SIDE_XACT_APPLY_OK
			   : RF_SIDE_XACT_APPLY_BLOCKED;
}

RfSideXactApplyResultV1
rf_side_xact_apply_owned_v1(const RfSideXactOperationV1 *operation, const uint8 *owned_payload,
							uint32 owned_payload_length)
{
	capture.xact_applies++;
	return operation != NULL && owned_payload == NULL && owned_payload_length == 0
			   ? RF_SIDE_XACT_APPLY_OK
			   : RF_SIDE_XACT_APPLY_BLOCKED;
}

ClusterUndoTargetPreflightV1
cluster_undo_preflight_tt_target_v1(const ClusterUndoDecoded *decoded)
{
	capture.undo_preflights++;
	return decoded != NULL ? CLUSTER_UNDO_TARGET_APPLY : CLUSTER_UNDO_TARGET_BLOCKED;
}

ClusterUndoApplyResultV1
cluster_undo_apply_tt_v1(const ClusterUndoDecoded *decoded)
{
	capture.undo_applies++;
	return decoded != NULL ? CLUSTER_UNDO_APPLY_OK : CLUSTER_UNDO_APPLY_BLOCKED;
}

RfPageProofDetailV1
rf_side_online_plan_preflight_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfSideOnlineOperationV1 operations[3];
	uint32 i;

	UT_ASSERT(plan != NULL);
	memset(operations, 0, sizeof(operations));
	operations[0].kind = RF_SIDE_ONLINE_OPERATION_XACT;
	operations[1].kind = RF_SIDE_ONLINE_OPERATION_UNDO;
	operations[2].kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (i = 0; i < 3; i++) {
		bool accepted = i == 0	 ? ops->preflight_xact(ops->arg, &operations[i])
						: i == 1 ? ops->preflight_undo(ops->arg, &operations[i])
								 : ops->preflight_projection(ops->arg, &operations[i]);

		if (!accepted) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	ops->end_protected_set(ops->arg, false);
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfSideOnlineOperationV1 operations[3];
	uint32 i;

	UT_ASSERT(plan != NULL);
	if (capture.plan_operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	memset(operations, 0, sizeof(operations));
	operations[0].kind = RF_SIDE_ONLINE_OPERATION_XACT;
	operations[1].kind = RF_SIDE_ONLINE_OPERATION_UNDO;
	operations[2].kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (i = 0; i < 3; i++) {
		bool accepted = i == 0	 ? ops->preflight_xact(ops->arg, &operations[i])
						: i == 1 ? ops->preflight_undo(ops->arg, &operations[i])
								 : ops->preflight_projection(ops->arg, &operations[i]);

		if (!accepted) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	for (i = 0; i < 3; i++) {
		bool applied = i == 0	? ops->apply_xact(ops->arg, &operations[i])
					   : i == 1 ? ops->apply_undo(ops->arg, &operations[i])
								: ops->apply_projection(ops->arg, &operations[i]);

		if (!applied) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	ops->end_protected_set(ops->arg, true);
	return RF_PAGE_PROOF_DETAIL_OK;
}

UT_TEST(test_owner_runs_all_preflights_before_fresh_gated_mutations)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 4);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 8);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT_EQ(capture.xact_preflights, 1);
	UT_ASSERT_EQ(capture.undo_preflights, 1);
	UT_ASSERT_EQ(capture.projection_preflights, 1);
	UT_ASSERT_EQ(capture.xact_applies, 1);
	UT_ASSERT_EQ(capture.undo_applies, 1);
	UT_ASSERT_EQ(capture.projection_applies, 1);
	UT_ASSERT(owner.protected_set_complete);
}

UT_TEST(test_stale_authority_before_first_mutation_closes_whole_set)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	capture.authority_fail_call = 5;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.xact_preflights, 1);
	UT_ASSERT_EQ(capture.undo_preflights, 1);
	UT_ASSERT_EQ(capture.projection_preflights, 1);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT(!owner.protected_set_complete);
}

UT_TEST(test_empty_plan_closes_without_writer_barrier_or_mutation)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 1);
	UT_ASSERT_EQ(capture.pushes, 0);
	UT_ASSERT_EQ(capture.pops, 0);
	UT_ASSERT_EQ(capture.xact_preflights, 0);
	UT_ASSERT_EQ(capture.undo_preflights, 0);
	UT_ASSERT_EQ(capture.projection_preflights, 0);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	UT_ASSERT(owner.protected_set_complete);
}

UT_TEST(test_empty_plan_requires_fresh_closure_authority)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.authority_fail_call = 1;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.authority_calls, 1);
	UT_ASSERT_EQ(capture.pushes, 0);
	UT_ASSERT_EQ(capture.pops, 0);
	UT_ASSERT(!owner.protected_set_complete);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_owner_runs_all_preflights_before_fresh_gated_mutations);
	UT_RUN(test_stale_authority_before_first_mutation_closes_whole_set);
	UT_RUN(test_empty_plan_closes_without_writer_barrier_or_mutation);
	UT_RUN(test_empty_plan_requires_fresh_closure_authority);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
