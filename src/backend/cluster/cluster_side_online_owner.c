/*-------------------------------------------------------------------------
 * cluster_side_online_owner.c
 *    RF-SIDE production owner for immutable protected-set apply.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_side_online_owner.h"

static bool
side_owner_authority_fresh(RfSideOnlineProductionOwnerV1 *owner)
{
	return owner != NULL && owner->revalidate_authority != NULL
		   && owner->revalidate_authority(owner->authority_arg);
}

static bool
side_owner_begin_protected_set(void *arg)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || owner->protected_set_active || !side_owner_authority_fresh(owner))
		return false;
	cluster_remote_xact_online_writer_push();
	owner->protected_set_active = true;
	owner->protected_set_complete = false;
	return true;
}

static void
side_owner_end_protected_set(void *arg, bool complete)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active)
		return;
	owner->protected_set_complete = complete && side_owner_authority_fresh(owner);
	owner->protected_set_active = false;
	cluster_remote_xact_online_writer_pop();
}

static bool
side_owner_preflight_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		   && rf_side_xact_target_preflight_owned_v1(&operation->xact, operation->owned_payload,
													 operation->owned_payload_length)
				  == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_preflight_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;
	ClusterUndoTargetPreflightV1 target;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_UNDO)
		return false;
	target = cluster_undo_preflight_tt_target_v1(&operation->undo);
	return target == CLUSTER_UNDO_TARGET_APPLY || target == CLUSTER_UNDO_TARGET_PROVED_NOOP;
}

static bool
side_owner_preflight_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && rf_side_online_projection_preflight_owned_v1(&owner->projection, operation);
}

static bool
side_owner_apply_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		   && rf_side_xact_apply_owned_v1(&operation->xact, operation->owned_payload,
										  operation->owned_payload_length)
				  == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_apply_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO
		   && cluster_undo_apply_tt_v1(&operation->undo) == CLUSTER_UNDO_APPLY_OK;
}

static bool
side_owner_apply_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && rf_side_online_projection_apply_owned_v1(&owner->projection, operation);
}

bool
rf_side_online_production_owner_init_v1(RfSideOnlineProductionOwnerV1 *owner, void *authority_arg,
										RfSideOnlineFreshAuthorityV1 revalidate_authority,
										uint32 cluster_epoch, bool failed_origin_redo_retained)
{
	if (owner == NULL || revalidate_authority == NULL || cluster_epoch == 0)
		return false;
	memset(owner, 0, sizeof(*owner));
	owner->authority_arg = authority_arg;
	owner->revalidate_authority = revalidate_authority;
	return rf_side_online_projection_owner_init_v1(&owner->projection, cluster_epoch,
												   failed_origin_redo_retained);
}

RfPageProofDetailV1
rf_side_online_production_preflight_v1(const RfSideOnlinePlanV1 *plan,
									   RfSideOnlineProductionOwnerV1 *owner)
{
	RfSideOnlineApplyOpsV1 ops;

	if (plan == NULL || owner == NULL || owner->protected_set_active)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_set_complete = false;
	memset(&ops, 0, sizeof(ops));
	ops.arg = owner;
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	return rf_side_online_plan_preflight_v1(plan, &ops);
}

RfPageProofDetailV1
rf_side_online_production_apply_v1(const RfSideOnlinePlanV1 *plan,
								   RfSideOnlineProductionOwnerV1 *owner)
{
	RfSideOnlineApplyOpsV1 ops;
	RfPageProofDetailV1 detail;

	if (plan == NULL || owner == NULL || owner->protected_set_active)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_set_complete = false;
	memset(&ops, 0, sizeof(ops));
	ops.arg = owner;
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	detail = rf_side_online_plan_apply_v1(plan, &ops);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (rf_side_online_plan_operation_count_v1(plan) == 0) {
		/*
		 * A sealed empty SIDE proof set closes without taking the online
		 * writer barrier: there are no SIDE bytes to protect or mutate.  It
		 * still needs a fresh authority observation at the closure point.
		 */
		owner->protected_set_complete = side_owner_authority_fresh(owner);
	}
	return owner->protected_set_complete ? RF_PAGE_PROOF_DETAIL_OK
										 : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

#endif
