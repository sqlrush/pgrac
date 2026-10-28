/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_side_owner.c
 *     Original SIDE cold-record executor under the input episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_side_owner.c
 * NOTES
 *     Original episode/plans/readers execute; cold authority, native mutation
 *     and projection leaves remain controlled. No lifetime or COMPLETE grant.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_episode_side_owner_namespace.h"
int d3_episode_side_main(void);
#define TEST_CLUSTER_EPISODE_SIDE_OWNER
#include "test_cluster_side_xact.c"
#include "test_cluster_recovery_episode_side_owner.h"

/* Keep every ordinary provider and assertion above. Only this bridge's
 * controlled scope and native mutation boundary follow the original source
 * supplied by the real episode; the original plan/executor are unchanged. */
static const ClusterRecoveryRelmapSourceV1 *episode_owner_source;
static const ClusterThreadRecoveryAuthorityV1 *episode_owner_authority;
static const RfSideOnlinePlanV1 *episode_owner_plan;
static unsigned episode_owner_fault;
static EpisodeSideOwnerStats episode_owner_stats;

static bool episode_owner_fresh(void *arg)
{
	return arg == episode_owner_authority && episode_owner_source != NULL &&
		   cluster_wal_inputs_relmap_source_current_v1(episode_owner_source);
}

static bool episode_owner_scope_enter(ClusterUndoRecoveryScopeV1 *scope,
									  const ClusterThreadRecoveryAuthorityV1 *authority,
									  const RfSideOnlinePlanV1 *plan)
{
	const ClusterWalInputV1 *input;
	if (scope == NULL || authority != episode_owner_authority || plan != episode_owner_plan ||
		canonical_scope != NULL || !episode_owner_fresh((void *)authority))
		return false;
	++episode_owner_stats.scopes;
	if (episode_owner_fault == 1)
		return false;
	if (episode_owner_fault == 2)
		siglongjmp(*PG_exception_stack, 1);
	input = &episode_owner_source->selected;
	memset(scope, 0, sizeof(*scope));
	scope->authority = authority;
	scope->plan = plan;
	scope->duty = *authority->duty;
	scope->cut.failed_thread = input->source.claim.identity.origin_thread_id;
	scope->cut.timeline_id = input->source.timeline;
	scope->cut.origin_owner_incarnation = input->source.claim.identity.origin_owner_incarnation;
	scope->cut.scan_begin_inclusive = input->checkpoint.checkpoint_lower_lsn;
	scope->cut.scan_end_exclusive = input->checkpoint.validated_tail_lsn_exclusive;
	scope->cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	canonical_scope = scope;
	return true;
}

static bool episode_owner_origin(int origin)
{
	return episode_owner_source != NULL && canonical_scope != NULL &&
		   canonical_scope->authority == episode_owner_authority &&
		   canonical_scope->plan == episode_owner_plan &&
		   origin == episode_owner_source->selected.source.claim.identity.origin_node_id &&
		   canonical_scope->duty.origin_thread_id == (uint16)(origin + 1);
}

static bool episode_owner_native_apply(int origin,
									   const ClusterSideProjectionOperationV1 *operation,
									   const uint8 *payload, uint32 length)
{
	UT_ASSERT(episode_owner_origin(origin));
	UT_ASSERT(operation != NULL && operation->kind == CLUSTER_SIDE_PROJECTION_CLOG);
	UT_ASSERT(payload != NULL && length == sizeof(xl_clog_zeropage_full));
	episode_owner_stats.native_applies++;
	UT_ASSERT(false); /* Complete source preflight must refuse first. */
	return false;
}

static bool episode_owner_native_verify(int origin,
										const ClusterSideProjectionOperationV1 *operation,
										const uint8 *payload, uint32 length)
{
	UT_ASSERT(episode_owner_origin(origin));
	UT_ASSERT(operation != NULL && operation->kind == CLUSTER_SIDE_PROJECTION_CLOG);
	UT_ASSERT(payload != NULL && length == sizeof(xl_clog_zeropage_full));
	episode_owner_stats.native_verifies++;
	UT_ASSERT(false);
	return false;
}

#undef cluster_undo_recovery_scope_enter_v1
#define cluster_undo_recovery_scope_enter_v1 episode_owner_scope_enter
#undef cluster_undo_recovery_origin_authorized_v1
#define cluster_undo_recovery_origin_authorized_v1 episode_owner_origin
#undef cluster_undo_recovery_cold_origin_authorized_v1
#define cluster_undo_recovery_cold_origin_authorized_v1 episode_owner_origin
#undef cluster_clog_native_recovery_full_record_apply_v1
#define cluster_clog_native_recovery_full_record_apply_v1 episode_owner_native_apply
#undef cluster_clog_native_recovery_full_record_verify_v1
#define cluster_clog_native_recovery_full_record_verify_v1 episode_owner_native_verify
#define RF_SIDE_OWNER_TESTING
#define cluster_undo_preflight_tt_target_v1 fixture_preflight_tt_target
#define cluster_undo_apply_tt_v1 fixture_apply_tt
#include "../../backend/cluster/cluster_side_online_owner.c"

RfPageProofDetailV1 test_episode_side_owner_apply(const RfSideOnlinePlanV1 *plan, uint32 index,
												  const ClusterRecoveryRelmapSourceV1 *source,
												  unsigned fault, EpisodeSideOwnerStats *stats)
{
	RfSideOnlineProductionOwnerV1 owner;
	ClusterRecoverySerialGuard guard = {.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED};
	ClusterRecoveryDutyKey duty = {0};
	ClusterThreadRecoveryAuthorityV1 authority = {.duty = &duty, .serial_guard = &guard};
	RfSideOnlineOperationV1 operation;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	memset(&episode_owner_stats, 0, sizeof(episode_owner_stats));
	if (stats == NULL)
		return detail;
	*stats = episode_owner_stats;
	if (source == NULL || !cluster_wal_inputs_relmap_source_current_v1(source) ||
		!rf_side_online_plan_operation_v1(plan, index, &operation))
		return detail;
	duty = source->selected.source.claim.identity;
	episode_owner_source = source;
	episode_owner_authority = &authority;
	episode_owner_plan = plan;
	episode_owner_fault = fault;
	clog_applied_owner(&owner, &authority);
	owner.revalidate_authority = episode_owner_fresh;
	PG_TRY();
	{
		detail =
			rf_side_online_production_apply_record_v1(plan, index, &operation.identity, &owner);
	}
	PG_FINALLY();
	{
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0);
		UT_ASSERT(!owner.protected_set_active && !owner.protected_set_complete &&
				  owner.protected_plan == NULL);
		episode_owner_stats.projections = cold_clog_record_projections;
		*stats = episode_owner_stats;
		cold_clog_test = cold_clog_record_test = control_scope_test = false;
		episode_owner_source = NULL;
		episode_owner_authority = NULL;
		episode_owner_plan = NULL;
	}
	PG_END_TRY();
	return detail;
}
