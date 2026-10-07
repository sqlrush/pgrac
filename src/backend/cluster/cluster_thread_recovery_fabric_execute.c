/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric_execute.c
 *    Scan, apply, and release exact ROOT-owned recovery inputs.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_thread_recovery_fabric.h"

static RfPageProofDetailV1
fabric_execute_apply(ClusterThreadRecoveryFabricPlanV1 *plan,
					 const struct ClusterThreadRecoveryAuthorityV1 *authorities, uint32 count,
					 ClusterThreadRecoveryFabricApplyResultV1 *result)
{
	RfPageProofDetailV1 detail;

	PG_TRY();
	{
		detail = cluster_thread_recovery_fabric_apply_sources_v1(plan, authorities, count, result);
	}
	PG_FINALLY();
	{
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
	}
	PG_END_TRY();
	return detail;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_execute_sources_v1(
	const struct ClusterThreadRecoveryAuthorityV1 *authorities, uint32 count, bool space_active,
	ClusterThreadRecoveryFabricApplyResultV1 *result, uint64 *out_record_count)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	ClusterThreadRecoveryFabricApplyResultV1 completed = { 0 };
	RfPageProofDetailV1 detail;
	uint64 record_count = 0;

	if (result == NULL || out_record_count == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	*out_record_count = 0;
	detail = cluster_thread_recovery_fabric_scan_roots_v1(authorities, count, space_active, &plan,
														  &record_count);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INTERNAL;
	detail = fabric_execute_apply(plan, authorities, count, &completed);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	*result = completed;
	*out_record_count = record_count;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_execute_root_v1(
	uint16 dead_thread, XLogRecPtr scan_begin_inclusive, XLogRecPtr scan_end_exclusive,
	const struct ClusterThreadRecoveryAuthorityV1 *authority, bool space_active,
	ClusterThreadRecoveryFabricApplyResultV1 *result, uint64 *out_record_count)
{
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	ClusterThreadRecoveryFabricApplyResultV1 completed;
	RfPageProofDetailV1 detail;
	uint64 record_count = 0;

	if (result == NULL || out_record_count == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	*out_record_count = 0;
	memset(&completed, 0, sizeof(completed));
	detail = cluster_thread_recovery_fabric_scan_root_v1(dead_thread, scan_begin_inclusive,
														 scan_end_exclusive, authority,
														 space_active, &plan, &record_count);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INTERNAL;
	detail = fabric_execute_apply(plan, authority, 1, &completed);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	*result = completed;
	*out_record_count = record_count;
	return RF_PAGE_PROOF_DETAIL_OK;
}

#endif
