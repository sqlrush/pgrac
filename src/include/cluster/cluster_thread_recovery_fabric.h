/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric.h
 *    Immutable PAGE/SIDE plan assembled before online recovery mutation.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_THREAD_RECOVERY_FABRIC_H
#define CLUSTER_THREAD_RECOVERY_FABRIC_H

#include "cluster/cluster_side_online_plan.h"

#define CLUSTER_THREAD_RECOVERY_FABRIC_INTERFACE_V1 1

typedef struct ClusterThreadRecoveryFabricPlanV1 ClusterThreadRecoveryFabricPlanV1;

typedef struct ClusterThreadRecoveryFabricPlanRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32 participant_count;
	uint64 retention_binding_cookie;
	Size page_memory_budget;
	Size side_memory_budget;
	bool space_active;
	uint8 reserved_zero[7];
} ClusterThreadRecoveryFabricPlanRequestV1;

typedef struct ClusterThreadRecoveryFabricApplyResultV1 {
	uint32 page_target_count;
	uint32 side_operation_count;
	uint32 page_write_count;
	uint32 page_result_skip_count;
	bool page_durability_complete;
	bool page_postread_complete;
	bool side_apply_complete;
	uint8 reserved_zero[5];
} ClusterThreadRecoveryFabricApplyResultV1;

struct ClusterThreadRecoveryAuthorityV1;

extern RfPageProofDetailV1 cluster_thread_recovery_fabric_plan_create_v1(
	const ClusterThreadRecoveryFabricPlanRequestV1 *request,
	ClusterThreadRecoveryFabricPlanV1 **out_plan);
extern RfPageProofDetailV1 cluster_thread_recovery_fabric_plan_feed_record_v1(
	ClusterThreadRecoveryFabricPlanV1 *plan, XLogReaderState *record, uint16 participant_index);
extern RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_seal_v1(ClusterThreadRecoveryFabricPlanV1 *plan);
extern const RfPageOnlinePlanV1 *
cluster_thread_recovery_fabric_page_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan);
extern const RfSideOnlinePlanV1 *
cluster_thread_recovery_fabric_side_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan);
extern uint32
cluster_thread_recovery_fabric_participant_count_v1(const ClusterThreadRecoveryFabricPlanV1 *plan);
extern bool
cluster_thread_recovery_fabric_identity_matches_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												   uint64 system_identifier,
												   const uint8 storage_uuid[16]);
extern bool cluster_thread_recovery_fabric_cut_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												  uint32 index, RfContributorStreamCutV1 *out_cut);
extern RfPageProofDetailV1 cluster_thread_recovery_fabric_scan_root_v1(
	uint16 dead_thread, XLogRecPtr scan_begin_inclusive, XLogRecPtr scan_end_exclusive,
	const struct ClusterThreadRecoveryAuthorityV1 *authority, bool space_active,
	ClusterThreadRecoveryFabricPlanV1 **out_plan, uint64 *out_record_count);
extern RfPageProofDetailV1
cluster_thread_recovery_fabric_apply_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
										const struct ClusterThreadRecoveryAuthorityV1 *authority,
										ClusterThreadRecoveryFabricApplyResultV1 *result);
extern RfPageProofDetailV1 cluster_thread_recovery_fabric_execute_root_v1(
	uint16 dead_thread, XLogRecPtr scan_begin_inclusive, XLogRecPtr scan_end_exclusive,
	const struct ClusterThreadRecoveryAuthorityV1 *authority, bool space_active,
	ClusterThreadRecoveryFabricApplyResultV1 *result, uint64 *out_record_count);
extern void
cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan);

#endif
