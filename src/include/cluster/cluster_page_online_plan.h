/*-------------------------------------------------------------------------
 *
 * cluster_page_online_plan.h
 *    STOP-06 immutable online PAGE plan assembled before IR.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_ONLINE_PLAN_H
#define CLUSTER_PAGE_ONLINE_PLAN_H

#include "cluster/cluster_page_detached.h"

#define CLUSTER_PAGE_ONLINE_PLAN_INTERFACE_V1 1
#define RF_PAGE_ONLINE_PLAN_MAX_BYTES (8 * 1024 * 1024)

typedef struct RfPageOnlinePlanV1 RfPageOnlinePlanV1;

typedef struct RfPageOnlinePlanRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32 participant_count;
	uint64 retention_binding_cookie;
	Size memory_budget;
} RfPageOnlinePlanRequestV1;

typedef struct RfPageOnlineRecordIdentityV1 {
	RfPageReplayRecordIdentityV1 record;
	uint16 participant_index;
	uint16 reserved_zero;
} RfPageOnlineRecordIdentityV1;

typedef struct RfPageOnlineTargetViewV1 {
	RfPageIdentityV1 page_identity;
	uint8 before_kind;
	uint8 reserved_zero[7];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 expected_result;
	const char *canonical_page;
	const RfPagePinnedSourceV1 *source;
	const RfContributorVectorV1 *contributors;
	const RfPageStableGraphRequestV1 *graph;
} RfPageOnlineTargetViewV1;

extern RfPageProofDetailV1 rf_page_online_plan_create_v1(const RfPageOnlinePlanRequestV1 *request,
														 RfPageOnlinePlanV1 **out_plan);
extern RfPageProofDetailV1
rf_page_online_plan_feed_record_v1(RfPageOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity);
extern RfPageProofDetailV1 rf_page_online_plan_seal_v1(RfPageOnlinePlanV1 *plan);
extern uint32 rf_page_online_plan_target_count_v1(const RfPageOnlinePlanV1 *plan);
extern bool rf_page_online_plan_target_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
										  RfPageOnlineTargetViewV1 *out_target);
extern void rf_page_online_plan_destroy_v1(RfPageOnlinePlanV1 **plan);

#endif /* CLUSTER_PAGE_ONLINE_PLAN_H */
