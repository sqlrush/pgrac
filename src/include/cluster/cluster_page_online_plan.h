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
#include "cluster/cluster_wal_source.h"

#define CLUSTER_PAGE_ONLINE_PLAN_INTERFACE_V1 1
#define CLUSTER_PAGE_DEPENDENCY_QUEUE_V1 1
#define RF_PAGE_ONLINE_PLAN_MAX_BYTES (8 * 1024 * 1024)

typedef struct RfPageOnlinePlanV1 RfPageOnlinePlanV1;

typedef struct RfPageOnlinePlanRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32 participant_count;
	uint64 retention_binding_cookie;
	Size memory_budget;
	/* Same original source's native checkpoint. NULL preserves a full-cut
	 * redo plan. Earlier retained records remain dependency input only. */
	const XLogRecPtr *redo_starts;
} RfPageOnlinePlanRequestV1;

typedef struct RfPageOnlineRecordIdentityV1 {
	RfPageReplayRecordIdentityV1 record;
	uint16 participant_index;
	uint16 reserved_zero;
} RfPageOnlineRecordIdentityV1;

typedef struct RfPageOnlineTargetViewV1 {
	RfPageIdentityV1 page_identity;
	uint8 before_kind;
	bool history_only;
	uint8 reserved_zero[6];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 expected_result;
	const char *canonical_page;
	const RfPagePinnedSourceV1 *source;
	const RfContributorVectorV1 *contributors;
	const RfPageStableGraphRequestV1 *graph;
} RfPageOnlineTargetViewV1;

/* An exact DATA version qualified by the original write/fsync owner. This
 * projection alone is not a durability proof or a grant to retire WAL. */
typedef struct RfPageDataCoverageV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 version;
} RfPageDataCoverageV1;

/* PAGE-only retention constraint in one original physical cut. The caller
 * retains that cut's full source identity/authority and combines this with
 * every SIDE, ROOT and other retention obligation before publishing a floor. */
typedef struct RfPageContributionPrefixV1 {
	uint16 origin_thread;
	uint16 reserved_zero;
	TimeLineID timeline;
	XLogRecPtr first_uncovered_lsn;
} RfPageContributionPrefixV1;

extern RfPageProofDetailV1 rf_page_online_plan_create_v1(const RfPageOnlinePlanRequestV1 *request,
														 RfPageOnlinePlanV1 **out_plan);
/* Retained source owner binds full claims once, before any record is fed.
 * Required by DATA completion qualification; pure graph consumers may omit.
 * These copies never acquire retention, fencing or replay permission. */
extern bool rf_page_online_plan_bind_sources_v1(RfPageOnlinePlanV1 *plan,
												const ClusterWalSourceRef *sources,
												uint32 participant_count);
extern bool rf_page_online_plan_source_v1(const RfPageOnlinePlanV1 *plan, uint32 participant_index,
										  ClusterWalSourceRef *out);
extern RfPageProofDetailV1
rf_page_online_plan_feed_record_v1(RfPageOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity);
/* Closed-input callers collect immutable records before seal resolves exact
 * cross-participant dependencies. Never mix queue and direct ordered feed. */
extern RfPageProofDetailV1
rf_page_online_plan_queue_record_v1(RfPageOnlinePlanV1 *plan,
									const RfDetachedRecordPlanV1 *record_plan,
									const RfPageOnlineRecordIdentityV1 *identity);
extern RfPageProofDetailV1 rf_page_online_plan_seal_v1(RfPageOnlinePlanV1 *plan);
extern uint32 rf_page_online_plan_target_count_v1(const RfPageOnlinePlanV1 *plan);
extern bool rf_page_online_plan_target_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
										  RfPageOnlineTargetViewV1 *out_target);
/* Pure, sealed-input projection. Coverage must be sorted by page identity,
 * unique and already qualified by the physical owner. Unknown versions never
 * imply coverage; every failure leaves the output unchanged. No data I/O,
 * authority acquisition, root publication or WAL retirement occurs here. */
extern bool rf_page_online_plan_page_prefix_v1(const RfPageOnlinePlanV1 *plan,
											   const RfPageDataCoverageV1 *coverage,
											   uint32 coverage_count,
											   RfPageContributionPrefixV1 *prefixes,
											   uint32 participant_count);
/* PAGE ancestry constraint for independently qualified original checkpoint
 * prefixes. While any edge on a page remains a recovery obligation, retain
 * that page's complete input chain, including its FPI base. These are only
 * necessary retention bounds; the caller still owns full source identity,
 * durable checkpoint publication and every non-PAGE dependency. This does
 * not turn a DATA receipt into a published checkpoint or authorize WAL GC.
 * Failure leaves output unchanged; input and output may be the same array. */
extern bool rf_page_online_plan_dependency_prefix_v1(const RfPageOnlinePlanV1 *plan,
													 const RfPageContributionPrefixV1 *checkpoints,
													 uint32 participant_count,
													 RfPageContributionPrefixV1 *retained);
extern void rf_page_online_plan_destroy_v1(RfPageOnlinePlanV1 **plan);

#endif /* CLUSTER_PAGE_ONLINE_PLAN_H */
