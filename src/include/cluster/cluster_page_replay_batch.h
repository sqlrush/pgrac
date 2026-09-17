/*-------------------------------------------------------------------------
 *
 * cluster_page_replay_batch.h
 *    STOP-06 detached-plan to canonical-install production bridge.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_REPLAY_BATCH_H
#define CLUSTER_PAGE_REPLAY_BATCH_H

#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_page_install.h"

#define CLUSTER_PAGE_REPLAY_BATCH_INTERFACE_V1 1

typedef struct RfPageReplayStepV1 {
	uint32 record_index;
	uint32 component_index;
} RfPageReplayStepV1;

typedef struct RfPageReplayRecordV1 {
	const RfDetachedRecordPlanV1 *record_plan;
	RfPageReplayRecordIdentityV1 identity;
	uint16 participant_index;
	uint16 reserved_zero;
} RfPageReplayRecordV1;

typedef struct RfPageReplayTargetV1 {
	RfPageIdentityV1 page_identity;
	uint8 before_kind;
	uint8 reserved_zero[7];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 expected_result;
	const char *base_page;
	const RfPageReplayStepV1 *steps;
	uint32 step_count;
} RfPageReplayTargetV1;

typedef struct RfPageReplayBatchRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *participants;
	uint32 participant_count;
	const RfPageReplayRecordV1 *records;
	uint32 record_count;
	const RfPageReplayTargetV1 *targets;
	uint32 target_count;
	uint8 *record_component_seen;
	Size record_component_seen_capacity;
	char *canonical_pages;
	Size canonical_capacity;
	char *install_prepared_pages;
	Size install_prepared_capacity;
	char *install_io_pages;
	Size install_io_capacity;
	const RfPageInstallStorageOpsV1 *storage;
	const RfPageInstallAuthorityOpsV1 *authority;
	bool global_preflight_ok;
} RfPageReplayBatchRequestV1;

typedef struct RfPageReplayBatchProofV1 {
	uint32 target_count;
	uint32 step_count;
	bool detached_apply_complete;
	RfPageStorageInstallProofV1 install;
} RfPageReplayBatchProofV1;

extern RfPageProofDetailV1
rf_page_replay_batch_execute_v1(const RfPageReplayBatchRequestV1 *request,
								RfPageReplayBatchProofV1 *proof);

#ifndef USE_CLUSTER_UNIT
extern RfPageProofDetailV1 rf_page_replay_batch_smgr_v1(const RfPageReplayBatchRequestV1 *request,
														RfPageReplayBatchProofV1 *proof);
#endif

#endif /* CLUSTER_PAGE_REPLAY_BATCH_H */
