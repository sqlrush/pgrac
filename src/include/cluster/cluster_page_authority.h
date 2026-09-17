/*-------------------------------------------------------------------------
 *
 * cluster_page_authority.h
 *    STOP-03/04/05 binding for STOP-06 PAGE target protection.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_AUTHORITY_H
#define CLUSTER_PAGE_AUTHORITY_H

#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_page_guard.h"
#include "cluster/cluster_page_install.h"
#include "cluster/cluster_wal_retention.h"

#define CLUSTER_PAGE_AUTHORITY_INTERFACE_V1 1

typedef enum RfPageAuthorityVerdictV1 {
	RF_PAGE_AUTHORITY_OK = 0,
	RF_PAGE_AUTHORITY_INVALID_ARGUMENT = 1,
	RF_PAGE_AUTHORITY_ROOT_STALE = 2,
	RF_PAGE_AUTHORITY_GENERATION_STALE = 3,
	RF_PAGE_AUTHORITY_SERIAL_NOT_HELD = 4,
	RF_PAGE_AUTHORITY_FENCE_STALE = 5,
	RF_PAGE_AUTHORITY_RETENTION_STALE = 6,
	RF_PAGE_AUTHORITY_SOURCE_GAP = 7,
	RF_PAGE_AUTHORITY_NO_STABLE_BASE = 8,
	RF_PAGE_AUTHORITY_CLASS_UNKNOWN = 9,
	RF_PAGE_AUTHORITY_IDENTITY_MISMATCH = 10,
	RF_PAGE_AUTHORITY_INCARNATION_MISMATCH = 11,
	RF_PAGE_AUTHORITY_VERSION_RULE_MISSING = 12,
	RF_PAGE_AUTHORITY_CONTRIBUTOR_INCOMPLETE = 13,
	RF_PAGE_AUTHORITY_SIDE_INCOMPLETE = 14,
	RF_PAGE_AUTHORITY_WOULD_BLOCK = 15,
	RF_PAGE_AUTHORITY_CANCELLED = 16,
	RF_PAGE_AUTHORITY_OOM = 17,
	RF_PAGE_AUTHORITY_INTERNAL = 18
} RfPageAuthorityVerdictV1;

typedef struct RfPageAuthorityTargetV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 expected_result;
	const RfPageStableBaseProofV1 *stable_base;
	const RfPagePinnedSourceV1 *source;
	const RfContributorVectorV1 *contributors;
} RfPageAuthorityTargetV1;

typedef struct RfPageAuthorityBatchRequestV1 {
	const RfPageAuthorityTargetV1 *targets;
	uint32 target_count;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	const ClusterRecoveryDutyKey *duties;
	const ClusterControlRootReadToken *root_tokens;
	uint32 participant_count;
	uint32 flags;
} RfPageAuthorityBatchRequestV1;

typedef struct RfPageAuthorityPreflightV1 RfPageAuthorityPreflightV1;
typedef struct RfPageAuthorityGuardV1 RfPageAuthorityGuardV1;

typedef struct RfPageInstallAuthorityAdapterV1 {
	RfPageInstallAuthorityOpsV1 ops;
	RfPageAuthorityPreflightV1 *preflight;
	ClusterRecoverySerialGuard *serial_guard;
	RfPageAuthorityGuardV1 *guard;
	bool proof_published;
} RfPageInstallAuthorityAdapterV1;

extern RfPageAuthorityVerdictV1
rf_page_authority_batch_preflight_wait_v1(const RfPageAuthorityBatchRequestV1 *request,
										  int timeout_ms,
										  RfPageAuthorityPreflightV1 **out_preflight);
extern RfPageAuthorityVerdictV1
rf_page_authority_batch_promote_nowait_v1(RfPageAuthorityPreflightV1 *preflight,
										  ClusterRecoverySerialGuard *serial_guard,
										  RfPageAuthorityGuardV1 **out_guard);
extern RfPageAuthorityVerdictV1
rf_page_authority_batch_revalidate_nowait_v1(const RfPageAuthorityGuardV1 *guard,
											 ClusterRecoverySerialGuard *serial_guard);
extern bool
rf_page_authority_preflight_matches_target_v1(const RfPageAuthorityPreflightV1 *preflight,
											  const RfPageIdentityV1 *identity,
											  const uint8 incarnation[16]);
extern void rf_page_authority_preflight_destroy_v1(RfPageAuthorityPreflightV1 **preflight);
extern void rf_page_authority_guard_release_v1(RfPageAuthorityGuardV1 **guard);
extern bool rf_page_install_authority_adapter_init_v1(RfPageAuthorityPreflightV1 *preflight,
													  ClusterRecoverySerialGuard *serial_guard,
													  RfPageInstallAuthorityAdapterV1 *adapter);

#endif /* CLUSTER_PAGE_AUTHORITY_H */
