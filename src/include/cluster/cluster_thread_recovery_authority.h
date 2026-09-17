/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_authority.h
 *    STOP-03/04/05 owner bundle for online thread recovery.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_THREAD_RECOVERY_AUTHORITY_H
#define CLUSTER_THREAD_RECOVERY_AUTHORITY_H

#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_wal_retention.h"

typedef enum ClusterThreadRecoveryAuthorityResultV1 {
	CLUSTER_THREAD_AUTHORITY_OK = 0,
	CLUSTER_THREAD_AUTHORITY_INVALID = 1,
	CLUSTER_THREAD_AUTHORITY_ROOT_STALE = 2,
	CLUSTER_THREAD_AUTHORITY_SERIAL_STALE = 3,
	CLUSTER_THREAD_AUTHORITY_FENCE_STALE = 4,
	CLUSTER_THREAD_AUTHORITY_PIN_STALE = 5
} ClusterThreadRecoveryAuthorityResultV1;

typedef enum ClusterThreadRecoveryRootFinalizeResultV1 {
	CLUSTER_THREAD_ROOT_FINALIZE_OK = 0,
	CLUSTER_THREAD_ROOT_FINALIZE_ALREADY_COMPLETE = 1,
	CLUSTER_THREAD_ROOT_FINALIZE_RETRY = 2,
	CLUSTER_THREAD_ROOT_FINALIZE_BLOCKED = 3,
	CLUSTER_THREAD_ROOT_FINALIZE_CLEANUP_UNCERTAIN = 4,
	CLUSTER_THREAD_ROOT_FINALIZE_INVALID = 5
} ClusterThreadRecoveryRootFinalizeResultV1;

typedef struct ClusterThreadRecoveryAuthorityV1 {
	const ClusterRecoveryDutyKey *duty;
	const ClusterControlRootSnapshot *root_snapshot;
	const ClusterControlRootReadToken *root_token;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	ClusterRecoverySerialGuard *serial_guard;
} ClusterThreadRecoveryAuthorityV1;

extern bool cluster_thread_recovery_pin_request_build_v1(
	uint16 dead_tid, const ClusterRecoveryDutyKey *duty,
	const ClusterControlRootSnapshot *root_snapshot, const ClusterControlRootReadToken *root_token,
	const ClusterFormationWitnessV1 *formation, const PgracExternalFenceNeedSetV1 *fence_need_set,
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set,
	ClusterWalRetentionInterval *out_interval, ClusterWalRetentionPinThreadRequest *out_request);
extern ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority);
extern bool cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 dead_tid, XLogRecPtr scan_lower,
	XLogRecPtr scan_upper);
extern bool cluster_thread_recovery_root_complete_patch_build_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, XLogRecPtr recovered_through,
	ClusterControlRootPatch *out_patch);
extern ClusterThreadRecoveryRootFinalizeResultV1
cluster_thread_recovery_root_finalize_after_ir_v1(const ClusterThreadRecoveryAuthorityV1 *authority,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootSnapshot *out_snapshot);

#endif /* CLUSTER_THREAD_RECOVERY_AUTHORITY_H */
