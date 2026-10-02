/*-------------------------------------------------------------------------
 *
 * cluster_space_recovery.h
 *    Protected canonical SPACE reservation recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_space_recovery.h
 *
 * NOTES
 *    Borrows original recovery authorities; never grants lifecycle authority.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_RECOVERY_H
#define CLUSTER_SPACE_RECOVERY_H

#include "cluster/cluster_thread_recovery_fabric.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "storage/buf.h"

typedef struct ClusterSpaceRecoveryBatchV1 ClusterSpaceRecoveryBatchV1;
struct ClusterRecoveryFencePlan;

extern bool cluster_space_recovery_preflight_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												const ClusterThreadRecoveryAuthorityV1 *sources,
												uint32 count, ClusterSpaceRecoveryBatchV1 **out);
extern bool cluster_space_recovery_apply_v1(ClusterSpaceRecoveryBatchV1 *batch);
/* Startup only, after the original cold fence plan is committed; requires
 * its complete foreign-origin set (not a founder or historical authority).
 * Borrows that plan and the sealed SIDE inputs until batch destruction;
 * neither an origin number nor a caller-provided mode grants mutation.
 * Currently accepts ADVANCE only with an already durable identity page. */
extern bool cluster_space_recovery_cold_preflight_v1(const RfSideOnlinePlanV1 *side,
													 struct ClusterRecoveryFencePlan *fence,
													 ClusterSpaceRecoveryBatchV1 **out);
extern void cluster_space_recovery_destroy_v1(ClusterSpaceRecoveryBatchV1 **batch);
/* The buffer manager calls this only inside its synchronous recovery flush.
 * It proves exact bytes and all retained sources, not a caller-supplied LSN. */
extern bool cluster_space_recovery_flush_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch,
													  Buffer buffer);
extern Size cluster_space_recovery_scratch_held_v1(const ClusterSpaceRecoveryBatchV1 *batch);
extern bool cluster_space_recovery_preflight_operation_v1(void *arg,
														  const RfSideOnlineOperationV1 *operation);
extern bool cluster_space_recovery_applied_operation_v1(void *arg,
														const RfSideOnlineOperationV1 *operation);

#endif
