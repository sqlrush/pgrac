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
#include "cluster/cluster_space_storage.h"
#include "storage/buf.h"

typedef struct ClusterSpaceRecoveryBatchV1 ClusterSpaceRecoveryBatchV1;
struct ClusterRecoveryFencePlan;

/* Native implementation of the storage owner's fixed cold entry. */
extern bool cluster_space_recovery_cold_relation_install_v1(const ClusterSpaceIdentityKey *key,
															const ClusterSpaceRecoveryInput *inputs,
															const ClusterSpaceColdSourceV1 *sources,
															uint32 count, uint32 through);

/* Read-only qualification for the original cold COMMIT deletion owner.
 * Both SPACE components must already be this exact, durable tombstone.
 * Borrows the original whole cold owner set; does not grant foreign SIDE
 * replay or recovery-window closure, and never writes target bytes. */
extern bool cluster_space_recovery_cold_drop_already_v1(const ClusterSpaceIdentityKey *key,
														const ClusterSpaceRecoveryInput *input,
														const ClusterSpaceColdSourceV1 *source);

extern bool cluster_space_recovery_preflight_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												const ClusterThreadRecoveryAuthorityV1 *sources,
												uint32 count, ClusterSpaceRecoveryBatchV1 **out);
extern bool cluster_space_recovery_apply_v1(ClusterSpaceRecoveryBatchV1 *batch);
/* Install one target through a position in its complete canonical input
 * order. Uses the same preflighted batch and original source authority;
 * success does not certify completion of the whole batch. */
extern bool cluster_space_recovery_apply_through_v1(ClusterSpaceRecoveryBatchV1 *batch,
													uint32 target, uint32 through);
/* Startup only, after the original cold fence plan is committed; requires
 * its complete foreign-origin set (not a founder or historical authority).
 * Borrows that plan and the sealed SIDE inputs until batch destruction;
 * neither an origin number nor a caller-provided mode grants mutation.
 * Accepts CREATE plus ADVANCE; ADVANCE alone needs a durable identity page.
 * TRUNCATE and DROP need their separate structural retirement owner. */
extern bool cluster_space_recovery_cold_preflight_v1(const RfSideOnlinePlanV1 *side,
													 struct ClusterRecoveryFencePlan *fence,
													 ClusterSpaceRecoveryBatchV1 **out);
extern void cluster_space_recovery_destroy_v1(ClusterSpaceRecoveryBatchV1 **batch);
/* The buffer manager calls this only inside its synchronous recovery flush.
 * It proves exact bytes and all retained sources, not a caller-supplied LSN. */
extern bool cluster_space_recovery_flush_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch,
													  Buffer buffer);
struct xl_smgr_truncate;
/* Original native truncation only, while the batch holds both SPACE pages
 * and the exact current structural input. A naked LSN never grants this. */
extern bool cluster_space_recovery_truncate_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch,
														 const struct xl_smgr_truncate *truncate);
extern Size cluster_space_recovery_scratch_held_v1(const ClusterSpaceRecoveryBatchV1 *batch);
extern bool cluster_space_recovery_preflight_operation_v1(void *arg,
														  const RfSideOnlineOperationV1 *operation);
extern bool cluster_space_recovery_applied_operation_v1(void *arg,
														const RfSideOnlineOperationV1 *operation);

#endif
