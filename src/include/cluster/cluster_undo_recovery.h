/*-------------------------------------------------------------------------
 * cluster_undo_recovery.h
 *    Qualified canonical UNDO paths for the original recovery owner.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RECOVERY_H
#define CLUSTER_UNDO_RECOVERY_H

#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/storage/cluster_undo_alloc.h"

typedef struct ClusterUndoRecoveryScopeV1 {
	const ClusterThreadRecoveryAuthorityV1 *authority;
	const RfSideOnlinePlanV1 *plan;
	ClusterRecoveryDutyKey duty;
	ClusterControlRootReadToken root_token;
	RfContributorStreamCutV1 cut;
} ClusterUndoRecoveryScopeV1;

extern bool cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
	const ClusterThreadRecoveryAuthorityV1 *authority, const RfSideOnlinePlanV1 *plan);
extern void cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope);
/* The SIDE projection shares this original sealed-source recovery scope. */
extern bool cluster_undo_recovery_origin_authorized_v1(int origin_node);
extern ClusterUndoPathIntent cluster_undo_recovery_intent_for_owner(uint8 owner);
extern int cluster_undo_recovery_path_resolve_v1(uint8 owner, uint32 segment,
	char *path, size_t size);

#endif
