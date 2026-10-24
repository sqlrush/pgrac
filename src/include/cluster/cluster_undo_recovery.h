/*-------------------------------------------------------------------------
 * cluster_undo_recovery.h
 *    Qualified canonical UNDO paths for the original recovery owner.
 *-------------------------------------------------------------------------
 * Author: SqlRush <sqlrush@gmail.com>
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
	bool wal_synced;
} ClusterUndoRecoveryScopeV1;

extern bool cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
												 const ClusterThreadRecoveryAuthorityV1 *authority,
												 const RfSideOnlinePlanV1 *plan);
extern void cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope);
/* Replay one already sealed C0 inside Startup's original pass-2 window,
 * under separate current writer/target owners.
 * Success covers the exact durable TT slot, target-native ABORT bit and its
 * derived projection, not cleanup, N4 or whole-source recovery completion. */
extern bool cluster_undo_recovery_abort_replay_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
												  const RfPageOnlineRecordIdentityV1 *identity,
												  const ClusterThreadRecoveryAuthorityV1 *writer,
												  const ClusterThreadRecoveryAuthorityV1 *target);
/* The SIDE projection shares this original sealed-source recovery scope. */
extern bool cluster_undo_recovery_origin_authorized_v1(int origin_node);
/* Native cold consumers also require the original cold serial mode and
 * an executable recovery plan; PI inspection cannot qualify this scope. */
extern bool cluster_undo_recovery_cold_origin_authorized_v1(int origin_node);
/* Read-only exact selected-source match inside that same original cold scope.
 * Reuses the original ROOT-selected native source accessor and rechecks the
 * authority afterwards. No scope is acquired, extended, or granted here. */
extern bool
cluster_undo_recovery_cold_source_matches_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr native_redo);
/* Only the already-durable exact C0 target's top-level native ABORT bit.
 * This does not authorize an origin scope or other native operations. */
extern bool cluster_undo_recovery_abort_clog_authorized_v1(int origin_node, TransactionId xid);
/* Exact C0-derived ABORT only, after native TT/CLOG completion. */
extern bool cluster_undo_recovery_abort_projection_authorized_v1(int origin_node,
																 TransactionId xid);
/* Sync the original cold source once in this scope, never a local WAL LSN.
 * A successful physical sync remains subject to the full original authority. */
extern bool cluster_undo_recovery_cold_source_sync_v1(int origin_node);
extern bool cluster_undo_recovery_cold_source_synced_v1(int origin_node);
extern bool cluster_undo_recovery_multixact_page_retired_v1(int origin_node, XLogRecPtr source_lsn,
															XLogRecPtr source_end_lsn, bool members,
															uint32 page);
extern ClusterUndoPathIntent cluster_undo_recovery_intent_for_owner(uint8 owner);
extern int cluster_undo_recovery_path_resolve_v1(uint8 owner, uint32 segment, char *path,
												 size_t size);

#endif
