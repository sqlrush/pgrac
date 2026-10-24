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
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_tail.h"
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
/* Retained C0 carried by this node's original restart source. The target
 * still requires its separate current foreign recovery authority. */
extern bool cluster_undo_recovery_abort_replay_native_v1(const RfSideOnlinePlanV1 *plan,
	uint32 index, const RfPageOnlineRecordIdentityV1 *identity,
	const ClusterControlRootSnapshot *writer_root, const ClusterControlRootReadToken *writer_token,
	const ClusterThreadRecoveryAuthorityV1 *target);
/* Original post-EOR TX_CLOSURE caller only, after its C0 WAL flush and
 * durable TT stamp have cleared F4. Keep source, target reservation and exact
 * held FD through native top ABORT and derived projection readback. This
 * restores foreign-origin native state only; it grants no general origin,
 * UNDO, cleanup, checkpoint or OPEN authority. */
extern bool cluster_undo_recovery_abort_project_tx_v1(struct ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, ClusterUndoSmgrRecoveryStampV1 stamp);
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
/* Nonzero process-local original scope serial after the same complete source
 * checks. Never reused within this process, even for the same stack address
 * and identical authority. Zero on refusal; not a durability/terminal grant. */
extern uint64 cluster_undo_recovery_cold_scope_id_v1(const ClusterWalSourceRef *source,
													 const ClusterControlRootSnapshot *checkpoint,
													 XLogRecPtr native_redo);
/* Same original cold source/ROOT/nonreused serial plus its own immutable
 * plan's single-physical/bound-source origin check. No caller plan or new
 * scope, retention, era/base or completion permission is accepted/created. */
extern bool
cluster_undo_recovery_cold_single_source_v1(const ClusterWalSourceRef *source,
											const ClusterControlRootSnapshot *checkpoint,
											XLogRecPtr native_redo, uint64 scope_serial);
/* Provisional, read-only original execution-owner input. No census read pin
 * or replacement scope is acquired. The callback must neither publish shared
 * state nor mutate/retire its borrowed owner or inputs; all observations are
 * invalid unless the complete visit returns true. The same original cold
 * scope/serial, full ROOT/source and checkpoint/window are checked after I/O.
 * False/ERROR leaves original ownership and retention obligations to caller.
 * This supplies checkpoint/WAL input, never terminal/base/era/completion or
 * release authority. Captured metadata is valid only during each callback. */
typedef bool (*ClusterUndoRecoveryCheckpointVisitorV1)(
	const ClusterWalRecoveryCheckpointV1 *checkpoint, const ClusterControlRootSnapshot *root,
	uint64 scope_serial, struct XLogReaderState *reader, void *arg);
extern bool cluster_undo_recovery_cold_checkpoint_visit_v1(
	int origin_node, ClusterUndoRecoveryCheckpointVisitorV1 visitor, void *arg);
/* Match an actual completed native COMMIT/ABORT carrier to this original
 * cold scope's own immutable recovery plan, never a caller-supplied plan or
 * terminal boolean. Requires the captured round serial and exact source.
 * Structural source seal only, not native base/era or completion authority. */
struct XLogReaderState;
extern bool cluster_undo_recovery_cold_terminal_matches_v1(
	const ClusterWalSourceRef *source, const ClusterControlRootSnapshot *checkpoint,
	XLogRecPtr native_redo, uint64 scope_serial, const struct XLogReaderState *reader);
/* Same original scope/source/redo/round; unavailable is UINT32_MAX, never an
 * empty completed source. Counts only that scope's own sealed native plan. */
extern uint32
cluster_undo_recovery_cold_terminal_count_v1(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr native_redo, uint64 scope_serial);
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
