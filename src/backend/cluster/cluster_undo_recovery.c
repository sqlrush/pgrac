/*-------------------------------------------------------------------------
 * cluster_undo_recovery.c
 *    Canonical UNDO access within the original failed-origin recovery scope.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_shared_fs.h"

static ClusterUndoRecoveryScopeV1 *active_scope;

static bool
undo_scope_current(const ClusterUndoRecoveryScopeV1 *scope)
{
	const ClusterThreadRecoveryAuthorityV1 *a;
	ClusterProtectedSetIdentityV1 storage;

	if (scope == NULL || (a = scope->authority) == NULL || a->duty == NULL
		|| a->root_snapshot == NULL || a->root_token == NULL || a->serial_guard == NULL
		|| (a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_ONLINE
			&& a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
		|| !cluster_peer_mode_enabled() || !cluster_undo_gcs_coherence
		|| scope->duty.system_identifier != GetSystemIdentifier()
		|| memcmp(&scope->duty, a->duty, sizeof(scope->duty)) != 0
		|| memcmp(&scope->root_token, a->root_token, sizeof(scope->root_token)) != 0
		|| a->root_snapshot->checkpoint_tli != scope->cut.timeline_id
		|| a->root_snapshot->tail_tli != scope->cut.timeline_id
		|| a->root_snapshot->checkpoint_lower_lsn != scope->cut.scan_begin_inclusive
		|| a->root_snapshot->validated_tail_lsn_exclusive != scope->cut.scan_end_exclusive
		|| cluster_thread_recovery_authority_revalidate_nowait_v1(a) != CLUSTER_THREAD_AUTHORITY_OK
		|| !cluster_shared_fs_get_protected_set_identity(&storage)
		|| storage.backend_id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		|| memcmp(storage.storage_uuid, scope->duty.storage_uuid, 16) != 0)
		return false;
	return rf_side_online_plan_source_matches_v1(scope->plan, scope->duty.system_identifier,
		scope->duty.storage_uuid, &scope->cut);
}

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
	const ClusterThreadRecoveryAuthorityV1 *authority, const RfSideOnlinePlanV1 *plan)
{
	ClusterUndoRecoveryScopeV1 candidate = {0};

	if (active_scope != NULL || scope == NULL || authority == NULL || plan == NULL
		|| authority->duty == NULL || authority->root_token == NULL
		|| authority->root_snapshot == NULL || authority->duty->origin_thread_id == 0
		|| authority->duty->origin_thread_id > 128)
		return false;
	candidate.authority = authority;
	candidate.plan = plan;
	candidate.duty = *authority->duty;
	candidate.root_token = *authority->root_token;
	candidate.cut.failed_thread = authority->duty->origin_thread_id;
	candidate.cut.timeline_id = authority->root_snapshot->checkpoint_tli;
	candidate.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	candidate.cut.scan_begin_inclusive = authority->root_snapshot->checkpoint_lower_lsn;
	candidate.cut.scan_end_exclusive = authority->root_snapshot->validated_tail_lsn_exclusive;
	if (candidate.cut.timeline_id == 0 || candidate.cut.scan_begin_inclusive == InvalidXLogRecPtr
		|| candidate.cut.scan_end_exclusive <= candidate.cut.scan_begin_inclusive
		|| !undo_scope_current(&candidate))
		return false;
	cluster_undo_smgr_fd_cache_reset();
	*scope = candidate;
	active_scope = scope;
	return true;
}

void
cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	if (scope != NULL && active_scope == scope) {
		active_scope = NULL;
		memset(scope, 0, sizeof(*scope));
		cluster_undo_smgr_fd_cache_reset();
	}
}

ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	/* Keep an expired or mismatched active request on the qualified path so
	 * its resolver refuses instead of silently materializing a local copy. */
	return active_scope != NULL ? CLUSTER_UNDO_PATH_RECOVERY_SHARED
		: cluster_undo_intent_for_owner(owner);
}

bool
cluster_undo_recovery_origin_authorized_v1(int origin_node)
{
	return origin_node >= 0 && origin_node < 128 && active_scope != NULL
		&& active_scope->duty.origin_thread_id == origin_node + 1
		&& undo_scope_current(active_scope);
}

int
cluster_undo_recovery_path_resolve_v1(uint8 owner, uint32 segment, char *path, size_t size)
{
	char resolved[MAXPGPATH];

	if (active_scope == NULL || owner == 0 || owner != active_scope->duty.origin_thread_id
		|| segment == 0 || ((segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1 != owner
		|| path == NULL || size == 0 || !undo_scope_current(active_scope)
		|| cluster_shared_fs_undo_path_resolve(owner, segment, resolved, sizeof(resolved)) != 0
		|| strlen(resolved) >= size)
		return -1;
	strlcpy(path, resolved, size);
	return 0;
}
#endif
