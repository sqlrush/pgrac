/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_startup.c
 *     Original Startup core with roots from the same held input episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_startup.c
 * NOTES
 *     Installed Startup/fence and pass-1 plan construction remain fixtures.
 *     No scope, authority or completion state is acquired by this bridge.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "test_cluster_recovery_scope_startup.h"
#include "../../backend/cluster/cluster_cold_recovery_startup.c"

/* This fixture has exactly the founder and one foreign failed origin. */
static ColdRoot scope_startup_roots[2];
static uint32 scope_startup_count;

bool
test_scope_startup_attach(ClusterColdTypedV1 *typed,
	const ClusterRecoveryRelmapSourceV1 *sources)
{
	if (scope_startup_count != 0 || typed == NULL || sources == NULL || typed->plan == NULL
		|| typed->replay_roots != NULL || typed->participant_count != lengthof(scope_startup_roots)
		|| typed->replay_count != typed->participant_count)
		return false;
	memset(scope_startup_roots, 0, sizeof(scope_startup_roots));
	for (uint32 i = 0; i < typed->participant_count; ++i) {
		ColdRoot *root = &scope_startup_roots[i];
		const ClusterWalSourceRef *source = &typed->sources[i];
		const ClusterWalInputV1 *selected = &sources[i].selected;

		/* These are the original input owner's by-value checkpoint/window,
		 * not a cold-role resample or a fabricated RECOVERY_REQUIRED token. */
		if (!cluster_wal_inputs_relmap_source_current_v1(&sources[i])
			|| memcmp(&selected->source, source, sizeof(*source)) != 0
			|| cluster_control_root_read_canonical(source->claim.identity.origin_thread_id,
				&source->claim.identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
				&root->root, &root->token) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| memcmp(&root->root, &selected->checkpoint, sizeof(root->root)) != 0)
			return false;
		root->checkpoint_start = selected->checkpoint_start;
		root->native_checkpoint = selected->native_checkpoint;
		memcpy(root->native_window, selected->native_window, sizeof(root->native_window));
		root->checkpoint_captured = true;
	}
	typed->replay_roots = scope_startup_roots;
	typed->replay_root_count = typed->replay_count;
	memcpy(typed->observer.storage_uuid, typed->sources[0].claim.identity.storage_uuid, 16);
	typed->observer.database_incarnation = typed->sources[0].claim.database_incarnation;
	scope_startup_count = typed->participant_count;
	cluster_cold_replay_window_enter_v1();
	return true;
}

bool
test_scope_startup_origin(uint16 index, uint16 *thread,
	ClusterControlRootSnapshot *root, ClusterControlRootReadToken *token)
{
	/* The founder is index zero; the fence's foreign order starts at one. */
	if (scope_startup_count == 0 || (uint32)index + 1 >= scope_startup_count)
		return false;
	*root = scope_startup_roots[index + 1].root;
	*token = scope_startup_roots[index + 1].token;
	*thread = root->identity.origin_thread_id;
	return true;
}

void
test_scope_startup_detach(ClusterColdTypedV1 *typed)
{
	cluster_cold_replay_window_leave_v1();
	typed->replay_roots = NULL;
	typed->replay_root_count = 0;
	scope_startup_count = 0;
}
