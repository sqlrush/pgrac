/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_startup.c
 *     Original Startup core with roots from the same held input episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_startup.c
 * NOTES
 *     Installed Startup/fence remain fixtures. The opt-in handoff builds
 *     and seals its original plan from complete original ROOT scans.
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
test_scope_startup_sources_current(ClusterColdTypedV1 *typed, ClusterRecoveryFencePlan *fence)
{
	return typed != NULL && typed->refusal == CLUSTER_COLD_OK
		&& typed->replay_roots == scope_startup_roots && scope_startup_count == 2
		&& typed->replay_root_count == scope_startup_count
		&& (typed->plan == (ClusterColdPlanV1 *)typed
			? cluster_cold_replay_window_active_v1() && cold_replay_sources_current(typed, fence)
			: cluster_cold_typed_sources_current_v1(typed, fence));
}

bool
test_scope_startup_attach_cold(ClusterColdTypedV1 *typed, const ClusterWalInputV1 *sources)
{
	if (scope_startup_count != 0 || typed == NULL || sources == NULL || typed->plan == NULL
		|| typed->replay_roots != NULL || typed->participant_count != lengthof(scope_startup_roots)
		|| typed->replay_count != typed->participant_count)
		return false;
	memset(scope_startup_roots, 0, sizeof(scope_startup_roots));
	for (uint32 i = 0; i < typed->participant_count; ++i) {
		ColdRoot *root = &scope_startup_roots[i];
		const ClusterWalInputV1 *selected = &sources[i];

		if (memcmp(&selected->source, &typed->sources[i], sizeof(selected->source)) != 0
			|| selected->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			|| cluster_control_root_read_canonical(selected->source.claim.identity.origin_thread_id,
				&selected->source.claim.identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
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

ClusterColdDetailV1
test_scope_startup_prepare_plan(ClusterColdTypedV1 *typed,
	const ClusterWalInputV1 *sources, Size budget)
{
	ClusterColdDetailV1 detail;
	ClusterColdDiagV1 diag;

	if (typed == NULL || typed->plan != NULL || scope_startup_count != 0)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	detail = cluster_cold_plan_create_v1(typed->participants,
		typed->participant_count, budget, &typed->plan);
	if (detail == CLUSTER_COLD_OK && !test_scope_startup_attach_cold(typed, sources))
		detail = CLUSTER_COLD_SOURCE_GAP;
	for (uint32 i = 0; detail == CLUSTER_COLD_OK && i < typed->participant_count; ++i) {
		const ColdRoot *root = &scope_startup_roots[i];
		ClusterColdScanResultV1 scan;

		detail = cluster_cold_scan_root_v1(typed->plan, i, &root->root, &root->token,
			false, i != typed->own_participant, NULL, &scan);
		typed->scanned_records += scan.records;
		if (detail == CLUSTER_COLD_OK
			&& memcmp(scan.native_window, root->native_window, sizeof(scan.native_window)) != 0)
			detail = CLUSTER_COLD_SOURCE_GAP;
	}
	if (detail == CLUSTER_COLD_OK)
		detail = cluster_cold_plan_seal_v1(typed->plan, cluster_cold_observe_data_v1,
			&typed->observer, &diag);
	if (detail != CLUSTER_COLD_OK)
		cold_refuse(typed, detail, "scope fixture original plan refused");
	return detail;
}

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
