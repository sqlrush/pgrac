/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_census.inc
 *     Original all-source scope owns and retires its local census first.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 * Original scope/ROOT/claim/namespace/holder lifetime executes. Startup/GES
 * and the already tested synchronous census producer are controlled leaves.
 * This is custody integration, never native qualification or COMPLETE.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_relmap_retention_census.h"
extern bool cluster_wal_inputs_relmap_census_prepare_v1(ClusterWalInputsV1 *inputs,
	size_t root_budget, size_t memory_budget, size_t payload_budget,
	const ClusterRelmapRetentionCensus **out);

static bool scope_census_active;
static unsigned scope_census_mode, scope_census_calls, scope_census_destroys;
static void *scope_census_live;
static int scope_census_fd;
static ClusterRecoveryRelmapSourceV1 scope_census_source;

bool
cluster_relmap_retention_census_execute_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	if (!scope_census_active) return false;
	scope_census_calls++;
	UT_ASSERT(root_budget == 4194304 && memory_budget == 1048576 && payload_budget == 131072);
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(source));
	UT_ASSERT(source->input_index == 0 && source->input_count == 2 && source->replay_count == 2);
	scope_census_source = *source;
	scope_census_live = calloc(1, 1);
	UT_ASSERT(scope_census_live != NULL);
	*out = scope_census_live;
	if (scope_census_mode == 2)
		ereport(ERROR, (errmsg("controlled census producer ERROR after local allocation")));
	if (scope_census_mode == 3) ctrc_inputs_work.generation++;
	return scope_census_mode != 1;
}

void
cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census)
{
	if (census == NULL) return;
	UT_ASSERT(scope_census_active && census == scope_census_live);
	/* Moving dependent cleanup past ANY original holder/fd release breaks
	 * these independent original observations, not just a callback count. */
	UT_ASSERT_EQ(ctrc_set_held, 2);
	UT_ASSERT(inputs_pin_held);
	UT_ASSERT(fcntl(scope_census_fd, F_GETFD) >= 0);
	free(census);
	scope_census_live = NULL;
	scope_census_destroys++;
}

#ifdef SCOPE_CENSUS_PARENT_CONTROL
/* Open-only forwarding has no original-scope adoption/latch/failure duty.
 * It is a component sensitivity control, not a native runtime RED. */
static bool
scope_census_parent_prepare(ClusterWalInputsV1 *inputs, size_t root_budget,
	size_t memory_budget, size_t payload_budget, const ClusterRelmapRetentionCensus **out)
{
	ClusterRecoveryRelmapSourceV1 source;
	if (inputs == NULL || out == NULL || (const void *)out == (const void *)inputs
		|| *out != NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return false;
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	return source.scope != NULL && cluster_relmap_retention_census_execute_native_episode(
		&source, root_budget, memory_budget, payload_budget, (ClusterRelmapRetentionCensus **)out);
}
#define cluster_wal_inputs_relmap_census_prepare_v1 scope_census_parent_prepare
#endif

static void
scope_census_setup(ClusterWalInputsV1 **inputs)
{
	relmap_participant_fixture(inputs, false);
	relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(*inputs, &scope_census_fd), 0);
	scope_census_active = true;
	scope_census_mode = scope_census_calls = scope_census_destroys = 0;
	scope_census_live = NULL;
}

static void
scope_census_finish(ClusterWalInputsV1 **inputs)
{
	ctrc_set_finish(inputs);
	/* Only the deliberately nonowning parent control can leave this leaf
	 * allocation behind. Never invoke the original destructor after fd close. */
	free(scope_census_live); scope_census_live = NULL;
	scope_census_active = false;
	relmap_declared = false;
}

static bool
scope_census_prepare(ClusterWalInputsV1 *inputs, const ClusterRelmapRetentionCensus **out)
{
	return cluster_wal_inputs_relmap_census_prepare_v1(inputs, 4194304, 1048576, 131072, out);
}

UT_TEST(test_scope_census_borrowed_result_retires_before_all_original_protections)
{
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterRelmapRetentionCensus *borrowed = NULL, *second = NULL;
	scope_census_setup(&inputs);
	UT_ASSERT(scope_census_prepare(inputs, &borrowed));
	UT_ASSERT(borrowed != NULL && borrowed == scope_census_live);
	UT_ASSERT_EQ(scope_census_destroys, 0);
	UT_ASSERT(!scope_census_prepare(inputs, &second));
	UT_ASSERT(second == NULL);
	UT_ASSERT_EQ(scope_census_calls, 1);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(inputs == NULL);
	UT_ASSERT_EQ(scope_census_destroys, 1);
	UT_ASSERT(scope_census_live == NULL);
	UT_ASSERT_EQ(ctrc_set_held, 0);
	UT_ASSERT(!inputs_pin_held);
	UT_ASSERT_EQ(fcntl(scope_census_fd, F_GETFD), -1);
	scope_census_finish(&inputs);
}

UT_TEST(test_scope_census_partial_error_and_final_drift_keep_original_debt)
{
	for (unsigned mode = 1; mode <= 3; mode++) {
		ClusterWalInputsV1 *inputs = NULL;
		const ClusterRelmapRetentionCensus *borrowed = NULL;
		volatile bool caught = false;
		scope_census_setup(&inputs); scope_census_mode = mode;
		test_capture_error_level = true;
		PG_TRY(); { UT_ASSERT(!scope_census_prepare(inputs, &borrowed)); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(caught, mode == 2);
		UT_ASSERT(borrowed == NULL);
		UT_ASSERT_EQ(scope_census_destroys, 1);
		UT_ASSERT_EQ(relmap_failed, 1);
		UT_ASSERT(ctrc_set_held == 2 && inputs_pin_held);
		UT_ASSERT(fcntl(scope_census_fd, F_GETFD) >= 0);
		UT_ASSERT(!scope_census_prepare(inputs, &borrowed));
		UT_ASSERT_EQ(scope_census_calls, 1);
		scope_census_finish(&inputs);
	}
}

UT_TEST(test_scope_census_invalid_entry_preserves_source_and_fresh_attempt)
{
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterRelmapRetentionCensus *borrowed = NULL;
	ClusterRecoveryRelmapSourceV1 before, after;
	scope_census_setup(&inputs); before = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(!scope_census_prepare((void *)1, &borrowed));
	UT_ASSERT(!scope_census_prepare(inputs, (const ClusterRelmapRetentionCensus **)inputs));
	CritSectionCount = 1;
	UT_ASSERT(!scope_census_prepare(inputs, &borrowed)); CritSectionCount = 0;
	MyBackendType = B_BG_WORKER;
	UT_ASSERT(!scope_census_prepare(inputs, &borrowed)); MyBackendType = B_STARTUP;
	borrowed = (void *)1;
	UT_ASSERT(!scope_census_prepare(inputs, &borrowed)); UT_ASSERT(borrowed == (void *)1);
	borrowed = NULL; after = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(scope_census_calls, 0); UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT(scope_census_prepare(inputs, &borrowed));
	scope_census_finish(&inputs);
}

UT_TEST(test_scope_census_first_owner_error_is_once_and_retains_original_holders)
{
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterRelmapRetentionCensus *borrowed = NULL;
	volatile bool caught = false;
	scope_census_setup(&inputs);
	test_capture_error_level = true;
	ctrc_replay_current_override = relmap_episode_terminal_error_current;
	PG_TRY(); { (void)scope_census_prepare(inputs, &borrowed); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	ctrc_replay_current_override = ctrc_set_original_current;
	test_capture_error_level = false;
	UT_ASSERT(caught && borrowed == NULL);
	UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT_EQ(scope_census_calls, 0);
	UT_ASSERT(!scope_census_prepare(inputs, &borrowed));
	UT_ASSERT(ctrc_set_held == 2 && inputs_pin_held);
	scope_census_finish(&inputs);
}

static void
scope_census_run(void)
{
	UT_RUN(test_scope_census_borrowed_result_retires_before_all_original_protections);
	UT_RUN(test_scope_census_partial_error_and_final_drift_keep_original_debt);
	UT_RUN(test_scope_census_invalid_entry_preserves_source_and_fresh_attempt);
	UT_RUN(test_scope_census_first_owner_error_is_once_and_retains_original_holders);
}
#ifdef SCOPE_CENSUS_PARENT_CONTROL
#undef cluster_wal_inputs_relmap_census_prepare_v1
#endif
