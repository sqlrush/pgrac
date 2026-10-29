/* Original Startup/PAGE/service consumer; the namespace recheck leaf is
 * controlled here and exercised with real files in the census target.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool final_tx_probe;
static unsigned final_tx_calls, final_tx_pending, final_tx_fault;

ClusterRecoveryGraphResult cluster_recovery_census_tx_recheck_v1(ClusterRecoveryCensus *, uint32);
ClusterRecoveryGraphResult
cluster_recovery_census_tx_recheck_v1(ClusterRecoveryCensus *cursor, uint32 index)
{
	UT_ASSERT(cursor == (ClusterRecoveryCensus *)&observation_cursor_object);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	if (final_tx_probe) {
		final_tx_calls++;
		UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		if (final_tx_pending != 0) {
			final_tx_pending--; ctrc_driver_waiting = true;
			return CLUSTER_RECOVERY_GRAPH_PENDING;
		}
		if (index == 2) {
			if (final_tx_fault == 1) return CLUSTER_RECOVERY_GRAPH_CHANGED;
			if (final_tx_fault == 2) pg_re_throw();
			if (final_tx_fault == 3) owner_boot++;
		}
		if (final_tx_fault == 4 && index == observation_manifest.tx_record_count)
			return CLUSTER_RECOVERY_GRAPH_RECORD;
	}
	return index == observation_manifest.tx_record_count
		? CLUSTER_RECOVERY_GRAPH_DRAINED : CLUSTER_RECOVERY_GRAPH_RECORD;
}

static void
final_tx_ready(unsigned path)
{
	post_service_ready(path, 0);
	observation_manifest.tx_record_count = 3;
	observation_manifest.root_count = 2;
	forest_terminal_fault = 0; start_tx_record_step = forest_terminal_step;
	final_tx_probe = true; final_tx_calls = final_tx_pending = final_tx_fault = 0;
}

static void
final_tx_after_service(void)
{
	post_service_publish(); final_tx_fault = 1;
}

UT_TEST(test_startup_final_tx_rechecks_complete_forest_before_and_after_service)
{
	for (unsigned path = 0; path < 3; path++) {
		final_tx_ready(path); final_tx_pending = 2;
		retirement_run_startup();
		UT_ASSERT(post_service_polled);
		UT_ASSERT_EQ(final_tx_calls, 10); /* 4 entries twice + 2 pending reads. */
		UT_ASSERT_EQ(ctrc_driver_waits, 2);
		UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
		UT_ASSERT_EQ(claim_releases, 1);
		final_tx_probe = false; post_service_done();
	}
}

UT_TEST(test_startup_final_tx_late_record_error_owner_and_end_retain_original_debt)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		final_tx_ready(0); final_tx_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!post_service_polled);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(claim_releases, 0);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		if (fault == 3) owner_boot--;
		final_tx_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		final_tx_probe = false; post_service_done();
	}
}

UT_TEST(test_startup_final_tx_drift_during_service_cannot_retire_inputs)
{
	for (unsigned path = 0; path < 3; path++) {
		final_tx_ready(path); ctrc_wait_step = final_tx_after_service;
		retirement_run_startup();
		UT_ASSERT(post_service_polled);
		UT_ASSERT_EQ(final_tx_calls, 7);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_tx_start->revoked);
		final_tx_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		final_tx_probe = false; post_service_retained();
	}
}
