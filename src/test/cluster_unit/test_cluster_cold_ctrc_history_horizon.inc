/* Actual Startup + original horizon fold. Native page/TT transport,
 * reports/ProcArray and independent service publication are controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned history_path, history_fault, history_wait_limit;

static void
history_reports_ready(void)
{
	history_local_floor = scn_encode(0, 1000);
	history_epoch = 4; history_members_ok = true; history_fence = history_disabled = false;
	cluster_undo_retention_horizon_enabled = true; history_view_count = CLUSTER_MAX_NODES;
	history_required[0] = 1U << 1;
	history_views[1] = (ClusterUndoHorizonReportView){
		.valid = true, .stable = true, .has_capability = true, .epoch = 4,
		.horizon_scn = scn_encode(1, 200), .recv_at_us = 10000000, .sender_interval_ms = 1000};
	history_expect_wait = false;
}

static void
history_ready(unsigned path)
{
	final_local_empty_ready(path); final_projection_check = false;
	/* Two complete original subjects, including a preexisting COMMITTED
 * one. Reverse physical order is already tested by the original loader. */
	observation_two_ready(); prefix_setup_targets();
	prefix_unchanged = path == 1; if (path == 2) prefix_count = 0;
	completed_readback_calls = completed_readback_mode = 0;
	unchanged_readback_calls = unchanged_readback_mode = 0;
	history_probe = true; history_path = path; history_fault = 0; history_wait_limit = 1;
	history_reports_ready(); retirement_service_reached = false;
	ctrc_wait_step = retirement_service_probe; tx_wait_destroy_base = input_destroys;
	if (path == 2) ctrc_wait_step = reference_terminals_empty_probe;
}

static void
history_resolve_wait(void)
{
	UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	if (ctrc_driver_waits < history_wait_limit) return;
	history_reports_ready();
	if (history_fault == 1) completed_readback_mode = 1;
	if (history_fault == 2) owner_boot++;
	if (history_fault == 3) startup_first_floor.dirty = 1;
	if (history_fault == 4) ShutdownRequestPending = true;
}

UT_TEST(test_startup_history_uses_original_finite_cohort_floor)
{
	for (unsigned path = 0; path < 3; path++) {
		history_ready(path); retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(history_samples, 1); UT_ASSERT_EQ(ctrc_driver_waits, 0);
		UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
	/* Original aborted-only path has no committed-history exit to admit. */
	final_local_empty_ready(0); history_probe = true;
	cluster_undo_retention_horizon_enabled = false;
	retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(history_samples, 0); retirement_local_cleanup();
}

UT_TEST(test_startup_history_missing_equal_stale_reports_wait_without_rebase)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		history_ready(fault % 3); history_expect_wait = true;
		switch (fault) {
		case 0: history_views[1].valid = false; break;
		case 1: history_views[1].stable = false; break;
		case 2: history_views[1].has_capability = false; break;
		case 3: history_views[1].epoch++; break;
		case 4: history_views[1].regression_flagged = true; break;
		case 5: history_views[1].recv_at_us = 1; break;
		case 6: history_views[1].recv_at_us = 10000001; break;
		case 7: history_views[1].horizon_scn = UINT64_MAX; break;
		case 8: history_local_floor = UINT64_MAX; break;
		case 9: history_views[1].horizon_scn = scn_encode(1, 97); break;
		case 10: history_local_floor = scn_encode(0, 96); break;
		case 11: history_view_count--; break;
		case 12: history_members_ok = false; break;
		case 13: history_epoch++; break;
		case 14: history_fence = true; break;
		case 15: cluster_undo_retention_horizon_enabled = false; break;
		case 16: history_disabled = true; break;
		}
		history_wait_limit = fault == 0 ? 129 : 1;
		ctrc_driver_wait_step = history_resolve_wait;
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(ctrc_driver_waits, history_wait_limit);
		UT_ASSERT_EQ(prefix_steps, history_path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_closes, 0); retirement_local_cleanup();
	}
}

UT_TEST(test_startup_history_wait_repeats_physical_checks_and_keeps_debt)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		history_ready(0); history_views[1].valid = false; history_expect_wait = true;
		history_fault = fault; ctrc_driver_wait_step = history_resolve_wait;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(ctrc_driver_waits, 1); UT_ASSERT_EQ(claim_arbitrations, 0);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		if (fault == 2) owner_boot--;
		history_reports_ready(); UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		retirement_local_cleanup();
	}
}

static void history_sample_error(void) { pg_re_throw(); }
static void history_sample_terminal_drift(void) { native_ctrc_work.entries[1].slot.commit_scn++; }

UT_TEST(test_startup_history_sampling_error_and_late_terminal_refuse)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		history_ready(0);
		history_sample_step = fault ? history_sample_terminal_drift : history_sample_error;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(history_samples, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(observation_scope_live && claim_held);
		history_sample_step = NULL; retirement_local_cleanup();
	}
}

static void
history_after_service(void)
{
	post_service_publish(); history_views[1].valid = false; history_expect_wait = true;
	ctrc_driver_wait_step = history_resolve_wait;
}

UT_TEST(test_startup_history_requalifies_after_independent_service)
{
	for (unsigned path = 0; path < 3; path++) {
		history_ready(path); post_service_path = path; post_service_fault = 0; post_service_polled = false;
		ctrc_wait_step = history_after_service; retirement_run_startup();
		UT_ASSERT(post_service_polled); UT_ASSERT_EQ(history_samples, 3);
		UT_ASSERT_EQ(ctrc_driver_waits, 1); UT_ASSERT_EQ(claim_arbitrations, 1);
		UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
	}
}
