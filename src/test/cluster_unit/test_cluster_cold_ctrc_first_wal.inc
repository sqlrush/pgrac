/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_first_wal.inc
 *     Original Startup consumes the complete native first-record scan.
 *     Native buffer scan/PAGE/census/TT leaves are controlled here.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static unsigned startup_first_late_fault;

static void
startup_first_late(void)
{
	if (startup_first_calls != 2) return;
	if (startup_first_late_fault == 0) startup_first_floor.dirty = 1;
	if (startup_first_late_fault == 1) owner_boot++;
	if (startup_first_late_fault == 2) pg_re_throw();
	if (startup_first_late_fault == 3) startup_first_available = false;
}

UT_TEST(test_startup_first_wal_empty_rechecks_all_native_buffers)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		final_projection_ready(false); final_projection_check = false;
		if (empty) { prefix_count = 0; ctrc_wait_step = reference_terminals_empty_probe; }
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(startup_first_calls, 2);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_first_wal_missing_or_nonempty_keeps_original_debt)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		final_projection_ready(false); final_projection_check = false;
		if (fault == 0) startup_first_available = false;
		if (fault == 1) startup_first_floor.dirty = 1;
		if (fault == 2) startup_first_floor.foreign = 1;
		if (fault == 3) startup_first_floor.unattributed = 1;
		if (fault == 4) startup_first_floor.floor = 17;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(startup_first_calls, 1);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_first_wal_late_failure_revalidates_original_owner)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		final_projection_ready(false); final_projection_check = false;
		startup_first_late_fault = fault; startup_first_step = startup_first_late;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(startup_first_calls, 2);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_first_wal_repeat_cannot_reuse_empty_observation)
{
	unsigned steps, opens;

	final_projection_ready(false); final_projection_check = false;
	retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	startup_first_floor.unattributed = startup_first_floor.dirty = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT(info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(startup_first_calls, 3);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}
