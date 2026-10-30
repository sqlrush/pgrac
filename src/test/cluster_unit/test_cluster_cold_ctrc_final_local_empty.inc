/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_final_local_empty.inc
 *     Original Startup final local-directory wait after complete readback.
 *     Native directory/PAGE/TT transport leaves remain controlled here.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static unsigned final_local_wait_fault;

static void
final_local_empty_ready(unsigned path)
{
	final_projection_ready(path == 1); final_projection_check = false;
	if (path == 2) { prefix_count = 0; ctrc_wait_step = reference_terminals_empty_probe; }
}

static void
final_local_empty_wait_drift(void)
{
	if (final_local_wait_fault == 0) completed_readback_mode = 1;
	if (final_local_wait_fault == 1) owner_boot++;
	if (final_local_wait_fault == 2) startup_first_floor.dirty = 1;
	if (final_local_wait_fault == 3) ShutdownRequestPending = true;
}

UT_TEST(test_startup_final_local_empty_covers_fpi_unchanged_and_zero_page_wal)
{
	for (unsigned path = 0; path < 3; path++) {
		final_local_empty_ready(path);
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(final_local_empty_calls, 1);
		UT_ASSERT_EQ(local_empty_calls, path == 0 ? 2 : 0);
		UT_ASSERT_EQ(startup_first_calls, 2);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_final_local_wait_rechecks_complete_pages_on_same_fds)
{
	for (unsigned path = 0; path < 3; path++) {
		final_local_empty_ready(path); final_local_empty_mode = 1;
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(final_local_empty_calls, 3);
		UT_ASSERT_EQ(ctrc_driver_waits, 2);
		UT_ASSERT_EQ(startup_first_calls, 6);
		if (path == 0) UT_ASSERT_EQ(completed_readback_calls, 6);
		UT_ASSERT_EQ(prefix_steps, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_created, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_final_local_refusal_error_and_late_owner_loss_keep_debt)
{
	for (unsigned fault = 2; fault <= 5; fault++) {
		final_local_empty_ready(0); final_local_empty_mode = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(final_local_empty_calls, 1);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
	final_local_empty_ready(2); final_local_empty_mode = 2;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(final_local_empty_calls, 1);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_final_local_wait_cannot_cache_readback_first_or_owner)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		final_local_empty_ready(0); final_local_empty_mode = 1;
		final_local_wait_fault = fault; ctrc_driver_wait_step = final_local_empty_wait_drift;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(final_local_empty_calls, 1);
		UT_ASSERT_EQ(ctrc_driver_waits, 1);
		if (fault == 0) UT_ASSERT_EQ(completed_readback_calls, 3);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_final_local_repeat_cannot_reuse_empty_directory)
{
	unsigned steps, opens;
	final_local_empty_ready(0); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	final_local_empty_mode = 2;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT(info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(final_local_empty_calls, 2);
	UT_ASSERT_EQ(completed_readback_calls, 4);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}
