/* Original native first-record responsibility meets the original Startup.
 * Native write completion/locks and Startup PAGE/census/TT remain controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static void
startup_native_first_ready(bool unchanged, unsigned mode)
{
	final_projection_ready(unchanged); final_projection_check = false;
	test_startup_first_native_begin(&observation_wal_sources[0].source, mode);
	startup_first_use_native = true;
}

static void
startup_native_first_cleanup(void)
{
	startup_first_use_native = false; startup_first_step = NULL;
	retirement_local_cleanup();
	test_startup_first_native_end();
}

UT_TEST(test_startup_native_first_exact_clean_write_releases_only_first_reference)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		ClusterPageWalDirtyFloorV1 floor;

		startup_native_first_ready(unchanged != 0, 0);
		UT_ASSERT_EQ(test_startup_first_native_references(), 2);
		UT_ASSERT(test_startup_first_native_scan(&observation_wal_sources[0].source, &floor));
		UT_ASSERT(floor.dirty == 1 && floor.floor == 0x120 && floor.foreign == 0);
		UT_ASSERT(test_startup_first_native_clear(0));
		UT_ASSERT_EQ(test_startup_first_native_references(), 1); /* latest stays retained */
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(startup_first_calls, 2);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		startup_native_first_cleanup();
	}
}

UT_TEST(test_startup_native_first_failed_write_keeps_debt_and_cannot_revive_start)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		startup_native_first_ready(false, 0);
		UT_ASSERT(!test_startup_first_native_clear(fault));
		UT_ASSERT_EQ(test_startup_first_native_references(), 2);
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(startup_first_calls, 1);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		/* A later legitimate write cannot revive this refused original episode. */
		UT_ASSERT(test_startup_first_native_clear(0));
		UT_ASSERT_EQ(test_startup_first_native_references(), 1);
		UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
		UT_ASSERT_EQ(pending_storage_closes, 0);
		startup_native_first_cleanup();
	}
}

UT_TEST(test_startup_native_first_foreign_and_unattributed_records_remain_obligations)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		ClusterPageWalDirtyFloorV1 floor;

		startup_native_first_ready(false, mode);
		UT_ASSERT(test_startup_first_native_scan(&observation_wal_sources[0].source, &floor));
		UT_ASSERT_EQ(floor.dirty, 1); UT_ASSERT_EQ(floor.floor, 0);
		UT_ASSERT_EQ(floor.foreign, mode == 1); UT_ASSERT_EQ(floor.unattributed, mode == 2);
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		startup_native_first_cleanup();
	}
}

static void
startup_native_first_late_capture(void)
{
	if (startup_first_calls == 2) test_startup_first_native_recapture();
}

UT_TEST(test_startup_native_first_late_native_capture_revokes_without_reopening_fds)
{
	startup_native_first_ready(false, 0);
	UT_ASSERT(test_startup_first_native_clear(0));
	startup_first_step = startup_native_first_late_capture;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(startup_first_calls, 2);
	UT_ASSERT_EQ(test_startup_first_native_references(), 2);
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	startup_native_first_cleanup();
}
