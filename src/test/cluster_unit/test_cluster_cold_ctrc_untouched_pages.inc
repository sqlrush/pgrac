/* Original Startup complete census physical recheck; census I/O is controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool untouched_probe;
static unsigned untouched_calls, untouched_fault, untouched_at;
static void (*untouched_step)(void);

ClusterRecoveryCensusPageDigestV1
cluster_recovery_census_page_digest_v1(ClusterRecoveryCensus *cursor, uint32 index)
{
	ClusterRecoveryCensusPageDigestV1 value = {0};

	UT_ASSERT(cursor == (ClusterRecoveryCensus *)&observation_cursor_object);
	UT_ASSERT(observation_cursor_live && observation_scope_live && index < observation_pages.count);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	value.observed = true;
	memcpy(value.sha256, observation_pages.entries[index].page_sha256, 32);
	if (untouched_probe) {
		untouched_calls++;
		if (untouched_calls == untouched_at) {
			if (untouched_fault == 1) value.sha256[31]++;
			if (untouched_fault == 2) value.observed = false;
			if (untouched_fault == 3) owner_boot++;
			if (untouched_fault == 4) pg_re_throw();
			if (untouched_step != NULL) untouched_step();
		}
	}
	return value;
}

static void
untouched_ready(bool zero)
{
	final_local_empty_ready(0);
	/* One real completed PAGE, plus the other saved physical MAIN page
	 * with no PAGE-WAL target. Zero PAGE still owes both original pages. */
	prefix_count = zero ? 0 : 1;
	if (zero) ctrc_wait_step = reference_terminals_empty_probe;
	untouched_probe = true; untouched_calls = untouched_fault = 0;
	untouched_at = 1; untouched_step = NULL;
}
static void
untouched_done(void)
{
	untouched_probe = false; untouched_step = NULL;
	retirement_local_cleanup();
}
UT_TEST(test_startup_untouched_pages_complete_saved_set_including_zero_page_wal)
{
	for (unsigned zero = 0; zero < 2; zero++) {
		untouched_ready(zero); retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(untouched_calls, zero ? 2 : 1);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		untouched_done();
	}
}
UT_TEST(test_startup_untouched_last_page_drift_or_absence_keeps_original_debt)
{
	for (unsigned zero = 0; zero < 2; zero++) {
		for (unsigned fault = 1; fault <= 2; fault++) {
			untouched_ready(zero); untouched_fault = fault; untouched_at = zero ? 2 : 1;
			retirement_run_startup();
			UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
			UT_ASSERT_EQ(untouched_calls, untouched_at);
			UT_ASSERT(observation_scope_live && observation_cursor_live);
			UT_ASSERT_EQ(pending_storage_closes, 0);
			if (!zero) UT_ASSERT(pending_storage_live);
			untouched_done();
		}
	}
}
UT_TEST(test_startup_untouched_late_owner_loss_or_error_preserves_same_fd)
{
	for (unsigned fault = 3; fault <= 4; fault++) {
		untouched_ready(false); untouched_fault = fault; retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(untouched_calls, 1);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		untouched_done();
	}
}
UT_TEST(test_startup_untouched_repeat_and_local_wait_reread_original_complete_set)
{
	unsigned steps, opens, reads;
	untouched_ready(false); retirement_run_startup();
	UT_ASSERT(retirement_service_reached); UT_ASSERT_EQ(untouched_calls, 1);
	steps = prefix_steps; opens = pending_storage_created; reads = completed_readback_calls;
	untouched_fault = 1; untouched_at = 2; retirement_service_reached = false; retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(untouched_calls, 2); UT_ASSERT(completed_readback_calls > reads);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	untouched_done();
	untouched_ready(false); final_local_empty_mode = 1; retirement_run_startup();
	UT_ASSERT(retirement_service_reached); UT_ASSERT_EQ(ctrc_driver_waits, 2);
	UT_ASSERT_EQ(untouched_calls, 3); UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT_EQ(pending_storage_created, 1);
	untouched_done();
}
