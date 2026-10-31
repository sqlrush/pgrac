/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_hot_refs.inc
 *     Same original Start consumes local HOT topology after PAGE cleanup.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static bool hot_refs_check;
static unsigned hot_refs_calls, hot_refs_fault;

bool
cluster_ctrc_recovery_heap_hot_references_v1(const char *image, BlockNumber block)
{
#ifdef TEST_CLUSTER_STARTUP_NATIVE_CTRC
	if (startup_native_images) {
		startup_native_image_calls[2]++;
		return test_startup_native_ctrc_hot(image, block);
	}
#endif
	if (!hot_refs_check) return true;
	hot_refs_calls++;
	UT_ASSERT(image != NULL);
	UT_ASSERT_EQ(block, 22 + hot_refs_calls);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	UT_ASSERT_EQ(heap_refs_calls, hot_refs_calls);
	if (hot_refs_calls == 2) {
		if (hot_refs_fault == 1) return false;
		if (hot_refs_fault == 2) owner_boot++;
		if (hot_refs_fault == 3) native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++;
		if (hot_refs_fault == 4) pg_re_throw();
	}
	return true;
}

static void
hot_refs_ready(bool unchanged)
{
	heap_refs_ready(unchanged);
	hot_refs_check = true; hot_refs_calls = hot_refs_fault = 0;
}

static void
hot_refs_finish(void)
{
	hot_refs_check = false;
	heap_refs_finish();
}

UT_TEST(test_startup_final_hot_refs_use_each_actual_block_after_page_cleanup)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		hot_refs_ready(unchanged != 0); retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(hot_refs_calls, 2);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		hot_refs_finish();
	}
}

UT_TEST(test_startup_final_hot_refs_late_refusal_or_input_drift_prevents_handoff)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++)
		for (unsigned fault = 1; fault <= 3; fault++) {
			hot_refs_ready(unchanged != 0); hot_refs_fault = fault;
			retirement_run_startup();
			UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
			UT_ASSERT_EQ(hot_refs_calls, 2);
			UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
			UT_ASSERT_EQ(pending_storage_closes, 0);
			hot_refs_finish();
		}
}

UT_TEST(test_startup_final_hot_refs_error_retains_original_fds_and_pins)
{
	hot_refs_ready(false); hot_refs_fault = 4; retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
	UT_ASSERT_EQ(hot_refs_calls, 2);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	hot_refs_finish(); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_final_hot_refs_repeat_never_caches_topology_success)
{
	unsigned steps;
	hot_refs_ready(false); retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps;
	hot_refs_calls = heap_refs_calls = final_projection_calls = 0; hot_refs_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(hot_refs_calls, 2); UT_ASSERT_EQ(prefix_steps, steps);
	UT_ASSERT_EQ(pending_storage_created, 2);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
	hot_refs_finish();
}
