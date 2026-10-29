/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_heap_refs.inc
 *     Same original Start consumes the final native tuple-binding check.
 *     PAGE/file/predicate leaves are controlled in this owner fixture.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static bool heap_refs_check;
static unsigned heap_refs_calls, heap_refs_fault;

bool
cluster_ctrc_recovery_heap_itl_references_v1(const char *image)
{
	if (!heap_refs_check) return true;
	heap_refs_calls++;
	UT_ASSERT(image != NULL);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	UT_ASSERT_EQ(final_projection_calls, heap_refs_calls);
	if (heap_refs_calls == 2) {
		if (heap_refs_fault == 1) return false;
		if (heap_refs_fault == 2) owner_boot++;
		if (heap_refs_fault == 3) native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++;
		if (heap_refs_fault == 4) pg_re_throw();
	}
	return true;
}

static void
heap_refs_ready(bool unchanged)
{
	final_projection_ready(unchanged);
	heap_refs_check = true; heap_refs_calls = heap_refs_fault = 0;
}

static void
heap_refs_finish(void)
{
	heap_refs_check = final_projection_check = false;
	retirement_local_cleanup();
}

UT_TEST(test_startup_final_heap_refs_consume_every_same_fd_image)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		heap_refs_ready(unchanged != 0); retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(heap_refs_calls, 2);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		heap_refs_finish();
	}
}

UT_TEST(test_startup_final_heap_refs_refusal_owner_or_work_drift_prevents_handoff)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++)
		for (unsigned fault = 1; fault <= 3; fault++) {
			heap_refs_ready(unchanged != 0); heap_refs_fault = fault;
			retirement_run_startup();
			UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
			UT_ASSERT_EQ(heap_refs_calls, 2);
			UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
			UT_ASSERT_EQ(pending_storage_closes, 0);
			heap_refs_finish();
		}
}

UT_TEST(test_startup_final_heap_refs_error_keeps_original_fds_and_retention)
{
	heap_refs_ready(false); heap_refs_fault = 4; retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
	UT_ASSERT_EQ(heap_refs_calls, 2);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	heap_refs_finish(); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_final_heap_refs_repeat_rechecks_without_new_wal_or_fd)
{
	unsigned steps;
	heap_refs_ready(false); retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; heap_refs_calls = final_projection_calls = 0; heap_refs_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(heap_refs_calls, 2); UT_ASSERT_EQ(prefix_steps, steps);
	UT_ASSERT_EQ(pending_storage_created, 2);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
	heap_refs_finish();
}
