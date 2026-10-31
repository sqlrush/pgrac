/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_final_projection.inc
 *     Original Startup final projection consumer. PAGE/file transport and
 *     the separately tested pure projector are controlled leaves here.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static unsigned final_projection_calls, final_projection_fault;
static bool final_projection_check;

ClusterCtrcItlCleanoutApplyResult
cluster_ctrc_recovery_itl_page_image_v1(const ClusterColdTtSlotInputV1 *subjects,
	Size count, const char *before, char *after)
{
#ifdef TEST_CLUSTER_STARTUP_NATIVE_CTRC
	if (startup_native_images) {
		startup_native_image_calls[0]++;
		return test_startup_native_ctrc_project(subjects, count, before, after);
	}
#endif
	memcpy(after, before, BLCKSZ);
	if (!final_projection_check)
		return CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL;
	final_projection_calls++;
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	UT_ASSERT_EQ(count, native_ctrc_work.counts.subject_count);
	UT_ASSERT_EQ(count, 1);
	if (count != 1) return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
	UT_ASSERT_EQ(subjects[0].key.xid, native_ctrc_work.entries[0].key.xid);
	UT_ASSERT_EQ(subjects[0].slot.status, TT_SLOT_ABORTED);
	UT_ASSERT(subjects[0].slot.flags == native_ctrc_work.entries[0].slot.flags);
	UT_ASSERT(memcmp(&subjects[0].slot.first_undo_block,
		&native_ctrc_work.entries[0].slot.first_undo_block, sizeof(UBA)) == 0);
	if (final_projection_calls == 2) {
		if (final_projection_fault == 1) return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
		if (final_projection_fault == 2) return CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN;
		if (final_projection_fault == 3) after[BLCKSZ - 1] ^= 1;
		if (final_projection_fault == 4) owner_boot++;
		if (final_projection_fault == 5) native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++;
		if (final_projection_fault == 6) pg_re_throw();
	}
	return CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL;
}

static void
final_projection_ready(bool unchanged)
{
	if (unchanged) unchanged_custody_ready(); else completed_readback_ready();
	observation_ready(); prefix_setup_targets(); prefix_unchanged = unchanged;
	ctrc_wait_step = retirement_service_probe; tx_wait_destroy_base = input_destroys;
	final_projection_check = true;
	final_projection_calls = final_projection_fault = 0;
}

UT_TEST(test_startup_final_projection_consumes_all_pages_after_page_cleanup)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		ClusterColdTtSlotInputV1 original;
		final_projection_ready(unchanged != 0);
		original = native_ctrc_work.entries[0];
		retirement_run_startup();
		final_projection_check = false;
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(final_projection_calls, 2);
		UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
		UT_ASSERT_EQ(pending_storage_mask, 3);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_final_projection_refuses_unresolved_rewritten_and_byte_drift)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++)
		for (unsigned fault = 1; fault <= 3; fault++) {
			final_projection_ready(unchanged != 0); final_projection_fault = fault;
			retirement_run_startup(); final_projection_check = false;
			UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
			UT_ASSERT_EQ(final_projection_calls, 2);
			UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
			UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
			retirement_local_cleanup();
		}
}

UT_TEST(test_startup_final_projection_rechecks_original_owner_and_full_work)
{
	for (unsigned fault = 4; fault <= 5; fault++) {
		final_projection_ready(false); final_projection_fault = fault;
		retirement_run_startup(); final_projection_check = false;
		UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
		UT_ASSERT_EQ(final_projection_calls, 2);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_final_projection_error_keeps_original_fds_and_debt)
{
	final_projection_ready(false); final_projection_fault = 6;
	retirement_run_startup(); final_projection_check = false;
	UT_ASSERT(!retirement_service_reached && cluster_recovery_cold_start->revoked);
	UT_ASSERT_EQ(final_projection_calls, 2);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup(); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_final_projection_wait_reuses_same_fd_without_new_wal)
{
	final_projection_ready(false); completed_readback_mode = 4;
	retirement_run_startup(); final_projection_check = false;
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(final_projection_calls, 2);
	UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
	UT_ASSERT_EQ(completed_readback_calls, 4);
	retirement_local_cleanup();
}

UT_TEST(test_startup_final_projection_repeat_does_not_cache_success)
{
	unsigned steps;
	final_projection_ready(false); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; final_projection_calls = 0; final_projection_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	final_projection_check = false;
	UT_ASSERT_EQ(final_projection_calls, 2); UT_ASSERT_EQ(prefix_steps, steps);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_final_projection_budget_is_reserved_before_first_page)
{
	Size old_bytes, extra;
	final_projection_ready(false);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	old_bytes = sizeof(ClusterColdCtrcPageProgressV1)
		+ 2 * sizeof(ClusterColdCtrcPageCompletedV1) + 2 * cluster_shared_fs_page_bytes_v1();
	extra = native_ctrc_work.counts.subject_count * sizeof(ClusterColdTtSlotInputV1)
		+ 2 * sizeof(PGAlignedBlock);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, old_bytes + extra));
	final_projection_check = false;
	UT_ASSERT_EQ(prefix_pages, 0); UT_ASSERT_EQ(prefix_steps, 0);
	UT_ASSERT_EQ(pending_storage_created, 0);
	UT_ASSERT(observation_scope_live);
	retirement_local_cleanup();
}
