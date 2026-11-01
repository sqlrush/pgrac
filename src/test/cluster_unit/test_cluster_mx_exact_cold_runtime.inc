/* Actual shared cold preload; only original disk/cache scheduling is controlled.
 * A READY result is never itself an operation/FullXid proof. */
static ClusterHistoryDataStep
mx_cold_load(HistoryOriginFixture *f, bool *available)
{
	return cluster_runtime_visibility_mx_history_prepare_data_held(&f->request.locator,
		CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, &f->base.origin.admission,
		&f->base.origin.guard, &f->base.origin.data_root, available);
}
UT_TEST(mx_cold_real_loader_shares_budget_and_observation)
{
	HistoryOriginFixture f; ClusterHistoryColdStats before, after; bool available = false;
	history_origin_init(&f, false); history_cold_acquire(&f);
	history_cold_cache_race = false; history_cold_fault = 0;
	UT_ASSERT_EQ(mx_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY); /* warm */
	test_candidate_data_copy_calls = 0; test_candidate_cold_copy_mask = 1;
	UT_ASSERT_EQ(mx_cold_load(&f, &available), CLUSTER_HISTORY_DATA_DEFERRED);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	available = true; test_candidate_data_copy_calls = 0;
	cluster_runtime_visibility_history_cold_snapshot(&before);
	UT_ASSERT_EQ(mx_cold_load(&f, &available), CLUSTER_HISTORY_DATA_READY);
	UT_ASSERT(!available); UT_ASSERT_EQ(test_candidate_held_count, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1); UT_ASSERT_EQ(test_operation_pin_depth, 0);
	cluster_runtime_visibility_history_cold_snapshot(&after);
	UT_ASSERT_EQ(after.reads - before.reads, 1); UT_ASSERT_EQ(after.read_ns - before.read_ns, 123000);
	test_candidate_data_copy_calls = 0;
	UT_ASSERT_EQ(history_cold_load(&f, &available), CLUSTER_HISTORY_DATA_DEFERRED);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 1); /* budget shared with history/CREATOR */
	cluster_undo_block0_current_cancel(&f.base.origin.guard);
}
UT_TEST(mx_cold_real_loader_rejects_scope_and_post_io_drift)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		HistoryOriginFixture f; bool available = true;
		history_origin_init(&f, false); history_cold_acquire(&f);
		test_candidate_cold_copy_mask = 1; history_cold_fault = 0;
		if (fault == 0) f.request.locator.uba = uba_encode(257, 2, 0, 0);
		if (fault == 1) test_admission_revoked = true;
		if (fault == 2) CritSectionCount = 1;
		if (fault >= 3) history_cold_fault = fault - 2; /* generation/admission/root after I/O */
		UT_ASSERT_EQ(mx_cold_load(&f, &available), CLUSTER_HISTORY_DATA_FAILED);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, fault >= 3 ? 1 : 0);
		UT_ASSERT_EQ(available, fault < 3); UT_ASSERT_EQ(test_operation_pin_depth, 0);
		CritSectionCount = 0; cluster_undo_block0_current_cancel(&f.base.origin.guard);
	}
	history_cold_fault = 0;
}
UT_TEST(mx_cold_real_loader_error_accounts_before_rethrow)
{
	HistoryOriginFixture f; ClusterHistoryColdStats before, after; bool available = true;
	volatile bool caught = false;
	history_origin_init(&f, false); history_cold_acquire(&f);
	test_candidate_cold_copy_mask = 1; test_candidate_data_io_throw = true;
	cluster_runtime_visibility_history_cold_snapshot(&before);
	PG_TRY(); { (void)mx_cold_load(&f, &available); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!available);
	cluster_runtime_visibility_history_cold_snapshot(&after);
	UT_ASSERT_EQ(after.reads - before.reads, 1);
	UT_ASSERT_EQ(after.read_failures - before.read_failures, 1);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	cluster_undo_block0_current_cancel(&f.base.origin.guard);
}
