/* Original step holds physical storage from before PAGE until final cleanup.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_native_step_fixed_fd_precedes_page_and_covers_all_io)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;

	native_step_fixture(&f, 0);
	result = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED);
	UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
	UT_ASSERT(!ctrc_page_fd_live); UT_ASSERT_EQ(ctrc_read_native_reads, 20);
	UT_ASSERT(result.inserted.wal_flushed); native_install_retained(); native_step_finish(&f);
}

UT_TEST(test_native_step_fixed_fd_open_failure_precedes_page_and_wal)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;

	native_step_fixture(&f, 0); ctrc_page_fd_fault = 1;
	result = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 0);
	UT_ASSERT_EQ(ctrc_read_native_reads, 0); UT_ASSERT_EQ(native_insert_records, 0);
	UT_ASSERT(!native_insert_page_held()); native_step_finish(&f);
}

UT_TEST(test_native_step_fixed_fd_gap_loss_cannot_insert)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;

	native_step_fixture(&f, 0); native_step_fault = 13;
	result = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
	UT_ASSERT(!ctrc_page_fd_live); native_step_finish(&f);
}

UT_TEST(test_native_step_fixed_fd_late_failure_keeps_inserted_debt)
{
	for (unsigned fault = 3; fault <= 4; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;

		native_step_fixture(&f, 0); ctrc_page_fd_fault = fault;
		result = native_step(&f, 2, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT(result.inserted.window.key.window_seq != 0 && result.inserted.wal_flushed);
		UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		UT_ASSERT(!ctrc_page_fd_live); native_install_retained(); native_step_finish(&f);
	}
}

UT_TEST(test_native_step_fixed_fd_error_always_closes_private_storage)
{
	for (unsigned fault = 5; fault <= 6; fault++) {
		CtrcReadNativeFixture f; volatile bool caught = false;

		native_step_fixture(&f, 0); ctrc_page_fd_fault = fault;
		PG_TRY(); { (void)native_step(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT(!ctrc_page_fd_live); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_step_finish(&f);
	}
}

UT_TEST(test_native_step_fixed_fd_close_failure_cannot_claim_target_synced)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;

	native_step_fixture(&f, 0); ctrc_page_fd_fault = 7;
	result = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT(result.inserted.window.key.window_seq != 0 && result.inserted.wal_flushed);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); UT_ASSERT(!ctrc_page_fd_live);
	native_install_retained(); native_step_finish(&f);
}
