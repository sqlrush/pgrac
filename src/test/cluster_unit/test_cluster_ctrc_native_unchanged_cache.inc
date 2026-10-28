/* Original unchanged-page sync must respect native cache responsibilities.
 * Cache transport is controlled; original PAGE, sync and five-read code run.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
native_unchanged_cache_fixture(CtrcReadNativeFixture *f, unsigned source,
	PGAlignedBlock *canonical)
{
	native_unchanged_fixture(f, source, canonical);
	native_cache_active = native_cache_present = true;
	native_cache_mode = native_cache_retire_calls = native_cache_observations = native_cache_reappear = 0;
	native_cache_before = *canonical;
}

UT_TEST(test_native_unchanged_cache_retires_only_through_original_page_gate)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical, actual;
		RfPageCtrcStepResultV1 result; unsigned releases;
		native_unchanged_cache_fixture(&f, source, &canonical); releases = test_tx_ir_releases;
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED);
		UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(!native_cache_present);
		UT_ASSERT(native_cache_observations > 0);
		UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_read_native_reads, 10);
		UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, canonical.data, BLCKSZ) == 0);
		native_unchanged_no_wal(&result, releases);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
	}
}

UT_TEST(test_native_unchanged_cache_busy_waits_before_sync_without_retiring_duties)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_cache_fixture(&f, source, &canonical); releases = test_tx_ir_releases;
		native_cache_mode = 2;
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_WAIT);
		UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(native_cache_present);
		UT_ASSERT_EQ(native_install_syncs, 0); UT_ASSERT_EQ(ctrc_read_native_reads, 5);
		native_unchanged_no_wal(&result, releases);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
	}
}

UT_TEST(test_native_unchanged_cache_refusal_never_reports_synced)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
	unsigned releases;
	native_unchanged_cache_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
	native_cache_mode = 3;
	result = native_unchanged_step(&f, &canonical, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(native_cache_present);
	UT_ASSERT_EQ(native_install_syncs, 0); UT_ASSERT_EQ(ctrc_read_native_reads, 5);
	native_unchanged_no_wal(&result, releases);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
}

UT_TEST(test_native_unchanged_cache_error_closes_only_private_storage)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result = {0};
	volatile bool caught = false; unsigned releases;
	native_unchanged_cache_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
	native_cache_mode = 4;
	PG_TRY(); { result = native_unchanged_step(&f, &canonical, 1024 * 1024); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(native_cache_present);
	UT_ASSERT_EQ(native_install_syncs, 0); native_unchanged_no_wal(&result, releases);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
}

UT_TEST(test_native_unchanged_cache_reappearance_refuses_after_sync_or_full_postread)
{
	for (unsigned after = 2; after <= 3; after++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_cache_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		native_cache_reappear = after;
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(native_cache_present);
		UT_ASSERT_EQ(native_install_syncs, 1);
		if (after == 3) UT_ASSERT_EQ(ctrc_read_native_reads, 10);
		native_unchanged_no_wal(&result, releases);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
	}
}

UT_TEST(test_native_unchanged_cache_preserves_all_observation_only_entries)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_cache_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		native_cache_mode = 2;
		if (mode == 0) result = native_step(&f, 2, 1024 * 1024);
		else if (mode == 1) result = native_graph_step(&f, &canonical);
		else result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED);
		UT_ASSERT(native_cache_present);
		UT_ASSERT_EQ(native_cache_retire_calls + native_cache_observations, 0);
		UT_ASSERT_EQ(native_install_syncs, 0); native_unchanged_no_wal(&result, releases);
		native_cache_finish(&f);
	}
}
