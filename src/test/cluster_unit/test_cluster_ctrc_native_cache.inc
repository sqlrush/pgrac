/* Original graph-sync step must not leave a cached predecessor behind.
 * The native bufmgr adapter is tested separately; this transport leaf binds
 * its result and late loss to the real PAGE/F4/file installation consumer.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool native_cache_active, native_cache_present;
static unsigned native_cache_mode, native_cache_retire_calls, native_cache_observations;
static unsigned native_cache_reappear;
static PGAlignedBlock native_cache_before;

ClusterCtrcPageCacheResultV1
cluster_bufmgr_ctrc_page_cache_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber blockno, const RfPageVersionV1 *before,
	const char *before_image)
{
	if (!native_cache_active) return CLUSTER_CTRC_PAGE_CACHE_ABSENT;
	UT_ASSERT(start == tx_inputs_start && work == &ctrc_inputs_work);
	UT_ASSERT(authority == native_insert_live->page.page.authority);
	UT_ASSERT(memcmp(space, &native_insert_live->space, sizeof(*space)) == 0);
	UT_ASSERT(blockno == 7 && ctrc_page_fd_live);
	UT_ASSERT(memcmp(before, &native_insert_live->page.target.expected_result, sizeof(*before)) == 0);
	UT_ASSERT(CritSectionCount == 0 && inputs_pin_held && ctrc_set_held == 2);
	if (before_image != NULL) {
		UT_ASSERT(native_insert_page_held());
		native_cache_retire_calls++;
		UT_ASSERT(native_cache_present && native_insert_records == 0);
		UT_ASSERT(memcmp(before_image, native_cache_before.data, BLCKSZ) == 0);
		if (native_cache_mode == 2) return CLUSTER_CTRC_PAGE_CACHE_WAIT;
		if (native_cache_mode == 3) return CLUSTER_CTRC_PAGE_CACHE_REFUSED;
		if (native_cache_mode == 4) pg_re_throw();
		native_cache_present = false;
	} else {
		native_cache_observations++;
		if ((native_cache_reappear == 1 && native_insert_records != 0)
			|| (native_cache_reappear == 2 && native_install_syncs != 0))
			native_cache_present = true;
	}
	return native_cache_present ? CLUSTER_CTRC_PAGE_CACHE_WAIT : CLUSTER_CTRC_PAGE_CACHE_ABSENT;
}

static void
native_cache_fixture(CtrcReadNativeFixture *f, unsigned source)
{
	native_step_fixture(f, source); native_durable_active = false;
	native_cache_active = native_cache_present = true;
	native_cache_mode = native_cache_retire_calls = native_cache_observations = native_cache_reappear = 0;
	UT_ASSERT_EQ(pread(f->main_fd, native_cache_before.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
}

static void
native_cache_finish(CtrcReadNativeFixture *f)
{
	native_cache_active = native_cache_present = false; native_step_finish(f);
}

UT_TEST(test_native_cache_retires_exact_predecessor_before_bufferless_fpi)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;
		native_cache_fixture(&f, source);
		result = native_unchanged_step(&f, &native_cache_before, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
		UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(!native_cache_present);
		UT_ASSERT(native_cache_observations > 0 && result.inserted.wal_flushed);
		UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		native_durable_retained(); native_cache_finish(&f);
	}
}

UT_TEST(test_native_cache_busy_waits_without_wal_or_duty_retirement)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result; unsigned releases;
	native_cache_fixture(&f, 0); native_cache_mode = 2; releases = test_tx_ir_releases;
	result = native_unchanged_step(&f, &native_cache_before, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_WAIT);
	UT_ASSERT_EQ(native_cache_retire_calls, 1); UT_ASSERT(native_cache_present);
	UT_ASSERT_EQ(native_insert_records + native_install_writes + native_install_syncs, 0);
	UT_ASSERT_EQ(result.inserted.window.key.window_seq, 0);
	UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_cache_finish(&f);
}

UT_TEST(test_native_cache_refusal_and_error_keep_original_owners)
{
	for (unsigned mode = 3; mode <= 4; mode++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result = {0}; volatile bool caught = false;
		native_cache_fixture(&f, 0); native_cache_mode = mode;
		PG_TRY(); { result = native_unchanged_step(&f, &native_cache_before, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, mode == 4);
		if (!caught) UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT(native_cache_present); UT_ASSERT_EQ(native_insert_records + native_install_writes, 0);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_cache_finish(&f);
	}
}

UT_TEST(test_native_cache_reappearance_keeps_exact_inserted_window)
{
	for (unsigned after = 1; after <= 2; after++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;
		native_cache_fixture(&f, 0); native_cache_reappear = after;
		result = native_unchanged_step(&f, &native_cache_before, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT(native_cache_present && result.inserted.window.key.window_seq != 0);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT_EQ(native_insert_records, 1); native_durable_retained(); native_cache_finish(&f);
	}
}

UT_TEST(test_native_cache_noop_keeps_matching_cache_and_old_components)
{
	CtrcReadNativeFixture f; PGAlignedBlock terminal; RfPageCtrcStepResultV1 result;
	native_unchanged_fixture(&f, 0, &terminal);
	native_cache_active = native_cache_present = true;
	native_cache_retire_calls = native_cache_observations = 0;
	result = native_unchanged_step(&f, &terminal, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED);
	UT_ASSERT(native_cache_present); UT_ASSERT_EQ(native_cache_retire_calls + native_cache_observations, 0);
	native_cache_finish(&f);
	native_cache_fixture(&f, 0);
	result = native_graph_step(&f, &native_cache_before);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED);
	UT_ASSERT(native_cache_present); UT_ASSERT_EQ(native_cache_retire_calls + native_cache_observations, 0);
	native_cache_finish(&f);
}

UT_TEST(test_native_cache_stale_canonical_refuses_before_cache_mutation)
{
	CtrcReadNativeFixture f; PGAlignedBlock changed; RfPageCtrcStepResultV1 result;
	native_cache_fixture(&f, 0); changed = native_cache_before; changed.data[BLCKSZ - 1]++;
	result = native_unchanged_step(&f, &changed, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT(native_cache_present); UT_ASSERT_EQ(native_cache_retire_calls + native_cache_observations, 0);
	UT_ASSERT_EQ(native_insert_records + native_install_writes, 0); native_cache_finish(&f);
}
