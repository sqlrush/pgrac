/* Pending FPI readback executes the original native PAGE/F4/five-read chain.
 * Startup/GES/WAL and raw transport remain controlled unit leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_fpi_readback_v1(
	const struct ClusterRecoveryStartV1 *, const ClusterColdCtrcWorkV1 *,
	const ClusterRecoveryCtrcReplaySetSourceV1 *, RfPageCtrcAuthorityV1 *,
	const RfPageCtrcFpiInsertV1 *, ClusterSharedFsPageV1 *, Size, Size);

static RfPageProofDetailV1
native_readback(CtrcReadNativeFixture *f, RfPageCtrcHeldStepV1 *held)
{
	return rf_page_recovery_ctrc_replays_fpi_readback_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &held->result.inserted,
		held->storage, 2, 1024 * 1024);
}

static RfPageCtrcHeldStepV1
native_readback_ready(CtrcReadNativeFixture *f, unsigned source)
{
	PGAlignedBlock canonical;
	RfPageCtrcHeldStepV1 held;
	native_durable_fixture(f, source, &canonical); held = native_held_step(f, &canonical);
	UT_ASSERT_EQ(held.result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
	UT_ASSERT(rf_page_authority_ctrc_replays_unpromote_v1(f->page.page.authority));
	UT_ASSERT(!native_insert_page_held());
	native_step_hashes = 0; /* Original hash leaf checks the deliberate PAGE gap. */
	return held;
}

static void
native_readback_finish(CtrcReadNativeFixture *f, RfPageCtrcHeldStepV1 *held)
{
	UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_install_writes, 1);
	UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_page_fd_opens, 1);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT(cluster_shared_fs_page_close_v1(held->storage)); native_durable_finish(f);
}

UT_TEST(test_native_readback_repromotes_same_page_and_reads_original_fds_only)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, source);
		RfPageCtrcFpiInsertV1 fixed = held.result.inserted;
		unsigned releases = test_tx_ir_releases;
		UT_ASSERT_EQ(native_readback(&f, &held), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(native_insert_page_held()); UT_ASSERT_EQ(ctrc_read_native_reads, 25);
		UT_ASSERT(memcmp(&held.result.inserted, &fixed, sizeof(fixed)) == 0);
		UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&fixed.window,
			fixed.record_start, fixed.record_end));
		UT_ASSERT_EQ(test_tx_ir_releases, releases); native_readback_finish(&f, &held);
	}
}

UT_TEST(test_native_readback_never_reinstalls_before_or_changed_result_bytes)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock changed;
		RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
		UT_ASSERT_EQ(pread(f.main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		if (fault == 0) ((PageHeader)changed.data)->pd_block_scn = held.result.inserted.image.version.before.mutation_token;
		if (fault == 1) changed.data[BLCKSZ - 1]++;
		if (fault == 2) PageSetLSNPreserveOrigin(changed.data, held.result.inserted.record_end + 8);
		if (fault == 3) UT_ASSERT(PageSetLSNOrigin(changed.data, 2));
		if (fault == 4) memset(changed.data, 0, BLCKSZ);
		if (fault == 4) UT_ASSERT_EQ(pwrite(f.main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		else ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &changed);
		UT_ASSERT(native_readback(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(native_install_writes, 1); native_readback_finish(&f, &held);
	}
}

UT_TEST(test_native_readback_rechecks_immutable_inputs_after_every_read)
{
	for (unsigned read = 1; read <= 5; read++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
		native_install_fault = 1; native_install_at = 20 + read;
		UT_ASSERT(native_readback(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		native_readback_finish(&f, &held);
	}
}

UT_TEST(test_native_readback_page_collision_waits_without_failing_window)
{
	CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
	RfPageGuardPreflightV1 preflight;
	UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &preflight));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
	native_step_foreign_blocker = true;
	/* The shared unit bitmap represents another process here; its old hash
	 * leaf observes that bitmap rather than the caller-held lock list. */
	native_step_hashes = 1;
	UT_ASSERT_EQ(native_readback(&f, &held), RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(ctrc_read_native_reads, 20);
	UT_ASSERT(!native_fpi_region.thread_replay[128].stamp.failed);
	rf_page_guard_release_v1(&native_insert_blocker); native_step_foreign_blocker = false;
	native_step_hashes = 0;
	UT_ASSERT_EQ(native_readback(&f, &held), RF_PAGE_PROOF_DETAIL_OK);
	native_readback_finish(&f, &held);
}

UT_TEST(test_native_readback_rejects_lost_fd_writer_flush_and_bad_entry)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
		if (fault == 0) ctrc_page_fd_invalid = true;
		if (fault == 1) native_install_fault = 21;
		if (fault == 2) native_install_flush = 0;
		if (fault == 3) native_install_timeline++;
		if (fault == 4) held.result.inserted.record_end += 8;
		if (fault == 5) native_insert_caller_lock = true;
		if (fault == 6) held.storage = NULL;
		if (fault == 7) ctrc_inputs_work.counts.subject_count++;
		UT_ASSERT(native_readback(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(ctrc_read_native_reads, 20);
		native_insert_caller_lock = false; held.storage = (void *)&ctrc_page_fd_live;
		native_readback_finish(&f, &held);
	}
}

UT_TEST(test_native_readback_error_keeps_storage_and_original_window_debt)
{
	CtrcReadNativeFixture f; volatile bool caught = false;
	RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
	ctrc_page_fd_fault = 5;
	PG_TRY(); { (void)native_readback(&f, &held); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && ctrc_page_fd_live);
	UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
	native_readback_finish(&f, &held);
}

UT_TEST(test_native_readback_busy_cache_waits_without_invalidating_a_buffer)
{
	CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
	native_cache_active = native_cache_present = true; native_cache_retire_calls = 0;
	UT_ASSERT_EQ(native_readback(&f, &held), RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT(native_cache_present); UT_ASSERT_EQ(native_cache_retire_calls, 0);
	UT_ASSERT_EQ(ctrc_read_native_reads, 20);
	UT_ASSERT(!native_fpi_region.thread_replay[128].stamp.failed);
	native_cache_present = false;
	UT_ASSERT(rf_page_authority_ctrc_replays_unpromote_v1(f.page.page.authority));
	UT_ASSERT_EQ(native_readback(&f, &held), RF_PAGE_PROOF_DETAIL_OK);
	native_cache_active = false; native_readback_finish(&f, &held);
}
