/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_durable.inc
 *     Original F4 transition inside the native PAGE executor's FD interval.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_durable.inc
 *
 * NOTES
 *     The real original stamp FSM executes. Faults bracket only its durable
 *     call; Startup/GES, writer, WAL and storage transport remain unit leaves.
 *-------------------------------------------------------------------------
 */
static bool native_durable_active;
static unsigned native_durable_calls, native_durable_fault;

bool
cluster_recovery_stamp_durable_v1(const ClusterRecoveryStampHandleV1 *handle)
{
	bool transitioned;

	if (!native_durable_active) return native_stamp_original_durable(handle);
	native_durable_calls++;
	UT_ASSERT(ctrc_page_fd_live && !ctrc_page_fd_invalid && native_insert_page_held());
	UT_ASSERT_EQ(ctrc_page_fd_closes, 0); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_read_native_reads, 20);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[handle->key.target_thread].stamp.phase,
		CLUSTER_RECOVERY_STAMP_INSERTED);
	if (native_durable_fault == 100) return false;
	if (native_durable_fault == 101) pg_re_throw();
	transitioned = native_stamp_original_durable(handle);
	UT_ASSERT(transitioned);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[handle->key.target_thread].stamp.phase,
		CLUSTER_RECOVERY_STAMP_DURABLE);
	switch (native_durable_fault) {
	case 1: ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++; break;
	case 2: native_stamp_source.root.file_txn_seq++; break;
	case 3: native_fpi_region.checkpoint_owner.tx_result.processed_count++; break;
	case 4: native_install_fault = 21; break;
	case 5: native_install_flush = 0; break;
	case 6: native_install_timeline++; break;
	case 7: ctrc_page_fd_invalid = true; break;
	case 102: pg_re_throw(); break;
	}
	return transitioned;
}

static void
native_durable_fixture(CtrcReadNativeFixture *f, unsigned source, PGAlignedBlock *canonical)
{
	native_step_fixture(f, source);
	native_durable_active = true; native_durable_calls = native_durable_fault = 0;
	UT_ASSERT_EQ(pread(f->main_fd, canonical->data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
}

static RfPageCtrcStepResultV1
native_durable_step(CtrcReadNativeFixture *f, const PGAlignedBlock *canonical)
{
	return rf_page_recovery_ctrc_replays_itl_graph_durable_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		canonical == NULL ? NULL : canonical->data, native_fpi_physical, 2, 1024 * 1024);
}

static void
native_durable_finish(CtrcReadNativeFixture *f)
{
	native_durable_active = false; native_step_finish(f);
}

static void
native_durable_retained(void)
{
	ClusterRecoveryStampCaptureV1 capture;
	uint16 thread = native_stamp_source.selected.source.claim.identity.origin_thread_id;

	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && native_insert_page_held());
	UT_ASSERT(native_fpi_region.thread_replay[thread].stamp.phase == CLUSTER_RECOVERY_STAMP_INSERTED
		|| native_fpi_region.thread_replay[thread].stamp.phase == CLUSTER_RECOVERY_STAMP_DURABLE);
	UT_ASSERT(native_fpi_allocation == NULL);
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	UT_ASSERT_EQ(capture.count, 1); UT_ASSERT(capture.caller_owned);
	UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture),
		native_fpi_region.thread_replay[thread].stamp.failed
		? CLUSTER_RECOVERY_STAMP_INVALID : CLUSTER_RECOVERY_STAMP_PENDING);
}

UT_TEST(test_native_durable_marks_under_original_page_and_fd)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical, expected, actual;
		RfPageCtrcStepResultV1 result; unsigned releases, grants;

		native_durable_fixture(&f, source, &canonical);
		releases = test_tx_ir_releases; grants = test_tx_ir_grants;
		result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
		UT_ASSERT_EQ(native_durable_calls, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
			result.inserted.record_start, result.inserted.record_end));
		UT_ASSERT(!cluster_recovery_stamp_fpi_inserted_current_v1(&result.inserted.window,
			result.inserted.record_start, result.inserted.record_end));
		native_install_expected(&result.inserted, &expected);
		UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(&actual, &expected, sizeof(actual)) == 0);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_durable_snapshot_is_exact_and_read_only)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
	ClusterRecoveryStampHandleV1 changed; ClusterRecoveryStampSlotV1 saved;
	int pid;

	native_durable_fixture(&f, 0, &canonical); result = native_durable_step(&f, &canonical);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
	saved = native_fpi_region.thread_replay[128].stamp;
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(NULL, 1, 2));
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
		result.inserted.record_start + 8, result.inserted.record_end));
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
		result.inserted.record_start, result.inserted.record_end + 8));
	changed = result.inserted.window; changed.owner.writer.epoch++;
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&changed,
		result.inserted.record_start, result.inserted.record_end));
	pid = MyProcPid; MyProcPid++;
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
		result.inserted.record_start, result.inserted.record_end)); MyProcPid = pid;
	CritSectionCount++;
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
		result.inserted.record_start, result.inserted.record_end)); CritSectionCount--;
	UT_ASSERT(memcmp(&saved, &native_fpi_region.thread_replay[128].stamp, sizeof(saved)) == 0);
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&result.inserted.window));
	UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
		result.inserted.record_start, result.inserted.record_end));
	native_durable_retained(); native_durable_finish(&f);
}

UT_TEST(test_native_durable_transition_refusal_keeps_inserted_debt)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;

	native_durable_fixture(&f, 0, &canonical); native_durable_fault = 100;
	result = native_durable_step(&f, &canonical);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(native_durable_calls, 1);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_durable_retained(); native_durable_finish(&f);
}

UT_TEST(test_native_durable_post_transition_drift_fails_exact_window)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;

		native_durable_fixture(&f, 0, &canonical); native_durable_fault = fault;
		result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(native_durable_calls, 1);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_DURABLE);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_durable_error_keeps_original_phase_and_debt)
{
	for (unsigned fault = 101; fault <= 102; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; volatile bool caught = false;

		native_durable_fixture(&f, 0, &canonical); native_durable_fault = fault;
		PG_TRY(); { (void)native_durable_step(&f, &canonical); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(native_durable_calls, 1);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase,
			fault == 101 ? CLUSTER_RECOVERY_STAMP_INSERTED : CLUSTER_RECOVERY_STAMP_DURABLE);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_durable_close_failure_retains_durable_failed_debt)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;

	native_durable_fixture(&f, 0, &canonical); ctrc_page_fd_fault = 7;
	result = native_durable_step(&f, &canonical);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(native_durable_calls, 1);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_DURABLE);
	UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); native_durable_retained(); native_durable_finish(&f);
}

UT_TEST(test_native_durable_requires_successful_sync_and_postread)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;

		native_durable_fixture(&f, 0, &canonical);
		if (fault == 0) ctrc_page_fd_fault = 4;
		if (fault == 1) native_install_fault = 10;
		if (fault == 2) { native_install_fault = 1; native_install_at = 20; }
		if (fault == 3) { native_install_fault = 2; native_install_stage = 2; }
		result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(native_durable_calls, 0);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_durable_preserves_old_entry_and_noop_contracts)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;

		native_durable_fixture(&f, 0, &canonical);
		if (mode == 2) {
			for (unsigned i = 0; i < 8; i++) {
				ClusterItlSlotData *slot = &ClusterPageGetItlSlots(canonical.data)[i];
				slot->flags = i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
				slot->commit_scn = i % 2 ? 900 : InvalidScn;
			}
			ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &canonical);
			UT_ASSERT_EQ(pread(f.main_fd, canonical.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		}
		result = mode == 0 ? native_step(&f, 2, 1024 * 1024)
			: native_durable_step(&f, mode == 1 ? NULL : &canonical);
		UT_ASSERT_EQ(result.phase, mode == 0 ? RF_PAGE_CTRC_STEP_TARGET_SYNCED
			: mode == 1 ? RF_PAGE_CTRC_STEP_REFUSED : RF_PAGE_CTRC_STEP_UNCHANGED);
		UT_ASSERT_EQ(native_durable_calls, 0);
		if (mode == 0) {
			UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
			UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&result.inserted.window,
				result.inserted.record_start, result.inserted.record_end));
		} else UT_ASSERT_EQ(native_insert_records, 0);
		native_durable_finish(&f);
	}
}
