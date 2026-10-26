/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_step.inc
 *     Continuous native CTRC image preparation, WAL and target installation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_step.inc
 *
 * NOTES
 *     Real native holders/PAGE, Generic/F4 and temporary-file sync/postread.
 *     Start/GES/WAL and native storage transport remain controlled leaves.
 *
 *-------------------------------------------------------------------------
 */
static unsigned native_step_fault, native_step_hashes;
static bool native_step_null_physical;
static PGAlignedBlock *native_graph_changed_input;

static void
native_step_hash_gap(void)
{
	PGAlignedBlock changed;
	if (native_step_hashes++ != 0) return;
	UT_ASSERT(!native_insert_page_held() && !native_insert_caller_lock);
	UT_ASSERT_EQ(CritSectionCount, 0);
	switch (native_step_fault) {
	case 1: ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++; break;
	case 2: native_fpi_physical[1].segment_generation++; break;
	case 3: native_stamp_source.root.file_txn_seq++; break;
	case 4: native_insert_live->space.sequence++; break;
	case 5: native_insert_live->page.target.expected_result.mutation_token++; break;
	case 6: {
		UT_ASSERT_EQ(pread(native_insert_live->main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		changed.data[BLCKSZ - 1]++;
		ctrc_read_native_write(native_insert_live, MAIN_FORKNUM, 7, &changed); break;
	}
	case 7:
		UT_ASSERT(cluster_space_identity_page_encode(&native_insert_live->space, 999, changed.data, BLCKSZ));
		ctrc_read_native_write(native_insert_live, SPACE_FORKNUM, 0, &changed); break;
	case 8: {
		ClusterSpaceReservation reservation = {0};
		reservation.identity = native_insert_live->space; reservation.next_block = 999;
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 999, changed.data, BLCKSZ));
		ctrc_read_native_write(native_insert_live, SPACE_FORKNUM, 1, &changed); break;
	}
	case 9: pg_re_throw(); break;
	case 10: native_fpi_region.checkpoint_owner.tx_result.processed_count++; break;
	case 11: cluster_wal_inputs_release_v1(&native_insert_live->page.replay.inputs); break;
	case 12: native_graph_changed_input->data[BLCKSZ - 1]++; break;
	}
}

static void
native_step_fixture(CtrcReadNativeFixture *f, unsigned source)
{
	native_step_active = native_step_foreign_blocker = false;
	native_step_null_physical = false;
	native_install_active = false; native_install_setup = true;
	native_insert_fixture(f, source); native_install_setup = false;
	native_install_input = NULL;
	native_install_fault = native_install_at = native_install_stage = 0;
	native_install_writes = native_install_syncs = ctrc_read_native_reads = ctrc_read_native_opens = 0;
	native_install_flush = 0x2000240; native_install_timeline = ctrc_inputs_owner.writer.ref.timeline;
	native_fpi_tokens = native_step_fault = native_step_hashes = native_step_allocation_depth = 0;
	native_install_active = native_step_active = true;
}

static RfPageCtrcStepResultV1
native_step(CtrcReadNativeFixture *f, Size count, Size budget)
{
	return rf_page_recovery_ctrc_replays_itl_step_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		count && !native_step_null_physical ? native_fpi_physical : NULL, count, budget);
}

static void
native_step_finish(CtrcReadNativeFixture *f)
{
	UT_ASSERT(native_fpi_allocation == NULL); UT_ASSERT_EQ(native_step_allocation_depth, 0);
	UT_ASSERT_EQ(native_fpi_allocs, native_fpi_frees);
	native_step_active = native_step_foreign_blocker = false;
	native_install_finish(f);
}

UT_TEST(test_native_step_prepares_logs_and_installs_under_original_owners)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;
		PGAlignedBlock expected, actual; ClusterRecoveryStampCaptureV1 capture;
		unsigned releases, grants;
		native_step_fixture(&f, source); releases = test_tx_ir_releases; grants = test_tx_ir_grants;
		result = native_step(&f, 2, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED); UT_ASSERT(result.inserted.wal_flushed);
		UT_ASSERT_EQ(result.inserted.window.key.target_thread, source ? 1 : 128);
		UT_ASSERT_EQ(ctrc_read_native_reads, 20); UT_ASSERT_EQ(native_fpi_tokens, 1);
		UT_ASSERT_EQ(native_insert_capacity, 1); UT_ASSERT_EQ(native_insert_records, 1);
		UT_ASSERT_EQ(native_insert_flushes, 1); UT_ASSERT_EQ(native_install_writes, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		native_install_expected(&result.inserted, &expected);
		UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(expected.data, actual.data, BLCKSZ) == 0);
		for (unsigned i = 0; i < 8; i++) {
			ClusterItlSlotData *slot = &ClusterPageGetItlSlots(actual.data)[i];
			UT_ASSERT_EQ(slot->flags, i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED);
			UT_ASSERT_EQ(slot->commit_scn, i % 2 ? 900 : InvalidScn);
		}
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
		native_install_retained(); native_step_finish(&f);
	}
}

UT_TEST(test_native_step_terminal_page_keeps_observation_and_debt)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result; PGAlignedBlock page;
	native_step_fixture(&f, 0);
	UT_ASSERT_EQ(pread(f.main_fd, page.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
	for (unsigned i = 0; i < 8; i++) {
		ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page.data)[i];
		slot->flags = i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
		slot->commit_scn = i % 2 ? 900 : InvalidScn;
	}
	ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &page);
	result = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED);
	UT_ASSERT_EQ(result.inserted.window.key.window_seq, 0); UT_ASSERT(!result.inserted.wal_flushed);
	UT_ASSERT_EQ(ctrc_read_native_reads, 5); UT_ASSERT_EQ(native_fpi_tokens, 0);
	UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && native_insert_page_held()); native_step_finish(&f);
}

UT_TEST(test_native_step_wait_keeps_original_preflight_and_native_owners)
{
	for (unsigned gap = 0; gap < 2; gap++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result; RfPageGuardPreflightV1 preflight;
		native_step_fixture(&f, 0);
		if (gap) native_insert_fault = 6;
		else {
			UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &preflight));
			UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
			native_step_foreign_blocker = true;
		}
		result = native_step(&f, 2, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_WAIT); UT_ASSERT_EQ(native_insert_records, 0);
		UT_ASSERT_EQ(result.inserted.window.key.window_seq, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(ctrc_read_native_reads, gap ? 5 : 0);
		rf_page_guard_release_v1(&native_insert_blocker); native_step_foreign_blocker = false;
		UT_ASSERT(!native_insert_page_held());
		if (!gap) {
			result = native_step(&f, 2, 1024 * 1024);
			UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED);
		}
		native_step_finish(&f);
	}
}

UT_TEST(test_native_step_bad_entry_refuses_before_owner_snapshot_or_io)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result = {0}; volatile bool caught = false;
		Size count = 2, budget = 1024 * 1024;
		native_step_fixture(&f, 0);
		switch (fault) {
		case 0: MyBackendType = B_CHECKPOINTER; break;
		case 1: ShutdownRequestPending = true; break;
		case 2: inputs_native_recovery = true; break;
		case 3: native_insert_caller_lock = true; break;
		case 4: count = 1; break;
		case 5: count = UINT32_MAX; break;
		case 6: budget = 1; break;
		case 7: f.page.target.expected_result.mutation_token = 0; break;
		case 8: native_step_null_physical = true; break;
		case 9: f.space.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
		}
		PG_TRY(); { result = native_step(&f, count, budget); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(!caught); UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(ctrc_read_native_opens, 0); UT_ASSERT_EQ(native_insert_records, 0);
		MyBackendType = B_STARTUP; ShutdownRequestPending = inputs_native_recovery = native_insert_caller_lock = false;
		native_step_finish(&f);
	}
}

UT_TEST(test_native_step_gap_rechecks_inputs_and_all_original_page_bytes)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;
		if (fault == 9) continue;
		native_step_fixture(&f, 0); native_step_fault = fault;
		result = native_step(&f, 2, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT_EQ(native_install_writes, 0);
		UT_ASSERT_EQ(result.inserted.window.key.window_seq, 0);
		if (fault == 11) UT_ASSERT(!inputs_pin_held && ctrc_set_held == 0);
		else UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_step_finish(&f);
	}
}

UT_TEST(test_native_step_rechecks_inputs_after_every_protected_read)
{
	for (unsigned read = 1; read <= 20; read++) {
		CtrcReadNativeFixture f; RfPageCtrcStepResultV1 result;
		native_step_fixture(&f, 0); native_install_fault = 1; native_install_at = read;
		result = native_step(&f, 2, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(ctrc_read_native_reads, read);
		UT_ASSERT_EQ(native_insert_records, read > 10 ? 1 : 0);
		UT_ASSERT_EQ(native_install_writes, read > 15 ? 1 : 0);
		UT_ASSERT_EQ(native_install_syncs, read > 15 ? 1 : 0);
		if (read > 10) UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && native_insert_page_held()); native_step_finish(&f);
	}
}

UT_TEST(test_native_step_error_frees_private_snapshots_and_keeps_debt)
{
	for (unsigned point = 0; point < 25; point++) {
		CtrcReadNativeFixture f; volatile bool caught = false; unsigned releases;
		native_step_fixture(&f, 0); releases = test_tx_ir_releases;
		if (!point) native_fpi_alloc_error = true;
		else if (point == 21) native_step_fault = 9;
		else if (point == 22) native_insert_fault = 13;
		else if (point > 22) { native_install_fault = 9; native_install_stage = point - 22; }
		else { native_install_fault = 9; native_install_at = point; }
		PG_TRY(); { (void)native_step(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(CritSectionCount, 0);
		if ((point > 10 && point <= 20) || point >= 22)
			UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		native_step_finish(&f);
	}
}

UT_TEST(test_native_step_outstanding_operation_cannot_reinsert)
{
	CtrcReadNativeFixture f; RfPageCtrcStepResultV1 first, second; unsigned reads;
	native_step_fixture(&f, 0); first = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(first.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED); reads = ctrc_read_native_reads;
	second = native_step(&f, 2, 1024 * 1024);
	UT_ASSERT_EQ(second.phase, RF_PAGE_CTRC_STEP_REFUSED);
	UT_ASSERT_EQ(ctrc_read_native_reads, reads); UT_ASSERT_EQ(native_insert_records, 1);
	UT_ASSERT(cluster_recovery_stamp_fpi_inserted_current_v1(&first.inserted.window,
		first.inserted.record_start, first.inserted.record_end));
	native_install_retained(); native_step_finish(&f);
}
