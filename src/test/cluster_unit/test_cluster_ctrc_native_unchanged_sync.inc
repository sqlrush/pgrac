/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_unchanged_sync.inc
 *     Same-PAGE/FD sync and complete readback without a new FPI.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_unchanged_sync.inc
 *
 * NOTES
 *     Original native PAGE/IR holders and real temporary-file bytes execute.
 *     Startup/GES/WAL and raw-FD transport remain controlled unit leaves.
 *-------------------------------------------------------------------------
 */
static unsigned native_unchanged_bytes_fault;

static void
native_unchanged_read(SMgrRelation relation, ForkNumber fork, BlockNumber block, void *out)
{
	if (ctrc_read_native_reads == 5 && native_unchanged_bytes_fault != 0) {
		PGAlignedBlock changed;
		UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT(ctrc_page_fd_live && native_insert_page_held());
		if (native_unchanged_bytes_fault == 1) {
			UT_ASSERT_EQ(pread(native_insert_live->main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
			changed.data[BLCKSZ - 1]++;
			ctrc_read_native_write(native_insert_live, MAIN_FORKNUM, 7, &changed);
		} else if (native_unchanged_bytes_fault == 2) {
			UT_ASSERT(cluster_space_identity_page_encode(&native_insert_live->space, 999, changed.data, BLCKSZ));
			ctrc_read_native_write(native_insert_live, SPACE_FORKNUM, 0, &changed);
		} else {
			ClusterSpaceReservation reservation = {0};
			reservation.identity = native_insert_live->space; reservation.next_block = 999;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 999, changed.data, BLCKSZ));
			ctrc_read_native_write(native_insert_live, SPACE_FORKNUM, 1, &changed);
		}
	}
	smgrread(relation, fork, block, out);
}

static void
native_unchanged_fixture(CtrcReadNativeFixture *f, unsigned source, PGAlignedBlock *canonical)
{
	native_step_fixture(f, source); native_durable_active = false;
	native_durable_calls = native_unchanged_bytes_fault = 0;
	UT_ASSERT_EQ(pread(f->main_fd, canonical->data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
	for (unsigned i = 0; i < 8; i++) {
		ClusterItlSlotData *slot = &ClusterPageGetItlSlots(canonical->data)[i];
		slot->flags = i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
		slot->commit_scn = i % 2 ? 900 : InvalidScn;
	}
	ctrc_read_native_write(f, MAIN_FORKNUM, 7, canonical);
	UT_ASSERT_EQ(pread(f->main_fd, canonical->data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
	ctrc_page_fd_read_transport = native_unchanged_read;
}

static RfPageCtrcStepResultV1
native_unchanged_step(CtrcReadNativeFixture *f, const PGAlignedBlock *canonical, Size budget)
{
	return rf_page_recovery_ctrc_replays_itl_graph_sync_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		canonical == NULL ? NULL : canonical->data, native_fpi_physical, 2, budget);
}

static void
native_unchanged_no_wal(const RfPageCtrcStepResultV1 *result, unsigned releases)
{
	RfPageCtrcFpiInsertV1 empty = {0};
	UT_ASSERT(memcmp(&result->inserted, &empty, sizeof(empty)) == 0);
	UT_ASSERT_EQ(native_fpi_tokens + native_insert_records + native_insert_flushes, 0);
	UT_ASSERT_EQ(native_install_writes + native_durable_calls, 0);
	UT_ASSERT_EQ(test_tx_ir_releases, releases);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
}

UT_TEST(test_native_unchanged_syncs_same_fd_and_full_image_without_wal)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical, actual;
		RfPageCtrcStepResultV1 result; unsigned releases;
		native_unchanged_fixture(&f, source, &canonical); releases = test_tx_ir_releases;
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED);
		UT_ASSERT_EQ(ctrc_read_native_reads, 10); UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, canonical.data, BLCKSZ) == 0);
		native_unchanged_no_wal(&result, releases);
		UT_ASSERT(native_insert_page_held()); native_step_finish(&f);
	}
}

UT_TEST(test_native_unchanged_sync_rechecks_original_inputs_after_sync_and_each_read)
{
	const unsigned faults[] = {1, 2, 6};
	for (unsigned fault = 0; fault < lengthof(faults); fault++)
		for (unsigned point = 0; point < 6; point++) {
			CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
			unsigned releases;
			native_unchanged_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
			native_install_fault = faults[fault];
			if (point == 0) native_install_stage = 2;
			else native_install_at = point + 5;
			result = native_unchanged_step(&f, &canonical, 1024 * 1024);
			UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
			UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
			native_unchanged_no_wal(&result, releases); native_step_finish(&f);
		}
}

UT_TEST(test_native_unchanged_sync_io_failure_error_and_close_keep_original_owners)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical;
		RfPageCtrcStepResultV1 result = {0}; volatile bool caught = false; unsigned releases;
		native_unchanged_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		if (fault < 3) ctrc_page_fd_fault = fault == 0 ? 4 : fault == 1 ? 6 : 7;
		if (fault == 3) { native_install_fault = 9; native_install_stage = 2; }
		if (fault == 4) { native_install_fault = 9; native_install_at = 10; }
		PG_TRY(); { result = native_unchanged_step(&f, &canonical, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1 || fault >= 3);
		if (!caught) UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		native_unchanged_no_wal(&result, releases); native_step_finish(&f);
	}
}

UT_TEST(test_native_unchanged_sync_refuses_changed_full_main_or_space_bytes)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		native_unchanged_bytes_fault = fault;
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_read_native_reads, 10);
		native_unchanged_no_wal(&result, releases); native_step_finish(&f);
	}
}

UT_TEST(test_native_unchanged_sync_preserves_old_entries_and_rewritten_fpi_debt)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		if (mode == 0) result = native_step(&f, 2, 1024 * 1024);
		else if (mode == 1) result = rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(
			tx_inputs_start, &ctrc_inputs_work, &native_stamp_source, f.page.page.authority,
			&f.space, 7, &f.page.target.expected_result, canonical.data, native_fpi_physical, 2, 1024 * 1024);
		else result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_UNCHANGED);
		UT_ASSERT_EQ(ctrc_read_native_reads, 5); UT_ASSERT_EQ(native_install_syncs, 0);
		native_unchanged_no_wal(&result, releases); native_step_finish(&f);
	}
	{
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		native_durable_fixture(&f, 0, &canonical);
		result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
		UT_ASSERT_EQ(native_durable_calls, 1); UT_ASSERT_EQ(native_insert_records, 1);
		native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_unchanged_sync_requires_original_entry_guards_and_canonical_image)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		unsigned releases;
		native_unchanged_fixture(&f, 0, &canonical); releases = test_tx_ir_releases;
		if (fault == 0) native_insert_caller_lock = true;
		if (fault == 3) canonical.data[BLCKSZ - 1]++;
		result = native_unchanged_step(&f, fault == 2 ? NULL : &canonical, fault == 1 ? 1 : 1024 * 1024);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT_EQ(native_install_syncs, 0);
		if (fault < 3) UT_ASSERT_EQ(ctrc_read_native_reads, 0);
		native_insert_caller_lock = false;
		native_unchanged_no_wal(&result, releases); native_step_finish(&f);
	}
}
