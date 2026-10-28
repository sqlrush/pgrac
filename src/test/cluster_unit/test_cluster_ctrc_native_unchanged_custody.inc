/*-------------------------------------------------------------------------
 * test_cluster_ctrc_native_unchanged_custody.inc
 *     Same-FD unchanged baseline and protected final byte comparison.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Original PAGE/read/cache and real file bytes; Start/GES/WAL are unit leaves.
 *-------------------------------------------------------------------------
 */
static const RfPageCtrcUnchangedV1 *native_unchanged_original;
static RfPageCtrcUnchangedV1 native_unchanged_value;
static ClusterSharedFsPageV1 *native_unchanged_storage;
static unsigned native_unchanged_drop_at;

bool
ClusterColdRecoveryCtrcPageUnchangedCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcUnchangedV1 *unchanged,
	ClusterSharedFsPageV1 *storage)
{
	return start == tx_inputs_start && work == &ctrc_inputs_work
		&& unchanged != NULL && unchanged == native_unchanged_original
		&& storage != NULL && storage == native_unchanged_storage
		&& memcmp(unchanged, &native_unchanged_value, sizeof(*unchanged)) == 0
		&& (native_unchanged_drop_at == 0 || ctrc_read_native_reads < native_unchanged_drop_at);
}

static RfPageCtrcHeldTargetV1
native_unchanged_custody_ready(CtrcReadNativeFixture *f, unsigned source)
{
	PGAlignedBlock canonical;
	RfPageCtrcHeldTargetV1 held;

	native_unchanged_fixture(f, source, &canonical);
	held = rf_page_recovery_ctrc_replays_itl_graph_hold_target_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		canonical.data, native_fpi_physical, 2, 1024 * 1024);
	UT_ASSERT_EQ(held.step.result.phase, RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED);
	UT_ASSERT(held.unchanged.ready && held.step.storage != NULL);
	UT_ASSERT(memcmp(held.unchanged.page.data, canonical.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 0);
	UT_ASSERT(rf_page_authority_ctrc_replays_unpromote_v1(f->page.page.authority));
	native_unchanged_original = NULL; native_unchanged_drop_at = 0;
	native_unchanged_storage = held.step.storage; native_unchanged_value = held.unchanged;
	return held;
}

static RfPageProofDetailV1
native_unchanged_custody_read(CtrcReadNativeFixture *f, const RfPageCtrcHeldTargetV1 *held)
{
	return rf_page_recovery_ctrc_replays_unchanged_readback_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &held->unchanged, held->step.storage, 2, 1024 * 1024);
}

static void
native_unchanged_custody_finish(CtrcReadNativeFixture *f, RfPageCtrcHeldTargetV1 *held)
{
	UT_ASSERT_EQ(native_insert_records + native_insert_flushes + native_install_writes + native_fpi_tokens, 0);
	UT_ASSERT_EQ(native_install_syncs, 1); UT_ASSERT_EQ(ctrc_page_fd_opens, 1);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	UT_ASSERT(cluster_shared_fs_page_close_v1(held->step.storage));
	native_unchanged_original = NULL; native_unchanged_storage = NULL; native_unchanged_drop_at = 0;
	native_step_finish(f);
}

UT_TEST(test_native_unchanged_custody_retains_exact_raw_pages_and_same_fd)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock actual;
		RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, source);

		native_unchanged_original = &held.unchanged;
		UT_ASSERT_EQ(pread(f.space_fd, actual.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, held.unchanged.space0.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(pread(f.space_fd, actual.data, BLCKSZ, BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, held.unchanged.space1.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(native_unchanged_custody_read(&f, &held), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(ctrc_read_native_reads, 15); UT_ASSERT(native_insert_page_held());
		native_unchanged_custody_finish(&f, &held);
	}
}

UT_TEST(test_native_unchanged_custody_refuses_copies_and_bad_entry_before_io)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f;
		RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, 0), clone = held;
		RfPageProofDetailV1 detail;

		native_unchanged_original = &held.unchanged;
		if (fault == 0) detail = native_unchanged_custody_read(&f, &clone);
		else if (fault == 1) {
			native_unchanged_storage = NULL;
			detail = native_unchanged_custody_read(&f, &held);
		} else if (fault == 2) {
			held.unchanged.space1.data[100]++;
			detail = native_unchanged_custody_read(&f, &held);
		} else if (fault == 3) {
			native_insert_caller_lock = true;
			detail = native_unchanged_custody_read(&f, &held);
			native_insert_caller_lock = false;
		} else detail = rf_page_recovery_ctrc_replays_unchanged_readback_v1(tx_inputs_start,
			&ctrc_inputs_work, &native_stamp_source, f.page.page.authority, &held.unchanged,
			held.step.storage, 2, sizeof(held.unchanged));
		UT_ASSERT(detail != RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT_EQ(ctrc_read_native_reads, 10);
		native_unchanged_custody_finish(&f, &held);
	}
}

UT_TEST(test_native_unchanged_custody_refuses_changed_valid_main_and_space_bytes)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock changed;
		RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, 0);

		native_unchanged_original = &held.unchanged; changed = held.unchanged.page;
		if (fault == 0) changed.data[BLCKSZ - 1]++;
		if (fault == 1) PageSetLSNPreserveOrigin(changed.data, PageGetLSN(changed.data) + 8);
		if (fault == 2) UT_ASSERT(PageSetLSNOrigin(changed.data, 2));
		if (fault < 3) ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &changed);
		else if (fault == 3) {
			UT_ASSERT(cluster_space_identity_page_encode(&f.space, 999, changed.data, BLCKSZ));
			ctrc_read_native_write(&f, SPACE_FORKNUM, 0, &changed);
		} else {
			ClusterSpaceReservation reservation = {0};
			reservation.identity = f.space; reservation.next_block = 999;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 999, changed.data, BLCKSZ));
			ctrc_read_native_write(&f, SPACE_FORKNUM, 1, &changed);
		}
		UT_ASSERT(native_unchanged_custody_read(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(ctrc_read_native_reads, 15);
		native_unchanged_custody_finish(&f, &held);
	}
}

UT_TEST(test_native_unchanged_custody_rechecks_binding_after_each_read)
{
	for (unsigned read = 1; read <= 5; read++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, 0);

		native_unchanged_original = &held.unchanged; native_unchanged_drop_at = 10 + read;
		UT_ASSERT(native_unchanged_custody_read(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		native_unchanged_custody_finish(&f, &held);
	}
}

UT_TEST(test_native_unchanged_custody_wait_and_error_keep_fd_and_original_debt)
{
	CtrcReadNativeFixture f; volatile bool caught = false;
	RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, 0);
	RfPageGuardPreflightV1 preflight;

	native_unchanged_original = &held.unchanged;
	UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &preflight));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
	native_step_foreign_blocker = true;
	UT_ASSERT_EQ(native_unchanged_custody_read(&f, &held), RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(ctrc_read_native_reads, 10);
	rf_page_guard_release_v1(&native_insert_blocker); native_step_foreign_blocker = false;
	ctrc_page_fd_fault = 5;
	PG_TRY(); { (void)native_unchanged_custody_read(&f, &held); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && ctrc_page_fd_live);
	native_unchanged_custody_finish(&f, &held);
}
