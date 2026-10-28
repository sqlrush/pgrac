/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_completed.inc
 *     Protected readback after the original F4 operation has been retired.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_completed.inc
 *
 * NOTES
 *     Real PAGE/F4/SPACE/file reads; original Start binding is controlled.
 *-------------------------------------------------------------------------
 */
static const RfPageCtrcFpiInsertV1 *native_completed_operation;
static ClusterSharedFsPageV1 *native_completed_storage;
static RfPageCtrcFpiInsertV1 native_completed_value;
static unsigned native_completed_drop_at;

extern bool ClusterColdRecoveryCtrcPageCompletedCurrent(const struct ClusterRecoveryStartV1 *,
	const ClusterColdCtrcWorkV1 *, const RfPageCtrcFpiInsertV1 *, ClusterSharedFsPageV1 *);
extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_completed_readback_v1(
	const struct ClusterRecoveryStartV1 *, const ClusterColdCtrcWorkV1 *,
	const ClusterRecoveryCtrcReplaySetSourceV1 *, RfPageCtrcAuthorityV1 *,
	const RfPageCtrcFpiInsertV1 *, ClusterSharedFsPageV1 *, Size, Size);

bool
ClusterColdRecoveryCtrcPageCompletedCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcFpiInsertV1 *inserted,
	ClusterSharedFsPageV1 *storage)
{
	return start == tx_inputs_start && work == &ctrc_inputs_work
		&& inserted != NULL && inserted == native_completed_operation
		&& storage != NULL && storage == native_completed_storage
		&& memcmp(inserted, &native_completed_value, sizeof(*inserted)) == 0
		&& (native_completed_drop_at == 0 || ctrc_read_native_reads < native_completed_drop_at);
}

static void
native_completed_bind(RfPageCtrcHeldStepV1 *held)
{
	UT_ASSERT(cluster_recovery_stamp_clear_v1(&held->result.inserted.window));
	native_completed_operation = &held->result.inserted;
	native_completed_storage = held->storage;
	native_completed_value = held->result.inserted;
	native_completed_drop_at = 0;
}

static RfPageProofDetailV1
native_completed_read(CtrcReadNativeFixture *f, RfPageCtrcHeldStepV1 *held)
{
	return rf_page_recovery_ctrc_replays_completed_readback_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &held->result.inserted,
		held->storage, 2, 1024 * 1024);
}

static void
native_completed_finish(CtrcReadNativeFixture *f, RfPageCtrcHeldStepV1 *held)
{
	native_completed_operation = NULL; native_completed_storage = NULL;
	native_completed_drop_at = 0;
	native_readback_finish(f, held);
}

UT_TEST(test_native_completed_readback_uses_original_page_and_same_fds_after_f4_clear)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, source);
		native_completed_bind(&held);
		UT_ASSERT_EQ(native_completed_read(&f, &held), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(native_insert_page_held()); UT_ASSERT_EQ(ctrc_read_native_reads, 25);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		UT_ASSERT(memcmp(&held.result.inserted, &native_completed_value, sizeof(native_completed_value)) == 0);
		native_completed_finish(&f, &held);
	}
}

UT_TEST(test_native_completed_readback_refuses_clones_lost_binding_and_pending_api)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0), clone;
		native_completed_bind(&held); clone = held;
		if (fault == 0) native_completed_operation = NULL;
		if (fault == 1) native_completed_storage = NULL;
		if (fault == 2) held.result.inserted.record_end++;
		if (fault == 3)
			UT_ASSERT(native_completed_read(&f, &clone) != RF_PAGE_PROOF_DETAIL_OK);
		else
			UT_ASSERT(native_completed_read(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(native_readback(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(ctrc_read_native_reads, 20);
		native_completed_finish(&f, &held);
	}
}

UT_TEST(test_native_completed_readback_refuses_changed_main_or_space_without_reinstall)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock changed;
		RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
		native_completed_bind(&held);
		UT_ASSERT_EQ(pread(f.main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		if (fault == 0) changed.data[BLCKSZ - 1]++;
		if (fault == 1) PageSetLSNPreserveOrigin(changed.data, held.result.inserted.record_end + 8);
		if (fault == 2) UT_ASSERT(PageSetLSNOrigin(changed.data, 2));
		if (fault == 3) ((PageHeader)changed.data)->pd_block_scn--;
		if (fault < 4) ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &changed);
		else {
			UT_ASSERT_EQ(pread(f.space_fd, changed.data, BLCKSZ, 0), BLCKSZ);
			changed.data[BLCKSZ - 1]++;
			UT_ASSERT_EQ(pwrite(f.space_fd, changed.data, BLCKSZ, 0), BLCKSZ);
		}
		UT_ASSERT(native_completed_read(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		native_completed_finish(&f, &held);
	}
}

UT_TEST(test_native_completed_readback_rechecks_binding_after_each_protected_read)
{
	for (unsigned read = 1; read <= 5; read++) {
		CtrcReadNativeFixture f; RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
		native_completed_bind(&held); native_completed_drop_at = 20 + read;
		UT_ASSERT(native_completed_read(&f, &held) != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		native_completed_finish(&f, &held);
	}
}

UT_TEST(test_native_completed_readback_collision_and_error_keep_original_storage)
{
	CtrcReadNativeFixture f; volatile bool caught = false;
	RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
	RfPageGuardPreflightV1 preflight;
	ClusterRecoveryStampHandleV1 later;
	native_completed_bind(&held);
	UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &preflight));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
	native_step_foreign_blocker = true; native_step_hashes = 1;
	UT_ASSERT_EQ(native_completed_read(&f, &held), RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(ctrc_read_native_reads, 20);
	rf_page_guard_release_v1(&native_insert_blocker); native_step_foreign_blocker = false;
	UT_ASSERT(stamp_prepare(&held.result.inserted.window.owner, CLUSTER_RECOVERY_STAMP_FPI,
		held.result.inserted.window.payload, held.result.inserted.window.payload_length, &later));
	UT_ASSERT(later.key.window_seq != held.result.inserted.window.key.window_seq);
	native_step_hashes = 0; ctrc_page_fd_fault = 5;
	PG_TRY(); { (void)native_completed_read(&f, &held); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && ctrc_page_fd_live);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
	UT_ASSERT(!native_fpi_region.thread_replay[128].stamp.failed);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.handle.key.window_seq, later.key.window_seq);
	UT_ASSERT(cluster_recovery_stamp_cancel_v1(&later));
	native_completed_finish(&f, &held);
}
