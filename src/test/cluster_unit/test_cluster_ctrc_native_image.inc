/* Original native five-read image publication; Start/current leaves controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifdef PGRAC_TEST_OLD_IMAGE_FORWARDING
/* Explicit old-reader forwarding control: gates execute, no image is supplied.
 * This is component sensitivity, not an unchanged-parent native API. */
RfPageCtrcReadbackImageV1
rf_page_recovery_ctrc_replays_completed_image_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
 ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
 RfPageCtrcReadbackImageV1 out = {0};
 out.detail = rf_page_recovery_ctrc_replays_completed_readback_v1(start, work, source,
  authority, inserted, storage, count, budget);
 return out;
}
RfPageCtrcReadbackImageV1
rf_page_recovery_ctrc_replays_unchanged_image_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const RfPageCtrcUnchangedV1 *unchanged,
 ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
 RfPageCtrcReadbackImageV1 out = {0};
 out.detail = rf_page_recovery_ctrc_replays_unchanged_readback_v1(start, work, source,
  authority, unchanged, storage, count, budget);
 return out;
}
#endif

static bool
native_image_zero(const PGAlignedBlock *image)
{
 static const PGAlignedBlock zero = {0};
 return memcmp(image, &zero, sizeof(zero)) == 0;
}

UT_TEST(test_native_completed_image_is_exact_disk_bytes_with_writer_coordinate)
{
 for (unsigned source = 0; source < 2; source++) {
  CtrcReadNativeFixture f; PGAlignedBlock actual;
  RfPageCtrcHeldStepV1 held = native_readback_ready(&f, source);
  RfPageCtrcReadbackImageV1 copied;
  native_completed_bind(&held);
  copied = rf_page_recovery_ctrc_replays_completed_image_v1(tx_inputs_start, &ctrc_inputs_work,
   &native_stamp_source, f.page.page.authority, &held.result.inserted, held.storage, 2, 1024 * 1024);
  UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_OK);
  UT_ASSERT_EQ(pread(f.main_fd, actual.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
  UT_ASSERT(memcmp(&copied.image, &actual, sizeof(actual)) == 0);
  UT_ASSERT_EQ(PageGetLSN(copied.image.data), held.result.inserted.record_end);
  UT_ASSERT_EQ(ctrc_read_native_reads, 25); UT_ASSERT(native_insert_page_held());
  native_completed_finish(&f, &held);
 }
}

UT_TEST(test_native_unchanged_image_preserves_full_baseline_without_wal)
{
 CtrcReadNativeFixture f;
 RfPageCtrcHeldTargetV1 held = native_unchanged_custody_ready(&f, 0);
 RfPageCtrcReadbackImageV1 copied;
 native_unchanged_original = &held.unchanged;
 copied = rf_page_recovery_ctrc_replays_unchanged_image_v1(tx_inputs_start, &ctrc_inputs_work,
  &native_stamp_source, f.page.page.authority, &held.unchanged, held.step.storage, 2, 1024 * 1024);
 UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_OK);
 UT_ASSERT(memcmp(&copied.image, &held.unchanged.page, sizeof(copied.image)) == 0);
 UT_ASSERT_EQ(ctrc_read_native_reads, 15); UT_ASSERT(native_insert_page_held());
 native_unchanged_custody_finish(&f, &held);
}

UT_TEST(test_native_image_late_binding_or_disk_drift_publishes_no_prefix)
{
 for (unsigned mode = 0; mode < 3; mode++) {
  CtrcReadNativeFixture f;
  RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
  RfPageCtrcReadbackImageV1 copied;
  native_completed_bind(&held);
  if (mode == 0) native_completed_operation = NULL;
  if (mode == 1) native_completed_drop_at = 25;
  if (mode == 2) {
   PGAlignedBlock changed;
   UT_ASSERT_EQ(pread(f.main_fd, changed.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
   changed.data[BLCKSZ - 1]++;
   ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &changed);
  }
  copied = rf_page_recovery_ctrc_replays_completed_image_v1(tx_inputs_start, &ctrc_inputs_work,
   &native_stamp_source, f.page.page.authority, &held.result.inserted, held.storage, 2, 1024 * 1024);
  UT_ASSERT(copied.detail != RF_PAGE_PROOF_DETAIL_OK && native_image_zero(&copied.image));
  UT_ASSERT(ctrc_page_fd_live && inputs_pin_held && ctrc_set_held == 2);
  native_completed_finish(&f, &held);
 }
}

UT_TEST(test_native_image_capacity_wait_and_error_keep_original_fd_and_debt)
{
 CtrcReadNativeFixture f; RfPageGuardPreflightV1 preflight;
 RfPageCtrcHeldStepV1 held = native_readback_ready(&f, 0);
 RfPageCtrcReadbackImageV1 copied; volatile bool caught = false;
 native_completed_bind(&held);
 copied = rf_page_recovery_ctrc_replays_completed_image_v1(tx_inputs_start, &ctrc_inputs_work,
  &native_stamp_source, f.page.page.authority, &held.result.inserted, held.storage, 2, BLCKSZ);
 UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
 UT_ASSERT(native_image_zero(&copied.image)); UT_ASSERT_EQ(ctrc_read_native_reads, 20);
 UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &preflight));
 UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
 native_step_foreign_blocker = true; native_step_hashes = 1;
 copied = rf_page_recovery_ctrc_replays_completed_image_v1(tx_inputs_start, &ctrc_inputs_work,
  &native_stamp_source, f.page.page.authority, &held.result.inserted, held.storage, 2, 1024 * 1024);
 UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
 UT_ASSERT(native_image_zero(&copied.image)); UT_ASSERT_EQ(ctrc_read_native_reads, 20);
 rf_page_guard_release_v1(&native_insert_blocker); native_step_foreign_blocker = false;
 native_step_hashes = 0; ctrc_page_fd_fault = 5;
 PG_TRY(); { (void)rf_page_recovery_ctrc_replays_completed_image_v1(tx_inputs_start, &ctrc_inputs_work,
  &native_stamp_source, f.page.page.authority, &held.result.inserted, held.storage, 2, 1024 * 1024); }
 PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught && ctrc_page_fd_live && inputs_pin_held && ctrc_set_held == 2);
 UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
 native_completed_finish(&f, &held);
}
