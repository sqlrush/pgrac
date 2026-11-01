/* Same original Start/FD image publication after PAGE cleanup; native transport
 * is a controlled leaf. Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned startup_image_calls;
static PGAlignedBlock startup_image_expected;
/* Optional transport observer; original default and all prior cases are unchanged. */
static RfPageProofDetailV1 (*startup_image_observer)(uint32, RfPageProofDetailV1);

RfPageCtrcReadbackImageV1
rf_page_recovery_ctrc_replays_completed_image_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
 ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
 RfPageCtrcReadbackImageV1 out = {0};
 startup_image_calls++;
 out.detail = rf_page_recovery_ctrc_replays_completed_readback_v1(start, work, source,
  authority, inserted, storage, count, budget);
 if (startup_image_observer != NULL)
  out.detail = startup_image_observer(inserted->image.block_number - 23, out.detail);
 if (out.detail == RF_PAGE_PROOF_DETAIL_OK) out.image = startup_image_expected;
 return out;
}
RfPageCtrcReadbackImageV1
rf_page_recovery_ctrc_replays_unchanged_image_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const RfPageCtrcUnchangedV1 *unchanged,
 ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
 RfPageCtrcReadbackImageV1 out = {0};
 startup_image_calls++;
 out.detail = rf_page_recovery_ctrc_replays_unchanged_readback_v1(start, work, source,
  authority, unchanged, storage, count, budget);
 if (startup_image_observer != NULL)
  out.detail = startup_image_observer(unchanged->blockno - 23, out.detail);
 if (out.detail == RF_PAGE_PROOF_DETAIL_OK) out.image = startup_image_expected;
 return out;
}
#ifdef PGRAC_TEST_OLD_IMAGE_FORWARDING
struct RfPageCtrcReadbackImageV1
ClusterColdRecoveryCtrcPageImage(const struct ClusterRecoveryStartV1 *start, uint32 index)
{
 RfPageCtrcReadbackImageV1 out = {0};
 if (start == NULL || start != cluster_recovery_cold_start
  || start->ctrc_page_progress == NULL
  || index >= start->ctrc_page_progress->target_count) {
  out.detail = RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT; return out;
 }
 out.detail = cluster_recovery_cold_page_completed_recheck(start, NULL, 0)
  ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_ROOT_STALE;
 return out;
}
#endif
static bool
startup_image_zero(const PGAlignedBlock *image)
{
 static const PGAlignedBlock zero = {0};
 return memcmp(image, &zero, sizeof(zero)) == 0;
}
static void
startup_image_ready(bool unchanged)
{
 if (unchanged) unchanged_custody_ready(); else completed_readback_ready();
 retirement_run_startup();
 UT_ASSERT(retirement_service_reached);
 startup_image_calls = 0; memset(&startup_image_expected, 93, sizeof(startup_image_expected));
}
UT_TEST(test_startup_page_image_is_from_original_completed_or_unchanged_fd)
{
 for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
  RfPageCtrcReadbackImageV1 copied;
  unsigned steps;
  startup_image_ready(unchanged != 0); steps = prefix_steps;
  copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 1);
  UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_OK);
  UT_ASSERT(memcmp(&copied.image, &startup_image_expected, sizeof(copied.image)) == 0);
  UT_ASSERT_EQ(startup_image_calls, 1); UT_ASSERT_EQ(prefix_steps, steps);
  UT_ASSERT_EQ(pending_storage_mask, 3); UT_ASSERT(observation_scope_live);
  UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
  retirement_local_cleanup();
 }
}
UT_TEST(test_startup_page_image_rejects_pending_missing_and_nonmember_targets)
{
 RfPageCtrcReadbackImageV1 copied;
 const struct ClusterRecoveryStartV1 *start;
 startup_image_ready(false); start = info.cluster_tx_start;
 copied = ClusterColdRecoveryCtrcPageImage(start, 2);
 UT_ASSERT(copied.detail != RF_PAGE_PROOF_DETAIL_OK && startup_image_zero(&copied.image));
 copied = ClusterColdRecoveryCtrcPageImage(NULL, 0);
 UT_ASSERT(copied.detail != RF_PAGE_PROOF_DETAIL_OK && startup_image_zero(&copied.image));
 UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(ClusterColdRecoveryTxCurrent(start));
 retirement_local_cleanup();
 prefix_ready(); startup_image_calls = 0;
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 0);
 UT_ASSERT(copied.detail != RF_PAGE_PROOF_DETAIL_OK && startup_image_zero(&copied.image));
 UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(pending_storage_live);
 retirement_local_cleanup();
}
static void
startup_image_late_drift(void)
{
 cluster_recovery_cold_start->ctrc_page_progress->completed[0].result.inserted.record_end++;
}
UT_TEST(test_startup_page_image_discards_bytes_after_page_cleanup_drift)
{
 RfPageCtrcReadbackImageV1 copied;
 startup_image_ready(false); prefix_destroy_callback = startup_image_late_drift;
 copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 0);
 prefix_destroy_callback = NULL;
 UT_ASSERT(copied.detail != RF_PAGE_PROOF_DETAIL_OK && startup_image_zero(&copied.image));
 UT_ASSERT(cluster_recovery_cold_start->revoked && pending_storage_mask == 3);
 UT_ASSERT_EQ(startup_image_calls, 1); retirement_local_cleanup();
}
UT_TEST(test_startup_page_image_wait_has_no_cached_success_or_new_wal)
{
 RfPageCtrcReadbackImageV1 copied;
 startup_image_ready(false); completed_readback_mode = 4; completed_readback_calls = 0;
 copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 0);
 UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
 UT_ASSERT(startup_image_zero(&copied.image) && !cluster_recovery_cold_start->revoked);
 completed_readback_mode = 0;
 copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 0);
 UT_ASSERT_EQ(copied.detail, RF_PAGE_PROOF_DETAIL_OK);
 UT_ASSERT(memcmp(&copied.image, &startup_image_expected, sizeof(copied.image)) == 0);
 UT_ASSERT_EQ(startup_image_calls, 2); UT_ASSERT_EQ(prefix_steps, 2);
 retirement_local_cleanup();
}
UT_TEST(test_startup_page_image_refusal_and_error_keep_original_cleanup_debt)
{
 for (unsigned mode = 1; mode <= 2; mode++) {
  RfPageCtrcReadbackImageV1 copied = {0}; volatile bool caught = false;
  startup_image_ready(false); completed_readback_mode = mode == 1 ? 1 : 3;
  PG_TRY(); { copied = ClusterColdRecoveryCtrcPageImage(info.cluster_tx_start, 0); }
  PG_CATCH(); { caught = true; } PG_END_TRY();
  UT_ASSERT(mode == 1 ? !caught && copied.detail != RF_PAGE_PROOF_DETAIL_OK : caught);
  UT_ASSERT(startup_image_zero(&copied.image));
  UT_ASSERT(cluster_recovery_cold_start->revoked && pending_storage_mask == 3 && observation_scope_live);
  UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
  retirement_local_cleanup();
 }
}
