/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_returned_fpi.inc
 *     Returned FPI debt survives subsequent PAGE cleanup and owner loss.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_returned_fpi.inc
 *
 * NOTES
 *     Actual Start and original F4 codec/transitions execute. Native PAGE,
 *     storage/WAL, GES and cleanup fault delivery remain controlled leaves.
 *-------------------------------------------------------------------------
 */
static unsigned returned_fpi_cleanup_calls;
static bool returned_fpi_cleanup_pending, returned_fpi_owner_loss, returned_fpi_error;
static RfPageCtrcStepResultV1 returned_fpi_cleanup_result;

static void
returned_fpi_cleanup(void)
{
 const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
 returned_fpi_cleanup_calls++;
 returned_fpi_cleanup_pending = progress->pending;
 memcpy(&returned_fpi_cleanup_result, &progress->result, sizeof(progress->result));
 if (returned_fpi_owner_loss) owner_boot++;
 if (returned_fpi_error) pg_re_throw();
}

static void
returned_fpi_ready(bool refused, bool unchanged, bool owner_loss, bool error)
{
 prefix_ready();
 prefix_insert_refuse = refused; prefix_unchanged = unchanged;
 prefix_legacy_unchanged = false; prefix_sync_steps = 0;
 returned_fpi_cleanup_calls = 0;
 returned_fpi_cleanup_pending = false;
 memset(&returned_fpi_cleanup_result, 0, sizeof(returned_fpi_cleanup_result));
 returned_fpi_owner_loss = owner_loss; returned_fpi_error = error;
 prefix_destroy_callback = returned_fpi_cleanup;
}

static void
returned_fpi_retained(bool refused)
{
 const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
 ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(prefix_window.key.target_thread);
 UT_ASSERT_EQ(returned_fpi_cleanup_calls, 1);
 UT_ASSERT(returned_fpi_cleanup_pending);
 UT_ASSERT(progress->pending && prefix_window.key.window_seq != 0);
 UT_ASSERT(memcmp(&progress->result, &returned_fpi_cleanup_result, sizeof(progress->result)) == 0);
 UT_ASSERT(memcmp(&progress->result.inserted.window, &prefix_window, sizeof(prefix_window)) == 0);
 UT_ASSERT_EQ(progress->result.phase, refused ? RF_PAGE_CTRC_STEP_REFUSED : RF_PAGE_CTRC_STEP_FPI_DURABLE);
 UT_ASSERT(progress->result.inserted.wal_flushed);
 UT_ASSERT_EQ(progress->result.inserted.record_start, prefix_window.owner.writer.startup_first_lsn + 40000);
 UT_ASSERT_EQ(progress->result.inserted.record_end, prefix_window.owner.writer.startup_first_lsn + 40128);
 UT_ASSERT_EQ(slot->stamp.phase, refused ? CLUSTER_RECOVERY_STAMP_INSERTED : CLUSTER_RECOVERY_STAMP_DURABLE);
 UT_ASSERT_EQ(slot->stamp.failed, refused);
 pending_window_debt_unchanged(&returned_fpi_cleanup_result);
}

static void
returned_fpi_no_advance(void)
{
 const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
 RfPageCtrcStepResultV1 empty = {0};
 UT_ASSERT_EQ(returned_fpi_cleanup_calls, 1);
 UT_ASSERT(!returned_fpi_cleanup_pending && !progress->pending);
 UT_ASSERT(memcmp(&progress->result, &empty, sizeof(empty)) == 0);
 UT_ASSERT(memcmp(&returned_fpi_cleanup_result, &empty, sizeof(empty)) == 0);
 UT_ASSERT_EQ(prefix_window.key.window_seq, 0);
 UT_ASSERT_EQ(progress->next_target, 0); UT_ASSERT_EQ(prefix_steps, 1);
 UT_ASSERT_EQ(prefix_destroyed, 2);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT(info.cluster_cold_inputs != NULL && info.cluster_tx_start != NULL);
}

UT_TEST(test_startup_returned_durable_is_saved_before_page_cleanup)
{
 returned_fpi_ready(false, false, false, false);
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 returned_fpi_retained(false);
 UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1);
}

UT_TEST(test_startup_returned_durable_survives_late_owner_loss)
{
 returned_fpi_ready(false, false, true, false);
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 returned_fpi_retained(false);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1);
}

UT_TEST(test_startup_returned_failed_operation_survives_late_owner_loss)
{
 returned_fpi_ready(true, false, true, false);
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 returned_fpi_retained(true);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1);
}

UT_TEST(test_startup_returned_durable_survives_page_cleanup_error)
{
 volatile bool caught = false;
 returned_fpi_ready(false, false, false, true);
 PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
 PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught); returned_fpi_retained(false);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1);
}

UT_TEST(test_startup_returned_failed_operation_survives_page_cleanup_error)
{
 volatile bool caught = false;
 returned_fpi_ready(true, false, false, true);
 PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
 PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught); returned_fpi_retained(true);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1);
}

UT_TEST(test_startup_returned_unchanged_cannot_advance_after_owner_loss)
{
 returned_fpi_ready(false, true, true, false);
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 returned_fpi_no_advance();
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}

UT_TEST(test_startup_returned_unchanged_cannot_advance_after_cleanup_error)
{
 volatile bool caught = false;
 returned_fpi_ready(false, true, false, true);
 PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
 PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught); returned_fpi_no_advance();
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}
