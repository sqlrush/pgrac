/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_pending_window.inc
 *     Original Start and service handoff retain the exact live F4 window.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_pending_window.inc
 *
 * NOTES
 *     Original F4 codec/transitions, Start and handoff execute. Native PAGE,
 *     WAL/storage, GES and scheduling remain controlled fixture boundaries.
 *-------------------------------------------------------------------------
 */
static RfPageCtrcStepResultV1
pending_window_ready(void)
{
 RfPageCtrcStepResultV1 saved;
 prefix_ready();
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 saved = cluster_recovery_cold_start->ctrc_page_progress->result;
 UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&saved.inserted.window,
  saved.inserted.record_start, saved.inserted.record_end));
 return saved;
}

static void
pending_window_debt_unchanged(const RfPageCtrcStepResultV1 *saved)
{
 const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
 UT_ASSERT(progress != NULL && progress->pending);
 UT_ASSERT(memcmp(saved, &progress->result, sizeof(*saved)) == 0);
 UT_ASSERT_EQ(progress->next_target, 0); UT_ASSERT_EQ(prefix_steps, 1);
 /* Every repeat may prepare another local readback PAGE/proof, but all of
  * them are destroyed without releasing the borrowed recovery protections. */
 UT_ASSERT_EQ(prefix_proofs, 1 + pending_readback_calls);
 UT_ASSERT_EQ(prefix_pages, 1 + pending_readback_calls);
 UT_ASSERT_EQ(prefix_destroyed, prefix_proofs + prefix_pages);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT(info.cluster_cold_inputs != NULL && info.cluster_tx_start != NULL);
}

UT_TEST(test_startup_pending_fpi_uses_original_shared_window_without_advancing)
{
 RfPageCtrcStepResultV1 saved = pending_window_ready();
 ClusterRecoveryStampSlotV1 before;
 ClusterThreadReplaySlot *slot = cluster_thread_recovery_replay_slot(saved.inserted.window.key.target_thread);
 stamp_snapshot(&slot->stamp, &before);
 for (unsigned i = 0; i < 3; i++) {
  UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
  UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 }
 UT_ASSERT(memcmp(&before.handle, &slot->stamp.handle, sizeof(before.handle)) == 0);
 UT_ASSERT_EQ(slot->stamp.phase, CLUSTER_RECOVERY_STAMP_DURABLE);
 UT_ASSERT_EQ(slot->stamp.record_start, before.record_start);
 UT_ASSERT_EQ(slot->stamp.record_end, before.record_end); UT_ASSERT(!slot->stamp.failed);
 pending_window_debt_unchanged(&saved);
}

UT_TEST(test_startup_pending_fpi_refuses_original_failed_window)
{
 RfPageCtrcStepResultV1 saved = pending_window_ready();
 UT_ASSERT(cluster_recovery_stamp_fail_v1(&saved.inserted.window));
 UT_ASSERT(!cluster_recovery_stamp_fpi_durable_current_v1(&saved.inserted.window,
  saved.inserted.record_start, saved.inserted.record_end));
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 pending_window_debt_unchanged(&saved);
 UT_ASSERT(!cluster_recovery_stamp_clear_v1(&saved.inserted.window));
}

UT_TEST(test_startup_pending_fpi_cannot_rebind_a_later_original_window)
{
 RfPageCtrcStepResultV1 saved = pending_window_ready();
 ClusterRecoveryStampHandleV1 next;
 bool inserted;
 /* Simulate retirement/replacement at the shared owner, not a discharge
  * producer. The original saved operation must remain unmodified/refused. */
 UT_ASSERT(cluster_recovery_stamp_clear_v1(&saved.inserted.window));
 UT_ASSERT(stamp_prepare(&saved.inserted.window.owner, CLUSTER_RECOVERY_STAMP_FPI,
  saved.inserted.window.payload, saved.inserted.window.payload_length, &next));
 CritSectionCount++;
 inserted = cluster_recovery_stamp_inserting_v1(&next)
  && cluster_recovery_stamp_inserted_v1(&next, saved.inserted.record_start, saved.inserted.record_end);
 CritSectionCount--;
 UT_ASSERT(inserted); UT_ASSERT(cluster_recovery_stamp_durable_v1(&next));
 UT_ASSERT(next.key.window_seq > saved.inserted.window.key.window_seq);
 UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&next,
  saved.inserted.record_start, saved.inserted.record_end));
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 pending_window_debt_unchanged(&saved);
}

UT_TEST(test_startup_pending_fpi_requires_complete_saved_operation)
{
 for (unsigned fault = 0; fault < 6; fault++) {
  RfPageCtrcStepResultV1 original = pending_window_ready(), changed;
  RfPageCtrcStepResultV1 *result = &cluster_recovery_cold_start->ctrc_page_progress->result;
  if (fault == 0) result->phase = RF_PAGE_CTRC_STEP_TARGET_SYNCED;
  if (fault == 1) result->inserted.wal_flushed = false;
  if (fault == 2) result->inserted.record_end++;
  if (fault == 3) result->inserted.window.payload[0] ^= 1;
  if (fault == 4) result->inserted.window.owner.recovery_serial++;
  if (fault == 5) result->inserted.window.key.window_seq++;
  changed = *result;
  UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&original.inserted.window,
   original.inserted.record_start, original.inserted.record_end));
  UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
  UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
  pending_window_debt_unchanged(&changed);
 }
}

static void
pending_window_fail_on_first_wait(void)
{
 if (tx_waits == 1) UT_ASSERT(cluster_recovery_stamp_fail_v1(&prefix_window));
 else owner_boot++; /* Bound the unchanged-parent regression without a deadline. */
}

UT_TEST(test_actual_startup_pending_fpi_failure_interrupts_service_wait)
{
 ctrc_handoff_fixture(false, false); prefix_setup_targets();
 ctrc_wait_step = pending_window_fail_on_first_wait;
 fatal_armed = true;
 if (setjmp(fatal_jump) == 0) { actual_startup_finish_tx(&info); UT_ASSERT(false); }
 fatal_armed = false;
 UT_ASSERT_EQ(tx_waits, 1);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 pending_window_debt_unchanged(&cluster_recovery_cold_start->ctrc_page_progress->result);
 UT_ASSERT(cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.failed);
}
