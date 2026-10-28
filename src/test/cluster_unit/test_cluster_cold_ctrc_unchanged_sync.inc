/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_unchanged_sync.inc
 *     Startup consumes explicit local sync, never observation-only no-op.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_unchanged_sync.inc
 *
 * NOTES
 *     Actual extracted Startup caller; PAGE storage transport is a unit leaf.
 *-------------------------------------------------------------------------
 */
static bool prefix_legacy_unchanged;
static unsigned prefix_sync_steps;

RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_sync_smgr_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space, BlockNumber block,
 const RfPageVersionV1 *before, const char canonical[BLCKSZ],
 const ClusterRecoveryUndoEntry *physical, Size count, Size budget)
{
 RfPageCtrcStepResultV1 result = rf_page_recovery_ctrc_replays_itl_graph_durable_smgr_v1(
  start, work, source, authority, space, block, before, canonical, physical, count, budget);
 prefix_sync_steps++;
 if (result.phase == RF_PAGE_CTRC_STEP_UNCHANGED && !prefix_legacy_unchanged)
  result.phase = RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED;
 return result;
}

UT_TEST(test_startup_unchanged_sync_advances_without_service_completion)
{
 prefix_ready(); prefix_unchanged = true; prefix_legacy_unchanged = false; prefix_sync_steps = 0;
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_sync_steps, 2); UT_ASSERT_EQ(prefix_steps, 2);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 2);
 UT_ASSERT(!cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}

UT_TEST(test_startup_legacy_unchanged_cannot_advance_or_complete)
{
 prefix_ready(); prefix_unchanged = true; prefix_legacy_unchanged = true; prefix_sync_steps = 0;
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_sync_steps, 1); UT_ASSERT_EQ(prefix_steps, 1);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
 UT_ASSERT(!cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 prefix_legacy_unchanged = false;
}
