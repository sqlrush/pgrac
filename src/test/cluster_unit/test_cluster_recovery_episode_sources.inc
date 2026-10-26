/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_sources.inc
 *     Complete original source observations in relmap history/base views.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_sources.inc
 * NOTES
 *     Original physical ROOT/claim/anchor/WAL/history inputs execute. Startup,
 *     GES and the base leaf keep their existing controlled fixture boundaries.
 *     Copied source values grant no era, lifetime or completion authority.
 *-------------------------------------------------------------------------
 */
UT_TEST(test_episode_sources_keep_complete_zero_page_generations_by_value)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapHistoryEntryV1 entry, again;
	unsigned calls;

	episode_history_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	calls = inventory_census_calls;
	UT_ASSERT_EQ(diag.sources, 2);
	for (uint32 i = 0; i < diag.sources; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, i, &entry));
		UT_ASSERT_EQ(memcmp(&entry.observation.source, &entry.source.selected.source,
			sizeof(entry.observation.source)), 0);
		UT_ASSERT_EQ(entry.observation.source_index, i);
		UT_ASSERT(entry.observation.cut.flags & RF_CONTRIBUTOR_CUT_COMPLETE);
		UT_ASSERT_EQ(entry.observation.observed.records, 1); /* native checkpoint only */
		UT_ASSERT_EQ(entry.observation.observed.complete_end, entry.checkpoint_end);
		UT_ASSERT_EQ(entry.observation.cut.scan_end_exclusive, entry.checkpoint_end);
		UT_ASSERT_EQ(entry.observation.observed.database_incarnation,
			entry.source.selected.source.claim.database_incarnation);
		UT_ASSERT(!v2_zero(entry.observation.transcript_sha256, 32));
		again = entry;
		memset(&entry.observation, 0x5a, sizeof(entry.observation));
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, i, &entry));
		UT_ASSERT_EQ(memcmp(&entry, &again, sizeof(entry)), 0);
	}
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_sources_base_keeps_full_tail_after_native_map)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary counts;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryRelmapHistoryEntryV1 entry;
	ClusterRecoveryNativeInventoryDiag diag;

	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &counts),
		CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	UT_ASSERT_EQ(base.history.observation.observed.records, 6);
	UT_ASSERT(base.history.observation.observed.complete_end > pending.pending_owner.wal_end);
	UT_ASSERT_EQ(base.history.observation.observed.complete_end,
		base.history.observation.cut.scan_end_exclusive);
	UT_ASSERT_EQ(base.history.observation.observed.complete_end,
		base.history.source.selected.checkpoint.validated_tail_lsn_exclusive);
	UT_ASSERT_EQ(counts.clog_after_wal, 2);
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history,
		base.history.observation.source_index, &entry));
	UT_ASSERT_EQ(memcmp(&base.history, &entry, sizeof(entry)), 0);
	UT_ASSERT_EQ(episode_base_calls, 1);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	UT_ASSERT_EQ(episode_base_calls, 2);
	episode_history_done(&history, &inputs);
	UT_ASSERT_EQ(episode_base_live, 0);
	episode_base_enabled = false;
}

UT_TEST(test_episode_sources_reject_borrowed_output_without_changing_baseline)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	NativeInventorySource saved;
	ClusterRecoveryRelmapHistoryEntryV1 entry;

	episode_history_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	saved = history->inventory->sources[0];
	UT_ASSERT(sizeof(entry) <= sizeof(saved) * diag.sources);
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0,
		(ClusterRecoveryRelmapHistoryEntryV1 *)&history->inventory->sources[0]));
	UT_ASSERT_EQ(memcmp(&history->inventory->sources[0], &saved, sizeof(saved)), 0);
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_sources_sibling_metadata_drift_revokes_complete_view)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapHistoryEntryV1 entry, zero = {0};
	ClusterWalInputV1 *sibling;
	unsigned calls;

	episode_history_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	sibling = (ClusterWalInputV1 *)cluster_wal_inputs_at_v1(inputs,
		entry.source.input_index == 0 ? 1 : 0);
	UT_ASSERT(sibling != NULL);
	sibling->native_window[64] ^= 1;
	calls = inventory_census_calls;
	memset(&entry, 0x5a, sizeof(entry));
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	episode_history_done(&history, &inputs);
}
