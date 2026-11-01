/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_handoff.inc
 *     Saved replay receipts survive the original replay-window boundary.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_handoff.inc
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
UT_TEST(test_relmap_saved_receipt_carries_actual_replay_without_new_io)
{
	ClusterColdRelmapWalV1 wal;
	ClusterColdRelmapReplayReceiptV1 value;
	TestColdRelmapStorageStats before, after;
	startup_relmap_replay_fixture();
	wal = startup_relmap_wal_owner();
	startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_wal_v1(
		(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(startup_relmap_replay.typed), UINT32_MAX);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	cluster_cold_replay_window_leave_v1();
	before = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(startup_relmap_replay.typed), 1);
	value = cluster_cold_typed_relmap_replay_input_v1(startup_relmap_replay.typed, 0);
	UT_ASSERT_EQ(value.count, 1);
	UT_ASSERT_EQ(memcmp(&value.input, &startup_relmap_replay.input, sizeof(value.input)), 0);
	UT_ASSERT_EQ(value.input.retention, wal.retention);
	value.input.staged.pending.mappings[0].mapoid++;
	value = cluster_cold_typed_relmap_replay_input_v1(startup_relmap_replay.typed, 0);
	UT_ASSERT_EQ(memcmp(&value.input.staged, &startup_relmap_replay.input.staged,
		sizeof(value.input.staged)), 0);
	after = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(after.reads, before.reads);
	UT_ASSERT_EQ(after.writes, before.writes);
	UT_ASSERT(after.pin_retained && after.may_be_staged);
	cluster_cold_replay_window_enter_v1();
	startup_relmap_replay_end();
}

UT_TEST(test_relmap_saved_receipt_refuses_partial_failed_or_other_source_batch)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterColdTypedV1 *typed;
		ClusterColdRelmapReplayReceiptV1 value, empty = {0};
		TestColdRelmapStorageStats before, after;
		startup_relmap_replay_begin();
		typed = startup_relmap_replay.typed;
		UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
		before = test_cold_relmap_storage_stats();
		if (fault == 0) typed->plan->relmap_inputs[0].applied = false;
		if (fault == 1) typed->plan->relmap_failed = true;
		if (fault == 2) typed->sources[0].timeline++;
		if (fault == 3) typed->participants[0].tail_end++;
		if (fault == 4) typed->plan->relmap_inputs[0].attempted = false;
		UT_ASSERT_EQ(cluster_cold_typed_relmap_replay_count_v1(typed), UINT32_MAX);
		value = cluster_cold_typed_relmap_replay_input_v1(typed, 0);
		UT_ASSERT_EQ(memcmp(&value, &empty, sizeof(value)), 0);
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(after.reads, before.reads);
		UT_ASSERT_EQ(after.writes, before.writes);
		UT_ASSERT(after.pin_retained && after.may_be_staged);
		startup_relmap_replay_end();
	}
}
