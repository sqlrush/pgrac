/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_pin_custody.inc
 *     Preserve actual original pin metadata from declaration through replay.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_pin_custody.inc
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static unsigned startup_relmap_pin_fault;

static void
startup_relmap_pin_after_read(unsigned reads)
{
	if (reads == 1)
		test_cold_relmap_storage_pin_drift(startup_relmap_pin_fault);
}

static void
startup_relmap_pin_prepare(bool bound)
{
	ClusterColdRelmapWalV1 wal;
	startup_relmap_replay_fixture();
	if (bound) test_cold_relmap_storage_pin_drift(5);
	wal = startup_relmap_wal_owner();
	startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_wal_v1(
		(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
	UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
	UT_ASSERT_EQ(startup_relmap_replay.typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_enter_v1();
}

UT_TEST(test_relmap_pin_snapshot_refuses_drift_during_baseline_read)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		ClusterColdPlanV1 *plan;
		ClusterColdRelmapWalV1 wal;
		Size used;
		TestColdRelmapStorageStats before, after;
		startup_relmap_replay_fixture();
		if (fault == 8) test_cold_relmap_storage_pin_drift(5);
		plan = startup_relmap_producer_plan();
		used = plan->memory_used;
		wal = startup_relmap_wal_owner();
		startup_relmap_pin_fault = fault;
		test_cold_relmap_storage_read_hook(startup_relmap_pin_after_read);
		UT_ASSERT(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, &wal, 1) != CLUSTER_COLD_OK);
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
		UT_ASSERT_NULL(plan->relmap_inputs);
		UT_ASSERT_EQ(plan->memory_used, used);
		before = test_cold_relmap_storage_stats();
		UT_ASSERT(before.reads > 0 && before.writes == 0 && before.pin_retained);
		UT_ASSERT(before.may_be_staged && before.io_under_retention_lock == 0);
		UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, &wal, 1), CLUSTER_COLD_STATE);
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(before.reads, after.reads);
		cluster_cold_plan_destroy_v1(&plan);
		startup_relmap_real_side = false;
		test_cold_relmap_storage_end();
	}
}

UT_TEST(test_relmap_pin_snapshot_enumerates_duplicate_slots_before_io)
{
	for (unsigned fault = 6; fault < 8; ++fault) {
		ClusterColdPlanV1 *plan;
		ClusterColdRelmapWalV1 wal;
		TestColdRelmapStorageStats stats;
		startup_relmap_replay_fixture();
		plan = startup_relmap_producer_plan();
		wal = startup_relmap_wal_owner();
		test_cold_relmap_storage_pin_drift(fault);
		UT_ASSERT(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, &wal, 1) != CLUSTER_COLD_OK);
		UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
		UT_ASSERT_NULL(plan->relmap_inputs);
		stats = test_cold_relmap_storage_stats();
		UT_ASSERT(stats.reads == 0 && stats.writes == 0 && stats.pin_retained);
		UT_ASSERT(!stats.may_be_staged && stats.io_under_retention_lock == 0);
		cluster_cold_plan_destroy_v1(&plan);
		startup_relmap_real_side = false;
		test_cold_relmap_storage_end();
	}
}

UT_TEST(test_relmap_pin_snapshot_survives_startup_declaration_until_replay)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		TestColdRelmapStorageStats stats;
		ClusterColdStepV1 step;
		startup_relmap_pin_prepare(fault == 8);
		test_cold_relmap_storage_pin_drift(fault);
		UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_CALLBACK);
		UT_ASSERT(startup_relmap_replay.typed->plan->relmap_failed);
		stats = test_cold_relmap_storage_stats();
		UT_ASSERT(stats.writes == 0 && stats.pin_retained && stats.may_be_staged);
		UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
		UT_ASSERT(!cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, 0);
		startup_relmap_replay_end();
	}
}

static bool startup_relmap_pin_changed;
static void
startup_relmap_pin_after_sync(bool directory)
{
	if (!directory && !startup_relmap_pin_changed) {
		startup_relmap_pin_changed = true;
		test_cold_relmap_storage_pin_drift(0);
	}
}

UT_TEST(test_relmap_pin_snapshot_refuses_drift_after_durable_pair_write)
{
	ClusterColdStepV1 step;
	TestColdRelmapStorageStats before, after;
	startup_relmap_pin_prepare(false);
	startup_relmap_pin_changed = false;
	test_cold_relmap_storage_fault(0, startup_relmap_pin_after_sync);
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_CALLBACK);
	test_cold_relmap_storage_fault(0, NULL);
	UT_ASSERT(startup_relmap_pin_changed && startup_relmap_replay.typed->plan->relmap_failed);
	before = test_cold_relmap_storage_stats();
	UT_ASSERT(before.writes == 2 && before.pin_retained && before.may_be_staged);
	UT_ASSERT(test_cold_relmap_storage_matches(&startup_relmap_replay.input, true));
	UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
	UT_ASSERT(!cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	after = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(before.writes, after.writes);
	UT_ASSERT_EQ(after.io_under_retention_lock, 0);
	startup_relmap_replay_end();
}

UT_TEST(test_relmap_pin_snapshot_rechecks_receipt_and_duplicate_without_writes)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		ClusterColdStepV1 step;
		TestColdRelmapStorageStats before, after;
		startup_relmap_pin_prepare(fault == 8);
		UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
		before = test_cold_relmap_storage_stats();
		test_cold_relmap_storage_pin_drift(fault);
		UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
		/* Alternate the first observer so both duplicate and final checks refuse. */
		if ((fault & 1) != 0)
			UT_ASSERT(!cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
				(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		UT_ASSERT(!cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(before.writes, after.writes);
		UT_ASSERT(after.writes == 2 && after.pin_retained && after.may_be_staged);
		UT_ASSERT_EQ(after.io_under_retention_lock, 0);
		startup_relmap_replay_end();
	}
}

UT_TEST(test_relmap_pin_snapshot_allows_only_original_stage_transition)
{
	ClusterColdStepV1 step;
	TestColdRelmapStorageStats before, after;
	startup_relmap_pin_prepare(true);
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(startup_relmap_replay.pages, 2);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	before = test_cold_relmap_storage_stats();
	UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
	UT_ASSERT(cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
	after = test_cold_relmap_storage_stats();
	UT_ASSERT(after.writes == 2 && after.writes == before.writes);
	UT_ASSERT(after.pin_retained && after.may_be_staged && after.io_under_retention_lock == 0);
	startup_relmap_replay_end();
}
