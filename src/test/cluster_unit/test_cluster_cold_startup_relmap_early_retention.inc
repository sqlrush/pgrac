/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_early_retention.inc
 *     Keep the original batch of pins before the first baseline file read.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_early_retention.inc
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static ClusterColdRelmapWalV1 early_retention_wal[2];
static DecodedXLogRecord early_retention_decoded;
static XLogReaderState early_retention_reader;
static char early_retention_payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
static unsigned early_retention_observed;
static ClusterRelmapRetentionResult early_retention_cancelled;

static void
early_retention_batch(void)
{
	xl_relmap_update header = {42, DEFAULTTABLESPACE_OID, sizeof(RelMapFile)};
	startup_relmap_replay_fixture();
	early_retention_wal[0] = early_retention_wal[1] = startup_relmap_wal_owner();
	startup_relmap_replay.reader.EndRecPtr = startup_relmap_replay.decoded.next_lsn = 0x1000;
	early_retention_reader = startup_relmap_replay.reader;
	early_retention_decoded = startup_relmap_replay.decoded;
	early_retention_reader.record = &early_retention_decoded;
	early_retention_reader.ReadRecPtr = early_retention_decoded.lsn = 0x1000;
	early_retention_reader.EndRecPtr = early_retention_decoded.next_lsn = 0x2000;
	early_retention_decoded.main_data = early_retention_payload;
	memcpy(early_retention_payload, &header, MinSizeOfRelmapUpdate);
	memcpy(early_retention_payload + MinSizeOfRelmapUpdate,
		&startup_relmap_replay.input.staged.pending, sizeof(RelMapFile));
	early_retention_wal[1].reader = &early_retention_reader;
	early_retention_wal[1].identity.shared = false;
	early_retention_wal[1].identity.dbid = 42;
	test_cold_relmap_storage_add_namespace(&early_retention_wal[1]);
	early_retention_observed = 0;
}

static void
early_retention_check_batch(unsigned reads)
{
	TestColdRelmapStorageStats stats = test_cold_relmap_storage_stats();
	(void)reads;
	UT_ASSERT_EQ(stats.pins, 2);
	UT_ASSERT_EQ(stats.armed_pins, 2);
	UT_ASSERT_EQ(stats.io_under_retention_lock, 0);
	++early_retention_observed;
}

static void
early_retention_cancel_on_read(unsigned reads)
{
	if (reads == 1)
		early_retention_cancelled = test_cold_relmap_storage_cancel(&early_retention_wal[0]);
}

static void
early_retention_end(ClusterColdPlanV1 **plan)
{
	cluster_cold_plan_destroy_v1(plan);
	startup_relmap_real_side = false;
	test_cold_relmap_storage_end();
}

UT_TEST(test_relmap_original_pin_cannot_cancel_at_first_baseline_read)
{
	ClusterColdPlanV1 *plan;
	startup_relmap_replay_fixture();
	early_retention_wal[0] = startup_relmap_wal_owner();
	plan = startup_relmap_producer_plan();
	early_retention_cancelled = CLUSTER_RELMAP_RETENTION_OK;
	test_cold_relmap_storage_read_hook(early_retention_cancel_on_read);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
		&startup_relmap_replay.input.source, 1, early_retention_wal, 1), CLUSTER_COLD_OK);
	test_cold_relmap_storage_read_hook(NULL);
	UT_ASSERT_EQ(early_retention_cancelled, CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, 0);
	UT_ASSERT(test_cold_relmap_storage_stats().may_be_staged);
	cluster_cold_plan_destroy_v1(&plan);
	UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[0]), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT(test_cold_relmap_storage_stats().pin_retained);
	early_retention_end(&plan);
}

UT_TEST(test_relmap_entire_pin_batch_is_armed_before_any_baseline_io)
{
	ClusterColdPlanV1 *plan;
	early_retention_batch();
	plan = startup_relmap_producer_plan();
	test_cold_relmap_storage_read_hook(early_retention_check_batch);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
		&startup_relmap_replay.input.source, 1, early_retention_wal, 2), CLUSTER_COLD_OK);
	test_cold_relmap_storage_read_hook(NULL);
	UT_ASSERT(early_retention_observed > 0);
	UT_ASSERT_EQ(plan->relmap_count, 2);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, 0);
	cluster_cold_plan_destroy_v1(&plan);
	for (unsigned i = 0; i < 2; ++i)
		UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[i]), CLUSTER_RELMAP_RETENTION_RETAINED);
	early_retention_end(&plan);
}

UT_TEST(test_relmap_later_preflight_refusal_does_not_arm_any_pin)
{
	ClusterColdPlanV1 *plan;
	ClusterColdRelmapWalV1 bad[2];
	TestColdRelmapStorageStats stats;
	early_retention_batch();
	plan = startup_relmap_producer_plan();
	memcpy(bad, early_retention_wal, sizeof(bad));
	++bad[1].owner.boot_incarnation;
	UT_ASSERT(cluster_cold_plan_relmap_from_wal_v1(plan,
		&startup_relmap_replay.input.source, 1, bad, 2) != CLUSTER_COLD_OK);
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT(stats.pins == 2 && stats.armed_pins == 0 && stats.reads == 0 && stats.writes == 0);
	for (unsigned i = 0; i < 2; ++i)
		UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[i]), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
	early_retention_end(&plan);
}

UT_TEST(test_relmap_partial_batch_arm_error_keeps_already_armed_pin)
{
	ClusterColdPlanV1 *plan;
	volatile bool caught = false;
	TestColdRelmapStorageStats stats;
	Size used;
	early_retention_batch();
	plan = startup_relmap_producer_plan();
	used = plan->memory_used;
	/* Two captures, first arm, then ERROR entering the second arm lock. */
	test_cold_relmap_storage_pin_error_after(4);
	PG_TRY(); {
		(void)cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, early_retention_wal, 2);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
	UT_ASSERT_EQ(plan->memory_used, used);
	UT_ASSERT_NULL(plan->relmap_inputs);
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT(stats.pins == 2 && stats.armed_pins == 1 && stats.reads == 0 && stats.writes == 0);
	UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[0]), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[1]), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
		&startup_relmap_replay.input.source, 1, early_retention_wal, 2), CLUSTER_COLD_STATE);
	early_retention_end(&plan);
}

UT_TEST(test_relmap_late_producer_refusal_or_error_keeps_armed_debt)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterColdPlanV1 *plan;
		volatile bool caught = false;
		volatile ClusterColdDetailV1 detail = CLUSTER_COLD_OK;
		TestColdRelmapStorageStats stats;
		startup_relmap_replay_fixture();
		plan = startup_relmap_producer_plan();
		early_retention_wal[0] = startup_relmap_wal_owner();
		startup_relmap_producer_fault = fault;
		test_cold_relmap_storage_read_hook(startup_relmap_producer_after_read);
		PG_TRY(); {
			detail = cluster_cold_plan_relmap_from_wal_v1(plan,
				&startup_relmap_replay.input.source, 1, early_retention_wal, 1);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(fault == 1 ? caught : detail != CLUSTER_COLD_OK);
		UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
		stats = test_cold_relmap_storage_stats();
		UT_ASSERT(stats.reads > 0 && stats.writes == 0 && stats.armed_pins == 1);
		UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, early_retention_wal, 1), CLUSTER_COLD_STATE);
		cluster_cold_plan_destroy_v1(&plan);
		UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[0]), CLUSTER_RELMAP_RETENTION_RETAINED);
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, stats.reads);
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().io_under_retention_lock, 0);
		early_retention_end(&plan);
	}
}

UT_TEST(test_relmap_early_pin_hold_survives_startup_replay_and_destruction)
{
	TestColdRelmapStorageStats stats;
	startup_relmap_pin_prepare(true);
	early_retention_wal[0] = startup_relmap_wal_owner();
	UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[0]), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(startup_relmap_replay.pages, 2);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	cluster_cold_typed_destroy_v1(&startup_relmap_replay.typed);
	cluster_cold_replay_window_leave_v1();
	UT_ASSERT_EQ(test_cold_relmap_storage_cancel(&early_retention_wal[0]), CLUSTER_RELMAP_RETENTION_RETAINED);
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT(stats.pin_retained && stats.may_be_staged && stats.writes == 2);
	UT_ASSERT_EQ(stats.io_under_retention_lock, 0);
	startup_relmap_real_side = false;
	UT_ASSERT_EQ(contexts_alive, 0);
	test_cold_relmap_storage_end();
}
