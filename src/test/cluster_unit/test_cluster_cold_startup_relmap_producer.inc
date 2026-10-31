/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_producer.inc
 *     Original pin and decoded WAL to Startup declaration and replay.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_producer.inc
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static ClusterColdRelmapWalV1
startup_relmap_wal_owner(void)
{
	const ClusterColdRelmapInputV1 *input = &startup_relmap_replay.input;
	ClusterColdRelmapWalV1 wal = {0};
	wal.participant = input->participant;
	wal.identity = input->staged.identity;
	wal.generation = input->staged.pending_generation;
	wal.owner = input->staged.pending_owner;
	wal.owner.wal_end = 0;
	wal.retention = input->retention;
	wal.retention_lock = input->retention_lock;
	wal.reader = &startup_relmap_replay.reader;
	return wal;
}

static ClusterColdPlanV1 *
startup_relmap_producer_plan(void)
{
	ClusterColdPlanV1 *plan = NULL;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&startup_relmap_replay.input.cut,
		1, 4 * 1024 * 1024, &plan), CLUSTER_COLD_OK);
	return plan;
}

UT_TEST(test_startup_relmap_produces_declaration_from_original_pin_and_wal)
{
	ClusterColdRelmapWalV1 wal;
	const ClusterColdRelmapInputV1 *built;
	TestColdRelmapStorageStats stats;
	startup_relmap_replay_fixture();
	wal = startup_relmap_wal_owner();
	startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_wal_v1(
		(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
	UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
	UT_ASSERT_EQ(startup_relmap_replay.typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(startup_relmap_replay.typed->plan->relmap_count, 1);
	built = &startup_relmap_replay.typed->plan->relmap_inputs[0].input;
	UT_ASSERT_EQ(memcmp(&built->staged, &startup_relmap_replay.input.staged,
		sizeof(built->staged)), 0);
	UT_ASSERT(cold_relmap_cut_equal(&built->cut, &startup_relmap_replay.input.cut));
	UT_ASSERT_EQ(memcmp(&built->frame, &startup_relmap_replay.input.frame,
		sizeof(built->frame)), 0);
	UT_ASSERT_EQ(built->retention, wal.retention);
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT(stats.reads > 0 && stats.writes == 0 && stats.pin_retained);
	UT_ASSERT(stats.may_be_staged);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(stats.writes, 2);
	UT_ASSERT_EQ(stats.io_under_retention_lock, 0);
	startup_relmap_replay_end();
}

UT_TEST(test_relmap_producer_preflights_whole_batch_and_budget_before_io)
{
	for (unsigned fault = 0; fault < 12; ++fault) {
		ClusterColdPlanV1 *plan;
		ClusterColdRelmapWalV1 wal[2];
		ClusterColdDetailV1 detail;
		TestColdRelmapStorageStats stats;
		uint32 count = 1;
		startup_relmap_replay_fixture();
		plan = startup_relmap_producer_plan();
		wal[0] = wal[1] = startup_relmap_wal_owner();
		if (fault == 0) wal[0].owner.top_full_xid++;
		if (fault == 1) wal[0].owner.boot_incarnation++;
		if (fault == 2) wal[0].reader->seg.ws_tli++;
		if (fault == 3) wal[0].reader->record->main_data_len--;
		if (fault == 4) { count = 2; wal[1].owner.top_full_xid++; }
		if (fault == 5) count = 2;
		if (fault == 6) plan->memory_budget = plan->memory_used + sizeof(ClusterColdRelmapInputV1);
		if (fault == 7) wal[0].participant = 1;
		if (fault == 8) wal[0].retention->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 9) wal[0].owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 10) wal[0].owner.wal_end = wal[0].reader->EndRecPtr;
		if (fault == 11) wal[0].reader->record->main_data[MinSizeOfRelmapUpdate] ^= 1;
		detail = cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, wal, count);
		UT_ASSERT(detail != CLUSTER_COLD_OK);
		UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
		UT_ASSERT_NULL(plan->relmap_inputs);
		stats = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(stats.reads, 0);
		UT_ASSERT(stats.writes == 0 && stats.pin_retained && !stats.may_be_staged);
		wal[0] = startup_relmap_wal_owner();
		UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, wal, 1), CLUSTER_COLD_STATE);
		cluster_cold_plan_destroy_v1(&plan);
		startup_relmap_real_side = false;
		test_cold_relmap_storage_end();
	}
}

static unsigned startup_relmap_producer_fault;
static void
startup_relmap_producer_after_read(unsigned reads)
{
	if (reads != 1) return;
	if (startup_relmap_producer_fault == 0) startup_relmap_replay.decoded.header.xl_crc++;
	if (startup_relmap_producer_fault == 1) test_cold_relmap_storage_pin_error();
	if (startup_relmap_producer_fault == 2) tokens[3].file_txn_seq++;
}

UT_TEST(test_relmap_producer_late_record_or_pin_error_is_sticky)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterColdPlanV1 *plan;
		ClusterColdRelmapWalV1 wal;
		Size used;
		volatile bool caught = false;
		volatile ClusterColdDetailV1 detail = CLUSTER_COLD_OK;
		TestColdRelmapStorageStats before, after;
		startup_relmap_replay_fixture();
		plan = startup_relmap_producer_plan();
		wal = startup_relmap_wal_owner();
		used = plan->memory_used;
		startup_relmap_producer_fault = fault;
		test_cold_relmap_storage_read_hook(startup_relmap_producer_after_read);
		PG_TRY(); {
			detail = cluster_cold_plan_relmap_from_wal_v1(plan,
				&startup_relmap_replay.input.source, 1, &wal, 1);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(fault == 1 ? caught : detail != CLUSTER_COLD_OK);
		UT_ASSERT(plan->relmap_failed && plan->phase == COLD_PHASE_FAILED);
		UT_ASSERT_EQ(plan->memory_used, used);
		UT_ASSERT_NULL(plan->relmap_inputs);
		before = test_cold_relmap_storage_stats();
		UT_ASSERT(before.reads > 0 && before.writes == 0 && before.pin_retained);
		UT_ASSERT(before.may_be_staged);
		UT_ASSERT_EQ(before.io_under_retention_lock, 0);
		UT_ASSERT_EQ(cluster_cold_plan_relmap_from_wal_v1(plan,
			&startup_relmap_replay.input.source, 1, &wal, 1), CLUSTER_COLD_STATE);
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(after.reads, before.reads);
		cluster_cold_plan_destroy_v1(&plan);
		startup_relmap_real_side = false;
		test_cold_relmap_storage_end();
	}
}

UT_TEST(test_startup_relmap_producer_rechecks_sources_before_pass_one)
{
	ClusterColdRelmapWalV1 wal;
	ClusterColdTypedV1 *typed;
	TestColdRelmapStorageStats stats;
	startup_relmap_replay_fixture();
	wal = startup_relmap_wal_owner();
	startup_relmap_producer_fault = 2;
	test_cold_relmap_storage_read_hook(startup_relmap_producer_after_read);
	typed = cluster_cold_typed_prepare_relmap_wal_v1(
		(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
	test_cold_relmap_storage_read_hook(NULL);
	UT_ASSERT_NOT_NULL(typed);
	UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
	UT_ASSERT(typed->plan->relmap_failed && typed->plan->phase == COLD_PHASE_FAILED);
	UT_ASSERT_EQ(typed->scanned_records, 0);
	stats = test_cold_relmap_storage_stats();
	UT_ASSERT(stats.reads > 0 && stats.writes == 0 && stats.pin_retained && stats.may_be_staged);
	cluster_cold_typed_destroy_v1(&typed);
	UT_ASSERT_EQ(contexts_alive, 0);
	startup_relmap_real_side = false;
	test_cold_relmap_storage_end();
}
