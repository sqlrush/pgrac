/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_recovery.inc
 *     Private recovery custody through original replay and real pair writes.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_recovery.inc
 * NOTES
 *     Publication is a controlled owner boundary, not a native joint proof.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_relmap_recovery_internal.h"


static ClusterColdRelmapWalV1
startup_recovery_wal(void)
{
	ClusterColdRelmapWalV1 wal = startup_relmap_wal_owner();
	cluster_relmap_retention_invalidate(wal.retention);
	startup_relmap_replay.input.retention = NULL;
	startup_relmap_replay.input.retention_lock = NULL;
	wal.retention = NULL;
	wal.retention_lock = NULL;
	return wal;
}

/* Only test-owned failed episodes may be retired at simulated process exit.
 * Production cannot discard this debt or free the containing context. */
static void
startup_recovery_end(void)
{
	if (startup_relmap_replay.typed != NULL
		&& startup_relmap_replay.typed->plan->relmap_recovery != NULL)
		startup_relmap_replay.typed->plan->relmap_recovery->transferred = true;
	startup_relmap_replay_end();
}

UT_TEST(test_recovery_carrier_stages_without_publishing_ordinary_directory)
{
	ClusterColdRelmapWalV1 wal;
	ClusterColdPlanV1 *plan;
	ClusterRelmapRecoveryPins *pins;
	ClusterRelmapRetentionDirectory target = {0};
	ClusterRelmapRetentionDirectory *targets[1] = {&target};
	TestColdRelmapStorageStats before, after;
	startup_relmap_replay_fixture();
	wal = startup_recovery_wal();
	startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_recovery_wal_v1(
		(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
	UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
	UT_ASSERT_EQ(startup_relmap_replay.typed->refusal, CLUSTER_COLD_OK);
	plan = startup_relmap_replay.typed->plan;
	pins = plan->relmap_recovery;
	UT_ASSERT_NOT_NULL(pins);
	UT_ASSERT(pins->restored && !pins->transferred && pins->count == 1);
	UT_ASSERT_EQ(pins->state, CLUSTER_RELMAP_RETENTION_BUILDING);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	before = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(before.writes, 2);
	UT_ASSERT(pins->attempted[0] && pins->staged[0]);
	UT_ASSERT(!cluster_relmap_recovery_transfer(pins, targets, 1));
	UT_ASSERT(!pins->transferred && pins->count == 1);
	/* Simulated CTRC refusal: no hook, no publication or retirement. */
	cluster_cold_typed_destroy_v1(&startup_relmap_replay.typed);
	UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(&target,
		wal.identity.system_identifier, wal.identity.database_incarnation,
		wal.owner.node, 2, wal.owner.top_full_xid - 16, wal.owner.top_full_xid + 16),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&target), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!cluster_cold_typed_relmap_recovery_transfer_v1(
		startup_relmap_replay.typed, targets, 1, NULL));
	CritSectionCount = 1;
	UT_ASSERT(cluster_cold_typed_relmap_recovery_transfer_v1(
		startup_relmap_replay.typed, targets, 1, test_cold_relmap_storage_publication_lock(true)));
	test_cold_relmap_storage_publication_lock(false);
	CritSectionCount = 0;
	UT_ASSERT(pins->transferred && pins->count == 1 && target.pins[0].occupied);
	UT_ASSERT(target.pins[0].may_be_staged);
	UT_ASSERT_EQ(target.pins[0].wal_end, wal.reader->EndRecPtr);
	UT_ASSERT_EQ(pins->state, CLUSTER_RELMAP_RETENTION_BUILDING);
	after = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(before.writes, after.writes);
	UT_ASSERT_EQ(after.io_under_retention_lock, 0);
	startup_relmap_replay_end();
}

UT_TEST(test_recovery_carrier_half_write_and_error_keep_debt)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdRelmapWalV1 wal;
		ClusterColdPlanV1 *plan;
		volatile bool caught = false;
		volatile int result = CLUSTER_COLD_REPLAY_OK;
		startup_relmap_replay_fixture();
		wal = startup_recovery_wal();
		startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_recovery_wal_v1(
			(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
		UT_ASSERT_EQ(startup_relmap_replay.typed->refusal, CLUSTER_COLD_OK);
		plan = startup_relmap_replay.typed->plan;
		startup_relmap_replay.fault = 2;
		test_cold_relmap_storage_fault(fault == 0 ? 2 : fault == 1 ? 1 : 0,
			fault == 2 ? startup_relmap_late_failure : NULL);
		cluster_cold_replay_window_enter_v1();
		PG_TRY(); { result = startup_relmap_run(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught || result != CLUSTER_COLD_REPLAY_OK);
		UT_ASSERT(plan->relmap_failed && !plan->relmap_inputs[0].applied);
		UT_ASSERT(plan->relmap_recovery->count == 1 && !plan->relmap_recovery->transferred);
		UT_ASSERT(plan->relmap_recovery->attempted[0] && !plan->relmap_recovery->staged[0]);
		{
			TestColdRelmapStorageStats before = test_cold_relmap_storage_stats();
			TestColdRelmapStorageStats after;
			UT_ASSERT(!cluster_relmap_authority_v2_write_recovery_pending(
				&plan->relmap_inputs[0].input.staged, plan->relmap_recovery));
			after = test_cold_relmap_storage_stats();
			UT_ASSERT_EQ(after.writes, before.writes);
			UT_ASSERT_EQ(after.reads, before.reads);
		}
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		cluster_cold_typed_destroy_v1(&startup_relmap_replay.typed);
		UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
		/* Rebuild the private carrier from the same retained WAL and actual
		 * files without pretending the one-owner Startup fixture supports
		 * two live coordinators. A torn/staged pair remains conservative. */
		{
			ClusterColdPlanV1 *rebuilt = startup_relmap_producer_plan();
			ClusterColdDetailV1 detail;
			detail = cluster_cold_plan_relmap_from_recovery_wal_v1(rebuilt,
				&startup_relmap_replay.input.source, 1, 1, &wal, 1);
			UT_ASSERT(fault == 0 ? detail != CLUSTER_COLD_OK : detail == CLUSTER_COLD_OK);
			UT_ASSERT(cluster_cold_plan_relmap_recovery_held_v1(rebuilt));
			UT_ASSERT_EQ(rebuilt->relmap_recovery->count, 1);
			rebuilt->relmap_recovery->transferred = true; /* simulated process exit only */
			cluster_cold_plan_destroy_v1(&rebuilt);
			UT_ASSERT_NULL(rebuilt);
		}
		startup_recovery_end();
	}
}

UT_TEST(test_recovery_carrier_refuses_generation_or_source_drift)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdRelmapWalV1 wal;
		startup_relmap_replay_fixture();
		wal = startup_recovery_wal();
		if (fault == 0) wal.generation++;
		if (fault == 1) wal.reader->seg.ws_tli++;
		if (fault == 2) {
			startup_relmap_producer_fault = 2;
			test_cold_relmap_storage_read_hook(startup_relmap_producer_after_read);
		}
		startup_relmap_replay.typed = cluster_cold_typed_prepare_relmap_recovery_wal_v1(
			(ClusterRecoveryFencePlan *)&fixture_context, 1, 0x800, &wal, 1);
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
		UT_ASSERT(startup_relmap_replay.typed->refusal != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, 0);
		if (startup_relmap_replay.typed->plan->relmap_recovery != NULL
			&& startup_relmap_replay.typed->plan->relmap_recovery->count != 0) {
			cluster_cold_typed_destroy_v1(&startup_relmap_replay.typed);
			UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
		}
		startup_recovery_end();
	}
}

UT_TEST(test_recovery_does_not_relax_unknown_or_building_ordinary_admission)
{
	ClusterColdRelmapWalV1 wal;
	ClusterRelmapRetentionDirectory saved;
	ClusterRelmapRetentionSnapshot snapshot;
	ClusterRelmapRetentionKey key = {0};
	ClusterRelmapRetentionTruncate truncation;
	startup_relmap_replay_fixture();
	wal = startup_relmap_wal_owner();
	key.identity = wal.identity;
	key.generation = wal.generation;
	key.origin = wal.owner.node;
	key.top_full_xid = wal.owner.top_full_xid;
	key.membership_epoch = wal.owner.membership_epoch;
	key.admitted_incarnation = wal.owner.admitted_incarnation;
	key.boot_incarnation = wal.owner.boot_incarnation;
	for (unsigned state = 0; state < 2; ++state) {
		wal.retention->state = state == 0 ? CLUSTER_RELMAP_RETENTION_UNKNOWN
			: CLUSTER_RELMAP_RETENTION_BUILDING;
		saved = *wal.retention;
		UT_ASSERT_EQ(cluster_relmap_retention_prepare_capture(wal.retention, &key, &snapshot),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(wal.retention, &key), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_stage_start(wal.retention, &key, wal.reader->EndRecPtr),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_stage_check(wal.retention, &key, wal.reader->EndRecPtr),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(wal.retention, key.top_full_xid + 1),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(wal.retention, key.top_full_xid, &truncation),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(wal.retention, &key),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&saved, wal.retention, sizeof(saved)), 0);
	}
	startup_relmap_real_side = false;
	test_cold_relmap_storage_end();
}

UT_TEST(test_recovery_transfer_preflights_all_targets_before_retiring_any_pin)
{
	ClusterRelmapRecoveryPins *pins = calloc(1, sizeof(*pins));
	ClusterRelmapRetentionDirectory dirs[2] = {0};
	ClusterRelmapRetentionDirectory *targets[2] = {&dirs[0], &dirs[1]};
	ClusterRelmapRetentionDirectory original[2];
	startup_relmap_replay_fixture();
	UT_ASSERT_NOT_NULL(pins);
	pins->state = CLUSTER_RELMAP_RETENTION_BUILDING;
	pins->expected = pins->count = 2;
	pins->restored = true;
	pins->images[0] = pins->images[1] = startup_relmap_replay.input.staged;
	pins->images[1].identity.shared = false;
	pins->images[1].identity.dbid = 42;
	for (unsigned i = 0; i < 2; ++i) {
		pins->attempted[i] = pins->staged[i] = true;
		UT_ASSERT_EQ(cluster_relmap_retention_build_start(&dirs[i],
			pins->images[i].identity.system_identifier, pins->images[i].identity.database_incarnation,
			pins->images[i].pending_owner.node, 9, pins->images[i].pending_owner.top_full_xid - 16,
			pins->images[i].pending_owner.top_full_xid + 16), CLUSTER_RELMAP_RETENTION_OK);
	}
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&dirs[0]), CLUSTER_RELMAP_RETENTION_OK);
	memcpy(original, dirs, sizeof(dirs));
	UT_ASSERT(!cluster_relmap_recovery_transfer(pins, targets, 2));
	UT_ASSERT_EQ(memcmp(original, dirs, sizeof(dirs)), 0);
	UT_ASSERT(!pins->transferred && pins->count == 2);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&dirs[1]), CLUSTER_RELMAP_RETENTION_OK);
	pins->staged[1] = false;
	UT_ASSERT(!cluster_relmap_recovery_transfer(pins, targets, 2));
	UT_ASSERT(!dirs[0].pins[0].occupied && !dirs[1].pins[0].occupied);
	pins->staged[1] = true;
	UT_ASSERT(cluster_relmap_recovery_transfer(pins, targets, 2));
	UT_ASSERT(pins->transferred && pins->count == 2);
	UT_ASSERT(dirs[0].pins[0].occupied && dirs[1].pins[0].occupied);
	free(pins);
	startup_relmap_real_side = false;
	test_cold_relmap_storage_end();
}
