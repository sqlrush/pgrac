/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_recheck.inc
 *     Recheck replayed files and original pins across the Start boundary.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_recheck.inc
 * NOTES
 *     Start identity/ownership remains a controlled boundary in this unit.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
struct ClusterRecoveryStartV1;
extern bool cluster_cold_relmap_replay_recheck_v1(const struct ClusterRecoveryStartV1 *start);
extern bool test_cold_relmap_start_current(const struct ClusterRecoveryStartV1 *start);
extern const ClusterColdTypedV1 *test_cold_relmap_start_inputs(const struct ClusterRecoveryStartV1 *start);
extern void test_cold_relmap_start_fail(const struct ClusterRecoveryStartV1 *start);

static int recheck_start_marker;
static const struct ClusterRecoveryStartV1 *recheck_start;
static const ClusterColdTypedV1 *recheck_inputs;
static bool recheck_revoked;
static unsigned recheck_current_calls, recheck_error_at, recheck_read_at, recheck_fault;

bool
test_cold_relmap_start_current(const struct ClusterRecoveryStartV1 *start)
{
	if (++recheck_current_calls == recheck_error_at) siglongjmp(*PG_exception_stack, 1);
	return start != NULL && start == recheck_start && !recheck_revoked;
}

const ClusterColdTypedV1 *
test_cold_relmap_start_inputs(const struct ClusterRecoveryStartV1 *start)
{
	return start != NULL && start == recheck_start && !recheck_revoked ? recheck_inputs : NULL;
}

void
test_cold_relmap_start_fail(const struct ClusterRecoveryStartV1 *start)
{
	if (start != NULL && start == recheck_start) recheck_revoked = true;
}

static void
recheck_begin(bool bound)
{
	startup_relmap_pin_prepare(bound);
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	cluster_cold_replay_window_leave_v1();
	recheck_start = (const struct ClusterRecoveryStartV1 *)&recheck_start_marker;
	recheck_inputs = startup_relmap_replay.typed;
	recheck_revoked = false;
	recheck_current_calls = recheck_error_at = recheck_fault = 0;
	recheck_read_at = test_cold_relmap_storage_stats().reads + 1;
	MyBackendType = B_STARTUP;
}

static void
recheck_end(void)
{
	test_cold_relmap_storage_read_hook(NULL);
	recheck_start = NULL;
	recheck_inputs = NULL;
	MyBackendType = B_INVALID;
	CritSectionCount = 0;
	cluster_cold_replay_window_enter_v1();
	startup_relmap_replay_end();
}

static void
recheck_debt_and_no_retry(TestColdRelmapStorageStats before, Size used)
{
	TestColdRelmapStorageStats after = test_cold_relmap_storage_stats();
	UT_ASSERT(recheck_revoked);
	UT_ASSERT(after.pin_retained && after.may_be_staged);
	UT_ASSERT_EQ(after.writes, before.writes);
	UT_ASSERT_EQ(after.io_under_retention_lock, 0);
	UT_ASSERT_EQ(startup_relmap_replay.typed->plan->memory_used, used);
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, after.reads);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, after.writes);
}

UT_TEST(test_relmap_start_recheck_reads_staged_pair_after_replay_window)
{
	ClusterColdRelmapReplayReceiptV1 saved, final;
	TestColdRelmapStorageStats before, after;
	Size used;
	recheck_begin(true);
	used = startup_relmap_replay.typed->plan->memory_used;
	saved = cluster_cold_typed_relmap_replay_input_v1(recheck_inputs, 0);
	before = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(saved.count, 1);
	UT_ASSERT(cluster_cold_relmap_replay_recheck_v1(recheck_start));
	after = test_cold_relmap_storage_stats();
	UT_ASSERT(after.reads > before.reads && after.writes == before.writes);
	UT_ASSERT(after.pin_retained && after.may_be_staged && !recheck_revoked);
	UT_ASSERT_EQ(after.io_under_retention_lock, 0);
	UT_ASSERT_EQ(startup_relmap_replay.typed->plan->memory_used, used);
	final = cluster_cold_typed_relmap_replay_input_v1(recheck_inputs, 0);
	UT_ASSERT_EQ(memcmp(&saved, &final, sizeof(saved)), 0);
	UT_ASSERT(cluster_cold_relmap_replay_recheck_v1(recheck_start));
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, before.writes);
	recheck_end();
}

UT_TEST(test_relmap_start_recheck_preflights_original_receipt_and_side_before_io)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		RfSideOnlinePlanV1 *side;
		TestColdRelmapStorageStats before;
		ClusterColdPlanV1 *plan;
		Size used;
		recheck_begin(true);
		plan = startup_relmap_replay.typed->plan;
		used = plan->memory_used;
		side = startup_relmap_replay.typed->side_plans[0];
		before = test_cold_relmap_storage_stats();
		if (fault == 0) plan->relmap_inputs[0].applied = false;
		if (fault == 1) startup_relmap_replay.typed->sources[0].timeline++;
		if (fault == 2) startup_relmap_replay.typed->participants[0].tail_end++;
		if (fault == 3) startup_relmap_replay.typed->side_plans[0] = NULL;
		if (fault == 4) plan->relmap_count = CLUSTER_COLD_RELMAP_MAX_INPUTS + 1;
		if (fault == 5) plan->relmap_inputs[0].input.retention = NULL;
		UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
		recheck_debt_and_no_retry(before, used);
		startup_relmap_replay.typed->side_plans[0] = side;
		plan->relmap_count = 1;
		recheck_end();
	}
}

UT_TEST(test_relmap_start_recheck_original_pin_drift_refuses_before_io)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		TestColdRelmapStorageStats before;
		Size used;
		recheck_begin(fault != 5);
		used = startup_relmap_replay.typed->plan->memory_used;
		before = test_cold_relmap_storage_stats();
		test_cold_relmap_storage_pin_drift(fault);
		UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
		recheck_debt_and_no_retry(before, used);
		recheck_end();
	}
}

UT_TEST(test_relmap_start_recheck_requires_both_complete_physical_copies)
{
	for (unsigned copy = 0; copy < 2; ++copy) {
		TestColdRelmapStorageStats before;
		Size used;
		recheck_begin(true);
		used = startup_relmap_replay.typed->plan->memory_used;
		test_cold_relmap_storage_damage_copy(copy);
		before = test_cold_relmap_storage_stats();
		UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
		recheck_debt_and_no_retry(before, used);
		recheck_end();
	}
}

static void
recheck_change_during_read(unsigned reads)
{
	ClusterColdTypedV1 *typed = startup_relmap_replay.typed;
	if (reads != recheck_read_at) return;
	switch (recheck_fault) {
	case 0: typed->plan->relmap_inputs[0].input.staged.pending.mappings[0].mapoid++; break;
	case 1: typed->sources[0].timeline++; break;
	case 2: typed->plan->relmap_inputs[0].retention_snapshot.generation++; break;
	case 3: recheck_revoked = true; break;
	case 4: recheck_inputs = NULL; break;
	case 5: test_cold_relmap_storage_pin_drift(0); break;
	case 6: siglongjmp(*PG_exception_stack, 1); break;
	default: abort();
	}
}

UT_TEST(test_relmap_start_recheck_late_input_owner_or_pin_change_stays_failed)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		TestColdRelmapStorageStats before;
		Size used;
		recheck_begin(true);
		used = startup_relmap_replay.typed->plan->memory_used;
		before = test_cold_relmap_storage_stats();
		recheck_fault = fault;
		test_cold_relmap_storage_read_hook(recheck_change_during_read);
		UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(test_cold_relmap_storage_stats().reads > before.reads);
		UT_ASSERT(startup_relmap_replay.typed->plan->relmap_failed);
		recheck_debt_and_no_retry(before, used);
		recheck_end();
	}
}

UT_TEST(test_relmap_start_recheck_first_owner_pin_and_read_error_revoke)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		TestColdRelmapStorageStats before;
		volatile bool caught = false;
		Size used;
		recheck_begin(true);
		used = startup_relmap_replay.typed->plan->memory_used;
		before = test_cold_relmap_storage_stats();
		if (fault == 0) recheck_error_at = 1;
		if (fault == 1) test_cold_relmap_storage_pin_error();
		if (fault == 2) {
			recheck_fault = 6;
			test_cold_relmap_storage_read_hook(recheck_change_during_read);
		}
		PG_TRY(); { (void)cluster_cold_relmap_replay_recheck_v1(recheck_start); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(caught);
		recheck_debt_and_no_retry(before, used);
		recheck_end();
	}
}

UT_TEST(test_relmap_start_recheck_caller_preconditions_do_not_consume_receipts)
{
	TestColdRelmapStorageStats before;
	LWLock *lock;
	recheck_begin(true);
	lock = startup_relmap_replay.input.retention_lock;
	before = test_cold_relmap_storage_stats();
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(NULL));
	MyBackendType = B_BACKEND;
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
	MyBackendType = B_STARTUP;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
	CritSectionCount = 0;
	UT_ASSERT(test_cold_relmap_storage_acquire(lock, LW_EXCLUSIVE));
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
	test_cold_relmap_storage_unlock(lock);
	UT_ASSERT_EQ(recheck_current_calls, 0);
	UT_ASSERT(!recheck_revoked && !startup_relmap_replay.typed->plan->relmap_failed);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
	UT_ASSERT(cluster_cold_relmap_replay_recheck_v1(recheck_start));
	recheck_end();
}

UT_TEST(test_relmap_start_recheck_whole_scratch_budget_refuses_before_io)
{
	ClusterColdPlanV1 *plan;
	TestColdRelmapStorageStats before;
	Size budget, used;
	recheck_begin(true);
	plan = startup_relmap_replay.typed->plan;
	used = plan->memory_used;
	budget = plan->memory_budget;
	plan->memory_budget = used;
	before = test_cold_relmap_storage_stats();
	UT_ASSERT(!cluster_cold_relmap_replay_recheck_v1(recheck_start));
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
	UT_ASSERT(plan->relmap_failed);
	plan->memory_budget = budget;
	recheck_debt_and_no_retry(before, used);
	recheck_end();
}

/* The creation leaf is deliberately unconsumed; only pending file/FD paths
 * are exercised here. Keep its count and values intact through every gate. */
static void
pending_recheck_begin(void)
{
	ClusterColdPlanV1 *plan;
	ColdRelmapCreateChunk *chunk;
	recheck_begin(true); plan = startup_relmap_replay.typed->plan;
	chunk = cold_plan_scratch(plan, sizeof(*chunk), NULL);
	UT_ASSERT_NOT_NULL(chunk);
	chunk->entries[0].captured = true;
	chunk->entries[0].input.creation.identity.database_oid = 2000;
	chunk->entries[0].input.creation.identity.tablespace_oid = DEFAULTTABLESPACE_OID;
	plan->relmap_creates = chunk; plan->relmap_create_count = 1;
}

UT_TEST(test_pending_staging_mixed_reads_both_copies_and_preserves_creation)
{
	TestColdRelmapStorageStats before;
	ClusterColdRelmapReplayReceiptV1 saved;
	Size used;
	pending_recheck_begin();
	used = startup_relmap_replay.typed->plan->memory_used;
	saved = cluster_cold_typed_relmap_pending_input_v1(recheck_inputs, 0);
	UT_ASSERT_EQ(saved.count, 1);
	before = test_cold_relmap_storage_stats();
	UT_ASSERT(cluster_cold_relmap_pending_replay_recheck_v1(recheck_start));
	UT_ASSERT(test_cold_relmap_storage_stats().reads > before.reads);
	UT_ASSERT_EQ(test_cold_relmap_storage_stats().writes, before.writes);
	UT_ASSERT_EQ(startup_relmap_replay.typed->plan->memory_used, used);
	UT_ASSERT_EQ(startup_relmap_replay.typed->plan->relmap_create_count, 1);
	UT_ASSERT(!cold_relmap_create_at(startup_relmap_replay.typed->plan, 0)->attempted);
	UT_ASSERT(!recheck_revoked && test_cold_relmap_storage_stats().pin_retained);
	recheck_end();
}

UT_TEST(test_pending_staging_legacy_mixed_and_empty_still_refuse)
{
	for (unsigned empty = 0; empty < 2; ++empty) {
		TestColdRelmapStorageStats before;
		ColdRelmapInput *saved;
		ClusterColdPlanV1 *plan;
		pending_recheck_begin(); plan = startup_relmap_replay.typed->plan;
		saved = plan->relmap_inputs;
		before = test_cold_relmap_storage_stats();
		if (empty) { plan->relmap_count = 0; plan->relmap_inputs = NULL; }
		UT_ASSERT(!(empty ? cluster_cold_relmap_pending_replay_recheck_v1(recheck_start)
			: cluster_cold_relmap_replay_recheck_v1(recheck_start)));
		UT_ASSERT(recheck_revoked && plan->relmap_failed && plan->relmap_create_count == 1);
		UT_ASSERT_EQ(test_cold_relmap_storage_stats().reads, before.reads);
		plan->relmap_count = 1; plan->relmap_inputs = saved;
		recheck_end();
	}
}

UT_TEST(test_pending_staging_late_source_pin_and_pair_changes_fail_closed)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		TestColdRelmapStorageStats before;
		Size used;
		pending_recheck_begin(); used = startup_relmap_replay.typed->plan->memory_used;
		before = test_cold_relmap_storage_stats();
		if (fault == 3) test_cold_relmap_storage_damage_copy(1);
		else { recheck_fault = fault; test_cold_relmap_storage_read_hook(recheck_change_during_read); }
		UT_ASSERT(!cluster_cold_relmap_pending_replay_recheck_v1(recheck_start));
		test_cold_relmap_storage_read_hook(NULL);
		recheck_debt_and_no_retry(before, used);
		UT_ASSERT(!cluster_cold_relmap_pending_replay_recheck_v1(recheck_start));
		UT_ASSERT_EQ(startup_relmap_replay.typed->plan->relmap_create_count, 1);
		recheck_end();
	}
}

UT_TEST(test_pending_staging_first_owner_and_native_errors_keep_debt)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		volatile bool caught = false;
		TestColdRelmapStorageStats before;
		Size used;
		pending_recheck_begin(); used = startup_relmap_replay.typed->plan->memory_used;
		before = test_cold_relmap_storage_stats();
		if (fault == 0) recheck_error_at = 1;
		else { recheck_fault = 6; test_cold_relmap_storage_read_hook(recheck_change_during_read); }
		PG_TRY(); { (void)cluster_cold_relmap_pending_replay_recheck_v1(recheck_start); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		recheck_error_at = 0; test_cold_relmap_storage_read_hook(NULL);
		UT_ASSERT(caught);
		recheck_debt_and_no_retry(before, used);
		UT_ASSERT(!cluster_cold_relmap_pending_replay_recheck_v1(recheck_start));
		recheck_end();
	}
}
