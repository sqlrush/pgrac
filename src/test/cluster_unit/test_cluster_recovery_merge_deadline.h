/* Actual cold merge entry and commit; monotonic clock and existing metadata,
 * fence, pin and IR services are explicit test leaves. No CF retry or actual
 * IR/CONTROL retirement is simulated here.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned deadline_source_at, deadline_source_reads;

static void
deadline_fixture(int timeout_ms)
{
	fixture();
	cluster_recovery_merge_wait_timeout = timeout_ms;
	deadline_clock_ns = INT64CONST(1000000000);
	deadline_clock_step_ns = 0;
	deadline_clock_failed = false;
	deadline_monotonic_reads = 0;
	deadline_source_hook = NULL;
	deadline_source_at = deadline_source_reads = 0;
	deadline_serial_leaf = false;
	deadline_ir_budget = deadline_ir_release_budget = -1;
	memset(deadline_formation_budget, 0, sizeof(deadline_formation_budget));
	memset(deadline_admission_budget, 0, sizeof(deadline_admission_budget));
}

static void
deadline_expire_at_source(void)
{
	if (++deadline_source_reads == deadline_source_at) {
		deadline_clock_ns += INT64CONST(25000000);
		/* A reload cannot renew this original caller's entry snapshot. */
		cluster_recovery_merge_wait_timeout = 600000;
	}
}

UT_TEST(test_cold_deadline_never_resnapshots_after_preflight)
{
	for (unsigned change = 0; change < 2; change++) {
		ClusterRecoveryFencePlan *plan = NULL;

		deadline_fixture(25);
		expect_engaged(&plan);
		cluster_recovery_merge_wait_timeout = change ? 600000 : 0;
		deadline_clock_ns += INT64CONST(25000000);
		UT_ASSERT(!commit_as_held(plan));
		UT_ASSERT(!plan->committed);
		UT_ASSERT(strstr(cluster_recovery_merge_fence_plan_refusal_v1(plan), "merge_deadline")
				  != NULL);
		cluster_recovery_merge_fence_plan_destroy(&plan);
	}
}

UT_TEST(test_cold_deadline_zero_keeps_one_complete_attempt)
{
	ClusterRecoveryFencePlan *plan = NULL;

	deadline_fixture(0);
	make_open(2);
	expect_engaged(&plan);
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(cold_input_waits, 0);
	UT_ASSERT_EQ(deadline_formation_budget[2], cluster_external_fence_acquire_timeout_ms);
	UT_ASSERT_EQ(deadline_admission_budget[2], cluster_external_fence_acquire_timeout_ms);
	/* Zero still permits this same first complete attempt at a later stage. */
	cluster_recovery_merge_wait_timeout = 1;
	deadline_clock_ns += INT64CONST(900000000000);
	UT_ASSERT(commit_as_held(plan));
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(cold_input_waits, 0);
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

UT_TEST(test_cold_deadline_first_projection_runs_before_expiry_refusal)
{
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	deadline_fixture(25);
	deadline_source_at = 1;
	deadline_source_hook = deadline_expire_at_source;
	UT_ASSERT(!preflight(&plan, &engage));
	UT_ASSERT(plan == NULL);
	UT_ASSERT_EQ(last_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(strstr(last_detail, "merge_deadline") != NULL);
	UT_ASSERT(source_calls[2] > 0); /* The initial attempt actually ran. */
	UT_ASSERT_EQ(formations_built, 0);
	UT_ASSERT_EQ(open_calls[2] + tail_calls[2], 0);
	if (plan != NULL)
		cluster_recovery_merge_fence_plan_destroy(&plan);
	deadline_source_hook = NULL;
}

UT_TEST(test_cold_deadline_post_seal_projection_cannot_renew_budget)
{
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	deadline_fixture(25);
	make_open(2);
	/* Initial projection proves sources 3/4; the sealed projection reads 2. */
	deadline_source_at = 3;
	deadline_source_hook = deadline_expire_at_source;
	UT_ASSERT(!preflight(&plan, &engage));
	UT_ASSERT(plan == NULL);
	UT_ASSERT_EQ(last_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(strstr(last_detail, "merge_deadline") != NULL);
	UT_ASSERT_EQ(open_calls[2], 1);
	UT_ASSERT_EQ(tail_calls[2], 1);
	UT_ASSERT_EQ(formations_destroyed, formations_built);
	UT_ASSERT_EQ(cold_input_waits, 0);
	if (plan != NULL)
		cluster_recovery_merge_fence_plan_destroy(&plan);
	deadline_source_hook = NULL;
}

UT_TEST(test_cold_deadline_bounds_waitable_owner_budgets)
{
	ClusterRecoveryFencePlan *plan = NULL;

	deadline_fixture(7);
	expect_engaged(&plan);
	for (unsigned tid = 2; tid <= NODES; tid++) {
		UT_ASSERT_EQ(deadline_formation_budget[tid], 7);
		UT_ASSERT_EQ(deadline_admission_budget[tid], 7);
	}
	UT_ASSERT(deadline_monotonic_reads > 0);
	UT_ASSERT_EQ(cold_input_waits, 0);
	cluster_recovery_merge_fence_plan_destroy(&plan);
}

UT_TEST(test_cold_deadline_ir_acquire_keeps_independent_release_budget)
{
	ClusterRecoveryFencePlan *plan = NULL;

	deadline_fixture(10);
	expect_engaged(&plan);
	deadline_clock_ns += INT64CONST(7000000);
	deadline_serial_leaf = true;
	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(deadline_ir_budget, 3);
	UT_ASSERT_EQ(deadline_ir_release_budget, cluster_external_fence_acquire_timeout_ms);
	deadline_clock_ns += INT64CONST(900000000000);
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	deadline_serial_leaf = false;
}

UT_TEST(test_cold_deadline_commit_rechecks_after_projection)
{
	ClusterRecoveryFencePlan *plan = NULL;

	deadline_fixture(25);
	expect_engaged(&plan);
	deadline_source_at = 1;
	deadline_source_hook = deadline_expire_at_source;
	UT_ASSERT(!commit_as_held(plan));
	UT_ASSERT(!plan->committed);
	UT_ASSERT_EQ(deadline_source_reads, 3);
	UT_ASSERT(strstr(cluster_recovery_merge_fence_plan_refusal_v1(plan), "merge_deadline") != NULL);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	deadline_source_hook = NULL;
}

UT_TEST(test_cold_deadline_clock_failure_is_explicit_refusal)
{
	ClusterRecoveryFencePlan *plan = NULL;
	ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;

	deadline_fixture(25);
	deadline_clock_failed = true;
	UT_ASSERT(!preflight(&plan, &engage));
	UT_ASSERT(plan == NULL);
	UT_ASSERT_EQ(last_sqlstate, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT(strstr(last_detail, "merge_deadline") != NULL);
	UT_ASSERT_EQ(source_calls[2] + formations_built, 0);
	if (plan != NULL)
		cluster_recovery_merge_fence_plan_destroy(&plan);
	deadline_clock_failed = false;
}
