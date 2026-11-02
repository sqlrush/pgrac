/* Actual merge owner, with explicit IR/pin/metadata service leaves. */

static ClusterRecoveryFencePlan *
suspend_fixture(int timeout_ms)
{
	ClusterRecoveryFencePlan *plan = NULL;
	static PGPROC proc;
	static Latch latch;

	deadline_fixture(timeout_ms);
	suspended_ir_grant_hook = NULL;
	suspended_read_watch = NULL;
	suspended_cf_exclusive = false;
	suspended_locked_reads = 0;
	MyProc = &proc;
	MyLatch = &latch;
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	InRecovery = true;
	expect_engaged(&plan);
	deadline_serial_leaf = true;
	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT(cluster_recovery_merge_commit_plan_nowait(plan));
	suspended_read_watch = &plan->serial_guards;
	return plan;
}

UT_TEST(test_suspend_keeps_original_pin_and_proofs_until_resume)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	ClusterWalRetentionPin *pin = plan->retention_pin;
	ClusterRecoveryFenceOrigin origins[NODES - 1];
	uint64 episode = 0, deadline = plan->merge_deadline_ns;
	unsigned built = formations_built;

	memcpy(origins, plan->origins, sizeof(origins));
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	UT_ASSERT(episode != 0);
	UT_ASSERT(plan->retention_pin == pin);
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count == 0 && plan->committed);
	UT_ASSERT_EQ(memcmp(origins, plan->origins, sizeof(origins)), 0);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
	deadline_clock_ns += INT64CONST(7000000);
	cluster_recovery_merge_wait_timeout = 600000;
	UT_ASSERT(cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(plan->serial_held && plan->retention_pin == pin && plan->committed);
	UT_ASSERT_EQ(plan->merge_deadline_ns, deadline);
	UT_ASSERT_EQ(deadline_ir_budget, 18);
	UT_ASSERT_EQ(deadline_ir_release_budget, cluster_external_fence_acquire_timeout_ms);
	UT_ASSERT_EQ(formations_built, built);
	UT_ASSERT_EQ(memcmp(origins, plan->origins, sizeof(origins)), 0);
	UT_ASSERT(cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT(plan->retention_pin == NULL);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_suspend_source_drift_is_sticky_and_never_rebaselines)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		ClusterRecoveryFenceOrigin original = plan->origins[0];
		uint64 episode = 0;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		if (fault == 0) tokens[2].root_publish_seq++;
		if (fault == 1) roots[2].validated_tail_lsn_exclusive++;
		if (fault == 2) redo[2]++;
		if (fault == 3) fence_current[2] = false;
		if (fault == 4) cold_cohort.local_epoch++;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
		UT_ASSERT_EQ(memcmp(&original, &plan->origins[0], sizeof(original)), 0);
		/* Restoring an observed input cannot revive this episode. */
		tokens[2] = original.root_token;
		roots[2] = original.root_snapshot;
		redo[2] = plan->start_lsn[2];
		fence_current[2] = true;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}

UT_TEST(test_suspend_expiry_cannot_be_renewed_by_reload_or_resume)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0, deadline = plan->merge_deadline_ns;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	deadline_clock_ns += INT64CONST(25000000);
	cluster_recovery_merge_wait_timeout = 600000;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT_EQ(plan->merge_deadline_ns, deadline);
	/* Cleanup keeps its independent budget after the episode expires. */
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_suspend_requires_exact_ir_release_and_keeps_cleanup_debt)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	ClusterWalRetentionPin *pin = plan->retention_pin;
	uint64 episode = 0;

	ir_release_result = CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	UT_ASSERT_EQ(episode, 0);
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count != 0);
	UT_ASSERT(plan->retention_pin == pin);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	ir_release_result = CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED;
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_suspend_rejects_foreign_episode_and_preserves_old_release)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode + 1));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT(plan->retention_pin == NULL && plan->serial_guards.count == 0);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_suspend_current_inside_cf_barrier_has_no_ir_and_resume_requires_cf_release)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;
	unsigned reads = cold_cf_reads, releases = cold_cf_releases;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	suspended_cf_exclusive = true;
	/* Keep the actual canonical reader's held-CF refusal in this fixture.
	 * A nested canonical/source read must not masquerade as a locked read. */
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0)
		UT_ASSERT(cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
	else
		UT_ASSERT(false);
	fatal_armed = false;
	UT_ASSERT_EQ(suspended_locked_reads, plan->origin_count);
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count == 0);
	UT_ASSERT_EQ(cold_cf_reads, reads);
	UT_ASSERT_EQ(cold_cf_releases, releases);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(plan->serial_guards.count == 0 && plan->retention_pin != NULL);
	suspended_cf_exclusive = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

static void
suspended_late_ir_grant(void)
{
	deadline_clock_ns += INT64CONST(25000000);
}

UT_TEST(test_resume_late_grant_keeps_ir_debt_but_cannot_deliver_authority)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(25);
	uint64 episode = 0;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	suspended_ir_grant_hook = suspended_late_ir_grant;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	UT_ASSERT(!plan->serial_held && plan->serial_guards.count == plan->origin_count);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	UT_ASSERT(plan->retention_pin != NULL);
	suspended_ir_grant_hook = NULL;
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_suspend_zero_budget_is_same_episode_with_no_deadline_renewal)
{
	ClusterRecoveryFencePlan *plan = suspend_fixture(0);
	uint64 episode = 0, first;

	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	first = episode;
	deadline_clock_ns += INT64CONST(900000000000);
	cluster_recovery_merge_wait_timeout = 1;
	UT_ASSERT(cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	episode = 0;
	UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
	UT_ASSERT_EQ(episode, first);
	UT_ASSERT_EQ(plan->merge_deadline_ns, 0);
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
	cluster_recovery_merge_fence_plan_destroy(&plan);
	suspended_read_watch = NULL;
}

UT_TEST(test_suspend_actor_loss_or_late_metadata_read_revokes_original_episode)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		uint64 episode = 0;
		ResourceOwner owner = CurrentResourceOwner;
		PGPROC *proc = MyProc;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		if (fault == 0) fence_owner_boot++;
		if (fault == 1) CurrentResourceOwner = NULL;
		if (fault == 2) MyProc = NULL;
		if (fault == 3) ShutdownRequestPending = true;
		if (fault == 4) {
			deadline_source_at = 1;
			deadline_source_hook = deadline_expire_at_source;
		}
		UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
		CurrentResourceOwner = owner;
		MyProc = proc;
		fence_owner_boot = plan->owner_boot;
		ShutdownRequestPending = false;
		deadline_source_hook = NULL;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}

UT_TEST(test_suspend_barrier_reads_original_root_token_native_cut_and_deadline)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryFencePlan *plan = suspend_fixture(25);
		uint64 episode = 0;

		UT_ASSERT(cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, &episode));
		suspended_cf_exclusive = true;
		if (fault == 0) roots[2].root_publish_seq++;
		if (fault == 1) tokens[2].file_txn_seq++;
		if (fault == 2) tokens[2].record_crc32c++;
		if (fault == 3) redo[2]++;
		if (fault == 4) {
			deadline_source_at = 1;
			deadline_source_hook = deadline_expire_at_source;
		}
		UT_ASSERT(!cluster_recovery_merge_fence_plan_suspended_current_v1(plan, episode));
		suspended_cf_exclusive = false;
		deadline_source_hook = NULL;
		UT_ASSERT(!cluster_recovery_merge_fence_plan_resume_serial_v1(plan, episode));
		UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
		cluster_recovery_merge_fence_plan_destroy(&plan);
		suspended_read_watch = NULL;
	}
}
