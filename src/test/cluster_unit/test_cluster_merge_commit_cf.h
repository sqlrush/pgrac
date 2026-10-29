/* Actual commit attempt, positive projection and held two-member IR set.
 * Physical ROOT/WAL, pin, fence, clocks and ACK delivery remain leaves.
 * This single attempt never waits, retires or rebinds those owners.
 * Author: SqlRush <sqlrush@gmail.com> */
static char commit_cf_pin;
static unsigned commit_cf_reads, commit_cf_failure_at, commit_cf_pin_checks;
static unsigned commit_cf_hard;
static ClusterWalPinResult commit_cf_pin_result;

static ClusterWalPinResult
commit_cf_pin_current(ClusterWalRetentionPin *pin)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)&commit_cf_pin);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	++commit_cf_pin_checks;
	return commit_cf_pin_result;
}

static ClusterControlRootResult
commit_cf_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
					ClusterControlRootReadMode policy, ClusterControlRootSnapshot *snapshot,
					ClusterControlRootReadToken *token)
{
	ClusterControlRootResult result;

	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	++commit_cf_reads;
	if (commit_cf_hard == 1)
		retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (commit_cf_hard == 2)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE; /* Explicit untyped leaf. */
	g_seven_result = commit_cf_reads == commit_cf_failure_at ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT
															 : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	result = cluster_control_root_read_canonical(thread, expected, policy, snapshot, token);
	if (commit_cf_hard == 3)
		++token->root_publish_seq;
	return result;
}

#define cluster_control_root_read_canonical commit_cf_canonical
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define cluster_wal_retention_pin_revalidate commit_cf_pin_current
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#include "test_cluster_merge_commit_cf.inc"
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_wal_retention_pin_revalidate
#undef cluster_recovery_duty_key_compare_for_claim
#undef cluster_control_root_read_canonical

static void
commit_cf_fixture(void)
{
	ClusterRecoverySerialRequest requests[2];
	uint16 failed = UINT16_MAX;

	pre_ir_fixture(true, false);
	retention_phase = 0;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	retention_image.record_crc32c[2] = 17;
	for (unsigned i = 0; i < 2; i++) {
		ClusterRecoveryFenceOrigin *origin = &cold_deadline_plan.origins[i];

		make_read_token(&retention_image, origin->origin_thread, CONTROL_ROOT_SOURCE_PRIMARY,
						&origin->root_token);
		requests[i] = ir_cf_requests[i];
		requests[i].duty = origin->duty;
		requests[i].expected_root_token = origin->root_token;
	}
	UT_ASSERT_EQ(cluster_recovery_serial_acquire_set(requests, 2, 1000,
													 &cold_deadline_plan.serial_guards, &failed),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	cold_deadline_plan.sealed = cold_deadline_plan.serial_held = true;
	cold_deadline_plan.acquire_timeout_ms_snapshot = 1000;
	cold_deadline_plan.retention_pin = (ClusterWalRetentionPin *)&commit_cf_pin;
	cold_deadline_plan.replay_thread_bitmap[0] = 7;
	cold_deadline_plan.foreign_origin_bitmap[0] = 6;
	cold_deadline_plan.start_lsn[1] = 90;
	cold_deadline_plan.start_lsn[2] = cold_deadline_plan.start_lsn[3] = 150;
	commit_cf_reads = commit_cf_failure_at = commit_cf_pin_checks = 0;
	commit_cf_hard = 0;
	commit_cf_pin_result = CLUSTER_WAL_PIN_OK;
	retention_attempts = retention_threads = retention_controls = 0;
	entry_on_wait = NULL;
}

static bool
commit_cf_call(ClusterCfAttemptObservation *retry, bool *committed)
{
	volatile bool completed = false;

	PG_TRY();
	{
		*committed = recovery_fence_commit_attempt(&cold_deadline_plan, retry);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

static void
commit_cf_cleanup(void)
{
	MyBackendType = B_STARTUP;
	ir_cf_confirm_release = true;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	retention_phase = 0;
	if (cold_deadline_plan.serial_guards.count != 0)
		UT_ASSERT_EQ(cluster_recovery_serial_release_set(&cold_deadline_plan.serial_guards),
					 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	cold_deadline_plan.serial_held = false;
	cold_deadline_plan.retention_pin = NULL; /* The leaf owns no real WALR. */
	ir_cf_cleanup();
}

static void
commit_cf_retry_owns_only_original_debt(const ClusterCfAttemptObservation *retry,
										const ClusterRecoverySerialGuardSet *held)
{
	ClusterCfRetirementSnapshot snapshot;

	UT_ASSERT(cluster_cf_attempt_wait_slice(retry));
	UT_ASSERT_EQ(retry->mode, ShareLock);
	UT_ASSERT_EQ(retry->control_owner_id, cluster_cf_owner_cookie(ShareLock));
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(memcmp(held, &cold_deadline_plan.serial_guards, sizeof(*held)), 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	UT_ASSERT(cold_deadline_plan.serial_held && !cold_deadline_plan.committed);
	UT_ASSERT(cold_deadline_plan.retention_pin == (ClusterWalRetentionPin *)&commit_cf_pin);
	UT_ASSERT_EQ(entry_waits | ir_cf_releases | commit_cf_pin_checks, 0);
	UT_ASSERT_EQ(cold_deadline_plan.refusal_detail[0], '\0');
	UT_ASSERT(cluster_cf_retirement_snapshot(ShareLock, NULL, &snapshot));
	UT_ASSERT_EQ(snapshot.cookie, retry->control_owner_id);
	UT_ASSERT_EQ(snapshot.registry.message.key.holder.request_id, retry->request_id);
	UT_ASSERT_EQ(snapshot.registry.message.key.holder.cluster_epoch, retry->request_epoch);
	ir_cf_trace("commit-single-attempt", snapshot.cookie, &snapshot.registry);
}

UT_TEST(startup_commit_project_cf_slice_returns_typed_without_waiting_under_ir)
{
	for (unsigned middle = 0; middle < 2; middle++) {
		ClusterCfAttemptObservation retry = { 0 };
		ClusterRecoverySerialGuardSet held;
		bool committed = false;

		commit_cf_fixture();
		held = cold_deadline_plan.serial_guards;
		retention_phase = middle ? 2 : 0;
		g_seven_result
			= middle ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed);
		commit_cf_retry_owns_only_original_debt(&retry, &held);
		UT_ASSERT_EQ(commit_cf_reads, 0);
		UT_ASSERT_EQ(project_sources, middle);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_canonical_cf_slice_returns_typed_without_waiting_under_ir)
{
	for (unsigned member = 1; member <= 2; member++) {
		ClusterCfAttemptObservation retry = { 0 };
		ClusterRecoverySerialGuardSet held;
		bool committed = false;

		commit_cf_fixture();
		held = cold_deadline_plan.serial_guards;
		commit_cf_failure_at = member;
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed);
		commit_cf_retry_owns_only_original_debt(&retry, &held);
		UT_ASSERT_EQ(commit_cf_reads, member);
		UT_ASSERT_EQ(project_sources, 2);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_native_source_cf_slice_retains_original_ir_and_pin)
{
	ClusterCfAttemptObservation retry = { 0 };
	ClusterRecoverySerialGuardSet held;
	bool committed = false;

	commit_cf_fixture();
	held = cold_deadline_plan.serial_guards;
	project_source_timeout = true;
	UT_ASSERT(commit_cf_call(&retry, &committed));
	UT_ASSERT(!committed);
	commit_cf_retry_owns_only_original_debt(&retry, &held);
	UT_ASSERT_EQ(commit_cf_reads, 0);
	UT_ASSERT_EQ(project_sources, 1);
	commit_cf_cleanup();
}

UT_TEST(startup_commit_ready_preserves_original_commit_and_clears_retry_output)
{
	ClusterCfAttemptObservation retry;
	ClusterCfAttemptObservation clear = { 0 };
	ClusterRecoverySerialGuardSet held;
	bool committed = false;

	commit_cf_fixture();
	held = cold_deadline_plan.serial_guards;
	memset(&retry, 0xa5, sizeof(retry));
	UT_ASSERT(commit_cf_call(&retry, &committed));
	UT_ASSERT(committed && !cold_deadline_plan.committed); /* Caller publishes after reproof. */
	UT_ASSERT_EQ(memcmp(&retry, &clear, sizeof(retry)), 0);
	UT_ASSERT_EQ(memcmp(&held, &cold_deadline_plan.serial_guards, sizeof(held)), 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	UT_ASSERT_EQ(entry_waits | ir_cf_releases, 0);
	UT_ASSERT_EQ(commit_cf_reads, 2);
	UT_ASSERT_EQ(commit_cf_pin_checks, 1);
	commit_cf_cleanup();
}

UT_TEST(startup_commit_hard_root_token_and_untyped_results_never_return_cf_retry)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterCfAttemptObservation retry;
		ClusterCfAttemptObservation clear = { 0 };
		bool committed = false;

		commit_cf_fixture();
		commit_cf_hard = fault;
		memset(&retry, 0xa5, sizeof(retry));
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed && !cold_deadline_plan.committed);
		UT_ASSERT_EQ(memcmp(&retry, &clear, sizeof(retry)), 0);
		UT_ASSERT(strstr(cold_deadline_plan.refusal_detail,
						 fault == 3 ? "commit_root_token" : "commit_root_read")
				  != NULL);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases | commit_cf_pin_checks, 0);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_old_slots_ordinary_roles_and_untyped_timeouts_keep_original_refusal)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterCfAttemptObservation retry;
		ClusterCfAttemptObservation clear = { 0 };
		bool committed = false;

		commit_cf_fixture();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		if (fault == 0)
			(void)entry_timeout(ShareLock);
		else if (fault == 1)
			MyBackendType = B_BACKEND;
		else
			g_timeout_detail.source = CLUSTER_GES_TSRC_NONE;
		memset(&retry, 0xa5, sizeof(retry));
		UT_ASSERT(!commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed && !cold_deadline_plan.committed);
		UT_ASSERT_EQ(memcmp(&retry, &clear, sizeof(retry)), 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases | commit_cf_pin_checks, 0);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_invalid_state_or_original_expiry_clears_retry_before_reads)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterCfAttemptObservation retry;
		ClusterCfAttemptObservation clear = { 0 };
		bool committed = false;

		commit_cf_fixture();
		if (fault == 0)
			cold_deadline_plan.magic = 0;
		if (fault == 1)
			++cold_deadline_plan.owner_pid;
		if (fault == 2)
			cold_deadline_plan.sealed = false;
		if (fault == 3)
			cold_deadline_plan.serial_held = false;
		if (fault == 4)
			cold_deadline_plan.committed = true;
		if (fault == 5)
			cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns;
		memset(&retry, 0xa5, sizeof(retry));
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed);
		UT_ASSERT_EQ(memcmp(&retry, &clear, sizeof(retry)), 0);
		UT_ASSERT_EQ(project_sources | commit_cf_reads | retention_attempts, 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases | commit_cf_pin_checks, 0);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_zero_budget_keeps_one_attempt_and_never_waits_with_ir)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		ClusterCfAttemptObservation retry = { 0 };
		bool committed = false;

		commit_cf_fixture();
		cold_deadline_plan.merge_wait_timeout_ms_snapshot = 0;
		cold_deadline_plan.merge_deadline_ns = 0;
		if (pending)
			g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT_EQ(committed, pending == 0);
		UT_ASSERT_EQ(cluster_cf_attempt_wait_slice(&retry), pending != 0);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases, 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_final_pin_or_fence_refusal_does_not_manufacture_cf_retry)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterCfAttemptObservation retry = { 0 };
		ClusterCfAttemptObservation clear = { 0 };
		bool committed = false;

		commit_cf_fixture();
		if (fault == 0)
			commit_cf_pin_result = CLUSTER_WAL_PIN_STALE;
		else
			failure_admissions = false;
		UT_ASSERT(commit_cf_call(&retry, &committed));
		UT_ASSERT(!committed && !cold_deadline_plan.committed);
		UT_ASSERT_EQ(memcmp(&retry, &clear, sizeof(retry)), 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases, 0);
		failure_admissions = true;
		commit_cf_cleanup();
	}
}
