/* Original cold-input consumer, actual CF/owner/CONTROL and original plan
 * deadline helpers. Metadata readiness, clocks and ACK delivery are leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
cold_deadline_ready(void)
{
	/* A reload must not renew the original entry snapshot. */
	if (entry_waits == 1)
		cluster_recovery_merge_wait_timeout = 600000;
	if (cluster_cf_held(ShareLock)) {
		if (claim_retiring_cookie == 0)
			claim_retiring_cookie = cluster_cf_owner_cookie(ShareLock);
	}
	if (entry_waits == 3) {
		if (!cluster_cf_held(ShareLock))
			entry_ack_at = 0;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
		cold_observation_pending = false;
	}
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
}

static void
cold_deadline_fixture(int timeout, bool cf_pending)
{
	cold_fixture();
	cold_deadline_active = true;
	cold_deadline_plan.merge_wait_timeout_ms_snapshot = timeout;
	cold_deadline_plan.merge_deadline_ns
		= timeout == 0 ? 0 : set_cf_clock_ns + (uint64)timeout * NS_PER_MS;
	g_seven_result
		= cf_pending ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	cold_observation_pending = !cf_pending;
	entry_on_wait = cold_deadline_ready;
}

UT_TEST(startup_cold_zero_attempt_never_sleeps_for_pending_metadata)
{
	bool ready = false;

	cold_deadline_fixture(0, false);
	UT_ASSERT(cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(cold_reads, 1);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(cold_refresh_calls, 1);
	UT_ASSERT(cluster_control_request_empty());
	cold_cleanup();
}

UT_TEST(startup_cold_zero_cf_slice_retains_exact_cleanup_without_sleep)
{
	bool ready = false;

	cold_deadline_fixture(0, true);
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits | cold_reads, 0);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	cold_cleanup();
}

UT_TEST(startup_cold_cf_retirement_wait_uses_the_original_remaining_deadline)
{
	bool ready = false;

	cold_deadline_fixture(150, true);
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(cold_deadline_waits[1], 50);
	UT_ASSERT_EQ(cold_reads, 0);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT(cluster_cf_held(ShareLock));
	cold_cleanup(); /* Cleanup is permitted after the original outer deadline. */
}

UT_TEST(startup_cold_metadata_wait_uses_the_same_remaining_deadline)
{
	bool ready = false;

	cold_deadline_fixture(150, false);
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(cold_deadline_waits[1], 50);
	UT_ASSERT_EQ(cold_refresh_calls, 2);
	UT_ASSERT_EQ(cold_refresh_budgets[0], 100);
	UT_ASSERT_EQ(cold_refresh_budgets[1], 50);
	UT_ASSERT_EQ(cold_reads, 2);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	cold_cleanup();
}

UT_TEST(startup_cold_refresh_cannot_start_cf_after_original_expiry)
{
	bool ready = false;

	cold_deadline_fixture(7, false);
	cold_expire_at_refresh = true;
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cold_refresh_calls, 1);
	UT_ASSERT_EQ(cold_refresh_budgets[0], 7);
	UT_ASSERT_EQ(g_seven_count, claim_acquires);
	UT_ASSERT_EQ(entry_waits | cold_reads, 0);
	cold_cleanup();
}

UT_TEST(startup_cold_ready_observation_still_refuses_after_original_expiry)
{
	bool ready = false;

	cold_deadline_fixture(7, false);
	cold_observation_pending = false;
	cold_expire_at_observation = true;
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cold_reads, 1);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	cold_cleanup();
}

UT_TEST(startup_cold_zero_ready_keeps_its_complete_first_attempt)
{
	bool ready = false;

	cold_deadline_fixture(0, false);
	cold_observation_pending = false;
	set_cf_clock_ns += UINT64_C(900000000000);
	UT_ASSERT(cold_call(&ready));
	UT_ASSERT(ready);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(cold_reads | cold_refresh_calls, 1);
	UT_ASSERT(cluster_control_request_empty());
	cold_cleanup();
}

UT_TEST(startup_cold_exact_cf_ack_before_deadline_can_still_reach_ready)
{
	bool ready = false;

	cold_deadline_fixture(350, true);
	UT_ASSERT(cold_call(&ready));
	UT_ASSERT(ready);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(cold_deadline_waits[1], 100);
	UT_ASSERT_EQ(cold_deadline_waits[2], 100);
	UT_ASSERT_EQ(cold_refresh_calls, 2);
	UT_ASSERT_EQ(cold_refresh_budgets[0], 100);
	UT_ASSERT_EQ(cold_refresh_budgets[1], 50);
	UT_ASSERT_EQ(cold_reads, 1);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 2);
	UT_ASSERT(cluster_control_request_empty());
	cold_cleanup();
}

static void
cold_deadline_expire_on_grant(void)
{
	set_cf_clock_ns = cold_deadline_plan.merge_deadline_ns;
}

UT_TEST(startup_cold_cf_grant_after_deadline_runs_only_original_cleanup)
{
	bool ready = false;

	cold_deadline_fixture(7, false);
	claim_after_acquire = cold_deadline_expire_on_grant;
	UT_ASSERT(!cold_call(&ready));
	UT_ASSERT(!ready);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(g_seven_count, claim_acquires + 1);
	UT_ASSERT_EQ(entry_waits | cold_reads, 0);
	UT_ASSERT(cold_release_waits > 0);
	UT_ASSERT(cluster_control_request_empty());
	cold_cleanup();
}

UT_TEST(startup_cold_original_plan_identity_is_required_before_refresh)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		bool ready = false;

		cold_deadline_fixture(100, false);
		switch (fault) {
		case 0:
			cold_deadline_plan.owner_pid++;
			break;
		case 1:
			cold_deadline_plan.owner = (ResourceOwner)2;
			break;
		case 2:
			cold_deadline_plan.owner_boot++;
			break;
		case 3:
			cold_deadline_plan.own_thread++;
			break;
		}
		UT_ASSERT(!cold_call(&ready));
		UT_ASSERT(!ready);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(cold_refresh_calls | cold_reads | entry_waits, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires);
		cold_cleanup();
	}
}
