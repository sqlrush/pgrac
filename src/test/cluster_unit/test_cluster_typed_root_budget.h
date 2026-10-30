/* Actual typed root collection, ROOT lookup and CF/owner/CONTROL. Clock,
 * physical I/O and ACK delivery are leaves, with no alternate retry policy.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_typed_root_budget.inc"

static ColdRoot typed_budget_roots[2];
static void
typed_budget_fixture(unsigned budget)
{
	own_fixture();
	cf_budget_clock_fail = false;
	cold_deadline_active = true;
	memset(typed_budget_roots, 0, sizeof(typed_budget_roots));
	own_typed.replay_roots = typed_budget_roots;
	cold_deadline_plan.sealed = true;
	cold_deadline_plan.origin_count = 1;
	cold_deadline_plan.merge_wait_timeout_ms_snapshot = budget;
	cold_deadline_plan.merge_deadline_ns = budget ? set_cf_clock_ns + budget * NS_PER_MS : 0;
	cold_deadline_plan.origins[0].origin_thread = 2;
	cold_deadline_plan.origins[0].duty = retention_image.records[0].identity;
	cold_deadline_plan.origins[0].duty.origin_node_id = 1;
	cold_deadline_plan.origins[0].duty.origin_thread_id = 2;
	cold_deadline_plan.origins[0].root_snapshot = retention_image.records[0];
	cold_deadline_plan.origins[0].root_snapshot.identity = cold_deadline_plan.origins[0].duty;
	cold_deadline_plan.origins[0].root_token.origin_thread_id = 2;
	cold_deadline_plan.origins[0].root_token.root_publish_seq = 13;
}
static bool
typed_budget_call(bool *accepted)
{
	volatile bool completed = false;
	PG_TRY();
	{
		*accepted = cold_collect_roots(&own_typed, &cold_deadline_plan, 1, typed_budget_roots);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
UT_TEST(typed_own_root_cannot_deliver_after_the_original_plan_deadline)
{
	bool accepted = false;
	typed_budget_fixture(45);
	UT_ASSERT(!typed_budget_call(&accepted));
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(cold_deadline_waits[0], 45);
	UT_ASSERT_EQ(g_last_timeout_ms, 45);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	cold_cleanup();
}
UT_TEST(typed_own_root_zero_budget_does_not_sleep_or_reapply)
{
	bool accepted = false;
	typed_budget_fixture(0);
	UT_ASSERT(!typed_budget_call(&accepted));
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	cold_cleanup();
}

UT_TEST(typed_own_root_delivers_only_after_exact_ack_with_the_remaining_budget)
{
	bool accepted = false;
	typed_budget_fixture(537);
	UT_ASSERT(typed_budget_call(&accepted));
	UT_ASSERT(accepted);
	UT_ASSERT_EQ(entry_waits, 3);
	for (unsigned i = 0; i < 3; ++i)
		UT_ASSERT_EQ(cold_deadline_waits[i], 100);
	UT_ASSERT_EQ(g_last_timeout_ms, 237);
	UT_ASSERT_EQ(retention_attempts, 2);
	UT_ASSERT_EQ(retention_controls, 1);
	UT_ASSERT_EQ(retention_threads, 1);
	UT_ASSERT_EQ(own_typed.participant_count, 2);
	UT_ASSERT_EQ(typed_budget_roots[0].root.root_publish_seq, 42);
	UT_ASSERT_EQ(typed_budget_roots[0].token.root_publish_seq, 42);
	UT_ASSERT_EQ(typed_budget_roots[1].root.identity.origin_thread_id, 2);
	UT_ASSERT_EQ(typed_budget_roots[1].token.root_publish_seq, 13);
	UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
	cold_cleanup();
}

UT_TEST(typed_own_root_zero_ready_delivers_one_complete_attempt)
{
	bool accepted = false;
	typed_budget_fixture(0);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(typed_budget_call(&accepted));
	UT_ASSERT(accepted);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(own_typed.participant_count, 2);
	UT_ASSERT_EQ(typed_budget_roots[0].token.root_publish_seq, 13);
	UT_ASSERT_EQ(typed_budget_roots[1].token.root_publish_seq, 13);
	UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
	cold_cleanup();
}

static void
typed_budget_change_guc(void)
{
	own_ready();
	cluster_recovery_merge_wait_timeout = 600000;
}

UT_TEST(typed_own_root_keeps_the_original_deadline_across_wakes_and_guc_changes)
{
	bool accepted = false;
	int configured = cluster_recovery_merge_wait_timeout;
	typed_budget_fixture(137);
	entry_ack_at = 999;
	entry_on_wait = typed_budget_change_guc;
	UT_ASSERT(!typed_budget_call(&accepted));
	cluster_recovery_merge_wait_timeout = configured;
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(cold_deadline_waits[1], 37);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads, 0);
	UT_ASSERT_EQ(own_typed.participant_count, 0);
	UT_ASSERT_EQ(cold_deadline_plan.merge_deadline_ns, UINT64_C(1137000000));
	UT_ASSERT(cluster_cf_held(ShareLock));
	cold_cleanup();
}

UT_TEST(typed_own_root_expired_unknown_or_submillisecond_budget_starts_no_request)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		bool accepted = false;
		typed_budget_fixture(137);
		if (fault == 0)
			set_cf_clock_ns = cold_deadline_plan.merge_deadline_ns;
		else if (fault == 1)
			cf_budget_clock_fail = true;
		else
			cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns + UINT64_C(500000);
		UT_ASSERT_EQ(typed_budget_call(&accepted), fault == 2);
		cf_budget_clock_fail = false;
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(g_seven_count, claim_acquires);
		UT_ASSERT_EQ(retention_controls | retention_threads, 0);
		UT_ASSERT_EQ(own_typed.participant_count, 0);
		if (fault < 2)
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		else
			UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
		cold_cleanup();
	}
}

static void
typed_budget_expire(void)
{
	set_cf_clock_ns = cold_deadline_plan.merge_deadline_ns;
}

UT_TEST(typed_own_root_cannot_deliver_a_read_that_expires_after_grant_or_io)
{
	for (unsigned after_io = 0; after_io < 2; ++after_io) {
		bool accepted = false;
		ClusterControlRootReadToken zero_token = { 0 };
		ClusterControlRootSnapshot zero_root = { 0 };
		typed_budget_fixture(537);
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		if (after_io)
			retention_after_read = typed_budget_expire;
		else
			claim_after_acquire = typed_budget_expire;
		UT_ASSERT(!typed_budget_call(&accepted));
		claim_after_acquire = retention_after_read = NULL;
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, 1);
		UT_ASSERT_EQ(own_typed.participant_count, 0);
		UT_ASSERT_EQ(memcmp(&typed_budget_roots[0].root, &zero_root, sizeof(zero_root)), 0);
		UT_ASSERT_EQ(memcmp(&typed_budget_roots[0].token, &zero_token, sizeof(zero_token)), 0);
		UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
		cold_cleanup();
	}
}

UT_TEST(typed_own_root_rejects_invalid_plan_or_held_set_before_cf)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		bool accepted = false;
		typed_budget_fixture(537);
		switch (fault) {
		case 0:
			cold_deadline_plan.owner_pid++;
			break;
		case 1:
			cold_deadline_plan.sealed = false;
			break;
		case 2:
			cold_deadline_plan.committed = true;
			break;
		case 3:
			cold_deadline_plan.serial_held = true;
			break;
		case 4:
			cold_deadline_plan.serial_guards.count = 1;
			break;
		case 5:
			cold_deadline_plan.retention_pin = (ClusterWalRetentionPin *)1;
			break;
		case 6:
			cold_deadline_plan.merge_wait_timeout_ms_snapshot = -1;
			break;
		case 7:
			cold_deadline_plan.merge_wait_timeout_ms_snapshot = 600001;
			break;
		case 8:
			cold_deadline_plan.merge_deadline_ns = 0;
			break;
		}
		UT_ASSERT(typed_budget_call(&accepted));
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, 0);
		UT_ASSERT_EQ(retention_controls | retention_threads, 0);
		UT_ASSERT_EQ(own_typed.participant_count, 0);
		UT_ASSERT(!cluster_cf_held(ShareLock) && cluster_control_request_empty());
		cold_cleanup();
	}
}

UT_TEST(typed_own_root_preserves_hard_refusal_and_error_cleanup)
{
	for (unsigned error = 0; error < 2; ++error) {
		bool accepted = false;
		typed_budget_fixture(537);
		if (error)
			entry_on_wait = entry_hard_error;
		else {
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		}
		UT_ASSERT_EQ(typed_budget_call(&accepted), !error);
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(entry_waits, error ? 1 : 0);
		UT_ASSERT_EQ(retention_attempts, 1);
		UT_ASSERT_EQ(own_typed.participant_count, 0);
		if (error) {
			UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
			UT_ASSERT(cluster_cf_held(ShareLock));
			UT_ASSERT_EQ(cf_slot(ShareLock)->owner.request.timeout_ms, 1000);
		} else
			UT_ASSERT_EQ(own_typed.refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
		cold_cleanup();
	}
}

UT_TEST(typed_own_root_ack_at_the_deadline_cannot_start_another_read)
{
	bool accepted = false;
	typed_budget_fixture(300);
	UT_ASSERT(!typed_budget_call(&accepted));
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(retention_attempts, 1);
	UT_ASSERT_EQ(retention_controls | retention_threads, 0);
	UT_ASSERT_EQ(own_typed.participant_count, 0);
	cold_cleanup();
}
