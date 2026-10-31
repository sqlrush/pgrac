/* Actual typed participant and CF/ROOT consumers; only I/O, clock and ACK
 * delivery are leaves. Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_typed_prepare_budget.inc"

static void
prepare_checkpoint_fixture(unsigned budget)
{
	bool accepted = false;
	typed_budget_fixture(budget);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(typed_budget_call(&accepted) && accepted);
	retention_image.header.v2.database_incarnation = 7;
	retention_image.header.v2.config_generation = 9;
	retention_image.refs[0].claim_sha256[0] = 3;
	retention_image.records[0].checkpoint_tli = 1;
	retention_image.records[0].checkpoint_lower_lsn = 80;
	retention_image.records[0].validated_tail_lsn_exclusive = 340;
	typed_budget_roots[0].root = retention_image.records[0];
	recovery_phase = 1;
	recovery_attempts = recovery_reads = recovery_wal_reads = 0;
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	recovery_root_result = recovery_wal_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	recovery_claim_valid = true;
	recovery_throw_read = false;
	recovery_work = NULL;
	recovery_wal_hook = recovery_read_hook = NULL;
	recovery_expected = recovery_selected = typed_budget_roots[0].root;
	recovery_token = typed_budget_roots[0].token;
	entry_on_wait = recovery_ready_after_ack;
}

static bool
prepare_checkpoint_call(bool *accepted)
{
	volatile bool returned = false;
	PG_TRY();
	{
		*accepted = cold_participant(&own_typed, &typed_budget_roots[0], 0,
									 &cold_deadline_plan.merge_deadline_ns);
		returned = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return returned;
}

UT_TEST(prepare_checkpoint_keeps_the_original_45ms_deadline)
{
	bool accepted = false;
	prepare_checkpoint_fixture(45);
	(void)prepare_checkpoint_call(&accepted);
	printf("# checkpoint accepted=%d waits=%u now=" UINT64_FORMAT " deadline=" UINT64_FORMAT "\n",
		   accepted, entry_waits, set_cf_clock_ns, cold_deadline_plan.merge_deadline_ns);
	UT_ASSERT(!accepted);
	UT_ASSERT(set_cf_clock_ns <= cold_deadline_plan.merge_deadline_ns);
	UT_ASSERT_EQ(own_typed.sources[0].claim.identity.origin_thread_id, 0);
	cold_cleanup();
}

UT_TEST(prepare_checkpoint_zero_busy_never_sleeps_or_reapplies)
{
	bool accepted = false;
	prepare_checkpoint_fixture(0);
	(void)prepare_checkpoint_call(&accepted);
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(recovery_attempts, 1);
	cold_cleanup();
}

UT_TEST(prepare_checkpoint_zero_ready_completes_once)
{
	bool accepted = false;
	prepare_checkpoint_fixture(0);
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(prepare_checkpoint_call(&accepted) && accepted);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(recovery_attempts, 1);
	UT_ASSERT_EQ(recovery_reads, 1);
	UT_ASSERT_EQ(own_typed.participants[0].native_redo, 80);
	cold_cleanup();
}

UT_TEST(prepare_checkpoint_ready_before_deadline_delivers_exact_source)
{
	bool accepted = false;
	prepare_checkpoint_fixture(537);
	UT_ASSERT(prepare_checkpoint_call(&accepted) && accepted);
	UT_ASSERT(set_cf_clock_ns < cold_deadline_plan.merge_deadline_ns);
	UT_ASSERT_EQ(recovery_reads, 1);
	UT_ASSERT_EQ(own_typed.sources[0].claim.identity.origin_thread_id, 1);
	cold_cleanup();
}

static void
prepare_expire_checkpoint_read(void)
{
	set_cf_clock_ns = cold_deadline_plan.merge_deadline_ns;
}
UT_TEST(prepare_checkpoint_expiring_during_io_never_delivers)
{
	bool accepted = false;
	prepare_checkpoint_fixture(45);
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	recovery_read_hook = prepare_expire_checkpoint_read;
	UT_ASSERT(!prepare_checkpoint_call(&accepted));
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(own_typed.sources[0].claim.identity.origin_thread_id, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	recovery_read_hook = NULL;
	cold_cleanup();
}
UT_TEST(prepare_census_cf_keeps_budget_for_initial_and_pinned_reads)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		for (unsigned zero = 0; zero < 2; ++zero) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			bool completed;
			input_fixture(phase);
			cold_deadline_active = true;
			input_scope.prepare_deadline_set = true;
			input_scope.prepare_deadline_ns = zero ? 0 : set_cf_clock_ns + 45 * NS_PER_MS;
			completed = input_call(&result);
			UT_ASSERT(!completed);
			UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(entry_waits, zero ? 0 : 3);
			UT_ASSERT_EQ(input_reads, 0);
			if (!zero)
				UT_ASSERT_EQ(set_cf_clock_ns, input_scope.prepare_deadline_ns);
			cold_cleanup();
		}
	}
}
static void
prepare_expire_wal_read(void)
{
	set_cf_clock_ns += 45 * NS_PER_MS;
}
UT_TEST(prepare_scan_rechecks_the_deadline_after_physical_wal)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ClusterWalTailObservation tail;
	uint64 deadline;
	recovery_fixture(2);
	cold_deadline_active = true;
	recovery_acquire_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	recovery_wal_hook = prepare_expire_wal_read;
	deadline = set_cf_clock_ns + 45 * NS_PER_MS;
	UT_ASSERT_EQ(cluster_control_root_recovery_visit_deadline_v1(
					 &recovery_expected, &recovery_token, NULL, NULL, &tail, deadline),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(recovery_reads, 1);
	UT_ASSERT_EQ(recovery_wal_reads, 1);
	UT_ASSERT_EQ(tail.complete_end, 0);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	(void)result;
	cold_cleanup();
}

static void
prepare_expire_census_read(void)
{
	set_cf_clock_ns = input_scope.prepare_deadline_ns;
}
UT_TEST(prepare_census_read_expiring_during_io_never_delivers)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		input_fixture(phase);
		cold_deadline_active = true;
		input_scope.prepare_deadline_set = true;
		input_scope.prepare_deadline_ns = set_cf_clock_ns + 45 * NS_PER_MS;
		input_read_hook = prepare_expire_census_read;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT(input_call(&result));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(input_reads, 1);
		UT_ASSERT_EQ(entry_waits, 0);
		if (input_first)
			UT_ASSERT_EQ(input_scope.token.file_txn_seq, 0);
		UT_ASSERT(!cluster_cf_held(ShareLock));
		cold_cleanup();
	}
}
UT_TEST(prepare_census_ready_and_zero_budget_keep_one_original_scope)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		for (unsigned zero = 0; zero < 2; ++zero) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			input_fixture(phase);
			cold_deadline_active = true;
			input_scope.prepare_deadline_set = true;
			input_scope.prepare_deadline_ns = zero ? 0 : set_cf_clock_ns + 45 * NS_PER_MS;
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT(input_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(input_reads, 1);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(input_scope.token.file_txn_seq, input_token.file_txn_seq);
			UT_ASSERT(!cluster_cf_held(ShareLock));
			cold_cleanup();
		}
	}
}
UT_TEST(prepare_unknown_clock_cannot_start_checkpoint_io)
{
	bool accepted = false;
	prepare_checkpoint_fixture(45);
	cf_budget_clock_fail = true;
	UT_ASSERT(!prepare_checkpoint_call(&accepted));
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(recovery_attempts | recovery_reads | entry_waits, 0);
	cf_budget_clock_fail = false;
	cold_cleanup();
}
