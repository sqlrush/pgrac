/* The original merge owner resumes a commit only after exact IR/CF cleanup.
 * CF, IR, owner registry and projection are actual code. Physical pin/I/O,
 * fence proofs, clocks and ACK delivery are explicit leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned commit_owner_pin_releases, commit_owner_pin_acquires, commit_owner_pin_binds;
static bool commit_owner_pin_live, commit_owner_pin_bound, commit_owner_pin_release_ok;
static unsigned commit_owner_fault;
static uint64 commit_owner_cookie;

static bool
commit_owner_pin_request(uint16 thread, const ClusterRecoveryDutyKey *duty,
						 const ClusterControlRootSnapshot *root,
						 const ClusterControlRootReadToken *token,
						 const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
						 const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
						 const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
						 ClusterWalRetentionInterval *interval,
						 ClusterWalRetentionPinThreadRequest *out)
{
	UT_ASSERT(thread == 2 || thread == 3);
	UT_ASSERT_EQ(memcmp(duty, &root->identity, sizeof(*duty)), 0);
	UT_ASSERT_EQ(memcmp(token, &cold_deadline_plan.origins[thread - 2].root_token, sizeof(*token)),
				 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(!commit_owner_pin_live);
	MemSet(interval, 0, sizeof(*interval));
	MemSet(out, 0, sizeof(*out));
	return true;
}

static ClusterWalPinResult
commit_owner_pin_acquire(const ClusterWalRetentionPinThreadRequest *requests pg_attribute_unused(),
						 uint16 count, ClusterWalRetentionPin **out)
{
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!commit_owner_pin_live && !commit_owner_pin_bound);
	++commit_owner_pin_acquires;
	if (commit_owner_fault == 8)
		return CLUSTER_WAL_PIN_STALE;
	commit_owner_pin_live = true;
	*out = (ClusterWalRetentionPin *)&commit_cf_pin;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalPinResult
commit_owner_pin_acquire_inputs(const ClusterWalRetentionPinThreadRequest *requests, uint16 count,
								const uint16 *threads, uint16 nthreads,
								ClusterWalRetentionPin **out)
{
	UT_ASSERT_EQ(nthreads, 3);
	for (unsigned i = 0; i < nthreads; ++i)
		UT_ASSERT_EQ(threads[i], i + 1);
	return commit_owner_pin_acquire(requests, count, out);
}

static ClusterWalPinResult
commit_owner_pin_bind(ClusterWalRetentionPin *pin, ClusterRecoverySerialGuardSet *set)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)&commit_cf_pin);
	UT_ASSERT(commit_owner_pin_live && !commit_owner_pin_bound);
	UT_ASSERT_EQ(set->count, 2);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	++commit_owner_pin_binds;
	commit_owner_pin_bound = true;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalrReleaseResult
commit_owner_pin_release(ClusterWalRetentionPin **pin)
{
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT_EQ(cold_deadline_plan.serial_guards.count, 0);
	UT_ASSERT(!cold_deadline_plan.serial_held && !cold_deadline_plan.committed);
	UT_ASSERT(commit_owner_pin_live && commit_owner_pin_bound);
	UT_ASSERT(*pin == (ClusterWalRetentionPin *)&commit_cf_pin);
	++commit_owner_pin_releases;
	if (!commit_owner_pin_release_ok)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	commit_owner_pin_live = commit_owner_pin_bound = false;
	*pin = NULL;
	if (commit_owner_fault == 9) {
		commit_owner_cookie = cluster_cf_owner_cookie(ShareLock);
		UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_CF_RESID_TYPE), 1);
		cluster_lock_owners_service_poll();
		cluster_cf_retirement_poll();
		UT_ASSERT(!cluster_cf_held(ShareLock));
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		commit_cf_failure_at = retention_phase = 0;
	}
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

#define recovery_fence_plan_acquire_serial_inputs commit_owner_acquire_serial
#define recovery_fence_cf_context_current commit_owner_ir_context_current
#define recovery_fence_wait_own_cf commit_owner_ir_wait
#define cluster_thread_recovery_pin_request_build_v1 commit_owner_pin_request
#define cluster_wal_retention_pin_acquire commit_owner_pin_acquire
#define cluster_wal_retention_pin_acquire_inputs_v1 commit_owner_pin_acquire_inputs
#define cluster_wal_retention_pin_preflight_revalidate_wait_v1 set_cf_pin_revalidate
#define cluster_wal_retention_pin_bind_set commit_owner_pin_bind
#define cluster_wal_retention_pin_bind_one set_cf_pin_bind_one
#define cluster_wal_retention_pin_release commit_owner_pin_release
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#include "test_cluster_commit_owner_cf.inc"
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_wal_retention_pin_release
#undef cluster_wal_retention_pin_bind_one
#undef cluster_wal_retention_pin_bind_set
#undef cluster_wal_retention_pin_preflight_revalidate_wait_v1
#undef cluster_wal_retention_pin_acquire_inputs_v1
#undef cluster_wal_retention_pin_acquire
#undef cluster_thread_recovery_pin_request_build_v1
#undef recovery_fence_wait_own_cf
#undef recovery_fence_cf_context_current
#undef recovery_fence_plan_acquire_serial_inputs

static void
commit_owner_ready(void)
{
	UT_ASSERT_EQ(ir_cf_held_count() | cold_deadline_plan.serial_guards.count, 0);
	UT_ASSERT(!cold_deadline_plan.serial_held && !cold_deadline_plan.committed);
	UT_ASSERT(cold_deadline_plan.retention_pin == NULL);
	UT_ASSERT(!commit_owner_pin_live && !commit_owner_pin_bound);
	UT_ASSERT_EQ(commit_owner_pin_releases, 1);
	UT_ASSERT_EQ(commit_owner_pin_acquires | commit_owner_pin_binds, 0);
	UT_ASSERT_EQ(pre_ir_seals, 0);
	if (commit_owner_cookie == 0)
		commit_owner_cookie = cluster_cf_owner_cookie(ShareLock);
	if (commit_owner_fault == 1)
		++owner_epoch;
	if (commit_owner_fault == 2)
		++cold_cohort.startup_formation_generation;
	if (commit_owner_fault == 3)
		++cold_restart.timeline;
	if (commit_owner_fault == 4)
		failure_admissions = false;
	if (commit_owner_fault == 5)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("fixture cancellation")));
	if (commit_owner_fault == 6)
		++cold_deadline_plan.owner_pid;
	if (commit_owner_fault == 7)
		++owner_generation;
	if (entry_waits == entry_ack_at) {
		retention_phase = 0;
		commit_cf_failure_at = 0;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
}

static void
commit_owner_fixture(void)
{
	commit_cf_fixture();
	commit_owner_pin_live = commit_owner_pin_bound = commit_owner_pin_release_ok = true;
	commit_owner_pin_releases = commit_owner_pin_acquires = commit_owner_pin_binds = 0;
	commit_owner_fault = 0;
	commit_owner_cookie = 0;
	entry_on_wait = commit_owner_ready;
}

static bool
commit_owner_call(bool *committed)
{
	const uint16 threads[] = { 1, 2, 3 };
	volatile bool completed = false;

	PG_TRY();
	{
		*committed = cluster_recovery_merge_commit_plan_inputs_v1(&cold_deadline_plan, threads,
																  lengthof(threads));
		if (!*committed)
			printf("# commit owner refused: %s\n", cold_deadline_plan.refusal_detail);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_commit_owner_rolls_back_first_and_middle_projection_then_commits)
{
	for (unsigned middle = 0; middle < 2; middle++) {
		bool committed = false;

		commit_owner_fixture();
		retention_phase = middle ? 2 : 0;
		g_seven_result
			= middle ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		UT_ASSERT(commit_owner_call(&committed));
		UT_ASSERT(committed && cold_deadline_plan.committed);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(ir_cf_releases, 2);
		UT_ASSERT_EQ(ir_cf_acquires, 4);
		UT_ASSERT_EQ(ir_cf_held_at_root[2], 0);
		UT_ASSERT_EQ(ir_cf_held_at_root[3], 1);
		UT_ASSERT_EQ(commit_owner_pin_releases, 1);
		UT_ASSERT_EQ(commit_owner_pin_acquires, 1);
		UT_ASSERT_EQ(commit_owner_pin_binds, 1);
		UT_ASSERT(commit_owner_pin_live && commit_owner_pin_bound);
		UT_ASSERT_EQ(commit_cf_reads, 2);
		UT_ASSERT_EQ(pre_ir_seals, 0);
		UT_ASSERT(commit_owner_cookie != 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_owner_rolls_back_first_and_middle_canonical_then_commits)
{
	for (unsigned member = 1; member <= 2; member++) {
		bool committed = false;

		commit_owner_fixture();
		commit_cf_failure_at = member;
		UT_ASSERT(commit_owner_call(&committed));
		UT_ASSERT(committed && cold_deadline_plan.committed);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(ir_cf_releases, 2);
		UT_ASSERT_EQ(ir_cf_acquires, 4);
		UT_ASSERT_EQ(commit_cf_reads, member + 2);
		UT_ASSERT_EQ(commit_owner_pin_releases, 1);
		UT_ASSERT_EQ(commit_owner_pin_acquires, 1);
		UT_ASSERT_EQ(commit_owner_pin_binds, 1);
		UT_ASSERT_EQ(pre_ir_seals, 0);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_owner_zero_is_one_complete_attempt_without_cf_wait)
{
	for (unsigned pending = 0; pending < 2; pending++) {
		bool committed = false;

		commit_owner_fixture();
		cold_deadline_plan.merge_wait_timeout_ms_snapshot = 0;
		cold_deadline_plan.merge_deadline_ns = 0;
		if (pending)
			commit_cf_failure_at = 1;
		UT_ASSERT(commit_owner_call(&committed));
		UT_ASSERT_EQ(committed, pending == 0);
		UT_ASSERT_EQ(cold_deadline_plan.committed, pending == 0);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(commit_owner_pin_acquires | commit_owner_pin_binds, 0);
		UT_ASSERT_EQ(commit_owner_pin_releases, pending);
		UT_ASSERT_EQ(ir_cf_releases, pending * 2);
		UT_ASSERT_EQ(ir_cf_acquires, 2);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_owner_keeps_the_original_absolute_deadline_and_cleanup_budget)
{
	bool committed = false;

	commit_owner_fixture();
	cold_deadline_plan.merge_wait_timeout_ms_snapshot = 150;
	cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns + 150 * NS_PER_MS;
	commit_cf_failure_at = 1;
	entry_ack_at = 0;
	UT_ASSERT(!commit_owner_call(&committed));
	UT_ASSERT(!committed && !cold_deadline_plan.committed);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(cold_deadline_waits[0], 100);
	UT_ASSERT_EQ(cold_deadline_waits[1], 50);
	UT_ASSERT_EQ(commit_owner_pin_acquires | ir_cf_held_count(), 0);
	UT_ASSERT_EQ(ir_cf_releases, 2);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	commit_cf_cleanup(); /* The original CF cleanup is allowed after expiry. */
}

UT_TEST(startup_commit_owner_unconfirmed_ir_or_pin_release_retains_the_original_debt)
{
	for (unsigned pin = 0; pin < 2; pin++) {
		bool committed = false;

		commit_owner_fixture();
		commit_cf_failure_at = 1;
		if (pin)
			commit_owner_pin_release_ok = false;
		else
			ir_cf_confirm_release = false;
		UT_ASSERT_EQ(commit_owner_call(&committed), pin != 0);
		UT_ASSERT(!committed && !cold_deadline_plan.committed && !cold_deadline_plan.serial_held);
		if (!pin) {
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_RECOVERY_OWNER_CONFLICT);
			UT_ASSERT(cold_deadline_plan.serial_guards.count != 0);
			UT_ASSERT_EQ(commit_owner_pin_releases, 0);
		} else {
			UT_ASSERT_EQ(cold_deadline_plan.serial_guards.count, 0);
			UT_ASSERT(strstr(cold_deadline_plan.refusal_detail, "commit_cf_pin_release") != NULL);
		}
		UT_ASSERT(commit_owner_pin_live && cold_deadline_plan.retention_pin != NULL);
		UT_ASSERT_EQ(entry_waits | commit_owner_pin_acquires | commit_owner_pin_binds, 0);
		UT_ASSERT(cluster_cf_held(ShareLock));
		if (!pin) {
			UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_IR_RESID_TYPE), 2);
			cluster_lock_owners_service_poll();
		}
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_owner_context_change_or_cancel_never_reacquires_or_commits)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		bool committed = false;
		uint64 epoch = owner_epoch, generation = owner_generation;

		commit_owner_fixture();
		commit_owner_fault = fault;
		commit_cf_failure_at = 1;
		UT_ASSERT(!commit_owner_call(&committed));
		UT_ASSERT(!committed && !cold_deadline_plan.committed);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(commit_owner_pin_acquires | commit_owner_pin_binds, 0);
		UT_ASSERT_EQ(cold_deadline_plan.serial_guards.count | ir_cf_held_count(), 0);
		UT_ASSERT(cold_deadline_plan.retention_pin == NULL);
		UT_ASSERT(cluster_cf_held(ShareLock));
		if (fault == 5)
			UT_ASSERT_EQ(entry_code, ERRCODE_QUERY_CANCELED);
		owner_epoch = epoch;
		owner_generation = generation;
		failure_admissions = true;
		commit_cf_cleanup();
	}
}

static void
commit_owner_change_after_new_grant(void)
{
	if (entry_waits == 3)
		++cold_cohort.startup_formation_generation;
}

UT_TEST(startup_commit_owner_reproves_context_after_the_new_cf_grant)
{
	bool committed = false;

	commit_owner_fixture();
	commit_cf_failure_at = 1;
	project_after_source = commit_owner_change_after_new_grant;
	UT_ASSERT(commit_owner_call(&committed));
	UT_ASSERT(!committed && !cold_deadline_plan.committed);
	UT_ASSERT_EQ(commit_owner_pin_acquires, 1);
	UT_ASSERT_EQ(commit_owner_pin_binds, 1);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	UT_ASSERT(strstr(cold_deadline_plan.refusal_detail, "commit_cf_context") != NULL);
	commit_cf_cleanup();
}

UT_TEST(startup_commit_owner_hard_or_untyped_root_refusal_never_retries)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		bool committed = false;

		commit_owner_fixture();
		commit_cf_hard = fault;
		UT_ASSERT(commit_owner_call(&committed));
		UT_ASSERT(!committed && !cold_deadline_plan.committed);
		UT_ASSERT_EQ(entry_waits | ir_cf_releases | commit_owner_pin_releases
						 | commit_owner_pin_acquires,
					 0);
		UT_ASSERT_EQ(ir_cf_held_count(), 2);
		commit_cf_cleanup();
	}
}

UT_TEST(startup_commit_owner_exact_cf_ack_during_cleanup_does_not_lose_continuation)
{
	bool committed = false;

	commit_owner_fixture();
	commit_cf_failure_at = 1;
	commit_owner_fault = 9;
	UT_ASSERT(commit_owner_call(&committed));
	UT_ASSERT(committed && cold_deadline_plan.committed);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(commit_owner_pin_releases, 1);
	UT_ASSERT_EQ(commit_owner_pin_acquires, 1);
	UT_ASSERT_EQ(commit_owner_pin_binds, 1);
	UT_ASSERT_EQ(ir_cf_acquires, 4);
	UT_ASSERT(commit_owner_cookie != 0);
	commit_cf_cleanup();
}

UT_TEST(startup_commit_owner_native_source_cf_retry_keeps_seals_and_pin_failure_refuses)
{
	for (unsigned fail_pin = 0; fail_pin < 2; fail_pin++) {
		bool committed = false;

		commit_owner_fixture();
		project_source_timeout = true;
		commit_owner_fault = fail_pin ? 8 : 0;
		UT_ASSERT(commit_owner_call(&committed));
		UT_ASSERT_EQ(committed, fail_pin == 0);
		UT_ASSERT_EQ(cold_deadline_plan.committed, fail_pin == 0);
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(commit_owner_pin_acquires, 1);
		UT_ASSERT_EQ(commit_owner_pin_binds, fail_pin ? 0 : 1);
		UT_ASSERT_EQ(pre_ir_seals, 0);
		commit_cf_cleanup();
	}
}
