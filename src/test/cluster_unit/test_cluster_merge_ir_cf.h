/* Actual merge caller, CF/owner/CONTROL and IR acquire-set/release.
 * The unbound composite WALR pin, fence proof, S4 transport, clock and
 * master ACK delivery are explicit leaves. No alternate retry driver.
 * Author: SqlRush <sqlrush@gmail.com> */

static ClusterRecoveryFencePlan set_cf_plan;
static char set_cf_pin;
static unsigned set_cf_pin_acquires, set_cf_pin_checks, set_cf_pin_binds;
static bool set_cf_pin_current;
static void (*set_cf_on_wait)(void);
static void (*set_cf_on_pin_check)(void);
static long set_cf_wait_budgets[8];

static int
set_cf_clock_gettime(clockid_t clock, struct timespec *out)
{
	UT_ASSERT_EQ(clock, CLOCK_MONOTONIC);
	out->tv_sec = set_cf_clock_ns / NS_PER_S;
	out->tv_nsec = set_cf_clock_ns % NS_PER_S;
	return 0;
}

static bool
set_cf_pin_request(uint16 thread, const ClusterRecoveryDutyKey *duty,
				   const ClusterControlRootSnapshot *root, const ClusterControlRootReadToken *token,
				   const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
				   const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
				   const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
				   ClusterWalRetentionInterval *interval, ClusterWalRetentionPinThreadRequest *out)
{
	UT_ASSERT(thread >= 1 && thread <= 2);
	UT_ASSERT_EQ(memcmp(duty, &root->identity, sizeof(*duty)), 0);
	UT_ASSERT_EQ(token->origin_thread_id, thread);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	memset(interval, 0, sizeof(*interval));
	memset(out, 0, sizeof(*out));
	return true;
}

static ClusterWalPinResult
set_cf_pin_acquire(const ClusterWalRetentionPinThreadRequest *requests pg_attribute_unused(),
				   uint16 count, ClusterWalRetentionPin **out)
{
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++set_cf_pin_acquires;
	*out = (ClusterWalRetentionPin *)&set_cf_pin;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalPinResult
set_cf_pin_acquire_inputs(const ClusterWalRetentionPinThreadRequest *requests, uint16 count,
						  const uint16 *input_threads, uint16 ninput_threads,
						  ClusterWalRetentionPin **out)
{
	UT_ASSERT(input_threads != NULL);
	UT_ASSERT_EQ(ninput_threads, 3);
	UT_ASSERT_EQ(input_threads[0], 1);
	UT_ASSERT_EQ(input_threads[1], 2);
	UT_ASSERT_EQ(input_threads[2], 3);
	return set_cf_pin_acquire(requests, count, out);
}

static ClusterWalPinResult
pg_attribute_unused() set_cf_pin_revalidate(ClusterWalRetentionPin *pin)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)&set_cf_pin);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++set_cf_pin_checks;
	if (set_cf_on_pin_check != NULL)
		set_cf_on_pin_check();
	return set_cf_pin_current ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

static ClusterWalPinResult
set_cf_pin_bind(ClusterWalRetentionPin *pin, ClusterRecoverySerialGuardSet *set)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)&set_cf_pin);
	UT_ASSERT_EQ(set->count, 2);
	UT_ASSERT_EQ(ir_cf_held_count(), 2);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	++set_cf_pin_binds;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalPinResult
set_cf_pin_bind_one(ClusterWalRetentionPin *pin pg_attribute_unused(),
					ClusterRecoverySerialGuard *guard pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_WAL_PIN_INVALID;
}

static int
pg_attribute_unused() set_cf_wait(Latch *latch, int events, long timeout, uint32 event)
{
	UT_ASSERT(latch == MyLatch);
	UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
	UT_ASSERT_EQ(event, WAIT_EVENT_CLUSTER_STARTUP_PHASE_3);
	UT_ASSERT(timeout > 0 && timeout <= 100);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT_EQ(set_cf_plan.serial_guards.count, 0);
	UT_ASSERT(!set_cf_plan.serial_held && !set_cf_plan.committed);
	UT_ASSERT_EQ(set_cf_pin_acquires, 1);
	UT_ASSERT_EQ(set_cf_pin_binds, 0);
	UT_ASSERT(entry_resets > entry_waits);
	if (entry_waits >= lengthof(set_cf_wait_budgets))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("set fixture exceeded eight wakes")));
	set_cf_wait_budgets[entry_waits++] = timeout;
	set_cf_clock_ns += (uint64)timeout * NS_PER_MS;
	if (set_cf_on_wait != NULL)
		set_cf_on_wait();
	if (entry_waits == entry_ack_at) {
		UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_CF_RESID_TYPE), 1);
		ir_cf_failure_at = 0;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
	return (entry_waits & 1) ? WL_LATCH_SET : WL_TIMEOUT;
}

#define clock_gettime set_cf_clock_gettime
#define WaitLatch set_cf_wait
#define cluster_thread_recovery_pin_request_build_v1 set_cf_pin_request
#define cluster_wal_retention_pin_acquire set_cf_pin_acquire
#define cluster_wal_retention_pin_acquire_inputs_v1 set_cf_pin_acquire_inputs
#define cluster_wal_retention_pin_preflight_revalidate_wait_v1 set_cf_pin_revalidate
#define cluster_wal_retention_pin_bind_set set_cf_pin_bind
#define cluster_wal_retention_pin_bind_one set_cf_pin_bind_one
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#include "test_cluster_merge_ir_cf.inc"
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_wal_retention_pin_bind_one
#undef cluster_wal_retention_pin_bind_set
#undef cluster_wal_retention_pin_preflight_revalidate_wait_v1
#undef cluster_wal_retention_pin_acquire_inputs_v1
#undef cluster_wal_retention_pin_acquire
#undef cluster_thread_recovery_pin_request_build_v1
#undef WaitLatch
#undef clock_gettime

static void
set_cf_fixture(unsigned failure_at)
{
	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, failure_at);
	memset(&set_cf_plan, 0, sizeof(set_cf_plan));
	set_cf_clock_ns = NS_PER_S;
	set_cf_plan.magic = CLUSTER_RECOVERY_FENCE_PLAN_MAGIC;
	set_cf_plan.owner_pid = MyProcPid;
	set_cf_plan.owner = CurrentResourceOwner;
	set_cf_plan.owner_boot = control_creator.incarnation;
	set_cf_plan.own_thread = 3;
	set_cf_plan.origin_count = 2;
	set_cf_plan.sealed = true;
	set_cf_plan.acquire_timeout_ms_snapshot = 1000;
	set_cf_plan.merge_wait_timeout_ms_snapshot = 10000;
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + UINT64_C(10000000000);
	for (unsigned i = 0; i < 2; i++) {
		ClusterRecoveryFenceOrigin *origin = &set_cf_plan.origins[i];

		origin->origin_thread = i + 1;
		origin->duty = ir_cf_requests[i].duty;
		origin->root_token = ir_cf_requests[i].expected_root_token;
		origin->root_snapshot = retention_image.records[i];
		origin->formation = (ClusterFormationWitnessV1 *)ir_cf_requests[i].formation;
		origin->needs = (PgracExternalFenceNeedSetV1 *)ir_cf_requests[i].fence_need_set;
		origin->admissions
			= (PgracExternalFenceAdmissionSetV1 *)ir_cf_requests[i].fence_admission_set;
	}
	set_cf_pin_acquires = set_cf_pin_checks = set_cf_pin_binds = 0;
	set_cf_pin_current = true;
	set_cf_on_wait = NULL;
	set_cf_on_pin_check = NULL;
	memset(set_cf_wait_budgets, 0, sizeof(set_cf_wait_budgets));
}

static void
set_cf_cleanup(void)
{
	ir_cf_confirm_release = true;
	if (set_cf_plan.serial_guards.count != 0)
		UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set_cf_plan.serial_guards),
					 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	set_cf_plan.serial_held = false;
	set_cf_plan.retention_pin = NULL; /* The leaf pin owns no real WALR. */
	ir_cf_cleanup();
}

UT_TEST(startup_merge_first_cf_slice_restarts_the_whole_ir_set)
{
	set_cf_fixture(1);
	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(&set_cf_plan));
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(ir_cf_root_calls, 3);
	UT_ASSERT_EQ(ir_cf_held_at_root[0], 0);
	UT_ASSERT_EQ(ir_cf_held_at_root[1], 0);
	UT_ASSERT_EQ(ir_cf_held_at_root[2], 1);
	UT_ASSERT_EQ(set_cf_pin_acquires, 1);
	UT_ASSERT_EQ(set_cf_pin_checks, 1);
	UT_ASSERT_EQ(set_cf_pin_binds, 1);
	UT_ASSERT(set_cf_plan.serial_held && !set_cf_plan.committed);
	set_cf_cleanup();
}

UT_TEST(startup_merge_middle_cf_slice_rolls_back_before_waiting)
{
	for (unsigned inputs = 0; inputs < 2; inputs++) {
		const uint16 threads[] = { 1, 2, 3 };

		set_cf_fixture(2);
		UT_ASSERT(inputs ? cluster_recovery_merge_fence_plan_acquire_serial_inputs_v1(
							   &set_cf_plan, threads, lengthof(threads))
						 : cluster_recovery_merge_fence_plan_acquire_serial(&set_cf_plan));
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(ir_cf_releases, 1);
		UT_ASSERT_EQ(ir_cf_acquires, 3);
		UT_ASSERT_EQ(ir_cf_root_calls, 4);
		UT_ASSERT_EQ(ir_cf_held_at_root[0], 0);
		UT_ASSERT_EQ(ir_cf_held_at_root[1], 1);
		UT_ASSERT_EQ(ir_cf_held_at_root[2], 0);
		UT_ASSERT_EQ(ir_cf_held_at_root[3], 1);
		UT_ASSERT_EQ(set_cf_pin_acquires, 1);
		UT_ASSERT_EQ(set_cf_pin_checks, 1);
		UT_ASSERT_EQ(set_cf_pin_binds, 1);
		UT_ASSERT(set_cf_plan.serial_held && !set_cf_plan.committed);
		set_cf_cleanup();
	}
}

static bool
set_cf_call(void)
{
	volatile bool result = false;

	PG_TRY();
	{
		result = cluster_recovery_merge_fence_plan_acquire_serial(&set_cf_plan);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return result;
}

UT_TEST(startup_merge_zero_wait_performs_one_complete_ir_attempt)
{
	for (unsigned failure_at = 1; failure_at <= 2; failure_at++) {
		set_cf_fixture(failure_at);
		set_cf_plan.merge_wait_timeout_ms_snapshot = 0;
		set_cf_plan.merge_deadline_ns = 0;
		UT_ASSERT(!set_cf_call());
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(ir_cf_root_calls, failure_at);
		UT_ASSERT_EQ(ir_cf_acquires, failure_at - 1);
		UT_ASSERT_EQ(ir_cf_releases, failure_at - 1);
		UT_ASSERT_EQ(set_cf_plan.serial_guards.count | ir_cf_held_count(), 0);
		UT_ASSERT_EQ(set_cf_pin_acquires, 1);
		UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
		UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
		set_cf_cleanup();
	}
}

UT_TEST(startup_merge_cf_wait_uses_remaining_original_deadline)
{
	set_cf_fixture(2);
	set_cf_plan.merge_wait_timeout_ms_snapshot = 150;
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + 150 * NS_PER_MS;
	entry_ack_at = 0;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(set_cf_wait_budgets[0], 100);
	UT_ASSERT_EQ(set_cf_wait_budgets[1], 50);
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(ir_cf_acquires, 1);
	UT_ASSERT_EQ(ir_cf_releases, 1);
	UT_ASSERT_EQ(set_cf_plan.serial_guards.count | ir_cf_held_count(), 0);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(strstr(set_cf_plan.refusal_detail, "MERGE_DEADLINE_OR_ZERO_ATTEMPT") != NULL);
	UT_ASSERT(cluster_cf_held(ShareLock));
	set_cf_cleanup(); /* Original cleanup has its own budget after expiry. */
}

static void
set_cf_wrong_ack(void)
{
	ClusterCfRetirementSnapshot snapshot;
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage message;

	UT_ASSERT(cluster_cf_retirement_snapshot(ShareLock, NULL, &snapshot));
	control_cut(&cut);
	UT_ASSERT(
		cluster_control_request_claim(&snapshot.registry.handle, retire_driver, &cut, &message));
	message.verb = CLUSTER_CONTROL_RETIRED;
	++message.key.holder.request_id;
	UT_ASSERT(!cluster_control_request_ack(&message, 0, retire_driver, &cut));
	UT_ASSERT(!cluster_cf_release_completed(ShareLock, snapshot.cookie));
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
}

UT_TEST(startup_merge_wrong_cf_ack_never_restarts_or_binds)
{
	set_cf_fixture(2);
	set_cf_plan.merge_wait_timeout_ms_snapshot = 200;
	set_cf_plan.merge_deadline_ns = set_cf_clock_ns + 200 * NS_PER_MS;
	entry_ack_at = 0;
	set_cf_on_wait = set_cf_wrong_ack;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_waits, 2);
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases, 1);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	set_cf_cleanup();
}

static unsigned set_cf_fault;
static CfHoldState set_cf_original_slot;
static ClusterControlRequestView set_cf_original_record;

static void
set_cf_change_identity(void)
{
	set_cf_original_slot = cf_hold_s;
	set_cf_original_record = control_requests->records[cf_hold_s.owner.attempt.slot];
	switch (set_cf_fault) {
	case 0:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 1:
		MyProcPid++;
		break;
	case 2:
		control_creator.incarnation++;
		break;
	case 3:
		owner_epoch++;
		break;
	case 4:
		owner_generation++;
		break;
	case 5:
		InRecovery = !InRecovery;
		break;
	case 6:
		MyBackendType = B_BACKEND;
		break;
	case 7:
		MyAuxProcType = CheckpointerProcess;
		break;
	case 8:
		cluster_shared_config = false;
		break;
	case 9:
		cluster_enabled = false;
		break;
	case 10:
		cluster_controlfile_shared_authority = false;
		break;
	case 11:
		entry_authority = false;
		break;
	case 12:
		CritSectionCount = 1;
		break;
	case 13:
		entry_native_lock = true;
		break;
	case 14:
		failure_formation = false;
		break;
	case 15:
		failure_needs = false;
		break;
	case 16:
		failure_admissions = false;
		break;
	case 17:
		cf_hold_s.owner.request.control_owner_id++;
		break;
	case 18:
		cf_hold_s.owner.attempt.generation++;
		break;
	case 19:
		control_requests->records[cf_hold_s.owner.attempt.slot].owner.incarnation++;
		break;
	case 20:
		control_requests->records[cf_hold_s.owner.attempt.slot].message.key.holder.request_id++;
		break;
	case 21:
		control_requests->records[cf_hold_s.owner.attempt.slot].state
			= CLUSTER_CONTROL_REQUEST_INVALID;
		break;
	case 22:
		cf_hold_s.coordinated = false;
		break;
	case 23:
		cf_hold_s.caller = &set_cf_fault;
		break;
	}
}

UT_TEST(startup_merge_cf_wait_refuses_changed_original_identity_or_fence)
{
	for (set_cf_fault = 0; set_cf_fault < 24; set_cf_fault++) {
		uint64 old_epoch = owner_epoch, old_generation = owner_generation;
		int old_pid = MyProcPid;

		set_cf_fixture(2);
		entry_ack_at = 0;
		set_cf_on_wait = set_cf_change_identity;
		UT_ASSERT(!set_cf_call());
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(ir_cf_root_calls, 2);
		UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases, 1);
		UT_ASSERT_EQ(set_cf_plan.serial_guards.count | set_cf_pin_checks | set_cf_pin_binds, 0);
		/* Restore fault-injected bytes only, then use the real retirement path. */
		CurrentResourceOwner = set_cf_plan.owner;
		MyProcPid = old_pid;
		control_creator.incarnation = set_cf_plan.owner_boot;
		owner_epoch = old_epoch;
		owner_generation = old_generation;
		MyBackendType = B_STARTUP;
		MyAuxProcType = StartupProcess;
		cluster_shared_config = cluster_enabled = cluster_controlfile_shared_authority = true;
		entry_authority = failure_formation = failure_needs = failure_admissions = true;
		entry_native_lock = false;
		CritSectionCount = 0;
		cf_hold_s = set_cf_original_slot;
		control_requests->records[cf_hold_s.owner.attempt.slot] = set_cf_original_record;
		set_cf_cleanup();
	}
}

static void
set_cf_missing_receipt(void)
{
	UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_CF_RESID_TYPE), 1);
	cluster_cf_retirement_poll();
	UT_ASSERT(!cluster_cf_held(ShareLock));
	cf_retired_s = 0;
}

UT_TEST(startup_merge_cf_slot_disappearance_requires_exact_completion)
{
	set_cf_fixture(2);
	entry_ack_at = 0;
	set_cf_on_wait = set_cf_missing_receipt;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	set_cf_cleanup();
}

static void
set_cf_stale_pin(void)
{
	set_cf_pin_current = false;
}

static void
set_cf_changed_after_pin_check(void)
{
	++owner_generation;
}

static void
set_cf_expired_after_pin_check(void)
{
	set_cf_clock_ns = set_cf_plan.merge_deadline_ns;
}

UT_TEST(startup_merge_cf_completion_revalidates_original_unbound_pin)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint64 old_generation = owner_generation;

		set_cf_fixture(2);
		if (fault == 0)
			set_cf_on_wait = set_cf_stale_pin;
		else if (fault == 1)
			set_cf_on_pin_check = set_cf_changed_after_pin_check;
		else
			set_cf_on_pin_check = set_cf_expired_after_pin_check;
		UT_ASSERT(!set_cf_call());
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(ir_cf_root_calls, 2);
		UT_ASSERT_EQ(set_cf_pin_acquires, 1);
		UT_ASSERT_EQ(set_cf_pin_checks, 1);
		UT_ASSERT_EQ(set_cf_pin_binds, 0);
		UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
		owner_generation = old_generation;
		set_cf_cleanup();
	}
}

UT_TEST(startup_merge_unconfirmed_ir_rollback_refuses_before_cf_wait)
{
	set_cf_fixture(2);
	ir_cf_confirm_release = false;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_level, FATAL);
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_RECOVERY_OWNER_CONFLICT);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases, 1);
	UT_ASSERT_EQ(set_cf_plan.serial_guards.count, 1);
	UT_ASSERT(set_cf_plan.serial_guards.guards[0].release_uncertain);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	ir_cf_wrong_ack = true;
	UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_IR_RESID_TYPE), 1);
	UT_ASSERT(!ir_cf_wrong_ack);
	set_cf_cleanup();
}

UT_TEST(startup_merge_non_cf_root_refusal_keeps_original_result)
{
	set_cf_fixture(0);
	retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_waits | ir_cf_acquires | ir_cf_releases, 0);
	UT_ASSERT_EQ(ir_cf_root_calls, 1);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(strstr(set_cf_plan.refusal_detail, "stage=serial_acquire") != NULL);
	set_cf_cleanup();
}

UT_TEST(ordinary_merge_cf_refusal_keeps_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BACKEND };

	for (unsigned i = 0; i < lengthof(roles); i++) {
		set_cf_fixture(2);
		MyBackendType = roles[i];
		UT_ASSERT(!set_cf_call());
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(ir_cf_root_calls, 2);
		UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases, 1);
		UT_ASSERT_EQ(set_cf_plan.serial_guards.count | set_cf_pin_checks | set_cf_pin_binds, 0);
		UT_ASSERT(cluster_cf_held(ShareLock));
		MyBackendType = B_STARTUP;
		set_cf_cleanup();
	}
}

static void
set_cf_grant_empty_attempt(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	ir_cf_failure_at = 0;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
}

UT_TEST(startup_merge_empty_cf_slice_waits_without_inventing_a_receipt)
{
	set_cf_fixture(1);
	fail_after_publication = false;
	entry_ack_at = 0;
	set_cf_on_wait = set_cf_grant_empty_attempt;
	UT_ASSERT(set_cf_call());
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(ir_cf_root_calls, 3);
	UT_ASSERT_EQ(set_cf_pin_acquires, 1);
	UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 1);
	UT_ASSERT(set_cf_plan.serial_held && !set_cf_plan.committed);
	set_cf_cleanup();
}

UT_TEST(startup_merge_owner_census_ack_keeps_exact_cf_completion)
{
	for (unsigned failure_at = 1; failure_at <= 2; failure_at++) {
		set_cf_fixture(failure_at);
		entry_ack_at = 0;
		entry_ack_on_service = true;
		/* The service callback consumes the original ACK before CF's own poll. */
		UT_ASSERT(set_cf_call());
		UT_ASSERT(entry_service_consumed);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(ir_cf_root_calls, failure_at + 2);
		UT_ASSERT_EQ(set_cf_pin_checks | set_cf_pin_binds, 1);
		set_cf_cleanup();
	}
}

UT_TEST(startup_merge_hard_error_keeps_original_cf_cleanup)
{
	set_cf_fixture(2);
	set_cf_on_wait = entry_hard_error;
	UT_ASSERT(!set_cf_call());
	UT_ASSERT_EQ(entry_level, ERROR);
	UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
	UT_ASSERT_EQ(entry_waits, 1);
	UT_ASSERT_EQ(ir_cf_root_calls, 2);
	UT_ASSERT_EQ(set_cf_plan.serial_guards.count | set_cf_pin_checks | set_cf_pin_binds, 0);
	UT_ASSERT(cluster_cf_held(ShareLock));
	set_cf_cleanup();
}
