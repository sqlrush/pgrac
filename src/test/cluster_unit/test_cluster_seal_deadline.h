/* Actual failure-input publisher and CF/owner/CONTROL. ROOT/WAL and the
 * original IR/WALR custody are explicit leaves. No alternate retry policy.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
seal_fixture(unsigned kind, unsigned phase)
{
	failure_fixture(kind, phase);
	/* The old child-only fixture did not traverse the public UUID guard. */
	retention_image.records[0].identity.authority_uuid[6] = 0x40;
	retention_image.records[0].identity.authority_uuid[8] = 0x80;
	failure_request.duty = retention_image.records[0].identity;
	failure_serial.duty = failure_request.duty;
	cold_restart.claim.identity = failure_request.duty;
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY,
					&failure_request.expected_root_token);
	failure_serial.root_read_token = failure_request.expected_root_token;
	UT_ASSERT(cluster_recovery_duty_key_valid_for_claim(&failure_request.duty, true));
}

UT_TEST(startup_seal_cf_failure_returns_to_its_caller_without_waiting)
{
	for (unsigned kind = 0; kind < 2; ++kind) {
		for (unsigned phase = 0; phase < (kind ? 2 : 1); ++phase) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

			seal_fixture(kind, phase);
			failure_work.caller_owns_cf_retry = true;
			UT_ASSERT(failure_call(&result));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(failure_publishes, 0);
			failure_finish();
		}
	}
}

UT_TEST(startup_seal_cf_request_obeys_the_remaining_original_budget)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

	seal_fixture(0, 0);
	set_cf_clock_ns = NS_PER_S;
	failure_work.caller_owns_cf_retry = true;
	failure_work.deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
	UT_ASSERT(failure_call(&result));
	UT_ASSERT_EQ(g_last_timeout_ms, 137);
	UT_ASSERT_EQ(entry_waits, 0);
	failure_finish();
}

static bool
seal_entry_call(uint64 deadline, ClusterControlRootResult *result, ClusterControlRootSnapshot *out,
				ClusterControlRootReadToken *token)
{
	volatile bool completed = false;
	PG_TRY();
	{
		*result = failure_kind == 0
					  ? cluster_control_root_v3_failure_open_deadline_v1(&failure_request, out,
																		 token, deadline)
					  : cluster_control_root_v3_failure_tail_deadline_v1(&failure_request, out,
																		 token, deadline);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}

UT_TEST(startup_seal_returns_cf_refusal_only_after_original_ir_and_walr_cleanup)
{
	for (unsigned kind = 0; kind < 2; ++kind) {
		for (unsigned phase = 0; phase < (kind ? 2 : 1); ++phase) {
			ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
			ClusterControlRootSnapshot out;
			ClusterControlRootReadToken token;
			ClusterControlRootSnapshot zero_out = { 0 };
			ClusterControlRootReadToken zero_token = { 0 };

			seal_fixture(kind, phase);
			memset(&out, 0x55, sizeof(out));
			memset(&token, 0x55, sizeof(token));
			UT_ASSERT(seal_entry_call(cold_deadline_plan.merge_deadline_ns, &result, &out, &token));
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(entry_waits, 0);
			UT_ASSERT_EQ(failure_ir_acquires, kind);
			UT_ASSERT_EQ(failure_ir_releases, kind);
			UT_ASSERT_EQ(failure_walr_begins, 1);
			UT_ASSERT_EQ(failure_walr_ends, 1);
			UT_ASSERT_EQ(memcmp(&out, &zero_out, sizeof(out)), 0);
			UT_ASSERT_EQ(memcmp(&token, &zero_token, sizeof(token)), 0);
			UT_ASSERT(cluster_cf_held(failure_wait_mode()));
			UT_ASSERT(!cluster_cf_held_is_usable(failure_wait_mode()));
			failure_finish();
		}
	}
}

UT_TEST(startup_seal_expiry_and_unknown_clock_start_no_new_owner)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		seal_fixture(1, 0);
		cf_budget_clock_fail = fault;
		UT_ASSERT(seal_entry_call(set_cf_clock_ns, &result, &out, &token));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(failure_ir_acquires | failure_walr_begins | failure_attempts, 0);
		UT_ASSERT_EQ(failure_publishes | entry_waits, 0);
		cf_budget_clock_fail = false;
		failure_finish();
	}
}

static void
seal_expire_clock(void)
{
	set_cf_clock_ns = cold_deadline_plan.merge_deadline_ns;
}
UT_TEST(startup_seal_expiry_after_cf_grant_or_wal_scan_never_publishes)
{
	for (unsigned phase = 0; phase < 2; ++phase) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		seal_fixture(1, 0);
		failure_acquire_result = g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		if (phase == 0)
			claim_after_acquire = seal_expire_clock;
		else
			failure_wal_hook = seal_expire_clock;
		UT_ASSERT(seal_entry_call(cold_deadline_plan.merge_deadline_ns, &result, &out, &token));
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(failure_reads, phase);
		UT_ASSERT_EQ(failure_wal_reads, phase);
		UT_ASSERT_EQ(failure_publishes | entry_waits, 0);
		UT_ASSERT_EQ(failure_ir_releases, 1);
		UT_ASSERT_EQ(failure_walr_ends, 1);
		claim_after_acquire = NULL;
		failure_finish();
	}
}

UT_TEST(startup_seal_uncertain_cleanup_preserves_debt_and_refuses_before_wait)
{
	for (unsigned walr = 0; walr < 2; ++walr) {
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		ClusterControlRootSnapshot out;
		ClusterControlRootReadToken token;
		seal_fixture(1, 0);
		failure_ir_confirmed = walr;
		failure_walr_confirmed = !walr;
		UT_ASSERT(!seal_entry_call(cold_deadline_plan.merge_deadline_ns, &result, &out, &token));
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_RECOVERY_OWNER_CONFLICT);
		UT_ASSERT_EQ(entry_waits | failure_publishes, 0);
		if (walr)
			UT_ASSERT(failure_uncertain_walr != NULL && *failure_uncertain_walr != NULL);
		else
			UT_ASSERT(failure_uncertain_ir != NULL && failure_uncertain_ir->release_uncertain);
		/* FATAL ends the production process. Restore only the custody leaves
		 * before draining the actual CF/registry, without inventing its ACK. */
		failure_ir_confirmed = failure_walr_confirmed = true;
		failure_finish();
	}
}

UT_TEST(startup_seal_error_keeps_original_cleanup_and_sqlstate)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	ClusterControlRootSnapshot out;
	ClusterControlRootReadToken token;
	seal_fixture(1, 0);
	failure_acquire_result = g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	failure_throw_read = true;
	UT_ASSERT(!seal_entry_call(cold_deadline_plan.merge_deadline_ns, &result, &out, &token));
	UT_ASSERT_EQ(entry_code, ERRCODE_IO_ERROR);
	UT_ASSERT_EQ(failure_ir_releases, 1);
	UT_ASSERT_EQ(failure_walr_ends, 1);
	UT_ASSERT_EQ(entry_waits | failure_publishes, 0);
	failure_finish();
}

UT_TEST(startup_seal_input_ir_uses_the_original_deadline)
{
	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_INPUT_SEAL, 0);
	set_cf_clock_ns = NS_PER_S;
	ir_cf_after_root_ns = 37 * NS_PER_MS;
	UT_ASSERT_EQ(cluster_recovery_serial_input_acquire_deadline_v1(
					 &ir_cf_requests[0], &ir_cf_guard, set_cf_clock_ns + 137 * NS_PER_MS),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT_EQ(ir_cf_root_budgets[0], 137);
	UT_ASSERT_EQ(ir_cf_acquire_budgets[0], 100);
	UT_ASSERT_EQ(entry_waits, 0);
	ir_cf_cleanup();
}

/* Join the actual merge caller to the actual publisher and CF implementation.
 * These adapters select the physical I/O fault only; they never retry. */
static unsigned seal_fail_step, seal_open_calls, seal_tail_calls, seal_readback_calls;
static void (*seal_on_wait)(void);
static void
seal_attempt_prepare(const ClusterRecoverySerialRequest *request, bool tail, bool first)
{
	failure_request = *request;
	failure_serial.duty = request->duty;
	failure_serial.root_read_token = request->expected_root_token;
	failure_kind = tail;
	failure_phase = tail && seal_fail_step == 2;
	failure_attempts = failure_reads = failure_wal_reads = 0;
	failure_acquire_result
		= first ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(failure_ir_acquires, failure_ir_releases);
	UT_ASSERT_EQ(failure_walr_begins, failure_walr_ends);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
}
static ClusterControlRootResult
seal_open_entry(const ClusterRecoverySerialRequest *request, ClusterControlRootSnapshot *out,
				ClusterControlRootReadToken *token, uint64 deadline)
{
	seal_attempt_prepare(request, false, seal_open_calls++ == 0 && seal_fail_step == 0);
	return cluster_control_root_v3_failure_open_deadline_v1(request, out, token, deadline);
}
static ClusterControlRootResult
seal_tail_entry(const ClusterRecoverySerialRequest *request, ClusterControlRootSnapshot *out,
				ClusterControlRootReadToken *token, uint64 deadline)
{
	seal_attempt_prepare(request, true,
						 seal_tail_calls++ == 0 && (seal_fail_step == 1 || seal_fail_step == 2));
	return cluster_control_root_v3_failure_tail_deadline_v1(request, out, token, deadline);
}
static ClusterControlRootResult
seal_canonical_entry(uint16 thread, const ClusterControlRootIdentity *duty,
					 ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token,
					 uint64 deadline)
{
	if (seal_readback_calls++ == 0 && seal_fail_step == 3)
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	return cluster_control_root_v3_read_canonical_deadline_v1(thread, duty, out, token, deadline);
}
static ClusterControlRootResult
seal_ordinary_publisher(const ClusterRecoverySerialRequest *request,
						ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token)
{
	(void)request;
	(void)out;
	(void)token;
	abort();
}
static void
pg_attribute_noreturn()
	seal_failed_leaf(ClusterRecoveryFencePlan *plan, const ClusterRecoveryDutyKey *duty,
					 const char *stage, int result)
{
	(void)plan;
	(void)duty;
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					errmsg("seal fixture stage=%s result=%d", stage, result)));
	pg_unreachable();
}
static ClusterControlRootResult
recovery_fence_seal_publish_wait(ClusterRecoveryFencePlan *plan, ClusterRecoveryFenceOrigin *origin,
								 const ClusterRecoverySerialRequest *request,
								 RecoveryFenceSealStep step, ClusterControlRootSnapshot *out,
								 ClusterControlRootReadToken *token);
#define recovery_fence_seal_refused pre_ir_seal_refused_leaf
#define recovery_fence_seal_failed seal_failed_leaf
#define cluster_control_root_v3_failure_open_publish seal_ordinary_publisher
#define cluster_control_root_v3_failure_tail_publish seal_ordinary_publisher
#define cluster_control_root_v3_failure_open_deadline_v1 seal_open_entry
#define cluster_control_root_v3_failure_tail_deadline_v1 seal_tail_entry
#define cluster_control_root_v3_read_canonical_deadline_v1 seal_canonical_entry
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#include "test_cluster_seal_publish_cf.inc"
#undef cluster_recovery_duty_key_compare_for_claim
#undef cluster_control_root_v3_read_canonical_deadline_v1
#undef cluster_control_root_v3_failure_tail_deadline_v1
#undef cluster_control_root_v3_failure_open_deadline_v1
#undef cluster_control_root_v3_failure_tail_publish
#undef cluster_control_root_v3_failure_open_publish
#undef recovery_fence_seal_failed
#undef recovery_fence_seal_refused

static void
seal_parent_ready(void)
{
	LOCKMODE mode = cluster_cf_held(ShareLock) ? ShareLock : ExclusiveLock;
	UT_ASSERT(cluster_cf_held(mode) && !cluster_cf_held_is_usable(mode));
	UT_ASSERT_EQ(failure_ir_acquires, failure_ir_releases);
	UT_ASSERT_EQ(failure_walr_begins, failure_walr_ends);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	UT_ASSERT(cold_deadline_plan.retention_pin == NULL);
	if (claim_retiring_cookie == 0)
		claim_retiring_cookie = cluster_cf_owner_cookie(mode);
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		++g_next_request_id;
	}
	if (seal_on_wait != NULL)
		seal_on_wait();
}
static void
seal_parent_fixture(unsigned step)
{
	ClusterRecoveryFenceOrigin *origin = &cold_deadline_plan.origins[0];
	seal_fixture(0, 0);
	seal_fail_step = step;
	seal_open_calls = seal_tail_calls = seal_readback_calls = 0;
	seal_on_wait = NULL;
	cold_deadline_plan.origin_count = 1;
	cold_deadline_plan.acquire_timeout_ms_snapshot = 1000;
	origin->origin_thread = 1;
	origin->duty = failure_request.duty;
	origin->root_snapshot = retention_image.records[0];
	origin->root_token = failure_request.expected_root_token;
	origin->formation = (ClusterFormationWitnessV1 *)1;
	origin->needs = (PgracExternalFenceNeedSetV1 *)2;
	origin->admissions = (PgracExternalFenceAdmissionSetV1 *)3;
	cold_deadline_active = true;
	entry_on_wait = seal_parent_ready;
}
static bool
seal_parent_call(void)
{
	volatile bool completed = false;
	PG_TRY();
	{
		recovery_fence_origin_seal(&cold_deadline_plan, &cold_deadline_plan.origins[0],
								   COLD_REQUIRED_ROOT_FLAGS);
		completed = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	return completed;
}
UT_TEST(startup_seal_merge_retries_after_cleanup_and_never_replays_open)
{
	for (unsigned step = 0; step < 3; ++step) {
		seal_parent_fixture(step);
		UT_ASSERT(seal_parent_call());
		UT_ASSERT_EQ(entry_waits, 3);
		UT_ASSERT_EQ(seal_open_calls, 1 + (step == 0));
		UT_ASSERT_EQ(seal_tail_calls, 1 + (step != 0));
		UT_ASSERT_EQ(failure_publishes, 2);
		UT_ASSERT_EQ(failure_ir_acquires, failure_ir_releases);
		UT_ASSERT_EQ(failure_walr_begins, failure_walr_ends);
		UT_ASSERT_EQ(cold_deadline_plan.origins[0].root_snapshot.root_flags
						 & COLD_REQUIRED_ROOT_FLAGS,
					 COLD_REQUIRED_ROOT_FLAGS);
		cold_deadline_active = false;
		failure_finish();
	}
}
UT_TEST(startup_seal_final_read_retries_without_republishing)
{
	seal_parent_fixture(3);
	UT_ASSERT(seal_parent_call());
	UT_ASSERT_EQ(entry_waits, 3);
	UT_ASSERT_EQ(seal_readback_calls, 2);
	UT_ASSERT_EQ(seal_open_calls, 1);
	UT_ASSERT_EQ(seal_tail_calls, 1);
	UT_ASSERT_EQ(failure_publishes, 2);
	cold_deadline_active = false;
	failure_finish();
}

UT_TEST(startup_seal_fresh_walr_uses_the_original_budget_without_changing_release)
{
	for (unsigned zero = 0; zero < 2; ++zero) {
		ClusterWalRootPublishGuard *guard = NULL;
		pin_cf_fixture(false, false);
		set_cf_clock_ns = NS_PER_S;
		UT_ASSERT_EQ(
			cluster_wal_retention_root_publish_begin_deadline_v1(
				&pin_cf_request.root_read, &guard, zero ? 0 : set_cf_clock_ns + 137 * NS_PER_MS),
			CLUSTER_WAL_PIN_OK);
		UT_ASSERT(guard != NULL && guard == active_root_publish_guard && guard->walr.held);
		UT_ASSERT(!guard->borrowed_from_pin && !guard->borrowed_from_read_pin);
		UT_ASSERT_EQ(pin_cf_walr_budgets[0], zero ? 0 : 137);
		UT_ASSERT_EQ(guard->walr.request.timeout_ms, 0);
		UT_ASSERT_EQ(walr_request_release_actual(&guard->walr.request),
					 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT_EQ(pin_cf_release_budget, 0);
		pfree(guard);
		active_root_publish_guard = NULL;
		pin_cf_cleanup();
	}
}

UT_TEST(startup_seal_fresh_walr_refuses_expired_deadline_and_foreign_pin)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterWalRootPublishGuard *guard = NULL;
		pin_cf_fixture(false, false);
		set_cf_clock_ns = NS_PER_S;
		if (fault == 1)
			cf_budget_clock_fail = true;
		if (fault == 2)
			MyBackendType = B_BACKEND;
		if (fault == 3)
			active_pin = &pin_cf_existing;
		if (fault == 4)
			active_read_pin = (ClusterWalReadPinV1 *)1;
		UT_ASSERT_EQ(cluster_wal_retention_root_publish_begin_deadline_v1(&pin_cf_request.root_read,
																		  &guard, set_cf_clock_ns),
					 fault < 2 ? CLUSTER_WAL_PIN_UNAVAILABLE : CLUSTER_WAL_PIN_INVALID);
		UT_ASSERT(guard == NULL && active_root_publish_guard == NULL);
		UT_ASSERT_EQ(pin_cf_walr_acquires | entry_waits, 0);
		cf_budget_clock_fail = false;
		MyBackendType = B_STARTUP;
		active_pin = NULL;
		active_read_pin = NULL;
		pin_cf_cleanup();
	}
}

static unsigned seal_context_fault;
static void
seal_change_original(void)
{
	switch (seal_context_fault) {
	case 0:
		owner_epoch++;
		break;
	case 1:
		cold_cohort.startup_formation_generation++;
		break;
	case 2:
		cold_deadline_plan.origins[0].root_token.file_txn_seq++;
		break;
	case 3:
		failure_formation = false;
		break;
	case 4:
		cold_restart.timeline++;
		break;
	}
}
UT_TEST(startup_seal_wait_rejects_changed_original_context_before_reapplication)
{
	for (seal_context_fault = 0; seal_context_fault < 5; ++seal_context_fault) {
		uint64 epoch;
		seal_parent_fixture(0);
		epoch = owner_epoch;
		seal_on_wait = seal_change_original;
		UT_ASSERT(!seal_parent_call());
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		/* Origin bytes are checked by the caller after exact retirement;
		 * the other faults are part of every waiter tick. */
		UT_ASSERT_EQ(entry_waits, seal_context_fault == 2 ? 3 : 1);
		UT_ASSERT_EQ(seal_open_calls, 1);
		UT_ASSERT_EQ(failure_publishes, 0);
		owner_epoch = epoch;
		cold_deadline_active = false;
		failure_finish();
	}
}
UT_TEST(startup_seal_wait_uses_the_original_deadline_and_zero_never_sleeps)
{
	for (unsigned zero = 0; zero < 2; ++zero) {
		seal_parent_fixture(1);
		cold_deadline_plan.merge_wait_timeout_ms_snapshot = zero ? 0 : 150;
		cold_deadline_plan.merge_deadline_ns = zero ? 0 : set_cf_clock_ns + 150 * NS_PER_MS;
		UT_ASSERT(!seal_parent_call());
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, zero ? 0 : 2);
		UT_ASSERT_EQ(seal_open_calls, 1);
		UT_ASSERT_EQ(seal_tail_calls, 1);
		UT_ASSERT_EQ(failure_publishes, 1);
		if (!zero) {
			UT_ASSERT_EQ(cold_deadline_waits[0], 100);
			UT_ASSERT_EQ(cold_deadline_waits[1], 50);
		}
		cold_deadline_active = false;
		failure_finish();
	}
}

static unsigned seal_receipt_fault;
static CfHoldState seal_saved_x;
static void
seal_change_x_receipt(void)
{
	if (entry_waits != 1)
		return;
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	seal_saved_x = cf_hold_x;
	if (seal_receipt_fault == 0)
		cf_hold_x.owner.request.control_owner_id++;
	if (seal_receipt_fault == 1)
		cf_hold_x.owner.attempt.generation++;
	if (seal_receipt_fault == 2)
		cf_hold_x.owner.request.holder.cluster_epoch++;
}
UT_TEST(startup_seal_x_wait_requires_the_original_exact_receipt)
{
	for (seal_receipt_fault = 0; seal_receipt_fault < 3; ++seal_receipt_fault) {
		seal_parent_fixture(0);
		seal_on_wait = seal_change_x_receipt;
		UT_ASSERT(!seal_parent_call());
		UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(entry_waits, 1);
		UT_ASSERT_EQ(seal_open_calls, 1);
		UT_ASSERT_EQ(failure_publishes, 0);
		cf_hold_x = seal_saved_x;
		cold_deadline_active = false;
		failure_finish();
	}
}
UT_TEST(startup_seal_hard_root_failure_never_waits_or_replays)
{
	seal_parent_fixture(4);
	failure_root_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	UT_ASSERT(!seal_parent_call());
	UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(entry_waits | failure_publishes | seal_tail_calls, 0);
	UT_ASSERT_EQ(seal_open_calls, 1);
	UT_ASSERT_EQ(failure_walr_begins, failure_walr_ends);
	cold_deadline_active = false;
	failure_finish();
}
UT_TEST(startup_seal_zero_complete_attempt_still_publishes_once)
{
	seal_parent_fixture(4);
	cold_deadline_plan.merge_wait_timeout_ms_snapshot = 0;
	cold_deadline_plan.merge_deadline_ns = 0;
	UT_ASSERT(seal_parent_call());
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(failure_publishes, 2);
	UT_ASSERT_EQ(seal_open_calls, 1);
	UT_ASSERT_EQ(seal_tail_calls, 1);
	UT_ASSERT_EQ(seal_readback_calls, 1);
	cold_deadline_active = false;
	failure_finish();
}
