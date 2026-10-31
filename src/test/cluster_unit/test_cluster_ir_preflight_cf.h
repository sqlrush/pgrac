/* Original IR acquire/set/release and ROOT consumers with actual CF/CONTROL.
 * Physical ROOT I/O, formation/fence, S4 replies, the monotonic clock and
 * master retirement ACKs are explicit leaves. No new wait policy lives here.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "portability/instr_time.h"

static ClusterRecoverySerialRequest ir_cf_requests[2];
static ClusterControlRecoverySubject ir_cf_subject;
static ClusterRecoverySerialGuard ir_cf_guard;
static ClusterRecoverySerialGuardSet ir_cf_set;
static ClusterLockAcquireRequest ir_cf_grants[8];
static unsigned ir_cf_root_calls, ir_cf_acquires, ir_cf_releases;
static unsigned ir_cf_held_at_root[8], ir_cf_failure_at;
static bool ir_cf_confirm_release, ir_cf_release_active, ir_cf_wrong_ack;
static int64 ir_cf_clock_ms, ir_cf_after_first_grant_ms;
static int ir_cf_acquire_budgets[8], ir_cf_root_budgets[8], ir_cf_release_budgets[8];
static uint64 ir_cf_after_root_ns;

static unsigned
ir_cf_held_count(void)
{
	unsigned count = 0;

	for (ClusterLockOwner *owner = private_owners; owner != NULL; owner = owner->next)
		if (owner->request.resid.type == CLUSTER_IR_RESID_TYPE
			&& cluster_lock_owner_is_usable(owner))
			++count;
	return count;
}

static bool
ir_cf_before_root(void)
{
	if (ir_cf_root_calls >= lengthof(ir_cf_held_at_root)) {
		UT_ASSERT(false);
		return false;
	}
	ir_cf_held_at_root[ir_cf_root_calls++] = ir_cf_held_count();
	g_seven_result = ir_cf_failure_at == ir_cf_root_calls ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT
														  : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	return true;
}

static ClusterControlRootResult
ir_cf_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
				ClusterControlRootReadMode policy, ClusterControlRootSnapshot *snapshot,
				ClusterControlRootReadToken *token)
{
	ClusterControlRootResult result;

	if (!ir_cf_before_root())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_read_canonical(thread, expected, policy, snapshot, token);
	ir_cf_root_budgets[ir_cf_root_calls - 1] = g_last_timeout_ms;
	set_cf_clock_ns += ir_cf_after_root_ns;
	return result;
}

static ClusterControlRootResult
ir_cf_canonical_deadline(uint16 thread, const ClusterControlRootIdentity *expected,
						 ClusterControlRootSnapshot *snapshot, ClusterControlRootReadToken *token,
						 uint64 deadline_ns)
{
	ClusterControlRootResult result;
	if (!ir_cf_before_root())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_v3_read_canonical_deadline_v1(thread, expected, snapshot, token,
																deadline_ns);
	ir_cf_root_budgets[ir_cf_root_calls - 1] = g_last_timeout_ms;
	set_cf_clock_ns += ir_cf_after_root_ns;
	return result;
}

static ClusterControlRootResult
ir_cf_recovery_subject(uint16 thread, const ClusterControlRootIdentity *expected,
					   ClusterControlRecoverySubject *subject)
{
	if (!ir_cf_before_root())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return cluster_control_root_read_recovery_subject(thread, expected, subject);
}

static ClusterLockAcquireResult
ir_cf_owner_acquire(ClusterLockAcquireRequest *request, ClusterControlNativeFinish finish,
					void *argument)
{
	ClusterLockAcquireResult result;

	if (request->resid.type != CLUSTER_IR_RESID_TYPE || ir_cf_acquires >= lengthof(ir_cf_grants)) {
		UT_ASSERT(false);
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	ir_cf_acquire_budgets[ir_cf_acquires] = request->timeout_ms;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	++g_next_request_id;
	result = cluster_lock_owner_request_acquire(request, finish, argument);
	ir_cf_grants[ir_cf_acquires++] = *request;
	++g_next_request_id;
	if (ir_cf_acquires == 1)
		ir_cf_clock_ms += ir_cf_after_first_grant_ms;
	return result;
}

static ClusterLockAcquireResult
ir_cf_owner_release(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireResult result;

	UT_ASSERT_EQ(request->resid.type, CLUSTER_IR_RESID_TYPE);

	UT_ASSERT(ir_cf_releases < lengthof(ir_cf_release_budgets));
	ir_cf_release_budgets[ir_cf_releases++] = request->timeout_ms;
	ir_cf_release_active = true;
	result = cluster_lock_owner_request_release(request);
	ir_cf_release_active = false;
	return result;
}

/* The dedicated post-DONE purpose is absent, not a generic IR capability. */
static ClusterControlRootResult
ir_cf_tx_validate(ClusterWalInputsV1 *inputs, const ClusterRecoverySerialRequest *request)
{
	(void)inputs;
	(void)request;
	UT_ASSERT(false);
	return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
static bool
ir_cf_tx_current(ClusterWalInputsV1 *inputs, const ClusterRecoverySerialRequest *request)
{
	(void)inputs;
	(void)request;
	UT_ASSERT(false);
	return false;
}

static instr_time
ir_cf_clock(void)
{
	instr_time value;

	value.ticks = ir_cf_clock_ms * NS_PER_MS;
	return value;
}

#pragma push_macro("INSTR_TIME_SET_CURRENT")
#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(value) ((value) = ir_cf_clock())
#define clock_gettime cf_budget_clock_gettime
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define cluster_control_root_v3_read_canonical_deadline_v1 ir_cf_canonical_deadline
#define cluster_control_root_read_canonical ir_cf_canonical
#define cluster_control_root_read_recovery_subject ir_cf_recovery_subject
#define cluster_formation_witness_revalidate_nowait failure_formation_current
#define cluster_external_fence_need_set_revalidate_nowait failure_needs_current
#define cluster_external_fence_revalidate_set_nowait failure_admissions_current
#define cluster_wal_inputs_tx_serial_validate_v1 ir_cf_tx_validate
#define cluster_wal_inputs_tx_serial_current_v1 ir_cf_tx_current
#define cluster_lock_owner_request_acquire ir_cf_owner_acquire
#define cluster_lock_owner_request_release ir_cf_owner_release
#include "../../backend/cluster/cluster_ir.c"
#include "../../backend/cluster/cluster_ir_lock.c"
#undef cluster_lock_owner_request_release
#undef cluster_lock_owner_request_acquire
#undef cluster_wal_inputs_tx_serial_current_v1
#undef cluster_wal_inputs_tx_serial_validate_v1
#undef cluster_external_fence_revalidate_set_nowait
#undef cluster_external_fence_need_set_revalidate_nowait
#undef cluster_formation_witness_revalidate_nowait
#undef cluster_control_root_read_recovery_subject
#undef cluster_control_root_read_canonical
#undef cluster_control_root_v3_read_canonical_deadline_v1
#undef cluster_recovery_duty_key_compare_for_claim
#undef clock_gettime
#pragma pop_macro("INSTR_TIME_SET_CURRENT")

static unsigned
ir_cf_ack_type(uint8 type)
{
	ClusterControlRequestView view;
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage message;
	uint32 cursor = 0;
	unsigned count = 0;

	control_cut(&cut);
	while (cluster_control_request_next(&cursor, &view)) {
		if (view.state != CLUSTER_CONTROL_REQUEST_ABANDONED || view.message.key.resid.type != type)
			continue;
		UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &message));
		message.verb = CLUSTER_CONTROL_RETIRED;
		if (type == CLUSTER_IR_RESID_TYPE && ir_cf_wrong_ack) {
			ClusterControlRetireMessage wrong = message;
			ClusterControlRequestView unchanged;

			++wrong.key.holder.request_id;
			UT_ASSERT(!cluster_control_request_ack(&wrong, 0, retire_driver, &cut));
			UT_ASSERT(cluster_control_request_snapshot(&view.handle, &unchanged));
			UT_ASSERT_EQ(unchanged.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
			ir_cf_wrong_ack = false;
		}
		UT_ASSERT(cluster_control_request_ack(&message, 0, retire_driver, &cut));
		++count;
	}
	return count;
}

static void
ir_cf_release_ack(void)
{
	if (ir_cf_release_active) {
		if (ir_cf_confirm_release)
			UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_IR_RESID_TYPE), 1);
	} else {
		if (g_seven_result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_CF_RESID_TYPE), 1);
	}
}

static void
ir_cf_wait_ready(void)
{
	if (entry_waits == entry_ack_at) {
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		ir_cf_failure_at = 0;
		++g_next_request_id;
	}
}

static void
ir_cf_fixture(ClusterRecoverySerialMode mode, unsigned failure_at)
{
	UT_ASSERT(lock_owner_creator());
	retention_fixture(0);
	retention_multiple_threads = true;
	retention_image.header.file_txn_seq = 7;
	retention_expected.authority_uuid[6] = 0x40;
	retention_expected.authority_uuid[8] = 0x80;
	ir_cf_root_calls = ir_cf_acquires = ir_cf_releases = 0;
	ir_cf_failure_at = failure_at;
	ir_cf_confirm_release = true;
	ir_cf_release_active = ir_cf_wrong_ack = false;
	ir_cf_clock_ms = ir_cf_after_first_grant_ms = 0;
	ir_cf_after_root_ns = 0;
	cf_budget_clock_fail = false;
	memset(ir_cf_release_budgets, 0, sizeof(ir_cf_release_budgets));
	memset(ir_cf_root_budgets, 0, sizeof(ir_cf_root_budgets));
	memset(ir_cf_held_at_root, 0, sizeof(ir_cf_held_at_root));
	memset(ir_cf_acquire_budgets, 0, sizeof(ir_cf_acquire_budgets));
	memset(ir_cf_grants, 0, sizeof(ir_cf_grants));
	memset(&ir_cf_guard, 0, sizeof(ir_cf_guard));
	memset(&ir_cf_set, 0, sizeof(ir_cf_set));
	memset(ir_cf_requests, 0, sizeof(ir_cf_requests));
	memset(&ir_cf_subject, 0, sizeof(ir_cf_subject));
	failure_formation = failure_needs = failure_admissions = true;
	entry_on_release_wait = ir_cf_release_ack;
	entry_on_wait = ir_cf_wait_ready;
	for (unsigned i = 0; i < lengthof(ir_cf_requests); i++) {
		ClusterRecoverySerialRequest *request = &ir_cf_requests[i];

		retention_image.present[i] = true;
		retention_image.records[i] = retention_image.records[0];
		retention_image.records[i].identity = retention_expected;
		retention_image.records[i].identity.origin_thread_id = i + 1;
		retention_image.records[i].identity.origin_node_id = i;
		retention_image.records[i].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		retention_image.records[i].root_flags
			= mode == CLUSTER_RECOVERY_SERIAL_INPUT_SEAL
				  ? CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						| CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
				  : COLD_REQUIRED_ROOT_FLAGS;
		retention_image.record_crc32c[i] = 17;
		request->duty = retention_image.records[i].identity;
		request->mode = mode;
		make_read_token(&retention_image, i + 1, CONTROL_ROOT_SOURCE_PRIMARY,
						&request->expected_root_token);
		request->formation = (const ClusterFormationWitnessV1 *)1;
		request->fence_need_set = (const PgracExternalFenceNeedSetV1 *)2;
		request->fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)3;
		request->acquire_timeout_ms = request->release_timeout_ms = 1000;
	}
	if (mode == CLUSTER_RECOVERY_SERIAL_INITIALIZER) {
		ClusterControlPendingToken *pending = &ir_cf_subject.pending;

		ir_cf_subject.kind = CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER;
		ir_cf_subject.duty = ir_cf_requests[0].duty;
		pending->generation = 19;
		pending->file.file_txn_seq = 7;
		pending->file.format_version = 3;
		pending->file.record_count = 128;
		pending->file.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
		pending->file.system_identifier = retention_expected.system_identifier;
		memcpy(pending->file.authority_uuid, retention_expected.authority_uuid, 16);
		pending->file.image_sha256[0] = 11;
		pending->sha256[0] = 12;
		pending->operation_uuid[0] = 13;
		ir_cf_requests[0].pending = *pending;
		memset(&ir_cf_requests[0].expected_root_token, 0,
			   sizeof(ir_cf_requests[0].expected_root_token));
		retention_subject = &ir_cf_subject;
	}
}

static void
ir_cf_cleanup(void)
{
	ir_cf_confirm_release = true;
	if (ir_cf_guard.held)
		UT_ASSERT_EQ(cluster_recovery_serial_release(&ir_cf_guard),
					 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	if (ir_cf_set.count != 0)
		UT_ASSERT_EQ(cluster_recovery_serial_release_set(&ir_cf_set),
					 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	entry_on_release_wait = NULL;
	cold_cleanup();
}

static void
ir_cf_trace(const char *label, uint64 cookie, const ClusterControlRequestView *view)
{
	printf("# IR CF identity %s cookie=%llu handle=%u/%llu creator=%d/%u/%llu cut=%llu/%llu/%d "
		   "resid=%u/%u/%u/%u/%u holder=%u/%u/%llu/%llu mode=%d state=%d driver=%llu cleanup=%llu "
		   "exchange=%llu previous=%llu/%d verb=%d exited=%d rebuild=%llu\n",
		   label, (unsigned long long)cookie, view->handle.slot,
		   (unsigned long long)view->handle.generation, view->owner.pid, view->owner.procno,
		   (unsigned long long)view->owner.incarnation, (unsigned long long)view->cut.epoch,
		   (unsigned long long)view->cut.generation, view->cut.master, view->message.key.resid.type,
		   view->message.key.resid.lockmethodid, view->message.key.resid.field1,
		   view->message.key.resid.field2, view->message.key.resid.field3,
		   view->message.key.holder.node_id, view->message.key.holder.procno,
		   (unsigned long long)view->message.key.holder.cluster_epoch,
		   (unsigned long long)view->message.key.holder.request_id, view->mode, view->state,
		   (unsigned long long)view->driver, (unsigned long long)view->message.cleanup_epoch,
		   (unsigned long long)view->message.exchange_id,
		   (unsigned long long)view->message.previous_request, view->message.previous_mode,
		   view->message.verb, view->owner_exited,
		   (unsigned long long)view->rebuild_previous_request);
}

static void
ir_cf_assert_retained(ClusterCfRetirementSnapshot *snapshot)
{
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT(cluster_cf_retirement_snapshot(ShareLock, NULL, snapshot));
	UT_ASSERT_EQ(snapshot->cookie, cluster_cf_owner_cookie(ShareLock));
	UT_ASSERT_EQ(cf_slot(ShareLock)->owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	UT_ASSERT(cf_slot(ShareLock)->release_pending && cf_slot(ShareLock)->coordinated);
	UT_ASSERT_EQ(snapshot->registry.owner.incarnation, control_creator.incarnation);
	UT_ASSERT_EQ(snapshot->registry.message.key.resid.type, CLUSTER_CF_RESID_TYPE);
	ir_cf_trace("retained-CF", snapshot->cookie, &snapshot->registry);
}

UT_TEST(startup_ir_cf_slice_requires_retirement_and_fresh_preflight)
{
	const ClusterRecoverySerialMode modes[]
		= { CLUSTER_RECOVERY_SERIAL_COLD_FORMED, CLUSTER_RECOVERY_SERIAL_INPUT_SEAL,
			CLUSTER_RECOVERY_SERIAL_INITIALIZER, CLUSTER_RECOVERY_SERIAL_COLD_FORMED };

	for (unsigned i = 0; i < lengthof(modes); i++) {
		ClusterRecoverySerialAcquireResult result;
		uint16 failed = CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE;

		ir_cf_fixture(modes[i], i == 3 ? 2 : 1);
		result = i == 3 ? cluster_recovery_serial_acquire_set(ir_cf_requests, 2, 1000, &ir_cf_set,
															  &failed)
						: cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard);
		printf("# IR CF liveness mode=%d member=%u result=%d waits=%u CF=%u reads=%u IR=%u "
			   "release=%u held-at-root=%u retained-set=%u failed=%u\n",
			   modes[i], i == 3 ? 2 : 1, result, entry_waits, retention_attempts, retention_threads,
			   ir_cf_acquires, ir_cf_releases, ir_cf_held_at_root[i == 3 ? 1 : 0], ir_cf_set.count,
			   failed);
		if (cluster_cf_held(ShareLock)) {
			ClusterCfRetirementSnapshot snapshot;

			ir_cf_assert_retained(&snapshot);
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_SERIAL_GRANTED);
		ir_cf_cleanup();
	}
}

UT_TEST(startup_ir_cf_refusal_retains_only_its_own_cleanup)
{
	const ClusterRecoverySerialMode modes[]
		= { CLUSTER_RECOVERY_SERIAL_COLD_FORMED, CLUSTER_RECOVERY_SERIAL_INPUT_SEAL,
			CLUSTER_RECOVERY_SERIAL_INITIALIZER };

	for (unsigned i = 0; i < lengthof(modes); i++) {
		ClusterCfRetirementSnapshot snapshot;

		ir_cf_fixture(modes[i], 1);
		UT_ASSERT_EQ(cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard),
					 CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE);
		UT_ASSERT(!ir_cf_guard.held && !ir_cf_guard.release_uncertain);
		UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases | retention_threads | entry_waits, 0);
		UT_ASSERT_EQ(retention_attempts, 1);
		ir_cf_assert_retained(&snapshot);
		ir_cf_cleanup();
	}
}

UT_TEST(startup_ir_later_cf_refusal_releases_prior_ir_with_exact_ack)
{
	ClusterCfRetirementSnapshot snapshot;
	uint16 failed;

	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 2);
	UT_ASSERT_EQ(cluster_recovery_serial_acquire_set(ir_cf_requests, 2, 1000, &ir_cf_set, &failed),
				 CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE);
	UT_ASSERT_EQ(failed, 1);
	UT_ASSERT_EQ(ir_cf_set.count, 0);
	UT_ASSERT_EQ(ir_cf_held_at_root[0], 0);
	UT_ASSERT_EQ(ir_cf_held_at_root[1], 1);
	UT_ASSERT_EQ(ir_cf_acquires, 1);
	UT_ASSERT_EQ(ir_cf_releases, 1);
	UT_ASSERT_EQ(retention_threads, 1);
	UT_ASSERT_EQ(retention_attempts, 2);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	ir_cf_assert_retained(&snapshot);
	UT_ASSERT(snapshot.cookie != ir_cf_grants[0].control_owner_id);
	ir_cf_cleanup();
}

UT_TEST(startup_ir_later_cf_refusal_retains_unconfirmed_ir)
{
	ClusterCfRetirementSnapshot snapshot;
	ClusterControlRequestView view;
	ClusterLockOwner *owner;
	uint16 failed;

	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 2);
	ir_cf_confirm_release = false;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire_set(ir_cf_requests, 2, 1000, &ir_cf_set, &failed),
				 CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
	UT_ASSERT_EQ(failed, 1);
	UT_ASSERT_EQ(ir_cf_set.count, 1);
	UT_ASSERT(ir_cf_set.guards[0].held && ir_cf_set.guards[0].release_uncertain);
	UT_ASSERT_EQ(ir_cf_acquires, 1);
	UT_ASSERT_EQ(ir_cf_releases, 1);
	UT_ASSERT_EQ(ir_cf_held_at_root[1], 1);
	UT_ASSERT_EQ(ir_cf_set.guards[0].lock_request.control_owner_id,
				 ir_cf_grants[0].control_owner_id);
	owner = lock_owner_from_guard(&ir_cf_set.guards[0].lock_request);
	UT_ASSERT(owner != NULL && owner->state == CLUSTER_LOCK_OWNER_RETIRING);
	UT_ASSERT(cluster_control_request_snapshot(&owner->attempt, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT_EQ(memcmp(&view.owner, &control_creator, sizeof(control_creator)), 0);
	UT_ASSERT_EQ(
		memcmp(&view.message.key.holder, &ir_cf_grants[0].holder, sizeof(view.message.key.holder)),
		0);
	ir_cf_trace("retained-IR", ir_cf_grants[0].control_owner_id, &view);
	UT_ASSERT_EQ(
		memcmp(&view.message.key.resid, &ir_cf_grants[0].resid, sizeof(view.message.key.resid)), 0);
	ir_cf_assert_retained(&snapshot);
	printf("# IR CF rollback-unconfirmed CF-cookie=%llu IR-cookie=%llu IR-request=%llu "
		   "CF-request=%llu IR-state=%d CONTROL-state=%d\n",
		   (unsigned long long)snapshot.cookie,
		   (unsigned long long)ir_cf_grants[0].control_owner_id,
		   (unsigned long long)ir_cf_grants[0].request_id,
		   (unsigned long long)snapshot.registry.message.key.holder.request_id, owner->state,
		   view.state);
	ir_cf_wrong_ack = true;
	UT_ASSERT_EQ(ir_cf_ack_type(CLUSTER_IR_RESID_TYPE), 1);
	UT_ASSERT(!ir_cf_wrong_ack);
	ir_cf_confirm_release = true;
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&ir_cf_set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(ir_cf_set.count, 0);
	UT_ASSERT_EQ(ir_cf_acquires, 1);
	ir_cf_assert_retained(&snapshot);
	ir_cf_cleanup();
}

UT_TEST(startup_ir_set_preserves_its_original_aggregate_budget)
{
	for (unsigned expired = 0; expired < 2; expired++) {
		uint16 failed;

		ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
		ir_cf_after_first_grant_ms = expired ? 1000 : 600;
		UT_ASSERT_EQ(
			cluster_recovery_serial_acquire_set(ir_cf_requests, 2, 1000, &ir_cf_set, &failed),
			expired ? CLUSTER_RECOVERY_SERIAL_RETRY : CLUSTER_RECOVERY_SERIAL_GRANTED);
		UT_ASSERT_EQ(ir_cf_acquire_budgets[0], 1000);
		if (expired) {
			UT_ASSERT_EQ(failed, 1);
			UT_ASSERT_EQ(ir_cf_root_calls, 1);
			UT_ASSERT_EQ(ir_cf_acquires, 1);
			UT_ASSERT_EQ(ir_cf_releases, 1);
			UT_ASSERT_EQ(ir_cf_set.count, 0);
		} else {
			UT_ASSERT_EQ(failed, CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE);
			UT_ASSERT_EQ(ir_cf_root_calls, 2);
			UT_ASSERT_EQ(ir_cf_acquires, 2);
			UT_ASSERT_EQ(ir_cf_acquire_budgets[1], 400);
			UT_ASSERT_EQ(ir_cf_set.count, 2);
			UT_ASSERT_EQ(ir_cf_held_at_root[1], 1);
		}
		ir_cf_cleanup();
	}
}

UT_TEST(startup_ir_real_root_and_fence_refusals_never_grant_authority)
{
	for (unsigned refusal = 0; refusal < 6; refusal++) {
		ClusterRecoverySerialAcquireResult expected;

		ir_cf_fixture(refusal == 5 ? CLUSTER_RECOVERY_SERIAL_INITIALIZER
								   : CLUSTER_RECOVERY_SERIAL_COLD_FORMED,
					  0);
		expected = CLUSTER_RECOVERY_SERIAL_STALE;
		switch (refusal) {
		case 0:
			retention_root_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			expected = CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE;
			break;
		case 1:
			++retention_image.records[0].root_publish_seq;
			break;
		case 2:
			failure_formation = false;
			break;
		case 3:
			failure_needs = false;
			expected = CLUSTER_RECOVERY_SERIAL_FENCE_DENIED;
			break;
		case 4:
			failure_admissions = false;
			expected = CLUSTER_RECOVERY_SERIAL_FENCE_DENIED;
			break;
		case 5:
			++ir_cf_subject.pending.generation;
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard), expected);
		UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases | entry_waits, 0);
		UT_ASSERT_EQ(retention_threads, 1);
		UT_ASSERT(!ir_cf_guard.held);
		UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
		ir_cf_cleanup();
	}
}

UT_TEST(startup_ir_set_excludes_single_purpose_modes_before_cf)
{
	const ClusterRecoverySerialMode modes[]
		= { CLUSTER_RECOVERY_SERIAL_INPUT_SEAL, CLUSTER_RECOVERY_SERIAL_INITIALIZER,
			CLUSTER_RECOVERY_SERIAL_TX_CLOSURE };

	for (unsigned i = 0; i < lengthof(modes); i++) {
		uint16 failed;

		ir_cf_fixture(modes[i], 1);
		if (modes[i] == CLUSTER_RECOVERY_SERIAL_TX_CLOSURE)
			ir_cf_requests[0].tx_closure_scope = (ClusterWalInputsV1 *)4;
		UT_ASSERT_EQ(
			cluster_recovery_serial_acquire_set(ir_cf_requests, 1, 1000, &ir_cf_set, &failed),
			CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
		UT_ASSERT_EQ(failed, CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE);
		UT_ASSERT_EQ(ir_cf_root_calls | ir_cf_acquires | retention_attempts | entry_waits, 0);
		UT_ASSERT_EQ(ir_cf_set.count, 0);
		ir_cf_cleanup();
	}
}

UT_TEST(ordinary_ir_cf_refusal_keeps_original_wait_policy)
{
	const BackendType roles[] = { B_BG_WORKER, B_BACKEND };

	for (unsigned i = 0; i < lengthof(roles); i++) {
		ClusterCfRetirementSnapshot snapshot;

		ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 1);
		MyBackendType = roles[i];
		UT_ASSERT_EQ(cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard),
					 CLUSTER_RECOVERY_SERIAL_ROOT_UNAVAILABLE);
		UT_ASSERT_EQ(ir_cf_acquires | ir_cf_releases | retention_threads | entry_waits, 0);
		ir_cf_assert_retained(&snapshot);
		MyBackendType = B_STARTUP;
		ir_cf_cleanup();
	}
}
