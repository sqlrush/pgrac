/*-------------------------------------------------------------------------
 * test_cluster_control_ir.c -- real IR caller and stable cleanup ownership.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Links production IR, owner and registry objects. Formation, durable root,
 * fencing and S1-S5/transport are explicit boundary fixtures, not live proof.
 *-------------------------------------------------------------------------
 */
#define PGRAC_CONTROL_CF_EMBEDDED
#include "test_cluster_cf_enqueue.c"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_thread_recovery_authority.h"

static ClusterControlRootReadToken ir_token;
static ClusterControlRootReadToken ir_tokens[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
static uint16 ir_unavailable_thread;
static unsigned ir_members_released;
static unsigned pin_acquire_calls, pin_bind_calls, pin_release_calls;
static unsigned pin_requested_origins, pin_ir_locks_at_acquire;
static bool pin_available = true, pin_bound, pin_current = true;
static bool pin_bind_ok = true, pin_release_ok = true;
static bool pin_acquire_uncertain;
static int pin_fixture;

ClusterWalPinResult
cluster_wal_retention_pin_acquire(const ClusterWalRetentionPinThreadRequest *requests, uint16 count,
								  ClusterWalRetentionPin **out)
{
	unsigned i;

	pin_acquire_calls++;
	pin_requested_origins = count;
	pin_ir_locks_at_acquire = pg_atomic_read_u32(&MyProc->cluster_grd_registered_count);
	for (i = 0; i < count; i++) {
		UT_ASSERT_EQ(requests[i].nintervals, 1);
		UT_ASSERT_EQ(requests[i].intervals[0].thread_id, requests[i].duty.origin_thread_id);
		UT_ASSERT_EQ(requests[i].intervals[0].tli, 7);
		UT_ASSERT_EQ(requests[i].intervals[0].start_lsn, 100);
		UT_ASSERT_EQ(requests[i].intervals[0].end_lsn, 500);
	}
	if (pin_acquire_uncertain) {
		*out = (ClusterWalRetentionPin *)&pin_fixture;
		return CLUSTER_WAL_PIN_RELEASE_UNCERTAIN;
	}
	if (!pin_available)
		return CLUSTER_WAL_PIN_UNAVAILABLE;
	*out = (ClusterWalRetentionPin *)&pin_fixture;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_one(ClusterWalRetentionPin *pin, ClusterRecoverySerialGuard *serial)
{
	UT_ASSERT(pin == (ClusterWalRetentionPin *)&pin_fixture);
	UT_ASSERT(serial->held);
	pin_bind_calls++;
	pin_bound = pin_bind_ok;
	return pin_bind_ok ? CLUSTER_WAL_PIN_OK : CLUSTER_WAL_PIN_STALE;
}

ClusterWalPinResult
cluster_wal_retention_pin_bind_set(ClusterWalRetentionPin *pin, ClusterRecoverySerialGuardSet *set)
{
	unsigned i;

	UT_ASSERT_EQ(set->count, pin_requested_origins);
	for (i = 0; i < set->count; i++)
		UT_ASSERT(set->guards[i].held);
	return cluster_wal_retention_pin_bind_one(pin, &set->guards[0]);
}

ClusterWalPinResult
cluster_wal_retention_pin_revalidate(ClusterWalRetentionPin *pin)
{
	return pin == (ClusterWalRetentionPin *)&pin_fixture && pin_bound && pin_current
			   ? CLUSTER_WAL_PIN_OK
			   : CLUSTER_WAL_PIN_STALE;
}

ClusterWalrReleaseResult
cluster_wal_retention_pin_release(ClusterWalRetentionPin **pin)
{
	UT_ASSERT(*pin == (ClusterWalRetentionPin *)&pin_fixture);
	pin_release_calls++;
	if (!pin_release_ok)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	*pin = NULL;
	pin_bound = false;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}
void
cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **set)
{
	*set = NULL;
	ir_members_released++;
}
void
cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **set)
{
	*set = NULL;
}
void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	*witness = NULL;
}

/* Actual cold-set producer and destructor, not a reconstruction in the test. */
#include "test_cluster_control_ir_plan.inc"

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 thread pg_attribute_unused(),
									const ClusterControlRootIdentity *identity,
									ClusterControlRootReadMode mode pg_attribute_unused(),
									ClusterControlRootSnapshot *snapshot,
									ClusterControlRootReadToken *token)
{
	if (thread == ir_unavailable_thread)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->identity = *identity;
	snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	snapshot->root_flags = ir_tokens[thread].root_flags;
	*token = ir_tokens[thread];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: this native-lock test has only ordinary checkpoint subjects.
 * Actual pending-file discovery is covered by test_cluster_control_root.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_control_root_read_recovery_subject(uint16 thread pg_attribute_unused(),
										   const ClusterControlRootIdentity *identity
											   pg_attribute_unused(),
										   ClusterControlRecoverySubject *out)
{
	memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare_for_claim(const ClusterRecoveryDutyKey *expected,
											const ClusterRecoveryDutyKey *observed, bool claim_v2)
{
	if (!cluster_recovery_duty_key_valid_for_claim(expected, claim_v2)
		|| !cluster_recovery_duty_key_valid_for_claim(observed, claim_v2))
		return CLUSTER_RECOVERY_DUTY_COMPARE_INVALID;
	return memcmp(expected, observed, sizeof(*expected)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *w pg_attribute_unused())
{
	return CLUSTER_FORMATION_WITNESS_READY;
}

bool
cluster_external_fence_need_set_revalidate_nowait(
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

bool
cluster_external_fence_revalidate_set_nowait(
	const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

static ClusterRecoverySerialRequest
ir_request(uint16 thread)
{
	ClusterRecoverySerialRequest request = { 0 };
	ClusterRecoveryDutyKey *duty = &request.duty;

	duty->system_identifier = 123;
	duty->storage_uuid[0] = 1;
	duty->authority_uuid[0] = 2;
	duty->authority_uuid[6] = 0x40;
	duty->authority_uuid[8] = 0x80;
	duty->origin_thread_id = thread;
	duty->origin_node_id = thread - 1;
	duty->thread_claim_created_at = 1;
	duty->origin_owner_incarnation = 10;
	duty->root_lineage_seq = 11;
	memset(&ir_token, 0, sizeof(ir_token));
	ir_token.origin_thread_id = thread;
	ir_token.root_lineage_seq = duty->root_lineage_seq;
	memcpy(ir_token.authority_uuid, duty->authority_uuid, sizeof(ir_token.authority_uuid));
	ir_token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	ir_token.file_txn_seq = 5;
	ir_token.root_publish_seq = 6;
	ir_token.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	ir_tokens[thread] = ir_token;
	request.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	request.expected_root_token = ir_token;
	request.formation = (const ClusterFormationWitnessV1 *)1;
	request.fence_need_set = (const PgracExternalFenceNeedSetV1 *)2;
	request.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)3;
	request.acquire_timeout_ms = 100;
	request.release_timeout_ms = 100;
	g_next_request_id++;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	return request;
}

static void
ack_ir_thread(uint16 thread)
{
	ClusterControlRequestView view;
	ClusterControlRequestCut cut;
	ClusterControlRetireMessage message;
	uint32 cursor = 0;
	unsigned found = 0;

	control_cut(&cut);
	while (cluster_control_request_next(&cursor, &view)) {
		if (view.message.key.resid.field1 != thread
			|| view.state != CLUSTER_CONTROL_REQUEST_ABANDONED)
			continue;
		UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &message));
		message.verb = CLUSTER_CONTROL_RETIRED;
		UT_ASSERT(cluster_control_request_ack(&message, 0, retire_driver, &cut));
		found++;
	}
	UT_ASSERT_EQ(found, 1);
}

UT_TEST(ir_copied_set_compacts_without_losing_cleanup_owner)
{
	ClusterRecoverySerialGuardSet set = { 0 };
	ClusterRecoverySerialGuard temporary;
	ClusterRecoverySerialRequest request;
	uint64 surviving_cookie;
	unsigned i;

	for (i = 0; i < 2; ++i) {
		request = ir_request(i + 1);
		UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &temporary),
					 CLUSTER_RECOVERY_SERIAL_GRANTED);
		set.guards[i] = temporary;
		memset(&temporary, 0, sizeof(temporary));
	}
	set.count = 2;
	set.release_timeout_ms = 100;
	surviving_cookie = set.guards[1].lock_request.control_owner_id;
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	ack_ir_thread(1);
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(set.count, 1);
	UT_ASSERT_EQ(set.guards[0].duty.origin_thread_id, 2);
	UT_ASSERT_EQ(set.guards[0].lock_request.control_owner_id, surviving_cookie);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&set.guards[0]),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN);
	ack_ir_thread(2);
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(set.count, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(ir_failed_s5_outlives_stack_without_becoming_a_holder)
{
	ClusterRecoverySerialGuard guard;
	ClusterRecoverySerialRequest request = ir_request(3);
	ClusterControlRequestView view;
	uint32 cursor = 0;
	uint64 enumerated;

	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &guard), CLUSTER_RECOVERY_SERIAL_RETRY);
	UT_ASSERT(!guard.held);
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT(cluster_control_request_next(&cursor, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 1);
	ack_ir_thread(3);
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 0);
}

UT_TEST(ir_old_guard_is_unusable_during_rebind_but_still_releasable)
{
	ClusterRecoverySerialGuard guard;
	ClusterRecoverySerialRequest request = ir_request(4);
	uint64 enumerated;

	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &guard),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard), CLUSTER_RECOVERY_SERIAL_CURRENT);
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_PENDING;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard),
				 CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE);
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements()); /* Both original and unpublished target. */
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(cold_plan_partial_set_keeps_cleanup_without_minting_full_authority)
{
	ClusterRecoveryFencePlan *plan = palloc0(sizeof(*plan));
	ClusterRecoverySerialGuardSet saved;
	unsigned i, released = ir_members_released;
	uint64 cookie;

	plan->magic = CLUSTER_RECOVERY_FENCE_PLAN_MAGIC;
	plan->owner_pid = MyProcPid;
	plan->own_thread = 3;
	plan->origin_count = 2;
	plan->sealed = true;
	plan->acquire_timeout_ms_snapshot = 100;
	for (i = 0; i < 2; i++) {
		ClusterRecoverySerialRequest request = ir_request(i + 1);
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		origin->origin_thread = i + 1;
		origin->duty = request.duty;
		origin->root_token = request.expected_root_token;
		origin->root_snapshot.identity = origin->duty;
		origin->root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		origin->root_snapshot.root_flags = origin->root_token.root_flags;
		origin->root_snapshot.checkpoint_tli = origin->root_snapshot.tail_tli = 7;
		origin->root_snapshot.checkpoint_lower_lsn = 100;
		origin->root_snapshot.validated_tail_lsn_exclusive = 500;
		origin->formation = (ClusterFormationWitnessV1 *)request.formation;
		origin->needs = (PgracExternalFenceNeedSetV1 *)request.fence_need_set;
		origin->admissions = (PgracExternalFenceAdmissionSetV1 *)request.fence_admission_set;
	}
	ir_unavailable_thread = 2;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT(!plan->serial_held);
	UT_ASSERT_EQ(plan->serial_guards.count, 1);
	saved = plan->serial_guards;
	cookie = saved.guards[0].lock_request.control_owner_id;
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan != NULL);
	UT_ASSERT_EQ(ir_members_released, released);
	if (plan == NULL) { /* Leave the behavioral RED's fixture clean. */
		ack_ir_thread(1);
		(void)cluster_recovery_serial_release_set(&saved);
		ir_unavailable_thread = 0;
		return;
	}
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(plan->serial_guards.count, 1);
	UT_ASSERT_EQ(plan->serial_guards.guards[0].lock_request.control_owner_id, cookie);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT(!plan->serial_held);
	ack_ir_thread(1);
	UT_ASSERT(cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan == NULL);
	UT_ASSERT_EQ(ir_members_released, released + 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	ir_unavailable_thread = 0;
}

static ClusterRecoveryFencePlan *
cold_pin_plan(uint16 count)
{
	ClusterRecoveryFencePlan *plan = palloc0(sizeof(*plan));
	unsigned i;

	pin_acquire_calls = pin_bind_calls = pin_release_calls = 0;
	pin_requested_origins = pin_ir_locks_at_acquire = 0;
	pin_available = pin_bind_ok = pin_current = pin_release_ok = true;
	pin_bound = false;
	pin_acquire_uncertain = false;
	plan->magic = CLUSTER_RECOVERY_FENCE_PLAN_MAGIC;
	plan->owner_pid = MyProcPid;
	plan->own_thread = 3;
	plan->origin_count = count;
	plan->sealed = true;
	plan->acquire_timeout_ms_snapshot = 100;
	for (i = 0; i < count; i++) {
		ClusterRecoverySerialRequest request = ir_request(i + 1);
		ClusterRecoveryFenceOrigin *origin = &plan->origins[i];

		origin->origin_thread = i + 1;
		origin->duty = request.duty;
		origin->root_token = request.expected_root_token;
		origin->root_snapshot.identity = origin->duty;
		origin->root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		origin->root_snapshot.root_flags = origin->root_token.root_flags;
		origin->root_snapshot.checkpoint_tli = origin->root_snapshot.tail_tli = 7;
		origin->root_snapshot.checkpoint_lower_lsn = 100;
		origin->root_snapshot.validated_tail_lsn_exclusive = 500;
		origin->formation = (ClusterFormationWitnessV1 *)request.formation;
		origin->needs = (PgracExternalFenceNeedSetV1 *)request.fence_need_set;
		origin->admissions = (PgracExternalFenceAdmissionSetV1 *)request.fence_admission_set;
	}
	return plan;
}

static void
cold_pin_cleanup(ClusterRecoveryFencePlan **plan)
{
	if (*plan == NULL)
		return;
	pin_release_ok = true;
	(void)cluster_recovery_merge_fence_plan_release_serial(*plan);
	(void)acknowledge_retirements();
	(void)cluster_recovery_merge_fence_plan_release_serial(*plan);
	cluster_recovery_merge_fence_plan_destroy(plan);
	UT_ASSERT(*plan == NULL);
}

UT_TEST(cold_plan_pins_exact_inputs_before_any_ir)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(2);

	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(pin_acquire_calls, 1);
	UT_ASSERT_EQ(pin_requested_origins, 2);
	UT_ASSERT_EQ(pin_ir_locks_at_acquire, 0);
	UT_ASSERT_EQ(pin_bind_calls, 1);
	UT_ASSERT(plan->retention_pin != NULL && pin_bound);
	cold_pin_cleanup(&plan);
	UT_ASSERT_EQ(pin_release_calls, 1);
}

UT_TEST(cold_plan_pin_refusal_takes_no_ir)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(1);

	pin_available = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	UT_ASSERT(!plan->serial_held);
	UT_ASSERT_EQ(pin_bind_calls, 0);
	cold_pin_cleanup(&plan);
}

UT_TEST(cold_plan_failed_pin_bind_does_not_publish_full_authority)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(1);

	pin_bind_ok = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT(!plan->serial_held);
	UT_ASSERT_EQ(pin_bind_calls, 1);
	UT_ASSERT(plan->retention_pin != NULL);
	cold_pin_cleanup(&plan);
}

UT_TEST(cold_plan_pin_release_uncertain_retains_borrowed_fence_owners)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(1);
	unsigned released = ir_members_released;

	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	pin_release_ok = false;
	(void)cluster_recovery_merge_fence_plan_release_serial(plan);
	(void)acknowledge_retirements();
	UT_ASSERT(!cluster_recovery_merge_fence_plan_release_serial(plan));
	UT_ASSERT(!plan->serial_held);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan != NULL);
	UT_ASSERT_EQ(ir_members_released, released);
	cold_pin_cleanup(&plan);
}

UT_TEST(cold_plan_bad_root_has_no_partial_pin_or_ir)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(2);

	plan->origins[1].root_snapshot.tail_tli++;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(pin_acquire_calls, 0);
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	cold_pin_cleanup(&plan);
}

UT_TEST(cold_plan_uncertain_pin_acquire_keeps_cleanup_owner)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(1);
	unsigned released = ir_members_released;

	pin_acquire_uncertain = true;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_acquire_serial(plan));
	UT_ASSERT_EQ(plan->serial_guards.count, 0);
	UT_ASSERT(plan->retention_pin != NULL);
	cluster_recovery_merge_fence_plan_destroy(&plan);
	UT_ASSERT(plan != NULL);
	UT_ASSERT_EQ(ir_members_released, released);
	cold_pin_cleanup(&plan);
	UT_ASSERT_EQ(pin_release_calls, 1);
}

UT_TEST(cold_plan_authority_borrows_exact_owners_only_after_commit)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(2);
	ClusterThreadRecoveryAuthorityV1 authority, zero = { 0 };

	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	memset(&authority, 0x7f, sizeof(authority));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 2, &authority));
	UT_ASSERT(memcmp(&authority, &zero, sizeof(zero)) == 0);
	plan->committed = true;
	UT_ASSERT(cluster_recovery_merge_fence_plan_authority(plan, 2, &authority));
	UT_ASSERT(authority.duty == &plan->origins[1].duty);
	UT_ASSERT(authority.root_snapshot == &plan->origins[1].root_snapshot);
	UT_ASSERT(authority.root_token == &plan->origins[1].root_token);
	UT_ASSERT(authority.serial_guard == &plan->serial_guards.guards[1]);
	UT_ASSERT(authority.retention_pin == plan->retention_pin);
	UT_ASSERT_EQ(authority.root_snapshot->checkpoint_lower_lsn, 100);
	UT_ASSERT_EQ(authority.root_snapshot->validated_tail_lsn_exclusive, 500);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 3, &authority));
	UT_ASSERT(memcmp(&authority, &zero, sizeof(zero)) == 0);
	(void)cluster_recovery_merge_fence_plan_release_serial(plan);
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 2, &authority));
	cold_pin_cleanup(&plan);
}

UT_TEST(cold_plan_stale_pin_or_mismatched_snapshot_never_exports_authority)
{
	ClusterRecoveryFencePlan *plan = cold_pin_plan(1);
	ClusterThreadRecoveryAuthorityV1 authority, zero = { 0 };

	UT_ASSERT(cluster_recovery_merge_fence_plan_acquire_serial(plan));
	plan->committed = true;
	UT_ASSERT(cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	pin_current = false;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_revalidate_nowait(plan));
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 1, &authority));
	UT_ASSERT(memcmp(&authority, &zero, sizeof(zero)) == 0);
	pin_current = true;
	plan->origins[0].root_snapshot.identity.origin_owner_incarnation++;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 1, &authority));
	UT_ASSERT(memcmp(&authority, &zero, sizeof(zero)) == 0);
	plan->origins[0].root_snapshot.identity = plan->origins[0].duty;
	plan->serial_guards.guards[0].mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	UT_ASSERT(!cluster_recovery_merge_fence_plan_authority(plan, 1, &authority));
	plan->serial_guards.guards[0].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	UT_ASSERT(cluster_recovery_merge_fence_plan_authority(plan, 1, &authority));
	cold_pin_cleanup(&plan);
}

int
main(void)
{
	MyProcPid = 4003;
	owner_proc.pgprocno = 42;
	pg_atomic_init_u32(&owner_proc.cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked_epoch, 0);
	cluster_control_request_shmem_init();
	retire_driver = cluster_control_request_driver_start();
	cluster_shared_config = true;
	MyBackendType = B_LMON; /* Nonblocking release; ACKs are explicit above. */
	UT_PLAN(12);
	UT_RUN(ir_copied_set_compacts_without_losing_cleanup_owner);
	UT_RUN(ir_failed_s5_outlives_stack_without_becoming_a_holder);
	UT_RUN(ir_old_guard_is_unusable_during_rebind_but_still_releasable);
	UT_RUN(cold_plan_partial_set_keeps_cleanup_without_minting_full_authority);
	UT_RUN(cold_plan_pins_exact_inputs_before_any_ir);
	UT_RUN(cold_plan_pin_refusal_takes_no_ir);
	UT_RUN(cold_plan_failed_pin_bind_does_not_publish_full_authority);
	UT_RUN(cold_plan_pin_release_uncertain_retains_borrowed_fence_owners);
	UT_RUN(cold_plan_bad_root_has_no_partial_pin_or_ir);
	UT_RUN(cold_plan_uncertain_pin_acquire_keeps_cleanup_owner);
	UT_RUN(cold_plan_authority_borrows_exact_owners_only_after_commit);
	UT_RUN(cold_plan_stale_pin_or_mismatched_snapshot_never_exports_authority);
	UT_DONE();
	return ut_failed_count != 0;
}
