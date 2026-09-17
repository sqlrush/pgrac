/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_serial.c
 *	  Unit tests for the IR (instance-recovery owner) pure layer (spec-5.7 D8,
 *	  §3.4).
 *
 *	  Covers the STOP03 §17 full-duty IR resource encoder, exact typed contracts,
 *	  fail-closed pre-P4 acquire/revalidation and confirmed release/set cleanup.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_serial.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_sequence.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "storage/relfilelocator.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool IsUnderPostmaster = false;
int cluster_ges_request_timeout_ms = 1000;

static union {
	uint64 align;
	char bytes[4096];
} fake_ir_shmem;
static ClusterLockAcquireResult stub_acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
static uint32 stub_acquire_calls;
static bool stub_formation_ready;
static bool stub_need_set_ready;
static bool stub_admission_set_ready;
static ClusterControlRootResult stub_root_result;
static ClusterControlRootReadToken stub_root_token;
static ClusterLockAcquireResult stub_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
static ClusterLockAcquireResult stub_release_results[CLUSTER_RECOVERY_SERIAL_SET_MAX];
static uint16 stub_release_result_count;
static uint16 stub_release_result_index;
static ClusterLockAcquireRequest stub_last_release_request;
static uint64 stub_release_request_ids[CLUSTER_RECOVERY_SERIAL_SET_MAX];
static uint32 stub_release_calls;

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64_C(1000000);
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	UT_ASSERT(size <= sizeof(fake_ir_shmem.bytes));
	*found = false;
	memset(&fake_ir_shmem, 0, sizeof(fake_ir_shmem));
	return fake_ir_shmem.bytes;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireRequest *mutable_request = (ClusterLockAcquireRequest *)request;

	stub_acquire_calls++;
	mutable_request->request_id = UINT64_C(9001);
	mutable_request->holder.node_id = 0;
	mutable_request->holder.procno = 9;
	mutable_request->holder.cluster_epoch = UINT64_C(7);
	mutable_request->holder.request_id = mutable_request->request_id;
	return stub_acquire_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id pg_attribute_unused(),
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode pg_attribute_unused(),
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
	if (out_snapshot != NULL) {
		memset(out_snapshot, 0, sizeof(*out_snapshot));
		if (expected_identity != NULL)
			out_snapshot->identity = *expected_identity;
		out_snapshot->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		out_snapshot->root_flags = stub_root_token.root_flags;
	}
	if (out_token != NULL)
		*out_token = stub_root_token;
	return stub_root_result;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *witness pg_attribute_unused())
{
	return stub_formation_ready ? CLUSTER_FORMATION_WITNESS_READY
								: CLUSTER_FORMATION_WITNESS_UNSTABLE;
}

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed)
{
	if (!cluster_recovery_duty_key_valid_v1(expected)
		|| !cluster_recovery_duty_key_valid_v1(observed))
		return CLUSTER_RECOVERY_DUTY_COMPARE_INVALID;
	return memcmp(expected, observed, sizeof(*expected)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

bool
cluster_external_fence_need_set_revalidate_nowait(
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = stub_need_set_ready ? PGRAC_EXTERNAL_FENCE_DENY_NONE
									  : PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE;
	return stub_need_set_ready;
}

bool
cluster_external_fence_revalidate_set_nowait(
	const PgracExternalFenceAdmissionSetV1 *admissions pg_attribute_unused(),
	const PgracExternalFenceNeedSetV1 *needs pg_attribute_unused(),
	const ClusterFormationWitnessV1 *formation pg_attribute_unused(),
	PgracExternalFenceDenyReason *reason)
{
	if (reason != NULL)
		*reason = stub_admission_set_ready ? PGRAC_EXTERNAL_FENCE_DENY_NONE
										   : PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
	return stub_admission_set_ready;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireResult result = stub_release_result;

	if (stub_release_result_index < stub_release_result_count)
		result = stub_release_results[stub_release_result_index++];
	stub_release_request_ids[stub_release_calls] = request->request_id;
	stub_release_calls++;
	stub_last_release_request = *request;
	return result;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static ClusterRecoveryDutyKey
valid_duty(uint16 origin_thread, uint64 lineage)
{
	ClusterRecoveryDutyKey duty;
	ClusterWalThreadClaim claim;
	int i;

	memset(&duty, 0, sizeof(duty));
	duty.system_identifier = UINT64_C(0x0102030405060708);
	for (i = 0; i < 16; i++) {
		duty.storage_uuid[i] = (uint8)(i + 1);
		duty.authority_uuid[i] = (uint8)(0x80 + i);
	}
	duty.authority_uuid[6] = (duty.authority_uuid[6] & UINT8_C(0x0f)) | UINT8_C(0x40);
	duty.authority_uuid[8] = (duty.authority_uuid[8] & UINT8_C(0x3f)) | UINT8_C(0x80);
	duty.origin_thread_id = origin_thread;
	duty.origin_node_id = (int32)origin_thread - 1;
	duty.thread_claim_created_at = INT64_C(0x1112131415161718);
	cluster_wal_thread_claim_fill(&claim, duty.origin_thread_id, duty.origin_node_id,
								  duty.thread_claim_created_at);
	duty.thread_claim_crc32c = claim.crc;
	duty.origin_owner_incarnation = UINT64_C(77);
	duty.root_lineage_seq = lineage;
	return duty;
}

static ClusterRecoverySerialGuard
valid_held_guard(void)
{
	ClusterRecoverySerialGuard guard;

	memset(&guard, 0, sizeof(guard));
	guard.held = true;
	guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	guard.duty = valid_duty(3, UINT64_C(100));
	guard.root_read_token.origin_thread_id = guard.duty.origin_thread_id;
	guard.root_read_token.root_lineage_seq = guard.duty.root_lineage_seq;
	guard.root_read_token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	guard.root_read_token.file_txn_seq = UINT64_C(5);
	guard.root_read_token.root_publish_seq = UINT64_C(6);
	guard.root_read_token.record_crc32c = UINT32_C(7);
	guard.root_read_token.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	memcpy(guard.root_read_token.authority_uuid, guard.duty.authority_uuid,
		   sizeof(guard.root_read_token.authority_uuid));
	guard.formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	guard.fence_need_set = (const PgracExternalFenceNeedSetV1 *)(uintptr_t)2;
	guard.fence_admission_set = (const PgracExternalFenceAdmissionSetV1 *)(uintptr_t)3;
	guard.release_timeout_ms = 5000;
	UT_ASSERT(cluster_recovery_serial_resid_encode(&guard.duty, &guard.resid));
	guard.lock_request.resid = guard.resid;
	guard.lock_request.lockmode = ExclusiveLock;
	guard.lock_request.op = CLUSTER_LOCK_OP_REQUEST;
	guard.lock_request.current_mode = NoLock;
	guard.lock_request.lockmethod_id = DEFAULT_LOCKMETHOD;
	guard.lock_request.dontwait = true;
	guard.lock_request.holder.node_id = 0;
	guard.lock_request.holder.procno = 9;
	guard.lock_request.holder.cluster_epoch = UINT64_C(7);
	guard.lock_request.holder.request_id = UINT64_C(11);
	guard.lock_request.request_id = UINT64_C(11);
	guard.lock_request.timeout_ms = 9999;
	return guard;
}

static ClusterRecoverySerialGuard
valid_held_guard_for_thread(uint16 thread)
{
	ClusterRecoverySerialGuard guard = valid_held_guard();

	guard.duty = valid_duty(thread, UINT64_C(100) + thread);
	guard.root_read_token.origin_thread_id = guard.duty.origin_thread_id;
	guard.root_read_token.root_lineage_seq = guard.duty.root_lineage_seq;
	memcpy(guard.root_read_token.authority_uuid, guard.duty.authority_uuid,
		   sizeof(guard.root_read_token.authority_uuid));
	UT_ASSERT(cluster_recovery_serial_resid_encode(&guard.duty, &guard.resid));
	guard.lock_request.resid = guard.resid;
	guard.lock_request.holder.request_id = UINT64_C(1000) + thread;
	guard.lock_request.request_id = UINT64_C(1000) + thread;
	return guard;
}

static ClusterRecoverySerialRequest
valid_serial_request(void)
{
	ClusterRecoverySerialGuard held = valid_held_guard();
	ClusterRecoverySerialRequest request;

	memset(&request, 0, sizeof(request));
	request.mode = held.mode;
	request.duty = held.duty;
	request.expected_root_token = held.root_read_token;
	request.formation = held.formation;
	request.fence_need_set = held.fence_need_set;
	request.fence_admission_set = held.fence_admission_set;
	request.acquire_timeout_ms = 5000;
	request.release_timeout_ms = 5000;
	return request;
}

/* ======================================================================
 * RF-ROOT P3 / STOP03 §17.2 -- the sole IR resource is keyed by exact
 * origin thread + root lineage while the full 80-byte duty remains the
 * guard identity.
 * ====================================================================== */
UT_TEST(test_recovery_serial_resid_encode)
{
	ClusterRecoveryDutyKey duty = valid_duty(3, (UINT64_C(0x1234) << 32) | UINT64_C(0xABCD0001));
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	UT_ASSERT(cluster_recovery_serial_resid_encode(&duty, &r));

	UT_ASSERT_EQ(r.field1, 3);
	UT_ASSERT_EQ(r.field2, UINT32_C(0xABCD0001));
	UT_ASSERT_EQ(r.field3, UINT32_C(0x1234));
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_IR_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF5);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* namespace 0xF5 is distinct from SQ / CF / HW / DL */
UT_TEST(test_ir_resid_namespace_distinct)
{
	ClusterResId r;
	ClusterRecoveryDutyKey duty = valid_duty(1, 1);

	UT_ASSERT(cluster_recovery_serial_resid_encode(&duty, &r));

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
	UT_ASSERT_NE(r.type, CLUSTER_DL_RESID_TYPE); /* != 0xF3 */
}

UT_TEST(test_recovery_serial_resid_thread_and_lineage_distinct)
{
	ClusterRecoveryDutyKey a = valid_duty(3, UINT64_C(100));
	ClusterRecoveryDutyKey b = valid_duty(3, UINT64_C(101));
	ClusterRecoveryDutyKey c = valid_duty(4, UINT64_C(100));
	ClusterResId ra, rb, rc;

	UT_ASSERT(cluster_recovery_serial_resid_encode(&a, &ra));
	UT_ASSERT(cluster_recovery_serial_resid_encode(&b, &rb));
	UT_ASSERT(cluster_recovery_serial_resid_encode(&c, &rc));
	UT_ASSERT_NE(ra.field2, rb.field2);
	UT_ASSERT_NE(ra.field1, rc.field1);
	UT_ASSERT_EQ(ra.type, rb.type);
	UT_ASSERT_EQ(ra.type, rc.type);
}

UT_TEST(test_recovery_serial_resid_full_key_collision_is_conservative)
{
	ClusterRecoveryDutyKey a = valid_duty(3, UINT64_C(100));
	ClusterRecoveryDutyKey b = a;
	ClusterResId ra, rb;

	b.system_identifier++;
	b.storage_uuid[0]++;
	b.authority_uuid[0]++;
	b.origin_owner_incarnation++;
	UT_ASSERT(memcmp(&a, &b, sizeof(a)) != 0);
	UT_ASSERT(cluster_recovery_serial_resid_encode(&a, &ra));
	UT_ASSERT(cluster_recovery_serial_resid_encode(&b, &rb));
	UT_ASSERT(memcmp(&ra, &rb, sizeof(ra)) == 0);
}

UT_TEST(test_recovery_serial_resid_invalid_preserves_output)
{
	ClusterRecoveryDutyKey duty = valid_duty(3, UINT64_C(100));
	ClusterResId out;
	ClusterResId before;

	memset(&out, 0xA5, sizeof(out));
	before = out;
	UT_ASSERT(!cluster_recovery_serial_resid_encode(NULL, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);

	duty.thread_claim_crc32c++;
	UT_ASSERT(!cluster_recovery_serial_resid_encode(&duty, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	duty = valid_duty(3, UINT64_C(100));
	duty.origin_owner_incarnation = 0;
	UT_ASSERT(!cluster_recovery_serial_resid_encode(&duty, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	duty = valid_duty(3, UINT64_C(100));
	duty.root_lineage_seq = 0;
	UT_ASSERT(!cluster_recovery_serial_resid_encode(&duty, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	duty = valid_duty(3, UINT64_C(100));
	duty.reserved42 = 1;
	UT_ASSERT(!cluster_recovery_serial_resid_encode(&duty, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT(!cluster_recovery_serial_resid_encode(&duty, NULL));
}

UT_TEST(test_recovery_serial_contract_types)
{
	ClusterRecoverySerialRequest request;
	ClusterRecoverySerialGuard guard;
	ClusterRecoverySerialGuardSet set;

	memset(&request, 0, sizeof(request));
	memset(&guard, 0, sizeof(guard));
	memset(&set, 0, sizeof(set));
	request.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	request.acquire_timeout_ms = 1;
	request.release_timeout_ms = 600000;
	guard.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	set.release_timeout_ms = request.release_timeout_ms;

	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_ONLINE, 1);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 2);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_GRANTED, 0);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE, 7);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_CURRENT, 0);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN, 5);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD, 0);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID, 3);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_SET_MAX, 128);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE, UINT16_MAX);
	UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_ONLINE);
	UT_ASSERT_EQ(guard.mode, CLUSTER_RECOVERY_SERIAL_COLD_FORMED);
	UT_ASSERT_EQ(set.count, 0);
}

UT_TEST(test_recovery_serial_counter_surface_starts_zero)
{
	cluster_ir_shmem_init();
	UT_ASSERT_EQ(cluster_recovery_serial_grant_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_busy_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_retry_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate_reject_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_node_cleanup_wait_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_release_confirmed_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_release_unconfirmed_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_cold_set_grant_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_capability_denied_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_native_result_rejected_count(), UINT64_C(0));
}

UT_TEST(test_recovery_serial_release_confirmed_and_clipped)
{
	ClusterRecoverySerialGuard guard = valid_held_guard();

	cluster_ir_shmem_init();
	stub_release_calls = 0;
	stub_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(stub_release_calls, 1);
	UT_ASSERT_EQ(stub_last_release_request.timeout_ms, 1000);
	UT_ASSERT_EQ(guard.lock_request.timeout_ms, 9999);
	UT_ASSERT(!guard.held);
	UT_ASSERT(!guard.release_uncertain);
	UT_ASSERT_EQ(cluster_recovery_serial_release_confirmed_count(), UINT64_C(1));
	UT_ASSERT_EQ(cluster_recovery_serial_release_unconfirmed_count(), UINT64_C(0));
}

UT_TEST(test_recovery_serial_release_unconfirmed_preserves_guard)
{
	ClusterRecoverySerialGuard guard = valid_held_guard();

	cluster_ir_shmem_init();
	stub_release_calls = 0;
	stub_release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(stub_release_calls, 1);
	UT_ASSERT(guard.held);
	UT_ASSERT(guard.release_uncertain);
	UT_ASSERT_EQ(cluster_recovery_serial_release_confirmed_count(), UINT64_C(0));
	UT_ASSERT_EQ(cluster_recovery_serial_release_unconfirmed_count(), UINT64_C(1));

	stub_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(stub_release_calls, 2);
	UT_ASSERT(!guard.held);
	UT_ASSERT(!guard.release_uncertain);
	UT_ASSERT_EQ(cluster_recovery_serial_release_confirmed_count(), UINT64_C(1));
	UT_ASSERT_EQ(cluster_recovery_serial_release_unconfirmed_count(), UINT64_C(1));
}

UT_TEST(test_recovery_serial_release_not_held_and_invalid)
{
	ClusterRecoverySerialGuard guard;

	memset(&guard, 0, sizeof(guard));
	stub_release_calls = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_release(NULL), CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard), CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD);
	guard.release_uncertain = true;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard), CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	guard = valid_held_guard();
	guard.release_timeout_ms = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard), CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	guard = valid_held_guard();
	guard.lock_request.resid.field1++;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard), CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	UT_ASSERT_EQ(stub_release_calls, 0);
}

UT_TEST(test_recovery_serial_release_set_reverse_and_compact)
{
	ClusterRecoverySerialGuardSet set;

	UT_ASSERT_EQ(cluster_recovery_serial_release_set(NULL),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	memset(&set, 0, sizeof(set));
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_NOT_HELD);

	set.count = 3;
	set.release_timeout_ms = 5000;
	set.guards[0] = valid_held_guard_for_thread(1);
	set.guards[1] = valid_held_guard_for_thread(2);
	set.guards[2] = valid_held_guard_for_thread(3);
	stub_release_calls = 0;
	stub_release_result_index = 0;
	stub_release_result_count = 3;
	stub_release_results[0] = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	stub_release_results[1] = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	stub_release_results[2] = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCONFIRMED);
	UT_ASSERT_EQ(stub_release_calls, 3);
	UT_ASSERT_EQ(stub_release_request_ids[0], UINT64_C(1003));
	UT_ASSERT_EQ(stub_release_request_ids[1], UINT64_C(1002));
	UT_ASSERT_EQ(stub_release_request_ids[2], UINT64_C(1001));
	UT_ASSERT_EQ(set.count, 1);
	UT_ASSERT_EQ(set.guards[0].duty.origin_thread_id, 2);
	UT_ASSERT(set.guards[0].held);
	UT_ASSERT(set.guards[0].release_uncertain);

	stub_release_calls = 0;
	stub_release_result_count = 0;
	stub_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(stub_release_calls, 1);
	UT_ASSERT_EQ(set.count, 0);
	UT_ASSERT(memcmp(&set, &(ClusterRecoverySerialGuardSet){ 0 }, sizeof(set)) == 0);
}

UT_TEST(test_recovery_serial_release_set_invalid_sends_nothing)
{
	ClusterRecoverySerialGuardSet set;

	memset(&set, 0, sizeof(set));
	set.count = 2;
	set.release_timeout_ms = 5000;
	set.guards[0] = valid_held_guard_for_thread(1);
	set.guards[1] = valid_held_guard_for_thread(2);
	set.guards[1].lock_request.resid.field1++;
	stub_release_calls = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_release_set(&set),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_INVALID);
	UT_ASSERT_EQ(stub_release_calls, 0);
}

UT_TEST(test_recovery_serial_p4_acquire_requires_fresh_fence_then_actual_grant)
{
	ClusterRecoverySerialRequest request = valid_serial_request();
	ClusterRecoverySerialGuard guard;

	stub_root_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	stub_root_token = request.expected_root_token;
	stub_formation_ready = true;
	stub_need_set_ready = false;
	stub_admission_set_ready = true;
	memset(&guard, 0xA5, sizeof(guard));
	stub_acquire_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	stub_acquire_calls = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &guard),
				 CLUSTER_RECOVERY_SERIAL_FENCE_DENIED);
	UT_ASSERT(memcmp(&guard, &(ClusterRecoverySerialGuard){ 0 }, sizeof(guard)) == 0);
	UT_ASSERT_EQ(stub_acquire_calls, 0);

	stub_need_set_ready = true;
	memset(&guard, 0xA5, sizeof(guard));
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &guard),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT(guard.held);
	UT_ASSERT(!guard.release_uncertain);
	UT_ASSERT_EQ(stub_acquire_calls, 1);
	UT_ASSERT_EQ(guard.lock_request.timeout_ms, 1000);
	UT_ASSERT_EQ(guard.duty.origin_thread_id, request.duty.origin_thread_id);

	stub_release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(cluster_recovery_serial_release(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);

	memset(&guard, 0xA5, sizeof(guard));
	request.acquire_timeout_ms = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, &guard),
				 CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
	UT_ASSERT(memcmp(&guard, &(ClusterRecoverySerialGuard){ 0 }, sizeof(guard)) == 0);
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(NULL, &guard),
				 CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&request, NULL),
				 CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
}

UT_TEST(test_recovery_serial_p4_revalidate_is_two_phase_fail_closed)
{
	ClusterRecoverySerialGuard guard;

	cluster_ir_shmem_init();
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(NULL), CLUSTER_RECOVERY_SERIAL_NOT_HELD);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard), CLUSTER_RECOVERY_SERIAL_NOT_HELD);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate_reject_count(), UINT64_C(0));
	guard = valid_held_guard();
	guard.release_uncertain = true;
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_UNCERTAIN);
	guard.release_uncertain = false;
	stub_formation_ready = true;
	stub_need_set_ready = true;
	stub_admission_set_ready = true;
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard), CLUSTER_RECOVERY_SERIAL_CURRENT);
	stub_admission_set_ready = false;
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard), CLUSTER_RECOVERY_SERIAL_FENCE_STALE);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate_reject_count(), UINT64_C(2));
}

UT_TEST(test_external_rejoin_new_epoch_invalidates_held_guard)
{
	ClusterRecoverySerialGuard guard;

	cluster_ir_shmem_init();
	guard = valid_held_guard();
	stub_formation_ready = true;
	stub_need_set_ready = true;
	stub_admission_set_ready = true;
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard), CLUSTER_RECOVERY_SERIAL_CURRENT);

	/* P/new epoch invalidates the formation before any held-IR mutation. */
	stub_formation_ready = false;
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate(&guard),
				 CLUSTER_RECOVERY_SERIAL_MEMBERSHIP_STALE);
	UT_ASSERT_EQ(cluster_recovery_serial_revalidate_reject_count(), UINT64_C(1));
}

UT_TEST(test_recovery_serial_acquire_set_zero_before_first_grant)
{
	ClusterRecoverySerialRequest requests[2];
	ClusterRecoverySerialGuardSet set;
	uint16 failed_index = 7;

	requests[0] = valid_serial_request();
	requests[1] = valid_serial_request();
	requests[1].duty = valid_duty(4, UINT64_C(104));
	requests[1].expected_root_token.origin_thread_id = requests[1].duty.origin_thread_id;
	requests[1].expected_root_token.root_lineage_seq = requests[1].duty.root_lineage_seq;
	memcpy(requests[1].expected_root_token.authority_uuid, requests[1].duty.authority_uuid,
		   sizeof(requests[1].expected_root_token.authority_uuid));

	memset(&set, 0xA5, sizeof(set));
	stub_root_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	stub_root_token = requests[0].expected_root_token;
	stub_formation_ready = true;
	stub_need_set_ready = false;
	stub_admission_set_ready = true;
	stub_acquire_calls = 0;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire_set(requests, 2, 5000, &set, &failed_index),
				 CLUSTER_RECOVERY_SERIAL_FENCE_DENIED);
	UT_ASSERT_EQ(failed_index, CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE);
	UT_ASSERT_EQ(stub_acquire_calls, 0);
	UT_ASSERT(memcmp(&set, &(ClusterRecoverySerialGuardSet){ 0 }, sizeof(set)) == 0);

	memset(&set, 0xA5, sizeof(set));
	failed_index = 7;
	requests[1] = requests[0];
	UT_ASSERT_EQ(cluster_recovery_serial_acquire_set(requests, 2, 5000, &set, &failed_index),
				 CLUSTER_RECOVERY_SERIAL_INTERNAL_FAILURE);
	UT_ASSERT_EQ(failed_index, CLUSTER_RECOVERY_SERIAL_SET_FAILED_NONE);
	UT_ASSERT(memcmp(&set, &(ClusterRecoverySerialGuardSet){ 0 }, sizeof(set)) == 0);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_recovery_serial_resid_encode);
	UT_RUN(test_ir_resid_namespace_distinct);
	UT_RUN(test_recovery_serial_resid_thread_and_lineage_distinct);
	UT_RUN(test_recovery_serial_resid_full_key_collision_is_conservative);
	UT_RUN(test_recovery_serial_resid_invalid_preserves_output);
	UT_RUN(test_recovery_serial_contract_types);
	UT_RUN(test_recovery_serial_counter_surface_starts_zero);
	UT_RUN(test_recovery_serial_release_confirmed_and_clipped);
	UT_RUN(test_recovery_serial_release_unconfirmed_preserves_guard);
	UT_RUN(test_recovery_serial_release_not_held_and_invalid);
	UT_RUN(test_recovery_serial_release_set_reverse_and_compact);
	UT_RUN(test_recovery_serial_release_set_invalid_sends_nothing);
	UT_RUN(test_recovery_serial_p4_acquire_requires_fresh_fence_then_actual_grant);
	UT_RUN(test_recovery_serial_p4_revalidate_is_two_phase_fail_closed);
	UT_RUN(test_external_rejoin_new_epoch_invalidates_held_guard);
	UT_RUN(test_recovery_serial_acquire_set_zero_before_first_grant);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
