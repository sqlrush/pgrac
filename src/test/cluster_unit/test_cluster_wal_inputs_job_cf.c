/* Real original job/scope/pin/CF/owner/registry and ACK bodies.
 * Transport, native, allocator/context, ERROR dispatch, clock and S1-S5
 * are fixture leaves. This is not ROOT/claim or input admission.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"

static ClusterWalInputsJobV1 *
phase_job(void)
{
	ClusterWalInputsJobV1 *job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);

	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	return job;
}

UT_TEST(cf_three_original_phases_only_reuse_confirmed_receipts)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, cookie = 0;

	pin_begin();
	job = phase_job();
	owner_ack_on_wait = true;
	for (unsigned phase = 0; phase < 3; ++phase) {
		UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
		UT_ASSERT_EQ(job->cf.original.cookie, 0);
		UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
		UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
		UT_ASSERT(job->cf.original.cookie != 0 && job->cf.original.cookie != cookie);
		cookie = job->cf.original.cookie;
		UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_BUILDING);
		UT_ASSERT_EQ(job->flags, 0);
		UT_ASSERT_EQ(job->original_result, UINT32_MAX);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(job->inputs), 0);
	}
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_pending_phase_revokes_scope_but_keeps_original_pin)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalReadPinV1 **pin, *local;
	uint16 thread = 1;
	uint64 before, next, requests;
	unsigned waits;

	pin_begin();
	job = phase_job();
	pin = cluster_wal_inputs_scope_job_pin_slot_v1(job);
	UT_ASSERT(pin != NULL);
	if (pin != NULL)
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(&thread, 1, job, pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_PENDING);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT(job->inputs->stale && (job->flags & WAL_INPUT_JOB_FLAG_REVOKED) != 0);
	UT_ASSERT_EQ(pin_native_refs, 1);
	UT_ASSERT(!cluster_wal_read_pin_covers_v1(job->pin, 1));
	waits = owner_wait_calls;
	requests = g_next_request_id;
	next = UINT64_MAX;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &next), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(next, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(g_next_request_id, requests);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(pin_native_refs, 1);
	job_ack(&job->cf);
	cluster_cf_retirement_poll();
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &next), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	pin_cleanup(&job, &local);
}

UT_TEST(cf_wrong_attempt_cannot_consume_an_original_owner)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, cookie;
	unsigned waits;

	pin_begin();
	job = phase_job();
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cookie = cluster_cf_owner_cookie(ShareLock);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before + 1), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_pending_receipt_cannot_be_reset_or_forged_confirmed)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, ignored;
	unsigned waits;

	pin_begin();
	job = phase_job();
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
	waits = owner_wait_calls;
	job->cf.state = CF_JOB_RECEIPT_CONFIRMED;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &ignored), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job->cf.state = CF_JOB_RECEIPT_RETIRING;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &ignored), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_ack(&job->cf);
	cluster_cf_retirement_poll();
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_confirmed_receipt_survives_last_cookie_reuse_and_foreign_hold)
{
	ClusterWalInputsJobV1 *job;
	ClusterCfJobReceiptV1 confirmed;
	uint64 before, next, cookie;
	unsigned waits;

	pin_begin();
	job = phase_job();
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
	confirmed = job->cf;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cookie = cluster_cf_owner_cookie(ShareLock);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &next), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(next, 0);
	UT_ASSERT_EQ(memcmp(&job->cf, &confirmed, sizeof(confirmed)), 0);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(!cluster_cf_release_completed(ShareLock, confirmed.original.cookie));
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &next), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(job->cf.original.cookie, 0);
	UT_ASSERT_EQ(job->cf.job_generation, job->generation);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_foreign_tag_and_wrong_mode_never_release_another_duty)
{
	static const char other_duty;
	ClusterWalInputsJobV1 *job;
	uint64 before, cookie;
	unsigned waits;

	pin_begin();
	job = phase_job();
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	MyBackendType = B_LMON; /* The original owned cooperative API's role gate. */
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* Original poll S1/S3 contract. */
	UT_ASSERT(cluster_cf_lock_poll_owned(ShareLock, &other_duty));
	MyBackendType = B_CHECKPOINTER;
	cookie = cluster_cf_owner_cookie(ShareLock);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT(cluster_cf_held_by(ShareLock, &other_duty));
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(cluster_cf_unlock_owned(ShareLock, &other_duty), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	cookie = cluster_cf_owner_cookie(ExclusiveLock);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ExclusiveLock), cookie);
	UT_ASSERT(cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_CONFIRMED);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_multiple_attempts_cannot_be_mistaken_for_one_original_phase)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, cookie;
	unsigned waits;

	pin_begin();
	job = phase_job();
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cookie = cluster_cf_owner_cookie(ShareLock);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_invalid_job_identity_and_sequence_preserve_the_original_owner)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, cookie, ignored;
	unsigned waits;

	pin_begin();
	job = phase_job();
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(NULL, &ignored), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(ignored, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cookie = cluster_cf_owner_cookie(ShareLock);
	waits = owner_wait_calls;
	job->cf.job_generation++;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &ignored), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	job->cf.job_generation--;
	job->owner_proc_generation++;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	job->owner_proc_generation--;
	job->cf.original.cookie = 1;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	job->cf.original.cookie = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, UINT64_MAX - 1), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, UINT64_MAX), CF_JOB_INVALID);
	UT_ASSERT_EQ(cluster_cf_owner_cookie(ShareLock), cookie);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(owner_wait_calls, waits);
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_actual_native_and_failed_empty_attempts_keep_their_original_polarity)
{
	ClusterWalInputsJobV1 *job;
	ClusterCfAttemptObservation attempt;
	uint64 before;
	unsigned waits;

	pin_begin();
	job = phase_job();
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT(!cluster_cf_held_is_clusterwide(ShareLock));
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_last_attempt_since(before, &attempt));
	UT_ASSERT_EQ(attempt.result, CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(attempt.owner_state_at_return, CLUSTER_LOCK_OWNER_EMPTY);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_CONFIRMED);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(job->original_result, UINT32_MAX);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(job->inputs), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_missing_original_owner_is_not_a_confirmed_receipt)
{
	ClusterWalInputsJobV1 *job;
	uint64 before;

	pin_begin();
	job = phase_job();
	owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job, before), CF_JOB_INVALID);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT_EQ(job->cf.original.cookie, 0);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_ack_arriving_before_prepare_still_cannot_resurrect_pending_build)
{
	ClusterWalInputsJobV1 *job;
	uint64 before, ignored;
	unsigned waits;

	pin_begin();
	job = phase_job();
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
	job_ack(&job->cf);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &ignored), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT(job->inputs->stale && (job->flags & WAL_INPUT_JOB_FLAG_REVOKED) != 0);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(cf_throw_after_binding_keeps_debt_for_original_owner_cleanup)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalReadPinV1 **pin, *local;
	uint16 thread = 1;
	uint64 before;
	unsigned waits;
	volatile bool caught = false;

	pin_begin();
	job = phase_job();
	pin = cluster_wal_inputs_scope_job_pin_slot_v1(job);
	UT_ASSERT(pin != NULL);
	if (pin != NULL)
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(&thread, 1, job, pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job, &before), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	job_throw_on_wait = true;
	PG_TRY(); { (void)cluster_wal_inputs_job_cf_release_v1(job, before); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !job_throw_on_wait);
	UT_ASSERT_EQ(job->cf.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(pin_native_refs, 1);
	waits = owner_wait_calls;
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT((job->flags & (WAL_INPUT_JOB_FLAG_ERROR | WAL_INPUT_JOB_FLAG_REVOKED))
		== (WAL_INPUT_JOB_FLAG_ERROR | WAL_INPUT_JOB_FLAG_REVOKED));
	UT_ASSERT(job->inputs->stale);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 1);
	UT_ASSERT_EQ(job->original_result, UINT32_MAX);
	job_ack(&job->cf);
	cluster_cf_retirement_poll();
	pin_cleanup(&job, &local);
}

int
main(void)
{
	MyProc = &job_procs[42];
	MyProc->pgprocno = 42;
	MyLatch = &MyProc->procLatch;
	job_proc_global.allProcs = job_procs;
	job_proc_global.allProcCount = lengthof(job_procs);
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch, 0);
	MyProc->cluster_grd_generation = 1;
	cluster_shared_config = true;
	cluster_control_request_shmem_init();
	control_driver = retire_driver = cluster_control_request_driver_start();
	UT_PLAN(12);
	UT_RUN(cf_three_original_phases_only_reuse_confirmed_receipts);
	UT_RUN(cf_pending_phase_revokes_scope_but_keeps_original_pin);
	UT_RUN(cf_wrong_attempt_cannot_consume_an_original_owner);
	UT_RUN(cf_pending_receipt_cannot_be_reset_or_forged_confirmed);
	UT_RUN(cf_confirmed_receipt_survives_last_cookie_reuse_and_foreign_hold);
	UT_RUN(cf_foreign_tag_and_wrong_mode_never_release_another_duty);
	UT_RUN(cf_multiple_attempts_cannot_be_mistaken_for_one_original_phase);
	UT_RUN(cf_invalid_job_identity_and_sequence_preserve_the_original_owner);
	UT_RUN(cf_actual_native_and_failed_empty_attempts_keep_their_original_polarity);
	UT_RUN(cf_missing_original_owner_is_not_a_confirmed_receipt);
	UT_RUN(cf_ack_arriving_before_prepare_still_cannot_resurrect_pending_build);
	UT_RUN(cf_throw_after_binding_keeps_debt_for_original_owner_cleanup);
	UT_DONE();
	return ut_failed_count != 0;
}
