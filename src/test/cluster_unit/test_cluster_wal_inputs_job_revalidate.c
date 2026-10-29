/* Original job/CF/ROOT revalidation and cleanup-only failed-wait bodies.
 * Native/context/transport/time and immutable I/O are explicit leaves.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"
#include "test_cluster_wal_inputs_job_begin_fixture.h"

static ClusterWalInputsV1 *
revalidate_scope(void)
{
	ClusterWalInputsV1 *usable = NULL;
	uint8 uuid[16] = { 1 };
	begin_setup();
	UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job, uuid, 1234, &usable),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(usable == begin_job->inputs);
	return usable;
}

UT_TEST(revalidate_original_root_without_reselecting_or_reacquiring_pin)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	ClusterWalReadPinV1 *pin = begin_job->pin;
	uint64 generation = begin_job->pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(begin_origins, 2);
	UT_ASSERT_EQ(begin_anchors, 2);
	UT_ASSERT_EQ(begin_claims, 2);
	UT_ASSERT(begin_job->inputs == scope && begin_job->pin == pin);
	UT_ASSERT_EQ(begin_job->pin_generation, generation);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
	UT_ASSERT_EQ(pin_native_refs, 2);
	begin_cleanup();
}

UT_TEST(wait_failed_revokes_without_new_root_or_repeated_confirmation_budget)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	unsigned waits;
	uint64 sequence;
	begin_failed_phase = 4;
	owner_ack_on_wait = false;
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(begin_origins, 2);
	UT_ASSERT_EQ(begin_claims, 2);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT(cluster_wal_inputs_at_v1(scope, 0) == NULL);
	UT_ASSERT(scope->stale);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(begin_roots, 3);
	begin_cleanup();
}

UT_TEST(revalidate_pending_cf_keeps_pin_until_ack_and_never_reenters_root)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	MemoryContext context = begin_job->context;
	unsigned waits;
	uint64 sequence;
	begin_pending_phase = 4;
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	sequence = cluster_cf_attempt_sequence();
	MemoryContextReset(work_context);
	UT_ASSERT(begin_job->context == context && begin_job->inputs == scope);
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 2);
	begin_cleanup();
}

UT_TEST(revalidate_root_errors_replace_old_success_and_survive_cleanup)
{
	for (unsigned variant = 0; variant < 3; ++variant) {
		ClusterWalInputsV1 *scope = revalidate_scope();
		ClusterControlRootResult expected = variant == 2
			? CLUSTER_CONTROL_ROOT_STALE_TOKEN : CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
		if (variant == 2)
			begin_changed_phase = 4;
		else {
			begin_failed_phase = 4;
			if (variant == 1)
				begin_pending_phase = 4;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), expected);
		UT_ASSERT_EQ(begin_job->original_result, expected);
		UT_ASSERT_EQ(begin_roots, 4);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
		UT_ASSERT_EQ(begin_job->original_result, expected);
		UT_ASSERT_EQ(begin_roots, 4);
		begin_cleanup();
	}
}

UT_TEST(revalidate_acquire_and_release_error_keep_unknown_verdict_and_original_debt)
{
	for (unsigned releasing = 0; releasing < 2; ++releasing) {
		ClusterWalInputsV1 *scope = revalidate_scope();
		volatile bool caught = false;
		unsigned waits;
		begin_pending_phase = 4;
		owner_ack_on_wait = false;
		throw_during_acquire = releasing == 0;
		job_throw_on_wait = releasing != 0;
		PG_TRY(); { (void)cluster_wal_inputs_job_revalidate_v1(begin_job); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		throw_during_acquire = job_throw_on_wait = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(begin_roots, releasing ? 4 : 3);
		UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
		UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
		UT_ASSERT((begin_job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		waits = owner_wait_calls;
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
		MemoryContextReset(work_context);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
		UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		begin_cleanup();
	}
}

UT_TEST(revalidate_only_real_cf_waits_keep_wait_polarity_with_or_without_debt)
{
	for (unsigned published = 0; published < 2; ++published) {
		ClusterWalInputsV1 *scope = revalidate_scope();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		g_timeout_detail.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
		fail_after_publication = published != 0;
		owner_ack_on_wait = false;
		UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		fail_after_publication = false;
		UT_ASSERT_EQ(begin_roots, 3);
		UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(begin_job->cf.state, published ? CF_JOB_RECEIPT_RETIRING : CF_JOB_RECEIPT_UNBOUND);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		UT_ASSERT_EQ(pin_native_refs, 2);
		begin_cleanup();
	}
}

UT_TEST(revalidate_unrelated_cf_refusal_is_not_a_classified_wait)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	g_timeout_detail.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(pin_native_refs, 2);
	begin_cleanup();
}

UT_TEST(revalidate_foreign_original_identities_cannot_start_or_retire_the_job)
{
	for (unsigned field = 0; field < 4; ++field) {
		uint64 sequence;
		ResourceOwner owner;
		(void)revalidate_scope();
		owner = CurrentResourceOwner;
		sequence = cluster_cf_attempt_sequence();
		if (field == 0) ++MyProc->cluster_grd_generation;
		if (field == 1) CurrentResourceOwner = (ResourceOwner)1;
		if (field == 2) MyBackendType = B_BG_WORKER;
		if (field == 3) ++begin_job->pin_generation;
		UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(begin_roots, 3);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
		UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
		UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		if (field == 0) --MyProc->cluster_grd_generation;
		if (field == 1) CurrentResourceOwner = owner;
		if (field == 2) MyBackendType = B_CHECKPOINTER;
		if (field == 3) --begin_job->pin_generation;
		begin_cleanup();
	}
}

UT_TEST(revalidate_never_consumes_a_preexisting_cf_duty)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	uint64 sequence;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT(cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	begin_cleanup();
}

UT_TEST(revalidate_failed_current_check_cannot_publish_old_inputs_or_read_root)
{
	for (unsigned field = 0; field < 3; ++field) {
		ClusterWalInputsV1 *scope = revalidate_scope();
		uint64 sequence = cluster_cf_attempt_sequence();
		if (field == 0) scope->stale = true;
		if (field == 1) scope->suspended = true;
		if (field == 2) cluster_enabled = false;
		UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(begin_roots, 3);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
		UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		UT_ASSERT_EQ(pin_native_refs, 2);
		cluster_enabled = true;
		begin_cleanup();
	}
}

UT_TEST(revalidate_rejects_null_foreign_and_unbuilt_jobs_without_dereferencing_them)
{
	begin_setup();
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1((ClusterWalInputsJobV1 *)1),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(NULL), WAL_INPUT_JOB_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1((ClusterWalInputsJobV1 *)1), WAL_INPUT_JOB_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_NEW);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(begin_job));
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_BUILDING);
	UT_ASSERT_EQ(begin_roots, 0);
	begin_cleanup();
}

UT_TEST(wait_failed_healthy_cleanup_can_finish_without_a_forced_pending_or_root_read)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	begin_failed_phase = 4;
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_DONE);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT(scope->stale && scope->suspended && scope->pin == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_DONE);
	UT_ASSERT_EQ(begin_roots, 3);
	begin_cleanup();
}

UT_TEST(revalidate_ack_completion_cannot_resurrect_a_pending_scope)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	unsigned waits;
	begin_pending_phase = 4;
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	job_ack(&begin_job->cf);
	cluster_cf_retirement_poll();
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	begin_cleanup();
}

UT_TEST(revalidate_all_four_original_aux_callers_keep_the_same_proof_and_pin)
{
	for (unsigned consumer = 0; consumer < CLUSTER_WAL_INPUT_JOB_COUNT; ++consumer) {
		ClusterWalInputsV1 *scope = NULL;
		uint8 uuid[16] = { 1 };
		begin_setup();
		foundation_finish(&begin_job);
		MyBackendType = consumer == WAL_INPUT_JOB_PI_NOTICE || consumer == WAL_INPUT_JOB_PI_REBUILD
			? B_BG_WRITER : B_CHECKPOINTER;
		begin_job = foundation_open(consumer);
		UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job, uuid, 1234, &scope),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cluster_wal_inputs_job_revalidate_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(begin_roots, 4);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT(begin_job->owner == AuxProcessResourceOwner);
		begin_cleanup();
	}
}

UT_TEST(revalidate_cf_identity_contradiction_stays_a_hard_refusal)
{
	ClusterWalInputsV1 *scope = revalidate_scope();
	volatile bool caught = false;
	begin_foreign_attempt = true;
	job_expect_fatal = true;
	PG_TRY(); { (void)cluster_wal_inputs_job_revalidate_v1(begin_job); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	job_expect_fatal = false;
	UT_ASSERT(caught && cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT(begin_job->state != WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
	UT_ASSERT_EQ(pin_native_refs, 2);
	/* Only the fixture continues after intercepting FATAL. Bind and confirm
	 * the actual original owner before allowing any subsequent cleanup. */
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &begin_job->cf), CF_JOB_CONFIRMED);
	begin_cleanup();
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
	UT_PLAN(15);
	UT_RUN(revalidate_original_root_without_reselecting_or_reacquiring_pin);
	UT_RUN(wait_failed_revokes_without_new_root_or_repeated_confirmation_budget);
	UT_RUN(revalidate_pending_cf_keeps_pin_until_ack_and_never_reenters_root);
	UT_RUN(revalidate_root_errors_replace_old_success_and_survive_cleanup);
	UT_RUN(revalidate_acquire_and_release_error_keep_unknown_verdict_and_original_debt);
	UT_RUN(revalidate_only_real_cf_waits_keep_wait_polarity_with_or_without_debt);
	UT_RUN(revalidate_unrelated_cf_refusal_is_not_a_classified_wait);
	UT_RUN(revalidate_foreign_original_identities_cannot_start_or_retire_the_job);
	UT_RUN(revalidate_never_consumes_a_preexisting_cf_duty);
	UT_RUN(revalidate_failed_current_check_cannot_publish_old_inputs_or_read_root);
	UT_RUN(revalidate_rejects_null_foreign_and_unbuilt_jobs_without_dereferencing_them);
	UT_RUN(wait_failed_healthy_cleanup_can_finish_without_a_forced_pending_or_root_read);
	UT_RUN(revalidate_ack_completion_cannot_resurrect_a_pending_scope);
	UT_RUN(revalidate_all_four_original_aux_callers_keep_the_same_proof_and_pin);
	UT_RUN(revalidate_cf_identity_contradiction_stays_a_hard_refusal);
	UT_DONE();
	return ut_failed_count != 0;
}
