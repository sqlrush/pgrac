/* Original job/pin/CF/owner/ACK and ROOT-selection/claim pipeline bodies.
 * Native/context/transport/time and immutable-file I/O are explicit leaves.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"
#include "test_cluster_wal_inputs_job_begin_fixture.h"

UT_TEST(begin_rejects_invalid_arguments_without_starting_a_scope)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsV1 *usable = (ClusterWalInputsV1 *)1;
	uint8 uuid[16] = { 1 };
	uint64 before;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	before = cluster_cf_attempt_sequence();
	UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(job, NULL, 1234, &usable),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(usable == NULL && job->inputs == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(job, uuid, 0, &usable),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(job, uuid, 1234, NULL),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), before);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_NEW);
	UT_ASSERT_EQ(job->original_result, UINT32_MAX);
	foundation_finish(&job);
	foundation_end();
}

static ClusterControlRootResult
begin_call(ClusterWalInputsV1 **usable)
{
	uint8 uuid[16] = { 1 };
	return cluster_wal_inputs_job_begin_v1(begin_job, uuid, 1234, usable);
}

UT_TEST(begin_publishes_only_after_original_three_roots_pin_selection_and_claims)
{
	ClusterWalInputsV1 *usable = NULL;
	ClusterControlRootFileToken token;
	uint64 sequence;
	unsigned roots;

	begin_setup();
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(usable == begin_job->inputs);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(begin_origins, 2);
	UT_ASSERT_EQ(begin_anchors, 2);
	UT_ASSERT_EQ(begin_claims, 2);
	UT_ASSERT(strcmp(begin_order, "RROAOACCR") == 0);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(usable), 2);
	UT_ASSERT(cluster_wal_inputs_at_v1(usable, 0) != NULL);
	UT_ASSERT(cluster_wal_inputs_at_v1(usable, 1) != NULL);
	UT_ASSERT(cluster_wal_inputs_at_v1(usable, 2) == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(usable, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&token, &begin_token, sizeof(token)), 0);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(usable), 2);
	sequence = cluster_cf_attempt_sequence();
	roots = begin_roots;
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(usable == NULL);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(begin_roots, roots);
	begin_cleanup();
}

UT_TEST(begin_each_pending_cf_phase_revokes_without_later_reads_or_reentry)
{
	for (unsigned phase = 1; phase <= 3; ++phase) {
		ClusterWalInputsV1 *usable = (ClusterWalInputsV1 *)1;
		uint64 sequence;
		unsigned waits;
		begin_setup();
		begin_pending_phase = phase;
		UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_roots, phase);
		UT_ASSERT_EQ(begin_claims, phase == 3 ? 2 : 0);
		UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
		UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT(begin_job->inputs->stale);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
		UT_ASSERT_EQ(pin_native_refs, phase == 1 ? 0 : 2);
		waits = owner_wait_calls;
		sequence = cluster_cf_attempt_sequence();
		UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(begin_roots, phase);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
		MemoryContextReset(work_context);
		UT_ASSERT_EQ(pin_native_refs, phase == 1 ? 0 : 2);
		UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(begin_job), WAL_INPUT_JOB_PENDING);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		begin_cleanup();
	}
}

UT_TEST(begin_second_or_final_root_change_never_publishes_partial_sources)
{
	for (unsigned phase = 2; phase <= 3; ++phase) {
		ClusterWalInputsV1 *usable = (ClusterWalInputsV1 *)1;
		begin_setup();
		begin_changed_phase = phase;
		UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_roots, phase);
		UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
		UT_ASSERT_EQ(pin_native_refs, 2);
		begin_cleanup();
	}
}

UT_TEST(begin_claim_failure_and_pending_root_crc_keep_the_original_business_error)
{
	for (unsigned variant = 0; variant < 2; ++variant) {
		ClusterWalInputsV1 *usable = NULL;
		ClusterControlRootResult expected;
		begin_setup();
		if (variant == 0) {
			begin_claim_failure = 2;
			expected = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		} else {
			begin_pending_phase = begin_failed_phase = 2;
			expected = begin_read_result;
		}
		UT_ASSERT_EQ(begin_call(&usable), expected);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_roots, 2);
		UT_ASSERT_EQ(begin_job->original_result, expected);
		UT_ASSERT_EQ(begin_job->cf.state,
			variant == 0 ? CF_JOB_RECEIPT_CONFIRMED : CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT_EQ(pin_native_refs, 2);
		begin_cleanup();
	}
}

UT_TEST(begin_acquire_error_binds_original_cf_debt_before_work_reset)
{
	ClusterWalInputsV1 *volatile usable = (ClusterWalInputsV1 *)1;
	volatile bool caught = false;
	begin_setup();
	owner_ack_on_wait = false;
	throw_during_acquire = true;
	PG_TRY(); { (void)begin_call((ClusterWalInputsV1 **)&usable); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	throw_during_acquire = false;
	UT_ASSERT(caught && usable == NULL);
	UT_ASSERT_EQ(begin_roots, 0);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT((begin_job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
	UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
	MemoryContextReset(work_context);
	UT_ASSERT(cluster_cf_held(ShareLock) && !cluster_cf_held_is_usable(ShareLock));
	begin_cleanup();
}

UT_TEST(begin_first_release_error_keeps_original_receipt_and_unknown_result)
{
	ClusterWalInputsV1 *volatile usable = (ClusterWalInputsV1 *)1;
	volatile bool caught = false;
	begin_setup();
	job_throw_on_wait = true;
	PG_TRY(); { (void)begin_call((ClusterWalInputsV1 **)&usable); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && !job_throw_on_wait && usable == NULL);
	UT_ASSERT_EQ(begin_roots, 1);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
	UT_ASSERT((begin_job->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0);
	MemoryContextReset(work_context);
	begin_cleanup();
}

UT_TEST(begin_claim_error_retains_original_pin_and_never_restarts_cf)
{
	ClusterWalInputsV1 *volatile usable = (ClusterWalInputsV1 *)1;
	volatile bool caught = false;
	unsigned waits;
	begin_setup();
	begin_claim_throw = 1;
	PG_TRY(); { (void)begin_call((ClusterWalInputsV1 **)&usable); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && usable == NULL);
	UT_ASSERT_EQ(begin_roots, 2);
	UT_ASSERT_EQ(begin_claims, 1);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
	begin_cleanup();
}

UT_TEST(begin_partial_pin_and_pending_origin_cannot_publish_or_discard_native_refs)
{
	for (unsigned variant = 0; variant < 2; ++variant) {
		ClusterWalInputsV1 *usable = NULL;
		begin_setup();
		if (variant == 0)
			pin_fail_at = 2;
		else
			begin_origin_pending = true;
		UT_ASSERT_EQ(begin_call(&usable), variant == 0
			? CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN : CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT_EQ(begin_roots, variant == 0 ? 1 : 2);
		UT_ASSERT_EQ(begin_claims, 0);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
		begin_cleanup();
	}
}

UT_TEST(begin_legacy_mutation_and_remote_entry_refuse_the_original_job_scope)
{
	ClusterWalInputsV1 *usable = NULL;
	ClusterWalWriterFlushV1 remote;
	uint64 sequence;
	unsigned waits;
	begin_setup();
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	sequence = cluster_cf_attempt_sequence();
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_wait_failed_v1(usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(inputs_remote_sample(usable, 0, &remote), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT(usable->remote[0] == NULL && !usable->stale && !usable->suspended);
	begin_cleanup();
}

UT_TEST(begin_foreign_identity_and_existing_cf_never_start_a_new_scope)
{
	ClusterWalInputsV1 *usable = (ClusterWalInputsV1 *)1;
	uint64 sequence;
	begin_setup();
	++MyProc->cluster_grd_generation;
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(usable == NULL && begin_job->inputs == NULL);
	--MyProc->cluster_grd_generation;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(usable == NULL && begin_job->inputs == NULL);
	UT_ASSERT_EQ(begin_roots, 0);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	begin_cleanup();
}

UT_TEST(begin_all_four_original_aux_consumers_use_the_same_complete_proof)
{
	for (unsigned consumer = 0; consumer < CLUSTER_WAL_INPUT_JOB_COUNT; ++consumer) {
		ClusterWalInputsV1 *usable = NULL;
		begin_setup();
		foundation_finish(&begin_job);
		MyBackendType = (consumer & 1) ? B_BG_WRITER : B_CHECKPOINTER;
		begin_job = foundation_open((ClusterWalInputsConsumerV1)consumer);
		UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(begin_roots, 3);
		UT_ASSERT_EQ(begin_claims, 2);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(usable), 2);
		UT_ASSERT(begin_job->owner == AuxProcessResourceOwner);
		UT_ASSERT(CurrentResourceOwner == AuxProcessResourceOwner);
		begin_cleanup();
	}
}

UT_TEST(begin_only_actual_classified_cf_waits_remain_waits_with_or_without_debt)
{
	for (unsigned published = 0; published < 2; ++published) {
		ClusterWalInputsV1 *usable = NULL;
		begin_setup();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		g_timeout_detail.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
		fail_after_publication = published != 0;
		owner_ack_on_wait = false;
		UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		fail_after_publication = false;
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_roots, 0);
		UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(begin_job->cf.state, published ? CF_JOB_RECEIPT_RETIRING : CF_JOB_RECEIPT_UNBOUND);
		begin_cleanup();
	}
}

UT_TEST(begin_other_cf_refusals_never_become_classified_wait)
{
	ClusterWalInputsV1 *usable = NULL;
	begin_setup();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	g_timeout_detail.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(usable == NULL);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(begin_roots, 0);
	begin_cleanup();
}

UT_TEST(begin_keeps_every_original_history_claim_under_the_complete_pin_set)
{
	ClusterWalInputsV1 *usable = NULL;
	const ClusterWalInputV1 *history, *current;
	begin_setup();
	begin_origin[0].history.count = 1;
	begin_origin[0].history.records[0] = begin_origin[0].current;
	begin_origin[0].history.records[0].snapshot.identity.root_lineage_seq = 8;
	begin_origin[0].current.snapshot.identity.root_lineage_seq = 9;
	UT_ASSERT_EQ(begin_call(&usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(begin_origins, 2);
	UT_ASSERT_EQ(begin_anchors, 3);
	UT_ASSERT_EQ(begin_claims, 3);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(usable), 3);
	history = cluster_wal_inputs_at_v1(usable, 0);
	current = cluster_wal_inputs_at_v1(usable, 1);
	UT_ASSERT(history != NULL && current != NULL);
	if (history != NULL && current != NULL) {
		UT_ASSERT(!history->current && current->current);
		UT_ASSERT_EQ(history->source.claim.identity.root_lineage_seq, 8);
		UT_ASSERT_EQ(current->source.claim.identity.root_lineage_seq, 9);
	}
	UT_ASSERT_EQ(pin_native_refs, 2);
	begin_cleanup();
}

UT_TEST(begin_absent_and_over_capacity_roots_never_become_an_empty_success)
{
	for (unsigned variant = 0; variant < 2; ++variant) {
		ClusterWalInputsV1 *usable = (ClusterWalInputsV1 *)1;
		begin_setup();
		if (variant == 0)
			memset(begin_root.present, 0, sizeof(begin_root.present));
		else
			begin_origin[0].history.count = CLUSTER_WAL_INPUTS_MAX;
		UT_ASSERT_EQ(begin_call(&usable), variant == 0
			? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_roots, variant == 0 ? 1 : 2);
		UT_ASSERT_EQ(begin_claims, 0);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
		begin_cleanup();
	}
}

UT_TEST(begin_contradictory_cf_observation_keeps_original_hard_refusal)
{
	ClusterWalInputsV1 *volatile usable = (ClusterWalInputsV1 *)1;
	volatile bool caught = false;
	begin_setup();
	begin_foreign_attempt = true;
	job_expect_fatal = true;
	PG_TRY(); { (void)begin_call((ClusterWalInputsV1 **)&usable); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	job_expect_fatal = false;
	UT_ASSERT(caught && usable == NULL);
	UT_ASSERT(cluster_cf_held(ShareLock));
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT(begin_job->state != WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT_EQ(begin_roots, 1);
	/* Fixture-only recovery after intercepting the production FATAL: bind
	 * and confirm the actual original owner before releasing any context. */
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
	UT_PLAN(17);
	UT_RUN(begin_rejects_invalid_arguments_without_starting_a_scope);
	UT_RUN(begin_publishes_only_after_original_three_roots_pin_selection_and_claims);
	UT_RUN(begin_each_pending_cf_phase_revokes_without_later_reads_or_reentry);
	UT_RUN(begin_second_or_final_root_change_never_publishes_partial_sources);
	UT_RUN(begin_claim_failure_and_pending_root_crc_keep_the_original_business_error);
	UT_RUN(begin_acquire_error_binds_original_cf_debt_before_work_reset);
	UT_RUN(begin_first_release_error_keeps_original_receipt_and_unknown_result);
	UT_RUN(begin_claim_error_retains_original_pin_and_never_restarts_cf);
	UT_RUN(begin_partial_pin_and_pending_origin_cannot_publish_or_discard_native_refs);
	UT_RUN(begin_legacy_mutation_and_remote_entry_refuse_the_original_job_scope);
	UT_RUN(begin_foreign_identity_and_existing_cf_never_start_a_new_scope);
	UT_RUN(begin_all_four_original_aux_consumers_use_the_same_complete_proof);
	UT_RUN(begin_only_actual_classified_cf_waits_remain_waits_with_or_without_debt);
	UT_RUN(begin_other_cf_refusals_never_become_classified_wait);
	UT_RUN(begin_keeps_every_original_history_claim_under_the_complete_pin_set);
	UT_RUN(begin_absent_and_over_capacity_roots_never_become_an_empty_success);
	UT_RUN(begin_contradictory_cf_observation_keeps_original_hard_refusal);
	UT_DONE();
	return ut_failed_count != 0;
}
