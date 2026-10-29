/* Actual original job, CF, pin and ROOT lifecycle; native/context/time are
 * explicit standalone leaves, not a native service-loop result.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#define PGRAC_WAL_INPUTS_REAL_CUT
#include "test_cluster_wal_inputs_job_pin.c"
#include "test_cluster_wal_inputs_job_cut_fixture.h"
#include "test_cluster_wal_inputs_job_begin_fixture.h"

static ClusterWalInputsV1 *
lifecycle_scope(void)
{
	ClusterWalInputsV1 *scope = NULL;
	uint8 uuid[16] = { 1 };
	begin_setup();
	lifecycle_cut_setup();
	for (unsigned i = 0; i < lengthof(begin_origin); ++i) {
		ClusterWalHistoryRecord *current = &begin_origin[i].current;
		ClusterControlRootIdentity *id = &current->snapshot.identity;
		id->system_identifier = 1234;
		id->storage_uuid[0] = 1;
		id->authority_uuid[0] = 2;
		id->thread_claim_created_at = 100;
		id->root_lineage_seq = 1;
		current->refs.claim_sha256[0] = i + 1;
		current->snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	}
	UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job, uuid, 1234, &scope),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	return scope;
}

UT_TEST(suspend_confirmed_then_resume_reproves_original_root_and_pin)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = NULL;
	uint64 generation = begin_job->pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_SUSPENDED);
	UT_ASSERT(scope->suspended && !scope->stale && scope->pin == NULL);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(usable == scope && begin_job->inputs == scope);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
	UT_ASSERT(!scope->suspended && !scope->stale);
	UT_ASSERT(begin_job->pin_generation > generation);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(begin_origins, 2);
	UT_ASSERT_EQ(begin_claims, 2);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
	begin_cleanup();
}

UT_TEST(suspend_pending_is_irreversible_and_never_repeats_first_budget)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
	unsigned waits;
	owner_ack_on_wait = false;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_WALR);
	UT_ASSERT(scope->stale && scope->suspended);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(usable == NULL);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(begin_roots, 3);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	begin_cleanup();
}

UT_TEST(suspend_is_idempotent_and_usable_resume_only_revalidates)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = NULL;
	unsigned waits;
	uint64 generation = begin_job->pin_generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(usable == scope);
	UT_ASSERT_EQ(begin_job->pin_generation, generation);
	UT_ASSERT_EQ(begin_roots, 4);
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	begin_cleanup();
}

UT_TEST(resume_pending_cf_retires_reacquired_pin_without_resurrection)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	begin_pending_phase = 4;
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(usable == NULL);
	UT_ASSERT(scope->stale);
	UT_ASSERT_EQ(begin_job->cf.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT_EQ(pin_native_refs, 2);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(begin_roots, 4);
	begin_cleanup();
}

UT_TEST(resume_root_change_keeps_original_business_error_and_never_publishes)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	begin_changed_phase = 4;
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(usable == NULL && scope->stale);
	UT_ASSERT_EQ(begin_job->original_result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(pin_native_refs, 2);
	begin_cleanup();
}

UT_TEST(resume_partial_acquire_and_error_keep_stable_original_debt)
{
	for (unsigned throwing = 0; throwing < 2; ++throwing) {
		ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
		volatile bool caught = false;
		ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		owner_ack_on_wait = false;
		if (throwing) pin_throw_at = pin_native_calls + 2;
		else pin_fail_at = pin_native_calls + 2;
		PG_TRY(); { result = cluster_wal_inputs_job_resume_v1(begin_job, &usable); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		throw_during_install = false;
		UT_ASSERT_EQ(caught, throwing != 0);
		if (!throwing) UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(usable == NULL && scope->stale);
		UT_ASSERT_EQ(begin_roots, 3);
		UT_ASSERT_EQ(pin_native_refs, 2);
		MemoryContextReset(work_context);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		begin_cleanup();
	}
}

UT_TEST(suspend_error_keeps_cursor_native_refs_and_unknown_verdict)
{
	ClusterWalInputsV1 *scope = lifecycle_scope();
	volatile bool caught = false;
	unsigned waits;
	owner_ack_on_wait = false;
	job_throw_on_wait = true;
	PG_TRY(); { (void)cluster_wal_inputs_job_suspend_v1(begin_job); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	job_throw_on_wait = false;
	UT_ASSERT(caught);
	UT_ASSERT(scope->stale && scope->suspended);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_RETIRING_WALR);
	UT_ASSERT_EQ(begin_job->original_result, UINT32_MAX);
	UT_ASSERT_EQ(begin_job->walr_next, 2);
	UT_ASSERT_EQ(pin_native_refs, 2);
	waits = owner_wait_calls;
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	MemoryContextReset(work_context);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	begin_cleanup();
}

UT_TEST(lifecycle_foreign_identity_and_unbuilt_jobs_cannot_change_state)
{
	for (unsigned variant = 0; variant < 6; ++variant) {
		ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
		ResourceOwner original = CurrentResourceOwner;
		if (variant == 0) ++MyProc->cluster_grd_generation;
		if (variant == 1) CurrentResourceOwner = (ResourceOwner)1;
		if (variant == 2) MyBackendType = B_BG_WORKER;
		if (variant == 3) ++begin_job->pin_generation;
		UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(variant == 4 ? NULL : variant == 5
			? (ClusterWalInputsJobV1 *)1 : begin_job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(variant == 4 ? NULL : variant == 5
			? (ClusterWalInputsJobV1 *)1 : begin_job, &usable), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(usable == NULL);
		UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
		UT_ASSERT_EQ(begin_roots, 3);
		if (variant == 0) --MyProc->cluster_grd_generation;
		if (variant == 1) CurrentResourceOwner = original;
		if (variant == 2) MyBackendType = B_CHECKPOINTER;
		if (variant == 3) --begin_job->pin_generation;
		begin_cleanup();
	}
	begin_setup();
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	begin_cleanup();
}

UT_TEST(lifecycle_preexisting_cf_is_never_consumed_and_disabled_resume_never_acquires)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = scope;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(usable == NULL && cluster_cf_held_is_usable(ShareLock));
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_USABLE);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_enabled = false;
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(usable == NULL);
	UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_SUSPENDED);
	UT_ASSERT_EQ(pin_native_refs, 0);
	UT_ASSERT_EQ(begin_roots, 3);
	cluster_enabled = true;
	begin_cleanup();
}

static ClusterWalCutV1 *
lifecycle_remote(ClusterWalInputsV1 *scope)
{
	ClusterWalWriterFlushV1 sample;
	UT_ASSERT_EQ(inputs_remote_sample(scope, 1, &sample), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(scope->remote[1] != NULL && scope->remote[1] == active_cut);
	return scope->remote[1];
}

UT_TEST(remote_descriptor_survives_work_reset_then_revocation_cancels_hint)
{
	ClusterWalInputsV1 *scope = lifecycle_scope();
	ClusterWalCutV1 *cut = lifecycle_remote(scope);
	ClusterWalWriterFlushV1 sample;
	cut_shared->completion_pending = true;
	MemoryContextReset(work_context);
	UT_ASSERT(GetMemoryChunkContext(cut) == begin_job->context);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
	owner_ack_on_wait = false;
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT(scope->remote[1] == cut && cut->stale && active_cut == NULL);
	UT_ASSERT(!cut_shared->completion_pending && cut_shared->owner_pid == 0);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &sample), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	UT_ASSERT_EQ(begin_roots, 3);
	begin_cleanup();
}

UT_TEST(remote_original_callbacks_in_either_order_revoke_without_freeing)
{
	for (unsigned order = 0; order < 2; ++order) {
		ClusterWalInputsV1 *scope = lifecycle_scope();
		ClusterWalCutV1 *cut = lifecycle_remote(scope);
		if (order == 0) lifecycle_cut_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
		if (order == 1) lifecycle_cut_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		MemoryContextReset(work_context);
		UT_ASSERT(scope->remote[1] == cut && cut->stale && scope->stale);
		UT_ASSERT(GetMemoryChunkContext(cut) == begin_job->context);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
		begin_cleanup();
	}
}

UT_TEST(remote_final_release_clears_each_descriptor_before_done)
{
	ClusterWalInputsV1 *scope = lifecycle_scope();
	ClusterWalCutV1 *cut = lifecycle_remote(scope);
	ClusterWalWriterFlushV1 sample;
	cut_shared->reply = cut->requested;
	cut_shared->reply.verb = CLUSTER_WAL_CUT_CONFIRMED;
	cut_shared->reply.native_config_generation = cut->requested.source.claim.max_config_generation;
	cut_shared->reply.reserved_end = cut_shared->reply.flushed_end = 200;
	cut_shared->result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	UT_ASSERT_EQ(inputs_remote_sample(scope, 1, &sample), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cut->complete && !cut->stale && active_cut == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(begin_job), WAL_INPUT_JOB_DONE);
	UT_ASSERT(scope->remote[1] == NULL && scope->pin == NULL && scope->stale);
	for (JobFixtureAllocation *entry = job_fixture_allocations; entry != NULL; entry = entry->next)
		UT_ASSERT(entry->allocation != cut);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&begin_job), WAL_INPUT_JOB_DONE);
	foundation_end();
	cluster_controlfile_shared_authority = false;
}

UT_TEST(remote_same_request_survives_confirmed_suspend_but_cannot_be_used_until_resume)
{
	ClusterWalInputsV1 *scope = lifecycle_scope(), *usable = NULL;
	ClusterWalCutV1 *cut = lifecycle_remote(scope);
	ClusterWalWriterFlushV1 sample;
	uint64 revision = cut->revision;
	UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(scope->remote[1] == cut && active_cut == cut && !cut->stale);
	UT_ASSERT_EQ(inputs_remote_sample(scope, 1, &sample), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(usable == scope && scope->remote[1] == cut);
	UT_ASSERT_EQ(cut->revision, revision);
	UT_ASSERT_EQ(inputs_remote_sample(scope, 1, &sample), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	begin_cleanup();
}

UT_TEST(remote_wrong_original_source_or_work_context_is_never_owned_cleanup)
{
	for (unsigned context = 0; context < 2; ++context) {
		ClusterWalInputsV1 *scope = lifecycle_scope();
		ClusterWalCutV1 *cut;
		if (context) {
			cut = NULL;
			UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&scope->items[1].source, &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			scope->remote[1] = cut;
		} else {
			cut = lifecycle_remote(scope);
			++cut->requested.source.claim.identity.root_lineage_seq;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(begin_job), WAL_INPUT_JOB_INVALID);
		UT_ASSERT(cut == active_cut && !cut->stale && cut_shared->owner_pid == MyProcPid);
		if (context) {
			scope->remote[1] = NULL;
			cluster_wal_cut_release_v1(&cut);
		} else --cut->requested.source.claim.identity.root_lineage_seq;
		begin_cleanup();
	}
}

UT_TEST(remote_public_prepare_keeps_original_full_job_proofs_before_and_after_sample)
{
	ClusterWalInputsV1 *scope = lifecycle_scope();
	ClusterWalCutV1 *cut;
	XLogRecPtr end = 999;
	UT_ASSERT_EQ(cluster_wal_inputs_prepare_live_v1(scope, 1, &end), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(end, 0);
	UT_ASSERT_EQ(begin_roots, 4);
	cut = scope->remote[1];
	if (cut != NULL) {
		cut_shared->reply = cut->requested;
		cut_shared->reply.verb = CLUSTER_WAL_CUT_CONFIRMED;
		cut_shared->reply.native_config_generation = cut->requested.source.claim.max_config_generation;
		cut_shared->reply.reserved_end = cut_shared->reply.flushed_end = 200;
		cut_shared->result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		UT_ASSERT_EQ(cluster_wal_inputs_prepare_live_v1(scope, 1, &end), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(end, 200);
		UT_ASSERT_EQ(begin_roots, 6);
	} else UT_ASSERT(cut != NULL);
	begin_cleanup();
}

UT_TEST(all_four_aux_consumers_suspend_and_resume_under_original_owner)
{
	for (unsigned consumer = 0; consumer < CLUSTER_WAL_INPUT_JOB_COUNT; ++consumer) {
		ClusterWalInputsV1 *scope = NULL, *usable = NULL;
		uint8 uuid[16] = { 1 };
		begin_setup();
		foundation_finish(&begin_job);
		MyBackendType = consumer == WAL_INPUT_JOB_PI_NOTICE || consumer == WAL_INPUT_JOB_PI_REBUILD
			? B_BG_WRITER : B_CHECKPOINTER;
		begin_job = foundation_open(consumer);
		UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job, uuid, 1234, &scope), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cluster_wal_inputs_job_suspend_v1(begin_job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(cluster_wal_inputs_job_resume_v1(begin_job, &usable), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(usable == scope && begin_job->owner == AuxProcessResourceOwner);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 2);
		UT_ASSERT_EQ(pin_native_refs, 2);
		UT_ASSERT_EQ(begin_roots, 4);
		begin_cleanup();
	}
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
	UT_PLAN(16);
	UT_RUN(suspend_confirmed_then_resume_reproves_original_root_and_pin);
	UT_RUN(suspend_pending_is_irreversible_and_never_repeats_first_budget);
	UT_RUN(suspend_is_idempotent_and_usable_resume_only_revalidates);
	UT_RUN(resume_pending_cf_retires_reacquired_pin_without_resurrection);
	UT_RUN(resume_root_change_keeps_original_business_error_and_never_publishes);
	UT_RUN(resume_partial_acquire_and_error_keep_stable_original_debt);
	UT_RUN(suspend_error_keeps_cursor_native_refs_and_unknown_verdict);
	UT_RUN(lifecycle_foreign_identity_and_unbuilt_jobs_cannot_change_state);
	UT_RUN(lifecycle_preexisting_cf_is_never_consumed_and_disabled_resume_never_acquires);
	UT_RUN(remote_descriptor_survives_work_reset_then_revocation_cancels_hint);
	UT_RUN(remote_original_callbacks_in_either_order_revoke_without_freeing);
	UT_RUN(remote_final_release_clears_each_descriptor_before_done);
	UT_RUN(remote_same_request_survives_confirmed_suspend_but_cannot_be_used_until_resume);
	UT_RUN(remote_wrong_original_source_or_work_context_is_never_owned_cleanup);
	UT_RUN(remote_public_prepare_keeps_original_full_job_proofs_before_and_after_sample);
	UT_RUN(all_four_aux_consumers_suspend_and_resume_under_original_owner);
	UT_DONE();
	return ut_failed_count != 0;
}
