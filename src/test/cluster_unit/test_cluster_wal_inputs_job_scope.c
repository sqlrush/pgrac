/* Original job/pin/CF/owner bodies; scope storage is not ROOT admission.
 * Native, allocator/context, dispatch, clock and S1-S5 are fixture leaves.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"

UT_TEST(scope_is_original_job_storage_without_input_authority)
{
	ClusterWalInputsJobV1 *job;
	JobFixtureAllocation *entry;
	MemoryContext allocated = NULL;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	UT_ASSERT(job->inputs != NULL);
	for (entry = job_fixture_allocations; entry != NULL; entry = entry->next)
		if (entry->allocation == job->inputs)
			allocated = entry->context;
	UT_ASSERT(allocated == job->context);
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_BUILDING);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(job->inputs), 0);
	UT_ASSERT(!cluster_wal_inputs_job_scope_create_v1(job));
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_DONE);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(scope_and_pin_share_original_context_through_pending_cleanup)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalReadPinV1 **pin, *local;
	ClusterWalInputsV1 *scope;
	uint16 threads[] = { 1, 2 };
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	pin = cluster_wal_inputs_scope_job_pin_slot_v1(job);
	UT_ASSERT(pin != NULL);
	if (pin != NULL) {
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads, 2, job, pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT(*pin == job->pin);
	}
	MemoryContextReset(work_context);
	UT_ASSERT(job->inputs == scope);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(pin_native_refs, 2);
	scope->pin = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	scope->pin = job->pin;
	waits = owner_wait_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT(job->inputs == scope);
	pin_cleanup(&job, &local);
}

UT_TEST(scope_lost_or_foreign_locator_is_rejected_before_dereference)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsV1 *scope;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	job->inputs = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	job->inputs = (ClusterWalInputsV1 *)(uintptr_t)1;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	job->inputs = scope;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_DONE);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(scope_original_callback_retains_debt_without_waiting)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalReadPinV1 **pin, *local;
	ClusterWalInputsV1 *scope;
	uint16 thread = 1;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	pin = cluster_wal_inputs_scope_job_pin_slot_v1(job);
	UT_ASSERT(pin != NULL);
	if (pin != NULL)
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(&thread, 1, job, pin), CLUSTER_WAL_PIN_OK);
	waits = owner_wait_calls;
	inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pin_native_refs, 1);
	UT_ASSERT(job->inputs == scope);
	UT_ASSERT((job->flags & (WAL_INPUT_JOB_FLAG_REVOKED | WAL_INPUT_JOB_FLAG_ERROR)) != 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	pin_cleanup(&job, &local);
}

UT_TEST(scope_legacy_role_and_release_refuse_even_with_a_real_pin)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalReadPinV1 **pin, *local;
	ClusterWalInputsV1 *scope;
	uint16 thread = 1;
	volatile bool caught = false;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	pin = cluster_wal_inputs_scope_job_pin_slot_v1(job);
	UT_ASSERT(pin != NULL);
	if (pin != NULL)
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(&thread, 1, job, pin), CLUSTER_WAL_PIN_OK);
	/* Deliberately populated counters are not a ROOT proof. Reject them. */
	scope->thread_count = scope->count = 1;
	scope->threads[0] = 1;
	UT_ASSERT(inputs_owned(scope));
	UT_ASSERT(cluster_wal_read_pin_covers_v1(job->pin, 1));
	UT_ASSERT(!inputs_scope_role(scope) && !inputs_current(scope));
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(scope), 0);
	scope_expect_error = true;
	PG_TRY(); { cluster_wal_inputs_release_v1(&scope); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	scope_expect_error = false;
	UT_ASSERT(caught && scope == job->inputs);
	UT_ASSERT_EQ(pin_native_refs, 1);
	pin_cleanup(&job, &local);
}

UT_TEST(scope_foreign_purpose_or_responsibility_is_never_discarded)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsV1 *scope, saved;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	memcpy(&saved, scope, sizeof(saved));
	waits = owner_wait_calls;
	for (unsigned fault = 0; fault < 10; ++fault) {
		switch (fault) {
			case 0: scope->job = (ClusterWalInputsJobV1 *)(uintptr_t)1; break;
			case 1: scope->owner = (ResourceOwner)&job_fixture_foreign_owner; break;
			case 2: scope->pid++; break;
			case 3: scope->purpose = WAL_INPUTS_COLD_REPLAY; break;
			case 4: scope->tx.ir.held = true; break;
			case 5: scope->ctrc.source_plan = (ClusterRecoveryCtrcSourcePlanV1 *)(uintptr_t)1; break;
			case 6: scope->ctrc.namespace_fd = 3; break;
			case 7: scope->remote[0] = (ClusterWalCutV1 *)(uintptr_t)1; break;
			case 8: scope->thread_count = CLUSTER_WAL_RETENTION_MAX_THREADS + 1; break;
			case 9: scope->pin = (ClusterWalReadPinV1 *)(uintptr_t)1; break;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_INVALID);
		UT_ASSERT(job->inputs == scope);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		memcpy(scope, &saved, sizeof(saved));
	}
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(scope_two_original_classes_are_distinct_bounded_locators)
{
	for (unsigned role = 0; role < 2; ++role) {
		ClusterWalInputsJobV1 *a, *b;
		ClusterWalInputsV1 *ascope, *bscope;

		pin_begin();
		MyBackendType = role == 0 ? B_CHECKPOINTER : B_BG_WRITER;
		a = foundation_open(role == 0 ? WAL_INPUT_JOB_RETAINED_CUT : WAL_INPUT_JOB_PI_NOTICE);
		b = foundation_open(role == 0 ? WAL_INPUT_JOB_PI_BATCH : WAL_INPUT_JOB_PI_REBUILD);
		UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(a));
		UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(b));
		ascope = a->inputs; bscope = b->inputs;
		UT_ASSERT(ascope != bscope && inputs_job_scopes[0] == ascope && inputs_job_scopes[1] == bscope);
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
		a->inputs = bscope;
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		a->inputs = ascope;
		foundation_finish(&a);
		foundation_finish(&b);
		UT_ASSERT(inputs_job_scopes[0] == NULL && inputs_job_scopes[1] == NULL);
		foundation_end();
	}
}

UT_TEST(scope_exit_callback_preserves_allocation_and_revokes_use)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsV1 *scope;
	unsigned waits;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	waits = owner_wait_calls;
	proc_exit_inprogress = true;
	inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT(job->inputs == scope && scope->stale);
	MemoryContextReset(work_context);
	UT_ASSERT(job->inputs == scope && inputs_job_scopes[0] == scope);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_INVALID);
	proc_exit_inprogress = false;
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(scope_done_is_only_storage_and_finish_clears_its_witness)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsV1 *scope;
	uint64 generation;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	scope = job->inputs;
	generation = job->generation;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_DONE);
	UT_ASSERT(scope->stale && scope->suspended && scope->pin == NULL);
	UT_ASSERT(job->inputs == scope && inputs_job_scopes[0] == scope);
	scope->stale = false;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_INVALID);
	scope->stale = true;
	foundation_finish(&job);
	UT_ASSERT(inputs_job_scopes[0] == NULL);
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(job->generation > generation);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(scope_factory_refuses_foreign_identity_without_new_work)
{
	ClusterWalInputsJobV1 *job;
	unsigned contexts;
	uint64 requests;

	pin_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	contexts = job_contexts_created;
	requests = g_next_request_id;
	UT_ASSERT(!cluster_wal_inputs_job_scope_create_v1((ClusterWalInputsJobV1 *)(uintptr_t)1));
	UT_ASSERT(cluster_wal_inputs_scope_job_pin_slot_v1((ClusterWalInputsJobV1 *)(uintptr_t)1) == NULL);
	CurrentResourceOwner = (ResourceOwner)&job_fixture_foreign_owner;
	UT_ASSERT(!cluster_wal_inputs_job_scope_create_v1(job));
	CurrentResourceOwner = AuxProcessResourceOwner;
	job->owner_proc_generation++;
	UT_ASSERT(!cluster_wal_inputs_job_scope_create_v1(job));
	job->owner_proc_generation--;
	UT_ASSERT_EQ(job_contexts_created, contexts);
	UT_ASSERT_EQ(g_next_request_id, requests);
	UT_ASSERT(job->inputs == NULL && inputs_job_scopes[0] == NULL);
	UT_ASSERT(sizeof(ClusterWalInputsJobControlBudgetV1) <= 2048);
	printf("# scope=%zu control=%zu; no new ROOT/claim admission\n", sizeof(ClusterWalInputsV1), sizeof(ClusterWalInputsJobControlBudgetV1));
	foundation_finish(&job);
	foundation_end();
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
	UT_PLAN(10);
	UT_RUN(scope_is_original_job_storage_without_input_authority);
	UT_RUN(scope_and_pin_share_original_context_through_pending_cleanup);
	UT_RUN(scope_lost_or_foreign_locator_is_rejected_before_dereference);
	UT_RUN(scope_original_callback_retains_debt_without_waiting);
	UT_RUN(scope_legacy_role_and_release_refuse_even_with_a_real_pin);
	UT_RUN(scope_foreign_purpose_or_responsibility_is_never_discarded);
	UT_RUN(scope_two_original_classes_are_distinct_bounded_locators);
	UT_RUN(scope_exit_callback_preserves_allocation_and_revokes_use);
	UT_RUN(scope_done_is_only_storage_and_finish_clears_its_witness);
	UT_RUN(scope_factory_refuses_foreign_identity_without_new_work);
	UT_DONE();
	return ut_failed_count != 0;
}
