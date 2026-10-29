/*-------------------------------------------------------------------------
 * test_cluster_wal_inputs_job.c -- original Aux caller lifetime foundation.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Job C, CF, stable owner, registry and ACK receiver are production bodies.
 * Context allocation/reset, ResourceOwner dispatch, transport and clock are
 * explicit standalone boundaries; no native service-loop result is claimed.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

void *cf_fixture_alloczero(MemoryContext context, Size size);
int cf_job_retirement_fixture_main(void);
bool cf_fixture_errstart_cold(int elevel, const char *domain);
#define TopMemoryContext cf_fixture_top
#define MemoryContextAllocZero cf_fixture_alloczero
#define main cf_job_retirement_fixture_main
#define errstart_cold cf_fixture_errstart_cold
#include "test_cluster_cf_job_retirement.c"
#undef errstart_cold
#undef main
#undef MemoryContextAllocZero
#undef TopMemoryContext

#include "cluster_wal_inputs_job_private.h"
#include "test_cluster_wal_inputs_job_callback.inc"

StaticAssertDecl(sizeof(ResourceReleaseCallbackItem) <= sizeof(ClusterWalInputsJobCallbackBudgetV1),
				 "original ResourceOwner callback item exceeds the job budget envelope");

typedef struct JobFixtureAllocation {
	MemoryContext context;
	void *allocation;
	struct JobFixtureAllocation *next;
} JobFixtureAllocation;

static MemoryContextData job_fixture_top = { .type = T_AllocSetContext };
MemoryContext TopMemoryContext = &job_fixture_top;
MemoryContext CurrentMemoryContext = &job_fixture_top;
MemoryContext ErrorContext, TopTransactionContext, CurTransactionContext;
ResourceOwner CurrentResourceOwner, AuxProcessResourceOwner;
static int job_fixture_aux_owner, job_fixture_foreign_owner;
static JobFixtureAllocation *job_fixture_allocations;
static unsigned job_contexts_created, job_contexts_deleted;
static ResourceReleaseCallback job_resource_callback;
static void *job_resource_argument;
static bool job_expect_fatal;

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	if (job_expect_fatal && elevel == FATAL)
		pg_re_throw();
	abort();
	return false;
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min_size,
							  Size init_size, Size max_size)
{
	MemoryContext context = calloc(1, sizeof(MemoryContextData));

	(void)min_size;
	(void)init_size;
	(void)max_size;
	context->type = T_AllocSetContext;
	context->parent = parent;
	context->name = name;
	job_contexts_created++;
	return context;
}

MemoryContext
MemoryContextGetParent(MemoryContext context)
{
	return context->parent;
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *result = calloc(1, size);
	JobFixtureAllocation *record;

	/* Existing stable owners use the original process-lifetime allocator and
	 * pfree. Only the new job contexts need reset tracking in this fixture. */
	if (context == TopMemoryContext)
		return result;
	record = malloc(sizeof(*record));
	record->context = context;
	record->allocation = result;
	record->next = job_fixture_allocations;
	job_fixture_allocations = record;
	return result;
}

void
MemoryContextReset(MemoryContext context)
{
	JobFixtureAllocation **next = &job_fixture_allocations;

	while (*next != NULL) {
		JobFixtureAllocation *record = *next;

		if (record->context != context) {
			next = &record->next;
			continue;
		}
		*next = record->next;
		free(record->allocation);
		free(record);
	}
}

void
MemoryContextDelete(MemoryContext context)
{
	UT_ASSERT(context != CurrentMemoryContext && context != TopMemoryContext);
	MemoryContextReset(context);
	job_contexts_deleted++;
	free(context);
}

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	UT_ASSERT(job_resource_callback == NULL);
	job_resource_callback = callback;
	job_resource_argument = arg;
}

/* Compile the complete, unchanged production implementation in this unit.
 * Internal state is visible only here for corruption and overflow cases. */
#include "cluster_wal_inputs_job.c"

static MemoryContext caller_context, work_context;

static void
foundation_begin(void)
{
	job_begin();
	CurrentResourceOwner = AuxProcessResourceOwner = (ResourceOwner)&job_fixture_aux_owner;
	MyProc->cluster_grd_generation = 1;
	caller_context = AllocSetContextCreate(TopMemoryContext, "original Aux caller",
										 ALLOCSET_DEFAULT_SIZES);
	work_context = AllocSetContextCreate(caller_context, "resettable work",
										ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext = work_context;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
}

static void
foundation_end(void)
{
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
	CurrentMemoryContext = TopMemoryContext;
	MemoryContextDelete(work_context);
	MemoryContextDelete(caller_context);
	UT_ASSERT(job_fixture_allocations == NULL);
	job_end();
}

static ClusterWalInputsJobV1 *
foundation_open(ClusterWalInputsConsumerV1 consumer)
{
	ClusterWalInputsJobV1 *job = NULL;

	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(consumer, caller_context, &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(job != NULL);
	return job;
}

static void
foundation_finish(ClusterWalInputsJobV1 **job)
{
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(*job), WAL_INPUT_JOB_DONE);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(job), WAL_INPUT_JOB_DONE);
	UT_ASSERT(*job == NULL);
}

UT_TEST(job_layout_and_original_context_are_bounded)
{
	ClusterWalInputsJobV1 *job;
	unsigned created;

	foundation_begin();
	created = job_contexts_created;
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(sizeof(*job), 272);
	UT_ASSERT_EQ(sizeof(job->cf), 184);
	UT_ASSERT(sizeof(ClusterWalInputsJobControlBudgetV1) <= 2048);
	printf("# job=%zu receipt=%zu slot=%zu process=%zu callback=%zu control=%zu\n",
		sizeof(*job), sizeof(job->cf), sizeof(ClusterWalInputsJobSlotV1),
		sizeof(ClusterWalInputsJobProcessV1), sizeof(ResourceReleaseCallbackItem),
		sizeof(ClusterWalInputsJobControlBudgetV1));
	UT_ASSERT_EQ(job_contexts_created, created + 1);
	UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == job->context);
	UT_ASSERT(MemoryContextGetParent(job->context) == caller_context);
	UT_ASSERT(CurrentMemoryContext == work_context);
	UT_ASSERT(job->owner == AuxProcessResourceOwner);
	UT_ASSERT_EQ(job->owner_pid, MyProcPid);
	UT_ASSERT_EQ(job->owner_procno, MyProc->pgprocno);
	UT_ASSERT_EQ(job->owner_proc_generation, 1);
	UT_ASSERT_EQ(job->original_result, UINT32_MAX);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_only_accepts_the_original_aux_role_and_owner)
{
	ClusterWalInputsJobV1 *job = NULL;
	BackendType roles[] = { B_BACKEND, B_BG_WORKER, B_STARTUP, B_LMON, B_LMS, B_BG_WRITER };

	foundation_begin();
	for (unsigned i = 0; i < lengthof(roles); ++i) {
		MyBackendType = roles[i];
		UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
			caller_context, &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(job == NULL);
	}
	MyBackendType = B_CHECKPOINTER;
	CurrentResourceOwner = (ResourceOwner)&job_fixture_foreign_owner;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	CurrentResourceOwner = AuxProcessResourceOwner;
	CritSectionCount = 1;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	CritSectionCount = 0;
	proc_exit_inprogress = true;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	proc_exit_inprogress = false;
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_rejects_transient_or_transaction_contexts)
{
	ClusterWalInputsJobV1 *job = NULL;
	MemoryContext bad[] = { NULL, TopMemoryContext, NULL, NULL, NULL };

	foundation_begin();
	bad[2] = work_context;
	bad[3] = ErrorContext = caller_context;
	bad[4] = TopTransactionContext = CurTransactionContext = caller_context;
	for (unsigned i = 0; i < lengthof(bad); ++i)
		UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
			bad[i], &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(job == NULL);
	ErrorContext = TopTransactionContext = CurTransactionContext = NULL;
	foundation_end();
}

UT_TEST(job_keeps_one_slot_and_does_not_rebind_its_parent)
{
	ClusterWalInputsJobV1 *job, *again = NULL;
	MemoryContext other;
	unsigned created;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	created = job_contexts_created;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &again), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(again == job);
	UT_ASSERT_EQ(job_contexts_created, created);
	other = AllocSetContextCreate(TopMemoryContext, "foreign caller", ALLOCSET_DEFAULT_SIZES);
	again = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		other, &again), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(again == NULL);
	MemoryContextDelete(other);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_rejects_changed_pid_procno_generation_and_owner)
{
	ClusterWalInputsJobV1 *job;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	for (unsigned i = 0; i < 6; ++i) {
		int32 pid = MyProc->pid;
		int procno = MyProc->pgprocno;
		ResourceOwner owner = CurrentResourceOwner;

		switch (i) {
		case 0: MyProcPid++; break;
		case 1: MyProc->pid++; break;
		case 2: MyProc->pgprocno++; break;
		case 3: MyProc->cluster_grd_generation = 2; break;
		case 4: CurrentResourceOwner = (ResourceOwner)&job_fixture_foreign_owner; break;
		case 5: MyBackendType = B_BG_WORKER; break;
		}
		UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == NULL);
		UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		MyProcPid = MyProc->pid = pid;
		MyProc->pgprocno = procno;
		MyProc->cluster_grd_generation = 1;
		CurrentResourceOwner = owner;
		MyBackendType = B_CHECKPOINTER;
	}
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_rejects_corrupt_headers_and_unsupported_input_debts)
{
	ClusterWalInputsJobV1 *job, saved;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	saved = *job;
	for (unsigned i = 0; i < 10; ++i) {
		switch (i) {
		case 0: job->generation++; break;
		case 1: job->consumer = WAL_INPUT_JOB_PI_NOTICE; break;
		case 2: job->state = UINT32_MAX; break;
		case 3: job->flags = 0x8000; break;
		case 4: job->reserved[0] = 1; break;
		case 5: job->cf.job_generation++; break;
		case 6: job->pin = (ClusterWalReadPinV1 *)1; break;
		case 7: job->inputs = (ClusterWalInputsV1 *)1; break;
		case 8: job->pin_generation = 1; break;
		case 9: job->walr_next = 1; break;
		}
		UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == NULL);
		UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		*job = saved;
	}
	job->context->parent = work_context;
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_INVALID);
	job->context->parent = caller_context;
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_cf_pending_is_cleanup_only_until_original_ack)
{
	ClusterWalInputsJobV1 *job, *again = NULL;
	unsigned waits, created;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
	waits = owner_wait_calls;
	created = job_contexts_created;
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &again), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(again == job);
	UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == NULL);
	UT_ASSERT_EQ(job_contexts_created, created);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	cluster_wal_inputs_jobs_service_poll_v1();
	UT_ASSERT_EQ(owner_wait_calls, waits);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	job_ack(&job->cf);
	cluster_wal_inputs_jobs_service_poll_v1();
	UT_ASSERT_EQ(job->state, WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_DONE);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_DONE);
	UT_ASSERT(job == NULL);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	foundation_end();
}

UT_TEST(job_callback_preserves_its_context_across_work_reset)
{
	ClusterWalInputsJobV1 *job;
	MemoryContext context;
	void *header;
	unsigned waits;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	context = cluster_wal_inputs_job_context_v1(job);
	header = MemoryContextAllocZero(context, 32);
	memset(header, 0x5a, 32);
	(void)MemoryContextAllocZero(work_context, 128);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &job->cf), CF_JOB_PENDING);
	waits = owner_wait_calls;
	UT_ASSERT(job_resource_callback != NULL);
	job_resource_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, job_resource_argument);
	UT_ASSERT_EQ(job->flags, 0);
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, job_resource_argument);
	MemoryContextReset(work_context);
	UT_ASSERT(job->context == context && context->parent == caller_context);
	UT_ASSERT(((uint8 *)header)[31] == 0x5a);
	UT_ASSERT((job->flags & (WAL_INPUT_JOB_FLAG_ERROR | WAL_INPUT_JOB_FLAG_REVOKED))
		== (WAL_INPUT_JOB_FLAG_ERROR | WAL_INPUT_JOB_FLAG_REVOKED));
	UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, waits);
	job_ack(&job->cf);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job), WAL_INPUT_JOB_DONE);
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_DONE);
	UT_ASSERT(job == NULL);
	foundation_end();
}

UT_TEST(job_census_checks_invalid_slots_before_pending)
{
	ClusterWalInputsJobV1 *pending, *other;

	foundation_begin();
	pending = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	other = foundation_open(WAL_INPUT_JOB_PI_BATCH);
	UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &pending->cf), CF_JOB_PENDING);
	UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(pending), WAL_INPUT_JOB_PENDING);
	other->reserved[7] = 1;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	other->reserved[7] = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_PENDING);
	job_ack(&pending->cf);
	foundation_finish(&pending);
	foundation_finish(&other);
	foundation_end();
}

UT_TEST(job_all_four_consumers_keep_their_original_roles)
{
	foundation_begin();
	for (unsigned i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i) {
		ClusterWalInputsJobV1 *job;

		MyBackendType = i == WAL_INPUT_JOB_RETAINED_CUT || i == WAL_INPUT_JOB_PI_BATCH
			? B_CHECKPOINTER : B_BG_WRITER;
		job = foundation_open((ClusterWalInputsConsumerV1)i);
		foundation_finish(&job);
	}
	MyBackendType = B_CHECKPOINTER;
	foundation_end();
}

UT_TEST(job_finish_clears_the_locator_before_context_deletion)
{
	ClusterWalInputsJobV1 *job, *stale, *next;
	uint64 generation;
	unsigned deleted;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	stale = job;
	generation = job->generation;
	deleted = job_contexts_deleted;
	UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job), WAL_INPUT_JOB_PENDING);
	foundation_finish(&job);
	UT_ASSERT_EQ(job_contexts_deleted, deleted + 1);
	UT_ASSERT(cluster_wal_inputs_job_context_v1(stale) == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(stale), WAL_INPUT_JOB_INVALID);
	next = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT(next->generation > generation);
	foundation_finish(&next);
	foundation_end();
}

UT_TEST(job_generation_never_wraps_or_reuses_an_overflow)
{
	ClusterWalInputsJobV1 *job = NULL;
	uint64 generation;
	unsigned created;

	foundation_begin();
	generation = inputs_jobs_process.generation;
	created = job_contexts_created;
	inputs_jobs_process.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
		caller_context, &job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(job == NULL);
	UT_ASSERT_EQ(job_contexts_created, created);
	inputs_jobs_process.generation = generation;
	foundation_end();
}

UT_TEST(job_state_and_context_cannot_forge_done)
{
	ClusterWalInputsJobV1 *job, saved;
	MemoryContext replacement;

	foundation_begin();
	job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	saved = *job;
	job->state = WAL_INPUT_JOB_STATE_DONE;
	job->flags = WAL_INPUT_JOB_FLAG_REVOKED;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	*job = saved;
	replacement = AllocSetContextCreate(caller_context, "different job", ALLOCSET_DEFAULT_SIZES);
	job->context = replacement;
	UT_ASSERT(cluster_wal_inputs_job_context_v1(job) == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
	*job = saved;
	MemoryContextDelete(replacement);
	foundation_finish(&job);
	foundation_end();
}

UT_TEST(job_empty_slot_corruption_cannot_be_hidden)
{
	ClusterWalInputsJobSlotV1 saved;
	ClusterWalInputsJobSlotV1 *slot;

	foundation_begin();
	slot = &inputs_jobs[WAL_INPUT_JOB_PI_BATCH];
	saved = *slot;
	for (unsigned i = 0; i < 6; ++i) {
		ClusterWalInputsJobV1 *job = NULL;

		switch (i) {
		case 0: slot->parent = caller_context; break;
		case 1: slot->context = work_context; break;
		case 2: slot->generation = 1; break;
		case 3: slot->state = WAL_INPUT_JOB_STATE_DONE; break;
		case 4: slot->flags = WAL_INPUT_JOB_FLAG_REVOKED; break;
		case 5: slot->reserved = 1; break;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(), WAL_INPUT_JOB_INVALID);
		UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
			caller_context, &job), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		*slot = saved;
		if (job != NULL) /* Keep a failing baseline's actual allocation owned. */
			foundation_finish(&job);
	}
	foundation_end();
}

static bool
foundation_callback_fails(bool isCommit)
{
	volatile bool caught = false;

	job_expect_fatal = true;
	PG_TRY();
	{
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, isCommit, true, job_resource_argument);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	job_expect_fatal = false;
	return caught;
}

UT_TEST(job_exit_callback_preserves_exact_identity_and_pending_debt)
{
	foundation_begin();
	for (unsigned i = 0; i < 2; ++i) {
		ClusterWalInputsJobV1 *done, *pending, *again = NULL;
		unsigned waits, deleted;

		done = foundation_open(WAL_INPUT_JOB_PI_BATCH);
		UT_ASSERT_EQ(cluster_wal_inputs_job_retire_v1(done), WAL_INPUT_JOB_DONE);
		pending = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
		UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock, NULL, &pending->cf), CF_JOB_PENDING);
		waits = owner_wait_calls;
		deleted = job_contexts_deleted;
		/* ipc.c sets this before shmem_exit dispatches the Aux owner release.
		 * This fixture calls the captured callback, not native proc_exit. */
		proc_exit_inprogress = true;
		UT_ASSERT(!foundation_callback_fails(i == 0));
		UT_ASSERT_EQ(done->state, WAL_INPUT_JOB_STATE_DONE);
		UT_ASSERT_EQ(pending->state, WAL_INPUT_JOB_STATE_RETIRING_CF);
		UT_ASSERT_EQ(pending->cf.state, CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT((pending->flags & WAL_INPUT_JOB_FLAG_REVOKED) != 0);
		UT_ASSERT_EQ((pending->flags & WAL_INPUT_JOB_FLAG_ERROR) != 0, i != 0);
		UT_ASSERT_EQ(owner_wait_calls, waits);
		UT_ASSERT_EQ(job_contexts_deleted, deleted);
		UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_RETAINED_CUT,
			caller_context, &again), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(again == NULL && cluster_wal_inputs_job_context_v1(pending) == NULL);
		UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(pending), WAL_INPUT_JOB_INVALID);
		MyProc->cluster_grd_generation++;
		UT_ASSERT(foundation_callback_fails(i == 0));
		MyProc->cluster_grd_generation--;
		UT_ASSERT_EQ(pending->cf.state, CF_JOB_RECEIPT_RETIRING);
		proc_exit_inprogress = false;
		job_ack(&pending->cf);
		foundation_finish(&pending);
		foundation_finish(&done);
	}
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
	UT_PLAN(15);
	UT_RUN(job_layout_and_original_context_are_bounded);
	UT_RUN(job_only_accepts_the_original_aux_role_and_owner);
	UT_RUN(job_rejects_transient_or_transaction_contexts);
	UT_RUN(job_keeps_one_slot_and_does_not_rebind_its_parent);
	UT_RUN(job_rejects_changed_pid_procno_generation_and_owner);
	UT_RUN(job_rejects_corrupt_headers_and_unsupported_input_debts);
	UT_RUN(job_cf_pending_is_cleanup_only_until_original_ack);
	UT_RUN(job_callback_preserves_its_context_across_work_reset);
	UT_RUN(job_census_checks_invalid_slots_before_pending);
	UT_RUN(job_all_four_consumers_keep_their_original_roles);
	UT_RUN(job_finish_clears_the_locator_before_context_deletion);
	UT_RUN(job_generation_never_wraps_or_reuses_an_overflow);
	UT_RUN(job_state_and_context_cannot_forge_done);
	UT_RUN(job_empty_slot_corruption_cannot_be_hidden);
	UT_RUN(job_exit_callback_preserves_exact_identity_and_pending_debt);
	UT_DONE();
	return ut_failed_count != 0;
}
