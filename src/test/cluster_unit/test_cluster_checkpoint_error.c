/*-------------------------------------------------------------------------
 * test_cluster_checkpoint_error.c
 *   PGRAC: execute the production checkpointer error block after an online
 *   checkpoint yields to normal shutdown.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_checkpoint_error.c
 * NOTES
 *   Native resource cleanup and scheduling are explicit test boundaries.
 *   The original error block, EOR abort, sync finish and stop handler execute.
 *-------------------------------------------------------------------------
 */
int checkpointer_stop_fixture_main(void);
#define main checkpointer_stop_fixture_main
#include "test_cluster_checkpointer_stop.c"
#undef main

#include "postmaster/bgwriter.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "storage/sync.h"

static bool cf_owner_eor_authority;
static unsigned broadcasts, spin_depth;
static void
error_spin_acquire(volatile slock_t *lock)
{
	(void)lock;
	UT_ASSERT_EQ(spin_depth, 0);
	++spin_depth;
}
static void
error_spin_release(volatile slock_t *lock)
{
	(void)lock;
	UT_ASSERT_EQ(spin_depth, 1);
	--spin_depth;
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	UT_ASSERT(cv != NULL);
	UT_ASSERT_EQ(spin_depth, 0);
	++broadcasts;
}
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) error_spin_acquire(lock)
#define SpinLockRelease(lock) error_spin_release(lock)
int MyProcPid = 91;
#include "test_cluster_checkpoint_error_state.inc"

ErrorContextCallback *error_context_stack;
volatile uint32 InterruptHoldoffCount;
enum ErrorCleanupAction {
	ERROR_REPORT,
	ERROR_LWLOCKS,
	ERROR_CV,
	ERROR_WAIT,
	ERROR_UNLOCK_BUFFERS,
	ERROR_RESOURCES,
	ERROR_BUFFERS,
	ERROR_SMGR,
	ERROR_FILES,
	ERROR_HASH,
	ERROR_CONTEXT,
	ERROR_FLUSH,
	ERROR_RESET,
	ERROR_SLEEP,
	ERROR_CLOSE,
	ERROR_ACTION_COUNT
};
static unsigned error_calls[ERROR_ACTION_COUNT], next_action;
static uint64 sleep_us;
static MemoryContext error_context;

static void
error_cleanup(enum ErrorCleanupAction action)
{
	UT_ASSERT_EQ(action, next_action++);
	UT_ASSERT_EQ(InterruptHoldoffCount, action < ERROR_SLEEP ? 1 : 0);
	++error_calls[action];
}
static void
error_abort_cleanup(enum ErrorCleanupAction action, bool is_commit)
{
	UT_ASSERT(!is_commit);
	error_cleanup(action);
}
static void
error_context_cleanup(enum ErrorCleanupAction action, MemoryContext context)
{
	UT_ASSERT(context == error_context);
	error_cleanup(action);
}
static void
error_sleep(long delay)
{
	UT_ASSERT_EQ(delay, 1000000L);
	error_cleanup(ERROR_SLEEP);
	sleep_us += delay;
	fixture_now += delay;
}

/* PGRAC: these boundaries record native cleanup calls; no replacement
 * exception policy or shutdown state machine is implemented in the test. */
#define EmitErrorReport() error_cleanup(ERROR_REPORT)
#define LWLockReleaseAll() error_cleanup(ERROR_LWLOCKS)
#define ConditionVariableCancelSleep() error_cleanup(ERROR_CV)
#define pgstat_report_wait_end() error_cleanup(ERROR_WAIT)
#define UnlockBuffers() error_cleanup(ERROR_UNLOCK_BUFFERS)
#define ReleaseAuxProcessResources(commit) error_abort_cleanup(ERROR_RESOURCES, commit)
#define AtEOXact_Buffers(commit) error_abort_cleanup(ERROR_BUFFERS, commit)
#define AtEOXact_SMgr() error_cleanup(ERROR_SMGR)
#define AtEOXact_Files(commit) error_abort_cleanup(ERROR_FILES, commit)
#define AtEOXact_HashTables(commit) error_abort_cleanup(ERROR_HASH, commit)
#define MemoryContextSwitchTo(context) error_context_cleanup(ERROR_CONTEXT, context)
#define FlushErrorState() error_cleanup(ERROR_FLUSH)
#undef MemoryContextResetAndDeleteChildren
#define MemoryContextResetAndDeleteChildren(context) error_context_cleanup(ERROR_RESET, context)
#define pg_usleep(delay) error_sleep(delay)
#define smgrcloseall() error_cleanup(ERROR_CLOSE)
#include "test_cluster_checkpoint_error.inc"

/* The foreground requester executes its original counter protocol while the
 * checkpointer error block runs at the first wait boundary. */
bool IsPostmasterEnvironment = true;
static unsigned request_waits;
#undef ConditionVariableCancelSleep
bool
ConditionVariableCancelSleep(void)
{
	return false;
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	UT_ASSERT(cv == &CheckpointerShmem->start_cv || cv == &CheckpointerShmem->done_cv);
}
void
ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event)
{
	UT_ASSERT(cv == &CheckpointerShmem->start_cv);
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_CHECKPOINT_START);
	UT_ASSERT_EQ(++request_waits, 1);
	++CheckpointerShmem->ckpt_started;
	CheckpointerErrorFixture(error_context);
}
void
CreateCheckPoint(int flags)
{
	(void)flags;
	UT_ASSERT(false); /* The requester is a backend, not standalone. */
}
static int
request_signal(pid_t pid, int sig)
{
	UT_ASSERT_EQ(pid, MyProcPid);
	UT_ASSERT_EQ(sig, SIGINT);
	return 0;
}
#define kill(pid, sig) request_signal(pid, sig)
#undef elog
#define elog(level, ...) ereport(level, (errmsg(__VA_ARGS__)))
#define errhint(...) fixture_errdetail(__VA_ARGS__)
#include "test_cluster_checkpoint_request.inc"
#undef kill

static CheckpointerShmemStruct error_shmem;
static ErrorContextCallback prior_error_context;
static void
reset_error_fixture(void)
{
	reset_fixture();
	cluster_shared_config = true;
	memset(&error_shmem, 0, sizeof(error_shmem));
	CheckpointerShmem = &error_shmem;
	CheckpointerShmem->checkpointer_pid = MyProcPid;
	CheckpointerShmem->ckpt_started = 44;
	CheckpointerShmem->ckpt_done = 43;
	CheckpointerShmem->ckpt_failed = 7;
	CheckpointerShmem->startup_sync_request = 12;
	CheckpointerShmem->startup_sync_checkpointer = MyProcPid;
	CheckpointerShmem->startup_sync_state = STARTUP_SYNC_DONE;
	startup_sync_active_request = 0;
	cf_owner_eor_authority = false;
	ckpt_active = true;
	broadcasts = spin_depth = next_action = 0;
	sleep_us = 0;
	request_waits = 0;
	memset(error_calls, 0, sizeof(error_calls));
	InterruptHoldoffCount = 0;
	error_context_stack = &prior_error_context;
	error_context = (MemoryContext)&prior_error_context;
}

UT_TEST(online_error_cleans_once_and_preserves_shutdown_intent)
{
	reset_error_fixture();
	retiring_mode = ShareLock;
	retire_remaining = 3;
	cf_attempt.control_owner_id = 201;
	CheckpointerErrorFixture(error_context);
	for (unsigned i = 0; i < ERROR_ACTION_COUNT; ++i)
		UT_ASSERT_EQ(error_calls[i], 1);
	UT_ASSERT_EQ(next_action, ERROR_ACTION_COUNT);
	UT_ASSERT(error_context_stack == NULL);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT(ShutdownRequestPending && requested);
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(retiring_mode, ShareLock);
	UT_ASSERT_EQ(cf_attempt.control_owner_id, 201);
	UT_ASSERT_EQ(retirement_polls, 0);
	UT_ASSERT_EQ(prepare_calls, 0);
	UT_ASSERT_EQ(budget_begins, 0);
	UT_ASSERT_EQ(sleep_us, UINT64_C(1000000));
}

UT_TEST(online_error_does_not_abort_a_completed_startup_sync)
{
	reset_error_fixture();
	CheckpointerErrorFixture(error_context);
	UT_ASSERT(!cf_owner_eor_authority);
	UT_ASSERT_EQ(startup_sync_active_request, 0);
	UT_ASSERT_EQ(CheckpointerShmem->startup_sync_request, 12);
	UT_ASSERT_EQ(CheckpointerShmem->startup_sync_state, STARTUP_SYNC_DONE);
	UT_ASSERT_EQ(broadcasts, 1); /* Only the failed online checkpoint. */
	reset_error_fixture();
	cf_owner_eor_authority = true;
	startup_sync_active_request = 12;
	CheckpointerShmem->startup_sync_state = STARTUP_SYNC_RUNNING;
	CheckpointerErrorFixture(error_context);
	UT_ASSERT(!cf_owner_eor_authority);
	UT_ASSERT_EQ(CheckpointerShmem->startup_sync_state, STARTUP_SYNC_FAILED);
	UT_ASSERT_EQ(broadcasts, 2); /* Genuine startup failure is still reported. */
}

UT_TEST(online_error_reports_failure_instead_of_checkpoint_success)
{
	reset_error_fixture();
	CheckpointerErrorFixture(error_context);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_failed, 8);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_done, 44);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_started, 44);
	UT_ASSERT(!ckpt_active);
	UT_ASSERT_EQ(broadcasts, 1);
	/* An error outside an active checkpoint must not notify a false result. */
	reset_error_fixture();
	ckpt_active = false;
	CheckpointerErrorFixture(error_context);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_failed, 7);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_done, 43);
	UT_ASSERT_EQ(broadcasts, 0);
}

UT_TEST(online_error_returns_to_the_original_normal_stop_handler)
{
	reset_error_fixture();
	CheckpointerErrorFixture(error_context);
	UT_ASSERT_EQ(fixture_now, UINT64_C(2000000));
	UT_ASSERT(!ExitOnAnyError);
	run_handler();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT(ExitOnAnyError);
	UT_ASSERT_EQ(prepare_calls, 1);
	UT_ASSERT_EQ(checkpoint_calls, 1);
	UT_ASSERT_EQ(budget_begins, 1);
	UT_ASSERT_EQ(tail_begins, 1);
	UT_ASSERT_EQ(complete_calls, 1);
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
	assert_no_fallback();
}

UT_TEST(waiting_checkpoint_requester_receives_the_original_failure)
{
	reset_error_fixture();
	if (setjmp(exit_boundary) == 0)
		RequestCheckpoint(CHECKPOINT_WAIT);
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT_EQ(request_waits, 1);
	UT_ASSERT(strcmp(last_message, "checkpoint request failed") == 0);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_failed, 8);
	UT_ASSERT_EQ(CheckpointerShmem->ckpt_done, 45);
	UT_ASSERT_EQ(sleep_us, UINT64_C(1000000));
	UT_ASSERT(ShutdownRequestPending && requested);
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
	UT_ASSERT_EQ(prepare_calls, 0);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(online_error_cleans_once_and_preserves_shutdown_intent);
	UT_RUN(online_error_does_not_abort_a_completed_startup_sync);
	UT_RUN(online_error_reports_failure_instead_of_checkpoint_success);
	UT_RUN(online_error_returns_to_the_original_normal_stop_handler);
	UT_RUN(waiting_checkpoint_requester_receives_the_original_failure);
	UT_DONE();
	return ut_failed_count != 0;
}
