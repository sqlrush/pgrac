/* Actual startup-to-checkpointer sync request and completion boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <signal.h>
#include <unistd.h>
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
bool cluster_enabled = true, cluster_shared_config = true;
BackendType MyBackendType = B_STARTUP;
AuxProcType MyAuxProcType = StartupProcess;
int MyProcPid = 101;
volatile sig_atomic_t ShutdownRequestPending;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static bool recovering = true, sync_error;
static unsigned sync_calls, broadcasts, sleeps, fault;
static sigjmp_buf sync_failure;
static sigjmp_buf startup_exit;
static volatile sig_atomic_t shutdown_requested, got_SIGHUP;
static bool in_restore_command;
volatile sig_atomic_t ProcSignalBarrierPending, LogMemoryContextPending;
static bool cv_waiting;
static int exit_code;
static int test_kill(pid_t pid, int sig);
static void test_proc_exit(int code) pg_attribute_noreturn();
static void
StartupRereadConfig(void)
{}
static void
WakeupRecovery(void)
{}
static bool
PostmasterIsAlive(void)
{
	return true;
}
static void
ProcessProcSignalBarrier(void)
{}
static void
ProcessLogMemoryContextInterrupt(void)
{}
static void
test_write_stderr_signal_safe(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

#define proc_exit test_proc_exit
#define exit test_proc_exit
#define _exit test_proc_exit
#define write_stderr_signal_safe test_write_stderr_signal_safe
#include "test_cluster_startup_interrupt.inc"
#undef write_stderr_signal_safe
#undef _exit
#undef exit
#undef proc_exit

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

bool
RecoveryInProgress(void)
{
	return recovering;
}
void
ProcessSyncRequests(void)
{
	sync_calls++;
	if (sync_error)
		siglongjmp(sync_failure, 1);
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	(void)cv;
	cv_waiting = true;
}
bool
ConditionVariableCancelSleep(void)
{
	cv_waiting = false;
	return false;
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	(void)cv;
	broadcasts++;
}

#define kill test_kill
#include "test_cluster_startup_sync.inc"
#undef kill
static CheckpointerShmemStruct shared;

static void
test_proc_exit(int code)
{
	exit_code = code;
	UT_ASSERT(SpinLockFree(&shared.ckpt_lck));
	/* AuxiliaryProcKill cancels the wait during the original proc_exit. */
	ConditionVariableCancelSleep();
	siglongjmp(startup_exit, 1);
}

static int
test_kill(pid_t pid, int sig)
{
	UT_ASSERT_EQ(pid, 202);
	UT_ASSERT_EQ(sig, SIGINT);
	return fault == 1 ? -1 : 0;
}

bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 event)
{
	(void)cv;
	UT_ASSERT_EQ(timeout, 100);
	UT_ASSERT_EQ(event, WAIT_EVENT_CHECKPOINT_DONE);
	sleeps++;
	UT_ASSERT(sleeps < 4);
	if (fault == 4) {
		StartupProcShutdownHandler(SIGTERM);
		/* Bound the negative test if the requester ignores Startup's flag. */
		if (sleeps > 1)
			shared.checkpointer_pid++;
		return true;
	}
	if (fault == 2) {
		shared.checkpointer_pid++;
		return true;
	}
	if (fault == 3) {
		shared.startup_sync_request++;
		return true;
	}
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	MyProcPid = 202;
	if (sigsetjmp(sync_failure, 1) == 0)
		CheckpointerStartupSyncPoll();
	else
		CheckpointerStartupSyncFinish(false);
	/* Re-polling cannot execute a completed request twice. */
	CheckpointerStartupSyncPoll();
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	MyProcPid = 101;
	if (fault == 5)
		StartupProcShutdownHandler(SIGTERM);
	return false;
}

static void
fixture(void)
{
	memset(&shared, 0, sizeof(shared));
	SpinLockInit(&shared.ckpt_lck);
	shared.checkpointer_pid = 202;
	shared.ckpt_flags = CHECKPOINT_FORCE;
	shared.ckpt_started = 7;
	shared.ckpt_done = 6;
	shared.ckpt_failed = 2;
	CheckpointerShmem = &shared;
	startup_sync_active_request = 0;
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	MyProcPid = 101;
	IsUnderPostmaster = cluster_enabled = cluster_shared_config = recovering = true;
	ShutdownRequestPending = false;
	shutdown_requested = got_SIGHUP = false;
	in_restore_command = false;
	ProcSignalBarrierPending = LogMemoryContextPending = false;
	cv_waiting = false;
	exit_code = 0;
	sync_error = false;
	sync_calls = broadcasts = sleeps = fault = 0;
}

UT_TEST(sync_completion_is_not_checkpoint_completion)
{
	fixture();
	UT_ASSERT(RequestStartupSync());
	UT_ASSERT_EQ(sync_calls, 1);
	UT_ASSERT_EQ(broadcasts, 1);
	UT_ASSERT_EQ(shared.startup_sync_request, 1);
	UT_ASSERT_EQ(shared.startup_sync_state, STARTUP_SYNC_IDLE);
	UT_ASSERT_EQ(shared.ckpt_flags, CHECKPOINT_FORCE);
	UT_ASSERT_EQ(shared.ckpt_started, 7);
	UT_ASSERT_EQ(shared.ckpt_done, 6);
	UT_ASSERT_EQ(shared.ckpt_failed, 2);
	UT_ASSERT(RequestStartupSync());
	UT_ASSERT_EQ(shared.startup_sync_request, 2);
	UT_ASSERT_EQ(sync_calls, 2);
}

UT_TEST(sync_error_has_no_success_receipt)
{
	fixture();
	sync_error = true;
	UT_ASSERT(!RequestStartupSync());
	UT_ASSERT_EQ(sync_calls, 1);
	UT_ASSERT_EQ(broadcasts, 1);
	UT_ASSERT_EQ(shared.ckpt_done, 6);
}

UT_TEST(wrong_role_phase_or_busy_request_cannot_publish)
{
	for (unsigned f = 0; f < 9; f++) {
		fixture();
		switch (f) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			MyAuxProcType = CheckpointerProcess;
			break;
		case 2:
			IsUnderPostmaster = false;
			break;
		case 3:
			cluster_shared_config = false;
			break;
		case 4:
			recovering = false;
			break;
		case 5:
			ShutdownRequestPending = true;
			break;
		case 6:
			shared.checkpointer_pid = 0;
			break;
		case 7:
			shared.startup_sync_state = STARTUP_SYNC_REQUESTED;
			break;
		case 8:
			shared.startup_sync_request = UINT64_MAX;
			break;
		}
		UT_ASSERT(!RequestStartupSync());
		UT_ASSERT_EQ(sync_calls | sleeps, 0);
	}
}

UT_TEST(lost_server_or_changed_request_cannot_acknowledge)
{
	for (unsigned f = 1; f <= 3; f++) {
		fixture();
		fault = f;
		UT_ASSERT(!RequestStartupSync());
		UT_ASSERT_EQ(sync_calls, 0);
		UT_ASSERT_EQ(shared.ckpt_done, 6);
	}
}

UT_TEST(unrelated_error_cannot_complete_an_unstarted_sync)
{
	fixture();
	shared.startup_sync_state = STARTUP_SYNC_REQUESTED;
	shared.startup_sync_request = 17;
	shared.startup_sync_checkpointer = 202;
	MyBackendType = B_CHECKPOINTER;
	MyAuxProcType = CheckpointerProcess;
	MyProcPid = 202;
	CheckpointerStartupSyncFinish(false);
	UT_ASSERT_EQ(shared.startup_sync_state, STARTUP_SYNC_REQUESTED);
	UT_ASSERT_EQ(broadcasts, 0);
}

UT_TEST(real_startup_shutdown_is_consumed_before_publish_wait_or_success)
{
	for (unsigned boundary = 0; boundary < 3; boundary++) {
		fixture();
		fault = boundary == 1 ? 4 : boundary == 2 ? 5 : 0;
		if (boundary == 0)
			StartupProcShutdownHandler(SIGTERM);
		if (sigsetjmp(startup_exit, 1) == 0) {
			(void)RequestStartupSync();
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(exit_code, 1);
		UT_ASSERT(shutdown_requested);
		UT_ASSERT(!ShutdownRequestPending);
		UT_ASSERT(!cv_waiting);
		UT_ASSERT_EQ(sync_calls, boundary == 2 ? 1 : 0);
		UT_ASSERT_EQ(sleeps, boundary == 0 ? 0 : 1);
		UT_ASSERT_EQ(shared.ckpt_done, 6);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(sync_completion_is_not_checkpoint_completion);
	UT_RUN(sync_error_has_no_success_receipt);
	UT_RUN(wrong_role_phase_or_busy_request_cannot_publish);
	UT_RUN(lost_server_or_changed_request_cannot_acknowledge);
	UT_RUN(unrelated_error_cannot_complete_an_unstarted_sync);
	UT_RUN(real_startup_shutdown_is_consumed_before_publish_wait_or_success);
	UT_DONE();
	return ut_failed_count != 0;
}
