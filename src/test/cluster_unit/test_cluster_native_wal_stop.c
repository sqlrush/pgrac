/* Author: SqlRush <sqlrush@gmail.com> */
/* Real xlog observer. Own control memory, process role and native locks are
 * boundary fixtures; this does not claim a disk checkpoint or native restart. */
#include "postgres.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include <sys/wait.h>
#include <unistd.h>

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ControlFileData own_control;
static ControlFileData *ControlFile = &own_control;
static PGPROC process;
PGPROC *MyProc = &process;
bool IsUnderPostmaster;
AuxProcType MyAuxProcType;
TimestampTz PgStartTime;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool native_mode, recovering, requested, held;
static unsigned lock_reads;

bool
cluster_normal_stop_native_wal_mode(void)
{
	return native_mode;
}
bool
cluster_normal_stop_requested(void)
{
	return requested;
}
bool
RecoveryInProgress(void)
{
	return recovering;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == ControlFileLock);
	return held;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == ControlFileLock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT(!held);
	held = true;
	lock_reads++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == ControlFileLock);
	UT_ASSERT(held);
	held = false;
}

#include "test_cluster_native_wal_stop.inc"

/* Exact two PostmasterMain slices, with the intervening native process work
 * outside this small test. fork/pipe/waitpid are real: no manually injected
 * boot timestamp is substituted for the early child inheritance. */
bool cluster_enabled;
static TimestampTz fixture_now;
static unsigned death_watch_calls, early_child_calls;
typedef struct NativeInheritedObservation {
	TimestampTz inherited;
	int64 observed;
	bool ready;
} NativeInheritedObservation;
static NativeInheritedObservation early_child;

TimestampTz
GetCurrentTimestamp(void)
{
	return ++fixture_now;
}

static NativeInheritedObservation
observe_real_child(AuxProcType role)
{
	NativeInheritedObservation result = { 0 };
	int fds[2], status = 0;
	pid_t pid;
	UT_ASSERT_EQ(pipe(fds), 0);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid == 0) {
		ssize_t sent;
		close(fds[0]);
		MyAuxProcType = role;
		result.inherited = PgStartTime;
		result.ready = cluster_native_wal_shutdown_observe(false, &result.observed);
		sent = write(fds[1], &result, sizeof(result));
		close(fds[1]);
		_exit(sent == sizeof(result) ? 0 : 2);
	}
	close(fds[1]);
	UT_ASSERT_EQ(read(fds[0], &result, sizeof(result)), sizeof(result));
	close(fds[0]);
	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	return result;
}

static void
InitPostmasterDeathWatchHandle(void)
{
	death_watch_calls++;
}

static void
cluster_run_startup_sequence(void)
{
	if (cluster_enabled) {
		early_child_calls++;
		early_child = observe_real_child(LmonProcess);
	}
}

#include "test_cluster_native_postmaster_time.inc"

static void
reset_native(void)
{
	memset(&own_control, 0, sizeof(own_control));
	own_control.state = DB_IN_PRODUCTION;
	own_control.checkPoint = UINT64CONST(0x2000080);
	own_control.checkPointCopy.redo = UINT64CONST(0x2000028);
	own_control.checkPointCopy.ThisTimeLineID = 2;
	own_control.checkPointCopy.PrevTimeLineID = 1;
	ControlFile = &own_control;
	MyProc = &process;
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	PgStartTime = 555;
	native_mode = true;
	recovering = requested = held = false;
	lock_reads = 0;
}

UT_TEST(active_is_read_only_and_does_not_request_stop)
{
	int64 started = 99;
	ControlFileData before;
	reset_native();
	before = own_control;
	UT_ASSERT(cluster_native_wal_shutdown_observe(false, &started));
	UT_ASSERT_EQ(started, 555);
	UT_ASSERT_EQ(lock_reads, 1);
	UT_ASSERT(!held && !requested);
	UT_ASSERT(memcmp(&before, &own_control, sizeof(before)) == 0);
}

UT_TEST(stopped_requires_requested_and_actual_shutdown_control)
{
	for (int role = 0; role < 2; role++) {
		int64 started = 99;
		reset_native();
		MyAuxProcType = role == 0 ? CheckpointerProcess : LmonProcess;
		requested = true;
		UT_ASSERT(!cluster_native_wal_shutdown_observe(true, &started));
		UT_ASSERT_EQ(started, 0);
		own_control.state = DB_SHUTDOWNED;
		UT_ASSERT(cluster_native_wal_shutdown_observe(true, &started));
		UT_ASSERT_EQ(started, 555);
		UT_ASSERT(!held);
		requested = false;
		UT_ASSERT(!cluster_native_wal_shutdown_observe(true, &started));
		UT_ASSERT_EQ(started, 0);
	}
}

UT_TEST(wrong_actor_missing_control_and_recovery_never_observe)
{
	for (int bad = 0; bad < 8; bad++) {
		int64 started = 99;
		reset_native();
		switch (bad) {
		case 0:
			IsUnderPostmaster = false;
			break;
		case 1:
			MyAuxProcType = StartupProcess;
			break;
		case 2:
			MyAuxProcType = NotAnAuxProcess;
			break;
		case 3:
			MyProc = NULL;
			break;
		case 4:
			ControlFile = NULL;
			break;
		case 5:
			PgStartTime = 0;
			break;
		case 6:
			native_mode = false;
			break;
		case 7:
			recovering = true;
			break;
		}
		UT_ASSERT(!cluster_native_wal_shutdown_observe(false, &started));
		UT_ASSERT_EQ(started, 0);
		UT_ASSERT_EQ(lock_reads, 0);
	}
}

UT_TEST(invalid_checkpoint_or_timeline_is_not_clean_evidence)
{
	for (int bad = 0; bad < 7; bad++) {
		int64 started = 99;
		reset_native();
		requested = true;
		own_control.state = DB_SHUTDOWNED;
		switch (bad) {
		case 0:
			own_control.state = DB_SHUTDOWNED_IN_RECOVERY;
			break;
		case 1:
			own_control.checkPoint = InvalidXLogRecPtr;
			break;
		case 2:
			own_control.checkPointCopy.redo = InvalidXLogRecPtr;
			break;
		case 3:
			own_control.checkPointCopy.redo = own_control.checkPoint + 1;
			break;
		case 4:
			own_control.checkPointCopy.ThisTimeLineID = 0;
			break;
		case 5:
			own_control.checkPointCopy.PrevTimeLineID = 0;
			break;
		case 6:
			own_control.checkPointCopy.PrevTimeLineID = 3;
			break;
		}
		UT_ASSERT(!cluster_native_wal_shutdown_observe(true, &started));
		UT_ASSERT_EQ(started, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(no_recursive_control_lock_or_null_output)
{
	int64 started = 99;
	reset_native();
	held = true;
	UT_ASSERT(!cluster_native_wal_shutdown_observe(false, &started));
	UT_ASSERT_EQ(started, 0);
	UT_ASSERT(held);
	UT_ASSERT_EQ(lock_reads, 0);
	held = false;
	UT_ASSERT(!cluster_native_wal_shutdown_observe(false, NULL));
	UT_ASSERT_EQ(lock_reads, 0);
}

UT_TEST(real_early_lmon_and_late_checkpointer_inherit_one_boot_identity)
{
	NativeInheritedObservation late_child;
	reset_native();
	cluster_enabled = true;
	PgStartTime = 0;
	fixture_now = 100;
	death_watch_calls = early_child_calls = 0;
	memset(&early_child, 0, sizeof(early_child));
	actual_early_start();
	actual_late_start();
	late_child = observe_real_child(CheckpointerProcess);
	UT_ASSERT_EQ(death_watch_calls, 1);
	UT_ASSERT_EQ(early_child_calls, 1);
	UT_ASSERT(early_child.ready);
	UT_ASSERT(early_child.inherited > 0);
	UT_ASSERT_EQ(early_child.inherited, PgStartTime);
	UT_ASSERT_EQ(early_child.observed, late_child.observed);
	UT_ASSERT(late_child.ready);
	UT_ASSERT_EQ(fixture_now, 101);
}

UT_TEST(noncluster_start_keeps_original_late_assignment)
{
	reset_native();
	cluster_enabled = false;
	PgStartTime = 0;
	fixture_now = 100;
	death_watch_calls = early_child_calls = 0;
	actual_early_start();
	UT_ASSERT_EQ(PgStartTime, 0);
	UT_ASSERT_EQ(early_child_calls, 0);
	actual_late_start();
	UT_ASSERT_EQ(PgStartTime, 101);
	UT_ASSERT_EQ(fixture_now, 101);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(active_is_read_only_and_does_not_request_stop);
	UT_RUN(stopped_requires_requested_and_actual_shutdown_control);
	UT_RUN(wrong_actor_missing_control_and_recovery_never_observe);
	UT_RUN(invalid_checkpoint_or_timeline_is_not_clean_evidence);
	UT_RUN(no_recursive_control_lock_or_null_output);
	UT_RUN(real_early_lmon_and_late_checkpointer_inherit_one_boot_identity);
	UT_RUN(noncluster_start_keeps_original_late_assignment);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
