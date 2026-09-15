/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual postmaster-private roster code; OS signals and existing atomic
 * product facts are boundary fixtures. No real process is stopped here. */
#include "postgres.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_undo_cleaner.h"
#include "miscadmin.h"
#include "lib/ilist.h"
#include <setjmp.h>
#include <sys/wait.h>
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", condition, file, line);
	abort();
}
#define NoShutdown 0
#define SmartShutdown 1
#define FastShutdown 2
#define ImmediateShutdown 3
enum {
	PM_INIT,
	PM_STARTUP,
	PM_RECOVERY,
	PM_HOT_STANDBY,
	PM_RUN,
	PM_STOP_BACKENDS,
	PM_WAIT_BACKENDS,
	PM_SHUTDOWN,
	PM_SHUTDOWN_2,
	PM_WAIT_DEAD_END,
	PM_NO_CHILDREN
};
#define EXIT_STATUS_0(st) ((st) == 0)
static int Shutdown, pmState;
static bool FatalError;
bool IsUnderPostmaster, IsPostmasterEnvironment;
bool cluster_enabled, cluster_lms_enabled, cluster_lmd_enabled;
int cluster_node_id, cluster_lms_workers;
static int configured_nodes = 4;
static pid_t LmonPID, LckPID, LmdPID, CssdPID, QvotecPID, LmsPID, SinvalBcastPID, CheckpointerPID;
static pid_t LmsWorkerPIDs[8], UndoCleanerPIDs[8];
static bool retention_hint, protocol_closed, clear_ok, request, request_available;
static unsigned request_calls, signal_count;
static pid_t signals[32];
static int expected_signal = SIGTERM;
static pid_t StartupPID, BgWriterPID, WalWriterPID, WalReceiverPID, AutoVacPID, PgArchPID;
static pid_t DiagPID, ClusterStatsPID, MrpPID, RfsPID;
static int StartupStatus;
static bool allow_final_walsender_wake;
#define STARTUP_SIGNALED 1
#define STARTUP_CRASHED 2
#define STARTUP_RUNNING 3
static void
SignalChildren(int signal)
{
	UT_ASSERT(signal == expected_signal || (allow_final_walsender_wake && signal == SIGUSR2));
}
static ClusterNormalStopFailure failure;
bool
cluster_semantic_normal_stop_needs_retention(void)
{
	return retention_hint;
}
int
cluster_conf_node_count(void)
{
	return configured_nodes;
}
bool
cluster_normal_stop_postmaster_request(void)
{
	request_calls++;
	if (!request_available || failure != 0)
		return false;
	request = true;
	return true;
}
bool
cluster_normal_stop_requested(void)
{
	return request;
}
ClusterNormalStopFailure
cluster_normal_stop_failure(void)
{
	return failure;
}
void
cluster_normal_stop_fail(ClusterNormalStopFailure f)
{
	if (failure == 0)
		failure = f;
}
bool
cluster_normal_stop_protocol_closed(void)
{
	return protocol_closed && failure == 0;
}
bool
cluster_normal_stop_qvotec_cleared(void)
{
	return clear_ok && failure == 0;
}
static void
signal_child(pid_t pid, int signal)
{
	UT_ASSERT_EQ(signal, expected_signal);
	UT_ASSERT(pid > 0);
	for (unsigned i = 0; i < signal_count; i++)
		UT_ASSERT(signals[i] != pid);
	if (signal_count >= 32)
		abort();
	signals[signal_count++] = pid;
}
#include "test_cluster_postmaster_stop.inc"
#include "test_cluster_postmaster_terminate.inc"

/* The FULL original state-machine body, with only native process/OS facts
 * replaced. Legacy disk/lock candidate calls fail if current was selected. */
#undef ereport
#define ereport(level, args) ((void)0)
#define BACKEND_TYPE_NORMAL 1
#define BACKEND_TYPE_WALSND 4
#define BACKEND_TYPE_ALL 15
#define PMQUIT_FOR_CRASH 1
static bool connsAllowed, restart_after_crash, remove_temp_files_after_crash;
static int child_count, frontend_cut_calls, legacy_shutdown_calls, exit_code;
static int legacy_candidate_calls, legacy_suppress_calls;
static time_t AbortStartTime;
static dlist_head BackendList;
static jmp_buf exit_jump;
static int
CountChildren(int mask)
{
	(void)mask;
	return child_count;
}
static void
ForgetUnstartedBackgroundWorkers(void)
{}
static void
SignalSomeChildren(int sig, int mask)
{
	(void)mask;
	UT_ASSERT_EQ(sig, SIGTERM);
}
static bool
cluster_registry_holds_admission(void)
{
	return true;
}
bool
cluster_clean_leave_phase1_full_stop_candidate(void)
{
	legacy_candidate_calls++;
	UT_ASSERT(!normal_stop_pm.selected);
	return false;
}
static bool
cluster_lmon_reconfig_suppressed(void)
{
	legacy_candidate_calls++;
	UT_ASSERT(!normal_stop_pm.selected);
	return true;
}
static void
cluster_lmon_suppress_reconfig(void)
{
	legacy_suppress_calls++;
	UT_ASSERT(!normal_stop_pm.selected);
}
static bool
LmsWorkersAllReaped(void)
{
	for (int i = 1; i < 8; i++)
		if (LmsWorkerPIDs[i] != 0)
			return false;
	return true;
}
bool
cluster_normal_stop_postmaster_frontends_gone(void)
{
	frontend_cut_calls++;
	return true;
}
static pid_t
StartCheckpointer(void)
{
	return 500;
}
static void
SetQuitSignalReason(int reason)
{
	(void)reason;
}
static void
ConfigurePostmasterWaitSet(bool accept)
{
	UT_ASSERT(!accept);
}
static void
ExitPostmaster(int code)
{
	exit_code = code;
	longjmp(exit_jump, 1);
}
static void
cluster_run_shutdown_sequence(void)
{
	legacy_shutdown_calls++;
}
static void
RemovePgTempFiles(void)
{
	abort();
}
static void
ResetBackgroundWorkerCrashTimes(void)
{
	abort();
}
static void
shmem_exit(int code)
{
	(void)code;
	abort();
}
void
LocalProcessControlFile(bool r)
{
	(void)r;
	abort();
}
static void
CreateSharedMemoryAndSemaphores(void)
{
	abort();
}
static void
cluster_run_startup_sequence(void)
{
	abort();
}
static pid_t
StartupDataBase(void)
{
	abort();
}
#include "test_cluster_postmaster_fsm.inc"

static bool pending_pm_shutdown_request, pending_pm_fast_shutdown_request;
static bool pending_pm_immediate_shutdown_request;
static unsigned status_writes;
#define LOCK_FILE_LINE_PM_STATUS 8
#define PM_STATUS_STOPPING "stopping"
#define PMQUIT_FOR_STOP 2
void
AddToDataDirLockFile(int line, const char *status)
{
	(void)line;
	(void)status;
	status_writes++;
}
#include "test_cluster_postmaster_request.inc"

static int child_crash_calls;
static void
HandleChildCrash(int pid, int status, const char *name)
{
	(void)pid;
	(void)status;
	(void)name;
	child_crash_calls++;
	FatalError = true;
	pmState = PM_WAIT_BACKENDS;
}
static void
actual_checkpoint_reaper(int pid, int exitstatus)
{
	/* The actual reaper hook and complete CP branch. Native waitpid input
	 * is supplied, not a hand-built normal-stop completion sequence. */
	for (int event = 0; event < 1; event++) {
#include "test_cluster_postmaster_reap.inc"
	}
}

static unsigned spawn_calls;
static bool qvotec_spawn_enabled;
#define CLUSTER_PHASE_RUNNING 4
static int
cluster_current_phase(void)
{
	return CLUSTER_PHASE_RUNNING;
}
static bool
cluster_mrp_should_start(void)
{
	return false;
}
static bool
cluster_rfs_should_start(void)
{
	return false;
}
#define SPAWN_FIXTURE(fn)                                                                          \
	static pid_t fn(void)                                                                          \
	{                                                                                              \
		return 800 + ++spawn_calls;                                                                \
	}
SPAWN_FIXTURE(StartLmon)
SPAWN_FIXTURE(StartLck)
SPAWN_FIXTURE(StartDiag)
SPAWN_FIXTURE(StartClusterStats)
SPAWN_FIXTURE(StartCssd)
SPAWN_FIXTURE(StartQvotec)
SPAWN_FIXTURE(StartLms)
SPAWN_FIXTURE(StartLmd)
SPAWN_FIXTURE(StartMrp)
SPAWN_FIXTURE(StartRfs)
SPAWN_FIXTURE(StartSinvalBcast)
static pid_t
StartLmsWorker(int worker)
{
	(void)worker;
	return 800 + ++spawn_calls;
}
static pid_t
StartUndoCleaner(int worker)
{
	(void)worker;
	return 800 + ++spawn_calls;
}
static void
actual_cluster_respawn_edges(void)
{
#include "test_cluster_postmaster_respawn.inc"
}

static void
setup(void)
{
	memset(&normal_stop_pm, 0, sizeof(normal_stop_pm));
	IsUnderPostmaster = false;
	IsPostmasterEnvironment = true;
	Shutdown = FastShutdown;
	pmState = PM_RUN;
	FatalError = false;
	cluster_enabled = cluster_lms_enabled = cluster_lmd_enabled = true;
	cluster_lms_workers = 8;
	cluster_node_id = 1;
	configured_nodes = 4;
	retention_hint = true;
	protocol_closed = clear_ok = request = false;
	request_available = true;
	failure = 0;
	request_calls = signal_count = 0;
	expected_signal = SIGTERM;
	allow_final_walsender_wake = false;
	child_crash_calls = 0;
	connsAllowed = true;
	child_count = 0;
	frontend_cut_calls = 0;
	legacy_shutdown_calls = 0;
	legacy_candidate_calls = legacy_suppress_calls = 0;
	exit_code = -1;
	restart_after_crash = true;
	remove_temp_files_after_crash = false;
	spawn_calls = 0;
	qvotec_spawn_enabled = true;
	dlist_init(&BackendList);
	AbortStartTime = 0;
	StartupPID = BgWriterPID = WalWriterPID = WalReceiverPID = AutoVacPID = PgArchPID = 0;
	DiagPID = ClusterStatsPID = MrpPID = RfsPID = 0;
	LmonPID = 100;
	LmsPID = 101;
	SinvalBcastPID = 109;
	LckPID = 110;
	LmdPID = 111;
	QvotecPID = 112;
	CssdPID = 113;
	CheckpointerPID = 140;
	LmsWorkerPIDs[0] = 0;
	for (int i = 1; i < 8; i++)
		LmsWorkerPIDs[i] = 101 + i;
	for (int i = 0; i < 8; i++)
		UndoCleanerPIDs[i] = 114 + i;
}
static void
forget_pid(pid_t pid)
{
	pid_t *single[] = { &LmonPID, &LmsPID,	  &SinvalBcastPID, &LckPID,
						&LmdPID,  &QvotecPID, &CssdPID,		   &CheckpointerPID };
	for (unsigned i = 0; i < lengthof(single); i++)
		if (*single[i] == pid)
			*single[i] = 0;
	for (int i = 0; i < 8; i++) {
		if (LmsWorkerPIDs[i] == pid)
			LmsWorkerPIDs[i] = 0;
		if (UndoCleanerPIDs[i] == pid)
			UndoCleanerPIDs[i] = 0;
	}
}
static void
finish_checkpoint(void)
{
	protocol_closed = true;
	pmState = PM_SHUTDOWN;
	NormalStopPostmasterBindCheckpointer();
	NormalStopPostmasterReap(CheckpointerPID, 0);
	CheckpointerPID = 0;
}
UT_TEST(capture_is_once_and_does_not_authorize_checkpoint)
{
	setup();
	UT_ASSERT(NormalStopPostmasterBegin());
	UT_ASSERT(normal_stop_pm.selected);
	UT_ASSERT_EQ(request_calls, 1);
	UT_ASSERT_EQ(normal_stop_pm.expected, (1U << 22) - 1);
	UT_ASSERT_EQ(normal_stop_pm.pids[21], 121);
	UT_ASSERT_EQ(normal_stop_pm.checkpointer, 140);
	UT_ASSERT(NormalStopPostmasterBegin());
	UT_ASSERT_EQ(request_calls, 1);
	UT_ASSERT(!NormalStopPostmasterComplete());
	UT_ASSERT_EQ(signal_count, 0);
}
UT_TEST(configured_pool_is_frozen_not_surviving_pids)
{
	setup();
	cluster_lms_workers = 2;
	cluster_lmd_enabled = false;
	LmdPID = 0;
	for (int i = 2; i < 8; i++)
		LmsWorkerPIDs[i] = 0;
	UT_ASSERT(NormalStopPostmasterBegin());
	UT_ASSERT(NormalStopPostmasterRosterMatches());
	UT_ASSERT_EQ(normal_stop_pm.pids[8], 0);
	LmsWorkerPIDs[1] = 0;
	UT_ASSERT(!NormalStopPostmasterRosterMatches());
	UT_ASSERT(failure != 0);
	UT_ASSERT((normal_stop_pm.expected & 4) != 0);
}
UT_TEST(missing_extra_and_duplicate_actor_fail_without_shrinking)
{
	for (int fault = 0; fault < 4; fault++) {
		setup();
		if (fault == 0)
			UndoCleanerPIDs[7] = 0;
		if (fault == 1)
			CssdPID = 0;
		if (fault == 2)
			LmdPID = LckPID;
		if (fault == 3) {
			cluster_lms_workers = 1;
		}
		(void)NormalStopPostmasterBegin();
		UT_ASSERT(normal_stop_pm.selected);
		UT_ASSERT(failure != 0);
		UT_ASSERT_EQ(signal_count, 0);
	}
}
UT_TEST(actual_normal_request_rejects_missing_roster_before_shutdown_mutation)
{
	for (int fast = 0; fast < 2; fast++) {
		setup();
		Shutdown = NoShutdown;
		child_count = 1;
		connsAllowed = true;
		status_writes = 0;
		UndoCleanerPIDs[7] = 0;
		pending_pm_shutdown_request = true;
		pending_pm_fast_shutdown_request = fast;
		pending_pm_immediate_shutdown_request = false;
		process_pm_shutdown_request();
		UT_ASSERT_EQ(Shutdown, NoShutdown);
		UT_ASSERT_EQ(pmState, PM_RUN);
		UT_ASSERT(connsAllowed);
		UT_ASSERT(!normal_stop_pm.selected);
		UT_ASSERT_EQ(request_calls, 0);
		UT_ASSERT_EQ(signal_count, 0);
		UT_ASSERT_EQ(status_writes, 0);
		UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
		/* The original respawn loop, not a shutdown replacement, fills it. */
		actual_cluster_respawn_edges();
		UT_ASSERT(UndoCleanerPIDs[7] > 0);
		pending_pm_shutdown_request = true;
		pending_pm_fast_shutdown_request = fast;
		process_pm_shutdown_request();
		UT_ASSERT_EQ(Shutdown, fast ? FastShutdown : SmartShutdown);
		UT_ASSERT(normal_stop_pm.selected);
		UT_ASSERT_EQ(request_calls, 1);
		UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
	}
}
UT_TEST(pristine_noncluster_and_abnormal_do_not_select_current)
{
	for (int fault = 0; fault < 6; fault++) {
		setup();
		if (fault == 0)
			retention_hint = false;
		if (fault == 1)
			cluster_enabled = false;
		if (fault == 2)
			Shutdown = ImmediateShutdown;
		if (fault == 3)
			FatalError = true;
		if (fault == 4)
			configured_nodes = 2;
		if (fault == 5)
			IsUnderPostmaster = true;
		UT_ASSERT(!NormalStopPostmasterBegin());
		UT_ASSERT(!normal_stop_pm.selected);
		UT_ASSERT_EQ(request_calls, 0);
	}
}
UT_TEST(early_or_abnormal_exit_never_signs_clean)
{
	for (int fault = 0; fault < 3; fault++) {
		pid_t pid;
		setup();
		(void)NormalStopPostmasterBegin();
		pid = fault == 2 ? CheckpointerPID : UndoCleanerPIDs[7];
		NormalStopPostmasterReap(pid, fault == 1 ? 256 : 0);
		forget_pid(pid);
		UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_CHILD_EXIT);
		UT_ASSERT(!NormalStopPostmasterRelease());
		UT_ASSERT_EQ(signal_count, 0);
	}
}
UT_TEST(replacement_and_config_drift_do_not_requalify)
{
	for (int fault = 0; fault < 3; fault++) {
		setup();
		(void)NormalStopPostmasterBegin();
		if (fault == 0)
			LmsPID = 900;
		if (fault == 1)
			cluster_lms_workers = 1;
		if (fault == 2)
			cluster_lmd_enabled = false;
		UT_ASSERT(!NormalStopPostmasterRosterMatches());
		UT_ASSERT(failure != 0);
		UT_ASSERT_EQ(normal_stop_pm.pids[1], 101);
	}
}
UT_TEST(success_needs_all_original_exits_and_all_disk_clear)
{
	setup();
	(void)NormalStopPostmasterBegin();
	finish_checkpoint();
	UT_ASSERT(NormalStopPostmasterRelease());
	UT_ASSERT_EQ(signal_count, 22);
	UT_ASSERT(NormalStopPostmasterRelease());
	UT_ASSERT_EQ(signal_count, 22);
	for (int i = 0; i < 22; i++) {
		pid_t pid = normal_stop_pm.pids[i];
		UT_ASSERT(!NormalStopPostmasterComplete());
		NormalStopPostmasterReap(pid, 0);
		forget_pid(pid);
	}
	UT_ASSERT_EQ(normal_stop_pm.reaped, normal_stop_pm.expected);
	UT_ASSERT(!NormalStopPostmasterComplete());
	clear_ok = true;
	UT_ASSERT(NormalStopPostmasterComplete());
	cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_QVOTEC);
	UT_ASSERT(!NormalStopPostmasterComplete());
}
UT_TEST(checkpoint_zero_exit_without_protocol_does_not_release_actors)
{
	setup();
	(void)NormalStopPostmasterBegin();
	pmState = PM_SHUTDOWN;
	NormalStopPostmasterReap(CheckpointerPID, 0);
	CheckpointerPID = 0;
	UT_ASSERT(!NormalStopPostmasterRelease());
	UT_ASSERT(failure != 0);
	UT_ASSERT_EQ(signal_count, 0);
}
UT_TEST(checkpointer_started_late_can_bind_once_but_not_replace)
{
	setup();
	CheckpointerPID = 0;
	(void)NormalStopPostmasterBegin();
	CheckpointerPID = 500;
	NormalStopPostmasterBindCheckpointer();
	UT_ASSERT_EQ(normal_stop_pm.checkpointer, 500);
	CheckpointerPID = 501;
	NormalStopPostmasterBindCheckpointer();
	UT_ASSERT_EQ(normal_stop_pm.checkpointer, 500);
	UT_ASSERT(failure != 0);
}
UT_TEST(actual_terminate_preserves_same_roster_only_for_normal_stop)
{
	for (int mode = SmartShutdown; mode <= FastShutdown; mode++) {
		setup();
		Shutdown = mode;
		TerminateChildren(SIGTERM);
		UT_ASSERT(normal_stop_pm.selected);
		UT_ASSERT_EQ(signal_count, 1);
		UT_ASSERT_EQ(signals[0], CheckpointerPID);
	}
	setup();
	Shutdown = ImmediateShutdown;
	expected_signal = SIGQUIT;
	TerminateChildren(SIGQUIT);
	UT_ASSERT(!normal_stop_pm.selected);
	UT_ASSERT_EQ(signal_count, 23);
}
UT_TEST(actual_stop_backends_keeps_every_required_actor)
{
	setup();
	(void)NormalStopPostmasterBegin();
	pmState = PM_STOP_BACKENDS;
	child_count = 1;
	PostmasterStateMachine();
	UT_ASSERT_EQ(pmState, PM_WAIT_BACKENDS);
	UT_ASSERT_EQ(signal_count, 0);
	UT_ASSERT_EQ(frontend_cut_calls, 0);
	UT_ASSERT_EQ(legacy_suppress_calls, 0);
	UT_ASSERT_EQ(legacy_candidate_calls, 0);
}
UT_TEST(actual_wait_backends_does_not_wait_for_retained_actors)
{
	setup();
	(void)NormalStopPostmasterBegin();
	pmState = PM_WAIT_BACKENDS;
	expected_signal = SIGUSR2;
	PostmasterStateMachine();
	UT_ASSERT_EQ(pmState, PM_SHUTDOWN);
	UT_ASSERT_EQ(frontend_cut_calls, 1);
	UT_ASSERT_EQ(signal_count, 1);
	UT_ASSERT_EQ(signals[0], CheckpointerPID);
	UT_ASSERT_EQ(legacy_candidate_calls, 0);
}
UT_TEST(actual_wait_requires_each_native_producer_to_exit)
{
	for (int f = 0; f < 10; f++) {
		setup();
		(void)NormalStopPostmasterBegin();
		pmState = PM_WAIT_BACKENDS;
		if (f == 0)
			child_count = 1;
		if (f == 1)
			StartupPID = 701;
		if (f == 2)
			WalReceiverPID = 702;
		if (f == 3)
			BgWriterPID = 703;
		if (f == 4)
			WalWriterPID = 704;
		if (f == 5)
			DiagPID = 705;
		if (f == 6)
			ClusterStatsPID = 706;
		if (f == 7)
			MrpPID = 707;
		if (f == 8)
			RfsPID = 708;
		if (f == 9)
			AutoVacPID = 709;
		PostmasterStateMachine();
		UT_ASSERT_EQ(pmState, PM_WAIT_BACKENDS);
		UT_ASSERT_EQ(frontend_cut_calls, 0);
		UT_ASSERT_EQ(signal_count, 0);
	}
}
UT_TEST(actual_shutdown2_waits_for_late_cleaner_then_checks_disk_result)
{
	setup();
	(void)NormalStopPostmasterBegin();
	finish_checkpoint();
	UT_ASSERT(NormalStopPostmasterRelease());
	signal_count = 0;
	for (int i = 0; i < 21; i++) {
		pid_t pid = normal_stop_pm.pids[i];
		NormalStopPostmasterReap(pid, 0);
		forget_pid(pid);
	}
	pmState = PM_SHUTDOWN_2;
	if (setjmp(exit_jump) == 0)
		PostmasterStateMachine();
	UT_ASSERT_EQ(exit_code, -1);
	UT_ASSERT_EQ(pmState, PM_SHUTDOWN_2);
	NormalStopPostmasterReap(UndoCleanerPIDs[7], 0);
	UndoCleanerPIDs[7] = 0;
	if (setjmp(exit_jump) == 0)
		PostmasterStateMachine();
	UT_ASSERT_EQ(exit_code, 1);
	UT_ASSERT(failure != 0);
}
UT_TEST(actual_final_exit_requires_original_exits_and_disk_clear)
{
	for (int f = 0; f < 3; f++) {
		setup();
		(void)NormalStopPostmasterBegin();
		finish_checkpoint();
		UT_ASSERT(NormalStopPostmasterRelease());
		for (int i = 0; i < 22; i++) {
			pid_t pid = normal_stop_pm.pids[i];
			if (f != 1)
				NormalStopPostmasterReap(pid, 0);
			forget_pid(pid);
		}
		clear_ok = true;
		if (f == 2)
			cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_CHILD_EXIT);
		pmState = PM_SHUTDOWN_2;
		if (setjmp(exit_jump) == 0)
			PostmasterStateMachine();
		UT_ASSERT_EQ(exit_code, f == 0 ? 0 : 1);
		UT_ASSERT_EQ(legacy_shutdown_calls, f == 0 ? 1 : 0);
	}
}
UT_TEST(actual_respawn_never_replaces_selected_aux_roster)
{
	setup();
	(void)NormalStopPostmasterBegin();
	for (int i = 0; i < 22; i++)
		forget_pid(normal_stop_pm.pids[i]);
	/* Diagnostics are not selected owners and keep their native behavior. */
	DiagPID = 701;
	ClusterStatsPID = 702;
	actual_cluster_respawn_edges();
	UT_ASSERT_EQ(spawn_calls, 0);
	UT_ASSERT(!NormalStopPostmasterRosterMatches());
}
UT_TEST(actual_checkpoint_reaper_releases_exact_roster_only_after_proof)
{
	for (int f = 0; f < 4; f++) {
		setup();
		(void)NormalStopPostmasterBegin();
		pmState = PM_SHUTDOWN;
		protocol_closed = f != 1;
		allow_final_walsender_wake = true;
		if (f == 3) {
			NormalStopPostmasterReap(LmsPID, 0);
			LmsPID = 0;
		}
		actual_checkpoint_reaper(CheckpointerPID, f == 2 ? 256 : 0);
		UT_ASSERT_EQ(CheckpointerPID, 0);
		UT_ASSERT_EQ(signal_count, f == 0 ? 22 : 0);
		UT_ASSERT_EQ(child_crash_calls, f == 0 ? 0 : 1);
		UT_ASSERT_EQ(pmState, f == 0 ? PM_SHUTDOWN_2 : PM_WAIT_BACKENDS);
		UT_ASSERT_EQ(normal_stop_pm.checkpoint_exited, f == 0);
	}
}
UT_TEST(immediate_upgrade_keeps_native_abnormal_exit_not_normal_proof)
{
	setup();
	(void)NormalStopPostmasterBegin();
	Shutdown = ImmediateShutdown;
	for (int i = 0; i < 22; i++)
		forget_pid(normal_stop_pm.pids[i]);
	CheckpointerPID = 0;
	pmState = PM_WAIT_BACKENDS;
	if (setjmp(exit_jump) == 0)
		PostmasterStateMachine();
	UT_ASSERT_EQ(exit_code, 0);
	UT_ASSERT_EQ(frontend_cut_calls, 0);
	UT_ASSERT(!normal_stop_pm.checkpoint_exited);
	UT_ASSERT(!NormalStopPostmasterComplete());
	UT_ASSERT_EQ(legacy_candidate_calls, 0);
}
UT_TEST(actual_intent_publication_failure_cannot_dispatch_legacy_checkpoint)
{
	setup();
	request_available = false;
	(void)NormalStopPostmasterBegin();
	pmState = PM_WAIT_BACKENDS;
	expected_signal = SIGQUIT;
	PostmasterStateMachine();
	UT_ASSERT(FatalError);
	UT_ASSERT_EQ(pmState, PM_WAIT_BACKENDS);
	UT_ASSERT_EQ(frontend_cut_calls, 0);
	UT_ASSERT(!request);
	UT_ASSERT_EQ(signal_count, 23);
	UT_ASSERT(failure != 0);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(capture_is_once_and_does_not_authorize_checkpoint);
	UT_RUN(configured_pool_is_frozen_not_surviving_pids);
	UT_RUN(missing_extra_and_duplicate_actor_fail_without_shrinking);
	UT_RUN(actual_normal_request_rejects_missing_roster_before_shutdown_mutation);
	UT_RUN(pristine_noncluster_and_abnormal_do_not_select_current);
	UT_RUN(early_or_abnormal_exit_never_signs_clean);
	UT_RUN(replacement_and_config_drift_do_not_requalify);
	UT_RUN(success_needs_all_original_exits_and_all_disk_clear);
	UT_RUN(checkpoint_zero_exit_without_protocol_does_not_release_actors);
	UT_RUN(checkpointer_started_late_can_bind_once_but_not_replace);
	UT_RUN(actual_terminate_preserves_same_roster_only_for_normal_stop);
	UT_RUN(actual_stop_backends_keeps_every_required_actor);
	UT_RUN(actual_wait_backends_does_not_wait_for_retained_actors);
	UT_RUN(actual_wait_requires_each_native_producer_to_exit);
	UT_RUN(actual_shutdown2_waits_for_late_cleaner_then_checks_disk_result);
	UT_RUN(actual_final_exit_requires_original_exits_and_disk_clear);
	UT_RUN(actual_respawn_never_replaces_selected_aux_roster);
	UT_RUN(actual_checkpoint_reaper_releases_exact_roster_only_after_proof);
	UT_RUN(immediate_upgrade_keeps_native_abnormal_exit_not_normal_proof);
	UT_RUN(actual_intent_publication_failure_cannot_dispatch_legacy_checkpoint);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
