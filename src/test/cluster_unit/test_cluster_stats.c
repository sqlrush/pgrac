/*-------------------------------------------------------------------------
 *
 * test_cluster_stats.c
 *	  Compile-time / link-level invariants for spec-1.14 DIAG Sprint A.
 *
 *	  Locks:
 *	    - ClusterStatsStatus enum values (NOT_STARTED=0, SPAWNING=1,
 *	      READY=2, SHUTTING_DOWN=3, EXITED=4) are frozen.
 *	    - ClusterStatsSharedState size stays under 4 KiB (catch
 *	      accidental field bloat early).
 *	    - cluster_stats_status_to_string() returns non-null for every
 *	      enum value and "(unknown)" for out-of-range.
 *	    - Public symbols cluster_stats_start / wait_for_ready /
 *	      request_shutdown / status / shmem_register/init / ClusterStatsMain
 *	      resolve at link time.
 *
 *	  Behavior tests (postmaster spawns Cluster Stats, phase 1 sync
 *	  wait ready, clean shutdown, kill -9 crash recovery) live in TAP
 *	  t/064_cluster_stats_skeleton.pl (Hardening v1.0.1 codex review
 *	  P2-4 fix: was wrongly pointing at 062_lck_skeleton.pl).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-1.14-cluster-stats-skeleton.md (Hardening v1.0.1
 *	        codex review P2-4 fix: was wrongly named
 *	        spec-1.14-lck-skeleton.md).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <signal.h>

#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_stats.o standalone.  Runtime paths
 * (ClusterStatsMain / shmem init) are not exercised here; these are address-
 * only / pure-function tests.
 * ----------
 */

bool IsUnderPostmaster = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
int MyProcPid = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}
void
pre_format_elog_string(int n pg_attribute_unused(), const char *d pg_attribute_unused())
{}
char *
format_elog_string(const char *f pg_attribute_unused(), ...)
{
	return NULL;
}

#include "storage/lwlock.h"
#include "storage/shmem.h"
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	static char fake_shmem[4096];

	memset(fake_shmem, 0, sizeof(fake_shmem));
	if (foundPtr != NULL)
		*foundPtr = false;
	return fake_shmem;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	static TimestampTz now = 1000;

	return now++;
}

/* postmaster-owned wrapper: DIAG main never invokes the runtime path
 * here, but cluster_stats_start() is a thin proxy that forwards to it,
 * so the symbol must resolve at link time. */
pid_t
cluster_postmaster_start_stats(void)
{
	return 0;
}

/* Spec-1.11 Sprint B: cluster_stats.c references
 * cluster_stats_main_loop_interval GUC + WaitLatch / ResetLatch /
 * MyLatch + cluster_inject framework.  Stubs cover them all --
 * runtime ClusterStatsMain is not exercised. */
int cluster_cluster_stats_main_loop_interval = 1000;

#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
/* spec-1.14.1 F20 stub. */
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

/* libpq + procsignal stubs (pulled in transitively via cluster_stats.c
 * includes; ClusterStatsMain runtime is not invoked). */
struct sigaction;
typedef void (*pqsigfunc)(int);
pqsigfunc
pqsignal(int signum pg_attribute_unused(), pqsigfunc handler pg_attribute_unused())
{
	return handler;
}
void
SignalHandlerForConfigReload(int sig pg_attribute_unused())
{}
void
SignalHandlerForShutdownRequest(int sig pg_attribute_unused())
{}
void
procsignal_sigusr1_handler(int sig pg_attribute_unused())
{}
sigset_t UnBlockSig;
void
ProcessConfigFile(int context pg_attribute_unused())
{}

void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}

static jmp_buf stats_main_exit;
static int stats_main_exit_code = -1;

void
proc_exit(int code)
{
	stats_main_exit_code = code;
	longjmp(stats_main_exit, 1);
}

#include "utils/timestamp.h"

/* CHECK_FOR_INTERRUPTS stubs */
volatile sig_atomic_t InterruptPending = false;
void
ProcessInterrupts(void)
{}

void
pg_usleep(long microsec pg_attribute_unused())
{}

/* Sprint B: Latch / WaitLatch / ResetLatch stubs (ClusterStatsMain runtime
 * is not invoked at unit-test level). */
struct Latch *MyLatch = NULL;
int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	ShutdownRequestPending = true;
	return 0;
}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}

/* cluster_stats.c references MyBackendType (set by ClusterStatsMain). */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;


/* ---------- RF A1 W2/W4 lifecycle fixtures. ---------- */
bool cluster_enabled = true;
char *cluster_wal_threads_dir = "/rf-a1/formed";
int cluster_node_id = 3;

static ClusterStartupPhase stats_test_phase = CLUSTER_PHASE_4_NORMAL;
static bool stats_test_self_fenced = false;
static int stats_test_self_check_calls = 0;
static int stats_test_checkpoint_calls = 0;
static int stats_test_checkpoint_flags = 0;
static int stats_test_slot_read_calls = 0;
static ClusterWalSlotVerdict stats_test_slot_verdict = CLUSTER_WAL_SLOT_OK;
static ClusterWalStateUpdateResult stats_test_active_result = CLUSTER_WAL_STATE_UPDATE_OK;
static ClusterWalStateUpdateResult stats_test_telemetry_result = CLUSTER_WAL_STATE_UPDATE_OK;
static int stats_test_active_calls = 0;
static int stats_test_telemetry_calls = 0;
static ClusterStatsStatus stats_test_active_status = CLUSTER_STATS_NOT_STARTED;
static ClusterStatsStatus stats_test_telemetry_status = CLUSTER_STATS_NOT_STARTED;
static ClusterWalStateUpdate stats_test_active_update;
static ClusterWalStateUpdate stats_test_telemetry_update;
static uint64 stats_test_refresh_fail_count = 0;

ClusterStartupPhase
cluster_current_phase(void)
{
	return stats_test_phase;
}

const char *
cluster_startup_phase_to_string(ClusterStartupPhase phase pg_attribute_unused())
{
	return "test_phase";
}

bool
cluster_write_fence_startup_self_check(void)
{
	stats_test_self_check_calls++;
	return stats_test_self_fenced;
}

void
RequestCheckpoint(int flags)
{
	stats_test_checkpoint_calls++;
	stats_test_checkpoint_flags = flags;
}

bool
RecoveryInProgress(void)
{
	return false;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	return 7;
}

XLogRecPtr
GetXLogWriteRecPtr(void)
{
	return 300;
}

XLogRecPtr
GetXLogReplayRecPtr(TimeLineID *replayTLI)
{
	if (replayTLI != NULL)
		*replayTLI = 7;
	return 300;
}

SCN
cluster_scn_current(void)
{
	return 400;
}

uint16
cluster_wal_thread_id(void)
{
	return 4;
}

uint64
cluster_wal_thread_refresh_fail_fetch_add(void)
{
	return stats_test_refresh_fail_count++;
}

uint64
cluster_wal_thread_refresh_fail_read(void)
{
	return stats_test_refresh_fail_count;
}

bool
cluster_wal_state_registry_ready(void)
{
	return cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0';
}

ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id, ClusterWalStateSlot *slot_out)
{
	stats_test_slot_read_calls++;
	if (slot_out != NULL) {
		memset(slot_out, 0, sizeof(*slot_out));
		slot_out->thread_id = thread_id;
		slot_out->node_id = cluster_node_id;
		slot_out->state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
	}
	return stats_test_slot_verdict;
}

ClusterWalStateUpdateResult
cluster_wal_state_update_own(const ClusterWalStateUpdate *update, ClusterWalStateCfMode cf_mode,
							 ClusterWalStateSlot *published_slot pg_attribute_unused())
{
	UT_ASSERT_EQ((int)cf_mode, (int)CLUSTER_WAL_STATE_CF_ACQUIRE_X);
	UT_ASSERT_NOT_NULL(update);
	if (update->kind == CLUSTER_WAL_STATE_UPDATE_ACTIVE) {
		stats_test_active_calls++;
		stats_test_active_status = cluster_stats_status();
		memcpy(&stats_test_active_update, update, sizeof(*update));
		return stats_test_active_result;
	}
	if (update->kind == CLUSTER_WAL_STATE_UPDATE_TELEMETRY) {
		stats_test_telemetry_calls++;
		stats_test_telemetry_status = cluster_stats_status();
		memcpy(&stats_test_telemetry_update, update, sizeof(*update));
		return stats_test_telemetry_result;
	}
	return CLUSTER_WAL_STATE_UPDATE_INVALID;
}


static void
reset_stats_lifecycle_fixture(void)
{
	ConfigReloadPending = false;
	ShutdownRequestPending = false;
	stats_main_exit_code = -1;
	stats_test_phase = CLUSTER_PHASE_4_NORMAL;
	stats_test_self_fenced = false;
	stats_test_self_check_calls = 0;
	stats_test_checkpoint_calls = 0;
	stats_test_checkpoint_flags = 0;
	stats_test_slot_read_calls = 0;
	stats_test_slot_verdict = CLUSTER_WAL_SLOT_OK;
	stats_test_active_result = CLUSTER_WAL_STATE_UPDATE_OK;
	stats_test_telemetry_result = CLUSTER_WAL_STATE_UPDATE_OK;
	stats_test_active_calls = 0;
	stats_test_telemetry_calls = 0;
	stats_test_active_status = CLUSTER_STATS_NOT_STARTED;
	stats_test_telemetry_status = CLUSTER_STATS_NOT_STARTED;
	memset(&stats_test_active_update, 0, sizeof(stats_test_active_update));
	memset(&stats_test_telemetry_update, 0, sizeof(stats_test_telemetry_update));
	stats_test_refresh_fail_count = 0;
	MyProcPid = 4242;
	IsUnderPostmaster = true;
	cluster_stats_shmem_init();
}


static void
run_one_stats_incarnation(void)
{
	if (setjmp(stats_main_exit) == 0)
		ClusterStatsMain();
	UT_ASSERT_EQ(stats_main_exit_code, 0);
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Compile-time anchors
 * ============================================================ */

UT_TEST(test_stats_status_enum_values_frozen)
{
	UT_ASSERT_EQ((int)CLUSTER_STATS_NOT_STARTED, 0);
	UT_ASSERT_EQ((int)CLUSTER_STATS_SPAWNING, 1);
	UT_ASSERT_EQ((int)CLUSTER_STATS_READY, 2);
	UT_ASSERT_EQ((int)CLUSTER_STATS_SHUTTING_DOWN, 3);
	UT_ASSERT_EQ((int)CLUSTER_STATS_EXITED, 4);
	UT_ASSERT_EQ((int)CLUSTER_STATS_STATUS_LAST, 4);
}


UT_TEST(test_stats_shared_state_size_under_4kb)
{
	/* Catch accidental field bloat early (typical size ~80 bytes). */
	UT_ASSERT(sizeof(ClusterStatsSharedState) < 4096);
}


UT_TEST(test_stats_status_to_string_lookup)
{
	int i;

	for (i = 0; i <= (int)CLUSTER_STATS_STATUS_LAST; i++) {
		const char *s = cluster_stats_status_to_string((ClusterStatsStatus)i);
		UT_ASSERT_NOT_NULL(s);
		if (s != NULL)
			UT_ASSERT(s[0] != '\0');
	}
}


UT_TEST(test_stats_status_unknown_returns_unknown)
{
	const char *neg = cluster_stats_status_to_string((ClusterStatsStatus)-1);
	const char *over
		= cluster_stats_status_to_string((ClusterStatsStatus)((int)CLUSTER_STATS_STATUS_LAST + 1));

	UT_ASSERT_STR_EQ(neg, "(unknown)");
	UT_ASSERT_STR_EQ(over, "(unknown)");
}


UT_TEST(test_stats_public_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_stats_start);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_wait_for_ready);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_request_shutdown);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_status);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_status_to_string);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_shmem_init);
	UT_ASSERT_NOT_NULL((void *)cluster_stats_shmem_register);
	UT_ASSERT_NOT_NULL((void *)ClusterStatsMain);
}


UT_TEST(test_rf_a1_initial_stats_owns_active_checkpoint_then_telemetry)
{
	reset_stats_lifecycle_fixture();
	run_one_stats_incarnation();

	UT_ASSERT_EQ(stats_test_self_check_calls, 1);
	UT_ASSERT_EQ(stats_test_active_calls, 1);
	UT_ASSERT_EQ((int)stats_test_active_status, (int)CLUSTER_STATS_SPAWNING);
	UT_ASSERT_EQ((int)stats_test_active_update.kind, (int)CLUSTER_WAL_STATE_UPDATE_ACTIVE);
	UT_ASSERT_EQ((int)stats_test_active_update.tli, 7);
	UT_ASSERT_EQ((uint64)stats_test_active_update.highest_lsn, (uint64)300);
	UT_ASSERT_EQ((uint64)stats_test_active_update.highest_scn, (uint64)400);
	UT_ASSERT_EQ((int)stats_test_active_update.refresh_interval_ms, 1000);
	UT_ASSERT_EQ(stats_test_checkpoint_calls, 1);
	UT_ASSERT_EQ(stats_test_checkpoint_flags,
				 CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
	UT_ASSERT_EQ(stats_test_telemetry_calls, 1);
	UT_ASSERT_EQ((int)stats_test_telemetry_status, (int)CLUSTER_STATS_READY);
	UT_ASSERT_EQ((int)stats_test_telemetry_update.kind, (int)CLUSTER_WAL_STATE_UPDATE_TELEMETRY);
	UT_ASSERT_EQ((int)stats_test_telemetry_update.tli, 7);
	UT_ASSERT_EQ((int64)stats_test_telemetry_update.started_at, (int64)0);
	UT_ASSERT_EQ((uint64)stats_test_telemetry_update.highest_lsn, (uint64)300);
	UT_ASSERT_EQ((uint64)stats_test_telemetry_update.highest_scn, (uint64)400);
	UT_ASSERT_EQ((int)stats_test_telemetry_update.refresh_interval_ms, 1000);
}


UT_TEST(test_rf_a1_self_fenced_initial_stats_skips_active_and_checkpoint)
{
	reset_stats_lifecycle_fixture();
	stats_test_self_fenced = true;
	stats_test_telemetry_result = CLUSTER_WAL_STATE_UPDATE_WRONG_STATE;
	run_one_stats_incarnation();

	UT_ASSERT_EQ(stats_test_self_check_calls, 1);
	UT_ASSERT_EQ(stats_test_active_calls, 0);
	UT_ASSERT_EQ(stats_test_checkpoint_calls, 0);
	UT_ASSERT_EQ(stats_test_telemetry_calls, 0);
	UT_ASSERT_EQ((uint64)stats_test_refresh_fail_count, (uint64)0);
}


UT_TEST(test_rf_a1_running_respawn_validates_active_without_replaying_w2)
{
	reset_stats_lifecycle_fixture();
	stats_test_phase = CLUSTER_PHASE_RUNNING;
	run_one_stats_incarnation();

	UT_ASSERT_EQ(stats_test_self_check_calls, 0);
	UT_ASSERT_EQ(stats_test_slot_read_calls, 1);
	UT_ASSERT_EQ(stats_test_active_calls, 0);
	UT_ASSERT_EQ(stats_test_checkpoint_calls, 0);
	UT_ASSERT_EQ(stats_test_telemetry_calls, 1);
}


UT_TEST(test_rf_a1_unconfigured_registry_keeps_stats_vanilla)
{
	reset_stats_lifecycle_fixture();
	cluster_wal_threads_dir = NULL;
	run_one_stats_incarnation();

	UT_ASSERT_EQ(stats_test_self_check_calls, 0);
	UT_ASSERT_EQ(stats_test_active_calls, 0);
	UT_ASSERT_EQ(stats_test_checkpoint_calls, 0);
	UT_ASSERT_EQ(stats_test_telemetry_calls, 0);
	cluster_wal_threads_dir = "/rf-a1/formed";
}


UT_TEST(test_rf_a1_w4_failure_increments_existing_counter)
{
	reset_stats_lifecycle_fixture();
	stats_test_telemetry_result = CLUSTER_WAL_STATE_UPDATE_IO_ERROR;
	run_one_stats_incarnation();

	UT_ASSERT_EQ(stats_test_telemetry_calls, 1);
	UT_ASSERT_EQ((uint64)stats_test_refresh_fail_count, (uint64)1);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_stats_status_enum_values_frozen);
	UT_RUN(test_stats_shared_state_size_under_4kb);
	UT_RUN(test_stats_status_to_string_lookup);
	UT_RUN(test_stats_status_unknown_returns_unknown);
	UT_RUN(test_stats_public_symbols_linkable);
	UT_RUN(test_rf_a1_initial_stats_owns_active_checkpoint_then_telemetry);
	UT_RUN(test_rf_a1_self_fenced_initial_stats_skips_active_and_checkpoint);
	UT_RUN(test_rf_a1_running_respawn_validates_active_without_replaying_w2);
	UT_RUN(test_rf_a1_unconfigured_registry_keeps_stats_vanilla);
	UT_RUN(test_rf_a1_w4_failure_increments_existing_counter);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
