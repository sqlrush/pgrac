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

#include <signal.h>

#include "cluster/cluster_stats.h"

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
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
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

void
proc_exit(int code pg_attribute_unused())
{
	abort();
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
	return 0;
}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}

/* cluster_stats.c references MyBackendType (set by ClusterStatsMain). */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;


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


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_stats_status_enum_values_frozen);
	UT_RUN(test_stats_shared_state_size_under_4kb);
	UT_RUN(test_stats_status_to_string_lookup);
	UT_RUN(test_stats_status_unknown_returns_unknown);
	UT_RUN(test_stats_public_symbols_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
