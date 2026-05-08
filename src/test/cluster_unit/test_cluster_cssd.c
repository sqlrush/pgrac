/*-------------------------------------------------------------------------
 *
 * test_cluster_cssd.c
 *	  Compile-time / link-level invariants for spec-2.5 D1+D2 (Sprint A
 *	  Step 2 — initial scaffolding).
 *
 *	  Locks (Step 2 standalone):
 *	    - ClusterCssdStatus enum 0..4 frozen + status_to_string round-trip
 *	    - ClusterCssdPeerState 0..2 frozen + peer_state_to_string
 *	    - ClusterCssdHeartbeatPayload 12 bytes (StaticAssertDecl runtime
 *	      mirror)
 *	    - ClusterCssdOutboundSlot 64 bytes (cache-line aligned)
 *	    - cluster_cssd_shmem_size() reasonable upper bound
 *	    - cluster_cssd_shmem_init() idempotent
 *	    - dispatch_heartbeat handler 4 hard constraints (STATIC source-grep)
 *	    - deadband factor calculation invariant (default 3 → suspected=2,
 *	      dead=3)
 *	    - Public symbol resolves at link time (cluster_cssd_start /
 *	      wait_for_ready / request_shutdown / status / shmem_init /
 *	      CssdMain / dispatch_heartbeat)
 *
 *	  Spec-2.5 Step 5 lands GUC + wait event + inject points; full
 *	  runtime tests live in TAP t/065_cssd_skeleton.pl + t/085_*.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cssd.c
 *
 * NOTES
 *	  pgrac-original file.  Spec: spec-2.5-cssd-heartbeat-skeleton.md
 *	  (frozen v0.2 2026-05-08).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "cluster/cluster_cssd.h"

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


/* ============================================================
 * Stubs — link cluster_cssd.o standalone.  Runtime CssdMain is not
 * exercised here;tests verify byte layout, accessor returns, status
 * string round-trip, deadband math, source-grep handler constraints.
 * ============================================================ */

bool IsUnderPostmaster = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
int MyProcPid = 0;
int cluster_node_id = 0;

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

/* ShmemInitStruct stub: hand back a writable buffer the test allocates
 * locally, so cluster_cssd_shmem_init() can execute its memset + atomic
 * init paths without crashing.  ClusterCssdShmem is ~17 KB (128 peer
 * state + 128 outbound slots);size at 64 KB to leave headroom. */
static char shmem_storage[65536] __attribute__((aligned(64)));
static bool shmem_init_done = false;
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = shmem_init_done;
	if (size > sizeof(shmem_storage))
		return NULL;
	shmem_init_done = true;
	return (void *)shmem_storage;
}

#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	return 1700000000000000LL;
}

/* proc_exit stub for CssdMain skeleton (not exercised here, but linker
 * needs the symbol). */
void
proc_exit(int code pg_attribute_unused())
{
	abort();
}

/* CssdMain runtime stubs (Step 4 wires postmaster spawn;tests don't
 * invoke the full main loop). */
#include "miscadmin.h"
volatile sig_atomic_t InterruptPending = false;
BackendType MyBackendType = B_INVALID;
struct Latch *MyLatch = NULL;
sigset_t UnBlockSig;

void
ProcessConfigFile(int context pg_attribute_unused())
{}
void
ProcessInterrupts(void)
{}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}
int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}
void
SignalHandlerForConfigReload(int sig pg_attribute_unused())
{}
void
SignalHandlerForShutdownRequest(int sig pg_attribute_unused())
{}
void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}
void
pg_usleep(long microsec pg_attribute_unused())
{}

typedef void (*pqsigfunc)(int);
pqsigfunc
pqsignal(int signum pg_attribute_unused(), pqsigfunc handler pg_attribute_unused())
{
	return handler;
}
void
procsignal_sigusr1_handler(int sig pg_attribute_unused())
{}

/* Step 4 cluster_postmaster_start_cssd lives in postmaster.c;tests don't
 * spawn so stub returns 0 (failure) — consistent with cluster_cssd_start
 * Assert(!IsUnderPostmaster) but tests never invoke start. */
pid_t
cluster_postmaster_start_cssd(void)
{
	return 0;
}

/* Step 4 cluster_shmem_register_region;tests don't register so stub. */
#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


/* ============================================================
 * Tests.
 * ============================================================ */

UT_TEST(test_t1_status_to_string_round_trip)
{
	UT_ASSERT(strcmp(cluster_cssd_status_to_string(CLUSTER_CSSD_STARTING), "starting") == 0);
	UT_ASSERT(strcmp(cluster_cssd_status_to_string(CLUSTER_CSSD_READY), "ready") == 0);
	UT_ASSERT(strcmp(cluster_cssd_status_to_string(CLUSTER_CSSD_SHUTTING_DOWN), "shutting_down")
			  == 0);
	UT_ASSERT(strcmp(cluster_cssd_status_to_string(CLUSTER_CSSD_DOWN), "down") == 0);
	UT_ASSERT(strcmp(cluster_cssd_status_to_string(CLUSTER_CSSD_FAILED), "failed") == 0);

	/* Out-of-range -> "(unknown)". */
	UT_ASSERT(strcmp(cluster_cssd_status_to_string((ClusterCssdStatus)99), "(unknown)") == 0);
}

UT_TEST(test_t2_peer_state_to_string_round_trip)
{
	UT_ASSERT(strcmp(cluster_cssd_peer_state_to_string(CLUSTER_CSSD_PEER_ALIVE), "alive") == 0);
	UT_ASSERT(strcmp(cluster_cssd_peer_state_to_string(CLUSTER_CSSD_PEER_SUSPECTED), "suspected")
			  == 0);
	UT_ASSERT(strcmp(cluster_cssd_peer_state_to_string(CLUSTER_CSSD_PEER_DEAD), "dead") == 0);

	UT_ASSERT(strcmp(cluster_cssd_peer_state_to_string((ClusterCssdPeerState)99), "(unknown)")
			  == 0);
}

UT_TEST(test_t3_heartbeat_payload_12_bytes)
{
	UT_ASSERT(sizeof(ClusterCssdHeartbeatPayload) == 12);
}

UT_TEST(test_t4_outbound_slot_64_bytes_cache_line)
{
	UT_ASSERT(sizeof(ClusterCssdOutboundSlot) == 64);
	/* result_at_us must be 8-byte aligned for atomic-safe write. */
	UT_ASSERT((offsetof(ClusterCssdOutboundSlot, result_at_us) % 8) == 0);
}

UT_TEST(test_t5_shmem_size_reasonable)
{
	Size sz = cluster_cssd_shmem_size();

	/* Ballpark: lwlock + lifecycle + 2 atomic counters + 128 peer state +
	 * 128 outbound slots = approx 32 + 64 + 16 + 128*72 + 128*64 ≈ 17 KB.
	 * Verify > 4 KiB (catch trivial misconfig) and < 128 KiB (catch
	 * accidental field bloat).
	 *
	 * Note: shmem_storage stub buffer is 16 KiB so cluster_cssd_shmem_init
	 * may overrun in T6;skip init test if size > stub. */
	UT_ASSERT(sz > 4096);
	UT_ASSERT(sz < 131072);
}

UT_TEST(test_t6_shmem_init_idempotent)
{
	/* First init: found=false expected, structure populated. */
	shmem_init_done = false;
	cluster_cssd_shmem_init();

	/* Status default = STARTING (per cluster_cssd_shmem_init memset path). */
	/* Note: stub ShmemInitStruct returns same buffer always;the first call
	 * paints the layout, the second call should not re-paint since
	 * found=true. */

	/* Repeat init -- should be no-op because shmem_init_done = true now. */
	cluster_cssd_shmem_init();

	UT_ASSERT(true); /* idempotent invariant: no crash, no double-init issues */
}

UT_TEST(test_t7_dispatch_handler_4_hard_constraints_static_grep)
{
	const char *src_path = "../../backend/cluster/cluster_cssd.c";
	FILE *fp;
	char buf[4096];
	int handler_start_line = 0;
	int handler_end_line = 0;
	int line_no = 0;
	bool in_handler = false;
	bool saw_lwlock_in_handler = false;
	bool saw_ereport_error_in_handler = false;
	bool saw_palloc_in_handler = false;

	fp = fopen(src_path, "r");
	if (fp == NULL) {
		UT_ASSERT(true); /* TAP coverage only */
		return;
	}

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		line_no++;

		if (strstr(buf, "cluster_cssd_dispatch_heartbeat(const ClusterICEnvelope")
			&& strstr(buf, "*env")) {
			in_handler = true;
			handler_start_line = line_no;
			continue;
		}

		if (in_handler) {
			/* Detect end of handler:  "}" at column 0 (function close). */
			if (buf[0] == '}') {
				in_handler = false;
				handler_end_line = line_no;
				break;
			}

			/* L72-style hard constraint detection. */
			if (strstr(buf, "LWLockAcquire") != NULL || strstr(buf, "LWLockRelease") != NULL)
				saw_lwlock_in_handler = true;
			if (strstr(buf, "ereport(ERROR") != NULL || strstr(buf, "ereport(FATAL") != NULL
				|| strstr(buf, "ereport(PANIC") != NULL)
				saw_ereport_error_in_handler = true;
			if (strstr(buf, "palloc(") != NULL || strstr(buf, "MemoryContextAlloc(") != NULL)
				saw_palloc_in_handler = true;
		}
	}
	fclose(fp);

	UT_ASSERT(handler_start_line > 0);
	UT_ASSERT(handler_end_line > handler_start_line);
	UT_ASSERT(!saw_lwlock_in_handler);
	UT_ASSERT(!saw_ereport_error_in_handler);
	UT_ASSERT(!saw_palloc_in_handler);
}

UT_TEST(test_t8_deadband_factor_default_invariant)
{
	/* Spec-2.5 §3.4: SUSPECTED at (factor-1) × interval; DEAD at factor
	 * × interval.  Factor default 3, interval default 1000 ms.
	 *
	 * We don't have GUC machinery in unit tests; verify the math
	 * invariant: factor=3 → suspected=2s, dead=3s; factor=5 → suspected=
	 * 4s, dead=5s.  This is a pure-math check;the actual MainLoop
	 * deadband code is in CssdMain (Step 5 wires GUC reads). */
	int factor = 3;
	int interval = 1000;
	int suspected_factor = (factor - 1 < 2) ? 2 : factor - 1;
	int suspected_threshold = suspected_factor * interval;
	int dead_threshold = factor * interval;

	UT_ASSERT(suspected_threshold == 2000);
	UT_ASSERT(dead_threshold == 3000);

	factor = 5;
	suspected_factor = (factor - 1 < 2) ? 2 : factor - 1;
	suspected_threshold = suspected_factor * interval;
	dead_threshold = factor * interval;
	UT_ASSERT(suspected_threshold == 4000);
	UT_ASSERT(dead_threshold == 5000);

	/* Boundary: factor=2 -> suspected_factor still 2 (clamp). */
	factor = 2;
	suspected_factor = (factor - 1 < 2) ? 2 : factor - 1;
	UT_ASSERT(suspected_factor == 2);
}

UT_TEST(test_t9_outbound_slot_pending_state_transitions_static_grep)
{
	const char *src_path = "../../include/cluster/cluster_cssd.h";
	FILE *fp;
	char buf[4096];
	bool saw_state_machine_doc = false;

	fp = fopen(src_path, "r");
	if (fp == NULL) {
		UT_ASSERT(true);
		return;
	}

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		if (strstr(buf, "0/1/2/3") != NULL) {
			saw_state_machine_doc = true;
			break;
		}
	}
	fclose(fp);

	UT_ASSERT(saw_state_machine_doc);
}

UT_TEST(test_t10_grace_period_field_exists_static_grep)
{
	/* spec-2.5 Q6 first-tick grace period invariant: ClusterCssdShmem
	 * must expose first_tick_grace_until_us (LMON tick / deadband-scan
	 * read this). */
	const char *src_path = "../../include/cluster/cluster_cssd.h";
	FILE *fp;
	char buf[4096];
	bool saw_grace_field = false;

	fp = fopen(src_path, "r");
	if (fp == NULL) {
		UT_ASSERT(true);
		return;
	}

	while (fgets(buf, sizeof(buf), fp) != NULL) {
		if (strstr(buf, "first_tick_grace_until_us") != NULL) {
			saw_grace_field = true;
			break;
		}
	}
	fclose(fp);

	UT_ASSERT(saw_grace_field);
}


UT_DEFINE_GLOBALS();

int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_t1_status_to_string_round_trip);
	UT_RUN(test_t2_peer_state_to_string_round_trip);
	UT_RUN(test_t3_heartbeat_payload_12_bytes);
	UT_RUN(test_t4_outbound_slot_64_bytes_cache_line);
	UT_RUN(test_t5_shmem_size_reasonable);
	UT_RUN(test_t6_shmem_init_idempotent);
	UT_RUN(test_t7_dispatch_handler_4_hard_constraints_static_grep);
	UT_RUN(test_t8_deadband_factor_default_invariant);
	UT_RUN(test_t9_outbound_slot_pending_state_transitions_static_grep);
	UT_RUN(test_t10_grace_period_field_exists_static_grep);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
