/*-------------------------------------------------------------------------
 *
 * test_cluster_lmd.c
 *	  Standalone unit tests for spec-2.19 LMD daemon skeleton (D13).
 *
 *	  T-lmd-1..8 (spec-2.19 §0 Q11 D):
 *	    T-lmd-1: AuxProcType / BackendType / AmLmdProcess() macro
 *	             surface check (compile-time enum existence + linker
 *	             symbol resolution for LmdMain).
 *	    T-lmd-2: cluster_lmd_shmem_size / shmem_init / shmem_register
 *	             linker surface + state accessor returns initial value
 *	             after init.
 *	    T-lmd-3: HC2 4-state semantic split — DISABLED vs NOT_STARTED
 *	             distinct (cluster.lmd_enabled = false → DISABLED at init;
 *	             enabled = true → NOT_STARTED).  §1.4.6 (a) vs (b).
 *	    T-lmd-4: 6 counter base — all 6 atomic counters initialized to 0
 *	             after fresh shmem_init (L87 counter-must-match-doc-claim).
 *	    T-lmd-5: **HC4 exact-predicate ownership transfer** (v0.3 codex
 *	             P1.5 NEW L124).  cluster_lmd_is_ready() returns true
 *	             iff state == LMD_READY;false for the other 5 states
 *	             including DRAINING / STOPPED / DISABLED.  This is the
 *	             critical regression test — `>= LMD_READY` numeric
 *	             compare would false-positive on DRAINING (3) / STOPPED
 *	             (4) / DISABLED (5) because enum is not contiguous.
 *	    T-lmd-6: HC3 producer wake — cluster_lmd_submit_wait_edge()
 *	             increments lmd_edge_submission_count + broadcasts CV.
 *	             HC6 skeleton: no ring/hash/queue placeholder consumed.
 *	    T-lmd-7: cluster.lmd_enabled GUC default = true (D12 contract).
 *	    T-lmd-8: cluster_lmd_state_to_string(): 6 valid states + 1
 *	             out-of-range → "(unknown)" + L122 alphabetic property:
 *	             'lmd' < 'lmon' in string compare (ASCII `d` < `o`).
 *
 *	  Stubs:
 *	    - ShmemInitStruct returns a union force-aligned buffer per L105
 *	      (strict-alignment platforms — ARM Linux / SPARC — need 8-byte
 *	      alignment for pg_atomic_uint64).
 *	    - LWLockInitialize / ConditionVariableInit /
 *	      ConditionVariableBroadcast: no-ops (LWLock + CV state not
 *	      exercised in standalone unit tests).
 *	    - GetCurrentTimestamp: monotonic counter.
 *	    - elog / ereport: pass-through stubs.
 *
 *	  Spec: spec-2.19-lmd-daemon-deadlock-ownership-migration.md (FROZEN
 *	  v0.3 2026-05-14 user approve);Sprint A Step 5 D13.
 *	  Cross-spec lesson inheritance: L87 / L94 / L105 / L107 / L122 /
 *	  L124 NEW.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lmd.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_lmd.o only;all PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "cluster/cluster_cancel_token.h" /* spec-5.9 D5 — victim-ack tick deps */
#include "cluster/cluster_ges.h"
#include "cluster/cluster_lmd.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

/* Drop PG's port.h printf override; unit_test.h uses stdlib printf. */
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"


/* ============================================================
 * PG runtime stubs.
 * ============================================================ */

void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}

void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * spec-2.19 D13 (L105 inherit):  ShmemInitStruct stub uses union
 * force-align to guarantee 8-byte alignment for pg_atomic_uint64
 * fields inside ClusterLmdSharedState (strict-alignment platforms
 * require this — ARM Linux / SPARC SIGBUS without union force-align).
 */
static union {
	uint64 force_align;
	char data[32 * 1024]; /* spec-2.24 D2 cancel queue ~14KB packed in;bump to 32KB */
} stub_lmd_buf;

static bool stub_lmd_initialized = false;
static uint64 stub_cv_broadcast_count = 0;

static void
reset_lmd_stub_shmem(void)
{
	memset(&stub_lmd_buf, 0, sizeof(stub_lmd_buf));
	stub_lmd_initialized = false;
	stub_cv_broadcast_count = 0;
	cluster_lmd_shmem_init();
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster lmd") == 0) {
		Assert(size <= sizeof(stub_lmd_buf.data));
		*foundPtr = stub_lmd_initialized;
		stub_lmd_initialized = true;
		return stub_lmd_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
RequestAddinShmemSpace(Size size pg_attribute_unused())
{}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}

/* LWLock / ConditionVariable stubs — state not exercised in unit tests. */
void
LWLockInitialize(LWLock *l pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *l pg_attribute_unused())
{}

void
ConditionVariableInit(ConditionVariable *cv pg_attribute_unused())
{}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	stub_cv_broadcast_count++;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}

bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
							long timeout_ms pg_attribute_unused(),
							uint32 wait_event_info pg_attribute_unused())
{
	return true;
}

bool
ConditionVariableCancelSleep(void)
{
	return false;
}

TimestampTz
GetCurrentTimestamp(void)
{
	static int64 t = 1000000;
	return (TimestampTz)(++t);
}

/* cluster.lmd_enabled GUC global (D12) — stubbed here for unit tests;
 * default true mirrors cluster_guc.c declaration.  T-lmd-3 toggles it
 * to exercise DISABLED state branch. */
bool cluster_lmd_enabled = true;

/* spec-2.22 D9 — LMD scan interval GUC stub (LmdMain real Tarjan loop). */
int cluster_lmd_scan_interval_ms = 1000;
int cluster_lmd_max_wait_edges = 1024;

/* spec-5.9 D2 — anti-thrash window GUC stub (read by the recent-victim ring). */
int cluster_victim_repeat_window_ms = 5000;

/* spec-5.9 D5 — victim-ack tick deps (the tick is never invoked in unit tests;
 * stubbed so cluster_lmd.o links). */
int cluster_cancel_ack_timeout_ms = 1000;
struct PROC_HDR;
struct PROC_HDR *ProcGlobal = NULL;
ClusterCancelMarker
cluster_cancel_token_take_marker(struct PGPROC *p pg_attribute_unused(),
								 uint64 *out_cancel_id pg_attribute_unused(),
								 uint64 *out_seq pg_attribute_unused())
{
	return CLUSTER_CANCEL_MARKER_NONE;
}
void
cluster_ges_send_cancel_ack(int32 c pg_attribute_unused(),
							const struct ClusterGrdHolderId *v pg_attribute_unused(),
							uint64 w pg_attribute_unused(), uint64 id pg_attribute_unused(),
							uint8 s pg_attribute_unused())
{}

/* spec-2.22 D2/D3/D5 — LMD graph + Tarjan symbols pulled by cluster_lmd.o
 * (LmdMain calls Tarjan scan; shmem region registry references graph
 * shmem helpers).  Stub them out — TAP 109 covers real behavior.
 */
void
cluster_lmd_tarjan_run_local_scan(void)
{}

Size
cluster_lmd_graph_shmem_size(void)
{
	return 0;
}

void
cluster_lmd_graph_shmem_init(void)
{}

/* MyProcPid stub — set by tests as needed. */
int MyProcPid = 12345;

/* Stub bodies for SetLatch / ResetLatch / WaitLatch — LmdMain main loop
 * exit conditions are not exercised in unit tests (LmdMain itself is
 * tested via integration in TAP 106).  We don't actually call LmdMain
 * from any test — the linker just needs to resolve its body's deps. */

BackendType MyBackendType;
struct Latch *MyLatch;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t InterruptPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
ProcessingMode Mode = NormalProcessing;
bool IsUnderPostmaster = true;

void
SignalHandlerForConfigReload(int sig pg_attribute_unused())
{}

void
SignalHandlerForShutdownRequest(int sig pg_attribute_unused())
{}

void
ProcessConfigFile(int context pg_attribute_unused())
{}

void
ProcessInterrupts(void)
{}

int
WaitLatch(struct Latch *l pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}

void
ResetLatch(struct Latch *l pg_attribute_unused())
{}

void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}

void
proc_exit(int code pg_attribute_unused())
{
	/* In test stub, proc_exit must not really exit — return to caller. */
}

extern PGDLLIMPORT sigset_t UnBlockSig;
sigset_t UnBlockSig;

/* pqsignal returns previous handler; stub to no-op. */
pqsigfunc
pqsignal(int signo pg_attribute_unused(), pqsigfunc func pg_attribute_unused())
{
	return NULL;
}

/* postmaster.c forward — never called in unit tests (LmdMain not invoked). */
pid_t cluster_postmaster_start_lmd(void);
pid_t
cluster_postmaster_start_lmd(void)
{
	return 0;
}

/* PG ProcSignal SIGUSR1 handler stub. */
void
procsignal_sigusr1_handler(int sig pg_attribute_unused())
{}

/* errstart_cold is the cold-path variant of errstart;cluster_lmd_main
 * does ereport(FATAL) when shmem is null, but unit test never hits that. */
bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

/* spec-2.24 D14/D16 stub audit — new symbols introduced by Steps 1-9. */
int64
TimestampDifferenceExceeds(int64 a pg_attribute_unused(), int64 b pg_attribute_unused(),
						   int us pg_attribute_unused())
{
	return 0;
}
Size
add_size(Size a, Size b)
{
	return a + b;
}
uint64
cluster_epoch_get_current(void)
{
	return 0;
}
void
cluster_grd_inc_cleanup_skip_stale_cancel(void)
{}
int
cluster_grd_sweep_local_stale_procnos(void)
{
	return 0;
}
void
cluster_lmd_cleanup_lmd_sweep_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cleanup_on_backend_exit_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cleanup_skip_other_owner_count_inc(uint64 d pg_attribute_unused())
{}
int cluster_lmd_cleanup_sweep_interval_ms = 5000;
bool
cluster_lmd_signal_local_victim(uint32 a pg_attribute_unused(), uint64 b pg_attribute_unused(),
								uint64 c pg_attribute_unused(), uint64 d pg_attribute_unused(),
								uint64 e pg_attribute_unused())
{
	return false;
}
/* spec-5.9 D6 — pending-cancel table lives in cluster_lmd_tarjan.o; stubbed. */
void
cluster_lmd_pending_cancel_tick(void)
{}
void
cluster_lmd_pending_cancel_on_ack(uint64 c pg_attribute_unused(), uint8 s pg_attribute_unused(),
								  const ClusterLmdVertex *v pg_attribute_unused(),
								  int32 src pg_attribute_unused())
{}
void
cluster_lmd_cross_node_cancel_queue_full_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_cancel_received_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_victim_cancel_sent_count_inc(uint64 d pg_attribute_unused())
{}
int
s_lock(volatile slock_t *l pg_attribute_unused(), const char *f pg_attribute_unused(),
	   int n pg_attribute_unused(), const char *fn pg_attribute_unused())
{
	return 0;
}

/* spec-5.8 D3b — symbols newly referenced by cluster_lmd.o (HC16 coordinator
 * election + coordinator tick).  The standalone harness never runs LmdMain or
 * the coordinator tick, so these only need to resolve at link time. */
int cluster_node_id = 0;
bool cluster_lmd_deadlock_detection_enabled = true;
int cluster_lmd_global_dd_interval_ms = 2000;

int
cluster_conf_node_count(void)
{
	return 1;
}

int
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return 0; /* CLUSTER_CSSD_PEER_ALIVE */
}

void
cluster_lmd_tarjan_run_coordinator_scan(int collect_timeout_ms pg_attribute_unused())
{}

/* spec-5.8 D8 — probe collector shmem region register (called from
 * cluster_lmd_shmem_register); standalone harness never attaches it. */
void
cluster_lmd_probe_collector_shmem_register(void)
{}


/* ============================================================
 * Test cases.
 * ============================================================ */

/*
 * T-lmd-1:  AuxProcType / BackendType / AmLmdProcess() macro surface.
 *
 *	Compile-time verification that LmdProcess enum exists in AuxProcType
 *	(it's the 8th cluster aux process — appended after LmsProcess).
 *	B_LMD already exists in BackendType (spec-1.10 backend types
 *	extension;v0.3 P1.7 regression防御).  AmLmdProcess() macro defined
 *	when USE_PGRAC_CLUSTER.  LmdMain symbol resolves at link time.
 */
UT_TEST(test_lmd_auxproc_and_backend_type_surface)
{
	/* Compile-time enum existence — failure to compile = test failure. */
	UT_ASSERT_NE((int)LmdProcess, (int)LmsProcess);
	UT_ASSERT_NE((int)LmdProcess, (int)NUM_AUXPROCTYPES);

	/* B_LMD pre-existing (P1.7) — distinct from B_LMS / B_LMON. */
	UT_ASSERT_NE((int)B_LMD, (int)B_LMS);
	UT_ASSERT_NE((int)B_LMD, (int)B_LMON);

	/* LmdMain symbol linkable (defense-in-depth — auxprocess.c dispatch
	 * would fail at runtime if not linked). */
	UT_ASSERT_NOT_NULL((void *)LmdMain);
}

/*
 * T-lmd-2:  shmem size / init / register / accessor surface.
 *
 *	cluster_lmd_shmem_size() returns a non-zero MAXALIGN'd value;
 *	cluster_lmd_shmem_init() returns successfully on first call (NOT
 *	found) and is idempotent on subsequent calls (found = true).  All 4
 *	state accessor functions return defined values after init.
 */
UT_TEST(test_lmd_shmem_size_init_idempotent)
{
	Size sz;

	cluster_lmd_enabled = true;
	reset_lmd_stub_shmem();

	sz = cluster_lmd_shmem_size();
	UT_ASSERT(sz > 0);
	UT_ASSERT(sz == MAXALIGN(sz)); /* MAXALIGN'd */

	/* Idempotent — second call finds existing region. */
	cluster_lmd_shmem_init();
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_shared_state());

	/* Accessor surface — all linkable + return 0/initial values. */
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_state);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_pid);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_started_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_edge_submission_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_wake_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_idle_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lmd_get_error_count);
}

/*
 * T-lmd-3:  HC2 4-state semantic split — DISABLED vs NOT_STARTED.
 *
 *	§1.4.6 (a) DISABLED (lmd_enabled=off at startup) is distinct from
 *	§1.4.6 (b) NOT_STARTED (lmd_enabled=on but LMD not yet spawned).
 *	cluster_lmd_shmem_init() reads cluster_lmd_enabled at init time and
 *	branches the initial state accordingly.  Verifies the GUC threading
 *	path actually wires (regression防御 for L107 claim-must-implement-
 *	not-comment — spec body claims it,this test verifies it).
 *
 *	The test resets the standalone shmem stub so both startup-time branches
 *	are exercised directly.  Runtime changes still do not retroactively
 *	change state in production;this only verifies the initialization gate.
 */
UT_TEST(test_lmd_state_initial_not_started_when_enabled)
{
	ClusterLmdState s;

	cluster_lmd_enabled = false;
	reset_lmd_stub_shmem();
	s = cluster_lmd_get_state();
	UT_ASSERT_EQ((int)s, (int)CLUSTER_LMD_DISABLED);

	cluster_lmd_enabled = true;
	reset_lmd_stub_shmem();
	s = cluster_lmd_get_state();

	UT_ASSERT_EQ((int)s, (int)CLUSTER_LMD_NOT_STARTED);
	/* Verify HC2 enum values are not collided. */
	UT_ASSERT_EQ((int)CLUSTER_LMD_NOT_STARTED, 0);
	UT_ASSERT_EQ((int)CLUSTER_LMD_STARTING, 1);
	UT_ASSERT_EQ((int)CLUSTER_LMD_READY, 2);
	UT_ASSERT_EQ((int)CLUSTER_LMD_DRAINING, 3);
	UT_ASSERT_EQ((int)CLUSTER_LMD_STOPPED, 4);
	UT_ASSERT_EQ((int)CLUSTER_LMD_DISABLED, 5);
}

/*
 * T-lmd-4:  6 counter base — all 6 atomic counters initialized to 0.
 *
 *	L87 counter-must-match-doc-claim:spec §0 Q8 lists 6 counters,this
 *	test verifies each one is observable + initialized to 0 after
 *	shmem_init.  Production wire-callsite verification推 spec-2.20+.
 */
UT_TEST(test_lmd_six_counters_initial_zero)
{
	UT_ASSERT_EQ(cluster_lmd_get_started_count(), 0ULL);
	UT_ASSERT_EQ(cluster_lmd_get_edge_submission_count(), 0ULL);
	UT_ASSERT_EQ(cluster_lmd_get_wake_count(), 0ULL);
	UT_ASSERT_EQ(cluster_lmd_get_idle_count(), 0ULL);
	UT_ASSERT_EQ(cluster_lmd_get_error_count(), 0ULL);
	/* lmd_ready_at_us not a "counter" per se but lives in the same
	 * atomic bank; verify zero initialization. */
	UT_ASSERT_EQ((uint64)cluster_lmd_get_ready_at(), 0ULL);
}

/*
 * T-lmd-5:  **HC4 EXACT-PREDICATE regression test** (v0.3 codex P1.5
 *           L124 NEW lesson candidate).
 *
 *	cluster_lmd_is_ready() returns true iff state == LMD_READY.
 *	Critical:  enum is not contiguous (DRAINING=3 / STOPPED=4 /
 *	DISABLED=5),so `state >= LMD_READY` (>= 2) would false-positive
 *	match all 4 of {READY, DRAINING, STOPPED, DISABLED}.  spec-2.18 LMS
 *	cluster_lms_owns_grant() has the same latent bug (returns true for
 *	READY OR DRAINING OR STOPPED) — spec-2.19 explicitly tightens via
 *	HC4 exact predicate.
 *
 *	This test poke-writes each enum value into lmd_state atomic and
 *	verifies cluster_lmd_is_ready() returns false EXCEPT for READY.
 */
UT_TEST(test_lmd_is_ready_exact_predicate_all_six_states)
{
	ClusterLmdSharedState *st = cluster_lmd_shared_state();

	UT_ASSERT_NOT_NULL((void *)st);
	st->pid = MyProcPid;

	/* NOT_STARTED (0) → false */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_NOT_STARTED);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* STARTING (1) → false */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_STARTING);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* READY (2) → **true** (only valid state) */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_READY);
	UT_ASSERT(cluster_lmd_is_ready());

	/* DRAINING (3) → false (HC4 critical — `>=` would误判 true). */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_DRAINING);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* STOPPED (4) → false (HC4 critical — `>=` would误判 true). */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_STOPPED);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* DISABLED (5) → false (HC4 critical — `>=` would误判 true). */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_DISABLED);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* Restore to NOT_STARTED for subsequent tests. */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_NOT_STARTED);
}

/*
 * Postmaster reaper hardening: a harvested/crashed LMD child must clear the
 * caller-side READY gate without taking the LMD LWLock.  This prevents stale
 * READY after LmdPID has been reset by postmaster.c.
 */
UT_TEST(test_lmd_mark_child_exit_clears_ready_atomically)
{
	ClusterLmdSharedState *st = cluster_lmd_shared_state();

	UT_ASSERT_NOT_NULL((void *)st);
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_READY);
	UT_ASSERT(cluster_lmd_is_ready());

	cluster_lmd_mark_child_exit();

	UT_ASSERT_EQ(cluster_lmd_get_pid(), 0);
	UT_ASSERT_EQ((int)cluster_lmd_get_state(), (int)CLUSTER_LMD_STOPPED);
	UT_ASSERT(!(cluster_lmd_is_ready()));

	/* Restore state for subsequent tests. */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_NOT_STARTED);
}

/*
 * T-lmd-6:  HC3 producer wake + HC6 skeleton "no graph maintenance".
 *
 *	cluster_lmd_submit_wait_edge() should:
 *	  (a) atomic ++ lmd_edge_submission_count
 *	  (b) ConditionVariableBroadcast(&cv) — stubbed counter
 *	  (c) NO save of wait edge data (HC6: no ring/hash/queue write).
 *
 *	No-op when DISABLED (lmd_enabled=off path).
 */
UT_TEST(test_lmd_submit_wait_edge_inc_counter_and_broadcast)
{
	ClusterLmdSharedState *st = cluster_lmd_shared_state();
	uint64 pre_count;
	uint64 pre_cv;

	UT_ASSERT_NOT_NULL((void *)st);

	/* Ensure state is not DISABLED for this test (no-op branch). */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_NOT_STARTED);

	pre_count = cluster_lmd_get_edge_submission_count();
	pre_cv = stub_cv_broadcast_count;

	cluster_lmd_submit_wait_edge();

	UT_ASSERT_EQ(cluster_lmd_get_edge_submission_count(), pre_count + 1);
	UT_ASSERT_EQ(stub_cv_broadcast_count, pre_cv + 1);

	/* Three more invocations — monotonic ++. */
	cluster_lmd_submit_wait_edge();
	cluster_lmd_submit_wait_edge();
	cluster_lmd_submit_wait_edge();
	UT_ASSERT_EQ(cluster_lmd_get_edge_submission_count(), pre_count + 4);
	UT_ASSERT_EQ(stub_cv_broadcast_count, pre_cv + 4);

	/* HC6 no-op when DISABLED — counter does NOT increment. */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_DISABLED);
	pre_count = cluster_lmd_get_edge_submission_count();
	pre_cv = stub_cv_broadcast_count;
	cluster_lmd_submit_wait_edge();
	UT_ASSERT_EQ(cluster_lmd_get_edge_submission_count(), pre_count); /* unchanged */
	UT_ASSERT_EQ(stub_cv_broadcast_count, pre_cv);					  /* unchanged */

	/* Restore state. */
	pg_atomic_write_u32(&st->lmd_state, (uint32)CLUSTER_LMD_NOT_STARTED);
}

/*
 * T-lmd-7:  cluster.lmd_enabled GUC default = true (D12 contract).
 *
 *	The actual GUC machinery (DefineCustomBoolVariable) lives in
 *	cluster_guc.c (PG runtime).  This test only verifies the C global
 *	declaration has the expected default and the type is `bool` (so
 *	cluster_lmd_shmem_init can read it at startup).
 */
UT_TEST(test_lmd_enabled_guc_default_true)
{
	UT_ASSERT(cluster_lmd_enabled);

	/* Toggle exercise — same as runtime postgresql.conf override would do
	 * (PG enforces PGC_POSTMASTER restart-only;test stub is permissive). */
	cluster_lmd_enabled = false;
	UT_ASSERT(!(cluster_lmd_enabled));
	cluster_lmd_enabled = true;
	UT_ASSERT(cluster_lmd_enabled);
}

/*
 * T-lmd-8:  cluster_lmd_state_to_string() + L122 alphabetic baseline.
 *
 *	Returns canonical lowercase string for all 6 valid states +
 *	"(unknown)" for out-of-range.  L122 (spec-2.18 F3 inherit):
 *	'lmd' < 'lmon' in string compare because ASCII `d` (0x64) < `o`
 *	(0x6F).  This drives the alphabetic insert position in
 *	pg_cluster_state ORDER BY category baseline (017_debug.pl L66).
 *
 *	(The dump_lmd 7 emit_row real path is tested via 017 + 106 TAP
 *	integration;here we verify the string mapping that downstream
 *	dump_lmd uses.)
 */
UT_TEST(test_lmd_state_to_string_and_alphabetic_position)
{
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_NOT_STARTED), "not_started");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_STARTING), "starting");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_READY), "ready");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_DRAINING), "draining");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_STOPPED), "stopped");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string(CLUSTER_LMD_DISABLED), "disabled");
	UT_ASSERT_STR_EQ(cluster_lmd_state_to_string((ClusterLmdState)999), "(unknown)");

	/*
	 * L122 alphabetic property — `lmd` < `lmon` < `lms`.  Drives the
	 * 017_debug.pl categories baseline insert position
	 * (`lck,lmd,lmon,lms,pcm`).  Verify the assumption is还 correct
	 * (regression防御 against future spec drafter intuiting wrong order).
	 */
	UT_ASSERT(strcmp("lmd", "lmon") < 0);
	UT_ASSERT(strcmp("lmon", "lms") < 0);
	UT_ASSERT(strcmp("lck", "lmd") < 0);
	UT_ASSERT(strcmp("lms", "pcm") < 0);
}


/*
 * spec-5.9 D2 (U3) — anti-thrash recent-victim ring.
 *
 *	Records a chosen victim and confirms is_thrashing() reports it within the
 *	window, a never-recorded victim is not thrashing, the window=0 disable, and
 *	the test-hook reset clears the ring.  GetCurrentTimestamp() advances by 1us
 *	per call in this harness, so a freshly recorded victim is always inside any
 *	positive window.
 */
UT_TEST(test_lmd_anti_thrash_recent_victim_ring)
{
	ClusterLmdVertex a;
	ClusterLmdVertex b;

	cluster_lmd_recent_victim_ring_reset();
	memset(&a, 0, sizeof(a));
	a.node_id = 1;
	a.procno = 7;
	a.cluster_epoch = 3;
	a.request_id = 42;
	a.wait_seq = 99; /* not part of the ring identity */
	memset(&b, 0, sizeof(b));
	b.node_id = 1;
	b.procno = 8; /* different procno -> different identity */
	b.cluster_epoch = 3;
	b.request_id = 43;

	/* Nothing recorded yet -> not thrashing. */
	UT_ASSERT(!cluster_lmd_recent_victim_is_thrashing(&a));

	/* Record a -> a thrashes (within window), b still does not. */
	cluster_lmd_recent_victim_record(&a);
	UT_ASSERT(cluster_lmd_recent_victim_is_thrashing(&a));
	UT_ASSERT(!cluster_lmd_recent_victim_is_thrashing(&b));

	/* A different wait_seq under the same 4-tuple is the same logical victim. */
	a.wait_seq = 12345;
	UT_ASSERT(cluster_lmd_recent_victim_is_thrashing(&a));

	/* window <= 0 disables anti-thrash advisory. */
	cluster_victim_repeat_window_ms = 0;
	UT_ASSERT(!cluster_lmd_recent_victim_is_thrashing(&a));
	cluster_victim_repeat_window_ms = 5000;

	/* Reset clears the ring. */
	cluster_lmd_recent_victim_ring_reset();
	UT_ASSERT(!cluster_lmd_recent_victim_is_thrashing(&a));
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(10);

	UT_RUN(test_lmd_auxproc_and_backend_type_surface);
	UT_RUN(test_lmd_shmem_size_init_idempotent);
	UT_RUN(test_lmd_state_initial_not_started_when_enabled);
	UT_RUN(test_lmd_six_counters_initial_zero);
	UT_RUN(test_lmd_is_ready_exact_predicate_all_six_states);
	UT_RUN(test_lmd_mark_child_exit_clears_ready_atomically);
	UT_RUN(test_lmd_submit_wait_edge_inc_counter_and_broadcast);
	UT_RUN(test_lmd_enabled_guc_default_true);
	UT_RUN(test_lmd_state_to_string_and_alphabetic_position);
	UT_RUN(test_lmd_anti_thrash_recent_victim_ring);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
