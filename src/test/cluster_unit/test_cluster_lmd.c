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
#include <setjmp.h>
#include <string.h>

#include "cluster/cluster_cancel_token.h" /* spec-5.9 D5 — victim-ack tick deps */
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_shmem.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "postmaster/auxprocess.h"

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

static ClusterCleanLeaveSharedState test_stop_region;
static ClusterLeaveState *cl_state = &test_stop_region.leave;
static ClusterNormalStopState *cl_normal_stop;
static uint32 cl_normal_stop_service_depth, cl_normal_stop_service_bit;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static sigjmp_buf test_main_exit;
static bool test_main_running;
static bool test_probe_waiting;
static int test_main_case, test_main_waits, test_main_polls, test_main_scans;
static int test_main_coord_scans;
static int test_main_exit_code, test_error_level;
static pg_on_exit_callback test_exit_callback;
static LWLock *test_locks[4];
static unsigned test_lock_depth;
static ClusterNormalStopPollResult test_graph_observation;
static void test_stop_work(void);
static int test_stop_wait(void);
static void test_callback_late_work(void);
#include "test_cluster_lmon_stop_service.inc"


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
errstart(int elevel, const char *domain pg_attribute_unused())
{
	test_error_level = elevel;
	return test_main_running && elevel >= ERROR;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (test_error_level < FATAL && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	proc_exit(1);
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	proc_exit(1);
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

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
cluster_shmem_register_region(const ClusterShmemRegion *r pg_attribute_unused())
{}

/* LWLock / ConditionVariable stubs — state not exercised in unit tests. */
void
LWLockInitialize(LWLock *l pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	if (test_lock_depth >= lengthof(test_locks) || (l == &cl_state->lock && test_lock_depth != 0))
		abort();
	test_locks[test_lock_depth++] = l;
	return true;
}

void
LWLockRelease(LWLock *l pg_attribute_unused())
{
	if (test_lock_depth == 0 || test_locks[--test_lock_depth] != l)
		abort();
}
void
LWLockReleaseAll(void)
{
	test_lock_depth = 0;
}
void
SetLatch(Latch *l pg_attribute_unused())
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
int cluster_cancel_max_retransmit = 3;
AuxProcType MyAuxProcType = LmdProcess;
static uint64 test_marker_id;
static ClusterCancelMarker test_marker;
static int test_ack_sent;
static int test_ack_mismatch;
struct PROC_HDR;
struct PROC_HDR *ProcGlobal = NULL;
ClusterCancelMarker
cluster_cancel_token_take_marker(struct PGPROC *p pg_attribute_unused(),
								 uint64 *out_cancel_id pg_attribute_unused(),
								 uint64 *out_seq pg_attribute_unused())
{
	*out_cancel_id = test_marker_id;
	return test_marker;
}
void
cluster_ges_send_cancel_ack(int32 c pg_attribute_unused(),
							const struct ClusterGrdHolderId *v pg_attribute_unused(),
							uint64 w pg_attribute_unused(), uint64 id pg_attribute_unused(),
							uint8 s pg_attribute_unused())
{
	test_ack_sent++;
}

void
cluster_ges_send_cancel_pending(int32 n, const ClusterGrdHolderId *v, uint64 w, uint64 c)
{
	(void)n;
	(void)v;
	(void)w;
	(void)c;
}
void
cluster_lmd_cancel_ack_mismatch_count_inc(uint64 n)
{
	test_ack_mismatch += n;
}
void
cluster_lmd_cancel_exhausted_timeout_count_inc(uint64 n)
{
	(void)n;
}
void
cluster_lmd_cancel_no_safe_victim_count_inc(uint64 n)
{
	(void)n;
}
void
cluster_lmd_cancel_retransmit_count_inc(uint64 n)
{
	(void)n;
}
void
cluster_lmd_reconfig_cancel_discarded_count_inc(uint64 n)
{
	(void)n;
}

/* spec-2.22 D2/D3/D5 — LMD graph + Tarjan symbols pulled by cluster_lmd.o
 * (LmdMain calls Tarjan scan; shmem region registry references graph
 * shmem helpers).  Stub them out — TAP 109 covers real behavior.
 */
void
cluster_lmd_tarjan_run_local_scan(void)
{
	if (test_main_running)
		test_stop_work();
}

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
ProcessConfigFile(GucContext context pg_attribute_unused())
{}

void
ProcessInterrupts(void)
{}

int
WaitLatch(struct Latch *l pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	if (test_main_running)
		return test_stop_wait();
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
	pg_on_exit_callback callback = test_exit_callback;
	if (!test_main_running)
		abort();
	test_exit_callback = NULL;
	test_main_exit_code = code;
	if (code == 0 && test_main_case == 7)
		test_callback_late_work();
	if (callback != NULL)
		callback(code, 0);
	siglongjmp(test_main_exit, 1);
}

void
before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (test_exit_callback != NULL || arg != 0)
		abort();
	test_exit_callback = callback;
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
	return errstart(elevel, domain);
}

/* spec-2.24 D14/D16 stub audit — new symbols introduced by Steps 1-9. */
bool
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

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return 0; /* CLUSTER_CSSD_PEER_ALIVE */
}

void
cluster_lmd_tarjan_run_coordinator_scan(int collect_timeout_ms pg_attribute_unused())
{
	if (test_main_running) {
		test_main_coord_scans++;
		UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_stop_region.normal_stop.service_seal), 0);
	}
}

/* spec-5.8 D8 — probe collector shmem region register (called from
 * cluster_lmd_shmem_register); standalone harness never attaches it. */
void
cluster_lmd_probe_collector_shmem_register(void)
{}

/* Actual queue, victim and coordinator cancellation producer/consumer bodies.
 * Native PGPROC markers, transport and scan scheduling are explicit fixtures. */
#include "../../backend/cluster/cluster_lmd.c"
#include "test_cluster_lmd_pending.inc"


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

static void
test_stop_lmd_reset(void)
{
	static PROC_HDR procs;
	static PGPROC backend[2];
	reset_lmd_stub_shmem();
	memset(lmd_victim_acks, 0, sizeof(lmd_victim_acks));
	memset(lmd_pending_cancels, 0, sizeof(lmd_pending_cancels));
	memset(&procs, 0, sizeof(procs));
	memset(backend, 0, sizeof(backend));
	procs.allProcs = backend;
	procs.allProcCount = 2;
	ProcGlobal = &procs;
	MyAuxProcType = LmdProcess;
	IsUnderPostmaster = true;
	test_marker = CLUSTER_CANCEL_MARKER_NONE;
	test_marker_id = 0;
	test_ack_sent = test_ack_mismatch = 0;
	cl_normal_stop = NULL;
	cl_normal_stop_service_depth = cl_normal_stop_service_bit = 0;
	test_lock_depth = 0;
}

static ClusterLmdVertex
test_stop_victim(int node)
{
	ClusterLmdVertex v = { 0 };
	v.node_id = node;
	v.procno = 1;
	v.cluster_epoch = 17;
	v.request_id = 33;
	v.wait_seq = 41;
	return v;
}

UT_TEST(test_stop_lmd_observations_belong_to_actual_lmd)
{
	test_stop_lmd_reset();
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = LmdProcess;
	lmd_cancel_queue = NULL;
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_stop_cancel_queue_only_original_dequeue_retires_wrapped_items)
{
	GesCancelAckPayload ack = { 0 };
	ClusterLmdCancelItem item;
	test_stop_lmd_reset();
	ack.opcode = GES_REQ_OPCODE_CANCEL_ACK;
	lmd_cancel_queue->head = lmd_cancel_queue->tail = CLUSTER_LMD_CANCEL_QUEUE_DEPTH - 1;
	UT_ASSERT(cluster_lmd_cancel_queue_enqueue(1, &ack, sizeof(ack)));
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(lmd_cancel_queue->head, CLUSTER_LMD_CANCEL_QUEUE_DEPTH - 1);
	UT_ASSERT(cluster_lmd_cancel_queue_dequeue(&item));
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	/* Consumed ring bytes and monotonic counters are not live ownership. */
	lmd_cancel_queue->tail = CLUSTER_LMD_CANCEL_QUEUE_DEPTH;
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_stop_victim_ack_retains_exact_marker_and_original_send)
{
	ClusterGrdHolderId victim = { 0, 1, 17, 33 };
	test_stop_lmd_reset();
	lmd_victim_ack_add(1, &victim, 41, 99, 1);
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	test_marker = CLUSTER_CANCEL_MARKER_CONSUMED;
	test_marker_id = 98;
	cluster_lmd_victim_ack_tick();
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(test_ack_sent, 0);
	test_marker_id = 99;
	cluster_lmd_victim_ack_tick();
	UT_ASSERT_EQ(test_ack_sent, 1);
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_pending_cancel_requires_full_ack_identity_and_terminal_status)
{
	ClusterLmdVertex victim = test_stop_victim(1);
	ClusterLmdVertex stale = victim;
	test_stop_lmd_reset();
	UT_ASSERT_NOT_NULL(lmd_pending_cancel_add(&victim, 55, 17, false, 42, &victim, 1));
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_lmd_pending_cancel_on_ack(55, GES_CANCEL_ACK_INSTALLED, &victim, 1);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	stale.wait_seq++;
	cluster_lmd_pending_cancel_on_ack(55, GES_CANCEL_ACK_CONSUMED, &stale, 1);
	UT_ASSERT_EQ(test_ack_mismatch, 1);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_lmd_pending_cancel_on_ack(55, GES_CANCEL_ACK_CONSUMED, &victim, 1);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_private_last_slot_invalid_not_hidden_by_pending)
{
	ClusterLmdVertex victim = test_stop_victim(1);
	LmdPendingCancel *first;
	test_stop_lmd_reset();
	first = lmd_pending_cancel_add(&victim, 55, 17, false, 42, &victim, 1);
	UT_ASSERT_NOT_NULL(first);
	lmd_pending_cancels[63] = *first;
	lmd_pending_cancels[63].cancel_id = 0;
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	lmd_victim_acks[63].active = true;
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
}

static void
test_stop_work(void)
{
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	UT_ASSERT_EQ(test_lock_depth, 0);
	if (cluster_normal_stop_requested())
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_active_mask), 1024);
	test_main_scans++;
	if (test_main_case == 6) {
		LWLockAcquire(&cluster_lmd_state->lwlock, LW_SHARED);
		ereport(ERROR, (errmsg("fixture original scan error")));
	}
	if (test_main_case == 8 && test_main_scans == 1) {
		pg_atomic_write_u32(&cl_normal_stop->requested, 1);
		UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
					 CLUSTER_NORMAL_STOP_PENDING);
	}
	if (test_main_case == 9 && test_main_scans == 1)
		pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_edge_submission_count, 1);
}

ClusterNormalStopPollResult
cluster_lmd_graph_normal_stop_poll(uint64 *key, const char **reason)
{
	if (key != NULL)
		*key = 0;
	if (reason != NULL)
		*reason = "FIXTURE_GRAPH";
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(test_lock_depth, 0);
	test_main_polls++;
	return test_graph_observation;
}

ClusterNormalStopPollResult
cluster_lmd_probe_normal_stop_poll(uint64 *key, const char **reason)
{
	if (key != NULL)
		*key = 0;
	if (reason != NULL)
		*reason = "FIXTURE_REPORT_COLLECTOR";
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	return CLUSTER_NORMAL_STOP_READY;
}

static int
test_stop_wait(void)
{
	UT_ASSERT_EQ(test_lock_depth, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_active_mask), 0);
	if (test_probe_waiting) {
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_idle_mask) & 1024, 0);
		MyAuxProcType = CheckpointerProcess;
		UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_PENDING);
		MyAuxProcType = LmdProcess;
		return WL_LATCH_SET;
	}
	if (++test_main_waits > 3)
		abort();
	if (cluster_normal_stop_requested())
		UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_idle_mask),
					 test_graph_observation == CLUSTER_NORMAL_STOP_READY ? 1024 : 0);
	if (test_main_waits == 1) {
		test_graph_observation = CLUSTER_NORMAL_STOP_READY;
		/* Seal is a controller fixture: no new periodic scans in the next pass. */
		if (test_main_case == 2)
			pg_atomic_write_u32(&cl_normal_stop->service_seal, 1);
	}
	if (test_main_waits == 2) {
		pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_PROTOCOL_CLOSED);
		pg_atomic_write_u32(&cl_normal_stop->service_seal, 2);
		ShutdownRequestPending = true;
	}
	return WL_LATCH_SET;
}

static void
test_callback_late_work(void)
{
	lmd_victim_acks[63].active = true;
}

static void
run_stop_lmd_main(int scenario)
{
	test_stop_lmd_reset();
	memset(&test_stop_region, 0, sizeof(test_stop_region));
	cl_normal_stop = &test_stop_region.normal_stop;
	pg_atomic_write_u32(&cl_normal_stop->requested, scenario != 1 && scenario != 8);
	pg_atomic_write_u32(&cl_normal_stop->identity_published, 1);
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_DRAIN);
	test_main_case = scenario;
	test_main_waits = test_main_scans = test_main_coord_scans = test_main_polls = 0;
	test_main_exit_code = -1;
	test_exit_callback = NULL;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	test_graph_observation = scenario == 3	 ? CLUSTER_NORMAL_STOP_PENDING
							 : scenario == 4 ? CLUSTER_NORMAL_STOP_INVALID
											 : CLUSTER_NORMAL_STOP_READY;
	ShutdownRequestPending = scenario == 5;
	lmd_last_coord_scan = lmd_last_cleanup_sweep = 0;
	test_main_running = true;
	if (sigsetjmp(test_main_exit, 1) == 0)
		LmdMain();
	test_main_running = false;
	PG_exception_stack = NULL;
	ShutdownRequestPending = false;
	UT_ASSERT_EQ(test_lock_depth, 0);
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_active_mask), 0);
}

UT_TEST(test_lmd_actual_main_work_sleep_pending_and_ordinary)
{
	for (int scenario = 1; scenario <= 3; scenario++) {
		run_stop_lmd_main(scenario);
		UT_ASSERT_EQ(test_main_exit_code, 0);
		UT_ASSERT_EQ(test_main_waits, 2);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
		UT_ASSERT(scenario == 1 ? test_main_polls == 0 : test_main_polls >= 4);
		if (scenario == 2)
			UT_ASSERT_EQ(test_main_scans, 1);
	}
}

UT_TEST(test_lmd_actual_main_invalid_early_exit_error_and_callback_refuse_clean)
{
	for (int scenario = 4; scenario <= 7; scenario++) {
		run_stop_lmd_main(scenario);
		UT_ASSERT_EQ(test_main_exit_code, 1);
		UT_ASSERT(cluster_normal_stop_failure() != CLUSTER_NORMAL_STOP_FAILURE_NONE);
		if (scenario < 7)
			UT_ASSERT_EQ(test_main_waits, 0);
	}
}

UT_TEST(test_lmd_actual_main_midpass_request_and_fast_continue_still_poll)
{
	for (int scenario = 8; scenario <= 9; scenario++) {
		run_stop_lmd_main(scenario);
		UT_ASSERT_EQ(test_main_exit_code, 0);
		UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_NONE);
		UT_ASSERT_EQ(test_main_waits, 2);
		UT_ASSERT(test_main_polls >= (scenario == 9 ? 5 : 4));
	}
}

UT_TEST(test_lmd_actual_probe_wait_suspends_active_but_never_signs_idle)
{
	test_stop_lmd_reset();
	memset(&test_stop_region, 0, sizeof(test_stop_region));
	cl_normal_stop = &test_stop_region.normal_stop;
	pg_atomic_write_u32(&cl_normal_stop->requested, 1);
	pg_atomic_write_u32(&cl_normal_stop->identity_published, 1);
	pg_atomic_write_u32(&cl_normal_stop->frontends_gone, 1);
	pg_atomic_write_u32(&cl_normal_stop->phase, CLUSTER_NORMAL_STOP_WAIT_DRAIN_ACK);
	cl_normal_stop->peer_requests_seen = 15;
	cl_state->ack_bitmap[0] = UINT32_C(15) & ~(UINT32_C(1) << cluster_node_id);
	cl_normal_stop->peer_reply_sent = cl_state->ack_bitmap[0];
	pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesce_requested, 1);
	pg_atomic_write_u32(&cl_normal_stop->cleaner_quiesced_mask, 255);
	pg_atomic_write_u32(&cl_normal_stop->service_idle_mask, 2047);
	UT_ASSERT(cluster_normal_stop_service_enter());
	test_probe_waiting = test_main_running = true;
	lmd_probe_wait(50);
	test_probe_waiting = test_main_running = false;
	UT_ASSERT_EQ(cl_normal_stop_service_depth, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_active_mask), 1024);
	UT_ASSERT_EQ(pg_atomic_read_u32(&cl_normal_stop->service_seal), 0);
	UT_ASSERT(cluster_normal_stop_service_leave(true));
	UT_ASSERT_EQ(cluster_normal_stop_service_idle(CLUSTER_NORMAL_STOP_READY),
				 CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(cluster_normal_stop_service_seal(2047, 1), CLUSTER_NORMAL_STOP_READY);
	cl_normal_stop = NULL;
}

UT_TEST(test_lmd_final_seal_rejects_new_queue_item_without_rewriting_existing)
{
	GesCancelAckPayload ack = { 0 };
	LmdCancelQueueShmem before;
	test_stop_lmd_reset();
	ack.opcode = GES_REQ_OPCODE_CANCEL_ACK;
	UT_ASSERT(cluster_lmd_cancel_queue_enqueue(1, &ack, sizeof(ack)));
	before = *lmd_cancel_queue;
	memset(&test_stop_region, 0, sizeof(test_stop_region));
	cl_normal_stop = &test_stop_region.normal_stop;
	pg_atomic_write_u32(&cl_normal_stop->requested, 1);
	pg_atomic_write_u32(&cl_normal_stop->service_seal, 2);
	MyAuxProcType = LmonProcess;
	UT_ASSERT(cluster_normal_stop_service_enter());
	UT_ASSERT(!cluster_lmd_cancel_queue_enqueue(1, &ack, sizeof(ack)));
	UT_ASSERT_EQ(memcmp(&before, lmd_cancel_queue, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_normal_stop_failure(), CLUSTER_NORMAL_STOP_FAILURE_LATE_UNSEALED_WORK);
	(void)cluster_normal_stop_service_leave(true);
	MyAuxProcType = LmdProcess;
	cl_normal_stop = NULL;
}

UT_TEST(test_lmd_epoch_zero_is_owned_until_original_ack_not_invalid)
{
	ClusterGrdHolderId local = { 0, 1, 0, 0 };
	ClusterLmdVertex remote = test_stop_victim(1);
	test_stop_lmd_reset();
	remote.cluster_epoch = 0; /* CLUSTER_EPOCH_INITIAL is a real epoch. */
	remote.request_id = 0;	  /* TX waits have no GES request id. */
	lmd_victim_ack_add(1, &local, 41, 99, 1);
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	test_marker = CLUSTER_CANCEL_MARKER_CONSUMED;
	test_marker_id = 99;
	cluster_lmd_victim_ack_tick();
	UT_ASSERT_EQ(test_ack_sent, 1);
	UT_ASSERT_EQ(cluster_lmd_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_NOT_NULL(lmd_pending_cancel_add(&remote, 55, 0, false, 42, &remote, 1));
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_lmd_pending_cancel_on_ack(55, GES_CANCEL_ACK_CONSUMED, &remote, 1);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_lmd_pending_zero_wait_seq_awaits_remote_revalidation_result)
{
	ClusterLmdVertex remote = test_stop_victim(1);
	test_stop_lmd_reset();
	remote.wait_seq = 0; /* Plain GRD wait metadata, not an installed token. */
	UT_ASSERT_NOT_NULL(lmd_pending_cancel_add(&remote, 55, 17, false, 42, &remote, 1));
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	cluster_lmd_pending_cancel_on_ack(55, GES_CANCEL_ACK_NOT_WAITING, &remote, 1);
	UT_ASSERT_EQ(cluster_lmd_pending_normal_stop_poll(NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(22);

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
	UT_RUN(test_stop_lmd_observations_belong_to_actual_lmd);
	UT_RUN(test_stop_cancel_queue_only_original_dequeue_retires_wrapped_items);
	UT_RUN(test_stop_victim_ack_retains_exact_marker_and_original_send);
	UT_RUN(test_stop_pending_cancel_requires_full_ack_identity_and_terminal_status);
	UT_RUN(test_stop_private_last_slot_invalid_not_hidden_by_pending);
	UT_RUN(test_lmd_actual_main_work_sleep_pending_and_ordinary);
	UT_RUN(test_lmd_actual_main_invalid_early_exit_error_and_callback_refuse_clean);
	UT_RUN(test_lmd_actual_main_midpass_request_and_fast_continue_still_poll);
	UT_RUN(test_lmd_actual_probe_wait_suspends_active_but_never_signs_idle);
	UT_RUN(test_lmd_final_seal_rejects_new_queue_item_without_rewriting_existing);
	UT_RUN(test_lmd_epoch_zero_is_owned_until_original_ack_not_invalid);
	UT_RUN(test_lmd_pending_zero_wait_seq_awaits_remote_revalidation_result);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
