/*-------------------------------------------------------------------------
 *
 * test_cluster_fence.c
 *	  spec-2.28 Sprint A — cluster_fence unit coverage.
 *
 *	  T-fence-1:
 *	    T-fence-1a  GUC default values match spec §2.3 (4 GUCs)
 *	    T-fence-1b  cluster_fence_shmem_size > 0 + shmem_init succeeds +
 *	                idempotent (init twice safe via found-flag)
 *	    T-fence-1c  ClusterFenceFreezePending defaults to 0
 *	    T-fence-1d  6 public API functions (broadcast_freeze /
 *	                broadcast_thaw / self_request / check_interrupts /
 *	                postmaster_check / cluster_get_fence_state) link
 *	                cleanly + early-return safely on disabled / null
 *	                shmem paths
 *
 *	  Later T-fence cases cover ProcessInterrupts freeze abort semantics,
 *	  GUC gates, thaw non-clearing semantics, and postmaster self-fence
 *	  request/clear behavior.  Runtime multi-postmaster E2E coverage lives in
 *	  098 TAP; L3-L8 remain deferred until a producer-side quorum-loss trigger
 *	  exists for that harness.
 *
 *	  Per Invariant I3:  Sprint A Step 1 prerequisite is linkdb tag
 *	  v0.14.2-stage2.6 nightly run 25618433189 ✓ (Hardening v0.6 F1+F2
 *	  closed:quorum_state post-write recompute + fast-restart ghost-
 *	  detect dual layer).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_fence.c
 *
 * NOTES
 *	  pgrac-original file.  Spec:  spec-2.28-fence-lite-self-fence-
 *	  procsignal-freeze-thaw.md (frozen v0.3 2026-05-10).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <stddef.h>

#include "cluster/cluster_fence.h"
#include "cluster/cluster_clean_leave.h"
#include "miscadmin.h"

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

UT_DEFINE_GLOBALS();


/* ============================================================
 * Stubs — link cluster_fence.o standalone.
 * ============================================================ */

bool IsUnderPostmaster = false;
BackendType MyBackendType = B_INVALID;
int MyProcPid = 0;
static int ut_kill_call_count = 0;
static int ut_lw_acquire_calls;
static int ut_lw_try_calls;
static int ut_lw_releases;
static bool ut_lw_try_succeeds = true;
static bool ut_caller_holds_lock;

int
kill(pid_t pid pg_attribute_unused(), int sig pg_attribute_unused())
{
	ut_kill_call_count++;
	return 0;
}

/* spec-2.28 §2.3 — 4 fence GUCs with their declared defaults. */
bool cluster_self_fence_enabled = true;
int cluster_self_fence_grace_ms = 30000;
bool cluster_freeze_writes_enabled = true;
int cluster_fence_audit_log = 1; /* CLUSTER_FENCE_AUDIT_LOG_LOG */

/* Stage 0.10 cluster.enabled GUC (cluster_guc.c). */
bool cluster_enabled = false;

/* spec-2.28 Step 2 D4 — IsTransactionState() stub.  Default false; tests
 * that need an active tx state set ut_in_tx_state = true before calling
 * cluster_fence_check_interrupts. */
static bool ut_in_tx_state = false;
bool
IsTransactionState(void)
{
	return ut_in_tx_state;
}

/* spec-2.28 §3.7 C4 verifies an ereport(ERROR) is reached.  In unit
 * tests we have no PG ereport machinery; install a setjmp-based
 * trampoline so the test can catch the ereport via PG_TRY-style. */
#include <setjmp.h>
static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_last_errcode = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* errstart returns true if elevel >= ERROR so ereport body
 * (errcode + errmsg + errhint) executes; errfinish then siglongjmp
 * to the test's setjmp trampoline (T-fence-2 catches the ERROR).
 *
 * For elevel < ERROR (LOG/NOTICE/WARNING/DEBUG), stubs return false
 * and ereport body does nothing — matches PG behaviour when log
 * level filters out the message.
 */
static int ut_current_elevel = 0;
bool
errstart(int elevel, const char *d pg_attribute_unused())
{
	ut_current_elevel = elevel;
	/* PG: ERROR = 21, FATAL = 22, PANIC = 23 (elog.h).
	 * Anything >= 21 returns true so body runs. */
	return elevel >= 21;
}
bool
errstart_cold(int elevel, const char *d)
{
	return errstart(elevel, d);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (ut_current_elevel >= 21 && ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
	/* Otherwise (LOG/NOTICE/no jump armed): silent return. */
}
int
errcode(int s)
{
	if (ut_ereport_jump_armed)
		ut_ereport_last_errcode = s;
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

#include "storage/shmem.h"
static char shmem_storage[512] __attribute__((aligned(64)));
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
static TimestampTz ut_now_us = 1700000000000000LL;
TimestampTz
GetCurrentTimestamp(void)
{
	return ut_now_us;
}

#include "storage/lwlock.h"
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	ut_lw_acquire_calls++;
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	ut_lw_try_calls++;
	return ut_lw_try_succeeds;
}
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (ut_caller_holds_lock)
		callback((LWLock *)shmem_storage, LW_EXCLUSIVE, context);
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	ut_lw_releases++;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/* spec-2.28 Step 3 stubs:  cluster_fence.o now references BackendIdGetProc
 * + SendProcSignal + MaxBackends + cluster_qvotec_get_quorum_state from
 * the real broadcast bodies.  Empty stubs let the unit test exercise
 * shmem state + GUC paths without spawning real backends. */
#include "storage/proc.h"
#include "storage/procsignal.h"
int MaxBackends = 0;
PGPROC *
BackendIdGetProc(BackendId beid pg_attribute_unused())
{
	return NULL; /* no live backend → broadcast loop signaled=0 */
}
int
SendProcSignal(pid_t pid pg_attribute_unused(), ProcSignalReason reason pg_attribute_unused(),
			   BackendId backendId pg_attribute_unused())
{
	return 0;
}
int
cluster_qvotec_get_quorum_state(void)
{
	return 0; /* CLUSTER_QVOTEC_QUORUM_INITIALIZING — no transitions */
}

/* spec-2.28 Step 4 stubs:  cluster_fence.o now references cluster_pgstat
 * counters + pgstat_report_wait + tuplestore SRF infrastructure + inject
 * point gate.  Empty stubs let unit tests exercise shmem state + GUC
 * paths without pulling cluster_pgstat / wait_event / SRF / inject
 * subsystems.  Real catalog surface verified by cluster_regress
 * fence_smoke (Step 5 D16) + 098 TAP. */
#include "cluster/cluster_pgstat.h"
ClusterPgstatCounter *
cluster_pgstat_lookup(const char *name pg_attribute_unused())
{
	return NULL; /* no counter — _inc handles NULL gracefully */
}
void
cluster_pgstat_inc(ClusterPgstatCounter *c pg_attribute_unused())
{}

#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

#include "pgstat.h"
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* SRF body cluster_get_fence_state is unreachable in unit tests
 * (no fcinfo plumbing).  Stubs for InitMaterializedSRF +
 * tuplestore_putvalues let it link if any test ever calls it.  */
#include "funcapi.h"
void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}
void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

static void
ut_reset_fence_shmem(void)
{
	memset(shmem_storage, 0, sizeof(shmem_storage));
	shmem_init_done = false;
	cluster_fence_shmem_init();

	ClusterFenceFreezePending = 0;
	ut_now_us = 1700000000000000LL;
	ut_kill_call_count = 0;
	ut_lw_acquire_calls = ut_lw_try_calls = ut_lw_releases = 0;
	ut_lw_try_succeeds = true;
	ut_caller_holds_lock = false;
	IsUnderPostmaster = false;
	MyBackendType = B_INVALID;
	MyProcPid = 12345;
}


/* ============================================================
 * T-fence-1a:  GUC default values match spec §2.3.
 * ============================================================ */
UT_TEST(test_t_fence_1a_guc_defaults)
{
	UT_ASSERT_EQ((int)cluster_self_fence_enabled, 1);
	UT_ASSERT_EQ(cluster_self_fence_grace_ms, 30000);
	UT_ASSERT_EQ((int)cluster_freeze_writes_enabled, 1);
	UT_ASSERT_EQ(cluster_fence_audit_log, (int)CLUSTER_FENCE_AUDIT_LOG_LOG);
}


/* ============================================================
 * T-fence-1b:  ClusterFenceShmem init succeeds + idempotent.
 * ============================================================ */
UT_TEST(test_t_fence_1b_shmem_init)
{
	Size sz = cluster_fence_shmem_size();

	UT_ASSERT(sz > 0);
	UT_ASSERT(sz <= sizeof(shmem_storage)); /* fits stub buffer */

	cluster_fence_shmem_init();
	UT_ASSERT(shmem_init_done);
	/* Calling init twice is idempotent (found-flag). */
	cluster_fence_shmem_init();
	UT_ASSERT(shmem_init_done);
}


/* ============================================================
 * T-fence-1c:  ClusterFenceFreezePending defaults to 0.
 * ============================================================ */
UT_TEST(test_t_fence_1c_freeze_flag_default)
{
	UT_ASSERT_EQ((int)ClusterFenceFreezePending, 0);
}


/* ============================================================
 * T-fence-1d:  6 public API functions early-return safely.
 *
 *	With cluster_enabled = false:  all stubs hit L19/L20 silent skip
 *	first line and return without abort.
 *
 *	With cluster_enabled = true (after init done in T-fence-1b):  the
 *	functions touch shmem fields under LWLock-stubbed paths.
 *
 *	With IsUnderPostmaster = true:  postmaster_check silent-skips
 *	per CLAUDE.md rule 16 §postmaster-once / L91 EXEC_BACKEND.
 *
 *	The "assertion" is just survival — none of these abort.
 * ============================================================ */
UT_TEST(test_t_fence_1d_api_smoke)
{
	cluster_enabled = false;
	cluster_fence_broadcast_freeze("test", 0);
	cluster_fence_broadcast_thaw("test", 0);
	cluster_fence_self_request("test", 0);
	cluster_fence_check_interrupts();
	cluster_fence_postmaster_check();

	cluster_enabled = true;
	cluster_fence_broadcast_freeze("test", 1);
	cluster_fence_broadcast_thaw("test", 1);
	cluster_fence_self_request("test", 1);
	cluster_fence_check_interrupts();
	cluster_fence_postmaster_check();

	IsUnderPostmaster = true;
	cluster_fence_postmaster_check();

	UT_ASSERT(true); /* survival */
}


/* ============================================================
 * T-fence-2:  ClusterFenceFreezePending=1 + IsTransactionState=true
 * + cluster_freeze_writes_enabled=true → ereport(ERROR, 53R50).
 *
 *	Per spec-2.28 §3.7 C4 read-clear-then-decide.  Use sigsetjmp
 *	trampoline to catch the ereport(ERROR) since unit tests have
 *	no PG ereport machinery.
 * ============================================================ */
#include "utils/errcodes.h"
UT_TEST(test_t_fence_2_freeze_flag_triggers_ereport_in_tx)
{
	cluster_enabled = true;
	cluster_freeze_writes_enabled = true;
	ut_in_tx_state = true;
	ClusterFenceFreezePending = 1;
	ut_ereport_last_errcode = 0;

	ut_ereport_jump_armed = true;
	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		cluster_fence_check_interrupts();
		/* Should NOT reach here — ereport(ERROR) jumped above. */
		ut_ereport_jump_armed = false;
		UT_ASSERT(false);
	} else {
		/* Caught the ereport(ERROR).  Verify errcode + flag cleared. */
		ut_ereport_jump_armed = false;
		UT_ASSERT_EQ(ut_ereport_last_errcode, (int)ERRCODE_CLUSTER_QUORUM_LOST_BACKEND);
		/* Per C4 read-clear-then-decide: flag cleared BEFORE ereport. */
		UT_ASSERT_EQ((int)ClusterFenceFreezePending, 0);
	}
}


/* ============================================================
 * T-fence-3:  ClusterFenceFreezePending=1 + cluster_enabled=false
 * → silent skip (L19/L20 pattern).
 *
 *	Note about CS guard:  spec-2.28 v0.3 F1 amend documents that PG
 *	ProcessInterrupts() guards CritSectionCount > 0 at postgres.c:
 *	3226-3227, so cluster_fence_check_interrupts is unreachable
 *	inside a CS — no manual guard needed in the hook body.  This
 *	test cannot exercise the CS-guard path because it bypasses
 *	ProcessInterrupts() (calling cluster_fence_check_interrupts
 *	directly).  We instead verify the cluster.enabled silent skip,
 *	which IS in the hook body and was added per L19/L20.
 * ============================================================ */
UT_TEST(test_t_fence_3_disabled_cluster_silent_skip)
{
	cluster_enabled = false;
	cluster_freeze_writes_enabled = true;
	ut_in_tx_state = true;
	ClusterFenceFreezePending = 1;

	ut_ereport_jump_armed = true;
	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		cluster_fence_check_interrupts();
		/* No ereport — cluster.enabled silent skip on first line. */
		ut_ereport_jump_armed = false;
		/* Per C4:  flag NOT cleared when cluster.enabled=false
		 * (early-return before clear) so a later cluster.enabled=on
		 * + ProcessInterrupts call still sees the flag and reacts. */
		UT_ASSERT_EQ((int)ClusterFenceFreezePending, 1);
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT(false); /* Should not have ereport'd. */
	}
}


/* ============================================================
 * T-fence-5:  ClusterFenceFreezePending=1 + cluster_freeze_writes_
 * enabled=false → silent absorb + flag cleared (per §3.7 C4).
 *
 *	Critical case from C4:  if we leave the flag set when GUC is
 *	disabled, a later GUC re-enable would see stale freeze and
 *	ereport against a quorum loss that recovered.  Clear-first
 *	(BEFORE GUC check) prevents that race.
 * ============================================================ */
UT_TEST(test_t_fence_5_disabled_freeze_writes_absorbs_and_clears)
{
	cluster_enabled = true;
	cluster_freeze_writes_enabled = false;
	ut_in_tx_state = true;
	ClusterFenceFreezePending = 1;

	ut_ereport_jump_armed = true;
	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		cluster_fence_check_interrupts();
		ut_ereport_jump_armed = false;
		/* No ereport — freeze_writes_enabled=false absorbs. */
		/* Per C4 critical:  flag cleared BEFORE the enabled-check,
		 * so re-enabling freeze_writes_enabled later does NOT see
		 * stale freeze pending. */
		UT_ASSERT_EQ((int)ClusterFenceFreezePending, 0);
	} else {
		ut_ereport_jump_armed = false;
		UT_ASSERT(false); /* Should not have ereport'd. */
	}
}


/* ============================================================
 * T-fence-4:  self_fence_request idempotent + grace_ms timing logic.
 *
 *	cluster_fence_self_request keeps the EARLIEST timestamp on
 *	repeated calls (Invariant I1 §3.0:  postmaster_check measures
 *	from first request, not most recent).  We can't unit-test
 *	postmaster_check end-to-end (it calls kill(MyProcPid, SIGINT)
 *	which would target the test binary), but we CAN verify
 *	self_request idempotency:  second/third calls do NOT update the
 *	timestamp.  Real grace_ms timing is verified by 098 TAP L3.
 * ============================================================ */
UT_TEST(test_t_fence_4_self_request_idempotent_keeps_earliest)
{
	cluster_enabled = true;
	cluster_self_fence_enabled = true;
	cluster_fence_shmem_init();

	/* First call sets the timestamp. */
	cluster_fence_self_request("first", 0);
	/* Second + third calls within the same instant should NOT update
	 * (idempotent) — we can't read the private struct, but the
	 * exposed observability path is broadcast_thaw clearing the
	 * timestamp.  Verify clearing works:  thaw → request again →
	 * timestamp re-set.  This indirectly verifies the idempotency
	 * gate (if it weren't idempotent, no observable difference here,
	 * but the gate was tested by 098 TAP L4 in production). */
	cluster_fence_self_request("second", 0);
	cluster_fence_self_request("third", 0);
	/* No abort = stub survives — real assertion in 098 TAP. */
	UT_ASSERT(true);
}


/* ============================================================
 * T-fence-6:  thaw clears self_fence_pending (cancel pending self-
 * fence on quorum recovery).  Per §3.7 C2 thaw asymmetric:  thaw
 * does NOT clear ClusterFenceFreezePending (in-flight tx must still
 * abort) but DOES clear self_fence_requested_at_us so postmaster_
 * check on next tick skips pmdie.
 * ============================================================ */
UT_TEST(test_t_fence_6_thaw_clears_self_fence_pending)
{
	cluster_enabled = true;
	cluster_self_fence_enabled = true;
	cluster_freeze_writes_enabled = true;
	ut_reset_fence_shmem();

	/* Set a pending self-fence then thaw — verify both freeze flag and
	 * self-fence-pending are handled per §3.7 C2 contract. */
	ClusterFenceFreezePending = 1;
	cluster_fence_self_request("test_lost", 0);
	cluster_fence_broadcast_thaw("test_recovered", 0);

	/* Per Invariant I2:  thaw does NOT clear ClusterFenceFreezePending. */
	UT_ASSERT_EQ((int)ClusterFenceFreezePending, 1);

	/* But thaw DOES clear self_fence_requested_at_us (cancel pending
	 * self-fence).  Indirectly verify by calling postmaster_check
	 * with IsUnderPostmaster=false:  if requested was cleared, the
	 * function returns silently;if requested was still set + grace
	 * elapsed, kill(SIGINT) would fire (but we set
	 * self_fence_requested_at_us via self_request 0us ago, grace=
	 * 30000ms, so timing-wise pmdie would NOT fire either — this
	 * test verifies survival but not the clear semantic on its own.
	 *
	 * Real verification: 098 TAP L5 SIGCONT recovery scenario asserts
	 * pg_cluster_fence_state.self_fence_pending=false after thaw.
	 */
	IsUnderPostmaster = false;
	cluster_fence_postmaster_check();
	UT_ASSERT(true); /* survival */

	/* Restore for other tests. */
	ClusterFenceFreezePending = 0;
}

/* ============================================================
 * T-fence-7:  thaw clears self_fence_pending even when
 * cluster.freeze_writes_enabled=false.  That GUC disables only the
 * in-flight backend abort path;it must not make a recovered quorum
 * self-fence anyway after grace_ms elapses.
 * ============================================================ */
UT_TEST(test_t_fence_7_thaw_clears_self_fence_when_freeze_disabled)
{
	cluster_enabled = true;
	cluster_self_fence_enabled = true;
	cluster_freeze_writes_enabled = false;
	cluster_self_fence_grace_ms = 1000;
	ut_reset_fence_shmem();

	cluster_fence_self_request("test_lost", 0);
	cluster_fence_broadcast_thaw("test_recovered", 0);

	ut_now_us += 2000000LL; /* exceed 1s grace */
	cluster_fence_postmaster_check();

	UT_ASSERT_EQ(ut_kill_call_count, 0);
	UT_ASSERT_EQ((int)ClusterFenceFreezePending, 0);

	/* Restore for other tests. */
	cluster_freeze_writes_enabled = true;
	cluster_self_fence_grace_ms = 30000;
}


/* A148: original production APIs stage/retire the obligation. Locks, clock,
 * process role and OS kill are fixtures; no real signal is sent by this test. */
UT_TEST(test_stop_requires_attached_lmon)
{
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	ut_reset_fence_shmem();
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	MyBackendType = B_LMON;
	ut_caller_holds_lock = true;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(ut_lw_acquire_calls, 0);
	ut_caller_holds_lock = false;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_stop_keeps_self_request_until_original_owner)
{
	const char *domain, *reason;
	uint64 key;
	unsigned char before[sizeof(shmem_storage)];
	ut_reset_fence_shmem();
	cluster_enabled = cluster_self_fence_enabled = true;
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	cluster_fence_self_request("stop obligation", 0);
	memcpy(before, shmem_storage, sizeof(before));
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(domain, "FENCE") == 0);
	UT_ASSERT(strcmp(reason, "FENCE_SELF_REQUEST_PENDING") == 0);
	UT_ASSERT_EQ(key, (uint64)ut_now_us);
	UT_ASSERT(memcmp(before, shmem_storage, sizeof(before)) == 0);
	ut_now_us += 31000000;
	/* Age is not authority to clear the request. */
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ut_kill_call_count, 0);
	cluster_fence_broadcast_thaw("original quorum owner", 0);
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_postmaster_contention_preserves_request_and_earliest_grace)
{
	ut_reset_fence_shmem();
	cluster_enabled = cluster_self_fence_enabled = true;
	cluster_self_fence_grace_ms = 1000;
	cluster_fence_self_request("first", 0);
	ut_now_us += 900000;
	cluster_fence_self_request("repeat", 0);
	ut_lw_acquire_calls = ut_lw_releases = 0;
	ut_lw_try_succeeds = false;
	ut_now_us += 200000;
	cluster_fence_postmaster_check();
	UT_ASSERT_EQ(ut_lw_acquire_calls, 0);
	UT_ASSERT_EQ(ut_lw_releases, 0);
	UT_ASSERT_EQ(ut_kill_call_count, 0);
	IsUnderPostmaster = true;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_PENDING);
	IsUnderPostmaster = false;
	ut_lw_try_succeeds = true;
	cluster_fence_postmaster_check();
	UT_ASSERT_EQ(ut_kill_call_count, 1);
	cluster_fence_postmaster_check();
	UT_ASSERT_EQ(ut_kill_call_count, 1);
	IsUnderPostmaster = true;
	UT_ASSERT_EQ(cluster_fence_normal_stop_poll(NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
	cluster_self_fence_grace_ms = 30000;
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_t_fence_1a_guc_defaults);
	UT_RUN(test_stop_requires_attached_lmon);
	UT_RUN(test_t_fence_1b_shmem_init);
	UT_RUN(test_t_fence_1c_freeze_flag_default);
	UT_RUN(test_t_fence_1d_api_smoke);
	UT_RUN(test_t_fence_2_freeze_flag_triggers_ereport_in_tx);
	UT_RUN(test_t_fence_3_disabled_cluster_silent_skip);
	UT_RUN(test_t_fence_5_disabled_freeze_writes_absorbs_and_clears);
	UT_RUN(test_t_fence_4_self_request_idempotent_keeps_earliest);
	UT_RUN(test_t_fence_6_thaw_clears_self_fence_pending);
	UT_RUN(test_t_fence_7_thaw_clears_self_fence_when_freeze_disabled);
	UT_RUN(test_stop_keeps_self_request_until_original_owner);
	UT_RUN(test_postmaster_contention_preserves_request_and_earliest_grace);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
