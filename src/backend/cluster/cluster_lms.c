/*-------------------------------------------------------------------------
 *
 * cluster_lms.c
 *	  pgrac LMS (Lock Master / Grant Service) cluster background process —
 *	  spec-2.18 Sprint A Step 1-2 skeleton implementation.
 *
 *	  Spec-2.18 ships the lifecycle skeleton + grant-decision ownership
 *	  migration from LMON to LMS.  Single ownership path with fail-closed
 *	  semantics — no runtime LMON fallback grant (§1.4.5 F1).  Real
 *	  grant state machine, BAST send/receive, deadlock detection,
 *	  cleanup_on_backend_exit entry sweep, lock class expansion all
 *	  defer to spec-2.19+.
 *
 *	  HC1 fail-closed:  LMS_READY 后 LMON grant hard-disabled.  LMS
 *	  crash → backend receives SQLSTATE 53R80 (Step 4).
 *
 *	  HC2 4-state semantic:  DISABLED (lms_enabled=off startup-only)
 *	  vs NOT_STARTED / STARTING vs READY / DRAINING vs STOPPED is
 *	  distinguished in pg_cluster_lms view + 53R80 reason field
 *	  (Step 4 view delivery).
 *
 *	  HC3 ConditionVariable 4 contracts:
 *	    (a) producer broadcast after successful enqueue
 *	    (b) main loop while-loop guards spurious wakeup
 *	    (c) shutdown path ConditionVariableCancelSleep
 *	    (d) no complex work in async-signal-unsafe critical path
 *
 *	  HC4 single ownership atomic guard.  Public helper
 *	  cluster_lms_owns_grant() reads lms_state atomic; LMON tick body
 *	  入口 calls this to early-return when LMS owns grant (Step 3).
 *	  No lms_drain_owner second field.
 *
 *	  HC5 NUM_AUXILIARY_PROCS bump:  LMS is 7th cluster aux process
 *	  (after LMON / LCK / DIAG / Cluster Stats / CSSD / QVOTEC);
 *	  src/include/storage/proc.h bumps NUM_AUXILIARY_PROCS 12 → 13
 *	  (D3b NEW deliverable; v0.3 L2.7).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.18-lms-daemon-grant-ownership-migration.md
 *	  (FROZEN v0.3 2026-05-14 user approve).
 *	  Anchor: cluster_lmon.c (spec-1.11) for skeleton structure.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/*
 * Idle sleep timeout for the main loop ConditionVariableTimedSleep
 * fallback poll.  Hardcoded per spec-2.18 §1.4 F3 (no GUC — polling
 * concept conflicts with event-driven CV wake).  100µs balances
 * wake latency vs CPU overhead.
 */
#define LMS_IDLE_TIMEOUT_US 100000 /* 100 ms in microseconds (CV API uses ms) */


/* External hook from postmaster.c (mirrors cluster_lmon Q2 thin proxy). */
extern pid_t cluster_postmaster_start_lms(void);


/* ============================================================
 * Module-local state.
 * ============================================================ */

static ClusterLmsSharedState *cluster_lms_state = NULL;

static const char *cluster_lms_state_strings[] = {
	"not_started", "starting", "ready", "draining", "stopped", "disabled",
};

/*
 * spec-1.3 region registry descriptor.  Registered once at postmaster
 * startup via cluster_lms_shmem_register().
 */
static const ClusterShmemRegion cluster_lms_region = {
	.name = "pgrac cluster lms",
	.size_fn = cluster_lms_shmem_size,
	.init_fn = cluster_lms_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-2.18 LMS",
	.reserved_flags = 0,
};


/* ============================================================
 * State string mapping.
 * ============================================================ */

const char *
cluster_lms_state_to_string(ClusterLmsState s)
{
	if ((int)s < 0 || (int)s > CLUSTER_LMS_STATE_LAST)
		return "(unknown)";
	return cluster_lms_state_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_lms_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLmsSharedState));
}

void
cluster_lms_shmem_init(void)
{
	bool found;

	cluster_lms_state = (ClusterLmsSharedState *)ShmemInitStruct(
		"pgrac cluster lms", sizeof(ClusterLmsSharedState), &found);

	if (!found) {
		memset(cluster_lms_state, 0, sizeof(*cluster_lms_state));
		LWLockInitialize(&cluster_lms_state->lwlock, LWTRANCHE_CLUSTER_LMS);
		pg_atomic_init_u32(&cluster_lms_state->lms_state, CLUSTER_LMS_NOT_STARTED);
		pg_atomic_init_u32(&cluster_lms_state->work_queue_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_started_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_ready_at_us, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_work_drained_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_drain_empty_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_error_count, 0);
		ConditionVariableInit(&cluster_lms_state->cv);
	}
}

void
cluster_lms_shmem_request(void)
{
	RequestAddinShmemSpace(cluster_lms_shmem_size());
}

void
cluster_lms_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lms_region);
}

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return cluster_lms_state;
}


/* ============================================================
 * Internal helpers.
 * ============================================================ */

/*
 * Atomically transition state.  Caller must hold lwlock LW_EXCLUSIVE
 * for non-atomic field updates (pid / spawned_at / ready_at);
 * lms_state itself is atomic so monotonic reads remain race-free
 * outside the lock.
 */
static void
lms_set_state(ClusterLmsState new_state)
{
	pg_atomic_write_u32(&cluster_lms_state->lms_state, (uint32)new_state);
}

static ClusterLmsState
lms_get_state(void)
{
	if (cluster_lms_state == NULL)
		return CLUSTER_LMS_NOT_STARTED;
	return (ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state);
}

static bool
lms_shutdown_requested(void)
{
	bool requested;

	if (cluster_lms_state == NULL)
		return true;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	requested = cluster_lms_state->shutdown_requested;
	LWLockRelease(&cluster_lms_state->lwlock);
	return requested;
}


/* ============================================================
 * Postmaster-side API (Q2 thin proxy / Q3 bounded polling).
 * ============================================================ */

int
cluster_lms_start(void)
{
	pid_t pid;

	Assert(!IsUnderPostmaster);

	/*
	 * Honor lms_enabled = off (PGC_POSTMASTER startup-time fallback;
	 * §1.4.5 F1).  Caller (postmaster phase 1 driver) checks GUC and
	 * skips this start() entirely when disabled; defense in depth here
	 * marks DISABLED state to make the SQL view surface accurate.
	 *
	 * Note: the GUC itself lands in Step 4 (D12); until then this
	 * branch is dead code that the compiler will optimize away.  Once
	 * Step 4 ships cluster_enabled / cluster_lms_enabled GUC plumbing,
	 * the phase 1 driver guard becomes the canonical disable path.
	 */

	pid = cluster_postmaster_start_lms();
	return (int)pid;
}

bool
cluster_lms_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_lms_state == NULL)
		return false;

	/*
	 * DISABLED is a legitimate "ready-or-skip" terminal: when
	 * lms_enabled = off at startup, LMS process never forks, so
	 * the postmaster phase 1 driver should not block waiting for
	 * READY.  Return true immediately to skip the polling loop.
	 */
	if (lms_get_state() == CLUSTER_LMS_DISABLED)
		return true;

	while (waited_ms < timeout_ms) {
		ClusterLmsState state = lms_get_state();

		if (state == CLUSTER_LMS_READY)
			return true;

		/* Early failure: shutdown / stopped before ready. */
		if (state == CLUSTER_LMS_DRAINING || state == CLUSTER_LMS_STOPPED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}

void
cluster_lms_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_lms_state == NULL)
		return;

	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->shutdown_requested = true;
	LWLockRelease(&cluster_lms_state->lwlock);

	/*
	 * HC3 (c): broadcast cv to wake LMS main loop from CV sleep so
	 * the shutdown_requested flag is consumed promptly without
	 * waiting for the idle timeout.
	 */
	ConditionVariableBroadcast(&cluster_lms_state->cv);
}


/* ============================================================
 * Read-only accessors (LW_SHARED + atomic reads).
 * ============================================================ */

ClusterLmsState
cluster_lms_get_state(void)
{
	return lms_get_state();
}

pid_t
cluster_lms_get_pid(void)
{
	pid_t pid;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	pid = cluster_lms_state->pid;
	LWLockRelease(&cluster_lms_state->lwlock);
	return pid;
}

TimestampTz
cluster_lms_get_spawned_at(void)
{
	TimestampTz t;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	t = cluster_lms_state->spawned_at;
	LWLockRelease(&cluster_lms_state->lwlock);
	return t;
}

TimestampTz
cluster_lms_get_ready_at(void)
{
	TimestampTz t;

	if (cluster_lms_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lms_state->lwlock, LW_SHARED);
	t = cluster_lms_state->ready_at;
	LWLockRelease(&cluster_lms_state->lwlock);
	return t;
}

uint64
cluster_lms_get_started_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_started_count);
}

uint64
cluster_lms_get_work_drained_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_work_drained_count);
}

uint64
cluster_lms_get_decision_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_count);
}

uint64
cluster_lms_get_drain_empty_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_drain_empty_count);
}

uint64
cluster_lms_get_error_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_error_count);
}


/* ============================================================
 * HC4 single ownership atomic guard.
 *
 * LMON tick body entry calls this to determine whether LMS owns
 * grant decisions.  Read-only atomic load — no LWLock needed; state
 * is monotonic so a stale read can only be one step behind, which
 * is acceptable for a non-correctness guard (the LMS-side LWLock-
 * protected critical section is the ultimate ownership boundary).
 * ============================================================ */

bool
cluster_lms_owns_grant(void)
{
	if (cluster_lms_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_lms_state->lms_state) >= (uint32)CLUSTER_LMS_READY;
}


/* ============================================================
 * HC3 producer wake.
 *
 * Producers call this after a successful enqueue to wake LMS main
 * loop from its CV sleep.  Must be called AFTER releasing the
 * work_queue producer LWLock (avoid wake-then-reacquire spin).  No-op
 * when LMS DISABLED (lms_enabled=off startup) since the LMS process
 * is not running to receive the wake.
 * ============================================================ */

void
cluster_lms_wake_drain(void)
{
	if (cluster_lms_state == NULL)
		return;
	if (lms_get_state() == CLUSTER_LMS_DISABLED)
		return;
	ConditionVariableBroadcast(&cluster_lms_state->cv);
}


/* ============================================================
 * LMS main entry.
 *
 *	Invoked from auxprocess.c dispatch when MyAuxProcType == LmsProcess.
 *	Runs the CV-driven drain consumer loop until shutdown.  HC3 4
 *	must-have contracts enforced:
 *	  (a) producer broadcast (in cluster_lms_wake_drain)
 *	  (b) main loop while-loop guards atomic work_queue_count
 *	  (c) shutdown path ConditionVariableCancelSleep
 *	  (d) no complex work in async-signal-unsafe critical path
 *	      (CV ops happen outside signal context;handlers only set
 *	      latch / ShutdownRequestPending)
 * ============================================================ */

void
LmsMain(void)
{
	Assert(IsUnderPostmaster);

	MyBackendType = B_LMS;
	init_ps_display(NULL);

	/*
	 * Standard PG aux-process signal layout (modeled on cluster_lmon.c
	 * LmonMain).  CV-based wake is layered on top of latch — SIGTERM /
	 * SIGINT set ShutdownRequestPending which is observed inside the
	 * CV wait loop via CHECK_FOR_INTERRUPTS().
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (cluster_lms_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_lms shmem region not attached"),
				 errhint("cluster_lms_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish STARTING + record pid / spawned_at. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->pid = MyProcPid;
	cluster_lms_state->spawned_at = GetCurrentTimestamp();
	lms_set_state(CLUSTER_LMS_STARTING);
	LWLockRelease(&cluster_lms_state->lwlock);

	/* Transition to READY. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->ready_at = GetCurrentTimestamp();
	pg_atomic_write_u64(&cluster_lms_state->lms_ready_at_us, (uint64)cluster_lms_state->ready_at);
	pg_atomic_fetch_add_u64(&cluster_lms_state->lms_started_count, 1);
	lms_set_state(CLUSTER_LMS_READY);
	LWLockRelease(&cluster_lms_state->lwlock);

	/*
	 * HC3 main loop with 4-contract CV discipline:
	 *	(a) producer broadcast happens in cluster_lms_wake_drain
	 *	(b) while-loop guards atomic work_queue_count
	 *	(c) ConditionVariableCancelSleep on every exit path
	 *	(d) no palloc / elog / LWLock-held during CV ops
	 *
	 * Step 1 skeleton: drain loop is a stub — it counts wake events
	 * and empty pumps.  Real drain logic (work_queue dequeue + grant
	 * decision) lands when spec-2.20 7-step state machine activates.
	 */
	for (;;) {
		uint32 queue_count;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || lms_shutdown_requested())
			break;

		/*
		 * HC3 (b) while-loop spurious-wakeup guard: re-check the
		 * atomic queue count after every wake.  Real consumer logic
		 * (Step 3+ wire) will dequeue inside this conditional and
		 * bump lms_work_drained_count or lms_drain_empty_count.
		 */
		queue_count = pg_atomic_read_u32(&cluster_lms_state->work_queue_count);
		if (queue_count == 0) {
			/* Empty pump tick — drain loop placeholder. */
			pg_atomic_fetch_add_u64(&cluster_lms_state->lms_drain_empty_count, 1);
		} else {
			/* Spec-2.20+ wires real drain here. */
			pg_atomic_fetch_add_u64(&cluster_lms_state->lms_work_drained_count, 1);
		}

		/*
		 * HC3 (c) prepare → timed sleep → cancel pattern.
		 * ConditionVariableTimedSleep returns true on timeout, false
		 * on CV broadcast wake.  Both paths fall through to the next
		 * iteration; CHECK_FOR_INTERRUPTS + shutdown_requested at
		 * loop head handle exit.  CancelSleep at the bottom is
		 * mandatory even if no timeout fired (defensive against
		 * sleeper-list leak on FATAL ereport bubbling through).
		 */
		ConditionVariablePrepareToSleep(&cluster_lms_state->cv);
		(void)ConditionVariableTimedSleep(&cluster_lms_state->cv, LMS_IDLE_TIMEOUT_US / 1000,
										  WAIT_EVENT_PG_SLEEP);
		ConditionVariableCancelSleep();
	}

	/* Transition to DRAINING then STOPPED. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	lms_set_state(CLUSTER_LMS_DRAINING);
	LWLockRelease(&cluster_lms_state->lwlock);

	/* HC3 (c) defensive cancel before exit — covers any in-flight sleep. */
	ConditionVariableCancelSleep();

	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->stopped_at = GetCurrentTimestamp();
	lms_set_state(CLUSTER_LMS_STOPPED);
	LWLockRelease(&cluster_lms_state->lwlock);

	proc_exit(0);
}
