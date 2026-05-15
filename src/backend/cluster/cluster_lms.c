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
 *	  HC3 ConditionVariable substrate is retained for producer-side
 *	  wake API compatibility, but the Step 6 LMS skeleton uses the
 *	  proven aux-process WaitLatch idle path until a dedicated LMS
 *	  latch handoff lands in the production activation spec.
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

#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_guc.h"
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
 * Idle sleep timeout for the main loop WaitLatch fallback poll.
 * Hardcoded for the Step 6 skeleton; producer-side CV broadcast remains
 * present, but this aux process does not rely on CV sleeper registration.
 */
#define LMS_IDLE_TIMEOUT_MS 100


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
		pg_atomic_init_u32(&cluster_lms_state->lms_state,
						   cluster_lms_enabled ? CLUSTER_LMS_NOT_STARTED : CLUSTER_LMS_DISABLED);
		pg_atomic_init_u32(&cluster_lms_state->work_queue_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_started_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_ready_at_us, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_work_drained_count, 0);
		/* spec-2.20 D4 — 3 NEW counter (replacing single lms_decision_count). */
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_grant_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_reject_count, 0);
		pg_atomic_init_u64(&cluster_lms_state->lms_decision_convert_count, 0);
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

	if (!cluster_lms_enabled) {
		/*
		 * Startup-time fallback: no LMS child should be forked when the
		 * POSTMASTER GUC is off.  Mark DISABLED so SQL/debug surfaces can
		 * distinguish intentional opt-out from "not started yet".
		 */
		if (cluster_lms_state != NULL) {
			LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
			lms_set_state(CLUSTER_LMS_DISABLED);
			LWLockRelease(&cluster_lms_state->lwlock);
		}
		return 0;
	}

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

	/* Wake any future CV-based LMS waiter; current skeleton also polls latch. */
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

/*
 * spec-2.20 D9 — 3 NEW decision counter accessors.
 *
 *	Replaces the single cluster_lms_get_decision_count() — each grant
 *	decision body in LWLock window inc exactly one of grant/reject/convert.
 *	dump_lms (D10) + pg_cluster_lms view (D11) reflect the 3 counters.
 */
uint64
cluster_lms_get_decision_grant_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_grant_count);
}

uint64
cluster_lms_get_decision_reject_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_reject_count);
}

uint64
cluster_lms_get_decision_convert_count(void)
{
	if (cluster_lms_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lms_state->lms_decision_convert_count);
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
	ClusterLmsState state;

	if (cluster_lms_state == NULL)
		return false;

	state = (ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state);

	/*
	 * DISABLED is a startup-only opt-out and must preserve LMON fallback.
	 * STOPPED is a runtime LMS failure and must remain fail-closed (no
	 * fallback grant path), matching HC1.
	 */
	return state == CLUSTER_LMS_READY || state == CLUSTER_LMS_DRAINING
		   || state == CLUSTER_LMS_STOPPED;
}


/*
 * HC4 EXACT predicate (spec-2.20 v0.3 frozen — L124 inherit).
 *
 *	cluster_lms_is_ready() returns true iff lms_state == CLUSTER_LMS_READY.
 *	**Critical regression防御** — 既有 cluster_lms_owns_grant() 有 latent
 *	bug 返回 READY OR DRAINING OR STOPPED(spec-2.19 P1.5 同款 bug 在 LMS
 *	pre-existing,只是无 caller exercise 未暴露)。spec-2.20 新代码必走
 *	cluster_lms_is_ready()(exact == READY)避免 false-positive:
 *	  - DRAINING (3) — LMS 正在 shutdown
 *	  - STOPPED (4) — LMS 已死 / not yet spawned
 *	  - DISABLED (5) — startup-time opt-out
 *	均 NOT ready。
 */
bool
cluster_lms_is_ready(void)
{
	if (cluster_lms_state == NULL)
		return false;

	return ((ClusterLmsState)pg_atomic_read_u32(&cluster_lms_state->lms_state))
		   == CLUSTER_LMS_READY;
}


/* ============================================================
 * HC3 producer wake.
 *
 * Producers call this after a successful enqueue.  Step 6 keeps the
 * producer-side CV contract as a stable API while the LMS skeleton uses
 * WaitLatch polling for idle sleep; production activation can switch the
 * consumer to CV once it owns a fully verified aux-process wake path.
 * No-op when LMS DISABLED (lms_enabled=off startup) since the LMS process
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
 *	Runs the drain consumer loop until shutdown.  The Step 6 skeleton
 *	uses the same WaitLatch idle pattern as the existing cluster aux
 *	processes; producer-side ConditionVariable broadcast is retained as
 *	the compatibility surface for the later event-driven LMS path.
 *	      (CV ops happen outside signal context;handlers only set
 *	      latch / ShutdownRequestPending)
 * ============================================================ */

void
LmsMain(void)
{
	Assert(IsUnderPostmaster);

	MyBackendType = B_LMS;
	init_ps_display(NULL);

	/* Standard PG aux-process signal layout (modeled on cluster_lmon.c). */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	/* No ProcSignal slot in the skeleton; see auxprocess.c early setup. */
	pqsignal(SIGUSR1, SIG_IGN);
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
	 * spec-2.18 Sprint A Step 1-6 skeleton main loop.  LMS daemon exists for
	 * catalog visibility + ABI surface (B_LMS / LmsProcess / pg_cluster_lms
	 * later) but does NOT yet own the GES drain consumer — LMON keeps that
	 * role until the Hardening round wires ownership transfer.  The body is
	 * a pure idle WaitLatch loop: SIGTERM sets ShutdownRequestPending +
	 * MyLatch, WaitLatch returns, the head guard breaks, proc_exit(0)
	 * completes cleanly without any cluster shmem cleanup.  No CV
	 * sleep / broadcast in this skeleton — pss_barrierCV is exercised
	 * only via standard PG paths during proc_exit, which is verified safe.
	 */
	for (;;) {
		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || lms_shutdown_requested())
			break;

		pg_atomic_fetch_add_u64(&cluster_lms_state->lms_drain_empty_count, 1);

		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						LMS_IDLE_TIMEOUT_MS, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
	}

	/* Transition to DRAINING then STOPPED. */
	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	lms_set_state(CLUSTER_LMS_DRAINING);
	LWLockRelease(&cluster_lms_state->lwlock);

	LWLockAcquire(&cluster_lms_state->lwlock, LW_EXCLUSIVE);
	cluster_lms_state->stopped_at = GetCurrentTimestamp();
	lms_set_state(CLUSTER_LMS_STOPPED);
	LWLockRelease(&cluster_lms_state->lwlock);

	proc_exit(0);
}
