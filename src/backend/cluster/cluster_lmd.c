/*-------------------------------------------------------------------------
 *
 * cluster_lmd.c
 *	  pgrac LMD (Lock Manager Daemon — deadlock detection actor) cluster
 *	  background process — spec-2.19 Sprint A Step 1-2 skeleton
 *	  implementation.
 *
 *	  Spec-2.19 ships the lifecycle skeleton + deadlock-detection ownership
 *	  migration from spec-2.17 caller-side 4-node placeholder to LMD.  Single
 *	  ownership path with fail-closed semantics — no runtime caller-side
 *	  fallback grant (§1.4.5 v0.2 P1.3 硬化).  Real Tarjan cycle detection,
 *	  wait-for graph maintenance, victim selection, cancellation all defer
 *	  to spec-2.20+ (7-step state machine activation) + spec-5.9 (victim +
 *	  cancellation).
 *
 *	  HC1 fail-closed:LMD READY 后 caller-side legacy hard-disabled.  LMD
 *	  unavailable (crash / state != READY) → backend receives SQLSTATE
 *	  53R81 (Step 4).
 *
 *	  HC2 4-state semantic split:DISABLED (lmd_enabled=off startup-only)
 *	  vs NOT_STARTED / STARTING / DRAINING / STOPPED vs READY is
 *	  distinguished in pg_cluster_lmd view + 53R81 reason field (Step 4).
 *
 *	  HC3 ConditionVariable substrate:producer-side wake API is wired in
 *	  this spec.  The skeleton loop does not maintain a graph yet; it
 *	  observes submission_count deltas on its bounded idle loop.  A real
 *	  CV consumer is deferred until the production graph-maintenance spec.
 *
 *	  HC4 single ownership EXACT predicate.  Public helper
 *	  cluster_lmd_is_ready() reads lmd_state atomic and returns true iff
 *	  state == CLUSTER_LMD_READY.  禁止 `state >= LMD_READY` 数值比较
 *	  (v0.3 codex P1.5 catch — enum 不连续值 DRAINING=3 / STOPPED=4 /
 *	  DISABLED=5 让 `>=` 误判).
 *
 *	  HC5 NUM_AUXILIARY_PROCS bump:LMD is 8th cluster aux process (after
 *	  LMON / LCK / DIAG / Cluster Stats / CSSD / QVOTEC / LMS);
 *	  src/include/storage/proc.h bumps NUM_AUXILIARY_PROCS 13 → 14
 *	  (D3b deliverable;I11 inherit spec-2.18).
 *
 *	  HC6 skeleton 占位不等于假装工作:LMD 不保存 wait edge,不维护 ring/
 *	  hash/queue;cluster_lmd_submit_wait_edge() 调用即 atomic ++
 *	  submission_count + ConditionVariableBroadcast(cv);LMD main loop
 *	  observes submission_count delta only (no dequeue, no graph
 *	  maintenance — defer spec-2.20+ with producer/consumer 同 spec ship
 *	  L114 family).
 *
 *	  HC7 early SIGTERM handler discipline:auxprocess.c LmdProcess branch
 *	  installs SIGTERM/SIGINT/SIGHUP handlers + sigprocmask UnBlockSig
 *	  BEFORE pgstat_bestart / pgstat-visible publication / any LMD
 *	  shmem-LWLock-CV operation (L121 spec-2.18 F1 root fix).  This file
 *	  re-installs handlers as a defense-in-depth (matches LMS pattern).
 *
 *	  I14 invariant:LMD skeleton MUST NOT register new ProcSignal slot
 *	  (auxprocess.c skips ProcSignalInit for LmdProcess);若 spec-2.20+
 *	  需引入 ProcSignal MUST 同 spec ship 完整 register + cleanup +
 *	  shutdown 语义 (L114 producer-consumer lifecycle 闭环 family).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.19-lmd-daemon-deadlock-ownership-migration.md
 *	  (FROZEN v0.3 2026-05-14 user approve).
 *	  Anchor: cluster_lms.c (spec-2.18) for skeleton structure;LMD adds
 *	  submit_wait_edge producer stub (HC6) and exact-predicate readiness
 *	  check (HC4 v0.3 P1.5).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
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
 * Idle sleep timeout for the skeleton loop.  Producer-side
 * ConditionVariableBroadcast() is retained as the forward-compatible API,
 * but this no-graph skeleton uses the ordinary aux-process latch path until
 * spec-2.20+ wires a real graph consumer.
 */
#define LMD_IDLE_TIMEOUT_MS 100


/* External hook from postmaster.c (mirrors LMS Q2 thin proxy). */
extern pid_t cluster_postmaster_start_lmd(void);


/* ============================================================
 * Module-local state.
 * ============================================================ */

static ClusterLmdSharedState *cluster_lmd_state = NULL;

static const char *cluster_lmd_state_strings[] = {
	"not_started", "starting", "ready", "draining", "stopped", "disabled",
};

/*
 * spec-1.3 region registry descriptor.  Registered once at postmaster
 * startup via cluster_lmd_shmem_register().
 */
static const ClusterShmemRegion cluster_lmd_region = {
	.name = "pgrac cluster lmd",
	.size_fn = cluster_lmd_shmem_size,
	.init_fn = cluster_lmd_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-2.19 LMD",
	.reserved_flags = 0,
};


/* ============================================================
 * State string mapping.
 * ============================================================ */

const char *
cluster_lmd_state_to_string(ClusterLmdState s)
{
	if ((int)s < 0 || (int)s > CLUSTER_LMD_STATE_LAST)
		return "(unknown)";
	return cluster_lmd_state_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_lmd_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLmdSharedState));
}

void
cluster_lmd_shmem_init(void)
{
	bool found;

	cluster_lmd_state = (ClusterLmdSharedState *)ShmemInitStruct(
		"pgrac cluster lmd", sizeof(ClusterLmdSharedState), &found);

	if (!found) {
		memset(cluster_lmd_state, 0, sizeof(*cluster_lmd_state));
		LWLockInitialize(&cluster_lmd_state->lwlock, LWTRANCHE_CLUSTER_LMD);
		/*
		 * spec-2.19 §1.4.6 HC2 (d) — DISABLED state is set once at startup
		 * when cluster.lmd_enabled=off (PGC_POSTMASTER restart-only).  The
		 * caller-side legacy path then remains active as the唯一 fallback.
		 * NOT_STARTED otherwise — LMD process will transition through
		 * STARTING → READY when postmaster forks it at PM_RUN.
		 */
		pg_atomic_init_u32(&cluster_lmd_state->lmd_state,
						   cluster_lmd_enabled ? CLUSTER_LMD_NOT_STARTED : CLUSTER_LMD_DISABLED);
		/*
		 * HC6:no ring buffer / hash table / queue placeholder.  Only
		 * skeleton counters (6 atomic) and ConditionVariable substrate.
		 */
		pg_atomic_init_u64(&cluster_lmd_state->lmd_started_count, 0);
		pg_atomic_init_u64(&cluster_lmd_state->lmd_ready_at_us, 0);
		pg_atomic_init_u64(&cluster_lmd_state->lmd_edge_submission_count, 0);
		pg_atomic_init_u64(&cluster_lmd_state->lmd_wake_count, 0);
		pg_atomic_init_u64(&cluster_lmd_state->lmd_idle_count, 0);
		pg_atomic_init_u64(&cluster_lmd_state->lmd_error_count, 0);
		ConditionVariableInit(&cluster_lmd_state->cv);
	}
}

void
cluster_lmd_shmem_request(void)
{
	RequestAddinShmemSpace(cluster_lmd_shmem_size());
}

void
cluster_lmd_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lmd_region);
}

ClusterLmdSharedState *
cluster_lmd_shared_state(void)
{
	return cluster_lmd_state;
}


/* ============================================================
 * Internal helpers.
 * ============================================================ */

/*
 * Atomically transition state.  Caller must hold lwlock LW_EXCLUSIVE
 * for non-atomic field updates (pid / spawned_at / ready_at);
 * lmd_state itself is atomic so monotonic reads remain race-free
 * outside the lock.
 */
static void
lmd_set_state(ClusterLmdState new_state)
{
	pg_atomic_write_u32(&cluster_lmd_state->lmd_state, (uint32)new_state);
}

static ClusterLmdState
lmd_get_state(void)
{
	if (cluster_lmd_state == NULL)
		return CLUSTER_LMD_NOT_STARTED;
	return (ClusterLmdState)pg_atomic_read_u32(&cluster_lmd_state->lmd_state);
}

static bool
lmd_shutdown_requested(void)
{
	bool requested;

	if (cluster_lmd_state == NULL)
		return true;
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_SHARED);
	requested = cluster_lmd_state->shutdown_requested;
	LWLockRelease(&cluster_lmd_state->lwlock);
	return requested;
}


/* ============================================================
 * Postmaster-side API (Q1 thin proxy / Q3 bounded polling).
 * ============================================================ */

int
cluster_lmd_start(void)
{
	pid_t pid;

	Assert(!IsUnderPostmaster);

	/*
	 * Honor lmd_enabled = off (PGC_POSTMASTER startup-time fallback;HC1).
	 * Caller (postmaster phase 4 driver) checks GUC and skips this start()
	 * entirely when disabled;defense in depth here marks DISABLED state
	 * to make the SQL view surface accurate.
	 *
	 * Note: the GUC itself lands in Step 4 (D12);until then this branch
	 * is dead code that the compiler will optimize away.
	 */

	pid = cluster_postmaster_start_lmd();
	return (int)pid;
}

bool
cluster_lmd_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_lmd_state == NULL)
		return false;

	/*
	 * DISABLED is a legitimate "ready-or-skip" terminal:when lmd_enabled =
	 * off at startup, LMD process never forks, so the postmaster phase 4
	 * driver should not block waiting for READY.  Return true immediately
	 * to skip the polling loop.
	 */
	if (lmd_get_state() == CLUSTER_LMD_DISABLED)
		return true;

	while (waited_ms < timeout_ms) {
		ClusterLmdState state = lmd_get_state();

		if (state == CLUSTER_LMD_READY)
			return true;

		/* Early failure: shutdown / stopped before ready. */
		if (state == CLUSTER_LMD_DRAINING || state == CLUSTER_LMD_STOPPED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}

void
cluster_lmd_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_lmd_state == NULL)
		return;

	LWLockAcquire(&cluster_lmd_state->lwlock, LW_EXCLUSIVE);
	cluster_lmd_state->shutdown_requested = true;
	LWLockRelease(&cluster_lmd_state->lwlock);

	/* Wake the future CV waiter; current skeleton also has latch timeout fallback. */
	ConditionVariableBroadcast(&cluster_lmd_state->cv);
}

void
cluster_lmd_mark_child_exit(void)
{
	ClusterLmdState state;

	if (cluster_lmd_state == NULL)
		return;

	/*
	 * Called from the postmaster reaper.  Do not take the LMD LWLock here:
	 * if the child died while holding it, the postmaster must not block.
	 * The atomic state transition is sufficient for caller-side ownership
	 * gates to fail closed after reaper harvest.  The child is gone, so
	 * clearing the diagnostic pid is safe without synchronizing with LMD.
	 */
	state = lmd_get_state();
	if (state != CLUSTER_LMD_DISABLED) {
		cluster_lmd_state->pid = 0;
		lmd_set_state(CLUSTER_LMD_STOPPED);
	}
}


/* ============================================================
 * Read-only accessors (LW_SHARED + atomic reads).
 * ============================================================ */

ClusterLmdState
cluster_lmd_get_state(void)
{
	return lmd_get_state();
}

pid_t
cluster_lmd_get_pid(void)
{
	pid_t pid;

	if (cluster_lmd_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_SHARED);
	pid = cluster_lmd_state->pid;
	LWLockRelease(&cluster_lmd_state->lwlock);
	return pid;
}

TimestampTz
cluster_lmd_get_spawned_at(void)
{
	TimestampTz t;

	if (cluster_lmd_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_SHARED);
	t = cluster_lmd_state->spawned_at;
	LWLockRelease(&cluster_lmd_state->lwlock);
	return t;
}

TimestampTz
cluster_lmd_get_ready_at(void)
{
	TimestampTz t;

	if (cluster_lmd_state == NULL)
		return 0;
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_SHARED);
	t = cluster_lmd_state->ready_at;
	LWLockRelease(&cluster_lmd_state->lwlock);
	return t;
}

uint64
cluster_lmd_get_started_count(void)
{
	if (cluster_lmd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_state->lmd_started_count);
}

uint64
cluster_lmd_get_edge_submission_count(void)
{
	if (cluster_lmd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_state->lmd_edge_submission_count);
}

uint64
cluster_lmd_get_wake_count(void)
{
	if (cluster_lmd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_state->lmd_wake_count);
}

uint64
cluster_lmd_get_idle_count(void)
{
	if (cluster_lmd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_state->lmd_idle_count);
}

uint64
cluster_lmd_get_error_count(void)
{
	if (cluster_lmd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_lmd_state->lmd_error_count);
}


/* ============================================================
 * HC4 single ownership EXACT predicate (v0.3 codex P1.5).
 *
 * spec-2.17 caller-side 4-node placeholder + future spec-2.20+ wait-edge
 * submitter call this to determine whether LMD owns deadlock detection.
 * Returns true iff state == CLUSTER_LMD_READY.  Read-only atomic load —
 * no LWLock needed and no postmaster-blocking dependency.  The postmaster
 * reaper calls cluster_lmd_mark_child_exit() to atomically clear stale
 * READY after child death.
 *
 * **禁止使用 `state >= LMD_READY` 数值比较** — enum 不连续值
 * (DRAINING=3 / STOPPED=4 / DISABLED=5) 让 `>=` 误判.  All caller-side
 * ownership gates MUST go through this helper or compare exact
 * == CLUSTER_LMD_READY.
 * ============================================================ */

bool
cluster_lmd_is_ready(void)
{
	if (cluster_lmd_state == NULL)
		return false;

	return ((ClusterLmdState)pg_atomic_read_u32(&cluster_lmd_state->lmd_state))
		   == CLUSTER_LMD_READY;
}


/* ============================================================
 * HC3 producer wake / HC6 skeleton "no graph maintenance".
 *
 * cluster_lmd_submit_wait_edge() — called by spec-2.17 caller-side
 * placeholder (D8) when gated by cluster_lmd_is_ready() (HC4).
 * Increments lmd_edge_submission_count atomically and broadcasts cv.
 *
 * HC6:不保存 wait edge;只 ++ counter + CV broadcast (no-op consumer-wise
 * for skeleton; the LMD main loop observes submission_count delta only).
 * Real graph maintenance + Tarjan 推 spec-2.20+ 同 spec ship producer +
 * consumer (L114 family).
 *
 * No-op when LMD shmem not yet attached (cluster_lmd_state == NULL).
 * ============================================================ */

void
cluster_lmd_submit_wait_edge(void)
{
	if (cluster_lmd_state == NULL)
		return;
	if (lmd_get_state() == CLUSTER_LMD_DISABLED)
		return;
	pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_edge_submission_count, 1);
	ConditionVariableBroadcast(&cluster_lmd_state->cv);
}

/*
 * spec-2.21 D7 — cluster_lmd_cancel_wait_edge symmetric stub.
 *
 *	Reuses lmd_edge_submission_count atomic for now (real cancel counter +
 *	wait-edge unregister lands in spec-2.22 LMD Tarjan).  No-op when LMD
 *	shmem is uninitialized or disabled (callers from S7 cleanup may run
 *	before LMD shmem is reachable in some early-bootstrap paths).
 */
void
cluster_lmd_cancel_wait_edge(void)
{
	if (cluster_lmd_state == NULL)
		return;
	if (lmd_get_state() == CLUSTER_LMD_DISABLED)
		return;
	/* spec-2.22 wires a dedicated cancel counter; for now share submission_count. */
	pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_edge_submission_count, 1);
}


/* ============================================================
 * LMD main entry.
 *
 *	Invoked from auxprocess.c dispatch when MyAuxProcType == LmdProcess.
 *	Runs the skeleton main loop until shutdown.  Producer-side
 *	ConditionVariable broadcast is retained as the API surface; the
 *	current no-graph skeleton uses the ordinary aux-process latch path and
 *	observes producer deltas on each bounded tick.
 *
 *	Per-iteration:read lmd_edge_submission_count atomically;if delta >
 *	cached seen_submission_count, increment lmd_wake_count (HC6 "real
 *	work" signal);else increment lmd_idle_count.  Then WaitLatch with the
 *	LMD idle wait event.
 * ============================================================ */

void
LmdMain(void)
{
	uint64 seen_submission_count;

	Assert(IsUnderPostmaster);

	MyBackendType = B_LMD;
	init_ps_display(NULL);

	/*
	 * Standard PG aux-process signal layout (modeled on cluster_lms.c).
	 * HC7 / L121:auxprocess.c already installed these BEFORE pgstat_bestart
	 * to close the pgstat-visible / pre-LmdMain window; we re-install here
	 * as defense-in-depth (matches LMS pattern).
	 */
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

	if (cluster_lmd_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_lmd shmem region not attached"),
				 errhint("cluster_lmd_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish STARTING + record pid / spawned_at. */
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_EXCLUSIVE);
	cluster_lmd_state->pid = MyProcPid;
	cluster_lmd_state->spawned_at = GetCurrentTimestamp();
	lmd_set_state(CLUSTER_LMD_STARTING);
	LWLockRelease(&cluster_lmd_state->lwlock);

	/* Transition to READY. */
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_EXCLUSIVE);
	cluster_lmd_state->ready_at = GetCurrentTimestamp();
	pg_atomic_write_u64(&cluster_lmd_state->lmd_ready_at_us, (uint64)cluster_lmd_state->ready_at);
	pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_started_count, 1);
	lmd_set_state(CLUSTER_LMD_READY);
	LWLockRelease(&cluster_lmd_state->lwlock);

	/* HC6 skeleton main loop:observe submission_count delta. */
	seen_submission_count = pg_atomic_read_u64(&cluster_lmd_state->lmd_edge_submission_count);

	for (;;) {
		uint64 current_submission_count;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || lmd_shutdown_requested())
			break;

		/*
		 * HC6:LMD does not consume / dequeue / maintain graph.  Observe
		 * submission_count delta only — distinguishes producer-triggered
		 * wake from idle-timeout poll.  Real Tarjan + graph maintenance
		 * defers to spec-2.20+ with producer/consumer 同 spec ship.
		 */
		current_submission_count
			= pg_atomic_read_u64(&cluster_lmd_state->lmd_edge_submission_count);

		if (current_submission_count > seen_submission_count) {
			pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_wake_count, 1);
			seen_submission_count = current_submission_count;
			continue;
		}

		pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_idle_count, 1);
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						LMD_IDLE_TIMEOUT_MS, WAIT_EVENT_CLUSTER_LMD_IDLE);
		ResetLatch(MyLatch);
	}

	/* Transition to DRAINING then STOPPED. */
	LWLockAcquire(&cluster_lmd_state->lwlock, LW_EXCLUSIVE);
	lmd_set_state(CLUSTER_LMD_DRAINING);
	LWLockRelease(&cluster_lmd_state->lwlock);

	LWLockAcquire(&cluster_lmd_state->lwlock, LW_EXCLUSIVE);
	cluster_lmd_state->pid = 0;
	cluster_lmd_state->stopped_at = GetCurrentTimestamp();
	lmd_set_state(CLUSTER_LMD_STOPPED);
	LWLockRelease(&cluster_lmd_state->lwlock);

	proc_exit(0);
}
