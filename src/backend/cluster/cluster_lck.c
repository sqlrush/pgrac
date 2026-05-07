/*-------------------------------------------------------------------------
 *
 * cluster_lck.c
 *	  pgrac LCK (Lock Process) cluster background process — Stage 1.12
 *	  Sprint A skeleton implementation.
 *
 *	  See cluster_lck.h for the architectural overview, HC1-HC6 hard
 *	  constraints, and Q1-Q3 implementation details.
 *
 *	  Ship status (Hardening v1.0.1 codex review P2-4 cleanup;
 *	  obsolete Sprint A/B planning blocks removed 2026-05-07):
 *	    - Sprint A skeleton (shmem state + bounded-polling readiness +
 *	      main loop liveness tick + shutdown protocol + start proxy
 *	      to postmaster.c) shipped per spec-1.12 v0.2.
 *	    - Sprint B surfaces (interval GUC + dedicated SQLSTATE + inject
 *	      points + wait event + dump view) progressively shipped
 *	      through subsequent spec-1.X main commits and the cluster_*
 *	      framework families (inject / wait_events / gviews).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lck.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.12-lck-skeleton.md (frozen 2026-05-04, Sprint A scope).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP (1.11 Sprint B) */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lck.h"
#include "cluster/cluster_shmem.h"


/*
 * Postmaster-owned spawn wrapper (lives in postmaster.c, Q2).  Declared
 * here so cluster_lck_start can forward to it.  The implementation
 * calls StartChildProcess(LckProcess) which is file-static in
 * postmaster.c.
 */
extern pid_t cluster_postmaster_start_lck(void);


/*
 * Module-level pointer to the LCK shmem region.  Set by
 * cluster_lck_shmem_init().  NULL only inside the cluster_unit test
 * harness when init was not invoked.
 */
static ClusterLckSharedState *cluster_lck_state = NULL;


/* ============================================================
 * Status enum -> string lookup.
 * ============================================================ */

static const char *const cluster_lck_status_strings[] = {
	"not_started",	 /* CLUSTER_LCK_NOT_STARTED  = 0 */
	"spawning",		 /* CLUSTER_LCK_SPAWNING     = 1 */
	"ready",		 /* CLUSTER_LCK_READY        = 2 */
	"shutting_down", /* CLUSTER_LCK_SHUTTING_DOWN = 3 */
	"exited"		 /* CLUSTER_LCK_EXITED       = 4 */
};


const char *
cluster_lck_status_to_string(ClusterLckStatus s)
{
	if ((int)s < 0 || (int)s > CLUSTER_LCK_STATUS_LAST)
		return "(unknown)";
	return cluster_lck_status_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_lck_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLckSharedState));
}


void
cluster_lck_shmem_init(void)
{
	bool found;

	cluster_lck_state = (ClusterLckSharedState *)ShmemInitStruct(
		"pgrac cluster lck", sizeof(ClusterLckSharedState), &found);

	if (!found) {
		memset(cluster_lck_state, 0, sizeof(*cluster_lck_state));
		LWLockInitialize(&cluster_lck_state->lwlock, LWTRANCHE_CLUSTER_LCK);
		cluster_lck_state->status = CLUSTER_LCK_NOT_STARTED;
	}
}


static const ClusterShmemRegion cluster_lck_region = {
	.name = "pgrac cluster lck",
	.size_fn = cluster_lck_shmem_size,
	.init_fn = cluster_lck_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_lck",
	.reserved_flags = 0,
};


void
cluster_lck_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lck_region);
}


/* ============================================================
 * Postmaster-side API (Q2 / Q3).
 * ============================================================ */

int
cluster_lck_start(void)
{
	pid_t pid;

	/* HC1 defense in depth — only postmaster spawns LCK. */
	Assert(!IsUnderPostmaster);

	/* Sprint B inject: pre-spawn (testable spawn-failure injection). */
	CLUSTER_INJECTION_POINT("cluster-lck-pre-spawn");

	/* spec-1.14.1 F20: see cluster_lmon_start; 'skip' simulates spawn
	 * failure without ereport so 53R0C plumbing can be verified. */
	if (cluster_injection_should_skip("cluster-lck-pre-spawn"))
		return 0;

	/*
	 * Q2 thin proxy: forward to the postmaster-owned wrapper that
	 * lives in postmaster.c.  cluster_lck.c does NOT directly call
	 * StartChildProcess (file-static) and does NOT bypass postmaster
	 * to fork on its own.
	 */
	pid = cluster_postmaster_start_lck();

	/* Sprint B inject: post-spawn (after fork; LCK main not yet active). */
	CLUSTER_INJECTION_POINT("cluster-lck-post-spawn");

	return (int)pid;
}


bool
cluster_lck_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_lck_state == NULL)
		return false;

	/*
	 * Q3 bounded polling: postmaster pre-ServerLoop has limited latch
	 * infrastructure.  Polling shmem ready flag with pg_usleep(100ms)
	 * is the PG-idiomatic pattern for postmaster startup waits (cf.
	 * StartupProcess startup-coordination in PG xlog.c).  Sprint B
	 * may upgrade to InitSharedLatch + OwnLatch if telemetry shows
	 * 100ms granularity is limiting.
	 */
	while (waited_ms < timeout_ms) {
		ClusterLckStatus status;

		LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
		status = cluster_lck_state->status;
		LWLockRelease(&cluster_lck_state->lwlock);

		if (status == CLUSTER_LCK_READY)
			return true;

		/*
		 * Watch for early failure: SHUTTING_DOWN / EXITED before READY
		 * means LCK crashed or shut down during startup -> fail fast
		 * instead of waiting full timeout.
		 */
		if (status == CLUSTER_LCK_SHUTTING_DOWN || status == CLUSTER_LCK_EXITED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}


void
cluster_lck_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_lck_state == NULL)
		return;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_EXCLUSIVE);
	cluster_lck_state->shutdown_requested = true;
	LWLockRelease(&cluster_lck_state->lwlock);
}


ClusterLckStatus
cluster_lck_status(void)
{
	ClusterLckStatus result;

	if (cluster_lck_state == NULL)
		return CLUSTER_LCK_NOT_STARTED;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->status;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}


/*
 * Spec-1.11.1 F11 (codex round 4 P2 fix): LW_SHARED accessors for the
 * 5 lck_* fields that Sprint B D12 left out of the view.  Each
 * acquires the per-region lwlock briefly and copies one scalar out;
 * call from any backend context.
 */

pid_t
cluster_lck_pid(void)
{
	pid_t result;

	if (cluster_lck_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->pid;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}

TimestampTz
cluster_lck_spawned_at(void)
{
	TimestampTz result;

	if (cluster_lck_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->spawned_at;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}

TimestampTz
cluster_lck_ready_at(void)
{
	TimestampTz result;

	if (cluster_lck_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->ready_at;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}

TimestampTz
cluster_lck_last_liveness_tick_at(void)
{
	TimestampTz result;

	if (cluster_lck_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->last_liveness_tick_at;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}

int64
cluster_lck_main_loop_iters(void)
{
	int64 result;

	if (cluster_lck_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	result = cluster_lck_state->main_loop_iters;
	LWLockRelease(&cluster_lck_state->lwlock);
	return result;
}


/* ============================================================
 * LCK main entry (AuxiliaryProcessMain dispatch target).
 * ============================================================ */

/*
 * Sprint B main loop interval is the cluster.lck_main_loop_interval
 * PGC_SIGHUP GUC (default 1000ms; range [100, 60000]).  We re-read
 * the GUC every iteration so SIGHUP changes take effect on the next
 * tick.
 */


static void
lck_publish_status(ClusterLckStatus status)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_lck_state != NULL);

	LWLockAcquire(&cluster_lck_state->lwlock, LW_EXCLUSIVE);
	cluster_lck_state->status = status;
	/*
	 * PGRAC: spec-1.12 v1.0.1 F16 — SPAWNING means a new LCK incarnation
	 * is starting.  Earlier code only wrote pid/spawned_at when
	 * spawned_at == 0, which leaves stale values from the previous
	 * incarnation after a normal-exit ServerLoop respawn (shmem is not
	 * recreated on normal exit, only on postmaster crash recovery).
	 * Refresh every field that scopes to a single incarnation
	 * unconditionally so SQL views (pg_cluster_state.lck.lck_pid /
	 * spawned_at / ready_at / last_liveness_tick_at / main_loop_iters)
	 * always reflect the live LCK.
	 */
	if (status == CLUSTER_LCK_SPAWNING) {
		cluster_lck_state->pid = MyProcPid;
		cluster_lck_state->spawned_at = now;
		cluster_lck_state->ready_at = 0;
		cluster_lck_state->last_liveness_tick_at = 0;
		cluster_lck_state->main_loop_iters = 0;
	} else if (status == CLUSTER_LCK_READY) {
		cluster_lck_state->ready_at = now;
	}
	LWLockRelease(&cluster_lck_state->lwlock);
}


static bool
lck_shutdown_requested(void)
{
	bool requested;

	Assert(cluster_lck_state != NULL);

	LWLockAcquire(&cluster_lck_state->lwlock, LW_SHARED);
	requested = cluster_lck_state->shutdown_requested;
	LWLockRelease(&cluster_lck_state->lwlock);
	return requested;
}


static void
lck_advance_liveness_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_lck_state != NULL);

	/*
	 * HC6: this is a LOCAL liveness tick.  It is NOT inter-node
	 * heartbeat consumption (that protocol lives in spec-1.15+
	 * Heartbeat process).  Field name last_liveness_tick_at reflects
	 * that distinction.
	 */
	LWLockAcquire(&cluster_lck_state->lwlock, LW_EXCLUSIVE);
	cluster_lck_state->last_liveness_tick_at = now;
	cluster_lck_state->main_loop_iters++;
	LWLockRelease(&cluster_lck_state->lwlock);
}


void
LckMain(void)
{
	/*
	 * HC1 reverse defense: LckMain runs in the LCK child, so
	 * IsUnderPostmaster MUST be true.  Catch a misconfigured
	 * single-user / standalone path that accidentally invokes us.
	 */
	Assert(IsUnderPostmaster);

	MyBackendType = B_LCK;
	init_ps_display(NULL);

	/*
	 * Standard PG aux-process signal layout (modeled on walwriter.c):
	 *	SIGHUP  -> ProcessConfigFile reload (we have no GUC of our own
	 *	           in Sprint A, but reload remains responsive to global
	 *	           settings like log_min_messages)
	 *	SIGTERM/SIGINT -> ShutdownRequestPending (graceful exit)
	 *	SIGQUIT -> already set up by InitPostmasterChild (immediate)
	 *	SIGUSR1 -> procsignal_sigusr1_handler (PG procsignal)
	 *	SIGALRM/SIGPIPE/SIGUSR2 -> ignored
	 *	SIGCHLD -> default
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

	/* Unblock signals (default state was BlockSig in InitPostmasterChild). */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (cluster_lck_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_lck shmem region not attached"),
				 errhint("cluster_lck_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish SPAWNING (records pid + spawned_at). */
	lck_publish_status(CLUSTER_LCK_SPAWNING);

	/* Sprint B inject: ready-publish (test slow startup / phase 1 wait timeout). */
	CLUSTER_INJECTION_POINT("cluster-lck-ready-publish");

	/*
	 * Sprint A has no startup-side initialization beyond shmem state
	 * registration (no interconnect, no heartbeat consumer, no GRD).
	 * Move directly to READY.
	 */
	lck_publish_status(CLUSTER_LCK_READY);

	/*
	 * Sprint B main loop — WaitLatch with explicit timeout +
	 * WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP wait event so
	 * pg_stat_activity surfaces idle LCK cleanly.  GUC-driven
	 * interval (re-read each iteration so SIGHUP propagates on the
	 * next tick).
	 *
	 * HC6: tick is LOCAL liveness only — no inter-node heartbeat.
	 */
	for (;;) {
		int rc;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || lck_shutdown_requested())
			break;

		lck_advance_liveness_tick();

		/* Sprint B inject: main-loop-iter (test mid-loop fault). */
		CLUSTER_INJECTION_POINT("cluster-lck-main-loop-iter");

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_lck_main_loop_interval, WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/* Sprint B inject: shutdown-pre (test cleanup-time fault). */
	CLUSTER_INJECTION_POINT("cluster-lck-shutdown-pre");

	/* Graceful shutdown path — HC5 normal exit. */
	lck_publish_status(CLUSTER_LCK_SHUTTING_DOWN);

	/* No cleanup work in Sprint A skeleton (no interconnect / GRD / etc). */

	lck_publish_status(CLUSTER_LCK_EXITED);

	/* Sprint B inject: shutdown-post (test final exit-code path). */
	CLUSTER_INJECTION_POINT("cluster-lck-shutdown-post");

	/*
	 * proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 ->
	 * normal-exit path -> NO crash recovery (HC5).  Abnormal exit
	 * (SIGSEGV / abort() / non-zero exit) hits HandleChildCrash ->
	 * restart_after_crash decides instance-level cycle.
	 */
	proc_exit(0);
}

#endif /* USE_PGRAC_CLUSTER */
