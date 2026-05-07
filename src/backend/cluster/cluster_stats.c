/*-------------------------------------------------------------------------
 *
 * cluster_stats.c
 *	  pgrac Cluster Stats (Cluster Stats Process) cluster background process — Stage 1.14
 *	  Sprint A skeleton implementation.
 *
 *	  See cluster_stats.h for the architectural overview, HC1-HC6 hard
 *	  constraints, and Q1-Q3 implementation details.
 *
 *	  Ship status (Hardening v1.0.1 codex review P2-4 cleanup;
 *	  obsolete Sprint A/B planning blocks removed 2026-05-07):
 *	    - Sprint A skeleton (shmem state + bounded-polling readiness +
 *	      main loop liveness tick + shutdown protocol + start proxy
 *	      to postmaster.c) shipped per spec-1.14 v0.2.
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
 *	  src/backend/cluster/cluster_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.14-cluster-stats-skeleton.md (frozen 2026-05-04, Sprint A scope).
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
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_CLUSTER_STATS_MAIN_LOOP (1.11 Sprint B) */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_shmem.h"


/*
 * Postmaster-owned spawn wrapper (lives in postmaster.c, Q2).  Declared
 * here so cluster_stats_start can forward to it.  The implementation
 * calls StartChildProcess(ClusterStatsProcess) which is file-static in
 * postmaster.c.
 */
extern pid_t cluster_postmaster_start_stats(void);


/*
 * Module-level pointer to the Cluster Stats shmem region.  Set by
 * cluster_stats_shmem_init().  NULL only inside the cluster_unit test
 * harness when init was not invoked.
 */
static ClusterStatsSharedState *cluster_stats_state = NULL;


/* ============================================================
 * Status enum -> string lookup.
 * ============================================================ */

static const char *const cluster_stats_status_strings[] = {
	"not_started",	 /* CLUSTER_STATS_NOT_STARTED  = 0 */
	"spawning",		 /* CLUSTER_STATS_SPAWNING     = 1 */
	"ready",		 /* CLUSTER_STATS_READY        = 2 */
	"shutting_down", /* CLUSTER_STATS_SHUTTING_DOWN = 3 */
	"exited"		 /* CLUSTER_STATS_EXITED       = 4 */
};


const char *
cluster_stats_status_to_string(ClusterStatsStatus s)
{
	if ((int)s < 0 || (int)s > CLUSTER_STATS_STATUS_LAST)
		return "(unknown)";
	return cluster_stats_status_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_stats_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterStatsSharedState));
}


void
cluster_stats_shmem_init(void)
{
	bool found;

	cluster_stats_state = (ClusterStatsSharedState *)ShmemInitStruct(
		"pgrac cluster stats", sizeof(ClusterStatsSharedState), &found);

	if (!found) {
		memset(cluster_stats_state, 0, sizeof(*cluster_stats_state));
		LWLockInitialize(&cluster_stats_state->lwlock, LWTRANCHE_CLUSTER_STATS);
		cluster_stats_state->status = CLUSTER_STATS_NOT_STARTED;
	}
}


static const ClusterShmemRegion cluster_stats_region = {
	.name = "pgrac cluster stats",
	.size_fn = cluster_stats_shmem_size,
	.init_fn = cluster_stats_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_stats",
	.reserved_flags = 0,
};


void
cluster_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_stats_region);
}


/* ============================================================
 * Postmaster-side API (Q2 / Q3).
 * ============================================================ */

int
cluster_stats_start(void)
{
	pid_t pid;

	/* HC1 defense in depth — only postmaster spawns Cluster Stats. */
	Assert(!IsUnderPostmaster);

	/* Sprint B inject: pre-spawn (testable spawn-failure injection). */
	CLUSTER_INJECTION_POINT("cluster-stats-pre-spawn");

	/* spec-1.14.1 F20: see cluster_lmon_start; 'skip' simulates spawn
	 * failure without ereport so 53R10 plumbing can be verified. */
	if (cluster_injection_should_skip("cluster-stats-pre-spawn"))
		return 0;


	/*
	 * Q2 thin proxy: forward to the postmaster-owned wrapper that
	 * lives in postmaster.c.  cluster_stats.c does NOT directly call
	 * StartChildProcess (file-static) and does NOT bypass postmaster
	 * to fork on its own.
	 */
	pid = cluster_postmaster_start_stats();

	/* Sprint B inject: post-spawn (after fork; Cluster Stats main not yet active). */
	CLUSTER_INJECTION_POINT("cluster-stats-post-spawn");

	return (int)pid;
}


bool
cluster_stats_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_stats_state == NULL)
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
		ClusterStatsStatus status;

		LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
		status = cluster_stats_state->status;
		LWLockRelease(&cluster_stats_state->lwlock);

		if (status == CLUSTER_STATS_READY)
			return true;

		/*
		 * Watch for early failure: SHUTTING_DOWN / EXITED before READY
		 * means Cluster Stats crashed or shut down during startup -> fail fast
		 * instead of waiting full timeout.
		 */
		if (status == CLUSTER_STATS_SHUTTING_DOWN || status == CLUSTER_STATS_EXITED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}


void
cluster_stats_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_stats_state == NULL)
		return;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_EXCLUSIVE);
	cluster_stats_state->shutdown_requested = true;
	LWLockRelease(&cluster_stats_state->lwlock);
}


ClusterStatsStatus
cluster_stats_status(void)
{
	ClusterStatsStatus result;

	if (cluster_stats_state == NULL)
		return CLUSTER_STATS_NOT_STARTED;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->status;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}


/*
 * Spec-1.11.1 F11 (codex round 4 P2 fix): LW_SHARED accessors for the
 * 5 cluster_stats_* fields that Sprint B D12 left out of the view.  Each
 * acquires the per-region lwlock briefly and copies one scalar out;
 * call from any backend context.
 */

pid_t
cluster_stats_pid(void)
{
	pid_t result;

	if (cluster_stats_state == NULL)
		return 0;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->pid;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}

TimestampTz
cluster_stats_spawned_at(void)
{
	TimestampTz result;

	if (cluster_stats_state == NULL)
		return 0;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->spawned_at;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}

TimestampTz
cluster_stats_ready_at(void)
{
	TimestampTz result;

	if (cluster_stats_state == NULL)
		return 0;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->ready_at;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}

TimestampTz
cluster_stats_last_liveness_tick_at(void)
{
	TimestampTz result;

	if (cluster_stats_state == NULL)
		return 0;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->last_liveness_tick_at;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}

int64
cluster_stats_main_loop_iters(void)
{
	int64 result;

	if (cluster_stats_state == NULL)
		return 0;

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	result = cluster_stats_state->main_loop_iters;
	LWLockRelease(&cluster_stats_state->lwlock);
	return result;
}


/* ============================================================
 * Cluster Stats main entry (AuxiliaryProcessMain dispatch target).
 * ============================================================ */

/*
 * Sprint B main loop interval is the cluster.cluster_stats_main_loop_interval
 * PGC_SIGHUP GUC (default 1000ms; range [100, 60000]).  We re-read
 * the GUC every iteration so SIGHUP changes take effect on the next
 * tick.
 */


static void
stats_publish_status(ClusterStatsStatus status)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_stats_state != NULL);

	LWLockAcquire(&cluster_stats_state->lwlock, LW_EXCLUSIVE);
	cluster_stats_state->status = status;
	/*
	 * PGRAC: spec-1.14 v1.0.1 F16 — SPAWNING means a new Cluster Stats incarnation
	 * is starting.  Earlier code only wrote pid/spawned_at when
	 * spawned_at == 0, which leaves stale values from the previous
	 * incarnation after a normal-exit ServerLoop respawn (shmem is not
	 * recreated on normal exit, only on postmaster crash recovery).
	 * Refresh every field that scopes to a single incarnation
	 * unconditionally so SQL views (pg_cluster_state.cluster_stats.cluster_stats_pid /
	 * spawned_at / ready_at / last_liveness_tick_at / main_loop_iters)
	 * always reflect the live Cluster Stats.
	 */
	if (status == CLUSTER_STATS_SPAWNING) {
		cluster_stats_state->pid = MyProcPid;
		cluster_stats_state->spawned_at = now;
		cluster_stats_state->ready_at = 0;
		cluster_stats_state->last_liveness_tick_at = 0;
		cluster_stats_state->main_loop_iters = 0;
	} else if (status == CLUSTER_STATS_READY) {
		cluster_stats_state->ready_at = now;
	}
	LWLockRelease(&cluster_stats_state->lwlock);
}


static bool
stats_shutdown_requested(void)
{
	bool requested;

	Assert(cluster_stats_state != NULL);

	LWLockAcquire(&cluster_stats_state->lwlock, LW_SHARED);
	requested = cluster_stats_state->shutdown_requested;
	LWLockRelease(&cluster_stats_state->lwlock);
	return requested;
}


static void
stats_advance_liveness_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_stats_state != NULL);

	/*
	 * HC6: this is a LOCAL liveness tick.  It is NOT inter-node
	 * heartbeat consumption (that protocol lives in spec-1.15+
	 * Heartbeat process).  Field name last_liveness_tick_at reflects
	 * that distinction.
	 */
	LWLockAcquire(&cluster_stats_state->lwlock, LW_EXCLUSIVE);
	cluster_stats_state->last_liveness_tick_at = now;
	cluster_stats_state->main_loop_iters++;
	LWLockRelease(&cluster_stats_state->lwlock);
}


void
ClusterStatsMain(void)
{
	/*
	 * HC1 reverse defense: ClusterStatsMain runs in the Cluster Stats child, so
	 * IsUnderPostmaster MUST be true.  Catch a misconfigured
	 * single-user / standalone path that accidentally invokes us.
	 */
	Assert(IsUnderPostmaster);

	MyBackendType = B_CLUSTER_STATS;
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

	if (cluster_stats_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_stats shmem region not attached"),
				 errhint("cluster_stats_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish SPAWNING (records pid + spawned_at). */
	stats_publish_status(CLUSTER_STATS_SPAWNING);

	/* Sprint B inject: ready-publish (test slow startup / phase 1 wait timeout). */
	CLUSTER_INJECTION_POINT("cluster-stats-ready-publish");

	/*
	 * Sprint A has no startup-side initialization beyond shmem state
	 * registration (no interconnect, no heartbeat consumer, no GRD).
	 * Move directly to READY.
	 */
	stats_publish_status(CLUSTER_STATS_READY);

	/*
	 * Sprint B main loop — WaitLatch with explicit timeout +
	 * WAIT_EVENT_CLUSTER_BGPROC_CLUSTER_STATS_MAIN_LOOP wait event so
	 * pg_stat_activity surfaces idle Cluster Stats cleanly.  GUC-driven
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

		if (ShutdownRequestPending || stats_shutdown_requested())
			break;

		stats_advance_liveness_tick();

		/* Sprint B inject: main-loop-iter (test mid-loop fault). */
		CLUSTER_INJECTION_POINT("cluster-stats-main-loop-iter");

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_cluster_stats_main_loop_interval,
					   WAIT_EVENT_CLUSTER_BGPROC_CLUSTER_STATS_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/* Sprint B inject: shutdown-pre (test cleanup-time fault). */
	CLUSTER_INJECTION_POINT("cluster-stats-shutdown-pre");

	/* Graceful shutdown path — HC5 normal exit. */
	stats_publish_status(CLUSTER_STATS_SHUTTING_DOWN);

	/* No cleanup work in Sprint A skeleton (no interconnect / GRD / etc). */

	stats_publish_status(CLUSTER_STATS_EXITED);

	/* Sprint B inject: shutdown-post (test final exit-code path). */
	CLUSTER_INJECTION_POINT("cluster-stats-shutdown-post");

	/*
	 * proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 ->
	 * normal-exit path -> NO crash recovery (HC5).  Abnormal exit
	 * (SIGSEGV / abort() / non-zero exit) hits HandleChildCrash ->
	 * restart_after_crash decides instance-level cycle.
	 */
	proc_exit(0);
}

#endif /* USE_PGRAC_CLUSTER */
