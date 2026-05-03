/*-------------------------------------------------------------------------
 *
 * cluster_lmon.c
 *	  pgrac LMON (Lock Monitor) cluster background process — Stage 1.11
 *	  Sprint A skeleton implementation.
 *
 *	  See cluster_lmon.h for the architectural overview, HC1-HC6 hard
 *	  constraints, and Q1-Q3 implementation details.
 *
 *	  Sprint A scope summary (this file):
 *	    - shmem state (ClusterLmonSharedState + LWTRANCHE_CLUSTER_LMON)
 *	    - bounded-polling readiness sync (cluster_lmon_wait_for_ready)
 *	    - LmonMain main loop = local liveness tick (HC6)
 *	    - shutdown protocol (cluster_lmon_request_shutdown +
 *	      shutdown_requested poll in main loop)
 *	    - cluster_lmon_start as thin proxy to
 *	      cluster_postmaster_start_lmon (lives in postmaster.c, Q2)
 *
 *	  NOT in Sprint A (deferred to Sprint B):
 *	    - cluster.lmon_main_loop_interval GUC (Sprint A uses 1000ms hardcoded)
 *	    - 53R0A LMON_SPAWN_FAILED / 53R0B LMON_NOT_READY SQLSTATE
 *	      (Sprint A uses ERRCODE_INTERNAL_ERROR + diagnostic errmsg)
 *	    - 6 cluster-lmon-* inject points
 *	    - WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP wait event
 *	    - dump_lmon view 6 keys
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmon.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.11-lmon-skeleton.md (frozen 2026-05-04, Sprint A scope).
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
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP (1.11 Sprint B) */

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"


/*
 * Postmaster-owned spawn wrapper (lives in postmaster.c, Q2).  Declared
 * here so cluster_lmon_start can forward to it.  The implementation
 * calls StartChildProcess(LmonProcess) which is file-static in
 * postmaster.c.
 */
extern pid_t cluster_postmaster_start_lmon(void);


/*
 * Module-level pointer to the LMON shmem region.  Set by
 * cluster_lmon_shmem_init().  NULL only inside the cluster_unit test
 * harness when init was not invoked.
 */
static ClusterLmonSharedState *cluster_lmon_state = NULL;


/* ============================================================
 * Status enum -> string lookup.
 * ============================================================ */

static const char *const cluster_lmon_status_strings[] = {
	"not_started",	 /* CLUSTER_LMON_NOT_STARTED  = 0 */
	"spawning",		 /* CLUSTER_LMON_SPAWNING     = 1 */
	"ready",		 /* CLUSTER_LMON_READY        = 2 */
	"shutting_down", /* CLUSTER_LMON_SHUTTING_DOWN = 3 */
	"exited"		 /* CLUSTER_LMON_EXITED       = 4 */
};


const char *
cluster_lmon_status_to_string(ClusterLmonStatus s)
{
	if ((int)s < 0 || (int)s > CLUSTER_LMON_STATUS_LAST)
		return "(unknown)";
	return cluster_lmon_status_strings[(int)s];
}


/* ============================================================
 * shmem region helpers (spec-1.3 registry-backed).
 * ============================================================ */

Size
cluster_lmon_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLmonSharedState));
}


void
cluster_lmon_shmem_init(void)
{
	bool found;

	cluster_lmon_state = (ClusterLmonSharedState *)ShmemInitStruct(
		"pgrac cluster lmon", sizeof(ClusterLmonSharedState), &found);

	if (!found) {
		memset(cluster_lmon_state, 0, sizeof(*cluster_lmon_state));
		LWLockInitialize(&cluster_lmon_state->lwlock, LWTRANCHE_CLUSTER_LMON);
		cluster_lmon_state->status = CLUSTER_LMON_NOT_STARTED;
	}
}


static const ClusterShmemRegion cluster_lmon_region = {
	.name = "pgrac cluster lmon",
	.size_fn = cluster_lmon_shmem_size,
	.init_fn = cluster_lmon_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_lmon",
	.reserved_flags = 0,
};


void
cluster_lmon_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lmon_region);
}


/* ============================================================
 * Postmaster-side API (Q2 / Q3).
 * ============================================================ */

int
cluster_lmon_start(void)
{
	pid_t pid;

	/* HC1 defense in depth — only postmaster spawns LMON. */
	Assert(!IsUnderPostmaster);

	/* Sprint B inject: pre-spawn (testable spawn-failure injection). */
	CLUSTER_INJECTION_POINT("cluster-lmon-pre-spawn");

	/*
	 * Q2 thin proxy: forward to the postmaster-owned wrapper that
	 * lives in postmaster.c.  cluster_lmon.c does NOT directly call
	 * StartChildProcess (file-static) and does NOT bypass postmaster
	 * to fork on its own.
	 */
	pid = cluster_postmaster_start_lmon();

	/* Sprint B inject: post-spawn (after fork; LMON main not yet active). */
	CLUSTER_INJECTION_POINT("cluster-lmon-post-spawn");

	return (int)pid;
}


bool
cluster_lmon_wait_for_ready(int timeout_ms)
{
	const int poll_interval_ms = 100;
	int waited_ms = 0;

	Assert(!IsUnderPostmaster);

	if (cluster_lmon_state == NULL)
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
		ClusterLmonStatus status;

		LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
		status = cluster_lmon_state->status;
		LWLockRelease(&cluster_lmon_state->lwlock);

		if (status == CLUSTER_LMON_READY)
			return true;

		/*
		 * Watch for early failure: SHUTTING_DOWN / EXITED before READY
		 * means LMON crashed or shut down during startup -> fail fast
		 * instead of waiting full timeout.
		 */
		if (status == CLUSTER_LMON_SHUTTING_DOWN || status == CLUSTER_LMON_EXITED)
			return false;

		pg_usleep(poll_interval_ms * 1000L);
		waited_ms += poll_interval_ms;
	}

	return false;
}


void
cluster_lmon_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (cluster_lmon_state == NULL)
		return;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_EXCLUSIVE);
	cluster_lmon_state->shutdown_requested = true;
	LWLockRelease(&cluster_lmon_state->lwlock);
}


ClusterLmonStatus
cluster_lmon_status(void)
{
	ClusterLmonStatus result;

	if (cluster_lmon_state == NULL)
		return CLUSTER_LMON_NOT_STARTED;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->status;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return result;
}


/* ============================================================
 * LMON main entry (AuxiliaryProcessMain dispatch target).
 * ============================================================ */

/*
 * Sprint B main loop interval is the cluster.lmon_main_loop_interval
 * PGC_SIGHUP GUC (default 1000ms; range [100, 60000]).  We re-read
 * the GUC every iteration so SIGHUP changes take effect on the next
 * tick.
 */


static void
lmon_publish_status(ClusterLmonStatus status)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_lmon_state != NULL);

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_EXCLUSIVE);
	cluster_lmon_state->status = status;
	if (status == CLUSTER_LMON_SPAWNING && cluster_lmon_state->spawned_at == 0) {
		cluster_lmon_state->pid = MyProcPid;
		cluster_lmon_state->spawned_at = now;
	} else if (status == CLUSTER_LMON_READY && cluster_lmon_state->ready_at == 0) {
		cluster_lmon_state->ready_at = now;
	}
	LWLockRelease(&cluster_lmon_state->lwlock);
}


static bool
lmon_shutdown_requested(void)
{
	bool requested;

	Assert(cluster_lmon_state != NULL);

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	requested = cluster_lmon_state->shutdown_requested;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return requested;
}


static void
lmon_advance_liveness_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(cluster_lmon_state != NULL);

	/*
	 * HC6: this is a LOCAL liveness tick.  It is NOT inter-node
	 * heartbeat consumption (that protocol lives in spec-1.15+
	 * Heartbeat process).  Field name last_liveness_tick_at reflects
	 * that distinction.
	 */
	LWLockAcquire(&cluster_lmon_state->lwlock, LW_EXCLUSIVE);
	cluster_lmon_state->last_liveness_tick_at = now;
	cluster_lmon_state->main_loop_iters++;
	LWLockRelease(&cluster_lmon_state->lwlock);
}


void
LmonMain(void)
{
	/*
	 * HC1 reverse defense: LmonMain runs in the LMON child, so
	 * IsUnderPostmaster MUST be true.  Catch a misconfigured
	 * single-user / standalone path that accidentally invokes us.
	 */
	Assert(IsUnderPostmaster);

	MyBackendType = B_LMON;
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

	if (cluster_lmon_state == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_lmon shmem region not attached"),
				 errhint("cluster_lmon_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	/* Publish SPAWNING (records pid + spawned_at). */
	lmon_publish_status(CLUSTER_LMON_SPAWNING);

	/* Sprint B inject: ready-publish (test slow startup / phase 1 wait timeout). */
	CLUSTER_INJECTION_POINT("cluster-lmon-ready-publish");

	/*
	 * Sprint A has no startup-side initialization beyond shmem state
	 * registration (no interconnect, no heartbeat consumer, no GRD).
	 * Move directly to READY.
	 */
	lmon_publish_status(CLUSTER_LMON_READY);

	/*
	 * Sprint B main loop — WaitLatch with explicit timeout +
	 * WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP wait event so
	 * pg_stat_activity surfaces idle LMON cleanly.  GUC-driven
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

		if (ShutdownRequestPending || lmon_shutdown_requested())
			break;

		lmon_advance_liveness_tick();

		/* Sprint B inject: main-loop-iter (test mid-loop fault). */
		CLUSTER_INJECTION_POINT("cluster-lmon-main-loop-iter");

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_lmon_main_loop_interval, WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/* Sprint B inject: shutdown-pre (test cleanup-time fault). */
	CLUSTER_INJECTION_POINT("cluster-lmon-shutdown-pre");

	/* Graceful shutdown path — HC5 normal exit. */
	lmon_publish_status(CLUSTER_LMON_SHUTTING_DOWN);

	/* No cleanup work in Sprint A skeleton (no interconnect / GRD / etc). */

	lmon_publish_status(CLUSTER_LMON_EXITED);

	/* Sprint B inject: shutdown-post (test final exit-code path). */
	CLUSTER_INJECTION_POINT("cluster-lmon-shutdown-post");

	/*
	 * proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 ->
	 * normal-exit path -> NO crash recovery (HC5).  Abnormal exit
	 * (SIGSEGV / abort() / non-zero exit) hits HandleChildCrash ->
	 * restart_after_crash decides instance-level cycle.
	 */
	proc_exit(0);
}

#endif /* USE_PGRAC_CLUSTER */
