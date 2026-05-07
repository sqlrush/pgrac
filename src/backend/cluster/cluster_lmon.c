/*-------------------------------------------------------------------------
 *
 * cluster_lmon.c
 *	  pgrac LMON (Lock Monitor) cluster background process — Stage 1.11
 *	  Sprint A skeleton implementation.
 *
 *	  See cluster_lmon.h for the architectural overview, HC1-HC6 hard
 *	  constraints, and Q1-Q3 implementation details.
 *
 *	  Ship status (Hardening v1.0.1 codex review P2-4 cleanup;
 *	  obsolete Sprint A/B planning blocks removed 2026-05-07):
 *	    - Sprint A skeleton (shmem state + bounded-polling readiness +
 *	      main loop liveness tick + shutdown protocol + start proxy
 *	      to postmaster.c) shipped per spec-1.11 v0.2.
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

#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_tier1.h"
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
	 * spec-1.14.1 F20: when inject 'skip' is armed on cluster-lmon-
	 * pre-spawn, simulate a spawn failure (return 0) WITHOUT firing
	 * ereport.  This is the only way to truly exercise the
	 * 53R0A LMON_SPAWN_FAILED path in phase_1_handler:
	 *   start() returns 0 → handler fills fail_ctx with 53R0A →
	 *   driver ereport(FATAL, 53R0A, ...).
	 *
	 * Without this skip path, inject 'error' fires ereport(ERROR)
	 * with generic ERRCODE_INTERNAL_ERROR — bypassing the 53R0A
	 * plumbing entirely.  TAP regression: 061 L9.
	 */
	if (cluster_injection_should_skip("cluster-lmon-pre-spawn"))
		return 0;

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


/*
 * Spec-1.11.1 F11 (codex round 4 P2 fix): LW_SHARED accessors for the
 * 5 lmon_* fields that Sprint B D12 left out of the view.  Each
 * acquires the per-region lwlock briefly and copies one scalar out;
 * call from any backend context.
 */

pid_t
cluster_lmon_pid(void)
{
	pid_t result;

	if (cluster_lmon_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->pid;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return result;
}

TimestampTz
cluster_lmon_spawned_at(void)
{
	TimestampTz result;

	if (cluster_lmon_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->spawned_at;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return result;
}

TimestampTz
cluster_lmon_ready_at(void)
{
	TimestampTz result;

	if (cluster_lmon_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->ready_at;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return result;
}

TimestampTz
cluster_lmon_last_liveness_tick_at(void)
{
	TimestampTz result;

	if (cluster_lmon_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->last_liveness_tick_at;
	LWLockRelease(&cluster_lmon_state->lwlock);
	return result;
}

int64
cluster_lmon_main_loop_iters(void)
{
	int64 result;

	if (cluster_lmon_state == NULL)
		return 0;

	LWLockAcquire(&cluster_lmon_state->lwlock, LW_SHARED);
	result = cluster_lmon_state->main_loop_iters;
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
	/*
	 * PGRAC: spec-1.11 v1.0.2 F16 — SPAWNING means a new LMON incarnation
	 * is starting.  Earlier code only wrote pid/spawned_at when
	 * spawned_at == 0, which leaves stale values from the previous
	 * incarnation after a normal-exit ServerLoop respawn (shmem is not
	 * recreated on normal exit, only on postmaster crash recovery).
	 * Refresh every field that scopes to a single incarnation
	 * unconditionally so SQL views (pg_cluster_state.lmon.lmon_pid /
	 * spawned_at / ready_at / last_liveness_tick_at / main_loop_iters)
	 * always reflect the live LMON.
	 */
	if (status == CLUSTER_LMON_SPAWNING) {
		cluster_lmon_state->pid = MyProcPid;
		cluster_lmon_state->spawned_at = now;
		cluster_lmon_state->ready_at = 0;
		cluster_lmon_state->last_liveness_tick_at = 0;
		cluster_lmon_state->main_loop_iters = 0;
	} else if (status == CLUSTER_LMON_READY) {
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
	 * spec-2.2 D5 Step 7 -- Tier1 listener bind (gated on cluster_enabled
	 * AND interconnect_tier == tier1).  Per spec-2.2 §3.7 cluster_enabled
	 * gate is double-checked (caller cluster_init_shmem already gated;
	 * defensive here too -- L15 belt-and-suspenders).  Per §3.10
	 * listener bind is the ONLY transport-setup path that may FATAL
	 * the postmaster; everything else (HELLO failure / connect failure
	 * / heartbeat timeout) is connection-level.
	 *
	 * Per §3.8 startup non-deadlock invariant: LMON READY := listener
	 * bind ok + accept loop running.  We do NOT wait for any peer to
	 * connect before publishing READY (that would deadlock when this
	 * node starts before its peers).
	 */
	if (cluster_enabled
		&& (ClusterICTier) cluster_interconnect_tier == CLUSTER_IC_TIER_1)
	{
		(void) cluster_ic_tier1_listener_bind();    /* FATAL on failure */
	}

	/*
	 * Move to READY.  In Tier1 mode this means listener is bound and
	 * accept loop is about to enter; peers will connect / handshake
	 * asynchronously over the lifetime of the loop.
	 */
	lmon_publish_status(CLUSTER_LMON_READY);

	/*
	 * Main loop: two flavours selected at startup.
	 *
	 * - Tier1 mode: WaitEventSet multiplexes the latch + listener fd
	 *   + heartbeat timer.  Per spec-2.2 约束 1, the existing
	 *   WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP wait event is reserved
	 *   for the IDLE tick path (no socket activity, waiting for next
	 *   heartbeat tick) and is NOT reused for IC socket waits; the 6
	 *   new wait events (ClusterICTcpAccept / ...Connect / ...Recv /
	 *   ...Send / ...HeartbeatWait / ...Reconnect) land in Step 8.
	 *   Until Step 8 ships those wait events, the listener event in
	 *   the WaitEventSet uses WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP
	 *   as a placeholder so pg_stat_activity still surfaces a sane
	 *   wait state.
	 *
	 * - Stub / mock mode: existing simple WaitLatch loop preserved
	 *   verbatim (regression baseline; spec-1.11 contract unchanged).
	 *
	 * HC6: tick is LOCAL liveness only -- no inter-node heartbeat
	 *      semantic in stub/mock mode.  In tier1 mode heartbeat is
	 *      transport liveness only, NOT membership (per §3.6 boundary).
	 */
	if (cluster_enabled
		&& (ClusterICTier) cluster_interconnect_tier == CLUSTER_IC_TIER_1)
	{
		/*
		 * spec-2.2 §2.1 Tier1 main loop.  Heartbeat cadence driven by
		 * cluster.interconnect_heartbeat_interval_ms (D7 GUC; default
		 * 1000, range 100..60000) -- read once at loop entry per
		 * PGC_POSTMASTER semantics (no SIGHUP refresh).
		 */
		const long  HEARTBEAT_INTERVAL_MS = cluster_interconnect_heartbeat_interval_ms;
		WaitEventSet *wes = NULL;
		int           listener_fd = cluster_ic_tier1_get_listener_fd();
		TimestampTz   next_heartbeat_at;

		/*
		 * Build the WaitEventSet.  Capacity: latch + listener +
		 * CLUSTER_MAX_NODES per-peer fds (Step 7 only adds latch +
		 * listener; per-peer fd registration in subsequent steps).
		 */
		wes = CreateWaitEventSet(CurrentMemoryContext, 2 + CLUSTER_MAX_NODES);
		AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
		if (listener_fd >= 0)
			AddWaitEventToSet(wes, WL_SOCKET_READABLE, listener_fd, NULL,
							  /* user_data tag = -1 means "listener" */
							  (void *) (uintptr_t) -1);
		/*
		 * spec-2.2 D8 / 约束 1 -- the WaitEventSetWait below uses
		 * WAIT_EVENT_CLUSTER_IC_HEARTBEAT_WAIT (timer-based wait until
		 * next heartbeat tick) for the IDLE-with-timeout path.  Per
		 * 约束 2 strict semantics, HEARTBEAT_WAIT is the timer wait
		 * NOT a socket recv; recv waits use TCP_RECV.
		 */

		next_heartbeat_at = GetCurrentTimestamp() + HEARTBEAT_INTERVAL_MS * INT64CONST(1000);

		for (;;) {
			WaitEvent ev[8];
			int       n_events;
			long      wait_ms;
			TimestampTz now;
			int32     i;

			CHECK_FOR_INTERRUPTS();

			if (ConfigReloadPending) {
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			if (ShutdownRequestPending || lmon_shutdown_requested())
				break;

			lmon_advance_liveness_tick();

			CLUSTER_INJECTION_POINT("cluster-lmon-main-loop-iter");

			now = GetCurrentTimestamp();
			wait_ms = (next_heartbeat_at > now)
				? (long) ((next_heartbeat_at - now) / 1000)    /* us -> ms */
				: 0;
			if (wait_ms < 0)
				wait_ms = 0;
			if (wait_ms > HEARTBEAT_INTERVAL_MS)
				wait_ms = HEARTBEAT_INTERVAL_MS;

			n_events = WaitEventSetWait(wes, wait_ms, ev,
										lengthof(ev),
										WAIT_EVENT_CLUSTER_IC_HEARTBEAT_WAIT);

			for (i = 0; i < n_events; i++)
			{
				if (ev[i].events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);
				}
				else if (ev[i].events & WL_SOCKET_READABLE)
				{
					/*
					 * Tag -1 in user_data => listener readable.  Step 7
					 * accepts the new fd and immediately closes it (no
					 * per-peer fd registration yet -- Steps 11+ wire
					 * the full HELLO handshake when 076 ClusterPair TAP
					 * exercises 2-node interaction).  For Step 7 the
					 * accept call itself proves the listener works +
					 * doesn't FATAL; full peer state machine drive is
					 * the next milestone.
					 */
					if ((intptr_t) ev[i].user_data == -1)
					{
						int peer_fd = -1;
						int32 peer_id = -1;

						if (cluster_ic_tier1_accept_one(&peer_fd, &peer_id))
						{
							/*
							 * Step 7 placeholder: close immediately.
							 * Step 11 will recv + verify HELLO and
							 * keep the fd in the WaitEventSet.
							 */
							if (peer_fd >= 0)
								(void) close(peer_fd);
						}
					}
				}
			}

			now = GetCurrentTimestamp();
			if (now >= next_heartbeat_at)
			{
				/*
				 * Step 7 heartbeat tick: bookkeeping only.  Step 11
				 * will iterate connected peers and call
				 * cluster_ic_tier1_send_heartbeat to actually emit
				 * heartbeats once 2-node TAP can exercise it.
				 */
				next_heartbeat_at = now + HEARTBEAT_INTERVAL_MS * INT64CONST(1000);
			}
		}

		FreeWaitEventSet(wes);
	}
	else
	{
		/* Stub / mock / disabled mode -- preserve spec-1.11 simple loop. */
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

			CLUSTER_INJECTION_POINT("cluster-lmon-main-loop-iter");

			rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   cluster_lmon_main_loop_interval,
						   WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP);
			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}
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
