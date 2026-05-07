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
	if (cluster_enabled && (ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_1) {
		(void)cluster_ic_tier1_listener_bind(); /* FATAL on failure */
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
	if (cluster_enabled && (ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_1) {
		/*
		 * spec-2.2 Step 11 D5 -- LMON Tier1 peer drive.
		 *
		 * Per-peer LMON-local state machine drives N×(N-1)/2 mesh
		 * connections to CONNECTED.  Mesh role per §3.5: lower
		 * node_id is the active connector, higher is passive
		 * accepter.  Per-peer substates:
		 *
		 *   DOWN          : no fd; eligible for active reconnect
		 *                   on next attempt-tick (active role only)
		 *   CONNECT_PEND  : nonblocking connect() in flight; waiting
		 *                   for WL_SOCKET_WRITEABLE
		 *   HELLO_WAIT    : HELLO sent (active) or peer HELLO not yet
		 *                   verified (passive); waiting for
		 *                   WL_SOCKET_READABLE to recv + verify
		 *   CONNECTED     : HELLO verified; full duplex; heartbeat
		 *                   send on tick + heartbeat drain on read
		 *
		 * Anonymous accept slots: when listener accepts, peer's
		 * node_id isn't known until we recv + verify HELLO, so the
		 * fd lands in lmon_pending_fds[] until HELLO binds it to
		 * tier1_peer_fds[learned_peer_id] and lmon_peer_track[].
		 */
#define LMON_SUB_DOWN 0
#define LMON_SUB_CONNECT_PEND 1
#define LMON_SUB_HELLO_SENDING 2 /* Hardening v1.0.1 F1: active partial-send tail */
#define LMON_SUB_HELLO_WAIT 3	 /* (legacy; passive uses anon path now) */
#define LMON_SUB_CONNECTED 4

		typedef struct LmonPeerTrack {
			int fd;
			int8 substate;
			bool is_active;
			TimestampTz next_attempt_at;
			TimestampTz connect_started_at; /* F2: connect_timeout deadline base */
		} LmonPeerTrack;

		const long HEARTBEAT_INTERVAL_MS = cluster_interconnect_heartbeat_interval_ms;
		LmonPeerTrack lmon_peer_track[CLUSTER_MAX_NODES];
		int lmon_pending_fds[CLUSTER_MAX_NODES];
		WaitEventSet *wes = NULL;
		bool wes_dirty = true;
		int listener_fd = cluster_ic_tier1_get_listener_fd();
		TimestampTz next_heartbeat_at;
		int32 self_id = cluster_node_id;
		int32 pi;

		/* Init per-peer track from pgrac.conf membership + mesh role. */
		for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
			lmon_peer_track[pi].fd = -1;
			lmon_peer_track[pi].substate = LMON_SUB_DOWN;
			lmon_peer_track[pi].is_active = false;
			lmon_peer_track[pi].next_attempt_at = 0;
			lmon_peer_track[pi].connect_started_at = 0;
			lmon_pending_fds[pi] = -1;

			if (pi == self_id)
				continue;
			if (cluster_conf_lookup_node(pi) == NULL)
				continue; /* peer not declared in pgrac.conf */
			lmon_peer_track[pi].is_active
				= (cluster_ic_mesh_role_for_pair(self_id, pi) == CLUSTER_IC_MESH_ACTIVE);
		}

		next_heartbeat_at = GetCurrentTimestamp() + HEARTBEAT_INTERVAL_MS * INT64CONST(1000);

		for (;;) {
			WaitEvent ev[2 * CLUSTER_MAX_NODES + 4];
			int n_events;
			long wait_ms;
			TimestampTz now;
			int32 i;

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

			/*
			 * Active-role reconnect: for each DOWN peer where we are
			 * the active connector and back-off has elapsed, kick
			 * off a nonblocking connect.
			 */
			for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
				int new_fd = -1;

				if (pi == self_id)
					continue;
				if (!lmon_peer_track[pi].is_active)
					continue;
				if (lmon_peer_track[pi].substate != LMON_SUB_DOWN)
					continue;
				if (lmon_peer_track[pi].next_attempt_at > now)
					continue;

				lmon_peer_track[pi].next_attempt_at
					= now + HEARTBEAT_INTERVAL_MS * INT64CONST(1000);

				if (cluster_ic_tier1_connect_one(pi, &new_fd) && new_fd >= 0) {
					lmon_peer_track[pi].fd = new_fd;
					lmon_peer_track[pi].substate = LMON_SUB_CONNECT_PEND;
					lmon_peer_track[pi].connect_started_at = now; /* F2 timeout base */
					wes_dirty = true;
				}
			}

			/*
			 * Hardening v1.0.1 F2: timeout / liveness scan.
			 *
			 *  - CONNECT_PEND or HELLO_SENDING > connect_timeout_ms since
			 *    connect_started_at -> close (peer never came up).
			 *  - CONNECTED + last_heartbeat_recv_at older than 3x heartbeat
			 *    interval -> close (silent peer death; pre-fix kept a
			 *    connection alive for ~2 hours via TCP keepalive default).
			 *
			 * Both paths transition to DOWN; next reconnect tick re-attempts
			 * via the active-role connect loop above.  spec-2.2 §3.6:
			 * heartbeat liveness is transport-only; this drop does NOT
			 * trigger fence / membership change (that is spec-2.29).
			 */
			{
				int64 connect_to_us
					= (int64)cluster_interconnect_connect_timeout_ms * INT64CONST(1000);
				int64 liveness_to_us = 3L * (int64)HEARTBEAT_INTERVAL_MS * INT64CONST(1000);

				for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
					if (lmon_peer_track[pi].fd < 0)
						continue;

					if ((lmon_peer_track[pi].substate == LMON_SUB_CONNECT_PEND
						 || lmon_peer_track[pi].substate == LMON_SUB_HELLO_SENDING
						 || lmon_peer_track[pi].substate == LMON_SUB_HELLO_WAIT)
						&& lmon_peer_track[pi].connect_started_at > 0
						&& now > lmon_peer_track[pi].connect_started_at + connect_to_us) {
						cluster_ic_tier1_close_peer(pi, "connect timeout");
						lmon_peer_track[pi].fd = -1;
						lmon_peer_track[pi].substate = LMON_SUB_DOWN;
						lmon_peer_track[pi].connect_started_at = 0;
						wes_dirty = true;
						continue;
					}

					if (lmon_peer_track[pi].substate == LMON_SUB_CONNECTED) {
						const ClusterICPeerStateShmem *p = cluster_ic_tier1_peer_get(pi);
						TimestampTz last;

						if (p == NULL)
							continue;
						last = p->last_heartbeat_recv_at;

						/* Skip if no heartbeat ever received yet (just CONNECTED;
						 * give peer 1 full liveness window before judging). */
						if (last == 0)
							continue;
						if (now > last + liveness_to_us) {
							cluster_ic_tier1_close_peer(pi, "heartbeat liveness timeout");
							lmon_peer_track[pi].fd = -1;
							lmon_peer_track[pi].substate = LMON_SUB_DOWN;
							lmon_peer_track[pi].connect_started_at = 0;
							wes_dirty = true;
						}
					}
				}
			}

			/* Heartbeat send tick. */
			if (now >= next_heartbeat_at) {
				for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
					if (lmon_peer_track[pi].substate != LMON_SUB_CONNECTED)
						continue;
					if (!cluster_ic_tier1_send_heartbeat(pi)) {
						cluster_ic_tier1_close_peer(pi, "heartbeat send failed");
						lmon_peer_track[pi].fd = -1;
						lmon_peer_track[pi].substate = LMON_SUB_DOWN;
						wes_dirty = true;
					}
				}
				next_heartbeat_at = now + HEARTBEAT_INTERVAL_MS * INT64CONST(1000);
			}

			/* (Re)build WaitEventSet whenever per-peer fd set changes. */
			if (wes_dirty) {
				if (wes != NULL) {
					FreeWaitEventSet(wes);
					wes = NULL;
				}
				wes = CreateWaitEventSet(CurrentMemoryContext, 2 + 2 * CLUSTER_MAX_NODES);
				AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
				if (listener_fd >= 0)
					AddWaitEventToSet(wes, WL_SOCKET_READABLE, listener_fd, NULL,
									  (void *)(intptr_t)-1);

				/* Per-peer (post-HELLO-bound) fds. */
				for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
					int events;

					if (lmon_peer_track[pi].fd < 0)
						continue;

					switch (lmon_peer_track[pi].substate) {
					case LMON_SUB_CONNECT_PEND:
					case LMON_SUB_HELLO_SENDING: /* F1: WRITEABLE for partial-HELLO drain */
						events = WL_SOCKET_WRITEABLE;
						break;
					case LMON_SUB_HELLO_WAIT:
					case LMON_SUB_CONNECTED:
						events = WL_SOCKET_READABLE;
						break;
					default:
						continue;
					}

					AddWaitEventToSet(wes, events, lmon_peer_track[pi].fd, NULL,
									  (void *)(intptr_t)pi);
				}

				/* Anonymous pending accept fds (peer_id learnt via HELLO). */
				for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
					if (lmon_pending_fds[pi] < 0)
						continue;
					AddWaitEventToSet(wes, WL_SOCKET_READABLE, lmon_pending_fds[pi], NULL,
									  (void *)(intptr_t)(CLUSTER_MAX_NODES + pi));
				}
				wes_dirty = false;
			}

			now = GetCurrentTimestamp();
			wait_ms = (next_heartbeat_at > now) ? (long)((next_heartbeat_at - now) / 1000) : 0;
			if (wait_ms < 0)
				wait_ms = 0;
			if (wait_ms > HEARTBEAT_INTERVAL_MS)
				wait_ms = HEARTBEAT_INTERVAL_MS;

			n_events = WaitEventSetWait(wes, wait_ms, ev, lengthof(ev),
										WAIT_EVENT_CLUSTER_IC_HEARTBEAT_WAIT);

			for (i = 0; i < n_events; i++) {
				intptr_t tag = (intptr_t)ev[i].user_data;

				if (ev[i].events & WL_LATCH_SET) {
					ResetLatch(MyLatch);
					continue;
				}

				if (tag == -1) {
					/* Listener: drain all pending accepts. */
					for (;;) {
						int new_fd = -1;
						int32 dummy_peer_id = -1;
						int slot;

						if (!cluster_ic_tier1_accept_one(&new_fd, &dummy_peer_id))
							break;
						if (new_fd < 0)
							break;

						for (slot = 0; slot < CLUSTER_MAX_NODES; slot++)
							if (lmon_pending_fds[slot] < 0)
								break;
						if (slot >= CLUSTER_MAX_NODES) {
							/* No room -- reject by closing. */
							(void)close(new_fd);
							continue;
						}
						lmon_pending_fds[slot] = new_fd;
						wes_dirty = true;
					}
				} else if (tag >= 0 && tag < CLUSTER_MAX_NODES) {
					int32 peer = (int32)tag;
					int peer_fd = lmon_peer_track[peer].fd;

					if (peer_fd < 0)
						continue; /* lost between events */

					if (lmon_peer_track[peer].substate == LMON_SUB_CONNECT_PEND
						&& (ev[i].events & WL_SOCKET_WRITEABLE)) {
						/*
						 * spec-2.2 §2.4 + Hardening v1.0.1 F1: active side
						 * sends HELLO via per-peer buffer.  finish_connect
						 * does SO_ERROR check + seeds buffer + first send.
						 * If HELLO fully fits in one send, hello_send_remaining
						 * goes to 0 and peer state flips to CONNECTED inside
						 * continue_hello_send (called by finish_connect).
						 * If partial, transition to HELLO_SENDING and re-enter
						 * on next WRITEABLE.
						 */
						if (cluster_ic_tier1_finish_connect(peer, peer_fd)) {
							if (cluster_ic_tier1_hello_send_remaining(peer) == 0)
								lmon_peer_track[peer].substate = LMON_SUB_CONNECTED;
							else
								lmon_peer_track[peer].substate = LMON_SUB_HELLO_SENDING;
							wes_dirty = true;
						} else {
							lmon_peer_track[peer].fd = -1;
							lmon_peer_track[peer].substate = LMON_SUB_DOWN;
							wes_dirty = true;
						}
					} else if (lmon_peer_track[peer].substate == LMON_SUB_HELLO_SENDING
							   && (ev[i].events & WL_SOCKET_WRITEABLE)) {
						/* Hardening v1.0.1 F1: continue partial HELLO send. */
						if (cluster_ic_tier1_continue_hello_send(peer, peer_fd)) {
							if (cluster_ic_tier1_hello_send_remaining(peer) == 0) {
								lmon_peer_track[peer].substate = LMON_SUB_CONNECTED;
								wes_dirty = true;
							}
							/* else: still partial, keep WRITEABLE */
						} else {
							lmon_peer_track[peer].fd = -1;
							lmon_peer_track[peer].substate = LMON_SUB_DOWN;
							wes_dirty = true;
						}
					} else if (lmon_peer_track[peer].substate == LMON_SUB_CONNECTED
							   && (ev[i].events & WL_SOCKET_READABLE)) {
						if (!cluster_ic_tier1_recv_heartbeat_drain(peer, peer_fd)) {
							cluster_ic_tier1_close_peer(peer, "heartbeat recv failed");
							lmon_peer_track[peer].fd = -1;
							lmon_peer_track[peer].substate = LMON_SUB_DOWN;
							wes_dirty = true;
						}
					}
				} else if (tag >= CLUSTER_MAX_NODES && tag < 2 * CLUSTER_MAX_NODES) {
					int slot = (int)(tag - CLUSTER_MAX_NODES);
					int pend_fd = lmon_pending_fds[slot];
					int32 learned = -1;

					if (pend_fd < 0)
						continue;

					/*
					 * Hardening v1.0.1 F1: continue_hello_recv accumulates
					 * partial HELLO bytes into per-anon-slot buffer; returns
					 * true with learned == -1 while still partial, true with
					 * learned >= 0 when HELLO fully verified.
					 */
					if (cluster_ic_tier1_continue_hello_recv(slot, pend_fd, &learned)) {
						if (learned >= 0) {
							/* HELLO complete + verified.  Migrate fd into
							 * peer_track + free anon slot. */
							lmon_peer_track[learned].fd = pend_fd;
							lmon_peer_track[learned].substate = LMON_SUB_CONNECTED;
							lmon_pending_fds[slot] = -1;
							cluster_ic_tier1_anon_hello_reset(slot);
							wes_dirty = true;
						}
						/* else: still accumulating; keep fd registered as READABLE */
					} else {
						/* HELLO failed (parse / verify / EOF / error).
						 * Drop anonymous fd; reset slot accumulator. */
						(void)close(pend_fd);
						lmon_pending_fds[slot] = -1;
						cluster_ic_tier1_anon_hello_reset(slot);
						wes_dirty = true;
					}
				}
			}
		}

		/* Shutdown: close every fd we own + free WES. */
		for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
			if (lmon_peer_track[pi].fd >= 0)
				cluster_ic_tier1_close_peer(pi, "lmon shutdown");
			if (lmon_pending_fds[pi] >= 0) {
				(void)close(lmon_pending_fds[pi]);
				lmon_pending_fds[pi] = -1;
			}
		}
		if (wes != NULL)
			FreeWaitEventSet(wes);
	} else {
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
