/*-------------------------------------------------------------------------
 *
 * cluster_cssd.c
 *	  pgrac CSSD aux process implementation — spec-2.5 D2 (Sprint A
 *	  Step 2 — initial scaffolding;Step 3-5 wire MainLoop to LMON
 *	  outbound queue drain, GUC, wait event, inject points).
 *
 *	  See cluster_cssd.h for the API contract + spec-2.5 architecture
 *	  references (Q1 LMON-mediated send / Q3 fanout layer / Q6 first-
 *	  tick nonfatal grace period).
 *
 *	  Step 2 deliverable scope:
 *	    - StaticAssertDecl byte-layout locks for 12 B payload + 64 B
 *	      outbound slot (Q1 cache-line alignment)
 *	    - status_to_string / peer_state_to_string lookups
 *	    - ClusterCssdShmem layout + shmem_size / init / register
 *	    - LW_SHARED accessors for lifecycle + per-peer state
 *	    - CssdMain skeleton (WaitLatch + heartbeat tick + deadband-
 *	      scan loop;reads GUC interval if registered, else fallback
 *	      default 1000 ms;wait event + inject points wired in Step 5)
 *	    - dispatch_heartbeat handler (handler 4 hard constraints
 *	      enforced by source structure)
 *	    - lifecycle (start / wait_for_ready / request_shutdown stubs;
 *	      real impl ties into postmaster phase 4 driver in Step 4)
 *
 *	  Following step deliverables (NOT in Step 2):
 *	    Step 3: LMON tick drain CSSD outbound queue (modifies
 *	      cluster_lmon.c)
 *	    Step 4: D3 AuxProcType CssdProcess + D4 postmaster spawn
 *	      wrapper + D5 phase 4 driver third upgrade (DIAG + Stats +
 *	      CSSD)
 *	    Step 5: D6 cluster_shmem.c register + D7 LWTRANCHE +
 *	      D8 wait event + D9 GUC + D10 SQLSTATE + D11 inject points +
 *	      D12 phase 1 register msg_type=11 + D13 producer mask + D14
 *	      per-peer counter (in cluster_pgstat.c)
 *	    Step 6: D15 SRF/view + D16 catversion bump 220 → 230 + D20
 *	      cluster_regress
 *	    Step 7: D18 065 + D19 085 TAP
 *	    Step 8: ship — linkdb tag v0.12.0-stage2.5
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cssd.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER); function bodies are gated.
 *
 *	  Spec authority: pgrac:specs/spec-2.5-cssd-heartbeat-skeleton.md
 *	  (frozen v0.2 2026-05-08).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>
#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cssd.h"
#include "cluster/cluster_conf.h"	   /* cluster_conf_lookup_node (SRF row filter) */
#include "cluster/cluster_guc.h"	   /* cluster_node_id + cssd_* GUCs */
#include "cluster/cluster_ic_router.h" /* ClusterICFanoutResult (heartbeat tick read result) */
#include "cluster/cluster_inject.h"	   /* CLUSTER_INJECTION_POINT (spec-2.5 D11) */
#include "cluster/cluster_shmem.h"	   /* cluster_shmem_register_region */
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h" /* CStringGetTextDatum */

extern pid_t cluster_postmaster_start_cssd(void);


/* ============================================================
 * StaticAssertDecl byte-layout locks.
 *
 *   Payload 12 bytes natural-aligned (uint32 + uint64).  Outbound slot
 *   64 bytes cache-line aligned (false-sharing defense).
 * ============================================================ */

StaticAssertDecl(sizeof(ClusterCssdHeartbeatPayload) == 12,
				 "ClusterCssdHeartbeatPayload frozen at 12 bytes");

StaticAssertDecl(sizeof(ClusterCssdOutboundSlot) == 64,
				 "ClusterCssdOutboundSlot 64-byte cache-line aligned");


/* ============================================================
 * Shmem region (single instance; pointer set by shmem_init).
 * ============================================================ */

static ClusterCssdShmem *CssdShmem = NULL;


/* ============================================================
 * Status / state name lookup helpers.
 * ============================================================ */

const char *
cluster_cssd_status_to_string(ClusterCssdStatus s)
{
	switch (s) {
	case CLUSTER_CSSD_STARTING:
		return "starting";
	case CLUSTER_CSSD_READY:
		return "ready";
	case CLUSTER_CSSD_SHUTTING_DOWN:
		return "shutting_down";
	case CLUSTER_CSSD_DOWN:
		return "down";
	case CLUSTER_CSSD_FAILED:
		return "failed";
	}
	return "(unknown)";
}

const char *
cluster_cssd_peer_state_to_string(ClusterCssdPeerState s)
{
	switch (s) {
	case CLUSTER_CSSD_PEER_ALIVE:
		return "alive";
	case CLUSTER_CSSD_PEER_SUSPECTED:
		return "suspected";
	case CLUSTER_CSSD_PEER_DEAD:
		return "dead";
	}
	return "(unknown)";
}


/* ============================================================
 * Shmem region.
 * ============================================================ */

Size
cluster_cssd_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCssdShmem));
}

void
cluster_cssd_shmem_init(void)
{
	bool found;
	Size sz = cluster_cssd_shmem_size();

	CssdShmem = (ClusterCssdShmem *)ShmemInitStruct("pgrac cluster cssd", sz, &found);

	if (!found) {
		int peer;

		memset(CssdShmem, 0, sz);
		LWLockInitialize(&CssdShmem->lwlock, LWTRANCHE_CLUSTER_CSSD);
		CssdShmem->status = CLUSTER_CSSD_STARTING;
		CssdShmem->shutdown_requested = false;

		pg_atomic_init_u64(&CssdShmem->total_heartbeat_send_count, 0);
		pg_atomic_init_u64(&CssdShmem->total_heartbeat_recv_count, 0);

		for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
			pg_atomic_init_u32(&CssdShmem->peers[peer].state, CLUSTER_CSSD_PEER_ALIVE);
			pg_atomic_init_u64(&CssdShmem->peers[peer].last_heartbeat_recv_at_us, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].last_heartbeat_send_at_us, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].heartbeat_send_count, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].heartbeat_recv_count, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].suspected_transitions, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].dead_transitions, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].suspected_since_us, 0);
			pg_atomic_init_u64(&CssdShmem->peers[peer].dead_since_us, 0);

			pg_atomic_init_u32(&CssdShmem->outbound[peer].pending, 0);
			pg_atomic_init_u32(&CssdShmem->outbound[peer].result_state, 0);
		}
	}
}

static const ClusterShmemRegion cluster_cssd_region = {
	.name = "pgrac cluster cssd",
	.size_fn = cluster_cssd_shmem_size,
	.init_fn = cluster_cssd_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_cssd",
	.reserved_flags = 0,
};

void
cluster_cssd_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cssd_region);
}


/* ============================================================
 * Lifecycle accessors (LW_SHARED).
 *
 *   Spec-1.11.1 F11 7-key + 2 cssd-specific (total send + recv counts;
 *   alive peer count is a derived value).
 * ============================================================ */

pid_t
cluster_cssd_get_pid(void)
{
	pid_t v;

	if (CssdShmem == NULL)
		return 0;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->pid;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

TimestampTz
cluster_cssd_get_spawned_at(void)
{
	TimestampTz v;

	if (CssdShmem == NULL)
		return 0;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->spawned_at;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

TimestampTz
cluster_cssd_get_ready_at(void)
{
	TimestampTz v;

	if (CssdShmem == NULL)
		return 0;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->ready_at;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

TimestampTz
cluster_cssd_get_last_liveness_tick_at(void)
{
	TimestampTz v;

	if (CssdShmem == NULL)
		return 0;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->last_liveness_tick_at;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

uint64
cluster_cssd_get_main_loop_iters(void)
{
	uint64 v;

	if (CssdShmem == NULL)
		return 0;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = (uint64)CssdShmem->main_loop_iters;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

ClusterCssdStatus
cluster_cssd_get_status(void)
{
	ClusterCssdStatus v;

	if (CssdShmem == NULL)
		return CLUSTER_CSSD_STARTING;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->status;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

uint64
cluster_cssd_get_total_heartbeat_send_count(void)
{
	if (CssdShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&CssdShmem->total_heartbeat_send_count);
}

uint64
cluster_cssd_get_total_heartbeat_recv_count(void)
{
	if (CssdShmem == NULL)
		return 0;
	return pg_atomic_read_u64(&CssdShmem->total_heartbeat_recv_count);
}

int
cluster_cssd_get_alive_peer_count(void)
{
	int alive = 0;
	int peer;

	if (CssdShmem == NULL)
		return 0;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if (peer == cluster_node_id)
			continue;
		if (pg_atomic_read_u32(&CssdShmem->peers[peer].state) == CLUSTER_CSSD_PEER_ALIVE)
			alive++;
	}
	return alive;
}

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	if (CssdShmem == NULL)
		return CLUSTER_CSSD_PEER_ALIVE;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return CLUSTER_CSSD_PEER_ALIVE;
	return (ClusterCssdPeerState)pg_atomic_read_u32(&CssdShmem->peers[peer_id].state);
}

TimestampTz
cluster_cssd_get_peer_last_recv_at(int32 peer_id)
{
	if (CssdShmem == NULL)
		return 0;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return 0;
	return (TimestampTz)pg_atomic_read_u64(&CssdShmem->peers[peer_id].last_heartbeat_recv_at_us);
}

ClusterCssdOutboundSlot *
cluster_cssd_outbound_slots(void)
{
	if (CssdShmem == NULL)
		return NULL;
	return &CssdShmem->outbound[0];
}


/* ============================================================
 * Per-peer counter bumpers.
 *
 *   Step 2 stubs;Step 5 wires these to cluster_pgstat per-peer counter
 *   array (D14: 4 NEW counter fields cssd_*).  Until then these
 *   forward to the per-peer atomic counter on CssdShmem itself so
 *   Sprint A standalone-test can verify counter increments work.
 * ============================================================ */

void
cluster_cssd_bump_send_count(int32 peer_id)
{
	if (CssdShmem == NULL)
		return;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_add_fetch_u64(&CssdShmem->peers[peer_id].heartbeat_send_count, 1);
	pg_atomic_add_fetch_u64(&CssdShmem->total_heartbeat_send_count, 1);
}

void
cluster_cssd_bump_recv_count(int32 peer_id)
{
	if (CssdShmem == NULL)
		return;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_add_fetch_u64(&CssdShmem->peers[peer_id].heartbeat_recv_count, 1);
	pg_atomic_add_fetch_u64(&CssdShmem->total_heartbeat_recv_count, 1);
}

void
cluster_cssd_bump_suspected_transition(int32 peer_id)
{
	if (CssdShmem == NULL)
		return;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_add_fetch_u64(&CssdShmem->peers[peer_id].suspected_transitions, 1);
}

void
cluster_cssd_bump_dead_transition(int32 peer_id)
{
	if (CssdShmem == NULL)
		return;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;
	pg_atomic_add_fetch_u64(&CssdShmem->peers[peer_id].dead_transitions, 1);
}


/* ============================================================
 * Heartbeat dispatch handler (msg_type=11).
 *
 *   Handler 4 hard constraints (per spec-2.3 §3.5):
 *     - nonblocking (no LWLockAcquire wait)
 *     - no catalog SQL
 *     - no ereport ERROR/FATAL/PANIC
 *     - bounded work (atomic update only)
 *
 *   Per-peer state transition (ALIVE → SUSPECTED → DEAD) happens in
 *   CssdMain's deadband-scan tick, NOT here, to avoid handler-side
 *   state machine vs. main-loop concurrency.  Handler only updates
 *   atomic counters + last_recv_at;recv counts here, transition
 *   logging there.
 * ============================================================ */

void
cluster_cssd_dispatch_heartbeat(const ClusterICEnvelope *env, const void *payload)
{
	int32 sender;
	ClusterCssdHeartbeatPayload hb;

	if (env == NULL || payload == NULL)
		return;
	if (CssdShmem == NULL)
		return; /* shmem not yet ready;handler runs in LMON context — drop */

	sender = (int32)env->source_node_id;
	if (sender < 0 || sender >= CLUSTER_MAX_NODES)
		return;
	if (env->payload_length != sizeof(hb))
		return;

	memcpy(&hb, payload, sizeof(hb)); /* L34 unaligned safe */

	pg_atomic_write_u64(&CssdShmem->peers[sender].last_heartbeat_recv_at_us,
						(uint64)GetCurrentTimestamp());
	pg_atomic_add_fetch_u64(&CssdShmem->peers[sender].heartbeat_recv_count, 1);
	pg_atomic_add_fetch_u64(&CssdShmem->total_heartbeat_recv_count, 1);

	(void)hb; /* hb fields are diagnostic-only;deadband scan uses
				* receiver-local clock, not sender_local_clock (clock skew
				* safe). */
}


/* ============================================================
 * Status publish + tick advance helpers (Step 4).
 *
 *   Mirror spec-1.14 Cluster Stats pattern (stats_publish_status +
 *   stats_advance_liveness_tick).  Single writer = CSSD process
 *   itself (LW_EXCLUSIVE).
 * ============================================================ */

static void
cssd_publish_status(ClusterCssdStatus new_status)
{
	TimestampTz now = GetCurrentTimestamp();

	if (CssdShmem == NULL)
		return;

	LWLockAcquire(&CssdShmem->lwlock, LW_EXCLUSIVE);
	CssdShmem->status = new_status;

	switch (new_status) {
	case CLUSTER_CSSD_STARTING:
		/* F16: SPAWNING unconditionally refreshes pid + spawned_at on
			 * every spawn (so SQL views never report stale PID after
			 * ServerLoop respawn). */
		CssdShmem->pid = MyProcPid;
		CssdShmem->spawned_at = now;
		break;
	case CLUSTER_CSSD_READY:
		CssdShmem->ready_at = now;
		break;
	case CLUSTER_CSSD_SHUTTING_DOWN:
	case CLUSTER_CSSD_DOWN:
	case CLUSTER_CSSD_FAILED:
		break;
	}
	LWLockRelease(&CssdShmem->lwlock);
}

static bool
cssd_shutdown_requested(void)
{
	bool v;

	if (CssdShmem == NULL)
		return false;
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	v = CssdShmem->shutdown_requested;
	LWLockRelease(&CssdShmem->lwlock);
	return v;
}

static void
cssd_advance_liveness_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (CssdShmem == NULL)
		return;
	LWLockAcquire(&CssdShmem->lwlock, LW_EXCLUSIVE);
	CssdShmem->last_liveness_tick_at = now;
	CssdShmem->main_loop_iters++;
	LWLockRelease(&CssdShmem->lwlock);
}


/* ============================================================
 * Heartbeat broadcast tick (spec-2.5 §3.2).
 *
 *   Per CSSD MainLoop tick:
 *     1. Read previous tick's LMON drain result for each peer slot;
 *        update send counter / last_send_at on DONE;LOG advisory on
 *        PEER_DOWN (spec §3.2.2 first-tick nonfatal).
 *     2. CAS pending 0→1 to write new request, copy payload, CAS to 2
 *        publish to LMON drain.
 *
 *   No LWLock — single producer (CSSD process) + single consumer
 *   (LMON tick) per peer slot.
 * ============================================================ */

static uint32 cssd_request_seq = 0;
static uint32 cssd_seq = 0;

static void
cssd_heartbeat_broadcast_tick(void)
{
	ClusterCssdHeartbeatPayload hb;
	ClusterCssdOutboundSlot *slots;
	int peer;

	if (CssdShmem == NULL)
		return;

	slots = cluster_cssd_outbound_slots();
	if (slots == NULL)
		return;

	hb.cssd_seq = ++cssd_seq;
	hb.sender_local_clock = (uint64)GetCurrentTimestamp();

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		uint32 prev_pending;
		uint32 expected;

		if (peer == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(peer) == NULL)
			continue;

		/* Step 1: read previous result + update counters / last_send_at. */
		prev_pending = pg_atomic_read_u32(&slots[peer].pending);
		if (prev_pending == 0) {
			ClusterICFanoutResult prev_result
				= (ClusterICFanoutResult)pg_atomic_read_u32(&slots[peer].result_state);
			uint64 prev_result_at_us = slots[peer].result_at_us;

			switch (prev_result) {
			case CLUSTER_IC_FANOUT_DONE:
				if (prev_result_at_us > 0) {
					pg_atomic_write_u64(&CssdShmem->peers[peer].last_heartbeat_send_at_us,
										prev_result_at_us);
					pg_atomic_add_fetch_u64(&CssdShmem->peers[peer].heartbeat_send_count, 1);
					pg_atomic_add_fetch_u64(&CssdShmem->total_heartbeat_send_count, 1);
				}
				break;
			case CLUSTER_IC_FANOUT_WOULD_BLOCK:
			case CLUSTER_IC_FANOUT_HARD_ERROR:
			case CLUSTER_IC_FANOUT_PEER_DOWN:
				/* nonfatal;next tick will retry. */
				break;
			}
		} else {
			/* LMON not yet drained previous tick;skip this peer to
			 * give backpressure (CSSD self-throttle prevents outbound
			 * queue starvation per spec-2.5 R11). */
			continue;
		}

		/* Step 2: write new request via CAS 0→1→memcpy→2. */
		expected = 0;
		if (!pg_atomic_compare_exchange_u32(&slots[peer].pending, &expected, 1))
			continue; /* race with LMON or self;skip this peer */

		slots[peer].request_seq = ++cssd_request_seq;
		memcpy(&slots[peer].payload, &hb, sizeof(hb));
		pg_atomic_write_u32(&slots[peer].pending, 2); /* publish to LMON */
	}
}


/* ============================================================
 * Deadband scan (spec-2.5 §3.4).
 *
 *   Per CSSD MainLoop tick (after heartbeat broadcast):
 *     For each declared peer (skip self):
 *       Skip if now < first_tick_grace_until_us (spec §3.2.1 grace).
 *       Compute elapsed_ms = (now - last_recv_at) / 1000.
 *       suspected_threshold = max(2, factor-1) × heartbeat_interval.
 *       dead_threshold      = factor × heartbeat_interval.
 *       Transition state per elapsed_ms;ereport LOG / WARNING + bump
 *       counter on transition;hysteresis ALIVE recovery on recv.
 * ============================================================ */

static void
cssd_deadband_scan_tick(void)
{
	uint64 now_us;
	uint64 grace_until;
	int factor;
	int interval_ms;
	int suspected_factor;
	int suspected_threshold_ms;
	int dead_threshold_ms;
	int peer;

	if (CssdShmem == NULL)
		return;

	now_us = (uint64)GetCurrentTimestamp();
	LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
	grace_until = CssdShmem->first_tick_grace_until_us;
	LWLockRelease(&CssdShmem->lwlock);

	/* Grace period: skip transitions until grace_until_us elapsed. */
	if (now_us < grace_until)
		return;

	factor = cluster_cssd_dead_deadband_factor;
	interval_ms = cluster_cssd_heartbeat_interval_ms;
	suspected_factor = (factor - 1 < 2) ? 2 : factor - 1;
	suspected_threshold_ms = suspected_factor * interval_ms;
	dead_threshold_ms = factor * interval_ms;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		uint64 last_recv_us;
		uint64 elapsed_us;
		int elapsed_ms;
		ClusterCssdPeerState old_state, new_state;

		if (peer == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(peer) == NULL)
			continue;

		last_recv_us = pg_atomic_read_u64(&CssdShmem->peers[peer].last_heartbeat_recv_at_us);

		/* spec-2.5 hardening v1.0.1 F1 (L78 default-state-vs-deadband-
		 * baseline):  never-received-heartbeat peer must NOT stay ALIVE
		 * forever.  Pre-fix `if (last_recv_us == 0) continue;` broke the
		 * dead-detection contract for declared-but-never-connected peers
		 * (shmem default state ALIVE persists indefinitely).
		 *
		 * Use grace_until_us as virtual baseline when no heartbeat has
		 * been received yet:  after the grace window expires, elapsed
		 * accumulates from grace boundary → SUSPECTED → DEAD eventually
		 * fire as expected. */
		if (last_recv_us == 0)
			last_recv_us = grace_until;

		if (now_us <= last_recv_us)
			continue; /* clock skew defense */
		elapsed_us = now_us - last_recv_us;
		elapsed_ms = (int)(elapsed_us / 1000);

		old_state = (ClusterCssdPeerState)pg_atomic_read_u32(&CssdShmem->peers[peer].state);
		if (elapsed_ms > dead_threshold_ms)
			new_state = CLUSTER_CSSD_PEER_DEAD;
		else if (elapsed_ms > suspected_threshold_ms)
			new_state = CLUSTER_CSSD_PEER_SUSPECTED;
		else
			new_state = CLUSTER_CSSD_PEER_ALIVE;

		if (old_state != new_state) {
			pg_atomic_write_u32(&CssdShmem->peers[peer].state, (uint32)new_state);

			switch (new_state) {
			case CLUSTER_CSSD_PEER_SUSPECTED:
				pg_atomic_write_u64(&CssdShmem->peers[peer].suspected_since_us, now_us);
				pg_atomic_add_fetch_u64(&CssdShmem->peers[peer].suspected_transitions, 1);
				ereport(LOG, (errcode(ERRCODE_CLUSTER_CSSD_PEER_SUSPECTED),
							  errmsg("cssd: peer %d transitioned ALIVE → SUSPECTED "
									 "(elapsed %d ms > %d ms)",
									 peer, elapsed_ms, suspected_threshold_ms)));
				break;
			case CLUSTER_CSSD_PEER_DEAD:
				pg_atomic_write_u64(&CssdShmem->peers[peer].dead_since_us, now_us);
				pg_atomic_add_fetch_u64(&CssdShmem->peers[peer].dead_transitions, 1);
				ereport(WARNING, (errcode(ERRCODE_CLUSTER_CSSD_PEER_DEAD),
								  errmsg("cssd: peer %d transitioned %s → DEAD "
										 "(elapsed %d ms > %d ms);NO reconfig in spec-2.5",
										 peer, cluster_cssd_peer_state_to_string(old_state),
										 elapsed_ms, dead_threshold_ms)));
				break;
			case CLUSTER_CSSD_PEER_ALIVE:
				/* Hysteresis recovery (peer was SUSPECTED/DEAD;recv brought
					 * elapsed below threshold).  Clear since-timestamps. */
				pg_atomic_write_u64(&CssdShmem->peers[peer].suspected_since_us, 0);
				pg_atomic_write_u64(&CssdShmem->peers[peer].dead_since_us, 0);
				ereport(LOG, (errmsg("cssd: peer %d recovered %s → ALIVE (heartbeat received)",
									 peer, cluster_cssd_peer_state_to_string(old_state))));
				break;
			}
		}
	}
}


/* ============================================================
 * CssdMain.
 *
 *   Step 4 — real lifecycle.  Heartbeat-tick body (write to outbound
 *   queue + deadband scan) lands in Step 5 once GUC + wait event +
 *   inject points are registered.  Step 4 minimal: proper spawn /
 *   READY publish / WaitLatch loop / shutdown protocol so postmaster
 *   reaper sees clean exit codes.
 *
 *   Asserts IsUnderPostmaster (HC1 reverse defense).
 * ============================================================ */

void
CssdMain(void)
{
	Assert(IsUnderPostmaster);

	MyBackendType = B_CSSD;
	init_ps_display(NULL);

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild. */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	if (CssdShmem == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("cluster_cssd shmem region not attached"),
				 errhint("cluster_cssd_shmem_init() must run during "
						 "CreateSharedMemoryAndSemaphores().")));

	cssd_publish_status(CLUSTER_CSSD_STARTING);

	CLUSTER_INJECTION_POINT("cluster-cssd-ready-publish");

	/* No Sprint-A startup work beyond shmem registration.  Move directly
	 * to READY. */
	cssd_publish_status(CLUSTER_CSSD_READY);

	/* spec-2.5 Q6 first-tick grace period:
	 *   grace_until_us = ready_at + (factor × heartbeat_interval × 1000)
	 * (per §3.2.1).  During this window deadband-scan does not trigger
	 * SUSPECTED/DEAD transitions, avoiding spawn-immediate-reconfig
	 * storm before peer connections finish establishing. */
	{
		uint64 ready_us = (uint64)GetCurrentTimestamp();
		uint64 grace = (uint64)cluster_cssd_dead_deadband_factor
					   * (uint64)cluster_cssd_heartbeat_interval_ms * 1000ULL;

		LWLockAcquire(&CssdShmem->lwlock, LW_EXCLUSIVE);
		CssdShmem->first_tick_grace_until_us = ready_us + grace;
		LWLockRelease(&CssdShmem->lwlock);
	}

	for (;;) {
		int rc;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (ShutdownRequestPending || cssd_shutdown_requested())
			break;

		cssd_advance_liveness_tick();

		CLUSTER_INJECTION_POINT("cluster-cssd-main-loop-pre-tick");

		/* spec-2.5 §3.2: heartbeat broadcast body — write to outbound
		 * queue;LMON tick will drain. */
		cssd_heartbeat_broadcast_tick();

		/* spec-2.5 §3.4: deadband-scan body — transition peer state. */
		cssd_deadband_scan_tick();

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_cssd_main_loop_interval_ms,
					   WAIT_EVENT_CLUSTER_BGPROC_CSSD_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	CLUSTER_INJECTION_POINT("cluster-cssd-shutdown-pre");

	cssd_publish_status(CLUSTER_CSSD_SHUTTING_DOWN);
	cssd_publish_status(CLUSTER_CSSD_DOWN);

	CLUSTER_INJECTION_POINT("cluster-cssd-shutdown-post");

	/* proc_exit(0) -> reaper sees WIFEXITED + WEXITSTATUS=0 ->
	 * normal-exit path -> NO crash recovery (HC5).  Abnormal exit hits
	 * HandleChildCrash -> restart_after_crash decides cycle. */
	proc_exit(0);
}


/* ============================================================
 * Lifecycle (Q2 thin proxies — postmaster-owned wrappers in
 * postmaster.c).
 * ============================================================ */

int
cluster_cssd_start(void)
{
	pid_t pid;

	Assert(!IsUnderPostmaster);

	CLUSTER_INJECTION_POINT("cluster-cssd-pre-spawn");

	pid = cluster_postmaster_start_cssd();

	CLUSTER_INJECTION_POINT("cluster-cssd-post-spawn");

	return (int)pid;
}

bool
cluster_cssd_wait_for_ready(int timeout_ms)
{
	const int sleep_ms = 100;
	int waited = 0;

	Assert(!IsUnderPostmaster);

	if (CssdShmem == NULL)
		return false;

	while (waited < timeout_ms) {
		ClusterCssdStatus s;

		LWLockAcquire(&CssdShmem->lwlock, LW_SHARED);
		s = CssdShmem->status;
		LWLockRelease(&CssdShmem->lwlock);

		if (s >= CLUSTER_CSSD_READY)
			return s == CLUSTER_CSSD_READY;

		pg_usleep(sleep_ms * 1000L);
		waited += sleep_ms;
	}
	return false;
}

void
cluster_cssd_request_shutdown(void)
{
	Assert(!IsUnderPostmaster);

	if (CssdShmem == NULL)
		return;
	LWLockAcquire(&CssdShmem->lwlock, LW_EXCLUSIVE);
	CssdShmem->shutdown_requested = true;
	LWLockRelease(&CssdShmem->lwlock);
}


/* ============================================================
 * cluster_get_cssd_peers SRF (spec-2.5 D15;oid 8916).
 *
 *	Returns one row per peer declared in pgrac.conf with current
 *	CSSD application-level state + heartbeat counters.  9 cols
 *	per Q7 ★ B independent view design.
 * ============================================================ */

/* PG_FUNCTION_INFO_V1(cluster_get_cssd_peers) lives in cluster_ic.c
 * to support disable-cluster build (same dual-link pattern as
 * cluster_get_ic_peers / cluster_get_ic_msg_types). */

Datum
cluster_get_cssd_peers(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (CssdShmem == NULL)
		PG_RETURN_VOID();

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterCssdPeerStateShmem *p = &CssdShmem->peers[i];
		Datum values[9];
		bool nulls[9];
		int col = 0;
		uint64 ts;

		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* peer not declared in pgrac.conf */

		memset(nulls, false, sizeof(nulls));

		values[col++] = Int32GetDatum(i);
		values[col++] = CStringGetTextDatum(
			cluster_cssd_peer_state_to_string((ClusterCssdPeerState)pg_atomic_read_u32(&p->state)));

#define ADD_TS_ATOMIC(atom_field)                                                                  \
	do {                                                                                           \
		ts = pg_atomic_read_u64(&p->atom_field);                                                   \
		if (ts == 0)                                                                               \
			nulls[col] = true;                                                                     \
		else                                                                                       \
			values[col] = TimestampTzGetDatum((TimestampTz)ts);                                    \
		col++;                                                                                     \
	} while (0)

		ADD_TS_ATOMIC(last_heartbeat_send_at_us);
		ADD_TS_ATOMIC(last_heartbeat_recv_at_us);

		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->heartbeat_send_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->heartbeat_recv_count));

		ADD_TS_ATOMIC(suspected_since_us);
		ADD_TS_ATOMIC(dead_since_us);

#undef ADD_TS_ATOMIC

		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->suspected_transitions));

		Assert(col == 9);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}


#endif /* USE_PGRAC_CLUSTER */
