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

#include "cluster/cluster_cancel_token.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_probe_collector.h"
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

/*
 * spec-2.22 D5:LMD wait-for graph region descriptor.  Separate from
 * cluster_lmd_region (L98 ownership — daemon-state vs graph-state are
 * distinct subsystems with different LWLock contention profiles).
 */
static const ClusterShmemRegion cluster_lmd_graph_region = {
	.name = "pgrac cluster lmd graph",
	.size_fn = cluster_lmd_graph_shmem_size,
	.init_fn = cluster_lmd_graph_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-2.22 LMD graph",
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

/* ============================================================
 * spec-2.24 D2 — LMD-owned bounded cancel queue (file-static).
 *
 *	Packed into the same "pgrac cluster lmd" shmem region per user
 *	§1.3 constraint (no NEW region).  Layout:
 *	  [ ClusterLmdSharedState ][ MAXALIGN pad ][ LmdCancelQueueShmem ]
 *	`lmd_cancel_queue` pointer is set inside shmem_init pointing past
 *	the state struct.
 * ============================================================ */

typedef struct LmdCancelQueueShmem {
	slock_t lock;
	uint32 head; /* dequeue index */
	uint32 tail; /* enqueue index */
	ClusterLmdCancelItem items[CLUSTER_LMD_CANCEL_QUEUE_DEPTH];
} LmdCancelQueueShmem;

static LmdCancelQueueShmem *lmd_cancel_queue = NULL;


Size
cluster_lmd_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterLmdSharedState));
	sz = add_size(sz, MAXALIGN(sizeof(LmdCancelQueueShmem)));
	return sz;
}

Size
cluster_lmd_cancel_queue_shmem_size(void)
{
	return MAXALIGN(sizeof(LmdCancelQueueShmem));
}

void
cluster_lmd_cancel_queue_shmem_init(void)
{
	/* No-op — packed into cluster_lmd_shmem_init below. */
}

void
cluster_lmd_shmem_init(void)
{
	bool found;
	char *base;

	cluster_lmd_state = (ClusterLmdSharedState *)ShmemInitStruct("pgrac cluster lmd",
																 cluster_lmd_shmem_size(), &found);

	base = (char *)cluster_lmd_state;
	lmd_cancel_queue = (LmdCancelQueueShmem *)(base + MAXALIGN(sizeof(ClusterLmdSharedState)));

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

		/* spec-2.24 D2 — initialize cancel queue. */
		SpinLockInit(&lmd_cancel_queue->lock);
		lmd_cancel_queue->head = 0;
		lmd_cancel_queue->tail = 0;
		memset(lmd_cancel_queue->items, 0, sizeof(lmd_cancel_queue->items));
	}
}

/* ============================================================
 * spec-2.24 D2 — cancel queue producer / consumer API.
 *
 *	Producer:  cluster_ges_request_handler CANCEL_PENDING case (D1).
 *	Consumer:  LMD daemon tick body (D5).
 *
 *	Bounded ring; head==tail means empty; (tail+1) % DEPTH == head means
 *	full.  Slot count = DEPTH - 1 = 255 effective.
 * ============================================================ */

bool
cluster_lmd_cancel_queue_enqueue(uint32 source_node_id, const void *payload, uint16 payload_len)
{
	uint32 next_tail;
	bool ok = false;

	if (lmd_cancel_queue == NULL || payload == NULL
		|| payload_len > sizeof(((ClusterLmdCancelItem *)NULL)->payload))
		return false;

	SpinLockAcquire(&lmd_cancel_queue->lock);
	next_tail = (lmd_cancel_queue->tail + 1) % CLUSTER_LMD_CANCEL_QUEUE_DEPTH;
	if (next_tail != lmd_cancel_queue->head) {
		ClusterLmdCancelItem *slot = &lmd_cancel_queue->items[lmd_cancel_queue->tail];

		slot->source_node_id = source_node_id;
		slot->payload_len = payload_len;
		memcpy(slot->payload, payload, payload_len);
		lmd_cancel_queue->tail = next_tail;
		ok = true;
	}
	SpinLockRelease(&lmd_cancel_queue->lock);
	if (ok && cluster_lmd_state != NULL)
		ConditionVariableBroadcast(&cluster_lmd_state->cv);
	return ok;
}

/* ============================================================
 * spec-5.9 D5/D6 — victim-side awaiting-ack table (node-local).
 *
 *	A node that installs a cancel token for a REMOTE coordinator records the
 *	victim here; the LMD tick (cluster_lmd_victim_ack_tick) observes the
 *	backend's consume marker and sends the correlated CANCEL_ACK back so the
 *	coordinator clears its pending entry instead of waiting out the timeout
 *	(and, crucially, learns the victim was handled before it might escalate to
 *	an alternate).  Runs on EVERY node (any node can host a victim).
 * ============================================================ */
#define CLUSTER_LMD_VICTIM_ACK_MAX 64

typedef struct LmdVictimAck {
	bool active;
	uint32 procno;
	ClusterGrdHolderId victim; /* 4-tuple for the ACK payload */
	uint64 wait_seq;
	uint64 cancel_id;
	int32 coordinator_node;
	TimestampTz deadline;
} LmdVictimAck;

static LmdVictimAck lmd_victim_acks[CLUSTER_LMD_VICTIM_ACK_MAX];

static void
lmd_victim_ack_add(uint32 procno, const ClusterGrdHolderId *victim, uint64 wait_seq,
				   uint64 cancel_id, int32 coordinator_node)
{
	for (int i = 0; i < CLUSTER_LMD_VICTIM_ACK_MAX; i++) {
		if (!lmd_victim_acks[i].active) {
			lmd_victim_acks[i].active = true;
			lmd_victim_acks[i].procno = procno;
			lmd_victim_acks[i].victim = *victim;
			lmd_victim_acks[i].wait_seq = wait_seq;
			lmd_victim_acks[i].cancel_id = cancel_id;
			lmd_victim_acks[i].coordinator_node = coordinator_node;
			lmd_victim_acks[i].deadline = TimestampTzPlusMilliseconds(
				GetCurrentTimestamp(), 3 * cluster_cancel_ack_timeout_ms);
			return;
		}
	}
	/* Table full — drop; the coordinator's retransmit + finite timeout backstop. */
}

void
cluster_lmd_victim_ack_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();

	for (int i = 0; i < CLUSTER_LMD_VICTIM_ACK_MAX; i++) {
		LmdVictimAck *a = &lmd_victim_acks[i];
		ClusterCancelMarker m;
		uint64 mc = 0;
		uint64 ms = 0;
		PGPROC *target;
		uint8 status;

		if (!a->active)
			continue;
		if (a->procno >= (uint32)ProcGlobal->allProcCount) {
			a->active = false;
			continue;
		}
		target = &ProcGlobal->allProcs[a->procno];
		m = cluster_cancel_token_take_marker(target, &mc, &ms);

		if (m != CLUSTER_CANCEL_MARKER_NONE && mc == a->cancel_id) {
			switch (m) {
			case CLUSTER_CANCEL_MARKER_CONSUMED:
				status = GES_CANCEL_ACK_CONSUMED;
				break;
			case CLUSTER_CANCEL_MARKER_PROTECTED:
				status = GES_CANCEL_ACK_PROTECTED;
				break;
			default: /* STALE_CLEARED */
				status = GES_CANCEL_ACK_NOT_WAITING;
				break;
			}
			cluster_ges_send_cancel_ack(a->coordinator_node, &a->victim, a->wait_seq, a->cancel_id,
										status);
			a->active = false;
			continue;
		}

		/* No marker by the deadline — the backend never consumed (it had already
		 * moved on).  Tell the coordinator NOT_WAITING so it stops waiting. */
		if (now >= a->deadline) {
			cluster_ges_send_cancel_ack(a->coordinator_node, &a->victim, a->wait_seq, a->cancel_id,
										GES_CANCEL_ACK_NOT_WAITING);
			a->active = false;
		}
	}
}

/*
 * spec-2.24 D5 — LMD cancel queue dispatch (HC24 stale procno 防御).
 *
 *	Called from LmdMain tick body.  Drains queue (bounded budget per
 *	tick) + validates 4-tuple (node_id, procno, cluster_epoch,
 *	request_id) before signaling local victim.
 */
static void
cluster_lmd_dispatch_cancel_item(const ClusterLmdCancelItem *item)
{
	const GesRequestPayload *req = (const GesRequestPayload *)item->payload;
	uint64 victim_epoch;
	uint64 victim_request_id;
	uint64 current_epoch;
	uint32 opcode;

	/*
	 * spec-5.9 D5/D6 — the LMD cancel queue now also carries CANCEL_ACK frames
	 * (routed from the LMON GES handler so the LMD process, which owns the
	 * coordinator pending-cancel table, can react).  Branch on the opcode in the
	 * first 4 payload bytes.
	 */
	memcpy(&opcode, item->payload, sizeof(opcode));
	if (opcode == GES_REQ_OPCODE_CANCEL_ACK) {
		const GesCancelAckPayload *ack = (const GesCancelAckPayload *)item->payload;

		cluster_lmd_pending_cancel_on_ack(ack->cancel_id, (uint8)ack->ack_status);
		return;
	}

	victim_epoch
		= ((uint64)req->holder_cluster_epoch_lo) | (((uint64)req->holder_cluster_epoch_hi) << 32);
	victim_request_id
		= ((uint64)req->holder_request_id_lo) | (((uint64)req->holder_request_id_hi) << 32);

	current_epoch = cluster_epoch_get_current();

	/* HC24 — 4-tuple stale procno 防御:
	 *   target_node already verified at D1 handler;
	 *   cluster_epoch must match current (else procno may be recycled).
	 *   cluster_lmd_signal_local_victim performs final PGPROC verify.
	 */
	if (victim_epoch != current_epoch) {
		cluster_grd_inc_cleanup_skip_stale_cancel();
		return;
	}

	/* The cross-node received-cancel counter is bumped by the receive handler;
	 * here we only dispatch.  D5 revalidate inside signal_local_victim decides
	 * whether the cancel is actually delivered.  spec-5.9 D5 — the coordinator's
	 * cancel_id rides CANCEL_PENDING's unused resid[0..1]; carry it onto the
	 * installed token so the marker -> CANCEL_ACK bridge can correlate it. */
	{
		uint64 cancel_id = ((uint64)req->resid[0]) | (((uint64)req->resid[1]) << 32);
		ClusterGrdHolderId victim;

		victim.node_id = req->holder_node_id;
		victim.procno = req->holder_procno;
		victim.cluster_epoch = victim_epoch;
		victim.request_id = victim_request_id;

		if (cluster_lmd_signal_local_victim(req->holder_procno, victim_request_id, victim_epoch,
											req->wait_seq, cancel_id)) {
			/* spec-5.9 D5 — token installed; observe its consume marker and ACK
			 * the coordinator (the CANCEL_PENDING sender) when the backend
			 * resolves it (CONSUMED / PROTECTED / NOT_WAITING). */
			lmd_victim_ack_add(req->holder_procno, &victim, req->wait_seq, cancel_id,
							   (int32)item->source_node_id);
		} else {
			/* Revalidate refused: the victim is no longer waiting on this exact
			 * request — tell the coordinator NOT_WAITING immediately so it stops
			 * waiting / does not escalate. */
			cluster_ges_send_cancel_ack((int32)item->source_node_id, &victim, req->wait_seq,
										cancel_id, GES_CANCEL_ACK_NOT_WAITING);
		}
	}
}

void
cluster_lmd_drain_cancel_queue(void)
{
	ClusterLmdCancelItem item;
	int drained = 0;

	while (drained < 32 && cluster_lmd_cancel_queue_dequeue(&item)) {
		cluster_lmd_dispatch_cancel_item(&item);
		drained++;
	}
}

/*
 * spec-2.24 D8 — LMD periodic dead-backend safety net sweep.
 *
 *	HC28 enforcement:
 *	  - per-shard chunked iteration via cluster_grd_lmon_tick_cleanup_local_
 *	    sweep (D8 helper in cluster_grd);
 *	  - sweep ONLY local stale procnos (holder.node_id == cluster_node_id
 *	    && procno not in local ProcArray) per user codereview Change 4 —
 *	    remote node death is handled exclusively by cssd dead-bitmap path
 *	    (D9 cluster_grd_cleanup_on_node_dead).
 *
 *	Called from LmdMain tick body at most once per
 *	cluster.lmd_cleanup_sweep_interval_ms.
 */
static TimestampTz lmd_last_cleanup_sweep = 0;

void
cluster_lmd_run_periodic_cleanup_sweep(void)
{
	TimestampTz now;
	int swept;

	if (cluster_lmd_cleanup_sweep_interval_ms <= 0)
		return; /* disabled */

	now = GetCurrentTimestamp();
	if (lmd_last_cleanup_sweep != 0
		&& TimestampDifferenceExceeds(lmd_last_cleanup_sweep, now,
									  cluster_lmd_cleanup_sweep_interval_ms)
			   == false)
		return; /* interval not elapsed */

	lmd_last_cleanup_sweep = now;

	swept = cluster_grd_sweep_local_stale_procnos();
	if (swept > 0)
		cluster_lmd_cleanup_lmd_sweep_count_inc((uint64)swept);
}

/* ============================================================
 * spec-5.9 D2 — anti-thrash recent-victim ring (LMD-process-local).
 *
 *	The deadlock coordinator runs single-threaded inside this LMD aux process,
 *	so the ring is plain process-local state — no shmem, no lock.  On
 *	coordinator drift (HC16) the new node's LMD starts with an empty ring (RC3
 *	stateless takeover), which merely forgoes one thrash-avoidance.  Bounded
 *	clock-replacement ring keyed on the victim 4-tuple + chosen timestamp.
 * ============================================================ */
typedef struct LmdRecentVictim {
	bool valid;
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
	TimestampTz chosen_ts;
} LmdRecentVictim;

static LmdRecentVictim lmd_recent_victims[CLUSTER_LMD_RECENT_VICTIM_DEPTH];
static int lmd_recent_victim_next = 0; /* clock hand */

static inline bool
lmd_recent_victim_same(const LmdRecentVictim *r, const ClusterLmdVertex *v)
{
	return r->valid && r->node_id == v->node_id && r->procno == v->procno
		   && r->cluster_epoch == v->cluster_epoch && r->request_id == v->request_id;
}

/*
 * True iff *victim's 4-tuple was recorded as a chosen victim within
 * cluster.victim_repeat_window_ms (a livelock symptom).  Advisory: the
 * coordinator uses this only to prefer an alternate, never to skip resolving a
 * deadlock.  window <= 0 disables anti-thrash.
 */
bool
cluster_lmd_recent_victim_is_thrashing(const ClusterLmdVertex *victim)
{
	TimestampTz now;
	int window_ms = cluster_victim_repeat_window_ms;

	if (victim == NULL || window_ms <= 0)
		return false;
	now = GetCurrentTimestamp();
	for (int i = 0; i < CLUSTER_LMD_RECENT_VICTIM_DEPTH; i++) {
		const LmdRecentVictim *r = &lmd_recent_victims[i];

		if (!lmd_recent_victim_same(r, victim))
			continue;
		if (!TimestampDifferenceExceeds(r->chosen_ts, now, window_ms))
			return true; /* chosen within the window */
	}
	return false;
}

/* Note *victim as just chosen for cancel (clock-replacement insert). */
void
cluster_lmd_recent_victim_record(const ClusterLmdVertex *victim)
{
	LmdRecentVictim *slot;

	if (victim == NULL)
		return;
	slot = &lmd_recent_victims[lmd_recent_victim_next];
	slot->valid = true;
	slot->node_id = victim->node_id;
	slot->procno = victim->procno;
	slot->cluster_epoch = victim->cluster_epoch;
	slot->request_id = victim->request_id;
	slot->chosen_ts = GetCurrentTimestamp();
	lmd_recent_victim_next = (lmd_recent_victim_next + 1) % CLUSTER_LMD_RECENT_VICTIM_DEPTH;
}

void
cluster_lmd_recent_victim_ring_reset(void)
{
	memset(lmd_recent_victims, 0, sizeof(lmd_recent_victims));
	lmd_recent_victim_next = 0;
}

bool
cluster_lmd_cancel_queue_dequeue(ClusterLmdCancelItem *out)
{
	bool ok = false;

	if (lmd_cancel_queue == NULL || out == NULL)
		return false;

	SpinLockAcquire(&lmd_cancel_queue->lock);
	if (lmd_cancel_queue->head != lmd_cancel_queue->tail) {
		*out = lmd_cancel_queue->items[lmd_cancel_queue->head];
		lmd_cancel_queue->head = (lmd_cancel_queue->head + 1) % CLUSTER_LMD_CANCEL_QUEUE_DEPTH;
		ok = true;
	}
	SpinLockRelease(&lmd_cancel_queue->lock);
	return ok;
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
	/* spec-2.22 D5:also register the wait-for graph region. */
	cluster_shmem_register_region(&cluster_lmd_graph_region);
	/* spec-5.8 D8:  the cross-node REPORT collector (LMON->LMD shmem hand-off). */
	cluster_lmd_probe_collector_shmem_register();
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


/*
 * spec-5.8 D3b — HC16 coordinator election.
 *
 *	The cross-node deadlock scan is driven by exactly one node: the lowest
 *	node_id that is currently alive.  Liveness comes directly from CSSD
 *	per-peer heartbeat state — the same alive/dead set the reconfig
 *	coordinator is computed from (spec-2.29: min(alive - dead)) — so the
 *	election is reconfig-aware (a dead lower node drops out and the next
 *	lowest survivor takes over) and also works on a healthy, never-
 *	reconfigured cluster.
 *
 *	Fail-closed on membership uncertainty (user directive): if ANY peer is
 *	SUSPECTED (heartbeat in flux, neither clearly ALIVE nor DEAD) return false
 *	so NO node runs the scan this tick — better to miss a tick (the finite
 *	enqueue timeouts backstop) than to risk two coordinators or the wrong one
 *	while CSSD views disagree.  A DEAD lower peer is excluded (it cannot run
 *	the scan), so the lowest survivor becomes coordinator without double
 *	driving.
 */
static bool
lmd_is_deadlock_coordinator(void)
{
	int32 self = cluster_node_id;
	int total;
	int32 lowest_alive;

	if (self < 0)
		return false; /* not a configured cluster node */

	total = cluster_conf_node_count();
	lowest_alive = self; /* self is alive by definition — it is running this */

	for (int32 i = 0; i < total; i++) {
		ClusterCssdPeerState st;

		if (i == self)
			continue;
		st = cluster_cssd_get_peer_state(i);
		if (st == CLUSTER_CSSD_PEER_SUSPECTED)
			return false; /* membership in flux — fail-closed, skip this tick */
		if (st == CLUSTER_CSSD_PEER_ALIVE && i < lowest_alive)
			lowest_alive = i;
		/* CLUSTER_CSSD_PEER_DEAD peers are excluded from the alive set. */
	}

	return lowest_alive == self;
}

/*
 * spec-5.8 D3b — coordinator cross-node deadlock scan tick.
 *
 *	Called once per LmdMain iteration.  Gated by
 *	cluster.deadlock_detection_enabled, the HC16 election, and the
 *	cluster.global_dd_interval_ms cadence (coarser than the local scan
 *	period).  The scan itself is two-round + revalidate gated (spec-5.8 D3a),
 *	so reaching it never risks a single-round false cancel.
 */
static TimestampTz lmd_last_coord_scan = 0;

static void
cluster_lmd_run_coordinator_tick(void)
{
	TimestampTz now;

	if (!cluster_lmd_deadlock_detection_enabled)
		return;
	if (!lmd_is_deadlock_coordinator())
		return;

	/*
	 * spec-5.9 D6 — advance in-flight cancels every coordinator iteration (more
	 * responsive than the throttled scan): observe local consume markers,
	 * retransmit remote cancels on ACK timeout, degrade on exhaustion.  On a
	 * coordinator drift this node stops being coordinator (returns above) and its
	 * pending entries go inert -> the finite GES timeout backstops them.
	 */
	cluster_lmd_pending_cancel_tick();

	now = GetCurrentTimestamp();
	if (lmd_last_coord_scan != 0
		&& !TimestampDifferenceExceeds(lmd_last_coord_scan, now, cluster_lmd_global_dd_interval_ms))
		return; /* not yet time for the next coordinator scan */

	lmd_last_coord_scan = now;
	cluster_lmd_tarjan_run_coordinator_scan(0); /* 0 → cluster.lmd_probe_collect_timeout_ms */
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

		/*
		 * spec-2.22 D2:invoke Tarjan local scan.  We always run scan on
		 * wake (CV broadcast) AND on periodic timeout — the scan itself
		 * is cheap when graph is empty (early return), so no need to
		 * differentiate paths.
		 */
		if (lmd_get_state() == CLUSTER_LMD_READY) {
			cluster_lmd_tarjan_run_local_scan();
			/* spec-2.24 D5 — drain cancel queue. */
			cluster_lmd_drain_cancel_queue();
			/* spec-5.9 D5 — victim-side: observe consume markers + ACK the
			 * coordinator (runs on every node — any node can host a victim). */
			cluster_lmd_victim_ack_tick();
			/* spec-2.24 D8 — periodic safety net cleanup sweep. */
			cluster_lmd_run_periodic_cleanup_sweep();
			/* spec-5.8 D3b — coordinator cross-node deadlock scan (HC16-gated,
			 * global_dd_interval cadence, two-round + revalidate). */
			cluster_lmd_run_coordinator_tick();
		}

		if (current_submission_count > seen_submission_count) {
			pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_wake_count, 1);
			seen_submission_count = current_submission_count;
			continue;
		}

		pg_atomic_fetch_add_u64(&cluster_lmd_state->lmd_idle_count, 1);
		/* spec-2.22 D9:scan interval GUC controls wake period. */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						cluster_lmd_scan_interval_ms, WAIT_EVENT_CLUSTER_LMD_SCAN);
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
