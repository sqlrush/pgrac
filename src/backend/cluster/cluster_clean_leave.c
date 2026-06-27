/*-------------------------------------------------------------------------
 *
 * cluster_clean_leave.c
 *	  pgrac clean leave reconfiguration (spec-5.13) — runtime driver.
 *
 *	  Runtime half of the clean-leave module: the shmem ClusterLeaveState,
 *	  the leaving-node phase state machine (cluster_clean_leave_request +
 *	  _drive_drain), the ProcSignal quiesce path, the voting-disk leave-intent
 *	  marker two-phase commit (qvotec-mediated), the IC announce/ack/commit-ready
 *	  glue, and the LMON orchestration (cluster_clean_leave_lmon_tick: leaving-node
 *	  barrier + survivor drop/ack + coordinator two-phase commit).  The pure
 *	  decisions it feeds on live in cluster_clean_leave_policy.c.
 *
 *	  Execution model (Option A): the leaving node self-drives the drain in its
 *	  OWN backend (REQUESTED -> QUIESCE -> GES drain -> GCS flush -> PCM release ->
 *	  BARRIER_WAIT; the flush is in a backend, never LMON, CL-I9), then the LMON
 *	  collects survivor ACKs and asks the survivor coordinator (min node id) to
 *	  bump the epoch (LEAVE_COMMIT_READY).  Still pending: CL-I12 touched
 *	  drain-grace dispatch + the CL-I5 block-serve gate call-site (spec-5.13 S6),
 *	  the SRF/UDF/inject/catversion surface (S7), and the 2-node TAP e2e (S8).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_clean_leave.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.13-clean-leave-reconfig.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "storage/ipc.h" /* on_shmem_exit (qvotec latch publish) */
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"		/* PGPROC (quiesce broadcast) */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE */
#include "storage/shmem.h"
#include "storage/sinvaladt.h" /* BackendIdGetProc */
#include "utils/timestamp.h"

#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"	   /* cluster_epoch_get_current (version-coherent) */
#include "cluster/cluster_gcs_block.h" /* GCS flush-all-self orchestration (D5) */
#include "cluster/cluster_grd.h"	   /* GES cooperative drain + no-leftover verify (D4) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"	  /* CLUSTER_INJECTION_POINT (D12) */
#include "cluster/cluster_pcm_lock.h" /* PCM release-all-self + no-leftover verify (D5) */
#include "cluster/cluster_qvotec.h"	  /* cluster_qvotec_in_quorum (request gate) */
#include "cluster/cluster_reconfig.h" /* apply_clean_leave + record/is_clean_departed */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_voting_disk_io.h" /* leave-slot raw I/O + CLUSTER_VOTING_SLOT_BYTES */

/*
 * The shmem state must stay small enough to embed cheaply (§2.1).  The §2.5
 * leave-marker submit mailbox (an embedded ClusterLeaveIntentMarker + handshake
 * atomics) pushes it past the original 256-byte budget; 512 is the new bound.
 */
StaticAssertDecl(sizeof(ClusterLeaveState) <= 512, "ClusterLeaveState exceeds 512-byte budget");

/*
 * ProcSignal pending flag for the quiesce request (D7).  Set async-signal-safe
 * in the handler; the real work runs from ProcessInterrupts.
 */
volatile sig_atomic_t cluster_clean_leave_quiesce_pending = false;

/* shmem singleton (NULL until attached). */
static ClusterLeaveState *cl_state = NULL;


/* ============================================================
 * shmem region (D2)
 * ============================================================ */

Size
cluster_clean_leave_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLeaveState));
}

void
cluster_clean_leave_shmem_init(void)
{
	bool found;

	cl_state = (ClusterLeaveState *)ShmemInitStruct("pgrac cluster clean_leave",
													cluster_clean_leave_shmem_size(), &found);
	if (!found) {
		memset(cl_state, 0, sizeof(*cl_state));
		LWLockInitialize(&cl_state->lock, LWTRANCHE_CLUSTER_CLEAN_LEAVE);
		pg_atomic_init_u32(&cl_state->phase, CLUSTER_LEAVE_IDLE);
		cl_state->leaving_node_id = -1;
		cl_state->leave_epoch = 0;
		cl_state->barrier_deadline_us = 0;
		pg_atomic_init_u64(&cl_state->ges_drained_count, 0);
		pg_atomic_init_u64(&cl_state->gcs_flushed_count, 0);
		pg_atomic_init_u64(&cl_state->shards_remastered, 0);
		pg_atomic_init_u64(&cl_state->escalate_count, 0);
		pg_atomic_init_u32(&cl_state->nak_received, 0);
		pg_atomic_init_u32(&cl_state->survivor_acked, 0);
		pg_atomic_init_u32(&cl_state->commit_ready_received, 0);
		pg_atomic_init_u32(&cl_state->announce_sent, 0);
		pg_atomic_init_u64(&cl_state->serve_gate_fail_closed_count, 0);
		/* §2.5 leave-marker submit mailbox. */
		cl_state->qvotec_latch = NULL;
		pg_atomic_init_u64(&cl_state->marker_request_seq, 0);
		pg_atomic_init_u64(&cl_state->marker_completion_seq, 0);
		pg_atomic_init_u32(&cl_state->marker_result, CLUSTER_LEAVE_MARKER_SUBMIT_ACK);
		memset(&cl_state->pending_marker, 0, sizeof(cl_state->pending_marker));
	}
}

static const ClusterShmemRegion cluster_clean_leave_region = {
	.name = "pgrac cluster clean_leave",
	.size_fn = cluster_clean_leave_shmem_size,
	.init_fn = cluster_clean_leave_shmem_init,
	.lwlock_count = 1, /* ClusterLeaveState.lock (LWTRANCHE_CLUSTER_CLEAN_LEAVE) */
	.owner_subsys = "cluster_clean_leave",
	.reserved_flags = 0,
};

void
cluster_clean_leave_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_clean_leave_region);
}


/* ============================================================
 * CL-I2 no-leftover proof + survivor readiness ACK (D4/D5/§3.2)
 * ============================================================ */

/*
 * cluster_clean_leave_verify_no_leftover -- CL-I2 acceptance gate.  After the
 * leaving node drained + the survivors dropped their refs + the leave epoch
 * committed, NO GRD holder/waiter/convert/master and NO PCM X/S/PI record may
 * still name the leaving node.  A leftover would be a cross-node double-grant
 * hazard (rule 8.A).  Read-only; safe to call from any node as an assertion /
 * acceptance helper.
 */
bool
cluster_clean_leave_verify_no_leftover(int32 leaving_node_id)
{
	if (!cluster_grd_clean_leave_verify_no_leftover(leaving_node_id))
		return false;
	if (!cluster_pcm_lock_clean_leave_verify_no_leftover(leaving_node_id))
		return false;
	return true;
}

/*
 * cluster_clean_leave_block_serve_gate_allows -- CL-I5 storage-fallback gate (S6).
 *
 *	Called from the GCS block-serve path (cluster_gcs_send_block_request_and_wait)
 *	right before it serves a block from the storage fallback.  The hazard: a
 *	leaving node remasters its shards to survivors (GES_DRAINING) BEFORE it flushes
 *	its dirty/X blocks to shared storage (GCS_FLUSHING).  In that window a survivor
 *	request finds the new master holding no copy → storage fallback → reads the
 *	PRE-flush stale image (false-visible, 8.A).  This gate withholds the fallback
 *	until the leave commits (the leaving node is clean_departed → it flushed, and
 *	this survivor invalidated its cache at the epoch advance — see cl_survivor_tick).
 *
 *	Coarse v1 (CL-I4 conservative): while ANY uncommitted leave is in progress on
 *	this node, withhold ALL storage-fallbacks (over-approximate "leaving-node
 *	block"); per-block precision is a forward perf optimization.  The common
 *	no-leave path is a single unlocked read of leaving_node_id (cheap).  The
 *	leaving_node_id read is unlocked: the announce sets it BEFORE the leaving node
 *	remasters, so by the time a leaving-block storage-fallback is even possible the
 *	flag is already visible (a spurious set only over-withholds = still sound).
 */
bool
cluster_clean_leave_block_serve_gate_allows(void)
{
	int32 leaving;

	if (cl_state == NULL)
		return true; /* subsystem not attached → normal serve */

	leaving = cl_state->leaving_node_id;
	if (leaving < 0 || leaving == cluster_node_id)
		return true; /* no survivor-side leave in progress here */

	/* an uncommitted leave: withhold; once committed (flushed + invalidated)
	 * the node is clean_departed and the fallback is allowed (reads current). */
	if (cluster_clean_leave_serve_gate_allows(/*block_from_leaving*/ true,
											  cluster_reconfig_is_clean_departed(leaving)))
		return true;

	pg_atomic_fetch_add_u64(&cl_state->serve_gate_fail_closed_count, 1);
	return false;
}

/*
 * cluster_clean_leave_node_refuses_writes -- §3.1 "refuse new writes" gate.
 *
 *	Returns true when THIS node has an active clean leave in progress
 *	(REQUESTED..COMMITTED): from the moment it commits to leaving until it
 *	departs, it must accept NO new writable transaction.  The one-shot quiesce
 *	PROCSIG only aborts the backends that existed when it fired; a writable
 *	transaction that starts AFTER the GCS flush would dirty a block the leave
 *	already snapshotted-and-flushed, which the survivor could then read stale
 *	from storage (false-visible) or which would be lost if the node exits before
 *	a checkpoint (lost commit) — both 8.A.  Callers fail-close such writes with
 *	53R62 at xid assignment (AssignTransactionId) and again at the commit
 *	boundary (CommitTransaction), so nothing the leave did not flush can ever
 *	become durable.  Cheap on the hot path: the common non-leaving case is one
 *	unlocked int read.
 */
bool
cluster_clean_leave_node_refuses_writes(void)
{
	ClusterLeavePhase phase;

	if (cl_state == NULL || cl_state->leaving_node_id != cluster_node_id)
		return false;
	phase = (ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase);
	return phase >= CLUSTER_LEAVE_REQUESTED && phase <= CLUSTER_LEAVE_COMMITTED;
}

/*
 * cluster_clean_leave_survivor_ack -- a survivor signals readiness (§3.2 step 3,
 * F1 PRE-epoch).  Sent to the leaving node AFTER this survivor has dropped all
 * refs to it (the LMON tick survivor branch runs the drop, then calls this).
 * Carries no cache-invalidate and does not wait for the epoch — the epoch bump
 * happens only after every survivor acks, and the POST-epoch invalidate is
 * automatic (on_epoch_advance), so ACK and epoch do not form a cycle (R13/F1).
 */
void
cluster_clean_leave_survivor_ack(int32 leaving_node_id, uint64 leave_epoch)
{
	if (!cluster_enabled)
		return;
	cluster_clean_leave_ic_send_ack(leaving_node_id, leaving_node_id, leave_epoch, /*nak*/ false,
									(uint8)CLUSTER_LEAVE_NAK_NONE);
}


/* ============================================================
 * observability — always 1 row (D13 backing)
 * ============================================================ */

void
cluster_clean_leave_get_state(ClusterLeaveState *out)
{
	if (cl_state == NULL) {
		memset(out, 0, sizeof(*out));
		out->leaving_node_id = -1;
		return;
	}
	LWLockAcquire(&cl_state->lock, LW_SHARED);
	*out = *cl_state;
	LWLockRelease(&cl_state->lock);
}


/* ============================================================
 * ProcSignal quiesce (D7) — three-step handler + ProcessInterrupts gate
 * ============================================================ */

/*
 * cluster_clean_leave_handle_quiesce_interrupt -- ProcSignal handler.
 *
 *	Async-signal-safe three-step (CL-I8 / L100/L118): set the pending flag,
 *	raise InterruptPending so ProcessInterrupts runs, and wake the latch.  The
 *	real decision (abort writable / absorb read-only) runs in
 *	cluster_clean_leave_check_pending_in_proc_interrupts() from normal backend
 *	context.  Missing InterruptPending would make the quiesce dead code → the
 *	leaving node could commit a write across the leave boundary.
 */
void
cluster_clean_leave_handle_quiesce_interrupt(void)
{
	cluster_clean_leave_quiesce_pending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

/*
 * cluster_clean_leave_check_pending_in_proc_interrupts -- run from
 * ProcessInterrupts (normal backend context).  §2.2 / CL-I6 (writable-only):
 * abort only a writable transaction (one with a real top-level xid) with
 * 53R62; read-only / idle / post-commit absorb the quiesce silently.
 */
void
cluster_clean_leave_check_pending_in_proc_interrupts(void)
{
	if (!cluster_enabled)
		return; /* L20 first-line gate */
	if (cluster_clean_leave_quiesce_pending == 0)
		return;									 /* hot-path early return */
	cluster_clean_leave_quiesce_pending = false; /* read-clear FIRST */

	/*
	 * PG ProcessInterrupts already returned early when CritSectionCount > 0,
	 * so this is unreachable inside a critical section; commit-durable safety
	 * is naturally covered by the !IsTransactionState() / no-top-xid checks.
	 */
	if (!IsTransactionState())
		return; /* idle / post-commit absorb (CL-I6) */

	/* writable-only: read-only SELECT is in a transaction but has no top xid */
	if (!cluster_clean_leave_should_abort_writable(
			true, TransactionIdIsValid(GetTopTransactionIdIfAny())))
		return; /* read-only absorb */

	ereport(ERROR, (errcode(ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS),
					errmsg("transaction aborted: this node is leaving the cluster"),
					errhint("this node initiated a clean leave; in-flight writes were "
							"rolled back before departure; reconnect to a surviving node "
							"and retry — retry is safe")));
}


/* ============================================================
 * IC wire (D8) — CLEAN_LEAVE_ANNOUNCE / LEAVE_DRAIN_ACK / LEAVE_DRAIN_NAK
 *
 *	Sends are LMON-owned (single IC-owner invariant): the producer mask is
 *	CLUSTER_IC_PRODUCER_LMON, mirroring GES.  The announce handler runs in the
 *	survivor's LMON recv context, so it may reply (ACK/NAK) inline.  The
 *	leaving node's announce broadcast + the survivor's readiness ACK are driven
 *	from cluster_clean_leave_lmon_tick (D6); the helpers here are the wire.
 * ============================================================ */

/*
 * cl_announce_handler -- survivor side: consume a CLEAN_LEAVE_ANNOUNCE.
 *
 *	Membership-layer consume — NOT gated by clean_leave_enabled (§3.4): even a
 *	disabled survivor MUST reply LEAVE_DRAIN_NAK rather than go silent, else the
 *	leaving node waits for an ACK that never comes and false-times-out.
 */
static void
cl_announce_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;
	int32 leaving;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p)) {
		ereport(DEBUG1,
				(errmsg_internal("cluster clean-leave: dropping invalid ANNOUNCE from node %d",
								 env->source_node_id)));
		return;
	}
	leaving = p->leaving_node_id;

	/* Disabled survivor: fail-closed reply NAK(disabled), never silent. */
	if (!cluster_clean_leave_enabled) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch, true,
										(uint8)CLUSTER_LEAVE_NAK_DISABLED);
		return;
	}

	/* preflight probe: we are enabled → ACK (enablement only, no state change). */
	if (p->preflight) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch, false,
										(uint8)CLUSTER_LEAVE_NAK_NONE);
		return;
	}

	/*
	 * Real announce: enter leave-aware reconfig (record the leaving node +
	 * bound epoch; CL-I4 fail-closed-until-drained applies from here).  The
	 * readiness ACK is sent later from the LMON tick (D6) AFTER dropping refs —
	 * NOT here (that would be a premature ready-to-commit).  Reset the survivor /
	 * coordinator latches for this fresh leave.
	 */
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = leaving;
	cl_state->leave_epoch = p->leave_epoch;
	pg_atomic_write_u32(&cl_state->survivor_acked, 0);
	pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
	LWLockRelease(&cl_state->lock);
}

/*
 * cl_commit_ready_handler -- coordinator side: the leaving node has drained and
 * every survivor acked, and asks us (the min-survivor coordinator, Q6-A) to bump
 * the leave epoch.  Kept light: just latch commit_ready_received; the heavy
 * two-phase commit runs in the lmon_tick coordinator branch.  Idempotent — a
 * re-sent LEAVE_COMMIT_READY after we already committed is a no-op there (the
 * node is clean_departed).
 */
static void
cl_commit_ready_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p))
		return;
	/* must be about the leave we are tracking as a survivor. */
	if (cl_state->leaving_node_id != p->leaving_node_id)
		return;

	pg_atomic_write_u32(&cl_state->commit_ready_received, 1);
}

/*
 * cl_ack_handler -- leaving node side: consume a LEAVE_DRAIN_ACK / _NAK.
 *	env->msg_type is the authoritative ACK-vs-NAK routing (envelope CRC-
 *	protected).  Only acted on if the message is about OUR leave.
 */
static void
cl_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAckPayload *p = (const ClusterLeaveAckPayload *)payload;
	bool is_nak;

	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_ack_payload_valid(p))
		return;
	if (p->leaving_node_id != cluster_node_id)
		return; /* not about our leave */

	is_nak = (env->msg_type == PGRAC_IC_MSG_LEAVE_DRAIN_NAK);

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (is_nak) {
		/* any NAK → clean ABORTED (driven by the driver/LMON tick). */
		pg_atomic_write_u32(&cl_state->nak_received, 1);
	} else if (p->survivor_node_id >= 0
			   && p->survivor_node_id < CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8) {
		cl_state->ack_bitmap[p->survivor_node_id / 8] |= (uint8)(1u << (p->survivor_node_id % 8));
	}
	LWLockRelease(&cl_state->lock);
}

void
cluster_clean_leave_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo announce_info = {
		.msg_type = PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE,
		.name = "clean_leave_announce",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = true, /* fanned out to all survivors */
		.handler = cl_announce_handler,
	};
	const ClusterICMsgTypeInfo ack_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_DRAIN_ACK,
		.name = "leave_drain_ack",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point back to the leaving node */
		.handler = cl_ack_handler,
	};
	const ClusterICMsgTypeInfo nak_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_DRAIN_NAK,
		.name = "leave_drain_nak",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = cl_ack_handler, /* shared; branches on env->msg_type */
	};
	const ClusterICMsgTypeInfo commit_ready_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_COMMIT_READY,
		.name = "leave_commit_ready",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point to the coordinator */
		.handler = cl_commit_ready_handler,
	};

	cluster_ic_register_msg_type(&announce_info);
	cluster_ic_register_msg_type(&ack_info);
	cluster_ic_register_msg_type(&nak_info);
	cluster_ic_register_msg_type(&commit_ready_info);
}

void
cluster_clean_leave_ic_broadcast_announce(uint64 leave_epoch, bool preflight)
{
	ClusterLeaveAnnouncePayload p;
	ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = preflight ? 1 : 0;
	p.leave_epoch = leave_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	cluster_clean_leave_announce_compute_crc(&p);

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, &p, (uint32)sizeof(p),
									per_peer);
}

void
cluster_clean_leave_ic_send_ack(int32 dest_node_id, int32 leaving_node_id, uint64 leave_epoch,
								bool nak, uint8 nak_reason)
{
	ClusterLeaveAckPayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.leaving_node_id = leaving_node_id;
	p.leave_epoch = leave_epoch;
	p.nak = nak ? 1 : 0;
	p.nak_reason = nak ? nak_reason : (uint8)CLUSTER_LEAVE_NAK_NONE;
	cluster_clean_leave_ack_compute_crc(&p);

	(void)cluster_ic_send_envelope(nak ? PGRAC_IC_MSG_LEAVE_DRAIN_NAK
									   : PGRAC_IC_MSG_LEAVE_DRAIN_ACK,
								   dest_node_id, &p, (uint32)sizeof(p));
}


/* ============================================================
 * Voting-disk leave-marker two-phase commit (§2.5) — qvotec-mediated.
 *
 *	The §2.5 marker is durable on the voting disk, but the voting disk is
 *	written ONLY by qvotec (the sole-writer invariant that protects the torn-
 *	write / generation / CRC protocol).  So the marker rides the same submit ->
 *	write-to-majority -> ack handshake as the spec-4.12 fence marker: the driver
 *	(REQUESTED) / the coordinator's LMON (COMMITTING/COMMITTED) stage a marker in
 *	this node's ClusterLeaveState, wake qvotec, and block (bounded) until qvotec
 *	has written it to THIS node's own leave-slot on a quorum-majority of disks.
 *	A marker reaches majority because qvotec replicates that one slot to every
 *	disk; cross-node visibility (the startup rebuild) reads every node's leave-
 *	slot across a majority of disks.
 * ============================================================ */

/* qvotec-side per-process handshake cursors (mirror the fence-marker ones). */
static uint64 cl_qvotec_last_processed_marker_seq = 0;
static uint64 cl_qvotec_inflight_marker_seq = 0;

ClusterLeaveMarkerSubmitResult
cluster_clean_leave_submit_marker(const ClusterLeaveIntentMarker *m)
{
	uint64 seq;
	Latch *qlatch;
	uint64 deadline_us;
	int wait_ms;

	if (cl_state == NULL || m == NULL)
		return CLUSTER_LEAVE_MARKER_SUBMIT_FAILED;

	/* Stage the marker, then publish the request (write barrier between so
	 * qvotec never reads a half-written marker). */
	cl_state->pending_marker = *m;
	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(&cl_state->marker_request_seq, 1);

	/* latch-wake; a NULL latch (qvotec not running) → we time out below =
	 * fail-closed (the driver escalates rather than assuming durable). */
	qlatch = cl_state->qvotec_latch;
	if (qlatch != NULL)
		SetLatch(qlatch);

	/*
	 * Bounded synchronous wait for qvotec to complete THIS exact request.  A few
	 * poll cycles is enough for qvotec to pick it up + write + fdatasync; on
	 * timeout the driver fails closed (no false "durable").
	 */
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	deadline_us = (uint64)GetCurrentTimestamp() + (uint64)wait_ms * 1000ULL;
	for (;;) {
		if (pg_atomic_read_u64(&cl_state->marker_completion_seq) == seq) {
			pg_read_barrier();
			return (ClusterLeaveMarkerSubmitResult)pg_atomic_read_u32(&cl_state->marker_result);
		}
		if ((uint64)GetCurrentTimestamp() >= deadline_us)
			return CLUSTER_LEAVE_MARKER_SUBMIT_TIMEOUT;
		pg_usleep(2 * 1000); /* 2 ms */
	}
}

bool
cluster_clean_leave_qvotec_poll_pending(void *out_slot512)
{
	uint64 req;

	if (cl_state == NULL || out_slot512 == NULL)
		return false;

	req = pg_atomic_read_u64(&cl_state->marker_request_seq);
	if (req == cl_qvotec_last_processed_marker_seq)
		return false; /* nothing new */

	pg_read_barrier();
	/* Pack the staged marker into a 512-byte slot buffer (rest zeroed). */
	memset(out_slot512, 0, CLUSTER_VOTING_SLOT_BYTES);
	memcpy(out_slot512, &cl_state->pending_marker, sizeof(cl_state->pending_marker));
	cl_qvotec_inflight_marker_seq = req;
	return true;
}

void
cluster_clean_leave_qvotec_complete(bool acked)
{
	if (cl_state == NULL)
		return;

	pg_atomic_write_u32(&cl_state->marker_result, acked ? CLUSTER_LEAVE_MARKER_SUBMIT_ACK
														: CLUSTER_LEAVE_MARKER_SUBMIT_FAILED);
	pg_write_barrier();
	pg_atomic_write_u64(&cl_state->marker_completion_seq, cl_qvotec_inflight_marker_seq);
	cl_qvotec_last_processed_marker_seq = cl_qvotec_inflight_marker_seq;
}

static void
cl_clear_qvotec_latch(int code, Datum arg)
{
	if (cl_state != NULL)
		cl_state->qvotec_latch = NULL;
}

void
cluster_clean_leave_publish_qvotec_latch(struct Latch *latch)
{
	if (cl_state == NULL)
		return;
	cl_state->qvotec_latch = latch;
	on_shmem_exit(cl_clear_qvotec_latch, (Datum)0);
}

/*
 * cluster_clean_leave_rebuild_from_disks -- startup recovery (P1-V0.7).
 *
 *	Scan every node's leave-slot across the voting disks.  A COMMITTED marker is
 *	written by the coordinator into ITS OWN leave-slot naming the DEPARTED node
 *	(not the slot index), so for each slot we read the marker's own
 *	leaving_node_id.  When a struct-valid COMMITTED marker for the same departed
 *	node (a declared peer) appears on a quorum-majority of disks, rebuild
 *	clean_departed + raise the epoch floor to its leave_epoch (the membership
 *	epoch is not durable, so the marker is the only proof the cluster reached it,
 *	§2.5).  A REQUESTED / COMMITTING marker is NOT a trust basis (skipped); a
 *	minority / torn / CRC-bad marker is ignored (fail-closed → that node's later
 *	CSSD DEAD escalates to fail-stop, a safe no-op since it already drained).
 */
void
cluster_clean_leave_rebuild_from_disks(const int *fds, int n_disks)
{
	int s;
	uint32 majority;

	if (cl_state == NULL || fds == NULL || n_disks <= 0)
		return;

	majority = ((uint32)n_disks / 2u) + 1u;

	for (s = 0; s < CLUSTER_MAX_NODES; s++) {
		union {
			uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
			uint64 _align;
		} slot;
		ClusterLeaveIntentMarker m;
		int32 departed = -1;
		uint64 epoch = 0;
		uint32 agree = 0;
		int d;

		for (d = 0; d < n_disks; d++) {
			if (cluster_voting_disk_read_leave_slot(fds[d], (uint32)s, slot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&m, slot.bytes, sizeof(m));

			/* COMMITTED + magic/version/CRC/dead_bitmap valid for its own
			 * leaving_node_id, which must be a declared peer (defence vs a
			 * marker naming a node not in cluster.conf). */
			if (!cluster_clean_leave_marker_is_committed_basis(&m, m.leaving_node_id))
				continue;
			if (cluster_conf_lookup_node(m.leaving_node_id) == NULL)
				continue;

			if (departed < 0) {
				departed = m.leaving_node_id;
				epoch = m.leave_epoch;
				agree = 1;
			} else if (m.leaving_node_id == departed) {
				agree++;
				if (m.leave_epoch > epoch)
					epoch = m.leave_epoch;
			}
		}

		if (departed >= 0 && agree >= majority) {
			cluster_reconfig_record_clean_departed(departed, epoch, /*raise_epoch_floor*/ true);
			ereport(LOG,
					(errmsg("cluster clean-leave: rebuilt clean-departed node %d at epoch %llu "
							"from %u/%d durable COMMITTED marker(s)",
							departed, (unsigned long long)epoch, agree, n_disks)));
		}
	}
}


/* ============================================================
 * Leaving-node driver FSM + survivor/coordinator orchestration (D2/D6).
 *
 *	Execution model (user-confirmed, Option A): the leaving node self-drives the
 *	drain (Q6-A) in its OWN backend — the pg_cluster_clean_leave_request() entry
 *	runs REQUESTED -> QUIESCE -> GES drain -> GCS flush -> PCM release ->
 *	BARRIER_WAIT synchronously, so the force-WAL-log + FlushBuffer runs in a real
 *	backend (CL-I9, never in LMON), then returns.  The leaving-node LMON tick then
 *	collects survivor ACKs and sends LEAVE_COMMIT_READY to the survivor
 *	coordinator (min node id), which owns the epoch bump (Q6-A): the coordinator's
 *	LMON runs the §3.1 two-phase commit (COMMITTING marker -> bump+publish
 *	CLEAN_LEAVE -> COMMITTED marker).  The new epoch propagates back; the leaving
 *	node observes its CLEAN_LEAVE event and reaches COMMITTED.
 *
 *	NOTE (review): the readiness handoff (LEAVE_COMMIT_READY) is the impl
 *	mechanism for the spec's under-specified commit trigger; the spec pins the
 *	decision (leaving self-drives, survivor coordinator commits, flush-before-
 *	commit for CL-I5) but not the trigger wire.  Flagged for the mandated opus
 *	review (DoD).  CL-I12 touched drain-grace dispatch + the CL-I5 block-serve
 *	gate call-site are spec-5.13 S6.
 * ============================================================ */

/* atomic phase write with a debug-time legal-transition assert. */
static void
cl_set_phase(ClusterLeavePhase to)
{
	Assert(cluster_clean_leave_phase_valid_transition(
		(ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase), to));
	pg_atomic_write_u32(&cl_state->phase, to);
}

/* min declared node that is alive (CSSD not DEAD) and != leaving; -1 if none. */
static int32
cl_compute_coordinator(int32 leaving)
{
	int32 i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* not a declared peer */
		if (i == cluster_node_id)
			return i; /* self is alive by definition */
		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_DEAD)
			return i;
	}
	return -1;
}

/* every alive declared survivor (!= leaving) has set its ack bit? */
static bool
cl_all_survivors_acked(int32 leaving)
{
	int32 i;
	bool all = true;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			continue; /* a dead survivor is not expected to ack (CL-I7 escalate handles it) */
		if (!(cl_state->ack_bitmap[i / 8] & (uint8)(1u << (i % 8)))) {
			all = false;
			break;
		}
	}
	LWLockRelease(&cl_state->lock);
	return all;
}

/* broadcast PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE to local backends (not self —
 * the leaving session is driving the leave, not a victim).  Mirrors
 * cluster_reconfig_broadcast_local_procsig. */
static void
cl_broadcast_quiesce_local(void)
{
	int beid;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue;
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_CLEAN_LEAVE_QUIESCE, (BackendId)beid);
	}
}

/* build a leave-intent marker for `leaving` at `epoch` in the given phase. */
static void
cl_build_marker(ClusterLeaveIntentMarker *m, uint8 marker_phase, int32 leaving, uint64 epoch)
{
	memset(m, 0, sizeof(*m));
	m->magic = CLUSTER_LEAVE_MARKER_MAGIC;
	m->version = CLUSTER_LEAVE_MARKER_VERSION;
	m->leaving_node_id = leaving;
	m->leave_epoch = epoch;
	if (leaving >= 0 && leaving < CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8)
		m->dead_bitmap[leaving / 8] = (uint8)(1u << (leaving % 8));
	m->cssd_dead_generation = cluster_cssd_get_dead_generation();
	m->event_id = cluster_reconfig_compute_event_id(m->dead_bitmap, m->cssd_dead_generation);
	m->written_at = GetCurrentTimestamp();
	m->phase = marker_phase;
	cluster_clean_leave_marker_compute_crc(m);
}

/* send LEAVE_COMMIT_READY (reuse the announce payload, preflight=0) to the
 * coordinator: "I have drained + every survivor acked; bump the epoch". */
static void
cl_send_commit_ready(int32 coordinator, uint64 baseline_epoch)
{
	ClusterLeaveAnnouncePayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = cluster_node_id;
	p.preflight = 0;
	p.leave_epoch = baseline_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	cluster_clean_leave_announce_compute_crc(&p);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_COMMIT_READY, coordinator, &p,
								   (uint32)sizeof(p));
}

/* clean abort (mixed-mode NAK / preflight reject): clear the marker, revert to
 * IDLE — NO escalate, NO epoch bump, nothing was drained (only reachable from
 * REQUESTED, before any drain side effect). */
static void
cl_clean_abort(void)
{
	ClusterLeaveIntentMarker zero;

	cl_set_phase(CLUSTER_LEAVE_ABORTED);
	/* best-effort clear of the durable marker (magic=0 → no marker). */
	memset(&zero, 0, sizeof(zero));
	(void)cluster_clean_leave_submit_marker(&zero);

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = -1;
	cl_state->leave_epoch = 0;
	cl_state->barrier_deadline_us = 0;
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_IDLE);
}

/* escalate (real death / deadline mid-drain): abandon the optimistic clean-leave
 * path; if the node truly dies the existing death-driven fail-stop (5.14) takes
 * over.  Records the failure and resets so the operator can retry. */
static void
cl_escalate(void)
{
	cl_set_phase(CLUSTER_LEAVE_ABORTED_ESCALATE);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-escalate-to-failstop");
	pg_atomic_fetch_add_u64(&cl_state->escalate_count, 1);
	ereport(LOG, (errmsg("cluster clean-leave: drain abandoned (version changed / deadline / NAK); "
						 "escalating to fail-stop fallback")));
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = -1;
	cl_state->leave_epoch = 0;
	cl_state->barrier_deadline_us = 0;
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_IDLE);
}

/*
 * cluster_clean_leave_request -- internal C driver entry (the operator SQL UDF
 * pg_cluster_clean_leave_request maps the result to its text return, D13b).
 * Gates on enabled + in_quorum + a live peer + no leave already in progress, then
 * records intent (durable REQUESTED marker) and drives the local drain
 * synchronously (Option A: the flush runs in THIS backend, CL-I9).
 *
 *	IC-send ownership (architectural): cluster_ic_send_envelope* is LMON-only
 *	(single-producer invariant), so this backend does NO IC send.  The
 *	CLEAN_LEAVE_ANNOUNCE is broadcast by the leaving-node LMON tick once it sees
 *	the REQUESTED state; the survivor ACK / NAK and the LEAVE_COMMIT_READY are
 *	likewise LMON-driven.  The mixed-mode preflight (a disabled survivor) is
 *	therefore caught by the announce-NAK rather than a synchronous probe — the
 *	drain waits briefly for that NAK before any destructive step (clean abort, no
 *	commit).  v1 thus returns ACCEPTED for a mixed-mode request and aborts
 *	cleanly async (no false commit); a synchronous rejected:peers_not_all_enabled
 *	would need a backend↔LMON preflight handshake (forward).
 */
ClusterLeaveRequestResult
cluster_clean_leave_request(void)
{
	uint64 baseline_epoch;
	int32 coordinator;
	ClusterLeaveIntentMarker m;

	if (cl_state == NULL || !cluster_enabled || !cluster_clean_leave_enabled)
		return CLUSTER_LEAVE_REQ_REJECTED_DISABLED;
	if (pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE)
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	if (!cluster_qvotec_in_quorum())
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
	coordinator = cl_compute_coordinator(cluster_node_id);
	if (coordinator < 0)
		return CLUSTER_LEAVE_REQ_NOOP_NO_PEER;

	CLUSTER_INJECTION_POINT("cluster-clean-leave-request");

	baseline_epoch = cluster_epoch_get_current();

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	cl_state->leaving_node_id = cluster_node_id;
	cl_state->leave_epoch = baseline_epoch;
	cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
	cl_state->barrier_deadline_us
		= (uint64)GetCurrentTimestamp() + (uint64)cluster_clean_leave_drain_timeout_ms * 1000ULL;
	memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
	pg_atomic_write_u32(&cl_state->nak_received, 0);
	pg_atomic_write_u32(&cl_state->announce_sent, 0); /* LMON broadcasts the announce */
	LWLockRelease(&cl_state->lock);
	cl_set_phase(CLUSTER_LEAVE_REQUESTED);

	/* REQUESTED marker must be majority-durable before we start the drain (no
	 * durable evidence → do not start; a mid-leave crash must be identifiable).
	 * The LMON broadcasts the announce concurrently while this marker write
	 * blocks, so a disabled survivor's NAK is already pending by the time
	 * drive_drain checks it. */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_REQUESTED, cluster_node_id, baseline_epoch);
	if (cluster_clean_leave_submit_marker(&m) != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
		ereport(LOG, (errmsg("cluster clean-leave: REQUESTED marker did not reach a voting-disk "
							 "majority; aborting the leave before drain")));
		cl_clean_abort();
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_DURABLE;
	}

	cluster_clean_leave_drive_drain();
	return CLUSTER_LEAVE_REQ_ACCEPTED;
}

/*
 * cluster_clean_leave_drive_drain -- the synchronous leaving-node phases, run in
 * the requesting backend (CL-I9: the GCS flush is here, never in LMON).  Advances
 * REQUESTED -> QUIESCING -> GES_DRAINING -> GCS_FLUSHING -> BARRIER_WAIT, then
 * returns; the leaving-node LMON drives BARRIER_WAIT -> COMMITTED.  Every step
 * re-checks version coherence (CL-I3: an external epoch bump = a real death
 * intruded -> escalate) and the mixed-mode NAK (clean abort).
 */
void
cluster_clean_leave_drive_drain(void)
{
	uint64 baseline_epoch;
	uint32 moved;
	int i;

	if (cl_state == NULL || !cluster_enabled)
		return;
	if (pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_REQUESTED)
		return;

	baseline_epoch = cl_state->leave_epoch;

	/* Wait briefly for an early NAK (a disabled survivor) before quiescing, so a
	 * mixed-mode leave aborts cleanly before any drain side effect (F6 layer 2).
	 * The LMON broadcasts the announce concurrently (it is LMON-only); the NAK is
	 * typically already pending by here (it arrived while the REQUESTED marker
	 * write blocked), but allow up to ~1s for the announce round-trip. */
	for (i = 0; i < 100; i++) { /* ~1s */
		if (pg_atomic_read_u32(&cl_state->nak_received))
			break;
		pg_usleep(10 * 1000);
	}
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}

	/*
	 * CL-I3 coherence gate, re-checked before every drain step.  Uses the
	 * dead_gen-aware helper, not an epoch-only compare: CSSD increments the
	 * dead_generation the moment it declares a peer dead, which is STRICTLY
	 * BEFORE the reconfig coordinator bumps the membership epoch.  Checking
	 * dead_gen too lets us escalate in that earlier window instead of draining
	 * into a death that has not yet reached the epoch.  At commit the guarded
	 * CAS in apply_clean_leave_as_coordinator is the final authority.
	 */
#define CL_COHERENT()                                                                              \
	cluster_clean_leave_version_coherent(baseline_epoch, cluster_epoch_get_current(),              \
										 cl_state->leave_baseline_dead_gen,                        \
										 cluster_cssd_get_dead_generation())

	/* REQUESTED -> QUIESCING: abort local writable backends (53R62). */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	CLUSTER_INJECTION_POINT("cluster-clean-leave-quiesce-pre");
	cl_broadcast_quiesce_local();
	cl_set_phase(CLUSTER_LEAVE_QUIESCING);

	/* QUIESCING -> GES_DRAINING: release GES grants + remaster shards. */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	moved = cluster_grd_clean_leave_drain_self(cluster_node_id, baseline_epoch);
	pg_atomic_fetch_add_u64(&cl_state->shards_remastered, moved);
	pg_atomic_fetch_add_u64(&cl_state->ges_drained_count, 1);
	cl_set_phase(CLUSTER_LEAVE_GES_DRAINING);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-ges-drained");

	/* GES_DRAINING -> GCS_FLUSHING: force-WAL-log + FlushBuffer dirty/X to shared
	 * storage (CL-I9 backend ctx) then release PCM X.  A writeback failure is
	 * fail-closed: the block keeps its PCM X, we escalate, and never claim flush. */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	PG_TRY();
	{
		uint32 flushed = cluster_gcs_block_clean_leave_flush_all_dirty();
		uint64 released;

		pg_atomic_fetch_add_u64(&cl_state->gcs_flushed_count, flushed);
		released = cluster_pcm_lock_clean_leave_release_all_self(baseline_epoch);
		(void)released;
	}
	PG_CATCH();
	{
		cl_set_phase(CLUSTER_LEAVE_ABORTED_ESCALATE);
		pg_atomic_fetch_add_u64(&cl_state->escalate_count, 1);
		ereport(LOG,
				(errmsg("cluster clean-leave: GCS flush failed; leave fail-closed (the "
						"unflushed blocks keep PCM X and follow the fail-stop recovery path)")));
		PG_RE_THROW();
	}
	PG_END_TRY();
	cl_set_phase(CLUSTER_LEAVE_GCS_FLUSHING);
	CLUSTER_INJECTION_POINT("cluster-clean-leave-gcs-flushed");

	/* GCS_FLUSHING -> BARRIER_WAIT: drain side is done; the LMON tick now collects
	 * survivor ACKs and drives the commit. */
	if (!CL_COHERENT()) {
		cl_escalate();
		return;
	}
	cl_set_phase(CLUSTER_LEAVE_BARRIER_WAIT);
#undef CL_COHERENT
}

/* leaving-node LMON: BARRIER_WAIT -> (COMMIT_READY ->) observe CLEAN_LEAVE commit
 * -> COMMITTED, or abort/escalate. */
static void
cl_leaving_barrier_tick(void)
{
	uint64 baseline_epoch = cl_state->leave_epoch;
	int32 coordinator;

	/*
	 * 1. observe the commit.  The survivor coordinator runs the actual two-phase
	 * commit and publishes the CLEAN_LEAVE event into ITS OWN reconfig state —
	 * the leaving node's last_applied never carries it.  What DOES propagate to
	 * the leaving node is the membership epoch (every IC envelope piggybacks it).
	 * So the leaving node detects its own commit by the epoch advancing past the
	 * bound baseline.  Discriminate from a real death intruding (CL-I3) by the
	 * CSSD dead_generation: a cooperative leave does NOT mark anyone CSSD-dead, so
	 * an unchanged dead_generation at the bump means OUR clean-leave committed; a
	 * changed one means a real death (escalate).
	 */
	if (cluster_epoch_get_current() > baseline_epoch) {
		if (cluster_cssd_get_dead_generation() == cl_state->leave_baseline_dead_gen) {
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			cl_state->leave_epoch = cluster_epoch_get_current(); /* the committed epoch E */
			LWLockRelease(&cl_state->lock);
			cl_set_phase(CLUSTER_LEAVE_COMMITTED);
			CLUSTER_INJECTION_POINT("cluster-clean-leave-barrier-complete");
			ereport(LOG,
					(errmsg("cluster clean-leave: committed at epoch %llu; this node has drained "
							"and may exit",
							(unsigned long long)cl_state->leave_epoch)));
		} else {
			cl_escalate(); /* a real death changed the version mid-leave (CL-I3) */
		}
		return;
	}

	/* 2. a survivor refused (mixed-mode) -> clean abort. */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}

	/* 3. fail-closed deadline. */
	if ((uint64)GetCurrentTimestamp() > cl_state->barrier_deadline_us) {
		cl_escalate();
		return;
	}

	/* 4. all survivors ready -> ask the coordinator to bump the epoch (idempotent
	 * re-send each tick until the commit is observed above). */
	if (cl_all_survivors_acked(cluster_node_id)) {
		coordinator = cl_compute_coordinator(cluster_node_id);
		if (coordinator >= 0)
			cl_send_commit_ready(coordinator, baseline_epoch);
	}
}

/* survivor coordinator: run the §3.1 two-phase commit for `leaving`. */
static void
cl_coordinator_commit(int32 leaving)
{
	uint64 baseline_epoch = cl_state->leave_epoch;
	uint64 new_epoch;
	ClusterLeaveIntentMarker m;
	int attempt;

	/* CL-I3 pre-check: refuse to commit a clean leave on a version a real death
	 * already bumped — the leaving node will then observe a non-CLEAN_LEAVE event
	 * and escalate.  This is a cheap early-out; the authoritative guard is the
	 * guarded CAS inside apply_clean_leave_as_coordinator below, which closes the
	 * check-then-bump TOCTOU at >=3 nodes. */
	if (cluster_epoch_get_current() != baseline_epoch)
		return;

	/* (1) COMMITTING(E) marker (coordinator's own slot, before the bump; NOT a
	 * trust basis).  Not durable -> do not commit. */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING, leaving, baseline_epoch + 1);
	if (cluster_clean_leave_submit_marker(&m) != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
		ereport(LOG, (errmsg("cluster clean-leave: COMMITTING marker not durable for node %d; "
							 "not committing (the leaving node will retry/escalate)",
							 leaving)));
		return;
	}

	/* (2) the real commit point: guarded epoch bump (only if still baseline) +
	 * publish CLEAN_LEAVE + record clean-departed.  Returns 0 if a concurrent
	 * reconfig moved the epoch off the baseline after the COMMITTING marker —
	 * fail-closed, do NOT write the COMMITTED marker (CL-I3). */
	new_epoch = cluster_reconfig_apply_clean_leave_as_coordinator(leaving, baseline_epoch);
	if (new_epoch == 0) {
		ereport(LOG,
				(errmsg("cluster clean-leave: epoch moved off baseline %llu before commit "
						"for node %d; not committing (the leaving node escalates to fail-stop)",
						(unsigned long long)baseline_epoch, leaving)));
		return; /* CL-I3: stale baseline (or cluster disabled / bad id) */
	}

	/* (3) COMMITTED(E) marker (after the bump; the ONLY rebuild trust basis).
	 * Post-commit it MUST become durable — retry; the leave is already committed
	 * (epoch bumped), so we never revert.  If every retry fails, LOG: the restart
	 * rebuild simply falls back to fail-stop for this node (a safe no-op since it
	 * already drained, §2.5 crash window). */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving, new_epoch);
	for (attempt = 0; attempt < 3; attempt++) {
		if (cluster_clean_leave_submit_marker(&m) == CLUSTER_LEAVE_MARKER_SUBMIT_ACK)
			break;
	}
	if (attempt == 3)
		ereport(WARNING,
				(errmsg("cluster clean-leave: COMMITTED marker for node %d did not reach a "
						"voting-disk majority after %d attempts; the leave is committed (epoch "
						"%llu) but a survivor restart will fall back to fail-stop for this node",
						leaving, attempt, (unsigned long long)new_epoch)));

	ereport(LOG, (errmsg("cluster clean-leave: committed departure of node %d at epoch %llu",
						 leaving, (unsigned long long)new_epoch)));
}

/* survivor (incl. coordinator) side of another node's leave. */
static void
cl_survivor_tick(int32 leaving)
{
	int32 coordinator;

	/*
	 * 0. CL-I7 escalate: if the leaving node died (CSSD DEAD) BEFORE its leave
	 * committed, the cooperative leave was abandoned mid-flight.  Clear this
	 * survivor's leave state so its serve-gate stops withholding (the node is not
	 * clean_departed, so effective_dead still includes it) and the existing
	 * death-driven fail-stop path takes over — clean leave never weakens fail-stop
	 * safety, and we never assume the drain completed (8.B).  A node that DID
	 * commit is clean_departed (handled in step 3); its later CSSD DEAD is the
	 * expected dormant exit, suppressed by CL-I13, and must NOT clear here.
	 */
	if (cluster_cssd_get_peer_state(leaving) == CLUSTER_CSSD_PEER_DEAD
		&& !cluster_reconfig_is_clean_departed(leaving)) {
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_state->leaving_node_id == leaving) {
			cl_state->leaving_node_id = -1;
			cl_state->leave_epoch = 0;
			pg_atomic_write_u32(&cl_state->survivor_acked, 0);
			pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		}
		LWLockRelease(&cl_state->lock);
		ereport(LOG, (errmsg("cluster clean-leave: leaving node %d died before committing; "
							 "abandoning the cooperative path, fail-stop takes over (CL-I7)",
							 leaving)));
		return;
	}

	/* 1. drop refs to the leaving node + send the PRE-epoch readiness ACK (once). */
	if (pg_atomic_read_u32(&cl_state->survivor_acked) == 0) {
		cluster_grd_cleanup_on_node_dead(leaving); /* drop GRD holders/waiters of leaving */
		cluster_clean_leave_survivor_ack(leaving, cl_state->leave_epoch);
		pg_atomic_write_u32(&cl_state->survivor_acked, 1);
	}

	/* 2. coordinator: on LEAVE_COMMIT_READY, run the two-phase commit (once —
	 * is_clean_departed gates re-entry). */
	coordinator = cl_compute_coordinator(leaving);
	if (coordinator == cluster_node_id && pg_atomic_read_u32(&cl_state->commit_ready_received)
		&& !cluster_reconfig_is_clean_departed(leaving))
		cl_coordinator_commit(leaving);

	/* 2b. EVERY survivor (not just the coordinator) must observe the CLEAN_LEAVE
	 * commit and, before recording the node clean_departed, invalidate its own
	 * cached copies of the leaving node's blocks (CL-I5 happens-before: once
	 * is_clean_departed is true the serve-gate allows the storage fallback, so the
	 * cache MUST already be invalidated here, else a stale cached read could
	 * slip through).  The coordinator did this in apply_clean_leave_as_coordinator
	 * (on_epoch_advance); a non-coordinator survivor does it here on observe. */
	if (!cluster_reconfig_is_clean_departed(leaving)) {
		ReconfigEvent ev;

		cluster_reconfig_get_last_event(&ev);
		if (ev.reconfig_kind == RECONFIG_KIND_CLEAN_LEAVE && ev.new_epoch > 0
			&& (ev.dead_bitmap[leaving / 8] & (uint8)(1u << (leaving % 8)))) {
			cluster_gcs_block_clean_leave_invalidate_for(leaving, ev.new_epoch);
			cluster_reconfig_record_clean_departed(leaving, ev.new_epoch, /*raise_floor*/ false);
		}
	}

	/* 3. once the leave is committed (clean_departed), this survivor is done with
	 * it — return its local leave state to idle (clean_departed persists in the
	 * reconfig region and suppresses the node's later CSSD DEAD, CL-I13). */
	if (cluster_reconfig_is_clean_departed(leaving)) {
		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		if (cl_state->leaving_node_id == leaving) {
			cl_state->leaving_node_id = -1;
			cl_state->leave_epoch = 0;
			pg_atomic_write_u32(&cl_state->survivor_acked, 0);
			pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		}
		LWLockRelease(&cl_state->lock);
	}
}

/*
 * cluster_clean_leave_lmon_tick -- D6 orchestration, called every LMON tick
 * (before cluster_reconfig_lmon_tick).  Branches on whether THIS node is the
 * leaver or a survivor of someone else's leave.  LMON only orchestrates (IC +
 * shmem); it never flushes (CL-I9).
 */
void
cluster_clean_leave_lmon_tick(void)
{
	int32 leaving;

	if (cl_state == NULL || !cluster_enabled)
		return;

	leaving = cl_state->leaving_node_id;
	if (leaving < 0)
		return; /* no leave in progress */

	if (leaving == cluster_node_id) {
		ClusterLeavePhase phase = (ClusterLeavePhase)pg_atomic_read_u32(&cl_state->phase);

		/*
		 * Broadcast the real CLEAN_LEAVE_ANNOUNCE once, here in LMON context
		 * (IC sends are LMON-only).  The backend set REQUESTED and is draining
		 * concurrently; the announce makes survivors drop refs + ACK so the
		 * BARRIER_WAIT below can collect them.  A disabled survivor replies NAK
		 * (caught by the drain's pre-quiesce NAK wait).
		 */
		if (phase >= CLUSTER_LEAVE_REQUESTED && phase <= CLUSTER_LEAVE_BARRIER_WAIT
			&& pg_atomic_read_u32(&cl_state->announce_sent) == 0) {
			cluster_clean_leave_ic_broadcast_announce(cl_state->leave_epoch, /*preflight*/ false);
			pg_atomic_write_u32(&cl_state->announce_sent, 1);
		}

		/* the backend drives up to BARRIER_WAIT; LMON finishes the commit. */
		if (phase == CLUSTER_LEAVE_BARRIER_WAIT)
			cl_leaving_barrier_tick();
	} else {
		cl_survivor_tick(leaving);
	}
}
