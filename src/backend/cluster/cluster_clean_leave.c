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
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (D12) */
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_pcm_lock.h" /* PCM release-all-self + no-leftover verify (D5) */
#include "cluster/cluster_qvotec.h"	  /* cluster_qvotec_in_quorum (request gate) */
#include "cluster/cluster_reconfig.h" /* apply_clean_leave + record/is_clean_departed + join_in_progress */
#include "cluster/cluster_membership.h" /* v1.0.4 — cluster_membership_is_member (P1-2 INV-J8) */
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
		/* Hardening v1.0.1 (P2 / P1-1). */
		pg_atomic_init_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
		pg_atomic_init_u32(&cl_state->commit_point_observed, 0);
		pg_atomic_init_u32(&cl_state->committed_durable_confirmed, 0);
		pg_atomic_init_u32(&cl_state->committed_marker_durable, 0);
		/* Hardening v1.0.2 (P1 preflight / P2 nonce). */
		pg_atomic_init_u64(&cl_state->leave_attempt_nonce, 0);
		pg_atomic_init_u32(&cl_state->preflight_pending, 0);
		pg_atomic_init_u32(&cl_state->preflight_sent, 0);
		/* Hardening v1.0.3 (P1 same-node serialization + preflight-incomplete reason). */
		pg_atomic_init_u32(&cl_state->request_in_progress, 0);
		pg_atomic_init_u32(&cl_state->abort_reason, (uint32)CLUSTER_LEAVE_ABORT_NONE);
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
 * cluster_clean_leave_in_progress -- is THIS node participating in any clean
 * leave right now (as the leaver OR as a survivor tracking someone else's
 * leave, OR mid-request before it has bound its slot)?  Hardening v1.0.4
 * (P1-1/P2): the spec-5.15 online-join driver consults this to enforce "one
 * membership reconfig at a time" — it must NOT start or commit a join (which
 * bumps the epoch with dead_gen unchanged, indistinguishable from a clean-leave
 * commit, CL barrier-observe hang) while a clean leave is active anywhere this
 * node can see it.  Cheap: a few unlocked atomic reads; the authoritative race
 * resolution is the re-check the join driver does under the reconfig lock at its
 * own commit point, plumbed symmetrically with cluster_reconfig_join_in_progress.
 */
bool
cluster_clean_leave_in_progress(void)
{
	if (cl_state == NULL)
		return false;
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0)
		return true; /* mid-request: reserved before phase/leaving_node_id are set */
	if (cl_state->leaving_node_id != -1)
		return true; /* leaver or survivor tracking a leave */
	return pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE;
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
	if (!cluster_enabled || cl_state == NULL)
		return;
	/* echo the tracked attempt's nonce so the leaver binds this ACK to it (P2). */
	cluster_clean_leave_ic_send_ack(leaving_node_id, leaving_node_id, leave_epoch,
									pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
									/*nak*/ false, (uint8)CLUSTER_LEAVE_NAK_NONE);
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

/* spec-2.29a ②b: forward decls — the bind sites (below) snapshot the
 * others-dead set that the coherence gate (defined near drive_drain) compares. */
static void cl_others_dead_snapshot(int32 leaving, uint8 *out);

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

	/* Disabled survivor: fail-closed reply NAK(disabled), never silent.  Echoes
	 * the announce nonce (P2) and is reachable on BOTH the preflight probe and the
	 * real announce — so a disabled survivor is caught at layer-1 preflight before
	 * any side effect (P1). */
	if (!cluster_clean_leave_enabled) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true, (uint8)CLUSTER_LEAVE_NAK_DISABLED);
		return;
	}

	/*
	 * Hardening v1.0.4 (P1-1 + P1-2 + P2) — decided BEFORE the preflight/real split
	 * so it gates BOTH; gating only the real announce is NOT enough (the leaver would
	 * pass preflight, broadcast the real announce, and OTHER survivors would accept +
	 * track it before this node's NAK arrived, then the leaver clean-aborts, leaving
	 * those survivors tracking an aborted leave).
	 *   - Non-MEMBER (INV-J8): silently ignore.  A JOINING/ABSENT node is not a
	 *     survivor, must not ACK/track/drop — and must NOT NAK either, because the
	 *     leaver's cl_all_survivors_acked already excludes it; a NAK would wrongly
	 *     abort the leave.
	 *   - MEMBER but busy (this node is itself mid-request to leave, or a membership
	 *     join is in flight): NAK LEAVE_IN_PROGRESS at preflight so the leaver rejects
	 *     before any survivor enters tracking (one membership reconfig at a time).
	 */
	if (!cluster_membership_is_member(cluster_node_id))
		return;
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0
		|| cluster_reconfig_join_in_progress()) {
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		return;
	}

	/* preflight probe (F6 layer-1): we are enabled → ACK (enablement only, NO state
	 * change, NO GRD/PCM cleanup).  This is the side-effect-free half of the
	 * two-layer mixed-mode defense — the leaver gathers these ACKs before it ever
	 * enters REQUESTED / broadcasts the real announce. */
	if (p->preflight) {
		/*
		 * Hardening v1.0.3 (P1 test seam): a :skip arm makes this survivor SILENT
		 * on the probe — neither ACK nor NAK — modelling a version-skewed / IC-
		 * dropping / slow survivor.  The leaver must then fail-closed
		 * (rejected:preflight_incomplete), not fail-open past the deadline.
		 */
		CLUSTER_INJECTION_POINT("cluster-clean-leave-survivor-suppress-preflight-ack");
		if (cluster_injection_should_skip("cluster-clean-leave-survivor-suppress-preflight-ack"))
			return;
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, false, (uint8)CLUSTER_LEAVE_NAK_NONE);
		return;
	}

	/*
	 * Hardening v1.0.1 (P1-3, single-leave-at-a-time): the survivor state is a
	 * single slot (one leave tracked).  If we are ALREADY tracking a DIFFERENT,
	 * uncommitted leave, do NOT overwrite leaving_node_id — that would silently
	 * drop the first leave's serve-gate protection (its still-unflushed blocks
	 * could then be served stale from storage = false-visible, 8.A).  Reject the
	 * second leave with a NAK so its leaving node clean-aborts and retries later;
	 * the wire handler ENFORCES the invariant the struct comment only documented.
	 * A re-announce of the SAME leave is idempotent (fall through, keep baseline).
	 */
	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != leaving) {
		LWLockRelease(&cl_state->lock);
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		ereport(LOG,
				(errmsg("cluster clean-leave: node %d announced a leave while node %d's leave is "
						"in progress; NAK (single-leave-at-a-time)",
						leaving, cl_state->leaving_node_id)));
		return;
	}
	/*
	 * Hardening v1.0.4 (P1-1, TOCTOU re-check under the lock): the membership/busy
	 * gate before the preflight split was lock-free; if this node reserved its OWN
	 * request between that gate and here, NAK now rather than become a survivor
	 * (the requester's self-bind re-check is the symmetric guard).
	 */
	if (pg_atomic_read_u32(&cl_state->request_in_progress) != 0) {
		LWLockRelease(&cl_state->lock);
		cluster_clean_leave_ic_send_ack(env->source_node_id, leaving, p->leave_epoch,
										p->leave_nonce, true,
										(uint8)CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS);
		return;
	}

	/*
	 * Real announce: enter leave-aware reconfig (record the leaving node + bound
	 * epoch; CL-I4 fail-closed-until-drained applies from here).  The readiness
	 * ACK is sent later from the LMON tick (D6) AFTER dropping refs — NOT here.
	 * Only capture state on the FIRST announce of this leave (idempotent re-
	 * announce keeps the original baseline dead_gen).
	 */
	if (cl_state->leaving_node_id != leaving) {
		cl_state->leaving_node_id = leaving;
		cl_state->leave_epoch = p->leave_epoch;
		/* Hardening v1.0.2 (P2): bind this survivor to the announced attempt's
		 * nonce so a stale ACK/READY/COMMITTED from a prior same-epoch attempt is
		 * dropped by the control-message handlers. */
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, p->leave_nonce);
		/*
		 * Hardening v1.0.1 (P1-2, CL-I3 commit-handoff coherence): snapshot THIS
		 * survivor's CSSD dead_generation when it starts tracking the leave.  The
		 * coordinator re-checks it at the commit point so a real death intruding
		 * between BARRIER_WAIT and the epoch bump (dead_gen bumped, but the death's
		 * fail-stop epoch not yet advanced — invisible to the epoch-only guard)
		 * fails the commit closed instead of committing on a stale membership view.
		 */
		cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
		/* spec-2.29a ②b: snapshot the others-dead set (excludes the leaving node)
		 * the coherence gate compares against. */
		cl_others_dead_snapshot(leaving, cl_state->leave_baseline_others_dead);
		pg_atomic_write_u32(&cl_state->survivor_acked, 0);
		pg_atomic_write_u32(&cl_state->commit_ready_received, 0);
		pg_atomic_write_u32(&cl_state->committed_marker_durable, 0);
	}
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
	/* Hardening v1.0.2 (P2): bind to THIS attempt — drop a stale/delayed READY
	 * from a prior same-epoch attempt (nonce) or a different epoch. */
	if (p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce)
		|| p->leave_epoch != cl_state->leave_epoch)
		return;

	pg_atomic_write_u32(&cl_state->commit_ready_received, 1);
}

/*
 * cl_committed_handler -- leaving node side (Hardening v1.0.1, P1-V0.7 exit gate):
 * the survivor coordinator has made the COMMITTED marker majority-durable and
 * signals that the durable truth-source now exists, so this leaving node may
 * proceed to COMMITTED and exit.  Only acted on if it is about OUR leave; idem-
 * potent (the coordinator re-sends each tick until we are gone).
 */
static void
cl_committed_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterLeaveAnnouncePayload *p = (const ClusterLeaveAnnouncePayload *)payload;

	(void)env;
	if (!cluster_enabled || cl_state == NULL)
		return;
	if (!cluster_clean_leave_announce_payload_valid(p))
		return;
	if (p->leaving_node_id != cluster_node_id)
		return; /* only the leaving node consumes its own COMMITTED confirmation */
	/* Hardening v1.0.2 (P2): bind to THIS attempt — a stale LEAVE_COMMITTED from a
	 * prior same-epoch attempt must not prematurely confirm the current one.  Only
	 * meaningful once we are the leaver actively awaiting confirmation. */
	if (p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce))
		return;
	if (cl_state->leaving_node_id != cluster_node_id)
		return; /* we are not currently leaving */

	pg_atomic_write_u32(&cl_state->committed_durable_confirmed, 1);
}

/*
 * cl_send_committed -- coordinator -> leaving node: "COMMITTED marker majority-
 * durable; you may exit" (Hardening v1.0.1).  Point-to-point, re-sent each tick
 * while the leaver is alive (best-effort; the leaving node's gate is idempotent).
 */
static void
cl_send_committed(int32 leaving, uint64 leave_epoch)
{
	ClusterLeaveAnnouncePayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.leaving_node_id = leaving;
	p.preflight = 0;
	p.leave_epoch = leave_epoch;
	p.cssd_dead_generation = cluster_cssd_get_dead_generation();
	p.leave_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce); /* P2: bind to attempt */
	cluster_clean_leave_announce_compute_crc(&p);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_LEAVE_COMMITTED, leaving, &p, (uint32)sizeof(p));
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
	/* Hardening v1.0.2 (P2): bind to THIS attempt's nonce — a stale ACK/NAK from a
	 * prior preflight or drain (same baseline epoch) must not fill our tally.  The
	 * leaver sets leave_attempt_nonce before both the preflight and the real
	 * announce, so both layers' replies are checked against the current value. */
	if (p->leave_nonce != pg_atomic_read_u64(&cl_state->leave_attempt_nonce))
		return;

	is_nak = (env->msg_type == PGRAC_IC_MSG_LEAVE_DRAIN_NAK);

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	if (is_nak) {
		/* any NAK → clean ABORTED (driven by the driver/LMON tick).  Record the
		 * reason (Hardening v1.0.1 P2) so the request can map a DISABLED NAK to
		 * rejected:peers_not_all_enabled (F6 preflight) rather than bare ACCEPTED. */
		pg_atomic_write_u32(&cl_state->nak_reason, (uint32)p->nak_reason);
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
	const ClusterICMsgTypeInfo committed_info = {
		.msg_type = PGRAC_IC_MSG_LEAVE_COMMITTED,
		.name = "leave_committed",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* point-to-point back to the leaving node */
		.handler = cl_committed_handler,
	};

	cluster_ic_register_msg_type(&announce_info);
	cluster_ic_register_msg_type(&ack_info);
	cluster_ic_register_msg_type(&nak_info);
	cluster_ic_register_msg_type(&commit_ready_info);
	cluster_ic_register_msg_type(&committed_info);
}

void
cluster_clean_leave_ic_broadcast_announce(uint64 leave_epoch, uint64 leave_nonce, bool preflight)
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
	p.leave_nonce = leave_nonce;
	cluster_clean_leave_announce_compute_crc(&p);

	cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE, &p, (uint32)sizeof(p),
									per_peer);
}

void
cluster_clean_leave_ic_send_ack(int32 dest_node_id, int32 leaving_node_id, uint64 leave_epoch,
								uint64 leave_nonce, bool nak, uint8 nak_reason)
{
	ClusterLeaveAckPayload p;

	memset(&p, 0, sizeof(p));
	p.magic = CLUSTER_CLEAN_LEAVE_IC_MAGIC;
	p.version = CLUSTER_CLEAN_LEAVE_IC_VERSION;
	p.survivor_node_id = cluster_node_id;
	p.leaving_node_id = leaving_node_id;
	p.leave_epoch = leave_epoch;
	p.leave_nonce = leave_nonce;
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
static ClusterMarkerAsync cl_lmon_marker_async;
static ClusterLeaveIntentMarker cl_lmon_marker;
static int cl_lmon_marker_phase = 0;
static int32 cl_lmon_marker_leaving = -1;
static uint64 cl_lmon_marker_epoch = 0;
static bool cl_lmon_marker_submitted = false;

static void cl_build_marker(ClusterLeaveIntentMarker *m, uint8 marker_phase, int32 leaving,
							uint64 epoch);

typedef enum ClusterLeaveAsyncMarkerResult {
	CL_LEAVE_ASYNC_PENDING = 0,
	CL_LEAVE_ASYNC_ACKED,
	CL_LEAVE_ASYNC_FAILED
} ClusterLeaveAsyncMarkerResult;

static ClusterMarkerAsyncKind
cl_marker_phase_kind(int phase)
{
	if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTING)
		return CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTING;
	if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTED)
		return CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTED;
	return CLUSTER_MARKER_KIND_UNKNOWN;
}

static void
cl_release_lmon_marker_stage(void)
{
	cluster_marker_async_release_stage(&cl_lmon_marker_async);
	memset(&cl_lmon_marker, 0, sizeof(cl_lmon_marker));
	cl_lmon_marker_phase = 0;
	cl_lmon_marker_leaving = -1;
	cl_lmon_marker_epoch = 0;
	cl_lmon_marker_submitted = false;
}

static bool
cl_start_lmon_marker_stage(const ClusterLeaveIntentMarker *m, int phase, int32 leaving,
						   uint64 epoch)
{
	if (cl_lmon_marker_async.has_staged_event)
		return false;
	cl_lmon_marker = *m;
	cl_lmon_marker_phase = phase;
	cl_lmon_marker_leaving = leaving;
	cl_lmon_marker_epoch = epoch;
	cl_lmon_marker_async.has_staged_event = true;
	cl_lmon_marker_submitted = false;
	return true;
}

static ClusterLeaveAsyncMarkerResult
cl_poll_lmon_marker_stage(void)
{
	TimestampTz now;
	uint32 result = CLUSTER_LEAVE_MARKER_SUBMIT_FAILED;
	uint64 elapsed_us = 0;
	ClusterMarkerPollResult pr;
	ClusterMarkerAsyncKind kind;

	if (!cl_lmon_marker_async.has_staged_event)
		return CL_LEAVE_ASYNC_FAILED;

	now = GetCurrentTimestamp();
	kind = cl_marker_phase_kind(cl_lmon_marker_phase);
	if (!cl_lmon_marker_submitted) {
		if (!cluster_clean_leave_submit_marker_async(&cl_lmon_marker_async, &cl_lmon_marker, kind,
													 cl_lmon_marker_leaving, now))
			return CL_LEAVE_ASYNC_PENDING;
		cl_lmon_marker_submitted = true;
		return CL_LEAVE_ASYNC_PENDING;
	}

	pr = cluster_clean_leave_poll_marker_async(&cl_lmon_marker_async, now, &result, &elapsed_us);
	if (pr == CLUSTER_MARKER_POLL_PENDING || pr == CLUSTER_MARKER_POLL_IDLE)
		return CL_LEAVE_ASYNC_PENDING;
	if (pr == CLUSTER_MARKER_POLL_TIMEOUT) {
		cluster_reconfig_note_marker_timeout(kind, cl_lmon_marker_leaving, elapsed_us);
		cl_release_lmon_marker_stage();
		return CL_LEAVE_ASYNC_FAILED;
	}

	cluster_reconfig_note_marker_slow_ack(kind, cl_lmon_marker_leaving, elapsed_us);
	if (result != CLUSTER_LEAVE_MARKER_SUBMIT_ACK) {
		cl_release_lmon_marker_stage();
		return CL_LEAVE_ASYNC_FAILED;
	}
	return CL_LEAVE_ASYNC_ACKED;
}

static void
cl_drive_committed_marker_stage(int32 leaving, uint64 committed_epoch)
{
	ClusterLeaveIntentMarker cm;
	ClusterLeaveAsyncMarkerResult ar;

	if (cl_lmon_marker_async.has_staged_event) {
		if (cl_lmon_marker_phase != CLUSTER_LEAVE_MARKER_PHASE_COMMITTED
			|| cl_lmon_marker_leaving != leaving || cl_lmon_marker_epoch != committed_epoch)
			return;
	} else {
		cl_build_marker(&cm, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving, committed_epoch);
		(void)cl_start_lmon_marker_stage(&cm, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving,
										 committed_epoch);
	}

	ar = cl_poll_lmon_marker_stage();
	if (ar == CL_LEAVE_ASYNC_ACKED) {
		pg_atomic_write_u32(&cl_state->committed_marker_durable, 1);
		cl_send_committed(leaving, committed_epoch);
		cl_release_lmon_marker_stage();
	} else if (ar == CL_LEAVE_ASYNC_FAILED) {
		ereport(LOG, (errmsg("cluster clean-leave: committed node %d at epoch %llu but the "
							 "COMMITTED marker is not yet majority-durable; retrying each tick "
							 "(leaving node waits)",
							 leaving, (unsigned long long)committed_epoch)));
	}
}

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
cluster_clean_leave_submit_marker_async(ClusterMarkerAsync *a, const ClusterLeaveIntentMarker *m,
										ClusterMarkerAsyncKind kind, int32 target_node,
										TimestampTz now)
{
	int wait_ms;

	if (cl_state == NULL || m == NULL || a == NULL)
		return false;
	if (cluster_marker_async_is_submitted(a))
		return true;
	if (cluster_marker_async_mailbox_busy(&cl_state->marker_request_seq,
										  &cl_state->marker_completion_seq))
		return false;

	cl_state->pending_marker = *m;
	wait_ms = cluster_quorum_poll_interval_ms * 3 + 2000;
	return cluster_marker_async_submit(a, &cl_state->marker_request_seq,
									   &cl_state->marker_completion_seq, cl_state->qvotec_latch,
									   now, (uint64)wait_ms * 1000ULL, kind, target_node);
}

ClusterMarkerPollResult
cluster_clean_leave_poll_marker_async(ClusterMarkerAsync *a, TimestampTz now, uint32 *out_result,
									  uint64 *out_elapsed_us)
{
	if (cl_state == NULL || a == NULL)
		return CLUSTER_MARKER_POLL_IDLE;
	return cluster_marker_async_poll(a, &cl_state->marker_completion_seq, &cl_state->marker_result,
									 now, out_result, out_elapsed_us);
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
	cluster_lmon_marker_complete_wakeup();
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

	if (n_disks > CLUSTER_MAX_VOTING_DISKS)
		n_disks = CLUSTER_MAX_VOTING_DISKS; /* defensive clamp (fixed-array bound, no VLA) */
	majority = ((uint32)n_disks / 2u) + 1u;

	for (s = 0; s < CLUSTER_MAX_NODES; s++) {
		ClusterLeaveIntentMarker markers[CLUSTER_MAX_VOTING_DISKS];
		bool valid[CLUSTER_MAX_VOTING_DISKS];
		int n_read = 0;
		int d, e;

		/*
		 * Read this slot from every disk, keeping each disk's COMMITTED-basis marker
		 * for a declared peer.  Hardening v1.0.4 (finding 4): the majority must be of
		 * the SAME durable proof.  The earlier code counted agreement by
		 * leaving_node_id and took max(leave_epoch); a stale ghost leave-slot is NOT
		 * zeroed on rejoin (qvotec read-only ghost mitigation), so an OLD attempt's
		 * COMMITTED marker can persist on most disks while a NEWER attempt's COMMITTED
		 * marker reached only a minority — node-id counting then synthesized a
		 * "majority" and rebuilt at the newer epoch that itself never reached majority
		 * (trusting a non-majority durable fact, 8.A-adjacent).  Keep the markers and
		 * count by full identity below instead.
		 */
		for (d = 0; d < n_disks; d++) {
			union {
				uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
				uint64 _align;
			} slot;

			valid[d] = false;
			if (cluster_voting_disk_read_leave_slot(fds[d], (uint32)s, slot.bytes)
				!= CLUSTER_VOTING_DISK_IO_OK)
				continue;
			memcpy(&markers[d], slot.bytes, sizeof(markers[d]));
			/* COMMITTED + magic/version/CRC/dead_bitmap valid for its own
			 * leaving_node_id, which must be a declared peer. */
			if (!cluster_clean_leave_marker_is_committed_basis(&markers[d],
															   markers[d].leaving_node_id))
				continue;
			if (cluster_conf_lookup_node(markers[d].leaving_node_id) == NULL)
				continue;
			valid[d] = true;
			n_read++;
		}
		if (n_read == 0)
			continue;

		/*
		 * Find a marker IDENTITY {leaving_node_id, leave_epoch, event_id,
		 * cssd_dead_generation} present on a quorum-majority of disks — THAT is the
		 * durable proof.  At most one identity can hold a majority, so the first
		 * match is unique; rebuild at its own leave_epoch (never a different
		 * attempt's higher epoch).
		 */
		for (d = 0; d < n_disks; d++) {
			uint32 agree = 0;

			if (!valid[d])
				continue;
			for (e = 0; e < n_disks; e++) {
				if (valid[e] && markers[e].leaving_node_id == markers[d].leaving_node_id
					&& markers[e].leave_epoch == markers[d].leave_epoch
					&& markers[e].event_id == markers[d].event_id
					&& markers[e].cssd_dead_generation == markers[d].cssd_dead_generation)
					agree++;
			}
			if (agree >= majority) {
				cluster_reconfig_record_clean_departed(markers[d].leaving_node_id,
													   markers[d].leave_epoch,
													   /*raise_epoch_floor*/ true);
				ereport(LOG,
						(errmsg("cluster clean-leave: rebuilt clean-departed node %d at epoch %llu "
								"from %u/%d durable COMMITTED marker(s) of one identity",
								markers[d].leaving_node_id,
								(unsigned long long)markers[d].leave_epoch, agree, n_disks)));
				break; /* one identity per slot can reach majority */
			}
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

/* min MEMBER node that is alive (CSSD not DEAD) and != leaving; -1 if none. */
static int32
cl_compute_coordinator(int32 leaving)
{
	int32 i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		/*
		 * Hardening v1.0.4 (P1-2, INV-J8): only a MEMBER may be elected clean-leave
		 * coordinator (it drives the two-phase commit + epoch bump).  A JOINING /
		 * ABSENT (but CSSD-alive) declared node must never be chosen — that would let
		 * a non-member bump the membership epoch, conflicting with safe-publication.
		 * Subsumes the old declared-peer check (a member is a declared peer).
		 */
		if (!cluster_membership_is_member(i))
			continue;
		if (i == cluster_node_id)
			return i; /* self is alive by definition */
		if (cluster_cssd_get_peer_state(i) != CLUSTER_CSSD_PEER_DEAD)
			return i;
	}
	return -1;
}

/* every alive MEMBER survivor (!= leaving) has set its ack bit? */
static bool
cl_all_survivors_acked(int32 leaving)
{
	int32 i;
	bool all = true;

	LWLockAcquire(&cl_state->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue;
		/*
		 * Hardening v1.0.4 (P1-2, INV-J8): only MEMBERs are expected to ACK a clean
		 * leave.  A JOINING / ABSENT (but CSSD-alive) node is NOT a survivor — it
		 * ignores the announce (mirror gate in the handler), so counting it here
		 * would wait forever for an ACK it must never send.  Subsumes the old
		 * declared-peer check (a member is a declared peer).
		 */
		if (!cluster_membership_is_member(i))
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
	p.leave_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce); /* P2: bind to attempt */
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
 * cl_request_body -- the reserved core of a clean-leave request.  The public
 * entry cluster_clean_leave_request holds the request_in_progress reservation
 * around this (Hardening v1.0.3); the operator SQL UDF maps the result to text,
 * D13b.  Gates on in_quorum + a live peer + no leave already in progress, runs the
 * F6 preflight (fail-CLOSED, Hardening v1.0.3), then records intent (durable
 * REQUESTED marker) and drives the local drain synchronously (Option A: the flush
 * runs in THIS backend, CL-I9).
 *
 *	IC-send ownership (architectural): cluster_ic_send_envelope* is LMON-only
 *	(single-producer invariant), so this backend does NO IC send.  The
 *	CLEAN_LEAVE_ANNOUNCE is broadcast by the leaving-node LMON tick once it sees
 *	the REQUESTED state; the survivor ACK / NAK and the LEAVE_COMMIT_READY are
 *	likewise LMON-driven.  The F6 preflight (Hardening v1.0.2) probes every survivor
 *	with a side-effect-free preflight=true announce BEFORE any state change: a
 *	disabled survivor NAKs (rejected:peers_not_all_enabled), and a silent / version-
 *	skewed survivor that never ACKs makes the request fail-CLOSED
 *	(rejected:preflight_incomplete, Hardening v1.0.3) instead of falling open into
 *	the drain.  Only an escalate (real death / deadline mid-drain) returns ACCEPTED
 *	and leaves via fail-stop.
 */
static ClusterLeaveRequestResult
cl_request_body(void)
{
	uint64 baseline_epoch;
	int32 coordinator;
	ClusterLeaveIntentMarker m;

	if (pg_atomic_read_u32(&cl_state->phase) != CLUSTER_LEAVE_IDLE)
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	/*
	 * Hardening v1.0.1 (P1-3, single-leave-at-a-time, request side): this node may
	 * already be tracking ANOTHER node's in-progress leave as a survivor (phase
	 * stays IDLE in that role, so the check above does not catch it).  Starting our
	 * own leave here would overwrite that tracking and drop the other leave's
	 * serve-gate protection (8.A).  Reject locally; the announce handler is the
	 * second enforcement point for the race where our announce races another's.
	 */
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id)
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	/*
	 * Hardening v1.0.4 (P2 serialization): one membership reconfig at a time.  If a
	 * spec-5.15 online JOIN is in its pending window, do NOT start a clean leave —
	 * the join bumps the membership epoch with dead_gen unchanged, which the leaving
	 * node would mis-observe as its own commit and wedge in BARRIER_WAIT.  The join
	 * driver defers symmetrically while a clean leave is active.
	 */
	if (cluster_reconfig_join_in_progress())
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	if (!cluster_qvotec_in_quorum())
		return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
	coordinator = cl_compute_coordinator(cluster_node_id);
	if (coordinator < 0)
		return CLUSTER_LEAVE_REQ_NOOP_NO_PEER;

	CLUSTER_INJECTION_POINT("cluster-clean-leave-request");

	/*
	 * F6 layer-1 TRUE preflight (Hardening v1.0.2, P1): BEFORE entering REQUESTED
	 * or touching any survivor's state, probe every survivor's enablement with a
	 * side-effect-free preflight=true announce.  Survivors only ACK/NAK — NO state
	 * change, NO GRD/PCM cleanup (§3.4 two-layer defense); a disabled survivor
	 * NAKs.  We proceed only when every alive survivor preflight-ACKs, so a
	 * mixed-mode request triggers NO survivor side effect.  (v1.0.1 only made the
	 * real-announce NAK synchronous, but by then an enabled survivor had already
	 * dropped its GRD/PCM refs to the still-alive leaver — 8.A at >=3 nodes.)  IC
	 * sends are LMON-only, so this is a backend<->LMON handshake: stage the probe,
	 * the LMON broadcasts it, we wait for the replies, then proceed or reject.
	 */
	{
		uint64 nonce = (uint64)GetCurrentTimestamp();
		int i;

		LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
		/*
		 * Hardening v1.0.4 (P1-1): re-check leaving_node_id UNDER the lock.  The
		 * test above was unlocked; in between, an incoming real announce could have
		 * made this node a survivor of someone else's leave (leaving_node_id :=
		 * other).  Bail before we stamp our own nonce/bitmap over that tracking.
		 */
		if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id) {
			LWLockRelease(&cl_state->lock);
			return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
		}
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, nonce);
		memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
		pg_atomic_write_u32(&cl_state->nak_received, 0);
		pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
		pg_atomic_write_u32(&cl_state->preflight_sent, 0);
		pg_atomic_write_u32(&cl_state->preflight_pending, 1);
		LWLockRelease(&cl_state->lock);

		for (i = 0; i < 500; i++) { /* up to ~5s — a few LMON ticks */
			if (pg_atomic_read_u32(&cl_state->nak_received))
				break;
			if (pg_atomic_read_u32(&cl_state->preflight_sent)
				&& cl_all_survivors_acked(cluster_node_id))
				break; /* probe out + every alive survivor is enabled */
			pg_usleep(10 * 1000);
		}
		pg_atomic_write_u32(&cl_state->preflight_pending, 0);

		if (pg_atomic_read_u32(&cl_state->nak_received)) {
			ClusterLeaveNakReason reason
				= (ClusterLeaveNakReason)pg_atomic_read_u32(&cl_state->nak_reason);

			/* fail-closed BEFORE any state / marker / survivor side effect. */
			if (reason == CLUSTER_LEAVE_NAK_NOT_IN_QUORUM)
				return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
			if (reason == CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS)
				return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
			return CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED;
		}

		/*
		 * Hardening v1.0.3 (P1, fail-CLOSED): the loop breaks early ONLY when every
		 * alive survivor preflight-ACKed.  Reaching here with no NAK but not-all-
		 * acked means the ~5s deadline expired with a survivor still silent (version
		 * skew drops the v2 frame / IC loss / slow).  v1.0.2 fell THROUGH to
		 * REQUESTED here — fail-OPEN: it then broadcast the real announce and an
		 * enabled-but-silent survivor would drop its GRD/PCM refs to a leaver whose
		 * readiness was never confirmed (8.A).  Fail closed instead: no marker, no
		 * state, no survivor side effect — the operator retries / diagnoses the
		 * silent peer.  Distinct from peers_not_all_enabled (a definite DISABLED
		 * NAK); incomplete = the handshake never finished.
		 */
		if (!cl_all_survivors_acked(cluster_node_id))
			return CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE;
	}

	baseline_epoch = cluster_epoch_get_current();

	LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
	/*
	 * Hardening v1.0.4 (P1-1, final recheck): the ~5s preflight wait above ran
	 * with the lock released, so an incoming real announce could have made this
	 * node a survivor (leaving_node_id := other) during it.  Binding self here
	 * would silently drop that other leave's serve-gate protection (false-visible,
	 * 8.A).  Re-check under the lock and bail if so; the announce handler is the
	 * symmetric enforcement point (it NAKs while our request is in progress).
	 */
	if (cl_state->leaving_node_id != -1 && cl_state->leaving_node_id != cluster_node_id) {
		LWLockRelease(&cl_state->lock);
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;
	}
	cl_state->leaving_node_id = cluster_node_id;
	cl_state->leave_epoch = baseline_epoch;
	cl_state->leave_baseline_dead_gen = cluster_cssd_get_dead_generation();
	/* spec-2.29a ②b: snapshot the others-dead set (excludes self, the leaver). */
	cl_others_dead_snapshot(cluster_node_id, cl_state->leave_baseline_others_dead);
	cl_state->barrier_deadline_us
		= (uint64)GetCurrentTimestamp() + (uint64)cluster_clean_leave_drain_timeout_ms * 1000ULL;
	/*
	 * Hardening v1.0.3 (P1, nonce pollution): the REAL announce gets a FRESH nonce,
	 * distinct from the preflight's.  The ack handler binds an ACK to the current
	 * leave_attempt_nonce only; with the preflight and real announce sharing one
	 * nonce (v1.0.2), a preflight ACK arriving AFTER the ack_bitmap memset below
	 * could re-set a survivor's bit and pollute the real readiness tally.  A fresh
	 * nonce makes any late preflight ACK/NAK fail the nonce gate and be dropped.
	 * GetCurrentTimestamp() is microseconds-monotonic and the preflight wait was
	 * seconds, so it already differs; the guard is belt-and-suspenders.
	 */
	{
		uint64 preflight_nonce = pg_atomic_read_u64(&cl_state->leave_attempt_nonce);
		uint64 real_nonce = (uint64)GetCurrentTimestamp();

		if (real_nonce == preflight_nonce)
			real_nonce++;
		pg_atomic_write_u64(&cl_state->leave_attempt_nonce, real_nonce);
	}
	memset(cl_state->ack_bitmap, 0, sizeof(cl_state->ack_bitmap));
	pg_atomic_write_u32(&cl_state->nak_received, 0);
	pg_atomic_write_u32(&cl_state->nak_reason, (uint32)CLUSTER_LEAVE_NAK_NONE);
	pg_atomic_write_u32(&cl_state->announce_sent, 0); /* LMON broadcasts the announce */
	pg_atomic_write_u32(&cl_state->commit_point_observed, 0);
	pg_atomic_write_u32(&cl_state->committed_durable_confirmed, 0);
	pg_atomic_write_u32(&cl_state->committed_marker_durable, 0);
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

	/*
	 * Hardening v1.0.1 (P2 / F6 preflight, D13b): drive_drain runs INLINE in this
	 * backend and clean-aborts BEFORE any destructive step on a survivor NAK
	 * (nothing drained), leaving nak_received set (cl_clean_abort ends in IDLE, so
	 * we key on nak_received, not the phase).  Surface that as the spec's
	 * synchronous reject code instead of the bare ACCEPTED: a DISABLED NAK
	 * (mixed-mode) → rejected:peers_not_all_enabled; a NOT_IN_QUORUM NAK →
	 * rejected:not_in_quorum.  An escalate (real death/deadline) sets no NAK and
	 * still returns ACCEPTED — the node leaves via fail-stop, honoring the intent.
	 */
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		ClusterLeaveNakReason reason
			= (ClusterLeaveNakReason)pg_atomic_read_u32(&cl_state->nak_reason);

		if (reason == CLUSTER_LEAVE_NAK_NOT_IN_QUORUM)
			return CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM;
		if (reason == CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS)
			return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS; /* another leave is committing (P1-3) */
		/* DISABLED (or an unspecified refusal): a survivor is not enabled. */
		return CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED;
	}
	/*
	 * Hardening v1.0.3 (P1): drive_drain clean-aborts (no NAK) when its own ~5s
	 * announce-ACK wait expires with a survivor still silent.  cl_clean_abort lands
	 * back in IDLE and sets NO nak, so the bare return below would mis-report
	 * ACCEPTED.  It records CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE; map it to the
	 * same fail-closed reject the request-side preflight uses.
	 */
	if (pg_atomic_read_u32(&cl_state->abort_reason)
		== (uint32)CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE)
		return CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE;
	return CLUSTER_LEAVE_REQ_ACCEPTED;
}

/*
 * cluster_clean_leave_request -- public C driver entry (the operator SQL UDF
 * pg_cluster_clean_leave_request maps the result to its text return, D13b).
 *
 *	Hardening v1.0.3 (P1, same-node serialization): the unlocked phase==IDLE test
 *	in cl_request_body cannot serialize two same-node callers — phase stays IDLE
 *	through the multi-second preflight window (REQUESTED is set only AFTER the
 *	preflight), so without a reservation BOTH could pass it, both set REQUESTED, and
 *	both run cluster_clean_leave_drive_drain (which gates only on phase==REQUESTED)
 *	= a double GES drain / double PCM-X release.  Reserve request_in_progress with a
 *	CAS held for the WHOLE request (entry..return, including the inline drain); a
 *	second caller whose CAS fails is rejected:leave_in_progress.  The reservation is
 *	released on every path, including an ereport(ERROR) escape (PG_CATCH +
 *	PG_RE_THROW) — a leaked flag would wedge the node (no future leave) until restart.
 */
ClusterLeaveRequestResult
cluster_clean_leave_request(void)
{
	ClusterLeaveRequestResult result;
	uint32 expected = 0;

	if (cl_state == NULL || !cluster_enabled || !cluster_clean_leave_enabled)
		return CLUSTER_LEAVE_REQ_REJECTED_DISABLED;

	/* Reserve before any other work; a second concurrent caller is rejected. */
	if (!pg_atomic_compare_exchange_u32(&cl_state->request_in_progress, &expected, 1))
		return CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS;

	/* Clear any prior attempt's abort reason so the post-drain mapping is fresh. */
	pg_atomic_write_u32(&cl_state->abort_reason, (uint32)CLUSTER_LEAVE_ABORT_NONE);

	PG_TRY();
	{
		result = cl_request_body();
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&cl_state->request_in_progress, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();

	pg_atomic_write_u32(&cl_state->request_in_progress, 0);
	return result;
}

/*
 * cluster_clean_leave_drive_drain -- the synchronous leaving-node phases, run in
 * the requesting backend (CL-I9: the GCS flush is here, never in LMON).  Advances
 * REQUESTED -> QUIESCING -> GES_DRAINING -> GCS_FLUSHING -> BARRIER_WAIT, then
 * returns; the leaving-node LMON drives BARRIER_WAIT -> COMMITTED.  Every step
 * re-checks version coherence (CL-I3: an external epoch bump = a real death
 * intruded -> escalate) and the mixed-mode NAK (clean abort).
 */

/*
 * spec-2.29a ②b: snapshot the CSSD dead set EXCLUDING the leaving node.  The
 * coherence gate compares this "others-dead" set rather than the scalar global
 * dead_generation, so the leaving node's own expected alive->DEAD transition
 * (it stops heart-beating once its drain finishes) never falsely escalates the
 * leave.  A third-party death still changes this set and escalates (CL-I3).
 */
static void
cl_others_dead_snapshot(int32 leaving, uint8 *out /* CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES */)
{
	int i;

	memset(out, 0, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == leaving)
			continue; /* the leaving node's own DEAD is expected, not incoherence */
		if (i >= CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8)
			break;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			out[i / 8] |= (uint8)(1u << (i % 8));
	}
}

/*
 * spec-2.29a ②b: the coherence gate used by every clean-leave step.  Coherent
 * iff the epoch has not moved AND no THIRD-PARTY death changed the others-dead
 * set since the leave was bound.  cl_state->leaving_node_id names the node
 * whose own DEAD is excluded.
 */
static bool
cl_coherent(uint64 baseline_epoch)
{
	uint8 now_others_dead[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];

	cl_others_dead_snapshot(cl_state->leaving_node_id, now_others_dead);
	return cluster_clean_leave_version_coherent(
		baseline_epoch, cluster_epoch_get_current(), cl_state->leave_baseline_others_dead,
		now_others_dead, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES);
}

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

	/*
	 * F6 preflight (Hardening v1.0.1, P2): wait until the LMON has actually
	 * broadcast the announce AND every alive survivor has replied — either a NAK
	 * (a disabled / not-in-quorum survivor) or the readiness ACK from all of them
	 * — BEFORE any destructive drain step.  The announce is LMON-driven (IC sends
	 * are LMON-only), so its dispatch can lag a tick behind this backend; gating on
	 * announce_sent + the replies (not a fixed sleep) makes the mixed-mode reject
	 * deterministic, so the request can return the spec's synchronous
	 * rejected:peers_not_all_enabled (D13b).  Bounded (~5s, a few LMON ticks) so a
	 * silently-slow survivor falls through to the drain (a late NAK is still caught
	 * by the BARRIER_WAIT async abort); a NAK here is a clean abort, no drain.
	 */
	for (i = 0; i < 500; i++) { /* up to ~5s */
		if (pg_atomic_read_u32(&cl_state->nak_received))
			break;
		if (pg_atomic_read_u32(&cl_state->announce_sent) && cl_all_survivors_acked(cluster_node_id))
			break; /* announce out + all alive survivors ready; no refusal */
		pg_usleep(10 * 1000);
	}
	if (pg_atomic_read_u32(&cl_state->nak_received)) {
		cl_clean_abort();
		return;
	}
	/*
	 * Hardening v1.0.3 (P1, fail-CLOSED; mirrors the request-side preflight gate):
	 * the loop also exits on the ~5s deadline.  If it expired with no NAK but not
	 * every alive survivor ACKed, a survivor is silent — do NOT fall through into
	 * the destructive drain (quiesce / GES drain / GCS flush + PCM-X release) on an
	 * unconfirmed barrier (8.A: a survivor that never dropped its refs could serve
	 * a leaving-node block stale after the flush).  Record the reason so the
	 * request maps it to rejected:preflight_incomplete, then clean-abort (revert
	 * the REQUESTED marker -> IDLE).  NOT an escalate: a silent survivor is not a
	 * real death, so the fail-stop fallback must not fire.
	 */
	if (!cl_all_survivors_acked(cluster_node_id)) {
		pg_atomic_write_u32(&cl_state->abort_reason,
							(uint32)CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE);
		ereport(LOG, (errmsg("cluster clean-leave: not every alive survivor acknowledged the leave "
							 "before the deadline; clean-aborting (no drain, no commit)")));
		cl_clean_abort();
		return;
	}

	/*
	 * CL-I3 coherence gate, re-checked before every drain step.  Uses the
	 * dead_gen-aware helper, not an epoch-only compare: CSSD increments the
	 * others-dead set the moment it declares a THIRD-PARTY peer dead, which is
	 * STRICTLY BEFORE the reconfig coordinator bumps the membership epoch.
	 * Checking that set too lets us escalate in that earlier window instead of
	 * draining into a death that has not yet reached the epoch.  The leaving
	 * node's OWN DEAD is excluded (spec-2.29a ②b).  At commit the guarded CAS
	 * in apply_clean_leave_as_coordinator is the final authority.
	 */
#define CL_COHERENT() cl_coherent(baseline_epoch)

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
	 *
	 *	Hardening v1.0.4 (P2): this "epoch bump + dead_gen unchanged == OUR commit"
	 *	inference is sound ONLY because there is no OTHER dead_gen-unchanged reconfig
	 *	in flight.  A spec-5.15 online JOIN also bumps the epoch with dead_gen
	 *	unchanged and would be mis-observed here as the leave's commit (then the node
	 *	stops escalating but never gets the real LEAVE_COMMITTED -> BARRIER_WAIT
	 *	hang).  That collision is removed STRUCTURALLY by the one-membership-reconfig-
	 *	at-a-time serialization: the join driver does not bump the epoch while a clean
	 *	leave is active (cluster_clean_leave_in_progress gates drive_joins +
	 *	commit_member), and the leave does not start while a join is pending
	 *	(cluster_reconfig_join_in_progress gates the request).  Under that invariant a
	 *	bump with an unchanged version during a leave can only be the leave's own
	 *	commit.
	 *
	 *	spec-2.29a ②b + r2 P2-1: "unchanged version" here means the others-dead
	 *	bitmap AND the scalar dead_generation both unchanged.  The bitmap excludes
	 *	the leaving node's own expected DEAD (②b: else its heartbeat stop would
	 *	falsely escalate), but the bitmap is not monotone — a third-party
	 *	false-DEAD→ALIVE rebound restores it while the scalar dead_generation only
	 *	advances (r2 P2-1: else the leaver could mis-latch a refused leave and
	 *	hang).  The leaver's own DEAD never bumps its OWN dead_generation, so the
	 *	scalar conjunct is safe on this side (it would NOT be safe on the survivor
	 *	side, which keeps the bitmap-only coherence check).
	 */
	if (cluster_epoch_get_current() > baseline_epoch) {
		uint8 now_others_dead[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
		bool others_dead_unchanged;
		bool dead_gen_unchanged;

		cl_others_dead_snapshot(cl_state->leaving_node_id, now_others_dead);
		others_dead_unchanged = (memcmp(now_others_dead, cl_state->leave_baseline_others_dead,
										CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES)
								 == 0);
		dead_gen_unchanged
			= (cluster_cssd_get_dead_generation() == cl_state->leave_baseline_dead_gen);
		if (cluster_clean_leave_own_commit_latched(true, others_dead_unchanged,
												   dead_gen_unchanged)) {
			/*
			 * Commit point observed (our clean-leave epoch was published; no
			 * third-party death intruded).  The leave can no longer be
			 * un-committed, so from here the barrier deadline must NOT escalate
			 * (Hardening v1.0.1 P1-1).
			 */
			pg_atomic_write_u32(&cl_state->commit_point_observed, 1);
			LWLockAcquire(&cl_state->lock, LW_EXCLUSIVE);
			cl_state->leave_epoch = cluster_epoch_get_current(); /* the committed epoch E */
			LWLockRelease(&cl_state->lock);

			/*
			 * P1-V0.7 exit gate: reach COMMITTED ("may exit") ONLY after the
			 * coordinator confirms the COMMITTED marker is majority-durable
			 * (LEAVE_COMMITTED).  Until then stay in BARRIER_WAIT and re-tick — the
			 * durable truth-source must exist before this node departs, else a
			 * survivor restart could not rebuild the clean-departed fact.
			 */
			if (pg_atomic_read_u32(&cl_state->committed_durable_confirmed)) {
				cl_set_phase(CLUSTER_LEAVE_COMMITTED);
				CLUSTER_INJECTION_POINT("cluster-clean-leave-barrier-complete");
				ereport(LOG,
						(errmsg("cluster clean-leave: committed at epoch %llu + COMMITTED marker "
								"majority-durable; this node has drained and may exit",
								(unsigned long long)cl_state->leave_epoch)));
			}
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

	/* 3. fail-closed deadline — ONLY before the commit point (P1-1: a committed
	 * leave is never un-committed; post-commit we wait for the durable marker,
	 * bounded by disk health, and never escalate). */
	if (!pg_atomic_read_u32(&cl_state->commit_point_observed)
		&& (uint64)GetCurrentTimestamp() > cl_state->barrier_deadline_us) {
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

	if (cl_lmon_marker_async.has_staged_event) {
		ClusterLeaveAsyncMarkerResult ar = cl_poll_lmon_marker_stage();
		int phase = cl_lmon_marker_phase;
		uint64 staged_epoch = cl_lmon_marker_epoch;

		if (ar == CL_LEAVE_ASYNC_PENDING)
			return;
		if (ar == CL_LEAVE_ASYNC_FAILED)
			return;
		if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTING) {
			cl_release_lmon_marker_stage();

			/*
			 * spec-2.29a review r1 P1 — CL-I3 re-check at the staged-ACK
			 * handoff.  The COMMITTING marker wait now spans LMON ticks, so a
			 * real death can bump CSSD dead_generation INSIDE the wait window
			 * while the fail-stop epoch has not yet advanced (the >=3-node
			 * window of Hardening v1.0.1 P1-2).  The guarded CAS inside
			 * apply_clean_leave_as_coordinator only catches the epoch move;
			 * re-run the same dead_gen-aware coherence check the non-staged
			 * pre-check uses, at the last observable point before the commit
			 * applies.  On failure the leave does not commit and the leaving
			 * node escalates to fail-stop (identical to the pre-check path).
			 */
			if (!cl_coherent(baseline_epoch)) {
				ereport(LOG,
						(errmsg("cluster clean-leave: version moved (epoch or third-party death) "
								"across the COMMITTING marker wait for node %d; not committing "
								"(escalate to fail-stop, CL-I3)",
								leaving)));
				return;
			}

			new_epoch = cluster_reconfig_apply_clean_leave_as_coordinator(leaving, baseline_epoch);
			if (new_epoch == 0) {
				ereport(
					LOG,
					(errmsg("cluster clean-leave: epoch moved off baseline %llu before commit "
							"for node %d; not committing (the leaving node escalates to fail-stop)",
							(unsigned long long)baseline_epoch, leaving)));
				return;
			}
			cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving, new_epoch);
			(void)cl_start_lmon_marker_stage(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTED, leaving,
											 new_epoch);
			(void)cl_poll_lmon_marker_stage();
			ereport(LOG,
					(errmsg("cluster clean-leave: committed departure of node %d at epoch %llu",
							leaving, (unsigned long long)new_epoch)));
			return;
		}
		if (phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTED) {
			pg_atomic_write_u32(&cl_state->committed_marker_durable, 1);
			cl_send_committed(leaving, staged_epoch);
			cl_release_lmon_marker_stage();
			return;
		}
		cl_release_lmon_marker_stage();
		return;
	}

	/* CL-I3 pre-check: refuse to commit a clean leave on a version a real death
	 * already bumped — the leaving node will then observe a non-CLEAN_LEAVE event
	 * and escalate.  This is a cheap early-out; the authoritative guard is the
	 * guarded CAS inside apply_clean_leave_as_coordinator below, which closes the
	 * check-then-bump TOCTOU at >=3 nodes. */
	/*
	 * CL-I3 commit-handoff coherence (epoch AND others-dead): refuse to commit if
	 * a THIRD-PARTY death intruded since this survivor started tracking the leave
	 * — either the death's fail-stop already bumped the epoch, OR (the >=3-node
	 * window, Hardening v1.0.1 P1-2) CSSD added it to the others-dead set but the
	 * fail-stop epoch has NOT yet advanced, which an epoch-only check (and the
	 * guarded CAS below) would miss and commit on a stale membership view.  Uses
	 * the same others-dead coherence helper as drive_drain (spec-2.29a ②b: the
	 * leaving node's own expected DEAD is excluded so it never falsely escalates);
	 * the leaving node then observes the eventual foreign event and escalates.
	 */
	if (!cl_coherent(baseline_epoch)) {
		ereport(LOG,
				(errmsg("cluster clean-leave: version moved (epoch or third-party death) before "
						"committing node %d; not committing (escalate to fail-stop, CL-I3)",
						leaving)));
		return;
	}

	/* (1) COMMITTING(E) marker (coordinator's own slot, before the bump; NOT a
	 * trust basis).  Not durable -> do not commit. */
	cl_build_marker(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING, leaving, baseline_epoch + 1);
	(void)cl_start_lmon_marker_stage(&m, CLUSTER_LEAVE_MARKER_PHASE_COMMITTING, leaving,
									 baseline_epoch + 1);
	(void)cl_poll_lmon_marker_stage();
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

	/*
	 * 2a (Hardening v1.0.1, P1-V0.7 exit gate): after the commit, the coordinator
	 * must drive the COMMITTED marker to majority-durability — never give up.  If
	 * the first attempt in cl_coordinator_commit did not reach majority, retry it
	 * here every tick until it does; once durable, tell the leaving node it may
	 * exit (LEAVE_COMMITTED), re-sending each tick while the leaver is alive (best-
	 * effort delivery, idempotent gate).  Until durable the leaving node stays in
	 * BARRIER_WAIT and never departs without a durable truth-source.
	 */
	if (coordinator == cluster_node_id && cluster_reconfig_is_clean_departed(leaving)) {
		uint64 committed_epoch = cluster_reconfig_get_clean_departed_epoch(leaving);

		if (!pg_atomic_read_u32(&cl_state->committed_marker_durable))
			cl_drive_committed_marker_stage(leaving, committed_epoch);

		/* Re-send LEAVE_COMMITTED every tick once durable until the leaver is gone
		 * (best-effort IC): the leaving node will not depart until it receives one,
		 * and step 3 holds the slot until it is CSSD-dead, so delivery is assured. */
		if (pg_atomic_read_u32(&cl_state->committed_marker_durable))
			cl_send_committed(leaving, committed_epoch);
	}

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

	/* 3. release the local leave-tracking slot only once the leave is committed
	 * (clean_departed) AND the leaving node has actually departed (CSSD DEAD).
	 * Holding it until departure (a) lets the coordinator keep re-sending
	 * LEAVE_COMMITTED until the leaver is gone (assured delivery, P1-1), and (b)
	 * serializes leaves (a second leave is NAK'd until this one fully departs,
	 * single-leave-at-a-time, P1-3).  clean_departed persists in the reconfig
	 * region and suppresses the node's CSSD DEAD from a spurious fail-stop (CL-I13). */
	if (cluster_reconfig_is_clean_departed(leaving)
		&& cluster_cssd_get_peer_state(leaving) == CLUSTER_CSSD_PEER_DEAD) {
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

	/*
	 * Hardening v1.0.2 (P1, F6 layer-1 preflight): the request backend staged a
	 * side-effect-free preflight probe (IC sends are LMON-only).  Broadcast it once
	 * here, BEFORE the leaving_node_id check below — during the preflight the
	 * leaver has NOT yet entered REQUESTED, so leaving_node_id is still -1.
	 */
	if (pg_atomic_read_u32(&cl_state->preflight_pending) == 1
		&& pg_atomic_read_u32(&cl_state->preflight_sent) == 0) {
		cluster_clean_leave_ic_broadcast_announce(
			0 /* leave_epoch=0 for a probe */, pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
			/*preflight*/ true);
		pg_atomic_write_u32(&cl_state->preflight_sent, 1);
	}

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
			cluster_clean_leave_ic_broadcast_announce(
				cl_state->leave_epoch, pg_atomic_read_u64(&cl_state->leave_attempt_nonce),
				/*preflight*/ false);
			pg_atomic_write_u32(&cl_state->announce_sent, 1);
		}

		/* the backend drives up to BARRIER_WAIT; LMON finishes the commit. */
		if (phase == CLUSTER_LEAVE_BARRIER_WAIT)
			cl_leaving_barrier_tick();
	} else {
		cl_survivor_tick(leaving);
	}
}
