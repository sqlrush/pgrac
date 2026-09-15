/*-------------------------------------------------------------------------
 *
 * cluster_clean_leave_policy.c
 *	  pgrac clean leave reconfiguration (spec-5.13) — pure decision layer.
 *
 *	  Every function here is pure: it decides a phase-FSM transition, a
 *	  quiesce gate, a version-coherence check, or validates a leave-intent
 *	  marker, with no PostgreSQL runtime dependency (no shmem, no locks, no
 *	  voting-disk I/O, no ereport).  That keeps the correctness-critical
 *	  decisions (CL-I3 version-coherent, CL-I6 writable-only, marker trust)
 *	  exercisable directly by cluster_unit (test_cluster_clean_leave).
 *
 *	  The runtime layer (cluster_clean_leave.c) snapshots the live facts
 *	  (epoch, dead_generation, transaction state, on-disk marker) and feeds
 *	  them here, then acts on the verdict.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_clean_leave_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.13-clean-leave-reconfig.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_clean_leave.h"
#include "port/pg_crc32c.h"


/* ============================================================
 * Phase-FSM transition validity (D2 / U2)
 * ============================================================ */

/*
 * cluster_clean_leave_phase_valid_transition -- is from->to a legal edge?
 *
 *	Forward drain path:
 *	  IDLE -> REQUESTED -> QUIESCING -> GES_DRAINING -> GCS_FLUSHING ->
 *	  BARRIER_WAIT -> COMMITTED.
 *	Clean abort (nothing drained yet) only from REQUESTED -> ABORTED.
 *	Escalate (real death / deadline mid-drain) from any active phase
 *	(REQUESTED..BARRIER_WAIT) -> ABORTED_ESCALATE.
 *	Terminal phases reset to IDLE (post-leave / post-abort clean slate).
 *	IDLE -> IDLE is a no-op self-loop.
 */
bool
cluster_clean_leave_phase_valid_transition(ClusterLeavePhase from, ClusterLeavePhase to)
{
	/* escalate is reachable from every active (mid-leave) phase */
	if (to == CLUSTER_LEAVE_ABORTED_ESCALATE)
		return (from == CLUSTER_LEAVE_REQUESTED || from == CLUSTER_LEAVE_QUIESCING
				|| from == CLUSTER_LEAVE_GES_DRAINING || from == CLUSTER_LEAVE_GCS_FLUSHING
				|| from == CLUSTER_LEAVE_BARRIER_WAIT);

	switch (from) {
	case CLUSTER_LEAVE_IDLE:
		return (to == CLUSTER_LEAVE_REQUESTED || to == CLUSTER_LEAVE_IDLE);
	case CLUSTER_LEAVE_REQUESTED:
		/* QUIESCING (marker durable) or clean ABORTED (preflight/NAK) */
		return (to == CLUSTER_LEAVE_QUIESCING || to == CLUSTER_LEAVE_ABORTED);
	case CLUSTER_LEAVE_QUIESCING:
		return (to == CLUSTER_LEAVE_GES_DRAINING);
	case CLUSTER_LEAVE_GES_DRAINING:
		return (to == CLUSTER_LEAVE_GCS_FLUSHING);
	case CLUSTER_LEAVE_GCS_FLUSHING:
		return (to == CLUSTER_LEAVE_BARRIER_WAIT);
	case CLUSTER_LEAVE_BARRIER_WAIT:
		return (to == CLUSTER_LEAVE_COMMITTED);
	case CLUSTER_LEAVE_COMMITTED:
	case CLUSTER_LEAVE_ABORTED:
	case CLUSTER_LEAVE_ABORTED_ESCALATE:
		/* terminal phases reset to IDLE for a clean slate */
		return (to == CLUSTER_LEAVE_IDLE);
	}
	return false; /* default-deny any unlisted edge */
}

const char *
cluster_clean_leave_phase_str(int phase)
{
	switch (phase) {
	case CLUSTER_LEAVE_IDLE:
		return "idle";
	case CLUSTER_LEAVE_REQUESTED:
		return "requested";
	case CLUSTER_LEAVE_QUIESCING:
		return "quiescing";
	case CLUSTER_LEAVE_GES_DRAINING:
		return "ges_draining";
	case CLUSTER_LEAVE_GCS_FLUSHING:
		return "gcs_flushing";
	case CLUSTER_LEAVE_BARRIER_WAIT:
		return "barrier_wait";
	case CLUSTER_LEAVE_COMMITTED:
		return "committed";
	case CLUSTER_LEAVE_ABORTED:
		return "aborted";
	case CLUSTER_LEAVE_ABORTED_ESCALATE:
		return "aborted_escalate";
	default:
		return "(unknown)";
	}
}


/* ============================================================
 * writable-only quiesce gate (D7 / U5 / CL-I6)
 * ============================================================ */

/*
 * cluster_clean_leave_should_abort_writable -- abort iff in a transaction that
 * has allocated a real top-level xid (a writable tx).  read-only SELECT (in a
 * transaction but no xid) and idle / post-commit (not in a transaction) absorb
 * the quiesce silently.  Mirrors the §2.2 ProcessInterrupts decision.
 */
bool
cluster_clean_leave_should_abort_writable(bool in_transaction, bool has_top_xid)
{
	return in_transaction && has_top_xid;
}


/* ============================================================
 * version-coherent leave (D2 / U3 / CL-I3 / L235)
 * ============================================================ */

/*
 * cluster_clean_leave_version_coherent -- the bound leave is still coherent
 * only if neither the cluster epoch nor the OTHERS-dead set (every dead node
 * except the leaving node itself) moved since the leave was bound.  Any
 * external membership change — a third-party death intruding mid-drain, or a
 * third-party fail-stop that already bumped the epoch — makes it incoherent →
 * caller must ABORTED_ESCALATE (never complete a clean leave on a stale
 * membership view, which would let a destructive sweep run on a newer view
 * and double-grant, CL-I3).
 *
 * spec-2.29a ②b fix: the pre-fix predicate compared the SCALAR global CSSD
 * dead_generation, which also counts the leaving node's OWN alive→DEAD
 * transition (it stops heart-beating once its drain finishes — the EXPECTED
 * terminal state of a clean leave).  Under the async COMMITTING marker that
 * spans several LMON ticks, a slow environment lets that transition land
 * inside the wait window and the scalar check wrongly escalated an otherwise
 * healthy leave.  Comparing the others-dead bitmap (which excludes the
 * leaving node) removes exactly that false positive while keeping every
 * third-party incoherence escalation (see the reflexive-case matrix in
 * spec-2.29a §②b 8.A argument).
 */
bool
cluster_clean_leave_version_coherent(uint64 bound_epoch, uint64 current_epoch,
									 const uint8 *bound_others_dead,
									 const uint8 *current_others_dead, int nbytes)
{
	if (bound_epoch != current_epoch)
		return false;
	if (bound_others_dead == NULL || current_others_dead == NULL)
		return false; /* fail-closed: cannot prove coherence without both views */
	return memcmp(bound_others_dead, current_others_dead, (size_t)nbytes) == 0;
}

/*
 * cluster_clean_leave_own_commit_latched -- the LEAVING node's barrier tick
 * latches "my clean-leave committed" on direct EVIDENCE, never on inference
 * (spec-2.29a r3).  The evidence is committed_marker_evidence: the survivor
 * coordinator made the COMMITTED marker for THIS leave attempt majority-
 * durable on the voting disk and attested it with a nonce-bound
 * LEAVE_COMMITTED (validated by cluster_clean_leave_committed_evidence_
 * matches before the flag this argument mirrors is ever set).  Latching
 * suppresses the barrier-deadline escalation and lets the leaver exit, so
 * the verdict must be exactly: latch <=> evidence.
 *
 * History of the two inference failure modes this replaces:
 *   - r2 P2-1 (mis-latch wedge): the pre-r2 "epoch advanced + others-dead
 *     bitmap unchanged" inference could mis-latch a REFUSED leave after a
 *     third-party false-DEAD -> ALIVE rebound restored the (non-monotone)
 *     bitmap, suppressing the deadline escalation forever.
 *   - r3 (t/331 C1/C4 false-escalation): the r2 scalar dead_generation
 *     conjunct is monotone the other way — a third-party transient flap on
 *     the leaver's local CSSD view advances it forever, so a healthy
 *     committed leave was refused until the deadline escalated it.
 * Marker evidence is immune to both: a refused leave never gets a COMMITTED
 * marker (no latch -> bounded deadline escalation), and a flap cannot make
 * durable evidence disappear (latch -> no false escalation).
 *
 * others_dead_unchanged / dead_gen_unchanged are the leaver's live coherence
 * observations.  They are deliberately kept in the signature as contract
 * inputs that MUST NOT affect the verdict — the U3b unit matrix pins the
 * flap-immunity on them — and the runtime uses them only for the flap-noise
 * LOG at the latch point (observability, never control flow).
 */
bool
cluster_clean_leave_own_commit_latched(bool committed_marker_evidence, bool others_dead_unchanged,
									   bool dead_gen_unchanged)
{
	(void)others_dead_unchanged; /* observability-only input (see above) */
	(void)dead_gen_unchanged;	 /* observability-only input (see above) */
	return committed_marker_evidence;
}

/*
 * cluster_clean_leave_committed_evidence_matches -- may the leaving node
 * accept a LEAVE_COMMITTED confirmation as marker evidence for THIS leave
 * attempt?  (spec-2.29a r3; the payload's magic/version/CRC were already
 * checked by cluster_clean_leave_announce_payload_valid.)
 *
 *	Identity is bound three ways, all fail-closed:
 *	  - payload_leaving_node == self_node: the confirmation is addressed to
 *	    this node's own leave (LEAVE_COMMITTED is point-to-point, but a
 *	    misrouted frame must still not latch).
 *	  - current_leaving_node == self_node: this node IS currently the leaver
 *	    (not idle, not a survivor of someone else's leave).
 *	  - payload_nonce == current_attempt_nonce: the per-attempt nonce
 *	    (Hardening v1.0.2) pins the confirmation to THIS attempt, so a stale
 *	    LEAVE_COMMITTED — and through it a stale COMMITTED marker — from a
 *	    PREVIOUS leave of the same node can never false-latch a new attempt.
 *	  - payload_epoch > bound_leave_epoch: the committed epoch E the
 *	    coordinator attests must lie past the baseline this leave bound
 *	    (sanity; the commit is a guarded CAS off that baseline).
 */
bool
cluster_clean_leave_committed_evidence_matches(int32 payload_leaving_node, uint64 payload_nonce,
											   uint64 payload_epoch, int32 self_node,
											   int32 current_leaving_node,
											   uint64 current_attempt_nonce,
											   uint64 bound_leave_epoch)
{
	if (payload_leaving_node != self_node)
		return false;
	if (current_leaving_node != self_node)
		return false;
	if (payload_nonce != current_attempt_nonce)
		return false;
	if (payload_epoch <= bound_leave_epoch)
		return false;
	return true;
}


/* ============================================================
 * leave-intent marker structural validation (D2 / §2.5)
 * ============================================================ */

/* CRC32C over [magic .. phase] — everything before the trailing crc field. */
static pg_crc32c
clean_leave_marker_crc(const ClusterLeaveIntentMarker *m)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, m, offsetof(ClusterLeaveIntentMarker, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_clean_leave_marker_compute_crc(ClusterLeaveIntentMarker *m)
{
	m->crc = clean_leave_marker_crc(m);
}

/*
 * cluster_clean_leave_marker_struct_valid -- magic / version / CRC / identity.
 *
 *	Checks the marker is a well-formed leave-intent marker for the expected
 *	declared peer: magic + version + CRC match, leaving_node_id == expected,
 *	and dead_bitmap names ONLY the leaving node.  Does NOT consult epoch
 *	(epoch is not durable; epoch-floor recovery / sanity is the caller's job,
 *	§2.5 P1-V0.7).
 */
bool
cluster_clean_leave_marker_struct_valid(const ClusterLeaveIntentMarker *m,
										int32 expected_leaving_node)
{
	int i;

	if (m->magic != CLUSTER_LEAVE_MARKER_MAGIC)
		return false;
	if (m->version == 0 || m->version > CLUSTER_LEAVE_MARKER_VERSION)
		return false;
	if (m->crc != clean_leave_marker_crc(m))
		return false;
	if (expected_leaving_node < 0 || m->leaving_node_id != expected_leaving_node)
		return false;

	/* dead_bitmap must name ONLY the leaving node (departed-clean set == {N}) */
	if (m->leaving_node_id / 8 >= CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES)
		return false;
	for (i = 0; i < CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES; i++) {
		uint8 expect = 0;

		if (i == m->leaving_node_id / 8)
			expect = (uint8)(1u << (m->leaving_node_id % 8));
		if (m->dead_bitmap[i] != expect)
			return false;
	}
	return true;
}

/*
 * cluster_clean_leave_marker_is_committed_basis -- is this marker a valid
 * clean-departed rebuild basis?  ONLY a struct-valid COMMITTED marker (written
 * after the real commit point) is trustworthy for startup rebuild (§2.5).
 */
bool
cluster_clean_leave_marker_is_committed_basis(const ClusterLeaveIntentMarker *m,
											  int32 expected_leaving_node)
{
	return m->phase == CLUSTER_LEAVE_MARKER_PHASE_COMMITTED
		   && cluster_clean_leave_marker_struct_valid(m, expected_leaving_node);
}


/* ------------------------------------------------------------------
 * Survivor-side serve / invalidate gates (D5 / CL-I5 / CL-I10).
 *
 *	These are the pure-decision halves of the survivor's storage-fallback
 *	soundness contract; the runtime call-sites (block-serve path, epoch-
 *	advance observe) wire them in spec-5.13 S6.  Kept here so the unit
 *	harness exercises the true/false branches with no PG runtime dep.
 * ------------------------------------------------------------------
 */

/*
 * cluster_clean_leave_should_invalidate -- has the survivor observed the
 * cluster epoch reach the bound leave_epoch?
 *
 *	CL-I10 stable-baseline observe gate: observed_epoch is the survivor's
 *	locally-tracked monotone epoch (NOT a leave event's fresh-read old_epoch
 *	field — an IC piggyback can deliver that before the local detector and
 *	wedge the gate forever).  A non-zero leave_epoch that the observed epoch
 *	has reached (>=) means the leaving node has committed; the survivor must
 *	now invalidate its stale cache of the leaving node's blocks before any
 *	post-epoch storage read (the happens-before boundary for CL-I5).
 *	leave_epoch == 0 means no leave is bound.
 */
bool
cluster_clean_leave_should_invalidate(uint64 observed_epoch, uint64 leave_epoch)
{
	return leave_epoch != 0 && observed_epoch >= leave_epoch;
}

/*
 * cluster_clean_leave_serve_gate_allows -- may a survivor serve a leaving
 * node's block from the storage fallback?
 *
 *	§0.3 命门 / CL-I5 storage-authoritative-only-after-flush.  A block that
 *	is NOT a leaving-node block always serves the normal way.  A block that
 *	IS a leaving-node block may be served from storage ONLY after the leaving
 *	node flushed it (force-WAL-log + FlushBuffer + release X) AND the survivor
 *	invalidated its stale cache (leave_flushed_invalidated).  Before that the
 *	caller MUST fail-closed (freeze_queue / 53R62 retry) — never read stale
 *	storage (Rule 8.A).  Fail-closed is the false return here.
 */
bool
cluster_clean_leave_serve_gate_allows(bool block_from_leaving, bool leave_flushed_invalidated)
{
	return !block_from_leaving ? true : leave_flushed_invalidated;
}

/* RF-ROOT P04 Scheme A.  A legacy/unmanaged boot retains the established
 * clean-leave behavior.  Once the readiness lifecycle is managed, clean leave
 * is an ordinary serving transition and cannot start or advance before OPEN. */
bool
cluster_clean_leave_startup_serving_allows(bool authority_managed,
										 bool serving_ready)
{
	return !authority_managed || serving_ready;
}


/* ------------------------------------------------------------------
 * IC payload integrity (D8 / U9 / rule 15) — pure CRC + validation.
 *
 *	Defense-in-depth on top of the IC envelope CRC: each clean-leave payload
 *	carries its own magic/version/CRC so a misrouted or version-skewed message
 *	is never acted on as a clean-leave command.  Node ids are range-checked
 *	against the 128-bit bitmap width here; the declared-peer identity check is
 *	the membership layer's job (cluster_clean_leave.c).
 * ------------------------------------------------------------------
 */

#define CLUSTER_CLEAN_LEAVE_MAX_NODE_ID (CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES * 8)

static pg_crc32c
clean_leave_announce_crc(const ClusterLeaveAnnouncePayload *p)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, offsetof(ClusterLeaveAnnouncePayload, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_clean_leave_announce_compute_crc(ClusterLeaveAnnouncePayload *p)
{
	p->crc = clean_leave_announce_crc(p);
}

bool
cluster_clean_leave_announce_payload_valid(const ClusterLeaveAnnouncePayload *p)
{
	if (p->magic != CLUSTER_CLEAN_LEAVE_IC_MAGIC)
		return false;
	/*
	 * Hardening v1.0.3 (P2): require the EXACT current wire version.  The v1->v2
	 * widening (leave_nonce) moved the crc offset and the struct width, so a v1
	 * sender's narrower frame parsed as v2 is garbage past cssd_dead_generation;
	 * accepting version<CURRENT would rely on the crc-offset mismatch happening to
	 * differ, not on an explicit gate.  Drop any non-current version (mixed-version
	 * fail-closed, 8.A defense-in-depth) — there is no length field to disambiguate.
	 */
	if (p->version != CLUSTER_CLEAN_LEAVE_IC_VERSION)
		return false;
	if (p->crc != clean_leave_announce_crc(p))
		return false;
	if (p->leaving_node_id < 0 || p->leaving_node_id >= CLUSTER_CLEAN_LEAVE_MAX_NODE_ID)
		return false;
	/* RF-ROOT P6 (shutdown-handoff wiring): the named producer byte must be a
	 * known kind (an unknown value fails closed like a bad version). */
	if (p->producer_kind > CLUSTER_LEAVE_PRODUCER_SHUTDOWN)
		return false;
	if (p->preflight > CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT)
		return false;
	if (p->preflight >= CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
		&& (p->producer_kind != CLUSTER_LEAVE_PRODUCER_SHUTDOWN || p->_pad0 != 0 || p->_pad1[0] != 0
			|| p->_pad1[1] != 0
			|| p->leaving_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
			/* Existing epoch bytes carry either pristine0 or the bound OPEN
			 * epoch. Only the corresponding runtime consumer grants meaning. */
			|| p->leave_epoch == UINT64_MAX || p->cssd_dead_generation != 0 || p->leave_nonce == 0
			|| p->leave_nonce == UINT64_MAX))
		return false;
	return true;
}

static pg_crc32c
clean_leave_ack_crc(const ClusterLeaveAckPayload *p)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, offsetof(ClusterLeaveAckPayload, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_clean_leave_ack_compute_crc(ClusterLeaveAckPayload *p)
{
	p->crc = clean_leave_ack_crc(p);
}

bool
cluster_clean_leave_ack_payload_valid(const ClusterLeaveAckPayload *p)
{
	if (p->magic != CLUSTER_CLEAN_LEAVE_IC_MAGIC)
		return false;
	/* Hardening v1.0.3 (P2): exact version gate (see announce validator). */
	if (p->version != CLUSTER_CLEAN_LEAVE_IC_VERSION)
		return false;
	if (p->crc != clean_leave_ack_crc(p))
		return false;
	if (p->survivor_node_id < 0 || p->survivor_node_id >= CLUSTER_CLEAN_LEAVE_MAX_NODE_ID)
		return false;
	if (p->leaving_node_id < 0 || p->leaving_node_id >= CLUSTER_CLEAN_LEAVE_MAX_NODE_ID)
		return false;
	if (p->phase1_round != 0
		&& p->phase1_round != CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE)
		return false;
	if (p->phase1_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
		&& (p->_pad0 != 0 || p->_pad1 != 0
			|| p->survivor_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
			|| p->leaving_node_id >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
			|| p->survivor_node_id == p->leaving_node_id || p->nak != 0
			|| p->nak_reason != CLUSTER_LEAVE_NAK_NONE || p->leave_epoch == UINT64_MAX
			|| p->leave_nonce == 0 || p->leave_nonce == UINT64_MAX))
		return false;
	return true;
}

/* ------------------------------------------------------------------
 * Stage 8 phase-1 coordinated full-cluster clean-stop policy.
 * ------------------------------------------------------------------ */

bool
cluster_clean_leave_phase1_full_stop_plan_valid(
	const ClusterPhase1FullStopPlan *plan)
{
	int i;

	if (plan == NULL || !plan->valid || plan->epoch != 0
		|| plan->attempt_nonce == 0 || plan->attempt_nonce == UINT64_MAX
		|| plan->absolute_deadline_us == 0
		|| plan->absolute_deadline_us == UINT64_MAX
		|| plan->own_wal_started_at <= 0)
		return false;
	for (i = 0; i < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; i++) {
		if (plan->member_incarnations[i] == 0
			|| plan->member_incarnations[i] == UINT64_MAX)
			return false;
	}
	return true;
}

bool
cluster_clean_leave_phase1_full_stop_probe_accepts(
	uint8 producer_kind, bool preflight, int32 envelope_source_node,
	int32 payload_leaving_node, uint64 envelope_epoch, uint64 payload_epoch,
	uint64 attempt_nonce, bool local_fast_shutdown, bool exact_phase1_eligible)
{
	return producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& preflight
		&& envelope_source_node >= 0
		&& envelope_source_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		&& envelope_source_node == payload_leaving_node
		&& envelope_epoch == payload_epoch
		&& payload_epoch == 0
		&& attempt_nonce != 0 && attempt_nonce != UINT64_MAX
		&& local_fast_shutdown && exact_phase1_eligible;
}

bool
cluster_clean_leave_phase1_full_stop_probe_phase_accepts(
	bool source_active, bool source_stopped,
	bool local_active, bool local_stopped)
{
	/* The two rounds deliberately reuse one wire shape.  Existing source
	 * WAL/root evidence is therefore the phase discriminator: an ACTIVE source
	 * is still in the first round, while STOPPED+CLOSED is the second round.
	 * A receiver may already have closed while helping a slower first-round
	 * peer, but it must never ACK a second-round peer before its own close. */
	if (source_active == source_stopped
		|| local_active == local_stopped)
		return false;
	if (source_stopped)
		return local_stopped;
	return source_active;
}

bool
cluster_clean_leave_phase1_full_stop_post_stopped_receiver_ready(
	bool local_stopped, bool request_in_progress, bool shutdown_driven,
	bool preflight_pending, bool preflight_sent, bool release_pending)
{
	/* The STOPPED after-image becomes an acknowledged local terminal only
	 * after its publisher has confirmed the CF release and armed this process's
	 * second round.  Requiring the existing dispatch edge as well prevents a
	 * peer from exiting before it has had a chance to receive our request.  A
	 * locally completed barrier remains the same exact round while its bounded
	 * release/completion phase is active; accepting a slower peer's replay in
	 * that interval prevents a one-way completion race without reopening the
	 * round or refreshing its deadline. */
	return local_stopped && request_in_progress && shutdown_driven
		&& preflight_sent && (preflight_pending || release_pending);
}

bool
cluster_clean_leave_phase1_full_stop_release_probe_accepts(
	uint8 producer_kind, uint8 wire_round, int32 envelope_source_node,
	int32 payload_leaving_node, uint64 envelope_epoch, uint64 payload_epoch,
	uint64 nonce, bool source_stopped, bool local_stopped,
	bool exact_phase1_eligible)
{
	return producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& wire_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RELEASE
		&& envelope_source_node >= 0
		&& envelope_source_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		&& envelope_source_node == payload_leaving_node
		&& envelope_epoch == payload_epoch
		&& payload_epoch == 0
		&& nonce != 0 && nonce != UINT64_MAX
		&& source_stopped && local_stopped && exact_phase1_eligible;
}

bool
cluster_clean_leave_phase1_full_stop_receipt_accepts(
	uint8 producer_kind, uint8 wire_round, bool release_pending,
	uint64 stored_nonce, uint64 receipt_nonce, bool request_seen)
{
	/* The exact receipt is peer-consumption evidence for a reply whose local
	 * reply_sent publication may still be catching up after dispatch.  Do not
	 * require that local bookkeeping bit here: the terminal predicate checks it
	 * independently together with every other delivery bit and transport drain. */
	return producer_kind == CLUSTER_LEAVE_PRODUCER_SHUTDOWN
		&& wire_round == CLUSTER_PHASE1_FULL_STOP_WIRE_RECEIPT
		&& release_pending
		&& stored_nonce != 0 && stored_nonce != UINT64_MAX
		&& receipt_nonce == stored_nonce
		&& request_seen;
}

bool
cluster_clean_leave_phase1_full_stop_release_complete(
	const ClusterPhase1FullStopPlan *plan, int32 self_node,
	const uint8 *request_sent, const uint8 *request_seen,
	const uint8 *reply_sent, const uint8 *reply_seen,
	const uint8 *receipt_sent, const uint8 *receipt_seen,
	int nbytes, bool transport_drained)
{
	return transport_drained
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, request_sent, nbytes)
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, request_seen, nbytes)
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, reply_sent, nbytes)
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, reply_seen, nbytes)
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, receipt_sent, nbytes)
		&& cluster_clean_leave_phase1_full_stop_ack_complete(
			plan, self_node, receipt_seen, nbytes);
}

bool
cluster_clean_leave_phase1_full_stop_request_ahead_can_consume(
	bool retained, bool retained_before_local_round,
	uint64 retained_local_nonce, uint64 retained_deadline_us,
	uint64 current_local_nonce, uint64 current_deadline_us,
	bool exact_identity, bool request_in_progress,
	bool shutdown_driven, bool post_requests_sent)
{
	if (!retained || !exact_identity || !request_in_progress
		|| !shutdown_driven || !post_requests_sent
		|| retained_local_nonce == 0
		|| retained_local_nonce == UINT64_MAX
		|| retained_deadline_us == 0
		|| retained_deadline_us == UINT64_MAX
		|| current_local_nonce == 0
		|| current_local_nonce == UINT64_MAX
		|| current_deadline_us != retained_deadline_us)
		return false;
	if (retained_before_local_round)
		return cluster_clean_leave_phase1_full_stop_nonce_fresh(
			retained_local_nonce, current_local_nonce);
	return current_local_nonce == retained_local_nonce;
}

bool
cluster_clean_leave_phase1_full_stop_request_ahead_uses_predecessor_nonce(
	bool local_active, bool local_stopped, bool local_post_round_armed)
{
	/* WAL STOPPED is published before the checkpointer freezes the fresh
	 * post-STOPPED nonce.  That interval still belongs to the predecessor
	 * local round even though the physical WAL after-image is already STOPPED.
	 * Once the post round is armed, an early peer request binds the current
	 * nonce and must not authorize another transition. */
	if (local_active == local_stopped)
		return false;
	return local_active || (local_stopped && !local_post_round_armed);
}

bool
cluster_clean_leave_phase1_full_stop_ack_matches(
	const ClusterPhase1FullStopPlan *plan, int32 self_node,
	int32 envelope_source_node, int32 survivor_node, int32 leaving_node,
	uint64 payload_epoch, uint64 attempt_nonce,
	uint64 current_survivor_incarnation)
{
	return cluster_clean_leave_phase1_full_stop_plan_valid(plan)
		&& self_node >= 0
		&& self_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		&& survivor_node >= 0
		&& survivor_node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		&& survivor_node != self_node
		&& envelope_source_node == survivor_node
		&& leaving_node == self_node
		&& payload_epoch == plan->epoch
		&& attempt_nonce == plan->attempt_nonce
		&& current_survivor_incarnation
			== plan->member_incarnations[survivor_node];
}

bool
cluster_clean_leave_phase1_full_stop_ack_complete(
	const ClusterPhase1FullStopPlan *plan, int32 self_node,
	const uint8 *ack_bitmap, int nbytes)
{
	uint8 expected;
	int i;

	if (!cluster_clean_leave_phase1_full_stop_plan_valid(plan)
		|| self_node < 0
		|| self_node >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT
		|| ack_bitmap == NULL
		|| nbytes != CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES)
		return false;
	expected = (uint8)(UINT8_C(0x0f) & ~(UINT8_C(1) << self_node));
	if (ack_bitmap[0] != expected)
		return false;
	for (i = 1; i < nbytes; i++) {
		if (ack_bitmap[i] != 0)
			return false;
	}
	return true;
}

bool
cluster_clean_leave_phase1_full_stop_nonce_fresh(uint64 prior_nonce,
										uint64 next_nonce)
{
	return prior_nonce != 0 && prior_nonce != UINT64_MAX
		&& next_nonce != 0 && next_nonce != UINT64_MAX
		&& next_nonce != prior_nonce;
}

ClusterPhase1FullStopProbeNonceDecision
cluster_clean_leave_phase1_full_stop_probe_nonce_decide(
	uint64 active_nonce, uint64 stopped_nonce, uint64 incoming_nonce)
{
	if (active_nonce == 0 || active_nonce == UINT64_MAX
		|| stopped_nonce == UINT64_MAX
		|| incoming_nonce == 0 || incoming_nonce == UINT64_MAX)
		return CLUSTER_PHASE1_PROBE_NONCE_CONFLICT;
	if (incoming_nonce == active_nonce)
		return CLUSTER_PHASE1_PROBE_NONCE_STALE_ACTIVE;
	if (stopped_nonce == 0)
		return CLUSTER_PHASE1_PROBE_NONCE_ACCEPT_STOPPED;
	if (incoming_nonce == stopped_nonce)
		return CLUSTER_PHASE1_PROBE_NONCE_DUPLICATE_STOPPED;
	return CLUSTER_PHASE1_PROBE_NONCE_CONFLICT;
}
