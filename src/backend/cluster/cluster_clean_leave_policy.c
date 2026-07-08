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
 * infers "my clean-leave committed" from the membership epoch advancing past
 * its bound baseline (the coordinator publishes the CLEAN_LEAVE event into ITS
 * own reconfig state, but the epoch piggybacks to the leaver).  Latching that
 * inference suppresses the barrier-deadline escalation, so a FALSE latch hangs
 * the leaver in BARRIER_WAIT forever.
 *
 * spec-2.29a r2 P2-1: the leaver's own-commit inference needs BOTH a bitmap
 * check AND the scalar dead_generation.  The others-dead bitmap alone is not
 * monotone: a third-party node that false-fail-stopped (bumping the epoch)
 * then recovered leaves the CSSD hysteresis DEAD→ALIVE, so the others-dead
 * bitmap returns to its bound value while the scalar dead_generation (which
 * only advances) does not.  If the leaver's first epoch>baseline observation
 * lands after that rebound, a bitmap-only check would mis-latch a leave the
 * survivor coordinator actually refused.  The scalar conjunct closes it — and
 * it does NOT reintroduce the ②b false positive, because the leaving node's
 * OWN alive→DEAD transition never bumps ITS OWN dead_generation (a node does
 * not observe itself dead), so on the leaver side dead_gen stays at baseline
 * through its own drain.  (The survivor-side coherence sites keep the
 * others-dead bitmap: a survivor DOES observe the leaver's DEAD and would be
 * falsely escalated by the scalar there.)
 */
bool
cluster_clean_leave_own_commit_latched(bool epoch_advanced, bool others_dead_unchanged,
									   bool dead_gen_unchanged)
{
	return epoch_advanced && others_dead_unchanged && dead_gen_unchanged;
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
	return true;
}
