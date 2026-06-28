/*-------------------------------------------------------------------------
 *
 * cluster_node_remove_policy.c
 *	  pgrac online node leave: fence + cluster-wide cleanup (spec-5.18) —
 *	  pure decision layer.
 *
 *	  Every function here is pure: it decides a phase-FSM transition, a
 *	  precheck verdict, the INV-LF2 ordering gate, a fence-arm-timeout
 *	  classify, a version-coherent contest classify, the INV-LF7 crash-
 *	  recovery resume phase, validates / authority-decides the three-phase
 *	  removal marker, or validates an IC payload — with no PostgreSQL runtime
 *	  dependency (no shmem, no locks, no voting-disk I/O, no ereport).  That
 *	  keeps the correctness-critical decisions (INV-LF2/LF3/LF7, P2-1)
 *	  exercisable directly by cluster_unit (test_cluster_node_remove).
 *
 *	  The runtime layer (cluster_node_remove.c) snapshots the live facts
 *	  (epoch, dead_generation, transaction state, on-disk markers) and feeds
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
 *	  src/backend/cluster/cluster_node_remove_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_node_remove.h"
#include "port/pg_crc32c.h"

/* 128-node bitmap width (same as the dead/ack bitmaps). */
#define CLUSTER_NODE_REMOVE_MAX_NODE_ID (CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES * 8)


/* ============================================================
 * Phase-FSM transition validity (D2 / U2)
 * ============================================================ */

/*
 * cluster_node_remove_phase_valid_transition -- is from->to a legal edge?
 *
 *	Forward path:
 *	  IDLE -> REQUESTED -> PRECHECK -> FENCE_ARMING -> SHRINK_COMMITTING ->
 *	  CLEANUP -> COMMITTED.
 *	CLEANUP <-> CLEANUP_BLOCKED is a resumable round-trip (post-SHRUNK
 *	  cleanup leftover/deadline; never escalates, fail-closed).
 *	Clean ABORTED only PRE-fence-commit (PRECHECK / FENCE_ARMING): nothing
 *	  committed yet, reverts to IDLE.
 *	ABORTED_ESCALATE only PRE-SHRUNK (REQUESTED..SHRINK_COMMITTING): removal
 *	  not committed -> hand to fail-stop 5.14.  Post-SHRUNK (CLEANUP /
 *	  CLEANUP_BLOCKED) is irreversible and MUST NOT escalate.
 *	Terminal phases reset to IDLE.  IDLE -> IDLE is a no-op self-loop.
 */
bool
cluster_node_remove_phase_valid_transition(ClusterRemovePhase from, ClusterRemovePhase to)
{
	/* escalate is reachable ONLY from a pre-SHRUNK active phase */
	if (to == CLUSTER_REMOVE_ABORTED_ESCALATE)
		return (from == CLUSTER_REMOVE_REQUESTED || from == CLUSTER_REMOVE_PRECHECK
				|| from == CLUSTER_REMOVE_FENCE_ARMING || from == CLUSTER_REMOVE_SHRINK_COMMITTING);

	/* clean abort is reachable ONLY pre-fence-commit */
	if (to == CLUSTER_REMOVE_ABORTED)
		return (from == CLUSTER_REMOVE_REQUESTED || from == CLUSTER_REMOVE_PRECHECK
				|| from == CLUSTER_REMOVE_FENCE_ARMING);

	switch (from) {
	case CLUSTER_REMOVE_IDLE:
		return (to == CLUSTER_REMOVE_REQUESTED || to == CLUSTER_REMOVE_IDLE);
	case CLUSTER_REMOVE_REQUESTED:
		return (to == CLUSTER_REMOVE_PRECHECK);
	case CLUSTER_REMOVE_PRECHECK:
		/* PRECHECK -> FENCE_ARMING (pass) — abort handled above */
		return (to == CLUSTER_REMOVE_FENCE_ARMING);
	case CLUSTER_REMOVE_FENCE_ARMING:
		/* fence majority-durable -> SHRINK_COMMITTING (INV-LF2) — abort/escalate above */
		return (to == CLUSTER_REMOVE_SHRINK_COMMITTING);
	case CLUSTER_REMOVE_SHRINK_COMMITTING:
		/* membership shrunk -> CLEANUP — escalate (pre-SHRUNK still) above */
		return (to == CLUSTER_REMOVE_CLEANUP);
	case CLUSTER_REMOVE_CLEANUP:
		return (to == CLUSTER_REMOVE_COMMITTED || to == CLUSTER_REMOVE_CLEANUP_BLOCKED);
	case CLUSTER_REMOVE_CLEANUP_BLOCKED:
		/* resumable: retry cleanup or, once verified, commit */
		return (to == CLUSTER_REMOVE_CLEANUP || to == CLUSTER_REMOVE_COMMITTED);
	case CLUSTER_REMOVE_COMMITTED:
	case CLUSTER_REMOVE_ABORTED:
	case CLUSTER_REMOVE_ABORTED_ESCALATE:
		/* terminal phases reset to IDLE for a clean slate */
		return (to == CLUSTER_REMOVE_IDLE);
	}
	return false; /* default-deny any unlisted edge */
}

const char *
cluster_node_remove_phase_str(int phase)
{
	switch (phase) {
	case CLUSTER_REMOVE_IDLE:
		return "idle";
	case CLUSTER_REMOVE_REQUESTED:
		return "requested";
	case CLUSTER_REMOVE_PRECHECK:
		return "precheck";
	case CLUSTER_REMOVE_FENCE_ARMING:
		return "fence_arming";
	case CLUSTER_REMOVE_SHRINK_COMMITTING:
		return "shrink_committing";
	case CLUSTER_REMOVE_CLEANUP:
		return "cleanup";
	case CLUSTER_REMOVE_CLEANUP_BLOCKED:
		return "cleanup_blocked";
	case CLUSTER_REMOVE_COMMITTED:
		return "committed";
	case CLUSTER_REMOVE_ABORTED:
		return "aborted";
	case CLUSTER_REMOVE_ABORTED_ESCALATE:
		return "aborted_escalate";
	default:
		return "(unknown)";
	}
}

const char *
cluster_node_remove_request_result_str(ClusterRemoveRequestResult r)
{
	switch (r) {
	case CLUSTER_REMOVE_REQ_ACCEPTED:
		return "accepted";
	case CLUSTER_REMOVE_REQ_FEATURE_DISABLED:
		return "rejected:feature_disabled";
	case CLUSTER_REMOVE_REQ_CANNOT_REMOVE_SELF:
		return "rejected:cannot_remove_self";
	case CLUSTER_REMOVE_REQ_NOT_DECLARED:
		return "rejected:not_declared";
	case CLUSTER_REMOVE_REQ_NOT_DRAINED:
		return "rejected:node_not_drained";
	case CLUSTER_REMOVE_REQ_NOT_IN_QUORUM:
		return "rejected:not_in_quorum";
	case CLUSTER_REMOVE_REQ_ALREADY_REMOVED:
		return "noop:already_removed";
	case CLUSTER_REMOVE_REQ_RESUME:
		return "resume:cleanup_pending";
	case CLUSTER_REMOVE_REQ_IN_PROGRESS:
		return "rejected:removal_in_progress";
	}
	return "rejected:unknown";
}


/* ============================================================
 * Precheck behaviour matrix (D16 / U10 / §3.2)
 * ============================================================ */

/*
 * cluster_node_remove_precheck -- map the live facts to a request result.
 *
 *	Order (fixed, §3.2):
 *	  feature off          -> feature_disabled
 *	  node == self         -> cannot_remove_self
 *	  node not declared    -> not_declared
 *	  marker == REMOVED    -> already_removed   (idempotent noop, fully complete)
 *	  marker == SHRUNK
 *	    OR cleanup_blocked -> resume            (committed but cleanup pending)
 *	  not drained          -> node_not_drained  (INV-LF4: no 5.13/5.14 precondition)
 *	  not in quorum        -> not_in_quorum
 *	  active drive running -> removal_in_progress
 *	  otherwise            -> accepted
 *
 *	The marker checks precede the drained check so an already-committed removal
 *	(SHRUNK/REMOVED) is recognised regardless of the live drained snapshot.
 */
ClusterRemoveRequestResult
cluster_node_remove_precheck(bool feature_enabled, bool is_self, bool is_declared, bool is_drained,
							 bool in_quorum, int marker_phase, bool cleanup_blocked,
							 bool drive_active)
{
	if (!feature_enabled)
		return CLUSTER_REMOVE_REQ_FEATURE_DISABLED;
	if (is_self)
		return CLUSTER_REMOVE_REQ_CANNOT_REMOVE_SELF;
	if (!is_declared)
		return CLUSTER_REMOVE_REQ_NOT_DECLARED;
	if (marker_phase == CLUSTER_REMOVAL_MARKER_REMOVED)
		return CLUSTER_REMOVE_REQ_ALREADY_REMOVED;
	if (marker_phase == CLUSTER_REMOVAL_MARKER_SHRUNK || cleanup_blocked)
		return CLUSTER_REMOVE_REQ_RESUME;
	if (!is_drained)
		return CLUSTER_REMOVE_REQ_NOT_DRAINED;
	if (!in_quorum)
		return CLUSTER_REMOVE_REQ_NOT_IN_QUORUM;
	if (drive_active)
		return CLUSTER_REMOVE_REQ_IN_PROGRESS;
	return CLUSTER_REMOVE_REQ_ACCEPTED;
}


/* ============================================================
 * INV-LF2 ordering gate + fence-arm timeout classify (U3 / U3b / P2-1)
 * ============================================================ */

bool
cluster_node_remove_may_enter_shrink(bool fence_armed)
{
	/* INV-LF2: SHRINK only after the fence marker is confirmed majority-durable. */
	return fence_armed;
}

bool
cluster_node_remove_fence_timeout_is_clean_abort(bool confirmed_no_majority)
{
	/*
	 * P2-1: a fence submit that timed out may already be majority-durable on
	 * disk (4.12 markers are monotone and cannot be cleanly retracted).  A clean
	 * ABORTED is allowed ONLY when a re-run of cluster_fence_authority_decide
	 * confirms there is NO majority.  Otherwise (majority formed, or the state is
	 * uncertain) we MUST NOT claim a clean abort — that would leave N a
	 * fenced-but-still-member half state; keep fence_armed and continue.
	 */
	return confirmed_no_majority;
}


/* ============================================================
 * Version-coherent contest classify (U4 / L235)
 * ============================================================ */

bool
cluster_node_remove_version_coherent(uint64 bound_epoch, uint64 current_epoch,
									 uint64 bound_dead_gen, uint64 current_dead_gen)
{
	return bound_epoch == current_epoch && bound_dead_gen == current_dead_gen;
}

ClusterRemovePhase
cluster_node_remove_classify_contest(bool membership_shrunk)
{
	/*
	 * A real external death/contest detected mid-removal:
	 *   pre-SHRUNK  (membership not committed) -> ABORTED_ESCALATE: removal is
	 *     not committed, N is still an ordinary member -> hand to fail-stop 5.14.
	 *   post-SHRUNK (committed + fenced, irreversible) -> CLEANUP_BLOCKED: never
	 *     escalate (N is already masked out of effective_dead, so fail-stop will
	 *     not re-clean it); resume cleanup instead, fail-closed.
	 */
	return membership_shrunk ? CLUSTER_REMOVE_CLEANUP_BLOCKED : CLUSTER_REMOVE_ABORTED_ESCALATE;
}


/* ============================================================
 * INV-LF7 crash-recovery matrix (U11 / §2.5 / §3.0)
 * ============================================================ */

/*
 * cluster_node_remove_recover_phase -- decide the resume phase from the durable
 * (fence-says-N-fenced, removal-marker-phase) pair recovered at startup.
 *
 *	The whole point of INV-LF2 (fence before shrink) is that a SHRUNK/REMOVED
 *	marker can NEVER coexist with an un-fenced node.  So fence=no + rm>=SHRUNK is
 *	impossible on a sound system; we flag it corruption and the caller fail-closes
 *	(FATAL, refuse to serve) rather than risk treating a removed-not-fenced node
 *	as committed (it could write shared storage).
 */
ClusterRemovePhase
cluster_node_remove_recover_phase(bool fence_says_fenced, int marker_phase, bool *is_corruption)
{
	*is_corruption = false;

	if (!fence_says_fenced) {
		/* SHRUNK/REMOVED durable without a durable fence is impossible (INV-LF2). */
		if (marker_phase == CLUSTER_REMOVAL_MARKER_SHRUNK
			|| marker_phase == CLUSTER_REMOVAL_MARKER_REMOVED)
			*is_corruption = true;
		/* none / REMOVING (pre-commit, not a trust source): nothing committed, no
		 * fence armed -> N is still an ordinary member -> abandon, no side effect. */
		return CLUSTER_REMOVE_IDLE;
	}

	/* fence is durable (N is fenced): drive the removal to completion. */
	switch (marker_phase) {
	case 0: /* no removal marker yet */
	case CLUSTER_REMOVAL_MARKER_REMOVING:
		/* fenced but membership not committed -> finish the shrink+cleanup */
		return CLUSTER_REMOVE_SHRINK_COMMITTING;
	case CLUSTER_REMOVAL_MARKER_SHRUNK:
		/* membership shrunk, cleanup pending -> resume cleanup (never COMMITTED yet) */
		return CLUSTER_REMOVE_CLEANUP;
	case CLUSTER_REMOVAL_MARKER_REMOVED:
		return CLUSTER_REMOVE_COMMITTED;
	default:
		*is_corruption = true; /* unknown marker phase */
		return CLUSTER_REMOVE_IDLE;
	}
}


/* ============================================================
 * Three-phase removal-marker structural validation + authority (§2.5 / U13)
 * ============================================================ */

/* CRC32C over [magic .. removal_event_id] — everything before the trailing crc. */
static pg_crc32c
removal_marker_crc(const ClusterRemovalMarker *m)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, m, offsetof(ClusterRemovalMarker, crc32c));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_removal_marker_compute_crc(ClusterRemovalMarker *m)
{
	m->crc32c = removal_marker_crc(m);
}

/*
 * cluster_removal_marker_struct_valid -- magic / version / CRC / phase / identity.
 *
 *	expected_removed_node < 0 skips the identity check (used by authority_decide,
 *	which validates structure only).  Phase must be one of the three known phases.
 */
bool
cluster_removal_marker_struct_valid(const ClusterRemovalMarker *m, int32 expected_removed_node)
{
	if (m->magic != CLUSTER_REMOVAL_MARKER_MAGIC)
		return false;
	if (m->version == 0 || m->version > CLUSTER_REMOVAL_MARKER_VERSION)
		return false;
	if (m->phase != CLUSTER_REMOVAL_MARKER_REMOVING && m->phase != CLUSTER_REMOVAL_MARKER_SHRUNK
		&& m->phase != CLUSTER_REMOVAL_MARKER_REMOVED)
		return false;
	if (m->crc32c != removal_marker_crc(m))
		return false;
	if (m->removed_node_id < 0 || m->removed_node_id >= CLUSTER_NODE_REMOVE_MAX_NODE_ID)
		return false;
	if (expected_removed_node >= 0 && m->removed_node_id != expected_removed_node)
		return false;
	return true;
}

/* tuple equality for the majority authority (excludes the derived crc + padding). */
static bool
removal_marker_tuple_equal(const ClusterRemovalMarker *a, const ClusterRemovalMarker *b)
{
	return a->magic == b->magic && a->version == b->version && a->phase == b->phase
		   && a->removed_node_id == b->removed_node_id && a->remove_epoch == b->remove_epoch
		   && a->removed_incarnation == b->removed_incarnation
		   && a->removal_event_id == b->removal_event_id;
}

/*
 * cluster_removal_marker_pack / _unpack -- write/read the marker at the fixed
 * _reserved1[64..] offset.  pack leaves the fence-marker region [0..64)
 * untouched.  unpack returns false + zeroed *out on a magic miss.
 */
void
cluster_removal_marker_pack(uint8 *reserved1, const ClusterRemovalMarker *m)
{
	memcpy(reserved1 + CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET, m, sizeof(ClusterRemovalMarker));
}

bool
cluster_removal_marker_unpack(const uint8 *reserved1, ClusterRemovalMarker *out)
{
	memcpy(out, reserved1 + CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET, sizeof(ClusterRemovalMarker));
	if (out->magic != CLUSTER_REMOVAL_MARKER_MAGIC) {
		memset(out, 0, sizeof(ClusterRemovalMarker));
		return false;
	}
	return true;
}

/*
 * cluster_removal_marker_preserve_per_disk -- carry THIS disk's own prior marker
 * forward (R13 anti-amplification: a 1-of-N minority stays 1-of-N; never copy a
 * marker from another disk).  If the prior slot has no marker, leave the new
 * region as-is (the caller zeroed it).
 */
void
cluster_removal_marker_preserve_per_disk(uint8 *new_reserved1,
										 const uint8 *prior_reserved1_same_disk)
{
	ClusterRemovalMarker m;

	if (cluster_removal_marker_unpack(prior_reserved1_same_disk, &m))
		cluster_removal_marker_pack(new_reserved1, &m);
	/* else: leave new_reserved1[64..] untouched (caller zeroed it). */
}

/*
 * cluster_removal_marker_authority_decide -- majority over per-disk markers.
 *
 *	A tuple is authority only when an IDENTICAL, struct-valid tuple appears on
 *	>= majority distinct disks (P0a anti-amplification — a torn/minority marker
 *	never becomes authority).  Among qualifying tuples the most-advanced removal
 *	wins: highest remove_epoch, then highest phase (REMOVED > SHRUNK > REMOVING).
 *	Returns false (no authority) if no tuple reaches majority.
 */
bool
cluster_removal_marker_authority_decide(const ClusterRemovalMarker *disk_markers,
										const bool *disk_has_marker, int n_disks,
										ClusterRemovalMarker *out)
{
	int majority = n_disks / 2 + 1;
	bool found = false;
	int i;

	for (i = 0; i < n_disks; i++) {
		int count = 0;
		int j;

		if (!disk_has_marker[i] || !cluster_removal_marker_struct_valid(&disk_markers[i], -1))
			continue;

		for (j = 0; j < n_disks; j++) {
			if (!disk_has_marker[j] || !cluster_removal_marker_struct_valid(&disk_markers[j], -1))
				continue;
			if (removal_marker_tuple_equal(&disk_markers[i], &disk_markers[j]))
				count++;
		}

		if (count < majority)
			continue;

		/* qualifying tuple; keep the most-advanced (epoch, then phase) */
		if (!found || disk_markers[i].remove_epoch > out->remove_epoch
			|| (disk_markers[i].remove_epoch == out->remove_epoch
				&& disk_markers[i].phase > out->phase)) {
			*out = disk_markers[i];
			found = true;
		}
	}
	return found;
}


/* ============================================================
 * IC payload integrity (D10 / U9 / rule 15) — pure CRC + validation.
 * ============================================================ */

static pg_crc32c
remove_announce_crc(const ClusterNodeRemoveAnnouncePayload *p)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, offsetof(ClusterNodeRemoveAnnouncePayload, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_node_remove_announce_compute_crc(ClusterNodeRemoveAnnouncePayload *p)
{
	p->crc = remove_announce_crc(p);
}

bool
cluster_node_remove_announce_payload_valid(const ClusterNodeRemoveAnnouncePayload *p)
{
	if (p->magic != CLUSTER_NODE_REMOVE_IC_MAGIC)
		return false;
	if (p->version != CLUSTER_NODE_REMOVE_IC_VERSION)
		return false;
	if (p->crc != remove_announce_crc(p))
		return false;
	if (p->target_node_id < 0 || p->target_node_id >= CLUSTER_NODE_REMOVE_MAX_NODE_ID)
		return false;
	if (p->coordinator_node_id < 0 || p->coordinator_node_id >= CLUSTER_NODE_REMOVE_MAX_NODE_ID)
		return false;
	return true;
}

static pg_crc32c
remove_ack_crc(const ClusterNodeRemoveCleanupAckPayload *p)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, offsetof(ClusterNodeRemoveCleanupAckPayload, crc));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_node_remove_ack_compute_crc(ClusterNodeRemoveCleanupAckPayload *p)
{
	p->crc = remove_ack_crc(p);
}

bool
cluster_node_remove_ack_payload_valid(const ClusterNodeRemoveCleanupAckPayload *p)
{
	if (p->magic != CLUSTER_NODE_REMOVE_IC_MAGIC)
		return false;
	if (p->version != CLUSTER_NODE_REMOVE_IC_VERSION)
		return false;
	if (p->crc != remove_ack_crc(p))
		return false;
	if (p->survivor_node_id < 0 || p->survivor_node_id >= CLUSTER_NODE_REMOVE_MAX_NODE_ID)
		return false;
	if (p->target_node_id < 0 || p->target_node_id >= CLUSTER_NODE_REMOVE_MAX_NODE_ID)
		return false;
	return true;
}
