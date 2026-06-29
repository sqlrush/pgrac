/*-------------------------------------------------------------------------
 *
 * cluster_membership.c
 *	  pgrac online-join membership decision SSOT (spec-5.15).
 *
 *	  Foundation slice: the incarnation monotonic guard (INV-J1/INV-J7) and the
 *	  membership-state decision table (INV-J8).  See cluster_membership.h for the
 *	  full contract and the DESIGN-AHEAD integration note (the backing table
 *	  moves into ClusterReconfigState under the reconfig LWLock at D4/D5).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.15-online-declared-node-join-membership.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_membership.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_in_quorum (quorum sub-gate) */

/*
 * Backing store.
 *
 * At integration the live table is &ReconfigShmem->membership in shared memory
 * (attached via cluster_membership_attach() from cluster_reconfig_shmem_init).
 * Before attach -- and in the pure cluster_unit layer -- a process-local default
 * is used.  All MUTATORS run under the reconfig LWLock held by the caller (the
 * coordinator tick / two-phase commit path, INV-J8); single-scalar reads are
 * naturally-atomic decision snapshots (same discipline as the spec-5.13
 * clean_departed_bitmap read in cluster_reconfig_lmon_tick).
 */
static ClusterMembershipTable LocalMembershipTable;
static ClusterMembershipTable *MembershipTable = &LocalMembershipTable;

/*
 * cluster_membership_attach
 *		Point the accessors at the shared backing table (cluster_reconfig_shmem_
 *		init passes &ReconfigShmem->membership).  NULL resets to the process-local
 *		default (pure unit-test layer).  Postmaster-once; no lock needed (runs at
 *		shmem init before any backend forks).
 */
void
cluster_membership_attach(ClusterMembershipTable *table)
{
	MembershipTable = (table != NULL) ? table : &LocalMembershipTable;
}

/* node_id is a declared-topology index in [0, CLUSTER_MAX_NODES) */
static inline bool
node_id_in_range(int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES;
}

/*
 * cluster_membership_vet_joiner
 *		Pure monotonic-incarnation + range gate.  No mutation.
 *
 * Foundation slice (rule 8.A correctness core):
 *	 - an out-of-range node_id is never admitted (fail-closed defense);
 *	 - INV-J1: a presented incarnation at or below the admitted floor is a STALE
 *	   rejoin and is rejected fail-closed.  The compare is uint64 end to end, so
 *	   incarnations above 2^32 are never truncated (P1a).
 *
 * DESIGN-AHEAD (spec-5.15 D5, gated on 5.13 ship): the quorum sub-gate
 * (-> REJECT_QUORUM via cluster_qvotec_in_quorum) and the readiness sub-gate
 * (-> REJECT_NOT_READY when the joiner is not crash-recovered/caught-up) are
 * wired at integration and MUST be in place before this verdict is consumed by
 * a live join (see coding plan / D0 re-ground checklist).  slot_generation is
 * an input to those sub-gates.
 */
ClusterJoinVerdict
cluster_membership_vet_joiner(int32 node_id, uint64 presented_incarnation, uint64 slot_generation)
{
	uint64 floor;

	/* Range defense (fail-closed): an out-of-range node_id is never admitted. */
	if (!node_id_in_range(node_id))
		return CLUSTER_JOIN_REJECT_NOT_READY;

	/*
	 * spec-5.18 INV-LF1 (rule 8.A): a permanently-removed node is fenced and is
	 * NEVER passively re-admitted — not even with a fresh incarnation.  This is a
	 * DEFINITIVE reject (checked before the transient quorum/readiness holds, so a
	 * removed node is never told to retry): it can only return via an operator
	 * un-fence (external plane, not implemented here) + a fresh join.  Distinct
	 * code (53R64) from the floor-based stale reject.
	 */
	if (MembershipTable->membership_state[node_id] == CLUSTER_MEMBER_REMOVED)
		return CLUSTER_JOIN_REJECT_REMOVED_FENCED;

	/*
	 * Readiness sub-gate (INV-J5 / Q10): a joiner that has not published a valid
	 * voting slot (generation 0) has not completed its own startup / crash
	 * recovery and is not yet ready to be admitted -- hold, never half-admit.
	 * The authoritative recovery-complete check is enforced joiner-side (D5,
	 * before it bumps its incarnation and writes a fresh slot); this is the
	 * coordinator-side backstop so vet stays fail-closed on any path.
	 */
	if (slot_generation == 0)
		return CLUSTER_JOIN_REJECT_NOT_READY;

	/*
	 * Quorum sub-gate (INV-J2): a node not itself in quorum cannot coordinate an
	 * admission -- hold (transient; the joiner retries).  The lmon tick already
	 * gates on quorum before vetting, but keeping the check here makes vet
	 * self-contained and fail-closed regardless of caller.
	 */
	if (!cluster_qvotec_in_quorum())
		return CLUSTER_JOIN_REJECT_QUORUM;

	/*
	 * INV-J1 monotonic guard (rule 8.A correctness core): a presented incarnation
	 * at or below the admitted floor is a STALE rejoin and MUST be rejected
	 * fail-closed -- never admitted.  The compare is uint64 end to end (P1a), so
	 * incarnations above 2^32 are never truncated.  This is the definitive,
	 * locally-certain reject (-> 53R61 FATAL); the holds above are transient.
	 */
	floor = MembershipTable->last_admitted_incarnation[node_id];
	if (presented_incarnation <= floor)
		return CLUSTER_JOIN_REJECT_STALE_INCARNATION;

	return CLUSTER_JOIN_ACCEPT;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	if (!node_id_in_range(node_id))
		return 0;
	return MembershipTable->last_admitted_incarnation[node_id];
}

/*
 * Raise the admitted floor for node_id.  Coordinator-only; mutates under the
 * reconfig LWLock at integration.  The floor is non-decreasing (INV-J7): a
 * lower incarnation is ignored, never lowering the bar against a stale rejoin.
 */
void
cluster_membership_record_admitted(int32 node_id, uint64 incarnation)
{
	if (!node_id_in_range(node_id))
		return;
	if (incarnation > MembershipTable->last_admitted_incarnation[node_id])
		MembershipTable->last_admitted_incarnation[node_id] = incarnation;
}

ClusterMembershipState
cluster_membership_get_state(int32 node_id)
{
	if (!node_id_in_range(node_id))
		return CLUSTER_MEMBER_ABSENT;
	return (ClusterMembershipState)MembershipTable->membership_state[node_id];
}

/* Coordinator-only; mutates under the reconfig LWLock at integration. */
void
cluster_membership_set_state(int32 node_id, ClusterMembershipState state)
{
	if (!node_id_in_range(node_id))
		return;
	MembershipTable->membership_state[node_id] = (uint8)state;
}

/*
 * Decision SSOT (INV-J8): a node counts toward survivor/quorum/coordinator
 * decisions iff its membership_state is MEMBER -- never raw CSSD ALIVE.
 */
bool
cluster_membership_is_member(int32 node_id)
{
	if (!node_id_in_range(node_id))
		return false;
	return MembershipTable->membership_state[node_id] == CLUSTER_MEMBER_MEMBER;
}

/*
 * spec-5.18 D4 — member-set denominator: count declared nodes in MEMBER state.
 * This is the denominator that shrinks on permanent removal (NOT the disk-quorum
 * denominator, §0.3).  Caller holds the reconfig LWLock for a coherent snapshot
 * (single-byte reads are naturally atomic; the count is advisory observability).
 */
int
cluster_membership_member_count(void)
{
	int count = 0;
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (MembershipTable->membership_state[i] == CLUSTER_MEMBER_MEMBER)
			count++;
	return count;
}

/*
 * spec-5.18 D4 — permanent removal (shrink dual of record_admitted).  Pin the
 * admitted-incarnation floor at last_incarnation (monotone; a future re-admit must
 * present strictly greater) then mark the node REMOVED (terminal).  Idempotent —
 * the startup rebuild re-applies it.  Coordinator-only; mutates under the reconfig
 * LWLock held by the caller.
 */
void
cluster_membership_shrink_to_removed(int32 node_id, uint64 last_incarnation)
{
	if (!node_id_in_range(node_id))
		return;
	/* raise the floor first (monotone) so re-admit must exceed the removed incarnation */
	if (last_incarnation > MembershipTable->last_admitted_incarnation[node_id])
		MembershipTable->last_admitted_incarnation[node_id] = last_incarnation;
	MembershipTable->membership_state[node_id] = (uint8)CLUSTER_MEMBER_REMOVED;
}


/* ============================================================
 * §2.6 join-commit marker — pure integrity layer (rule 15).
 * ============================================================
 */

/* Set m->crc32c = CRC32C over [magic .. commit_nonce]. */
void
cluster_join_marker_compute_crc(ClusterJoinCommitMarker *m)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, m, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	m->crc32c = (uint32)crc;
}

/*
 * Structural validity: magic + version + node_id range + identity (== the
 * expected joiner) + CRC.  FAIL-CLOSED: any mismatch -> false (a stale / torn /
 * misrouted slot is never trusted).  Does NOT consult phase or epoch.
 */
bool
cluster_join_marker_struct_valid(const ClusterJoinCommitMarker *m, int32 expected_node)
{
	pg_crc32c crc;

	if (m == NULL)
		return false;
	if (m->magic != CLUSTER_JCMK_MAGIC || m->version != CLUSTER_JCMK_VERSION)
		return false;
	if (!node_id_in_range(m->node_id) || m->node_id != expected_node)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, m, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == m->crc32c;
}

/*
 * Is this marker a seed basis?  struct-valid AND phase == COMMITTED (P1#1-r4:
 * NEVER an epoch comparison — a restart resets the volatile epoch to 0, which
 * would wrongly discard every admitted@epoch>0 marker and re-open the gate).
 */
bool
cluster_join_marker_is_committed_basis(const ClusterJoinCommitMarker *m, int32 expected_node)
{
	return cluster_join_marker_struct_valid(m, expected_node)
		   && m->phase == CLUSTER_JCMK_PHASE_COMMITTED;
}

/*
 * INV-J13 (Hardening v1.1): same commit attempt?  Compares the whole identity
 * range [0 .. offsetof(crc32c)) -- magic/version/node_id/phase/_pad/generation/
 * admitted_incarnation/admitted_epoch/supersedes_leave_epoch/commit_nonce.  The
 * markers are memset-0 before fill so _pad never differs.  Used by the self-
 * admit and the startup-seed majority judgements so that two minority writes
 * from DIFFERENT commit attempts (different coordinator / epoch / nonce) cannot
 * be counted together as a false majority (P1-3).
 */
bool
cluster_join_marker_same_commit(const ClusterJoinCommitMarker *a, const ClusterJoinCommitMarker *b)
{
	if (a == NULL || b == NULL)
		return false;
	return memcmp(a, b, offsetof(ClusterJoinCommitMarker, crc32c)) == 0;
}

/*
 * INV-J13 majority selector (shared by self-admit, startup-seed and qvotec
 * peer-observe).  `markers` must already be committed-basis; O(n^2) over n disks
 * (n <= CLUSTER_MAX_VOTING_DISKS, small).  Returns the index of the first marker
 * that is same_commit with >= `majority` of the array (i.e. a single commit
 * attempt that actually reached a disk majority), or -1.  A set of distinct-
 * attempt minority markers therefore selects NOTHING — they never aggregate
 * into a false majority (reviewer P1 #2 / P1-3).
 */
int
cluster_join_marker_select_majority(const ClusterJoinCommitMarker *markers, int n, uint32 majority,
									uint32 *out_agree)
{
	int a, b;

	if (out_agree != NULL)
		*out_agree = 0;
	if (markers == NULL || n <= 0)
		return -1;

	for (a = 0; a < n; a++) {
		uint32 same = 0;

		for (b = 0; b < n; b++)
			if (cluster_join_marker_same_commit(&markers[a], &markers[b]))
				same++;
		if (same >= majority) {
			if (out_agree != NULL)
				*out_agree = same;
			return a;
		}
	}
	return -1;
}

/*
 * Apply one durable marker to the admitted floor (INV-J7).  Only a committed
 * basis raises the floor; record_admitted is monotonic so re-applying / lower
 * markers are no-ops.  Returns true iff applied.  Pure w.r.t. the attached table;
 * the runtime seed calls it per region-3 slot (after the slot-index cross-check),
 * the unit tests call it directly.
 */
bool
cluster_membership_seed_apply_marker(const ClusterJoinCommitMarker *m)
{
	if (m == NULL || !cluster_join_marker_is_committed_basis(m, m->node_id))
		return false;
	cluster_membership_record_admitted(m->node_id, m->admitted_incarnation);
	return true;
}
