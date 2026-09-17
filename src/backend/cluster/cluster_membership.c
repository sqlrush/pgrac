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

#include "cluster/cluster_guc.h" /* cluster_node_id (spec-5.22e D5-8) */
#include "cluster/cluster_membership.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_qvotec.h"		  /* cluster_qvotec_in_quorum (quorum sub-gate) */
#include "cluster/cluster_undo_horizon.h" /* note_self_member (spec-5.22e D5-8) */

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

static inline uint32
jcmk_get_le32(const uint8 *p)
{
	return (uint32)p[0] | ((uint32)p[1] << 8) | ((uint32)p[2] << 16) | ((uint32)p[3] << 24);
}

static inline uint64
jcmk_get_le64(const uint8 *p)
{
	return (uint64)jcmk_get_le32(p) | ((uint64)jcmk_get_le32(p + 4) << 32);
}

static inline void
jcmk_put_le32(uint8 *p, uint32 v)
{
	p[0] = (uint8)v;
	p[1] = (uint8)(v >> 8);
	p[2] = (uint8)(v >> 16);
	p[3] = (uint8)(v >> 24);
}

static inline void
jcmk_put_le64(uint8 *p, uint64 v)
{
	jcmk_put_le32(p, (uint32)v);
	jcmk_put_le32(p + 4, (uint32)(v >> 32));
}

static bool
replacement_marker_v3_phase_valid(uint8 phase, uint32 ready_state_generation)
{
	switch (phase) {
	case CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE:
	case CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED:
	case CLUSTER_JCMK_REPLACEMENT_PHASE_ABORTED_CLOSED:
		return ready_state_generation == 0;
	case CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED:
		return ready_state_generation != 0;
	default:
		return false;
	}
}

static uint32
replacement_marker_v3_crc(const uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 92);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

bool
cluster_replacement_marker_v3_encode(const ClusterReplacementCommitMarkerV3 *m,
									 uint8 out[CLUSTER_JCMK_REPLACEMENT_BYTES])
{
	uint8 image[CLUSTER_JCMK_REPLACEMENT_BYTES];

	if (m == NULL || out == NULL)
		return false;
	if (m->magic != CLUSTER_JCMK_MAGIC || m->version != CLUSTER_JCMK_REPLACEMENT_VERSION
		|| !node_id_in_range(m->target_node_id) || m->generation == 0 || m->reserved0[0] != 0
		|| m->reserved0[1] != 0 || m->reserved0[2] != 0
		|| !replacement_marker_v3_phase_valid(m->phase, m->ready_state_generation))
		return false;

	memset(image, 0, sizeof(image));
	jcmk_put_le32(image + 0, m->magic);
	jcmk_put_le32(image + 4, m->version);
	jcmk_put_le32(image + 8, (uint32)m->target_node_id);
	image[12] = m->phase;
	jcmk_put_le64(image + 16, m->generation);
	jcmk_put_le64(image + 24, m->old_admitted_incarnation);
	jcmk_put_le64(image + 32, m->fresh_incarnation);
	jcmk_put_le64(image + 40, m->baseline_epoch);
	jcmk_put_le64(image + 48, m->reserved_or_committed_epoch);
	jcmk_put_le64(image + 56, m->request_nonce);
	memcpy(image + 64, m->expected_purge_survivors, 16);
	jcmk_put_le64(image + 80, m->grammar_fingerprint);
	jcmk_put_le32(image + 88, m->ready_state_generation);
	jcmk_put_le32(image + 92, replacement_marker_v3_crc(image));
	memcpy(out, image, sizeof(image));
	return true;
}

bool
cluster_replacement_marker_v3_decode(const uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES],
									 int32 expected_target_node,
									 ClusterReplacementCommitMarkerV3 *out)
{
	ClusterReplacementCommitMarkerV3 decoded;

	if (bytes == NULL || out == NULL || !node_id_in_range(expected_target_node))
		return false;
	if (jcmk_get_le32(bytes + 0) != CLUSTER_JCMK_MAGIC
		|| jcmk_get_le32(bytes + 4) != CLUSTER_JCMK_REPLACEMENT_VERSION
		|| (int32)jcmk_get_le32(bytes + 8) != expected_target_node || bytes[13] != 0
		|| bytes[14] != 0 || bytes[15] != 0 || jcmk_get_le64(bytes + 16) == 0
		|| !replacement_marker_v3_phase_valid(bytes[12], jcmk_get_le32(bytes + 88))
		|| replacement_marker_v3_crc(bytes) != jcmk_get_le32(bytes + 92))
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.magic = CLUSTER_JCMK_MAGIC;
	decoded.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	decoded.target_node_id = expected_target_node;
	decoded.phase = bytes[12];
	decoded.generation = jcmk_get_le64(bytes + 16);
	decoded.old_admitted_incarnation = jcmk_get_le64(bytes + 24);
	decoded.fresh_incarnation = jcmk_get_le64(bytes + 32);
	decoded.baseline_epoch = jcmk_get_le64(bytes + 40);
	decoded.reserved_or_committed_epoch = jcmk_get_le64(bytes + 48);
	decoded.request_nonce = jcmk_get_le64(bytes + 56);
	memcpy(decoded.expected_purge_survivors, bytes + 64, 16);
	decoded.grammar_fingerprint = jcmk_get_le64(bytes + 80);
	decoded.ready_state_generation = jcmk_get_le32(bytes + 88);
	decoded.crc32c = jcmk_get_le32(bytes + 92);
	*out = decoded;
	return true;
}

/*
 * Compare decoded markers by the exact canonical 96-byte image.  Re-encoding
 * keeps host padding and the caller-provided crc32c field outside identity.
 */
bool
cluster_replacement_marker_v3_same_image(const ClusterReplacementCommitMarkerV3 *a,
										 const ClusterReplacementCommitMarkerV3 *b)
{
	uint8 a_image[CLUSTER_JCMK_REPLACEMENT_BYTES];
	uint8 b_image[CLUSTER_JCMK_REPLACEMENT_BYTES];

	if (!cluster_replacement_marker_v3_encode(a, a_image)
		|| !cluster_replacement_marker_v3_encode(b, b_image))
		return false;
	return memcmp(a_image, b_image, CLUSTER_JCMK_REPLACEMENT_BYTES) == 0;
}

/*
 * Select one exact valid v3 disk image.  Counting is image-first: neither a
 * higher generation nor fields shared across different episodes can combine
 * minority images into a false majority.  Outputs are committed only after a
 * winning group is found.
 */
int
cluster_replacement_marker_v3_select_majority(const uint8 images[][CLUSTER_JCMK_REPLACEMENT_BYTES],
											  int n, uint32 majority, int32 expected_target_node,
											  ClusterReplacementCommitMarkerV3 *out_marker,
											  uint32 *out_agree)
{
	int a;
	int b;

	if (images == NULL || n <= 0 || majority <= (uint32)n / 2 || majority > (uint32)n
		|| !node_id_in_range(expected_target_node))
		return -1;

	for (a = 0; a < n; a++) {
		ClusterReplacementCommitMarkerV3 candidate;
		uint32 same = 0;

		if (!cluster_replacement_marker_v3_decode(images[a], expected_target_node, &candidate))
			continue;
		for (b = 0; b < n; b++) {
			if (memcmp(images[a], images[b], CLUSTER_JCMK_REPLACEMENT_BYTES) == 0)
				same++;
		}
		if (same >= majority) {
			if (out_marker != NULL)
				*out_marker = candidate;
			if (out_agree != NULL)
				*out_agree = same;
			return a;
		}
	}
	return -1;
}

/*
 * Every valid replacement phase supplies an incarnation floor, but the phase
 * determines which incarnation is authoritative.  PREPARE/ABORTED_CLOSED keep
 * the old admitted floor; post-COMMIT phases pin the fresh incarnation.
 */
bool
cluster_replacement_marker_v3_floor_basis(const uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES],
										  int32 expected_target_node, uint64 *out_incarnation_floor)
{
	ClusterReplacementCommitMarkerV3 marker;
	uint64 floor;

	if (out_incarnation_floor == NULL
		|| !cluster_replacement_marker_v3_decode(bytes, expected_target_node, &marker))
		return false;

	switch (marker.phase) {
	case CLUSTER_JCMK_REPLACEMENT_PHASE_PREPARE:
	case CLUSTER_JCMK_REPLACEMENT_PHASE_ABORTED_CLOSED:
		floor = marker.old_admitted_incarnation;
		break;
	case CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED:
	case CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED:
		floor = marker.fresh_incarnation;
		break;
	default:
		return false;
	}
	if (floor == 0)
		return false;
	*out_incarnation_floor = floor;
	return true;
}

bool
cluster_replacement_marker_v3_is_committed_closed_basis(
	const uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES], int32 expected_target_node,
	uint64 *out_incarnation_floor)
{
	ClusterReplacementCommitMarkerV3 marker;

	if (out_incarnation_floor == NULL
		|| !cluster_replacement_marker_v3_decode(bytes, expected_target_node, &marker)
		|| marker.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED
		|| marker.fresh_incarnation == 0)
		return false;
	*out_incarnation_floor = marker.fresh_incarnation;
	return true;
}

bool
cluster_replacement_marker_v3_is_admitted_basis(const uint8 bytes[CLUSTER_JCMK_REPLACEMENT_BYTES],
												int32 expected_target_node,
												uint64 *out_incarnation_floor,
												uint32 *out_ready_state_generation)
{
	ClusterReplacementCommitMarkerV3 marker;

	if (out_incarnation_floor == NULL || out_ready_state_generation == NULL
		|| !cluster_replacement_marker_v3_decode(bytes, expected_target_node, &marker)
		|| marker.phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED || marker.fresh_incarnation == 0)
		return false;
	*out_incarnation_floor = marker.fresh_incarnation;
	*out_ready_state_generation = marker.ready_state_generation;
	return true;
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
	if (incarnation > MembershipTable->last_admitted_incarnation[node_id]) {
		cluster_write_fence_authority_cache_invalidate();
		MembershipTable->last_admitted_incarnation[node_id] = incarnation;
	}
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
	ClusterMembershipState prev;

	if (!node_id_in_range(node_id))
		return;
	prev = (ClusterMembershipState)MembershipTable->membership_state[node_id];
	if (prev != state)
		cluster_write_fence_authority_cache_invalidate();
	MembershipTable->membership_state[node_id] = (uint8)state;

	/*
	 * spec-5.22e D5-8: capture the exact epoch at which THIS node becomes
	 * MEMBER (bootstrap formation, online join and rejoin all funnel through
	 * this choke).  The read-admission gate compares snapshot->read_epoch
	 * against it so pre-join snapshots can never consume foreign undo, and
	 * a late capture would mis-refuse legitimate post-admission snapshots
	 * after an unrelated epoch bump.
	 */
	if (node_id == cluster_node_id && state == CLUSTER_MEMBER_MEMBER
		&& prev != CLUSTER_MEMBER_MEMBER)
		cluster_undo_horizon_note_self_member();
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
	if (last_incarnation > MembershipTable->last_admitted_incarnation[node_id]
		|| MembershipTable->membership_state[node_id] != CLUSTER_MEMBER_REMOVED)
		cluster_write_fence_authority_cache_invalidate();
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
