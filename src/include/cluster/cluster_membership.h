/*-------------------------------------------------------------------------
 *
 * cluster_membership.h
 *	  pgrac online-join membership decision SSOT (spec-5.15).
 *
 *	  This module owns the two correctness primitives of online declared-node
 *	  join/rejoin:
 *
 *	    1. the monotonic-incarnation guard (cluster_membership_vet_joiner) that
 *	       fail-closed rejects a stale rejoin (a zombie presenting an incarnation
 *	       at or below the cluster's last-admitted floor), and
 *	    2. the per-node membership_state[] table that is the DECISION SSOT for
 *	       survivor / quorum / coordinator-selection / member decisions in ALL
 *	       directions (death/join/off) -- raw CSSD ALIVE only ever makes a node a
 *	       JOIN CANDIDATE, never a member (INV-J8).
 *
 *	  8.A correctness contract (spec-5.15 §3.0, membership safe-publication is
 *	  MVCC/visibility-class correctness and must not be forward-linked):
 *	   - INV-J1: presented incarnation (uint64) <= last_admitted_incarnation[N]
 *	     is a STALE rejoin and MUST be rejected fail-closed, never admitted.
 *	   - INV-J7: last_admitted_incarnation[] is a non-decreasing floor; it is
 *	     never lowered below a value that was previously admitted (otherwise a
 *	     restart/regression would re-open the gate to a stale incarnation).
 *	   - INV-J8: a node is counted in any survivor/quorum/coordinator decision
 *	     ONLY when membership_state == MEMBER.
 *
 *	  P1a (review r1): incarnation / generation are uint64 to MATCH the durable
 *	  on-disk ClusterVotingSlot ABI (cluster_qvotec.h: uint64 incarnation, uint64
 *	  generation, µs-clock seed).  uint32 would truncate the persistent field and
 *	  alias on wrap (rule 16 on-disk format).  The D6 SRF surfaces int8.
 *
 *	  DESIGN-AHEAD STATUS (spec-5.15 §0.0): spec-5.15 is FROZEN but its coding
 *	  hard-gate is "spec-5.13 ship".  This file is the dependency-free foundation
 *	  slice (the incarnation-monotonic vet + the membership-state accessors) and
 *	  is NOT wired into any production path yet.  At integration (D4/D5, after the
 *	  5.13/5.14 substrate ships) the backing table moves INTO ClusterReconfigState
 *	  (cluster_reconfig.h) under the reconfig LWLock, and vet gains its quorum
 *	  (REJECT_QUORUM) and readiness (REJECT_NOT_READY) sub-gates.  See the coding
 *	  plan / D0 re-ground checklist.
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
 *	  src/include/cluster/cluster_membership.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MEMBERSHIP_H
#define CLUSTER_MEMBERSHIP_H

#ifndef FRONTEND

#include "c.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "port/pg_crc32c.h"		  /* join-commit marker integrity */

/*
 * Verdict of cluster_membership_vet_joiner.  ACCEPT is the only value that
 * lets a join proceed; the three REJECT_* values are fail-closed holds.
 */
typedef enum ClusterJoinVerdict {
	CLUSTER_JOIN_ACCEPT = 0,
	CLUSTER_JOIN_REJECT_STALE_INCARNATION, /* presented <= last_admitted -> 53R61 */
	CLUSTER_JOIN_REJECT_QUORUM,			   /* joiner/cluster not in quorum -> hold */
	CLUSTER_JOIN_REJECT_NOT_READY,		   /* recovery/precondition unmet  -> hold */
	/*
	 * spec-5.18 D4 (INV-LF1): the node is permanently removed (membership_state
	 * == REMOVED) and fenced.  A definitive, non-transient reject regardless of
	 * the presented incarnation — it can only return via an operator un-fence
	 * (external plane) followed by a fresh join, NEVER by passive rejoin (-> 53R64
	 * FATAL).  Distinct from STALE_INCARNATION so diagnostics tell "removed" apart
	 * from "merely behind the floor".
	 */
	CLUSTER_JOIN_REJECT_REMOVED_FENCED
} ClusterJoinVerdict;

/*
 * Per-node membership state.  This enum (not raw CSSD state) is the decision
 * SSOT: only CLUSTER_MEMBER_MEMBER counts a node into survivor/quorum/
 * coordinator-selection decisions (INV-J8).
 */
typedef enum ClusterMembershipState {
	CLUSTER_MEMBER_ABSENT = 0,
	CLUSTER_MEMBER_DEAD,
	CLUSTER_MEMBER_JOINING,
	CLUSTER_MEMBER_MEMBER,
	CLUSTER_MEMBER_REJECTED,
	/*
	 * spec-5.18 D4 — terminal: permanently removed (decommissioned) + fenced.
	 * REMOVED is excluded from the member-set denominator (member_count) and is a
	 * definitive vet_joiner reject (REJECT_REMOVED_FENCED).  This is the shrink
	 * dual of 5.15's JOIN admission (MEMBER -> REMOVED, never the reverse without
	 * an operator un-fence).
	 */
	CLUSTER_MEMBER_REMOVED
} ClusterMembershipState;

/*
 * Backing store for the membership SSOT.
 *
 * At integration (spec-5.15 D2) this struct is EMBEDDED in ClusterReconfigState
 * (cluster_reconfig.h) so all backends share one membership view in shmem,
 * mutated under the reconfig LWLock.  cluster_membership.c reaches it through a
 * module-static pointer set by cluster_membership_attach() (called from
 * cluster_reconfig_shmem_init); until attached, a process-local default table is
 * used, which keeps the module pure-linkable for the cluster_unit layer (the vet
 * / state accessors carry no PG-backend dependency beyond the optional quorum
 * sub-gate).
 *
 *   last_admitted_incarnation[N] — INV-J1 monotonic floor (P1a uint64); a
 *     presented incarnation <= the floor is a stale rejoin, rejected fail-closed.
 *   membership_state[N]          — ClusterMembershipState; the decision SSOT
 *     (INV-J8): only MEMBER counts toward survivor/quorum/coordinator decisions.
 */
typedef struct ClusterMembershipTable {
	uint64 last_admitted_incarnation[CLUSTER_MAX_NODES];
	uint8 membership_state[CLUSTER_MAX_NODES];
} ClusterMembershipTable;

/*
 * Point the module's accessors at the shared backing table (called once from
 * cluster_reconfig_shmem_init with &ReconfigShmem->membership).  Passing NULL
 * resets to the process-local default table (used by the pure unit-test layer).
 * The caller owns the reconfig LWLock discipline for mutations.
 */
extern void cluster_membership_attach(ClusterMembershipTable *table);

/*
 * INV-J7 bring-up durable seed.  Rebuilds last_admitted_incarnation[] from the
 * durable §2.6 join-commit markers (phase==COMMITTED && majority && crc) so a
 * coordinator/postmaster restart does not zero the floor and re-open the gate to
 * a stale incarnation.  The trust gate is NEVER an epoch comparison (a restart
 * resets the volatile epoch to 0).  Runs at startup, scanning the voting-disk
 * region-3 join-marker slots (cluster_reconfig.c owns the disk read; this applies
 * each committed-basis marker to the floor via cluster_membership_seed_apply_marker).
 */
extern void cluster_membership_seed_last_admitted_from_voting_disk(const int *fds, int n_disks);

/*
 * §2.6 ClusterJoinCommitMarker — durable, CRC-checked voting-disk record written
 * by the coordinator into the region-3 join-slot.  PHASE is the commit proof
 * (P1#1-r4): trust is NEVER a comparison against the volatile local epoch (a
 * restart resets epoch to 0).  Mirrors spec-5.13's ClusterLeaveIntentMarker but
 * with magic 'JCMK' (L372 unique) and a 2-phase bracket where the COMMITTED
 * marker IS the commit point (written BEFORE the publish), seeding only the
 * monotonic incarnation floor.
 */
#define CLUSTER_JCMK_MAGIC 0x4A434D4B /* "JCMK" */
/*
 * version 2 (Hardening v1.1, INV-J13): adds commit_nonce so that the majority
 * judgement groups markers by full commit identity, not "any COMMITTED marker".
 * A v1 marker fails struct_valid (version mismatch) and is fail-closed rejected
 * -- correct: a marker whose layout we cannot parse is never trusted (pre-1.0,
 * no on-disk upgrade guarantee).
 */
#define CLUSTER_JCMK_VERSION 2
#define CLUSTER_JCMK_PHASE_PREPARE 1   /* intent recorded; does NOT seed */
#define CLUSTER_JCMK_PHASE_COMMITTED 2 /* committed; THIS seeds last_admitted */

typedef struct ClusterJoinCommitMarker {
	uint32 magic;	/* CLUSTER_JCMK_MAGIC */
	uint32 version; /* CLUSTER_JCMK_VERSION */
	int32 node_id;	/* the admitted joiner N */
	uint8 phase;	/* PREPARE | COMMITTED (self-contained proof) */
	uint8 _pad[3];
	uint64 generation;			   /* monotonic torn-write guard (read newest) */
	uint64 admitted_incarnation;   /* incarnation N was admitted at (uint64, P1a) */
	uint64 admitted_epoch;		   /* INFORMATIONAL only — NOT a trust gate */
	uint64 supersedes_leave_epoch; /* clears clean_departed/leave marker for N */
	uint64 commit_nonce;		   /* per-commit-attempt id (INV-J13 identity group) */
	uint32 crc32c;				   /* CRC32C over [magic .. commit_nonce] */
} ClusterJoinCommitMarker;

/* outcome of cluster_reconfig_submit_join_marker (mirrors the fence/leave result). */
typedef enum ClusterJoinMarkerSubmitResult {
	CLUSTER_JOIN_MARKER_SUBMIT_ACK = 0, /* durable on >= quorum-majority disks */
	CLUSTER_JOIN_MARKER_SUBMIT_FAILED,	/* qvotec wrote but did not reach majority */
	CLUSTER_JOIN_MARKER_SUBMIT_TIMEOUT	/* qvotec did not complete within the bound */
} ClusterJoinMarkerSubmitResult;

/* pure marker integrity (rule 15) — compute / validate / committed-basis gate. */
extern void cluster_join_marker_compute_crc(ClusterJoinCommitMarker *m);
extern bool cluster_join_marker_struct_valid(const ClusterJoinCommitMarker *m, int32 expected_node);
extern bool cluster_join_marker_is_committed_basis(const ClusterJoinCommitMarker *m,
												   int32 expected_node);
/*
 * INV-J13 (Hardening v1.1): do two markers describe the SAME commit attempt?
 * Compares the full identity range [magic .. commit_nonce] (the markers are
 * memset-0 before fill, so _pad is stable).  The majority judgement counts only
 * markers that are same_commit -- different attempts (different coordinator /
 * epoch / nonce) MUST NOT aggregate into a false majority (P1-3).
 */
extern bool cluster_join_marker_same_commit(const ClusterJoinCommitMarker *a,
											const ClusterJoinCommitMarker *b);

/*
 * INV-J13 — select a marker that belongs to a same-commit MAJORITY from an
 * array of (already committed-basis) markers.  Returns the index of any member
 * of the winning same-commit group (and, if out_agree != NULL, the number of
 * markers in that group), or -1 if no single commit attempt reached `majority`.
 * Shared by self-admit, startup-seed and qvotec peer-observe so a distinct-
 * attempt minority can never aggregate into a false majority (P1 #2).
 */
extern int cluster_join_marker_select_majority(const ClusterJoinCommitMarker *markers, int n,
											   uint32 majority, uint32 *out_agree);

/*
 * Apply one durable marker to the admitted floor (INV-J7): if it is a committed
 * basis for expected_node, raise last_admitted_incarnation[node] toward its
 * admitted_incarnation (monotonic).  Pure (operates on the attached table); the
 * runtime seed calls it per region-3 slot, and the unit tests exercise it
 * directly (U10/U13/U15).  Returns true iff the marker was applied.
 */
extern bool cluster_membership_seed_apply_marker(const ClusterJoinCommitMarker *m);

/*
 * cluster_membership_vet_joiner
 *		Pure monotonic-incarnation + range gate.  NO shmem mutation; returns a
 *		verdict the coordinator acts on under lock.  Foundation slice implements
 *		INV-J1 (monotonic incarnation) + node_id range defense; the quorum and
 *		readiness sub-gates are wired at D5 integration (see file banner).
 */
extern ClusterJoinVerdict cluster_membership_vet_joiner(int32 node_id, uint64 presented_incarnation,
														uint64 slot_generation);

/* admitted-incarnation floor (non-decreasing; coord-only mutation under lock) */
extern uint64 cluster_membership_get_last_admitted_incarnation(int32 node_id);
extern void cluster_membership_record_admitted(int32 node_id, uint64 incarnation);

/* membership-state decision SSOT (coord-only mutation under lock) */
extern ClusterMembershipState cluster_membership_get_state(int32 node_id);
extern void cluster_membership_set_state(int32 node_id, ClusterMembershipState state);
extern bool cluster_membership_is_member(int32 node_id);

/*
 * spec-5.18 D4 — member-set denominator SSOT.  Counts declared nodes whose
 * membership_state == MEMBER (excludes REMOVED, DEAD, JOINING, ABSENT, REJECTED).
 * This is the denominator that shrinks on permanent removal (barrier expected-set
 * / GRD baseline), NOT the disk-quorum denominator (§0.3 honest split).
 */
extern int cluster_membership_member_count(void);

/*
 * spec-5.18 D4 — permanent removal (the shrink dual of record_admitted): set
 * membership_state[node]=REMOVED (terminal) and pin the admitted-incarnation floor
 * at last_incarnation (a future re-admit must present > this).  Coordinator-only,
 * under the reconfig LWLock; idempotent (also used by the startup rebuild).
 */
extern void cluster_membership_shrink_to_removed(int32 node_id, uint64 last_incarnation);

#endif /* FRONTEND */
#endif /* CLUSTER_MEMBERSHIP_H */
