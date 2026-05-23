/*-------------------------------------------------------------------------
 *
 * cluster_tt_status.h
 *	  pgrac cluster Undo Transaction Table (TT) status foundation —
 *	  exact-key API for cross-node transaction status visibility.
 *
 *	  spec-3.1 Stage 3 starter foundation:provide bounded in-memory TT
 *	  status overlay keyed by exact identity {origin_node_id,
 *	  undo_segment_id, tt_slot_id, cluster_epoch, local_xid};lookup miss
 *	  returns CLUSTER_TT_STATUS_UNKNOWN with authoritative=false.
 *
 *	  spec-3.1 v1.0 FROZEN scope:
 *	    - foundation ONLY:no HeapTupleSatisfiesMVCC cluster path real
 *	      activation (spec-3.2);no snapshot cross-node (spec-3.3);no ITL
 *	      commit_scn persistent write (spec-3.4);no cross-node TT status
 *	      wire propagation (spec-3.2+)
 *	    - provisional `tt_slot_id` minted by D5 monotonic counter
 *	      (cluster_tt_local_slot_seq);incompatible with future real
 *	      undo-segment TT slot allocation (spec-3.4 must flush all before
 *	      switching)
 *	    - exact-key lookup only;raw TransactionId cluster lookup is NOT
 *	      provided (origin ambiguity is a correctness hazard;feature-069
 *	      AD-006 第五轮 hard guardrail)
 *	    - PG CLOG / TransactionIdDidCommit untouched (feature-069 lock;
 *	      L176 lesson)
 *
 *	  HC contracts in this header (HC180-HC183 4 NEW):
 *	    HC180 exact-key only — raw xid lookup is unsupported by design
 *	          (raw TransactionId has no origin in PG tuple headers;
 *	          spec-3.1 Q3 ★ A;L176)
 *	    HC181 miss fail-closed — lookup miss MUST return UNKNOWN with
 *	          authoritative=false;MUST NOT silent fallback to PG CLOG
 *	          (spec-3.1 Q4 ★ B;L176)
 *	    HC182 epoch fencing — cluster_epoch mismatch on lookup returns
 *	          UNKNOWN;flush_all on reconfig epoch bump (spec-2.39 D14
 *	          callsite pattern;L172 family)
 *	    HC183 ClusterTTStatusKey wire-stable — sizeof == 24 bytes,
 *	          explicit _reserved2 padding required (no implicit padding;
 *	          spec-3.1 v0.4 N9)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.1-cluster-xid-status-foundation.md
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tt_status.h
 *
 * NOTES
 *	  pgrac-original file (Stage 3 starter,2026-05-22).  All types use the
 *	  ClusterTT prefix.  All exported functions use the cluster_tt_status_
 *	  prefix.  Companion overlay implementation lives in
 *	  src/backend/cluster/cluster_tt_status.c (D2);local install helper
 *	  with provisional tt_slot_id minting lives in
 *	  src/backend/cluster/cluster_tt_local.c (D5).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_STATUS_H
#define CLUSTER_TT_STATUS_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_scn.h" /* SCN */

/*
 * ClusterTTStatus -- cluster-aware transaction status enum.
 *
 * Distinct from PG TransactionIdStatus to keep PG-core API untouched
 * (feature-069 AD-006 第五轮 PG CLOG 原生不动).  SUBCOMMITTED reserved for
 * future SUBTRANS spec — NOT in spec-3.1 scope (spec-3.1 §0.1 F4 + §1.3 #10).
 */
typedef enum ClusterTTStatus {
	CLUSTER_TT_STATUS_UNKNOWN = 0,
	CLUSTER_TT_STATUS_IN_PROGRESS = 1,
	CLUSTER_TT_STATUS_COMMITTED = 2,
	CLUSTER_TT_STATUS_ABORTED = 3,
	CLUSTER_TT_STATUS_CLEANED_OUT = 4
} ClusterTTStatus;

/*
 * ClusterTTStatusKey -- exact identity for cluster transaction status.
 *
 * 24 bytes wire-stable (HC183).  Raw TransactionId alone is unsafe as a
 * cluster status key because PG tuple headers carry no origin (spec-3.1
 * Q3 ★ A;HC180).  Cluster visibility must reach the TT slot through
 * ITL / UBA / origin_node, then look up by this exact key.
 *
 * Field layout (must remain stable across pgrac versions in Stage 3+):
 *	  offset  0, 2B : origin_node_id (cluster.node_id of the node that
 *	                  allocated the TT slot)
 *	  offset  2, 2B : undo_segment_id (per-node undo segment id;
 *	                  spec-3.4 真激活 — spec-3.1 uses 0 placeholder)
 *	  offset  4, 4B : tt_slot_id (slot within undo segment TT;
 *	                  spec-3.1 D5 mints monotonic in-memory counter,
 *	                  incompatible with future real TT slot allocation)
 *	  offset  8, 4B : cluster_epoch (reconfig epoch at status mutation;
 *	                  HC182)
 *	  offset 12, 4B : local_xid (PG TransactionId,for diagnostics + tie
 *	                  to provisional install path;NOT a cluster identity
 *	                  component by itself — HC180)
 *	  offset 16, 4B : _reserved (zero on emit;future wrap/generation bits)
 *	  offset 20, 4B : _reserved2 (zero on emit;explicit padding per
 *	                  spec-3.1 v0.4 N9 — DO NOT rely on implicit padding)
 */
typedef struct ClusterTTStatusKey {
	uint16 origin_node_id;
	uint16 undo_segment_id;
	uint32 tt_slot_id;
	uint32 cluster_epoch;
	TransactionId local_xid;
	uint32 _reserved;
	uint32 _reserved2;
} ClusterTTStatusKey;

StaticAssertDecl(sizeof(ClusterTTStatusKey) == 24,
				 "ClusterTTStatusKey must be 24 bytes wire-stable; "
				 "spec-3.1 v0.4 N9 requires explicit _reserved2 padding "
				 "(no implicit)");

/*
 * ClusterTTStatusResult -- lookup result.
 *
 * authoritative=true  -> status came from overlay entry that passed
 *                        epoch/TTL/install checks.
 * authoritative=false -> miss/expired/stale/unknown.  Caller MUST NOT
 *                        treat status field as authoritative; MUST NOT
 *                        silent fallback to PG CLOG (HC181;L176).
 */
typedef struct ClusterTTStatusResult {
	ClusterTTStatus status;
	SCN commit_scn; /* valid for COMMITTED/CLEANED_OUT */
	uint32 status_epoch;
	bool authoritative;
} ClusterTTStatusResult;

/*
 * ClusterVisibilityDecision -- 3-state visibility decision (spec-3.3 D5).
 *
 * MVCC visibility is fundamentally 3-state: yes / no / data not available.
 * Returning a bool collapses "unknown" into one of the two visible states
 * and produces silent-wrong behaviour -- a caller cannot distinguish "tuple
 * is invisible" from "we don't know if the tuple is visible because the
 * remote commit_scn hasn't been propagated yet". L180 NEW candidate
 * (visibility-decision-must-be-enum-not-bool) bans bool returns for any
 * cluster visibility decision helper.
 *
 * Mapping at the cluster path (heapam_visibility.c D10):
 *   VISIBLE   -> return true
 *   INVISIBLE -> return false
 *   UNKNOWN   -> ereport ERROR 53R97 (caller retries / aborts via PG
 *                standard machinery; no wait inside the visibility hot
 *                path -- L177).
 */
typedef enum ClusterVisibilityDecision {
	CLUSTER_VISIBILITY_VISIBLE = 0,
	CLUSTER_VISIBILITY_INVISIBLE = 1,
	CLUSTER_VISIBILITY_UNKNOWN = 2
} ClusterVisibilityDecision;

/*
 * cluster_visibility_decide_by_scn -- spec-3.3 D5 inline helper.
 *
 * Decides cluster-side MVCC visibility from a remote commit_scn and the
 * snapshot's read_scn. Used by heapam_visibility.c D10 fork once an
 * authoritative TT status returns COMMITTED / CLEANED_OUT.
 *
 * Hard rules:
 *   - InvalidScn on either side -> UNKNOWN (NEVER short-circuit to
 *     INVISIBLE; L180 P0).
 *   - Compare via scn_time_cmp(), NEVER raw `<=` (R1 P0). SCN high bits
 *     carry node_id and would pollute time ordering on raw compare.
 *
 * Pure / no syscall / no wait (L177 hot path).
 */
static inline ClusterVisibilityDecision
cluster_visibility_decide_by_scn(SCN commit_scn, SCN read_scn)
{
	if (!SCN_VALID(commit_scn) || !SCN_VALID(read_scn))
		return CLUSTER_VISIBILITY_UNKNOWN;
	if (scn_time_cmp(commit_scn, read_scn) <= 0)
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}

/*
 * Public API.
 *
 * cluster_tt_status_lookup_exact:
 *	  exact-key lookup;returns true on hit (authoritative);false on miss
 *	  (result.status = UNKNOWN,authoritative=false).  Caller MUST honor
 *	  HC181 fail-closed semantics.
 *
 * cluster_tt_status_install_local:
 *	  D5 local install helper companion;writes overlay entry.
 *
 * cluster_tt_status_flush_all:
 *	  reconfig epoch bump hook;clears all overlay entries and bumps
 *	  generation;D7 callsite in cluster_reconfig.c (HC182).
 *
 * cluster_tt_status_generation:
 *	  monotonic generation counter (for debug/observability).
 *
 * cluster_tt_status_shmem_size / _shmem_init / _shmem_register:
 *	  shmem layout (D2).
 */
extern bool cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key,
										   ClusterTTStatusResult *result);
extern void cluster_tt_status_install_local(const ClusterTTStatusKey *key, ClusterTTStatus status,
											SCN commit_scn);
extern void cluster_tt_status_flush_all(uint32 new_epoch);
extern uint64 cluster_tt_status_generation(void);

extern Size cluster_tt_status_shmem_size(void);
extern void cluster_tt_status_shmem_init(void);
extern void cluster_tt_status_shmem_register(void);

/*
 * cluster_tt_status_bump_self_consumer_hit -- internal counter bump used
 * ONLY by D5/D6 commit hook to record the runtime N7 self-consumer
 * lookup (spec-3.1 v0.4 N7).  Do not call from unrelated paths.
 */
extern void cluster_tt_status_bump_self_consumer_hit(void);

/*
 * Counter getters — exposed via pg_cluster_state "tt_status" category
 * (cluster_debug.c).  Always linked (return 0 in disabled-cluster
 * builds).
 */
extern uint64 cluster_tt_status_get_install_count(void);
extern uint64 cluster_tt_status_get_lookup_hit_count(void);
extern uint64 cluster_tt_status_get_lookup_miss_count(void);
extern uint64 cluster_tt_status_get_evict_count(void);
extern uint64 cluster_tt_status_get_flush_count(void);
extern uint64 cluster_tt_status_get_self_consumer_hit_count(void);
extern uint64 cluster_tt_status_get_evict_fail_count(void);

#endif /* CLUSTER_TT_STATUS_H */
