/*-------------------------------------------------------------------------
 *
 * cluster_cr.h
 *	  pgrac own-instance Consistent Read (CR) block construction API.
 *
 *	  Stage 3 第 13 sub-spec.  Builds a snapshot-visible historical block
 *	  image at snapshot.read_scn by copying the current page into a
 *	  backend-local scratch slot, then walking the ITL-rooted undo chain
 *	  backward and inverse-applying each undo record whose write_scn is
 *	  newer than read_scn.  Consumes spec-3.7 undo reader + spec-3.4a/b
 *	  ITL on-page metadata + spec-3.8 segment lifecycle + cluster_undo_smgr.
 *
 *	  2-layer API (spec-3.9 §2.1 Q9a):
 *	    - cluster_cr_construct_block()       — bottom layer; always constructs
 *	    - cluster_cr_lookup_or_construct()   — top layer; spec-3.9 fall-through
 *	                                            to construct, spec-3.10 adds a
 *	                                            CR cache lookup here
 *
 *	  The MVCC cluster visibility path (heapam_visibility.c PGRAC
 *	  MODIFICATIONS) calls ONLY the top layer.
 *
 *	  Invariants (spec-3.9 §3.1):
 *	    - I-lock-1   caller holds >= BUFFER_LOCK_SHARE on buf
 *	    - I-lock-2   CR internal 0 LockBuffer / 0 LWLockAcquire / 0 spinlock
 *	    - I-lock-3   non-reentrant (single backend-local scratch slot)
 *	    - I-lock-4   read-only: no mark dirty / no WAL / no shared-buffer replace
 *	    - I-fail-1   CR construction failure is NEVER tuple-invisible; raise
 *	                 ereport(ERROR) with the precise SQLSTATE (53R9F / 53R9G /
 *	                 data_corrupted), never a silent NULL/false
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Function bodies land in src/backend/cluster/cluster_cr.c (Step 2-4);
 *	  this header declares the schema only (spec-3.9 Step 1 / D1).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_H
#define CLUSTER_CR_H

#ifndef FRONTEND

#include "postgres.h"

#include "access/htup.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h" /* ClusterVisibilityDecision */
#include "storage/bufmgr.h"
#include "utils/snapshot.h"


/*
 * cluster_cr_lookup_or_construct -- top-layer CR API (Q9a B forward-link).
 *
 *   spec-3.9: fall-through to cluster_cr_construct_block().
 *   spec-3.10: insert CR cache probe/install here (miss → construct + cache,
 *              hit → return cached image).  Visibility caller MUST use this
 *              entry, NOT the bottom layer.  itl_idx retired (spec-3.10 D7):
 *              full-block CR rolls back every candidate chain, so no single
 *              chain index is passed; the gate still reads the queried tuple's
 *              live ITL slot for its tier-2 + xmin-side checks.
 *
 *   Return semantics identical to cluster_cr_construct_block().
 */
extern const char *cluster_cr_lookup_or_construct(Buffer buf, SCN read_scn);

/*
 * cluster_cr_construct_block -- bottom-layer full-block CR (always builds).
 *
 *   1. Assert caller holds >= BUFFER_LOCK_SHARE + non-reentrant guard.
 *   2. memcpy(scratch, BufferGetPage(buf), BLCKSZ).
 *   3. Snapshot every ITL slot with write_scn newer than read_scn (candidate chains),
 *      then inverse-apply each chain newest-transaction-first (write_scn DESC)
 *      until each record's write_scn is not later than read_scn.
 *   4. Reaching a chain end without a reconstructable base state surfaces as
 *      a missing undo record (53R9F), not silent success.
 *
 *   Return:
 *     - success: const char * to the backend-local scratch page; VALID only
 *       until the next cluster_cr_construct_block / lookup_or_construct call;
 *       caller MUST NOT free / modify / cache the pointer.
 *     - On failure the function ereport(ERROR)s (53R9F / 53R9G / data_corrupted)
 *       before returning; it does not return a silent NULL to the visibility
 *       caller (spec-3.9 I-fail-1).
 */
extern const char *cluster_cr_construct_block(Buffer buf, SCN read_scn);

/*
 * cluster_cr_remap_tuple -- materialize a HeapTupleData wrapper for a CR-image
 *                           tuple at the given offset on the CR scratch page.
 *
 *   out_htup is caller-owned (stack) storage; this fills t_data / t_len /
 *   t_self / t_tableOid(InvalidOid).  out_htup->t_data points INTO cr_page
 *   and is valid only while cr_page is valid.  Returns false if off is
 *   out-of-range or the ItemId is LP_UNUSED (CR-removed via inverse INSERT).
 */
extern bool cluster_cr_remap_tuple(const char *cr_page, OffsetNumber off, HeapTupleData *out_htup);

/*
 * cluster_visibility_decide_tuple -- tuple-level cluster visibility helper.
 *
 *   spec-3.9 adds the tuple-level helper used by the MVCC cluster path fast
 *   exits (page gate / ITL gate).  MUST NOT call XidInMVCCSnapshot(),
 *   TransactionIdDidCommit(), or any CLOG/ProcArray fallback (AD-012 例外 9).
 *   Generic helper; implemented in the cluster visibility helper layer.
 */
extern ClusterVisibilityDecision cluster_visibility_decide_tuple(HeapTuple htup, Snapshot snapshot,
																 Buffer buffer);

/*
 * cluster_visibility_decide_cr_tuple -- tuple-level helper for transient CR
 *   image tuples.  Same SCN/ITL/TT visibility semantics as
 *   cluster_visibility_decide_tuple, but the tuple storage lives in
 *   backend-local CR scratch: the helper MUST NOT set hint bits nor assume
 *   the tuple belongs to a dirtyable shared buffer.  Implemented in cluster_cr.c.
 */
extern ClusterVisibilityDecision cluster_visibility_decide_cr_tuple(HeapTuple htup,
																	Snapshot snapshot);

/*
 * ClusterCrVerdict -- spec-4.5a D8 (P0-2): the CR gate's tri-state result.
 *
 *   bool could not distinguish "the gate did not fire" from "the gate fired
 *   but a REMOTE-origin chain is unresolvable": both were false, and the
 *   caller silently fell through to the remote-xid / PG-native paths --
 *   which resolve by a different (exact-identity / local-ProcArray) model
 *   and may return a wrong answer for a materialized-remote tuple.  The
 *   FAILCLOSED arm makes that case an explicit caller-side ereport.
 *
 *   NOT_APPLICABLE is 0 so a zeroed default stays the safe fall-through.
 */
typedef enum ClusterCrVerdict {
	CLUSTER_CR_NOT_APPLICABLE = 0, /* gate did not fire: caller continues to
									* the remote-xid path / PG-native body
									* (pre-4.5a `false`) */
	CLUSTER_CR_DECIDED,			   /* *out_visible is authoritative: caller
									* returns it (pre-4.5a `true`) */
	CLUSTER_CR_FAILCLOSED,		   /* a materialized-remote chain could not be
									* resolved: caller MUST ereport (53R9G) --
									* NEVER fall through (规则 8.A) */
} ClusterCrVerdict;

/*
 * cluster_cr_satisfies_mvcc -- spec-3.9 Step 5 MVCC 3-tier short-circuit gate.
 *
 *   Called additively at the top of HeapTupleSatisfiesMVCC's cluster path.
 *   DECIDED + *out_visible when the historical-CR case applies (block_scn
 *   newer than read_scn AND the tuple's ITL write_scn newer than read_scn
 *   AND the tuple's origin is this instance OR a merged-materialized remote
 *   instance -- spec-4.5a D8).  NOT_APPLICABLE for every other case — the
 *   caller continues to the existing spec-3.2/3.3 remote-xid path /
 *   PG-native body unchanged.  FAILCLOSED when a materialized-remote
 *   tuple's chain needs a commit-state the local node cannot lawfully
 *   answer yet (raw-xid CLOG/ProcArray would cross-instance alias --
 *   AD-012 例外 9); the caller ereports 53R9G.
 *
 *   The three short-circuit tiers (page gate / ITL gate) avoid constructing
 *   a CR image on the common fast path; only a genuinely post-snapshot
 *   tuple reaches cluster_cr_lookup_or_construct.
 */
extern ClusterCrVerdict cluster_cr_satisfies_mvcc(HeapTuple htup, Snapshot snapshot, Buffer buffer,
												  bool *out_visible);

/*
 * cluster_cr_no_peer_fastpath_decide -- spec-3.24 D1 pure verdict.
 *
 *   Returns true (=> the caller skips the whole cluster visibility fork and
 *   uses the PG-native MVCC body) ONLY when ALL hold:
 *     gate_on       cluster.cr_gate_no_peer_fastpath is on
 *     !has_peers    no cross-node xids exist, so AD-012 例外 9's "never
 *                   PG-native" premise (remote xid absent from local ProcArray)
 *                   is void -> PG-native verdict == SCN/CR verdict
 *     session_local snapshot taken by this backend's GetSnapshotData (AD-012
 *                   row #1); an imported/restored snapshot (row #2) is excluded
 *     !has_materialized_remote
 *                   no dead peer's stream was merged-materialized on this node
 *                   (spec-4.5a G6); materialized tuples carry foreign xids that
 *                   alias in the local CLOG, so they must take the cluster fork
 *   Fail-closed: any uncertainty returns false -> the CR/SCN cluster path runs.
 *   Dependency-free pure verdict; exercised end-to-end by t/239 (fastpath on)
 *   and t/248 (materialized-remote premise voids it).
 */
extern bool cluster_cr_no_peer_fastpath_decide(bool gate_on, bool has_peers, bool session_local,
											   bool has_materialized_remote);

/*
 * cluster_cr_no_peer_fastpath_eligible -- live-state wrapper over the pure
 * verdict (reads the GUC, topology, and the snapshot's session-local flag).
 * Returns false under an active cluster_test_force_visibility_cluster_path
 * override so a forced-CR test still exercises the cluster path.
 */
extern bool cluster_cr_no_peer_fastpath_eligible(Snapshot snapshot);

/* Counter accessors (spec-3.9 §2.5 — 9 counters; spec-3.10 D5 +4 cache = 13). */
extern uint64 cluster_cr_construct_count(void);
extern uint64 cluster_cr_snapshot_too_old_count(void);
extern uint64 cluster_cr_cross_instance_unsupported_count(void);
extern uint64 cluster_cr_corruption_count(void);
extern uint64 cluster_cr_chain_walk_steps_sum(void);
extern uint64 cluster_cr_inverse_insert_count(void);
extern uint64 cluster_cr_inverse_update_count(void);
extern uint64 cluster_cr_inverse_delete_count(void);
extern uint64 cluster_cr_inverse_itl_count(void);
extern uint64 cluster_cr_cache_hit_count(void);
extern uint64 cluster_cr_cache_miss_count(void);
extern uint64 cluster_cr_cache_evict_count(void);
extern uint64 cluster_cr_cache_install_count(void);
/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
extern uint64 cluster_cr_xmax_resolved_count(void);
extern uint64 cluster_cr_xmax_recycled_invisible_count(void);
extern uint64 cluster_cr_xmax_invalid_or_ambiguous_count(void);
extern uint64 cluster_cr_xmax_scan_unavail_or_no_proof_count(void);

/* spec-6.12b: CR-server data plane counters. */
extern uint64 cluster_cr_remote_full_count(void);
extern uint64 cluster_cr_remote_partial_count(void);
extern uint64 cluster_cr_remote_failed_count(void);
extern uint64 cluster_cr_server_full_count(void);
extern uint64 cluster_cr_server_partial_count(void);
extern uint64 cluster_cr_server_denied_count(void);

/* spec-6.12i: undo-TT fetch data plane counters + requester-side bumps. */
extern uint64 cluster_rtvis_undo_fetch_wire_count(void);
extern uint64 cluster_rtvis_undo_fetch_cache_hit_count(void);
extern uint64 cluster_rtvis_undo_fetch_failclosed_count(void);
extern uint64 cluster_cr_server_undo_served_count(void);
extern uint64 cluster_cr_server_undo_denied_count(void);
extern void cluster_rtvis_undo_fetch_note_wire(void);
extern void cluster_rtvis_undo_fetch_note_cache_hit(void);
extern void cluster_rtvis_undo_fetch_note_failclosed(void);

/* spec-6.12i CP3: recycled-slot active-runtime resolution verdicts. */
extern uint64 cluster_rtvis_resolve_committed_count(void);
extern uint64 cluster_rtvis_resolve_aborted_count(void);
extern uint64 cluster_rtvis_resolve_failclosed_count(void);
extern void cluster_rtvis_resolve_note_committed(void);
extern void cluster_rtvis_resolve_note_aborted(void);
extern void cluster_rtvis_resolve_note_failclosed(void);

/* spec-6.12i CP5 (D-i4): origin-verdict leg (5 requester + 2 server). */
extern uint64 cluster_rtvis_verdict_wire_count(void);
extern uint64 cluster_rtvis_verdict_failclosed_count(void);
extern uint64 cluster_rtvis_verdict_exact_count(void);
extern uint64 cluster_rtvis_verdict_below_horizon_count(void);
extern uint64 cluster_rtvis_verdict_inadmissible_count(void);
extern uint64 cluster_cr_server_verdict_served_count(void);
extern uint64 cluster_cr_server_verdict_denied_count(void);
extern void cluster_rtvis_verdict_note_wire(void);
extern void cluster_rtvis_verdict_note_failclosed(void);
extern void cluster_rtvis_verdict_note_exact(void);
extern void cluster_rtvis_verdict_note_below_horizon(void);
extern void cluster_rtvis_verdict_note_inadmissible(void);

/* spec-6.15 D4: underivable-origin refusals (classify_ref). */
extern uint64 cluster_rtvis_underivable_failclosed_count(void);
extern void cluster_rtvis_note_underivable_failclosed(void);

/* spec-5.22f D6-3: fresh-remote-ITL-ref widening outcome counters. */
extern uint64 cluster_vis_freshref_verdict_resolved_count(void);
extern uint64 cluster_vis_freshref_verdict_failclosed_count(void);
extern void cluster_vis_freshref_verdict_note_resolved(void);
extern void cluster_vis_freshref_verdict_note_failclosed(void);

/* spec-5.22d D4-4/D4-5: dead-owner authority block0 serve counters. */
extern uint64 cluster_undo_authority_serve_hit_count(void);
extern uint64 cluster_undo_authority_fail_closed_count(void);
extern uint64 cluster_undo_authority_epoch_stale_reject_count(void);
extern void cluster_undo_authority_note_serve_hit(void);
extern void cluster_undo_authority_note_failclosed(void);
extern void cluster_undo_authority_note_epoch_stale_reject(void);

/*
 * spec-6.12i CP5 (D-i4): origin-side pieces of the cross-instance verdict
 * serve (cluster_cr_server.c).
 *
 *	cluster_cr_retention_proof_origin_legs — the spec-3.22 retention-proof
 *	legs (a)-(d) evaluated on THIS node with the current horizon returned
 *	for the wire; the requester decides leg (e) against its own read_scn.
 *	MUST be evaluated after the complete by-xid scan (ordering contract in
 *	the function comment — the horizon monotonicity argument).
 *
 *	cluster_cr_accept_resolved_scn — the authoritative wrap-suspect
 *	acceptance gate for a by-xid RESOLVED_SCN match (bumps the
 *	wrap_generation_disambiguated counter on refusal).
 */
extern bool cluster_cr_retention_proof_origin_legs(SCN *out_horizon);
extern bool cluster_cr_accept_resolved_scn(SCN scn);

/* Shmem region register / size / init (L206 5-step). */
extern Size cluster_cr_shmem_size(void);
extern void cluster_cr_shmem_init(void);
extern void cluster_cr_shmem_register(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_H */
