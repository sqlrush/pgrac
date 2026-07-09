/*-------------------------------------------------------------------------
 *
 * cluster_visibility_resolve.h
 *	  pgrac single tuple-xid cluster status resolver (spec-3.14 D1).
 *
 *	  spec-3.2/3.3 wired the cluster visibility fork into
 *	  HeapTupleSatisfiesMVCC only.  spec-3.14 extends the remaining
 *	  HeapTupleSatisfies* variants (Update / Dirty / Self / Toast) and
 *	  the prune/vacuum guards.  To avoid five divergent copies of the
 *	  "ITL ref -> exact key -> TT overlay lookup -> SUBCOMMITTED follow"
 *	  logic (L212 anti-divergence), that evidence/status resolution is
 *	  extracted here as ONE pure-ish resolver.  Each variant keeps its
 *	  OWN visibility policy (truth table); this file only answers
 *	  "what is the authoritative cluster status of this tuple-side xid,
 *	  and is the evidence local / remote / stale?".
 *
 *	  The resolver performs NO visibility-policy ereport: UNKNOWN /
 *	  STALE_OR_AMBIGUOUS are returned as evidence/status for the caller
 *	  to fail-closed per its own table (53R97).  Genuinely corrupt
 *	  metadata (malformed UBA) still raises DATA_CORRUPTED via the
 *	  underlying helpers, as today.
 *
 *	  Exact-key discipline (spec-3.14 v0.2, R10 refined): an ITL slot
 *	  ref is authoritative remote evidence only when ref.tt_slot_id != 0,
 *	  ref.origin_node_id != self, and ref.local_xid == raw_xid.  A remote
 *	  slot whose recorded xid no longer matches the tuple-side xid was
 *	  recycled to another owner -> STALE_OR_AMBIGUOUS -> caller 53R97;
 *	  NEVER silently fall through to the PG-native CLOG path for remote
 *	  evidence.  Own-instance refs are LOCAL and always route to PG-native
 *	  CLOG, including the normal local hot-page case where the 8-slot ITL
 *	  cache has recycled the tuple's old slot.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_visibility_resolve.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.14-remaining-visibility-paths.md (FROZEN v0.2) §2.1.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VISIBILITY_RESOLVE_H
#define CLUSTER_VISIBILITY_RESOLVE_H

#include "c.h"
#include "access/htup.h"
#include "access/xlogdefs.h" /* XLogRecPtr (spec-4.8 D2 anchor_lsn) */
#include "storage/buf.h"

#include "cluster/cluster_scn.h"	   /* SCN */
#include "cluster/cluster_tt_slot.h"   /* ClusterUndoTTSlotRef */
#include "cluster/cluster_tt_status.h" /* ClusterTTStatus */
#include "cluster/cluster_undo_verdict.h" /* ClusterUndoVerdictResult (spec-5.22f D6) */


/*
 * Which tuple-side xid + how to reach its ITL ref.
 *
 *	XMIN / XMAX_UPDATE : ref is the tuple's own t_itl_slot_idx slot.
 *	XMAX_LOCK_ONLY     : ref via cluster_itl_find_lock_tt_ref_by_xmax().
 *	XMAX_MULTI         : marker-only evidence (HEAP_XMAX_IS_MULTI); the
 *	                     caller resolves members through the 3.6 overlay
 *	                     (member visibility is policy, not this layer).
 */
typedef enum ClusterVisXidKind {
	CLUSTER_VIS_XMIN,
	CLUSTER_VIS_XMAX_UPDATE,
	CLUSTER_VIS_XMAX_LOCK_ONLY,
	CLUSTER_VIS_XMAX_MULTI
} ClusterVisXidKind;

/*
 * Evidence quality for the requested xid.  Only REMOTE carries a
 * meaningful status/commit_scn; the caller treats the others as:
 *	  NONE  -> PG-native body (no cluster evidence; local hot path)
 *	  LOCAL -> PG-native body (own-instance xid; PG CLOG resolves)
 *	  REMOTE -> use status (+ commit_scn for COMMITTED/CLEANED_OUT)
 *	  STALE_OR_AMBIGUOUS -> 53R97 fail-closed (NEVER PG-native)
 */
typedef enum ClusterVisEvidence {
	CLUSTER_VIS_EVIDENCE_NONE = 0,
	CLUSTER_VIS_EVIDENCE_LOCAL,
	CLUSTER_VIS_EVIDENCE_REMOTE,
	CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS
} ClusterVisEvidence;

typedef struct ClusterVisResolve {
	ClusterVisEvidence evidence;
	ClusterTTStatus status;		 /* valid when evidence == REMOTE */
	SCN commit_scn;				 /* valid for COMMITTED / CLEANED_OUT */
	bool commit_scn_is_bound;	 /* spec-6.12i CP5: commit_scn is a HORIZON
								   * BOUND from a below-horizon verdict, not
								   * the exact commit_scn.  It decides
								   * correctly against the read_scn this
								   * resolve ran under ONLY; consumers must
								   * never stamp / cache / memo it as an
								   * exact scn (a later smaller read_scn
								   * would read it as committed-after ->
								   * false-invisible, Rule 8.A). */
	ClusterUndoTTSlotRef ref;	 /* copied exact ref (REMOTE/LOCAL/STALE) */
	uint16 multi_marker_origin;	 /* XMAX_MULTI: origin node of marker, else 0 */
	bool multi_marker_is_remote; /* XMAX_MULTI: marker hit + origin != self */
} ClusterVisResolve;


/*
 * Resolve one tuple-side xid's cluster status.
 *
 *	Caller MUST hold the buffer content lock (L200 / spec-3.4d F10
 *	family) so the page ITL slots are stable for the duration.  Pure
 *	w.r.t. visibility policy: no policy ereport, no SetHintBits, no
 *	CLOG touch on the remote branch (C-V1).  SUBCOMMITTED is followed
 *	to its parent (bounded by cluster.subtrans_max_chain_depth) so the
 *	returned status is terminal-or-in-progress, never SUBCOMMITTED.
 */
extern void cluster_visibility_resolve_tuple(Buffer buffer, HeapTupleHeader htup,
											 TransactionId raw_xid, ClusterVisXidKind which,
											 ClusterVisResolve *out);

/*
 * spec-6.12i CP5: the _scn variant threads the caller's snapshot read_scn
 * down to the active-runtime resolver so a below-horizon origin verdict can
 * be judged admissible (requester leg (e)) and returned as a bound
 * (commit_scn_is_bound).  Callers without snapshot semantics use the plain
 * variant above (read_scn = InvalidScn -> bounds are never admissible;
 * exact verdicts still resolve).  Only the HeapTupleSatisfiesMVCC fork
 * sites pass a real read_scn.
 */
extern void cluster_visibility_resolve_tuple_scn(Buffer buffer, HeapTupleHeader htup,
												 TransactionId raw_xid, ClusterVisXidKind which,
												 SCN read_scn, ClusterVisResolve *out);

/*
 * Resolve directly from a caller-supplied ITL ref (e.g. the spec-3.2 D5b
 * test inject hook).  Same classification + remote resolve as
 * cluster_visibility_resolve_tuple but without re-reading the page slot.
 * anchor_lsn = the tuple's page LSN (spec-4.8 D2 cross-node recovered_through
 * gate); pass InvalidXLogRecPtr to skip the LSN gate (is_materialized only).
 */
extern void cluster_visibility_resolve_from_ref(TransactionId raw_xid,
												const ClusterUndoTTSlotRef *ref,
												XLogRecPtr anchor_lsn, ClusterVisResolve *out);

/* spec-6.12i CP5: _scn variant of the above (see resolve_tuple_scn). */
extern void cluster_visibility_resolve_from_ref_scn(TransactionId raw_xid,
													const ClusterUndoTTSlotRef *ref,
													XLogRecPtr anchor_lsn, SCN read_scn,
													ClusterVisResolve *out);


/*
 * ============================================================
 * spec-3.14 §2.2 OBS truth tables as pure verdict functions.
 *
 *	Each variant fork resolves a tuple-side xid's cluster status via the
 *	resolver above, then maps it to a variant verdict here.  Keeping the
 *	policy as pure functions (status -> verdict, no buffer / no ereport)
 *	makes the OBS-2~5 tables a fully enumerable unit test (60 cases) and
 *	the single source of truth (L212).  The fork translates the verdict
 *	to its native return type (TM_Result / bool) and raises the
 *	fail-closed SQLSTATE.
 * ============================================================
 */

typedef enum ClusterVisVerdict {
	CVV_VISIBLE,			/* proceed / visible / tuple still live */
	CVV_INVISIBLE,			/* not visible to this read */
	CVV_BEING_MODIFIED,		/* remote in-progress writer (Update xmax) */
	CVV_GONE_UPDATED,		/* remote committed update (caller TM_Updated) */
	CVV_GONE_DELETED,		/* remote committed delete (caller TM_Deleted) */
	CVV_FAILCLOSED_UNKNOWN, /* 53R97: status not determinable */
	CVV_FAILCLOSED_CONFLICT /* 53R9H: cross-node write conflict (Dirty) */
} ClusterVisVerdict;

/* OBS-4 Self / OBS-5 Toast: one xid side, "is it visible now". */
extern ClusterVisVerdict cluster_vis_self_verdict(ClusterTTStatus status);
extern ClusterVisVerdict cluster_vis_toast_verdict(ClusterTTStatus status);

/* OBS-2 Update: xmin gate then xmax outcome. */
extern ClusterVisVerdict cluster_vis_update_xmin_verdict(ClusterTTStatus status);
extern ClusterVisVerdict cluster_vis_update_xmax_verdict(ClusterTTStatus status, bool is_delete);

/*
 * spec-3.21 §2.3: CR image xmax-side MVCC visibility verdict.
 *
 *	The SatisfiesUpdate verdict above (cluster_vis_update_xmax_verdict) is
 *	status-only: a COMMITTED deleter makes the live tuple GONE regardless of
 *	SCN, because SatisfiesUpdate answers "is this tuple updatable NOW".  A
 *	snapshot MVCC read of a CR image is different: a deleter that is uncommitted
 *	(IN_PROGRESS / ABORTED) at read_scn, or committed AFTER read_scn, did not
 *	delete the row as of the snapshot, so the row is VISIBLE.  Only a deleter
 *	committed at/before read_scn (exact commit_scn) makes the CR tuple INVISIBLE.
 *	An UNKNOWN status or an unresolved committed commit_scn (committed_scn_decision
 *	== CLUSTER_VISIBILITY_UNKNOWN) is fail-closed (CVV_FAILCLOSED_UNKNOWN), never
 *	silently invisible (rule 8.A / spec-3.21 P1-a: no CLOG/write_scn proxy).
 *
 *	committed_scn_decision is the caller's cluster_visibility_decide_by_scn(
 *	commit_scn, read_scn) result, consulted only for COMMITTED / CLEANED_OUT.
 *	Keeping the SCN compare in the caller leaves this a pure status->verdict
 *	function (no scn_time_cmp link dependency; the SCN compare is unit-tested
 *	separately by test_cluster_visibility_decide_scn).
 */
extern ClusterVisVerdict
cluster_vis_cr_xmax_verdict(ClusterTTStatus xmax_status,
							ClusterVisibilityDecision committed_scn_decision);

/* OBS-3 Dirty: no wait_policy layer, so remote in-progress -> 53R9H. */
extern ClusterVisVerdict cluster_vis_dirty_verdict(ClusterTTStatus status, bool is_xmax,
												   bool is_delete);


/*
 * ============================================================
 * spec-5.22f D6: shared-catalog seed / fresh-ref visibility consumer.
 *
 *	Two PURE helpers (no I/O / no shmem / no elog) that let the
 *	classify_ref_guts fresh-remote-ITL-ref branch consume the D3
 *	cross-node verdict (cluster_undo_verdict_resolve).  Kept pure so the
 *	mapping + origin-decision truth tables are a fully enumerable unit
 *	test (test_cluster_vis_undo_verdict_map, U1-U10) exactly as the OBS-2~5
 *	tables above.  Implementation lives with them in
 *	cluster_visibility_verdict.c (no cluster_visibility_verdict.h churn).
 * ============================================================
 */

/*
 * cluster_vis_from_undo_verdict -- map a D3 cross-node verdict onto the local
 * visibility out-params.  COMMITTED_EXACT/BOUND -> REMOTE/COMMITTED (BOUND
 * sets commit_scn_is_bound so a below-horizon bound is never stamped/cached);
 * ABORTED -> REMOTE/ABORTED; UNKNOWN_FAIL_CLOSED / IN_PROGRESS (D3 never
 * produces IN_PROGRESS) / any other -> STALE_OR_AMBIGUOUS so the caller keeps
 * the 53R97 fail-closed boundary (Rule 8.A / L10).  ALL branches overwrite
 * commit_scn + commit_scn_is_bound so a non-terminal verdict never leaks a
 * residual scn.  Returns true iff a terminal (COMMITTED/ABORTED) was produced.
 */
extern bool cluster_vis_from_undo_verdict(ClusterUndoVerdictResult v, ClusterVisResolve *out);

/*
 * cluster_vis_freshref_origin_decision -- the fresh-remote-ITL-ref origin
 * decision (spec-5.22f Q2 / Option B, P1-a).  A fresh ref (local_xid ==
 * raw_xid) carries the tuple page's PHYSICAL ITL binding, so ref_origin is the
 * true owner; the value-derived cluster_xid_origin_slot(raw_xid) is only a
 * derivable-time integrity cross-check:
 *	  derived_slot < 0 (underivable: below-floor pre-striping seed / striping
 *	                    off)                                       -> ASK (P1-a:
 *	                    NOT fail-closed -- the fresh ref's physical binding is
 *	                    authoritative and D3's wrap-suspect / covers / serve
 *	                    gates fail-close every unproven leg internally)
 *	  derived_slot == ref_origin (corroborated)                   -> ASK
 *	  derived_slot >= 0 && derived_slot != ref_origin (striping bug / page
 *	                    corruption / alias)                        -> STALE
 * Pure.
 */
typedef enum ClusterVisFreshRefOriginDecision {
	CLUSTER_VIS_FRESHREF_ORIGIN_ASK = 0,
	CLUSTER_VIS_FRESHREF_ORIGIN_STALE = 1
} ClusterVisFreshRefOriginDecision;

extern ClusterVisFreshRefOriginDecision cluster_vis_freshref_origin_decision(int derived_slot,
																			 int32 ref_origin);


/*
 * spec-3.14 D5: cheap "does this tuple have any REMOTE writer evidence"
 * test for the prune / vacuum / surely-dead guards.  Looks only at ITL
 * ref origin (no TT overlay lookup) -- a tuple whose xmin or xmax was
 * written by another instance must never be physically removed by this
 * node's local horizon (hole #2: false-dead under overlapping xid
 * spaces).  Caller holds the buffer content lock.
 */
extern bool cluster_tuple_has_remote_evidence(Buffer buffer, HeapTupleHeader tuple);

/*
 * spec-6.14 D8: catalog-safe no-recursion guard.  in_flight() is checked by
 * GetCatalogSnapshot (snapmgr.c) -- a catalog snapshot may never be built
 * while a cluster visibility resolution is running (catalog scan ->
 * cluster_tt lookup -> catalog scan circularity).  abort_reset() clears the
 * depth counter after an ERROR escaped mid-resolve ((Sub)AbortTransaction).
 */
extern bool cluster_vis_resolve_in_flight(void);
extern void cluster_vis_resolve_abort_reset(void);


#endif /* CLUSTER_VISIBILITY_RESOLVE_H */
