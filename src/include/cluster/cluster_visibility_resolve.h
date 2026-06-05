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
 *	  Exact-key discipline (spec-3.14 v0.2, R10): an ITL slot ref is
 *	  only authoritative when ref.tt_slot_id != 0 AND
 *	  ref.local_xid == raw_xid AND ref.origin_node_id != self.  A slot
 *	  whose recorded xid no longer matches the tuple-side xid was
 *	  recycled to another owner -> STALE_OR_AMBIGUOUS -> caller 53R97;
 *	  NEVER silently fall through to the PG-native CLOG path (that is
 *	  exactly the false-resolve this resolver exists to prevent).
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
#include "storage/buf.h"

#include "cluster/cluster_scn.h"	   /* SCN */
#include "cluster/cluster_tt_slot.h"   /* ClusterUndoTTSlotRef */
#include "cluster/cluster_tt_status.h" /* ClusterTTStatus */


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
 * Resolve directly from a caller-supplied ITL ref (e.g. the spec-3.2 D5b
 * test inject hook).  Same classification + remote resolve as
 * cluster_visibility_resolve_tuple but without re-reading the page slot.
 */
extern void cluster_visibility_resolve_from_ref(TransactionId raw_xid,
												const ClusterUndoTTSlotRef *ref,
												ClusterVisResolve *out);


#endif /* CLUSTER_VISIBILITY_RESOLVE_H */
