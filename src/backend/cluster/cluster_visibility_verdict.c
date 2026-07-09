/*-------------------------------------------------------------------------
 *
 * cluster_visibility_verdict.c
 *	  pgrac spec-3.14 §2.2 OBS truth tables as pure verdict functions.
 *
 *	  Split out from cluster_visibility_resolve.c so the truth-table
 *	  policy (status -> verdict) carries ZERO PG-backend dependency and
 *	  can be unit-tested by full enumeration on its own.  The five
 *	  HeapTupleSatisfies* forks (Update/Dirty/Self/Toast) and the unit
 *	  test are the only callers; this is the single source of truth for
 *	  the OBS-2~5 tables (L212).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_visibility_verdict.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.14-remaining-visibility-paths.md (FROZEN v0.2) §2.2.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_resolve.h"

/* ============================================================
 *	spec-3.14 §2.2 OBS truth tables (pure verdict functions).
 *
 *	Input is a terminal-or-in-progress ClusterTTStatus (the resolver
 *	already followed SUBCOMMITTED to its parent; a residual SUBCOMMITTED
 *	is treated as in-progress, defensively).  COMMITTED and CLEANED_OUT
 *	are the two "committed" states and behave identically here.
 * ============================================================ */

ClusterVisVerdict
cluster_vis_self_verdict(ClusterTTStatus status)
{
	switch (status) {
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		return CVV_VISIBLE;
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
	case CLUSTER_TT_STATUS_ABORTED:
		return CVV_INVISIBLE;
	case CLUSTER_TT_STATUS_UNKNOWN:
	default:
		return CVV_FAILCLOSED_UNKNOWN;
	}
}

ClusterVisVerdict
cluster_vis_toast_verdict(ClusterTTStatus status)
{
	/* OBS-5: toast is read only after the main row passed visibility, so
	 * PG-native is permissive -- only an ABORTED writer hides the chunk. */
	switch (status) {
	case CLUSTER_TT_STATUS_ABORTED:
		return CVV_INVISIBLE;
	case CLUSTER_TT_STATUS_UNKNOWN:
		return CVV_FAILCLOSED_UNKNOWN; /* a torn toast chain must be heard */
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
	default:
		return CVV_VISIBLE;
	}
}

ClusterVisVerdict
cluster_vis_update_xmin_verdict(ClusterTTStatus status)
{
	switch (status) {
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		return CVV_VISIBLE; /* proceed to xmax judgement */
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
	case CLUSTER_TT_STATUS_ABORTED:
		return CVV_INVISIBLE;
	case CLUSTER_TT_STATUS_UNKNOWN:
	default:
		return CVV_FAILCLOSED_UNKNOWN;
	}
}

ClusterVisVerdict
cluster_vis_update_xmax_verdict(ClusterTTStatus status, bool is_delete)
{
	switch (status) {
	case CLUSTER_TT_STATUS_ABORTED:
		return CVV_VISIBLE; /* writer aborted -> tuple still ok (TM_Ok) */
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		return is_delete ? CVV_GONE_DELETED : CVV_GONE_UPDATED;
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
		/* spec-3.14 C-V4: the Satisfies layer only flags this; the
		 * caller-side wait bridge (D2b) fail-closes to 53R9H. */
		return CVV_BEING_MODIFIED;
	case CLUSTER_TT_STATUS_UNKNOWN:
	default:
		return CVV_FAILCLOSED_UNKNOWN;
	}
}

/*
 * spec-3.21 §2.3: CR image xmax-side MVCC verdict (pure; mirrors OBS-1 amend
 * MVCC-accurate, the multixact-member table at cluster_visibility_resolve.c).
 *
 *	ABORTED / IN_PROGRESS / SUBCOMMITTED -> the delete never committed at the
 *	snapshot, so the row was LIVE at read_scn -> VISIBLE.  This is the spec-3.21
 *	fix: the prior cluster_visibility_decide_cr_tuple treated ANY valid xmax as
 *	"deleted" -> invisible, which false-hid a hot row whose deleter was still in
 *	progress and produced silent UPDATE 0 / lost updates (D0.6: 538 cases).
 *
 *	COMMITTED / CLEANED_OUT -> invisible only if the delete is visible at
 *	read_scn, i.e. exact commit_scn at/before read_scn (cluster_visibility_decide_by_scn
 *	returns VISIBLE for the delete; we invert to INVISIBLE for the tuple).  A
 *	delete committed after read_scn leaves the row VISIBLE.
 *
 *	UNKNOWN, or COMMITTED with an unresolved commit_scn (committed_scn_decision
 *	== CLUSTER_VISIBILITY_UNKNOWN), -> CVV_FAILCLOSED_UNKNOWN.  The caller raises
 *	53R9F; never silently invisible (rule 8.A; P1-a forbids a CLOG/write_scn proxy).
 *
 *	committed_scn_decision is the caller's cluster_visibility_decide_by_scn(
 *	commit_scn, read_scn); kept in the caller so this stays a pure status->verdict
 *	function (no scn_time_cmp dependency).
 */
ClusterVisVerdict
cluster_vis_cr_xmax_verdict(ClusterTTStatus xmax_status,
							ClusterVisibilityDecision committed_scn_decision)
{
	switch (xmax_status) {
	case CLUSTER_TT_STATUS_ABORTED:
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
		/* Deleter not committed at read_scn -> row live -> visible. */
		return CVV_VISIBLE;
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		switch (committed_scn_decision) {
		case CLUSTER_VISIBILITY_VISIBLE:
			/* delete visible at read_scn (committed at/before snapshot) -> tuple gone */
			return CVV_INVISIBLE;
		case CLUSTER_VISIBILITY_INVISIBLE:
			/* delete not yet visible (committed after snapshot) -> tuple live */
			return CVV_VISIBLE;
		case CLUSTER_VISIBILITY_UNKNOWN:
		default:
			/* InvalidScn / unresolved commit_scn -> fail-closed, not invisible */
			return CVV_FAILCLOSED_UNKNOWN;
		}
	case CLUSTER_TT_STATUS_UNKNOWN:
	default:
		return CVV_FAILCLOSED_UNKNOWN;
	}
}

ClusterVisVerdict
cluster_vis_dirty_verdict(ClusterTTStatus status, bool is_xmax, bool is_delete)
{
	switch (status) {
	case CLUSTER_TT_STATUS_IN_PROGRESS:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
		/* OBS-3: Dirty has no wait_policy layer -> cross-node pending is a
		 * conflict the caller cannot wait on locally (53R9H). */
		return CVV_FAILCLOSED_CONFLICT;
	case CLUSTER_TT_STATUS_ABORTED:
		return is_xmax ? CVV_VISIBLE : CVV_INVISIBLE;
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		if (!is_xmax)
			return CVV_VISIBLE; /* xmin committed -> tuple exists */
		return is_delete ? CVV_GONE_DELETED : CVV_GONE_UPDATED;
	case CLUSTER_TT_STATUS_UNKNOWN:
	default:
		return CVV_FAILCLOSED_UNKNOWN;
	}
}

/* ============================================================
 *	spec-5.22f D6-1: fresh-remote-ITL-ref visibility consumer helpers.
 *
 *	Two PURE helpers the classify_ref_guts fresh-ref widening (D6-2) calls
 *	to consume a D3 cross-node verdict.  No I/O, no shmem, no elog, so they
 *	unit-test by full enumeration (test_cluster_vis_undo_verdict_map,
 *	U1-U10) alongside the OBS-2~5 tables above.
 * ============================================================ */

/*
 * cluster_vis_from_undo_verdict -- map a D3 five-value verdict onto the local
 * visibility out-params.  See the header for the truth table.  EVERY branch
 * overwrites commit_scn + commit_scn_is_bound so a non-terminal verdict never
 * leaks a residual scn (U7).  Returns true iff a terminal (COMMITTED/ABORTED)
 * outcome was produced; false keeps the caller on the 53R97 fail-closed path.
 */
bool
cluster_vis_from_undo_verdict(ClusterUndoVerdictResult v, ClusterVisResolve *out)
{
	switch (v.kind) {
	case CLUSTER_UNDO_VERDICT_COMMITTED_EXACT:
		out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = v.commit_scn;
		out->commit_scn_is_bound = false;
		return true;

	case CLUSTER_UNDO_VERDICT_COMMITTED_BOUND:
		/*
		 * A below-horizon bound decides correctly ONLY against the read_scn
		 * this resolve ran under; commit_scn_is_bound forbids stamping/caching
		 * it as an exact commit_scn (spec-6.12i CP5 / Rule 8.A).
		 */
		out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = v.commit_scn;
		out->commit_scn_is_bound = true;
		return true;

	case CLUSTER_UNDO_VERDICT_ABORTED:
		out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
		out->status = CLUSTER_TT_STATUS_ABORTED;
		out->commit_scn = InvalidScn;
		out->commit_scn_is_bound = false;
		return true;

	case CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED:
	case CLUSTER_UNDO_VERDICT_IN_PROGRESS:
	default:
		/*
		 * Not proven terminal -> STALE_OR_AMBIGUOUS so the caller keeps 53R97
		 * (Rule 8.A / L10: a zero / unknown verdict is never visible).  D3
		 * folds a proven-live xid to UNKNOWN already; IN_PROGRESS is handled
		 * here only defensively.
		 */
		out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
		out->status = CLUSTER_TT_STATUS_UNKNOWN;
		out->commit_scn = InvalidScn;
		out->commit_scn_is_bound = false;
		return false;
	}
}

/*
 * cluster_vis_freshref_origin_decision -- the fresh-ref origin decision
 * (spec-5.22f Q2 / Option B, P1-a).  A fresh ref's ref_origin is the tuple
 * page's physical ITL binding (the true owner); the value-derived slot is only
 * a derivable-time integrity cross-check.  Underivable (-1) is deliberately
 * ASK, not fail-closed: the physical binding is authoritative and D3's
 * wrap-suspect / covers / serve gates fail-close every unproven leg inside the
 * verdict.  A derivable mismatch is a striping bug / page corruption / alias
 * -> STALE (Rule 8.A integrity guard).  Pure.
 */
ClusterVisFreshRefOriginDecision
cluster_vis_freshref_origin_decision(int derived_slot, int32 ref_origin)
{
	if (derived_slot < 0)
		return CLUSTER_VIS_FRESHREF_ORIGIN_ASK; /* underivable (P1-a) */
	if (derived_slot == (int) ref_origin)
		return CLUSTER_VIS_FRESHREF_ORIGIN_ASK; /* corroborated */
	return CLUSTER_VIS_FRESHREF_ORIGIN_STALE;	/* derivable mismatch (8.A) */
}

#endif /* USE_PGRAC_CLUSTER */
