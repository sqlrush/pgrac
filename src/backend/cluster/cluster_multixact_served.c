/*-------------------------------------------------------------------------
 *
 * cluster_multixact_served.c
 *	  pgrac spec-7.1 D3-b (A1) — pure combination resolver for a foreign
 *	  multixact xmax whose members' terminal states were SERVED by the
 *	  origin (no local TT lookup, no shmem) so cluster_unit exercises the
 *	  visibility truth table standalone.
 *
 *	  The origin's member-verdict serve (cluster_cr_server.c) enumerates the
 *	  multi's members from its own pg_multixact and resolves each updater
 *	  member's terminal via the same verdict path as the single-xid serve.
 *	  This resolver consumes those served terminals and applies the multixact
 *	  visibility combination, deferring the committed-updater tuple-visibility
 *	  polarity to cluster_vis_cr_xmax_verdict -- the SSOT shared with the
 *	  single-xmax path and the (D3-b-hotfixed) local-TT resolver
 *	  cluster_multixact_resolve_visibility (spec-3.6, TAP t/212).  The only
 *	  difference from the local-TT resolver is the terminal source: the wire
 *	  verdict here vs cluster_tt_status_lookup_exact there.
 *
 *	  8.A (positive proof only): lock-only members never gate visibility; any
 *	  updater member without a proven terminal (no verdict, inadmissible
 *	  below-horizon bound, or an UNKNOWN decision) yields UNKNOWN so the caller
 *	  fail-closes 53R9C.  This resolver never returns VISIBLE/INVISIBLE on an
 *	  unproven updater.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_served.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Pure: uses only the inline cluster_visibility_decide_by_scn
 *	  helper (cluster_tt_status.h) + scn_time_cmp (cluster_scn.h).
 *	  Spec: spec-7.1-cross-instance-positive-interread.md (D3-b, A1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/multixact.h"		   /* MultiXactStatusForUpdate boundary (3) */
#include "cluster/cluster_gcs_block.h" /* ClusterGcsUndoVerdictKind */
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_scn.h"				/* SCN_VALID, scn_time_cmp */
#include "cluster/cluster_tt_status.h"			/* cluster_visibility_decide_by_scn (inline) */
#include "cluster/cluster_visibility_resolve.h" /* cluster_vis_cr_xmax_verdict (polarity SSOT) */

ClusterVisibilityDecision
cluster_multixact_resolve_visibility_served(const ClusterMultiXactServedMember *members,
											uint16 member_count, SCN read_scn)
{
	uint16 i;

	if (members == NULL)
		return CLUSTER_VISIBILITY_UNKNOWN;

	for (i = 0; i < member_count; i++) {
		const ClusterMultiXactServedMember *m = &members[i];
		ClusterTTStatus status;
		ClusterVisibilityDecision scn_decision = CLUSTER_VISIBILITY_UNKNOWN;

		/*
		 * Lock-only members (FOR_KEY_SHARE / FOR_SHARE / FOR_NOKEYUPDATE /
		 * FOR_UPDATE, status 0-3) cannot hide tuple data regardless of
		 * commit/abort state -- they only lock the row (A2).  They carry no
		 * served verdict and are skipped without gating visibility.
		 */
		if (m->member_status <= MultiXactStatusForUpdate) /* <= 3 */
			continue;

		/*
		 * Update / NoKeyUpdate members (status 4-5): map the origin-SERVED
		 * terminal verdict to a (ClusterTTStatus, scn_decision) pair, then
		 * defer the tuple-visibility polarity to cluster_vis_cr_xmax_verdict --
		 * the SSOT shared with the single-xmax path and the (hotfixed) local-TT
		 * multixact resolver.  8.A: an updater without a proven terminal
		 * (verdict 0 / REFUSE / in-progress -- which the served path cannot
		 * distinguish, so it is deliberately MORE conservative than local-TT)
		 * or an inadmissible below-horizon bound -> UNKNOWN (caller fail-closes).
		 */
		switch (m->verdict) {
		case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
			status = CLUSTER_TT_STATUS_ABORTED;
			break;

		case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
			status = CLUSTER_TT_STATUS_COMMITTED;
			scn_decision = cluster_visibility_decide_by_scn(m->commit_scn, read_scn);
			break;

		case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
			/*
			 * Retention leg (e), mirroring the requester single-verdict
			 * consumer (cluster_runtime_visibility.c:271): the bound (horizon)
			 * decides against read_scn only when the horizon is not newer than
			 * read_scn;  otherwise the bound is inadmissible -> fail closed.
			 */
			if (!SCN_VALID(read_scn) || !SCN_VALID(m->horizon_scn)
				|| scn_time_cmp(m->horizon_scn, read_scn) > 0)
				return CLUSTER_VISIBILITY_UNKNOWN; /* inadmissible -> UNPROVABLE */
			status = CLUSTER_TT_STATUS_COMMITTED;
			scn_decision = cluster_visibility_decide_by_scn(m->horizon_scn, read_scn);
			break;

		default:
			return CLUSTER_VISIBILITY_UNKNOWN; /* no proven terminal -> UNPROVABLE */
		}

		switch (cluster_vis_cr_xmax_verdict(status, scn_decision)) {
		case CVV_VISIBLE:
			continue; /* this updater does not hide the tuple at read_scn */
		case CVV_INVISIBLE:
			return CLUSTER_VISIBILITY_INVISIBLE; /* committed delete visible -> tuple gone */
		default:
			return CLUSTER_VISIBILITY_UNKNOWN; /* CVV_FAILCLOSED_* -> fail closed */
		}
	}

	/* No updater member hid the tuple -> visible. */
	return CLUSTER_VISIBILITY_VISIBLE;
}

#endif /* USE_PGRAC_CLUSTER */
