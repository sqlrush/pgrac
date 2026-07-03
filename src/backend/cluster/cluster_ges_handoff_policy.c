/*-------------------------------------------------------------------------
 *
 * cluster_ges_handoff_policy.c
 *	  pgrac spec-6.12e1 -- GES release-side handoff verifier (pure layer).
 *
 *	  Every function here is pure: it inspects a caller-provided drain
 *	  snapshot with no PostgreSQL runtime dependency (no shmem, no locks,
 *	  no pgstat, no ereport, no palloc).  That lets the three
 *	  safety/liveness invariants of the spec-5.3 D3 deterministic drain be
 *	  model-checked directly by cluster_unit (test_cluster_ges_handoff)
 *	  over adversarial interleavings, independent of the runtime that
 *	  produced the snapshot.
 *
 *	  See cluster_ges_handoff.h for the invariant contract.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_handoff_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ges_handoff.h"
#include "cluster/cluster_ges_mode.h" /* ges_modes_compatible (frozen matrix) */

bool
cluster_ges_handoff_modes_compatible(LOCKMODE a, LOCKMODE b)
{
	if (!ges_mode_is_valid((ClusterGesMode)a) || !ges_mode_is_valid((ClusterGesMode)b))
		return false; /* out of range fails closed */
	return ges_modes_compatible((ClusterGesMode)a, (ClusterGesMode)b);
}

/*
 * no-double-grant: every pair of granted identities is mode-compatible,
 * and every granted identity is compatible with every SURVIVING holder
 * (including the ones the drain just installed for its own grants -- so a
 * granted party appears in both the granted[] and holders[] arrays and
 * must not conflict with a different grant).
 */
static bool
handoff_no_double_grant(const ClusterGesHandoffSnapshot *snap)
{
	int i;
	int j;

	for (i = 0; i < snap->ngranted; i++) {
		for (j = i + 1; j < snap->ngranted; j++) {
			if (!cluster_ges_handoff_modes_compatible(snap->granted[i].mode, snap->granted[j].mode))
				return false;
		}
		for (j = 0; j < snap->nholders; j++) {
			/* a grant's own holder slot is itself; skip identity match */
			if (snap->holders[j].node_id == snap->granted[i].node_id
				&& snap->holders[j].procno == snap->granted[i].procno)
				continue;
			if (!cluster_ges_handoff_modes_compatible(snap->granted[i].mode, snap->holders[j].mode))
				return false;
		}
	}
	return true;
}

/*
 * no-stale-holder: the released identity must not appear among the
 * post-drain holders (remove-before-grant ordering held).
 */
static bool
handoff_no_stale_holder(const ClusterGesHandoffSnapshot *snap)
{
	int i;

	for (i = 0; i < snap->nholders; i++) {
		if (snap->holders[i].node_id == snap->released_node_id
			&& snap->holders[i].procno == snap->released_procno)
			return false;
	}
	return true;
}

/*
 * no-lost-waiter: forward progress.  The drain grants every compatible
 * convert plus one FIFO waiter per release (spec-5.3 D3 one-at-a-time
 * semantics), so a servable waiter legitimately remains when the drain
 * DID grant something -- the next release serves it.  The liveness bug we
 * must catch is a drain that granted NOTHING while a waiter was servable:
 * compatible with every surviving holder and not barriered behind an
 * earlier boosted waiter.  That waiter should have been popped this pass.
 */
static bool
handoff_no_lost_waiter(const ClusterGesHandoffSnapshot *snap)
{
	int w;
	int h;

	if (snap->ngranted > 0)
		return true; /* progress made; one-at-a-time is legitimate */

	for (w = 0; w < snap->nwaiters; w++) {
		bool holder_ok = true;

		if (snap->waiters[w].barriered)
			continue; /* legitimately held behind an earlier boosted */

		for (h = 0; h < snap->nholders; h++) {
			if (!cluster_ges_handoff_modes_compatible(snap->holders[h].mode,
													  snap->waiters[w].mode)) {
				holder_ok = false;
				break;
			}
		}
		if (!holder_ok)
			continue; /* still blocked by a surviving holder */

		/* servable + unblocked + the drain granted nothing -> lost. */
		return false;
	}
	return true;
}

ClusterGesHandoffVerdict
cluster_ges_handoff_verify(const ClusterGesHandoffSnapshot *snap)
{
	if (snap == NULL)
		return CLUSTER_GES_HANDOFF_OK; /* nothing to verify */

	if (!handoff_no_stale_holder(snap))
		return CLUSTER_GES_HANDOFF_STALE_HOLDER;
	if (!handoff_no_double_grant(snap))
		return CLUSTER_GES_HANDOFF_DOUBLE_GRANT;
	if (!handoff_no_lost_waiter(snap))
		return CLUSTER_GES_HANDOFF_LOST_WAITER;
	return CLUSTER_GES_HANDOFF_OK;
}
