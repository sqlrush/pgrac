/*-------------------------------------------------------------------------
 *
 * cluster_ges_handoff.c
 *	  pgrac spec-6.12e1 -- GES release-side handoff runtime shim.
 *
 *	  Thin observability + hardening layer over the spec-5.3 D3
 *	  deterministic drain (cluster_grd_release_and_drain).  It does not
 *	  change the grant decision; it counts the drains and, when
 *	  cluster.ges_handoff is on, feeds every drain's flattened snapshot to
 *	  the pure verifier (cluster_ges_handoff_policy.c) so an invariant
 *	  break surfaces as a counter + LOG rather than silent corruption
 *	  (8.A-dual: a double grant is a cross-node write-write conflict).
 *
 *	  Counters live in the shared "pgrac cluster xnode lever" region
 *	  (e1_* fields), dumped through the cluster_dump_state 'xnode_lever'
 *	  category.  With cluster.ges_handoff off the drain path is
 *	  byte-identical (the shim's verify is skipped; only the cheap drain
 *	  counter ticks when profiling is on).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_handoff.c
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
#include "cluster/cluster_guc.h"
#include "cluster/cluster_xnode_lever.h"

bool
cluster_ges_handoff_enabled(void)
{
	return cluster_ges_handoff;
}

/*
 * Record one release drain.  n_granted is always counted (cheap); verdict
 * is the pure verifier's result when the wave GUC armed the check
 * (CLUSTER_GES_HANDOFF_OK otherwise).  A non-OK verdict is an 8.A-dual
 * safety break: bump the violation counter and LOG once so ship-gate /
 * chaos runs catch it (never silently proceed).
 */
void
cluster_ges_handoff_note_drain(int n_granted, ClusterGesHandoffVerdict verdict)
{
	if (ClusterXnodeLeverCtl == NULL)
		return;
	if (!(cluster_ges_handoff || cluster_xnode_profile_enabled))
		return;

	pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->e1_drain_count, 1);
	if (n_granted > 0)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->e1_grant_count, (uint64)n_granted);

	if (verdict != CLUSTER_GES_HANDOFF_OK) {
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->e1_invariant_violation_count, 1);
		elog(LOG,
			 "cluster_ges_handoff: drain invariant violation verdict=%d granted=%d "
			 "(spec-6.12e1 8.A-dual)",
			 (int)verdict, n_granted);
	}
}

uint64
cluster_ges_handoff_grant_count(void)
{
	return ClusterXnodeLeverCtl != NULL ? pg_atomic_read_u64(&ClusterXnodeLeverCtl->e1_grant_count)
										: 0;
}

uint64
cluster_ges_handoff_drain_count(void)
{
	return ClusterXnodeLeverCtl != NULL ? pg_atomic_read_u64(&ClusterXnodeLeverCtl->e1_drain_count)
										: 0;
}

uint64
cluster_ges_handoff_invariant_violation_count(void)
{
	return ClusterXnodeLeverCtl != NULL
			   ? pg_atomic_read_u64(&ClusterXnodeLeverCtl->e1_invariant_violation_count)
			   : 0;
}
