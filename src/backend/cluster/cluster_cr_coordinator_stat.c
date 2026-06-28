/*-------------------------------------------------------------------------
 *
 * cluster_cr_coordinator_stat.c
 *	  pgrac cross-instance CR/current read-path coordinator boundary (spec-5.57).
 *
 *	  Hosts the boundary's two non-data-plane surfaces (see the header):
 *	   - the pure origin classifier (own / merged-materialized remote /
 *	     runtime-warm remote / invalid); and
 *	   - the independent observability counter region for pg_cluster_state's
 *	     'cr_coord' category.
 *
 *	  The GUC backing variables live here (co-located with the subsystem);
 *	  cluster_guc.c registers them in cluster_init_guc().
 *
 *	  This module ships NO data-plane code (AD-013).  The fail-closed 53R9G
 *	  boundary itself lives in the CR walker (cluster_cr.c); this file only
 *	  classifies and counts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.57-cross-instance-cr-current-coordinator.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_coordinator_stat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_coordinator_stat.h"
#include "cluster/cluster_guc.h"			/* cluster_node_id */
#include "cluster/cluster_recovery_merge.h" /* cluster_merged_instance_is_materialized */
#include "cluster/cluster_shmem.h"			/* ClusterShmemRegion, register */
#include "port/atomics.h"
#include "storage/shmem.h" /* ShmemInitStruct */

/*
 * GUC backing variables (spec-5.57 §2.2).  boundary is the default mode; probe
 * defaults off.  These ONLY gate the observability surface, never the
 * fail-closed boundary (8.A non-degradable).
 */
int cluster_cross_instance_cr_coordinator = CR_COORD_MODE_BOUNDARY;
bool cluster_cross_instance_cr_probe = false;

/*
 * ClusterCrCoordStat -- one atomic counter per ClusterCrCoordCounter.
 */
typedef struct ClusterCrCoordStat {
	pg_atomic_uint64 counter[CR_COORD_COUNTER__COUNT];
} ClusterCrCoordStat;

static ClusterCrCoordStat *CRCoordStat = NULL;

/* ===== origin classifier (the boundary's decision surface) ===== */

ClusterCrCoordOriginClass
cluster_cr_coordinator_classify_origin(NodeId origin_node)
{
	/*
	 * An origin that is not a derivable in-membership 0-based owner (e.g.
	 * InvalidNodeId == -1, or an out-of-range owner) is NEVER silently treated
	 * as own/visible -- L10/L69 boundary semantics.
	 */
	if (!SCN_NODE_ID_VALID(origin_node))
		return CR_COORD_ORIGIN_INVALID;

	/* ① own-instance: node 0 is a perfectly legal own id (0-based, not sentinel) */
	if (origin_node == (NodeId)cluster_node_id)
		return CR_COORD_ORIGIN_OWN;

	/* ② merged-materialized remote: undo lives in the local tree (spec-4.5a D8) */
	if (cluster_merged_instance_is_materialized((int)origin_node))
		return CR_COORD_ORIGIN_MATERIALIZED_REMOTE;

	/* ③ runtime-warm remote: the class③ boundary -- data plane is Stage 6 */
	return CR_COORD_ORIGIN_RUNTIME_REMOTE;
}

/* ===== origin(0-based) <-> owner_instance(1-based) namespace (L48) ===== */

int
cluster_cr_coordinator_origin_to_owner_instance(NodeId origin_node)
{
	Assert(origin_node >= 0);
	return (int)origin_node + 1;
}

NodeId
cluster_cr_coordinator_owner_instance_to_origin(int owner_instance)
{
	Assert(owner_instance >= 1);
	return (NodeId)(owner_instance - 1);
}

/* ===== observability counter region ===== */

Size
cluster_cr_coordinator_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCrCoordStat));
}

void
cluster_cr_coordinator_shmem_init(void)
{
	bool found;

	CRCoordStat = (ClusterCrCoordStat *)ShmemInitStruct(
		"ClusterCrCoordStat", cluster_cr_coordinator_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < CR_COORD_COUNTER__COUNT; i++)
			pg_atomic_init_u64(&CRCoordStat->counter[i], 0);
	}
}

static const ClusterShmemRegion cluster_cr_coordinator_region = {
	.name = "pgrac cluster cr coordinator",
	.size_fn = cluster_cr_coordinator_shmem_size,
	.init_fn = cluster_cr_coordinator_shmem_init,
	.lwlock_count = 0, /* lock-free atomic counters */
	.owner_subsys = "cluster_cr_coordinator",
	.reserved_flags = 0,
};

void
cluster_cr_coordinator_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_coordinator_region);
}

void
cluster_cr_coordinator_stat_bump(ClusterCrCoordCounter counter)
{
	if (CRCoordStat == NULL || counter < 0 || counter >= CR_COORD_COUNTER__COUNT)
		return;
	pg_atomic_fetch_add_u64(&CRCoordStat->counter[counter], 1);
}

uint64
cluster_cr_coordinator_stat_count(ClusterCrCoordCounter counter)
{
	if (CRCoordStat == NULL || counter < 0 || counter >= CR_COORD_COUNTER__COUNT)
		return 0;
	return pg_atomic_read_u64(&CRCoordStat->counter[counter]);
}

const char *
cluster_cr_coordinator_counter_key(ClusterCrCoordCounter counter)
{
	switch (counter) {
	case CR_COORD_CROSS_INSTANCE_CR_REFUSED:
		return "cross_instance_cr_refused";
	case CR_COORD_REMOTE_UNDO_READ_REFUSED:
		return "remote_undo_read_refused";
	case CR_COORD_MATERIALIZED_REMOTE_SERVED:
		return "materialized_remote_served";
	case CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE:
		return "cross_instance_boundary_probe";
	case CR_COORD_COUNTER__COUNT:
		break;
	}
	return "unknown";
}
