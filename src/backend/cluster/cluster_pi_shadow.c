/*-------------------------------------------------------------------------
 *
 * cluster_pi_shadow.c
 *	  pgrac spec-6.12h D-h3a -- Past Image ship-SCN shadow table.
 *
 *	  One SCN slot per shared buffer (NBuffers x 8B), stamped by
 *	  cluster_bufmgr_convert_to_pi_locked at the instant a buffer is
 *	  converted into a Past Image, and consumed by the D-h3 recovery
 *	  base rebuild.  See cluster_pi_shadow.h for the boundary proof and
 *	  the slot validity contract.  Zero-initialized = InvalidScn in
 *	  every slot (a stale or missing stamp fails closed to
 *	  CLUSTER_PI_GATE_UNUSABLE, never to a wrong boundary).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pi_shadow.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_shmem.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"

SCN *ClusterPiShadow = NULL;

Size
cluster_pi_shadow_shmem_size(void)
{
	return MAXALIGN(mul_size((Size)NBuffers, sizeof(SCN)));
}

void
cluster_pi_shadow_shmem_init(void)
{
	bool found;

	ClusterPiShadow
		= (SCN *)ShmemInitStruct("pgrac cluster pi shadow", cluster_pi_shadow_shmem_size(), &found);
	if (!found)
		memset(ClusterPiShadow, 0, cluster_pi_shadow_shmem_size());
}

static const ClusterShmemRegion cluster_pi_shadow_region = {
	.name = "pgrac cluster pi shadow",
	.size_fn = cluster_pi_shadow_shmem_size,
	.init_fn = cluster_pi_shadow_shmem_init,
	.lwlock_count = 0, /* slots ride the per-buffer header spinlocks */
	.owner_subsys = "cluster_pi_shadow",
	.reserved_flags = 0,
};

void
cluster_pi_shadow_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_pi_shadow_region);
}
