/*-------------------------------------------------------------------------
 *
 * cluster_catalog_stats.c
 *	  Shared-catalog observability counters (spec-6.14 D10b).
 *
 *	  A tiny shmem region of monotonic counters for the shared-catalog
 *	  data plane: cluster-path visibility resolutions of catalog-page
 *	  tuples, fail-closed (53R97-family) outcomes, and catalog-page
 *	  buffer-cache traffic.  Backs the "catalog" category of
 *	  cluster_dump_state; every key is live substrate (no
 *	  permanently-dead zeros).
 *
 *	  All increment points sit on rare paths (a resolver invocation
 *	  means an unhinted or foreign-evidence tuple; a buffer miss means
 *	  real I/O), so lock-free pg_atomic bumps suffice.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_catalog_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D10b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_catalog_stats.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"

typedef struct ClusterCatalogStatsShared
{
	pg_atomic_uint64 vis_resolve_count;
	pg_atomic_uint64 vis_unknown_count;
	pg_atomic_uint64 buf_hit_count;
	pg_atomic_uint64 buf_miss_count;
} ClusterCatalogStatsShared;

/* NULL until cluster_catalog_stats_shmem_init; accessors are NULL-safe. */
static ClusterCatalogStatsShared *catalog_stats = NULL;

/* ---- shmem region wiring ---------------------------------------------- */

static Size
cluster_catalog_stats_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCatalogStatsShared));
}

static void
cluster_catalog_stats_shmem_init(void)
{
	bool		found;

	catalog_stats = (ClusterCatalogStatsShared *)
		ShmemInitStruct("pgrac cluster catalog stats",
						MAXALIGN(sizeof(ClusterCatalogStatsShared)), &found);

	if (!IsUnderPostmaster)
	{
		pg_atomic_init_u64(&catalog_stats->vis_resolve_count, 0);
		pg_atomic_init_u64(&catalog_stats->vis_unknown_count, 0);
		pg_atomic_init_u64(&catalog_stats->buf_hit_count, 0);
		pg_atomic_init_u64(&catalog_stats->buf_miss_count, 0);
	}
}

static const ClusterShmemRegion cluster_catalog_stats_region = {
	.name = "pgrac cluster catalog stats",
	.size_fn = cluster_catalog_stats_shmem_size,
	.init_fn = cluster_catalog_stats_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-6.14 D10b shared-catalog counters",
};

void
cluster_catalog_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_catalog_stats_region);
}

/* ---- increment points -------------------------------------------------- */

void
cluster_catalog_stats_vis_resolve_inc(void)
{
	if (catalog_stats != NULL)
		pg_atomic_fetch_add_u64(&catalog_stats->vis_resolve_count, 1);
}

void
cluster_catalog_stats_vis_unknown_inc(void)
{
	if (catalog_stats != NULL)
		pg_atomic_fetch_add_u64(&catalog_stats->vis_unknown_count, 1);
}

void
cluster_catalog_stats_buf_hit_inc(void)
{
	if (catalog_stats != NULL)
		pg_atomic_fetch_add_u64(&catalog_stats->buf_hit_count, 1);
}

void
cluster_catalog_stats_buf_miss_inc(void)
{
	if (catalog_stats != NULL)
		pg_atomic_fetch_add_u64(&catalog_stats->buf_miss_count, 1);
}

/* ---- dump accessors ---------------------------------------------------- */

uint64
cluster_catalog_stats_vis_resolve_count(void)
{
	return catalog_stats != NULL
		? pg_atomic_read_u64(&catalog_stats->vis_resolve_count) : 0;
}

uint64
cluster_catalog_stats_vis_unknown_count(void)
{
	return catalog_stats != NULL
		? pg_atomic_read_u64(&catalog_stats->vis_unknown_count) : 0;
}

uint64
cluster_catalog_stats_buf_hit_count(void)
{
	return catalog_stats != NULL
		? pg_atomic_read_u64(&catalog_stats->buf_hit_count) : 0;
}

uint64
cluster_catalog_stats_buf_miss_count(void)
{
	return catalog_stats != NULL
		? pg_atomic_read_u64(&catalog_stats->buf_miss_count) : 0;
}
