/*-------------------------------------------------------------------------
 *
 * cluster_catalog_stats.h
 *	  Shared-catalog observability counters (spec-6.14 D10b).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_catalog_stats.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D10b)
 *
 *	  Monotonic counters for the shared-catalog data plane, backing the
 *	  "catalog" category of cluster_dump_state.  All accessors are
 *	  NULL-safe: before the shmem region attaches (or in cluster_unit
 *	  binaries that never attach shmem) increments are dropped and reads
 *	  return 0.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CATALOG_STATS_H
#define CLUSTER_CATALOG_STATS_H

/* shmem region wiring (registered from cluster_shmem_register_all) */
extern void cluster_catalog_stats_shmem_register(void);

/*
 * Increment points:
 *	 vis_resolve  -- a catalog-page tuple entered the cluster visibility
 *					 resolver (ITL/TT evidence path; rare: only unhinted or
 *					 foreign-evidence tuples reach the resolver).
 *	 vis_unknown  -- a catalog-page tuple resolution ended fail-closed
 *					 (resolver STALE/AMBIGUOUS verdict, or the D8 LOCAL
 *					 snapshot guard); the reader raises 53R97-family ERROR.
 *	 buf_hit/miss -- catalog-page buffer-cache traffic under
 *					 cluster.shared_catalog (a miss is a page sourced from
 *					 shared storage or a cross-node CF transfer).
 */
extern void cluster_catalog_stats_vis_resolve_inc(void);
extern void cluster_catalog_stats_vis_unknown_inc(void);
extern void cluster_catalog_stats_buf_hit_inc(void);
extern void cluster_catalog_stats_buf_miss_inc(void);

/* dump accessors (cluster_debug.c "catalog" category) */
extern uint64 cluster_catalog_stats_vis_resolve_count(void);
extern uint64 cluster_catalog_stats_vis_unknown_count(void);
extern uint64 cluster_catalog_stats_buf_hit_count(void);
extern uint64 cluster_catalog_stats_buf_miss_count(void);

#endif							/* CLUSTER_CATALOG_STATS_H */
