/*-------------------------------------------------------------------------
 *
 * cluster_views.h
 *	  pgrac cluster system view backing functions (SRF declarations).
 *
 *	  Stage 0.16 introduces the cluster pg_stat_cluster_* view registration
 *	  framework.  Each view is backed by a Set-Returning Function declared
 *	  here and implemented in src/backend/cluster/cluster_views.c.  The
 *	  matching pg_proc.dat entries register the SRFs with PG's catalog,
 *	  and src/backend/catalog/system_views.sql declares the SQL views
 *	  on top of them.
 *
 *	  Stage 0.16 ships ONE view: pg_stat_cluster_wait_events, exposing
 *	  the 46 cluster wait events registered by spec-0.11.  Future views
 *	  (pg_stat_cluster_nodes, pg_stat_cluster_ges_locks, ...) land here
 *	  together with their owning subsystem spec, per the registration
 *	  policy in docs/cluster-views-impl-design.md.
 *
 *	  PG calls these SRFs through fmgr; this header therefore provides
 *	  only the C symbol declarations needed by cluster_views.c itself
 *	  and by unit tests.  The fmgr glue is generated automatically by
 *	  PG_FUNCTION_INFO_V1() in cluster_views.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_views.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VIEWS_H
#define CLUSTER_VIEWS_H

#include "fmgr.h"


/*
 * Number of cluster wait events backing pg_stat_cluster_wait_events.
 *
 *	Equal to the count of values in the WaitEventCluster enum (see
 *	src/include/utils/wait_event.h, registered by spec-0.11).
 *	Anchored as a compile-time constant so unit tests can verify the
 *	internal table in cluster_views.c stays in sync with the enum.
 */
#define CLUSTER_WAIT_EVENTS_COUNT                                                                  \
	103 /* spec-5.18 D12: +1 ReconfigNodeRemoveCleanupWait; was 102 (spec-4.12 D7) */


/*
 * cluster_get_wait_events -- SRF backing pg_stat_cluster_wait_events.
 *
 *	Emits one row per registered cluster wait event:
 *	    type text   -- one of "Cluster: GES", "Cluster: PCM", ...
 *	    name text   -- e.g. "GesEnqueueAcquire"
 *
 *	Stage 0.16 returns the static registration table; runtime call /
 *	wait-time accumulators are added by Stage 1+ subsystem specs once
 *	pgstat_report_wait_start() call sites land in their owning code.
 */
extern Datum cluster_get_wait_events(PG_FUNCTION_ARGS);


/*
 * cluster_get_gcluster_wait_events -- SRF backing pg_stat_gcluster_wait_events.
 *
 *	Emits one row per (node, registered cluster wait event) pair:
 *	    node_id int  -- the node that observed the event (-1 == self,
 *	                    unconfigured)
 *	    type text    -- same as cluster_get_wait_events
 *	    name text    -- same as cluster_get_wait_events
 *
 *	Stage 0.17 returns 46 rows for the local node only; the SRF body is
 *	written so that swapping the inner loop with a real cross-node RPC
 *	fan-out (Stage 6+ AD-007) leaves the column shape unchanged.  The
 *	column contract is a stable interface from 0.17 onward.
 */
extern Datum cluster_get_gcluster_wait_events(PG_FUNCTION_ARGS);


/*
 * cluster_get_stat_nodes -- SRF backing pg_stat_cluster_nodes (stage 0.28).
 *
 *	Returns one row per cluster node with runtime metadata (state /
 *	startup_time / pgrac_version / pg_version) joined with the
 *	configured role from pgrac.conf (looked up by node_id).  Stage 0
 *	always returns exactly one row (the local node, single-node
 *	pseudo-cluster); Stage 2+ extends to all nodes by iterating the
 *	cluster_conf topology.
 *
 *	Output columns (column contract anchor; stable from 0.28 onward):
 *	    node_id       int4
 *	    role          text     -- "primary" / "standby" / "arbiter" / "unknown"
 *	    state         text     -- "online" hardcoded at stage 0
 *	    startup_time  timestamptz
 *	    pgrac_version text
 *	    pg_version    text
 *
 *	See specs/spec-0.28-perfmon-framework.md §2.5 for the full
 *	field-source mapping and stage 1+ extension plan.
 */
extern Datum cluster_get_stat_nodes(PG_FUNCTION_ARGS);


/*
 * cluster_shmem_dump_regions -- SRF backing pg_cluster_shmem (stage 1.3).
 *
 *	Emits one row per registered cluster shmem region:
 *	    name          text     -- region.name (unique within registry)
 *	    size_bytes    int8     -- region.size_fn() at view query time
 *	    lwlock_count  int4     -- region.lwlock_count (informational)
 *	    owner_subsys  text     -- "cluster_ctl" / "cluster_conf" / ...
 *
 *	Stage 1.3 baseline returns 2 rows (cluster_ctl + cluster_conf).
 *	Stage 2+ subsystems appear here automatically as they call
 *	cluster_shmem_register_region from their own init helper.
 *
 *	The column contract is a stable interface from 1.3 onward; future
 *	amends may append columns at the tail (e.g. wait_time / hit_ratio
 *	for stage 4+ performance monitoring) but not change/remove existing
 *	columns.
 *
 *	See specs/spec-1.3-shmem-region-registry.md §2.3 (column contract)
 *	and docs/cluster-shmem-design.md §10 (diagnostic views).
 */
extern Datum cluster_shmem_dump_regions(PG_FUNCTION_ARGS);


#endif /* CLUSTER_VIEWS_H */
