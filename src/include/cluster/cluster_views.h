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
#define CLUSTER_WAIT_EVENTS_COUNT 46


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


#endif /* CLUSTER_VIEWS_H */
