/*-------------------------------------------------------------------------
 *
 * cluster_elog.c
 *	  Storage for the pgrac cluster logging context (CLUSTER_LOG macro).
 *
 *	  Defines the placeholder globals that the CLUSTER_LOG macro reads
 *	  every time a cluster log line is emitted:
 *	      cluster_node_id = -1   (will become a GUC in stage 0.13+)
 *	      cluster_phase   = "init"
 *
 *	  Callers are expected to update cluster_phase when crossing
 *	  lifecycle boundaries (e.g. set "running" after cluster_init,
 *	  "shutdown" before cluster_shutdown).  The variable is shared
 *	  process-wide; in the long run it will become per-process state
 *	  populated from the postmaster startup phase machine.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_elog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Globals defined here are intentionally simple in stage 0.9.
 *	  Stage 0.13+ will replace them with GUC-backed state.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_elog.h"


/*
 * Placeholder cluster node id.
 *
 * -1 means "not configured".  Stage 0.13+ will wire this to a
 * cluster_node_id GUC that is set per-instance via postgresql.conf
 * (or pgrac.conf).
 */
int cluster_node_id = -1;


/*
 * Placeholder lifecycle phase tag.
 *
 * Conventional values:
 *   "init"     - initialisation in progress (default at process start)
 *   "running"  - normal steady-state
 *   "shutdown" - shutdown in progress
 *   "reconfig" - cluster reconfiguration in progress
 *
 * Callers update this directly; CLUSTER_LOG dereferences it on every
 * log line.  NULL is tolerated by the macro and rendered as "(unset)".
 */
const char *cluster_phase = "init";
