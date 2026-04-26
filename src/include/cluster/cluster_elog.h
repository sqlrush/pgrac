/*-------------------------------------------------------------------------
 *
 * cluster_elog.h
 *	  pgrac cluster logging context wrapper around PostgreSQL's elog().
 *
 *	  This header defines CLUSTER_LOG -- a thin wrapper that prepends
 *	  cluster context (node id, lifecycle phase) to every log line
 *	  emitted by the cluster subsystem.  Output format:
 *
 *	      [cluster node=<N> phase=<S>] <user message>
 *
 *	  Stage 0.9 ships placeholder values:
 *	      cluster_node_id = -1   (will become a GUC in stage 0.13+)
 *	      cluster_phase   = "init" (writer manually transitions)
 *
 *	  Stage 0.13+ will wire cluster_node_id to the cluster_node_id GUC
 *	  and cluster_phase to the postmaster startup phase machine.
 *
 *	  Conventions (CLAUDE.md rule 17):
 *	    DEBUG5..DEBUG1   - tracing, default off
 *	    LOG              - server log only
 *	    INFO/NOTICE      - user-facing
 *	    WARNING          - operation continues, attention warranted
 *	    ERROR            - aborts the current transaction
 *	    FATAL            - terminates the connection
 *	    PANIC            - kills the postmaster
 *
 *	  Use CLUSTER_LOG for cluster-internal lifecycle events.  For
 *	  end-user-facing errors that need a SQLSTATE code, use ereport()
 *	  directly until CLUSTER_REPORT lands in stage 0.12 (paired with
 *	  the SQLSTATE registration framework).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_elog.h
 *
 * NOTES
 *	  This header DOES include postgres.h (transitively via utils/elog.h)
 *	  and is therefore not safe to use from cluster_unit/ tests that
 *	  link only cluster_version.o.  See cluster_version.h for the
 *	  PG-free path used by unit tests.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ELOG_H
#define CLUSTER_ELOG_H

#include "postgres.h"

#include "utils/elog.h"


/*
 * Cluster logging context (placeholder values in stage 0.9).
 *
 * cluster_node_id: -1 == "not configured" (will be a GUC in stage 0.13+)
 * cluster_phase:   short tag describing the current lifecycle phase.
 *                  Common values: "init", "running", "shutdown",
 *                  "reconfig".  NULL is tolerated (rendered "(unset)").
 */
extern int cluster_node_id;
extern const char *cluster_phase;


/*
 * CLUSTER_LOG -- emit a log line with cluster context prefix.
 *
 *	Wrapper around PG's elog(); accepts the same format/args as elog.
 *
 *	Format prepended:
 *	  "[cluster node=<id> phase=<phase>] "
 *
 *	Examples:
 *	  CLUSTER_LOG(DEBUG1, "starting LMS worker %d", worker_id);
 *	  CLUSTER_LOG(LOG, "GRD rebuild complete in %d ms", elapsed);
 *
 *	Implementation: a function-like macro that splices a prefix into
 *	the elog format string.  The prefix is built from the current
 *	values of cluster_node_id and cluster_phase at call time.
 */
#define CLUSTER_LOG(level, ...)                                                                    \
	elog(level, "[cluster node=%d phase=%s] " __VA_ARGS__, cluster_node_id,                        \
		 cluster_phase ? cluster_phase : "(unset)")


#endif /* CLUSTER_ELOG_H */
