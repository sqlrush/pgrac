/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  Public interface for the pgrac cluster subsystem (top-level entry).
 *
 *	  This header declares the top-level entry points for the pgrac
 *	  cluster subsystem.  Each subsystem (GRD, GCS, GES, SCN, ...) will
 *	  add its own header under src/include/cluster/<subsys>/ in later
 *	  feature points.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "postgres.h"

/*
 * The pgrac cluster API is only available when configured with
 * --enable-cluster (USE_PGRAC_CLUSTER defined in pg_config.h).
 *
 * Calling these functions when USE_PGRAC_CLUSTER is undefined will
 * result in a compile-time error, which is the intended behavior:
 * cluster code paths must be guarded by `#ifdef USE_PGRAC_CLUSTER`
 * at the call site.
 */
#ifdef USE_PGRAC_CLUSTER

/*
 * Pure version-string accessor (no PG deps).  Defined in
 * cluster_version.c so that unit tests can link it standalone.
 */
#include "cluster/cluster_version.h"

/*
 * cluster_init -- Initialize the pgrac cluster subsystem.
 *
 *	Stub function in stage 0.2.  Real implementation lands in stage 0.3+
 *	when wired into postmaster startup.  See docs/background-process-design.md
 *	§4 (Phase 1-4 startup sequence).
 */
extern void cluster_init(void);

/*
 * cluster_shutdown -- Shut down the pgrac cluster subsystem.
 *
 *	Stub function in stage 0.2.  Real implementation lands in stage 0.3+
 *	when wired into postmaster shutdown.
 */
extern void cluster_shutdown(void);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_H */
