/*-------------------------------------------------------------------------
 *
 * cluster.c
 *	  pgrac cluster subsystem top-level entry (stage 0.2 stub).
 *
 *	  This file is the umbrella entry point for the pgrac cluster
 *	  subsystem.  In stage 0.2 it only provides stub functions to
 *	  validate the build-system integration; real cluster logic
 *	  (GRD bootstrap, background process spawning, shared memory
 *	  layout, etc.) lands in later feature points.
 *
 *	  See:
 *	    docs/development-roadmap.md     - 6-stage roadmap
 *	    docs/background-process-design.md - process layout
 *	    specs/spec-0.2-cluster-skeleton.md - this stage's spec
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ or pgrac_ prefix.
 *
 *	  Stage 0.2 deliberately keeps this file minimal: the goal is to
 *	  validate that the new src/backend/cluster/ subtree wires into
 *	  PG's build system correctly, without introducing any cluster
 *	  behavior at runtime.  None of these functions are called from
 *	  postmaster yet (that wiring is stage 0.3).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster.h"
#include "cluster/cluster_elog.h"
#include "utils/elog.h"


/*
 * cluster_init -- Initialize the pgrac cluster subsystem.
 *
 *	Stub implementation for stage 0.2.  Logs at DEBUG1 and returns;
 *	no shared memory is allocated, no background workers are spawned,
 *	and no GRD state is established.
 *
 *	Real initialization lands in stage 0.3+ when this function is
 *	wired into PostmasterMain after CreateSharedMemoryAndSemaphores().
 *
 * Inputs:
 *	(none)
 *
 * Returns:
 *	(void)
 *
 * Side Effects:
 *	None other than emitting a DEBUG1-level log message.
 *
 * NOTES
 *	This function MUST stay safe to call multiple times in stage 0.2,
 *	even though it is not actually invoked.  Future implementations
 *	will need to track an initialized flag to remain idempotent.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_init(void)
{
	cluster_phase = "init";
	CLUSTER_LOG(DEBUG1, "cluster_init: stub, no-op in stage 0.9");
}

/*
 * cluster_shutdown -- Shut down the pgrac cluster subsystem.
 *
 *	Stub implementation for stage 0.2.  Logs at DEBUG1 and returns.
 *
 *	Real shutdown lands in stage 0.3+ when this function is wired
 *	into postmaster shutdown sequence (reverse of Phase 1-4).
 *
 * Inputs:
 *	(none)
 *
 * Returns:
 *	(void)
 *
 * Side Effects:
 *	None other than emitting a DEBUG1-level log message.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_shutdown(void)
{
	cluster_phase = "shutdown";
	CLUSTER_LOG(DEBUG1, "cluster_shutdown: stub, no-op in stage 0.9");
}

/*
 * pgrac_version_string -- moved to cluster_version.c (stage 0.4).
 *
 *	The implementation now lives in src/backend/cluster/cluster_version.c
 *	to allow unit tests to link the version string accessor without
 *	pulling in the full PG backend (cluster_version.c has no PG deps).
 *
 *	The declaration is forwarded via cluster.h -> cluster_version.h so
 *	external callers see no API change.
 */
