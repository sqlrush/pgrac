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
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (stage 0.30 sweep) */
#include "cluster/cluster_shmem.h"	/* cluster_init_shmem_module (stage 1.3) */
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
	CLUSTER_INJECTION_POINT("cluster-init-top");

	/*
	 * PGRAC: spec-1.10 (2026-05-03) — cluster_init is the logical
	 * Phase 0 (pre-shmem-ready) entry helper.  It does NOT mutate
	 * cluster_phase directly (HC2 SSOT: only cluster_advance_phase()
	 * in cluster_startup_phase.c is permitted to update the legacy
	 * mirror).  Phase 0 transition itself happens later in
	 * PostmasterMain when cluster_run_startup_sequence() advances
	 * from PRE_INIT into CLUSTER_PHASE_0_BASE; cluster_phase mirror
	 * flips to "phase0_base" then.
	 *
	 * Note: cluster_init() is invoked by process_shared_preload_libraries
	 * (miscinit.c), which runs both in PostmasterMain AND in
	 * SubPostmasterMain (EXEC_BACKEND children).  For HC1
	 * compliance, cluster_init must NOT call cluster_advance_phase()
	 * (which is postmaster-only); the actual phase advance is
	 * orchestrated in PostmasterMain only.
	 */
	CLUSTER_LOG(DEBUG1, "cluster_init: registering cluster shmem regions");

	/*
	 * Stage 1.3: register the foundational shmem regions (cluster_ctl +
	 * cluster_conf) into the registry.  Subsystems with their own
	 * cluster_<subsys>_init() helper register their own regions there.
	 * Must run after cluster_init_guc and before cluster_request_shmem.
	 */
	cluster_init_shmem_module();
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
	CLUSTER_INJECTION_POINT("cluster-shutdown-top");

	/*
	 * PGRAC: spec-1.10 (2026-05-03) — direct cluster_phase assignment
	 * removed (HC2 SSOT: only cluster_advance_phase() updates the
	 * legacy mirror).  Real shutdown phase transition happens via
	 * cluster_run_shutdown_sequence() (cluster_startup_phase.c) when
	 * postmaster shutdown is wired up by 1.10 D4 PostmasterMain
	 * PGRAC MODIFICATIONS.  This function remains a stub callable
	 * by tests; the phase transition itself goes through the
	 * authoritative path.
	 */
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
