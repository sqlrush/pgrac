/*-------------------------------------------------------------------------
 *
 * cluster_signal.c
 *	  pgrac cluster signal handlers (async-signal-safe).
 *
 *	  Stage 0.15 introduces the cluster procsignal extension by
 *	  registering PROCSIG_CLUSTER_RECONFIG_START in PG's ProcSignalReason
 *	  enum and wiring the matching dispatcher case to the handler in
 *	  this file.  See docs/cluster-signal-design.md and
 *	  specs/spec-0.15-signal-framework.md.
 *
 *	  Handler contract (CLAUDE.md rule 16):
 *
 *	  Signal handlers run in signal context and must be async-signal-safe.
 *	  In practice this restricts each handler to:
 *	    - writing a volatile sig_atomic_t flag
 *	    - calling SetLatch(MyLatch) to wake the main loop
 *	  Anything else (palloc, elog, LWLockAcquire, ...) is forbidden.
 *
 *	  The real processing of a cluster signal -- e.g. pausing new
 *	  transactions on PROCSIG_CLUSTER_RECONFIG_START -- happens later in
 *	  the backend main loop via a ProcessClusterXxxInterrupt() function
 *	  that reads the pending flag.  Stage 0.15 ships only the handlers;
 *	  the matching consumers land in their owning subsystem specs
 *	  (Stage 2.X LMON spec for the reconfig flag).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_signal.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ prefix.  Future cluster
 *	  reasons (RECONFIG_END, FENCE_TRIGGERED, RECOVERY_TRIGGER, ...)
 *	  add their own handler functions here, wired through dispatcher
 *	  cases in src/backend/storage/ipc/procsignal.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h" /* MyLatch */
#include "storage/latch.h"

#include "cluster/cluster_signal.h"


/* ============================================================
 * Per-process pending flags.
 *
 *	Set by handlers in signal context, cleared by the matching
 *	Process<X>Interrupt() function in the backend main loop.
 *	Stage 0.15 has no consumers; readers land in Stage 2.X.
 * ============================================================ */
volatile sig_atomic_t cluster_reconfig_start_pending = false;


/* ============================================================
 * Cluster signal handlers (async-signal-safe).
 * ============================================================ */

/*
 * cluster_handle_reconfig_start_interrupt -- handler for
 *	PROCSIG_CLUSTER_RECONFIG_START.
 *
 *	Sets the pending flag and bumps the latch so the backend main loop
 *	notices on the next CHECK_FOR_INTERRUPTS.  Real reconfig handling
 *	is implemented by ProcessClusterReconfigStartInterrupt(), shipped
 *	in the Stage 2.X LMON spec (drains in-flight transactions and
 *	waits at the reconfig barrier).
 */
void
cluster_handle_reconfig_start_interrupt(void)
{
	cluster_reconfig_start_pending = true;
	SetLatch(MyLatch);
}
