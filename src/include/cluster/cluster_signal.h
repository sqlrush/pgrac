/*-------------------------------------------------------------------------
 *
 * cluster_signal.h
 *	  pgrac cluster signal handlers and per-process pending flags.
 *
 *	  Stage 0.15 introduces the cluster procsignal extension: PG's
 *	  SIGUSR1 / ProcSignal multiplexer is extended with cluster-specific
 *	  ProcSignalReason values (currently just PROCSIG_CLUSTER_RECONFIG_START),
 *	  and this header declares the matching async-signal-safe handlers
 *	  plus the volatile pending flags they set.
 *
 *	  Why SIGUSR1 (not SIGUSR2):
 *
 *	  The original roadmap entry mentioned "SIGUSR2 cluster 信号" but PG
 *	  already assigns process-specific meanings to SIGUSR2 (walwriter
 *	  wake, archiver wake, autovac launcher wake, postmaster status
 *	  signal, ...).  Multiplexing through SIGUSR2 would conflict with
 *	  those meanings.  PG's only public extension point is SIGUSR1 +
 *	  ProcSignalReason, the same mechanism PG uses internally for
 *	  PROCSIG_RECOVERY_CONFLICT_*.  See docs/cluster-signal-design.md §1.
 *
 *	  Handler contract (CLAUDE.md rule 16):
 *
 *	  - handlers run in signal context; they MUST be async-signal-safe
 *	  - allowed: write volatile sig_atomic_t flags, SetLatch(MyLatch)
 *	  - forbidden: palloc, elog, LWLock, anything calling ProcessInterrupts
 *
 *	  Real processing happens later (typically in CHECK_FOR_INTERRUPTS
 *	  driven Process<X>Interrupt() functions); stage 0.15 only sets
 *	  the flag.  The first reader / consumer of these flags lands in
 *	  Stage 2.X LMON spec.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_signal.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Includes <signal.h> for sig_atomic_t.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIGNAL_H
#define CLUSTER_SIGNAL_H

#include <signal.h> /* sig_atomic_t */


/*
 * cluster_reconfig_start_pending -- per-process flag set by the
 *	PROCSIG_CLUSTER_RECONFIG_START handler.
 *
 *	Volatile sig_atomic_t so the signal handler can write it safely.
 *	Stage 0.15 has no reader; Stage 2.X LMON spec adds the
 *	ProcessClusterReconfigStartInterrupt() consumer in the backend
 *	main loop, which clears the flag and runs the real reconfig
 *	pause / barrier wait logic.
 */
extern volatile sig_atomic_t cluster_reconfig_start_pending;


/*
 * cluster_handle_reconfig_start_interrupt -- async-signal-safe handler
 *	called by procsignal_sigusr1_handler when a backend or cluster
 *	worker receives PROCSIG_CLUSTER_RECONFIG_START.
 *
 *	Sets cluster_reconfig_start_pending = true and SetLatch(MyLatch);
 *	never returns an error, never blocks, never allocates.
 */
extern void cluster_handle_reconfig_start_interrupt(void);


#endif /* CLUSTER_SIGNAL_H */
