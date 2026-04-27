/*-------------------------------------------------------------------------
 *
 * procsignal.h
 *	  Routines for interprocess signaling
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procsignal.h
 *
 *-------------------------------------------------------------------------
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  Stage:        0.15
 *
 *	  Extended ProcSignalReason with cluster-specific reasons (currently
 *	  PROCSIG_CLUSTER_RECONFIG_START).  Cluster reasons are appended at
 *	  the end of the enum, guarded by #ifdef USE_PGRAC_CLUSTER, so PG's
 *	  original 13 numeric positions are preserved and --disable-cluster
 *	  builds remain byte-for-byte identical to upstream PG.
 *
 *	  Each cluster reason is dispatched in
 *	  src/backend/storage/ipc/procsignal.c::procsignal_sigusr1_handler
 *	  to a handler in src/backend/cluster/cluster_signal.c.  See
 *	  docs/cluster-signal-design.md and
 *	  specs/spec-0.15-signal-framework.md.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCSIGNAL_H
#define PROCSIGNAL_H

#include "storage/backendid.h"


/*
 * Reasons for signaling a Postgres child process (a backend or an auxiliary
 * process, like checkpointer).  We can cope with concurrent signals for different
 * reasons.  However, if the same reason is signaled multiple times in quick
 * succession, the process is likely to observe only one notification of it.
 * This is okay for the present uses.
 *
 * Also, because of race conditions, it's important that all the signals be
 * defined so that no harm is done if a process mistakenly receives one.
 */
typedef enum
{
	PROCSIG_CATCHUP_INTERRUPT,	/* sinval catchup interrupt */
	PROCSIG_NOTIFY_INTERRUPT,	/* listen/notify interrupt */
	PROCSIG_PARALLEL_MESSAGE,	/* message from cooperating parallel backend */
	PROCSIG_WALSND_INIT_STOPPING,	/* ask walsenders to prepare for shutdown  */
	PROCSIG_BARRIER,			/* global barrier interrupt  */
	PROCSIG_LOG_MEMORY_CONTEXT, /* ask backend to log the memory contexts */
	PROCSIG_PARALLEL_APPLY_MESSAGE, /* Message from parallel apply workers */

	/* Recovery conflict reasons */
	PROCSIG_RECOVERY_CONFLICT_DATABASE,
	PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
	PROCSIG_RECOVERY_CONFLICT_LOCK,
	PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
	PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
	PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
	PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: cluster ProcSignalReasons (stage 0.15+).
	 * Appended after PG-native values to preserve the 0..13 numeric
	 * positions for ABI compatibility.  Handlers live in
	 * src/backend/cluster/cluster_signal.c.  See
	 * docs/cluster-signal-design.md §3 for the full registration
	 * roster and reservation policy.
	 */
	PROCSIG_CLUSTER_RECONFIG_START,	/* LMON: cluster reconfig starting */
#endif

	NUM_PROCSIGNALS				/* Must be last! */
} ProcSignalReason;

typedef enum
{
	PROCSIGNAL_BARRIER_SMGRRELEASE	/* ask smgr to close files */
} ProcSignalBarrierType;

/*
 * prototypes for functions in procsignal.c
 */
extern Size ProcSignalShmemSize(void);
extern void ProcSignalShmemInit(void);

extern void ProcSignalInit(int pss_idx);
extern int	SendProcSignal(pid_t pid, ProcSignalReason reason,
						   BackendId backendId);

extern uint64 EmitProcSignalBarrier(ProcSignalBarrierType type);
extern void WaitForProcSignalBarrier(uint64 generation);
extern void ProcessProcSignalBarrier(void);

extern void procsignal_sigusr1_handler(SIGNAL_ARGS);

#endif							/* PROCSIGNAL_H */
