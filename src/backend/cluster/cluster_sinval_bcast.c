/*-------------------------------------------------------------------------
 *
 * cluster_sinval_bcast.c
 *	  pgrac cluster SI Broadcaster aux process — spec-2.38 D4.
 *
 *	  Aux process spawned by postmaster Phase 4 (after IC + LMON).
 *	  Main loop:
 *	    (1) apply pending fail-safe SIResetAll if inbound overflowed
 *	    (2) drain ClusterSinvalInbound — SendSharedInvalidMessages locally
 *	    (3) WaitLatch (cluster.sinval_broadcast_batch_timeout_ms)
 *
 *	  Outbound broadcast is LMON-mediated because tier1 TCP fds are LMON
 *	  process-local.  Backends enqueue, LMON drains outbound + fanouts, and
 *	  this aux process applies inbound messages locally.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_sinval_bcast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h" /* cluster_ko_drain_inbound_and_apply (spec-5.7 D6) */
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_sinval_bcast.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"


static void
SinvalBcastBeforeShmemExit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	cluster_sinval_unregister_proc_latch();
}

/*
 * SinvalBcastMain — aux process main entry.  Modeled on LmonMain (spec-1.11)
 * and bgwriter.c / walwriter.c PG aux-process signal layout.
 */
void
SinvalBcastMain(void)
{
	sigjmp_buf local_sigjmp_buf;
	MemoryContext sinval_bcast_context;

	Assert(IsUnderPostmaster);

	MyBackendType = B_SINVAL_BCAST;
	init_ps_display(NULL);

	/* Standard PG aux-process signal layout. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT installed by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Register MyLatch with the cluster_sinval module so backends /
	 * IC handlers can SetLatch to wake us for drain work.
	 */
	cluster_sinval_register_proc_latch(MyLatch);
	before_shmem_exit(SinvalBcastBeforeShmemExit, (Datum)0);

	/*
	 * Work in a dedicated, reset-able memory context so a single drain cycle's
	 * allocations (e.g. the per-relation array FlushRelationsAllBuffers palloc's
	 * for the KO peer-apply drain) cannot accumulate in TopMemoryContext.  Modeled
	 * on bgwriter.c.
	 */
	sinval_bcast_context
		= AllocSetContextCreate(TopMemoryContext, "SI Broadcaster", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(sinval_bcast_context);

	/*
	 * Error recovery: log and continue.  The KO peer-apply drain (spec-5.7 D6)
	 * touches the buffer pool (FlushRelationsAllBuffers / DropRelationsAllBuffers),
	 * so a failed flush can leave buffer pins / LWLocks held; release them with the
	 * bgwriter-style aux cleanup subset rather than the original skeleton's
	 * log-only handler.  A dropped KO request leaves the dropping node to time out
	 * and fail closed (53RAA), which is the correct fail-closed behavior.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		EmitErrorReport();

		/*
		 * Minimal subset of AbortTransaction() -- this aux runs no transactions
		 * but, via the KO drain, holds LWLocks, buffer pins, and smgr/file handles.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		MemoryContextSwitchTo(sinval_bcast_context);
		FlushErrorState();
		MemoryContextResetAndDeleteChildren(sinval_bcast_context);

		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;
	(void)local_sigjmp_buf;

	for (;;) {
		int rc;

		if (ShutdownRequestPending)
			proc_exit(0);

		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Bound per-cycle allocations (the KO drain palloc's). */
		MemoryContextReset(sinval_bcast_context);

		/*
		 * HC136 main loop drain pattern (reuse spec-1.11 LMON):
		 *   (1) inbound overflow → SIResetAll fail-safe
		 *   (2) inbound → SendSharedInvalidMessages
		 *   (3) spec-5.7 D6: KO inbound → flush + drop relfilenode buffers, then ACK
		 *
		 * Outbound fanout (sinval + KO ACK) is drained by LMON, not this process.
		 */
		cluster_sinval_apply_inbound_overflow_reset_if_pending();
		cluster_sinval_drain_inbound_and_apply();
		cluster_ko_drain_inbound_and_apply();

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_sinval_broadcast_batch_timeout_ms, WAIT_EVENT_SINVAL_BROADCAST_SEND);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

#endif /* USE_PGRAC_CLUSTER */
