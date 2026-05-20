/*-------------------------------------------------------------------------
 *
 * cluster_sinval_bcast.c
 *	  pgrac cluster SI Broadcaster aux process — spec-2.38 D4.
 *
 *	  Aux process spawned by postmaster Phase 4 (after IC + LMON).
 *	  Main loop:
 *	    (1) drain ClusterSinvalOutbound — broadcast via PGRAC_IC_MSG_SINVAL
 *	    (2) apply pending fail-safe SIResetAll if inbound overflowed
 *	    (3) drain ClusterSinvalInbound — SendSharedInvalidMessages locally
 *	    (4) WaitLatch (cluster.sinval_broadcast_batch_timeout_ms)
 *
 *	  HC139 producer mask:  this aux process is the ONLY context where
 *	  cluster_ic_send_envelope(PGRAC_IC_MSG_SINVAL) is permitted by the
 *	  dispatch table (allowed_producer_mask = CLUSTER_IC_PRODUCER_SINVAL_BCAST).
 *	  Backends invoking cluster_ic_send_envelope with SINVAL msg_type are
 *	  rejected at send time.
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
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_sinval_bcast.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/wait_event.h"


/*
 * SinvalBcastMain — aux process main entry.  Modeled on LmonMain (spec-1.11)
 * and bgwriter.c / walwriter.c PG aux-process signal layout.
 */
void
SinvalBcastMain(void)
{
	sigjmp_buf local_sigjmp_buf;

	Assert(IsUnderPostmaster);

	write_stderr("SI Broadcaster: entered SinvalBcastMain\n");

	MyBackendType = B_SINVAL_BCAST;
	init_ps_display(NULL);
	write_stderr("SI Broadcaster: post init_ps_display\n");

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

	/* Error context for unexpected ereport ERROR — log and continue.
	 * Skeleton scope: minimal error handling (no buffer/lock/smgr cleanup
	 * because aux process performs no DB-level transactions; just ring
	 * buffer + IC send/recv).  Hardening can add full PG aux error
	 * scaffolding when DDL hook lands in spec-2.39. */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
		EmitErrorReport();
		FlushErrorState();
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

		/*
		 * HC136 main loop drain pattern (reuse spec-1.11 LMON):
		 *   (1) outbound → broadcast
		 *   (2) inbound overflow → SIResetAll fail-safe
		 *   (3) inbound → SendSharedInvalidMessages
		 */
		cluster_sinval_drain_outbound_and_broadcast();
		cluster_sinval_apply_inbound_overflow_reset_if_pending();
		cluster_sinval_drain_inbound_and_apply();

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   cluster_sinval_broadcast_batch_timeout_ms, WAIT_EVENT_SINVAL_BROADCAST_SEND);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}
}

#endif /* USE_PGRAC_CLUSTER */
