/*-------------------------------------------------------------------------
 * auxprocess.c
 *	  functions related to auxiliary processes.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/auxprocess.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <signal.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/auxprocess.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "postmaster/walwriter.h"
#include "replication/walreceiver.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_diag.h"		  /* DiagMain (stage 1.13 Sprint A) */
#include "cluster/cluster_lck.h"		  /* LckMain (stage 1.12 Sprint A) */
#include "cluster/cluster_lmon.h"		  /* LmonMain (stage 1.11 Sprint A) */
#include "cluster/cluster_cssd.h"		  /* CssdMain (stage 2.5 Sprint A) */
#include "cluster/cluster_qvotec.h"		  /* ClusterQvotecMain (spec-2.6 Sprint A Step 3 D7) */
#include "cluster/cluster_lms.h"		  /* LmsMain (spec-2.18 Sprint A Step 1) */
#include "cluster/cluster_lmd.h"		  /* LmdMain (spec-2.19 Sprint A Step 1) */
#include "cluster/cluster_sinval_bcast.h" /* SinvalBcastMain (spec-2.38 D4) */
#include "cluster/cluster_stats.h"		  /* ClusterStatsMain (stage 1.14 Sprint A) */
#endif


static void ShutdownAuxiliaryProcess(int code, Datum arg);


/* ----------------
 *		global variables
 * ----------------
 */

AuxProcType MyAuxProcType = NotAnAuxProcess; /* declared in miscadmin.h */


/*
 *	 AuxiliaryProcessMain
 *
 *	 The main entry point for auxiliary processes, such as the bgwriter,
 *	 walwriter, walreceiver, bootstrapper and the shared memory checker code.
 *
 *	 This code is here just because of historical reasons.
 */
void
AuxiliaryProcessMain(AuxProcType auxtype)
{
	Assert(IsUnderPostmaster);

	MyAuxProcType = auxtype;

	switch (MyAuxProcType) {
	case StartupProcess:
		MyBackendType = B_STARTUP;
		break;
	case ArchiverProcess:
		MyBackendType = B_ARCHIVER;
		break;
	case BgWriterProcess:
		MyBackendType = B_BG_WRITER;
		break;
	case CheckpointerProcess:
		MyBackendType = B_CHECKPOINTER;
		break;
	case WalWriterProcess:
		MyBackendType = B_WAL_WRITER;
		break;
	case WalReceiverProcess:
		MyBackendType = B_WAL_RECEIVER;
		break;
#ifdef USE_PGRAC_CLUSTER
	/* PGRAC (stage 1.11 Sprint A): LMON aux process. */
	case LmonProcess:
		MyBackendType = B_LMON;
		break;
	/* PGRAC (stage 1.12 Sprint A): LCK aux process. */
	case LckProcess:
		MyBackendType = B_LCK;
		break;
	/* PGRAC (stage 1.13 Sprint A): DIAG aux process. */
	case DiagProcess:
		MyBackendType = B_DIAG;
		break;
	/* PGRAC (stage 1.14 Sprint A): Cluster Stats aux process. */
	case ClusterStatsProcess:
		MyBackendType = B_CLUSTER_STATS;
		break;
	/* PGRAC (stage 2.5 Sprint A): CSSD aux process. */
	case CssdProcess:
		MyBackendType = B_CSSD;
		break;
	/* PGRAC (stage 2.6 Sprint A Step 3 D7): QVOTEC aux process. */
	case QvotecProcess:
		MyBackendType = B_QVOTEC;
		break;
	/* PGRAC (spec-2.18 Sprint A Step 1): LMS aux process. */
	case LmsProcess:
		MyBackendType = B_LMS;
		break;
	/*
	 * PGRAC (spec-2.19 Sprint A Step 1): LMD aux process.  B_LMD
	 * BackendType already exists (spec-1.10 backend types extension); D3
	 * reuses existing surface without duplicate definition.  v0.3 codex
	 * P1.7 audit (grep linkdb confirms miscadmin.h:375 B_LMD already
	 * present + miscinit.c:357 GetBackendTypeDesc("lmd") already mapped).
	 */
	case LmdProcess:
		MyBackendType = B_LMD;
		break;
	/*
	 * spec-2.38 D4: SI Broadcaster (Shared Invalidation Broadcaster)
	 * aux process — drains ClusterSinvalOutbound and broadcasts via
	 * PGRAC_IC_MSG_SINVAL; drains ClusterSinvalInbound and applies
	 * SendSharedInvalidMessages; executes fail-safe SIResetAll on
	 * inbound overflow.  Producer mask CLUSTER_IC_PRODUCER_SINVAL_BCAST
	 * (HC139); backends MUST enqueue via cluster_sinval_enqueue_batch.
	 */
	case SinvalBcastProcess:
		MyBackendType = B_SINVAL_BCAST;
		break;
#endif
	default:
		elog(PANIC, "unrecognized process type: %d", (int)MyAuxProcType);
		MyBackendType = B_INVALID;
	}

	init_ps_display(NULL);

	SetProcessingMode(BootstrapProcessing);
	IgnoreSystemIndexes = true;

	/*
	 * As an auxiliary process, we aren't going to do the full InitPostgres
	 * pushups, but there are a couple of things that need to get lit up even
	 * in an auxiliary process.
	 */

	/*
	 * Create a PGPROC so we can use LWLocks.  In the EXEC_BACKEND case, this
	 * was already done by SubPostmasterMain().
	 */
#ifndef EXEC_BACKEND
	InitAuxiliaryProcess();
#endif

	BaseInit();

	/*
	 * Assign the ProcSignalSlot for an auxiliary process.  Since it doesn't
	 * have a BackendId, the slot is statically allocated based on the
	 * auxiliary process type (MyAuxProcType).  Backends use slots indexed in
	 * the range from 1 to MaxBackends (inclusive), so we use MaxBackends +
	 * AuxProcType + 1 as the index of the slot for an auxiliary process.
	 *
	 * This will need rethinking if we ever want more than one of a particular
	 * auxiliary process type.
	 */
	/*
	 * PGRAC: spec-2.18 Sprint A LMS skeleton + spec-2.19 Sprint A LMD
	 * skeleton do not consume PG ProcSignal reasons yet.  Registering them
	 * in ProcSignal makes proc_exit() run CleanupProcSignalState(), whose
	 * pss_barrierCV broadcast can spin during fast shutdown on these
	 * no-work skeletons.  Keep both on the ordinary aux-process PGPROC/latch
	 * path, but leave ProcSignal opt-in to the later production spec that
	 * wires real BAST/CANCEL SIGUSR1 handling.  I14 invariant (spec-2.19
	 * §1.4 P1.4):LMD skeleton MUST NOT register ProcSignal slot;若 spec-2.20+
	 * 需引入必同 spec ship 完整 register + cleanup + shutdown 语义 (L114
	 * producer-consumer lifecycle 闭环 family).
	 */
#ifdef USE_PGRAC_CLUSTER
	if (MyAuxProcType != LmsProcess && MyAuxProcType != LmdProcess
		&& MyAuxProcType != SinvalBcastProcess)
#endif
		ProcSignalInit(MaxBackends + MyAuxProcType + 1);

	/*
	 * Auxiliary processes don't run transactions, but they may need a
	 * resource owner anyway to manage buffer pins acquired outside
	 * transactions (and, perhaps, other things in future).
	 */
	CreateAuxProcessResourceOwner();


	/* Initialize backend status information */
	pgstat_beinit();
#ifdef USE_PGRAC_CLUSTER
	/*
	 * LMS / LMD become externally visible in pg_stat_activity at
	 * pgstat_bestart(), while their regular signal setup lives in
	 * LmsMain() / LmdMain().  Install the same minimal handlers before
	 * publishing backend status so fast shutdown cannot lose SIGTERM in the
	 * pgstat-visible / pre-LmsMain (resp. pre-LmdMain) window.  HC7 in
	 * spec-2.19 §1.4.8 / L121 (spec-2.18 F1 root fix 14cd35c027):handler
	 * install + sigprocmask UnBlockSig MUST happen BEFORE pgstat_bestart /
	 * pgstat-visible publication / any LMS/LMD shmem-LWLock-CV operation.
	 */
	if (MyAuxProcType == LmsProcess || MyAuxProcType == LmdProcess
		|| MyAuxProcType == SinvalBcastProcess) {
		pqsignal(SIGHUP, SignalHandlerForConfigReload);
		pqsignal(SIGINT, SignalHandlerForShutdownRequest);
		pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
		pqsignal(SIGALRM, SIG_IGN);
		pqsignal(SIGPIPE, SIG_IGN);
		/*
		 * LMS / LMD deliberately skip ProcSignalInit() in this skeleton
		 * phase.  Installing procsignal_sigusr1_handler before a slot exists
		 * makes any startup-window SIGUSR1 dereference uninitialized state.
		 * Keep SIGUSR1 ignored until the production spec registers the full
		 * ProcSignal lifecycle.
		 */
		pqsignal(SIGUSR1, SIG_IGN);
		pqsignal(SIGUSR2, SIG_IGN);
		pqsignal(SIGCHLD, SIG_DFL);
		sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
	}
#endif
	pgstat_bestart();

	/* register a before-shutdown callback for LWLock cleanup */
	before_shmem_exit(ShutdownAuxiliaryProcess, 0);

	SetProcessingMode(NormalProcessing);

	switch (MyAuxProcType) {
	case StartupProcess:
		StartupProcessMain();
		proc_exit(1);

	case ArchiverProcess:
		PgArchiverMain();
		proc_exit(1);

	case BgWriterProcess:
		BackgroundWriterMain();
		proc_exit(1);

	case CheckpointerProcess:
		CheckpointerMain();
		proc_exit(1);

	case WalWriterProcess:
		WalWriterMain();
		proc_exit(1);

	case WalReceiverProcess:
		WalReceiverMain();
		proc_exit(1);

#ifdef USE_PGRAC_CLUSTER
	/*
		 * PGRAC (stage 1.11 Sprint A): LMON aux process dispatch.
		 * LmonMain is pg_attribute_noreturn() (proc_exit on shutdown);
		 * the proc_exit(1) below is a defensive bailout in case the
		 * compiler does not honor the attribute.
		 *
		 * Spec: spec-1.11-lmon-skeleton.md Sprint A D3
		 */
	case LmonProcess:
		LmonMain();
		proc_exit(1);
	/* PGRAC (stage 1.12 Sprint A): LCK aux process dispatch. */
	case LckProcess:
		LckMain();
		proc_exit(1);
	/* PGRAC (stage 1.13 Sprint A): DIAG aux process dispatch. */
	case DiagProcess:
		DiagMain();
		proc_exit(1);
	/* PGRAC (stage 1.14 Sprint A): Cluster Stats aux process dispatch. */
	case ClusterStatsProcess:
		ClusterStatsMain();
		proc_exit(1);
	/* PGRAC (stage 2.5 Sprint A): CSSD aux process dispatch. */
	case CssdProcess:
		CssdMain();
		proc_exit(1);
	/* PGRAC (stage 2.6 Sprint A Step 3 D7): QVOTEC aux process dispatch.
	 * ClusterQvotecMain is pg_attribute_noreturn() (proc_exit on shutdown);
	 * the proc_exit(1) below is a defensive bailout in case the
	 * compiler does not honor the attribute.
	 *
	 * Spec: spec-2.6-voting-disk-quorum-lite.md Sprint A Step 3 D7. */
	case QvotecProcess:
		ClusterQvotecMain();
		proc_exit(1);
	/* PGRAC (spec-2.18 Sprint A Step 1): LMS aux process dispatch.  LmsMain
	 * is pg_attribute_noreturn(); proc_exit(1) below is a defensive bailout
	 * if the compiler does not honor the attribute. */
	case LmsProcess:
		LmsMain();
		proc_exit(1);
	/* PGRAC (spec-2.19 Sprint A Step 1): LMD aux process dispatch.  LmdMain
	 * is pg_attribute_noreturn(); proc_exit(1) below is a defensive bailout
	 * if the compiler does not honor the attribute.  See cluster_lmd.h. */
	case LmdProcess:
		LmdMain();
		proc_exit(1);
	/* PGRAC (spec-2.38 D4): SI Broadcaster aux process dispatch.
	 * SinvalBcastMain is pg_attribute_noreturn();  proc_exit(1) below is
	 * a defensive bailout if the compiler does not honor the attribute.
	 * See cluster_sinval_bcast.h. */
	case SinvalBcastProcess:
		write_stderr("AUXPROCESS DISPATCH: SinvalBcastProcess case hit\n");
		SinvalBcastMain();
		proc_exit(1);
#endif

	default:
		elog(PANIC, "unrecognized process type: %d", (int)MyAuxProcType);
		proc_exit(1);
	}
}

/*
 * Begin shutdown of an auxiliary process.  This is approximately the equivalent
 * of ShutdownPostgres() in postinit.c.  We can't run transactions in an
 * auxiliary process, so most of the work of AbortTransaction() is not needed,
 * but we do need to make sure we've released any LWLocks we are holding.
 * (This is only critical during an error exit.)
 */
static void
ShutdownAuxiliaryProcess(int code, Datum arg)
{
	LWLockReleaseAll();
	ConditionVariableCancelSleep();
	pgstat_report_wait_end();
}
