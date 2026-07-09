/*-------------------------------------------------------------------------
 *
 * miscadmin.h
 *	  This file contains general postgres administration and initialization
 *	  stuff that used to be spread out between the following files:
 *		globals.h						global variables
 *		pdir.h							directory path crud
 *		pinit.h							postgres initialization
 *		pmod.h							processing modes
 *	  Over time, this has also become the preferred place for widely known
 *	  resource-limitation stuff, such as work_mem and check_stack_depth().
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/miscadmin.h
 *
 * NOTES
 *	  some of the information in this file should be moved to other files.
 *
 *-------------------------------------------------------------------------
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  Stage:        0.10
 *
 *	  Extended BackendType enum with pgrac cluster background process
 *	  types (B_CLUSTER_STATS .. B_RFS), appended after the 13
 *	  PG-native values to preserve PG ABI for the original numeric
 *	  positions.  BACKEND_NUM_TYPES is anchored on the final pgrac value.
 *
 *	  Stage 0.10 only registers the enum identifiers; postmaster fork
 *	  paths for these processes land in stage 0.13+ (ProcessAux + GUC).
 *	  No #ifdef USE_PGRAC_CLUSTER guard around the new values: keeping the
 *	  identifiers always defined avoids #ifdef sprawl across every site
 *	  that references BackendType (matching PG's treatment of
 *	  B_AUTOVAC_LAUNCHER even when autovacuum is off).
 *
 *	  Related design:
 *	    docs/background-process-design.md §8.2  (the 14-process roster)
 *	    docs/background-process-design.md §8.3  (ps display names)
 *	    specs/spec-0.10-backend-type-extension.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef MISCADMIN_H
#define MISCADMIN_H

#include <signal.h>

#include "datatype/timestamp.h" /* for TimestampTz */
#include "pgtime.h"				/* for pg_time_t */


#define InvalidPid (-1)


/*****************************************************************************
 *	  System interrupt and critical section handling
 *
 * There are two types of interrupts that a running backend needs to accept
 * without messing up its state: QueryCancel (SIGINT) and ProcDie (SIGTERM).
 * In both cases, we need to be able to clean up the current transaction
 * gracefully, so we can't respond to the interrupt instantaneously ---
 * there's no guarantee that internal data structures would be self-consistent
 * if the code is interrupted at an arbitrary instant.  Instead, the signal
 * handlers set flags that are checked periodically during execution.
 *
 * The CHECK_FOR_INTERRUPTS() macro is called at strategically located spots
 * where it is normally safe to accept a cancel or die interrupt.  In some
 * cases, we invoke CHECK_FOR_INTERRUPTS() inside low-level subroutines that
 * might sometimes be called in contexts that do *not* want to allow a cancel
 * or die interrupt.  The HOLD_INTERRUPTS() and RESUME_INTERRUPTS() macros
 * allow code to ensure that no cancel or die interrupt will be accepted,
 * even if CHECK_FOR_INTERRUPTS() gets called in a subroutine.  The interrupt
 * will be held off until CHECK_FOR_INTERRUPTS() is done outside any
 * HOLD_INTERRUPTS() ... RESUME_INTERRUPTS() section.
 *
 * There is also a mechanism to prevent query cancel interrupts, while still
 * allowing die interrupts: HOLD_CANCEL_INTERRUPTS() and
 * RESUME_CANCEL_INTERRUPTS().
 *
 * Note that ProcessInterrupts() has also acquired a number of tasks that
 * do not necessarily cause a query-cancel-or-die response.  Hence, it's
 * possible that it will just clear InterruptPending and return.
 *
 * INTERRUPTS_PENDING_CONDITION() can be checked to see whether an
 * interrupt needs to be serviced, without trying to do so immediately.
 * Some callers are also interested in INTERRUPTS_CAN_BE_PROCESSED(),
 * which tells whether ProcessInterrupts is sure to clear the interrupt.
 *
 * Special mechanisms are used to let an interrupt be accepted when we are
 * waiting for a lock or when we are waiting for command input (but, of
 * course, only if the interrupt holdoff counter is zero).  See the
 * related code for details.
 *
 * A lost connection is handled similarly, although the loss of connection
 * does not raise a signal, but is detected when we fail to write to the
 * socket. If there was a signal for a broken connection, we could make use of
 * it by setting ClientConnectionLost in the signal handler.
 *
 * A related, but conceptually distinct, mechanism is the "critical section"
 * mechanism.  A critical section not only holds off cancel/die interrupts,
 * but causes any ereport(ERROR) or ereport(FATAL) to become ereport(PANIC)
 * --- that is, a system-wide reset is forced.  Needless to say, only really
 * *critical* code should be marked as a critical section!	Currently, this
 * mechanism is only used for XLOG-related code.
 *
 *****************************************************************************/

/* in globals.c */
/* these are marked volatile because they are set by signal handlers: */
extern PGDLLIMPORT volatile sig_atomic_t InterruptPending;
extern PGDLLIMPORT volatile sig_atomic_t QueryCancelPending;
extern PGDLLIMPORT volatile sig_atomic_t ProcDiePending;
extern PGDLLIMPORT volatile sig_atomic_t IdleInTransactionSessionTimeoutPending;
extern PGDLLIMPORT volatile sig_atomic_t IdleSessionTimeoutPending;
extern PGDLLIMPORT volatile sig_atomic_t ProcSignalBarrierPending;
extern PGDLLIMPORT volatile sig_atomic_t LogMemoryContextPending;
extern PGDLLIMPORT volatile sig_atomic_t IdleStatsUpdateTimeoutPending;

extern PGDLLIMPORT volatile sig_atomic_t CheckClientConnectionPending;
extern PGDLLIMPORT volatile sig_atomic_t ClientConnectionLost;

/* these are marked volatile because they are examined by signal handlers: */
extern PGDLLIMPORT volatile uint32 InterruptHoldoffCount;
extern PGDLLIMPORT volatile uint32 QueryCancelHoldoffCount;
extern PGDLLIMPORT volatile uint32 CritSectionCount;

/* in tcop/postgres.c */
extern void ProcessInterrupts(void);

/* Test whether an interrupt is pending */
#ifndef WIN32
#define INTERRUPTS_PENDING_CONDITION() (unlikely(InterruptPending))
#else
#define INTERRUPTS_PENDING_CONDITION()                                                             \
	(unlikely(UNBLOCKED_SIGNAL_QUEUE()) ? pgwin32_dispatch_queued_signals() : (void)0,             \
	 unlikely(InterruptPending))
#endif

/* Service interrupt, if one is pending and it's safe to service it now */
#define CHECK_FOR_INTERRUPTS()                                                                     \
	do {                                                                                           \
		if (INTERRUPTS_PENDING_CONDITION())                                                        \
			ProcessInterrupts();                                                                   \
	} while (0)

/* Is ProcessInterrupts() guaranteed to clear InterruptPending? */
#define INTERRUPTS_CAN_BE_PROCESSED()                                                              \
	(InterruptHoldoffCount == 0 && CritSectionCount == 0 && QueryCancelHoldoffCount == 0)

#define HOLD_INTERRUPTS() (InterruptHoldoffCount++)

#define RESUME_INTERRUPTS()                                                                        \
	do {                                                                                           \
		Assert(InterruptHoldoffCount > 0);                                                         \
		InterruptHoldoffCount--;                                                                   \
	} while (0)

#define HOLD_CANCEL_INTERRUPTS() (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS()                                                                 \
	do {                                                                                           \
		Assert(QueryCancelHoldoffCount > 0);                                                       \
		QueryCancelHoldoffCount--;                                                                 \
	} while (0)

#define START_CRIT_SECTION() (CritSectionCount++)

#define END_CRIT_SECTION()                                                                         \
	do {                                                                                           \
		Assert(CritSectionCount > 0);                                                              \
		CritSectionCount--;                                                                        \
	} while (0)


/*****************************************************************************
 *	  globals.h --															 *
 *****************************************************************************/

/*
 * from utils/init/globals.c
 */
extern PGDLLIMPORT pid_t PostmasterPid;
extern PGDLLIMPORT bool IsPostmasterEnvironment;
extern PGDLLIMPORT bool IsUnderPostmaster;
extern PGDLLIMPORT bool IsBackgroundWorker;
extern PGDLLIMPORT bool IsBinaryUpgrade;

extern PGDLLIMPORT bool ExitOnAnyError;

extern PGDLLIMPORT char *DataDir;
extern PGDLLIMPORT int data_directory_mode;

extern PGDLLIMPORT int NBuffers;
extern PGDLLIMPORT int MaxBackends;
extern PGDLLIMPORT int MaxConnections;
extern PGDLLIMPORT int max_worker_processes;
extern PGDLLIMPORT int max_parallel_workers;

extern PGDLLIMPORT int MyProcPid;
extern PGDLLIMPORT pg_time_t MyStartTime;
extern PGDLLIMPORT TimestampTz MyStartTimestamp;
extern PGDLLIMPORT struct Port *MyProcPort;
extern PGDLLIMPORT struct Latch *MyLatch;
extern PGDLLIMPORT int32 MyCancelKey;
extern PGDLLIMPORT int MyPMChildSlot;

extern PGDLLIMPORT char OutputFileName[];
extern PGDLLIMPORT char my_exec_path[];
extern PGDLLIMPORT char pkglib_path[];

#ifdef EXEC_BACKEND
extern PGDLLIMPORT char postgres_exec_path[];
#endif

/*
 * done in storage/backendid.h for now.
 *
 * extern BackendId    MyBackendId;
 */
extern PGDLLIMPORT Oid MyDatabaseId;

extern PGDLLIMPORT Oid MyDatabaseTableSpace;

/*
 * Date/Time Configuration
 *
 * DateStyle defines the output formatting choice for date/time types:
 *	USE_POSTGRES_DATES specifies traditional Postgres format
 *	USE_ISO_DATES specifies ISO-compliant format
 *	USE_SQL_DATES specifies Oracle/Ingres-compliant format
 *	USE_GERMAN_DATES specifies German-style dd.mm/yyyy
 *
 * DateOrder defines the field order to be assumed when reading an
 * ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
 * year field first, is taken to be ambiguous):
 *	DATEORDER_YMD specifies field order yy-mm-dd
 *	DATEORDER_DMY specifies field order dd-mm-yy ("European" convention)
 *	DATEORDER_MDY specifies field order mm-dd-yy ("US" convention)
 *
 * In the Postgres and SQL DateStyles, DateOrder also selects output field
 * order: day comes before month in DMY style, else month comes before day.
 *
 * The user-visible "DateStyle" run-time parameter subsumes both of these.
 */

/* valid DateStyle values */
#define USE_POSTGRES_DATES 0
#define USE_ISO_DATES 1
#define USE_SQL_DATES 2
#define USE_GERMAN_DATES 3
#define USE_XSD_DATES 4

/* valid DateOrder values */
#define DATEORDER_YMD 0
#define DATEORDER_DMY 1
#define DATEORDER_MDY 2

extern PGDLLIMPORT int DateStyle;
extern PGDLLIMPORT int DateOrder;

/*
 * IntervalStyles
 *	 INTSTYLE_POSTGRES			   Like Postgres < 8.4 when DateStyle = 'iso'
 *	 INTSTYLE_POSTGRES_VERBOSE	   Like Postgres < 8.4 when DateStyle != 'iso'
 *	 INTSTYLE_SQL_STANDARD		   SQL standard interval literals
 *	 INTSTYLE_ISO_8601			   ISO-8601-basic formatted intervals
 */
#define INTSTYLE_POSTGRES 0
#define INTSTYLE_POSTGRES_VERBOSE 1
#define INTSTYLE_SQL_STANDARD 2
#define INTSTYLE_ISO_8601 3

extern PGDLLIMPORT int IntervalStyle;

#define MAXTZLEN 10 /* max TZ name len, not counting tr. null */

extern PGDLLIMPORT bool enableFsync;
extern PGDLLIMPORT bool allowSystemTableMods;
extern PGDLLIMPORT int work_mem;
extern PGDLLIMPORT double hash_mem_multiplier;
extern PGDLLIMPORT int maintenance_work_mem;
extern PGDLLIMPORT int max_parallel_maintenance_workers;

/*
 * Upper and lower hard limits for the buffer access strategy ring size
 * specified by the VacuumBufferUsageLimit GUC and BUFFER_USAGE_LIMIT option
 * to VACUUM and ANALYZE.
 */
#define MIN_BAS_VAC_RING_SIZE_KB 128
#define MAX_BAS_VAC_RING_SIZE_KB (16 * 1024 * 1024)

extern PGDLLIMPORT int VacuumBufferUsageLimit;
extern PGDLLIMPORT int VacuumCostPageHit;
extern PGDLLIMPORT int VacuumCostPageMiss;
extern PGDLLIMPORT int VacuumCostPageDirty;
extern PGDLLIMPORT int VacuumCostLimit;
extern PGDLLIMPORT double VacuumCostDelay;

extern PGDLLIMPORT int64 VacuumPageHit;
extern PGDLLIMPORT int64 VacuumPageMiss;
extern PGDLLIMPORT int64 VacuumPageDirty;

extern PGDLLIMPORT int VacuumCostBalance;
extern PGDLLIMPORT bool VacuumCostActive;


/* in tcop/postgres.c */

typedef char *pg_stack_base_t;

extern pg_stack_base_t set_stack_base(void);
extern void restore_stack_base(pg_stack_base_t base);
extern void check_stack_depth(void);
extern bool stack_is_too_deep(void);

/* in tcop/utility.c */
extern void PreventCommandIfReadOnly(const char *cmdname);
extern void PreventCommandIfParallelMode(const char *cmdname);
extern void PreventCommandDuringRecovery(const char *cmdname);

/* in utils/misc/guc_tables.c */
extern PGDLLIMPORT int trace_recovery_messages;
extern int trace_recovery(int trace_level);

/*****************************************************************************
 *	  pdir.h --																 *
 *			POSTGRES directory path definitions.                             *
 *****************************************************************************/

/* flags to be OR'd to form sec_context */
#define SECURITY_LOCAL_USERID_CHANGE 0x0001
#define SECURITY_RESTRICTED_OPERATION 0x0002
#define SECURITY_NOFORCE_RLS 0x0004

extern PGDLLIMPORT char *DatabasePath;

/* now in utils/init/miscinit.c */
extern void InitPostmasterChild(void);
extern void InitStandaloneProcess(const char *argv0);
extern void InitProcessLocalLatch(void);
extern void SwitchToSharedLatch(void);
extern void SwitchBackToLocalLatch(void);

typedef enum BackendType {
	B_INVALID = 0,
	B_ARCHIVER,
	B_AUTOVAC_LAUNCHER,
	B_AUTOVAC_WORKER,
	B_BACKEND,
	B_BG_WORKER,
	B_BG_WRITER,
	B_CHECKPOINTER,
	B_LOGGER,
	B_STANDALONE_BACKEND,
	B_STARTUP,
	B_WAL_RECEIVER,
	B_WAL_SENDER,
	B_WAL_WRITER,

	/*
	 * PGRAC: pgrac cluster background process types (stage 0.10).
	 * Values are append-only to preserve backend-type ABI.  Stage 0.10
	 * registered identifiers only -- spawning paths land in later stages.
	 */
	B_CLUSTER_STATS,
	B_CSSD,
	B_DIAG,
	B_HEARTBEAT,
	B_INTERCONNECT,
	B_LCK,
	B_LMD,
	B_LMON,
	B_LMS,
	B_LMS_WORKER,
	B_MRP,
	B_QVOTEC,
	B_RECOVERY_COORD,
	B_RECOVERY_WORKER,
	B_SINVAL_BCAST,
	B_TT_GC,
	B_UNDO_CLEANER,
	B_RFS,
} BackendType;

/* PGRAC: anchored on the last pgrac value so additions stay correct.    */
#define BACKEND_NUM_TYPES (B_RFS + 1)

extern PGDLLIMPORT BackendType MyBackendType;

#define AmRegularBackendProcess() (MyBackendType == B_BACKEND)

extern const char *GetBackendTypeDesc(BackendType backendType);

extern void SetDatabasePath(const char *path);
extern void checkDataDir(void);
extern void SetDataDir(const char *dir);
extern void ChangeToDataDir(void);

extern char *GetUserNameFromId(Oid roleid, bool noerr);
extern Oid GetUserId(void);
extern Oid GetOuterUserId(void);
extern Oid GetSessionUserId(void);
extern bool GetSessionUserIsSuperuser(void);
extern Oid GetAuthenticatedUserId(void);
extern bool GetAuthenticatedUserIsSuperuser(void);
extern void SetAuthenticatedUserId(Oid userid, bool is_superuser);
extern void GetUserIdAndSecContext(Oid *userid, int *sec_context);
extern void SetUserIdAndSecContext(Oid userid, int sec_context);
extern bool InLocalUserIdChange(void);
extern bool InSecurityRestrictedOperation(void);
extern bool InNoForceRLSOperation(void);
extern void GetUserIdAndContext(Oid *userid, bool *sec_def_context);
extern void SetUserIdAndContext(Oid userid, bool sec_def_context);
extern void InitializeSessionUserId(const char *rolename, Oid roleid);
extern void InitializeSessionUserIdStandalone(void);
extern void SetSessionAuthorization(Oid userid, bool is_superuser);
extern Oid GetCurrentRoleId(void);
extern void SetCurrentRoleId(Oid roleid, bool is_superuser);
extern void InitializeSystemUser(const char *authn_id, const char *auth_method);
extern const char *GetSystemUser(void);

/* in utils/misc/superuser.c */
extern bool superuser(void);		   /* current user is superuser */
extern bool superuser_arg(Oid roleid); /* given user is superuser */


/*****************************************************************************
 *	  pmod.h --																 *
 *			POSTGRES processing mode definitions.                            *
 *****************************************************************************/

/*
 * Description:
 *		There are three processing modes in POSTGRES.  They are
 * BootstrapProcessing or "bootstrap," InitProcessing or
 * "initialization," and NormalProcessing or "normal."
 *
 * The first two processing modes are used during special times. When the
 * system state indicates bootstrap processing, transactions are all given
 * transaction id "one" and are consequently guaranteed to commit. This mode
 * is used during the initial generation of template databases.
 *
 * Initialization mode: used while starting a backend, until all normal
 * initialization is complete.  Some code behaves differently when executed
 * in this mode to enable system bootstrapping.
 *
 * If a POSTGRES backend process is in normal mode, then all code may be
 * executed normally.
 */

typedef enum ProcessingMode {
	BootstrapProcessing, /* bootstrap creation of template database */
	InitProcessing,		 /* initializing system */
	NormalProcessing	 /* normal processing */
} ProcessingMode;

extern PGDLLIMPORT ProcessingMode Mode;

#define IsBootstrapProcessingMode() (Mode == BootstrapProcessing)
#define IsInitProcessingMode() (Mode == InitProcessing)
#define IsNormalProcessingMode() (Mode == NormalProcessing)

#define GetProcessingMode() Mode

#define SetProcessingMode(mode)                                                                    \
	do {                                                                                           \
		Assert((mode) == BootstrapProcessing || (mode) == InitProcessing                           \
			   || (mode) == NormalProcessing);                                                     \
		Mode = (mode);                                                                             \
	} while (0)


/*
 * Auxiliary-process type identifiers.  These used to be in bootstrap.h
 * but it seems saner to have them here, with the ProcessingMode stuff.
 * The MyAuxProcType global is defined and set in auxprocess.c.
 *
 * Make sure to list in the glossary any items you add here.
 */

typedef enum {
	NotAnAuxProcess = -1,
	StartupProcess = 0,
	BgWriterProcess,
	ArchiverProcess,
	CheckpointerProcess,
	WalWriterProcess,
	WalReceiverProcess,
#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC (stage 1.11 Sprint A): LMON aux process — first cluster
	 * background process spawned by postmaster.  Appended to preserve
	 * existing AuxProcType numeric values; this slot only exists in
	 * --enable-cluster builds so non-cluster postgres binaries are
	 * ABI-compatible at the AuxProcType level.  Lifecycle skeleton
	 * only at 1.11 Sprint A; real reconfig / fence / GRD / heartbeat
	 * consumption land in Stage 2-6.  See cluster_lmon.h.
	 */
	LmonProcess,
	/*
	 * PGRAC (stage 1.12 Sprint A): LCK aux process — second cluster
	 * background process; instance-level lock placeholder.  Appended
	 * after LmonProcess to preserve numeric values.  Lifecycle
	 * skeleton only; real dictionary/catalog lock protocol lands in
	 * Stage 2+ GES feature.  See cluster_lck.h.
	 */
	LckProcess,
	/*
	 * DIAG (Diagnostic Process) is stage 1.13 Sprint A's third real
	 * cluster background process; cross-node diagnostic placeholder.
	 * Appended after LckProcess to preserve numeric values.  Lifecycle
	 * skeleton only; real diagnostic snapshot / hang dump / cross-node
	 * DIAG request protocol lands in Stage 2+ when interconnect is
	 * fully wired.  See cluster_diag.h.
	 */
	DiagProcess,
	/*
	 * Cluster Stats (Cluster Statistics process) is stage 1.14 Sprint
	 * A's fourth real cluster background process; pg_stat_cluster_*
	 * view filling / cross-node aggregation / wait event tracking /
	 * history retention placeholder.  Appended after DiagProcess to
	 * preserve numeric values.  Lifecycle skeleton only; real protocols
	 * land in Stage 2+ when interconnect is fully wired.  See
	 * cluster_stats.h.
	 */
	ClusterStatsProcess,
	/*
	 * CSSD (Cluster Synchronization Service Daemon) is the 5th cluster
	 * background process — spec-2.5 Sprint A.  Appended after
	 * ClusterStatsProcess to preserve numeric values.  Application-level
	 * peer dead detection via heartbeat broadcast (paired with spec-2.4
	 * D8 socket-level TCP KeepAlive for two-layer dead detection).
	 * Lifecycle skeleton + heartbeat broadcast + per-peer state machine;
	 * does NOT trigger reconfig (spec-2.29 separate sub-spec).  See
	 * cluster_cssd.h.
	 */
	CssdProcess,
	/*
	 * QVOTEC (Quorum Voting Coordinator) is the 6th cluster background
	 * process — spec-2.6 Sprint A Step 3 D7.  Appended after CssdProcess
	 * to preserve numeric values.  Polls voting disks on shared storage
	 * to derive cluster-wide quorum view;broadcasts cluster_freeze_writes
	 * / cluster_thaw_writes via PG ProcSignal multiplexer when
	 * quorum_state transitions.  Implements spec-2.0 §3 Invariant 1
	 * (no-quorum no dual-write fail-closed) + Invariant 3 (uncertainty
	 * → fail-closed).  Lease-based in_quorum semantics (Q4 v0.2)
	 * defends against qvotec hung > 2 × poll interval.  See
	 * cluster_qvotec.h.
	 */
	QvotecProcess,
	/*
	 * LMS (Lock Master / Grant Service) is the 7th cluster background
	 * process — spec-2.18 Sprint A.  Appended after QvotecProcess to
	 * preserve numeric values.  Spec-2.18 ships the LMS skeleton +
	 * grant-decision ownership migration from LMON to LMS with
	 * single ownership path + fail-closed semantics (no runtime
	 * LMON fallback grant; cluster.lms_enabled=off is PGC_POSTMASTER
	 * startup-only fallback).  Real grant state machine activation,
	 * BAST send, deadlock detection, cleanup_on_backend_exit, lock
	 * class expansion all defer to spec-2.19+ Phase 2.C.  See
	 * cluster_lms.h.
	 */
	LmsProcess,
	/*
	 * LMD (Lock Manager Daemon — deadlock detection actor) is the 8th
	 * cluster background process — spec-2.19 Sprint A.  Appended after
	 * LmsProcess to preserve numeric values.  Spec-2.19 ships the LMD
	 * skeleton + deadlock-detection ownership migration from spec-2.17
	 * caller-side 4-node placeholder to LMD with single ownership path +
	 * fail-closed semantics (no runtime caller-side fallback grant;
	 * cluster.lmd_enabled=off is PGC_POSTMASTER startup-only fallback).
	 * Real Tarjan cycle detection, wait-for graph maintenance, victim
	 * selection, cancellation all defer to spec-2.20+ Phase 2.C + spec-5.9
	 * Stage 5.  See cluster_lmd.h.  B_LMD BackendType already exists
	 * (spec-1.10 backend types extension) — spec-2.19 D3 reuses existing
	 * surface without duplicate definition.
	 */
	LmdProcess,
	/*
	 * SI Broadcaster (Shared Invalidation Broadcaster) is the 9th cluster
	 * background process — spec-2.38 Sprint A D4.  Appended after
	 * LmdProcess to preserve numeric values.  Cross-node sinval message
	 * propagation:  LMON drains ClusterSinvalOutbound and fanouts
	 * PGRAC_IC_MSG_SINVAL wire envelopes because tier1 TCP descriptors
	 * are LMON-local;  this aux process drains ClusterSinvalInbound and
	 * applies SendSharedInvalidMessages (PG-native sinval API); executes
	 * fail-safe SIResetAll() on inbound overflow (HC134).  The IC inbound
	 * handler nonblocking 约束 (HC133) forbids LWLockAcquire inside handler
	 * context, so all real apply work is deferred to this aux process main
	 * loop.  Producer mask CLUSTER_IC_PRODUCER_SINVAL_FANOUT means only
	 * LMON may directly send SINVAL wire envelopes (HC139); backends must
	 * enqueue via cluster_sinval_enqueue_batch().  See cluster_sinval_bcast.h.
	 */
	SinvalBcastProcess,

	/*
	 * Undo Cleaner is the Stage 3.13 cluster background process —
	 * proactive undo/TT-slot retention GC (spec-3.13).  Appended after
	 * SinvalBcastProcess to preserve numeric values.  Consumes the
	 * B_UNDO_CLEANER BackendType reserved at Stage 1.  ServerLoop-
	 * managed (NOT phase-4 gated): absence degrades to spec-3.12
	 * lazy-only recycling.  See cluster_undo_cleaner.h.
	 */
	UndoCleanerProcess,

	/*
	 * MRP (Managed Recovery Process) is the spec-6.4 ADG physical standby
	 * apply process.  Appended after UndoCleanerProcess to preserve every
	 * existing AuxProcType value; postmaster only forks it when
	 * cluster.dg_role=standby and cluster.enable_adg=on.
	 */
	MrpProcess,
	RfsProcess,

	/*
	 * LMS worker pool (spec-7.3 D2) — LmsWorker1..7Process are the DATA-plane
	 * worker siblings of LmsProcess (which is worker 0).  Appended after
	 * RfsProcess to preserve every existing AuxProcType numeric value.  Each
	 * is a DISTINCT AuxProcType so it gets its own BackendStatus /
	 * AuxiliaryProc slot (slotno = MaxBackends + MyAuxProcType) — sidestepping
	 * the "one process per aux type" assumption (backend_status.c) that ruled
	 * out a single-type-multi-instance carrier.  Their MyBackendType is
	 * B_LMS_WORKER (the Stage 0.10 reserved BackendType).  Postmaster forks
	 * cluster.lms_workers - 1 of them (0 when lms_workers = 1, i.e. the
	 * spec-7.2 topology identity).  Contiguity and the worker_id derivation
	 * (MyAuxProcType - LmsWorker1Process + 1) are asserted in
	 * test_cluster_backend_types.c.
	 */
	LmsWorker1Process,
	LmsWorker2Process,
	LmsWorker3Process,
	LmsWorker4Process,
	LmsWorker5Process,
	LmsWorker6Process,
	LmsWorker7Process,

#endif
	NUM_AUXPROCTYPES /* Must be last! */
} AuxProcType;

extern PGDLLIMPORT AuxProcType MyAuxProcType;

#define AmStartupProcess() (MyAuxProcType == StartupProcess)
#define AmBackgroundWriterProcess() (MyAuxProcType == BgWriterProcess)
#define AmArchiverProcess() (MyAuxProcType == ArchiverProcess)
#define AmCheckpointerProcess() (MyAuxProcType == CheckpointerProcess)
#define AmWalWriterProcess() (MyAuxProcType == WalWriterProcess)
#define AmWalReceiverProcess() (MyAuxProcType == WalReceiverProcess)
#ifdef USE_PGRAC_CLUSTER
#define AmLmonProcess() (MyAuxProcType == LmonProcess)
#define AmLckProcess() (MyAuxProcType == LckProcess)
#define AmDiagProcess() (MyAuxProcType == DiagProcess)
#define AmClusterStatsProcess() (MyAuxProcType == ClusterStatsProcess)
#define AmCssdProcess() (MyAuxProcType == CssdProcess)
#define AmQvotecProcess() (MyAuxProcType == QvotecProcess)
#define AmLmsProcess() (MyAuxProcType == LmsProcess)
#define AmLmdProcess() (MyAuxProcType == LmdProcess)
#define AmSinvalBcastProcess() (MyAuxProcType == SinvalBcastProcess)
#define AmUndoCleanerProcess() (MyAuxProcType == UndoCleanerProcess)
#define AmMrpProcess() (MyAuxProcType == MrpProcess)
#define AmRfsProcess() (MyAuxProcType == RfsProcess)
/*
 * LMS worker pool (spec-7.3 D2).  AmLmsWorkerProcess() is true in a DATA-plane
 * worker sibling (worker id 1..7);  worker 0 is the plain LmsProcess and is
 * NOT matched here (AmLmsProcess()).  ClusterLmsWorkerIdForType() maps a
 * worker AuxProcType to its 1-based worker id (only valid when
 * AmLmsWorkerProcess()).
 */
#define AmLmsWorkerProcess() \
	(MyAuxProcType >= LmsWorker1Process && MyAuxProcType <= LmsWorker7Process)
#define ClusterLmsWorkerIdForType(t) ((int)((t) - LmsWorker1Process) + 1)
#endif


/*****************************************************************************
 *	  pinit.h --															 *
 *			POSTGRES initialization and cleanup definitions.                 *
 *****************************************************************************/

/* in utils/init/postinit.c */
extern void pg_split_opts(char **argv, int *argcp, const char *optstr);
extern void InitializeMaxBackends(void);
extern void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid,
						 bool load_session_libraries, bool override_allow_connections,
						 char *out_dbname);
extern void BaseInit(void);

/* in utils/init/miscinit.c */
extern PGDLLIMPORT bool IgnoreSystemIndexes;
extern PGDLLIMPORT bool process_shared_preload_libraries_in_progress;
extern PGDLLIMPORT bool process_shared_preload_libraries_done;
extern PGDLLIMPORT bool process_shmem_requests_in_progress;
extern PGDLLIMPORT char *session_preload_libraries_string;
extern PGDLLIMPORT char *shared_preload_libraries_string;
extern PGDLLIMPORT char *local_preload_libraries_string;

extern void CreateDataDirLockFile(bool amPostmaster);
extern void CreateSocketLockFile(const char *socketfile, bool amPostmaster, const char *socketDir);
extern void TouchSocketLockFiles(void);
extern void AddToDataDirLockFile(int target_line, const char *str);
extern bool RecheckDataDirLockFile(void);
extern void ValidatePgVersion(const char *path);
extern void process_shared_preload_libraries(void);
extern void process_session_preload_libraries(void);
extern void process_shmem_requests(void);
extern void pg_bindtextdomain(const char *domain);
extern bool has_rolreplication(Oid roleid);

typedef void (*shmem_request_hook_type)(void);
extern PGDLLIMPORT shmem_request_hook_type shmem_request_hook;

extern Size EstimateClientConnectionInfoSpace(void);
extern void SerializeClientConnectionInfo(Size maxsize, char *start_address);
extern void RestoreClientConnectionInfo(char *conninfo);

/* in executor/nodeHash.c */
extern size_t get_hash_memory_limit(void);

#endif /* MISCADMIN_H */
