/*-------------------------------------------------------------------------
 *
 * xlogrecovery.c
 *		Functions for WAL recovery, standby mode
 *
 * This source file contains functions controlling WAL recovery.
 * InitWalRecovery() initializes the system for crash or archive recovery,
 * or standby mode, depending on configuration options and the state of
 * the control file and possible backup label file.  PerformWalRecovery()
 * performs the actual WAL replay, calling the rmgr-specific redo routines.
 * FinishWalRecovery() performs end-of-recovery checks and cleanup actions,
 * and prepares information needed to initialize the WAL for writes.  In
 * addition to these three main functions, there are a bunch of functions
 * for interrogating recovery state and controlling the recovery process.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogrecovery.c
 *
 *-------------------------------------------------------------------------
 *
 * PGRAC MODIFICATIONS (spec-4.1 v1.0)
 *
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	Spec: spec-4.1-per-thread-wal-routing.md
 *
 *	What changed:
 *	  - InitWalRecovery(): after allocating the recovery xlogreader, set
 *	    xlogreader->cluster_expected_thread_id to this node's WAL thread
 *	    id (cluster_wal_thread_id()) -- but ONLY for plain crash
 *	    recovery of the node's own stream (neither archive recovery nor
 *	    standby mode requested) and only when the id is real.  Standby
 *	    replay and archive recovery keep the permissive
 *	    XLP_THREAD_ID_INVALID default so upstream WAL stamped with the
 *	    primary's thread id is never rejected with a local node_id
 *	    (spec-4.1 RL1).
 *
 *	Why:
 *	  Own-stream strict validation is the read-time second defence
 *	  (after cluster_wal_thread_init's startup directory check) against
 *	  a mis-linked pg_wal serving another thread's stream to crash
 *	  recovery (AD-009 per-instance redo thread, spec-4.1 Q4-A).
 *
 * PGRAC MODIFICATIONS (spec-4.3 v1.0)
 *
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	Spec: spec-4.3-recovery-coordinator-skeleton.md
 *
 *	What changed:
 *	  - InitWalRecovery(): immediately after the spec-4.1 RL1 hook and
 *	    under the same plain-local gate, run the observational recovery
 *	    plan pass (cluster_recovery_plan_generate) which classifies all
 *	    128 WAL-state registry slots into per-thread recovery verdicts.
 *	    Note this runs BEFORE InRecovery is determined: it is a plain
 *	    local STARTUP planning pass (clean starts produce a plan too),
 *	    not a "crash recovery confirmed" pass; ControlFile facts are
 *	    passed along so readers can tell the two apart.
 *
 *	Why:
 *	  The Recovery Coordinator (= this node's startup process, Oracle
 *	  apply-centralised model) needs the merge-set enumeration input
 *	  for spec-4.4/4.5; in this stage the plan is observational only
 *	  and never blocks or alters recovery (spec-4.3 §3.3).
 *
 * PGRAC MODIFICATIONS (spec-4.4 v1.0)
 *
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	Spec: spec-4.4-recovery-worker-skeleton.md
 *
 *	What changed:
 *	  - InitWalRecovery(): after the spec-4.3 plan pass and under the
 *	    same plain-local gate, launch the candidate stream-validation
 *	    workers (cluster_recovery_workers_launch; registration only,
 *	    never waits).
 *
 *	Why:
 *	  Observational pre-merge stream evidence for spec-4.5; workers
 *	  read only OTHER threads' directories and never interact with
 *	  this node's own-stream replay.
 *
 * PGRAC MODIFICATIONS (spec-4.5 v1.0, A-closure)
 *
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	Spec: spec-4.5-kway-scn-merge-replay.md
 *
 *	What changed:
 *	  - PerformWalRecovery(): decide whether to engage k-way merged
 *	    recovery (cluster_recovery_merge_preflight_readonly) once InRecovery is
 *	    settled; on a gated cold crash it FATALs 53RA3 (no shared-data
 *	    backend yet, A-closure capability gate).  When engaged (only
 *	    reachable once roadmap 4.5a lands the shared-data backend), the
 *	    whole redo is driven from the merge engine
 *	    (cluster_recovery_merged_replay) instead of the single-stream
 *	    loop.  Off / no-candidate keep today's single-stream path.
 *
 *	Why:
 *	  spec-4.5 §3.1/§3.2 cold-crash merged replay (structure ship;
 *	  apply-through deferred to 4.5a).
 *
 * PGRAC MODIFICATIONS (spec-4.5a v1.0)
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  1. The merged-loop §3.3e classification is extracted
 *	               into cluster_record_apply_class.  2. The merged
 *	               foreign path applies MATERIALIZE_LOCAL records
 *	               (RM_CLUSTER_UNDO) so a peer's undo materializes into
 *	               the local pg_undo/instance_<owner> tree.
 *	Why:           spec-4.5a-shared-storage-data-backend.md §0 G2 +
 *	               §3.3a (P0-3).
 *
 * PGRAC MODIFICATIONS (spec-6.14 D9 amend, INV-D9-R)
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	What changed:  1. PerformWalRecovery acquires the shared-regime
 *	               crash-recovery claim (blocking) before the engage
 *	               decision, crash recovery only -- serializing merged
 *	               replays AND plain own-stream redo across concurrently
 *	               booting nodes.  2. cluster_record_apply_class routes
 *	               RM_SMGR create/truncate by their PAYLOAD relfile
 *	               locator (they carry no block ref; pre-amend they fell
 *	               through to UNCLASSIFIABLE and 53RA3'd the merge on any
 *	               foreign CREATE TABLE tail).
 *	Why:           spec-6.14-shared-catalog-single-authority.md D9 amend
 *	               (the spec-5.6 Phase-2 rendezvous forces concurrent
 *	               boots, breaking spec-4.5a Q3's serialized-cold-merge
 *	               premise).
 *
 * PGRAC MODIFICATIONS
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	What changed: XLogFileRead opens the authenticated restart generation
 *	              independently of the writable pg_wal route.
 *	Why:          A successor writer must not substitute its WAL for restart input.
 *
 * PGRAC MODIFICATIONS (S9P2-05 typed cold replay)
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	What changed: With cluster.shared_config, an engaged cold merge runs the
 *	              typed cold plan: pass 1 (cluster_recovery_typed_begin)
 *	              scans every retained writer generation of the participant
 *	              census (crashed ones replayed, older or closed ones as
 *	              history only) before the serial set is taken; pass 2
 *	              (cluster_recovery_typed_replay) replays the sealed
 *	              schedule, matching every page record to its pass-1
 *	              identity. An exact C0 dependency may finish a target
 *	              source whose scheduled steps already ran, without changing
 *	              the PAGE/SPACE schedule. The SPACE owner installs changes
 *	              at their steps (repeating a TRUNCATE's shrink only where
 *	              nothing proves it durable) and in full at the end, never
 *	              replayed natively; another generation's native control
 *	              records (checkpoints, counters, standby) have no effect on
 *	              the founder, whose SCN is advanced past every record of
 *	              the plan.  Pass 2 starts only once every consumer it
 *	              needs exists (cluster_cold_typed_ready_v1), including a
 *	              restartpoint owner that does not adopt own checkpoints
 *	              replayed inside it.  After pass 2 every fenced generation
 *	              whose replay ended exactly at its sealed tail is published
 *	              RECOVERY_COMPLETE, once the files pass 2 changed are
 *	              durable and while IR is still held until the handoff.
 *	              Without shared_config a cold merge of several threads is
 *	              refused before any fence or claim; with it, a crash start
 *	              the typed plan does not engage is refused before the claim
 *	              (never a native replay of this node's stream alone).
 *	Why:          Retained history before a native redo start is ancestry
 *	              only; SCN order with unconditional full-page images can
 *	              overwrite a newer durable page another thread wrote.  A
 *	              generation published recovered is never replayed again,
 *	              so its replayed changes must be durable first.
 */

#include "postgres.h"

#include <ctype.h>
#include <math.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/timeline.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/htup_details.h"
/* PGRAC: spec-4.1 own-stream strict thread-id for crash recovery. */
#include "cluster/cluster_apply_master_election.h"
#include "cluster/cluster_backup.h" /* PGRAC: spec-6.5 restore/PITR manifest */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h" /* PGRAC: spec-4.5a cluster_wal_threads_dir */
#include "cluster/cluster_hw_snapshot.h" /* PGRAC: spec-5.7 D3 HW authority recovery load */
#include "cluster/cluster_mrp.h"	   /* PGRAC: spec-6.4 ADG Apply Master gate */
#include "cluster/cluster_recovery_anchor.h" /* PGRAC: spec-5.6a per-node restart inputs */
#include "cluster/cluster_mrp_apply.h" /* PGRAC: spec-6.4 MRP apply facade */
#include "cluster/cluster_rfs.h"	   /* PGRAC: spec-6.4 ADG RFS coordinator */
#include "cluster/cluster_remote_xact.h" /* PGRAC: spec-4.5a G5 */
#include "cluster/cluster_tt_status.h" /* PGRAC: spec-4.5a D11 merged counters */
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_scn.h" /* PGRAC: spec-4.5a G6 checkpoint SCN seed */
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_census.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_page_install_ctrc.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/storage/cluster_shared_fs_page.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/storage/cluster_smgr.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_write_fence.h"
#endif
#include "access/xlogarchive.h"
#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "backup/basebackup.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h" /* PGRAC: spec-6.14 D9 amend smgr payload routing */
#include "commands/tablespace.h"
#include "common/file_utils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc_hooks.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/pg_rusage.h"
#include "utils/wait_event.h"

/* Unsupported old recovery command file names (relative to $PGDATA) */
#define RECOVERY_COMMAND_FILE	"recovery.conf"
#define RECOVERY_COMMAND_DONE	"recovery.done"

/*
 * GUC support
 */
const struct config_enum_entry recovery_target_action_options[] = {
	{"pause", RECOVERY_TARGET_ACTION_PAUSE, false},
	{"promote", RECOVERY_TARGET_ACTION_PROMOTE, false},
	{"shutdown", RECOVERY_TARGET_ACTION_SHUTDOWN, false},
	{NULL, 0, false}
};

/* options formerly taken from recovery.conf for archive recovery */
char	   *recoveryRestoreCommand = NULL;
char	   *recoveryEndCommand = NULL;
char	   *archiveCleanupCommand = NULL;
RecoveryTargetType recoveryTarget = RECOVERY_TARGET_UNSET;
bool		recoveryTargetInclusive = true;
int			recoveryTargetAction = RECOVERY_TARGET_ACTION_PAUSE;
TransactionId recoveryTargetXid;
char	   *recovery_target_time_string;
TimestampTz recoveryTargetTime;
const char *recoveryTargetName;
XLogRecPtr	recoveryTargetLSN;
int			recovery_min_apply_delay = 0;

/* options formerly taken from recovery.conf for XLOG streaming */
char	   *PrimaryConnInfo = NULL;
char	   *PrimarySlotName = NULL;
bool		wal_receiver_create_temp_slot = false;

/*
 * recoveryTargetTimeLineGoal: what the user requested, if any
 *
 * recoveryTargetTLIRequested: numeric value of requested timeline, if constant
 *
 * recoveryTargetTLI: the currently understood target timeline; changes
 *
 * expectedTLEs: a list of TimeLineHistoryEntries for recoveryTargetTLI and
 * the timelines of its known parents, newest first (so recoveryTargetTLI is
 * always the first list member).  Only these TLIs are expected to be seen in
 * the WAL segments we read, and indeed only these TLIs will be considered as
 * candidate WAL files to open at all.
 *
 * curFileTLI: the TLI appearing in the name of the current input WAL file.
 * (This is not necessarily the same as the timeline from which we are
 * replaying WAL, which StartupXLOG calls replayTLI, because we could be
 * scanning data that was copied from an ancestor timeline when the current
 * file was created.)  During a sequential scan we do not allow this value
 * to decrease.
 */
RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal = RECOVERY_TARGET_TIMELINE_LATEST;
TimeLineID	recoveryTargetTLIRequested = 0;
TimeLineID	recoveryTargetTLI = 0;
static List *expectedTLEs;
static TimeLineID curFileTLI;

/*
 * When ArchiveRecoveryRequested is set, archive recovery was requested,
 * ie. signal files were present.  When InArchiveRecovery is set, we are
 * currently recovering using offline XLOG archives.  These variables are only
 * valid in the startup process.
 *
 * When ArchiveRecoveryRequested is true, but InArchiveRecovery is false, we're
 * currently performing crash recovery using only XLOG files in pg_wal, but
 * will switch to using offline XLOG archives as soon as we reach the end of
 * WAL in pg_wal.
*/
bool		ArchiveRecoveryRequested = false;
bool		InArchiveRecovery = false;

/*
 * When StandbyModeRequested is set, standby mode was requested, i.e.
 * standby.signal file was present.  When StandbyMode is set, we are currently
 * in standby mode.  These variables are only valid in the startup process.
 * They work similarly to ArchiveRecoveryRequested and InArchiveRecovery.
 */
static bool StandbyModeRequested = false;
bool		StandbyMode = false;

/* was a signal file present at startup? */
static bool standby_signal_file_found = false;
static bool recovery_signal_file_found = false;

/*
 * CheckPointLoc is the position of the checkpoint record that determines
 * where to start the replay.  It comes from the backup label file or the
 * control file.
 *
 * RedoStartLSN is the checkpoint's REDO location, also from the backup label
 * file or the control file.  In standby mode, XLOG streaming usually starts
 * from the position where an invalid record was found.  But if we fail to
 * read even the initial checkpoint record, we use the REDO location instead
 * of the checkpoint location as the start position of XLOG streaming.
 * Otherwise we would have to jump backwards to the REDO location after
 * reading the checkpoint record, because the REDO record can precede the
 * checkpoint record.
 */
static XLogRecPtr CheckPointLoc = InvalidXLogRecPtr;
static TimeLineID CheckPointTLI = 0;
static XLogRecPtr RedoStartLSN = InvalidXLogRecPtr;
static TimeLineID RedoStartTLI = 0;

/*
 * Local copy of SharedHotStandbyActive variable. False actually means "not
 * known, need to check the shared state".
 */
static bool LocalHotStandbyActive = false;

/*
 * Local copy of SharedPromoteIsTriggered variable. False actually means "not
 * known, need to check the shared state".
 */
static bool LocalPromoteIsTriggered = false;

/* Has the recovery code requested a walreceiver wakeup? */
static bool doRequestWalReceiverReply;

/* XLogReader object used to parse the WAL records */
static XLogReaderState *xlogreader = NULL;

/* XLogPrefetcher object used to consume WAL records with read-ahead */
static XLogPrefetcher *xlogprefetcher = NULL;

/* Parameters passed down from ReadRecord to the XLogPageRead callback. */
typedef struct XLogPageReadPrivate
{
	int			emode;
	bool		fetching_ckpt;	/* are we fetching a checkpoint record? */
	bool		randAccess;
	TimeLineID	replayTLI;
} XLogPageReadPrivate;

/* flag to tell XLogPageRead that we have started replaying */
static bool InRedo = false;

/*
 * Codes indicating where we got a WAL file from during recovery, or where
 * to attempt to get one.
 */
typedef enum
{
	XLOG_FROM_ANY = 0,			/* request to read WAL from any source */
	XLOG_FROM_ARCHIVE,			/* restored using restore_command */
	XLOG_FROM_PG_WAL,			/* existing file in pg_wal */
	XLOG_FROM_STREAM			/* streamed from primary */
} XLogSource;

/* human-readable names for XLogSources, for debugging output */
static const char *const xlogSourceNames[] = {"any", "archive", "pg_wal", "stream"};

/*
 * readFile is -1 or a kernel FD for the log file segment that's currently
 * open for reading.  readSegNo identifies the segment.  readOff is the offset
 * of the page just read, readLen indicates how much of it has been read into
 * readBuf, and readSource indicates where we got the currently open file from.
 *
 * Note: we could use Reserve/ReleaseExternalFD to track consumption of this
 * FD too (like for openLogFile in xlog.c); but it doesn't currently seem
 * worthwhile, since the XLOG is not read by general-purpose sessions.
 */
static int	readFile = -1;
static XLogSegNo readSegNo = 0;
static uint32 readOff = 0;
static uint32 readLen = 0;
static XLogSource readSource = XLOG_FROM_ANY;

/*
 * Keeps track of which source we're currently reading from. This is
 * different from readSource in that this is always set, even when we don't
 * currently have a WAL file open. If lastSourceFailed is set, our last
 * attempt to read from currentSource failed, and we should try another source
 * next.
 *
 * pendingWalRcvRestart is set when a config change occurs that requires a
 * walreceiver restart.  This is only valid in XLOG_FROM_STREAM state.
 */
static XLogSource currentSource = XLOG_FROM_ANY;
static bool lastSourceFailed = false;
static bool pendingWalRcvRestart = false;

/*
 * These variables track when we last obtained some WAL data to process,
 * and where we got it from.  (XLogReceiptSource is initially the same as
 * readSource, but readSource gets reset to zero when we don't have data
 * to process right now.  It is also different from currentSource, which
 * also changes when we try to read from a source and fail, while
 * XLogReceiptSource tracks where we last successfully read some WAL.)
 */
static TimestampTz XLogReceiptTime = 0;
static XLogSource XLogReceiptSource = XLOG_FROM_ANY;

/* Local copy of WalRcv->flushedUpto */
static XLogRecPtr flushedUpto = 0;
static TimeLineID receiveTLI = 0;

/*
 * Copy of minRecoveryPoint and backupEndPoint from the control file.
 *
 * In order to reach consistency, we must replay the WAL up to
 * minRecoveryPoint.  If backupEndRequired is true, we must also reach
 * backupEndPoint, or if it's invalid, an end-of-backup record corresponding
 * to backupStartPoint.
 *
 * Note: In archive recovery, after consistency has been reached, the
 * functions in xlog.c will start updating minRecoveryPoint in the control
 * file.  But this copy of minRecoveryPoint variable reflects the value at the
 * beginning of recovery, and is *not* updated after consistency is reached.
 */
static XLogRecPtr minRecoveryPoint;
static TimeLineID minRecoveryPointTLI;

static XLogRecPtr backupStartPoint;
static XLogRecPtr backupEndPoint;
static bool backupEndRequired = false;

/*
 * Have we reached a consistent database state?  In crash recovery, we have
 * to replay all the WAL, so reachedConsistency is never set.  During archive
 * recovery, the database is consistent once minRecoveryPoint is reached.
 *
 * Consistent state means that the system is internally consistent, all
 * the WAL has been replayed up to a certain point, and importantly, there
 * is no trace of later actions on disk.
 */
bool		reachedConsistency = false;

/* Buffers dedicated to consistency checks of size BLCKSZ */
static char *replay_image_masked = NULL;
static char *primary_image_masked = NULL;


/*
 * Shared-memory state for WAL recovery.
 */
typedef struct XLogRecoveryCtlData
{
	/*
	 * SharedHotStandbyActive indicates if we allow hot standby queries to be
	 * run.  Protected by info_lck.
	 */
	bool		SharedHotStandbyActive;

	/*
	 * SharedPromoteIsTriggered indicates if a standby promotion has been
	 * triggered.  Protected by info_lck.
	 */
	bool		SharedPromoteIsTriggered;

	/*
	 * recoveryWakeupLatch is used to wake up the startup process to continue
	 * WAL replay, if it is waiting for WAL to arrive or promotion to be
	 * requested.
	 *
	 * Note that the startup process also uses another latch, its procLatch,
	 * to wait for recovery conflict. If we get rid of recoveryWakeupLatch for
	 * signaling the startup process in favor of using its procLatch, which
	 * comports better with possible generic signal handlers using that latch.
	 * But we should not do that because the startup process doesn't assume
	 * that it's waken up by walreceiver process or SIGHUP signal handler
	 * while it's waiting for recovery conflict. The separate latches,
	 * recoveryWakeupLatch and procLatch, should be used for inter-process
	 * communication for WAL replay and recovery conflict, respectively.
	 */
	Latch		recoveryWakeupLatch;

	/*
	 * Last record successfully replayed.
	 */
	XLogRecPtr	lastReplayedReadRecPtr; /* start position */
	XLogRecPtr	lastReplayedEndRecPtr;	/* end+1 position */
	TimeLineID	lastReplayedTLI;	/* timeline */

	/*
	 * When we're currently replaying a record, ie. in a redo function,
	 * replayEndRecPtr points to the end+1 of the record being replayed,
	 * otherwise it's equal to lastReplayedEndRecPtr.
	 */
	XLogRecPtr	replayEndRecPtr;
	TimeLineID	replayEndTLI;
	/* timestamp of last COMMIT/ABORT record replayed (or being replayed) */
	TimestampTz recoveryLastXTime;

	/*
	 * timestamp of when we started replaying the current chunk of WAL data,
	 * only relevant for replication or archive recovery
	 */
	TimestampTz currentChunkStartTime;
	/* Recovery pause state */
	RecoveryPauseState recoveryPauseState;
	ConditionVariable recoveryNotPausedCV;

	slock_t		info_lck;		/* locks shared variables shown above */
} XLogRecoveryCtlData;

static XLogRecoveryCtlData *XLogRecoveryCtl = NULL;

/*
 * abortedRecPtr is the start pointer of a broken record at end of WAL when
 * recovery completes; missingContrecPtr is the location of the first
 * contrecord that went missing.  See CreateOverwriteContrecordRecord for
 * details.
 */
static XLogRecPtr abortedRecPtr;
static XLogRecPtr missingContrecPtr;

/*
 * if recoveryStopsBefore/After returns true, it saves information of the stop
 * point here
 */
static TransactionId recoveryStopXid;
static TimestampTz recoveryStopTime;
static XLogRecPtr recoveryStopLSN;
static char recoveryStopName[MAXFNAMELEN];
static bool recoveryStopAfter;

/* prototypes for local functions */
static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI);
#ifdef USE_PGRAC_CLUSTER
/* PGRAC: spec-4.5a -- shared §3.3e classification for the merged loop. */
static ClusterRecoveryRecordClass cluster_record_apply_class(XLogReaderState *r);
/* PGRAC: spec-6.4 INV-ADG5 -- the apply-master term this startup process has
 * already applied records under.  0 = nothing applied yet.  Once set, losing
 * the lease or seeing a different term is a FATAL fail-stop (P0-1): cached
 * pages may be stale against an interim master and must be discarded. */
static uint64 cluster_adg_applied_under_term = 0;
/* Renewal-gap deadline: an ex-master waits at most half the takeover grace
 * for a same-term renewal to land before failing stop.  0 = not waiting. */
static int64 cluster_adg_gate_gap_deadline_ms = 0;
/* Once-per-episode logging for the closed gate (operator visibility). */
static bool cluster_adg_gate_wait_logged = false;

/*
 * cluster_adg_apply_gate_self_valid -- INV-ADG5 gate predicate + fail-stop
 *	policy, shared by the streaming-loop pre-gates and the ApplyWalRecord
 *	gate so a deposed master fails stop promptly wherever it is waiting.
 *
 *	Returns true when this node holds a fresh apply-master lease and may
 *	apply; records the term it clears us under.  A node that has already
 *	applied records under some term FATALs on term change, on a foreign
 *	takeover, and when a renewal gap outlives half the takeover grace --
 *	its cached pages may be stale against an interim master, and the only
 *	safe reaction is to discard them by failing stop (P0-1).
 */
static bool
cluster_adg_apply_gate_self_valid(void)
{
	uint64		term = 0;
	ClusterMrpApplyGateState gate = cluster_mrp_apply_gate_probe(&term);

	if (gate == CLUSTER_MRP_APPLY_GATE_SELF_VALID)
	{
		if (cluster_adg_applied_under_term != 0
			&& term != cluster_adg_applied_under_term)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_ADG_NOT_APPLY_MASTER),
					 errmsg("ADG apply-master term changed from " UINT64_FORMAT
							" to " UINT64_FORMAT " while this node held replayed state",
							cluster_adg_applied_under_term, term),
					 errdetail("Another Apply Master may have applied WAL in between; "
							   "this node fails stop to discard its cached pages.")));
		cluster_adg_applied_under_term = term;
		cluster_adg_gate_gap_deadline_ms = 0;
		if (cluster_adg_gate_wait_logged)
		{
			ereport(LOG,
					(errmsg("ADG standby acquired the apply-master lease (term " UINT64_FORMAT
							"); resuming WAL replay",
							term)));
			cluster_adg_gate_wait_logged = false;
		}
		return true;
	}

	/* Once per wait episode: make the closed gate operator-visible. */
	if (!cluster_adg_gate_wait_logged)
	{
		ereport(LOG,
				(errmsg("ADG standby is waiting for the apply-master lease before "
						"replaying WAL")));
		cluster_adg_gate_wait_logged = true;
	}

	if (cluster_adg_applied_under_term != 0)
	{
		int64		now_ms = (int64) (GetCurrentTimestamp() / 1000);

		if (gate == CLUSTER_MRP_APPLY_GATE_FOREIGN_MASTER)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_ADG_NOT_APPLY_MASTER),
					 errmsg("ADG apply-master lease was taken over by another node"),
					 errdetail("This deposed node fails stop to discard its cached pages "
							   "before the new Apply Master's writes could be shadowed.")));
		if (cluster_adg_gate_gap_deadline_ms == 0)
			cluster_adg_gate_gap_deadline_ms =
				now_ms + cluster_adg_lease_takeover_grace_ms / 2;
		else if (now_ms >= cluster_adg_gate_gap_deadline_ms)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_ADG_NOT_APPLY_MASTER),
					 errmsg("ADG apply-master lease was not renewed within the "
							"takeover-grace window"),
					 errdetail("This node's lease may already be up for takeover; it fails "
							   "stop to discard its cached pages.")));
	}
	return false;
}
#endif
#ifdef USE_PGRAC_CLUSTER
static XLogRecPtr cluster_recovery_merged_replay(const uint64 *bitmap, const XLogRecPtr *start,
												 const XLogRecPtr *stop, const char *wal_root,
												 TimeLineID tli, uint16 own_thread,
												 TimeLineID *replayTLI, bool restore_mode,
												 ClusterRecoveryFencePlan **fence_plan);
/* Startup-process local, one-shot handoff of the original typed preparation.
 * Nothing here is a durable certificate or a successor writer capability. */
typedef struct ClusterColdStartupPreparationV1 {
	bool started;
	bool ready;
	ResourceOwner owner;
	int pid;
	XLogRecPtr checkpoint;
	XLogRecPtr redo;
	TimeLineID checkpoint_tli;
	TimeLineID redo_tli;
	ClusterMergeEngage engage;
	ClusterRecoveryFencePlan *fence;
	ClusterColdTypedV1 *typed;
} ClusterColdStartupPreparationV1;
static ClusterColdStartupPreparationV1 cluster_recovery_cold_preparation;
static void cluster_recovery_typed_fail(ClusterRecoveryFencePlan **fence_plan,
									  ClusterColdTypedV1 **typed,
									  const char *message, const char *detail);
static ClusterColdTypedV1 *cluster_recovery_typed_begin(ClusterRecoveryFencePlan **fence_plan);
static XLogRecPtr cluster_recovery_typed_replay(ClusterColdTypedV1 **typed,
												TimeLineID *replayTLI,
												ClusterRecoveryFencePlan **fence_plan);
static ClusterColdTypedV1 *cluster_recovery_cold_inputs;

/* Completed operation values and their original raw descriptors remain on
 * the same Start. Clearing a durable F4 does not release replay/history debt. */
typedef struct ClusterColdCtrcPageCompletedV1 {
	RfPageCtrcStepResultV1 result;
	struct ClusterSharedFsPageV1 *storage;
	RfPageCtrcUnchangedV1 unchanged;
} ClusterColdCtrcPageCompletedV1;

typedef struct ClusterColdCtrcPageProgressV1 {
	Size budget;
	Size allocation_bytes;
	Size reserved_bytes;
	const RfPageOnlinePlanV1 *plan;
	const ClusterRecoveryCtrcReplaySetSourceV1 *sources;
	uint32 source_count, target_count;
	uint32 next_target;
	bool pending;
	RfPageCtrcStepResultV1 result;
	struct ClusterSharedFsPageV1 *storage;
	ClusterColdCtrcPageCompletedV1 completed[FLEXIBLE_ARRAY_MEMBER];
} ClusterColdCtrcPageProgressV1;

/* Input identity for the original post-EOR attempt. Old subject identities
 * stay in inputs; writer is a separate, actual installed successor. */
struct ClusterRecoveryStartV1 {
	const ClusterColdTypedV1 *inputs;
	const ClusterColdTopTxCensusV1 *census;
	ClusterRecoveryFencePlan *tx_fence;
	ResourceOwner owner;
	int pid;
	uint64 owner_boot;
	uint64 generation;
	uint64 serial;
	uint64 claim_owner;
	uint64 install_generation;
	ClusterWalWriterToken writer;
	uint8 operation_uuid[16];
	bool revoked;
	bool relmap_declared;
	bool ctrc_observation_started, ctrc_observation_complete;
	bool ctrc_page_plan_started;
	RfPageOnlinePlanV1 *ctrc_page_plan;
	ClusterRecoveryCtrcReplaySetSourceV1 *ctrc_page_sources;
	uint32 ctrc_page_source_count;
	ClusterColdCtrcPageProgressV1 *ctrc_page_progress;
	const ClusterColdCtrcWorkV1 *ctrc_observation_work;
	ClusterColdCtrcWorkV1 *ctrc_observation_work_copy;
	Size ctrc_observation_work_bytes;
	ClusterRecoveryTxResultObservationV1 ctrc_observation_tx;
	ClusterControlRootFileToken ctrc_observation_root;
	ClusterWalInputsV1 *ctrc_observation_inputs;
	ClusterRecoveryCensus *ctrc_observation_census;
	ClusterRecoveryCensusManifest ctrc_observation_manifest;
	ClusterColdCtrcPhysicalV1 *ctrc_physical;
	const ClusterRecoveryCensusReferencesV1 *ctrc_references;
	const ClusterRecoveryCensusPagesV1 *ctrc_pages; /* PGRAC: original census owns this view */
	uint32 ctrc_page_wal_count; /* PGRAC: full original PAGE WAL observation */
	uint32 ctrc_wal_source_count; /* PGRAC: full original source coverage */
	char ctrc_observation_refusal[384];
};
static struct ClusterRecoveryStartV1 *cluster_recovery_cold_start;
static uint64 cluster_recovery_cold_generation;
static bool cluster_recovery_cold_callback_registered;

/* Retire dependents before the original scope/FD/WALR owner. The original
 * scope's unconfirmed-release refusal must not free the retained inputs. */
static void
cluster_recovery_cold_observation_close(struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryCtrcReplaySetSourceV1 *page_sources = start->ctrc_page_sources;

	start->ctrc_observation_complete = false;
	/* Retire borrowed source storage before any dependent callback. */
	start->ctrc_page_sources = NULL;
	start->ctrc_page_source_count = 0;
	/* Private image storage is not the original failed/INSERTED window. */
	if (start->ctrc_page_progress != NULL) {
		ClusterColdCtrcPageProgressV1 *progress = start->ctrc_page_progress;
		struct ClusterSharedFsPageV1 *storage = progress->storage;

		start->ctrc_page_progress = NULL;
		progress->storage = NULL;
		/* Abandon local custody, never erase the unfinished shared duty. */
		if (progress->pending && progress->result.inserted.window.key.window_seq != 0)
			(void)cluster_recovery_stamp_fail_v1(&progress->result.inserted.window);
		(void)cluster_shared_fs_page_close_v1(storage);
		for (uint32 i = 0; i < progress->target_count; i++) {
			storage = progress->completed[i].storage;
			progress->completed[i].storage = NULL;
			(void)cluster_shared_fs_page_close_v1(storage);
		}
		pfree(progress);
	}
	rf_page_online_plan_destroy_v1(&start->ctrc_page_plan);
	if (page_sources != NULL) pfree(page_sources);
	start->ctrc_references = NULL;
	start->ctrc_pages = NULL;
	start->ctrc_page_wal_count = 0;
	start->ctrc_wal_source_count = 0;
	cluster_recovery_census_destroy(start->ctrc_observation_census);
	start->ctrc_observation_census = NULL;
	if (start->ctrc_observation_inputs != NULL)
		cluster_wal_inputs_release_v1(&start->ctrc_observation_inputs);
	if (start->ctrc_physical != NULL) {
		pfree(start->ctrc_physical);
		start->ctrc_physical = NULL;
	}
	if (start->ctrc_observation_work_copy != NULL) {
		pfree(start->ctrc_observation_work_copy);
		start->ctrc_observation_work_copy = NULL;
	}
}

static bool
cluster_recovery_cold_writer_same(const ClusterWalWriterToken *a,
								const ClusterWalWriterToken *b)
{
	return memcmp(&a->ref.claim.identity, &b->ref.claim.identity,
				  sizeof(a->ref.claim.identity)) == 0
		&& a->ref.claim.database_incarnation == b->ref.claim.database_incarnation
		&& a->ref.claim.max_config_generation == b->ref.claim.max_config_generation
		&& memcmp(a->ref.claim.claim_sha256, b->ref.claim.claim_sha256, 32) == 0
		&& a->ref.timeline == b->ref.timeline && a->epoch == b->epoch
		&& a->startup_first_lsn == b->startup_first_lsn;
}

/* ERROR/release invalidates only the process resource, never persistent debt.
 * The original input owner remains responsible for its eventual destruction. */
static void
cluster_recovery_cold_resource_release(ResourceReleasePhase phase, bool isCommit,
									   bool isTopLevel, void *arg)
{
	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && cluster_recovery_cold_start != NULL
		&& cluster_recovery_cold_start->owner == CurrentResourceOwner)
	{
		cluster_recovery_checkpoint_revoke_v1(cluster_recovery_cold_start);
		cluster_recovery_cold_start->revoked = true;
		cluster_recovery_cold_observation_close(cluster_recovery_cold_start);
	}
}

static void
cluster_recovery_cold_inputs_cleanup(int code, Datum arg)
{
	(void)code;
	(void)arg;
	if (cluster_recovery_cold_start != NULL) {
		cluster_recovery_checkpoint_revoke_v1(cluster_recovery_cold_start);
		cluster_recovery_cold_start->revoked = true;
		cluster_recovery_cold_observation_close(cluster_recovery_cold_start);
		pfree(cluster_recovery_cold_start);
		cluster_recovery_cold_start = NULL;
	}
	cluster_cold_typed_destroy_v1(&cluster_recovery_cold_inputs);
}

/* Keep original inputs and their original formation/isolation proof owners
 * across EOR, never a live read scope or the old execution guards/pin.
 * Later mutation still needs fresh source, serial and writer authorization. */
static void
cluster_recovery_cold_inputs_retain(ClusterColdTypedV1 **typed)
{
	if (typed == NULL || *typed == NULL || (*typed)->inputs != NULL
		|| !cluster_recovery_merge_fence_plan_tx_current_v1((*typed)->tx_fence, NULL)
		|| cluster_recovery_cold_inputs != NULL)
		ereport(FATAL, (errmsg("could not retain original cold recovery inputs")));
	before_shmem_exit(cluster_recovery_cold_inputs_cleanup, (Datum)0);
	cluster_recovery_cold_inputs = *typed;
	*typed = NULL;
}

static void
cluster_recovery_cold_inputs_handoff(EndOfWalRecoveryInfo *info)
{
	info->cluster_cold_inputs = cluster_recovery_cold_inputs;
	info->cluster_tx_start = NULL;
}

/* A pending result requires its original unfinished F4. Completed operations
 * retain their exact raw descriptors after that window has been cleared.
 * These checks never grant a fresh PAGE operation or release replay debt. */
static bool
cluster_recovery_cold_pending_page_current(const struct ClusterRecoveryStartV1 *start)
{
	const ClusterColdCtrcPageProgressV1 *progress = start->ctrc_page_progress;
	const RfPageCtrcStepResultV1 *result;

	if (progress == NULL)
		return true;
	if (progress->target_count > (MaxAllocSize - sizeof(*progress))
			/ sizeof(ClusterColdCtrcPageCompletedV1)
		|| progress->allocation_bytes != sizeof(*progress)
			+ (Size)progress->target_count * sizeof(ClusterColdCtrcPageCompletedV1)
		|| progress->reserved_bytes < progress->allocation_bytes
		|| progress->budget <= progress->reserved_bytes
		|| progress->next_target > progress->target_count
		|| (progress->pending && progress->next_target == progress->target_count))
		return false;
	for (uint32 i = 0; i < progress->next_target; i++) {
		const ClusterColdCtrcPageCompletedV1 *completed = &progress->completed[i];
		static const RfPageCtrcFpiInsertV1 empty = {0};

		if (completed->storage != NULL
			&& (!cluster_shared_fs_page_current_v1(completed->storage)
				|| (completed->result.phase == RF_PAGE_CTRC_STEP_FPI_DURABLE
					? !completed->result.inserted.wal_flushed
					: completed->result.phase != RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED
						|| !completed->unchanged.ready
						|| memcmp(&completed->result.inserted, &empty, sizeof(empty)) != 0)))
			return false;
	}
	if (!progress->pending) return true;
	result = &progress->result;
	return result->phase == RF_PAGE_CTRC_STEP_FPI_DURABLE
		&& result->inserted.wal_flushed
		&& cluster_recovery_stamp_fpi_durable_current_v1(&result->inserted.window,
			result->inserted.record_start, result->inserted.record_end)
		&& progress->storage != NULL && cluster_shared_fs_page_current_v1(progress->storage);
}

bool
ClusterColdRecoveryTxCurrent(const struct ClusterRecoveryStartV1 *start)
{
	ClusterWalWriterToken writer;
	uint8 operation_uuid[16];
	uint64 install_generation;

	/* Compare the opaque pointer before any dereference, including stale ones. */
	if (start == NULL || start != cluster_recovery_cold_start || start->revoked
		|| start->generation == 0 || start->serial == 0 || start->pid != MyProcPid
		|| start->owner != CurrentResourceOwner || start->owner_boot == 0
		|| start->owner_boot != cluster_qvotec_get_self_incarnation()
		|| !cluster_recovery_merge_claim_owner_current_nowait_v1(start->claim_owner)
		|| start->inputs != cluster_recovery_cold_inputs || start->inputs == NULL
		|| start->inputs->inputs != NULL || start->inputs->plan == NULL
		|| start->tx_fence == NULL || start->tx_fence != start->inputs->tx_fence
		|| start->census != start->inputs->top_tx_census || start->census == NULL
		|| start->census->plan != start->inputs->plan
		|| !ClusterStartupInstalledRecoveryWriterV1(&writer, operation_uuid, &install_generation))
		return false;
	return start->install_generation == install_generation
		&& cluster_recovery_cold_writer_same(&start->writer, &writer)
		&& memcmp(start->operation_uuid, operation_uuid, 16) == 0
		&& cluster_recovery_cold_pending_page_current(start);
}

bool
ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *out)
{
	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !ClusterColdRecoveryTxCurrent(start))
		return false;
	out->writer = start->writer;
	memcpy(out->operation_uuid, start->operation_uuid, 16);
	out->install_generation = start->install_generation;
	out->start_generation = start->generation;
	out->start_serial = start->serial;
	out->owner_boot = start->owner_boot;
	out->owner_pid = start->pid;
	return true;
}

bool
ClusterColdRecoveryRelmapDeclare(const struct ClusterRecoveryStartV1 *start)
{
	if (MyBackendType != B_STARTUP || CritSectionCount != 0
		|| !ClusterColdRecoveryTxCurrent(start))
		return false;
	if (start->relmap_declared)
		return true;
	if (start->ctrc_observation_started || !cluster_recovery_ctrc_require_relmap_v1(start))
		return false;
	/* The shared obligation is installed before exposing any input. */
	cluster_recovery_cold_start->relmap_declared = true;
	return true;
}

bool
ClusterColdRecoveryRelmapCurrent(const struct ClusterRecoveryStartV1 *start)
{
	return ClusterColdRecoveryTxCurrent(start) && start->relmap_declared;
}

void
ClusterColdRecoveryRelmapFail(const struct ClusterRecoveryStartV1 *start)
{
	/* A stale borrowed pointer is never dereferenced or used for cleanup. */
	if (start == NULL || start != cluster_recovery_cold_start
		|| !start->relmap_declared || start->pid != MyProcPid)
		return;
	cluster_recovery_cold_start->revoked = true;
	cluster_recovery_checkpoint_revoke_v1(start);
	/* Dependents, IR and WALR remain for the original reverse cleanup. */
}

const struct ClusterColdTypedV1 *
ClusterColdRecoveryTxInputs(const struct ClusterRecoveryStartV1 *start)
{
	return ClusterColdRecoveryTxCurrent(start) ? start->inputs : NULL;
}

bool
ClusterColdRecoveryStartOutputDistinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)start;

	/* Keep the opaque extent at its owner. No dereference, writer validation
	 * or authority is implied by this storage-only check. */
	return start != NULL && start == cluster_recovery_cold_start && out != NULL && bytes != 0
		&& (a <= b ? b - a >= bytes : a - b >= sizeof(*start));
}

bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	/* Locate the original pointer before reading it, including stale handles. */
	return ClusterColdRecoveryTxCurrent(start) && work != NULL
		&& work == start->inputs->ctrc_work && work->plan == start->inputs->plan
		&& work->top_tx_census == start->census && work->generation != 0
		&& work->generation == start->inputs->ctrc_work_generation;
}

const struct ClusterColdCtrcWorkV1 *
ClusterColdRecoveryCtrcWorkPrepare(const struct ClusterRecoveryStartV1 *start)
{
	if (!ClusterColdRecoveryTxCurrent(start)
		|| !cluster_cold_typed_ctrc_work_prepare_v1(cluster_recovery_cold_inputs)
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, cluster_recovery_cold_inputs->ctrc_work))
		return NULL;
	return cluster_recovery_cold_inputs->ctrc_work;
}

static void
cluster_recovery_cold_observation_refuse(struct ClusterRecoveryStartV1 *start,
	const char *stage, int result)
{
	ClusterRecoveryCensusDiag diag = {0};

	if (start->ctrc_observation_refusal[0] != '\0')
		return;
	if (start->ctrc_observation_census != NULL)
		(void)cluster_recovery_census_diag(start->ctrc_observation_census, &diag);
	snprintf(start->ctrc_observation_refusal, sizeof(start->ctrc_observation_refusal),
		"stage=%s result=%d phase=%d input=%d roots=%d graph=%d relation=%d "
		"mx=%d slru=%d relmap=%d join=%d xact=%d inventory=%d terminal=%d",
		stage, result, (int)diag.phase, (int)diag.input_result, (int)diag.roots_result,
		(int)diag.graph_result, (int)diag.relation_result, (int)diag.mx_result,
		(int)diag.slru_result, (int)diag.relmap_result, (int)diag.join_result,
		(int)diag.xact_result, (int)diag.inventory_result, (int)diag.terminal_result);
}

static bool
cluster_recovery_cold_observation_work_current(const struct ClusterRecoveryStartV1 *start)
{
	return start->ctrc_observation_work_copy != NULL
		&& start->ctrc_physical != NULL
		&& start->ctrc_physical->count == start->ctrc_observation_work_copy->counts.subject_count
		&& ClusterColdRecoveryCtrcWorkCurrent(start, start->ctrc_observation_work)
		&& start->inputs->census_root_captured
		&& memcmp(&start->inputs->census_root, &start->ctrc_observation_root,
			sizeof(start->ctrc_observation_root)) == 0
		/* Locate the original full-sized allocation before comparing entries.
		 * A reused address with a smaller new work cannot supply this extent. */
		&& start->ctrc_observation_work->generation == start->ctrc_observation_work_copy->generation
		&& memcmp(&start->ctrc_observation_work->counts,
			&start->ctrc_observation_work_copy->counts,
			sizeof(start->ctrc_observation_work->counts)) == 0
		&& memcmp(start->ctrc_observation_work, start->ctrc_observation_work_copy,
			start->ctrc_observation_work_bytes) == 0;
}

static bool
cluster_recovery_cold_observation_current(const struct ClusterRecoveryStartV1 *start)
{
	ClusterRecoveryTxResultObservationV1 tx;

	return cluster_recovery_cold_observation_work_current(start)
		&& cluster_recovery_ctrc_terminal_result_v1(start, start->ctrc_observation_work, &tx)
		&& memcmp(&tx, &start->ctrc_observation_tx, sizeof(tx)) == 0
		&& cluster_recovery_cold_observation_work_current(start);
}

/* One original TT_SLOT, including unrelated physical slots. Only the view
 * is indexed by work; the full original census is never shortened. */
static bool
cluster_recovery_cold_observation_physical(struct ClusterRecoveryStartV1 *start,
	const ClusterRecoveryUndoEntry *entry)
{
	const ClusterColdCtrcWorkV1 *work = start->ctrc_observation_work_copy;
	uint32 found = UINT32_MAX;
	uint8 *seen = (uint8 *)(start->ctrc_physical->entries + work->counts.subject_count);

	for (uint32 i = 0; i < work->counts.subject_count; i++) {
		const ClusterColdTtSlotInputV1 *subject = &work->entries[i];
		const ClusterColdTtSlotKeyV1 *key = &subject->key;
		if (key->owner_instance != entry->owner_instance || key->segment_id != entry->segment_id
			|| key->slot_offset != entry->slot_offset)
			continue;
		if (found != UINT32_MAX || key->segment_generation != entry->segment_generation
			|| key->xid != entry->slot.xid || key->slot_wrap != entry->slot.wrap
			|| key->xid != subject->slot.xid || key->slot_wrap != subject->slot.wrap) {
			cluster_recovery_cold_observation_refuse(start, "physical_identity", (int)i);
			return false;
		}
		found = i;
	}
	if (found == UINT32_MAX)
		return true;
	if (seen[found]) {
		cluster_recovery_cold_observation_refuse(start, "physical_duplicate", (int)found);
		return false;
	}
	memcpy(&start->ctrc_physical->entries[found], entry, sizeof(*entry));
	seen[found] = 1;
	return true;
}

static bool
cluster_recovery_cold_observation_physical_complete(struct ClusterRecoveryStartV1 *start)
{
	uint32 count = start->ctrc_observation_work_copy->counts.subject_count;
	const uint8 *seen = (const uint8 *)(start->ctrc_physical->entries + count);

	for (uint32 i = 0; i < count; i++)
		if (!seen[i]) {
			cluster_recovery_cold_observation_refuse(start, "physical_missing", (int)i);
			return false;
		}
	return true;
}

/* The complete raw view must also be usable by the original C0 terminal
 * consumer before Startup waits for independent CTRC service. Never replace
 * the retained pre-C0 work or raw bytes with these private derived values.
 * This validates terminal inputs, not history coverage or apply authority. */
static bool
cluster_recovery_cold_observation_terminals_current(struct ClusterRecoveryStartV1 *start)
{
	uint32 count = start->ctrc_observation_work_copy->counts.subject_count;

	for (uint32 i = 0; i < count; i++) {
		ClusterColdTtSlotInputV1 terminal;

		/* Check complete original inputs around each canonical/C0 lookup,
		 * including the last subject and late owner or accepted-TX loss. */
		if (!cluster_recovery_cold_observation_current(start)
			|| !cluster_recovery_ctrc_terminal_subject_v1(start, start->ctrc_observation_work,
				i, &start->ctrc_physical->entries[i], &terminal)
			|| !cluster_recovery_cold_observation_current(start)) {
			cluster_recovery_cold_observation_refuse(start, "physical_terminal", (int)i);
			return false;
		}
	}
	return cluster_recovery_cold_observation_current(start);
}

bool
ClusterColdRecoveryCtrcObserve(const struct ClusterRecoveryStartV1 *start, Size memory_budget)
{
	struct ClusterRecoveryStartV1 *state;
	volatile bool candidate = false, observed = false;
	ClusterRecoveryCensusManifest confirmed;

	if (start == NULL)
		return true;
	if (start != cluster_recovery_cold_start)
		return false;
	state = cluster_recovery_cold_start;
	if (state->ctrc_observation_started && !state->ctrc_observation_complete)
		return false;
	/* This original Startup boundary holds no PAGE/content or slot lock. */
	if (CritSectionCount != 0 || !ClusterColdRecoveryTxCurrent(start)) {
		state->ctrc_observation_started = true;
		state->ctrc_observation_complete = false;
		cluster_recovery_cold_observation_refuse(state, "owner", 0);
		return false;
	}
	PG_TRY();
	{
		if (state->ctrc_observation_started) {
			if (cluster_recovery_cold_observation_current(start)
				&& cluster_recovery_census_manifest(state->ctrc_observation_census, &confirmed)
				&& memcmp(&confirmed, &state->ctrc_observation_manifest, sizeof(confirmed)) == 0)
				candidate = true;
			else
				cluster_recovery_cold_observation_refuse(state, "revalidate", 0);
		} else {
			const ClusterColdCtrcWorkV1 *work = start->inputs->ctrc_work;
			Size bytes = 0, physical_bytes = 0;
			ClusterControlRootResult scope_result;
			ClusterRecoveryCensusResult result;
			ClusterRecoveryCensusEntry entry;
			ClusterRecoveryTxResultObservationV1 tx;

			state->ctrc_observation_started = true;
			do {
				if (!ClusterColdRecoveryCtrcWorkCurrent(start, work)
					|| work->counts.subject_count > (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries))
						/ sizeof(work->entries[0])) {
					cluster_recovery_cold_observation_refuse(state, "work", 0);
					break;
				}
				bytes = offsetof(ClusterColdCtrcWorkV1, entries)
					+ (Size)work->counts.subject_count * sizeof(work->entries[0]);
				/* One complete view plus one presence byte per original subject.
				 * Reserve all storage before opening the original input scope. */
				if (work->counts.subject_count > (MaxAllocSize - offsetof(ClusterColdCtrcPhysicalV1, entries))
						/ (sizeof(ClusterRecoveryUndoEntry) + 1)) {
					cluster_recovery_cold_observation_refuse(state, "physical_budget", 0);
					break;
				}
				physical_bytes = offsetof(ClusterColdCtrcPhysicalV1, entries)
					+ (Size)work->counts.subject_count * (sizeof(ClusterRecoveryUndoEntry) + 1);
				if (memory_budget > MaxAllocSize || memory_budget <= bytes
					|| physical_bytes >= memory_budget - bytes) {
					cluster_recovery_cold_observation_refuse(state, "memory_budget", 0);
					break;
				}
				state->ctrc_observation_work = work;
				state->ctrc_observation_work_bytes = bytes;
				state->ctrc_observation_root = start->inputs->census_root;
				state->ctrc_observation_work_copy = MemoryContextAllocExtended(TopMemoryContext,
					bytes, MCXT_ALLOC_NO_OOM);
				if (state->ctrc_observation_work_copy == NULL) {
					cluster_recovery_cold_observation_refuse(state, "work_allocation", 0);
					break;
				}
				memcpy(state->ctrc_observation_work_copy, work, bytes);
				state->ctrc_physical = MemoryContextAllocExtended(TopMemoryContext,
					physical_bytes, MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
				if (state->ctrc_physical == NULL) {
					cluster_recovery_cold_observation_refuse(state, "physical_allocation", 0);
					break;
				}
				state->ctrc_physical->count = work->counts.subject_count;
				if (!cluster_recovery_ctrc_terminal_result_v1(start, work, &tx)) {
					cluster_recovery_cold_observation_refuse(state, "accepted_tx", 0);
					break;
				}
				state->ctrc_observation_tx = tx;
				if (!cluster_recovery_cold_observation_current(start)) {
					cluster_recovery_cold_observation_refuse(state, "accepted_tx", 0);
					break;
				}
				scope_result = cluster_wal_inputs_begin_ctrc_census_v1(start, work,
					&state->ctrc_observation_inputs);
				if (scope_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
					cluster_recovery_cold_observation_refuse(state, "scope_begin", (int)scope_result);
					break;
				}
				result = cluster_recovery_census_begin_ctrc(state->ctrc_observation_inputs,
					memory_budget - bytes - physical_bytes, &state->ctrc_observation_census);
				if (result != CLUSTER_RECOVERY_CENSUS_MORE) {
					cluster_recovery_cold_observation_refuse(state, "census_begin", (int)result);
					break;
				}
				for (;;) {
					HandleStartupProcInterrupts();
					if (!cluster_recovery_cold_observation_current(start)) {
						cluster_recovery_cold_observation_refuse(state, "original_input_changed", 0);
						break;
					}
					result = cluster_recovery_census_next(state->ctrc_observation_census, &entry);
					/* Every original kind and phase is visited; no TT/target or
					 * terminal filtering can shorten this complete observation. */
					if (result == CLUSTER_RECOVERY_CENSUS_ENTRY) {
						if (!cluster_recovery_cold_observation_current(start)) {
							cluster_recovery_cold_observation_refuse(state, "original_input_changed", 0);
							break;
						}
						if (entry.kind == CLUSTER_RECOVERY_CENSUS_TT_SLOT
							&& !cluster_recovery_cold_observation_physical(state, &entry.value.tt))
							break;
						continue;
					}
					if (result == CLUSTER_RECOVERY_CENSUS_MORE)
						continue;
					if (result != CLUSTER_RECOVERY_CENSUS_DRAINED) {
						cluster_recovery_cold_observation_refuse(state, "census_next", (int)result);
						break;
					}
					if (!cluster_recovery_cold_observation_physical_complete(state))
						break;
					if (!cluster_recovery_cold_observation_current(start)
						|| !cluster_recovery_census_manifest(state->ctrc_observation_census,
							&state->ctrc_observation_manifest)) {
						cluster_recovery_cold_observation_refuse(state, "manifest", 0);
						break;
					}
					candidate = true;
					break;
				}
			} while (false);
		}
		if (candidate) {
			HandleStartupProcInterrupts();
			if (cluster_recovery_cold_observation_current(start)
				&& cluster_recovery_census_manifest(state->ctrc_observation_census, &confirmed)
				&& memcmp(&confirmed, &state->ctrc_observation_manifest, sizeof(confirmed)) == 0
				&& cluster_recovery_cold_observation_current(start)
				&& cluster_recovery_cold_observation_terminals_current(state)) {
				const ClusterRecoveryCensusPagesV1 *pages =
					cluster_recovery_census_pages_v1(state->ctrc_observation_census);
				const ClusterRecoveryCensusReferencesV1 *references =
					cluster_recovery_census_references_v1(state->ctrc_observation_census);
				uint32 page_wal_count = 0, wal_source_count = 0;

				/* PGRAC: retain the complete original PAGE view only after all
				 * original terminals. A repeat cannot replace that borrowed view. */
				if (pages != NULL && references != NULL
					&& cluster_recovery_cold_observation_current(start)
					&& (state->ctrc_pages == NULL || state->ctrc_pages == pages)
					&& (state->ctrc_references == NULL || state->ctrc_references == references)
					&& cluster_recovery_census_page_wal_count_v1(state->ctrc_observation_census,
						&page_wal_count)
					&& (!state->ctrc_observation_complete
						|| page_wal_count == state->ctrc_page_wal_count)
					&& cluster_recovery_cold_observation_current(start)) {
					/* PGRAC: a zero PAGE count cannot omit non-PAGE or empty
					 * sources. Freeze the entire original manifest's source set. */
					if (cluster_recovery_census_wal_source_count_v1(state->ctrc_observation_census,
							&wal_source_count)
						&& wal_source_count != 0 && wal_source_count == confirmed.source_count
						&& (!state->ctrc_observation_complete
							|| wal_source_count == state->ctrc_wal_source_count)
						&& cluster_recovery_cold_observation_current(start)) {
						state->ctrc_references = references;
						state->ctrc_pages = pages;
						state->ctrc_page_wal_count = page_wal_count;
						state->ctrc_wal_source_count = wal_source_count;
						state->ctrc_observation_complete = true;
						observed = true;
					} else
						cluster_recovery_cold_observation_refuse(state, "wal_sources", 0);
				} else
					cluster_recovery_cold_observation_refuse(state, "page_inventory", 0);
			} else
				cluster_recovery_cold_observation_refuse(state, "manifest_changed", 0);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_observation_refuse(state, "error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!observed)
		cluster_recovery_cold_observation_close(state);
	return observed;
}

static void
cluster_recovery_cold_ctrc_lock_held(LWLock *lock, LWLockMode mode, void *arg)
{
	(void)lock;
	(void)mode;
	*(bool *)arg = true;
}

static bool
cluster_recovery_cold_ctrc_replays_actor(void)
{
	bool held = false;

	if (MyBackendType != B_STARTUP || !IsUnderPostmaster || CritSectionCount != 0
		|| ShutdownRequestPending)
		return false;
	/* Original owner checks may need a transient native LWLock. Do not enter
	 * them or a cancellable wait with caller PAGE/content/other locks held. */
	ForEachLWLockHeldByMe(cluster_recovery_cold_ctrc_lock_held, &held);
	return !held && !RecoveryInProgress();
}

static bool
cluster_recovery_cold_ctrc_replays_current(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const struct ClusterWalInputsV1 *scope)
{
	return cluster_recovery_cold_ctrc_replays_actor()
		&& start != NULL && start == cluster_recovery_cold_start
		&& start->ctrc_observation_complete && scope != NULL
		&& ClusterColdRecoveryCtrcScope(start, work) == scope;
}

bool
ClusterColdRecoveryCtrcReplaysAcquire(const struct ClusterRecoveryStartV1 *start)
{
	const ClusterColdCtrcWorkV1 *work;
	struct ClusterWalInputsV1 *scope;

	if (start == NULL)
		return true;
	if (!cluster_recovery_cold_ctrc_replays_actor()
		|| start != cluster_recovery_cold_start || !start->ctrc_observation_complete)
		return false;
	work = start->ctrc_observation_work;
	scope = ClusterColdRecoveryCtrcScope(start, work);
	if (scope == NULL)
		return false;
	for (;;) {
		ClusterControlRootResult result;

		ResetLatch(MyLatch);
		HandleStartupProcInterrupts();
		if (!cluster_recovery_cold_ctrc_replays_current(start, work, scope))
			return false;
		result = cluster_wal_inputs_ctrc_replays_acquire_v1(scope);
		/* Full preflight can retire the original observation. Locate it again
		 * through Start before passing the borrowed scope to any consumer. */
		if (!cluster_recovery_cold_ctrc_replays_current(start, work, scope))
			return false;
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (!cluster_wal_inputs_ctrc_replays_authorized_v1(scope)
				|| !cluster_recovery_cold_ctrc_replays_current(start, work, scope)) {
				if (start == cluster_recovery_cold_start)
					cluster_recovery_cold_observation_refuse(cluster_recovery_cold_start,
						"replay_holders", 0);
				return false;
			}
			if (!ClusterColdRecoveryCtrcObserve(start, 0)
				|| !cluster_recovery_cold_ctrc_replays_current(start, work, scope)
				|| !cluster_wal_inputs_ctrc_replays_authorized_v1(scope)
				|| !cluster_recovery_cold_ctrc_replays_current(start, work, scope))
				return false;
			return true;
		}
		if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT) {
			cluster_recovery_cold_observation_refuse(cluster_recovery_cold_start,
				"replay_acquire", (int)result);
			return false;
		}
		/* Retain this prepared set and every partial native grant. A local
		 * poll timeout is never a transaction failure or a new source round. */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
			100L, WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
	}
}

static bool
cluster_recovery_cold_page_plan_current(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, struct ClusterWalInputsV1 *scope)
{
	return cluster_recovery_cold_ctrc_replays_current(start, work, scope)
		&& cluster_wal_inputs_ctrc_replays_authorized_v1(scope)
		&& cluster_recovery_cold_ctrc_replays_current(start, work, scope);
}

static void
cluster_recovery_cold_page_plan_refuse(struct ClusterRecoveryStartV1 *start,
	RfPageProofDetailV1 detail)
{
	if (start != cluster_recovery_cold_start)
		return;
	cluster_recovery_cold_observation_refuse(start, "page_plan", (int)detail);
	start->revoked = true;
	cluster_recovery_checkpoint_revoke_v1(start);
	/* Original ResourceOwner still owns the graph/census/IR/WALR debt. */
}

/* Full graph order is distinct from original input order. Every source must
 * retain its exact native generation and canonical physical cut. Calls may
 * retire the published vector; copy a row first and re-locate Start before
 * dereferencing the next borrowed byte. Candidate storage is still private. */
static bool
cluster_recovery_cold_page_sources_current(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, struct ClusterWalInputsV1 *scope,
	const RfPageOnlinePlanV1 *plan, const ClusterRecoveryCtrcReplaySetSourceV1 *sources,
	uint32 count)
{
	bool seen[RF_PAGE_STABLE_MAX_PARTICIPANTS] = {false};
	bool published;
	ClusterWalSourceRef extra;

	if (!cluster_recovery_cold_page_plan_current(start, work, scope)
		|| plan == NULL || sources == NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS || count != start->ctrc_wal_source_count
		|| cluster_wal_inputs_count_v1(scope) != count
		|| !cluster_recovery_cold_page_plan_current(start, work, scope))
		return false;
	published = sources == start->ctrc_page_sources;
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryCtrcReplaySetSourceV1 source;
		ClusterRecoveryWalSourceObservationV1 observed;
		ClusterWalSourceRef bound;

		if (!cluster_recovery_cold_page_plan_current(start, work, scope)
			|| (published && (start->ctrc_page_sources != sources
				|| start->ctrc_page_source_count != count || start->ctrc_page_plan != plan)))
			return false;
		memcpy(&source, &sources[i], sizeof(source));
		if (!cluster_wal_inputs_ctrc_replays_source_current_v1(&source)
			|| !cluster_recovery_cold_page_plan_current(start, work, scope)
			|| (published && (start->ctrc_page_sources != sources
				|| start->ctrc_page_source_count != count || start->ctrc_page_plan != plan))
			|| !rf_page_online_plan_source_v1(plan, i, &bound)
			|| !cluster_recovery_census_wal_source_at_v1(start->ctrc_observation_census,
				i, &observed)
			|| !cluster_recovery_cold_page_plan_current(start, work, scope)
			|| (published && (start->ctrc_page_sources != sources
				|| start->ctrc_page_source_count != count || start->ctrc_page_plan != plan)))
			return false;
		if (source.scope != scope || source.generation_count != count
			|| source.generation_index >= count || seen[source.generation_index]
			|| observed.source_index != i
			|| memcmp(&bound, &observed.source, sizeof(bound)) != 0
			|| memcmp(&bound, &source.generation.source, sizeof(bound)) != 0
			|| observed.cut.failed_thread != bound.claim.identity.origin_thread_id
			|| observed.cut.origin_owner_incarnation != bound.claim.identity.origin_owner_incarnation
			|| observed.cut.timeline_id != bound.timeline
			|| observed.cut.flags != source.generation_flags
			|| observed.cut.scan_begin_inclusive != source.generation_begin
			|| observed.cut.scan_end_exclusive != source.generation_end
			|| memcmp(&source, &sources[i], sizeof(source)) != 0)
			return false;
		seen[source.generation_index] = true;
	}
	/* A prefix of a larger sealed graph is never complete membership. */
	return !rf_page_online_plan_source_v1(plan, count, &extra)
		&& cluster_recovery_cold_page_plan_current(start, work, scope);
}

bool
ClusterColdRecoveryCtrcPagePlanPrepare(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget)
{
	struct ClusterRecoveryStartV1 *state;
	const ClusterColdCtrcWorkV1 *work;
	struct ClusterWalInputsV1 *scope;
	RfPageOnlinePlanV1 *volatile candidate = NULL;
	ClusterRecoveryCtrcReplaySetSourceV1 *volatile sources = NULL;
	Size budget, source_bytes;
	uint32 count;
	bool existing, ready = false;

	if (start == NULL)
		return true;
	if (!cluster_recovery_cold_ctrc_replays_actor() || start != cluster_recovery_cold_start
		|| !start->ctrc_observation_complete
		|| (start->ctrc_page_plan_started && (start->ctrc_page_plan == NULL
			|| start->ctrc_page_sources == NULL || start->ctrc_page_source_count == 0))
		|| (!start->ctrc_page_plan_started && memory_budget == 0))
		return false;
	state = cluster_recovery_cold_start;
	work = state->ctrc_observation_work;
	scope = ClusterColdRecoveryCtrcScope(start, work);
	if (!cluster_recovery_cold_page_plan_current(start, work, scope)
		|| !ClusterColdRecoveryCtrcObserve(start, 0)
		|| !cluster_recovery_cold_page_plan_current(start, work, scope))
		return false;
	existing = state->ctrc_page_plan_started;
	count = state->ctrc_wal_source_count;
	budget = Min(memory_budget, (Size)RF_PAGE_ONLINE_PLAN_MAX_BYTES);
	source_bytes = count <= RF_PAGE_STABLE_MAX_PARTICIPANTS
		? (Size)count * sizeof(*sources) : 0;
	state->ctrc_page_plan_started = true;
	PG_TRY();
	{
		if (existing) {
			ready = cluster_recovery_cold_page_sources_current(start, work, scope,
				state->ctrc_page_plan, state->ctrc_page_sources, state->ctrc_page_source_count);
			if (!ready)
				cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_ROOT_STALE);
		} else if (count == 0 || count > RF_PAGE_STABLE_MAX_PARTICIPANTS
			|| source_bytes >= budget)
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_CAPACITY);
		else if (cluster_wal_inputs_count_v1(scope) != count
			|| !cluster_recovery_cold_page_plan_current(start, work, scope))
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING);
		else {
			ClusterRecoveryPagePlanBuildV1 built;
			RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_OOM;

			/* Reserve the entire vector before the graph's own allocation.
			 * No second budget or refreshed source round is introduced. */
			sources = MemoryContextAllocExtended(TopMemoryContext, source_bytes,
				MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
			if (sources != NULL) {
				built = cluster_recovery_census_page_plan_v1(
					state->ctrc_observation_census, budget - source_bytes);
				candidate = built.plan;
				detail = built.detail;
				if (detail == RF_PAGE_PROOF_DETAIL_OK && candidate == NULL)
					detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
			}
			if (detail == RF_PAGE_PROOF_DETAIL_OK
				&& cluster_recovery_cold_page_plan_current(start, work, scope)
				&& cluster_wal_inputs_count_v1(scope) == count) {
				bool seen[RF_PAGE_STABLE_MAX_PARTICIPANTS] = {false};

				for (uint32 j = 0; j < count; j++) {
					ClusterRecoveryCtrcReplaySetSourceV1 source;
					uint32 match = count;

					if (!cluster_recovery_cold_page_plan_current(start, work, scope)) {
						detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE; break;
					}
					source = cluster_wal_inputs_ctrc_generation_v1(scope, j);
					if (!cluster_recovery_cold_page_plan_current(start, work, scope)
						|| source.scope != scope || source.generation_count != count
						|| source.generation_index != j) {
						detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING; break;
					}
					for (uint32 i = 0; i < count; i++) {
						ClusterWalSourceRef bound;

						if (!rf_page_online_plan_source_v1(candidate, i, &bound)) {
							detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP; break;
						}
						if (memcmp(&bound, &source.generation.source, sizeof(bound)) != 0)
							continue;
						if (match != count) {
							detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH; break;
						}
						match = i;
					}
					if (detail != RF_PAGE_PROOF_DETAIL_OK) break;
					if (match == count || seen[match]) {
						detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING; break;
					}
					memcpy(&sources[match], &source, sizeof(source));
					seen[match] = true;
				}
			} else if (detail == RF_PAGE_PROOF_DETAIL_OK)
				detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
			if (detail == RF_PAGE_PROOF_DETAIL_OK
				&& cluster_recovery_cold_page_sources_current(start, work, scope,
					candidate, sources, count)
				&& ClusterColdRecoveryCtrcObserve(start, 0)
				&& cluster_recovery_cold_page_sources_current(start, work, scope,
					candidate, sources, count)) {
				state->ctrc_page_plan = candidate;
				state->ctrc_page_sources = sources;
				state->ctrc_page_source_count = count;
				candidate = NULL; sources = NULL;
				ready = true;
			} else
				cluster_recovery_cold_page_plan_refuse(state,
					detail == RF_PAGE_PROOF_DETAIL_OK ? RF_PAGE_PROOF_DETAIL_ROOT_STALE : detail);
		}
	}
	PG_CATCH();
	{
		RfPageOnlinePlanV1 *discard = candidate;
		candidate = NULL;
		rf_page_online_plan_destroy_v1(&discard);
		if (sources != NULL) { pfree(sources); sources = NULL; }
		cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_INTERNAL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (candidate != NULL) {
		RfPageOnlinePlanV1 *discard = candidate;
		rf_page_online_plan_destroy_v1(&discard);
	}
	if (sources != NULL) pfree(sources);
	return ready;
}

const RfPageOnlinePlanV1 *
ClusterColdRecoveryCtrcPagePlan(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work)
{
	if (start == NULL || start != cluster_recovery_cold_start
		|| work != start->ctrc_observation_work)
		return NULL;
	return ClusterColdRecoveryCtrcPagePlanPrepare(start, 0) ? start->ctrc_page_plan : NULL;
}

const struct ClusterRecoveryCtrcReplaySetSourceV1 *
ClusterColdRecoveryCtrcPageSources(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 participant_count)
{
	if (start == NULL || start != cluster_recovery_cold_start
		|| work != start->ctrc_observation_work || participant_count == 0
		|| participant_count != start->ctrc_page_source_count)
		return NULL;
	return ClusterColdRecoveryCtrcPagePlanPrepare(start, 0) ? start->ctrc_page_sources : NULL;
}

/* Re-locate every borrowed graph/vector/progress through the original Start
 * after calls which may retire its observation. This runs only outside PAGE. */
static bool
cluster_recovery_cold_page_advance_current(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	const ClusterColdCtrcPageProgressV1 *progress)
{
	return cluster_recovery_cold_page_plan_current(start, work, scope)
		&& start->ctrc_page_progress == progress
		&& (progress == NULL || (progress->plan == start->ctrc_page_plan
			&& progress->sources == start->ctrc_page_sources
			&& progress->source_count == start->ctrc_page_source_count))
		&& cluster_recovery_cold_page_sources_current(start, work, scope,
			start->ctrc_page_plan, start->ctrc_page_sources, start->ctrc_page_source_count);
}

/* Identity mapping is provisional; the existing native proof and protected
 * reader still establish graph/holder/SPACE/version/complete-byte validity. */
static bool
cluster_recovery_cold_page_target(const struct ClusterRecoveryStartV1 *start,
	const RfPageOnlineTargetViewV1 *target, ClusterRecoveryCensusPageV1 *out)
{
	const RfPageIdentityV1 *id = &target->page_identity;
	bool found = false;

	if (start->ctrc_pages == NULL) return false;
	for (uint32 i = 0; i < start->ctrc_pages->count; i++) {
		const ClusterRecoveryCensusPageV1 *page = &start->ctrc_pages->entries[i];
		const ClusterSpaceIdentity *space = &page->reservation.identity;

		if (id->system_identifier != space->key.system_identifier
			|| memcmp(id->storage_uuid, space->key.storage_uuid, 16) != 0
			|| id->locator.spcOid != space->key.locator.spcOid
			|| id->locator.dbOid != space->key.locator.dbOid
			|| id->locator.relNumber != space->key.locator.relNumber
			|| id->forknum != (uint32)page->forknum || id->blockno != page->blockno)
			continue;
		if (found || space->state != CLUSTER_SPACE_IDENTITY_LIVE
			|| (page->forknum == MAIN_FORKNUM && page->blockno >= page->reservation.next_block)
			|| memcmp(target->expected_result.segment_incarnation, space->incarnation, 16) != 0)
			return false;
		*out = *page;
		found = true;
	}
	return found;
}

/* Select the terminal edge's original generation, not a TT/census/replay
 * ordinal or a numerically greatest LSN from unrelated threads. */
static bool
cluster_recovery_cold_page_tail(const struct ClusterRecoveryStartV1 *start,
	const RfPageOnlineTargetViewV1 *target, uint32 *index)
{
	const RfContributorVectorV1 *v = target->contributors;
	bool found = false;

	if (v == NULL || v->edges == NULL || v->edge_count == 0
		|| v->edge_count > RF_PAGE_STABLE_MAX_EDGES) return false;
	for (uint32 i = 0; i < v->edge_count; i++) {
		const RfPageStableEdgeInputV1 *edge = &v->edges[i];
		const ClusterRecoveryCtrcReplaySetSourceV1 *source;
		const RfPageReplayRecordIdentityV1 *record = &edge->record_identity;

		if (edge->result_token != target->expected_result.mutation_token
			|| memcmp(edge->edge.result_incarnation, target->expected_result.segment_incarnation, 16) != 0)
			continue;
		if (found || edge->edge.result_kind != RF_PAGE_STATE_PRESENT
			|| memcmp(&edge->page_identity, &target->page_identity, sizeof(edge->page_identity)) != 0
			|| edge->participant_index >= start->ctrc_page_source_count)
			return false;
		source = &start->ctrc_page_sources[edge->participant_index];
		if (record->origin_thread != source->generation.source.claim.identity.origin_thread_id
			|| record->timeline_id != source->generation.source.timeline
			|| record->read_rec_ptr < source->generation_begin
			|| record->read_rec_ptr >= record->end_rec_ptr || record->end_rec_ptr > source->generation_end)
			return false;
		*index = edge->participant_index;
		found = true;
	}
	return found;
}

/* A refused return can still carry an outstanding operation. Preserve it
 * before PAGE cleanup or original currentness checks can fail. */
static bool
cluster_recovery_cold_page_step_has_operation(const RfPageCtrcStepResultV1 *step)
{
	return step->inserted.window.key.window_seq != 0 || step->inserted.record_start != 0
		|| step->inserted.record_end != 0 || step->inserted.wal_flushed;
}

/* Classify only after PAGE cleanup and original currentness checks. Failed
 * installation never turns the retained operation into a new attempt. */
static RfPageProofDetailV1
cluster_recovery_cold_page_step_result(ClusterColdCtrcPageProgressV1 *progress,
	const RfPageCtrcStepResultV1 *step)
{
	if (cluster_recovery_cold_page_step_has_operation(step)) {
		if (step->phase == RF_PAGE_CTRC_STEP_FPI_DURABLE
			&& step->inserted.window.key.window_seq != 0 && step->inserted.wal_flushed
			&& step->inserted.record_start < step->inserted.record_end)
			return RF_PAGE_PROOF_DETAIL_OK;
		return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
	}
	if (step->phase == RF_PAGE_CTRC_STEP_WAIT) return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	if (step->phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED) {
		progress->next_target++;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
}

/* Private PAGE/proof cleanup never retires a borrowed IR/WALR or an INSERTED
 * window. The original native step retains failed WAL debt on every ERROR. */
static RfPageProofDetailV1
cluster_recovery_cold_page_attempt(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	ClusterColdCtrcPageProgressV1 *progress, const RfPageOnlineTargetViewV1 *view,
	const ClusterRecoveryCensusPageV1 *physical_page, uint32 source_index)
{
	RfPageStableBaseProofV1 *volatile proof = NULL;
	RfPageCtrcAuthorityV1 *volatile authority = NULL;
	RfPageAuthorityTargetV1 target = {0};
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	Size available = progress->budget - progress->reserved_bytes;
	Size part = available / 3;
	bool pending = progress->pending;
	RfPageCtrcStepResultV1 pending_result = progress->result;
	struct ClusterSharedFsPageV1 *pending_storage = progress->storage;
	uint32 pending_target = progress->next_target;

	PG_TRY();
	{
		RfPageStableCtrcBuildV1 built = rf_page_stable_base_proof_build_ctrc_replays_v1(
			view->graph, start->ctrc_page_sources, start->ctrc_page_source_count, part);
		proof = built.proof;
		detail = built.detail;
		if (detail == RF_PAGE_PROOF_DETAIL_OK && proof != NULL
			&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)) {
			RfPageCtrcAuthorityBuildV1 page;

			target.page_identity = view->page_identity;
			target.expected_result = view->expected_result;
			target.stable_base = proof;
			target.source = view->source;
			target.contributors = view->contributors;
			page = rf_page_authority_ctrc_replays_preflight_v1(&target, 1,
				start->ctrc_page_sources, start->ctrc_page_source_count, part);
			authority = page.authority;
			detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
			if (page.verdict == RF_PAGE_AUTHORITY_OK && authority != NULL
				&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)) {
				RfPageCtrcHeldTargetV1 held = {0};
				RfPageCtrcAuthorityV1 *release = authority;
				const RfPageCtrcStepResultV1 *step = &held.step.result;

				if (pending) {
					const GenericXLogRecoveryImageV1 *image = &pending_result.inserted.image;

					if (image->block_number == view->page_identity.blockno
						&& memcmp(&image->identity, &physical_page->reservation.identity,
							sizeof(image->identity)) == 0
						&& memcmp(&image->version.before, &view->expected_result,
							sizeof(view->expected_result)) == 0)
						detail = rf_page_recovery_ctrc_replays_fpi_readback_v1(start, work,
							&start->ctrc_page_sources[source_index], authority,
							&progress->result.inserted, pending_storage,
							work->counts.subject_count, available - 2 * part);
				} else {
					held = rf_page_recovery_ctrc_replays_itl_graph_hold_target_smgr_v1(
						start, work, &start->ctrc_page_sources[source_index], authority,
						&physical_page->reservation.identity, view->page_identity.blockno,
						&view->expected_result, view->canonical_page, start->ctrc_physical->entries,
						start->ctrc_physical->count, available - 2 * part);
					if (cluster_recovery_cold_page_step_has_operation(step)) {
						progress->result = *step;
						progress->storage = held.step.storage;
						progress->pending = true;
					} else if (step->phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED) {
						/* No callback between return and transfer to original
						 * cleanup, even if PAGE destruction subsequently ERRORs. */
						progress->completed[pending_target].result = *step;
						progress->completed[pending_target].storage = held.step.storage;
						progress->completed[pending_target].unchanged = held.unchanged;
					}
				}
				authority = NULL;
				rf_page_authority_ctrc_replays_destroy_v1(&release);
				/* PAGE is gone before any original ROOT/current snapshot. */
				if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress))
					detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
				else if (pending) {
					if (!progress->pending || progress->next_target != pending_target
						|| progress->storage != pending_storage
						|| memcmp(&progress->result, &pending_result, sizeof(pending_result)) != 0)
						detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
					else if (detail == RF_PAGE_PROOF_DETAIL_OK) {
						ClusterCtrcLocalEmptyV1 empty = cluster_ctrc_recovery_local_empty_v1();

						/* Observe only after PAGE cleanup; then revalidate the whole
						 * original owner and saved operation before using the result.
						 * Even EMPTY does not clear a window or release a reference. */
						if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
							|| !progress->pending || progress->next_target != pending_target
							|| progress->storage != pending_storage
							|| memcmp(&progress->result, &pending_result, sizeof(pending_result)) != 0)
							detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
						else if (empty != CLUSTER_CTRC_LOCAL_EMPTY)
							detail = empty == CLUSTER_CTRC_LOCAL_WAIT
								? RF_PAGE_PROOF_DETAIL_WOULD_BLOCK : RF_PAGE_PROOF_DETAIL_ROOT_STALE;
					}
				} else if (step->phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED) {
					const ClusterColdCtrcPageCompletedV1 *completed = &progress->completed[pending_target];

					if (progress->pending || progress->next_target != pending_target
						|| held.step.storage == NULL || !held.unchanged.ready
						|| completed->storage != held.step.storage
						|| memcmp(&completed->result, step, sizeof(*step)) != 0
						|| memcmp(&completed->unchanged, &held.unchanged, sizeof(held.unchanged)) != 0
						|| !cluster_shared_fs_page_current_v1(held.step.storage))
						detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
					else detail = cluster_recovery_cold_page_step_result(progress, step);
				} else detail = cluster_recovery_cold_page_step_result(progress, step);
			}
		}
		if (detail == RF_PAGE_PROOF_DETAIL_OK && proof == NULL)
			detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
	}
	PG_FINALLY();
	{
		RfPageCtrcAuthorityV1 *release = authority;
		RfPageStableBaseProofV1 *discard = proof;
		authority = NULL; proof = NULL;
		rf_page_authority_ctrc_replays_destroy_v1(&release);
		rf_page_stable_base_proof_destroy_v1(&discard);
	}
	PG_END_TRY();
	return detail;
}

bool
ClusterColdRecoveryCtrcPageAdvance(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget)
{
	struct ClusterRecoveryStartV1 *state;
	const ClusterColdCtrcWorkV1 *work;
	ClusterWalInputsV1 *scope;
	ClusterColdCtrcPageProgressV1 *progress;
	uint32 count;
	bool ready = false;

	if (start == NULL) return true;
	if (!cluster_recovery_cold_ctrc_replays_actor() || start != cluster_recovery_cold_start
		|| !ClusterColdRecoveryCtrcPagePlanPrepare(start, 0)) return false;
	state = cluster_recovery_cold_start;
	work = state->ctrc_observation_work;
	scope = state->ctrc_observation_inputs;
	progress = state->ctrc_page_progress;
	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	count = rf_page_online_plan_target_count_v1(start->ctrc_page_plan);
	/* Empty PAGE input is still subject to the original independent service. */
	if (count == 0) return true;
	if (progress == NULL) {
		Size budget = Min(memory_budget, (Size)RF_PAGE_ONLINE_PLAN_MAX_BYTES);
		Size bytes;
		Size storage_bytes = cluster_shared_fs_page_bytes_v1();
		Size projection_bytes;

		/* Reserve every result/FD slot before the first operation, never
		 * allocate while an INSERTED window waits for its continuation. */
		if (count > (MaxAllocSize - sizeof(*progress))
				/ sizeof(ClusterColdCtrcPageCompletedV1)) {
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_CAPACITY);
			return false;
		}
		bytes = sizeof(*progress) + (Size)count * sizeof(ClusterColdCtrcPageCompletedV1);
		/* Completed raw handles outlive each native step. Reserve the full
		 * preparation peak for every fixed target as well, before PAGE;
		 * the active step keeps its existing independent peak check. */
		if (work->counts.subject_count > (MaxAllocSize - 2 * sizeof(PGAlignedBlock))
				/ sizeof(ClusterColdTtSlotInputV1)) {
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_CAPACITY);
			return false;
		}
		projection_bytes = (Size)work->counts.subject_count * sizeof(ClusterColdTtSlotInputV1)
			+ 2 * sizeof(PGAlignedBlock);
		if (budget <= bytes || projection_bytes >= budget - bytes || storage_bytes == 0
			|| storage_bytes > (budget - bytes - projection_bytes) / count
			|| budget - bytes - projection_bytes <= (Size)count * storage_bytes) {
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_CAPACITY);
			return false;
		}
		progress = MemoryContextAllocExtended(TopMemoryContext, bytes,
			MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
		if (progress == NULL) {
			cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_OOM);
			return false;
		}
		progress->budget = budget;
		progress->allocation_bytes = bytes;
		/* The final original-subject snapshot and both whole-page images
		 * share this budget, reserved before the first PAGE/FPI as well. */
		progress->reserved_bytes = bytes + (Size)count * storage_bytes + projection_bytes;
		progress->plan = state->ctrc_page_plan;
		progress->sources = state->ctrc_page_sources;
		progress->source_count = state->ctrc_page_source_count;
		progress->target_count = count;
		state->ctrc_page_progress = progress;
	}
	PG_TRY();
	{
		for (;;) {
			RfPageOnlineTargetViewV1 view;
			ClusterRecoveryCensusPageV1 physical_page;
			RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
			uint32 source_index;
			bool pending;

			ResetLatch(MyLatch);
			HandleStartupProcInterrupts();
			if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) break;
			if (progress->target_count != count) break;
			pending = progress->pending;
			if (!pending && progress->next_target == count) { ready = true; break; }
			if (progress->next_target > count
				|| !rf_page_online_plan_target_v1(start->ctrc_page_plan, progress->next_target, &view)) break;
			/* These targets remain original obligations, not ITL projections. */
			if (view.history_only || view.page_identity.forknum != MAIN_FORKNUM) {
				if (pending) break;
				progress->next_target++; continue;
			}
			if (!cluster_recovery_cold_page_target(start, &view, &physical_page)) break;
			if (!physical_page.heap) {
				if (pending) break;
				progress->next_target++; continue;
			}
			if (!cluster_recovery_cold_page_tail(start, &view, &source_index)) break;
			detail = cluster_recovery_cold_page_attempt(start, work, scope, progress,
				&view, &physical_page, source_index);
			if (detail == RF_PAGE_PROOF_DETAIL_OK) {
				/* Readback retains the original pending window and FD. Its
				 * first/pin/companion and service duties are still outstanding. */
				if (pending) { ready = true; break; }
				continue;
			}
			if (detail != RF_PAGE_PROOF_DETAIL_WOULD_BLOCK) {
				if (start == cluster_recovery_cold_start)
					cluster_recovery_cold_observation_refuse(state, "page_execute", (int)detail);
				break;
			}
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				100L, WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_INTERNAL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ready) cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	return ready;
}

bool
ClusterColdRecoveryCtrcPageCompletedCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcFpiInsertV1 *inserted,
	struct ClusterSharedFsPageV1 *storage)
{
	const ClusterColdCtrcPageProgressV1 *progress;

	if (inserted == NULL || storage == NULL || CritSectionCount != 0
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work)
		return false;
	progress = start->ctrc_page_progress;
	if (progress == NULL || progress->pending || progress->next_target != progress->target_count
		|| progress->plan != start->ctrc_page_plan || progress->sources != start->ctrc_page_sources
		|| progress->source_count != start->ctrc_page_source_count)
		return false;
	for (uint32 i = 0; i < progress->next_target; i++) {
		const ClusterColdCtrcPageCompletedV1 *completed = &progress->completed[i];

		if (inserted == &completed->result.inserted)
			return storage == completed->storage
				&& completed->result.phase == RF_PAGE_CTRC_STEP_FPI_DURABLE
				&& inserted->wal_flushed && inserted->record_start < inserted->record_end
				&& inserted->window.key.window_seq != 0;
	}
	return false;
}

bool
ClusterColdRecoveryCtrcPageUnchangedCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageCtrcUnchangedV1 *unchanged,
	struct ClusterSharedFsPageV1 *storage)
{
	const ClusterColdCtrcPageProgressV1 *progress;

	if (unchanged == NULL || storage == NULL || CritSectionCount != 0
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work)
		return false;
	progress = start->ctrc_page_progress;
	if (progress == NULL || progress->pending || progress->next_target != progress->target_count
		|| progress->plan != start->ctrc_page_plan || progress->sources != start->ctrc_page_sources
		|| progress->source_count != start->ctrc_page_source_count) return false;
	for (uint32 i = 0; i < progress->next_target; i++) {
		const ClusterColdCtrcPageCompletedV1 *completed = &progress->completed[i];

		if (unchanged == &completed->unchanged)
			return storage == completed->storage && completed->unchanged.ready
				&& completed->result.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED;
	}
	return false;
}

/* The original result/FD vector remains owned by Start after F4 clear. Build
 * only a temporary original PAGE/proof, then compare the entire retained
 * operation again after both destructors, outside PAGE and before service. */
static RfPageProofDetailV1
cluster_recovery_cold_page_completed_attempt(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	ClusterColdCtrcPageProgressV1 *progress, uint32 index,
	const RfPageOnlineTargetViewV1 *view, const ClusterRecoveryCensusPageV1 *physical_page,
	uint32 source_index, PGAlignedBlock *out_image)
{
	RfPageStableBaseProofV1 *volatile proof = NULL;
	RfPageCtrcAuthorityV1 *volatile authority = NULL;
	RfPageCtrcStepResultV1 saved = progress->completed[index].result;
	RfPageCtrcUnchangedV1 unchanged = progress->completed[index].unchanged;
	struct ClusterSharedFsPageV1 *storage = progress->completed[index].storage;
	const GenericXLogRecoveryImageV1 *image = &saved.inserted.image;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	Size available = progress->budget - progress->reserved_bytes;
	Size part = available / 3;

	if (saved.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED
		? !unchanged.ready || unchanged.blockno != view->page_identity.blockno
			|| memcmp(&unchanged.space, &physical_page->reservation.identity, sizeof(unchanged.space)) != 0
			|| memcmp(&unchanged.before, &view->expected_result, sizeof(unchanged.before)) != 0
		: saved.phase != RF_PAGE_CTRC_STEP_FPI_DURABLE || image->block_number != view->page_identity.blockno
			|| memcmp(&image->identity, &physical_page->reservation.identity, sizeof(image->identity)) != 0
			|| memcmp(&image->version.before, &view->expected_result, sizeof(view->expected_result)) != 0)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	PG_TRY();
	{
		RfPageStableCtrcBuildV1 built = rf_page_stable_base_proof_build_ctrc_replays_v1(
			view->graph, start->ctrc_page_sources, start->ctrc_page_source_count, part);
		proof = built.proof; detail = built.detail;
		if (detail == RF_PAGE_PROOF_DETAIL_OK && proof != NULL
			&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)) {
			RfPageAuthorityTargetV1 target = {0};
			RfPageCtrcAuthorityBuildV1 page;

			target.page_identity = view->page_identity; target.expected_result = view->expected_result;
			target.stable_base = proof; target.source = view->source; target.contributors = view->contributors;
			page = rf_page_authority_ctrc_replays_preflight_v1(&target, 1,
				start->ctrc_page_sources, start->ctrc_page_source_count, part);
			authority = page.authority;
			detail = page.verdict == RF_PAGE_AUTHORITY_WOULD_BLOCK
				? RF_PAGE_PROOF_DETAIL_WOULD_BLOCK : RF_PAGE_PROOF_DETAIL_ROOT_STALE;
			if (page.verdict == RF_PAGE_AUTHORITY_OK && authority != NULL
				&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)
				&& index < progress->next_target && progress->completed[index].storage == storage
				&& memcmp(&progress->completed[index].result, &saved, sizeof(saved)) == 0
				&& memcmp(&progress->completed[index].unchanged, &unchanged, sizeof(unchanged)) == 0) {
				if (saved.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED
					&& ClusterColdRecoveryCtrcPageUnchangedCurrent(start, work,
						&progress->completed[index].unchanged, storage)) {
					if (out_image != NULL) {
						RfPageCtrcReadbackImageV1 copied = rf_page_recovery_ctrc_replays_unchanged_image_v1(
							start, work, &start->ctrc_page_sources[source_index], authority,
							&progress->completed[index].unchanged, storage,
							work->counts.subject_count, available - 2 * part);

						detail = copied.detail;
						if (detail == RF_PAGE_PROOF_DETAIL_OK) *out_image = copied.image;
					} else
						detail = rf_page_recovery_ctrc_replays_unchanged_readback_v1(start, work,
							&start->ctrc_page_sources[source_index], authority,
							&progress->completed[index].unchanged, storage,
							work->counts.subject_count, available - 2 * part);
				}
				else if (saved.phase == RF_PAGE_CTRC_STEP_FPI_DURABLE
					&& ClusterColdRecoveryCtrcPageCompletedCurrent(start, work,
						&progress->completed[index].result.inserted, storage)) {
					if (out_image != NULL) {
						RfPageCtrcReadbackImageV1 copied = rf_page_recovery_ctrc_replays_completed_image_v1(
							start, work, &start->ctrc_page_sources[source_index], authority,
							&progress->completed[index].result.inserted, storage,
							work->counts.subject_count, available - 2 * part);

						detail = copied.detail;
						if (detail == RF_PAGE_PROOF_DETAIL_OK) *out_image = copied.image;
					} else
						detail = rf_page_recovery_ctrc_replays_completed_readback_v1(start, work,
							&start->ctrc_page_sources[source_index], authority,
							&progress->completed[index].result.inserted, storage,
							work->counts.subject_count, available - 2 * part);
				}
			}
		}
		if (detail == RF_PAGE_PROOF_DETAIL_OK && proof == NULL)
			detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
	}
	PG_FINALLY();
	{
		RfPageCtrcAuthorityV1 *release = authority;
		RfPageStableBaseProofV1 *discard = proof;
		authority = NULL; proof = NULL;
		rf_page_authority_ctrc_replays_destroy_v1(&release);
		rf_page_stable_base_proof_destroy_v1(&discard);
	}
	PG_END_TRY();
	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		|| index >= progress->next_target || progress->completed[index].storage != storage
		|| memcmp(&progress->completed[index].result, &saved, sizeof(saved)) != 0
		|| memcmp(&progress->completed[index].unchanged, &unchanged, sizeof(unchanged)) != 0)
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	return detail;
}

RfPageCtrcReadbackImageV1
ClusterColdRecoveryCtrcPageImage(const struct ClusterRecoveryStartV1 *start, uint32 index)
{
	RfPageCtrcReadbackImageV1 result = {0};
	struct ClusterRecoveryStartV1 *state;
	ClusterColdCtrcPageProgressV1 *progress;
	const ClusterColdCtrcWorkV1 *work;
	ClusterWalInputsV1 *scope;

	result.detail = RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (start == NULL || start != cluster_recovery_cold_start
		|| !cluster_recovery_cold_ctrc_replays_actor()) return result;
	state = cluster_recovery_cold_start;
	progress = state->ctrc_page_progress;
	/* Absence, a skipped target or a pending prefix supplies no page image. */
	if (progress == NULL || progress->pending || progress->next_target != progress->target_count
		|| index >= progress->target_count || progress->completed[index].storage == NULL)
		return result;
	work = state->ctrc_observation_work; scope = state->ctrc_observation_inputs;
	PG_TRY();
	{
		RfPageOnlineTargetViewV1 view;
		ClusterRecoveryCensusPageV1 physical_page;
		uint32 source_index;

		result.detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			&& rf_page_online_plan_target_v1(start->ctrc_page_plan, index, &view)
			&& !view.history_only && view.page_identity.forknum == MAIN_FORKNUM
			&& cluster_recovery_cold_page_target(start, &view, &physical_page) && physical_page.heap
			&& cluster_recovery_cold_page_tail(start, &view, &source_index))
			result.detail = cluster_recovery_cold_page_completed_attempt(start, work, scope,
				progress, index, &view, &physical_page, source_index, &result.image);
		/* The attempt destroys PAGE/proof and rechecks every original input.
		 * No candidate bytes escape on late cleanup or progress drift. */
		if (result.detail == RF_PAGE_PROOF_DETAIL_OK
			&& (progress->pending || progress->next_target != progress->target_count))
			result.detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (result.detail != RF_PAGE_PROOF_DETAIL_OK) {
			MemSet(&result.image, 0, sizeof(result.image));
			if (result.detail != RF_PAGE_PROOF_DETAIL_WOULD_BLOCK)
				cluster_recovery_cold_page_plan_refuse(state, result.detail);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_INTERNAL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

/* Native physical TT is input only. Re-run every original canonical terminal
 * factory against the accepted TX result; never derive one from a page xid,
 * slot wrap, count, empty receipt directory or already-projected bytes. */
static bool
cluster_recovery_cold_page_projection_subjects(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	ClusterColdCtrcPageProgressV1 *progress, ClusterColdTtSlotInputV1 *subjects,
	const ClusterRecoveryTxResultObservationV1 *accepted, bool initialize)
{
	ClusterRecoveryTxResultObservationV1 current;
	const ClusterColdCtrcPhysicalV1 *physical;

	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	physical = start->ctrc_physical;
	if (physical == NULL || physical->count != work->counts.subject_count
		|| (physical->count != 0 && subjects == NULL)
		|| !cluster_recovery_ctrc_terminal_result_v1(start, work, &current)
		|| memcmp(&current, accepted, sizeof(current)) != 0) return false;
	for (uint32 i = 0; i < work->counts.subject_count; i++) {
		ClusterRecoveryUndoEntry observed;
		ClusterColdTtSlotInputV1 terminal;

		if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			|| start->ctrc_physical != physical || physical->count != work->counts.subject_count)
			return false;
		observed = physical->entries[i];
		if (!cluster_recovery_ctrc_terminal_subject_v1(start, work, i, &observed, &terminal)
			|| !cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			|| start->ctrc_physical != physical
			|| physical->count != work->counts.subject_count
			|| memcmp(&physical->entries[i], &observed, sizeof(observed)) != 0)
			return false;
		if (initialize) subjects[i] = terminal;
		else if (memcmp(&subjects[i], &terminal, sizeof(terminal)) != 0) return false;
	}
	return cluster_recovery_ctrc_terminal_result_v1(start, work, &current)
		&& memcmp(&current, accepted, sizeof(current)) == 0
		&& cluster_recovery_cold_page_advance_current(start, work, scope, progress);
}

/* Original census owns every byte. Bind one tuple only to the complete ITL
 * array of its own physical page and inverse version, never a newer slot or
 * another PAGE-WAL target. This is not a reconstructed physical page/proof. */
static bool
cluster_recovery_cold_page_reference_tuple(const ClusterRecoveryCensusReferencesV1 *references,
	const ClusterRecoveryCensusReferenceV1 *reference)
{
	const ClusterRecoveryHeapEntry *entry = &reference->entry;
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	uint32 seen = 0;

	if (entry->index < FirstOffsetNumber || entry->index > MaxHeapTuplesPerPage) return false;
	if (!ItemIdHasStorage(&entry->item))
		return entry->tuple_bytes == NULL && entry->tuple_length == 0
			&& (!ItemIdIsUsed(&entry->item) || ItemIdIsDead(&entry->item)
				|| ItemIdIsRedirected(&entry->item));
	if (!ItemIdIsNormal(&entry->item) || entry->tuple_bytes == NULL
		|| entry->tuple_length != ItemIdGetLength(&entry->item)
		|| entry->tuple_length > BLCKSZ) return false;
	for (uint32 i = 0; i < references->count; i++) {
		const ClusterRecoveryCensusReferenceV1 *candidate = &references->entries[i];
		uint32 bit;

		if (candidate->page_index != reference->page_index
			|| candidate->version != reference->version
			|| candidate->entry.kind != CLUSTER_RECOVERY_HEAP_ITL) continue;
		if (candidate->entry.index >= CLUSTER_ITL_INITRANS_DEFAULT) return false;
		bit = UINT32_C(1) << candidate->entry.index;
		if ((seen & bit) != 0) return false;
		seen |= bit;
		slots[candidate->entry.index] = candidate->entry.itl;
	}
	return seen == (UINT32_C(1) << CLUSTER_ITL_INITRANS_DEFAULT) - 1
		&& cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
			entry->tuple_bytes, entry->tuple_length);
}

/* Group only the original census page/version. Every line pointer, including
 * no-storage items, is present in native offset order. This is a private
 * reference view, not a reconstructed physical image or a history proof. */
static bool
cluster_recovery_cold_page_reference_hot(const ClusterRecoveryCensusReferencesV1 *references,
	const ClusterRecoveryCensusReferenceV1 *reference, BlockNumber block_number)
{
	ClusterCtrcRecoveryHeapItemV1 items[MaxHeapTuplesPerPage];
	uint8 item_seen[MaxHeapTuplesPerPage] = {0};
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	uint32 slot_seen = 0;
	Size count = 0;

	for (uint32 i = 0; i < references->count; i++) {
		const ClusterRecoveryCensusReferenceV1 *candidate = &references->entries[i];
		const ClusterRecoveryHeapEntry *entry = &candidate->entry;

		if (candidate->page_index != reference->page_index
			|| candidate->version != reference->version) continue;
		if (entry->kind == CLUSTER_RECOVERY_HEAP_ITL) {
			uint32 bit;

			if (entry->index >= CLUSTER_ITL_INITRANS_DEFAULT) return false;
			bit = UINT32_C(1) << entry->index;
			if ((slot_seen & bit) != 0) return false;
			slot_seen |= bit; slots[entry->index] = entry->itl;
		} else if (entry->kind == CLUSTER_RECOVERY_HEAP_ITEM) {
			Size index;

			if (entry->index < FirstOffsetNumber || entry->index > MaxHeapTuplesPerPage) return false;
			index = entry->index - FirstOffsetNumber;
			if (item_seen[index] != 0) return false;
			item_seen[index] = 1;
			items[index].item = entry->item;
			items[index].tuple_bytes = entry->tuple_bytes;
			items[index].tuple_length = entry->tuple_length;
			if (index >= count) count = index + 1;
		} else return false;
	}
	if (slot_seen != (UINT32_C(1) << CLUSTER_ITL_INITRANS_DEFAULT) - 1) return false;
	for (Size i = 0; i < count; i++)
		if (item_seen[i] == 0) return false;
	return cluster_ctrc_recovery_heap_hot_items_v1(slots, items, count, block_number);
}

/* Compare fields, never native structure padding. A ctid is relative to this
 * exact physical SPACE, not another relation or a reused incarnation. */
static bool
cluster_recovery_cold_reference_same_space(const ClusterSpaceIdentity *a,
	const ClusterSpaceIdentity *b)
{
	return a->key.system_identifier == b->key.system_identifier
		&& a->key.database_incarnation == b->key.database_incarnation
		&& memcmp(a->key.storage_uuid, b->key.storage_uuid, 16) == 0
		&& a->key.locator.spcOid == b->key.locator.spcOid
		&& a->key.locator.dbOid == b->key.locator.dbOid
		&& a->key.locator.relNumber == b->key.locator.relNumber
		&& memcmp(a->incarnation, b->incarnation, 16) == 0
		&& a->sequence == b->sequence && a->operation == b->operation && a->state == b->state;
}

/* The complete tuple/terminal checks precede this lookup. Keep their original
 * DATA identity for this page/version; neither a raw xid nor a lock-only slot
 * can supply the other half of a cross-page update. */
static const ClusterRecoveryTxnKey *
cluster_recovery_cold_reference_transaction(const ClusterRecoveryCensusReferencesV1 *references,
	const ClusterRecoveryCensusReferenceV1 *item, TransactionId xid)
{
	const ClusterRecoveryTxnKey *found = NULL;

	for (uint32 i = 0; i < references->count; i++) {
		const ClusterRecoveryCensusReferenceV1 *candidate = &references->entries[i];
		const ClusterItlSlotData *slot = &candidate->entry.itl;

		if (candidate->page_index != item->page_index || candidate->version != item->version
			|| candidate->entry.kind != CLUSTER_RECOVERY_HEAP_ITL || slot->xid != xid
			|| (slot->flags != ITL_FLAG_ACTIVE && slot->flags != ITL_FLAG_COMMITTED
				&& slot->flags != ITL_FLAG_ABORTED && slot->flags != ITL_FLAG_NEEDS_CLEANOUT)) continue;
		if (found != NULL) return NULL;
		found = &candidate->transaction.transaction;
	}
	return found;
}

/* Only ordinal zero describes the simultaneous current physical pages.
 * Inverse ordinals are local undo steps, never a cross-page chronology.
 * Native ordinary stale ctids remain non-edges; a matching current pair must
 * also match the complete canonical transaction, not just a recycled xid.
 * This is a reference consistency check, not a full graph or history proof. */
static bool
cluster_recovery_cold_page_reference_cross_page(const ClusterRecoveryCensusPagesV1 *pages,
	const ClusterRecoveryCensusReferencesV1 *references)
{
	for (uint32 i = 0; i < references->count; i++) {
		const ClusterRecoveryCensusReferenceV1 *item = &references->entries[i];
		const ClusterRecoveryCensusReferenceV1 *target = NULL;
		const ClusterRecoveryCensusPageV1 *page;
		const ClusterRecoveryTxnKey *from, *to;
		HeapTupleHeaderData tuple, next;
		TransactionId xmax;
		BlockNumber block;
		OffsetNumber offset;

		if (item->version != 0 || item->entry.kind != CLUSTER_RECOVERY_HEAP_ITEM
			|| !ItemIdIsNormal(&item->entry.item)) continue;
		if (item->page_index >= pages->count || item->entry.tuple_bytes == NULL
			|| item->entry.tuple_length < SizeofHeapTupleHeader) return false;
		page = &pages->entries[item->page_index];
		memcpy(&tuple, item->entry.tuple_bytes, SizeofHeapTupleHeader);
		if ((tuple.t_infomask & HEAP_XMAX_INVALID) != 0) continue;
		if ((tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0) return false;
		/* The preceding complete tuple/terminal checks retain the plain
		 * locker independently; its ctid cannot join an update successor. */
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple.t_infomask)) continue;
		xmax = HeapTupleHeaderGetRawXmax(&tuple);
		if (!TransactionIdIsNormal(xmax) || !ItemPointerIsValid(&tuple.t_ctid)) continue;
		block = ItemPointerGetBlockNumber(&tuple.t_ctid);
		offset = ItemPointerGetOffsetNumber(&tuple.t_ctid);
		if (block == page->blockno || offset > MaxHeapTuplesPerPage) continue;
		for (uint32 j = 0; j < references->count; j++) {
			const ClusterRecoveryCensusReferenceV1 *candidate = &references->entries[j];
			const ClusterRecoveryCensusPageV1 *candidate_page;

			if (candidate->version != 0 || candidate->page_index == item->page_index
				|| candidate->entry.kind != CLUSTER_RECOVERY_HEAP_ITEM
				|| candidate->entry.index != offset || !ItemIdIsNormal(&candidate->entry.item)) continue;
			if (candidate->page_index >= pages->count) return false;
			candidate_page = &pages->entries[candidate->page_index];
			if (candidate_page->blockno != block || candidate_page->forknum != page->forknum
				|| !cluster_recovery_cold_reference_same_space(&page->reservation.identity,
					&candidate_page->reservation.identity)) continue;
			if (target != NULL) return false;
			target = candidate;
		}
		if (target == NULL) continue;
		if (target->entry.tuple_bytes == NULL || target->entry.tuple_length < SizeofHeapTupleHeader)
			return false;
		memcpy(&next, target->entry.tuple_bytes, SizeofHeapTupleHeader);
		if (HeapTupleHeaderGetXmin(&next) != xmax) continue;
		from = cluster_recovery_cold_reference_transaction(references, item, xmax);
		to = cluster_recovery_cold_reference_transaction(references, target, xmax);
		if ((tuple.t_infomask2 & HEAP_HOT_UPDATED) != 0 || HeapTupleHeaderIsHeapOnly(&next)
			|| from == NULL || to == NULL
			|| from->system_identifier != to->system_identifier
			|| from->database_incarnation != to->database_incarnation
			|| from->origin_node_id != to->origin_node_id || from->segment_id != to->segment_id
			|| from->segment_generation != to->segment_generation
			|| from->slot_offset != to->slot_offset || from->slot_wrap != to->slot_wrap
			|| from->xid != to->xid) return false;
		/* Native update chains cannot merge. The local HOT check cannot
		 * see a predecessor on another page; include local ordinary links
		 * in this same complete current view. Stale ctids and plain locks
		 * retain their non-edge meaning, and inverse steps are not ordered
		 * across pages. Every canonical ITL was qualified above. */
		{
			unsigned predecessors = 0;

			for (uint32 j = 0; j < references->count; j++) {
				const ClusterRecoveryCensusReferenceV1 *candidate = &references->entries[j];
				const ClusterRecoveryCensusPageV1 *candidate_page;
				HeapTupleHeaderData previous;

				if (candidate->version != 0 || candidate->entry.kind != CLUSTER_RECOVERY_HEAP_ITEM
					|| !ItemIdIsNormal(&candidate->entry.item)) continue;
				if (candidate->page_index >= pages->count || candidate->entry.tuple_bytes == NULL
					|| candidate->entry.tuple_length < SizeofHeapTupleHeader) return false;
				candidate_page = &pages->entries[candidate->page_index];
				if (candidate_page->forknum != page->forknum
					|| !cluster_recovery_cold_reference_same_space(&page->reservation.identity,
						&candidate_page->reservation.identity)
					|| (candidate_page->blockno == block && candidate->entry.index == offset)) continue;
				memcpy(&previous, candidate->entry.tuple_bytes, SizeofHeapTupleHeader);
				if ((previous.t_infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI)) != 0
					|| HEAP_XMAX_IS_LOCKED_ONLY(previous.t_infomask)
					|| HeapTupleHeaderGetRawXmax(&previous) != xmax
					|| !ItemPointerIsValid(&previous.t_ctid)
					|| ItemPointerGetBlockNumber(&previous.t_ctid) != block
					|| ItemPointerGetOffsetNumber(&previous.t_ctid) != offset) continue;
				if (++predecessors > 1) return false;
			}
			if (predecessors != 1) return false;
		}
	}
	return true;
}

/* PGRAC: every fully reread TX node, including non-root predecessors, must
 * have its original canonical terminal. PAGE heads alone do not cover the
 * forest. Physical record generation is independent of TT generation.
 * This identity/terminal check grants no effect, owner or retention release. */
static bool
cluster_recovery_cold_page_tx_terminals(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	ClusterColdCtrcPageProgressV1 *progress)
{
	ClusterRecoveryTxResultObservationV1 accepted, confirmed;
	const ClusterColdCtrcPhysicalV1 *physical;
	uint32 count;

	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		|| start->inputs == NULL
		|| !cluster_recovery_ctrc_terminal_result_v1(start, work, &accepted)) return false;
	physical = start->ctrc_physical;
	if (physical == NULL || physical->count != work->counts.subject_count) return false;
	count = start->ctrc_observation_manifest.tx_record_count;
	for (uint32 index = 0;; index++) {
		ClusterRecoveryCensusTxRecordV1 record = ClusterColdRecoveryCtrcTxRecord(start, work, index);
		const ClusterRecoveryTxnKey *transaction = &record.value.transaction;
		ClusterRecoveryUndoEntry observed;
		ClusterColdTtSlotInputV1 terminal;
		uint32 subject = 0;
		unsigned matches = 0;

		if (!record.observed || record.count != count
			|| !cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			|| start->ctrc_physical != physical
			|| physical->count != work->counts.subject_count) return false;
		if (index == count) {
			if (record.value.record != NULL) return false;
			break;
		}
		if (record.value.record == NULL
			|| transaction->system_identifier != start->inputs->system_identifier
			|| transaction->database_incarnation != start->inputs->observer.database_incarnation)
			return false;
		for (uint32 i = 0; i < work->counts.subject_count; i++) {
			const ClusterColdTtSlotKeyV1 *key = &work->entries[i].key;

			if ((uint32)transaction->origin_node_id + 1 == key->owner_instance
				&& transaction->segment_id == key->segment_id
				&& transaction->segment_generation == key->segment_generation
				&& transaction->slot_offset == key->slot_offset
				&& transaction->slot_wrap == key->slot_wrap && transaction->xid == key->xid) {
				subject = i;
				matches++;
			}
		}
		if (matches != 1) return false;
		observed = physical->entries[subject];
		if (!cluster_recovery_ctrc_terminal_subject_v1(start, work, subject, &observed, &terminal)
			|| !cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			|| start->ctrc_physical != physical || physical->count != work->counts.subject_count
			|| memcmp(&physical->entries[subject], &observed, sizeof(observed)) != 0)
			return false;
	}
	return cluster_recovery_ctrc_terminal_result_v1(start, work, &confirmed)
		&& memcmp(&accepted, &confirmed, sizeof(accepted)) == 0
		&& cluster_recovery_cold_page_advance_current(start, work, scope, progress);
}

/* PGRAC: consume the complete original relation/history view, including
 * pages absent from PAGE WAL. Match the graph's full canonical identity,
 * never a raw xid or a current slot chosen for an older generation. The
 * original terminal factory brackets this check; tuple/MX/HOT/history and
 * owner-completion obligations are not discharged by this identity join. */
static bool
cluster_recovery_cold_page_reference_terminals(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	ClusterColdCtrcPageProgressV1 *progress)
{
	const ClusterRecoveryCensusReferencesV1 *references;
	const ClusterRecoveryCensusPagesV1 *pages;

	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	/* PGRAC: include the complete original TX forest before PAGE references. */
	if (!cluster_recovery_cold_page_tx_terminals(start, work, scope, progress)) return false;
	references = ClusterColdRecoveryCtrcReferences(start, work);
	if (references == NULL
		|| !cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	pages = start->ctrc_pages;
	if (pages == NULL) return false;
	for (uint32 i = 0; i < references->count; i++) {
		const ClusterRecoveryCensusReferenceV1 *reference = &references->entries[i];
		const ClusterRecoveryHeapEntry *entry = &reference->entry;
		const ClusterRecoveryTxnKey *transaction = &reference->transaction.transaction;
		const ClusterSpaceIdentityKey *space;
		unsigned matches = 0;

		if (reference->page_index >= pages->count
			|| !pages->entries[reference->page_index].heap
			|| pages->entries[reference->page_index].forknum != MAIN_FORKNUM) return false;
		if (entry->kind == CLUSTER_RECOVERY_HEAP_ITEM) {
			bool first = true;

			if (!cluster_recovery_cold_page_reference_tuple(references, reference)) return false;
			for (uint32 j = 0; j < i; j++) {
				const ClusterRecoveryCensusReferenceV1 *previous = &references->entries[j];

				if (previous->page_index == reference->page_index && previous->version == reference->version
					&& previous->entry.kind == CLUSTER_RECOVERY_HEAP_ITEM) { first = false; break; }
			}
			if (first && !cluster_recovery_cold_page_reference_hot(references, reference,
				pages->entries[reference->page_index].blockno)) return false;
			continue;
		}
		if (entry->kind != CLUSTER_RECOVERY_HEAP_ITL
			|| entry->index >= CLUSTER_ITL_INITRANS_DEFAULT) return false;
		if (entry->itl.flags == ITL_FLAG_FREE) continue;
		space = &pages->entries[reference->page_index].reservation.identity.key;
		if (reference->transaction.record == NULL
			|| UBA_is_invalid(entry->itl.undo_segment_head)
			|| memcmp(&entry->itl.undo_segment_head, &reference->transaction.record->address,
				sizeof(UBA)) != 0
			|| transaction->system_identifier != space->system_identifier
			|| transaction->database_incarnation != space->database_incarnation
			|| transaction->xid != entry->itl.xid) return false;
		for (uint32 j = 0; j < work->counts.subject_count; j++) {
			const ClusterColdTtSlotKeyV1 *key = &work->entries[j].key;

			/* The original graph converts native one-based undo owners
			 * into zero-based NodeIds; retain that exact mapping. */
			if (key->owner_instance != 0
				&& transaction->origin_node_id == key->owner_instance - 1
				&& transaction->segment_id == key->segment_id
				&& transaction->segment_generation == key->segment_generation
				&& transaction->slot_offset == key->slot_offset
				&& transaction->slot_wrap == key->slot_wrap && transaction->xid == key->xid)
				matches++;
		}
		if (matches != 1) return false;
	}
	if (!cluster_recovery_cold_page_reference_cross_page(pages, references)) return false;
	/* Original complete getter checks every retained byte/record lifetime and
	 * canonical terminal again. A callback cannot replace the saved views. */
	return references == ClusterColdRecoveryCtrcReferences(start, work)
		&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		&& start->ctrc_pages == pages;
}

/* The native scan counts first records in EVERY buffer, including foreign
 * and unattributed sources. No dirty first may be retired from raw FD/page
 * equality. Empty is only this observation, never future custody or release. */
static bool
cluster_recovery_cold_page_first_empty(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	const ClusterColdCtrcPageProgressV1 *progress)
{
	ClusterWalSourceRef source;
	ClusterPageWalDirtyFloorV1 observed;

	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	source = start->ctrc_page_sources[0].generation.source;
	if (!cluster_page_wal_dirty_floor_v1(&source, &observed)
		|| observed.floor != InvalidXLogRecPtr || observed.dirty != 0
		|| observed.foreign != 0 || observed.unattributed != 0) return false;
	return cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		&& memcmp(&source, &start->ctrc_page_sources[0].generation.source, sizeof(source)) == 0;
}

/* Saved reference bytes cannot stand in for a later physical observation.
 * Re-read every original page not covered by the completed original PAGE
 * owner. This does not establish a new namespace/history superset or replace
 * the remaining independent owner/lifetime and retention obligations. */
static bool
cluster_recovery_cold_page_census_recheck(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, ClusterWalInputsV1 *scope,
	const ClusterColdCtrcPageProgressV1 *progress)
{
	const ClusterRecoveryCensusPagesV1 *pages;
	uint32 count;

	if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) return false;
	pages = start->ctrc_pages;
	if (pages == NULL) return false;
	count = pages->count;
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryCensusPageV1 saved;
		unsigned covered = 0;

		HandleStartupProcInterrupts();
		if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
			|| start->ctrc_pages != pages || pages->count != count) return false;
		saved = pages->entries[i];
		if (progress != NULL) {
			if (progress->pending || progress->next_target != progress->target_count) return false;
			for (uint32 j = 0; j < progress->target_count; j++) {
				RfPageOnlineTargetViewV1 target;
				ClusterRecoveryCensusPageV1 mapped;

				if (progress->completed[j].storage == NULL) continue;
				if (!rf_page_online_plan_target_v1(start->ctrc_page_plan, j, &target)
					|| target.history_only) return false;
				/* Only a possible address match needs the complete unique
				 * mapping. Avoid rescanning all pages for unrelated targets. */
				if (target.page_identity.forknum != (uint32)saved.forknum
					|| target.page_identity.blockno != saved.blockno
					|| target.page_identity.locator.spcOid != saved.reservation.identity.key.locator.spcOid
					|| target.page_identity.locator.dbOid != saved.reservation.identity.key.locator.dbOid
					|| target.page_identity.locator.relNumber != saved.reservation.identity.key.locator.relNumber)
					continue;
				if (!cluster_recovery_cold_page_target(start, &target, &mapped)
					|| memcmp(&mapped, &saved, sizeof(saved)) != 0) return false;
				covered++;
			}
		}
		if (covered > 1) return false;
		if (covered == 0) {
			ClusterRecoveryCensusPageDigestV1 current = cluster_recovery_census_page_digest_v1(
				start->ctrc_observation_census, i);

			if (!current.observed
				|| !cluster_recovery_cold_page_advance_current(start, work, scope, progress)
				|| start->ctrc_pages != pages || pages->count != count
				|| memcmp(&saved, &pages->entries[i], sizeof(saved)) != 0
				|| memcmp(current.sha256, saved.page_sha256, 32) != 0) return false;
		}
	}
	return cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		&& cluster_recovery_census_namespace_current_v1(start->ctrc_observation_census)
		&& cluster_recovery_cold_page_advance_current(start, work, scope, progress)
		&& start->ctrc_pages == pages && pages->count == count;
}

static bool
cluster_recovery_cold_page_completed_recheck(const struct ClusterRecoveryStartV1 *start)
{
	const ClusterColdCtrcWorkV1 *work = start->ctrc_observation_work;
	ClusterWalInputsV1 *scope = start->ctrc_observation_inputs;
	ClusterColdCtrcPageProgressV1 *progress = start->ctrc_page_progress;
	ClusterRecoveryTxResultObservationV1 accepted;
	ClusterColdTtSlotInputV1 *volatile subjects = NULL;
	volatile bool ready = false;
	Size bytes;

	if (!cluster_recovery_cold_page_first_empty(start, work, scope, progress)) return false;
	if (progress == NULL)
		return rf_page_online_plan_target_count_v1(start->ctrc_page_plan) == 0
			&& cluster_recovery_cold_page_census_recheck(start, work, scope, progress)
			&& cluster_recovery_cold_page_reference_terminals(start, work, scope, progress)
			&& cluster_recovery_cold_page_first_empty(start, work, scope, progress);
	if (work->counts.subject_count > MaxAllocSize / sizeof(*subjects)
		|| !cluster_recovery_ctrc_terminal_result_v1(start, work, &accepted)) return false;
	bytes = (Size)work->counts.subject_count * sizeof(*subjects);
	PG_TRY();
	{
		uint32 index = 0;

		if (bytes != 0)
			subjects = MemoryContextAllocExtended(TopMemoryContext, bytes,
				MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
		if ((bytes == 0 || subjects != NULL)
			&& cluster_recovery_cold_page_projection_subjects(start, work, scope,
				progress, subjects, &accepted, true)) {
			for (;;) {
				RfPageCtrcReadbackImageV1 observed;
				PGAlignedBlock projected;
				RfPageOnlineTargetViewV1 target;

				ResetLatch(MyLatch);
				HandleStartupProcInterrupts();
				if (!cluster_recovery_cold_page_projection_subjects(start, work, scope,
						progress, subjects, &accepted, false)
					|| progress->pending || progress->next_target != progress->target_count) break;
				if (index == progress->target_count) { ready = true; break; }
				/* No saved baseline qualifies history/non-heap duties. Their
				 * independent graph/owner obligations remain unchanged. */
				if (progress->completed[index].storage == NULL) { index++; continue; }
				observed = ClusterColdRecoveryCtrcPageImage(start, index);
				if (observed.detail == RF_PAGE_PROOF_DETAIL_WOULD_BLOCK) {
					(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						100L, WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
					continue;
				}
				/* PAGE/proof are already destroyed. Projection is a read-only
				 * assertion of the final native result, never another write. */
				if (observed.detail != RF_PAGE_PROOF_DETAIL_OK
					|| cluster_ctrc_recovery_itl_page_image_v1(subjects, work->counts.subject_count,
						observed.image.data, projected.data) != CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL
					|| memcmp(&observed.image, &projected, sizeof(projected)) != 0
					|| !cluster_ctrc_recovery_heap_itl_references_v1(observed.image.data)
					|| !rf_page_online_plan_target_v1(start->ctrc_page_plan, index, &target)
					|| target.history_only || target.page_identity.forknum != MAIN_FORKNUM
					|| !cluster_ctrc_recovery_heap_hot_references_v1(observed.image.data,
						target.page_identity.blockno)
					|| !cluster_recovery_cold_page_projection_subjects(start, work, scope,
						progress, subjects, &accepted, false)) break;
				index++;
			}
			if (ready)
				ready = cluster_recovery_cold_page_census_recheck(start, work, scope, progress)
					&& cluster_recovery_cold_page_reference_terminals(start, work, scope, progress)
					&& cluster_recovery_cold_page_projection_subjects(start, work, scope,
						progress, subjects, &accepted, false)
					&& cluster_recovery_cold_page_first_empty(start, work, scope, progress);
		}
	}
	PG_FINALLY();
	{
		if (subjects != NULL) pfree((void *)subjects);
	}
	PG_END_TRY();
	return ready;
}

/* Finish each exact bufferless FPI operation through the original protected
 * readback/current/local-debt path. Keep its full value and same raw FD for
 * the remaining fixed graph and independent joint service, not a new owner. */
bool
ClusterColdRecoveryCtrcPageDrain(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget)
{
	struct ClusterRecoveryStartV1 *state;
	bool ready = false;

	if (start == NULL) return true;
	if (!cluster_recovery_cold_ctrc_replays_actor() || start != cluster_recovery_cold_start)
		return false;
	state = cluster_recovery_cold_start;
	PG_TRY();
	{
		for (;;) {
			const ClusterColdCtrcWorkV1 *work;
			ClusterWalInputsV1 *scope;
			ClusterColdCtrcPageProgressV1 *progress;
			RfPageCtrcStepResultV1 result;
			struct ClusterSharedFsPageV1 *storage;
			uint32 index;

			if (!ClusterColdRecoveryCtrcPageAdvance(start, memory_budget)
				|| start != cluster_recovery_cold_start) break;
			memory_budget = 0;
			work = state->ctrc_observation_work;
			scope = state->ctrc_observation_inputs;
			progress = state->ctrc_page_progress;
			if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)) break;
			if (progress == NULL) {
				ready = rf_page_online_plan_target_count_v1(start->ctrc_page_plan) == 0;
				break;
			}
			if (!progress->pending) {
				ready = progress->next_target == progress->target_count;
				break;
			}
			result = progress->result;
			storage = progress->storage;
			index = progress->next_target;
			/* Full original owner/source validation may yield. Re-locate and
			 * compare every saved value before the exact F4 transition. */
			if (!cluster_recovery_cold_page_advance_current(start, work, scope, progress)
				|| start != cluster_recovery_cold_start || state->ctrc_page_progress != progress
				|| !progress->pending || progress->next_target != index
				|| index >= progress->target_count || progress->completed[index].storage != NULL
				|| storage == NULL || progress->storage != storage
				|| memcmp(&progress->result, &result, sizeof(result)) != 0
				|| !cluster_recovery_stamp_clear_v1(&result.inserted.window)) break;
			/* No callback, I/O or allocation between clearing this operation
			 * and retaining its immutable result/FD in the preallocated slot. */
			progress->completed[index].result = result;
			progress->completed[index].storage = storage;
			progress->storage = NULL;
			memset(&progress->result, 0, sizeof(progress->result));
			progress->pending = false;
			progress->next_target++;
		}
		while (ready) {
			const ClusterColdCtrcWorkV1 *work;
			ClusterWalInputsV1 *scope;
			ClusterColdCtrcPageProgressV1 *progress;
			ClusterCtrcLocalEmptyV1 empty;

			if (start != cluster_recovery_cold_start) { ready = false; break; }
			work = state->ctrc_observation_work;
			scope = state->ctrc_observation_inputs;
			progress = state->ctrc_page_progress;
			ResetLatch(MyLatch);
			ready = cluster_recovery_cold_page_completed_recheck(start);
			if (!ready) break;
			/* F4-cleared, unchanged and zero-PAGE paths still owe the
			 * original whole local directory check. EMPTY is an instant
			 * observation, not first/pin/history discharge or completion. */
			empty = cluster_ctrc_recovery_local_empty_v1();
			ready = cluster_recovery_cold_page_advance_current(start, work, scope, progress)
				&& (progress == NULL
					? rf_page_online_plan_target_count_v1(start->ctrc_page_plan) == 0
					: !progress->pending && progress->next_target == progress->target_count);
			if (!ready || empty == CLUSTER_CTRC_LOCAL_EMPTY) break;
			if (empty != CLUSTER_CTRC_LOCAL_WAIT) { ready = false; break; }
			/* No partial directory locks survive the native probe. Keep
			 * the same FDs/IR/WALR and redo the entire original readback
			 * after each local wait; elapsed time is not a refusal. */
			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				100L, WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
			HandleStartupProcInterrupts();
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_INTERNAL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ready) cluster_recovery_cold_page_plan_refuse(state, RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	return ready;
}

const char *
ClusterColdRecoveryCtrcObservationRefusal(const struct ClusterRecoveryStartV1 *start)
{
	return start != NULL && start == cluster_recovery_cold_start
		&& start->ctrc_observation_refusal[0] != '\0'
		? start->ctrc_observation_refusal : "stage=original_owner";
}

const ClusterColdCtrcPhysicalV1 *
ClusterColdRecoveryCtrcPhysical(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	if (start == NULL || start != cluster_recovery_cold_start || CritSectionCount != 0
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work)
		return NULL;
	/* Repeat observation only revalidates the same retained inputs/manifest;
	 * budget0 cannot start a replacement scope or rebuild a refused view. */
	return ClusterColdRecoveryCtrcObserve(start, 0) ? start->ctrc_physical : NULL;
}

const ClusterRecoveryCensusPagesV1 *
ClusterColdRecoveryCtrcPages(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	if (start == NULL || start != cluster_recovery_cold_start || CritSectionCount != 0
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work)
		return NULL;
	/* PGRAC: this is an observation getter before PAGE acquisition. The
	 * original DONE/ROOT/accepted-TX path may do I/O and retire this view. */
	return ClusterColdRecoveryCtrcObserve(start, 0) ? start->ctrc_pages : NULL;
}

const ClusterRecoveryCensusReferencesV1 *
ClusterColdRecoveryCtrcReferences(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	if (start == NULL || start != cluster_recovery_cold_start || CritSectionCount != 0
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work)
		return NULL;
	/* PGRAC: this is an observation getter before PAGE acquisition. The
	 * original DONE/ROOT/accepted-TX path may do I/O and retire this view. */
	return ClusterColdRecoveryCtrcObserve(start, 0) ? start->ctrc_references : NULL;
}

/* Bind the original forest's borrowed records to this exact complete Start.
 * Stable TX dependencies alone cannot retire reference or owner obligations. */
ClusterRecoveryCensusTxRecordV1
ClusterColdRecoveryCtrcTxRecord(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index)
{
	ClusterRecoveryCensusTxRecordV1 out = {0};
	struct ClusterRecoveryStartV1 *state = cluster_recovery_cold_start;
	bool held = false;

	if (start == NULL || start != state || CritSectionCount != 0 || ShutdownRequestPending
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| index > start->ctrc_observation_manifest.tx_record_count) return out;
	ForEachLWLockHeldByMe(cluster_recovery_cold_ctrc_lock_held, &held);
	if (held || !ClusterColdRecoveryCtrcObserve(start, 0)) return out;
	PG_TRY();
	{
		ClusterRecoveryCensusTxRecordV1 current = cluster_recovery_census_tx_record_v1(
			state->ctrc_observation_census, index);

		if (current.observed && current.count == state->ctrc_observation_manifest.tx_record_count
			&& (index == current.count ? current.value.record == NULL : current.value.record != NULL)
			&& ClusterColdRecoveryCtrcObserve(start, 0))
			out = current;
		else {
			cluster_recovery_cold_observation_refuse(state, "tx_record", 0);
			cluster_recovery_cold_observation_close(state);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_observation_refuse(state, "tx_record_error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return out;
}

ClusterRecoveryCensusTxDependencyV1
ClusterColdRecoveryCtrcTxDependency(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index)
{
	ClusterRecoveryCensusTxDependencyV1 out = {0};
	struct ClusterRecoveryStartV1 *state = cluster_recovery_cold_start;
	bool held = false;

	if (start == NULL || start != state || CritSectionCount != 0 || ShutdownRequestPending
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| index > start->ctrc_observation_manifest.tx_record_count) return out;
	ForEachLWLockHeldByMe(cluster_recovery_cold_ctrc_lock_held, &held);
	if (held || !ClusterColdRecoveryCtrcObserve(start, 0)) return out;
	PG_TRY();
	{
		ClusterRecoveryCensusTxDependencyV1 current = cluster_recovery_census_tx_dependency_v1(
			state->ctrc_observation_census, index);
		const ClusterRecoveryGraphDependency *dependency = &current.value;

		if (current.observed
			&& dependency->count == state->ctrc_observation_manifest.tx_record_count
			&& dependency->index == index && dependency->root_count <= dependency->count
			&& dependency->root == (index < dependency->root_count)
			&& (index == dependency->count ? dependency->value.record == NULL
				&& dependency->predecessor.record == NULL : dependency->value.record != NULL)
			&& (dependency->predecessor.record == NULL
				|| (dependency->predecessor_index < dependency->count
					&& dependency->predecessor_index != index))
			&& ClusterColdRecoveryCtrcObserve(start, 0))
			out = current;
		else {
			cluster_recovery_cold_observation_refuse(state, "tx_dependency", 0);
			cluster_recovery_cold_observation_close(state);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_observation_refuse(state, "tx_dependency_error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return out;
}

ClusterRecoveryCensusTxReferenceV1
ClusterColdRecoveryCtrcTxReference(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index)
{
	ClusterRecoveryCensusTxReferenceV1 out = {0};
	struct ClusterRecoveryStartV1 *state = cluster_recovery_cold_start;
	bool held = false;

	if (start == NULL || start != state || CritSectionCount != 0 || ShutdownRequestPending
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| start->ctrc_references == NULL || index > start->ctrc_references->count) return out;
	ForEachLWLockHeldByMe(cluster_recovery_cold_ctrc_lock_held, &held);
	if (held || !ClusterColdRecoveryCtrcObserve(start, 0)) return out;
	PG_TRY();
	{
		ClusterRecoveryCensusTxReferenceV1 current = cluster_recovery_census_tx_reference_v1(
			state->ctrc_observation_census, index);
		const ClusterRecoveryGraphDependency *dependency = &current.transaction;
		ClusterRecoveryGraphDependency empty = {0};
		bool ok = current.observed && current.index == index
			&& current.count == state->ctrc_references->count;

		if (ok && index == current.count)
			ok = current.reference == NULL && memcmp(dependency, &empty, sizeof(empty)) == 0;
		else if (ok) {
			const ClusterRecoveryCensusReferenceV1 *reference = &state->ctrc_references->entries[index];

			ok = current.reference == reference
				&& (reference->entry.kind == CLUSTER_RECOVERY_HEAP_ITL
					|| reference->entry.kind == CLUSTER_RECOVERY_HEAP_ITEM);
			if (ok && reference->entry.kind == CLUSTER_RECOVERY_HEAP_ITL
				&& reference->entry.itl.flags != ITL_FLAG_FREE)
				ok = dependency->value.record != NULL
					&& dependency->value.record == reference->transaction.record
					&& dependency->value.record_segment_generation == reference->transaction.record_segment_generation
					&& memcmp(&dependency->value.transaction, &reference->transaction.transaction,
						sizeof(dependency->value.transaction)) == 0
					&& dependency->count == state->ctrc_observation_manifest.tx_record_count
					&& dependency->index < dependency->count && dependency->root_count <= dependency->count
					&& dependency->root == (dependency->index < dependency->root_count)
					&& (dependency->predecessor.record == NULL
						|| (dependency->predecessor_index < dependency->count
							&& dependency->predecessor_index != dependency->index));
			else if (ok) ok = reference->transaction.record == NULL
				&& memcmp(dependency, &empty, sizeof(empty)) == 0;
		}
		if (ok && ClusterColdRecoveryCtrcObserve(start, 0)) out = current;
		else {
			cluster_recovery_cold_observation_refuse(state, "tx_reference", 0);
			cluster_recovery_cold_observation_close(state);
		}
	}
	PG_CATCH();
	{
		cluster_recovery_cold_observation_refuse(state, "tx_reference_error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return out;
}

bool
ClusterColdRecoveryCtrcWalSourceCount(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 *out)
{
	if (out != NULL) *out = 0;
	if (out == NULL || start == NULL || start != cluster_recovery_cold_start
		|| CritSectionCount != 0 || !start->ctrc_observation_complete
		|| work != start->ctrc_observation_work || !ClusterColdRecoveryCtrcObserve(start, 0))
		return false;
	*out = start->ctrc_wal_source_count;
	return true;
}

bool
ClusterColdRecoveryCtrcWalSourceAt(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index,
	struct ClusterRecoveryWalSourceObservationV1 *out)
{
	struct ClusterRecoveryStartV1 *state = cluster_recovery_cold_start;
	uint32 count;
	bool ok = false;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !ClusterColdRecoveryCtrcWalSourceCount(start, work, &count) || index >= count)
		return false;
	PG_TRY();
	{
		ok = cluster_recovery_census_wal_source_at_v1(state->ctrc_observation_census, index, out);
		if (!ok || !cluster_recovery_cold_observation_current(start)) {
			memset(out, 0, sizeof(*out)); ok = false;
			cluster_recovery_cold_observation_refuse(state, "wal_source", 0);
			cluster_recovery_cold_observation_close(state);
		}
	}
	PG_CATCH();
	{
		memset(out, 0, sizeof(*out));
		cluster_recovery_cold_observation_refuse(state, "wal_source_error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
ClusterColdRecoveryCtrcPageWalCount(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 *out)
{
	if (out != NULL) *out = 0;
	if (out == NULL || start == NULL || start != cluster_recovery_cold_start
		|| CritSectionCount != 0 || !start->ctrc_observation_complete
		|| work != start->ctrc_observation_work || !ClusterColdRecoveryCtrcObserve(start, 0))
		return false;
	*out = start->ctrc_page_wal_count;
	return true;
}

bool
ClusterColdRecoveryCtrcPageWalAt(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, struct ClusterRecoveryPageWalV1 *out)
{
	struct ClusterRecoveryStartV1 *state = cluster_recovery_cold_start;
	uint32 count;
	bool ok = false;

	if (out != NULL) memset(out, 0, sizeof(*out));
	if (out == NULL || !ClusterColdRecoveryCtrcPageWalCount(start, work, &count) || index >= count)
		return false;
	PG_TRY();
	{
		ok = cluster_recovery_census_page_wal_at_v1(state->ctrc_observation_census, index, out);
		if (!ok || !cluster_recovery_cold_observation_current(start)) {
			memset(out, 0, sizeof(*out)); ok = false;
			cluster_recovery_cold_observation_refuse(state, "page_wal", 0);
			cluster_recovery_cold_observation_close(state);
		}
	}
	PG_CATCH();
	{
		memset(out, 0, sizeof(*out));
		cluster_recovery_cold_observation_refuse(state, "page_wal_error", 0);
		cluster_recovery_cold_observation_close(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

/* History endpoints cannot substitute for the current failed source of a TT
 * owner. Locate a unique original replay participant before reading its source. */
static uint32
cluster_recovery_cold_ctrc_source(const ClusterColdTypedV1 *typed, uint16 owner)
{
	uint32 target = UINT32_MAX;

	if (owner == 0 || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count == 0 || typed->replay_count > typed->participant_count)
		return UINT32_MAX;
	for (uint32 p = 0; p < typed->replay_count; p++) {
		if (typed->participants[p].thread_id != owner)
			continue;
		if (target != UINT32_MAX)
			return UINT32_MAX;
		target = p;
	}
	if (target == UINT32_MAX || typed->participants[target].owner_incarnation == 0
		|| typed->sources[target].claim.identity.origin_thread_id != owner
		|| typed->sources[target].claim.identity.origin_owner_incarnation
			!= typed->participants[target].owner_incarnation
		|| typed->sources[target].timeline != typed->participants[target].timeline)
		return UINT32_MAX;
	return target;
}

/* An IR callback cannot invoke the full getter: manifest revalidation may
 * perform I/O and refusal cleanup would retire the scope it is borrowing.
 * Original private physical bytes and canonical terminal are checked locally. */
static const ClusterColdCtrcPhysicalV1 *
cluster_recovery_cold_ctrc_local_physical(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index)
{
	ClusterColdTtSlotInputV1 terminal;

	if (!ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| !cluster_recovery_cold_observation_current(start)
		|| subject_index >= start->ctrc_physical->count
		|| !cluster_recovery_ctrc_terminal_subject_v1(start, work, subject_index,
			&start->ctrc_physical->entries[subject_index], &terminal)
		|| !cluster_recovery_cold_observation_current(start))
		return NULL;
	return start->ctrc_physical;
}

static ClusterRecoverySerialRequest
cluster_recovery_cold_ctrc_serial_request(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index, bool manifest)
{
	ClusterRecoverySerialRequest request, empty;
	const ClusterColdTypedV1 *typed;
	const ClusterColdCtrcPhysicalV1 *physical, *confirmed;
	struct ClusterRecoveryFencePlan *fence;
	ClusterWalSourceRef source;
	ClusterColdParticipantV1 participant;
	ClusterRecoveryUndoEntry observed;
	uint32 target, replay_count, participant_count;
	uint16 owner;

	memset(&request, 0, sizeof(request));
	memset(&empty, 0, sizeof(empty));
	if (CritSectionCount != 0 || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| (physical = manifest ? ClusterColdRecoveryCtrcPhysical(start, work)
			: cluster_recovery_cold_ctrc_local_physical(start, work, subject_index)) == NULL
		|| subject_index >= physical->count)
		return empty;
	typed = start->inputs;
	fence = start->tx_fence;
	owner = work->entries[subject_index].key.owner_instance;
	target = cluster_recovery_cold_ctrc_source(typed, owner);
	if (target == UINT32_MAX)
		return empty;
	replay_count = typed->replay_count;
	participant_count = typed->participant_count;
	memcpy(&source, &typed->sources[target], sizeof(source));
	memcpy(&participant, &typed->participants[target], sizeof(participant));
	memcpy(&observed, &physical->entries[subject_index], sizeof(observed));

	/* The original completed fence owner alone supplies the old duty, RR token
	 * and live formation/fence bindings. Never manufacture them from the new
	 * installed writer, or treat this unheld template as PAGE/write authority. */
	if (!cluster_recovery_merge_fence_plan_tx_request_v1(fence,
			&typed->sources[target], &request))
		return empty;
	confirmed = manifest ? ClusterColdRecoveryCtrcPhysical(start, work)
		: cluster_recovery_cold_ctrc_local_physical(start, work, subject_index);
	/* The full getter validates the manifest and every canonical terminal;
	 * the local callback validates the same work/TX and selected terminal
	 * without I/O or cleanup. Neither starts a new view. Keep selected raw
	 * bytes and the full old-source identity across both paths' callbacks. */
	if (confirmed != physical || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| start->inputs != typed || start->tx_fence != fence
		|| typed->replay_count != replay_count || typed->participant_count != participant_count
		|| cluster_recovery_cold_ctrc_source(typed, owner) != target
		|| memcmp(&source, &typed->sources[target], sizeof(source)) != 0
		|| memcmp(&participant, &typed->participants[target], sizeof(participant)) != 0
		|| memcmp(&observed, &confirmed->entries[subject_index], sizeof(observed)) != 0
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(fence, &source)
		|| !cluster_recovery_cold_observation_current(start))
		return empty;
	return request;
}

ClusterRecoverySerialRequest
ClusterColdRecoveryCtrcSerialRequest(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index)
{
	return cluster_recovery_cold_ctrc_serial_request(start, work, subject_index, true);
}

struct ClusterWalInputsV1 *
ClusterColdRecoveryCtrcScope(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	if (CritSectionCount != 0 || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| !cluster_recovery_cold_observation_current(start))
		return NULL;
	return start->ctrc_observation_inputs;
}

bool
ClusterColdRecoveryCtrcSerialRequestCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index,
	struct ClusterWalInputsV1 *scope, const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest original, expected;

	if (request == NULL || scope == NULL || ClusterColdRecoveryCtrcScope(start, work) != scope)
		return false;
	memcpy(&original, request, sizeof(original));
	expected = cluster_recovery_cold_ctrc_serial_request(start, work, subject_index, false);
	return expected.mode == CLUSTER_RECOVERY_SERIAL_TX_CLOSURE
		&& original.mode == expected.mode && original.tx_closure_scope == scope
		&& memcmp(&original.duty, &expected.duty, sizeof(expected.duty)) == 0
		&& memcmp(&original.expected_root_token, &expected.expected_root_token,
			sizeof(expected.expected_root_token)) == 0
		&& memcmp(&original.pending, &expected.pending, sizeof(expected.pending)) == 0
		&& original.formation == expected.formation
		&& original.fence_need_set == expected.fence_need_set
		&& original.fence_admission_set == expected.fence_admission_set
		&& original.acquire_timeout_ms == expected.acquire_timeout_ms
		&& original.release_timeout_ms == expected.release_timeout_ms
		&& memcmp(request, &original, sizeof(original)) == 0;
}

/* An origin can contribute redo with no TT subject. Its native purpose
 * callback still validates every original physical terminal locally, including
 * a legitimately empty work, without manifest I/O or borrowed-scope cleanup. */
static const ClusterColdCtrcPhysicalV1 *
cluster_recovery_cold_ctrc_all_local_physical(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	const ClusterColdCtrcPhysicalV1 *physical;
	uint32 count;

	if (!ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| !start->ctrc_observation_complete || work != start->ctrc_observation_work
		|| !cluster_recovery_cold_observation_current(start))
		return NULL;
	physical = start->ctrc_physical;
	count = physical->count;
	for (uint32 i = 0; i < count; i++)
		if (cluster_recovery_cold_ctrc_local_physical(start, work, i) != physical)
			return NULL;
	return cluster_recovery_cold_observation_current(start)
		&& start->ctrc_physical == physical && physical->count == count ? physical : NULL;
}

static bool
cluster_recovery_cold_ctrc_replay_index(const ClusterColdTypedV1 *typed, uint32 index)
{
	return typed->participant_count <= CLUSTER_COLD_MAX_PARTICIPANTS
		&& typed->replay_count > 0 && typed->replay_count <= typed->participant_count
		&& index < typed->replay_count && typed->sources[index].timeline != 0
		&& cluster_recovery_cold_ctrc_source(typed, typed->participants[index].thread_id) == index;
}

static ClusterRecoverySerialRequest
cluster_recovery_cold_ctrc_replay_request(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 source_index, bool manifest)
{
	ClusterRecoverySerialRequest request, empty;
	const ClusterColdTypedV1 *typed;
	const ClusterColdCtrcPhysicalV1 *physical, *confirmed;
	struct ClusterRecoveryFencePlan *fence;
	ClusterWalSourceRef source;
	ClusterColdParticipantV1 participant;
	uint32 replay_count, participant_count;

	memset(&request, 0, sizeof(request));
	memset(&empty, 0, sizeof(empty));
	if (CritSectionCount != 0 || !ClusterColdRecoveryCtrcWorkCurrent(start, work))
		return empty;
	typed = start->inputs;
	if (!cluster_recovery_cold_ctrc_replay_index(typed, source_index)
		|| (physical = manifest ? ClusterColdRecoveryCtrcPhysical(start, work)
			: cluster_recovery_cold_ctrc_all_local_physical(start, work)) == NULL
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work) || start->inputs != typed
		|| !cluster_recovery_cold_ctrc_replay_index(typed, source_index))
		return empty;
	fence = start->tx_fence;
	replay_count = typed->replay_count;
	participant_count = typed->participant_count;
	memcpy(&source, &typed->sources[source_index], sizeof(source));
	memcpy(&participant, &typed->participants[source_index], sizeof(participant));
	/* The original completed fence supplies the old identity, RR token and
	 * formation/isolation owners. No TT occupancy or new writer substitutes. */
	if (!cluster_recovery_merge_fence_plan_tx_request_v1(fence,
			&typed->sources[source_index], &request))
		return empty;
	confirmed = manifest ? ClusterColdRecoveryCtrcPhysical(start, work)
		: cluster_recovery_cold_ctrc_all_local_physical(start, work);
	if (confirmed != physical || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| start->inputs != typed || start->tx_fence != fence
		|| typed->replay_count != replay_count || typed->participant_count != participant_count
		|| !cluster_recovery_cold_ctrc_replay_index(typed, source_index)
		|| memcmp(&source, &typed->sources[source_index], sizeof(source)) != 0
		|| memcmp(&participant, &typed->participants[source_index], sizeof(participant)) != 0
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(fence, &source)
		|| !cluster_recovery_cold_observation_current(start))
		return empty;
	return request;
}

ClusterRecoverySerialRequest
ClusterColdRecoveryCtrcReplayRequest(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 source_index)
{
	return cluster_recovery_cold_ctrc_replay_request(start, work, source_index, true);
}

bool
ClusterColdRecoveryCtrcReplayRequestCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 source_index,
	struct ClusterWalInputsV1 *scope, const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest original, expected;

	if (request == NULL || scope == NULL || ClusterColdRecoveryCtrcScope(start, work) != scope)
		return false;
	memcpy(&original, request, sizeof(original));
	expected = cluster_recovery_cold_ctrc_replay_request(start, work, source_index, false);
	return expected.mode == CLUSTER_RECOVERY_SERIAL_TX_CLOSURE
		&& original.mode == expected.mode && original.tx_closure_scope == scope
		&& memcmp(&original.duty, &expected.duty, sizeof(expected.duty)) == 0
		&& memcmp(&original.expected_root_token, &expected.expected_root_token,
			sizeof(expected.expected_root_token)) == 0
		&& memcmp(&original.pending, &expected.pending, sizeof(expected.pending)) == 0
		&& original.formation == expected.formation
		&& original.fence_need_set == expected.fence_need_set
		&& original.fence_admission_set == expected.fence_admission_set
		&& original.acquire_timeout_ms == expected.acquire_timeout_ms
		&& original.release_timeout_ms == expected.release_timeout_ms
		&& memcmp(request, &original, sizeof(original)) == 0;
}

bool
ClusterColdRecoveryTxEffectsCurrent(const struct ClusterRecoveryStartV1 *start)
{
	const ClusterColdTypedV1 *typed = ClusterColdRecoveryTxInputs(start);

	/* The receipt is independently sealed to the original plan/result.  Do
	 * not infer completeness from a caller bit or from a proven loser. */
	return typed != NULL && cluster_cold_typed_effects_ready_v1(typed);
}

bool
ClusterColdRecoveryTxSubjectCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, uint64 *generation, uint64 *serial)
{
	if (generation != NULL) *generation = 0;
	if (serial != NULL) *serial = 0;
	if (generation == NULL || serial == NULL || subject == NULL
		|| !ClusterColdRecoveryTxCurrent(start))
		return false;
	/* Never dereference a caller's subject before locating it in the original
	 * complete census. Unproven ACTIVE inputs may be read to build a proof;
	 * this check grants neither loser status nor permission to change TT. */
	for (uint32 i = 0; i < start->census->count; i++) {
		const ClusterColdTopTxCensusEntryV1 *entry = &start->census->entries[i];
		const ClusterColdTtSlotKeyV1 *key = &entry->input.key;
		if (entry != subject)
			continue;
		if (key->owner_instance == 0 || key->owner_instance > UNDO_OWNER_INSTANCE_MAX
			|| key->segment_id == 0
			|| (key->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != key->owner_instance
			|| key->segment_generation == UINT32_MAX || key->slot_offset >= TT_SLOTS_PER_SEGMENT
			|| !TransactionIdIsNormal(key->xid) || key->slot_wrap == TT_WRAP_INVALID
			|| entry->input.slot.status != TT_SLOT_ACTIVE
			|| entry->input.slot.xid != key->xid || entry->input.slot.wrap != key->slot_wrap)
			return false;
		*generation = start->generation;
		*serial = start->serial;
		return true;
	}
	return false;
}

bool
ClusterColdRecoveryTxLoserCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject)
{
	uint64 generation;
	uint64 serial;

	if (!ClusterColdRecoveryTxSubjectCurrent(start, subject, &generation, &serial)
		|| !subject->loser_proven)
		return false;

	/* The producer's loser is a physical identity only.  Keep this helper
	 * independent of C0, FullXid, TT mutation and checkpoint authority. */
	return memcmp(&subject->loser.key, &subject->input.key,
				  sizeof(subject->input.key)) == 0;
}

bool
ClusterColdRecoveryTxTopProofCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject)
{
	/* Physical loser identity is a prerequisite, but it is not the complete
	 * old-ACTIVE qualification.  Require every independent producer gate from
	 * the same retained census entry and never consult caller assertions. */
	if (!ClusterColdRecoveryTxLoserCurrent(start, subject)
		|| memcmp(&subject->proof.key, &subject->input.key,
				  sizeof(subject->input.key)) != 0
		|| !subject->proof.producer_top
		|| !subject->proof.qualified_base
		|| !subject->proof.sealed_redo
		|| !subject->proof.ordinary_effects
		|| !subject->proof.old_writer_isolated
		|| !subject->proof.terminal_match
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(start->tx_fence, NULL))
		return false;
	return true;
}

bool
ClusterColdRecoveryTxC0Candidate(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterRecoveryTTAbortDerivationV1 *out)
{
	const ClusterColdTypedV1 *typed;
	const ClusterColdTtSlotKeyV1 *key;
	ClusterRecoveryTTAbortV1 shape;
	ClusterRecoveryTTAbortDerivationV1 candidate;
	uint64 next_xid;
	uint32 target = UINT32_MAX;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	/* Locate the subject in the retained census before dereferencing it.
	 * Numeric completeness is a C0 requirement, never a physical loser gate. */
	if (!ClusterColdRecoveryTxTopProofCurrent(start, subject)
		|| !ClusterColdRecoveryTxEffectsCurrent(start))
		return false;
	typed = start->inputs;
	key = &subject->input.key;
	if (typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count == 0 || typed->replay_count > typed->participant_count
		|| !cluster_cold_typed_xid_maximum_v1(typed, &next_xid))
		return false;
	for (uint32 p = 0; p < typed->replay_count; p++) {
		if (typed->participants[p].thread_id != key->owner_instance)
			continue;
		if (target != UINT32_MAX)
			return false;
		target = p;
	}
	if (target == UINT32_MAX
		|| typed->sources[target].claim.identity.origin_thread_id != key->owner_instance
		|| typed->sources[target].claim.identity.origin_owner_incarnation
			!= typed->participants[target].owner_incarnation
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(start->tx_fence, &typed->sources[target]))
		return false;

	memset(&shape, 0, sizeof(shape));
	shape.version = CLUSTER_RECOVERY_TT_ABORT_VERSION;
	shape.length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	shape.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	shape.target_thread = key->owner_instance;
	shape.target_incarnation = typed->participants[target].owner_incarnation;
	memcpy(shape.target_claim_sha256, typed->sources[target].claim.claim_sha256,
		   sizeof(shape.target_claim_sha256));
	shape.writer_thread = start->writer.ref.claim.identity.origin_thread_id;
	shape.writer_incarnation = start->writer.ref.claim.identity.origin_owner_incarnation;
	shape.recovery_serial = start->serial;
	/* In E1 this field means the actual recoverer's epoch, not bind time. */
	shape.target_epoch = start->writer.epoch;
	shape.exact.segment_id = key->segment_id;
	shape.exact.segment_generation = key->segment_generation;
	shape.exact.xid = key->xid;
	shape.exact.slot_offset = key->slot_offset;
	shape.exact.wrap = key->slot_wrap;
	shape.exact.instance = key->owner_instance;
	shape.exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	memcpy(&shape.before, &subject->input.slot, sizeof(shape.before));
	/* No independently retained L is available here. Preserve the existing
	 * AD-012 exception 10/spec-6.15 dependency in the derivation result. */
	if (!cluster_recovery_tt_abort_derive_fields_v1(&shape, next_xid, true, false,
			InvalidTransactionId, start->writer.epoch, &candidate)
		|| cluster_recovery_tt_abort_match_v1(&candidate.record, key->segment_generation,
			&subject->input.slot) != CLUSTER_RECOVERY_TT_ABORT_APPLY
		|| !ClusterColdRecoveryTxTopProofCurrent(start, subject)
		|| !ClusterColdRecoveryTxEffectsCurrent(start)
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(start->tx_fence, &typed->sources[target]))
		return false;
	*out = candidate;
	return true;
}

bool
ClusterColdRecoveryTxSerialRequest(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, ClusterRecoverySerialRequest *out)
{
	ClusterRecoveryTTAbortDerivationV1 candidate;
	const ClusterColdTypedV1 *typed;

	if (out == NULL || !ClusterColdRecoveryStartOutputDistinct(start, out, sizeof(*out))
		|| !ClusterColdRecoveryTxC0Candidate(start, subject, &candidate))
		return false;
	typed = start->inputs;
	if (((uintptr_t)out <= (uintptr_t)typed
		 ? (uintptr_t)typed - (uintptr_t)out < sizeof(*out)
		 : (uintptr_t)out - (uintptr_t)typed < sizeof(*typed))
		|| ((uintptr_t)out <= (uintptr_t)subject
			? (uintptr_t)subject - (uintptr_t)out < sizeof(*out)
			: (uintptr_t)out - (uintptr_t)subject < sizeof(*subject)))
		return false;
	for (uint32 p = 0; p < typed->replay_count; p++) {
		if (typed->participants[p].thread_id != candidate.record.target_thread)
			continue;
		if (!ClusterColdRecoveryTxTopProofCurrent(start, subject)
			|| !ClusterColdRecoveryTxEffectsCurrent(start))
			return false;
		/* The original owner must see the final output address so it can
		 * reject aliases throughout its opaque proof storage before publishing. */
		return cluster_recovery_merge_fence_plan_tx_request_v1(start->tx_fence,
			&typed->sources[p], out);
	}
	return false;
}

bool
ClusterColdRecoveryTxC0Reserve(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, struct ClusterUndoBlock0Pin *pin)
{
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0AuthorityProof proof;

	/* Validate the retained subject pointer before reading its physical key. */
	if (pin == NULL || !ClusterColdRecoveryTxTopProofCurrent(start, subject)
		|| !ClusterColdRecoveryTxEffectsCurrent(start))
		return false;
	memset(&logical, 0, sizeof(logical));
	logical.owner_instance = subject->input.key.owner_instance;
	logical.segment_id = subject->input.key.segment_id;
	memset(&proof, 0, sizeof(proof));
	proof.kind = CLUSTER_UNDO_BLOCK0_STARTUP_REDO;
	proof.owner_instance = logical.owner_instance;
	proof.cluster_epoch_present = true;
	proof.cluster_epoch = start->writer.epoch;
	proof.recovery_generation = start->generation;
	if (cluster_undo_block0_recovery_reserve(&logical, &proof, pin) != CLUSTER_UNDO_BLOCK0_OK)
		return false;
	if (!ClusterColdRecoveryTxTopProofCurrent(start, subject)
		|| !ClusterColdRecoveryTxEffectsCurrent(start)) {
		cluster_undo_block0_release_reservation(pin);
		return false;
	}
	return true;
}

bool
ClusterBeginColdRecoveryTx(EndOfWalRecoveryInfo *info)
{
	struct ClusterRecoveryStartV1 *start;
	ClusterWalWriterToken writer;
	uint8 operation_uuid[16];
	uint64 owner_boot, install_generation, claim_owner;

	if (info == NULL)
		return false;
	/* Non-cold/clean startup has no retained attempt and needs no resource. */
	if (info->cluster_cold_inputs == NULL)
		return cluster_recovery_cold_inputs == NULL && cluster_recovery_cold_start == NULL
			&& info->cluster_tx_start == NULL;
	if (info->cluster_cold_inputs != cluster_recovery_cold_inputs
		|| cluster_recovery_cold_start != NULL || info->cluster_tx_start != NULL
		|| cluster_recovery_cold_inputs->inputs != NULL
		|| cluster_recovery_cold_inputs->plan == NULL
		|| cluster_recovery_cold_inputs->participant_count == 0
		|| cluster_recovery_cold_inputs->top_tx_census == NULL
		|| cluster_recovery_cold_inputs->top_tx_census->plan != cluster_recovery_cold_inputs->plan
		|| !cluster_recovery_merge_fence_plan_tx_current_v1(cluster_recovery_cold_inputs->tx_fence, NULL)
		|| CurrentResourceOwner == NULL || MyProcPid <= 0
		|| cluster_recovery_cold_generation == PG_UINT64_MAX
		|| !ClusterStartupInstalledRecoveryWriterV1(&writer, operation_uuid, &install_generation))
		return false;
	/* Original retained recovery is still serialized by its exact creator.
	 * This full file check runs before target protection, not in Current. */
	claim_owner = cluster_recovery_merge_claim_owner_v1();
	if (claim_owner == 0)
		return false;
	owner_boot = cluster_qvotec_get_self_incarnation();
	if (owner_boot == 0 || owner_boot != writer.ref.claim.identity.origin_owner_incarnation)
		return false;
	if (!cluster_recovery_cold_callback_registered) {
		RegisterResourceReleaseCallback(cluster_recovery_cold_resource_release, NULL);
		cluster_recovery_cold_callback_registered = true;
	}
	start = MemoryContextAllocExtended(TopMemoryContext, sizeof(*start),
										MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
	if (start == NULL)
		return false;
	start->inputs = cluster_recovery_cold_inputs;
	start->census = cluster_recovery_cold_inputs->top_tx_census;
	start->tx_fence = cluster_recovery_cold_inputs->tx_fence;
	start->owner = CurrentResourceOwner;
	start->pid = MyProcPid;
	start->owner_boot = owner_boot;
	start->generation = ++cluster_recovery_cold_generation;
	start->serial = 1;
	start->claim_owner = claim_owner;
	start->install_generation = install_generation;
	start->writer = writer;
	memcpy(start->operation_uuid, operation_uuid, 16);
	cluster_recovery_cold_start = start;
	if (!ClusterColdRecoveryTxCurrent(start)
		|| !cluster_recovery_checkpoint_register_v1(start)) {
		cluster_recovery_cold_start = NULL;
		pfree(start);
		return false;
	}
	info->cluster_tx_start = start;
	return true;
}

void
ClusterReleaseColdRecoveryInputs(EndOfWalRecoveryInfo *info)
{
	if (info == NULL || info->cluster_cold_inputs != cluster_recovery_cold_inputs
		|| info->cluster_tx_start != cluster_recovery_cold_start)
		ereport(FATAL, (errmsg("cold recovery input handoff belongs to another attempt")));
	if (cluster_recovery_cold_start != NULL)
		cluster_recovery_cold_observation_close(cluster_recovery_cold_start);
	/* Preserve the original registered checkpoint owner after successful
	 * Startup completion, before destroying its private source inputs.
	 * An already-revoked attempt is cleanup only and transfers nothing. */
	if (cluster_recovery_cold_start != NULL && !cluster_recovery_cold_start->revoked
		&& !cluster_recovery_checkpoint_handoff_v1(cluster_recovery_cold_start))
		ereport(FATAL, (errmsg("could not hand off the original recovered checkpoint owner")));
	info->cluster_tx_start = NULL;
	info->cluster_cold_inputs = NULL;
	if (cluster_recovery_cold_inputs != NULL) {
		cluster_recovery_cold_inputs_cleanup(0, (Datum)0);
		cancel_before_shmem_exit(cluster_recovery_cold_inputs_cleanup, (Datum)0);
	}
}

static bool cluster_backup_recovery_have_manifest = false;
static ClusterBackupManifest cluster_backup_recovery_manifest;
static bool cluster_backup_recovery_target_active = false;
static bool cluster_backup_recovery_merge_required = false;
static ClusterRestorePoint cluster_backup_recovery_target_point;
static uint64 cluster_backup_recovery_merge_bitmap[2];
static XLogRecPtr cluster_backup_recovery_merge_start[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
static XLogRecPtr cluster_backup_recovery_merge_stop[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
static char cluster_backup_recovery_wal_root[MAXPGPATH];
static void cluster_backup_recovery_load_manifest(void);
static void cluster_backup_recovery_apply_target(void);
#endif

#ifdef USE_PGRAC_CLUSTER
static bool
cluster_backup_recovery_point_usable(const ClusterRestorePoint *point)
{
	if (point == NULL || !point->present || !SCN_VALID(point->cut_scn))
		return false;
	if (point->thread_count != cluster_backup_recovery_manifest.thread_count)
		return false;
	if (scn_time_cmp(point->cut_scn, cluster_backup_recovery_manifest.consistent_scn) < 0)
		return false;
	return true;
}

static bool
cluster_backup_recovery_select_scn(SCN requested_scn, ClusterRestorePoint *out)
{
	const ClusterRestorePoint *best = NULL;
	uint32		i;

	for (i = 0; i < cluster_backup_recovery_manifest.restore_point_count; i++)
	{
		const ClusterRestorePoint *point = &cluster_backup_recovery_manifest.restore_points[i];

		if (!cluster_backup_recovery_point_usable(point))
			continue;
		if (scn_time_cmp(point->cut_scn, requested_scn) > 0)
			continue;
		if (best == NULL || scn_time_cmp(point->cut_scn, best->cut_scn) > 0)
			best = point;
	}
	if (best == NULL)
		return false;
	*out = *best;
	return true;
}

static bool
cluster_backup_recovery_select_name(const char *name, ClusterRestorePoint *out)
{
	uint32		i;

	for (i = 0; i < cluster_backup_recovery_manifest.restore_point_count; i++)
	{
		const ClusterRestorePoint *point = &cluster_backup_recovery_manifest.restore_points[i];

		if (!cluster_backup_recovery_point_usable(point))
			continue;
		if (strcmp(point->name, name) == 0)
		{
			*out = *point;
			return true;
		}
	}
	return false;
}

static bool
cluster_backup_recovery_select_time(TimestampTz requested_time, ClusterRestorePoint *out)
{
	const ClusterRestorePoint *best = NULL;
	uint32		i;

	for (i = 0; i < cluster_backup_recovery_manifest.restore_point_count; i++)
	{
		const ClusterRestorePoint *point = &cluster_backup_recovery_manifest.restore_points[i];

		if (!cluster_backup_recovery_point_usable(point) || point->created_at == 0)
			continue;
		if (point->created_at > requested_time)
			continue;
		if (best == NULL || point->created_at > best->created_at)
			best = point;
	}
	if (best == NULL)
		return false;
	*out = *best;
	return true;
}

static void
cluster_backup_recovery_prepare_merge(const ClusterRestorePoint *target_point)
{
	uint32		i;
	uint16		local_thread_id;
	bool		have_local_thread = false;
	uint16		first_thread_id = XLP_THREAD_ID_LEGACY;

	MemSet(cluster_backup_recovery_merge_bitmap, 0, sizeof(cluster_backup_recovery_merge_bitmap));
	MemSet(cluster_backup_recovery_merge_start, 0, sizeof(cluster_backup_recovery_merge_start));
	MemSet(cluster_backup_recovery_merge_stop, 0, sizeof(cluster_backup_recovery_merge_stop));
	MemSet(cluster_backup_recovery_wal_root, 0, sizeof(cluster_backup_recovery_wal_root));

	if (cluster_backup_recovery_manifest.backup_set_path[0] == '\0')
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifest has no backup-set path")));
	strlcpy(cluster_backup_recovery_wal_root, cluster_backup_recovery_manifest.backup_set_path,
			sizeof(cluster_backup_recovery_wal_root));

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		const ClusterBackupManifestThread *thread = &cluster_backup_recovery_manifest.threads[i];
		uint16		tid;

		if (!thread->present)
			continue;
		tid = (uint16) thread->thread_id;
		if (tid == 0 || tid > CLUSTER_WAL_STATE_SLOT_COUNT)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
					 errmsg("cluster backup manifest has invalid WAL thread %u", (unsigned) tid)));
		if (first_thread_id == XLP_THREAD_ID_LEGACY)
			first_thread_id = tid;
		cluster_backup_recovery_merge_bitmap[(tid - 1) / 64] |=
			((uint64) 1 << ((tid - 1) % 64));
		cluster_backup_recovery_merge_start[tid] = thread->start_redo_lsn;
		if (target_point != NULL)
		{
			if (target_point->cut_lsn[tid - 1] == InvalidXLogRecPtr)
				ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
								errmsg("cluster PITR restore point is missing WAL thread %u",
									   (unsigned) tid)));
			cluster_backup_recovery_merge_stop[tid] = target_point->cut_lsn[tid - 1];
		}
	}

	local_thread_id = cluster_wal_thread_id();
	if (local_thread_id == XLP_THREAD_ID_LEGACY)
		local_thread_id = 1;
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		if (cluster_backup_recovery_manifest.threads[i].present &&
			cluster_backup_recovery_manifest.threads[i].thread_id == local_thread_id)
		{
			have_local_thread = true;
			break;
		}
	}
	if (!have_local_thread && first_thread_id != XLP_THREAD_ID_LEGACY)
		ereport(LOG, (errmsg("cluster backup restore: local WAL thread %u is not in manifest; "
							 "using manifest thread %u as replay owner",
							 (unsigned) local_thread_id, (unsigned) first_thread_id)));
	cluster_backup_recovery_merge_required = true;
}

static void
cluster_backup_recovery_load_manifest(void)
{
	ClusterBackupManifest manifest;
	ClusterRestoreCompatibilityReason compat;
	struct stat st;
	uint32 expected_node_count;

	cluster_backup_recovery_have_manifest = false;
	cluster_backup_recovery_target_active = false;
	cluster_backup_recovery_merge_required = false;
	MemSet(&cluster_backup_recovery_manifest, 0, sizeof(cluster_backup_recovery_manifest));
	MemSet(&cluster_backup_recovery_target_point, 0, sizeof(cluster_backup_recovery_target_point));
	MemSet(cluster_backup_recovery_merge_bitmap, 0, sizeof(cluster_backup_recovery_merge_bitmap));
	MemSet(cluster_backup_recovery_merge_start, 0, sizeof(cluster_backup_recovery_merge_start));
	MemSet(cluster_backup_recovery_merge_stop, 0, sizeof(cluster_backup_recovery_merge_stop));
	MemSet(cluster_backup_recovery_wal_root, 0, sizeof(cluster_backup_recovery_wal_root));

	if (!cluster_enabled)
		return;
	if (stat(CLUSTER_BACKUP_MANIFEST_DATADIR_FILE, &st) != 0) {
		if (errno != ENOENT)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not stat cluster backup manifest \"%s\": %m",
								   CLUSTER_BACKUP_MANIFEST_DATADIR_FILE)));
		return;
	}
	if (!S_ISREG(st.st_mode)
		|| !cluster_backup_manifest_read_file(CLUSTER_BACKUP_MANIFEST_DATADIR_FILE, &manifest))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifest is invalid"),
						errdetail("File \"%s\" exists but could not be validated.",
								  CLUSTER_BACKUP_MANIFEST_DATADIR_FILE)));
	expected_node_count = (uint32)cluster_conf_node_count();
	compat = cluster_backup_manifest_compatible(&manifest, CATALOG_VERSION_NO,
												(uint32)cluster_shared_storage_backend,
												expected_node_count);
	if (compat != CLUSTER_RESTORE_COMPAT_OK)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_RESTORE_INCOMPATIBLE),
						errmsg("cluster backup manifest is not compatible with this server"),
						errdetail("Reason: %s.",
								  cluster_restore_compat_reason_name(compat))));

	cluster_backup_recovery_manifest = manifest;
	cluster_backup_recovery_have_manifest = true;
	ereport(LOG,
			(errmsg("cluster backup manifest \"%s\" loaded for recovery",
					CLUSTER_BACKUP_MANIFEST_DATADIR_FILE),
			 errdetail("backup_id=%s consistent_scn=%llu incarnation=%u crc=%u",
					   manifest.backup_id,
					   (unsigned long long)manifest.consistent_scn,
					   manifest.incarnation,
					   manifest.manifest_crc)));
}

static void
cluster_backup_recovery_apply_target(void)
{
	bool have_scn_target;
	bool have_time_target;
	bool have_name_target;
	int target_count;
	uint16 local_thread_id;
	int local_thread_index;
	ClusterRestorePoint selected_point;

	if (!cluster_enabled)
		return;

	have_scn_target = cluster_recovery_target_scn != NULL
		&& cluster_recovery_target_scn[0] != '\0';
	have_time_target = cluster_recovery_target_cluster_time != NULL
		&& cluster_recovery_target_cluster_time[0] != '\0';
	have_name_target = cluster_recovery_target_name != NULL
		&& cluster_recovery_target_name[0] != '\0';
	target_count = (have_scn_target ? 1 : 0) + (have_time_target ? 1 : 0)
		+ (have_name_target ? 1 : 0);
	if (!have_scn_target && !have_time_target && !have_name_target)
	{
		if (ArchiveRecoveryRequested && cluster_backup_recovery_have_manifest &&
			cluster_backup_recovery_manifest.thread_count > 1)
		{
			ClusterRestorePoint manifest_point;
			SCN			requested_scn = cluster_backup_recovery_manifest.scn_durable_peak;

			if (!cluster_backup_recovery_select_scn(requested_scn, &manifest_point))
				ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
								errmsg("cluster backup restore has no consistent multi-thread "
									   "restore point")));
			cluster_backup_recovery_target_point = manifest_point;
			cluster_backup_recovery_prepare_merge(&cluster_backup_recovery_target_point);
		}
		return;
	}

	if (!ArchiveRecoveryRequested)
		return;
	if (target_count > 1)
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("multiple cluster PITR targets are configured"),
						errdetail("Set only one of cluster.recovery_target_scn, "
								  "cluster.recovery_target_cluster_time, or "
								  "cluster.recovery_target_name.")));
	if (recoveryTarget != RECOVERY_TARGET_UNSET)
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster PITR target conflicts with native recovery target"),
						errdetail("Use either cluster.recovery_target_* or native recovery_target_* "
								  "settings, not both.")));
	if (!cluster_backup_recovery_have_manifest)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster PITR target requires a valid cluster backup manifest")));

	MemSet(&selected_point, 0, sizeof(selected_point));
	if (have_name_target)
	{
		if (!cluster_backup_recovery_select_name(cluster_recovery_target_name, &selected_point))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
							errmsg("cluster PITR named restore point is not reachable"),
							errdetail("Requested restore point \"%s\" is not recorded in this "
									  "backup manifest.",
									  cluster_recovery_target_name)));
	}
	if (have_scn_target)
	{
		int64 parsed = pg_strtoint64(cluster_recovery_target_scn);
		SCN requested_scn;

		if (parsed <= 0)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
							errmsg("cluster PITR target SCN is invalid")));
		requested_scn = (SCN)parsed;
		if (scn_time_cmp(requested_scn,
						 cluster_backup_recovery_manifest.consistent_scn) < 0)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
							errmsg("cluster PITR target SCN is before the backup consistent SCN"),
							errdetail("Requested SCN %llu, backup consistent SCN %llu.",
									  (unsigned long long)requested_scn,
									  (unsigned long long)
										  cluster_backup_recovery_manifest.consistent_scn)));
		if (!cluster_backup_recovery_select_scn(requested_scn, &selected_point))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
							errmsg("cluster PITR target SCN has no reachable restore point"),
							errdetail("Requested SCN %llu; backup manifest has %u restore "
									  "point(s).",
									  (unsigned long long) requested_scn,
									  cluster_backup_recovery_manifest.restore_point_count)));
	}
	if (have_time_target)
	{
		TimestampTz requested_time;

		requested_time = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
																 CStringGetDatum(cluster_recovery_target_cluster_time),
																 ObjectIdGetDatum(InvalidOid),
																 Int32GetDatum(-1)));
		if (!cluster_backup_recovery_select_time(requested_time, &selected_point))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
							errmsg("cluster PITR target time has no reachable restore point"),
							errdetail("Requested cluster time %s; backup manifest has %u restore "
									  "point(s).",
									  timestamptz_to_str(requested_time),
									  cluster_backup_recovery_manifest.restore_point_count)));
	}

	if (!selected_point.present)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE),
						errmsg("cluster PITR target has no reachable restore point")));

	cluster_backup_recovery_target_point = selected_point;
	cluster_backup_recovery_target_active = true;
	local_thread_id = cluster_wal_thread_id();
	if (local_thread_id == XLP_THREAD_ID_LEGACY)
		local_thread_id = 1;
	local_thread_index = (int)local_thread_id - 1;
	if (local_thread_index < 0 || local_thread_index >= CLUSTER_MAX_NODES
		|| !cluster_backup_recovery_manifest.threads[local_thread_index].present
		|| cluster_backup_recovery_manifest.threads[local_thread_index].stop_cut_lsn
			   == InvalidXLogRecPtr)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster PITR target is missing this node's WAL thread cut")));

	if (cluster_backup_recovery_manifest.thread_count > 1)
		cluster_backup_recovery_prepare_merge(&cluster_backup_recovery_target_point);
	else
	{
		recoveryTarget = RECOVERY_TARGET_LSN;
		recoveryTargetLSN = cluster_backup_recovery_target_point.cut_lsn[local_thread_index];
		recoveryTargetInclusive = true;
	}
	switch (cluster_recovery_target_action) {
	case CLUSTER_RECOVERY_TARGET_ACTION_PROMOTE:
		recoveryTargetAction = RECOVERY_TARGET_ACTION_PROMOTE;
		break;
	case CLUSTER_RECOVERY_TARGET_ACTION_SHUTDOWN:
		recoveryTargetAction = RECOVERY_TARGET_ACTION_SHUTDOWN;
		break;
	case CLUSTER_RECOVERY_TARGET_ACTION_PAUSE:
	default:
		recoveryTargetAction = RECOVERY_TARGET_ACTION_PAUSE;
		break;
	}
	ereport(LOG,
			(errmsg("cluster PITR target resolved to restore point \"%s\" at SCN %llu",
					cluster_backup_recovery_target_point.name,
					(unsigned long long) cluster_backup_recovery_target_point.cut_scn),
			 errdetail("Backup consistent SCN %llu; %u WAL thread(s) covered.",
					   (unsigned long long) cluster_backup_recovery_manifest.consistent_scn,
					   cluster_backup_recovery_manifest.thread_count)));
}

static XLogRecPtr cluster_adg_streaming_replay(XLogPrefetcher *xlogprefetcher,
											   XLogReaderState *xlogreader,
											   XLogRecord *first_record, TimeLineID tli,
											   TimeLineID *replayTLI,
											   bool *reachedRecoveryTarget);
#endif

static void EnableStandbyMode(void);
static void readRecoverySignalFile(void);
static void validateRecoveryParameters(void);
static bool read_backup_label(XLogRecPtr *checkPointLoc,
							  TimeLineID *backupLabelTLI,
							  bool *backupEndRequired, bool *backupFromStandby);
static bool read_tablespace_map(List **tablespaces);

static void xlogrecovery_redo(XLogReaderState *record, TimeLineID replayTLI);
static void CheckRecoveryConsistency(void);
static void rm_redo_error_callback(void *arg);
#ifdef WAL_DEBUG
static void xlog_outrec(StringInfo buf, XLogReaderState *record);
#endif
static void xlog_block_info(StringInfo buf, XLogReaderState *record);
static void checkTimeLineSwitch(XLogRecPtr lsn, TimeLineID newTLI,
								TimeLineID prevTLI, TimeLineID replayTLI);
static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime);
static void verifyBackupPageConsistency(XLogReaderState *record);

static bool recoveryStopsBefore(XLogReaderState *record);
static bool recoveryStopsAfter(XLogReaderState *record);
static char *getRecoveryStopReason(void);
static void recoveryPausesHere(bool endOfRecovery);
static bool recoveryApplyDelay(XLogReaderState *record);
static void ConfirmRecoveryPaused(void);

static XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher,
							  int emode, bool fetching_ckpt,
							  TimeLineID replayTLI);

static int	XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
						 int reqLen, XLogRecPtr targetRecPtr, char *readBuf);
static XLogPageReadResult WaitForWALToBecomeAvailable(XLogRecPtr RecPtr,
													  bool randAccess,
													  bool fetching_ckpt,
													  XLogRecPtr tliRecPtr,
													  TimeLineID replayTLI,
													  XLogRecPtr replayLSN,
													  bool nonblocking);
static int	emode_for_corrupt_record(int emode, XLogRecPtr RecPtr);
static XLogRecord *ReadCheckpointRecord(XLogPrefetcher *xlogprefetcher,
										XLogRecPtr RecPtr, TimeLineID replayTLI);
static bool rescanLatestTimeLine(TimeLineID replayTLI, XLogRecPtr replayLSN);
static int	XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli,
						 XLogSource source, bool notfoundOk);
static int	XLogFileReadAnyTLI(XLogSegNo segno, int emode, XLogSource source);

static bool CheckForStandbyTrigger(void);
static void SetPromoteIsTriggered(void);
static bool HotStandbyActiveInReplay(void);

static void SetCurrentChunkStartTime(TimestampTz xtime);
static void SetLatestXTime(TimestampTz xtime);

/*
 * Initialization of shared memory for WAL recovery
 */
Size
XLogRecoveryShmemSize(void)
{
	Size		size;

	/* XLogRecoveryCtl */
	size = sizeof(XLogRecoveryCtlData);

	return size;
}

void
XLogRecoveryShmemInit(void)
{
	bool		found;

	XLogRecoveryCtl = (XLogRecoveryCtlData *)
		ShmemInitStruct("XLOG Recovery Ctl", XLogRecoveryShmemSize(), &found);
	if (found)
		return;
	memset(XLogRecoveryCtl, 0, sizeof(XLogRecoveryCtlData));

	SpinLockInit(&XLogRecoveryCtl->info_lck);
	InitSharedLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
	ConditionVariableInit(&XLogRecoveryCtl->recoveryNotPausedCV);
}

/*
 * A thin wrapper to enable StandbyMode and do other preparatory work as
 * needed.
 */
static void
EnableStandbyMode(void)
{
	StandbyMode = true;

	/*
	 * To avoid server log bloat, we don't report recovery progress in a
	 * standby as it will always be in recovery unless promoted. We disable
	 * startup progress timeout in standby mode to avoid calling
	 * startup_progress_timeout_handler() unnecessarily.
	 */
	disable_startup_progress_timeout();
}

/*
 * Prepare the system for WAL recovery, if needed.
 *
 * This is called by StartupXLOG() which coordinates the server startup
 * sequence.  This function analyzes the control file and the backup label
 * file, if any, and figures out whether we need to perform crash recovery or
 * archive recovery, and how far we need to replay the WAL to reach a
 * consistent state.
 *
 * This doesn't yet change the on-disk state, except for creating the symlinks
 * from table space map file if any, and for fetching WAL files needed to find
 * the checkpoint record.  On entry, the caller has already read the control
 * file into memory, and passes it as argument.  This function updates it to
 * reflect the recovery state, and the caller is expected to write it back to
 * disk does after initializing other subsystems, but before calling
 * PerformWalRecovery().
 *
 * This initializes some global variables like ArchiveRecoveryRequested, and
 * StandbyModeRequested and InRecovery.
 */
void
InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdown_ptr,
				bool *haveBackupLabel_ptr, bool *haveTblspcMap_ptr)
{
	XLogPageReadPrivate *private;
	struct stat st;
	bool		wasShutdown;
	XLogRecord *record;
	DBState		dbstate_at_startup;
	bool		haveTblspcMap = false;
	bool		haveBackupLabel = false;
	CheckPoint	checkPoint;
	bool		backupFromStandby = false;

	dbstate_at_startup = ControlFile->state;

	/*
	 * Initialize on the assumption we want to recover to the latest timeline
	 * that's active according to pg_control.
	 */
	if (ControlFile->minRecoveryPointTLI >
		ControlFile->checkPointCopy.ThisTimeLineID)
		recoveryTargetTLI = ControlFile->minRecoveryPointTLI;
	else
		recoveryTargetTLI = ControlFile->checkPointCopy.ThisTimeLineID;

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC (spec-5.6a D3 / L437 sweep): the shared copy's TLI is the last
	 * authority writer's; initialize from this node's own anchor copy.
	 * minRecoveryPoint is an archive/standby-regime field and stays invalid
	 * on pgrac primaries, so the max() shape above is preserved.
	 */
	if (cluster_recovery_anchor_active() &&
		ControlFile->minRecoveryPointTLI <=
		cluster_recovery_anchor_get()->checkPointCopy.ThisTimeLineID)
		recoveryTargetTLI = cluster_recovery_anchor_get()->checkPointCopy.ThisTimeLineID;
#endif

	/*
	 * Check for signal files, and if so set up state for offline recovery
	 */
	readRecoverySignalFile();
	validateRecoveryParameters();
#ifdef USE_PGRAC_CLUSTER
	cluster_backup_recovery_load_manifest();
	cluster_backup_recovery_apply_target();
#endif

	if (ArchiveRecoveryRequested)
	{
		if (StandbyModeRequested)
			ereport(LOG,
					(errmsg("entering standby mode")));
		else if (recoveryTarget == RECOVERY_TARGET_XID)
			ereport(LOG,
					(errmsg("starting point-in-time recovery to XID %u",
							recoveryTargetXid)));
		else if (recoveryTarget == RECOVERY_TARGET_TIME)
			ereport(LOG,
					(errmsg("starting point-in-time recovery to %s",
							timestamptz_to_str(recoveryTargetTime))));
		else if (recoveryTarget == RECOVERY_TARGET_NAME)
			ereport(LOG,
					(errmsg("starting point-in-time recovery to \"%s\"",
							recoveryTargetName)));
		else if (recoveryTarget == RECOVERY_TARGET_LSN)
			ereport(LOG,
					(errmsg("starting point-in-time recovery to WAL location (LSN) \"%X/%X\"",
							LSN_FORMAT_ARGS(recoveryTargetLSN))));
		else if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE)
			ereport(LOG,
					(errmsg("starting point-in-time recovery to earliest consistent point")));
		else
			ereport(LOG,
					(errmsg("starting archive recovery")));
	}

	/*
	 * Take ownership of the wakeup latch if we're going to sleep during
	 * recovery.
	 */
	if (ArchiveRecoveryRequested)
		OwnLatch(&XLogRecoveryCtl->recoveryWakeupLatch);

	private = palloc0(sizeof(XLogPageReadPrivate));
	xlogreader =
		XLogReaderAllocate(wal_segment_size, NULL,
						   XL_ROUTINE(.page_read = &XLogPageRead,
									  .segment_open = NULL,
									  .segment_close = wal_segment_close),
						   private);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));
	xlogreader->system_identifier = ControlFile->system_identifier;

#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
	 * What changed: spec-4.1 -- own-stream strict thread-id acceptance
	 * for plain crash recovery.  Archive recovery and standby mode keep
	 * the accept-any-valid default set by XLogReaderAllocate: replayed
	 * WAL may legitimately carry another node's (the primary's) thread
	 * id and must never be rejected with this node's local identity
	 * (spec-4.1 RL1).
	 * Why: second defence against a mis-linked pg_wal serving a foreign
	 * thread's stream to crash recovery (spec-4.1 Q4-A; the first is
	 * cluster_wal_thread_init's startup directory check).
	 */
	if (!ArchiveRecoveryRequested && !StandbyModeRequested) {
		uint16 own_thread = cluster_wal_thread_id();

		if (own_thread != XLP_THREAD_ID_LEGACY)
			xlogreader->cluster_expected_thread_id = own_thread;
	}

	/*
	 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
	 * What changed: spec-4.3 -- observational recovery plan pass under
	 * the same plain-local gate as the RL1 hook above.  Runs before
	 * InRecovery is determined (plain startup planning, not "crash
	 * recovery confirmed"); the ControlFile facts let readers tell a
	 * clean-start plan from a crash-start plan (spec-4.3 P1-3).
	 * Why: merge-set enumeration input for spec-4.4/4.5; observational
	 * only, never blocks startup (spec-4.3 §3.3 fail-open).
	 */
	if (!ArchiveRecoveryRequested && !StandbyModeRequested)
	{
		DBState		plan_state = ControlFile->state;

		/*
		 * PGRAC (spec-5.6a D3 consumption point #3): the plan's
		 * dbstate_at_startup / local_recovery_needed inputs must describe
		 * THIS node's previous incarnation, not the last authority writer's.
		 */
		if (cluster_recovery_anchor_active())
			plan_state = (DBState) cluster_recovery_anchor_get()->state;

		cluster_recovery_plan_generate((uint32) plan_state,
									   plan_state != DB_SHUTDOWNED);

		/*
		 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
		 * What changed: spec-4.4 -- launch the candidate stream
		 * validation workers (dynamic bgworkers) when the plan reports
		 * crash candidates.  Registration only; never waits, recovery
		 * is not delayed (spec-4.4 §3.1).
		 * Why: pre-merge stream readability evidence for spec-4.5.
		 */
		cluster_recovery_workers_launch();
	}
#endif

	/*
	 * Set the WAL decode buffer size.  This limits how far ahead we can read
	 * in the WAL.
	 */
	XLogReaderSetDecodeBuffer(xlogreader, NULL, wal_decode_buffer_size);

	/* Create a WAL prefetcher. */
	xlogprefetcher = XLogPrefetcherAllocate(xlogreader);

	/*
	 * Allocate two page buffers dedicated to WAL consistency checks.  We do
	 * it this way, rather than just making static arrays, for two reasons:
	 * (1) no need to waste the storage in most instantiations of the backend;
	 * (2) a static char array isn't guaranteed to have any particular
	 * alignment, whereas palloc() will provide MAXALIGN'd storage.
	 */
	replay_image_masked = (char *) palloc(BLCKSZ);
	primary_image_masked = (char *) palloc(BLCKSZ);

	if (read_backup_label(&CheckPointLoc, &CheckPointTLI, &backupEndRequired,
						  &backupFromStandby))
	{
		List	   *tablespaces = NIL;

#ifdef USE_PGRAC_CLUSTER
		/*
		 * PGRAC: a backup label can point at the source node's WAL checkpoint.
		 * Backup-label recovery is replay, not plain local crash recovery, and
		 * must accept any valid thread id just like archive/standby replay.
		 * Keep the strict own-thread check for the local crash recovery path
		 * with no recovery signal and no backup_label.
		 */
		xlogreader->cluster_expected_thread_id = XLP_THREAD_ID_INVALID;
#endif

		/*
		 * Archive recovery was requested, and thanks to the backup label
		 * file, we know how far we need to replay to reach consistency. Enter
		 * archive recovery directly.
		 */
		InArchiveRecovery = true;
		if (StandbyModeRequested)
			EnableStandbyMode();

		/*
		 * Omitting backup_label when creating a new replica, PITR node etc.
		 * unfortunately is a common cause of corruption.  Logging that
		 * backup_label was used makes it a bit easier to exclude that as the
		 * cause of observed corruption.
		 *
		 * Do so before we try to read the checkpoint record (which can fail),
		 * as otherwise it can be hard to understand why a checkpoint other
		 * than ControlFile->checkPoint is used.
		 */
		ereport(LOG,
				(errmsg("starting backup recovery with redo LSN %X/%X, checkpoint LSN %X/%X, on timeline ID %u",
						LSN_FORMAT_ARGS(RedoStartLSN),
						LSN_FORMAT_ARGS(CheckPointLoc),
						CheckPointTLI)));

		/*
		 * When a backup_label file is present, we want to roll forward from
		 * the checkpoint it identifies, rather than using pg_control.
		 */
		record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc,
									  CheckPointTLI);
		if (record != NULL)
		{
			memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
			wasShutdown = ((record->xl_info & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN);
#ifdef USE_PGRAC_CLUSTER
			/* PGRAC (spec-4.5a G6): restore the SCN clock from the checkpoint
			 * record's stamp.  Replay-side observes (xact_redo) only fire for
			 * records replayed AFTER this point; without this seed a start
			 * that replays no commit (clean shutdown, or an idle crash
			 * window) boots with SCN 0 -- snapshots then carry an invalid
			 * read_scn and every commit_scn comparison fails closed (and a
			 * later commit would mint SCNs below already-durable ones). */
			cluster_scn_recovery_replay_observe(record->xl_scn);
#endif
			ereport(DEBUG1,
					(errmsg_internal("checkpoint record is at %X/%X",
									 LSN_FORMAT_ARGS(CheckPointLoc))));
			InRecovery = true;	/* force recovery even if SHUTDOWNED */

			/*
			 * Make sure that REDO location exists. This may not be the case
			 * if there was a crash during an online backup, which left a
			 * backup_label around that references a WAL segment that's
			 * already been archived.
			 */
			if (checkPoint.redo < CheckPointLoc)
			{
				XLogPrefetcherBeginRead(xlogprefetcher, checkPoint.redo);
				if (!ReadRecord(xlogprefetcher, LOG, false,
								checkPoint.ThisTimeLineID))
					ereport(FATAL,
							(errmsg("could not find redo location referenced by checkpoint record"),
							 errhint("If you are restoring from a backup, touch \"%s/recovery.signal\" and add required recovery options.\n"
									 "If you are not restoring from a backup, try removing the file \"%s/backup_label\".\n"
									 "Be careful: removing \"%s/backup_label\" will result in a corrupt cluster if restoring from a backup.",
									 DataDir, DataDir, DataDir)));
			}
		}
		else
		{
			ereport(FATAL,
					(errmsg("could not locate required checkpoint record"),
					 errhint("If you are restoring from a backup, touch \"%s/recovery.signal\" and add required recovery options.\n"
							 "If you are not restoring from a backup, try removing the file \"%s/backup_label\".\n"
							 "Be careful: removing \"%s/backup_label\" will result in a corrupt cluster if restoring from a backup.",
							 DataDir, DataDir, DataDir)));
			wasShutdown = false;	/* keep compiler quiet */
		}

		/* Read the tablespace_map file if present and create symlinks. */
		if (read_tablespace_map(&tablespaces))
		{
			ListCell   *lc;

			foreach(lc, tablespaces)
			{
				tablespaceinfo *ti = lfirst(lc);
				char	   *linkloc;

				linkloc = psprintf("pg_tblspc/%s", ti->oid);

				/*
				 * Remove the existing symlink if any and Create the symlink
				 * under PGDATA.
				 */
				remove_tablespace_symlink(linkloc);

				if (symlink(ti->path, linkloc) < 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not create symbolic link \"%s\": %m",
									linkloc)));

				pfree(ti->oid);
				pfree(ti->path);
				pfree(ti);
			}

			/* tell the caller to delete it later */
			haveTblspcMap = true;
		}

		/* tell the caller to delete it later */
		haveBackupLabel = true;
	}
	else
	{
		/*
		 * If tablespace_map file is present without backup_label file, there
		 * is no use of such file.  There is no harm in retaining it, but it
		 * is better to get rid of the map file so that we don't have any
		 * redundant file in data directory and it will avoid any sort of
		 * confusion.  It seems prudent though to just rename the file out of
		 * the way rather than delete it completely, also we ignore any error
		 * that occurs in rename operation as even if map file is present
		 * without backup_label file, it is harmless.
		 */
		if (stat(TABLESPACE_MAP, &st) == 0)
		{
			unlink(TABLESPACE_MAP_OLD);
			if (durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, DEBUG1) == 0)
				ereport(LOG,
						(errmsg("ignoring file \"%s\" because no file \"%s\" exists",
								TABLESPACE_MAP, BACKUP_LABEL_FILE),
						 errdetail("File \"%s\" was renamed to \"%s\".",
								   TABLESPACE_MAP, TABLESPACE_MAP_OLD)));
			else
				ereport(LOG,
						(errmsg("ignoring file \"%s\" because no file \"%s\" exists",
								TABLESPACE_MAP, BACKUP_LABEL_FILE),
						 errdetail("Could not rename file \"%s\" to \"%s\": %m.",
								   TABLESPACE_MAP, TABLESPACE_MAP_OLD)));
		}

		/*
		 * It's possible that archive recovery was requested, but we don't
		 * know how far we need to replay the WAL before we reach consistency.
		 * This can happen for example if a base backup is taken from a
		 * running server using an atomic filesystem snapshot, without calling
		 * pg_backup_start/stop. Or if you just kill a running primary server
		 * and put it into archive recovery by creating a recovery signal
		 * file.
		 *
		 * Our strategy in that case is to perform crash recovery first,
		 * replaying all the WAL present in pg_wal, and only enter archive
		 * recovery after that.
		 *
		 * But usually we already know how far we need to replay the WAL (up
		 * to minRecoveryPoint, up to backupEndPoint, or until we see an
		 * end-of-backup record), and we can enter archive recovery directly.
		 */
		if (ArchiveRecoveryRequested &&
			(ControlFile->minRecoveryPoint != InvalidXLogRecPtr ||
			 ControlFile->backupEndRequired ||
			 ControlFile->backupEndPoint != InvalidXLogRecPtr ||
			 ControlFile->state == DB_SHUTDOWNED))
		{
			InArchiveRecovery = true;
			if (StandbyModeRequested)
				EnableStandbyMode();
		}

		/*
		 * For the same reason as when starting up with backup_label present,
		 * emit a log message when we continue initializing from a base
		 * backup.
		 */
		if (!XLogRecPtrIsInvalid(ControlFile->backupStartPoint))
			ereport(LOG,
					(errmsg("restarting backup recovery with redo LSN %X/%X",
							LSN_FORMAT_ARGS(ControlFile->backupStartPoint))));

		/* Get the last valid checkpoint record. */
		CheckPointLoc = ControlFile->checkPoint;
		CheckPointTLI = ControlFile->checkPointCopy.ThisTimeLineID;
		RedoStartLSN = ControlFile->checkPointCopy.redo;
		RedoStartTLI = ControlFile->checkPointCopy.ThisTimeLineID;

#ifdef USE_PGRAC_CLUSTER

		/*
		 * PGRAC (spec-5.6a D3 consumption point #4): under the shared
		 * pg_control authority the fields just read are the last authority
		 * writer's own-thread values; locating that checkpoint in THIS
		 * node's WAL thread PANICs ("could not locate a valid checkpoint
		 * record") or, worse, replays from a foreign redo start.  Take this
		 * node's own last checkpoint from its recovery anchor (this also
		 * heals the cold-merge own-stream start, which is derived from
		 * RedoStartLSN).  A label-less authority boot without a valid
		 * anchor already failed closed (53RB3) in StartupXLOG.
		 */
		if (cluster_recovery_anchor_active())
		{
			const ClusterRecoveryAnchor *ra = cluster_recovery_anchor_get();

			CheckPointLoc = ra->checkPoint;
			CheckPointTLI = ra->checkPointCopy.ThisTimeLineID;
			RedoStartLSN = ra->checkPointCopy.redo;
			RedoStartTLI = ra->checkPointCopy.ThisTimeLineID;
		}
#endif
		record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc,
									  CheckPointTLI);
		if (record != NULL)
		{
			ereport(DEBUG1,
					(errmsg_internal("checkpoint record is at %X/%X",
									 LSN_FORMAT_ARGS(CheckPointLoc))));
		}
		else
		{
			/*
			 * We used to attempt to go back to a secondary checkpoint record
			 * here, but only when not in standby mode. We now just fail if we
			 * can't read the last checkpoint because this allows us to
			 * simplify processing around checkpoints.
			 */
			ereport(PANIC,
					(errmsg("could not locate a valid checkpoint record")));
		}
		memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
		wasShutdown = ((record->xl_info & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN);
#ifdef USE_PGRAC_CLUSTER
		/* PGRAC (spec-4.5a G6): seed the SCN clock from the checkpoint record
		 * (see the backup_label arm above for why). */
		cluster_scn_recovery_replay_observe(record->xl_scn);
		/* Capture the actual own WAL record, not a shared control-file copy.
		 * Semantic recovery retains the initial online checkpoint too; neither
		 * capture certifies CLEAN. StartupXLOG separately confirms clean start. */
		if (wasShutdown)
			cluster_tt_slot_capture_startup_checkpoint(record->xl_scn, checkPoint.nextXid);
		cluster_semantic_normal_start_capture(CheckPointLoc, checkPoint.redo,
											  U64FromFullTransactionId(checkPoint.nextXid),
											  record->xl_scn);
#endif

		/* Make sure that REDO location exists. */
		if (checkPoint.redo < CheckPointLoc)
		{
			XLogPrefetcherBeginRead(xlogprefetcher, checkPoint.redo);
			if (!ReadRecord(xlogprefetcher, LOG, false, checkPoint.ThisTimeLineID))
				ereport(PANIC,
						errmsg("could not find redo location %X/%08X referenced by checkpoint record at %X/%08X",
							   LSN_FORMAT_ARGS(checkPoint.redo), LSN_FORMAT_ARGS(CheckPointLoc)));
		}
	}

	/*
	 * If the location of the checkpoint record is not on the expected
	 * timeline in the history of the requested timeline, we cannot proceed:
	 * the backup is not part of the history of the requested timeline.
	 */
	Assert(expectedTLEs);		/* was initialized by reading checkpoint
								 * record */
	if (tliOfPointInHistory(CheckPointLoc, expectedTLEs) !=
		CheckPointTLI)
	{
		XLogRecPtr	switchpoint;

		/*
		 * tliSwitchPoint will throw an error if the checkpoint's timeline is
		 * not in expectedTLEs at all.
		 */
		switchpoint = tliSwitchPoint(CheckPointTLI, expectedTLEs, NULL);
		ereport(FATAL,
				(errmsg("requested timeline %u is not a child of this server's history",
						recoveryTargetTLI),
				 errdetail("Latest checkpoint is at %X/%X on timeline %u, but in the history of the requested timeline, the server forked off from that timeline at %X/%X.",
						   LSN_FORMAT_ARGS(CheckPointLoc),
						   CheckPointTLI,
						   LSN_FORMAT_ARGS(switchpoint))));
	}

	/*
	 * The min recovery point should be part of the requested timeline's
	 * history, too.
	 */
	if (!XLogRecPtrIsInvalid(ControlFile->minRecoveryPoint) &&
		tliOfPointInHistory(ControlFile->minRecoveryPoint - 1, expectedTLEs) !=
		ControlFile->minRecoveryPointTLI)
		ereport(FATAL,
				(errmsg("requested timeline %u does not contain minimum recovery point %X/%X on timeline %u",
						recoveryTargetTLI,
						LSN_FORMAT_ARGS(ControlFile->minRecoveryPoint),
						ControlFile->minRecoveryPointTLI)));

	ereport(DEBUG1,
			(errmsg_internal("redo record is at %X/%X; shutdown %s",
							 LSN_FORMAT_ARGS(checkPoint.redo),
							 wasShutdown ? "true" : "false")));
	ereport(DEBUG1,
			(errmsg_internal("next transaction ID: " UINT64_FORMAT "; next OID: %u",
							 U64FromFullTransactionId(checkPoint.nextXid),
							 checkPoint.nextOid)));
	ereport(DEBUG1,
			(errmsg_internal("next MultiXactId: %u; next MultiXactOffset: %u",
							 checkPoint.nextMulti, checkPoint.nextMultiOffset)));
	ereport(DEBUG1,
			(errmsg_internal("oldest unfrozen transaction ID: %u, in database %u",
							 checkPoint.oldestXid, checkPoint.oldestXidDB)));
	ereport(DEBUG1,
			(errmsg_internal("oldest MultiXactId: %u, in database %u",
							 checkPoint.oldestMulti, checkPoint.oldestMultiDB)));
	ereport(DEBUG1,
			(errmsg_internal("commit timestamp Xid oldest/newest: %u/%u",
							 checkPoint.oldestCommitTsXid,
							 checkPoint.newestCommitTsXid)));
	if (!TransactionIdIsNormal(XidFromFullTransactionId(checkPoint.nextXid)))
		ereport(PANIC,
				(errmsg("invalid next transaction ID")));

	/* sanity check */
	if (checkPoint.redo > CheckPointLoc)
		ereport(PANIC,
				(errmsg("invalid redo in checkpoint record")));

	/*
	 * Check whether we need to force recovery from WAL.  If it appears to
	 * have been a clean shutdown and we did not have a recovery signal file,
	 * then assume no recovery needed.
	 */
	if (checkPoint.redo < CheckPointLoc)
	{
		if (wasShutdown)
			ereport(PANIC,
					(errmsg("invalid redo record in shutdown checkpoint")));
		InRecovery = true;
	}
#ifdef USE_PGRAC_CLUSTER

	/*
	 * PGRAC (spec-5.6a D3 / L437 sweep): the clean-shutdown test must use
	 * this node's own state.  A peer's clean shutdown recorded in the shared
	 * authority must not mask this node's crash (its tail WAL would be
	 * silently skipped -- lost writes), and a peer's crash must not force
	 * recovery after this node's clean shutdown.
	 */
	else if (cluster_recovery_anchor_active() ?
			 cluster_recovery_anchor_get()->state != (uint32) DB_SHUTDOWNED :
			 ControlFile->state != DB_SHUTDOWNED)
		InRecovery = true;
#else
	else if (ControlFile->state != DB_SHUTDOWNED)
		InRecovery = true;
#endif
	else if (ArchiveRecoveryRequested)
	{
		/* force recovery due to presence of recovery signal file */
		InRecovery = true;
	}

	/*
	 * If recovery is needed, update our in-memory copy of pg_control to show
	 * that we are recovering and to show the selected checkpoint as the place
	 * we are starting from. We also mark pg_control with any minimum recovery
	 * stop point obtained from a backup history file.
	 *
	 * We don't write the changes to disk yet, though. Only do that after
	 * initializing various subsystems.
	 */
	if (InRecovery)
	{
		if (InArchiveRecovery)
		{
			ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
		}
		else
		{
			ereport(LOG,
					(errmsg("database system was not properly shut down; "
							"automatic recovery in progress")));
			if (recoveryTargetTLI > ControlFile->checkPointCopy.ThisTimeLineID)
				ereport(LOG,
						(errmsg("crash recovery starts in timeline %u "
								"and has target timeline %u",
								ControlFile->checkPointCopy.ThisTimeLineID,
								recoveryTargetTLI)));
			ControlFile->state = DB_IN_CRASH_RECOVERY;
		}
		ControlFile->checkPoint = CheckPointLoc;
		ControlFile->checkPointCopy = checkPoint;
		if (InArchiveRecovery)
		{
			/* initialize minRecoveryPoint if not set yet */
			if (ControlFile->minRecoveryPoint < checkPoint.redo)
			{
				ControlFile->minRecoveryPoint = checkPoint.redo;
				ControlFile->minRecoveryPointTLI = checkPoint.ThisTimeLineID;
			}
		}

		/*
		 * Set backupStartPoint if we're starting recovery from a base backup.
		 *
		 * Also set backupEndPoint and use minRecoveryPoint as the backup end
		 * location if we're starting recovery from a base backup which was
		 * taken from a standby. In this case, the database system status in
		 * pg_control must indicate that the database was already in recovery.
		 * Usually that will be DB_IN_ARCHIVE_RECOVERY but also can be
		 * DB_SHUTDOWNED_IN_RECOVERY if recovery previously was interrupted
		 * before reaching this point; e.g. because restore_command or
		 * primary_conninfo were faulty.
		 *
		 * Any other state indicates that the backup somehow became corrupted
		 * and we can't sensibly continue with recovery.
		 */
		if (haveBackupLabel)
		{
			ControlFile->backupStartPoint = checkPoint.redo;
			ControlFile->backupEndRequired = backupEndRequired;

			if (backupFromStandby)
			{
				if (dbstate_at_startup != DB_IN_ARCHIVE_RECOVERY &&
					dbstate_at_startup != DB_SHUTDOWNED_IN_RECOVERY)
					ereport(FATAL,
							(errmsg("backup_label contains data inconsistent with control file"),
							 errhint("This means that the backup is corrupted and you will "
									 "have to use another backup for recovery.")));
				ControlFile->backupEndPoint = ControlFile->minRecoveryPoint;
			}
		}
	}

	/* remember these, so that we know when we have reached consistency */
	backupStartPoint = ControlFile->backupStartPoint;
	backupEndRequired = ControlFile->backupEndRequired;
	backupEndPoint = ControlFile->backupEndPoint;
	if (InArchiveRecovery)
	{
		minRecoveryPoint = ControlFile->minRecoveryPoint;
		minRecoveryPointTLI = ControlFile->minRecoveryPointTLI;
	}
	else
	{
		minRecoveryPoint = InvalidXLogRecPtr;
		minRecoveryPointTLI = 0;
	}

	/*
	 * Start recovery assuming that the final record isn't lost.
	 */
	abortedRecPtr = InvalidXLogRecPtr;
	missingContrecPtr = InvalidXLogRecPtr;

#ifdef USE_PGRAC_CLUSTER
	/*
	 * Classify from the actual WAL/recovery decision, never from whether an HW
	 * file happens to exist.  checkPoint is the record decoded for this boot;
	 * the shared control-file copy may describe a different owner's checkpoint.
	 * A normal boot must rebuild before checkpoint or allocation can consume HW.
	 */
	{
		const char *hw_failure = NULL;

		if (!cluster_hw_startup_prepare(
				InRecovery, wasShutdown && !haveBackupLabel && !ArchiveRecoveryRequested,
				checkPoint.redo, &hw_failure))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_RELATION_EXTEND_UNAVAILABLE),
					 errmsg("could not prepare cluster HW startup authority"),
					 errdetail("reason=%s", hw_failure != NULL ? hw_failure : "UNCLASSIFIED")));
		if (cluster_hw_metadata_configured())
			ereport(LOG, (errmsg("cluster HW startup mode=%s state=%u",
								 InRecovery ? "RECOVERY_EXISTING_CONTRACT" : "NORMAL_SELF",
								 (unsigned)cluster_hw_cold_boot_state())));
	}
#endif

	*wasShutdown_ptr = wasShutdown;
	*haveBackupLabel_ptr = haveBackupLabel;
	*haveTblspcMap_ptr = haveTblspcMap;
}

/*
 * See if there are any recovery signal files and if so, set state for
 * recovery.
 *
 * See if there is a recovery command file (recovery.conf), and if so
 * throw an ERROR since as of PG12 we no longer recognize that.
 */
static void
readRecoverySignalFile(void)
{
	struct stat stat_buf;

	if (IsBootstrapProcessingMode())
		return;

	/*
	 * Check for old recovery API file: recovery.conf
	 */
	if (stat(RECOVERY_COMMAND_FILE, &stat_buf) == 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("using recovery command file \"%s\" is not supported",
						RECOVERY_COMMAND_FILE)));

	/*
	 * Remove unused .done file, if present. Ignore if absent.
	 */
	unlink(RECOVERY_COMMAND_DONE);

	/*
	 * Check for recovery signal files and if found, fsync them since they
	 * represent server state information.  We don't sweat too much about the
	 * possibility of fsync failure, however.
	 *
	 * If present, standby signal file takes precedence. If neither is present
	 * then we won't enter archive recovery.
	 */
	if (stat(STANDBY_SIGNAL_FILE, &stat_buf) == 0)
	{
		int			fd;

		fd = BasicOpenFilePerm(STANDBY_SIGNAL_FILE, O_RDWR | PG_BINARY,
							   S_IRUSR | S_IWUSR);
		if (fd >= 0)
		{
			(void) pg_fsync(fd);
			close(fd);
		}
		standby_signal_file_found = true;
	}
	else if (stat(RECOVERY_SIGNAL_FILE, &stat_buf) == 0)
	{
		int			fd;

		fd = BasicOpenFilePerm(RECOVERY_SIGNAL_FILE, O_RDWR | PG_BINARY,
							   S_IRUSR | S_IWUSR);
		if (fd >= 0)
		{
			(void) pg_fsync(fd);
			close(fd);
		}
		recovery_signal_file_found = true;
	}

	StandbyModeRequested = false;
	ArchiveRecoveryRequested = false;
	if (standby_signal_file_found)
	{
		StandbyModeRequested = true;
		ArchiveRecoveryRequested = true;
	}
	else if (recovery_signal_file_found)
	{
		StandbyModeRequested = false;
		ArchiveRecoveryRequested = true;
	}
	else
		return;

	/*
	 * We don't support standby mode in standalone backends; that requires
	 * other processes such as the WAL receiver to be alive.
	 */
	if (StandbyModeRequested && !IsUnderPostmaster)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("standby mode is not supported by single-user servers")));
}

static void
validateRecoveryParameters(void)
{
	if (!ArchiveRecoveryRequested)
		return;

	/*
	 * Check for compulsory parameters
	 */
	if (StandbyModeRequested)
	{
		if ((PrimaryConnInfo == NULL || strcmp(PrimaryConnInfo, "") == 0) &&
			(recoveryRestoreCommand == NULL || strcmp(recoveryRestoreCommand, "") == 0))
			ereport(WARNING,
					(errmsg("specified neither primary_conninfo nor restore_command"),
					 errhint("The database server will regularly poll the pg_wal subdirectory to check for files placed there.")));
	}
	else
	{
		if (recoveryRestoreCommand == NULL ||
			strcmp(recoveryRestoreCommand, "") == 0)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("must specify restore_command when standby mode is not enabled")));
	}

	/*
	 * Override any inconsistent requests. Note that this is a change of
	 * behaviour in 9.5; prior to this we simply ignored a request to pause if
	 * hot_standby = off, which was surprising behaviour.
	 */
	if (recoveryTargetAction == RECOVERY_TARGET_ACTION_PAUSE &&
		!EnableHotStandby)
		recoveryTargetAction = RECOVERY_TARGET_ACTION_SHUTDOWN;

	/*
	 * Final parsing of recovery_target_time string; see also
	 * check_recovery_target_time().
	 */
	if (recoveryTarget == RECOVERY_TARGET_TIME)
	{
		recoveryTargetTime = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
																	 CStringGetDatum(recovery_target_time_string),
																	 ObjectIdGetDatum(InvalidOid),
																	 Int32GetDatum(-1)));
	}

	/*
	 * If user specified recovery_target_timeline, validate it or compute the
	 * "latest" value.  We can't do this until after we've gotten the restore
	 * command and set InArchiveRecovery, because we need to fetch timeline
	 * history files from the archive.
	 */
	if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC)
	{
		TimeLineID	rtli = recoveryTargetTLIRequested;

		/* Timeline 1 does not have a history file, all else should */
		if (rtli != 1 && !existsTimeLineHistory(rtli))
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("recovery target timeline %u does not exist",
							rtli)));
		recoveryTargetTLI = rtli;
	}
	else if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST)
	{
		/* We start the "latest" search from pg_control's timeline */
		recoveryTargetTLI = findNewestTimeLine(recoveryTargetTLI);
	}
	else
	{
		/*
		 * else we just use the recoveryTargetTLI as already read from
		 * ControlFile
		 */
		Assert(recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_CONTROLFILE);
	}
}

/*
 * read_backup_label: check to see if a backup_label file is present
 *
 * If we see a backup_label during recovery, we assume that we are recovering
 * from a backup dump file, and we therefore roll forward from the checkpoint
 * identified by the label file, NOT what pg_control says.  This avoids the
 * problem that pg_control might have been archived one or more checkpoints
 * later than the start of the dump, and so if we rely on it as the start
 * point, we will fail to restore a consistent database state.
 *
 * Returns true if a backup_label was found (and fills the checkpoint
 * location and TLI into *checkPointLoc and *backupLabelTLI, respectively);
 * returns false if not. If this backup_label came from a streamed backup,
 * *backupEndRequired is set to true. If this backup_label was created during
 * recovery, *backupFromStandby is set to true.
 *
 * Also sets the global variables RedoStartLSN and RedoStartTLI with the LSN
 * and TLI read from the backup file.
 */
static bool
read_backup_label(XLogRecPtr *checkPointLoc, TimeLineID *backupLabelTLI,
				  bool *backupEndRequired, bool *backupFromStandby)
{
	char		startxlogfilename[MAXFNAMELEN];
	TimeLineID	tli_from_walseg,
				tli_from_file;
	FILE	   *lfp;
	char		ch;
	char		backuptype[20];
	char		backupfrom[20];
	char		backuplabel[MAXPGPATH];
	char		backuptime[128];
	uint32		hi,
				lo;

	/* suppress possible uninitialized-variable warnings */
	*checkPointLoc = InvalidXLogRecPtr;
	*backupLabelTLI = 0;
	*backupEndRequired = false;
	*backupFromStandby = false;

	/*
	 * See if label file is present
	 */
	lfp = AllocateFile(BACKUP_LABEL_FILE, "r");
	if (!lfp)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							BACKUP_LABEL_FILE)));
		return false;			/* it's not there, all is fine */
	}

	/*
	 * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
	 * is pretty crude, but we are not expecting any variability in the file
	 * format).
	 */
	if (fscanf(lfp, "START WAL LOCATION: %X/%X (file %08X%16s)%c",
			   &hi, &lo, &tli_from_walseg, startxlogfilename, &ch) != 5 || ch != '\n')
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
	RedoStartLSN = ((uint64) hi) << 32 | lo;
	RedoStartTLI = tli_from_walseg;
	if (fscanf(lfp, "CHECKPOINT LOCATION: %X/%X%c",
			   &hi, &lo, &ch) != 3 || ch != '\n')
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
	*checkPointLoc = ((uint64) hi) << 32 | lo;
	*backupLabelTLI = tli_from_walseg;

	/*
	 * BACKUP METHOD lets us know if this was a typical backup ("streamed",
	 * which could mean either pg_basebackup or the pg_backup_start/stop
	 * method was used) or if this label came from somewhere else (the only
	 * other option today being from pg_rewind).  If this was a streamed
	 * backup then we know that we need to play through until we get to the
	 * end of the WAL which was generated during the backup (at which point we
	 * will have reached consistency and backupEndRequired will be reset to be
	 * false).
	 */
	if (fscanf(lfp, "BACKUP METHOD: %19s\n", backuptype) == 1)
	{
		if (strcmp(backuptype, "streamed") == 0)
			*backupEndRequired = true;
	}

	/*
	 * BACKUP FROM lets us know if this was from a primary or a standby.  If
	 * it was from a standby, we'll double-check that the control file state
	 * matches that of a standby.
	 */
	if (fscanf(lfp, "BACKUP FROM: %19s\n", backupfrom) == 1)
	{
		if (strcmp(backupfrom, "standby") == 0)
			*backupFromStandby = true;
	}

	/*
	 * Parse START TIME and LABEL. Those are not mandatory fields for recovery
	 * but checking for their presence is useful for debugging and the next
	 * sanity checks. Cope also with the fact that the result buffers have a
	 * pre-allocated size, hence if the backup_label file has been generated
	 * with strings longer than the maximum assumed here an incorrect parsing
	 * happens. That's fine as only minor consistency checks are done
	 * afterwards.
	 */
	if (fscanf(lfp, "START TIME: %127[^\n]\n", backuptime) == 1)
		ereport(DEBUG1,
				(errmsg_internal("backup time %s in file \"%s\"",
								 backuptime, BACKUP_LABEL_FILE)));

	if (fscanf(lfp, "LABEL: %1023[^\n]\n", backuplabel) == 1)
		ereport(DEBUG1,
				(errmsg_internal("backup label %s in file \"%s\"",
								 backuplabel, BACKUP_LABEL_FILE)));

	/*
	 * START TIMELINE is new as of 11. Its parsing is not mandatory, still use
	 * it as a sanity check if present.
	 */
	if (fscanf(lfp, "START TIMELINE: %u\n", &tli_from_file) == 1)
	{
		if (tli_from_walseg != tli_from_file)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE),
					 errdetail("Timeline ID parsed is %u, but expected %u.",
							   tli_from_file, tli_from_walseg)));

		ereport(DEBUG1,
				(errmsg_internal("backup timeline %u in file \"%s\"",
								 tli_from_file, BACKUP_LABEL_FILE)));
	}

	if (ferror(lfp) || FreeFile(lfp))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						BACKUP_LABEL_FILE)));

	return true;
}

/*
 * read_tablespace_map: check to see if a tablespace_map file is present
 *
 * If we see a tablespace_map file during recovery, we assume that we are
 * recovering from a backup dump file, and we therefore need to create symlinks
 * as per the information present in tablespace_map file.
 *
 * Returns true if a tablespace_map file was found (and fills *tablespaces
 * with a tablespaceinfo struct for each tablespace listed in the file);
 * returns false if not.
 */
static bool
read_tablespace_map(List **tablespaces)
{
	tablespaceinfo *ti;
	FILE	   *lfp;
	char		str[MAXPGPATH];
	int			ch,
				i,
				n;
	bool		was_backslash;

	/*
	 * See if tablespace_map file is present
	 */
	lfp = AllocateFile(TABLESPACE_MAP, "r");
	if (!lfp)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							TABLESPACE_MAP)));
		return false;			/* it's not there, all is fine */
	}

	/*
	 * Read and parse the link name and path lines from tablespace_map file
	 * (this code is pretty crude, but we are not expecting any variability in
	 * the file format).  De-escape any backslashes that were inserted.
	 */
	i = 0;
	was_backslash = false;
	while ((ch = fgetc(lfp)) != EOF)
	{
		if (!was_backslash && (ch == '\n' || ch == '\r'))
		{
			if (i == 0)
				continue;		/* \r immediately followed by \n */

			/*
			 * The de-escaped line should contain an OID followed by exactly
			 * one space followed by a path.  The path might start with
			 * spaces, so don't be too liberal about parsing.
			 */
			str[i] = '\0';
			n = 0;
			while (str[n] && str[n] != ' ')
				n++;
			if (n < 1 || n >= i - 1)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));
			str[n++] = '\0';

			ti = palloc0(sizeof(tablespaceinfo));
			ti->oid = pstrdup(str);
			ti->path = pstrdup(str + n);
			*tablespaces = lappend(*tablespaces, ti);

			i = 0;
			continue;
		}
		else if (!was_backslash && ch == '\\')
			was_backslash = true;
		else
		{
			if (i < sizeof(str) - 1)
				str[i++] = ch;
			was_backslash = false;
		}
	}

	if (i != 0 || was_backslash)	/* last line not terminated? */
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));

	if (ferror(lfp) || FreeFile(lfp))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						TABLESPACE_MAP)));

	return true;
}

/*
 * Finish WAL recovery.
 *
 * This does not close the 'xlogreader' yet, because in some cases the caller
 * still wants to re-read the last checkpoint record by calling
 * ReadCheckpointRecord().
 *
 * Returns the position of the last valid or applied record, after which new
 * WAL should be appended, information about why recovery was ended, and some
 * other things. See the EndOfWalRecoveryInfo struct for details.
 */
EndOfWalRecoveryInfo *
FinishWalRecovery(void)
{
	EndOfWalRecoveryInfo *result = palloc(sizeof(EndOfWalRecoveryInfo));
	XLogRecPtr	lastRec;
	TimeLineID	lastRecTLI;
	XLogRecPtr	endOfLog;

	/*
	 * Kill WAL receiver, if it's still running, before we continue to write
	 * the startup checkpoint and aborted-contrecord records. It will trump
	 * over these records and subsequent ones if it's still alive when we
	 * start writing WAL.
	 */
	XLogShutdownWalRcv();

	/*
	 * We are now done reading the xlog from stream. Turn off streaming
	 * recovery to force fetching the files (which would be required at end of
	 * recovery, e.g., timeline history file) from archive or pg_wal.
	 *
	 * Note that standby mode must be turned off after killing WAL receiver,
	 * i.e., calling XLogShutdownWalRcv().
	 */
	Assert(!WalRcvStreaming());
	StandbyMode = false;

	/*
	 * Determine where to start writing WAL next.
	 *
	 * Re-fetch the last valid or last applied record, so we can identify the
	 * exact endpoint of what we consider the valid portion of WAL.  There may
	 * be an incomplete continuation record after that, in which case
	 * 'abortedRecPtr' and 'missingContrecPtr' are set and the caller will
	 * write a special OVERWRITE_CONTRECORD message to mark that the rest of
	 * it is intentionally missing.  See CreateOverwriteContrecordRecord().
	 *
	 * An important side-effect of this is to load the last page into
	 * xlogreader. The caller uses it to initialize the WAL for writing.
	 */
	if (!InRecovery)
	{
		lastRec = CheckPointLoc;
		lastRecTLI = CheckPointTLI;
	}
	else
	{
		lastRec = XLogRecoveryCtl->lastReplayedReadRecPtr;
		lastRecTLI = XLogRecoveryCtl->lastReplayedTLI;
	}
	XLogPrefetcherBeginRead(xlogprefetcher, lastRec);
	(void) ReadRecord(xlogprefetcher, PANIC, false, lastRecTLI);
	endOfLog = xlogreader->EndRecPtr;

	/*
	 * Remember the TLI in the filename of the XLOG segment containing the
	 * end-of-log.  It could be different from the timeline that endOfLog
	 * nominally belongs to, if there was a timeline switch in that segment,
	 * and we were reading the old WAL from a segment belonging to a higher
	 * timeline.
	 */
	result->endOfLogTLI = xlogreader->seg.ws_tli;

	if (ArchiveRecoveryRequested)
	{
		/*
		 * We are no longer in archive recovery state.
		 *
		 * We are now done reading the old WAL.  Turn off archive fetching if
		 * it was active.
		 */
		Assert(InArchiveRecovery);
		InArchiveRecovery = false;

		/*
		 * If the ending log segment is still open, close it (to avoid
		 * problems on Windows with trying to rename or delete an open file).
		 */
		if (readFile >= 0)
		{
			close(readFile);
			readFile = -1;
		}
	}

	/*
	 * Copy the last partial block to the caller, for initializing the WAL
	 * buffer for appending new WAL.
	 */
	if (endOfLog % XLOG_BLCKSZ != 0)
	{
		char	   *page;
		int			len;
		XLogRecPtr	pageBeginPtr;

		pageBeginPtr = endOfLog - (endOfLog % XLOG_BLCKSZ);
		Assert(readOff == XLogSegmentOffset(pageBeginPtr, wal_segment_size));

		/* Copy the valid part of the last block */
		len = endOfLog % XLOG_BLCKSZ;
		page = palloc(len);
		memcpy(page, xlogreader->readBuf, len);

		result->lastPageBeginPtr = pageBeginPtr;
		result->lastPage = page;
	}
	else
	{
		/* There is no partial block to copy. */
		result->lastPageBeginPtr = endOfLog;
		result->lastPage = NULL;
	}

	/*
	 * Create a comment for the history file to explain why and where timeline
	 * changed.
	 */
	result->recoveryStopReason = getRecoveryStopReason();

	result->lastRec = lastRec;
	result->lastRecTLI = lastRecTLI;
	result->endOfLog = endOfLog;

	result->abortedRecPtr = abortedRecPtr;
	result->missingContrecPtr = missingContrecPtr;

	result->standby_signal_file_found = standby_signal_file_found;
	result->recovery_signal_file_found = recovery_signal_file_found;
#ifdef USE_PGRAC_CLUSTER
	cluster_recovery_cold_inputs_handoff(result);
#endif

	return result;
}

/*
 * Clean up the WAL reader and leftovers from restoring WAL from archive
 */
void
ShutdownWalRecovery(void)
{
	char		recoveryPath[MAXPGPATH];

	/* Final update of pg_stat_recovery_prefetch. */
	XLogPrefetcherComputeStats(xlogprefetcher);

	/* Shut down xlogreader */
	if (readFile >= 0)
	{
		close(readFile);
		readFile = -1;
	}
	XLogReaderFree(xlogreader);
	XLogPrefetcherFree(xlogprefetcher);

	if (ArchiveRecoveryRequested)
	{
		/*
		 * Since there might be a partial WAL segment named RECOVERYXLOG, get
		 * rid of it.
		 */
		snprintf(recoveryPath, MAXPGPATH, XLOGDIR "/RECOVERYXLOG");
		unlink(recoveryPath);	/* ignore any error */

		/* Get rid of any remaining recovered timeline-history file, too */
		snprintf(recoveryPath, MAXPGPATH, XLOGDIR "/RECOVERYHISTORY");
		unlink(recoveryPath);	/* ignore any error */
	}

	/*
	 * We don't need the latch anymore. It's not strictly necessary to disown
	 * it, but let's do it for the sake of tidiness.
	 */
	if (ArchiveRecoveryRequested)
		DisownLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
}

#ifdef USE_PGRAC_CLUSTER
/* The same readonly/claim/typed preparation is used by native replay and by
 * the earlier shared Startup control boundary. It creates no successor WAL
 * writer or control publication. */
static void
cluster_recovery_prepare_cold(ClusterMergeEngage *engage,
							 ClusterRecoveryFencePlan **fence_plan,
							 ClusterColdTypedV1 **typed)
{
	ClusterMergeEngage cluster_engage = CLUSTER_MERGE_NO_DISABLED;
	ClusterRecoveryFencePlan *cluster_fence_plan = NULL;
	ClusterColdTypedV1 *cluster_typed = NULL;

	/*
	 * RF-ROOT P4 S04-11: the sole cold readonly producer freezes the exact
	 * replay/proof-origin set and obtains all external admissions before the
	 * shared merge claim.  Native own-stream recovery still takes the same
	 * claim, but only after this readonly decision, so no foreign destructive
	 * path can cross the claim without the P4 plan.
	 */
	if (!ArchiveRecoveryRequested && !StandbyMode
		&& !cluster_backup_recovery_merge_required && !cluster_mrp_should_start())
	{
		cluster_engage = cluster_recovery_merge_preflight_readonly(
			cluster_wal_thread_id(), RedoStartLSN, &cluster_fence_plan);

		/*
		 * PGRAC: a cold crash that needs several WAL threads merged is
		 * recovered only by the shared profile's typed plan.  The preflight
		 * refused the unshared case before any fence admission; fail here,
		 * before the claim and before any replay.
		 */
		if (cluster_engage == CLUSTER_MERGE_REFUSE_UNSHARED)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("multi-node crash recovery requires cluster.shared_config"),
					 errdetail("Crashed peers' WAL threads must be merged, and this profile "
							   "cannot order their page versions."),
					 errhint("PRE2 supports crash recovery of several failed nodes only in "
							 "shared mode (cluster.shared_config = on). Preserve every "
							 "thread's WAL; single-node recovery is unaffected.")));

		/*
		 * PGRAC (S9P2-05): in the shared profile this node's own crashed
		 * generation is replayed only through the typed cold plan.  Its native
		 * stream alone could rewrite pages other generations changed later
		 * (their LSNs are not comparable), so a crash start the typed path does
		 * not engage stops here, before the claim and any replay.
		 */
		if (cluster_shared_config && cluster_engage != CLUSTER_MERGE_ENGAGE)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("crash recovery of this node is not supported without the shared "
							"cold path"),
					 errdetail("The cold merge decision was %d; only a cold start with crashed "
							   "peers is recovered by the typed plan in this build.",
							   (int) cluster_engage),
					 errhint("Preserve every thread's WAL; this node's sealed generation stays "
							 "sealed until it is recovered.")));
		cluster_recovery_merge_claim_acquire_blocking();
	}
	/*
	 * PGRAC: the shared profile replays every retained writer generation
	 * through the typed cold plan (pass 1 here, pass 2 below); there is no
	 * SCN-ordered legacy merge for crash recovery any more.
	 */
	switch (cluster_cold_route_v1(cluster_shared_config, cluster_engage == CLUSTER_MERGE_ENGAGE))
	{
		case CLUSTER_COLD_ROUTE_TYPED:
			cluster_typed = cluster_recovery_typed_begin(&cluster_fence_plan);
			break;
		case CLUSTER_COLD_ROUTE_REFUSE:
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("multi-node crash recovery requires cluster.shared_config")));
			break;
		case CLUSTER_COLD_ROUTE_NATIVE:
			break;
	}
	if (cluster_engage == CLUSTER_MERGE_ENGAGE)
		ereport(LOG, (errmsg("cluster merged recovery: engage decision PASSED")));
	else
		ereport(DEBUG1, (errmsg("cluster merged recovery: not engaged (reason %d)",
								(int) cluster_engage)));

	*engage = cluster_engage;
	*fence_plan = cluster_fence_plan;
	*typed = cluster_typed;
}

static bool
cluster_recovery_cold_preparation_context(void)
{
	return cluster_shared_config && AmStartupProcess() && InRecovery
		&& !ArchiveRecoveryRequested && !StandbyMode
		&& !cluster_backup_recovery_merge_required && !cluster_mrp_should_start()
		&& CurrentResourceOwner != NULL && MyProcPid > 0
		&& !XLogRecPtrIsInvalid(CheckPointLoc) && !XLogRecPtrIsInvalid(RedoStartLSN)
		&& CheckPointTLI != 0 && RedoStartTLI != 0;
}

/* Prepare once, before Startup's shared crash control site. The original
 * typed owner retains its source/retention/IR set until that exact plan is
 * consumed by replay; caller flags cannot substitute for its readiness. */
void
ClusterPrepareColdWalRecovery(void)
{
	ClusterColdStartupPreparationV1 *prepared = &cluster_recovery_cold_preparation;
	ClusterMergeEngage engage;
	ClusterRecoveryFencePlan *fence_plan = NULL;
	ClusterColdTypedV1 *typed = NULL;

	if (prepared->started || !cluster_recovery_cold_preparation_context())
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("shared cold recovery preparation has no current Startup input")));
	prepared->started = true;
	prepared->owner = CurrentResourceOwner;
	prepared->pid = MyProcPid;
	prepared->checkpoint = CheckPointLoc;
	prepared->redo = RedoStartLSN;
	prepared->checkpoint_tli = CheckPointTLI;
	prepared->redo_tli = RedoStartTLI;
	cluster_recovery_prepare_cold(&engage, &fence_plan, &typed);
	if (!cluster_recovery_cold_preparation_context()
		|| prepared->owner != CurrentResourceOwner || prepared->pid != MyProcPid
		|| prepared->checkpoint != CheckPointLoc || prepared->redo != RedoStartLSN
		|| prepared->checkpoint_tli != CheckPointTLI || prepared->redo_tli != RedoStartTLI
		|| engage != CLUSTER_MERGE_ENGAGE || fence_plan == NULL || typed == NULL)
		cluster_recovery_typed_fail(&fence_plan, &typed,
									"shared cold recovery preparation changed", NULL);
	prepared->engage = engage;
	prepared->fence = fence_plan;
	prepared->typed = typed;
	prepared->ready = true;
}

static void
cluster_recovery_take_cold_preparation(ClusterMergeEngage *engage,
									 ClusterRecoveryFencePlan **fence_plan,
									 ClusterColdTypedV1 **typed)
{
	ClusterColdStartupPreparationV1 *prepared = &cluster_recovery_cold_preparation;

	if (!prepared->started || !prepared->ready
		|| !cluster_recovery_cold_preparation_context()
		|| prepared->owner != CurrentResourceOwner || prepared->pid != MyProcPid
		|| prepared->checkpoint != CheckPointLoc || prepared->redo != RedoStartLSN
		|| prepared->checkpoint_tli != CheckPointTLI || prepared->redo_tli != RedoStartTLI
		|| prepared->engage != CLUSTER_MERGE_ENGAGE
		|| prepared->fence == NULL || prepared->typed == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("shared cold replay has no exact prepared Startup input")));
	*engage = prepared->engage;
	*fence_plan = prepared->fence;
	*typed = prepared->typed;
	prepared->ready = false;
	prepared->fence = NULL;
	prepared->typed = NULL;
}
#endif

/*
 * Perform WAL recovery.
 *
 * If the system was shut down cleanly, this is never called.
 */
void
PerformWalRecovery(void)
{
	XLogRecord *record;
	bool		reachedRecoveryTarget = false;
	TimeLineID	replayTLI;
#ifdef USE_PGRAC_CLUSTER
	ClusterMergeEngage cluster_engage = CLUSTER_MERGE_NO_DISABLED;
	ClusterRecoveryFencePlan *cluster_fence_plan = NULL;
	ClusterColdTypedV1 *cluster_typed = NULL;
#endif

	/*
	 * Initialize shared variables for tracking progress of WAL replay, as if
	 * we had just replayed the record before the REDO location (or the
	 * checkpoint record itself, if it's a shutdown checkpoint).
	 */
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	if (RedoStartLSN < CheckPointLoc)
	{
		XLogRecoveryCtl->lastReplayedReadRecPtr = InvalidXLogRecPtr;
		XLogRecoveryCtl->lastReplayedEndRecPtr = RedoStartLSN;
		XLogRecoveryCtl->lastReplayedTLI = RedoStartTLI;
	}
	else
	{
		XLogRecoveryCtl->lastReplayedReadRecPtr = xlogreader->ReadRecPtr;
		XLogRecoveryCtl->lastReplayedEndRecPtr = xlogreader->EndRecPtr;
		XLogRecoveryCtl->lastReplayedTLI = CheckPointTLI;
	}
	XLogRecoveryCtl->replayEndRecPtr = XLogRecoveryCtl->lastReplayedEndRecPtr;
	XLogRecoveryCtl->replayEndTLI = XLogRecoveryCtl->lastReplayedTLI;
	XLogRecoveryCtl->recoveryLastXTime = 0;
	XLogRecoveryCtl->currentChunkStartTime = 0;
	XLogRecoveryCtl->recoveryPauseState = RECOVERY_NOT_PAUSED;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	/* Also ensure XLogReceiptTime has a sane value */
	XLogReceiptTime = GetCurrentTimestamp();

	/*
	 * Let postmaster know we've started redo now, so that it can launch the
	 * archiver if necessary.
	 */
	if (IsUnderPostmaster)
		SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED);

	/*
	 * Allow read-only connections immediately if we're consistent already.
	 */
	CheckRecoveryConsistency();

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config || cluster_recovery_cold_preparation.started)
		cluster_recovery_take_cold_preparation(&cluster_engage, &cluster_fence_plan,
											 &cluster_typed);
	else
		cluster_recovery_prepare_cold(&cluster_engage, &cluster_fence_plan, &cluster_typed);

	/*
	 * RF A1 W6: merge_recovered_lsn is a retained on-disk compatibility
	 * field, not replay authority.  Surface a historical raw value for
	 * diagnosis, but never derive a skip bound from it: this node always
	 * replays its retained own WAL.
	 */
	if (cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0') {
		uint16		own_tid = cluster_wal_thread_id();
		ClusterWalStateSlot own_slot;

		if (own_tid != XLP_THREAD_ID_LEGACY
			&& cluster_wal_state_read_slot(own_tid, &own_slot) == CLUSTER_WAL_SLOT_OK)
		{
			if (own_slot.merge_recovered_lsn > 0)
				ereport(LOG,
						(errmsg("cluster recovery: historical merge_recovered_lsn %X/%X "
								"is raw_ignored; retained own WAL will be replayed",
								LSN_FORMAT_ARGS((XLogRecPtr) own_slot.merge_recovered_lsn))));
		}
	}

	/*
	 * PGRAC (spec-5.7 D3 §3.1b R3/R6): load this node's HW relation-extend
	 * authority snapshot into the authority table BEFORE redo replays the
	 * HW_RESERVE tail on top, so the rebuilt HWM = max(snapshot, tail) and a
	 * surviving extender's already-replied reservation is never re-handed (no
	 * double-allocation).  A missing snapshot rebuilds from the WAL tail alone;
	 * a present-but-corrupt / foreign one FATALs (R6).  Self-gates to a no-op
	 * unless the HW authority is active (multi-node + shared storage).
	 */
	cluster_hw_snapshot_recovery_load();
#endif

	/*
	 * Find the first record that logically follows the checkpoint --- it
	 * might physically precede it, though.
	 */
	if (RedoStartLSN < CheckPointLoc)
	{
		/* back up to find the record */
		replayTLI = RedoStartTLI;
		XLogPrefetcherBeginRead(xlogprefetcher, RedoStartLSN);
		record = ReadRecord(xlogprefetcher, PANIC, false, replayTLI);
	}
	else
	{
		/* just have to read next record after CheckPoint */
		Assert(xlogreader->ReadRecPtr == CheckPointLoc);
		replayTLI = CheckPointTLI;
		record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
	}

	/*
	 * PGRAC spec-4.5a: enter the redo phase whenever merged recovery
	 * engaged, EVEN IF this node's own thread has no record after its
	 * checkpoint (record == NULL).  A surviving node recovering a crashed
	 * peer must still drive the k-way merge to replay the PEER's committed
	 * WAL; gating solely on the own thread's first record would silently
	 * skip the merge and let the node start up without the peer's data.
	 * The merged path (below) uses the typed plan's starts, not `record`.
	 */
	if (record != NULL
#ifdef USE_PGRAC_CLUSTER
		|| cluster_engage == CLUSTER_MERGE_ENGAGE
		|| cluster_backup_recovery_merge_required
		|| cluster_mrp_should_start()
#endif
	)
	{
		TimestampTz xtime;
		PGRUsage	ru0;

		pg_rusage_init(&ru0);

		InRedo = true;

		RmgrStartup();

		ereport(LOG,
				(errmsg("redo starts at %X/%X",
						LSN_FORMAT_ARGS(xlogreader->ReadRecPtr))));

		/* Prepare to report progress of the redo phase. */
		if (!StandbyMode)
			begin_startup_progress_phase();

#ifdef USE_PGRAC_CLUSTER
		if (cluster_backup_recovery_merge_required)
		{
			XLogRecPtr	merged_end;
			uint16		own_thread = cluster_wal_thread_id();
			uint16		foreign_thread = 0;

			if (own_thread == XLP_THREAD_ID_LEGACY)
				own_thread = 1;
			if (cluster_recovery_restore_first_foreign(
					cluster_backup_recovery_merge_bitmap, own_thread,
					&foreign_thread))
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
						 errmsg("external write exclusion is not proven for node %d incarnation "
								UINT64_FORMAT, (int)foreign_thread - 1, (uint64) 0),
						 errdetail("The backup manifest contains a foreign WAL thread but has no "
								   "canonical recovery duty or uint64 victim incarnation."),
						 errhint("Restore the configured root fencing provider and retry; do not "
								 "disable the fence or force recovery.")));
			merged_end = cluster_recovery_merged_replay(cluster_backup_recovery_merge_bitmap,
													cluster_backup_recovery_merge_start,
													cluster_backup_recovery_merge_stop,
													cluster_backup_recovery_wal_root,
													replayTLI, own_thread, &replayTLI,
													true, NULL);
			reachedRecoveryTarget = true;
			if (cluster_backup_recovery_target_active && !reachedConsistency)
				ereport(FATAL,
						(errmsg("requested cluster PITR stop point is before consistent recovery "
								"point")));
			if (cluster_backup_recovery_target_active)
			{
				switch (recoveryTargetAction)
				{
					case RECOVERY_TARGET_ACTION_SHUTDOWN:
						proc_exit(3);
					case RECOVERY_TARGET_ACTION_PAUSE:
						SetRecoveryPause(true);
						recoveryPausesHere(true);
						/* drop into promote */
					case RECOVERY_TARGET_ACTION_PROMOTE:
						break;
				}
			}
			RmgrCleanup();
			ereport(LOG, (errmsg("redo done (cluster backup restore) at %X/%X system usage: %s",
								 LSN_FORMAT_ARGS(merged_end), pg_rusage_show(&ru0))));
			InRedo = false;
			goto cluster_merged_redo_done;
		}

		if (cluster_mrp_should_start())
		{
			XLogRecPtr	adg_end;

			if (ArchiveRecoveryRequested && recoveryTarget != RECOVERY_TARGET_UNSET)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("ADG streaming standby does not support configured recovery targets")));

			adg_end = cluster_adg_streaming_replay(xlogprefetcher, xlogreader, record,
												   replayTLI, &replayTLI,
												   &reachedRecoveryTarget);
			RmgrCleanup();
			ereport(LOG, (errmsg("redo done (ADG streaming) at %X/%X system usage: %s",
								 LSN_FORMAT_ARGS(adg_end), pg_rusage_show(&ru0))));
			InRedo = false;
			goto cluster_merged_redo_done;
		}

		/*
		 * PGRAC spec-4.5 §3.3: when merged recovery engaged, drive the
		 * whole redo from the k-way SCN merge engine instead of the
		 * single-stream loop below.  (Unreachable in this build -- the
		 * capability gate FATALs first; live once 4.5a lands.)
		 */
		if (cluster_engage == CLUSTER_MERGE_ENGAGE)
		{
			XLogRecPtr	merged_end;

			merged_end = cluster_recovery_typed_replay(&cluster_typed, &replayTLI,
													   &cluster_fence_plan);
			RmgrCleanup();
			ereport(LOG, (errmsg("redo done (merged) at %X/%X system usage: %s",
								 LSN_FORMAT_ARGS(merged_end), pg_rusage_show(&ru0))));
			InRedo = false;
			goto cluster_merged_redo_done;
		}
#endif

		/*
		 * main redo apply loop
		 */
		do
		{
			if (!StandbyMode)
				ereport_startup_progress("redo in progress, elapsed time: %ld.%02d s, current LSN: %X/%X",
										 LSN_FORMAT_ARGS(xlogreader->ReadRecPtr));

#ifdef WAL_DEBUG
			if (XLOG_DEBUG ||
				(record->xl_rmid == RM_XACT_ID && trace_recovery_messages <= DEBUG2) ||
				(record->xl_rmid != RM_XACT_ID && trace_recovery_messages <= DEBUG3))
			{
				StringInfoData buf;

				initStringInfo(&buf);
				appendStringInfo(&buf, "REDO @ %X/%X; LSN %X/%X: ",
								 LSN_FORMAT_ARGS(xlogreader->ReadRecPtr),
								 LSN_FORMAT_ARGS(xlogreader->EndRecPtr));
				xlog_outrec(&buf, xlogreader);
				appendStringInfoString(&buf, " - ");
				xlog_outdesc(&buf, xlogreader);
				elog(LOG, "%s", buf.data);
				pfree(buf.data);
			}
#endif

			/* Handle interrupt signals of startup process */
			HandleStartupProcInterrupts();

			/*
			 * Pause WAL replay, if requested by a hot-standby session via
			 * SetRecoveryPause().
			 *
			 * Note that we intentionally don't take the info_lck spinlock
			 * here.  We might therefore read a slightly stale value of the
			 * recoveryPause flag, but it can't be very stale (no worse than
			 * the last spinlock we did acquire).  Since a pause request is a
			 * pretty asynchronous thing anyway, possibly responding to it one
			 * WAL record later than we otherwise would is a minor issue, so
			 * it doesn't seem worth adding another spinlock cycle to prevent
			 * that.
			 */
			if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
				RECOVERY_NOT_PAUSED)
				recoveryPausesHere(false);

			/*
			 * Have we reached our recovery target?
			 */
			if (recoveryStopsBefore(xlogreader))
			{
				reachedRecoveryTarget = true;
				break;
			}

			/*
			 * If we've been asked to lag the primary, wait on latch until
			 * enough time has passed.
			 */
			if (recoveryApplyDelay(xlogreader))
			{
				/*
				 * We test for paused recovery again here. If user sets
				 * delayed apply, it may be because they expect to pause
				 * recovery in case of problems, so we must test again here
				 * otherwise pausing during the delay-wait wouldn't work.
				 */
				if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
					RECOVERY_NOT_PAUSED)
					recoveryPausesHere(false);
			}

			/*
			 * Apply the record
			 */
			ApplyWalRecord(xlogreader, record, &replayTLI);

			/* Exit loop if we reached inclusive recovery target */
			if (recoveryStopsAfter(xlogreader))
			{
				reachedRecoveryTarget = true;
				break;
			}

			/* Else, try to fetch the next WAL record */
			record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
		} while (record != NULL);

		/*
		 * end of main redo apply loop
		 */

		if (reachedRecoveryTarget)
		{
			if (!reachedConsistency)
				ereport(FATAL,
						(errmsg("requested recovery stop point is before consistent recovery point")));

			/*
			 * This is the last point where we can restart recovery with a new
			 * recovery target, if we shutdown and begin again. After this,
			 * Resource Managers may choose to do permanent corrective actions
			 * at end of recovery.
			 */
			switch (recoveryTargetAction)
			{
				case RECOVERY_TARGET_ACTION_SHUTDOWN:

					/*
					 * exit with special return code to request shutdown of
					 * postmaster.  Log messages issued from postmaster.
					 */
					proc_exit(3);

				case RECOVERY_TARGET_ACTION_PAUSE:
					SetRecoveryPause(true);
					recoveryPausesHere(true);

					/* drop into promote */

				case RECOVERY_TARGET_ACTION_PROMOTE:
					break;
			}
		}

		RmgrCleanup();

		ereport(LOG,
				(errmsg("redo done at %X/%X system usage: %s",
						LSN_FORMAT_ARGS(xlogreader->ReadRecPtr),
						pg_rusage_show(&ru0))));
		xtime = GetLatestXTime();
		if (xtime)
			ereport(LOG,
					(errmsg("last completed transaction was at log time %s",
							timestamptz_to_str(xtime))));

		InRedo = false;
#ifdef USE_PGRAC_CLUSTER
cluster_merged_redo_done:;
#endif
	}
	else
	{
		/* there are no WAL records following the checkpoint */
		ereport(LOG,
				(errmsg("redo is not required")));
	}

	/*
	 * This check is intentionally after the above log messages that indicate
	 * how far recovery went.
	 */
	if (ArchiveRecoveryRequested &&
		recoveryTarget != RECOVERY_TARGET_UNSET &&
		!reachedRecoveryTarget)
		ereport(FATAL,
				(errmsg("recovery ended before configured recovery target was reached")));
}

#ifdef USE_PGRAC_CLUSTER
/*
 * cluster_recovery_merged_replay -- spec-4.5 §3.3 merged replay loop.
 *
 *	Drives the whole redo (own thread + crash candidates) from the
 *	k-way SCN merge engine instead of the single-stream ReadRecord
 *	loop.  Each popped record is classified by the §3.3e G/S/L/U
 *	matrix: the own thread applies everything; a foreign candidate
 *	stream applies G (global logical state -- the peer's commit bits)
 *	and S (shared storage pages), SKIPS L (node-local, would clobber
 *	this node's own files), and FATALs 53RA3 on U.
 *
 *	recovery-progress globals track the OWN thread only -- foreign
 *	streams are applied but do not extend this node's WAL (the gate
 *	already excludes archive/standby, the single-LSN-space consumers).
 *	Per-candidate serving authority is node-local; the registry's retained
 *	merge_recovered_lsn compatibility bytes are never read or written as
 *	recovery authority.
 */
static XLogRecPtr
cluster_recovery_merged_replay(const uint64 *bitmap, const XLogRecPtr *start,
							   const XLogRecPtr *stop, const char *wal_root, TimeLineID tli,
							   uint16 own_thread, TimeLineID *replayTLI, bool restore_mode,
							   ClusterRecoveryFencePlan **fence_plan)
{
	XLogRecPtr own_end = InvalidXLogRecPtr;
	XLogRecPtr own_read = InvalidXLogRecPtr;
	ClusterRecoveryMergeState *st;
	XLogReaderState *r;
	uint16 thread;
	uint64 max_recovered[CLUSTER_WAL_STATE_SLOT_COUNT + 1];
	uint16 t;
	bool foreign_mutation_started = false;

	memset(max_recovered, 0, sizeof(max_recovered));
	if (restore_mode)
		st = cluster_recovery_merge_begin_restore(bitmap, start, stop, wal_root, own_thread, tli);
	else
		st = cluster_recovery_merge_begin(bitmap, start, own_thread, tli);
	cluster_recovery_merge_window_enter();
	/* A foreign record's page is stamped with this node's own recovery redo
	 * (start[own_thread]) instead of the peer's incomparable LSN (§3.3b). */
	cluster_recovery_merge_set_own_lsn((uint64)start[own_thread]);

	while ((r = cluster_recovery_merge_next(st, &thread, NULL)) != NULL) {
		XLogRecord *rec = &r->record->header;
		bool is_own = (thread == own_thread);
		ClusterRecoveryRecordClass cls = cluster_record_apply_class(r);

		if (!is_own) {
			if (cls == CLUSTER_RECMERGE_LOCAL) {
				cluster_vis_bump_merged_skipped_local(); /* D11 */
				goto advance;							 /* node-local foreign record: skip */
			}
			if (cls == CLUSTER_RECMERGE_UNCLASSIFIABLE)
				ereport(FATAL, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
								errmsg("merged recovery hit an unclassifiable record on thread %u "
									   "(rmgr %u)",
									   (unsigned)thread, (unsigned)rec->xl_rmid),
								errhint("This foreign record cannot be safely routed; resolve the "
										"shared storage or recover single-stream "
								 "(cluster.merged_recovery=off).")));
			if (!foreign_mutation_started)
			{
				if (restore_mode || fence_plan == NULL || *fence_plan == NULL ||
					!cluster_recovery_merge_fence_plan_revalidate_nowait(
						*fence_plan))
				{
					cluster_write_fence_note_external_mutation_gate_blocked();
					ereport(FATAL,
							(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
							 errmsg("external write exclusion became stale before cold replay"),
								 errhint("Retry startup so every foreign origin is fenced again; "
									 "do not force partial recovery.")));
				}
				foreign_mutation_started = true;
			}
			/* G, S or MATERIALIZE_LOCAL: apply the peer's record.
			 * (MATERIALIZE_LOCAL = RM_CLUSTER_UNDO: its redo handler
			 * resolves pg_undo/instance_<owner> from the record itself,
			 * materializing the peer's undo locally -- spec-4.5a P0-3.)
			 *
			 * spec-4.5a D10b (F1): a foreign XACT/CLOG record must NOT go
			 * through ApplyWalRecord -- xact_redo_commit writes the LOCAL
			 * pg_xact by raw xid, which aliases across instances and would
			 * corrupt this node's own visibility.  Divert it into the
			 * per-origin pg_xact_remote store (fail-closed on any
			 * cross-instance side effect, P1-1) and consume the record.
			 *
			 * MULTIXACT and COMMIT_TS are the same pollution class (their
			 * redo writes the LOCAL pg_multixact/pg_commit_ts by raw xid)
			 * but carry state this store cannot absorb -- the divert FATALs
			 * 53RA3 on them rather than letting them alias.  RM_STANDBY
			 * stays on ApplyWalRecord: merged replay only runs in crash
			 * recovery, where standbyState == STANDBY_DISABLED makes
			 * standby_redo a no-op. */
			if (rec->xl_rmid == RM_XACT_ID || rec->xl_rmid == RM_CLOG_ID ||
				rec->xl_rmid == RM_MULTIXACT_ID || rec->xl_rmid == RM_COMMIT_TS_ID) {
				cluster_recovery_merge_set_scn(rec->xl_scn);
				/* online=false: cold startup merged replay keeps the FATAL
				 * fail-closed (spec-4.11 3b-2 R13); the online survivor path
				 * passes true so an unmaterializable record is demotable. */
				cluster_remote_xact_apply((int)thread - 1, r, false);
				cluster_vis_bump_merged_records_applied(); /* D11 */
				goto advance;
			}
		}

		cluster_recovery_merge_set_scn(rec->xl_scn);
		cluster_recovery_merge_set_apply_foreign(!is_own);
		ApplyWalRecord(r, rec, replayTLI);
		cluster_recovery_merge_set_apply_foreign(false);
		if (is_own) {
			own_end = r->EndRecPtr;
			own_read = r->ReadRecPtr;
		} else
			cluster_vis_bump_merged_records_applied(); /* D11 */

	advance:
		if (!is_own && r->EndRecPtr > max_recovered[thread])
			max_recovered[thread] = r->EndRecPtr;
	}

	cluster_recovery_merge_window_leave();
	cluster_recovery_merge_end(st);

	/*
	 * spec-4.5a: restore the recovery position to the OWN thread's last
	 * applied record.  ApplyWalRecord updated lastReplayedReadRecPtr for the
	 * foreign records too, but their ReadRecPtr is in the PEER's WAL space --
	 * FinishWalRecovery re-reads lastReplayedReadRecPtr through THIS node's
	 * own xlogreader to determine end-of-log, and a foreign position there
	 * decodes as garbage (invalid magic) and PANICs.  Own records define this
	 * node's WAL extent; pin the recovery ctl to the own last record.
	 */
	if (own_read != InvalidXLogRecPtr) {
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		XLogRecoveryCtl->lastReplayedReadRecPtr = own_read;
		XLogRecoveryCtl->lastReplayedEndRecPtr = own_end;
		XLogRecoveryCtl->lastReplayedTLI = *replayTLI;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);
	}

	/* spec-4.5a G5: persist the per-origin outcomes materialized above so
	 * backends can read them after recovery finishes. */
	cluster_remote_xact_flush();
	if (fence_plan != NULL && *fence_plan != NULL &&
		!cluster_recovery_merge_fence_plan_revalidate_nowait(*fence_plan))
	{
		cluster_write_fence_note_external_publish_gate_blocked();
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
				 errmsg("external write exclusion became stale before recovery authority publish"),
				 errhint("Retry startup; replayed foreign state remains unpublished until the "
						 "complete fence plan is current.")));
	}

	/*
	 * Publish the node-local reader-authority marker.  It proves THIS restored
	 * pgdata materialized the peer's undo + outcome store, so restore-mode needs
	 * it just like crash merged recovery or post-open reads fail closed.  The
	 * registry merge_recovered_lsn compatibility field is deliberately untouched.
	 */
	for (t = 1; t <= CLUSTER_WAL_STATE_SLOT_COUNT; t++) {
		if (t == own_thread)
			continue;
		if ((bitmap[(t - 1) / 64] & ((uint64)1 << ((t - 1) % 64))) == 0)
			continue;
		if (max_recovered[t] > 0)
			cluster_merged_authority_publish((int)t - 1, max_recovered[t]);
	}
	if (fence_plan != NULL && *fence_plan != NULL)
	{
		if (!cluster_recovery_merge_fence_plan_release_serial(*fence_plan))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("cold recovery serialization release was not confirmed")));
		cluster_recovery_merge_fence_plan_destroy(fence_plan);
	}
	ereport(LOG, (errmsg("cluster %srecovery: replay complete (own thread %u)",
						 restore_mode ? "backup restore " : "merged ",
						 (unsigned)own_thread)));
	return own_end;
}

/*
 * Consumers this cold driver calls (cluster_cold_typed_ready_v1).  The
 * per-block redo consultation is provided by xlogutils.h, while xlog.c owns
 * the restartpoint hold for the typed replay window.  Pass 1 takes every
 * generation with retained WAL from the participant census, and fenced
 * generations are published recovered after replay.  The typed side-record
 * owner is already called by cluster_typed_replay_unscheduled() for admitted
 * side routes; exact route/source/authority/proof gates still decide whether
 * a plan may proceed, and unknown or incomplete side plans remain fail-closed.
 */
static const ClusterColdHandshakeV1 cluster_cold_handshake = {
#ifdef CLUSTER_COLD_REDO_HOOK_CONSUMER_V1
	.redo_block_hook = true,
#else
	.redo_block_hook = false,
#endif
	.participant_census = true,
	/* cluster_cold_typed_side_record_v1 is the existing pass-2 owner for
	 * every side route admitted by the sealed plan. */
	.side_owners = true,
	.completion_publish = true,
	/* xlog.c rejects own-thread restartpoint publication while the typed
	 * replay window is active; this is the R-A9 consumer. */
	.restartpoint_hold = true,
};

/*
 * cluster_recovery_typed_fail -- release cold serialization, then fail.
 *
 *	Releasing the serial set also releases the retention pin; neither may
 *	outlive a refused startup.  The detail is copied out of the typed plan's
 *	memory context before that context is deleted.
 */
static void
pg_attribute_noreturn()
cluster_recovery_typed_fail(ClusterRecoveryFencePlan **fence_plan, ClusterColdTypedV1 **typed,
							const char *message, const char *detail)
{
	char	   *detail_copy = detail != NULL ? pstrdup(detail) : NULL;
	const char *hint = cluster_cold_refusal_hint_v1(*typed != NULL ? (*typed)->refusal
													: CLUSTER_COLD_OK);
	bool		released = true;

	if (fence_plan != NULL && *fence_plan != NULL)
	{
		released = cluster_recovery_merge_fence_plan_release_serial(*fence_plan);
		if (released)
			cluster_recovery_merge_fence_plan_destroy(fence_plan);
	}
	cluster_cold_typed_destroy_v1(typed);
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("%s", message),
			 detail_copy != NULL ? errdetail("%s", detail_copy) : 0,
			 !released ? errdetail_log("Cold recovery serialization release was not confirmed.") : 0,
			 errhint("%s", hint)));
	pg_unreachable();
}

/*
 * cluster_recovery_typed_begin -- typed cold replay pass 1.
 *
 *	With every foreign origin's external admission held and before the
 *	serial set (retention pin, then IR) is taken, run pass 1: scan every
 *	retained cut, observe DATA and seal the plan.  These reads are
 *	provisional.  Pass 2 starts only if every consumer it needs is wired
 *	(cluster_cold_typed_ready_v1); otherwise startup fails here, before the
 *	serial set.  The serial set then retains the same input and the
 *	unchanged ROOT tokens are recommitted; pass 2 re-reads that input,
 *	matching each scheduled page record's identity, checking that other
 *	records carry no page versions or refused flags, and checking every
 *	participant's record count.  Nothing is mutated before this returns;
 *	every refusal releases what was taken.
 */
static ClusterColdTypedV1 *
cluster_recovery_typed_begin(ClusterRecoveryFencePlan **fence_plan)
{
	ClusterColdTypedV1 *typed = NULL;
	ClusterColdHandshakeV1 handshake = cluster_cold_handshake;
	bool		serial_acquired;
	bool		plan_committed = false;
	char readiness[1152];

	typed = cluster_cold_typed_prepare_v1(*fence_plan, cluster_wal_thread_id(), RedoStartLSN);
	if (typed->refusal != CLUSTER_COLD_OK)
		cluster_recovery_typed_fail(fence_plan, &typed, "typed cold recovery refused",
									typed->refusal_detail);
	/* The cold SPACE install exists once its owner publishes it. */
	handshake.space_owner = cluster_cold_space_owner_v1();
	if (!cluster_cold_typed_ready_v1(typed, &handshake, readiness, sizeof(readiness)))
		cluster_recovery_typed_fail(fence_plan, &typed,
									"typed cold recovery cannot replay this plan yet", readiness);
	/* Recheck/release the census, then retain all physical input threads
	 * before taking the unchanged foreign-origin IR set. */
	serial_acquired = cluster_cold_typed_acquire_serial_v1(typed, *fence_plan);
	if (serial_acquired)
		plan_committed = cluster_recovery_merge_commit_plan_nowait(*fence_plan);
	if (!serial_acquired || !plan_committed)
	{
		if (serial_acquired)
			cluster_write_fence_note_external_mutation_gate_blocked();
		snprintf(
			readiness, sizeof(readiness),
			"serial_acquired=%d commit_attempted=%d plan_committed=%d typed_refusal=%d; %s; %s",
			serial_acquired, serial_acquired, plan_committed, (int)typed->refusal,
			typed->refusal_detail, cluster_recovery_merge_fence_plan_refusal_v1(*fence_plan));
		cluster_recovery_typed_fail(fence_plan, &typed,
									"cold recovery fence plan changed before replay", readiness);
	}
	ereport(LOG,
			(errmsg("cluster typed cold recovery: plan sealed for %u writer generations",
					typed->participant_count),
			 errdetail_log(UINT64_FORMAT " records scanned, %u replay steps, " UINT64_FORMAT
						   " pages observed, " UINT64_FORMAT " unreadable.",
						   typed->scanned_records, cluster_cold_plan_step_count_v1(typed->plan),
						   typed->observer.pages_observed, typed->observer.pages_invalid)));
	return typed;
}

/* Pass-2 adapter: native readers and redo for the pure sequencer. */
typedef struct ClusterColdTypedReplay
{
	ClusterColdTypedV1 *typed;
	ClusterRecoveryFencePlan **fence_plan;
	TimeLineID *replayTLI;
	ClusterColdReaderV1 *readers[CLUSTER_COLD_MAX_PARTICIPANTS];
	XLogReaderState *current[CLUSTER_COLD_MAX_PARTICIPANTS];
	char	   *errormsg[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterColdXidProgressV1 xid_progress[CLUSTER_COLD_MAX_PARTICIPANTS];
	bool		foreign_mutation_started;
	bool		apply_foreign;	/* the record being applied is another generation's */
	ClusterColdEffectsProofV1 effects;	/* complete pass-2 ordinary-effect coverage */
	ClusterColdDecodedV1 decoded;	/* scratch for unscheduled records */
	ClusterColdTouchedV1 touched;	/* files pass 2 changed, made durable before completion */
} ClusterColdTypedReplay;

static bool
cluster_typed_replay_xid_begin(ClusterColdTypedReplay *rep)
{
	ClusterColdTypedV1 *typed = rep->typed;

	memset(&typed->xid_receipt, 0, sizeof(typed->xid_receipt));
	memset(&typed->effects_receipt, 0, sizeof(typed->effects_receipt));
	memset(rep->xid_progress, 0, sizeof(rep->xid_progress));
	memset(typed->xid_next_at_drain, 0, sizeof(typed->xid_next_at_drain));
	typed->xid_next_after_redo = 0;
	if (typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count)
		return false;
	for (uint32 i = 0; i < typed->participant_count; i++)
	{
		ClusterControlRootSnapshot root;
		ClusterWalRecoveryCheckpointV1 checkpoint;
		if (!cluster_cold_typed_checkpoint_v1(typed, i, &root, &checkpoint))
			return false;
		/* History normally has its native replay cut at the tail. Its
		 * checkpoint seed alone cannot stand in for the omitted records. */
		if (checkpoint.checkpoint.redo != typed->participants[i].native_redo)
		{
			if (i < typed->replay_count)
				return false;
			continue;
		}
		if (!cluster_cold_xid_begin_v1(&rep->xid_progress[i], &typed->participants[i],
									 U64FromFullTransactionId(checkpoint.checkpoint.nextXid),
									 cluster_cold_plan_replay_record_count_v1(typed->plan, i)))
			return false;
	}
	return true;
}

static void
pg_attribute_noreturn()
cluster_typed_replay_mismatch(ClusterColdTypedReplay *rep, uint32 participant,
							  XLogRecPtr read_rec_ptr, const char *what)
{
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("typed cold recovery input changed between passes"),
			 errdetail("Thread %u record %X/%X: %s.",
					   (unsigned) rep->typed->participants[participant].thread_id,
					   LSN_FORMAT_ARGS(read_rec_ptr), what),
			 errhint("Preserve all original thread WAL; the retained input must not change "
					 "while cold recovery holds it.")));
	pg_unreachable();
}

/* The first foreign mutation revalidates the complete fence plan. */
static void
cluster_typed_replay_foreign_gate(ClusterColdTypedReplay *rep)
{
	if (rep->foreign_mutation_started)
		return;
	if (rep->fence_plan == NULL || *rep->fence_plan == NULL ||
		!cluster_recovery_merge_fence_plan_revalidate_nowait(*rep->fence_plan))
	{
		cluster_write_fence_note_external_mutation_gate_blocked();
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
				 errmsg("external write exclusion became stale before cold replay"),
				 errhint("Retry startup so every foreign origin is fenced again; do not force "
						 "partial recovery.")));
	}
	rep->foreign_mutation_started = true;
}

/* Native redo of the current record; the caller sets apply_foreign. */
static void
cluster_typed_replay_apply_native(XLogReaderState *r, void *arg)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;

	cluster_recovery_merge_set_scn(r->record->header.xl_scn);
	cluster_recovery_merge_set_apply_foreign(rep->apply_foreign);
	cluster_cold_touched_add_record_v1(&rep->touched, r);
	ApplyWalRecord(r, &r->record->header, rep->replayTLI);
	cluster_recovery_merge_set_apply_foreign(false);
}

static void
cluster_typed_replay_apply(XLogReaderState *r, void *arg)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;

	if (!rep->apply_foreign && XLogRecGetRmid(r) == RM_CLOG_ID) {
		if (rep->fence_plan == NULL
			|| !cluster_cold_typed_clog_native_v1(rep->typed, *rep->fence_plan, r,
				cluster_typed_replay_apply_native, arg))
			cluster_typed_replay_mismatch(rep, rep->typed->own_participant, r->ReadRecPtr,
				"the original native CLOG consumer lost its source or record");
	} else
		cluster_typed_replay_apply_native(r, arg);
}

static bool
cluster_typed_replay_next(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;
	XLogReaderState *r;

	HandleStartupProcInterrupts();
	rep->current[participant] = NULL;
	r = cluster_cold_reader_next_v1(rep->readers[participant], &rep->errormsg[participant]);
	if (r == NULL)
		return false;
	rep->current[participant] = r;
	out->read_rec_ptr = r->ReadRecPtr;
	out->end_rec_ptr = r->EndRecPtr;
	out->record_crc = (uint32) r->record->header.xl_crc;
	out->rmid = r->record->header.xl_rmid;
	out->info = r->record->header.xl_info;
	/* Fold the original reader's ordered inputs even for records whose
	 * pages will be skipped. This is provisional until consumption and
	 * the original source owner's counted drain both succeed. */
	if (rep->xid_progress[participant].begun)
	{
		ClusterColdTypedV1 *typed = rep->typed;
		bool is_own = participant == typed->own_participant;

		if (cluster_cold_recovery_decode_v1(r, typed->system_identifier,
											typed->sources[participant].claim.identity.storage_uuid,
											false, !is_own, &rep->decoded) != CLUSTER_COLD_OK)
		{
			rep->xid_progress[participant].failed = true;
			rep->xid_progress[participant].next_xid = 0;
			return false;
		}
		if (!cluster_cold_xid_record_v1(&rep->xid_progress[participant],
									  &typed->participants[participant], &rep->decoded.record,
									  &rep->decoded.xid_input))
			return false;
	}
	return true;
}

/*
 * A record without a scheduled step.  Pass 1 refused every lifecycle,
 * prepared-transaction and unowned side record after native redo, so what
 * reaches here is the founder's own record (native owner), another
 * generation's rebuildable FSM-only page record, relation file creation or
 * native control record (no effect on the founder), an explicitly owned
 * record-local native SIDE effect, or a SPACE change the
 * SPACE owner installs later or the SPACE pages on disk already cover (never
 * replayed natively); anything else means pass 1 saw other input
 * (cluster_cold_unscheduled_v1).
 */
static bool
cluster_typed_replay_unscheduled(void *arg, uint32 participant)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;
	ClusterColdTypedV1 *typed = rep->typed;
	const ClusterWalSourceRef *source = &typed->sources[participant];
	bool		is_own = participant == typed->own_participant;
	XLogReaderState *r = rep->current[participant];

	if (cluster_cold_recovery_decode_v1(r, typed->system_identifier,
										source->claim.identity.storage_uuid, false, !is_own,
										&rep->decoded) != CLUSTER_COLD_OK)
		cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
									  "an unscheduled record no longer decodes");
	if (rep->effects.invalid || !cluster_cold_replay_effects_v1(&rep->decoded, NULL, is_own))
		cluster_typed_replay_mismatch(
			rep, participant, r->ReadRecPtr,
			"an unscheduled record has effects without a replay consumer");
	switch (cluster_cold_unscheduled_v1(&rep->decoded.record))
	{
		case CLUSTER_COLD_UNSCHEDULED_REFUSE:
			cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
										  "a record pass 1 would have refused or scheduled "
										  "reached replay unscheduled");
			break;
		case CLUSTER_COLD_UNSCHEDULED_SPACE_SKIP:
			if (is_own)
				AdvanceNextFullTransactionIdPastXid(r->record->header.xl_xid);
			break;
		case CLUSTER_COLD_UNSCHEDULED_FOREIGN_NOOP:
			/* Consume the exact captured control through its original cold
			 * owner; no-effect classification is not a source completion. */
			if (!cluster_cold_typed_side_record_v1(typed, *rep->fence_plan, participant,
												   &rep->decoded.record))
				return false;
			break;
		case CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE:
			/* Keep the original pre-mutation fence and source-local consumer;
			 * never run another origin's SLRU through founder native redo. */
			cluster_typed_replay_foreign_gate(rep);
			if (!cluster_cold_typed_side_record_v1(typed, *rep->fence_plan, participant,
												   &rep->decoded.record))
				return false;
			break;
		case CLUSTER_COLD_UNSCHEDULED_NATIVE:
			if (!is_own)
				cluster_typed_replay_foreign_gate(rep);
			rep->apply_foreign = !is_own;
			cluster_typed_replay_apply(r, rep);
			if (!is_own)
				cluster_vis_bump_merged_records_applied();
			break;
	}
	/* Coverage follows a returned original owner or the validated skip,
	 * never a refused or interrupted record. Durability is checked later. */
	if (!cluster_cold_effects_proof_record_v1(&rep->effects, participant, &rep->decoded, NULL,
											  is_own))
		cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
									  "a consumed record lost its complete effect route");
	return true;
}

static void
pg_attribute_noreturn()
cluster_typed_replay_space_refused(const RelFileLocator *locator, uint32 through)
{
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("typed cold recovery could not install SPACE changes of relation %u/%u/%u",
					locator->spcOid, locator->dbOid, locator->relNumber),
			 errdetail("The SPACE owner refused input %u in its pass-1 order, or this build has "
					   "no cold SPACE install.", through),
			 errhint("Preserve all original thread WAL and DATA; do not force recovery.")));
	pg_unreachable();
}

/*
 * Bring one SPACE relation through `through`, or stop recovery.  `step`: at
 * that input's own SPACE step, where a TRUNCATE's unproven shrink is
 * repeated.
 */
static void
cluster_typed_replay_space_install(ClusterColdTypedReplay *rep, uint32 relation, uint32 through,
								   bool step)
{
	RelFileLocator locator;
	uint32		count = 0;

	memset(&locator, 0, sizeof(locator));
	if (!cluster_cold_plan_space_relation_v1(rep->typed->plan, relation, &locator, &count) ||
		through >= count ||
		!cluster_cold_typed_space_install_v1(rep->typed, relation, through, step))
		cluster_typed_replay_space_refused(&locator, through);
}

/*
 * A CREATE, TRUNCATE or a commit's relation drops.  The SPACE owner brings
 * each relation through this step's input first (never further, so pages
 * of an incarnation never see a later one). The complete COMMIT then uses
 * native own-thread redo or the exact captured foreign cold SIDE owner,
 * retaining all native effects. Without a cold SPACE install the readiness
 * gate refused such a plan before IR.
 */
static void
cluster_typed_replay_space_step(ClusterColdTypedReplay *rep, uint32 participant,
								const ClusterColdStepV1 *step, XLogReaderState *r)
{
	bool		is_own = participant == rep->typed->own_participant;
	uint32		i;

	if (!is_own)
		cluster_typed_replay_foreign_gate(rep);
	for (i = 0; i < step->space_count; i++)
	{
		uint32		relation;
		uint32		input;

		if (!cluster_cold_plan_step_space_v1(rep->typed->plan, step->step_index, i, &relation,
											 &input))
			cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
										  "a SPACE step lost its effects");
		cluster_typed_replay_space_install(rep, relation, input, true);
	}
	if (step->space_kind == CLUSTER_COLD_SPACE_DROP)
	{
		if (!is_own)
		{
			if (!cluster_cold_typed_side_record_v1(rep->typed, *rep->fence_plan,
												 participant, &rep->decoded.record))
				cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
												  "a complete foreign DROP COMMIT consumer refused");
			return;
		}
		rep->apply_foreign = false;
		cluster_typed_replay_apply(r, rep);
	}
	else if (is_own)
		AdvanceNextFullTransactionIdPastXid(r->record->header.xl_xid);
}

/* Every scheduled record already succeeded and all sources were drained
 * by the original sequencer. A final DROP's complete COMMIT has therefore
 * deleted SPACE in this invocation: sync and recheck its result, never
 * reinstall the deleted fork. A final CREATE/ADVANCE may instead have been
 * consumed by its exact native ABORT; its original owner checks that result.
 * Neither check authorizes replay from absent SPACE. */
static bool
cluster_typed_replay_space_final(void *arg, uint32 relation)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;
	RelFileLocator locator;
	uint32		count = 0;
	ClusterColdSpaceInputV1 last;
	bool		aborted = false;

	if (!cluster_cold_plan_space_relation_v1(rep->typed->plan, relation, &locator, &count) ||
		count == 0 ||
		!cluster_cold_plan_space_input_v1(rep->typed->plan, relation, count - 1, &last))
		return false;
	if (last.kind == CLUSTER_COLD_SPACE_DROP)
	{
		ClusterSpaceIdentityKey key = {0};
		ClusterSpaceRecoveryInput input = {last.payload, last.payload_length};
		ClusterSpaceColdSourceV1 source;
		const ClusterColdParticipantV1 *participant;

		if (last.participant >= rep->typed->participant_count)
			return false;
		if (last.participant != rep->typed->own_participant)
			return cluster_cold_typed_drop_final_v1(rep->typed, *rep->fence_plan, relation);
		/* The original native branch remains bound to its own replayed stream. */
		participant = &rep->typed->participants[last.participant];
		if (XLogRecPtrIsInvalid(last.read_rec_ptr) ||
			last.read_rec_ptr < participant->native_redo ||
			last.end_rec_ptr <= last.read_rec_ptr || last.end_rec_ptr > participant->tail_end)
			return false;
		key.system_identifier = rep->typed->observer.system_identifier;
		key.database_incarnation = rep->typed->observer.database_incarnation;
		memcpy(key.storage_uuid, rep->typed->observer.storage_uuid, sizeof(key.storage_uuid));
		key.locator = locator;
		source.origin_thread = participant->thread_id;
		source.end_rec_ptr = last.end_rec_ptr;
		return cluster_space_cold_drop_sync_v1(&key, &input, &source);
	}
	if (!cluster_cold_typed_space_abort_final_v1(rep->typed, relation, &aborted))
		return false;
	if (aborted)
		return true;
	cluster_typed_replay_space_install(rep, relation, count - 1, false);
	return true;
}

/* One scheduled record, already matched to its pass-1 identity. */
static bool
cluster_typed_replay_scheduled(void *arg, uint32 participant, const ClusterColdStepV1 *step,
							   ClusterColdPageActionV1 action)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *) arg;
	ClusterColdTypedV1 *typed = rep->typed;
	const ClusterWalSourceRef *source = &typed->sources[participant];
	bool is_own = participant == typed->own_participant;
	XLogReaderState *r = rep->current[participant];

	/* Check the whole record before the first consumer or skip, including a
	 * COMMIT's native effects before installing any of its SPACE drops. */
	if (cluster_cold_recovery_decode_v1(r, typed->system_identifier,
										source->claim.identity.storage_uuid, false, !is_own,
										&rep->decoded)
			!= CLUSTER_COLD_OK
		|| rep->effects.invalid || !cluster_cold_replay_effects_v1(&rep->decoded, step, is_own)
		|| action != cluster_cold_page_action_v1(step, is_own))
		cluster_typed_replay_mismatch(rep, participant, r->ReadRecPtr,
									  "a scheduled record has effects without a replay consumer");

	switch (action)
	{
		case CLUSTER_COLD_SPACE_STEP:
			cluster_typed_replay_space_step(rep, participant, step, r);
			break;
		case CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID:

			/*
			 * DATA already holds this record's pages, but its xid must still
			 * be behind nextXid, exactly as replaying it would have left it.
			 */
			AdvanceNextFullTransactionIdPastXid(r->record->header.xl_xid);
			break;
		case CLUSTER_COLD_PAGE_SKIP:
			break;
		case CLUSTER_COLD_PAGE_APPLY:
#ifdef CLUSTER_COLD_REDO_HOOK_CONSUMER_V1
			rep->apply_foreign = participant != rep->typed->own_participant;
			if (rep->apply_foreign)
				cluster_typed_replay_foreign_gate(rep);
			cluster_cold_apply_step_v1(step, r, cluster_typed_replay_apply, rep);
			break;
#else
			ereport(
				FATAL,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("typed cold recovery cannot apply page record %X/%X of thread %u",
						LSN_FORMAT_ARGS(step->read_rec_ptr),
						(unsigned)rep->typed->participants[participant].thread_id),
				 errdetail("This build has no per-block typed redo consultation; applying the "
						   "record through the freshness checks could skip or overwrite a newer "
						   "page version."),
				 errhint("Preserve all original thread WAL and start with a build that supports "
						 "typed cold replay.")));
			pg_unreachable();
#endif
		}
		if (!cluster_cold_effects_proof_record_v1(&rep->effects, participant, &rep->decoded, step,
												  is_own))
			cluster_typed_replay_mismatch(
				rep, participant, r->ReadRecPtr,
				"a consumed scheduled record lost its complete effect route");
		return true;
}

/* PGRAC: bind the current C0 before the sequencer touches its target. */
static bool
cluster_typed_replay_dependency(void *arg, uint32 participant,
								const ClusterColdReplayRecordV1 *record, uint32 *target)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *)arg;
	ClusterColdTypedV1 *typed;
	XLogReaderState *r;
	bool is_own;

	if (target != NULL)
		*target = CLUSTER_COLD_NO_INDEX;
	if (rep == NULL || rep->typed == NULL || record == NULL || target == NULL
		|| participant >= rep->typed->participant_count
		|| participant >= CLUSTER_COLD_MAX_PARTICIPANTS || rep->fence_plan == NULL
		|| *rep->fence_plan == NULL)
		return false;
	typed = rep->typed;
	r = rep->current[participant];
	if (r == NULL || r->record == NULL || record->read_rec_ptr != r->ReadRecPtr
		|| record->end_rec_ptr != r->EndRecPtr
		|| record->record_crc != (uint32)r->record->header.xl_crc
		|| record->rmid != r->record->header.xl_rmid || record->info != r->record->header.xl_info)
		return false;
	if (record->rmid != RM_CLUSTER_UNDO_ID
		|| (record->info & ~XLR_INFO_MASK) != XLOG_UNDO_RECOVERY_TT_ABORT)
		return true;
	/* Validate the complete current carrier before touching another stream.
	 * The ordinary consumer decodes it again after the target drain, since
	 * that drain uses the same scratch but preserves this source's reader. */
	is_own = participant == typed->own_participant;
	if (cluster_cold_recovery_decode_v1(r, typed->system_identifier,
										typed->sources[participant].claim.identity.storage_uuid,
										false, !is_own, &rep->decoded)
			!= CLUSTER_COLD_OK
		|| !cluster_cold_replay_effects_v1(&rep->decoded, NULL, is_own))
		return false;
	return cluster_cold_typed_record_target_v1(typed, *rep->fence_plan, participant,
											   &rep->decoded.record, target);
}

/* Preserve the original drain owner; a counted counter is still neither
 * global replay completion nor a qualified all-thread endpoint. */
static bool
cluster_typed_replay_source_drained(void *arg, uint32 participant,
									const ClusterColdReplayResultV1 *progress, uint64 records)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *)arg;
	const ClusterColdXidProgressV1 *xid = &rep->xid_progress[participant];
	uint64 next_xid;

	rep->typed->xid_next_at_drain[participant] = 0;
	if (!cluster_cold_typed_source_drained_v1(rep->typed, *rep->fence_plan, participant, progress,
												 records))
		return false;
	if (!xid->begun)
		return true; /* Missing history coverage stays missing, never a seed-only endpoint. */
	/* The sequencer's result cursors stay zero for an empty source. The
	 * candidate still proves that its checkpoint redo equals the tail. */
	if (records != xid->records || progress->last_read[participant] != xid->last_read
		|| progress->last_end[participant] != (records != 0 ? xid->last_end : InvalidXLogRecPtr)
		|| !cluster_cold_xid_candidate_v1(xid, &rep->typed->participants[participant], &next_xid))
		return false;
	rep->typed->xid_next_at_drain[participant] = next_xid;
	return true;
}

static bool
cluster_typed_replay_source_order(void *arg, uint32 *order, uint32 count)
{
	ClusterColdTypedReplay *rep = (ClusterColdTypedReplay *)arg;

	return cluster_cold_typed_drain_order_v1(rep->typed, *rep->fence_plan, order, count);
}

static const ClusterColdReplayOpsV1 cluster_typed_replay_ops = {
	cluster_typed_replay_next,			 cluster_typed_replay_unscheduled,
	cluster_typed_replay_scheduled,		 cluster_typed_replay_space_final,
	cluster_typed_replay_source_drained, cluster_typed_replay_source_order,
	cluster_typed_replay_dependency,
};

/* A sequencer stop is fatal: report it in pass-1 terms. */
static void
pg_attribute_noreturn()
cluster_typed_replay_stopped(ClusterColdTypedReplay *rep, const ClusterColdReplayResultV1 *result)
{
	const char *reader_error = result->participant < rep->typed->participant_count
		? rep->errormsg[result->participant] : NULL;

	switch ((ClusterColdReplayDetailV1) result->detail)
	{
		case CLUSTER_COLD_REPLAY_SOURCE_ENDED:
			cluster_typed_replay_mismatch(rep, result->participant, result->rec_ptr,
										  reader_error != NULL ? reader_error
										  : "the scheduled record is missing");
		case CLUSTER_COLD_REPLAY_TARGET_PASSED:
			cluster_typed_replay_mismatch(rep, result->participant, result->rec_ptr,
										  "the scheduled record start was passed");
		case CLUSTER_COLD_REPLAY_IDENTITY:
			cluster_typed_replay_mismatch(rep, result->participant, result->rec_ptr,
										  "the scheduled record identity differs");
		case CLUSTER_COLD_REPLAY_PAST_TAIL:
			cluster_typed_replay_mismatch(rep, result->participant, result->rec_ptr,
										  "a record lies past the validated tail");
		case CLUSTER_COLD_REPLAY_CUT_DIFFERS:
			cluster_typed_replay_mismatch(rep, result->participant, result->rec_ptr,
										  reader_error != NULL ? reader_error
										  : "the replayed cut differs from pass 1");
		default:
			break;
	}
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("typed cold recovery pass 2 stopped at step %u (detail %u)",
					result->steps_done, (unsigned) result->detail)));
	pg_unreachable();
}

static void
pg_attribute_noreturn()
cluster_typed_replay_unpublished(uint16 thread, const char *what, int detail)
{
	ereport(FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("typed cold recovery could not publish thread %u recovered", (unsigned) thread),
			 errdetail("%s (result %d).", what, detail),
			 errhint("Retry startup; a generation not published recovered is recovered again. "
					 "Preserve all original thread WAL; do not open the database.")));
	pg_unreachable();
}

/* Keep the original durability barrier, then final PAGE readback, before
 * counter publication. Neither step grants recovered-generation authority. */
static bool
cluster_typed_replay_durable_endpoints(ClusterColdTypedReplay *rep,
									   const ClusterColdReplayResultV1 *result)
{
	return cluster_cold_durable_barrier_v1(&rep->touched)
		   && cluster_cold_data_readback_v1(rep->typed)
		   && cluster_cold_typed_xid_endpoints_v1(rep->typed, *rep->fence_plan, result,
												  rep->xid_progress);
}

/*
 * Publish every fenced generation recovered: pass 2 must have ended exactly
 * at its sealed tail, every file pass 2 changed (and every SPACE relation)
 * is made durable while IR is still held, then the fence plan hands each
 * root to RECOVERY_COMPLETE.  The founder's own generation is not
 * published here.
 */
static void
cluster_typed_replay_publish(ClusterColdTypedReplay *rep, const ClusterColdReplayResultV1 *result)
{
	ClusterRecoveryFencePlan *plan = *rep->fence_plan;
	uint16		origins = cluster_recovery_merge_fence_plan_origin_count(plan);
	uint16		thread = 0;
	int			detail = 0;

	if (!cluster_cold_typed_effects_complete_v1(rep->typed, &rep->effects, result))
		cluster_typed_replay_unpublished(
			rep->typed->participants[rep->typed->own_participant].thread_id,
			"Complete ordinary effect coverage is missing or inconsistent", (int) result->detail);
	if (!cluster_cold_completion_ready_v1(rep->typed, plan, result, &rep->touched, &thread))
		cluster_typed_replay_unpublished(thread, "Replay did not end at its sealed tail",
										 (int) result->detail);
	if (!cluster_typed_replay_durable_endpoints(rep, result))
		cluster_typed_replay_unpublished(
			thread, "Native durability, PAGE readback or source counter endpoints are incomplete",
			(int)rep->typed->refusal);
	/* Retain read-only ACTIVE inputs before the original pass-2 authority
	 * is released. Unproven tops stay explicit; C0 requires its own later
	 * TX_CLOSURE source/isolation and writer authority after native EOR. */
	if (!cluster_cold_typed_top_tx_census_v1(rep->typed, plan))
		cluster_typed_replay_unpublished(
			thread, "Post-redo ACTIVE input census is incomplete", (int)rep->typed->refusal);
	if (!cluster_recovery_merge_fence_plan_bind_tx_inputs_v1(
			plan, rep->typed->sources, rep->typed->replay_count))
		cluster_typed_replay_unpublished(
			thread, "Original transaction isolation inputs could not be retained", 0);
	switch (cluster_recovery_merge_fence_plan_complete_v1(plan, &thread, &detail))
	{
		case CLUSTER_RECOVERY_FENCE_COMPLETE_OK:
			break;
		case CLUSTER_RECOVERY_FENCE_COMPLETE_AUTHORITY:
			cluster_write_fence_note_external_publish_gate_blocked();
			cluster_typed_replay_unpublished(thread, "Its external write exclusion became stale",
											 detail);
		case CLUSTER_RECOVERY_FENCE_COMPLETE_IR_RELEASE:
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("cold recovery serialization release was not confirmed"),
					 errdetail("No fenced generation was published recovered (result %d).",
							   detail)));
			break;
		default:
			cluster_typed_replay_unpublished(thread, "The recovery completion handoff refused",
											 detail);
	}
	ereport(LOG,
			(errmsg("cluster typed cold recovery: %u fenced writer generations published recovered",
					(unsigned) origins),
			 errdetail_log("%u relations made durable first.", rep->touched.count)));
}

/* Pin progress to the own stream, publish completion and release. */
static void
cluster_typed_replay_finish(ClusterColdTypedReplay *rep, const ClusterColdReplayResultV1 *result)
{
	if (result->own_read != InvalidXLogRecPtr)
	{
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		XLogRecoveryCtl->lastReplayedReadRecPtr = result->own_read;
		XLogRecoveryCtl->lastReplayedEndRecPtr = result->own_end;
		XLogRecoveryCtl->lastReplayedTLI = *rep->replayTLI;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);
	}
	/*
	 * Every generation's records, history included, were written at or
	 * below the plan's highest SCN; the founder's clock must pass them all
	 * before any of its own commits, also for records not replayed here.
	 */
	cluster_scn_recovery_replay_observe((SCN) cluster_cold_plan_max_scn_v1(rep->typed->plan));
	if (*rep->fence_plan != NULL &&
		!cluster_recovery_merge_fence_plan_revalidate_nowait(*rep->fence_plan))
	{
		cluster_write_fence_note_external_publish_gate_blocked();
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_EXTERNAL_FENCE_UNAVAILABLE),
				 errmsg("external write exclusion became stale before recovery completion"),
				 errhint("Retry startup; replayed foreign state remains unpublished until the "
						 "complete fence plan is current.")));
	}
	cluster_typed_replay_publish(rep, result);
	if (*rep->fence_plan != NULL)
	{
		if (!cluster_recovery_merge_fence_plan_release_serial(*rep->fence_plan))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("cold recovery serialization release was not confirmed")));
		if (rep->typed->tx_fence != NULL
			|| !cluster_recovery_merge_fence_plan_tx_current_v1(*rep->fence_plan, NULL))
			cluster_typed_replay_unpublished(
				0, "Original completed transaction fence owner could not be transferred", 0);
		rep->typed->tx_fence = *rep->fence_plan;
		*rep->fence_plan = NULL;
	}
}

/*
 * cluster_recovery_typed_replay -- typed cold replay pass 2.
 *
 *	cluster_cold_replay_run_v1 sequences the sealed schedule over one native
 *	reader per participant: records before each scheduled page record
 *	replay in stream order, each scheduled record must match its pass-1
 *	identity, and every participant must end at its validated tail with
 *	exactly the records pass 1 counted.  Applied page records run inside
 *	their published step and the native block consumer's bracket.
 */
static XLogRecPtr
cluster_recovery_typed_replay(ClusterColdTypedV1 **typed_address, TimeLineID *replayTLI,
							  ClusterRecoveryFencePlan **fence_plan)
{
	ClusterColdTypedV1 *typed = *typed_address;
	ClusterColdTypedReplay *rep = palloc0(sizeof(*rep));
	ClusterColdReplayResultV1 result;
	uint32		i;
	XLogRecPtr	own_end;

	rep->typed = typed;
	rep->fence_plan = fence_plan;
	rep->replayTLI = replayTLI;
	cluster_cold_effects_proof_init_v1(&rep->effects);
	for (i = 0; i < typed->participant_count; i++)
	{
		rep->readers[i] = cluster_cold_reader_open_v1(&typed->sources[i],
													  typed->system_identifier,
													  typed->participants[i].native_redo);
		if (rep->readers[i] == NULL)
			cluster_typed_replay_mismatch(rep, i, typed->participants[i].native_redo,
										  "the selected source cannot be opened");
	}
	cluster_cold_replay_window_enter_v1();
	if (!cluster_cold_typed_sync_sources_v1(typed, *fence_plan))
		cluster_recovery_typed_fail(fence_plan, typed_address,
									"typed cold recovery source durability refused",
									typed->refusal_detail);
	if (!cluster_typed_replay_xid_begin(rep))
		cluster_recovery_typed_fail(fence_plan, typed_address,
									"typed cold recovery counter input refused",
									"An original checkpoint seed or replay cut is missing or invalid.");
	cluster_recovery_merge_window_enter();
	cluster_recovery_merge_set_own_lsn((uint64) typed->participants[typed->own_participant].native_redo);
	if (cluster_cold_replay_run_v1(typed->plan, typed->participants, typed->participant_count,
								   typed->own_participant, &cluster_typed_replay_ops, rep,
								   &result) != CLUSTER_COLD_REPLAY_OK)
		cluster_typed_replay_stopped(rep, &result);
	cluster_recovery_merge_window_leave();
	for (i = 0; i < typed->participant_count; i++)
		cluster_cold_reader_close_v1(&rep->readers[i]);
	cluster_cold_decoded_release_v1(&rep->decoded);
	cluster_typed_replay_finish(rep, &result);
	cluster_cold_replay_window_leave_v1();
	ereport(LOG,
			(errmsg("cluster typed cold recovery: replay complete (own thread %u)",
					(unsigned) typed->participants[typed->own_participant].thread_id),
			 errdetail_log(UINT64_FORMAT " page records skipped, " UINT64_FORMAT " applied; "
						   "%u SPACE steps, %u SPACE relations installed.",
						   result.pages_skipped, result.pages_applied, result.space_steps,
						   result.space_relations_finished)));
	own_end = result.own_end;
	if (rep->touched.rels != NULL)
		pfree(rep->touched.rels);
	pfree(rep);
	cluster_recovery_cold_inputs_retain(typed_address);
	return own_end;
}

static void
cluster_adg_streaming_wait(bool *streaming_reply_sent)
{
	if (streaming_reply_sent != NULL && !*streaming_reply_sent)
	{
		WalRcvForceReply();
		*streaming_reply_sent = true;
	}

	KnownAssignedTransactionIdsIdleMaintenance();
	(void) WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 1000L,
					 WAIT_EVENT_RECOVERY_WAL_STREAM);
	ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
}

static XLogRecPtr
cluster_adg_streaming_replay(XLogPrefetcher *xlogprefetcher, XLogReaderState *xlogreader,
							 XLogRecord *first_record, TimeLineID tli, TimeLineID *replayTLI,
							 bool *reachedRecoveryTarget)
{
	ClusterRecoveryMergeState *st = NULL;
	XLogRecPtr start_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	XLogRecPtr receive_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	XLogRecPtr barrier_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	SCN barrier_scn[CLUSTER_WAL_THREAD_MAX + 1];
	uint64 bitmap[2];
	uint64 active_bitmap[2] = {0, 0};
	XLogRecPtr replay_end;
	XLogRecord *native_record = first_record;
	bool window_active = false;
	bool streaming_reply_sent = false;
	uint16 recovery_thread;
	XLogRecPtr own_progress_lsn;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	replay_end = XLogRecoveryCtl->lastReplayedEndRecPtr;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	/*
	 * spec-6.4 P1-c: this standby's durable recovery state (backup label,
	 * minRecoveryPoint, local pg_wal) all live in the LSN space of the WAL
	 * thread its base backup came from -- the thread the just-read
	 * checkpoint record sits on.  Records on that thread advance the native
	 * recovery-progress globals; records on every other thread are FOREIGN
	 * (incomparable LSN space) and get the merged-window pd_lsn/progress
	 * clamp, mirroring cluster_recovery_merged_replay's §3.3b discipline.
	 */
	recovery_thread = XLogReaderGetThreadId(xlogreader);
	if (recovery_thread < XLP_THREAD_ID_FIRST_REAL || recovery_thread > CLUSTER_WAL_THREAD_MAX)
		recovery_thread = cluster_wal_thread_id();
	own_progress_lsn = replay_end;
	/* P0-3: RFS restart-point selection needs to know the recovery space. */
	cluster_mrp_publish_recovery_thread(recovery_thread);

	for (;;)
	{
		XLogReaderState *r;
		XLogRecord *rec;
		uint16 thread_id;

		HandleStartupProcInterrupts();

		if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
			RECOVERY_NOT_PAUSED)
			recoveryPausesHere(false);

		if (CheckForStandbyTrigger())
			break;

		memset(start_lsn, 0, sizeof(start_lsn));
		memset(receive_lsn, 0, sizeof(receive_lsn));
		memset(barrier_lsn, 0, sizeof(barrier_lsn));
		memset(barrier_scn, 0, sizeof(barrier_scn));
		if (cluster_mrp_streaming_snapshot(bitmap, start_lsn, receive_lsn, barrier_lsn,
										   barrier_scn) <= 0)
		{
			if (native_record != NULL && cluster_rfs_configured_upstream_count() <= 0)
			{
				/*
				 * spec-6.4 INV-ADG5 (F1): the apply-master gate covers the
				 * whole replay, including pre-consistency bootstrap.  The
				 * "local copy" a base backup restores routes user-relation
				 * writes through the shared smgr, so replaying it without
				 * the lease would race the current Apply Master's writes
				 * into the same shared files.  A node that cannot obtain
				 * the lease waits here and never opens for reads; an
				 * ex-master fails stop inside the predicate (P0-1).
				 */
				if (!cluster_adg_apply_gate_self_valid())
				{
					cluster_adg_streaming_wait(&streaming_reply_sent);
					continue;
				}

				if (recoveryStopsBefore(xlogreader))
				{
					if (reachedRecoveryTarget != NULL)
						*reachedRecoveryTarget = true;
					break;
				}
				if (recoveryApplyDelay(xlogreader)
					&& ((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
					RECOVERY_NOT_PAUSED)
					recoveryPausesHere(false);

				ApplyWalRecord(xlogreader, native_record, replayTLI);
				replay_end = xlogreader->EndRecPtr;
				own_progress_lsn = replay_end;
				streaming_reply_sent = false;

				if (recoveryStopsAfter(xlogreader))
				{
					if (reachedRecoveryTarget != NULL)
						*reachedRecoveryTarget = true;
					break;
				}

				native_record = ReadRecord(xlogprefetcher, LOG, false, *replayTLI);
				continue;
			}

			cluster_adg_streaming_wait(&streaming_reply_sent);
			continue;
		}

		if (st != NULL && (active_bitmap[0] != bitmap[0] || active_bitmap[1] != bitmap[1]))
		{
			cluster_recovery_merge_set_apply_foreign(false);
			if (window_active)
			{
				cluster_recovery_merge_window_leave();
				window_active = false;
			}
			cluster_recovery_merge_end(st);
			st = NULL;
		}

		if (st == NULL)
		{
			st = cluster_recovery_merge_streaming_begin(bitmap, start_lsn, tli);
			if (st == NULL)
			{
				cluster_adg_streaming_wait(&streaming_reply_sent);
				continue;
			}
			cluster_recovery_merge_window_enter();
			/* §3.3b: foreign records' pages get the own-space clamp (P1-c). */
			cluster_recovery_merge_set_own_lsn((uint64) own_progress_lsn);
			window_active = true;
			active_bitmap[0] = bitmap[0];
			active_bitmap[1] = bitmap[1];
		}

		if (!cluster_adg_apply_gate_self_valid())
		{
			cluster_adg_streaming_wait(&streaming_reply_sent);
			continue;
		}

		r = cluster_recovery_merge_streaming_next(st, receive_lsn, barrier_lsn, barrier_scn,
												  &thread_id, NULL);
		if (r == NULL)
		{
			cluster_adg_streaming_wait(&streaming_reply_sent);
			continue;
		}
		rec = &r->record->header;

		if (recoveryStopsBefore(r))
		{
			if (reachedRecoveryTarget != NULL)
				*reachedRecoveryTarget = true;
			break;
		}
		if (recoveryApplyDelay(r)
			&& ((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
			RECOVERY_NOT_PAUSED)
			recoveryPausesHere(false);

		{
			bool		is_own = (thread_id == recovery_thread);

			cluster_recovery_merge_set_scn(rec->xl_scn);
			/*
			 * spec-6.4 P1-c: only recovery-thread records carry LSNs in this
			 * node's own recovery space.  Every other thread is FOREIGN --
			 * its pd_lsn stamps and the shared recovery-progress globals get
			 * clamped to own_progress_lsn (see PageSetLSN and
			 * ApplyWalRecord), keeping minRecoveryPoint single-space.
			 */
			cluster_recovery_merge_set_apply_foreign(!is_own);
			ApplyWalRecord(r, rec, replayTLI);
			cluster_recovery_merge_set_apply_foreign(false);
			if (is_own)
			{
				own_progress_lsn = r->EndRecPtr;
				cluster_recovery_merge_set_own_lsn((uint64) own_progress_lsn);
			}
		}
		replay_end = r->EndRecPtr;
		streaming_reply_sent = false;

		if (recoveryStopsAfter(r))
		{
			if (reachedRecoveryTarget != NULL)
				*reachedRecoveryTarget = true;
			break;
		}
	}

	cluster_recovery_merge_set_apply_foreign(false);
	if (window_active)
		cluster_recovery_merge_window_leave();
	cluster_recovery_merge_end(st);
	return replay_end;
}
#endif

/*
 * Subroutine of PerformWalRecovery, to apply one WAL record.
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * cluster_record_apply_class -- §3.3e classification of the decoded record
 *	for the spec-4.5 merged loop.
 *
 *	Walks the record's block refs through the smgr routing predicate:
 *	a mixed shared/local set is UNCLASSIFIABLE; otherwise the class is
 *	decided by cluster_recovery_record_class.
 */
static ClusterRecoveryRecordClass
cluster_record_apply_class(XLogReaderState *r)
{
	bool		has_block = XLogRecHasAnyBlockRefs(r);
	bool		first_shared = false;
	bool		same_routing = true;

	/* PGRAC: typed SPACE lifecycle records never carry ordinary block refs.
	 * Validate their complete shape before the generic block-ref route, so
	 * malformed foreign WAL cannot bypass its typed owner or look ignorable. */
	if (XLogRecGetRmid(r) == RM_SMGR_ID &&
		(XLogRecGetInfo(r) & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_IDENTITY)
	{
		ClusterSpaceStructureChange change;

		if (has_block ||
			!cluster_space_structure_wal_decode(XLogRecGetData(r), XLogRecGetDataLen(r), &change))
			return CLUSTER_RECMERGE_UNCLASSIFIABLE;
		first_shared = (cluster_smgr_which_for(change.identity.result.key.locator, InvalidBackendId) == 1);
		return cluster_recovery_record_class(RM_SMGR_ID, false, first_shared, true);
	}
	if (XLogRecGetRmid(r) == RM_SMGR_ID &&
		(XLogRecGetInfo(r) & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_RESERVATION)
	{
		ClusterSpaceReservationChange change;

		if (has_block ||
			!cluster_space_reservation_wal_decode(XLogRecGetData(r), XLogRecGetDataLen(r), &change) ||
			change.action != CLUSTER_SPACE_RESERVATION_ADVANCE)
			return CLUSTER_RECMERGE_UNCLASSIFIABLE;
		first_shared = (cluster_smgr_which_for(change.result.identity.key.locator, InvalidBackendId) == 1);
		return cluster_recovery_record_class(RM_SMGR_ID, false, first_shared, true);
	}

	if (has_block)
	{
		int			bid;
		int			routed = -1;	/* -1 unset, 0 local, 1 shared */

		for (bid = 0; bid <= XLogRecMaxBlockId(r); bid++)
		{
			RelFileLocator rl;
			int			this_shared;

			if (!XLogRecGetBlockTagExtended(r, bid, &rl, NULL, NULL, NULL))
				continue;
			this_shared = (cluster_smgr_which_for(rl, InvalidBackendId) == 1) ? 1 : 0;
			if (routed < 0)
				routed = this_shared;
			else if (routed != this_shared)
				same_routing = false;
		}
		first_shared = (routed == 1);
	}
	else if (XLogRecGetRmid(r) == RM_SMGR_ID)
	{
		/*
		 * PGRAC: spec-6.14 D9 amend -- smgr create/truncate carry their
		 * target relfile in the PAYLOAD, not in a block ref, so the block
		 * walk above cannot route them (pre-amend they fell through to
		 * UNCLASSIFIABLE and 53RA3'd the merge on the first foreign CREATE
		 * TABLE tail).  Route the payload locator through the same smgr
		 * predicate: a shared relfile's create/truncate is S (smgr_redo is
		 * idempotent under isRedo -- create-if-absent / truncate-to-size),
		 * a local one is L (a foreign stream must not re-drive this node's
		 * local files).  The routing rides the first_block_is_shared
		 * parameter of the pure matrix.
		 */
		uint8		info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;

		if (info == XLOG_SMGR_CREATE)
		{
			const xl_smgr_create *xlrec = (const xl_smgr_create *) XLogRecGetData(r);

			first_shared = (cluster_smgr_which_for(xlrec->rlocator, InvalidBackendId) == 1);
		}
		else if (info == XLOG_SMGR_TRUNCATE)
		{
			const xl_smgr_truncate *xlrec = (const xl_smgr_truncate *) XLogRecGetData(r);

			first_shared = (cluster_smgr_which_for(xlrec->rlocator, InvalidBackendId) == 1);
		}
		else
			return CLUSTER_RECMERGE_UNCLASSIFIABLE; /* unknown smgr info bits */
	}

	return cluster_recovery_record_class(XLogRecGetRmid(r), has_block, first_shared,
										 same_routing);
}
#endif

static void
ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI)
{
	ErrorContextCallback errcallback;
	bool		switchedTLI = false;

#ifdef USE_PGRAC_CLUSTER
	/*
	 * spec-6.4 INV-ADG5: an ADG standby may only replay (and thus write
	 * shared standby storage) while this node holds the Apply Master lease.
	 * The gate covers every record, including pre-consistency bootstrap
	 * replay from a base backup (F1): those writes go through the shared
	 * smgr too, so a node that cannot obtain the lease waits here forever
	 * and never opens for reads.
	 *
	 * A node that has already applied records under some term must never
	 * resume under a different term (P0-1): an interim Apply Master may
	 * have advanced shared storage past this node's cached pages, so the
	 * only safe reaction to deposition is a FATAL fail-stop that discards
	 * the caches.  A same-term renewal gap is tolerated for at most half
	 * the takeover grace -- a successor can only win the durable lease CAS
	 * a full grace after expiry.
	 */
	if (RecoveryInProgress() && cluster_mrp_should_start())
	{
		while (!cluster_adg_apply_gate_self_valid())
		{
			HandleStartupProcInterrupts();
			(void) WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 100L,
							 WAIT_EVENT_ADG_MRP_APPLY_WAIT);
			ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
		}
	}
#endif

	/* Setup error traceback support for ereport() */
	errcallback.callback = rm_redo_error_callback;
	errcallback.arg = (void *) xlogreader;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * ShmemVariableCache->nextXid must be beyond record's xid.
	 *
	 * PGRAC: a merged-recovery FOREIGN record belongs to a peer's xid
	 * authority, not this node's allocator.  Advancing the local nextXid here
	 * would make StartupCLOG/TrimCLOG expect local pg_xact pages for peer
	 * striped xids, while the peer outcome truth lives in pg_xact_remote.
	 */
#ifdef USE_PGRAC_CLUSTER
	if (!(cluster_recmerge_window_active && cluster_recmerge_apply_foreign))
#endif
		AdvanceNextFullTransactionIdPastXid(record->xl_xid);

	/*
	 * Before replaying this record, check if this record causes the current
	 * timeline to change. The record is already considered to be part of the
	 * new timeline, so we update replayTLI before replaying it. That's
	 * important so that replayEndTLI, which is recorded as the minimum
	 * recovery point's TLI if recovery stops after this record, is set
	 * correctly.
	 */
	if (record->xl_rmid == RM_XLOG_ID)
	{
		TimeLineID	newReplayTLI = *replayTLI;
		TimeLineID	prevReplayTLI = *replayTLI;
		uint8		info = record->xl_info & ~XLR_INFO_MASK;

		if (info == XLOG_CHECKPOINT_SHUTDOWN)
		{
			CheckPoint	checkPoint;

			memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
			newReplayTLI = checkPoint.ThisTimeLineID;
			prevReplayTLI = checkPoint.PrevTimeLineID;
		}
		else if (info == XLOG_END_OF_RECOVERY)
		{
			xl_end_of_recovery xlrec;

			memcpy(&xlrec, XLogRecGetData(xlogreader), sizeof(xl_end_of_recovery));
			newReplayTLI = xlrec.ThisTimeLineID;
			prevReplayTLI = xlrec.PrevTimeLineID;
		}

		if (newReplayTLI != *replayTLI)
		{
			/* Check that it's OK to switch to this TLI */
			checkTimeLineSwitch(xlogreader->EndRecPtr,
								newReplayTLI, prevReplayTLI, *replayTLI);

			/* Following WAL records should be run with new TLI */
			*replayTLI = newReplayTLI;
			switchedTLI = true;
		}
	}

	/*
	 * Update shared replayEndRecPtr before replaying this record, so that
	 * XLogFlush will update minRecoveryPoint correctly.
	 *
	 * PGRAC: spec-6.4 P1-c -- a FOREIGN record's EndRecPtr is in another WAL
	 * thread's LSN space.  replayEndRecPtr feeds UpdateMinRecoveryPoint (and
	 * GetCurrentReplayRecPtr in other processes), which assume this node's
	 * recovery-thread space, so pin the shared progress at the own-space
	 * clamp instead of poisoning the control file with a mixed-space LSN.
	 */
#ifdef USE_PGRAC_CLUSTER
	if (cluster_recmerge_window_active && cluster_recmerge_apply_foreign)
	{
		XLogRecPtr	own_lsn = (XLogRecPtr) cluster_recmerge_window_own_lsn;

		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		if (XLogRecoveryCtl->replayEndRecPtr < own_lsn)
			XLogRecoveryCtl->replayEndRecPtr = own_lsn;
		XLogRecoveryCtl->replayEndTLI = *replayTLI;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);
	}
	else
#endif
	{
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		XLogRecoveryCtl->replayEndRecPtr = xlogreader->EndRecPtr;
		XLogRecoveryCtl->replayEndTLI = *replayTLI;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);
	}

	/*
	 * If we are attempting to enter Hot Standby mode, process XIDs we see
	 */
	if (standbyState >= STANDBY_INITIALIZED &&
		TransactionIdIsValid(record->xl_xid))
		RecordKnownAssignedTransactionIds(record->xl_xid);

	/*
	 * Some XLOG record types that are related to recovery are processed
	 * directly here, rather than in xlog_redo()
	 */
	if (record->xl_rmid == RM_XLOG_ID)
		xlogrecovery_redo(xlogreader, *replayTLI);

	/* Now apply the WAL record itself. */
	GetRmgr(record->xl_rmid).rm_redo(xlogreader);

	/*
	 * After redo, check whether the backup pages associated with the
	 * WAL record are consistent with the existing pages. This check
	 * is done only if consistency check is enabled for this record.
	 */
	if ((record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
		verifyBackupPageConsistency(xlogreader);

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	/*
	 * Update lastReplayedEndRecPtr after this record has been successfully
	 * replayed.
	 *
	 * PGRAC: spec-6.4 P1-c -- skip the update for a FOREIGN record: the
	 * shared last-replayed pointers stay pinned at the last own-space
	 * record so GetXLogReplayRecPtr readers never observe a mixed-space
	 * LSN.  Per-thread progress is tracked separately in the MRP shmem
	 * watermarks below.
	 */
#ifdef USE_PGRAC_CLUSTER
	if (!(cluster_recmerge_window_active && cluster_recmerge_apply_foreign))
#endif
	{
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		XLogRecoveryCtl->lastReplayedReadRecPtr = xlogreader->ReadRecPtr;
		XLogRecoveryCtl->lastReplayedEndRecPtr = xlogreader->EndRecPtr;
		XLogRecoveryCtl->lastReplayedTLI = *replayTLI;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);
	}

#ifdef USE_PGRAC_CLUSTER
	if (cluster_mrp_should_start())
	{
		if (!cluster_mrp_apply_record_replayed(XLogReaderGetThreadId(xlogreader),
											   xlogreader->EndRecPtr,
											   XLogRecGetScn(xlogreader)))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("ADG standby apply-master lease is not valid for WAL replay")));
	}
#endif

	/* ------
	 * Wakeup walsenders:
	 *
	 * On the standby, the WAL is flushed first (which will only wake up
	 * physical walsenders) and then applied, which will only wake up logical
	 * walsenders.
	 *
	 * Indeed, logical walsenders on standby can't decode and send data until
	 * it's been applied.
	 *
	 * Physical walsenders don't need to be woken up during replay unless
	 * cascading replication is allowed and time line change occurred (so that
	 * they can notice that they are on a new time line).
	 *
	 * That's why the wake up conditions are for:
	 *
	 *  - physical walsenders in case of new time line and cascade
	 *    replication is allowed
	 *  - logical walsenders in case cascade replication is allowed (could not
	 *    be created otherwise)
	 * ------
	 */
	if (AllowCascadeReplication())
		WalSndWakeup(switchedTLI, true);

	/*
	 * If rm_redo called XLogRequestWalReceiverReply, then we wake up the
	 * receiver so that it notices the updated lastReplayedEndRecPtr and sends
	 * a reply to the primary.
	 */
	if (doRequestWalReceiverReply)
	{
		doRequestWalReceiverReply = false;
		WalRcvForceReply();
	}

	/* Allow read-only connections if we're consistent now */
	CheckRecoveryConsistency();

	/* Is this a timeline switch? */
	if (switchedTLI)
	{
		/*
		 * Before we continue on the new timeline, clean up any (possibly
		 * bogus) future WAL segments on the old timeline.
		 */
		RemoveNonParentXlogFiles(xlogreader->EndRecPtr, *replayTLI, true);

		/* Reset the prefetcher. */
		XLogPrefetchReconfigure();
	}
}

/*
 * Some XLOG RM record types that are directly related to WAL recovery are
 * handled here rather than in the xlog_redo()
 */
static void
xlogrecovery_redo(XLogReaderState *record, TimeLineID replayTLI)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	XLogRecPtr	lsn = record->EndRecPtr;

	Assert(XLogRecGetRmid(record) == RM_XLOG_ID);

	if (info == XLOG_OVERWRITE_CONTRECORD)
	{
		/* Verify the payload of a XLOG_OVERWRITE_CONTRECORD record. */
		xl_overwrite_contrecord xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_overwrite_contrecord));
		if (xlrec.overwritten_lsn != record->overwrittenRecPtr)
			elog(FATAL, "mismatching overwritten LSN %X/%X -> %X/%X",
				 LSN_FORMAT_ARGS(xlrec.overwritten_lsn),
				 LSN_FORMAT_ARGS(record->overwrittenRecPtr));

		/* We have safely skipped the aborted record */
		abortedRecPtr = InvalidXLogRecPtr;
		missingContrecPtr = InvalidXLogRecPtr;

		ereport(LOG,
				(errmsg("successfully skipped missing contrecord at %X/%X, overwritten at %s",
						LSN_FORMAT_ARGS(xlrec.overwritten_lsn),
						timestamptz_to_str(xlrec.overwrite_time))));

		/* Verifying the record should only happen once */
		record->overwrittenRecPtr = InvalidXLogRecPtr;
	}
	else if (info == XLOG_BACKUP_END)
	{
		XLogRecPtr	startpoint;

		memcpy(&startpoint, XLogRecGetData(record), sizeof(startpoint));

		if (backupStartPoint == startpoint)
		{
			/*
			 * We have reached the end of base backup, the point where
			 * pg_backup_stop() was done.  The data on disk is now consistent
			 * (assuming we have also reached minRecoveryPoint).  Set
			 * backupEndPoint to the current LSN, so that the next call to
			 * CheckRecoveryConsistency() will notice it and do the
			 * end-of-backup processing.
			 */
			elog(DEBUG1, "end of backup record reached");

			backupEndPoint = lsn;
		}
		else
			elog(DEBUG1, "saw end-of-backup record for backup starting at %X/%X, waiting for %X/%X",
				 LSN_FORMAT_ARGS(startpoint), LSN_FORMAT_ARGS(backupStartPoint));
	}
}

/*
 * Verify that, in non-test mode, ./pg_tblspc doesn't contain any real
 * directories.
 *
 * Replay of database creation XLOG records for databases that were later
 * dropped can create fake directories in pg_tblspc.  By the time consistency
 * is reached these directories should have been removed; here we verify
 * that this did indeed happen.  This is to be called at the point where
 * consistent state is reached.
 *
 * allow_in_place_tablespaces turns the PANIC into a WARNING, which is
 * useful for testing purposes, and also allows for an escape hatch in case
 * things go south.
 */
static void
CheckTablespaceDirectory(void)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir("pg_tblspc");
	while ((de = ReadDir(dir, "pg_tblspc")) != NULL)
	{
		char		path[MAXPGPATH + 10];

		/* Skip entries of non-oid names */
		if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
			continue;

		snprintf(path, sizeof(path), "pg_tblspc/%s", de->d_name);

		if (get_dirent_type(path, de, false, ERROR) != PGFILETYPE_LNK)
			ereport(allow_in_place_tablespaces ? WARNING : PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected directory entry \"%s\" found in %s",
							de->d_name, "pg_tblspc/"),
					 errdetail("All directory entries in pg_tblspc/ should be symbolic links."),
					 errhint("Remove those directories, or set allow_in_place_tablespaces to ON transiently to let recovery complete.")));
	}
}

/*
 * Checks if recovery has reached a consistent state. When consistency is
 * reached and we have a valid starting standby snapshot, tell postmaster
 * that it can start accepting read-only connections.
 */
static void
CheckRecoveryConsistency(void)
{
	XLogRecPtr	lastReplayedEndRecPtr;
	TimeLineID	lastReplayedTLI;

	/*
	 * During crash recovery, we don't reach a consistent state until we've
	 * replayed all the WAL.
	 */
	if (XLogRecPtrIsInvalid(minRecoveryPoint))
		return;

	Assert(InArchiveRecovery);

	/*
	 * assume that we are called in the startup process, and hence don't need
	 * a lock to read lastReplayedEndRecPtr
	 */
	lastReplayedEndRecPtr = XLogRecoveryCtl->lastReplayedEndRecPtr;
	lastReplayedTLI = XLogRecoveryCtl->lastReplayedTLI;

	/*
	 * Have we reached the point where our base backup was completed?
	 */
	if (!XLogRecPtrIsInvalid(backupEndPoint) &&
		backupEndPoint <= lastReplayedEndRecPtr)
	{
		XLogRecPtr	saveBackupStartPoint = backupStartPoint;
		XLogRecPtr	saveBackupEndPoint = backupEndPoint;

		elog(DEBUG1, "end of backup reached");

		/*
		 * We have reached the end of base backup, as indicated by pg_control.
		 * Update the control file accordingly.
		 */
		ReachedEndOfBackup(lastReplayedEndRecPtr, lastReplayedTLI);
		backupStartPoint = InvalidXLogRecPtr;
		backupEndPoint = InvalidXLogRecPtr;
		backupEndRequired = false;

		ereport(LOG,
				(errmsg("completed backup recovery with redo LSN %X/%X and end LSN %X/%X",
						LSN_FORMAT_ARGS(saveBackupStartPoint),
						LSN_FORMAT_ARGS(saveBackupEndPoint))));
	}

	/*
	 * Have we passed our safe starting point? Note that minRecoveryPoint is
	 * known to be incorrectly set if recovering from a backup, until the
	 * XLOG_BACKUP_END arrives to advise us of the correct minRecoveryPoint.
	 * All we know prior to that is that we're not consistent yet.
	 */
	if (!reachedConsistency && !backupEndRequired &&
		minRecoveryPoint <= lastReplayedEndRecPtr)
	{
		/*
		 * Check to see if the XLOG sequence contained any unresolved
		 * references to uninitialized pages.
		 */
		XLogCheckInvalidPages();

		/*
		 * Check that pg_tblspc doesn't contain any real directories. Replay
		 * of Database/CREATE_* records may have created fictitious tablespace
		 * directories that should have been removed by the time consistency
		 * was reached.
		 */
		CheckTablespaceDirectory();

		reachedConsistency = true;
		ereport(LOG,
				(errmsg("consistent recovery state reached at %X/%X",
						LSN_FORMAT_ARGS(lastReplayedEndRecPtr))));
	}

	/*
	 * Have we got a valid starting snapshot that will allow queries to be
	 * run? If so, we can tell postmaster that the database is consistent now,
	 * enabling connections.
	 */
	if (standbyState == STANDBY_SNAPSHOT_READY &&
		!LocalHotStandbyActive &&
		reachedConsistency &&
		IsUnderPostmaster)
	{
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		XLogRecoveryCtl->SharedHotStandbyActive = true;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);

		LocalHotStandbyActive = true;

		SendPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY);
	}
}

/*
 * Error context callback for errors occurring during rm_redo().
 */
static void
rm_redo_error_callback(void *arg)
{
	XLogReaderState *record = (XLogReaderState *) arg;
	StringInfoData buf;

	initStringInfo(&buf);
	xlog_outdesc(&buf, record);
	xlog_block_info(&buf, record);

	/* translator: %s is a WAL record description */
	errcontext("WAL redo at %X/%X for %s",
			   LSN_FORMAT_ARGS(record->ReadRecPtr),
			   buf.data);

	pfree(buf.data);
}

/*
 * Returns a string describing an XLogRecord, consisting of its identity
 * optionally followed by a colon, a space, and a further description.
 */
void
xlog_outdesc(StringInfo buf, XLogReaderState *record)
{
	RmgrData	rmgr = GetRmgr(XLogRecGetRmid(record));
	uint8		info = XLogRecGetInfo(record);
	const char *id;

	appendStringInfoString(buf, rmgr.rm_name);
	appendStringInfoChar(buf, '/');

	id = rmgr.rm_identify(info);
	if (id == NULL)
		appendStringInfo(buf, "UNKNOWN (%X): ", info & ~XLR_INFO_MASK);
	else
		appendStringInfo(buf, "%s: ", id);

	rmgr.rm_desc(buf, record);
}

#ifdef WAL_DEBUG

static void
xlog_outrec(StringInfo buf, XLogReaderState *record)
{
	appendStringInfo(buf, "prev %X/%X; xid %u",
					 LSN_FORMAT_ARGS(XLogRecGetPrev(record)),
					 XLogRecGetXid(record));

	appendStringInfo(buf, "; len %u",
					 XLogRecGetDataLen(record));

	xlog_block_info(buf, record);
}
#endif							/* WAL_DEBUG */

/*
 * Returns a string giving information about all the blocks in an
 * XLogRecord.
 */
static void
xlog_block_info(StringInfo buf, XLogReaderState *record)
{
	int			block_id;

	/* decode block references */
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		RelFileLocator rlocator;
		ForkNumber	forknum;
		BlockNumber blk;

		if (!XLogRecGetBlockTagExtended(record, block_id,
										&rlocator, &forknum, &blk, NULL))
			continue;

		if (forknum != MAIN_FORKNUM)
			appendStringInfo(buf, "; blkref #%d: rel %u/%u/%u, fork %u, blk %u",
							 block_id,
							 rlocator.spcOid, rlocator.dbOid,
							 rlocator.relNumber,
							 forknum,
							 blk);
		else
			appendStringInfo(buf, "; blkref #%d: rel %u/%u/%u, blk %u",
							 block_id,
							 rlocator.spcOid, rlocator.dbOid,
							 rlocator.relNumber,
							 blk);
		if (XLogRecHasBlockImage(record, block_id))
			appendStringInfoString(buf, " FPW");
	}
}


/*
 * Check that it's OK to switch to new timeline during recovery.
 *
 * 'lsn' is the address of the shutdown checkpoint record we're about to
 * replay. (Currently, timeline can only change at a shutdown checkpoint).
 */
static void
checkTimeLineSwitch(XLogRecPtr lsn, TimeLineID newTLI, TimeLineID prevTLI,
					TimeLineID replayTLI)
{
	/* Check that the record agrees on what the current (old) timeline is */
	if (prevTLI != replayTLI)
		ereport(PANIC,
				(errmsg("unexpected previous timeline ID %u (current timeline ID %u) in checkpoint record",
						prevTLI, replayTLI)));

	/*
	 * The new timeline better be in the list of timelines we expect to see,
	 * according to the timeline history. It should also not decrease.
	 */
	if (newTLI < replayTLI || !tliInHistory(newTLI, expectedTLEs))
		ereport(PANIC,
				(errmsg("unexpected timeline ID %u (after %u) in checkpoint record",
						newTLI, replayTLI)));

	/*
	 * If we have not yet reached min recovery point, and we're about to
	 * switch to a timeline greater than the timeline of the min recovery
	 * point: trouble. After switching to the new timeline, we could not
	 * possibly visit the min recovery point on the correct timeline anymore.
	 * This can happen if there is a newer timeline in the archive that
	 * branched before the timeline the min recovery point is on, and you
	 * attempt to do PITR to the new timeline.
	 */
	if (!XLogRecPtrIsInvalid(minRecoveryPoint) &&
		lsn < minRecoveryPoint &&
		newTLI > minRecoveryPointTLI)
		ereport(PANIC,
				(errmsg("unexpected timeline ID %u in checkpoint record, before reaching minimum recovery point %X/%X on timeline %u",
						newTLI,
						LSN_FORMAT_ARGS(minRecoveryPoint),
						minRecoveryPointTLI)));

	/* Looks good */
}


/*
 * Extract timestamp from WAL record.
 *
 * If the record contains a timestamp, returns true, and saves the timestamp
 * in *recordXtime. If the record type has no timestamp, returns false.
 * Currently, only transaction commit/abort records and restore points contain
 * timestamps.
 */
static bool
getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint8		xact_info = info & XLOG_XACT_OPMASK;
	uint8		rmid = XLogRecGetRmid(record);

	if (rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT)
	{
		*recordXtime = ((xl_restore_point *) XLogRecGetData(record))->rp_time;
		return true;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_COMMIT ||
							   xact_info == XLOG_XACT_COMMIT_PREPARED))
	{
		*recordXtime = ((xl_xact_commit *) XLogRecGetData(record))->xact_time;
		return true;
	}
	if (rmid == RM_XACT_ID && (xact_info == XLOG_XACT_ABORT ||
							   xact_info == XLOG_XACT_ABORT_PREPARED))
	{
		*recordXtime = ((xl_xact_abort *) XLogRecGetData(record))->xact_time;
		return true;
	}
	return false;
}

/*
 * Checks whether the current buffer page and backup page stored in the
 * WAL record are consistent or not. Before comparing the two pages, a
 * masking can be applied to the pages to ignore certain areas like hint bits,
 * unused space between pd_lower and pd_upper among other things. This
 * function should be called once WAL replay has been completed for a
 * given record.
 */
static void
verifyBackupPageConsistency(XLogReaderState *record)
{
	RmgrData	rmgr = GetRmgr(XLogRecGetRmid(record));
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;
	int			block_id;

	/* Records with no backup blocks have no need for consistency checks. */
	if (!XLogRecHasAnyBlockRefs(record))
		return;

	Assert((XLogRecGetInfo(record) & XLR_CHECK_CONSISTENCY) != 0);

	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		Buffer		buf;
		Page		page;

		if (!XLogRecGetBlockTagExtended(record, block_id,
										&rlocator, &forknum, &blkno, NULL))
		{
			/*
			 * WAL record doesn't contain a block reference with the given id.
			 * Do nothing.
			 */
			continue;
		}

		Assert(XLogRecHasBlockImage(record, block_id));

		if (XLogRecBlockImageApply(record, block_id))
		{
			/*
			 * WAL record has already applied the page, so bypass the
			 * consistency check as that would result in comparing the full
			 * page stored in the record with itself.
			 */
			continue;
		}

		/*
		 * Read the contents from the current buffer and store it in a
		 * temporary page.
		 */
		buf = XLogReadBufferExtended(rlocator, forknum, blkno,
									 RBM_NORMAL_NO_LOG,
									 InvalidBuffer);
		if (!BufferIsValid(buf))
			continue;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		/*
		 * Take a copy of the local page where WAL has been applied to have a
		 * comparison base before masking it...
		 */
		memcpy(replay_image_masked, page, BLCKSZ);

		/* No need for this page anymore now that a copy is in. */
		UnlockReleaseBuffer(buf);

		/*
		 * If the block LSN is already ahead of this WAL record, we can't
		 * expect contents to match.  This can happen if recovery is
		 * restarted.
		 */
		if (PageGetLSN(replay_image_masked) > record->EndRecPtr)
			continue;

		/*
		 * Read the contents from the backup copy, stored in WAL record and
		 * store it in a temporary page. There is no need to allocate a new
		 * page here, a local buffer is fine to hold its contents and a mask
		 * can be directly applied on it.
		 */
		if (!RestoreBlockImage(record, block_id, primary_image_masked))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg_internal("%s", record->errormsg_buf)));

		/*
		 * If masking function is defined, mask both the primary and replay
		 * images
		 */
		if (rmgr.rm_mask != NULL)
		{
			rmgr.rm_mask(replay_image_masked, blkno);
			rmgr.rm_mask(primary_image_masked, blkno);
		}

		/* Time to compare the primary and replay images. */
		if (memcmp(replay_image_masked, primary_image_masked, BLCKSZ) != 0)
		{
			elog(FATAL,
				 "inconsistent page found, rel %u/%u/%u, forknum %u, blkno %u",
				 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
				 forknum, blkno);
		}
	}
}

/*
 * For point-in-time recovery, this function decides whether we want to
 * stop applying the XLOG before the current record.
 *
 * Returns true if we are stopping, false otherwise. If stopping, some
 * information is saved in recoveryStopXid et al for use in annotating the
 * new timeline's history file.
 */
static bool
recoveryStopsBefore(XLogReaderState *record)
{
	bool		stopsHere = false;
	uint8		xact_info;
	bool		isCommit;
	TimestampTz recordXtime = 0;
	TransactionId recordXid;

	/*
	 * Ignore recovery target settings when not in archive recovery (meaning
	 * we are in crash recovery).
	 */
	if (!ArchiveRecoveryRequested)
		return false;

	/* Check if we should stop as soon as reaching consistency */
	if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency)
	{
		ereport(LOG,
				(errmsg("recovery stopping after reaching consistency")));

		recoveryStopAfter = false;
		recoveryStopXid = InvalidTransactionId;
		recoveryStopLSN = InvalidXLogRecPtr;
		recoveryStopTime = 0;
		recoveryStopName[0] = '\0';
		return true;
	}

	/* Check if target LSN has been reached */
	if (recoveryTarget == RECOVERY_TARGET_LSN &&
		!recoveryTargetInclusive &&
		record->ReadRecPtr >= recoveryTargetLSN)
	{
		recoveryStopAfter = false;
		recoveryStopXid = InvalidTransactionId;
		recoveryStopLSN = record->ReadRecPtr;
		recoveryStopTime = 0;
		recoveryStopName[0] = '\0';
		ereport(LOG,
				(errmsg("recovery stopping before WAL location (LSN) \"%X/%X\"",
						LSN_FORMAT_ARGS(recoveryStopLSN))));
		return true;
	}

	/* Otherwise we only consider stopping before COMMIT or ABORT records. */
	if (XLogRecGetRmid(record) != RM_XACT_ID)
		return false;

	xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	if (xact_info == XLOG_XACT_COMMIT)
	{
		isCommit = true;
		recordXid = XLogRecGetXid(record);
	}
	else if (xact_info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
		xl_xact_parsed_commit parsed;

		isCommit = true;
		if (!ParseCommitRecord(XLogRecGetInfo(record), xlrec, XLogRecGetDataLen(record), &parsed))
			elog(PANIC, "recovery: invalid prepared commit payload");
		recordXid = parsed.twophase_xid;
	}
	else if (xact_info == XLOG_XACT_ABORT)
	{
		isCommit = false;
		recordXid = XLogRecGetXid(record);
	}
	else if (xact_info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		isCommit = false;
		ParseAbortRecord(XLogRecGetInfo(record),
						 xlrec,
						 &parsed);
		recordXid = parsed.twophase_xid;
	}
	else
		return false;

	if (recoveryTarget == RECOVERY_TARGET_XID && !recoveryTargetInclusive)
	{
		/*
		 * There can be only one transaction end record with this exact
		 * transactionid
		 *
		 * when testing for an xid, we MUST test for equality only, since
		 * transactions are numbered in the order they start, not the order
		 * they complete. A higher numbered xid will complete before you about
		 * 50% of the time...
		 */
		stopsHere = (recordXid == recoveryTargetXid);
	}

	/*
	 * Note: we must fetch recordXtime regardless of recoveryTarget setting.
	 * We don't expect getRecordTimestamp ever to fail, since we already know
	 * this is a commit or abort record; but test its result anyway.
	 */
	if (getRecordTimestamp(record, &recordXtime) &&
		recoveryTarget == RECOVERY_TARGET_TIME)
	{
		/*
		 * There can be many transactions that share the same commit time, so
		 * we stop after the last one, if we are inclusive, or stop at the
		 * first one if we are exclusive
		 */
		if (recoveryTargetInclusive)
			stopsHere = (recordXtime > recoveryTargetTime);
		else
			stopsHere = (recordXtime >= recoveryTargetTime);
	}

	if (stopsHere)
	{
		recoveryStopAfter = false;
		recoveryStopXid = recordXid;
		recoveryStopTime = recordXtime;
		recoveryStopLSN = InvalidXLogRecPtr;
		recoveryStopName[0] = '\0';

		if (isCommit)
		{
			ereport(LOG,
					(errmsg("recovery stopping before commit of transaction %u, time %s",
							recoveryStopXid,
							timestamptz_to_str(recoveryStopTime))));
		}
		else
		{
			ereport(LOG,
					(errmsg("recovery stopping before abort of transaction %u, time %s",
							recoveryStopXid,
							timestamptz_to_str(recoveryStopTime))));
		}
	}

	return stopsHere;
}

/*
 * Same as recoveryStopsBefore, but called after applying the record.
 *
 * We also track the timestamp of the latest applied COMMIT/ABORT
 * record in XLogRecoveryCtl->recoveryLastXTime.
 */
static bool
recoveryStopsAfter(XLogReaderState *record)
{
	uint8		info;
	uint8		xact_info;
	uint8		rmid;
	TimestampTz recordXtime = 0;

	/*
	 * Ignore recovery target settings when not in archive recovery (meaning
	 * we are in crash recovery).
	 */
	if (!ArchiveRecoveryRequested)
		return false;

	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	rmid = XLogRecGetRmid(record);

	/*
	 * There can be many restore points that share the same name; we stop at
	 * the first one.
	 */
	if (recoveryTarget == RECOVERY_TARGET_NAME &&
		rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT)
	{
		xl_restore_point *recordRestorePointData;

		recordRestorePointData = (xl_restore_point *) XLogRecGetData(record);

		if (strcmp(recordRestorePointData->rp_name, recoveryTargetName) == 0)
		{
			recoveryStopAfter = true;
			recoveryStopXid = InvalidTransactionId;
			recoveryStopLSN = InvalidXLogRecPtr;
			(void) getRecordTimestamp(record, &recoveryStopTime);
			strlcpy(recoveryStopName, recordRestorePointData->rp_name, MAXFNAMELEN);

			ereport(LOG,
					(errmsg("recovery stopping at restore point \"%s\", time %s",
							recoveryStopName,
							timestamptz_to_str(recoveryStopTime))));
			return true;
		}
	}

	/* Check if the target LSN has been reached */
	if (recoveryTarget == RECOVERY_TARGET_LSN &&
		recoveryTargetInclusive &&
		record->ReadRecPtr >= recoveryTargetLSN)
	{
		recoveryStopAfter = true;
		recoveryStopXid = InvalidTransactionId;
		recoveryStopLSN = record->ReadRecPtr;
		recoveryStopTime = 0;
		recoveryStopName[0] = '\0';
		ereport(LOG,
				(errmsg("recovery stopping after WAL location (LSN) \"%X/%X\"",
						LSN_FORMAT_ARGS(recoveryStopLSN))));
		return true;
	}

	if (rmid != RM_XACT_ID)
		return false;

	xact_info = info & XLOG_XACT_OPMASK;

	if (xact_info == XLOG_XACT_COMMIT ||
		xact_info == XLOG_XACT_COMMIT_PREPARED ||
		xact_info == XLOG_XACT_ABORT ||
		xact_info == XLOG_XACT_ABORT_PREPARED)
	{
		TransactionId recordXid;

		/* Update the last applied transaction timestamp */
		if (getRecordTimestamp(record, &recordXtime))
			SetLatestXTime(recordXtime);

		/* Extract the XID of the committed/aborted transaction */
		if (xact_info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
			xl_xact_parsed_commit parsed;

			if (!ParseCommitRecord(XLogRecGetInfo(record), xlrec, XLogRecGetDataLen(record), &parsed))
				elog(PANIC, "recovery: invalid prepared commit payload");
			recordXid = parsed.twophase_xid;
		}
		else if (xact_info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
			xl_xact_parsed_abort parsed;

			ParseAbortRecord(XLogRecGetInfo(record),
							 xlrec,
							 &parsed);
			recordXid = parsed.twophase_xid;
		}
		else
			recordXid = XLogRecGetXid(record);

		/*
		 * There can be only one transaction end record with this exact
		 * transactionid
		 *
		 * when testing for an xid, we MUST test for equality only, since
		 * transactions are numbered in the order they start, not the order
		 * they complete. A higher numbered xid will complete before you about
		 * 50% of the time...
		 */
		if (recoveryTarget == RECOVERY_TARGET_XID && recoveryTargetInclusive &&
			recordXid == recoveryTargetXid)
		{
			recoveryStopAfter = true;
			recoveryStopXid = recordXid;
			recoveryStopTime = recordXtime;
			recoveryStopLSN = InvalidXLogRecPtr;
			recoveryStopName[0] = '\0';

			if (xact_info == XLOG_XACT_COMMIT ||
				xact_info == XLOG_XACT_COMMIT_PREPARED)
			{
				ereport(LOG,
						(errmsg("recovery stopping after commit of transaction %u, time %s",
								recoveryStopXid,
								timestamptz_to_str(recoveryStopTime))));
			}
			else if (xact_info == XLOG_XACT_ABORT ||
					 xact_info == XLOG_XACT_ABORT_PREPARED)
			{
				ereport(LOG,
						(errmsg("recovery stopping after abort of transaction %u, time %s",
								recoveryStopXid,
								timestamptz_to_str(recoveryStopTime))));
			}
			return true;
		}
	}

	/* Check if we should stop as soon as reaching consistency */
	if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency)
	{
		ereport(LOG,
				(errmsg("recovery stopping after reaching consistency")));

		recoveryStopAfter = true;
		recoveryStopXid = InvalidTransactionId;
		recoveryStopTime = 0;
		recoveryStopLSN = InvalidXLogRecPtr;
		recoveryStopName[0] = '\0';
		return true;
	}

	return false;
}

/*
 * Create a comment for the history file to explain why and where
 * timeline changed.
 */
static char *
getRecoveryStopReason(void)
{
	char		reason[200];

	if (recoveryTarget == RECOVERY_TARGET_XID)
		snprintf(reason, sizeof(reason),
				 "%s transaction %u",
				 recoveryStopAfter ? "after" : "before",
				 recoveryStopXid);
	else if (recoveryTarget == RECOVERY_TARGET_TIME)
		snprintf(reason, sizeof(reason),
				 "%s %s\n",
				 recoveryStopAfter ? "after" : "before",
				 timestamptz_to_str(recoveryStopTime));
	else if (recoveryTarget == RECOVERY_TARGET_LSN)
		snprintf(reason, sizeof(reason),
				 "%s LSN %X/%X\n",
				 recoveryStopAfter ? "after" : "before",
				 LSN_FORMAT_ARGS(recoveryStopLSN));
	else if (recoveryTarget == RECOVERY_TARGET_NAME)
		snprintf(reason, sizeof(reason),
				 "at restore point \"%s\"",
				 recoveryStopName);
	else if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE)
		snprintf(reason, sizeof(reason), "reached consistency");
	else
		snprintf(reason, sizeof(reason), "no recovery target specified");

	return pstrdup(reason);
}

/*
 * Wait until shared recoveryPauseState is set to RECOVERY_NOT_PAUSED.
 *
 * endOfRecovery is true if the recovery target is reached and
 * the paused state starts at the end of recovery because of
 * recovery_target_action=pause, and false otherwise.
 */
static void
recoveryPausesHere(bool endOfRecovery)
{
	/* Don't pause unless users can connect! */
	if (!LocalHotStandbyActive)
		return;

	/* Don't pause after standby promotion has been triggered */
	if (LocalPromoteIsTriggered)
		return;

	if (endOfRecovery)
		ereport(LOG,
				(errmsg("pausing at the end of recovery"),
				 errhint("Execute pg_wal_replay_resume() to promote.")));
	else
		ereport(LOG,
				(errmsg("recovery has paused"),
				 errhint("Execute pg_wal_replay_resume() to continue.")));

	/* loop until recoveryPauseState is set to RECOVERY_NOT_PAUSED */
	while (GetRecoveryPauseState() != RECOVERY_NOT_PAUSED)
	{
		HandleStartupProcInterrupts();
		if (CheckForStandbyTrigger())
			return;

		/*
		 * If recovery pause is requested then set it paused.  While we are in
		 * the loop, user might resume and pause again so set this every time.
		 */
		ConfirmRecoveryPaused();

		/*
		 * We wait on a condition variable that will wake us as soon as the
		 * pause ends, but we use a timeout so we can check the above exit
		 * condition periodically too.
		 */
		ConditionVariableTimedSleep(&XLogRecoveryCtl->recoveryNotPausedCV, 1000,
									WAIT_EVENT_RECOVERY_PAUSE);
	}
	ConditionVariableCancelSleep();
}

/*
 * When recovery_min_apply_delay is set, we wait long enough to make sure
 * certain record types are applied at least that interval behind the primary.
 *
 * Returns true if we waited.
 *
 * Note that the delay is calculated between the WAL record log time and
 * the current time on standby. We would prefer to keep track of when this
 * standby received each WAL record, which would allow a more consistent
 * approach and one not affected by time synchronisation issues, but that
 * is significantly more effort and complexity for little actual gain in
 * usability.
 */
static bool
recoveryApplyDelay(XLogReaderState *record)
{
	uint8		xact_info;
	TimestampTz xtime;
	TimestampTz delayUntil;
	long		msecs;

	/* nothing to do if no delay configured */
	if (recovery_min_apply_delay <= 0)
		return false;

	/* no delay is applied on a database not yet consistent */
	if (!reachedConsistency)
		return false;

	/* nothing to do if crash recovery is requested */
	if (!ArchiveRecoveryRequested)
		return false;

	/*
	 * Is it a COMMIT record?
	 *
	 * We deliberately choose not to delay aborts since they have no effect on
	 * MVCC. We already allow replay of records that don't have a timestamp,
	 * so there is already opportunity for issues caused by early conflicts on
	 * standbys.
	 */
	if (XLogRecGetRmid(record) != RM_XACT_ID)
		return false;

	xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	if (xact_info != XLOG_XACT_COMMIT &&
		xact_info != XLOG_XACT_COMMIT_PREPARED)
		return false;

	if (!getRecordTimestamp(record, &xtime))
		return false;

	delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay);

	/*
	 * Exit without arming the latch if it's already past time to apply this
	 * record
	 */
	msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), delayUntil);
	if (msecs <= 0)
		return false;

	while (true)
	{
		ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);

		/* This might change recovery_min_apply_delay. */
		HandleStartupProcInterrupts();

		if (CheckForStandbyTrigger())
			break;

		/*
		 * Recalculate delayUntil as recovery_min_apply_delay could have
		 * changed while waiting in this loop.
		 */
		delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay);

		/*
		 * Wait for difference between GetCurrentTimestamp() and delayUntil.
		 */
		msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(),
												delayUntil);

		if (msecs <= 0)
			break;

		elog(DEBUG2, "recovery apply delay %ld milliseconds", msecs);

		(void) WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 msecs,
						 WAIT_EVENT_RECOVERY_APPLY_DELAY);
	}
	return true;
}

/*
 * Get the current state of the recovery pause request.
 */
RecoveryPauseState
GetRecoveryPauseState(void)
{
	RecoveryPauseState state;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	state = XLogRecoveryCtl->recoveryPauseState;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return state;
}

/*
 * Set the recovery pause state.
 *
 * If recovery pause is requested then sets the recovery pause state to
 * 'pause requested' if it is not already 'paused'.  Otherwise, sets it
 * to 'not paused' to resume the recovery.  The recovery pause will be
 * confirmed by the ConfirmRecoveryPaused.
 */
void
SetRecoveryPause(bool recoveryPause)
{
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);

	if (!recoveryPause)
		XLogRecoveryCtl->recoveryPauseState = RECOVERY_NOT_PAUSED;
	else if (XLogRecoveryCtl->recoveryPauseState == RECOVERY_NOT_PAUSED)
		XLogRecoveryCtl->recoveryPauseState = RECOVERY_PAUSE_REQUESTED;

	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (!recoveryPause)
		ConditionVariableBroadcast(&XLogRecoveryCtl->recoveryNotPausedCV);
}

/*
 * Confirm the recovery pause by setting the recovery pause state to
 * RECOVERY_PAUSED.
 */
static void
ConfirmRecoveryPaused(void)
{
	/* If recovery pause is requested then set it paused */
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	if (XLogRecoveryCtl->recoveryPauseState == RECOVERY_PAUSE_REQUESTED)
		XLogRecoveryCtl->recoveryPauseState = RECOVERY_PAUSED;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);
}


/*
 * Attempt to read the next XLOG record.
 *
 * Before first call, the reader needs to be positioned to the first record
 * by calling XLogPrefetcherBeginRead().
 *
 * If no valid record is available, returns NULL, or fails if emode is PANIC.
 * (emode must be either PANIC, LOG). In standby mode, retries until a valid
 * record is available.
 */
static XLogRecord *
ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,
		   bool fetching_ckpt, TimeLineID replayTLI)
{
	XLogRecord *record;
	XLogReaderState *xlogreader = XLogPrefetcherGetReader(xlogprefetcher);
	XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;

	/* Pass through parameters to XLogPageRead */
	private->fetching_ckpt = fetching_ckpt;
	private->emode = emode;
	private->randAccess = (xlogreader->ReadRecPtr == InvalidXLogRecPtr);
	private->replayTLI = replayTLI;

	/* This is the first attempt to read this page. */
	lastSourceFailed = false;

	for (;;)
	{
		char	   *errormsg;

		record = XLogPrefetcherReadRecord(xlogprefetcher, &errormsg);
		if (record == NULL)
		{
			/*
			 * When we find that WAL ends in an incomplete record, keep track
			 * of that record.  After recovery is done, we'll write a record
			 * to indicate to downstream WAL readers that that portion is to
			 * be ignored.
			 *
			 * However, when ArchiveRecoveryRequested = true, we're going to
			 * switch to a new timeline at the end of recovery. We will only
			 * copy WAL over to the new timeline up to the end of the last
			 * complete record, so if we did this, we would later create an
			 * overwrite contrecord in the wrong place, breaking everything.
			 */
			if (!ArchiveRecoveryRequested &&
				!XLogRecPtrIsInvalid(xlogreader->abortedRecPtr))
			{
				abortedRecPtr = xlogreader->abortedRecPtr;
				missingContrecPtr = xlogreader->missingContrecPtr;
			}

			if (readFile >= 0)
			{
				close(readFile);
				readFile = -1;
			}

			/*
			 * We only end up here without a message when XLogPageRead()
			 * failed - in that case we already logged something. In
			 * StandbyMode that only happens if we have been triggered, so we
			 * shouldn't loop anymore in that case.
			 */
			if (errormsg)
				ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
						(errmsg_internal("%s", errormsg) /* already translated */ ));
		}

		/*
		 * Check page TLI is one of the expected values.
		 */
		else if (!tliInHistory(xlogreader->latestPageTLI, expectedTLEs))
		{
			char		fname[MAXFNAMELEN];
			XLogSegNo	segno;
			int32		offset;

			XLByteToSeg(xlogreader->latestPagePtr, segno, wal_segment_size);
			offset = XLogSegmentOffset(xlogreader->latestPagePtr,
									   wal_segment_size);
			XLogFileName(fname, xlogreader->seg.ws_tli, segno,
						 wal_segment_size);
			ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
					(errmsg("unexpected timeline ID %u in WAL segment %s, LSN %X/%X, offset %u",
							xlogreader->latestPageTLI,
							fname,
							LSN_FORMAT_ARGS(xlogreader->latestPagePtr),
							offset)));
			record = NULL;
		}

		if (record)
		{
			/* Great, got a record */
			return record;
		}
		else
		{
			/* No valid record available from this source */
			lastSourceFailed = true;

			/*
			 * If archive recovery was requested, but we were still doing
			 * crash recovery, switch to archive recovery and retry using the
			 * offline archive. We have now replayed all the valid WAL in
			 * pg_wal, so we are presumably now consistent.
			 *
			 * We require that there's at least some valid WAL present in
			 * pg_wal, however (!fetching_ckpt).  We could recover using the
			 * WAL from the archive, even if pg_wal is completely empty, but
			 * we'd have no idea how far we'd have to replay to reach
			 * consistency.  So err on the safe side and give up.
			 */
			if (!InArchiveRecovery && ArchiveRecoveryRequested &&
				!fetching_ckpt)
			{
				ereport(DEBUG1,
						(errmsg_internal("reached end of WAL in pg_wal, entering archive recovery")));
				InArchiveRecovery = true;
				if (StandbyModeRequested)
					EnableStandbyMode();

				SwitchIntoArchiveRecovery(xlogreader->EndRecPtr, replayTLI);
				minRecoveryPoint = xlogreader->EndRecPtr;
				minRecoveryPointTLI = replayTLI;

				CheckRecoveryConsistency();

				/*
				 * Before we retry, reset lastSourceFailed and currentSource
				 * so that we will check the archive next.
				 */
				lastSourceFailed = false;
				currentSource = XLOG_FROM_ANY;

				continue;
			}

			/* In standby mode, loop back to retry. Otherwise, give up. */
			if (StandbyMode && !CheckForStandbyTrigger())
				continue;
			else
				return NULL;
		}
	}
}

/*
 * Read the XLOG page containing targetPagePtr into readBuf (if not read
 * already).  Returns number of bytes read, if the page is read successfully,
 * or XLREAD_FAIL in case of errors.  When errors occur, they are ereport'ed,
 * but only if they have not been previously reported.
 *
 * See XLogReaderRoutine.page_read for more details.
 *
 * While prefetching, xlogreader->nonblocking may be set.  In that case,
 * returns XLREAD_WOULDBLOCK if we'd otherwise have to wait for more WAL.
 *
 * This is responsible for restoring files from archive as needed, as well
 * as for waiting for the requested WAL record to arrive in standby mode.
 *
 * xlogreader->private_data->emode specifies the log level used for reporting
 * "file not found" or "end of WAL" situations in archive recovery, or in
 * standby mode when promotion is triggered. If set to WARNING or below,
 * XLogPageRead() returns XLREAD_FAIL in those situations, on higher log
 * levels the ereport() won't return.
 *
 * In standby mode, if after a successful return of XLogPageRead() the
 * caller finds the record it's interested in to be broken, it should
 * ereport the error with the level determined by
 * emode_for_corrupt_record(), and then set lastSourceFailed
 * and call XLogPageRead() again with the same arguments. This lets
 * XLogPageRead() to try fetching the record from another source, or to
 * sleep and retry.
 */
static int
XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
			 XLogRecPtr targetRecPtr, char *readBuf)
{
	XLogPageReadPrivate *private =
		(XLogPageReadPrivate *) xlogreader->private_data;
	int			emode = private->emode;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo PG_USED_FOR_ASSERTS_ONLY;
	int			r;

	XLByteToSeg(targetPagePtr, targetSegNo, wal_segment_size);
	targetPageOff = XLogSegmentOffset(targetPagePtr, wal_segment_size);

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (readFile >= 0 &&
		!XLByteInSeg(targetPagePtr, readSegNo, wal_segment_size))
	{
		/*
		 * Request a restartpoint if we've replayed too much xlog since the
		 * last one.
		 */
		if (ArchiveRecoveryRequested && IsUnderPostmaster)
		{
			if (XLogCheckpointNeeded(readSegNo))
			{
				(void) GetRedoRecPtr();
				if (XLogCheckpointNeeded(readSegNo))
					RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
			}
		}

		close(readFile);
		readFile = -1;
		readSource = XLOG_FROM_ANY;
	}

	XLByteToSeg(targetPagePtr, readSegNo, wal_segment_size);

retry:
	/* See if we need to retrieve more data */
	if (readFile < 0 ||
		(readSource == XLOG_FROM_STREAM &&
		 flushedUpto < targetPagePtr + reqLen))
	{
		if (readFile >= 0 &&
			xlogreader->nonblocking &&
			readSource == XLOG_FROM_STREAM &&
			flushedUpto < targetPagePtr + reqLen)
			return XLREAD_WOULDBLOCK;

		switch (WaitForWALToBecomeAvailable(targetPagePtr + reqLen,
											private->randAccess,
											private->fetching_ckpt,
											targetRecPtr,
											private->replayTLI,
											xlogreader->EndRecPtr,
											xlogreader->nonblocking))
		{
			case XLREAD_WOULDBLOCK:
				return XLREAD_WOULDBLOCK;
			case XLREAD_FAIL:
				if (readFile >= 0)
					close(readFile);
				readFile = -1;
				readLen = 0;
				readSource = XLOG_FROM_ANY;
				return XLREAD_FAIL;
			case XLREAD_SUCCESS:
				break;
		}
	}

	/*
	 * At this point, we have the right segment open and if we're streaming we
	 * know the requested record is in it.
	 */
	Assert(readFile != -1);

	/*
	 * If the current segment is being streamed from the primary, calculate
	 * how much of the current page we have received already. We know the
	 * requested record has been received, but this is for the benefit of
	 * future calls, to allow quick exit at the top of this function.
	 */
	if (readSource == XLOG_FROM_STREAM)
	{
		if (((targetPagePtr) / XLOG_BLCKSZ) != (flushedUpto / XLOG_BLCKSZ))
			readLen = XLOG_BLCKSZ;
		else
			readLen = XLogSegmentOffset(flushedUpto, wal_segment_size) -
				targetPageOff;
	}
	else
		readLen = XLOG_BLCKSZ;

	/* Read the requested page */
	readOff = targetPageOff;

	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	r = pg_pread(readFile, readBuf, XLOG_BLCKSZ, (off_t) readOff);
	if (r != XLOG_BLCKSZ)
	{
		char		fname[MAXFNAMELEN];
		int			save_errno = errno;

		pgstat_report_wait_end();
		XLogFileName(fname, curFileTLI, readSegNo, wal_segment_size);
		if (r < 0)
		{
			errno = save_errno;
			ereport(emode_for_corrupt_record(emode, targetPagePtr + reqLen),
					(errcode_for_file_access(),
					 errmsg("could not read from WAL segment %s, LSN %X/%X, offset %u: %m",
							fname, LSN_FORMAT_ARGS(targetPagePtr),
							readOff)));
		}
		else
			ereport(emode_for_corrupt_record(emode, targetPagePtr + reqLen),
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read from WAL segment %s, LSN %X/%X, offset %u: read %d of %zu",
							fname, LSN_FORMAT_ARGS(targetPagePtr),
							readOff, r, (Size) XLOG_BLCKSZ)));
		goto next_record_is_invalid;
	}
	pgstat_report_wait_end();

	Assert(targetSegNo == readSegNo);
	Assert(targetPageOff == readOff);
	Assert(reqLen <= readLen);

	xlogreader->seg.ws_tli = curFileTLI;

	/*
	 * Check the page header immediately, so that we can retry immediately if
	 * it's not valid. This may seem unnecessary, because ReadPageInternal()
	 * validates the page header anyway, and would propagate the failure up to
	 * ReadRecord(), which would retry. However, there's a corner case with
	 * continuation records, if a record is split across two pages such that
	 * we would need to read the two pages from different sources across two
	 * WAL segments.
	 *
	 * The first page is only available locally, in pg_wal, because it's
	 * already been recycled on the primary. The second page, however, is not
	 * present in pg_wal, and we should stream it from the primary. There is a
	 * recycled WAL segment present in pg_wal, with garbage contents, however.
	 * We would read the first page from the local WAL segment, but when
	 * reading the second page, we would read the bogus, recycled, WAL
	 * segment. If we didn't catch that case here, we would never recover,
	 * because ReadRecord() would retry reading the whole record from the
	 * beginning.
	 *
	 * Of course, this only catches errors in the page header, which is what
	 * happens in the case of a recycled WAL segment. Other kinds of errors or
	 * corruption still has the same problem. But this at least fixes the
	 * common case, which can happen as part of normal operation.
	 *
	 * Validating the page header is cheap enough that doing it twice
	 * shouldn't be a big deal from a performance point of view.
	 *
	 * When not in standby mode, an invalid page header should cause recovery
	 * to end, not retry reading the page, so we don't need to validate the
	 * page header here for the retry. Instead, ReadPageInternal() is
	 * responsible for the validation.
	 */
	if (StandbyMode &&
		(targetPagePtr % wal_segment_size) == 0 &&
		!XLogReaderValidatePageHeader(xlogreader, targetPagePtr, readBuf))
	{
		/*
		 * Emit this error right now then retry this page immediately. Use
		 * errmsg_internal() because the message was already translated.
		 */
		if (xlogreader->errormsg_buf[0])
			ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
					(errmsg_internal("%s", xlogreader->errormsg_buf)));

		/* reset any error XLogReaderValidatePageHeader() might have set */
		XLogReaderResetError(xlogreader);
		goto next_record_is_invalid;
	}

	return readLen;

next_record_is_invalid:

	/*
	 * If we're reading ahead, give up fast.  Retries and error reporting will
	 * be handled by a later read when recovery catches up to this point.
	 */
	if (xlogreader->nonblocking)
		return XLREAD_WOULDBLOCK;

	lastSourceFailed = true;

	if (readFile >= 0)
		close(readFile);
	readFile = -1;
	readLen = 0;
	readSource = XLOG_FROM_ANY;

	/* In standby-mode, keep trying */
	if (StandbyMode)
		goto retry;
	else
		return XLREAD_FAIL;
}

/*
 * Open the WAL segment containing WAL location 'RecPtr'.
 *
 * The segment can be fetched via restore_command, or via walreceiver having
 * streamed the record, or it can already be present in pg_wal. Checking
 * pg_wal is mainly for crash recovery, but it will be polled in standby mode
 * too, in case someone copies a new segment directly to pg_wal. That is not
 * documented or recommended, though.
 *
 * If 'fetching_ckpt' is true, we're fetching a checkpoint record, and should
 * prepare to read WAL starting from RedoStartLSN after this.
 *
 * 'RecPtr' might not point to the beginning of the record we're interested
 * in, it might also point to the page or segment header. In that case,
 * 'tliRecPtr' is the position of the WAL record we're interested in. It is
 * used to decide which timeline to stream the requested WAL from.
 *
 * 'replayLSN' is the current replay LSN, so that if we scan for new
 * timelines, we can reject a switch to a timeline that branched off before
 * this point.
 *
 * If the record is not immediately available, the function returns false
 * if we're not in standby mode. In standby mode, waits for it to become
 * available.
 *
 * When the requested record becomes available, the function opens the file
 * containing it (if not open already), and returns XLREAD_SUCCESS. When end
 * of standby mode is triggered by the user, and there is no more WAL
 * available, returns XLREAD_FAIL.
 *
 * If nonblocking is true, then give up immediately if we can't satisfy the
 * request, returning XLREAD_WOULDBLOCK instead of waiting.
 */
static XLogPageReadResult
WaitForWALToBecomeAvailable(XLogRecPtr RecPtr, bool randAccess,
							bool fetching_ckpt, XLogRecPtr tliRecPtr,
							TimeLineID replayTLI, XLogRecPtr replayLSN,
							bool nonblocking)
{
	static TimestampTz last_fail_time = 0;
	TimestampTz now;
	bool		streaming_reply_sent = false;

	/*-------
	 * Standby mode is implemented by a state machine:
	 *
	 * 1. Read from either archive or pg_wal (XLOG_FROM_ARCHIVE), or just
	 *	  pg_wal (XLOG_FROM_PG_WAL)
	 * 2. Check for promotion trigger request
	 * 3. Read from primary server via walreceiver (XLOG_FROM_STREAM)
	 * 4. Rescan timelines
	 * 5. Sleep wal_retrieve_retry_interval milliseconds, and loop back to 1.
	 *
	 * Failure to read from the current source advances the state machine to
	 * the next state.
	 *
	 * 'currentSource' indicates the current state. There are no currentSource
	 * values for "check trigger", "rescan timelines", and "sleep" states,
	 * those actions are taken when reading from the previous source fails, as
	 * part of advancing to the next state.
	 *
	 * If standby mode is turned off while reading WAL from stream, we move
	 * to XLOG_FROM_ARCHIVE and reset lastSourceFailed, to force fetching
	 * the files (which would be required at end of recovery, e.g., timeline
	 * history file) from archive or pg_wal. We don't need to kill WAL receiver
	 * here because it's already stopped when standby mode is turned off at
	 * the end of recovery.
	 *-------
	 */
	if (!InArchiveRecovery)
		currentSource = XLOG_FROM_PG_WAL;
	else if (currentSource == XLOG_FROM_ANY ||
			 (!StandbyMode && currentSource == XLOG_FROM_STREAM))
	{
		lastSourceFailed = false;
		currentSource = XLOG_FROM_ARCHIVE;
	}

	for (;;)
	{
		XLogSource	oldSource = currentSource;
		bool		startWalReceiver = false;

		/*
		 * First check if we failed to read from the current source, and
		 * advance the state machine if so. The failure to read might've
		 * happened outside this function, e.g when a CRC check fails on a
		 * record, or within this loop.
		 */
		if (lastSourceFailed)
		{
			/*
			 * Don't allow any retry loops to occur during nonblocking
			 * readahead.  Let the caller process everything that has been
			 * decoded already first.
			 */
			if (nonblocking)
				return XLREAD_WOULDBLOCK;

			switch (currentSource)
			{
				case XLOG_FROM_ARCHIVE:
				case XLOG_FROM_PG_WAL:

					/*
					 * Check to see if promotion is requested. Note that we do
					 * this only after failure, so when you promote, we still
					 * finish replaying as much as we can from archive and
					 * pg_wal before failover.
					 */
					if (StandbyMode && CheckForStandbyTrigger())
					{
						XLogShutdownWalRcv();
						return XLREAD_FAIL;
					}

					/*
					 * Not in standby mode, and we've now tried the archive
					 * and pg_wal.
					 */
					if (!StandbyMode)
						return XLREAD_FAIL;

					/*
					 * Move to XLOG_FROM_STREAM state, and set to start a
					 * walreceiver if necessary.
					 */
					currentSource = XLOG_FROM_STREAM;
					startWalReceiver = true;
					break;

				case XLOG_FROM_STREAM:

					/*
					 * Failure while streaming. Most likely, we got here
					 * because streaming replication was terminated, or
					 * promotion was triggered. But we also get here if we
					 * find an invalid record in the WAL streamed from the
					 * primary, in which case something is seriously wrong.
					 * There's little chance that the problem will just go
					 * away, but PANIC is not good for availability either,
					 * especially in hot standby mode. So, we treat that the
					 * same as disconnection, and retry from archive/pg_wal
					 * again. The WAL in the archive should be identical to
					 * what was streamed, so it's unlikely that it helps, but
					 * one can hope...
					 */

					/*
					 * We should be able to move to XLOG_FROM_STREAM only in
					 * standby mode.
					 */
					Assert(StandbyMode);

					/*
					 * Before we leave XLOG_FROM_STREAM state, make sure that
					 * walreceiver is not active, so that it won't overwrite
					 * WAL that we restore from archive.
					 *
					 * If walreceiver is actively streaming (or attempting to
					 * connect), we must shut it down. However, if it's
					 * already in WAITING state (e.g., due to timeline
					 * divergence), we only need to reset the install flag to
					 * allow archive restoration.
					 */
					if (WalRcvStreaming())
						XLogShutdownWalRcv();
					else
					{
						ResetInstallXLogFileSegmentActive();
					}

					/*
					 * Before we sleep, re-scan for possible new timelines if
					 * we were requested to recover to the latest timeline.
					 */
					if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST)
					{
						if (rescanLatestTimeLine(replayTLI, replayLSN))
						{
							currentSource = XLOG_FROM_ARCHIVE;
							break;
						}
					}

					/*
					 * XLOG_FROM_STREAM is the last state in our state
					 * machine, so we've exhausted all the options for
					 * obtaining the requested WAL. We're going to loop back
					 * and retry from the archive, but if it hasn't been long
					 * since last attempt, sleep wal_retrieve_retry_interval
					 * milliseconds to avoid busy-waiting.
					 */
					now = GetCurrentTimestamp();
					if (!TimestampDifferenceExceeds(last_fail_time, now,
													wal_retrieve_retry_interval))
					{
						long		wait_time;

						wait_time = wal_retrieve_retry_interval -
							TimestampDifferenceMilliseconds(last_fail_time, now);

						elog(LOG, "waiting for WAL to become available at %X/%X",
							 LSN_FORMAT_ARGS(RecPtr));

						/* Do background tasks that might benefit us later. */
						KnownAssignedTransactionIdsIdleMaintenance();

						(void) WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
										 WL_LATCH_SET | WL_TIMEOUT |
										 WL_EXIT_ON_PM_DEATH,
										 wait_time,
										 WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL);
						ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
						now = GetCurrentTimestamp();

						/* Handle interrupt signals of startup process */
						HandleStartupProcInterrupts();
					}
					last_fail_time = now;
					currentSource = XLOG_FROM_ARCHIVE;
					break;

				default:
					elog(ERROR, "unexpected WAL source %d", currentSource);
			}
		}
		else if (currentSource == XLOG_FROM_PG_WAL)
		{
			/*
			 * We just successfully read a file in pg_wal. We prefer files in
			 * the archive over ones in pg_wal, so try the next file again
			 * from the archive first.
			 */
			if (InArchiveRecovery)
				currentSource = XLOG_FROM_ARCHIVE;
		}

		if (currentSource != oldSource)
			elog(DEBUG2, "switched WAL source from %s to %s after %s",
				 xlogSourceNames[oldSource], xlogSourceNames[currentSource],
				 lastSourceFailed ? "failure" : "success");

		/*
		 * We've now handled possible failure. Try to read from the chosen
		 * source.
		 */
		lastSourceFailed = false;

		switch (currentSource)
		{
			case XLOG_FROM_ARCHIVE:
			case XLOG_FROM_PG_WAL:

				/*
				 * WAL receiver must not be running when reading WAL from
				 * archive or pg_wal.
				 */
				Assert(!WalRcvStreaming());

				/* Close any old file we might have open. */
				if (readFile >= 0)
				{
					close(readFile);
					readFile = -1;
				}
				/* Reset curFileTLI if random fetch. */
				if (randAccess)
					curFileTLI = 0;

				/*
				 * Try to restore the file from archive, or read an existing
				 * file from pg_wal.
				 */
				readFile = XLogFileReadAnyTLI(readSegNo, DEBUG2,
											  currentSource == XLOG_FROM_ARCHIVE ? XLOG_FROM_ANY :
											  currentSource);
				if (readFile >= 0)
					return XLREAD_SUCCESS;	/* success! */

				/*
				 * Nope, not found in archive or pg_wal.
				 */
				lastSourceFailed = true;
				break;

			case XLOG_FROM_STREAM:
				{
					bool		havedata;

					/*
					 * We should be able to move to XLOG_FROM_STREAM only in
					 * standby mode.
					 */
					Assert(StandbyMode);

					/*
					 * First, shutdown walreceiver if its restart has been
					 * requested -- but no point if we're already slated for
					 * starting it.
					 */
					if (pendingWalRcvRestart && !startWalReceiver)
					{
						XLogShutdownWalRcv();

						/*
						 * Re-scan for possible new timelines if we were
						 * requested to recover to the latest timeline.
						 */
						if (recoveryTargetTimeLineGoal ==
							RECOVERY_TARGET_TIMELINE_LATEST)
							rescanLatestTimeLine(replayTLI, replayLSN);

						startWalReceiver = true;
					}
					pendingWalRcvRestart = false;

					/*
					 * Launch walreceiver if needed.
					 *
					 * If fetching_ckpt is true, RecPtr points to the initial
					 * checkpoint location. In that case, we use RedoStartLSN
					 * as the streaming start position instead of RecPtr, so
					 * that when we later jump backwards to start redo at
					 * RedoStartLSN, we will have the logs streamed already.
					 */
					if (startWalReceiver &&
						PrimaryConnInfo && strcmp(PrimaryConnInfo, "") != 0)
					{
						XLogRecPtr	ptr;
						TimeLineID	tli;

						if (fetching_ckpt)
						{
							ptr = RedoStartLSN;
							tli = RedoStartTLI;
						}
						else
						{
							ptr = RecPtr;

							/*
							 * Use the record begin position to determine the
							 * TLI, rather than the position we're reading.
							 */
							tli = tliOfPointInHistory(tliRecPtr, expectedTLEs);

							if (curFileTLI > 0 && tli < curFileTLI)
								elog(ERROR, "according to history file, WAL location %X/%X belongs to timeline %u, but previous recovered WAL file came from timeline %u",
									 LSN_FORMAT_ARGS(tliRecPtr),
									 tli, curFileTLI);
						}
						curFileTLI = tli;
						SetInstallXLogFileSegmentActive();
						RequestXLogStreaming(tli, ptr, PrimaryConnInfo,
											 PrimarySlotName,
											 wal_receiver_create_temp_slot);
						flushedUpto = 0;
					}

					/*
					 * Check if WAL receiver is active or wait to start up.
					 */
					if (!WalRcvStreaming())
					{
						lastSourceFailed = true;
						break;
					}

					/*
					 * Walreceiver is active, so see if new data has arrived.
					 *
					 * We only advance XLogReceiptTime when we obtain fresh
					 * WAL from walreceiver and observe that we had already
					 * processed everything before the most recent "chunk"
					 * that it flushed to disk.  In steady state where we are
					 * keeping up with the incoming data, XLogReceiptTime will
					 * be updated on each cycle. When we are behind,
					 * XLogReceiptTime will not advance, so the grace time
					 * allotted to conflicting queries will decrease.
					 */
					if (RecPtr < flushedUpto)
						havedata = true;
					else
					{
						XLogRecPtr	latestChunkStart;

						flushedUpto = GetWalRcvFlushRecPtr(&latestChunkStart, &receiveTLI);
						if (RecPtr < flushedUpto && receiveTLI == curFileTLI)
						{
							havedata = true;
							if (latestChunkStart <= RecPtr)
							{
								XLogReceiptTime = GetCurrentTimestamp();
								SetCurrentChunkStartTime(XLogReceiptTime);
							}
						}
						else
							havedata = false;
					}
					if (havedata)
					{
						/*
						 * Great, streamed far enough.  Open the file if it's
						 * not open already.  Also read the timeline history
						 * file if we haven't initialized timeline history
						 * yet; it should be streamed over and present in
						 * pg_wal by now.  Use XLOG_FROM_STREAM so that source
						 * info is set correctly and XLogReceiptTime isn't
						 * changed.
						 *
						 * NB: We must set readTimeLineHistory based on
						 * recoveryTargetTLI, not receiveTLI. Normally they'll
						 * be the same, but if recovery_target_timeline is
						 * 'latest' and archiving is configured, then it's
						 * possible that we managed to retrieve one or more
						 * new timeline history files from the archive,
						 * updating recoveryTargetTLI.
						 */
						if (readFile < 0)
						{
							if (!expectedTLEs)
								expectedTLEs = readTimeLineHistory(recoveryTargetTLI);
							readFile = XLogFileRead(readSegNo, PANIC,
													receiveTLI,
													XLOG_FROM_STREAM, false);
							Assert(readFile >= 0);
						}
						else
						{
							/* just make sure source info is correct... */
							readSource = XLOG_FROM_STREAM;
							XLogReceiptSource = XLOG_FROM_STREAM;
							return XLREAD_SUCCESS;
						}
						break;
					}

					/* In nonblocking mode, return rather than sleeping. */
					if (nonblocking)
						return XLREAD_WOULDBLOCK;

					/*
					 * Data not here yet. Check for trigger, then wait for
					 * walreceiver to wake us up when new WAL arrives.
					 */
					if (CheckForStandbyTrigger())
					{
						/*
						 * Note that we don't return XLREAD_FAIL immediately
						 * here. After being triggered, we still want to
						 * replay all the WAL that was already streamed. It's
						 * in pg_wal now, so we just treat this as a failure,
						 * and the state machine will move on to replay the
						 * streamed WAL from pg_wal, and then recheck the
						 * trigger and exit replay.
						 */
						lastSourceFailed = true;
						break;
					}

					/*
					 * Since we have replayed everything we have received so
					 * far and are about to start waiting for more WAL, let's
					 * tell the upstream server our replay location now so
					 * that pg_stat_replication doesn't show stale
					 * information.
					 */
					if (!streaming_reply_sent)
					{
						WalRcvForceReply();
						streaming_reply_sent = true;
					}

					/* Do any background tasks that might benefit us later. */
					KnownAssignedTransactionIdsIdleMaintenance();

					/* Update pg_stat_recovery_prefetch before sleeping. */
					XLogPrefetcherComputeStats(xlogprefetcher);

					/*
					 * Wait for more WAL to arrive, when we will be woken
					 * immediately by the WAL receiver.
					 */
					(void) WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
									 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
									 -1L,
									 WAIT_EVENT_RECOVERY_WAL_STREAM);
					ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
					break;
				}

			default:
				elog(ERROR, "unexpected WAL source %d", currentSource);
		}

		/*
		 * Check for recovery pause here so that we can confirm more quickly
		 * that a requested pause has actually taken effect.
		 */
		if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState !=
			RECOVERY_NOT_PAUSED)
			recoveryPausesHere(false);

		/*
		 * This possibly-long loop needs to handle interrupts of startup
		 * process.
		 */
		HandleStartupProcInterrupts();
	}

	return XLREAD_FAIL;			/* not reached */
}


/*
 * Determine what log level should be used to report a corrupt WAL record
 * in the current WAL page, previously read by XLogPageRead().
 *
 * 'emode' is the error mode that would be used to report a file-not-found
 * or legitimate end-of-WAL situation.   Generally, we use it as-is, but if
 * we're retrying the exact same record that we've tried previously, only
 * complain the first time to keep the noise down.  However, we only do when
 * reading from pg_wal, because we don't expect any invalid records in archive
 * or in records streamed from the primary. Files in the archive should be complete,
 * and we should never hit the end of WAL because we stop and wait for more WAL
 * to arrive before replaying it.
 *
 * NOTE: This function remembers the RecPtr value it was last called with,
 * to suppress repeated messages about the same record. Only call this when
 * you are about to ereport(), or you might cause a later message to be
 * erroneously suppressed.
 */
static int
emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)
{
	static XLogRecPtr lastComplaint = 0;

	if (readSource == XLOG_FROM_PG_WAL && emode == LOG)
	{
		if (RecPtr == lastComplaint)
			emode = DEBUG1;
		else
			lastComplaint = RecPtr;
	}
	return emode;
}


/*
 * Subroutine to try to fetch and validate a prior checkpoint record.
 */
static XLogRecord *
ReadCheckpointRecord(XLogPrefetcher *xlogprefetcher, XLogRecPtr RecPtr,
					 TimeLineID replayTLI)
{
	XLogRecord *record;
	uint8		info;

	Assert(xlogreader != NULL);

	if (!XRecOffIsValid(RecPtr))
	{
		ereport(LOG,
				(errmsg("invalid checkpoint location")));
		return NULL;
	}

	XLogPrefetcherBeginRead(xlogprefetcher, RecPtr);
	record = ReadRecord(xlogprefetcher, LOG, true, replayTLI);

	if (record == NULL)
	{
		ereport(LOG,
				(errmsg("invalid checkpoint record")));
		return NULL;
	}
	if (record->xl_rmid != RM_XLOG_ID)
	{
		ereport(LOG,
				(errmsg("invalid resource manager ID in checkpoint record")));
		return NULL;
	}
	info = record->xl_info & ~XLR_INFO_MASK;
	if (info != XLOG_CHECKPOINT_SHUTDOWN &&
		info != XLOG_CHECKPOINT_ONLINE)
	{
		ereport(LOG,
				(errmsg("invalid xl_info in checkpoint record")));
		return NULL;
	}
	if (record->xl_tot_len != SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint))
	{
		ereport(LOG,
				(errmsg("invalid length of checkpoint record")));
		return NULL;
	}
	return record;
}

/*
 * Scan for new timelines that might have appeared in the archive since we
 * started recovery.
 *
 * If there are any, the function changes recovery target TLI to the latest
 * one and returns 'true'.
 */
static bool
rescanLatestTimeLine(TimeLineID replayTLI, XLogRecPtr replayLSN)
{
	List	   *newExpectedTLEs;
	bool		found;
	ListCell   *cell;
	TimeLineID	newtarget;
	TimeLineID	oldtarget = recoveryTargetTLI;
	TimeLineHistoryEntry *currentTle = NULL;

	newtarget = findNewestTimeLine(recoveryTargetTLI);
	if (newtarget == recoveryTargetTLI)
	{
		/* No new timelines found */
		return false;
	}

	/*
	 * Determine the list of expected TLIs for the new TLI
	 */

	newExpectedTLEs = readTimeLineHistory(newtarget);

	/*
	 * If the current timeline is not part of the history of the new timeline,
	 * we cannot proceed to it.
	 */
	found = false;
	foreach(cell, newExpectedTLEs)
	{
		currentTle = (TimeLineHistoryEntry *) lfirst(cell);

		if (currentTle->tli == recoveryTargetTLI)
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		ereport(LOG,
				(errmsg("new timeline %u is not a child of database system timeline %u",
						newtarget,
						replayTLI)));
		return false;
	}

	/*
	 * The current timeline was found in the history file, but check that the
	 * next timeline was forked off from it *after* the current recovery
	 * location.
	 */
	if (currentTle->end < replayLSN)
	{
		ereport(LOG,
				(errmsg("new timeline %u forked off current database system timeline %u before current recovery point %X/%X",
						newtarget,
						replayTLI,
						LSN_FORMAT_ARGS(replayLSN))));
		return false;
	}

	/* The new timeline history seems valid. Switch target */
	recoveryTargetTLI = newtarget;
	list_free_deep(expectedTLEs);
	expectedTLEs = newExpectedTLEs;

	/*
	 * As in StartupXLOG(), try to ensure we have all the history files
	 * between the old target and new target in pg_wal.
	 */
	restoreTimeLineHistoryFiles(oldtarget + 1, newtarget);

	ereport(LOG,
			(errmsg("new target timeline is %u",
					recoveryTargetTLI)));

	return true;
}


/*
 * Open a logfile segment for reading (during recovery).
 *
 * If source == XLOG_FROM_ARCHIVE, the segment is retrieved from archive.
 * Otherwise, it's assumed to be already available in pg_wal.
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 * What changed: shared-config startup opens the selected read-only input.
 * Why: the writable route must not substitute for retained restart WAL.
 */
static int
XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli,
			 XLogSource source, bool notfoundOk)
{
	char		xlogfname[MAXFNAMELEN];
	char		activitymsg[MAXFNAMELEN + 16];
	char		path[MAXPGPATH];
	int			fd;

	XLogFileName(xlogfname, tli, segno, wal_segment_size);

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: refuse alternate sources before archive restore can mutate pg_wal.
	 * An absent segment may be native EOF; an absent identity never is. */
	if (cluster_enabled && cluster_shared_config)
	{
		ClusterWalSourceRef input;
		ClusterControlRootResult result;

		if (source != XLOG_FROM_PG_WAL || !cluster_wal_thread_restart_v2_ref(&input))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("selected restart WAL input is unavailable"),
					 errhint("Verify the selected WAL generation before restarting; do not substitute local or archived WAL.")));
		result = cluster_wal_restart_segment_open(cluster_wal_threads_dir, &input,
												 tli, segno, wal_segment_size, &fd);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && result != CLUSTER_CONTROL_ROOT_ABSENT)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not validate selected restart WAL segment"),
					 errdetail("Thread %u, timeline %u, result %d.",
							   input.claim.identity.origin_thread_id, tli, result),
					 errhint("Preserve the selected generation and check its claim and storage; no fallback is permitted.")));
		snprintf(path, sizeof(path), "selected restart WAL/%s", xlogfname);
		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			errno = ENOENT;
		goto file_opened;
	}
#endif

	switch (source)
	{
		case XLOG_FROM_ARCHIVE:
			/* Report recovery progress in PS display */
			snprintf(activitymsg, sizeof(activitymsg), "waiting for %s",
					 xlogfname);
			set_ps_display(activitymsg);

			if (!RestoreArchivedFile(path, xlogfname,
									 "RECOVERYXLOG",
									 wal_segment_size,
									 InRedo))
				return -1;
			break;

		case XLOG_FROM_PG_WAL:
		case XLOG_FROM_STREAM:
			XLogFilePath(path, tli, segno, wal_segment_size);
			break;

		default:
			elog(ERROR, "invalid XLogFileRead source %d", source);
	}

	/*
	 * If the segment was fetched from archival storage, replace the existing
	 * xlog segment (if any) with the archival version.
	 */
	if (source == XLOG_FROM_ARCHIVE)
	{
		Assert(!IsInstallXLogFileSegmentActive());
		KeepFileRestoredFromArchive(path, xlogfname);

		/*
		 * Set path to point at the new file in pg_wal.
		 */
		snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
	}

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
#ifdef USE_PGRAC_CLUSTER
file_opened:
#endif
	if (fd >= 0)
	{
		/* Success! */
		curFileTLI = tli;

		/* Report recovery progress in PS display */
		snprintf(activitymsg, sizeof(activitymsg), "recovering %s",
				 xlogfname);
		set_ps_display(activitymsg);

		/* Track source of data in assorted state variables */
		readSource = source;
		XLogReceiptSource = source;
		/* In FROM_STREAM case, caller tracks receipt time, not me */
		if (source != XLOG_FROM_STREAM)
			XLogReceiptTime = GetCurrentTimestamp();

		return fd;
	}
	if (errno != ENOENT || !notfoundOk) /* unexpected failure? */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	return -1;
}

/*
 * Open a logfile segment for reading (during recovery).
 *
 * This version searches for the segment with any TLI listed in expectedTLEs.
 */
static int
XLogFileReadAnyTLI(XLogSegNo segno, int emode, XLogSource source)
{
	char		path[MAXPGPATH];
	ListCell   *cell;
	int			fd;
	List	   *tles;

	/*
	 * Loop looking for a suitable timeline ID: we might need to read any of
	 * the timelines listed in expectedTLEs.
	 *
	 * We expect curFileTLI on entry to be the TLI of the preceding file in
	 * sequence, or 0 if there was no predecessor.  We do not allow curFileTLI
	 * to go backwards; this prevents us from picking up the wrong file when a
	 * parent timeline extends to higher segment numbers than the child we
	 * want to read.
	 *
	 * If we haven't read the timeline history file yet, read it now, so that
	 * we know which TLIs to scan.  We don't save the list in expectedTLEs,
	 * however, unless we actually find a valid segment.  That way if there is
	 * neither a timeline history file nor a WAL segment in the archive, and
	 * streaming replication is set up, we'll read the timeline history file
	 * streamed from the primary when we start streaming, instead of
	 * recovering with a dummy history generated here.
	 */
	if (expectedTLEs)
		tles = expectedTLEs;
	else
		tles = readTimeLineHistory(recoveryTargetTLI);

	foreach(cell, tles)
	{
		TimeLineHistoryEntry *hent = (TimeLineHistoryEntry *) lfirst(cell);
		TimeLineID	tli = hent->tli;

		if (tli < curFileTLI)
			break;				/* don't bother looking at too-old TLIs */

		/*
		 * Skip scanning the timeline ID that the logfile segment to read
		 * doesn't belong to
		 */
		if (hent->begin != InvalidXLogRecPtr)
		{
			XLogSegNo	beginseg = 0;

			XLByteToSeg(hent->begin, beginseg, wal_segment_size);

			/*
			 * The logfile segment that doesn't belong to the timeline is
			 * older or newer than the segment that the timeline started or
			 * ended at, respectively. It's sufficient to check only the
			 * starting segment of the timeline here. Since the timelines are
			 * scanned in descending order in this loop, any segments newer
			 * than the ending segment should belong to newer timeline and
			 * have already been read before. So it's not necessary to check
			 * the ending segment of the timeline here.
			 */
			if (segno < beginseg)
				continue;
		}

		if (source == XLOG_FROM_ANY || source == XLOG_FROM_ARCHIVE)
		{
			fd = XLogFileRead(segno, emode, tli,
							  XLOG_FROM_ARCHIVE, true);
			if (fd != -1)
			{
				elog(DEBUG1, "got WAL segment from archive");
				if (!expectedTLEs)
					expectedTLEs = tles;
				return fd;
			}
		}

		if (source == XLOG_FROM_ANY || source == XLOG_FROM_PG_WAL)
		{
			fd = XLogFileRead(segno, emode, tli,
							  XLOG_FROM_PG_WAL, true);
			if (fd != -1)
			{
				if (!expectedTLEs)
					expectedTLEs = tles;
				return fd;
			}
		}
	}

	/* Couldn't find it.  For simplicity, complain about front timeline */
	XLogFilePath(path, recoveryTargetTLI, segno, wal_segment_size);
	errno = ENOENT;
	ereport(emode,
			(errcode_for_file_access(),
			 errmsg("could not open file \"%s\": %m", path)));
	return -1;
}

/*
 * Set flag to signal the walreceiver to restart.  (The startup process calls
 * this on noticing a relevant configuration change.)
 */
void
StartupRequestWalReceiverRestart(void)
{
	if (currentSource == XLOG_FROM_STREAM && WalRcvRunning())
	{
		ereport(LOG,
				(errmsg("WAL receiver process shutdown requested")));

		pendingWalRcvRestart = true;
	}
}


/*
 * Has a standby promotion already been triggered?
 *
 * Unlike CheckForStandbyTrigger(), this works in any process
 * that's connected to shared memory.
 */
bool
PromoteIsTriggered(void)
{
	/*
	 * We check shared state each time only until a standby promotion is
	 * triggered. We can't trigger a promotion again, so there's no need to
	 * keep checking after the shared variable has once been seen true.
	 */
	if (LocalPromoteIsTriggered)
		return true;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	LocalPromoteIsTriggered = XLogRecoveryCtl->SharedPromoteIsTriggered;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return LocalPromoteIsTriggered;
}

static void
SetPromoteIsTriggered(void)
{
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	XLogRecoveryCtl->SharedPromoteIsTriggered = true;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	/*
	 * Mark the recovery pause state as 'not paused' because the paused state
	 * ends and promotion continues if a promotion is triggered while recovery
	 * is paused. Otherwise pg_get_wal_replay_pause_state() can mistakenly
	 * return 'paused' while a promotion is ongoing.
	 */
	SetRecoveryPause(false);

	LocalPromoteIsTriggered = true;
}

/*
 * Check whether a promote request has arrived.
 */
static bool
CheckForStandbyTrigger(void)
{
	if (LocalPromoteIsTriggered)
		return true;

	if (IsPromoteSignaled() && CheckPromoteSignal())
	{
#ifdef USE_PGRAC_CLUSTER
		/*
		 * spec-6.4 P1-b: only the current Apply Master may end recovery.  A
		 * non-master standby's replay position is arbitrarily stale, so
		 * promoting it would rewind shared storage to that stale point and
		 * shadow everything the Apply Master wrote since.  Fail closed and
		 * leave the promote signal removed so the node does not FATAL-loop.
		 */
		if (cluster_mrp_should_start()
			&& !(cluster_apply_master_is_self()
				 && cluster_apply_master_term_still_valid(cluster_apply_master_current_term())))
		{
			ereport(LOG, (errmsg("received promote request")));
			RemovePromoteSignalFiles();
			ResetPromoteSignaled();
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_ADG_NOT_APPLY_MASTER),
					 errmsg("promotion refused: this node is not the ADG apply master"),
					 errhint("Promote the node that currently holds the apply-master lease, "
							 "or wait until this node is elected Apply Master.")));
		}
#endif
		ereport(LOG, (errmsg("received promote request")));
		RemovePromoteSignalFiles();
		ResetPromoteSignaled();
		SetPromoteIsTriggered();
		return true;
	}

	return false;
}

/*
 * Remove the files signaling a standby promotion request.
 */
void
RemovePromoteSignalFiles(void)
{
	unlink(PROMOTE_SIGNAL_FILE);
}

/*
 * Check to see if a promote request has arrived.
 */
bool
CheckPromoteSignal(void)
{
	struct stat stat_buf;

	if (stat(PROMOTE_SIGNAL_FILE, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * Wake up startup process to replay newly arrived WAL, or to notice that
 * failover has been requested.
 */
void
WakeupRecovery(void)
{
	SetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
}

/*
 * Schedule a walreceiver wakeup in the main recovery loop.
 */
void
XLogRequestWalReceiverReply(void)
{
	doRequestWalReceiverReply = true;
}

/*
 * Is HotStandby active yet? This is only important in special backends
 * since normal backends won't ever be able to connect until this returns
 * true. Postmaster knows this by way of signal, not via shared memory.
 *
 * Unlike testing standbyState, this works in any process that's connected to
 * shared memory.  (And note that standbyState alone doesn't tell the truth
 * anyway.)
 */
bool
HotStandbyActive(void)
{
	/*
	 * We check shared state each time only until Hot Standby is active. We
	 * can't de-activate Hot Standby, so there's no need to keep checking
	 * after the shared variable has once been seen true.
	 */
	if (LocalHotStandbyActive)
		return true;
	else
	{
		/* spinlock is essential on machines with weak memory ordering! */
		SpinLockAcquire(&XLogRecoveryCtl->info_lck);
		LocalHotStandbyActive = XLogRecoveryCtl->SharedHotStandbyActive;
		SpinLockRelease(&XLogRecoveryCtl->info_lck);

		return LocalHotStandbyActive;
	}
}

/*
 * Like HotStandbyActive(), but to be used only in WAL replay code,
 * where we don't need to ask any other process what the state is.
 */
static bool
HotStandbyActiveInReplay(void)
{
	Assert(AmStartupProcess() || !IsPostmasterEnvironment);
	return LocalHotStandbyActive;
}

/*
 * Get latest redo apply position.
 *
 * Exported to allow WALReceiver to read the pointer directly.
 */
XLogRecPtr
GetXLogReplayRecPtr(TimeLineID *replayTLI)
{
	XLogRecPtr	recptr;
	TimeLineID	tli;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	recptr = XLogRecoveryCtl->lastReplayedEndRecPtr;
	tli = XLogRecoveryCtl->lastReplayedTLI;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (replayTLI)
		*replayTLI = tli;
	return recptr;
}


/*
 * Get position of last applied, or the record being applied.
 *
 * This is different from GetXLogReplayRecPtr() in that if a WAL
 * record is currently being applied, this includes that record.
 */
XLogRecPtr
GetCurrentReplayRecPtr(TimeLineID *replayEndTLI)
{
	XLogRecPtr	recptr;
	TimeLineID	tli;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	recptr = XLogRecoveryCtl->replayEndRecPtr;
	tli = XLogRecoveryCtl->replayEndTLI;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	if (replayEndTLI)
		*replayEndTLI = tli;
	return recptr;
}

/*
 * Save timestamp of latest processed commit/abort record.
 *
 * We keep this in XLogRecoveryCtl, not a simple static variable, so that it can be
 * seen by processes other than the startup process.  Note in particular
 * that CreateRestartPoint is executed in the checkpointer.
 */
static void
SetLatestXTime(TimestampTz xtime)
{
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	XLogRecoveryCtl->recoveryLastXTime = xtime;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 */
TimestampTz
GetLatestXTime(void)
{
	TimestampTz xtime;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	xtime = XLogRecoveryCtl->recoveryLastXTime;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return xtime;
}

/*
 * Save timestamp of the next chunk of WAL records to apply.
 *
 * We keep this in XLogRecoveryCtl, not a simple static variable, so that it can be
 * seen by all backends.
 */
static void
SetCurrentChunkStartTime(TimestampTz xtime)
{
	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	XLogRecoveryCtl->currentChunkStartTime = xtime;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 * Startup process maintains an accurate local copy in XLogReceiptTime
 */
TimestampTz
GetCurrentChunkReplayStartTime(void)
{
	TimestampTz xtime;

	SpinLockAcquire(&XLogRecoveryCtl->info_lck);
	xtime = XLogRecoveryCtl->currentChunkStartTime;
	SpinLockRelease(&XLogRecoveryCtl->info_lck);

	return xtime;
}

/*
 * Returns time of receipt of current chunk of XLOG data, as well as
 * whether it was received from streaming replication or from archives.
 */
void
GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream)
{
	/*
	 * This must be executed in the startup process, since we don't export the
	 * relevant state to shared memory.
	 */
	Assert(InRecovery);

	*rtime = XLogReceiptTime;
	*fromStream = (XLogReceiptSource == XLOG_FROM_STREAM);
}

/*
 * Note that text field supplied is a parameter name and does not require
 * translation
 */
void
RecoveryRequiresIntParameter(const char *param_name, int currValue, int minValue)
{
	if (currValue < minValue)
	{
		if (HotStandbyActiveInReplay())
		{
			bool		warned_for_promote = false;

			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("hot standby is not possible because of insufficient parameter settings"),
					 errdetail("%s = %d is a lower setting than on the primary server, where its value was %d.",
							   param_name,
							   currValue,
							   minValue)));

			SetRecoveryPause(true);

			ereport(LOG,
					(errmsg("recovery has paused"),
					 errdetail("If recovery is unpaused, the server will shut down."),
					 errhint("You can then restart the server after making the necessary configuration changes.")));

			while (GetRecoveryPauseState() != RECOVERY_NOT_PAUSED)
			{
				HandleStartupProcInterrupts();

				if (CheckForStandbyTrigger())
				{
					if (!warned_for_promote)
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("promotion is not possible because of insufficient parameter settings"),

						/*
						 * Repeat the detail from above so it's easy to find
						 * in the log.
						 */
								 errdetail("%s = %d is a lower setting than on the primary server, where its value was %d.",
										   param_name,
										   currValue,
										   minValue),
								 errhint("Restart the server after making the necessary configuration changes.")));
					warned_for_promote = true;
				}

				/*
				 * If recovery pause is requested then set it paused.  While
				 * we are in the loop, user might resume and pause again so
				 * set this every time.
				 */
				ConfirmRecoveryPaused();

				/*
				 * We wait on a condition variable that will wake us as soon
				 * as the pause ends, but we use a timeout so we can check the
				 * above conditions periodically too.
				 */
				ConditionVariableTimedSleep(&XLogRecoveryCtl->recoveryNotPausedCV, 1000,
											WAIT_EVENT_RECOVERY_PAUSE);
			}
			ConditionVariableCancelSleep();
		}

		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("recovery aborted because of insufficient parameter settings"),
		/* Repeat the detail from above so it's easy to find in the log. */
				 errdetail("%s = %d is a lower setting than on the primary server, where its value was %d.",
						   param_name,
						   currValue,
						   minValue),
				 errhint("You can restart the server after making the necessary configuration changes.")));
	}
}


/*
 * GUC check_hook for primary_slot_name
 */
bool
check_primary_slot_name(char **newval, void **extra, GucSource source)
{
	int			err_code;
	char	   *err_msg = NULL;
	char	   *err_hint = NULL;

	if (*newval && strcmp(*newval, "") != 0 &&
		!ReplicationSlotValidateNameInternal(*newval, &err_code, &err_msg,
											 &err_hint))
	{
		GUC_check_errcode(err_code);
		GUC_check_errdetail("%s", err_msg);
		if (err_hint != NULL)
			GUC_check_errhint("%s", err_hint);
		return false;
	}

	return true;
}

/*
 * Recovery target settings: Only one of the several recovery_target* settings
 * may be set.  Setting a second one results in an error.  The global variable
 * recoveryTarget tracks which kind of recovery target was chosen.  Other
 * variables store the actual target value (for example a string or a xid).
 * The assign functions of the parameters check whether a competing parameter
 * was already set.  But we want to allow setting the same parameter multiple
 * times.  We also want to allow unsetting a parameter and setting a different
 * one, so we unset recoveryTarget when the parameter is set to an empty
 * string.
 *
 * XXX this code is broken by design.  Throwing an error from a GUC assign
 * hook breaks fundamental assumptions of guc.c.  So long as all the variables
 * for which this can happen are PGC_POSTMASTER, the consequences are limited,
 * since we'd just abort postmaster startup anyway.  Nonetheless it's likely
 * that we have odd behaviors such as unexpected GUC ordering dependencies.
 */

static void
pg_attribute_noreturn()
error_multiple_recovery_targets(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("multiple recovery targets specified"),
			 errdetail("At most one of recovery_target, recovery_target_lsn, recovery_target_name, recovery_target_time, recovery_target_xid may be set.")));
}

/*
 * GUC check_hook for recovery_target
 */
bool
check_recovery_target(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "immediate") != 0 && strcmp(*newval, "") != 0)
	{
		GUC_check_errdetail("The only allowed value is \"immediate\".");
		return false;
	}
	return true;
}

/*
 * GUC assign_hook for recovery_target
 */
void
assign_recovery_target(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_IMMEDIATE)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
		recoveryTarget = RECOVERY_TARGET_IMMEDIATE;
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

/*
 * GUC check_hook for recovery_target_lsn
 */
bool
check_recovery_target_lsn(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		XLogRecPtr	lsn;
		XLogRecPtr *myextra;
		bool		have_error = false;

		lsn = pg_lsn_in_internal(*newval, &have_error);
		if (have_error)
			return false;

		myextra = (XLogRecPtr *) guc_malloc(ERROR, sizeof(XLogRecPtr));
		*myextra = lsn;
		*extra = (void *) myextra;
	}
	return true;
}

/*
 * GUC assign_hook for recovery_target_lsn
 */
void
assign_recovery_target_lsn(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_LSN)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_LSN;
		recoveryTargetLSN = *((XLogRecPtr *) extra);
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

/*
 * GUC check_hook for recovery_target_name
 */
bool
check_recovery_target_name(char **newval, void **extra, GucSource source)
{
	/* Use the value of newval directly */
	if (strlen(*newval) >= MAXFNAMELEN)
	{
		GUC_check_errdetail("%s is too long (maximum %d characters).",
							"recovery_target_name", MAXFNAMELEN - 1);
		return false;
	}
	return true;
}

/*
 * GUC assign_hook for recovery_target_name
 */
void
assign_recovery_target_name(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_NAME)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_NAME;
		recoveryTargetName = newval;
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

/*
 * GUC check_hook for recovery_target_time
 *
 * The interpretation of the recovery_target_time string can depend on the
 * time zone setting, so we need to wait until after all GUC processing is
 * done before we can do the final parsing of the string.  This check function
 * only does a parsing pass to catch syntax errors, but we store the string
 * and parse it again when we need to use it.
 */
bool
check_recovery_target_time(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		/* reject some special values */
		if (strcmp(*newval, "now") == 0 ||
			strcmp(*newval, "today") == 0 ||
			strcmp(*newval, "tomorrow") == 0 ||
			strcmp(*newval, "yesterday") == 0)
		{
			return false;
		}

		/*
		 * parse timestamp value (see also timestamptz_in())
		 */
		{
			char	   *str = *newval;
			fsec_t		fsec;
			struct pg_tm tt,
					   *tm = &tt;
			int			tz;
			int			dtype;
			int			nf;
			int			dterr;
			char	   *field[MAXDATEFIELDS];
			int			ftype[MAXDATEFIELDS];
			char		workbuf[MAXDATELEN + MAXDATEFIELDS];
			DateTimeErrorExtra dtextra;
			TimestampTz timestamp;

			dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
								  field, ftype, MAXDATEFIELDS, &nf);
			if (dterr == 0)
				dterr = DecodeDateTime(field, ftype, nf,
									   &dtype, tm, &fsec, &tz, &dtextra);
			if (dterr != 0)
				return false;
			if (dtype != DTK_DATE)
				return false;

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
			{
				GUC_check_errdetail("timestamp out of range: \"%s\"", str);
				return false;
			}
		}
	}
	return true;
}

/*
 * GUC assign_hook for recovery_target_time
 */
void
assign_recovery_target_time(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_TIME)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
		recoveryTarget = RECOVERY_TARGET_TIME;
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}

/*
 * GUC check_hook for recovery_target_timeline
 */
bool
check_recovery_target_timeline(char **newval, void **extra, GucSource source)
{
	RecoveryTargetTimeLineGoal rttg;
	RecoveryTargetTimeLineGoal *myextra;

	if (strcmp(*newval, "current") == 0)
		rttg = RECOVERY_TARGET_TIMELINE_CONTROLFILE;
	else if (strcmp(*newval, "latest") == 0)
		rttg = RECOVERY_TARGET_TIMELINE_LATEST;
	else
	{
		rttg = RECOVERY_TARGET_TIMELINE_NUMERIC;

		errno = 0;
		strtoul(*newval, NULL, 0);
		if (errno == EINVAL || errno == ERANGE)
		{
			GUC_check_errdetail("recovery_target_timeline is not a valid number.");
			return false;
		}
	}

	myextra = (RecoveryTargetTimeLineGoal *) guc_malloc(ERROR, sizeof(RecoveryTargetTimeLineGoal));
	*myextra = rttg;
	*extra = (void *) myextra;

	return true;
}

/*
 * GUC assign_hook for recovery_target_timeline
 */
void
assign_recovery_target_timeline(const char *newval, void *extra)
{
	recoveryTargetTimeLineGoal = *((RecoveryTargetTimeLineGoal *) extra);
	if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC)
		recoveryTargetTLIRequested = (TimeLineID) strtoul(newval, NULL, 0);
	else
		recoveryTargetTLIRequested = 0;
}

/*
 * GUC check_hook for recovery_target_xid
 */
bool
check_recovery_target_xid(char **newval, void **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		TransactionId xid;
		TransactionId *myextra;

		errno = 0;
		xid = (TransactionId) strtou64(*newval, NULL, 0);
		if (errno == EINVAL || errno == ERANGE)
			return false;

		myextra = (TransactionId *) guc_malloc(ERROR, sizeof(TransactionId));
		*myextra = xid;
		*extra = (void *) myextra;
	}
	return true;
}

/*
 * GUC assign_hook for recovery_target_xid
 */
void
assign_recovery_target_xid(const char *newval, void *extra)
{
	if (recoveryTarget != RECOVERY_TARGET_UNSET &&
		recoveryTarget != RECOVERY_TARGET_XID)
		error_multiple_recovery_targets();

	if (newval && strcmp(newval, "") != 0)
	{
		recoveryTarget = RECOVERY_TARGET_XID;
		recoveryTargetXid = *((TransactionId *) extra);
	}
	else
		recoveryTarget = RECOVERY_TARGET_UNSET;
}
