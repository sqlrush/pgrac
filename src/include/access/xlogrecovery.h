/*
 * xlogrecovery.h
 *
 * Functions for WAL recovery and standby mode
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogrecovery.h
 */
#ifndef XLOGRECOVERY_H
#define XLOGRECOVERY_H

#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
#include "utils/timestamp.h"

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when in standby mode.
 */
typedef enum
{
	RECOVERY_TARGET_UNSET,
	RECOVERY_TARGET_XID,
	RECOVERY_TARGET_TIME,
	RECOVERY_TARGET_NAME,
	RECOVERY_TARGET_LSN,
	RECOVERY_TARGET_IMMEDIATE
} RecoveryTargetType;

/*
 * Recovery target TimeLine goal
 */
typedef enum
{
	RECOVERY_TARGET_TIMELINE_CONTROLFILE,
	RECOVERY_TARGET_TIMELINE_LATEST,
	RECOVERY_TARGET_TIMELINE_NUMERIC
} RecoveryTargetTimeLineGoal;

/* Recovery pause states */
typedef enum RecoveryPauseState
{
	RECOVERY_NOT_PAUSED,		/* pause not requested */
	RECOVERY_PAUSE_REQUESTED,	/* pause requested, but not yet paused */
	RECOVERY_PAUSED				/* recovery is paused */
} RecoveryPauseState;

/* User-settable GUC parameters */
extern PGDLLIMPORT bool recoveryTargetInclusive;
extern PGDLLIMPORT int recoveryTargetAction;
extern PGDLLIMPORT int recovery_min_apply_delay;
extern PGDLLIMPORT char *PrimaryConnInfo;
extern PGDLLIMPORT char *PrimarySlotName;
extern PGDLLIMPORT char *recoveryRestoreCommand;
extern PGDLLIMPORT char *recoveryEndCommand;
extern PGDLLIMPORT char *archiveCleanupCommand;

/* indirectly set via GUC system */
extern PGDLLIMPORT TransactionId recoveryTargetXid;
extern PGDLLIMPORT char *recovery_target_time_string;
extern PGDLLIMPORT TimestampTz recoveryTargetTime;
extern PGDLLIMPORT const char *recoveryTargetName;
extern PGDLLIMPORT XLogRecPtr recoveryTargetLSN;
extern PGDLLIMPORT RecoveryTargetType recoveryTarget;
extern PGDLLIMPORT bool wal_receiver_create_temp_slot;
extern PGDLLIMPORT RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal;
extern PGDLLIMPORT TimeLineID recoveryTargetTLIRequested;
extern PGDLLIMPORT TimeLineID recoveryTargetTLI;

/* Have we already reached a consistent database state? */
extern PGDLLIMPORT bool reachedConsistency;

/* Are we currently in standby mode? */
extern PGDLLIMPORT bool StandbyMode;

extern Size XLogRecoveryShmemSize(void);
extern void XLogRecoveryShmemInit(void);

extern void InitWalRecovery(ControlFileData *ControlFile,
							bool *wasShutdown_ptr, bool *haveBackupLabel_ptr,
							bool *haveTblspcMap_ptr);
#ifdef USE_PGRAC_CLUSTER
/* Original typed cold plan, prepared before shared Startup control handling;
 * one-shot replay input only, without control publication or a WAL writer. */
extern void ClusterPrepareColdWalRecovery(void);
#endif
extern void PerformWalRecovery(void);

/*
 * FinishWalRecovery() returns this.  It contains information about the point
 * where recovery ended, and why it ended.
 */
typedef struct
{
	/*
	 * Information about the last valid or applied record, after which new WAL
	 * can be appended.  'lastRec' is the position where the last record
	 * starts, and 'endOfLog' is its end.  'lastPage' is a copy of the last
	 * partial page that contains endOfLog (or NULL if endOfLog is exactly at
	 * page boundary).  'lastPageBeginPtr' is the position where the last page
	 * begins.
	 *
	 * endOfLogTLI is the TLI in the filename of the XLOG segment containing
	 * the last applied record.  It could be different from lastRecTLI, if
	 * there was a timeline switch in that segment, and we were reading the
	 * old WAL from a segment belonging to a higher timeline.
	 */
	XLogRecPtr	lastRec;		/* start of last valid or applied record */
	TimeLineID	lastRecTLI;
	XLogRecPtr	endOfLog;		/* end of last valid or applied record */
	TimeLineID	endOfLogTLI;

	XLogRecPtr	lastPageBeginPtr;	/* LSN of page that contains endOfLog */
	char	   *lastPage;		/* copy of the last page, up to endOfLog */

	/*
	 * abortedRecPtr is the start pointer of a broken record at end of WAL
	 * when recovery completes; missingContrecPtr is the location of the first
	 * contrecord that went missing.  See CreateOverwriteContrecordRecord for
	 * details.
	 */
	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;

	/* short human-readable string describing why recovery ended */
	char	   *recoveryStopReason;

	/*
	 * If standby or recovery signal file was found, these flags are set
	 * accordingly.
	 */
	bool		standby_signal_file_found;
	bool		recovery_signal_file_found;
#ifdef USE_PGRAC_CLUSTER
	/* Borrowed, attempt-local replay inputs until Startup's final close.
	 * No retained execution pin, current source proof, or C0 write authority. */
	const struct ClusterColdTypedV1 *cluster_cold_inputs;
	/* Original post-EOR resource identity, not an I/O or C0 grant. */
	struct ClusterRecoveryStartV1 *cluster_tx_start;
#endif
} EndOfWalRecoveryInfo;

extern EndOfWalRecoveryInfo *FinishWalRecovery(void);
extern void ShutdownWalRecovery(void);
#ifdef USE_PGRAC_CLUSTER
extern void ClusterReleaseColdRecoveryInputs(EndOfWalRecoveryInfo *info);
extern bool ClusterBeginColdRecoveryTx(EndOfWalRecoveryInfo *info);
extern bool ClusterColdRecoveryTxCurrent(const struct ClusterRecoveryStartV1 *start);
struct ClusterRecoveryCheckpointOwnerV1;
/* Original recovered INSTALL only; ordinary CLEAN/first creation cannot
 * provide this checkpoint input. The output clears on refusal. */
extern bool ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
	struct ClusterRecoveryCheckpointOwnerV1 *out);
struct ClusterColdTopTxCensusEntryV1;
/* Original ACTIVE census subject only; not a loser proof or I/O grant. */
extern bool ClusterColdRecoveryTxSubjectCurrent(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject, uint64 *generation, uint64 *serial);

/* Original post-EOR owner only: return a physical loser candidate from the
 * exact retained ACTIVE census entry.  This is a read-only identity check;
 * it grants no C0, TT, WAL, checkpoint or mutation authority. */
extern bool ClusterColdRecoveryTxLoserCurrent(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject);

/* Original post-EOR owner only: require the producer's complete independent
 * TopTxProof for this exact ACTIVE census subject.  This is a read-only
 * qualified old-ACTIVE input; it grants no C0, TT, WAL, checkpoint or
 * mutation authority. */
extern bool ClusterColdRecoveryTxTopProofCurrent(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject);

extern const struct ClusterColdTypedV1 *
ClusterColdRecoveryTxInputs(const struct ClusterRecoveryStartV1 *start);

/* Check the entire original opaque Start extent without exposing its layout.
 * Storage separation only; this grants no input/current/write authority. */
extern bool ClusterColdRecoveryStartOutputDistinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes);

struct ClusterColdCtrcWorkV1;
/* Original RECOVERED/native-DONE Startup only. Prepare retained TT-native
 * work before C0; this grants no page, receipt, I/O or release authority.
 * A subsequent prepare or input reset invalidates the previous pointer. */
extern const struct ClusterColdCtrcWorkV1 *
ClusterColdRecoveryCtrcWorkPrepare(const struct ClusterRecoveryStartV1 *start);
extern bool ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work);

/* Original Startup after accepted TX: drive the complete read-only census
 * under its original CTRC scope, retaining both under this same Start.
 * memory_budget includes the immutable work copy, current physical TT view
 * and census-owned allocations (not borrowed inputs/reader/cryptohash storage).
 * After manifest checks, qualify every retained physical subject with the
 * original canonical C0/preexisting terminal consumer without changing it.
 * Repeated calls only revalidate the saved observation; refusal is sticky.
 * DRAINED/manifest never supplies a semantic seal, freeze, PAGE authority,
 * service completion or release. Original Start cleanup retires cursor before
 * scope. Native ERROR propagates, with original WALR release debt unchanged. */
extern bool ClusterColdRecoveryCtrcObserve(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget);
/* First bounded observation refusal, valid only while original Start lives. */
extern const char *ClusterColdRecoveryCtrcObservationRefusal(
	const struct ClusterRecoveryStartV1 *start);

/* Borrow the same complete raw physical TT view after original observation.
 * Each subject must also pass the original canonical C0/preexisting terminal
 * consumer after the manifest checks, including every repeat validation.
 * Exact original Start/work and repeat observation revalidation are required;
 * no new scope/census or source refresh. Run before PAGE/content protection:
 * original interrupt/manifest checks may execute. NULL refuses, including a
 * provisional/failed observation. Borrowed bytes are immutable until original
 * owner cleanup; later protected consumers still recheck their own premises.
 * The view grants no semantic seal, freeze, terminal or mutation authority. */
struct ClusterColdCtrcPhysicalV1;
extern const struct ClusterColdCtrcPhysicalV1 *ClusterColdRecoveryCtrcPhysical(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);

/* Original post-EOR owner only: require the complete sealed ordinary-effects
 * receipt before consuming any proven physical loser.  This is a read-only
 * input check; the restricted empty-effects proof remains a per-entry
 * TopTxProof fact and this accessor grants no C0, TT, WAL or checkpoint
 * authority. */
extern bool ClusterColdRecoveryTxEffectsCurrent(
	const struct ClusterRecoveryStartV1 *start);

struct ClusterRecoveryTTAbortDerivationV1;
/* Pure C0 candidate fields from the original proof/endpoints and installed
 * writer. The caller holds and revalidates the original TX_CLOSURE read
 * scope. This grants no WAL, TT, checkpoint or execution authority. */
extern bool ClusterColdRecoveryTxC0Candidate(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterRecoveryTTAbortDerivationV1 *out);
struct ClusterRecoverySerialRequest;
/* Original C0 subject/source and completed isolation owner only. Provisional
 * request bindings, with no grant or replacement of the old duty identity. */
extern bool ClusterColdRecoveryTxSerialRequest(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterRecoverySerialRequest *out);
struct ClusterUndoBlock0Pin;
/* Reserve the original proven target's EMPTY block0 slot without a content
 * lock. The caller releases it before the retained source scope. This is
 * local exclusion only, with no C0/WAL/TT/I/O or checkpoint authority. */
extern bool ClusterColdRecoveryTxC0Reserve(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterUndoBlock0Pin *pin);
#endif
extern void RemovePromoteSignalFiles(void);

extern bool HotStandbyActive(void);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
extern RecoveryPauseState GetRecoveryPauseState(void);
extern void SetRecoveryPause(bool recoveryPause);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern XLogRecPtr GetCurrentReplayRecPtr(TimeLineID *replayEndTLI);

extern bool PromoteIsTriggered(void);
extern bool CheckPromoteSignal(void);
extern void WakeupRecovery(void);

extern void StartupRequestWalReceiverRestart(void);
extern void XLogRequestWalReceiverReply(void);

extern void RecoveryRequiresIntParameter(const char *param_name, int currValue, int minValue);

extern void xlog_outdesc(StringInfo buf, XLogReaderState *record);

#endif							/* XLOGRECOVERY_H */
