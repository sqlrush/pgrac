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
/* Fixed obligation in the original episode; no relmap success certificate.
 * Declare before physical observation or accepted CTRC handoff. Declaration
 * withholds independent CTRC COMPLETE until the original joint publisher is
 * connected. Failure revokes this entire original episode, retaining cleanup
 * debt and all original resources. No caller-supplied completion polarity. */
extern bool ClusterColdRecoveryRelmapDeclare(const struct ClusterRecoveryStartV1 *start);
extern bool ClusterColdRecoveryRelmapCurrent(const struct ClusterRecoveryStartV1 *start);
extern void ClusterColdRecoveryRelmapFail(const struct ClusterRecoveryStartV1 *start);
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
struct ClusterWalReadPinOwnerV1;
/* Same original episode's continuous WALR, borrowed by value. No new pin,
 * scope or recovery permission; retire borrowers before original cleanup. */
extern struct ClusterWalReadPinOwnerV1
ClusterColdRecoveryTxInputOwnerV1(const struct ClusterRecoveryStartV1 *start);

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
/* After complete observation and accepted TX, retain the same original scope
 * and acquire every native replay IR through its existing all-source owner.
 * Called by Startup outside recovery/critical sections with no caller LWLocks.
 * Native BUSY/RETRY/CF contention waits locally, with partial owners retained
 * and no overall deadline. Interrupts and callbacks revalidate original inputs;
 * final success repeats the original full observation and actual-holder check.
 * No new scope/source round, PAGE mutation, debt retirement or service result.
 * False/ERROR leaves original cleanup responsibility in place, including any
 * original observation cleanup triggered by preflight refusal. */
extern bool ClusterColdRecoveryCtrcReplaysAcquire(const struct ClusterRecoveryStartV1 *start);
/* Prepare once after original all-replay IR acquisition, outside native
 * recovery/critical sections and with zero caller LWLocks. Keep the complete
 * all-generation graph on the same Start; callbacks revalidate original full
 * work/manifest/scope/actual holders before publication. Zero budget only
 * revalidates an existing graph; nonzero budget is additional private storage,
 * capped at the existing PAGE plan limit, including the complete native
 * generation vector reserved before the graph. Entered failure/ERROR revokes this
 * episode and retains original cleanup/debt. Destroy before census/scope.
 * Getter repeats original full validation and borrows immutable Start storage.
 * NULL Start is the clean-start no-op; no mutation, completion or release. */
struct RfPageOnlinePlanV1;
extern bool ClusterColdRecoveryCtrcPagePlanPrepare(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget);
/* Advance the original immutable PAGE graph to its first pending FPI.
 * Native local WAIT retains the same owners without an overall deadline.
 * The original Start retains INSERTED/image state; repeats cannot reinsert.
 * A true result is only a retained prefix, never discharge or service COMPLETE. */
extern bool ClusterColdRecoveryCtrcPageAdvance(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget);
/* Retire exact durable FPI operations only after original protected readback
 * and complete owner/source/current/local-debt checks. Retain each full result
 * and its original raw FD on the same Start until original cleanup. All result
 * slots count against memory_budget before the first PAGE/FPI; zero budget
 * only continues existing progress. Final same-FD images must remain byte-exact
 * under the original all-ITL projector and a frozen complete canonical terminal
 * vector, then every effective tuple xmin/xmax must bind its actual terminal
 * DATA ITL in that same whole image. Full current checks follow both predicates.
 * Local HOT topology is checked against the actual target block; nonlocal
 * and retained history coverage remain separate obligations. The vector and image scratch peak are
 * reserved before the first PAGE. No replay pin/history release, independent
 * or joint service COMPLETE, relmap acceptance or OPEN is granted. */
extern bool ClusterColdRecoveryCtrcPageDrain(const struct ClusterRecoveryStartV1 *start,
	Size memory_budget);
/* Copy one original completed target after the SAME held-FD protected readback
 * and PAGE/proof cleanup, followed by full Start/work/source/result checks.
 * Only a fully drained original vector with a saved MAIN heap baseline enters.
 * No new scan/source round, write, F4 transition, release or service result.
 * Local contention returns WOULD_BLOCK; entered failure revokes this episode
 * while retaining its original descriptors and all IR/WALR/pins/debt. */
struct RfPageCtrcReadbackImageV1;
extern struct RfPageCtrcReadbackImageV1 ClusterColdRecoveryCtrcPageImage(
	const struct ClusterRecoveryStartV1 *start, uint32 target_index);
/* Execute the original completed/unchanged PAGE owner predicates and bind
 * its full readback to the exact original target/source, after PAGE/proof
 * cleanup and complete Start/result/FD revalidation. No supplied rule or
 * success flag. The original clearing preflight consumes this proof; it
 * does not clear first/pin/history/companion debt or grant joint completion.
 * Same local WAIT, sticky failure/ERROR, custody and lifetime as PageImage. */
struct RfPageCtrcOwnerProofV1;
extern struct RfPageCtrcOwnerProofV1 ClusterColdRecoveryCtrcPageOwnerProof(
	const struct ClusterRecoveryStartV1 *start, uint32 target_index);
/* Original PAGE owner -> canonical completion bytes. Accept exactly one
 * complete canonical atom2 for the SAME native MAIN path/block and full page.
 * Its logical origin is the original replay generation's origin; native
 * relation file generation is the explicit no-U64-generation value zero,
 * not a qualified base/era. Snapshot the entire bounded witness before the
 * original owner read; derive outcome/rule only from that actual operation.
 * memory_budget bounds additional temporary witness/path/ID storage.
 * Return only by value. Inputs remain immutable; normal/ERROR frees private
 * scratch, preserving native custody and sticky refusal. Caller must retain
 * and revalidate the original Start and this contribution before graph use.
 * No complete owner scope/graph, D0 admission, release or joint success. */
struct RfPageCtrcOwnerCompletionV1;
extern struct RfPageCtrcOwnerCompletionV1 ClusterColdRecoveryCtrcPageOwnerCompletion(
	const struct ClusterRecoveryStartV1 *start, uint32 target_index,
	const uint8 *raw_evidence, Size evidence_length, Size memory_budget);
/* Produce all original nonempty PAGE targets as one immutable local inventory.
 * No supplied witness, target subset, outcome or rule. Budget the entire
 * output/snapshot/codec peak before first PAGE; WAIT/false/ERROR publishes no
 * prefix and retains original FD/all IR-WALR/pins debt. entries is local-only:
 * pfree it before original Start/scope/ResourceOwner ends, never close its FDs.
 * Original current/graph/source/owner/base/lifetime checks remain mandatory
 * before consuming these bytes. No zero-target/no-effect or joint grant. */
struct RfPageCtrcOwnersV1;
extern struct RfPageCtrcOwnersV1 ClusterColdRecoveryCtrcPageOwners(
	const struct ClusterRecoveryStartV1 *start, Size memory_budget);
/* Complete original references joined to the complete actual PAGE inventory.
 * Validate the full original TX-reference membership, retain every other
 * obligation and publish only after the complete saved set is unchanged.
 * Budget the entire additional peak before PAGE. On success pfree entries
 * and owners.entries before the original Start/scope/ResourceOwner ends;
 * references borrows that original lifetime. On refusal all fields except
 * detail are zero. Original failed-reader cleanup/debt policy remains;
 * no prefix, new FD release, graph/D0 or joint success grant. */
struct RfPageCtrcOwnerReferencesV1;
extern struct RfPageCtrcOwnerReferencesV1 ClusterColdRecoveryCtrcPageOwnerReferences(
	const struct ClusterRecoveryStartV1 *start, Size memory_budget);

/* Original complete PAGE owner/reference inventory plus full transaction and
 * sealed contributor scope. Canonical owner keys use original ROOT source
 * indexes and original decoded page-class/component ordinals; evidence joins
 * the actual whole DATA readback and its original completion predicate.
 * No caller-supplied subset, transaction, witness or success flag. Historical,
 * FREE, tuple and unmatched references remain separate obligations. This does
 * not establish full G_ref/non-PAGE/base/lifetime coverage or authorize D0.
 * On success pfree entries, related.entries and related.owners.entries before
 * the original Start/scope ends. Do not close their original backing FDs.
 * Full simultaneous budget precedes PAGE; WAIT/refusal/ERROR publish no prefix
 * and preserve original FD/allIR-WALR/pins-debt. */
struct RfPageCtrcOwnerGraphV1;
extern struct RfPageCtrcOwnerGraphV1 ClusterColdRecoveryCtrcPageOwnerGraph(
	const struct ClusterRecoveryStartV1 *start, Size memory_budget);
/* Explicit current-field projection contribution. Revalidate the entire original
 * canonical terminal vector before/after the same actual PAGE owners, then
 * use the original pure ITL projector to check the same field's final bytes.
 * Only exact DATA/plain-lock terminal projections supply kind4 to that PAGE
 * owner. Current tuple xmin/xmax/plain-lock fields use the complete same-page
 * original ITL set's full identities, exact item/payload snapshot and original
 * whole native projector; kind2 references those proven ITLs. All fields of a
 * tuple contribute together. Current HOT pages additionally require EVERY
 * original item and all eight slots to reproduce the actual whole after-image
 * under the original native topology; exact same-page xmax -> xmin links use
 * the same full canonical T. Actual current redirects retain their own physical
 * line-pointer offset/ordinal and reference only that complete page's proven
 * target xmin; a missing canonical target retains the redirect obligation.
 * A missing/changed member leaves its page's tuple
 * contributions unresolved. Historical/FREE/MX/unmatched references remain
 * unresolved. Complete tuple bytes and all output arrays are budgeted before
 * the first PAGE and rechecked with original source/terminal identity at return.
 * Return canonical nodes/edges and unique current-reference roots in the
 * contribution, alongside the complete original entry order and unresolved
 * count. Its arrays share entries' lifetime; no caller subset is accepted.
 * The ordinary observation API above stays unchanged. Same whole budget,
 * output/lifetime/WAIT/ERROR contract; no first/pin/history retirement, full
 * G_ref/G_owner coverage, D0 admission or joint completion is provided. */
extern struct RfPageCtrcOwnerGraphV1 ClusterColdRecoveryCtrcPageProjectedGraph(
	const struct ClusterRecoveryStartV1 *start, Size memory_budget);
/* Complete current PAGE slot/item coverage around the original graph factory.
 * Native FREE, no-storage unused/dead and exact frozen/invalid-xmax tuples
 * may have no current transaction reference. Missing/duplicate/current-byte
 * mismatches and unmapped physical pages cannot disappear from coverage.
 * History, other owners, first/pin debt and full graph/joint completion remain
 * separate duties. Old APIs retain their unresolved counts and semantics. */
struct RfPageCtrcReferenceCoverageV1;
extern struct RfPageCtrcReferenceCoverageV1 ClusterColdRecoveryCtrcPageReferenceCoverage(
	const struct ClusterRecoveryStartV1 *start, Size memory_budget);
struct RfPageCtrcFpiInsertV1;
struct ClusterSharedFsPageV1;
/* Local immutable membership only: the ORIGINAL retained result address and
 * SAME raw descriptor must belong to this Start's completed target vector.
 * Safe under its original PAGE; no manifest I/O, cleanup or new authority.
 * Full source/PAGE/writer/flush/bytes checks remain the readback's duty. */
extern bool ClusterColdRecoveryCtrcPageCompletedCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageCtrcFpiInsertV1 *inserted,
	struct ClusterSharedFsPageV1 *storage);
struct RfPageCtrcUnchangedV1;
extern bool ClusterColdRecoveryCtrcPageUnchangedCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageCtrcUnchangedV1 *unchanged,
	struct ClusterSharedFsPageV1 *storage);
extern const struct RfPageOnlinePlanV1 *ClusterColdRecoveryCtrcPagePlan(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);
/* Original complete generation vector in the SAME sealed graph's participant
 * order. input indexes remain distinct. Count must equal the entire original
 * selected set. Revalidates the exact Start/work/full observation/native IR
 * holders and source cuts; borrows immutable storage until original cleanup.
 * Call before PAGE/content locks. No new scope, I/O cut, grant or completion. */
struct ClusterRecoveryCtrcReplaySetSourceV1;
extern const struct ClusterRecoveryCtrcReplaySetSourceV1 *ClusterColdRecoveryCtrcPageSources(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	uint32 participant_count);
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

/* PGRAC: same original completed observation/work and accepted TX. Borrow every
 * physical relation target's value-only metadata and full-page digest under
 * this same Start/cursor/scope lifetime, after repeat original validation.
 * Call before PAGE/content locks. NULL refuses; a count-zero view is valid.
 * No source refresh, stable-base/PAGE authority, FPI or completion is supplied. */
struct ClusterRecoveryCensusPagesV1;
extern const struct ClusterRecoveryCensusPagesV1 *ClusterColdRecoveryCtrcPages(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);
/* Same original immutable relation/history references and private tuple bytes;
 * no complete graph, retention transfer or service permission is implied. */
struct ClusterRecoveryCensusReferencesV1;
extern const struct ClusterRecoveryCensusReferencesV1 *ClusterColdRecoveryCtrcReferences(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);

/* Complete original physical/history page versions under this same Start.
 * The original census reserves and owns every full image; callers only borrow
 * its complete immutable collection before PAGE/content locks. No image is
 * reconstructed from references and no graph/discharge right is supplied. */
struct ClusterRecoveryCensusImagesV1;
extern const struct ClusterRecoveryCensusImagesV1 *ClusterColdRecoveryCtrcImages(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);

/* Original complete TX forest through this SAME Start/work/accepted-TX and
 * full simultaneous IR/WALR observation. The by-value wrapper never accepts a
 * caller graph or source. Includes non-root predecessors, complete native
 * record bytes and independent record-segment generations under original
 * borrowed-owner lifetime, not fresh physical-file/lifetime qualification.
 * Original current/manifest checks may do I/O; call
 * before PAGE/content protection. Invalid results are all zero; complete end
 * is observed with a NULL record at index==count, including count zero.
 * Entered refusal/ERROR revokes the original observation with original cleanup.
 * Stable supplied forest is not full G_ref/G_owner, discharge or completion. */
struct ClusterRecoveryCensusTxRecordV1;
extern struct ClusterRecoveryCensusTxRecordV1 ClusterColdRecoveryCtrcTxRecord(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	uint32 index);

/* Complete original native WAL records through the same Start/work and full
 * simultaneous IR/WALR observation. Keeps complete payloads/source positions,
 * including nontransaction/query records; other WAL remains in the manifest.
 * Same pre-PAGE/current, borrowed lifetime and sticky cleanup as TxRecord.
 * observed/index==count/empty primary is complete end, not discharge or OPEN. */
struct ClusterRecoveryNativeRecordObservationV1;
extern struct ClusterRecoveryNativeRecordObservationV1 ClusterColdRecoveryCtrcNativeRecord(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	uint64 index);

/* Exact roots and resolved TX predecessor edges of this SAME complete
 * original Start/work/accepted-TX/full simultaneous ALL IR-WALR observation.
 * Same pre-PAGE checks, borrowed lifetime, whole-output failure and original
 * sticky reverse cleanup as TxRecord. No caller graph/root set is accepted;
 * this is native adjacency, not a semantic owner proof or COMPLETE authority. */
struct ClusterRecoveryCensusTxDependencyV1;
extern struct ClusterRecoveryCensusTxDependencyV1 ClusterColdRecoveryCtrcTxDependency(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	uint32 index);

/* Same original Start/work/accepted-TX/full simultaneous ALL IR-WALR.
 * Complete PAGE/history emissions remain borrowed from this original Start;
 * non-FREE ITL stored heads resolve to this SAME observed TX dependency index.
 * Raw tuple/MX/HOT obligations remain explicit. Same pre-PAGE/current and
 * sticky cleanup contract as TxDependency; no independent semantic graph,
 * terminal/owner proof, discharge, retention transfer or COMPLETE right. */
struct ClusterRecoveryCensusTxReferenceV1;
extern struct ClusterRecoveryCensusTxReferenceV1 ClusterColdRecoveryCtrcTxReference(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	uint32 index);

/* Same original complete observation and accepted TX; raw, immutable decoded
 * PAGE WAL inputs of every selected generation. Zero count is complete-empty.
 * Outputs clear on refusal; readers borrow original cursor/scope lifetime.
 * Pre-PAGE only: original observation may do I/O/cleanup. No contributor graph,
 * codec/image validation, native PAGE authority or service completion granted. */
struct ClusterRecoveryPageWalV1;
struct ClusterRecoveryWalSourceObservationV1;
/* Complete original source observations, before PAGE acquisition. Getters
 * repeat the original full observation/current checks and may do I/O. */
extern bool ClusterColdRecoveryCtrcWalSourceCount(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 *out_count);
extern bool ClusterColdRecoveryCtrcWalSourceAt(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index,
	struct ClusterRecoveryWalSourceObservationV1 *out);
extern bool ClusterColdRecoveryCtrcPageWalCount(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 *out_count);
extern bool ClusterColdRecoveryCtrcPageWalAt(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, struct ClusterRecoveryPageWalV1 *out);

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
/* One already observed original CTRC subject, including existing terminals.
 * By-value provisional old-source bindings only; empty on refusal. Call before
 * PAGE/content locks: the retained observation/manifest is revalidated. This
 * does not acquire IR/WALR/PAGE or replace the later complete-source scope.
 * Store the returned value in caller-owned storage distinct from all inputs. */
extern struct ClusterRecoverySerialRequest ClusterColdRecoveryCtrcSerialRequest(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index);
struct ClusterWalInputsV1;
/* Borrow only the original completed observation's scope, without I/O or
 * acquisition. Retain Start and its observation through every dependent use. */
extern struct ClusterWalInputsV1 *ClusterColdRecoveryCtrcScope(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work);
/* Native IR purpose callback: local original owner/work/accepted TX/terminal
 * and completed-source checks only. Never invokes manifest/ROOT I/O, frees a
 * borrowed scope, or revalidates the IR recursively. Request must be bound to
 * that exact original scope; full ROOT validation remains a separate step. */
extern bool ClusterColdRecoveryCtrcSerialRequestCurrent(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 subject_index,
	struct ClusterWalInputsV1 *scope, const struct ClusterRecoverySerialRequest *request);
/* Every original replay origin, including one with no TT subject. This
 * source-index purpose returns only the original completed-fence provisional
 * request after revalidating the complete retained observation/manifest.
 * Store the by-value result in disjoint caller storage. No native IR/WALR,
 * PAGE, DATA, FPI or service right is supplied; other sources retain their
 * own custody requirements. Nonreplay/history-only indexes refuse. */
extern struct ClusterRecoverySerialRequest ClusterColdRecoveryCtrcReplayRequest(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 source_index);
/* Local complete work/TX/terminal/source recheck under the exact original
 * borrowed scope. No I/O/allocation/cleanup or recursive IR validation.
 * Full ROOT/native-holder validation remains the owning consumer's duty. */
extern bool ClusterColdRecoveryCtrcReplayRequestCurrent(
	const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 source_index,
	struct ClusterWalInputsV1 *scope, const struct ClusterRecoverySerialRequest *request);
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
