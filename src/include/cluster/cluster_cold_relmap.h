/*-------------------------------------------------------------------------
 * cluster_cold_relmap.h
 *     Exact relmap participant of the original typed cold replay plan.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/include/cluster/cluster_cold_relmap.h
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RELMAP_H
#define CLUSTER_COLD_RELMAP_H

#include "cluster/cluster_cold_recovery.h"
#include "common/cluster_relmap_v2.h"

#define CLUSTER_COLD_RELMAP_MAX_INPUTS 64
struct ClusterRelmapRetentionDirectory;
struct LWLock;
struct ClusterRecoveryStartV1;

/* Original pending custody, not a way to infer FullXid/era from raw WAL.
 * The original owner supplies its exact retained pin and image. Declaration
 * copies all value fields within the plan budget, before any record feed.
 * The directory/lock remain borrowed and must outlive the entire episode.
 * This declaration supplies no lock, retention, terminal or lifetime grant. */
typedef struct ClusterColdRelmapInputV1 {
	uint32 participant;
	ClusterColdParticipantV1 cut;
	ClusterWalSourceRef source;
	ClusterColdRecordFrameV1 frame;
	ClusterRelmapV2Image staged;
	struct ClusterRelmapRetentionDirectory *retention;
	struct LWLock *retention_lock;
} ClusterColdRelmapInputV1;

/* Original owner pin plus a complete decoded WAL record. The owner supplies
 * its actual FullXid/era and pin generation, never an epoch inferred from WAL.
 * owner.wal_end must be zero: the producer takes the actual record end.
 * All pointers are borrowed through prepare; the resulting declaration owns
 * copied values and retains only the original directory/lock pointers. */
typedef struct ClusterColdRelmapWalV1 {
	uint32 participant;
	ClusterRelmapV2Namespace identity;
	uint64 generation;
	ClusterRelmapV2Owner owner;
	struct ClusterRelmapRetentionDirectory *retention;
	struct LWLock *retention_lock;
	struct XLogReaderState *reader;
} ClusterColdRelmapWalV1;

/* Saved pass-2 input, not a current physical/pin/terminal certificate. Values
 * are copied; directory/lock stay borrowed from this exact original plan.
 * They must not outlive the original Start and its reverse cleanup. */
typedef struct ClusterColdRelmapReplayReceiptV1 {
	uint32 count, index;
	ClusterColdRelmapInputV1 input;
} ClusterColdRelmapReplayReceiptV1;

/* Local immutable receipt checks only, including the complete batch and
 * original selected source/cut. UINT32_MAX means invalid/incomplete; zero
 * means no declared participant. No replay-window check, I/O, acquire,
 * cancellation, source revalidation or new lifetime/retention grant. */
extern uint32 cluster_cold_typed_relmap_replay_count_v1(const ClusterColdTypedV1 *typed);
extern ClusterColdRelmapReplayReceiptV1 cluster_cold_typed_relmap_replay_input_v1(
	const ClusterColdTypedV1 *typed, uint32 index);

/* Recheck the complete saved staging batch under the same declared Start,
 * after pass-2's window has ended. Requires Startup, no critical section or
 * caller-held LWLocks and the original namespace X. Whole-batch saved values,
 * SIDE and original pins precede file I/O; both complete physical copies and
 * the same inputs/pins/Start are checked before returning. Scratch uses the
 * original plan budget. Entered refusal/ERROR revokes the original episode
 * and keeps its plan, FD, pins and debt; no retry, rewrite or cleanup follows.
 * Success is a current staging observation only, not terminal/era/lifetime
 * qualification, a reusable certificate, COMPLETE or a release permission. */
extern bool cluster_cold_relmap_replay_recheck_v1(const struct ClusterRecoveryStartV1 *start);

/* Explicit original-source adapter before pass-1. Whole-batch validation of
 * exact unstaged pins precedes any mutation. Recheck and arm every original
 * pin with the actual WAL end before the first clean-pair I/O, outside the
 * retention lock. This conservatively prevents cancellation even when no
 * physical pending was written. Partial arm, later failure and plan destruction
 * retain those original pins/debt; no rollback or automatic retry follows.
 * Sources must be the original selected replay generations; the Startup
 * entry below brackets this call with original ROOT/fence revalidation.
 * Entered failure/ERROR poisons the plan, keeps all original pins/debt and
 * permits no retry. No retention qualification, COMPLETE or activation. */
extern ClusterColdDetailV1 cluster_cold_plan_relmap_from_wal_v1(ClusterColdPlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 replay_count,
	const ClusterColdRelmapWalV1 *inputs, uint32 count);

extern ClusterColdTypedV1 *cluster_cold_typed_prepare_relmap_wal_v1(
	struct ClusterRecoveryFencePlan *fence, uint16 own_thread, XLogRecPtr own_redo,
	const ClusterColdRelmapWalV1 *inputs, uint32 count);

extern ClusterColdDetailV1 cluster_cold_plan_relmap_declare_v1(ClusterColdPlanV1 *plan,
	const ClusterColdRelmapInputV1 *inputs, uint32 count);

/* Original source obligation, including declarations not reached yet.
 * Failure invalidation is sticky and cannot grant capture or execution. */
extern bool cluster_cold_plan_relmap_required_v1(const ClusterColdPlanV1 *plan,
	uint32 participant);
extern void cluster_cold_plan_relmap_invalidate_v1(ClusterColdPlanV1 *plan);

/* Borrowed read-only declaration only after this exact pass-1 record was
 * captured. Valid until plan destruction; no pin, execution or lifetime grant. */
extern const ClusterColdRelmapInputV1 *cluster_cold_plan_relmap_captured_v1(
	const ClusterColdPlanV1 *plan, uint32 participant, const ClusterColdRecordV1 *record);

/* Explicit original-owner input before pass-1. The ordinary prepare entry
 * supplies no declaration and retains the original structural refusal. */
extern ClusterColdTypedV1 *cluster_cold_typed_prepare_relmap_v1(
	struct ClusterRecoveryFencePlan *fence, uint16 own_thread, XLogRecPtr own_redo,
	const ClusterColdRelmapInputV1 *inputs, uint32 count);

/* Original scheduled position only. Requires the original executable source
 * bracket, complete sealed SIDE carrier and namespace X; uses the original
 * retained FullXid pin for stage.
 * Attempt is latched before the first current check or I/O. False/ERROR is
 * sticky, leaves both physical copies and every original pin/debt in place,
 * and permits no retry. A successful duplicate only checks the saved receipt
 * and complete staged pair; it never writes again. */
extern bool cluster_cold_typed_relmap_record_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, const ClusterColdStepV1 *step,
	struct XLogReaderState *reader);

/* One conjunct at the original completion cut, not a completion setter.
 * Every declared input must have been captured and consumed; current source,
 * complete sealed SIDE declarations, exact original pins and staged pairs
 * are revalidated. No declaration
 * does not waive pass-1's original structural refusal. No cleanup/release. */
extern bool cluster_cold_typed_relmap_ready_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence);

#endif
