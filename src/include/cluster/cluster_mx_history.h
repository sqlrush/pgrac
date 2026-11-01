/*-------------------------------------------------------------------------
 * cluster_mx_history.h
 *    Backend-private capture for exact MultiXact history collection.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MX_HISTORY_H
#define CLUSTER_MX_HISTORY_H
#include "c.h"
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
#include "access/htup_details.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_mx_history_wire.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

typedef struct ClusterMxHistoryCollector ClusterMxHistoryCollector;
/* A copy of captured inputs, never a proof or retention permission. */
typedef struct ClusterMxHistoryCapture {
	ClusterPcmOwnSnapshot pcm;
	XLogRecPtr page_lsn;
	ItemPointerData tid;
	ItemIdData line_pointer;
	uint16 tuple_length;
	uint8 tuple_header[SizeofHeapTupleHeader];
	ClusterItlSlotData roots[CLUSTER_ITL_INITRANS_DEFAULT];
	Oid relation_id;
	Buffer buffer;
	ClusterCurrentMxKey mxkey;
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 nmembers;
	bool read_only;
	ClusterSemanticAdmissionToken admission;
	ClusterR4CrOwnerChallenge requester_owner;
	SCN read_scn, retained_floor;
	uint64 read_epoch, snapshot_identity, descriptor_hash;
	uint64 collector_id, capture_generation, backend_incarnation;
} ClusterMxHistoryCapture;

/* Operation-aware successor compare input. No canonical identity is inferred
 * from xmin or the tuple's current ITL slot. Backend-private, never wire. */
typedef struct ClusterMxHistorySuccessorChallenge {
	ClusterUndoRecordTarget target;
	TransactionId xmin;
	uint16 member_ordinal, reserved16;
} ClusterMxHistorySuccessorChallenge;
typedef struct ClusterMxHistorySuccessorProof {
	ClusterSavepointOperationKey key;
	ClusterCurrentMemberOperationProof member;
	uint32 requester_capability_generation;
} ClusterMxHistorySuccessorProof;

/* Caller retains its relation lock and buffer pin. Begin/recheck require the
 * original content lock and must be outside critical sections. A single
 * capture per backend owns one reference to the actual retained Snapshot. */
extern bool cluster_mx_history_capture_begin(Relation rel, Buffer buffer, HeapTuple tuple,
											 Snapshot actual, const ClusterCurrentMxKey *mxkey,
											 const ClusterCurrentMxMemberDesc *members,
											 uint16 nmembers,
											 const ClusterSemanticAdmissionToken *admission,
											 ClusterMxHistoryCollector **out);
/* Dormant opt-in updater attempt: copy a page-derived compare input in the
 * same native Snapshot lifetime. B is required and cannot be dropped by the
 * legacy finish API. This does not enable the deployed wire family. */
extern bool cluster_mx_history_capture_begin_with_updater(
	Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual, const ClusterCurrentMxKey *mxkey,
	const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	const ClusterSemanticAdmissionToken *admission, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterMxHistoryCollector **out);
/* Refreshes native scope/admission/owner evidence; failure zeroes out and
 * cancels this capture. No DATA request or proof is produced by this API. */
extern bool cluster_mx_history_capture_read(ClusterMxHistoryCollector *collector,
											ClusterMxHistoryCapture *out);
/* Full relock comparison, including descriptor bytes, not just their hash.
 * False invalidates the entire capture. This does not complete collection. */
extern bool cluster_mx_history_capture_recheck(ClusterMxHistoryCollector *collector, Relation rel,
											   Buffer buffer, HeapTuple tuple,
											   const ClusterCurrentMxKey *mxkey,
											   const ClusterCurrentMxMemberDesc *members,
											   uint16 nmembers);
typedef struct ClusterMxHistoryGraph ClusterMxHistoryGraph;
/* Outside heap content locks: one bounded graph/transport step. COMPLETE
 * means traversal only, never assembled member or SQL authority. */
extern ClusterMxHistoryStep cluster_mx_history_collect_poll(ClusterMxHistoryCollector *collector);
/* Outside heap content locks. COMPLETE means all member results are staged,
 * not delivered or authorized for SQL. No per-member transport is repeated.
 * With a challenge, reuse a qualified B already in the graph; otherwise wait
 * on the original foreign B provenance request (event-driven, cancellable).
 * collect_poll itself remains one bounded graph/transport step. */
extern ClusterMxHistoryStep cluster_mx_history_poll(ClusterMxHistoryCollector *collector);
/* Blocking counterpart, outside heap content locks. Uses original local
 * events, rechecking the capture between bounded steps; ERROR cancels the
 * whole attempt before native Snapshot release. COMPLETE still requires the
 * original full relock/finish and retains the Snapshot until cancel. */
extern ClusterMxHistoryStep cluster_mx_history_wait(ClusterMxHistoryCollector *collector);
/* Original heap lock held: recheck actual Relation/MX/heap/scope before one
 * complete-set copy. Outputs have capacity entries and must not overlap any
 * input. capacity<=256 is reset to UNKNOWN on non-success; larger capacities
 * are rejected without writes. REQUALIFY cancels a changed capture.
 * Successful delivery retains its Snapshot reference until the caller has
 * consumed the results and calls cancel (or original owner cleanup runs).
 * This does not replace proof-set/recompose/before-mutation grant checks. */
extern ClusterMxHistoryStep cluster_mx_history_recheck_and_finish(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs,
	uint16 capacity);
/* Caller rebuilds fresh_challenge under the original content lock. Both A
 * and B are copied once only after exact challenge and full heap/scope checks;
 * B is UNKNOWN on every non-success. The native Snapshot remains retained
 * until cancel, including after successful delivery. Outputs cannot overlap
 * inputs or one another. Original before-mutation grant checks still apply. */
extern ClusterMxHistoryStep cluster_mx_history_recheck_and_finish_with_updater(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs, uint16 capacity,
	const ClusterCurrentUpdaterChallenge *fresh_challenge, ClusterCurrentUpdaterProof *updater);
/* Receipt consumers receive the original qualified request capability for
 * each selected member in the same consume-once batch as keys/A/B. Never
 * reconstruct this generation from a proof or a fresh backend sample.
 * capabilities has capacity entries; non-success clears all bounded entries.
 * A NULL capability output invalidates the attempt. The original grant and
 * final heap/scope checks remain mandatory; no deployed gate is enabled. */
extern ClusterMxHistoryStep cluster_mx_history_recheck_and_finish_for_receipts(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs,
	uint32 *capabilities, uint16 capacity,
	const ClusterCurrentUpdaterChallenge *fresh_challenge, ClusterCurrentUpdaterProof *updater);
/* Full original heap relock comparison before borrowing completed private
 * graph views. The caller retains that lock; cancellation expires the view. */
extern bool cluster_mx_history_collect_recheck_graph(ClusterMxHistoryCollector *collector,
													 Relation rel, Buffer buffer, HeapTuple tuple,
													 const ClusterCurrentMxKey *mxkey,
													 const ClusterCurrentMxMemberDesc *members,
													 uint16 nmembers,
													 const ClusterMxHistoryGraph **out);
/* Idempotent until TopTransactionContext resets. No waiting or retirement.
 * The original ResourceOwner BEFORE_LOCKS/exit callbacks also cancel it. */
extern void cluster_mx_history_cancel(ClusterMxHistoryCollector *collector);
/* Target-aware read attempt; optional raw challenge is retained solely for
 * the original V1 A branch. EXACT A must independently qualify typed B.
 * These backend-private entry points do not activate any deployed family. */
extern bool cluster_mx_history_capture_begin_with_successor(
	Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	const ClusterSemanticAdmissionToken *admission,
	const ClusterMxHistorySuccessorChallenge *challenge,
	const ClusterCurrentUpdaterChallenge *legacy, ClusterMxHistoryCollector **out);
/* Original root/candidate content lock held. One complete A+B copy only;
 * all bounded outputs reset on non-success; old finish cannot drop typed B.
 * The optional legacy challenge must equal the captured V1 compare input.
 * The caller owns the original Snapshot until cancel after consumption. */
extern ClusterMxHistoryStep cluster_mx_history_recheck_and_finish_with_successor(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs,
	uint32 *capabilities, uint16 capacity, const ClusterMxHistorySuccessorChallenge *challenge,
	const ClusterCurrentUpdaterChallenge *legacy, ClusterCurrentUpdaterProof *legacy_out,
	ClusterMxHistorySuccessorProof *out);
#endif
#endif
