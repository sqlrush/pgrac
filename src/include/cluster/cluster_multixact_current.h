/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current.h
 *	  Current-DML authority for cluster MultiXacts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_multixact_current.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_CURRENT_H
#define CLUSTER_MULTIXACT_CURRENT_H

#include "access/multixact.h"
#include "access/tableam.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"
#include "datatype/timestamp.h"
#include "nodes/lockoptions.h"


#define CLUSTER_CURRENT_MX_MAX_MEMBERS 256
#define CLUSTER_CURRENT_MX_MAX_CHUNKS 256
#define CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK 32


/*
 * Stable identity of an immutable member list at its MXID origin.
 */
typedef struct ClusterCurrentMxKey {
	uint16 origin_node_id;
	uint16 reserved16;
	MultiXactId multixact_id;
	uint32 cluster_epoch;
	uint32 reserved32;
} ClusterCurrentMxKey;


/*
 * Immutable member entry returned by the MXID origin.
 */
typedef struct ClusterCurrentMxMemberDesc {
	TransactionId xid;
	uint8 member_status;
	uint8 reserved8[3];
} ClusterCurrentMxMemberDesc;


typedef enum ClusterCurrentMemberState {
	CCM_SELF = 0,
	CCM_ACTIVE,
	CCM_COMMITTED,
	CCM_ABORTED,
	CCM_UNKNOWN
} ClusterCurrentMemberState;


#define CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION UINT16_C(1)

/*
 * Proof-private canonical identity.  The first 16 bytes mirror the routing
 * portion of ClusterTTStatusKey; the final eight bytes are the incarnation
 * sampled by the member origin.  This type must be normalized explicitly
 * before entering a generic TT-status or TX-wait API.
 */
typedef struct ClusterCurrentMemberProofKey {
	uint16 origin_node_id;
	uint16 undo_segment_id;
	uint32 tt_slot_id;
	uint32 cluster_epoch;
	TransactionId local_xid;
	uint32 segment_generation;
	uint16 slot_wrap;
	uint16 binding_version;
} ClusterCurrentMemberProofKey;


/*
 * Current-state proof returned by the origin of member_xid.
 */
typedef struct ClusterCurrentMemberProof {
	ClusterCurrentMemberProofKey key;
	SCN commit_scn;
	TransactionId member_xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 state;
	uint8 reserved8[4];
} ClusterCurrentMemberProof;


static inline void
ClusterCurrentMemberProofSetCtrcBinding(ClusterCurrentMemberProof *proof, uint32 segment_generation,
										uint16 slot_wrap)
{
	proof->key.segment_generation = segment_generation;
	proof->key.slot_wrap = slot_wrap;
	proof->key.binding_version = CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION;
}


static inline bool
ClusterCurrentMemberProofGetStatusKey(const ClusterCurrentMemberProof *proof,
									  ClusterTTStatusKey *status_key, uint32 *segment_generation,
									  uint16 *slot_wrap)
{
	if (proof == NULL || status_key == NULL || segment_generation == NULL || slot_wrap == NULL
		|| (proof->state != CCM_ACTIVE && proof->state != CCM_SELF)
		|| proof->key.binding_version != CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION
		|| proof->key.segment_generation == UINT32_MAX || proof->key.slot_wrap == UINT16_MAX)
		return false;
	memset(status_key, 0, sizeof(*status_key));
	status_key->origin_node_id = proof->key.origin_node_id;
	status_key->undo_segment_id = proof->key.undo_segment_id;
	status_key->tt_slot_id = proof->key.tt_slot_id;
	status_key->cluster_epoch = proof->key.cluster_epoch;
	status_key->local_xid = proof->key.local_xid;
	*segment_generation = proof->key.segment_generation;
	*slot_wrap = proof->key.slot_wrap;
	return true;
}


static inline void
ClusterCurrentMemberProofSetCtrcGrant(ClusterCurrentMemberProof *proof, uint32 grant_generation)
{
	proof->reserved8[0] = (uint8)(grant_generation & UINT32_C(0xff));
	proof->reserved8[1] = (uint8)((grant_generation >> 8) & UINT32_C(0xff));
	proof->reserved8[2] = (uint8)((grant_generation >> 16) & UINT32_C(0xff));
	proof->reserved8[3] = (uint8)((grant_generation >> 24) & UINT32_C(0xff));
}


static inline uint32
ClusterCurrentMemberProofGetCtrcGrant(const ClusterCurrentMemberProof *proof)
{
	return (uint32)proof->reserved8[0] | ((uint32)proof->reserved8[1] << 8)
		   | ((uint32)proof->reserved8[2] << 16) | ((uint32)proof->reserved8[3] << 24);
}


typedef enum ClusterUpdaterCandidateVerdict {
	CUCP_MATCH = 0,
	CUCP_MISMATCH,
	CUCP_UNKNOWN
} ClusterUpdaterCandidateVerdict;


/* Page-derived identity of the successor's undo DATA record.  This is an
 * echo/requalification alias only; its segment is not a canonical TT key. */
typedef struct ClusterCurrentMxSuccessorAlias {
	uint16 origin_node_id;
	uint16 undo_record_segment_id;
	uint32 tt_slot_id;
	uint32 cluster_epoch;
	TransactionId local_xid;
	uint32 reserved32;
	uint32 reserved32_2;
} ClusterCurrentMxSuccessorAlias;


typedef struct ClusterCurrentUpdaterProof {
	ClusterCurrentMxKey mxkey;
	ClusterCurrentMxSuccessorAlias candidate_next_xmin_alias;
	ClusterTxLocator candidate_next_xmin_locator;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint8 verdict;
	uint8 reserved8;
} ClusterCurrentUpdaterProof;


typedef struct ClusterCurrentUpdaterChallenge {
	ClusterCurrentMxSuccessorAlias candidate_next_xmin_alias;
	ClusterTxLocator candidate_next_xmin_locator;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint16 reserved16;
} ClusterCurrentUpdaterChallenge;


typedef enum ClusterMxDescribeResult {
	CMX_DESC_OK = 0,
	CMX_DESC_DENIED,
	CMX_DESC_SUPPORTED_LIMIT,
	CMX_DESC_TIMEOUT,
	CMX_DESC_UNKNOWN
} ClusterMxDescribeResult;


typedef enum ClusterMxResolveResult {
	CMX_RESOLVE_OK = 0,
	CMX_RESOLVE_DENIED,
	CMX_RESOLVE_SUPPORTED_LIMIT,
	CMX_RESOLVE_TIMEOUT,
	CMX_RESOLVE_UNKNOWN,
	/* Whole-batch, zero-output pre-send freshness loss. */
	CMX_RESOLVE_RETRY
} ClusterMxResolveResult;


typedef enum ClusterCurrentTupleAction {
	CCM_ACTION_UPDATE = 0,
	CCM_ACTION_DELETE,
	CCM_ACTION_LOCK,
	CCM_ACTION_HOT_FOLLOW
} ClusterCurrentTupleAction;


typedef enum ClusterCurrentTupleShape {
	CCM_SHAPE_LOCK_ONLY = 0,
	CCM_SHAPE_UPDATED,
	CCM_SHAPE_DELETED
} ClusterCurrentTupleShape;


/*
 * Requester-local decision context.  This structure is not a wire ABI.
 */
typedef struct ClusterCurrentMxRequestContext {
	ClusterCurrentMxKey mxkey;
	TransactionId top_xid;
	TransactionId current_member_xid;
	CommandId curcid;
	CommandId tuple_cmax;
	TM_Result precheck_result;
	MultiXactStatus desired_status;
	LockTupleMode lock_mode;
	LockWaitPolicy wait_policy;
	uint8 action;
	uint8 tuple_shape;
	uint8 follow_updates;
	uint8 wait_for_conflict;
	/* Requester-derived updater member origin; -1 when no challenge exists. */
	int32 updater_origin_node_id;
} ClusterCurrentMxRequestContext;


typedef enum ClusterCurrentMxDecision {
	CMDL_CONTINUE = 0,
	CMDL_INVISIBLE,
	CMDL_SELF_MODIFIED,
	CMDL_BEING_MODIFIED,
	CMDL_WAIT_MEMBER,
	CMDL_WOULD_BLOCK,
	CMDL_LOCK_NOT_AVAILABLE,
	CMDL_UPDATED,
	CMDL_DELETED,
	CMDL_UNKNOWN,
	CMDL_FOLLOW_UPDATED
} ClusterCurrentMxDecision;


typedef enum ClusterCurrentMxUnknownReason {
	CMX_UNKNOWN_NONE = 0,
	CMX_UNKNOWN_REQUEST_CONTEXT,
	CMX_UNKNOWN_DESCRIPTOR,
	CMX_UNKNOWN_TUPLE_SHAPE,
	CMX_UNKNOWN_PROOFS_NULL,
	CMX_UNKNOWN_PROOF_ENTRY,
	CMX_UNKNOWN_STATUS_MODE,
	CMX_UNKNOWN_SELF_XID,
	CMX_UNKNOWN_MEMBER_STATE,
	CMX_UNKNOWN_COMMITTED_UPDATER_SHAPE,
	CMX_UNKNOWN_COMMITTED_UPDATER_PROOF,
	CMX_UNKNOWN_ACTIVE_UPDATER_SHAPE,
	CMX_UNKNOWN_ACTIVE_UPDATER_PROOF
} ClusterCurrentMxUnknownReason;


typedef struct ClusterCurrentMxDecisionTrace {
	ClusterCurrentMxUnknownReason unknown_reason;
	int32 member_ordinal;
} ClusterCurrentMxDecisionTrace;


/*
 * Requester-local validated view of one proof reply chunk.  The pointed-to
 * proof entries use the stable wire structure above; the pointer itself is
 * never transmitted.
 */
typedef struct ClusterCurrentProofChunkView {
	uint64 request_id;
	ClusterCurrentMxKey mxkey;
	uint64 descriptor_hash;
	uint32 total_count;
	uint16 source_node_id;
	uint16 chunk_ordinal;
	uint16 chunk_count;
	uint16 proof_count;
	uint16 reserved16;
	const ClusterCurrentMemberProof *proofs;
} ClusterCurrentProofChunkView;


typedef bool (*ClusterCurrentMxExactLookupFn)(const ClusterTTStatusKey *key,
											  ClusterTTStatusResult *result, void *arg);


/*
 * Identity for an operation-local immutable-descriptor memo.  The tuple/PCM-X
 * fingerprint is opaque to this module and supplied by the heap caller.
 */
typedef struct ClusterCurrentMxOperationFingerprint {
	uint64 operation_id;
	ClusterCurrentMxKey mxkey;
	MultiXactId captured_raw_xmax;
	uint32 reserved32;
	uint64 tuple_pcm_fingerprint_hi;
	uint64 tuple_pcm_fingerprint_lo;
} ClusterCurrentMxOperationFingerprint;


typedef enum ClusterMxRecomposeResult {
	CMX_RECOMPOSE_OK = 0,
	CMX_RECOMPOSE_DENIED,
	CMX_RECOMPOSE_SUPPORTED_LIMIT,
	CMX_RECOMPOSE_UNKNOWN
} ClusterMxRecomposeResult;


/* Caller-specific fixed-header publication plan.  The planner is pure: it
 * copies one fixed heap header and returns the exact bytes the caller will
 * publish after every matching CTRC receipt is APPLIED. */
typedef enum ClusterCurrentMxHeapPublishKind {
	CMX_HEAP_PUBLISH_DELETE = 0,
	CMX_HEAP_PUBLISH_UPDATE_OLD,
	CMX_HEAP_PUBLISH_UPDATE_NEW,
	CMX_HEAP_PUBLISH_TEMP_LOCK,
	CMX_HEAP_PUBLISH_TUPLE_LOCK
} ClusterCurrentMxHeapPublishKind;

typedef struct ClusterCurrentMxHeapHeaderPlan {
	ClusterCurrentMxHeapPublishKind kind;
	MultiXactId multixact_id;
	TransactionId xmin;
	CommandId command_id;
	ItemPointerData self_tid;
	ItemPointerData successor_tid;
	uint16 infomask;
	uint16 infomask2;
	uint8 itl_slot_index;
	bool command_is_combo;
	bool changing_partition;
	bool hot_update;
} ClusterCurrentMxHeapHeaderPlan;

typedef enum ClusterCurrentMxHeapPublishStage {
	CMX_HEAP_STAGE_LOCAL_DESCRIPTOR = 0,
	CMX_HEAP_STAGE_RECEIPT_PREPARED,
	CMX_HEAP_STAGE_RECEIPT_APPLIED,
	CMX_HEAP_STAGE_REFERENCE_PUBLISHED,
	CMX_HEAP_STAGE_CANCELLED
} ClusterCurrentMxHeapPublishStage;

typedef enum ClusterCurrentMxHeapPublishEvent {
	CMX_HEAP_EVENT_PREPARE = 0,
	CMX_HEAP_EVENT_RETRY,
	CMX_HEAP_EVENT_ERROR,
	CMX_HEAP_EVENT_APPLY,
	CMX_HEAP_EVENT_PUBLISH
} ClusterCurrentMxHeapPublishEvent;


StaticAssertDecl(sizeof(ClusterCurrentMxKey) == 16, "ClusterCurrentMxKey must remain 16 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxMemberDesc) == 8,
				 "ClusterCurrentMxMemberDesc must remain 8 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMemberProofKey) == 24,
				 "ClusterCurrentMemberProofKey must remain 24 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMemberProof) == 48,
				 "ClusterCurrentMemberProof must remain 48 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxSuccessorAlias) == 24,
				 "ClusterCurrentMxSuccessorAlias must remain 24 bytes");
StaticAssertDecl(sizeof(ClusterCurrentUpdaterProof) == 72,
				 "ClusterCurrentUpdaterProof must remain 72 bytes");
StaticAssertDecl(sizeof(ClusterCurrentUpdaterChallenge) == 56,
				 "ClusterCurrentUpdaterChallenge must remain 56 bytes");


extern ClusterMxDescribeResult cluster_multixact_current_validate_descriptor(
	const ClusterCurrentMxKey *key, uint16 source_node_id, uint32 current_epoch,
	const ClusterCurrentMxMemberDesc *members, uint16 nmembers, uint32 reported_total_members);
extern uint64 cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
														const ClusterCurrentMxMemberDesc *members,
														uint16 nmembers);
extern ClusterMxResolveResult cluster_multixact_current_validate_proof_set(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	/* Requester-derived before send; never copied from reply data. */
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 request_id, uint64 descriptor_hash,
	const ClusterCurrentProofChunkView *chunks, uint16 nchunks,
	ClusterCurrentMemberProof *ordered_proofs);
extern bool cluster_multixact_current_resolve_origin_member_proof(
	TransactionId member_xid, uint8 member_status, uint16 member_ordinal, uint16 member_origin_node,
	uint32 current_epoch, bool requester_self, const ClusterTTStatusKey *initial_key,
	const ClusterTTStatusResult *initial_result, ClusterCurrentMxExactLookupFn exact_lookup,
	void *exact_lookup_arg, ClusterCurrentMemberProof *proof);
extern bool
cluster_multixact_current_member_proof_bind_ctrc(ClusterCurrentMemberProof *proof,
												 const struct ClusterCtrcTxnKeyV1 *ctrc_key);
extern ClusterUpdaterCandidateVerdict cluster_multixact_current_updater_candidate_verdict(
	const ClusterTTStatusKey *candidate, TransactionId updater_xid, uint16 updater_origin_node,
	uint32 current_epoch, ClusterTTStatusKey *current_binding,
	ClusterTTStatusResult *current_result);
extern bool cluster_multixact_current_successor_provenance_well_formed(
	const ClusterCurrentMxSuccessorAlias *alias, const ClusterTxLocator *locator,
	TransactionId updater_xid, uint16 updater_origin_node, uint32 current_epoch);
extern bool cluster_multixact_current_validate_updater_proof(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof, uint16 updater_origin_node_id);
extern bool cluster_multixact_current_status_conflicts(uint8 member_status,
													   LockTupleMode wanted_mode, bool *valid_out);
extern ClusterCurrentMxDecision cluster_multixact_current_decide(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 nmembers, const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof, ClusterTTStatusKey *wait_key);
extern ClusterCurrentMxDecision cluster_multixact_current_decide_observed(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 nmembers, const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof, ClusterTTStatusKey *wait_key,
	ClusterCurrentMxDecisionTrace *trace);

extern ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members, uint16 members_cap,
								   uint16 *nmembers, uint32 *reported_total_members);
extern ClusterMxResolveResult cluster_multixact_current_members_resolve(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *proof_capability_generations);
extern ClusterMxResolveResult cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *proof_capability_generations, TimestampTz *operation_deadline);
extern ClusterMxRecomposeResult cluster_multixact_current_recompose(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 nmembers, TransactionId requester_xid, MultiXactStatus requester_status,
	MultiXactMember *normalized_members, uint16 normalized_cap, uint16 *normalized_count);
extern bool cluster_multixact_current_plan_heap_header(const void *base_header, Size header_size,
													   const ClusterCurrentMxHeapHeaderPlan *plan,
													   void *planned_header);
extern bool
cluster_multixact_current_heap_publish_transition(ClusterCurrentMxHeapPublishStage stage,
												  ClusterCurrentMxHeapPublishEvent event,
												  ClusterCurrentMxHeapPublishStage *next_stage);

#endif /* CLUSTER_MULTIXACT_CURRENT_H */
