/* Runtime-boundary adapter for actual dormant heap receipt functions. */
#ifndef TEST_CLUSTER_MX_OPERATION_RECEIPT_H
#define TEST_CLUSTER_MX_OPERATION_RECEIPT_H
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_savepoint_reader.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "access/heapam.h"
typedef struct MxReceiptReadout {
	MultiXactMember normalized[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint32 capabilities[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 normalized_count, proof_count;
	uint64 operation_id;
	uint32 attempt_generation;
} MxReceiptReadout;
typedef struct MxHeapReadout {
	MxReceiptReadout retained;
	TM_Result result;
	TransactionId authoritative_updater;
	bool follow_updated_chain;
	ClusterHeapSuccessorProof successor;
	ClusterTTStatusKey wait_key;
	ClusterCurrentMxDecisionTrace trace;
} MxHeapReadout;
/* Native descriptor/PREPARE/APPLY stages, before physical publication. */
typedef struct MxPublicationReadout {
	MultiXactId descriptor;
	uint64 descriptor_hash;
	uint16 receipt_count, infomask, infomask2;
	int stage;
	bool active, apply_started, any_applied;
} MxPublicationReadout;
extern bool mx_publication_prepare(TransactionId requester, int kind,
	LockTupleMode mode, const ClusterCtrcTargetV1 *target, MxPublicationReadout *out);
extern int mx_publication_apply(const ClusterCtrcTargetV1 *target,
	bool prior_applied, MxPublicationReadout *out);
extern void mx_publication_cancel(MxPublicationReadout *out);
struct ClusterR4CrOwnerChallenge;
#include "cluster/cluster_mx_history.h"
typedef struct MxAuthorizeState {
	uint64 operation_id;
	uint32 restarts;
	bool active, finished;
} MxAuthorizeState;
void mx_history_authorize_reset(void);
void mx_history_authorize_state(MxAuthorizeState *out);
int mx_history_authorize(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterSemanticAdmissionToken *admission, const ClusterCurrentMxRequestContext *requested,
	MxHeapReadout *out);
int mx_history_authorize_caller(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterCurrentMxRequestContext *requested, MxHeapReadout *out);
typedef struct MxHotReadout {
	TransactionId updater;
	bool restart;
	ClusterHeapSuccessorProof successor;
} MxHotReadout;
bool mx_history_hot(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterSemanticAdmissionToken *admission, bool full, MxHotReadout *out);
bool mx_history_hot_matches(Buffer buffer, HeapTuple candidate, bool full);
ClusterMxHistoryStep mx_history_collect(Relation rel, Buffer buffer, HeapTuple tuple,
	Snapshot actual, const ClusterSemanticAdmissionToken *admission,
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 count,
	const ClusterCurrentMxRequestContext *requested, ClusterCurrentMxDecision *decision,
	MxHeapReadout *out);
ClusterCurrentMxDecision mx_receipt_decide(
	const ClusterCurrentMxMemberDesc *members, const ClusterSavepointOperationKey *keys,
	const ClusterCurrentMemberOperationProof *proofs, const uint32 *capabilities, uint16 count,
	const ClusterCurrentMxRequestContext *ctx, const struct ClusterR4CrOwnerChallenge *owner,
	const ClusterCurrentUpdaterChallenge *challenge, const ClusterCurrentUpdaterProof *updater,
	const ItemPointerData *successor_tid, uint64 operation_id, uint32 restarts,
	MxHeapReadout *out);
bool mx_receipt_retain(const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members, const ClusterSavepointOperationKey *keys,
	const ClusterCurrentMemberOperationProof *proofs, const uint32 *capabilities,
	uint16 count, TransactionId requester, MultiXactStatus status,
	uint64 operation_id, uint32 restarts, MxReceiptReadout *out);
bool mx_receipt_prepare(TransactionId requester, uint64 hash,
	const ClusterCtrcTargetV1 *target, uint16 *count);
#endif
