/* Actual heap bodies; no USE_CLUSTER_UNIT substitution of identity checks.
 * Portions Copyright (c) 2026, pgrac contributors */
#include "../../backend/access/heap/heapam.c"
#include "test_cluster_mx_operation_receipt.h"
static ClusterCurrentMxHeapResult receipt_result;
ClusterMxHistoryStep
mx_history_collect(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterSemanticAdmissionToken *admission, const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members, uint16 count,
	const ClusterCurrentMxRequestContext *requested, ClusterCurrentMxDecision *decision,
	MxHeapReadout *out)
{
	ClusterCurrentMxOperationState operation = { 0 };
	ClusterMxHistoryStep step;
	operation.operation_id = 900;
	operation.restarts = 4;
	memset(out, 0, sizeof(*out));
	memset(&receipt_result, 0xa5, sizeof(receipt_result));
	memset(&out->wait_key, 0xa5, sizeof(out->wait_key));
	step = cluster_current_mx_history_decide(rel, buffer, tuple, actual, admission,
		key, members, count, requested, &operation, &receipt_result, decision,
		&out->wait_key, &out->trace);
	memcpy(out->retained.normalized, receipt_result.normalized, sizeof(out->retained.normalized));
	memcpy(out->retained.proofs, receipt_result.proofs, sizeof(out->retained.proofs));
	memcpy(out->retained.capabilities, receipt_result.proof_capability_generations,
		sizeof(out->retained.capabilities));
	out->retained.normalized_count = receipt_result.normalized_count;
	out->retained.proof_count = receipt_result.proof_count;
	out->retained.operation_id = receipt_result.operation_id;
	out->retained.attempt_generation = receipt_result.attempt_generation;
	out->result = receipt_result.result;
	out->authoritative_updater = receipt_result.authoritative_updater;
	out->follow_updated_chain = receipt_result.follow_updated_chain;
	out->successor = receipt_result.successor_proof;
	return step;
}
ClusterCurrentMxDecision
mx_receipt_decide(const ClusterCurrentMxMemberDesc *members,
	const ClusterSavepointOperationKey *keys, const ClusterCurrentMemberOperationProof *proofs,
	const uint32 *capabilities, uint16 count, const ClusterCurrentMxRequestContext *ctx,
	const ClusterR4CrOwnerChallenge *owner, const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater, const ItemPointerData *successor_tid,
	uint64 operation_id, uint32 restarts, MxHeapReadout *out)
{
	ClusterCurrentMxOperationState operation = { 0 };
	ClusterCurrentMxDecision decision;
	operation.operation_id = operation_id;
	operation.restarts = restarts;
	memset(out, 0, sizeof(*out));
	memset(&out->wait_key, 0xa5, sizeof(out->wait_key));
	memset(&receipt_result, 0xa5, sizeof(receipt_result));
	decision = cluster_current_mx_result_consume_operations(&receipt_result, members,
		keys, proofs, capabilities, count, ctx, owner, challenge, updater,
		successor_tid, &operation, &out->wait_key, NULL);
	memcpy(out->retained.normalized, receipt_result.normalized, sizeof(out->retained.normalized));
	memcpy(out->retained.proofs, receipt_result.proofs, sizeof(out->retained.proofs));
	memcpy(out->retained.capabilities, receipt_result.proof_capability_generations,
		sizeof(out->retained.capabilities));
	out->retained.normalized_count = receipt_result.normalized_count;
	out->retained.proof_count = receipt_result.proof_count;
	out->retained.operation_id = receipt_result.operation_id;
	out->retained.attempt_generation = receipt_result.attempt_generation;
	out->result = receipt_result.result;
	out->authoritative_updater = receipt_result.authoritative_updater;
	out->follow_updated_chain = receipt_result.follow_updated_chain;
	out->successor = receipt_result.successor_proof;
	return decision;
}
bool
mx_receipt_retain(const ClusterCurrentMxKey *key,
	const ClusterCurrentMxMemberDesc *members, const ClusterSavepointOperationKey *keys,
	const ClusterCurrentMemberOperationProof *proofs, const uint32 *capabilities,
	uint16 count, TransactionId requester, MultiXactStatus status,
	uint64 operation_id, uint32 restarts, MxReceiptReadout *out)
{
	ClusterCurrentMxOperationState operation = { 0 };
	bool ok;
	memset(&receipt_result, 0xa5, sizeof(receipt_result));
	operation.operation_id = operation_id;
	operation.restarts = restarts;
	ok = cluster_current_mx_result_retain_operations(&receipt_result, key, members,
		keys, proofs, capabilities, count, requester, status, &operation);
	memset(out, 0, sizeof(*out));
	memcpy(out->normalized, receipt_result.normalized, sizeof(out->normalized));
	memcpy(out->proofs, receipt_result.proofs, sizeof(out->proofs));
	memcpy(out->capabilities, receipt_result.proof_capability_generations,
		sizeof(out->capabilities));
	out->normalized_count = receipt_result.normalized_count;
	out->proof_count = receipt_result.proof_count;
	out->operation_id = receipt_result.operation_id;
	out->attempt_generation = receipt_result.attempt_generation;
	return ok;
}
bool
mx_receipt_prepare(TransactionId requester, uint64 hash,
	const ClusterCtrcTargetV1 *target, uint16 *count)
{
	ClusterCurrentMxStampPlan plan = { 0 };
	bool ok;
	plan.active = true;
	plan.stage = CMX_HEAP_STAGE_RECEIPT_PREPARED;
	ok = cluster_current_mx_stamp_prepare_slow(&receipt_result,
		receipt_result.normalized, receipt_result.normalized_count, requester, hash,
		target, plan.handles, plan.new_receipts, &plan.receipt_count);
	*count = plan.receipt_count;
	/* Same original cancellation body used by the native plan caller. */
	if (!ok)
		cluster_current_mx_stamp_cancel(&plan);
	return ok;
}
