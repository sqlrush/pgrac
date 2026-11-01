/* Actual heap bodies; no USE_CLUSTER_UNIT substitution of identity checks.
 * Portions Copyright (c) 2026, pgrac contributors */
#include "../../backend/access/heap/heapam.c"
#include "test_cluster_mx_operation_receipt.h"
static ClusterCurrentMxHeapResult receipt_result;
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
