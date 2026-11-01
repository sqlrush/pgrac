/* Actual heap bodies; no USE_CLUSTER_UNIT substitution of identity checks.
 * Portions Copyright (c) 2026, pgrac contributors */
#include "../../backend/access/heap/heapam.c"
#include "test_cluster_mx_operation_receipt.h"
static ClusterCurrentMxHeapResult receipt_result;
static ClusterCurrentMxStampPlan publication_plan;
static void
mx_publication_readout(MxPublicationReadout *out)
{
	out->descriptor = publication_plan.descriptor_mxid;
	out->descriptor_hash = publication_plan.descriptor_hash;
	out->receipt_count = publication_plan.receipt_count;
	out->stage = publication_plan.stage;
	out->active = publication_plan.active;
	out->apply_started = publication_plan.apply_started;
}
bool
mx_publication_prepare(TransactionId requester, int kind, LockTupleMode mode,
	const ClusterCtrcTargetV1 *target, MxPublicationReadout *out)
{
	TransactionId xmax;
	bool prepared;
	memset(out, 0, sizeof(*out));
	/* Same catch/cancel contract as the native existing-TID caller. */
	PG_TRY();
	{
		prepared = cluster_current_mx_stamp_prepare_plan(&receipt_result, requester,
			(ClusterCurrentMxStampKind) kind, mode, target, &publication_plan,
			&xmax, &out->infomask, &out->infomask2);
	}
	PG_CATCH();
	{
		cluster_current_mx_stamp_cancel(&publication_plan);
		PG_RE_THROW();
	}
	PG_END_TRY();
	mx_publication_readout(out);
	return prepared;
}
int
mx_publication_apply(const ClusterCtrcTargetV1 *target, bool prior_applied,
	MxPublicationReadout *out)
{
	ClusterHeapCurrentMxExactPublication publication = { 0 };
	ClusterHeapNoRetryBoundaryResult result;
	/* The test supplies an already-qualified exact target. This adapter does
	 * not stand in for native page capture or the final boundary recheck. */
	publication.valid = true;
	publication.plan = &publication_plan;
	publication.final_target = *target;
	out->any_applied = prior_applied;
	result = cluster_current_mx_stamp_apply_exact_publication(&publication,
		&out->any_applied);
	mx_publication_readout(out);
	return result;
}
void
mx_publication_cancel(MxPublicationReadout *out)
{
	cluster_current_mx_stamp_cancel(&publication_plan);
	mx_publication_readout(out);
}
static ClusterCurrentMxOperationState authorize_operation;
void
mx_history_authorize_reset(void)
{
	memset(&authorize_operation, 0, sizeof(authorize_operation));
}
void
mx_history_authorize_state(MxAuthorizeState *out)
{
	out->operation_id = authorize_operation.operation_id;
	out->restarts = authorize_operation.restarts;
	out->active = authorize_operation.active;
	out->finished = authorize_operation.finished;
}
int
mx_history_authorize(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterSemanticAdmissionToken *admission, const ClusterCurrentMxRequestContext *requested,
	MxHeapReadout *out)
{
	volatile int disposition = -1;
	memset(out, 0, sizeof(*out));
	memset(&receipt_result, 0xa5, sizeof(receipt_result));
	PG_TRY();
	{
		disposition = cluster_current_mx_history_authorize(rel, buffer, tuple, actual, admission,
			requested->current_member_xid, requested->action, requested->lock_mode,
			requested->action == CCM_ACTION_UPDATE || requested->action == CCM_ACTION_DELETE,
			requested->wait_policy, requested->wait_for_conflict, requested->follow_updates,
			&receipt_result, &authorize_operation);
	}
	PG_FINALLY();
	{
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
	}
	PG_END_TRY();
	return disposition;
}
int
mx_history_authorize_caller(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterCurrentMxRequestContext *requested,
	MxHeapReadout *out)
{
	volatile int disposition = -1;
	memset(out, 0, sizeof(*out));
	memset(&receipt_result, 0xa5, sizeof(receipt_result));
	PG_TRY();
	{
		disposition = cluster_current_mx_history_authorize_caller(rel, buffer, tuple, actual,
			requested->current_member_xid, requested->action, requested->lock_mode,
			requested->action == CCM_ACTION_UPDATE || requested->action == CCM_ACTION_DELETE,
			requested->wait_policy, requested->wait_for_conflict, requested->follow_updates,
			&receipt_result, &authorize_operation);
	}
	PG_FINALLY();
	{
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
	}
	PG_END_TRY();
	return disposition;
}
static ClusterHeapSuccessorProof hot_native_proof;
static HeapHotCurrentMxSuccessorProof hot_full_proof;
bool
mx_history_hot(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	const ClusterSemanticAdmissionToken *admission, bool full, MxHotReadout *out)
{
	volatile bool result = false;
	memset(out, 0xa5, sizeof(*out));
	PG_TRY();
	{
		if (full)
			result = heap_hot_current_mx_history_resolve_updater(rel, buffer, tuple, actual,
				admission, &out->updater, &out->restart, &hot_full_proof, &authorize_operation);
		else
			result = cluster_current_mx_history_hot_updater_for_chain(rel, buffer, tuple, actual,
				admission, &out->updater, &out->restart, &hot_native_proof, &authorize_operation);
	}
	PG_FINALLY();
	{
		memset(&out->successor, 0, sizeof(out->successor));
		if (full && hot_full_proof.valid)
			cluster_current_mx_set_successor_proof(&out->successor, &hot_full_proof.tid,
				hot_full_proof.updater_xid, &hot_full_proof.alias, &hot_full_proof.locator);
		else if (!full)
			out->successor = hot_native_proof;
	}
	PG_END_TRY();
	return result;
}
bool
mx_history_hot_caller(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
	bool full, MxHotReadout *out)
{
	volatile bool result = false;
	memset(out, 0xa5, sizeof(*out));
	PG_TRY();
	{
		if (full)
			result = heap_hot_current_mx_history_resolve_updater_caller(rel, buffer, tuple, actual,
				&out->updater, &out->restart, &hot_full_proof, &authorize_operation);
		else
			result = cluster_current_mx_history_hot_updater_for_chain_caller(rel, buffer, tuple, actual,
				&out->updater, &out->restart, &hot_native_proof, &authorize_operation);
	}
	PG_FINALLY();
	{
		memset(&out->successor, 0, sizeof(out->successor));
		if (full && hot_full_proof.valid)
			cluster_current_mx_set_successor_proof(&out->successor, &hot_full_proof.tid,
				hot_full_proof.updater_xid, &hot_full_proof.alias, &hot_full_proof.locator);
		else if (!full)
			out->successor = hot_native_proof;
	}
	PG_END_TRY();
	return result;
}
bool
mx_history_native_hot_walk(Relation rel, Buffer buffer, Snapshot actual,
	ItemPointer tid, bool latest, bool first_call, bool *all_dead)
{
	HeapTupleData tuple = { 0 };
	TableScanDescData scan = { 0 };
	if (!latest)
		return heap_hot_search_buffer_scoped(tid, rel, buffer, actual, &tuple,
			all_dead, first_call, true);
	scan.rs_rd = rel;
	scan.rs_snapshot = actual;
	heap_get_latest_tid_scoped(&scan, tid, true);
	return ItemPointerGetOffsetNumber(tid) == 2;
}
bool
mx_history_hot_matches(Buffer buffer, HeapTuple candidate, bool full)
{
	return full ? heap_hot_current_mx_successor_matches(buffer, candidate, &hot_full_proof)
		: cluster_current_mx_successor_key_matches(buffer, candidate, &hot_native_proof);
}
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

static HeapHotSearchResult full_companion_result;
int
mx_history_full_companion(Relation rel, Buffer buffer, Snapshot actual,
	ItemPointer tid, bool first_call, bool *all_dead, MxFullReadout *out)
{
	volatile int kind = -1;
	ClusterR4ProjectionScratch empty_projection = {0};
	memset(out, 0, sizeof(*out));
	PG_TRY();
	{
		kind = heap_hot_search_buffer_result_scoped(tid, rel, buffer, actual,
			&full_companion_result, all_dead, first_call, true);
	}
	PG_FINALLY();
	{
		out->kind = full_companion_result.kind;
		out->owned = full_companion_result.tuple.t_data ==
			(HeapTupleHeader)full_companion_result.scratch_page;
		out->cr_full_page = full_companion_result.cr_full_page;
		out->cr_private_projection = full_companion_result.cr_private_projection;
		out->projection_empty = memcmp(&full_companion_result.projection,
			&empty_projection, sizeof(empty_projection)) == 0;
		out->tid = full_companion_result.tuple.t_self;
		out->xmin = out->owned ? HeapTupleHeaderGetRawXmin(full_companion_result.tuple.t_data) : 0;
		out->operation_bytes = sizeof(ClusterCurrentMxOperationState);
	}
	PG_END_TRY();
	return kind;
}
TransactionId
mx_history_full_owned_xmin(void)
{
	Assert(full_companion_result.tuple.t_data ==
		(HeapTupleHeader)full_companion_result.scratch_page);
	return HeapTupleHeaderGetRawXmin(full_companion_result.tuple.t_data);
}

void
mx_history_lock_failure(HeapTuple tuple, TM_Result result, bool handled,
	TM_FailureData *failure, ClusterHeapSuccessorProof *next)
{
	cluster_current_mx_lock_failure(tuple, result, handled, &receipt_result,
		&authorize_operation, failure, next);
}
void
mx_history_lock_successor_fault(unsigned fault)
{
	if (fault == 1) receipt_result.successor_proof.valid = false;
	if (fault == 2) receipt_result.successor_proof.updater_xid++;
	if (fault == 3) receipt_result.authoritative_updater++;
	if (fault == 4) ItemPointerSetOffsetNumber(&receipt_result.successor_proof.tid, 3);
	if (fault == 5) ItemPointerSetInvalid(&receipt_result.successor_proof.tid);
	if (fault == 6) ItemPointerSet(&receipt_result.successor_proof.tid, 3, 1);
	if (fault == 7) receipt_result.authoritative_updater = InvalidTransactionId;
}

/* Same production bracket and follow checks as the dormant native entry.
 * Tests supply the unlocked recursive/I/O work; no native root run claim. */
static ClusterCurrentMxLockScope window_scope;
static ClusterCurrentMxHeapFingerprint window_before;
int
mx_history_lock_window_open(Relation rel, Buffer buffer, HeapTuple tuple,
	Snapshot actual, const ClusterCurrentMxRequestContext *requested, MxHeapReadout *out)
{
	volatile int result = -1;

	memset(&window_scope, 0, sizeof(window_scope));
	memset(&receipt_result, 0, sizeof(receipt_result));
	memset(out, 0, sizeof(*out));
	PG_TRY();
	{
		cluster_current_mx_lock_scope_begin(&window_scope, actual);
		result = cluster_current_mx_history_authorize(rel, buffer, tuple,
			window_scope.actual, &window_scope.admission, requested->current_member_xid,
			requested->action, requested->lock_mode, false, requested->wait_policy,
			requested->wait_for_conflict, requested->follow_updates,
			&receipt_result, &window_scope.operation);
		cluster_current_mx_lock_scope_require(&window_scope);
	}
	PG_CATCH();
	{
		cluster_current_mx_lock_scope_end(&window_scope);
		memset(&receipt_result, 0, sizeof(receipt_result));
		receipt_result.result = TM_BeingModified;
		authorize_operation = window_scope.operation;
		PG_RE_THROW();
	}
	PG_END_TRY();
	authorize_operation = window_scope.operation;
	out->result = receipt_result.result;
	out->authoritative_updater = receipt_result.authoritative_updater;
	out->follow_updated_chain = receipt_result.follow_updated_chain;
	out->successor = receipt_result.successor_proof;
	return result;
}
void mx_history_lock_window_check(void)
{
	cluster_current_mx_lock_scope_require(&window_scope);
}
void
mx_history_lock_window_before(Buffer buffer, HeapTuple tuple)
{
	cluster_current_mx_lock_follow_capture(&window_scope, buffer, tuple,
		&receipt_result, &window_before);
}
bool
mx_history_lock_window_after(Buffer buffer, HeapTuple tuple)
{
	bool valid = cluster_current_mx_lock_follow_recheck(&window_scope, buffer, tuple,
		&window_before, &receipt_result);

	authorize_operation = window_scope.operation;
	return valid;
}
void mx_history_lock_window_close(void)
{
	cluster_current_mx_lock_scope_end(&window_scope);
	authorize_operation = window_scope.operation;
}
