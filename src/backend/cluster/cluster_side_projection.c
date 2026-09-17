/*-------------------------------------------------------------------------
 *
 * cluster_side_projection.c
 *	  RF-SIDE D-SIDE-04 — derived-projection judgements (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-04 / §2.4 / §5.1 U-SIDE-08/09/10.
 *
 *	  Pure judgement; the projection store, invalidation triggers and
 *	  rebuild execution stay with the production cluster_remote_xact
 *	  wiring (RED).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_projection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_multixact.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_projection.h"

#define RF_SIDE_CLOG_XACTS_PER_PAGE ((uint32)BLCKSZ * 4)
#define RF_SIDE_COMMIT_TS_ENTRY_BYTES UINT32_C(10)
#define RF_SIDE_COMMIT_TS_XACTS_PER_PAGE ((uint32)BLCKSZ / RF_SIDE_COMMIT_TS_ENTRY_BYTES)

static bool
cluster_side_projection_zero_range(const ClusterSideProjectionApplyInputV1 *input,
								   TransactionId *first_xid, uint32 *xid_count)
{
	const ClusterSideProjectionOperationV1 *operation = input->operation;
	uint32 per_page;
	uint64 first;
	uint64 remaining;

	if (operation->action != CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE || operation->page_number < 0
		|| first_xid == NULL || xid_count == NULL)
		return false;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_CLOG)
		per_page = RF_SIDE_CLOG_XACTS_PER_PAGE;
	else if (operation->kind == CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		per_page = RF_SIDE_COMMIT_TS_XACTS_PER_PAGE;
	else
		return false;
	first = (uint64)(uint32)operation->page_number * per_page;
	if (first > UINT32_MAX)
		return false;
	remaining = (uint64)UINT32_MAX - first + 1;
	*first_xid = (TransactionId)first;
	*xid_count = (uint32)Min((uint64)per_page, remaining);
	return cluster_remote_xact_reset_range_valid_v2(input->origin_thread - 1, *first_xid,
													*xid_count);
}

ClusterSideProjectionApplyResultV1
cluster_side_projection_target_preflight_v1(const ClusterSideProjectionApplyInputV1 *input)
{
	const ClusterSideProjectionOperationV1 *operation;
	TransactionId first_xid;
	uint32 xid_count;

	if (input == NULL || input->operation == NULL || input->origin_thread == 0
		|| input->origin_thread > (1 << 7))
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	operation = input->operation;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_MULTIXACT) {
		uint32 i;

		if (!input->source_retained || input->cluster_epoch == 0
			|| input->source_lsn == InvalidXLogRecPtr || input->source_end_lsn <= input->source_lsn)
			return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_CREATE) {
			const MultiXactMember *members = (const MultiXactMember *)input->owned_payload;

			if (operation->normalized_info != XLOG_MULTIXACT_CREATE_ID
				|| !MultiXactIdIsValid(operation->multixact_id) || operation->member_offset == 0
				|| operation->member_count == 0 || operation->member_count > 256 || members == NULL
				|| input->owned_payload_length != operation->member_count * sizeof(MultiXactMember))
				return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
			for (i = 0; i < operation->member_count; i++)
				if (!TransactionIdIsNormal(members[i].xid)
					|| members[i].status < MultiXactStatusForKeyShare
					|| members[i].status > MaxMultiXactStatus)
					return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		} else if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE) {
			if ((operation->normalized_info != XLOG_MULTIXACT_ZERO_OFF_PAGE
				 && operation->normalized_info != XLOG_MULTIXACT_ZERO_MEM_PAGE)
				|| operation->page_number < 0 || input->owned_payload_length != 0)
				return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		} else if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE) {
			if (operation->normalized_info != XLOG_MULTIXACT_TRUNCATE_ID
				|| !OidIsValid(operation->oldest_database)
				|| !MultiXactIdIsValid(operation->truncate_start_multixact)
				|| !MultiXactIdIsValid(operation->truncate_end_multixact)
				|| input->owned_payload_length != 0)
				return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		} else
			return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	}
	if (operation->kind != CLUSTER_SIDE_PROJECTION_CLOG
		&& operation->kind != CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_COMMIT_TS && !input->source_retained)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE)
		return cluster_side_projection_zero_range(input, &first_xid, &xid_count)
				   ? CLUSTER_SIDE_PROJECTION_APPLY_OK
				   : CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE && operation->page_number >= 0
		&& TransactionIdIsNormal(operation->oldest_xid))
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
}

ClusterSideProjectionApplyResultV1
cluster_side_projection_apply_owned_v1(const ClusterSideProjectionApplyInputV1 *input,
									   const ClusterSideProjectionApplyOpsV1 *ops)
{
	const ClusterSideProjectionOperationV1 *operation;
	TransactionId first_xid;
	uint32 xid_count;
	int origin_slot;

	if (cluster_side_projection_target_preflight_v1(input) != CLUSTER_SIDE_PROJECTION_APPLY_OK
		|| ops == NULL)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	operation = input->operation;
	origin_slot = input->origin_thread - 1;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_MULTIXACT) {
		if (ops->apply_multixact_projection == NULL || ops->verify_multixact_projection == NULL
			|| !ops->apply_multixact_projection(
				ops->arg, origin_slot, input->cluster_epoch, operation, input->owned_payload,
				input->owned_payload_length, input->source_lsn, input->source_end_lsn))
			return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		if (!ops->verify_multixact_projection(
				ops->arg, origin_slot, input->cluster_epoch, operation, input->owned_payload,
				input->owned_payload_length, input->source_lsn, input->source_end_lsn))
			return CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED;
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	}
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE) {
		if (ops->reset_remote_xact_range == NULL || ops->remote_xact_range_empty == NULL
			|| !cluster_side_projection_zero_range(input, &first_xid, &xid_count)
			|| !ops->reset_remote_xact_range(ops->arg, origin_slot, first_xid, xid_count))
			return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		if (!ops->remote_xact_range_empty(ops->arg, origin_slot, first_xid, xid_count))
			return CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED;
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	}
	if (ops->truncate_remote_xact_before == NULL
		|| !ops->truncate_remote_xact_before(ops->arg, origin_slot, operation->oldest_xid))
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	return CLUSTER_SIDE_PROJECTION_APPLY_OK;
}

static bool
side_projection_reset_remote_xact_range(void *arg, int origin_slot, TransactionId first_xid,
										uint32 xid_count)
{
	(void)arg;
	return cluster_remote_xact_reset_range_v2(origin_slot, first_xid, xid_count);
}

static bool
side_projection_remote_xact_range_empty(void *arg, int origin_slot, TransactionId first_xid,
										uint32 xid_count)
{
	(void)arg;
	return cluster_remote_xact_range_empty_v2(origin_slot, first_xid, xid_count);
}

static bool
side_projection_truncate_remote_xact_before(void *arg, int origin_slot, TransactionId oldest_xid)
{
	(void)arg;
	return cluster_remote_xact_truncate_before_v2(origin_slot, oldest_xid);
}

bool
rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner, uint32 cluster_epoch,
										bool failed_origin_redo_retained)
{
	if (owner == NULL || cluster_epoch == 0)
		return false;
	memset(owner, 0, sizeof(*owner));
	owner->cluster_epoch = cluster_epoch;
	owner->failed_origin_redo_retained = failed_origin_redo_retained;
	owner->projection_ops.reset_remote_xact_range = side_projection_reset_remote_xact_range;
	owner->projection_ops.remote_xact_range_empty = side_projection_remote_xact_range_empty;
	owner->projection_ops.truncate_remote_xact_before = side_projection_truncate_remote_xact_before;
	owner->projection_ops.apply_multixact_projection = cluster_multixact_recovery_projection_apply;
	owner->projection_ops.verify_multixact_projection
		= cluster_multixact_recovery_projection_verify;
	return true;
}

static bool
side_projection_owned_input(RfSideOnlineProjectionOwnerV1 *owner,
							const RfSideOnlineOperationV1 *operation,
							ClusterSideProjectionApplyInputV1 *input)
{
	if (owner == NULL || operation == NULL || input == NULL || owner->cluster_epoch == 0
		|| operation->kind != RF_SIDE_ONLINE_OPERATION_PROJECTION)
		return false;
	memset(input, 0, sizeof(*input));
	input->operation = &operation->projection;
	input->owned_payload = operation->owned_payload;
	input->owned_payload_length = operation->owned_payload_length;
	input->origin_thread = operation->identity.record.origin_thread;
	input->source_retained = owner->failed_origin_redo_retained;
	input->cluster_epoch = owner->cluster_epoch;
	input->source_lsn = operation->identity.record.read_rec_ptr;
	input->source_end_lsn = operation->identity.record.end_rec_ptr;
	return true;
}

bool
rf_side_online_projection_preflight_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *owner = (RfSideOnlineProjectionOwnerV1 *)arg;
	ClusterSideProjectionApplyInputV1 input;

	return side_projection_owned_input(owner, operation, &input)
		   && cluster_side_projection_target_preflight_v1(&input)
				  == CLUSTER_SIDE_PROJECTION_APPLY_OK;
}

bool
rf_side_online_projection_apply_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *owner = (RfSideOnlineProjectionOwnerV1 *)arg;
	ClusterSideProjectionApplyInputV1 input;

	return side_projection_owned_input(owner, operation, &input)
		   && cluster_side_projection_apply_owned_v1(&input, &owner->projection_ops)
				  == CLUSTER_SIDE_PROJECTION_APPLY_OK;
}

bool
cluster_side_projection_verified(ClusterSideProjectionKind kind,
								 const ClusterSideProjectionVerifyInput *in)
{
	/*
	 * §2.4: every projection serves only behind the canonical producer
	 * bidirectional match + exact coverage + integrity.  "Durable" is
	 * never "authoritative": the caller re-checks these facts before
	 * every serve.  Any false closes the scope (U-SIDE-08: a local CLOG
	 * bit cannot override the canonical TT/redo truth; U-SIDE-09:
	 * empty/missing is never "no locker"; U-SIDE-10: a missing timestamp
	 * stays unknown, it cannot flip UNKNOWN to COMMITTED).
	 */
	if (in == NULL)
		return false;
	if (!in->canonical_truth_ok || !in->coverage_ok || !in->integrity_ok)
		return false;
	/* The kind itself only selects the contract; the facts are the same
	 * three.  A kind value out of range fails closed. */
	if (kind != CLUSTER_SIDE_PROJECTION_CLOG && kind != CLUSTER_SIDE_PROJECTION_MULTIXACT
		&& kind != CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		return false;
	return true;
}

bool
cluster_side_projection_rebuildable(ClusterSideProjectionKind kind, bool source_retained,
									bool canonical_producer_ok)
{
	/*
	 * §2.4 rebuild rule:
	 *   - CLOG rebuilds from the canonical transaction truth, so the
	 *     failed-origin redo need not be retained for the rebuild;
	 *   - MULTIXACT and COMMIT_TS rebuild from the retained redo — the
	 *     source must still be retained (U-SIDE-09: "在 source redo 仍
	 *     retained 时重建；retire 后若不存在独立可验证 canonical
	 *     source，相关 tuple/resource 保持 BLOCKED").
	 * The canonical producer identity/version/coverage check is required
	 * for every kind (a rebuild without a verified producer is a guess).
	 */
	if (!canonical_producer_ok)
		return false;
	switch (kind) {
	case CLUSTER_SIDE_PROJECTION_CLOG:
		return true; /* canonical truth rebuild */
	case CLUSTER_SIDE_PROJECTION_MULTIXACT:
	case CLUSTER_SIDE_PROJECTION_COMMIT_TS:
		return source_retained;
	}
	return false;
}

ClusterSideProjectionLookup
cluster_side_projection_lookup(bool verified)
{
	/* §2.4 common rule 5: a miss/UNKNOWN fails closed until the rebuild
	 * completes — the judgement adds no synchronous network or durable
	 * I/O (pure function). */
	return verified ? CLUSTER_SIDE_PROJECTION_LOOKUP_OK
					: CLUSTER_SIDE_PROJECTION_LOOKUP_FAIL_CLOSED;
}
