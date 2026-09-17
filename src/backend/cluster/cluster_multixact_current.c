/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current.c
 *	  Pure validation and decision core for current-DML MultiXacts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_current.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/lock.h"

#define CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH 1024


static bool
current_mx_key_equal(const ClusterCurrentMxKey *a, const ClusterCurrentMxKey *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}


static bool
current_mx_key_valid(const ClusterCurrentMxKey *key)
{
	return key != NULL && key->origin_node_id < CLUSTER_MAX_NODES && key->reserved16 == 0
		   && key->reserved32 == 0 && MultiXactIdIsValid(key->multixact_id);
}


static bool
proof_key_is_zero(const ClusterCurrentMemberProofKey *key)
{
	static const ClusterCurrentMemberProofKey zero_key;

	return memcmp(key, &zero_key, sizeof(*key)) == 0;
}


static void
proof_key_set_unbound(ClusterCurrentMemberProofKey *proof_key, const ClusterTTStatusKey *status_key)
{
	memset(proof_key, 0, sizeof(*proof_key));
	proof_key->origin_node_id = status_key->origin_node_id;
	proof_key->undo_segment_id = status_key->undo_segment_id;
	proof_key->tt_slot_id = status_key->tt_slot_id;
	proof_key->cluster_epoch = status_key->cluster_epoch;
	proof_key->local_xid = status_key->local_xid;
}


static bool
tt_key_valid(const ClusterTTStatusKey *key, TransactionId xid, uint32 epoch, int expected_origin)
{
	if (key == NULL || key->origin_node_id >= CLUSTER_MAX_NODES || key->_reserved != 0
		|| key->_reserved2 != 0 || key->undo_segment_id == 0 || key->tt_slot_id == 0
		|| key->cluster_epoch != epoch || !TransactionIdIsNormal(key->local_xid)
		|| (TransactionIdIsValid(xid) && key->local_xid != xid))
		return false;
	if (expected_origin >= 0 && key->origin_node_id != (uint16)expected_origin)
		return false;
	return true;
}


static bool
tt_key_valid_holder(const ClusterTTStatusKey *key, TransactionId member_xid, uint32 epoch,
					int expected_origin)
{
	return tt_key_valid(key, InvalidTransactionId, epoch, expected_origin)
		   && (key->local_xid == member_xid || TransactionIdPrecedes(key->local_xid, member_xid));
}


static bool
proof_key_valid_holder(const ClusterCurrentMemberProof *proof, TransactionId member_xid,
					   uint32 epoch, int expected_origin)
{
	ClusterTTStatusKey status_key;
	uint32 segment_generation;
	uint16 slot_wrap;

	return ClusterCurrentMemberProofGetStatusKey(proof, &status_key, &segment_generation,
												 &slot_wrap)
		   && slot_wrap <= TT_WRAP_MAX
		   && tt_key_valid_holder(&status_key, member_xid, epoch, expected_origin);
}


bool
cluster_multixact_current_member_proof_bind_ctrc(ClusterCurrentMemberProof *proof,
												 const ClusterCtrcTxnKeyV1 *ctrc_key)
{
	if (proof == NULL || ctrc_key == NULL
		|| (proof->state != CCM_ACTIVE && proof->state != CCM_SELF)
		|| ClusterCurrentMemberProofGetCtrcGrant(proof) == 0
		|| ctrc_key->format_version != CLUSTER_CTRC_FORMAT_VERSION
		|| ctrc_key->owner_instance != ctrc_key->origin_node_id + 1 || ctrc_key->segment_id == 0
		|| ctrc_key->segment_id > UINT16_MAX || ctrc_key->segment_generation == UINT32_MAX
		|| ctrc_key->slot_offset >= TT_SLOTS_PER_SEGMENT || ctrc_key->slot_wrap > TT_WRAP_MAX
		|| proof->key.origin_node_id != ctrc_key->origin_node_id
		|| proof->key.undo_segment_id != (uint16)ctrc_key->segment_id
		|| proof->key.tt_slot_id != cluster_tt_slot_offset_to_id(ctrc_key->slot_offset)
		|| proof->key.cluster_epoch != ctrc_key->cluster_epoch
		|| proof->key.local_xid != ctrc_key->xid)
		return false;
	ClusterCurrentMemberProofSetCtrcBinding(proof, ctrc_key->segment_generation,
											ctrc_key->slot_wrap);
	return true;
}


bool
cluster_multixact_current_resolve_origin_member_proof(
	TransactionId member_xid, uint8 member_status, uint16 member_ordinal, uint16 member_origin_node,
	uint32 current_epoch, bool requester_self, const ClusterTTStatusKey *initial_key,
	const ClusterTTStatusResult *initial_result, ClusterCurrentMxExactLookupFn exact_lookup,
	void *exact_lookup_arg, ClusterCurrentMemberProof *proof)
{
	ClusterTTStatusKey seen_keys[CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH + 1];
	ClusterTTStatusKey resolved_key;
	ClusterTTStatusResult resolved_result;
	uint16 followed_count = 0;
	uint16 seen_count = 0;

	if (proof != NULL) {
		memset(proof, 0, sizeof(*proof));
		proof->state = CCM_UNKNOWN;
	}
	if (proof == NULL || !TransactionIdIsNormal(member_xid) || member_status > MaxMultiXactStatus
		|| member_origin_node >= CLUSTER_MAX_NODES || initial_key == NULL || initial_result == NULL
		|| !initial_result->authoritative || initial_result->status_epoch != current_epoch
		|| !tt_key_valid(initial_key, member_xid, current_epoch, member_origin_node))
		return false;

	resolved_key = *initial_key;
	resolved_result = *initial_result;
	seen_keys[seen_count++] = resolved_key;

	while (resolved_result.status == CLUSTER_TT_STATUS_SUBCOMMITTED) {
		ClusterTTStatusKey parent_key;
		ClusterTTStatusResult parent_result;
		uint16 i;

		if (!resolved_result.has_parent_key || exact_lookup == NULL
			|| cluster_subtrans_max_chain_depth <= 0
			|| followed_count >= (uint16)Min(cluster_subtrans_max_chain_depth,
											 CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH)
			|| seen_count >= lengthof(seen_keys))
			goto unknown;
		parent_key = resolved_result.parent_key;
		if (!tt_key_valid(&parent_key, InvalidTransactionId, current_epoch, member_origin_node)
			|| !TransactionIdPrecedes(parent_key.local_xid, resolved_key.local_xid))
			goto unknown;
		for (i = 0; i < seen_count; i++)
			if (memcmp(&seen_keys[i], &parent_key, sizeof(parent_key)) == 0)
				goto unknown;
		memset(&parent_result, 0, sizeof(parent_result));
		if (!exact_lookup(&parent_key, &parent_result, exact_lookup_arg)
			|| !parent_result.authoritative || parent_result.status_epoch != current_epoch)
			goto unknown;
		seen_keys[seen_count++] = parent_key;
		followed_count++;
		resolved_key = parent_key;
		resolved_result = parent_result;
	}

	proof->member_xid = member_xid;
	proof->member_ordinal = member_ordinal;
	proof->member_status = member_status;
	switch (resolved_result.status) {
	case CLUSTER_TT_STATUS_IN_PROGRESS:
		if (resolved_result.has_parent_key || resolved_result.commit_scn != InvalidScn)
			goto unknown;
		proof_key_set_unbound(&proof->key, &resolved_key);
		proof->state = requester_self ? CCM_SELF : CCM_ACTIVE;
		return true;
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		if (resolved_result.has_parent_key || !SCN_VALID(resolved_result.commit_scn))
			goto unknown;
		proof->commit_scn = resolved_result.commit_scn;
		proof->state = CCM_COMMITTED;
		return true;
	case CLUSTER_TT_STATUS_ABORTED:
		if (resolved_result.has_parent_key || resolved_result.commit_scn != InvalidScn)
			goto unknown;
		proof->state = CCM_ABORTED;
		return true;
	case CLUSTER_TT_STATUS_UNKNOWN:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
		break;
	}

unknown:
	memset(proof, 0, sizeof(*proof));
	proof->state = CCM_UNKNOWN;
	return false;
}


ClusterUpdaterCandidateVerdict
cluster_multixact_current_updater_candidate_verdict(const ClusterTTStatusKey *candidate,
													TransactionId updater_xid,
													uint16 updater_origin_node,
													uint32 current_epoch,
													ClusterTTStatusKey *current_binding,
													ClusterTTStatusResult *current_result)
{
	ClusterTTStatusKey sampled_binding;
	ClusterTTStatusResult sampled_result;

	if (current_binding != NULL)
		memset(current_binding, 0, sizeof(*current_binding));
	if (current_result != NULL) {
		memset(current_result, 0, sizeof(*current_result));
		current_result->status = CLUSTER_TT_STATUS_UNKNOWN;
		current_result->commit_scn = InvalidScn;
	}
	if (candidate == NULL || current_binding == NULL || current_result == NULL
		|| updater_origin_node >= CLUSTER_MAX_NODES || cluster_node_id < 0
		|| updater_origin_node != (uint16)cluster_node_id
		|| !tt_key_valid(candidate, updater_xid, current_epoch, updater_origin_node))
		return CUCP_UNKNOWN;

	memset(&sampled_binding, 0, sizeof(sampled_binding));
	memset(&sampled_result, 0, sizeof(sampled_result));
	if (!(cluster_runtime_visibility_current_owner_lookup_exact(updater_xid, &sampled_binding,
																&sampled_result)
		  || cluster_runtime_visibility_local_terminal_lookup_exact(updater_xid, &sampled_binding,
																	&sampled_result))
		|| !sampled_result.authoritative || sampled_result.status_epoch != current_epoch
		|| !tt_key_valid(&sampled_binding, updater_xid, current_epoch, updater_origin_node))
		return CUCP_UNKNOWN;

	if (memcmp(candidate, &sampled_binding, sizeof(*candidate)) == 0) {
		*current_binding = sampled_binding;
		*current_result = sampled_result;
		return CUCP_MATCH;
	}
	/* A page alias and a canonical TT key occupy different identity domains.
	 * This legacy helper has no durable L -> R -> C edge, so disagreement can
	 * only be diagnostic and must never manufacture a stable MISMATCH. */
	return CUCP_UNKNOWN;
}


bool
cluster_multixact_current_successor_provenance_well_formed(
	const ClusterCurrentMxSuccessorAlias *alias, const ClusterTxLocator *locator,
	TransactionId updater_xid, uint16 updater_origin_node, uint32 current_epoch)
{
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	uint32 derived_origin;
	bool data_kind;

	if (alias == NULL || locator == NULL || updater_origin_node >= CLUSTER_MAX_NODES
		|| !TransactionIdIsNormal(updater_xid)
		|| !uba_decode_record(locator->uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
		return false;
	data_kind = locator->itl_kind == ITL_FLAG_ACTIVE || locator->itl_kind == ITL_FLAG_COMMITTED
				|| locator->itl_kind == ITL_FLAG_ABORTED
				|| locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	derived_origin = (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE;
	return data_kind && segment_id <= UINT16_MAX && derived_origin == updater_origin_node
		   && row_offset < (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry)
		   && locator->xid == updater_xid && locator->tt_wrap == TT_WRAP_INVALID
		   && locator->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT
		   && alias->origin_node_id == updater_origin_node
		   && alias->undo_record_segment_id == (uint16)segment_id
		   && alias->tt_slot_id == (uint32)tt_slot_offset + 1
		   && alias->cluster_epoch == current_epoch && alias->local_xid == updater_xid
		   && alias->reserved32 == 0 && alias->reserved32_2 == 0;
}


/* Local member proof sampling has two disjoint authority paths.  ACTIVE/SELF
 * must record the exact CTRC touch and return its participant capability
 * generation; terminal state is sampled from the exact current-or-rolled
 * physical slot and deliberately carries neither grant nor participant. */
static bool
current_mx_local_member_sample_exact(TransactionId xid, ClusterTTStatusKey *key,
									 ClusterTTStatusResult *result, uint32 *ctrc_grant,
									 uint32 *participant_capability_generation,
									 ClusterCtrcTxnKeyV1 *ctrc_key_out)
{
	ClusterCtrcTxnKeyV1 ctrc_key;
	ClusterCtrcParticipantIdentity participant;

	if (key == NULL || result == NULL || ctrc_grant == NULL
		|| participant_capability_generation == NULL || ctrc_key_out == NULL)
		return false;
	MemSet(key, 0, sizeof(*key));
	MemSet(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	result->commit_scn = InvalidScn;
	*ctrc_grant = 0;
	*participant_capability_generation = 0;
	MemSet(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	MemSet(&ctrc_key, 0, sizeof(ctrc_key));
	MemSet(&participant, 0, sizeof(participant));
	if (cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
			xid, key, result, ctrc_grant, &ctrc_key, &participant)) {
		if (result->status != CLUSTER_TT_STATUS_IN_PROGRESS || *ctrc_grant == 0
			|| participant.capability_record_generation == 0)
			return false;
		*participant_capability_generation = participant.capability_record_generation;
		*ctrc_key_out = ctrc_key;
		return true;
	}

	MemSet(key, 0, sizeof(*key));
	MemSet(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	result->commit_scn = InvalidScn;
	*ctrc_grant = 0;
	return cluster_runtime_visibility_local_terminal_lookup_exact(xid, key, result)
		   && (result->status == CLUSTER_TT_STATUS_COMMITTED
			   || result->status == CLUSTER_TT_STATUS_ABORTED);
}


static bool
descriptor_entries_valid(const ClusterCurrentMxMemberDesc *members, uint16 nmembers)
{
	int updater_count = 0;
	uint16 i;
	uint16 j;

	if (members == NULL || nmembers < 1 || nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return false;

	for (i = 0; i < nmembers; i++) {
		const ClusterCurrentMxMemberDesc *member = &members[i];

		if (!TransactionIdIsNormal(member->xid) || member->member_status > MaxMultiXactStatus
			|| member->reserved8[0] != 0 || member->reserved8[1] != 0 || member->reserved8[2] != 0)
			return false;
		if (ISUPDATE_from_mxstatus(member->member_status) && ++updater_count > 1)
			return false;

		for (j = 0; j < i; j++)
			if (members[j].xid == member->xid)
				return false;
	}

	return true;
}


static bool
descriptor_shape_matches(const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
						 ClusterCurrentTupleShape shape)
{
	bool has_updater = false;
	uint16 i;

	for (i = 0; i < nmembers; i++)
		if (ISUPDATE_from_mxstatus(members[i].member_status)) {
			has_updater = true;
			break;
		}

	return shape == CCM_SHAPE_LOCK_ONLY ? !has_updater : has_updater;
}


bool
cluster_multixact_current_plan_heap_header(const void *base_header, Size header_size,
										   const ClusterCurrentMxHeapHeaderPlan *plan,
										   void *planned_header)
{
	HeapTupleHeaderData storage;
	HeapTupleHeader header = &storage;

	if (base_header == NULL || plan == NULL || planned_header == NULL
		|| header_size != SizeofHeapTupleHeader || plan->kind < CMX_HEAP_PUBLISH_DELETE
		|| plan->kind > CMX_HEAP_PUBLISH_TUPLE_LOCK || !MultiXactIdIsValid(plan->multixact_id)
		|| (plan->infomask & HEAP_XMAX_IS_MULTI) == 0
		|| (plan->itl_slot_index != CLUSTER_ITL_SLOT_UNALLOCATED
			&& plan->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT))
		return false;
	if (!ItemPointerIsValid(&plan->self_tid)
		|| (plan->kind == CMX_HEAP_PUBLISH_UPDATE_OLD && !ItemPointerIsValid(&plan->successor_tid)))
		return false;

	memcpy(&storage, base_header, header_size);

	switch (plan->kind) {
	case CMX_HEAP_PUBLISH_DELETE:
		header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		header->t_infomask |= plan->infomask;
		header->t_infomask2 |= plan->infomask2;
		HeapTupleHeaderClearHotUpdated(header);
		HeapTupleHeaderSetXmax(header, plan->multixact_id);
		HeapTupleHeaderSetCmax(header, plan->command_id, plan->command_is_combo);
		header->t_ctid = plan->self_tid;
		if (plan->changing_partition)
			HeapTupleHeaderSetMovedPartitions(header);
		break;

	case CMX_HEAP_PUBLISH_UPDATE_OLD:
		if (plan->changing_partition)
			return false;
		if (plan->hot_update)
			HeapTupleHeaderSetHotUpdated(header);
		else
			HeapTupleHeaderClearHotUpdated(header);
		header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		header->t_infomask |= plan->infomask;
		header->t_infomask2 |= plan->infomask2;
		HeapTupleHeaderSetXmax(header, plan->multixact_id);
		HeapTupleHeaderSetCmax(header, plan->command_id, plan->command_is_combo);
		header->t_ctid = plan->successor_tid;
		break;

	case CMX_HEAP_PUBLISH_UPDATE_NEW:
		if (plan->changing_partition || plan->command_is_combo
			|| !TransactionIdIsNormal(plan->xmin))
			return false;
		header->t_infomask &= ~HEAP_XACT_MASK;
		header->t_infomask2 &= ~HEAP2_XACT_MASK;
		HeapTupleHeaderSetXmin(header, plan->xmin);
		HeapTupleHeaderSetCmin(header, plan->command_id);
		header->t_infomask |= HEAP_UPDATED | plan->infomask;
		header->t_infomask2 |= plan->infomask2;
		HeapTupleHeaderSetXmax(header, plan->multixact_id);
		if (plan->hot_update)
			HeapTupleHeaderSetHeapOnly(header);
		else
			HeapTupleHeaderClearHeapOnly(header);
		header->t_ctid = plan->self_tid;
		break;

	case CMX_HEAP_PUBLISH_TEMP_LOCK:
		if (plan->changing_partition || plan->hot_update)
			return false;
		header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleHeaderClearHotUpdated(header);
		HeapTupleHeaderSetXmax(header, plan->multixact_id);
		header->t_infomask |= plan->infomask;
		header->t_infomask2 |= plan->infomask2;
		HeapTupleHeaderSetCmax(header, plan->command_id, plan->command_is_combo);
		header->t_ctid = plan->self_tid;
		break;

	case CMX_HEAP_PUBLISH_TUPLE_LOCK:
		if (plan->changing_partition || plan->hot_update || plan->command_is_combo)
			return false;
		header->t_infomask &= ~HEAP_XMAX_BITS;
		header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		header->t_infomask |= plan->infomask;
		header->t_infomask2 |= plan->infomask2;
		if (HEAP_XMAX_IS_LOCKED_ONLY(plan->infomask)) {
			HeapTupleHeaderClearHotUpdated(header);
			header->t_ctid = plan->self_tid;
		}
		HeapTupleHeaderSetXmax(header, plan->multixact_id);
		break;
	}

	if (plan->itl_slot_index != CLUSTER_ITL_SLOT_UNALLOCATED)
		header->t_itl_slot_idx = plan->itl_slot_index;
	memcpy(planned_header, &storage, header_size);
	return true;
}


bool
cluster_multixact_current_heap_publish_transition(ClusterCurrentMxHeapPublishStage stage,
												  ClusterCurrentMxHeapPublishEvent event,
												  ClusterCurrentMxHeapPublishStage *next_stage)
{
	if (next_stage == NULL)
		return false;

	if (stage == CMX_HEAP_STAGE_LOCAL_DESCRIPTOR && event == CMX_HEAP_EVENT_PREPARE)
		*next_stage = CMX_HEAP_STAGE_RECEIPT_PREPARED;
	else if (stage == CMX_HEAP_STAGE_RECEIPT_PREPARED && event == CMX_HEAP_EVENT_RETRY)
		*next_stage = CMX_HEAP_STAGE_CANCELLED;
	else if ((stage == CMX_HEAP_STAGE_LOCAL_DESCRIPTOR || stage == CMX_HEAP_STAGE_RECEIPT_PREPARED)
			 && event == CMX_HEAP_EVENT_ERROR)
		*next_stage = CMX_HEAP_STAGE_CANCELLED;
	else if ((stage == CMX_HEAP_STAGE_RECEIPT_APPLIED
			  || stage == CMX_HEAP_STAGE_REFERENCE_PUBLISHED)
			 && event == CMX_HEAP_EVENT_ERROR)
		*next_stage = stage;
	else if (stage == CMX_HEAP_STAGE_RECEIPT_PREPARED && event == CMX_HEAP_EVENT_APPLY)
		*next_stage = CMX_HEAP_STAGE_RECEIPT_APPLIED;
	else if (stage == CMX_HEAP_STAGE_RECEIPT_APPLIED && event == CMX_HEAP_EVENT_PUBLISH)
		*next_stage = CMX_HEAP_STAGE_REFERENCE_PUBLISHED;
	else
		return false;
	return true;
}


ClusterMxDescribeResult
cluster_multixact_current_validate_descriptor(const ClusterCurrentMxKey *key, uint16 source_node_id,
											  uint32 current_epoch,
											  const ClusterCurrentMxMemberDesc *members,
											  uint16 nmembers, uint32 reported_total_members)
{
	if (!current_mx_key_valid(key) || key->origin_node_id != source_node_id
		|| key->cluster_epoch != current_epoch)
		return CMX_DESC_DENIED;

	if (reported_total_members > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return nmembers == 0 ? CMX_DESC_SUPPORTED_LIMIT : CMX_DESC_DENIED;

	if (reported_total_members != nmembers || !descriptor_entries_valid(members, nmembers))
		return CMX_DESC_DENIED;

	return CMX_DESC_OK;
}


static uint64
hash_byte(uint64 hash, uint8 value)
{
	hash ^= value;
	hash *= UINT64CONST(1099511628211);
	return hash;
}


static uint64
hash_u16(uint64 hash, uint16 value)
{
	hash = hash_byte(hash, (uint8)(value >> 8));
	hash = hash_byte(hash, (uint8)value);
	return hash;
}


static uint64
hash_u32(uint64 hash, uint32 value)
{
	hash = hash_byte(hash, (uint8)(value >> 24));
	hash = hash_byte(hash, (uint8)(value >> 16));
	hash = hash_byte(hash, (uint8)(value >> 8));
	hash = hash_byte(hash, (uint8)value);
	return hash;
}


uint64
cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers)
{
	uint64 hash = UINT64CONST(14695981039346656037);
	uint16 i;

	if (!current_mx_key_valid(key) || members == NULL || nmembers == 0
		|| nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return 0;

	hash = hash_u32(hash, (uint32)0x434d5831); /* "CMX1" */
	hash = hash_u16(hash, key->origin_node_id);
	hash = hash_u32(hash, key->multixact_id);
	hash = hash_u32(hash, key->cluster_epoch);
	hash = hash_u16(hash, nmembers);

	for (i = 0; i < nmembers; i++) {
		hash = hash_u32(hash, members[i].xid);
		hash = hash_byte(hash, members[i].member_status);
	}

	return hash == 0 ? 1 : hash;
}


static bool
proof_entry_semantic_valid(const ClusterCurrentMemberProof *proof,
						   const ClusterCurrentMxMemberDesc *member, uint16 ordinal, uint32 epoch,
						   int expected_origin)
{
	if (proof == NULL || proof->member_ordinal != ordinal || proof->member_xid != member->xid
		|| proof->member_status != member->member_status || proof->state > CCM_UNKNOWN)
		return false;

	switch ((ClusterCurrentMemberState)proof->state) {
	case CCM_SELF:
	case CCM_ACTIVE:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) != 0 && proof->commit_scn == InvalidScn
			   && proof_key_valid_holder(proof, proof->member_xid, epoch, expected_origin);

	case CCM_COMMITTED:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) == 0 && proof_key_is_zero(&proof->key)
			   && SCN_VALID(proof->commit_scn);

	case CCM_ABORTED:
	case CCM_UNKNOWN:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) == 0 && proof_key_is_zero(&proof->key)
			   && proof->commit_scn == InvalidScn;
	}

	return false;
}


static void
proof_array_set_unknown(ClusterCurrentMemberProof *proofs, uint16 nmembers)
{
	uint16 i;

	if (proofs == NULL)
		return;

	memset(proofs, 0, sizeof(*proofs) * nmembers);
	for (i = 0; i < nmembers; i++)
		proofs[i].state = CCM_UNKNOWN;
}


ClusterMxResolveResult
cluster_multixact_current_validate_proof_set(const ClusterCurrentMxKey *key,
											 const ClusterCurrentMxMemberDesc *members,
											 const uint16 *member_origin_nodes, uint16 nmembers,
											 uint64 request_id, uint64 descriptor_hash,
											 const ClusterCurrentProofChunkView *chunks,
											 uint16 nchunks,
											 ClusterCurrentMemberProof *ordered_proofs)
{
	bool seen_members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool seen_chunks[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool saw_unknown = false;
	uint16 i;
	uint16 j;

	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	proof_array_set_unknown(ordered_proofs, nmembers);
	if (!current_mx_key_valid(key) || !descriptor_entries_valid(members, nmembers)
		|| member_origin_nodes == NULL || request_id == 0 || descriptor_hash == 0
		|| descriptor_hash != cluster_multixact_current_descriptor_hash(key, members, nmembers)
		|| chunks == NULL || nchunks == 0 || nchunks > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| ordered_proofs == NULL)
		return CMX_RESOLVE_UNKNOWN;

	memset(seen_members, 0, sizeof(seen_members));
	memset(seen_chunks, 0, sizeof(seen_chunks));
	for (i = 0; i < nmembers; i++)
		if (member_origin_nodes[i] >= CLUSTER_MAX_NODES)
			goto invalid;

	for (i = 0; i < nchunks; i++) {
		const ClusterCurrentProofChunkView *chunk = &chunks[i];

		if (chunk->request_id != request_id || !current_mx_key_equal(&chunk->mxkey, key)
			|| chunk->descriptor_hash != descriptor_hash || chunk->total_count != nmembers
			|| chunk->chunk_count != nchunks || chunk->chunk_ordinal >= nchunks
			|| chunk->source_node_id >= CLUSTER_MAX_NODES || seen_chunks[chunk->chunk_ordinal]
			|| chunk->proof_count == 0
			|| chunk->proof_count > CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK
			|| chunk->proof_count > nmembers || chunk->reserved16 != 0 || chunk->proofs == NULL)
			goto invalid;

		seen_chunks[chunk->chunk_ordinal] = true;

		for (j = 0; j < chunk->proof_count; j++) {
			const ClusterCurrentMemberProof *proof = &chunk->proofs[j];
			uint16 ordinal = proof->member_ordinal;

			if (ordinal >= nmembers || seen_members[ordinal]
				|| chunk->source_node_id != member_origin_nodes[ordinal]
				|| !proof_entry_semantic_valid(proof, &members[ordinal], ordinal,
											   key->cluster_epoch, member_origin_nodes[ordinal]))
				goto invalid;

			seen_members[ordinal] = true;
			ordered_proofs[ordinal] = *proof;
			if (proof->state == CCM_UNKNOWN)
				saw_unknown = true;
		}
	}

	for (i = 0; i < nchunks; i++)
		if (!seen_chunks[i])
			goto invalid;
	for (i = 0; i < nmembers; i++)
		if (!seen_members[i])
			goto invalid;

	if (saw_unknown)
		goto invalid;

	return CMX_RESOLVE_OK;

invalid:
	proof_array_set_unknown(ordered_proofs, nmembers);
	return CMX_RESOLVE_UNKNOWN;
}


static bool
validate_updater_proof_state(const ClusterCurrentMxKey *key,
							 const ClusterCurrentMxMemberDesc *members,
							 const ClusterCurrentMemberProof *proofs, uint16 nmembers,
							 const ClusterCurrentUpdaterChallenge *challenge,
							 const ClusterCurrentUpdaterProof *updater_proof,
							 uint16 updater_origin_node_id,
							 ClusterCurrentMemberState expected_state)
{
	int updater_ordinal = -1;
	uint16 i;

	if (!current_mx_key_valid(key) || !descriptor_entries_valid(members, nmembers) || proofs == NULL
		|| challenge == NULL || updater_proof == NULL || updater_origin_node_id >= CLUSTER_MAX_NODES
		|| challenge->reserved16 != 0 || updater_proof->reserved8 != 0
		|| updater_proof->verdict != CUCP_MATCH
		|| !current_mx_key_equal(&updater_proof->mxkey, key))
		return false;

	for (i = 0; i < nmembers; i++) {
		if (!proof_entry_semantic_valid(&proofs[i], &members[i], i, key->cluster_epoch, -1)
			|| proofs[i].state == CCM_UNKNOWN)
			return false;
		if (ISUPDATE_from_mxstatus(members[i].member_status))
			updater_ordinal = i;
	}

	if (updater_ordinal < 0 || proofs[updater_ordinal].state != expected_state
		|| challenge->member_ordinal != (uint16)updater_ordinal
		|| updater_proof->member_ordinal != (uint16)updater_ordinal
		|| challenge->updater_xid != members[updater_ordinal].xid
		|| updater_proof->updater_xid != members[updater_ordinal].xid
		|| !cluster_multixact_current_successor_provenance_well_formed(
			&challenge->candidate_next_xmin_alias, &challenge->candidate_next_xmin_locator,
			members[updater_ordinal].xid, updater_origin_node_id, key->cluster_epoch)
		|| memcmp(&challenge->candidate_next_xmin_alias, &updater_proof->candidate_next_xmin_alias,
				  sizeof(ClusterCurrentMxSuccessorAlias))
			   != 0
		|| updater_proof->candidate_next_xmin_locator.tt_wrap > TT_WRAP_MAX
		|| !cluster_tx_locator_reply_matches(&challenge->candidate_next_xmin_locator,
											 &updater_proof->candidate_next_xmin_locator))
		return false;

	return true;
}

bool
cluster_multixact_current_validate_updater_proof(const ClusterCurrentMxKey *key,
												 const ClusterCurrentMxMemberDesc *members,
												 const ClusterCurrentMemberProof *proofs,
												 uint16 nmembers,
												 const ClusterCurrentUpdaterChallenge *challenge,
												 const ClusterCurrentUpdaterProof *updater_proof,
												 uint16 updater_origin_node_id)
{
	return validate_updater_proof_state(key, members, proofs, nmembers, challenge, updater_proof,
										updater_origin_node_id, CCM_COMMITTED);
}


bool
cluster_multixact_current_status_conflicts(uint8 member_status, LockTupleMode wanted_mode,
										   bool *valid_out)
{
	LOCKMODE held;
	LOCKMODE wanted;

	if (valid_out != NULL)
		*valid_out = false;
	if (member_status > MaxMultiXactStatus || wanted_mode < LockTupleKeyShare
		|| wanted_mode > LockTupleExclusive)
		return false;

	/*
	 * Keep the translations byte-for-byte equivalent to heapam.c's file-local
	 * MultiXactStatusLock/tupleLockExtraInfo tables, then use PostgreSQL's
	 * canonical heavyweight lock conflict source.
	 */
	switch ((MultiXactStatus)member_status) {
	case MultiXactStatusForKeyShare:
		held = AccessShareLock;
		break;
	case MultiXactStatusForShare:
		held = RowShareLock;
		break;
	case MultiXactStatusForNoKeyUpdate:
	case MultiXactStatusNoKeyUpdate:
		held = ExclusiveLock;
		break;
	case MultiXactStatusForUpdate:
	case MultiXactStatusUpdate:
		held = AccessExclusiveLock;
		break;
	default:
		return false;
	}

	switch (wanted_mode) {
	case LockTupleKeyShare:
		wanted = AccessShareLock;
		break;
	case LockTupleShare:
		wanted = RowShareLock;
		break;
	case LockTupleNoKeyExclusive:
		wanted = ExclusiveLock;
		break;
	case LockTupleExclusive:
		wanted = AccessExclusiveLock;
		break;
	default:
		return false;
	}

	if (valid_out != NULL)
		*valid_out = true;
	return DoLockModesConflict(held,
							   wanted); /* GES_MODE_OK: native tuple locks, not GES enqueue. */
}


static bool
desired_status_matches_mode(MultiXactStatus status, LockTupleMode mode)
{
	switch (status) {
	case MultiXactStatusForKeyShare:
		return mode == LockTupleKeyShare;
	case MultiXactStatusForShare:
		return mode == LockTupleShare;
	case MultiXactStatusForNoKeyUpdate:
	case MultiXactStatusNoKeyUpdate:
		return mode == LockTupleNoKeyExclusive;
	case MultiXactStatusForUpdate:
	case MultiXactStatusUpdate:
		return mode == LockTupleExclusive;
	}

	return false;
}


static bool
action_status_valid(const ClusterCurrentMxRequestContext *ctx)
{
	switch ((ClusterCurrentTupleAction)ctx->action) {
	case CCM_ACTION_UPDATE:
		return ctx->desired_status == MultiXactStatusNoKeyUpdate
			   || ctx->desired_status == MultiXactStatusUpdate;
	case CCM_ACTION_DELETE:
		return ctx->desired_status == MultiXactStatusUpdate && ctx->lock_mode == LockTupleExclusive;
	case CCM_ACTION_LOCK:
	case CCM_ACTION_HOT_FOLLOW:
		return ctx->desired_status <= MultiXactStatusForUpdate;
	}

	return false;
}


static bool
request_context_valid(const ClusterCurrentMxRequestContext *ctx)
{
	return ctx != NULL && current_mx_key_valid(&ctx->mxkey) && TransactionIdIsNormal(ctx->top_xid)
		   && TransactionIdIsNormal(ctx->current_member_xid)
		   && ctx->desired_status <= MaxMultiXactStatus && ctx->lock_mode >= LockTupleKeyShare
		   && ctx->lock_mode <= LockTupleExclusive && ctx->wait_policy >= LockWaitBlock
		   && ctx->wait_policy <= LockWaitError && ctx->action <= CCM_ACTION_HOT_FOLLOW
		   && ctx->tuple_shape <= CCM_SHAPE_DELETED && ctx->follow_updates <= 1
		   && ctx->wait_for_conflict <= 1 && ctx->updater_origin_node_id >= -1
		   && ctx->updater_origin_node_id < CLUSTER_MAX_NODES
		   && desired_status_matches_mode(ctx->desired_status, ctx->lock_mode)
		   && action_status_valid(ctx)
		   && (ctx->precheck_result == TM_Ok || ctx->precheck_result == TM_Invisible
			   || ctx->precheck_result == TM_SelfModified
			   || ctx->precheck_result == TM_BeingModified);
}


static bool
wait_key_precedes(const ClusterTTStatusKey *candidate, const ClusterTTStatusKey *current)
{
	if (candidate->origin_node_id != current->origin_node_id)
		return candidate->origin_node_id < current->origin_node_id;
	if (candidate->local_xid != current->local_xid)
		return candidate->local_xid < current->local_xid;
	if (candidate->tt_slot_id != current->tt_slot_id)
		return candidate->tt_slot_id < current->tt_slot_id;
	return candidate->undo_segment_id < current->undo_segment_id;
}


static ClusterCurrentMxDecision
active_conflict_decision(const ClusterCurrentMxRequestContext *ctx,
						 const ClusterTTStatusKey *holder_key, ClusterTTStatusKey *wait_key)
{
	if (ctx->action == CCM_ACTION_UPDATE || ctx->action == CCM_ACTION_DELETE) {
		if (!ctx->wait_for_conflict)
			return CMDL_BEING_MODIFIED;
	} else {
		if (ctx->wait_policy == LockWaitSkip)
			return CMDL_WOULD_BLOCK;
		if (ctx->wait_policy == LockWaitError)
			return CMDL_LOCK_NOT_AVAILABLE;
	}

	if (wait_key != NULL)
		*wait_key = *holder_key;
	return CMDL_WAIT_MEMBER;
}


static ClusterCurrentMxDecision
current_mx_unknown(ClusterCurrentMxDecisionTrace *trace, ClusterCurrentMxUnknownReason reason,
				   int32 member_ordinal)
{
	if (trace != NULL) {
		trace->unknown_reason = reason;
		trace->member_ordinal = member_ordinal;
	}
	return CMDL_UNKNOWN;
}


ClusterCurrentMxDecision
cluster_multixact_current_decide_observed(const ClusterCurrentMxMemberDesc *members,
										  const ClusterCurrentMemberProof *proofs, uint16 nmembers,
										  const ClusterCurrentMxRequestContext *ctx,
										  const ClusterCurrentUpdaterChallenge *challenge,
										  const ClusterCurrentUpdaterProof *updater_proof,
										  ClusterTTStatusKey *wait_key,
										  ClusterCurrentMxDecisionTrace *trace)
{
	ClusterTTStatusKey selected_wait_key;
	ClusterCurrentMxDecision self_result = CMDL_CONTINUE;
	bool have_active_conflict = false;
	bool have_unknown = false;
	int active_updater = -1;
	int committed_updater = -1;
	int unknown_ordinal = -1;
	uint16 i;

	if (wait_key != NULL)
		memset(wait_key, 0, sizeof(*wait_key));
	if (trace != NULL) {
		trace->unknown_reason = CMX_UNKNOWN_NONE;
		trace->member_ordinal = -1;
	}
	memset(&selected_wait_key, 0, sizeof(selected_wait_key));

	if (!request_context_valid(ctx))
		return current_mx_unknown(trace, CMX_UNKNOWN_REQUEST_CONTEXT, -1);
	if (!descriptor_entries_valid(members, nmembers))
		return current_mx_unknown(trace, CMX_UNKNOWN_DESCRIPTOR, -1);
	if (!descriptor_shape_matches(members, nmembers, (ClusterCurrentTupleShape)ctx->tuple_shape))
		return current_mx_unknown(trace, CMX_UNKNOWN_TUPLE_SHAPE, -1);
	if (proofs == NULL)
		return current_mx_unknown(trace, CMX_UNKNOWN_PROOFS_NULL, -1);

	for (i = 0; i < nmembers; i++) {
		bool conflicts;
		bool valid;

		if (!proof_entry_semantic_valid(&proofs[i], &members[i], i, ctx->mxkey.cluster_epoch, -1))
			return current_mx_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, i);

		conflicts = cluster_multixact_current_status_conflicts(members[i].member_status,
															   ctx->lock_mode, &valid);
		if (!valid)
			return current_mx_unknown(trace, CMX_UNKNOWN_STATUS_MODE, i);

		switch ((ClusterCurrentMemberState)proofs[i].state) {
		case CCM_SELF:
			if (proofs[i].member_xid != ctx->current_member_xid
				&& proofs[i].member_xid != ctx->top_xid)
				return current_mx_unknown(trace, CMX_UNKNOWN_SELF_XID, i);
			if (ISUPDATE_from_mxstatus(members[i].member_status)
				&& ctx->action != CCM_ACTION_HOT_FOLLOW)
				self_result = ctx->tuple_cmax >= ctx->curcid ? CMDL_SELF_MODIFIED : CMDL_INVISIBLE;
			break;

		case CCM_ACTIVE: {
			ClusterTTStatusKey candidate_wait_key;
			uint32 segment_generation;
			uint16 slot_wrap;

			if (ISUPDATE_from_mxstatus(members[i].member_status))
				active_updater = i;
			if (!ClusterCurrentMemberProofGetStatusKey(&proofs[i], &candidate_wait_key,
													   &segment_generation, &slot_wrap))
				return current_mx_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, i);
			if (conflicts
				&& (!have_active_conflict
					|| wait_key_precedes(&candidate_wait_key, &selected_wait_key))) {
				selected_wait_key = candidate_wait_key;
				have_active_conflict = true;
			}
			break;
		}

		case CCM_COMMITTED:
			/*
			 * A compatible committed NoKeyUpdate can be ignored only when the
			 * caller explicitly does not follow update chains.  With
			 * follow_updates=true, authenticate the successor exactly as for a
			 * conflicting updater before allowing the outer heap path to
			 * advance.
			 */
			if (ISUPDATE_from_mxstatus(members[i].member_status)
				&& (conflicts || ctx->follow_updates))
				committed_updater = i;
			break;

		case CCM_ABORTED:
			break;

		case CCM_UNKNOWN:
			have_unknown = true;
			if (unknown_ordinal < 0)
				unknown_ordinal = i;
			break;
		default:
			return current_mx_unknown(trace, CMX_UNKNOWN_MEMBER_STATE, i);
		}
	}

	if (have_unknown)
		return current_mx_unknown(trace, CMX_UNKNOWN_MEMBER_STATE, unknown_ordinal);

	if (ctx->precheck_result == TM_Invisible)
		return CMDL_INVISIBLE;
	if (ctx->precheck_result == TM_SelfModified)
		return CMDL_SELF_MODIFIED;

	if (self_result != CMDL_CONTINUE)
		return self_result;

	if (committed_updater >= 0) {
		if (ctx->tuple_shape == CCM_SHAPE_DELETED)
			return CMDL_DELETED;
		if (ctx->tuple_shape != CCM_SHAPE_UPDATED)
			return current_mx_unknown(trace, CMX_UNKNOWN_COMMITTED_UPDATER_SHAPE,
									  committed_updater);
		if (!cluster_multixact_current_validate_updater_proof(&ctx->mxkey, members, proofs,
															  nmembers, challenge, updater_proof,
															  (uint16)ctx->updater_origin_node_id))
			return current_mx_unknown(trace, CMX_UNKNOWN_COMMITTED_UPDATER_PROOF,
									  committed_updater);
		return CMDL_UPDATED;
	}

	/*
	 * KeyShare is compatible with an in-progress NoKeyUpdate, but native
	 * follow_updates semantics still require the exact successor chain to be
	 * locked.  Keep that continuation distinct from ordinary CONTINUE so the
	 * heap caller cannot stamp only the stale root.  The first successor is
	 * usable only after the same full-key proof required for a committed
	 * updater.
	 */
	if (active_updater >= 0 && !have_active_conflict && ctx->action == CCM_ACTION_LOCK
		&& ctx->follow_updates) {
		if (ctx->tuple_shape != CCM_SHAPE_UPDATED)
			return current_mx_unknown(trace, CMX_UNKNOWN_ACTIVE_UPDATER_SHAPE, active_updater);
		if (!validate_updater_proof_state(&ctx->mxkey, members, proofs, nmembers, challenge,
										  updater_proof, (uint16)ctx->updater_origin_node_id,
										  CCM_ACTIVE))
			return current_mx_unknown(trace, CMX_UNKNOWN_ACTIVE_UPDATER_PROOF, active_updater);
		return CMDL_FOLLOW_UPDATED;
	}

	if (have_active_conflict)
		return active_conflict_decision(ctx, &selected_wait_key, wait_key);

	return CMDL_CONTINUE;
}


ClusterCurrentMxDecision
cluster_multixact_current_decide(const ClusterCurrentMxMemberDesc *members,
								 const ClusterCurrentMemberProof *proofs, uint16 nmembers,
								 const ClusterCurrentMxRequestContext *ctx,
								 const ClusterCurrentUpdaterChallenge *challenge,
								 const ClusterCurrentUpdaterProof *updater_proof,
								 ClusterTTStatusKey *wait_key)
{
	return cluster_multixact_current_decide_observed(members, proofs, nmembers, ctx, challenge,
													 updater_proof, wait_key, NULL);
}


ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members, uint16 members_cap,
								   uint16 *nmembers, uint32 *reported_total_members)
{
	uint64 current_epoch;
	MultiXactMember *native_members = NULL;
	int native_count = -1;
	ClusterMxDescribeResult result = CMX_DESC_UNKNOWN;
	int origin_slot;
	uint16 i;

	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (nmembers != NULL)
		*nmembers = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;
	if (key == NULL || members == NULL || nmembers == NULL || reported_total_members == NULL
		|| members_cap < 1 || !current_mx_key_valid(key) || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return CMX_DESC_UNKNOWN;

	current_epoch = cluster_epoch_get_current();
	if (current_epoch > UINT32_MAX || key->cluster_epoch != (uint32)current_epoch)
		return CMX_DESC_UNKNOWN;

	origin_slot = cluster_mxid_origin_slot(key->multixact_id);
	if (origin_slot < 0 || origin_slot != (int)key->origin_node_id)
		return CMX_DESC_UNKNOWN;
	if (key->origin_node_id != (uint16)cluster_node_id) {
		cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_ASK);
		PG_TRY();
		{
			result = cluster_gcs_current_mx_describe_fetch_and_wait((int32)key->origin_node_id, key,
																	members, members_cap, nmembers,
																	reported_total_members);
		}
		PG_CATCH();
		{
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
			PG_RE_THROW();
		}
		PG_END_TRY();
		switch (result) {
		case CMX_DESC_OK:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_HIT);
			break;
		case CMX_DESC_DENIED:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_DENIED);
			break;
		case CMX_DESC_SUPPORTED_LIMIT:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT);
			break;
		case CMX_DESC_TIMEOUT:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_TIMEOUT);
			break;
		case CMX_DESC_UNKNOWN:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
			break;
		}
		return result;
	}
	if (!cluster_mxid_is_mine(key->multixact_id))
		return CMX_DESC_DENIED;

	cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_LOCAL);
	native_count = GetMultiXactIdMembers(key->multixact_id, &native_members, false, false);
	if (native_count > CLUSTER_CURRENT_MX_MAX_MEMBERS) {
		*reported_total_members = (uint32)native_count;
		result = CMX_DESC_SUPPORTED_LIMIT;
	} else if (native_count < 1 || native_count > members_cap) {
		result = CMX_DESC_DENIED;
	} else {
		for (i = 0; i < (uint16)native_count; i++) {
			members[i].xid = native_members[i].xid;
			members[i].member_status = (uint8)native_members[i].status;
		}
		result = cluster_multixact_current_validate_descriptor(
			key, (uint16)cluster_node_id, (uint32)current_epoch, members, (uint16)native_count,
			(uint32)native_count);
		if (result == CMX_DESC_OK) {
			*nmembers = (uint16)native_count;
			*reported_total_members = (uint32)native_count;
		}
	}

	if (native_members != NULL)
		pfree(native_members);
	if (result != CMX_DESC_OK && result != CMX_DESC_SUPPORTED_LIMIT) {
		memset(members, 0, sizeof(*members) * members_cap);
		*nmembers = 0;
		*reported_total_members = 0;
	}
	return result;
}


static ClusterMxResolveResult
cluster_multixact_current_members_resolve_internal(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *proof_capability_generations, TimestampTz *operation_deadline_io)
{
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	uint16 member_origins[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool seen[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint32 source_capability_generations[CLUSTER_MAX_NODES];
	ClusterCtrcTxnKeyV1 local_ctrc_keys[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCtrcParticipantIdentity local_participant;
	uint32 local_ctrc_grants[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint32 local_ctrc_capability_generations[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint64 current_epoch;
	TimestampTz operation_deadline;
	uint16 plan_count = 0;
	uint16 i;
	ClusterMxResolveResult result;

	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	proof_array_set_unknown(proofs, nmembers);
	if (proof_capability_generations != NULL)
		memset(proof_capability_generations, 0, sizeof(*proof_capability_generations) * nmembers);
	if (key == NULL || members == NULL || proofs == NULL || updater_proof == NULL
		|| proof_capability_generations == NULL || nmembers < 1 || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return CMX_RESOLVE_UNKNOWN;

	current_epoch = cluster_epoch_get_current();
	operation_deadline = operation_deadline_io != NULL ? *operation_deadline_io : 0;
	if (operation_deadline == 0 && cluster_gcs_reply_timeout_ms > 0) {
		operation_deadline
			= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), cluster_gcs_reply_timeout_ms);
		if (operation_deadline_io != NULL)
			*operation_deadline_io = operation_deadline;
	}
	if (operation_deadline != 0 && GetCurrentTimestamp() >= operation_deadline)
		return CMX_RESOLVE_TIMEOUT;
	if (current_epoch > UINT32_MAX || key->cluster_epoch != (uint32)current_epoch
		|| descriptor_hash != cluster_multixact_current_descriptor_hash(key, members, nmembers))
		return CMX_RESOLVE_UNKNOWN;
	for (i = 0; i < nmembers; i++) {
		int origin = cluster_xid_origin_slot(members[i].xid);

		if (origin < 0 || origin >= CLUSTER_MAX_NODES)
			return CMX_RESOLVE_UNKNOWN;
		member_origins[i] = (uint16)origin;
	}

	result = cluster_multixact_current_wire_build_proof_requests(
		key, members, member_origins, nmembers, descriptor_hash, challenge, UINT64CONST(1),
		current_epoch, cluster_node_id, (int32)MyBackendId, plans, lengthof(plans), &plan_count);
	if (result != CMX_RESOLVE_OK)
		return result;

	memset(seen, 0, sizeof(seen));
	memset(source_capability_generations, 0, sizeof(source_capability_generations));
	memset(local_ctrc_keys, 0, sizeof(local_ctrc_keys));
	memset(local_ctrc_grants, 0, sizeof(local_ctrc_grants));
	memset(local_ctrc_capability_generations, 0, sizeof(local_ctrc_capability_generations));
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMemberProof chunk_proofs[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
		ClusterCurrentUpdaterProof chunk_updater;
		ClusterCurrentMxProofForwardV2 *request = &plans[i].request;
		uint16 chunk_count = 0;
		uint32 chunk_capability_generation = 0;
		uint32 chunk_member_capability_generations[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
		uint16 j;

		if (operation_deadline != 0 && GetCurrentTimestamp() >= operation_deadline) {
			result = CMX_RESOLVE_TIMEOUT;
			goto non_ok;
		}

		memset(chunk_proofs, 0, sizeof(chunk_proofs));
		memset(chunk_member_capability_generations, 0, sizeof(chunk_member_capability_generations));
		memset(&chunk_updater, 0, sizeof(chunk_updater));
		chunk_updater.verdict = CUCP_UNKNOWN;

		if (plans[i].destination_node_id == (uint16)cluster_node_id) {
			if (request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
				for (j = 0; j < request->prefix.entry_count; j++) {
					const ClusterCurrentMxProofAskWire *ask = &request->trailer.body.asks[j];
					ClusterTTStatusKey initial_key;
					ClusterTTStatusResult initial_result;
					ClusterCtrcTxnKeyV1 ctrc_key;
					uint32 ctrc_grant = 0;
					uint32 participant_capability_generation = 0;

					if (!current_mx_local_member_sample_exact(
							ask->xid, &initial_key, &initial_result, &ctrc_grant,
							&participant_capability_generation, &ctrc_key)
						|| !cluster_multixact_current_resolve_origin_member_proof(
							ask->xid, ask->member_status, ask->member_ordinal,
							(uint16)cluster_node_id, (uint32)current_epoch,
							TransactionIdIsCurrentTransactionId(ask->xid), &initial_key,
							&initial_result, NULL, NULL, &chunk_proofs[j]))
						goto unknown;
					if (chunk_proofs[j].state == CCM_ACTIVE || chunk_proofs[j].state == CCM_SELF) {
						if (ctrc_grant == 0)
							goto unknown;
						ClusterCurrentMemberProofSetCtrcGrant(&chunk_proofs[j], ctrc_grant);
						if (!cluster_multixact_current_member_proof_bind_ctrc(&chunk_proofs[j],
																			  &ctrc_key))
							goto unknown;
						if (participant_capability_generation == 0
							|| (chunk_capability_generation != 0
								&& chunk_capability_generation
									   != participant_capability_generation))
							goto unknown;
						chunk_capability_generation = participant_capability_generation;
						chunk_member_capability_generations[j] = participant_capability_generation;
						local_ctrc_keys[ask->member_ordinal] = ctrc_key;
						local_ctrc_grants[ask->member_ordinal] = ctrc_grant;
						local_ctrc_capability_generations[ask->member_ordinal]
							= participant_capability_generation;
					} else if (ctrc_grant != 0 || participant_capability_generation != 0)
						goto unknown;
				}
				chunk_count = request->prefix.entry_count;
			} else {
				const ClusterCurrentMxUpdaterChallengeWire *wire_challenge
					= &request->trailer.body.updater.challenge;
				ClusterTTStatusKey initial_key;
				ClusterTTStatusResult initial_result;
				ClusterCtrcTxnKeyV1 ctrc_key;
				ClusterTxLocator canonical_locator;
				uint32 ctrc_grant = 0;
				uint32 participant_capability_generation = 0;
				bool cross_segment = false;

				if (!cluster_runtime_visibility_current_mx_updater_provenance_exact(
						&wire_challenge->candidate_next_xmin_locator, operation_deadline,
						&initial_key, &initial_result, &ctrc_grant,
						&participant_capability_generation, &ctrc_key, &canonical_locator,
						&cross_segment)
					|| !cluster_multixact_current_resolve_origin_member_proof(
						wire_challenge->updater_xid, wire_challenge->member_status,
						wire_challenge->member_ordinal, (uint16)cluster_node_id,
						(uint32)current_epoch,
						TransactionIdIsCurrentTransactionId(wire_challenge->updater_xid),
						&initial_key, &initial_result, NULL, NULL, &chunk_proofs[0]))
					goto unknown;
				if (chunk_proofs[0].state == CCM_ACTIVE || chunk_proofs[0].state == CCM_SELF) {
					if (ctrc_grant == 0)
						goto unknown;
					ClusterCurrentMemberProofSetCtrcGrant(&chunk_proofs[0], ctrc_grant);
					if (!cluster_multixact_current_member_proof_bind_ctrc(&chunk_proofs[0],
																		  &ctrc_key))
						goto unknown;
					if (participant_capability_generation == 0)
						goto unknown;
					chunk_capability_generation = participant_capability_generation;
					chunk_member_capability_generations[0] = participant_capability_generation;
					local_ctrc_keys[wire_challenge->member_ordinal] = ctrc_key;
					local_ctrc_grants[wire_challenge->member_ordinal] = ctrc_grant;
					local_ctrc_capability_generations[wire_challenge->member_ordinal]
						= participant_capability_generation;
				} else if (ctrc_grant != 0 || participant_capability_generation != 0)
					goto unknown;
				chunk_count = 1;
				chunk_updater.mxkey = request->prefix.mxkey;
				chunk_updater.candidate_next_xmin_alias = wire_challenge->candidate_next_xmin_alias;
				chunk_updater.candidate_next_xmin_locator = canonical_locator;
				chunk_updater.updater_xid = wire_challenge->updater_xid;
				chunk_updater.member_ordinal = wire_challenge->member_ordinal;
				chunk_updater.verdict = CUCP_MATCH;
				if (cross_segment)
					cluster_multixact_current_stats_bump(
						CMX_STAT_UPDATER_PROVENANCE_CROSS_SEGMENT_MATCH);
			}
		} else {
			cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_ASK);
			PG_TRY();
			{
				result = cluster_gcs_current_mx_member_proof_fetch_and_wait(
					plans[i].destination_node_id, request, chunk_proofs, lengthof(chunk_proofs),
					&chunk_count, &chunk_updater, &chunk_capability_generation, operation_deadline);
			}
			PG_CATCH();
			{
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_UNKNOWN);
				PG_RE_THROW();
			}
			PG_END_TRY();
			switch (result) {
			case CMX_RESOLVE_OK:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_HIT);
				break;
			case CMX_RESOLVE_DENIED:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_DENIED);
				break;
			case CMX_RESOLVE_SUPPORTED_LIMIT:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_SUPPORTED_LIMIT);
				break;
			case CMX_RESOLVE_TIMEOUT:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_TIMEOUT);
				break;
			case CMX_RESOLVE_UNKNOWN:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_UNKNOWN);
				break;
			case CMX_RESOLVE_RETRY:
				/* The stale batch is still a non-positive observation. */
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_UNKNOWN);
				break;
			}
			if (result != CMX_RESOLVE_OK)
				goto non_ok;
		}

		if (chunk_count != request->prefix.entry_count)
			goto unknown;
		for (j = 0; j < chunk_count; j++) {
			if (chunk_proofs[j].state != CCM_ACTIVE && chunk_proofs[j].state != CCM_SELF)
				continue;
			if (chunk_capability_generation == 0)
				goto unknown;
			if (plans[i].destination_node_id != (uint16)cluster_node_id)
				chunk_member_capability_generations[j] = chunk_capability_generation;
		}
		if (chunk_capability_generation != 0) {
			if (source_capability_generations[plans[i].destination_node_id] != 0
				&& source_capability_generations[plans[i].destination_node_id]
					   != chunk_capability_generation)
				goto unknown;
			source_capability_generations[plans[i].destination_node_id]
				= chunk_capability_generation;
		}
		for (j = 0; j < chunk_count; j++) {
			uint16 ordinal = chunk_proofs[j].member_ordinal;

			if (ordinal >= nmembers || seen[ordinal]
				|| member_origins[ordinal] != plans[i].destination_node_id
				|| !proof_entry_semantic_valid(&chunk_proofs[j], &members[ordinal], ordinal,
											   (uint32)current_epoch, plans[i].destination_node_id)
				|| chunk_proofs[j].state == CCM_UNKNOWN)
				goto unknown;
			seen[ordinal] = true;
			proofs[ordinal] = chunk_proofs[j];
			proof_capability_generations[ordinal] = chunk_member_capability_generations[j];
		}

		if (request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
			const ClusterCurrentMxUpdaterChallengeWire *wire_challenge
				= &request->trailer.body.updater.challenge;

			if (!current_mx_key_equal(&chunk_updater.mxkey, key)
				|| memcmp(&chunk_updater.candidate_next_xmin_alias,
						  &wire_challenge->candidate_next_xmin_alias,
						  sizeof(ClusterCurrentMxSuccessorAlias))
					   != 0
				|| chunk_updater.candidate_next_xmin_locator.tt_wrap > TT_WRAP_MAX
				|| !cluster_tx_locator_reply_matches(&wire_challenge->candidate_next_xmin_locator,
													 &chunk_updater.candidate_next_xmin_locator)
				|| chunk_updater.updater_xid != wire_challenge->updater_xid
				|| chunk_updater.member_ordinal != wire_challenge->member_ordinal
				|| chunk_updater.verdict > CUCP_UNKNOWN || chunk_updater.reserved8 != 0
				|| chunk_updater.verdict == CUCP_UNKNOWN)
				goto unknown;
			*updater_proof = chunk_updater;
		}
		if (cluster_epoch_get_current() != current_epoch)
			goto unknown;
	}

	for (i = 0; i < nmembers; i++)
		if (!seen[i])
			goto unknown;
	if (operation_deadline != 0 && GetCurrentTimestamp() >= operation_deadline) {
		result = CMX_RESOLVE_TIMEOUT;
		goto non_ok;
	}
	for (i = 0; i < nmembers; i++) {
		if (local_ctrc_grants[i] == 0)
			continue;
		memset(&local_participant, 0, sizeof(local_participant));
		local_participant.node_id = (uint16)cluster_node_id;
		local_participant.capability_record_generation = local_ctrc_capability_generations[i];
		local_participant.boot_incarnation = local_ctrc_keys[i].origin_boot_incarnation;
		local_participant.formation_epoch = local_ctrc_keys[i].formation_epoch;
		local_participant.admission_record_generation
			= local_ctrc_keys[i].admission_record_generation;
		if (local_participant.capability_record_generation == 0
			|| !cluster_ctrc_origin_grant_publishable(&local_ctrc_keys[i], &local_participant,
													  local_ctrc_grants[i])) {
			result = CMX_RESOLVE_RETRY;
			goto non_ok;
		}
	}
	return CMX_RESOLVE_OK;

non_ok:
	proof_array_set_unknown(proofs, nmembers);
	memset(proof_capability_generations, 0, sizeof(*proof_capability_generations) * nmembers);
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->verdict = CUCP_UNKNOWN;
	return result;

unknown:
	proof_array_set_unknown(proofs, nmembers);
	memset(proof_capability_generations, 0, sizeof(*proof_capability_generations) * nmembers);
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->verdict = CUCP_UNKNOWN;
	return CMX_RESOLVE_UNKNOWN;
}


ClusterMxResolveResult
cluster_multixact_current_members_resolve(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers, uint64 descriptor_hash,
										  const ClusterCurrentUpdaterChallenge *challenge,
										  ClusterCurrentMemberProof *proofs,
										  ClusterCurrentUpdaterProof *updater_proof,
										  uint32 *proof_capability_generations)
{
	return cluster_multixact_current_members_resolve_internal(
		key, members, nmembers, descriptor_hash, challenge, proofs, updater_proof,
		proof_capability_generations, NULL);
}


ClusterMxResolveResult
cluster_multixact_current_members_resolve_until(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *proof_capability_generations, TimestampTz *operation_deadline)
{
	return cluster_multixact_current_members_resolve_internal(
		key, members, nmembers, descriptor_hash, challenge, proofs, updater_proof,
		proof_capability_generations, operation_deadline);
}


ClusterMxRecomposeResult
cluster_multixact_current_recompose(const ClusterCurrentMxMemberDesc *members,
									const ClusterCurrentMemberProof *proofs, uint16 nmembers,
									TransactionId requester_xid, MultiXactStatus requester_status,
									MultiXactMember *normalized_members, uint16 normalized_cap,
									uint16 *normalized_count)
{
	MultiXactMember scratch[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 out_count = 0;
	int requester_index = -1;
	int updater_index = -1;
	uint16 i;

	memset(scratch, 0, sizeof(scratch));
	if (normalized_members != NULL && normalized_cap > 0)
		memset(normalized_members, 0, sizeof(*normalized_members) * normalized_cap);
	if (normalized_count != NULL)
		*normalized_count = 0;

	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;
	if (!descriptor_entries_valid(members, nmembers) || proofs == NULL
		|| !TransactionIdIsNormal(requester_xid) || requester_status > MaxMultiXactStatus
		|| normalized_members == NULL || normalized_count == NULL)
		return CMX_RECOMPOSE_UNKNOWN;
	if (normalized_cap == 0 || normalized_cap > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;

	for (i = 0; i < nmembers; i++) {
		const ClusterCurrentMemberProof *proof = &proofs[i];
		bool keep = false;

		if (proof->member_ordinal != i || proof->member_xid != members[i].xid
			|| proof->member_status != members[i].member_status || proof->state > CCM_UNKNOWN)
			return CMX_RECOMPOSE_UNKNOWN;

		switch ((ClusterCurrentMemberState)proof->state) {
		case CCM_SELF:
		case CCM_ACTIVE:
			if (ClusterCurrentMemberProofGetCtrcGrant(proof) == 0 || proof->commit_scn != InvalidScn
				|| !proof_key_valid_holder(proof, proof->member_xid, proof->key.cluster_epoch,
										   proof->key.origin_node_id))
				return CMX_RECOMPOSE_UNKNOWN;
			keep = true;
			break;
		case CCM_COMMITTED:
			if (ClusterCurrentMemberProofGetCtrcGrant(proof) != 0 || !proof_key_is_zero(&proof->key)
				|| !SCN_VALID(proof->commit_scn))
				return CMX_RECOMPOSE_UNKNOWN;
			/*
			 * A committed updater changes the tuple version and must have
			 * been returned as UPDATED/DELETED by the compositor.  Never
			 * silently normalize it into a writable old-version set.
			 */
			if (ISUPDATE_from_mxstatus(members[i].member_status))
				return CMX_RECOMPOSE_DENIED;
			break;
		case CCM_ABORTED:
			if (ClusterCurrentMemberProofGetCtrcGrant(proof) != 0 || !proof_key_is_zero(&proof->key)
				|| proof->commit_scn != InvalidScn)
				return CMX_RECOMPOSE_UNKNOWN;
			break;
		case CCM_UNKNOWN:
			if (ClusterCurrentMemberProofGetCtrcGrant(proof) != 0)
				return CMX_RECOMPOSE_UNKNOWN;
			return CMX_RECOMPOSE_UNKNOWN;
		}

		if (keep) {
			uint16 out = out_count;

			if (out_count >= normalized_cap)
				return CMX_RECOMPOSE_SUPPORTED_LIMIT;
			scratch[out].xid = members[i].xid;
			scratch[out].status = (MultiXactStatus)members[i].member_status;
			if (TransactionIdEquals(members[i].xid, requester_xid))
				requester_index = out;
			if (ISUPDATE_from_mxstatus(members[i].member_status))
				updater_index = out;
			out_count++;
		}
	}

	if (requester_index >= 0) {
		MultiXactStatus old_status = scratch[requester_index].status;
		int old_strength;
		int new_strength;
		bool updater;

		switch (old_status) {
		case MultiXactStatusForKeyShare:
			old_strength = 0;
			break;
		case MultiXactStatusForShare:
			old_strength = 1;
			break;
		case MultiXactStatusForNoKeyUpdate:
		case MultiXactStatusNoKeyUpdate:
			old_strength = 2;
			break;
		case MultiXactStatusForUpdate:
		case MultiXactStatusUpdate:
			old_strength = 3;
			break;
		default:
			return CMX_RECOMPOSE_UNKNOWN;
		}
		switch (requester_status) {
		case MultiXactStatusForKeyShare:
			new_strength = 0;
			break;
		case MultiXactStatusForShare:
			new_strength = 1;
			break;
		case MultiXactStatusForNoKeyUpdate:
		case MultiXactStatusNoKeyUpdate:
			new_strength = 2;
			break;
		case MultiXactStatusForUpdate:
		case MultiXactStatusUpdate:
			new_strength = 3;
			break;
		default:
			return CMX_RECOMPOSE_UNKNOWN;
		}

		new_strength = Max(old_strength, new_strength);
		updater = ISUPDATE_from_mxstatus(old_status) || ISUPDATE_from_mxstatus(requester_status);
		if (updater)
			scratch[requester_index].status
				= new_strength == 3 ? MultiXactStatusUpdate : MultiXactStatusNoKeyUpdate;
		else {
			static const MultiXactStatus lock_status[] = {
				MultiXactStatusForKeyShare,
				MultiXactStatusForShare,
				MultiXactStatusForNoKeyUpdate,
				MultiXactStatusForUpdate,
			};

			scratch[requester_index].status = lock_status[new_strength];
		}
		if (updater)
			updater_index = requester_index;
	} else {
		if (ISUPDATE_from_mxstatus(requester_status) && updater_index >= 0)
			return CMX_RECOMPOSE_DENIED;
		if (out_count >= normalized_cap)
			return CMX_RECOMPOSE_SUPPORTED_LIMIT;
		scratch[out_count].xid = requester_xid;
		scratch[out_count].status = requester_status;
		out_count++;
	}

	if (out_count > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;
	memcpy(normalized_members, scratch, sizeof(*normalized_members) * out_count);
	*normalized_count = out_count;
	return CMX_RECOMPOSE_OK;
}
