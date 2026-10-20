/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_tt_redo.c
 *    Read-only ordinary redo inputs from original captured TT and UNDO.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_cold_recovery_tt_redo.c
 *
 * NOTES
 *    PGRAC-original. Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *    No original bytes are installed. Canonical top qualification consumes
 *    the native BIND producer; all other transaction gates remain separate.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_wal_tail.h"

static bool
cold_tt_source(const ClusterColdTypedV1 *typed, uint32 participant, uint8 owner, uint32 segment,
			   bool *has_header_work)
{
	const ClusterColdParticipantV1 *source;
	const RfSideOnlinePlanV1 *plan;
	RfContributorStreamCutV1 cut = { 0 };
	uint32 count;

	*has_header_work = false;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count || participant >= typed->participant_count
		|| typed->side_capture_detail[participant] != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	source = &typed->participants[participant];
	/* A retained generation supplies input only; it has no replay interval. */
	if (participant >= typed->replay_count && source->native_redo != source->tail_end)
		return false;
	plan = typed->side_plans[participant];
	if (owner != 0 && owner != source->thread_id)
		return false;
	cut.failed_thread = source->thread_id;
	cut.origin_owner_incarnation = source->owner_incarnation;
	cut.timeline_id = source->timeline;
	cut.scan_begin_inclusive = source->physical_lower;
	cut.scan_end_exclusive = source->tail_end;
	cut.flags
		= RF_CONTRIBUTOR_CUT_COMPLETE
		  | (source->physical_lower == source->tail_end ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0);
	if (!rf_side_online_plan_recovery_source_matches_v1(plan, typed->system_identifier,
														typed->observer.storage_uuid, &cut)
		|| !rf_side_online_plan_recovery_claim_matches_v1(plan, &typed->sources[participant])
		|| !rf_side_online_plan_replay_start_matches_v1(
			plan, source->thread_id, source->owner_incarnation,
			typed->observer.database_incarnation, source->native_redo))
		return false;

	count = rf_side_online_plan_operation_count_v1(plan);
	if (count == UINT32_MAX)
		return false;
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (participant >= typed->replay_count && !operation.history_only)
			return false;
		/* These need their own complete proof/consumer. Do not silently
		 * return an ordinary-only image of a source that contains them. */
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT
			|| (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
				&& operation.xact.kind != RF_SIDE_XACT_COMMIT
				&& operation.xact.kind != RF_SIDE_XACT_ABORT))
			return false;
		/* The prepared producer can leave these standalone records before
		 * its XACT terminal. Exact ordinary TT_ABORT is a different carrier. */
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
			&& (operation.undo.kind == CLUSTER_UNDO_KIND_TT_COMMIT
				|| operation.undo.kind == CLUSTER_UNDO_KIND_TT_SET_HEAD
				|| (operation.undo.kind == CLUSTER_UNDO_KIND_TT_ABORT
					&& operation.undo.format_version != CLUSTER_UNDO_TT_ABORT_EXACT_VERSION)))
			return false;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO && operation.undo.instance == owner
			&& operation.undo.segment_id == segment)
			*has_header_work = true;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& operation.xact.kind == RF_SIDE_XACT_COMMIT && operation.xact.has_tt_delta
			&& operation.xact.tt_delta.instance == owner
			&& operation.xact.tt_delta.segment_id == segment)
			*has_header_work = true;
	}
	return true;
}

/* Readback/fold consumers remain limited to actual replay owners. */
static bool
cold_tt_redo_source(const ClusterColdTypedV1 *typed, uint32 participant, uint8 owner,
					uint32 segment, bool *has_header_work)
{
	return typed != NULL && participant < typed->replay_count
		   && cold_tt_source(typed, participant, owner, segment, has_header_work);
}

bool
cluster_cold_typed_tt_redo_header_v1(const ClusterColdTypedV1 *typed, uint32 participant,
									 uint32 segment_index, RfSideUndoHeaderImageV1 *out)
{
	const UndoSegmentHeaderData *base;
	const RfSideOnlinePlanV1 *plan;
	RfSideUndoHeaderImageV1 image = { 0 };
	uint8 owner;
	uint32 segment;
	bool has_header_work;

	if (out == NULL)
		return false;
	base = cluster_cold_tt_base_header_v1(typed, segment_index, &owner, &segment);
	if (base == NULL || !cold_tt_redo_source(typed, participant, owner, segment, &has_header_work))
		return false;
	plan = typed->side_plans[participant];
	if (has_header_work) {
		/* Preserve native lifecycle, generation, slot and source-membership
		 * checks in the existing fold; no duplicate TT replay implementation. */
		if (rf_side_online_plan_prepare_undo_header_v1(plan, owner, segment, (const char *)base,
													   &image)
			!= RF_PAGE_PROOF_DETAIL_OK)
			return false;
	} else {
		/* Identity fold, including an explicitly empty WAL cut. An ACTIVE
		 * slot stays raw ACTIVE; no absence-of-COMMIT or loser inference. */
		memcpy(image.page.data, base, BLCKSZ);
		image.source_index = UINT32_MAX;
	}
	*out = image;
	return true;
}

bool
cluster_cold_typed_tt_redo_readback_v1(const ClusterColdTypedV1 *typed, uint32 participant,
									   uint32 segment_index, RfSideUndoHeaderImageV1 *out)
{
	RfSideUndoHeaderImageV1 folded;
	uint8 owner;
	uint32 segment, count;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (typed == NULL
		|| !cluster_cold_tt_base_namespace_matches_v1(typed, typed->system_identifier,
													  typed->observer.database_incarnation)
		|| cluster_cold_tt_base_header_v1(typed, segment_index, &owner, &segment) == NULL
		|| !cluster_cold_typed_tt_redo_header_v1(typed, participant, segment_index, &folded))
		return false;
	/* Redo may add/remove files, so the independently captured inventories
	 * need not share indices. This comparison covers only this original
	 * physical header; it does not certify the remaining inventory. */
	count = cluster_cold_tt_after_redo_count_v1(typed);
	for (uint32 i = 0; i < count; i++) {
		const UndoSegmentHeaderData *observed;
		uint8 observed_owner;
		uint32 observed_segment;

		observed
			= cluster_cold_tt_after_redo_header_v1(typed, i, &observed_owner, &observed_segment);
		if (observed == NULL)
			return false;
		if (observed_owner != owner || observed_segment != segment)
			continue;
		/* Include all slots, generation, header metadata and untouched bytes.
		 * Matching one ACTIVE key or terminal status is insufficient. */
		if (memcmp(observed, folded.page.data, BLCKSZ) != 0)
			return false;
		*out = folded;
		return true;
	}
	return false;
}

bool
cluster_cold_typed_tt_after_redo_readback_v1(const ClusterColdTypedV1 *typed, uint32 participant,
											 uint32 post_segment_index,
											 RfSideUndoHeaderImageV1 *out)
{
	const UndoSegmentHeaderData *observed;
	RfSideUndoHeaderImageV1 folded;
	uint8 owner;
	uint32 segment, original_index;
	bool has_header_work;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (typed == NULL
		|| !cluster_cold_tt_base_namespace_matches_v1(typed, typed->system_identifier,
													  typed->observer.database_incarnation))
		return false;
	observed = cluster_cold_tt_after_redo_header_v1(typed, post_segment_index, &owner, &segment);
	if (observed == NULL)
		return false;
	if (cluster_cold_tt_header_find_v1(typed, false, owner, segment, &original_index) != NULL) {
		if (!cluster_cold_typed_tt_redo_header_v1(typed, participant, original_index, &folded))
			return false;
	} else {
		/* A new physical file supplies no predecessor. Only the existing
		 * native full-image/lifecycle proof may reconstruct it from redo. */
		if (!cold_tt_redo_source(typed, participant, owner, segment, &has_header_work)
			|| !has_header_work
			|| rf_side_online_plan_prepare_undo_header_v1(typed->side_plans[participant], owner,
														  segment, NULL, &folded)
				   != RF_PAGE_PROOF_DETAIL_OK)
			return false;
	}
	if (memcmp(observed, folded.page.data, BLCKSZ) != 0)
		return false;
	*out = folded;
	return true;
}

bool
cluster_cold_typed_tt_redo_inventory_v1(const ClusterColdTypedV1 *typed)
{
	uint32 originals_seen = 0;
	uint32 post_count;

	if (!cluster_cold_tt_after_redo_namespace_matches_v1(typed) || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count)
		return false;
	/* An empty physical inventory still needs every selected source's sealed
	 * ordinary cut, including retained generations without surviving TT files.
	 * This validates their input/profile, not their checkpoint/effects proof. */
	for (uint32 p = 0; p < typed->participant_count; p++) {
		bool unused_header_work;

		if (p < typed->replay_count)
			for (uint32 q = 0; q < p; q++)
				if (typed->participants[p].thread_id == typed->participants[q].thread_id)
					return false;
		if (!cold_tt_source(typed, p, 0, 0, &unused_header_work))
			return false;
		/* Pre-redo history is already in the base. Only actually replayed
		 * operations require a present physical target in this inventory. */
		for (uint32 i = 0; i < rf_side_online_plan_operation_count_v1(typed->side_plans[p]); i++) {
			RfSideOnlineOperationV1 operation;
			uint8 owner;
			uint32 segment;

			if (!rf_side_online_plan_operation_v1(typed->side_plans[p], i, &operation))
				return false;
			if (operation.history_only)
				continue;
			if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO) {
				owner = operation.undo.instance;
				segment = operation.undo.segment_id;
			} else if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
					   && operation.xact.has_tt_delta) {
				owner = operation.xact.tt_delta.instance;
				segment = operation.xact.tt_delta.segment_id;
			} else
				continue;
			if (owner != typed->participants[p].thread_id
				|| cluster_cold_tt_header_find_v1(typed, true, owner, segment, NULL) == NULL)
				return false;
		}
	}
	post_count = cluster_cold_tt_after_redo_count_v1(typed);
	for (uint32 i = 0; i < post_count; i++) {
		const UndoSegmentHeaderData *observed, *base;
		ClusterRecoveryUndoScan *scan = NULL;
		RfSideUndoHeaderImageV1 folded;
		uint8 owner;
		uint32 segment, p;

		observed = cluster_cold_tt_after_redo_header_v1(typed, i, &owner, &segment);
		if (observed == NULL)
			return false;
		/* Exact unchanged bytes still need the original full header codec:
		 * identity alone does not interpret every slot/status/flag or layout.
		 * This grants no checkpoint coverage or transaction classification. */
		if (cluster_recovery_undo_scan_begin(observed, BLCKSZ, segment, owner, &scan)
			!= CLUSTER_RECOVERY_UNDO_MORE)
			return false;
		cluster_recovery_undo_scan_destroy(scan);
		base = cluster_cold_tt_header_find_v1(typed, false, owner, segment, NULL);
		if (base != NULL)
			originals_seen++;
		for (p = 0; p < typed->replay_count; p++)
			if (typed->participants[p].thread_id == owner)
				break;
		if (p < typed->replay_count) {
			if (!cluster_cold_typed_tt_after_redo_readback_v1(typed, p, i, &folded))
				return false;
		} else {
			/* A history-only source is not a current redo owner. Its bytes
			 * must remain unchanged; this does not qualify its old effects. */
			if (base == NULL || memcmp(observed, base, BLCKSZ) != 0)
				return false;
		}
	}
	/* Captures enumerate each physical owner/segment once. Matching all post
	 * headers must account for every original, including untouched segments. */
	return originals_seen == cluster_cold_tt_base_count_v1(typed)
		   && cluster_cold_tt_after_redo_undo_matches_v1(typed);
}

bool
cluster_cold_typed_tt_redo_slot_v1(const ClusterColdTypedV1 *typed, uint32 participant,
								   uint32 segment_index, uint32 slot_offset,
								   ClusterColdTtSlotInputV1 *out)
{
	ClusterColdTtSlotInputV1 input;
	RfSideUndoHeaderImageV1 image;
	const UndoSegmentHeaderData *header;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_cold_tt_base_slot_v1(typed, segment_index, slot_offset, &input)
		|| !cluster_cold_typed_tt_redo_header_v1(typed, participant, segment_index, &image))
		return false;
	header = (const UndoSegmentHeaderData *)image.page.data;
	input.slot = header->tt_slots[slot_offset];
	input.key.segment_generation = header->wrap_count;
	input.key.xid = input.slot.xid;
	input.key.slot_wrap = input.slot.wrap;
	*out = input;
	return true;
}

bool
cluster_cold_typed_tt_redo_match_v1(const ClusterColdTypedV1 *typed, uint32 participant,
									uint32 segment_index, const ClusterColdTtSlotKeyV1 *expected,
									ClusterColdTtSlotInputV1 *out)
{
	ClusterColdTtSlotKeyV1 key;
	ClusterColdTtSlotInputV1 input;

	if (out == NULL)
		return false;
	if (expected != NULL)
		key = *expected;
	memset(out, 0, sizeof(*out));
	if (expected == NULL || key.owner_instance == 0 || key.owner_instance > 128
		|| key.segment_id == 0
		|| (key.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != key.owner_instance
		|| key.slot_offset >= TT_SLOTS_PER_SEGMENT || !TransactionIdIsNormal(key.xid)
		|| key.slot_wrap == TT_WRAP_INVALID)
		return false;
	if (!cluster_cold_typed_tt_redo_slot_v1(typed, participant, segment_index, key.slot_offset,
											&input)
		|| input.key.owner_instance != key.owner_instance || input.key.segment_id != key.segment_id
		|| input.key.segment_generation != key.segment_generation || input.key.xid != key.xid
		|| input.key.slot_wrap != key.slot_wrap)
		return false;
	*out = input;
	return true;
}

bool
cluster_cold_typed_tt_active_identity_v1(const ClusterColdTypedV1 *typed, uint32 participant,
										 uint32 segment_index,
										 const ClusterColdTtSlotKeyV1 *expected,
										 ClusterColdTtActiveIdentityV1 *out)
{
	ClusterColdTtSlotKeyV1 key;
	ClusterColdTtSlotInputV1 input;

	if (expected != NULL)
		key = *expected;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (expected == NULL
		|| !cluster_cold_typed_tt_redo_match_v1(typed, participant, segment_index, &key, &input)
		|| input.slot.status != TT_SLOT_ACTIVE
		|| input.key.xid != input.slot.xid
		|| input.key.slot_wrap != input.slot.wrap)
		return false;
	/* The returned value is just the physical key. All source, base, effects,
	 * old-writer, top and C0 gates remain with their respective owners. */
	out->key = input.key;
	return true;
}

static bool
cold_top_tx_key_valid_v1(const ClusterColdTtSlotKeyV1 *key)
{
	return key != NULL && key->owner_instance != 0
		   && key->owner_instance <= UNDO_OWNER_INSTANCE_MAX && key->segment_id != 0
		   && (key->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1
				  == key->owner_instance
		   && key->segment_generation != UINT32_MAX
		   && key->slot_offset < TT_SLOTS_PER_SEGMENT && TransactionIdIsNormal(key->xid)
		   && key->slot_wrap != TT_WRAP_INVALID;
}

static bool
cold_top_tx_key_equal_v1(const ClusterColdTtSlotKeyV1 *left,
							 const ClusterColdTtSlotKeyV1 *right)
{
	return left != NULL && right != NULL && left->owner_instance == right->owner_instance
		   && left->segment_id == right->segment_id
		   && left->segment_generation == right->segment_generation
		   && left->slot_offset == right->slot_offset && left->xid == right->xid
		   && left->slot_wrap == right->slot_wrap;
}

static bool
cold_typed_page_binding_v1(const ClusterColdTypedV1 *typed, uint32 frame_index, bool new_creator,
						   ClusterColdRecordFrameV1 *out_frame, ClusterColdTtSlotKeyV1 *out_key)
{
	ClusterColdRecordFrameV1 frame = { 0 };
	ClusterColdTtSlotKeyV1 key = { 0 };
	const ClusterColdParticipantV1 *source;
	const RfSideOnlinePlanV1 *plan;
	RfSideOnlineOperationV1 prefix;
	RfSideUndoHeaderImageV1 image;
	const UndoSegmentHeaderData *header;
	const TTSlot *slot;
	uint32 count, prefix_index = UINT32_MAX;
	bool has_header_work, supported, found = false;
	uint8 opcode;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || typed->participant_count == 0
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count
		|| !cluster_cold_plan_record_frame_v1(typed->plan, frame_index, &frame)
		|| frame.participant >= typed->replay_count || frame.history || frame.page_components == 0
		|| frame.space_effects != 0 || !TransactionIdIsNormal(frame.source_xid)
		|| (TransactionIdIsValid(frame.source_top_xid) && frame.source_top_xid != frame.source_xid))
		return false;
	opcode = frame.info & XLOG_HEAP_OPMASK;
	if (new_creator)
		supported = (frame.rmid == RM_HEAP_ID
					 && (opcode == XLOG_HEAP_INSERT || opcode == XLOG_HEAP_UPDATE
						 || opcode == XLOG_HEAP_HOT_UPDATE))
					|| (frame.rmid == RM_HEAP2_ID && opcode == XLOG_HEAP2_MULTI_INSERT);
	else
		supported
			= (frame.rmid == RM_HEAP_ID && (opcode == XLOG_HEAP_DELETE || opcode == XLOG_HEAP_LOCK))
			  || (frame.rmid == RM_HEAP2_ID && opcode == XLOG_HEAP2_LOCK_UPDATED);
	if (!supported)
		return false;
	source = &typed->participants[frame.participant];
	if (!cluster_cold_plan_participant_matches_v1(typed->plan, frame.participant, source)
		|| !cold_tt_redo_source(typed, frame.participant, 0, 0, &has_header_work))
		return false;
	plan = typed->side_plans[frame.participant];
	count = rf_side_online_plan_operation_count_v1(plan);
	/* The native publisher accepts only its original top xid. An absent
	 * xl_top_xid is not itself top proof: the matching BIND supplies it. */
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;
		const ClusterUndoDecoded *bind = &operation.undo;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.identity.record.read_rec_ptr >= frame.end_rec_ptr)
			continue;
		if (operation.identity.record.end_rec_ptr > frame.read_rec_ptr)
			return false;
		if (operation.kind != RF_SIDE_ONLINE_OPERATION_UNDO
			|| bind->kind != CLUSTER_UNDO_KIND_TT_BIND || bind->xid != frame.source_xid)
			continue;
		if (found || bind->format_version != CLUSTER_UNDO_TT_BIND_VERSION
			|| bind->instance != source->thread_id
			|| (TransactionIdIsValid(operation.source_xid) && operation.source_xid != bind->xid)
			|| (TransactionIdIsValid(operation.source_top_xid)
				&& operation.source_top_xid != bind->xid))
			return false;
		key.owner_instance = bind->instance;
		key.segment_id = bind->segment_id;
		key.segment_generation = bind->expected_generation;
		key.slot_offset = bind->slot_offset;
		key.slot_wrap = bind->wrap;
		key.xid = bind->xid;
		found = true;
	}
	if (!found || !cold_top_tx_key_valid_v1(&key))
		return false;
	/* Fold only the causal prefix of this physical segment. Later COMMIT,
	 * ABORT or reuse cannot erase this operation's earlier ACTIVE input. */
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;
		bool same_segment;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.identity.record.end_rec_ptr > frame.read_rec_ptr)
			continue;
		same_segment = operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
					   && operation.undo.instance == key.owner_instance
					   && operation.undo.segment_id == key.segment_id;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& operation.xact.kind == RF_SIDE_XACT_COMMIT && operation.xact.has_tt_delta)
			same_segment = operation.xact.tt_delta.instance == key.owner_instance
						   && operation.xact.tt_delta.segment_id == key.segment_id;
		if (same_segment) {
			prefix_index = i;
			prefix = operation;
		}
	}
	if (prefix_index == UINT32_MAX
		|| rf_side_online_plan_prepare_undo_record_input_header_v1(plan, prefix_index,
																   &prefix.identity, NULL, &image)
			   != RF_PAGE_PROOF_DETAIL_OK
		|| !image.has_full_image)
		return false;
	header = (const UndoSegmentHeaderData *)image.page.data;
	slot = &header->tt_slots[key.slot_offset];
	if (header->wrap_count != key.segment_generation || slot->status != TT_SLOT_ACTIVE
		|| slot->xid != key.xid || slot->wrap != key.slot_wrap)
		return false;
	*out_frame = frame;
	*out_key = key;
	return true;
}

bool
cluster_cold_typed_page_creator_v1(const ClusterColdTypedV1 *typed, uint32 frame_index,
								   ClusterColdPageCreatorV1 *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	return cold_typed_page_binding_v1(typed, frame_index, true, &out->frame, &out->key);
}

bool
cluster_cold_typed_page_actor_v1(const ClusterColdTypedV1 *typed, uint32 frame_index,
								 ClusterColdPageActorV1 *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	return cold_typed_page_binding_v1(typed, frame_index, false, &out->frame, &out->key);
}

bool
cluster_cold_top_tx_loser_v1(const ClusterColdTtActiveIdentityV1 *active,
							 const ClusterColdTopTxProofV1 *proof,
							 ClusterColdLoserIdentityV1 *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (active == NULL || proof == NULL || !cold_top_tx_key_valid_v1(&active->key)
		|| !cold_top_tx_key_valid_v1(&proof->key)
		|| !cold_top_tx_key_equal_v1(&active->key, &proof->key)
		|| !proof->producer_top || !proof->qualified_base || !proof->sealed_redo
		|| !proof->ordinary_effects || !proof->old_writer_isolated || !proof->terminal_match)
		return false;
	out->key = active->key;
	return true;
}

bool
cluster_cold_top_tx_c0_v1(const ClusterColdLoserIdentityV1 *loser,
						  const ClusterRecoveryTTAbortV1 *shape, uint64 next_xid,
						  bool all_endpoints_complete, bool has_oldest_xid,
						  TransactionId oldest_xid, uint64 recoverer_epoch,
						  ClusterRecoveryTTAbortDerivationV1 *out)
{
	const ClusterColdTtSlotKeyV1 *key;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (loser == NULL || shape == NULL || !cold_top_tx_key_valid_v1(&loser->key))
		return false;
	key = &loser->key;
	/* Exact physical identity is checked before any FullXid widening. */
	if (shape->target_thread != key->owner_instance
		|| shape->exact.segment_id != key->segment_id
		|| shape->exact.segment_generation != key->segment_generation
		|| shape->exact.xid != key->xid || shape->exact.slot_offset != key->slot_offset
		|| shape->exact.wrap != key->slot_wrap || shape->exact.instance != key->owner_instance
		|| shape->before.xid != key->xid || shape->before.wrap != key->slot_wrap
		|| shape->before.status != TT_SLOT_ACTIVE)
		return false;
	/* This helper owns only C0 field derivation. The physical loser remains
	 * valid if this call refuses because N, L, or epoch evidence is missing. */
	return cluster_recovery_tt_abort_derive_fields_v1(shape, next_xid,
										  all_endpoints_complete, has_oldest_xid,
										  oldest_xid, recoverer_epoch, out);
}

/* The current native BIND has one production emitter, called by canonical
 * ACTIVE publication with the original owner's top XID. Its exact physical
 * identity can qualify top ownership, including retained pre-redo input from
 * this same source. This is not a lifetime BIND retention requirement: an
 * older ACTIVE without this input still needs a qualified canonical base.
 * Neither raw ACTIVE bytes nor another slot's BIND can supply that fact. */
static bool
cold_top_tx_native_bind_v1(const RfSideOnlinePlanV1 *plan, const ClusterColdTtSlotKeyV1 *key)
{
	uint32 count = rf_side_online_plan_operation_count_v1(plan);

	if (count == UINT32_MAX)
		return false;
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;
		const ClusterUndoDecoded *bind = &operation.undo;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
			&& bind->kind == CLUSTER_UNDO_KIND_TT_BIND
			&& bind->format_version == CLUSTER_UNDO_TT_BIND_VERSION
			&& bind->instance == key->owner_instance && bind->segment_id == key->segment_id
			&& bind->expected_generation == key->segment_generation
			&& bind->slot_offset == key->slot_offset && bind->wrap == key->slot_wrap
			&& bind->xid == key->xid)
			return true;
	}
	return false;
}

static bool
cold_top_tx_frame_xids_v1(TransactionId xid, TransactionId top)
{
	return (!TransactionIdIsValid(xid) || TransactionIdIsNormal(xid))
		   && (!TransactionIdIsValid(top)
			   || (TransactionIdIsNormal(top) && TransactionIdIsNormal(xid) && top == xid));
}

/* A native FPI can retain records from several transactions. Select the first
 * record only to address the existing reader, which checks the entire block
 * before returning it. These raw identities need not name the WAL actor or
 * selected loser and do not establish generation or terminal authority. */
static bool
cold_top_tx_undo_fpi_v1(const RfSideOnlineOperationV1 *operation)
{
	const ClusterUndoDecoded *undo = &operation->undo;
	UndoBlockHeader block;
	UndoSlotDirEntry slot;
	UndoRecordHeader header;
	ClusterRecoveryUndoRecord record;
	UBA address;

	if (operation->owned_payload == NULL || operation->owned_payload_length != BLCKSZ
		|| undo->instance == 0 || undo->instance > UNDO_OWNER_INSTANCE_MAX || undo->segment_id == 0
		|| undo->segment_id > UINT16_MAX || undo->block_no == 0)
		return false;
	memcpy(&block, operation->owned_payload, sizeof(block));
	memcpy(&slot, operation->owned_payload + UNDO_SLOT_DIR_OFFSET(0), sizeof(slot));
	if (!cluster_undo_record_slot_range_valid(block.slot_count, 0, slot.record_offset,
											  slot.record_length)
		|| slot.record_length < sizeof(header))
		return false;
	memcpy(&header, operation->owned_payload + slot.record_offset, sizeof(header));
	if (header.tt_slot_id == 0 || header.tt_slot_id > TT_SLOTS_PER_SEGMENT)
		return false;
	address = uba_encode(undo->segment_id, undo->block_no, header.tt_slot_id - 1, 0);
	return cluster_recovery_undo_record_read(operation->owned_payload,
											 operation->owned_payload_length, undo->segment_id,
											 undo->block_no, undo->instance, address, &record)
		   == CLUSTER_RECOVERY_UNDO_ENTRY;
}

/* Preserve operation-local SIDE/PAGE/SPACE evidence, including retained history. A
 * known child frame cannot become canonical just because the candidate's
 * BIND is top. Conversely an unrelated operation names its own actor, not
 * this candidate. Replay DML additionally needs its own canonical creator
 * or actor prefix. Old tuple xmin/undo before-images and individual MX
 * members are not qualified here; ordinary_effects remains independent. */
static bool
cold_top_tx_frames_v1(const ClusterColdTypedV1 *typed)
{
	uint32 count;

	if (!cluster_cold_plan_record_frame_count_v1(typed->plan, &count))
		return false;
	for (uint32 i = 0; i < count; i++) {
		ClusterColdRecordFrameV1 frame;
		uint8 opcode;
		bool creator, actor;

		if (!cluster_cold_plan_record_frame_v1(typed->plan, i, &frame)
			|| frame.participant >= typed->participant_count
			|| !cold_top_tx_frame_xids_v1(frame.source_xid, frame.source_top_xid))
			return false;
		if (frame.history || frame.page_components == 0)
			continue;
		/* Raw/top equality alone does not prove a replay DML's canonical
		 * owner. Use that record's original BIND and ACTIVE causal prefix,
		 * independently of the selected loser's identity or final status
		 * of this other actor. Old tuple and MX inputs remain separate. */
		opcode = frame.info & XLOG_HEAP_OPMASK;
		creator = (frame.rmid == RM_HEAP_ID
				   && (opcode == XLOG_HEAP_INSERT || opcode == XLOG_HEAP_UPDATE
					   || opcode == XLOG_HEAP_HOT_UPDATE))
				  || (frame.rmid == RM_HEAP2_ID && opcode == XLOG_HEAP2_MULTI_INSERT);
		actor
			= (frame.rmid == RM_HEAP_ID && (opcode == XLOG_HEAP_DELETE || opcode == XLOG_HEAP_LOCK))
			  || (frame.rmid == RM_HEAP2_ID && opcode == XLOG_HEAP2_LOCK_UPDATED);
		if (creator) {
			ClusterColdPageCreatorV1 input;

			if (!cluster_cold_typed_page_creator_v1(typed, i, &input))
				return false;
		} else if (actor) {
			ClusterColdPageActorV1 input;

			if (!cluster_cold_typed_page_actor_v1(typed, i, &input))
				return false;
		}
	}
	for (uint32 p = 0; p < typed->participant_count; p++) {
		const RfSideOnlinePlanV1 *plan = typed->side_plans[p];
		uint32 count = rf_side_online_plan_operation_count_v1(plan);

		if (count == UINT32_MAX)
			return false;
		for (uint32 i = 0; i < count; i++) {
			RfSideOnlineOperationV1 operation;

			if (!rf_side_online_plan_operation_v1(plan, i, &operation)
				|| !cold_top_tx_frame_xids_v1(operation.source_xid, operation.source_top_xid))
				return false;
			/* Native BIND and ordinary exact TT_ABORT persist their own
			 * slot owner. A present record actor must agree with that xid,
			 * even when this frame omitted top metadata. An unrelated loser's
			 * BIND cannot repair a conflicting abort actor. */
			if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
				&& (operation.undo.kind == CLUSTER_UNDO_KIND_TT_BIND
					|| (operation.undo.kind == CLUSTER_UNDO_KIND_TT_ABORT
						&& operation.undo.format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION))
				&& TransactionIdIsValid(operation.source_xid)
				&& operation.source_xid != operation.undo.xid)
				return false;
			if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO && operation.undo.has_fpi
				&& (operation.undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
					|| operation.undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
				&& !cold_top_tx_undo_fpi_v1(&operation))
				return false;
		}
	}
	return true;
}

/* A retained incarnation is not folded into the current owner's header,
 * but its exact terminal still contradicts the same physical ACTIVE key.
 * Neither a different claim/epoch nor history_only erases that conflict.
 * A native ABORT has no physical TT key: require its original exact carrier
 * when it could name this transaction. Absence of conflicting carriers alone
 * is insufficient without the complete native lifecycle and redo below. */
static bool
cold_top_tx_terminals_compatible_v1(const ClusterColdTypedV1 *typed,
									const ClusterColdTtSlotKeyV1 *key)
{
	for (uint32 p = 0; p < typed->participant_count; p++) {
		const RfSideOnlinePlanV1 *plan = typed->side_plans[p];
		uint32 count = rf_side_online_plan_operation_count_v1(plan);

		if (count == UINT32_MAX)
			return false;
		for (uint32 i = 0; i < count; i++) {
			RfSideOnlineOperationV1 operation;
			ClusterColdTtSlotKeyV1 terminal = { 0 };

			if (!rf_side_online_plan_operation_v1(plan, i, &operation))
				return false;
			if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
				&& operation.xact.kind == RF_SIDE_XACT_COMMIT && operation.xact.has_tt_delta) {
				const xl_xact_tt_commit *delta = &operation.xact.tt_delta;

				terminal.owner_instance = delta->instance;
				terminal.segment_id = delta->segment_id;
				terminal.segment_generation = delta->segment_generation;
				terminal.slot_offset = delta->slot_offset;
				terminal.slot_wrap = delta->wrap;
				terminal.xid = delta->xid;
			} else if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
					   && operation.undo.kind == CLUSTER_UNDO_KIND_TT_ABORT
					   && operation.undo.format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION) {
				const ClusterUndoDecoded *abort = &operation.undo;

				terminal.owner_instance = abort->instance;
				terminal.segment_id = abort->segment_id;
				terminal.segment_generation = abort->expected_generation;
				terminal.slot_offset = abort->slot_offset;
				terminal.slot_wrap = abort->wrap;
				terminal.xid = abort->xid;
			} else if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
					   && operation.xact.kind == RF_SIDE_XACT_ABORT) {
				ClusterUndoDecoded abort;

				if (operation.xact.origin_thread != key->owner_instance
					|| operation.xact.xid != key->xid)
					continue;
				/* Never assign raw xid to a physical slot ourselves. The
				 * original owner requires the unique immutable preceding
				 * exact TT_ABORT and excludes intervening reuse/mutation.
				 * It cannot qualify history-only ABORTs, which stay blocked. */
				if (!rf_side_online_plan_abort_target_v1(plan, &operation, &abort))
					return false;
				terminal.owner_instance = abort.instance;
				terminal.segment_id = abort.segment_id;
				terminal.segment_generation = abort.expected_generation;
				terminal.slot_offset = abort.slot_offset;
				terminal.slot_wrap = abort.wrap;
				terminal.xid = abort.xid;
			} else
				continue;
			if (cold_top_tx_key_equal_v1(&terminal, key))
				return false;
		}
	}
	return true;
}

/* Consume the original owner's selected native checkpoint, not a caller's
 * checkpoint number. This binds the source and replay cut only; it does not
 * establish that any captured TT base is covered by that checkpoint. */
static bool
cold_top_tx_checkpoint_v1(const ClusterColdTypedV1 *typed, uint32 participant)
{
	const ClusterColdParticipantV1 *cut = &typed->participants[participant];
	const ClusterWalSourceRef *source = &typed->sources[participant];
	ClusterControlRootSnapshot root;
	ClusterWalRecoveryCheckpointV1 selected;

	if (!cluster_cold_typed_checkpoint_v1(typed, participant, &root, &selected))
		return false;
	return memcmp(&selected.source, source, sizeof(*source)) == 0
		   && memcmp(&root.identity, &source->claim.identity, sizeof(root.identity)) == 0
		   && root.identity.origin_thread_id == cut->thread_id
		   && root.identity.origin_owner_incarnation == cut->owner_incarnation
		   && root.checkpoint_tli == cut->timeline && source->timeline == cut->timeline
		   && selected.checkpoint.ThisTimeLineID == cut->timeline
		   && root.checkpoint_lower_lsn == cut->physical_lower
		   && root.validated_tail_lsn_exclusive == cut->tail_end
		   && !XLogRecPtrIsInvalid(selected.checkpoint_start)
		   && selected.checkpoint_start >= cut->physical_lower
		   && selected.checkpoint_start < cut->tail_end
		   && !XLogRecPtrIsInvalid(selected.checkpoint.redo)
		   && selected.checkpoint.redo == cut->native_redo
		   && selected.checkpoint.redo >= cut->physical_lower
		   && selected.checkpoint.redo <= selected.checkpoint_start;
}

bool
cluster_cold_typed_top_tx_c0_producer_v1(ClusterColdTypedV1 *typed,
										 struct ClusterRecoveryFencePlan *fence, uint32 participant,
										 const ClusterColdTopTxProofV1 *proof,
										 const ClusterRecoveryTTAbortV1 *shape, bool has_oldest_xid,
										 TransactionId oldest_xid, uint64 recoverer_epoch,
										 ClusterColdTopTxC0ProducerResultV1 *out)
{
	ClusterColdTopTxC0ProducerResultV1 result = { 0 };
	ClusterColdTopTxProofV1 qualified;
	RfSideUndoHeaderImageV1 post_image, base_image;
	ClusterColdTtSlotInputV1 post_slot;
	uint32 post_segment_index;
	uint64 next_xid;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count || participant >= typed->replay_count
		|| proof == NULL || shape == NULL
		|| proof->key.owner_instance != typed->participants[participant].thread_id
		|| !cluster_recovery_tt_abort_shape_valid_v1(shape)
		|| shape->target_incarnation != typed->participants[participant].owner_incarnation
		|| memcmp(shape->target_claim_sha256, typed->sources[participant].claim.claim_sha256,
				  sizeof(shape->target_claim_sha256))
			   != 0
		|| !cold_top_tx_checkpoint_v1(typed, participant)
		|| !cluster_cold_typed_xid_maximum_v1(typed, &next_xid)
		|| !cluster_cold_typed_effects_ready_v1(typed)
		|| !cluster_cold_typed_sources_current_v1(typed, fence))
		return false;
	/* Cached TT/UNDO bytes cannot attest to their own physical freshness.
	 * Reuse the original owner's full namespace, inode and byte recheck;
	 * refusal invalidates the post-redo snapshot rather than refreshing it. */
	if (cluster_cold_tt_after_redo_recheck_v1(typed) != CLUSTER_COLD_OK
		|| cluster_cold_tt_header_find_v1(typed, true, proof->key.owner_instance,
										  proof->key.segment_id, &post_segment_index)
			   == NULL
		|| !cluster_cold_tt_after_redo_slot_v1(typed, post_segment_index, proof->key.slot_offset,
											   &post_slot)
		|| post_slot.slot.status != TT_SLOT_ACTIVE
		|| !cold_top_tx_key_equal_v1(&post_slot.key, &proof->key))
		return false;
	/* A valid target alone cannot discharge missing/unsupported retained
	 * sources, another TT header, or an uncached native UNDO effect. Reuse
	 * the complete ordinary inventory rather than trusting the proof flags
	 * for those inputs. This does not establish checkpoint coverage,
	 * atomic transaction effects or old-writer isolation. */
	if (!cluster_cold_typed_tt_redo_inventory_v1(typed) || !cold_top_tx_frames_v1(typed))
		return false;
	/* Compare the whole selected header with its sealed original fold.  A
	 * newly-created segment requires the existing native full-image proof;
	 * its observed bytes can never supply a missing predecessor. */
	if (!cluster_cold_typed_tt_after_redo_readback_v1(typed, participant, post_segment_index,
													  &post_image))
		return false;
	/* The caller's producer_top bit is not an original producer fact. Only
	 * this source's exact native BIND currently qualifies this gate here. */
	qualified = *proof;
	qualified.producer_top
		= cold_top_tx_native_bind_v1(typed->side_plans[participant], &post_slot.key);
	/* Reconstruct the complete generation from its retained native INIT or
	 * REUSE, with no captured TT predecessor. A raw page and a caller's bit
	 * cannot establish checkpoint coverage. Older ACTIVE generations without
	 * this full lifecycle remain blocked until their original base owner can
	 * supply the independent checkpoint coverage proof. */
	qualified.qualified_base = rf_side_online_plan_prepare_undo_header_v1(
								   typed->side_plans[participant], post_slot.key.owner_instance,
								   post_slot.key.segment_id, NULL, &base_image)
								   == RF_PAGE_PROOF_DETAIL_OK
							   && base_image.has_full_image
							   && memcmp(base_image.page.data, post_image.page.data, BLCKSZ) == 0;
	/* The original all-record effects receipt, bound by the endpoint receipt
	 * to this exact plan and source set, and the verified ordinary inventory
	 * establish completed sealed redo. The caller's bit cannot add or revoke
	 * that fact. Atomic transaction effects remain an independent proof. */
	qualified.sealed_redo = true;
	/* The whole retained INIT/REUSE lifecycle reaches this exact physical
	 * ACTIVE image after completed all-record redo. Together with the full
	 * retained terminal check, this supplies the terminal proof on either
	 * side of the selected redo cut. A missing lifecycle/coverage or an
	 * ambiguous ABORT cannot be replaced by the caller's terminal bit. */
	qualified.terminal_match = qualified.qualified_base && qualified.sealed_redo
							   && cold_top_tx_terminals_compatible_v1(typed, &post_slot.key);
	/* Only the original target owner can supply real old-writer isolation.
	 * Its current pass-2 authority remains distinct from a C0 write grant. */
	qualified.old_writer_isolated
		= cluster_cold_typed_old_writer_isolated_v1(typed, fence, participant);
	result.active.key = post_slot.key;
	if (!cluster_cold_top_tx_loser_v1(&result.active, &qualified, &result.loser)
		|| !cluster_cold_top_tx_c0_v1(&result.loser, shape, next_xid, true, has_oldest_xid,
									  oldest_xid, recoverer_epoch, &result.c0)
		|| cluster_recovery_tt_abort_match_v1(&result.c0.record, post_slot.key.segment_generation,
											  &post_slot.slot)
			   != CLUSTER_RECOVERY_TT_ABORT_APPLY)
		return false;
	/* The inventory may read additional UNDO blocks. Recheck every retained
	 * physical input and original target isolation again before publishing
	 * this candidate descriptor. Neither check grants C0 write authority. */
	if (cluster_cold_tt_after_redo_recheck_v1(typed) != CLUSTER_COLD_OK
		|| !cluster_cold_typed_sources_current_v1(typed, fence)
		|| !cluster_cold_typed_old_writer_isolated_v1(typed, fence, participant))
		return false;
	*out = result;
	return true;
}

bool
cluster_cold_typed_undo_redo_record_v1(const ClusterColdTypedV1 *typed, uint32 participant,
									   uint32 segment_index, UBA address,
									   ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	RfSideUndoHeaderImageV1 header;
	RfSideUndoBlockImageV1 block;
	uint8 owner;
	uint32 segment, address_segment, block_no;
	uint16 tt_slot, row;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (out == NULL || generation == NULL
		|| cluster_cold_tt_base_header_v1(typed, segment_index, &owner, &segment) == NULL
		|| !uba_decode_record(address, &address_segment, &block_no, &tt_slot, &row)
		|| address_segment != segment || block_no == 0
		|| !cluster_cold_typed_tt_redo_header_v1(typed, participant, segment_index, &header))
		return false;
	/* Use the same sealed source in its entirety. The original block fold
	 * requires an FPI after the last INIT/REUSE, and refuses an earlier
	 * unanchored delta rather than borrowing a future FPI or target DATA. */
	if (rf_side_online_plan_prepare_undo_block_v1(typed->side_plans[participant], owner, segment,
												  block_no, &block)
			!= RF_PAGE_PROOF_DETAIL_OK
		|| cluster_recovery_undo_record_read(block.page.data, BLCKSZ, segment, block_no, owner,
											 address, out)
			   != CLUSTER_RECOVERY_UNDO_ENTRY)
		return false;
	*generation = ((const UndoSegmentHeaderData *)header.page.data)->wrap_count;
	return true;
}

static const RfSideOnlinePlanV1 *
cold_tt_undo_base_redo_plan(const ClusterColdTypedV1 *typed, uint32 participant,
							uint32 segment_index, uint8 *owner, uint32 *segment)
{
	const UndoSegmentHeaderData *base_header;
	const RfSideOnlinePlanV1 *plan;
	RfSideUndoHeaderImageV1 header;

	base_header = cluster_cold_tt_base_header_v1(typed, segment_index, owner, segment);
	if (base_header == NULL
		|| !cluster_cold_typed_tt_redo_header_v1(typed, participant, segment_index, &header)
		|| base_header->wrap_count != ((const UndoSegmentHeaderData *)header.page.data)->wrap_count)
		return NULL;
	plan = typed->side_plans[participant];
	/* Prefix history is already represented by the captured base. Only a
	 * lifecycle operation in the replay interval breaks its DATA ancestry;
	 * a later FPI cannot restore that ancestry after INIT or REUSE. */
	for (uint32 i = 0; i < rf_side_online_plan_operation_count_v1(plan); i++) {
		RfSideOnlineOperationV1 operation;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation)
			|| (!operation.history_only && operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO
				&& operation.undo.instance == *owner && operation.undo.segment_id == *segment
				&& (operation.undo.kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
					|| operation.undo.kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE)))
			return NULL;
	}
	return plan;
}

static bool
cold_tt_undo_base_redo_block(const ClusterColdTypedV1 *typed, uint32 participant,
							 const RfSideOnlinePlanV1 *plan, uint8 owner, uint32 segment,
							 uint32 block_no, PGAlignedBlock *block)
{
	XLogRecPtr base_lsn = ((const UndoBlockHeader *)block->data)->block_lsn;

	/* This bound alone never qualifies checkpoint/base coverage. */
	if (XLogRecPtrIsInvalid(base_lsn) || base_lsn > typed->participants[participant].native_redo)
		return false;
	for (uint32 i = 0; i < rf_side_online_plan_operation_count_v1(plan); i++) {
		RfSideOnlineOperationV1 operation;
		const ClusterUndoDecoded *decoded = &operation.undo;

		if (!rf_side_online_plan_operation_v1(plan, i, &operation))
			return false;
		if (operation.history_only || operation.kind != RF_SIDE_ONLINE_OPERATION_UNDO
			|| decoded->instance != owner || decoded->segment_id != segment
			|| decoded->block_no != block_no
			|| (decoded->kind != CLUSTER_UNDO_KIND_BLOCK_WRITE
				&& decoded->kind != CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI))
			continue;
		if (!cluster_undo_prepare_block_v1(
				decoded, operation.owned_payload, operation.owned_payload_length,
				operation.identity.record.end_rec_ptr, block->data, block->data))
			return false;
	}
	return true;
}

bool
cluster_cold_typed_undo_base_redo_block_v1(const ClusterColdTypedV1 *typed, uint32 participant,
										   uint32 segment_index, uint32 block_no,
										   const PGAlignedBlock *base, PGAlignedBlock *out)
{
	const RfSideOnlinePlanV1 *plan;
	PGAlignedBlock block;
	uint8 owner;
	uint32 segment;

	if (base != NULL)
		block = *base;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (base == NULL || out == NULL || block_no == 0 || block_no >= UNDO_SEGMENT_SIZE_BYTES / BLCKSZ
		|| (plan = cold_tt_undo_base_redo_plan(typed, participant, segment_index, &owner, &segment))
			   == NULL
		|| !cold_tt_undo_base_redo_block(typed, participant, plan, owner, segment, block_no,
										 &block))
		return false;
	*out = block;
	return true;
}

bool
cluster_cold_typed_undo_base_redo_record_v1(ClusterColdTypedV1 *typed, uint32 participant,
											uint32 segment_index, UBA address,
											ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	const RfSideOnlinePlanV1 *plan;
	PGAlignedBlock block;
	ClusterRecoveryUndoRecord record;
	uint8 owner;
	uint32 segment, address_segment, block_no, captured_generation;
	uint16 tt_slot, row;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (out == NULL || generation == NULL
		|| (plan = cold_tt_undo_base_redo_plan(typed, participant, segment_index, &owner, &segment))
			   == NULL
		|| !uba_decode_record(address, &address_segment, &block_no, &tt_slot, &row)
		|| address_segment != segment || block_no == 0
		|| !cluster_cold_tt_base_block_v1(typed, segment_index, block_no, &block,
										  &captured_generation)
		|| !cold_tt_undo_base_redo_block(typed, participant, plan, owner, segment, block_no, &block)
		|| cluster_recovery_undo_record_read(block.data, BLCKSZ, segment, block_no, owner, address,
											 &record)
			   != CLUSTER_RECOVERY_UNDO_ENTRY)
		return false;
	*out = record;
	*generation = captured_generation;
	return true;
}

/* Both explicit views require the same after-redo canonical TT key, while
 * the record can reside in another captured physical segment. */
static ClusterRecoveryGraphResult
cold_tt_redo_graph_match(const ClusterColdTypedV1 *typed, uint32 participant,
						 const ClusterRecoveryTxnKey *key, UBA address, uint32 *physical_index_out)
{
	ClusterColdTtSlotKeyV1 expected = { 0 };
	ClusterColdTtSlotInputV1 slot;
	uint32 physical_segment, block, canonical_index = UINT32_MAX, physical_index = UINT32_MAX;
	uint16 tt_slot, row;

	if (!cluster_cold_tt_base_namespace_matches_v1(typed, key->system_identifier,
												   key->database_incarnation)
		|| key->origin_node_id >= UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	if (!uba_decode_record(address, &physical_segment, &block, &tt_slot, &row))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	expected.owner_instance = key->origin_node_id + 1;
	expected.segment_id = key->segment_id;
	expected.segment_generation = key->segment_generation;
	expected.slot_offset = key->slot_offset;
	expected.xid = key->xid;
	expected.slot_wrap = key->slot_wrap;
	for (uint32 i = 0; i < cluster_cold_tt_base_count_v1(typed); i++) {
		uint8 owner;
		uint32 segment;

		if (cluster_cold_tt_base_header_v1(typed, i, &owner, &segment) == NULL)
			return CLUSTER_RECOVERY_GRAPH_INVALID;
		if (owner != expected.owner_instance)
			continue;
		if (segment == expected.segment_id)
			canonical_index = i;
		if (segment == physical_segment)
			physical_index = i;
	}
	if (canonical_index == UINT32_MAX || physical_index == UINT32_MAX
		|| !cluster_cold_typed_tt_redo_match_v1(typed, participant, canonical_index, &expected,
												&slot))
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	*physical_index_out = physical_index;
	return CLUSTER_RECOVERY_GRAPH_MORE;
}

ClusterRecoveryGraphResult
cluster_cold_typed_tt_redo_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
										 ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	const ClusterColdTtRedoGraphInputV1 *input = arg;
	ClusterRecoveryGraphResult result;
	uint32 physical_index;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (input == NULL || input->typed == NULL || key == NULL || out == NULL || generation == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	result
		= cold_tt_redo_graph_match(input->typed, input->participant, key, address, &physical_index);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		return result;
	if (!cluster_cold_typed_undo_redo_record_v1(input->typed, input->participant, physical_index,
												address, out, generation))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	result = cluster_recovery_tx_record_check(key, address, out, *generation);
	if (result == CLUSTER_RECOVERY_GRAPH_MORE)
		return CLUSTER_RECOVERY_GRAPH_RECORD;
	memset(out, 0, sizeof(*out));
	*generation = 0;
	return result;
}

ClusterRecoveryGraphResult
cluster_cold_typed_tt_base_redo_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key,
											  UBA address, ClusterRecoveryUndoRecord *out,
											  uint32 *generation)
{
	ClusterColdTtBaseRedoGraphInputV1 *input = arg;
	ClusterRecoveryGraphResult result;
	uint32 physical_index;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (input == NULL || input->typed == NULL || key == NULL || out == NULL || generation == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	result
		= cold_tt_redo_graph_match(input->typed, input->participant, key, address, &physical_index);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		return result;
	if (!cluster_cold_typed_undo_base_redo_record_v1(input->typed, input->participant,
													 physical_index, address, out, generation))
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	result = cluster_recovery_tx_record_check(key, address, out, *generation);
	if (result == CLUSTER_RECOVERY_GRAPH_MORE)
		return CLUSTER_RECOVERY_GRAPH_RECORD;
	memset(out, 0, sizeof(*out));
	*generation = 0;
	return result;
}

static ClusterRecoveryGraphResult
cold_tt_redo_graph_begin(const ClusterColdTypedV1 *typed, uint32 participant,
						 ClusterRecoveryGraphRead read, void *input, size_t memory_budget,
						 ClusterRecoveryReferenceGraph **out)
{
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphResult result;
	bool unused_header_work;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || typed == NULL)
		return CLUSTER_RECOVERY_GRAPH_INVALID;
	/* The source must be valid even when its owner has no TT files. */
	if (!cluster_cold_tt_base_namespace_matches_v1(typed, typed->system_identifier,
												   typed->observer.database_incarnation)
		|| !cold_tt_redo_source(typed, participant, 0, 0, &unused_header_work))
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	result = cluster_recovery_reference_graph_begin(memory_budget, read, input, &graph);
	if (result != CLUSTER_RECOVERY_GRAPH_MORE)
		return result;
	for (uint32 i = 0; i < cluster_cold_tt_base_count_v1(typed); i++) {
		RfSideUndoHeaderImageV1 image;
		const UndoSegmentHeaderData *header;
		uint8 owner;
		uint32 segment;

		if (cluster_cold_tt_base_header_v1(typed, i, &owner, &segment) == NULL)
			goto identity_failure;
		if (owner != typed->participants[participant].thread_id)
			continue;
		if (!cluster_cold_typed_tt_redo_header_v1(typed, participant, i, &image))
			goto identity_failure;
		header = (const UndoSegmentHeaderData *)image.page.data;
		for (uint16 offset = 0; offset < TT_SLOTS_PER_SEGMENT; offset++) {
			const TTSlot *slot = &header->tt_slots[offset];
			ClusterRecoveryTxnKey key = { 0 };

			/* Retention survives terminal status. A zero head is merely absent
			 * from this TX forest, never proof that other effects are absent. */
			if (UBA_is_invalid(slot->first_undo_block))
				continue;
			key.system_identifier = typed->system_identifier;
			key.database_incarnation = typed->observer.database_incarnation;
			key.origin_node_id = owner - 1;
			key.segment_id = segment;
			key.segment_generation = header->wrap_count;
			key.slot_offset = offset;
			key.slot_wrap = slot->wrap;
			key.xid = slot->xid;
			result = cluster_recovery_reference_graph_add(graph, &key, slot->first_undo_block);
			if (result != CLUSTER_RECOVERY_GRAPH_MORE)
				goto failure;
		}
	}
	*out = graph;
	return CLUSTER_RECOVERY_GRAPH_MORE;

identity_failure:
	result = CLUSTER_RECOVERY_GRAPH_IDENTITY;
failure:
	cluster_recovery_reference_graph_destroy(graph);
	return result;
}

ClusterRecoveryGraphResult
cluster_cold_typed_tt_redo_graph_begin_v1(ClusterColdTtRedoGraphInputV1 *input,
										  size_t memory_budget, ClusterRecoveryReferenceGraph **out)
{
	return cold_tt_redo_graph_begin(
		input == NULL ? NULL : input->typed, input == NULL ? 0 : input->participant,
		cluster_cold_typed_tt_redo_graph_read_v1, input, memory_budget, out);
}

ClusterRecoveryGraphResult
cluster_cold_typed_tt_base_redo_graph_begin_v1(ClusterColdTtBaseRedoGraphInputV1 *input,
											   size_t memory_budget,
											   ClusterRecoveryReferenceGraph **out)
{
	return cold_tt_redo_graph_begin(
		input == NULL ? NULL : input->typed, input == NULL ? 0 : input->participant,
		cluster_cold_typed_tt_base_redo_graph_read_v1, input, memory_budget, out);
}
