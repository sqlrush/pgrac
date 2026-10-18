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
 *    No original bytes are installed and no transaction proof is created.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_uba.h"

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
