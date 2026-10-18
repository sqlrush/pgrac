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

bool
cluster_cold_typed_tt_redo_header_v1(const ClusterColdTypedV1 *typed, uint32 participant,
									 uint32 segment_index, RfSideUndoHeaderImageV1 *out)
{
	const ClusterColdParticipantV1 *source;
	const UndoSegmentHeaderData *base;
	const RfSideOnlinePlanV1 *plan;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideUndoHeaderImageV1 image = { 0 };
	uint8 owner;
	uint32 segment, count;
	bool has_header_work = false;

	if (typed == NULL || out == NULL || typed->refusal != CLUSTER_COLD_OK
		|| typed->participant_count == 0 || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count > typed->participant_count || participant >= typed->replay_count
		|| typed->side_capture_detail[participant] != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	base = cluster_cold_tt_base_header_v1(typed, segment_index, &owner, &segment);
	source = &typed->participants[participant];
	plan = typed->side_plans[participant];
	if (base == NULL || owner != source->thread_id)
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
			has_header_work = true;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& operation.xact.kind == RF_SIDE_XACT_COMMIT && operation.xact.has_tt_delta
			&& operation.xact.tt_delta.instance == owner
			&& operation.xact.tt_delta.segment_id == segment)
			has_header_work = true;
	}
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

ClusterRecoveryGraphResult
cluster_cold_typed_tt_redo_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
										 ClusterRecoveryUndoRecord *out, uint32 *generation)
{
	const ClusterColdTtRedoGraphInputV1 *input = arg;
	const ClusterColdTypedV1 *typed;
	ClusterColdTtSlotKeyV1 expected = { 0 };
	ClusterColdTtSlotInputV1 slot;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_INVALID;
	uint32 physical_segment, block, canonical_index = UINT32_MAX, physical_index = UINT32_MAX;
	uint16 tt_slot, row;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (generation != NULL)
		*generation = 0;
	if (input == NULL || input->typed == NULL || key == NULL || out == NULL || generation == NULL)
		return result;
	typed = input->typed;
	if (!cluster_cold_tt_base_namespace_matches_v1(typed, key->system_identifier,
												   key->database_incarnation)
		|| key->origin_node_id >= UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	if (!uba_decode_record(address, &physical_segment, &block, &tt_slot, &row))
		return result;
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
			return result;
		if (owner != expected.owner_instance)
			continue;
		if (segment == expected.segment_id)
			canonical_index = i;
		if (segment == physical_segment)
			physical_index = i;
	}
	if (canonical_index == UINT32_MAX || physical_index == UINT32_MAX
		|| !cluster_cold_typed_tt_redo_match_v1(typed, input->participant, canonical_index,
												&expected, &slot))
		return CLUSTER_RECOVERY_GRAPH_IDENTITY;
	if (!cluster_cold_typed_undo_redo_record_v1(typed, input->participant, physical_index, address,
												out, generation))
		return result;
	result = cluster_recovery_tx_record_check(key, address, out, *generation);
	if (result == CLUSTER_RECOVERY_GRAPH_MORE)
		return CLUSTER_RECOVERY_GRAPH_RECORD;
	memset(out, 0, sizeof(*out));
	*generation = 0;
	return result;
}
