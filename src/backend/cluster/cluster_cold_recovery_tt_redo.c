/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_tt_redo.c
 *    Read-only ordinary redo of one original captured TT base input.
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
#include "cluster/cluster_side_online_plan.h"

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
