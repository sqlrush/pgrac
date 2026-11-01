/*-------------------------------------------------------------------------
 * cluster_cold_recovery_relmap.c
 *     One-shot relmap participant at the original cold replay position.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/backend/cluster/cluster_cold_recovery_relmap.c
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_side_online_plan.h"
#include "storage/lwlock.h"
#include "utils/relmapper.h"
#include "miscadmin.h"
#include "catalog/pg_tablespace_d.h"
#include "replication/origin.h"

static bool
relmap_current(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence,
	const ClusterColdRelmapInputV1 *input)
{
	const ClusterColdPlanV1 *plan = typed->plan;
	uint32 p = input->participant;

	return typed->refusal == CLUSTER_COLD_OK && p < typed->replay_count
		&& typed->participant_count == plan->participant_count
		&& typed->system_identifier == input->staged.identity.system_identifier
		&& cold_relmap_cut_equal(&input->cut, &typed->participants[p])
		&& memcmp(&input->source, &typed->sources[p], sizeof(input->source)) == 0
		&& cluster_cold_typed_sources_current_v1(typed, fence);
}

static bool
relmap_side_matches(const ClusterColdTypedV1 *typed, const ClusterColdRelmapInputV1 *input,
	const XLogReaderState *reader)
{
	const ClusterColdPlanV1 *plan = typed->plan;
	const RfSideOnlinePlanV1 *side;
	uint32 expected = 0;

	if (input->participant >= typed->participant_count)
		return false;
	side = typed->side_plans[input->participant];
	if (!rf_side_online_plan_recovery_sources_match_v1(side, typed->sources, typed->participant_count))
		return false;
	for (uint32 i = 0; i < plan->relmap_count; ++i) {
		const ColdRelmapInput *entry = &plan->relmap_inputs[i];
		if (entry->input.participant != input->participant)
			continue;
		if (!entry->captured || !rf_side_online_plan_relmap_input_matches_v1(side, &entry->input))
			return false;
		++expected;
	}
	return expected != 0 && expected == rf_side_online_plan_relmap_count_v1(side, &input->source)
		&& (reader == NULL || rf_side_online_plan_relmap_matches_v1(side, &input->source, reader));
}

typedef enum RelmapPinUse {
	RELMAP_PIN_PREPARED,
	RELMAP_PIN_ARM,
	RELMAP_PIN_STAGED
} RelmapPinUse;

static bool
relmap_pin(const ClusterColdRelmapInputV1 *input, RelmapPinUse use,
	const ClusterRelmapRetentionSnapshot *expected, ClusterRelmapRetentionSnapshot *capture)
{
	const ClusterRelmapV2Image *image = &input->staged;
	const ClusterRelmapV2Owner *owner = &image->pending_owner;
	ClusterRelmapRetentionKey key = {0};
	volatile bool locked = false;
	volatile bool ok = false;
	bool staged = use == RELMAP_PIN_STAGED;

	if (LWLockHeldByMe(input->retention_lock))
		return false;
	key.identity = image->identity;
	key.generation = image->pending_generation;
	key.origin = owner->node;
	key.top_full_xid = owner->top_full_xid;
	key.membership_epoch = owner->membership_epoch;
	key.admitted_incarnation = owner->admitted_incarnation;
	key.boot_incarnation = owner->boot_incarnation;
	PG_TRY();
	{
		LWLockAcquire(input->retention_lock, LW_EXCLUSIVE);
		locked = true;
		if (capture != NULL)
			ok = !staged && expected == NULL
				&& cluster_relmap_retention_prepare_capture(input->retention, &key, capture)
					== CLUSTER_RELMAP_RETENTION_OK;
		else if (expected != NULL)
			ok = cluster_relmap_retention_prepare_matches(input->retention, &key, expected,
				staged, staged ? owner->wal_end : 0) == CLUSTER_RELMAP_RETENTION_OK;
		else
			ok = (staged
				? cluster_relmap_retention_stage_check(input->retention, &key, owner->wal_end)
				: cluster_relmap_retention_prepare_check(input->retention, &key))
				== CLUSTER_RELMAP_RETENTION_OK;
		/* Preserve the exact original pin before any baseline I/O can
		 * expose it to cancellation. A partial batch or later failure keeps
		 * this conservative debt; no declaration cleanup may cancel it. */
		if (ok && use == RELMAP_PIN_ARM)
			ok = expected != NULL && capture == NULL
				&& cluster_relmap_retention_stage_start(input->retention, &key, owner->wal_end)
					== CLUSTER_RELMAP_RETENTION_OK;
		LWLockRelease(input->retention_lock);
		locked = false;
	}
	PG_CATCH();
	{
		HOLD_INTERRUPTS();
		if (locked)
			LWLockRelease(input->retention_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

static bool
relmap_entry_pin(const ClusterColdPlanV1 *plan, const ColdRelmapInput *entry, bool staged)
{
	if (plan->relmap_recovery != NULL)
		return cluster_relmap_recovery_check(plan->relmap_recovery, &entry->input.staged);
	/* Producer snapshots already armed the original pin before pass-1.
	 * Legacy explicit declarations retain their original stage transition. */
	return relmap_pin(&entry->input, staged || entry->has_retention_snapshot
		? RELMAP_PIN_STAGED : RELMAP_PIN_PREPARED,
		entry->has_retention_snapshot ? &entry->retention_snapshot : NULL, NULL);
}

static bool
relmap_record_matches(const ClusterColdRelmapInputV1 *input, XLogReaderState *reader)
{
	xl_relmap_update header;
	const ClusterColdRecordFrameV1 *frame = &input->frame;
	const DecodedXLogRecord *record;

	if (reader == NULL || reader->record == NULL)
		return false;
	record = reader->record;
	if (reader->ReadRecPtr != frame->read_rec_ptr || reader->EndRecPtr != frame->end_rec_ptr
		|| record->header.xl_crc != frame->record_crc || record->header.xl_rmid != frame->rmid
		|| record->header.xl_info != frame->info || record->header.xl_xid != frame->source_xid
		|| record->toplevel_xid != frame->source_top_xid || XLogRecHasAnyBlockRefs(reader)
		|| record->has_page_version_edge || XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| XLogRecGetData(reader) == NULL
		|| XLogRecGetDataLen(reader) != MinSizeOfRelmapUpdate + sizeof(RelMapFile))
		return false;
	memcpy(&header, XLogRecGetData(reader), MinSizeOfRelmapUpdate);
	return header.dbid == input->staged.identity.dbid
		&& header.tsid == (input->staged.identity.shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID)
		&& header.nbytes == sizeof(RelMapFile)
		&& memcmp(XLogRecGetData(reader) + MinSizeOfRelmapUpdate, &input->staged.pending,
			sizeof(RelMapFile)) == 0;
}

static bool
relmap_wal_input(const ClusterColdPlanV1 *plan, const ClusterWalSourceRef *sources,
	uint32 replay_count, const ClusterColdRelmapWalV1 *wal, ClusterColdRelmapInputV1 *input)
{
	XLogReaderState *reader = wal->reader;
	const DecodedXLogRecord *record;
	ClusterRelmapV2Image checked;
	unsigned char encoded[CLUSTER_RELMAP_V2_FILE_SIZE];

	if (wal->participant >= replay_count || wal->generation < 2
		|| wal->owner.wal_end != 0 || reader == NULL || reader->record == NULL)
		return false;
	record = reader->record;
	if (record->header.xl_rmid != RM_RELMAP_ID || record->header.xl_info != XLOG_RELMAP_UPDATE
		|| record->main_data == NULL
		|| record->main_data_len != MinSizeOfRelmapUpdate + sizeof(RelMapFile)
		|| reader->system_identifier != wal->identity.system_identifier
		|| reader->cluster_expected_thread_id != sources[wal->participant].claim.identity.origin_thread_id
		|| reader->seg.ws_tli != sources[wal->participant].timeline)
		return false;
	input->participant = wal->participant;
	input->cut = plan->participants[plan->canonical[wal->participant]].cut;
	input->source = sources[wal->participant];
	input->frame = (ClusterColdRecordFrameV1){reader->ReadRecPtr, reader->EndRecPtr,
		record->header.xl_crc, wal->participant, record->header.xl_xid,
		record->toplevel_xid, 0, 0, record->header.xl_rmid, record->header.xl_info, false};
	input->staged.identity = wal->identity;
	input->staged.committed_generation = wal->generation - 1;
	input->staged.pending_generation = wal->generation;
	input->staged.pending_owner = wal->owner;
	input->staged.pending_owner.wal_end = reader->EndRecPtr;
	memcpy(&input->staged.pending, record->main_data + MinSizeOfRelmapUpdate, sizeof(RelMapFile));
	input->retention = wal->retention;
	input->retention_lock = wal->retention_lock;
	/* Validate the WAL image before any physical baseline read. This local
	 * duplicate is only codec input; it never supplies the committed base. */
	checked = input->staged;
	checked.committed = checked.pending;
	return relmap_record_matches(input, reader)
		&& cluster_relmap_v2_encode(&checked, encoded, sizeof(encoded)) == CLUSTER_RELMAP_V2_VALID;
}

ClusterColdDetailV1
cluster_cold_plan_relmap_from_wal_v1(ClusterColdPlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 replay_count,
	const ClusterColdRelmapWalV1 *wal, uint32 count)
{
	ClusterColdRelmapInputV1 *inputs;
	ClusterRelmapRetentionSnapshot *snapshots;
	Size input_bytes, scratch, total;
	volatile ClusterColdDetailV1 detail = CLUSTER_COLD_STRUCTURAL_UNSUPPORTED;

	if (!plan_valid(plan) || sources == NULL || wal == NULL || count == 0
		|| count > CLUSTER_COLD_RELMAP_MAX_INPUTS || replay_count == 0
		|| replay_count > plan->participant_count)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	if (plan->phase != COLD_PHASE_FEEDING || plan->relmap_failed
		|| plan->input_frame_count != 0 || plan->relmap_inputs != NULL)
		return CLUSTER_COLD_STATE;
	/* No reentry once any original provider may be reached. The complete
	 * scratch AND copied declarations must fit the original plan budget. */
	plan->relmap_failed = true;
	plan->phase = COLD_PHASE_FAILED;
	input_bytes = MAXALIGN((Size)count * sizeof(*inputs));
	scratch = input_bytes + (Size)count * sizeof(*snapshots);
	total = scratch + (Size)count * sizeof(ColdRelmapInput);
	if (total > plan->memory_budget || plan->memory_used > plan->memory_budget - total)
		return CLUSTER_COLD_CAPACITY;
	inputs = cold_plan_scratch(plan, scratch, NULL);
	if (inputs == NULL)
		return CLUSTER_COLD_CAPACITY;
	snapshots = (ClusterRelmapRetentionSnapshot *)((char *)inputs + input_bytes);
	PG_TRY();
	{
		bool ok = enableFsync && cluster_relmap_authority_x_held();
		for (uint32 i = 0; i < count && ok; ++i)
			ok = relmap_wal_input(plan, sources, replay_count, &wal[i], &inputs[i]);
		if (ok)
			ok = cold_relmap_inputs_valid(plan, inputs, count) == CLUSTER_COLD_OK;
		/* Complete shape/source/namespace and all original pins first. */
		for (uint32 i = 0; i < count && ok; ++i)
			ok = relmap_pin(&inputs[i], RELMAP_PIN_PREPARED, NULL, &snapshots[i]);
		/* Every original pin was checked without mutation. Now arm the
		 * entire batch, under its original short locks, before the first
		 * file open/read. Never undo even a partially completed arm batch. */
		for (uint32 i = 0; i < count && ok; ++i)
			ok = relmap_pin(&inputs[i], RELMAP_PIN_ARM, &snapshots[i], NULL);
		for (uint32 i = 0; i < count && ok; ++i) {
			ClusterRelmapV2View base;
			unsigned char encoded[CLUSTER_RELMAP_V2_FILE_SIZE];
			ok = cluster_relmap_authority_x_held()
				&& relmap_pin(&inputs[i], RELMAP_PIN_STAGED, &snapshots[i], NULL)
				&& cluster_relmap_authority_v2_read(&inputs[i].staged.identity, &base)
					== CLUSTER_RELMAP_V2_PAIR_VALID
				&& base.pending_complete && !base.has_pending
				&& base.committed_generation == inputs[i].staged.committed_generation;
			if (ok) {
				inputs[i].staged.committed = base.committed;
				ok = cluster_relmap_v2_encode(&inputs[i].staged, encoded, sizeof(encoded))
					== CLUSTER_RELMAP_V2_VALID;
			}
		}
		/* A later read may invalidate an earlier pin or reuse its reader.
		 * Recheck the complete original batch before publishing any output. */
		for (uint32 i = 0; i < count && ok; ++i)
			ok = cluster_relmap_authority_x_held()
				&& relmap_pin(&inputs[i], RELMAP_PIN_STAGED, &snapshots[i], NULL)
				&& wal[i].reader != NULL
				&& wal[i].reader->system_identifier == inputs[i].staged.identity.system_identifier
				&& wal[i].reader->cluster_expected_thread_id == inputs[i].cut.thread_id
				&& wal[i].reader->seg.ws_tli == inputs[i].cut.timeline
				&& relmap_record_matches(&inputs[i], wal[i].reader);
		if (ok) {
			plan->phase = COLD_PHASE_FEEDING;
			detail = cluster_cold_plan_relmap_declare_v1(plan, inputs, count);
			if (detail == CLUSTER_COLD_OK)
				for (uint32 i = 0; i < count; ++i) {
					plan->relmap_inputs[i].retention_snapshot = snapshots[i];
					plan->relmap_inputs[i].has_retention_snapshot = true;
				}
		}
	}
	PG_CATCH();
	{
		plan->phase = COLD_PHASE_FAILED;
		cold_free(inputs);
		cold_plan_release(plan, scratch);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cold_free(inputs);
	cold_plan_release(plan, scratch);
	if (detail == CLUSTER_COLD_OK)
		plan->relmap_failed = false;
	else
		plan->phase = COLD_PHASE_FAILED;
	return detail;
}

ClusterColdDetailV1
cluster_cold_plan_relmap_from_recovery_wal_v1(ClusterColdPlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 source_count, uint32 replay_count,
	const ClusterColdRelmapWalV1 *wal, uint32 count)
{
	ClusterColdRelmapInputV1 *inputs;
	ClusterRelmapRecoveryPins *pins;
	Size bytes = (Size)count * sizeof(*inputs);
	volatile ClusterColdDetailV1 detail = CLUSTER_COLD_STRUCTURAL_UNSUPPORTED;
	if (!plan_valid(plan) || sources == NULL || wal == NULL || count == 0
		|| count > CLUSTER_COLD_RELMAP_MAX_INPUTS || source_count != plan->participant_count
		|| replay_count == 0 || replay_count > source_count)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	if (plan->phase != COLD_PHASE_FEEDING || plan->relmap_failed
		|| plan->input_frame_count != 0 || plan->relmap_inputs != NULL || plan->relmap_recovery != NULL)
		return CLUSTER_COLD_STATE;
	plan->relmap_failed = true;
	plan->phase = COLD_PHASE_FAILED;
	/* All permanent custody, scratch and copied declarations must fit before
	 * any pin or file operation. The carrier stays attached on every failure. */
	if (sizeof(*pins) + bytes + (Size)count * sizeof(ColdRelmapInput) > plan->memory_budget
		|| plan->memory_used > plan->memory_budget - sizeof(*pins) - bytes
			- (Size)count * sizeof(ColdRelmapInput))
		return CLUSTER_COLD_CAPACITY;
	pins = cold_plan_scratch(plan, sizeof(*pins), NULL);
	if (pins == NULL) return CLUSTER_COLD_CAPACITY;
	plan->relmap_recovery = pins;
	pins->state = CLUSTER_RELMAP_RETENTION_BUILDING;
	pins->expected = count;
	inputs = cold_plan_scratch(plan, bytes, NULL);
	if (inputs == NULL) return CLUSTER_COLD_CAPACITY;
	PG_TRY();
	{
		bool ok = enableFsync && cluster_relmap_authority_x_held();
		for (uint32 i = 0; i < source_count && ok; ++i) {
			const ClusterColdParticipantV1 *cut = &plan->participants[plan->canonical[i]].cut;
			ok = sources[i].claim.identity.origin_thread_id == cut->thread_id
				&& sources[i].timeline == cut->timeline
				&& sources[i].claim.identity.origin_owner_incarnation == cut->owner_incarnation;
		}
		for (uint32 i = 0; i < count && ok; ++i)
			ok = wal[i].retention == NULL && wal[i].retention_lock == NULL
				&& relmap_wal_input(plan, sources, replay_count, &wal[i], &inputs[i]);
		if (ok) ok = cold_relmap_inputs_valid(plan, inputs, count) == CLUSTER_COLD_OK;
		/* Restore the ENTIRE original batch before the first baseline I/O.
		 * Partial or poisoned declarations never become staging authority. */
		for (uint32 i = 0; i < count && ok; ++i) {
			pins->images[i] = inputs[i].staged;
			++pins->count;
		}
		for (uint32 i = 0; i < count && ok; ++i) {
			ClusterRelmapV2View base;
			unsigned char encoded[CLUSTER_RELMAP_V2_FILE_SIZE];
			ok = cluster_relmap_authority_x_held()
				&& cluster_relmap_authority_v2_read(&inputs[i].staged.identity, &base)
					== CLUSTER_RELMAP_V2_PAIR_VALID
				&& base.pending_complete && !base.has_pending
				&& base.committed_generation == inputs[i].staged.committed_generation;
			if (ok) {
				inputs[i].staged.committed = base.committed;
				pins->images[i] = inputs[i].staged;
				ok = cluster_relmap_v2_encode(&inputs[i].staged, encoded, sizeof(encoded))
					== CLUSTER_RELMAP_V2_VALID;
			}
		}
		for (uint32 i = 0; i < count && ok; ++i)
			ok = cluster_relmap_authority_x_held()
				&& wal[i].reader->system_identifier == inputs[i].staged.identity.system_identifier
				&& wal[i].reader->cluster_expected_thread_id == inputs[i].cut.thread_id
				&& wal[i].reader->seg.ws_tli == inputs[i].cut.timeline
				&& relmap_record_matches(&inputs[i], wal[i].reader);
		if (ok) {
			plan->phase = COLD_PHASE_FEEDING;
			detail = cluster_cold_plan_relmap_declare_v1(plan, inputs, count);
			if (detail == CLUSTER_COLD_OK) pins->restored = true;
		}
	}
	PG_CATCH();
	{
		plan->phase = COLD_PHASE_FAILED;
		cold_free(inputs);
		cold_plan_release(plan, bytes);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cold_free(inputs);
	cold_plan_release(plan, bytes);
	if (detail == CLUSTER_COLD_OK) plan->relmap_failed = false;
	else plan->phase = COLD_PHASE_FAILED;
	return detail;
}

bool
cluster_cold_typed_relmap_recovery_transfer_v1(ClusterColdTypedV1 *typed,
	ClusterRelmapRetentionDirectory *const *targets, uint32 count, LWLock *publication_lock)
{
	ClusterColdPlanV1 *plan;
	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || !plan_valid(typed->plan)
		|| publication_lock == NULL || CritSectionCount == 0
		|| !LWLockHeldByMeInMode(publication_lock, LW_EXCLUSIVE))
		return false;
	plan = typed->plan;
	if (plan->phase != COLD_PHASE_SEALED || plan->relmap_failed || plan->relmap_recovery == NULL
		|| count == 0 || count != plan->relmap_count || count != plan->relmap_recovery->count
		|| typed->participant_count != plan->participant_count || plan->relmap_inputs == NULL)
		return false;
	for (uint32 i = 0; i < count; ++i) {
		const ColdRelmapInput *entry = &plan->relmap_inputs[i];
		uint32 p = entry->input.participant;
		if (!entry->captured || !entry->attempted || !entry->applied || p >= typed->replay_count
			|| !cold_relmap_cut_equal(&entry->input.cut, &typed->participants[p])
			|| memcmp(&entry->input.source, &typed->sources[p], sizeof(entry->input.source)) != 0
			|| !cluster_relmap_recovery_check(plan->relmap_recovery, &entry->input.staged))
			return false;
	}
	return cluster_relmap_recovery_transfer(plan->relmap_recovery, targets, count);
}

bool
cluster_cold_typed_relmap_record_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, const ClusterColdStepV1 *step, XLogReaderState *reader)
{
	ClusterColdPlanV1 *plan;
	ClusterColdStepV1 expected;
	const ColdRecord *record;
	ColdRelmapInput *entry;
	const ClusterColdRelmapInputV1 *input;
	ClusterRelmapV2View base;
	unsigned char encoded[CLUSTER_RELMAP_V2_FILE_SIZE];
	bool ok = false;

	if (typed == NULL || fence == NULL || step == NULL || !plan_valid(typed->plan))
		return false;
	plan = typed->plan;
	if (plan->relmap_failed || !cluster_cold_plan_step_v1(plan, step->step_index, &expected)
		|| expected.step_kind != CLUSTER_COLD_STEP_RELMAP || memcmp(step, &expected, sizeof(expected)) != 0)
		return false;
	record = cold_record(plan, plan->schedule[step->step_index]);
	if (record->first_component >= plan->relmap_count)
		return false;
	entry = &plan->relmap_inputs[record->first_component];
	input = &entry->input;
	if (entry->attempted && !entry->applied)
		return false;
	/* Fail sticky before any provider, pin or I/O can throw. Applied repeats
	 * only verify the old receipt and complete pair; they never enter stage. */
	entry->attempted = true;
	plan->relmap_failed = true;
	PG_TRY();
	{
		if (entry->captured && relmap_current(typed, fence, input)
			&& relmap_side_matches(typed, input, reader)
			&& relmap_record_matches(input, reader)
			&& cluster_relmap_v2_encode(&input->staged, encoded, sizeof(encoded)) == CLUSTER_RELMAP_V2_VALID
			&& relmap_entry_pin(plan, entry, entry->applied)) {
			if (entry->applied)
				ok = cluster_relmap_authority_v2_check_staged(&input->staged);
			else if (cluster_relmap_authority_v2_read(&input->staged.identity, &base)
					== CLUSTER_RELMAP_V2_PAIR_VALID && !base.has_pending
				&& base.committed_generation == input->staged.committed_generation
				&& memcmp(&base.committed, &input->staged.committed, sizeof(RelMapFile)) == 0
				&& relmap_current(typed, fence, input) && relmap_side_matches(typed, input, reader)
				&& relmap_entry_pin(plan, entry, false))
				ok = (plan->relmap_recovery != NULL
					? cluster_relmap_authority_v2_write_recovery_pending(&input->staged, plan->relmap_recovery)
					: cluster_relmap_authority_v2_write_pending(&input->staged.identity,
					input->staged.pending_generation, &input->staged.pending_owner,
					&input->staged.pending, input->retention, input->retention_lock))
					&& cluster_relmap_authority_v2_check_staged(&input->staged);
			ok = ok && relmap_current(typed, fence, input) && relmap_entry_pin(plan, entry, true)
				&& relmap_side_matches(typed, input, reader);
		}
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ok) {
		entry->applied = true;
		plan->relmap_failed = false;
	}
	return ok;
}

bool
cluster_cold_typed_relmap_ready_v1(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence)
{
	ClusterColdPlanV1 *plan;
	bool ok = true;

	if (typed == NULL || fence == NULL || !plan_valid(typed->plan))
		return false;
	plan = typed->plan;
	if (plan->phase != COLD_PHASE_SEALED || plan->relmap_failed)
		return false;
	for (uint32 i = 0; i < plan->relmap_count; ++i)
		if (!plan->relmap_inputs[i].captured || !plan->relmap_inputs[i].applied)
			return false;
	/* Querying an incomplete batch grants nothing but does not consume it.
	 * Once all receipts exist, even an ERROR during revalidation is sticky. */
	if (plan->relmap_count == 0)
		return true;
	plan->relmap_failed = true;
	for (uint32 i = 0; i < plan->relmap_count && ok; ++i) {
		const ColdRelmapInput *entry = &plan->relmap_inputs[i];
		const ClusterColdRelmapInputV1 *input = &entry->input;
		ok = relmap_current(typed, fence, input) && relmap_side_matches(typed, input, NULL)
			&& relmap_entry_pin(plan, entry, true)
			&& cluster_relmap_authority_v2_check_staged(&input->staged)
			&& relmap_current(typed, fence, input) && relmap_entry_pin(plan, entry, true)
			&& relmap_side_matches(typed, input, NULL);
	}
	if (ok)
		plan->relmap_failed = false;
	return ok;
}

/* The original plan owns these immutable saved values across EOR. Reading
 * them cannot reuse pass-2's expired replay window as current authority. */
uint32
cluster_cold_typed_relmap_replay_count_v1(const ClusterColdTypedV1 *typed)
{
	const ClusterColdPlanV1 *plan;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || !plan_valid(typed->plan))
		return UINT32_MAX;
	plan = typed->plan;
	if (plan->phase != COLD_PHASE_SEALED || plan->relmap_failed
		|| plan->relmap_count > CLUSTER_COLD_RELMAP_MAX_INPUTS)
		return UINT32_MAX;
	if (plan->relmap_count == 0)
		return plan->relmap_inputs == NULL ? 0 : UINT32_MAX;
	if (plan->relmap_inputs == NULL || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->participant_count != plan->participant_count)
		return UINT32_MAX;
	for (uint32 i = 0; i < plan->relmap_count; ++i) {
		const ColdRelmapInput *entry = &plan->relmap_inputs[i];
		const ClusterColdRelmapInputV1 *input = &entry->input;
		uint32 p = input->participant;

		if (!entry->captured || !entry->attempted || !entry->applied
			|| p >= typed->replay_count || input->frame.participant != p
			|| input->staged.identity.system_identifier != typed->system_identifier
			|| !cold_relmap_cut_equal(&input->cut, &typed->participants[p])
			|| memcmp(&input->source, &typed->sources[p], sizeof(input->source)) != 0)
			return UINT32_MAX;
	}
	return plan->relmap_count;
}

ClusterColdRelmapReplayReceiptV1
cluster_cold_typed_relmap_replay_input_v1(const ClusterColdTypedV1 *typed, uint32 index)
{
	ClusterColdRelmapReplayReceiptV1 value = {0};
	uint32 count = cluster_cold_typed_relmap_replay_count_v1(typed);

	if (count == UINT32_MAX || index >= count)
		return value;
	value.count = count;
	value.index = index;
	memcpy(&value.input, &typed->plan->relmap_inputs[index].input, sizeof(value.input));
	return value;
}

typedef struct RelmapReplayCheck {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdTypedV1 *typed;
	ClusterColdPlanV1 *plan;
	const ColdRelmapInput *original, *saved;
	uint32 count, replay_count, participant_count;
	uint64 system_identifier;
} RelmapReplayCheck;

static void
relmap_recheck_held_lock(LWLock *lock, LWLockMode mode, void *arg)
{
	(void)lock;
	(void)mode;
	*(bool *)arg = true;
}

static bool
relmap_recheck_current(const RelmapReplayCheck *check)
{
	const ClusterColdTypedV1 *typed = check->typed;
	const ClusterColdPlanV1 *plan = check->plan;

	/* Revalidate the opaque owner before dereferencing its borrowed inputs.
	 * The saved array also detects a provider changing a receipt already read. */
	if (!ClusterColdRecoveryRelmapCurrent(check->start)
		|| ClusterColdRecoveryTxInputs(check->start) != typed
		|| typed->refusal != CLUSTER_COLD_OK || typed->plan != plan || !plan_valid(plan)
		|| plan->phase != COLD_PHASE_SEALED || !plan->relmap_failed
		|| plan->relmap_inputs != check->original || plan->relmap_count != check->count
		|| typed->replay_count != check->replay_count
		|| typed->participant_count != check->participant_count
		|| plan->participant_count != check->participant_count
		|| typed->system_identifier != check->system_identifier
		|| memcmp(check->original, check->saved, (Size)check->count * sizeof(*check->saved)) != 0)
		return false;
	for (uint32 i = 0; i < check->count; ++i) {
		const ClusterColdRelmapInputV1 *input = &check->saved[i].input;
		uint32 p = input->participant;
		if (p >= check->replay_count || !cold_relmap_cut_equal(&input->cut, &typed->participants[p])
			|| memcmp(&input->source, &typed->sources[p], sizeof(input->source)) != 0)
			return false;
	}
	return true;
}

bool
cluster_cold_relmap_replay_recheck_v1(const struct ClusterRecoveryStartV1 *start)
{
	RelmapReplayCheck check = {0};
	ColdRelmapInput *volatile saved = NULL;
	ClusterColdPlanV1 *volatile cleanup_plan = NULL;
	volatile Size bytes = 0;
	volatile bool ok = false;
	bool held = false;

	if (start == NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return false;
	ForEachLWLockHeldByMe(relmap_recheck_held_lock, &held);
	if (held)
		return false;
	PG_TRY();
	{
		do {
			if (!ClusterColdRecoveryRelmapCurrent(start))
				break;
			check.start = start;
			check.typed = ClusterColdRecoveryTxInputs(start);
			check.count = cluster_cold_typed_relmap_replay_count_v1(check.typed);
			if (check.typed == NULL || !plan_valid(check.typed->plan))
				break;
			check.plan = cleanup_plan = check.typed->plan;
			/* The first Start provider is inside TRY too. Once original inputs
			 * are located, all incomplete batches and provider/I/O failures are
			 * sticky before any further external call or allocation. */
			check.plan->relmap_failed = true;
			if (check.count == 0 || check.count == UINT32_MAX)
				break;
			check.original = check.plan->relmap_inputs;
			check.replay_count = check.typed->replay_count;
			check.participant_count = check.typed->participant_count;
			check.system_identifier = check.typed->system_identifier;
			saved = cold_plan_scratch(check.plan, (Size)check.count * sizeof(*saved), NULL);
			if (saved == NULL)
				break;
			bytes = (Size)check.count * sizeof(*saved);
			memcpy(saved, check.original, bytes);
			check.saved = saved;
			ok = enableFsync && cluster_relmap_authority_x_held() && relmap_recheck_current(&check);
			/* Complete the original batch preflight before opening any file. */
			for (uint32 i = 0; i < check.count && ok; ++i) {
				const ColdRelmapInput *entry = &check.saved[i];
				ok = cold_relmap_inputs_valid(check.plan, &entry->input, 1) == CLUSTER_COLD_OK
					&& relmap_side_matches(check.typed, &entry->input, NULL)
					&& relmap_entry_pin(check.plan, entry, true);
			}
			for (uint32 i = 0; i < check.count && ok; ++i) {
				const ColdRelmapInput *entry = &check.saved[i];
				ok = relmap_recheck_current(&check) && relmap_entry_pin(check.plan, entry, true)
					&& cluster_relmap_authority_v2_check_staged(&entry->input.staged)
					&& relmap_recheck_current(&check) && relmap_entry_pin(check.plan, entry, true)
					&& relmap_side_matches(check.typed, &entry->input, NULL);
			}
			/* A later file/provider may invalidate an earlier pin. Recheck the
			 * entire original batch and owner before publishing this observation. */
			for (uint32 i = 0; i < check.count && ok; ++i)
				ok = relmap_entry_pin(check.plan, &check.saved[i], true)
					&& relmap_side_matches(check.typed, &check.saved[i].input, NULL);
			ok = ok && relmap_recheck_current(&check);
		} while (false);
	}
	PG_CATCH();
	{
		ClusterColdRecoveryRelmapFail(start);
		if (saved != NULL) {
			cold_free(saved);
			cold_plan_release(cleanup_plan, bytes);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (saved != NULL) {
		cold_free(saved);
		cold_plan_release(cleanup_plan, bytes);
	}
	if (ok)
		check.plan->relmap_failed = false;
	else
		ClusterColdRecoveryRelmapFail(start);
	return ok;
}
#endif
