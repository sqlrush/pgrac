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
#include "cluster/cluster_relmap_terminal_internal.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_wal_writer.h"
#include <unistd.h>
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
/* Immutable evidence lives in the original typed plan, not in Start's output.
 * All FD custody is local; failure never retires the original pin carrier. */
typedef struct ColdRelmapTerminalEntry {
	ColdRelmapInput input;
	ClusterRelmapTerminalCustodyV1 native;
} ColdRelmapTerminalEntry;

typedef struct ColdRelmapTerminal {
	ClusterColdTypedV1 *typed;
	ClusterColdPlanV1 *plan;
	const ColdRelmapInput *original;
	ClusterRelmapRecoveryPins *pins;
	const struct ClusterRecoveryStartV1 *start;
	const struct ClusterRecoveryFencePlan *fence, *typed_fence;
	ClusterRecoveryCheckpointOwnerV1 owner;
	ClusterWalWriterToken writer;
	uint8 operation[16];
	uint64 install_generation, system_identifier;
	pid_t pid;
	uint32 count, participants, replay_count;
	bool failed, sealed;
	ClusterWalSourceRef sources[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterColdParticipantV1 cuts[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterControlRootSnapshot roots[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterWalRecoveryCheckpointV1 checkpoints[CLUSTER_COLD_MAX_PARTICIPANTS];
	const RfSideOnlinePlanV1 *sides[CLUSTER_COLD_MAX_PARTICIPANTS];
	ColdRelmapTerminalEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ColdRelmapTerminal;

static Size
relmap_terminal_bytes(uint32 count)
{
	return offsetof(ColdRelmapTerminal, entries) + (Size)count * sizeof(ColdRelmapTerminalEntry);
}

static void
relmap_terminal_release(void *value)
{
	ColdRelmapTerminal *receipt = value;
	ClusterColdPlanV1 *plan = receipt->plan;
	Size bytes = relmap_terminal_bytes(receipt->count);
	for (uint32 i = 0; i < receipt->count; ++i)
		cluster_clog_native_recovery_base_release_v1(&receipt->entries[i].native.held);
	cold_free(receipt);
	cold_plan_release(plan, bytes);
}

/* The original replay owner moves NULL to this exact fence after scope leave;
 * Start requires the completed transfer. No other fence is a valid successor.
 * Value identity only. No old cold-scope provider, terminal reader, CLOG I/O
 * or expired role permission is used by the Start path. */
static bool
relmap_terminal_inputs(const ColdRelmapTerminal *receipt)
{
	const ClusterColdTypedV1 *typed = receipt->typed;
	const ClusterColdPlanV1 *plan = receipt->plan;
	if (receipt->pid != getpid() || typed->refusal != CLUSTER_COLD_OK
		|| typed->plan != plan || !plan_valid(plan)
		|| (typed->tx_fence != receipt->typed_fence && typed->tx_fence != receipt->fence) || plan->phase != COLD_PHASE_SEALED
		|| plan->relmap_terminal != receipt || !plan->relmap_terminal_attempted
		|| plan->relmap_inputs != receipt->original || plan->relmap_count != receipt->count
		|| plan->relmap_recovery != receipt->pins
		|| typed->participant_count != receipt->participants
		|| plan->participant_count != receipt->participants
		|| typed->replay_count != receipt->replay_count
		|| typed->system_identifier != receipt->system_identifier)
		return false;
	for (uint32 p = 0; p < receipt->participants; ++p) {
		ClusterControlRootSnapshot root;
		ClusterWalRecoveryCheckpointV1 checkpoint;
		if (!cluster_cold_typed_checkpoint_v1(typed, p, &root, &checkpoint)
			|| memcmp(&root, &receipt->roots[p], sizeof(root)) != 0
			|| memcmp(&checkpoint, &receipt->checkpoints[p], sizeof(checkpoint)) != 0
			|| memcmp(&typed->sources[p], &receipt->sources[p], sizeof(receipt->sources[p])) != 0
			|| !cold_relmap_cut_equal(&typed->participants[p], &receipt->cuts[p])
			|| typed->side_plans[p] != receipt->sides[p])
			return false;
	}
	for (uint32 i = 0; i < receipt->count; ++i) {
		const ColdRelmapInput *entry = &receipt->entries[i].input;
		if (memcmp(&receipt->original[i], entry, sizeof(*entry)) != 0
			|| !relmap_entry_pin(plan, entry, true)
			|| !relmap_side_matches(typed, &entry->input, NULL))
			return false;
	}
	return true;
}

static bool
relmap_terminal_start(ColdRelmapTerminal *receipt,
	const struct ClusterRecoveryStartV1 *start, ClusterRecoveryCheckpointOwnerV1 *owner)
{
	ClusterWalWriterToken writer;
	uint8 operation[16];
	uint64 generation;
	if (MyBackendType != B_STARTUP || RecoveryInProgress() || CritSectionCount != 0
		|| !ClusterColdRecoveryRelmapCurrent(start)
		|| ClusterColdRecoveryTxInputs(start) != receipt->typed
		|| receipt->typed->tx_fence != receipt->fence
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, owner)
		|| owner->owner_pid != MyProcPid || owner->owner_boot == 0
		|| owner->start_generation == 0 || owner->start_serial == 0
		|| !relmap_terminal_inputs(receipt)
		|| !ClusterStartupInstalledRecoveryWriterV1(&writer, operation, &generation)
		|| generation == 0 || owner->install_generation != generation
		|| memcmp(&owner->writer, &writer, sizeof(writer)) != 0
		|| memcmp(owner->operation_uuid, operation, sizeof(operation)) != 0)
		return false;
	/* The installed writer exists only after native DONE. Bind it once via
	 * this original Start; capture never borrows that later role permission.
	 * Start serial may advance under the same original generation; Current
	 * and CheckpointOwner retain responsibility for that current serial. */
	if (receipt->start == NULL) {
		receipt->start = start;
		receipt->owner = *owner;
		receipt->writer = writer;
		memcpy(receipt->operation, operation, sizeof(operation));
		receipt->install_generation = generation;
	}
	return receipt->start == start && receipt->owner.owner_pid == owner->owner_pid
		&& receipt->owner.owner_boot == owner->owner_boot
		&& receipt->owner.start_generation == owner->start_generation
		&& receipt->install_generation == generation
		&& memcmp(&receipt->writer, &writer, sizeof(writer)) == 0
		&& memcmp(receipt->operation, operation, sizeof(operation)) == 0;
}

/* No owner is created here. Keep the exact original IR guard/request in
 * this call's stack until the native reader has moved its descriptor. */
typedef struct ColdRelmapTerminalEpisode {
	ColdRelmapTerminal *receipt;
	uint32 entry;
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterRecoverySerialGuard serial;
} ColdRelmapTerminalEpisode;

static bool
relmap_episode_permitted(void *arg, int origin)
{
	ColdRelmapTerminalEpisode *episode = arg;
	ColdRelmapTerminal *receipt = episode->receipt;
	const ClusterColdPlanV1 *plan = receipt->plan;
	return receipt->pid == getpid() && MyBackendType == B_STARTUP && RecoveryInProgress()
		&& CritSectionCount == 0 && !receipt->sealed && receipt->failed
		&& receipt->typed->refusal == CLUSTER_COLD_OK && receipt->typed->plan == plan
		&& plan->phase == COLD_PHASE_SEALED && plan->relmap_terminal == receipt
		&& plan->relmap_terminal_attempted && episode->entry < receipt->count
		&& plan->relmap_inputs == receipt->original && plan->relmap_count == receipt->count
		&& receipt->entries[episode->entry].input.input.source.claim.identity.origin_node_id == origin
		&& episode->authority.serial_guard != NULL
		&& memcmp(episode->authority.serial_guard, &episode->serial, sizeof(episode->serial)) == 0
		&& episode->serial.held && !episode->serial.release_uncertain
		&& episode->serial.lock_request.request_id != 0;
}

static bool
relmap_episode_current(void *arg, const ClusterWalInputV1 *selected)
{
	ColdRelmapTerminalEpisode *episode = arg;
	ColdRelmapTerminal *receipt = episode->receipt;
	const ClusterColdRelmapInputV1 *input = &receipt->entries[episode->entry].input.input;
	ClusterThreadRecoveryAuthorityV1 current;
	uint32 p = input->participant;
	if (!relmap_episode_permitted(arg, input->source.claim.identity.origin_node_id)
		|| !relmap_terminal_inputs(receipt) || !relmap_current(receipt->typed, (struct ClusterRecoveryFencePlan *)receipt->fence, input)
		|| !cluster_cold_typed_terminal_authority_v1(receipt->typed,
			(struct ClusterRecoveryFencePlan *)receipt->fence, p, &current)
		|| current.serial_guard != episode->authority.serial_guard
		|| memcmp(current.serial_guard, &episode->serial, sizeof(episode->serial)) != 0
		|| !rf_side_online_plan_recovery_single_source_v1(receipt->sides[p], &input->source)
		|| !rf_side_online_plan_replay_start_matches_v1(receipt->sides[p],
			input->source.claim.identity.origin_thread_id, input->source.claim.identity.origin_owner_incarnation,
			input->source.claim.database_incarnation, input->cut.native_redo))
		return false;
	return selected == NULL || (selected->kind == CLUSTER_WAL_INPUT_CHECKPOINT
		&& memcmp(&selected->source, &input->source, sizeof(input->source)) == 0
		&& memcmp(&selected->checkpoint, &receipt->roots[p], sizeof(selected->checkpoint)) == 0
		&& selected->checkpoint_start == receipt->checkpoints[p].checkpoint_start
		&& selected->native_redo == input->cut.native_redo
		&& memcmp(&selected->native_checkpoint, &receipt->checkpoints[p].checkpoint, sizeof(CheckPoint)) == 0
		&& memcmp(selected->native_window, receipt->checkpoints[p].native_window,
			CLUSTER_RELMAP_CHECKPOINT_WINDOW_SIZE) == 0);
}

static bool
relmap_episode_visit(void *arg, ClusterUndoRecoveryCheckpointVisitorV1 visitor, void *visitor_arg)
{
	ColdRelmapTerminalEpisode *episode = arg;
	ColdRelmapTerminal *receipt = episode->receipt;
	return relmap_episode_current(arg, NULL)
		&& cluster_cold_typed_terminal_visit_v1(receipt->typed,
			(struct ClusterRecoveryFencePlan *)receipt->fence,
			receipt->entries[episode->entry].input.input.participant, visitor, visitor_arg)
		&& relmap_episode_current(arg, NULL);
}

bool
cluster_cold_relmap_terminal_capture_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence)
{
	ClusterColdPlanV1 *plan;
	ColdRelmapTerminal *receipt;
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	uint32 count;
	bool held = false;
	volatile bool ok = false;

	if (typed == NULL || fence == NULL || !plan_valid(typed->plan))
		return false;
	plan = typed->plan;
	if (plan->relmap_terminal_attempted)
		return false;
	count = cluster_cold_typed_relmap_replay_count_v1(typed);
	/* Lock the attempt before any provider/allocation; partial capture never
	 * becomes an empty successful batch and cannot be retried. */
	plan->relmap_terminal_attempted = true;
	if (count == UINT32_MAX || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| (typed->tx_fence != NULL && typed->tx_fence != fence)
		|| MyBackendType != B_STARTUP || !RecoveryInProgress() || CritSectionCount != 0) {
		plan->relmap_failed = true;
		return false;
	}
	ForEachLWLockHeldByMe(relmap_recheck_held_lock, &held);
	if (held) { plan->relmap_failed = true; return false; }
	/* Declarations are unique namespaces; reject an inconsistent saved batch
	 * in full before its first native reader. */
	for (uint32 i = 0; i < count; ++i)
		for (uint32 j = 0; j < i; ++j) {
			const ClusterRelmapV2Namespace *a = &plan->relmap_inputs[i].input.staged.identity;
			const ClusterRelmapV2Namespace *b = &plan->relmap_inputs[j].input.staged.identity;
			if (a->system_identifier == b->system_identifier
				&& a->database_incarnation == b->database_incarnation
				&& a->dbid == b->dbid && a->shared == b->shared) {
				plan->relmap_failed = true; return false;
			}
		}
	receipt = cold_plan_scratch(plan, relmap_terminal_bytes(count), NULL);
	if (receipt == NULL) { plan->relmap_failed = true; return false; }
	receipt->plan = plan; receipt->typed = typed; receipt->pid = getpid();
	receipt->fence = fence; receipt->typed_fence = typed->tx_fence;
	receipt->count = count; receipt->participants = typed->participant_count;
	receipt->replay_count = typed->replay_count;
	receipt->system_identifier = typed->system_identifier;
	receipt->original = plan->relmap_inputs; receipt->pins = plan->relmap_recovery;
	receipt->failed = true;
	plan->relmap_terminal = receipt;
	plan->relmap_terminal_bytes = relmap_terminal_bytes(count);
	plan->relmap_terminal_release = relmap_terminal_release;
	for (uint32 p = 0; p < receipt->participants; ++p) {
		receipt->sources[p] = typed->sources[p];
		receipt->cuts[p] = typed->participants[p];
		receipt->sides[p] = typed->side_plans[p];
	}
	for (uint32 i = 0; i < count; ++i)
		receipt->entries[i].input = plan->relmap_inputs[i];
	PG_TRY();
	{
		ok = true;
		for (uint32 p = 0; p < receipt->participants && ok; ++p)
			ok = cluster_cold_typed_checkpoint_v1(typed, p, &receipt->roots[p], &receipt->checkpoints[p]);
		ok = ok && cluster_cold_typed_relmap_ready_v1(typed, fence);
		plan->relmap_failed = true;
		ok = ok && relmap_terminal_inputs(receipt);
		for (uint32 i = 0; i < count && ok; ++i) {
			ColdRelmapTerminalEntry *entry = &receipt->entries[i];
			const ClusterColdRelmapInputV1 *input = &entry->input.input;
			ClusterRelmapV2View view = {0};
			ClusterRelmapRecoveryTerminalV1 terminal;
			ColdRelmapTerminalEpisode episode = {0};
			ClusterRelmapTerminalEpisodeV1 adapter = {0};
			episode.receipt = receipt; episode.entry = i;
			if (!cluster_cold_typed_terminal_authority_v1(typed, fence, input->participant, &episode.authority)) {
				ok = false; break;
			}
			episode.serial = *episode.authority.serial_guard;
			adapter.arg = &episode; adapter.current = relmap_episode_current;
			adapter.permitted = relmap_episode_permitted; adapter.visit = relmap_episode_visit;
			adapter.side = typed->side_plans[input->participant];
			adapter.serial = episode.serial.lock_request.request_id;
			view.committed = view.pending_committed = input->staged.committed;
			view.committed_generation = view.pending_committed_generation = input->staged.committed_generation;
			view.pending = input->staged.pending;
			view.pending_generation = input->staged.pending_generation;
			view.pending_owner = input->staged.pending_owner;
			view.pending_complete = view.has_pending = true;
			ok = relmap_terminal_inputs(receipt)
				&& relmap_current(typed, fence, input)
				&& cluster_relmap_recovery_prepare_episode_v1(&input->staged.identity, &view, &adapter,
					(ClusterRelmapRecoveryHandle **)&handle)
				&& cluster_relmap_recovery_terminal_read(handle, &input->staged.identity, &view, &terminal)
				&& terminal.top_full_xid == input->staged.pending_owner.top_full_xid
				&& (terminal.status == TRANSACTION_STATUS_COMMITTED || terminal.status == TRANSACTION_STATUS_ABORTED)
				&& cluster_relmap_recovery_terminal_detach_v1(handle, &entry->native);
			cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
			ok = ok && memcmp(&entry->native.selected.source, &input->source, sizeof(input->source)) == 0
				&& memcmp(&entry->native.selected.checkpoint, &receipt->roots[input->participant],
					sizeof(entry->native.selected.checkpoint)) == 0
				&& entry->native.selected.checkpoint_start == receipt->checkpoints[input->participant].checkpoint_start
				&& memcmp(&entry->native.selected.native_checkpoint,
					&receipt->checkpoints[input->participant].checkpoint, sizeof(CheckPoint)) == 0
				&& memcmp(entry->native.selected.native_window,
					receipt->checkpoints[input->participant].native_window, CLUSTER_RELMAP_CHECKPOINT_WINDOW_SIZE) == 0
				&& entry->native.selected.native_redo == input->cut.native_redo
				&& entry->native.selected.checkpoint.validated_tail_lsn_exclusive == input->cut.tail_end
				&& relmap_current(typed, fence, input) && relmap_terminal_inputs(receipt);
		}
		for (uint32 i = 0; i < count && ok; ++i)
			ok = cluster_clog_native_terminal_current_v1(receipt->entries[i].native.held);
		ok = ok && relmap_terminal_inputs(receipt);
		if (ok) {
			receipt->sealed = true; receipt->failed = false;
			plan->relmap_failed = false;
		}
	}
	PG_CATCH();
	{
		plan->relmap_failed = true;
		cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_cold_relmap_terminal_recheck_v1(const struct ClusterRecoveryStartV1 *start,
	ClusterColdRelmapTerminalCheckV1 *out)
{
	const ClusterColdTypedV1 *typed;
	ColdRelmapTerminal *volatile receipt = NULL;
	ClusterRecoveryCheckpointOwnerV1 owner = {0};
	volatile bool ok = false;
	bool held = false;

	if (out == NULL || start == NULL
		|| !cluster_recovery_plan_output_distinct_v1(start, out, sizeof(*out)))
		return false;
	memset(out, 0, sizeof(*out));
	/* Locate only borrowed identity before touching output. Both initial
	 * providers are inside TRY: their ERROR still closes the original Start. */
	PG_TRY();
	{
		do {
			if (!ClusterColdRecoveryRelmapCurrent(start))
				break;
			typed = ClusterColdRecoveryTxInputs(start);
			if (typed == NULL || !plan_valid(typed->plan))
				break;
			receipt = typed->plan->relmap_terminal;
			if (receipt == NULL || receipt->failed || !receipt->sealed || typed->plan->relmap_failed)
				break;
			receipt->failed = true; typed->plan->relmap_failed = true;
			ForEachLWLockHeldByMe(relmap_recheck_held_lock, &held);
			if (held || MyBackendType != B_STARTUP || RecoveryInProgress() || CritSectionCount != 0)
				break;
			ok = relmap_terminal_start(receipt, start, &owner);
			for (uint32 i = 0; i < receipt->count && ok; ++i)
				ok = cluster_clog_native_terminal_current_v1(receipt->entries[i].native.held);
			ok = ok && relmap_terminal_start(receipt, start, &owner);
			for (uint32 i = 0; i < receipt->count && ok; ++i)
				ok = cluster_clog_native_terminal_current_v1(receipt->entries[i].native.held);
			if (!ok) break;
			/* No fallible work after this all-entry identity preflight. */
			for (uint32 i = 0; i < receipt->count; ++i) {
				const ColdRelmapTerminalEntry *entry = &receipt->entries[i];
				out->entries[i].participant = entry->input.input.participant;
				out->entries[i].identity = entry->input.input.staged.identity;
				out->entries[i].generation = entry->input.input.staged.pending_generation;
				out->entries[i].top_full_xid = entry->native.terminal.top_full_xid;
				out->entries[i].status = entry->native.terminal.status;
				out->entries[i].staged = entry->input.input.staged;
			}
			out->count = receipt->count;
			receipt->failed = false; typed->plan->relmap_failed = false;
		} while (false);
	}
	PG_CATCH();
	{
		ClusterColdRecoveryRelmapFail(start);
		/* No entered provider may leave a previous local result usable. */
		if (receipt != NULL) { receipt->failed = true; receipt->plan->relmap_failed = true; }
		memset(out, 0, sizeof(*out));
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ok) {
		memset(out, 0, sizeof(*out));
		ClusterColdRecoveryRelmapFail(start);
	}
	return ok;
}
#endif
