/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_replay.c
 *	  Pure pass-2 sequencer of typed cold-crash replay.
 *
 *	  Pass 2 replays exactly what pass 1 sealed.  For each scheduled record
 *	  (a page record or a SPACE step), in schedule order, its participant's
 *	  stream is read up to the record; every record before it has no
 *	  scheduled step and replays in stream order.  The scheduled record must
 *	  carry its pass-1 identity (start, end, CRC, rmgr, info). An exact
 *	  dependency may drain a source once its scheduled steps have run;
 *	  afterwards every remaining participant drains to its tail and must have
 *	  consumed exactly the records pass 1 counted after its native redo
 *	  start, and every SPACE relation is installed in full.
 *
 *	  The caller's callbacks read and apply; nothing here touches WAL,
 *	  buffers, locks or authority.  Any difference from pass 1 stops replay
 *	  with a typed detail naming the participant and record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_replay.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"

typedef struct ColdReplayRun {
	const ClusterColdPlanV1 *plan;
	const ClusterColdParticipantV1 *participants;
	uint32 own;
	const ClusterColdReplayOpsV1 *ops;
	void *arg;
	ClusterColdReplayResultV1 *result;
	uint32 input_frames;
	uint32 input_cursor[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint64 consumed[CLUSTER_COLD_MAX_PARTICIPANTS];
	XLogRecPtr last_end[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint32 scheduled_through[CLUSTER_COLD_MAX_PARTICIPANTS];
	bool draining[CLUSTER_COLD_MAX_PARTICIPANTS];
	bool drained[CLUSTER_COLD_MAX_PARTICIPANTS];
} ColdReplayRun;

/* This is the original dispatch's consumer set, not another WAL decoder.
 * The caller must decode the complete record again before invoking it.
 * A page/SPACE skip never discharges an unrelated native side effect. */
bool
cluster_cold_replay_effects_v1(const ClusterColdDecodedV1 *decoded, const ClusterColdStepV1 *step,
							   bool own)
{
	const ClusterColdRecordV1 *record;
	const ClusterColdEffectRoutesV1 *e;
	uint32 routes[4];
	uint32 seen = 0;
	uint32 no_effect_mask = CLUSTER_COLD_EFFECT_RECORD;
	const uint32 page_mask
		= CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_PAGE | CLUSTER_COLD_EFFECT_FSM;
	const uint32 space_mask = CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_SPACE;

	if (decoded == NULL)
		return false;
	record = &decoded->record;
	e = &decoded->effects;
	/* Only the native physical-redo no-op carries this logical message
	 * array. COMMIT and standby invalidations still require consumers. */
	if (record->record_flags == CLUSTER_COLD_RECORD_FOREIGN_CONTROL && record->rmid == RM_XACT_ID
		&& record->info == XLOG_XACT_INVALIDATIONS)
		no_effect_mask |= CLUSTER_COLD_EFFECT_INVALIDATIONS;
	if (!(e->required & CLUSTER_COLD_EFFECT_RECORD)
		|| (e->required & ~((CLUSTER_COLD_EFFECT_SCN << 1) - 1U)) != 0 || e->refused != 0
		|| (record->record_flags
			& ~(CLUSTER_COLD_RECORD_FOREIGN_CONTROL | CLUSTER_COLD_RECORD_FOREIGN_SIDE))
			   != 0
		|| record->component_count > CLUSTER_COLD_MAX_COMPONENTS
		|| (record->component_count != 0 && record->components == NULL)
		|| (record->space_count != 0 && record->space_ops == NULL)
		|| ((e->required & CLUSTER_COLD_EFFECT_PAGE) != 0) != (record->component_count != 0)
		|| ((e->required & CLUSTER_COLD_EFFECT_SPACE) != 0) != (record->space_count != 0)
		|| (e->native_apply
			& (CLUSTER_COLD_EFFECT_PAGE | CLUSTER_COLD_EFFECT_FSM | CLUSTER_COLD_EFFECT_SPACE))
			   != 0
		|| (e->page_apply & ~page_mask) != 0 || (e->space_apply & ~space_mask) != 0
		|| (e->no_effect & ~no_effect_mask) != 0)
		return false;
	routes[0] = e->native_apply;
	routes[1] = e->page_apply;
	routes[2] = e->space_apply;
	routes[3] = e->no_effect;
	for (uint32 i = 0; i < lengthof(routes); i++) {
		if ((seen & routes[i]) != 0)
			return false;
		seen |= routes[i];
	}
	if (seen != e->required)
		return false;
	if (step == NULL) {
		switch (cluster_cold_unscheduled_v1(record)) {
		case CLUSTER_COLD_UNSCHEDULED_NATIVE:
			/* The existing native callback also owns FSM-only page records.
			 * Other generations' native file creation has its original owner;
			 * their XACT/SLRU adapters are not wired by this check. */
			return e->page_apply == e->required
				   || (e->native_apply == e->required
					   && (own
						   || (record->rmid == RM_SMGR_ID
							   && (record->info & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE)));
		case CLUSTER_COLD_UNSCHEDULED_FOREIGN_NOOP:
			return !own && e->no_effect == e->required;
		case CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE:
			return !own && e->native_apply == e->required;
		case CLUSTER_COLD_UNSCHEDULED_SPACE_SKIP:
			return record->rmid == RM_SMGR_ID && record->space_count == 1
				   && e->space_apply == e->required
				   && record->space_ops[0].kind >= CLUSTER_COLD_SPACE_CREATE
				   && record->space_ops[0].kind <= CLUSTER_COLD_SPACE_ADVANCE;
		case CLUSTER_COLD_UNSCHEDULED_REFUSE:
			return false;
		}
		return false;
	}
	if ((record->record_flags != 0
		 && !(!own && record->record_flags == CLUSTER_COLD_RECORD_FOREIGN_SIDE))
		|| record->read_rec_ptr != step->read_rec_ptr || record->end_rec_ptr != step->end_rec_ptr
		|| record->record_crc != step->record_crc || record->rmid != step->rmid
		|| record->info != step->info)
		return false;
	if (step->step_kind == CLUSTER_COLD_STEP_PAGE)
		return record->record_flags == 0 && record->component_count > 0
			   && e->page_apply == e->required;
	if (step->step_kind != CLUSTER_COLD_STEP_SPACE || record->space_count == 0
		|| record->space_count != step->space_count || record->component_count != 0)
		return false;
	for (uint32 i = 0; i < record->space_count; i++)
		if (record->space_ops[i].kind != step->space_kind)
			return false;
	if (step->space_kind == CLUSTER_COLD_SPACE_DROP)
		/* SPACE installs every drop before the original native or exact
		 * foreign consumer executes the complete COMMIT effects. */
		return record->record_flags == (own ? 0 : CLUSTER_COLD_RECORD_FOREIGN_SIDE)
			   && record->rmid == RM_XACT_ID
			   && (record->info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT
			   && e->space_apply == CLUSTER_COLD_EFFECT_SPACE
			   && (e->native_apply & (CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL))
					  == (CLUSTER_COLD_EFFECT_RECORD | CLUSTER_COLD_EFFECT_TERMINAL)
			   && (e->native_apply | e->space_apply) == e->required;
	return record->record_flags == 0 && record->rmid == RM_SMGR_ID && record->space_count == 1
		   && step->space_kind >= CLUSTER_COLD_SPACE_CREATE
		   && step->space_kind <= CLUSTER_COLD_SPACE_ADVANCE && e->space_apply == e->required;
}

void
cluster_cold_effects_proof_init_v1(ClusterColdEffectsProofV1 *proof)
{
	if (proof != NULL)
		memset(proof, 0, sizeof(*proof));
}

bool
cluster_cold_effects_proof_record_v1(ClusterColdEffectsProofV1 *proof, uint32 participant,
										 const ClusterColdDecodedV1 *decoded,
										 const ClusterColdStepV1 *step, bool own)
{
	uint32 routed;

	if (proof == NULL || decoded == NULL || participant >= CLUSTER_COLD_MAX_PARTICIPANTS
		|| proof->invalid || !cluster_cold_replay_effects_v1(decoded, step, own)) {
		if (proof != NULL)
			proof->invalid = true;
		return false;
	}
	routed = decoded->effects.native_apply | decoded->effects.page_apply
			  | decoded->effects.space_apply | decoded->effects.no_effect;
	proof->records_seen[participant]++;
	proof->records_routed++;
	proof->required_union |= decoded->effects.required;
	proof->routed_union |= routed;
	return true;
}

bool
cluster_cold_effects_proof_finish_v1(const ClusterColdEffectsProofV1 *proof,
										 const ClusterColdPlanV1 *plan,
										 const ClusterColdReplayResultV1 *result,
										 uint32 participant_count)
{
	uint64 expected_records = 0;
	uint32 expected_steps;
	uint32 expected_space_steps = 0;
	uint32 i;

	if (proof == NULL || plan == NULL || result == NULL || proof->invalid || participant_count == 0
		|| participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| participant_count != cluster_cold_plan_participant_count_v1(plan)
		|| result->detail != CLUSTER_COLD_REPLAY_OK)
		return false;
	for (i = 0; i < participant_count; i++) {
		uint64 expected = cluster_cold_plan_replay_record_count_v1(plan, i);

		if (proof->records_seen[i] != expected)
			return false;
		expected_records += expected;
	}
	expected_steps = cluster_cold_plan_step_count_v1(plan);
	for (i = 0; i < expected_steps; i++) {
		ClusterColdStepV1 step;

		if (!cluster_cold_plan_step_v1(plan, i, &step))
			return false;
		if (step.step_kind == CLUSTER_COLD_STEP_SPACE)
			expected_space_steps++;
	}
	return proof->records_routed == expected_records
		   && proof->required_union == proof->routed_union
		   && result->steps_done == expected_steps
		   && result->space_steps == expected_space_steps
		   && result->space_relations_finished == cluster_cold_plan_space_relation_count_v1(plan);
}

bool
cluster_cold_typed_effects_complete_v1(ClusterColdTypedV1 *typed,
									  const ClusterColdEffectsProofV1 *proof,
									  const ClusterColdReplayResultV1 *result)
{
	ClusterColdEffectsReceiptV1 completed = { 0 };
	bool ready = false;

	if (typed == NULL)
		return false;
	if (typed->refusal == CLUSTER_COLD_OK
		&& cluster_cold_effects_proof_finish_v1(proof, typed->plan, result,
													typed->participant_count)) {
		completed.proof = *proof;
		completed.result = *result;
		completed.plan = typed->plan;
		ready = true;
	}
	typed->effects_receipt = completed;
	return ready;
}

bool
cluster_cold_typed_effects_ready_v1(const ClusterColdTypedV1 *typed)
{
	return typed != NULL && typed->refusal == CLUSTER_COLD_OK && typed->plan != NULL
		   && typed->effects_receipt.plan == typed->plan
		   && cluster_cold_effects_proof_finish_v1(&typed->effects_receipt.proof, typed->plan,
													 &typed->effects_receipt.result,
													 typed->participant_count);
}

bool
cluster_cold_typed_xid_sources_ready_v1(const ClusterColdTypedV1 *typed)
{
	const ClusterColdXidReceiptV1 *receipt;

	if (typed == NULL || typed->refusal != CLUSTER_COLD_OK || typed->plan == NULL
		|| typed->own_participant != 0 || typed->replay_count == 0
		|| typed->replay_count > typed->participant_count
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS)
		return false;
	receipt = &typed->xid_receipt;
	if (receipt->plan != typed->plan || receipt->participant_count != typed->participant_count
		|| receipt->replay_count != typed->replay_count
		|| receipt->system_identifier != typed->system_identifier
		|| cluster_cold_plan_participant_count_v1(typed->plan) != typed->participant_count)
		return false;
	for (uint32 p = 0; p < typed->participant_count; p++) {
		if (!cluster_cold_plan_participant_matches_v1(typed->plan, p, &typed->participants[p])
			|| memcmp(&receipt->sources[p], &typed->sources[p], sizeof(receipt->sources[p])) != 0)
			return false;
	}
	return true;
}

bool
cluster_cold_typed_xid_maximum_v1(const ClusterColdTypedV1 *typed, uint64 *next_xid)
{
	const ClusterColdXidReceiptV1 *receipt;
	uint64 maximum = 0;

	if (next_xid == NULL)
		return false;
	*next_xid = 0;
	if (!cluster_cold_typed_xid_sources_ready_v1(typed))
		return false;
	receipt = &typed->xid_receipt;
	for (uint32 p = 0; p < typed->participant_count; p++) {
		if (!TransactionIdIsNormal((TransactionId)receipt->next_xid[p]))
			return false;
		if (receipt->next_xid[p] > maximum)
			maximum = receipt->next_xid[p];
	}
	if (maximum != typed->xid_next_after_redo)
		return false;
	*next_xid = maximum;
	return true;
}

ClusterColdPageActionV1
cluster_cold_page_action_v1(const ClusterColdStepV1 *step, bool own)
{
	if (step != NULL && step->step_kind == CLUSTER_COLD_STEP_SPACE)
		return CLUSTER_COLD_SPACE_STEP;
	if (step == NULL || !step->all_skip)
		return CLUSTER_COLD_PAGE_APPLY;
	return own ? CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID : CLUSTER_COLD_PAGE_SKIP;
}

ClusterColdUnscheduledV1
cluster_cold_unscheduled_v1(const ClusterColdRecordV1 *record)
{
	uint32 i;

	if (record == NULL || record->component_count != 0
		|| (record->record_flags & ~CLUSTER_COLD_RECORD_KNOWN_FLAGS) != 0
		|| (record->record_flags
			& (CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_UNSUPPORTED
			   | CLUSTER_COLD_RECORD_SIDE_UNOWNED))
			   != 0)
		return CLUSTER_COLD_UNSCHEDULED_REFUSE;
	if ((record->record_flags & CLUSTER_COLD_RECORD_FOREIGN_SIDE) != 0)
		return record->record_flags == CLUSTER_COLD_RECORD_FOREIGN_SIDE && record->space_count == 0
				   ? CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE
				   : CLUSTER_COLD_UNSCHEDULED_REFUSE;
	if ((record->record_flags & CLUSTER_COLD_RECORD_FOREIGN_CONTROL) != 0)
		return record->space_count == 0 ? CLUSTER_COLD_UNSCHEDULED_FOREIGN_NOOP
										: CLUSTER_COLD_UNSCHEDULED_REFUSE;
	if (record->space_count == 0)
		return CLUSTER_COLD_UNSCHEDULED_NATIVE;
	for (i = 0; i < record->space_count; i++)
		if (record->space_ops[i].kind == CLUSTER_COLD_SPACE_DROP)
			return CLUSTER_COLD_UNSCHEDULED_REFUSE;
	return CLUSTER_COLD_UNSCHEDULED_SPACE_SKIP;
}

static ClusterColdReplayDetailV1
replay_stop(ColdReplayRun *run, ClusterColdReplayDetailV1 detail, uint32 participant,
			XLogRecPtr rec_ptr)
{
	run->result->detail = (uint8)detail;
	run->result->participant = participant;
	run->result->rec_ptr = rec_ptr;
	return detail;
}

static void
replay_consumed(ColdReplayRun *run, uint32 participant, const ClusterColdReplayRecordV1 *record)
{
	run->consumed[participant]++;
	run->last_end[participant] = record->end_rec_ptr;
	run->result->last_read[participant] = record->read_rec_ptr;
	run->result->last_end[participant] = record->end_rec_ptr;
	run->result->last_crc[participant] = record->record_crc;
	if (participant == run->own) {
		run->result->own_read = record->read_rec_ptr;
		run->result->own_end = record->end_rec_ptr;
	}
}

static bool
replay_identity_matches(const ClusterColdReplayRecordV1 *record, const ClusterColdStepV1 *step)
{
	return record->end_rec_ptr == step->end_rec_ptr && record->record_crc == step->record_crc
		   && record->rmid == step->rmid && record->info == step->info;
}

/* Bind every pass-2 record, including those without a planning step, to
 * the sealed input transcript before dependency or effect callbacks. Feed
 * order may interleave participants; each caller-index cursor advances
 * only forward and skips retained history. At most one full metadata scan
 * per participant is needed, without allocating another replay index. */
static bool
replay_input_matches(ColdReplayRun *run, uint32 participant,
					 const ClusterColdReplayRecordV1 *record)
{
	while (run->input_cursor[participant] < run->input_frames) {
		ClusterColdRecordFrameV1 frame;

		if (!cluster_cold_plan_input_frame_v1(run->plan, run->input_cursor[participant]++, &frame))
			return false;
		if (frame.participant != participant || frame.history)
			continue;
		return frame.read_rec_ptr == record->read_rec_ptr
			   && frame.end_rec_ptr == record->end_rec_ptr && frame.record_crc == record->record_crc
			   && frame.rmid == record->rmid && frame.info == record->info;
	}
	return false;
}

static ClusterColdReplayDetailV1 replay_drain(ColdReplayRun *run, uint32 participant);

/* The original owner resolves the captured record's exact dependency. A
 * target with a future scheduled step cannot be drained out of plan order. */
static ClusterColdReplayDetailV1
replay_unscheduled(ColdReplayRun *run, uint32 participant, const ClusterColdReplayRecordV1 *record)
{
	uint32 target = CLUSTER_COLD_NO_INDEX;
	ClusterColdReplayDetailV1 detail;

	if (!replay_input_matches(run, participant, record))
		return replay_stop(run, CLUSTER_COLD_REPLAY_IDENTITY, participant, record->read_rec_ptr);
	if (run->ops->record_dependency != NULL
		&& !run->ops->record_dependency(run->arg, participant, record, &target))
		return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, participant, record->read_rec_ptr);
	if (target != CLUSTER_COLD_NO_INDEX && target != participant) {
		if (target >= cluster_cold_plan_participant_count_v1(run->plan)
			|| run->ops->source_drained == NULL)
			return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, participant,
							   record->read_rec_ptr);
		detail = replay_drain(run, target);
		if (detail != CLUSTER_COLD_REPLAY_OK)
			return detail;
	}
	replay_consumed(run, participant, record);
	if (!run->ops->unscheduled(run->arg, participant))
		return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, participant, record->read_rec_ptr);
	return CLUSTER_COLD_REPLAY_OK;
}

/* Replay a participant's records up to the scheduled one and return it. */
static ClusterColdReplayDetailV1
replay_until(ColdReplayRun *run, uint32 participant, XLogRecPtr target,
			 ClusterColdReplayRecordV1 *found)
{
	for (;;) {
		ClusterColdReplayRecordV1 record;

		memset(&record, 0, sizeof(record));
		if (!run->ops->next(run->arg, participant, &record))
			return replay_stop(run, CLUSTER_COLD_REPLAY_SOURCE_ENDED, participant, target);
		if (record.read_rec_ptr == target) {
			*found = record;
			return CLUSTER_COLD_REPLAY_OK;
		}
		if (record.read_rec_ptr > target)
			return replay_stop(run, CLUSTER_COLD_REPLAY_TARGET_PASSED, participant, target);
		{
			ClusterColdReplayDetailV1 detail = replay_unscheduled(run, participant, &record);
			if (detail != CLUSTER_COLD_REPLAY_OK)
				return detail;
		}
	}
}

static ClusterColdReplayDetailV1
replay_step(ColdReplayRun *run, uint32 index)
{
	ClusterColdStepV1 step;
	ClusterColdReplayRecordV1 record;
	ClusterColdReplayDetailV1 detail;
	ClusterColdPageActionV1 action;

	if (!cluster_cold_plan_step_v1(run->plan, index, &step)
		|| step.participant >= cluster_cold_plan_participant_count_v1(run->plan))
		return replay_stop(run, CLUSTER_COLD_REPLAY_STEP_UNUSABLE, 0, InvalidXLogRecPtr);
	detail = replay_until(run, step.participant, step.read_rec_ptr, &record);
	if (detail != CLUSTER_COLD_REPLAY_OK)
		return detail;
	if (!replay_identity_matches(&record, &step)
		|| !replay_input_matches(run, step.participant, &record))
		return replay_stop(run, CLUSTER_COLD_REPLAY_IDENTITY, step.participant, step.read_rec_ptr);
	replay_consumed(run, step.participant, &record);
	action = cluster_cold_page_action_v1(&step, step.participant == run->own);
	if (action == CLUSTER_COLD_SPACE_STEP)
		run->result->space_steps++;
	else if (action == CLUSTER_COLD_PAGE_APPLY)
		run->result->pages_applied++;
	else
		run->result->pages_skipped++;
	if (!run->ops->scheduled(run->arg, step.participant, &step, action))
		return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, step.participant, step.read_rec_ptr);
	run->result->steps_done++;
	return CLUSTER_COLD_REPLAY_OK;
}

/* Replay the rest of one participant and prove its cut was consumed. */
static ClusterColdReplayDetailV1
replay_drain(ColdReplayRun *run, uint32 participant)
{
	const ClusterColdParticipantV1 *cut = &run->participants[participant];
	ClusterColdReplayRecordV1 record;

	if (run->drained[participant])
		return CLUSTER_COLD_REPLAY_OK;
	if (run->draining[participant] || run->scheduled_through[participant] > run->result->steps_done)
		return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, participant,
						   run->last_end[participant]);
	/* At most one active drain per participant: recursive dependencies are
	 * bounded by the existing source capacity, and cycles cannot consume a
	 * pending record or pretend that its source completed. */
	run->draining[participant] = true;
	memset(&record, 0, sizeof(record));
	while (run->ops->next(run->arg, participant, &record)) {
		if (record.end_rec_ptr > cut->tail_end)
			return replay_stop(run, CLUSTER_COLD_REPLAY_PAST_TAIL, participant,
							   record.read_rec_ptr);
		{
			ClusterColdReplayDetailV1 detail = replay_unscheduled(run, participant, &record);
			if (detail != CLUSTER_COLD_REPLAY_OK)
				return detail;
		}
		memset(&record, 0, sizeof(record));
	}
	if (run->last_end[participant] != cut->tail_end
		|| run->consumed[participant]
			   != cluster_cold_plan_replay_record_count_v1(run->plan, participant))
		return replay_stop(run, CLUSTER_COLD_REPLAY_CUT_DIFFERS, participant,
						   run->last_end[participant]);
	if (run->ops->source_drained != NULL
		&& !run->ops->source_drained(run->arg, participant, run->result,
									 run->consumed[participant]))
		return replay_stop(run, CLUSTER_COLD_REPLAY_CALLBACK, participant,
						   run->last_end[participant]);
	run->draining[participant] = false;
	run->drained[participant] = true;
	return CLUSTER_COLD_REPLAY_OK;
}

ClusterColdReplayDetailV1
cluster_cold_replay_run_v1(const ClusterColdPlanV1 *plan,
						   const ClusterColdParticipantV1 *participants, uint32 participant_count,
						   uint32 own, const ClusterColdReplayOpsV1 *ops, void *arg,
						   ClusterColdReplayResultV1 *result)
{
	ColdReplayRun run;
	ClusterColdReplayDetailV1 detail = CLUSTER_COLD_REPLAY_OK;
	uint32 steps;
	uint32 relations;
	uint32 i;
	uint32 drain_order[CLUSTER_COLD_MAX_PARTICIPANTS];
	bool seen[CLUSTER_COLD_MAX_PARTICIPANTS] = { 0 };

	if (result == NULL)
		return CLUSTER_COLD_REPLAY_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	memset(&run, 0, sizeof(run));
	run.result = result;
	/* Only a sealed plan reports its participants. */
	if (plan == NULL || participants == NULL || participant_count == 0
		|| participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| participant_count != cluster_cold_plan_participant_count_v1(plan)
		|| own >= participant_count || ops == NULL || ops->next == NULL || ops->unscheduled == NULL
		|| ops->scheduled == NULL
		|| !cluster_cold_plan_input_frame_count_v1(plan, &run.input_frames))
		return replay_stop(&run, CLUSTER_COLD_REPLAY_INVALID_ARGUMENT, 0, InvalidXLogRecPtr);
	relations = cluster_cold_plan_space_relation_count_v1(plan);
	if (relations != 0 && ops->space_final == NULL)
		return replay_stop(&run, CLUSTER_COLD_REPLAY_INVALID_ARGUMENT, 0, InvalidXLogRecPtr);
	run.plan = plan;
	run.participants = participants;
	run.own = own;
	run.ops = ops;
	run.arg = arg;
	for (i = 0; i < participant_count; i++) {
		/* Count equality cannot bind an empty/history-only source. Rebind
		 * every original cut before any owner callback can consume effects. */
		if (!cluster_cold_plan_participant_matches_v1(plan, i, &participants[i]))
			return replay_stop(&run, CLUSTER_COLD_REPLAY_CUT_DIFFERS, i,
							   participants[i].native_redo);
		run.last_end[i] = participants[i].native_redo;
		drain_order[i] = ops->source_order != NULL ? UINT32_MAX : i;
	}
	if (ops->source_order != NULL && !ops->source_order(arg, drain_order, participant_count))
		return replay_stop(&run, CLUSTER_COLD_REPLAY_CALLBACK, 0, InvalidXLogRecPtr);
	/* Reject partial/duplicate output before the first consumer. */
	for (i = 0; i < participant_count; i++) {
		if (drain_order[i] >= participant_count || seen[drain_order[i]])
			return replay_stop(&run, CLUSTER_COLD_REPLAY_CALLBACK, 0, InvalidXLogRecPtr);
		seen[drain_order[i]] = true;
	}
	steps = cluster_cold_plan_step_count_v1(plan);
	for (i = 0; i < steps; i++) {
		ClusterColdStepV1 step;
		if (!cluster_cold_plan_step_v1(plan, i, &step) || step.participant >= participant_count)
			return replay_stop(&run, CLUSTER_COLD_REPLAY_STEP_UNUSABLE, 0, InvalidXLogRecPtr);
		run.scheduled_through[step.participant] = i + 1;
	}
	for (i = 0; i < steps && detail == CLUSTER_COLD_REPLAY_OK; i++)
		detail = replay_step(&run, i);
	for (i = 0; i < participant_count && detail == CLUSTER_COLD_REPLAY_OK; i++)
		detail = replay_drain(&run, drain_order[i]);
	for (i = 0; i < relations && detail == CLUSTER_COLD_REPLAY_OK; i++) {
		if (!ops->space_final(arg, i))
			return replay_stop(&run, CLUSTER_COLD_REPLAY_CALLBACK, own, run.last_end[own]);
		result->space_relations_finished++;
	}
	return detail;
}

#endif /* USE_PGRAC_CLUSTER */
