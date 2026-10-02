/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan.c
 *	  Pure typed cold-crash replay plan (first, read-only pass).
 *
 *	  Every retained writer generation feeds its decoded records.  Only the
 *	  ordinary-page PageVersion components are kept; record payloads never
 *	  are.  seal() then:
 *
 *	    1. checks that every participant cut was fed completely;
 *	    2. groups components by exact page identity and links them into one
 *	       chain by expected-before == result equality (no numeric order);
 *	    3. places the observed DATA version on the chain and assigns SKIP or
 *	       APPLY per component, refusing history gaps, missing ancestors,
 *	       branches, cycles and incarnation mismatches;
 *	    4. orders the replayable records: per-participant LSN order plus the
 *	       chain predecessor of every APPLY component, choosing the smallest
 *	       (xl_scn, thread, owner incarnation, LSN) key among ready records.
 *
 *	  This file takes the input and serves the sealed steps; storage is in
 *	  cluster_cold_recovery_plan_store.c and seal() in
 *	  cluster_cold_recovery_plan_seal.c.  Nothing here reads WAL or DATA,
 *	  takes locks or grants authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_plan.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cold_recovery_plan_internal.h"

static int
identity_compare(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	int cmp;

#define COLD_CMP_FIELD(field_)                                                                     \
	do {                                                                                           \
		if (left->field_ != right->field_)                                                         \
			return left->field_ < right->field_ ? -1 : 1;                                          \
	} while (0)
	COLD_CMP_FIELD(system_identifier);
	cmp = memcmp(left->storage_uuid, right->storage_uuid, sizeof(left->storage_uuid));
	if (cmp != 0)
		return cmp;
	COLD_CMP_FIELD(locator.spcOid);
	COLD_CMP_FIELD(locator.dbOid);
	COLD_CMP_FIELD(locator.relNumber);
	COLD_CMP_FIELD(forknum);
	COLD_CMP_FIELD(blockno);
	return 0;
#undef COLD_CMP_FIELD
}

static bool
participant_cut_valid(const ClusterColdParticipantV1 *cut)
{
	return cut->thread_id != 0 && cut->reserved_zero == 0 && cut->timeline != 0
		   && cut->owner_incarnation != 0 && cut->physical_lower != InvalidXLogRecPtr
		   && cut->physical_lower <= cut->native_redo && cut->native_redo <= cut->tail_end;
}

static bool
participant_precedes(const ClusterColdParticipantV1 *left, const ClusterColdParticipantV1 *right)
{
	if (left->thread_id != right->thread_id)
		return left->thread_id < right->thread_id;
	return left->owner_incarnation < right->owner_incarnation;
}

ClusterColdDetailV1
cluster_cold_plan_create_v1(const ClusterColdParticipantV1 *participants, uint32 participant_count,
							Size memory_budget, ClusterColdPlanV1 **out_plan)
{
	ClusterColdPlanV1 *plan;
	Size fixed;
	uint32 i;

	if (out_plan == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	*out_plan = NULL;
	if (participants == NULL || participant_count == 0
		|| participant_count > CLUSTER_COLD_MAX_PARTICIPANTS)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	for (i = 0; i < participant_count; i++) {
		uint32 j;

		if (!participant_cut_valid(&participants[i]))
			return CLUSTER_COLD_PARTICIPANT_INVALID;
		for (j = 0; j < i; j++)
			if (participants[j].thread_id == participants[i].thread_id
				&& participants[j].owner_incarnation == participants[i].owner_incarnation)
				return CLUSTER_COLD_PARTICIPANT_INVALID;
	}
	fixed = sizeof(*plan) + (Size)participant_count * (sizeof(ColdParticipant) + sizeof(uint32));
	if (fixed > memory_budget)
		return CLUSTER_COLD_CAPACITY;
	plan = (ClusterColdPlanV1 *)cold_alloc0(sizeof(*plan));
	if (plan == NULL)
		return CLUSTER_COLD_OOM;
	plan->participants
		= (ColdParticipant *)cold_alloc0((Size)participant_count * sizeof(ColdParticipant));
	plan->canonical = (uint32 *)cold_alloc0((Size)participant_count * sizeof(uint32));
	if (plan->participants == NULL || plan->canonical == NULL) {
		if (plan->participants != NULL)
			cold_free(plan->participants);
		if (plan->canonical != NULL)
			cold_free(plan->canonical);
		cold_free(plan);
		return CLUSTER_COLD_OOM;
	}
	/* Canonical (thread, owner incarnation) order by stable insertion. */
	for (i = 0; i < participant_count; i++) {
		uint32 at = i;

		while (at > 0 && participant_precedes(&participants[i], &plan->participants[at - 1].cut)) {
			plan->participants[at] = plan->participants[at - 1];
			at--;
		}
		plan->participants[at].cut = participants[i];
		plan->participants[at].input_index = i;
	}
	for (i = 0; i < participant_count; i++)
		plan->canonical[plan->participants[i].input_index] = i;
	plan->magic = CLUSTER_COLD_PLAN_MAGIC;
	plan->phase = COLD_PHASE_FEEDING;
	plan->participant_count = participant_count;
	plan->memory_budget = memory_budget;
	plan->memory_used = fixed;
	*out_plan = plan;
	return CLUSTER_COLD_OK;
}

static bool
page_identity_valid(const RfPageIdentityV1 *page)
{
	return page->system_identifier != 0 && bytes_nonzero(page->storage_uuid, 16)
		   && page->locator.relNumber != InvalidRelFileNumber && page->blockno != InvalidBlockNumber
		   && page->reserved_zero == 0
		   && (page->forknum == MAIN_FORKNUM || page->forknum == VISIBILITYMAP_FORKNUM
			   || page->forknum == INIT_FORKNUM);
}

static bool
anchor_flags_valid(uint16 flags)
{
	return flags == (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE)
		   || flags == (RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE)
		   || flags
				  == (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_WILL_INIT
					  | RF_PAGE_EDGE_FULL_COVERAGE);
}

/* Same closed shape the WAL decoder enforces for an ordinary component. */
static bool
component_shape_valid(const ClusterColdComponentV1 *component)
{
	static const uint8 zero[16] = { 0 };

	if (component->page_class != RF_PAGE_CLASS_ORDINARY
		|| component->result_kind != RF_PAGE_STATE_PRESENT || component->block_id > XLR_MAX_BLOCK_ID
		|| !page_identity_valid(&component->page) || !version_present(&component->result)
		|| (component->edge_flags != 0 && !anchor_flags_valid(component->edge_flags)))
		return false;
	switch (component->before_kind) {
	case RF_PAGE_STATE_PRESENT:
		return version_present(&component->before)
			   && incarnation_equal(&component->before, &component->result)
			   && !version_equal(&component->before, &component->result);
	case RF_PAGE_STATE_UNFORMATTED:
		return component->before.mutation_token == 0
			   && incarnation_equal(&component->before, &component->result)
			   && component->edge_flags != 0;
	case RF_PAGE_STATE_ABSENT:
		return component->before.mutation_token == 0
			   && memcmp(component->before.segment_incarnation, zero, 16) == 0
			   && component->edge_flags != 0;
	default:
		return false;
	}
}

static ClusterColdDetailV1
components_validate(ClusterColdPlanV1 *plan, const ClusterColdRecordV1 *record)
{
	uint16 i;

	for (i = 0; i < record->component_count; i++) {
		const ClusterColdComponentV1 *component = &record->components[i];
		uint16 j;

		if (!component_shape_valid(component))
			return CLUSTER_COLD_COMPONENT_INVALID;
		for (j = 0; j < i; j++)
			if (record->components[j].block_id == component->block_id
				|| identity_compare(&record->components[j].page, &component->page) == 0)
				return CLUSTER_COLD_COMPONENT_INVALID;
		if (!plan->namespace_known) {
			plan->system_identifier = component->page.system_identifier;
			memcpy(plan->storage_uuid, component->page.storage_uuid, 16);
			plan->namespace_known = true;
		} else if (component->page.system_identifier != plan->system_identifier
				   || memcmp(component->page.storage_uuid, plan->storage_uuid, 16) != 0)
			return CLUSTER_COLD_COMPONENT_INVALID;
	}
	return CLUSTER_COLD_OK;
}

static ClusterColdDetailV1
record_cursor_check(const ColdParticipant *participant, const ClusterColdRecordV1 *record)
{
	const ClusterColdParticipantV1 *cut = &participant->cut;

	if (record->read_rec_ptr >= record->end_rec_ptr || record->read_rec_ptr < cut->physical_lower
		|| record->end_rec_ptr > cut->tail_end)
		return CLUSTER_COLD_SOURCE_GAP;
	/* Contiguity: each record names its predecessor (xl_prev), so a record
	 * skipped by the feeder cannot hide behind a monotonic LSN. */
	if (!participant->seen ? record->read_rec_ptr != cut->physical_lower
						   : record->read_rec_ptr < participant->last_end
								 || record->prev_rec_ptr != participant->last_read)
		return CLUSTER_COLD_SOURCE_GAP;
	/* History ends at the native redo start; nothing may span it. */
	if (record->read_rec_ptr < cut->native_redo && record->end_rec_ptr > cut->native_redo)
		return CLUSTER_COLD_SOURCE_GAP;
	return CLUSTER_COLD_OK;
}

static ClusterColdDetailV1
record_store(ClusterColdPlanV1 *plan, uint32 canonical, const ClusterColdRecordV1 *record)
{
	uint32 segments[CLUSTER_COLD_MAX_COMPONENTS];
	ColdRecord *stored;
	ClusterColdDetailV1 detail;
	uint16 i;

	if (plan->record_count == UINT32_MAX
		|| (uint64)plan->component_count + record->component_count > UINT32_MAX)
		return CLUSTER_COLD_CAPACITY;
	detail = cold_plan_chunk_reserve(plan, (void ***)&plan->record_chunks,
									 &plan->record_chunk_count, &plan->record_chunk_capacity,
									 (uint64)plan->record_count + 1, sizeof(ColdRecord));
	if (detail == CLUSTER_COLD_OK)
		detail = cold_plan_chunk_reserve(
			plan, (void ***)&plan->component_chunks, &plan->component_chunk_count,
			&plan->component_chunk_capacity,
			(uint64)plan->component_count + record->component_count, sizeof(ColdComponent));
	/* Intern every page before storing anything; the incarnation of the
	 * before-state equals the result's unless it is ABSENT (validated). */
	for (i = 0; i < record->component_count && detail == CLUSTER_COLD_OK; i++) {
		const ClusterColdComponentV1 *source = &record->components[i];
		ColdRelation relation;
		ColdSegment segment;

		memset(&relation, 0, sizeof(relation));
		relation.locator = source->page.locator;
		relation.forknum = source->page.forknum;
		memset(&segment, 0, sizeof(segment));
		detail = cold_plan_intern(plan, &plan->relations, &relation, sizeof(relation),
								  &segment.relation);
		memcpy(segment.incarnation, source->result.segment_incarnation, 16);
		if (detail == CLUSTER_COLD_OK)
			detail
				= cold_plan_intern(plan, &plan->segments, &segment, sizeof(segment), &segments[i]);
	}
	if (detail != CLUSTER_COLD_OK)
		return detail;
	stored = cold_record(plan, plan->record_count);
	memset(stored, 0, sizeof(*stored));
	stored->read_rec_ptr = record->read_rec_ptr;
	stored->end_rec_ptr = record->end_rec_ptr;
	stored->scn = record->scn;
	stored->record_crc = record->record_crc;
	stored->participant = (uint8)canonical;
	stored->first_component = plan->component_count;
	stored->component_count = (uint8)record->component_count;
	stored->rmid = record->rmid;
	stored->info = record->info;
	if (record->end_rec_ptr <= plan->participants[canonical].cut.native_redo)
		stored->flags |= COLD_RECORD_HISTORY;
	for (i = 0; i < record->component_count; i++) {
		const ClusterColdComponentV1 *source = &record->components[i];
		ColdComponent *component = cold_component(plan, plan->component_count + i);

		memset(component, 0, sizeof(*component));
		component->before_token = source->before.mutation_token;
		component->result_token = source->result.mutation_token;
		component->segment = segments[i];
		component->blockno = source->page.blockno;
		component->record = plan->record_count;
		component->link = CLUSTER_COLD_NO_INDEX;
		component->edge_flags = source->edge_flags;
		component->block_id = source->block_id;
		component->before_kind = source->before_kind;
	}
	plan->component_count += record->component_count;
	plan->record_count++;
	return CLUSTER_COLD_OK;
}

/* Shape of one SPACE effect; the SPACE owner validates the payload itself. */
static bool
space_op_valid(const ClusterColdSpaceOpV1 *op)
{
	static const uint8 zero[3] = { 0 };
	bool before = bytes_nonzero(op->before_incarnation, 16);
	bool result = bytes_nonzero(op->result_incarnation, 16);

	if (memcmp(op->reserved_zero, zero, sizeof(zero)) != 0 || op->payload == NULL
		|| op->payload_length == 0 || op->locator.relNumber == InvalidRelFileNumber)
		return false;
	switch (op->kind) {
	case CLUSTER_COLD_SPACE_CREATE:
	case CLUSTER_COLD_SPACE_ADVANCE:
		return !before && result;
	case CLUSTER_COLD_SPACE_TRUNCATE:
		return before && result && memcmp(op->before_incarnation, op->result_incarnation, 16) != 0;
	case CLUSTER_COLD_SPACE_DROP:
		return before && !result;
	default:
		return false;
	}
}

/* A CREATE, TRUNCATE or ADVANCE stands alone; only a commit or abort drops
 * several relations at once. */
static bool
space_ops_valid(const ClusterColdRecordV1 *record)
{
	uint32 i;

	for (i = 0; i < record->space_count; i++) {
		const ClusterColdSpaceOpV1 *op = &record->space_ops[i];

		if (!space_op_valid(op) || (record->space_count > 1 && op->kind != CLUSTER_COLD_SPACE_DROP))
			return false;
	}
	return true;
}

/* History keeps the structural effects, without payload. */
static bool
space_op_kept(const ClusterColdSpaceOpV1 *op, bool history)
{
	return !history || op->kind != CLUSTER_COLD_SPACE_ADVANCE;
}

static ClusterColdDetailV1
space_record_store(ClusterColdPlanV1 *plan, uint32 canonical, const ClusterColdRecordV1 *record)
{
	bool history = record->end_rec_ptr <= plan->participants[canonical].cut.native_redo;
	uint32 kept = 0;
	ColdRecord *stored;
	ClusterColdDetailV1 detail;
	uint32 i;

	for (i = 0; i < record->space_count; i++)
		kept += space_op_kept(&record->space_ops[i], history) ? 1 : 0;
	if (kept == 0)
		return CLUSTER_COLD_OK;
	if (plan->record_count == UINT32_MAX || (uint64)plan->space_count + kept > UINT32_MAX)
		return CLUSTER_COLD_CAPACITY;
	detail = cold_plan_chunk_reserve(plan, (void ***)&plan->record_chunks,
									 &plan->record_chunk_count, &plan->record_chunk_capacity,
									 (uint64)plan->record_count + 1, sizeof(ColdRecord));
	if (detail == CLUSTER_COLD_OK)
		detail = cold_plan_chunk_reserve(plan, (void ***)&plan->space_chunks,
										 &plan->space_chunk_count, &plan->space_chunk_capacity,
										 (uint64)plan->space_count + kept, sizeof(ColdSpaceOp));
	if (detail != CLUSTER_COLD_OK)
		return detail;
	stored = cold_record(plan, plan->record_count);
	memset(stored, 0, sizeof(*stored));
	stored->read_rec_ptr = record->read_rec_ptr;
	stored->end_rec_ptr = record->end_rec_ptr;
	stored->scn = record->scn;
	stored->record_crc = record->record_crc;
	stored->participant = (uint8)canonical;
	stored->first_component = plan->space_count;
	stored->rmid = record->rmid;
	stored->info = record->info;
	stored->flags = COLD_RECORD_SPACE | (history ? COLD_RECORD_HISTORY : 0);
	/* Only an ADVANCE waits for the end of replay. */
	if (!history && record->space_ops[0].kind != CLUSTER_COLD_SPACE_ADVANCE)
		stored->flags |= COLD_RECORD_SPACE_STEP;
	for (i = 0; i < record->space_count; i++) {
		const ClusterColdSpaceOpV1 *source = &record->space_ops[i];
		ColdSpaceOp *op;

		if (!space_op_kept(source, history))
			continue;
		op = cold_space_op(plan, plan->space_count);
		memset(op, 0, sizeof(*op));
		if (!history) {
			if (!cold_plan_reserve(plan, source->payload_length))
				return CLUSTER_COLD_CAPACITY;
			op->payload = cold_alloc0(source->payload_length);
			if (op->payload == NULL) {
				cold_plan_release(plan, source->payload_length);
				return CLUSTER_COLD_OOM;
			}
			memcpy(op->payload, source->payload, source->payload_length);
			op->payload_length = source->payload_length;
		}
		op->record = plan->record_count;
		op->locator = source->locator;
		op->nblocks = source->nblocks;
		op->kind = source->kind;
		memcpy(op->before, source->before_incarnation, 16);
		memcpy(op->result, source->result_incarnation, 16);
		op->relation = CLUSTER_COLD_NO_INDEX;
		op->input = CLUSTER_COLD_NO_INDEX;
		plan->space_count++;
	}
	plan->record_count++;
	return CLUSTER_COLD_OK;
}

ClusterColdDetailV1
cluster_cold_plan_feed_v1(ClusterColdPlanV1 *plan, uint32 participant,
						  const ClusterColdRecordV1 *record)
{
	ColdParticipant *owner;
	ClusterColdDetailV1 detail;

	if (!plan_valid(plan))
		return CLUSTER_COLD_INVALID_ARGUMENT;
	if (plan->phase != COLD_PHASE_FEEDING)
		return CLUSTER_COLD_STATE;
	if (record == NULL || participant >= plan->participant_count
		|| record->component_count > CLUSTER_COLD_MAX_COMPONENTS
		|| (record->component_count != 0 && record->components == NULL)
		|| (record->space_count != 0 && record->space_ops == NULL)
		|| (record->record_flags & ~CLUSTER_COLD_RECORD_KNOWN_FLAGS) != 0
		|| record->reserved_zero != 0)
		detail = CLUSTER_COLD_INVALID_ARGUMENT;
	else if (record->space_count != 0 && record->component_count != 0)
		detail = CLUSTER_COLD_COMPONENT_INVALID;
	else {
		owner = &plan->participants[plan->canonical[participant]];
		detail = record_cursor_check(owner, record);
		/* A durable lifecycle change before native redo is history; after
		 * it, its order against other generations needs its own owner. */
		if (detail == CLUSTER_COLD_OK
			&& ((record->record_flags & CLUSTER_COLD_RECORD_UNSUPPORTED) != 0
				|| ((record->record_flags & CLUSTER_COLD_RECORD_STRUCTURAL) != 0
					&& record->end_rec_ptr > owner->cut.native_redo)))
			detail = CLUSTER_COLD_STRUCTURAL_UNSUPPORTED;
		/* A side effect without a cold owner is never replayed as a no-op. */
		if (detail == CLUSTER_COLD_OK
			&& (record->record_flags & CLUSTER_COLD_RECORD_SIDE_UNOWNED) != 0
			&& record->end_rec_ptr > owner->cut.native_redo)
			detail = CLUSTER_COLD_SIDE_OWNER_MISSING;
		if (detail == CLUSTER_COLD_OK)
			detail = components_validate(plan, record);
		if (detail == CLUSTER_COLD_OK && !space_ops_valid(record))
			detail = CLUSTER_COLD_SPACE_INVALID;
		if (detail == CLUSTER_COLD_OK && record->space_count != 0)
			detail = space_record_store(plan, plan->canonical[participant], record);
		if (detail == CLUSTER_COLD_OK && record->component_count != 0)
			detail = record_store(plan, plan->canonical[participant], record);
		if (detail == CLUSTER_COLD_OK) {
			owner->seen = true;
			owner->last_read = record->read_rec_ptr;
			owner->last_end = record->end_rec_ptr;
			if (record->end_rec_ptr > owner->cut.native_redo)
				owner->replay_records++;
		}
	}
	if (detail != CLUSTER_COLD_OK)
		plan->phase = COLD_PHASE_FAILED;
	return detail;
}

/* Expected state of an applied component (see COLD_STATE_*). */
static void
component_expected(const ClusterColdPlanV1 *plan, const ColdComponent *component,
				   ClusterColdBlockStepV1 *block)
{
	const ColdComponent *source;
	uint8 kind;

	/* The block is applied while the SPACE identity carries this component's
	 * incarnation (a TRUNCATE rebases the predecessor's token into it). */
	if ((component->state & COLD_STATE_FIRST_APPLY) == 0) {
		block->expected_kind = CLUSTER_COLD_DATA_PRESENT;
		block->expected_before = cold_plan_component_before(plan, component);
		return;
	}
	if ((component->state & COLD_STATE_EXACT) == 0) {
		block->expected_kind = CLUSTER_COLD_DATA_INVALID;
		return;
	}
	kind = (component->state >> COLD_STATE_DATA_KIND_SHIFT) & COLD_STATE_DATA_KIND_MASK;
	/*
	 * A block DATA did not have yet can exist as a zero page by the time its
	 * anchor applies: redo of a later block extends the relation over it.
	 * The anchor replaces the whole page, so any content is expected.
	 */
	if (kind == CLUSTER_COLD_DATA_ABSENT) {
		Assert(component->verdict != CLUSTER_COLD_BLOCK_APPLY_DELTA);
		block->expected_kind = CLUSTER_COLD_DATA_INVALID;
		return;
	}
	source = cold_component(plan, component->link);
	block->expected_kind = kind;
	if (kind == CLUSTER_COLD_DATA_PRESENT || kind == CLUSTER_COLD_DATA_UNFORMATTED)
		memcpy(block->expected_before.segment_incarnation,
			   cold_segment(plan, component->segment)->incarnation,
			   sizeof(block->expected_before.segment_incarnation));
	if (kind == CLUSTER_COLD_DATA_PRESENT)
		block->expected_before.mutation_token = (component->state & COLD_STATE_DATA_BEFORE) != 0
													? source->before_token
													: source->result_token;
}

uint32
cluster_cold_plan_step_count_v1(const ClusterColdPlanV1 *plan)
{
	return plan_valid(plan) && plan->phase == COLD_PHASE_SEALED ? plan->schedule_count : 0;
}

bool
cluster_cold_plan_step_v1(const ClusterColdPlanV1 *plan, uint32 index, ClusterColdStepV1 *out)
{
	const ColdRecord *record;
	bool any_skip = false;
	bool any_apply = false;
	uint16 i;

	if (out == NULL || !plan_valid(plan) || plan->phase != COLD_PHASE_SEALED
		|| index >= plan->schedule_count)
		return false;
	record = cold_record(plan, plan->schedule[index]);
	memset(out, 0, sizeof(*out));
	out->step_index = index;
	out->participant = plan->participants[record->participant].input_index;
	out->read_rec_ptr = record->read_rec_ptr;
	out->end_rec_ptr = record->end_rec_ptr;
	out->record_crc = record->record_crc;
	out->rmid = record->rmid;
	out->info = record->info;
	if ((record->flags & COLD_RECORD_SPACE) != 0) {
		const ColdSpaceOp *op = cold_space_op(plan, record->first_component);

		out->step_kind = CLUSTER_COLD_STEP_SPACE;
		out->space_kind = op->kind;
		out->space_count = cold_record_space_count(plan, plan->schedule[index]);
		out->space_relation = op->relation;
		out->space_input = op->input;
		return true;
	}
	for (i = 0; i < record->component_count; i++) {
		const ColdComponent *component = cold_component(plan, record->first_component + i);
		ClusterColdBlockStepV1 *block = &out->blocks[component->block_id];

		/* Every component of a scheduled record has a verdict; anything else
		 * is a planner bug and must not reach replay as "no action". */
		if (component->verdict == CLUSTER_COLD_BLOCK_NONE) {
			memset(out, 0, sizeof(*out));
			return false;
		}
		block->verdict = component->verdict;
		block->result = cold_plan_component_result(plan, component);
		if (component->verdict == CLUSTER_COLD_BLOCK_SKIP)
			any_skip = true;
		else {
			component_expected(plan, component, block);
			any_apply = true;
		}
	}
	out->all_skip = any_skip && !any_apply;
	out->mixed = any_skip && any_apply;
	return true;
}

bool
cluster_cold_plan_step_space_v1(const ClusterColdPlanV1 *plan, uint32 index, uint32 effect,
								uint32 *relation, uint32 *input)
{
	const ColdRecord *record;
	const ColdSpaceOp *op;

	if (!plan_valid(plan) || plan->phase != COLD_PHASE_SEALED || relation == NULL || input == NULL
		|| index >= plan->schedule_count)
		return false;
	record = cold_record(plan, plan->schedule[index]);
	if ((record->flags & COLD_RECORD_SPACE) == 0
		|| effect >= cold_record_space_count(plan, plan->schedule[index]))
		return false;
	op = cold_space_op(plan, record->first_component + effect);
	*relation = op->relation;
	*input = op->input;
	return true;
}

ClusterColdDetailV1
cluster_cold_plan_set_space_check_v1(ClusterColdPlanV1 *plan, ClusterColdSpaceCheckV1 check,
									 void *arg)
{
	if (!plan_valid(plan) || check == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	if (plan->phase != COLD_PHASE_FEEDING)
		return CLUSTER_COLD_STATE;
	plan->space_check = check;
	plan->space_check_arg = arg;
	return CLUSTER_COLD_OK;
}

uint32
cluster_cold_plan_space_relation_count_v1(const ClusterColdPlanV1 *plan)
{
	return plan_valid(plan) && plan->phase == COLD_PHASE_SEALED ? plan->space_relation_count : 0;
}

bool
cluster_cold_plan_space_relation_v1(const ClusterColdPlanV1 *plan, uint32 index,
									RelFileLocator *locator, uint32 *input_count)
{
	uint32 first;

	if (!plan_valid(plan) || plan->phase != COLD_PHASE_SEALED || locator == NULL
		|| input_count == NULL || index >= plan->space_relation_count)
		return false;
	first = plan->space_relation_start[index];
	*locator = cold_space_op(plan, plan->space_inputs[first])->locator;
	*input_count = plan->space_relation_start[index + 1] - first;
	return true;
}

bool
cluster_cold_plan_space_input_v1(const ClusterColdPlanV1 *plan, uint32 relation, uint32 input,
								 ClusterColdSpaceInputV1 *out)
{
	const ColdSpaceOp *op;
	const ColdRecord *record;
	uint32 at;

	if (!plan_valid(plan) || plan->phase != COLD_PHASE_SEALED || out == NULL
		|| relation >= plan->space_relation_count)
		return false;
	at = plan->space_relation_start[relation] + input;
	if (input >= plan->space_relation_start[relation + 1] - plan->space_relation_start[relation])
		return false;
	op = cold_space_op(plan, plan->space_inputs[at]);
	record = cold_record(plan, op->record);
	memset(out, 0, sizeof(*out));
	out->kind = op->kind;
	out->participant = plan->participants[record->participant].input_index;
	out->read_rec_ptr = record->read_rec_ptr;
	out->end_rec_ptr = record->end_rec_ptr;
	out->payload = op->payload;
	out->payload_length = op->payload_length;
	return true;
}

uint32
cluster_cold_plan_participant_count_v1(const ClusterColdPlanV1 *plan)
{
	return plan_valid(plan) && plan->phase == COLD_PHASE_SEALED ? plan->participant_count : 0;
}

uint64
cluster_cold_plan_replay_record_count_v1(const ClusterColdPlanV1 *plan, uint32 participant)
{
	if (!plan_valid(plan) || participant >= plan->participant_count)
		return 0;
	return plan->participants[plan->canonical[participant]].replay_records;
}

void
cluster_cold_plan_destroy_v1(ClusterColdPlanV1 **plan_address)
{
	ClusterColdPlanV1 *plan;
	uint32 i;

	if (plan_address == NULL || *plan_address == NULL)
		return;
	plan = *plan_address;
	if (plan->magic != CLUSTER_COLD_PLAN_MAGIC)
		return;
	plan->magic = 0;
	if (plan->schedule != NULL)
		cold_free(plan->schedule);
	for (i = 0; i < plan->component_chunk_count; i++)
		cold_free(plan->component_chunks[i]);
	if (plan->component_chunks != NULL)
		cold_free(plan->component_chunks);
	for (i = 0; i < plan->space_count; i++)
		if (cold_space_op(plan, i)->payload != NULL)
			cold_free(cold_space_op(plan, i)->payload);
	for (i = 0; i < plan->space_chunk_count; i++)
		cold_free(plan->space_chunks[i]);
	if (plan->space_chunks != NULL)
		cold_free(plan->space_chunks);
	cold_plan_space_release(plan);
	if (plan->space_inputs != NULL)
		cold_free(plan->space_inputs);
	if (plan->space_relation_start != NULL)
		cold_free(plan->space_relation_start);
	for (i = 0; i < plan->record_chunk_count; i++)
		cold_free(plan->record_chunks[i]);
	if (plan->record_chunks != NULL)
		cold_free(plan->record_chunks);
	cold_plan_intern_free(&plan->relations);
	cold_plan_intern_free(&plan->segments);
	cold_free(plan->canonical);
	cold_free(plan->participants);
	cold_free(plan);
	*plan_address = NULL;
}

#endif /* USE_PGRAC_CLUSTER */
