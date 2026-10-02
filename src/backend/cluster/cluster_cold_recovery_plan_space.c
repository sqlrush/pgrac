/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_space.c
 *	  SPACE effects in the typed cold-crash replay plan.
 *
 *	  A relation's SPACE identity names its current segment incarnation.  A
 *	  CREATE starts an incarnation, a TRUNCATE ends one and starts the next
 *	  (blocks below the new size keep their versions, the rest are gone), a
 *	  DROP ends the last one, and an ADVANCE moves only the reservation.
 *	  Before a TRUNCATE or DROP is logged every buffer of the relation has
 *	  been written, so what an ended incarnation still holds is durable and
 *	  is never replayed, and changes to retired or dropped blocks are
 *	  irrelevant.
 *
 *	  This file checks the effects, derives those facts for every page
 *	  segment, answers the chain questions seal asks (which incarnation a
 *	  block was carried from, which incarnations a page header may name)
 *	  and lists the SPACE owner's inputs per relation.  The owner computes
 *	  and installs the SPACE state itself.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_plan_space.c
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
locator_compare(const RelFileLocator *left, const RelFileLocator *right)
{
	if (left->spcOid != right->spcOid)
		return left->spcOid < right->spcOid ? -1 : 1;
	if (left->dbOid != right->dbOid)
		return left->dbOid < right->dbOid ? -1 : 1;
	if (left->relNumber != right->relNumber)
		return left->relNumber < right->relNumber ? -1 : 1;
	return 0;
}

/* Key of a structural effect: the incarnation it creates or ends. */
static const uint8 *
op_key(const ColdSpaceOp *op, bool by_result)
{
	return by_result ? op->result : op->before;
}

static int
op_key_compare(const ColdSpaceOp *op, const RelFileLocator *locator, const uint8 *incarnation,
			   bool by_result)
{
	int cmp = locator_compare(&op->locator, locator);

	return cmp != 0 ? cmp : memcmp(op_key(op, by_result), incarnation, 16);
}

static int
creator_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	const ColdSpaceOp *b = cold_space_op(plan, *(const uint32 *)right);

	return op_key_compare(cold_space_op(plan, *(const uint32 *)left), &b->locator, b->result, true);
}

static int
ender_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	const ColdSpaceOp *b = cold_space_op(plan, *(const uint32 *)right);

	return op_key_compare(cold_space_op(plan, *(const uint32 *)left), &b->locator, b->before,
						  false);
}

/* Inputs of one relation in feed order: participant, then position. */
static int
input_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	uint32 l = *(const uint32 *)left;
	uint32 r = *(const uint32 *)right;
	const ColdSpaceOp *a = cold_space_op(plan, l);
	const ColdSpaceOp *b = cold_space_op(plan, r);
	const ColdRecord *ra = cold_record(plan, a->record);
	const ColdRecord *rb = cold_record(plan, b->record);
	int cmp = locator_compare(&a->locator, &b->locator);

	if (cmp != 0)
		return cmp;
	if (ra->participant != rb->participant)
		return ra->participant < rb->participant ? -1 : 1;
	if (ra->read_rec_ptr != rb->read_rec_ptr)
		return ra->read_rec_ptr < rb->read_rec_ptr ? -1 : 1;
	return l < r ? -1 : (l > r ? 1 : 0);
}

static const ColdSpaceOp *
space_find(const ClusterColdPlanV1 *plan, const RelFileLocator *locator, const uint8 *incarnation,
		   bool by_result)
{
	const uint32 *sorted = by_result ? plan->space_creators : plan->space_enders;
	uint32 low = 0;
	uint32 high = by_result ? plan->space_creator_count : plan->space_ender_count;

	while (low < high) {
		uint32 middle = low + (high - low) / 2;
		const ColdSpaceOp *probe = cold_space_op(plan, sorted[middle]);
		int cmp = op_key_compare(probe, locator, incarnation, by_result);

		if (cmp == 0)
			return probe;
		if (cmp < 0)
			low = middle + 1;
		else
			high = middle;
	}
	return NULL;
}

/* A TRUNCATE carries the blocks of the main fork below its size; the other
 * forks shrink by their own map rules, so seal infers their retirement. */
static bool
truncate_carries(const ColdSpaceOp *op, uint32 forknum, BlockNumber blockno)
{
	return op != NULL && op->kind == CLUSTER_COLD_SPACE_TRUNCATE
		   && (forknum != MAIN_FORKNUM || blockno < op->nblocks);
}

static void *
scratch_alloc(ClusterColdPlanV1 *plan, Size bytes, bool keep)
{
	return cold_plan_scratch(plan, bytes, keep ? NULL : &plan->space_scratch_bytes);
}

/*
 * Sort the structural effects.  An incarnation is created once and ended
 * once; anything else is a branch of the relation's history.
 */
static ClusterColdDetailV1
space_structural(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag)
{
	uint32 i;

	plan->space_creators = scratch_alloc(plan, (Size)plan->space_count * sizeof(uint32), false);
	plan->space_enders = scratch_alloc(plan, (Size)plan->space_count * sizeof(uint32), false);
	if (plan->space_creators == NULL || plan->space_enders == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < plan->space_count; i++) {
		uint8 kind = cold_space_op(plan, i)->kind;

		if (kind == CLUSTER_COLD_SPACE_CREATE || kind == CLUSTER_COLD_SPACE_TRUNCATE)
			plan->space_creators[plan->space_creator_count++] = i;
		if (kind == CLUSTER_COLD_SPACE_TRUNCATE || kind == CLUSTER_COLD_SPACE_DROP)
			plan->space_enders[plan->space_ender_count++] = i;
	}
	qsort_arg(plan->space_creators, plan->space_creator_count, sizeof(uint32), creator_compare,
			  plan);
	qsort_arg(plan->space_enders, plan->space_ender_count, sizeof(uint32), ender_compare, plan);
	for (i = 1; i < plan->space_creator_count; i++)
		if (creator_compare(&plan->space_creators[i - 1], &plan->space_creators[i], plan) == 0) {
			cold_diag_record(plan, diag, cold_space_op(plan, plan->space_creators[i])->record);
			return CLUSTER_COLD_SPACE_INVALID;
		}
	for (i = 1; i < plan->space_ender_count; i++)
		if (ender_compare(&plan->space_enders[i - 1], &plan->space_enders[i], plan) == 0) {
			cold_diag_record(plan, diag, cold_space_op(plan, plan->space_enders[i])->record);
			return CLUSTER_COLD_SPACE_INVALID;
		}
	return CLUSTER_COLD_OK;
}

/*
 * An input after a native redo start whose resulting incarnation a
 * structural change in history already ended is covered by the SPACE pages
 * on disk: every change to an incarnation precedes its end.  It is neither
 * handed to the SPACE owner nor a step.
 */
static void
space_covered(ClusterColdPlanV1 *plan)
{
	uint32 i;

	for (i = 0; i < plan->space_count; i++) {
		ColdSpaceOp *op = cold_space_op(plan, i);
		const ColdSpaceOp *ender;

		if (op->payload == NULL || op->kind == CLUSTER_COLD_SPACE_DROP)
			continue;
		ender = space_find(plan, &op->locator, op->result, false);
		if (ender == NULL || !record_history(plan, ender->record))
			continue;
		op->covered = true;
		cold_record(plan, op->record)->flags &= ~COLD_RECORD_SPACE_STEP;
	}
}

/* Per segment: the record that must replay first, and how it ended. */
static ClusterColdDetailV1
space_segments(ClusterColdPlanV1 *plan)
{
	uint32 i;

	plan->segment_space
		= scratch_alloc(plan, (Size)Max(plan->segments.count, 1) * sizeof(ColdSegmentSpace), false);
	if (plan->segment_space == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < plan->segments.count; i++) {
		const ColdSegment *segment = cold_segment(plan, i);
		const ColdRelation *relation = cold_relation(plan, segment->relation);
		ColdSegmentSpace *facts = &plan->segment_space[i];
		const ColdSpaceOp *creator
			= space_find(plan, &relation->locator, segment->incarnation, true);
		const ColdSpaceOp *ender
			= space_find(plan, &relation->locator, segment->incarnation, false);

		facts->creator_record
			= creator != NULL
					  && (cold_record(plan, creator->record)->flags & COLD_RECORD_SPACE_STEP) != 0
				  ? creator->record
				  : CLUSTER_COLD_NO_INDEX;
		facts->retired_from = InvalidBlockNumber;
		if (ender == NULL)
			continue;
		facts->ended = true;
		if (ender->kind == CLUSTER_COLD_SPACE_DROP)
			facts->retired_from = 0;
		else if (relation->forknum == MAIN_FORKNUM)
			facts->retired_from = ender->nblocks;
	}
	for (i = 0; i < plan->component_count; i++) {
		ColdComponent *component = cold_component(plan, i);
		const ColdSegmentSpace *facts = &plan->segment_space[component->segment];

		if (!facts->ended)
			continue;
		component->state |= component->blockno >= facts->retired_from ? COLD_STATE_IRRELEVANT
																	  : COLD_STATE_DURABLE;
	}
	return CLUSTER_COLD_OK;
}

/* The SPACE owner's view of one stored effect. */
static void
space_input_of(const ClusterColdPlanV1 *plan, const ColdSpaceOp *op, ClusterColdSpaceInputV1 *out)
{
	const ColdRecord *record = cold_record(plan, op->record);

	memset(out, 0, sizeof(*out));
	out->kind = op->kind;
	out->participant = plan->participants[record->participant].input_index;
	out->read_rec_ptr = record->read_rec_ptr;
	out->end_rec_ptr = record->end_rec_ptr;
	out->payload = op->payload;
	out->payload_length = op->payload_length;
}

/*
 * Have the SPACE owner check one relation's inputs (slice, in canonical
 * participant and LSN order) against its current SPACE pages, then keep
 * them in the owner's order.  scratch holds 3 * count words.
 */
static ClusterColdDetailV1
space_relation_check(ClusterColdPlanV1 *plan, uint32 *slice, uint32 count, uint32 *scratch,
					 ClusterColdSpaceInputV1 *inputs, ClusterColdDiagV1 *diag)
{
	uint32 *order = scratch;
	uint32 *seen = scratch + count;
	uint32 *ordered = scratch + 2 * (Size)count;
	const ColdSpaceOp *first = cold_space_op(plan, slice[0]);
	uint32 i;

	for (i = 0; i < count; i++) {
		space_input_of(plan, cold_space_op(plan, slice[i]), &inputs[i]);
		seen[i] = 0;
	}
	if (!plan->space_check(plan->space_check_arg, &first->locator, inputs, count, order)) {
		cold_diag_record(plan, diag, first->record);
		return CLUSTER_COLD_SPACE_REFUSED;
	}
	for (i = 0; i < count; i++) {
		if (order[i] >= count || seen[order[i]]++ != 0) {
			cold_diag_record(plan, diag, first->record);
			return CLUSTER_COLD_SPACE_REFUSED;
		}
		ordered[i] = slice[order[i]];
	}
	memcpy(slice, ordered, (Size)count * sizeof(uint32));
	return CLUSTER_COLD_OK;
}

/*
 * Number the relation's inputs in the owner's order, and chain its SPACE
 * steps so they are scheduled in that order.
 */
static void
space_relation_number(ClusterColdPlanV1 *plan, uint32 relation, const uint32 *slice, uint32 count)
{
	uint32 previous_step = CLUSTER_COLD_NO_INDEX;
	uint32 i;

	for (i = 0; i < count; i++) {
		ColdSpaceOp *op = cold_space_op(plan, slice[i]);

		op->relation = relation;
		op->input = i;
		op->wait_record = previous_step != op->record ? previous_step : CLUSTER_COLD_NO_INDEX;
		if ((cold_record(plan, op->record)->flags & COLD_RECORD_SPACE_STEP) != 0)
			previous_step = op->record;
	}
}

/* The SPACE owner's inputs: every effect after a native redo start. */
static ClusterColdDetailV1
space_inputs(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag)
{
	ClusterColdSpaceInputV1 *inputs;
	ClusterColdDetailV1 detail = CLUSTER_COLD_OK;
	uint32 *scratch;
	uint32 start = 0;
	uint32 i;

	for (i = 0; i < plan->space_count; i++)
		if (cold_space_op(plan, i)->payload != NULL && !cold_space_op(plan, i)->covered)
			plan->space_input_count++;
	if (plan->space_input_count != 0 && plan->space_check == NULL)
		return CLUSTER_COLD_SIDE_OWNER_MISSING;
	plan->space_inputs = scratch_alloc(plan, (Size)plan->space_input_count * sizeof(uint32), true);
	plan->space_relation_start
		= scratch_alloc(plan, ((Size)plan->space_input_count + 1) * sizeof(uint32), true);
	scratch = scratch_alloc(plan, (Size)plan->space_input_count * 3 * sizeof(uint32), false);
	inputs = scratch_alloc(plan, (Size)plan->space_input_count * sizeof(*inputs), false);
	if (plan->space_inputs == NULL || plan->space_relation_start == NULL || scratch == NULL
		|| inputs == NULL)
		detail = CLUSTER_COLD_CAPACITY;
	if (detail == CLUSTER_COLD_OK) {
		plan->space_input_count = 0;
		for (i = 0; i < plan->space_count; i++)
			if (cold_space_op(plan, i)->payload != NULL && !cold_space_op(plan, i)->covered)
				plan->space_inputs[plan->space_input_count++] = i;
		qsort_arg(plan->space_inputs, plan->space_input_count, sizeof(uint32), input_compare, plan);
	}
	for (i = 1; detail == CLUSTER_COLD_OK && i <= plan->space_input_count; i++) {
		if (i < plan->space_input_count
			&& locator_compare(&cold_space_op(plan, plan->space_inputs[start])->locator,
							   &cold_space_op(plan, plan->space_inputs[i])->locator)
				   == 0)
			continue;
		detail = space_relation_check(plan, plan->space_inputs + start, i - start, scratch, inputs,
									  diag);
		if (detail != CLUSTER_COLD_OK)
			break;
		plan->space_relation_start[plan->space_relation_count] = start;
		space_relation_number(plan, plan->space_relation_count++, plan->space_inputs + start,
							  i - start);
		start = i;
	}
	if (detail == CLUSTER_COLD_OK)
		plan->space_relation_start[plan->space_relation_count] = plan->space_input_count;
	/* Released with the rest of the seal scratch. */
	if (scratch != NULL)
		cold_free(scratch);
	if (inputs != NULL)
		cold_free(inputs);
	return detail;
}

ClusterColdDetailV1
cold_plan_space_seal(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag)
{
	ClusterColdDetailV1 detail = space_structural(plan, diag);

	if (detail == CLUSTER_COLD_OK) {
		space_covered(plan);
		detail = space_segments(plan);
	}
	if (detail == CLUSTER_COLD_OK)
		detail = space_inputs(plan, diag);
	return detail;
}

void
cold_plan_space_release(ClusterColdPlanV1 *plan)
{
	void **arrays[] = { (void **)&plan->segment_space, (void **)&plan->space_creators,
						(void **)&plan->space_enders };
	Size i;

	for (i = 0; i < lengthof(arrays); i++)
		if (*arrays[i] != NULL) {
			cold_free(*arrays[i]);
			*arrays[i] = NULL;
		}
	plan->space_creator_count = 0;
	plan->space_ender_count = 0;
	cold_plan_release(plan, plan->space_scratch_bytes);
	plan->space_scratch_bytes = 0;
}

/* Walks stop after as many steps as there are structural effects. */
static uint32
walk_limit(const ClusterColdPlanV1 *plan)
{
	return plan->space_creator_count + 1;
}

/*
 * Nearest interned segment this block was carried from by TRUNCATEs, or
 * NO_INDEX.  Incarnations without changes to this fork are walked through.
 */
uint32
cold_plan_space_carried_from(const ClusterColdPlanV1 *plan, uint32 segment, BlockNumber blockno)
{
	const ColdSegment *start = cold_segment(plan, segment);
	const ColdRelation *relation = cold_relation(plan, start->relation);
	ColdSegment key;
	uint32 steps;

	memset(&key, 0, sizeof(key));
	key.relation = start->relation;
	memcpy(key.incarnation, start->incarnation, sizeof(key.incarnation));
	for (steps = 0; steps < walk_limit(plan); steps++) {
		const ColdSpaceOp *op = space_find(plan, &relation->locator, key.incarnation, true);
		uint32 found;

		if (!truncate_carries(op, relation->forknum, blockno))
			break;
		memcpy(key.incarnation, op->before, sizeof(key.incarnation));
		if (cold_plan_intern_find(&plan->segments, &key, sizeof(key), &found))
			return found;
	}
	return CLUSTER_COLD_NO_INDEX;
}

/* Walk from `from` along TRUNCATEs carrying this block; true on `target`. */
static bool
space_walk_reaches(const ClusterColdPlanV1 *plan, const ColdRelation *relation, BlockNumber blockno,
				   const uint8 *from, const uint8 *target, bool backward)
{
	uint8 current[16];
	uint32 steps;

	memcpy(current, from, sizeof(current));
	for (steps = 0; steps < walk_limit(plan); steps++) {
		const ColdSpaceOp *op = space_find(plan, &relation->locator, current, backward);

		if (!truncate_carries(op, relation->forknum, blockno))
			return false;
		memcpy(current, backward ? op->before : op->result, sizeof(current));
		if (memcmp(current, target, sizeof(current)) == 0)
			return true;
	}
	return false;
}

/*
 * Whether a page header naming `incarnation` may hold a version of this
 * component's block.  The SPACE identity read with the page can be older or
 * newer than the page itself, but only within the incarnations the block was
 * carried through.
 */
bool
cold_plan_space_lineage(const ClusterColdPlanV1 *plan, const ColdComponent *component,
						const uint8 *incarnation)
{
	const ColdSegment *segment = cold_segment(plan, component->segment);
	const ColdRelation *relation = cold_relation(plan, segment->relation);

	if (memcmp(segment->incarnation, incarnation, 16) == 0)
		return true;
	return space_walk_reaches(plan, relation, component->blockno, segment->incarnation, incarnation,
							  true)
		   || space_walk_reaches(plan, relation, component->blockno, segment->incarnation,
								 incarnation, false);
}

/*
 * A new page in an incarnation a TRUNCATE created, at a block that
 * TRUNCATE did not carry: whatever a header shows there is retired content.
 */
bool
cold_plan_space_retired_start(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	const ColdSegment *segment = cold_segment(plan, component->segment);
	const ColdRelation *relation = cold_relation(plan, segment->relation);
	const ColdSpaceOp *op;

	if (component->before_kind == RF_PAGE_STATE_PRESENT)
		return false;
	op = space_find(plan, &relation->locator, segment->incarnation, true);
	return op != NULL && op->kind == CLUSTER_COLD_SPACE_TRUNCATE
		   && (relation->forknum != MAIN_FORKNUM || component->blockno >= op->nblocks);
}

/*
 * Whether a CREATE after a native redo start made this lineage, so its
 * SPACE identity may not be on disk yet.
 */
bool
cold_plan_space_created_here(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	const ColdSegment *segment = cold_segment(plan, component->segment);
	const ColdRelation *relation = cold_relation(plan, segment->relation);
	uint8 current[16];
	uint32 steps;

	memcpy(current, segment->incarnation, sizeof(current));
	for (steps = 0; steps < walk_limit(plan); steps++) {
		const ColdSpaceOp *op = space_find(plan, &relation->locator, current, true);

		/* A structural effect in history is on disk. */
		if (op == NULL || record_history(plan, op->record))
			return false;
		if (op->kind == CLUSTER_COLD_SPACE_CREATE)
			return true;
		memcpy(current, op->before, sizeof(current));
	}
	return false;
}

/*
 * Outside the main fork a TRUNCATE's size is not known here.  When a page
 * starts new in an incarnation a TRUNCATE created, the truncation removed
 * that block: changes to it in the earlier incarnations are irrelevant.
 */
void
cold_plan_space_retire_inferred(ClusterColdPlanV1 *plan, const uint32 *group, uint32 count)
{
	const ColdRelation *relation;
	uint32 i;
	uint32 j;

	if (count == 0)
		return;
	relation = cold_relation(plan,
							 cold_segment(plan, cold_component(plan, group[0])->segment)->relation);
	if (relation->forknum == MAIN_FORKNUM)
		return;
	for (i = 0; i < count; i++) {
		const ColdComponent *fresh = cold_component(plan, group[i]);
		const uint8 *created = cold_segment(plan, fresh->segment)->incarnation;

		if ((fresh->state & COLD_STATE_IRRELEVANT) != 0
			|| !cold_plan_space_retired_start(plan, fresh))
			continue;
		for (j = 0; j < count; j++) {
			ColdComponent *older = cold_component(plan, group[j]);

			if (older->segment != fresh->segment
				&& space_walk_reaches(plan, relation, older->blockno, created,
									  cold_segment(plan, older->segment)->incarnation, true))
				older->state = (older->state & ~COLD_STATE_DURABLE) | COLD_STATE_IRRELEVANT;
		}
	}
}

#endif /* USE_PGRAC_CLUSTER */
