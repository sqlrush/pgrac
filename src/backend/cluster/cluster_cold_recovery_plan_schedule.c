/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_schedule.c
 *	  Replay order of the typed cold-crash replay plan.
 *
 *	  Each participant's replayable records keep their stream order.  A
 *	  record is ready when every applied component's chain predecessor has
 *	  been scheduled, and the CREATE or TRUNCATE that made its incarnation
 *	  the relation's SPACE identity.  Among the ready stream heads the
 *	  smallest (xl_scn, thread, owner incarnation, LSN) goes next; SCN only
 *	  breaks ties.  No ready head is a cycle, refused before any change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_plan_schedule.c
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

/* Per-participant lists of replayable records, with a cursor each. */
typedef struct ColdScheduleWork {
	uint32 *lists;
	uint32 *list_start;
	uint32 *list_next;
	Size bytes;
} ColdScheduleWork;

static uint32 *
schedule_alloc(ClusterColdPlanV1 *plan, ColdScheduleWork *work, uint32 count)
{
	return (uint32 *)cold_plan_scratch(plan, (Size)count * sizeof(uint32), &work->bytes);
}

/* Record whose result an applied component's before-state is. */
static uint32
component_dependency(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	if (component->verdict < CLUSTER_COLD_BLOCK_APPLY_DELTA
		|| (component->state & COLD_STATE_FIRST_APPLY) != 0
		|| component->link == CLUSTER_COLD_NO_INDEX)
		return CLUSTER_COLD_NO_INDEX;
	return cold_component(plan, component->link)->record;
}

static bool
dependency_pending(const ClusterColdPlanV1 *plan, uint32 dependency, uint32 *blocking)
{
	if (dependency == CLUSTER_COLD_NO_INDEX
		|| (cold_record(plan, dependency)->flags & COLD_RECORD_SCHEDULED) != 0)
		return false;
	if (blocking != NULL)
		*blocking = dependency;
	return true;
}

/*
 * An applied component waits for its chain predecessor's record and for
 * the CREATE or TRUNCATE that made its incarnation the relation's SPACE
 * identity; a SPACE step waits for its relations' previous SPACE steps.
 */
static bool
record_ready(const ClusterColdPlanV1 *plan, uint32 index, uint32 *blocking)
{
	const ColdRecord *record = cold_record(plan, index);
	uint32 i;

	if ((record->flags & COLD_RECORD_SPACE) != 0) {
		uint32 count = cold_record_space_count(plan, index);

		for (i = 0; i < count; i++)
			if (dependency_pending(
					plan, cold_space_op(plan, record->first_component + i)->wait_record, blocking))
				return false;
		return true;
	}
	for (i = 0; i < record->component_count; i++) {
		const ColdComponent *component = cold_component(plan, record->first_component + i);

		if (dependency_pending(plan, component_dependency(plan, component), blocking)
			|| (component->verdict >= CLUSTER_COLD_BLOCK_APPLY_DELTA
				&& dependency_pending(plan, plan->segment_space[component->segment].creator_record,
									  blocking)))
			return false;
	}
	return true;
}

/* Scheduled: page records and SPACE steps (CREATE, TRUNCATE, drops). */
static bool
record_scheduled_kind(const ClusterColdPlanV1 *plan, uint32 record)
{
	const ColdRecord *stored = cold_record(plan, record);

	return !record_history(plan, record)
		   && ((stored->flags & COLD_RECORD_SPACE) == 0
			   || (stored->flags & COLD_RECORD_SPACE_STEP) != 0);
}

/* Deterministic candidate key: (xl_scn, thread, owner incarnation, LSN). */
static bool
record_key_less(const ColdRecord *left, const ColdRecord *right)
{
	if (left->scn != right->scn)
		return left->scn < right->scn;
	if (left->participant != right->participant)
		return left->participant < right->participant;
	return left->read_rec_ptr < right->read_rec_ptr;
}

/* Bucket replayable records by canonical participant, keeping feed order. */
static ClusterColdDetailV1
schedule_lists(ClusterColdPlanV1 *plan, ColdScheduleWork *work, uint32 *total)
{
	uint32 p = plan->participant_count;
	uint32 i;

	work->lists = schedule_alloc(plan, work, plan->record_count);
	work->list_start = schedule_alloc(plan, work, p + 1);
	work->list_next = schedule_alloc(plan, work, p);
	if (work->lists == NULL || work->list_start == NULL || work->list_next == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < plan->record_count; i++)
		if (record_scheduled_kind(plan, i))
			work->list_start[cold_record(plan, i)->participant + 1]++;
	for (i = 0; i < p; i++)
		work->list_start[i + 1] += work->list_start[i];
	*total = work->list_start[p];
	for (i = 0; i < p; i++)
		work->list_next[i] = work->list_start[i];
	for (i = 0; i < plan->record_count; i++)
		if (record_scheduled_kind(plan, i))
			work->lists[work->list_next[cold_record(plan, i)->participant]++] = i;
	for (i = 0; i < p; i++)
		work->list_next[i] = work->list_start[i];
	return CLUSTER_COLD_OK;
}

static void
schedule_deadlock(const ClusterColdPlanV1 *plan, const ColdScheduleWork *work,
				  ClusterColdDiagV1 *diag)
{
	uint32 best = CLUSTER_COLD_NO_INDEX;
	uint32 blocking = CLUSTER_COLD_NO_INDEX;
	uint32 p;

	for (p = 0; p < plan->participant_count; p++) {
		uint32 head;

		if (work->list_next[p] == work->list_start[p + 1])
			continue;
		head = work->lists[work->list_next[p]];
		if (best == CLUSTER_COLD_NO_INDEX
			|| record_key_less(cold_record(plan, head), cold_record(plan, best)))
			best = head;
	}
	if (best == CLUSTER_COLD_NO_INDEX)
		return;
	cold_diag_record(plan, diag, best);
	(void)record_ready(plan, best, &blocking);
	if (blocking != CLUSTER_COLD_NO_INDEX) {
		diag->has_dependency = true;
		diag->dependency_participant
			= plan->participants[cold_record(plan, blocking)->participant].input_index;
		diag->dependency_read_rec_ptr = cold_record(plan, blocking)->read_rec_ptr;
	}
}

static ClusterColdDetailV1
schedule_run(ClusterColdPlanV1 *plan, ColdScheduleWork *work, ClusterColdDiagV1 *diag)
{
	ClusterColdDetailV1 detail;
	uint32 total = 0;
	Size bytes;

	detail = schedule_lists(plan, work, &total);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	bytes = (Size)Max(total, 1) * sizeof(uint32);
	if (!cold_plan_reserve(plan, bytes))
		return CLUSTER_COLD_CAPACITY;
	plan->schedule = (uint32 *)cold_alloc0(bytes);
	if (plan->schedule == NULL) {
		cold_plan_release(plan, bytes);
		return CLUSTER_COLD_OOM;
	}
	while (plan->schedule_count < total) {
		uint32 best = CLUSTER_COLD_NO_INDEX;
		uint32 p;

		for (p = 0; p < plan->participant_count; p++) {
			uint32 head;

			if (work->list_next[p] == work->list_start[p + 1])
				continue;
			head = work->lists[work->list_next[p]];
			if (record_ready(plan, head, NULL)
				&& (best == CLUSTER_COLD_NO_INDEX
					|| record_key_less(cold_record(plan, head), cold_record(plan, best))))
				best = head;
		}
		if (best == CLUSTER_COLD_NO_INDEX) {
			schedule_deadlock(plan, work, diag);
			return CLUSTER_COLD_DEADLOCK;
		}
		cold_record(plan, best)->flags |= COLD_RECORD_SCHEDULED;
		work->list_next[cold_record(plan, best)->participant]++;
		plan->schedule[plan->schedule_count++] = best;
	}
	return CLUSTER_COLD_OK;
}

ClusterColdDetailV1
cold_plan_schedule(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag)
{
	ColdScheduleWork work;
	ClusterColdDetailV1 detail;
	uint32 **arrays[] = { &work.lists, &work.list_start, &work.list_next };
	Size i;

	memset(&work, 0, sizeof(work));
	detail = schedule_run(plan, &work, diag);
	for (i = 0; i < lengthof(arrays); i++)
		if (*arrays[i] != NULL)
			/* Function call through cold_free/pfree, not a shadow declaration. */
			// cppcheck-suppress shadowVariable
			cold_free(*arrays[i]);
	cold_plan_release(plan, work.bytes);
	return detail;
}

#endif /* USE_PGRAC_CLUSTER */
