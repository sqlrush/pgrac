/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_seal.c
 *	  Seal of the typed cold-crash replay plan.
 *
 *	  seal() checks that every participant cut was fed completely, links
 *	  each page's components into one chain by exact version equality,
 *	  places the observed DATA state on it, assigns SKIP or APPLY with the
 *	  expected state, and orders the replayable records by dependency with
 *	  a deterministic (xl_scn, thread, owner incarnation, LSN) tie-break.
 *	  Every refusal happens here, before the caller modifies anything.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_plan_seal.c
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

/* Scratch owned by one seal() call; every member is accounted. */
typedef struct ColdSealWork {
	uint32 *by_page;
	uint32 *by_result;
	uint32 *chain;
	uint32 *successor; /* per component, during chain linking */
	uint32 *lists;
	uint32 *list_start;
	uint32 *list_next;
	Size bytes;
} ColdSealWork;

/* Canonical page order: relation fork fields, then block. */
static int
page_compare(const ClusterColdPlanV1 *plan, const ColdComponent *left, const ColdComponent *right)
{
	uint32 lrel = cold_segment(plan, left->segment)->relation;
	uint32 rrel = cold_segment(plan, right->segment)->relation;

	if (lrel != rrel) {
		const ColdRelation *a = cold_relation(plan, lrel);
		const ColdRelation *b = cold_relation(plan, rrel);

#define COLD_CMP_FIELD(field_)                                                                     \
	do {                                                                                           \
		if (a->field_ != b->field_)                                                                \
			return a->field_ < b->field_ ? -1 : 1;                                                 \
	} while (0)
		COLD_CMP_FIELD(locator.spcOid);
		COLD_CMP_FIELD(locator.dbOid);
		COLD_CMP_FIELD(locator.relNumber);
		COLD_CMP_FIELD(forknum);
#undef COLD_CMP_FIELD
	}
	if (left->blockno != right->blockno)
		return left->blockno < right->blockno ? -1 : 1;
	return 0;
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

static void
diag_record(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 record)
{
	const ColdRecord *stored = cold_record(plan, record);

	diag->has_record = true;
	diag->participant = plan->participants[stored->participant].input_index;
	diag->read_rec_ptr = stored->read_rec_ptr;
}

static void
diag_component(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 component)
{
	const ColdComponent *stored = cold_component(plan, component);

	diag_record(plan, diag, stored->record);
	diag->has_page = true;
	diag->page = cold_plan_component_page(plan, stored);
}

static int
by_page_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	const ColdComponent *a = cold_component(plan, *(const uint32 *)left);
	const ColdComponent *b = cold_component(plan, *(const uint32 *)right);
	const ColdRecord *ra = cold_record(plan, a->record);
	const ColdRecord *rb = cold_record(plan, b->record);
	int cmp = page_compare(plan, a, b);

	if (cmp != 0)
		return cmp;
	if (ra->participant != rb->participant)
		return ra->participant < rb->participant ? -1 : 1;
	if (ra->read_rec_ptr != rb->read_rec_ptr)
		return ra->read_rec_ptr < rb->read_rec_ptr ? -1 : 1;
	return 0;
}

/* Within one page: by (segment incarnation, result token). */
static int
by_result_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	const ColdComponent *a = cold_component(plan, *(const uint32 *)left);
	const ColdComponent *b = cold_component(plan, *(const uint32 *)right);

	if (a->segment != b->segment)
		return a->segment < b->segment ? -1 : 1;
	if (a->result_token != b->result_token)
		return a->result_token < b->result_token ? -1 : 1;
	return 0;
}

static uint32
find_result(const ClusterColdPlanV1 *plan, const uint32 *by_result, uint32 count, uint32 segment,
			uint64 token)
{
	uint32 low = 0;
	uint32 high = count;

	while (low < high) {
		uint32 middle = low + (high - low) / 2;
		const ColdComponent *probe = cold_component(plan, by_result[middle]);

		if (probe->segment == segment && probe->result_token == token)
			return by_result[middle];
		if (probe->segment < segment || (probe->segment == segment && probe->result_token < token))
			low = middle + 1;
		else
			high = middle;
	}
	return CLUSTER_COLD_NO_INDEX;
}

/*
 * Link one page's components into a single chain by exact version equality.
 * chain receives the components from the chain start to its terminal.
 */
static ClusterColdDetailV1
page_chain_link(ClusterColdPlanV1 *plan, const uint32 *group, uint32 count, ColdSealWork *work,
				ClusterColdDiagV1 *diag)
{
	uint32 *by_result = work->by_result;
	uint32 *chain = work->chain;
	uint32 *successor = work->successor;
	uint32 start = CLUSTER_COLD_NO_INDEX;
	uint32 walked = 0;
	uint32 i;

	memcpy(by_result, group, (Size)count * sizeof(uint32));
	qsort_arg(by_result, count, sizeof(uint32), by_result_compare, plan);
	for (i = 1; i < count; i++)
		if (by_result_compare(&by_result[i - 1], &by_result[i], plan) == 0) {
			diag_component(plan, diag, by_result[i]);
			return CLUSTER_COLD_EDGE_CYCLE;
		}
	for (i = 0; i < count; i++)
		successor[group[i]] = CLUSTER_COLD_NO_INDEX;
	for (i = 0; i < count; i++) {
		ColdComponent *component = cold_component(plan, group[i]);
		uint32 predecessor = CLUSTER_COLD_NO_INDEX;

		if (component->before_kind == RF_PAGE_STATE_PRESENT)
			predecessor
				= find_result(plan, by_result, count, component->segment, component->before_token);
		component->link = predecessor;
		if (predecessor == CLUSTER_COLD_NO_INDEX) {
			if (start != CLUSTER_COLD_NO_INDEX) {
				const ColdComponent *first = cold_component(plan, start);
				bool same_start = first->before_kind == component->before_kind
								  && (component->before_kind == RF_PAGE_STATE_ABSENT
									  || (first->segment == component->segment
										  && first->before_token == component->before_token));

				/* Two starts from one version is a fork; otherwise an edge
				 * is missing or a second incarnation needs its SPACE owner. */
				diag_component(plan, diag, group[i]);
				return same_start ? CLUSTER_COLD_EDGE_BRANCH : CLUSTER_COLD_CHAIN_AMBIGUOUS;
			}
			start = group[i];
			continue;
		}
		if (successor[predecessor] != CLUSTER_COLD_NO_INDEX) {
			diag_component(plan, diag, group[i]);
			return CLUSTER_COLD_EDGE_BRANCH;
		}
		successor[predecessor] = group[i];
	}
	for (i = start; i != CLUSTER_COLD_NO_INDEX && walked < count; i = successor[i])
		chain[walked++] = i;
	if (start == CLUSTER_COLD_NO_INDEX || walked != count
		|| successor[chain[count - 1]] != CLUSTER_COLD_NO_INDEX) {
		diag_component(plan, diag, group[0]);
		return CLUSTER_COLD_EDGE_CYCLE;
	}
	return CLUSTER_COLD_OK;
}

static bool
data_shape_valid(const ClusterColdDataV1 *data)
{
	static const uint8 zero[6] = { 0 };

	if (memcmp(data->reserved_zero, zero, sizeof(zero)) != 0
		|| (data->flags & ~CLUSTER_COLD_DATA_KNOWN_FLAGS) != 0)
		return false;
	switch (data->kind) {
	case CLUSTER_COLD_DATA_INVALID:
		return data->flags == 0;
	case CLUSTER_COLD_DATA_ABSENT:
		return true;
	case CLUSTER_COLD_DATA_PRESENT:
		return version_present(&data->version);
	case CLUSTER_COLD_DATA_UNFORMATTED:
		return data->version.mutation_token == 0
			   && bytes_nonzero(data->version.segment_incarnation, 16);
	default:
		return false;
	}
}

/*
 * Earliest anchor (full image or full-coverage init) at chain index <= limit
 * whose whole chain suffix is replayable; -1 when none.  History is never
 * replayed, so only anchors after the last history edge qualify.
 */
static int64
page_earliest_replayable_anchor(const ClusterColdPlanV1 *plan, const uint32 *chain, uint32 count,
								int64 limit)
{
	uint32 start = 0;
	uint32 i;

	for (i = count; i > 0; i--)
		if (record_history(plan, cold_component(plan, chain[i - 1])->record)) {
			start = i;
			break;
		}
	for (i = start; i < count && (int64)i <= limit; i++)
		if (cold_component(plan, chain[i])->edge_flags != 0)
			return (int64)i;
	return -1;
}

/*
 * Position of a readable DATA state on the chain: the index of the
 * component whose result DATA holds, or -1 when DATA is the chain start
 * (the first expected-before, or the unformatted/absent start of a new
 * page).  Anything else is refused.
 */
static ClusterColdDetailV1
page_header_position(ClusterColdPlanV1 *plan, const uint32 *chain, uint32 count,
					 const ClusterColdDataV1 *data, int64 *position, ClusterColdDiagV1 *diag)
{
	const ColdComponent *first = cold_component(plan, chain[0]);
	bool new_start = first->before_kind != RF_PAGE_STATE_PRESENT;
	RfPageVersionV1 before;
	uint32 i;

	if (data->kind != CLUSTER_COLD_DATA_PRESENT) {
		Assert(new_start);
		*position = -1;
		if (data->kind == CLUSTER_COLD_DATA_UNFORMATTED
			&& !cold_plan_incarnation_is(plan, first, &data->version)) {
			diag_component(plan, diag, chain[0]);
			diag->version = data->version;
			return CLUSTER_COLD_INCARNATION_MISMATCH;
		}
		return CLUSTER_COLD_OK;
	}
	for (i = 0; i < count; i++) {
		const ColdComponent *component = cold_component(plan, chain[i]);

		if (component->result_token == data->version.mutation_token
			&& cold_plan_incarnation_is(plan, component, &data->version)) {
			*position = (int64)i;
			return CLUSTER_COLD_OK;
		}
	}
	before = cold_plan_component_before(plan, first);
	if (!new_start && version_equal(&before, &data->version)) {
		*position = -1;
		return CLUSTER_COLD_OK;
	}
	diag_component(plan, diag, chain[0]);
	diag->version = data->version;
	return cold_plan_incarnation_is(plan, first, &data->version)
			   ? CLUSTER_COLD_ANCESTOR_MISSING
			   : CLUSTER_COLD_INCARNATION_MISMATCH;
}

/* DATA cannot be a redo base: rebuild from the earliest replayable anchor. */
static ClusterColdDetailV1
page_rebuild_from_anchor(ClusterColdPlanV1 *plan, const uint32 *chain, uint32 count,
						 const ClusterColdDataV1 *data, ClusterColdDetailV1 missing,
						 uint32 diag_index, int64 *covered, ClusterColdDiagV1 *diag)
{
	int64 anchor = page_earliest_replayable_anchor(plan, chain, count, (int64)count);

	if (anchor < 0) {
		diag_component(plan, diag, chain[diag_index]);
		diag->version = data->version;
		return missing;
	}
	*covered = anchor - 1;
	return CLUSTER_COLD_OK;
}

/*
 * Place DATA on the chain.  *covered is the last chain index DATA already
 * contains (-1: none).  *exact means the page holds the DATA state exactly
 * before the first applied component; otherwise that component is an
 * anchor that replaces unreadable or unrelated content.
 *
 * The shared profile does not require checksums, so a torn write can pair
 * a newer header with an older body.  As with full_page_writes, the earliest
 * replayable anchor whose predecessor DATA has reached is always restored,
 * so no delta is applied to a body whose header alone placed it.
 */
static ClusterColdDetailV1
page_data_position(ClusterColdPlanV1 *plan, const uint32 *chain, uint32 count,
				   const ClusterColdDataV1 *data, int64 *covered, bool *exact, int64 *position,
				   ClusterColdDiagV1 *diag)
{
	bool new_start = cold_component(plan, chain[0])->before_kind != RF_PAGE_STATE_PRESENT;
	ClusterColdDetailV1 detail;
	int64 anchor;

	*exact = true;
	*position = -1;
	/* Unreadable, or a new page where a formatted one was expected. */
	if (data->kind == CLUSTER_COLD_DATA_INVALID
		|| (data->kind != CLUSTER_COLD_DATA_PRESENT && !new_start)) {
		*exact = false;
		return page_rebuild_from_anchor(plan, chain, count, data, CLUSTER_COLD_ANCHOR_MISSING,
										count - 1, covered, diag);
	}
	detail = page_header_position(plan, chain, count, data, position, diag);
	if (detail != CLUSTER_COLD_OK)
		return detail;

	/*
	 * Unless its content was verified, only the header placed DATA.  Any
	 * replayable change may have been in flight when the instances failed,
	 * and a torn write can leave that header over another version's body,
	 * so that content is never a redo base: as with full_page_writes, the
	 * earliest anchor after the last history change rebuilds the page and
	 * everything before it is skipped.  A single stream always has that
	 * anchor (its first change after the redo start logs a full image);
	 * across generations it can be missing (a change after another
	 * generation's history need not log one), and the page is refused.
	 */
	if ((data->flags & CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED) == 0)
		return page_rebuild_from_anchor(plan, chain, count, data, CLUSTER_COLD_CONTENT_UNPROVEN, 0,
										covered, diag);
	anchor = page_earliest_replayable_anchor(plan, chain, count, *position + 1);
	*covered = anchor >= 0 ? anchor - 1 : *position;
	return CLUSTER_COLD_OK;
}

static uint8
apply_verdict(uint16 edge_flags)
{
	if ((edge_flags & RF_PAGE_EDGE_FULL_IMAGE_APPLY) != 0)
		return CLUSTER_COLD_BLOCK_APPLY_IMAGE;
	if ((edge_flags & RF_PAGE_EDGE_WILL_INIT) != 0)
		return CLUSTER_COLD_BLOCK_APPLY_INIT;
	return CLUSTER_COLD_BLOCK_APPLY_DELTA;
}

static ClusterColdDetailV1
page_assign_verdicts(ClusterColdPlanV1 *plan, const uint32 *chain, uint32 count,
					 const ClusterColdDataV1 *data, int64 covered, bool exact, int64 position,
					 ClusterColdDiagV1 *diag)
{
	uint32 i;

	for (i = 0; i < count; i++) {
		ColdComponent *component = cold_component(plan, chain[i]);

		if ((int64)i <= covered) {
			component->verdict = CLUSTER_COLD_BLOCK_SKIP;
			continue;
		}
		if (record_history(plan, component->record)) {
			diag_component(plan, diag, chain[i]);
			diag->version = cold_plan_component_before(plan, component);
			return CLUSTER_COLD_HISTORY_GAP;
		}
		component->verdict = apply_verdict(component->edge_flags);
		/* Later components keep link = chain predecessor (chain[i - 1]). */
		if ((int64)i != covered + 1)
			continue;
		component->state = COLD_STATE_FIRST_APPLY;
		component->link = CLUSTER_COLD_NO_INDEX;
		if (exact) {
			component->state
				|= COLD_STATE_EXACT | (uint8)(data->kind << COLD_STATE_DATA_KIND_SHIFT);
			if (position >= 0)
				component->link = chain[position];
			else {
				component->link = chain[0];
				component->state |= COLD_STATE_DATA_BEFORE;
			}
		}
	}
	return CLUSTER_COLD_OK;
}

static ClusterColdDetailV1
page_group_resolve(ClusterColdPlanV1 *plan, const uint32 *group, uint32 count, ColdSealWork *work,
				   ClusterColdObserveV1 observe, void *arg, ClusterColdDiagV1 *diag)
{
	ClusterColdDataV1 data;
	ClusterColdDetailV1 detail;
	RfPageIdentityV1 page;
	bool replayable = false;
	bool exact;
	int64 covered;
	int64 position;
	uint32 i;

	for (i = 0; i < count && !replayable; i++)
		replayable = !record_history(plan, cold_component(plan, group[i])->record);
	if (!replayable)
		return CLUSTER_COLD_OK; /* completed history: no DATA duty */
	detail = page_chain_link(plan, group, count, work, diag);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	memset(&data, 0, sizeof(data));
	page = cold_plan_component_page(plan, cold_component(plan, group[0]));
	if (!observe(arg, &page, &data) || !data_shape_valid(&data)) {
		diag_component(plan, diag, work->chain[0]);
		return CLUSTER_COLD_OBSERVATION_FAILED;
	}
	detail = page_data_position(plan, work->chain, count, &data, &covered, &exact, &position, diag);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	return page_assign_verdicts(plan, work->chain, count, &data, covered, exact, position, diag);
}

static ClusterColdDetailV1
seal_inputs_complete(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag)
{
	uint32 i;

	for (i = 0; i < plan->participant_count; i++) {
		const ColdParticipant *participant = &plan->participants[i];
		bool empty = participant->cut.physical_lower == participant->cut.tail_end;

		if (participant->seen ? participant->last_end != participant->cut.tail_end : !empty) {
			diag->has_record = true;
			diag->participant = participant->input_index;
			diag->read_rec_ptr
				= participant->seen ? participant->last_end : participant->cut.physical_lower;
			return CLUSTER_COLD_SOURCE_GAP;
		}
	}
	return CLUSTER_COLD_OK;
}

static uint32 *
work_alloc(ClusterColdPlanV1 *plan, ColdSealWork *work, uint32 count)
{
	Size bytes = (Size)Max(count, 1) * sizeof(uint32);
	uint32 *array;

	if (!cold_plan_reserve(plan, bytes))
		return NULL;
	array = (uint32 *)cold_alloc0(bytes);
	if (array == NULL) {
		cold_plan_release(plan, bytes);
		return NULL;
	}
	work->bytes += bytes;
	return array;
}

static void
work_free(ClusterColdPlanV1 *plan, ColdSealWork *work)
{
	uint32 **arrays[] = { &work->by_page, &work->by_result,	 &work->chain,	  &work->successor,
						  &work->lists,	  &work->list_start, &work->list_next };
	Size i;

	for (i = 0; i < lengthof(arrays); i++)
		if (*arrays[i] != NULL) {
			cold_free(*arrays[i]);
			*arrays[i] = NULL;
		}
	cold_plan_release(plan, work->bytes);
	work->bytes = 0;
}

static ClusterColdDetailV1
seal_pages(ClusterColdPlanV1 *plan, ColdSealWork *work, ClusterColdObserveV1 observe, void *arg,
		   ClusterColdDiagV1 *diag)
{
	uint32 n = plan->component_count;
	uint32 start = 0;
	uint32 i;

	work->by_page = work_alloc(plan, work, n);
	work->by_result = work_alloc(plan, work, n);
	work->chain = work_alloc(plan, work, n);
	work->successor = work_alloc(plan, work, n);
	if (work->by_page == NULL || work->by_result == NULL || work->chain == NULL
		|| work->successor == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < n; i++)
		work->by_page[i] = i;
	qsort_arg(work->by_page, n, sizeof(uint32), by_page_compare, plan);
	for (i = 1; i <= n; i++) {
		ClusterColdDetailV1 detail;

		if (i < n
			&& page_compare(plan, cold_component(plan, work->by_page[start]),
							cold_component(plan, work->by_page[i]))
				   == 0)
			continue;
		detail
			= page_group_resolve(plan, work->by_page + start, i - start, work, observe, arg, diag);
		if (detail != CLUSTER_COLD_OK)
			return detail;
		start = i;
	}
	return CLUSTER_COLD_OK;
}

static bool
record_ready(const ClusterColdPlanV1 *plan, const ColdRecord *record, uint32 *blocking)
{
	uint16 i;

	for (i = 0; i < record->component_count; i++) {
		uint32 dependency
			= component_dependency(plan, cold_component(plan, record->first_component + i));

		if (dependency != CLUSTER_COLD_NO_INDEX
			&& (cold_record(plan, dependency)->flags & COLD_RECORD_SCHEDULED) == 0) {
			if (blocking != NULL)
				*blocking = dependency;
			return false;
		}
	}
	return true;
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
schedule_lists(ClusterColdPlanV1 *plan, ColdSealWork *work, uint32 *total)
{
	uint32 p = plan->participant_count;
	uint32 i;

	work->lists = work_alloc(plan, work, plan->record_count);
	work->list_start = work_alloc(plan, work, p + 1);
	work->list_next = work_alloc(plan, work, p);
	if (work->lists == NULL || work->list_start == NULL || work->list_next == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < plan->record_count; i++)
		if (!record_history(plan, i))
			work->list_start[cold_record(plan, i)->participant + 1]++;
	for (i = 0; i < p; i++)
		work->list_start[i + 1] += work->list_start[i];
	*total = work->list_start[p];
	for (i = 0; i < p; i++)
		work->list_next[i] = work->list_start[i];
	for (i = 0; i < plan->record_count; i++)
		if (!record_history(plan, i))
			work->lists[work->list_next[cold_record(plan, i)->participant]++] = i;
	for (i = 0; i < p; i++)
		work->list_next[i] = work->list_start[i];
	return CLUSTER_COLD_OK;
}

static void
schedule_deadlock(const ClusterColdPlanV1 *plan, const ColdSealWork *work, ClusterColdDiagV1 *diag)
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
	diag_record(plan, diag, best);
	(void)record_ready(plan, cold_record(plan, best), &blocking);
	if (blocking != CLUSTER_COLD_NO_INDEX) {
		diag->has_dependency = true;
		diag->dependency_participant
			= plan->participants[cold_record(plan, blocking)->participant].input_index;
		diag->dependency_read_rec_ptr = cold_record(plan, blocking)->read_rec_ptr;
	}
}

static ClusterColdDetailV1
seal_schedule(ClusterColdPlanV1 *plan, ColdSealWork *work, ClusterColdDiagV1 *diag)
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
			if (record_ready(plan, cold_record(plan, head), NULL)
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
cluster_cold_plan_seal_v1(ClusterColdPlanV1 *plan, ClusterColdObserveV1 observe, void *arg,
						  ClusterColdDiagV1 *diag)
{
	ColdSealWork work;
	ClusterColdDiagV1 local;
	ClusterColdDetailV1 detail;

	if (diag == NULL)
		diag = &local;
	memset(diag, 0, sizeof(*diag));
	if (!plan_valid(plan) || observe == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	if (plan->phase != COLD_PHASE_FEEDING)
		return CLUSTER_COLD_STATE;
	memset(&work, 0, sizeof(work));
	detail = seal_inputs_complete(plan, diag);
	if (detail == CLUSTER_COLD_OK)
		detail = seal_pages(plan, &work, observe, arg, diag);
	if (detail == CLUSTER_COLD_OK)
		detail = seal_schedule(plan, &work, diag);
	work_free(plan, &work);
	if (detail != CLUSTER_COLD_OK) {
		plan->phase = COLD_PHASE_FAILED;
		plan->schedule_count = 0;
		diag->detail = (uint8)detail;
		return detail;
	}
	plan->phase = COLD_PHASE_SEALED;
	return CLUSTER_COLD_OK;
}

#endif /* USE_PGRAC_CLUSTER */
