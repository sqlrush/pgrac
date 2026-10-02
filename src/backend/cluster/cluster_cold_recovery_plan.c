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
 *	  Nothing here reads WAL or DATA, takes locks or grants authority.
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

#include "cluster/cluster_cold_recovery.h"

#ifdef USE_CLUSTER_UNIT
#define cold_alloc0(size_) calloc(1, (size_))
#define cold_realloc(pointer_, size_) realloc((pointer_), (size_))
#define cold_free(pointer_) free((pointer_))
#else
#define cold_alloc0(size_)                                                                         \
	palloc_extended((size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO)
#define cold_realloc(pointer_, size_)                                                              \
	((pointer_) == NULL                                                                            \
		 ? palloc_extended((size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM)                           \
		 : repalloc_extended((pointer_), (size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM))
#define cold_free(pointer_) pfree(pointer_)
#endif

#define CLUSTER_COLD_PLAN_MAGIC UINT32_C(0x434f4c44)

typedef enum ColdPhase {
	COLD_PHASE_FEEDING = 1,
	COLD_PHASE_SEALED = 2,
	COLD_PHASE_FAILED = 3
} ColdPhase;

typedef struct ColdParticipant {
	ClusterColdParticipantV1 cut;
	uint32 input_index;
	bool seen;
	XLogRecPtr last_read;
	XLogRecPtr last_end;
	uint64 replay_records; /* fed records ending after native redo */
} ColdParticipant;

typedef struct ColdRecord {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint64 scn;
	uint32 record_crc;
	uint32 participant; /* canonical index */
	uint32 first_component;
	uint16 component_count;
	uint8 rmid;
	uint8 info;
	bool history;
	bool scheduled;
} ColdRecord;

typedef struct ColdComponent {
	RfPageIdentityV1 page;
	RfPageVersionV1 before;
	RfPageVersionV1 result;
	RfPageVersionV1 expected_before;
	uint32 record;
	uint32 dependency; /* record index or CLUSTER_COLD_NO_INDEX */
	uint32 predecessor;
	uint32 successor;
	uint16 edge_flags;
	uint8 block_id;
	uint8 before_kind;
	uint8 verdict;
	uint8 expected_kind;
} ColdComponent;

struct ClusterColdPlanV1 {
	uint32 magic;
	uint8 phase;
	bool namespace_known;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint32 participant_count;
	Size memory_budget;
	Size memory_used;
	ColdParticipant *participants; /* canonical order */
	uint32 *canonical;			   /* caller index -> canonical index */
	ColdRecord *records;
	uint32 record_count;
	uint32 record_capacity;
	ColdComponent *components;
	uint32 component_count;
	uint32 component_capacity;
	uint32 *schedule;
	uint32 schedule_count;
};

/* Scratch owned by one seal() call; every member is accounted. */
typedef struct ColdSealWork {
	uint32 *by_page;
	uint32 *by_result;
	uint32 *chain;
	uint32 *lists;
	uint32 *list_start;
	uint32 *list_next;
	Size bytes;
} ColdSealWork;

static bool
bytes_nonzero(const uint8 *bytes, Size size)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < size; i++)
		value |= bytes[i];
	return value != 0;
}

static bool
version_present(const RfPageVersionV1 *version)
{
	return version->mutation_token != 0
		   && bytes_nonzero(version->segment_incarnation, sizeof(version->segment_incarnation));
}

static bool
version_equal(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	return left->mutation_token == right->mutation_token
		   && memcmp(left->segment_incarnation, right->segment_incarnation,
					 sizeof(left->segment_incarnation))
				  == 0;
}

static bool
incarnation_equal(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	return memcmp(left->segment_incarnation, right->segment_incarnation,
				  sizeof(left->segment_incarnation))
		   == 0;
}

static int
version_compare(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	int cmp = memcmp(left->segment_incarnation, right->segment_incarnation,
					 sizeof(left->segment_incarnation));

	if (cmp != 0)
		return cmp;
	if (left->mutation_token != right->mutation_token)
		return left->mutation_token < right->mutation_token ? -1 : 1;
	return 0;
}

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
plan_valid(const ClusterColdPlanV1 *plan)
{
	return plan != NULL && plan->magic == CLUSTER_COLD_PLAN_MAGIC;
}

static bool
plan_reserve(ClusterColdPlanV1 *plan, Size bytes)
{
	if (bytes > plan->memory_budget || plan->memory_used > plan->memory_budget - bytes)
		return false;
	plan->memory_used += bytes;
	return true;
}

static void
plan_release(ClusterColdPlanV1 *plan, Size bytes)
{
	Assert(plan->memory_used >= bytes);
	plan->memory_used -= bytes;
}

/* Grow one owned array by doubling, accounting the delta first. */
static ClusterColdDetailV1
plan_grow(ClusterColdPlanV1 *plan, void **array, uint32 *capacity, uint32 required, Size element)
{
	uint32 wanted = *capacity == 0 ? 64 : *capacity;
	Size delta;
	void *grown;

	if (required <= *capacity)
		return CLUSTER_COLD_OK;
	while (wanted < required) {
		if (wanted > UINT32_MAX / 2)
			return CLUSTER_COLD_CAPACITY;
		wanted *= 2;
	}
	delta = (Size)(wanted - *capacity) * element;
	if (!plan_reserve(plan, delta))
		return CLUSTER_COLD_CAPACITY;
	grown = cold_realloc(*array, (Size)wanted * element);
	if (grown == NULL) {
		plan_release(plan, delta);
		return CLUSTER_COLD_OOM;
	}
	*array = grown;
	*capacity = wanted;
	return CLUSTER_COLD_OK;
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
	ColdRecord *stored;
	ClusterColdDetailV1 detail;
	uint16 i;

	if (plan->record_count == UINT32_MAX
		|| (uint64)plan->component_count + record->component_count > UINT32_MAX)
		return CLUSTER_COLD_CAPACITY;
	detail = plan_grow(plan, (void **)&plan->records, &plan->record_capacity,
					   plan->record_count + 1, sizeof(ColdRecord));
	if (detail == CLUSTER_COLD_OK)
		detail = plan_grow(plan, (void **)&plan->components, &plan->component_capacity,
						   plan->component_count + record->component_count, sizeof(ColdComponent));
	if (detail != CLUSTER_COLD_OK)
		return detail;
	stored = &plan->records[plan->record_count];
	memset(stored, 0, sizeof(*stored));
	stored->read_rec_ptr = record->read_rec_ptr;
	stored->end_rec_ptr = record->end_rec_ptr;
	stored->scn = record->scn;
	stored->record_crc = record->record_crc;
	stored->participant = canonical;
	stored->first_component = plan->component_count;
	stored->component_count = record->component_count;
	stored->rmid = record->rmid;
	stored->info = record->info;
	stored->history = record->end_rec_ptr <= plan->participants[canonical].cut.native_redo;
	for (i = 0; i < record->component_count; i++) {
		const ClusterColdComponentV1 *source = &record->components[i];
		ColdComponent *component = &plan->components[plan->component_count + i];

		memset(component, 0, sizeof(*component));
		component->page = source->page;
		component->before = source->before;
		component->result = source->result;
		component->record = plan->record_count;
		component->dependency = CLUSTER_COLD_NO_INDEX;
		component->predecessor = CLUSTER_COLD_NO_INDEX;
		component->successor = CLUSTER_COLD_NO_INDEX;
		component->edge_flags = source->edge_flags;
		component->block_id = source->block_id;
		component->before_kind = source->before_kind;
	}
	plan->component_count += record->component_count;
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
		|| (record->record_flags & ~CLUSTER_COLD_RECORD_KNOWN_FLAGS) != 0
		|| record->reserved_zero != 0)
		detail = CLUSTER_COLD_INVALID_ARGUMENT;
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

static void
diag_record(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 record)
{
	const ColdRecord *stored = &plan->records[record];

	diag->has_record = true;
	diag->participant = plan->participants[stored->participant].input_index;
	diag->read_rec_ptr = stored->read_rec_ptr;
}

static void
diag_component(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 component)
{
	diag_record(plan, diag, plan->components[component].record);
	diag->has_page = true;
	diag->page = plan->components[component].page;
}

static int
by_page_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;
	const ColdComponent *a = &plan->components[*(const uint32 *)left];
	const ColdComponent *b = &plan->components[*(const uint32 *)right];
	const ColdRecord *ra = &plan->records[a->record];
	const ColdRecord *rb = &plan->records[b->record];
	int cmp = identity_compare(&a->page, &b->page);

	if (cmp != 0)
		return cmp;
	if (ra->participant != rb->participant)
		return ra->participant < rb->participant ? -1 : 1;
	if (ra->read_rec_ptr != rb->read_rec_ptr)
		return ra->read_rec_ptr < rb->read_rec_ptr ? -1 : 1;
	return 0;
}

static int
by_result_compare(const void *left, const void *right, void *arg)
{
	const ClusterColdPlanV1 *plan = (const ClusterColdPlanV1 *)arg;

	return version_compare(&plan->components[*(const uint32 *)left].result,
						   &plan->components[*(const uint32 *)right].result);
}

static uint32
find_result(const ClusterColdPlanV1 *plan, const uint32 *by_result, uint32 count,
			const RfPageVersionV1 *version)
{
	uint32 low = 0;
	uint32 high = count;

	while (low < high) {
		uint32 middle = low + (high - low) / 2;
		int cmp = version_compare(&plan->components[by_result[middle]].result, version);

		if (cmp == 0)
			return by_result[middle];
		if (cmp < 0)
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
page_chain_link(ClusterColdPlanV1 *plan, const uint32 *group, uint32 count, uint32 *by_result,
				uint32 *chain, ClusterColdDiagV1 *diag)
{
	uint32 start = CLUSTER_COLD_NO_INDEX;
	uint32 walked = 0;
	uint32 i;

	memcpy(by_result, group, (Size)count * sizeof(uint32));
	qsort_arg(by_result, count, sizeof(uint32), by_result_compare, plan);
	for (i = 1; i < count; i++)
		if (version_equal(&plan->components[by_result[i - 1]].result,
						  &plan->components[by_result[i]].result)) {
			diag_component(plan, diag, by_result[i]);
			return CLUSTER_COLD_EDGE_CYCLE;
		}
	for (i = 0; i < count; i++) {
		ColdComponent *component = &plan->components[group[i]];
		uint32 predecessor = CLUSTER_COLD_NO_INDEX;

		if (component->before_kind == RF_PAGE_STATE_PRESENT)
			predecessor = find_result(plan, by_result, count, &component->before);
		component->predecessor = predecessor;
		if (predecessor == CLUSTER_COLD_NO_INDEX) {
			if (start != CLUSTER_COLD_NO_INDEX) {
				const ColdComponent *first = &plan->components[start];

				/* Two starts from one version is a fork; otherwise an edge
				 * is missing or a second incarnation needs its SPACE owner. */
				diag_component(plan, diag, group[i]);
				return first->before_kind == component->before_kind
							   && version_equal(&first->before, &component->before)
						   ? CLUSTER_COLD_EDGE_BRANCH
						   : CLUSTER_COLD_CHAIN_AMBIGUOUS;
			}
			start = group[i];
			continue;
		}
		if (plan->components[predecessor].successor != CLUSTER_COLD_NO_INDEX) {
			diag_component(plan, diag, group[i]);
			return CLUSTER_COLD_EDGE_BRANCH;
		}
		plan->components[predecessor].successor = group[i];
	}
	for (i = start; i != CLUSTER_COLD_NO_INDEX && walked < count; i = plan->components[i].successor)
		chain[walked++] = i;
	if (start == CLUSTER_COLD_NO_INDEX || walked != count
		|| plan->components[chain[count - 1]].successor != CLUSTER_COLD_NO_INDEX) {
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
		if (plan->records[plan->components[chain[i - 1]].record].history) {
			start = i;
			break;
		}
	for (i = start; i < count && (int64)i <= limit; i++)
		if (plan->components[chain[i]].edge_flags != 0)
			return (int64)i;
	return -1;
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
				   const ClusterColdDataV1 *data, int64 *covered, bool *exact,
				   ClusterColdDiagV1 *diag)
{
	const ColdComponent *first = &plan->components[chain[0]];
	bool new_start = first->before_kind != RF_PAGE_STATE_PRESENT;
	int64 anchor;
	int64 position = -2;
	uint32 i;

	*exact = true;
	if (data->kind == CLUSTER_COLD_DATA_PRESENT) {
		for (i = 0; i < count && position == -2; i++)
			if (version_equal(&plan->components[chain[i]].result, &data->version))
				position = (int64)i;
		if (position == -2 && !new_start && version_equal(&first->before, &data->version))
			position = -1;
		if (position == -2) {
			diag_component(plan, diag, chain[0]);
			diag->version = data->version;
			return incarnation_equal(&first->result, &data->version)
					   ? CLUSTER_COLD_ANCESTOR_MISSING
					   : CLUSTER_COLD_INCARNATION_MISMATCH;
		}
	} else if (data->kind != CLUSTER_COLD_DATA_INVALID && new_start) {
		if (data->kind == CLUSTER_COLD_DATA_UNFORMATTED
			&& !incarnation_equal(&first->result, &data->version)) {
			diag_component(plan, diag, chain[0]);
			diag->version = data->version;
			return CLUSTER_COLD_INCARNATION_MISMATCH;
		}
		position = -1;
	} else {
		/* Unreadable, or a new page where a formatted one was expected. */
		anchor = page_earliest_replayable_anchor(plan, chain, count, (int64)count);
		if (anchor < 0) {
			diag_component(plan, diag, chain[count - 1]);
			diag->version = data->version;
			return CLUSTER_COLD_ANCHOR_MISSING;
		}
		*exact = false;
		*covered = anchor - 1;
		return CLUSTER_COLD_OK;
	}
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
	if ((data->flags & CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED) == 0) {
		anchor = page_earliest_replayable_anchor(plan, chain, count, (int64)count);
		if (anchor < 0) {
			diag_component(plan, diag, chain[0]);
			diag->version = data->version;
			return CLUSTER_COLD_CONTENT_UNPROVEN;
		}
		*covered = anchor - 1;
		return CLUSTER_COLD_OK;
	}
	anchor = page_earliest_replayable_anchor(plan, chain, count, position + 1);
	*covered = anchor >= 0 ? anchor - 1 : position;
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
					 const ClusterColdDataV1 *data, int64 covered, bool exact,
					 ClusterColdDiagV1 *diag)
{
	uint32 i;

	for (i = 0; i < count; i++) {
		ColdComponent *component = &plan->components[chain[i]];

		if ((int64)i <= covered) {
			component->verdict = CLUSTER_COLD_BLOCK_SKIP;
			continue;
		}
		if (plan->records[component->record].history) {
			diag_component(plan, diag, chain[i]);
			diag->version = component->before;
			return CLUSTER_COLD_HISTORY_GAP;
		}
		component->verdict = apply_verdict(component->edge_flags);
		if ((int64)i == covered + 1) {
			component->expected_kind = exact ? data->kind : CLUSTER_COLD_DATA_INVALID;
			if (exact)
				component->expected_before = data->version;
		} else {
			const ColdComponent *previous = &plan->components[chain[i - 1]];

			component->expected_kind = CLUSTER_COLD_DATA_PRESENT;
			component->expected_before = previous->result;
			component->dependency = previous->record;
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
	bool replayable = false;
	bool exact;
	int64 covered;
	uint32 i;

	for (i = 0; i < count && !replayable; i++)
		replayable = !plan->records[plan->components[group[i]].record].history;
	if (!replayable)
		return CLUSTER_COLD_OK; /* completed history: no DATA duty */
	detail = page_chain_link(plan, group, count, work->by_result, work->chain, diag);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	memset(&data, 0, sizeof(data));
	if (!observe(arg, &plan->components[group[0]].page, &data) || !data_shape_valid(&data)) {
		diag_component(plan, diag, work->chain[0]);
		return CLUSTER_COLD_OBSERVATION_FAILED;
	}
	detail = page_data_position(plan, work->chain, count, &data, &covered, &exact, diag);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	return page_assign_verdicts(plan, work->chain, count, &data, covered, exact, diag);
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

	if (!plan_reserve(plan, bytes))
		return NULL;
	array = (uint32 *)cold_alloc0(bytes);
	if (array == NULL) {
		plan_release(plan, bytes);
		return NULL;
	}
	work->bytes += bytes;
	return array;
}

static void
work_free(ClusterColdPlanV1 *plan, ColdSealWork *work)
{
	uint32 **arrays[] = { &work->by_page, &work->by_result,	 &work->chain,
						  &work->lists,	  &work->list_start, &work->list_next };
	Size i;

	for (i = 0; i < lengthof(arrays); i++)
		if (*arrays[i] != NULL) {
			cold_free(*arrays[i]);
			*arrays[i] = NULL;
		}
	plan_release(plan, work->bytes);
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
	if (work->by_page == NULL || work->by_result == NULL || work->chain == NULL)
		return CLUSTER_COLD_CAPACITY;
	for (i = 0; i < n; i++)
		work->by_page[i] = i;
	qsort_arg(work->by_page, n, sizeof(uint32), by_page_compare, plan);
	for (i = 1; i <= n; i++) {
		ClusterColdDetailV1 detail;

		if (i < n
			&& identity_compare(&plan->components[work->by_page[start]].page,
								&plan->components[work->by_page[i]].page)
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
		const ColdComponent *component = &plan->components[record->first_component + i];

		if (component->dependency != CLUSTER_COLD_NO_INDEX
			&& !plan->records[component->dependency].scheduled) {
			if (blocking != NULL)
				*blocking = component->dependency;
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
		if (!plan->records[i].history)
			work->list_start[plan->records[i].participant + 1]++;
	for (i = 0; i < p; i++)
		work->list_start[i + 1] += work->list_start[i];
	*total = work->list_start[p];
	for (i = 0; i < p; i++)
		work->list_next[i] = work->list_start[i];
	for (i = 0; i < plan->record_count; i++)
		if (!plan->records[i].history)
			work->lists[work->list_next[plan->records[i].participant]++] = i;
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
			|| record_key_less(&plan->records[head], &plan->records[best]))
			best = head;
	}
	if (best == CLUSTER_COLD_NO_INDEX)
		return;
	diag_record(plan, diag, best);
	(void)record_ready(plan, &plan->records[best], &blocking);
	if (blocking != CLUSTER_COLD_NO_INDEX) {
		diag->has_dependency = true;
		diag->dependency_participant
			= plan->participants[plan->records[blocking].participant].input_index;
		diag->dependency_read_rec_ptr = plan->records[blocking].read_rec_ptr;
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
	if (!plan_reserve(plan, bytes))
		return CLUSTER_COLD_CAPACITY;
	plan->schedule = (uint32 *)cold_alloc0(bytes);
	if (plan->schedule == NULL) {
		plan_release(plan, bytes);
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
			if (record_ready(plan, &plan->records[head], NULL)
				&& (best == CLUSTER_COLD_NO_INDEX
					|| record_key_less(&plan->records[head], &plan->records[best])))
				best = head;
		}
		if (best == CLUSTER_COLD_NO_INDEX) {
			schedule_deadlock(plan, work, diag);
			return CLUSTER_COLD_DEADLOCK;
		}
		plan->records[best].scheduled = true;
		work->list_next[plan->records[best].participant]++;
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
	record = &plan->records[plan->schedule[index]];
	memset(out, 0, sizeof(*out));
	out->participant = plan->participants[record->participant].input_index;
	out->read_rec_ptr = record->read_rec_ptr;
	out->end_rec_ptr = record->end_rec_ptr;
	out->record_crc = record->record_crc;
	out->rmid = record->rmid;
	out->info = record->info;
	for (i = 0; i < record->component_count; i++) {
		const ColdComponent *component = &plan->components[record->first_component + i];
		ClusterColdBlockStepV1 *block = &out->blocks[component->block_id];

		/* Every component of a scheduled record has a verdict; anything else
		 * is a planner bug and must not reach replay as "no action". */
		if (component->verdict == CLUSTER_COLD_BLOCK_NONE) {
			memset(out, 0, sizeof(*out));
			return false;
		}
		block->verdict = component->verdict;
		block->expected_kind = component->expected_kind;
		block->expected_before = component->expected_before;
		block->result = component->result;
		if (component->verdict == CLUSTER_COLD_BLOCK_SKIP)
			any_skip = true;
		else
			any_apply = true;
	}
	out->all_skip = any_skip && !any_apply;
	out->mixed = any_skip && any_apply;
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

	if (plan_address == NULL || *plan_address == NULL)
		return;
	plan = *plan_address;
	if (plan->magic != CLUSTER_COLD_PLAN_MAGIC)
		return;
	plan->magic = 0;
	if (plan->schedule != NULL)
		cold_free(plan->schedule);
	if (plan->components != NULL)
		cold_free(plan->components);
	if (plan->records != NULL)
		cold_free(plan->records);
	cold_free(plan->canonical);
	cold_free(plan->participants);
	cold_free(plan);
	*plan_address = NULL;
}

#endif /* USE_PGRAC_CLUSTER */
