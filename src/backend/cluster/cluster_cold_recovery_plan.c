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

/*
 * Storage is compact: the page namespace (system identifier, storage UUID)
 * is plan-wide; a component names its page by an interned relation fork
 * and its block; both of its versions share one interned segment
 * incarnation (an ABSENT before-state has none); and expected state and
 * dependency are derived from chain links when a step is read.  Records
 * and components live in fixed-size chunks, so growth never doubles the
 * footprint.
 */
#define COLD_CHUNK_SHIFT 10
#define COLD_CHUNK_ENTRIES (UINT32_C(1) << COLD_CHUNK_SHIFT)
#define COLD_CHUNK_MASK (COLD_CHUNK_ENTRIES - 1)

typedef struct ColdRecord {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint64 scn;
	uint32 record_crc;
	uint32 first_component;
	uint8 participant; /* canonical index */
	uint8 component_count;
	uint8 rmid;
	uint8 info;
	uint8 flags; /* COLD_RECORD_* */
} ColdRecord;

#define COLD_RECORD_HISTORY 0x01
#define COLD_RECORD_SCHEDULED 0x02

StaticAssertDecl(CLUSTER_COLD_MAX_PARTICIPANTS <= UINT8_MAX + 1,
				 "canonical participant index must fit ColdRecord");
StaticAssertDecl(CLUSTER_COLD_MAX_COMPONENTS <= UINT8_MAX, "component count must fit ColdRecord");

/* Interned relation fork; the plan-wide namespace completes the page. */
typedef struct ColdRelation {
	RelFileLocator locator;
	uint32 forknum;
} ColdRelation;

/* Interned segment incarnation of one relation fork. */
typedef struct ColdSegment {
	uint32 relation;
	uint8 incarnation[16];
} ColdSegment;

typedef struct ColdComponent {
	uint64 before_token; /* 0 unless the before-state is PRESENT */
	uint64 result_token;
	uint32 segment; /* (relation, result incarnation) */
	BlockNumber blockno;
	uint32 record;
	uint32 link; /* seal: chain predecessor; sealed APPLY: expected source */
	uint16 edge_flags;
	uint8 block_id;
	uint8 before_kind;
	uint8 verdict;
	uint8 state; /* COLD_STATE_* */
} ColdComponent;

/*
 * A sealed APPLY component expects the result of its link (the chain
 * predecessor, whose record it depends on) unless it is the first applied
 * component of its page.  That one expects DATA: with EXACT, the observed
 * state of kind DATA_KIND, whose version is link's result, or link's
 * before-state with DATA_BEFORE; without EXACT, any content (an anchor).
 */
#define COLD_STATE_FIRST_APPLY 0x01
#define COLD_STATE_EXACT 0x02
#define COLD_STATE_DATA_BEFORE 0x04
#define COLD_STATE_DATA_KIND_SHIFT 4
#define COLD_STATE_DATA_KIND_MASK 0x03

/* Open-addressing intern table; slots hold entry index + 1. */
typedef struct ColdIntern {
	char *entries;
	uint32 count;
	uint32 capacity;
	uint32 *slots;
	uint32 slot_count; /* power of two */
} ColdIntern;

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
	ColdRecord **record_chunks;
	uint32 record_count;
	uint32 record_chunk_count;
	uint32 record_chunk_capacity;
	ColdComponent **component_chunks;
	uint32 component_count;
	uint32 component_chunk_count;
	uint32 component_chunk_capacity;
	ColdIntern relations; /* ColdRelation */
	ColdIntern segments;  /* ColdSegment */
	uint32 *schedule;
	uint32 schedule_count;
};

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

static inline ColdRecord *
cold_record(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &plan->record_chunks[index >> COLD_CHUNK_SHIFT][index & COLD_CHUNK_MASK];
}

static inline ColdComponent *
cold_component(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &plan->component_chunks[index >> COLD_CHUNK_SHIFT][index & COLD_CHUNK_MASK];
}

static inline const ColdSegment *
cold_segment(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &((const ColdSegment *)plan->segments.entries)[index];
}

static inline const ColdRelation *
cold_relation(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &((const ColdRelation *)plan->relations.entries)[index];
}

static inline bool
record_history(const ClusterColdPlanV1 *plan, uint32 record)
{
	return (cold_record(plan, record)->flags & COLD_RECORD_HISTORY) != 0;
}

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

/* Make room for `required` entries of a chunked array, accounting first. */
static ClusterColdDetailV1
chunk_reserve(ClusterColdPlanV1 *plan, void ***chunks, uint32 *chunk_count, uint32 *chunk_capacity,
			  uint64 required, Size element)
{
	uint64 needed = (required + COLD_CHUNK_MASK) >> COLD_CHUNK_SHIFT;

	if (required > UINT32_MAX)
		return CLUSTER_COLD_CAPACITY;
	while (*chunk_count < needed) {
		void *chunk;

		if (*chunk_count == *chunk_capacity) {
			uint32 wanted = *chunk_capacity == 0 ? 16 : *chunk_capacity * 2;
			Size delta = (Size)(wanted - *chunk_capacity) * sizeof(void *);
			void **grown;

			if (!plan_reserve(plan, delta))
				return CLUSTER_COLD_CAPACITY;
			grown = (void **)cold_realloc(*chunks, (Size)wanted * sizeof(void *));
			if (grown == NULL) {
				plan_release(plan, delta);
				return CLUSTER_COLD_OOM;
			}
			*chunks = grown;
			*chunk_capacity = wanted;
		}
		if (!plan_reserve(plan, (Size)COLD_CHUNK_ENTRIES * element))
			return CLUSTER_COLD_CAPACITY;
		chunk = cold_alloc0((Size)COLD_CHUNK_ENTRIES * element);
		if (chunk == NULL) {
			plan_release(plan, (Size)COLD_CHUNK_ENTRIES * element);
			return CLUSTER_COLD_OOM;
		}
		(*chunks)[(*chunk_count)++] = chunk;
	}
	return CLUSTER_COLD_OK;
}

static uint32
intern_hash(const void *key, Size size)
{
	const uint8 *bytes = (const uint8 *)key;
	uint32 hash = UINT32_C(2166136261);
	Size i;

	for (i = 0; i < size; i++) {
		hash ^= bytes[i];
		hash *= UINT32_C(16777619);
	}
	return hash;
}

static ClusterColdDetailV1
intern_rehash(ClusterColdPlanV1 *plan, ColdIntern *intern, Size key_size, uint32 slot_count)
{
	Size bytes = (Size)slot_count * sizeof(uint32);
	uint32 *slots;
	uint32 i;

	if (!plan_reserve(plan, bytes))
		return CLUSTER_COLD_CAPACITY;
	slots = (uint32 *)cold_alloc0(bytes);
	if (slots == NULL) {
		plan_release(plan, bytes);
		return CLUSTER_COLD_OOM;
	}
	for (i = 0; i < intern->count; i++) {
		uint32 at = intern_hash(intern->entries + (Size)i * key_size, key_size) & (slot_count - 1);

		while (slots[at] != 0)
			at = (at + 1) & (slot_count - 1);
		slots[at] = i + 1;
	}
	if (intern->slots != NULL) {
		cold_free(intern->slots);
		plan_release(plan, (Size)intern->slot_count * sizeof(uint32));
	}
	intern->slots = slots;
	intern->slot_count = slot_count;
	return CLUSTER_COLD_OK;
}

/* Index of `key` in the table, adding it when new. */
static ClusterColdDetailV1
intern_lookup(ClusterColdPlanV1 *plan, ColdIntern *intern, const void *key, Size key_size,
			  uint32 *out)
{
	ClusterColdDetailV1 detail;
	uint32 at;

	if (intern->count >= UINT32_MAX / 4)
		return CLUSTER_COLD_CAPACITY;
	if ((uint64)(intern->count + 1) * 2 > intern->slot_count) {
		detail = intern_rehash(plan, intern, key_size,
							   intern->slot_count == 0 ? 64 : intern->slot_count * 2);
		if (detail != CLUSTER_COLD_OK)
			return detail;
	}
	at = intern_hash(key, key_size) & (intern->slot_count - 1);
	while (intern->slots[at] != 0) {
		uint32 index = intern->slots[at] - 1;

		if (memcmp(intern->entries + (Size)index * key_size, key, key_size) == 0) {
			*out = index;
			return CLUSTER_COLD_OK;
		}
		at = (at + 1) & (intern->slot_count - 1);
	}
	detail = plan_grow(plan, (void **)&intern->entries, &intern->capacity, intern->count + 1,
					   key_size);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	memcpy(intern->entries + (Size)intern->count * key_size, key, key_size);
	intern->slots[at] = intern->count + 1;
	*out = intern->count++;
	return CLUSTER_COLD_OK;
}

static void
intern_free(ColdIntern *intern)
{
	if (intern->entries != NULL)
		cold_free(intern->entries);
	if (intern->slots != NULL)
		cold_free(intern->slots);
	memset(intern, 0, sizeof(*intern));
}

static RfPageVersionV1
component_result(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	RfPageVersionV1 version;

	memcpy(version.segment_incarnation, cold_segment(plan, component->segment)->incarnation,
		   sizeof(version.segment_incarnation));
	version.mutation_token = component->result_token;
	return version;
}

static RfPageVersionV1
component_before(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	if (component->before_kind != RF_PAGE_STATE_ABSENT)
		memcpy(version.segment_incarnation, cold_segment(plan, component->segment)->incarnation,
			   sizeof(version.segment_incarnation));
	version.mutation_token = component->before_token;
	return version;
}

static bool
segment_incarnation_is(const ClusterColdPlanV1 *plan, const ColdComponent *component,
					   const RfPageVersionV1 *version)
{
	return memcmp(cold_segment(plan, component->segment)->incarnation, version->segment_incarnation,
				  sizeof(version->segment_incarnation))
		   == 0;
}

static RfPageIdentityV1
component_page(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	const ColdRelation *relation
		= cold_relation(plan, cold_segment(plan, component->segment)->relation);
	RfPageIdentityV1 page;

	memset(&page, 0, sizeof(page));
	page.system_identifier = plan->system_identifier;
	memcpy(page.storage_uuid, plan->storage_uuid, sizeof(page.storage_uuid));
	page.locator = relation->locator;
	page.forknum = relation->forknum;
	page.blockno = component->blockno;
	return page;
}

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

/* Expected state of an applied component (see COLD_STATE_*). */
static void
component_expected(const ClusterColdPlanV1 *plan, const ColdComponent *component,
				   ClusterColdBlockStepV1 *block)
{
	const ColdComponent *source;
	uint8 kind;

	if ((component->state & COLD_STATE_FIRST_APPLY) == 0) {
		block->expected_kind = CLUSTER_COLD_DATA_PRESENT;
		block->expected_before = component_result(plan, cold_component(plan, component->link));
		return;
	}
	if ((component->state & COLD_STATE_EXACT) == 0) {
		block->expected_kind = CLUSTER_COLD_DATA_INVALID;
		return;
	}
	kind = (component->state >> COLD_STATE_DATA_KIND_SHIFT) & COLD_STATE_DATA_KIND_MASK;
	source = cold_component(plan, component->link);
	block->expected_kind = kind;
	if (kind == CLUSTER_COLD_DATA_PRESENT)
		block->expected_before = (component->state & COLD_STATE_DATA_BEFORE) != 0
									 ? component_before(plan, source)
									 : component_result(plan, source);
	else if (kind == CLUSTER_COLD_DATA_UNFORMATTED)
		memcpy(block->expected_before.segment_incarnation,
			   cold_segment(plan, source->segment)->incarnation,
			   sizeof(block->expected_before.segment_incarnation));
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
	detail = chunk_reserve(plan, (void ***)&plan->record_chunks, &plan->record_chunk_count,
						   &plan->record_chunk_capacity, (uint64)plan->record_count + 1,
						   sizeof(ColdRecord));
	if (detail == CLUSTER_COLD_OK)
		detail = chunk_reserve(plan, (void ***)&plan->component_chunks,
							   &plan->component_chunk_count, &plan->component_chunk_capacity,
							   (uint64)plan->component_count + record->component_count,
							   sizeof(ColdComponent));
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
		detail
			= intern_lookup(plan, &plan->relations, &relation, sizeof(relation), &segment.relation);
		memcpy(segment.incarnation, source->result.segment_incarnation, 16);
		if (detail == CLUSTER_COLD_OK)
			detail = intern_lookup(plan, &plan->segments, &segment, sizeof(segment), &segments[i]);
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
	diag->page = component_page(plan, stored);
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
			&& !segment_incarnation_is(plan, first, &data->version)) {
			diag_component(plan, diag, chain[0]);
			diag->version = data->version;
			return CLUSTER_COLD_INCARNATION_MISMATCH;
		}
		return CLUSTER_COLD_OK;
	}
	for (i = 0; i < count; i++) {
		const ColdComponent *component = cold_component(plan, chain[i]);

		if (component->result_token == data->version.mutation_token
			&& segment_incarnation_is(plan, component, &data->version)) {
			*position = (int64)i;
			return CLUSTER_COLD_OK;
		}
	}
	before = component_before(plan, first);
	if (!new_start && version_equal(&before, &data->version)) {
		*position = -1;
		return CLUSTER_COLD_OK;
	}
	diag_component(plan, diag, chain[0]);
	diag->version = data->version;
	return segment_incarnation_is(plan, first, &data->version) ? CLUSTER_COLD_ANCESTOR_MISSING
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
			diag->version = component_before(plan, component);
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
	page = component_page(plan, cold_component(plan, group[0]));
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
	uint32 **arrays[] = { &work->by_page, &work->by_result,	 &work->chain,	  &work->successor,
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
	out->participant = plan->participants[record->participant].input_index;
	out->read_rec_ptr = record->read_rec_ptr;
	out->end_rec_ptr = record->end_rec_ptr;
	out->record_crc = record->record_crc;
	out->rmid = record->rmid;
	out->info = record->info;
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
		block->result = component_result(plan, component);
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
	for (i = 0; i < plan->record_chunk_count; i++)
		cold_free(plan->record_chunks[i]);
	if (plan->record_chunks != NULL)
		cold_free(plan->record_chunks);
	intern_free(&plan->relations);
	intern_free(&plan->segments);
	cold_free(plan->canonical);
	cold_free(plan->participants);
	cold_free(plan);
	*plan_address = NULL;
}

#endif /* USE_PGRAC_CLUSTER */
