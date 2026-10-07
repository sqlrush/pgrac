/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_store.c
 *	  Compact storage of the typed cold-crash replay plan.
 *
 *	  Every allocation of a plan is accounted against its memory budget.
 *	  Records and components live in fixed-size chunks; relation forks and
 *	  segment incarnations are interned, so a component carries only tokens,
 *	  indexes and flags.  Versions and page identities are rebuilt from them.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_plan_store.c
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

bool
cold_plan_reserve(ClusterColdPlanV1 *plan, Size bytes)
{
	if (bytes > plan->memory_budget || plan->memory_used > plan->memory_budget - bytes)
		return false;
	plan->memory_used += bytes;
	return true;
}

void
cold_plan_release(ClusterColdPlanV1 *plan, Size bytes)
{
	Assert(plan->memory_used >= bytes);
	plan->memory_used -= bytes;
}

/*
 * Zeroed scratch of `bytes` (at least one word), accounted first and added
 * to *accounted for the caller's release; NULL when over budget or out of
 * memory.
 */
void *
cold_plan_scratch(ClusterColdPlanV1 *plan, Size bytes, Size *accounted)
{
	void *array;

	bytes = Max(bytes, sizeof(uint32));
	if (!cold_plan_reserve(plan, bytes))
		return NULL;
	array = cold_alloc0(bytes);
	if (array == NULL) {
		cold_plan_release(plan, bytes);
		return NULL;
	}
	if (accounted != NULL)
		*accounted += bytes;
	return array;
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
	if (!cold_plan_reserve(plan, delta))
		return CLUSTER_COLD_CAPACITY;
	grown = cold_realloc(*array, (Size)wanted * element);
	if (grown == NULL) {
		cold_plan_release(plan, delta);
		return CLUSTER_COLD_OOM;
	}
	*array = grown;
	*capacity = wanted;
	return CLUSTER_COLD_OK;
}

/* Make room for `required` entries of a chunked array, accounting first. */
ClusterColdDetailV1
cold_plan_chunk_reserve(ClusterColdPlanV1 *plan, void ***chunks, uint32 *chunk_count,
						uint32 *chunk_capacity, uint64 required, Size element)
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

			if (!cold_plan_reserve(plan, delta))
				return CLUSTER_COLD_CAPACITY;
			grown = (void **)cold_realloc(*chunks, (Size)wanted * sizeof(void *));
			if (grown == NULL) {
				cold_plan_release(plan, delta);
				return CLUSTER_COLD_OOM;
			}
			*chunks = grown;
			*chunk_capacity = wanted;
		}
		if (!cold_plan_reserve(plan, (Size)COLD_CHUNK_ENTRIES * element))
			return CLUSTER_COLD_CAPACITY;
		chunk = cold_alloc0((Size)COLD_CHUNK_ENTRIES * element);
		if (chunk == NULL) {
			cold_plan_release(plan, (Size)COLD_CHUNK_ENTRIES * element);
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

	if (!cold_plan_reserve(plan, bytes))
		return CLUSTER_COLD_CAPACITY;
	slots = (uint32 *)cold_alloc0(bytes);
	if (slots == NULL) {
		cold_plan_release(plan, bytes);
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
		cold_plan_release(plan, (Size)intern->slot_count * sizeof(uint32));
	}
	intern->slots = slots;
	intern->slot_count = slot_count;
	return CLUSTER_COLD_OK;
}

/* Index of `key` in the table, adding it when new. */
ClusterColdDetailV1
cold_plan_intern(ClusterColdPlanV1 *plan, ColdIntern *intern, const void *key, Size key_size,
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

/* Index of `key` if it was interned; never adds it. */
bool
cold_plan_intern_find(const ColdIntern *intern, const void *key, Size key_size, uint32 *out)
{
	uint32 at;

	if (intern->slot_count == 0)
		return false;
	at = intern_hash(key, key_size) & (intern->slot_count - 1);
	while (intern->slots[at] != 0) {
		uint32 index = intern->slots[at] - 1;

		if (memcmp(intern->entries + (Size)index * key_size, key, key_size) == 0) {
			*out = index;
			return true;
		}
		at = (at + 1) & (intern->slot_count - 1);
	}
	return false;
}

void
cold_plan_intern_free(ColdIntern *intern)
{
	if (intern->entries != NULL)
		cold_free(intern->entries);
	if (intern->slots != NULL)
		cold_free(intern->slots);
	memset(intern, 0, sizeof(*intern));
}

RfPageVersionV1
cold_plan_component_result(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	RfPageVersionV1 version;

	memcpy(version.segment_incarnation, cold_segment(plan, component->segment)->incarnation,
		   sizeof(version.segment_incarnation));
	version.mutation_token = component->result_token;
	return version;
}

RfPageVersionV1
cold_plan_component_before(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	if (component->before_kind != RF_PAGE_STATE_ABSENT)
		memcpy(version.segment_incarnation, cold_segment(plan, component->segment)->incarnation,
			   sizeof(version.segment_incarnation));
	version.mutation_token = component->before_token;
	return version;
}

bool
cold_plan_incarnation_is(const ClusterColdPlanV1 *plan, const ColdComponent *component,
						 const RfPageVersionV1 *version)
{
	return memcmp(cold_segment(plan, component->segment)->incarnation, version->segment_incarnation,
				  sizeof(version->segment_incarnation))
		   == 0;
}

RfPageIdentityV1
cold_plan_component_page(const ClusterColdPlanV1 *plan, const ColdComponent *component)
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

#endif /* USE_PGRAC_CLUSTER */
