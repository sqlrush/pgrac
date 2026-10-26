/*-------------------------------------------------------------------------
 *
 * cluster_page_authority_ctrc.c
 *     Native CTRC replay-set protection of exact PAGE targets.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/backend/cluster/cluster_page_authority_ctrc.c
 *
 * NOTES
 *     Reuse original PAGE locks without manufacturing ordinary RR authority.
 *     Only private snapshots and local PAGE locks belong to this adapter.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_authority_ctrc.h"
#include "cluster/cluster_wal_inputs.h"
#include "storage/lwlock.h"

#ifdef USE_CLUSTER_UNIT
#define ctrc_page_alloc0(bytes_) calloc(1, (bytes_))
#define ctrc_page_free(pointer_) free((pointer_))
#else
#define ctrc_page_alloc0(bytes_) palloc0(bytes_)
#define ctrc_page_free(pointer_) pfree((pointer_))
#endif

#define RF_PAGE_CTRC_AUTHORITY_MAGIC UINT32_C(0x52504341)
#define RF_PAGE_CTRC_PREFLIGHTED 1
#define RF_PAGE_CTRC_PROMOTED 2

struct RfPageCtrcAuthorityV1 {
	uint32 magic;
	uint32 target_count;
	uint32 source_count;
	uint32 guard_count;
	uint8 state;
	const RfPageAuthorityTargetV1 *original_targets;
	const ClusterRecoveryCtrcReplaySetSourceV1 *original_sources;
	RfPageAuthorityTargetV1 *targets;
	ClusterRecoveryCtrcReplaySetSourceV1 *sources;
	RfPageGuardPreflightV1 *page_preflights;
	uint8 *target_guard_index;
	RfPageGuardV1 page_guards[RF_PAGE_GUARD_PARTITIONS];
	/* Aligned copies and per-target PAGE bookkeeping follow this header. */
};

static int
ctrc_page_identity_compare(const RfPageIdentityV1 *a, const RfPageIdentityV1 *b)
{
	int cmp;
#define CMP(field_) do { if (a->field_ < b->field_) return -1; \
	if (a->field_ > b->field_) return 1; } while (0)
	CMP(system_identifier);
	cmp = memcmp(a->storage_uuid, b->storage_uuid, sizeof(a->storage_uuid));
	if (cmp != 0) return cmp;
	CMP(locator.spcOid); CMP(locator.dbOid); CMP(locator.relNumber);
	CMP(forknum); CMP(blockno);
#undef CMP
	return 0;
}

static bool
ctrc_page_version_valid(const RfPageVersionV1 *version)
{
	uint8 incarnation = 0;

	for (Size i = 0; i < sizeof(version->segment_incarnation); i++)
		incarnation |= version->segment_incarnation[i];
	return incarnation != 0 && version->mutation_token != 0;
}

static bool
ctrc_page_shape_valid(const RfPageAuthorityTargetV1 *targets, uint32 count,
	const ClusterRecoveryCtrcReplaySetSourceV1 *sources, uint32 source_count)
{
	if (targets == NULL || count == 0 || count > RF_PAGE_STABLE_MAX_EDGES
		|| sources == NULL || source_count == 0 || source_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return false;
	for (uint32 i = 0; i < count; i++)
		if (!rf_page_identity_valid_v1(&targets[i].page_identity)
			|| !ctrc_page_version_valid(&targets[i].expected_result)
			|| targets[i].stable_base == NULL || targets[i].source == NULL
			|| targets[i].contributors == NULL
			|| (i > 0 && ctrc_page_identity_compare(&targets[i - 1].page_identity,
				&targets[i].page_identity) >= 0))
			return false;
	return true;
}

static RfPageCtrcAuthorityV1 *
ctrc_page_snapshot(const RfPageAuthorityTargetV1 *targets, uint32 count,
	const ClusterRecoveryCtrcReplaySetSourceV1 *sources, uint32 source_count, Size budget)
{
	Size target_bytes = (Size)count * sizeof(*targets);
	Size source_bytes = (Size)source_count * sizeof(*sources);
	Size preflight_bytes = (Size)count * sizeof(RfPageGuardPreflightV1);
	Size bytes = MAXALIGN(sizeof(RfPageCtrcAuthorityV1)) + MAXALIGN(target_bytes)
		+ MAXALIGN(source_bytes) + MAXALIGN(preflight_bytes) + (Size)count;
	RfPageCtrcAuthorityV1 *a;
	char *next;

	/* Prior count bounds keep all products and this sum bounded at Size32. */
	if (bytes > budget) return NULL;
	a = ctrc_page_alloc0(bytes);
	if (a == NULL) return NULL;
	a->magic = RF_PAGE_CTRC_AUTHORITY_MAGIC; a->state = RF_PAGE_CTRC_PREFLIGHTED;
	a->target_count = count; a->source_count = source_count;
	a->original_targets = targets; a->original_sources = sources;
	next = (char *)a + MAXALIGN(sizeof(*a));
	a->targets = (RfPageAuthorityTargetV1 *)next; next += MAXALIGN(target_bytes);
	a->sources = (ClusterRecoveryCtrcReplaySetSourceV1 *)next; next += MAXALIGN(source_bytes);
	a->page_preflights = (RfPageGuardPreflightV1 *)next; next += MAXALIGN(preflight_bytes);
	a->target_guard_index = (uint8 *)next;
	memcpy(a->targets, targets, target_bytes); memcpy(a->sources, sources, source_bytes);
	return a;
}

static bool
ctrc_page_owners_current(const RfPageCtrcAuthorityV1 *a)
{
	/* Pointer-bearing target values precede any nested proof/source access. */
	if (memcmp(a->original_targets, a->targets,
		(Size)a->target_count * sizeof(*a->targets)) != 0
		|| memcmp(a->original_sources, a->sources,
			(Size)a->source_count * sizeof(*a->sources)) != 0)
		return false;
	for (uint32 i = 0; i < a->target_count; i++) {
		const RfPageAuthorityTargetV1 *target = &a->targets[i];

		if (!rf_page_stable_base_proof_matches_ctrc_replays_v1(target->stable_base,
			&target->page_identity, &target->expected_result, a->original_sources,
			a->source_count, target->source, target->contributors))
			return false;
	}
	return true;
}

RfPageCtrcAuthorityBuildV1
rf_page_authority_ctrc_replays_preflight_v1(const RfPageAuthorityTargetV1 *targets,
	uint32 target_count, const ClusterRecoveryCtrcReplaySetSourceV1 *sources,
	uint32 source_count, Size memory_budget)
{
	RfPageCtrcAuthorityBuildV1 result = {NULL, RF_PAGE_AUTHORITY_INVALID_ARGUMENT};
	RfPageCtrcAuthorityV1 *a;

	if (!ctrc_page_shape_valid(targets, target_count, sources, source_count)) return result;
	a = ctrc_page_snapshot(targets, target_count, sources, source_count, memory_budget);
	if (a == NULL) { result.verdict = RF_PAGE_AUTHORITY_OOM; return result; }
	for (uint32 i = 0; i < target_count; i++)
		if (!rf_page_guard_preflight_v1(&a->targets[i].page_identity, &a->page_preflights[i])) {
			rf_page_authority_ctrc_replays_destroy_v1(&a);
			result.verdict = RF_PAGE_AUTHORITY_INTERNAL; return result;
		}
	if (!ctrc_page_owners_current(a)) {
		rf_page_authority_ctrc_replays_destroy_v1(&a);
		result.verdict = RF_PAGE_AUTHORITY_NO_STABLE_BASE; return result;
	}
	result.authority = a; result.verdict = RF_PAGE_AUTHORITY_OK;
	return result;
}

static void
ctrc_page_release(RfPageCtrcAuthorityV1 *a)
{
	while (a->guard_count > 0) {
		RfPageGuardV1 *guard = &a->page_guards[--a->guard_count];
		rf_page_guard_release_v1(guard);
		memset(guard, 0, sizeof(*guard));
	}
}

#ifndef USE_CLUSTER_UNIT
static void
ctrc_page_count_held(LWLock *lock pg_attribute_unused(),
	LWLockMode mode pg_attribute_unused(), void *context)
{
	++*(uint32 *)context;
}
#endif

static RfPageAuthorityVerdictV1
ctrc_page_capacity(const RfPageCtrcAuthorityV1 *a)
{
	bool seen[RF_PAGE_GUARD_PARTITIONS] = {false};
	uint32 needed = 0, held = 0;

	/* Refuse the whole batch before taking any PAGE lock. The native
	 * conditional primitive raises ERROR when its held list is full. */
	for (uint32 i = 0; i < a->target_count; i++) {
		uint32 partition = a->page_preflights[i].partition;

		if (partition >= RF_PAGE_GUARD_PARTITIONS) return RF_PAGE_AUTHORITY_INTERNAL;
		if (!seen[partition]) { seen[partition] = true; needed++; }
	}
	if (needed > LWLOCK_MAX_HELD_BY_PROC) return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
#ifndef USE_CLUSTER_UNIT
	ForEachLWLockHeldByMe(ctrc_page_count_held, &held);
#endif
	if (held > LWLOCK_MAX_HELD_BY_PROC - needed) return RF_PAGE_AUTHORITY_WOULD_BLOCK;
	return RF_PAGE_AUTHORITY_OK;
}

RfPageAuthorityVerdictV1
rf_page_authority_ctrc_replays_promote_nowait_v1(RfPageCtrcAuthorityV1 *a)
{
	int16 partitions[RF_PAGE_GUARD_PARTITIONS];
	RfPageAuthorityVerdictV1 capacity;

	if (a == NULL || a->magic != RF_PAGE_CTRC_AUTHORITY_MAGIC
		|| a->state != RF_PAGE_CTRC_PREFLIGHTED)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	if (!ctrc_page_owners_current(a)) return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
	capacity = ctrc_page_capacity(a);
	if (capacity != RF_PAGE_AUTHORITY_OK) return capacity;
	memset(partitions, -1, sizeof(partitions));
	for (uint32 i = 0; i < a->target_count; i++) {
		uint32 partition = a->page_preflights[i].partition;
		int16 index;

		if (partition >= RF_PAGE_GUARD_PARTITIONS) {
			ctrc_page_release(a); return RF_PAGE_AUTHORITY_INTERNAL;
		}
		index = partitions[partition];

		if (index < 0) {
			if (a->guard_count >= RF_PAGE_GUARD_PARTITIONS) {
				ctrc_page_release(a); return RF_PAGE_AUTHORITY_INTERNAL;
			}
			if (!rf_page_guard_promote_nowait_v1(&a->page_preflights[i],
				&a->page_guards[a->guard_count])) {
				ctrc_page_release(a);
				return RF_PAGE_AUTHORITY_WOULD_BLOCK;
			}
			index = (int16)a->guard_count++; partitions[partition] = index;
		}
		a->target_guard_index[i] = (uint8)index;
	}
	if (!ctrc_page_owners_current(a)) {
		ctrc_page_release(a); return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
	}
	a->state = RF_PAGE_CTRC_PROMOTED;
	return RF_PAGE_AUTHORITY_OK;
}

RfPageAuthorityVerdictV1
rf_page_authority_ctrc_replays_revalidate_nowait_v1(const RfPageCtrcAuthorityV1 *a)
{
	if (a == NULL || a->magic != RF_PAGE_CTRC_AUTHORITY_MAGIC
		|| a->state != RF_PAGE_CTRC_PROMOTED)
		return RF_PAGE_AUTHORITY_INVALID_ARGUMENT;
	if (!ctrc_page_owners_current(a)) return RF_PAGE_AUTHORITY_NO_STABLE_BASE;
	for (uint32 i = 0; i < a->target_count; i++) {
		uint8 index = a->target_guard_index[i];

		if (index >= a->guard_count
			|| !rf_page_guard_covers_nowait_v1(&a->page_guards[index], &a->targets[i].page_identity))
			return RF_PAGE_AUTHORITY_WOULD_BLOCK;
	}
	return RF_PAGE_AUTHORITY_OK;
}

bool
rf_page_authority_ctrc_replays_covers_nowait_v1(const RfPageCtrcAuthorityV1 *a,
	const RfPageIdentityV1 *identity, const RfPageVersionV1 *expected_before)
{
	if (rf_page_authority_ctrc_replays_revalidate_nowait_v1(a) != RF_PAGE_AUTHORITY_OK)
		return false;
	for (uint32 i = 0; i < a->target_count; i++)
		if (rf_page_identity_equal_v1(identity, &a->targets[i].page_identity)
			&& rf_page_version_equal_v1(expected_before, &a->targets[i].expected_result))
			return true;
	return false;
}

void
rf_page_authority_ctrc_replays_destroy_v1(RfPageCtrcAuthorityV1 **authority)
{
	RfPageCtrcAuthorityV1 *a;

	if (authority == NULL || *authority == NULL) return;
	a = *authority;
	if (a->magic != RF_PAGE_CTRC_AUTHORITY_MAGIC) return;
	ctrc_page_release(a);
	a->magic = 0; ctrc_page_free(a); *authority = NULL;
}
