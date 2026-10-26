/*-------------------------------------------------------------------------
 *
 * test_cluster_page_ctrc_capacity.c
 *     Native PAGE branch and per-process lock-capacity boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_page_ctrc_capacity.c
 *
 * NOTES
 *     Compile both complete product CUs without USE_CLUSTER_UNIT. Only the
 *     allocator, native-proof leaf and LWLock service are controlled. The
 *     service models the original held-list ceiling and nonlocal ERROR; it
 *     does not use the PAGE primitive's boolean unit partition branch.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include "cluster/cluster_page_authority_ctrc.h"
#include "cluster/cluster_wal_inputs.h"
#include "storage/lwlock.h"
#include "unit_test.h"
#undef printf
#undef fprintf

UT_DEFINE_GLOBALS();
int MyProcPid = 123;
static LWLockPadded native_page_locks[RF_PAGE_GUARD_PARTITIONS];
static LWLock other_locks[LWLOCK_MAX_HELD_BY_PROC];
static LWLock *held[LWLOCK_MAX_HELD_BY_PROC], *contended;
static unsigned held_count, acquire_calls, release_calls, capacity_errors;
static jmp_buf capacity_error;
static RfPageAuthorityTargetV1 targets[RF_PAGE_GUARD_PARTITIONS + 1];
static ClusterRecoveryCtrcReplaySetSourceV1 native_source;
static RfPagePinnedSourceV1 pinned_source;
static RfContributorVectorV1 contributors;
static uint64 proof_storage;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

void
RequestNamedLWLockTranche(const char *name, int count)
{
	UT_ASSERT(strcmp(name, "RfPageGuard") == 0);
	UT_ASSERT_EQ(count, RF_PAGE_GUARD_PARTITIONS);
}

LWLockPadded *
GetNamedLWLockTranche(const char *name)
{
	UT_ASSERT(strcmp(name, "RfPageGuard") == 0);
	return native_page_locks;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	acquire_calls++;
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	if (held_count >= LWLOCK_MAX_HELD_BY_PROC) {
		capacity_errors++;
		longjmp(capacity_error, 1);
	}
	if (lock == contended) return false;
	for (unsigned i = 0; i < held_count; i++)
		if (held[i] == lock) return false;
	held[held_count++] = lock;
	return true;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	for (unsigned i = 0; i < held_count; i++)
		if (held[i] == lock) return true;
	return false;
}

void
LWLockRelease(LWLock *lock)
{
	for (unsigned i = 0; i < held_count; i++)
		if (held[i] == lock) {
			memmove(&held[i], &held[i + 1], (--held_count - i) * sizeof(*held));
			release_calls++;
			return;
		}
	UT_ASSERT(false);
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	for (unsigned i = 0; i < held_count; i++) callback(held[i], LW_EXCLUSIVE, context);
}

bool
rf_page_stable_base_proof_matches_ctrc_replays_v1(const RfPageStableBaseProofV1 *proof,
	const RfPageIdentityV1 *identity, const RfPageVersionV1 *version,
	const ClusterRecoveryCtrcReplaySetSourceV1 *sources, uint32 count,
	const RfPagePinnedSourceV1 *source, const RfContributorVectorV1 *vector)
{
	/* Authority validation itself executes in the separate original-set tests. */
	return proof == (const void *)&proof_storage && rf_page_identity_valid_v1(identity)
		&& version->mutation_token == 43 && sources == &native_source && count == 1
		&& source == &pinned_source && vector == &contributors;
}

/* Keep native PAGE and capacity branches; replace only the memory service. */
#define palloc0(bytes_) calloc(1, (bytes_))
#define pfree(pointer_) free((pointer_))
#include "../../backend/cluster/cluster_page_authority_ctrc.c"
#include "../../backend/cluster/cluster_page_guard.c"

static void
fixture(unsigned count, unsigned distinct, unsigned already_held)
{
	bool selected[RF_PAGE_GUARD_PARTITIONS] = {false};
	unsigned used = 0, filled = 0;
	RfPageIdentityV1 identity = {0};
	RfPageGuardPreflightV1 preflight;

	held_count = acquire_calls = release_calls = capacity_errors = 0; contended = NULL;
	memset(held, 0, sizeof(held)); memset(targets, 0, sizeof(targets));
	rf_page_guard_shmem_init_v1();
	for (unsigned i = 0; i < already_held; i++) held[held_count++] = &other_locks[i];
	identity.system_identifier = 99; memset(identity.storage_uuid, 3, 16);
	identity.locator.spcOid = 1; identity.locator.dbOid = 2; identity.locator.relNumber = 3;
	while (filled < count) {
		identity.blockno++;
		UT_ASSERT(rf_page_guard_preflight_v1(&identity, &preflight));
		if (!selected[preflight.partition]) {
			if (used == distinct) continue;
			selected[preflight.partition] = true; used++;
		} else if (distinct == count) continue;
		targets[filled].page_identity = identity;
		memset(targets[filled].expected_result.segment_incarnation, 1, 16);
		targets[filled].expected_result.mutation_token = 43;
		targets[filled].stable_base = (const void *)&proof_storage;
		targets[filled].source = &pinned_source; targets[filled].contributors = &contributors;
		filled++;
	}
	UT_ASSERT_EQ(used, distinct);
}

static RfPageCtrcAuthorityBuildV1
build(unsigned count)
{
	RfPageCtrcAuthorityBuildV1 result = rf_page_authority_ctrc_replays_preflight_v1(
		targets, count, &native_source, 1, 1024 * 1024);
	UT_ASSERT_EQ(result.verdict, RF_PAGE_AUTHORITY_OK);
	return result;
}

static RfPageAuthorityVerdictV1
promote(RfPageCtrcAuthorityV1 *authority)
{
	if (setjmp(capacity_error) != 0) return RF_PAGE_AUTHORITY_INTERNAL;
	return rf_page_authority_ctrc_replays_promote_nowait_v1(authority);
}

UT_TEST(test_native_page_batch_exceeding_capacity_takes_no_locks)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(LWLOCK_MAX_HELD_BY_PROC + 1, LWLOCK_MAX_HELD_BY_PROC + 1, 0);
	result = build(LWLOCK_MAX_HELD_BY_PROC + 1);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(capacity_errors, 0);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
}

UT_TEST(test_native_page_batch_at_capacity_promotes_and_retires)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(LWLOCK_MAX_HELD_BY_PROC, LWLOCK_MAX_HELD_BY_PROC, 0);
	result = build(LWLOCK_MAX_HELD_BY_PROC);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC);
	UT_ASSERT(rf_page_authority_ctrc_replays_covers_nowait_v1(result.authority,
		&targets[0].page_identity, &targets[0].expected_result));
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, 0); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(release_calls, LWLOCK_MAX_HELD_BY_PROC);
}

UT_TEST(test_native_page_remaining_capacity_waits_without_partial_grants)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(2, 2, LWLOCK_MAX_HELD_BY_PROC - 1); result = build(2);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 1);
	LWLockRelease(&other_locks[LWLOCK_MAX_HELD_BY_PROC - 2]);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 2);
}

UT_TEST(test_native_page_full_caller_capacity_is_retryable)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(1, 1, LWLOCK_MAX_HELD_BY_PROC); result = build(1);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC);
}

UT_TEST(test_native_page_capacity_counts_shared_partitions_once)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(LWLOCK_MAX_HELD_BY_PROC + 1, 2, LWLOCK_MAX_HELD_BY_PROC - 2);
	result = build(LWLOCK_MAX_HELD_BY_PROC + 1);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(acquire_calls, 2); UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 2);
	UT_ASSERT_EQ(capacity_errors, 0);
}

UT_TEST(test_native_page_contention_unwinds_prefix_with_capacity_held)
{
	RfPageCtrcAuthorityBuildV1 result; RfPageGuardPreflightV1 preflight;
	fixture(2, 2, LWLOCK_MAX_HELD_BY_PROC - 2); result = build(2);
	UT_ASSERT(rf_page_guard_preflight_v1(&targets[1].page_identity, &preflight));
	contended = &native_page_locks[preflight.partition].lock;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 2); UT_ASSERT_EQ(release_calls, 1);
	contended = NULL;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 2); UT_ASSERT_EQ(capacity_errors, 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_native_page_batch_exceeding_capacity_takes_no_locks);
	UT_RUN(test_native_page_batch_at_capacity_promotes_and_retires);
	UT_RUN(test_native_page_remaining_capacity_waits_without_partial_grants);
	UT_RUN(test_native_page_full_caller_capacity_is_retryable);
	UT_RUN(test_native_page_capacity_counts_shared_partitions_once);
	UT_RUN(test_native_page_contention_unwinds_prefix_with_capacity_held);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
