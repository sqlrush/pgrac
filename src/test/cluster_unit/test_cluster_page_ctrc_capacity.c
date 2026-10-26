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
 *     Compile the complete product CUs without USE_CLUSTER_UNIT. Only the
 *     allocator, graph-proof leaf and LWLock service are controlled. Keep
 *     original WALR read-pin/owner/shared-snapshot checks, including their
 *     transient LW_SHARED acquisition while all PAGE locks remain held. The
 *     service models the original held-list ceiling and nonlocal ERROR; it
 *     does not use the PAGE primitive's boolean unit partition branch.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include "cluster/cluster_epoch.h"
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
static unsigned snapshot_calls, snapshot_releases, peak_held;
static LWLock *snapshot_lock;
static bool exit_owner_after_pages;
static void capacity_owner_exit(void);
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
	if (exit_owner_after_pages && acquire_calls == 2) capacity_owner_exit();
	return true;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == snapshot_lock); UT_ASSERT_EQ(mode, LW_SHARED);
	snapshot_calls++;
	if (held_count >= LWLOCK_MAX_HELD_BY_PROC) {
		capacity_errors++;
		longjmp(capacity_error, 1);
	}
	held[held_count++] = lock;
	if (held_count > peak_held) peak_held = held_count;
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
			if (lock == snapshot_lock) snapshot_releases++;
			else release_calls++;
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
		&& source == &pinned_source && vector == &contributors
		&& cluster_wal_read_pin_owner_current_v1(&sources[0].input_owner);
}

bool cluster_shared_config = true;
ResourceOwner CurrentResourceOwner;
uint64 cluster_epoch_get_current(void) { return 7; }
uint64 cluster_grd_redeclare_generation(void) { return 11; }

/* Retain the complete original path from WALR custody through owner lookup
 * to the shared snapshot. Backing state and lock service are fixture leaves. */
#include "../../backend/cluster/cluster_control_request.c"
#include "../../backend/cluster/cluster_lock_owner.c"
#include "../../backend/cluster/cluster_wal_retention.c"
static ControlRequestShared native_requests;
static ClusterLockOwner native_owner;
static PGAlignedBlock native_pin_storage;

static void
capacity_owner_exit(void)
{
	native_requests.records[0].owner_exited = true;
}

static void
owner_fixture(void)
{
	ClusterWalReadPinV1 *pin = (void *)&native_pin_storage;
	ClusterControlRequestView *record = &native_requests.records[0];

	memset(&native_requests, 0, sizeof(native_requests));
	memset(&native_owner, 0, sizeof(native_owner));
	memset(&native_pin_storage, 0, sizeof(native_pin_storage));
	memset(&native_source, 0, sizeof(native_source));
	control_requests = &native_requests; snapshot_lock = &native_requests.lock;
	native_owner.shared = true; native_owner.state = CLUSTER_LOCK_OWNER_HELD;
	native_owner.generation = 11;
	native_owner.request.control_owner_id = 1; native_owner.request.request_id = 101;
	native_owner.request.lockmode = ShareLock;
	native_owner.request.resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	native_owner.request.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	native_owner.request.resid.field1 = 1;
	native_owner.request.holder.node_id = 1; native_owner.request.holder.procno = 2;
	native_owner.request.holder.cluster_epoch = 7;
	native_owner.request.holder.request_id = 101;
	native_owner.attempt.slot = 0; native_owner.attempt.generation = 1;
	private_owners = &native_owner;
	record->handle = native_owner.attempt; record->state = CLUSTER_CONTROL_REQUEST_HELD;
	record->mode = ShareLock; record->message.key.resid = native_owner.request.resid;
	record->message.key.holder = native_owner.request.holder;
	Assert(offsetof(ClusterWalReadPinV1, locks) + sizeof(pin->locks[0]) <= sizeof(native_pin_storage));
	CurrentResourceOwner = (ResourceOwner)&native_owner;
	pin->magic = CLUSTER_WAL_READ_PIN_MAGIC; pin->generation = 1;
	pin->owner_pid = MyProcPid; pin->owner = CurrentResourceOwner; pin->nthreads = 1;
	pin->locks[0].held = true; pin->locks[0].request = native_owner.request;
	active_read_pin = pin;
	UT_ASSERT(cluster_wal_read_pin_owner_v1(pin, &native_source.input_owner));
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
	exit_owner_after_pages = false;
	memset(held, 0, sizeof(held)); memset(targets, 0, sizeof(targets));
	owner_fixture();
	snapshot_calls = snapshot_releases = peak_held = 0;
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

UT_TEST(test_native_page_batch_leaves_room_for_original_owner_snapshot)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(199, 199, 0);
	result = build(199);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(held_count, 199); UT_ASSERT_EQ(peak_held, 200);
	UT_ASSERT(rf_page_authority_ctrc_replays_covers_nowait_v1(result.authority,
		&targets[0].page_identity, &targets[0].expected_result));
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, 0); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(release_calls, 199); UT_ASSERT(snapshot_calls > 0);
	UT_ASSERT_EQ(snapshot_calls, snapshot_releases);
}

UT_TEST(test_native_page_remaining_capacity_waits_without_partial_grants)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(2, 2, LWLOCK_MAX_HELD_BY_PROC - 1); result = build(2);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 1);
	LWLockRelease(&other_locks[LWLOCK_MAX_HELD_BY_PROC - 2]);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	LWLockRelease(&other_locks[LWLOCK_MAX_HELD_BY_PROC - 3]);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC - 3);
}

UT_TEST(test_native_page_full_caller_capacity_is_retryable)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(1, 1, 199); result = build(1);
	held[held_count++] = &other_locks[199];
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, LWLOCK_MAX_HELD_BY_PROC);
}

UT_TEST(test_native_page_capacity_counts_shared_partitions_once)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(LWLOCK_MAX_HELD_BY_PROC + 1, 2, 197);
	result = build(LWLOCK_MAX_HELD_BY_PROC + 1);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(acquire_calls, 2); UT_ASSERT_EQ(held_count, 199);
	UT_ASSERT_EQ(peak_held, 200);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, 197);
	UT_ASSERT_EQ(capacity_errors, 0);
}

UT_TEST(test_native_page_contention_unwinds_prefix_with_capacity_held)
{
	RfPageCtrcAuthorityBuildV1 result; RfPageGuardPreflightV1 preflight;
	fixture(2, 2, 197); result = build(2);
	UT_ASSERT(rf_page_guard_preflight_v1(&targets[1].page_identity, &preflight));
	contended = &native_page_locks[preflight.partition].lock;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(held_count, 197); UT_ASSERT_EQ(release_calls, 1);
	contended = NULL;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, 197); UT_ASSERT_EQ(capacity_errors, 0);
}

UT_TEST(test_native_page_two_hundred_partitions_refuse_before_first_page)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(200, 200, 0); result = build(200);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(held_count, 0);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT(cluster_lock_owner_request_usable(&native_owner.request));
}

UT_TEST(test_native_page_single_partition_keeps_one_snapshot_slot_with_caller)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(1, 1, 199); result = build(1);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(capacity_errors, 0);
	LWLockRelease(&other_locks[198]);
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(held_count, 199); UT_ASSERT_EQ(peak_held, 200);
	UT_ASSERT(rf_page_authority_ctrc_replays_revalidate_nowait_v1(result.authority) == RF_PAGE_AUTHORITY_OK);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(held_count, 198); UT_ASSERT_EQ(snapshot_calls, snapshot_releases);
	UT_ASSERT_EQ(native_requests.records[0].state, CLUSTER_CONTROL_REQUEST_HELD);
}

UT_TEST(test_native_page_original_snapshot_refuses_exited_owner_before_page)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(1, 1, 198); result = build(1);
	native_requests.records[0].owner_exited = true;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_NO_STABLE_BASE);
	UT_ASSERT_EQ(acquire_calls, 0); UT_ASSERT_EQ(held_count, 198);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(native_owner.state, CLUSTER_LOCK_OWNER_HELD);
}

UT_TEST(test_native_page_original_snapshot_rejects_after_last_page_and_unwinds)
{
	RfPageCtrcAuthorityBuildV1 result;
	fixture(2, 2, 197); result = build(2); exit_owner_after_pages = true;
	UT_ASSERT_EQ(promote(result.authority), RF_PAGE_AUTHORITY_NO_STABLE_BASE);
	UT_ASSERT_EQ(acquire_calls, 2); UT_ASSERT_EQ(release_calls, 2);
	UT_ASSERT_EQ(held_count, 197); UT_ASSERT_EQ(capacity_errors, 0);
	UT_ASSERT_EQ(snapshot_calls, snapshot_releases); UT_ASSERT_EQ(peak_held, 200);
	rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	UT_ASSERT_EQ(native_requests.records[0].state, CLUSTER_CONTROL_REQUEST_HELD);
	UT_ASSERT_EQ(native_owner.state, CLUSTER_LOCK_OWNER_HELD);
}

UT_TEST(test_native_fpi_promotion_requires_one_exact_page_and_no_caller_locks)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		RfPageCtrcAuthorityBuildV1 result;
		RfPageIdentityV1 identity;
		RfPageVersionV1 before;

		fixture(fault == 0 ? 2 : 1, fault == 0 ? 2 : 1, fault == 3 ? 1 : 0);
		result = build(fault == 0 ? 2 : 1);
		identity = targets[0].page_identity; before = targets[0].expected_result;
		if (fault == 1) identity.blockno++;
		if (fault == 2) before.mutation_token++;
		UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_fpi_nowait_v1(result.authority, &identity, &before),
			fault < 3 ? RF_PAGE_AUTHORITY_INVALID_ARGUMENT : fault == 3 ? RF_PAGE_AUTHORITY_WOULD_BLOCK : RF_PAGE_AUTHORITY_OK);
		UT_ASSERT_EQ(acquire_calls, fault == 4 ? 1 : 0);
		UT_ASSERT_EQ(capacity_errors, 0);
		if (fault == 4) UT_ASSERT_EQ(peak_held, 2);
		rf_page_authority_ctrc_replays_destroy_v1(&result.authority);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_native_page_batch_exceeding_capacity_takes_no_locks);
	UT_RUN(test_native_page_batch_leaves_room_for_original_owner_snapshot);
	UT_RUN(test_native_page_remaining_capacity_waits_without_partial_grants);
	UT_RUN(test_native_page_full_caller_capacity_is_retryable);
	UT_RUN(test_native_page_capacity_counts_shared_partitions_once);
	UT_RUN(test_native_page_contention_unwinds_prefix_with_capacity_held);
	UT_RUN(test_native_page_two_hundred_partitions_refuse_before_first_page);
	UT_RUN(test_native_page_single_partition_keeps_one_snapshot_slot_with_caller);
	UT_RUN(test_native_page_original_snapshot_refuses_exited_owner_before_page);
	UT_RUN(test_native_page_original_snapshot_rejects_after_last_page_and_unwinds);
	UT_RUN(test_native_fpi_promotion_requires_one_exact_page_and_no_caller_locks);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
