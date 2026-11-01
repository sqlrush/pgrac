/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Real selected-segment caller and local resource predicates. Resource arrays,
 * local lock iteration, admission and native scheduling are boundary inputs;
 * this does not acquire real pins/locks or execute a native deadlock graph.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN native_context_owner_fixture_main
#define TT_ACTIVE_OWNER_EXTERNAL_WAIT_SCOPE
#define TT_ACTIVE_OWNER_EXTERNAL_CONTEXT
#define slot_wait_in_native undo_cleaner_tx_slot_wait_active
#include "test_cluster_tt_active_owner.c"
#undef slot_wait_in_native
#undef TT_ACTIVE_OWNER_EXTERNAL_WAIT_SCOPE
#undef TT_ACTIVE_OWNER_EXTERNAL_CONTEXT
#include "lib/ilist.h"
#include "test_cluster_native_slot_scope.inc"

static struct {
	Buffer buffer;
	int32 refcount;
} PrivateRefCountArray[8];
static int32 PrivateRefCountOverflowed;
int NLocBuffer;
int32 *LocalRefCount;
static int32 context_local_pin;
static dlist_head current_active_guards = DLIST_STATIC_INIT(current_active_guards);
static dlist_head Block0OwnedResources = DLIST_STATIC_INIT(Block0OwnedResources);
static uint32 semantic_activation_local_inflight[2][64];
volatile uint32 InterruptHoldoffCount;
#include "test_cluster_undo_cleaner_holds.inc"

static LOCALLOCK context_locks[1];
static unsigned context_lock_count, context_scans;
static bool context_lwlock, context_late_lwlock;

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	context_scans++;
	if (context_lwlock || (context_late_lwlock && context_scans == 2))
		callback(NULL, LW_EXCLUSIVE, context);
}

HTAB *
GetLockMethodLocalHash(void)
{
	return (HTAB *)context_locks;
}

void
hash_seq_init(HASH_SEQ_STATUS *scan, HTAB *table)
{
	scan->hashp = table;
	scan->curBucket = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *scan)
{
	if (scan->curBucket >= context_lock_count)
		return NULL;
	return &context_locks[scan->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *scan pg_attribute_unused())
{
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return 100;
}

#include "test_cluster_native_slot_context.inc"

static void
clear_context_inputs(void)
{
	MyProc = &g_fake_proc;
	MyProc->backendId = 1;
	MyProc->lxid = 2;
	MyBackendType = B_BACKEND;
	CritSectionCount = InterruptHoldoffCount = 0;
	MemSet(PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
	PrivateRefCountOverflowed = NLocBuffer = context_local_pin = 0;
	LocalRefCount = NULL;
	dlist_init(&current_active_guards);
	dlist_init(&Block0OwnedResources);
	MemSet(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	MemSet(context_locks, 0, sizeof(context_locks));
	context_lock_count = context_scans = 0;
	context_lwlock = context_late_lwlock = false;
}

static void
start_context_test(void)
{
	clear_context_inputs();
	start_full_active_test();
}

static void
require_context_refusal(unsigned lookups)
{
	ClusterTTSlotAllocEntry before[48];
	uint64 binding = allocator_storage.per_node[0].binding_generation;

	memcpy(before, allocator_storage.per_node[0].slots, sizeof(before));
	require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(slot_wait_lookups, lookups);
	UT_ASSERT_EQ(slot_wait_calls + cold_selection_calls, 0);
	UT_ASSERT_EQ(memcmp(before, allocator_storage.per_node[0].slots, sizeof(before)), 0);
	UT_ASSERT_EQ(allocator_storage.per_node[0].binding_generation, binding);
	UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
	UT_ASSERT(!cluster_undo_cleaner_tx_slot_waiting());
	assert_active_refusal_stats(1, 0);
}

static void
require_context_retry(uint64 refusals)
{
	ClusterUndoCapacityStats stats;
	ClusterTTSlotAllocEntry others[47];

	memcpy(others, &allocator_storage.per_node[0].slots[1], sizeof(others));
	clear_context_inputs();
	require_published_on(1);
	UT_ASSERT_EQ(slot_wait_calls, 1);
	UT_ASSERT_EQ(cold_selection_calls, 0);
	UT_ASSERT_EQ(memcmp(others, &allocator_storage.per_node[0].slots[1], sizeof(others)), 0);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.refused_context_count, refusals);
	UT_ASSERT_EQ(stats.refused_proof_count, 0);
	UT_ASSERT_EQ(stats.active_full_wait_count, 1);
	UT_ASSERT_EQ(stats.native_wakeup_recheck_count, 1);
	UT_ASSERT_EQ(stats.pool_wait_count + stats.cancel_count, 0);
	UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
	UT_ASSERT(!cluster_undo_cleaner_tx_slot_waiting());
}

UT_TEST(test_blocking_resources_refuse_before_native_wait_and_allow_release_retry)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		dlist_node resource;

		start_context_test();
		switch (fault) {
		case 0: CritSectionCount = 1; break;
		case 1: InterruptHoldoffCount = 1; break;
		case 2: context_lwlock = true; break;
		case 3: PrivateRefCountArray[7].refcount = 1; break;
		case 4: PrivateRefCountOverflowed = 1; break;
		case 5:
			NLocBuffer = context_local_pin = 1;
			LocalRefCount = &context_local_pin;
			break;
		case 6: NLocBuffer = 1; break;
		case 7: dlist_push_head(&current_active_guards, &resource); break;
		case 8: dlist_push_head(&Block0OwnedResources, &resource); break;
		case 9: semantic_activation_local_inflight[0][0] = 1; break;
		case 10: semantic_activation_local_inflight[1][63] = 1; break;
		case 11: MyBackendType = B_UNDO_CLEANER; break;
		case 12: MyProc = NULL; break;
		case 13: PrivateRefCountArray[0].refcount = -1; break;
		}
		require_context_refusal(0);
		require_context_retry(1);
	}
}

UT_TEST(test_heavyweight_identity_and_mode_gate_the_full_slot_caller)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		LOCALLOCK *lock = &context_locks[0];
		VirtualTransactionId vxid;
		bool allowed = fault < 3 || fault == 8 || fault == 10 || fault == 16;

		start_context_test();
		context_lock_count = 1;
		lock->nLocks = 1;
		/* The real gate must never dereference these shared pointers. */
		lock->lock = (LOCK *)(uintptr_t)1;
		lock->proclock = (PROCLOCK *)(uintptr_t)1;
		SET_LOCKTAG_RELATION(lock->tag.lock, 5, 123);
		lock->tag.mode = fault < 8 ? (LOCKMODE)(fault + 1) : ExclusiveLock;
		if (fault == 8 || fault == 9)
			SET_LOCKTAG_TRANSACTION(lock->tag.lock, fault == 8 ? 100 : 101);
		else if (fault == 10 || fault == 11) {
			GET_VXID_FROM_PGPROC(vxid, *MyProc);
			if (fault == 11)
				vxid.localTransactionId++;
			SET_LOCKTAG_VIRTUALTRANSACTION(lock->tag.lock, vxid);
		} else if (fault == 12)
			lock->tag.lock.locktag_type = LOCKTAG_ADVISORY;
		else if (fault == 13) {
			lock->tag.mode = AccessShareLock;
			lock->tag.lock.locktag_lockmethodid = USER_LOCKMETHOD;
		} else if (fault == 14) {
			lock->tag.mode = AccessShareLock;
			lock->holdsStrongLockCount = true;
		} else if (fault == 15)
			lock->nLocks = -1;
		else if (fault == 16)
			lock->nLocks = 0;
		if (!allowed) {
			require_context_refusal(0);
			require_context_retry(1);
		} else {
			/* Keep the permitted lock present through both real checks. */
			require_published_on(1);
			UT_ASSERT_EQ(slot_wait_calls, 1);
			UT_ASSERT_EQ(context_scans, 2);
			UT_ASSERT_EQ(g_bind_emit_calls, 1);
		}
	}
}

UT_TEST(test_resource_appearing_after_owner_lookup_prevents_native_wait)
{
	start_context_test();
	context_late_lwlock = true;
	require_context_refusal(1);
	UT_ASSERT_EQ(context_scans, 2);
	require_context_retry(1);
	UT_ASSERT_EQ(slot_wait_lookups, 2);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_blocking_resources_refuse_before_native_wait_and_allow_release_retry);
	UT_RUN(test_heavyweight_identity_and_mode_gate_the_full_slot_caller);
	UT_RUN(test_resource_appearing_after_owner_lookup_prevents_native_wait);
	UT_DONE();
	return ut_failed_count != 0;
}
