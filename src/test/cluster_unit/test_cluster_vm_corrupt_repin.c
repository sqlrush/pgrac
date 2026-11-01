/* Complete original corruption collector and raw VM reader. Heap I/O, local
 * recent-pin admission, tuple classifiers, allocation and cancellation are
 * controlled leaves; this is not ResourceOwner, holder ACK or durability.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "storage/procarray.h"

int vm_corrupt_inherited_main(void);
#define main vm_corrupt_inherited_main
#include "test_cluster_vm_read_visibility.c"
#undef main

static RelationData corrupt_relation;
static FormData_pg_class corrupt_form;
static PGAlignedBlock corrupt_heap[4];
static BlockNumber corrupt_nblocks, corrupt_block;
static unsigned corrupt_pins, corrupt_reads, corrupt_unlocks, corrupt_checks;
static unsigned corrupt_recents, corrupt_refusals, corrupt_av_calls, corrupt_af_calls;
static unsigned corrupt_cancel_at, corrupt_error_at, corrupt_bounds;
static TransactionId corrupt_new_bound, corrupt_visible_at;
static bool corrupt_locked, corrupt_clear_on_refusal, corrupt_needs_freeze;
static int corrupt_change_pair;
static void *corrupt_allocations[2];
static unsigned corrupt_allocated;
static sigjmp_buf corrupt_jump;

static void
corrupt_no_vm_pin(void)
{
	if (cluster_heap_maintenance_restricted(&corrupt_relation))
		UT_ASSERT_EQ(corrupt_pins, 0);
}

static void
corrupt_interrupt(void)
{
	corrupt_no_vm_pin();
	UT_ASSERT(!corrupt_locked);
	if (++corrupt_checks == corrupt_cancel_at)
		siglongjmp(corrupt_jump, 1);
}

static uint8
corrupt_raw(Relation rel, BlockNumber block, Buffer *buffer)
{
	bool had_pin = BufferIsValid(*buffer);
	uint8 status;

	/* A recheck below heap content must already own the exact local pin. */
	if (corrupt_locked)
		UT_ASSERT(had_pin);
	status = visibilitymap_get_status(rel, block, buffer);
	if (!had_pin && BufferIsValid(*buffer))
		corrupt_pins++;
	return status;
}

static void
corrupt_release(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(corrupt_pins, 1);
	if (corrupt_pins > 0)
		corrupt_pins--;
	ReleaseBuffer(buffer);
}

static Buffer
corrupt_read_heap(Relation rel, ForkNumber fork, BlockNumber block,
				  ReadBufferMode mode, BufferAccessStrategy strategy pg_attribute_unused())
{
	corrupt_no_vm_pin();
	UT_ASSERT(rel == &corrupt_relation && fork == MAIN_FORKNUM && mode == RBM_NORMAL);
	UT_ASSERT(block < corrupt_nblocks && !corrupt_locked);
	corrupt_block = block;
	if (++corrupt_reads == corrupt_error_at)
		siglongjmp(corrupt_jump, 1);
	if (corrupt_change_pair >= 0 && corrupt_pins == 0) {
		uint8 *map = (uint8 *)PageGetContents(disk_vm.data);

		*map = (*map & ~(VISIBILITYMAP_VALID_BITS << (block * 2)))
			| (corrupt_change_pair << (block * 2));
		corrupt_change_pair = -1;
	}
	return 2;
}

static bool pg_attribute_unused()
corrupt_recent(RelFileLocator locator, ForkNumber fork, BlockNumber block, Buffer recent)
{
	UT_ASSERT(RelFileLocatorEquals(locator, corrupt_relation.rd_locator));
	UT_ASSERT(fork == VISIBILITYMAP_FORKNUM && block == 0 && recent == 1);
	UT_ASSERT(corrupt_locked && corrupt_pins == 0);
	/* Fixture runaway guard only, not a product retry or waiting budget. */
	if (++corrupt_recents > 32)
		abort();
	if (corrupt_refusals) {
		corrupt_refusals--;
		if (corrupt_clear_on_refusal)
			PageGetContents(disk_vm.data)[0] = 0;
		return false;
	}
	corrupt_pins++;
	reader_vm = disk_vm;
	return true;
}

static void
corrupt_lock(Buffer buffer, int mode)
{
	corrupt_no_vm_pin();
	UT_ASSERT(buffer == 2 && mode == BUFFER_LOCK_SHARE && !corrupt_locked);
	corrupt_locked = true;
}

static Page
corrupt_page(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && corrupt_locked);
	return corrupt_heap[corrupt_block].data;
}

static void
corrupt_unlock(Buffer buffer)
{
	corrupt_no_vm_pin();
	UT_ASSERT(buffer == 2 && corrupt_locked);
	corrupt_locked = false;
	corrupt_unlocks++;
}

static void
corrupt_close(Relation rel, LOCKMODE mode)
{
	UT_ASSERT(rel == &corrupt_relation && mode == AccessShareLock);
	UT_ASSERT_EQ(corrupt_pins, 0);
	UT_ASSERT(!corrupt_locked);
}

static TransactionId
corrupt_oldest(Relation rel)
{
	UT_ASSERT(rel == &corrupt_relation);
	return ++corrupt_bounds == 1 ? 50 : corrupt_new_bound;
}

static bool
corrupt_precedes(TransactionId first, TransactionId second)
{
	/* The controlled horizon leaf supplies only normal transaction IDs. */
	UT_ASSERT(TransactionIdIsNormal(first) && TransactionIdIsNormal(second));
	return NormalTransactionIdPrecedes(first, second);
}

static bool
corrupt_visible(HeapTuple tuple, TransactionId oldest, Buffer buffer)
{
	corrupt_no_vm_pin();
	UT_ASSERT(buffer == 2 && corrupt_locked);
	UT_ASSERT(ItemPointerGetBlockNumber(&tuple->t_self) == corrupt_block);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_self), 1);
	corrupt_av_calls++;
	return oldest >= corrupt_visible_at;
}

static bool
corrupt_freeze(HeapTupleHeader header pg_attribute_unused())
{
	corrupt_no_vm_pin();
	UT_ASSERT(corrupt_locked);
	corrupt_af_calls++;
	return corrupt_needs_freeze;
}

static void *
corrupt_allocate(Size size)
{
	void *memory;

	UT_ASSERT(corrupt_allocated < 2);
	memory = calloc(1, size);
	UT_ASSERT_NOT_NULL(memory);
	corrupt_allocations[corrupt_allocated++] = memory;
	return memory;
}

static void *
corrupt_reallocate(void *memory, Size size)
{
	UT_ASSERT(memory == corrupt_allocations[1]);
	corrupt_allocations[1] = realloc(memory, size);
	UT_ASSERT_NOT_NULL(corrupt_allocations[1]);
	return corrupt_allocations[1];
}

#define relation_open(oid, mode) ((void)(oid), (void)(mode), &corrupt_relation)
#define relation_close corrupt_close
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(rel) ((void)(rel), corrupt_nblocks)
#define GetAccessStrategy(kind) ((void)(kind), (BufferAccessStrategy)1)
#define GetOldestNonRemovableTransactionId corrupt_oldest
#define TransactionIdPrecedes corrupt_precedes
#define tuple_all_visible corrupt_visible
#define heap_tuple_needs_eventual_freeze corrupt_freeze
#define palloc0 corrupt_allocate
#define palloc corrupt_allocate
#define repalloc corrupt_reallocate
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() corrupt_interrupt()
#define visibilitymap_get_status corrupt_raw
#undef ReleaseBuffer
#define ReleaseBuffer corrupt_release
#define ReadBufferExtended corrupt_read_heap
#define ReadRecentBuffer corrupt_recent
#undef LockBuffer
#define LockBuffer corrupt_lock
#undef BufferGetPage
#define BufferGetPage corrupt_page
#define UnlockReleaseBuffer corrupt_unlock
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#ifndef CORRUPT_REPIN_FIXTURE
#define CORRUPT_REPIN_FIXTURE "test_cluster_vm_corrupt_repin.inc"
#endif
#include CORRUPT_REPIN_FIXTURE

static void
corrupt_reset(bool shared, bool local, uint8 bits, bool normal)
{
	for (unsigned i = 0; i < corrupt_allocated; i++)
		free(corrupt_allocations[i]);
	corrupt_allocated = 0;
	memset(&corrupt_relation, 0, sizeof(corrupt_relation));
	memset(&corrupt_form, 0, sizeof(corrupt_form));
	corrupt_relation.rd_rel = &corrupt_form;
	corrupt_relation.rd_id = 101;
	corrupt_relation.rd_locator = (RelFileLocator){1663, 42, 909};
	corrupt_form.relkind = RELKIND_RELATION;
	corrupt_form.relpersistence = local ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	fixture_shared = shared;
	fixture_reads = fixture_releases = 0;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 1;
	memset(&disk_vm, 0, sizeof(disk_vm));
	((PageHeader)disk_vm.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_vm.data)->pd_upper = BLCKSZ;
	PageGetContents(disk_vm.data)[0] = bits;
	for (int i = 0; i < 4; i++) {
		ItemId item;

		memset(&corrupt_heap[i], 0, sizeof(corrupt_heap[i]));
		((PageHeader)corrupt_heap[i].data)->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
		((PageHeader)corrupt_heap[i].data)->pd_upper = BLCKSZ;
		item = PageGetItemId(corrupt_heap[i].data, 1);
		if (normal)
			ItemIdSetNormal(item, BLCKSZ - MAXALIGN(SizeofHeapTupleHeader), SizeofHeapTupleHeader);
		else
			ItemIdSetDead(item);
	}
	corrupt_nblocks = 4;
	corrupt_pins = corrupt_reads = corrupt_unlocks = corrupt_checks = 0;
	corrupt_recents = corrupt_refusals = corrupt_av_calls = corrupt_af_calls = 0;
	corrupt_cancel_at = corrupt_error_at = corrupt_bounds = 0;
	corrupt_new_bound = 50;
	corrupt_visible_at = 1;
	corrupt_locked = corrupt_clear_on_refusal = corrupt_needs_freeze = false;
	corrupt_change_pair = -1;
}

UT_TEST(shared_two_phase_check_retires_both_vm_pins)
{
	corrupt_items *items;

	corrupt_reset(true, false, 0xff, false);
	items = collect_corrupt_items(101, true, true);
	UT_ASSERT_EQ(items->count, 4);
	for (unsigned i = 0; i < 4; i++) {
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&items->tids[i]), i);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&items->tids[i]), 1);
	}
	UT_ASSERT_EQ(corrupt_reads, 4);
	UT_ASSERT_EQ(corrupt_recents, 4);
	UT_ASSERT_EQ(fixture_releases, 8);
}

UT_TEST(shared_zero_bits_skip_without_retained_vm_pin)
{
	corrupt_reset(true, false, 0, false);
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 0);
	UT_ASSERT_EQ(corrupt_reads, 0);
	UT_ASSERT_EQ(corrupt_recents, 0);
	UT_ASSERT_EQ(fixture_releases, 4);
}

UT_TEST(cleared_pair_is_rechecked_after_heap_wait)
{
	corrupt_reset(true, false, 3, false);
	corrupt_nblocks = 1;
	corrupt_change_pair = 0;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 0);
	UT_ASSERT_EQ(corrupt_recents, 1);
	UT_ASSERT_EQ(corrupt_unlocks, 1);
}

UT_TEST(local_repin_refusal_releases_heap_before_same_page_retry)
{
	corrupt_reset(true, false, 3, false);
	corrupt_nblocks = 1;
	corrupt_refusals = 2;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 1);
	UT_ASSERT_EQ(corrupt_reads, 3);
	UT_ASSERT_EQ(corrupt_recents, 3);
	UT_ASSERT_EQ(corrupt_unlocks, 3);
	UT_ASSERT_EQ(corrupt_checks, 3);
}

UT_TEST(refused_repin_does_not_keep_previous_check_flags)
{
	corrupt_reset(true, false, 3, false);
	corrupt_nblocks = 1;
	corrupt_refusals = 1;
	corrupt_clear_on_refusal = true;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 0);
	UT_ASSERT_EQ(corrupt_reads, 1);
	UT_ASSERT_EQ(corrupt_recents, 1);
	UT_ASSERT_EQ(corrupt_checks, 2);
}

UT_TEST(repin_retry_can_cancel_with_no_heap_or_vm_pin)
{
	corrupt_reset(true, false, 3, false);
	corrupt_refusals = 4;
	corrupt_cancel_at = 3;
	if (sigsetjmp(corrupt_jump, 1) == 0) {
		(void)collect_corrupt_items(101, true, true);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(corrupt_checks, 3);
	UT_ASSERT_EQ(corrupt_pins, 0);
	UT_ASSERT_EQ(corrupt_unlocks, 2);
	UT_ASSERT(!corrupt_locked);
}

UT_TEST(heap_read_error_arrives_after_vm_pin_retirement)
{
	corrupt_reset(true, false, 3, false);
	corrupt_error_at = 1;
	if (sigsetjmp(corrupt_jump, 1) == 0) {
		(void)collect_corrupt_items(101, true, true);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(corrupt_pins, 0);
	UT_ASSERT_EQ(corrupt_recents, 0);
	UT_ASSERT(!corrupt_locked);
}

UT_TEST(native_and_local_preserve_original_pin_cache_and_tids)
{
	for (int local = 0; local < 2; local++) {
		corrupt_reset(local, local, 0xff, false);
		UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 4);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_releases, 1);
		UT_ASSERT_EQ(corrupt_recents, 0);
	}
}

UT_TEST(raw_af_only_missing_and_empty_keep_original_diagnostics)
{
	corrupt_items *items;

	corrupt_reset(true, false, 0x80, false);
	items = collect_corrupt_items(101, false, true);
	UT_ASSERT_EQ(items->count, 1);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&items->tids[0]), 3);
	corrupt_reset(true, false, 0xff, false);
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 0);
	UT_ASSERT_EQ(corrupt_reads, 0);
	UT_ASSERT_EQ(fixture_reads, 0);
	corrupt_reset(true, false, 0xff, false);
	corrupt_nblocks = 0;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 0);
	UT_ASSERT_EQ(fixture_reads, 0);
}

UT_TEST(recheck_preserves_independent_visible_and_frozen_flags)
{
	for (int pair = 1; pair <= 2; pair++) {
		corrupt_reset(true, false, 3, true);
		corrupt_nblocks = 1;
		corrupt_change_pair = pair;
		corrupt_visible_at = 100;
		corrupt_needs_freeze = true;
		UT_ASSERT_EQ(collect_corrupt_items(101, true, true)->count, 1);
		UT_ASSERT_EQ(corrupt_av_calls, pair == 1);
		UT_ASSERT_EQ(corrupt_af_calls, pair == 2);
	}
}

UT_TEST(original_oldest_xmin_recompute_runs_without_vm_pin)
{
	corrupt_reset(true, false, 1, true);
	corrupt_nblocks = 1;
	corrupt_visible_at = corrupt_new_bound = 60;
	UT_ASSERT_EQ(collect_corrupt_items(101, true, false)->count, 0);
	UT_ASSERT_EQ(corrupt_av_calls, 2);
	UT_ASSERT_EQ(corrupt_bounds, 2);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(shared_two_phase_check_retires_both_vm_pins);
	UT_RUN(shared_zero_bits_skip_without_retained_vm_pin);
	UT_RUN(cleared_pair_is_rechecked_after_heap_wait);
	UT_RUN(local_repin_refusal_releases_heap_before_same_page_retry);
	UT_RUN(refused_repin_does_not_keep_previous_check_flags);
	UT_RUN(repin_retry_can_cancel_with_no_heap_or_vm_pin);
	UT_RUN(heap_read_error_arrives_after_vm_pin_retirement);
	UT_RUN(native_and_local_preserve_original_pin_cache_and_tids);
	UT_RUN(raw_af_only_missing_and_empty_keep_original_diagnostics);
	UT_RUN(recheck_preserves_independent_visible_and_frozen_flags);
	UT_RUN(original_oldest_xmin_recompute_runs_without_vm_pin);
	UT_DONE();
	return ut_failed_count != 0;
}
