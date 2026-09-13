/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_hio_vm.h
 *    Real heap-allocation VM pin-lifetime test fixture.
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_hio_vm.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */

/* Real HIO pin/relock bodies. The controlled buffer boundary detects a
 * blocking acquire with passive VM pins, instead of hanging this test. */
static PGAlignedBlock hio_pages[4];
static int hio_refs[4];
static bool hio_locks[4];
static int hio_wait_pin_conflicts, hio_waits, hio_reads, hio_repin_failures;
static int hio_pair_misses;
static int hio_recent_calls, hio_fail_recent_at;
static bool hio_make_second_visible, hio_cancel;
static bool hio_same_vm, hio_enforce_wait_order;
static sigjmp_buf hio_jump;

static Buffer
hio_map(BlockNumber heap_block)
{
	return heap_block == 0 || hio_same_vm ? 3 : 4;
}

static void
hio_lock(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(hio_locks[buffer - 1]);
		hio_locks[buffer - 1] = false;
		return;
	}
	UT_ASSERT(!hio_locks[0] && !hio_locks[1]);
	hio_waits++;
	if (hio_enforce_wait_order && hio_refs[2] + hio_refs[3] != 0) {
		hio_wait_pin_conflicts++;
		siglongjmp(hio_jump, 1);
	}
	hio_locks[buffer - 1] = true;
	if (hio_make_second_visible)
		PageSetAllVisible(hio_pages[1].data);
}

static bool
hio_conditional(Buffer buffer)
{
	if (hio_pair_misses > 0) {
		hio_pair_misses--;
		return false;
	}
	UT_ASSERT(!hio_locks[buffer - 1]);
	hio_locks[buffer - 1] = true;
	return true;
}

static void
hio_release(Buffer buffer)
{
	UT_ASSERT(buffer >= 1 && buffer <= 4);
	UT_ASSERT(hio_refs[buffer - 1] > 0);
	hio_refs[buffer - 1]--;
}

static bool
hio_pin_ok(BlockNumber heap_block, Buffer vm)
{
	return vm == hio_map(heap_block) && hio_refs[vm - 1] > 0;
}

static void
hio_pin(Relation relation, BlockNumber heap_block, Buffer *vm)
{
	UT_ASSERT(!hio_locks[0] && !hio_locks[1]);
	if (hio_enforce_wait_order)
		UT_ASSERT_EQ(hio_refs[2] + hio_refs[3], 0);
	hio_reads++;
	if (hio_pin_ok(heap_block, *vm))
		return;
	if (BufferIsValid(*vm))
		hio_release(*vm);
	*vm = hio_map(heap_block);
	hio_refs[*vm - 1]++;
}

static bool
hio_recent(Relation relation, BlockNumber heap_block, Buffer recent, Buffer *vm)
{
	UT_ASSERT(hio_locks[heap_block]);
	UT_ASSERT(!BufferIsValid(*vm));
	hio_recent_calls++;
	if (hio_recent_calls == hio_fail_recent_at)
		return false;
	if (hio_repin_failures > 0) {
		hio_repin_failures--;
		return false;
	}
	if (recent != hio_map(heap_block))
		return false;
	*vm = recent;
	hio_refs[*vm - 1]++;
	return true;
}

#undef LockBuffer
#undef ReleaseBuffer
#undef BufferGetPage
#undef visibilitymap_pin
#undef visibilitymap_pin_recent
#define LockBuffer hio_lock
#define ReleaseBuffer hio_release
#define BufferGetPage(buffer) (hio_pages[(buffer) - 1].data)
#define ConditionalLockBuffer hio_conditional
#define visibilitymap_pin hio_pin
#define visibilitymap_pin_recent hio_recent
#define visibilitymap_pin_ok hio_pin_ok
#define cluster_storage_mode_enabled() hio_enforce_wait_order
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS()                                                                     \
	do {                                                                                           \
		if (hio_cancel)                                                                            \
			siglongjmp(hio_jump, 2);                                                               \
	} while (0)
#include "test_cluster_heap_hio_vm.inc"

static void
hio_reset(bool same_vm)
{
	int i;

	memset(hio_pages, 0, sizeof(hio_pages));
	memset(hio_refs, 0, sizeof(hio_refs));
	memset(hio_locks, 0, sizeof(hio_locks));
	hio_wait_pin_conflicts = hio_waits = hio_reads = hio_repin_failures = 0;
	hio_pair_misses = 0;
	hio_recent_calls = hio_fail_recent_at = 0;
	hio_make_second_visible = hio_cancel = false;
	hio_same_vm = same_vm;
	hio_enforce_wait_order = true;
	NBuffers = 4;
	relation_data.rd_rel = &relation_class;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	for (i = 0; i < 2; i++) {
		PageHeader page = (PageHeader)hio_pages[i].data;

		page->pd_lower = SizeOfPageHeaderData;
		page->pd_upper = page->pd_special = BLCKSZ;
	}
	PageSetAllVisible(hio_pages[0].data);
	PageSetAllVisible(hio_pages[1].data);
}

UT_TEST(actual_hio_single_relock_carries_no_vm_pin)
{
	Buffer vm = InvalidBuffer;
	bool completed = false;

	hio_reset(false);
	hio_locks[0] = true;
	if (sigsetjmp(hio_jump, 0) == 0) {
		(void)GetVisibilityMapPins(&relation_data, 1, InvalidBuffer, 0, InvalidBlockNumber, &vm,
								   NULL, InvalidBuffer);
		completed = true;
	}
	UT_ASSERT(completed);
	UT_ASSERT_EQ(hio_wait_pin_conflicts, 0);
	UT_ASSERT(hio_locks[0] && hio_pin_ok(0, vm));
}

UT_TEST(actual_hio_pair_relock_carries_neither_vm_alias)
{
	int same;

	for (same = 0; same < 2; same++) {
		Buffer first_vm = InvalidBuffer, second_vm = InvalidBuffer;
		bool completed = false;

		hio_reset(same != 0);
		hio_locks[0] = hio_locks[1] = true;
		hio_pair_misses = 1;
		if (sigsetjmp(hio_jump, 0) == 0) {
			(void)GetVisibilityMapPins(&relation_data, 2, 1, 1, 0, &second_vm, &first_vm,
									   InvalidBuffer);
			completed = true;
		}
		UT_ASSERT(completed);
		UT_ASSERT_EQ(hio_wait_pin_conflicts, 0);
		UT_ASSERT(hio_locks[0] && hio_locks[1]);
		UT_ASSERT(hio_pin_ok(0, first_vm) && hio_pin_ok(1, second_vm));
		UT_ASSERT_EQ(hio_refs[2] + hio_refs[3], 2);
	}
}

UT_TEST(actual_hio_repin_failure_releases_partial_result)
{
	int failed;

	for (failed = 1; failed <= 2; failed++) {
		Buffer first_vm = InvalidBuffer, second_vm = InvalidBuffer;

		hio_reset(false);
		hio_locks[0] = hio_locks[1] = true;
		hio_fail_recent_at = failed;
		UT_ASSERT(
			GetVisibilityMapPins(&relation_data, 1, 2, 0, 1, &first_vm, &second_vm, InvalidBuffer));
		UT_ASSERT(hio_locks[0] && hio_locks[1]);
		UT_ASSERT_EQ(hio_refs[2] + hio_refs[3], 2);
		UT_ASSERT(hio_reads >= 4 && hio_waits >= 2);
		UT_ASSERT_EQ(hio_wait_pin_conflicts, 0);
	}
}

UT_TEST(actual_hio_newly_visible_and_frozen_require_exact_maps)
{
	Buffer first_vm = InvalidBuffer, second_vm = InvalidBuffer;

	hio_reset(false);
	PageClearAllVisible(hio_pages[1].data);
	hio_locks[0] = hio_locks[1] = true;
	hio_make_second_visible = true;
	UT_ASSERT(
		GetVisibilityMapPins(&relation_data, 1, 2, 0, 1, &first_vm, &second_vm, InvalidBuffer));
	UT_ASSERT(hio_pin_ok(0, first_vm) && hio_pin_ok(1, second_vm));
	UT_ASSERT(hio_waits >= 2);

	hio_reset(false);
	PageClearAllVisible(hio_pages[0].data);
	first_vm = InvalidBuffer;
	hio_locks[0] = true;
	UT_ASSERT(GetVisibilityMapPins(&relation_data, InvalidBuffer, 1, InvalidBlockNumber, 0, NULL,
								   &first_vm, 1));
	UT_ASSERT(hio_pin_ok(0, first_vm));
	UT_ASSERT_EQ(hio_refs[2], 1);
}

UT_TEST(actual_hio_declared_pin_release_and_alias_counts)
{
	Buffer first_vm = 3, second_vm = 3;

	hio_reset(true);
	hio_refs[2] = 2;
	cluster_hio_release_vm_pins(&first_vm, &second_vm);
	UT_ASSERT_EQ(hio_refs[2], 0);
	UT_ASSERT(!BufferIsValid(first_vm) && !BufferIsValid(second_vm));
	first_vm = 3;
	hio_refs[2] = 1;
	cluster_hio_release_vm_pins(&first_vm, &first_vm);
	UT_ASSERT_EQ(hio_refs[2], 0);
	UT_ASSERT_EQ(first_vm, InvalidBuffer);
	hio_locks[0] = hio_locks[1] = true;
	UT_ASSERT(
		GetVisibilityMapPins(&relation_data, 1, 2, 0, 1, &first_vm, &first_vm, InvalidBuffer));
	UT_ASSERT_EQ(hio_refs[2], 1);
}

UT_TEST(actual_hio_disabled_local_and_cancellation_boundaries)
{
	Buffer vm = InvalidBuffer;
	int canceled;

	hio_reset(false);
	hio_enforce_wait_order = false;
	hio_locks[0] = true;
	UT_ASSERT(GetVisibilityMapPins(&relation_data, 1, InvalidBuffer, 0, InvalidBlockNumber, &vm,
								   NULL, InvalidBuffer));
	UT_ASSERT_EQ(hio_recent_calls, 0);
	UT_ASSERT(hio_pin_ok(0, vm));
	hio_reset(false);
	relation_class.relpersistence = RELPERSISTENCE_TEMP;
	UT_ASSERT(!cluster_hio_vm_repin_enabled(&relation_data));

	hio_reset(false);
	vm = 4; /* A previous candidate's map; this page now needs map3. */
	hio_refs[3] = 1;
	hio_locks[0] = true;
	hio_cancel = true;
	canceled = sigsetjmp(hio_jump, 0);
	if (canceled == 0)
		(void)GetVisibilityMapPins(&relation_data, 1, InvalidBuffer, 0, InvalidBlockNumber, &vm,
								   NULL, InvalidBuffer);
	UT_ASSERT_EQ(canceled, 2);
	UT_ASSERT(!hio_locks[0] && !hio_locks[1]);
	UT_ASSERT_EQ(hio_refs[2] + hio_refs[3], 0);
	UT_ASSERT_EQ(vm, InvalidBuffer);
}

/* Execute the entire real allocation consumer as well as its helpers. Only
 * physical reads, the FSM and extension are controlled runtime boundaries. */
static BlockNumber hio_target;
static int hio_data_reads, hio_extensions, hio_fsm_calls, hio_dirties;
static bool hio_extension_unlock;

static void
hio_assert_no_vm_wait(void)
{
	UT_ASSERT(!hio_locks[0] && !hio_locks[1]);
	if (hio_enforce_wait_order)
		UT_ASSERT_EQ(hio_refs[2] + hio_refs[3], 0);
}

static Buffer
hio_read(Relation relation, BlockNumber block)
{
	hio_assert_no_vm_wait();
	UT_ASSERT(block < 2);
	hio_data_reads++;
	hio_refs[block]++;
	return (Buffer)(block + 1);
}

static Buffer
hio_read_bi(Relation relation, BlockNumber block, ReadBufferMode mode, BulkInsertState state)
{
	UT_ASSERT_EQ(mode, RBM_NORMAL);
	return hio_read(relation, block);
}

static BlockNumber
hio_fsm(void)
{
	hio_assert_no_vm_wait();
	hio_fsm_calls++;
	return InvalidBlockNumber;
}

static Buffer
hio_extend(Relation relation, BulkInsertState state, int num_pages, bool use_fsm, bool *unlocked)
{
	PageHeader page = (PageHeader)hio_pages[1].data;

	hio_assert_no_vm_wait();
	hio_extensions++;
	hio_refs[1]++;
	page->pd_upper = page->pd_special = BLCKSZ;
	PageClearAllVisible((Page)page);
	*unlocked = hio_extension_unlock;
	hio_locks[1] = !*unlocked;
	return 2;
}

static void
hio_init(Page page, Size size, Size special)
{
	PageHeader header = (PageHeader)page;

	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = header->pd_special = size - special;
}

static void
hio_unlock_release(Buffer buffer)
{
	hio_lock(buffer, BUFFER_LOCK_UNLOCK);
	hio_release(buffer);
}

#undef RecordPageWithFreeSpace
#undef MarkBufferDirty
#undef RelationGetTargetBlock
#undef RelationSetTargetBlock
#undef BufferGetBlockNumber
#undef RelationGetNumberOfBlocks
#undef UnlockReleaseBuffer
#undef ReadBuffer
#define BufferGetBlockNumber(buffer) ((BlockNumber)((buffer) - 1))
#define ReadBuffer hio_read
#define ReadBufferBI hio_read_bi
#define RelationGetTargetBlock(relation) hio_target
#define RelationSetTargetBlock(relation, block) (hio_target = (block))
#define GetPageWithFreeSpace(relation, size) hio_fsm()
#define RecordAndGetPageWithFreeSpace(relation, block, size, target) hio_fsm()
#define RecordPageWithFreeSpace(relation, block, size) ((void)hio_fsm())
#define RelationGetNumberOfBlocks(relation) 2
#define RelationAddBlocks hio_extend
#define cluster_hw_lease_active() false
#define cluster_hio_lease_target_block(relation) InvalidBlockNumber
#define BufferGetPageSize(buffer) BLCKSZ
#define PageInitHeapPage hio_init
#define PageInit hio_init
#define MarkBufferDirty(buffer) (hio_dirties++)
#define UnlockReleaseBuffer hio_unlock_release
#undef elog
#define elog(level, ...) fixture_error()
#define RelationGetBufferForTuple hio_allocate
Buffer hio_allocate(Relation relation, Size len, Buffer other, int options, BulkInsertState state,
					Buffer *vm, Buffer *other_vm, int num_pages);
#include "test_cluster_heap_hio_allocate.inc"

UT_TEST(actual_hio_allocation_reads_and_locks_without_incoming_vm_pins)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		Buffer vm = 3, other_vm = 3, result;
		Buffer other = leg == 0 ? InvalidBuffer : 1;

		hio_reset(false);
		hio_target = leg == 2 ? 0 : 1;
		hio_data_reads = hio_fsm_calls = hio_extensions = hio_dirties = 0;
		hio_refs[2] = 2;
		hio_refs[0] = BufferIsValid(other) ? 1 : 0;
		hio_pair_misses = leg == 1 ? 1 : 0;
		result = hio_allocate(&relation_data, 80, other, 0, NULL, &vm, &other_vm, 0);
		UT_ASSERT_EQ(result, (Buffer)(hio_target + 1));
		UT_ASSERT(hio_locks[result - 1]);
		if (BufferIsValid(other))
			UT_ASSERT(hio_locks[other - 1] && hio_pin_ok(0, other_vm));
		else
			UT_ASSERT_EQ(other_vm, InvalidBuffer);
		UT_ASSERT(hio_pin_ok(hio_target, vm));
		UT_ASSERT_EQ(hio_extensions, 0);
		UT_ASSERT_EQ(hio_wait_pin_conflicts, 0);
	}
}

UT_TEST(actual_hio_rejected_candidate_and_frozen_extension_recheck_maps)
{
	int unlocked;

	for (unlocked = 0; unlocked < 2; unlocked++) {
		Buffer vm = 3, other_vm = 3, result;
		PageHeader full;

		hio_reset(false);
		hio_target = 1;
		hio_data_reads = hio_fsm_calls = hio_extensions = hio_dirties = 0;
		hio_refs[0] = 1;
		hio_refs[2] = 2;
		full = (PageHeader)hio_pages[1].data;
		full->pd_upper = full->pd_lower;
		hio_extension_unlock = unlocked != 0;
		hio_pair_misses = 1;
		result = hio_allocate(&relation_data, 80, 1, HEAP_INSERT_FROZEN, NULL, &vm, &other_vm, 1);
		UT_ASSERT_EQ(result, 2);
		UT_ASSERT_EQ(hio_extensions, 1);
		UT_ASSERT_EQ(hio_fsm_calls, 1);
		UT_ASSERT(hio_locks[0] && hio_locks[1]);
		UT_ASSERT(hio_pin_ok(0, other_vm) && hio_pin_ok(1, vm));
		UT_ASSERT(!PageIsAllVisible(hio_pages[1].data));
		UT_ASSERT_EQ(hio_wait_pin_conflicts, 0);
	}
}
