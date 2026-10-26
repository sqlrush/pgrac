/*-------------------------------------------------------------------------
 * test_cluster_heap_lock_vm_pin.c
 *    Original row-lock acquisition must not obstruct VM current transfer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Actual row-lock/rebind, heap repin and VM recent-pin bodies; current image
 * arrival, resident lookup and pin/I/O boundaries are controlled leaves.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/visibilitymap.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "../../backend/access/heap/heapam_r4_private.h"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifdef FIXTURE_NO_ASSERTS
#undef Assert
#define Assert(condition) ((void)0)
#endif

static PGAlignedBlock pages[2], incoming;
static RelationData relation;
static FormData_pg_class form;
static SMgrRelationData fixture_smgr;
static RelFileLocator vm_locator;
static HeapTupleData tuple;
static TransactionId creator;
static Buffer vm;
static bool vm_pinned, locked, replace_image, wait_error;
static unsigned waits, waits_with_vm, prepares, releases, repins, misses;
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, NBuffers = 2, NLocBuffer;
char *BufferBlocks = (char *)pages;
Block *LocalBufferBlockPointers;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

#define RelationGetSmgr(rel) ((void)(rel), &fixture_smgr)

bool
errstart(int level, const char *domain)
{
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s at %s:%d\n", condition, file, line);
	abort();
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 2);
	UT_ASSERT(vm_pinned);
	vm_pinned = false;
	releases++;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(locked);
		locked = false;
		return;
	}
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(!locked);
	waits++;
	waits_with_vm += vm_pinned;
	if (wait_error)
		elog(ERROR, "controlled heap-current acquisition error");
	if (replace_image) {
		memcpy(pages[0].data, incoming.data, BLCKSZ);
		replace_image = false;
	}
	locked = true;
}

void
visibilitymap_pin(Relation rel, BlockNumber block, Buffer *buffer)
{
	UT_ASSERT(!locked);
	UT_ASSERT(rel == &relation && block == 17);
	if (BufferIsValid(*buffer)) {
		UT_ASSERT(vm_pinned && *buffer == 2);
		UT_ASSERT(RelFileLocatorEquals(vm_locator, rel->rd_locator));
		return;
	}
	UT_ASSERT(!vm_pinned);
	UT_ASSERT_EQ(*buffer, InvalidBuffer);
	prepares++;
	vm_locator = rel->rd_locator;
	if (PageIsNew(pages[1].data)) {
		PageInit(pages[1].data, BLCKSZ, 0);
		((PageHeader)pages[1].data)->pd_block_scn = 30;
	}
	vm_pinned = true;
	*buffer = 2;
}

bool
ReadRecentBuffer(RelFileLocator locator, ForkNumber fork, BlockNumber block, Buffer recent)
{
	UT_ASSERT(locked && !vm_pinned);
	UT_ASSERT_EQ(fork, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT_EQ(recent, 2);
	repins++;
	if (misses > 0) {
		misses--;
		return false;
	}
	if (!RelFileLocatorEquals(locator, vm_locator))
		return false;
	vm_pinned = true;
	return true;
}

#include "test_cluster_heap_lock_vm_recent.inc"
#include "test_cluster_pcm_aux_heap_repin.inc"
#include "test_cluster_heap_lock_rebind.inc"

static void
acquire(Buffer *map)
{
#ifdef FIXTURE_ROW_LOCK_VM_PIN
	heap_lock_tuple_buffer(&relation, 1, &tuple, &creator, map);
#else
	/* The unchanged parent helper has no VM pointer; exercise its actual
	 * body so RED is an ownership failure, not an absent-API build failure. */
	(void)map;
	heap_lock_tuple_buffer(&relation, 1, &tuple, &creator);
#endif
}

static void
reset(bool cached, bool visible)
{
	PGAlignedBlock row;
	HeapTupleHeader header = (HeapTupleHeader)row.data;
	Page page = pages[0].data;

	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	memset(&tuple, 0, sizeof(tuple));
	memset(row.data, 0, 109);
	relation.rd_rel = &form;
	relation.rd_id = 42;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 900 };
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	fixture_smgr.smgr_rlocator.locator = vm_locator = relation.rd_locator;
	cluster_enabled = cluster_shared_config = true;
	waits = waits_with_vm = prepares = releases = repins = misses = 0;
	locked = wait_error = false;
	vm = cached ? 2 : InvalidBuffer;
	vm_pinned = cached;
	creator = 700;
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_infomask = HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(header, creator);
	PageInit(page, BLCKSZ, 0);
	UT_ASSERT_EQ(PageAddItem(page, row.data, 109, 1, false, true), 1);
	UT_ASSERT_EQ(PageAddItem(page, row.data, 109, 2, false, true), 2);
	if (visible)
		PageSetAllVisible(page);
	tuple.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	tuple.t_len = 109;
	ItemPointerSet(&tuple.t_self, 17, 2);
	memcpy(incoming.data, page, BLCKSZ);
	ItemIdSetUnused(PageGetItemId(incoming.data, 1));
	PageRepairFragmentation(incoming.data);
	UT_ASSERT(ItemIdGetOffset(PageGetItemId(page, 2))
			  != ItemIdGetOffset(PageGetItemId(incoming.data, 2)));
	PageInit(pages[1].data, BLCKSZ, 0);
	((PageHeader)pages[1].data)->pd_block_scn = 30;
	replace_image = true;
}

static void
check_current(void)
{
	UT_ASSERT(locked);
	UT_ASSERT_EQ(waits_with_vm, 0);
	UT_ASSERT(tuple.t_data
			  == (HeapTupleHeader)PageGetItem(pages[0].data, PageGetItemId(pages[0].data, 2)));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple.t_data), creator);
	UT_ASSERT_EQ(tuple.t_tableOid, 42);
}

UT_TEST(cached_vm_pin_is_released_before_heap_current)
{
	reset(true, true);
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(prepares, 0);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(repins, 1);
}

UT_TEST(cold_vm_is_prepared_outside_heap_content)
{
	reset(false, true);
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(repins, 1);
}

UT_TEST(new_all_visible_image_gets_exact_pin_before_tuple_rebind)
{
	reset(false, false);
	PageSetAllVisible(incoming.data);
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(repins, 1);
}

UT_TEST(resident_misses_release_heap_before_preparation_and_requalify)
{
	reset(true, true);
	misses = 3;
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 4);
	UT_ASSERT_EQ(prepares, 3);
	UT_ASSERT_EQ(releases, 4);
	UT_ASSERT_EQ(repins, 4);
}

UT_TEST(retagged_vm_descriptor_is_not_reused_for_another_relation)
{
	reset(true, true);
	vm_locator.relNumber++;
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(repins, 2);
}

UT_TEST(new_vm_image_returns_to_original_preparation_outside_heap)
{
	reset(true, true);
	memset(pages[1].data, 0, BLCKSZ);
	acquire(&vm);
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(releases, 3);
	UT_ASSERT_EQ(repins, 2);
}

UT_TEST(nonvisible_current_drops_old_vm_pin_without_repin)
{
	reset(true, false);
	acquire(&vm);
	check_current();
	UT_ASSERT(!vm_pinned && vm == InvalidBuffer);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(prepares + repins, 0);
}

UT_TEST(actual_chain_bracket_keeps_the_exact_returned_pin_without_an_extra_relock)
{
	Relation rel = &relation;
	HeapTupleData mytup;
	Buffer buf = 1, vmbuffer;
	BlockNumber block = 17;
	TransactionId creation_xmin;
	bool pinned_desired_page;

	reset(false, false);
	PageSetAllVisible(incoming.data);
	mytup = tuple;
	creation_xmin = creator;
	vmbuffer = vm;
#include "test_cluster_heap_lock_chain_pin.inc"
	tuple = mytup;
	creator = creation_xmin;
	vm = vmbuffer;
	check_current();
	UT_ASSERT(vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(repins, 1);
}

UT_TEST(creator_contradiction_preserves_original_error_without_mutation)
{
	sigjmp_buf jump;
	HeapTupleData before;
	volatile bool caught = false;

	reset(true, true);
	HeapTupleHeaderSetXmin(
		(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
	before = tuple;
	PG_exception_stack = &jump;
	if (sigsetjmp(jump, 0) == 0)
		acquire(&vm);
	else
		caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught && locked);
	UT_ASSERT_EQ(waits_with_vm, 0);
	UT_ASSERT_EQ(creator, 700);
	UT_ASSERT(memcmp(&tuple, &before, sizeof(tuple)) == 0);
	UT_ASSERT(memcmp(pages[0].data, incoming.data, BLCKSZ) == 0);
}

UT_TEST(wait_error_retains_no_obstructing_vm_pin)
{
	sigjmp_buf jump;
	HeapTupleData before;
	volatile bool caught = false;

	reset(true, true);
	before = tuple;
	wait_error = true;
	PG_exception_stack = &jump;
	if (sigsetjmp(jump, 0) == 0)
		acquire(&vm);
	else
		caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught && !locked);
	UT_ASSERT(!vm_pinned && vm == InvalidBuffer);
	UT_ASSERT_EQ(waits_with_vm, 0);
	UT_ASSERT(memcmp(&tuple, &before, sizeof(tuple)) == 0);
}

static void
native_control(bool local)
{
	HeapTupleData before;

	reset(true, false);
	cluster_enabled = local;
	form.relpersistence = local ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	memset(pages[0].data, 0, BLCKSZ);
	replace_image = false;
	before = tuple;
	acquire(&vm);
	UT_ASSERT(locked && vm_pinned && vm == 2);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(releases + prepares + repins, 0);
	UT_ASSERT(memcmp(&tuple, &before, sizeof(tuple)) == 0);
}
UT_TEST(native_profile_keeps_original_lock_and_does_not_decode_tuple)
{
	native_control(false);
}
UT_TEST(local_relation_keeps_original_lock_and_does_not_decode_tuple)
{
	native_control(true);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(cached_vm_pin_is_released_before_heap_current);
	UT_RUN(cold_vm_is_prepared_outside_heap_content);
	UT_RUN(new_all_visible_image_gets_exact_pin_before_tuple_rebind);
	UT_RUN(resident_misses_release_heap_before_preparation_and_requalify);
	UT_RUN(retagged_vm_descriptor_is_not_reused_for_another_relation);
	UT_RUN(new_vm_image_returns_to_original_preparation_outside_heap);
	UT_RUN(nonvisible_current_drops_old_vm_pin_without_repin);
	UT_RUN(actual_chain_bracket_keeps_the_exact_returned_pin_without_an_extra_relock);
	UT_RUN(creator_contradiction_preserves_original_error_without_mutation);
	UT_RUN(wait_error_retains_no_obstructing_vm_pin);
	UT_RUN(native_profile_keeps_original_lock_and_does_not_decode_tuple);
	UT_RUN(local_relation_keeps_original_lock_and_does_not_decode_tuple);
	UT_DONE();
	return ut_failed_count != 0;
}
