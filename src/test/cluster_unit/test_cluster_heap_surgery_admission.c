/*-------------------------------------------------------------------------
 * test_cluster_heap_surgery_admission.c
 *    Direct repair admission before shared heap/VM mutation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The entire original entry and maintenance policy execute. Array/ACL,
 * relation/buffer access and WAL sinks are controlled. Native page/tuple
 * changes are real; this is not SQL or physical durability verification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "storage/bufmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "unit_test.h"

#ifdef FIXTURE_NO_ASSERTS
#undef Assert
#define Assert(condition) ((void)0)
#endif

UT_DEFINE_GLOBALS();
int NBuffers = 2, NLocBuffer;

static RelationData relation;
static FormData_pg_class form;
static PGAlignedBlock heap_page, map_page, original_heap, original_map;
static struct { ArrayType header; ItemPointerData tid; } input;
static bool storage, recovering, owner, invalid_array, heap_locked, map_locked;
static int ntids;
static unsigned copies, validations, opens, closes, frees, policy_checks;
static unsigned sizes, reads, locks, pins, clears, dirties, wals;
static int last_code;
static char last_message[160];
static jmp_buf error_jump;
static volatile uint32 fixture_crit;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition; (void)file; (void)line;
	abort();
}

static bool
storage_enabled(void)
{
	policy_checks++;
	return storage;
}

#define cluster_storage_mode_enabled storage_enabled
#include "test_cluster_heap_maintenance_policy.inc"

static int
fixture_code(int code)
{
	last_code = code;
	return 0;
}

static int fixture_message(const char *format, ...) pg_attribute_printf(1, 2);
static int
fixture_message(const char *format, ...)
{
	va_list args;

	va_start(args, format);
	vsnprintf(last_message, sizeof(last_message), format, args);
	va_end(args);
	return 0;
}

static void
fixture_report(int level)
{
	if (level >= ERROR)
		longjmp(error_jump, 1);
}

static ArrayType *
array_copy(void)
{
	copies++;
	return &input.header;
}

static void
sanity_check_tid_array(ArrayType *array, int *count)
{
	(void)array;
	validations++;
	if (invalid_array) {
		last_code = ERRCODE_INVALID_PARAMETER_VALUE;
		longjmp(error_jump, 1);
	}
	*count = ntids;
}

static Relation
open_relation(Oid oid, LOCKMODE mode)
{
	(void)oid; (void)mode;
	opens++;
	return &relation;
}

static void
close_relation(Relation rel, LOCKMODE mode)
{
	(void)rel; (void)mode;
	closes++;
}

static BlockNumber
find_tids_one_page(ItemPointer tids, int count, OffsetNumber *next)
{
	if (count != 1 || *next != 0)
		abort();
	*next = 1;
	return ItemPointerGetBlockNumber(tids);
}

static int32
tidcmp(const void *left, const void *right)
{
	(void)left; (void)right;
	/* Tests supply zero or one TID; sorting must remain unreachable. */
	abort();
}

static void
deny_owner(void)
{
	last_code = ERRCODE_INSUFFICIENT_PRIVILEGE;
	longjmp(error_jump, 1);
}

static BlockNumber
number_of_blocks(Relation rel)
{
	(void)rel;
	sizes++;
	return 1;
}

static Buffer
read_heap(Relation rel, BlockNumber block)
{
	(void)rel;
	if (block != 0)
		abort();
	reads++;
	return 1;
}

static void
lock_heap(Buffer buffer)
{
	if (buffer != 1 || heap_locked)
		abort();
	locks++;
	heap_locked = true;
}

static void
pin_map(Relation rel, BlockNumber block, Buffer *buffer)
{
	(void)rel; (void)block;
	pins++;
	*buffer = 2;
}

static void
lock_map(Buffer buffer, int mode)
{
	if (buffer != 2)
		abort();
	map_locked = mode != BUFFER_LOCK_UNLOCK;
}

static bool
clear_map(Relation rel, BlockNumber block, Buffer buffer, uint8 flags)
{
	(void)rel; (void)block;
	if (!heap_locked || !map_locked || fixture_crit == 0 || buffer != 2)
		abort();
	map_page.data[SizeOfPageHeaderData] &= ~flags;
	clears++;
	return true;
}

static void
dirty_page(Buffer buffer)
{
	if (buffer != 1 || !heap_locked || fixture_crit == 0)
		abort();
	dirties++;
}

static void
log_page(Buffer buffer, bool standard)
{
	if (fixture_crit == 0 || (buffer == 1 ? !heap_locked || !standard : !map_locked || standard))
		abort();
	wals++;
}

static void
release_heap(Buffer buffer)
{
	if (buffer != 1 || !heap_locked)
		abort();
	heap_locked = false;
}

#undef PG_GETARG_OID
#define PG_GETARG_OID(arg) ((void)fcinfo, (Oid)77)
#undef PG_GETARG_ARRAYTYPE_P_COPY
#define PG_GETARG_ARRAYTYPE_P_COPY(arg) array_copy()
#undef ARR_DATA_PTR
#define ARR_DATA_PTR(array) ((void)(array), &input.tid)
#define RecoveryInProgress() recovering
#define relation_open open_relation
#define relation_close close_relation
#define object_ownercheck(...) owner
#define aclcheck_error(...) deny_owner()
#define GetUserId() ((Oid)10)
#define errdetail_relkind_not_supported(...) 0
#undef errcode
#define errcode fixture_code
#undef errmsg
#define errmsg fixture_message
#undef errhint
#define errhint(...) 0
#undef ereport
#define ereport(level, rest) do { (void)(rest); fixture_report(level); } while (0)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks number_of_blocks
#undef RelationNeedsWAL
#define RelationNeedsWAL(rel) ((rel)->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT)
#define ReadBuffer read_heap
#define LockBufferForCleanup lock_heap
#undef BufferGetPage
#define BufferGetPage(buffer) ((Page)(buffer == 1 ? heap_page.data : map_page.data))
#define visibilitymap_pin pin_map
#define LockBuffer lock_map
#define visibilitymap_clear_locked clear_map
#define MarkBufferDirty dirty_page
#define log_newpage_buffer log_page
#define UnlockReleaseBuffer release_heap
#define ReleaseBuffer(buffer) ((void)(buffer))
#undef START_CRIT_SECTION
#define START_CRIT_SECTION() (fixture_crit++)
#undef END_CRIT_SECTION
#define END_CRIT_SECTION() (fixture_crit--)
#define pfree(pointer) ((void)(pointer), frees++)
#include "test_cluster_heap_surgery_entry.inc"

static void
prepare(bool shared, bool local, bool visible)
{
	PageHeader page;
	ItemId item;
	HeapTupleHeader tuple;

	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	memset(&heap_page, 0, sizeof(heap_page));
	memset(&map_page, 0, sizeof(map_page));
	form.relkind = RELKIND_RELATION;
	form.relam = HEAP_TABLE_AM_OID;
	form.relpersistence = local ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &form;
	strlcpy(NameStr(form.relname), "repair_input", NAMEDATALEN);
	page = (PageHeader)heap_page.data;
	page->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	page->pd_upper = BLCKSZ - MAXALIGN(SizeofHeapTupleHeader);
	page->pd_special = BLCKSZ;
	page->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	item = PageGetItemId((Page)page, 1);
	ItemIdSetNormal(item, page->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)PageGetItem((Page)page, item);
	HeapTupleHeaderSetXmin(tuple, 80);
	HeapTupleHeaderSetXmax(tuple, 90);
	ItemPointerSet(&tuple->t_ctid, 0, 1);
	if (visible)
		PageSetAllVisible((Page)page);
	map_page.data[SizeOfPageHeaderData] = VISIBILITYMAP_VALID_BITS;
	original_heap = heap_page; original_map = map_page;
	ItemPointerSet(&input.tid, 0, 1);
	storage = shared; owner = true;
	recovering = invalid_array = heap_locked = map_locked = false;
	ntids = 1; fixture_crit = 0; last_code = 0; last_message[0] = '\0';
	copies = validations = opens = closes = frees = policy_checks = 0;
	sizes = reads = locks = pins = clears = dirties = wals = 0;
}

static bool
run_entry(HeapTupleForceOption option)
{
	FunctionCallInfoBaseData call = { 0 };

	if (setjmp(error_jump) != 0)
		return false;
	(void)heap_force_common(&call, option);
	return true;
}

static void
check_no_page_access(void)
{
	UT_ASSERT_EQ(sizes + reads + locks + pins + clears + dirties + wals, 0);
	UT_ASSERT(!heap_locked && !map_locked && fixture_crit == 0);
	UT_ASSERT(memcmp(&heap_page, &original_heap, BLCKSZ) == 0);
	UT_ASSERT(memcmp(&map_page, &original_map, BLCKSZ) == 0);
}

UT_TEST(shared_force_kill_refuses_before_block_access)
{
	for (int hint = 0; hint < 2; hint++)
		for (int count = 0; count < 2; count++) {
			prepare(true, false, hint); ntids = count;
			UT_ASSERT(!run_entry(HEAP_FORCE_KILL));
			UT_ASSERT_EQ(last_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			check_no_page_access();
			UT_ASSERT_EQ(opens, 1);
		}
}
UT_TEST(shared_force_freeze_retains_early_refusal)
{
	for (int hint = 0; hint < 2; hint++)
		for (int count = 0; count < 2; count++) {
			prepare(true, false, hint); ntids = count;
			UT_ASSERT(!run_entry(HEAP_FORCE_FREEZE));
			UT_ASSERT_EQ(last_code, ERRCODE_FEATURE_NOT_SUPPORTED);
			check_no_page_access();
		}
}
UT_TEST(native_kill_keeps_tuple_clear_and_wal)
{
	for (int hint = 0; hint < 2; hint++) {
		prepare(false, false, hint);
		UT_ASSERT(run_entry(HEAP_FORCE_KILL));
		UT_ASSERT(ItemIdIsDead(PageGetItemId((Page)heap_page.data, 1)));
		UT_ASSERT(!PageIsAllVisible((Page)heap_page.data));
		UT_ASSERT_EQ(clears, hint); UT_ASSERT_EQ(wals, 1 + hint);
		UT_ASSERT_EQ(dirties, 1); UT_ASSERT_EQ(closes, 1); UT_ASSERT_EQ(frees, 1);
		UT_ASSERT(!heap_locked && !map_locked && fixture_crit == 0);
	}
}
UT_TEST(native_freeze_keeps_original_tuple_changes)
{
	HeapTupleHeader tuple;

	prepare(false, false, true);
	UT_ASSERT(run_entry(HEAP_FORCE_FREEZE));
	tuple = (HeapTupleHeader)PageGetItem((Page)heap_page.data, PageGetItemId((Page)heap_page.data, 1));
	UT_ASSERT_EQ(HeapTupleHeaderGetXmin(tuple), FrozenTransactionId);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), InvalidTransactionId);
	UT_ASSERT_EQ(pins + clears, 0); UT_ASSERT_EQ(wals, 1);
}
UT_TEST(local_buffers_keep_both_repairs_without_shared_wal)
{
	for (int freeze = 0; freeze < 2; freeze++) {
		prepare(true, true, true);
		UT_ASSERT(run_entry(freeze ? HEAP_FORCE_FREEZE : HEAP_FORCE_KILL));
		UT_ASSERT_EQ(dirties, 1); UT_ASSERT_EQ(wals, 0);
		UT_ASSERT_EQ(closes, 1);
	}
}
UT_TEST(owner_check_precedes_shared_policy)
{
	for (int freeze = 0; freeze < 2; freeze++) {
		prepare(true, false, true); owner = false;
		UT_ASSERT(!run_entry(freeze ? HEAP_FORCE_FREEZE : HEAP_FORCE_KILL));
		UT_ASSERT_EQ(last_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
		UT_ASSERT_EQ(policy_checks, 0); check_no_page_access();
	}
}
UT_TEST(original_recovery_input_and_relation_checks_precede_policy)
{
	for (int fault = 0; fault < 4; fault++) {
		prepare(true, false, true);
		recovering = fault == 0; invalid_array = fault == 1;
		if (fault == 2) form.relkind = RELKIND_INDEX;
		if (fault == 3) form.relam = InvalidOid;
		UT_ASSERT(!run_entry(HEAP_FORCE_KILL));
		UT_ASSERT_EQ(policy_checks, 0); check_no_page_access();
		UT_ASSERT_EQ(opens, fault < 2 ? 0 : 1);
	}
}
UT_TEST(native_empty_tid_input_keeps_normal_exit)
{
	for (int freeze = 0; freeze < 2; freeze++) {
		prepare(false, false, true); ntids = 0;
		UT_ASSERT(run_entry(freeze ? HEAP_FORCE_FREEZE : HEAP_FORCE_KILL));
		UT_ASSERT_EQ(sizes, 1); UT_ASSERT_EQ(reads + wals + dirties, 0);
		UT_ASSERT_EQ(closes, 1); UT_ASSERT_EQ(frees, 1);
	}
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(shared_force_kill_refuses_before_block_access);
	UT_RUN(shared_force_freeze_retains_early_refusal);
	UT_RUN(native_kill_keeps_tuple_clear_and_wal);
	UT_RUN(native_freeze_keeps_original_tuple_changes);
	UT_RUN(local_buffers_keep_both_repairs_without_shared_wal);
	UT_RUN(owner_check_precedes_shared_policy);
	UT_RUN(original_recovery_input_and_relation_checks_precede_policy);
	UT_RUN(native_empty_tid_input_keeps_normal_exit);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
