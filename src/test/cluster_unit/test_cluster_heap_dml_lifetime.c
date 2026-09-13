/*-------------------------------------------------------------------------
 * test_cluster_heap_dml_lifetime.c
 *   Exact DML consumer lifetime across an installed compacted current image.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Runtime allocation/image arrival are explicit fixture seams; the generated
 * consumer and page compaction routines are unchanged production code.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_itl_slot.h"
#include "storage/bufmgr.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "../../backend/access/heap/heapam_r4_private.h"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static PGAlignedBlock current_page, incoming_page;
char *BufferBlocks = current_page.data;
Block *LocalBufferBlockPointers;
int NBuffers = 1, NLocBuffer = 0;
static bool replace_image, locked, poison_after_unlock;
bool cluster_enabled = true;
int cluster_node_id = 0;
static unsigned allocation_calls, lock_calls;
static unsigned copies, frees, toast_calls, invalidations;
static char expected_tuple[109];
static HeapTuple borrowed_tuple;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

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
int
errdetail(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
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
#include "test_cluster_heap_lock_rebind.inc"

static void
install_and_lock(void)
{
	UT_ASSERT(!locked);
	if (replace_image)
		memcpy(current_page.data, incoming_page.data, BLCKSZ);
	locked = true;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(locked);
		locked = false;
		if (poison_after_unlock)
			memset(current_page.data, 0xA5, BLCKSZ);
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
		lock_calls++;
		install_and_lock();
	}
}

void *
palloc(Size size)
{
	/* Only real heap_copytuple calls allocate in these extracted consumers. */
	UT_ASSERT(locked);
	copies++;
	return malloc(size);
}
void
pfree(void *ptr)
{
	UT_ASSERT(!locked);
	frees++;
	free(ptr);
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(!locked);
}

static void
check_unlocked_input(Relation relation, HeapTuple tuple)
{
	UT_ASSERT(!locked);
	UT_ASSERT_EQ(tuple->t_len, sizeof(expected_tuple));
	UT_ASSERT_EQ(memcmp(tuple->t_data, expected_tuple, sizeof(expected_tuple)), 0);
	UT_ASSERT_EQ(tuple->t_tableOid, relation->rd_id);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&tuple->t_self), 17);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_self), 1);
	if (cluster_heap_read_needs_copy(relation)) {
		UT_ASSERT(tuple != borrowed_tuple);
		UT_ASSERT(tuple->t_data != borrowed_tuple->t_data);
	} else
		UT_ASSERT(tuple == borrowed_tuple);
}
HeapTuple
heap_toast_insert_or_update(Relation relation, HeapTuple newtuple, HeapTuple oldtuple, int options)
{
	check_unlocked_input(relation, oldtuple);
	toast_calls++;
	return newtuple;
}
void
heap_toast_delete(Relation relation, HeapTuple tuple, bool speculative)
{
	check_unlocked_input(relation, tuple);
	toast_calls++;
}
void
CacheInvalidateHeapTuple(Relation relation, HeapTuple oldtuple, HeapTuple newtuple)
{
	check_unlocked_input(relation, oldtuple);
	invalidations++;
}
void
cluster_heap_lock_with_vm_repin(Relation relation, BlockNumber block, Buffer buffer, Buffer *vm)
{
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
}
void
visibilitymap_pin(Relation relation, BlockNumber block, Buffer *vm)
{
	UT_ASSERT(false); /* no all-visible VM work in this witness */
}
Buffer
RelationGetBufferForTuple(Relation relation, Size len, Buffer otherBuffer, int options,
						  BulkInsertStateData *bistate, Buffer *vm, Buffer *other_vm, int pages)
{
	allocation_calls++;
	UT_ASSERT_EQ(otherBuffer, 1);
	install_and_lock();
	return 2; /* new page has no byte consumer in the extracted boundary */
}

static void
fill_row(char *bytes, TransactionId xmin, int32 id)
{
	HeapTupleHeader tuple = (HeapTupleHeader)bytes;
	memset(bytes, 0, 109);
	tuple->t_hoff = 24;
	tuple->t_infomask = HEAP_XMAX_INVALID;
	tuple->t_infomask2 = 2;
	HeapTupleHeaderSetXmin(tuple, xmin);
	memcpy(bytes + tuple->t_hoff, &id, sizeof(id));
}
static void
witness(bool cross_page, bool replace)
{
	PGAlignedBlock tuple_bytes;
	RelationData relation_data = { 0 };
	FormData_pg_class form = { 0 };
	Relation relation = &relation_data;
	HeapTupleData oldtup = { 0 }, newtuple = { 0 };
	HeapTuple newtup = &newtuple, heaptup = newtup;
	Page page = (Page)current_page.data;
	Buffer buffer = 1, newbuf = InvalidBuffer;
	Buffer vmbuffer = InvalidBuffer, vmbuffer_new = InvalidBuffer;
	BlockNumber block = 17;
	TransactionId creation_xmin = 700;
	Size pagefree = cross_page ? 0 : 1024, newtupsize = 112;
	int32 consumed_id = 0, current_id = 0;
	HeapTupleHeader current_target;

	relation_data.rd_id = 9901;
	relation_data.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	PageInit(page, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	((PageHeader)page)->pd_flags |= PD_HAS_ITL;
	fill_row(tuple_bytes.data, 900, 222);
	UT_ASSERT_EQ(PageAddItem(page, tuple_bytes.data, 109, 1, false, true), 1);
	fill_row(tuple_bytes.data, creation_xmin, 111);
	UT_ASSERT_EQ(PageAddItem(page, tuple_bytes.data, 109, 2, false, true), 2);
	oldtup.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	oldtup.t_len = 109;
	ItemPointerSet(&oldtup.t_self, block, 2);
	HeapTupleHeaderSetXmax(oldtup.t_data, 1000);
	oldtup.t_data->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	oldtup.t_data->t_ctid = oldtup.t_self;
	newtup->t_data = (HeapTupleHeader)tuple_bytes.data;
	newtup->t_len = 109;
	memcpy(incoming_page.data, page, BLCKSZ);
	ItemIdSetUnused(PageGetItemId((Page)incoming_page.data, 1));
	PageRepairFragmentation((Page)incoming_page.data);
	fill_row(tuple_bytes.data, 900, 222);
	UT_ASSERT_EQ(PageAddItem((Page)incoming_page.data, tuple_bytes.data, 109, 1, true, true), 1);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber((Page)incoming_page.data), 2);
	UT_ASSERT(ItemIdGetOffset(PageGetItemId((Page)incoming_page.data, 2))
			  != ItemIdGetOffset(PageGetItemId(page, 2)));
	UT_ASSERT_EQ(ItemIdGetOffset(PageGetItemId((Page)incoming_page.data, 1)),
				 ItemIdGetOffset(PageGetItemId(page, 2)));
	replace_image = replace;
	locked = false;
	allocation_calls = lock_calls = 0;
	goto l_pgrac_reacquire;
#include "test_cluster_heap_update_reacquire.inc"
	current_target = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	memcpy(&consumed_id, (char *)oldtup.t_data + oldtup.t_data->t_hoff, sizeof(int32));
	memcpy(&current_id, (char *)current_target + current_target->t_hoff, sizeof(int32));
	printf("# cross=%d replace=%d logical_id=%d consumed_id=%d logical_xmin=%u consumed_xmin=%u\n",
		   cross_page, replace, current_id, consumed_id, HeapTupleHeaderGetRawXmin(current_target),
		   HeapTupleHeaderGetRawXmin(oldtup.t_data));
	UT_ASSERT(locked);
	UT_ASSERT_EQ(allocation_calls, cross_page ? 1 : 0);
	UT_ASSERT_EQ(lock_calls, cross_page ? 0 : 1);
	UT_ASSERT_EQ(newbuf, cross_page ? 2 : 1);
	UT_ASSERT_EQ(current_id, 111);
	UT_ASSERT_EQ(consumed_id, current_id);
	UT_ASSERT(oldtup.t_data == current_target);
}
UT_TEST(no_install_control)
{
	witness(true, false);
}
UT_TEST(cross_page_reacquire_must_rebind)
{
	witness(true, true);
}
UT_TEST(same_page_reacquire_must_rebind)
{
	witness(false, true);
}

static void
rebind_negative(int fault)
{
	PGAlignedBlock bytes, before;
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	HeapTupleData tuple = { 0 }, tuple_before;
	TransactionId creator = 700;
	Page page = (Page)current_page.data;
	ItemId lp;
	sigjmp_buf jump;
	volatile bool caught = false;

	relation.rd_id = 9901;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	PageInit(page, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	fill_row(bytes.data, 700, 111);
	UT_ASSERT_EQ(PageAddItem(page, bytes.data, 109, 1, false, true), 1);
	lp = PageGetItemId(page, 1);
	tuple.t_data = (HeapTupleHeader)PageGetItem(page, lp);
	tuple.t_len = 109;
	ItemPointerSet(&tuple.t_self, 17, 1);
	switch (fault) {
	case 0:
		HeapTupleHeaderSetXmin(tuple.t_data, 701);
		break;
	case 1:
		ItemIdSetUnused(lp);
		break;
	case 2:
		ItemPointerSetOffsetNumber(&tuple.t_self, 2);
		break;
	case 3:
		((PageHeader)page)->pd_lower = 0;
		break;
	case 4:
		ItemIdSetNormal(lp, ItemIdGetOffset(lp), 1);
		break;
	case 5:
		ItemIdSetNormal(lp, BLCKSZ - 1, 109);
		break;
	case 6:
		HeapTupleHeaderSetXmin(tuple.t_data, InvalidTransactionId);
		break;
	}
	tuple_before = tuple;
	memcpy(before.data, page, BLCKSZ);
	cluster_enabled = true;
	replace_image = poison_after_unlock = false;
	locked = false;
	PG_exception_stack = &jump;
	if (sigsetjmp(jump, 0) == 0)
		heap_lock_tuple_buffer(&relation, 1, &tuple, &creator);
	else
		caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught);
	UT_ASSERT(locked);
	UT_ASSERT_EQ(creator, 700);
	UT_ASSERT_EQ(memcmp(page, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(&tuple, &tuple_before, sizeof(tuple)), 0);
	locked = false;
}
UT_TEST(creator_contradiction_is_zero_mutation)
{
	rebind_negative(0);
}
UT_TEST(missing_target_is_zero_mutation)
{
	rebind_negative(1);
	rebind_negative(2);
}
UT_TEST(malformed_target_is_zero_mutation)
{
	for (int fault = 3; fault <= 6; fault++)
		rebind_negative(fault);
}

static void
excluded_rebind(bool native)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	HeapTupleData tuple = { 0 }, before;
	TransactionId creator = 700;

	relation.rd_rel = &form;
	form.relpersistence = native ? RELPERSISTENCE_PERMANENT : RELPERSISTENCE_TEMP;
	cluster_enabled = !native;
	memset(current_page.data, 0, BLCKSZ); /* must not inspect this invalid page */
	tuple.t_data = (HeapTupleHeader)incoming_page.data;
	tuple.t_len = 123;
	ItemPointerSet(&tuple.t_self, 17, 1);
	before = tuple;
	replace_image = poison_after_unlock = false;
	locked = false;
	heap_lock_tuple_buffer(&relation, 1, &tuple, &creator);
	UT_ASSERT(locked);
	UT_ASSERT_EQ(creator, 700);
	UT_ASSERT_EQ(memcmp(&before, &tuple, sizeof(tuple)), 0);
	cluster_enabled = true;
	locked = false;
}
UT_TEST(native_rebind_excluded)
{
	excluded_rebind(true);
}
UT_TEST(local_rebind_excluded)
{
	excluded_rebind(false);
}

static void
unlocked_consumer(int surface, int mode)
{
	PGAlignedBlock bytes;
	RelationData relation_data = { 0 };
	FormData_pg_class form = { 0 };
	Relation relation = &relation_data;
	HeapTupleData tp = { 0 }, oldtup, newtuple = { 0 };
	HeapTuple unlocked_tuple = &tp, unlocked_old_tuple, toast_old_tuple;
	HeapTuple newtup = &newtuple, heaptup = newtup;
	Buffer buffer = 1, newbuf = 1, vmbuffer = InvalidBuffer;
	bool need_toast = true;
	Size newtupsize = 0;
	Page page = (Page)current_page.data;

	relation_data.rd_id = 9901;
	relation_data.rd_rel = &form;
	form.relpersistence = mode == 2 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
	form.relkind = RELKIND_RELATION;
	cluster_enabled = mode != 1;
	PageInit(page, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	fill_row(bytes.data, 700, 111);
	((HeapTupleHeader)bytes.data)->t_infomask |= HEAP_HASEXTERNAL;
	UT_ASSERT_EQ(PageAddItem(page, bytes.data, 109, 1, false, true), 1);
	tp.t_data = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
	tp.t_len = 109;
	tp.t_tableOid = relation_data.rd_id;
	ItemPointerSet(&tp.t_self, 17, 1);
	oldtup = tp;
	toast_old_tuple = unlocked_old_tuple = &oldtup;
	newtuple.t_len = 109;
	memcpy(expected_tuple, tp.t_data, sizeof(expected_tuple));
	borrowed_tuple = surface == 0 ? &tp : &oldtup;
	locked = true;
	replace_image = false;
	poison_after_unlock = mode == 0;
	copies = frees = toast_calls = invalidations = 0;
	if (surface == 0) {
#include "test_cluster_heap_delete_owned.inc"
	} else if (surface == 1) {
#include "test_cluster_heap_update_owned.inc"
	} else {
#include "test_cluster_heap_toast_capture.inc"
#include "test_cluster_heap_toast_consume.inc"
		UT_ASSERT(heaptup == newtup);
		UT_ASSERT_EQ(newtupsize, MAXALIGN(newtup->t_len));
	}
	UT_ASSERT(!locked);
	UT_ASSERT_EQ(copies, mode == 0 ? 1 : 0);
	UT_ASSERT_EQ(frees, copies);
	UT_ASSERT_EQ(toast_calls, surface == 1 ? 0 : 1);
	UT_ASSERT_EQ(invalidations, surface == 2 ? 0 : 1);
	cluster_enabled = true;
	poison_after_unlock = false;
}
UT_TEST(delete_postunlock_uses_owned_bytes)
{
	for (int mode = 0; mode < 3; mode++)
		unlocked_consumer(0, mode);
}
UT_TEST(update_invalidation_uses_owned_bytes)
{
	for (int mode = 0; mode < 3; mode++)
		unlocked_consumer(1, mode);
}
UT_TEST(update_toast_uses_owned_bytes)
{
	for (int mode = 0; mode < 3; mode++)
		unlocked_consumer(2, mode);
}
int
main(void)
{
	UT_PLAN(11);
	UT_RUN(no_install_control);
	UT_RUN(cross_page_reacquire_must_rebind);
	UT_RUN(same_page_reacquire_must_rebind);
	UT_RUN(creator_contradiction_is_zero_mutation);
	UT_RUN(missing_target_is_zero_mutation);
	UT_RUN(malformed_target_is_zero_mutation);
	UT_RUN(native_rebind_excluded);
	UT_RUN(local_rebind_excluded);
	UT_RUN(delete_postunlock_uses_owned_bytes);
	UT_RUN(update_invalidation_uses_owned_bytes);
	UT_RUN(update_toast_uses_owned_bytes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
