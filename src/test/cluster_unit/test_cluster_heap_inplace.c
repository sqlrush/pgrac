/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual genam inplace consumers and heap lock/rebind/unlock bodies.
 * Scan production, visibility/MultiXact decisions and WAL are external seams.
 * This suite proves the selected-row/current-buffer ownership boundary, not
 * full catalog invalidation, transaction recovery or WAL durability. */
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_multixact_current.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
volatile uint32 CritSectionCount;
int NBuffers = 1;
int NLocBuffer;
static PGAlignedBlock current_page, selected_bytes, moved_page;
char *BufferBlocks = current_page.data;
Block *LocalBufferBlockPointers;
static RelationData rel;
static FormData_pg_class relform;
static SysScanDescData scan;
static BufferHeapTupleTableSlot slot;
const TupleTableSlotOps TTSOpsBufferHeapTuple = { 0 };
static HeapTupleData selected_tuple;
static int pins, reads, scans, ends, waits, visibility_calls, finishes;
static int invals, forgets, snapshot_invalidations;
static bool content_locked, tuple_locked, owned, parallel, no_row, move_on_lock;
static TM_Result first_verdict, later_verdict;
static int change_on_lock;
static bool expect_error;
static int error_count, assertion_count;

static HeapTupleHeader
current_tuple(void)
{
	return (HeapTupleHeader)PageGetItem(current_page.data, PageGetItemId(current_page.data, 1));
}
static int
payload(HeapTupleHeader tuple)
{
	int value;
	memcpy(&value, (char *)tuple + MAXALIGN(SizeofHeapTupleHeader), sizeof(value));
	return value;
}
static void
set_payload(HeapTupleHeader tuple, int value)
{
	memcpy((char *)tuple + MAXALIGN(SizeofHeapTupleHeader), &value, sizeof(value));
}
void *
palloc(Size size)
{
	return malloc(size ? size : 1);
}
void *
palloc0(Size size)
{
	return calloc(1, size ? size : 1);
}
void
pfree(void *p)
{
	free(p);
}
bool
IsInParallelMode(void)
{
	return parallel;
}
bool
IsInplaceUpdateRelation(Relation relation)
{
	return true;
}
bool
IsSystemRelation(Relation relation)
{
	return true;
}
static void
check_inplace_rel_lock(HeapTuple tuple)
{}
void
ProcessInterrupts(void)
{
	abort();
}
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
errfinish(const char *file, int line, const char *func)
{
	error_count++;
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
pg_re_throw(void)
{
	errfinish(__FILE__, __LINE__, __func__);
	abort();
}
void
ExceptionalCondition(const char *c, const char *file, int line)
{
	assertion_count++;
	printf("# actual production assertion: %s at %s:%d\n", c, file, line);
	errfinish(file, line, c);
	abort();
}
bool
ItemPointerEquals(ItemPointer a, ItemPointer b)
{
	return ItemPointerGetBlockNumber(a) == ItemPointerGetBlockNumber(b)
		   && ItemPointerGetOffsetNumber(a) == ItemPointerGetOffsetNumber(b);
}
HeapTuple
heap_copytuple(HeapTuple tuple)
{
	HeapTuple copy = palloc(sizeof(*copy) + tuple->t_len);
	*copy = *tuple;
	copy->t_data = (HeapTupleHeader)(copy + 1);
	memcpy(copy->t_data, tuple->t_data, tuple->t_len);
	return copy;
}
void
heap_freetuple(HeapTuple tuple)
{
	pfree(tuple);
}
SysScanDesc
systable_beginscan(Relation relation, Oid index, bool indexOK, Snapshot snapshot, int nkeys,
				   ScanKey key)
{
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(!content_locked);
	scans++;
	scan.heap_rel = relation;
	scan.slot = &slot.base.base;
	slot.base.base.tts_flags = owned ? TTS_FLAG_SHOULDFREE : 0;
	slot.buffer = owned ? InvalidBuffer : 1;
	if (!owned)
		pins++;
	if (scans > 1 && !no_row) {
		/* A new catalog snapshot selects the now-current row. */
		selected_tuple.t_len = ItemIdGetLength(PageGetItemId(current_page.data, 1));
		memcpy(selected_bytes.data, current_tuple(), selected_tuple.t_len);
	}
	selected_tuple.t_data = owned ? (HeapTupleHeader)selected_bytes.data : current_tuple();
	slot.base.tuple = &selected_tuple;
	return &scan;
}
HeapTuple
systable_getnext(SysScanDesc scan)
{
	return no_row ? NULL : slot.base.tuple;
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pins > 0);
	UT_ASSERT(!content_locked);
	pins--;
}
void
systable_endscan(SysScanDesc scan)
{
	UT_ASSERT(!content_locked);
	ends++;
	if (BufferIsValid(slot.buffer))
		ReleaseBuffer(slot.buffer);
	slot.buffer = InvalidBuffer;
	/* Poison the old observation so a post-release use cannot look valid. */
	memset(selected_bytes.data, 0xcc, sizeof(selected_bytes.data));
}
Buffer
ReadBuffer(Relation relation, BlockNumber block)
{
	UT_ASSERT_EQ(block, 41);
	UT_ASSERT_EQ(pins, 0);
	reads++;
	pins++;
	return 1;
}
BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	return 41;
}
void
LockTuple(Relation relation, ItemPointer tid, LOCKMODE mode)
{
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), 41);
	UT_ASSERT(!tuple_locked);
	UT_ASSERT(!content_locked);
	tuple_locked = true;
}
void
UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE mode)
{
	UT_ASSERT(tuple_locked);
	UT_ASSERT(!content_locked);
	tuple_locked = false;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pins > 0);
	if (mode == BUFFER_LOCK_EXCLUSIVE) {
		UT_ASSERT(!content_locked);
		UT_ASSERT(tuple_locked);
		content_locked = true;
		if (move_on_lock) {
			memcpy(current_page.data, moved_page.data, BLCKSZ);
			move_on_lock = false;
		}
		if (change_on_lock == 1) {
			HeapTupleHeaderSetXmin(current_tuple(), 902);
			change_on_lock = 0;
		} else if (change_on_lock == 2) {
			ItemIdSetUnused(PageGetItemId(current_page.data, 1));
			no_row = true;
			change_on_lock = 0;
		} else if (change_on_lock == 3)
			((PageHeader)current_page.data)->pd_upper = BLCKSZ + 1;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
		UT_ASSERT(content_locked);
		content_locked = false;
	}
}
void
CacheInvalidateHeapTupleInplace(Relation relation, HeapTuple tuple)
{
	UT_ASSERT(!content_locked);
	UT_ASSERT(!tuple_locked);
	invals++;
}
void
ForgetInplace_Inval(void)
{
	forgets++;
}
void
InvalidateCatalogSnapshot(void)
{
	snapshot_invalidations++;
}
CommandId
GetCurrentCommandId(bool used)
{
	return 1;
}
TransactionId
GetCurrentTransactionId(void)
{
	return 700;
}
bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	return xid == 700;
}
TM_Result
HeapTupleSatisfiesUpdate(HeapTuple tuple, CommandId cid, Buffer buffer)
{
	UT_ASSERT(content_locked);
	UT_ASSERT(tuple_locked);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(tuple->t_data == current_tuple());
	visibility_calls++;
	return visibility_calls == 1 ? first_verdict : later_verdict;
}
void
XactLockTableWait(TransactionId xid, Relation relation, ItemPointer tid, XLTW_Oper oper)
{
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(!content_locked);
	UT_ASSERT(tuple_locked);
	waits++;
}
static bool
DoesMultiXactIdConflict(MultiXactId xid, uint16 mask, LockTupleMode mode, bool *current)
{
	return true;
}
static void
MultiXactIdWait(MultiXactId xid, MultiXactStatus status, uint16 mask, Relation relation,
				ItemPointer tid, XLTW_Oper oper, int *remaining)
{
	XactLockTableWait(xid, relation, tid, oper);
}

/* Opaque current-MX authorization is not the unit under test. Exercise its
 * NOT_APPLICABLE edge into the real native conflict wait, not a fake lock. */
typedef struct {
	int unused;
} ClusterCurrentMxOperationState;
typedef struct {
	TM_Result result;
} ClusterCurrentMxHeapResult;
typedef enum { CCMH_NOT_APPLICABLE, CCMH_RESTART, CCMH_DECIDED } ClusterCurrentMxHeapDisposition;
static ClusterCurrentMxHeapDisposition
cluster_current_mx_authorize(Relation relation, Buffer buffer, HeapTuple tuple, TransactionId xid,
							 ClusterCurrentTupleAction action, LockTupleMode mode, bool update,
							 LockWaitPolicy policy, bool wait, bool follow,
							 ClusterCurrentMxHeapResult *result,
							 ClusterCurrentMxOperationState *operation)
{
	UT_ASSERT(content_locked);
	return CCMH_NOT_APPLICABLE;
}
static void
cluster_current_mx_operation_finish(ClusterCurrentMxOperationState *operation)
{}

#include "test_cluster_heap_inplace_lock.inc"

/* The unchanged WAL mutation implementation is an external seam; verify
 * that the real finish consumer supplies the current target and retained X. */
void
heap_inplace_update_and_unlock(Relation relation, HeapTuple old, HeapTuple updated, Buffer buffer)
{
	UT_ASSERT(content_locked);
	UT_ASSERT(old->t_data == current_tuple());
	UT_ASSERT(updated->t_data != current_tuple());
	UT_ASSERT_EQ(pins, 1);
	set_payload(old->t_data, payload(updated->t_data));
	finishes++;
	heap_inplace_unlock(relation, old, buffer);
}

#include "test_cluster_heap_inplace_consumer.inc"

static void
reset_case(bool materialized)
{
	PGAlignedBlock tuple;
	HeapTupleHeader header = (HeapTupleHeader)tuple.data;
	BufferHeapTupleTableSlot empty = { .base.base.tts_ops = &TTSOpsBufferHeapTuple };
	memset(&rel, 0, sizeof(rel));
	memset(&relform, 0, sizeof(relform));
	memcpy(&slot, &empty, sizeof(slot));
	memset(tuple.data, 0, sizeof(tuple.data));
	rel.rd_rel = &relform;
	rel.rd_id = 1259;
	PageInit(current_page.data, BLCKSZ, 0);
	header->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(header, 900);
	HeapTupleHeaderSetXmax(header, 901);
	ItemPointerSet(&header->t_ctid, 41, 1);
	set_payload(header, 11);
	selected_tuple.t_len = header->t_hoff + sizeof(int);
	UT_ASSERT_EQ(PageAddItem(current_page.data, tuple.data, selected_tuple.t_len, 1, false, true),
				 1);
	ItemPointerSet(&selected_tuple.t_self, 41, 1);
	selected_tuple.t_tableOid = rel.rd_id;
	memcpy(selected_bytes.data, current_tuple(), selected_tuple.t_len);
	/* Statistics can change between the scan's observation and current X. */
	set_payload(current_tuple(), 22);
	pins = reads = scans = ends = waits = visibility_calls = finishes = 0;
	invals = forgets = snapshot_invalidations = error_count = assertion_count = 0;
	content_locked = tuple_locked = parallel = no_row = move_on_lock = false;
	owned = materialized;
	change_on_lock = 0;
	first_verdict = later_verdict = TM_Ok;
	expect_error = false;
}
static bool
begin_case(HeapTuple *copy, void **state)
{
	volatile bool caught = false;
	PG_TRY();
	{
		systable_inplace_update_begin(&rel, 1, true, NULL, 0, NULL, copy, state);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, expect_error);
	return !caught;
}
static void
assert_released(void)
{
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(!content_locked);
	UT_ASSERT(!tuple_locked);
	UT_ASSERT_EQ(ends, scans);
}
UT_TEST(native_pinned_finish)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(false);
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT_EQ(payload(copy->t_data), 22);
	set_payload(copy->t_data, 33);
	systable_inplace_update_finish(state, copy);
	UT_ASSERT_EQ(payload(current_tuple()), 33);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(finishes, 1);
	heap_freetuple(copy);
	assert_released();
}
UT_TEST(materialized_finish_uses_current_not_owned_bytes)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(payload(copy->t_data), 22);
	UT_ASSERT_EQ(payload((HeapTupleHeader)selected_bytes.data), 11);
	UT_ASSERT(!BufferIsValid(slot.buffer));
	UT_ASSERT(TTS_SHOULDFREE(&slot.base.base));
	set_payload(copy->t_data, 44);
	systable_inplace_update_finish(state, copy);
	UT_ASSERT_EQ(payload(current_tuple()), 44);
	heap_freetuple(copy);
	assert_released();
}
UT_TEST(materialized_cancel_preserves_current)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	if (!begin_case(&copy, &state))
		return;
	set_payload(copy->t_data, 77);
	systable_inplace_update_cancel(state);
	UT_ASSERT_EQ(payload(current_tuple()), 22);
	UT_ASSERT_EQ(finishes, 0);
	heap_freetuple(copy);
	assert_released();
}
UT_TEST(relocated_same_creation_rebinds_before_visibility)
{
	PGAlignedBlock tuple;
	ItemIdData item;
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	/* Build a different physical placement of the same logical tuple. */
	memcpy(tuple.data, current_tuple(), selected_tuple.t_len);
	PageInit(moved_page.data, BLCKSZ, 0);
	UT_ASSERT_EQ(PageAddItem(moved_page.data, tuple.data, selected_tuple.t_len, 1, false, true), 1);
	UT_ASSERT_EQ(PageAddItem(moved_page.data, tuple.data, selected_tuple.t_len, 2, false, true), 2);
	item = *PageGetItemId(moved_page.data, 1);
	*PageGetItemId(moved_page.data, 1) = *PageGetItemId(moved_page.data, 2);
	*PageGetItemId(moved_page.data, 2) = item;
	HeapTupleHeaderSetXmin(
		(HeapTupleHeader)PageGetItem(moved_page.data, PageGetItemId(moved_page.data, 2)), 999);
	UT_ASSERT(ItemIdGetOffset(PageGetItemId(moved_page.data, 1))
			  != ItemIdGetOffset(PageGetItemId(current_page.data, 1)));
	move_on_lock = true;
	if (!begin_case(&copy, &state))
		return;
	set_payload(copy->t_data, 55);
	systable_inplace_update_finish(state, copy);
	UT_ASSERT_EQ(payload(current_tuple()), 55);
	UT_ASSERT_EQ(payload((HeapTupleHeader)PageGetItem(current_page.data,
													  PageGetItemId(current_page.data, 2))),
				 22);
	heap_freetuple(copy);
	assert_released();
}
static void
retry_case(TM_Result verdict, bool multi)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	first_verdict = verdict;
	if (multi)
		current_tuple()->t_infomask |= HEAP_XMAX_IS_MULTI;
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT_EQ(scans, 2);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(waits, verdict == TM_BeingModified ? 1 : 0);
	UT_ASSERT_EQ(snapshot_invalidations, 1);
	systable_inplace_update_cancel(state);
	heap_freetuple(copy);
	assert_released();
}
UT_TEST(updated_releases_before_rescan)
{
	retry_case(TM_Updated, false);
}
UT_TEST(conflicting_xid_releases_before_wait)
{
	retry_case(TM_BeingModified, false);
}
UT_TEST(conflicting_mx_releases_before_wait)
{
	retry_case(TM_BeingModified, true);
}
UT_TEST(recycled_selection_restarts_without_mutating_replacement)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	change_on_lock = 1;
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT_EQ(scans, 2);
	UT_ASSERT_EQ(visibility_calls, 1);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(copy->t_data), 902);
	UT_ASSERT_EQ(payload(current_tuple()), 22);
	systable_inplace_update_cancel(state);
	heap_freetuple(copy);
	assert_released();
}
UT_TEST(absent_selection_rescans_to_no_row)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	change_on_lock = 2;
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT(copy == NULL);
	UT_ASSERT(state == NULL);
	UT_ASSERT_EQ(scans, 2);
	UT_ASSERT_EQ(visibility_calls, 0);
	assert_released();
}
UT_TEST(empty_scan_does_not_acquire_current)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	no_row = true;
	if (!begin_case(&copy, &state))
		return;
	UT_ASSERT(copy == NULL);
	UT_ASSERT_EQ(reads, 0);
	assert_released();
}
UT_TEST(parallel_prohibition_retained)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(true);
	parallel = expect_error = true;
	UT_ASSERT(!begin_case(&copy, &state));
	UT_ASSERT_EQ(scans, 0);
	UT_ASSERT_EQ(reads, 0);
}
UT_TEST(invisible_verdict_still_errors)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(false);
	first_verdict = TM_Invisible;
	expect_error = true;
	UT_ASSERT(!begin_case(&copy, &state));
	UT_ASSERT_EQ(assertion_count, 0);
	UT_ASSERT_EQ(finishes, 0);
}
UT_TEST(corrupt_page_is_not_a_retry)
{
	HeapTuple copy = NULL;
	void *state = NULL;
	reset_case(false);
	change_on_lock = 3;
	expect_error = true;
	UT_ASSERT(!begin_case(&copy, &state));
	UT_ASSERT_EQ(assertion_count, 0);
	UT_ASSERT_EQ(visibility_calls, 0);
	UT_ASSERT_EQ(finishes, 0);
}
int
main(void)
{
	UT_PLAN(13);
	UT_RUN(native_pinned_finish);
	UT_RUN(materialized_finish_uses_current_not_owned_bytes);
	UT_RUN(materialized_cancel_preserves_current);
	UT_RUN(relocated_same_creation_rebinds_before_visibility);
	UT_RUN(updated_releases_before_rescan);
	UT_RUN(conflicting_xid_releases_before_wait);
	UT_RUN(conflicting_mx_releases_before_wait);
	UT_RUN(recycled_selection_restarts_without_mutating_replacement);
	UT_RUN(absent_selection_rescans_to_no_row);
	UT_RUN(empty_scan_does_not_acquire_current);
	UT_RUN(parallel_prohibition_retained);
	UT_RUN(invisible_verdict_still_errors);
	UT_RUN(corrupt_page_is_not_a_retry);
	UT_DONE();
	return ut_failed_count != 0;
}
