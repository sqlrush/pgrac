/* Real, extracted heapam_tuple_lock consumer; only external boundaries are stubs. */
#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
const TupleTableSlotOps TTSOpsBufferHeapTuple = { 0 };
static HeapTupleHeaderData old_header, fetched_header;
static int leg, pins, locks, fetches, waits, native_waits, stored;
static uint64 *first_deadline;
static PGAlignedBlock fetch_pages[7];
int NBuffers = 7;
int NLocBuffer = 0;
int old_snapshot_threshold = -1;
char *BufferBlocks = (char *)fetch_pages;
Block *LocalBufferBlockPointers;
static bool content_locked;
static int proved_entry_calls, dispatch_entry_calls;

TM_Result
HeapTupleSatisfiesUpdate(HeapTuple tuple, CommandId cid, Buffer buffer)
{
	UT_ASSERT(content_locked);
	proved_entry_calls++;
	return TM_Invisible; /* only distinguish entry selection, not model TT proof */
}

TM_Result
HeapTupleSatisfiesUpdateForWriter(HeapTuple tuple, CommandId cid, Buffer buffer)
{
	UT_ASSERT(content_locked);
	dispatch_entry_calls++;
	return TM_BeingModified;
}

static void
check_lock_dispatch(LockTupleMode mode, uint16 infomask, bool needs_proved_entry)
{
	HeapTupleHeaderData header = { 0 }, before;
	HeapTupleData tuple_data = { .t_data = &header };
	HeapTuple tuple = &tuple_data;
	Buffer selected_buffer = 7, *buffer = &selected_buffer;
	CommandId cid = 1;
	TM_Result result = TM_Ok;

	header.t_infomask = infomask;
	HeapTupleHeaderSetXmax(&header, 900);
	before = header;
	proved_entry_calls = dispatch_entry_calls = 0;
	content_locked = true;
#include "test_cluster_heap_lock_dispatch.inc"
	content_locked = false;
	UT_ASSERT_EQ(proved_entry_calls, needs_proved_entry ? 1 : 0);
	UT_ASSERT_EQ(dispatch_entry_calls, needs_proved_entry ? 0 : 1);
	UT_ASSERT_EQ(result, needs_proved_entry ? TM_Invisible : TM_BeingModified);
	UT_ASSERT_EQ(memcmp(&before, &header, sizeof(header)), 0);
}

UT_TEST(test_keyshare_keeps_proved_entry_for_every_plain_holder)
{
	check_lock_dispatch(LockTupleKeyShare, 0, true);
	check_lock_dispatch(LockTupleKeyShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK, true);
	check_lock_dispatch(LockTupleKeyShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK, true);
	check_lock_dispatch(LockTupleKeyShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, true);
}
UT_TEST(test_share_compatible_locks_cannot_use_dispatch_only)
{
	check_lock_dispatch(LockTupleShare, 0, false);
	check_lock_dispatch(LockTupleShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK, true);
	check_lock_dispatch(LockTupleShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK, true);
	check_lock_dispatch(LockTupleShare, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, false);
}
UT_TEST(test_nokeyexclusive_compatible_lock_cannot_use_dispatch_only)
{
	check_lock_dispatch(LockTupleNoKeyExclusive, 0, false);
	check_lock_dispatch(LockTupleNoKeyExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK, true);
	check_lock_dispatch(LockTupleNoKeyExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK, false);
	check_lock_dispatch(LockTupleNoKeyExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, false);
}
UT_TEST(test_exclusive_conflicting_locks_retain_exact_wait_dispatch)
{
	check_lock_dispatch(LockTupleExclusive, 0, false);
	check_lock_dispatch(LockTupleExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK, false);
	check_lock_dispatch(LockTupleExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK, false);
	check_lock_dispatch(LockTupleExclusive, HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, false);
}

bool
ItemPointerEquals(ItemPointer a, ItemPointer b)
{
	return ItemPointerGetBlockNumber(a) == ItemPointerGetBlockNumber(b)
		   && ItemPointerGetOffsetNumber(a) == ItemPointerGetOffsetNumber(b);
}

void
ExceptionalCondition(const char *c, const char *f, int l)
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
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
pg_re_throw(void)
{
	errfinish(__FILE__, __LINE__, __func__);
}
bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	return false;
}
CommandId
HeapTupleHeaderGetCmin(HeapTupleHeader tuple)
{
	return 0;
}
TransactionId
HeapTupleGetUpdateXid(HeapTupleHeader tuple)
{
	return 901;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 7);
	UT_ASSERT(pins > 0);
	UT_ASSERT(!content_locked);
	pins--;
}

TM_Result
heap_lock_tuple(Relation relation, HeapTuple tuple, CommandId cid, LockTupleMode mode,
				LockWaitPolicy policy, bool follow_updates, Buffer *buffer, TM_FailureData *tmfd,
				const ClusterHeapSuccessorProof *expected, ClusterHeapSuccessorProof *next)
{
	locks++;
	UT_ASSERT_EQ(pins, 0);
	*buffer = 7;
	pins++;
	if (locks == 1) {
		tuple->t_data = &old_header;
		tmfd->xmax = 900;
		ItemPointerSet(&tmfd->ctid, 41, 2);
		if (leg == 5) {
			next->valid = true;
			next->tid = tmfd->ctid;
		}
		return TM_Updated;
	}
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_self), 2);
	UT_ASSERT_EQ(expected != NULL, leg == 5);
	tuple->t_data = &fetched_header;
	return TM_Ok;
}

Buffer
ReadBuffer(Relation relation, BlockNumber block)
{
	Page page = (Page)fetch_pages[6].data;
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;

	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT_EQ(block, 41);
	fetches++;
	pins++;
	memset(page, 0, BLCKSZ);
	header->pd_lower = SizeOfPageHeaderData + 2 * sizeof(ItemIdData);
	header->pd_upper = BLCKSZ - MAXALIGN(sizeof(HeapTupleHeaderData));
	header->pd_special = BLCKSZ;
	item = PageGetItemId(page, 2);
	ItemIdSetNormal(item, header->pd_upper, sizeof(HeapTupleHeaderData));
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	HeapTupleHeaderSetXmin(tuple, leg == 3 || (leg == 6 && fetches == 2) ? 902 : 900);
	return 7;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 7);
	UT_ASSERT(mode == BUFFER_LOCK_SHARE || mode == BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(content_locked, mode == BUFFER_LOCK_UNLOCK);
	content_locked = mode != BUFFER_LOCK_UNLOCK;
}

static bool
visibility_fixture(HeapTuple tuple, Snapshot snapshot, Buffer buffer, bool *remote,
				   ClusterTxLocator *locator)
{
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT(content_locked);
	UT_ASSERT_EQ(snapshot->snapshot_type, SNAPSHOT_DIRTY);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_self), 2);
	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	if (remote != NULL) {
		*remote = false;
		memset(locator, 0, sizeof(*locator));
	}
	if (fetches == 1 && leg != 5) {
		if (leg == 4)
			snapshot->xmax = 901; /* explicit local control */
		else if (remote != NULL) {
			*remote = true;
			locator->xid = 901;
			locator->tt_wrap = 22; /* page wrap is not canonical TT wrap */
		} else
			ereport(ERROR, (errmsg("remote Dirty consumer omitted its typed wait channel")));
	}
	return true;
}

bool
HeapTupleSatisfiesVisibility(HeapTuple tuple, Snapshot snapshot, Buffer buffer)
{
	return visibility_fixture(tuple, snapshot, buffer, NULL, NULL);
}

bool
cluster_heap_tuple_satisfies_visibility_waitable(HeapTuple tuple, Snapshot snapshot, Buffer buffer,
												 bool *remote, ClusterTxLocator *locator)
{
	return visibility_fixture(tuple, snapshot, buffer, remote, locator);
}

void
TestForOldSnapshot_impl(Snapshot snapshot, Relation relation)
{
	UT_ASSERT(false);
}
void
PredicateLockTID(Relation relation, ItemPointer tid, Snapshot snapshot, TransactionId xid)
{
	UT_ASSERT(content_locked);
}
void
HeapCheckForSerializableConflictOut(bool visible, Relation relation, HeapTuple tuple, Buffer buffer,
									Snapshot snapshot)
{
	UT_ASSERT(content_locked);
}

#include "test_cluster_heap_fetch_consumer.inc"

bool
cluster_heap_wait_successor(const ClusterTxLocator *locator, LockWaitPolicy policy,
							uint64 *deadline)
{
	UT_ASSERT_EQ(pins, 0); /* no fetched tuple/pin survives the wait */
	UT_ASSERT_EQ(locator->xid, 901);
	waits++;
	if (first_deadline == NULL) {
		first_deadline = deadline;
		UT_ASSERT_EQ(*deadline, 0);
		*deadline = 1000;
	}
	UT_ASSERT(deadline == first_deadline);
	if (policy == LockWaitError)
		ereport(ERROR, (errmsg("could not obtain lock on row")));
	return policy != LockWaitSkip;
}

void
XactLockTableWait(TransactionId xid, Relation relation, ItemPointer tid, XLTW_Oper oper)
{
	UT_ASSERT_EQ(leg, 4);
	UT_ASSERT_EQ(pins, 0);
	native_waits++;
}
bool
ConditionalXactLockTableWait(TransactionId xid)
{
	UT_ASSERT(false);
	return false;
}
TupleTableSlot *
ExecStorePinnedBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
	UT_ASSERT_EQ(pins, 1);
	stored++;
	return slot;
}

#include "test_cluster_heap_epq_consumer.inc"

static void
run_leg(int which)
{
	RelationData relation = { 0 };
	BufferHeapTupleTableSlot slot = { .base.base.tts_ops = &TTSOpsBufferHeapTuple };
	SnapshotData snapshot = { 0 };
	TM_FailureData failure = { 0 };
	ItemPointerData tid;
	volatile bool caught = false;
	volatile TM_Result result = TM_Invisible;
	LockWaitPolicy policy = which == 1 ? LockWaitSkip : which == 2 ? LockWaitError : LockWaitBlock;

	leg = which;
	pins = locks = fetches = waits = native_waits = stored = 0;
	content_locked = false;
	first_deadline = NULL;
	memset(&old_header, 0, sizeof(old_header));
	relation.rd_id = 123;
	ItemPointerSet(&tid, 41, 1);
	PG_TRY();
	{
		result
			= heapam_tuple_lock(&relation, &tid, &snapshot, &slot.base.base, 1, LockTupleExclusive,
								policy, TUPLE_LOCK_FLAG_FIND_LAST_VERSION, &failure);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, leg == 2);
	if (leg == 2) {
		UT_ASSERT_EQ(pins, 0);
		UT_ASSERT_EQ(waits, 1);
		return;
	}
	UT_ASSERT_EQ(native_waits, leg == 4 ? 1 : 0);
	UT_ASSERT_EQ(waits, leg == 3 || leg == 4 || leg == 5 ? 0 : 1);
	if (leg == 1 || leg == 3 || leg == 6) {
		UT_ASSERT_EQ(result, leg == 1 ? TM_WouldBlock : TM_Deleted);
		UT_ASSERT_EQ(pins, 0);
		UT_ASSERT_EQ(stored, 0);
	} else {
		UT_ASSERT_EQ(result, TM_Ok);
		UT_ASSERT_EQ(locks, 2);
		UT_ASSERT_EQ(fetches, leg == 5 ? 0 : 2);
		UT_ASSERT_EQ(stored, 1);
		ReleaseBuffer(7);
	}
}

UT_TEST(test_remote_wait_refetches_without_pin)
{
	run_leg(0);
}
UT_TEST(test_skip_drops_pin_and_returns_would_block)
{
	run_leg(1);
}
UT_TEST(test_nowait_preserves_error_and_drops_pin)
{
	run_leg(2);
}
UT_TEST(test_reused_successor_never_waits)
{
	run_leg(3);
}
UT_TEST(test_local_dirty_wait_is_unchanged)
{
	run_leg(4);
}
UT_TEST(test_multixact_successor_proof_bypasses_dirty)
{
	run_leg(5);
}
UT_TEST(test_after_wait_refetch_rechecks_xmin)
{
	run_leg(6);
}
UT_TEST(test_fetch_miss_clears_wait_output_and_releases_pin)
{
	RelationData relation = { 0 };
	SnapshotData snapshot = { .snapshot_type = SNAPSHOT_DIRTY };
	HeapTupleData tuple = { 0 };
	ClusterTxLocator locator, zero = { 0 };
	Buffer buffer = 7;
	bool remote = true;

	leg = pins = fetches = 0;
	content_locked = false;
	memset(&locator, 0xff, sizeof(locator));
	ItemPointerSet(&tuple.t_self, 41, 3); /* actual fixture page has only two LPs */
	UT_ASSERT(!cluster_heap_fetch_waitable(&relation, &snapshot, &tuple, &buffer, true, &remote,
										   &locator));
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(!content_locked && !remote);
	UT_ASSERT(tuple.t_data == NULL);
	UT_ASSERT_EQ(memcmp(&locator, &zero, sizeof(locator)), 0);
}
int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_remote_wait_refetches_without_pin);
	UT_RUN(test_skip_drops_pin_and_returns_would_block);
	UT_RUN(test_nowait_preserves_error_and_drops_pin);
	UT_RUN(test_reused_successor_never_waits);
	UT_RUN(test_local_dirty_wait_is_unchanged);
	UT_RUN(test_multixact_successor_proof_bypasses_dirty);
	UT_RUN(test_after_wait_refetch_rechecks_xmin);
	UT_RUN(test_fetch_miss_clears_wait_output_and_releases_pin);
	UT_RUN(test_keyshare_keeps_proved_entry_for_every_plain_holder);
	UT_RUN(test_share_compatible_locks_cannot_use_dispatch_only);
	UT_RUN(test_nokeyexclusive_compatible_lock_cannot_use_dispatch_only);
	UT_RUN(test_exclusive_conflicting_locks_retain_exact_wait_dispatch);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
