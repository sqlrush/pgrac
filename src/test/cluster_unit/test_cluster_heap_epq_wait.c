/* Real, extracted heapam_tuple_lock consumer; only external boundaries are stubs. */
#include "postgres.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/tsmapi.h"
#include "cluster/cluster_mode.h"
#include "nodes/execnodes.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "access/syncscan.h"
#include "access/valid.h"
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
static void clear_test_slot(TupleTableSlot *slot);
const TupleTableSlotOps TTSOpsBufferHeapTuple = { .clear = clear_test_slot };
static HeapTupleHeaderData old_header, fetched_header;
static int leg, pins, locks, fetches, waits, native_waits, stored;
static uint64 *first_deadline;
static PGAlignedBlock fetch_pages[7];
int NBuffers = 7;
int NLocBuffer = 0;
int old_snapshot_threshold = -1;
bool cluster_enabled;
int cluster_node_id = 0;
char *BufferBlocks = (char *)fetch_pages;
Block *LocalBufferBlockPointers;
static bool content_locked;
static int proved_entry_calls, dispatch_entry_calls;
static bool replace_on_unlock;
static bool replace_on_lock;
static bool scan_lifetime_case;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
volatile uint32 CritSectionCount;
static PGAlignedBlock replacement_page;
static PGAlignedBlock stored_row;
static HeapTupleData stored_tuple;
int XactIsoLevel = XACT_READ_COMMITTED;
static bool snapshot_recheck_case;
static int snapshot_current_change;
static int snapshot_visibility_calls;
bool synchronize_seqscans;
static int scan_additional_page;
static bool self_xid_case;
static bool rewrite_recheck_case;
static bool capture_index_log, finishing_index_log;
static char index_log[512];
static int index_log_count, index_tids_left, index_fetch_count, index_found_on;
static void
clear_test_slot(TupleTableSlot *slot)
{
	slot->tts_flags |= TTS_FLAG_EMPTY;
}

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
	if (snapshot_recheck_case && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool
errstart(int level, const char *domain)
{
	if (capture_index_log && level == LOG) {
		finishing_index_log = true;
		return true;
	}
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
	if (finishing_index_log) {
		finishing_index_log = false;
		return;
	}
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
	return self_xid_case && xid == 900;
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
	tuple->t_len = sizeof(HeapTupleHeaderData);
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

/* External row-lock verdict seam. The actual output-unlock producer is
 * independently exercised below, not implemented by this stub. */
TM_Result
cluster_heap_lock_tuple_owned(Relation relation, HeapTuple tuple, CommandId cid, LockTupleMode mode,
							  LockWaitPolicy policy, bool follow_updates, Buffer *buffer,
							  TM_FailureData *tmfd, const ClusterHeapSuccessorProof *expected,
							  ClusterHeapSuccessorProof *next, char storage[BLCKSZ])
{
	TM_Result result = heap_lock_tuple(relation, tuple, cid, mode, policy, follow_updates, buffer,
									   tmfd, expected, next);
	if (storage != NULL) {
		memcpy(storage, tuple->t_data, tuple->t_len);
		tuple->t_data = (HeapTupleHeader)storage;
	}
	return result;
}

Buffer
ReadBuffer(Relation relation, BlockNumber block)
{
	Page page = (Page)fetch_pages[6].data;
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;

	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(block == 41 || (scan_lifetime_case && block == 40));
	fetches++;
	pins++;
	if (leg == 7) {
		PGAlignedBlock bytes;
		OffsetNumber old_offset;

		/* Actual page operations: keep logical successor 41/2, move its
		 * bytes, then insert a different row at its old physical address. */
		fetches = 2;
		PageInit(page, BLCKSZ, 0);
		memset(bytes.data, 0, BLCKSZ);
		tuple = (HeapTupleHeader)bytes.data;
		tuple->t_hoff = SizeofHeapTupleHeader;
		HeapTupleHeaderSetXmin(tuple, 901);
		UT_ASSERT_EQ(PageAddItem(page, bytes.data, sizeof(*tuple), 1, false, true), 1);
		HeapTupleHeaderSetXmin(tuple, block == 40 ? 903 : 900);
		ItemPointerSet(&tuple->t_ctid, block, 2);
		UT_ASSERT_EQ(PageAddItem(page, bytes.data, sizeof(*tuple), 2, false, true), 2);
		old_offset = ItemIdGetOffset(PageGetItemId(page, 2));
		memcpy(replacement_page.data, page, BLCKSZ);
		ItemIdSetUnused(PageGetItemId((Page)replacement_page.data, 1));
		PageRepairFragmentation((Page)replacement_page.data);
		UT_ASSERT(ItemIdGetOffset(PageGetItemId((Page)replacement_page.data, 2)) != old_offset);
		HeapTupleHeaderSetXmin(tuple, 902);
		ItemPointerSet(&tuple->t_ctid, 41, 1);
		UT_ASSERT_EQ(
			PageAddItem((Page)replacement_page.data, bytes.data, sizeof(*tuple), 1, true, true), 1);
		UT_ASSERT_EQ(PageGetMaxOffsetNumber((Page)replacement_page.data), 2);
		UT_ASSERT_EQ(ItemIdGetOffset(PageGetItemId((Page)replacement_page.data, 1)), old_offset);
		if (snapshot_recheck_case) {
			item = PageGetItemId(page, 2);
			tuple = (HeapTupleHeader)PageGetItem(page, item);
			if (snapshot_current_change == 1)
				tuple->t_infomask ^= HEAP_XMAX_INVALID;
			else if (snapshot_current_change == 2)
				ItemIdSetUnused(item);
			else if (snapshot_current_change == 3)
				item->lp_len--;
		}
		return 7;
	}
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
	if (rewrite_recheck_case && buffer == InvalidBuffer && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	UT_ASSERT_EQ(buffer, 7);
	UT_ASSERT(mode == BUFFER_LOCK_SHARE || mode == BUFFER_LOCK_EXCLUSIVE
			  || mode == BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(content_locked, mode == BUFFER_LOCK_UNLOCK);
	content_locked = mode != BUFFER_LOCK_UNLOCK;
	if (mode == BUFFER_LOCK_EXCLUSIVE && replace_on_lock)
		memcpy(fetch_pages[6].data, replacement_page.data, BLCKSZ);
	if (mode == BUFFER_LOCK_UNLOCK && replace_on_unlock) {
		UT_ASSERT_EQ(leg, 7);
		UT_ASSERT_EQ(pins, 1);
		/* External image delivery is the only interleaving seam here. */
		memcpy(fetch_pages[6].data, replacement_page.data, BLCKSZ);
	}
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
	if (snapshot_recheck_case) {
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(pins, 1);
		UT_ASSERT(tuple->t_data
				  == (HeapTupleHeader)PageGetItem(BufferGetPage(buffer),
												  PageGetItemId(BufferGetPage(buffer), 2)));
		snapshot_visibility_calls++;
		return true;
	}
	if (scan_lifetime_case) {
		UT_ASSERT(content_locked);
		return ItemPointerGetOffsetNumber(&tuple->t_self) == 2;
	}
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

void
ExecForceStoreHeapTuple(HeapTuple tuple, TupleTableSlot *slot, bool should_free)
{
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT(!content_locked);
	UT_ASSERT(!should_free);
	UT_ASSERT(tuple->t_data
			  != (HeapTupleHeader)PageGetItem((Page)fetch_pages[6].data,
											  PageGetItemId((Page)fetch_pages[6].data, 2)));
	stored_tuple = *tuple;
	memcpy(stored_row.data, tuple->t_data, tuple->t_len);
	stored_tuple.t_data = (HeapTupleHeader)stored_row.data;
	stored++;
}

TupleTableSlot *
ExecStoreBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
	/* Executor boundary: the native method borrows bytes. Actual materializing
	 * executor methods are independently tested by the HOT suite. */
	stored_tuple = *tuple;
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
static void
check_epq_read_lifetime(bool replace)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	BufferHeapTupleTableSlot slot = { .base.base.tts_ops = &TTSOpsBufferHeapTuple };
	SnapshotData snapshot = { 0 };
	TM_FailureData failure = { 0 };
	ItemPointerData tid;
	TM_Result result;
	HeapTupleHeader successor;

	leg = 7;
	pins = locks = fetches = waits = native_waits = stored = 0;
	content_locked = false;
	first_deadline = NULL;
	replace_on_unlock = replace;
	memset(&old_header, 0, sizeof(old_header));
	relation.rd_id = 123;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	cluster_enabled = true;
	ItemPointerSet(&tid, 41, 1);
	result = heapam_tuple_lock(&relation, &tid, &snapshot, &slot.base.base, 1, LockTupleExclusive,
							   LockWaitBlock, TUPLE_LOCK_FLAG_FIND_LAST_VERSION, &failure);
	successor = (HeapTupleHeader)PageGetItem((Page)fetch_pages[6].data,
											 PageGetItemId((Page)fetch_pages[6].data, 2));
	replace_on_unlock = false;
	cluster_enabled = false;
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(successor), 900);
	UT_ASSERT_EQ(result, TM_Ok);
	UT_ASSERT_EQ(locks, 2);
	if (pins)
		ReleaseBuffer(7);
}

UT_TEST(test_epq_no_replacement_control)
{
	check_epq_read_lifetime(false);
}
UT_TEST(test_epq_relocated_live_successor_is_not_deleted)
{
	check_epq_read_lifetime(true);
}

static void
check_locked_output_lifetime(bool replace)
{
	RelationData relation = { 0 };
	HeapTupleData value = { 0 };
	HeapTuple tuple = &value;
	Buffer selected, *buffer = &selected;
	PGAlignedBlock output;
	char *owned_storage = output.data;
	Page page;
	ItemId lp;

	leg = 7;
	pins = fetches = 0;
	content_locked = false;
	replace_on_unlock = replace;
	selected = ReadBuffer(&relation, 41);
	page = BufferGetPage(selected);
	lp = PageGetItemId(page, 2);
	tuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
	tuple->t_len = ItemIdGetLength(lp);
	ItemPointerSet(&tuple->t_self, 41, 2);
	LockBuffer(selected, BUFFER_LOCK_SHARE);
	/* Actual common row-lock exit. The verdict itself is an external
	 * boundary: this witness tests its bytes across content unlock. */
#include "test_cluster_heap_lock_output.inc"
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple->t_data), 900);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_data->t_ctid), 2);
	replace_on_unlock = false;
	ReleaseBuffer(selected);
	(void)owned_storage;
}

UT_TEST(test_lock_output_without_replacement)
{
	check_locked_output_lifetime(false);
}
UT_TEST(test_lock_output_survives_unlock_relocation)
{
	check_locked_output_lifetime(true);
}

static bool
cluster_xwait_has_remote_evidence(Page page, HeapTupleHeader tuple, TransactionId xwait,
								  uint16 infomask)
{
	UT_ASSERT(content_locked);
	return false;
}

int
GetMultiXactIdMembers(MultiXactId multi, MultiXactMember **members, bool allow_old, bool lock_only)
{
	UT_ASSERT(false); /* This witness uses the real plain self-lock branch. */
	return 0;
}

void
pfree(void *pointer)
{
	UT_ASSERT(false);
}
#include "test_cluster_heap_lock_strength.inc"

UT_TEST(test_compatible_self_lock_keeps_owned_output_after_unlock)
{
	RelationData relation = { 0 };
	HeapTupleData value = { 0 };
	HeapTuple tuple = &value;
	Buffer selected, *buffer = &selected;
	PGAlignedBlock output;
	char *owned_storage = output.data;
	TransactionId xwait = 900;
	uint16 infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK;
	uint16 infomask2 = 0;
	bool first_time = true, skip_tuple_lock = false, cluster_xwait_remote;
	LockTupleMode mode = LockTupleKeyShare;
	TM_Result result = TM_Invisible;
	ItemId lp;

	leg = 7;
	pins = fetches = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	self_xid_case = true;
	selected = ReadBuffer(&relation, 41);
	lp = PageGetItemId(BufferGetPage(selected), 2);
	tuple->t_data = (HeapTupleHeader)PageGetItem(BufferGetPage(selected), lp);
	tuple->t_len = ItemIdGetLength(lp);
	ItemPointerSet(&tuple->t_self, 41, 2);
	LockBuffer(selected, BUFFER_LOCK_EXCLUSIVE);
	replace_on_unlock = true;
#include "test_cluster_heap_lock_self.inc"
	UT_ASSERT(false); /* The actual compatible-self branch must return early. */
#include "test_cluster_heap_lock_output.inc"
	UT_ASSERT_EQ(result, TM_Ok);
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple->t_data), 900);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_data->t_ctid), 2);
	replace_on_unlock = self_xid_case = false;
	ReleaseBuffer(selected);
	(void)skip_tuple_lock;
}

#include "test_cluster_heap_lock_rebind.inc"

UT_TEST(test_lock_reacquisition_resolves_same_tid_after_relocation)
{
	RelationData rel = { 0 };
	Relation relation = &rel;
	FormData_pg_class form = { 0 };
	HeapTupleData value = { 0 };
	HeapTuple tuple = &value;
	Buffer selected, *buffer = &selected;
	TransactionId creation_xmin = 900;
	Page page;
	ItemId lp;

	leg = 7;
	pins = fetches = 0;
	content_locked = false;
	replace_on_unlock = false;
	replace_on_lock = true;
	cluster_enabled = true;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	selected = ReadBuffer(relation, 41);
	page = BufferGetPage(selected);
	lp = PageGetItemId(page, 2);
	tuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
	tuple->t_len = ItemIdGetLength(lp);
	ItemPointerSet(&tuple->t_self, 41, 2);
#include "test_cluster_heap_lock_reacquire.inc"
	UT_ASSERT(content_locked);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple->t_data), 900);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_data->t_ctid), 2);
	UT_ASSERT(tuple->t_data
			  == (HeapTupleHeader)PageGetItem(BufferGetPage(selected),
											  PageGetItemId(BufferGetPage(selected), 2)));
	LockBuffer(selected, BUFFER_LOCK_UNLOCK);
	replace_on_lock = false;
	cluster_enabled = false;
	ReleaseBuffer(selected);
	(void)creation_xmin;
}

UT_TEST(test_lock_rebind_rejects_missing_or_changed_identity_without_mutation)
{
	int invalid;
	for (invalid = 0; invalid < 6; invalid++) {
		RelationData relation = { 0 };
		FormData_pg_class form = { 0 };
		HeapTupleData tuple = { 0 };
		TransactionId creation_xmin = 900;
		Buffer buffer;
		Page page;
		ItemId lp;
		PGAlignedBlock before;
		volatile bool caught = false;

		leg = 7;
		pins = fetches = 0;
		content_locked = replace_on_lock = replace_on_unlock = false;
		cluster_enabled = true;
		relation.rd_rel = &form;
		form.relpersistence = RELPERSISTENCE_PERMANENT;
		buffer = ReadBuffer(&relation, 41);
		page = BufferGetPage(buffer);
		lp = PageGetItemId(page, 2);
		ItemPointerSet(&tuple.t_self, 41, 2);
		if (invalid == 0)
			ItemIdSetUnused(lp);
		else if (invalid == 1)
			ItemIdSetNormal(lp, BLCKSZ - 1, sizeof(HeapTupleHeaderData));
		else if (invalid == 2)
			ItemPointerSet(&tuple.t_self, 41, 3);
		else if (invalid == 3)
			HeapTupleHeaderSetXmin((HeapTupleHeader)PageGetItem(page, lp), 901);
		else if (invalid == 4)
			((PageHeader)page)->pd_lower = BLCKSZ;
		else {
			creation_xmin = InvalidTransactionId;
			HeapTupleHeaderSetXmin((HeapTupleHeader)PageGetItem(page, lp), InvalidTransactionId);
		}
		memcpy(before.data, page, BLCKSZ);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		PG_TRY();
		{
			cluster_heap_rebind_locked_tuple(&relation, buffer, &tuple, &creation_xmin);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
		UT_ASSERT(tuple.t_data == NULL);
		UT_ASSERT_EQ(creation_xmin, invalid == 5 ? InvalidTransactionId : 900);
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
		cluster_enabled = false;
	}
}

void
ProcessInterrupts(void)
{
	UT_ASSERT(false);
}

Buffer
ReadBufferExtended(Relation relation, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	return ReadBuffer(relation, block);
}

void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	UT_ASSERT(!content_locked);
}
void
pgstat_assoc_relation(Relation relation)
{
	UT_ASSERT(false);
}

/* Block allocation is external to this one-page lifetime witness. */
static BlockNumber
heapgettup_initial_block(HeapScanDesc scan, ScanDirection dir)
{
	return 41;
}
static BlockNumber
heapgettup_advance_block(HeapScanDesc scan, BlockNumber block, ScanDirection dir)
{
	if (scan_additional_page > 0) {
		scan_additional_page--;
		return 40;
	}
	return InvalidBlockNumber;
}

BlockNumber
RelationGetNumberOfBlocksInFork(Relation relation, ForkNumber fork)
{
	return 42;
}
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType type)
{
	UT_ASSERT(false);
	return NULL;
}
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	UT_ASSERT(false);
}
BlockNumber
ss_get_location(Relation relation, BlockNumber nblocks)
{
	UT_ASSERT(false);
	return 0;
}
int32
ItemPointerCompare(ItemPointer a, ItemPointer b)
{
	BlockNumber ab = ItemPointerGetBlockNumber(a), bb = ItemPointerGetBlockNumber(b);
	if (ab != bb)
		return ab < bb ? -1 : 1;
	return (int32)ItemPointerGetOffsetNumber(a) - ItemPointerGetOffsetNumber(b);
}

/* No scan keys in these lifetime witnesses; fail if an unrelated expression
 * evaluation is accidentally reached. */
Datum
FunctionCall2Coll(FmgrInfo *info, Oid collation, Datum a, Datum b)
{
	UT_ASSERT(false);
	return 0;
}
Datum
nocachegetattr(HeapTuple tuple, int attnum, TupleDesc desc)
{
	UT_ASSERT(false);
	return 0;
}
Datum
heap_getsysattr(HeapTuple tuple, int attnum, TupleDesc desc, bool *isnull)
{
	UT_ASSERT(false);
	return 0;
}
Datum
getmissingattr(TupleDesc desc, int attnum, bool *isnull)
{
	UT_ASSERT(false);
	return 0;
}

#include "test_cluster_heap_scan_lifetime.inc"

Buffer
ReleaseAndReadBuffer(Buffer buffer, Relation relation, BlockNumber block)
{
	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);
	return ReadBuffer(relation, block);
}

bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
					   HeapTuple tuple, bool *all_dead, bool first_call)
{
	/* The bitmap witness uses the actual lossy branch; HOT has its own suite. */
	UT_ASSERT(false);
	return false;
}

static bool SampleHeapTupleVisible(TableScanDesc scan, Buffer buffer, HeapTuple tuple,
								   OffsetNumber offset);
#include "test_cluster_heap_scan_consumers.inc"

static OffsetNumber
sample_second_tuple(SampleScanState *state, BlockNumber block, OffsetNumber maxoffset)
{
	return 2;
}

static void
check_scan_consumer_lifetime(int method)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	SnapshotData snapshot = { .snapshot_type = SNAPSHOT_MVCC };
	HeapScanDescData scan = { 0 };
	TupleTableSlot slot = { 0 };
	TBMIterateResult bitmap = { .blockno = 41, .ntuples = -1 };
	TsmRoutine sampler = { .NextSampleTuple = sample_second_tuple };
	SampleScanState sample = { .tsmroutine = &sampler };

	leg = 7;
	pins = fetches = stored = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = scan_lifetime_case = true;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	scan.rs_base.rs_rd = &relation;
	scan.rs_base.rs_snapshot = &snapshot;
	scan.rs_base.rs_flags = method == 2 ? 0 : SO_ALLOW_PAGEMODE;
	scan.rs_nblocks = 42;
	if (method == 0) {
		UT_ASSERT(heapam_scan_bitmap_next_block((TableScanDesc)&scan, &bitmap));
		UT_ASSERT(heapam_scan_bitmap_next_tuple((TableScanDesc)&scan, &bitmap, &slot));
	} else {
		heapgetpage((TableScanDesc)&scan, 41);
		UT_ASSERT(heapam_scan_sample_next_tuple((TableScanDesc)&scan, &sample, &slot));
	}
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(stored, 1);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 900);
	memcpy(fetch_pages[6].data, replacement_page.data, BLCKSZ);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 900);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&stored_tuple.t_data->t_ctid), 2);
	ReleaseBuffer(scan.rs_cbuf);
	cluster_enabled = scan_lifetime_case = false;
}

UT_TEST(test_bitmap_slot_survives_image_relocation)
{
	check_scan_consumer_lifetime(0);
}
UT_TEST(test_pagemode_sample_slot_survives_image_relocation)
{
	check_scan_consumer_lifetime(1);
}
UT_TEST(test_row_sample_slot_survives_image_relocation)
{
	check_scan_consumer_lifetime(2);
}

static void
check_scan_advance_lifetime(bool pagemode, bool tidrange)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	SnapshotData snapshot = { .snapshot_type = SNAPSHOT_MVCC };
	HeapScanDescData scan = { 0 };
	TupleTableSlot slot = { .tts_ops = &TTSOpsBufferHeapTuple };
	bool found;

	leg = 7;
	pins = fetches = stored = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = scan_lifetime_case = true;
	scan_additional_page = 0;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	scan.rs_base.rs_rd = &relation;
	scan.rs_base.rs_snapshot = &snapshot;
	scan.rs_base.rs_flags = pagemode ? SO_ALLOW_PAGEMODE : 0;
	scan.rs_ctup.t_tableOid = 123;
	initscan(&scan, NULL, false);
	ItemPointerSet(&scan.rs_base.rs_mintid, 40, 1);
	ItemPointerSet(&scan.rs_base.rs_maxtid, 41, 2);
	found = tidrange ? heap_getnextslot_tidrange((TableScanDesc)&scan, BackwardScanDirection, &slot)
					 : heap_getnextslot((TableScanDesc)&scan, BackwardScanDirection, &slot);
	UT_ASSERT(found);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 900);
	/* A slot outlives both the shared image and the scan-owned image. */
	memcpy(fetch_pages[6].data, replacement_page.data, BLCKSZ);
	memset(scan.rs_owned_page, 0, BLCKSZ);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 900);
	UT_ASSERT(ItemPointerIsValid(&slot.tts_tid));
	if (ItemPointerIsValid(&slot.tts_tid))
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot.tts_tid), 41);
	UT_ASSERT_EQ(slot.tts_tableOid, 123);
	/* Restart uses the real reset producer, not a hand-cleared descriptor. */
	heap_rescan((TableScanDesc)&scan, NULL, false, false, false, pagemode);
	UT_ASSERT_EQ(scan.rs_owned_kind, 0);
	UT_ASSERT(scan.rs_ctup.t_data == NULL && !scan.rs_inited);
	UT_ASSERT_EQ(pins, 0);
	scan_additional_page = 1;
	UT_ASSERT(heap_getnextslot((TableScanDesc)&scan, BackwardScanDirection, &slot));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 900);
	UT_ASSERT(heap_getnextslot((TableScanDesc)&scan, BackwardScanDirection, &slot));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(stored_tuple.t_data), 903);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot.tts_tid), 40);
	UT_ASSERT(!heap_getnextslot((TableScanDesc)&scan, BackwardScanDirection, &slot));
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT_EQ(scan.rs_owned_kind, 0);
	cluster_enabled = scan_lifetime_case = false;
}

UT_TEST(test_pagemode_scan_slot_rescan_and_backward_page_advance)
{
	check_scan_advance_lifetime(true, false);
}
UT_TEST(test_rowmode_scan_slot_rescan_and_backward_page_advance)
{
	check_scan_advance_lifetime(false, false);
}
UT_TEST(test_tidrange_scan_slot_rescan_and_backward_page_advance)
{
	check_scan_advance_lifetime(true, true);
}

#include "test_cluster_heap_snapshot_consumer.inc"

static void
check_materialized_snapshot(int change)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	SnapshotData snapshot = { .snapshot_type = SNAPSHOT_MVCC };
	BufferHeapTupleTableSlot slot = { .base.base.tts_ops = &TTSOpsBufferHeapTuple };
	PGAlignedBlock owned;
	HeapTupleData tuple;
	ItemId lp;
	Buffer buffer;
	volatile bool caught = false, visible = false;

	leg = 7;
	pins = fetches = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = true;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	buffer = ReadBuffer(&relation, 41);
	lp = PageGetItemId(BufferGetPage(buffer), 2);
	tuple.t_len = ItemIdGetLength(lp);
	tuple.t_data = (HeapTupleHeader)owned.data;
	tuple.t_tableOid = 123;
	ItemPointerSet(&tuple.t_self, 41, 2);
	memcpy(owned.data, PageGetItem(BufferGetPage(buffer), lp), tuple.t_len);
	ReleaseBuffer(buffer);
	slot.base.tuple = &tuple;
	slot.base.base.tts_tid = tuple.t_self;
	slot.base.base.tts_tableOid = 123;
	slot.base.base.tts_flags = TTS_FLAG_SHOULDFREE;
	slot.buffer = InvalidBuffer;
	snapshot_recheck_case = true;
	snapshot_current_change = change;
	snapshot_visibility_calls = 0;
	PG_TRY();
	{
		visible = heapam_tuple_satisfies_snapshot(&relation, &slot.base.base, &snapshot);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, change != 0);
	UT_ASSERT_EQ(visible, change == 0);
	UT_ASSERT_EQ(snapshot_visibility_calls, change == 0 ? 1 : 0);
	/* ERROR cleanup is the external native resource-owner boundary. */
	if (caught && content_locked)
		LockBuffer(7, BUFFER_LOCK_UNLOCK);
	if (caught && pins)
		ReleaseBuffer(7);
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(!content_locked);
	snapshot_recheck_case = cluster_enabled = false;
}

UT_TEST(test_materialized_snapshot_exact_current_match)
{
	check_materialized_snapshot(0);
}
UT_TEST(test_materialized_snapshot_changed_bytes_refused)
{
	check_materialized_snapshot(1);
}
UT_TEST(test_materialized_snapshot_missing_tuple_refused)
{
	check_materialized_snapshot(2);
}
UT_TEST(test_materialized_snapshot_changed_length_refused)
{
	check_materialized_snapshot(3);
}

UT_TEST(test_index_snapshot_any_rebinds_and_recaptures_current_tuple)
{
	RelationData relation = { 0 };
	Relation heapRelation = &relation;
	FormData_pg_class form = { 0 };
	HeapScanDescData scan = { 0 };
	HeapScanDesc hscan = &scan;
	HeapTuple heapTuple = &hscan->rs_ctup;
	TransactionId creation_xmin = 900;
	ItemId lp;
	Buffer buffer;

	leg = 7;
	pins = fetches = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = true;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	scan.rs_base.rs_rd = &relation;
	buffer = scan.rs_cbuf = ReadBuffer(&relation, 41);
	lp = PageGetItemId(BufferGetPage(buffer), 2);
	heapTuple->t_len = ItemIdGetLength(lp);
	ItemPointerSet(&heapTuple->t_self, 41, 2);
	memcpy(scan.rs_owned_page, PageGetItem(BufferGetPage(buffer), lp), heapTuple->t_len);
	heapTuple->t_data = (HeapTupleHeader)scan.rs_owned_page;
	scan.rs_owned_kind = 2;
	/* Actual SnapshotAny entry must give vacuum visibility the live tuple,
	 * never the owned scan image paired with unrelated live ITL metadata. */
#include "test_cluster_heap_index_recheck.inc"
	UT_ASSERT(content_locked);
	UT_ASSERT(heapTuple->t_data == (HeapTupleHeader)PageGetItem(BufferGetPage(buffer), lp));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(heapTuple->t_data), 900);
	replace_on_unlock = true;
#include "test_cluster_heap_index_recapture.inc"
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(heapTuple->t_data), 900);
	replace_on_unlock = cluster_enabled = false;
	ReleaseBuffer(buffer);
	(void)creation_xmin;
	(void)heapRelation;
}

HeapTuple
ExecFetchSlotHeapTuple(TupleTableSlot *slot, bool materialize, bool *should_free)
{
	return ((BufferHeapTupleTableSlot *)slot)->base.tuple;
}

UT_TEST(test_rewrite_consumer_owns_temporary_pin_and_recaptured_bytes)
{
	RelationData relation = { 0 };
	Relation OldHeap = &relation;
	FormData_pg_class form = { 0 };
	BufferHeapTupleTableSlot owned_slot = { 0 };
	BufferHeapTupleTableSlot *hslot = &owned_slot;
	TupleTableSlot *slot = &owned_slot.base.base;
	HeapTupleData selected;
	HeapTuple tuple;
	Buffer initial, buf;
	PGAlignedBlock owned;
	/* These match the production local storage surrounding the extracted
	 * consumer; no slot pin is fabricated for the materialized input. */
	HeapTupleData rewrite_current;
	PGAlignedBlock rewrite_image;
	TransactionId creation_xmin = InvalidTransactionId;
	bool temporary_pin = false;
	volatile bool caught = false;
	ItemId lp;

	leg = 7;
	pins = fetches = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = true;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	initial = ReadBuffer(OldHeap, 41);
	lp = PageGetItemId(BufferGetPage(initial), 2);
	selected.t_len = ItemIdGetLength(lp);
	memcpy(owned.data, PageGetItem(BufferGetPage(initial), lp), selected.t_len);
	selected.t_data = (HeapTupleHeader)owned.data;
	ItemPointerSet(&selected.t_self, 41, 2);
	selected.t_tableOid = 123;
	ReleaseBuffer(initial);
	owned_slot.base.tuple = &selected;
	owned_slot.buffer = InvalidBuffer;
	rewrite_recheck_case = true;
	PG_TRY();
	{
#include "test_cluster_heap_copy_entry.inc"
		UT_ASSERT(content_locked);
		UT_ASSERT_EQ(pins, 1);
		UT_ASSERT(tuple->t_data
				  == (HeapTupleHeader)PageGetItem(BufferGetPage(buf),
												  PageGetItemId(BufferGetPage(buf), 2)));
		/* Vacuum's hint side effect must target live bytes, then be present
		 * in the separately recaptured row passed to the existing rewriter. */
		tuple->t_data->t_infomask |= HEAP_XMIN_COMMITTED;
		replace_on_unlock = true;
#include "test_cluster_heap_copy_exit.inc"
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple->t_data), 900);
		UT_ASSERT(tuple->t_data->t_infomask & HEAP_XMIN_COMMITTED);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT_EQ(owned_slot.buffer, InvalidBuffer);
	UT_ASSERT(owned_slot.base.tuple == &selected);
	rewrite_recheck_case = replace_on_unlock = cluster_enabled = false;
	(void)rewrite_current;
	(void)rewrite_image;
	(void)creation_xmin;
	(void)temporary_pin;
}

UT_TEST(test_owned_scan_rejects_oversize_before_copy)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	struct {
		HeapScanDescData scan;
		char trailing[BLCKSZ];
	} target;
	char source[BLCKSZ + 8];
	volatile bool caught = false;

	memset(&target, 0x33, sizeof(target));
	memset(source, 0x5a, sizeof(source));
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	target.scan.rs_base.rs_rd = &relation;
	target.scan.rs_ctup.t_data = (HeapTupleHeader)source;
	target.scan.rs_ctup.t_len = BLCKSZ + 1;
	target.scan.rs_owned_kind = 0;
	cluster_enabled = true;
	PG_TRY();
	{
		cluster_heap_scan_capture_tuple(&target.scan);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(target.scan.rs_owned_page[0], 0x33);
	UT_ASSERT_EQ(target.scan.rs_owned_kind, 0);
	UT_ASSERT(target.scan.rs_ctup.t_data == (HeapTupleHeader)source);
	cluster_enabled = false;
}

UT_TEST(test_scan_result_survives_current_page_relocation)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	SnapshotData snapshot = { .snapshot_type = SNAPSHOT_MVCC };
	HeapScanDescData scan = { 0 };

	leg = 7;
	pins = fetches = 0;
	content_locked = replace_on_lock = replace_on_unlock = false;
	cluster_enabled = true;
	scan_lifetime_case = true;
	relation.rd_rel = &form;
	relation.rd_id = 123;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	scan.rs_base.rs_rd = &relation;
	scan.rs_base.rs_snapshot = &snapshot;
	scan.rs_base.rs_flags = SO_ALLOW_PAGEMODE;
	scan.rs_nblocks = 42;
	scan.rs_cbuf = InvalidBuffer;
	heapgetpage((TableScanDesc)&scan, 41);
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(scan.rs_ntuples, 1);
	scan.rs_inited = true;
	scan.rs_cindex = -1;
	heapgettup_pagemode(&scan, ForwardScanDirection, 0, NULL);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(scan.rs_ctup.t_data), 900);
	/* Actual producer returned; external whole-page delivery preserves the
	 * logical row but relocates it before this consumer examines the result. */
	memcpy(fetch_pages[6].data, replacement_page.data, BLCKSZ);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(scan.rs_ctup.t_data), 900);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan.rs_ctup.t_data->t_ctid), 2);
	ReleaseBuffer(scan.rs_cbuf);
	cluster_enabled = scan_lifetime_case = false;
}

int
errdetail(const char *fmt, ...)
{
	if (finishing_index_log && strstr(fmt, "PGRAC_FAMILY=R4_SELECTION") != NULL) {
		va_list args;

		va_start(args, fmt);
		vsnprintf(index_log, sizeof(index_log), fmt, args);
		va_end(args);
		index_log_count++;
	}
	return 0;
}

ItemPointer
index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	if (index_tids_left-- <= 0)
		return NULL;
	ItemPointerSet(&scan->xs_heaptid, 17, index_tids_left + 1);
	return &scan->xs_heaptid;
}

bool
index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
{
	index_fetch_count++;
	return index_fetch_count == index_found_on;
}

#include "test_cluster_index_exhaustion.inc"

UT_TEST(test_index_exhaustion_evidence_distinguishes_no_tid_from_invisible_heap)
{
	int scenario;

	for (scenario = 0; scenario < 5; scenario++) {
		IndexScanDescData scan = { 0 };
		RelationData heap = { 0 }, index = { 0 };
		FormData_pg_class form = { 0 };
		SnapshotData snapshot = { 0 };
		TupleTableSlot slot = { 0 };
		bool found;

		heap.rd_id = 9001;
		heap.rd_rel = &form;
		form.relpersistence = RELPERSISTENCE_PERMANENT;
		index.rd_id = 9002;
		scan.heapRelation = &heap;
		scan.indexRelation = &index;
		scan.xs_snapshot = &snapshot;
		snapshot.snapshot_type = scenario == 4 ? SNAPSHOT_DIRTY : SNAPSHOT_MVCC;
		snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		snapshot.read_scn = 1234;
		snapshot.read_epoch = 9;
		cluster_enabled = scenario != 3;
		index_tids_left = scenario == 0 ? 0 : 2;
		index_found_on = scenario == 2 ? 2 : -1;
		index_fetch_count = index_log_count = 0;
		index_log[0] = '\0';
		capture_index_log = true;
		found = index_getnext_slot(&scan, ForwardScanDirection, &slot);
		capture_index_log = false;
		UT_ASSERT_EQ(found, scenario == 2);
		UT_ASSERT_EQ(index_fetch_count, scenario == 0 ? 0 : 2);
		UT_ASSERT_EQ(index_log_count, scenario < 2 ? 1 : 0);
		if (scenario < 2) {
			UT_ASSERT(strstr(index_log, "PGRAC_REASON=INDEX_EXHAUSTED") != NULL);
			UT_ASSERT(strstr(index_log, "read_scn=1234") != NULL);
			UT_ASSERT(
				strstr(index_log, scenario == 0 ? "fetches_this_call=0" : "fetches_this_call=2")
				!= NULL);
		}
	}
	cluster_enabled = false;
}

int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_index_exhaustion_evidence_distinguishes_no_tid_from_invisible_heap);
	UT_RUN(test_owned_scan_rejects_oversize_before_copy);
	UT_RUN(test_rewrite_consumer_owns_temporary_pin_and_recaptured_bytes);
	UT_RUN(test_compatible_self_lock_keeps_owned_output_after_unlock);
	UT_RUN(test_pagemode_scan_slot_rescan_and_backward_page_advance);
	UT_RUN(test_rowmode_scan_slot_rescan_and_backward_page_advance);
	UT_RUN(test_tidrange_scan_slot_rescan_and_backward_page_advance);
	UT_RUN(test_materialized_snapshot_exact_current_match);
	UT_RUN(test_materialized_snapshot_changed_bytes_refused);
	UT_RUN(test_materialized_snapshot_missing_tuple_refused);
	UT_RUN(test_materialized_snapshot_changed_length_refused);
	UT_RUN(test_index_snapshot_any_rebinds_and_recaptures_current_tuple);
	UT_RUN(test_bitmap_slot_survives_image_relocation);
	UT_RUN(test_pagemode_sample_slot_survives_image_relocation);
	UT_RUN(test_row_sample_slot_survives_image_relocation);
	UT_RUN(test_scan_result_survives_current_page_relocation);
	UT_RUN(test_lock_rebind_rejects_missing_or_changed_identity_without_mutation);
	UT_RUN(test_lock_reacquisition_resolves_same_tid_after_relocation);
	UT_RUN(test_lock_output_without_replacement);
	UT_RUN(test_lock_output_survives_unlock_relocation);
	UT_RUN(test_epq_no_replacement_control);
	UT_RUN(test_epq_relocated_live_successor_is_not_deleted);
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
