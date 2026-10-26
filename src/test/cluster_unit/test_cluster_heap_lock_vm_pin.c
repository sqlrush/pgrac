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
#include "access/xact.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_tt_local.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
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
static bool vm_locked, needs_clear, warm_error, warm_replaces_pin;
static int warm_change;
static unsigned nested_refusals, nested_replaces, warm_refusals;
static unsigned nested_calls, warm_calls, plain_vm_waits, chain_fallthroughs;
static unsigned context_starts;
static unsigned dml_waits;
static unsigned transaction_wait_pins, tuple_wait_pins, tuple_wait_calls, member_frees;
static int transaction_wait_error;
static MultiXactMember wait_member;
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
int
errdetail(const char *fmt, ...)
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
	if (buffer == 2) {
		UT_ASSERT(vm_pinned);
		if (mode == BUFFER_LOCK_UNLOCK) {
			UT_ASSERT(vm_locked);
			vm_locked = false;
		} else {
			UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
			UT_ASSERT(!vm_locked);
			plain_vm_waits += locked;
			vm_locked = true;
		}
		return;
	}
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

/* Only the acquisition leaf is controlled: false owns no VM content lock,
 * and replacement relinquishes the sole pin and invalidates the handle. */
bool
ClusterLockBufferExclusiveAuxiliaryAliasAware(Buffer *buffer, Buffer *alias,
											  const ClusterBufferBarrierSiteId *site,
											  ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT(*buffer == 2 && vm_pinned && !vm_locked);
	UT_ASSERT(alias == NULL && site == NULL && context != NULL);
	if (!context->active) {
		context->active = true;
		context->absolute_deadline_us = 4242;
		context_starts++;
	} else
		UT_ASSERT_EQ(context->absolute_deadline_us, 4242);
	if (locked) {
		nested_calls++;
		if (nested_refusals > 0) {
			nested_refusals--;
			if (nested_replaces > 0) {
				nested_replaces--;
				ReleaseBuffer(*buffer);
				*buffer = InvalidBuffer;
			}
			return false;
		}
	} else {
		warm_calls++;
		if (warm_error)
			elog(ERROR, "controlled auxiliary acquisition error");
		if (warm_change == 1)
			needs_clear = false;
		else if (warm_change == 2)
			PageClearAllVisible(pages[0].data);
		else if (warm_change == 3)
			HeapTupleHeaderSetXmin(
				(HeapTupleHeader)PageGetItem(pages[0].data, PageGetItemId(pages[0].data, 2)), 701);
		else if (warm_change == 4)
			ItemIdSetUnused(PageGetItemId(pages[0].data, 2));
		if (warm_refusals > 0) {
			warm_refusals--;
			if (warm_replaces_pin) {
				ReleaseBuffer(*buffer);
				*buffer = InvalidBuffer;
			}
			return false;
		}
	}
	vm_locked = true;
	memset(context, 0, sizeof(*context));
	return true;
}

/* Coherent AF classification is tested separately.  This controlled leaf
 * permits changes during the original unlocked VM-acquisition boundary. */
static bool
cluster_heap_lock_vm_needs_clear(Relation rel, BlockNumber block, Buffer map)
{
	UT_ASSERT(rel == &relation && block == 17 && map == 2);
	UT_ASSERT(locked && vm_pinned && !vm_locked);
	return needs_clear;
}

#include "test_cluster_heap_lock_vm_recent.inc"
#include "test_cluster_heap_vm_unwind_helpers.inc"
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
	vm_locked = warm_error = warm_replaces_pin = false;
	needs_clear = true;
	warm_change = 0;
	nested_refusals = nested_replaces = warm_refusals = 0;
	nested_calls = warm_calls = plain_vm_waits = chain_fallthroughs = context_starts = 0;
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

Buffer
ReadBuffer(Relation rel, BlockNumber block)
{
	UT_ASSERT(rel == &relation && block == 17 && !locked);
	return 1;
}

static void
MultiXactIdWait(MultiXactId multi, MultiXactStatus status, uint16 infomask, Relation rel,
				ItemPointer tid, XLTW_Oper oper, int *remaining)
{
	UT_ASSERT(!locked && rel == &relation);
	UT_ASSERT_EQ(multi, 345);
	UT_ASSERT_EQ(status, MultiXactStatusUpdate);
	UT_ASSERT_EQ(infomask, HEAP_XMAX_IS_MULTI);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), ItemPointerGetBlockNumber(&tuple.t_self));
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), ItemPointerGetOffsetNumber(&tuple.t_self));
	UT_ASSERT(oper == XLTW_Delete || oper == XLTW_Update);
	transaction_wait_pins += vm_pinned;
	if (transaction_wait_error == 2)
		elog(ERROR, "controlled native transaction wait error");
	if (remaining != NULL)
		*remaining = 1;
	dml_waits++;
}

void
XactLockTableWait(TransactionId xid, Relation rel, ItemPointer tid, XLTW_Oper oper)
{
	UT_ASSERT(!locked && rel == &relation);
	UT_ASSERT_EQ(xid, 345);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), ItemPointerGetBlockNumber(&tuple.t_self));
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), ItemPointerGetOffsetNumber(&tuple.t_self));
	UT_ASSERT(oper == XLTW_Delete || oper == XLTW_Update || oper == XLTW_LockUpdated);
	transaction_wait_pins += vm_pinned;
	if (transaction_wait_error == 2)
		elog(ERROR, "controlled native transaction wait error");
	dml_waits++;
}

static bool
heap_acquire_tuplock(Relation rel, ItemPointer tid, LockTupleMode mode, LockWaitPolicy policy,
					 bool *have_tuple_lock)
{
	UT_ASSERT(!locked && rel == &relation);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), 17);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), 2);
	UT_ASSERT_EQ(mode, LockTupleExclusive);
	UT_ASSERT_EQ(policy, LockWaitBlock);
	tuple_wait_pins += vm_pinned;
	tuple_wait_calls++;
	if (transaction_wait_error == 1)
		elog(ERROR, "controlled tuple priority wait error");
	*have_tuple_lock = true;
	return true;
}

static void
fixture_wait_free(void *pointer)
{
	UT_ASSERT(pointer == &wait_member);
	member_frees++;
}

/* Actual unlock -> first priority wait -> transaction wait -> original
 * reacquisition. Chain branches continue through the original unlocked l4. */
static void
native_wait(unsigned entry, bool current_is_member, bool already_locked)
{
	Relation relation_ptr = &relation;
	Relation relation = relation_ptr, rel = relation_ptr;
	Buffer buffer = 1, buf = 1, vmbuffer = vm;
	BlockNumber block = 17;
	HeapTupleData tp = tuple, oldtup = tuple, mytup = tuple;
	TransactionId creation_xmin = creator, xwait = 345, rawxmax = 345;
	uint16 infomask = HEAP_XMAX_IS_MULTI;
	LockTupleMode mode = LockTupleExclusive, *lockmode = &mode;
	MultiXactStatus mxact_status = MultiXactStatusUpdate;
	MultiXactMember *members = &wait_member;
	int i = 0, remain = 0;
	bool have_tuple_lock = already_locked;
	bool checked_lockers = false, locker_remains = false, pinned_desired_page;

	locked = true;
	wait_member.xid = 345;
	if (false)
		goto l4;
	switch (entry) {
#define pfree fixture_wait_free
#include "test_cluster_heap_native_wait_vm.inc"
#undef pfree
	default:
		abort();
	}
	goto done;
l4:
#include "test_cluster_heap_lock_chain_pin.inc"
done:
	tuple = entry < 2 ? tp : (entry < 4 ? oldtup : mytup);
	creator = creation_xmin;
	vm = vmbuffer;
	UT_ASSERT_EQ(dml_waits, 1);
	UT_ASSERT_EQ(tuple_wait_calls, entry < 4 && (!(entry % 2 == 0) || !current_is_member));
	UT_ASSERT_EQ(member_frees, entry == 4);
	UT_ASSERT_EQ(checked_lockers, entry == 2 || entry == 3);
	UT_ASSERT_EQ(locker_remains, entry == 2);
	if (entry < 4 && (entry % 2 != 0 || !current_is_member))
		UT_ASSERT(have_tuple_lock);
}

static void
reset_native_wait(bool cached, bool visible)
{
	reset(cached, visible);
	dml_waits = transaction_wait_pins = tuple_wait_pins = tuple_wait_calls = member_frees = 0;
	transaction_wait_error = 0;
}

UT_TEST(native_waits_release_vm_before_priority_and_transaction_waits)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_native_wait(true, true);
		native_wait(entry, false, false);
		check_current();
		UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins, 0);
		UT_ASSERT(vm_pinned && vm == 2);
		UT_ASSERT_EQ(waits, 1);
	}
}

UT_TEST(native_waits_preserve_member_skip_and_existing_priority)
{
	for (unsigned entry = 0; entry < 4; entry++) {
		reset_native_wait(true, true);
		native_wait(entry, true, true);
		check_current();
		UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins, 0);
		UT_ASSERT(vm_pinned && vm == 2);
	}
}

UT_TEST(native_waits_recheck_all_visible_after_current_install)
{
	for (unsigned entry = 0; entry < 6; entry++)
		for (int visible = 0; visible < 2; visible++) {
			reset_native_wait(true, true);
			if (!visible)
				PageClearAllVisible(incoming.data);
			native_wait(entry, false, false);
			check_current();
			UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins, 0);
			UT_ASSERT_EQ(vm_pinned, visible != 0);
		}
}

UT_TEST(native_waits_requalify_after_repeated_resident_misses)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_native_wait(true, true);
		misses = 3;
		native_wait(entry, false, false);
		check_current();
		UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins, 0);
		UT_ASSERT_EQ(waits, 4);
		UT_ASSERT(vm_pinned && vm == 2);
	}
}

UT_TEST(native_waits_preserve_creator_refusal_after_rebind)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset_native_wait(true, true);
		HeapTupleHeaderSetXmin(
			(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			native_wait(entry, false, false);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && locked);
		UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins + waits_with_vm, 0);
		UT_ASSERT_EQ(creator, 700);
	}
}

UT_TEST(native_wait_errors_retain_neither_heap_content_nor_vm_pin)
{
	for (unsigned entry = 0; entry < 6; entry++)
		for (int stage = entry < 4 ? 1 : 2; stage <= 2; stage++) {
			sigjmp_buf jump;
			volatile bool caught = false;

			reset_native_wait(true, true);
			transaction_wait_error = stage;
			PG_exception_stack = &jump;
			if (sigsetjmp(jump, 0) == 0)
				native_wait(entry, false, false);
			else
				caught = true;
			PG_exception_stack = NULL;
			UT_ASSERT(caught && !locked && !vm_pinned);
			UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins + waits, 0);
		}
}

UT_TEST(native_waits_accept_an_already_absent_vm_pin)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_native_wait(false, true);
		native_wait(entry, false, false);
		check_current();
		UT_ASSERT_EQ(tuple_wait_pins + transaction_wait_pins, 0);
		UT_ASSERT(vm_pinned && vm == 2);
	}
}

UT_TEST(native_waits_keep_native_and_local_pin_lifetimes)
{
	for (int local = 0; local < 2; local++)
		for (unsigned entry = 0; entry < 6; entry++) {
			reset_native_wait(true, true);
			if (local)
				form.relpersistence = RELPERSISTENCE_TEMP;
			else
				cluster_enabled = false;
			native_wait(entry, false, false);
			UT_ASSERT(locked && vm_pinned && vm == 2);
			UT_ASSERT_EQ(transaction_wait_pins, 1);
			UT_ASSERT_EQ(tuple_wait_pins, entry < 4);
			UT_ASSERT_EQ(releases + repins, 0);
		}
}

/* Run the six original acquisition brackets, stopping before the original
 * visibility classifier. Native wait/transport services are controlled leaves. */
static void
acquire_dml(unsigned entry)
{
	Relation rel = &relation;
	Relation relation = rel;
	Buffer buffer = 1, vmbuffer = vm;
	BlockNumber block = 17;
	Page page = pages[0].data;
	ItemPointer tid = &tuple.t_self;
	ItemId lp;
	HeapTupleData tp = tuple, oldtup = tuple;
	TransactionId creation_xmin = creator, xwait = 345;
	MultiXactStatus mxact_status = MultiXactStatusUpdate;
	uint16 infomask = HEAP_XMAX_IS_MULTI;
	int remain = 0;
	bool checked_lockers = false, locker_remains = false;

	dml_waits = 0;
	if (entry == 1)
		locked = true;
	switch (entry) {
#include "test_cluster_heap_dml_vm_relock.inc"
	default:
		abort();
	}
	if (entry == 0)
		cluster_heap_rebind_locked_tuple(relation, buffer, &tp, &creation_xmin);
	if (entry >= 4)
		tp = oldtup;
	tuple = tp;
	creator = creation_xmin;
	vm = vmbuffer;
	UT_ASSERT_EQ(dml_waits, entry >= 2);
	UT_ASSERT_EQ(checked_lockers, entry >= 4);
	UT_ASSERT_EQ(locker_remains, entry == 4);
}

static void
reset_dml(unsigned entry, bool cached, bool visible)
{
	/* The AV-race bracket starts with heap X and no VM pin. */
	reset(entry == 1 ? false : cached, entry == 1 ? true : visible);
}

UT_TEST(dml_cached_vm_pin_is_released_at_every_heap_current_entry)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_dml(entry, true, true);
		acquire_dml(entry);
		check_current();
		UT_ASSERT(vm_pinned && vm == 2);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(repins, 1);
	}
}

UT_TEST(dml_cold_vm_preparation_precedes_heap_current)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_dml(entry, false, true);
		acquire_dml(entry);
		check_current();
		UT_ASSERT(vm_pinned && vm == 2);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(waits, 1);
	}
}

UT_TEST(dml_new_all_visible_current_is_repinned_before_original_rebind)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		if (entry == 1)
			continue;
		reset_dml(entry, false, false);
		PageSetAllVisible(incoming.data);
		acquire_dml(entry);
		check_current();
		UT_ASSERT(vm_pinned && vm == 2);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(repins, 1);
	}
}

UT_TEST(dml_resident_misses_repeat_only_unlocked_preparation)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_dml(entry, true, true);
		misses = 3;
		acquire_dml(entry);
		check_current();
		UT_ASSERT(vm_pinned && vm == 2);
		UT_ASSERT_EQ(waits, 4);
		UT_ASSERT_EQ(repins, 4);
		UT_ASSERT_EQ(releases, 4);
	}
}

UT_TEST(dml_nonvisible_current_releases_the_old_vm_pin)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		reset_dml(entry, true, true);
		PageClearAllVisible(incoming.data);
		acquire_dml(entry);
		check_current();
		UT_ASSERT(!vm_pinned && vm == InvalidBuffer);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(repins, 0);
	}
}

UT_TEST(dml_wait_rebind_preserves_creator_contradiction_refusal)
{
	for (unsigned entry = 1; entry < 6; entry++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset_dml(entry, true, true);
		HeapTupleHeaderSetXmin(
			(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			acquire_dml(entry);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && locked);
		UT_ASSERT_EQ(waits_with_vm, 0);
		UT_ASSERT_EQ(creator, 700);
		UT_ASSERT(memcmp(pages[0].data, incoming.data, BLCKSZ) == 0);
	}
}

UT_TEST(dml_current_wait_error_retains_no_obstructing_vm_pin)
{
	for (unsigned entry = 0; entry < 6; entry++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset_dml(entry, true, true);
		wait_error = true;
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			acquire_dml(entry);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && !locked && !vm_pinned);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(waits_with_vm, 0);
	}
}

UT_TEST(dml_native_and_local_entries_keep_the_original_pin_behavior)
{
	for (int local = 0; local < 2; local++)
		for (unsigned entry = 0; entry < 6; entry++) {
			reset_dml(entry, true, true);
			if (local)
				form.relpersistence = RELPERSISTENCE_TEMP;
			else
				cluster_enabled = false;
			acquire_dml(entry);
			UT_ASSERT(locked && vm_pinned && vm == 2);
			UT_ASSERT_EQ(waits, 1);
			UT_ASSERT_EQ(waits_with_vm, 1);
			UT_ASSERT_EQ(releases + repins, 0);
		}
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

/* Run the actual original unlocked l4 acquisition and either complete VM-X
 * fragment.  Fallthrough represents the next capture/APPLY boundary, not a
 * replacement implementation of receipt planning, classification or WAL. */
static void
chain_vm(bool itl)
{
	Relation rel = &relation;
	Buffer buf = 1, vmbuffer = vm;
	BlockNumber block = 17;
	HeapTupleData mytup = tuple;
	TransactionId creation_xmin = creator;
	bool pinned_desired_page;
	bool cluster_chain_vm_locked = false, cluster_page_versioned = true;
	ResourceXAuxiliaryAcquireContext cluster_chain_vm_context = { 0 };

	(void)cluster_chain_vm_context; /* Also compile the unchanged parent. */
	(void)cluster_heap_vm_barrier_warm;
	if (false)
		goto l4;
l4:
#include "test_cluster_heap_lock_chain_pin.inc"
	if (itl) {
#include "test_cluster_heap_chain_vm_itl.inc"
	} else {
#include "test_cluster_heap_chain_vm_no_itl.inc"
	}
	UT_ASSERT_EQ(cluster_chain_vm_locked, vm_locked);
	vm = vmbuffer;
	tuple = mytup;
	creator = creation_xmin;
	chain_fallthroughs++;
}

UT_TEST(chain_cached_vm_current_preserves_both_locks_and_rebind)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(warm_calls, 0);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

UT_TEST(chain_vm_refusal_unlocks_heap_before_warm_and_requalification)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = 1;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(plain_vm_waits, 0);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(warm_calls, 1);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

UT_TEST(chain_repeated_vm_refusals_do_not_fall_through_or_wait_with_heap)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = 3;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(plain_vm_waits, 0);
		UT_ASSERT_EQ(waits, 4);
		UT_ASSERT_EQ(warm_calls, 3);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

UT_TEST(chain_replaced_vm_handle_is_prepared_before_heap_reentry)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = nested_replaces = 1;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(warm_calls, 0);
		UT_ASSERT_EQ(context_starts, 1);
	}
}

UT_TEST(chain_warm_replacement_preserves_original_acquisition_age)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = 2;
		warm_refusals = 1;
		warm_replaces_pin = true;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(vm_locked && vm_pinned);
		UT_ASSERT_EQ(waits, 3);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(warm_calls, 2);
		/* The first failed warm/reobserve cannot reset the context.  The
		 * successful second warm finishes it, then cached-X starts anew. */
		UT_ASSERT_EQ(context_starts, 2);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

UT_TEST(chain_rechecks_frozen_clear_decision_after_unlocked_vm_wait)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = 1;
		warm_change = 1;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(!vm_locked && vm_pinned);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(nested_calls, 1);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

UT_TEST(chain_nonvisible_current_discards_the_old_vm_pin_after_wait)
{
	for (int itl = 0; itl < 2; itl++) {
		reset(true, true);
		nested_refusals = 1;
		warm_change = 2;
		chain_vm(itl != 0);
		check_current();
		UT_ASSERT(!vm_locked && !vm_pinned && vm == InvalidBuffer);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(nested_calls, 1);
		UT_ASSERT_EQ(chain_fallthroughs, 1);
	}
}

static void
chain_error(int change, bool acquisition_error)
{
	for (int itl = 0; itl < 2; itl++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset(true, true);
		nested_refusals = 1;
		warm_change = change;
		warm_error = acquisition_error;
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			chain_vm(itl != 0);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && !vm_locked);
		UT_ASSERT_EQ(chain_fallthroughs, 0);
		UT_ASSERT_EQ(waits_with_vm, 0);
		UT_ASSERT_EQ(locked, !acquisition_error);
	}
}

UT_TEST(chain_creator_contradiction_after_vm_wait_cannot_reach_apply)
{
	chain_error(3, false);
}
UT_TEST(chain_pruned_tuple_after_vm_wait_cannot_reach_apply)
{
	chain_error(4, false);
}
UT_TEST(chain_warm_error_holds_no_content_lock_and_never_reaches_apply)
{
	chain_error(0, true);
}

static unsigned receipt_prepares, receipt_pending, receipt_requals, receipt_deadlines;
static unsigned receipt_interrupts, receipt_returns, receipt_prepare_retries;
static bool receipt_refused, receipt_error, receipt_binding_ready, receipt_identity_ready;
static bool receipt_invalidated, receipt_install_image;
static uint64 receipt_expected_deadline, receipt_returned_deadline;
static ClusterUndoRecordPrepareResult receipt_retry_result;
static ClusterUndoRecordPrepareReceipt receipt_returned;
static bool receipt_returned_owned;

static void
receipt_interrupt(void)
{
	UT_ASSERT(!locked && !vm_locked);
	receipt_interrupts++;
}

#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() receipt_interrupt()
#define cluster_heap_prepare_undo_record_call cluster_undo_record_prepare
#include "test_cluster_heap_chain_receipt_types.inc"
#undef cluster_heap_prepare_undo_record_call
#undef CHECK_FOR_INTERRUPTS

/* Services may block only after the actual retry block released content
 * ownership. Their canonical/receipt decisions are controlled leaves. */
static void
receipt_service(void)
{
	UT_ASSERT(!locked && !vm_locked);
	if (receipt_error)
		elog(ERROR, "controlled receipt service error");
	if (receipt_install_image)
		replace_image = true;
}

uint64
cluster_undo_record_prepare_deadline_us(void)
{
	receipt_service();
	receipt_deadlines++;
	return 7000;
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *binding)
{
	receipt_service();
	UT_ASSERT_EQ(xid, 501);
	memset(binding, 0, sizeof(*binding));
	binding->xid = xid;
	binding->segment_id = 17;
	binding->slot_offset = 19;
	return receipt_binding_ready;
}

static bool
cluster_heap_itl_receipt_identity_admitted(TransactionId xid, uint32 segment)
{
	receipt_service();
	UT_ASSERT_EQ(xid, 501);
	UT_ASSERT_EQ(segment, 17);
	return receipt_identity_ready;
}

ClusterUndoRecordPrepareResult
cluster_undo_record_prepare(uint8 type, uint16 capacity, uint16 segment, uint16 slot, UBA previous,
							uint64 deadline, ClusterUndoRecordPrepareReceipt *receipt)
{
	receipt_service();
	UT_ASSERT_EQ(type, UNDO_RECORD_ITL);
	UT_ASSERT_EQ(capacity, sizeof(UndoItlPayload));
	UT_ASSERT_EQ(segment, 17);
	UT_ASSERT_EQ(slot, 19);
	UT_ASSERT(UBA_is_invalid(previous));
	UT_ASSERT_EQ(deadline, receipt_expected_deadline);
	receipt_prepares++;
	if (receipt_refused)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	if (receipt_prepare_retries > 0) {
		receipt_prepare_retries--;
		return CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
	}
	receipt->reservation_sequence = 909;
	receipt->absolute_deadline_us = deadline;
	return CLUSTER_UNDO_RECORD_PREPARE_READY;
}

static int
cluster_heap_undo_receipt_errdetail(bool ctrc)
{
	return 0;
}

const char *
cluster_undo_record_receipt_last_reason(void)
{
	return "controlled receipt refusal";
}

bool
cluster_undo_record_ctrc_prepare_pending(ClusterUndoRecordPrepareReceipt *receipt, uint8 ordinal)
{
	receipt_service();
	UT_ASSERT_EQ(ordinal, 0);
	UT_ASSERT_EQ(receipt->reservation_sequence, 909);
	UT_ASSERT_EQ(receipt->absolute_deadline_us, receipt_expected_deadline);
	receipt_pending++;
	if (receipt_refused)
		return false;
	receipt->ctrc_prepared_mask = 1;
	return true;
}

ClusterUndoRecordPrepareResult
cluster_undo_record_requalify_for_retry(ClusterUndoRecordPrepareReceipt *receipt, uint16 capacity,
										bool invalidated)
{
	receipt_service();
	UT_ASSERT_EQ(capacity, sizeof(UndoItlPayload));
	UT_ASSERT_EQ(receipt->reservation_sequence, 909);
	UT_ASSERT_EQ(receipt->absolute_deadline_us, receipt_expected_deadline);
	UT_ASSERT_EQ(invalidated, receipt_invalidated);
	receipt_requals++;
	if (receipt_refused)
		return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
	if (receipt_retry_result == CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED)
		receipt->reservation_sequence = 0;
	return receipt_retry_result;
}

static void
reset_receipt(void)
{
	reset(true, true);
	receipt_prepares = receipt_pending = receipt_requals = receipt_deadlines = 0;
	receipt_interrupts = receipt_returns = receipt_prepare_retries = 0;
	receipt_refused = receipt_error = receipt_invalidated = receipt_install_image = false;
	receipt_binding_ready = receipt_identity_ready = true;
	receipt_expected_deadline = 7000;
	receipt_returned_deadline = 0;
	receipt_returned_owned = false;
	receipt_retry_result = CLUSTER_UNDO_RECORD_PREPARE_READY;
	memset(&receipt_returned, 0, sizeof(receipt_returned));
}

/* Execute all three full retry blocks, not only their final goto. The
 * driver chooses controlled pre-APPLY outcomes and stops after the next
 * original l4 acquisition; it does not replace the full receipt planner. */
static void
chain_receipt(int edge, bool content_unlocked, bool pending, bool vm_content, unsigned iterations,
			  uint64 initial_deadline)
{
	Relation rel = &relation;
	Buffer buf = 1, vmbuffer = vm;
	BlockNumber block = 17;
	HeapTupleData mytup = tuple;
	TransactionId creation_xmin = creator, canonical_xid = 501;
	bool pinned_desired_page, cluster_chain_vm_locked = false;
	bool cluster_chain_receipt_owned = edge != 0, targets_invalidated = receipt_invalidated;
	ClusterUndoRecordPrepareReceipt cluster_chain_receipt = { 0 };
	uint64 cluster_chain_prepare_deadline_us = initial_deadline;
	ClusterHeapPreparedUndoResult undo_result = CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED;
	ClusterHeapNoRetryBoundaryResult boundary_result = CLUSTER_HEAP_BOUNDARY_ZERO_APPLY_RETRY;
	unsigned visits = 0;

	cluster_chain_receipt.reservation_sequence = 909;
	cluster_chain_receipt.absolute_deadline_us = receipt_expected_deadline;
	cluster_chain_receipt.ctrc_pending_mask = pending ? 1 : 0;
l4:
#include "test_cluster_heap_lock_chain_pin.inc"
	if (visits++ < iterations) {
		if (edge == 0) {
#include "test_cluster_heap_chain_receipt_reserve.inc"
		} else if (edge == 1) {
			/* The real prepare caller reports whether it already unlocked. */
			if (content_unlocked)
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
#include "test_cluster_heap_chain_receipt_prepare.inc"
		} else {
			/* A ZERO_APPLY boundary can own the already acquired VM-X. */
			vm_locked = cluster_chain_vm_locked = vm_content;
#include "test_cluster_heap_chain_receipt_boundary.inc"
		}
		UT_ASSERT(false); /* Every selected retry must return through l4. */
		return;
	}
	UT_ASSERT(!vm_locked && !cluster_chain_vm_locked);
	vm = vmbuffer;
	tuple = mytup;
	creator = creation_xmin;
	receipt_returned = cluster_chain_receipt;
	receipt_returned_owned = cluster_chain_receipt_owned;
	receipt_returned_deadline = cluster_chain_prepare_deadline_us;
	receipt_returns++;
}

static void
check_receipt_retry(unsigned acquisitions)
{
	check_current();
	UT_ASSERT(vm_pinned && !vm_locked);
	UT_ASSERT_EQ(waits, acquisitions);
	UT_ASSERT_EQ(receipt_returns, 1);
	UT_ASSERT_EQ(receipt_returned.absolute_deadline_us, receipt_expected_deadline);
}

UT_TEST(receipt_reservation_returns_unlocked_to_the_single_l4_acquisition)
{
	reset_receipt();
	chain_receipt(0, false, false, false, 1, 0);
	check_receipt_retry(2);
	UT_ASSERT(receipt_returned_owned);
	UT_ASSERT_EQ(receipt_returned.reservation_sequence, 909);
	UT_ASSERT_EQ(receipt_returned_deadline, 7000);
	UT_ASSERT_EQ(receipt_prepares, 1);
	UT_ASSERT_EQ(receipt_deadlines, 1);
	UT_ASSERT_EQ(receipt_pending + receipt_requals, 0);
}

UT_TEST(receipt_ready_retry_preserves_reservation_with_either_unlock_owner)
{
	for (int unlocked = 0; unlocked < 2; unlocked++) {
		reset_receipt();
		chain_receipt(1, unlocked != 0, false, false, 1, 7000);
		check_receipt_retry(2);
		UT_ASSERT(receipt_returned_owned);
		UT_ASSERT_EQ(receipt_returned.reservation_sequence, 909);
		UT_ASSERT_EQ(receipt_requals, 1);
		UT_ASSERT_EQ(receipt_prepares + receipt_pending + receipt_deadlines, 0);
	}
}

UT_TEST(receipt_pending_target_prepare_does_not_reacquire_before_l4)
{
	for (int unlocked = 0; unlocked < 2; unlocked++) {
		reset_receipt();
		chain_receipt(1, unlocked != 0, true, false, 1, 7000);
		check_receipt_retry(2);
		UT_ASSERT(receipt_returned_owned);
		UT_ASSERT_EQ(receipt_returned.reservation_sequence, 909);
		UT_ASSERT_EQ(receipt_returned.ctrc_prepared_mask, 1);
		UT_ASSERT_EQ(receipt_pending, 1);
		UT_ASSERT_EQ(receipt_requals + receipt_prepares, 0);
	}
}

UT_TEST(receipt_invalidated_target_keeps_original_reprepare_decision_and_budget)
{
	reset_receipt();
	receipt_invalidated = true;
	receipt_retry_result = CLUSTER_UNDO_RECORD_PREPARE_RETRY_REQUIRED;
	chain_receipt(1, false, true, false, 1, 7000);
	check_receipt_retry(2);
	UT_ASSERT(!receipt_returned_owned);
	UT_ASSERT_EQ(receipt_returned.reservation_sequence, 0);
	UT_ASSERT_EQ(receipt_returned_deadline, 7000);
	UT_ASSERT_EQ(receipt_requals, 1);
	UT_ASSERT_EQ(receipt_pending + receipt_prepares + receipt_deadlines, 0);
}

UT_TEST(receipt_zero_apply_retry_releases_vm_and_heap_before_requalifying)
{
	for (int map = 0; map < 2; map++) {
		reset_receipt();
		chain_receipt(2, false, false, map != 0, 1, 7000);
		check_receipt_retry(2);
		UT_ASSERT(receipt_returned_owned);
		UT_ASSERT_EQ(receipt_returned.reservation_sequence, 909);
		UT_ASSERT_EQ(receipt_requals, 1);
	}
}

UT_TEST(receipt_repeated_zero_apply_retries_do_not_accumulate_heap_ownership)
{
	for (int edge = 1; edge < 3; edge++) {
		reset_receipt();
		chain_receipt(edge, false, false, true, 3, 7000);
		check_receipt_retry(4);
		UT_ASSERT_EQ(receipt_requals, 3);
		UT_ASSERT_EQ(receipt_returned.reservation_sequence, 909);
		UT_ASSERT_EQ(receipt_deadlines + receipt_prepares, 0);
	}
}

UT_TEST(receipt_prepare_retries_keep_the_original_operation_deadline)
{
	reset_receipt();
	receipt_expected_deadline = 6000;
	receipt_prepare_retries = 2;
	chain_receipt(0, false, false, false, 1, 6000);
	check_receipt_retry(2);
	UT_ASSERT_EQ(receipt_returned_deadline, 6000);
	UT_ASSERT_EQ(receipt_prepares, 3);
	UT_ASSERT_EQ(receipt_interrupts, 2);
	UT_ASSERT_EQ(receipt_deadlines, 0);
}

UT_TEST(receipt_wait_rebinds_the_installed_compacted_tuple_before_returning)
{
	for (int edge = 0; edge < 3; edge++) {
		HeapTupleHeader old_address;

		reset_receipt();
		replace_image = false;
		receipt_install_image = true;
		old_address = tuple.t_data;
		chain_receipt(edge, false, false, edge == 2, 1, 7000);
		check_receipt_retry(2);
		UT_ASSERT(tuple.t_data != old_address);
		UT_ASSERT_EQ(creator, 700);
	}
}

static void
receipt_error_path(int edge)
{
	sigjmp_buf jump;
	volatile bool caught = false;

	PG_exception_stack = &jump;
	if (sigsetjmp(jump, 0) == 0)
		chain_receipt(edge, false, false, edge == 2, 1, 7000);
	else
		caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(receipt_returns, 0);
	UT_ASSERT(!vm_locked);
	UT_ASSERT_EQ(waits_with_vm, 0);
}

UT_TEST(receipt_wait_creator_contradiction_cannot_reach_the_next_apply_boundary)
{
	for (int edge = 0; edge < 3; edge++) {
		reset_receipt();
		replace_image = false;
		receipt_install_image = true;
		HeapTupleHeaderSetXmin(
			(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
		receipt_error_path(edge);
		UT_ASSERT(locked);
		UT_ASSERT_EQ(creator, 700);
	}
}

UT_TEST(receipt_service_refusal_or_error_keeps_content_unlocked)
{
	for (int edge = 0; edge < 3; edge++)
		for (int error = 0; error < 2; error++) {
			reset_receipt();
			receipt_error = error != 0;
			receipt_refused = error == 0;
			receipt_error_path(edge);
			UT_ASSERT(!locked);
			UT_ASSERT_EQ(waits, 1);
		}
}

UT_TEST(receipt_missing_canonical_binding_or_admission_never_reacquires_heap)
{
	for (int admission = 0; admission < 2; admission++) {
		reset_receipt();
		receipt_binding_ready = admission != 0;
		receipt_identity_ready = admission == 0;
		receipt_error_path(0);
		UT_ASSERT(!locked);
		UT_ASSERT_EQ(receipt_prepares, 0);
		UT_ASSERT_EQ(waits, 1);
	}
}

#include "test_cluster_heap_chain_call_primitives.inc"

static bool recursive_peers, recursive_error, recursive_creator_changed, recursive_failed;
static unsigned recursive_calls, recursive_outer_pins;
static TM_Result recursive_result, recursive_observed;
static ItemPointerData recursive_tid;
static ClusterConf recursive_conf;
ClusterConf *ClusterConfShmem = &recursive_conf;

bool
cluster_itl_find_multixact_origin_by_xmax(Page page, MultiXactId id, uint16 *origin)
{
	UT_ASSERT(false); /* The controlled root has no ITL marker to decode. */
	return false;
}

TransactionId
GetCurrentTransactionId(void)
{
	return 123;
}

int
errhint(const char *fmt, ...)
{
	return 0;
}

/* The recursive authority/consumer is controlled. Observe the outer pin at
 * entry, after the production caller's release, independently of child pins. */
static TM_Result
recursive_consumer(Relation rel, const ItemPointerData *tid, TransactionId xid, LockTupleMode mode)
{
	UT_ASSERT(rel == &relation && !locked);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), ItemPointerGetBlockNumber(&recursive_tid));
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), ItemPointerGetOffsetNumber(&recursive_tid));
	UT_ASSERT_EQ(xid, 123);
	UT_ASSERT(mode == LockTupleKeyShare || mode == LockTupleExclusive);
	recursive_calls++;
	recursive_outer_pins += vm_pinned;
	if (recursive_error)
		elog(ERROR, "controlled recursive chain error");
	if (recursive_creator_changed) {
		HeapTupleHeaderSetXmin(
			(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
		replace_image = true;
	}
	return recursive_result;
}

static TM_Result
heap_lock_updated_tuple(Relation rel, uint16 infomask, TransactionId rawxmax,
						const ItemPointerData *tid, TransactionId xid, LockTupleMode mode)
{
	UT_ASSERT_EQ(rawxmax, 345);
	UT_ASSERT(infomask == 0 || infomask == HEAP_XMAX_IS_MULTI);
	return recursive_consumer(rel, tid, xid, mode);
}

static TM_Result
heap_lock_updated_tuple_authoritative(Relation rel, const ClusterHeapSuccessorProof *proof,
									  TransactionId xid, LockTupleMode mode)
{
	UT_ASSERT(proof->valid);
	UT_ASSERT_EQ(proof->updater_xid, 345);
	return recursive_consumer(rel, &proof->tid, xid, mode);
}

static void
reset_recursive_call(bool cached, bool peers)
{
	reset(cached, true);
	recursive_peers = peers;
	recursive_conf.node_count = peers ? 2 : 1;
	recursive_error = recursive_creator_changed = recursive_failed = false;
	recursive_calls = recursive_outer_pins = 0;
	recursive_result = recursive_observed = TM_Ok;
	ItemPointerSet(&recursive_tid, 18, 2);
	for (int current = 0; current < 2; current++) {
		Page page = current ? incoming.data : pages[0].data;
		HeapTupleHeader header = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));

		header->t_infomask = peers ? HEAP_XMAX_IS_MULTI : 0;
		HeapTupleHeaderSetXmax(header, 345);
		header->t_ctid = recursive_tid;
	}
}

static void
recursive_call(unsigned entry)
{
	Relation rel = &relation;
	Relation relation = rel;
	HeapTuple root = &tuple;
	HeapTuple tuple = root;
	Buffer heap = 1, *buffer = &heap, vmbuffer = vm;
	Page page = pages[0].data;
	TransactionId creation_xmin = creator, xwait = 345;
	uint16 infomask = recursive_peers ? HEAP_XMAX_IS_MULTI : 0;
	ItemPointerData t_ctid = recursive_tid;
	LockTupleMode mode = entry == 1 ? LockTupleKeyShare : LockTupleExclusive;
	bool follow_updates = true, updated = true;
	TM_Result result = TM_Ok, follow_result = TM_Ok;
	struct {
		ClusterHeapSuccessorProof successor_proof;
	} cluster_current_mx = { 0 };

	cluster_current_mx.successor_proof.valid = true;
	cluster_current_mx.successor_proof.updater_xid = 345;
	cluster_current_mx.successor_proof.tid = recursive_tid;
	locked = entry == 0;
	switch (entry) {
#include "test_cluster_heap_chain_call_vm.inc"
	default:
		abort();
	}
	goto done;
l3:
	UT_ASSERT(false); /* Original marker recheck must retain the same root. */
	goto done;
failed:
	recursive_failed = true;
done:
	vm = vmbuffer;
	creator = creation_xmin;
	recursive_observed = entry == 0 ? follow_result : result;
}

UT_TEST(recursive_callers_drop_outer_vm_pin_before_child_entry)
{
	for (unsigned entry = 0; entry < 3; entry++)
		for (int peers = 0; peers < 2; peers++) {
			reset_recursive_call(true, peers != 0);
			recursive_call(entry);
			check_current();
			UT_ASSERT_EQ(recursive_calls, 1);
			UT_ASSERT_EQ(recursive_outer_pins, 0);
			UT_ASSERT(vm_pinned && vm == 2);
		}
}

UT_TEST(recursive_callers_rebind_compacted_current_before_continuing)
{
	for (unsigned entry = 0; entry < 3; entry++) {
		HeapTupleHeader before;

		reset_recursive_call(true, false);
		before = tuple.t_data;
		recursive_call(entry);
		check_current();
		UT_ASSERT(tuple.t_data != before);
		UT_ASSERT_EQ(recursive_outer_pins, 0);
		UT_ASSERT_EQ(waits, 1);
	}
}

UT_TEST(recursive_callers_reobserve_resident_misses_and_nonvisible_current)
{
	for (unsigned entry = 0; entry < 3; entry++)
		for (int visible = 0; visible < 2; visible++) {
			reset_recursive_call(true, false);
			if (visible)
				misses = 3;
			else
				PageClearAllVisible(incoming.data);
			recursive_call(entry);
			check_current();
			UT_ASSERT_EQ(recursive_outer_pins, 0);
			UT_ASSERT_EQ(waits, visible ? 4 : 1);
			UT_ASSERT_EQ(vm_pinned, visible != 0);
		}
}

UT_TEST(recursive_callers_keep_original_rebind_for_non_ok_child_results)
{
	for (unsigned entry = 0; entry < 3; entry++)
		for (int blocked = 0; blocked < 2; blocked++) {
			reset_recursive_call(true, false);
			recursive_result = blocked ? TM_WouldBlock : TM_Updated;
			recursive_call(entry);
			check_current();
			UT_ASSERT_EQ(recursive_observed, recursive_result);
			UT_ASSERT_EQ(recursive_failed, entry != 0);
			UT_ASSERT_EQ(recursive_outer_pins, 0);
		}
}

UT_TEST(recursive_child_error_retains_no_outer_vm_pin_or_heap_content)
{
	for (unsigned entry = 0; entry < 3; entry++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset_recursive_call(true, false);
		recursive_error = true;
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			recursive_call(entry);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && !locked && !vm_pinned);
		UT_ASSERT_EQ(recursive_outer_pins + waits, 0);
	}
}

UT_TEST(recursive_child_wait_preserves_original_creator_refusal)
{
	for (unsigned entry = 0; entry < 3; entry++) {
		sigjmp_buf jump;
		volatile bool caught = false;

		reset_recursive_call(true, false);
		recursive_creator_changed = true;
		PG_exception_stack = &jump;
		if (sigsetjmp(jump, 0) == 0)
			recursive_call(entry);
		else
			caught = true;
		PG_exception_stack = NULL;
		UT_ASSERT(caught && locked);
		UT_ASSERT_EQ(recursive_outer_pins + waits_with_vm, 0);
		UT_ASSERT_EQ(creator, 700);
	}
}

UT_TEST(recursive_callers_preserve_absent_native_and_local_pin_controls)
{
	for (unsigned entry = 0; entry < 3; entry++)
		for (int profile = 0; profile < 3; profile++) {
			reset_recursive_call(profile != 0, false);
			if (profile == 1)
				cluster_enabled = false;
			else if (profile == 2)
				form.relpersistence = RELPERSISTENCE_TEMP;
			recursive_call(entry);
			UT_ASSERT(locked && vm_pinned && vm == 2);
			UT_ASSERT_EQ(recursive_outer_pins, profile != 0);
			if (profile == 0)
				check_current();
			else
				UT_ASSERT_EQ(releases + repins, 0);
		}
}

/* Keep the existing DML wait leaves above independent. These services observe
 * the row-lock caller while the original priority helper and branch execute. */
static unsigned row_priority_calls, row_priority_pins, row_transaction_calls, row_transaction_pins;
static unsigned row_bridge_calls;
static bool row_priority_allow, row_transaction_allow, row_multi, row_creator_change;
static bool row_failed, row_restarted, row_have_priority;
static int row_wait_error;
static TM_Result row_result;

static void
row_priority(Relation rel, ItemPointer tid, LOCKMODE mode)
{
	UT_ASSERT(rel == &relation && !locked);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), 17);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(tid), 2);
	UT_ASSERT_EQ(mode, AccessExclusiveLock);
	row_priority_calls++;
	row_priority_pins += vm_pinned;
	if (row_wait_error == 1)
		elog(ERROR, "controlled row priority service error");
}

void
LockTuple(Relation rel, ItemPointer tid, LOCKMODE mode)
{
	row_priority(rel, tid, mode);
}

bool
ConditionalLockTuple(Relation rel, ItemPointer tid, LOCKMODE mode)
{
	row_priority(rel, tid, mode);
	return row_priority_allow;
}

#define heap_acquire_tuplock row_heap_acquire_tuplock
#include "test_cluster_heap_row_wait_helpers.inc"
#undef heap_acquire_tuplock
#undef LockTupleTuplock
#undef ConditionalLockTupleTuplock

static bool
row_transaction(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 345);
	UT_ASSERT(!locked);
	row_transaction_calls++;
	row_transaction_pins += vm_pinned;
	if (row_wait_error == 2)
		elog(ERROR, "controlled row transaction service error");
	if (row_creator_change) {
		HeapTupleHeaderSetXmin(
			(HeapTupleHeader)PageGetItem(incoming.data, PageGetItemId(incoming.data, 2)), 701);
		replace_image = true;
	}
	return row_transaction_allow;
}

static void
row_xid_wait(TransactionId xid, Relation rel, ItemPointer tid, XLTW_Oper oper)
{
	UT_ASSERT(rel == &relation && tid == &tuple.t_self && oper == XLTW_Lock);
	(void)row_transaction(xid);
}

static bool
row_conditional_xid_wait(TransactionId xid)
{
	return row_transaction(xid);
}

static void
row_mx_wait(MultiXactId multi, MultiXactStatus status, uint16 infomask, Relation rel,
			ItemPointer tid, XLTW_Oper oper, int *remaining)
{
	UT_ASSERT(rel == &relation && tid == &tuple.t_self && oper == XLTW_Lock);
	UT_ASSERT_EQ(status, MultiXactStatusForUpdate);
	UT_ASSERT_EQ(infomask, HEAP_XMAX_IS_MULTI);
	UT_ASSERT(remaining == NULL);
	(void)row_transaction(multi);
}

static bool
row_conditional_mx_wait(MultiXactId multi, MultiXactStatus status, uint16 infomask, Relation rel,
						int *remaining)
{
	UT_ASSERT(rel == &relation && remaining == NULL);
	UT_ASSERT_EQ(status, MultiXactStatusForUpdate);
	UT_ASSERT_EQ(infomask, HEAP_XMAX_IS_MULTI);
	return row_transaction(multi);
}

/* Controlled LOCAL classification only; this does not exercise the bridge's
 * remote proof or wait, whose VM-pin ownership is a separate boundary. */
static bool
cluster_heap_writer_wait_failclosed(Relation rel, Buffer buffer, HeapTuple target,
									TransactionId xid, uint16 infomask, LockWaitPolicy policy,
									TM_Result *result, uint64 *deadline)
{
	UT_ASSERT(rel == &relation && target == &tuple && locked);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(xid, 345);
	UT_ASSERT_EQ(infomask, 0);
	UT_ASSERT_EQ(*deadline, 0);
	row_bridge_calls++;
	return false;
}

void
cluster_heap_stamp_released_xmax_invalid(HeapTupleHeader header, Buffer buffer)
{
	UT_ASSERT(false); /* A controlled LOCAL result must not stamp authority. */
}

static void
row_update_xmax_hint(HeapTupleHeader header, Buffer buffer, TransactionId xid)
{
	UT_ASSERT(locked && buffer == 1 && xid == 345);
	UT_ASSERT(header == tuple.t_data);
}

static void
reset_row_wait(bool cached, bool multi, bool peers)
{
	reset_recursive_call(cached, false);
	recursive_peers = peers;
	recursive_conf.node_count = peers ? 2 : 1;
	row_multi = multi;
	row_priority_calls = row_priority_pins = row_transaction_calls = row_transaction_pins = 0;
	row_bridge_calls = 0;
	row_priority_allow = row_transaction_allow = true;
	row_creator_change = row_failed = row_restarted = row_have_priority = false;
	row_wait_error = 0;
	row_result = TM_BeingModified;
	for (int current = 0; current < 2; current++) {
		Page page = current ? incoming.data : pages[0].data;
		HeapTupleHeader header = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));

		header->t_infomask = multi ? HEAP_XMAX_IS_MULTI : 0;
	}
}

/* Full original require-sleep branch, including peer rechecks, native wait
 * policies, root rebind and xmax comparison. Recursion is disabled input. */
static void
row_wait(LockWaitPolicy wait_policy, bool skip_tuple_lock, bool already_locked, TM_Result initial)
{
	Relation rel = &relation;
	Relation relation = rel;
	HeapTuple target = &tuple;
	HeapTuple tuple = target;
	Buffer heap = 1, *buffer = &heap, vmbuffer = vm;
	Page page = pages[0].data;
	ItemPointer tid = &tuple->t_self;
	ItemPointerData t_ctid = tuple->t_data->t_ctid;
	TransactionId creation_xmin = creator, xwait = 345;
	uint16 infomask = row_multi ? HEAP_XMAX_IS_MULTI : 0;
	uint64 cluster_writer_wait_deadline_us = 0;
	LockTupleMode mode = LockTupleExclusive;
	TM_Result result = initial;
	bool require_sleep = true, follow_updates = false, have_tuple_lock = already_locked;

#define heap_acquire_tuplock row_heap_acquire_tuplock
#define MultiXactIdWait row_mx_wait
#define ConditionalMultiXactIdWait row_conditional_mx_wait
#define XactLockTableWait row_xid_wait
#define ConditionalXactLockTableWait row_conditional_xid_wait
#define UpdateXmaxHintBits row_update_xmax_hint
#include "test_cluster_heap_row_wait_vm.inc"
#undef heap_acquire_tuplock
#undef MultiXactIdWait
#undef ConditionalMultiXactIdWait
#undef XactLockTableWait
#undef ConditionalXactLockTableWait
#undef UpdateXmaxHintBits
	(void)get_mxact_status_for_lock;
	goto done;
l3:
	row_restarted = true;
	goto done;
failed:
	row_failed = true;
done:
	row_result = result;
	row_have_priority = have_tuple_lock;
	creator = creation_xmin;
	vm = vmbuffer;
}

static bool
row_wait_catches_error(LockWaitPolicy policy)
{
	sigjmp_buf jump;
	volatile bool caught = false;

	PG_exception_stack = &jump;
	if (sigsetjmp(jump, 0) == 0)
		row_wait(policy, false, false, TM_BeingModified);
	else
		caught = true;
	PG_exception_stack = NULL;
	return caught;
}

UT_TEST(row_waits_drop_pins_before_priority_and_requalified_native_waits)
{
	for (int multi = 0; multi < 2; multi++)
		for (int peers = 0; peers < 2; peers++) {
			reset_row_wait(true, multi != 0, peers != 0);
			row_wait(LockWaitBlock, false, false, TM_BeingModified);
			check_current();
			UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
			UT_ASSERT_EQ(row_priority_calls, 1);
			UT_ASSERT_EQ(row_transaction_calls, 1);
			UT_ASSERT_EQ(row_bridge_calls, peers && !multi);
			UT_ASSERT(row_have_priority && !row_failed && !row_restarted && vm_pinned);
			UT_ASSERT_EQ(waits, peers ? 2 : 1);
		}
}

UT_TEST(row_waits_preserve_all_native_wait_policies)
{
	for (int policy = LockWaitBlock; policy <= LockWaitError; policy++)
		for (int multi = 0; multi < 2; multi++)
			for (int peers = 0; peers < 2; peers++) {
				reset_row_wait(true, multi != 0, peers != 0);
				row_wait((LockWaitPolicy)policy, false, false, TM_BeingModified);
				check_current();
				UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
				UT_ASSERT_EQ(row_priority_calls, 1);
				UT_ASSERT_EQ(row_transaction_calls, 1);
				UT_ASSERT(row_have_priority && !row_failed && !row_restarted);
			}
}

UT_TEST(row_waits_keep_preheld_priority_and_skip_decisions)
{
	for (int multi = 0; multi < 2; multi++)
		for (int peers = 0; peers < 2; peers++)
			for (int profile = 0; profile < 3; profile++) {
				reset_row_wait(true, multi != 0, peers != 0);
				row_wait(LockWaitBlock, profile == 1, profile == 2, TM_BeingModified);
				check_current();
				UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
				UT_ASSERT_EQ(row_priority_calls, profile == 0);
				UT_ASSERT_EQ(row_transaction_calls, 1);
				UT_ASSERT_EQ(row_have_priority, profile != 1);
			}
}

UT_TEST(row_priority_refusal_keeps_original_skip_and_error_results)
{
	for (int policy = LockWaitSkip; policy <= LockWaitError; policy++) {
		reset_row_wait(true, false, true);
		row_priority_allow = false;
		if (policy == LockWaitError)
			UT_ASSERT(row_wait_catches_error((LockWaitPolicy)policy) && !locked && !vm_pinned);
		else {
			row_wait((LockWaitPolicy)policy, false, false, TM_BeingModified);
			check_current();
			UT_ASSERT(row_failed && !row_have_priority);
			UT_ASSERT_EQ(row_result, TM_WouldBlock);
		}
		UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
		UT_ASSERT_EQ(row_transaction_calls + row_bridge_calls, 0);
	}
}

UT_TEST(row_transaction_refusal_keeps_original_skip_and_error_results)
{
	for (int policy = LockWaitSkip; policy <= LockWaitError; policy++)
		for (int multi = 0; multi < 2; multi++)
			for (int peers = 0; peers < 2; peers++) {
				reset_row_wait(true, multi != 0, peers != 0);
				row_transaction_allow = false;
				if (policy == LockWaitError)
					UT_ASSERT(row_wait_catches_error((LockWaitPolicy)policy) && !locked
							  && !vm_pinned);
				else {
					row_wait((LockWaitPolicy)policy, false, false, TM_BeingModified);
					check_current();
					UT_ASSERT(row_failed && row_have_priority);
					UT_ASSERT_EQ(row_result, TM_WouldBlock);
				}
				UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
				UT_ASSERT_EQ(row_transaction_calls, 1);
			}
}

UT_TEST(row_native_service_errors_retain_no_heap_content_or_vm_pin)
{
	for (int stage = 1; stage <= 2; stage++)
		for (int multi = 0; multi < 2; multi++)
			for (int peers = 0; peers < 2; peers++) {
				reset_row_wait(true, multi != 0, peers != 0);
				row_wait_error = stage;
				UT_ASSERT(row_wait_catches_error(LockWaitBlock));
				UT_ASSERT(!locked && !vm_pinned);
				UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
			}
}

UT_TEST(row_waits_reobserve_current_and_residency_after_peer_rechecks)
{
	for (int multi = 0; multi < 2; multi++)
		for (int peers = 0; peers < 2; peers++)
			for (int visible = 0; visible < 2; visible++) {
				reset_row_wait(true, multi != 0, peers != 0);
				if (visible)
					misses = 3;
				else
					PageClearAllVisible(incoming.data);
				row_wait(LockWaitBlock, false, false, TM_BeingModified);
				check_current();
				UT_ASSERT_EQ(row_priority_pins + row_transaction_pins, 0);
				UT_ASSERT_EQ(waits, (peers ? 2 : 1) + (visible ? 3 : 0));
				UT_ASSERT_EQ(vm_pinned, visible != 0);
			}
}

UT_TEST(row_native_wait_keeps_original_creator_refusal)
{
	for (int multi = 0; multi < 2; multi++)
		for (int peers = 0; peers < 2; peers++) {
			reset_row_wait(true, multi != 0, peers != 0);
			row_creator_change = true;
			UT_ASSERT(row_wait_catches_error(LockWaitBlock) && locked);
			UT_ASSERT_EQ(row_priority_pins + row_transaction_pins + waits_with_vm, 0);
			UT_ASSERT_EQ(row_transaction_calls, 1);
			UT_ASSERT_EQ(creator, 700);
		}
}

UT_TEST(row_waits_preserve_absent_native_and_local_pin_controls)
{
	for (int multi = 0; multi < 2; multi++)
		for (int profile = 0; profile < 3; profile++) {
			reset_row_wait(profile != 0, multi != 0, profile != 0);
			if (profile == 1)
				cluster_enabled = false;
			else if (profile == 2)
				form.relpersistence = RELPERSISTENCE_TEMP;
			if (profile != 0)
				replace_image = false;
			row_wait(LockWaitBlock, false, false, TM_BeingModified);
			UT_ASSERT(locked && vm_pinned);
			UT_ASSERT_EQ(row_priority_pins, profile != 0);
			UT_ASSERT_EQ(row_transaction_pins, profile != 0);
			if (profile == 0)
				check_current();
			else
				UT_ASSERT_EQ(releases + repins, 0);
		}
}

UT_TEST(row_committed_update_or_delete_keeps_original_immediate_failure)
{
	for (int deleted = 0; deleted < 2; deleted++) {
		TM_Result expected = deleted ? TM_Deleted : TM_Updated;

		reset_row_wait(true, false, true);
		row_wait(LockWaitBlock, false, false, expected);
		check_current();
		UT_ASSERT(row_failed && !row_have_priority);
		UT_ASSERT_EQ(row_result, expected);
		UT_ASSERT_EQ(row_priority_calls + row_transaction_calls + row_bridge_calls, 0);
	}
}

int
main(void)
{
	UT_PLAN(66);
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
	UT_RUN(chain_cached_vm_current_preserves_both_locks_and_rebind);
	UT_RUN(chain_vm_refusal_unlocks_heap_before_warm_and_requalification);
	UT_RUN(chain_repeated_vm_refusals_do_not_fall_through_or_wait_with_heap);
	UT_RUN(chain_replaced_vm_handle_is_prepared_before_heap_reentry);
	UT_RUN(chain_warm_replacement_preserves_original_acquisition_age);
	UT_RUN(chain_rechecks_frozen_clear_decision_after_unlocked_vm_wait);
	UT_RUN(chain_nonvisible_current_discards_the_old_vm_pin_after_wait);
	UT_RUN(chain_creator_contradiction_after_vm_wait_cannot_reach_apply);
	UT_RUN(chain_pruned_tuple_after_vm_wait_cannot_reach_apply);
	UT_RUN(chain_warm_error_holds_no_content_lock_and_never_reaches_apply);
	UT_RUN(receipt_reservation_returns_unlocked_to_the_single_l4_acquisition);
	UT_RUN(receipt_ready_retry_preserves_reservation_with_either_unlock_owner);
	UT_RUN(receipt_pending_target_prepare_does_not_reacquire_before_l4);
	UT_RUN(receipt_invalidated_target_keeps_original_reprepare_decision_and_budget);
	UT_RUN(receipt_zero_apply_retry_releases_vm_and_heap_before_requalifying);
	UT_RUN(receipt_repeated_zero_apply_retries_do_not_accumulate_heap_ownership);
	UT_RUN(receipt_prepare_retries_keep_the_original_operation_deadline);
	UT_RUN(receipt_wait_rebinds_the_installed_compacted_tuple_before_returning);
	UT_RUN(receipt_wait_creator_contradiction_cannot_reach_the_next_apply_boundary);
	UT_RUN(receipt_service_refusal_or_error_keeps_content_unlocked);
	UT_RUN(receipt_missing_canonical_binding_or_admission_never_reacquires_heap);
	UT_RUN(dml_cached_vm_pin_is_released_at_every_heap_current_entry);
	UT_RUN(dml_cold_vm_preparation_precedes_heap_current);
	UT_RUN(dml_new_all_visible_current_is_repinned_before_original_rebind);
	UT_RUN(dml_resident_misses_repeat_only_unlocked_preparation);
	UT_RUN(dml_nonvisible_current_releases_the_old_vm_pin);
	UT_RUN(dml_wait_rebind_preserves_creator_contradiction_refusal);
	UT_RUN(dml_current_wait_error_retains_no_obstructing_vm_pin);
	UT_RUN(dml_native_and_local_entries_keep_the_original_pin_behavior);
	UT_RUN(native_waits_release_vm_before_priority_and_transaction_waits);
	UT_RUN(native_waits_preserve_member_skip_and_existing_priority);
	UT_RUN(native_waits_recheck_all_visible_after_current_install);
	UT_RUN(native_waits_requalify_after_repeated_resident_misses);
	UT_RUN(native_waits_preserve_creator_refusal_after_rebind);
	UT_RUN(native_wait_errors_retain_neither_heap_content_nor_vm_pin);
	UT_RUN(native_waits_accept_an_already_absent_vm_pin);
	UT_RUN(native_waits_keep_native_and_local_pin_lifetimes);
	UT_RUN(recursive_callers_drop_outer_vm_pin_before_child_entry);
	UT_RUN(recursive_callers_rebind_compacted_current_before_continuing);
	UT_RUN(recursive_callers_reobserve_resident_misses_and_nonvisible_current);
	UT_RUN(recursive_callers_keep_original_rebind_for_non_ok_child_results);
	UT_RUN(recursive_child_error_retains_no_outer_vm_pin_or_heap_content);
	UT_RUN(recursive_child_wait_preserves_original_creator_refusal);
	UT_RUN(recursive_callers_preserve_absent_native_and_local_pin_controls);
	UT_RUN(row_waits_drop_pins_before_priority_and_requalified_native_waits);
	UT_RUN(row_waits_preserve_all_native_wait_policies);
	UT_RUN(row_waits_keep_preheld_priority_and_skip_decisions);
	UT_RUN(row_priority_refusal_keeps_original_skip_and_error_results);
	UT_RUN(row_transaction_refusal_keeps_original_skip_and_error_results);
	UT_RUN(row_native_service_errors_retain_no_heap_content_or_vm_pin);
	UT_RUN(row_waits_reobserve_current_and_residency_after_peer_rechecks);
	UT_RUN(row_native_wait_keeps_original_creator_refusal);
	UT_RUN(row_waits_preserve_absent_native_and_local_pin_controls);
	UT_RUN(row_committed_update_or_delete_keeps_original_immediate_failure);
	UT_DONE();
	return ut_failed_count != 0;
}
