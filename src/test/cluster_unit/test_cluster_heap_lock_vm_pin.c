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
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_tt_local.h"
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
static bool vm_locked, needs_clear, warm_error, warm_replaces_pin;
static int warm_change;
static unsigned nested_refusals, nested_replaces, warm_refusals;
static unsigned nested_calls, warm_calls, plain_vm_waits, chain_fallthroughs;
static unsigned context_starts;
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

int
main(void)
{
	UT_PLAN(33);
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
	UT_DONE();
	return ut_failed_count != 0;
}
