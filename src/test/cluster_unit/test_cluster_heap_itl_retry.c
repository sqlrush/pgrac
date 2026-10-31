/*-------------------------------------------------------------------------
 * test_cluster_heap_itl_retry.c
 *    Execute UPDATE's capacity-retry owner with real receipt requalification.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The UPDATE consumer, retry helpers, VM repin and undo owner are production
 * code. Capacity wakeup, buffer I/O and shared receipt persistence are runtime
 * fixtures. The page-choice label is an exit boundary, not a full UPDATE.
 *-------------------------------------------------------------------------
 */
#define cluster_ctrc_receipt_prepare_shared heap_itl_fixture_prepare_shared
#define cluster_ctrc_receipt_apply_shared heap_itl_fixture_apply_shared
#define cluster_ctrc_receipt_retarget_itl_shared heap_itl_fixture_retarget_shared
#define cluster_ctrc_receipt_cancel_shared heap_itl_fixture_cancel_shared
int original_itl_undo_fixture_main(int argc, char **argv);
#define main original_itl_undo_fixture_main
#include "test_cluster_undo_record.c"
#undef main
#undef cluster_ctrc_receipt_prepare_shared
#undef cluster_ctrc_receipt_apply_shared
#undef cluster_ctrc_receipt_retarget_itl_shared
#undef cluster_ctrc_receipt_cancel_shared

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_tx_enqueue.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_native_itl_trace.h"

extern bool heap_receipt_test_capture(Page page, bool first, uint8 operation,
									  ClusterCtrcTargetV1 *target);
extern bool heap_receipt_test_plan_capture(ClusterUndoRecordPrepareReceipt *receipt);
extern bool heap_receipt_test_plan_recheck(ClusterUndoRecordPrepareReceipt *receipt);

static int pins[5];
static bool heap_locked[3];
static int wait_calls, key_frees, heap_relocks, recent_calls;
static int vm_pins_during_wait, vm_pins_at_relock;
static int vm_failure_site, vm_failure_sqlstate, vm_failure_hits;
typedef enum RetryFault {
	RETRY_FAULT_NONE,
	RETRY_FAULT_MODIFIER,
	RETRY_FAULT_BINDING,
	RETRY_FAULT_CANCEL,
	RETRY_FAULT_ADMISSION,
	RETRY_FAULT_TUPLE_UNUSED,
	RETRY_FAULT_TUPLE_REDIRECT,
	RETRY_FAULT_TUPLE_DEAD
} RetryFault;

static RetryFault retry_fault;
static int recent_misses;
static ClusterTxwResult wake_result;
static bool pair_case;
static uint64 expected_capacity_deadline;
static OffsetNumber fresh_offset;
static ClusterCanonicalTxnBinding *retry_binding;
static ClusterUndoRecordPrepareReceipt before_wait_receipt;

/* Observe the actual extracted consumer's arguments and ordering. The
 * recorder itself is exercised separately with its real shared publisher. */
static struct RetryTraceCut {
	uint8 phase;
	Buffer old_vm, new_vm;
	uint64 deadline;
	int waits, vm_refs;
	bool old_locked, new_locked;
	ClusterCanonicalTxnBinding binding;
	ClusterUndoRecordPrepareReceipt receipt;
} retry_trace[4];
static unsigned retry_trace_count;

static void
retry_observe(uint8 phase, const ClusterCanonicalTxnBinding *binding,
	Buffer old_vm, Buffer new_vm, uint64 deadline,
	const ClusterUndoRecordPrepareReceipt *receipt)
{
	struct RetryTraceCut *cut;

	UT_ASSERT(retry_trace_count < lengthof(retry_trace));
	cut = &retry_trace[retry_trace_count++];
	cut->phase = phase;
	cut->old_vm = old_vm;
	cut->new_vm = new_vm;
	cut->deadline = deadline;
	cut->waits = wait_calls;
	cut->vm_refs = pins[3] + pins[4];
	cut->old_locked = heap_locked[1];
	cut->new_locked = heap_locked[2];
	cut->binding = *binding;
	cut->receipt = *receipt;
}

bool cluster_update_trace_enabled;

uint64
cluster_update_trace_phase_begin_at(int bucket, uint64 now)
{
	UT_ASSERT(false); /* Profiling is disabled in this consumer fixture. */
	return 0;
}

void
cluster_update_trace_phase_end_at(uint64 token, uint64 now)
{
	UT_ASSERT(false);
}

void *
palloc(Size size)
{
	void *result = malloc(size);
	if (result == NULL)
		abort();
	return result;
}

void *
repalloc(void *pointer, Size size)
{
	void *result = realloc(pointer, size);
	if (result == NULL)
		abort();
	return result;
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
heap_freetuple(HeapTuple tuple)
{
	key_frees++;
	free(tuple);
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && buffer < lengthof(pins));
	UT_ASSERT(pins[buffer] > 0);
	pins[buffer]--;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	UT_ASSERT(pins[buffer] == 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(heap_locked[buffer]);
		heap_locked[buffer] = false;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
		UT_ASSERT(!heap_locked[buffer]);
		vm_pins_at_relock = pins[3] + pins[4];
		heap_relocks++;
		heap_locked[buffer] = true;
	}
}

void
visibilitymap_pin(Relation relation, BlockNumber block, Buffer *vm)
{
	UT_ASSERT_EQ(block, 44);
	UT_ASSERT(!heap_locked[1] && !heap_locked[2]);
	UT_ASSERT_EQ(*vm, InvalidBuffer);
	if (vm_failure_site == 1) {
		vm_failure_hits++;
		ereport(ERROR, (errcode(vm_failure_sqlstate), errmsg("scripted VM pin interruption")));
	}
	*vm = 3;
	pins[3]++;
}

bool
visibilitymap_pin_recent(Relation relation, BlockNumber block, Buffer recent, Buffer *vm)
{
	UT_ASSERT_EQ(block, 44);
	UT_ASSERT(heap_locked[1]);
	UT_ASSERT_EQ(recent, 3);
	UT_ASSERT_EQ(*vm, InvalidBuffer);
	recent_calls++;
	if (vm_failure_site == 2) {
		vm_failure_hits++;
		ereport(ERROR,
				(errcode(vm_failure_sqlstate), errmsg("scripted resident VM pin interruption")));
	}
	if (recent_misses > 0) {
		recent_misses--;
		return false;
	}
	*vm = 3;
	pins[3]++;
	return true;
}

void
cluster_vis_evidence_note(ClusterVisEvidenceMetric metric)
{
	UT_ASSERT_EQ(metric, CLUSTER_VIS_METRIC_ITL_REQUALIFY);
}

static ClusterTxwResult
cluster_heap_itl_wait_capacity_after_census(Buffer old_buffer, Buffer new_buffer,
											Buffer full_buffer, TransactionId xid, bool lock_only,
											uint64 *deadline, const char **reason)
{
	Page page = BufferGetPage(old_buffer);
	ItemId item = PageGetItemId(page, FirstOffsetNumber);
	HeapTupleHeader header;

	wait_calls++;
	UT_ASSERT_EQ(old_buffer, 1);
	UT_ASSERT_EQ(new_buffer, pair_case ? 2 : 1);
	UT_ASSERT(full_buffer == old_buffer || full_buffer == new_buffer);
	UT_ASSERT_EQ(xid, 700);
	UT_ASSERT(!lock_only);
	UT_ASSERT_EQ(*deadline, expected_capacity_deadline);
	vm_pins_during_wait = pins[3] + pins[4];
	if (new_buffer != old_buffer) {
		LockBuffer(new_buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(new_buffer);
	}
	LockBuffer(old_buffer, BUFFER_LOCK_UNLOCK);
	/* A pruning/compaction window moves the physical tuple at the same TID.
	 * The consumer must reload its address and discard the prior DML plan. */
	fresh_offset = ItemIdGetOffset(item) - 64;
	memmove((char *)page + fresh_offset, PageGetItem(page, item), ItemIdGetLength(item));
	ItemIdSetNormal(item, fresh_offset, SizeofHeapTupleHeader);
	header = (HeapTupleHeader)PageGetItem(page, item);
	header->t_infomask ^= HEAP_XMAX_INVALID;
	/* Inject only the page change while the original heap lock is absent.
	 * Requalification and the refusal/receipt owner remain production code. */
	UT_ASSERT(!heap_locked[old_buffer]);
	if (retry_fault == RETRY_FAULT_TUPLE_UNUSED)
		ItemIdSetUnused(item);
	else if (retry_fault == RETRY_FAULT_TUPLE_REDIRECT)
		ItemIdSetRedirect(item, FirstOffsetNumber + 1);
	else if (retry_fault == RETRY_FAULT_TUPLE_DEAD)
		ItemIdSetDead(item);
	/* Publication coordinates are the runtime fixture, not a WAL replay. */
	PageSetLSN(page, 200);
	((PageHeader)page)->pd_block_scn = 200;
	receipt_clock_us = 101; /* READY's prepare deadline was 100. */
	if (retry_fault == RETRY_FAULT_MODIFIER)
		receipt_modifier_valid = false;
	if (retry_fault == RETRY_FAULT_BINDING)
		retry_binding->slot_offset += sizeof(TTSlot);
	if (retry_fault == RETRY_FAULT_CANCEL || retry_fault == RETRY_FAULT_ADMISSION)
		ereport(ERROR,
				(errcode(retry_fault == RETRY_FAULT_CANCEL ? ERRCODE_QUERY_CANCELED
														   : ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("scripted capacity-wait interruption")));
	switch (wake_result) {
	case CLUSTER_TXW_RESOLVED:
	case CLUSTER_TXW_RETRY:
		*reason = "ITL_FRESH_REQUALIFY";
		break;
	case CLUSTER_TXW_TIMEOUT:
		*reason = "ITL_CAPACITY_WAIT_EXPIRED";
		break;
	case CLUSTER_TXW_DEADLOCK:
		*reason = "ITL_WAIT_DEADLOCK";
		break;
	default:
		*reason = "ITL_BLOCKER_UNPROVABLE";
		break;
	}
	return wake_result;
}

/* Initial PREPARE's storage service is outside this READY-retry test. An
 * exact receipt must survive; invalidation after its old budget must refuse.
 * Neither case may start a new allocation or publication. */
static ClusterUndoRecordPrepareResult
itl_unexpected_prepare(uint8 record_type, uint16 capacity, uint16 segment, uint16 offset,
					   UBA previous, uint64 deadline, ClusterUndoRecordPrepareReceipt *receipt)
{
	UT_ASSERT(false);
	return CLUSTER_UNDO_RECORD_PREPARE_REFUSED;
}

#define cluster_undo_record_prepare itl_unexpected_prepare
#include "test_cluster_heap_itl_retry_helpers.inc"
#undef cluster_undo_record_prepare

typedef struct RetryObservation {
	bool requalified;
	bool reacquire;
	bool errored;
	bool old_key_retired;
	bool tuple_address_reloaded;
	Buffer old_vm;
	Buffer new_vm;
	uint64 deadline;
} RetryObservation;

/* Error exits inspect these fields after longjmp. Automatic nonvolatile
 * locals changed by the real consumer would have indeterminate values. */
typedef struct RetryOwner {
	ClusterUndoRecordPrepareReceipt receipt;
	PGAlignedBlock image;
	Buffer old_vm;
	Buffer new_vm;
	uint64 capacity_deadline;
	HeapTuple old_key;
	bool key_copied;
} RetryOwner;

static RetryObservation
run_retry(bool pair, bool temporary_lock, int vm_kind, RetryFault fault, ClusterTxwResult result,
		  ClusterUndoRecordPrepareReceipt *final_receipt)
{
	ClusterCtrcTargetV1 pending;
	RetryOwner *const retry_owner = palloc(sizeof(*retry_owner));
	ClusterCanonicalTxnBinding canonical_binding = { 0 };
	Relation relation = (Relation)1;
	Buffer buffer = 1, newbuf = pair ? 2 : 1;
	Buffer itl_capacity_wait_buffer = pair ? 2 : 1;
	BlockNumber block = 44;
	TransactionId canonical_xid = 700;
	uint64 undo_prepare_deadline_us = 100;
	CommandId pgrac_entry_cid = 7, cid = 999;
	bool iscombo = true, old_tuple_temp_locked = temporary_lock;
	HeapTupleData oldtup = { 0 };
	ItemPointerData tid;
	ItemPointer otid = &tid;
	ItemId lp;
	Page page;
	volatile RetryObservation observed = { 0 };

#define undo_receipt (retry_owner->receipt)
#define image (retry_owner->image)
#define vmbuffer (retry_owner->old_vm)
#define vmbuffer_new (retry_owner->new_vm)
#define itl_capacity_absolute_deadline_us (retry_owner->capacity_deadline)
#define old_key_tuple (retry_owner->old_key)
#define old_key_copied (retry_owner->key_copied)
	vmbuffer = vm_kind ? 3 : InvalidBuffer;
	vmbuffer_new = pair && vm_kind > 1 ? (vm_kind == 2 ? 3 : 4) : InvalidBuffer;
	itl_capacity_absolute_deadline_us = 500;
	old_key_tuple = palloc(sizeof(HeapTupleData));
	old_key_copied = true;
	/* The VM pin services below model descriptors 3/4; production page
	 * planning only accesses the reused adapter's real heap descriptor 1. */
	NBuffers = 4;
	cluster_undo_record_inline_max_bytes = 128;
	receipt_fixture_ready(UNDO_RECORD_UPDATE, &undo_receipt);
	PageInitHeapPage(image.data, BLCKSZ, 0);
	{
		HeapTupleHeaderData header = { 0 };
		header.t_hoff = SizeofHeapTupleHeader;
		header.t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
		UT_ASSERT_EQ(
			PageAddItem(image.data, (Item)&header, sizeof(header), FirstOffsetNumber, false, true),
			FirstOffsetNumber);
		if (fault == RETRY_FAULT_TUPLE_REDIRECT) {
			/* The redirect points to an allocated line pointer. This does
			 * not execute or certify a native HOT pruning operation. */
			header.t_infomask2 = HEAP_ONLY_TUPLE;
			ItemPointerSet(&header.t_ctid, block, FirstOffsetNumber + 1);
			UT_ASSERT_EQ(PageAddItem(image.data, (Item)&header, sizeof(header),
									 FirstOffsetNumber + 1, false, true),
						 FirstOffsetNumber + 1);
		}
	}
	PageSetLSN(image.data, 100);
	((PageHeader)image.data)->pd_block_scn = 100;
	if (vm_kind)
		PageSetAllVisible(image.data);
	UT_ASSERT(heap_receipt_test_capture(image.data, true, undo_receipt.record_type, &pending));
	UT_ASSERT(cluster_undo_record_ctrc_stage_pending(&undo_receipt, 0, &pending));
	UT_ASSERT(cluster_undo_record_ctrc_prepare_pending(&undo_receipt, 0));
	UT_ASSERT(heap_receipt_test_plan_capture(&undo_receipt));
	UT_ASSERT(heap_receipt_test_plan_recheck(&undo_receipt));
	before_wait_receipt = undo_receipt;
	canonical_binding.segment_id = undo_receipt.tt_slot_segment_id;
	canonical_binding.slot_offset = undo_receipt.tt_slot_offset;
	page = BufferGetPage(buffer);
	lp = PageGetItemId(page, FirstOffsetNumber);
	oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
	oldtup.t_len = ItemIdGetLength(lp);
	ItemPointerSet(otid, block, FirstOffsetNumber);
	memset(pins, 0, sizeof(pins));
	pins[1] = 1;
	pins[2] = pair ? 1 : 0;
	if (BufferIsValid(vmbuffer))
		pins[vmbuffer]++;
	if (BufferIsValid(vmbuffer_new))
		pins[vmbuffer_new]++;
	heap_locked[1] = true;
	heap_locked[2] = pair;
	wait_calls = key_frees = heap_relocks = recent_calls = 0;
	retry_trace_count = 0;
	memset(retry_trace, 0, sizeof(retry_trace));
	vm_pins_during_wait = vm_pins_at_relock = -1;
	pair_case = pair;
	retry_fault = fault;
	wake_result = result;
	expected_capacity_deadline = itl_capacity_absolute_deadline_us;
	retry_binding = &canonical_binding;
	receipt_trap_error = true;
	if (sigsetjmp(receipt_error_jump, 1) == 0) {
		goto l_pgrac_itl_capacity_wait;
#define cluster_native_itl_trace_update_retry retry_observe
#include "test_cluster_heap_itl_retry_consumer.inc"
#undef cluster_native_itl_trace_update_retry
	l_pgrac_reacquire:
		observed.reacquire = true;
		goto done;
	l2:
		observed.requalified = true;
		UT_ASSERT_EQ(cid, 7);
		UT_ASSERT(!iscombo);
		observed.tuple_address_reloaded
			= oldtup.t_data == (HeapTupleHeader)((char *)page + fresh_offset)
			  && oldtup.t_len == SizeofHeapTupleHeader;
	done:
		observed.old_key_retired = old_key_tuple == NULL && !old_key_copied && key_frees == 1;
	} else {
		observed.errored = true;
	}
	receipt_trap_error = false;
	observed.old_vm = vmbuffer;
	observed.new_vm = vmbuffer_new;
	observed.deadline = itl_capacity_absolute_deadline_us;
	*final_receipt = undo_receipt;
	/* No stale pre-wait final plan may authorize a write on the moved tuple. */
	UT_ASSERT(!heap_receipt_test_plan_recheck(&undo_receipt));
	/* Model the test's private allocation context after observing ownership. */
	if (old_key_tuple != NULL)
		pfree(old_key_tuple);
	pfree(retry_owner);
#undef undo_receipt
#undef image
#undef vmbuffer
#undef vmbuffer_new
#undef itl_capacity_absolute_deadline_us
#undef old_key_tuple
#undef old_key_copied
	return observed;
}

UT_TEST(retry_observations_bracket_actual_vm_release_and_exact_receipt_retry)
{
	for (int pair = 0; pair <= 1; pair++)
		for (int vm_kind = 0; vm_kind <= 3; vm_kind++) {
			ClusterUndoRecordPrepareReceipt receipt;
			RetryObservation observed;
			int refs = vm_kind ? 1 + (pair && vm_kind > 1) : 0;

			recent_misses = 0;
			observed = run_retry(pair, pair, vm_kind, RETRY_FAULT_NONE,
							 CLUSTER_TXW_RESOLVED, &receipt);
			UT_ASSERT(!observed.errored);
			UT_ASSERT_EQ(retry_trace_count, 3);
			for (unsigned i = 0; i < 3; i++) {
				UT_ASSERT_EQ(retry_trace[i].phase, i + 1);
				UT_ASSERT_EQ(retry_trace[i].deadline, 500);
				UT_ASSERT_EQ(retry_trace[i].waits, i == 2 ? 1 : 0);
				UT_ASSERT_EQ(retry_trace[i].vm_refs, i == 0 ? refs : 0);
				UT_ASSERT_EQ(retry_trace[i].old_locked, i < 2);
				UT_ASSERT_EQ(retry_trace[i].new_locked, pair && i < 2);
				UT_ASSERT_EQ(retry_trace[i].old_vm,
							 i == 0 && vm_kind ? 3 : InvalidBuffer);
				UT_ASSERT_EQ(retry_trace[i].new_vm,
							 i == 0 && pair && vm_kind > 1 ? (vm_kind == 2 ? 3 : 4)
																 : InvalidBuffer);
				UT_ASSERT_EQ(retry_trace[i].binding.xid, 0); /* Supplied fixture binding. */
				UT_ASSERT_EQ(retry_trace[i].binding.segment_id, receipt.tt_slot_segment_id);
				UT_ASSERT_EQ(retry_trace[i].binding.slot_offset, receipt.tt_slot_offset);
				UT_ASSERT_EQ(memcmp(&retry_trace[i].receipt, &before_wait_receipt,
								   sizeof(receipt)), 0);
			}
			cluster_undo_record_cancel_prepared(&receipt);
		}
}

UT_TEST(failed_wait_or_requalification_never_emits_a_completed_retry)
{
	for (unsigned failure = 0; failure < 4; failure++) {
		ClusterUndoRecordPrepareReceipt receipt;
		RetryObservation observed;
		ClusterTxwResult result = failure == 0 ? CLUSTER_TXW_TIMEOUT
			: failure == 1 ? CLUSTER_TXW_DEADLOCK : CLUSTER_TXW_RESOLVED;
		RetryFault fault = failure == 2 ? RETRY_FAULT_CANCEL
			: failure == 3 ? RETRY_FAULT_MODIFIER : RETRY_FAULT_NONE;

		recent_misses = 0;
		observed = run_retry(true, true, 2, fault, result, &receipt);
		UT_ASSERT(observed.errored);
		UT_ASSERT_EQ(retry_trace_count, 2);
		UT_ASSERT_EQ(retry_trace[0].phase, 1);
		UT_ASSERT_EQ(retry_trace[1].phase, 2);
		UT_ASSERT_EQ(retry_trace[1].old_vm, InvalidBuffer);
		UT_ASSERT_EQ(retry_trace[1].new_vm, InvalidBuffer);
		UT_ASSERT_EQ(retry_trace[1].waits, 0);
		UT_ASSERT_EQ(memcmp(&retry_trace[1].receipt, &before_wait_receipt, sizeof(receipt)), 0);
		receipt_abort_after_lwlock_release();
	}
}

UT_TEST(single_page_retry_releases_vm_before_wait_and_reloads_tuple)
{
	for (int result = 0; result < 2; result++) {
		ClusterUndoRecordPrepareReceipt receipt;
		RetryObservation observed;

		recent_misses = 0;
		observed = run_retry(false, false, 1, 0, result ? CLUSTER_TXW_RETRY : CLUSTER_TXW_RESOLVED,
							 &receipt);
		UT_ASSERT(!observed.errored && observed.requalified && !observed.reacquire);
		UT_ASSERT(observed.old_key_retired && observed.tuple_address_reloaded);
		UT_ASSERT_EQ(wait_calls, 1);
		UT_ASSERT_EQ(vm_pins_during_wait, 0);
		UT_ASSERT_EQ(vm_pins_at_relock, 0);
		UT_ASSERT_EQ(observed.old_vm, 3);
		UT_ASSERT_EQ(pins[3], 1);
		UT_ASSERT_EQ(observed.deadline, 500);
		UT_ASSERT_EQ(receipt.absolute_deadline_us, 100);
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
		UT_ASSERT(cluster_undo_record_reservation.active);
		UT_ASSERT_EQ(memcmp(&receipt, &before_wait_receipt, sizeof(receipt)), 0);
		UT_ASSERT_EQ(memcmp(&receipt, &cluster_undo_record_reservation.receipt, sizeof(receipt)),
					 0);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT(!cluster_undo_record_reservation.active);
	}
}

UT_TEST(pair_retry_releases_each_vm_reference_before_page_choice_reentry)
{
	for (int vm_kind = 0; vm_kind <= 3; vm_kind++) {
		ClusterUndoRecordPrepareReceipt receipt;
		RetryObservation observed;

		recent_misses = 0;
		observed = run_retry(true, true, vm_kind, 0, CLUSTER_TXW_RESOLVED, &receipt);
		UT_ASSERT(!observed.errored && observed.reacquire && !observed.requalified);
		UT_ASSERT(observed.old_key_retired);
		UT_ASSERT_EQ(vm_pins_during_wait, 0);
		UT_ASSERT_EQ(pins[2] + pins[3] + pins[4], 0);
		UT_ASSERT(!heap_locked[1] && !heap_locked[2]);
		UT_ASSERT_EQ(observed.old_vm, InvalidBuffer);
		UT_ASSERT_EQ(observed.new_vm, InvalidBuffer);
		UT_ASSERT_EQ(observed.deadline, 500);
		UT_ASSERT_EQ(memcmp(&receipt, &before_wait_receipt, sizeof(receipt)), 0);
		UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
		UT_ASSERT(cluster_undo_record_reservation.active);
		cluster_undo_record_cancel_prepared(&receipt);
		UT_ASSERT(!cluster_undo_record_reservation.active);
	}
}

UT_TEST(repin_retries_a_resident_miss_without_carrying_a_vm_pin)
{
	ClusterUndoRecordPrepareReceipt receipt;
	RetryObservation observed;

	recent_misses = 1;
	observed = run_retry(false, false, 1, 0, CLUSTER_TXW_RESOLVED, &receipt);
	UT_ASSERT(!observed.errored && observed.requalified && observed.tuple_address_reloaded);
	UT_ASSERT_EQ(recent_calls, 2);
	UT_ASSERT_EQ(heap_relocks, 2);
	UT_ASSERT_EQ(vm_pins_at_relock, 0);
	UT_ASSERT_EQ(pins[3], 1);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls, 0);
	cluster_undo_record_cancel_prepared(&receipt);
}

UT_TEST(exact_receipt_invalidation_refuses_without_renewing_or_orphaning)
{
	for (int fault = 1; fault <= 2; fault++) {
		ClusterUndoRecordPrepareReceipt receipt;
		RetryObservation observed;

		recent_misses = 0;
		observed = run_retry(false, false, 0, fault, CLUSTER_TXW_RESOLVED, &receipt);
		UT_ASSERT(observed.errored && !observed.requalified && !observed.reacquire);
		UT_ASSERT_EQ(observed.deadline, 500);
		UT_ASSERT_EQ(receipt_error_code, ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA);
		UT_ASSERT_EQ(receipt_unref_calls, 1);
		UT_ASSERT_EQ(receipt_cancel_calls, 1);
		UT_ASSERT_EQ(receipt_apply_calls, 0);
		UT_ASSERT(!cluster_undo_record_reservation.active);
		UT_ASSERT(!heap_locked[1]);
	}
}

static void
check_wait_exit(bool pair, int vm_kind, ClusterTxwResult result, RetryFault fault, int sqlstate,
				const char *reason)
{
	ClusterUndoRecordPrepareReceipt receipt;
	RetryObservation observed;

	recent_misses = 0;
	observed = run_retry(pair, pair, vm_kind, fault, result, &receipt);
	UT_ASSERT(observed.errored);
	if (!observed.errored) {
		if (heap_locked[1])
			LockBuffer(1, BUFFER_LOCK_UNLOCK);
		receipt_abort_after_lwlock_release();
		return;
	}
	UT_ASSERT(!observed.requalified && !observed.reacquire);
	UT_ASSERT_EQ(receipt_error_code, sqlstate);
	if (reason != NULL) {
		UT_ASSERT_STR_CONTAINS(receipt_error_detail, reason);
		UT_ASSERT_STR_CONTAINS(receipt_error_detail, "deadline_us=500");
	} else
		UT_ASSERT_EQ(receipt_error_detail[0], '\0');
	UT_ASSERT_EQ(wait_calls, 1);
	UT_ASSERT_EQ(vm_pins_during_wait, 0);
	UT_ASSERT_EQ(pins[2] + pins[3] + pins[4], 0);
	UT_ASSERT_EQ(observed.old_vm, InvalidBuffer);
	UT_ASSERT_EQ(observed.new_vm, InvalidBuffer);
	UT_ASSERT(!heap_locked[1] && !heap_locked[2]);
	UT_ASSERT_EQ(heap_relocks + recent_calls, 0);
	UT_ASSERT(!observed.old_key_retired);
	UT_ASSERT_EQ(key_frees, 0);
	UT_ASSERT_EQ(observed.deadline, 500);
	UT_ASSERT_EQ(receipt.absolute_deadline_us, 100);
	UT_ASSERT_EQ(memcmp(&receipt, &before_wait_receipt, sizeof(receipt)), 0);
	UT_ASSERT_EQ(memcmp(&receipt, &cluster_undo_record_reservation.receipt, sizeof(receipt)), 0);
	UT_ASSERT(cluster_undo_record_reservation.active);
	UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
	UT_ASSERT_EQ(receipt_prepare_calls, 1);
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
	UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);

	/* PG releases its locks before invoking this real exact receipt owner. */
	receipt_abort_after_lwlock_release();
	UT_ASSERT(!cluster_undo_record_reservation.active);
	UT_ASSERT(!cluster_undo_record_reservation.consume_locked);
	UT_ASSERT_EQ(receipt_cancel_calls, 1);
	UT_ASSERT_EQ(receipt_unref_calls, 1);
	UT_ASSERT_EQ(receipt_leave_calls, 1);
	UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);
	UT_ASSERT_EQ(receipt_error_code, sqlstate);
	receipt_abort_after_lwlock_release();
	UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
}

UT_TEST(capacity_timeout_keeps_original_sqlstate_and_abort_owner)
{
	for (int pair = 0; pair < 2; pair++)
		for (int vm_kind = 0; vm_kind <= (pair ? 3 : 1); vm_kind++)
			check_wait_exit(pair, vm_kind, CLUSTER_TXW_TIMEOUT, 0, ERRCODE_CLUSTER_GES_TIMEOUT,
							"PGRAC_REASON=ITL_CAPACITY_WAIT_EXPIRED");
}

UT_TEST(capacity_deadlock_keeps_original_sqlstate_and_abort_owner)
{
	for (int pair = 0; pair < 2; pair++)
		for (int vm_kind = 0; vm_kind <= (pair ? 3 : 1); vm_kind++)
			check_wait_exit(pair, vm_kind, CLUSTER_TXW_DEADLOCK, 0, ERRCODE_T_R_DEADLOCK_DETECTED,
							"PGRAC_REASON=ITL_WAIT_DEADLOCK");
}

UT_TEST(unprovable_capacity_keeps_refusal_and_abort_owner)
{
	for (int pair = 0; pair < 2; pair++)
		for (int vm_kind = 0; vm_kind <= (pair ? 3 : 1); vm_kind++)
			check_wait_exit(pair, vm_kind, CLUSTER_TXW_UNPROVABLE, 0,
							ERRCODE_PROGRAM_LIMIT_EXCEEDED, "PGRAC_REASON=ITL_BLOCKER_UNPROVABLE");
}

UT_TEST(wait_errors_preserve_cancel_or_admission_sqlstate_and_abort_owner)
{
	for (int pair = 0; pair < 2; pair++)
		for (int vm_kind = 0; vm_kind <= (pair ? 3 : 1); vm_kind++)
			for (int error = 0; error < 2; error++)
				check_wait_exit(
					pair, vm_kind, CLUSTER_TXW_RESOLVED,
					error ? RETRY_FAULT_ADMISSION : RETRY_FAULT_CANCEL,
					error ? ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS : ERRCODE_QUERY_CANCELED, NULL);
}

/* Removing the real consumer's post-wait line-pointer check must not let
 * a pruned or redirected tuple reach l2 using the old READY plan. */
UT_TEST(pruned_tuple_after_capacity_wait_refuses_and_keeps_exact_abort_owner)
{
	const RetryFault faults[]
		= { RETRY_FAULT_TUPLE_UNUSED, RETRY_FAULT_TUPLE_REDIRECT, RETRY_FAULT_TUPLE_DEAD };

	for (unsigned fault = 0; fault < lengthof(faults); fault++)
		for (int result = 0; result < 2; result++)
			for (int vm_kind = 0; vm_kind < 2; vm_kind++) {
				ClusterUndoRecordPrepareReceipt receipt;
				RetryObservation observed;

				recent_misses = 0;
				observed = run_retry(false, false, vm_kind, faults[fault],
									 result ? CLUSTER_TXW_RETRY : CLUSTER_TXW_RESOLVED, &receipt);
				UT_ASSERT(observed.errored && !observed.requalified && !observed.reacquire);
				UT_ASSERT_EQ(receipt_error_code, ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA);
				UT_ASSERT_STR_CONTAINS(receipt_error_detail, "PGRAC_REASON=CALLER_CAUSE_UNPROVEN");
				UT_ASSERT_EQ(wait_calls, 1);
				UT_ASSERT_EQ(vm_pins_during_wait, 0);
				UT_ASSERT_EQ(vm_pins_at_relock, 0);
				UT_ASSERT_EQ(heap_relocks, 1);
				UT_ASSERT_EQ(recent_calls, vm_kind);
				UT_ASSERT_EQ(key_frees, 1);
				UT_ASSERT_EQ(observed.deadline, 500);
				UT_ASSERT_EQ(receipt.absolute_deadline_us, 100);
				UT_ASSERT_EQ(memcmp(&receipt, &before_wait_receipt, sizeof(receipt)), 0);
				UT_ASSERT_EQ(
					memcmp(&receipt, &cluster_undo_record_reservation.receipt, sizeof(receipt)), 0);
				UT_ASSERT(cluster_undo_record_reservation.active);
				UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
				UT_ASSERT_EQ(receipt_prepare_calls, 1);
				UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 0);
				UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);
				UT_ASSERT(heap_locked[1] && !heap_locked[2]);
				UT_ASSERT_EQ(pins[1], 1);
				UT_ASSERT_EQ(pins[2] + pins[4], 0);
				UT_ASSERT_EQ(pins[3], vm_kind);
				UT_ASSERT_EQ(observed.old_vm, vm_kind ? 3 : InvalidBuffer);
				UT_ASSERT_EQ(observed.new_vm, InvalidBuffer);

				/* The refusal occurs after the original relock/repin. Model
				 * buffer cleanup, then invoke the real receipt abort owner. */
				if (heap_locked[1])
					LockBuffer(1, BUFFER_LOCK_UNLOCK);
				if (BufferIsValid(observed.old_vm))
					ReleaseBuffer(observed.old_vm);
				ReleaseBuffer(1);
				receipt_abort_after_lwlock_release();
				UT_ASSERT(!cluster_undo_record_reservation.active);
				UT_ASSERT(!cluster_undo_record_reservation.consume_locked);
				UT_ASSERT_EQ(receipt_cancel_calls, 1);
				UT_ASSERT_EQ(receipt_unref_calls, 1);
				UT_ASSERT_EQ(receipt_leave_calls, 1);
				UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);
				receipt_abort_after_lwlock_release();
				UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
			}
}

UT_TEST(vm_repin_errors_keep_ready_receipt_for_exact_abort_owner)
{
	const int sqlstates[] = { ERRCODE_QUERY_CANCELED, ERRCODE_T_R_DEADLOCK_DETECTED,
							  ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS };

	for (int site = 1; site <= 2; site++)
		for (int result = 0; result < 2; result++)
			for (unsigned state = 0; state < lengthof(sqlstates); state++) {
				ClusterUndoRecordPrepareReceipt receipt;
				RetryObservation observed;

				recent_misses = 0;
				vm_failure_site = site;
				vm_failure_sqlstate = sqlstates[state];
				vm_failure_hits = 0;
				observed = run_retry(false, false, 1, RETRY_FAULT_NONE,
									 result ? CLUSTER_TXW_RETRY : CLUSTER_TXW_RESOLVED, &receipt);
				UT_ASSERT(observed.errored);
				if (observed.errored) {
					UT_ASSERT(!observed.requalified && !observed.reacquire);
					UT_ASSERT_EQ(vm_failure_hits, 1);
					UT_ASSERT_EQ(receipt_error_level, ERROR);
					UT_ASSERT_EQ(receipt_error_code, sqlstates[state]);
					UT_ASSERT_EQ(receipt_error_detail[0], '\0');
					UT_ASSERT_EQ(wait_calls, 1);
					UT_ASSERT_EQ(vm_pins_during_wait, 0);
					UT_ASSERT_EQ(heap_relocks, site - 1);
					UT_ASSERT_EQ(recent_calls, site - 1);
					UT_ASSERT_EQ(heap_locked[1], site == 2);
					UT_ASSERT(!heap_locked[2]);
					UT_ASSERT_EQ(pins[1], 1);
					UT_ASSERT_EQ(pins[2] + pins[3] + pins[4], 0);
					UT_ASSERT_EQ(observed.old_vm, InvalidBuffer);
					UT_ASSERT_EQ(observed.new_vm, InvalidBuffer);
					UT_ASSERT_EQ(key_frees, 1);
					UT_ASSERT_EQ(observed.deadline, 500);
					UT_ASSERT_EQ(receipt.absolute_deadline_us, 100);
					UT_ASSERT_EQ(memcmp(&receipt, &before_wait_receipt, sizeof(receipt)), 0);
					UT_ASSERT_EQ(
						memcmp(&receipt, &cluster_undo_record_reservation.receipt, sizeof(receipt)),
						0);
					UT_ASSERT(cluster_undo_record_reservation.active);
					UT_ASSERT_EQ(receipt.ctrc_prepared_mask, 1);
					UT_ASSERT_EQ(receipt_prepare_calls, 1);
					UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls,
								 0);
					UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);
				}

				/* Buffer cleanup is modeled explicitly; the exact receipt
				 * abort owner below is production code, not ResourceOwner. */
				if (heap_locked[1])
					LockBuffer(1, BUFFER_LOCK_UNLOCK);
				if (BufferIsValid(observed.old_vm))
					ReleaseBuffer(observed.old_vm);
				ReleaseBuffer(1);
				receipt_abort_after_lwlock_release();
				UT_ASSERT(!cluster_undo_record_reservation.active);
				UT_ASSERT(!cluster_undo_record_reservation.consume_locked);
				UT_ASSERT_EQ(receipt_cancel_calls, 1);
				UT_ASSERT_EQ(receipt_unref_calls, 1);
				UT_ASSERT_EQ(receipt_leave_calls, 1);
				UT_ASSERT_EQ(receipt_apply_calls + receipt_wal_calls, 0);
				if (observed.errored)
					UT_ASSERT_EQ(receipt_error_code, sqlstates[state]);
				receipt_abort_after_lwlock_release();
				UT_ASSERT_EQ(receipt_cancel_calls + receipt_unref_calls + receipt_leave_calls, 3);
			}
	vm_failure_site = 0;
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(single_page_retry_releases_vm_before_wait_and_reloads_tuple);
	UT_RUN(pair_retry_releases_each_vm_reference_before_page_choice_reentry);
	UT_RUN(repin_retries_a_resident_miss_without_carrying_a_vm_pin);
	UT_RUN(exact_receipt_invalidation_refuses_without_renewing_or_orphaning);
	UT_RUN(capacity_timeout_keeps_original_sqlstate_and_abort_owner);
	UT_RUN(capacity_deadlock_keeps_original_sqlstate_and_abort_owner);
	UT_RUN(unprovable_capacity_keeps_refusal_and_abort_owner);
	UT_RUN(wait_errors_preserve_cancel_or_admission_sqlstate_and_abort_owner);
	UT_RUN(pruned_tuple_after_capacity_wait_refuses_and_keeps_exact_abort_owner);
	UT_RUN(vm_repin_errors_keep_ready_receipt_for_exact_abort_owner);
	UT_RUN(retry_observations_bracket_actual_vm_release_and_exact_receipt_retry);
	UT_RUN(failed_wait_or_requalification_never_emits_a_completed_retry);
	UT_DONE();
	return ut_failed_count != 0;
}
