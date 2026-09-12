/*-------------------------------------------------------------------------
 * test_cluster_heap_update_temp_lock.c
 *    Ordinary UPDATE predecessor-lock ownership tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_update_temp_lock.c
 *
 * Actual extracted heap adapter and consumers; row-lock I/O and receipt
 * services are explicit runtime seams, not a full backend substitute.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "storage/bufmgr.h"
#include "utils/elog.h"

#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int pins, releases, cancels, lock_calls, prepares, fault;
static TM_Result lock_result;
static ClusterUndoRecordPrepareReceipt *outer;
static bool content_locked, route_enabled;
static PGAlignedBlock current_page;
char *BufferBlocks = current_page.data;
Block *LocalBufferBlockPointers;
int cluster_undo_record_inline_max_bytes = 128;
int NBuffers = 1;
int NLocBuffer = 0;
int cluster_node_id = 0;

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
errdetail(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	siglongjmp(*PG_exception_stack, 1);
}
void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

void
cluster_undo_record_cancel_prepared(ClusterUndoRecordPrepareReceipt *receipt)
{
	cancels++;
	if (fault != 3)
		memset(receipt, 0, sizeof(*receipt));
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(pins, 2);
	pins--;
	releases++;
}

static bool
cluster_heap_prepare_undo_record_exact(uint8 record_type, uint16 capacity, uint16 segment,
									   uint16 offset, UBA previous, uint64 deadline,
									   ClusterUndoRecordPrepareReceipt *receipt)
{
	prepares++;
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(record_type, UNDO_RECORD_UPDATE);
	UT_ASSERT_EQ(capacity, 128);
	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT_EQ(offset, 2);
	UT_ASSERT_EQ(deadline, 900);
	UT_ASSERT(UBA_is_invalid(previous));
	UT_ASSERT_EQ(receipt->magic, 0);
	if (fault == 4)
		return false;
	receipt->magic = 2;
	return true;
}

static int
cluster_heap_undo_receipt_errdetail(bool ctrc)
{
	return 0;
}

TM_Result
heap_lock_tuple(Relation relation, HeapTuple tuple, CommandId cid, LockTupleMode mode,
				LockWaitPolicy policy, bool follow_updates, Buffer *buffer, TM_FailureData *failure,
				const ClusterHeapSuccessorProof *expected, ClusterHeapSuccessorProof *next)
{
	lock_calls++;
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(outer->magic, 0);
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&tuple->t_self), 17);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tuple->t_self), 3);
	UT_ASSERT_EQ(cid, 7);
	UT_ASSERT_EQ(mode, LockTupleNoKeyExclusive);
	UT_ASSERT_EQ(policy, LockWaitBlock);
	UT_ASSERT(!follow_updates && expected == NULL && next == NULL);
	if (fault == 5)
		siglongjmp(*PG_exception_stack, 1);
	*buffer = 1;
	pins++;
	if (fault == 1)
		siglongjmp(*PG_exception_stack, 1);
	return lock_result;
}

#include "test_cluster_heap_update_temp_lock.inc"

static bool
cluster_itl_write_path_enabled(Relation relation)
{
	return route_enabled;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(content_locked);
	content_locked = false;
}
void
cluster_heap_lock_with_vm_repin(Relation relation, BlockNumber block, Buffer buffer, Buffer *vm)
{
	UT_ASSERT(!content_locked);
	UT_ASSERT_EQ(pins, 1);
	content_locked = true;
}

UT_TEST(real_update_consumer_requalifies_and_preserves_excluded_routes)
{
	int leg;

	for (leg = 0; leg < 6; leg++) {
		ClusterUndoRecordPrepareReceipt undo_receipt = { 0 };
		ClusterCanonicalTxnBinding canonical_binding = { 0 };
		Relation relation = (Relation)1;
		Page page = (Page)current_page.data;
		PageHeader header = (PageHeader)page;
		HeapTupleData oldtup = { 0 };
		ItemId lp;
		LockTupleMode lock_mode = LockTupleNoKeyExclusive;
		LockTupleMode *lockmode = &lock_mode;
		Buffer buffer = 1, vmbuffer = InvalidBuffer;
		BlockNumber block = 17;
		CommandId pgrac_entry_cid = 7, cid = 999;
		uint64 undo_prepare_deadline_us = 900;
		Size newtupsize = leg == 5 ? 32 : 256, pagefree = 64;
		bool need_toast = false, wait = true, iscombo = true;
		bool old_tuple_temp_locked = leg == 2;
		bool cluster_current_mx_recomposed = leg == 3;
		bool requalified = false;

		memset(page, 0, BLCKSZ);
		header->pd_flags = PD_HAS_ITL;
		header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
		header->pd_lower = SizeOfPageHeaderData + 3 * sizeof(ItemIdData);
		header->pd_upper = header->pd_special - MAXALIGN(SizeofHeapTupleHeader);
		lp = PageGetItemId(page, 3);
		ItemIdSetNormal(lp, header->pd_upper, SizeofHeapTupleHeader);
		oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
		oldtup.t_len = SizeofHeapTupleHeader;
		ItemPointerSet(&oldtup.t_self, 17, 3);
		undo_receipt.magic = 1;
		outer = &undo_receipt;
		canonical_binding.segment_id = 1;
		canonical_binding.slot_offset = 2;
		fault = 0;
		pins = 1;
		cancels = releases = prepares = lock_calls = 0;
		content_locked = true;
		route_enabled = leg != 4;
		lock_result = leg == 1 ? TM_Updated : TM_Ok;
#include "test_cluster_heap_update_temp_consumer.inc"
		goto finished;
	l2:
		requalified = true;
	finished:
		UT_ASSERT_EQ(requalified, leg < 2);
		UT_ASSERT_EQ(old_tuple_temp_locked, leg == 0 || leg == 2);
		UT_ASSERT_EQ(lock_calls, leg < 2 ? 1 : 0);
		UT_ASSERT_EQ(pins, 1);
		UT_ASSERT(content_locked);
		if (requalified) {
			UT_ASSERT_EQ(cid, 7);
			UT_ASSERT(!iscombo);
		}
	}
	content_locked = false;
}

static void
run_adapter(TM_Result expected_result, int failure, bool applied)
{
	ClusterUndoRecordPrepareReceipt receipt = { 0 };
	ClusterCanonicalTxnBinding binding = { 0 };
	ItemPointerData tid;
	volatile bool caught = false;
	volatile TM_Result result = TM_Invisible;

	pins = 1;
	content_locked = false;
	releases = cancels = lock_calls = prepares = 0;
	fault = failure;
	lock_result = expected_result;
	receipt.magic = 1;
	receipt.ctrc_applied_mask = applied ? 1 : 0;
	outer = &receipt;
	binding.segment_id = 1;
	binding.slot_offset = 2;
	ItemPointerSet(&tid, 17, 3);
	PG_TRY();
	{
		result = cluster_heap_lock_update_predecessor((Relation)1, &tid, 7, LockTupleNoKeyExclusive,
													  LockWaitBlock, &binding, 900, &receipt);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, applied || failure != 0);
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(lock_calls, applied || failure == 3 ? 0 : 1);
	UT_ASSERT_EQ(releases, applied || failure == 3 || failure == 5 ? 0 : 1);
	UT_ASSERT_EQ(cancels, applied ? 0 : 1);
	UT_ASSERT_EQ(prepares, applied || failure == 1 || failure == 3 || failure == 5 ? 0 : 1);
	if (!caught) {
		UT_ASSERT_EQ(result, expected_result);
		UT_ASSERT_EQ(receipt.magic, 2);
	}
}

UT_TEST(success_preserves_outer_pin_and_original_budget)
{
	run_adapter(TM_Ok, 0, false);
}
UT_TEST(updated_is_not_success)
{
	run_adapter(TM_Updated, 0, false);
}
UT_TEST(would_block_is_not_success)
{
	run_adapter(TM_WouldBlock, 0, false);
}
UT_TEST(error_after_pin_releases_only_nested_pin)
{
	run_adapter(TM_Ok, 1, false);
}
UT_TEST(published_outer_is_never_cancelled)
{
	run_adapter(TM_Ok, 0, true);
}
UT_TEST(cancel_mismatch_cannot_enter_nested_producer)
{
	run_adapter(TM_Ok, 3, false);
}
UT_TEST(reprepare_failure_releases_nested_pin_first)
{
	run_adapter(TM_Ok, 4, false);
}
UT_TEST(error_before_pin_does_not_release_outer_pin)
{
	run_adapter(TM_Ok, 5, false);
}

UT_TEST(real_row_lock_return_cancels_only_unpublished_receipt)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterUndoRecordPrepareReceipt undo_receipt = { 0 };

		fault = 0;
		cancels = 0;
		undo_receipt.magic = leg == 0 ? 0 : 2;
		undo_receipt.ctrc_applied_mask = leg == 2 ? 1 : 0;
#include "test_cluster_heap_lock_return_receipt.inc"
		UT_ASSERT_EQ(cancels, leg == 1 ? 1 : 0);
		UT_ASSERT_EQ(undo_receipt.magic, leg == 2 ? 2 : 0);
		UT_ASSERT_EQ(undo_receipt.ctrc_applied_mask, leg == 2 ? 1 : 0);
	}
}

static int successor_mx_calls;

/* The real planner's existing multi-locker branch is preserved; membership
 * lookup is the explicit fixture seam, not a replacement header planner. */
static void
GetMultiXactIdHintBits(TransactionId xmax, uint16 *infomask, uint16 *infomask2)
{
	successor_mx_calls++;
	*infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
	*infomask2 = 0;
}

static void
check_successor_header(int leg)
{
	PGAlignedBlock old_bytes, new_bytes, old_before;
	HeapTupleData oldtup = { 0 }, new_tuple = { 0 };
	HeapTuple newtup = &new_tuple;
	TransactionId xid = 12345, xmax_new_tuple = InvalidTransactionId;
	CommandId cid = 7;
	uint16 infomask_new_tuple = 0, infomask2_new_tuple = 0;
	bool old_tuple_temp_locked = leg != 1;
	bool cluster_current_mx_recomposed = leg == 4;
	bool checked_lockers = leg == 6, locker_remains = false;
	bool expected_invalid = leg == 0 || leg == 4 || leg == 5 || leg == 6;

	memset(&old_bytes, 0, sizeof(old_bytes));
	memset(&new_bytes, 0, sizeof(new_bytes));
	oldtup.t_data = (HeapTupleHeader)old_bytes.data;
	newtup->t_data = (HeapTupleHeader)new_bytes.data;
	oldtup.t_data->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmax(oldtup.t_data, leg == 2 ? xid + 4 : xid);
	if (leg == 3)
		oldtup.t_data->t_infomask |= HEAP_XMAX_IS_MULTI;
	if (leg == 5)
		oldtup.t_data->t_infomask |= HEAP_XMAX_INVALID;
	if (leg == 7)
		oldtup.t_data->t_infomask = 0; /* A real deleting xmax, not legacy EXCL-only. */
	newtup->t_data->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED | HEAP_HASNULL;
	newtup->t_data->t_infomask2 = HEAP_KEYS_UPDATED;
	memcpy(old_before.data, old_bytes.data, BLCKSZ);
	successor_mx_calls = 0;
#include "test_cluster_heap_update_successor_header.inc"
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(newtup->t_data), expected_invalid ? InvalidTransactionId
															: leg == 2		 ? xid + 4
																			 : xid);
	UT_ASSERT_EQ((newtup->t_data->t_infomask & HEAP_XMAX_INVALID) != 0, expected_invalid);
	UT_ASSERT_EQ((newtup->t_data->t_infomask & HEAP_XMAX_LOCK_ONLY) != 0, !expected_invalid);
	UT_ASSERT_EQ((newtup->t_data->t_infomask & HEAP_XMAX_IS_MULTI) != 0, leg == 3);
	UT_ASSERT_EQ(successor_mx_calls, leg == 3 ? 1 : 0);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(newtup->t_data), xid);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawCommandId(newtup->t_data), cid);
	UT_ASSERT((newtup->t_data->t_infomask & (HEAP_UPDATED | HEAP_HASNULL))
			  == (HEAP_UPDATED | HEAP_HASNULL));
	UT_ASSERT_EQ(newtup->t_data->t_infomask2 & HEAP2_XACT_MASK, 0);
	UT_ASSERT_EQ(memcmp(old_before.data, old_bytes.data, BLCKSZ), 0);
}

UT_TEST(real_successor_does_not_inherit_own_temporary_lock)
{
	check_successor_header(0);
}
UT_TEST(real_successor_preserves_all_other_planner_branches)
{
	int leg;

	for (leg = 1; leg < 8; leg++)
		check_successor_header(leg);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(real_successor_does_not_inherit_own_temporary_lock);
	UT_RUN(real_successor_preserves_all_other_planner_branches);
	UT_RUN(real_row_lock_return_cancels_only_unpublished_receipt);
	UT_RUN(real_update_consumer_requalifies_and_preserves_excluded_routes);
	UT_RUN(success_preserves_outer_pin_and_original_budget);
	UT_RUN(updated_is_not_success);
	UT_RUN(would_block_is_not_success);
	UT_RUN(error_after_pin_releases_only_nested_pin);
	UT_RUN(published_outer_is_never_cancelled);
	UT_RUN(cancel_mismatch_cannot_enter_nested_producer);
	UT_RUN(reprepare_failure_releases_nested_pin_first);
	UT_RUN(error_before_pin_does_not_release_outer_pin);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
