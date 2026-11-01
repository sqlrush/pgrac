/*-------------------------------------------------------------------------
 * test_cluster_copy_vm_index.c
 *    COPY publishes indexes only after its entire heap/VM batch returns.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Complete original COPY flush and heap_multi_insert execute. Receipt-safe
 * delegation uses the original VM capture/clear/WAL fixture; native batches
 * retain their original loop. Preflight, materialization, lease, physical
 * selection/transport, receipt/ITL, memory contexts and index/trigger leaves
 * are controlled. This is not SQL, full heap_insert, commit or durability.
 *-------------------------------------------------------------------------
 */
int copy_wal_inherited_main(void);
#define INSERT_VM_WAL_MAIN copy_wal_inherited_main
#include "test_cluster_heap_insert_vm_wal_chain.c"
#undef INSERT_VM_WAL_MAIN

#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "../../backend/access/heap/heapam_r4_private.h"

static CopyFromStateData copy_state;
static EState copy_estate;
static ResultRelInfo copy_result;
static TriggerDesc copy_triggers;
static TupleTableSlot copy_slots[2];
static HeapTupleData copy_tuples[2];
static PGAlignedBlock copy_tuple_storage[2];
static HeapTuple copy_allocated[2];
static BulkInsertStateData copy_bulk;
static List copy_rechecks[2];
static PGAlignedBlock copy_heap_images[2], copy_map_images[2];
static int copy_count, copy_completed, copy_indexes, copy_afters, copy_cleared;
static int copy_preflights, copy_leases, copy_fetches, copy_progress;
static int copy_fail_row, copy_fail_kind;
static int64 copy_processed;
static bool copy_route, copy_shared, copy_visible, copy_index_error;
static bool copy_slot_cleared[2];
static Buffer copy_expected_heap[2];
static char copy_outer_context, copy_tuple_context;
static MemoryContext copy_current_context;

void copy_heap_multi_insert(Relation, TupleTableSlot **, int, CommandId, int,
						   BulkInsertState);

static int
copy_slot_number(TupleTableSlot *slot)
{
	for (int i = 0; i < copy_count; i++)
		if (slot == &copy_slots[i])
			return i;
	abort();
}

static HeapTuple
copy_fetch(TupleTableSlot *slot, bool materialize, bool *shouldFree)
{
	int row = copy_slot_number(slot);

	UT_ASSERT(materialize && shouldFree == NULL && !copy_slot_cleared[row]);
	copy_fetches++;
	return &copy_tuples[row];
}

static void
copy_preflight(Relation relation, const char *operation)
{
	UT_ASSERT(relation == &relation_data && strcmp(operation, "MULTI INSERT") == 0);
	UT_ASSERT_EQ(copy_fetches + copy_leases, 0);
	copy_preflights++;
}

static void
copy_lease(Relation relation, BulkInsertState bistate)
{
	UT_ASSERT(relation == &relation_data && bistate == &copy_bulk);
	UT_ASSERT_EQ(copy_preflights, 1);
	UT_ASSERT_EQ(copy_fetches, 0);
	copy_leases++;
}

static void
copy_heap_insert(Relation relation, HeapTuple value, CommandId cid,
				 int options, BulkInsertState bistate)
{
	int row = copy_completed;
	Buffer heap;

	if (row < 0 || row >= copy_count || row >= lengthof(copy_expected_heap))
		abort();
	heap = copy_expected_heap[row];
	UT_ASSERT(value == &copy_tuples[row]);
	UT_ASSERT(relation == &relation_data && bistate == &copy_bulk);
	UT_ASSERT_EQ(cid, 73);
	UT_ASSERT_EQ(options, 0);
	UT_ASSERT_EQ(value->t_tableOid, 4242);
	UT_ASSERT_EQ(copy_slots[row].tts_tableOid, 4242);
	UT_ASSERT_EQ(copy_indexes + copy_afters + copy_cleared + copy_progress, 0);
	if (row == copy_fail_row) {
		multi_missing_writer = copy_fail_kind == 0;
		if (copy_fail_kind == 1) {
			multi_refusals = multi_replacements = multi_fault = 1;
			multi_refusal_page = row;
		}
		if (copy_fail_kind == 2)
			multi_writer_error = 2;
	}
	tuple = *value;
	execute_single_wal_chain(copy_shared);
	*value = tuple;
	copy_heap_images[row] = images[0];
	copy_map_images[row] = images[1];
	UT_ASSERT_EQ(registered_buffers[0], heap);
	copy_completed++;
}

static HeapTuple
copy_prepare(Relation relation, HeapTuple value, TransactionId xid,
			 CommandId cid, int options)
{
	UT_ASSERT(relation == &relation_data && xid == 500 && cid == 73 && options == 0);
	return value;
}

static void *
copy_alloc(Size size)
{
	if (size > sizeof(copy_allocated))
		abort();
	return copy_allocated;
}

static Buffer
copy_select(Relation relation, Size len, Buffer *other, int options,
			BulkInsertState bistate, Buffer *vm, Buffer *other_vm, int count)
{
	UT_ASSERT(bistate == &copy_bulk);
	return RelationGetBufferForTuple(relation, len, other, options, NULL,
									 vm, other_vm, count);
}

#define ExecFetchSlotHeapTuple copy_fetch
#define cluster_heap_preflight_write copy_preflight
#define cluster_dl_bulk_acquire copy_lease
#define cluster_itl_write_path_enabled(rel) copy_route
#undef IsParallelWorker
#define IsParallelWorker() false
#define GetCurrentTransactionId() ((TransactionId)500)
#define AssertHasSnapshotForToast(rel) ((void)(rel))
#define heap_insert copy_heap_insert
#define heap_multi_insert copy_heap_multi_insert
#define heap_prepare_insert copy_prepare
#define cluster_smgr_which_for(...) 1
#define cluster_space_relation_get_identity(rel, out) (*(out) = identity, true)
#undef RelationGetTargetPageFreeSpace
#define RelationGetTargetPageFreeSpace(...) (PageGetHeapFreeSpace(pages[0].data) - MAXALIGN(tuple.t_len))
#define palloc copy_alloc
#define CheckForSerializableConflictIn(...) ((void)0)
#define IsCatalogRelation(rel) false
#define CacheInvalidateHeapTuple(...) abort()
#define pgstat_count_heap_insert(...) ((void)0)
#define errhint(...) 0
#define RelationGetBufferForTuple copy_select
#include "test_cluster_copy_heap_insert.inc"
#undef RelationGetBufferForTuple
#undef heap_multi_insert
#undef heap_insert
#undef palloc

static MemoryContext
copy_switch(MemoryContext context)
{
	MemoryContext previous = copy_current_context;

	copy_current_context = context;
	return previous;
}

static void
check_copy_row(int row)
{
	Buffer heap = copy_expected_heap[row], map = heap + 1;

	UT_ASSERT_EQ(copy_processed, 7);
	UT_ASSERT_EQ(inserts, copy_count);
	UT_ASSERT(copy_current_context == (MemoryContext)&copy_outer_context);
	UT_ASSERT(!multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!copy_state.line_buf_valid);
	UT_ASSERT_EQ(copy_state.cur_lineno, 100 + row);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&copy_slots[row].tts_tid), blocks[heap - 1]);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&copy_slots[row].tts_tid), 1);
	UT_ASSERT_EQ(copy_slots[row].tts_tableOid, 4242);
	UT_ASSERT(!PageIsAllVisible(pages[heap - 1].data));
	UT_ASSERT_EQ(PageGetLSN(pages[heap - 1].data), 200);
	if (copy_visible)
		UT_ASSERT_EQ(pages[map - 1].data[SizeOfPageHeaderData] & 3, 0);
	if (copy_route) {
		UT_ASSERT_EQ(copy_completed, copy_count);
		/* Registration copied the input LSN before the original PageSetLSN.
		 * The live page above must already carry the returned record LSN. */
		UT_ASSERT_EQ(PageGetLSN(copy_heap_images[row].data), InvalidXLogRecPtr);
		if (copy_shared) {
			UT_ASSERT_EQ(((PageHeader)copy_heap_images[row].data)->pd_block_scn, 101 + row);
			if (copy_visible) {
				UT_ASSERT_EQ(((PageHeader)copy_map_images[row].data)->pd_block_scn, 101 + row);
				UT_ASSERT_EQ(copy_map_images[row].data[SizeOfPageHeaderData] & 3, 0);
				UT_ASSERT_EQ(copy_map_images[row].data[BLCKSZ - 1], 42);
			}
		}
	} else
		UT_ASSERT_EQ(copy_fetches, copy_count);
}

static List *
copy_index(ResultRelInfo *result, TupleTableSlot *slot, EState *estate,
		   bool update, bool noDupErr, bool *specConflict, List *arbiterIndexes,
		   bool onlySummarizing)
{
	int row = copy_slot_number(slot);

	UT_ASSERT(result == &copy_result && estate == &copy_estate);
	UT_ASSERT(!update && !noDupErr && specConflict == NULL);
	UT_ASSERT(arbiterIndexes == NIL && !onlySummarizing);
	UT_ASSERT_EQ(copy_indexes, row);
	check_copy_row(row);
	copy_indexes++;
	if (copy_index_error)
		elog(ERROR, "controlled COPY index error");
	return &copy_rechecks[row];
}

static void
copy_after(EState *estate, ResultRelInfo *result, TupleTableSlot *slot,
		   List *rechecks, TransitionCaptureState *capture)
{
	int row = copy_slot_number(slot);

	UT_ASSERT(estate == &copy_estate && result == &copy_result && capture == NULL);
	UT_ASSERT(rechecks == (result->ri_NumIndices ? &copy_rechecks[row] : NIL));
	UT_ASSERT_EQ(copy_indexes, result->ri_NumIndices ? row + 1 : 0);
	check_copy_row(row);
	copy_afters++;
}

static TupleTableSlot *
copy_clear(TupleTableSlot *slot)
{
	int row = copy_slot_number(slot);

	UT_ASSERT_EQ(inserts, copy_count);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid),
				 blocks[copy_expected_heap[row] - 1]);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), 1);
	copy_slot_cleared[row] = true;
	copy_cleared++;
	return slot;
}

static void
copy_update_progress(int parameter, int64 value)
{
	UT_ASSERT_EQ(parameter, PROGRESS_COPY_TUPLES_PROCESSED);
	UT_ASSERT_EQ(value, 7 + copy_count);
	UT_ASSERT_EQ(copy_cleared, copy_count);
	copy_progress++;
}

#define MemoryContextSwitchTo copy_switch
#undef GetPerTupleMemoryContext
#define GetPerTupleMemoryContext(estate) ((MemoryContext)&copy_tuple_context)
#define table_multi_insert copy_heap_multi_insert
#define ExecInsertIndexTuples copy_index
#define ExecARInsertTriggers copy_after
#define ExecClearTuple copy_clear
#define list_free(list) ((void)(list))
#define pgstat_progress_update_param copy_update_progress
#ifndef COPY_FLUSH_FIXTURE
#define COPY_FLUSH_FIXTURE "test_cluster_copy_flush.inc"
#endif
#include COPY_FLUSH_FIXTURE

static CopyMultiInsertBuffer copy_buffer;
static CopyMultiInsertInfo copy_info;

static void
prepare_copy(int count, bool shared, bool receipt_route, bool visible, bool indexed)
{
	if (count < 1 || count > lengthof(copy_slots))
		abort();
	prepare_wal_chain(shared, visible);
	memset(&copy_state, 0, sizeof(copy_state));
	memset(&copy_estate, 0, sizeof(copy_estate));
	memset(&copy_result, 0, sizeof(copy_result));
	memset(&copy_buffer, 0, sizeof(copy_buffer));
	memset(&copy_info, 0, sizeof(copy_info));
	memset(copy_slots, 0, sizeof(copy_slots));
	memset(copy_slot_cleared, 0, sizeof(copy_slot_cleared));
	memset(copy_heap_images, 0, sizeof(copy_heap_images));
	memset(copy_map_images, 0, sizeof(copy_map_images));
	copy_count = count;
	copy_route = receipt_route; copy_shared = shared; copy_visible = visible;
	copy_completed = copy_indexes = copy_afters = copy_cleared = 0;
	copy_preflights = copy_leases = copy_fetches = copy_progress = 0;
	copy_fail_row = -1; copy_fail_kind = 0; copy_index_error = false;
	copy_processed = 7;
	copy_current_context = (MemoryContext)&copy_outer_context;
	relation_data.rd_id = 4242;
	copy_result.ri_RelationDesc = &relation_data;
	copy_result.ri_NumIndices = indexed ? 1 : 0;
	copy_info.cstate = &copy_state; copy_info.estate = &copy_estate;
	copy_info.mycid = 73;
	copy_buffer.resultRelInfo = &copy_result;
	copy_buffer.nused = count; copy_buffer.bistate = &copy_bulk;
	copy_state.cur_lineno = 319; copy_state.line_buf_valid = true;
	for (int i = 0; i < count; i++) {
		copy_tuples[i] = tuple;
		memcpy(copy_tuple_storage[i].data, tuple_bytes, sizeof(tuple_bytes));
		copy_tuples[i].t_data = (HeapTupleHeader)copy_tuple_storage[i].data;
		copy_expected_heap[i] = i == 0 ? 1 : 3;
		copy_buffer.slots[i] = &copy_slots[i];
		copy_buffer.linenos[i] = 100 + i;
		ItemPointerSet(&copy_slots[i].tts_tid, 999, 7);
	}
}

static bool
try_copy(void)
{
	if (sigsetjmp(multi_jump, 0) != 0)
		return false;
	CopyMultiInsertBufferFlush(&copy_info, &copy_buffer, &copy_processed);
	UT_ASSERT_EQ(copy_buffer.nused, 0);
	UT_ASSERT_EQ(copy_processed, 7 + copy_count);
	UT_ASSERT_EQ(copy_progress, 1);
	UT_ASSERT_EQ(copy_state.cur_lineno, 319);
	UT_ASSERT(copy_state.line_buf_valid);
	return true;
}

UT_TEST(shared_copy_returns_all_clear_wal_and_tids_before_first_index)
{
	for (int count = 1; count <= 2; count++) {
		for (int bit = 0; bit < 2; bit++) {
			prepare_copy(count, true, true, true, true);
			pages[1].data[SizeOfPageHeaderData] = bit ? 3 : 0;
			UT_ASSERT(try_copy());
			UT_ASSERT_EQ(copy_indexes, count);
			UT_ASSERT_EQ(copy_afters, count);
		}
	}
}
UT_TEST(copy_vm_retries_and_pin_replacement_do_not_publish_early)
{
	prepare_copy(2, true, true, true, true);
	multi_refusals = multi_replacements = 3;
	multi_resume_refusals = 2;
	UT_ASSERT(try_copy());
	UT_ASSERT_EQ(copy_indexes, 2);
	UT_ASSERT_EQ(next_token, 102);
}
UT_TEST(copy_retarget_publishes_only_the_actual_new_heap_tid)
{
	prepare_copy(1, true, true, true, true);
	copy_expected_heap[0] = 3;
	multi_refusals = multi_replacements = 1; multi_change = 2;
	UT_ASSERT(try_copy());
	UT_ASSERT_EQ(copy_indexes, 1);
	UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 3);
}
UT_TEST(second_row_refusal_error_or_cancel_publishes_no_indexes_or_progress)
{
	for (int fault = 0; fault < 3; fault++) {
		prepare_copy(2, true, true, true, true);
		copy_fail_row = 1; copy_fail_kind = fault;
		UT_ASSERT(!try_copy());
		UT_ASSERT_EQ(copy_completed, 1);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(copy_indexes + copy_afters + copy_cleared + copy_progress, 0);
		UT_ASSERT_EQ(copy_processed, 7);
		UT_ASSERT_EQ(copy_buffer.nused, 2);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&copy_slots[1].tts_tid), 999);
	}
}
UT_TEST(shared_copy_freeze_still_refuses_before_materialization_or_lease)
{
	prepare_copy(2, true, true, true, true);
	copy_info.ti_options = HEAP_INSERT_FROZEN;
	UT_ASSERT(!try_copy());
	UT_ASSERT_EQ(copy_fetches + copy_preflights + copy_leases, 0);
	UT_ASSERT_EQ(inserts + copy_indexes + copy_afters + copy_progress, 0);
}
UT_TEST(native_batch_and_no_visible_hint_keep_original_publication_order)
{
	for (int mode = 0; mode < 3; mode++) {
		prepare_copy(1, mode != 0, mode == 2, mode != 2, true);
		UT_ASSERT(try_copy());
		UT_ASSERT_EQ(copy_indexes, 1);
		UT_ASSERT_EQ(copy_afters, 1);
		if (mode == 0)
			UT_ASSERT_EQ(multi_aux_calls + multi_captures + multi_resumes, 0);
	}
}
UT_TEST(no_index_copy_runs_only_requested_triggers_after_heap_completion)
{
	for (int trigger = 0; trigger < 2; trigger++) {
		prepare_copy(2, true, true, true, false);
		memset(&copy_triggers, 0, sizeof(copy_triggers));
		copy_triggers.trig_insert_after_row = true;
		copy_result.ri_TrigDesc = trigger ? &copy_triggers : NULL;
		UT_ASSERT(try_copy());
		UT_ASSERT_EQ(copy_indexes, 0);
		UT_ASSERT_EQ(copy_afters, trigger ? 2 : 0);
	}
}
UT_TEST(index_error_keeps_remaining_rows_and_progress_unpublished)
{
	prepare_copy(2, true, true, true, true);
	copy_index_error = true;
	UT_ASSERT(!try_copy());
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(copy_indexes, 1);
	UT_ASSERT_EQ(copy_afters + copy_cleared + copy_progress, 0);
	UT_ASSERT_EQ(copy_buffer.nused, 2);
	UT_ASSERT_EQ(copy_processed, 7);
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(shared_copy_returns_all_clear_wal_and_tids_before_first_index);
	UT_RUN(copy_vm_retries_and_pin_replacement_do_not_publish_early);
	UT_RUN(copy_retarget_publishes_only_the_actual_new_heap_tid);
	UT_RUN(second_row_refusal_error_or_cancel_publishes_no_indexes_or_progress);
	UT_RUN(shared_copy_freeze_still_refuses_before_materialization_or_lease);
	UT_RUN(native_batch_and_no_visible_hint_keep_original_publication_order);
	UT_RUN(no_index_copy_runs_only_requested_triggers_after_heap_completion);
	UT_RUN(index_error_keeps_remaining_rows_and_progress_unpublished);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
