/*-------------------------------------------------------------------------
 * test_cluster_heap_insert_index.c
 *    Ordinary INSERT publishes index entries after VM clear and WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The original executor block and complete heap AM wrapper execute against
 * the existing VM acquisition/capture/mutation/WAL fixture. Tuple fetching,
 * table AM dispatch and index AM are controlled leaves. This is not a full
 * executor/heap_insert, SQL, commit or remote ACK/durability test.
 *-------------------------------------------------------------------------
 */
int insert_index_inherited_main(void);
#define INSERT_VM_WAL_MAIN insert_index_inherited_main
#include "test_cluster_heap_insert_vm_wal_chain.c"
#undef INSERT_VM_WAL_MAIN

#include "executor/executor.h"

static TupleTableSlot publication_slot;
static ResultRelInfo publication_result;
static EState publication_estate;
static List publication_rechecks;
static bool publication_shared, publication_visible, publication_should_free;
static bool publication_heap_done;
static Buffer publication_heap, publication_map;
static unsigned publication_fetches, publication_frees;
static unsigned publication_tables, publication_heaps, publication_indexes;

static HeapTuple
publication_fetch(TupleTableSlot *slot, bool materialize, bool *shouldFree)
{
	UT_ASSERT(slot == &publication_slot && materialize);
	UT_ASSERT(shouldFree != NULL && *shouldFree);
	publication_fetches++;
	*shouldFree = publication_should_free;
	return &tuple;
}

static void
publication_free(void *value)
{
	UT_ASSERT(value == &tuple && publication_heap_done);
	UT_ASSERT(memcmp(&tuple.t_self, &publication_slot.tts_tid,
					 sizeof(ItemPointerData)) == 0);
	publication_frees++;
}

static void
publication_heap_insert(Relation relation, HeapTuple value, CommandId cid,
						int options, BulkInsertState bistate)
{
	UT_ASSERT(relation == &relation_data && value == &tuple);
	UT_ASSERT_EQ(cid, publication_estate.es_output_cid);
	UT_ASSERT_EQ(options, 0);
	UT_ASSERT(bistate == NULL && !publication_heap_done);
	UT_ASSERT_EQ(publication_indexes, 0);
	UT_ASSERT_EQ(publication_slot.tts_tableOid, relation_data.rd_id);
	UT_ASSERT_EQ(tuple.t_tableOid, relation_data.rd_id);
	publication_heaps++;
	execute_single_wal_chain(publication_shared);
	publication_heap_done = true;
}

#define ExecFetchSlotHeapTuple publication_fetch
#define heap_insert publication_heap_insert
#define pfree publication_free
#include "test_cluster_heap_insert_am.inc"
#undef pfree
#undef heap_insert
#undef ExecFetchSlotHeapTuple

static void
publication_table_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
						 int options, BulkInsertState bistate)
{
	UT_ASSERT(relation == &relation_data && slot == &publication_slot);
	UT_ASSERT_EQ(cid, publication_estate.es_output_cid);
	UT_ASSERT_EQ(options, 0);
	UT_ASSERT(bistate == NULL);
	publication_tables++;
	heapam_tuple_insert(relation, slot, cid, options, bistate);
}

static void
check_index_ready(void)
{
	UT_ASSERT(publication_heap_done);
	UT_ASSERT_EQ(publication_tables, 1);
	UT_ASSERT_EQ(publication_heaps, 1);
	UT_ASSERT_EQ(publication_fetches, 1);
	UT_ASSERT_EQ(publication_frees, publication_should_free ? 1 : 0);
	UT_ASSERT_EQ(publication_slot.tts_tableOid, relation_data.rd_id);
	UT_ASSERT(memcmp(&tuple.t_self, &publication_slot.tts_tid,
					 sizeof(ItemPointerData)) == 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&publication_slot.tts_tid),
				 blocks[publication_heap - 1]);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&publication_slot.tts_tid), 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!PageIsAllVisible(pages[publication_heap - 1].data));
	UT_ASSERT_EQ(PageGetLSN(pages[publication_heap - 1].data), 200);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(images[0].data), 1);
	if (publication_shared && publication_visible)
		check_clear_publication(publication_heap, publication_map);
	else {
		UT_ASSERT_EQ(vm_clears, publication_visible ? 1 : 0);
		UT_ASSERT_EQ(edge_count, publication_shared ? 1 : 0);
		UT_ASSERT(!registered[1]);
		UT_ASSERT_EQ(multi_aux_calls + multi_captures + multi_resumes, 0);
		if (publication_visible)
			UT_ASSERT_EQ(pages[publication_map - 1].data[SizeOfPageHeaderData] & 3, 0);
		else
			UT_ASSERT_EQ(pages[publication_map - 1].data[SizeOfPageHeaderData] & 3, 3);
	}
}

static List *
publication_index_insert(ResultRelInfo *result, TupleTableSlot *slot,
						 EState *estate, bool update, bool noDupErr,
						 bool *specConflict, List *arbiterIndexes,
						 bool onlySummarizing)
{
	UT_ASSERT(result == &publication_result && slot == &publication_slot);
	UT_ASSERT(estate == &publication_estate && result->ri_NumIndices > 0);
	UT_ASSERT(!update && !noDupErr && !onlySummarizing);
	UT_ASSERT(specConflict == NULL && arbiterIndexes == NIL);
	check_index_ready();
	publication_indexes++;
	return &publication_rechecks;
}

#ifndef INSERT_EXECUTOR_FIXTURE
#define INSERT_EXECUTOR_FIXTURE "test_cluster_heap_insert_index.inc"
#endif
static void
execute_ordinary_insert(void)
{
	Relation resultRelationDesc = &relation_data;
	TupleTableSlot *slot = &publication_slot;
	ResultRelInfo *resultRelInfo = &publication_result;
	EState *estate = &publication_estate;
	List *recheckIndexes = NIL;

#define table_tuple_insert publication_table_insert
#define ExecInsertIndexTuples publication_index_insert
#include INSERT_EXECUTOR_FIXTURE
#undef ExecInsertIndexTuples
#undef table_tuple_insert
	UT_ASSERT(recheckIndexes == (resultRelInfo->ri_NumIndices > 0
								 ? &publication_rechecks : NIL));
}

static bool
try_ordinary_insert(void)
{
	if (sigsetjmp(multi_jump, 0) != 0)
		return false;
	execute_ordinary_insert();
	return true;
}

static void
prepare_publication(bool shared, bool visible, bool indexed)
{
	prepare_wal_chain(shared, visible);
	memset(&publication_slot, 0, sizeof(publication_slot));
	memset(&publication_result, 0, sizeof(publication_result));
	memset(&publication_estate, 0, sizeof(publication_estate));
	publication_shared = shared;
	publication_visible = visible;
	publication_should_free = publication_heap_done = false;
	publication_heap = 1; publication_map = 2;
	publication_fetches = publication_frees = 0;
	publication_tables = publication_heaps = publication_indexes = 0;
	relation_data.rd_id = 4242;
	publication_estate.es_output_cid = 73;
	publication_result.ri_NumIndices = indexed ? 1 : 0;
	/* A valid but stale TID makes missing/late slot publication observable. */
	ItemPointerSet(&publication_slot.tts_tid, 999, 7);
	publication_slot.tts_tableOid = 999;
}

UT_TEST(index_callback_observes_shared_clear_wal_and_slot_publication)
{
	for (int bit = 0; bit < 2; bit++) {
		prepare_publication(true, true, true);
		pages[1].data[SizeOfPageHeaderData] = bit ? 3 : 0;
		UT_ASSERT(try_ordinary_insert());
		UT_ASSERT_EQ(publication_indexes, 1);
	}
}
UT_TEST(retries_and_pin_replacement_publish_only_the_final_insert)
{
	for (int replacement = 0; replacement < 2; replacement++) {
		prepare_publication(true, true, true);
		multi_refusals = 3; multi_replacements = replacement ? 3 : 0;
		multi_resume_refusals = 2;
		UT_ASSERT(try_ordinary_insert());
		UT_ASSERT_EQ(publication_indexes, 1);
		UT_ASSERT_EQ(multi_aux_calls, 6);
		UT_ASSERT_EQ(multi_context_starts, 3);
		UT_ASSERT_EQ(next_token, 101);
	}
}
UT_TEST(repicked_heap_tid_and_vm_are_ready_at_index_callback)
{
	prepare_publication(true, true, true);
	publication_heap = 3; publication_map = 4;
	multi_refusals = multi_replacements = 1; multi_change = 2;
	UT_ASSERT(try_ordinary_insert());
	UT_ASSERT_EQ(publication_indexes, 1);
	UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 3);
	UT_ASSERT(!dirty[0] && !dirty[1]);
}
UT_TEST(unprovable_writer_and_requalification_error_never_reach_index)
{
	for (int fault = 0; fault < 5; fault++) {
		PGAlignedBlock before[4];

		prepare_publication(true, true, true);
		memcpy(before, pages, sizeof(before));
		multi_missing_writer = fault == 0;
		if (fault == 1 || fault == 2)
			multi_writer_error = fault;
		if (fault >= 3) {
			multi_refusals = multi_replacements = 1;
			multi_change = fault;
		}
		UT_ASSERT(!try_ordinary_insert());
		UT_ASSERT_EQ(publication_indexes, 0);
		UT_ASSERT(!publication_heap_done);
		UT_ASSERT_EQ(inserts + vm_clears, 0);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&publication_slot.tts_tid), 999);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	}
}
UT_TEST(cancel_during_reobserve_never_reaches_index)
{
	prepare_publication(true, true, true);
	multi_refusals = multi_replacements = multi_fault = 1;
	UT_ASSERT(!try_ordinary_insert());
	UT_ASSERT_EQ(publication_indexes, 0);
	UT_ASSERT_EQ(inserts + vm_clears, 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&publication_slot.tts_tid), 999);
	UT_ASSERT(!multi_heap_locked && !multi_vm_locked && !multi_vm_pins[1]);
}
UT_TEST(native_and_no_visible_hint_keep_their_original_index_order)
{
	for (int shared = 0; shared < 2; shared++) {
		for (int visible = 0; visible < 2; visible++) {
			prepare_publication(shared, visible, true);
			UT_ASSERT(try_ordinary_insert());
			UT_ASSERT_EQ(publication_indexes, 1);
		}
	}
}
UT_TEST(no_index_relation_still_clears_and_publishes_the_slot)
{
	prepare_publication(true, true, false);
	UT_ASSERT(try_ordinary_insert());
	UT_ASSERT_EQ(publication_indexes, 0);
	check_index_ready();
}
UT_TEST(owned_tuple_is_freed_after_tid_copy_and_before_index)
{
	prepare_publication(true, true, true);
	publication_should_free = true;
	UT_ASSERT(try_ordinary_insert());
	UT_ASSERT_EQ(publication_indexes, 1);
	UT_ASSERT_EQ(publication_frees, 1);
}
#ifndef INSERT_INDEX_MAIN
#define INSERT_INDEX_MAIN main
#endif
int
INSERT_INDEX_MAIN(void)
{
	UT_PLAN(8);
	UT_RUN(index_callback_observes_shared_clear_wal_and_slot_publication);
	UT_RUN(retries_and_pin_replacement_publish_only_the_final_insert);
	UT_RUN(repicked_heap_tid_and_vm_are_ready_at_index_callback);
	UT_RUN(unprovable_writer_and_requalification_error_never_reach_index);
	UT_RUN(cancel_during_reobserve_never_reaches_index);
	UT_RUN(native_and_no_visible_hint_keep_their_original_index_order);
	UT_RUN(no_index_relation_still_clears_and_publishes_the_slot);
	UT_RUN(owned_tuple_is_freed_after_tid_copy_and_before_index);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
