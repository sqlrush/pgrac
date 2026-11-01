/*-------------------------------------------------------------------------
 * test_cluster_heap_speculative_index.c
 *    Speculative INSERT clears VM before indexes, completion and wakeup.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The original speculative executor block and both heap AM wrappers run
 * with the existing acquisition/capture/clear/WAL fixture. Conflict results,
 * lock management, physical finish/abort, UPDATE delegation and the old
 * transport/receipt/ITL leaves remain controlled. This is not full INSERT,
 * actual row rollback, backend ERROR cleanup, commit or durability.
 *-------------------------------------------------------------------------
 */
int speculative_inherited_main(void);
#define INSERT_INDEX_MAIN speculative_inherited_main
#include "test_cluster_heap_insert_index.c"
#undef INSERT_INDEX_MAIN

static unsigned spec_checks, spec_attempts, spec_locks, spec_releases;
static unsigned spec_completions, spec_aborts, spec_confirms, spec_frees;
static unsigned spec_update_calls, spec_visible_checks;
static unsigned spec_conflicts, spec_precheck_mode;
static uint32 spec_token;
static bool spec_held, spec_index_error;
static OnConflictAction spec_action;
static TupleTableSlot spec_returning_slot;
static TupleTableSlot *spec_result;
static ModifyTableState spec_mtstate;

static void
spec_heap_insert(Relation relation, HeapTuple value, CommandId cid,
				 int options, BulkInsertState bistate)
{
	UT_ASSERT(relation == &relation_data && value == &tuple && bistate == NULL);
	UT_ASSERT_EQ(cid, publication_estate.es_output_cid);
	UT_ASSERT_EQ(options, HEAP_INSERT_SPECULATIVE);
	UT_ASSERT(spec_held && !publication_heap_done);
	UT_ASSERT_EQ(spec_attempts, publication_indexes);
	UT_ASSERT_EQ(spec_completions, spec_attempts);
	UT_ASSERT_EQ(value->t_tableOid, relation_data.rd_id);
	UT_ASSERT(HeapTupleHeaderIsSpeculative(value->t_data));
	if (HeapTupleHeaderIsSpeculative(value->t_data))
		UT_ASSERT_EQ(HeapTupleHeaderGetSpeculativeToken(value->t_data), spec_token);
	spec_attempts++;
	execute_single_wal_chain_with_options(publication_shared, options);
	publication_heap_done = true;
}

static void
spec_finish(Relation relation, ItemPointer tid)
{
	UT_ASSERT(relation == &relation_data && tid == &publication_slot.tts_tid);
	UT_ASSERT(publication_heap_done && spec_held);
	UT_ASSERT_EQ(publication_indexes, spec_attempts);
	UT_ASSERT_EQ(spec_completions + 1, spec_attempts);
	spec_completions++;
	spec_confirms++;
}

static void
spec_abort(Relation relation, ItemPointer tid)
{
	spec_finish(relation, tid);
	spec_confirms--;
	spec_aborts++;
}

#define ExecFetchSlotHeapTuple publication_fetch
#define pfree publication_free
#define heap_insert spec_heap_insert
#define heap_finish_speculative spec_finish
#define heap_abort_speculative spec_abort
#include "test_cluster_heap_speculative_am.inc"
#undef heap_abort_speculative
#undef heap_finish_speculative
#undef heap_insert
#undef pfree
#undef ExecFetchSlotHeapTuple

static bool
spec_check_constraints(ResultRelInfo *result, TupleTableSlot *slot, EState *estate,
					   ItemPointer conflict, List *arbiters)
{
	UT_ASSERT(result == &publication_result && slot == &publication_slot);
	UT_ASSERT(estate == &publication_estate && arbiters == NIL && !spec_held);
	spec_checks++;
	ItemPointerSet(conflict, 123, 1);
	return spec_precheck_mode == 0 || (spec_precheck_mode == 3 && spec_checks > 1);
}

static bool
spec_update(void *context, ResultRelInfo *result, ItemPointer conflict,
			TupleTableSlot *slot, bool canSetTag, TupleTableSlot **returning)
{
	UT_ASSERT(context == NULL && result == &publication_result);
	UT_ASSERT(slot == &publication_slot && !canSetTag && !spec_held);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(conflict), 123);
	UT_ASSERT_EQ(inserts + publication_indexes, 0);
	spec_update_calls++;
	*returning = &spec_returning_slot;
	return spec_precheck_mode == 2;
}

static TupleTableSlot *
spec_returning(EState *estate, ResultRelInfo *result)
{
	UT_ASSERT(estate == &publication_estate && result == &publication_result);
	return &spec_returning_slot;
}

static void
spec_tid_visible(EState *estate, ResultRelInfo *result, ItemPointer tid,
				 TupleTableSlot *slot)
{
	UT_ASSERT(estate == &publication_estate && result == &publication_result);
	UT_ASSERT(slot == &spec_returning_slot && !spec_held);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(tid), 123);
	UT_ASSERT_EQ(inserts + publication_indexes, 0);
	spec_visible_checks++;
}

static uint32
spec_acquire(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 500);
	UT_ASSERT(!spec_held && !multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	spec_held = true;
	spec_token = 1000 + ++spec_locks;
	return spec_token;
}

static List *
spec_index(ResultRelInfo *result, TupleTableSlot *slot, EState *estate,
		   bool update, bool noDupErr, bool *conflict, List *arbiters,
		   bool onlySummarizing)
{
	Buffer heap = spec_attempts > 1 ? 3 : publication_heap;
	Buffer map = heap + 1;

	UT_ASSERT(result == &publication_result && slot == &publication_slot);
	UT_ASSERT(estate == &publication_estate && !update && noDupErr);
	UT_ASSERT(conflict != NULL && arbiters == NIL && !onlySummarizing);
	UT_ASSERT(publication_heap_done && spec_held);
	UT_ASSERT_EQ(spec_attempts, publication_indexes + 1);
	UT_ASSERT_EQ(spec_completions, publication_indexes);
	UT_ASSERT_EQ(inserts, spec_attempts);
	UT_ASSERT_EQ(vm_clears, publication_visible ? spec_attempts : 0);
	UT_ASSERT_EQ(slot->tts_tableOid, 4242);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), blocks[heap - 1]);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), 1);
	UT_ASSERT((heap_flags & XLH_INSERT_IS_SPECULATIVE) != 0);
	UT_ASSERT(HeapTupleHeaderIsSpeculative(tuple.t_data));
	if (HeapTupleHeaderIsSpeculative(tuple.t_data))
		UT_ASSERT_EQ(HeapTupleHeaderGetSpeculativeToken(tuple.t_data), spec_token);
	UT_ASSERT(!multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!PageIsAllVisible(pages[heap - 1].data));
	UT_ASSERT_EQ(PageGetLSN(pages[heap - 1].data), 200);
	if (publication_visible)
		UT_ASSERT_EQ(pages[map - 1].data[SizeOfPageHeaderData] & 3, 0);
	if (publication_shared) {
		UT_ASSERT_EQ(edge_count, publication_visible ? 2 : 1);
		UT_ASSERT_EQ(((PageHeader)images[0].data)->pd_block_scn, 100 + spec_attempts);
		if (publication_visible) {
			UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE);
			UT_ASSERT_EQ(registered_buffers[1], map);
			UT_ASSERT_EQ(images[1].data[SizeOfPageHeaderData] & 3, 0);
			UT_ASSERT_EQ(((PageHeader)images[1].data)->pd_block_scn, 100 + spec_attempts);
		}
	}
	publication_indexes++;
	if (spec_index_error)
		elog(ERROR, "controlled speculative index error");
	*conflict = spec_conflicts > 0;
	if (spec_conflicts)
		spec_conflicts--;
	return &publication_rechecks;
}

static void
spec_release(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 500);
	UT_ASSERT(spec_held && publication_heap_done);
	UT_ASSERT_EQ(spec_completions, spec_attempts);
	UT_ASSERT_EQ(spec_releases + 1, spec_completions);
	spec_releases++;
	spec_held = false;
	publication_heap_done = false;
}

static void
spec_list_free(List *list)
{
	UT_ASSERT(list == &publication_rechecks && !spec_held);
	UT_ASSERT_EQ(spec_aborts, spec_frees + 1);
	spec_frees++;
}

#ifndef SPECULATIVE_EXECUTOR_FIXTURE
#define SPECULATIVE_EXECUTOR_FIXTURE "test_cluster_heap_speculative_index.inc"
#endif
static TupleTableSlot *
execute_speculative_insert(void)
{
	Relation resultRelationDesc = &relation_data;
	ResultRelInfo *resultRelInfo = &publication_result;
	TupleTableSlot *slot = &publication_slot;
	EState *estate = &publication_estate;
	ModifyTableState *mtstate = &spec_mtstate;
	void *context = NULL;
	bool canSetTag = false;
	OnConflictAction onconflict = spec_action;
	List *recheckIndexes = NIL;

#define ExecCheckIndexConstraints spec_check_constraints
#define ExecOnConflictUpdate spec_update
#define ExecGetReturningSlot spec_returning
#define ExecCheckTIDVisible spec_tid_visible
#define GetCurrentTransactionId() ((TransactionId)500)
#define SpeculativeInsertionLockAcquire spec_acquire
#define SpeculativeInsertionLockRelease spec_release
#define table_tuple_insert_speculative heapam_tuple_insert_speculative
#define table_tuple_complete_speculative heapam_tuple_complete_speculative
#define ExecInsertIndexTuples spec_index
#define list_free spec_list_free
#include SPECULATIVE_EXECUTOR_FIXTURE
#undef list_free
#undef ExecInsertIndexTuples
#undef table_tuple_complete_speculative
#undef table_tuple_insert_speculative
#undef SpeculativeInsertionLockRelease
#undef SpeculativeInsertionLockAcquire
#undef GetCurrentTransactionId
#undef ExecCheckTIDVisible
#undef ExecGetReturningSlot
#undef ExecOnConflictUpdate
#undef ExecCheckIndexConstraints
	UT_ASSERT(recheckIndexes == &publication_rechecks);
	return slot;
}

static bool
try_speculative_insert(void)
{
	if (sigsetjmp(multi_jump, 0) != 0)
		return false;
	spec_result = execute_speculative_insert();
	return true;
}

static void
prepare_speculative(bool shared, bool visible)
{
	prepare_publication(shared, visible, true);
	memset(&spec_mtstate, 0, sizeof(spec_mtstate));
	spec_checks = spec_attempts = spec_locks = spec_releases = 0;
	spec_completions = spec_aborts = spec_confirms = spec_frees = 0;
	spec_update_calls = spec_visible_checks = spec_conflicts = spec_precheck_mode = 0;
	spec_token = 0;
	spec_held = spec_index_error = false;
	spec_action = ONCONFLICT_NOTHING;
	spec_result = NULL;
}

UT_TEST(speculative_clear_wal_and_tid_precede_index_confirm_and_wakeup)
{
	for (int bit = 0; bit < 2; bit++) {
		prepare_speculative(true, true);
		pages[1].data[SizeOfPageHeaderData] = bit ? 3 : 0;
		UT_ASSERT(try_speculative_insert());
		UT_ASSERT(spec_result == &publication_slot && !spec_held);
		UT_ASSERT_EQ(publication_indexes, 1);
		UT_ASSERT_EQ(spec_confirms, 1);
		UT_ASSERT_EQ(spec_releases, 1);
	}
}
UT_TEST(vm_refusals_and_pin_replacements_never_publish_speculative_index_early)
{
	prepare_speculative(true, true);
	multi_refusals = multi_replacements = 3; multi_resume_refusals = 2;
	UT_ASSERT(try_speculative_insert());
	UT_ASSERT_EQ(publication_indexes + spec_confirms + spec_releases, 3);
	UT_ASSERT_EQ(next_token, 101);
}
UT_TEST(index_conflict_completes_abort_and_releases_before_original_retry)
{
	prepare_speculative(true, true);
	spec_conflicts = 1;
	UT_ASSERT(try_speculative_insert());
	UT_ASSERT_EQ(spec_checks, 2);
	UT_ASSERT_EQ(spec_attempts, 2);
	UT_ASSERT_EQ(publication_indexes, 2);
	UT_ASSERT_EQ(spec_aborts, 1);
	UT_ASSERT_EQ(spec_confirms, 1);
	UT_ASSERT_EQ(spec_releases, 2);
	UT_ASSERT_EQ(spec_frees, 1);
	UT_ASSERT_EQ(next_token, 102);
}
UT_TEST(committed_conflict_takes_visibility_or_update_without_speculative_heap_write)
{
	for (int mode = 1; mode <= 2; mode++) {
		prepare_speculative(true, true);
		spec_precheck_mode = mode;
		spec_action = mode == 1 ? ONCONFLICT_NOTHING : ONCONFLICT_UPDATE;
		UT_ASSERT(try_speculative_insert());
		UT_ASSERT_EQ(inserts + publication_indexes + spec_locks + spec_completions, 0);
		UT_ASSERT_EQ(spec_visible_checks, mode == 1 ? 1 : 0);
		UT_ASSERT_EQ(spec_update_calls, mode == 2 ? 1 : 0);
		UT_ASSERT(spec_result == (mode == 2 ? &spec_returning_slot : NULL));
	}
}
UT_TEST(update_conflict_retry_rechecks_before_any_speculative_publication)
{
	prepare_speculative(true, true);
	spec_precheck_mode = 3; spec_action = ONCONFLICT_UPDATE;
	UT_ASSERT(try_speculative_insert());
	UT_ASSERT_EQ(spec_update_calls, 1);
	UT_ASSERT_EQ(spec_checks, 2);
	UT_ASSERT_EQ(publication_indexes, 1);
	UT_ASSERT_EQ(spec_releases, 1);
}
UT_TEST(unprovable_writer_or_cancel_never_enters_index_completion_or_wakeup)
{
	for (int cancel = 0; cancel < 2; cancel++) {
		prepare_speculative(true, true);
		multi_missing_writer = !cancel;
		if (cancel)
			multi_refusals = multi_replacements = multi_fault = 1;
		UT_ASSERT(!try_speculative_insert());
		UT_ASSERT_EQ(inserts + publication_indexes + spec_completions + spec_releases, 0);
		UT_ASSERT(spec_held);
	}
}
UT_TEST(index_error_propagates_without_fabricating_completion_or_wakeup)
{
	prepare_speculative(true, true);
	spec_index_error = true;
	UT_ASSERT(!try_speculative_insert());
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(publication_indexes, 1);
	UT_ASSERT_EQ(spec_completions + spec_releases, 0);
	UT_ASSERT(spec_held);
}
UT_TEST(native_and_no_visible_hint_preserve_speculative_token_and_wal_option)
{
	for (int shared = 0; shared < 2; shared++) {
		for (int visible = 0; visible < 2; visible++) {
			prepare_speculative(shared, visible);
			UT_ASSERT(try_speculative_insert());
			UT_ASSERT_EQ(publication_indexes, 1);
			UT_ASSERT_EQ(spec_confirms, 1);
			UT_ASSERT_EQ(spec_releases, 1);
		}
	}
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(speculative_clear_wal_and_tid_precede_index_confirm_and_wakeup);
	UT_RUN(vm_refusals_and_pin_replacements_never_publish_speculative_index_early);
	UT_RUN(index_conflict_completes_abort_and_releases_before_original_retry);
	UT_RUN(committed_conflict_takes_visibility_or_update_without_speculative_heap_write);
	UT_RUN(update_conflict_retry_rechecks_before_any_speculative_publication);
	UT_RUN(unprovable_writer_or_cancel_never_enters_index_completion_or_wakeup);
	UT_RUN(index_error_propagates_without_fabricating_completion_or_wakeup);
	UT_RUN(native_and_no_visible_hint_preserve_speculative_token_and_wal_option);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
