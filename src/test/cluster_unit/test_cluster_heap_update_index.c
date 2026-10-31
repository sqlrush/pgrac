/*-------------------------------------------------------------------------
 * test_cluster_heap_update_index.c
 *    Original UPDATE executor publishes indexes after heap/VM WAL returns.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Complete executor, preparation, action, epilogue and heap AM run through
 * the real table AM inline to the existing DML VM acquisition/clear/WAL
 * fixture. Heap scheduling and classifiers, materialization, BEFORE/AFTER,
 * index, RETURNING and trace leaves are controlled. Unsupported EPQ,
 * partition and FDW branches abort; this is not SQL, full heap_update,
 * commit, native ResourceOwner, remote ACK or durability.
 *-------------------------------------------------------------------------
 */
int update_index_inherited_main(void);
#define DML_VM_WAL_MAIN update_index_inherited_main
#include "test_cluster_heap_dml_vm_wal_chain.c"
#undef DML_VM_WAL_MAIN

#include "access/tableam.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "cluster/cluster_xnode_profile.h"
#include "test_cluster_heap_update_context.inc"

static ModifyTableContext up_context;
static ModifyTableState up_state;
static EState up_estate;
static ResultRelInfo up_result;
static TupleTableSlot up_slot, up_return_slot;
static struct TupleDescData up_desc;
static TableAmRoutine up_am;
static SnapshotData up_snapshot, up_crosscheck;
static ItemPointerData up_old_tid;
static List up_rechecks, up_options;
static bool up_shared, up_crosspage, up_hot, up_done, up_free_tuple;
static bool up_before, up_xact_snapshot, up_can_tag, up_bad_cmax;
static int up_vm_mode, up_error;
static TM_Result up_status;
static TU_UpdateIndexes up_indexes;
static unsigned up_prologues, up_materializes, up_fetches, up_frees;
static unsigned up_tables, up_heaps, up_index_calls, up_afters, up_returns;
static unsigned up_rls, up_view, up_list_frees, up_trace_begins, up_trace_ends;

static bool
up_ready(void)
{
	UT_ASSERT(up_done);
	if (!up_done)
		return false;
	UT_ASSERT_EQ(up_tables, 1);
	UT_ASSERT_EQ(up_heaps, 1);
	UT_ASSERT_EQ(up_fetches, 1);
	UT_ASSERT_EQ(up_frees, up_free_tuple ? 1 : 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!chain_heap && !multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!chain_held[1] && !chain_held[3]);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(up_slot.tts_tableOid, relation_data.rd_id);
	UT_ASSERT(memcmp(&tuple.t_self, &up_slot.tts_tid, sizeof(ItemPointerData)) == 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&up_slot.tts_tid),
				 blocks[up_crosspage ? 2 : 0]);
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	if (up_crosspage)
		UT_ASSERT(!PageIsAllVisible(pages[2].data));
	if (up_vm_mode == 1 || up_vm_mode == 2)
		UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData] & 3, 0);
	if (up_vm_mode >= 2)
		UT_ASSERT_EQ(pages[3].data[SizeOfPageHeaderData] & 3, 0);
	if (up_shared)
	{
		unsigned count = (up_crosspage ? 2 : 1) +
			(up_vm_mode == 0 ? 0 : up_vm_mode == 2 ? 2 : 1);

		check_wal(count, next_token);
	}
	else
	{
		UT_ASSERT_EQ(edge_count, 0);
		/* Native code locks both VM buffers through its original local path. */
		UT_ASSERT_EQ(chain_warms, 2);
		UT_ASSERT_EQ(chain_captures + chain_resumes, 0);
	}
	return true;
}

static HeapTuple
up_fetch(TupleTableSlot *slot, bool materialize, bool *should_free)
{
	UT_ASSERT(slot == &up_slot && materialize && should_free && *should_free);
	UT_ASSERT_EQ(up_materializes, 1);
	up_fetches++;
	*should_free = up_free_tuple;
	return &tuple;
}

static void
up_free(void *value)
{
	UT_ASSERT(value == &tuple && up_done);
	UT_ASSERT(memcmp(&tuple.t_self, &up_slot.tts_tid, sizeof(ItemPointerData)) == 0);
	up_frees++;
}

static TM_Result
up_heap_update(Relation relation, ItemPointer tid, HeapTuple value, CommandId cid,
			   Snapshot crosscheck, bool wait, TM_FailureData *failure,
			   LockTupleMode *lockmode, TU_UpdateIndexes *indexes)
{
	UT_ASSERT(relation == &relation_data && tid == &up_old_tid && value == &tuple);
	UT_ASSERT_EQ(cid, up_estate.es_output_cid);
	UT_ASSERT(crosscheck == &up_crosscheck && wait && failure == &up_context.tmfd);
	UT_ASSERT_EQ(up_index_calls + up_afters + up_returns, 0);
	UT_ASSERT_EQ(up_slot.tts_tableOid, relation_data.rd_id);
	UT_ASSERT_EQ(value->t_tableOid, relation_data.rd_id);
	up_heaps++;
	*lockmode = LockTupleExclusive;
	*indexes = TU_None;
	failure->cmax = up_estate.es_output_cid + (up_bad_cmax ? 1 : 0);
	if (up_status != TM_Ok)
		return up_status;
	execute_chain(CHAIN_UPDATE, up_shared, up_crosspage, up_vm_mode, up_hot, false);
	*indexes = up_indexes;
	up_done = true;
	return TM_Ok;
}

#define ExecFetchSlotHeapTuple up_fetch
#define heap_update up_heap_update
#define pfree up_free
#ifndef UPDATE_INDEX_AM_FIXTURE
#define UPDATE_INDEX_AM_FIXTURE "test_cluster_heap_update_am.inc"
#endif
#include UPDATE_INDEX_AM_FIXTURE
#undef pfree
#undef heap_update
#undef ExecFetchSlotHeapTuple

static TM_Result
up_table_update(Relation relation, ItemPointer tid, TupleTableSlot *slot,
				CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *failure, LockTupleMode *lockmode, TU_UpdateIndexes *indexes)
{
	UT_ASSERT(slot == &up_slot && snapshot == &up_snapshot);
	UT_ASSERT_EQ(up_prologues, 1);
	up_tables++;
	return heapam_tuple_update(relation, tid, slot, cid, snapshot, crosscheck,
							   wait, failure, lockmode, indexes);
}

static bool
up_prologue(ModifyTableContext *context, ResultRelInfo *result, ItemPointer tid,
			HeapTuple oldtuple, TupleTableSlot *slot, TM_Result *status)
{
	UT_ASSERT(context == &up_context && result == &up_result && tid == &up_old_tid);
	UT_ASSERT(oldtuple == NULL && slot == &up_slot && status == NULL);
	UT_ASSERT_EQ(inserts + up_tables + up_index_calls + up_afters + up_returns, 0);
	up_prologues++;
	return up_before;
}

static void
up_materialize(TupleTableSlot *slot)
{
	UT_ASSERT(slot == &up_slot && !up_done);
	UT_ASSERT_EQ(slot->tts_tableOid, relation_data.rd_id);
	up_materializes++;
}

static List *
up_index_insert(ResultRelInfo *result, TupleTableSlot *slot, EState *estate,
				bool update, bool no_duplicate_error, bool *conflict,
				List *arbiters, bool only_summarizing)
{
	UT_ASSERT(result == &up_result && slot == &up_slot && estate == &up_estate);
	UT_ASSERT(update && !no_duplicate_error && conflict == NULL && arbiters == NIL);
	UT_ASSERT_EQ(only_summarizing, up_indexes == TU_Summarizing);
	up_ready();
	UT_ASSERT_EQ(up_estate.es_processed, up_can_tag ? 1 : 0);
	up_index_calls++;
	if (up_error == 1)
		elog(ERROR, "controlled UPDATE index error");
	return &up_rechecks;
}

static void
up_after(EState *estate, ResultRelInfo *result, ResultRelInfo *source,
		 ResultRelInfo *dest, ItemPointer tid, HeapTuple oldtuple,
		 TupleTableSlot *slot, List *rechecks, TransitionCaptureState *capture,
		 bool crosspart)
{
	bool indexed = up_result.ri_NumIndices > 0 && up_indexes != TU_None;

	UT_ASSERT(estate == &up_estate && result == &up_result && tid == &up_old_tid);
	UT_ASSERT(source == NULL && dest == NULL && oldtuple == NULL && !crosspart);
	UT_ASSERT(slot == &up_slot && capture == up_state.mt_transition_capture);
	UT_ASSERT(rechecks == (indexed ? &up_rechecks : NIL));
	up_ready();
	UT_ASSERT_EQ(up_index_calls, indexed ? 1 : 0);
	UT_ASSERT_EQ(up_estate.es_processed, up_can_tag ? 1 : 0);
	up_afters++;
	if (up_error == 2)
		elog(ERROR, "controlled UPDATE AFTER error");
}

static void
up_check(WCOKind kind, ResultRelInfo *result, TupleTableSlot *slot, EState *estate)
{
	UT_ASSERT(result == &up_result && slot == &up_slot && estate == &up_estate);
	if (kind == WCO_RLS_UPDATE_CHECK)
	{
		UT_ASSERT(!up_done && up_tables == 0);
		up_rls++;
	}
	else
	{
		UT_ASSERT_EQ(kind, WCO_VIEW_CHECK);
		up_ready();
		UT_ASSERT_EQ(up_afters, 1);
		up_view++;
	}
}

static void
up_list_free(List *list)
{
	UT_ASSERT(list == (up_index_calls ? &up_rechecks : NIL));
	UT_ASSERT_EQ(up_afters, 1);
	up_list_frees++;
}

static TupleTableSlot *
up_return(ResultRelInfo *result, TupleTableSlot *slot, TupleTableSlot *plan)
{
	UT_ASSERT(result == &up_result && slot == &up_slot && plan == &up_slot);
	up_ready();
	UT_ASSERT_EQ(up_afters, 1);
	UT_ASSERT_EQ(up_list_frees, 1);
	UT_ASSERT_EQ(up_view, up_result.ri_WithCheckOptions != NIL ? 1 : 0);
	up_returns++;
	return &up_return_slot;
}

static void
up_trace_begin(ClusterXpScope *scope, ClusterXnodeBucket bucket)
{
	UT_ASSERT(bucket == CLXP_UPDATE_STORAGE || bucket == CLXP_UPDATE_INDEX);
	scope->active = true;
	up_trace_begins++;
}

static void
up_trace_end(ClusterXpScope *scope)
{
	UT_ASSERT(scope->active);
	scope->active = false;
	up_trace_ends++;
}

static void up_unexpected(const char *branch,...) pg_attribute_noreturn();
static void
up_unexpected(const char *branch,...)
{
	(void)branch;
	abort();
}

/* Branches outside this ordinary-update fixture must never fake success. */
#define ExecComputeStoredGenerated(...) up_unexpected("generated", __VA_ARGS__)
#define ExecConstraints(...) up_unexpected("constraint", __VA_ARGS__)
#define ExecPartitionCheck(...) (up_unexpected("partition check", __VA_ARGS__), false)
#define ExecCrossPartitionUpdate(...) (up_unexpected("partition move", __VA_ARGS__), false)
#define ExecCrossPartitionUpdateForeignKey(...) up_unexpected("partition FK", __VA_ARGS__)
#define ExecIRUpdateTriggers(...) (up_unexpected("instead trigger", __VA_ARGS__), false)
#define EvalPlanQualSlot(...) (up_unexpected("EPQ slot", __VA_ARGS__), (TupleTableSlot *)NULL)
#define EvalPlanQual(...) (up_unexpected("EPQ", __VA_ARGS__), (TupleTableSlot *)NULL)
#define table_tuple_lock(...) (up_unexpected("tuple lock", __VA_ARGS__), TM_Ok)
#define ExecInitUpdateProjection(...) up_unexpected("projection", __VA_ARGS__)
#define UnlockTuple(...) up_unexpected("tuple unlock", __VA_ARGS__)
#define LockTuple(...) up_unexpected("tuple lock tag", __VA_ARGS__)
#define table_tuple_fetch_row_version(...) (up_unexpected("row fetch", __VA_ARGS__), false)
#define ExecGetUpdateNewTuple(...) (up_unexpected("EPQ new tuple", __VA_ARGS__), (TupleTableSlot *)NULL)
#undef IsBootstrapProcessingMode
#define IsBootstrapProcessingMode() false
#undef IsolationUsesXactSnapshot
#define IsolationUsesXactSnapshot() up_xact_snapshot
#define ExecUpdatePrologue up_prologue
#define ExecMaterializeSlot up_materialize
#define ExecInsertIndexTuples up_index_insert
#define ExecARUpdateTriggers up_after
#define ExecWithCheckOptions up_check
#define list_free up_list_free
#define ExecProcessReturning up_return
#define cluster_xp_begin up_trace_begin
#define cluster_xp_end up_trace_end
#define errhint(...) 0
#ifndef UPDATE_INDEX_EXECUTOR_FIXTURE
#define UPDATE_INDEX_EXECUTOR_FIXTURE "test_cluster_heap_update_executor.inc"
#endif
#include UPDATE_INDEX_EXECUTOR_FIXTURE

static bool
try_update(void)
{
	volatile bool success = false;

	PG_TRY();
	{
		TupleTableSlot *returned = ExecUpdate(&up_context, &up_result, &up_old_tid,
											NULL, &up_slot, up_can_tag);

		UT_ASSERT(returned == (up_returns ? &up_return_slot : NULL));
		success = true;
	}
	PG_CATCH();
	{
		/* No simulated rollback, ResourceOwner cleanup or successful ACK. */
	}
	PG_END_TRY();
	UT_ASSERT_EQ(up_trace_begins, up_trace_ends);
	return success;
}

static void
prepare_update_index(bool shared, bool crosspage, int vm_mode, TU_UpdateIndexes indexes)
{
	prepare_chain(shared, crosspage, vm_mode);
	memset(&up_context, 0, sizeof(up_context));
	memset(&up_state, 0, sizeof(up_state));
	memset(&up_estate, 0, sizeof(up_estate));
	memset(&up_result, 0, sizeof(up_result));
	memset(&up_slot, 0, sizeof(up_slot));
	memset(&up_desc, 0, sizeof(up_desc));
	memset(&up_am, 0, sizeof(up_am));
	up_context.estate = &up_estate;
	up_context.mtstate = &up_state;
	up_context.planSlot = &up_slot;
	up_state.operation = CMD_UPDATE;
	up_estate.es_output_cid = 2;
	up_estate.es_snapshot = &up_snapshot;
	up_estate.es_crosscheck_snapshot = &up_crosscheck;
	up_result.ri_RelationDesc = &relation_data;
	up_result.ri_NumIndices = 1;
	up_result.ri_projectReturning = (ProjectionInfo *)&up_return_slot;
	up_am.tuple_update = up_table_update;
	relation_data.rd_tableam = &up_am;
	relation_data.rd_att = &up_desc;
	relation_data.rd_id = 4242;
	relation_data.rd_rel->relispartition = false;
	ItemPointerSet(&up_old_tid, blocks[0], 1);
	ItemPointerSet(&up_slot.tts_tid, 999, 7);
	up_slot.tts_tableOid = 999;
	up_shared = shared;
	up_crosspage = crosspage;
	up_vm_mode = vm_mode;
	up_indexes = indexes;
	up_hot = indexes != TU_All;
	up_status = TM_Ok;
	up_before = up_can_tag = true;
	up_done = up_free_tuple = up_xact_snapshot = up_bad_cmax = false;
	up_error = 0;
	up_prologues = up_materializes = up_fetches = up_frees = 0;
	up_tables = up_heaps = up_index_calls = up_afters = up_returns = 0;
	up_rls = up_view = up_list_frees = up_trace_begins = up_trace_ends = 0;
}

UT_TEST(plain_update_publishes_index_after_vm_wal_and_new_tid)
{
	for (int crosspage = 0; crosspage < 2; crosspage++)
	{
		prepare_update_index(true, crosspage, 1, TU_All);
		UT_ASSERT(try_update());
		UT_ASSERT_EQ(up_index_calls, 1);
		UT_ASSERT_EQ(up_afters, 1);
		UT_ASSERT_EQ(up_returns, 1);
	}
}

UT_TEST(distinct_vm_and_new_only_vm_are_ready_before_index)
{
	for (int vm = 2; vm <= 3; vm++)
	{
		prepare_update_index(true, true, vm, TU_All);
		UT_ASSERT(try_update());
		UT_ASSERT_EQ(up_index_calls, 1);
		UT_ASSERT_EQ(up_returns, 1);
	}
}

UT_TEST(hot_none_and_summarizing_preserve_original_index_choice)
{
	for (int summary = 0; summary < 2; summary++)
	{
		prepare_update_index(true, false, 1, summary ? TU_Summarizing : TU_None);
		UT_ASSERT(try_update());
		UT_ASSERT_EQ(up_index_calls, summary);
		UT_ASSERT_EQ(up_afters, 1);
		UT_ASSERT_EQ(up_returns, 1);
	}
}

UT_TEST(no_indexes_and_no_visible_hint_preserve_update_and_returning)
{
	prepare_update_index(true, true, 0, TU_All);
	up_result.ri_NumIndices = 0;
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_index_calls, 0);
	UT_ASSERT_EQ(up_afters, 1);
	UT_ASSERT_EQ(up_returns, 1);
}

UT_TEST(nonshared_update_keeps_native_vm_and_wal_shape)
{
	prepare_update_index(false, true, 2, TU_All);
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_index_calls, 1);
	UT_ASSERT_EQ(up_returns, 1);
}

UT_TEST(rejected_original_writer_retries_before_any_publication)
{
	prepare_update_index(true, true, 2, TU_All);
	chain_refusals = 2;
	chain_resume_refusals = 1;
	UT_ASSERT(try_update());
	UT_ASSERT(chain_attempts > 1);
	UT_ASSERT_EQ(up_index_calls, 1);
	UT_ASSERT_EQ(up_returns, 1);
}

UT_TEST(unproven_writer_or_bad_space_never_reaches_update_epilogue)
{
	for (int fault = 0; fault < 3; fault++)
	{
		prepare_update_index(true, true, 2, TU_All);
		chain_missing = fault == 0;
		chain_capture_error = fault == 1;
		if (fault == 2)
			memset(identity.incarnation, 0, sizeof(identity.incarnation));
		UT_ASSERT(!try_update());
		UT_ASSERT_EQ(inserts + up_index_calls + up_afters + up_returns, 0);
		UT_ASSERT_EQ(up_estate.es_processed, 0);
	}
}

UT_TEST(cancel_before_heap_mutation_never_publishes_index_or_returning)
{
	prepare_update_index(true, false, 1, TU_All);
	chain_refusals = 1;
	chain_cancel = true;
	UT_ASSERT(!try_update());
	UT_ASSERT_EQ(inserts + up_index_calls + up_afters + up_returns, 0);
	UT_ASSERT_EQ(up_estate.es_processed, 0);
}

UT_TEST(non_ok_heap_results_do_not_publish_or_tag_a_row)
{
	const TM_Result statuses[] = {TM_SelfModified, TM_Deleted, TM_Updated, TM_Deleted};

	for (unsigned i = 0; i < lengthof(statuses); i++)
	{
		prepare_update_index(true, false, 1, TU_All);
		up_status = statuses[i];
		up_xact_snapshot = i >= 2;
		UT_ASSERT_EQ(try_update(), i < 2);
		UT_ASSERT_EQ(inserts + up_index_calls + up_afters + up_returns, 0);
		UT_ASSERT_EQ(up_estate.es_processed, 0);
	}
}

UT_TEST(before_trigger_stop_keeps_heap_and_all_later_consumers_untouched)
{
	prepare_update_index(true, false, 1, TU_All);
	up_before = false;
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_materializes + up_tables + up_fetches + inserts, 0);
	UT_ASSERT_EQ(up_index_calls + up_afters + up_returns, 0);
}

UT_TEST(materialized_tuple_is_freed_only_after_slot_tid_publication)
{
	prepare_update_index(true, true, 2, TU_All);
	up_free_tuple = true;
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_frees, 1);
	UT_ASSERT_EQ(up_index_calls, 1);
	UT_ASSERT_EQ(up_returns, 1);
}

UT_TEST(index_and_after_errors_preserve_original_later_consumer_order)
{
	for (int error = 1; error <= 2; error++)
	{
		prepare_update_index(true, false, 1, TU_All);
		up_error = error;
		UT_ASSERT(!try_update());
		UT_ASSERT_EQ(inserts, 1); /* ERROR is not a simulated rollback. */
		UT_ASSERT_EQ(up_index_calls, 1);
		UT_ASSERT_EQ(up_afters, error == 2);
		UT_ASSERT_EQ(up_returns, 0);
	}
}

UT_TEST(rls_and_view_checks_stay_on_their_original_sides_of_update)
{
	prepare_update_index(true, false, 1, TU_All);
	up_result.ri_WithCheckOptions = &up_options;
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_rls, 1);
	UT_ASSERT_EQ(up_view, 1);
	UT_ASSERT_EQ(up_returns, 1);
}

UT_TEST(can_set_tag_and_returning_remain_independent)
{
	prepare_update_index(true, false, 1, TU_All);
	up_can_tag = false;
	UT_ASSERT(try_update());
	UT_ASSERT_EQ(up_estate.es_processed, 0);
	UT_ASSERT_EQ(up_returns, 1);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(plain_update_publishes_index_after_vm_wal_and_new_tid);
	UT_RUN(distinct_vm_and_new_only_vm_are_ready_before_index);
	UT_RUN(hot_none_and_summarizing_preserve_original_index_choice);
	UT_RUN(no_indexes_and_no_visible_hint_preserve_update_and_returning);
	UT_RUN(nonshared_update_keeps_native_vm_and_wal_shape);
	UT_RUN(rejected_original_writer_retries_before_any_publication);
	UT_RUN(unproven_writer_or_bad_space_never_reaches_update_epilogue);
	UT_RUN(cancel_before_heap_mutation_never_publishes_index_or_returning);
	UT_RUN(non_ok_heap_results_do_not_publish_or_tag_a_row);
	UT_RUN(before_trigger_stop_keeps_heap_and_all_later_consumers_untouched);
	UT_RUN(materialized_tuple_is_freed_only_after_slot_tid_publication);
	UT_RUN(index_and_after_errors_preserve_original_later_consumer_order);
	UT_RUN(rls_and_view_checks_stay_on_their_original_sides_of_update);
	UT_RUN(can_set_tag_and_returning_remain_independent);
	UT_DONE();
	return ut_failed_count != 0;
}
