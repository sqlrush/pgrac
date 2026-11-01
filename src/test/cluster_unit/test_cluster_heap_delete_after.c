/*-------------------------------------------------------------------------
 * test_cluster_heap_delete_after.c
 *    DELETE reaches AFTER/RETURNING only after heap/VM WAL returns.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Complete original executor and heap AM run through the actual table
 * inline to the existing DML VM writer/clear/WAL fixture. Scheduling,
 * receipt/ITL, trigger, old-row, projection and slot leaves are controlled.
 * Unsupported EPQ, INSTEAD and partition leaves abort; FDW is outside
 * the input domain. This is not full
 * heap_delete, SQL, commit, ResourceOwner, remote ACK or durability.
 *-------------------------------------------------------------------------
 */
int delete_after_inherited_main(void);
#define DML_VM_WAL_MAIN delete_after_inherited_main
#include "test_cluster_heap_dml_vm_wal_chain.c"
#undef DML_VM_WAL_MAIN

#include "access/tableam.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "utils/snapmgr.h"
#include "test_cluster_heap_update_context.inc"

SnapshotData SnapshotAnyData = {.snapshot_type = SNAPSHOT_ANY};
TransactionId CheckXidAlive;
bool bsysscan;

static ModifyTableContext dl_context;
static ModifyTableState dl_state;
static EState dl_estate;
static ResultRelInfo dl_result;
static TriggerDesc dl_triggers;
static TupleTableSlot dl_slot, dl_plan, dl_return_slot;
static TupleTableSlot *dl_epq_slot;
static TableAmRoutine dl_am;
static SnapshotData dl_snapshot, dl_crosscheck;
static ItemPointerData dl_tid;
static bool dl_shared, dl_visible, dl_done, dl_before;
static bool dl_process_returning, dl_can_tag, dl_with_outputs, dl_deleted;
static bool dl_xact_snapshot, dl_bad_cmax, dl_materialized;
static HeapTuple dl_oldtuple;
static TM_Result dl_status, dl_tmresult;
static unsigned dl_tables, dl_heaps, dl_befores, dl_afters;
static unsigned dl_fetches, dl_stores, dl_projects, dl_materializes, dl_clears;
static int dl_error, dl_slot_value, dl_return_value;

static bool
dl_ready(void)
{
	UT_ASSERT(dl_done);
	if (!dl_done)
		return false;
	UT_ASSERT_EQ(dl_tables, 1);
	UT_ASSERT_EQ(dl_heaps, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!chain_heap && !multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!chain_held[1] && !multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	if (dl_visible)
		UT_ASSERT_EQ((unsigned char)pages[1].data[SizeOfPageHeaderData], 0xfc);
	if (dl_shared)
		check_wal(dl_visible ? 2 : 1, next_token);
	else
	{
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(chain_warms, dl_visible ? 1 : 0);
		UT_ASSERT_EQ(chain_captures + chain_resumes, 0);
	}
	UT_ASSERT_EQ(dl_estate.es_processed, dl_can_tag ? 1 : 0);
	if (dl_with_outputs)
	{
		UT_ASSERT(dl_deleted);
		UT_ASSERT_EQ(dl_tmresult, TM_Ok);
	}
	return true;
}

static TM_Result
dl_heap_delete(Relation relation, ItemPointer tid, CommandId cid,
			   Snapshot crosscheck, bool wait, TM_FailureData *failure,
			   bool changing_part)
{
	UT_ASSERT(relation == &relation_data && tid == &dl_tid);
	UT_ASSERT_EQ(cid, dl_estate.es_output_cid);
	UT_ASSERT(crosscheck == &dl_crosscheck && wait && failure == &dl_context.tmfd);
	UT_ASSERT(!changing_part);
	UT_ASSERT_EQ(dl_afters + dl_fetches + dl_stores + dl_projects, 0);
	dl_heaps++;
	failure->cmax = cid + (dl_bad_cmax ? 1 : 0);
	if (dl_status != TM_Ok)
		return dl_status;
	execute_chain(CHAIN_DELETE, dl_shared, false, dl_visible ? 1 : 0, false, false);
	dl_done = true;
	return TM_Ok;
}

#define heap_delete dl_heap_delete
#include "test_cluster_heap_delete_am.inc"
#undef heap_delete

static TM_Result
dl_table_delete(Relation relation, ItemPointer tid, CommandId cid,
				Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *failure, bool changing_part)
{
	UT_ASSERT(snapshot == &dl_snapshot);
	UT_ASSERT_EQ(dl_befores, 1);
	dl_tables++;
	return heapam_tuple_delete(relation, tid, cid, snapshot, crosscheck,
							   wait, failure, changing_part);
}

static bool
dl_before_trigger(EState *estate, EPQState *epq, ResultRelInfo *result,
				  ItemPointer tid, HeapTuple oldtuple, TupleTableSlot **epqslot,
				  TM_Result *status, TM_FailureData *failure)
{
	UT_ASSERT(estate == &dl_estate && epq == NULL && result == &dl_result);
	UT_ASSERT(tid == &dl_tid && oldtuple == dl_oldtuple && epqslot == &dl_epq_slot);
	UT_ASSERT(failure == &dl_context.tmfd);
	UT_ASSERT_EQ(inserts + dl_tables + dl_afters + dl_projects, 0);
	if (dl_with_outputs)
	{
		UT_ASSERT(status == &dl_tmresult && !dl_deleted);
		UT_ASSERT_EQ(*status, TM_Ok);
	}
	else
		UT_ASSERT(status == NULL);
	dl_befores++;
	return dl_before;
}

static void
dl_after_trigger(EState *estate, ResultRelInfo *result, ItemPointer tid,
				 HeapTuple oldtuple, TransitionCaptureState *capture, bool changing_part)
{
	UT_ASSERT(estate == &dl_estate && result == &dl_result && tid == &dl_tid);
	UT_ASSERT(oldtuple == dl_oldtuple && capture == NULL && !changing_part);
	dl_ready();
	UT_ASSERT_EQ(dl_fetches + dl_stores + dl_projects, 0);
	dl_afters++;
	if (dl_error == 1)
		elog(ERROR, "controlled DELETE AFTER error");
}

static TupleTableSlot *
dl_returning_slot(EState *estate, ResultRelInfo *result)
{
	UT_ASSERT(estate == &dl_estate && result == &dl_result);
	dl_ready();
	UT_ASSERT_EQ(dl_afters, 1);
	return &dl_slot;
}

static void
dl_fill_slot(void)
{
	dl_slot.tts_tableOid = relation_data.rd_id;
	dl_slot.tts_tid = dl_tid;
	dl_slot_value = 601;
}

static bool
dl_fetch_row(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	UT_ASSERT(relation == &relation_data && tid == &dl_tid && slot == &dl_slot);
	UT_ASSERT(snapshot == SnapshotAny && dl_oldtuple == NULL);
	dl_ready();
	UT_ASSERT_EQ(dl_afters, 1);
	dl_fetches++;
	if (dl_error == 2)
		return false;
	dl_fill_slot();
	return true;
}

static void
dl_store_row(HeapTuple value, TupleTableSlot *slot, bool should_free)
{
	UT_ASSERT(value == dl_oldtuple && value != NULL && slot == &dl_slot && !should_free);
	dl_ready();
	UT_ASSERT_EQ(dl_afters, 1);
	dl_stores++;
	dl_fill_slot();
}

static TupleTableSlot *
dl_project(ResultRelInfo *result, TupleTableSlot *slot, TupleTableSlot *plan)
{
	UT_ASSERT(result == &dl_result && slot == &dl_slot && plan == &dl_plan);
	dl_ready();
	UT_ASSERT_EQ(dl_fetches + dl_stores, 1);
	UT_ASSERT_EQ(dl_slot_value, 601);
	UT_ASSERT_EQ(slot->tts_tableOid, relation_data.rd_id);
	UT_ASSERT(memcmp(&slot->tts_tid, &dl_tid, sizeof(ItemPointerData)) == 0);
	dl_projects++;
	return &dl_return_slot;
}

static void
dl_materialize(TupleTableSlot *slot)
{
	UT_ASSERT(slot == &dl_return_slot && dl_projects == 1 && dl_clears == 0);
	UT_ASSERT_EQ(dl_slot_value, 601);
	dl_return_value = dl_slot_value;
	dl_materialized = true;
	dl_materializes++;
}

static TupleTableSlot *
dl_clear(TupleTableSlot *slot)
{
	UT_ASSERT(slot == &dl_slot && dl_materialized);
	UT_ASSERT_EQ(dl_materializes, 1);
	dl_slot_value = 0;
	dl_clears++;
	return slot;
}

static void dl_unexpected(const char *branch, ...) pg_attribute_noreturn();
static void
dl_unexpected(const char *branch, ...)
{
	(void)branch;
	abort();
}

#define ExecPendingInserts(...) dl_unexpected("pending INSERT", __VA_ARGS__)
#define ExecIRDeleteTriggers(...) (dl_unexpected("instead trigger", __VA_ARGS__), false)
#define ExecARUpdateTriggers(...) dl_unexpected("partition trigger", __VA_ARGS__)
#define ExecStoreAllNullTuple(...) (dl_unexpected("FDW slot", __VA_ARGS__), (TupleTableSlot *)NULL)
#define EvalPlanQualBegin(...) dl_unexpected("EPQ begin", __VA_ARGS__)
#define EvalPlanQualSlot(...) (dl_unexpected("EPQ slot", __VA_ARGS__), (TupleTableSlot *)NULL)
#define EvalPlanQual(...) (dl_unexpected("EPQ", __VA_ARGS__), (TupleTableSlot *)NULL)
#define table_tuple_lock(...) (dl_unexpected("tuple lock", __VA_ARGS__), TM_Ok)
#define ExecInsertIndexTuples(...) (dl_unexpected("DELETE index insert", __VA_ARGS__), (List *)NULL)
#undef IsolationUsesXactSnapshot
#define IsolationUsesXactSnapshot() dl_xact_snapshot
#define ExecBRDeleteTriggers dl_before_trigger
#define ExecARDeleteTriggers dl_after_trigger
#define ExecGetReturningSlot dl_returning_slot
#define ExecForceStoreHeapTuple dl_store_row
#define ExecProcessReturning dl_project
#define ExecMaterializeSlot dl_materialize
#define ExecClearTuple dl_clear
#define errhint(...) 0
#ifndef DELETE_AFTER_EXECUTOR_FIXTURE
#define DELETE_AFTER_EXECUTOR_FIXTURE "test_cluster_heap_delete_executor.inc"
#endif
#include DELETE_AFTER_EXECUTOR_FIXTURE

static bool
try_delete(void)
{
	volatile bool success = false;

	PG_TRY();
	{
		TupleTableSlot *returned = ExecDelete(&dl_context, &dl_result, &dl_tid,
												dl_oldtuple, dl_process_returning, false, dl_can_tag,
												dl_with_outputs ? &dl_tmresult : NULL,
												dl_with_outputs ? &dl_deleted : NULL, &dl_epq_slot);

		UT_ASSERT(returned == (dl_projects ? &dl_return_slot : NULL));
		if (returned)
		{
			UT_ASSERT_EQ(dl_materializes, 1);
			UT_ASSERT_EQ(dl_clears, 1);
			UT_ASSERT_EQ(dl_return_value, 601);
			UT_ASSERT_EQ(dl_slot_value, 0);
		}
		success = true;
	}
	PG_CATCH();
	{
		/* No simulated rollback or native ResourceOwner cleanup. */
	}
	PG_END_TRY();
	return success;
}

static void
prepare_delete_after(bool shared, bool visible)
{
	prepare_chain(shared, false, visible ? 1 : 0);
	memset(&dl_context, 0, sizeof(dl_context));
	memset(&dl_state, 0, sizeof(dl_state));
	memset(&dl_estate, 0, sizeof(dl_estate));
	memset(&dl_result, 0, sizeof(dl_result));
	memset(&dl_triggers, 0, sizeof(dl_triggers));
	memset(&dl_slot, 0, sizeof(dl_slot));
	memset(&dl_return_slot, 0, sizeof(dl_return_slot));
	memset(&dl_am, 0, sizeof(dl_am));
	dl_context.estate = &dl_estate;
	dl_context.mtstate = &dl_state;
	dl_context.planSlot = &dl_plan;
	dl_state.operation = CMD_DELETE;
	dl_estate.es_output_cid = 2;
	dl_estate.es_snapshot = &dl_snapshot;
	dl_estate.es_crosscheck_snapshot = &dl_crosscheck;
	dl_result.ri_RelationDesc = &relation_data;
	dl_result.ri_TrigDesc = &dl_triggers;
	dl_result.ri_NumIndices = 1;
	dl_result.ri_projectReturning = (ProjectionInfo *)&dl_return_slot;
	dl_triggers.trig_delete_before_row = true;
	dl_am.tuple_delete = dl_table_delete;
	dl_am.tuple_fetch_row_version = dl_fetch_row;
	relation_data.rd_tableam = &dl_am;
	relation_data.rd_id = 4242;
	ItemPointerSet(&dl_tid, blocks[0], 1);
	dl_epq_slot = NULL;
	dl_shared = shared;
	dl_visible = visible;
	dl_done = dl_xact_snapshot = dl_bad_cmax = dl_materialized = false;
	dl_before = dl_process_returning = dl_can_tag = dl_with_outputs = dl_deleted = true;
	dl_status = TM_Ok;
	dl_tmresult = TM_Invisible;
	dl_oldtuple = NULL;
	dl_tables = dl_heaps = dl_befores = dl_afters = 0;
	dl_fetches = dl_stores = dl_projects = dl_materializes = dl_clears = 0;
	dl_error = dl_slot_value = dl_return_value = 0;
}

UT_TEST(after_and_returning_observe_shared_vm_clear_wal_and_deleted_flag)
{
	prepare_delete_after(true, true);
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(dl_afters, 1);
	UT_ASSERT_EQ(dl_fetches, 1);
	UT_ASSERT_EQ(dl_projects, 1);
}

UT_TEST(no_visible_hint_needs_no_vm_writer_before_after)
{
	prepare_delete_after(true, false);
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(vm_clears + chain_warms + chain_captures + chain_resumes, 0);
	UT_ASSERT_EQ(dl_afters + dl_projects, 2);
}

UT_TEST(nonshared_delete_keeps_native_vm_and_wal_shape)
{
	prepare_delete_after(false, true);
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(dl_afters + dl_projects, 2);
}

UT_TEST(rejected_writer_retries_before_after_or_returning)
{
	prepare_delete_after(true, true);
	chain_refusals = 2;
	chain_resume_refusals = 1;
	UT_ASSERT(try_delete());
	UT_ASSERT(chain_attempts > 1);
	UT_ASSERT_EQ(dl_afters, 1);
	UT_ASSERT_EQ(dl_projects, 1);
}

UT_TEST(unproven_writer_capture_error_and_bad_space_publish_no_consumers)
{
	for (int fault = 0; fault < 3; fault++)
	{
		PGAlignedBlock before[4];

		prepare_delete_after(true, true);
		chain_missing = fault == 0;
		chain_capture_error = fault == 1;
		if (fault == 2)
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		memcpy(before, pages, sizeof(before));
		UT_ASSERT(!try_delete());
		UT_ASSERT_EQ(inserts + dl_afters + dl_fetches + dl_projects, 0);
		UT_ASSERT_EQ(dl_estate.es_processed, 0);
		UT_ASSERT(!dl_deleted);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	}
}

UT_TEST(cancel_before_heap_mutation_reaches_no_delete_consumer)
{
	prepare_delete_after(true, true);
	chain_refusals = 1;
	chain_cancel = true;
	UT_ASSERT(!try_delete());
	UT_ASSERT_EQ(inserts + dl_afters + dl_fetches + dl_projects, 0);
	UT_ASSERT_EQ(dl_estate.es_processed, 0);
	UT_ASSERT(!dl_deleted);
}

UT_TEST(non_ok_heap_results_do_not_publish_or_tag_a_delete)
{
	const TM_Result statuses[] = {TM_SelfModified, TM_Deleted, TM_Updated,
		TM_Deleted, TM_SelfModified};

	for (unsigned i = 0; i < lengthof(statuses); i++)
	{
		prepare_delete_after(true, true);
		dl_status = statuses[i];
		dl_xact_snapshot = i >= 2;
		dl_bad_cmax = i == 4;
		UT_ASSERT_EQ(try_delete(), i < 2);
		UT_ASSERT_EQ(dl_tmresult, statuses[i]);
		UT_ASSERT_EQ(inserts + dl_afters + dl_fetches + dl_projects, 0);
		UT_ASSERT_EQ(dl_estate.es_processed, 0);
		UT_ASSERT(!dl_deleted);
	}
}

UT_TEST(before_trigger_stop_preserves_original_output_initialization)
{
	prepare_delete_after(true, true);
	dl_before = false;
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(dl_befores, 1);
	UT_ASSERT_EQ(dl_tables + inserts + dl_afters + dl_fetches + dl_projects, 0);
	UT_ASSERT_EQ(dl_tmresult, TM_Ok);
	UT_ASSERT(!dl_deleted);
}

UT_TEST(returning_requires_both_caller_request_and_projection)
{
	for (int missing = 0; missing < 2; missing++)
	{
		prepare_delete_after(true, true);
		if (missing)
			dl_result.ri_projectReturning = NULL;
		else
			dl_process_returning = false;
		UT_ASSERT(try_delete());
		UT_ASSERT_EQ(dl_afters, 1);
		UT_ASSERT_EQ(dl_fetches + dl_projects + dl_materializes + dl_clears, 0);
		UT_ASSERT(dl_deleted);
	}
}

UT_TEST(nullable_outputs_and_can_set_tag_do_not_disable_returning)
{
	prepare_delete_after(true, true);
	dl_with_outputs = dl_can_tag = false;
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(dl_estate.es_processed, 0);
	UT_ASSERT_EQ(dl_projects, 1);
	UT_ASSERT_EQ(dl_tmresult, TM_Invisible);
}

UT_TEST(after_or_returning_fetch_error_preserves_later_consumer_order)
{
	for (int error = 1; error <= 2; error++)
	{
		prepare_delete_after(true, true);
		dl_error = error;
		UT_ASSERT(!try_delete());
		UT_ASSERT_EQ(inserts, 1); /* ERROR is not simulated rollback. */
		UT_ASSERT(dl_deleted);
		UT_ASSERT_EQ(dl_afters, 1);
		UT_ASSERT_EQ(dl_fetches, error == 2);
		UT_ASSERT_EQ(dl_projects + dl_materializes + dl_clears, 0);
	}
}

UT_TEST(provided_old_tuple_is_materialized_before_returning_slot_clear)
{
	prepare_delete_after(true, true);
	dl_oldtuple = &tuple;
	UT_ASSERT(try_delete());
	UT_ASSERT_EQ(dl_fetches, 0);
	UT_ASSERT_EQ(dl_stores, 1);
	UT_ASSERT_EQ(dl_materializes, 1);
	UT_ASSERT_EQ(dl_clears, 1);
	UT_ASSERT_EQ(dl_return_value, 601);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(after_and_returning_observe_shared_vm_clear_wal_and_deleted_flag);
	UT_RUN(no_visible_hint_needs_no_vm_writer_before_after);
	UT_RUN(nonshared_delete_keeps_native_vm_and_wal_shape);
	UT_RUN(rejected_writer_retries_before_after_or_returning);
	UT_RUN(unproven_writer_capture_error_and_bad_space_publish_no_consumers);
	UT_RUN(cancel_before_heap_mutation_reaches_no_delete_consumer);
	UT_RUN(non_ok_heap_results_do_not_publish_or_tag_a_delete);
	UT_RUN(before_trigger_stop_preserves_original_output_initialization);
	UT_RUN(returning_requires_both_caller_request_and_projection);
	UT_RUN(nullable_outputs_and_can_set_tag_do_not_disable_returning);
	UT_RUN(after_or_returning_fetch_error_preserves_later_consumer_order);
	UT_RUN(provided_old_tuple_is_materialized_before_returning_slot_clear);
	UT_DONE();
	return ut_failed_count != 0;
}
