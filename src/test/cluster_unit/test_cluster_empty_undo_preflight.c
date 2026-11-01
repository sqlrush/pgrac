/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Original executor/heap preflight -> actual allocator/BIND -> original local
 * commit/abort consumers, with no tuple or data-undo operation. The inherited
 * physical IO/WAL and admission boundaries are controlled, not durable media.
 * This does not execute xact.c, SQL, CTRC release or a native scheduler.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN empty_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/heapam.h"
#include "executor/executor.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "utils/rel.h"
#include "../../backend/nodes/list.c"

static RelationData empty_relation;
static FormData_pg_class empty_form;
static EState empty_estate;
static PlannedStmt empty_statement;
static PlanRowMark empty_rowmark;
static TransactionId empty_top, empty_current;
static bool empty_child;
static unsigned empty_xids, empty_lookups, empty_overlay_calls;

/* The original List implementation shares the inherited fixture allocator. */
void *
palloc(Size size)
{
	void *pointer;

	check_memory_allocation();
	pointer = malloc(size);
	if (pointer == NULL)
		abort();
	return pointer;
}

bool
IsSubTransaction(void)
{
	return empty_child;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return empty_current;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return empty_top;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(empty_top)) {
		empty_xids++;
		empty_top = empty_current = 100;
	}
	return empty_top;
}

/* Relation lookup is a leaf; the real executor and heap routing predicates
 * decide whether this relation needs the original canonical publication. */
static Relation
empty_range_relation(EState *estate, Index index)
{
	UT_ASSERT(estate == &empty_estate);
	UT_ASSERT_EQ(index, 1);
	UT_ASSERT_EQ(lock_depth, 0);
	empty_lookups++;
	return &empty_relation;
}

/* Negative-only overlay boundary. It cannot fabricate a local terminal hint
 * or authoritative lookup; physical and allocator terminal results stay real. */
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	UT_ASSERT_EQ(op, CLUSTER_TT_SOURCE_INSTALL_LOCAL);
	UT_ASSERT(request != NULL && request->key != NULL && result != NULL);
	UT_ASSERT_EQ(request->key->local_xid, 100);
	UT_ASSERT(request->status == CLUSTER_TT_STATUS_COMMITTED
			  || request->status == CLUSTER_TT_STATUS_ABORTED);
	empty_overlay_calls++;
	MemSet(result, 0, sizeof(*result));
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp op pg_attribute_unused(),
									   const ClusterTTStatusHintSourceRequest *request
										   pg_attribute_unused())
{
	abort(); /* A refused overlay install must not emit a hint. */
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

#define ExecGetRangeTableRelation empty_range_relation
#include "test_cluster_empty_undo_preflight.inc"
#undef ExecGetRangeTableRelation

static void
empty_reset(bool rowmark)
{
	reset_active_owner();
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	MemSet(&empty_relation, 0, sizeof(empty_relation));
	MemSet(&empty_form, 0, sizeof(empty_form));
	MemSet(&empty_estate, 0, sizeof(empty_estate));
	MemSet(&empty_statement, 0, sizeof(empty_statement));
	MemSet(&empty_rowmark, 0, sizeof(empty_rowmark));
	empty_relation.rd_rel = &empty_form;
	empty_relation.rd_id = empty_relation.rd_locator.relNumber = 20000;
	empty_form.relkind = RELKIND_RELATION;
	empty_form.relpersistence = RELPERSISTENCE_PERMANENT;
	empty_child = false;
	empty_top = empty_current = InvalidTransactionId;
	empty_xids = empty_lookups = empty_overlay_calls = 0;
	if (rowmark) {
		empty_rowmark.type = T_PlanRowMark;
		empty_rowmark.markType = ROW_MARK_EXCLUSIVE;
		empty_rowmark.rti = empty_rowmark.prti = 1;
		empty_statement.rowMarks = list_make1(&empty_rowmark);
	} else
		empty_statement.resultRelations = list_make1_int(1);
}

static bool
empty_preflight(ClusterCanonicalTxnBinding *binding)
{
	volatile bool caught = false;

	PG_TRY();
	{
		cluster_executor_preflight_write(&empty_statement, &empty_estate, 0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	if (caught)
		return false;
	UT_ASSERT(cluster_tt_local_get_published_binding(100, binding));
	return cluster_tt_local_get_published_binding(100, binding);
}

static bool
empty_record_abort(void)
{
	volatile bool caught = false;

	PG_TRY();
	{
		cluster_tt_local_record_abort(100);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	return !caught;
}

static void
empty_assert_active(const ClusterCanonicalTxnBinding *binding)
{
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)g_current_resident;
	const TTSlot *slot = &header->tt_slots[binding->slot_offset];

	UT_ASSERT_EQ(empty_xids, 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(slot->status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(slot->xid, 100);
	UT_ASSERT(UBA_is_invalid(slot->first_undo_block));
	UT_ASSERT_EQ(slot->flags, TT_FLAGS_RESERVED);
	UT_ASSERT_EQ(cluster_tt_local_bindings[0].active_alias_count, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(lock_depth, 0);
}

static void
empty_assert_finished(const ClusterCanonicalTxnBinding *binding, bool committed)
{
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)g_current_resident;
	const TTSlot *slot = &header->tt_slots[binding->slot_offset];
	const ClusterTTSlotAllocEntry *entry = &allocator_storage.per_node[0].slots[binding->slot_offset];

	UT_ASSERT_EQ(slot->status, committed ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED);
	UT_ASSERT_EQ(slot->xid, 100);
	UT_ASSERT_EQ(slot->wrap, binding->slot_wrap);
	UT_ASSERT_EQ(slot->commit_scn, committed ? 91 : InvalidScn);
	UT_ASSERT(UBA_is_invalid(slot->first_undo_block));
	UT_ASSERT_EQ(slot->flags, TT_FLAGS_RESERVED); /* Terminal is not release authority. */
	UT_ASSERT_EQ(entry->status, committed ? CTS_COMMITTED : CTS_ABORTED);
	UT_ASSERT_EQ(entry->xid, 100);
	UT_ASSERT_EQ(entry->wrap, binding->slot_wrap);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_NULL(cluster_tt_local_bindings);
	UT_ASSERT_EQ(empty_overlay_calls, 1);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(g_writer_fd_open, 0);
	UT_ASSERT(!g_pin_reserved && !g_current_active);
	UT_ASSERT_EQ(lock_depth, 0);
	assert_sealed_resident_matches_disk();
	list_free(empty_statement.resultRelations);
	list_free(empty_statement.rowMarks);
}

UT_TEST(test_empty_write_and_rowmark_preflight_commit_original_binding)
{
	for (unsigned rowmark = 0; rowmark < 2; rowmark++) {
		ClusterCanonicalTxnBinding binding;
		xl_xact_tt_commit fold = { 0 };
		void *bindings;
		int flags;

		empty_reset(rowmark != 0);
		if (!empty_preflight(&binding))
			return;
		empty_assert_active(&binding);
		bindings = cluster_tt_local_bindings;
		UT_ASSERT(cluster_tt_local_precommit_durable_finish(100, 91, &fold));
		UT_ASSERT_EQ(fold.segment_id, 1);
		UT_ASSERT_EQ(fold.xid, 100);
		UT_ASSERT_EQ(fold.slot_offset, binding.slot_offset);
		UT_ASSERT_EQ(fold.wrap, binding.slot_wrap);
		UT_ASSERT_EQ(fold.commit_scn, 91);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves + 1);
		UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_ACTIVE);
		/* Native flushed commit/checkpoint window is an explicit input, not
		 * a fake commit decision or a simulated execution of xact.c. */
		g_flush_ptr = g_write_requires_flush = 0x1000000;
		flags = MyProc->delayChkptFlags;
		MyProc->delayChkptFlags |= DELAY_CHKPT_START;
		CritSectionCount++;
		PG_TRY();
		{
			cluster_tt_local_commit_durable_apply(g_flush_ptr);
		}
		PG_FINALLY();
		{
			MyProc->delayChkptFlags = flags;
			CritSectionCount--;
		}
		PG_END_TRY();
		UT_ASSERT(!g_write_before_flush);
		cluster_tt_local_record_commit(100, 91);
		empty_assert_finished(&binding, true);
		free(bindings);
	}
}

UT_TEST(test_repeated_empty_preflight_reuses_one_binding_then_aborts)
{
	ClusterCanonicalTxnBinding binding, repeated;
	void *bindings;

	empty_reset(false);
	if (!empty_preflight(&binding))
		return;
	/* Subsequent lookups happen after BIND; their relation identity is unchanged. */
	for (unsigned i = 0; i < 2; i++) {
		if (!empty_preflight(&repeated))
			return;
		UT_ASSERT_EQ(memcmp(&binding, &repeated, sizeof(binding)), 0);
	}
	empty_assert_active(&binding);
	bindings = cluster_tt_local_bindings;
	g_require_abort_flush_before_write = true;
	UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT_EQ(g_flushed_lsn, g_abort_exact_lsn);
	if (!empty_record_abort())
		return;
	empty_assert_finished(&binding, false);
	free(bindings);
}

UT_TEST(test_post_preflight_error_and_cancel_keep_binding_until_original_abort)
{
	static const int errors[] = { ERRCODE_RAISE_EXCEPTION, ERRCODE_QUERY_CANCELED };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		ClusterCanonicalTxnBinding binding;
		volatile bool caught = false;
		void *bindings;

		empty_reset(false);
		if (!empty_preflight(&binding))
			return;
		PG_TRY();
		{
			ereport(ERROR, (errcode(errors[i]), errmsg("controlled post-preflight error")));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(last_ereport_errcode, errors[i]);
		empty_assert_active(&binding);
		bindings = cluster_tt_local_bindings;
		UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
		if (!empty_record_abort())
			return;
		empty_assert_finished(&binding, false);
		free(bindings);
	}
}

UT_TEST(test_unflushed_empty_commit_unstages_before_original_abort)
{
	ClusterCanonicalTxnBinding binding;
	xl_xact_tt_commit fold = { 0 };
	void *bindings;

	empty_reset(false);
	if (!empty_preflight(&binding))
		return;
	bindings = cluster_tt_local_bindings;
	UT_ASSERT(cluster_tt_local_precommit_durable_finish(100, 91, &fold));
	UT_ASSERT_EQ(g_writer_fd_open, 1);
	UT_ASSERT(g_pin_reserved);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves + 1);
	UT_ASSERT(cluster_tt_local_preabort_durable_finish(100));
	UT_ASSERT(!cluster_tt_slot_durable_commit_staged(1, binding.slot_offset, 100,
													 binding.slot_wrap));
	UT_ASSERT_EQ(g_reservation_release_calls, 1);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	if (!empty_record_abort())
		return;
	empty_assert_finished(&binding, false);
	free(bindings);
}

UT_TEST(test_no_shared_write_explain_and_child_do_not_allocate_a_binding)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		volatile bool caught = false;

		empty_reset(false);
		if (mode == 0) {
			list_free(empty_statement.resultRelations);
			empty_statement.resultRelations = NIL;
		} else if (mode == 2)
			empty_form.relpersistence = RELPERSISTENCE_TEMP;
		empty_child = mode == 3;
		PG_TRY();
		{
			cluster_executor_preflight_write(&empty_statement, &empty_estate,
											 mode == 1 ? EXEC_FLAG_EXPLAIN_ONLY : 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, mode == 3);
		if (caught)
			UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(empty_xids + g_bind_emit_calls + cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
		list_free(empty_statement.resultRelations);
	}
}

#ifdef EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN
int EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN(void);
#define main EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN
#endif

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_empty_write_and_rowmark_preflight_commit_original_binding);
	UT_RUN(test_repeated_empty_preflight_reuses_one_binding_then_aborts);
	UT_RUN(test_post_preflight_error_and_cancel_keep_binding_until_original_abort);
	UT_RUN(test_unflushed_empty_commit_unstages_before_original_abort);
	UT_RUN(test_no_shared_write_explain_and_child_do_not_allocate_a_binding);
	UT_DONE();
	return ut_failed_count != 0;
}

#ifdef EMPTY_UNDO_PREFLIGHT_FIXTURE_MAIN
#undef main
#endif
