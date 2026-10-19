/*-------------------------------------------------------------------------
 * test_cluster_write_preflight.c
 *    Run the native plan initialization before any child can write or lock.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_write_preflight.c
 *
 * The complete InitPlan body and heap preflight/guard bodies are extracted
 * verbatim. Relation lookup, canonical publication and child execution are
 * explicit boundaries; this fixture does not simulate BIND durability.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_tt_local.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_shared_catalog;
int cluster_node_id;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static RelationData relations[4];
static FormData_pg_class forms[4];
static RangeTblEntry ranges[4];
static EState estate;
static PlannedStmt statement;
static Plan plan, child_plan;
static PlanState state;
static QueryDesc query;
static TransactionId top_xid, current_xid;
static bool in_child, publish_ok, permissions_ok, child_error;
static unsigned allocations, prepares, bindings, child_calls, unbound_children;
static int reported_code;

void *
palloc(Size size)
{
	void *ptr = malloc(size);

	if (ptr == NULL)
		abort();
	return ptr;
}

void *
palloc0(Size size)
{
	void *ptr = palloc(size);

	MemSet(ptr, 0, size);
	return ptr;
}

void *
repalloc(void *ptr, Size size)
{
	return realloc(ptr, size);
}

void
pfree(void *ptr)
{
	free(ptr);
}

MemoryContext
GetMemoryChunkContext(void *ptr)
{
	return NULL;
}

void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	return palloc(size);
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

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
	reported_code = code;
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

int
errhint(const char *fmt, ...)
{
	return 0;
}

void
errfinish(const char *file, int line, const char *func)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
IsSubTransaction(void)
{
	return in_child;
}

TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return current_xid;
}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return top_xid;
}

TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(top_xid)) {
		top_xid = current_xid = 400;
		allocations++;
	}
	return top_xid;
}

bool
cluster_tt_local_prepare_canonical_active(TransactionId xid, ClusterCanonicalTxnBinding *binding)
{
	prepares++;
	UT_ASSERT_EQ(xid, 400);
	if (!publish_ok)
		return false;
	bindings = 1;
	MemSet(binding, 0, sizeof(*binding));
	binding->xid = xid;
	return true;
}

bool
ExecCheckPermissions(List *rtable, List *perminfos, bool report)
{
	if (!permissions_ok)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("fixture permission failure")));
	return true;
}

void
ExecInitRangeTable(EState *es, List *rtable, List *perminfos)
{
	es->es_range_table = rtable;
	es->es_range_table_size = 4;
}

Relation
ExecGetRangeTableRelation(EState *es, Index rti)
{
	if (rti < 1 || rti > 4)
		abort();
	return &relations[rti - 1];
}

static void
CheckValidRowMarkRel(Relation relation, RowMarkType type)
{
	if (RowMarkRequiresRowShareLock(type) && bindings == 0)
		unbound_children++;
}

PlanState *
ExecInitNode(Plan *node, EState *es, int flags)
{
	child_calls++;
	if (bindings == 0)
		unbound_children++;
	if (child_error)
		ereport(ERROR,
				(errcode(ERRCODE_RAISE_EXCEPTION), errmsg("fixture child or trigger error")));
	state.plan = node;
	return &state;
}

TupleDesc
ExecGetResultType(PlanState *ps)
{
	return NULL;
}

TupleTableSlot *
ExecInitExtraTupleSlot(EState *es, TupleDesc desc, const TupleTableSlotOps *ops)
{
	abort();
}

JunkFilter *
ExecInitJunkFilter(List *targetlist, TupleTableSlot *slot)
{
	abort();
}

const TupleTableSlotOps TTSOpsVirtual = { 0 };

bool
bms_is_member(int member, const Bitmapset *set)
{
	return false;
}

#include "../../backend/nodes/list.c"
#include "test_cluster_write_preflight.inc"

static void
reset(void)
{
	MemSet(&estate, 0, sizeof(estate));
	MemSet(&statement, 0, sizeof(statement));
	MemSet(&plan, 0, sizeof(plan));
	MemSet(&child_plan, 0, sizeof(child_plan));
	MemSet(&query, 0, sizeof(query));
	MemSet(relations, 0, sizeof(relations));
	MemSet(forms, 0, sizeof(forms));
	MemSet(ranges, 0, sizeof(ranges));
	for (unsigned i = 0; i < 4; ++i) {
		relations[i].rd_rel = &forms[i];
		relations[i].rd_id = 20000 + i;
		relations[i].rd_locator.relNumber = 20000 + i;
		forms[i].relkind = RELKIND_RELATION;
		forms[i].relpersistence = RELPERSISTENCE_PERMANENT;
		ranges[i].rtekind = RTE_RELATION;
		ranges[i].relid = relations[i].rd_id;
		statement.rtable = lappend(statement.rtable, &ranges[i]);
	}
	cluster_enabled = publish_ok = permissions_ok = true;
	cluster_shared_catalog = in_child = child_error = false;
	cluster_node_id = 0;
	allocations = prepares = bindings = child_calls = unbound_children = 0;
	top_xid = current_xid = InvalidTransactionId;
	reported_code = 0;
	statement.planTree = &plan;
	query.plannedstmt = &statement;
	query.estate = &estate;
	query.operation = CMD_UPDATE;
	statement.resultRelations = list_make1_int(1);
}

static bool
init_refused(int flags)
{
	volatile bool refused = false;

	PG_TRY();
	{
		InitPlan(&query, flags);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	return refused;
}

UT_TEST(zero_row_dml_binds_before_the_child_plan)
{
	CmdType operations[] = { CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE };

	for (unsigned i = 0; i < lengthof(operations); ++i) {
		reset();
		query.operation = operations[i];
		InitPlan(&query, 0);
		UT_ASSERT_EQ(allocations, 1);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(bindings, 1);
		UT_ASSERT_EQ(child_calls, 1);
		UT_ASSERT_EQ(unbound_children, 0);
	}
}

UT_TEST(modifying_cte_binds_before_any_subplan)
{
	reset();
	query.operation = CMD_SELECT;
	statement.hasModifyingCTE = true;
	statement.subplans = list_make1(&child_plan);
	InitPlan(&query, 0);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(child_calls, 2);
	UT_ASSERT_EQ(unbound_children, 0);
}

UT_TEST(row_locks_bind_before_rowmark_and_child_initialization)
{
	for (RowMarkType type = ROW_MARK_EXCLUSIVE; type <= ROW_MARK_KEYSHARE; ++type) {
		PlanRowMark mark = { .type = T_PlanRowMark };

		reset();
		query.operation = CMD_SELECT;
		statement.resultRelations = NIL;
		mark.markType = type;
		mark.rti = mark.prti = 1;
		statement.rowMarks = list_make1(&mark);
		InitPlan(&query, 0);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(unbound_children, 0);
	}
}

UT_TEST(partition_root_binds_before_routing_and_even_without_a_child)
{
	reset();
	forms[0].relkind = RELKIND_PARTITIONED_TABLE;
	relations[0].rd_locator.relNumber = 0;
	InitPlan(&query, 0);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(unbound_children, 0);
}

UT_TEST(mixed_targets_bind_only_once_before_child_work)
{
	reset();
	forms[0].relpersistence = RELPERSISTENCE_TEMP;
	statement.resultRelations = lappend_int(statement.resultRelations, 2);
	statement.resultRelations = lappend_int(statement.resultRelations, 3);
	InitPlan(&query, 0);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(unbound_children, 0);
}

UT_TEST(pure_select_and_nonlocking_rowmarks_keep_lazy_xid)
{
	PlanRowMark mark = { .type = T_PlanRowMark };

	reset();
	query.operation = CMD_SELECT;
	statement.resultRelations = NIL;
	InitPlan(&query, 0);
	UT_ASSERT_EQ(allocations + prepares, 0);
	reset();
	query.operation = CMD_SELECT;
	statement.resultRelations = NIL;
	mark.markType = ROW_MARK_REFERENCE;
	mark.rti = mark.prti = 1;
	statement.rowMarks = list_make1(&mark);
	InitPlan(&query, 0);
	UT_ASSERT_EQ(allocations + prepares, 0);
}

UT_TEST(explain_only_keeps_lazy_xid)
{
	reset();
	InitPlan(&query, EXEC_FLAG_EXPLAIN_ONLY);
	UT_ASSERT_EQ(allocations + prepares, 0);
	UT_ASSERT_EQ(child_calls, 1);
}

UT_TEST(private_temp_and_disabled_targets_keep_native_path)
{
	for (unsigned kind = 0; kind < 5; ++kind) {
		reset();
		if (kind == 0)
			forms[0].relpersistence = RELPERSISTENCE_TEMP;
		else if (kind == 1)
			relations[0].rd_locator.relNumber = 1249;
		else if (kind == 2)
			cluster_enabled = false;
		else if (kind == 3)
			cluster_node_id = -1;
		else {
			forms[0].relkind = RELKIND_PARTITIONED_TABLE;
			forms[0].relpersistence = RELPERSISTENCE_TEMP;
		}
		InitPlan(&query, 0);
		UT_ASSERT_EQ(allocations + prepares, 0);
	}
}

UT_TEST(shared_catalog_uses_the_same_binding_path)
{
	reset();
	relations[0].rd_locator.relNumber = 1249;
	cluster_shared_catalog = true;
	InitPlan(&query, 0);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(unbound_children, 0);
}

UT_TEST(nonheap_targets_do_not_bind_even_with_shared_catalog)
{
	char kinds[] = { RELKIND_VIEW, RELKIND_FOREIGN_TABLE };

	for (unsigned i = 0; i < lengthof(kinds); ++i) {
		reset();
		forms[0].relkind = kinds[i];
		cluster_shared_catalog = true;
		InitPlan(&query, 0);
		UT_ASSERT_EQ(allocations + prepares, 0);
	}
}

UT_TEST(subtransaction_rejects_before_xid_or_bind)
{
	for (unsigned partitioned = 0; partitioned < 2; ++partitioned) {
		reset();
		in_child = true;
		if (partitioned) {
			forms[0].relkind = RELKIND_PARTITIONED_TABLE;
			relations[0].rd_locator.relNumber = 0;
		}
		UT_ASSERT(init_refused(0));
		UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(allocations + prepares + child_calls, 0);
	}
}

UT_TEST(identity_mismatch_rejects_before_prepare)
{
	reset();
	top_xid = 400;
	current_xid = 401;
	UT_ASSERT(init_refused(0));
	UT_ASSERT_EQ(reported_code, ERRCODE_INTERNAL_ERROR);
	UT_ASSERT_EQ(allocations + prepares + child_calls, 0);
}

UT_TEST(bind_failure_prevents_child_work)
{
	reset();
	publish_ok = false;
	UT_ASSERT(init_refused(0));
	UT_ASSERT_EQ(reported_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings + child_calls, 0);
}

UT_TEST(permissions_reject_before_bind)
{
	reset();
	permissions_ok = false;
	UT_ASSERT(init_refused(0));
	UT_ASSERT_EQ(reported_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
	UT_ASSERT_EQ(allocations + prepares + child_calls, 0);
}

UT_TEST(child_error_preserves_original_transaction_ownership)
{
	reset();
	child_error = true;
	UT_ASSERT(init_refused(0));
	UT_ASSERT_EQ(reported_code, ERRCODE_RAISE_EXCEPTION);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(unbound_children, 0);
	UT_ASSERT_EQ(top_xid, 400);
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(15);
	UT_RUN(zero_row_dml_binds_before_the_child_plan);
	UT_RUN(modifying_cte_binds_before_any_subplan);
	UT_RUN(row_locks_bind_before_rowmark_and_child_initialization);
	UT_RUN(partition_root_binds_before_routing_and_even_without_a_child);
	UT_RUN(mixed_targets_bind_only_once_before_child_work);
	UT_RUN(pure_select_and_nonlocking_rowmarks_keep_lazy_xid);
	UT_RUN(explain_only_keeps_lazy_xid);
	UT_RUN(private_temp_and_disabled_targets_keep_native_path);
	UT_RUN(shared_catalog_uses_the_same_binding_path);
	UT_RUN(nonheap_targets_do_not_bind_even_with_shared_catalog);
	UT_RUN(subtransaction_rejects_before_xid_or_bind);
	UT_RUN(identity_mismatch_rejects_before_prepare);
	UT_RUN(bind_failure_prevents_child_work);
	UT_RUN(permissions_reject_before_bind);
	UT_RUN(child_error_preserves_original_transaction_ownership);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
