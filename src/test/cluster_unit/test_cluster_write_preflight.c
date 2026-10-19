/*-------------------------------------------------------------------------
 * test_cluster_write_preflight.c
 *    Run native write entries before child work can write or lock.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_write_preflight.c
 *
 * Complete InitPlan, DoCopy, BeginCopyFrom, ExecSimpleRelation* and heap
 * preflight/guard bodies are extracted verbatim. Relation lookup, canonical
 * publication, COPY planning/input, triggers, expressions and tuple/index writes
 * are explicit boundaries. This fixture does not execute the COPY row loop or
 * simulate BIND durability, replication admission or transaction cleanup.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <sys/stat.h>
#include <unistd.h>
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_tt_local.h"
#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_shared_catalog;
int cluster_node_id;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext CurrentMemoryContext;
CommandDest whereToSendOutput = DestNone;
bool XactReadOnly;
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
static CopyFormatOptions copy_options;
static bool copy_default, copy_default_error, copy_option_error;
static unsigned default_plans, binary_headers, copy_unbound_work, input_reads;
static Expr copy_default_expr;
static ExprState copy_default_state;
static bool copy_where_error;
static unsigned where_folds, where_canonicalizations, where_lists, copy_runs;
static Expr copy_where_expr;
static List *copy_where_list;
static ParseNamespaceItem copy_namespace;
static RTEPermissionInfo copy_permission;
static bool replica_expect_binding, replica_identity_ok, replica_skip, replica_trigger_error;
static unsigned replica_identity_checks, replica_before, replica_generated, replica_constraints;
static unsigned replica_partitions, replica_tuples, replica_indexes, replica_after, replica_unbound;
static TupleConstr replica_tuple_constraints;
static TriggerDesc replica_triggers;
static const CmdType replica_commands[] = { CMD_INSERT, CMD_UPDATE, CMD_DELETE };

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

void *
MemoryContextAllocZeroAligned(MemoryContext context, Size size)
{
	return palloc0(size);
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

/* COPY catalog, memory, option and input boundaries. No rows are consumed. */
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min, Size init, Size max)
{
	return NULL;
}

void
ProcessCopyOptions(ParseState *pstate, CopyFormatOptions *opts, bool is_from, List *options)
{
	if (copy_option_error)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fixture COPY option error")));
	*opts = copy_options;
}

List *
CopyGetAttnums(TupleDesc desc, Relation rel, List *names)
{
	return copy_default ? NIL : list_make1_int(1);
}

int
pg_get_client_encoding(void)
{
	return PG_SQL_ASCII;
}

int
GetDatabaseEncoding(void)
{
	return PG_SQL_ASCII;
}

Oid
FindDefaultConversionProc(int32 from, int32 to)
{
	abort();
}

const char *
pg_encoding_to_char(int encoding)
{
	abort();
}

void
initStringInfo(StringInfo str)
{
	MemSet(str, 0, sizeof(*str));
}

void
getTypeInputInfo(Oid type, Oid *input, Oid *param)
{
	*input = *param = 1;
}

void
getTypeBinaryInputInfo(Oid type, Oid *input, Oid *param)
{
	getTypeInputInfo(type, input, param);
}

void
fmgr_info(Oid oid, FmgrInfo *info)
{
	MemSet(info, 0, sizeof(*info));
}

Node *
build_column_default(Relation rel, int attr)
{
	return (Node *)&copy_default_expr;
}

Expr *
expression_planner(Expr *expr)
{
	default_plans++;
	if (bindings == 0)
		copy_unbound_work++;
	if (copy_default_error)
		ereport(ERROR,
				(errcode(ERRCODE_RAISE_EXCEPTION), errmsg("fixture default planning error")));
	return expr;
}

ExprState *
ExecInitExpr(Expr *expr, PlanState *parent)
{
	return &copy_default_state;
}

bool
contain_volatile_functions_not_nextval(Node *expr)
{
	return false;
}

void
pgstat_progress_start_command(ProgressCommandType command, Oid oid)
{}

void
pgstat_progress_update_multi_param(int nparam, const int *index, const int64 *value)
{}

void
ReceiveCopyBegin(CopyFromState cstate)
{
	abort();
}

void
ReceiveCopyBinaryHeader(CopyFromState cstate)
{
	binary_headers++;
	if (bindings == 0)
		copy_unbound_work++;
}

char *
pstrdup(const char *str)
{
	return strdup(str);
}

FILE *
OpenPipeStream(const char *command, const char *mode)
{
	abort();
}

FILE *
AllocateFile(const char *name, const char *mode)
{
	abort();
}

int
errcode_for_file_access(void)
{
	abort();
}

static int
empty_copy_input(void *outbuf, int minread, int maxread)
{
	input_reads++;
	return 0;
}

/* Execute the complete SQL caller with parser/catalog and row-loop boundaries. */
Oid
GetUserId(void)
{
	return 1;
}

bool
has_privs_of_role(Oid member, Oid role)
{
	return true;
}

Relation
table_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	return &relations[0];
}

void
table_close(Relation relation, LOCKMODE lockmode)
{}

ParseNamespaceItem *
addRangeTableEntryForRelation(ParseState *pstate, Relation relation, int lockmode, Alias *alias,
							  bool inh, bool in_from)
{
	copy_namespace.p_perminfo = &copy_permission;
	return &copy_namespace;
}

void
addNSItemToQuery(ParseState *pstate, ParseNamespaceItem *item, bool join, bool rel, bool var)
{}

Node *
transformExpr(ParseState *pstate, Node *expr, ParseExprKind kind)
{
	return expr;
}

Node *
coerce_to_boolean(ParseState *pstate, Node *expr, const char *construct)
{
	return expr;
}

void
assign_expr_collations(ParseState *pstate, Node *expr)
{}

void
pull_varattnos(Node *expr, Index varno, Bitmapset **attrs)
{
	*attrs = NULL;
}

Bitmapset *
bms_add_range(Bitmapset *set, int lower, int upper)
{
	abort();
}

Bitmapset *
bms_del_member(Bitmapset *set, int member)
{
	abort();
}

int
bms_next_member(const Bitmapset *set, int previous)
{
	return -1;
}

Bitmapset *
bms_add_member(Bitmapset *set, int member)
{
	return set;
}

char *
get_attname(Oid relation, AttrNumber attr, bool missing_ok)
{
	abort();
}

char *
get_namespace_name(Oid namespace)
{
	abort();
}

RangeVar *
makeRangeVar(char *schema, char *relation, int location)
{
	abort();
}

int
check_enable_rls(Oid relation, Oid user, bool no_error)
{
	return RLS_NONE;
}

void
PreventCommandIfReadOnly(const char *command)
{
	ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("fixture read-only")));
}

Node *
eval_const_expressions(PlannerInfo *root, Node *expr)
{
	where_folds++;
	if (bindings == 0)
		copy_unbound_work++;
	if (copy_where_error)
		ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION), errmsg("fixture WHERE function error")));
	return expr;
}

Expr *
canonicalize_qual(Expr *expr, bool is_check)
{
	where_canonicalizations++;
	return expr;
}

List *
make_ands_implicit(Expr *expr)
{
	where_lists++;
	copy_where_list = list_make1(expr);
	return copy_where_list;
}

uint64
CopyFrom(CopyFromState copy)
{
	copy_runs++;
	if (copy->whereClause)
		UT_ASSERT(copy->whereClause == (Node *)copy_where_list);
	return 0;
}

void
EndCopyFrom(CopyFromState copy)
{}

CopyToState
BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query, Oid oid, const char *filename,
			bool program, copy_data_dest_cb callback, List *attrs, List *options)
{
	abort();
}

uint64
DoCopyTo(CopyToState copy)
{
	abort();
}

void
EndCopyTo(CopyToState copy)
{
	abort();
}

/* User work and storage are boundaries; all must see the early shared BIND. */
static void
replica_work(unsigned *counter)
{
	(*counter)++;
	if (replica_expect_binding && bindings != 1)
		replica_unbound++;
}

bool
IsCatalogRelation(Relation rel)
{
	return RelationGetRelid(rel) < FirstNormalObjectId;
}

void
CheckCmdReplicaIdentity(Relation rel, CmdType cmd)
{
	replica_identity_checks++;
	if (!replica_identity_ok)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("fixture replica identity failure")));
}

static bool
replica_before_trigger(void)
{
	replica_work(&replica_before);
	if (replica_trigger_error)
		ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION), errmsg("fixture trigger failure")));
	return !replica_skip;
}

bool
ExecBRInsertTriggers(EState *es, ResultRelInfo *info, TupleTableSlot *slot)
{
	return replica_before_trigger();
}

bool
ExecBRUpdateTriggers(EState *es, EPQState *epq, ResultRelInfo *info, ItemPointer tid, HeapTuple old,
					 TupleTableSlot *slot, TM_Result *result, TM_FailureData *failure)
{
	return replica_before_trigger();
}

bool
ExecBRDeleteTriggers(EState *es, EPQState *epq, ResultRelInfo *info, ItemPointer tid, HeapTuple old,
					 TupleTableSlot **slot, TM_Result *result, TM_FailureData *failure)
{
	return replica_before_trigger();
}

void
ExecComputeStoredGenerated(ResultRelInfo *info, EState *es, TupleTableSlot *slot, CmdType cmd)
{
	replica_work(&replica_generated);
}

void
ExecConstraints(ResultRelInfo *info, TupleTableSlot *slot, EState *es)
{
	replica_work(&replica_constraints);
}

bool
ExecPartitionCheck(ResultRelInfo *info, TupleTableSlot *slot, EState *es, bool emit)
{
	replica_work(&replica_partitions);
	return true;
}

void
simple_table_tuple_insert(Relation rel, TupleTableSlot *slot)
{
	replica_work(&replica_tuples);
}

void
simple_table_tuple_update(Relation rel, ItemPointer tid, TupleTableSlot *slot, Snapshot snapshot,
						  TU_UpdateIndexes *indexes)
{
	replica_work(&replica_tuples);
	*indexes = TU_All;
}

void
simple_table_tuple_delete(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	replica_work(&replica_tuples);
}

List *
ExecInsertIndexTuples(ResultRelInfo *info, TupleTableSlot *slot, EState *es, bool update,
					  bool no_dup, bool *conflict, List *arbiters, bool summarizing)
{
	replica_work(&replica_indexes);
	return list_make1_oid(123);
}

void
ExecARInsertTriggers(EState *es, ResultRelInfo *info, TupleTableSlot *slot, List *indexes,
					 TransitionCaptureState *capture)
{
	replica_work(&replica_after);
}

void
ExecARUpdateTriggers(EState *es, ResultRelInfo *info, ResultRelInfo *src, ResultRelInfo *dst,
					 ItemPointer tid, HeapTuple old, TupleTableSlot *slot, List *indexes,
					 TransitionCaptureState *capture, bool cross_partition)
{
	replica_work(&replica_after);
}

void
ExecARDeleteTriggers(EState *es, ResultRelInfo *info, ItemPointer tid, HeapTuple old,
					 TransitionCaptureState *capture, bool cross_partition)
{
	replica_work(&replica_after);
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
	MemSet(&copy_options, 0, sizeof(copy_options));
	copy_default = copy_default_error = copy_option_error = false;
	default_plans = binary_headers = copy_unbound_work = input_reads = 0;
	copy_where_error = XactReadOnly = false;
	where_folds = where_canonicalizations = where_lists = copy_runs = 0;
	copy_where_list = NIL;
	MemSet(&copy_namespace, 0, sizeof(copy_namespace));
	MemSet(&copy_permission, 0, sizeof(copy_permission));
	relations[0].rd_att = palloc0(offsetof(TupleDescData, attrs) + sizeof(FormData_pg_attribute));
	relations[0].rd_att->natts = 1;
}

static void
replica_reset(void)
{
	reset();
	replica_expect_binding = replica_identity_ok = true;
	replica_skip = replica_trigger_error = false;
	replica_identity_checks = replica_before = replica_generated = replica_constraints = 0;
	replica_partitions = replica_tuples = replica_indexes = replica_after = replica_unbound = 0;
	MemSet(&replica_tuple_constraints, 0, sizeof(replica_tuple_constraints));
	replica_tuple_constraints.has_generated_stored = true;
	relations[0].rd_att->constr = &replica_tuple_constraints;
	forms[0].relispartition = true;
	MemSet(&replica_triggers, 0, sizeof(replica_triggers));
	replica_triggers.trig_insert_before_row = true;
	replica_triggers.trig_update_before_row = true;
	replica_triggers.trig_delete_before_row = true;
}

static void
replica_run(CmdType cmd)
{
	ResultRelInfo info = { .ri_RelationDesc = &relations[0],
						   .ri_TrigDesc = &replica_triggers,
						   .ri_NumIndices = 1 };
	TupleTableSlot slot = { 0 }, search = { 0 };
	EPQState epq = { 0 };

	switch (cmd) {
	case CMD_INSERT:
		ExecSimpleRelationInsert(&info, &estate, &slot);
		break;
	case CMD_UPDATE:
		ExecSimpleRelationUpdate(&info, &estate, &epq, &search, &slot);
		break;
	case CMD_DELETE:
		ExecSimpleRelationDelete(&info, &estate, &epq, &search);
		break;
	default:
		abort();
	}
}

static bool
replica_refused(CmdType cmd)
{
	volatile bool refused = false;

	PG_TRY();
	{
		replica_run(cmd);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	return refused;
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

static CopyFromState
begin_copy(void)
{
	return BeginCopyFrom(NULL, &relations[0], NULL, NULL, false, empty_copy_input, NIL, NIL);
}

static bool
copy_refused(void)
{
	volatile bool refused = false;

	PG_TRY();
	{
		(void)begin_copy();
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	return refused;
}

UT_TEST(copy_entry_binds_without_reading_any_input)
{
	CopyFromState copy;

	reset();
	copy = begin_copy();
	UT_ASSERT(copy != NULL);
	UT_ASSERT_EQ(copy->copy_src, COPY_CALLBACK);
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(input_reads, 0);
}

UT_TEST(copy_defaults_are_planned_after_bind)
{
	reset();
	copy_default = true;
	(void)begin_copy();
	UT_ASSERT_EQ(default_plans, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(copy_unbound_work, 0);
}

UT_TEST(copy_binary_header_is_read_after_bind)
{
	reset();
	copy_options.binary = true;
	(void)begin_copy();
	UT_ASSERT_EQ(binary_headers, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(copy_unbound_work, 0);
}

UT_TEST(copy_partition_root_binds_before_any_partition_is_selected)
{
	reset();
	forms[0].relkind = RELKIND_PARTITIONED_TABLE;
	relations[0].rd_locator.relNumber = 0;
	(void)begin_copy();
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
}

UT_TEST(copy_shared_catalog_uses_original_binding)
{
	reset();
	cluster_shared_catalog = true;
	relations[0].rd_locator.relNumber = 1249;
	(void)begin_copy();
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
}

UT_TEST(copy_private_temp_and_disabled_targets_remain_lazy)
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
		(void)begin_copy();
		UT_ASSERT_EQ(allocations + prepares, 0);
	}
}

UT_TEST(copy_nonwrite_target_kinds_do_not_bind)
{
	char kinds[] = { RELKIND_VIEW, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_SEQUENCE };

	for (unsigned i = 0; i < lengthof(kinds); ++i) {
		reset();
		forms[0].relkind = kinds[i];
		cluster_shared_catalog = true;
		(void)begin_copy();
		UT_ASSERT_EQ(allocations + prepares, 0);
	}
}

UT_TEST(copy_subtransaction_rejects_before_bind_defaults_or_input)
{
	reset();
	in_child = copy_default = copy_options.binary = true;
	UT_ASSERT(copy_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(allocations + prepares + default_plans + binary_headers, 0);
}

UT_TEST(copy_bind_failure_prevents_defaults_and_input)
{
	reset();
	copy_default = copy_options.binary = true;
	publish_ok = false;
	UT_ASSERT(copy_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings + default_plans + binary_headers, 0);
}

UT_TEST(copy_shared_freeze_rejects_before_bind_defaults_or_input)
{
	char kinds[] = { RELKIND_RELATION, RELKIND_PARTITIONED_TABLE };

	for (unsigned i = 0; i < lengthof(kinds); ++i) {
		reset();
		forms[0].relkind = kinds[i];
		copy_options.freeze = copy_default = copy_options.binary = true;
		UT_ASSERT(copy_refused());
		UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(allocations + prepares + default_plans + binary_headers, 0);
	}
}

UT_TEST(copy_existing_binding_does_not_allocate_a_new_xid)
{
	reset();
	top_xid = current_xid = 400;
	bindings = 1;
	copy_default = true;
	(void)begin_copy();
	UT_ASSERT_EQ(allocations, 0);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(copy_unbound_work, 0);
}

UT_TEST(copy_default_error_retains_original_transaction_ownership)
{
	reset();
	copy_default = copy_default_error = true;
	UT_ASSERT(copy_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_RAISE_EXCEPTION);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(copy_unbound_work, 0);
	UT_ASSERT_EQ(top_xid, 400);
}

UT_TEST(copy_invalid_options_reject_before_bind)
{
	reset();
	copy_option_error = true;
	UT_ASSERT(copy_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_INVALID_PARAMETER_VALUE);
	UT_ASSERT_EQ(allocations + prepares + default_plans + binary_headers, 0);
}

UT_TEST(copy_identity_mismatch_rejects_before_prepare)
{
	reset();
	top_xid = 400;
	current_xid = 401;
	copy_default = copy_options.binary = true;
	UT_ASSERT(copy_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_INTERNAL_ERROR);
	UT_ASSERT_EQ(allocations + prepares + default_plans + binary_headers, 0);
}

UT_TEST(copy_temp_freeze_does_not_bind)
{
	reset();
	forms[0].relpersistence = RELPERSISTENCE_TEMP;
	copy_options.freeze = true;
	(void)begin_copy();
	UT_ASSERT_EQ(allocations + prepares, 0);
}

static bool
sql_copy_where_refused(void)
{
	ParseState pstate = { 0 };
	RangeVar target = { 0 };
	CopyStmt stmt = { 0 };
	uint64 processed;
	volatile bool refused = false;

	stmt.relation = &target;
	stmt.is_from = true;
	stmt.whereClause = (Node *)&copy_where_expr;
	PG_TRY();
	{
		DoCopy(&pstate, &stmt, 0, 0, &processed);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	return refused;
}

UT_TEST(copy_sql_where_folds_after_bind)
{
	reset();
	copy_default = copy_options.binary = true;
	UT_ASSERT(!sql_copy_where_refused());
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(where_folds, 1);
	UT_ASSERT_EQ(where_canonicalizations, 1);
	UT_ASSERT_EQ(where_lists, 1);
	UT_ASSERT_EQ(default_plans, 1);
	UT_ASSERT_EQ(binary_headers, 1);
	UT_ASSERT_EQ(copy_unbound_work, 0);
	UT_ASSERT_EQ(copy_runs, 1);
}

UT_TEST(copy_sql_where_subtransaction_refuses_before_folding)
{
	reset();
	in_child = true;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(allocations + prepares + where_folds + copy_runs, 0);
}

UT_TEST(copy_sql_where_freeze_refuses_before_folding)
{
	reset();
	copy_options.freeze = true;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(allocations + prepares + where_folds + copy_runs, 0);
}

UT_TEST(copy_sql_where_permission_refuses_before_folding_or_bind)
{
	reset();
	permissions_ok = false;
	copy_where_error = true;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_INSUFFICIENT_PRIVILEGE);
	UT_ASSERT_EQ(allocations + prepares + where_folds + copy_runs, 0);
}

UT_TEST(copy_sql_where_readonly_refuses_before_folding_or_bind)
{
	reset();
	XactReadOnly = copy_where_error = true;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_READ_ONLY_SQL_TRANSACTION);
	UT_ASSERT_EQ(allocations + prepares + where_folds + copy_runs, 0);
}

UT_TEST(copy_sql_where_bind_failure_prevents_folding)
{
	reset();
	publish_ok = false;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bindings + where_folds + copy_runs, 0);
}

UT_TEST(copy_sql_where_error_retains_original_binding)
{
	reset();
	copy_where_error = true;
	UT_ASSERT(sql_copy_where_refused());
	UT_ASSERT_EQ(reported_code, ERRCODE_RAISE_EXCEPTION);
	UT_ASSERT_EQ(where_folds, 1);
	UT_ASSERT_EQ(bindings, 1);
	UT_ASSERT_EQ(copy_unbound_work + copy_runs, 0);
}

UT_TEST(replica_entries_bind_before_all_user_and_storage_work)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		replica_run(cmd);
		UT_ASSERT_EQ(replica_identity_checks, 1);
		UT_ASSERT_EQ(allocations, 1);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(bindings, 1);
		UT_ASSERT_EQ(replica_before, 1);
		UT_ASSERT_EQ(replica_generated, cmd == CMD_DELETE ? 0 : 1);
		UT_ASSERT_EQ(replica_constraints, cmd == CMD_DELETE ? 0 : 1);
		UT_ASSERT_EQ(replica_partitions, cmd == CMD_DELETE ? 0 : 1);
		UT_ASSERT_EQ(replica_tuples, 1);
		UT_ASSERT_EQ(replica_indexes, cmd == CMD_DELETE ? 0 : 1);
		UT_ASSERT_EQ(replica_after, 1);
		UT_ASSERT_EQ(replica_unbound, 0);
	}
}

UT_TEST(replica_trigger_skipped_rows_still_have_original_binding)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		replica_skip = true;
		replica_run(cmd);
		UT_ASSERT_EQ(bindings, 1);
		UT_ASSERT_EQ(replica_before, 1);
		UT_ASSERT_EQ(replica_generated + replica_constraints + replica_partitions + replica_tuples
						 + replica_indexes + replica_after,
					 0);
		UT_ASSERT_EQ(replica_unbound, 0);
	}
}

UT_TEST(replica_subtransactions_refuse_before_bind_or_user_work)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		in_child = true;
		UT_ASSERT(replica_refused(cmd));
		UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(allocations + prepares + bindings + replica_before + replica_tuples, 0);
	}
}

UT_TEST(replica_bind_failure_prevents_user_work)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		publish_ok = false;
		UT_ASSERT(replica_refused(cmd));
		UT_ASSERT_EQ(reported_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(bindings + replica_before + replica_generated + replica_tuples, 0);
	}
}

UT_TEST(replica_private_temp_and_disabled_targets_keep_native_path)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		for (unsigned kind = 0; kind < 3; ++kind) {
			replica_reset();
			replica_expect_binding = false;
			if (kind == 0)
				relations[0].rd_locator.relNumber = 1249;
			else if (kind == 1)
				forms[0].relpersistence = RELPERSISTENCE_TEMP;
			else
				cluster_enabled = false;
			replica_run(cmd);
			UT_ASSERT_EQ(allocations + prepares + bindings, 0);
			UT_ASSERT_EQ(replica_before, 1);
			UT_ASSERT_EQ(replica_tuples, 1);
			UT_ASSERT_EQ(replica_after, 1);
		}
	}
}

UT_TEST(replica_existing_top_xid_reuses_original_binding)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		top_xid = current_xid = 400;
		bindings = 1;
		replica_run(cmd);
		UT_ASSERT_EQ(allocations, 0);
		UT_ASSERT_EQ(prepares, 1);
		UT_ASSERT_EQ(bindings, 1);
		UT_ASSERT_EQ(replica_unbound, 0);
	}
}

UT_TEST(replica_identity_refuses_before_bind_and_user_work)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		replica_identity_ok = false;
		UT_ASSERT(replica_refused(cmd));
		UT_ASSERT_EQ(reported_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(replica_identity_checks, 1);
		UT_ASSERT_EQ(allocations + prepares + bindings + replica_before + replica_tuples, 0);
	}
}

UT_TEST(replica_trigger_error_retains_original_transaction_binding)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		replica_trigger_error = true;
		UT_ASSERT(replica_refused(cmd));
		UT_ASSERT_EQ(reported_code, ERRCODE_RAISE_EXCEPTION);
		UT_ASSERT_EQ(top_xid, 400);
		UT_ASSERT_EQ(current_xid, 400);
		UT_ASSERT_EQ(bindings, 1);
		UT_ASSERT_EQ(replica_before, 1);
		UT_ASSERT_EQ(replica_unbound + replica_tuples + replica_after, 0);
	}
}

UT_TEST(replica_mismatched_xid_refuses_before_prepare_or_user_work)
{
	for (unsigned i = 0; i < lengthof(replica_commands); ++i) {
		CmdType cmd = replica_commands[i];

		replica_reset();
		top_xid = 400;
		current_xid = 401;
		UT_ASSERT(replica_refused(cmd));
		UT_ASSERT_EQ(reported_code, ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(allocations + prepares + bindings + replica_before + replica_tuples, 0);
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(46);
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
	UT_RUN(copy_entry_binds_without_reading_any_input);
	UT_RUN(copy_defaults_are_planned_after_bind);
	UT_RUN(copy_binary_header_is_read_after_bind);
	UT_RUN(copy_partition_root_binds_before_any_partition_is_selected);
	UT_RUN(copy_shared_catalog_uses_original_binding);
	UT_RUN(copy_private_temp_and_disabled_targets_remain_lazy);
	UT_RUN(copy_nonwrite_target_kinds_do_not_bind);
	UT_RUN(copy_subtransaction_rejects_before_bind_defaults_or_input);
	UT_RUN(copy_bind_failure_prevents_defaults_and_input);
	UT_RUN(copy_shared_freeze_rejects_before_bind_defaults_or_input);
	UT_RUN(copy_existing_binding_does_not_allocate_a_new_xid);
	UT_RUN(copy_default_error_retains_original_transaction_ownership);
	UT_RUN(copy_invalid_options_reject_before_bind);
	UT_RUN(copy_identity_mismatch_rejects_before_prepare);
	UT_RUN(copy_temp_freeze_does_not_bind);
	UT_RUN(copy_sql_where_folds_after_bind);
	UT_RUN(copy_sql_where_subtransaction_refuses_before_folding);
	UT_RUN(copy_sql_where_freeze_refuses_before_folding);
	UT_RUN(copy_sql_where_permission_refuses_before_folding_or_bind);
	UT_RUN(copy_sql_where_readonly_refuses_before_folding_or_bind);
	UT_RUN(copy_sql_where_bind_failure_prevents_folding);
	UT_RUN(copy_sql_where_error_retains_original_binding);
	UT_RUN(replica_entries_bind_before_all_user_and_storage_work);
	UT_RUN(replica_trigger_skipped_rows_still_have_original_binding);
	UT_RUN(replica_subtransactions_refuse_before_bind_or_user_work);
	UT_RUN(replica_bind_failure_prevents_user_work);
	UT_RUN(replica_private_temp_and_disabled_targets_keep_native_path);
	UT_RUN(replica_existing_top_xid_reuses_original_binding);
	UT_RUN(replica_identity_refuses_before_bind_and_user_work);
	UT_RUN(replica_trigger_error_retains_original_transaction_binding);
	UT_RUN(replica_mismatched_xid_refuses_before_prepare_or_user_work);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
