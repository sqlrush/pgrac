/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Real utility preflight, rewrite classifier, allocator, local BIND and abort.
 * The parent supplies physical IO/WAL and exact-owner scheduling boundaries.
 * Parser/catalog doubles below cover nullable/identity ADD and logged/AM
 * metadata. Complex ALTER TYPE preparation fails closed if reached.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN alter_active_owner_fixture_main
#include "test_cluster_tt_active_owner.c"
#undef main
#include "access/xact.h"
#include "access/relation.h"
#include "catalog/heap.h"
#include "commands/user.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "catalog/namespace.h"
#include "catalog/index.h"
#include "catalog/pg_inherits.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "../../backend/nodes/list.c"

bool XactReadOnly;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
static bool alter_parallel, alter_recovery, alter_child;
static TransactionId alter_top, alter_current;
static unsigned alter_xids, alter_locks, alter_lookups, alter_completed;
static unsigned abort_status_installs, abort_hints;
static bool alter_release_proven;
static ClusterConf alter_topology = {.node_count = 4};
static RelationData alter_relation;
static FormData_pg_class alter_class;

void *
palloc(Size size)
{
	return malloc(size);
}
void *
MemoryContextAlloc(MemoryContext c pg_attribute_unused(), Size size)
{
	return malloc(size);
}
MemoryContext
GetMemoryChunkContext(void *p pg_attribute_unused())
{
	return CurrentMemoryContext;
}
char *
pstrdup(const char *s)
{
	return strdup(s);
}
void
check_stack_depth(void)
{
}
bool
IsInTransactionBlock(bool top pg_attribute_unused())
{
	return false;
}
bool
IsInParallelMode(void)
{
	return alter_parallel;
}
bool
RecoveryInProgress(void)
{
	return alter_recovery;
}
bool
IsSubTransaction(void)
{
	return alter_child;
}
TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return alter_current;
}
TransactionId
GetTopTransactionIdIfAny(void)
{
	return alter_top;
}
TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(alter_top)) {
		alter_xids++;
		alter_top = alter_current = 100;
	}
	return alter_top;
}
CommandTag
CreateCommandTag(Node *n)
{
	UT_ASSERT(IsA(n, AlterTableStmt));
	return CMDTAG_ALTER_TABLE;
}
const char *
GetCommandTagName(CommandTag tag)
{
	UT_ASSERT_EQ(tag, CMDTAG_ALTER_TABLE);
	return "ALTER TABLE";
}
static void
unexpected_dependency(void)
{
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unexpected ALTER test dependency")));
}
bool
defGetBoolean(DefElem *d pg_attribute_unused())
{
	unexpected_dependency();
	return false;
}
void
CheckRelationsStorageDrop(DropStmt *s pg_attribute_unused(), bool top pg_attribute_unused())
{
	unexpected_dependency();
}
void
CheckObjectsStorageDrop(DropStmt *s pg_attribute_unused(), bool top pg_attribute_unused())
{
	unexpected_dependency();
}
void
CheckOwnedObjectsStorageDrop(DropOwnedStmt *s pg_attribute_unused(), bool top pg_attribute_unused())
{
	unexpected_dependency();
}

#include "test_cluster_shared_utility.inc"
#include "test_cluster_alter_early_index.inc"

/* Only the rewrite field is used by the classifier. No positive ALTER TYPE
 * result is supplied by this unavailable queue/preparation boundary. */
typedef struct AlteredTableInfo {
	int rewrite;
} AlteredTableInfo;
static AlteredTableInfo *
ATGetQueueEntry(List **q pg_attribute_unused(), Relation r pg_attribute_unused())
{
	unexpected_dependency();
	return NULL;
}
static void
ATPrepAlterColumnType(List **q pg_attribute_unused(), AlteredTableInfo *t pg_attribute_unused(),
					  Relation r pg_attribute_unused(), bool rec pg_attribute_unused(),
					  bool orig pg_attribute_unused(), AlterTableCmd *c pg_attribute_unused(),
					  LOCKMODE l pg_attribute_unused(),
					  AlterTableUtilityContext *ct pg_attribute_unused())
{
	unexpected_dependency();
}
static List *
find_typed_table_dependencies(Oid o pg_attribute_unused(), const char *s pg_attribute_unused(),
							  DropBehavior b pg_attribute_unused())
{
	unexpected_dependency();
	return NIL;
}
List *
find_all_inheritors(Oid o pg_attribute_unused(), LOCKMODE l pg_attribute_unused(),
					List **p pg_attribute_unused())
{
	unexpected_dependency();
	return NIL;
}
void
LockRelationOid(Oid o pg_attribute_unused(), LOCKMODE l pg_attribute_unused())
{
	unexpected_dependency();
}
LOCKMODE
AlterTableGetLockLevel(List *cmds pg_attribute_unused())
{
	return AccessExclusiveLock;
}
static void
RangeVarCallbackForAlterRelation(const RangeVar *r pg_attribute_unused(),
								 Oid o pg_attribute_unused(), Oid old pg_attribute_unused(),
								 void *arg pg_attribute_unused())
{
}
Oid
RangeVarGetRelidExtended(const RangeVar *r pg_attribute_unused(), LOCKMODE mode,
						 uint32 flags pg_attribute_unused(),
						 RangeVarGetRelidCallback cb pg_attribute_unused(),
						 void *arg pg_attribute_unused())
{
	ClusterCanonicalTxnBinding binding;

	alter_locks++;
	UT_ASSERT_EQ(mode, AccessExclusiveLock);
	/* An absent binding is an observable ordering failure, not a fake success. */
	if (!cluster_tt_local_get_published_binding(100, &binding))
		unexpected_dependency();
	UT_ASSERT_EQ(binding.segment_id, 1);
	UT_ASSERT_EQ(binding.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
	UT_ASSERT_EQ(slot_wait_calls, slot_wait_begins);
	UT_ASSERT_EQ(slot_wait_calls, slot_wait_ends);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	slot_wait_context_safe = false; /* The original DDL lock is now held. */
	return 701;
}
#include "test_cluster_fresh_alter_lookup.inc"
Relation
relation_open(Oid o, LOCKMODE l)
{
	UT_ASSERT_EQ(o, 701);
	UT_ASSERT_EQ(l, NoLock);
	UT_ASSERT(alter_locks > 0);
	alter_lookups++;
	return &alter_relation;
}
void
relation_close(Relation r, LOCKMODE l)
{
	UT_ASSERT(r == &alter_relation);
	UT_ASSERT_EQ(l, NoLock);
}
AttrNumber
get_attnum(Oid o pg_attribute_unused(), const char *s pg_attribute_unused())
{
	return 0;
}
Oid
get_table_am_oid(const char *s, bool missing pg_attribute_unused())
{
	UT_ASSERT_STR_EQ(s, "heap");
	return HEAP_TABLE_AM_OID;
}
char *
get_namespace_name(Oid o pg_attribute_unused())
{
	return pstrdup("public");
}
RangeVar *
makeRangeVar(char *s, char *r, int location)
{
	RangeVar *v = palloc0(sizeof(*v));
	v->type = T_RangeVar;
	v->schemaname = s;
	v->relname = r;
	v->location = location;
	return v;
}
void *
copyObjectImpl(const void *from)
{
	Size size = IsA(from, AlterTableStmt)  ? sizeof(AlterTableStmt)
				: IsA(from, AlterTableCmd) ? sizeof(AlterTableCmd)
										   : 0;
	void *to;
	if (!size)
		unexpected_dependency();
	to = palloc(size);
	memcpy(to, from, size);
	return to;
}
AlterTableStmt *
transformAlterTableStmt(Oid o pg_attribute_unused(), AlterTableStmt *s,
						const char *q pg_attribute_unused(), List **before, List **after)
{
	*before = *after = NIL;
	return s;
}
void
typenameTypeIdAndMod(ParseState *p pg_attribute_unused(), const TypeName *n, Oid *o, int32 *m)
{
	UT_ASSERT_EQ(n->typeOid, INT4OID);
	*o = INT4OID;
	*m = -1;
}
bool
DomainHasConstraints(Oid o)
{
	UT_ASSERT_EQ(o, INT4OID);
	return false;
}
ParseState *
make_parsestate(ParseState *p pg_attribute_unused())
{
	return palloc0(sizeof(ParseState));
}
void
free_parsestate(ParseState *p)
{
	pfree(p);
}
Node *
get_typdefault(Oid o)
{
	UT_ASSERT_EQ(o, INT4OID);
	return NULL;
}
Node *
cookDefault(ParseState *p pg_attribute_unused(), Node *n pg_attribute_unused(),
			Oid o pg_attribute_unused(), int32 m pg_attribute_unused(),
			const char *s pg_attribute_unused(), char g pg_attribute_unused())
{
	unexpected_dependency();
	return NULL;
}
Node *
coerce_to_target_type(ParseState *p pg_attribute_unused(), Node *n pg_attribute_unused(),
					  Oid a pg_attribute_unused(), Oid b pg_attribute_unused(),
					  int32 m pg_attribute_unused(), CoercionContext c pg_attribute_unused(),
					  CoercionForm f pg_attribute_unused(), int l pg_attribute_unused())
{
	unexpected_dependency();
	return NULL;
}
Oid
exprType(const Node *n pg_attribute_unused())
{
	unexpected_dependency();
	return InvalidOid;
}
bool
contain_volatile_functions_after_planning(Expr *n pg_attribute_unused())
{
	unexpected_dependency();
	return true;
}
#include "test_cluster_alter_early_classifier.inc"

/* Original terminal consumers remain real; only the overlay/hint sinks are
 * external. They cannot install success before the exact abort is durable. */
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	UT_ASSERT(g_abort_flush_seen);
	memset(result, 0, sizeof(*result));
	if (op == CLUSTER_TT_SOURCE_INSTALL_LOCAL) {
		UT_ASSERT_EQ(request->status, CLUSTER_TT_STATUS_ABORTED);
		abort_status_installs++;
	}
	result->bool_value = result->lookup.authoritative = true;
	result->lookup.status = CLUSTER_TT_STATUS_ABORTED;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp op,
									   const ClusterTTStatusHintSourceRequest *request)
{
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(op, CLUSTER_TT_HINT_SOURCE_EMIT);
	UT_ASSERT_EQ(request->status, CLUSTER_TT_STATUS_ABORTED);
	abort_hints++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

/* CTRC evidence is an external boundary; keep both refusal and exact release
 * cases, then let the real GC pass free the allocator slot. */
bool
cluster_ctrc_terminal_release_sample_exact(uint32 segment, uint16 slot, TransactionId xid,
										   uint16 wrap, uint8 status, SCN scn, uint64 epoch)
{
	TTSlot *disk = &((UndoSegmentHeaderData *)g_canned_block)->tt_slots[slot];
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT_EQ(epoch, 17);
	UT_ASSERT_EQ(xid, disk->xid);
	UT_ASSERT_EQ(wrap, disk->wrap);
	UT_ASSERT_EQ(status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(disk->status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(scn, InvalidScn);
	UT_ASSERT(g_abort_flush_seen);
	return alter_release_proven;
}
void
cluster_ctrc_stat_bump(ClusterCtrcStatId counter)
{
	UT_ASSERT_EQ(counter, CTRC_STAT_L12_RECYCLE);
}
bool
cluster_undo_horizon_epoch_fence_tripped(uint64 expected)
{
	return expected != 17;
}

static void
alter_entry(AlterTableStmt *stmt)
{
	PlannedStmt plan = {.utilityStmt = (Node *)stmt};
	PlannedStmt *pstmt = &plan;
	Node *parsetree;
	const char *queryString = "ALTER TABLE early_bind";
	bool isTopLevel = true;
	int readonly_flags;
#include "test_cluster_create_utility_entry.inc"
	alter_completed++;
}
static int
run_alter(AlterTableStmt *stmt)
{
	volatile int error = 0;
	PG_TRY();
	{
		alter_entry(stmt);
	}
	PG_CATCH();
	{
		error = last_ereport_errcode;
	}
	PG_END_TRY();
	return error;
}
static void
reset_alter(bool full)
{
	if (full)
		start_full_active_test();
	else
		reset_active_owner();
	ClusterConfShmem = &alter_topology;
	slot_wait_terminal_capacity = full;
	cluster_shared_catalog = true;
	/* The parent resets its canned disk at each allocation admission. This
	 * composition keeps the same physical image through BIND and abort. */
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	XactReadOnly = alter_parallel = alter_recovery = alter_child = false;
	alter_top = alter_current = InvalidTransactionId;
	alter_xids = alter_locks = alter_lookups = alter_completed = 0;
	abort_status_installs = abort_hints = 0;
	alter_release_proven = false;
	memset(&alter_relation, 0, sizeof(alter_relation));
	memset(&alter_class, 0, sizeof(alter_class));
	alter_relation.rd_rel = &alter_class;
	alter_relation.rd_id = 701;
	alter_class.relam = HEAP_TABLE_AM_OID;
	alter_class.relkind = RELKIND_RELATION;
	alter_class.relpersistence = RELPERSISTENCE_PERMANENT;
	strcpy(NameStr(alter_class.relname), "early_bind");
	last_ereport_errcode = 0;
}
static void
abort_alter(void)
{
	ClusterTTLocalBinding *allocation = cluster_tt_local_bindings;
	uint16 slot;
	UT_ASSERT(allocation != NULL);
	if (allocation == NULL)
		return;
	slot = allocation[0].slot_offset;

	{
		volatile bool aborted = false;
		g_require_abort_flush_before_write = true;
		PG_TRY();
		{
			aborted = cluster_tt_local_preabort_durable_finish(100);
		}
		PG_CATCH();
		{
			aborted = false;
		}
		PG_END_TRY();
		UT_ASSERT(aborted);
		if (!aborted)
			return;
	}
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT(g_abort_flush_seen);
	cluster_tt_local_record_abort(100);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_NULL(cluster_tt_local_bindings);
	UT_ASSERT_EQ(allocator_storage.per_node[0].slots[slot].status, CTS_ABORTED);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)g_current_resident)->tt_slots[slot].status,
				 TT_SLOT_ABORTED);
	UT_ASSERT_EQ(abort_status_installs, 1);
	UT_ASSERT_EQ(abort_hints, 1);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(g_current_pin_calls, g_current_unpin_calls);
	UT_ASSERT_EQ(g_current_acquire_calls, g_current_cancel_calls);
	UT_ASSERT_EQ(lock_depth, 0);
	free(allocation);
}

UT_TEST(test_dynamic_alter_waits_for_real_48_slots_before_classifier_lock)
{
	RangeVar relation = {.type = T_RangeVar, .relname = "early_bind"};
	TypeName type = {.type = T_TypeName, .typeOid = INT4OID};
	ColumnDef column = {.type = T_ColumnDef, .colname = "payload", .typeName = &type};
	AlterTableCmd cmd = {.type = T_AlterTableCmd, .subtype = AT_AddColumn, .def = (Node *)&column};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation};
	stmt.cmds = list_make1(&cmd);
	reset_alter(true);
	UT_ASSERT_EQ(run_alter(&stmt), 0);
	UT_ASSERT_EQ(slot_wait_calls, 1);
	UT_ASSERT_EQ(slot_wait_begins, 1);
	UT_ASSERT_EQ(slot_wait_ends, 1);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(alter_completed, 1);
	UT_ASSERT_EQ(alter_locks, 1);
	UT_ASSERT_EQ(cold_selection_calls, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	for (unsigned i = 1; i < 48; i++) {
		UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].status, CTS_ACTIVE);
		UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].xid, 200 + i);
	}
	abort_alter();
}
UT_TEST(test_context_and_static_refusals_precede_real_binding_and_lookup)
{
	RangeVar relation = {.type = T_RangeVar, .relname = "early_bind"};
	AlterTableCmd dynamic = {.type = T_AlterTableCmd, .subtype = AT_SetLogged};
	AlterTableCmd denied = {.type = T_AlterTableCmd, .subtype = AT_SetUnLogged};
	Constraint index = {.type = T_Constraint, .contype = CONSTR_UNIQUE, .access_method = "hash"};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation};
	for (unsigned guard = 0; guard < 8; guard++) {
		int expected = guard == 1	? ERRCODE_INVALID_TRANSACTION_STATE
					   : guard < 3	? ERRCODE_READ_ONLY_SQL_TRANSACTION
					   : guard == 4 ? ERRCODE_INTERNAL_ERROR
									: ERRCODE_FEATURE_NOT_SUPPORTED;
		reset_alter(true);
		stmt.cmds = list_make1(&dynamic);
		XactReadOnly = guard == 0;
		alter_parallel = guard == 1;
		alter_recovery = guard == 2;
		alter_child = guard == 3;
		if (guard == 4) {
			alter_current = 101;
			alter_top = 100;
		}
		if (guard == 5 || guard == 6)
			stmt.cmds = guard == 5 ? list_make2(&dynamic, &denied) : list_make2(&denied, &dynamic);
		if (guard == 7) {
			AlterTableCmd bad = {
				.type = T_AlterTableCmd, .subtype = AT_AddConstraint, .def = (Node *)&index};
			stmt.cmds = list_make2(&dynamic, &bad);
			UT_ASSERT_EQ(run_alter(&stmt), expected);
		} else
			UT_ASSERT_EQ(run_alter(&stmt), expected);
		UT_ASSERT_EQ(alter_xids + alter_locks + alter_lookups + alter_completed, 0);
		UT_ASSERT_EQ(g_bind_emit_calls + g_ctrc_reserve_calls + slot_wait_calls, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	}
}
UT_TEST(test_rewrite_rejection_retires_real_binding_without_leaking_slot)
{
	RangeVar relation = {.type = T_RangeVar, .relname = "early_bind"};
	ColumnDef column = {.type = T_ColumnDef, .colname = "payload", .identity = 'a'};
	AlterTableCmd cmd = {.type = T_AlterTableCmd, .subtype = AT_SetLogged};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation};
	stmt.cmds = list_make1(&cmd);
	for (unsigned rewrite = 0; rewrite < 3; rewrite++) {
		bool retained, drift;
		uint16 wrap;
		reset_alter(false);
		cmd.subtype = rewrite == 0	 ? AT_SetLogged
					  : rewrite == 1 ? AT_SetAccessMethod
									 : AT_AddColumn;
		cmd.name = "heap";
		cmd.def = rewrite == 2 ? (Node *)&column : NULL;
		if (rewrite == 0)
			alter_class.relpersistence = RELPERSISTENCE_UNLOGGED;
		if (rewrite == 1)
			alter_class.relam = InvalidOid;
		UT_ASSERT_EQ(run_alter(&stmt), ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(alter_completed, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
		abort_alter();
		{
			ClusterUndoCleanerPassStats stats = {0};
			/* ABORTED remains retained until the original exact release gate. */
			UT_ASSERT(cluster_tt_slot_gc_current_pass(InvalidScn, 17, &stats));
			UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 0);
			UT_ASSERT_EQ(allocator_storage.per_node[0].slots[0].status, CTS_ABORTED);
			alter_release_proven = true;
			UT_ASSERT(cluster_tt_slot_gc_current_pass(InvalidScn, 17, &stats));
			UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 1);
			UT_ASSERT_EQ(allocator_storage.per_node[0].slots[0].status, CTS_FREE);
		}
		UT_ASSERT_EQ(cluster_tt_slot_alloc_current_exact(0, 1, 101, &retained, &drift, &wrap), 0);
		UT_ASSERT(!retained && !drift);
	}
}
UT_TEST(test_all_four_dynamic_mixed_commands_reuse_one_binding)
{
	static const AlterTableType kinds[] = {AT_AddColumn, AT_AlterColumnType, AT_SetAccessMethod,
										   AT_SetLogged};
	RangeVar relation = {.type = T_RangeVar, .relname = "early_bind"};
	TypeName type = {.type = T_TypeName, .typeOid = INT4OID};
	ColumnDef column = {.type = T_ColumnDef, .colname = "payload", .typeName = &type};
	AlterTableCmd metadata = {.type = T_AlterTableCmd, .subtype = AT_SetStatistics};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation};
	for (unsigned kind = 0; kind < 4; kind++)
		for (unsigned position = 0; position < 2; position++) {
			AlterTableCmd cmd = {.type = T_AlterTableCmd,
								 .subtype = kinds[kind],
								 .name = "heap",
								 .def = (Node *)&column};
			reset_alter(false);
			/* TYPE preparation is not mocked into success: a non-storage relation
			 * exercises its real classifier routing, not its expression semantics. */
			if (kind == 1)
				alter_class.relkind = RELKIND_VIEW;
			stmt.cmds = position ? list_make2(&metadata, &cmd) : list_make2(&cmd, &metadata);
			UT_ASSERT_EQ(run_alter(&stmt), 0);
			UT_ASSERT_EQ(run_alter(&stmt), 0);
			UT_ASSERT_EQ(alter_xids, 1);
			UT_ASSERT_EQ(g_bind_emit_calls, 1);
			UT_ASSERT_EQ(g_ctrc_reserve_calls, 1);
			UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
			UT_ASSERT_EQ(alter_locks, 2);
			abort_alter();
		}
}
UT_TEST(test_full_slot_wait_keeps_native_cancel_deadlock_and_membership_errors)
{
	static const int errors[] = {ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
								 ERRCODE_T_R_DEADLOCK_DETECTED,
								 ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS};
	RangeVar relation = {.type = T_RangeVar, .relname = "early_bind"};
	AlterTableCmd cmd = {.type = T_AlterTableCmd, .subtype = AT_SetLogged};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt, .objtype = OBJECT_TABLE, .relation = &relation};
	stmt.cmds = list_make1(&cmd);
	for (unsigned i = 0; i < lengthof(errors); i++) {
		reset_alter(true);
		slot_wait_error = errors[i];
		UT_ASSERT_EQ(run_alter(&stmt), errors[i]);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, slot_wait_ends);
		UT_ASSERT_EQ(alter_locks + alter_lookups + alter_completed, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count + g_bind_emit_calls + g_ctrc_reserve_calls, 0);
	}
}
int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_dynamic_alter_waits_for_real_48_slots_before_classifier_lock);
	UT_RUN(test_context_and_static_refusals_precede_real_binding_and_lookup);
	UT_RUN(test_rewrite_rejection_retires_real_binding_without_leaking_slot);
	UT_RUN(test_all_four_dynamic_mixed_commands_reuse_one_binding);
	UT_RUN(test_full_slot_wait_keeps_native_cancel_deadlock_and_membership_errors);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
