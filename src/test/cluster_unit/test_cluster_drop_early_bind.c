/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Real utility DROP preflight, allocator, dependency consumer, BIND and abort.
 * The parent supplies physical IO/WAL and exact-owner scheduling boundaries.
 * Root lookup and dependency rows are explicit model inputs. The real
 * whole-list storage consumer runs; physical deletion fails if reached.
 */
#define TT_ACTIVE_OWNER_FIXTURE_MAIN drop_active_owner_fixture_main
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
static bool drop_parallel, drop_recovery, drop_child, drop_in_transaction;
static TransactionId drop_top, drop_current;
static unsigned drop_xids, drop_locks, drop_lookups, drop_completed;
static unsigned abort_status_installs, abort_hints;
static bool drop_release_proven;
static ClusterConf drop_topology = {.node_count = 4};

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
IsInTransactionBlock(bool top)
{
	return drop_in_transaction || !top;
}

bool
IsInParallelMode(void)
{
	return drop_parallel;
}
bool
RecoveryInProgress(void)
{
	return drop_recovery;
}
bool
IsSubTransaction(void)
{
	return drop_child;
}
TransactionId
GetCurrentTransactionIdIfAny(void)
{
	return drop_current;
}
TransactionId
GetTopTransactionIdIfAny(void)
{
	return drop_top;
}
TransactionId
GetTopTransactionId(void)
{
	if (!TransactionIdIsValid(drop_top)) {
		drop_xids++;
		drop_top = drop_current = 100;
	}
	return drop_top;
}
CommandTag
CreateCommandTag(Node *n)
{
	return IsA(n, DropOwnedStmt) ? CMDTAG_DROP_OWNED : CMDTAG_DROP_TABLE;
}
const char *
GetCommandTagName(CommandTag tag)
{
	return tag == CMDTAG_DROP_OWNED ? "DROP OWNED" : "DROP";
}

#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/storage.h"
#include "catalog/pg_namespace_d.h"
#include "cluster/storage/cluster_smgr.h"
#include "catalog/pg_auth_members_d.h"
#include "catalog/pg_depend_d.h"
#include "commands/event_trigger.h"

/* Catalog/root lookup and pg_depend discovery are modeled inputs, not grants.
 * The original wrappers, performMultipleDeletions, deletion locks, whole-list
 * consumer and exact storage route/transaction/drain judges execute below.
 * Final physical deletion is an ERROR-only boundary; SQL/ownership and live
 * catalog discovery are not certified by this fixture. */
static Node *drop_statement;
static unsigned drop_route, storage_passes, dependency_passes, relation_checks;
static unsigned graph_rows, initial_roots, deletion_attempts, post_command_flags;
static RelationData graph_relations[2];
static FormData_pg_class graph_forms[2];
static bool graph_shared[2];
static bool require_bind_before_lock = true;

static void
unavailable_drop_dependency(void)
{
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("unavailable DROP dependency")));
}
bool
defGetBoolean(DefElem *d pg_attribute_unused())
{
	unavailable_drop_dependency();
	return false;
}
void
CheckClusterSharedAlterTable(AlterTableStmt *s pg_attribute_unused(), const char *q pg_attribute_unused())
{
	unavailable_drop_dependency();
}
static void
model_lock(void)
{
	ClusterCanonicalTxnBinding binding;
	if (require_bind_before_lock && !cluster_tt_local_get_published_binding(100, &binding))
		unavailable_drop_dependency();
	drop_locks++;
	/* Any attempt to start capacity sleep after this modeled strong lock
	 * remains unsafe. A warm binding still uses the original reuse path. */
	slot_wait_context_safe = false;
}
void
LockRelationOid(Oid oid, LOCKMODE mode)
{
	UT_ASSERT(oid == 701 || oid == 702);
	UT_ASSERT_EQ(mode, AccessExclusiveLock);
	model_lock();
}
void
LockDatabaseObject(Oid classid, Oid oid, uint16 subid, LOCKMODE mode)
{
	UT_ASSERT_EQ(classid, NamespaceRelationId);
	UT_ASSERT_EQ(oid, 701);
	UT_ASSERT_EQ(subid, 0);
	UT_ASSERT_EQ(mode, AccessExclusiveLock);
	model_lock();
}
void
LockSharedObject(Oid classid pg_attribute_unused(), Oid oid pg_attribute_unused(),
				 uint16 subid pg_attribute_unused(), LOCKMODE mode pg_attribute_unused())
{
	unavailable_drop_dependency();
}
Relation
table_open(Oid oid, LOCKMODE mode)
{
	UT_ASSERT_EQ(oid, DependRelationId);
	UT_ASSERT_EQ(mode, RowExclusiveLock);
	model_lock();
	return &graph_relations[0];
}
void
table_close(Relation rel pg_attribute_unused(), LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, RowExclusiveLock);
}
Relation
relation_open(Oid oid, LOCKMODE mode)
{
	UT_ASSERT(oid == 701 || oid == 702);
	UT_ASSERT_EQ(mode, NoLock);
	relation_checks++;
	return &graph_relations[oid - 701];
}
void
relation_close(Relation rel pg_attribute_unused(), LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, NoLock);
}
int
cluster_smgr_which_for(RelFileLocator loc, BackendId backend)
{
	UT_ASSERT_EQ(backend, InvalidBackendId);
	UT_ASSERT(loc.relNumber == 701 || loc.relNumber == 702);
	return graph_shared[loc.relNumber - 701] ? 1 : 0;
}
void
PreventInTransactionBlock(bool top, const char *command)
{
	/* Native xact internals are a separate regression. The real storage
	 * judge must reject transaction blocks before reaching this boundary. */
	UT_ASSERT(top && !drop_in_transaction);
	UT_ASSERT(strcmp(command, "DROP") == 0);
	post_command_flags++;
}
#include "test_cluster_drop_storage.inc"

bool
trackDroppedObjectsNeeded(void)
{
	return false;
}
ObjectClass
getObjectClass(const ObjectAddress *o pg_attribute_unused())
{
	unavailable_drop_dependency();
	return OCLASS_CLASS;
}
bool
EventTriggerSupportsObjectClass(ObjectClass c pg_attribute_unused())
{
	unavailable_drop_dependency();
	return false;
}
void
EventTriggerSQLDropAddObject(const ObjectAddress *o pg_attribute_unused(),
						   bool original pg_attribute_unused(), bool normal pg_attribute_unused())
{
	unavailable_drop_dependency();
}
static void
deleteOneObject(const ObjectAddress *o pg_attribute_unused(),
				Relation *dep pg_attribute_unused(), int32 flags pg_attribute_unused())
{
	deletion_attempts++;
	unavailable_drop_dependency(); /* No successful physical deletion supplied. */
}
#include "test_cluster_storage_delete_gate.inc"

ObjectAddresses *
new_object_addresses(void)
{
	ObjectAddresses *o = calloc(1, sizeof(*o));
	o->refs = calloc(2, sizeof(*o->refs));
	o->extras = calloc(2, sizeof(*o->extras));
	o->maxrefs = 2;
	return o;
}
void
free_object_addresses(ObjectAddresses *o)
{
	free(o->refs);
	free(o->extras);
	free(o);
}
void
add_exact_object_address(const ObjectAddress *obj, ObjectAddresses *o)
{
	UT_ASSERT(o->numrefs < 2);
	o->refs[o->numrefs] = *obj;
	o->extras[o->numrefs++].flags = DEPFLAG_ORIGINAL;
}
static void
findDependentObjects(const ObjectAddress *root, int objflags, int flags,
					 void *stack, ObjectAddresses *target, const ObjectAddresses *pending,
					 Relation *dep)
{
	UT_ASSERT_EQ(root->objectId, 701);
	UT_ASSERT_EQ(objflags, DEPFLAG_ORIGINAL);
	UT_ASSERT(stack == NULL && dep != NULL && pending->numrefs == 1);
	dependency_passes++;
	/* Literal catalog graph: a schema root may have a dependent relation.
	 * Discovery is modeled; locks and whole-list storage checks are real. */
	for (unsigned i = 0; i < graph_rows; i++) {
		ObjectAddress obj = {RelationRelationId, 701 + i, 0};
		if (drop_route == 1 && i == 0)
			obj.classId = NamespaceRelationId;
		AcquireDeletionLock(&obj, flags);
		add_exact_object_address(&obj, target);
		target->extras[target->numrefs - 1].flags = i ? DEPFLAG_NORMAL : DEPFLAG_ORIGINAL;
	}
}
static void
reportDependentObjects(const ObjectAddresses *o, DropBehavior b, int flags,
					   const ObjectAddress *root)
{
	/* RESTRICT diagnostics/catalog naming are unavailable here; refuse any
	 * attempt to claim them. All final-consumer cases explicitly use CASCADE. */
	if (b != DROP_CASCADE || !(flags & PERFORM_DELETION_USER_COMMAND)
		|| root == NULL || root->objectId != 701 || o->numrefs != 2)
		unavailable_drop_dependency();
}
#include "test_cluster_drop_dependency.inc"

static void
model_root_lookup(Node *stmt, unsigned route, bool check_only, bool top)
{
	ObjectAddresses *o;
	ObjectAddress obj = {route == 1 ? NamespaceRelationId : RelationRelationId, 701, 0};
	UT_ASSERT(stmt == drop_statement && route == drop_route && check_only);
	storage_passes++;
	o = new_object_addresses();
	if (initial_roots)
		add_exact_object_address(&obj, o);
	performMultipleDeletions(o, DROP_CASCADE,
							PERFORM_DELETION_USER_COMMAND | PERFORM_DELETION_CHECK_STORAGE_ONLY
								| PERFORM_DELETION_QUIETLY | (top ? PERFORM_DELETION_TOPLEVEL : 0));
	free_object_addresses(o);
}
static void
RemoveRelationsInternal(DropStmt *s, bool check_only, bool top)
{
	model_root_lookup((Node *)s, 0, check_only, top);
}
static void
RemoveObjectsInternal(DropStmt *s, bool check_only, bool top)
{
	model_root_lookup((Node *)s, 1, check_only, top);
}
static void
DropOwnedObjectsInternal(DropOwnedStmt *s, bool check_only, bool top)
{
	model_root_lookup((Node *)s, 2, check_only, top);
}
#include "test_cluster_drop_storage_roots.inc"
#include "test_cluster_shared_utility.inc"
#include "test_cluster_alter_early_index.inc"

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
	return drop_release_proven;
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
abort_drop(void)
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

static void
drop_entry(bool top)
{
	PlannedStmt plan = {.utilityStmt = drop_statement};
	PlannedStmt *pstmt = &plan;
	Node *parsetree;
	const char *queryString = "DROP early_bind";
	bool isTopLevel = top;
	int readonly_flags;
#include "test_cluster_create_utility_entry.inc"
	drop_completed++;
}
static int
run_drop(bool top)
{
	volatile int error = 0;
	PG_TRY(); { drop_entry(top); }
	PG_CATCH(); { error = last_ereport_errcode; }
	PG_END_TRY();
	return error;
}
static void
reset_drop(unsigned route, bool full)
{
	static DropStmt stmt;
	static DropOwnedStmt owned;
	if (full) start_full_active_test(); else reset_active_owner();
	ClusterConfShmem = &drop_topology;
	g_modifier_recheck_hook = NULL;
	enable_sealed_header_io();
	slot_wait_terminal_capacity = full;
	cluster_shared_catalog = cluster_shared_config = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_2;
	XactReadOnly = drop_parallel = drop_recovery = drop_child = drop_in_transaction = false;
	drop_top = drop_current = InvalidTransactionId;
	drop_xids = drop_locks = drop_lookups = drop_completed = 0;
	abort_status_installs = abort_hints = 0;
	drop_release_proven = false;
	storage_passes = dependency_passes = relation_checks = deletion_attempts = post_command_flags = 0;
	require_bind_before_lock = true;
	drop_route = route;
	initial_roots = 1;
	graph_rows = 2;
	memset(graph_relations, 0, sizeof(graph_relations));
	memset(graph_forms, 0, sizeof(graph_forms));
	for (unsigned i = 0; i < 2; i++) {
		graph_relations[i].rd_id = graph_relations[i].rd_locator.relNumber = 701 + i;
		graph_relations[i].rd_backend = InvalidBackendId;
		graph_relations[i].rd_rel = &graph_forms[i];
		graph_forms[i].relkind = RELKIND_RELATION;
		graph_forms[i].relpersistence = RELPERSISTENCE_PERMANENT;
		graph_shared[i] = false;
	}
	stmt = (DropStmt){.type = T_DropStmt, .removeType = route ? OBJECT_SCHEMA : OBJECT_TABLE,
		.behavior = DROP_CASCADE};
	owned = (DropOwnedStmt){.type = T_DropOwnedStmt, .behavior = DROP_CASCADE};
	drop_statement = route == 2 ? (Node *)&owned : (Node *)&stmt;
}
static void
abort_and_recycle_drop(void)
{
	ClusterUndoCleanerPassStats stats = {0};
	bool retained, drift;
	uint16 wrap;
	abort_drop();
	UT_ASSERT(cluster_tt_slot_gc_current_pass(InvalidScn, 17, &stats));
	UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 0);
	drop_release_proven = true;
	UT_ASSERT(cluster_tt_slot_gc_current_pass(InvalidScn, 17, &stats));
	UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 1);
	UT_ASSERT_EQ(cluster_tt_slot_alloc_current_exact(0, 1, 101, &retained, &drift, &wrap), 0);
	UT_ASSERT(!retained && !drift);
}
UT_TEST(test_three_drop_roots_wait_for_real_48_active_before_first_lock)
{
	for (unsigned route = 0; route < 3; route++) {
		reset_drop(route, true);
		UT_ASSERT_EQ(run_drop(true), 0);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(capacity_waits, 1);
		UT_ASSERT_EQ(slot_wait_begins, slot_wait_ends);
		UT_ASSERT_EQ(storage_passes, 1);
		UT_ASSERT_EQ(dependency_passes, 1);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		for (unsigned i = 1; i < 48; i++)
			UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].xid, 200 + i);
		abort_drop();
	}
}
UT_TEST(test_storage_refusals_preserve_codes_and_recycle_real_binding)
{
	for (unsigned route = 0; route < 3; route++)
		for (unsigned tx = 0; tx < 2; tx++)
			for (unsigned dependent = 0; dependent < 2; dependent++) {
				if (route == 1 && !dependent) continue;
				reset_drop(route, false);
				graph_shared[dependent] = true;
				drop_in_transaction = tx;
				UT_ASSERT_EQ(run_drop(true), tx ? ERRCODE_ACTIVE_SQL_TRANSACTION : ERRCODE_FEATURE_NOT_SUPPORTED);
				UT_ASSERT_EQ(g_bind_emit_calls, 1);
				UT_ASSERT_EQ(storage_passes, 1);
				UT_ASSERT_EQ(drop_completed + deletion_attempts, 0);
				abort_and_recycle_drop();
			}
}
UT_TEST(test_catalog_temp_local_and_empty_keep_original_admission)
{
	for (unsigned route = 0; route < 3; route++)
		for (unsigned variant = 0; variant < 4; variant++) {
			reset_drop(route, false);
			drop_in_transaction = true;
			if (variant == 0) graph_forms[0].relkind = graph_forms[1].relkind = RELKIND_VIEW;
			if (variant == 1) {
				graph_shared[0] = graph_shared[1] = true;
				graph_forms[0].relpersistence = graph_forms[1].relpersistence = RELPERSISTENCE_TEMP;
			}
			if (variant == 3) initial_roots = 0;
			UT_ASSERT_EQ(run_drop(true), 0);
			UT_ASSERT_EQ(storage_passes, 1);
			UT_ASSERT_EQ(g_bind_emit_calls, 1);
			UT_ASSERT_EQ(deletion_attempts, 0);
			abort_and_recycle_drop();
		}
}
UT_TEST(test_new_dynamic_dependency_is_checked_again_after_upper_preflight)
{
	for (unsigned route = 0; route < 3; route++) {
		volatile int error = 0;
		ObjectAddresses *o;
		ObjectAddress root = {RelationRelationId, 701, 0};
		reset_drop(route, false);
		UT_ASSERT_EQ(run_drop(true), 0);
		UT_ASSERT_EQ(dependency_passes, 1);
		graph_shared[1] = true; /* A later event/catalog snapshot adds shared storage. */
		o = new_object_addresses();
		add_exact_object_address(&root, o);
		PG_TRY(); { performMultipleDeletions(o, DROP_CASCADE, PERFORM_DELETION_USER_COMMAND | PERFORM_DELETION_TOPLEVEL); }
		PG_CATCH(); { error = last_ereport_errcode; }
		PG_END_TRY();
		UT_ASSERT_EQ(error, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(dependency_passes, 2);
		UT_ASSERT_EQ(deletion_attempts, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		free_object_addresses(o);
		abort_and_recycle_drop();
	}
}
UT_TEST(test_static_index_context_child_and_top_precede_storage_and_bind)
{
	for (unsigned route = 0; route < 3; route++)
	for (unsigned variant = route == 2 ? 2 : 0; variant < 8; variant++) {
		int expected = ERRCODE_FEATURE_NOT_SUPPORTED;
		reset_drop(route, true);
		graph_shared[0] = true;
		if (variant == 0) ((DropStmt *)drop_statement)->removeType = OBJECT_EXTENSION;
		if (variant == 1) {
			((DropStmt *)drop_statement)->removeType = OBJECT_INDEX;
			((DropStmt *)drop_statement)->concurrent = true;
		}
		if (variant == 2) { XactReadOnly = true; expected = ERRCODE_READ_ONLY_SQL_TRANSACTION; }
		if (variant == 3) { drop_parallel = true; expected = ERRCODE_INVALID_TRANSACTION_STATE; }
		if (variant == 4) { drop_recovery = true; expected = ERRCODE_READ_ONLY_SQL_TRANSACTION; }
		if (variant == 5) drop_child = true;
		if (variant == 6) { drop_current = 200; expected = ERRCODE_INTERNAL_ERROR; }
		if (variant == 7) { drop_in_transaction = true; drop_child = true; }
		UT_ASSERT_EQ(run_drop(true), expected);
		UT_ASSERT_EQ(storage_passes + drop_locks + g_bind_emit_calls + slot_wait_calls + drop_xids, 0);
	}
}
UT_TEST(test_native_wait_errors_and_unsafe_context_remain_before_storage)
{
	static const int errors[] = {ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE,
		ERRCODE_T_R_DEADLOCK_DETECTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
	for (unsigned i = 0; i < lengthof(errors) + 1; i++) {
		reset_drop(2, true);
		if (i < lengthof(errors)) slot_wait_error = errors[i];
		else slot_wait_context_safe = false;
		UT_ASSERT_EQ(run_drop(true), i < lengthof(errors) ? errors[i] : ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(storage_passes + drop_locks + g_bind_emit_calls, 0);
		UT_ASSERT_EQ(slot_wait_begins, slot_wait_ends);
	}
}
UT_TEST(test_repeated_upper_storage_check_reuses_exact_top_binding)
{
	for (unsigned route = 0; route < 3; route++) {
		reset_drop(route, false);
		UT_ASSERT_EQ(run_drop(true), 0);
		UT_ASSERT_EQ(run_drop(true), 0);
		UT_ASSERT_EQ(storage_passes, 2);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
		UT_ASSERT_EQ(drop_xids, 1);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
		abort_and_recycle_drop();
	}
}
int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_three_drop_roots_wait_for_real_48_active_before_first_lock);
	UT_RUN(test_storage_refusals_preserve_codes_and_recycle_real_binding);
	UT_RUN(test_catalog_temp_local_and_empty_keep_original_admission);
	UT_RUN(test_new_dynamic_dependency_is_checked_again_after_upper_preflight);
	UT_RUN(test_static_index_context_child_and_top_precede_storage_and_bind);
	UT_RUN(test_native_wait_errors_and_unsafe_context_remain_before_storage);
	UT_RUN(test_repeated_upper_storage_check_reuses_exact_top_binding);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
