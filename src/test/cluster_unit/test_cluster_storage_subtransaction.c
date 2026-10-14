/*
 * Shared subtransaction DDL must refuse before the native storage producer.
 * Reuse the native I/O boundaries; catalog/storage.c is linked unchanged.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int space_storage_fixture_main(void);
#define main space_storage_fixture_main
#define errcode space_fixture_errcode
#define errhint space_fixture_errhint
#include "test_cluster_space_storage.c"
#undef main
#undef errcode
#undef errhint

static int refusal_code;
static char refusal_hint[512];

int errcode(int code);
int errhint(const char *fmt,...) pg_attribute_printf(1, 2);

int
errcode(int code)
{
	refusal_code = code;
	return 0;
}

int
errhint(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	vsnprintf(refusal_hint, sizeof(refusal_hint), fmt, args);
	va_end(args);
	return 0;
}

static void
check_refusal(void)
{
	UT_ASSERT_EQ(reported_level, ERROR);
	UT_ASSERT_EQ(refusal_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(strstr(refusal_hint, "top-level transaction") != NULL);
	UT_ASSERT(strstr(refusal_hint, "PL/pgSQL EXCEPTION") != NULL);
}

UT_TEST(test_child_create_refuses_before_storage_or_abort_owner)
{
	volatile bool caught = false;

	reset();
	native_owner = expecting_error = true;
	nest_level = 2;
	PG_TRY();
	{
		RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(io_calls + main_create_calls + create_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT_EQ(capture_calls + shared_identity_locks + maintenance_identity_locks, 0);
	UT_ASSERT(!delete_registered);
	/* Also exercise the original rollback cleanup, which must have no work. */
	AtSubAbort_smgr();
	UT_ASSERT_EQ(unlink_calls, 0);
	nest_level = 1;
	smgrDoPendingDeletes(true);
}

UT_TEST(test_child_drop_refuses_before_ko_or_pending_delete)
{
	RelationData rel = {0};
	FormData_pg_class form = {0};
	volatile bool caught = false;

	reset();
	native_owner = expecting_error = true;
	nest_level = 2;
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	PG_TRY();
	{
		RelationDropStorage(&rel);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(ko_calls + io_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT(!delete_registered);
	AtSubAbort_smgr();
	nest_level = 1;
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(unlink_calls, 0);
}

UT_TEST(test_child_truncate_refuses_before_flush_or_structure)
{
	Relation rel = native_truncate_relation();
	PGAlignedBlock saved[2];
	volatile bool caught = false;
	unsigned before_io = io_calls;

	memcpy(saved, pages, sizeof(saved));
	nest_level = 2;
	expecting_error = true;
	PG_TRY();
	{
		RelationTruncate(rel, 4);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(ko_calls + relation_flushes + wal_calls + dirty_calls + truncate_calls, 0);
	UT_ASSERT_EQ(io_calls, before_io);
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	nest_level = 1;
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_private_child_create_keeps_native_cleanup)
{
	locator.relNumber = 1249;
	reset();
	shared = false;
	native_owner = true;
	nest_level = 2;
	UT_ASSERT(RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true) == &storage);
	UT_ASSERT_EQ(main_create_calls, 1);
	UT_ASSERT_EQ(create_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT(delete_registered);
	AtSubAbort_smgr();
	UT_ASSERT_EQ(unlink_calls, 1);
	nest_level = 1;
	locator.relNumber = 16384;
}

UT_TEST(test_temp_and_disabled_storage_keep_native_eligibility)
{
	reset();
	nest_level = 3;
	RelationCheckStorageTransaction(locator, RELPERSISTENCE_TEMP, "creation");
	cluster_enabled = false;
	RelationCheckStorageTransaction(locator, RELPERSISTENCE_PERMANENT, "creation");
	cluster_enabled = true;
	UT_ASSERT_EQ(io_calls + wal_calls + ko_calls, 0);
	UT_ASSERT(!delete_registered);
}

UT_TEST(test_shared_catalog_and_internal_create_cannot_bypass_refusal)
{
	for (int variant = 0; variant < 3; variant++)
	{
		volatile bool caught = false;

		reset();
		nest_level = 3;
		native_owner = expecting_error = true;
		cluster_shared_catalog = variant == 0;
		if (cluster_shared_catalog)
			locator.relNumber = 1249;
		PG_TRY();
		{
			RelationCreateStorage(locator,
				variant == 2 ? RELPERSISTENCE_UNLOGGED : RELPERSISTENCE_PERMANENT,
				false);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_refusal();
		UT_ASSERT_EQ(io_calls + main_create_calls + wal_calls + capture_calls, 0);
		UT_ASSERT(!delete_registered);
		locator.relNumber = 16384;
		cluster_shared_catalog = false;
	}
}

static void
check_tier_refusal(void)
{
	UT_ASSERT_EQ(reported_level, ERROR);
	UT_ASSERT_EQ(refusal_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(strstr(refusal_hint, "cluster.interconnect_tier=tier1") != NULL);
}

UT_TEST(test_rdma_tier_drop_refuses_before_any_storage_effect)
{
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		RelationData rel = { 0 };
		FormData_pg_class form = { 0 };
		volatile bool caught = false;
		reset();
		cluster_interconnect_tier = tier;
		native_owner = expecting_error = true;
		rel.rd_locator = locator;
		rel.rd_backend = InvalidBackendId;
		rel.rd_smgr = &storage;
		rel.rd_rel = &form;
		form.relpersistence = RELPERSISTENCE_PERMANENT;
		form.relkind = RELKIND_RELATION;
		storage.smgr_owner = &rel.rd_smgr;
		PG_TRY();
		{
			RelationDropStorage(&rel);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_tier_refusal();
		UT_ASSERT_EQ(ko_calls + io_calls + wal_calls + dirty_calls, 0);
		UT_ASSERT(!delete_registered);
		smgrDoPendingDeletes(false);
		UT_ASSERT_EQ(unlink_calls, 0);
	}
}

UT_TEST(test_rdma_tier_truncate_refuses_before_flush_or_structure)
{
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		Relation rel = native_truncate_relation();
		PGAlignedBlock saved[2];
		volatile bool caught = false;
		unsigned before_io = io_calls;
		memcpy(saved, pages, sizeof(saved));
		cluster_interconnect_tier = tier;
		expecting_error = true;
		PG_TRY();
		{
			RelationTruncate(rel, 4);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_tier_refusal();
		UT_ASSERT_EQ(ko_calls + relation_flushes + wal_calls + dirty_calls + truncate_calls, 0);
		UT_ASSERT_EQ(io_calls, before_io);
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		FreeFakeRelcacheEntry(rel);
	}
}

#include "access/relation.h"
#include "catalog/dependency.h"
#include "commands/event_trigger.h"
#include "cluster/cluster_mode.h"

static RelationData deletion_relations[2];
static FormData_pg_class deletion_forms[2];
static unsigned deletion_effects, deletion_events;

Relation
relation_open(Oid oid, LOCKMODE lockmode)
{
	UT_ASSERT(oid == 9100 || oid == 9101);
	UT_ASSERT_EQ(lockmode, NoLock);
	return &deletion_relations[oid - 9100];
}
void
relation_close(Relation rel, LOCKMODE lockmode)
{}
bool
trackDroppedObjectsNeeded(void)
{
	return true;
}
ObjectClass
getObjectClass(const ObjectAddress *obj)
{
	return OCLASS_CLASS;
}
bool
EventTriggerSupportsObjectClass(ObjectClass cls)
{
	return true;
}
void
EventTriggerSQLDropAddObject(const ObjectAddress *obj, bool original, bool normal)
{
	deletion_events++;
}
static void
deleteOneObject(const ObjectAddress *obj, Relation *depRel, int flags)
{
	deletion_effects++;
}

#include "test_cluster_storage_delete_gate.inc"

UT_TEST(test_rdma_drop_preflights_whole_dependency_list_before_first_deletion)
{
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		ObjectAddress refs[2]
			= { { RelationRelationId, 9100, 0 }, { RelationRelationId, 9101, 0 } };
		ObjectAddressExtra extra[2]
			= { { .flags = DEPFLAG_ORIGINAL }, { .flags = DEPFLAG_NORMAL } };
		ObjectAddresses objects = { .refs = refs, .extras = extra, .numrefs = 2, .maxrefs = 2 };
		volatile bool caught = false;
		reset();
		cluster_interconnect_tier = tier;
		deletion_events = deletion_effects = 0;
		memset(deletion_relations, 0, sizeof(deletion_relations));
		memset(deletion_forms, 0, sizeof(deletion_forms));
		for (int i = 0; i < 2; i++) {
			deletion_relations[i].rd_locator = locator;
			deletion_relations[i].rd_rel = &deletion_forms[i];
			deletion_forms[i].relkind = i == 0 ? RELKIND_RELATION : RELKIND_INDEX;
			deletion_forms[i].relpersistence
				= i == 0 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
		}
		expecting_error = true;
		PG_TRY();
		{
			deleteObjectsInList(&objects, NULL, 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_tier_refusal();
		UT_ASSERT_EQ(deletion_events + deletion_effects + io_calls + wal_calls + ko_calls, 0);
		deletion_forms[1].relpersistence = RELPERSISTENCE_TEMP;
		expecting_error = false;
		deleteObjectsInList(&objects, NULL, 0);
		UT_ASSERT_EQ(deletion_effects, 2);
		UT_ASSERT_EQ(deletion_events, 2);
		deletion_effects = deletion_events = 0;
		deleteObjectsInList(&objects, NULL, PERFORM_DELETION_CHECK_STORAGE_ONLY);
		UT_ASSERT_EQ(deletion_effects + deletion_events, 0);
	}
}

#include "access/table.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_auth_members_d.h"
#include "catalog/pg_policy_d.h"
#include "catalog/pg_shdepend.h"
#include "commands/policy.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "commands/tablecmds.h"
#include "nodes/execnodes.h"
/* PG List allocation is a boundary; the actual list algorithms are unchanged. */
static void *
list_gate_alloc(MemoryContext context, Size size)
{
	return malloc(size);
}
MemoryContext
GetMemoryChunkContext(void *pointer)
{
	return TopMemoryContext;
}
#define MemoryContextAlloc list_gate_alloc
#include "../../backend/nodes/list.c"
#undef MemoryContextAlloc

static unsigned truncate_preflight_exits, fk_steps;
static bool add_fk_target;
bool
IsCatalogRelation(Relation rel)
{
	return false;
}
Relation
table_open(Oid oid, LOCKMODE lockmode)
{
	if (oid == SharedDependRelationId) {
		UT_ASSERT_EQ(lockmode, RowExclusiveLock);
		return &deletion_relations[0];
	}
	UT_ASSERT_EQ(lockmode, AccessExclusiveLock);
	UT_ASSERT(oid == 9100 || oid == 9101);
	return &deletion_relations[oid - 9100];
}
List *
heap_truncate_find_FKs(List *ids)
{
	return add_fk_target && fk_steps++ == 0 ? list_make1_oid(9101) : NIL;
}
static void
truncate_check_rel(Oid oid, Form_pg_class form)
{}
static void
truncate_check_perms(Oid oid, Form_pg_class form)
{}
static void
truncate_check_activity(Relation rel)
{}
#include "test_cluster_storage_truncate_gate.inc"

UT_TEST(test_rdma_truncate_preflights_explicit_and_cascade_before_triggers)
{
	for (int cascade = 0; cascade < 2; cascade++) {
		List *rels, *ids;
		volatile bool caught = false;
		reset();
		cluster_interconnect_tier = cascade ? CLUSTER_IC_TIER_3 : CLUSTER_IC_TIER_2;
		fk_steps = truncate_preflight_exits = 0;
		add_fk_target = cascade;
		memset(deletion_relations, 0, sizeof(deletion_relations));
		memset(deletion_forms, 0, sizeof(deletion_forms));
		for (int i = 0; i < 2; i++) {
			deletion_relations[i].rd_id = 9100 + i;
			deletion_relations[i].rd_locator = locator;
			deletion_relations[i].rd_rel = &deletion_forms[i];
			deletion_forms[i].relkind = RELKIND_RELATION;
			deletion_forms[i].relpersistence
				= i == 0 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
		}
		rels = list_make1(&deletion_relations[0]);
		ids = list_make1_oid(9100);
		if (!cascade) {
			rels = lappend(rels, &deletion_relations[1]);
			ids = lappend_oid(ids, 9101);
		}
		expecting_error = true;
		PG_TRY();
		{
			ExecuteTruncateGuts(rels, ids, NIL, cascade ? DROP_CASCADE : DROP_RESTRICT, false,
								false);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_tier_refusal();
		UT_ASSERT_EQ(truncate_preflight_exits + io_calls + wal_calls + ko_calls, 0);
		/* The same explicit set remains supported when routed privately. */
		expecting_error = false;
		shared = false;
		fk_steps = 0;
		ExecuteTruncateGuts(rels, ids, NIL, cascade ? DROP_CASCADE : DROP_RESTRICT, false, false);
		UT_ASSERT_EQ(truncate_preflight_exits, 1);
		list_free(rels);
		list_free(ids);
	}
}

/* Execute the original ownership scan; catalog access and dependency expansion
 * are explicit boundaries.  The deletion-list gate above is the real consumer. */
static PGAlignedBlock owned_rows[3];
static HeapTupleData owned_tuples[3];
static unsigned owned_next, owned_acl_effects, owned_policy_effects;

void
table_close(Relation rel, LOCKMODE lockmode)
{}
bool
IsPinnedObject(Oid classid, Oid objectid)
{
	return false;
}
char *
getObjectDescription(const ObjectAddress *obj, bool missing_ok)
{
	return "fixture role";
}
void
ScanKeyInit(ScanKey key, AttrNumber attno, StrategyNumber strategy, RegProcedure proc, Datum arg)
{}
SysScanDesc
systable_beginscan(Relation rel, Oid index, bool indexok, Snapshot snap, int nkeys, ScanKey key)
{
	owned_next = 0;
	return (SysScanDesc)&owned_next;
}
HeapTuple
systable_getnext(SysScanDesc scan)
{
	return owned_next < lengthof(owned_tuples) ? &owned_tuples[owned_next++] : NULL;
}
bool
systable_recheck_tuple(SysScanDesc scan, HeapTuple tuple)
{
	return true;
}
void
systable_endscan(SysScanDesc scan)
{}
void
AcquireDeletionLock(const ObjectAddress *obj, int flags)
{}
void
ReleaseDeletionLock(const ObjectAddress *obj)
{}
void
RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objectid)
{
	owned_acl_effects++;
}
bool
RemoveRoleFromObjectPolicy(Oid roleid, Oid classid, Oid objectid)
{
	owned_policy_effects++;
	return false;
}
ObjectAddresses *
new_object_addresses(void)
{
	ObjectAddresses *objects = calloc(1, sizeof(*objects));
	objects->refs = calloc(3, sizeof(*objects->refs));
	objects->extras = calloc(3, sizeof(*objects->extras));
	objects->maxrefs = 3;
	return objects;
}
void
add_exact_object_address(const ObjectAddress *obj, ObjectAddresses *objects)
{
	UT_ASSERT(objects->numrefs < objects->maxrefs);
	objects->extras[objects->numrefs].flags = DEPFLAG_ORIGINAL;
	objects->refs[objects->numrefs++] = *obj;
}
void
sort_object_addresses(ObjectAddresses *objects)
{}
void
free_object_addresses(ObjectAddresses *objects)
{
	pfree(objects->refs);
	pfree(objects->extras);
	pfree(objects);
}
void
performMultipleDeletions(const ObjectAddresses *objects, DropBehavior behavior, int flags)
{
	deleteObjectsInList((ObjectAddresses *)objects, NULL, flags);
}
#include "test_cluster_storage_owned_gate.inc"

UT_TEST(test_rdma_drop_owned_refuses_before_acl_or_policy_changes)
{
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		List *roles = list_make1_oid(9200);
		volatile bool caught = false;
		reset();
		cluster_interconnect_tier = tier;
		owned_acl_effects = owned_policy_effects = deletion_effects = deletion_events = 0;
		memset(owned_rows, 0, sizeof(owned_rows));
		for (int i = 0; i < 3; i++) {
			Form_pg_shdepend row;
			owned_tuples[i].t_data = (HeapTupleHeader)owned_rows[i].data;
			owned_tuples[i].t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
			row = (Form_pg_shdepend)GETSTRUCT(&owned_tuples[i]);
			row->dbid = MyDatabaseId;
			row->classid = i == 1 ? PolicyRelationId : RelationRelationId;
			row->objid = 9101;
			row->deptype = i == 0	? SHARED_DEPENDENCY_ACL
						   : i == 1 ? SHARED_DEPENDENCY_POLICY
									: SHARED_DEPENDENCY_OWNER;
		}
		memset(deletion_relations, 0, sizeof(deletion_relations));
		memset(deletion_forms, 0, sizeof(deletion_forms));
		deletion_relations[1].rd_locator = locator;
		deletion_relations[1].rd_rel = &deletion_forms[1];
		deletion_forms[1].relkind = RELKIND_RELATION;
		deletion_forms[1].relpersistence = RELPERSISTENCE_PERMANENT;
		expecting_error = true;
		PG_TRY();
		{
			shdepDropOwned(roles, DROP_CASCADE);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_tier_refusal();
		UT_ASSERT_EQ(owned_acl_effects + owned_policy_effects + deletion_effects + deletion_events,
					 0);
		/* Private objects keep the original ACL, policy and deletion work. */
		shared = false;
		expecting_error = false;
		shdepDropOwned(roles, DROP_CASCADE);
		UT_ASSERT_EQ(owned_acl_effects, 1);
		UT_ASSERT_EQ(owned_policy_effects, 1);
		UT_ASSERT_EQ(deletion_effects, 2);
		list_free(roles);
	}
}

UT_TEST(test_storage_drain_tier_gate_preserves_temp_private_and_disabled)
{
	for (int tier = CLUSTER_IC_TIER_2; tier <= CLUSTER_IC_TIER_3; tier++) {
		reset();
		cluster_interconnect_tier = tier;
		RelationCheckStorageDrain(locator, RELPERSISTENCE_TEMP, "deletion");
		shared = false;
		RelationCheckStorageDrain(locator, RELPERSISTENCE_PERMANENT, "truncation");
		shared = true;
		cluster_enabled = false;
		RelationCheckStorageDrain(locator, RELPERSISTENCE_PERMANENT, "deletion");
		cluster_enabled = true;
		cluster_interconnect_tier = CLUSTER_IC_TIER_1;
		RelationCheckStorageDrain(locator, RELPERSISTENCE_PERMANENT, "truncation");
		UT_ASSERT_EQ(reported_level + io_calls + wal_calls + ko_calls, 0);
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(13);
	UT_RUN(test_child_create_refuses_before_storage_or_abort_owner);
	UT_RUN(test_child_drop_refuses_before_ko_or_pending_delete);
	UT_RUN(test_child_truncate_refuses_before_flush_or_structure);
	UT_RUN(test_private_child_create_keeps_native_cleanup);
	UT_RUN(test_temp_and_disabled_storage_keep_native_eligibility);
	UT_RUN(test_shared_catalog_and_internal_create_cannot_bypass_refusal);
	UT_RUN(test_native_create_registers_abort_cleanup_before_space);
	UT_RUN(test_rdma_tier_drop_refuses_before_any_storage_effect);
	UT_RUN(test_rdma_tier_truncate_refuses_before_flush_or_structure);
	UT_RUN(test_rdma_drop_preflights_whole_dependency_list_before_first_deletion);
	UT_RUN(test_rdma_truncate_preflights_explicit_and_cascade_before_triggers);
	UT_RUN(test_rdma_drop_owned_refuses_before_acl_or_policy_changes);
	UT_RUN(test_storage_drain_tier_gate_preserves_temp_private_and_disabled);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
