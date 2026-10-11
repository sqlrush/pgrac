/*-------------------------------------------------------------------------
 *
 * test_cluster_snapshot_vacuum_fixture.inc
 *    Catalogue I/O leaves for the actual database maintenance entry.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_snapshot_vacuum_fixture.inc
 *
 * NOTES
 *    The included snapshot manager and admission functions are production
 *    code. Tuple storage, locks and the ProcArray xid scan are fixture leaves;
 *    cluster snapshot fields use the original producer. This fixture does not
 *    claim to test heap visibility, cross-node locks or physical truncation.
 *-------------------------------------------------------------------------
 */
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/table.h"
#include "catalog/pg_database.h"
#include "commands/vacuum.h"
#include "storage/lmgr.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

bool cluster_shared_catalog = true;
int XactIsoLevel = XACT_READ_COMMITTED;
Oid MyDatabaseId = 42;

typedef enum VacuumFault {
	VAC_FAULT_NONE,
	VAC_FAULT_SNAPSHOT,
	VAC_FAULT_LOCK,
	VAC_FAULT_SCAN,
	VAC_FAULT_INPLACE,
	VAC_FAULT_FINISH,
	VAC_FAULT_EMPTY,
	VAC_FAULT_WRONG_TOP
} VacuumFault;

static VacuumFault vacuum_fault;
static int vacuum_injected_code;
static unsigned vacuum_pushes, vacuum_pops, vacuum_begins, vacuum_finishes;
static unsigned vacuum_cancels, vacuum_truncates, vacuum_snapshots;
static bool vacuum_no_tables;
static bool vacuum_class_returned;
static Snapshot vacuum_seen_active;
static struct RelationData vacuum_relation;
static HeapTupleData vacuum_class_tuple, vacuum_database_tuple;
static union {
	max_align_t alignment;
	char bytes[MAXALIGN(SizeofHeapTupleHeader) + sizeof(FormData_pg_class)];
} vacuum_class_storage;
static union {
	max_align_t alignment;
	char bytes[MAXALIGN(SizeofHeapTupleHeader) + sizeof(FormData_pg_database)];
} vacuum_database_storage;

static void
vacuum_inject(VacuumFault point)
{
	if (vacuum_fault == point)
		ereport(ERROR, (errcode(vacuum_injected_code), errmsg("injected maintenance failure"),
						errhint("Abort the test transaction.")));
}

/* Only the ProcArray scan is replaced; identity stamping is the real leaf. */
Snapshot
GetSnapshotData(Snapshot snapshot)
{
	vacuum_inject(VAC_FAULT_SNAPSHOT);
	vacuum_snapshots++;
	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->snapshot_type = SNAPSHOT_MVCC;
	snapshot->xmin = 10;
	snapshot->xmax = 200;
	ClusterSnapshotRefreshFields(snapshot);
	MyProc->xmin = snapshot->xmin;
	return snapshot;
}

Snapshot
GetSerializableTransactionSnapshot(Snapshot snapshot)
{
	/* Maintenance does not select serializable isolation in this fixture. */
	abort();
}

void
LockDatabaseFrozenIds(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	vacuum_seen_active = ActiveSnapshotSet() ? GetActiveSnapshot() : NULL;
	vacuum_inject(VAC_FAULT_LOCK);
}

TransactionId
GetOldestNonRemovableTransactionId(Relation relation)
{
	return 100;
}

MultiXactId
GetOldestMultiXactId(void)
{
	return 100;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(200);
}

MultiXactId
ReadNextMultiXactId(void)
{
	return 200;
}

bool
MultiXactIdPrecedes(MultiXactId a, MultiXactId b)
{
	return (int32)(a - b) < 0;
}

bool
ForceTransactionIdLimitUpdate(void)
{
	return false;
}

Relation
table_open(Oid oid, LOCKMODE mode)
{
	UT_ASSERT(oid == RelationRelationId || oid == DatabaseRelationId);
	return &vacuum_relation;
}

void
table_close(Relation relation, LOCKMODE mode)
{
	UT_ASSERT(relation == &vacuum_relation);
}

SysScanDesc
systable_beginscan(Relation relation, Oid index, bool index_ok, Snapshot snapshot, int nkeys,
				   ScanKey keys)
{
	UT_ASSERT(snapshot == NULL);
	vacuum_inject(VAC_FAULT_SCAN);
	return (SysScanDesc)&vacuum_relation;
}

HeapTuple
systable_getnext(SysScanDesc scan)
{
	if (vacuum_class_returned || vacuum_no_tables)
		return NULL;
	vacuum_class_returned = true;
	return &vacuum_class_tuple;
}

void
systable_endscan(SysScanDesc scan)
{}

void
ScanKeyInit(ScanKey entry, AttrNumber attribute, StrategyNumber strategy, RegProcedure procedure,
			Datum argument)
{
	memset(entry, 0, sizeof(*entry));
}

void
systable_inplace_update_begin(Relation relation, Oid index, bool index_ok, Snapshot snapshot,
							  int nkeys, const ScanKeyData *keys, HeapTuple *tuple, void **state)
{
	UT_ASSERT(snapshot == NULL);
	vacuum_begins++;
	vacuum_inject(VAC_FAULT_INPLACE);
	/* A foreign tuple's terminal read consumes the actual original gate. */
	if (cluster_storage_mode_enabled() && cluster_shared_catalog) {
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(vacuum_database_tuple.t_data) % 16, 0);
		UT_ASSERT(cluster_node_id != 0);
		(void)cluster_undo_horizon_read_admission_enforce(InvalidScn);
	}
	*tuple = &vacuum_database_tuple;
	*state = &vacuum_relation;
}

void
systable_inplace_update_finish(void *state, HeapTuple tuple)
{
	vacuum_inject(VAC_FAULT_FINISH);
	vacuum_finishes++;
}

void
systable_inplace_update_cancel(void *state)
{
	vacuum_cancels++;
}

void
heap_freetuple(HeapTuple tuple)
{
	UT_ASSERT(tuple == &vacuum_database_tuple);
}

static void
vac_truncate_clog(TransactionId frozen, MultiXactId multi, TransactionId sane,
				  MultiXactId sane_multi)
{
	vacuum_truncates++;
	if (vacuum_fault == VAC_FAULT_EMPTY || vacuum_fault == VAC_FAULT_WRONG_TOP) {
		if (vacuum_fault == VAC_FAULT_EMPTY && ActiveSnapshotSet())
			PopActiveSnapshot();
		if (vacuum_fault == VAC_FAULT_WRONG_TOP) {
			Snapshot other = registered(90);
			PushActiveSnapshot(other);
			UnregisterSnapshot(other);
		}
	}
}

/* Observe calls, without replacing production stack or retention behavior. */
static void
vacuum_count_push(Snapshot snapshot)
{
	vacuum_pushes++;
	PushActiveSnapshot(snapshot);
	UT_ASSERT_EQ(GetActiveSnapshot()->cluster_source, SNAPSHOT_SOURCE_CLUSTER);
	UT_ASSERT_EQ(GetActiveSnapshot()->read_epoch, cluster_epoch_get_current());
	UT_ASSERT_EQ(GetActiveSnapshot()->active_count, 1);
}

static void
vacuum_count_pop(void)
{
	vacuum_pops++;
	PopActiveSnapshot();
}

#define PushActiveSnapshot vacuum_count_push
#define PopActiveSnapshot vacuum_count_pop
#include "test_cluster_snapshot_admission_vacuum.inc"
#undef PushActiveSnapshot
#undef PopActiveSnapshot

static void
vacuum_reset(void)
{
	Form_pg_class relation;
	Form_pg_database database;

	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	AtEOXact_Snapshot(false, true);
	cluster_enabled = cluster_shared_catalog = true;
	vacuum_fault = VAC_FAULT_NONE;
	vacuum_injected_code = ERRCODE_QUERY_CANCELED;
	vacuum_pushes = vacuum_pops = vacuum_begins = vacuum_finishes = 0;
	vacuum_cancels = vacuum_truncates = vacuum_snapshots = 0;
	vacuum_no_tables = false;
	vacuum_class_returned = false;
	vacuum_seen_active = NULL;
	memset(&vacuum_class_storage, 0, sizeof(vacuum_class_storage));
	memset(&vacuum_database_storage, 0, sizeof(vacuum_database_storage));
	vacuum_class_tuple.t_data = (HeapTupleHeader)&vacuum_class_storage;
	vacuum_database_tuple.t_data = (HeapTupleHeader)&vacuum_database_storage;
	vacuum_class_tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	vacuum_database_tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(vacuum_database_tuple.t_data, 32);
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	relation->relkind = RELKIND_RELATION;
	relation->relfrozenxid = relation->relminmxid = 80;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datfrozenxid = database->datminmxid = 40;
}

static int
vacuum_call(void)
{
	volatile int result = 0;

	PG_TRY();
	{
		vac_update_datfrozenxid();
	}
	PG_CATCH();
	{
		result = error_code;
	}
	PG_END_TRY();
	return result;
}

static void
vacuum_abort(void)
{
	AtEOXact_Snapshot(false, true);
	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 0);
	UT_ASSERT_EQ(proc.xmin, InvalidTransactionId);
}

UT_TEST(vacuum_foreign_terminal_read_has_owned_snapshot)
{
	vacuum_reset();
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(vacuum_pushes, 1);
	UT_ASSERT_EQ(vacuum_pops, 1);
	UT_ASSERT(vacuum_seen_active != NULL);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	UT_ASSERT_EQ(vacuum_truncates, 1);
	UT_ASSERT(!ActiveSnapshotSet());
	vacuum_abort();
}

UT_TEST(vacuum_registered_only_is_not_terminal_admission)
{
	Snapshot retained;

	vacuum_reset();
	(void)GetTransactionSnapshot();
	retained = registered(70);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 70);
	UT_ASSERT(admission_rejected(InvalidScn));
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(vacuum_pushes, 1);
	UT_ASSERT_EQ(vacuum_pops, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 70);
	UT_ASSERT(admission_rejected(InvalidScn));
	UnregisterSnapshot(retained);
	vacuum_abort();
}

UT_TEST(vacuum_early_and_clean_returns_balance_ownership)
{
	for (int early = 0; early < 2; early++) {
		vacuum_reset();
		if (early)
			((Form_pg_class)GETSTRUCT(&vacuum_class_tuple))->relfrozenxid = 300;
		else {
			Form_pg_database db = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
			db->datfrozenxid = db->datminmxid = 80;
		}
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(vacuum_pushes, 1);
		UT_ASSERT_EQ(vacuum_pops, 1);
		UT_ASSERT_EQ(vacuum_finishes, 0);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		UT_ASSERT_EQ(vacuum_cancels, early ? 0 : 1);
		vacuum_abort();
	}
}

UT_TEST(vacuum_borrowed_snapshot_keeps_identity)
{
	Snapshot borrowed;

	vacuum_reset();
	borrowed = registered(70);
	PushActiveSnapshot(borrowed);
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT(GetActiveSnapshot() == borrowed && vacuum_seen_active == borrowed);
	UT_ASSERT_EQ(vacuum_pushes + vacuum_pops + vacuum_snapshots, 0);
	UT_ASSERT_EQ(borrowed->read_scn, 70);
	UT_ASSERT_EQ(borrowed->read_epoch, 7);
	PopActiveSnapshot();
	UnregisterSnapshot(borrowed);
	vacuum_abort();
}

UT_TEST(vacuum_errors_use_original_abort_cleanup)
{
	for (int point = VAC_FAULT_SNAPSHOT; point <= VAC_FAULT_FINISH; point++) {
		vacuum_reset();
		vacuum_fault = point;
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(vacuum_pops, 0);
		UT_ASSERT_EQ(vacuum_pushes, point == VAC_FAULT_SNAPSHOT ? 0 : 1);
		UT_ASSERT(ActiveSnapshotSet() == (point != VAC_FAULT_SNAPSHOT));
		vacuum_abort();
	}
}

UT_TEST(vacuum_misplaced_owned_snapshot_is_runtime_error)
{
	for (int point = VAC_FAULT_EMPTY; point <= VAC_FAULT_WRONG_TOP; point++) {
		vacuum_reset();
		vacuum_fault = point;
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(vacuum_pops, 0);
		UT_ASSERT(ActiveSnapshotSet() == (point == VAC_FAULT_WRONG_TOP));
		vacuum_abort();
	}
}

UT_TEST(vacuum_nontarget_modes_keep_original_body)
{
	for (int mode = 0; mode < 2; mode++) {
		vacuum_reset();
		if (mode == 0)
			cluster_enabled = false;
		else
			cluster_shared_catalog = false;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(vacuum_pushes + vacuum_pops + vacuum_snapshots, 0);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		UT_ASSERT_EQ(vacuum_truncates, 1);
		vacuum_abort();
	}
}

UT_TEST(vacuum_direct_worker_entry_needs_no_table_snapshot)
{
	vacuum_reset();
	vacuum_no_tables = true;
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(vacuum_pushes, 1);
	UT_ASSERT_EQ(vacuum_pops, 1);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	vacuum_abort();
}

UT_TEST(vacuum_stale_borrowed_snapshot_is_not_refreshed)
{
	Snapshot borrowed;

	vacuum_reset();
	borrowed = registered(70);
	borrowed->read_epoch = 6;
	PushActiveSnapshot(borrowed);
	UT_ASSERT_EQ(vacuum_call(), ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
	UT_ASSERT(GetActiveSnapshot() == borrowed);
	UT_ASSERT_EQ(vacuum_pushes + vacuum_pops + vacuum_snapshots, 0);
	UT_ASSERT_EQ(borrowed->read_epoch, 6);
	PopActiveSnapshot();
	UnregisterSnapshot(borrowed);
	vacuum_abort();
}
