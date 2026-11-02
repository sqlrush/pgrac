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
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "commands/vacuum.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

bool cluster_shared_catalog = true;
bool cluster_xid_striping = true;
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
static unsigned vacuum_class_read, vacuum_class_count;
static unsigned vacuum_sample_seen_at_scan;
static bool vacuum_revoke_on_scan_end;
static TransactionId vacuum_truncate_sane;
static TransactionId vacuum_next_xid;
static TransactionId vacuum_oldest_xid;
static TransactionId vacuum_inplace_frozenxid;
static VariableCacheData vacuum_cache;
VariableCache ShmemVariableCache = &vacuum_cache;
static LWLockPadded vacuum_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = vacuum_locks;
static bool vacuum_xid_lock, vacuum_inplace_lock;
static bool vacuum_promise_ready, vacuum_promise_current, vacuum_revoke_on_release;
static uint64 vacuum_legal_next;
static unsigned vacuum_sample_reads, vacuum_fence_reads;
static bool vacuum_f_ready, vacuum_f_zero, vacuum_revoke_in_f_sample;
static uint64 vacuum_f_value;
static unsigned vacuum_f_reads;

/* The real global sampler observes peer/worker state. Keep that boundary
 * explicit: it must never run under allocator or inplace/content locks. */
bool
cluster_heap_freeze_watermark_sample(ClusterHeapFreezeWatermark *out)
{
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	vacuum_f_reads++;
	if (vacuum_revoke_in_f_sample)
		vacuum_promise_current = false;
	memset(out, 0, sizeof(*out));
	if (!vacuum_f_ready)
		return false;
	out->active_full_xid_min
		= vacuum_f_zero ? 0 : (vacuum_f_value ? vacuum_f_value : vacuum_legal_next);
	out->future_full_xid_max = out->active_full_xid_min;
	out->horizon_scn = 70;
	out->publication_sequence = out->f_sample_id = out->published_at_us = 1;
	out->record_generation = out->formation_epoch = 7;
	out->admitted_members_lo = 3;
	return true;
}
static Snapshot vacuum_seen_active;
static struct RelationData vacuum_relation;
static HeapTupleData vacuum_class_tuple, vacuum_database_tuple;
typedef union VacuumClassStorage {
	max_align_t alignment;
	char bytes[MAXALIGN(SizeofHeapTupleHeader) + sizeof(FormData_pg_class)];
} VacuumClassStorage;
static VacuumClassStorage vacuum_class_storage, vacuum_extra_class_storage[2];
static HeapTupleData vacuum_extra_class_tuple[2];
static union {
	max_align_t alignment;
	char bytes[MAXALIGN(SizeofHeapTupleHeader) + sizeof(FormData_pg_database)];
} vacuum_database_storage;

/* Durable lease and locks are leaves; FullXid widening is linked product code. */
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == ProcArrayLock) {
		if (mode == LW_SHARED)
			UT_ASSERT(import_install_active && !import_restored);
		else
			UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
		if (import_probe_after_install || import_observe_install)
			import_install_mode = mode;
		UT_ASSERT(!retention_lock && !vacuum_xid_lock && !vacuum_inplace_lock);
		retention_lock = true;
		retention_acquires++;
		return true;
	}
	UT_ASSERT(lock == XidGenLock && mode == LW_SHARED);
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	vacuum_xid_lock = true;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == ProcArrayLock) {
		UT_ASSERT(retention_lock);
		if (import_observe_install) {
			import_scn_at_unlock = pg_atomic_read_u64(&proc.cluster_read_scn_atomic);
			import_observe_install = false;
		}
		retention_lock = false;
		retention_releases++;
		snapshot_import_after_procarray_unlock();
		return;
	}
	UT_ASSERT(lock == XidGenLock && vacuum_xid_lock);
	vacuum_xid_lock = false;
	if (vacuum_revoke_on_release)
		vacuum_promise_current = false;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(vacuum_xid_lock && !vacuum_inplace_lock);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), vacuum_next_xid);
	vacuum_sample_reads++;
	*future = InvalidFullTransactionId;
	memset(fence, 0, sizeof(*fence));
	if (!vacuum_promise_ready)
		return false;
	*future = FullTransactionIdFromU64(vacuum_legal_next);
	fence->incarnation = 81;
	return true;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	UT_ASSERT_EQ(fence->incarnation, 81);
	vacuum_fence_reads++;
	return vacuum_promise_current;
}

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
	/* Native GetSnapshotData owns reusable XID storage before import copies.
	 * These arrays replace allocation/storage only, not source visibility. */
	if (import_install_active)
	{
		static TransactionId import_xip[16], import_subxip[16];

		snapshot->xip = import_xip;
		snapshot->subxip = import_subxip;
	}
	ClusterSnapshotRefreshFields(snapshot);
	MyProc->xmin = snapshot->xmin;
	/* Schedule an actual H scan in the original unpublished-reader gap.
	 * Its decision and output come from the product sampler, not this leaf. */
	if (retention_probe_before_publish) {
		ClusterUndoRetentionSample sample, zero = { 0 };

		memset(&sample, 0xa5, sizeof(sample));
		retention_gap_refused = !cluster_undo_retention_horizon_sample(&sample)
			&& memcmp(&sample, &zero, sizeof(sample)) == 0;
	}
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
	return vacuum_oldest_xid;
}

MultiXactId
GetOldestMultiXactId(void)
{
	return 100;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(vacuum_next_xid);
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
	vacuum_sample_seen_at_scan = vacuum_sample_reads;
	return (SysScanDesc)&vacuum_relation;
}

HeapTuple
systable_getnext(SysScanDesc scan)
{
	if (vacuum_class_read >= vacuum_class_count || vacuum_no_tables)
		return NULL;
	if (vacuum_class_read++ == 0)
		return &vacuum_class_tuple;
	return &vacuum_extra_class_tuple[vacuum_class_read - 2];
}

void
systable_endscan(SysScanDesc scan)
{
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	if (vacuum_revoke_on_scan_end)
		vacuum_promise_current = false;
}

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
	if (index == DatabaseOidIndexId && cluster_storage_mode_enabled() && cluster_shared_catalog) {
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(vacuum_database_tuple.t_data) % 16, 0);
		UT_ASSERT(cluster_node_id != 0);
		(void)cluster_undo_horizon_read_admission_enforce(InvalidScn);
	}
	if (index == ClassOidIndexId) {
		*tuple = &vacuum_class_tuple;
		if (TransactionIdIsValid(vacuum_inplace_frozenxid))
			((Form_pg_class)GETSTRUCT(*tuple))->relfrozenxid = vacuum_inplace_frozenxid;
	} else {
		UT_ASSERT_EQ(index, DatabaseOidIndexId);
		*tuple = &vacuum_database_tuple;
		if (TransactionIdIsValid(vacuum_inplace_frozenxid))
			((Form_pg_database)GETSTRUCT(*tuple))->datfrozenxid = vacuum_inplace_frozenxid;
	}
	*state = &vacuum_relation;
	vacuum_inplace_lock = true;
}

void
systable_inplace_update_finish(void *state, HeapTuple tuple)
{
	vacuum_inject(VAC_FAULT_FINISH);
	vacuum_finishes++;
	vacuum_inplace_lock = false;
}

void
systable_inplace_update_cancel(void *state)
{
	vacuum_cancels++;
	vacuum_inplace_lock = false;
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
	vacuum_truncate_sane = sane;
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
	/* The following independent maintenance setup starts a fresh boot. */
	procArray->cluster_read_scn_floor = InvalidScn;
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	vacuum_fault = VAC_FAULT_NONE;
	vacuum_injected_code = ERRCODE_QUERY_CANCELED;
	vacuum_pushes = vacuum_pops = vacuum_begins = vacuum_finishes = 0;
	vacuum_cancels = vacuum_truncates = vacuum_snapshots = 0;
	vacuum_no_tables = false;
	vacuum_class_read = vacuum_sample_seen_at_scan = 0;
	vacuum_class_count = 1;
	vacuum_truncate_sane = InvalidTransactionId;
	vacuum_revoke_on_scan_end = false;
	vacuum_next_xid = 200;
	vacuum_oldest_xid = 100;
	vacuum_cache.nextXid = FullTransactionIdFromU64(200);
	ShmemVariableCache = &vacuum_cache;
	vacuum_legal_next = 200;
	vacuum_xid_lock = vacuum_inplace_lock = false;
	vacuum_promise_ready = vacuum_promise_current = true;
	vacuum_revoke_on_release = false;
	vacuum_sample_reads = vacuum_fence_reads = vacuum_f_reads = 0;
	vacuum_f_ready = true;
	vacuum_f_zero = vacuum_revoke_in_f_sample = false;
	vacuum_f_value = 0;
	vacuum_inplace_frozenxid = InvalidTransactionId;
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
	vacuum_relation.rd_id = 43;
	vacuum_relation.rd_rel = relation;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datfrozenxid = database->datminmxid = 40;
}

/* Add bounded catalogue rows; the production scan decides their horizons. */
static void
vacuum_add_class(TransactionId frozen, MultiXactId multi)
{
	HeapTuple tuple;
	Form_pg_class relation;
	unsigned index = vacuum_class_count - 1;

	UT_ASSERT(index < lengthof(vacuum_extra_class_tuple));
	memset(&vacuum_extra_class_storage[index], 0, sizeof(VacuumClassStorage));
	tuple = &vacuum_extra_class_tuple[index];
	tuple->t_data = (HeapTupleHeader)&vacuum_extra_class_storage[index];
	tuple->t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	relation = (Form_pg_class)GETSTRUCT(tuple);
	relation->relkind = RELKIND_RELATION;
	relation->relfrozenxid = frozen;
	relation->relminmxid = multi;
	vacuum_class_count++;
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
	/* AbortTransaction releases LWLocks/UnlockBuffers before the original
	 * CleanupTransaction invokes AtEOXact_Snapshot. Model only that boundary. */
	vacuum_xid_lock = vacuum_inplace_lock = false;
	AtEOXact_Snapshot(false, true);
	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 0);
	UT_ASSERT_EQ(proc.xmin, InvalidTransactionId);
}

/* Execute the actual consumer, preserving production ERROR/SQLSTATE. */
static int
vacuum_relstats_try(Relation relation, BlockNumber pages, double tuples,
					BlockNumber visible, bool indexes, TransactionId candidate,
					MultiXactId multi, bool *updated, bool *multi_updated, bool outer)
{
	volatile int result = 0;

	PG_TRY();
	{
		vac_update_relstats(relation, pages, tuples, visible, indexes, candidate,
							multi, updated, multi_updated, outer);
	}
	PG_CATCH();
	{
		result = error_code;
	}
	PG_END_TRY();
	return result;
}

static int
vacuum_relstats_call(TransactionId candidate)
{
	return vacuum_relstats_try(&vacuum_relation, 5, 20, 2, false, candidate,
							  InvalidMultiXactId, NULL, NULL, false);
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
		UT_ASSERT_EQ(vacuum_call(), early ? ERRCODE_DATA_CORRUPTED : 0);
		UT_ASSERT_EQ(vacuum_pushes, 1);
		UT_ASSERT_EQ(vacuum_pops, !early);
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

/* The real inplace entry sees the new tuple, after the caller's older scan. */
UT_TEST(vacuum_shared_relstats_keeps_newer_locked_frozen_xid)
{
	for (int newer = 0; newer < 2; newer++) {
		Form_pg_class relation;
		bool frozen_updated = true, multi_updated = true;

		vacuum_reset();
		vacuum_legal_next = 400;
		vacuum_inplace_frozenxid = newer ? 300 : 150;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		vac_update_relstats(&vacuum_relation, 5, 20, 2, false, 80, InvalidMultiXactId,
							&frozen_updated, &multi_updated, false);
		UT_ASSERT_EQ(relation->relfrozenxid, vacuum_inplace_frozenxid);
		UT_ASSERT(!frozen_updated && !multi_updated);
		UT_ASSERT_EQ(relation->relpages, 5);
		UT_ASSERT_EQ(relation->reltuples, 20);
		UT_ASSERT_EQ(relation->relallvisible, 2);
		UT_ASSERT_EQ(relation->relminmxid, 80);
		UT_ASSERT_EQ(vacuum_begins, 1);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_shared_database_keeps_newer_locked_frozen_xid)
{
	for (int newer = 0; newer < 2; newer++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_legal_next = 400;
		vacuum_inplace_frozenxid = newer ? 300 : 150;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 80;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(database->datfrozenxid, vacuum_inplace_frozenxid);
		UT_ASSERT_EQ(database->datminmxid, 80);
		UT_ASSERT_EQ(vacuum_begins, 1);
		UT_ASSERT_EQ(vacuum_finishes, 0);
		UT_ASSERT_EQ(vacuum_cancels, 1);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_shared_relstats_keeps_invalid_and_equal_candidates)
{
	for (int equal = 0; equal < 2; equal++) {
		Form_pg_class relation;
		bool frozen_updated = true;

		vacuum_reset();
		vacuum_legal_next = 400;
		vacuum_inplace_frozenxid = 300;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		vac_update_relstats(&vacuum_relation, 0, 0, 0, false, equal ? 300 : InvalidTransactionId,
							InvalidMultiXactId, &frozen_updated, NULL, false);
		UT_ASSERT_EQ(relation->relfrozenxid, 300);
		UT_ASSERT(!frozen_updated);
		UT_ASSERT_EQ(vacuum_finishes, 0);
		UT_ASSERT_EQ(vacuum_cancels, 1);
		vacuum_abort();
	}
}

UT_TEST(vacuum_shared_relstats_retains_native_wrap_order)
{
	for (int old_epoch = 0; old_epoch < 2; old_epoch++) {
		TransactionId candidate = old_epoch ? 20 : 0xffffff00U;
		TransactionId current = old_epoch ? 0xffffff00U : 20;
		Form_pg_class relation;
		bool frozen_updated = false;

		vacuum_reset();
		vacuum_next_xid = old_epoch ? 30 : 0xfffffff0U;
		vacuum_cache.nextXid = FullTransactionIdFromU64(vacuum_next_xid);
		vacuum_legal_next = old_epoch ? UINT64CONST(0x10000001e) : UINT64CONST(0xfffffff0);
		vacuum_inplace_frozenxid = current;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, candidate,
									   InvalidMultiXactId, &frozen_updated, NULL, false),
					 old_epoch ? 0 : ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(relation->relfrozenxid, old_epoch ? candidate : current);
		UT_ASSERT_EQ(frozen_updated, old_epoch);
		UT_ASSERT_EQ(vacuum_finishes, old_epoch);
		UT_ASSERT_EQ(vacuum_cancels, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_shared_frozen_merge_keeps_independent_mx_path)
{
	Form_pg_class relation;
	Form_pg_database database;
	bool frozen_updated = true, multi_updated = false;

	vacuum_reset();
	vacuum_legal_next = 400;
	vacuum_inplace_frozenxid = 300;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	relation->relminmxid = 300;
	vac_update_relstats(&vacuum_relation, 0, 0, 0, false, 80, 80, &frozen_updated, &multi_updated,
						false);
	UT_ASSERT_EQ(relation->relfrozenxid, 300);
	UT_ASSERT_EQ(relation->relminmxid, 80);
	UT_ASSERT(!frozen_updated && multi_updated);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	vacuum_abort();

	vacuum_reset();
	vacuum_legal_next = 400;
	vacuum_inplace_frozenxid = 300;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datminmxid = 300;
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(database->datfrozenxid, 300);
	UT_ASSERT_EQ(database->datminmxid, 80);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	UT_ASSERT_EQ(vacuum_truncates, 1);
	vacuum_abort();
}

UT_TEST(vacuum_nontarget_frozen_merge_keeps_native_repair)
{
	for (int mode = 0; mode < 2; mode++) {
		Form_pg_class relation;
		Form_pg_database database;
		bool frozen_updated = false;

		vacuum_reset();
		if (mode == 0)
			cluster_enabled = false;
		else
			cluster_shared_catalog = false;
		vacuum_inplace_frozenxid = 300;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		vac_update_relstats(&vacuum_relation, 0, 0, 0, false, 80, InvalidMultiXactId,
							&frozen_updated, NULL, false);
		UT_ASSERT_EQ(relation->relfrozenxid, 80);
		UT_ASSERT(frozen_updated);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		vacuum_class_read = vacuum_sample_seen_at_scan = 0;
		vacuum_class_count = 1;
		vacuum_revoke_on_scan_end = false;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 80;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(database->datfrozenxid, 80);
		UT_ASSERT_EQ(vacuum_finishes, 2);
		UT_ASSERT_EQ(vacuum_truncates, 1);
		UT_ASSERT_EQ(vacuum_sample_reads + vacuum_fence_reads, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_relstats_rejects_future_candidate)
{
	bool updated = true;
	Form_pg_class relation;

	vacuum_reset();
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 201, InvalidMultiXactId,
						&updated, NULL, false), ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(relation->relfrozenxid, 80);
	/* ERROR has no successful catalogue update. */
	UT_ASSERT_EQ(vacuum_finishes, 0);
	UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_rejects_future_locked_current)
{
	bool updated = true;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_inplace_frozenxid = 250;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 260, InvalidMultiXactId,
						&updated, NULL, false), ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(relation->relfrozenxid, 250);
	/* ERROR has no successful catalogue update. */
	UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_rejects_half_space_current)
{
	bool updated = true;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_legal_next = UINT64CONST(0x1000000c8);
	vacuum_inplace_frozenxid = 0x800000c8U;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 0x800000c9U, InvalidMultiXactId,
						&updated, NULL, false), ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(relation->relfrozenxid, 0x800000c8U);
	/* ERROR has no successful catalogue update. */
	UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_missing_promise_preserves_xid)
{
	bool updated = true;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_promise_ready = false;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 120, InvalidMultiXactId,
						&updated, NULL, false), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(relation->relfrozenxid, 80);
	/* ERROR has no successful catalogue update. */
	UT_ASSERT_EQ(vacuum_sample_reads, 1);
	UT_ASSERT_EQ(vacuum_fence_reads, 0);
	UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_revoked_promise_preserves_xid)
{
	bool updated = true;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_revoke_on_release = true;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 120, InvalidMultiXactId,
						&updated, NULL, false), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(relation->relfrozenxid, 80);
	/* ERROR has no successful catalogue update. */
	UT_ASSERT_EQ(vacuum_sample_reads, 1);
	UT_ASSERT_EQ(vacuum_fence_reads, 1);
	UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_invalid_reference_and_current_preserve_xid)
{
	for (int invalid = 0; invalid < 4; invalid++) {
		bool updated = true;
		Form_pg_class relation;

		vacuum_reset();
		if (invalid == 0)
			vacuum_legal_next = 0;
		else if (invalid == 1)
			vacuum_legal_next = UINT64CONST(0x100000002);
		else if (invalid == 2)
			vacuum_inplace_frozenxid = FrozenTransactionId;
		else
			ShmemVariableCache = NULL;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_try(&vacuum_relation, 0, 0, 0, false, 120,
									   InvalidMultiXactId, &updated, NULL, false),
					 invalid == 2 ? ERRCODE_DATA_CORRUPTED
								  : ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(relation->relfrozenxid, invalid == 2 ? FrozenTransactionId : 80);
		/* ERROR has no successful catalogue update. */
		UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_relstats_uses_durable_legal_bound)
{
	bool updated = false;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_legal_next = 10000;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	vac_update_relstats(&vacuum_relation, 0, 0, 0, false, 9000, InvalidMultiXactId,
						&updated, NULL, false);
	UT_ASSERT_EQ(relation->relfrozenxid, 9000);
	UT_ASSERT(updated);
	UT_ASSERT_EQ(vacuum_sample_reads, 1);
	UT_ASSERT_EQ(vacuum_fence_reads, 1);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_unknown_window_keeps_stats_and_mx)
{
	bool updated = true, multi_updated = false;
	Form_pg_class relation;

	vacuum_reset();
	vacuum_promise_ready = false;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	relation->relminmxid = 300;
	vac_update_relstats(&vacuum_relation, 5, 20, 2, false, InvalidTransactionId, 80, &updated,
						&multi_updated, false);
	UT_ASSERT_EQ(relation->relfrozenxid, 80);
	UT_ASSERT(!updated && multi_updated);
	UT_ASSERT_EQ(relation->relpages, 5);
	UT_ASSERT_EQ(relation->reltuples, 20);
	UT_ASSERT_EQ(relation->relallvisible, 2);
	UT_ASSERT_EQ(relation->relminmxid, 80);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	UT_ASSERT_EQ(vacuum_cancels, 0);
	UT_ASSERT_EQ(vacuum_truncates, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_accepts_legal_next_boundary)
{
	bool updated = false;
	Form_pg_class relation;

	vacuum_reset();
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	vac_update_relstats(&vacuum_relation, 0, 0, 0, false, 200, InvalidMultiXactId,
						&updated, NULL, false);
	UT_ASSERT_EQ(relation->relfrozenxid, 200);
	UT_ASSERT(updated);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	vacuum_abort();
}

UT_TEST(vacuum_database_rejects_future_values)
{
	for (int future_current = 0; future_current < 2; future_current++) {
		Form_pg_database database;

		vacuum_reset();
		/* Isolate the merge from the separate pg_class scan bound. */
		vacuum_no_tables = true;
		vacuum_oldest_xid = future_current ? 100 : 201;
		vacuum_inplace_frozenxid = future_current ? 250 : 40;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 100;
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(database->datfrozenxid, future_current ? 250 : 40);
		UT_ASSERT_EQ(vacuum_finishes, 0);
		UT_ASSERT_EQ(vacuum_cancels, 0);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_rejects_half_space_current)
{
	Form_pg_database database;

	vacuum_reset();
	vacuum_no_tables = true;
	vacuum_oldest_xid = 0x800000c9U;
	vacuum_legal_next = UINT64CONST(0x1000000c8);
	vacuum_inplace_frozenxid = 0x800000c8U;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datminmxid = 100;
	UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
	UT_ASSERT_EQ(database->datfrozenxid, 0x800000c8U);
	UT_ASSERT_EQ(vacuum_cancels, 0);
	UT_ASSERT_EQ(vacuum_truncates, 0);
	vacuum_abort();
}

UT_TEST(vacuum_database_unavailable_promise_preserves_xid)
{
	for (int revoked = 0; revoked < 2; revoked++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_promise_ready = revoked;
		vacuum_revoke_on_release = revoked;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 80;
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(database->datfrozenxid, 40);
		UT_ASSERT_EQ(vacuum_sample_reads, 1);
		UT_ASSERT_EQ(vacuum_fence_reads, revoked);
		UT_ASSERT_EQ(vacuum_begins + vacuum_cancels, 0);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_invalid_window_preserves_xid)
{
	for (int invalid = 0; invalid < 6; invalid++) {
		Form_pg_database database;

		vacuum_reset();
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 80;
		if (invalid == 0)
			vacuum_legal_next = 0;
		else if (invalid == 1)
			vacuum_legal_next = UINT64CONST(0x100000002);
		else if (invalid == 2)
			vacuum_inplace_frozenxid = FrozenTransactionId;
		else if (invalid == 3)
			database->datfrozenxid = InvalidTransactionId;
		else if (invalid == 4)
			ShmemVariableCache = NULL;
		else
			cluster_xid_striping = false;
		UT_ASSERT_EQ(vacuum_call(), invalid == 2 || invalid == 3 ?
					 ERRCODE_DATA_CORRUPTED : ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(database->datfrozenxid, invalid == 2 ? FrozenTransactionId :
					 invalid == 3 ? InvalidTransactionId : 40);
		UT_ASSERT_EQ(vacuum_cancels, 0);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_accepts_durable_bound_and_next_boundary)
{
	for (int remote_ahead = 0; remote_ahead < 2; remote_ahead++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_no_tables = true;
		vacuum_oldest_xid = remote_ahead ? 9000 : 200;
		vacuum_legal_next = remote_ahead ? 10000 : 200;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 100;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(database->datfrozenxid, remote_ahead ? 9000 : 200);
		UT_ASSERT_EQ(vacuum_sample_reads, 1);
		UT_ASSERT_EQ(vacuum_fence_reads, 1);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		UT_ASSERT_EQ(vacuum_truncates, 1);
		UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_retains_full_xid_wrap_order)
{
	for (int advance = 0; advance < 2; advance++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_no_tables = true;
		vacuum_oldest_xid = advance ? 20 : 0xffffff00U;
		vacuum_inplace_frozenxid = advance ? 0xffffff00U : 20;
		vacuum_legal_next = UINT64CONST(0x10000001e);
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		database->datminmxid = 100;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(database->datfrozenxid, 20);
		UT_ASSERT_EQ(vacuum_finishes, advance);
		UT_ASSERT_EQ(vacuum_cancels, !advance);
		UT_ASSERT_EQ(vacuum_truncates, advance);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_unknown_window_preserves_both_horizons)
{
	Form_pg_database database;

	vacuum_reset();
	vacuum_promise_ready = false;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datminmxid = 300;
	/* Boundary ERROR prevents both catalogue fields from changing. */
	UT_ASSERT_EQ(vacuum_call(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(database->datfrozenxid, 40);
	UT_ASSERT_EQ(database->datminmxid, 300);
	UT_ASSERT_EQ(vacuum_begins + vacuum_finishes, 0);
	UT_ASSERT_EQ(vacuum_cancels, 0);
	UT_ASSERT_EQ(vacuum_truncates, 0);
	vacuum_abort();
}


UT_TEST(vacuum_database_scan_accepts_foreign_xid_and_keeps_seed)
{
	for (int low_seed = 0; low_seed < 2; low_seed++) {
		Form_pg_class relation;
		Form_pg_database database;

		vacuum_reset();
		vacuum_legal_next = 10000;
		vacuum_oldest_xid = low_seed ? 100 : 9000;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		relation->relfrozenxid = 8000;
		vacuum_add_class(7000, 60);
		vacuum_add_class(8500, 70);
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(database->datfrozenxid, low_seed ? 100 : 7000);
		UT_ASSERT_EQ(database->datminmxid, 60);
		UT_ASSERT_EQ(vacuum_class_read, 3);
		UT_ASSERT_EQ(vacuum_sample_seen_at_scan, 1);
		UT_ASSERT_EQ(vacuum_sample_reads, 1);
		UT_ASSERT_EQ(vacuum_fence_reads, 1);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_scan_uses_full_xid_minimum_across_wrap)
{
	Form_pg_class relation;
	Form_pg_database database;

	vacuum_reset();
	vacuum_legal_next = UINT64CONST(0x10000001e);
	vacuum_oldest_xid = 20;
	relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
	relation->relfrozenxid = 15;
	vacuum_add_class(0xffffff00U, 60);
	vacuum_add_class(25, 70);
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	database->datfrozenxid = 0xfffffe00U;
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(database->datfrozenxid, 0xffffff00U);
	UT_ASSERT_EQ(database->datminmxid, 60);
	UT_ASSERT_EQ(vacuum_class_read, 3);
	UT_ASSERT_EQ(vacuum_sample_seen_at_scan, 1);
	UT_ASSERT_EQ(vacuum_finishes, 1);
	vacuum_abort();
}

UT_TEST(vacuum_database_scan_rejects_invalid_later_xid)
{
	const TransactionId invalid[] = { 10001, 0x80002710U, FrozenTransactionId };

	for (unsigned i = 0; i < lengthof(invalid); i++) {
		Form_pg_class relation;
		Form_pg_database database;

		vacuum_reset();
		vacuum_legal_next = UINT64CONST(0x100002710);
		vacuum_oldest_xid = 9000;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		relation->relfrozenxid = 8000;
		vacuum_add_class(invalid[i], 60);
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(vacuum_class_read, 2);
		UT_ASSERT_EQ(database->datfrozenxid, 40);
		UT_ASSERT_EQ(database->datminmxid, 40);
		UT_ASSERT_EQ(vacuum_begins + vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_scan_unknown_window_preserves_both_horizons)
{
	for (int unknown = 0; unknown < 6; unknown++) {
		Form_pg_class relation;
		Form_pg_database database;

		vacuum_reset();
		vacuum_legal_next = 10000;
		if (unknown == 0)
			vacuum_promise_ready = false;
		else if (unknown == 1)
			vacuum_revoke_on_release = true;
		else if (unknown == 2)
			cluster_xid_striping = false;
		else if (unknown == 3)
			ShmemVariableCache = NULL;
		else if (unknown == 4)
			vacuum_legal_next = 0;
		else
			vacuum_legal_next = UINT64CONST(0x100000002);
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		relation->relfrozenxid = 300;
		vacuum_add_class(400, 60);
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(database->datfrozenxid, 40);
		UT_ASSERT_EQ(database->datminmxid, 40);
		UT_ASSERT_EQ(vacuum_class_read, unknown == 1 ? 2 : 0);
		UT_ASSERT_EQ(vacuum_begins + vacuum_finishes, 0);
		UT_ASSERT_EQ(vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_scan_rechecks_fence_after_catalog_read)
{
	Form_pg_database database;

	vacuum_reset();
	vacuum_revoke_on_scan_end = true;
	vacuum_add_class(60, 60);
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	UT_ASSERT_EQ(vacuum_call(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(database->datfrozenxid, 40);
	UT_ASSERT_EQ(database->datminmxid, 40);
	UT_ASSERT_EQ(vacuum_sample_seen_at_scan, 1);
	UT_ASSERT_EQ(vacuum_sample_reads, 1);
	UT_ASSERT_EQ(vacuum_fence_reads, 1);
	UT_ASSERT_EQ(vacuum_begins + vacuum_finishes + vacuum_truncates, 0);
	UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
	vacuum_abort();
}

UT_TEST(vacuum_database_scan_rejects_invalid_seed_before_fold)
{
	for (int half_space = 0; half_space < 2; half_space++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_legal_next = UINT64CONST(0x100002710);
		vacuum_oldest_xid = half_space ? 0x80002710U : 10001;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(database->datfrozenxid, 40);
		UT_ASSERT_EQ(database->datminmxid, 40);
		UT_ASSERT_EQ(vacuum_class_read + vacuum_begins + vacuum_finishes, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_scan_nontarget_keeps_native_future_refusal)
{
	for (int native = 0; native < 2; native++) {
		vacuum_reset();
		cluster_enabled = native;
		cluster_shared_catalog = !native;
		vacuum_legal_next = 10000;
		((Form_pg_class)GETSTRUCT(&vacuum_class_tuple))->relfrozenxid = 300;
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(vacuum_begins + vacuum_truncates, 0);
		UT_ASSERT_EQ(vacuum_sample_reads + vacuum_fence_reads, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_scan_keeps_mx_future_refusal)
{
	Form_pg_database database;

	vacuum_reset();
	vacuum_legal_next = 10000;
	((Form_pg_class)GETSTRUCT(&vacuum_class_tuple))->relminmxid = 201;
	database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
	UT_ASSERT_EQ(vacuum_call(), 0);
	UT_ASSERT_EQ(database->datfrozenxid, 40);
	UT_ASSERT_EQ(database->datminmxid, 40);
	UT_ASSERT_EQ(vacuum_begins + vacuum_truncates, 0);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_requires_ready_f_before_catalogue_mutation)
{
	static const uint64 invalid[] = {0, 0, 2, UINT64_MAX, 201, UINT64CONST(0x100000002),
									UINT64CONST(0x800000c8), UINT64_MAX - 1, 150};

	for (unsigned i = 0; i < lengthof(invalid); i++) {
		Form_pg_class relation;

		vacuum_reset();
		vacuum_f_ready = i != 0;
		vacuum_f_zero = i == 1;
		vacuum_f_value = invalid[i];
		vacuum_revoke_in_f_sample = i == 8;
		if (i == 6)
			vacuum_legal_next = UINT64CONST(0x1000000c8);
		else if (i == 7)
			vacuum_legal_next = UINT64_MAX;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_call(120), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(relation->relfrozenxid, 80);
		UT_ASSERT_EQ(relation->relpages, 0);
		UT_ASSERT_EQ(vacuum_begins + vacuum_finishes + vacuum_cancels, 0);
		UT_ASSERT(!vacuum_xid_lock && !vacuum_inplace_lock);
		vacuum_abort();
	}
}

UT_TEST(vacuum_relstats_checks_proposal_and_locked_equal_current_against_f)
{
	for (unsigned which = 0; which < 3; which++) {
		Form_pg_class relation;

		vacuum_reset();
		vacuum_f_value = 150;
		vacuum_inplace_frozenxid = which ? 151 : 80;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_call(which == 1 ? 120 : 151), ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(relation->relfrozenxid, which ? 151 : 80);
		UT_ASSERT_EQ(relation->relpages, 0);
		UT_ASSERT_EQ(vacuum_finishes + vacuum_cancels, 0);
		UT_ASSERT_EQ(vacuum_begins, 1);
		UT_ASSERT_EQ(vacuum_f_reads, 1);
		vacuum_abort();
	}
}

UT_TEST(vacuum_relstats_uses_f_only_as_bound_and_keeps_locked_max)
{
	static const TransactionId current[] = {80, 80, 150};
	static const TransactionId candidate[] = {140, 150, 140};
	static const TransactionId want[] = {140, 150, 150};

	for (unsigned i = 0; i < lengthof(current); i++) {
		Form_pg_class relation;

		vacuum_reset();
		vacuum_f_value = 150;
		vacuum_inplace_frozenxid = current[i];
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_call(candidate[i]), 0);
		UT_ASSERT_EQ(relation->relfrozenxid, want[i]);
		UT_ASSERT_EQ(relation->relpages, 5);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		UT_ASSERT_EQ(vacuum_f_reads, 1);
		vacuum_abort();
	}
	vacuum_reset();
	vacuum_next_xid = 30;
	vacuum_cache.nextXid = FullTransactionIdFromU64(30);
	vacuum_legal_next = UINT64CONST(0x10000001e);
	vacuum_f_value = UINT64CONST(0x100000014);
	vacuum_inplace_frozenxid = 0xfffffff0U;
	UT_ASSERT_EQ(vacuum_relstats_call(20), 0);
	UT_ASSERT_EQ(((Form_pg_class)GETSTRUCT(&vacuum_class_tuple))->relfrozenxid, 20);
	vacuum_abort();
}

UT_TEST(vacuum_relstats_stats_only_and_nontarget_do_not_require_f)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		Form_pg_class relation;

		vacuum_reset();
		vacuum_f_ready = false;
		if (mode == 1)
			cluster_enabled = false;
		else if (mode == 2)
			cluster_shared_catalog = false;
		relation = (Form_pg_class)GETSTRUCT(&vacuum_class_tuple);
		UT_ASSERT_EQ(vacuum_relstats_call(mode ? 120 : InvalidTransactionId), 0);
		UT_ASSERT_EQ(relation->relfrozenxid, mode ? 120 : 80);
		UT_ASSERT_EQ(relation->relpages, 5);
		UT_ASSERT_EQ(vacuum_f_reads, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_requires_ready_f_and_no_late_allocation_revocation)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		Form_pg_database database;

		vacuum_reset();
		if (fault == 0)
			vacuum_f_ready = false;
		else if (fault == 1)
			vacuum_f_zero = true;
		else if (fault == 2)
			vacuum_f_value = 201;
		else if (fault == 3)
			vacuum_revoke_in_f_sample = true;
		else if (fault == 4)
			vacuum_f_value = UINT64_MAX;
		else if (fault == 5)
			vacuum_f_value = FrozenTransactionId;
		else if (fault == 6)
			vacuum_legal_next = UINT64_MAX;
		else {
			vacuum_legal_next = UINT64CONST(0x1000000c8);
			vacuum_f_value = UINT64CONST(0x800000c8);
		}
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
		UT_ASSERT_EQ(database->datfrozenxid, 40);
		UT_ASSERT_EQ(database->datminmxid, 40);
		UT_ASSERT_EQ(vacuum_begins + vacuum_finishes + vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_rejects_each_relation_above_f_or_unknown)
{
	static const TransactionId bad[] = {151, InvalidTransactionId};

	for (unsigned kind = 0; kind < 3; kind++) {
		for (unsigned invalid = 0; invalid < lengthof(bad); invalid++) {
			Form_pg_database database;
			Form_pg_class last;

			vacuum_reset();
			vacuum_f_value = 150;
			vacuum_add_class(60, 60);
			vacuum_add_class(bad[invalid], 60);
			last = (Form_pg_class)GETSTRUCT(&vacuum_extra_class_tuple[1]);
			last->relkind = kind == 0 ? RELKIND_RELATION :
							kind == 1 ? RELKIND_MATVIEW : RELKIND_TOASTVALUE;
			last->relpersistence = kind == 0 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
			database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
			UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
			UT_ASSERT_EQ(vacuum_class_read, 3);
			UT_ASSERT_EQ(database->datfrozenxid, 40);
			UT_ASSERT_EQ(database->datminmxid, 40);
			UT_ASSERT_EQ(vacuum_begins + vacuum_finishes + vacuum_truncates, 0);
			vacuum_abort();
		}
	}
}

UT_TEST(vacuum_database_checks_equal_and_later_locked_current_against_f)
{
	for (unsigned equal = 0; equal < 2; equal++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_f_value = 150;
		vacuum_inplace_frozenxid = 151;
		if (equal) {
			vacuum_no_tables = true;
			vacuum_oldest_xid = 151;
		}
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(database->datfrozenxid, 151);
		UT_ASSERT_EQ(database->datminmxid, 40);
		UT_ASSERT_EQ(vacuum_finishes + vacuum_truncates, 0);
		vacuum_abort();
	}
}

UT_TEST(vacuum_database_uses_real_scan_minimum_and_proven_sanity_bound)
{
	for (unsigned newer = 0; newer < 2; newer++) {
		Form_pg_database database;

		vacuum_reset();
		vacuum_f_value = 150;
		vacuum_oldest_xid = 180;
		((Form_pg_class)GETSTRUCT(&vacuum_class_tuple))->relfrozenxid = 140;
		vacuum_add_class(130, 60);
		vacuum_add_class(145, 70);
		if (newer)
			vacuum_inplace_frozenxid = 145;
		database = (Form_pg_database)GETSTRUCT(&vacuum_database_tuple);
		UT_ASSERT_EQ(vacuum_call(), 0);
		UT_ASSERT_EQ(vacuum_class_read, 3);
		UT_ASSERT_EQ(database->datfrozenxid, newer ? 145 : 130);
		UT_ASSERT_EQ(database->datminmxid, 60);
		UT_ASSERT_EQ(vacuum_truncate_sane, 150);
		UT_ASSERT_EQ(vacuum_finishes, 1);
		UT_ASSERT_EQ(vacuum_f_reads, 1);
		vacuum_abort();
	}
}
