/*-------------------------------------------------------------------------
 *
 * test_cluster_vacuum_native_window.c
 *    Actual native maintenance entry with catalogue, lease and I/O leaves.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vacuum_native_window.c
 *
 * NOTES
 *    The entry and FullXid widening are production code. This fixture checks
 *    maintenance dispatch, not physical SLRU deletion or complete pin proof.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "unit_test.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_database.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/rel.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_shared_catalog = true;
bool cluster_xid_striping = true;
int cluster_node_id = 1;
Oid MyDatabaseId = 42;
static VariableCacheData cache;
VariableCache ShmemVariableCache = &cache;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static bool xid_locked, wrap_locked;
static bool promise_ready, promise_current, revoke_on_unlock, revoke_on_scan_end;
static uint64 legal_next;
static unsigned samples, fences, scans, rows_read, warnings;
static unsigned wrap_acquires, wrap_releases;
static struct RelationData relation;
static TableScanDescData scan;
static HeapTupleData tuples[4];
static union {
	max_align_t alignment;
	char bytes[MAXALIGN(SizeofHeapTupleHeader) + sizeof(FormData_pg_database)];
} storage[4];
static unsigned row_count;

typedef enum NativeAction {
	NATIVE_NOTIFY,
	NATIVE_OLDEST_COMMIT_TS,
	NATIVE_CLOG,
	NATIVE_COMMIT_TS,
	NATIVE_MX,
	NATIVE_XID_LIMIT,
	NATIVE_MX_LIMIT
} NativeAction;
static struct {
	NativeAction action;
	TransactionId cut;
	Oid database;
} actions[7];
static unsigned action_count;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == XidGenLock) {
		UT_ASSERT_EQ(mode, LW_SHARED);
		UT_ASSERT(!xid_locked && !wrap_locked);
		xid_locked = true;
	} else {
		UT_ASSERT(lock == WrapLimitsVacuumLock);
		UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
		UT_ASSERT(!xid_locked && !wrap_locked);
		wrap_locked = true;
		wrap_acquires++;
	}
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == XidGenLock) {
		UT_ASSERT(xid_locked);
		xid_locked = false;
		if (revoke_on_unlock)
			promise_current = false;
	} else {
		UT_ASSERT(lock == WrapLimitsVacuumLock && wrap_locked);
		wrap_locked = false;
		wrap_releases++;
	}
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	return cache.nextXid;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(xid_locked && !wrap_locked);
	UT_ASSERT_EQ(U64FromFullTransactionId(reference), U64FromFullTransactionId(cache.nextXid));
	samples++;
	memset(fence, 0, sizeof(*fence));
	*future = InvalidFullTransactionId;
	if (!promise_ready)
		return false;
	fence->incarnation = 81;
	*future = FullTransactionIdFromU64(legal_next);
	return true;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(!xid_locked && wrap_locked);
	UT_ASSERT_EQ(fence->incarnation, 81);
	UT_ASSERT_EQ(action_count, 0);
	fences++;
	return promise_current;
}

bool
MultiXactIdPrecedes(MultiXactId a, MultiXactId b)
{
	return (int32)(a - b) < 0;
}

bool
database_is_invalid_form(Form_pg_database database)
{
	return database->datconnlimit == DATCONNLIMIT_INVALID_DB;
}

Relation
table_open(Oid oid, LOCKMODE mode)
{
	UT_ASSERT_EQ(oid, DatabaseRelationId);
	UT_ASSERT_EQ(mode, AccessShareLock);
	UT_ASSERT(wrap_locked && !xid_locked);
	return &relation;
}

void
table_close(Relation rel, LOCKMODE mode)
{
	UT_ASSERT(rel == &relation);
	UT_ASSERT(wrap_locked && !xid_locked);
}

TableScanDesc
table_beginscan_catalog(Relation rel, int nkeys, ScanKey key)
{
	UT_ASSERT(rel == &relation);
	UT_ASSERT(wrap_locked && !xid_locked);
	scans++;
	return &scan;
}

HeapTuple
heap_getnext(TableScanDesc desc, ScanDirection direction)
{
	UT_ASSERT(desc == &scan && direction == ForwardScanDirection);
	UT_ASSERT(wrap_locked && !xid_locked);
	if (rows_read == row_count)
		return NULL;
	return &tuples[rows_read++];
}

static void
end_scan(TableScanDesc desc)
{
	UT_ASSERT(desc == &scan);
	UT_ASSERT(!xid_locked);
	if (revoke_on_scan_end)
		promise_current = false;
}

static const TableAmRoutine tableam = { .scan_end = end_scan };

/* Record the original destructive leaves without deleting native files. */
static void
observe(NativeAction action, TransactionId cut, Oid database)
{
	UT_ASSERT(wrap_locked && !xid_locked);
	UT_ASSERT(action_count < lengthof(actions));
	if (action_count >= lengthof(actions))
		abort();
	actions[action_count].action = action;
	actions[action_count].cut = cut;
	actions[action_count++].database = database;
}

void
AsyncNotifyFreezeXids(TransactionId cut)
{
	observe(NATIVE_NOTIFY, cut, InvalidOid);
}
void
AdvanceOldestCommitTsXid(TransactionId cut)
{
	observe(NATIVE_OLDEST_COMMIT_TS, cut, InvalidOid);
}
void
TruncateCLOG(TransactionId cut, Oid db)
{
	observe(NATIVE_CLOG, cut, db);
}
void
TruncateCommitTs(TransactionId cut)
{
	observe(NATIVE_COMMIT_TS, cut, InvalidOid);
}
void
TruncateMultiXact(MultiXactId cut, Oid db)
{
	observe(NATIVE_MX, cut, db);
}
void
SetTransactionIdLimit(TransactionId cut, Oid db)
{
	observe(NATIVE_XID_LIMIT, cut, db);
}
void
SetMultiXactIdLimit(MultiXactId cut, Oid db, bool startup)
{
	UT_ASSERT(!startup);
	observe(NATIVE_MX_LIMIT, cut, db);
}

/* Diagnostics are counted at the logger leaf; no SQLSTATE claim is made. */
bool
errstart(int elevel, const char *domain)
{
	if (elevel >= ERROR)
		abort();
	if (elevel == WARNING)
		warnings++;
	return false;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "Assertion failed: %s, %s:%d\n", condition, file, line);
	abort();
}

#include "test_cluster_vacuum_native_window_xid.inc"
#include "test_cluster_vacuum_native_window.inc"

static void
add_database(Oid oid, TransactionId xid, MultiXactId mxid, bool invalid)
{
	HeapTuple tuple;
	Form_pg_database database;

	if (row_count >= lengthof(tuples))
		abort();
	tuple = &tuples[row_count];
	tuple->t_data = (HeapTupleHeader)&storage[row_count++];
	tuple->t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	database = (Form_pg_database)GETSTRUCT(tuple);
	database->oid = oid;
	database->datfrozenxid = xid;
	database->datminmxid = mxid;
	database->datconnlimit = invalid ? DATCONNLIMIT_INVALID_DB : -1;
}

static void
reset(void)
{
	UT_ASSERT(!xid_locked && !wrap_locked);
	cluster_enabled = cluster_shared_catalog = cluster_xid_striping = true;
	cluster_node_id = 1;
	ShmemVariableCache = &cache;
	cache.nextXid = FullTransactionIdFromU64(200);
	legal_next = 200;
	promise_ready = promise_current = true;
	revoke_on_unlock = revoke_on_scan_end = false;
	samples = fences = scans = rows_read = warnings = 0;
	wrap_acquires = wrap_releases = action_count = row_count = 0;
	memset(storage, 0, sizeof(storage));
	memset(actions, 0, sizeof(actions));
	relation.rd_tableam = &tableam;
	scan.rs_rd = &relation;
}

static void
expect_kept(void)
{
	UT_ASSERT_EQ(action_count, 0);
	UT_ASSERT(!xid_locked && !wrap_locked);
	UT_ASSERT_EQ(wrap_acquires, wrap_releases);
}

static void
expect_dispatch(TransactionId xid, Oid xid_db, MultiXactId mxid, Oid mx_db)
{
	UT_ASSERT_EQ(action_count, 7);
	for (unsigned i = 0; i < lengthof(actions); i++) {
		UT_ASSERT_EQ(actions[i].action, i);
		UT_ASSERT_EQ(actions[i].cut, (i == NATIVE_MX || i == NATIVE_MX_LIMIT) ? mxid : xid);
	}
	UT_ASSERT_EQ(actions[NATIVE_CLOG].database, xid_db);
	UT_ASSERT_EQ(actions[NATIVE_XID_LIMIT].database, xid_db);
	UT_ASSERT_EQ(actions[NATIVE_MX].database, mx_db);
	UT_ASSERT_EQ(actions[NATIVE_MX_LIMIT].database, mx_db);
	UT_ASSERT(!xid_locked && !wrap_locked);
	UT_ASSERT_EQ(wrap_acquires, wrap_releases);
}

UT_TEST(missing_promise_keeps_all_native_state)
{
	reset();
	promise_ready = false;
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(revoked_after_sampling_keeps_all_native_state)
{
	reset();
	revoke_on_unlock = true;
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(revoked_after_catalogue_scan_keeps_all_native_state)
{
	reset();
	revoke_on_scan_end = true;
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
	UT_ASSERT_EQ(rows_read, 1);
}

UT_TEST(unavailable_shared_striping_keeps_all_native_state)
{
	reset();
	cluster_xid_striping = false;
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(missing_allocator_cache_keeps_all_native_state)
{
	reset();
	ShmemVariableCache = NULL;
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(invalid_legal_bound_keeps_all_native_state)
{
	const uint64 invalid[] = { 0, FrozenTransactionId, UINT64CONST(0x100000002) };
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		legal_next = invalid[i];
		add_database(8, 90, 70, false);
		vac_truncate_clog(100, 100, 200, 200);
		expect_kept();
	}
}

UT_TEST(caller_seed_outside_legal_window_keeps_native_state)
{
	const TransactionId invalid[] = { FrozenTransactionId, 201, UINT32_C(0xffffffec) };
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		add_database(8, 90, 70, false);
		vac_truncate_clog(invalid[i], 100, 200, 200);
		expect_kept();
	}
}

UT_TEST(caller_sane_bound_outside_legal_window_keeps_native_state)
{
	reset();
	add_database(8, 90, 70, false);
	vac_truncate_clog(100, 100, 201, 200);
	expect_kept();
}

UT_TEST(impossible_previous_epoch_row_keeps_native_state)
{
	reset();
	add_database(8, UINT32_C(0xffffffec), 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(half_space_row_keeps_native_state)
{
	reset();
	cache.nextXid = FullTransactionIdFromU64(UINT64CONST(0x1000000c8));
	legal_next = UINT64CONST(0x1000000c8);
	add_database(8, UINT32_C(0x800000c8), 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(real_database_minima_keep_original_dispatch_and_order)
{
	reset();
	add_database(8, 120, 70, false);
	add_database(9, 90, 85, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_dispatch(90, 9, 70, 8);
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(fences, 1);
	UT_ASSERT_EQ(rows_read, 2);
}

UT_TEST(original_seed_is_not_replaced_by_legal_next)
{
	reset();
	legal_next = 400;
	add_database(8, 120, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_dispatch(100, MyDatabaseId, 70, 8);
}

UT_TEST(previous_epoch_minimum_keeps_native_wrap_order)
{
	reset();
	cache.nextXid = FullTransactionIdFromU64(UINT64CONST(0x1000000c8));
	legal_next = UINT64CONST(0x1000000c8);
	add_database(8, UINT32_C(0xffffffec), 70, false);
	add_database(9, 90, 85, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_dispatch(UINT32_C(0xffffffec), 8, 70, 8);
}

UT_TEST(invalid_database_retains_original_skip)
{
	reset();
	add_database(8, 300, 300, true);
	add_database(9, 90, 70, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_dispatch(90, 9, 70, 9);
}

UT_TEST(original_future_and_mx_refusals_remain)
{
	reset();
	legal_next = 400;
	add_database(8, 300, 70, false);
	vac_truncate_clog(100, 100, 400, 200);
	expect_kept();
	UT_ASSERT_EQ(warnings, 1);
	reset();
	add_database(8, 90, 201, false);
	vac_truncate_clog(100, 100, 200, 200);
	expect_kept();
}

UT_TEST(nontarget_modes_keep_native_entry)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		reset();
		promise_ready = false;
		if (mode == 0)
			cluster_enabled = false;
		if (mode == 1)
			cluster_shared_catalog = false;
		if (mode == 2)
			cluster_node_id = -1;
		add_database(8, 90, 70, false);
		vac_truncate_clog(100, 100, 200, 200);
		expect_dispatch(90, 8, 70, 8);
		UT_ASSERT_EQ(samples, 0);
		UT_ASSERT_EQ(fences, 0);
	}
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(missing_promise_keeps_all_native_state);
	UT_RUN(revoked_after_sampling_keeps_all_native_state);
	UT_RUN(revoked_after_catalogue_scan_keeps_all_native_state);
	UT_RUN(unavailable_shared_striping_keeps_all_native_state);
	UT_RUN(missing_allocator_cache_keeps_all_native_state);
	UT_RUN(invalid_legal_bound_keeps_all_native_state);
	UT_RUN(caller_seed_outside_legal_window_keeps_native_state);
	UT_RUN(caller_sane_bound_outside_legal_window_keeps_native_state);
	UT_RUN(impossible_previous_epoch_row_keeps_native_state);
	UT_RUN(half_space_row_keeps_native_state);
	UT_RUN(real_database_minima_keep_original_dispatch_and_order);
	UT_RUN(original_seed_is_not_replaced_by_legal_next);
	UT_RUN(previous_epoch_minimum_keeps_native_wrap_order);
	UT_RUN(invalid_database_retains_original_skip);
	UT_RUN(original_future_and_mx_refusals_remain);
	UT_RUN(nontarget_modes_keep_native_entry);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
