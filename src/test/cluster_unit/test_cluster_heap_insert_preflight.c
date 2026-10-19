/*-------------------------------------------------------------------------
 * test_cluster_heap_insert_preflight.c
 *    Execute native insert preparation before the first main heap pin.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_insert_preflight.c
 *
 * Verbatim heap_insert / heap_multi_insert entry prefixes and complete routing,
 * guards and heap_prepare_insert bodies. Stop before main buffer acquisition
 * or native batch preparation. Canonical publication, lease, TOAST storage,
 * slot materialization and undo reservation are explicit boundaries. This does
 * not simulate BIND durability, receipt APPLY or transaction cleanup.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/parallel.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/storage/cluster_smgr.h"
#include "executor/tuptable.h"
#include "utils/rel.h"
#include "../../backend/access/heap/heapam_r4_private.h"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog;
int cluster_node_id;
int ParallelWorkerNumber = -1;
int wal_level = WAL_LEVEL_REPLICA;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static RelationData relation_data;
static FormData_pg_class relform;
static TupleDescData tuple_desc;
static HeapTupleData tuple;
static PGAlignedBlock tuple_bytes;
static TransactionId top_xid, current_xid;
static bool in_child, publish_ok, expect_binding, toast_error;
static unsigned allocations, current_allocations, prepares, reservations;
static unsigned bulk_calls, toast_writes, slot_fetches, undo_reservations, unbound_work;
static int reported_code;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# assertion %s at %s:%d\n", condition, file, line);
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
bool
IsCatalogRelation(Relation rel)
{
	return rel->rd_locator.relNumber == 1259;
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
		top_xid = 400;
		allocations++;
	}
	if (!in_child)
		current_xid = top_xid;
	return top_xid;
}

TransactionId
GetCurrentTransactionId(void)
{
	if (!TransactionIdIsValid(current_xid)) {
		GetTopTransactionId();
		if (in_child)
			current_xid = 401;
		current_allocations++;
	}
	return current_xid;
}

bool
cluster_tt_local_prepare_canonical_active(TransactionId xid, ClusterCanonicalTxnBinding *binding)
{
	prepares++;
	UT_ASSERT_EQ(xid, 400);
	if (!publish_ok)
		return false;
	if (reservations == 0)
		reservations++;
	MemSet(binding, 0, sizeof(*binding));
	binding->xid = xid;
	binding->segment_id = 7;
	binding->slot_offset = 3;
	return true;
}

static void
check_bound_work(void)
{
	if (expect_binding && reservations == 0)
		unbound_work++;
}

void
cluster_dl_bulk_acquire(Relation relation, BulkInsertState bistate)
{
	UT_ASSERT(relation == &relation_data);
	check_bound_work();
	bulk_calls++;
}

static void
AssertHasSnapshotForToast(Relation relation)
{
	/* Snapshot existence is outside this entry-order fixture. */
}

HeapTuple
heap_toast_insert_or_update(Relation relation, HeapTuple newtup, HeapTuple oldtup, int options)
{
	UT_ASSERT(relation == &relation_data);
	UT_ASSERT(oldtup == NULL);
	check_bound_work();
	toast_writes++;
	if (toast_error)
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("fixture TOAST error")));
	return newtup;
}

HeapTuple
ExecFetchSlotHeapTuple(TupleTableSlot *slot, bool materialize, bool *shouldFree)
{
	UT_ASSERT(materialize);
	UT_ASSERT(shouldFree == NULL);
	check_bound_work();
	slot_fetches++;
	return &tuple;
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	return expect_binding ? 1 : 0;
}

bool
cluster_space_relation_get_identity(Relation relation, ClusterSpaceIdentity *identity)
{
	MemSet(identity, 0, sizeof(*identity));
	return true;
}

uint64
cluster_undo_record_prepare_deadline_us(void)
{
	return 1000;
}

static bool
cluster_heap_prepare_undo_record_exact(uint8 record_type, uint16 capacity, uint16 segment,
									   uint16 slot, UBA prev, uint64 deadline,
									   ClusterUndoRecordPrepareReceipt *receipt)
{
	UT_ASSERT_EQ(record_type, UNDO_RECORD_INSERT);
	UT_ASSERT_EQ(capacity, sizeof(UndoInsertPayload));
	UT_ASSERT_EQ(segment, 7);
	UT_ASSERT_EQ(slot, 3);
	UT_ASSERT_EQ(deadline, 1000);
	check_bound_work();
	undo_reservations++;
	return true;
}

static int
cluster_heap_undo_receipt_errdetail(bool ctrc)
{
	return 0;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#pragma GCC diagnostic ignored "-Wunused-label"
#include "test_cluster_heap_insert_preflight.inc"
#pragma GCC diagnostic pop

static void
reset(void)
{
	MemSet(&relation_data, 0, sizeof(relation_data));
	MemSet(&relform, 0, sizeof(relform));
	MemSet(&tuple_desc, 0, sizeof(tuple_desc));
	MemSet(&tuple, 0, sizeof(tuple));
	MemSet(&tuple_bytes, 0, sizeof(tuple_bytes));
	relation_data.rd_rel = &relform;
	relation_data.rd_att = &tuple_desc;
	relation_data.rd_id = relation_data.rd_locator.relNumber = 20000;
	relform.relkind = RELKIND_RELATION;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	tuple_desc.natts = 1;
	tuple.t_data = (HeapTupleHeader)tuple_bytes.data;
	tuple.t_len = TOAST_TUPLE_THRESHOLD + 1;
	cluster_enabled = cluster_shared_config = publish_ok = expect_binding = true;
	cluster_shared_catalog = in_child = toast_error = false;
	cluster_node_id = 0;
	ParallelWorkerNumber = -1;
	top_xid = current_xid = InvalidTransactionId;
	allocations = current_allocations = prepares = reservations = 0;
	bulk_calls = toast_writes = slot_fetches = undo_reservations = unbound_work = 0;
	reported_code = 0;
}

static bool
run_insert(bool batch, int ntuples, int options)
{
	TupleTableSlot first = { 0 }, second = { 0 };
	TupleTableSlot *slots[] = { &first, &second };
	bool failed = false;

	PG_TRY();
	{
		if (batch)
			heap_multi_insert(&relation_data, slots, ntuples, 0, options, NULL);
		else
			heap_insert(&relation_data, &tuple, 0, options, NULL);
	}
	PG_CATCH();
	{
		failed = true;
	}
	PG_END_TRY();
	return failed;
}

UT_TEST(direct_insert_binds_before_lease_toast_and_undo)
{
	reset();
	UT_ASSERT(!run_insert(false, 1, 0));
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(bulk_calls, 1);
	UT_ASSERT_EQ(toast_writes, 1);
	UT_ASSERT_EQ(undo_reservations, 1);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple.t_data), 400);
	UT_ASSERT_EQ(unbound_work, 0);
}

UT_TEST(batch_binds_before_materialization_and_reuses_one_top_binding)
{
	reset();
	UT_ASSERT(!run_insert(true, 2, 0));
	UT_ASSERT_EQ(allocations, 1);
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(slot_fetches, 2);
	UT_ASSERT_EQ(toast_writes, 2);
	UT_ASSERT_EQ(undo_reservations, 2);
	UT_ASSERT_EQ(unbound_work, 0);
}

UT_TEST(empty_batch_still_binds_before_bulk_work)
{
	reset();
	UT_ASSERT(!run_insert(true, 0, 0));
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(slot_fetches + toast_writes + undo_reservations, 0);
	UT_ASSERT_EQ(unbound_work, 0);
}

UT_TEST(bind_failure_prevents_all_insert_effects)
{
	for (int batch = 0; batch < 2; ++batch) {
		reset();
		publish_ok = false;
		UT_ASSERT(run_insert(batch, 2, 0));
		UT_ASSERT_EQ(reported_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(reservations, 0);
		UT_ASSERT_EQ(bulk_calls + toast_writes + slot_fetches + undo_reservations, 0);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple.t_data), InvalidTransactionId);
	}
}

UT_TEST(subtransaction_refuses_before_xid_binding_or_effects)
{
	for (int batch = 0; batch < 2; ++batch) {
		reset();
		in_child = true;
		UT_ASSERT(run_insert(batch, 2, 0));
		UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(allocations + current_allocations + prepares, 0);
		UT_ASSERT_EQ(bulk_calls + toast_writes + slot_fetches + undo_reservations, 0);
	}
}

UT_TEST(mismatched_current_xid_refuses_before_publication_or_effects)
{
	for (int batch = 0; batch < 2; ++batch) {
		reset();
		top_xid = 400;
		current_xid = 401;
		UT_ASSERT(run_insert(batch, 2, 0));
		UT_ASSERT_EQ(reported_code, ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(prepares + bulk_calls + toast_writes + slot_fetches + undo_reservations, 0);
	}
}

UT_TEST(parallel_workers_with_inherited_xid_refuse_before_binding_or_effects)
{
	for (int route = 0; route < 3; ++route) {
		reset();
		ParallelWorkerNumber = 0;
		top_xid = current_xid = 400;
		UT_ASSERT(run_insert(route != 0, route == 1 ? 0 : 2, 0));
		UT_ASSERT_EQ(reported_code, ERRCODE_INVALID_TRANSACTION_STATE);
		UT_ASSERT_EQ(allocations + current_allocations + prepares + reservations, 0);
		UT_ASSERT_EQ(bulk_calls + toast_writes + slot_fetches + undo_reservations, 0);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple.t_data), InvalidTransactionId);
	}
}

UT_TEST(shared_frozen_insert_refuses_before_binding_or_effects)
{
	for (int batch = 0; batch < 2; ++batch) {
		reset();
		UT_ASSERT(run_insert(batch, 2, HEAP_INSERT_FROZEN));
		UT_ASSERT_EQ(reported_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(allocations + current_allocations + prepares, 0);
		UT_ASSERT_EQ(bulk_calls + toast_writes + slot_fetches + undo_reservations, 0);
	}
}

UT_TEST(private_temp_and_disabled_targets_keep_native_binding_behavior)
{
	for (int route = 0; route < 3; ++route)
		for (int batch = 0; batch < 2; ++batch) {
			reset();
			expect_binding = false;
			if (route == 0)
				relation_data.rd_locator.relNumber = 1259; /* node-local catalog */
			else if (route == 1)
				relform.relpersistence = RELPERSISTENCE_TEMP;
			else
				cluster_enabled = false;
			UT_ASSERT(!run_insert(batch, 2, 0));
			UT_ASSERT_EQ(reservations + prepares + undo_reservations, 0);
			UT_ASSERT_EQ(allocations, 1);
			UT_ASSERT_EQ(unbound_work, 0);
		}
}

UT_TEST(shared_catalog_uses_the_original_top_binding)
{
	reset();
	cluster_shared_catalog = true;
	relation_data.rd_locator.relNumber = 1259;
	UT_ASSERT(!run_insert(false, 1, 0));
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(unbound_work, 0);
}

UT_TEST(existing_binding_is_reused_without_a_new_xid_or_reservation)
{
	for (int batch = 0; batch < 2; ++batch) {
		reset();
		top_xid = current_xid = 400;
		reservations = 1;
		UT_ASSERT(!run_insert(batch, 2, 0));
		UT_ASSERT_EQ(allocations + current_allocations, 0);
		UT_ASSERT_EQ(reservations, 1);
		UT_ASSERT_EQ(unbound_work, 0);
	}
}

UT_TEST(toast_error_retains_the_original_transaction_binding)
{
	reset();
	toast_error = true;
	UT_ASSERT(run_insert(false, 1, 0));
	UT_ASSERT_EQ(reported_code, ERRCODE_DATA_EXCEPTION);
	UT_ASSERT_EQ(reservations, 1);
	UT_ASSERT_EQ(toast_writes, 1);
	UT_ASSERT_EQ(undo_reservations, 0);
	UT_ASSERT_EQ(unbound_work, 0);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(direct_insert_binds_before_lease_toast_and_undo);
	UT_RUN(batch_binds_before_materialization_and_reuses_one_top_binding);
	UT_RUN(empty_batch_still_binds_before_bulk_work);
	UT_RUN(bind_failure_prevents_all_insert_effects);
	UT_RUN(subtransaction_refuses_before_xid_binding_or_effects);
	UT_RUN(mismatched_current_xid_refuses_before_publication_or_effects);
	UT_RUN(parallel_workers_with_inherited_xid_refuse_before_binding_or_effects);
	UT_RUN(shared_frozen_insert_refuses_before_binding_or_effects);
	UT_RUN(private_temp_and_disabled_targets_keep_native_binding_behavior);
	UT_RUN(shared_catalog_uses_the_original_top_binding);
	UT_RUN(existing_binding_is_reused_without_a_new_xid_or_reservation);
	UT_RUN(toast_error_retains_the_original_transaction_binding);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
