/*-------------------------------------------------------------------------
 * test_cluster_heap_point_full.c
 *    Execute exact point-TID owners with private FULL projections.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Storage/transport/native visibility/slot are explicit boundary doubles;
 * holder, server, registered landing and scratch MVCC run original code.
 *-------------------------------------------------------------------------
 */
#define CLUSTER_SCAN_FULL_ENTRY point_unused_scan_main
int point_unused_scan_main(void);
#include "test_cluster_heap_scan_full.c"
#include "storage/predicate.h"

const TupleTableSlotOps TTSOpsBufferHeapTuple = { 0 };
static unsigned point_predicates, point_wait_calls, point_pin_stores;
static bool point_store_error;

Buffer
ReadBuffer(Relation relation, BlockNumber block)
{
	return ReadBufferExtended(relation, MAIN_FORKNUM, block, RBM_NORMAL, NULL);
}

void
PredicateLockTID(Relation relation, ItemPointer tid, Snapshot snapshot, TransactionId xmin)
{
	UT_ASSERT(scan_locked);
	point_predicates++;
}

bool
cluster_heap_tuple_satisfies_visibility_waitable(HeapTuple tuple, Snapshot snapshot, Buffer buffer,
												 bool *remote, ClusterTxLocator *locator)
{
	UT_ASSERT(scan_locked);
	point_wait_calls++;
	return scan_native_visibility(tuple, snapshot, buffer);
}

TupleTableSlot *
ExecStorePinnedBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
	UT_ASSERT(scan_pins == 1 && !scan_locked);
	point_pin_stores++;
	ExecForceStoreHeapTuple(tuple, slot, false);
	return slot;
}

static void
point_store_tuple(HeapTuple tuple, TupleTableSlot *slot, bool free_tuple)
{
	ExecForceStoreHeapTuple(tuple, slot, free_tuple);
	if (point_store_error)
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED), errmsg("point slot cancellation fixture")));
}

#define HeapTupleSatisfiesVisibility scan_native_visibility
#define HeapCheckForSerializableConflictOut scan_conflict
#define TestForOldSnapshot scan_old_snapshot
#include "test_cluster_heap_fetch_consumer.inc"
#define ExecForceStoreHeapTuple point_store_tuple
#include "test_cluster_heap_point_callback.inc"
#undef ExecForceStoreHeapTuple
#undef HeapTupleSatisfiesVisibility
#undef HeapCheckForSerializableConflictOut
#undef TestForOldSnapshot

typedef struct PointFixture {
	HeapScanDescData unused_scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	HeapTupleData tuple;
	PGAlignedBlock owned;
	BufferHeapTupleTableSlot slot;
	Buffer buffer;
	bool remote;
	ClusterTxLocator locator;
} PointFixture;

static void
point_setup(PointFixture *f, OffsetNumber offset)
{
	memset(f, 0, offsetof(PointFixture, slot));
	scan_setup(&f->unused_scan, &f->relation, &f->form, &f->snapshot, false, false);
	ItemPointerSet(&f->tuple.t_self, scan_fixture.tag.blockNum, offset);
	f->buffer = InvalidBuffer;
	f->remote = true;
	memset(&f->locator, 0xff, sizeof(f->locator));
	point_predicates = point_wait_calls = point_pin_stores = 0;
	point_store_error = false;
}

/* Error capture stays outside the actual fetch and callback owner frames. */
static int
point_next(PointFixture *f, bool callback, bool keep, unsigned api)
{
	volatile int result = -1;
	ut_error_code = 0;
	ut_error_armed = true;
	PG_TRY();
	{
		if (callback)
			result = heapam_fetch_row_version(&f->relation, &f->tuple.t_self, &f->snapshot,
											  &f->slot.base.base);
		else if (api == 0)
			result = cluster_heap_fetch_owned(&f->relation, &f->snapshot, &f->tuple, &f->buffer,
											  keep, f->owned.data, &f->remote, &f->locator);
		else if (api == 1)
			result = heap_fetch(&f->relation, &f->snapshot, &f->tuple, &f->buffer, keep);
		else
			result = cluster_heap_fetch_waitable(&f->relation, &f->snapshot, &f->tuple, &f->buffer,
												 keep, &f->remote, &f->locator);
	}
	PG_CATCH();
	{
		result = -1;
	}
	PG_END_TRY();
	ut_error_armed = false;
	UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL && !scan_locked);
	return result;
}

static void
point_finish(PointFixture *f)
{
	if (scan_pins > 0)
		ReleaseBuffer(1);
	scan_finish();
}

UT_TEST(point_callback_copies_exact_full_versions)
{
	for (unsigned kind = 0; kind <= 6; kind++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		HeapTupleHeader expected;
		int result;
		if (!scan_load(kind))
			continue;
		point_setup(&f, 3);
		expected = (HeapTupleHeader)PageGetItem((Page)scan_fixture.full.data,
												PageGetItemId((Page)scan_fixture.full.data, 3));
		result = point_next(&f, true, false, 0);
		UT_ASSERT_EQ(result, 1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_live_calls + point_wait_calls + point_predicates + scan_conflicts, 0);
		UT_ASSERT_EQ(scan_pins, 0);
		if (result == 1) {
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self), 3);
			UT_ASSERT_EQ(memcmp(scan_stored_tuple.t_data, expected, scan_stored_tuple.t_len), 0);
			memset(ut_visibility_page.data, 0xa5, BLCKSZ);
			UT_ASSERT_EQ(memcmp(scan_stored_tuple.t_data, expected, scan_stored_tuple.t_len), 0);
		}
		point_finish(&f);
	}
}

UT_TEST(point_fetch_never_follows_hot_root)
{
	for (unsigned kind = 1; kind <= 6; kind++) {
		if (!scan_load(kind))
			continue;
		for (unsigned member = 0; member < 2; member++) {
			PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
			bool visible = member && kind != 2 && kind != 4;
			point_setup(&f, member ? 12 : 1);
			UT_ASSERT_EQ(point_next(&f, false, false, 0), visible);
			UT_ASSERT_EQ(scan_fetches, 1);
			UT_ASSERT_EQ(scan_pins, visible ? 1 : 0);
			UT_ASSERT_EQ(f.buffer, visible ? 1 : InvalidBuffer);
			UT_ASSERT(!f.remote);
			if (visible) {
				UT_ASSERT(f.tuple.t_data == (HeapTupleHeader)f.owned.data);
				UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&f.tuple.t_self), 12);
			} else
				UT_ASSERT(f.tuple.t_data == NULL);
			point_finish(&f);
		}
	}
}

UT_TEST(point_keep_buf_preserves_only_exact_invisible_tuple)
{
	if (!scan_load(6))
		return;
	for (unsigned absent = 0; absent < 3; absent++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		point_setup(&f, absent == 0 ? 1 : absent == 1 ? 20 : MaxHeapTuplesPerPage + 1);
		UT_ASSERT_EQ(point_next(&f, false, true, 0), 0);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_pins, absent == 0 ? 1 : 0);
		UT_ASSERT_EQ(f.buffer, absent == 0 ? 1 : InvalidBuffer);
		if (absent == 0) {
			UT_ASSERT(f.tuple.t_data == (HeapTupleHeader)f.owned.data);
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&f.tuple.t_self), 1);
		} else
			UT_ASSERT(f.tuple.t_data == NULL);
		UT_ASSERT(!f.remote);
		point_finish(&f);
	}
}

UT_TEST(incomplete_proof_refuses_even_unrequested_operation)
{
	TestOwnerFullFixture saved;
	if (!scan_load(0))
		return;
	saved = scan_fixture;
	for (unsigned fault = 0; fault < 7; fault++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		ClusterR4ProjectionBinding binding;
		unsigned before = ut_vis_unknown_count;
		scan_fixture = saved;
		if (fault == 0)
			scan_fixture.projection.valid = false;
		if (fault == 1)
			scan_fixture.projection.wire.bytes[12] ^= 1;
		if (fault == 2) {
			unsigned count = ClusterR4OperationRead(saved.projection.wire.bytes + 8, 2);
			UT_ASSERT(ClusterR4ProjectionDecode(&saved.projection.wire,
												saved.projection.reply_header, saved.full.data,
												saved.projection.wire.bytes + 140, &binding));
			UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, count - 1, &binding,
											  scan_fixture.projection.reply_header,
											  scan_fixture.full.data));
		}
		if (fault == 3)
			((PageHeader)scan_fixture.full.data)->pd_lower = 0;
		point_setup(&f, 3);
		if (fault >= 4) {
			scan_fetch_result
				= fault == 4 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason
				= fault == 5 ? CLUSTER_CR_BUILD_NONE : CLUSTER_CR_BUILD_EPOCH_MISMATCH;
		}
		UT_ASSERT_EQ(point_next(&f, true, false, 0), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + point_wait_calls, 0);
		UT_ASSERT_EQ(scan_pins, 0);
		point_finish(&f);
	}
	scan_fixture = saved;
}

UT_TEST(point_cancellation_and_horizon_keep_error_and_release_pin)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 3; fault++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		point_setup(&f, 3);
		if (fault == 0)
			scan_fetch_cancel = true;
		if (fault == 1) {
			scan_fetch_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
			scan_fetch_reason = CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
		}
		if (fault == 2)
			ut_current_epoch++;
		UT_ASSERT_EQ(point_next(&f, false, false, 0), -1);
		UT_ASSERT_EQ(ut_error_code, fault == 0	 ? ERRCODE_QUERY_CANCELED
									: fault == 1 ? ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD
												 : ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + point_wait_calls, 0);
		UT_ASSERT_EQ(scan_pins, 0);
		UT_ASSERT(f.tuple.t_data == NULL && f.buffer == InvalidBuffer);
		point_finish(&f);
	}
}

UT_TEST(buffer_only_wrappers_refuse_owned_full_tuple)
{
	if (!scan_load(0))
		return;
	for (unsigned api = 1; api < 3; api++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		unsigned before = ut_vis_unknown_count;
		point_setup(&f, 3);
		UT_ASSERT_EQ(point_next(&f, false, false, api), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_pins + scan_live_calls, 0);
		UT_ASSERT(f.tuple.t_data == NULL && f.buffer == InvalidBuffer);
		point_finish(&f);
	}
}

UT_TEST(buffer_only_proven_absence_keeps_false_contract)
{
	if (!scan_load(6))
		return;
	for (unsigned api = 1; api < 3; api++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		point_setup(&f, 1);
		UT_ASSERT_EQ(point_next(&f, false, false, api), 0);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_pins + scan_live_calls, 0);
		UT_ASSERT(f.tuple.t_data == NULL && f.buffer == InvalidBuffer);
		point_finish(&f);
	}
}

UT_TEST(point_slot_error_releases_the_owned_fetch_pin)
{
	PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
	if (!scan_load(0))
		return;
	point_setup(&f, 3);
	scan_live_visible = true; /* Also reaches the old callback's store in RED. */
	point_store_error = true;
	UT_ASSERT_EQ(point_next(&f, true, false, 0), -1);
	UT_ASSERT_EQ(ut_error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(scan_pins, 0);
	point_finish(&f);
}

UT_TEST(point_serializable_full_refuses_but_dormant_stays_native)
{
	if (!scan_load(0))
		return;
	for (unsigned dormant = 0; dormant < 2; dormant++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		point_setup(&f, 3);
		XactIsoLevel = XACT_SERIALIZABLE;
		if (dormant) {
			scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
			scan_live_visible = true;
		}
		UT_ASSERT_EQ(point_next(&f, true, false, 0), dormant ? 1 : -1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_live_calls, dormant);
		UT_ASSERT_EQ(scan_conflicts, dormant);
		UT_ASSERT_EQ(scan_pins, 0);
		point_finish(&f);
	}
}

UT_TEST(point_native_local_non_mvcc_and_dirty_wait_keep_original_paths)
{
	if (!scan_load(0))
		return;
	for (unsigned control = 0; control < 6; control++) {
		PointFixture f = { .slot.base.base.tts_ops = &TTSOpsBufferHeapTuple };
		point_setup(&f, 3);
		if (control == 0) {
			scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
		}
		if (control == 1)
			f.snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
		if (control == 2)
			f.form.relpersistence = RELPERSISTENCE_TEMP;
		if (control == 3)
			f.snapshot.snapshot_type = SNAPSHOT_ANY;
		if (control == 4)
			cluster_shared_config = cluster_shared_catalog = cluster_enabled = false;
		if (control == 5)
			f.snapshot.snapshot_type = SNAPSHOT_DIRTY;
		scan_live_visible = true;
		UT_ASSERT_EQ(point_next(&f, control != 5, false, 0), 1);
		UT_ASSERT_EQ(scan_fetches, control == 0 ? 1 : 0);
		UT_ASSERT_EQ(scan_live_calls, 1);
		UT_ASSERT_EQ(point_pin_stores, control == 2 || control == 4);
		UT_ASSERT_EQ(point_wait_calls, control == 5);
		if (control == 5)
			UT_ASSERT(!f.remote);
		point_finish(&f);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(point_callback_copies_exact_full_versions);
	UT_RUN(point_fetch_never_follows_hot_root);
	UT_RUN(point_keep_buf_preserves_only_exact_invisible_tuple);
	UT_RUN(incomplete_proof_refuses_even_unrequested_operation);
	UT_RUN(point_cancellation_and_horizon_keep_error_and_release_pin);
	UT_RUN(buffer_only_wrappers_refuse_owned_full_tuple);
	UT_RUN(buffer_only_proven_absence_keeps_false_contract);
	UT_RUN(point_slot_error_releases_the_owned_fetch_pin);
	UT_RUN(point_serializable_full_refuses_but_dormant_stays_native);
	UT_RUN(point_native_local_non_mvcc_and_dirty_wait_keep_original_paths);
	UT_DONE();
	return ut_failed_count != 0;
}
