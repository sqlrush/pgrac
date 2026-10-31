/*-------------------------------------------------------------------------
 * test_cluster_heap_bitmap_full.c
 *    Execute original bitmap callbacks with private FULL projections.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Storage, transport and native visibility are explicit boundary doubles.
 * FULL bytes use the actual holder, server and registered reply landing.
 *-------------------------------------------------------------------------
 */
#define CLUSTER_SCAN_FULL_ENTRY bitmap_unused_scan_main
int bitmap_unused_scan_main(void);
#include "test_cluster_heap_scan_full.c"
#include "nodes/tidbitmap.h"
#include "storage/predicate.h"

static unsigned bitmap_native_hot, bitmap_predicates;

Buffer
ReleaseAndReadBuffer(Buffer buffer, Relation relation, BlockNumber block)
{
	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);
	return ReadBufferExtended(relation, MAIN_FORKNUM, block, RBM_NORMAL, NULL);
}

bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
					   HeapTuple tuple, bool *all_dead, bool first_call)
{
	UT_ASSERT(scan_locked && first_call);
	bitmap_native_hot++;
	return scan_live_visible;
}

void
PredicateLockTID(Relation relation, ItemPointer tid, Snapshot snapshot, TransactionId xmin)
{
	UT_ASSERT(scan_locked);
	bitmap_predicates++;
}

#define HeapTupleSatisfiesVisibility scan_native_visibility
#define HeapCheckForSerializableConflictOut scan_conflict
#include "test_cluster_heap_bitmap_callbacks.inc"
#undef HeapTupleSatisfiesVisibility
#undef HeapCheckForSerializableConflictOut

static TBMIterateResult *
bitmap_input(bool lossy)
{
	static const OffsetNumber roots[] = { 1, 3, 5, 20 };
	TBMIterateResult *input = calloc(1, offsetof(TBMIterateResult, offsets)
											+ (MaxHeapTuplesPerPage + 1) * sizeof(OffsetNumber));
	input->blockno = scan_fixture.tag.blockNum;
	input->ntuples = lossy ? -1 : lengthof(roots);
	input->recheck = lossy;
	memcpy(input->offsets, roots, sizeof(roots));
	bitmap_native_hot = bitmap_predicates = 0;
	return input;
}

static void
bitmap_setup(HeapScanDesc scan, Relation relation, Form_pg_class form, Snapshot snapshot)
{
	scan_setup(scan, relation, form, snapshot, true, false);
	scan->rs_base.rs_flags = SO_TYPE_BITMAPSCAN | SO_ALLOW_PAGEMODE;
}

/* Catch outside both production consumer frames, as the executor does. */
static int
bitmap_next(HeapScanDesc scan, TBMIterateResult *input, bool block)
{
	TupleTableSlot slot = { 0 };
	volatile int result = -1;
	ut_error_code = 0;
	ut_error_armed = true;
	PG_TRY();
	{
		result = block ? heapam_scan_bitmap_next_block((TableScanDesc)scan, input)
					   : heapam_scan_bitmap_next_tuple((TableScanDesc)scan, input, &slot);
	}
	PG_CATCH();
	{
		result = -1;
	}
	PG_END_TRY();
	ut_error_armed = false;
	UT_ASSERT(PG_exception_stack == NULL && ut_snapshot_scope == NULL);
	return result;
}

static void
bitmap_check(unsigned kind, bool lossy)
{
	HeapScanDescData scan;
	RelationData relation;
	FormData_pg_class form;
	SnapshotData snapshot;
	TBMIterateResult *input;
	OffsetNumber expected[13];
	unsigned count = 0, expected_count = 0;
	int result;
	if (!scan_load(kind))
		return;
	input = bitmap_input(lossy);
	if (lossy) {
		for (OffsetNumber off = kind == 0 ? 1 : 2; off <= 13; off++)
			if ((kind != 2 && kind != 4) || off != 12)
				expected[expected_count++] = off;
	} else {
		if (kind == 0 || kind >= 5)
			expected[expected_count++] = kind == 0 ? 1 : 12;
		expected[expected_count++] = 3;
		expected[expected_count++] = 5;
	}
	bitmap_setup(&scan, &relation, &form, &snapshot);
	UT_ASSERT_EQ(bitmap_next(&scan, input, true), 1);
	while ((result = bitmap_next(&scan, input, false)) == 1) {
		UT_ASSERT(count < expected_count);
		if (count < expected_count)
			UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&scan_stored_tuple.t_self), expected[count]);
		count++;
		/* A live-buffer install cannot change any later private tuple. */
		memset(ut_visibility_page.data, 0xa5, BLCKSZ);
	}
	if (result < 0)
		printf("# bitmap kind=%u lossy=%u: %s\n", kind, lossy, ut_error_message);
	UT_ASSERT_EQ(result, 0);
	UT_ASSERT_EQ(count, expected_count);
	UT_ASSERT_EQ(scan_fetches, 1);
	UT_ASSERT_EQ(scan_live_calls + bitmap_native_hot + scan_prunes + bitmap_predicates, 0);
	UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
	scan_cleanup(&scan);
	free(input);
}

UT_TEST(exact_bitmap_uses_sparse_full_roots)
{
	for (unsigned kind = 0; kind <= 4; kind++)
		bitmap_check(kind, false);
}
UT_TEST(lossy_bitmap_selects_all_full_versions)
{
	for (unsigned kind = 0; kind <= 6; kind++)
		bitmap_check(kind, true);
}
UT_TEST(exact_bitmap_follows_v1_and_child_hot_chains)
{
	bitmap_check(5, false);
	bitmap_check(6, false);
}

UT_TEST(incomplete_full_refuses_before_any_bitmap_row)
{
	TestOwnerFullFixture saved;
	if (!scan_load(0))
		return;
	saved = scan_fixture;
	for (unsigned fault = 0; fault < 7; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		ClusterR4ProjectionBinding binding;
		unsigned before = ut_vis_unknown_count;
		TBMIterateResult *input = bitmap_input(fault % 2 == 0);
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
		bitmap_setup(&scan, &relation, &form, &snapshot);
		if (fault >= 4) {
			scan_fetch_result
				= fault == 4 ? CLUSTER_CR_BUILD_FAIL_CLOSED : CLUSTER_CR_BUILD_RETRYABLE;
			scan_fetch_reason
				= fault == 5 ? CLUSTER_CR_BUILD_NONE : CLUSTER_CR_BUILD_EPOCH_MISMATCH;
		}
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + bitmap_native_hot + scan_prunes, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
		free(input);
	}
	scan_fixture = saved;
}

/* Change only structural test bytes, then seal the original typed identities.
 * This is a malformed-producer boundary, never a production proof builder. */
static void
bitmap_reseal(void)
{
	ClusterR4ProjectionBinding binding;
	ClusterR4ProjectionWire original = scan_fixture.projection.wire;
	unsigned count = ClusterR4OperationRead(original.bytes + 8, 2);
	/* Decode against the original page retained by the fixture loader. */
	UT_ASSERT(ClusterR4ProjectionDecode(&original, scan_fixture.projection.reply_header,
										self_owner_full.full.data, original.bytes + 140, &binding));
	for (unsigned i = 0; i < count; i++) {
		ClusterR4ProjectionEntry entry;
		ItemId item;
		UT_ASSERT(ClusterR4ProjectionEntryGet(&original, i, &entry));
		item = PageGetItemId((Page)scan_fixture.full.data, entry.offset);
		entry.tuple_crc
			= ClusterR4ProjectionCrc(PageGetItem((Page)scan_fixture.full.data, item), entry.length);
		UT_ASSERT(ClusterR4ProjectionEntryPut(&scan_fixture.projection.wire, i, &entry));
	}
	UT_ASSERT(ClusterR4ProjectionSeal(&scan_fixture.projection.wire, count, &binding,
									  scan_fixture.projection.reply_header,
									  scan_fixture.full.data));
}

UT_TEST(malformed_exact_roots_and_hot_edges_refuse)
{
	TestOwnerFullFixture saved;
	if (!scan_load(5))
		return;
	saved = scan_fixture;
	for (unsigned fault = 0; fault < 8; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		TBMIterateResult *input = bitmap_input(false);
		unsigned before = ut_vis_unknown_count;
		scan_fixture = saved;
		bitmap_setup(&scan, &relation, &form, &snapshot);
		if (fault == 0)
			input->offsets[0] = 0;
		if (fault == 1)
			input->offsets[1] = input->offsets[0];
		if (fault == 2)
			input->ntuples = MaxHeapTuplesPerPage + 1;
		if (fault == 3)
			input->ntuples = -2;
		if (fault == 4)
			input->offsets[0] = MaxHeapTuplesPerPage + 1;
		if (fault == 5) {
			input->ntuples = 1;
			input->offsets[0] = 12; /* A heap-only member cannot be an index root. */
		}
		if (fault >= 6) {
			HeapTupleHeader root = (HeapTupleHeader)PageGetItem(
				(Page)scan_fixture.full.data, PageGetItemId((Page)scan_fixture.full.data, 1));
			ItemPointerSet(&root->t_ctid, fault == 6 ? input->blockno + 1 : input->blockno,
						   fault == 6 ? 12 : 1);
			bitmap_reseal();
		}
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + bitmap_native_hot + scan_prunes, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
		free(input);
	}
	scan_fixture = saved;
}

UT_TEST(retained_bitmap_rejects_snapshot_or_page_drift)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 9; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot, replacement;
		TBMIterateResult *input = bitmap_input(fault % 2 == 0);
		unsigned before = ut_vis_unknown_count;
		bitmap_setup(&scan, &relation, &form, &snapshot);
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), 1);
		if (fault == 0)
			snapshot.read_scn++;
		if (fault == 1)
			snapshot.read_epoch++;
		if (fault == 2)
			ut_current_epoch++;
		if (fault == 3)
			snapshot.curcid++;
		if (fault == 4)
			snapshot.snapshot_type = SNAPSHOT_ANY;
		if (fault == 5)
			snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
		if (fault == 6) {
			replacement = snapshot;
			scan.rs_base.rs_snapshot = &replacement;
		}
		if (fault == 7)
			scan.rs_owned_page[1024] ^= 1;
		if (fault == 8)
			scan.rs_projection.valid = false;
		UT_ASSERT_EQ(bitmap_next(&scan, input, false), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(scan_stores, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
		free(input);
	}
}

UT_TEST(empty_exact_selection_retires_page_proof)
{
	if (!scan_load(0))
		return;
	for (unsigned empty = 0; empty < 2; empty++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		PGAlignedBlock zero = { 0 };
		TBMIterateResult *input = bitmap_input(false);
		input->ntuples = empty ? 1 : 0;
		input->offsets[0] = 20; /* Continuous FULL scope proves a later absent root. */
		bitmap_setup(&scan, &relation, &form, &snapshot);
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), 0);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		UT_ASSERT_EQ(memcmp(scan.rs_owned_page, zero.data, BLCKSZ), 0);
		scan_cleanup(&scan);
		free(input);
	}
}

UT_TEST(bitmap_new_block_rescan_end_and_slot_lifetimes)
{
	if (!scan_load(0))
		return;
	for (unsigned stop = 0; stop < 4; stop++) {
		HeapScanDesc scan = malloc(sizeof(*scan));
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		PGAlignedBlock stored, zero = { 0 };
		TBMIterateResult *input = bitmap_input(false);
		bitmap_setup(scan, &relation, &form, &snapshot);
		UT_ASSERT_EQ(bitmap_next(scan, input, true), 1);
		UT_ASSERT_EQ(bitmap_next(scan, input, false), 1);
		stored = scan_stored;
		if (stop == 0) {
			scan->rs_projection.valid = false;
			UT_ASSERT_EQ(bitmap_next(scan, input, true), 1);
			UT_ASSERT_EQ(scan_fetches, 2);
			while (bitmap_next(scan, input, false) == 1) {}
		} else if (stop == 1) {
			heap_rescan((TableScanDesc)scan, NULL, false, false, false, false);
		} else if (stop == 2) {
			input->blockno = scan->rs_nblocks;
			UT_ASSERT_EQ(bitmap_next(scan, input, true), 0);
		} else {
			heap_endscan((TableScanDesc)scan);
			scan = NULL;
		}
		if (scan != NULL) {
			UT_ASSERT(!scan->rs_projection.valid && scan->rs_owned_kind == 0);
			UT_ASSERT_EQ(memcmp(scan->rs_owned_page, zero.data, BLCKSZ), 0);
			scan_cleanup(scan);
			free(scan);
		} else
			scan_finish();
		if (stop != 0)
			UT_ASSERT_EQ(memcmp(scan_stored.data, stored.data, BLCKSZ), 0);
		free(input);
	}
}

UT_TEST(bitmap_full_cancellation_and_horizon_keep_error_polarity)
{
	if (!scan_load(0))
		return;
	for (unsigned fault = 0; fault < 2; fault++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		TBMIterateResult *input = bitmap_input(false);
		bitmap_setup(&scan, &relation, &form, &snapshot);
		scan_fetch_cancel = fault == 0;
		if (fault == 1) {
			scan_fetch_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
			scan_fetch_reason = CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
		}
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), -1);
		UT_ASSERT_EQ(ut_error_code,
					 fault == 0 ? ERRCODE_QUERY_CANCELED : ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD);
		UT_ASSERT_EQ(scan_stores + scan_live_calls + bitmap_native_hot + scan_prunes, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
		free(input);
	}
}

UT_TEST(bitmap_native_and_dormant_paths_keep_locked_visibility)
{
	if (!scan_load(0))
		return;
	for (unsigned control = 0; control < 6; control++)
		for (unsigned lossy = 0; lossy < 2; lossy++) {
			HeapScanDescData scan;
			RelationData relation;
			FormData_pg_class form;
			SnapshotData snapshot;
			TBMIterateResult *input = bitmap_input(lossy);
			unsigned rows = 0;
			/* The native leaf deliberately doesn't emulate HOT or absent roots. */
			input->ntuples = lossy ? -1 : 3;
			bitmap_setup(&scan, &relation, &form, &snapshot);
			if (control == 0 || control == 5) {
				scan_fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
				scan_fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
			}
			if (control == 1)
				snapshot.cluster_source = SNAPSHOT_SOURCE_LOCAL;
			if (control == 2)
				form.relpersistence = RELPERSISTENCE_TEMP;
			if (control == 3)
				snapshot.snapshot_type = SNAPSHOT_ANY;
			if (control == 4)
				cluster_shared_config = cluster_shared_catalog = cluster_enabled = false;
			if (control == 5)
				XactIsoLevel = XACT_SERIALIZABLE;
			scan_live_visible = true;
			UT_ASSERT_EQ(bitmap_next(&scan, input, true), 1);
			while (bitmap_next(&scan, input, false) == 1)
				rows++;
			UT_ASSERT_EQ(rows, lossy ? 13 : 3);
			UT_ASSERT_EQ(scan_fetches, control == 0 || control == 5 ? 1 : 0);
			UT_ASSERT_EQ(bitmap_native_hot, lossy ? 0 : 3);
			UT_ASSERT_EQ(scan_live_calls, lossy ? 13 : 0);
			UT_ASSERT_EQ(bitmap_predicates, lossy ? 13 : 0);
			UT_ASSERT_EQ(scan_conflicts, lossy ? 13 : 0);
			UT_ASSERT_EQ(scan_prunes, 1);
			UT_ASSERT(!scan.rs_projection.valid);
			scan_cleanup(&scan);
			free(input);
		}
}

UT_TEST(serializable_bitmap_full_refuses_before_any_output)
{
	if (!scan_load(0))
		return;
	for (unsigned lossy = 0; lossy < 2; lossy++) {
		HeapScanDescData scan;
		RelationData relation;
		FormData_pg_class form;
		SnapshotData snapshot;
		TBMIterateResult *input = bitmap_input(lossy);
		unsigned before = ut_vis_unknown_count;
		bitmap_setup(&scan, &relation, &form, &snapshot);
		XactIsoLevel = XACT_SERIALIZABLE;
		UT_ASSERT_EQ(bitmap_next(&scan, input, true), -1);
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
		UT_ASSERT_EQ(ut_vis_unknown_count, before + 1);
		UT_ASSERT_EQ(scan_fetches, 1);
		UT_ASSERT_EQ(
			scan_stores + scan_live_calls + bitmap_native_hot + scan_conflicts + scan_prunes, 0);
		UT_ASSERT(!scan.rs_projection.valid && scan.rs_owned_kind == 0);
		scan_cleanup(&scan);
		free(input);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(exact_bitmap_uses_sparse_full_roots);
	UT_RUN(lossy_bitmap_selects_all_full_versions);
	UT_RUN(exact_bitmap_follows_v1_and_child_hot_chains);
	UT_RUN(incomplete_full_refuses_before_any_bitmap_row);
	UT_RUN(malformed_exact_roots_and_hot_edges_refuse);
	UT_RUN(retained_bitmap_rejects_snapshot_or_page_drift);
	UT_RUN(empty_exact_selection_retires_page_proof);
	UT_RUN(bitmap_new_block_rescan_end_and_slot_lifetimes);
	UT_RUN(bitmap_full_cancellation_and_horizon_keep_error_polarity);
	UT_RUN(bitmap_native_and_dormant_paths_keep_locked_visibility);
	UT_RUN(serializable_bitmap_full_refuses_before_any_output);
	UT_DONE();
	return ut_failed_count != 0;
}
