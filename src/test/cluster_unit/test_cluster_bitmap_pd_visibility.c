/* Complete lossy bitmap callbacks with original PD qualification/copy helpers.
 * Rows, PCM/SPACE/snapshot and exact-HOT services are controlled leaves.
 * This is not executor recheck, parallel SQL or transport/durability evidence.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int bitmap_pd_inherited_main(void);
#define HEAP_PD_VISIBILITY_MAIN bitmap_pd_inherited_main
#include "test_cluster_heap_pd_visibility.c"
#undef HEAP_PD_VISIBILITY_MAIN

#include "nodes/tidbitmap.h"

static bool bitmap_serializable;
static unsigned bitmap_predicates, bitmap_hot_calls, bitmap_fetches;

static Buffer
bitmap_read(Buffer previous, Relation rel, BlockNumber block)
{
	if (BufferIsValid(previous))
		pd_release(previous);
	return pd_read(rel, MAIN_FORKNUM, block, RBM_NORMAL, NULL);
}

static void
bitmap_predicate(Relation rel, ItemPointer tid, Snapshot snapshot,
				 TransactionId xmin)
{
	UT_ASSERT(pd_locked && rel == &relation && snapshot == &ios_snapshot);
	UT_ASSERT(ItemPointerGetOffsetNumber(tid) <= 2);
	(void)xmin;
	bitmap_predicates++;
}

static bool
bitmap_hot(ItemPointer tid, Relation rel, Buffer buffer, Snapshot snapshot,
		   HeapTuple tuple, bool *all_dead, bool first_call)
{
	UT_ASSERT(pd_locked && rel == &relation && buffer == 1);
	UT_ASSERT(snapshot == &ios_snapshot && all_dead == NULL && first_call);
	bitmap_hot_calls++;
	/* Controlled HOT result: the selected member differs from the root.
	 * Original HOT traversal has separate tests and is not executed here. */
	if (ItemPointerGetOffsetNumber(tid) != 1)
		return false;
	ItemPointerSetOffsetNumber(tid, 2);
	(void)tuple;
	return true;
}

#undef IsolationIsSerializable
#define IsolationIsSerializable() bitmap_serializable
#define ReleaseAndReadBuffer bitmap_read
#define PredicateLockTID bitmap_predicate
#define heap_hot_search_buffer bitmap_hot
#undef pgstat_count_heap_fetch
#define pgstat_count_heap_fetch(rel) ((void)(rel), bitmap_fetches++)
#include "test_cluster_bitmap_pd_visibility.inc"

static void
bitmap_reset(void)
{
	pd_reset();
	bitmap_serializable = false;
	bitmap_predicates = bitmap_hot_calls = bitmap_fetches = 0;
}

static bool
bitmap_page(TBMIterateResult *result)
{
	bool found = heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, result);
	UT_ASSERT(!pd_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	UT_ASSERT_EQ(pd_reads, 1);
	UT_ASSERT_EQ(pd_conflicts, result->ntuples < 0 ? 2 : 0);
	UT_ASSERT_EQ(pd_locks, pd_unlocks);
	UT_ASSERT_EQ(ios_reads, 0);
	return found;
}

UT_TEST(lossy_current_qualification_preserves_predicates_conflicts_and_offsets)
{
	for (int n = 0; n < 3; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1, .recheck = true};
		bitmap_reset();
		pd_own.pcm_state = n == 0 ? PCM_STATE_S :
			n == 1 ? PCM_STATE_X : PCM_STATE_READ_IMAGE;
		UT_ASSERT(bitmap_page(&result));
		UT_ASSERT_EQ(pd_checks, 0);
		UT_ASSERT_EQ(bitmap_predicates, 2);
		UT_ASSERT_EQ(pd_scan.rs_ntuples, 2);
		UT_ASSERT_EQ(pd_scan.rs_vistuples[0], 1);
		UT_ASSERT_EQ(pd_scan.rs_vistuples[1], 2);
		UT_ASSERT_EQ(pd_scan.rs_owned_kind, 1);
		UT_ASSERT_EQ(bitmap_hot_calls, 0);
		UT_ASSERT(result.recheck && result.ntuples == -1);
	}
}

UT_TEST(disk_one_current_zero_or_old_catalog_stays_precise)
{
	for (int n = 0; n < 4; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1};
		bitmap_reset();
		if (n == 0) PageClearAllVisible(pd_current.data);
		if (n == 1) control.catalog_version_no = 0;
		if (n == 2) control.catalog_version_no = 17;
		if (n == 3) ControlFile = NULL;
		UT_ASSERT(bitmap_page(&result));
		UT_ASSERT_EQ(pd_checks, 2);
		UT_ASSERT_EQ(bitmap_predicates, 1);
		UT_ASSERT_EQ(pd_scan.rs_ntuples, 1);
	}
}

UT_TEST(snapshot_identity_command_or_admission_drift_uses_precise_checks)
{
	for (int n = 1; n <= 5; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1};
		bitmap_reset();
		pd_drift = n;
		UT_ASSERT(bitmap_page(&result));
		UT_ASSERT_EQ(pd_checks, 2);
		UT_ASSERT_EQ(bitmap_predicates, 1);
	}
}

UT_TEST(unknown_services_or_noncurrent_unbound_grant_stays_precise)
{
	for (int n = 0; n < 14; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1};
		bitmap_reset();
		if (n == 0) ios_admission = false;
		if (n == 1) ios_space = false;
		if (n == 2) ios_snapshot_live = false;
		if (n == 3) ios_pcm = false;
		if (n == 4) pd_own.buffer_type = BUF_TYPE_CR;
		if (n == 5) pd_own.buffer_type = BUF_TYPE_PI;
		if (n == 6) pd_own.pcm_state = PCM_STATE_N;
		if (n == 7) pd_own.flags = PCM_OWN_FLAG_REVOKING;
		if (n == 8) pd_own.writer_activation_token = 9;
		if (n == 9) pd_own.resource_x_activation_generation = 9;
		if (n == 10) pd_own.tag.blockNum = 1;
		if (n == 11) pd_own.semantic_buf_state = BM_TAG_VALID;
		if (n == 12) pd_own.generation = 0;
		if (n == 13) pd_own.reservation_token = 0;
		UT_ASSERT(bitmap_page(&result));
		UT_ASSERT_EQ(pd_checks, 2);
	}
}

UT_TEST(non_mvcc_recovery_native_and_temporary_keep_original_checks)
{
	for (int n = 0; n < 6; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1};
		bitmap_reset();
		if (n == 0) ios_snapshot.snapshot_type = SNAPSHOT_ANY;
		if (n == 1) ios_snapshot.snapshot_type = SNAPSHOT_SELF;
		if (n == 2) ios_snapshot.snapshot_type = SNAPSHOT_NON_VACUUMABLE;
		if (n == 3) ios_snapshot.takenDuringRecovery = true;
		if (n == 4) cluster_enabled = cluster_shared_config = false;
		if (n == 5) relation.rd_rel->relpersistence = RELPERSISTENCE_TEMP;
		UT_ASSERT(bitmap_page(&result));
		UT_ASSERT_EQ(pd_checks, 2);
		UT_ASSERT_EQ(pd_prepares, 0);
	}
}

UT_TEST(saved_lossy_offsets_and_bytes_survive_current_replacement)
{
	TBMIterateResult result = {.blockno = 0, .ntuples = -1, .recheck = true};
	TupleTableSlot slot = {0};
	bitmap_reset();
	pd_replace = true;
	UT_ASSERT(bitmap_page(&result));
	for (int n = 1; n <= 2; n++)
	{
		UT_ASSERT(heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, &result, &slot));
		UT_ASSERT_EQ(pd_stored.data[SizeofHeapTupleHeader], '0' + n);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&pd_scan.rs_ctup.t_self), n);
	}
	UT_ASSERT(!heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, &result, &slot));
	UT_ASSERT_EQ(bitmap_fetches, 2);
	UT_ASSERT_EQ(pd_stores, 2);
	UT_ASSERT(result.recheck && result.ntuples == -1);
}

UT_TEST(saved_pd_does_not_expand_precisely_selected_bitmap_offsets)
{
	TBMIterateResult result = {.blockno = 0, .ntuples = -1};
	TupleTableSlot slot = {0};
	bitmap_reset();
	control.catalog_version_no = 0;
	pd_replace = true;
	UT_ASSERT(bitmap_page(&result));
	UT_ASSERT(PageIsAllVisible(pd_scan.rs_owned_page));
	UT_ASSERT(heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, &result, &slot));
	UT_ASSERT_EQ(pd_stored.data[SizeofHeapTupleHeader], '1');
	UT_ASSERT(!heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, &result, &slot));
	UT_ASSERT_EQ(pd_checks, 2);
	UT_ASSERT_EQ(pd_stores, 1);
}

UT_TEST(exact_bitmap_retains_original_hot_returned_offset)
{
	PGAlignedBlock storage = {0};
	TBMIterateResult *result = (TBMIterateResult *)storage.data;
	TupleTableSlot slot = {0};
	bitmap_reset();
	result->ntuples = 3;
	result->recheck = true;
	result->offsets[0] = 1;
	result->offsets[1] = 3;
	result->offsets[2] = 4;
	UT_ASSERT(bitmap_page(result));
	UT_ASSERT_EQ(bitmap_hot_calls, 3);
	UT_ASSERT_EQ(pd_prepares, 0);
	UT_ASSERT_EQ(bitmap_predicates, 0);
	UT_ASSERT_EQ(pd_scan.rs_ntuples, 1);
	UT_ASSERT(heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, result, &slot));
	UT_ASSERT_EQ(pd_stored.data[SizeofHeapTupleHeader], '2');
	UT_ASSERT(!heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, result, &slot));
	UT_ASSERT(result->recheck);
}

UT_TEST(lossy_page_exchange_releases_previous_pin_before_new_read)
{
	TBMIterateResult result = {.blockno = 0, .ntuples = -1};
	bitmap_reset();
	pd_scan.rs_cbuf = 1;
	pd_pins = 1;
	UT_ASSERT(bitmap_page(&result));
	UT_ASSERT_EQ(pd_pins, 1);
	UT_ASSERT_EQ(pd_prepares, 1);
}

UT_TEST(lossy_serializable_keeps_predicate_and_conflict_checks)
{
	TBMIterateResult result = {.blockno = 0, .ntuples = -1, .recheck = true};
	bitmap_reset();
	bitmap_serializable = true;
	UT_ASSERT(bitmap_page(&result));
	UT_ASSERT_EQ(pd_checks, 0);
	UT_ASSERT_EQ(bitmap_predicates, 2);
	UT_ASSERT_EQ(pd_conflicts, 2);
}

UT_TEST(empty_lossy_or_out_of_range_block_returns_no_tuples)
{
	TBMIterateResult result = {.blockno = 0, .ntuples = -1};
	bitmap_reset();
	ItemIdSetUnused(PageGetItemId(pd_current.data, 1));
	ItemIdSetDead(PageGetItemId(pd_current.data, 2));
	UT_ASSERT(!heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, &result));
	UT_ASSERT_EQ(pd_scan.rs_ntuples, 0);
	UT_ASSERT_EQ(pd_checks + pd_conflicts + bitmap_predicates, 0);
	bitmap_reset();
	result.blockno = 1;
	UT_ASSERT(!heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, &result));
	UT_ASSERT_EQ(pd_reads + pd_prepares + pd_locks, 0);
	UT_ASSERT_EQ(pd_scan.rs_ntuples, 0);
}

UT_TEST(lossy_error_exits_new_snapshot_evaluators)
{
	for (int n = 0; n < 2; n++)
	{
		TBMIterateResult result = {.blockno = 0, .ntuples = -1};
		volatile bool caught = false;
		bitmap_reset();
		pd_space_error = n == 0;
		ios_error = n == 1;
		expecting_error = true;
		PG_TRY();
		{
			heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, &result);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ios_evaluators, 0);
		UT_ASSERT_EQ(ios_enters, ios_exits);
		/* Existing pin/content-lock ERROR cleanup is ResourceOwner's job;
		 * controlled leaves do not execute that layer. */
		pd_locked = false;
		pd_pins = 0;
	}
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0)
	{
		UT_PLAN(3);
		UT_RUN(disk_one_current_zero_or_old_catalog_stays_precise);
		UT_RUN(saved_pd_does_not_expand_precisely_selected_bitmap_offsets);
		UT_RUN(exact_bitmap_retains_original_hot_returned_offset);
	}
	else
	{
		UT_PLAN(12);
		UT_RUN(lossy_current_qualification_preserves_predicates_conflicts_and_offsets);
		UT_RUN(disk_one_current_zero_or_old_catalog_stays_precise);
		UT_RUN(snapshot_identity_command_or_admission_drift_uses_precise_checks);
		UT_RUN(unknown_services_or_noncurrent_unbound_grant_stays_precise);
		UT_RUN(non_mvcc_recovery_native_and_temporary_keep_original_checks);
		UT_RUN(saved_lossy_offsets_and_bytes_survive_current_replacement);
		UT_RUN(saved_pd_does_not_expand_precisely_selected_bitmap_offsets);
		UT_RUN(exact_bitmap_retains_original_hot_returned_offset);
		UT_RUN(lossy_page_exchange_releases_previous_pin_before_new_read);
		UT_RUN(lossy_serializable_keeps_predicate_and_conflict_checks);
		UT_RUN(empty_lossy_or_out_of_range_block_returns_no_tuples);
		UT_RUN(lossy_error_exits_new_snapshot_evaluators);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
