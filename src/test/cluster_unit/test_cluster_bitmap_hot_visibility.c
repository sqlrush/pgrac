/* Real bitmap callbacks and bool HOT traversal with qualified PD consumers.
 * Snapshot/transport/tuple-visibility/MX authority are controlled leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int bitmap_hot_inherited_main(void);
#define BITMAP_PD_VISIBILITY_MAIN bitmap_hot_inherited_main
#define BITMAP_PD_REAL_HOT 1
#include "test_cluster_bitmap_pd_visibility.c"
#undef BITMAP_PD_VISIBILITY_MAIN

static PGAlignedBlock exact_storage;

static TBMIterateResult *
exact_reset(OffsetNumber root)
{
	TBMIterateResult *result = (TBMIterateResult *)exact_storage.data;
	bitmap_reset();
	memset(&exact_storage, 0, sizeof(exact_storage));
	result->ntuples = 1;
	result->recheck = true;
	result->offsets[0] = root;
	bitmap_visible_offset = 1;
	bitmap_peer = bitmap_mx_restart = bitmap_all_sure_dead = false;
	bitmap_mx_calls = bitmap_native_updaters = bitmap_finishes = 0;
	return result;
}

static HeapTupleHeader
exact_row(OffsetNumber offset)
{
	return (HeapTupleHeader)PageGetItem(pd_current.data,
		PageGetItemId(pd_current.data, offset));
}

static bool
exact_page(TBMIterateResult *result)
{
	bool found = heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, result);
	UT_ASSERT(!pd_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	UT_ASSERT_EQ(pd_locks, pd_unlocks);
	UT_ASSERT_EQ(ios_reads, 0);
	UT_ASSERT(result->recheck && result->ntuples > 0);
	return found;
}

static void
exact_chain(bool multi)
{
	HeapTupleHeader root = exact_row(1);
	HeapTupleHeader next = exact_row(2);
	PageClearAllVisible(pd_current.data);
	HeapTupleHeaderSetXmin(root, 500);
	HeapTupleHeaderSetXmax(root, 600);
	root->t_infomask2 |= HEAP_HOT_UPDATED;
	if (multi) root->t_infomask |= HEAP_XMAX_IS_MULTI;
	ItemPointerSet(&root->t_ctid, 0, 2);
	HeapTupleHeaderSetXmin(next, 600);
	next->t_infomask2 |= HEAP_ONLY_TUPLE;
	ItemPointerSet(&next->t_ctid, 0, 2);
	bitmap_visible_offset = 2;
	bitmap_peer = multi;
}

UT_TEST(qualified_exact_root_uses_original_hot_checks_without_visibility)
{
	for (int n = 0; n < 3; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		pd_own.pcm_state = n == 0 ? PCM_STATE_S :
			n == 1 ? PCM_STATE_X : PCM_STATE_READ_IMAGE;
		UT_ASSERT(exact_page(result));
		UT_ASSERT_EQ(pd_checks, 0);
		UT_ASSERT_EQ(pd_conflicts, 1);
		UT_ASSERT_EQ(bitmap_predicates, 1);
		UT_ASSERT_EQ(pd_scan.rs_vistuples[0], 2);
		UT_ASSERT_EQ(bitmap_mx_calls + bitmap_native_updaters, 0);
	}
}

UT_TEST(qualified_redirect_follows_original_root_before_returning_member)
{
	TBMIterateResult *result = exact_reset(1);
	ItemIdSetRedirect(PageGetItemId(pd_current.data, 1), 2);
	exact_row(2)->t_infomask2 |= HEAP_ONLY_TUPLE;
	UT_ASSERT(exact_page(result));
	UT_ASSERT_EQ(pd_scan.rs_vistuples[0], 2);
	UT_ASSERT_EQ(pd_checks, 0);
	UT_ASSERT_EQ(pd_conflicts, 1);
	UT_ASSERT_EQ(bitmap_predicates, 1);
}

UT_TEST(qualified_pd_never_creates_stale_or_heap_only_root_tids)
{
	for (int n = 0; n < 5; n++)
	{
		OffsetNumber roots[] = {3, 4, 5, 2, 1};
		TBMIterateResult *result = exact_reset(roots[n]);
		if (n == 3) exact_row(2)->t_infomask2 |= HEAP_ONLY_TUPLE;
		if (n == 4) ItemIdSetRedirect(PageGetItemId(pd_current.data, 1), 4);
		UT_ASSERT(!exact_page(result));
		UT_ASSERT_EQ(pd_scan.rs_ntuples, 0);
		UT_ASSERT_EQ(pd_checks + pd_conflicts + bitmap_predicates, 0);
	}
}

UT_TEST(exact_old_catalog_disk_one_current_zero_or_unknown_stays_precise)
{
	for (int n = 0; n < 8; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		if (n == 0) PageClearAllVisible(pd_current.data);
		if (n == 1) control.catalog_version_no = 0;
		if (n == 2) control.catalog_version_no = 17;
		if (n == 3) ControlFile = NULL;
		if (n == 4) ios_space = false;
		if (n == 5) ios_admission = false;
		if (n == 6) ios_snapshot_live = false;
		if (n == 7) ios_pcm = false;
		UT_ASSERT(!exact_page(result));
		UT_ASSERT_EQ(pd_checks, 1);
		UT_ASSERT_EQ(pd_conflicts, 1);
		UT_ASSERT_EQ(bitmap_predicates, 0);
	}
}

UT_TEST(exact_identity_or_command_drift_does_not_borrow_permission)
{
	for (int n = 1; n <= 5; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		pd_drift = n;
		UT_ASSERT(!exact_page(result));
		UT_ASSERT_EQ(pd_checks, 1);
	}
}

UT_TEST(exact_noncurrent_fenced_unbound_images_stay_precise)
{
	for (int n = 0; n < 10; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		if (n == 0) pd_own.buffer_type = BUF_TYPE_CR;
		if (n == 1) pd_own.buffer_type = BUF_TYPE_PI;
		if (n == 2) pd_own.pcm_state = PCM_STATE_N;
		if (n == 3) pd_own.flags = PCM_OWN_FLAG_REVOKING;
		if (n == 4) pd_own.writer_activation_token = 9;
		if (n == 5) pd_own.resource_x_activation_generation = 9;
		if (n == 6) pd_own.tag.blockNum = 1;
		if (n == 7) pd_own.semantic_buf_state = BM_TAG_VALID;
		if (n == 8) pd_own.generation = 0;
		if (n == 9) pd_own.reservation_token = 0;
		UT_ASSERT(!exact_page(result));
		UT_ASSERT_EQ(pd_checks, 1);
	}
}

UT_TEST(exact_non_mvcc_recovery_native_or_temporary_stays_precise)
{
	for (int n = 0; n < 6; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		if (n == 0) ios_snapshot.snapshot_type = SNAPSHOT_ANY;
		if (n == 1) ios_snapshot.snapshot_type = SNAPSHOT_SELF;
		if (n == 2) ios_snapshot.snapshot_type = SNAPSHOT_NON_VACUUMABLE;
		if (n == 3) ios_snapshot.takenDuringRecovery = true;
		if (n == 4) cluster_enabled = cluster_shared_config = false;
		if (n == 5) relation.rd_rel->relpersistence = RELPERSISTENCE_TEMP;
		UT_ASSERT(!exact_page(result));
		UT_ASSERT_EQ(pd_checks, 1);
		UT_ASSERT_EQ(pd_prepares, 0);
	}
}

UT_TEST(unqualified_exact_real_hot_chain_retains_xmin_link_check)
{
	for (int n = 0; n < 2; n++)
	{
		TBMIterateResult *result = exact_reset(1);
		exact_chain(false);
		if (n) HeapTupleHeaderSetXmin(exact_row(2), 601);
		UT_ASSERT_EQ(exact_page(result), n == 0);
		UT_ASSERT_EQ(pd_checks, n == 0 ? 2 : 1);
		UT_ASSERT_EQ(bitmap_predicates, n == 0 ? 1 : 0);
		UT_ASSERT_EQ(bitmap_native_updaters, 1);
		if (!n) UT_ASSERT_EQ(pd_scan.rs_vistuples[0], 2);
	}
}

UT_TEST(unqualified_exact_mx_restart_keeps_original_precise_path)
{
	for (int n = 0; n < 2; n++)
	{
		TBMIterateResult *result = exact_reset(1);
		exact_chain(true);
		bitmap_mx_restart = n != 0;
		UT_ASSERT(exact_page(result));
		UT_ASSERT_EQ(pd_scan.rs_vistuples[0], 2);
		UT_ASSERT_EQ(bitmap_mx_calls, n == 0 ? 1 : 2);
		UT_ASSERT_EQ(pd_checks, n == 0 ? 2 : 3);
		UT_ASSERT_EQ(bitmap_native_updaters, 0);
	}
}

UT_TEST(public_hot_api_keeps_first_call_and_all_dead_contract)
{
	for (int n = 0; n < 3; n++)
	{
		HeapTupleData tuple;
		ItemPointerData tid;
		bool all_dead = true;
		exact_reset(2);
		if (n == 1) exact_chain(false);
		if (n == 2)
		{
			cluster_enabled = cluster_shared_config = false;
			bitmap_all_sure_dead = true;
		}
		pd_scan.rs_cbuf = bitmap_read(InvalidBuffer, &relation, 0);
		pd_lock(1, BUFFER_LOCK_SHARE);
		ItemPointerSet(&tid, 0, n == 1 ? 1 : 2);
		UT_ASSERT_EQ(heap_hot_search_buffer(&tid, &relation, 1, &ios_snapshot,
			&tuple, &all_dead, n != 1), n == 1);
		UT_ASSERT_EQ(all_dead, n == 2);
		UT_ASSERT_EQ(pd_checks, 1);
		pd_lock(1, BUFFER_LOCK_UNLOCK);
	}
}

UT_TEST(exact_root_and_redirect_do_not_duplicate_heap_only_member)
{
	TBMIterateResult *result = exact_reset(1);
	TupleTableSlot slot = {0};
	result->ntuples = 2;
	result->offsets[1] = 2;
	ItemIdSetRedirect(PageGetItemId(pd_current.data, 1), 2);
	exact_row(2)->t_infomask2 |= HEAP_ONLY_TUPLE;
	pd_replace = true;
	UT_ASSERT(exact_page(result));
	UT_ASSERT_EQ(pd_scan.rs_ntuples, 1);
	UT_ASSERT(heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, result, &slot));
	UT_ASSERT_EQ(pd_stored.data[SizeofHeapTupleHeader], '2');
	UT_ASSERT(!heapam_scan_bitmap_next_tuple((TableScanDesc)&pd_scan, result, &slot));
	UT_ASSERT(result->recheck);
}

UT_TEST(exact_cancel_exits_new_snapshot_evaluators)
{
	for (int n = 0; n < 2; n++)
	{
		TBMIterateResult *result = exact_reset(2);
		volatile bool caught = false;
		pd_space_error = n == 0;
		ios_error = n == 1;
		expecting_error = true;
		PG_TRY();
		{
			heapam_scan_bitmap_next_block((TableScanDesc)&pd_scan, result);
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
		/* Existing heap pin/S ERROR cleanup remains ResourceOwner's job. */
		pd_locked = false;
		pd_pins = 0;
	}
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0)
	{
		UT_PLAN(4);
		UT_RUN(exact_old_catalog_disk_one_current_zero_or_unknown_stays_precise);
		UT_RUN(qualified_pd_never_creates_stale_or_heap_only_root_tids);
		UT_RUN(unqualified_exact_real_hot_chain_retains_xmin_link_check);
		UT_RUN(public_hot_api_keeps_first_call_and_all_dead_contract);
	}
	else
	{
		UT_PLAN(12);
		UT_RUN(qualified_exact_root_uses_original_hot_checks_without_visibility);
		UT_RUN(qualified_redirect_follows_original_root_before_returning_member);
		UT_RUN(qualified_pd_never_creates_stale_or_heap_only_root_tids);
		UT_RUN(exact_old_catalog_disk_one_current_zero_or_unknown_stays_precise);
		UT_RUN(exact_identity_or_command_drift_does_not_borrow_permission);
		UT_RUN(exact_noncurrent_fenced_unbound_images_stay_precise);
		UT_RUN(exact_non_mvcc_recovery_native_or_temporary_stays_precise);
		UT_RUN(unqualified_exact_real_hot_chain_retains_xmin_link_check);
		UT_RUN(unqualified_exact_mx_restart_keeps_original_precise_path);
		UT_RUN(public_hot_api_keeps_first_call_and_all_dead_contract);
		UT_RUN(exact_root_and_redirect_do_not_duplicate_heap_only_member);
		UT_RUN(exact_cancel_exits_new_snapshot_evaluators);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
