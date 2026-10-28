/*-------------------------------------------------------------------------
 * test_cluster_ctrc_census_hot.inc
 *     Real native HOT rule over complete packed census item views.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
typedef struct CensusHotFixture {
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterCtrcRecoveryHeapItemV1 items[MaxHeapTuplesPerPage];
	char bytes[MaxHeapTuplesPerPage][MAXALIGN(SizeofHeapTupleHeader) + 1];
} CensusHotFixture;

static void
census_hot_pack(const PGAlignedBlock *image, unsigned count, CensusHotFixture *out)
{
	memset(out, 0, sizeof(*out));
	memcpy(out->slots, ClusterPageGetItlSlots((Page)image->data), sizeof(out->slots));
	for (unsigned i = 0; i < count; i++) {
		ItemId item = PageGetItemId((Page)image->data, i + 1);

		out->items[i].item = *item;
		if (ItemIdHasStorage(item)) {
			out->items[i].tuple_length = ItemIdGetLength(item);
			out->items[i].tuple_bytes = out->bytes[i] + 1;
			memcpy(out->bytes[i] + 1, PageGetItem((Page)image->data, item), ItemIdGetLength(item));
		}
	}
}

UT_TEST(test_ctrc_census_hot_shares_complete_native_chain_and_redirect_rule)
{
	PGAlignedBlock image;
	CensusHotFixture view;
	char saved[sizeof(view)];

	hot_refs_native_page(&image, MaxHeapTuplesPerPage);
	for (unsigned i = 1; i < MaxHeapTuplesPerPage; i++) hot_refs_link(&image, i, i + 1, true);
	for (unsigned redirect = 0; redirect < 2; redirect++) {
		if (redirect) ItemIdSetRedirect(PageGetItemId((Page)image.data, 1), 2);
		census_hot_pack(&image, MaxHeapTuplesPerPage, &view);
		memcpy(saved, &view, sizeof(view));
		UT_ASSERT(cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items,
			MaxHeapTuplesPerPage, 23));
		UT_ASSERT(memcmp(saved, &view, sizeof(view)) == 0);
	}
}

UT_TEST(test_ctrc_census_hot_rejects_complete_view_bounds_and_malformed_items)
{
	PGAlignedBlock image;
	CensusHotFixture view;

	hot_refs_native_page(&image, 2); hot_refs_link(&image, 1, 2, true);
	census_hot_pack(&image, 2, &view);
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(NULL, view.items, 2, 23));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, NULL, 2, 23));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items,
		MaxHeapTuplesPerPage + 1, 23));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 2, InvalidBlockNumber));
	for (unsigned fault = 0; fault < 6; fault++) {
		census_hot_pack(&image, 2, &view);
		if (fault == 0) view.items[1].tuple_bytes = NULL;
		if (fault == 1) view.items[1].tuple_length--;
		if (fault == 2) ItemIdSetDead(&view.items[1].item);
		if (fault == 3) { ItemIdSetRedirect(&view.items[1].item, 1);
			view.items[1].tuple_bytes = NULL; view.items[1].tuple_length = 0; }
		if (fault == 4) view.items[1].tuple_length = BLCKSZ + 1;
		if (fault == 5) view.bytes[1][1 + offsetof(HeapTupleHeaderData, t_hoff)] = 0;
		UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 2, 23));
	}
}

UT_TEST(test_ctrc_census_hot_rejects_native_cycles_collisions_and_wrong_successors)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		PGAlignedBlock image;
		CensusHotFixture view;

		hot_refs_native_page(&image, 3); hot_refs_link(&image, 1, 2, true);
		if (fault == 0) hot_refs_link(&image, 3, 2, true);
		if (fault == 1) hot_refs_link(&image, 2, 1, true);
		if (fault == 2) ItemPointerSet(&hot_refs_tuple(&image, 1)->t_ctid, 24, 2);
		if (fault == 3) HeapTupleHeaderClearHeapOnly(hot_refs_tuple(&image, 2));
		if (fault == 4) HeapTupleHeaderSetXmin(hot_refs_tuple(&image, 2), 7002);
		census_hot_pack(&image, 3, &view);
		UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 3, 23));
	}
}

UT_TEST(test_ctrc_census_hot_preserves_history_active_and_actual_aborted_orphans)
{
	PGAlignedBlock image;
	CensusHotFixture view;

	hot_refs_native_page(&image, 2); hot_refs_link(&image, 1, 2, true);
	census_hot_pack(&image, 2, &view);
	view.slots[2].flags = ITL_FLAG_ACTIVE; view.slots[2].commit_scn = InvalidScn;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 2, 23));
	UT_ASSERT_EQ(view.slots[2].flags, ITL_FLAG_ACTIVE);
	/* An ACTIVE historical orphan cannot borrow the later canonical ABORT. */
	ItemIdSetUnused(&view.items[0].item);
	view.items[0].tuple_bytes = NULL; view.items[0].tuple_length = 0;
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 2, 23));
	view.slots[2].flags = ITL_FLAG_ABORTED;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_items_v1(view.slots, view.items, 2, 23));
	UT_ASSERT_EQ(view.slots[2].flags, ITL_FLAG_ABORTED);
}
