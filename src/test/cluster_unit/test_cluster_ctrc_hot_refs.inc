/*-------------------------------------------------------------------------
 * test_cluster_ctrc_hot_refs.inc
 *     Real immutable native HOT and redirect reference topology.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static HeapTupleHeader
hot_refs_tuple(PGAlignedBlock *image, OffsetNumber offset)
{
	return (HeapTupleHeader)PageGetItem((Page)image->data,
		PageGetItemId((Page)image->data, offset));
}

static void
hot_refs_native_page(PGAlignedBlock *image, unsigned count)
{
	HeapTupleHeader seed = heap_refs_native_page(image);
	char header_bytes[MAXALIGN(SizeofHeapTupleHeader)];
	PageHeader header = (PageHeader)image->data;

	memset(header_bytes, 0, sizeof(header_bytes));
	memcpy(header_bytes, seed, SizeofHeapTupleHeader);
	header->pd_lower = SizeOfPageHeaderData + count * sizeof(ItemIdData);
	header->pd_upper = header->pd_special - count * sizeof(header_bytes);
	for (unsigned i = 1; i <= count; i++) {
		unsigned position = header->pd_special - i * sizeof(header_bytes);
		HeapTupleHeader tuple;

		memcpy(image->data + position, header_bytes, sizeof(header_bytes));
		ItemIdSetNormal(PageGetItemId((Page)image->data, i), position, sizeof(header_bytes));
		tuple = hot_refs_tuple(image, i);
		tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
		tuple->t_infomask = HEAP_XMAX_INVALID;
		tuple->t_infomask2 = 0;
		HeapTupleHeaderSetXmin(tuple, 7001);
		HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
		ItemPointerSet(&tuple->t_ctid, 23, i);
	}
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image->data));
}

static void
hot_refs_link(PGAlignedBlock *image, OffsetNumber from, OffsetNumber to, bool hot)
{
	HeapTupleHeader tuple = hot_refs_tuple(image, from);
	HeapTupleHeader successor = hot_refs_tuple(image, to);

	tuple->t_infomask &= ~HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmax(tuple, 7001); tuple->t_itl_slot_idx = 2;
	ItemPointerSet(&tuple->t_ctid, 23, to);
	successor->t_infomask |= HEAP_UPDATED;
	if (hot) {
		HeapTupleHeaderSetHotUpdated(tuple);
		HeapTupleHeaderSetHeapOnly(successor);
	}
}

UT_TEST(test_ctrc_hot_refs_accept_native_chain_redirect_and_bounded_maximum)
{
	PGAlignedBlock image, saved;

	hot_refs_native_page(&image, MaxHeapTuplesPerPage);
	for (unsigned i = 1; i < MaxHeapTuplesPerPage; i++) hot_refs_link(&image, i, i + 1, true);
	saved = image;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
	/* A pruned root redirects to its original heap-only successor. */
	ItemIdSetRedirect(PageGetItemId((Page)image.data, 1), 2);
	saved = image;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
}

UT_TEST(test_ctrc_hot_refs_reject_effective_hot_wrong_block_offset_or_successor)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		PGAlignedBlock image, saved;
		HeapTupleHeader tuple;

		hot_refs_native_page(&image, 3); hot_refs_link(&image, 1, 2, true);
		tuple = hot_refs_tuple(&image, 1);
		if (fault == 0) ItemPointerSet(&tuple->t_ctid, 24, 2);
		if (fault == 1) ItemPointerSet(&tuple->t_ctid, 23, 1);
		if (fault == 2) ItemPointerSet(&tuple->t_ctid, 23, 0);
		if (fault == 3) ItemPointerSet(&tuple->t_ctid, 23, 4);
		if (fault == 4) ItemIdSetDead(PageGetItemId((Page)image.data, 2));
		if (fault == 5) ItemIdSetRedirect(PageGetItemId((Page)image.data, 2), 3);
		if (fault == 6) HeapTupleHeaderSetXmin(hot_refs_tuple(&image, 2), 7002);
		saved = image;
		UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
		UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
	}
}

UT_TEST(test_ctrc_hot_refs_reject_intersections_redirect_collisions_and_cycles)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		PGAlignedBlock image;

		hot_refs_native_page(&image, 3); hot_refs_link(&image, 1, 2, true);
		if (fault == 0) hot_refs_link(&image, 3, 2, true);
		if (fault == 1) ItemIdSetRedirect(PageGetItemId((Page)image.data, 3), 2);
		if (fault == 2) hot_refs_link(&image, 2, 1, true);
		UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	}
}

UT_TEST(test_ctrc_hot_refs_require_heap_only_update_polarity_and_root)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		PGAlignedBlock image;

		hot_refs_native_page(&image, 2); hot_refs_link(&image, 1, 2, true);
		if (fault == 0) HeapTupleHeaderClearHeapOnly(hot_refs_tuple(&image, 2));
		if (fault == 1) hot_refs_tuple(&image, 2)->t_infomask &= ~HEAP_UPDATED;
		if (fault == 2) { ItemIdSetRedirect(PageGetItemId((Page)image.data, 1), 2);
			HeapTupleHeaderClearHeapOnly(hot_refs_tuple(&image, 2)); }
		if (fault == 3) HeapTupleHeaderClearHotUpdated(hot_refs_tuple(&image, 1));
		UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	}
}

UT_TEST(test_ctrc_hot_refs_keep_aborted_orphans_and_stale_ordinary_ctid_distinct)
{
	PGAlignedBlock image;
	HeapTupleHeader tuple;

	hot_refs_native_page(&image, 2); tuple = hot_refs_tuple(&image, 2);
	HeapTupleHeaderSetHeapOnly(tuple); tuple->t_infomask |= HEAP_UPDATED;
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	ClusterPageGetItlSlots((Page)image.data)[2].flags = ITL_FLAG_ABORTED;
	ClusterPageGetItlSlots((Page)image.data)[2].commit_scn = InvalidScn;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	/* Masked-aborted HOT is not an effective successor edge. */
	HeapTupleHeaderSetHotUpdated(hot_refs_tuple(&image, 1));
	ItemPointerSet(&hot_refs_tuple(&image, 1)->t_ctid, 24, 99);
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	for (unsigned variant = 0; variant < 4; variant++) {
		hot_refs_native_page(&image, 2); tuple = hot_refs_tuple(&image, 1);
		tuple->t_infomask &= ~HEAP_XMAX_INVALID;
		HeapTupleHeaderSetXmax(tuple, 7002); tuple->t_itl_slot_idx = 3;
		ItemPointerSet(&tuple->t_ctid, variant == 0 ? 24 : 23, variant == 1 ? 9 : (variant == 3 ? 0 : 2));
		/* Different xmin or a reused/outside location is not a local edge. */
		UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	}
}

UT_TEST(test_ctrc_hot_refs_refuse_invalid_images_and_unqualified_special_links)
{
	PGAlignedBlock image[2] = {{0}};

	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(NULL, 23));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image[0].data, 23));
	hot_refs_native_page(&image[0], 1);
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image[0].data + 1, 23));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image[0].data, InvalidBlockNumber));
	HeapTupleHeaderSetSpeculativeToken(hot_refs_tuple(&image[0], 1), 900);
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image[0].data, 23));
	HeapTupleHeaderSetMovedPartitions(hot_refs_tuple(&image[0], 1));
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image[0].data, 23));
}
