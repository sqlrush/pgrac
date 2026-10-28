/*-------------------------------------------------------------------------
 * test_cluster_ctrc_heap_refs.inc
 *     Real immutable native tuple/ITL reference binding.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static HeapTupleHeader
heap_refs_native_page(PGAlignedBlock *image)
{
	ClusterColdTtSlotInputV1 subject, subjects[2];
	PGAlignedBlock projected;
	ClusterItlSlotData *slots;
	HeapTupleHeader tuple;

	test_native_itl_image(&subject, image, 1);
	slots = ClusterPageGetItlSlots((Page)image->data);
	slots[2].flags = ITL_FLAG_COMMITTED; slots[2].commit_scn = 900;
	slots[3] = slots[2]; slots[3].xid = 7002;
	slots[3].undo_segment_head = uba_encode(subject.key.segment_id, 13, 46, 132);
	tuple = (HeapTupleHeader)PageGetItem((Page)image->data,
		PageGetItemId((Page)image->data, FirstOffsetNumber));
	tuple->t_infomask = 0; tuple->t_itl_slot_idx = 3;
	HeapTupleHeaderSetXmin(tuple, 7001); HeapTupleHeaderSetXmax(tuple, 7002);
	subjects[0] = subject; subjects[1] = subject;
	subjects[1].key.xid = subjects[1].slot.xid = 7002;
	subjects[1].key.slot_offset = 46;
	subjects[1].slot.first_undo_block = uba_encode(subject.key.segment_id, 40, 46, 100);
	UT_ASSERT_EQ(cluster_ctrc_recovery_itl_page_image_v1(subjects, 2,
		image->data, projected.data), CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL);
	UT_ASSERT(memcmp(image->data, projected.data, BLCKSZ) == 0);
	return tuple;
}

UT_TEST(test_ctrc_heap_refs_bind_distinct_creator_and_actual_deleter_itls)
{
	PGAlignedBlock image, saved;
	HeapTupleHeader tuple = heap_refs_native_page(&image);
	saved = image;
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
	/* Frozen xmin and invalid xmax retain the original native meanings. */
	HeapTupleHeaderSetXminFrozen(tuple); tuple->t_infomask |= HEAP_XMAX_INVALID;
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
}

UT_TEST(test_ctrc_heap_refs_do_not_use_hints_or_raw_xid_without_unique_data_itl)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		PGAlignedBlock image, saved;
		HeapTupleHeader tuple = heap_refs_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);
		if (fault == 0) HeapTupleHeaderSetXmin(tuple, 9900);
		if (fault == 1) { HeapTupleHeaderSetXmin(tuple, 9900); tuple->t_infomask |= HEAP_XMIN_COMMITTED; }
		if (fault == 2) { slots[4] = slots[2]; slots[4].undo_segment_head = uba_encode(33, 14, 45, 132); }
		if (fault == 3) { slots[2].flags = ITL_FLAG_LOCK_ONLY_COMMITTED; }
		if (fault == 4) { HeapTupleHeaderSetXmin(tuple, 9900); tuple->t_infomask |= HEAP_XMIN_INVALID; }
		saved = image;
		UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
		UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
	}
}

UT_TEST(test_ctrc_heap_refs_reject_wrong_deleter_slot_moved_mx_and_effective_locks)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		PGAlignedBlock image;
		HeapTupleHeader tuple = heap_refs_native_page(&image);
		if (fault == 0) tuple->t_itl_slot_idx = 2;
		if (fault == 1) tuple->t_itl_slot_idx = CLUSTER_ITL_INITRANS_DEFAULT;
		if (fault == 2) tuple->t_infomask |= HEAP_MOVED_OFF;
		if (fault == 3) tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		if (fault == 4) tuple->t_infomask |= HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		if (fault == 5) HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
		UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	}
}

UT_TEST(test_ctrc_heap_refs_reject_incomplete_itl_or_tuple_grammar)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		PGAlignedBlock image;
		HeapTupleHeader tuple = heap_refs_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);
		ItemId item = PageGetItemId((Page)image.data, FirstOffsetNumber);
		if (fault == 0) slots[2].flags = ITL_FLAG_ACTIVE;
		if (fault == 1) slots[7].xid = 9000;
		if (fault == 2) memset(&slots[3].undo_segment_head, 0, sizeof(UBA));
		if (fault == 3) tuple->t_hoff = SizeofHeapTupleHeader - 1;
		if (fault == 4) tuple->t_hoff = SizeofHeapTupleHeader + 1;
		if (fault == 5) ItemIdSetRedirect(item, 2);
		if (fault == 6) ItemIdSetNormal(item, ((PageHeader)image.data)->pd_upper + 1, SizeofHeapTupleHeader);
		UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	}
}

UT_TEST(test_ctrc_heap_refs_invalid_xmax_is_not_an_effective_reference)
{
	PGAlignedBlock image;
	HeapTupleHeader tuple = heap_refs_native_page(&image);
	tuple->t_infomask |= HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI;
	HeapTupleHeaderSetXmax(tuple, 9900); tuple->t_itl_slot_idx = UINT8_MAX;
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
}

UT_TEST(test_ctrc_heap_refs_reject_null_unaligned_and_new_images)
{
	PGAlignedBlock image[2] = {{0}};
	UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(NULL));
	UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image[0].data));
	heap_refs_native_page(&image[0]);
	UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image[0].data + 1));
}

UT_TEST(test_ctrc_heap_refs_native_header_alignment_and_null_bitmap_are_required)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		PGAlignedBlock image;
		HeapTupleHeader old = heap_refs_native_page(&image), tuple;
		PageHeader header = (PageHeader)image.data;
		unsigned start = header->pd_special - 64;

		memmove(image.data + start, old, SizeofHeapTupleHeader);
		header->pd_upper = start;
		ItemIdSetNormal(PageGetItemId((Page)image.data, FirstOffsetNumber), start, 64);
		tuple = (HeapTupleHeader)(image.data + start);
		UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
		if (fault == 0) tuple->t_hoff = 25;
		if (fault == 1) { tuple->t_infomask |= HEAP_HASNULL; HeapTupleHeaderSetNatts(tuple, 1); tuple->t_hoff = 24; }
		if (fault == 2) { tuple->t_infomask |= HEAP_HASNULL; HeapTupleHeaderSetNatts(tuple, 73); tuple->t_hoff = 32; }
		UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
		/* The original native one-column bitmap header occupies32 bytes. */
		tuple->t_infomask |= HEAP_HASNULL; HeapTupleHeaderSetNatts(tuple, 1); tuple->t_hoff = 32;
		UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	}
}
