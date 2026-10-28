/*-------------------------------------------------------------------------
 * test_cluster_ctrc_plain_lock_refs.inc
 *     Real native plain-lock tuple binding, independent of DATA update links.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static HeapTupleHeader
plain_lock_native_page(PGAlignedBlock *image)
{
	HeapTupleHeader tuple = heap_refs_native_page(image);
	ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image->data);

	slots[3].flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	/* The tuple's stored slot belongs to DATA, not its separate plain locker. */
	tuple->t_itl_slot_idx = 2;
	return tuple;
}

UT_TEST(test_ctrc_plain_lock_refs_bind_one_lock_carrier_without_using_data_slot)
{
	for (unsigned state = 0; state < 3; state++) {
		PGAlignedBlock image, original;
		HeapTupleHeader tuple = plain_lock_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);
		char packed[65] = {0}, saved[65];

		slots[3].flags = state == 0 ? ITL_FLAG_LOCK_ONLY_ACTIVE
			: state == 1 ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
		slots[3].commit_scn = state == 1 ? 900 : InvalidScn;
		if (state == 2) tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
		memcpy(packed + 1, tuple, SizeofHeapTupleHeader);
		memcpy(saved, packed, sizeof(saved)); original = image;
		UT_ASSERT(cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots, packed + 1, 64));
		UT_ASSERT(memcmp(saved, packed, sizeof(saved)) == 0);
		UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data) == (state != 0));
		UT_ASSERT(memcmp(&image, &original, sizeof(image)) == 0);
	}
}

UT_TEST(test_ctrc_plain_lock_refs_require_unique_valid_plain_lock_not_raw_data_or_multi)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		PGAlignedBlock image, original;
		HeapTupleHeader tuple = plain_lock_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);

		if (fault == 0) slots[3].flags = ITL_FLAG_COMMITTED;
		if (fault == 1) slots[3].xid++;
		if (fault == 2 || fault == 3) {
			slots[4] = slots[3];
			if (fault == 3) slots[4].wrap++;
		}
		if (fault == 4) memset(&slots[3].undo_segment_head, 0, sizeof(UBA));
		if (fault == 5) tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		if (fault == 6) HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
		original = image;
		UT_ASSERT(!cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
			(const char *)tuple, SizeofHeapTupleHeader));
		UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
		UT_ASSERT(memcmp(&image, &original, sizeof(image)) == 0);
	}
}

UT_TEST(test_ctrc_plain_lock_refs_never_make_local_update_edges_from_matching_raw_xids)
{
	PGAlignedBlock image, original;
	ClusterItlSlotData *slots;

	hot_refs_native_page(&image, 2);
	slots = ClusterPageGetItlSlots((Page)image.data);
	slots[3].flags = ITL_FLAG_LOCK_ONLY_COMMITTED; slots[3].xid = slots[2].xid;
	for (unsigned i = 1; i <= 2; i++) {
		HeapTupleHeader tuple = hot_refs_tuple(&image, i);

		tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		HeapTupleHeaderSetXmax(tuple, slots[3].xid); tuple->t_itl_slot_idx = 2;
		/* Were a locker an updater, these apparent links would form a cycle. */
		ItemPointerSet(&tuple->t_ctid, 23, i == 1 ? 2 : 1);
	}
	original = image;
	UT_ASSERT(cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
	UT_ASSERT(memcmp(&image, &original, sizeof(image)) == 0);
	/* A plain lock also cannot provide a missing heap-only predecessor. */
	HeapTupleHeaderSetHeapOnly(hot_refs_tuple(&image, 2));
	hot_refs_tuple(&image, 2)->t_infomask |= HEAP_UPDATED;
	UT_ASSERT(!cluster_ctrc_recovery_heap_hot_references_v1(image.data, 23));
}

UT_TEST(test_ctrc_plain_lock_refs_keep_original_projection_and_historical_binding_distinct)
{
	ClusterColdTtSlotInputV1 subject;
	PGAlignedBlock image, projected, original;
	HeapTupleHeader tuple;
	ClusterItlSlotData *slots;

	test_native_itl_image(&subject, &image, 2);
	tuple = hot_refs_tuple(&image, 1); tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	HeapTupleHeaderSetXminFrozen(tuple); tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	slots = ClusterPageGetItlSlots((Page)image.data); original = image;
	UT_ASSERT(cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
		(const char *)tuple, SizeofHeapTupleHeader));
	UT_ASSERT_EQ(cluster_ctrc_recovery_itl_cleanout_image_v1(&subject, 2, slots[2].wrap,
		2, image.data, projected.data), CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	UT_ASSERT(memcmp(&image, &original, sizeof(image)) == 0);
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)projected.data)[2].flags,
		ITL_FLAG_LOCK_ONLY_COMMITTED);
	UT_ASSERT((hot_refs_tuple(&projected, 1)->t_infomask & HEAP_XMAX_INVALID) != 0);
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(projected.data));
}
