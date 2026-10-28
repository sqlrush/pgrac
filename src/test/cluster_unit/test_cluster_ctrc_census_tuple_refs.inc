/*-------------------------------------------------------------------------
 * test_cluster_ctrc_census_tuple_refs.inc
 *     Real native tuple binding on complete packed census bytes.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
UT_TEST(test_ctrc_census_tuple_refs_share_native_grammar_without_alignment_assumptions)
{
	PGAlignedBlock image;
	HeapTupleHeader tuple = heap_refs_native_page(&image);
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT], saved[CLUSTER_ITL_INITRANS_DEFAULT];
	char packed[65] = {0}, original[65];

	memcpy(slots, ClusterPageGetItlSlots((Page)image.data), sizeof(slots));
	/* Same historical identities; no rewrite or terminal proof is inferred. */
	slots[2].flags = ITL_FLAG_ACTIVE; slots[2].commit_scn = InvalidScn;
	slots[3].flags = ITL_FLAG_NEEDS_CLEANOUT;
	memcpy(packed + 1, tuple, SizeofHeapTupleHeader);
	memcpy(saved, slots, sizeof(saved)); memcpy(original, packed, sizeof(original));
	UT_ASSERT(cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots, packed + 1, 64));
	UT_ASSERT(memcmp(saved, slots, sizeof(saved)) == 0);
	UT_ASSERT(memcmp(original, packed, sizeof(original)) == 0);
	UT_ASSERT(cluster_ctrc_recovery_heap_itl_references_v1(image.data));
	ClusterPageGetItlSlots((Page)image.data)[2].flags = ITL_FLAG_ACTIVE;
	UT_ASSERT(!cluster_ctrc_recovery_heap_itl_references_v1(image.data));
}

UT_TEST(test_ctrc_census_tuple_refs_require_unique_data_creator_and_actual_updater)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		PGAlignedBlock image;
		HeapTupleHeader tuple = heap_refs_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);

		if (fault == 0) { HeapTupleHeaderSetXmin(tuple, 9999); tuple->t_infomask |= HEAP_XMIN_COMMITTED; }
		if (fault == 1) slots[4] = slots[2];
		if (fault == 2) slots[2].flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
		if (fault == 3) tuple->t_itl_slot_idx = 2;
		if (fault == 4) tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		if (fault == 5) tuple->t_infomask |= HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		if (fault == 6) tuple->t_infomask |= HEAP_MOVED_OFF;
		UT_ASSERT(!cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
			(const char *)tuple, SizeofHeapTupleHeader));
	}
}

UT_TEST(test_ctrc_census_tuple_refs_preserve_frozen_and_invalid_xmax_meanings)
{
	PGAlignedBlock image;
	HeapTupleHeader tuple = heap_refs_native_page(&image);
	ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);

	HeapTupleHeaderSetXminFrozen(tuple);
	tuple->t_infomask |= HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI;
	tuple->t_itl_slot_idx = UINT8_MAX; HeapTupleHeaderSetXmax(tuple, 9999);
	UT_ASSERT(cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
		(const char *)tuple, SizeofHeapTupleHeader));
	for (TransactionId xid = BootstrapTransactionId; xid <= FrozenTransactionId; xid++) {
		tuple->t_infomask &= ~HEAP_XMIN_FROZEN;
		HeapTupleHeaderSetXmin(tuple, xid);
		UT_ASSERT(cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots,
			(const char *)tuple, SizeofHeapTupleHeader));
	}
}

UT_TEST(test_ctrc_census_tuple_refs_reject_bounds_grammar_and_unqualified_itl_shapes)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		PGAlignedBlock image;
		HeapTupleHeader tuple = heap_refs_native_page(&image);
		ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)image.data);
		Size length = SizeofHeapTupleHeader;
		const char *bytes = (const char *)tuple;

		if (fault == 0) bytes = NULL;
		if (fault == 1) slots = NULL;
		if (fault == 2) length--;
		if (fault == 3) length = BLCKSZ + 1;
		if (fault == 4) tuple->t_hoff++;
		if (fault == 5) { tuple->t_infomask |= HEAP_HASNULL; HeapTupleHeaderSetNatts(tuple, 73); }
		if (fault == 6) slots[7].xid = 9999;
		if (fault == 7) slots[3].flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
		if (fault == 8) memset(&slots[3].undo_segment_head, 0, sizeof(UBA));
		UT_ASSERT(!cluster_ctrc_recovery_heap_tuple_itl_references_v1(slots, bytes, length));
	}
}
