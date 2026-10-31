/* Original native CTRC projection/tuple/HOT predicates in the same Startup.
 * PAGE transport, canonical TT input, locks and service remain controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static HeapTupleHeader
startup_native_image_tuple(void)
{
	Page page = (Page)startup_image_expected.data;
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
}

static void
startup_native_images_ready(bool unchanged)
{
	Page page = (Page)startup_image_expected.data;
	PageHeader header = (PageHeader)page;
	ClusterItlSlotData *slot;
	HeapTupleHeader tuple;
	const ClusterColdTtSlotInputV1 *subject;
	Size tuple_bytes = MAXALIGN(SizeofHeapTupleHeader);

	final_projection_ready(unchanged); final_projection_check = false;
	memset(&startup_image_expected, 0, sizeof(startup_image_expected));
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = header->pd_special - tuple_bytes;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ItemIdSetNormal(PageGetItemId(page, 1), header->pd_upper, tuple_bytes);
	subject = &native_ctrc_work.entries[0];
	slot = &ClusterPageGetItlSlots(page)[0];
	slot->xid = subject->key.xid; slot->wrap = 7; slot->flags = ITL_FLAG_ABORTED;
	slot->undo_segment_head = uba_encode(subject->key.segment_id, 3,
		subject->key.slot_offset, 16);
	tuple = startup_native_image_tuple();
	tuple->t_hoff = tuple_bytes;
	tuple->t_infomask = HEAP_XMAX_INVALID;
	tuple->t_itl_slot_idx = 0;
	HeapTupleHeaderSetXmin(tuple, subject->key.xid);
	ItemPointerSet(&tuple->t_ctid, 23, 1);
	memset(startup_native_image_calls, 0, sizeof(startup_native_image_calls));
	startup_native_images = true;
}

static void
startup_native_images_cleanup(void)
{
	startup_native_images = false;
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
	UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_native_images_accept_only_unchanged_complete_terminal_page)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		PGAlignedBlock original;
		ClusterColdTtSlotInputV1 subject;

		startup_native_images_ready(unchanged != 0);
		original = startup_image_expected; subject = native_ctrc_work.entries[0];
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
		for (unsigned i = 0; i < 3; i++) UT_ASSERT_EQ(startup_native_image_calls[i], 2);
		UT_ASSERT(memcmp(&original, &startup_image_expected, sizeof(original)) == 0);
		UT_ASSERT(memcmp(&subject, &native_ctrc_work.entries[0], sizeof(subject)) == 0);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
		startup_native_images_cleanup();
	}
}

UT_TEST(test_startup_native_images_reject_unresolved_ambiguous_and_mx_itls)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++)
		for (unsigned fault = 0; fault < 5; fault++) {
			ClusterItlSlotData *slots;

			startup_native_images_ready(unchanged != 0);
			slots = ClusterPageGetItlSlots((Page)startup_image_expected.data);
			if (fault == 0) slots[0].flags = ITL_FLAG_ACTIVE;
			if (fault == 1) slots[0].xid++;
			if (fault == 2) slots[0].flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			if (fault == 3) slots[1] = slots[0];
			if (fault == 4) memset(&slots[0].undo_segment_head, 0, sizeof(UBA));
			retirement_run_startup();
			UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
			UT_ASSERT_EQ(startup_native_image_calls[0], 1);
			UT_ASSERT_EQ(startup_native_image_calls[1], fault == 3 ? 1 : 0);
			UT_ASSERT_EQ(startup_native_image_calls[2], 0);
			startup_native_images_cleanup();
		}
}

UT_TEST(test_startup_native_images_reject_dangling_tuple_hot_and_residual_locks)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++)
		for (unsigned fault = 0; fault < 6; fault++) {
			HeapTupleHeader tuple;
			PGAlignedBlock original;

			startup_native_images_ready(unchanged != 0);
			tuple = startup_native_image_tuple();
			if (fault == 0) HeapTupleHeaderSetXmin(tuple, native_ctrc_work.entries[0].key.xid + 1);
			if (fault == 1) {
				tuple->t_infomask = 0;
				HeapTupleHeaderSetXmax(tuple, native_ctrc_work.entries[0].key.xid + 1);
			}
			if (fault == 2) {
				ClusterItlSlotData *slots = ClusterPageGetItlSlots((Page)startup_image_expected.data);
				slots[1] = slots[0]; slots[1].flags = ITL_FLAG_LOCK_ONLY_ABORTED;
				tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
				HeapTupleHeaderSetXmax(tuple, slots[1].xid);
			}
			if (fault == 3) tuple->t_infomask2 |= HEAP_ONLY_TUPLE;
			if (fault == 4) tuple->t_hoff++;
			if (fault == 5) {
				tuple->t_infomask = 0; tuple->t_infomask2 |= HEAP_HOT_UPDATED;
				HeapTupleHeaderSetXmax(tuple, native_ctrc_work.entries[0].key.xid);
			}
			original = startup_image_expected;
			retirement_run_startup();
			UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
			UT_ASSERT_EQ(startup_native_image_calls[0], 1);
			UT_ASSERT_EQ(startup_native_image_calls[1], fault == 2 ? 0 : 1);
			UT_ASSERT_EQ(startup_native_image_calls[2], fault == 3 || fault == 5 ? 1 : 0);
			UT_ASSERT(memcmp(&original, &startup_image_expected, sizeof(original)) == 0);
			startup_native_images_cleanup();
		}
}

UT_TEST(test_startup_native_images_wait_and_repeat_keep_same_fd_and_sticky_debt)
{
	unsigned steps;
	PGAlignedBlock original;

	startup_native_images_ready(false); completed_readback_mode = 4;
	original = startup_image_expected;
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached && !info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(ctrc_driver_waits, 2);
	for (unsigned i = 0; i < 3; i++) UT_ASSERT_EQ(startup_native_image_calls[i], 2);
	steps = prefix_steps;
	HeapTupleHeaderSetXmin(startup_native_image_tuple(), native_ctrc_work.entries[0].key.xid + 1);
	memset(startup_native_image_calls, 0, sizeof(startup_native_image_calls));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(startup_native_image_calls[0], 1); UT_ASSERT_EQ(startup_native_image_calls[1], 1);
	UT_ASSERT_EQ(startup_native_image_calls[2], 0);
	UT_ASSERT(info.cluster_tx_start->revoked);
	startup_image_expected = original;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, 2);
	startup_native_images_cleanup();
}
