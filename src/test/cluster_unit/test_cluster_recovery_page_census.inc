/* Actual SPACE/MAIN/TT/undo bytes through the joined cold page consumer.
 * Included by test_cluster_recovery_undo_store.c; no visibility success stubs.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static void
census_file(int root, const char *path, const void *bytes, size_t size)
{
	int fd = openat(root, path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, size) != size || close(fd) != 0)
		abort();
}

static int
census_files(unsigned fault)
{
	int root = store_files();
	ClusterRecoveryTxnKey key = store_key();
	ClusterSpaceReservation reservation = { 0 };
	PGAlignedBlock data, metadata[2], undo;
	PageHeader page = (PageHeader)data.data;
	HeapTupleHeader tuple;
	ClusterItlSlotData *itl;
	UndoRecordHeader *header;
	UndoBlockHeader *block;
	UndoSlotDirEntry *directory;
	UndoItlHistoryTrailer trailer = { 0 };
	UndoInsertPayload insert = { SizeofHeapTupleHeader, 0 };
	int fd;

	memset(&data, 0, sizeof(data));
	page->pd_flags = PD_HAS_ITL | PD_ALL_VISIBLE; /* hint is not a terminal proof */
	page->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	page->pd_special = BLCKSZ - MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE);
	page->pd_upper = page->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	PageSetPageSizeAndVersion(data.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ItemIdSetNormal(&page->pd_linp[0], page->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)(data.data + page->pd_upper);
	HeapTupleHeaderSetXmin(tuple, key.xid);
	HeapTupleHeaderSetXmax(tuple, 4195140);
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = 0;
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI;
	tuple->t_infomask2 = HEAP_HOT_UPDATED;
	ItemPointerSet(&tuple->t_ctid, 0, 1);
	itl = ClusterPageGetItlSlots(data.data);
	itl[0].xid = key.xid;
	itl[0].flags = ITL_FLAG_COMMITTED;
	itl[0].write_scn = 100;
	itl[0].commit_scn = 110;
	itl[0].undo_segment_head = uba_encode(257, 1, 6, 0);
	if (fault == 1)
		itl[0].xid++;
	if (fault == 2)
		itl[0].undo_segment_head = uba_encode(257, 2, 6, 0);
	if (fault == 3) {
		itl[1] = itl[0];
		itl[1].xid++; /* lower SCN, still must be enumerated before any inverse */
		itl[1].write_scn = 50;
	}
	if (fault == 5)
		ClusterPageGetItlHeader(data.data)->itl_recycle_watermark_scn = 90;
	if (fault == 6)
		itl[0].wrap = 1; /* a different transaction reused page ITL wrap zero */
	UT_ASSERT_EQ(mkdirat(root, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(root, "base/5", 0700), 0);
	census_file(root, "base/5/16385", data.data, BLCKSZ);
	reservation.identity.key.system_identifier = 817;
	reservation.identity.key.database_incarnation = 91;
	memcpy(reservation.identity.key.storage_uuid, store_uuid, 16);
	reservation.identity.key.locator = (RelFileLocator){ 1663, 5, 16385 };
	reservation.identity.incarnation[0] = 7;
	reservation.identity.sequence = 2;
	reservation.identity.operation = 9;
	reservation.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	reservation.next_block = 1;
	UT_ASSERT(
		cluster_space_identity_page_encode(&reservation.identity, 7, metadata[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 11, metadata[1].data, BLCKSZ));
	census_file(root, "base/5/16385_space", metadata, sizeof(metadata));

	/* Canonical transaction head is in segment 258 and targets another
	 * relation; this page references its predecessor in segment 257. */
	store_record(undo.data, 16385, (UBA){ { 0, 0 } });
	block = (UndoBlockHeader *)undo.data;
	header = (UndoRecordHeader *)(undo.data + sizeof(*block));
	directory = (UndoSlotDirEntry *)(undo.data + UNDO_SLOT_DIR_OFFSET(0));
	header->flags |= UNDO_REC_FLAG_HAS_ITL_HISTORY;
	header->payload_length = sizeof(insert) + sizeof(trailer);
	header->write_scn = 101;
	trailer.magic = UNDO_ITL_HISTORY_MAGIC;
	trailer.version = UNDO_ITL_HISTORY_VERSION;
	trailer.count = 1;
	trailer.entries[0].block = 0;
	trailer.entries[0].slot_index = 0;
	trailer.entries[0].after_kind = ITL_FLAG_ACTIVE;
	trailer.entries[0].after_write_scn = 100;
	if (fault == 6) {
		trailer.entries[0].prior = itl[0];
		trailer.entries[0].prior.wrap = 0;
		trailer.entries[0].prior.xid--;
		trailer.entries[0].prior.write_scn = 50;
		trailer.entries[0].prior.commit_scn = 60;
		trailer.entries[0].prior.undo_segment_head = uba_encode(257, 2, 6, 0);
	}
	if (fault == 4) {
		header->flags &= ~UNDO_REC_FLAG_HAS_ITL_HISTORY;
		header->payload_length = sizeof(insert);
	}
	block->free_offset = sizeof(*block) + sizeof(*header) + header->payload_length;
	directory->record_length = sizeof(*header) + header->payload_length;
	directory->flags = header->flags;
	memcpy(undo.data + sizeof(*block) + sizeof(*header), &insert, sizeof(insert));
	memcpy(undo.data + sizeof(*block) + sizeof(*header) + sizeof(insert), &trailer,
		   sizeof(trailer));
	fd = openat(root, "pg_undo/instance_1/seg_257.dat", O_WRONLY);
	if (fd < 0 || pwrite(fd, undo.data, BLCKSZ, BLCKSZ) != BLCKSZ || close(fd) != 0)
		abort();
	return root;
}

static ClusterRecoveryReferenceGraph *
census_graph(ClusterRecoveryRootSet *roots, ClusterRecoveryUndoStore **store)
{
	ClusterRecoveryGraphResult result;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryGraphRecord record;
	unsigned slots = 0, records = 0;
	*store = open_store(roots);
	if (*store == NULL)
		return NULL;
	UT_ASSERT_EQ(cluster_recovery_undo_store_graph_begin(*store, 817, 91, 1024 * 1024), 0);
	UT_ASSERT_EQ(inventory(*store, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(slots, 96);
	graph = cluster_recovery_undo_store_graph(*store);
	if (graph == NULL)
		return NULL;
	for (unsigned i = 0; i < 100; i++) {
		result = cluster_recovery_reference_graph_step(graph, &record);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD)
			records++;
		else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 2);
	return graph;
}

static ClusterRecoveryRelationScan *
census_relation(ClusterRecoveryRootSet *roots, ClusterRecoveryRelationPage *page)
{
	ClusterRecoveryRelationScan *scan = NULL;
	uint32 index = UINT32_MAX;
	ClusterRecoveryRelationResult result = CLUSTER_RECOVERY_RELATION_INVALID;
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELATION
			&& entry->file.locator.relNumber == 16385 && entry->file.forknum == MAIN_FORKNUM)
			index = i;
	}
	UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, index, 64 * 1024, &scan),
				 CLUSTER_RECOVERY_RELATION_MORE);
	for (unsigned i = 0; scan != NULL && i < 10; i++) {
		result = cluster_recovery_relation_next(scan, page);
		if (result != CLUSTER_RECOVERY_RELATION_MORE)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELATION_PAGE);
	return scan;
}

UT_TEST(test_physical_relation_page_joins_the_original_tt_forest_and_history)
{
	int fd = census_files(0);
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
	ClusterRecoveryRelationPage page;
	ClusterRecoveryRelationScan *scan = census_relation(roots, &page);
	ClusterRecoveryPageCensus *census = NULL;
	ClusterRecoveryPageReference ref;
	ClusterRecoveryPageCensusResult result = CLUSTER_RECOVERY_PAGE_INVALID;
	unsigned itls = 0, tuples = 0, matched = 0;
	uint64 version = 0;
	PGAlignedBlock before;
	memcpy(before.data, page.page, BLCKSZ);
	UT_ASSERT_EQ(cluster_recovery_page_census_begin(&page, graph, 128 * 1024, &census),
				 CLUSTER_RECOVERY_PAGE_MORE);
	for (unsigned i = 0; census != NULL && i < 200; i++) {
		result = cluster_recovery_page_census_next(census, &ref);
		if (result == CLUSTER_RECOVERY_PAGE_ENTRY) {
			if (ref.entry.kind == CLUSTER_RECOVERY_HEAP_ITL)
				itls++;
			if (ref.entry.tuple_bytes != NULL) {
				HeapTupleHeader tuple = (HeapTupleHeader)ref.entry.tuple_bytes;
				tuples++;
				UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), 4195140);
				UT_ASSERT(tuple->t_infomask & HEAP_XMAX_IS_MULTI);
				UT_ASSERT(tuple->t_infomask2 & HEAP_HOT_UPDATED);
				UT_ASSERT(ref.transaction.record == NULL); /* native obligation not resolved */
			}
			if (ref.transaction.record != NULL) {
				matched++;
				UT_ASSERT_EQ(ref.transaction.transaction.segment_id, 257);
				UT_ASSERT_EQ(ref.transaction.transaction.segment_generation, 0);
				UT_ASSERT_EQ(ref.transaction.transaction.slot_wrap, 1);
				UT_ASSERT_EQ(ref.transaction.record->header.target_locator.relNumber, 16385);
			}
			version = ref.version;
		} else if (result != CLUSTER_RECOVERY_PAGE_MORE)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_PAGE_DRAINED);
	UT_ASSERT_EQ(itls, 16);
	UT_ASSERT_EQ(tuples, 1);
	UT_ASSERT_EQ(matched, 1);
	UT_ASSERT_EQ(version, 1);
	UT_ASSERT_EQ(memcmp(page.page, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &page), CLUSTER_RECOVERY_RELATION_MORE);
	UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &page), CLUSTER_RECOVERY_RELATION_MORE);
	UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &page), CLUSTER_RECOVERY_RELATION_DRAINED);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_DRAINED);
	cluster_recovery_page_census_destroy(census);
	cluster_recovery_relation_destroy(scan);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

static void
census_missing(unsigned fault)
{
	int fd = census_files(fault);
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
	ClusterRecoveryRelationPage page;
	ClusterRecoveryRelationScan *scan = census_relation(roots, &page);
	ClusterRecoveryPageCensus *census = NULL;
	ClusterRecoveryPageReference ref;
	ClusterRecoveryPageCensusResult result = CLUSTER_RECOVERY_PAGE_INVALID;
	UT_ASSERT_EQ(cluster_recovery_page_census_begin(&page, graph, 128 * 1024, &census), 0);
	for (unsigned i = 0; census != NULL && i < 200; i++) {
		result = cluster_recovery_page_census_next(census, &ref);
		if (result > CLUSTER_RECOVERY_PAGE_ENTRY)
			break;
		if (fault == 3 && result == CLUSTER_RECOVERY_PAGE_ENTRY)
			UT_ASSERT_EQ(ref.version, 0);
	}
	if (result != CLUSTER_RECOVERY_PAGE_BASE_REQUIRED)
		printf("# page census fault %u returned %u\n", fault, result);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_PAGE_BASE_REQUIRED);
	UT_ASSERT(ref.transaction.record == NULL);
	UT_ASSERT(ref.entry.tuple_bytes == NULL);
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
	if (census != NULL)
		UT_ASSERT_EQ(cluster_recovery_page_census_next(census, &ref), result);
	cluster_recovery_page_census_destroy(census);
	cluster_recovery_relation_destroy(scan);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_page_reference_cannot_borrow_a_new_slot_generation)
{
	census_missing(1);
	census_missing(2);
}
UT_TEST(test_each_itl_is_checked_before_page_inverse)
{
	census_missing(3);
}
UT_TEST(test_history_without_a_record_or_prior_base_never_drains)
{
	census_missing(4);
	census_missing(5);
	census_missing(6);
}

UT_TEST(test_page_census_rejects_partial_changed_or_invalid_inputs)
{
	for (unsigned mode = 0; mode < 7; mode++) {
		int fd = census_files(0);
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = NULL;
		ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
		ClusterRecoveryRelationPage page;
		ClusterRecoveryRelationScan *scan = census_relation(roots, &page);
		ClusterRecoveryPageCensus *census = NULL;
		ClusterRecoveryPageReference ref;
		ClusterRecoveryPageCensusResult result;
		if (mode == 0)
			page.forknum = VISIBILITYMAP_FORKNUM;
		if (mode == 1)
			page.identity_token = 0;
		if (mode == 2)
			page.reservation_token = 0;
		if (mode == 3)
			page.block = page.reservation.next_block;
		if (mode == 4)
			cluster_recovery_reference_graph_invalidate(graph);
		result
			= cluster_recovery_page_census_begin(&page, graph, mode == 5 ? 1 : 128 * 1024, &census);
		UT_ASSERT_EQ(result, mode == 6	 ? CLUSTER_RECOVERY_PAGE_MORE
							 : mode == 5 ? CLUSTER_RECOVERY_PAGE_CAPACITY
										 : CLUSTER_RECOVERY_PAGE_INVALID);
		if (mode == 6 && census != NULL) {
			UT_ASSERT_EQ(cluster_recovery_page_census_next(census, &ref),
						 CLUSTER_RECOVERY_PAGE_MORE);
			inject_pread_fault = 2;
			UT_ASSERT_EQ(cluster_recovery_undo_store_recheck(store), CLUSTER_RECOVERY_GRAPH_IO);
			inject_pread_fault = 0;
			UT_ASSERT_EQ(cluster_recovery_page_census_next(census, &ref),
						 CLUSTER_RECOVERY_PAGE_CHANGED);
			UT_ASSERT(ref.entry.tuple_bytes == NULL);
		}
		cluster_recovery_page_census_destroy(census);
		cluster_recovery_relation_destroy(scan);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}


UT_TEST(test_final_whole_namespace_change_revokes_completed_page_census)
{
	int fd = census_files(0);
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
	ClusterRecoveryRelationPage page;
	ClusterRecoveryRelationScan *scan = census_relation(roots, &page);
	ClusterRecoveryPageCensus *census = NULL;
	ClusterRecoveryPageReference ref;
	ClusterRecoveryPageCensusResult result = CLUSTER_RECOVERY_PAGE_INVALID;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult checked = CLUSTER_RECOVERY_ROOTS_MORE;
	PGAlignedBlock original;
	int changed;

	memcpy(original.data, page.page, BLCKSZ);
	UT_ASSERT_EQ(cluster_recovery_page_census_begin(&page, graph, 128 * 1024, &census), 0);
	for (unsigned i = 0; census != NULL && i < 200; i++) {
		result = cluster_recovery_page_census_next(census, &ref);
		if (result > CLUSTER_RECOVERY_PAGE_ENTRY)
			break;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_PAGE_DRAINED);
	changed = openat(fd, "base/5/16385", O_WRONLY);
	UT_ASSERT(changed >= 0);
	if (changed >= 0) {
		char byte = 'x';
		UT_ASSERT_EQ(pwrite(changed, &byte, 1, 2000), 1); /* unchanged inode/length */
		close(changed);
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	for (unsigned i = 0; i < 1000; i++) {
		checked = cluster_recovery_root_set_step(roots, &event);
		if (checked >= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			break;
	}
	UT_ASSERT_EQ(checked, CLUSTER_RECOVERY_ROOTS_CHANGED);
	UT_ASSERT(cluster_recovery_undo_store_graph(store) == NULL);
	if (census != NULL) {
		UT_ASSERT_EQ(cluster_recovery_page_census_next(census, &ref),
					 CLUSTER_RECOVERY_PAGE_CHANGED);
		UT_ASSERT(ref.entry.tuple_bytes == NULL && ref.transaction.record == NULL);
		census_file(fd, "base/5/16385", original.data, BLCKSZ);
		UT_ASSERT_EQ(cluster_recovery_page_census_next(census, &ref),
					 CLUSTER_RECOVERY_PAGE_CHANGED);
	}
	cluster_recovery_page_census_destroy(census);
	cluster_recovery_relation_destroy(scan);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
