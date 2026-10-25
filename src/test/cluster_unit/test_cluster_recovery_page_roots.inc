/* Real SPACE/PAGE/TT/undo bytes through pre-TX root discovery.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static void
roots_empty_heads(int fd)
{
	for (unsigned segment = 257; segment <= 258; segment++) {
		UndoSegmentHeaderData header;
		load_store_header(fd, segment, &header);
		for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
			header.tt_slots[i].first_undo_block = (UBA){ { 0, 0 } };
		replace_store_header(fd, segment, &header);
	}
}

static ClusterRecoveryReferenceGraph *
roots_inventory(ClusterRecoveryRootSet *roots, ClusterRecoveryUndoStore **store)
{
	unsigned slots = 0;
	*store = open_store(roots);
	UT_ASSERT_EQ(cluster_recovery_undo_store_graph_begin(*store, 817, 91, 1024 * 1024), 0);
	UT_ASSERT_EQ(inventory(*store, &slots), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(slots, 96);
	return cluster_recovery_undo_store_graph(*store);
}

UT_TEST(test_page_root_binding_keeps_canonical_and_physical_generations_distinct)
{
	int fd = store_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store;
	ClusterRecoveryTxnKey key;
	ClusterRecoveryUndoRecord record;
	uint32 generation = UINT32_MAX;
	ClusterRecoveryReferenceGraph *graph = roots_inventory(roots, &store);

	UT_ASSERT_EQ(cluster_recovery_undo_store_read_base(store, uba_encode(258, 1, 6, 0),
					4195136, &key, &record, &generation), CLUSTER_RECOVERY_GRAPH_RECORD);
	UT_ASSERT_EQ(key.segment_id, 257);
	UT_ASSERT_EQ(key.segment_generation, 0);
	UT_ASSERT_EQ(key.slot_wrap, 1);
	UT_ASSERT_EQ(generation, 7);
	UT_ASSERT_EQ(record.header.target_locator.relNumber, 16388);
	UT_ASSERT(cluster_recovery_reference_graph_accepts_roots(graph));
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_page_root_reused_or_unknown_binding_remains_unresolved)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		int fd = store_files(), file;
		UndoSegmentHeaderData header;
		UndoRecordHeader raw;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryUndoStore *store;
		ClusterRecoveryReferenceGraph *graph;
		ClusterRecoveryTxnKey key;
		ClusterRecoveryUndoRecord record;
		uint32 generation = 77;

		load_store_header(fd, 257, &header);
		if (mode == 0) header.tt_slots[6].xid++;
		if (mode == 1) header.tt_slots[6].wrap++;
		if (mode == 2) header.tt_slots[6].status = TT_SLOT_UNUSED;
		if (mode == 3) header.tt_slots[6].status = TT_SLOT_RECYCLABLE;
		replace_store_header(fd, 257, &header);
		file = openat(fd, "pg_undo/instance_1/seg_257.dat", O_RDWR);
		UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(pread(file, &raw, sizeof(raw), BLCKSZ + sizeof(UndoBlockHeader)), sizeof(raw));
		if (mode == 4) raw.tt_wrap_plus1 = 0;
		if (mode == 5) raw.tt_slot_segment_id = 259;
		UT_ASSERT_EQ(pwrite(file, &raw, sizeof(raw), BLCKSZ + sizeof(UndoBlockHeader)), sizeof(raw));
		UT_ASSERT_EQ(close(file), 0);
		roots = store_roots(fd);
		graph = roots_inventory(roots, &store);
		memset(&key, 0x88, sizeof(key));
		memset(&record, 0x88, sizeof(record));
		UT_ASSERT_EQ(cluster_recovery_undo_store_read_base(store, uba_encode(257, 1, 6, 0),
					4195136, &key, &record, &generation), CLUSTER_RECOVERY_GRAPH_PENDING);
		UT_ASSERT_EQ(key.system_identifier, 0);
		UT_ASSERT_EQ(key.segment_generation, 0);
		UT_ASSERT_EQ(record.header.xid, 0);
		UT_ASSERT(UBA_is_invalid(record.address));
		UT_ASSERT_EQ(generation, 0);
		UT_ASSERT(cluster_recovery_reference_graph_accepts_roots(graph));
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

static void
roots_record_file(int root, unsigned segment, const UndoRecordHeader *record,
				  const void *body, size_t length, const UndoItlHistoryTrailer *history)
{
	PGAlignedBlock bytes = { 0 };
	UndoBlockHeader block = { 0 };
	UndoSlotDirEntry directory = { 0 };
	char path[96];
	int file;

	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.slot_count = 1;
	directory.record_offset = sizeof(block);
	directory.record_length = sizeof(*record) + length + sizeof(*history);
	directory.record_type = record->record_type;
	directory.flags = record->flags;
	block.free_offset = sizeof(block) + directory.record_length;
	memcpy(bytes.data, &block, sizeof(block));
	memcpy(bytes.data + sizeof(block), record, sizeof(*record));
	memcpy(bytes.data + sizeof(block) + sizeof(*record), body, length);
	memcpy(bytes.data + sizeof(block) + sizeof(*record) + length, history, sizeof(*history));
	memcpy(bytes.data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	snprintf(path, sizeof(path), "pg_undo/instance_1/seg_%u.dat", segment);
	file = openat(root, path, O_WRONLY);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pwrite(file, bytes.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

static int
roots_history_files(void)
{
	int fd = census_files(0), file;
	UndoSegmentHeaderData header;
	PGAlignedBlock page;
	UndoRecordHeader record = { 0 };
	UndoItlHistoryTrailer history = { 0 };
	UndoItlPayload lock = { 0 };
	UndoInsertPayload insert = { SizeofHeapTupleHeader, 0 };
	ClusterItlSlotData prior = { 0 };
	HeapTupleHeader tuple;

	roots_empty_heads(fd);
	load_store_header(fd, 258, &header);
	header.tt_slots[6].xid = 4195135;
	header.tt_slots[6].wrap = 5;
	header.tt_slots[6].status = TT_SLOT_COMMITTED;
	header.tt_slots[6].commit_scn = 60;
	replace_store_header(fd, 258, &header);
	prior.xid = 4195135;
	prior.flags = ITL_FLAG_COMMITTED;
	prior.wrap = 11;
	prior.write_scn = 50;
	prior.commit_scn = 60;
	prior.undo_segment_head = uba_encode(258, 1, 6, 0);
	file = openat(fd, "base/5/16385", O_RDWR);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(read(file, page.data, BLCKSZ), BLCKSZ);
	ClusterPageGetItlSlots(page.data)[0].wrap = 12;
	ClusterPageGetItlSlots(page.data)[0].flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	tuple = (HeapTupleHeader)PageGetItem(page.data, PageGetItemId(page.data, 1));
	HeapTupleHeaderSetXmin(tuple, 4195135);
	HeapTupleHeaderSetXmax(tuple, 4195136);
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	tuple->t_infomask2 = 0;
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	record.record_type = UNDO_RECORD_ITL;
	record.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY;
	record.payload_length = sizeof(lock) + sizeof(history);
	record.xid = 4195136;
	record.origin_node_id = 1;
	record.tt_slot_segment_id = 257;
	record.tt_slot_id = 7;
	record.tt_wrap_plus1 = 2;
	record.write_scn = 101;
	record.target_locator = (RelFileLocator){ 1663, 5, 16385 };
	record.target_fork = MAIN_FORKNUM;
	record.target_offset = 1;
	lock.itl_slot_idx = 0;
	lock.prev_flags = ITL_FLAG_COMMITTED;
	lock.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	lock.lock_xid = 4195136;
	lock.prev_commit_scn = 60;
	lock.prev_undo_segment_head = prior.undo_segment_head;
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
	history.entries[0].after_write_scn = 100;
	history.entries[0].prior = prior;
	roots_record_file(fd, 257, &record, &lock, sizeof(lock), &history);
	record.record_type = UNDO_RECORD_INSERT;
	record.payload_length = sizeof(insert) + sizeof(history);
	record.xid = 4195135;
	record.tt_slot_segment_id = 258;
	record.tt_wrap_plus1 = 6;
	record.write_scn = 51;
	history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = 50;
	memset(&history.entries[0].prior, 0, sizeof(prior));
	history.entries[0].prior.wrap = 11;
	roots_record_file(fd, 258, &record, &insert, sizeof(insert), &history);
	return fd;
}

UT_TEST(test_page_roots_find_inverse_only_transaction_before_graph_walk)
{
	int fd = roots_history_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store;
	ClusterRecoveryReferenceGraph *graph = roots_inventory(roots, &store);
	ClusterRecoveryPageRoots *cursor = NULL;
	ClusterRecoveryPageRootsDiag diag = { 0 };
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	ClusterRecoveryPageCensus *page_census = NULL;
	ClusterRecoveryPageReference ref;
	ClusterRecoveryRelationPage page;
	ClusterRecoveryRelationScan *relation;
	ClusterRecoveryPageCensusResult checked = CLUSTER_RECOVERY_PAGE_MORE;
	unsigned records = 0;

	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 0);
	UT_ASSERT_EQ(cluster_recovery_page_roots_begin(store, 256 * 1024, &cursor), 0);
	for (unsigned i = 0; i < 1000 && result == CLUSTER_RECOVERY_GRAPH_MORE; i++)
		result = cluster_recovery_page_roots_step(cursor, &diag);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(diag.pages, 1);
	UT_ASSERT_EQ(diag.versions, 3);
	UT_ASSERT_EQ(diag.references, 4);
	UT_ASSERT_EQ(diag.deferred_references, 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 2);
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
	cluster_recovery_page_roots_destroy(cursor);
	UT_ASSERT_EQ(drain_store_graph(graph, &records), CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_EQ(records, 2);
	relation = census_relation(roots, &page);
	UT_ASSERT_EQ(cluster_recovery_page_census_begin(&page, graph, 128 * 1024, &page_census), 0);
	for (unsigned i = 0; i < 1000 && checked <= CLUSTER_RECOVERY_PAGE_ENTRY; i++)
		checked = cluster_recovery_page_census_next(page_census, &ref);
	UT_ASSERT_EQ(checked, CLUSTER_RECOVERY_PAGE_DRAINED);
	cluster_recovery_page_census_destroy(page_census);
	cluster_recovery_relation_destroy(relation);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_page_roots_read_failures_revoke_the_original_owners)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		int fd = census_files(0), file;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryUndoStore *store;
		ClusterRecoveryReferenceGraph *graph;
		ClusterRecoveryPageRoots *cursor = NULL;
		ClusterRecoveryPageRootsDiag diag;
		ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
		uint8 digest[32];

		if (mode < 2) {
			file = openat(fd, "pg_undo/instance_1/seg_257.dat", O_WRONLY);
			UT_ASSERT(file >= 0);
			if (mode == 0) UT_ASSERT_EQ(ftruncate(file, BLCKSZ), 0);
			else {
				PGAlignedBlock zero = { 0 };
				UT_ASSERT_EQ(pwrite(file, zero.data, BLCKSZ, BLCKSZ), BLCKSZ);
			}
			UT_ASSERT_EQ(close(file), 0);
		}
		roots = store_roots(fd);
		graph = roots_inventory(roots, &store);
		UT_ASSERT_EQ(cluster_recovery_page_roots_begin(store, 256 * 1024, &cursor), 0);
		if (mode == 2) inject_pread_fault = 2;
		if (mode == 3) UT_ASSERT_EQ(unlinkat(fd, "base/5/16385", 0), 0);
		for (unsigned i = 0; i < 1000 && result == CLUSTER_RECOVERY_GRAPH_MORE; i++)
			result = cluster_recovery_page_roots_step(cursor, &diag);
		inject_pread_fault = 0;
		UT_ASSERT(result > CLUSTER_RECOVERY_GRAPH_DRAINED);
		UT_ASSERT_EQ(cluster_recovery_page_roots_step(cursor, &diag), result);
		UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_page_roots_destroy(cursor);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_page_roots_budget_and_lifetime_refuse_late_seeding)
{
	int fd = census_files(0);
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store;
	ClusterRecoveryReferenceGraph *graph = roots_inventory(roots, &store);
	ClusterRecoveryPageRoots *cursor = NULL;
	ClusterRecoveryGraphRecord record;
	ClusterRecoveryPageRootsDiag diag;
	uint8 digest[32];

	UT_ASSERT_EQ(cluster_recovery_page_roots_begin(store, 1, &cursor), CLUSTER_RECOVERY_GRAPH_CAPACITY);
	UT_ASSERT(cursor == NULL);
	UT_ASSERT(cluster_recovery_reference_graph_accepts_roots(graph));
	UT_ASSERT_EQ(cluster_recovery_page_roots_begin(store, 256 * 1024, &cursor), 0);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &record), CLUSTER_RECOVERY_GRAPH_RECORD);
	UT_ASSERT_EQ(cluster_recovery_page_roots_step(cursor, &diag), CLUSTER_RECOVERY_GRAPH_CHANGED);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
	cluster_recovery_page_roots_destroy(cursor);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
