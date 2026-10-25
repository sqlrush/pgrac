/* Physical PAGE roots must enter the original census before its TX walk.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static void
owned_page_root_files(int root)
{
	UndoSegmentHeaderData header;
	PGAlignedBlock data = { 0 }, undo = { 0 };
	PageHeader page = (PageHeader)data.data;
	HeapTupleHeader tuple;
	ClusterItlSlotData *itl;
	UndoBlockHeader block = { 0 };
	UndoRecordHeader record = { 0 };
	UndoSlotDirEntry directory = { 0 };
	UndoInsertPayload insert = { SizeofHeapTupleHeader, 0 };
	UndoItlHistoryTrailer history = { 0 };
	int file = openat(root, "pg_undo/instance_0/seg_1.dat", O_RDWR);

	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, &header, BLCKSZ, 0), BLCKSZ);
	header.wrap_count = 7; /* not inferable from UBA or the local page-slot wrap */
	header.tt_slots[0].xid = 4195136;
	header.tt_slots[0].wrap = 1;
	header.tt_slots[0].status = TT_SLOT_COMMITTED;
	header.tt_slots[0].commit_scn = 110;
	header.tt_slots[0].first_undo_block = (UBA){ { 0, 0 } };
	UT_ASSERT_EQ(pwrite(file, &header, BLCKSZ, 0), BLCKSZ);
	page->pd_flags = PD_HAS_ITL;
	page->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	page->pd_special = BLCKSZ - MAXALIGN(CLUSTER_ITL_SPECIAL_SIZE);
	page->pd_upper = page->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	PageSetPageSizeAndVersion(data.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ItemIdSetNormal(&page->pd_linp[0], page->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)(data.data + page->pd_upper);
	HeapTupleHeaderSetXmin(tuple, 4195136);
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = 0;
	ItemPointerSet(&tuple->t_ctid, 0, 1);
	itl = ClusterPageGetItlSlots(data.data);
	itl[0].xid = 4195136;
	itl[0].flags = ITL_FLAG_COMMITTED;
	itl[0].wrap = 12; /* page slot wrap is deliberately not TT wrap1 */
	itl[0].write_scn = 100;
	itl[0].commit_scn = 110;
	itl[0].undo_segment_head = uba_encode(1, 1, 0, 0);
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.slot_count = 1;
	record.record_type = UNDO_RECORD_INSERT;
	record.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_HAS_ITL_HISTORY;
	record.payload_length = sizeof(insert) + sizeof(history);
	record.xid = 4195136;
	record.tt_slot_segment_id = 1;
	record.tt_slot_id = 1;
	record.tt_wrap_plus1 = 2;
	record.write_scn = 101;
	record.target_locator = (RelFileLocator){ 1663, 5, 16385 };
	record.target_fork = MAIN_FORKNUM;
	record.target_offset = 1;
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].slot_index = 0;
	history.entries[0].prior.wrap = 12;
	history.entries[0].after_kind = ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = 100;
	directory.record_offset = sizeof(block);
	directory.record_length = sizeof(record) + record.payload_length;
	directory.record_type = record.record_type;
	directory.flags = record.flags;
	block.free_offset = sizeof(block) + directory.record_length;
	memcpy(undo.data, &block, sizeof(block));
	memcpy(undo.data + sizeof(block), &record, sizeof(record));
	memcpy(undo.data + sizeof(block) + sizeof(record), &insert, sizeof(insert));
	memcpy(undo.data + sizeof(block) + sizeof(record) + sizeof(insert), &history, sizeof(history));
	memcpy(undo.data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	UT_ASSERT_EQ(pwrite(file, undo.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	file = openat(root, "base/5/16385", O_WRONLY);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, data.data, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
}

UT_TEST(test_owned_census_page_roots_without_tt_or_wal_heads)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusEntry entry;
	ClusterRecoveryCensusDiag diag = { 0 };
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	unsigned tx = 0;
	char path[MAXPGPATH];
	int fd;

	owned_page_roots_fixture = true;
	census = owned_census_fixture(&inputs, &fd, path, false);
	owned_page_roots_fixture = false;
	owned_page_root_files(fd);
	for (unsigned steps = 0; steps < 200000; steps++) {
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
			break;
		if (result == CLUSTER_RECOVERY_CENSUS_ENTRY
			&& entry.kind == CLUSTER_RECOVERY_CENSUS_TX_RECORD) {
			tx++;
			UT_ASSERT_EQ(entry.value.tx.transaction.segment_id, 1);
			UT_ASSERT_EQ(entry.value.tx.transaction.segment_generation, 7);
			UT_ASSERT_EQ(entry.value.tx.transaction.slot_wrap, 1);
		}
	}
	UT_ASSERT_EQ(cluster_recovery_census_diag(census, &diag), true);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_EQ(tx, 1);
	UT_ASSERT_EQ(diag.inventory.tt_roots_added, 0);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_page_root_deferred_base_preserves_independent_wal_forest)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd, file;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	ClusterRecoveryCensusDiag diag;
	UndoSegmentHeaderData header;

	owned_page_root_files(fd);
	file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_RDWR);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, &header, BLCKSZ, 0), BLCKSZ);
	header.wrap_count = 0; /* exact original retained WAL generation */
	header.tt_slots[0].xid = 4195144;
	header.tt_slots[0].wrap = 2; /* a newer binding cannot identify the old PAGE */
	UT_ASSERT_EQ(pwrite(file, &header, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.tx_records, 2);
	UT_ASSERT_EQ(diag.page_roots.deferred_references, 2);
	UT_ASSERT_EQ(diag.page_roots.deferred_histories, 1);
	UT_ASSERT_EQ(diag.page_roots.references, 0);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_page_root_deferred_base_never_replaces_later_coverage)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusDiag diag;
	UndoSegmentHeaderData header;
	char path[MAXPGPATH];
	int fd, file;

	owned_page_roots_fixture = true;
	census = owned_census_fixture(&inputs, &fd, path, false);
	owned_page_roots_fixture = false;
	owned_page_root_files(fd);
	file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_RDWR);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, &header, BLCKSZ, 0), BLCKSZ);
	header.tt_slots[0].wrap = 2;
	UT_ASSERT_EQ(pwrite(file, &header, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(cluster_recovery_census_diag(census, &diag));
	UT_ASSERT_EQ(diag.page_roots.deferred_references, 2);
	UT_ASSERT_EQ(diag.page_roots.deferred_histories, 1);
	UT_ASSERT_EQ(diag.relation_result, CLUSTER_RECOVERY_RELATIONS_BASE_REQUIRED);
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
	owned_census_done(census, &inputs, fd, path);
}
