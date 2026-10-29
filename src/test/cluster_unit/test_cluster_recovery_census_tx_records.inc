/* Entire original census/forest and native undo records/files execute.
 * Source/lock/final-boundary faults are controlled; no completion is granted.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_tx_fault, census_tx_calls;
static ClusterRecoveryCensus *census_tx_owner;
static bool census_tx_extra_files;

static ClusterRecoveryGraphResult
census_tx_test_at(const ClusterRecoveryReferenceGraph *graph, uint32 index,
	ClusterRecoveryGraphRecord *out)
{
	ClusterRecoveryGraphResult result = cluster_recovery_reference_graph_at(graph, index, out);
	census_tx_calls++;
	if (census_tx_fault == 1) inputs_pin_current = false;
	if (census_tx_fault == 2) pg_re_throw();
	if (census_tx_fault == 3) cluster_recovery_reference_graph_invalidate(census_tx_owner->graph);
	if (census_tx_fault == 4) census_tx_owner->manifest.tx_record_count++;
	return result;
}

static ClusterRecoveryCensus *
census_tx_setup(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH], bool empty)
{
	ClusterRecoveryCensus *cursor;
	census_tx_fault = census_tx_calls = 0; census_tx_extra_files = false;
	owned_page_roots_fixture = empty;
	cursor = owned_census_fixture(inputs, fd, path, false);
	owned_page_roots_fixture = false;
	census_tx_owner = cursor;
	return cursor;
}

static void
census_tx_done(ClusterRecoveryCensus *cursor, ClusterWalInputsV1 **inputs, int fd, const char *path)
{
	census_tx_fault = 0; inputs_pin_current = true;
	if (census_tx_extra_files) {
		UT_ASSERT_EQ(unlinkat(fd, "pg_undo/instance_0/seg_2.dat", 0), 0);
		UT_ASSERT_EQ(unlinkat(fd, "base/5/20001", 0), 0);
		UT_ASSERT_EQ(unlinkat(fd, "base/5/20001_space", 0), 0);
		census_tx_extra_files = false;
	}
	owned_census_done(cursor, inputs, fd, path); census_tx_owner = NULL;
}

/* The WAL head names segment1/row0. A rollover predecessor lives in a
 * different physical segment/generation and relation, outside the supplied
 * root list. Same-segment forward edges would correctly fail the old gate. */
static void
census_tx_add_predecessor(int fd, const ClusterWalSourceRef *source)
{
	PGAlignedBlock page;
	UndoRecordHeader head;
	UndoSegmentHeaderData previous_segment;
	UndoBlockHeader block;
	UndoSlotDirEntry directory;
	int file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_RDWR);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	memcpy(&block, page.data, sizeof(block));
	memcpy(&head, page.data + sizeof(block), sizeof(head));
	memcpy(&directory, page.data + UNDO_SLOT_DIR_OFFSET(0), sizeof(directory));
	head.flags = 0; head.prev_uba = uba_encode(2, 1, 0, 0);
	directory.flags = head.flags;
	memcpy(page.data + sizeof(block), &head, sizeof(head));
	memcpy(page.data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	/* The predecessor retains the original transaction's segment1 binding. */
	head.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	memset(&head.prev_uba, 0, sizeof(head.prev_uba));
	head.target_locator.relNumber = 20001; directory.flags = head.flags;
	memcpy(page.data + sizeof(block), &head, sizeof(head));
	memcpy(page.data + UNDO_SLOT_DIR_OFFSET(0), &directory, sizeof(directory));
	cluster_undo_segment_make_header_bytes(2, 1, (char *)&previous_segment);
	previous_segment.wrap_count = 17;
	file = openat(fd, "pg_undo/instance_0/seg_2.dat", O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, &previous_segment, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(write(file, page.data, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	owned_census_relation(fd, source, 20001, false);
	census_tx_extra_files = true;
}

UT_TEST(test_census_tx_records_include_real_cross_relation_predecessor)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryCensusManifest manifest;
	ClusterRecoveryCensusTxRecordV1 got;
	unsigned seen = 0;
	census_tx_add_predecessor(fd, &cluster_wal_inputs_at_v1(inputs, 0)->source);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	if (cursor->status != CLUSTER_RECOVERY_CENSUS_DRAINED)
		printf("# census TX fixture phase=%u graph=%u target_missing=%llu space_missing=%llu\n",
			(unsigned)cursor->diag.phase, (unsigned)cursor->diag.graph_result,
			(unsigned long long)cursor->diag.tx_target_missing,
			(unsigned long long)cursor->diag.tx_space_missing);
	UT_ASSERT(cluster_recovery_census_manifest(cursor, &manifest));
	UT_ASSERT_EQ(manifest.tx_record_count, 3);
	for (uint32 i = 0; i < 3; i++) {
		got = cluster_recovery_census_tx_record_v1(cursor, i);
		UT_ASSERT(got.observed && got.value.record != NULL); UT_ASSERT_EQ(got.count, 3);
		if (!got.observed || got.value.record == NULL) continue;
		UT_ASSERT_EQ(got.value.transaction.xid, 4195136);
		UT_ASSERT_EQ(got.value.transaction.slot_wrap, 1);
		UT_ASSERT_EQ(got.value.record->header.record_type, UNDO_RECORD_INSERT);
		UT_ASSERT_EQ(got.value.record->body_length, sizeof(UndoInsertPayload));
		UT_ASSERT_EQ(((const UndoInsertPayload *)got.value.record->payload)->inserted_tuple_len, 32);
		if (got.value.record->header.target_locator.relNumber == 20001) {
			UT_ASSERT_EQ(got.value.transaction.origin_node_id, 0);
			UT_ASSERT_EQ(got.value.transaction.segment_id, 1);
			UT_ASSERT_EQ(got.value.record_segment_generation, 17);
			UT_ASSERT(cluster_undo_record_uba_equal(got.value.record->address, uba_encode(2,1,0,0)));
			seen |= 4;
		} else {
			UT_ASSERT_EQ(got.value.record->header.target_locator.relNumber, 16385);
			seen |= got.value.transaction.origin_node_id == 0 ? 1 : 2;
		}
	}
	UT_ASSERT_EQ(seen, 7);
	got = cluster_recovery_census_tx_record_v1(cursor, 3);
	UT_ASSERT(got.observed && got.value.record == NULL); UT_ASSERT_EQ(got.count, 3);
	UT_ASSERT(memcmp(&manifest, &cursor->manifest, sizeof(manifest)) == 0);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_tx_done(cursor, &inputs, fd, path);
	/* A real original census with no TT/WAL/PAGE roots is complete empty. */
	cursor = census_tx_setup(&inputs, &fd, path, true);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	got = cluster_recovery_census_tx_record_v1(cursor, 0);
	UT_ASSERT(got.observed && got.count == 0 && got.value.record == NULL);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_tx_records_preconditions_and_range_do_not_advance)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryCensusTxRecordV1 got, zero = {0};
	got = cluster_recovery_census_tx_record_v1(cursor, 0);
	UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
	UT_ASSERT_EQ(cursor->diag.phase, CLUSTER_RECOVERY_CENSUS_WAL);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (unsigned mode = 0; mode < 4; mode++) {
		unsigned calls = census_tx_calls;
		if (mode == 0) inventory_graph_caller_lock = true;
		if (mode == 1) CritSectionCount = 1;
		if (mode == 2) ShutdownRequestPending = true;
		got = cluster_recovery_census_tx_record_v1(cursor, mode == 3 ? 3 : 0);
		inventory_graph_caller_lock = false; CritSectionCount = 0; ShutdownRequestPending = false;
		UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		UT_ASSERT_EQ(census_tx_calls, calls);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	}
	got = cluster_recovery_census_tx_record_v1(cursor, 0); UT_ASSERT(got.observed);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_tx_records_late_source_graph_error_and_count_are_sticky)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
		ClusterRecoveryCensusTxRecordV1 got = {0}, zero = {0}; volatile bool caught = false;
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_tx_fault = fault;
		PG_TRY(); { got = cluster_recovery_census_tx_record_v1(cursor, 0); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught == (fault == 2)); UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_tx_fault = 0; inputs_pin_current = true;
		got = cluster_recovery_census_tx_record_v1(cursor, 0);
		UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		census_tx_done(cursor, &inputs, fd, path);
	}
}

UT_TEST(test_census_tx_records_original_second_read_refuses_file_rewrite)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd, file;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryCensusTxRecordV1 got, zero = {0};
	/* Mutation precedes the original full physical second read. A DONE
	 * accessor is a saved-forest view, not another physical lifetime check. */
	UT_ASSERT_EQ(owned_census_until(cursor, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
		CLUSTER_RECOVERY_CENSUS_MORE);
	file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_RDWR); UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(ftruncate(file, BLCKSZ), 0); UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
	got = cluster_recovery_census_tx_record_v1(cursor, 0);
	UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
	UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_tx_done(cursor, &inputs, fd, path);
}
