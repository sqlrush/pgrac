/* Original census, namespace and native undo files execute; the source
 * authority fixture remains controlled. No completion authority is minted.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_tx_recheck_fault;
static ClusterRecoveryGraphResult
census_tx_test_recheck_at(ClusterRecoveryReferenceGraph *graph, uint32 index)
{
	ClusterRecoveryGraphResult result = cluster_recovery_reference_graph_recheck_at(graph, index);
	if (census_tx_recheck_fault == 1) inputs_pin_current = false;
	if (census_tx_recheck_fault == 2) pg_re_throw();
	return result;
}

UT_TEST(test_census_tx_final_recheck_accepts_original_complete_files_and_empty_end)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, empty != 0);
		ClusterRecoveryCensusManifest saved;

		if (!empty) census_tx_add_predecessor(fd, &cluster_wal_inputs_at_v1(inputs, 0)->source);
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT(cluster_recovery_census_manifest(cursor, &saved));
		UT_ASSERT_EQ(saved.tx_record_count, empty ? 0 : 3);
		for (uint32 i = 0; i <= saved.tx_record_count; i++)
			UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor, i),
				i == saved.tx_record_count ? CLUSTER_RECOVERY_GRAPH_DRAINED : CLUSTER_RECOVERY_GRAPH_RECORD);
		UT_ASSERT(memcmp(&saved, &cursor->manifest, sizeof(saved)) == 0);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_tx_done(cursor, &inputs, fd, path);
	}
}

UT_TEST(test_census_tx_final_recheck_detects_same_inode_late_predecessor_rewrite)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd, file;
	PGAlignedBlock page;
	UndoRecordHeader header;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	uint32 index = UINT32_MAX;

	census_tx_add_predecessor(fd, &cluster_wal_inputs_at_v1(inputs, 0)->source);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (uint32 i = 0; i < 3; i++) {
		ClusterRecoveryCensusTxRecordV1 record = cluster_recovery_census_tx_record_v1(cursor, i);
		if (record.value.record->header.target_locator.relNumber == 20001) index = i;
	}
	UT_ASSERT(index < 3);
	file = openat(fd, "pg_undo/instance_0/seg_2.dat", O_RDWR); UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pread(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	memcpy(&header, page.data + sizeof(UndoBlockHeader), sizeof(header));
	header.write_scn++;
	memcpy(page.data + sizeof(UndoBlockHeader), &header, sizeof(header));
	UT_ASSERT_EQ(pwrite(file, page.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(file), 0);
	/* The earlier observed view remains immutable; final reads must not
	 * substitute another saved-view lookup for the actual namespace read. */
	UT_ASSERT(cluster_recovery_census_tx_record_v1(cursor, index).observed);
	UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor, index), CLUSTER_RECOVERY_GRAPH_CHANGED);
	UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor, 0), CLUSTER_RECOVERY_GRAPH_INVALID);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_tx_final_recheck_does_not_enter_under_locks_or_stale_source)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);

	UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor, 0), CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (unsigned fault = 0; fault < 4; fault++) {
		inventory_graph_caller_lock = fault == 0;
		CritSectionCount = fault == 1;
		ShutdownRequestPending = fault == 2;
		UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor,
			fault == 3 ? cursor->manifest.tx_record_count + 1 : 0), CLUSTER_RECOVERY_GRAPH_INVALID);
		inventory_graph_caller_lock = false; CritSectionCount = 0; ShutdownRequestPending = false;
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	}
	inputs_pin_current = false;
	UT_ASSERT(cluster_recovery_census_tx_recheck_v1(cursor, 0) >= CLUSTER_RECOVERY_GRAPH_INVALID);
	UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_tx_done(cursor, &inputs, fd, path);
	/* Controlled late source/ERROR faults after the actual original graph
	 * read must revoke the census while the input scope retains every pin. */
	for (unsigned fault = 1; fault <= 2; fault++) {
		volatile bool caught = false;
		cursor = census_tx_setup(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_tx_recheck_fault = fault;
		PG_TRY(); { (void)cluster_recovery_census_tx_recheck_v1(cursor, 0); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		census_tx_recheck_fault = 0;
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(cursor->roots == NULL && cursor->graph == NULL);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		UT_ASSERT_EQ(cluster_recovery_census_tx_recheck_v1(cursor, 0), CLUSTER_RECOVERY_GRAPH_INVALID);
		census_tx_done(cursor, &inputs, fd, path);
	}
}
