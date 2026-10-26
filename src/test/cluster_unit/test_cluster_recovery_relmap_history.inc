/* Original checkpoint/window and CLOG history inputs, never completion proof.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterWalInputsV1 *inventory_test_history_inputs;

static ClusterWalInputsV1 *
relmap_history_fixture(uint32 era, unsigned window_version, bool barriers)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	char path[MAXPGPATH];

	inputs_closed_wal_fixture(bytes, anchors);
	for (unsigned i = 0; i < 2; ++i) {
		ClusterRecoveryAnchorV2 *a = &anchors[i];
		ControlFileData native = { 0 };
		ClusterRelmapCheckpointWindow window = { 0 };
		ClusterWalSourceRef source = { 0 };
		WalTestRecord next;
		char map[TEST_RELMAP_LENGTH];
		int page = 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE;
		xl_clog_truncate truncate = { .pageno = 1, .oldestXact = 32768, .oldestXactDb = 5 };

		a->checkpoint_copy.nextXid = FullTransactionIdFromU64(((uint64)era << 32) | 4196000);
		native.state = a->state;
		native.checkPoint = a->checkpoint;
		native.checkPointCopy = a->checkpoint_copy;
		snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
				 "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix/current",
				 cluster_wal_threads_dir, a->identity.origin_thread_id,
				 a->identity.origin_owner_incarnation);
		v2_checkpoint_wal_record(&a->identity, &native, 8);
		window.system_identifier = a->identity.system_identifier;
		window.database_incarnation = a->database_incarnation;
		window.origin = a->identity.origin_node_id;
		window.admitted_incarnation = a->identity.origin_owner_incarnation;
		window.boot_incarnation = 17;
		window.epoch = 9;
		window.native_floor = ((uint64)era << 32) | 4195000;
		window.last_issued = ((uint64)era << 32) | 4195200;
		window.allocation_next = ((uint64)era << 32) | 4195300;
		/* Native CLOG/window supports origins0..63. Keep origin127 as a
		 * complete original source with NO invented native qualification. */
		if (window_version == 1 && i == 0)
			UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&window, &a->checkpoint_copy,
																 a->checkpoint, test_checkpoint_end,
																 a->native_window));
		else if (window_version == 2 && i == 0)
			UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(
				&window, &a->checkpoint_copy, a->checkpoint, test_checkpoint_end,
				((uint64)era << 32) | 4195250, a->native_window));
		v2_anchor_object(bytes, a, &a->identity, path);
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
		root.records[i == 0 ? 0 : 127].checkpoint_record_crc32c = test_checkpoint_crc;
		source.claim.identity = a->identity;
		source.timeline = a->checkpoint_copy.ThisTimeLineID;
		next = (WalTestRecord){ 1, test_checkpoint_end, a->checkpoint, test_checkpoint_crc };
		if (barriers) {
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		}
		test_relmap_payload(map, i == 0, 2);
		next = inventory_write_record(&source, next.exclusive_end, next.record_start, RM_RELMAP_ID,
									  XLOG_RELMAP_UPDATE, map, sizeof(map));
		if (barriers) {
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		}
		root.records[i == 0 ? 0 : 127].tail_last_record_lsn = next.record_start;
		root.records[i == 0 ? 0 : 127].tail_last_record_crc32c = next.record_crc;
		root.records[i == 0 ? 0 : 127].validated_tail_lsn_exclusive = next.exclusive_end;
		UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
		memcpy(bytes, root.bytes, sizeof(bytes));
	}
	v2_write_roots(bytes);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	return inputs;
}

UT_TEST(test_relmap_history_binds_actual_checkpoint_and_full_source_window)
{
	for (unsigned era = 0; era <= 7; era += 7) {
		ClusterWalInputsV1 *inputs = relmap_history_fixture(era, 2, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapHistorySource source;
		uint32 flags;
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
		for (unsigned i = 0; i < 2; ++i) {
			ClusterRelmapV2Image pending = relmap_fold_pending(inventory, i == 0 ? 0 : 127);
			pending.pending_owner.top_full_xid = ((uint64)era << 32) | 4195136;
			UT_ASSERT(cluster_recovery_native_inventory_relmap_source(inventory, i, &source));
			UT_ASSERT_EQ(
				memcmp(&source.input, cluster_wal_inputs_at_v1(inputs, i), sizeof(source.input)),
				0);
			UT_ASSERT(source.checkpoint_observed);
			UT_ASSERT_EQ(source.window_observed, i == 0);
			UT_ASSERT(source.checkpoint_end > source.input.checkpoint_start);
			UT_ASSERT_EQ(source.post_flush_issued, i == 0 ? ((uint64)era << 32) | 4195250 : 0);
			UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
						 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			UT_ASSERT_EQ(summary.checkpoint_sources, 1);
			UT_ASSERT_EQ(summary.window_sources, i == 0);
			UT_ASSERT_EQ(summary.query_window_sources, i == 0);
			UT_ASSERT_EQ(summary.owner_window_sources, i == 0);
			/* A different FullXid era must never be inferred from a raw match. */
			pending.pending_owner.top_full_xid += UINT64_C(1) << 32;
			UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
						 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			UT_ASSERT_EQ(summary.wal_matches, 1); /* old raw-carrier observation only */
			UT_ASSERT_EQ(summary.query_window_sources | summary.owner_window_sources, 0);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_relmap_history_keeps_legacy_windows_unqualified)
{
	for (unsigned version = 0; version < 2; ++version) {
		ClusterWalInputsV1 *inputs = relmap_history_fixture(7, version, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapHistorySource source;
		ClusterRelmapV2Image pending;
		uint32 flags;
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
		pending = relmap_fold_pending(inventory, 0);
		UT_ASSERT(cluster_recovery_native_inventory_relmap_source(inventory, 0, &source));
		UT_ASSERT(source.checkpoint_observed && !source.window_observed);
		UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
					 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT_EQ(summary.checkpoint_sources, 1);
		UT_ASSERT_EQ(summary.window_sources | summary.query_window_sources
						 | summary.owner_window_sources,
					 0);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_relmap_history_orders_barriers_only_in_unique_exact_wal_source)
{
	ClusterWalInputsV1 *inputs = relmap_history_fixture(7, 2, true);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRelmapV2Image pending;
	uint32 flags;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 0);
	pending = relmap_fold_pending(inventory, 0);
	pending.pending_owner.top_full_xid = (UINT64_C(7) << 32) | 4195136;
	UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
				 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.clog_zeroes, 2);
	UT_ASSERT_EQ(summary.clog_truncates, 2);
	UT_ASSERT_EQ(summary.clog_before_wal, 2);
	UT_ASSERT_EQ(summary.clog_after_wal, 2);
	UT_ASSERT_EQ(summary.clog_unordered, 0);
	/* An LSN alone, without the exact native map, is not a lifetime anchor. */
	pending.pending_owner.wal_end += 8;
	UT_ASSERT_EQ(relmap_fold_drain(inventory, &pending, &summary, &flags),
				 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.wal_matches, 0);
	UT_ASSERT_EQ(summary.clog_before_wal | summary.clog_after_wal, 0);
	UT_ASSERT_EQ(summary.clog_unordered, 4);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

static void
relmap_history_metadata_drift(void)
{
	ClusterWalInputV1 *input
		= (ClusterWalInputV1 *)cluster_wal_inputs_at_v1(inventory_test_history_inputs, 0);
	input->native_window[64] ^= 1;
}

UT_TEST(test_relmap_history_metadata_drift_revokes_original_inventory)
{
	for (unsigned phase = 0; phase < 2; ++phase) {
		ClusterWalInputsV1 *inputs = relmap_history_fixture(7, 2, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapHistorySource source, zero = { 0 };
		inventory_test_history_inputs = inputs;
		if (phase == 0) {
			inventory_between_passes = relmap_history_metadata_drift;
			UT_ASSERT_NE(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
			UT_ASSERT(inventory == NULL);
		} else {
			UT_ASSERT_EQ(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
			relmap_history_metadata_drift();
			UT_ASSERT_NE(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag), 0);
			memset(&source, 0xa5, sizeof(source));
			UT_ASSERT(!cluster_recovery_native_inventory_relmap_source(inventory, 0, &source));
			UT_ASSERT_EQ(memcmp(&source, &zero, sizeof(source)), 0);
		}
		inventory_between_passes = NULL;
		inventory_test_history_inputs = NULL;
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_relmap_history_owner_window_and_borrowed_record_source)
{
	ClusterWalInputsV1 *inputs = relmap_history_fixture(7, 2, false);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapFoldEntry entry;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapHistorySource source;
	ClusterRecoveryRelmapFoldResult result;
	ClusterRelmapV2Image pending;
	uint32 flags;
	unsigned entries = 0;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 0);
	pending = relmap_fold_pending(inventory, 0);
	pending.pending_owner.top_full_xid = (UINT64_C(7) << 32) | 4195136;
	do {
		result = cluster_recovery_native_inventory_relmap_step(inventory, &pending, &entry,
															   &summary, &diag);
		if (result == CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY) {
			entries++;
			UT_ASSERT(entry.history != NULL);
			UT_ASSERT(cluster_recovery_native_inventory_relmap_source(inventory, entry.source_index,
																	  &source));
			UT_ASSERT_EQ(memcmp(entry.history, &source, sizeof(source)), 0);
			UT_ASSERT(inventory_source_equal(&entry.record->source, &source.input.source));
		}
	} while (result <= CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(entries > 0);
	for (unsigned changed = 0; changed < 3; ++changed) {
		ClusterRelmapV2Image wrong = pending;
		if (changed == 0)
			wrong.pending_owner.membership_epoch++;
		else if (changed == 1)
			wrong.pending_owner.admitted_incarnation++;
		else
			wrong.pending_owner.boot_incarnation++;
		UT_ASSERT_EQ(relmap_fold_drain(inventory, &wrong, &summary, &flags),
					 CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT_EQ(summary.query_window_sources, 1);
		UT_ASSERT_EQ(summary.owner_window_sources, 0);
	}
	memset(&source, 0xa5, sizeof(source));
	UT_ASSERT(!cluster_recovery_native_inventory_relmap_source(inventory, 2, &source));
	UT_ASSERT(v2_zero(&source, sizeof(source)));
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

UT_TEST(test_relmap_history_preserves_sorted_empty_and_initializer_inputs)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	unsigned empty = 0;

	(void)pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed)
		goto cleanup;
	test_worker_pin_held = false;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
															 &control, &token),
				 0);
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		goto cleanup;
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 0);
	if (inventory == NULL)
		goto cleanup;
	for (uint32 i = 0; i < diag.sources; ++i) {
		ClusterRecoveryRelmapHistorySource source;
		UT_ASSERT(cluster_recovery_native_inventory_relmap_source(inventory, i, &source));
		UT_ASSERT(inventory_source_equal(&source.input.source, &inventory->sources[i].source));
		UT_ASSERT_EQ(memcmp(&source.input, cluster_wal_inputs_at_v1(inputs, source.input_index),
							sizeof(source.input)),
					 0);
		if (inventory->sources[i].cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) {
			empty++;
			UT_ASSERT_EQ(source.input.kind, CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL);
			UT_ASSERT(!source.checkpoint_observed && !source.window_observed);
			UT_ASSERT_EQ(source.checkpoint_end | source.post_flush_issued, 0);
		}
	}
	UT_ASSERT_EQ(empty, 1);
cleanup:
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	test_reserve_mode = false;
	cluster_node_id = saved_node;
	test_self_incarnation = saved_incarnation;
}
