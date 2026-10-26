/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_pages.inc
 *     Original episode history copies preserve native PAGE snapshot custody.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_pages.inc
 * NOTES
 *     Original ROOT/claim/anchor/WAL/PAGE readers execute. Startup/GES retain
 *     their existing controlled fixture boundaries. No replay grant is made.
 *-------------------------------------------------------------------------
 */
static void
episode_pages_begin(ClusterWalInputsV1 **inputs)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ClusterWalInputsV1 *original = NULL;

	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	for (unsigned i = 0; i < 2; ++i) {
		ClusterControlRootSnapshot *snapshot = &root.records[i ? 127 : 0];
		ClusterWalSourceRef source = {0};
		WalTestRecord page;
		source.claim.identity = snapshot->identity;
		source.timeline = snapshot->tail_tli;
		page = inventory_write_page(&source, snapshot->validated_tail_lsn_exclusive,
			snapshot->tail_last_record_lsn, false);
		snapshot->tail_last_record_lsn = page.record_start;
		snapshot->tail_last_record_crc32c = page.record_crc;
		snapshot->validated_tail_lsn_exclusive = page.exclusive_end;
		tx_ir_complete(snapshot);
	}
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &original), 0);
	tx_inputs_capture(original, NULL);
	cluster_wal_inputs_release_v1(&original);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = relmap_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_sibling = NULL; relmap_failed = 0; relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	inventory_census_calls = 0; inventory_between_passes = NULL;
}

UT_TEST(test_episode_pages_borrowed_output_preserves_complete_baseline)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryRelmapHistoryEntryV1 entry;
	ClusterRecoveryNativeInventoryDiag diag;
	NativeInventoryPage *page;
	char *saved;
	size_t bytes;
	unsigned calls;

	episode_pages_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	UT_ASSERT(history != NULL);
	if (history == NULL) return;
	page = history->inventory->pages;
	UT_ASSERT(page != NULL && history->inventory->page_count == 2);
	if (page == NULL) return;
	bytes = page->bytes;
	UT_ASSERT(bytes >= sizeof(entry));
	if (bytes < sizeof(entry)) return;
	saved = malloc(bytes);
	UT_ASSERT(saved != NULL);
	if (saved == NULL) return;
	memcpy(saved, page, bytes);
	calls = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0,
		(ClusterRecoveryRelmapHistoryEntryV1 *)page));
	UT_ASSERT_EQ(memcmp(page, saved, bytes), 0);
	/* Preserve valid cleanup even for a buggy implementation that wrote into
	 * its borrowed output, so the regression reports the actual refusal. */
	memcpy(page, saved, bytes);
	free(saved);
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_pages_changed_payload_revokes_copied_source)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryRelmapHistoryEntryV1 entry, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	NativeInventoryPage *page;
	unsigned calls;

	episode_pages_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	UT_ASSERT(history != NULL);
	if (history == NULL) return;
	page = history->inventory->pages;
	UT_ASSERT(page != NULL && page->reader.record->main_data_len == 4);
	if (page == NULL) return;
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	page->reader.record->main_data[0] ^= 1;
	calls = inventory_census_calls;
	memset(&entry, 0x5a, sizeof(entry));
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT(history->failed && !history->inventory->complete && relmap_failed > 0);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	episode_history_done(&history, &inputs);
}
