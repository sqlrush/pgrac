/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_wal_sources.inc
 *   Complete original native WAL source observations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_recovery_wal_sources.inc
 *
 * NOTES
 *   PGRAC-original fixture fragment; cluster_recovery_* product entry points
 *   run against the existing native ROOT/claim/WAL files and both full passes.
 *-------------------------------------------------------------------------
 */

UT_TEST(test_wal_sources_include_all_records_without_page_inputs)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryWalSourceObservationV1 value, copy, zero = {0};
	ClusterWalInputsV1 *inputs;
	uint32 count = 99, pages = 99;
	uint64 records = 0;
	uint8 empty_digest[32] = {0};
	inventory_extra = 1; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, cluster_wal_inputs_count_v1(inputs)); UT_ASSERT_EQ(count, 2);
	UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &pages));
	UT_ASSERT_EQ(pages, 0);
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, i, &value));
		UT_ASSERT_EQ(value.source_index, i);
		UT_ASSERT(memcmp(&value.source, &item->source, sizeof(value.source)) == 0);
		UT_ASSERT_EQ(value.cut.failed_thread, i ? 128 : 1);
		UT_ASSERT_EQ(value.cut.scan_end_exclusive, item->checkpoint.validated_tail_lsn_exclusive);
		UT_ASSERT_EQ(value.observed.complete_end, value.cut.scan_end_exclusive);
		UT_ASSERT_EQ(value.observed.last_record_start, item->checkpoint.tail_last_record_lsn);
		UT_ASSERT_EQ(value.observed.last_record_crc, item->checkpoint.tail_last_record_crc32c);
		UT_ASSERT(value.observed.records >= 4); records += value.observed.records;
		UT_ASSERT(memcmp(value.transcript_sha256, empty_digest, 32) != 0);
		copy = value; memset(&value, 0xa5, sizeof(value));
		UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, i, &value));
		UT_ASSERT(memcmp(&value, &copy, sizeof(value)) == 0);
	}
	UT_ASSERT_EQ(records, diag.records); UT_ASSERT_EQ(inventory_census_calls, 2);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_native_inventory_source_at_v1(inventory, count, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_wal_sources_keep_explicit_empty_and_every_original_generation)
{
	int saved_node = cluster_node_id, fd = -1;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootSet *roots = NULL;
	ClusterRecoveryWalSourceObservationV1 value;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	uint32 count = 99, empty = 0;
	(void)pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed) goto cleanup;
	test_worker_pin_held = false; test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0; test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
		&control, &token), 0);
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock; test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL) goto cleanup;
	inventory_census_calls = 0; inventory_between_passes = NULL;
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
		&inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, cluster_wal_inputs_count_v1(inputs));
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *item = NULL;
		unsigned matched = 0;
		UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, i, &value));
		UT_ASSERT_EQ(value.source_index, i);
		/* Native physical order is independent of the original directory
		 * order, especially for historical generations of the same thread. */
		for (uint32 j = 0; j < count; j++) {
			const ClusterWalInputV1 *candidate = cluster_wal_inputs_at_v1(inputs, j);
			if (memcmp(&value.source, &candidate->source, sizeof(value.source)) == 0) {
				item = candidate; matched++;
			}
		}
		UT_ASSERT_EQ(matched, 1);
		if (item == NULL) continue;
		UT_ASSERT_EQ(value.cut.origin_owner_incarnation,
			item->source.claim.identity.origin_owner_incarnation);
		if (value.cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) {
			empty++;
			UT_ASSERT_EQ(value.cut.scan_begin_inclusive, value.cut.scan_end_exclusive);
			UT_ASSERT_EQ(value.observed.records, 0);
		}
	}
	UT_ASSERT_EQ(empty, 1); UT_ASSERT_EQ(inventory_census_calls, 2);
cleanup:
	if (roots != NULL) inventory_roots_done(roots, fd, path);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	test_reserve_mode = false; cluster_node_id = saved_node;
	test_self_incarnation = saved_incarnation;
}

UT_TEST(test_wal_sources_preserve_page_and_native_effect_cursors)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryWalSourceObservationV1 before, after;
	ClusterRecoveryPageWalV1 page;
	ClusterWalInputsV1 *inputs;
	uint32 count;
	inventory_extra = 17; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, 0, &before));
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	UT_ASSERT(cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, 0, &page));
	UT_ASSERT_EQ(page.source_index, before.source_index);
	UT_ASSERT(page.reader->EndRecPtr < before.cut.scan_end_exclusive);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, 0, &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_wal_sources_missing_incomplete_and_crc_valid_rewrite_refuse)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryWalSourceObservationV1 value, zero = {0};
	ClusterWalInputsV1 *inputs;
	uint32 count = 99;
	UT_ASSERT(!cluster_recovery_native_inventory_source_count_v1(NULL, &count));
	UT_ASSERT_EQ(count, 0);
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	inventory->complete = false;
	UT_ASSERT(!cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 0); inventory->complete = true;
	inventory_fault = 0; inventory_drift();
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_native_inventory_source_at_v1(inventory, 0, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(!cluster_recovery_native_inventory_source_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 0);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
}

UT_TEST(test_wal_sources_census_requires_full_manifest_and_retains_zero_page_sources)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusManifest manifest;
	ClusterRecoveryWalSourceObservationV1 value, zero = {0};
	uint32 count = 99, pages = 99;
	uint64 records = 0;
	char path[MAXPGPATH]; int fd;
	census = owned_census_fixture(&inputs, &fd, path, false);
	UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_manifest(census, &manifest));
	UT_ASSERT(cluster_recovery_census_wal_source_count_v1(census, &count));
	UT_ASSERT_EQ(count, manifest.source_count);
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &pages)); UT_ASSERT_EQ(pages, 0);
	for (uint32 i = 0; i < count; i++) {
		UT_ASSERT(cluster_recovery_census_wal_source_at_v1(census, i, &value));
		UT_ASSERT_EQ(value.source_index, i); records += value.observed.records;
	}
	UT_ASSERT_EQ(records, manifest.wal_record_count);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_census_wal_source_at_v1(census, count, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(cluster_recovery_census_wal_source_count_v1(census, &count));
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_wal_sources_census_scope_loss_or_original_rewrite_is_sticky)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryCensus *census;
		ClusterRecoveryWalSourceObservationV1 value, zero = {0};
		uint32 count = 99;
		char path[MAXPGPATH]; int fd;
		census = owned_census_fixture(&inputs, &fd, path, false);
		if (fault == 0) {
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
			UT_ASSERT(cluster_recovery_census_wal_source_count_v1(census, &count));
			inputs_pin_current = false;
		} else {
			UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_WAL),
				CLUSTER_RECOVERY_CENSUS_MORE);
			inventory_fault = 0; inventory_drift();
			UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		}
		UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census, &count));
		UT_ASSERT_EQ(count, 0); inputs_pin_current = true;
		memset(&value, 0xa5, sizeof(value));
		UT_ASSERT(!cluster_recovery_census_wal_source_at_v1(census, 0, &value));
		UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
		UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census, &count));
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_wal_sources_census_full_source_bytes_remain_bound_to_original_manifest)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryCensus *census;
		ClusterRecoveryWalSourceObservationV1 value, zero = {0};
		uint32 count = 99;
		char path[MAXPGPATH]; int fd;
		census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT(cluster_recovery_census_wal_source_count_v1(census, &count));
		if (fault == 0) census->inventory->sources[0].source.claim.identity.root_lineage_seq++;
		if (fault == 1) census->inventory->sources[0].cut.scan_end_exclusive++;
		if (fault == 2) census->inventory->sources[0].observed.last_record_crc++;
		if (fault == 3) census->inventory->sources[0].transcript[31]++;
		UT_ASSERT(!cluster_recovery_census_wal_source_count_v1(census, &count));
		UT_ASSERT_EQ(count, 0);
		memset(&value, 0xa5, sizeof(value));
		UT_ASSERT(!cluster_recovery_census_wal_source_at_v1(census, 0, &value));
		UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
		owned_census_done(census, &inputs, fd, path);
	}
}
