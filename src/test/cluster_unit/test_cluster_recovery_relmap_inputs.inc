/* Original retained ROOT/claim/WAL reader, including non-tail image drift.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool inventory_relmap_change_family;

static void
inventory_relmap_drift(void)
{
	char bytes[TEST_RELMAP_LENGTH];
	test_relmap_payload(bytes, true, 2);
	bytes[MinSizeOfRelmapUpdate + offsetof(RelMapFile, mappings) + 3 * sizeof(RelMapping)] ^= 1;
	test_relmap_crc(bytes); /* valid image CRC AND WAL CRC; inactive mapping changes */
	(void)inventory_write_record(
		&inventory_fault_source, inventory_extra_start, inventory_extra_previous,
		inventory_relmap_change_family ? RM_XLOG_ID : RM_RELMAP_ID,
		inventory_relmap_change_family ? XLOG_NOOP : XLOG_RELMAP_UPDATE, bytes, sizeof(bytes));
}

UT_TEST(test_cold_relmap_owns_images_entries_and_all_target_paths)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventoryResult result;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryRootsResult observed;
	ClusterRecoveryRootsEvent event;
	char path[MAXPGPATH];
	int fd;
	unsigned maps = 0, mappings = 0, copies = 0, mapped_forks = 0;
	inventory_extra = 14;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 8);
	UT_ASSERT_EQ(diag.native_records, 6);
	UT_ASSERT_EQ(diag.relmap_records, 2);
	UT_ASSERT_EQ(diag.other_records, 2);
	UT_ASSERT_EQ(diag.unowned_target_records, 4); /* original checkpoints plus map obligations */
	do {
		observed = cluster_recovery_root_set_step(roots, &event);
	} while (observed < CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(observed, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (entry->file.kind >= CLUSTER_RECOVERY_FILE_RELMAP
			&& entry->file.kind <= CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP) {
			UT_ASSERT(entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->required_blocks, 0);
			copies++;
		}
		if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELATION
			&& (entry->file.locator.relNumber == 16385 || entry->file.locator.relNumber == 16386))
			mapped_forks++;
	}
	UT_ASSERT_EQ(copies, 8);
	UT_ASSERT_EQ(mapped_forks, 16);
	while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE) {
			UT_ASSERT_EQ(effect.length, TEST_RELMAP_LENGTH);
			UT_ASSERT_EQ(effect.record->nmappings, 2);
			UT_ASSERT_EQ(effect.record->database,
						 effect.record->source.claim.identity.origin_node_id == 0 ? 0 : 5);
			UT_ASSERT_EQ(effect.record->cut.failed_thread,
						 effect.record->source.claim.identity.origin_thread_id);
			maps++;
		} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_RELMAP_ENTRY) {
			UT_ASSERT_EQ(effect.length, sizeof(RelMapping));
			mappings++;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT_EQ(maps, 2);
	UT_ASSERT_EQ(mappings, 4);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cluster_recovery_native_inventory_destroy(inventory);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_relmap_second_and_final_reads_revoke_observations)
{
	for (unsigned mode = 0; mode < 4; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		inventory_extra = 14;
		inputs = inventory_fixture(false, false);
		roots = inventory_roots(inputs, path, &fd, false);
		inventory_relmap_change_family = mode % 2;
		if (mode < 2) {
			inventory_between_passes = inventory_relmap_drift;
			UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																		 &inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT(inventory == NULL);
		} else {
			UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																		 &inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			while (cluster_recovery_native_inventory_next(inventory, &effect)
				   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {}
			inventory_relmap_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
			UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		}
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		inventory_between_passes = NULL;
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_cold_relmap_malformed_or_unknown_never_becomes_other)
{
	for (inventory_relmap_fault = 1; inventory_relmap_fault <= 2; ++inventory_relmap_fault) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		inventory_extra = 14;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED);
		UT_ASSERT(inventory == NULL);
		cold_census_done(&inputs);
	}
	inventory_relmap_fault = inventory_extra = 0;
}
