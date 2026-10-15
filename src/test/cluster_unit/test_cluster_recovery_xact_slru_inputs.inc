/* Native transaction targets under the original cold ROOT/claim/WAL readers.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static void
inventory_assignment_drift(void)
{
	char bytes[MinSizeOfXactAssignment + 2 * sizeof(TransactionId)] = { 0 };
	TransactionId top = 4196000, children[] = { 4196001, 4198400 };
	int count = 2;
	memcpy(bytes, &top, sizeof(top));
	memcpy(bytes + sizeof(top), &count, sizeof(count));
	memcpy(bytes + MinSizeOfXactAssignment, children, sizeof(children));
	(void)inventory_write_record(&inventory_fault_source, inventory_extra_start,
								 inventory_extra_previous, RM_XACT_ID, XLOG_XACT_ASSIGNMENT, bytes,
								 sizeof(bytes));
}

UT_TEST(test_cold_terminal_assignment_target_union_without_zero)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;
	char path[MAXPGPATH];
	int fd;
	unsigned files[2][2] = { { 0 } };
	inventory_extra = 13;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 8);
	do {
		result = cluster_recovery_root_set_step(roots, &event);
	} while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		bool sub;
		if (entry == NULL || entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
			continue;
		UT_ASSERT(entry->file.native_origin_node == 0 || entry->file.native_origin_node == 127);
		UT_ASSERT(entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_XACT
				  || entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS);
		sub = entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS;
		files[entry->file.native_origin_node == 127][sub]++;
		UT_ASSERT_EQ(entry->file.native_segment, sub ? 0x40 : 4);
		UT_ASSERT_EQ(entry->required_blocks, sub ? 2 : 1);
		UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
	}
	for (unsigned n = 0; n < 2; ++n)
		for (unsigned f = 0; f < 2; ++f)
			UT_ASSERT_EQ(files[n][f], 1);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	cluster_recovery_native_inventory_destroy(inventory);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_child_record_drift_revokes_all_targets)
{
	for (unsigned after = 0; after < 2; ++after) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		char path[MAXPGPATH];
		int fd;
		uint8 digest[32];
		inventory_extra = 13;
		inputs = inventory_fixture(false, false);
		roots = inventory_roots(inputs, path, &fd, false);
		if (!after)
			inventory_between_passes = inventory_assignment_drift;
		UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																	 &inventory, &diag),
					 after ? CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED
						   : CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		if (after) {
			while (cluster_recovery_native_inventory_next(inventory, &effect)
				   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {}
			inventory_assignment_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			memset(&effect, 0xA5, sizeof(effect));
			UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
			UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		} else
			UT_ASSERT(inventory == NULL);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		inventory_between_passes = NULL;
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}
