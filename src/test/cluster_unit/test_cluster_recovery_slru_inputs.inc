/* Real retained cold WAL for the native transaction SLRU lifecycle records.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned inventory_slru_drift_shape;
static bool inventory_slru_change_family;

static void
inventory_slru_drift(void)
{
	unsigned shape = inventory_slru_drift_shape;
	char bytes[sizeof(xl_clog_truncate)];
	uint8 length = inventory_slru_payload(shape, bytes, true);
	(void)inventory_write_record(&inventory_fault_source, inventory_slru_start[shape],
								 inventory_slru_previous[shape],
								 inventory_slru_change_family ? RM_XLOG_ID
								 : shape < 2				  ? RM_CLOG_ID
															  : RM_COMMIT_TS_ID,
								 inventory_slru_change_family ? XLOG_NOOP : 0x10, bytes, length);
}

UT_TEST(test_cold_slru_records_keep_all_shapes_and_origin_targets)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventoryResult result;
	ClusterRecoveryRootSet *roots;
	char path[MAXPGPATH];
	int fd;
	unsigned counts[4] = { 0 }, origins[2] = { 0 };
	inventory_extra = 10;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 10);
	UT_ASSERT_EQ(diag.native_records, 8);
	UT_ASSERT_EQ(diag.clog_records, 4);
	UT_ASSERT_EQ(diag.commit_ts_records, 0);
	UT_ASSERT_EQ(diag.other_records, 2);
	UT_ASSERT(diag.unowned_target_records >= 4);
	{
		ClusterRecoveryRootsEvent event;
		ClusterRecoveryRootsResult observed;
		do {
			observed = cluster_recovery_root_set_step(roots, &event);
		} while (observed < CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(observed, CLUSTER_RECOVERY_ROOTS_OBSERVED);
		for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
			const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
			if (entry == NULL || entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
				continue;
			UT_ASSERT(entry->file.native_origin_node == 0 || entry->file.native_origin_node == 127);
			origins[entry->file.native_origin_node == 127]++;
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->file.native_segment, 1);
			UT_ASSERT_EQ(entry->required_blocks, 2);
		}
		UT_ASSERT_EQ(origins[0], 1);
		UT_ASSERT_EQ(origins[1], 1);
	}
	while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		if (effect.kind < CLUSTER_RECOVERY_NATIVE_CLOG_ZERO)
			continue;
		UT_ASSERT(effect.kind <= CLUSTER_RECOVERY_NATIVE_COMMIT_TS_TRUNCATE);
		if (effect.kind > CLUSTER_RECOVERY_NATIVE_COMMIT_TS_TRUNCATE)
			break;
		counts[effect.kind - CLUSTER_RECOVERY_NATIVE_CLOG_ZERO]++;
		UT_ASSERT_EQ(effect.record->cut.failed_thread,
					 effect.record->source.claim.identity.origin_thread_id);
		UT_ASSERT_EQ(effect.record->terminal_scn, 0);
		UT_ASSERT_EQ(effect.length, effect.record->payload_length);
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	for (unsigned i = 0; i < 4; ++i)
		UT_ASSERT_EQ(counts[i], i < 2 ? 2 : 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	cluster_recovery_native_inventory_destroy(inventory);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_slru_second_and_final_reads_revoke_the_union)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		inventory_extra = 10;
		inputs = inventory_fixture(false, false);
		roots = inventory_roots(inputs, path, &fd, false);
		inventory_slru_drift_shape = 1;
		inventory_slru_change_family = mode % 2;
		if (mode < 2) {
			inventory_between_passes = inventory_slru_drift;
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
			inventory_slru_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			memset(&effect, 0xA5, sizeof(effect));
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

UT_TEST(test_cold_slru_bad_records_never_become_other)
{
	for (inventory_slru_fault = 1; inventory_slru_fault <= 3; inventory_slru_fault += 2) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		inventory_extra = 10;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED);
		UT_ASSERT(inventory == NULL);
		cold_census_done(&inputs);
	}
	inventory_slru_fault = inventory_extra = 0;
}

UT_TEST(test_cold_commit_ts_keeps_original_profile_refusal)
{
	for (inventory_extra = 11; inventory_extra <= 12; ++inventory_extra) {
		ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		roots = inventory_roots(inputs, path, &fd, false);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																	 &inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT);
		UT_ASSERT_EQ(diag.input_result, CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
		UT_ASSERT_EQ(diag.commit_ts_records, 0);
		UT_ASSERT(inventory == NULL);
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}
