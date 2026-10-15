/* Native PREPARE through original ROOT/claim/WAL and complete re-observation.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned inventory_prepare_change;

static void
inventory_prepare_drift(void)
{
	char bytes[TEST_PREPARE_MAX];
	uint32 length = test_prepare_payload(bytes, true, 13);
	if (inventory_prepare_change == 0)
		bytes[length - MAXALIGN(sizeof(TwoPhaseRecordOnDisk)) - 1] ^= 1;
	else if (inventory_prepare_change == 1)
		bytes[MAXALIGN(sizeof(xl_xact_prepare)) + 13] ^= 1; /* GID padding */
	(void)inventory_write_record(
		&inventory_fault_source, inventory_extra_start, inventory_extra_previous,
		inventory_prepare_change == 2 ? RM_XLOG_ID : RM_XACT_ID,
		inventory_prepare_change == 2 ? XLOG_NOOP : XLOG_XACT_PREPARE, bytes, length);
}

UT_TEST(test_cold_prepare_full_payload_and_polarities_remain_unowned)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventoryResult result;
	ClusterRecoveryRootSet *roots;
	char path[MAXPGPATH], expected[TEST_PREPARE_MAX];
	uint32 length = test_prepare_payload(expected, true, 13);
	int fd;
	unsigned prepared = 0, commitrels = 0, abortrels = 0, resources = 0, ends = 0;
	unsigned terminals = 0, other_pending = 0;
	inventory_extra = 15;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	result = cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024, &inventory,
															 &diag);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	if (inventory == NULL)
		goto cleanup;
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 8);
	UT_ASSERT_EQ(diag.native_records, 6);
	UT_ASSERT_EQ(diag.prepare_records, 2);
	UT_ASSERT_EQ(diag.other_records, 2);
	UT_ASSERT_EQ(diag.unowned_target_records, 4); /* 2 checkpoints + 2 pending PREPARE */
	while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_PREPARE) {
			UT_ASSERT_EQ(effect.length, length);
			UT_ASSERT(memcmp(effect.bytes, expected, length) == 0);
			UT_ASSERT_EQ(effect.record->terminal_scn, 0);
			UT_ASSERT_EQ(effect.record->prepared_resource_count, TWOPHASE_RM_MAX_ID);
			UT_ASSERT(effect.record->source.claim.identity.origin_node_id == 0
					  || effect.record->source.claim.identity.origin_node_id == 127);
			UT_ASSERT_EQ(effect.record->cut.failed_thread,
						 effect.record->source.claim.identity.origin_thread_id);
			prepared++;
		} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_RELATION)
			commitrels++;
		else if (effect.kind == CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_RELATION)
			abortrels++;
		else if (effect.kind == CLUSTER_RECOVERY_NATIVE_PREPARE_RESOURCE)
			resources++;
		else if (effect.kind == CLUSTER_RECOVERY_NATIVE_PREPARE_END)
			ends++;
		else if (effect.kind == CLUSTER_RECOVERY_NATIVE_TERMINAL)
			terminals++;
		else if (effect.kind >= CLUSTER_RECOVERY_NATIVE_PREPARE)
			other_pending++;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT_EQ(prepared, 2);
	UT_ASSERT_EQ(commitrels, 4);
	UT_ASSERT_EQ(abortrels, 4);
	UT_ASSERT_EQ(resources, 2 * TWOPHASE_RM_MAX_ID);
	UT_ASSERT_EQ(ends, 2);
	UT_ASSERT_EQ(other_pending, 16);
	UT_ASSERT_EQ(terminals, 2); /* only the ordinary COMMITs, never PREPARE */
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
cleanup:
	cluster_recovery_native_inventory_destroy(inventory);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_prepare_two_pass_and_final_drift_revoke_roots)
{
	for (unsigned mode = 0; mode < 6; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		inventory_extra = 15;
		inputs = inventory_fixture(false, false);
		roots = inventory_roots(inputs, path, &fd, false);
		inventory_prepare_change = mode % 3;
		if (mode < 3) {
			inventory_between_passes = inventory_prepare_drift;
			UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																		 &inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT(inventory == NULL);
		} else {
			UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																		 &inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			if (inventory == NULL)
				goto cleanup;
			while (cluster_recovery_native_inventory_next(inventory, &effect)
				   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {}
			inventory_prepare_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
			UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		}
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cleanup:
		inventory_between_passes = NULL;
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_cold_prepare_invalid_or_completion_records_never_become_other)
{
	for (inventory_prepare_fault = 1; inventory_prepare_fault <= 3; ++inventory_prepare_fault) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		inventory_extra = 15;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED);
		UT_ASSERT(inventory == NULL);
		cold_census_done(&inputs);
	}
	inventory_prepare_fault = inventory_extra = 0;
}
