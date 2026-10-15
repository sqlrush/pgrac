/* Real cold ROOT/claim/WAL input for native MultiXact effects.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static bool inventory_mx_change_family;

static void
inventory_mx_drift(void)
{
	char bytes[SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember)];

	inventory_mx_payload(bytes, false, true);
	(void)inventory_write_record(&inventory_fault_source, inventory_mx_start, inventory_mx_previous,
								 inventory_mx_change_family ? RM_XLOG_ID : RM_MULTIXACT_ID,
								 inventory_mx_change_family ? XLOG_NOOP : XLOG_MULTIXACT_CREATE_ID,
								 bytes, sizeof(bytes));
}

UT_TEST(test_cold_multixact_records_keep_all_shapes_and_origin_sources)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventoryResult result;
	ClusterRecoveryRootSet *roots;
	char path[MAXPGPATH];
	int fd;
	unsigned counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE + 1] = { 0 };
	unsigned origin_counts[2] = { 0 };

	inventory_extra = 6;
	inputs = inventory_fixture(false, false);
	roots = inventory_roots(inputs, path, &fd, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(inputs, roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(diag.records, 14);
	UT_ASSERT_EQ(diag.native_records, 12);
	UT_ASSERT_EQ(diag.multixact_records, 8);
	UT_ASSERT_EQ(diag.other_records, 2);
	/* Physical MX targets/retention remain separate owner obligations. */
	UT_ASSERT(diag.unowned_target_records >= 8);
	while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		UT_ASSERT(effect.kind <= CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE);
		if (effect.kind > CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE)
			break;
		counts[effect.kind]++;
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER) {
			MultiXactMember member;
			unsigned thread = effect.record->source.claim.identity.origin_thread_id;

			UT_ASSERT(thread == 1 || thread == 128);
			UT_ASSERT_EQ(effect.record->cut.failed_thread, thread);
			UT_ASSERT_EQ(effect.record->source.claim.identity.origin_node_id, thread - 1);
			origin_counts[thread == 128]++;
			memcpy(&member, effect.bytes, sizeof(member));
			UT_ASSERT_EQ(member.xid, effect.ordinal == 0 ? 4195136 : 4195138);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER], 4);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_OFFSETS], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_MEMBERS], 2);
	UT_ASSERT_EQ(counts[CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE], 2);
	UT_ASSERT_EQ(origin_counts[0], 2);
	UT_ASSERT_EQ(origin_counts[1], 2);
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

UT_TEST(test_cold_multixact_second_and_final_reads_revoke_observations)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;

		inventory_extra = 6;
		inputs = inventory_fixture(false, false);
		inventory_mx_change_family = mode % 2;
		if (mode < 2) {
			inventory_between_passes = inventory_mx_drift;
			UT_ASSERT_EQ(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT(inventory == NULL);
		} else {
			unsigned members = 0;

			UT_ASSERT_EQ(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			while (cluster_recovery_native_inventory_next(inventory, &effect)
				   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT)
				members += effect.kind == CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER;
			UT_ASSERT_EQ(members, 4);
			inventory_mx_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			memset(&effect, 0xa5, sizeof(effect));
			UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
			UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
		}
		inventory_between_passes = NULL;
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_cold_multixact_unknown_and_bad_shapes_never_become_other)
{
	for (inventory_extra = 7; inventory_extra <= 8; inventory_extra++) {
		ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;

		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED);
		UT_ASSERT(inventory == NULL);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}
