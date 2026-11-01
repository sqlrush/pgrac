/* Complete compound input through the original two-pass inventory/census.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_census_compound_groups_keep_all_components_without_cursor_or_io)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryNativeRecordObservationV1 out;
	ClusterRecoveryNativeEffect effect;
	uint64 total = 0;
	unsigned messages = 0, undo = 0, calls;
	size_t bytes;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	calls = inventory_census_calls; bytes = cursor->inventory->diag.owned_bytes;
	for (uint64 i = 0;; i++) {
		out = cluster_recovery_census_native_record_v1(cursor, i);
		UT_ASSERT(out.observed); if (!out.observed) break;
		if (i == out.count) {
			UT_ASSERT(out.effects == NULL && out.effect_count == 0); break;
		}
		UT_ASSERT(out.effects != NULL && out.effect_count != 0);
		if (out.effects == NULL || out.effect_count == 0) continue;
		UT_ASSERT(out.effects[0].record == out.primary.record);
		UT_ASSERT_EQ(out.effects[0].kind, out.primary.kind);
		UT_ASSERT_EQ(out.effects[0].ordinal, 0);
		for (size_t j = 0; j < out.effect_count; j++) {
			const ClusterRecoveryNativeEffect *part = &out.effects[j];
			UT_ASSERT(part->record == out.primary.record);
			UT_ASSERT(part->bytes != NULL && part->length != 0);
			messages += part->kind == CLUSTER_RECOVERY_NATIVE_INVALIDATION;
			undo += part->kind == CLUSTER_RECOVERY_NATIVE_UNDO;
		}
		total += out.effect_count;
		{
			ClusterRecoveryNativeRecordObservationV1 again
				= cluster_recovery_census_native_record_v1(cursor, i);
			UT_ASSERT(again.observed && again.effects == out.effects);
			UT_ASSERT_EQ(again.effect_count, out.effect_count);
		}
	}
	UT_ASSERT_EQ(total, cursor->diag.effects); UT_ASSERT(messages > 0 && undo > 0);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(cursor->inventory->diag.owned_bytes, bytes);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(cursor->inventory, &effect),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_compound_groups_refuse_partial_mixed_and_nonempty_end)
{
	for (unsigned fault = 21; fault <= 26; fault++) {
		ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_native_record_fault = fault;
		out = cluster_recovery_census_native_record_v1(cursor, fault == 26
			? cursor->diag.inventory.native_records + cursor->diag.inventory.undo_records : 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_native_record_fault = 0;
		out = cluster_recovery_census_native_record_v1(cursor, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		census_tx_done(cursor, &inputs, fd, path);
	}
}

UT_TEST(test_inventory_compound_groups_charge_the_original_budget_without_prefix)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	size_t exact;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	exact = diag.owned_bytes;
	cluster_recovery_native_inventory_destroy(inventory); inventory = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, exact, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(diag.owned_bytes, exact);
	cluster_recovery_native_inventory_destroy(inventory); inventory = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, exact - 1, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY);
	UT_ASSERT(inventory == NULL && inputs_pin_held);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}
