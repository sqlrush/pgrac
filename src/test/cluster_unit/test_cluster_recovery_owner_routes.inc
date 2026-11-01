/* Unresolved original native component duties, never completion receipts.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_census_owner_routes_keep_every_component_and_original_custody)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	unsigned calls; uint64 seen = 0;
	bool terminal = false, undo = false, mx = false;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	calls = inventory_census_calls;
	for (uint64 i = 0;; i++) {
		ClusterRecoveryNativeRecordObservationV1 out = cluster_recovery_census_native_record_v1(cursor, i);
		UT_ASSERT(out.observed); if (!out.observed) break;
		if (i == out.count) { UT_ASSERT(out.owner_families == NULL); break; }
		UT_ASSERT(out.owner_families != NULL);
		if (out.owner_families == NULL) continue;
		for (size_t j = 0; j < out.effect_count; j++) {
			uint16 family = out.owner_families[j];
			UT_ASSERT(family != 0 && (family & ~UINT16_C(0x3fff)) == 0);
			if (out.effects[j].kind == CLUSTER_RECOVERY_NATIVE_TERMINAL) {
				UT_ASSERT_EQ(family & 3, 3); terminal = true;
			}
			if (out.effects[j].kind == CLUSTER_RECOVERY_NATIVE_UNDO) {
				UT_ASSERT_EQ(family, 8192); undo = true;
			}
			if (out.effects[j].kind == CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER) {
				UT_ASSERT_EQ(family, 16); mx = true;
			}
			seen++;
		}
	}
	UT_ASSERT(terminal && undo && mx); UT_ASSERT_EQ(seen, cursor->diag.effects);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT(cursor->inventory->cursor == NULL && inputs_pin_held);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_owner_routes_missing_wrong_extra_and_nonempty_end_revoke_whole_group)
{
	for (unsigned fault = 31; fault <= 36; fault++) {
		ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_native_record_fault = fault;
		out = cluster_recovery_census_native_record_v1(cursor, fault == 36
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
