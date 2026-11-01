/* Complete original native records through the real two-pass census.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned census_native_record_fault, census_native_record_calls;
static uint16 census_bad_owner_families[64];
static uint8 census_bad_raw[8192];
static ClusterRecoveryNativeEffect census_native_bad_effects[64];

static ClusterRecoveryNativeRecordObservationV1
census_native_test_record(ClusterRecoveryNativeInventory *inventory, uint64 index)
{
	ClusterRecoveryNativeRecordObservationV1 out
		= cluster_recovery_native_inventory_record_v1(inventory, index);
	census_native_record_calls++;
	if (census_native_record_fault == 1) inputs_pin_current = false;
	if (census_native_record_fault == 2) pg_re_throw();
	if (census_native_record_fault == 3) out.count++;
	if (census_native_record_fault == 4) out.source_index = out.source_count;
	if (census_native_record_fault == 5) out.primary.record = (void *)inventory;
	if (census_native_record_fault == 21) out.effect_count = 0;
	if (census_native_record_fault == 22) out.effects = NULL;
	if (census_native_record_fault == 23) out.effect_count = SIZE_MAX;
	if (census_native_record_fault == 24 || census_native_record_fault == 25) {
		if (out.effects != NULL && out.effect_count < lengthof(census_native_bad_effects))
			memcpy(census_native_bad_effects, out.effects, out.effect_count * sizeof(*out.effects));
		out.effects = census_native_bad_effects;
		if (census_native_record_fault == 24) census_native_bad_effects[0].kind = 0;
		else census_native_bad_effects[0].record = NULL;
	}
	if (census_native_record_fault == 26) { out.effects = census_native_bad_effects; out.effect_count = 1; }
	if (census_native_record_fault >= 31 && census_native_record_fault <= 36) {
		memset(census_bad_owner_families, 0, sizeof(census_bad_owner_families));
		if (out.owner_families != NULL && out.effect_count < lengthof(census_bad_owner_families))
			memcpy(census_bad_owner_families, out.owner_families, out.effect_count * sizeof(uint16));
		out.owner_families = census_native_record_fault == 31 ? NULL : census_bad_owner_families;
		if (census_native_record_fault == 32) census_bad_owner_families[0] = 0;
		if (census_native_record_fault == 33) census_bad_owner_families[0] = 0x8000;
		if (census_native_record_fault == 34) census_bad_owner_families[0] = 8;
		if (census_native_record_fault == 35) census_bad_owner_families[0] |= 0x3fff;
	}
	if (census_native_record_fault >= 41 && census_native_record_fault <= 48) {
		if (out.raw_record != NULL && out.raw_length <= sizeof(census_bad_raw))
			memcpy(census_bad_raw, out.raw_record, out.raw_length);
		if (census_native_record_fault == 41) out.raw_record = NULL;
		if (census_native_record_fault == 42) out.raw_length--;
		if (census_native_record_fault == 43) out.raw_length++;
		if (census_native_record_fault == 44) out.raw_record_sha256[31] ^= 1;
		if (census_native_record_fault == 45 || census_native_record_fault == 46) {
			out.raw_record = census_bad_raw;
			census_bad_raw[census_native_record_fault == 45 && out.raw_length > 0
				? out.raw_length - 1 : 0] ^= 1;
		}
		if (census_native_record_fault == 47) memset(out.raw_record_sha256, 0, 32);
		if (census_native_record_fault == 48) out.raw_record = census_bad_raw;
	}
	return out;
}

UT_TEST(test_census_native_records_keep_entire_payload_source_and_drained_cursor)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryNativeRecordObservationV1 got;
	ClusterRecoveryNativeEffect effect;
	NativeInventoryRecord *saved;
	uint64 count, index = 0;
	unsigned sources = 0;
	bool undo = false;
	census_native_record_fault = census_native_record_calls = 0;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	count = cursor->diag.inventory.native_records + cursor->diag.inventory.undo_records;
	UT_ASSERT(count > 2); UT_ASSERT(cursor->inventory->cursor == NULL);
	for (saved = cursor->inventory->head; saved != NULL; saved = saved->next, index++) {
		ClusterRecoveryNativeEffect expected;
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(saved->scan, &expected),
			CLUSTER_RECOVERY_NATIVE_EFFECT);
		got = cluster_recovery_census_native_record_v1(cursor, index);
		UT_ASSERT(got.observed); if (!got.observed) goto done;
		UT_ASSERT_EQ(got.index, index); UT_ASSERT_EQ(got.count, count);
		UT_ASSERT_EQ(got.source_count, 2); UT_ASSERT_EQ(got.source_index, saved->source_index);
		UT_ASSERT_EQ(got.record_ordinal, saved->ordinal);
		UT_ASSERT_EQ(got.primary.kind, expected.kind);
		UT_ASSERT(got.primary.record == expected.record && got.primary.bytes == expected.bytes);
		UT_ASSERT_EQ(got.primary.length, expected.length);
		undo |= got.primary.kind == CLUSTER_RECOVERY_NATIVE_UNDO;
		if (got.primary.record != NULL) {
			UT_ASSERT_EQ(got.primary.record->payload_length, expected.record->payload_length);
			UT_ASSERT(memcmp(got.primary.record->payload, expected.record->payload,
				expected.record->payload_length) == 0);
		}
		sources |= 1U << saved->source_index;
	}
	UT_ASSERT_EQ(index, count); UT_ASSERT_EQ(sources, 3); UT_ASSERT(undo);
	got = cluster_recovery_census_native_record_v1(cursor, count);
	UT_ASSERT(got.observed && got.primary.record == NULL && got.primary.bytes == NULL);
	UT_ASSERT_EQ(got.count, count); UT_ASSERT_EQ(got.index, count);
	UT_ASSERT_EQ(got.primary.kind, 0); UT_ASSERT_EQ(got.record_ordinal, 0);
	UT_ASSERT(cursor->inventory->cursor == NULL);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(cursor->inventory, &effect),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
	got = cluster_recovery_census_native_record_v1(cursor, 0);
	UT_ASSERT(got.observed); UT_ASSERT(inputs_pin_held && inputs != NULL);
done:
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_native_records_prefix_locks_and_range_do_not_enter)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryNativeRecordObservationV1 got, zero = {0};
	census_native_record_fault = census_native_record_calls = 0;
	got = cluster_recovery_census_native_record_v1(cursor, 0);
	UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
	UT_ASSERT_EQ(cursor->diag.phase, CLUSTER_RECOVERY_CENSUS_WAL);
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (unsigned mode = 0; mode < 4; mode++) {
		if (mode == 0) inventory_graph_caller_lock = true;
		if (mode == 1) CritSectionCount = 1;
		if (mode == 2) ShutdownRequestPending = true;
		got = cluster_recovery_census_native_record_v1(cursor, mode == 3 ? UINT64_MAX : 0);
		inventory_graph_caller_lock = false; CritSectionCount = 0; ShutdownRequestPending = false;
		UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		UT_ASSERT_EQ(census_native_record_calls, 0);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	}
	got = cluster_recovery_census_native_record_v1(cursor, 0); UT_ASSERT(got.observed);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_native_records_late_current_shape_and_error_are_sticky)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
		ClusterRecoveryNativeRecordObservationV1 got = {0}, zero = {0};
		volatile bool caught = false;
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_native_record_fault = fault;
		PG_TRY(); {
			got = cluster_recovery_census_native_record_v1(cursor,
				fault == 5 ? cursor->diag.inventory.native_records + cursor->diag.inventory.undo_records : 0);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught == (fault == 2)); UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_native_record_fault = 0; inputs_pin_current = true;
		got = cluster_recovery_census_native_record_v1(cursor, 0);
		UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
		census_tx_done(cursor, &inputs, fd, path);
	}
}

UT_TEST(test_census_native_records_original_final_wal_rewrite_refuses_whole_view)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryNativeRecordObservationV1 got, zero = {0};
	census_native_record_fault = census_native_record_calls = 0;
	UT_ASSERT_EQ(owned_census_until(cursor, CLUSTER_RECOVERY_CENSUS_FINAL_WAL),
		CLUSTER_RECOVERY_CENSUS_MORE);
	inventory_fault = 0; inventory_drift();
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
	got = cluster_recovery_census_native_record_v1(cursor, 0);
	UT_ASSERT(memcmp(&got, &zero, sizeof(got)) == 0);
	UT_ASSERT_EQ(census_native_record_calls, 0); UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_tx_done(cursor, &inputs, fd, path);
}
