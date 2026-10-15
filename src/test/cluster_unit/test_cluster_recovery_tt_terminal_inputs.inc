/* Full retained terminal comparison, using original cold reader and TT files. */
static ClusterRecoveryNativeInventory *
tt_terminal_inventory(unsigned mode, ClusterWalInputsV1 **inputs, ClusterRecoveryTxnKey *key)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryTerminalEntry entry;
	ClusterRecoveryTerminalResult result;
	bool found = false;
	inventory_extra = 16;
	inventory_terminal_mode = mode;
	*inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(
		cluster_recovery_native_inventory_build(*inputs, 16 * 1024 * 1024, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	while ((result = terminal_next_entry(inventory, &entry, &diag))
		   == CLUSTER_RECOVERY_TERMINAL_ENTRY) {
		if (!found && terminal_key_known(entry.identity)) {
			*key = entry.transaction;
			found = true;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_TERMINAL_DRAINED);
	UT_ASSERT(found);
	return inventory;
}

static ClusterRecoveryTerminalResult
tt_terminal_lookup(ClusterRecoveryNativeInventory *inventory, const ClusterRecoveryTxnKey *key,
				   ClusterRecoveryTerminalSummary *out)
{
	ClusterRecoveryNativeInventoryDiag diag = { 0 };
	uint64 examined = 0;
	for (unsigned step = 0; step < 10000; step++) {
		ClusterRecoveryTerminalResult result
			= cluster_recovery_native_inventory_terminal_lookup_step(inventory, key, out, &diag);
		UT_ASSERT(diag.terminal_lookup_examined - examined <= 128 || examined == 0);
		examined = diag.terminal_lookup_examined;
		if (result != CLUSTER_RECOVERY_TERMINAL_MORE)
			return result;
		UT_ASSERT_EQ(out->exact_records | out->unresolved_records, 0);
		UT_ASSERT(out->first_exact == NULL);
	}
	return CLUSTER_RECOVERY_TERMINAL_INVALID;
}

UT_TEST(test_terminal_lookup_exact_identity_and_unbound_records)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryTxnKey key, changed;
	ClusterRecoveryNativeInventory *inventory = tt_terminal_inventory(0, &inputs, &key);
	ClusterRecoveryTerminalSummary out;
	for (unsigned origin = 0; origin < 2; origin++) {
		key.origin_node_id = origin ? 127 : 0;
		key.segment_id = origin ? 32513 : 1;
		UT_ASSERT_EQ(tt_terminal_lookup(inventory, &key, &out), CLUSTER_RECOVERY_TERMINAL_ENTRY);
		UT_ASSERT_EQ(out.exact_records, 4);
		UT_ASSERT_EQ(out.unresolved_records, 2);
		UT_ASSERT(out.has_commit && out.has_abort && out.conflict);
		UT_ASSERT(out.first_exact != NULL);
		if (out.first_exact != NULL)
			UT_ASSERT_EQ(out.first_exact->source.claim.identity.origin_thread_id, origin ? 128 : 1);
		for (unsigned field = 0; field < 4; field++) {
			changed = key;
			if (field == 0)
				changed.segment_generation++;
			if (field == 1)
				changed.slot_wrap++;
			if (field == 2)
				changed.slot_offset++;
			if (field == 3)
				changed.xid++;
			UT_ASSERT_EQ(tt_terminal_lookup(inventory, &changed, &out),
						 CLUSTER_RECOVERY_TERMINAL_ENTRY);
			UT_ASSERT_EQ(out.exact_records, 0);
			UT_ASSERT_EQ(out.unresolved_records, field == 3 ? 0 : 2);
			UT_ASSERT(!out.has_commit && !out.has_abort && !out.conflict);
			UT_ASSERT(out.first_exact == NULL);
		}
	}
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_terminal_lookup_consistent_and_conflicting_endpoints)
{
	for (unsigned mode = 16; mode <= 18; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryTxnKey key;
		ClusterRecoveryTerminalSummary out;
		ClusterRecoveryNativeInventory *inventory = tt_terminal_inventory(mode, &inputs, &key);
		UT_ASSERT_EQ(tt_terminal_lookup(inventory, &key, &out), CLUSTER_RECOVERY_TERMINAL_ENTRY);
		UT_ASSERT_EQ(out.exact_records, 2);
		UT_ASSERT_EQ(out.unresolved_records, mode == 18 ? 2 : 1);
		UT_ASSERT_EQ(out.has_commit, mode != 18);
		UT_ASSERT_EQ(out.has_abort, mode == 18);
		UT_ASSERT_EQ(out.conflict, mode == 17);
		UT_ASSERT_EQ(out.commit_scn, mode == 18 ? InvalidScn : 5906);
		/* The native-only carrier remains unresolved even when exact TT WAL agrees. */
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_terminal_lookup_bounded_query_and_key_change)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryTxnKey key;
		ClusterRecoveryTerminalSummary out, zero = { 0 };
		ClusterRecoveryNativeInventoryDiag diag = { 0 };
		ClusterRecoveryNativeInventory *inventory = tt_terminal_inventory(20, &inputs, &key);
		uint64 previous = 0;
		unsigned calls = 0;
		ClusterRecoveryTerminalResult result;
		do {
			memset(&out, 0xff, sizeof(out));
			result = cluster_recovery_native_inventory_terminal_lookup_step(inventory, &key, &out,
																			&diag);
			UT_ASSERT(diag.terminal_lookup_examined - previous <= 128);
			previous = diag.terminal_lookup_examined;
			calls++;
			if (result == CLUSTER_RECOVERY_TERMINAL_MORE) {
				UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
				if (fault)
					key.slot_wrap++;
			}
		} while (result == CLUSTER_RECOVERY_TERMINAL_MORE && calls < 20);
		if (fault) {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_TERMINAL_INVALID);
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
			UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, (uint8[32]){ 0 }));
		} else {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_TERMINAL_ENTRY);
			UT_ASSERT(calls >= 2 && calls < 20);
			UT_ASSERT(previous > 144);
			UT_ASSERT_EQ(out.exact_records, 4);
			UT_ASSERT(out.conflict);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_terminal_lookup_final_input_drift_revokes)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryTxnKey key;
	ClusterRecoveryTerminalSummary out, zero = { 0 };
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeInventory *inventory = tt_terminal_inventory(16, &inputs, &key);
	UT_ASSERT_EQ(tt_terminal_lookup(inventory, &key, &out), CLUSTER_RECOVERY_TERMINAL_ENTRY);
	inventory_fault = 0;
	inventory_drift();
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
	memset(&out, 0xff, sizeof(out));
	UT_ASSERT_EQ(tt_terminal_lookup(inventory, &key, &out), CLUSTER_RECOVERY_TERMINAL_CHANGED);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_physical_tt_terminal_census_keeps_every_slot)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusEntry entry;
		ClusterRecoveryCensusDiag diag;
		ClusterRecoveryCensusResult result;
		unsigned total = 0, usable = 0, conflicts = 0, unbound = 0, matches = 0;
		owned_terminals = true;
		inventory_terminal_mode = mode == 3 ? 21 : mode == 2 ? 16 : 0;
		census = owned_census_fixture(&inputs, &fd, path, false);
		if (mode)
			terminal_physical_slots(fd, TT_SLOT_COMMITTED);
		for (unsigned i = 0; i < 200000; i++) {
			result = cluster_recovery_census_next(census, &entry);
			if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED)
				break;
			if (result != CLUSTER_RECOVERY_CENSUS_ENTRY
				|| entry.kind != CLUSTER_RECOVERY_CENSUS_TT_TERMINAL)
				continue;
			total++;
			usable += entry.value.tt_terminal.key_usable;
			conflicts += entry.value.tt_terminal.retained.conflict;
			unbound += entry.value.tt_terminal.retained.unresolved_records != 0;
			matches += entry.value.tt_terminal.raw_terminal_matches;
			if (!entry.value.tt_terminal.key_usable)
				UT_ASSERT_EQ(entry.value.tt_terminal.retained.exact_records, 0);
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT_EQ(total, 96);
		UT_ASSERT_EQ(usable, 2);
		UT_ASSERT_EQ(conflicts, mode == 1 ? 2 : 0);
		UT_ASSERT_EQ(unbound, mode == 1 || mode == 2 ? 2 : 0);
		/* Native-only records preclude claiming complete terminal agreement. */
		UT_ASSERT_EQ(matches, mode == 3 ? 2 : 0);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.tt_terminal_slots, 96);
		UT_ASSERT_EQ(diag.tt_terminal_conflicts, conflicts);
		UT_ASSERT_EQ(diag.tt_terminal_unresolved, unbound);
		UT_ASSERT_EQ(diag.tt_terminal_missing, mode ? 0 : 2);
		owned_census_done(census, &inputs, fd, path);
	}
	owned_terminals = false;
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_physical_tt_terminal_census_final_rechecks)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusDiag diag;
		owned_terminals = true;
		inventory_terminal_mode = 16;
		census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.tt_terminal_slots, 96);
		if (fault == 0)
			terminal_physical_slots(fd, TT_SLOT_ABORTED);
		else if (fault == 1) {
			inventory_fault = 0;
			inventory_drift();
		} else
			inputs_pin_current = false;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
	owned_terminals = false;
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_terminal_lookup_invalid_keys_and_incomplete_cursor_refuse)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryTxnKey key, original;
		ClusterRecoveryTerminalSummary out, zero = { 0 };
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeInventory *inventory = tt_terminal_inventory(16, &inputs, &key);
		original = key;
		if (fault == 0)
			key.system_identifier++;
		if (fault == 1)
			key.database_incarnation++;
		if (fault == 2)
			key.origin_node_id = 128;
		if (fault == 3)
			key.segment_id = 257;
		if (fault == 4)
			key.segment_generation = UINT32_MAX;
		if (fault == 5)
			key.slot_offset = TT_SLOTS_PER_SEGMENT;
		if (fault == 6)
			key.slot_wrap = TT_WRAP_INVALID;
		if (fault == 7)
			key.xid = InvalidTransactionId;
		if (fault == 8) {
			cluster_recovery_native_inventory_destroy(inventory);
			inventory = NULL;
			UT_ASSERT_EQ(
				cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		}
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT_EQ(cluster_recovery_native_inventory_terminal_lookup_step(
						 inventory, fault == 9 ? NULL : &key, &out, &diag),
					 CLUSTER_RECOVERY_TERMINAL_INVALID);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(tt_terminal_lookup(inventory, &original, &out),
					 CLUSTER_RECOVERY_TERMINAL_CHANGED);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_terminal_mode = 0;
}
