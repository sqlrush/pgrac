/* Exact retained terminal edges, not transaction completion authority.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static bool
terminal_key_known(ClusterRecoveryTerminalIdentity identity)
{
	return identity == CLUSTER_RECOVERY_TERMINAL_EXACT
		   || identity == CLUSTER_RECOVERY_TERMINAL_BOUND;
}

static ClusterRecoveryTerminalResult
terminal_next_entry(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryTerminalEntry *entry,
					ClusterRecoveryNativeInventoryDiag *diag)
{
	for (unsigned i = 0; i < 10000; i++) {
		ClusterRecoveryTerminalResult r
			= cluster_recovery_native_inventory_terminal_step(inventory, entry, diag);
		if (r != CLUSTER_RECOVERY_TERMINAL_MORE)
			return r;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_TERMINAL_INVALID;
}

UT_TEST(test_terminal_edges_keep_every_exact_and_native_carrier)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryTerminalEntry entry;
	ClusterRecoveryTerminalResult r;
	ClusterWalInputsV1 *inputs;
	unsigned identities[6] = { 0 }, commits = 0, aborts = 0;
	inventory_extra = 16;
	inventory_terminal_mode = 0;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	while ((r = terminal_next_entry(inventory, &entry, &diag)) == CLUSTER_RECOVERY_TERMINAL_ENTRY) {
		UT_ASSERT(entry.record != NULL);
		UT_ASSERT(entry.identity >= 1 && entry.identity <= 5);
		identities[entry.identity]++;
		UT_ASSERT_EQ(entry.transaction.system_identifier,
					 entry.record->source.claim.identity.system_identifier);
		UT_ASSERT_EQ(entry.transaction.database_incarnation,
					 entry.record->source.claim.database_incarnation);
		UT_ASSERT_EQ(entry.transaction.origin_node_id,
					 entry.record->source.claim.identity.origin_thread_id - 1);
		UT_ASSERT_EQ(entry.transaction.xid, 4195136);
		commits += entry.status == TT_SLOT_COMMITTED;
		aborts += entry.status == TT_SLOT_ABORTED;
		if (terminal_key_known(entry.identity)) {
			UT_ASSERT_EQ(entry.transaction.segment_id, entry.transaction.origin_node_id * 256 + 1);
			UT_ASSERT_EQ(entry.transaction.segment_generation, 0);
			UT_ASSERT_EQ(entry.transaction.slot_offset, 0);
			UT_ASSERT_EQ(entry.transaction.slot_wrap, 1);
			UT_ASSERT_EQ(entry.scn, entry.status == TT_SLOT_COMMITTED ? 5906 : InvalidScn);
		}
		if (entry.identity == CLUSTER_RECOVERY_TERMINAL_BOUND) {
			UT_ASSERT(entry.binding_record != NULL);
			UT_ASSERT(entry.binding_record->read_lsn < entry.record->read_lsn);
			UT_ASSERT(inventory_source_equal(&entry.binding_record->source, &entry.record->source));
			UT_ASSERT(inventory_cut_equal(&entry.binding_record->cut, &entry.record->cut));
		} else
			UT_ASSERT(entry.binding_record == NULL);
	}
	UT_ASSERT_EQ(r, CLUSTER_RECOVERY_TERMINAL_DRAINED);
	UT_ASSERT_EQ(identities[CLUSTER_RECOVERY_TERMINAL_NATIVE_ONLY], 4);
	UT_ASSERT_EQ(identities[CLUSTER_RECOVERY_TERMINAL_EXACT], 4);
	UT_ASSERT_EQ(identities[CLUSTER_RECOVERY_TERMINAL_BOUND], 4);
	UT_ASSERT_EQ(commits, 6);
	UT_ASSERT_EQ(aborts, 6);
	UT_ASSERT_EQ(diag.terminal_records, 12);
	UT_ASSERT_EQ(diag.terminal_bound, 8);
	UT_ASSERT_EQ(diag.terminal_unbound, 4);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_terminal_edges_never_guess_missing_or_reused_bindings)
{
	for (unsigned mode = 2; mode <= 5; mode++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryTerminalEntry entry;
		ClusterRecoveryTerminalResult r;
		ClusterWalInputsV1 *inputs;
		unsigned ids[6] = { 0 };
		inventory_extra = 16;
		inventory_terminal_mode = mode;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		while ((r = terminal_next_entry(inventory, &entry, &diag))
			   == CLUSTER_RECOVERY_TERMINAL_ENTRY) {
			ids[entry.identity]++;
			if (entry.identity >= CLUSTER_RECOVERY_TERMINAL_BINDING_MISSING) {
				UT_ASSERT(!terminal_key_known(entry.identity));
				UT_ASSERT(entry.binding_record == NULL);
			}
		}
		UT_ASSERT_EQ(r, CLUSTER_RECOVERY_TERMINAL_DRAINED);
		UT_ASSERT_EQ(ids[CLUSTER_RECOVERY_TERMINAL_EXACT], 4);
		UT_ASSERT_EQ(ids[CLUSTER_RECOVERY_TERMINAL_BINDING_MISSING], mode == 2	 ? 4
																	 : mode == 5 ? 2
																				 : 0);
		UT_ASSERT_EQ(ids[CLUSTER_RECOVERY_TERMINAL_BINDING_MISMATCH],
					 mode == 3 || mode == 4 ? 4 : 0);
		UT_ASSERT_EQ(ids[CLUSTER_RECOVERY_TERMINAL_BOUND], mode == 5 ? 2 : 0);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_terminal_mode = 0;
}

UT_TEST(test_terminal_edges_preserve_effects_and_final_revocation)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryTerminalEntry entry, zero = { 0 };
		ClusterWalInputsV1 *inputs;
		volatile bool caught = false;
		unsigned effects = 0;
		inventory_extra = 16;
		inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(
			cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
		while (terminal_next_entry(inventory, &entry, &diag) == CLUSTER_RECOVERY_TERMINAL_ENTRY) {}
		UT_ASSERT_EQ(diag.terminal_records, 12);
		while (cluster_recovery_native_inventory_next(inventory, &effect)
			   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT)
			effects++;
		UT_ASSERT(effects > 20);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(terminal_next_entry(inventory, &entry, &diag),
					 CLUSTER_RECOVERY_TERMINAL_DRAINED);
		if (fault == 0) {
			inventory_fault = 0;
			inventory_drift();
		} else if (fault == 1)
			inputs_pin_current = false;
		else {
			inventory_census_calls = 0;
			inventory_fault = 4;
			inventory_between_passes = inventory_drift;
		}
		PG_TRY();
		{
			UT_ASSERT(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag)
					  != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		memset(&entry, 0xaa, sizeof(entry));
		UT_ASSERT(terminal_next_entry(inventory, &entry, &diag)
				  > CLUSTER_RECOVERY_TERMINAL_DRAINED);
		UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
		inputs_pin_current = true;
		inventory_between_passes = NULL;
		UT_ASSERT(terminal_next_entry(inventory, &entry, &diag)
				  > CLUSTER_RECOVERY_TERMINAL_DRAINED);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_terminal_binding_search_is_bounded)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag = { 0 };
	ClusterRecoveryTerminalEntry entry;
	ClusterRecoveryNativeEffect effect;
	ClusterWalInputsV1 *inputs;
	NativeInventoryRecord *binding, *after;
	unsigned entries = 0, calls = 0;
	inventory_extra = 16;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	binding = inventory->head->next;
	after = binding->next;
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(binding->scan, &effect),
				 CLUSTER_RECOVERY_NATIVE_EFFECT);
	/* Scheduling-only repeated saved records. Not a physical retained-cut proof. */
	for (unsigned i = 0; i < 260; i++) {
		NativeInventoryRecord *copy = calloc(1, sizeof(*copy));
		DecodedXLogRecord decoded = { 0 };
		XLogReaderState reader = { 0 };
		decoded.header = effect.record->header;
		decoded.lsn = effect.record->read_lsn;
		decoded.next_lsn = effect.record->end_lsn;
		decoded.main_data = (char *)effect.record->payload;
		decoded.main_data_len = effect.record->payload_length;
		decoded.max_block_id = -1;
		reader.record = &decoded;
		reader.ReadRecPtr = effect.record->read_lsn;
		reader.EndRecPtr = effect.record->end_lsn;
		reader.system_identifier = effect.record->source.claim.identity.system_identifier;
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &effect.record->source,
														&effect.record->cut, 65536, &copy->scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		copy->source_index = binding->source_index;
		binding->next = copy;
		binding = copy;
	}
	binding->next = after;
	for (; calls < 10000; calls++) {
		uint64 prior = diag.terminal_binding_examined;
		ClusterRecoveryTerminalResult r
			= cluster_recovery_native_inventory_terminal_step(inventory, &entry, &diag);
		UT_ASSERT(diag.terminal_binding_examined - prior <= 128);
		if (r == CLUSTER_RECOVERY_TERMINAL_ENTRY)
			entries++;
		else if (r == CLUSTER_RECOVERY_TERMINAL_DRAINED)
			break;
		else {
			UT_ASSERT_EQ(r, CLUSTER_RECOVERY_TERMINAL_MORE);
			if (r != CLUSTER_RECOVERY_TERMINAL_MORE)
				break;
		}
	}
	UT_ASSERT_EQ(entries, 12);
	UT_ASSERT(calls > 15 && calls < 10000);
	UT_ASSERT(diag.terminal_binding_examined > 520);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

static void
terminal_physical_slots(int fd, uint8 status)
{
	TTSlot slot = { 0 };
	slot.xid = 4195136;
	slot.wrap = 1;
	slot.status = status;
	slot.commit_scn = status == TT_SLOT_COMMITTED ? 5906 : InvalidScn;
	for (unsigned i = 0; i < 2; i++) {
		const char *path
			= i ? "pg_undo/instance_127/seg_32513.dat" : "pg_undo/instance_0/seg_1.dat";
		int file = openat(fd, path, O_WRONLY);
		UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(pwrite(file, &slot, sizeof(slot), offsetof(UndoSegmentHeaderData, tt_slots)),
					 sizeof(slot));
		UT_ASSERT_EQ(close(file), 0);
	}
}

UT_TEST(test_terminal_census_compares_original_physical_slots)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusEntry entry;
		ClusterRecoveryCensusDiag diag;
		ClusterRecoveryCensusResult r = CLUSTER_RECOVERY_CENSUS_MORE;
		unsigned entries = 0, present = 0, identities = 0, matches = 0;
		owned_terminals = true;
		census = owned_census_fixture(&inputs, &fd, path, false);
		if (mode)
			terminal_physical_slots(fd, mode == 1	? TT_SLOT_ACTIVE
										: mode == 2 ? TT_SLOT_COMMITTED
													: TT_SLOT_ABORTED);
		for (unsigned step = 0; step < 200000; step++) {
			r = cluster_recovery_census_next(census, &entry);
			if (r >= CLUSTER_RECOVERY_CENSUS_DRAINED)
				break;
			if (r != CLUSTER_RECOVERY_CENSUS_ENTRY
				|| entry.kind != CLUSTER_RECOVERY_CENSUS_TERMINAL)
				continue;
			entries++;
			present += entry.value.terminal.physical_present;
			identities += entry.value.terminal.identity_matches;
			matches += entry.value.terminal.raw_terminal_matches;
			if (!terminal_key_known(entry.value.terminal.terminal.identity)) {
				UT_ASSERT(!entry.value.terminal.physical_present);
				UT_ASSERT(!entry.value.terminal.identity_matches);
				UT_ASSERT(!entry.value.terminal.raw_terminal_matches);
			}
		}
		UT_ASSERT_EQ(r, CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT_EQ(entries, 12);
		UT_ASSERT_EQ(present, 8);
		UT_ASSERT_EQ(identities, mode == 0 ? 0 : 8);
		UT_ASSERT_EQ(matches, mode >= 2 ? 4 : 0);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.terminal_entries, 12);
		UT_ASSERT_EQ(diag.terminal_identity_mismatches, mode == 0 ? 8 : 0);
		UT_ASSERT_EQ(diag.terminal_state_mismatches, mode == 1 ? 8 : mode >= 2 ? 4 : 0);
		owned_census_done(census, &inputs, fd, path);
	}
	owned_terminals = false;
}

UT_TEST(test_terminal_census_final_changes_revoke_output)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census;
		ClusterRecoveryCensusDiag diag;
		ClusterRecoveryCensusManifest manifest;
		owned_terminals = true;
		census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.terminal_entries, 12);
		if (fault == 0)
			terminal_physical_slots(fd, TT_SLOT_COMMITTED);
		else if (fault == 1) {
			inventory_fault = 0;
			inventory_drift();
		} else
			inputs_pin_current = false;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		memset(&manifest, 0xff, sizeof(manifest));
		UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
		UT_ASSERT(manifest_zero(&manifest));
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
	owned_terminals = false;
}

UT_TEST(test_terminal_bad_exact_fields_refuse_and_revoke)
{
	for (unsigned mode = 6; mode <= 15; mode++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryTerminalEntry entry, zero = { 0 };
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventoryResult built;
		ClusterRecoveryTerminalResult result = CLUSTER_RECOVERY_TERMINAL_MORE;
		inventory_extra = 16;
		inventory_terminal_mode = mode;
		inputs = inventory_fixture(false, false);
		built = cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag);
		if (built == CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED) {
			while ((result = terminal_next_entry(inventory, &entry, &diag))
				   == CLUSTER_RECOVERY_TERMINAL_ENTRY)
				UT_ASSERT_EQ(entry.identity, CLUSTER_RECOVERY_TERMINAL_NATIVE_ONLY);
			UT_ASSERT(result > CLUSTER_RECOVERY_TERMINAL_DRAINED);
			UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
			UT_ASSERT(cluster_recovery_native_inventory_next(inventory,
															 &(ClusterRecoveryNativeEffect){ 0 })
					  > CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		} else
			UT_ASSERT(inventory == NULL);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
	inventory_extra = inventory_terminal_mode = 0;
}
