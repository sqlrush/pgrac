/* Online WAL observations retain the original physical sources and cuts.
 * Native file/TT closure and recovery authority are separate obligations.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static ClusterWalInputsV1 *
online_inventory_fixture(bool unsupported, bool open_source)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(unsupported, open_source);

	cold_census_done(&inputs);
	UT_ASSERT_EQ(inputs_begin_for_role(false, &inputs), 0);
	return inputs;
}

UT_TEST(test_online_inventory_reads_complete_native_wal_twice)
{
	for (unsigned open = 0; open < 2; open++) {
		ClusterWalInputsV1 *inputs = online_inventory_fixture(false, open);
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		unsigned effects = 0, terminals = 0, messages = 0, creates = 0;
		ClusterRecoveryNativeInventoryResult result;

		if (open)
			inputs_peer_confirm(cluster_wal_inputs_at_v1(inputs, 1));
		UT_ASSERT_EQ(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
				&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(inventory_census_calls, 2);
		UT_ASSERT_EQ(diag.sources, 2);
		UT_ASSERT_EQ(diag.records, 6);
		UT_ASSERT_EQ(diag.native_records, 4);
		UT_ASSERT_EQ(diag.other_records, 2);
		if (inventory != NULL) {
			while ((result = cluster_recovery_native_inventory_next(inventory, &effect))
				   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
				effects++;
				UT_ASSERT_EQ(effect.record->source.claim.identity.origin_thread_id,
							 effect.record->cut.failed_thread);
				UT_ASSERT(effect.record->read_lsn >= effect.record->cut.scan_begin_inclusive);
				UT_ASSERT(effect.record->end_lsn <= effect.record->cut.scan_end_exclusive);
				if (effect.kind == CLUSTER_RECOVERY_NATIVE_TERMINAL) {
					terminals++;
					UT_ASSERT_EQ(effect.record->xact_time, 8765);
					UT_ASSERT_EQ(effect.record->terminal_scn, 5906);
				} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_INVALIDATION)
					messages++;
				else if (effect.kind == CLUSTER_RECOVERY_NATIVE_CREATE)
					creates++;
			}
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
			UT_ASSERT_EQ(effects, 8);
			UT_ASSERT_EQ(terminals, 2);
			UT_ASSERT_EQ(messages, 4);
			UT_ASSERT_EQ(creates, 2);
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck_online(inputs, inventory,
					&diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					CLUSTER_RECOVERY_NATIVE_INVENTORY_END);
		}
		UT_ASSERT_EQ(inventory_census_calls, 3);
		UT_ASSERT_EQ(inputs_peer_begins, open);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_online_inventory_remote_wait_has_no_partial_inventory)
{
	ClusterWalInputsV1 *inputs = online_inventory_fixture(false, true);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;

	UT_ASSERT_EQ(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
			&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT);
	UT_ASSERT_EQ(diag.input_result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(diag.records, 0);
	UT_ASSERT(inventory == NULL);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT(inputs_pin_held);
	inputs_peer_confirm(cluster_wal_inputs_at_v1(inputs, 1));
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
			&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(diag.records, 6);
	UT_ASSERT_EQ(inputs_peer_begins, 1);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	UT_ASSERT_EQ(inputs_peer_releases, 1);
}

UT_TEST(test_online_inventory_drift_io_and_error_never_publish)
{
	for (inventory_fault = 0; inventory_fault < 8; inventory_fault++) {
		ClusterWalInputsV1 *inputs;
		struct OnlineInventoryFailure {
			ClusterRecoveryNativeInventory *inventory;
			ClusterRecoveryNativeInventoryDiag diag;
		} *state = palloc0(sizeof(*state));
		ClusterRecoveryNativeEffect effect;
		volatile bool threw = false;

		inventory_extra = inventory_fault == 6 ? 1 : inventory_fault == 7 ? 2 : 0;
		inputs = online_inventory_fixture(false, false);
		inventory_between_passes = inventory_drift;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
					&state->inventory, &state->diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, inventory_fault == 4);
		UT_ASSERT(state->inventory == NULL);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(state->inventory, &effect),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
		UT_ASSERT(inputs_pin_held);
		inventory_between_passes = NULL;
		inputs_pin_current = true;
		cold_census_done(&inputs);
		pfree(state);
	}
	inventory_extra = 0;
}

UT_TEST(test_online_inventory_recheck_preserves_cursor_and_fixed_end)
{
	ClusterWalInputsV1 *inputs = online_inventory_fixture(false, true);
	const ClusterWalInputV1 *peer = cluster_wal_inputs_at_v1(inputs, 1);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect first, second;
	InputsVisitTest direct = { 0 };
	ClusterWalTailObservation physical;
	XLogRecPtr accepted, appended;

	inputs_peer_confirm(peer);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
			&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	if (inventory == NULL)
		goto cleanup;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &first),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(first.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	accepted = inventory->sources[1].cut.scan_end_exclusive;
	appended = online_census_append_noop(peer, accepted);
	UT_ASSERT(appended > accepted);
	UT_ASSERT_EQ(cluster_wal_flushed_prefix_visit(cluster_wal_threads_dir,
			&peer->source, wal_segment_size, peer->checkpoint.checkpoint_lower_lsn,
			accepted, appended, appended, peer->checkpoint_start,
			peer->checkpoint.checkpoint_record_crc32c, inputs_visit_record,
			&direct, &physical), 0);
	UT_ASSERT_EQ(physical.records, 4);
	UT_ASSERT_EQ(physical.complete_end, appended);
	inputs_peer_flush.complete_end = inputs_peer_flush.flushed_end = appended;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck_online(inputs, inventory,
			&diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory->sources[1].cut.scan_end_exclusive, accepted);
	UT_ASSERT_EQ(diag.records, 6);
	UT_ASSERT_EQ(inputs_peer_begins, 1);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &second),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(second.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
	UT_ASSERT(second.record == first.record);
	inventory_fault = 0;
	inventory_drift();
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck_online(inputs, inventory,
			&diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &second),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
cleanup:
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

UT_TEST(test_online_inventory_keeps_cold_owner_and_budget_guards)
{
	size_t required = 0;

	for (unsigned mode = 0; mode < 7; mode++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ResourceOwner owner = CurrentResourceOwner;
		size_t budget = mode == 1 ? required - 1 : mode == 2 ? required : 1024 * 1024;
		ClusterRecoveryNativeInventoryResult result;

		inputs = mode == 3 ? inventory_fixture(false, false)
			: online_inventory_fixture(mode == 6, false);
		if (mode == 4)
			CurrentResourceOwner = (ResourceOwner)&diag;
		result = cluster_recovery_native_inventory_build_online(inputs, budget, &inventory, &diag);
		CurrentResourceOwner = owner;
		if (mode < 3) {
			UT_ASSERT_EQ(result, mode == 1 ? CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
						: CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			if (mode == 0) {
				required = diag.owned_bytes;
				UT_ASSERT(required > 0 && required < budget);
			} else if (mode == 2)
				UT_ASSERT_EQ(diag.owned_bytes, required);
		} else if (mode == 5) {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			cluster_recovery_native_inventory_destroy(inventory);
			inventory = NULL;
			/* The old entry still refuses an original online scope. */
			UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget, &inventory,
					&diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT);
			UT_ASSERT_EQ(diag.input_result, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		} else {
			UT_ASSERT_NE(result, CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
			if (mode == 3)
				UT_ASSERT_EQ(diag.input_result, CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			if (mode == 6)
				UT_ASSERT_EQ(diag.native_result, CLUSTER_RECOVERY_NATIVE_SHAPE);
		}
		if (mode == 1 || mode >= 3)
			UT_ASSERT(inventory == NULL);
		cluster_recovery_native_inventory_destroy(inventory);
		cold_census_done(&inputs);
		if (required == 0)
			break;
	}
}

UT_TEST(test_online_inventory_keeps_actual_empty_terminal_identity)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalStartupImage op;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;

	op = pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed)
		goto cleanup;
	test_worker_pin_held = false;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
			&control, &token), 0);
	/* The unrelated unfinished peer reservation remains outside this
	 * physical terminal fixture, as in the original cold inventory test. */
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(inputs_begin_for_role(false, &inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build_online(inputs, 1024 * 1024,
			&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(diag.sources, cluster_wal_inputs_count_v1(inputs));
	UT_ASSERT_EQ(inventory_census_calls, 2);
	if (inventory != NULL) {
		unsigned empty = 0;
		for (uint32 i = 0; i < diag.sources; i++) {
			const NativeInventorySource *source = &inventory->sources[i];
			if (source->cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) {
				empty++;
				UT_ASSERT_EQ(source->observed.records, 0);
				UT_ASSERT(cluster_control_root_identity_equal(&source->source.claim.identity,
						&op.claim.identity));
			}
		}
		UT_ASSERT_EQ(empty, 1);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck_online(inputs, inventory,
				&diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	}
cleanup:
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
	test_reserve_mode = false;
	cluster_node_id = saved_node;
	test_self_incarnation = saved_incarnation;
}
