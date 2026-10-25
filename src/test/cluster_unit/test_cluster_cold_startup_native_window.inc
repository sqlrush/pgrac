/* Actual Startup/plan and native-window codec; original ROOT selection,
 * retained-WAL scans and execution ownership remain controlled boundaries.
 * Transporting these bytes grants no native/base/history lifetime authority. */
static void
startup_window_encode(const ClusterWalSourceRef *source, const CheckPoint *checkpoint,
					  XLogRecPtr start, unsigned version, uint8 out[160])
{
	ClusterRelmapCheckpointWindow window = { 0 };

	memset(out, 0, 160);
	if (version == 0)
		return;
	window.system_identifier = source->claim.identity.system_identifier;
	window.database_incarnation = source->claim.database_incarnation;
	window.origin = source->claim.identity.origin_node_id;
	window.admitted_incarnation = source->claim.identity.origin_owner_incarnation;
	window.boot_incarnation = 0x1000 + window.admitted_incarnation;
	window.epoch = 7 + window.origin;
	window.allocation_next = U64FromFullTransactionId(checkpoint->nextXid);
	window.last_issued = window.allocation_next - 1;
	window.native_floor = window.last_issued - 20;
	if (version == 1)
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&window, checkpoint, start,
															 start + 0x10, out));
	else
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(
			&window, checkpoint, start, start + 0x10, window.last_issued, out));
}

static void
startup_window_fixture(unsigned version)
{
	fixture();
	history_generation(1, 8, 0x40, 0x80);
	history_generation(2, 9, 0x40, 0x80);
	history_generation(2, 10, 0x80, 0xc0);
	for (uint32 i = 1; i <= NROOTS; i++) {
		ClusterWalRecoveryCheckpointV1 selected;

		native_checkpoint[i].checkpoint.PrevTimeLineID = 1;
		UT_ASSERT_EQ(cluster_control_root_recovery_checkpoint_v1(&roots[i], &tokens[i], &selected),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		startup_window_encode(&selected.source, &selected.checkpoint, selected.checkpoint_start,
							  version, native_checkpoint[i].native_window);
	}
	for (uint32 i = 0; i < census_history_count; i++) {
		const ClusterColdCensusEntryV1 *entry = &census_history[i];
		CheckPoint checkpoint = native_checkpoint[entry->cut.thread_id].checkpoint;

		checkpoint.redo = entry->cut.physical_lower;
		startup_window_encode(&entry->source, &checkpoint, entry->cut.physical_lower, version,
							  history_native_window[i]);
	}
}

UT_TEST(test_checkpoint_windows_survive_every_original_generation)
{
	for (unsigned version = 0; version <= 2; version++) {
		ClusterColdTypedV1 *typed;
		uint8 saved[6][160];

		startup_window_fixture(version);
		for (uint32 i = 0; i < 6; i++)
			memcpy(saved[i],
				   i < 3 ? native_checkpoint[i + 1].native_window : history_native_window[i - 3],
				   160);
		typed = prepare(0x800);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_EQ(typed->participant_count, 6);
		UT_ASSERT_EQ(census_begins, census_releases);
		/* The getter must use the captured generation, not released storage
		 * or the current incarnation's window for an older generation. */
		memset(history_native_window, 0xa5, sizeof(history_native_window));
		for (uint32 i = 1; i <= NROOTS; i++)
			memset(native_checkpoint[i].native_window, 0xa5, 160);
		memset(census_inputs, 0xa5, sizeof(census_inputs));
		for (uint32 i = 0; i < 6; i++) {
			ClusterControlRootSnapshot root;
			ClusterWalRecoveryCheckpointV1 checkpoint;
			uint32 expected_thread[6] = { 1, 2, 3, 1, 2, 2 };
			uint64 expected_incarnation[6] = { 11, 12, 13, 8, 9, 10 };

			UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, i, &root, &checkpoint));
			UT_ASSERT_EQ(root.identity.origin_thread_id, expected_thread[i]);
			UT_ASSERT_EQ(root.identity.origin_owner_incarnation, expected_incarnation[i]);
			UT_ASSERT_EQ(memcmp(checkpoint.native_window, saved[i], 160), 0);
			UT_ASSERT_EQ(memcmp(&checkpoint.source, &typed->sources[i], sizeof(checkpoint.source)),
						 0);
		}
		cluster_cold_typed_destroy_v1(&typed);
		UT_ASSERT_EQ(contexts_alive, 0);
	}
}

UT_TEST(test_checkpoint_window_census_drift_stops_before_capture)
{
	for (uint32 thread = 1; thread <= NROOTS; thread++)
		for (unsigned fault = 10; fault <= 11; fault++) {
			ClusterColdTypedV1 *typed;

			startup_window_fixture(2);
			census_fault_index = thread;
			census_input_fault = fault;
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_PARTICIPANT_INVALID);
			UT_ASSERT_EQ(typed->scanned_records, 0);
			UT_ASSERT_EQ(tt_base_captures, 0);
			UT_ASSERT_EQ(side_creates, 0);
			cluster_cold_typed_destroy_v1(&typed);
			UT_ASSERT_EQ(census_begins, census_releases);
			UT_ASSERT_EQ(contexts_alive, 0);
		}
}

UT_TEST(test_checkpoint_window_scan_drift_never_seals_input)
{
	for (uint32 participant = 0; participant < 6; participant++)
		for (unsigned zero = 0; zero < 2; zero++) {
			ClusterColdTypedV1 *typed;
			ClusterControlRootSnapshot root;
			ClusterWalRecoveryCheckpointV1 checkpoint, empty = { 0 };

			startup_window_fixture(2);
			if (participant < 3)
				native_window_scan_thread = participant + 1;
			else
				native_window_scan_history = 11 + participant - 3;
			native_window_scan_zero = zero != 0;
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
			UT_ASSERT(!typed->census_root_captured);
			UT_ASSERT(!tt_base_captured);
			UT_ASSERT(!cluster_cold_typed_checkpoint_v1(typed, 0, &root, &checkpoint));
			UT_ASSERT_EQ(memcmp(&checkpoint, &empty, sizeof(checkpoint)), 0);
			for (uint32 i = 0; i < typed->participant_count; i++) {
				UT_ASSERT_NULL(typed->side_plans[i]);
				UT_ASSERT(!typed->history_xid[i].begun);
			}
			cluster_cold_typed_destroy_v1(&typed);
			UT_ASSERT_EQ(census_begins, census_releases);
			UT_ASSERT_EQ(contexts_alive, 0);
		}
}

UT_TEST(test_checkpoint_window_history_census_recheck_is_exact)
{
	for (uint32 history = 0; history < 3; history++)
		for (unsigned fault = 10; fault <= 11; fault++) {
			ClusterColdTypedV1 *typed;

			startup_window_fixture(2);
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
			census_fault_index = 11 + history;
			census_input_fault = fault;
			UT_ASSERT(!cluster_cold_typed_recheck_inputs_v1(typed));
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
			UT_ASSERT(!typed->census_root_captured);
			UT_ASSERT_EQ(serial_input_calls, 0);
			UT_ASSERT_EQ(census_begins, census_releases);
			cluster_cold_typed_destroy_v1(&typed);
			UT_ASSERT_EQ(contexts_alive, 0);
		}
}

UT_TEST(test_checkpoint_window_original_reselect_drift_is_sticky)
{
	for (uint32 participant = 0; participant < 6; participant++)
		for (unsigned zero = 0; zero < 2; zero++) {
			ClusterColdTypedV1 *typed;
			ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
			uint8 saved[160], *window;

			startup_window_fixture(2);
			typed = prepare(0x800);
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
			cluster_cold_replay_window_enter_v1();
			UT_ASSERT(cluster_cold_typed_sources_current_v1(typed, fence));
			typed->xid_receipt.plan = typed->effects_receipt.plan = typed->plan;
			typed->xid_next_after_redo = 999;
			window = participant < 3 ? native_checkpoint[participant + 1].native_window
									 : history_native_window[participant - 3];
			memcpy(saved, window, 160);
			if (zero)
				memset(window, 0, 160);
			else
				window[159] ^= 1;
			UT_ASSERT(!cluster_cold_typed_sources_current_v1(typed, fence));
			UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
			UT_ASSERT_NULL(typed->xid_receipt.plan);
			UT_ASSERT_NULL(typed->effects_receipt.plan);
			UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
			UT_ASSERT(!typed->census_root_captured);
			UT_ASSERT_EQ(source_sync_calls, 0);
			UT_ASSERT_EQ(history_sync_calls, 0);
			UT_ASSERT_EQ(serial_input_calls, 0);
			UT_ASSERT_EQ(census_begins, census_releases);
			memcpy(window, saved, 160);
			UT_ASSERT(!cluster_cold_typed_sources_current_v1(typed, fence));
			endpoint_destroy(&typed);
			UT_ASSERT_EQ(contexts_alive, 0);
		}
}
