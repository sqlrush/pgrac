/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_history.inc
 *     Original episode and physical native history input tests.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_history.inc
 *
 * NOTES
 *     Original ROOT, history, claim, anchor and native WAL readers execute.
 *     Startup and GES remain controlled fixture leaves. Observed metadata
 *     supplies no native era, lifetime or completion qualification.
 *
 *-------------------------------------------------------------------------
 */
UT_TEST(test_episode_history_selected_window_drift_invalidates_inventory)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalInputV1 *selected;

	inventory_extra = 0;
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024,
		&inventory, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	selected = (ClusterWalInputV1 *)cluster_wal_inputs_at_v1(inputs, 0);
	UT_ASSERT(selected != NULL);
	selected->native_window[64] ^= 1;
	UT_ASSERT_NE(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

static void
episode_history_begin(ClusterWalInputsV1 **inputs)
{
	relmap_participant_fixture(inputs, false);
	relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
}

static ClusterRecoveryNativeInventoryResult
episode_history_build(ClusterWalInputsV1 *inputs, size_t budget,
	ClusterRecoveryRelmapHistory **out, ClusterRecoveryNativeInventoryDiag *diag)
{
	ClusterRecoveryRelmapSourceV1 source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	return cluster_recovery_relmap_history_build_v1(&source, budget, out, diag);
}

static void
episode_history_done(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	cluster_recovery_relmap_history_destroy_v1(history);
	UT_ASSERT(*history == NULL);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	ctrc_set_finish(inputs);
	relmap_declared = false;
	inventory_between_passes = NULL;
}

UT_TEST(test_episode_history_uses_all_original_sources_and_actual_window)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapHistoryEntryV1 entry;
	unsigned calls, windows = 0;

	episode_history_begin(&inputs);
	calls = ctrc_set_calls;
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024,
		&history, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	UT_ASSERT_EQ(diag.sources, 2);
	for (uint32 i = 0; i < diag.sources; ++i) {
		const ClusterWalInputV1 *selected;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, i, &entry));
		UT_ASSERT(entry.source.scope == inputs);
		selected = cluster_wal_inputs_at_v1(inputs, entry.source.input_index);
		UT_ASSERT(selected != NULL);
		UT_ASSERT_EQ(memcmp(&entry.source.selected, selected, sizeof(*selected)), 0);
		UT_ASSERT(entry.checkpoint_observed);
		UT_ASSERT_EQ(entry.checkpoint_end, selected->checkpoint.validated_tail_lsn_exclusive);
		if (entry.window_observed) {
			windows++;
			UT_ASSERT_EQ(entry.source.selected.source.claim.identity.origin_node_id, 0);
			UT_ASSERT_EQ(entry.window.epoch, 17);
			UT_ASSERT_EQ(entry.window.boot_incarnation, 31);
			UT_ASSERT_EQ(entry.post_flush_issued, 95);
		} else {
			UT_ASSERT_EQ(entry.post_flush_issued, 0);
			UT_ASSERT_EQ(entry.source.selected.source.claim.identity.origin_node_id, 127);
		}
	}
	UT_ASSERT_EQ(windows, 1);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	UT_ASSERT_EQ(ctrc_set_calls, calls);
	UT_ASSERT_EQ(relmap_failed, 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_history_requires_declaration_and_complete_original_holders)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;

	relmap_participant_fixture(&inputs, false);
	inventory_census_calls = 0;
	UT_ASSERT_NE(episode_history_build(inputs, 1024 * 1024,
		&history, &diag), 0);
	relmap_declared = true;
	UT_ASSERT_NE(episode_history_build(inputs, 1024 * 1024,
		&history, &diag), 0);
	UT_ASSERT(history == NULL);
	UT_ASSERT_EQ(inventory_census_calls, 0);
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	test_actual_cf = ShareLock;
	UT_ASSERT_NE(episode_history_build(inputs, 1024 * 1024,
		&history, &diag), 0);
	test_actual_cf = NoLock;
	UT_ASSERT_EQ(inventory_census_calls, 0);
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024,
		&history, &diag), 0);
	episode_history_done(&history, &inputs);
}

static ClusterWalInputsV1 *episode_history_fault_inputs;
static void
episode_history_mutate_window(void)
{
	ClusterWalInputV1 *input = (ClusterWalInputV1 *)cluster_wal_inputs_at_v1(
		episode_history_fault_inputs, 0);
	UT_ASSERT(input != NULL);
	input->native_window[64] ^= 1;
}

UT_TEST(test_episode_history_refusal_revokes_episode_and_keeps_protection)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapHistoryEntryV1 entry, zero = {0};
		episode_history_begin(&inputs);
		episode_history_fault_inputs = inputs;
		if (phase == 0) inventory_between_passes = episode_history_mutate_window;
		if (phase == 0) {
			UT_ASSERT_NE(episode_history_build(inputs, 1024 * 1024,
				&history, &diag), 0);
			UT_ASSERT(history == NULL);
		} else {
			UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024,
				&history, &diag), 0);
			if (phase == 1) {
				relmap_sibling->release_uncertain = true;
				memset(&entry, 0xa5, sizeof(entry));
				UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
				UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
				relmap_sibling->release_uncertain = false;
			} else {
				episode_history_mutate_window();
				UT_ASSERT_NE(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
			}
			UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0, &entry));
		}
		UT_ASSERT(relmap_failed > 0);
		UT_ASSERT_EQ(ctrc_set_release_calls, 0);
		episode_history_done(&history, &inputs);
	}
}

static bool
episode_history_current_error(const struct ClusterRecoveryStartV1 *start pg_attribute_unused(),
	const struct ClusterColdCtrcWorkV1 *work pg_attribute_unused(), uint32 index pg_attribute_unused(),
	ClusterWalInputsV1 *scope pg_attribute_unused(),
	const ClusterRecoverySerialRequest *request pg_attribute_unused())
{
	ereport(ERROR, (errmsg("episode history current ERROR")));
	return false;
}

UT_TEST(test_episode_history_first_current_error_is_sticky)
{
	for (unsigned operation = 0; operation < 3; ++operation) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapHistoryEntryV1 entry;
		ClusterRecoveryRelmapSourceV1 original;
		volatile bool caught = false;
		episode_history_begin(&inputs);
		if (operation != 0)
			UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024,
				&history, &diag), 0);
		original = cluster_wal_inputs_relmap_source_v1(inputs, 0);
		ctrc_replay_current_override = episode_history_current_error;
		test_capture_error_level = true;
		PG_TRY();
		{
			if (operation == 0)
				(void)cluster_recovery_relmap_history_build_v1(&original, 1024 * 1024,
					&history, &diag);
			else if (operation == 1)
				(void)cluster_recovery_relmap_history_source_v1(history, 0, &entry);
			else
				(void)cluster_recovery_relmap_history_recheck_v1(history, &diag);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		test_capture_error_level = false;
		ctrc_replay_current_override = ctrc_set_original_current;
		UT_ASSERT(caught);
		UT_ASSERT(relmap_failed > 0);
		UT_ASSERT_EQ(ctrc_set_release_calls, 0);
		episode_history_done(&history, &inputs);
	}
}

static void
episode_history_lose_sibling(void)
{
	relmap_sibling->release_uncertain = true;
}

UT_TEST(test_episode_history_stops_before_second_pass_after_sibling_loss)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	episode_history_begin(&inputs);
	inventory_between_passes = episode_history_lose_sibling;
	UT_ASSERT_NE(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	UT_ASSERT(history == NULL);
	UT_ASSERT_EQ(inventory_census_calls, 1);
	UT_ASSERT(relmap_failed > 0);
	relmap_sibling->release_uncertain = false;
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_history_retains_history_and_current_generations_together)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapHistoryEntryV1 entry;
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;
	unsigned current = 0, historical = 0, calls;
	bool selected[CLUSTER_WAL_INPUTS_MAX] = {false};

	ctrc_local_fixture(false);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	/* The INSTALL fixture leaves its original replay predecessors CLOSED.
	 * Supply their completed-replay metadata before this episode acquires IR;
	 * retain the actual claims, checkpoint bytes, cuts and original census. */
	for (unsigned node = 0; node <= 3; node += 3) {
		ClusterWalHistoryImage retained;
		ClusterWalHistoryStage stage;
		uint8 uuid[16] = {0x73};
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, node, &retained), 0);
		UT_ASSERT_EQ(retained.count, 1);
		tx_ir_complete(&retained.records[0].snapshot);
		put_u64_le(uuid + 8, ++root.header.file_txn_seq);
		test_actual_cf = test_cf_mode = ExclusiveLock;
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, node, &retained,
			root.header.file_txn_seq, uuid, &stage), 0);
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
		root.refs[node].history_generation = stage.generation;
		memcpy(root.refs[node].history_sha256, stage.sha256, sizeof(stage.sha256));
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
		test_actual_cf = test_cf_mode = ShareLock;
	}
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	ctrc_ir_original_scope = inputs;
	ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = ctrc_set_preflight_retire = 0;
	ctrc_set_after_grant = NULL;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = ctrc_set_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_declared = true;
	relmap_failed = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	calls = ctrc_set_calls;
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	UT_ASSERT_EQ(diag.sources, 4);
	UT_ASSERT_EQ(inventory_census_calls, 2);
	for (uint32 i = 0; i < diag.sources; ++i) {
		const ClusterWalInputV1 *original;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, i, &entry));
		UT_ASSERT(entry.source.input_index < diag.sources);
		UT_ASSERT(!selected[entry.source.input_index]);
		selected[entry.source.input_index] = true;
		original = cluster_wal_inputs_at_v1(inputs, entry.source.input_index);
		UT_ASSERT_EQ(memcmp(&entry.source.selected, original, sizeof(*original)), 0);
		if (entry.source.selected.current) current++; else historical++;
		UT_ASSERT(entry.checkpoint_observed);
	}
	UT_ASSERT_EQ(current, 2);
	UT_ASSERT_EQ(historical, 2);
	UT_ASSERT_EQ(ctrc_set_calls, calls);
	UT_ASSERT_EQ(ctrc_set_held, 2);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	UT_ASSERT_EQ(relmap_failed, 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_history_budget_index_and_alias_refusals_do_not_rebase)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapHistoryEntryV1 entry, zero = {0};
	episode_history_begin(&inputs);
	UT_ASSERT_EQ(episode_history_build(inputs, 0, &history, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY);
	UT_ASSERT_EQ(inventory_census_calls, 0);
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(episode_history_build(inputs, 1024 * 1024, &history, &diag), 0);
	memset(&entry, 0xa5, sizeof(entry));
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, diag.sources, &entry));
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_source_v1(history, 0,
		(ClusterRecoveryRelmapHistoryEntryV1 *)history));
	UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, 0, &entry));
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT(diag.owned_bytes <= 1024 * 1024);
	episode_history_done(&history, &inputs);
}
