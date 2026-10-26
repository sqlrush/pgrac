/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_generation_xid.inc
 *     Whole original generation-set native counter inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_generation_xid.inc
 * NOTES
 *     Real ROOT/anchor/claim/WAL and cold decoder/counter, with the original
 *     controlled Startup/GES/base leaves. No era/lifetime/completion grant.
 *-------------------------------------------------------------------------
 */
/* Change the actual second physical source before the original episode and
 * immutable inventory are created. Its LSNs, seed and counter belong to that
 * source; they must not be evaluated with the pending map's source instead. */
static void
episode_generation_begin(unsigned mode, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	ClusterWalInputsV1 *raw = relmap_history_fixture(3, 2, true);
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalSourceRef source = {0};
	WalTestRecord tail;
	char map[TEST_RELMAP_LENGTH];

	cluster_wal_inputs_release_v1(&raw);
	test_inputs_startup_cf = false;
	MyBackendType = B_BG_WORKER;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	source.claim.identity = root.records[127].identity;
	source.timeline = root.records[127].tail_tli;
	tail = (WalTestRecord){1, root.records[127].validated_tail_lsn_exclusive,
		root.records[127].tail_last_record_lsn, root.records[127].tail_last_record_crc32c};
	test_relmap_payload(map, false, 2);
	tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
		RM_RELMAP_ID, XLOG_RELMAP_UPDATE, map, sizeof(map));
	tail = episode_xid_frame(&source, tail,
		mode == 1 ? BootstrapTransactionId : mode == 2
			? UINT32_C(0x80000000) + 4196000 : 4199000, InvalidTransactionId);
	root.records[127].validated_tail_lsn_exclusive = tail.exclusive_end;
	root.records[127].tail_last_record_lsn = tail.record_start;
	root.records[127].tail_last_record_crc32c = tail.record_crc;
	tx_ir_complete(&root.records[0]);
	tx_ir_complete(&root.records[127]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &raw), 0);
	tx_inputs_capture(raw, NULL);
	cluster_wal_inputs_release_v1(&raw);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs;
	ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = ctrc_set_preflight_retire = 0;
	ctrc_set_after_grant = NULL;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = relmap_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_declared = true; relmap_failed = 0; relmap_sibling = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	inventory_census_calls = 0; inventory_between_passes = NULL;
	UT_ASSERT_EQ(episode_history_build(*inputs, 1024 * 1024, history, &diag), 0);
	UT_ASSERT(*history != NULL);
	if (*history != NULL) *pending = relmap_fold_pending((*history)->inventory, 0);
}

UT_TEST(test_episode_generation_xid_keeps_every_original_source_and_counter)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, acquires;

	episode_base_reset();
	episode_generation_begin(0, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	census = inventory_census_calls; acquires = ctrc_set_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.generation_count, 2);
	if (xid.generation_count == 2) for (uint32 i = 0; i < xid.generation_count; ++i) {
		const ClusterRecoveryRelmapXidGenerationV1 *generation = &xid.generations[i];
		ClusterRecoveryRelmapHistoryEntryV1 entry;
		bool other;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, i, &entry));
		other = entry.observation.source.claim.identity.origin_node_id == 127;
		UT_ASSERT_EQ(memcmp(&generation->observation, &entry.observation, sizeof(entry.observation)), 0);
		UT_ASSERT_EQ(generation->input_index, entry.source.input_index);
		UT_ASSERT_EQ(generation->native_redo, entry.source.selected.native_checkpoint.redo);
		UT_ASSERT(generation->counter_observed);
		UT_ASSERT_EQ(generation->checkpoint_next, ((uint64)3 << 32) | 4196000);
		UT_ASSERT_EQ(generation->tail_next, ((uint64)3 << 32) | (other ? 4199001 : 4196000));
		UT_ASSERT_EQ(generation->records, other ? 7 : 6);
		if (!other) {
			UT_ASSERT_EQ(generation->tail_next, xid.tail_next);
			UT_ASSERT_EQ(generation->records, xid.records);
		}
	}
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_generation_xid_sibling_frames_cannot_hide_behind_the_map)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;

	episode_base_reset();
	episode_generation_begin(1, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->inventory->relmap_xid == NULL);
	census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(inventory_census_calls, census);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_generation_xid_sibling_half_range_refuses_without_rebase)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;

	episode_base_reset();
	episode_generation_begin(2, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->inventory->relmap_xid == NULL);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_history_done(&history, &inputs);
}

static ClusterRecoveryRelmapHistory *episode_generation_error_history;
static bool
episode_generation_current_error(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	NativeInventoryRelmapXid *pass = episode_generation_error_history->inventory->relmap_xid;
	if (pass != NULL && pass->result != NULL && pass->result->generation_count == 2)
		ereport(ERROR, (errmsg("controlled original owner ERROR during generation counter fold")));
	return ctrc_set_original_current(start, work, index, scope, request);
}

UT_TEST(test_episode_generation_xid_late_error_retains_original_native_and_pins)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;

	episode_base_reset();
	episode_generation_begin(0, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base;
	episode_generation_error_history = history;
	ctrc_replay_current_override = episode_generation_current_error;
	test_capture_error_level = true;
	PG_TRY(); { (void)cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	test_capture_error_level = false;
	ctrc_replay_current_override = ctrc_set_original_current;
	UT_ASSERT(caught && history->failed && relmap_failed != 0);
	UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
	UT_ASSERT(history->inventory->relmap_xid == NULL && history->native_base == held);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_history_done(&history, &inputs);
}
