/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_birth_terminal.inc
 *    Same physical reread for original birth, CLOG and SIDE inputs.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_birth_terminal.inc
 *
 * NOTES
 *    Original ROOT/WAL/scanner/counter/sealed SIDE execute; Startup/GES and
 *    the native CLOG leaf remain controlled. No lifetime authority.
 *-------------------------------------------------------------------------
 */
static void
episode_birth_terminal_begin(unsigned birth_mode, EpisodeTerminalFixture terminal_mode,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	episode_base_reset(); episode_birth_match_mode = birth_mode;
	episode_pending_prefix_hook = episode_birth_match_prefix;
	episode_terminal_begin(terminal_mode, inputs, history, pending);
	episode_pending_prefix_hook = NULL;
	episode_terminal_status(terminal_mode == EPISODE_TERMINAL_ABORT
		? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
}

UT_TEST(test_episode_birth_terminal_joins_source_counter_and_both_polarities_in_one_pass)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native;
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census, reads, acquires;

		episode_birth_terminal_begin(0, mode ? EPISODE_TERMINAL_ABORT : EPISODE_TERMINAL_COMMIT,
			&inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		census = inventory_census_calls; reads = episode_base_calls; acquires = ctrc_set_calls;
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census + 1);
		UT_ASSERT_EQ(episode_base_calls, reads + 2);
		UT_ASSERT_EQ(native.birth.inputs.generation_count, 2);
		UT_ASSERT_EQ(native.birth.inputs.clog_count, 1);
		UT_ASSERT_EQ(native.birth.birth_index, 0);
		UT_ASSERT_EQ(native.birth.inputs.clog[0].full_birth.first_full_xid,
			(UINT64_C(3) << 32) | UINT64_C(4194304));
		UT_ASSERT_EQ(memcmp(&native.birth.inputs.history, &native.terminal.base.history,
			sizeof(native.birth.inputs.history)), 0);
		UT_ASSERT_EQ(native.terminal.base.native.queried_full_xid, pending.pending_owner.top_full_xid);
		UT_ASSERT_EQ(native.terminal.base.native.status,
			mode ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT(native.terminal.terminal_start >= native.birth.inputs.map_end);
		UT_ASSERT(native.terminal.terminal_ordinal > native.birth.inputs.map_ordinal);
		UT_ASSERT_EQ(native.terminal.terminal_source_index, native.birth.inputs.history.observation.source_index);
		UT_ASSERT(history->native_base != NULL && episode_base_live == 1);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		UT_ASSERT_EQ(ctrc_set_calls, acquires);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls | relmap_failed, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && !history->failed);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_terminal_refuses_old_terminal_without_exact_birth)
{
	const unsigned modes[] = {1, 3, 5};
	for (unsigned i = 0; i < lengthof(modes); i++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingTerminalV1 terminal;
		ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		unsigned census;

		episode_birth_terminal_begin(modes[i], EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
		held = history->native_base;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base == held);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		census = inventory_census_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_terminal_keeps_original_side_and_native_status_refusals)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;

		episode_birth_terminal_begin(0, mode ? EPISODE_TERMINAL_COMMIT : EPISODE_TERMINAL_SIDE_MISMATCH,
			&inputs, &history, &pending);
		if (history == NULL) return;
		if (mode) episode_terminal_status(TRANSACTION_STATUS_ABORTED);
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base != NULL);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_terminal_whole_output_and_owner_error_keep_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;
	unsigned census, reads;

	episode_birth_terminal_begin(0, EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base; saved = pending; census = inventory_census_calls; reads = episode_base_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, (void *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, (void *)history, &diag));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, (void *)&native.terminal));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
	CritSectionCount = 0;
	UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
	UT_ASSERT_EQ(inventory_census_calls, census);
	UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT(!history->failed && relmap_failed == 0 && history->native_base == held);
	episode_clog_history_error_handle = history; episode_clog_history_entered_count = 0;
	ctrc_replay_current_override = episode_clog_history_current_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false; ctrc_replay_current_override = ctrc_set_original_current;
	episode_clog_history_error_handle = NULL;
	UT_ASSERT(caught && episode_clog_history_entered_count != 0);
	UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base == held);
	UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
	episode_terminal_done(&history, &inputs);
}
