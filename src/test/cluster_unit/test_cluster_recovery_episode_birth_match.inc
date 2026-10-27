/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_birth_match.inc
 *    Original native page-birth correspondence beside complete history.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_birth_match.inc
 *
 * NOTES
 *    Constructed native WAL runs through original ROOT/scanner/counter paths.
 *    Original Startup/GES/base leaves are controlled. No lifetime authority.
 *-------------------------------------------------------------------------
 */

static unsigned episode_birth_match_mode;

static WalTestRecord
episode_birth_match_record(const ClusterWalSourceRef *source, WalTestRecord previous,
	uint64 first, bool legacy)
{
	xl_clog_zeropage_full birth = {128, 0, first};
	if (legacy) {
		int page = 128;
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
	} else {
		birth.pageno = (uint32)first / CLUSTER_NATIVE_CLOG_PER_PAGE;
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
	}
	return episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
}

static WalTestRecord
episode_birth_match_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (source->claim.identity.origin_node_id != 0 || episode_birth_match_mode == 1
		|| episode_birth_match_mode == 8)
		return previous;
	if (episode_birth_match_mode == 2)
		return episode_birth_match_record(source, previous, 0, true);
	previous = episode_birth_match_record(source, previous,
		((episode_birth_match_mode == 3 ? UINT64_C(7) : UINT64_C(3)) << 32)
			| (episode_birth_match_mode == 4 ? UINT64_C(4227072) : UINT64_C(4194304)), false);
	if (episode_birth_match_mode == 5)
		previous = episode_birth_match_record(source, previous,
			(UINT64_C(3) << 32) | UINT64_C(4194304), false);
	return previous;
}

static WalTestRecord
episode_birth_match_tail(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (episode_birth_match_mode == 6)
		return episode_birth_match_record(source, previous, 0, true);
	if (episode_birth_match_mode == 7) {
		xl_clog_truncate truncate = {17, 557065, 606};
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		return episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
	if (episode_birth_match_mode == 8)
		return episode_birth_match_record(source, previous,
			(UINT64_C(3) << 32) | UINT64_C(4194304), false);
	return previous;
}

static void
episode_birth_match_begin(unsigned mode, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	episode_base_reset(); episode_birth_match_mode = mode;
	episode_pending_prefix_hook = episode_birth_match_prefix;
	episode_terminal_tail_hook = episode_birth_match_tail;
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, inputs, history, pending);
	episode_pending_prefix_hook = NULL; episode_terminal_tail_hook = NULL;
}

UT_TEST(test_episode_birth_match_retains_original_source_counter_and_complete_input)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBirthV1 birth;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, acquires;

	episode_birth_match_begin(0, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	census = inventory_census_calls; acquires = ctrc_set_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
	UT_ASSERT_EQ(birth.inputs.generation_count, 2);
	UT_ASSERT_EQ(birth.inputs.clog_count, 1);
	UT_ASSERT_EQ(birth.birth_index, 0);
	if (birth.inputs.clog_count == 1) {
		const ClusterRecoveryRelmapClogEventV1 *event = &birth.inputs.clog[0];
		UT_ASSERT(event->full_birth_observed && event->counter_observed);
		UT_ASSERT_EQ(event->full_birth.first_full_xid, (UINT64_C(3) << 32) | UINT64_C(4194304));
		UT_ASSERT_EQ(event->source_index, birth.inputs.history.observation.source_index);
		UT_ASSERT(event->end_lsn <= birth.inputs.map_start && event->record_ordinal < birth.inputs.map_ordinal);
		UT_ASSERT_EQ(event->counter_before, birth.inputs.checkpoint_next);
		UT_ASSERT_EQ(event->counter_after, birth.inputs.before_map);
	}
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(episode_base_calls | episode_base_releases | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && !history->failed);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_birth_match_refuses_absent_legacy_wrong_era_and_other_page)
{
	for (unsigned mode = 1; mode <= 4; mode++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 raw;
		ClusterRecoveryRelmapPendingBirthV1 birth, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census;

		episode_birth_match_begin(mode, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &raw, &diag));
		UT_ASSERT(!history->failed); /* raw observation remains available */
		UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
		UT_ASSERT_EQ(memcmp(&birth, &zero, sizeof(birth)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0);
		census = inventory_census_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census);
		UT_ASSERT_EQ(episode_base_calls | episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_match_rejects_duplicate_late_birth_reset_and_truncate)
{
	for (unsigned mode = 5; mode <= 8; mode++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 raw;
		ClusterRecoveryRelmapPendingBirthV1 birth, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;

		episode_birth_match_begin(mode, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &raw, &diag));
		UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
		UT_ASSERT_EQ(memcmp(&birth, &zero, sizeof(birth)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(episode_base_calls | episode_base_releases | ctrc_set_release_calls, 0);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_match_preconditions_preserve_original_query_and_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBirthV1 birth, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;

	episode_birth_match_begin(0, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	saved = pending; census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, (void *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, (void *)history, &diag));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
	CritSectionCount = 0;
	UT_ASSERT_EQ(memcmp(&birth, &zero, sizeof(birth)), 0);
	UT_ASSERT_EQ(inventory_census_calls, census);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT(cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag));
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_birth_match_owner_error_keeps_original_held_fd_and_pins)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryRelmapPendingBirthV1 birth, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;
	unsigned reads;

	episode_birth_match_begin(0, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base; reads = episode_base_calls;
	episode_clog_history_error_handle = history; episode_clog_history_entered_count = 0;
	ctrc_replay_current_override = episode_clog_history_current_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_birth_v1(history, &pending, &birth, &diag);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false; ctrc_replay_current_override = ctrc_set_original_current;
	episode_clog_history_error_handle = NULL;
	UT_ASSERT(caught && episode_clog_history_entered_count != 0);
	UT_ASSERT_EQ(memcmp(&birth, &zero, sizeof(birth)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->inventory->relmap_xid == NULL);
	UT_ASSERT(history->native_base == held && held != NULL && episode_base_live == 1);
	UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	episode_terminal_done(&history, &inputs);
}
