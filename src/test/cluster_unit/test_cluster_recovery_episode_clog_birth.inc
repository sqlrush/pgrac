/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_clog_birth.inc
 *    Complete native FULL CLOG birth inputs in the original episode.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_clog_birth.inc
 *
 * NOTES
 *    Original ROOT/WAL/scanner/counter paths run. Original Startup/GES/base
 *    leaves remain controlled. Birth inputs are not lifetime authority.
 *-------------------------------------------------------------------------
 */

static unsigned episode_clog_birth_mode, episode_clog_birth_count;

static WalTestRecord
episode_clog_birth_tail(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	for (unsigned i = 0; i < episode_clog_birth_count; i++) {
		xl_clog_zeropage_full birth = {128, 0, (UINT64_C(7) << 32) | UINT64_C(4194304)};
		if (episode_clog_birth_mode == 0 && i == 0) {
			int page = 128;
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
		} else if (episode_clog_birth_mode == 0 && i == 3) {
			xl_clog_truncate truncate = {17, 557065, 606};
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		} else {
			if (episode_clog_birth_mode == 1 && i == 0) {
				birth.pageno = 0;
				birth.first_full_xid = (UINT64_C(7) << 32) | FirstNormalTransactionId;
			} else if (i == 2)
				birth.first_full_xid = (UINT64_C(8) << 32) | UINT64_C(4194304);
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
		}
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
	return previous;
}

static void
episode_clog_birth_begin(unsigned mode, unsigned count, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	episode_base_reset();
	episode_clog_birth_mode = mode; episode_clog_birth_count = count;
	episode_terminal_tail_hook = episode_clog_birth_tail;
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, inputs, history, pending);
	episode_terminal_tail_hook = NULL;
}

UT_TEST(test_episode_clog_birth_retains_full_native_image_beside_legacy_history)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;
	xl_clog_zeropage_full zero = {0};
	unsigned census, acquires;

	episode_clog_birth_begin(0, 4, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	census = inventory_census_calls; acquires = ctrc_set_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.clog_count, 4);
	if (xid.clog_count == 4) for (uint32 i = 0; i < xid.clog_count; i++) {
		const ClusterRecoveryRelmapClogEventV1 *event = &xid.clog[i];
		ClusterRecoveryRelmapHistoryEntryV1 source;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, event->source_index, &source));
		UT_ASSERT_EQ(source.source.selected.source.claim.identity.origin_node_id, pending.pending_owner.node);
		UT_ASSERT_EQ(event->source_index, xid.history.observation.source_index);
		UT_ASSERT(event->record_ordinal > xid.map_ordinal && event->read_lsn >= xid.map_end);
		UT_ASSERT(event->read_lsn < event->end_lsn && event->record_ordinal < xid.records);
		UT_ASSERT(event->counter_observed);
		UT_ASSERT_EQ(event->counter_before, xid.tail_next);
		UT_ASSERT_EQ(event->counter_after, xid.tail_next);
		UT_ASSERT_EQ(event->full_birth_observed, i == 1 || i == 2);
		if (i == 1 || i == 2) {
			UT_ASSERT_EQ(event->kind, CLUSTER_RECOVERY_NATIVE_CLOG_ZERO);
			UT_ASSERT_EQ(event->page, 128);
			UT_ASSERT_EQ(event->full_birth.pageno, 128);
			UT_ASSERT_EQ(event->full_birth.reserved_zero, 0);
			UT_ASSERT_EQ(event->full_birth.first_full_xid,
				((i == 1 ? UINT64_C(7) : UINT64_C(8)) << 32) | UINT64_C(4194304));
		} else
			UT_ASSERT_EQ(memcmp(&event->full_birth, &zero, sizeof(zero)), 0);
		if (i == 3) {
			UT_ASSERT_EQ(event->kind, CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE);
			UT_ASSERT_EQ(event->oldest_xact, 557065);
			UT_ASSERT_EQ(event->oldest_database, 606);
		}
		if (i != 0) UT_ASSERT(event->record_ordinal > xid.clog[i-1].record_ordinal);
	}
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_clog_birth_keeps_native_epochs_separate_from_counter_candidates)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;

	episode_clog_birth_begin(1, 3, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.clog_count, 3);
	if (xid.clog_count == 3) {
		UT_ASSERT_EQ(xid.clog[0].page, 0);
		UT_ASSERT_EQ(xid.clog[0].full_birth.first_full_xid, (UINT64_C(7) << 32) | UINT64_C(3));
		UT_ASSERT_EQ(xid.clog[1].page, xid.clog[2].page);
		UT_ASSERT_EQ(xid.clog[1].full_birth.first_full_xid, (UINT64_C(7) << 32) | UINT64_C(4194304));
		UT_ASSERT_EQ(xid.clog[2].full_birth.first_full_xid, (UINT64_C(8) << 32) | UINT64_C(4194304));
		UT_ASSERT_EQ(xid.tail_next >> 32, 3); /* original checkpoint counter, not either birth era */
		for (uint32 i = 0; i < xid.clog_count; i++) {
			UT_ASSERT(xid.clog[i].full_birth_observed && xid.clog[i].counter_observed);
			UT_ASSERT_EQ(xid.clog[i].counter_before, xid.tail_next);
			UT_ASSERT_EQ(xid.clog[i].counter_after, xid.tail_next);
		}
	}
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_clog_birth_capacity_never_publishes_a_partial_history)
{
	for (unsigned extra = 0; extra < 2; extra++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census;

		episode_clog_birth_begin(2, 128 + extra, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (extra == 0) {
			UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(xid.clog_count, 128);
			if (xid.clog_count == 128) for (uint32 i = 0; i < xid.clog_count; i++)
				UT_ASSERT(xid.clog[i].full_birth_observed);
		} else {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
			UT_ASSERT(history->failed && relmap_failed != 0);
			census = inventory_census_calls;
			UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(inventory_census_calls, census);
		}
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_clog_birth_late_error_keeps_original_held_fd_and_all_protections)
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
	unsigned reads, census;

	episode_clog_birth_begin(1, 3, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base; reads = episode_base_calls;
	episode_clog_history_error_handle = history; episode_clog_history_entered_count = 0;
	ctrc_replay_current_override = episode_clog_history_current_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false;
	ctrc_replay_current_override = ctrc_set_original_current;
	episode_clog_history_error_handle = NULL;
	UT_ASSERT(caught && episode_clog_history_entered_count != 0);
	UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->inventory->relmap_xid == NULL);
	UT_ASSERT(history->native_base == held && held != NULL && episode_base_live == 1);
	UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(inventory_census_calls, census);
	episode_terminal_done(&history, &inputs);
}
