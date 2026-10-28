/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_all_clog.inc
 *    Complete original all-origin CLOG input in the same native reread.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_all_clog.inc
 *
 * NOTES
 *    Original ROOT/WAL/counter/SIDE execute; native CLOG/Startup/GES leaves
 *    remain controlled. Input equality does not establish native lifetime.
 *-------------------------------------------------------------------------
 */
static unsigned episode_all_clog_foreign_count;
static bool episode_all_clog_foreign_full;

static WalTestRecord
episode_all_clog_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (source->claim.identity.origin_node_id == 0)
		return episode_birth_match_prefix(source, previous);
	for (unsigned i = 0; i < episode_all_clog_foreign_count; i++) {
		int page = 17 + i;
		if (i == 0 && episode_all_clog_foreign_full) {
			xl_clog_zeropage_full birth = {17, 0, (UINT64_C(3) << 32) | UINT64_C(557056)};
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
		} else if (episode_all_clog_foreign_count == 2 && i == 1) {
			xl_clog_truncate truncate = {16, 524291, 606};
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		} else
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
	return previous;
}

static void
episode_all_clog_begin(unsigned count, bool full, bool abort, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	episode_base_reset(); episode_birth_match_mode = 0;
	episode_all_clog_foreign_count = count; episode_all_clog_foreign_full = full;
	episode_pending_prefix_hook = episode_all_clog_prefix;
	episode_clog_side_terminal_begin(abort ? EPISODE_TERMINAL_ABORT : EPISODE_TERMINAL_COMMIT,
		inputs, history, pending);
	episode_pending_prefix_hook = NULL;
	episode_terminal_status(abort ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; i++)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
	episode_clog_side_fault = 0;
	episode_clog_side_plans(*inputs);
}

UT_TEST(test_episode_all_clog_retains_every_source_without_changing_query_input)
{
	for (unsigned abort = 0; abort < 2; abort++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 query;
		ClusterRecoveryRelmapPendingAllNativeV1 all;
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census, reads, seen[2] = {0};
		uint64 previous[2] = {0};

		episode_all_clog_begin(2, false, abort, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &query, &diag));
		census = inventory_census_calls; reads = episode_base_calls;
		UT_ASSERT(cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census + 1); UT_ASSERT_EQ(episode_base_calls, reads + 2);
		UT_ASSERT_EQ(memcmp(&query, &all.query, sizeof(query)), 0);
		UT_ASSERT_EQ(query.birth.inputs.clog_count, 1); UT_ASSERT_EQ(all.clog_count, 3);
		for (uint32 i = 0; i < all.clog_count; i++) {
			const ClusterRecoveryRelmapClogEventV1 *event = &all.clog[i];
			uint32 index = event->source_index;
			UT_ASSERT(index < all.query.birth.inputs.generation_count);
			if (index >= all.query.birth.inputs.generation_count || index >= 2) continue;
			UT_ASSERT(event->record_ordinal > previous[index]); previous[index] = event->record_ordinal;
			UT_ASSERT(event->read_lsn < event->end_lsn); seen[index]++;
			if (all.query.birth.inputs.generations[index].observation.source.claim.identity.origin_node_id == 0)
				UT_ASSERT_EQ(memcmp(event, &query.birth.inputs.clog[0], sizeof(*event)), 0);
			else if (event->kind == CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE) {
				UT_ASSERT_EQ(event->oldest_xact, 524291); UT_ASSERT_EQ(event->oldest_database, 606);
				UT_ASSERT(!event->full_birth_observed);
			} else
				UT_ASSERT(!event->full_birth_observed && event->page == 17);
		}
		UT_ASSERT_EQ(seen[0] + seen[1], 3); UT_ASSERT(seen[0] != 0 && seen[1] != 0);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls | relmap_failed, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_all_clog_keeps_foreign_full_birth_and_its_own_counter)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingAllNativeV1 all;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned foreign_full = 0;

	episode_all_clog_begin(2, true, false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, &diag));
	UT_ASSERT_EQ(all.clog_count, 3);
	for (uint32 i = 0; i < all.clog_count; i++) {
		const ClusterRecoveryRelmapClogEventV1 *event = &all.clog[i];
		const ClusterRecoveryRelmapXidGenerationV1 *generation = &all.query.birth.inputs.generations[event->source_index];
		if (generation->observation.source.claim.identity.origin_node_id != 127 || !event->full_birth_observed)
			continue;
		foreign_full++;
		UT_ASSERT_EQ(event->full_birth.pageno, 17); UT_ASSERT_EQ(event->full_birth.reserved_zero, 0);
		UT_ASSERT_EQ(event->full_birth.first_full_xid, (UINT64_C(3) << 32) | UINT64_C(557056));
		UT_ASSERT(event->counter_observed && generation->counter_observed);
		UT_ASSERT(event->read_lsn >= generation->native_redo);
		UT_ASSERT(event->counter_before >= generation->checkpoint_next);
		UT_ASSERT(event->counter_after >= event->counter_before && event->counter_after <= generation->tail_next);
	}
	UT_ASSERT_EQ(foreign_full, 1);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_all_clog_capacity_is_for_whole_set_not_query_origin)
{
	for (unsigned count = 127; count <= 128; count++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 query;
		ClusterRecoveryRelmapPendingAllNativeV1 all, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		bool observed;
		unsigned census, reads;

		episode_all_clog_begin(count, false, false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &query, &diag));
		held = history->native_base; census = inventory_census_calls; reads = episode_base_calls;
		observed = cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, &diag);
		UT_ASSERT_EQ(observed, count == 127);
		if (observed) {
			UT_ASSERT_EQ(all.clog_count, 128); UT_ASSERT_EQ(all.query.birth.inputs.clog_count, 1);
		} else {
			UT_ASSERT_EQ(memcmp(&all, &zero, sizeof(all)), 0);
			UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base == held);
			UT_ASSERT_EQ(inventory_census_calls, census); UT_ASSERT_EQ(episode_base_calls, reads);
		}
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_all_clog_whole_output_and_late_error_preserve_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingNativeV1 query;
	ClusterRecoveryRelmapPendingAllNativeV1 all, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;
	unsigned census, reads;

	episode_all_clog_begin(2, true, false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &query, &diag));
	held = history->native_base; saved = pending; census = inventory_census_calls; reads = episode_base_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, (void *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(saved)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, (void *)history, &diag));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, (void *)&all.clog[0]));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, &diag));
	CritSectionCount = 0;
	UT_ASSERT_EQ(inventory_census_calls, census); UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT(!history->failed && history->native_base == held);
	inventory_census_calls = 0; inventory_between_passes = episode_terminal_recheck_error;
	test_capture_error_level = true;
	PG_TRY(); { (void)cluster_recovery_relmap_history_pending_all_native_v1(history, &pending, &all, &diag); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false; inventory_between_passes = NULL;
	UT_ASSERT(caught && history->failed && relmap_failed != 0 && history->native_base == held);
	UT_ASSERT_EQ(memcmp(&all, &zero, sizeof(all)), 0);
	UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
	episode_terminal_done(&history, &inputs);
}
