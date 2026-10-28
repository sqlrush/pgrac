/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_applied_pages.inc
 *    Original-episode historical apply input joined with held native pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_applied_pages.inc
 * NOTES
 *    Native files, complete source/frame matching and original held-page
 *    reads stay real. The opt-in join's applied-observation leaf is controlled
 *    only in the upper composition cases; it is not native execution proof.
 *-------------------------------------------------------------------------
 */
static unsigned episode_applied_mode, episode_applied_calls, episode_applied_foreign;

static bool
episode_applied_match(const ClusterRecoveryRelmapSourceV1 *source,
	const XLogReaderState *reader)
{
	episode_applied_calls++;
	if (episode_applied_mode == 0)
		return cluster_wal_inputs_relmap_clog_applied_v1(source, reader);
	/* Control only the historical applied observation. The original sealed
	 * plan must still match the complete source, native header and MAIN. */
	if (!cluster_wal_inputs_relmap_clog_matches_v1(source, reader))
		return false;
	if (source->selected.source.claim.identity.origin_node_id == 127) {
		episode_applied_foreign++;
		if (episode_applied_foreign == 2 && episode_applied_mode == 2)
			return false;
		if (episode_applied_foreign == 2 && episode_applied_mode == 3)
			ereport(ERROR, (errmsg("controlled foreign applied observation ERROR")));
	}
	return true;
}

static void
episode_applied_reset(unsigned mode)
{
	episode_applied_mode = mode;
	episode_applied_calls = episode_applied_foreign = 0;
}

UT_TEST(test_episode_applied_pages_fresh_carriers_cannot_supply_execution)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 result, zero = {0};
	ClusterRecoveryRelmapNativePageV1 page;
	ClusterRecoveryNativeInventoryDiag diag;
	int held = -1;

	episode_applied_reset(0);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(result.page_count, 3);
	UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, 0, &page));
	held = episode_native_pages_find_fd(page.native.inode); UT_ASSERT(held >= 0);
	memset(&result, 0xa5, sizeof(result));
	/* A freshly sealed plan has exact carriers but no production record
	 * execution. Real applied getter must refuse, despite held equal bytes. */
	UT_ASSERT(!cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
	UT_ASSERT(episode_applied_calls != 0 && history->failed && relmap_failed > 0);
	UT_ASSERT(fcntl(held, F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(held, F_GETFD), -1);
}

UT_TEST(test_episode_applied_pages_joins_all_sources_in_original_pass)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 result;
	ClusterRecoveryRelmapNativePageV1 page;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, reads;

	episode_applied_reset(1);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(result.inputs.clog_count, 3); UT_ASSERT_EQ(result.page_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT_EQ(episode_applied_calls, 3); UT_ASSERT_EQ(episode_applied_foreign, 2);
	reads = episode_native_clog_calls;
	for (uint32 i = 0; i < result.page_count; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		UT_ASSERT_EQ(page.birth.pageno, page.native.page_number);
		UT_ASSERT_EQ(memcmp(&page.history.observation,
			&result.inputs.query.birth.inputs.generations[page.history.observation.source_index].observation,
			sizeof(page.history.observation)), 0);
		UT_ASSERT(episode_native_pages_find_fd(page.native.inode) >= 0);
	}
	UT_ASSERT_EQ(episode_native_clog_calls, reads);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && relmap_failed == 0);
	episode_native_pages_end(&history, &inputs);
	episode_applied_reset(0);
}

UT_TEST(test_episode_applied_pages_foreign_failure_keeps_whole_custody)
{
	for (unsigned mode = 2; mode <= 3; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 result, zero = {0};
		ClusterRecoveryRelmapNativePageV1 page;
		ClusterRecoveryNativeInventoryDiag diag;
		int held[3] = {-1, -1, -1};
		unsigned reads, calls;
		volatile bool caught = false;

		episode_applied_reset(mode);
		episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
		for (uint32 i = 0; i < result.page_count && i < 3; ++i) {
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			held[i] = episode_native_pages_find_fd(page.native.inode); UT_ASSERT(held[i] >= 0);
		}
		memset(&result, 0xa5, sizeof(result));
		test_capture_error_level = mode == 3;
		PG_TRY(); {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending, &result, &diag));
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(caught, mode == 3);
		UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
		UT_ASSERT_EQ(episode_applied_calls, 3); UT_ASSERT_EQ(episode_applied_foreign, 2);
		UT_ASSERT(history->failed && relmap_failed > 0 && history->inventory->relmap_xid == NULL);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(held[i], F_GETFD) >= 0);
		reads = episode_native_clog_calls; calls = episode_applied_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending, &result, &diag));
		UT_ASSERT_EQ(episode_native_clog_calls, reads); UT_ASSERT_EQ(episode_applied_calls, calls);
		episode_native_pages_end(&history, &inputs);
		for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(held[i], F_GETFD), -1);
	}
	episode_applied_reset(0);
}

UT_TEST(test_episode_applied_pages_opt_out_and_alias_keep_original_inputs)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapPendingNativePagesV1 result;
	ClusterRecoveryNativeInventoryDiag diag;

	episode_applied_reset(2);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	saved = pending;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending,
		(ClusterRecoveryRelmapPendingNativePagesV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!history->failed && episode_applied_calls == 0 && episode_native_clog_calls == 0);
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(result.page_count, 3); UT_ASSERT_EQ(episode_applied_calls, 0);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	episode_applied_reset(1);
	UT_ASSERT(cluster_recovery_relmap_history_pending_applied_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(result.page_count, 3); UT_ASSERT_EQ(episode_applied_calls, 3);
	UT_ASSERT_EQ(episode_native_clog_calls, 12);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && relmap_failed == 0);
	episode_native_pages_end(&history, &inputs);
	episode_applied_reset(0);
}
