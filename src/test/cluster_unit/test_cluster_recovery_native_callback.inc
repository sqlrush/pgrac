/*-------------------------------------------------------------------------
 * test_cluster_recovery_native_callback.inc
 *     Complete native CLOG callback, original consumption and held pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_native_callback.inc
 * NOTES
 *     Native redo/sync/readback, original plans and held file reads execute.
 *     XACT outcome, foreign cold-applied, merge/touched/dispatch, authority
 *     and locks remain fixtures. No native lifetime or completion grant.
 *-------------------------------------------------------------------------
 */
static ClusterRecoveryRelmapHistory *native_callback_history;
static TestEpisodeClogReplayStats native_callback_stats;
static TransactionId native_callback_xid;
static unsigned native_callback_calls, native_callback_commits;
static unsigned native_callback_sync_fault, native_callback_late_fault;
static bool native_callback_commit, native_callback_consumed;

static void
native_callback_redo(XLogReaderState *reader, void *arg)
{
	ClusterRecoveryRelmapSourceV1 *source = arg;

	UT_ASSERT(consumed_current(source));
	++native_callback_calls;
	test_episode_native_clog_replay(reader, source->selected.source.timeline,
		native_callback_sync_fault, &native_callback_stats);
	/* Test late refusal after the complete original callback returned. */
	if (native_callback_late_fault == 1)
		++source->work_generation;
	if (native_callback_late_fault == 2)
		++reader->record->header.xl_scn;
}

static RfPageProofDetailV1
native_callback_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	(void)cut;
	UT_ASSERT(arg == native_callback_history);
	if (memcmp(source, &tx_inputs_typed.sources[0], sizeof(*source)) != 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (XLogRecGetRmid(reader) == RM_CLOG_ID) {
		for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
			ClusterRecoveryRelmapHistoryEntryV1 saved;
			uint64 original_scn = reader->record->header.xl_scn;
			bool ok;

			if (!cluster_recovery_relmap_history_source_v1(native_callback_history, i, &saved))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (memcmp(&saved.source.selected.source, source, sizeof(*source)) != 0)
				continue;
			ok = side_plan_clog_native_replay(tx_inputs_typed.side_plans[0], source,
				reader, native_callback_redo, consumed_current, &saved.source);
			native_callback_consumed = rf_side_online_plan_clog_native_consumed_v1(
				tx_inputs_typed.side_plans[0], source, reader);
			/* The deliberate frame fault changes only this borrowed reader. */
			reader->record->header.xl_scn = original_scn;
			return ok ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	}
	if (native_callback_commit && XLogRecGetRmid(reader) == RM_XACT_ID
		&& XLogRecGetXid(reader) == native_callback_xid
		&& (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT) {
		/* Controlled outcome, through the original native status writer. */
		test_episode_native_clog_set_committed(native_callback_xid);
		++native_callback_commits;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
native_callback_reset(void)
{
	native_callback_calls = native_callback_commits = 0;
	native_callback_sync_fault = native_callback_late_fault = 0;
	native_callback_consumed = false;
	memset(&native_callback_stats, 0, sizeof(native_callback_stats));
	consumed_mode = 1;
	consumed_cold_calls = 0;
}

static ClusterControlRootResult
native_callback_replay(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, bool commit)
{
	uint64 records;
	RfPageProofDetailV1 detail;

	native_callback_history = history;
	native_callback_xid = (TransactionId)pending->pending_owner.top_full_xid;
	native_callback_commit = commit;
	return cluster_wal_inputs_cold_census_v1(inputs, native_callback_record,
		episode_terminal_census_source, history, &records, &detail);
}

static void
native_callback_refused_pages(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, int fd)
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && fcntl(fd, F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
}

UT_TEST(test_episode_native_callback_consumes_only_after_sync_and_reads_whole_pages)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	uint8 bytes[BLCKSZ], expected[BLCKSZ] = {0};
	unsigned census;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	native_callback_reset();
	consumed_native_page(NULL, true);
	UT_ASSERT_EQ(native_callback_replay(inputs, history, &pending, true), 0);
	UT_ASSERT(native_callback_consumed && native_callback_stats.returned);
	UT_ASSERT_EQ(native_callback_calls, 1); UT_ASSERT_EQ(native_callback_commits, 1);
	UT_ASSERT_EQ(native_callback_stats.redos, 1); UT_ASSERT_EQ(native_callback_stats.touches, 1);
	UT_ASSERT(native_callback_stats.file_syncs > 0 && native_callback_stats.directory_syncs > 0);
	test_episode_native_clog_write_all();
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3); UT_ASSERT_EQ(out.inputs.clog_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6); UT_ASSERT_EQ(consumed_cold_calls, 2);
	consumed_native_page(bytes, false);
	expected[((uint32)pending.pending_owner.top_full_xid % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4]
		= TRANSACTION_STATUS_COMMITTED << (((uint32)pending.pending_owner.top_full_xid % 4) * 2);
	UT_ASSERT_EQ(memcmp(bytes, expected, sizeof(bytes)), 0);
	UT_ASSERT(!history->failed && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_native_callback_sync_failures_keep_attempt_fd_and_debt)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		uint8 bytes[BLCKSZ], zero[BLCKSZ] = {0};
		volatile bool caught = false;
		int fd;

		episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		native_callback_reset();
		fd = episode_owner_hold(history, &pending);
		consumed_native_page(NULL, true);
		native_callback_sync_fault = fault;
		test_capture_error_level = true;
		PG_TRY(); {
			(void)native_callback_replay(inputs, history, &pending, false);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(caught && !native_callback_stats.returned && !native_callback_consumed);
		UT_ASSERT_EQ(native_callback_calls, 1); UT_ASSERT_EQ(native_callback_stats.redos, 1);
		UT_ASSERT(native_callback_stats.file_syncs > 0);
		if (fault == 2) UT_ASSERT(native_callback_stats.directory_syncs > 0);
		/* Redo already changed disk; neither failure rolls it back nor allows retry. */
		consumed_native_page(bytes, false);
		UT_ASSERT_EQ(memcmp(bytes, zero, sizeof(bytes)), 0);
		native_callback_sync_fault = 0;
		UT_ASSERT_NE(native_callback_replay(inputs, history, &pending, false), 0);
		UT_ASSERT_EQ(native_callback_calls, 1);
		native_callback_refused_pages(history, &pending, fd);
		consumed_mode = 0;
		episode_native_pages_end(&history, &inputs);
		UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	}
}

UT_TEST(test_episode_native_callback_late_source_and_frame_changes_keep_one_attempt)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		uint8 bytes[BLCKSZ], zero[BLCKSZ] = {0};
		int fd;

		episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		native_callback_reset();
		fd = episode_owner_hold(history, &pending);
		consumed_native_page(NULL, true);
		native_callback_late_fault = fault;
		UT_ASSERT_NE(native_callback_replay(inputs, history, &pending, false), 0);
		UT_ASSERT(native_callback_stats.returned && !native_callback_consumed);
		UT_ASSERT_EQ(native_callback_calls, 1); UT_ASSERT_EQ(native_callback_stats.redos, 1);
		UT_ASSERT(native_callback_stats.file_syncs > 0 && native_callback_stats.directory_syncs > 0);
		consumed_native_page(bytes, false);
		UT_ASSERT_EQ(memcmp(bytes, zero, sizeof(bytes)), 0);
		native_callback_late_fault = 0;
		UT_ASSERT_NE(native_callback_replay(inputs, history, &pending, false), 0);
		UT_ASSERT_EQ(native_callback_calls, 1);
		native_callback_refused_pages(history, &pending, fd);
		consumed_mode = 0;
		episode_native_pages_end(&history, &inputs);
		UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	}
}

UT_TEST(test_episode_native_callback_does_not_replace_foreign_cold_consumption)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	native_callback_reset();
	consumed_mode = 0;
	UT_ASSERT_EQ(native_callback_replay(inputs, history, &pending, true), 0);
	UT_ASSERT(native_callback_consumed && native_callback_stats.returned);
	test_episode_native_clog_write_all();
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && inputs_pin_held && ctrc_set_held == 2);
	consumed_mode = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(native_callback_calls, 1); UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_native_callback_cannot_replace_earlier_held_observation)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	int fd;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	native_callback_reset();
	fd = episode_owner_hold(history, &pending);
	UT_ASSERT_EQ(native_callback_replay(inputs, history, &pending, true), 0);
	UT_ASSERT(native_callback_consumed && native_callback_stats.returned);
	test_episode_native_clog_write_all();
	native_callback_refused_pages(history, &pending, fd);
	UT_ASSERT_EQ(native_callback_calls, 1);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}
