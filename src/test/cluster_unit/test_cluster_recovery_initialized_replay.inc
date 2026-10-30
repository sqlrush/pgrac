/*-------------------------------------------------------------------------
 * test_cluster_recovery_initialized_replay.inc
 *     Complete native replay before comparing every held initialization.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_initialized_replay.inc
 * NOTES
 *     Original replay callback, CLOG/SLRU, sync/readback and held files run.
 *     Foreign cold-applied receipts, outcome, authority and locks are fixtures.
 *     Matching initialization is input consistency, not lifetime or COMPLETE.
 *-------------------------------------------------------------------------
 */
static unsigned initialized_replay_mode, initialized_replay_foreign;

static RfPageProofDetailV1
initialized_replay_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	if (memcmp(source, &tx_inputs_typed.sources[0], sizeof(*source)) == 0
		|| XLogRecGetRmid(reader) != RM_CLOG_ID)
		return native_callback_record(reader, source, cut, arg);
	if ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != CLOG_ZEROPAGE_FULL
		|| XLogRecHasAnyBlockRefs(reader) || XLogRecGetData(reader) == NULL
		|| XLogRecGetDataLen(reader) != sizeof(xl_clog_zeropage_full))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
		ClusterRecoveryRelmapHistoryEntryV1 saved;
		xl_clog_zeropage_full birth;

		if (memcmp(source, &tx_inputs_typed.sources[i], sizeof(*source)) != 0)
			continue;
		if (!cluster_recovery_relmap_history_source_v1(arg, i, &saved)
			|| memcmp(&saved.source.selected.source, source, sizeof(*source)) != 0
			|| !rf_side_online_plan_clog_matches_v1(tx_inputs_typed.side_plans[i], source, reader))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		memcpy(&birth, XLogRecGetData(reader), sizeof(birth));
		/* Negative fixtures change only the second native input, never the
		 * original saved WAL or its source. Present bytes still remain equal. */
		if (birth.pageno == 18 && initialized_replay_mode == 1)
			return RF_PAGE_PROOF_DETAIL_OK;
		if (birth.pageno == 18 && initialized_replay_mode == 2)
			birth.first_full_xid += UINT64_C(1) << 32;
		if (!test_episode_native_clog_full_apply(&saved.source, &birth))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		++initialized_replay_foreign;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static void
initialized_replay_begin(ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, inputs, history, pending);
	native_callback_reset();
	initialized_replay_mode = initialized_replay_foreign = 0;
	native_callback_history = *history;
	native_callback_xid = (TransactionId)pending->pending_owner.top_full_xid;
	native_callback_commit = true;
}

static ClusterControlRootResult
initialized_replay_run(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history)
{
	uint64 records;
	RfPageProofDetailV1 detail;

	return cluster_wal_inputs_cold_census_v1(inputs, initialized_replay_record,
		episode_terminal_census_source, history, &records, &detail);
}

static void
initialized_replay_refused(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, const int *fds)
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned reads;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(
		history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	reads = episode_native_clog_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(
		history, pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, reads);
	UT_ASSERT_EQ(native_callback_calls, 1);
}

UT_TEST(test_episode_initialized_replay_joins_complete_native_and_foreign_initialization)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, own = 0, foreign = 0;
	int fds[3] = {-1, -1, -1};

	initialized_replay_begin(&inputs, &history, &pending);
	UT_ASSERT_EQ(initialized_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(native_callback_consumed && native_callback_stats.returned);
	UT_ASSERT_EQ(native_callback_calls, 1); UT_ASSERT_EQ(native_callback_commits, 1);
	UT_ASSERT(native_callback_stats.file_syncs > 0 && native_callback_stats.directory_syncs > 0);
	UT_ASSERT_EQ(initialized_replay_foreign, 2);
	test_episode_native_clog_write_all();
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3); UT_ASSERT_EQ(out.inputs.clog_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	for (unsigned i = 0; i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		uint8 expected[BLCKSZ] = {0};

		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		UT_ASSERT(page.native.resident_birth_observed);
		UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
		if (page.native.origin == 0) {
			TransactionId xid = (TransactionId)pending.pending_owner.top_full_xid;
			expected[(xid % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4]
				= TRANSACTION_STATUS_COMMITTED << ((xid % 4) * 2);
			UT_ASSERT_EQ(page.native.status, TRANSACTION_STATUS_COMMITTED);
			++own;
		} else {
			UT_ASSERT_EQ(page.native.origin, 127);
			foreign |= page.birth.pageno == 17 ? 1 : page.birth.pageno == 18 ? 2 : 0;
		}
		UT_ASSERT_EQ(memcmp(page.native.image, expected, sizeof(expected)), 0);
	}
	UT_ASSERT_EQ(own, 1); UT_ASSERT_EQ(foreign, 3);
	multipage_init_save_fds(history, 3, fds);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	multipage_init_end(&history, &inputs, 3, fds);
}

UT_TEST(test_episode_initialized_replay_missing_or_wrong_foreign_birth_keeps_all_fds)
{
	for (unsigned mode = 1; mode <= 2; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out;
		ClusterRecoveryNativeInventoryDiag diag;
		int fds[3] = {-1, -1, -1};

		initialized_replay_begin(&inputs, &history, &pending);
		initialized_replay_mode = mode;
		UT_ASSERT_EQ(initialized_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(native_callback_consumed && native_callback_stats.returned);
		test_episode_native_clog_write_all();
		/* Original complete bytes/consumption remain observations. Neither may
		 * fill the missing or same-byte different native initialization. */
		UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(
			history, &pending, &out, &diag));
		UT_ASSERT_EQ(out.page_count, 3);
		multipage_init_save_fds(history, 3, fds);
		initialized_replay_refused(history, &pending, fds);
		multipage_init_end(&history, &inputs, 3, fds);
	}
}

UT_TEST(test_episode_initialized_replay_sync_error_does_not_supply_consumption)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 observed;
		ClusterRecoveryNativeInventoryDiag diag;
		volatile bool caught = false;
		int fds[3] = {-1, -1, -1};

		initialized_replay_begin(&inputs, &history, &pending);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(
			history, &pending, &observed, &diag));
		multipage_init_save_fds(history, 3, fds);
		native_callback_sync_fault = fault;
		test_capture_error_level = true;
		PG_TRY(); {
			(void)initialized_replay_run(inputs, history);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(caught && !native_callback_stats.returned && !native_callback_consumed);
		UT_ASSERT_EQ(native_callback_stats.redos, 1);
		UT_ASSERT(native_callback_stats.file_syncs > 0);
		if (fault == 2) UT_ASSERT(native_callback_stats.directory_syncs > 0);
		native_callback_sync_fault = 0;
		UT_ASSERT_NE(initialized_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		initialized_replay_refused(history, &pending, fds);
		multipage_init_end(&history, &inputs, 3, fds);
	}
}

UT_TEST(test_episode_initialized_replay_lost_birth_cannot_rebase_held_pages)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[3] = {-1, -1, -1};

	initialized_replay_begin(&inputs, &history, &pending);
	UT_ASSERT_EQ(initialized_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_episode_native_clog_write_all();
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(
		history, &pending, &out, &diag));
	multipage_init_save_fds(history, 3, fds);
	test_episode_native_clog_discard_resident();
	initialized_replay_refused(history, &pending, fds);
	multipage_init_end(&history, &inputs, 3, fds);
}
