/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_consumed_pages.inc
 *    Keep native consumption distinct while joining the original held pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_consumed_pages.inc
 * NOTES
 *    Original plans, source checks, physical reread and held files execute.
 *    Native redo and foreign cold-applied leaves are controlled. Neither
 *    consumption nor equal present bytes qualify durability or lifetime.
 *-------------------------------------------------------------------------
 */
extern bool cluster_recovery_relmap_history_pending_consumed_pages_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapPendingNativePagesV1 *out, ClusterRecoveryNativeInventoryDiag *diag);
extern bool cluster_wal_inputs_relmap_clog_consumed_v1(
	const ClusterRecoveryRelmapSourceV1 *source, const XLogReaderState *reader);
extern bool side_plan_clog_native_replay(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg);
extern bool test_episode_consumed_cold_applied(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, const XLogReaderState *reader);

static unsigned consumed_mode, consumed_cold_calls, consumed_redo_calls;
static unsigned consumed_foreign_origin = 127;
static bool consumed_include_foreign;
static ClusterRecoveryRelmapHistory *consumed_history;

/* Only this target's WAL-input object redirects this leaf. All old cases and
 * siblings retain the original getter. Exact source/header/MAIN still match. */
bool
test_episode_consumed_cold_applied(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, const XLogReaderState *reader)
{
	if (consumed_mode == 0 || source->claim.identity.origin_node_id != consumed_foreign_origin)
		return rf_side_online_plan_clog_applied_v1(plan, source, reader);
	if (!rf_side_online_plan_clog_matches_v1(plan, source, reader))
		return false;
	++consumed_cold_calls;
	if (consumed_cold_calls == 2 && consumed_mode == 2)
		return false;
	if (consumed_cold_calls == 2 && consumed_mode == 3)
		ereport(ERROR, (errmsg("controlled foreign cold consumption ERROR")));
	return true;
}

static bool
consumed_current(void *arg)
{
	return cluster_wal_inputs_relmap_source_current_v1(arg);
}

static void
consumed_redo(XLogReaderState *reader, void *arg)
{
	UT_ASSERT(reader != NULL && consumed_current(arg));
	++consumed_redo_calls;
}

static RfPageProofDetailV1
consumed_setup_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	(void)cut;
	UT_ASSERT(arg == consumed_history);
	if (XLogRecGetRmid(reader) != RM_CLOG_ID
		|| (!consumed_include_foreign
			&& memcmp(source, &tx_inputs_typed.sources[0], sizeof(*source)) != 0))
		return RF_PAGE_PROOF_DETAIL_OK;
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
		ClusterRecoveryRelmapHistoryEntryV1 saved;
		if (!cluster_recovery_relmap_history_source_v1(consumed_history, i, &saved))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (memcmp(&saved.source.selected.source, source, sizeof(*source)) != 0)
			continue;
		for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p)
			if (memcmp(&tx_inputs_typed.sources[p], source, sizeof(*source)) == 0)
				return side_plan_clog_native_replay(tx_inputs_typed.side_plans[p], source,
					reader, consumed_redo, consumed_current, &saved.source)
					? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	}
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static void
consumed_setup(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history,
	bool foreign, unsigned mode)
{
	uint64 records;
	RfPageProofDetailV1 detail;
	consumed_mode = mode;
	consumed_cold_calls = consumed_redo_calls = 0;
	consumed_include_foreign = foreign;
	consumed_history = history;
	/* Test setup only, before native-page observation. Production adds no pass
 * or replay. The same original private plan records callback consumption. */
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, consumed_setup_record,
		episode_terminal_census_source, history, &records, &detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(consumed_redo_calls, foreign ? 3 : 1);
}

UT_TEST(test_episode_consumed_pages_fresh_carriers_have_no_consumption)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	int fd;
	consumed_mode = 0;
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	fd = episode_owner_hold(history, &pending);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && fcntl(fd, F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}

UT_TEST(test_episode_consumed_pages_native_founder_and_cold_foreign_share_original_reads)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, false, 1);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(out.inputs.clog_count, 3);
	UT_ASSERT_EQ(consumed_cold_calls, 2);
	UT_ASSERT_EQ(consumed_redo_calls, 1);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && !history->failed && relmap_failed == 0);
	/* The native-only receipt still cannot satisfy the old cold-applied API. */
	consumed_mode = 0;
	episode_applied_reset(0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_applied_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT(history->failed && inputs_pin_held && ctrc_set_release_calls == 0);
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_consumed_pages_foreign_late_failure_retains_all_custody)
{
	for (unsigned mode = 2; mode <= 3; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
		ClusterRecoveryRelmapNativePageV1 page;
		ClusterRecoveryNativeInventoryDiag diag;
		int fds[3] = {-1, -1, -1};
		unsigned reads, calls;
		volatile bool caught = false;
		episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		consumed_setup(inputs, history, false, mode);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(
			history, &pending, &out, &diag));
		for (uint32 i = 0; i < 3; ++i) {
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			fds[i] = episode_native_pages_find_fd(page.native.inode);
			UT_ASSERT(fds[i] >= 0);
		}
		memset(&out, 0xa5, sizeof(out));
		test_capture_error_level = mode == 3;
		PG_TRY(); {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
				history, &pending, &out, &diag));
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT_EQ(caught, mode == 3);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(consumed_cold_calls, 2);
		UT_ASSERT(history->failed && relmap_failed > 0 && history->inventory->relmap_xid == NULL);
		for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		reads = episode_native_clog_calls; calls = consumed_cold_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
			history, &pending, &out, &diag));
		UT_ASSERT_EQ(episode_native_clog_calls, reads);
		UT_ASSERT_EQ(consumed_cold_calls, calls);
		consumed_mode = 0;
		episode_native_pages_end(&history, &inputs);
		for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	}
}

UT_TEST(test_episode_consumed_pages_foreign_native_receipt_cannot_replace_cold_apply)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, true, 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_consumed_pages_alias_and_old_modes_keep_original_contracts)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, false, 1);
	saved = pending;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending,
		(ClusterRecoveryRelmapPendingNativePagesV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!history->failed && consumed_cold_calls == 0 && episode_native_clog_calls == 0);
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(consumed_cold_calls, 0);
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(consumed_cold_calls, 2);
	UT_ASSERT_EQ(episode_native_clog_calls, 12);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
}
