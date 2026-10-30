/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_replayed_census.inc
 *     Complete founder replay, held initialization and same-scope pair census.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_replayed_census.inc
 * NOTES
 *     Native callback/redo/sync, original executor, files and pair readers run.
 *     XACT outcome, foreign applied receipts, authority and locks are fixtures.
 *     Matching inputs do not qualify era, continuous lifetime or completion.
 *-------------------------------------------------------------------------
 */
static ClusterRecoveryRelmapHistory *scope_replay_history;
static TestEpisodeClogReplayStats scope_replay_stats;
static unsigned scope_replay_fault, scope_replay_calls, scope_replay_foreign;
static TransactionId scope_replay_xid;
static bool scope_replay_consumed, scope_replay_error;
static ClusterControlRootResult scope_replay_result;
static int scope_replay_early_fds[2];

static void
scope_replay_native(XLogReaderState *reader, void *arg)
{
	ClusterRecoveryRelmapSourceV1 *source = arg;

	UT_ASSERT(consumed_current(source));
	++scope_replay_calls;
	test_episode_native_clog_replay(reader, source->selected.source.timeline,
		scope_replay_fault <= 2 ? scope_replay_fault : 0, &scope_replay_stats);
	if (scope_replay_fault == 3) ++source->work_generation;
}

static RfPageProofDetailV1
scope_replay_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	(void)cut;
	UT_ASSERT(arg == scope_replay_history);
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
		ClusterRecoveryRelmapHistoryEntryV1 saved;
		const RfSideOnlinePlanV1 *plan = tx_inputs_typed.side_plans[i];
		bool own = source->claim.identity.origin_node_id == 0;

		if (memcmp(source, &tx_inputs_typed.sources[i], sizeof(*source)) != 0) continue;
		if (!cluster_recovery_relmap_history_source_v1(scope_replay_history, i, &saved)
			|| memcmp(source, &saved.source.selected.source, sizeof(*source)) != 0)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (XLogRecGetRmid(reader) == RM_CLOG_ID) {
			if (own) {
				bool ok = side_plan_clog_native_replay(plan, source, reader,
					scope_replay_native, consumed_current, &saved.source);
				scope_replay_consumed = rf_side_online_plan_clog_native_consumed_v1(plan, source, reader);
				return ok ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			} else {
				xl_clog_zeropage_full birth;

				if ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != CLOG_ZEROPAGE_FULL
					|| XLogRecHasAnyBlockRefs(reader) || XLogRecGetData(reader) == NULL
					|| XLogRecGetDataLen(reader) != sizeof(birth)
					|| !rf_side_online_plan_clog_matches_v1(plan, source, reader))
					return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
				if (scope_replay_fault == 4) return RF_PAGE_PROOF_DETAIL_OK;
				memcpy(&birth, XLogRecGetData(reader), sizeof(birth));
				if (!test_episode_native_clog_full_apply(&saved.source, &birth))
					return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
				++scope_replay_foreign;
			}
		}
		if (own && XLogRecGetRmid(reader) == RM_XACT_ID
			&& XLogRecGetXid(reader) == scope_replay_xid
			&& (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT)
			test_episode_native_clog_set_committed(scope_replay_xid);
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static ClusterControlRootResult
scope_replay_run(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history)
{
	uint64 records;
	RfPageProofDetailV1 detail;

	return cluster_wal_inputs_cold_census_v1(inputs, scope_replay_record,
		episode_terminal_census_source, history, &records, &detail);
}

static void
scope_replay_setup(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, bool foreign)
{
	ClusterRecoveryRelmapPendingNativePagesV1 pages;
	ClusterRecoveryNativeInventoryDiag diag;

	UT_ASSERT(foreign);
	scope_replay_history = history;
	scope_replay_xid = (TransactionId)pending->pending_owner.top_full_xid;
	scope_replay_calls = scope_replay_foreign = 0;
	scope_replay_consumed = scope_replay_error = false;
	scope_replay_result = CLUSTER_CONTROL_ROOT_ABSENT;
	memset(&scope_replay_stats, 0, sizeof(scope_replay_stats));
	scope_replay_early_fds[0] = scope_replay_early_fds[1] = -1;
	consumed_mode = 1; consumed_foreign_origin = 1; consumed_cold_calls = 0;
	if (scope_replay_fault > 0 && scope_replay_fault <= 3) {
		/* Seed only a prior controlled outcome, so the raw observer can hold
		 * the old page. Native FULL redo must still replace it and finish. */
		test_episode_native_clog_set_committed(scope_replay_xid);
		test_episode_native_clog_write_all();
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, pending, &pages, &diag));
		initialized_pages_saved_fds(history, 2, scope_replay_early_fds);
	}
	test_capture_error_level = true;
	PG_TRY(); {
		scope_replay_result = scope_replay_run(inputs, history);
	} PG_CATCH(); { scope_replay_error = true; } PG_END_TRY();
	test_capture_error_level = false;
	if (scope_replay_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		test_episode_native_clog_write_all();
}

static void
scope_replay_begin(unsigned fault, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	scope_replay_fault = fault;
	scope_initialized_replay_hook = scope_replay_setup;
	scope_initialized_begin(inputs, history, pending, true);
	scope_initialized_replay_hook = NULL;
}

static void
scope_replay_refused(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, const int fds[2])
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	const ClusterRelmapRetentionCensus *census = NULL;
	unsigned calls = scope_replay_calls;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0);
	UT_ASSERT(!scope_native_prepare(inputs, &census) && census == NULL);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	for (unsigned i = 0; i < 2; ++i) if (fds[i] >= 0) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(scope_replay_calls, calls);
}

UT_TEST(test_scope_replayed_census_connects_callback_initialization_and_both_pairs)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL;
	int fds[2] = {-1, -1};
	unsigned irs;

	scope_replay_begin(0, &inputs, &history, &pending);
	UT_ASSERT(!scope_replay_error && scope_replay_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(scope_replay_consumed && scope_replay_stats.returned);
	UT_ASSERT_EQ(scope_replay_calls, 1); UT_ASSERT_EQ(scope_replay_foreign, 1);
	UT_ASSERT(scope_replay_stats.file_syncs > 0 && scope_replay_stats.directory_syncs > 0);
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	for (unsigned i = 0; i < 2; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		uint8 expected[BLCKSZ] = {0};

		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		if (page.native.origin == 0) expected[208] = TRANSACTION_STATUS_COMMITTED;
		UT_ASSERT_EQ(memcmp(page.native.image, expected, sizeof(expected)), 0);
	}
	irs = ctrc_set_calls;
	UT_ASSERT(scope_native_prepare(inputs, &census));
	UT_ASSERT_EQ(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	UT_ASSERT_EQ(ctrc_set_calls, irs);
	UT_ASSERT_EQ(relmap_failed | ctrc_set_release_calls | scope_native_legacy_calls, 0);
	scope_initialized_end(&inputs, &history, fds);
	UT_ASSERT_EQ(scope_native_destroyed, 1);
}

UT_TEST(test_scope_replayed_census_sync_errors_preserve_attempt_and_early_fds)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;

		scope_replay_begin(fault, &inputs, &history, &pending);
		UT_ASSERT(scope_replay_error && !scope_replay_stats.returned && !scope_replay_consumed);
		UT_ASSERT_EQ(scope_replay_stats.redos, 1); UT_ASSERT_EQ(scope_replay_calls, 1);
		UT_ASSERT(scope_replay_stats.file_syncs > 0);
		if (fault == 2) UT_ASSERT(scope_replay_stats.directory_syncs > 0);
		scope_replay_fault = 0;
		UT_ASSERT_NE(scope_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(scope_replay_calls, 1);
		scope_replay_refused(inputs, history, &pending, scope_replay_early_fds);
		scope_initialized_end(&inputs, &history, scope_replay_early_fds);
	}
}

UT_TEST(test_scope_replayed_census_late_source_change_does_not_replay)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;

	scope_replay_begin(3, &inputs, &history, &pending);
	UT_ASSERT(!scope_replay_error && scope_replay_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(scope_replay_stats.returned && !scope_replay_consumed);
	scope_replay_fault = 0;
	UT_ASSERT_NE(scope_replay_run(inputs, history), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(scope_replay_calls, 1);
	scope_replay_refused(inputs, history, &pending, scope_replay_early_fds);
	scope_initialized_end(&inputs, &history, scope_replay_early_fds);
}

UT_TEST(test_scope_replayed_census_founder_does_not_replace_foreign_initialization)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	int fds[2] = {-1, -1};

	scope_replay_begin(4, &inputs, &history, &pending);
	UT_ASSERT(scope_replay_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && scope_replay_consumed);
	UT_ASSERT_EQ(scope_replay_foreign, 0);
	scope_replay_refused(inputs, history, &pending, fds);
	scope_initialized_end(&inputs, &history, fds);
}

UT_TEST(test_scope_replayed_census_pair_failure_preserves_replayed_pages)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		const ClusterRelmapRetentionCensus *census = NULL;
		int fds[2] = {-1, -1};

		scope_replay_begin(0, &inputs, &history, &pending);
		UT_ASSERT(scope_replay_consumed && scope_initialized_read(history, &pending, fds));
		if (fault == 0) UT_ASSERT_EQ(unlinkat(scope_native_fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		else scope_native_write_pair(5, fault == 2, fault == 1);
		UT_ASSERT(!scope_native_prepare(inputs, &census) && census == NULL);
		scope_replay_refused(inputs, history, &pending, fds);
		scope_initialized_end(&inputs, &history, fds);
	}
}

UT_TEST(test_scope_replayed_census_retains_joint_completion_boundary)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	const ClusterRelmapRetentionCensus *census = NULL;
	ClusterRelmapCensusCommitted committed, zero = {0};
	int fds[2] = {-1, -1};

	scope_replay_begin(0, &inputs, &history, &pending);
	UT_ASSERT(scope_initialized_read(history, &pending, fds));
	UT_ASSERT(scope_native_prepare(inputs, &census));
	memset(&committed, 0xa5, sizeof(committed));
	UT_ASSERT(!cluster_relmap_retention_census_committed_cold((ClusterRelmapRetentionCensus *)census,
		InvalidOid, &committed));
	UT_ASSERT_EQ(memcmp(&committed, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(ctrc_set_release_calls | scope_native_legacy_calls | relmap_failed, 0);
	relmap_sibling->release_uncertain = true;
	scope_replay_refused(inputs, history, &pending, fds);
	UT_ASSERT_NE(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)census), CLUSTER_RELMAP_CENSUS_DRAINED);
	relmap_sibling->release_uncertain = false;
	scope_initialized_end(&inputs, &history, fds);
}
