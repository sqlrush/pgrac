/*-------------------------------------------------------------------------
 * test_cluster_recovery_consumed_native_redo.inc
 *    Original CLOG redo and held-page observations in the same episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_consumed_native_redo.inc
 * NOTES
 *    Native CLOG redo, status storage, SLRU and file I/O execute. The XACT
 *    outcome, foreign cold-applied leaf, authority and locks remain fixtures.
 *    This does not qualify native startup, durability or continuous lifetime.
 *-------------------------------------------------------------------------
 */
extern void test_episode_native_clog_redo(XLogReaderState *reader);
extern void test_episode_native_clog_set_committed(TransactionId xid);
extern void test_episode_native_clog_write_all(void);

static ClusterRecoveryRelmapHistory *consumed_native_history;
static TransactionId consumed_native_xid;
static unsigned consumed_native_calls, consumed_native_commits;
static bool consumed_native_commit, consumed_native_error;

static void
consumed_native_redo(XLogReaderState *reader, void *arg)
{
	UT_ASSERT(consumed_current(arg));
	test_episode_native_clog_redo(reader);
	++consumed_native_calls;
	if (consumed_native_error)
		ereport(ERROR, (errmsg("controlled ERROR after original native CLOG redo")));
}

static RfPageProofDetailV1
consumed_native_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	(void)cut;
	UT_ASSERT(arg == consumed_native_history);
	if (memcmp(source, &tx_inputs_typed.sources[0], sizeof(*source)) != 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (XLogRecGetRmid(reader) == RM_CLOG_ID) {
		for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
			ClusterRecoveryRelmapHistoryEntryV1 saved;
			if (!cluster_recovery_relmap_history_source_v1(consumed_native_history, i, &saved))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (memcmp(&saved.source.selected.source, source, sizeof(*source)) == 0)
				return side_plan_clog_native_replay(tx_inputs_typed.side_plans[0], source,
					reader, consumed_native_redo, consumed_current, &saved.source)
					? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	}
	/* Seed only this fixture's outcome through original CLOG storage, not
	 * an XACT redo/TT authority or a WAL-flush certificate. */
	if (consumed_native_commit && XLogRecGetRmid(reader) == RM_XACT_ID
		&& XLogRecGetXid(reader) == consumed_native_xid
		&& (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT) {
		test_episode_native_clog_set_committed(consumed_native_xid);
		++consumed_native_commits;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static ClusterControlRootResult
consumed_native_replay(ClusterWalInputsV1 *inputs, ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, bool commit, bool error)
{
	uint64 records;
	RfPageProofDetailV1 detail;
	consumed_native_history = history;
	consumed_native_xid = (TransactionId)pending->pending_owner.top_full_xid;
	consumed_native_calls = consumed_native_commits = 0;
	consumed_native_commit = commit;
	consumed_native_error = error;
	return cluster_wal_inputs_cold_census_v1(inputs, consumed_native_record,
		episode_terminal_census_source, history, &records, &detail);
}

static void
consumed_native_page(uint8 *bytes, bool write_marker)
{
	char path[MAXPGPATH];
	uint8 marker = 0x5a;
	int fd;
	episode_native_pages_path(path, 0, 128);
	fd = open(path, write_marker ? O_RDWR : O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (write_marker)
		UT_ASSERT_EQ(pwrite(fd, &marker, 1, 43), 1);
	else
		UT_ASSERT_EQ(pread(fd, bytes, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_episode_consumed_native_redo_reaches_original_sync_and_whole_page)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	uint8 bytes[BLCKSZ], expected[BLCKSZ] = {0};
	unsigned census;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_mode = 1; consumed_cold_calls = 0;
	consumed_native_page(NULL, true);
	UT_ASSERT_EQ(consumed_native_replay(inputs, history, &pending, true, false), 0);
	UT_ASSERT_EQ(consumed_native_calls, 1); UT_ASSERT_EQ(consumed_native_commits, 1);
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
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_consumed_native_dirty_status_is_not_a_disk_commit)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	uint8 bytes[BLCKSZ], cleared[BLCKSZ] = {0};

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_mode = 1; consumed_cold_calls = 0;
	UT_ASSERT_EQ(consumed_native_replay(inputs, history, &pending, true, false), 0);
	consumed_native_page(bytes, false);
	UT_ASSERT_EQ(memcmp(bytes, cleared, sizeof(bytes)), 0);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && inputs_pin_held && ctrc_set_held == 2);
	test_episode_native_clog_write_all();
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_consumed_native_redo_cannot_rebase_an_earlier_held_page)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	int fd;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_mode = 1; consumed_cold_calls = 0;
	fd = episode_owner_hold(history, &pending);
	UT_ASSERT_EQ(consumed_native_replay(inputs, history, &pending, false, false), 0);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && fcntl(fd, F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}

UT_TEST(test_episode_consumed_native_redo_late_error_keeps_attempt_and_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	uint8 bytes[BLCKSZ], cleared[BLCKSZ] = {0};
	volatile bool caught = false;
	int fd;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_mode = 1; consumed_cold_calls = 0;
	fd = episode_owner_hold(history, &pending);
	consumed_native_page(NULL, true);
	test_capture_error_level = true;
	PG_TRY(); {
		(void)consumed_native_replay(inputs, history, &pending, false, true);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(caught && consumed_native_calls == 1 && fcntl(fd, F_GETFD) >= 0);
	consumed_native_page(bytes, false);
	UT_ASSERT_EQ(memcmp(bytes, cleared, sizeof(bytes)), 0);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_consumed_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(consumed_native_calls, 1);
	consumed_mode = 0;
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}
