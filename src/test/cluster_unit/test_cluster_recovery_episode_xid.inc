/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_xid.inc
 *     Original complete native counter inputs beside pending history.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_xid.inc
 * NOTES
 *     Execute the original ROOT/WAL readers and cold counter decoder/fold.
 *     Startup, GES and native CLOG leaves retain their original fixtures.
 *     Counter candidates do not qualify FullXid era or physical lifetime.
 *-------------------------------------------------------------------------
 */
static unsigned episode_xid_tail_mode;

static WalTestRecord
episode_xid_frame(const ClusterWalSourceRef *source, WalTestRecord next,
	TransactionId raw, TransactionId top)
{
	XLogRecord header = {0};
	char path[MAXPGPATH];
	uint8 bytes[2048];
	int fd;
	v2_checkpoint_wal_path(&source->claim.identity, next.record_start, source->timeline, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd < 0) return next;
	UT_ASSERT_EQ(pread(fd, &header, SizeOfXLogRecord, next.record_start % wal_segment_size), SizeOfXLogRecord);
	UT_ASSERT(header.xl_tot_len + 1 + sizeof(top) <= sizeof(bytes));
	if (header.xl_tot_len < SizeOfXLogRecord
		|| header.xl_tot_len + 1 + sizeof(top) > sizeof(bytes)) {
		close(fd); return next;
	}
	UT_ASSERT_EQ(pread(fd, bytes, header.xl_tot_len, next.record_start % wal_segment_size), header.xl_tot_len);
	if (TransactionIdIsValid(top)) {
		memmove(bytes + SizeOfXLogRecord + 1 + sizeof(top), bytes + SizeOfXLogRecord,
			header.xl_tot_len - SizeOfXLogRecord);
		bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_TOPLEVEL_XID;
		memcpy(bytes + SizeOfXLogRecord + 1, &top, sizeof(top));
		header.xl_tot_len += 1 + sizeof(top);
	}
	header.xl_xid = raw;
	INIT_CRC32C(header.xl_crc);
	COMP_CRC32C(header.xl_crc, bytes + SizeOfXLogRecord, header.xl_tot_len - SizeOfXLogRecord);
	COMP_CRC32C(header.xl_crc, &header, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(header.xl_crc);
	memcpy(bytes, &header, SizeOfXLogRecord);
	UT_ASSERT_EQ(pwrite(fd, bytes, header.xl_tot_len, next.record_start % wal_segment_size), header.xl_tot_len);
	UT_ASSERT_EQ(close(fd), 0);
	next.record_crc = header.xl_crc;
	next.exclusive_end = MAXALIGN(next.record_start + header.xl_tot_len);
	return next;
}

static WalTestRecord
episode_xid_tail(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	CheckPoint checkpoint = {0};
	WalTestRecord next;
	if (episode_xid_tail_mode == 0)
		return episode_terminal_write(source, previous, false, 4197000);
	if (episode_xid_tail_mode >= 4) {
		char map[TEST_RELMAP_LENGTH];
		test_relmap_payload(map, true, 2);
		next = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_RELMAP_ID, XLOG_RELMAP_UPDATE, map, sizeof(map));
		return episode_xid_frame(source, next,
			episode_xid_tail_mode == 4 ? BootstrapTransactionId : 4197000,
			episode_xid_tail_mode == 4 ? InvalidTransactionId : 4198000);
	}
	checkpoint.nextXid = FullTransactionIdFromU64(((uint64)3 << 32)
		| (episode_xid_tail_mode == 1 ? 4198000 : episode_xid_tail_mode == 2 ? 4195100
			: (UINT32_C(0x80000000) + 4195136)));
	checkpoint.ThisTimeLineID = source->timeline;
	checkpoint.PrevTimeLineID = source->timeline;
	checkpoint.redo = episode_xid_tail_mode == 2 ? previous.exclusive_end : previous.record_start;
	checkpoint.fullPageWrites = true;
	next = inventory_write_record(source, previous.exclusive_end, previous.record_start,
		RM_XLOG_ID, episode_xid_tail_mode == 2 ? XLOG_CHECKPOINT_SHUTDOWN : XLOG_CHECKPOINT_ONLINE,
		&checkpoint, sizeof(checkpoint));
	/* Native checkpoints have no transaction xid; preserve framing and CRC. */
	return episode_xid_frame(source, next, InvalidTransactionId, InvalidTransactionId);
}

UT_TEST(test_episode_xid_keeps_original_checkpoint_map_and_complete_tail)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid, again;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned acquires, census;

	episode_base_reset();
	episode_terminal_begin(EPISODE_TERMINAL_OTHER_XID, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	acquires = ctrc_set_calls; census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.checkpoint_next, ((uint64)3 << 32) | 4196000);
	UT_ASSERT_EQ(xid.before_map, xid.checkpoint_next);
	UT_ASSERT_EQ(xid.after_map, xid.checkpoint_next);
	UT_ASSERT_EQ(xid.tail_next, xid.checkpoint_next);
	UT_ASSERT_EQ(xid.map_end, pending.pending_owner.wal_end);
	UT_ASSERT(xid.map_start < xid.map_end && xid.map_ordinal != 0);
	UT_ASSERT_EQ(xid.records, xid.history.observation.observed.records);
	UT_ASSERT(xid.history.observation.observed.complete_end > xid.map_end);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(diag.sources, 2);
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT(history->native_base == NULL);
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &again, &diag));
	UT_ASSERT_EQ(memcmp(&xid, &again, sizeof(xid)), 0);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_xid_tail_header_and_checkpoint_use_original_counter_rules)
{
	for (unsigned mode = 0; mode < 4; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;

		episode_base_reset();
		episode_xid_tail_mode = mode;
		episode_terminal_tail_hook = episode_xid_tail;
		episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
		episode_terminal_tail_hook = NULL;
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (mode < 2) {
			UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(xid.after_map, ((uint64)3 << 32) | 4196000);
			UT_ASSERT_EQ(xid.tail_next, ((uint64)3 << 32) | (mode == 0 ? 4197001 : 4198000));
			UT_ASSERT(xid.history.observation.observed.complete_end > xid.map_end);
			UT_ASSERT(!history->failed && relmap_failed == 0);
		} else {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
			UT_ASSERT(history->failed && relmap_failed != 0);
		}
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_xid_full_counter_bounds_keep_wrap_and_exact_half_range_distinct)
{
	uint64 q = ((uint64)3 << 32) | UINT32_C(0xfffffff0);
	uint64 after = q + 1, tail = ((uint64)4 << 32) | 5;
	UT_ASSERT(history_pending_xid_bounds(q, q - 1, q, after, tail));
	UT_ASSERT(!history_pending_xid_bounds(q + (UINT64_C(1) << 32), q - 1, q, after, tail));
	UT_ASSERT(!history_pending_xid_bounds(q - (UINT64_C(1) << 32), q - 1, q, after, tail));
	UT_ASSERT(!history_pending_xid_bounds(q, q - 1, q, after, q + UINT64_C(0x80000000)));
	UT_ASSERT(history_pending_xid_bounds(q, q - 1, q, after, q + UINT64_C(0x7ffffffe)));
	UT_ASSERT(!history_pending_xid_bounds(q, q + 1, q, after, tail));
	UT_ASSERT(!history_pending_xid_bounds(q, q - 1, q, q, tail));
	UT_ASSERT(!history_pending_xid_bounds(q, q - 1, after, q, tail));
	UT_ASSERT(!history_pending_xid_bounds(q, q - 1, q, after, after - 1));
	UT_ASSERT(!history_pending_xid_bounds(UINT64_MAX, q - 1, q, after, tail));
	UT_ASSERT(!history_pending_xid_bounds(q, q - 1, q, after, (uint64)4 << 32));
}

UT_TEST(test_episode_xid_error_clears_private_pass_and_retains_original_native_fd)
{
	for (unsigned phase = 0; phase < 2; ++phase) {
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

		episode_base_reset();
		episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		held = history->native_base; reads = episode_base_calls;
		if (phase == 0) ctrc_replay_current_override = episode_history_current_error;
		else {
			inventory_census_calls = 0;
			inventory_between_passes = episode_terminal_recheck_error;
		}
		test_capture_error_level = true;
		PG_TRY();
		{
			(void)cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		inventory_between_passes = NULL;
		ctrc_replay_current_override = ctrc_set_original_current;
		UT_ASSERT(caught && history->failed && relmap_failed != 0);
		UT_ASSERT(history->inventory->relmap_xid == NULL);
		UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
		UT_ASSERT(history->native_base == held && held != NULL && episode_base_live == 1);
		UT_ASSERT_EQ(episode_base_calls, reads);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		census = inventory_census_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_xid_missing_checkpoint_or_window_fails_original_episode)
{
	for (unsigned missing = 0; missing < 2; ++missing) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		NativeInventoryHistory *selected;

		episode_base_reset();
		episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		selected = &history->inventory->sources[history->inventory->relmap_wal_source].history;
		if (missing == 0) selected->checkpoint_observed = false;
		else selected->window_observed = false;
		memset(&xid, 0x5a, sizeof(xid));
		UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
		UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_xid_output_and_caller_preconditions_preserve_original_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;

	episode_base_reset();
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
	if (history == NULL) return;
	census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	saved = pending;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending,
		(ClusterRecoveryRelmapPendingXidV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(saved)), 0);
	test_actual_cf = ShareLock;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	test_actual_cf = NoLock;
	CritSectionCount++;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	CritSectionCount--;
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(inventory_census_calls, census);
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_xid_unrelated_relmap_retains_original_frame_identity_refusal)
{
	for (unsigned mode = 4; mode < 6; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		episode_base_reset();
		episode_xid_tail_mode = mode;
		episode_terminal_tail_hook = episode_xid_tail;
		episode_terminal_begin(EPISODE_TERMINAL_MISSING, &inputs, &history, &pending);
		episode_terminal_tail_hook = NULL;
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT_EQ(summary.wal_matches, 1);
		UT_ASSERT_EQ(summary.wal_aliases, 0);
		UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
		UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		episode_terminal_done(&history, &inputs);
	}
}
