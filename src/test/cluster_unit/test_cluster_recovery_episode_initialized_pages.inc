/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_initialized_pages.inc
 *     Match original FULL births to actual per-origin native initialization.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_initialized_pages.inc
 * NOTES
 *     Original own redo, foreign cold apply, CLOG/SLRU and files execute.
 *     Episode, locks, WAL and terminal leaves are controlled. Matching birth
 *     is necessary input consistency, not era or continuous lifetime proof.
 *-------------------------------------------------------------------------
 */
extern bool cluster_recovery_relmap_history_pending_initialized_pages_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapPendingNativePagesV1 *out, ClusterRecoveryNativeInventoryDiag *diag);

static void
initialized_pages_begin(unsigned count, bool own, bool foreign, unsigned wrong,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	xl_clog_zeropage_full birth = {128, 0, UINT64_C(0x300400000)};
	DecodedXLogRecord decoded = {0};
	XLogReaderState reader = {0};
	unsigned applied = 0;

	episode_native_pages_begin(count, false, 8 * 1024 * 1024, inputs, history, pending);
	consumed_setup(*inputs, *history, false, 1);
	if (own) {
		if (wrong == 1) birth.first_full_xid += UINT64_C(1) << 32;
		decoded.header.xl_info = CLOG_ZEROPAGE_FULL;
		decoded.header.xl_rmid = RM_CLOG_ID;
		decoded.main_data = (char *)&birth;
		decoded.main_data_len = sizeof(birth);
		decoded.max_block_id = -1;
		reader.record = &decoded;
		test_episode_native_clog_redo(&reader);
		test_episode_native_clog_set_committed((uint32)pending->pending_owner.top_full_xid);
		test_episode_native_clog_write_all();
	}
	if (foreign) {
		birth.pageno = 17;
		birth.first_full_xid = (UINT64_C(3) << 32) | (uint64)17 * CLUSTER_NATIVE_CLOG_PER_PAGE;
		if (wrong == 2) birth.first_full_xid += UINT64_C(1) << 32;
		for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
			ClusterRecoveryRelmapHistoryEntryV1 source;
			UT_ASSERT(cluster_recovery_relmap_history_source_v1(*history, i, &source));
			if (source.source.selected.source.claim.identity.origin_node_id != 127) continue;
			UT_ASSERT(test_episode_native_clog_full_apply(&source.source, &birth));
			applied++;
		}
		UT_ASSERT_EQ(applied, 1);
	}
}

static void
initialized_pages_end(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	consumed_mode = 0;
	episode_native_pages_end(history, inputs);
}

static void
initialized_pages_saved_fds(ClusterRecoveryRelmapHistory *history, unsigned count, int *fds)
{
	for (unsigned i = 0; i < count; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
	}
}

static void
initialized_pages_refusal(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, unsigned count, const int *fds)
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryRelmapNativePageV1 page, no_page = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned calls;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	for (unsigned i = 0; i < count; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	memset(&page, 0xa5, sizeof(page));
	UT_ASSERT(!cluster_recovery_relmap_history_native_page_v1(history, 0, &page));
	UT_ASSERT_EQ(memcmp(&page, &no_page, sizeof(page)), 0);
	calls = episode_native_clog_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, calls);
}

UT_TEST(test_episode_initialized_pages_uses_both_actual_producers)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, own = 0, foreign = 0;

	initialized_pages_begin(1, true, true, 0, &inputs, &history, &pending);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 2); UT_ASSERT_EQ(out.inputs.clog_count, 2);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 4);
	for (unsigned i = 0; i < out.page_count && i < 2; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		UT_ASSERT(page.native.resident_birth_observed);
		UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
		if (page.native.origin == 0) own++;
		else { UT_ASSERT_EQ(page.native.origin, 127); foreign++; }
	}
	UT_ASSERT_EQ(own, 1); UT_ASSERT_EQ(foreign, 1);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	initialized_pages_end(&history, &inputs);
}

UT_TEST(test_episode_initialized_pages_refuses_each_unknown_origin)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out;
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned count = mode == 2 ? 3 : 2;
		int fds[3] = {-1, -1, -1};

		initialized_pages_begin(mode == 2 ? 2 : 1, mode != 0, mode != 1, 0,
			&inputs, &history, &pending);
		/* Complete bytes and historical consumption remain valid observations.
		 * Mode2 has one more physical FULL page with no actual resident input. */
		UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
		UT_ASSERT_EQ(out.page_count, count);
		initialized_pages_saved_fds(history, count, fds);
		initialized_pages_refusal(history, &pending, count, fds);
		initialized_pages_end(&history, &inputs);
		for (unsigned i = 0; i < count; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	}
}

UT_TEST(test_episode_initialized_pages_refuses_equal_bytes_wrong_birth)
{
	for (unsigned wrong = 1; wrong <= 2; ++wrong) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out;
		ClusterRecoveryNativeInventoryDiag diag;
		int fds[2] = {-1, -1};

		initialized_pages_begin(1, true, true, wrong, &inputs, &history, &pending);
		UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
		for (unsigned i = 0; i < 2; ++i) {
			ClusterRecoveryRelmapNativePageV1 page;
			uint8 expected[BLCKSZ] = {0};
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			if (page.native.origin == 0) expected[208] = TRANSACTION_STATUS_COMMITTED;
			UT_ASSERT_EQ(memcmp(page.native.image, expected, sizeof(expected)), 0);
		}
		initialized_pages_saved_fds(history, 2, fds);
		initialized_pages_refusal(history, &pending, 2, fds);
		initialized_pages_end(&history, &inputs);
		for (unsigned i = 0; i < 2; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	}
}

UT_TEST(test_episode_initialized_pages_cache_loss_keeps_old_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[2] = {-1, -1};

	initialized_pages_begin(1, true, true, 0, &inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(history, &pending, &out, &diag));
	initialized_pages_saved_fds(history, 2, fds);
	test_episode_native_clog_discard_resident();
	initialized_pages_refusal(history, &pending, 2, fds);
	initialized_pages_end(&history, &inputs);
	for (unsigned i = 0; i < 2; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_initialized_pages_alias_has_no_native_effect)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census;

	initialized_pages_begin(1, true, true, 0, &inputs, &history, &pending);
	saved = pending; census = inventory_census_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_initialized_pages_v1(history, &pending,
		(ClusterRecoveryRelmapPendingNativePagesV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT_EQ(episode_native_clog_calls, 0);
	UT_ASSERT_EQ(inventory_census_calls, census);
	UT_ASSERT(!history->failed && inputs_pin_held && ctrc_set_held == 2);
	initialized_pages_end(&history, &inputs);
}
