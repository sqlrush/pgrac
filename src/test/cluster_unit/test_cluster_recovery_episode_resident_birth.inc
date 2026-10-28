/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_resident_birth.inc
 *     Carry actual native FULL initialization through the original episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_resident_birth.inc
 * NOTES
 *     Original CLOG redo, SLRU, files and held readers execute. Startup,
 *     locks, WAL-source authority and terminal leaves remain controlled.
 *     Resident initialization remains input, never an era/lifetime grant.
 *-------------------------------------------------------------------------
 */
static void
episode_resident_birth_begin(uint64 first, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	xl_clog_zeropage_full birth = {128, 0, first};
	DecodedXLogRecord decoded = {0};
	XLogReaderState reader = {0};

	episode_local_bounds_begin(UINT64_C(0x3004006a0), 4195000, inputs, history, pending);
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

UT_TEST(test_episode_resident_birth_reaches_original_page_and_query)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, own = 0, foreign = 0;

	episode_resident_birth_begin(UINT64_C(0x300400000), &inputs, &history, &pending);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT(out.inputs.query.terminal.base.native.resident_birth_observed);
	UT_ASSERT_EQ(out.inputs.query.terminal.base.native.resident_first_full_xid,
		UINT64_C(0x300400000));
	for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		if (page.native.origin == 0) {
			own++;
			UT_ASSERT(page.native.resident_birth_observed);
			UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
			UT_ASSERT_EQ(page.native.resident_first_full_xid, UINT64_C(0x300400000));
			UT_ASSERT_EQ(memcmp(&page.native, &out.inputs.query.terminal.base.native,
				sizeof(page.native)), 0);
		} else {
			foreign++;
			UT_ASSERT(!page.native.resident_birth_observed);
			UT_ASSERT_EQ(page.native.resident_first_full_xid, 0);
		}
	}
	UT_ASSERT_EQ(own, 1); UT_ASSERT_EQ(foreign, 2);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	episode_local_bounds_end(&history, &inputs);
}

UT_TEST(test_episode_resident_birth_does_not_borrow_wal_or_query_epoch)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	uint8 expected[BLCKSZ] = {0};
	unsigned own = 0;

	/* Equal native page bytes do not replace a different actual initialization
	 * with the expected WAL birth, query epoch or current local counter. */
	expected[208] = TRANSACTION_STATUS_COMMITTED;
	episode_resident_birth_begin(UINT64_C(0x400400000), &inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT_EQ(memcmp(out.inputs.query.terminal.base.native.image, expected, sizeof(expected)), 0);
	UT_ASSERT(out.inputs.query.terminal.base.native.resident_birth_observed);
	UT_ASSERT_EQ(out.inputs.query.terminal.base.native.resident_first_full_xid,
		UINT64_C(0x400400000));
	for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		if (page.native.origin != 0) continue;
		own++;
		UT_ASSERT_EQ(page.birth.first_full_xid, UINT64_C(0x300400000));
		UT_ASSERT_EQ(page.native.resident_first_full_xid, UINT64_C(0x400400000));
		UT_ASSERT_EQ(memcmp(page.native.image, expected, sizeof(expected)), 0);
		UT_ASSERT_NE(page.native.resident_first_full_xid >> 32, page.native.queried_full_xid >> 32);
		UT_ASSERT_NE(page.native.resident_first_full_xid >> 32, page.native.next_full_xid >> 32);
	}
	UT_ASSERT_EQ(own, 1);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	episode_local_bounds_end(&history, &inputs);
}

UT_TEST(test_episode_resident_birth_loss_poisoned_history_keeps_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryRelmapNativePageV1 page, zero_page = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned calls;
	int fds[3] = {-1, -1, -1};

	episode_resident_birth_begin(UINT64_C(0x300400000), &inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT(out.inputs.query.terminal.base.native.resident_birth_observed);
	for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
	}
	/* Original discard drops only the resident metadata/cache, preserving the
	 * native file and logical mutation sequence. A held observation cannot rebase. */
	test_episode_native_clog_discard_resident();
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	memset(&page, 0xa5, sizeof(page));
	UT_ASSERT(!cluster_recovery_relmap_history_native_page_v1(history, 0, &page));
	UT_ASSERT_EQ(memcmp(&page, &zero_page, sizeof(page)), 0);
	calls = episode_native_clog_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, calls);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	cluster_recovery_relmap_history_destroy_v1(&history);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	episode_local_bounds_end(&history, &inputs);
}
