/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_native_bounds.inc
 *    Original native counter/horizon producers with strict episode reads.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_native_bounds.inc
 * NOTES
 *    Complete native producer bodies, CLOG files and held readers execute.
 *    Startup, locks, WAL/episode authority and consumption stay controlled.
 *    These inputs do not certify era, lifetime or joint completion.
 *-------------------------------------------------------------------------
 */
static void
episode_native_bounds_capture(ClusterRecoveryRelmapHistory *history, int fds[3],
	uint64 next, TransactionId oldest)
{
	for (unsigned i = 0; i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page = {0};
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
		if (page.native.origin == 0) {
			UT_ASSERT(page.native.native_next_observed && page.native.truncation_horizon_observed);
			UT_ASSERT_EQ(page.native.next_full_xid, next);
			UT_ASSERT_EQ(page.native.oldest_clog_xid, oldest);
		} else {
			UT_ASSERT(!page.native.native_next_observed && !page.native.truncation_horizon_observed);
			UT_ASSERT_EQ(page.native.next_full_xid, 0);
			UT_ASSERT_EQ(page.native.oldest_clog_xid, InvalidTransactionId);
		}
	}
}

static void
episode_native_bounds_failed(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, const int fds[3])
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned reads;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	reads = episode_native_clog_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, reads);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
}

UT_TEST(test_episode_native_bounds_advance_before_observation)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, again;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[3] = {-1, -1, -1};
	unsigned census;
	episode_local_bounds_begin(UINT64_C(0x30040069f), 4195000, &inputs, &history, &pending);
	census = inventory_census_calls;
	test_episode_native_clog_advance_next(4195999);
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(out.inputs.query.birth.inputs.tail_next, UINT64_C(0x3004006a0));
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	episode_native_bounds_capture(history, fds, UINT64_C(0x3004006a0), 4195000);
	/* The original monotonic producer's no-op must preserve attached custody. */
	test_episode_native_clog_advance_next(4195999);
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &again, &diag));
	UT_ASSERT_EQ(memcmp(&out, &again, sizeof(out)), 0);
	UT_ASSERT_EQ(inventory_census_calls, census + 2);
	UT_ASSERT_EQ(episode_native_clog_calls, 12);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	episode_local_bounds_end(&history, &inputs);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_native_bounds_counter_drift_keeps_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[3] = {-1, -1, -1};
	unsigned reads;
	episode_local_bounds_begin(UINT64_C(0x3004006a0), 4195000, &inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	episode_native_bounds_capture(history, fds, UINT64_C(0x3004006a0), 4195000);
	reads = episode_native_clog_calls;
	test_episode_native_clog_advance_next(4196000);
	UT_ASSERT_EQ(episode_native_clog_calls, reads);
	episode_native_bounds_failed(history, &pending, fds);
	episode_local_bounds_end(&history, &inputs);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_native_bounds_horizon_equality_and_crossing)
{
	for (unsigned attached = 0; attached < 2; ++attached) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out;
		ClusterRecoveryNativeInventoryDiag diag;
		int fds[3] = {-1, -1, -1};
		episode_local_bounds_begin(UINT64_C(0x3004006a0), 4195000, &inputs, &history, &pending);
		test_episode_native_clog_advance_oldest(4195136);
		if (attached) {
			UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
				history, &pending, &out, &diag));
			episode_native_bounds_capture(history, fds, UINT64_C(0x3004006a0), 4195136);
			test_episode_native_clog_advance_oldest(4195000);
			UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
				history, &pending, &out, &diag));
			episode_native_bounds_capture(history, fds, UINT64_C(0x3004006a0), 4195136);
		}
		test_episode_native_clog_advance_oldest(4195137);
		if (!attached) {
			/* The old API exposes this raw horizon without qualifying it. */
			UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(
				history, &pending, &out, &diag));
			episode_native_bounds_capture(history, fds, UINT64_C(0x3004006a0), 4195137);
		}
		episode_native_bounds_failed(history, &pending, fds);
		episode_local_bounds_end(&history, &inputs);
		for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
	}
}
