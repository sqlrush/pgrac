/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_local_bounds.inc
 *    Join original local counter and raw horizon with held CLOG inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_local_bounds.inc
 * NOTES
 *    Original CLOG files, cache fields, readers and source fold execute.
 *    Locks, Startup/WAL authority and consumption leaves remain controlled.
 *    These bounds do not certify native era or continuous lifetime.
 *-------------------------------------------------------------------------
 */
extern bool cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapPendingNativePagesV1 *out, ClusterRecoveryNativeInventoryDiag *diag);

static void
episode_local_bounds_begin(uint64 next, TransactionId oldest,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, inputs, history, pending);
	test_episode_native_clog_local_bounds(true, next, oldest);
	consumed_setup(*inputs, *history, false, 1);
	UT_ASSERT_EQ(pending->pending_owner.top_full_xid, UINT64_C(0x300400340));
}

static void
episode_local_bounds_end(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	test_episode_native_clog_local_bounds(false, 0, InvalidTransactionId);
	consumed_mode = 0;
	episode_native_pages_end(history, inputs);
}

UT_TEST(test_episode_local_bounds_keeps_original_reads_and_foreign_unknown)
{
	const struct { uint64 next; TransactionId oldest; } cases[] = {
		{UINT64_C(0x3004006a0), 4195000},
		{UINT64_C(0x3004006a0), 4195136},
		{UINT64_C(0x300400a88), 4195000}
	};
	for (unsigned n = 0; n < lengthof(cases); ++n) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 out;
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census;
		episode_local_bounds_begin(cases[n].next, cases[n].oldest, &inputs, &history, &pending);
		census = inventory_census_calls;
		UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
			history, &pending, &out, &diag));
		UT_ASSERT_EQ(out.page_count, 3);
		UT_ASSERT_EQ(out.inputs.query.birth.inputs.tail_next, UINT64_C(0x3004006a0));
		UT_ASSERT_EQ(inventory_census_calls, census + 1);
		UT_ASSERT_EQ(episode_native_clog_calls, 6);
		for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
			ClusterRecoveryRelmapNativePageV1 page;
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			if (page.native.origin == 0) {
				UT_ASSERT(page.native.native_next_observed && page.native.truncation_horizon_observed);
				UT_ASSERT_EQ(page.native.next_full_xid, cases[n].next);
				UT_ASSERT_EQ(page.native.oldest_clog_xid, cases[n].oldest);
			} else {
				UT_ASSERT(!page.native.native_next_observed && !page.native.truncation_horizon_observed);
				UT_ASSERT_EQ(page.native.next_full_xid, 0);
				UT_ASSERT_EQ(page.native.oldest_clog_xid, InvalidTransactionId);
			}
		}
		UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
		episode_local_bounds_end(&history, &inputs);
	}
}

static void
episode_local_bounds_refuse(uint64 next, TransactionId oldest)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryRelmapNativePageV1 page;
	ClusterRecoveryNativeInventoryDiag diag;
	int fds[3] = {-1, -1, -1};
	unsigned calls, census;
	episode_local_bounds_begin(next, oldest, &inputs, &history, &pending);
	/* The old endpoint continues to expose unqualified raw observations. */
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &out, &diag));
	for (unsigned i = 0; i < 3; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = episode_native_pages_find_fd(page.native.inode);
		UT_ASSERT(fds[i] >= 0);
	}
	census = inventory_census_calls;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT(fcntl(fds[i], F_GETFD) >= 0);
	calls = episode_native_clog_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(episode_native_clog_calls, calls);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	episode_local_bounds_end(&history, &inputs);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_local_bounds_refuses_counter_tail_and_halfcycle)
{
	const uint64 next[] = {UINT64_C(0x30040069f), UINT64_C(0x300400340),
		UINT64_C(0x2004006a0), UINT64_C(0x4004006a0), UINT64_C(0x380400340)};
	for (unsigned i = 0; i < lengthof(next); ++i)
		episode_local_bounds_refuse(next[i], 4195000);
}

UT_TEST(test_episode_local_bounds_refuses_raw_horizon_without_rebasing)
{
	const TransactionId oldest[] = {4195137, InvalidTransactionId, UINT32_C(0x804006a0)};
	for (unsigned i = 0; i < lengthof(oldest); ++i)
		episode_local_bounds_refuse(UINT64_C(0x3004006a0), oldest[i]);
}

UT_TEST(test_episode_local_bounds_alias_and_late_failure_keep_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	volatile bool caught = false;
	int fd;
	episode_local_bounds_begin(UINT64_C(0x3004006a0), 4195000, &inputs, &history, &pending);
	saved = pending;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(history, &pending,
		(ClusterRecoveryRelmapPendingNativePagesV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!history->failed && episode_native_clog_calls == 0);
	fd = episode_owner_hold(history, &pending);
	consumed_mode = 3;
	test_capture_error_level = true;
	PG_TRY(); {
		UT_ASSERT(!cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
			history, &pending, &out, &diag));
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(caught && history->failed && relmap_failed > 0);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(fcntl(fd, F_GETFD) >= 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	episode_local_bounds_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}
