/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_multipage_init.inc
 *     Join actual same-origin FULL pages in the original episode reader.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_multipage_init.inc
 * NOTES
 *     Original native CLOG/SLRU and files execute at their two-slot capacity.
 *     Episode, locks, WAL and terminal leaves remain controlled. Resident
 *     initialization is input consistency, not era or lifetime authority.
 *-------------------------------------------------------------------------
 */
static void
multipage_init_begin(unsigned count, unsigned wrong_page,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending, ClusterRecoveryRelmapSourceV1 *foreign)
{
	unsigned matched = 0;

	initialized_pages_begin(count, true, false, 0, inputs, history, pending);
	memset(foreign, 0, sizeof(*foreign));
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i) {
		ClusterRecoveryRelmapHistoryEntryV1 source;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(*history, i, &source));
		if (source.source.selected.source.claim.identity.origin_node_id != 127) continue;
		*foreign = source.source;
		matched++;
	}
	UT_ASSERT_EQ(matched, 1);
	for (unsigned i = 0; i < count; ++i) {
		xl_clog_zeropage_full birth = {0};
		birth.pageno = 17 + i;
		birth.first_full_xid = (UINT64_C(3) << 32)
			| (uint64)birth.pageno * CLUSTER_NATIVE_CLOG_PER_PAGE;
		if ((unsigned)birth.pageno == wrong_page) birth.first_full_xid += UINT64_C(1) << 32;
		UT_ASSERT(test_episode_native_clog_full_apply(foreign, &birth));
	}
}

/* Both foreign pages share one segment inode. Inspect each original handle,
 * rather than finding any descriptor with that inode. */
static void
multipage_init_save_fds(ClusterRecoveryRelmapHistory *history, unsigned count, int *fds)
{
	UT_ASSERT_EQ(history->native_page_count, count);
	for (unsigned i = 0; i < count && i < history->native_page_count; ++i) {
		const NativeRelmapPage *held = &history->native_pages[i];
		ClusterRecoveryRelmapNativePageV1 page;
		struct stat st;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		fds[i] = test_episode_native_clog_fd(held->query ? history->native_base : held->held);
		UT_ASSERT(fds[i] >= 0);
		UT_ASSERT_EQ(fstat(fds[i], &st), 0);
		UT_ASSERT_EQ(st.st_ino, page.native.inode);
		for (unsigned j = 0; j < i; ++j) UT_ASSERT(fds[i] != fds[j]);
	}
}

static void
multipage_init_end(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs,
	unsigned count, const int *fds)
{
	initialized_pages_end(history, inputs);
	for (unsigned i = 0; i < count; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_multipage_init_keeps_both_actual_foreign_births)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 foreign;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, own = 0, seen = 0;
	int fds[3] = {-1, -1, -1};

	multipage_init_begin(2, 0, &inputs, &history, &pending, &foreign);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3); UT_ASSERT_EQ(out.inputs.clog_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		UT_ASSERT(page.native.resident_birth_observed);
		UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
		if (page.native.origin == 0) {
			own++;
			UT_ASSERT_EQ(page.native.status, TRANSACTION_STATUS_COMMITTED);
		} else {
			UT_ASSERT_EQ(page.native.origin, 127);
			UT_ASSERT_EQ(memcmp(&page.history.source, &foreign, sizeof(foreign)), 0);
			UT_ASSERT(page.birth.pageno == 17 || page.birth.pageno == 18);
			if (page.birth.pageno == 17) seen |= 1;
			if (page.birth.pageno == 18) seen |= 2;
		}
	}
	UT_ASSERT_EQ(own, 1); UT_ASSERT_EQ(seen, 3);
	multipage_init_save_fds(history, 3, fds);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	multipage_init_end(&history, &inputs, 3, fds);
}

UT_TEST(test_episode_multipage_init_wrong_foreign_epoch_closes_whole_group)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 foreign;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned wrong = 0;
	int fds[3] = {-1, -1, -1};

	multipage_init_begin(2, 18, &inputs, &history, &pending, &foreign);
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	for (unsigned i = 0; i < out.page_count && i < 3; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		if (page.native.origin == 127 && page.birth.pageno == 18) {
			UT_ASSERT(page.native.resident_birth_observed);
			UT_ASSERT_EQ(page.native.resident_first_full_xid,
				page.birth.first_full_xid + (UINT64_C(1) << 32));
			wrong++;
		}
	}
	UT_ASSERT_EQ(wrong, 1);
	multipage_init_save_fds(history, 3, fds);
	initialized_pages_refusal(history, &pending, 3, fds);
	multipage_init_end(&history, &inputs, 3, fds);
}

UT_TEST(test_episode_multipage_init_real_eviction_stays_unknown)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 foreign;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned unknown = 0, resident = 0;
	int fds[4] = {-1, -1, -1, -1};

	multipage_init_begin(3, 0, &inputs, &history, &pending, &foreign);
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 4); UT_ASSERT_EQ(out.inputs.clog_count, 4);
	for (unsigned i = 0; i < out.page_count && i < 4; ++i) {
		ClusterRecoveryRelmapNativePageV1 page;
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		if (!page.native.resident_birth_observed) {
			UT_ASSERT_EQ(page.native.origin, 127); UT_ASSERT_EQ(page.birth.pageno, 17);
			UT_ASSERT_EQ(page.native.resident_first_full_xid, 0);
			unknown++;
		} else {
			UT_ASSERT_EQ(page.native.resident_first_full_xid, page.birth.first_full_xid);
			resident++;
		}
	}
	UT_ASSERT_EQ(unknown, 1); UT_ASSERT_EQ(resident, 3);
	multipage_init_save_fds(history, 4, fds);
	initialized_pages_refusal(history, &pending, 4, fds);
	multipage_init_end(&history, &inputs, 4, fds);
}

UT_TEST(test_episode_multipage_init_late_equal_bytes_keep_original_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 foreign;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	xl_clog_zeropage_full birth = {18, 0, (UINT64_C(3) << 32)
		| UINT64_C(18) * CLUSTER_NATIVE_CLOG_PER_PAGE};
	uint8 before[BLCKSZ], after[BLCKSZ];
	unsigned matched = 0;
	int fds[3] = {-1, -1, -1};

	multipage_init_begin(2, 0, &inputs, &history, &pending, &foreign);
	UT_ASSERT(cluster_recovery_relmap_history_pending_initialized_pages_v1(history, &pending, &out, &diag));
	multipage_init_save_fds(history, 3, fds);
	for (unsigned i = 0; i < history->native_page_count && i < 3; ++i) {
		const NativeRelmapPage *page = &history->native_pages[i];
		if (page->observed.native.origin != 127 || page->observed.birth.pageno != 18) continue;
		UT_ASSERT_EQ(pread(fds[i], before, sizeof(before), (off_t)18 * BLCKSZ), BLCKSZ);
		UT_ASSERT(test_episode_native_clog_full_apply(&foreign, &birth));
		UT_ASSERT_EQ(pread(fds[i], after, sizeof(after), (off_t)18 * BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(memcmp(before, after, sizeof(before)), 0);
		matched++;
	}
	UT_ASSERT_EQ(matched, 1);
	initialized_pages_refusal(history, &pending, 3, fds);
	multipage_init_end(&history, &inputs, 3, fds);
}
