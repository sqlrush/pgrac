/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_native_pages.inc
 *    Original-episode custody of complete native FULL CLOG witness pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_native_pages.inc
 * NOTES
 *    Real native CLOG/SLRU files and held descriptors; original Startup,
 *    shared memory, locks and fault delivery remain controlled.
 *-------------------------------------------------------------------------
 */
static unsigned episode_native_pages_foreign;
static bool episode_native_pages_duplicate;
static char episode_native_pages_cwd[MAXPGPATH], episode_native_pages_work[MAXPGPATH];

static WalTestRecord
episode_native_pages_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (source->claim.identity.origin_node_id == 0)
		return episode_birth_match_prefix(source, previous);
	for (unsigned i = 0; i < episode_native_pages_foreign; ++i) {
		xl_clog_zeropage_full birth = {0};
		birth.pageno = 17 + (episode_native_pages_duplicate ? 0 : i);
		birth.first_full_xid = (UINT64_C(3) << 32)
			| (uint64)birth.pageno * CLUSTER_NATIVE_CLOG_PER_PAGE;
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
	return previous;
}

static void
episode_native_pages_path(char *out, int origin, unsigned page)
{
	snprintf(out, MAXPGPATH, "%s/native_side/origin_%d/pg_xact/%04X",
		cluster_shared_data_dir, origin, page / 32);
}

static void
episode_native_pages_file(int origin, unsigned page, unsigned byte, uint8 value)
{
	char path[MAXPGPATH];
	uint8 image[BLCKSZ] = {0};
	int fd;
	image[byte] = value;
	episode_native_pages_path(path, origin, page);
	fd = open(path, O_CREAT | O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, image, sizeof(image), (off_t)(page % 32) * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
episode_native_pages_begin(unsigned count, bool duplicate, size_t budget,
	ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeInventoryDiag diag;
	char path[MAXPGPATH];
	const char *dirs[] = {"native_side", "native_side/origin_0",
		"native_side/origin_0/pg_xact", "native_side/origin_127",
		"native_side/origin_127/pg_xact", "episode-page-custody"};

	episode_native_clog = true;
	episode_base_reset(); episode_birth_match_mode = 0;
	episode_native_pages_foreign = count; episode_native_pages_duplicate = duplicate;
	episode_pending_prefix_hook = episode_native_pages_prefix;
	episode_clog_side_terminal_begin(EPISODE_TERMINAL_COMMIT, inputs, history, pending);
	episode_pending_prefix_hook = NULL;
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
	episode_clog_side_fault = 0;
	episode_clog_side_plans(*inputs);
	/* The native provider's exact fixed custody size was selected before
	 * the history build; rebuild only to select this case's original budget. */
	cluster_recovery_relmap_history_destroy_v1(history);
	UT_ASSERT_EQ(episode_history_build(*inputs, budget, history, &diag), 0);
	UT_ASSERT(*history != NULL);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	UT_ASSERT(getcwd(episode_native_pages_cwd, sizeof(episode_native_pages_cwd)) != NULL);
	snprintf(episode_native_pages_work, sizeof(episode_native_pages_work), "%s/episode-page-custody",
		cluster_shared_data_dir);
	UT_ASSERT_EQ(chdir(episode_native_pages_work), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", cluster_shared_data_dir);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
	episode_native_pages_file(0, 128, 208, TRANSACTION_STATUS_COMMITTED);
	for (unsigned i = 0; i < count; ++i)
		episode_native_pages_file(127, 17 + (duplicate ? 0 : i), 0, 0);
	test_episode_native_clog_start(cluster_shared_data_dir);
	episode_native_clog_calls = 0;
	UT_ASSERT_EQ(episode_pending_drain(*history, pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
}

static void
episode_native_pages_end(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	char path[MAXPGPATH];
	const char *dirs[] = {"episode-page-custody", "native_side/origin_0/pg_xact",
		"native_side/origin_0", "native_side/origin_127/pg_xact",
		"native_side/origin_127", "native_side"};

	inventory_between_passes = NULL;
	episode_terminal_done(history, inputs);
	test_episode_native_clog_stop();
	episode_native_pages_path(path, 0, 128); UT_ASSERT_EQ(unlink(path), 0);
	for (unsigned segment = 17 / 32; segment <= (17 + episode_native_pages_foreign - 1) / 32; ++segment) {
		episode_native_pages_path(path, 127, segment * 32);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(episode_native_pages_cwd), 0);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	episode_native_clog = false;
}

static int
episode_native_pages_find_fd(ino_t inode)
{
	struct stat st;
	for (int fd = 3; fd < 512; ++fd)
		if (fstat(fd, &st) == 0 && st.st_ino == inode)
			return fd;
	return -1;
}

UT_TEST(test_episode_native_pages_holds_all_origins_on_original_descriptors)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 result, again;
	ClusterRecoveryRelmapNativePageV1 page, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, reads, seen = 0;
	int fd[3] = {-1, -1, -1};

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(result.page_count, 3); UT_ASSERT_EQ(result.inputs.clog_count, 3);
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	for (uint32 i = 0; i < result.page_count && i < 3; ++i) {
		UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
		{
			unsigned matched = 0;
			for (uint32 j = 0; j < result.inputs.clog_count; ++j) {
				const ClusterRecoveryRelmapClogEventV1 *event = &result.inputs.clog[j];
				if (event->source_index == page.history.observation.source_index
					&& event->record_ordinal == page.record_ordinal
					&& event->read_lsn == page.read_lsn && event->end_lsn == page.end_lsn
					&& event->record_crc == page.record_crc
					&& memcmp(&event->full_birth, &page.birth, sizeof(page.birth)) == 0)
					matched++;
			}
			UT_ASSERT_EQ(matched, 1);
		}
		UT_ASSERT_EQ(page.birth.pageno, page.native.page_number);
		UT_ASSERT(page.read_lsn < page.end_lsn && page.record_ordinal != 0);
		UT_ASSERT_EQ(page.native.origin,
			page.history.source.selected.source.claim.identity.origin_node_id);
		fd[i] = episode_native_pages_find_fd(page.native.inode); UT_ASSERT(fd[i] >= 0);
		if (page.native.origin == 127) {
			seen++;
			UT_ASSERT_EQ(page.native.queried_full_xid, page.birth.first_full_xid);
			UT_ASSERT_EQ(page.birth.first_full_xid >> 32, 3);
			UT_ASSERT_EQ(page.native.status, TRANSACTION_STATUS_IN_PROGRESS);
		} else UT_ASSERT_EQ(page.native.queried_full_xid, pending.pending_owner.top_full_xid);
		UT_ASSERT_EQ(memcmp(&page.history.observation,
			&result.inputs.query.birth.inputs.generations[page.history.observation.source_index].observation,
			sizeof(page.history.observation)), 0);
	}
	UT_ASSERT_EQ(seen, 2);
	memset(&page, 0xa5, sizeof(page));
	UT_ASSERT(!cluster_recovery_relmap_history_native_page_v1(history, result.page_count, &page));
	UT_ASSERT_EQ(memcmp(&page, &zero, sizeof(page)), 0);
	reads = episode_native_clog_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &again, &diag));
	UT_ASSERT_EQ(memcmp(&result, &again, sizeof(result)), 0);
	UT_ASSERT_EQ(episode_native_clog_calls, reads + 6);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	UT_ASSERT_EQ(episode_native_clog_calls, reads + 12);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && relmap_failed == 0);
	cluster_recovery_relmap_history_destroy_v1(&history);
	for (unsigned i = 0; i < 3; ++i) if (fd[i] >= 0) UT_ASSERT_EQ(fcntl(fd[i], F_GETFD), -1);
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_native_pages_partial_failure_keeps_all_open_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 result, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	char path[MAXPGPATH];
	struct stat st;
	int fd, held;

	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	episode_native_pages_path(path, 127, 17); fd = open(path, O_RDWR); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(ftruncate(fd, 18 * BLCKSZ), 0); UT_ASSERT_EQ(close(fd), 0);
	memset(&result, 0xa5, sizeof(result));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
	UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
	held = episode_native_pages_find_fd(st.st_ino); UT_ASSERT(held >= 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && history->native_base != NULL);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	{
		unsigned reads = episode_native_clog_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
		UT_ASSERT_EQ(episode_native_clog_calls, reads);
	}
	cluster_recovery_relmap_history_destroy_v1(&history);
	if (held >= 0) UT_ASSERT_EQ(fcntl(held, F_GETFD), -1);
	episode_native_pages_end(&history, &inputs);
}

UT_TEST(test_episode_native_pages_whole_capacity_budget_and_duplicate_preflight)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingNativePagesV1 result, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned count = fault == 1 ? 128 : fault == 3 ? 2 : 127;
		bool ok;

		episode_native_pages_begin(count, fault == 3, (fault == 2 ? 1 : 8) * 1024 * 1024,
			&inputs, &history, &pending);
		ok = cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag);
		UT_ASSERT_EQ(ok, fault == 0);
		if (fault == 0) {
			UT_ASSERT_EQ(result.page_count, 128); UT_ASSERT_EQ(result.inputs.clog_count, 128);
			UT_ASSERT_EQ(episode_native_clog_calls, 256);
			UT_ASSERT(diag.owned_bytes <= 8 * 1024 * 1024);
		} else {
			UT_ASSERT_EQ(episode_native_clog_calls, 0);
			UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
			UT_ASSERT(history->failed && relmap_failed > 0);
		}
		episode_native_pages_end(&history, &inputs);
	}
}

UT_TEST(test_episode_native_pages_alias_error_and_final_recheck_keep_debt)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending, saved;
		ClusterRecoveryRelmapPendingNativePagesV1 result;
		ClusterRecoveryRelmapNativePageV1 page;
		ClusterRecoveryNativeInventoryDiag diag;
		char path[MAXPGPATH];
		int fd = -1;
		volatile bool caught = false;

		episode_native_pages_begin(1, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		saved = pending;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending,
			(ClusterRecoveryRelmapPendingNativePagesV1 *)&pending, &diag));
		UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
		UT_ASSERT(!history->failed && episode_native_clog_calls == 0);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_pages_v1(history, &pending, &result, &diag));
		for (uint32 i = 0; i < result.page_count; ++i) {
			UT_ASSERT(cluster_recovery_relmap_history_native_page_v1(history, i, &page));
			if (page.native.origin == 127) fd = episode_native_pages_find_fd(page.native.inode);
		}
		UT_ASSERT(fd >= 0);
		if (fault == 0) {
			uint8 byte = 0x40;
			episode_native_pages_path(path, 127, 17);
			{
				int writer = open(path, O_RDWR); UT_ASSERT(writer >= 0);
				UT_ASSERT_EQ(pwrite(writer, &byte, 1, (off_t)17 * BLCKSZ + 13), 1);
				UT_ASSERT_EQ(close(writer), 0);
			}
			UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		} else {
			inventory_census_calls = 0;
			inventory_between_passes = episode_terminal_recheck_error;
			test_capture_error_level = true;
			PG_TRY(); {
				(void)cluster_recovery_relmap_history_recheck_v1(history, &diag);
			} PG_CATCH(); { caught = true; } PG_END_TRY();
			inventory_between_passes = NULL;
			test_capture_error_level = false;
			UT_ASSERT(caught);
		}
		UT_ASSERT(history->failed && relmap_failed > 0 && fcntl(fd, F_GETFD) >= 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		UT_ASSERT(!cluster_recovery_relmap_history_native_page_v1(history, 0, &page));
		cluster_recovery_relmap_history_destroy_v1(&history);
		UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
		episode_native_pages_end(&history, &inputs);
	}
}
