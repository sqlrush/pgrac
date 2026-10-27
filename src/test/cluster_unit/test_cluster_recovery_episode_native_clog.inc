/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_native_clog.inc
 *     Native held-fd CLOG observations under the original replay episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_native_clog.inc
 * NOTES
 *     Real ROOT, claim, WAL and native CLOG I/O; shmem, locks and Startup
 *     remain controlled. Status and fd custody grant no era/lifetime proof.
 *-------------------------------------------------------------------------
 */
static char episode_clog_cwd[MAXPGPATH], episode_clog_work[MAXPGPATH];
static char episode_clog_path[MAXPGPATH], episode_clog_saved[MAXPGPATH];

static void
episode_clog_begin(ClusterWalInputsV1 **inputs, ClusterRecoveryRelmapHistory **history,
	ClusterRelmapV2Image *pending)
{
	char path[MAXPGPATH];
	const char *dirs[] = {"native_side", "native_side/origin_0",
		"native_side/origin_0/pg_xact", "episode-native-clog"};
	ClusterRecoveryRelmapFoldSummary summary;
	uint8 page[BLCKSZ] = {0};
	uint32 xid;
	int fd;

	episode_native_clog = true;
	episode_pending_begin(false, inputs, history, pending);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	UT_ASSERT(getcwd(episode_clog_cwd, sizeof(episode_clog_cwd)) != NULL);
	snprintf(episode_clog_work, sizeof(episode_clog_work), "%s/episode-native-clog", cluster_shared_data_dir);
	UT_ASSERT_EQ(chdir(episode_clog_work), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", cluster_shared_data_dir);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
	xid = (uint32)pending->pending_owner.top_full_xid;
	/* Independent native layout: 2 bits/xid, BLCKSZ bytes/page, 32 pages/segment. */
	snprintf(episode_clog_path, sizeof(episode_clog_path), "%s/%04X", path,
		(xid / (BLCKSZ * 4)) / 32);
	snprintf(episode_clog_saved, sizeof(episode_clog_saved), "%s.saved", episode_clog_path);
	fd = open(episode_clog_path, O_CREAT | O_EXCL | O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page, sizeof(page), (off_t)((xid / (BLCKSZ * 4)) % 32) * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	test_episode_native_clog_start(cluster_shared_data_dir);
	UT_ASSERT_EQ(episode_pending_drain(*history, pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.sources, 2);
	UT_ASSERT_EQ(summary.clog_after_wal, 2); /* Never discharged by a base read. */
}

static void
episode_clog_end(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	char path[MAXPGPATH];
	const char *dirs[] = {"episode-native-clog", "native_side/origin_0/pg_xact",
		"native_side/origin_0", "native_side"};
	episode_history_done(history, inputs);
	test_episode_native_clog_stop();
	UT_ASSERT_EQ(unlink(episode_clog_path), 0);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(episode_clog_cwd), 0);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	episode_native_clog = false;
}

UT_TEST(test_episode_native_clog_keeps_real_fd_and_all_status_observations)
{
	for (unsigned status = 0; status < 4; ++status) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingBaseV1 base, again;
		ClusterRecoveryNativeInventoryDiag diag;
		struct stat disk, held;
		uint8 byte = status;
		int fd, native_fd;

		episode_clog_begin(&inputs, &history, &pending);
		fd = open(episode_clog_path, O_RDWR);
		UT_ASSERT(fd >= 0);
		/* Fixture query is raw 4195136: page128, segment4, byte208, bit0. */
		UT_ASSERT_EQ((uint32)pending.pending_owner.top_full_xid, 4195136);
		UT_ASSERT_EQ(pwrite(fd, &byte, 1, 208), 1);
		UT_ASSERT_EQ(fstat(fd, &disk), 0); UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		if (history->native_base == NULL) { episode_clog_end(&history, &inputs); continue; }
		native_fd = test_episode_native_clog_fd(history->native_base);
		UT_ASSERT(native_fd >= 0 && fcntl(native_fd, F_GETFD) >= 0);
		UT_ASSERT_EQ(fstat(native_fd, &held), 0);
		UT_ASSERT_EQ(held.st_ino, disk.st_ino);
		UT_ASSERT_EQ(base.native.inode, disk.st_ino);
		UT_ASSERT_EQ(base.native.device, disk.st_dev);
		UT_ASSERT_EQ(base.native.status, status);
		UT_ASSERT_EQ(base.native.queried_full_xid, pending.pending_owner.top_full_xid);
		UT_ASSERT_EQ(base.fold.clog_after_wal, 2);
		UT_ASSERT_EQ(base.fold.sources, 2);
		UT_ASSERT_EQ(base.history.observation.source.claim.identity.origin_node_id, 0);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &again, &diag));
		UT_ASSERT_EQ(test_episode_native_clog_fd(history->native_base), native_fd);
		UT_ASSERT_EQ(memcmp(&base, &again, sizeof(base)), 0);
		UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && relmap_failed == 0);
		UT_ASSERT_EQ(ctrc_set_release_calls, 0);
		cluster_recovery_relmap_history_destroy_v1(&history);
		errno = 0;
		UT_ASSERT_EQ(fcntl(native_fd, F_GETFD), -1); UT_ASSERT_EQ(errno, EBADF);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_clog_end(&history, &inputs);
	}
}

UT_TEST(test_episode_native_clog_inode_and_neighbor_change_revoke_same_episode)
{
	for (unsigned replace = 0; replace < 2; ++replace) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		uint8 page[BLCKSZ] = {0}, byte = 4;
		int fd, native_fd;

		episode_clog_begin(&inputs, &history, &pending);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		if (history->native_base == NULL) { episode_clog_end(&history, &inputs); continue; }
		native_fd = test_episode_native_clog_fd(history->native_base);
		if (replace) {
			UT_ASSERT_EQ(rename(episode_clog_path, episode_clog_saved), 0);
			fd = open(episode_clog_path, O_CREAT | O_EXCL | O_RDWR, 0600);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(write(fd, page, BLCKSZ), BLCKSZ);
		} else {
			fd = open(episode_clog_path, O_RDWR);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, &byte, 1, 208), 1); /* neighbor xid, query status stays0 */
		}
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		UT_ASSERT(history->failed && relmap_failed > 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
		UT_ASSERT(fcntl(native_fd, F_GETFD) >= 0);
		memset(&base, 0x5a, sizeof(base));
		UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
		if (replace) {
			UT_ASSERT_EQ(unlink(episode_clog_path), 0);
			UT_ASSERT_EQ(rename(episode_clog_saved, episode_clog_path), 0);
		}
		episode_clog_end(&history, &inputs);
		errno = 0; UT_ASSERT_EQ(fcntl(native_fd, F_GETFD), -1); UT_ASSERT_EQ(errno, EBADF);
	}
}

UT_TEST(test_episode_native_clog_error_retains_fd_and_original_protection)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryNativeInventoryDiag diag;
	volatile bool caught = false;
	int native_fd;

	episode_clog_begin(&inputs, &history, &pending);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	if (history->native_base == NULL) { episode_clog_end(&history, &inputs); return; }
	native_fd = test_episode_native_clog_fd(history->native_base);
	test_episode_native_clog_error(true);
	PG_TRY(); {
		(void)cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	test_episode_native_clog_error(false);
	UT_ASSERT(caught && history->failed && relmap_failed > 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	UT_ASSERT_EQ(test_episode_native_clog_fd(history->native_base), native_fd);
	UT_ASSERT(fcntl(native_fd, F_GETFD) >= 0);
	episode_clog_end(&history, &inputs);
	errno = 0; UT_ASSERT_EQ(fcntl(native_fd, F_GETFD), -1); UT_ASSERT_EQ(errno, EBADF);
}
