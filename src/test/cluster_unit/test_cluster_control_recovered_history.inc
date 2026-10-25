/* Selected recovered input remains an episode dependency after INSTALL.
 * ROOT, native checkpoint/WAL and immutable history readers are production;
 * original claim custody and native counters remain explicit fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
recovered_history_installed(uint8 before[66048], ControlRootImage *root,
							ClusterWalStartupImage ops[2], ClusterWalSourceRef *restart)
{
	ControlFileData candidate;
	ClusterWalStartupImage durable;
	ClusterWalSourceRef writer;

	recovered_founder_running(before, root, ops, 0, false, &candidate);
	if (ut_current_failed) return;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&ops[0].claim.identity,
					 ops[0].operation_uuid, &candidate, test_checkpoint_end, &durable), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &writer), 0);
	if (ut_current_failed) return;
	recovered_owner_read(root);
	UT_ASSERT_EQ(root->records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(root->records[3].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(ops[1].input_kind, CLUSTER_WAL_STARTUP_CLEAN);
	UT_ASSERT_EQ(root->startup[0].generation, 0);
	UT_ASSERT(root->refs[0].history_generation > ops[1].predecessor_file_sequence);
	memcpy(before, root->bytes, 66048);
	*restart = v3_recovered_driver_restart(root, 3);
}

UT_TEST(test_recovered_history_clean_follower_waits_after_sole_complete_install)
{
	uint8 before[66048];
	ControlRootImage root, selected;
	ClusterWalStartupImage ops[2], out;
	ClusterWalSourceRef restart;

	recovered_history_installed(before, &root, ops, &restart);
	if (ut_current_failed) goto done;
	for (unsigned retry = 0; retry < 2; retry++) {
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		recovered_owner_read(&selected);
		recovered_founder_peer_unchanged(&root, &selected, &ops[1]);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	}
done:
	cluster_node_id = 0;
	test_reserve_mode = false;
}

UT_TEST(test_recovered_history_driver_consumes_exact_selected_history)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint8 before[66048], byte;
		ControlRootImage root;
		ClusterWalStartupImage ops[2], out;
		ClusterWalSourceRef restart;
		ClusterWalHistoryStage stage = {0};
		ClusterWalHistoryImage history;
		ClusterControlRootResult expected;
		char path[MAXPGPATH], temp[MAXPGPATH], saved[MAXPGPATH];
		struct stat st;
		int fd;

		recovered_history_installed(before, &root, ops, &restart);
		if (ut_current_failed) goto done;
		stage.origin_node = 0;
		stage.generation = root.refs[0].history_generation;
		memcpy(stage.sha256, root.refs[0].history_sha256, 32);
		history_stage_paths(&stage, path, temp);
		if (fault == 0) {
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (fault == 1) {
			fd = open(path, O_RDWR);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pread(fd, &byte, 1, 40), 1);
			byte ^= 1;
			UT_ASSERT_EQ(pwrite(fd, &byte, 1, 40), 1);
			UT_ASSERT_EQ(close(fd), 0);
		} else {
			UT_ASSERT(snprintf(saved, sizeof(saved), "%s.saved", path) < sizeof(saved));
			UT_ASSERT_EQ(rename(path, saved), 0);
			UT_ASSERT_EQ(symlink(saved, path), 0);
		}
		test_cf_mode = test_actual_cf = ShareLock;
		expected = cluster_wal_history_read_locked(&root, 0, &history);
		test_cf_mode = test_actual_cf = NoLock;
		UT_ASSERT_NE(expected, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out), expected);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		v3_target_path(path, &ops[1], "");
		UT_ASSERT(lstat(path, &st) < 0 && errno == ENOENT);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed) return;
	}
}

UT_TEST(test_recovered_history_wait_preserves_original_current_owner_guards)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage ops[2], out;
		ClusterWalSourceRef restart;
		char path[MAXPGPATH];
		struct stat st;

		recovered_history_installed(before, &root, ops, &restart);
		if (ut_current_failed) goto done;
		if (fault == 0) test_self_incarnation++;
		else if (fault == 1) test_fence = false;
		else if (fault == 2) test_reserve_formation.prebump_sync_active = 1;
		else restart.claim.claim_sha256[0] ^= 1;
		UT_ASSERT_NE(cluster_control_root_v3_startup_advance_clean(&restart, &out),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		v3_target_path(path, &ops[1], "");
		UT_ASSERT(lstat(path, &st) < 0 && errno == ENOENT);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		test_fence = true;
		if (ut_current_failed) return;
	}
}

UT_TEST(test_recovered_history_follower_waits_before_native_install_too)
{
	for (unsigned phase = 0; phase < 3; phase++) {
		uint8 before[66048];
		ControlRootImage root, selected;
		ClusterWalStartupImage ops[2], out, durable;
		ClusterWalSourceRef restart;
		ClusterControlRootFileToken token;
		ControlFileData candidate;

		if (phase == 0)
			recovered_founder_pending(before, &root, &token, ops, 0, false);
		else
			recovered_founder_running(before, &root, ops, 0, false, &candidate);
		if (ut_current_failed) goto done;
		if (phase == 2) {
			UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&ops[0].claim.identity,
							 ops[0].operation_uuid, &candidate, test_checkpoint_end, &durable), 0);
			if (ut_current_failed) goto done;
			recovered_owner_read(&root);
		}
		memcpy(before, root.bytes, sizeof(before));
		restart = v3_recovered_driver_restart(&root, 3);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out),
					 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		recovered_owner_read(&selected);
		recovered_founder_peer_unchanged(&root, &selected, &ops[1]);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed) return;
	}
}

UT_TEST(test_recovered_history_does_not_promote_an_older_episode)
{
	for (unsigned older = 0; older < 2; older++) {
		uint8 before[66048];
		ControlRootImage root;
		ClusterWalStartupImage ops[2], out;
		ClusterWalSourceRef restart;
		ClusterWalHistoryStage stage = {0};
		char path[MAXPGPATH], old_path[MAXPGPATH], temp[MAXPGPATH];

		recovered_history_installed(before, &root, ops, &restart);
		if (ut_current_failed) goto done;
		/* Boundary metadata is changed deliberately, without granting a
		 * physical writer proof. Arbitrary older COMPLETE is not this cut. */
		if (older == 0)
			root.records[0].identity.root_lineage_seq++;
		else {
			stage.origin_node = 0;
			stage.generation = root.refs[0].history_generation;
			memcpy(stage.sha256, root.refs[0].history_sha256, 32);
			history_stage_paths(&stage, path, temp);
			stage.generation = ops[1].predecessor_file_sequence;
			history_stage_paths(&stage, old_path, temp);
			UT_ASSERT_EQ(rename(path, old_path), 0);
			root.refs[0].history_generation = stage.generation;
		}
		UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
		memcpy(before, root.bytes, sizeof(before));
		v2_write_roots(before);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_control_root_v3_startup_advance_clean(&restart, &out),
					 CLUSTER_CONTROL_ROOT_ABSENT);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		v2_assert_primary_unchanged(before);
		UT_ASSERT_EQ(test_actual_cf, NoLock);
	done:
		cluster_node_id = 0;
		test_reserve_mode = false;
		if (ut_current_failed) return;
	}
}
