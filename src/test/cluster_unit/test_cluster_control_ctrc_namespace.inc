/* Original CTRC scope with real selected shared directory/ROOT files. */
UT_TEST(test_ctrc_namespace_retains_readonly_fd_through_scope_lifetime)
{
	ClusterWalInputsV1 *inputs = NULL;
	int fd = -1, same = -1;
	struct stat a, b;

	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd), 0);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fstat(fd, &a), 0);
	UT_ASSERT_EQ(lstat(cluster_shared_data_dir, &b), 0);
	UT_ASSERT(S_ISDIR(a.st_mode) && a.st_dev == b.st_dev && a.st_ino == b.st_ino);
	UT_ASSERT_EQ(fcntl(fd, F_GETFL) & O_ACCMODE, O_RDONLY);
	UT_ASSERT((fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &same), 0);
	UT_ASSERT_EQ(same, fd);
	UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_namespace_v1(inputs, &same) != 0);
	UT_ASSERT_EQ(same, -1);
	UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &same), 0);
	UT_ASSERT_EQ(same, fd);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	UT_ASSERT_EQ(errno, EBADF);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_namespace_rejects_other_purpose_and_changed_owner)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ResourceOwner saved;
		int fd = 99;

		ctrc_inputs_fixture();
		if (fault == 0) {
			inputs_native_recovery = true;
			UT_ASSERT_EQ(cluster_wal_inputs_cold_begin_v1(v2_storage, TEST_SYSID, &inputs), 0);
			inputs_native_recovery = false;
		} else
			UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		saved = CurrentResourceOwner;
		if (fault == 1) tx_inputs_typed.ctrc_work_generation++;
		if (fault == 2) ctrc_inputs_owner.writer.epoch++;
		if (fault == 3) tx_inputs_generation++;
		if (fault == 4) CurrentResourceOwner = (ResourceOwner)(uintptr_t)17;
		if (fault == 5) CritSectionCount++;
		if (fault == 6) MyBackendType = B_BG_WORKER;
		UT_ASSERT(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd) != 0);
		UT_ASSERT_EQ(fd, -1);
		CurrentResourceOwner = saved;
		CritSectionCount = 0;
		cluster_wal_inputs_release_v1(&inputs);
		UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_namespace_refuses_symlink_unsafe_mode_and_name_switch)
{
	char alias[MAXPGPATH];
	char *selected;
	struct stat original;

	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterControlRootResult result;
		int fd = 99;

		ctrc_inputs_fixture();
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		selected = cluster_shared_data_dir;
		UT_ASSERT_EQ(lstat(selected, &original), 0);
		if (fault == 0) {
			snprintf(alias, sizeof(alias), "%s.ctrc-link", test_root);
			UT_ASSERT_EQ(symlink(test_root, alias), 0);
			cluster_shared_data_dir = alias;
		}
		if (fault == 1) UT_ASSERT_EQ(chmod(selected, 0770), 0);
		if (fault == 2) {
			UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd), 0);
			cluster_shared_data_dir = "";
		}
		if (fault == 3) {
			/* A valid alias still changes the original scope's selected name,
			 * even before its first lazy namespace open. */
			snprintf(alias, sizeof(alias), "%s/.", test_root);
			cluster_shared_data_dir = alias;
		}
		result = cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd);
		cluster_shared_data_dir = selected;
		if (fault == 0) UT_ASSERT_EQ(unlink(alias), 0);
		if (fault == 1) UT_ASSERT_EQ(chmod(selected, original.st_mode & 0777), 0);
		UT_ASSERT(result != 0);
		UT_ASSERT_EQ(fd, -1);
		/* Restoration never rebases a failed namespace observation. */
		UT_ASSERT(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd) != 0);
		cluster_wal_inputs_release_v1(&inputs);
	}
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_namespace_refuses_replaced_directory_without_reopening)
{
	ClusterWalInputsV1 *inputs = NULL;
	char saved[MAXPGPATH];
	ClusterControlRootResult result;
	int fd = -1, refused = 99;

	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd), 0);
	snprintf(saved, sizeof(saved), "%s.ctrc-saved", test_root);
	UT_ASSERT_EQ(rename(test_root, saved), 0);
	UT_ASSERT_EQ(mkdir(test_root, 0700), 0);
	result = cluster_wal_inputs_ctrc_namespace_v1(inputs, &refused);
	UT_ASSERT_EQ(rmdir(test_root), 0);
	UT_ASSERT_EQ(rename(saved, test_root), 0);
	UT_ASSERT(result != 0);
	UT_ASSERT_EQ(refused, -1);
	UT_ASSERT(cluster_wal_inputs_ctrc_namespace_v1(inputs, &refused) != 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	UT_ASSERT_EQ(errno, EBADF);
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_namespace_initial_root_error_closes_fd_and_retains_scope_debt)
{
	ClusterWalInputsV1 *inputs = NULL;
	volatile bool caught = false;
	static int fd;
	int next_fd;

	fd = 99;
	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	/* Single-threaded fixture: the next open uses this lowest free descriptor. */
	next_fd = dup(STDERR_FILENO);
	UT_ASSERT(next_fd >= 0);
	UT_ASSERT_EQ(close(next_fd), 0);
	test_throw_root_read = true;
	PG_TRY();
	{
		(void)cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT_EQ(fcntl(next_fd, F_GETFD), -1);
	UT_ASSERT_EQ(errno, EBADF);
	UT_ASSERT(inputs_recovery_held && test_actual_cf == NoLock);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!inputs_pin_held);
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_namespace_same_root_bytes_cannot_replace_original_directory)
{
	ClusterWalInputsV1 *inputs = NULL;
	uint8 bytes[66048];
	char saved[MAXPGPATH], path[MAXPGPATH];
	ClusterControlRootResult result;
	int file, fd = 99;

	ctrc_inputs_fixture();
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	file = open(path, O_RDONLY);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(read(file, bytes, sizeof(bytes)), sizeof(bytes));
	UT_ASSERT_EQ(close(file), 0);
	snprintf(saved, sizeof(saved), "%s.ctrc-before-open", test_root);
	UT_ASSERT_EQ(rename(test_root, saved), 0);
	UT_ASSERT_EQ(mkdir(test_root, 0700), 0);
	snprintf(path, sizeof(path), "%s/global", test_root);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	v2_write_roots(bytes);
	/* Original ROOT bytes in a new directory do not select that namespace.
	 * Exercise this binding before any separate ROOT read can reject it. */
	result = cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	UT_ASSERT_EQ(unlink(path), 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/global", test_root);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(test_root), 0);
	UT_ASSERT_EQ(rename(saved, test_root), 0);
	UT_ASSERT(result != 0);
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT(cluster_wal_inputs_ctrc_namespace_v1(inputs, &fd) != 0);
	cluster_wal_inputs_release_v1(&inputs);
	test_inputs_startup_cf = false;
}
