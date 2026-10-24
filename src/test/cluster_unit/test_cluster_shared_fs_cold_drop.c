/* Cold DROP/CREATE existing results, actual backend and POSIX files.
 * Author: SqlRush <sqlrush@gmail.com> */
int sharedfs_existing_tests_main(void);
#define main sharedfs_existing_tests_main
#include "test_cluster_shared_fs_sharedfs.c"
#undef main

enum ColdSyncFault {
	COLD_SYNC_OK,
	COLD_MAIN_SYNC_FAIL,
	COLD_DIR_SYNC_FAIL,
	COLD_MAIN_REPLACE,
	COLD_DIR_REPLACE,
	COLD_AUX_APPEAR,
	COLD_MAIN_GROW,
	COLD_STAT_FAIL,
	COLD_CLOSE_FAIL
};

static RelFileLocator cold_locator = { 1663, 5, 16384 };
static char cold_paths[MAX_FORKNUM + 1][MAXPGPATH];
static char cold_parent[MAXPGPATH];
static enum ColdSyncFault cold_fault;
static unsigned cold_main_syncs, cold_dir_syncs, cold_closes;
static bool cold_create_sync;
static unsigned cold_fail_directory;
static unsigned cold_replace_ancestor;

static void
cold_file(const char *path, bool nonempty)
{
	int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);

	if (fd < 0 || (nonempty && write(fd, "x", 1) != 1) || close(fd) != 0)
		abort();
}

static int
cold_fsync(int fd)
{
	struct stat st;
	char moved[MAXPGPATH];

	if (fstat(fd, &st) != 0)
		abort();
	if (S_ISDIR(st.st_mode)) {
		cold_dir_syncs++;
		if (cold_create_sync) {
			char expected[MAXPGPATH];
			struct stat identity;

			if (cold_dir_syncs == 1)
				snprintf(expected, sizeof(expected), "%s/base/5", test_root);
			else if (cold_dir_syncs == 2)
				snprintf(expected, sizeof(expected), "%s/base", test_root);
			else
				snprintf(expected, sizeof(expected), "%s", test_root);
			if (stat(expected, &identity) != 0 || st.st_dev != identity.st_dev
				|| st.st_ino != identity.st_ino)
				abort();
		}
		if (cold_main_syncs != 1)
			abort();
		if (cold_fault == COLD_DIR_SYNC_FAIL || cold_dir_syncs == cold_fail_directory) {
			errno = EIO;
			return -1;
		}
		if (cold_fault == COLD_DIR_REPLACE) {
			snprintf(moved, sizeof(moved), "%s.old", cold_parent);
			if (rename(cold_parent, moved) != 0 || mkdir(cold_parent, 0700) != 0)
				abort();
			cold_file(cold_paths[MAIN_FORKNUM], false);
		}
		if (cold_create_sync && cold_replace_ancestor == cold_dir_syncs) {
			char ancestor[MAXPGPATH];

			snprintf(ancestor, sizeof(ancestor), "%s/base", test_root);
			snprintf(moved, sizeof(moved), "%s/base.old", test_root);
			if (rename(ancestor, moved) != 0 || mkdir(ancestor, 0700) != 0)
				abort();
		}
	} else {
		cold_main_syncs++;
		if ((!cold_create_sync && st.st_size != 0) || cold_dir_syncs != 0)
			abort();
		if (cold_fault == COLD_MAIN_SYNC_FAIL) {
			errno = EIO;
			return -1;
		}
		if (cold_fault == COLD_MAIN_REPLACE) {
			snprintf(moved, sizeof(moved), "%s.old", cold_paths[MAIN_FORKNUM]);
			if (rename(cold_paths[MAIN_FORKNUM], moved) != 0)
				abort();
			cold_file(cold_paths[MAIN_FORKNUM], false);
		}
		if (cold_fault == COLD_AUX_APPEAR)
			cold_file(cold_paths[SPACE_FORKNUM], false);
		if (cold_fault == COLD_MAIN_GROW && pwrite(fd, "x", 1, st.st_size) != 1)
			abort();
	}
	return fsync(fd);
}

static int
cold_fstatat(int fd, const char *path, struct stat *out, int flags)
{
	if (cold_fault == COLD_STAT_FAIL && (cold_create_sync || strstr(path, "_space") != NULL)) {
		errno = EACCES;
		return -1;
	}
	return fstatat(fd, path, out, flags);
}

static int
cold_close(int fd)
{
	int rc = close(fd);

	cold_closes++;
	if (cold_fault == COLD_CLOSE_FAIL && cold_closes == 1) {
		errno = EIO;
		return -1;
	}
	return rc;
}

#define pg_fsync cold_fsync
#define fstatat cold_fstatat
#define close(fd) cold_close(fd)
#include "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#undef pg_fsync
#undef fstatat
#undef close

static int
cold_fd_count(void)
{
	int count = 0;

	for (int fd = 0; fd < 1024; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			count++;
	return count;
}

static void
cold_setup(const char *tag)
{
	fresh_root(tag);
	snprintf(cold_parent, sizeof(cold_parent), "%s/base/5", test_root);
	if (pg_mkdir_p(cold_parent, 0700) != 0)
		abort();
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		char *path = cluster_shared_fs_sharedfs_relpath(cold_locator, fork);

		snprintf(cold_paths[fork], sizeof(cold_paths[fork]), "%s", path);
		pfree(path);
	}
	cold_file(cold_paths[MAIN_FORKNUM], false);
	cold_fault = COLD_SYNC_OK;
	cold_main_syncs = cold_dir_syncs = cold_closes = 0;
	cold_create_sync = false;
	cold_fail_directory = cold_replace_ancestor = 0;
	enableFsync = true;
}

UT_TEST(test_create_existing_files_sync_all_directory_levels_without_mutation)
{
	for (unsigned fork_index = 0; fork_index < 2; fork_index++) {
		ForkNumber fork = fork_index == 0 ? MAIN_FORKNUM : SPACE_FORKNUM;
		struct stat before, after;
		char bytes[14];
		int fd, descriptors;

		cold_setup(fork_index == 0 ? "cold_create_main" : "cold_create_space");
		cold_create_sync = true;
		if (fork == SPACE_FORKNUM)
			cold_file(cold_paths[fork], false);
		fd = open(cold_paths[fork], O_WRONLY);
		UT_ASSERT(fd >= 0 && write(fd, "existing bytes", 14) == 14);
		UT_ASSERT_EQ(close(fd), 0);
		descriptors = cold_fd_count();
		UT_ASSERT_EQ(stat(cold_paths[fork], &before), 0);
		UT_ASSERT(cluster_shared_fs_sharedfs_sync_create_result(cold_locator, fork));
		UT_ASSERT_EQ(stat(cold_paths[fork], &after), 0);
		UT_ASSERT_EQ(before.st_ino, after.st_ino);
		UT_ASSERT_EQ(after.st_size, 14);
		fd = open(cold_paths[fork], O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, bytes, sizeof(bytes)) == sizeof(bytes));
		UT_ASSERT(memcmp(bytes, "existing bytes", sizeof(bytes)) == 0);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(cold_main_syncs, 1);
		UT_ASSERT_EQ(cold_dir_syncs, 3);
		UT_ASSERT_EQ(cold_closes, 4);
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

UT_TEST(test_create_sync_failures_and_namespace_drift_close_every_descriptor)
{
	for (enum ColdSyncFault fault = COLD_MAIN_SYNC_FAIL; fault <= COLD_CLOSE_FAIL; fault++) {
		char tag[64];
		int descriptors;

		if (fault == COLD_AUX_APPEAR)
			continue; /* CREATE does not require other forks to be absent. */
		snprintf(tag, sizeof(tag), "cold_create_fault_%u", fault);
		cold_setup(tag);
		cold_create_sync = true;
		descriptors = cold_fd_count();
		cold_fault = fault;
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_create_result(cold_locator, MAIN_FORKNUM));
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

UT_TEST(test_create_sync_requires_each_ancestor_and_its_identity)
{
	for (unsigned variant = 0; variant < 6; variant++) {
		char tag[64];
		int descriptors;

		snprintf(tag, sizeof(tag), "cold_create_ancestor_%u", variant);
		cold_setup(tag);
		cold_create_sync = true;
		descriptors = cold_fd_count();
		if (variant < 3)
			cold_fail_directory = variant + 1;
		else
			cold_replace_ancestor = variant - 2;
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_create_result(cold_locator, MAIN_FORKNUM));
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

UT_TEST(test_create_missing_linked_disabled_and_unsupported_results_refuse)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		char tag[64], other[MAXPGPATH];
		ForkNumber fork = MAIN_FORKNUM;
		int descriptors;

		snprintf(tag, sizeof(tag), "cold_create_invalid_%u", variant);
		cold_setup(tag);
		cold_create_sync = true;
		descriptors = cold_fd_count();
		snprintf(other, sizeof(other), "%s.other", cold_paths[MAIN_FORKNUM]);
		if (variant == 0)
			UT_ASSERT_EQ(unlink(cold_paths[MAIN_FORKNUM]), 0);
		else if (variant == 1) {
			UT_ASSERT_EQ(rename(cold_paths[MAIN_FORKNUM], other), 0);
			UT_ASSERT_EQ(symlink(other, cold_paths[MAIN_FORKNUM]), 0);
		} else if (variant == 2)
			UT_ASSERT_EQ(link(cold_paths[MAIN_FORKNUM], other), 0);
		else if (variant == 3) {
			snprintf(other, sizeof(other), "%s.other", cold_parent);
			UT_ASSERT_EQ(rename(cold_parent, other), 0);
			UT_ASSERT_EQ(symlink(other, cold_parent), 0);
		} else if (variant == 4)
			enableFsync = false;
		else if (variant == 5)
			fork = FSM_FORKNUM;
		else {
			UT_ASSERT_EQ(unlink(cold_paths[MAIN_FORKNUM]), 0);
			UT_ASSERT_EQ(mkfifo(cold_paths[MAIN_FORKNUM], 0600), 0);
		}
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_create_result(cold_locator, fork));
		UT_ASSERT_EQ(cold_main_syncs + cold_dir_syncs, 0);
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

UT_TEST(test_cold_existing_result_syncs_main_then_parent)
{
	struct stat before, after;
	int descriptors;

	cold_setup("cold_sync_ok");
	descriptors = cold_fd_count();
	UT_ASSERT_EQ(stat(cold_paths[MAIN_FORKNUM], &before), 0);
	UT_ASSERT(cluster_shared_fs_sharedfs_sync_drop_result(cold_locator));
	UT_ASSERT_EQ(stat(cold_paths[MAIN_FORKNUM], &after), 0);
	UT_ASSERT_EQ(before.st_ino, after.st_ino);
	UT_ASSERT_EQ(after.st_size, 0);
	UT_ASSERT_EQ(cold_main_syncs, 1);
	UT_ASSERT_EQ(cold_dir_syncs, 1);
	UT_ASSERT_EQ(cold_fd_count(), descriptors);
}

UT_TEST(test_cold_sync_io_errors_and_drift_refuse_and_close)
{
	for (enum ColdSyncFault fault = COLD_MAIN_SYNC_FAIL; fault <= COLD_CLOSE_FAIL; fault++) {
		char tag[64];
		int descriptors;

		snprintf(tag, sizeof(tag), "cold_sync_fault_%u", fault);
		cold_setup(tag);
		descriptors = cold_fd_count();
		cold_fault = fault;
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_drop_result(cold_locator));
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

UT_TEST(test_cold_incomplete_result_never_changes_files)
{
	for (int fault = -1; fault <= MAX_FORKNUM; fault++) {
		char tag[64];
		struct stat st;

		snprintf(tag, sizeof(tag), "cold_incomplete_%d", fault);
		cold_setup(tag);
		if (fault == -1)
			UT_ASSERT_EQ(unlink(cold_paths[MAIN_FORKNUM]), 0);
		else if (fault == MAIN_FORKNUM) {
			int fd = open(cold_paths[MAIN_FORKNUM], O_WRONLY);

			UT_ASSERT(fd >= 0 && write(fd, "x", 1) == 1);
			UT_ASSERT_EQ(close(fd), 0);
		} else
			cold_file(cold_paths[fault], false);
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_drop_result(cold_locator));
		UT_ASSERT_EQ(cold_main_syncs + cold_dir_syncs, 0);
		if (fault >= 0) {
			UT_ASSERT_EQ(stat(cold_paths[fault], &st), 0);
			UT_ASSERT_EQ(st.st_size, fault == MAIN_FORKNUM ? 1 : 0);
		}
	}
}

UT_TEST(test_cold_links_and_disabled_sync_are_not_durable)
{
	char other[MAXPGPATH];

	for (unsigned mode = 0; mode < 4; mode++) {
		char tag[64];
		int descriptors;

		snprintf(tag, sizeof(tag), "cold_links_%u", mode);
		cold_setup(tag);
		descriptors = cold_fd_count();
		snprintf(other, sizeof(other), "%s.other", cold_paths[MAIN_FORKNUM]);
		if (mode == 0) {
			UT_ASSERT_EQ(rename(cold_paths[MAIN_FORKNUM], other), 0);
			UT_ASSERT_EQ(symlink(other, cold_paths[MAIN_FORKNUM]), 0);
		} else if (mode == 1)
			UT_ASSERT_EQ(link(cold_paths[MAIN_FORKNUM], other), 0);
		else if (mode == 2)
			enableFsync = false;
		else {
			snprintf(other, sizeof(other), "%s.other", cold_parent);
			UT_ASSERT_EQ(rename(cold_parent, other), 0);
			UT_ASSERT_EQ(symlink(other, cold_parent), 0);
		}
		UT_ASSERT(!cluster_shared_fs_sharedfs_sync_drop_result(cold_locator));
		UT_ASSERT_EQ(cold_main_syncs + cold_dir_syncs, 0);
		UT_ASSERT_EQ(cold_fd_count(), descriptors);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_create_existing_files_sync_all_directory_levels_without_mutation);
	UT_RUN(test_create_sync_failures_and_namespace_drift_close_every_descriptor);
	UT_RUN(test_create_sync_requires_each_ancestor_and_its_identity);
	UT_RUN(test_create_missing_linked_disabled_and_unsupported_results_refuse);
	UT_RUN(test_cold_existing_result_syncs_main_then_parent);
	UT_RUN(test_cold_sync_io_errors_and_drift_refuse_and_close);
	UT_RUN(test_cold_incomplete_result_never_changes_files);
	UT_RUN(test_cold_links_and_disabled_sync_are_not_durable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
