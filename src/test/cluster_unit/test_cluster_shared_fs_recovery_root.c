/* Actual sharedfs sentinel, POSIX files and original backend body. Native
 * recovery/source ownership is covered separately by the retained-WAL unit.
 * Author: SqlRush <sqlrush@gmail.com> */
int recovery_root_existing_main(void);
#define main recovery_root_existing_main
#include "test_cluster_shared_fs_sharedfs.c"
#undef main

enum RootFdFault {
	ROOT_FD_OK,
	ROOT_OPEN_FAIL,
	SENTINEL_OPEN_FAIL,
	ROOT_STAT_FAIL,
	SENTINEL_STAT_FAIL,
	ROOT_READ_FAIL,
	ROOT_READ_SHORT,
	SENTINEL_CLOSE_FAIL,
	ROOT_CLOSE_FAIL,
	ROOT_BYTES_CHANGE,
	ROOT_SENTINEL_REPLACE,
	ROOT_DIRECTORY_REPLACE,
	ROOT_GUC_CHANGE,
	ROOT_OPEN_ERROR,
	SENTINEL_OPEN_ERROR,
	ROOT_READ_ERROR,
	ROOT_SECOND_READ_ERROR
};
static enum RootFdFault root_fd_fault;
static bool root_capture_active;
static int root_capture_fd = -1, root_sentinel_fd = -1, root_live;
static unsigned root_reads;

static int
root_capture_open(const char *path, int flags)
{
	bool sentinel = strstr(path, "/pgrac_shared.control") != NULL;
	int fd;
	if (!root_capture_active)
		return OpenTransientFile(path, flags);
	if ((!sentinel && root_fd_fault == ROOT_OPEN_ERROR)
		|| (sentinel && root_fd_fault == SENTINEL_OPEN_ERROR))
		pg_re_throw();
	if ((!sentinel && root_fd_fault == ROOT_OPEN_FAIL)
		|| (sentinel && root_fd_fault == SENTINEL_OPEN_FAIL)) {
		errno = EIO;
		return -1;
	}
	fd = OpenTransientFile(path, flags);
	if (fd >= 0) {
		root_live++;
		if (sentinel)
			root_sentinel_fd = fd;
		else
			root_capture_fd = fd;
	}
	return fd;
}

static int
root_capture_close(int fd)
{
	int rc = CloseTransientFile(fd);
	if (root_capture_active) {
		root_live--;
		if ((fd == root_sentinel_fd && root_fd_fault == SENTINEL_CLOSE_FAIL)
			|| (fd == root_capture_fd && root_fd_fault == ROOT_CLOSE_FAIL)) {
			errno = EIO;
			return -1;
		}
	}
	return rc;
}

static int
root_capture_stat(int fd, struct stat *st)
{
	if (root_capture_active
		&& ((fd == root_capture_fd && root_fd_fault == ROOT_STAT_FAIL)
			|| (fd == root_sentinel_fd && root_fd_fault == SENTINEL_STAT_FAIL))) {
		errno = EIO;
		return -1;
	}
	return fstat(fd, st);
}

static ssize_t
root_capture_pread(int fd, void *bytes, size_t length, off_t at)
{
	ssize_t n;
	char moved[MAXPGPATH];
	if (!root_capture_active)
		return pg_pread(fd, bytes, length, at);
	root_reads++;
	if (root_fd_fault == ROOT_READ_ERROR
		|| (root_fd_fault == ROOT_SECOND_READ_ERROR && root_reads == 2))
		pg_re_throw();
	if (root_fd_fault == ROOT_READ_FAIL || root_fd_fault == ROOT_CLOSE_FAIL) {
		errno = EIO;
		return -1;
	}
	n = pg_pread(fd, bytes, root_fd_fault == ROOT_READ_SHORT ? length - 1 : length, at);
	if (n == (ssize_t)length && root_reads == 1) {
		if (root_fd_fault == ROOT_BYTES_CHANGE) {
			MirrorSharedControl changed;
			int write_fd = open(sentinel_path, O_WRONLY);
			pg_crc32c crc;
			memcpy(&changed, bytes, sizeof(changed));
			changed.participant_node_ids[0]++;
			INIT_CRC32C(crc);
			COMP_CRC32C(crc, &changed, offsetof(MirrorSharedControl, crc));
			FIN_CRC32C(crc);
			changed.crc = crc;
			if (write_fd < 0 || pwrite(write_fd, &changed, sizeof(changed), 0) != sizeof(changed)
				|| close(write_fd) != 0)
				abort();
		}
		if (root_fd_fault == ROOT_SENTINEL_REPLACE) {
			int write_fd;
			snprintf(moved, sizeof(moved), "%s.old", sentinel_path);
			if (rename(sentinel_path, moved) != 0)
				abort();
			write_fd = open(sentinel_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
			if (write_fd < 0 || write(write_fd, bytes, length) != (ssize_t)length
				|| close(write_fd) != 0)
				abort();
		}
		if (root_fd_fault == ROOT_DIRECTORY_REPLACE) {
			snprintf(moved, sizeof(moved), "%s.old", test_root);
			if (rename(test_root, moved) != 0 || mkdir(test_root, 0700) != 0)
				abort();
		}
		if (root_fd_fault == ROOT_GUC_CHANGE)
			cluster_shared_data_dir = "/missing-original-root";
	}
	return n;
}

#define OpenTransientFile root_capture_open
#define CloseTransientFile root_capture_close
#define fstat root_capture_stat
#undef pg_pread
#define pg_pread root_capture_pread
#ifndef RECOVERY_ROOT_PRODUCT_SOURCE
#define RECOVERY_ROOT_PRODUCT_SOURCE "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#endif
#include RECOVERY_ROOT_PRODUCT_SOURCE
#undef OpenTransientFile
#undef CloseTransientFile
#undef fstat
#undef pg_pread

static void
root_fd_setup(uint8 uuid[16])
{
	static unsigned serial;
	char tag[48];
	UT_ASSERT_EQ(root_live, 0);
	root_capture_active = false;
	root_fd_fault = ROOT_FD_OK;
	root_capture_fd = root_sentinel_fd = -1;
	root_reads = 0;
	cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	snprintf(tag, sizeof(tag), "recovery_root_%u", serial++);
	fresh_root(tag);
	cluster_shared_storage_uuid = "1234567890abcdef1234567890abcdef";
	cluster_shared_fs_sharedfs_ops.init();
	for (unsigned i = 0; i < 16; i++)
		uuid[i] = (uint8[]){ 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef }[i % 8];
	root_capture_active = true;
}

static void
root_fd_cleanup(void)
{
	char path[MAXPGPATH];
	root_capture_active = false;
	root_fd_fault = ROOT_FD_OK;
	cluster_shared_data_dir = test_root;
	cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	snprintf(path, sizeof(path), "%s.old", test_root);
	if (access(path, F_OK) == 0) {
		if (rmdir(test_root) != 0 || rename(path, test_root) != 0)
			abort();
	}
	(void)rmdir(sentinel_path);
	(void)unlink(sentinel_path);
	snprintf(path, sizeof(path), "%s.old", sentinel_path);
	(void)unlink(path);
	snprintf(path, sizeof(path), "%s/hard", test_root);
	(void)unlink(path);
	if (rmdir(test_root) != 0)
		abort();
	UT_ASSERT_EQ(root_live, 0);
}

UT_TEST(test_recovery_root_fd_is_readonly_tracked_and_preserves_sentinel)
{
	uint8 uuid[16];
	MirrorSharedControl before, after;
	int fd;
	root_fd_setup(uuid);
	UT_ASSERT(read_mirror(&before));
	fd = cluster_shared_fs_recovery_root_open_v1(uuid);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		struct stat st;
		UT_ASSERT_EQ(fstat(fd, &st), 0);
		UT_ASSERT(S_ISDIR(st.st_mode));
		UT_ASSERT_EQ(fcntl(fd, F_GETFL) & O_ACCMODE, O_RDONLY);
		UT_ASSERT(fcntl(fd, F_GETFD) & FD_CLOEXEC);
		UT_ASSERT_EQ(root_live, 1);
		UT_ASSERT_EQ(root_capture_close(fd), 0);
	}
	UT_ASSERT_EQ(root_reads, 2);
	UT_ASSERT(read_mirror(&after) && memcmp(&before, &after, sizeof(before)) == 0);
	root_fd_cleanup();
}

UT_TEST(test_recovery_root_fd_requires_original_backend_path_and_uuid)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		uint8 uuid[16];
		int fd;
		root_fd_setup(uuid);
		if (fault == 1)
			cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_LOCAL;
		if (fault == 2)
			cluster_shared_data_dir = NULL;
		if (fault == 3)
			cluster_shared_data_dir = "";
		if (fault == 4)
			cluster_shared_data_dir = "relative";
		if (fault == 5)
			memset(uuid, 0, 16);
		fd = cluster_shared_fs_recovery_root_open_v1(fault == 6 ? NULL : uuid);
		UT_ASSERT_EQ(fd >= 0, fault == 0);
		if (fd >= 0)
			UT_ASSERT_EQ(root_capture_close(fd), 0);
		if (fault != 0)
			UT_ASSERT_EQ(root_reads, 0);
		root_fd_cleanup();
	}
}

UT_TEST(test_recovery_root_fd_rejects_missing_corrupt_linked_and_nonregular_sentinel)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		uint8 uuid[16];
		char path[MAXPGPATH];
		int fd;
		root_fd_setup(uuid);
		snprintf(path, sizeof(path), "%s/hard", test_root);
		if (fault == 1) {
			if (unlink(sentinel_path) != 0)
				abort();
		}
		if (fault == 2)
			uuid[0]++;
		if (fault == 3) {
			if (link(sentinel_path, path) != 0)
				abort();
		}
		if (fault == 4 || fault == 5 || fault == 6) {
			if (rename(sentinel_path, path) != 0)
				abort();
			if (fault == 4 && symlink(path, sentinel_path) != 0)
				abort();
			if (fault == 5 && mkfifo(sentinel_path, 0600) != 0)
				abort();
			if (fault == 6 && mkdir(sentinel_path, 0700) != 0)
				abort();
		}
		if (fault >= 7) {
			fd = open(sentinel_path, O_WRONLY);
			if (fd < 0)
				abort();
			if (fault == 7 && ftruncate(fd, 1) != 0)
				abort();
			if (fault == 8 && pwrite(fd, "x", 1, sizeof(MirrorSharedControl)) != 1)
				abort();
			if (fault == 9 && pwrite(fd, "x", 1, 0) != 1)
				abort();
			if (close(fd) != 0)
				abort();
		}
		fd = cluster_shared_fs_recovery_root_open_v1(uuid);
		UT_ASSERT_EQ(fd >= 0, fault == 0);
		if (fd >= 0)
			UT_ASSERT_EQ(root_capture_close(fd), 0);
		root_fd_cleanup();
	}
}

UT_TEST(test_recovery_root_fd_io_and_close_refusals_close_every_new_descriptor)
{
	for (unsigned fault = ROOT_FD_OK; fault <= ROOT_CLOSE_FAIL; fault++) {
		uint8 uuid[16];
		int fd;
		root_fd_setup(uuid);
		root_fd_fault = fault;
		fd = cluster_shared_fs_recovery_root_open_v1(uuid);
		UT_ASSERT_EQ(fd >= 0, fault == ROOT_FD_OK);
		if (fd >= 0)
			UT_ASSERT_EQ(root_capture_close(fd), 0);
		UT_ASSERT_EQ(root_live, 0);
		root_fd_cleanup();
	}
}

UT_TEST(test_recovery_root_fd_full_bytes_inode_namespace_and_config_drift_refuse)
{
	for (unsigned sample = 0; sample < 5; sample++) {
		uint8 uuid[16];
		int fd;
		root_fd_setup(uuid);
		root_fd_fault = sample == 0 ? ROOT_FD_OK : ROOT_BYTES_CHANGE + sample - 1;
		fd = cluster_shared_fs_recovery_root_open_v1(uuid);
		UT_ASSERT_EQ(fd >= 0, sample == 0);
		if (fd >= 0)
			UT_ASSERT_EQ(root_capture_close(fd), 0);
		UT_ASSERT_EQ(root_live, 0);
		root_fd_cleanup();
	}
}

UT_TEST(test_recovery_root_fd_error_cleanup_preserves_original_sentinel)
{
	for (unsigned sample = 0; sample < 5; sample++) {
		uint8 uuid[16];
		MirrorSharedControl before, after;
		volatile bool threw = false;
		volatile int fd = -1;
		root_fd_setup(uuid);
		UT_ASSERT(read_mirror(&before));
		root_fd_fault = sample == 0 ? ROOT_FD_OK : ROOT_OPEN_ERROR + sample - 1;
		PG_TRY();
		{
			fd = cluster_shared_fs_recovery_root_open_v1(uuid);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, sample != 0);
		UT_ASSERT_EQ(fd >= 0, sample == 0);
		if (fd >= 0)
			UT_ASSERT_EQ(root_capture_close(fd), 0);
		UT_ASSERT_EQ(root_live, 0);
		UT_ASSERT(read_mirror(&after) && memcmp(&before, &after, sizeof(before)) == 0);
		root_fd_cleanup();
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_recovery_root_fd_is_readonly_tracked_and_preserves_sentinel);
	UT_RUN(test_recovery_root_fd_requires_original_backend_path_and_uuid);
	UT_RUN(test_recovery_root_fd_rejects_missing_corrupt_linked_and_nonregular_sentinel);
	UT_RUN(test_recovery_root_fd_io_and_close_refusals_close_every_new_descriptor);
	UT_RUN(test_recovery_root_fd_full_bytes_inode_namespace_and_config_drift_refuse);
	UT_RUN(test_recovery_root_fd_error_cleanup_preserves_original_sentinel);
	UT_DONE();
	return ut_failed_count != 0;
}
