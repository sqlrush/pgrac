/* Original sharedfs sentinel/POSIX body beside original recovery ownership.
 * Namespacing isolates the existing fixture's PG configuration/error leaves.
 * No source UUID, path or descriptor is translated.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_storage_namespace.h"
int d3_storage_existing_main(void);
#define main d3_storage_existing_main
#include "test_cluster_shared_fs_sharedfs.c"
#undef main
#include "test_cluster_relmap_native_storage.h"

static int (*storage_original_open)(const char *, int) = OpenTransientFile;
static int (*storage_original_close)(int) = CloseTransientFile;
static unsigned storage_calls, storage_reads, storage_fault;
static bool storage_active, storage_sentinel_open, storage_root_closed;
static int storage_root_fd = -1, storage_sentinel_fd = -1;
static char storage_root[MAXPGPATH], storage_path[MAXPGPATH], storage_saved[MAXPGPATH];
static char storage_uuid[CLUSTER_SHARED_UUID_LEN];
static unsigned char storage_expected[BLCKSZ];
static ssize_t storage_expected_size;
static struct stat storage_expected_stat;
static bool storage_expected_exists;

static void
storage_remember_sentinel(void)
{
	int fd;
	storage_expected_exists = lstat(storage_path, &storage_expected_stat) == 0;
	storage_expected_size = 0;
	if (!storage_expected_exists) {
		UT_ASSERT_EQ(errno, ENOENT);
		return;
	}
	if (S_ISLNK(storage_expected_stat.st_mode))
		storage_expected_size = readlink(storage_path, (char *)storage_expected,
										 sizeof(storage_expected));
	else {
		fd = open(storage_path, O_RDONLY | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		storage_expected_size = pread(fd, storage_expected, sizeof(storage_expected), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	UT_ASSERT(storage_expected_size >= 0);
}

static bool
storage_sentinel_same(void)
{
	unsigned char bytes[BLCKSZ];
	struct stat st;
	ssize_t size;
	int fd;
	if (lstat(storage_path, &st) != 0)
		return !storage_expected_exists && errno == ENOENT;
	if (!storage_expected_exists || st.st_dev != storage_expected_stat.st_dev
		|| st.st_ino != storage_expected_stat.st_ino || st.st_mode != storage_expected_stat.st_mode
		|| st.st_nlink != storage_expected_stat.st_nlink || st.st_size != storage_expected_stat.st_size)
		return false;
	if (S_ISLNK(st.st_mode))
		size = readlink(storage_path, (char *)bytes, sizeof(bytes));
	else {
		fd = open(storage_path, O_RDONLY | O_NOFOLLOW);
		if (fd < 0)
			return false;
		size = pread(fd, bytes, sizeof(bytes), 0);
		if (close(fd) != 0)
			return false;
	}
	return size == storage_expected_size && size >= 0
		   && memcmp(bytes, storage_expected, (size_t)size) == 0;
}

static int
storage_capture_open(const char *path, int flags)
{
	int fd = storage_original_open(path, flags);
	if (storage_active && fd >= 0) {
		if (strcmp(path, storage_path) == 0) {
			storage_sentinel_fd = fd;
			storage_sentinel_open = true;
		} else
			storage_root_fd = fd;
	}
	return fd;
}

static int
storage_capture_close(int fd)
{
	int rc = storage_original_close(fd);
	if (storage_active && rc == 0) {
		if (fd == storage_sentinel_fd)
			storage_sentinel_open = false;
		if (fd == storage_root_fd)
			storage_root_closed = true;
	}
	return rc;
}

static ssize_t
storage_capture_pread(int fd, void *bytes, size_t length, off_t offset)
{
	ssize_t n;
	if (!storage_active)
		return pread(fd, bytes, length, offset);
	storage_reads++;
	if (storage_fault == 8 && storage_reads == 2)
		pg_re_throw();
	n = pread(fd, bytes, storage_fault == 9 && storage_reads == 2 ? length - 1 : length,
			  offset);
	if (storage_fault == 7 && storage_reads == 1 && n == (ssize_t)length) {
		struct stat before, after;
		int fresh;
		UT_ASSERT_EQ(fstat(fd, &before), 0);
		UT_ASSERT_EQ(rename(storage_path, storage_saved), 0);
		fresh = open(storage_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fresh >= 0);
		UT_ASSERT_EQ(write(fresh, bytes, length), (ssize_t)length);
		UT_ASSERT_EQ(fstat(fresh, &after), 0);
		UT_ASSERT(before.st_dev != after.st_dev || before.st_ino != after.st_ino);
		UT_ASSERT_EQ(close(fresh), 0);
		storage_remember_sentinel();
	}
	return n;
}

#undef OpenTransientFile
#undef CloseTransientFile
#undef pg_pread
#define OpenTransientFile storage_capture_open
#define CloseTransientFile storage_capture_close
#define pg_pread storage_capture_pread
#include "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#undef OpenTransientFile
#undef CloseTransientFile
#undef pg_pread

TestRelmapNativeStorageStats
test_relmap_native_storage_stats(void)
{
	bool root_live = storage_root_fd >= 0 && !storage_root_closed
					 && fcntl(storage_root_fd, F_GETFD) >= 0;
	TestRelmapNativeStorageStats out = { storage_calls, storage_reads,
		(int)root_live + (int)storage_sentinel_open, ut_current_failed,
		storage_sentinel_same(), false };
	if (root_live)
		out.readonly_cloexec = (fcntl(storage_root_fd, F_GETFL) & O_ACCMODE) == O_RDONLY
							  && (fcntl(storage_root_fd, F_GETFD) & FD_CLOEXEC) != 0;
	return out;
}

void
test_relmap_native_storage_setup(const char *root, const uint8 uuid[16])
{
	static const char hex[] = "0123456789abcdef";
	storage_active = false;
	storage_calls = storage_reads = storage_fault = 0;
	storage_root_fd = storage_sentinel_fd = -1;
	storage_root_closed = storage_sentinel_open = false;
	ut_current_failed = 0;
	UT_ASSERT(strlcpy(storage_root, root, sizeof(storage_root)) < sizeof(storage_root));
	UT_ASSERT(snprintf(storage_path, sizeof(storage_path), "%s/%s", root,
					   PGRAC_SHARED_CONTROL_FILE) < sizeof(storage_path));
	UT_ASSERT(snprintf(storage_saved, sizeof(storage_saved), "%s.saved", storage_path)
			  < sizeof(storage_saved));
	for (unsigned i = 0; i < 16; i++) {
		storage_uuid[2 * i] = hex[uuid[i] >> 4];
		storage_uuid[2 * i + 1] = hex[uuid[i] & 15];
	}
	storage_uuid[32] = '\0';
	cluster_shared_data_dir = storage_root;
	cluster_shared_storage_uuid = storage_uuid;
	cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	cluster_node_id = 0;
	cluster_shared_fs_sentinel_attach();
	storage_remember_sentinel();
	storage_active = true;
}

void
test_relmap_native_storage_fault(unsigned fault)
{
	PgracSharedControl changed;
	int fd;
	storage_fault = fault;
	if (fault >= 1 && fault <= 3) {
		memcpy(&changed, storage_expected, sizeof(changed));
		if (fault == 1) {
			changed.storage_uuid[0] = changed.storage_uuid[0] == '0' ? '1' : '0';
			changed.crc = cluster_shared_fs_sentinel_crc(&changed);
		}
		if (fault == 2)
			changed.crc ^= 1;
		fd = open(storage_path, O_WRONLY | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, &changed, sizeof(changed), 0), sizeof(changed));
		if (fault == 3)
			UT_ASSERT_EQ(ftruncate(fd, sizeof(changed) - 1), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	if (fault == 4 || fault == 6) {
		UT_ASSERT_EQ(rename(storage_path, storage_saved), 0);
		if (fault == 4)
			UT_ASSERT_EQ(symlink(storage_saved, storage_path), 0);
	}
	if (fault == 5)
		UT_ASSERT_EQ(link(storage_path, storage_saved), 0);
	storage_remember_sentinel();
}

int
test_relmap_native_storage_open(const uint8 uuid[16], sigjmp_buf *outer_stack)
{
	storage_calls++;
	PG_exception_stack = outer_stack;
	return cluster_shared_fs_recovery_root_open_v1(uuid);
}

TestRelmapNativeStorageStats
test_relmap_native_storage_done(void)
{
	TestRelmapNativeStorageStats result = test_relmap_native_storage_stats();
	UT_ASSERT_EQ(result.live_fds, 0);
	UT_ASSERT(result.sentinel_unchanged);
	UT_ASSERT(unlink(storage_path) == 0 || errno == ENOENT);
	UT_ASSERT(unlink(storage_saved) == 0 || errno == ENOENT);
	result.assertion_failed = ut_current_failed;
	storage_active = false;
	return result;
}
