/* Original configuration creation I/O: real files, syscall-only faults.
 * TAP 005 covers the real creator, canonical request and native policy.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "unit_test.h"

UT_DEFINE_GLOBALS();

enum Fault {
	NONE,
	WRITE_FAIL,
	FILE_SYNC,
	DIRECTORY_SYNC,
	ROOT_SYNC,
	CORRUPT,
	SHORT_READ,
	EXTRA_BYTE,
	REPLACED,
	CLOSE_FAIL,
	PARTIAL_IO,
	OCCUPIED
};
static enum Fault fault;
static jmp_buf refused;
static bool expect_error;
static unsigned writes, syncs, reads;
static int target = -1, parent = -1;
static char directory[MAXPGPATH], reason[256], object[72];

static ssize_t
test_pwrite(int fd, const void *data, size_t size, off_t off)
{
	target = fd;
	writes++;
	if (fault == WRITE_FAIL) {
		errno = ENOSPC;
		return -1;
	}
	if (fault == PARTIAL_IO && writes == 1) {
		errno = EINTR;
		return -1;
	}
	return pwrite(fd, data, fault == PARTIAL_IO ? Min(size, 317) : size, off);
}

static ssize_t
test_pread(int fd, void *data, size_t size, off_t off)
{
	reads++;
	if (fd == target && fault == SHORT_READ)
		return 0;
	if (fault == PARTIAL_IO && reads == 1) {
		errno = EINTR;
		return -1;
	}
	return pread(fd, data, fault == PARTIAL_IO ? Min(size, 317) : size, off);
}

static int
test_fsync(int fd)
{
	struct stat st;
	syncs++;
	UT_ASSERT(fstat(fd, &st) == 0);
	if ((fault == FILE_SYNC && fd == target)
		|| (fault == DIRECTORY_SYNC && S_ISDIR(st.st_mode) && fd != parent)
		|| (fault == ROOT_SYNC && fd == parent)) {
		errno = EIO;
		return -1;
	}
	if (fd == target && (fault == CORRUPT || fault == EXTRA_BYTE))
		UT_ASSERT(pwrite(fd, "X", 1, fault == CORRUPT ? 99 : st.st_size) == 1);
	if (fd == target && fault == REPLACED) {
		char path[MAXPGPATH], old[MAXPGPATH];
		int replacement;
		snprintf(path, sizeof(path), "%s/config_images/%s", directory, object);
		snprintf(old, sizeof(old), "%s/config_images/old", directory);
		UT_ASSERT(rename(path, old) == 0);
		replacement = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
		UT_ASSERT(replacement >= 0 && close(replacement) == 0);
	}
	return fsync(fd);
}

static int
test_close(int fd)
{
	int result = close(fd);
	if (fd == target && fault == CLOSE_FAIL) {
		errno = EIO;
		return -1;
	}
	return result;
}

#define pwrite test_pwrite
#define pread test_pread
#define fsync test_fsync
#define close test_close
#include "../../backend/cluster/cluster_initdb_config.c"
#undef pwrite
#undef pread
#undef fsync
#undef close

bool
errstart(int level, const char *domain)
{
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return true;
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(reason, sizeof(reason), fmt, args);
	va_end(args);
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	if (expect_error)
		longjmp(refused, 1);
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
void
pfree(void *ptr)
{
	free(ptr);
}

static void
run_case(enum Fault injection)
{
	int status, images;
	pid_t child;
	bool success = injection == NONE || injection == PARTIAL_IO;
	strlcpy(directory, "/tmp/pgrac-initdb-config-XXXXXX", sizeof(directory));
	UT_ASSERT(mkdtemp(directory) != NULL);
	parent = open(directory, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(parent >= 0);
	memset(object, '0', 66);
	memcpy(object, "1-aa", 4);
	memcpy(object + 66, ".conf", 6);
	if (injection == OCCUPIED) {
		int fd;
		UT_ASSERT(mkdirat(parent, "config_images", 0700) == 0);
		images = openat(parent, "config_images", O_RDONLY | O_DIRECTORY);
		fd = openat(images, object, O_CREAT | O_EXCL | O_WRONLY, 0600);
		UT_ASSERT(fd >= 0 && write(fd, "keep", 4) == 4 && close(fd) == 0 && close(images) == 0);
	}
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		ClusterInitdbConfig *config = calloc(1, sizeof(*config));
		UT_ASSERT(config != NULL);
		config->ref.sha256[0] = 0xaa;
		config->len = 8203;
		config->bytes = malloc(config->len);
		UT_ASSERT(config->bytes != NULL);
		memset(config->bytes, 'a', config->len);
		fault = injection;
		expect_error = !success;
		writes = reads = syncs = 0;
		target = -1;
		if (setjmp(refused) == 0) {
			cluster_initdb_config_create(config, parent);
			UT_ASSERT(success && writes > 0 && reads >= 3 && syncs == 3);
		} else {
			UT_ASSERT(!success && strstr(reason, "INITDB_CONFIG_CREATE:") != NULL);
			if (injection == OCCUPIED)
				UT_ASSERT(writes == 0 && reads == 0 && syncs == 0);
			if (injection == WRITE_FAIL)
				UT_ASSERT(writes == 1 && reads == 0 && syncs == 0);
			if (injection == FILE_SYNC)
				UT_ASSERT(syncs == 1 && reads == 0);
		}
		if (ut_current_failed)
			fflush(stdout);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
	images = openat(parent, "config_images", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(images >= 0);
	if (success || injection == OCCUPIED) {
		char bytes[8204];
		int fd = openat(images, object, O_RDONLY);
		ssize_t n = read(fd, bytes, sizeof(bytes));
		UT_ASSERT(fd >= 0);
		if (success) {
			UT_ASSERT(n == 8203);
			for (int i = 0; i < n; i++)
				UT_ASSERT(bytes[i] == 'a');
		} else
			UT_ASSERT(n == 4 && memcmp(bytes, "keep", 4) == 0);
		UT_ASSERT(close(fd) == 0);
	}
	unlinkat(images, object, 0);
	unlinkat(images, "old", 0);
	UT_ASSERT(close(images) == 0 && unlinkat(parent, "config_images", AT_REMOVEDIR) == 0);
	UT_ASSERT(close(parent) == 0 && rmdir(directory) == 0);
}

UT_TEST(real_persistence_and_exact_readback)
{
	run_case(NONE);
}
UT_TEST(partial_io_and_eintr_preserve_all_bytes)
{
	run_case(PARTIAL_IO);
}
UT_TEST(write_failure_never_reaches_sync)
{
	run_case(WRITE_FAIL);
}
UT_TEST(file_sync_failure_never_reaches_readback)
{
	run_case(FILE_SYNC);
}
UT_TEST(directory_sync_failure_refuses)
{
	run_case(DIRECTORY_SYNC);
}
UT_TEST(parent_sync_failure_refuses)
{
	run_case(ROOT_SYNC);
}
UT_TEST(corrupt_readback_refuses)
{
	run_case(CORRUPT);
}
UT_TEST(short_readback_refuses)
{
	run_case(SHORT_READ);
}
UT_TEST(extra_byte_refuses)
{
	run_case(EXTRA_BYTE);
}
UT_TEST(replaced_name_refuses)
{
	run_case(REPLACED);
}
UT_TEST(close_failure_refuses)
{
	run_case(CLOSE_FAIL);
}
UT_TEST(existing_object_is_unchanged)
{
	run_case(OCCUPIED);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(real_persistence_and_exact_readback);
	UT_RUN(partial_io_and_eintr_preserve_all_bytes);
	UT_RUN(write_failure_never_reaches_sync);
	UT_RUN(file_sync_failure_never_reaches_readback);
	UT_RUN(directory_sync_failure_refuses);
	UT_RUN(parent_sync_failure_refuses);
	UT_RUN(corrupt_readback_refuses);
	UT_RUN(short_readback_refuses);
	UT_RUN(extra_byte_refuses);
	UT_RUN(replaced_name_refuses);
	UT_RUN(close_failure_refuses);
	UT_RUN(existing_object_is_unchanged);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
