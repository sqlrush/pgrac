/* Native SIDE creation: real files, hash readback, syscall-only faults.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/logging.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
enum Fault { NONE, WRITE_FAIL, SYNC_FAIL, CORRUPT, SHORT_READ };
static enum Fault fault;
static int written_fd = -1;
static unsigned writes, file_syncs;
static char root[MAXPGPATH];
static int root_fd, source_fd, shared_fd;

static ssize_t
test_pwrite(int fd, const void *data, size_t size, off_t off)
{
	++writes;
	written_fd = fd;
	if (fault == WRITE_FAIL) { errno = ENOSPC; return -1; }
	return pwrite(fd, data, size, off);
}
static ssize_t
test_pread(int fd, void *data, size_t size, off_t off)
{
	if (fault == SHORT_READ && fd == written_fd) return 0;
	return pread(fd, data, size, off);
}
static int
test_fsync(int fd)
{
	struct stat st;
	UT_ASSERT(fstat(fd, &st) == 0);
	if (S_ISREG(st.st_mode))
	{
		++file_syncs;
		if (fault == SYNC_FAIL) { errno = EIO; return -1; }
		if (fault == CORRUPT)
		{
			char byte = 'X';
			UT_ASSERT(pwrite(fd, &byte, 1, 77) == 1);
		}
	}
	return fsync(fd);
}

#define pwrite test_pwrite
#define pread test_pread
#define fsync test_fsync
#include "../../bin/initdb/pgrac_side.c"
#undef pwrite
#undef pread
#undef fsync

static void
prepare(void)
{
	int dirs[7];
	PGAlignedBlock page;
	fault = NONE; writes = file_syncs = 0; written_fd = -1;
	memset(page.data, 'a', BLCKSZ);
	strlcpy(root, "/tmp/pgrac-initdb-side-XXXXXX", sizeof(root));
	UT_ASSERT(mkdtemp(root) != NULL);
	root_fd = open(root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(root_fd >= 0);
	UT_ASSERT(mkdirat(root_fd, "source", 0700) == 0 && mkdirat(root_fd, "shared", 0700) == 0);
	dirs[0] = source_fd = open_directory(root_fd, "source");
	shared_fd = open_directory(root_fd, "shared");
	UT_ASSERT(source_fd >= 0 && shared_fd >= 0);
	for (unsigned i = 0; i < lengthof(directories); ++i)
	{
		UT_ASSERT(mkdirat(dirs[parents[i]], directories[i], 0700) == 0);
		dirs[i + 1] = open_directory(dirs[parents[i]], directories[i]);
		UT_ASSERT(dirs[i + 1] >= 0);
		if (i != 2 && i != 3)
		{
			int fd = openat(dirs[i + 1], "0000", O_CREAT | O_EXCL | O_WRONLY, 0600);
			UT_ASSERT(fd >= 0 && write(fd, page.data, BLCKSZ) == BLCKSZ && close(fd) == 0);
		}
	}
	for (unsigned i = 1; i < lengthof(dirs); ++i) close(dirs[i]);
}

/* Only the tree made by this fixture; lstat never follows a test alias. */
static void
remove_contents(int fd)
{
	DIR *dir = fdopendir(openat(fd, ".", O_RDONLY | O_DIRECTORY));
	struct dirent *entry;
	UT_ASSERT(dir != NULL);
	while ((entry = readdir(dir)) != NULL)
	{
		struct stat st;
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
		UT_ASSERT(fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0);
		if (S_ISDIR(st.st_mode))
		{
			int child = openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
			UT_ASSERT(child >= 0);
			remove_contents(child);
			UT_ASSERT(close(child) == 0 && unlinkat(fd, entry->d_name, AT_REMOVEDIR) == 0);
		}
		else UT_ASSERT(unlinkat(fd, entry->d_name, 0) == 0);
	}
	UT_ASSERT(closedir(dir) == 0);
}
static void cleanup(void)
{
	UT_ASSERT(fcntl(source_fd, F_GETFD) >= 0 && fcntl(shared_fd, F_GETFD) >= 0);
	close(source_fd); close(shared_fd);
	remove_contents(root_fd); close(root_fd);
	UT_ASSERT(rmdir(root) == 0);
}
static bool target_absent(void)
{
	struct stat st;
	return fstatat(shared_fd, "native_side", &st, AT_SYMLINK_NOFOLLOW) != 0 && errno == ENOENT;
}

UT_TEST(actual_copy_sync_and_readback)
{
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	UT_ASSERT(writes == 4 && file_syncs == 4);
	UT_ASSERT(!target_absent());
	cleanup();
}
UT_TEST(existing_namespace_is_unchanged)
{
	struct stat st;
	prepare();
	UT_ASSERT(mkdirat(shared_fd, "native_side", 0700) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && writes == 0);
	UT_ASSERT(fstatat(shared_fd, "native_side/origin_0", &st, 0) != 0 && errno == ENOENT);
	cleanup();
}
UT_TEST(source_alias_refuses_before_mutation)
{
	prepare();
	UT_ASSERT(renameat(source_fd, "pg_xact/0000", source_fd, "original") == 0);
	UT_ASSERT(symlinkat("../original", source_fd, "pg_xact/0000") == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
UT_TEST(source_partial_page_refuses_before_mutation)
{
	int fd;
	prepare();
	fd = openat(source_fd, "pg_subtrans/0000", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, BLCKSZ - 1) == 0 && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
UT_TEST(unsupported_commit_ts_input_refuses)
{
	PGAlignedBlock page = {0};
	int fd;
	prepare();
	fd = openat(source_fd, "pg_commit_ts/0000", O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0 && write(fd, page.data, BLCKSZ) == BLCKSZ && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
static void io_failure(enum Fault value)
{
	prepare(); fault = value;
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd));
	UT_ASSERT(writes > 0);
	if (value != WRITE_FAIL) UT_ASSERT(file_syncs > 0);
	cleanup();
}
UT_TEST(write_failure_refuses) { io_failure(WRITE_FAIL); }
UT_TEST(fsync_failure_refuses) { io_failure(SYNC_FAIL); }
UT_TEST(changed_readback_refuses) { io_failure(CORRUPT); }
UT_TEST(short_readback_refuses) { io_failure(SHORT_READ); }

UT_TEST(peer_copies_its_own_original_source)
{
	int native, file;
	char byte;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0);
	file = openat(source_fd, "pg_xact/0000", O_WRONLY);
	UT_ASSERT(file >= 0 && pwrite(file, "b", 1, 0) == 1 && fsync(file) == 0 && close(file) == 0);
	UT_ASSERT(pgrac_initdb_side_origin_create(source_fd, native, 127));
	file = openat(native, "origin_127/pg_xact/0000", O_RDONLY);
	UT_ASSERT(file >= 0 && read(file, &byte, 1) == 1 && byte == 'b' && close(file) == 0);
	file = openat(native, "origin_0/pg_xact/0000", O_RDONLY);
	UT_ASSERT(file >= 0 && read(file, &byte, 1) == 1 && byte == 'a' && close(file) == 0);
	writes = 0;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 127) && writes == 0);
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 0) && writes == 0);
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 128) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

UT_TEST(peer_alias_is_not_adopted)
{
	int native;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0 && symlinkat("origin_0", native, "origin_1") == 0);
	writes = 0;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

UT_TEST(peer_failed_write_cannot_be_adopted)
{
	int native;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0);
	writes = 0; fault = SYNC_FAIL;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes > 0);
	writes = 0; fault = NONE;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

int main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(12);
	UT_RUN(actual_copy_sync_and_readback);
	UT_RUN(existing_namespace_is_unchanged);
	UT_RUN(source_alias_refuses_before_mutation);
	UT_RUN(source_partial_page_refuses_before_mutation);
	UT_RUN(unsupported_commit_ts_input_refuses);
	UT_RUN(write_failure_refuses);
	UT_RUN(fsync_failure_refuses);
	UT_RUN(changed_readback_refuses);
	UT_RUN(short_readback_refuses);
	UT_RUN(peer_copies_its_own_original_source);
	UT_RUN(peer_alias_is_not_adopted);
	UT_RUN(peer_failed_write_cannot_be_adopted);
	UT_DONE();
}
