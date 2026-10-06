/* Original creator I/O failures with real files and the configured SHA256.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static jmp_buf refused;
static bool expect_error;
static char fixture_reason[256];
static int fixture_target = -1, fixture_parent = -1;
static char fixture_directory[MAXPGPATH];
enum Fault { NONE, SYNC, CORRUPT, SHORT_READ, WRITE, CLOSE };
static enum Fault fault;
static unsigned syncs, reads;

static int
test_fsync(int fd)
{
	++syncs;
	if (fd == fixture_target && fault == SYNC) {
		errno = EIO;
		return -1;
	}
	if (fd == fixture_target && fault == CORRUPT) {
		char changed = 'X';
		UT_ASSERT(pwrite(fd, &changed, 1, 99) == 1);
	}
	if (fd == fixture_target && fault == SHORT_READ)
		UT_ASSERT(ftruncate(fd, BLCKSZ - 1) == 0);
	return fsync(fd);
}

static ssize_t
test_pread(int fd, void *data, size_t size, off_t off)
{
	++reads;
	return pread(fd, data, size, off);
}

static ssize_t
test_pwrite(int fd, const void *data, size_t size, off_t off)
{
	if (fd == fixture_target && fault == WRITE) {
		errno = ENOSPC;
		return -1;
	}
	return pwrite(fd, data, size, off);
}

static int
test_close(int fd)
{
	int result = close(fd);
	if (fd == fixture_target && fault == CLOSE) {
		errno = EIO;
		return -1;
	}
	return result;
}

#define fsync test_fsync
#define pread test_pread
#define pwrite test_pwrite
#define close test_close
#include "../../backend/cluster/cluster_initdb_base.c"
#undef fsync
#undef pread
#undef pwrite
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
errmsg(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(fixture_reason, sizeof(fixture_reason), fmt, args);
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
/* This fixture tests syscall/refusal ordering only. TAP 004 checks actual
 * production page checksums; do not replace the production checksum there. */
void
PageSetChecksumInplace(Page page, BlockNumber block)
{}

static pg_cryptohash_ctx *
prepare(void)
{
	pg_cryptohash_ctx *digest = new_digest();
	PGAlignedBlock bytes;

	memset(bytes.data, 'a', BLCKSZ);
	strlcpy(fixture_directory, "/tmp/pgrac-initdb-base-XXXXXX", sizeof(fixture_directory));
	UT_ASSERT(mkdtemp(fixture_directory) != NULL);
	fixture_parent = open(fixture_directory, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fixture_parent >= 0);
	fixture_target = openat(fixture_parent, "data", O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fixture_target >= 0 && write(fixture_target, bytes.data, BLCKSZ) == BLCKSZ);
	UT_ASSERT(pg_cryptohash_update(digest, (uint8 *)bytes.data, BLCKSZ) == 0);
	fault = NONE;
	syncs = reads = 0;
	fixture_reason[0] = 0;
	return digest;
}

static void
cleanup(void)
{
	close(fixture_target);
	unlinkat(fixture_parent, "data", 0);
	unlinkat(fixture_parent, "old", 0);
	close(fixture_parent);
	UT_ASSERT(rmdir(fixture_directory) == 0);
	expect_error = false;
}

UT_TEST(persisted_bytes_read_back_before_success)
{
	pg_cryptohash_ctx *digest = prepare();
	sync_readback_close(fixture_parent, "data", fixture_target, 1, digest);
	UT_ASSERT(syncs == 1 && reads == 1);
	UT_ASSERT(fcntl(fixture_target, F_GETFD) == -1 && errno == EBADF);
	cleanup();
}

static void
failure(enum Fault injection, const char *message)
{
	pg_cryptohash_ctx *digest = prepare();
	fault = injection;
	expect_error = true;
	if (setjmp(refused) == 0) {
		sync_readback_close(fixture_parent, "data", fixture_target, 1, digest);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(fixture_reason, message) != NULL);
	UT_ASSERT(syncs == 1);
	if (injection == SYNC)
		UT_ASSERT(reads == 0);
	pg_cryptohash_free(digest);
	cleanup();
}

UT_TEST(fsync_failure_is_terminal)
{
	failure(SYNC, "persist original");
}
UT_TEST(changed_persisted_bytes_refuse)
{
	failure(CORRUPT, "bytes or identity");
}
UT_TEST(short_readback_refuses)
{
	failure(SHORT_READ, "read persisted");
}
UT_TEST(close_failure_refuses)
{
	failure(CLOSE, "bytes or identity");
}

UT_TEST(replaced_target_name_refuses)
{
	pg_cryptohash_ctx *digest = prepare();
	UT_ASSERT(renameat(fixture_parent, "data", fixture_parent, "old") == 0);
	UT_ASSERT(symlinkat("old", fixture_parent, "data") == 0);
	expect_error = true;
	if (setjmp(refused) == 0) {
		sync_readback_close(fixture_parent, "data", fixture_target, 1, digest);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(fixture_reason, "bytes or identity") != NULL);
	pg_cryptohash_free(digest);
	cleanup();
}

UT_TEST(data_write_failure_is_terminal)
{
	pg_cryptohash_ctx *digest = prepare();
	PGAlignedBlock page = { 0 };
	fault = WRITE;
	expect_error = true;
	if (setjmp(refused) == 0) {
		write_page(fixture_target, 0, page.data);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(fixture_reason, "write original target") != NULL);
	UT_ASSERT(syncs == 0 && reads == 0);
	pg_cryptohash_free(digest);
	cleanup();
}

UT_TEST(directory_is_persisted_and_closed)
{
	pg_cryptohash_ctx *digest = prepare();
	int fd;
	UT_ASSERT(mkdirat(fixture_parent, "child", 0700) == 0);
	fd = open_directory(fixture_parent, "child");
	sync_directory_close(fixture_parent, "child", fd);
	UT_ASSERT(syncs == 1 && fcntl(fd, F_GETFD) == -1 && errno == EBADF);
	UT_ASSERT(unlinkat(fixture_parent, "child", AT_REMOVEDIR) == 0);
	pg_cryptohash_free(digest);
	cleanup();
}

UT_TEST(replaced_directory_refuses)
{
	pg_cryptohash_ctx *digest = prepare();
	int fd;
	UT_ASSERT(mkdirat(fixture_parent, "child", 0700) == 0);
	fd = open_directory(fixture_parent, "child");
	UT_ASSERT(renameat(fixture_parent, "child", fixture_parent, "saved") == 0);
	UT_ASSERT(symlinkat("saved", fixture_parent, "child") == 0);
	expect_error = true;
	if (setjmp(refused) == 0) {
		sync_directory_close(fixture_parent, "child", fd);
		UT_ASSERT(false);
	}
	UT_ASSERT(strstr(fixture_reason, "directory identity") != NULL);
	UT_ASSERT(close(fd) == 0);
	UT_ASSERT(unlinkat(fixture_parent, "child", 0) == 0);
	UT_ASSERT(unlinkat(fixture_parent, "saved", AT_REMOVEDIR) == 0);
	pg_cryptohash_free(digest);
	cleanup();
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(persisted_bytes_read_back_before_success);
	UT_RUN(fsync_failure_is_terminal);
	UT_RUN(changed_persisted_bytes_refuse);
	UT_RUN(short_readback_refuses);
	UT_RUN(close_failure_refuses);
	UT_RUN(replaced_target_name_refuses);
	UT_RUN(data_write_failure_is_terminal);
	UT_RUN(directory_is_persisted_and_closed);
	UT_RUN(replaced_directory_refuses);
	UT_DONE();
}
