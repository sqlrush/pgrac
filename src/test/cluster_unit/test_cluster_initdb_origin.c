/* Immutable origin object I/O uses real files; faults intercept syscalls.
 * The native codecs/checkpoint are exercised end to end by initdb TAP 006.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"
UT_DEFINE_GLOBALS();

enum Fault { NONE, PARTIAL, WRITE_FAIL, FILE_SYNC, DIR_SYNC, CORRUPT,
	SHORT_READ, EXTRA, REPLACED, CLOSE_FAIL, OCCUPIED, ALIAS, UNSAFE };
static enum Fault fault;
static int parent, target;
static unsigned writes, reads;
static char directory[MAXPGPATH];

static ssize_t
fault_write(int fd, const void *bytes, size_t length, off_t offset)
{
	target = fd;
	writes++;
	if (fault == WRITE_FAIL) { errno = ENOSPC; return -1; }
	if (fault == PARTIAL && writes == 1) { errno = EINTR; return -1; }
	return pwrite(fd, bytes, fault == PARTIAL ? Min(length, 71) : length, offset);
}

static ssize_t
fault_read(int fd, void *bytes, size_t length, off_t offset)
{
	reads++;
	if (fault == SHORT_READ) return 0;
	if (fault == PARTIAL && reads == 1) { errno = EINTR; return -1; }
	return pread(fd, bytes, fault == PARTIAL ? Min(length, 89) : length, offset);
}

static int
fault_sync(int fd)
{
	if ((fault == FILE_SYNC && fd == target) || (fault == DIR_SYNC && fd == parent))
	{ errno = EIO; return -1; }
	if (fd == target && (fault == CORRUPT || fault == EXTRA))
		UT_ASSERT(pwrite(fd, "X", 1, fault == CORRUPT ? 78 : 512) == 1);
	if (fd == target && fault == REPLACED)
	{
		int replacement;
		UT_ASSERT(renameat(parent, "object", parent, "old") == 0);
		replacement = openat(parent, "object", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(replacement >= 0 && close(replacement) == 0);
	}
	return fsync(fd);
}

static int
fault_close(int fd)
{
	int result = close(fd);
	if (fd == target && fault == CLOSE_FAIL) { errno = EIO; return -1; }
	return result;
}

#define pwrite fault_write
#define pread fault_read
#define fsync fault_sync
#define close fault_close
#include "../../backend/cluster/cluster_initdb_origin.c"
#undef pwrite
#undef pread
#undef fsync
#undef close

void ExceptionalCondition(const char *condition, const char *file, int line) { abort(); }

static void
exercise(enum Fault value)
{
	uint8 bytes[512], observed[513];
	int fd;
	fault = value; writes = reads = 0; target = -1;
	strlcpy(directory, "/tmp/pgrac-initdb-origin-XXXXXX", sizeof(directory));
	UT_ASSERT(mkdtemp(directory) != NULL);
	parent = open(directory, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(parent >= 0);
	memset(bytes, 0x6c, sizeof(bytes));
	if (fault == OCCUPIED || fault == ALIAS)
	{
		fd = openat(parent, fault == ALIAS ? "original" : "object", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, "keep", 4) == 4 && close(fd) == 0);
		if (fault == ALIAS) UT_ASSERT(symlinkat("original", parent, "object") == 0);
	}
	if (fault == UNSAFE) UT_ASSERT(fchmod(parent, 0777) == 0);
	UT_ASSERT(cluster_initdb_object_write_new(parent, "object", bytes, sizeof(bytes)) == (fault == NONE || fault == PARTIAL));
	if (fault == NONE || fault == PARTIAL)
	{
		fd = openat(parent, "object", O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, observed, sizeof(observed)) == sizeof(bytes));
		UT_ASSERT(memcmp(observed, bytes, sizeof(bytes)) == 0 && close(fd) == 0);
	}
	if (fault == OCCUPIED || fault == ALIAS)
	{
		UT_ASSERT(writes == 0);
		fd = openat(parent, "object", O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, observed, sizeof(observed)) == 4);
		UT_ASSERT(memcmp(observed, "keep", 4) == 0 && close(fd) == 0);
	}
	if (fault == UNSAFE) UT_ASSERT(writes == 0 && fchmod(parent, 0700) == 0);
	(void)unlinkat(parent, "object", 0);
	(void)unlinkat(parent, "original", 0);
	(void)unlinkat(parent, "old", 0);
	UT_ASSERT(close(parent) == 0 && rmdir(directory) == 0);
}

#define CASE(name, value) static void name(void) { exercise(value); }
CASE(success, NONE)
CASE(partial_io, PARTIAL)
CASE(write_fail, WRITE_FAIL)
CASE(file_sync, FILE_SYNC)
CASE(directory_sync, DIR_SYNC)
CASE(corruption, CORRUPT)
CASE(short_read, SHORT_READ)
CASE(extra_byte, EXTRA)
CASE(replacement, REPLACED)
CASE(close_failure, CLOSE_FAIL)
CASE(occupied, OCCUPIED)
CASE(alias, ALIAS)
CASE(unsafe, UNSAFE)

int main(void)
{
	UT_PLAN(13);
	UT_RUN(success); UT_RUN(partial_io); UT_RUN(write_fail);
	UT_RUN(file_sync); UT_RUN(directory_sync); UT_RUN(corruption);
	UT_RUN(short_read); UT_RUN(extra_byte); UT_RUN(replacement);
	UT_RUN(close_failure); UT_RUN(occupied); UT_RUN(alias); UT_RUN(unsafe);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
