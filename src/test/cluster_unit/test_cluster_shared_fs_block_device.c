/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_fs_block_device.c
 *	  Runtime unit tests for spec-6.0a raw block_device backend.
 *
 *	  Uses a regular-file device image with O_DIRECT disabled to exercise
 *	  the raw provider's layout initialization, extent allocation, logical
 *	  EOF checks, WAL emit path, truncate fail-closed guard, reopen, barrier
 *	  sync, fence-surface reporting, and unlink behavior without starting a
 *	  PostgreSQL postmaster.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_fs_block_device.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, raw block_device conformance unit).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/storage/cluster_raw_xlog.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_block_device_path = NULL;
bool cluster_block_device_use_odirect = false;
int cluster_storage_fence_driver = CLUSTER_STORAGE_FENCE_DRIVER_AUTO;
char *cluster_shared_storage_uuid = NULL;
ClusterConf *ClusterConfShmem = NULL;
PGPROC *MyProc = NULL;

MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
bool IsUnderPostmaster = false;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

static jmp_buf error_jmp;
static bool expect_error = false;
static int last_elevel = 0;
static uint64 raw_wal_emit_count = 0;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	last_elevel = elevel;
	if (elevel >= ERROR)
		return true;
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (last_elevel >= ERROR && expect_error)
		longjmp(error_jmp, 1);
	if (last_elevel >= ERROR)
		abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
elog_start(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		   const char *funcname pg_attribute_unused())
{}

void
elog_finish(int elevel pg_attribute_unused(), const char *fmt pg_attribute_unused(), ...)
{}

void
pre_format_elog_string(int errnumber pg_attribute_unused(),
					   const char *domain pg_attribute_unused())
{}
char *
format_elog_string(const char *fmt pg_attribute_unused(), ...)
{
	return NULL;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}
void
pfree(void *pointer)
{
	free(pointer);
}

File
PathNameOpenFile(const char *fileName, int fileFlags)
{
	return (File)open(fileName, fileFlags, 0600);
}

void
FileClose(File file)
{
	close((int)file);
}

int
FileRead(File f, void *b, size_t a, off_t o, uint32 w pg_attribute_unused())
{
	return (int)pread((int)f, b, a, o);
}

int
FileWrite(File f, const void *b, size_t a, off_t o, uint32 w pg_attribute_unused())
{
	return (int)pwrite((int)f, b, a, o);
}

int
FileSync(File f, uint32 w pg_attribute_unused())
{
	return fsync((int)f);
}

off_t
FileSize(File f)
{
	struct stat st;

	if (fstat((int)f, &st) != 0)
		return -1;
	return st.st_size;
}

int
FileTruncate(File f, off_t o, uint32 w pg_attribute_unused())
{
	return ftruncate((int)f, o);
}

int
FileGetRawDesc(File file)
{
	return (int)file;
}

XLogRecPtr
cluster_raw_layout_emit_write(uint64 offset pg_attribute_unused(),
							  const char *image pg_attribute_unused())
{
	raw_wal_emit_count++;
	return raw_wal_emit_count;
}

void
XLogFlush(XLogRecPtr record pg_attribute_unused())
{}

bool
XLogInsertAllowed(void)
{
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
}

static pg_crc32c
sw_crc32c(pg_crc32c crc, const void *data, size_t len)
{
	const unsigned char *p = (const unsigned char *)data;

	while (len--) {
		int i;

		crc ^= *p++;
		for (i = 0; i < 8; i++)
			crc = (crc >> 1) ^ (0x82F63B78 & (0 - (crc & 1)));
	}
	return crc;
}

extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

pg_crc32c
pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len)
{
	return sw_crc32c(crc, data, len);
}

pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len)
{
	return sw_crc32c(crc, data, len);
}

pg_crc32c (*pg_comp_crc32c)(pg_crc32c crc, const void *data, size_t len) = sw_crc32c;

static bool
read_past_eof_errors(ClusterSharedFsHandle *handle)
{
	char buf[BLCKSZ];

	expect_error = true;
	if (setjmp(error_jmp) == 0) {
		cluster_shared_fs_block_device_ops.read(handle, 130, buf);
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}

static bool
truncate_extend_errors(const ClusterSharedFsOps *ops, ClusterSharedFsHandle *handle)
{
	expect_error = true;
	if (setjmp(error_jmp) == 0) {
		ops->truncate(handle, 2);
		expect_error = false;
		return false;
	}
	expect_error = false;
	return true;
}

UT_TEST(test_block_device_roundtrip_layout_and_eof)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_block_device_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 60001 };
	ClusterSharedFsHandle *handle = NULL;
	char path[256];
	char in0[BLCKSZ];
	char in130[BLCKSZ];
	char out[BLCKSZ];
	int fd;

	snprintf(path, sizeof(path), "/tmp/pgrac_raw_backend_ut_%d.dat", (int)getpid());
	fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, 8 * 1024 * 1024), 0);
	close(fd);

	cluster_block_device_path = path;
	cluster_block_device_use_odirect = false;
	cluster_storage_fence_driver = CLUSTER_STORAGE_FENCE_DRIVER_AUTO;
	cluster_shared_storage_uuid = "raw-ut-storage";

	UT_ASSERT_NOT_NULL((void *)ops->caps);
	UT_ASSERT_EQ(ops->caps->durability_class, CLUSTER_DURABILITY_ODIRECT_BARRIER);
	ops->init();

	raw_wal_emit_count = 0;
	UT_ASSERT(!ops->exists(rl, MAIN_FORKNUM));
	ops->create(rl, MAIN_FORKNUM, false, &handle);
	UT_ASSERT_NOT_NULL(handle);
	UT_ASSERT(ops->exists(rl, MAIN_FORKNUM));
	UT_ASSERT_EQ(ops->nblocks(handle), 0);
	UT_ASSERT(raw_wal_emit_count > 0);

	memset(in0, 0x5a, sizeof(in0));
	ops->extend(handle, 0);
	ops->write(handle, 0, in0);
	UT_ASSERT_EQ(ops->nblocks(handle), 1);
	memset(out, 0, sizeof(out));
	ops->read(handle, 0, out);
	UT_ASSERT_EQ(memcmp(in0, out, BLCKSZ), 0);

	memset(in130, 0xc3, sizeof(in130));
	ops->extend(handle, 130);
	ops->write(handle, 130, in130);
	UT_ASSERT_EQ(ops->nblocks(handle), 131);
	memset(out, 0, sizeof(out));
	ops->read(handle, 130, out);
	UT_ASSERT_EQ(memcmp(in130, out, BLCKSZ), 0);

	ops->truncate(handle, 1);
	UT_ASSERT_EQ(ops->nblocks(handle), 1);
	UT_ASSERT(read_past_eof_errors(handle));
	UT_ASSERT(truncate_extend_errors(ops, handle));

	UT_ASSERT_EQ(ops->barrier_sync(handle), 0);
	UT_ASSERT_EQ(ops->fence_capability(), CLUSTER_FENCE_CAP_NONE);
	UT_ASSERT_NE(ops->register_fence_key(0), 0);
	ops->close(handle);

	ops->open_existing(rl, MAIN_FORKNUM, &handle);
	memset(out, 0, sizeof(out));
	ops->read(handle, 0, out);
	UT_ASSERT_EQ(memcmp(in0, out, BLCKSZ), 0);
	ops->close(handle);

	ops->unlink(rl, MAIN_FORKNUM);
	UT_ASSERT(!ops->exists(rl, MAIN_FORKNUM));
	ops->shutdown();
	unlink(path);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_block_device_roundtrip_layout_and_eof);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
