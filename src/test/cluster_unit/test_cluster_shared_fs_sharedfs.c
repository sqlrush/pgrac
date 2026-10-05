/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_fs_sharedfs.c
 *	  Runtime unit tests for the shared_fs storage backend + cross-node
 *	  shared-root sentinel (spec-4.5a D1/D2, deliverable D12).
 *
 *	  Unlike test_cluster_shared_fs.c (link-level vtable invariants),
 *	  this binary exercises cluster_shared_fs_sharedfs.o for REAL against
 *	  a temp directory: the fd.c VFD stubs map straight onto open(2)/
 *	  pread(2)/pwrite(2), the CRC stub is a correct software CRC32C, and
 *	  palloc0/psprintf are functional.  No server is needed.
 *
 *	  Covers:
 *	    - 13-callback round trip on a real file under the shared root
 *	      (create -> write -> read -> nblocks -> truncate -> immedsync
 *	       -> close -> exists -> unlink)
 *	    - owner-agnostic path resolution: a second handle (the "other
 *	      node") opened from the same RelFileLocator reads the bytes the
 *	      first wrote, and the on-disk path is literally
 *	      <root>/base/<db>/<relfile>
 *	    - sentinel attach / has_participant: self recorded, stranger
 *	      absent, idempotent re-attach, second node joins, corrupt
 *	      sentinel fails CLOSED (has_participant = false), preset
 *	      storage uuid is recorded verbatim
 *
 *	  NOT covered here (TAP t/248 territory): FATAL paths (uuid
 *	  mismatch, corrupt-on-attach), the merged-recovery capability gate,
 *	  and true two-node merged replay.  The errstart stub aborts on
 *	  elevel >= ERROR so any unexpected error path fails this binary
 *	  loudly instead of silently falling through.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_fs_sharedfs.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D12)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cluster/storage/cluster_shared_fs.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/relfilelocator.h"
#include "utils/elog.h"

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
#include "test_cluster_drop_work_unavailable.h"
#include "storage/sync.h"

bool
RegisterSyncRequest(const FileTag *tag pg_attribute_unused(),
					SyncRequestType type pg_attribute_unused(), bool retry pg_attribute_unused())
{
	return false;
}


/* ----------
 * GUC storages + globals read by cluster_shared_fs_sharedfs.o.
 * ----------
 */
char *cluster_shared_data_dir = NULL;
char *cluster_shared_storage_uuid = NULL;
int cluster_node_id = 0;
bool enableFsync = true;
bool cluster_shared_catalog = false;
bool IsUnderPostmaster = false;
int io_direct_flags = 0;
static int last_open_flags;
static int last_open_fd;
static bool direct_fds[4096];
static int prefetch_calls;
static int writeback_calls;
static int sync_calls;
static int open_calls;
static int io_fault;
static bool reject_direct_open;
static const void *last_io_buffer;

/* Scripted FileSize/pg_usleep surface for the concurrent-extend EOF tests. */
static off_t file_size_script[8];
static int file_size_script_len = 0;
static int file_size_script_pos = 0;
static bool file_size_script_repeat_last = false;
static int file_size_call_count = 0;
static int pg_usleep_call_count = 0;

/* Cluster injection support (CLUSTER_INJECTION_POINT() expansion). */
#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ----------
 * ereport machinery.  elevel >= ERROR aborts: production code assumes
 * ereport(ERROR/FATAL) does not return, so silently continuing would
 * corrupt the test.  Lower levels (LOG in sentinel_attach) are no-ops.
 * ----------
 */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		printf("# unexpected ereport(elevel=%d) -- aborting\n", elevel);
		abort();
	}
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
{}

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

/* ----------
 * Functional memory + string stubs.
 * ----------
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;

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

char *
pstrdup(const char *in)
{
	return strdup(in);
}

char *
psprintf(const char *fmt, ...)
{
	char buf[4096];
	va_list args;
	int n;

	va_start(args, fmt);
	n = vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);
	if (n < 0 || n >= (int)sizeof(buf))
		abort(); /* test paths never exceed 4k */
	return strdup(buf);
}

/* ----------
 * Functional fd.c VFD stubs: File IS the kernel fd.
 * ----------
 */
File
PathNameOpenFile(const char *fileName, int fileFlags)
{
	last_open_flags = fileFlags;
	open_calls++;
	if (reject_direct_open && (fileFlags & PG_O_DIRECT)) {
		errno = EINVAL;
		return -1;
	}
#ifdef PG_O_DIRECT_USE_F_NOCACHE
	fileFlags &= ~PG_O_DIRECT;
#endif
	last_open_fd = open(fileName, fileFlags, 0600);
	if (last_open_fd >= 0) {
		if (last_open_fd >= lengthof(direct_fds))
			abort();
		direct_fds[last_open_fd] = (last_open_flags & PG_O_DIRECT) != 0;
#ifdef PG_O_DIRECT_USE_F_NOCACHE
		if (direct_fds[last_open_fd] && fcntl(last_open_fd, F_NOCACHE, 1) < 0)
			abort();
#endif
	}
	return last_open_fd;
}

void
FileClose(File file)
{
	close((int)file);
}

int
FileRead(File f, void *b, size_t a, off_t o, uint32 w pg_attribute_unused())
{
	last_io_buffer = b;
	if (direct_fds[f]
		&& ((uintptr_t)b % PG_IO_ALIGN_SIZE || a % PG_IO_ALIGN_SIZE || o % PG_IO_ALIGN_SIZE)) {
		errno = EINVAL;
		return -1;
	}
	if (io_fault == 1) {
		memset(b, 0x17, a / 2);
		return a / 2;
	}
	if (io_fault == 2) {
		errno = EIO;
		return -1;
	}
	return (int)pread((int)f, b, a, o);
}

int
FileWrite(File f, const void *b, size_t a, off_t o, uint32 w pg_attribute_unused())
{
	last_io_buffer = b;
	if (direct_fds[f]
		&& ((uintptr_t)b % PG_IO_ALIGN_SIZE || a % PG_IO_ALIGN_SIZE || o % PG_IO_ALIGN_SIZE)) {
		errno = EINVAL;
		return -1;
	}
	if (io_fault == 3)
		return a / 2;
	if (io_fault == 4) {
		errno = EIO;
		return -1;
	}
	return (int)pwrite((int)f, b, a, o);
}

int
FileSync(File f, uint32 w pg_attribute_unused())
{
	sync_calls++;
	if (io_fault == 5) {
		errno = EIO;
		return -1;
	}
	return fsync((int)f);
}

int
FilePrefetch(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
			 off_t a pg_attribute_unused(), uint32 w pg_attribute_unused())
{
	prefetch_calls++;
	return 0;
}

void
FileWriteback(File f pg_attribute_unused(), off_t o pg_attribute_unused(),
			  off_t a pg_attribute_unused(), uint32 w pg_attribute_unused())
{
	writeback_calls++;
}

off_t
FileSize(File f)
{
	struct stat st;

	file_size_call_count++;
	if (file_size_script_len > 0) {
		int pos = file_size_script_pos;

		if (pos >= file_size_script_len) {
			if (!file_size_script_repeat_last)
				abort();
			pos = file_size_script_len - 1;
		} else
			file_size_script_pos++;
		return file_size_script[pos];
	}

	if (fstat((int)f, &st) != 0)
		return -1;
	return st.st_size;
}

void
pg_usleep(long microsec pg_attribute_unused())
{
	pg_usleep_call_count++;
}

int
FileTruncate(File f, off_t o, uint32 w pg_attribute_unused())
{
	return ftruncate((int)f, o);
}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return open(fileName, fileFlags, 0600);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

/*
 * Minimal GetRelationPath: the sharedfs relpath helper goes through
 * relpathperm -> GetRelationPath.  The tests use permanent MAIN/FSM forks
 * in the default tablespace (mirrors common/relpath.c for these cases).
 */
char *
GetRelationPath(Oid dbOid, Oid spcOid pg_attribute_unused(), RelFileNumber relNumber,
				int backendId pg_attribute_unused(), ForkNumber forkNumber)
{
	const char *suffixes[] = { "", "_fsm", "_vm", "_init", "_space" };

	if (forkNumber < MAIN_FORKNUM || forkNumber > SPACE_FORKNUM)
		abort();
	return psprintf("base/%u/%u%s", dbOid, relNumber, suffixes[forkNumber]);
}

int pg_dir_create_mode = 0700;

int
pg_mkdir_p(char *path, int omode)
{
	char *p;

	for (p = path + 1; *p; p++) {
		if (*p == '/') {
			*p = '\0';
			if (mkdir(path, (mode_t)omode) != 0 && errno != EEXIST) {
				*p = '/';
				return -1;
			}
			*p = '/';
		}
	}
	if (mkdir(path, (mode_t)omode) != 0 && errno != EEXIST)
		return -1;
	return 0;
}

bool
pg_strong_random(void *buf, size_t len)
{
	int fd = open("/dev/urandom", O_RDONLY);
	unsigned char *p = (unsigned char *)buf;

	if (fd >= 0) {
		ssize_t n = read(fd, buf, len);

		close(fd);
		if (n == (ssize_t)len)
			return true;
	}
	while (len--)
		*p++ = (unsigned char)(rand() & 0xff);
	return true;
}

/*
 * Correct software CRC32C (Castagnoli, reflected, poly 0x82F63B78).  The
 * sentinel round-trip + corruption-detection tests need a REAL CRC -- an
 * identity stub would let a corrupted payload pass validation.
 */
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


UT_DEFINE_GLOBALS();


/* ----------
 * Layout mirror of the production PgracSharedControl (private to
 * cluster_shared_fs_sharedfs.c).  Drift in the production layout makes
 * the uuid / participant probes below fail loudly, which is the point:
 * the on-disk sentinel format is part of the D2 contract.
 * ----------
 */
#define MIRROR_MAX_NODES 128
typedef struct MirrorSharedControl {
	uint32 magic;
	uint32 layout_version;
	char storage_uuid[33];
	char _pad[3];
	uint32 participant_count;
	int32 participant_node_ids[MIRROR_MAX_NODES];
	pg_crc32c crc;
} MirrorSharedControl;

static char test_root[256];
static char sentinel_path[512];

static void
fresh_root(const char *tag)
{
	static char rootbuf[256];

	snprintf(rootbuf, sizeof(rootbuf), "/tmp/pgrac_sharedfs_ut_%d_%s", (int)getpid(), tag);
	snprintf(test_root, sizeof(test_root), "%s", rootbuf);
	(void)pg_mkdir_p(test_root, 0700);
	snprintf(sentinel_path, sizeof(sentinel_path), "%s/pgrac_shared.control", test_root);
	cluster_shared_data_dir = test_root;
}

static bool
read_mirror(MirrorSharedControl *out)
{
	int fd = open(sentinel_path, O_RDONLY);
	ssize_t n;

	if (fd < 0)
		return false;
	n = pread(fd, out, sizeof(*out), 0);
	close(fd);
	return n == (ssize_t)sizeof(*out);
}

static void
set_file_size_script(const off_t *values, int count, bool repeat_last)
{
	UT_ASSERT(count > 0);
	UT_ASSERT(count <= lengthof(file_size_script));
	memcpy(file_size_script, values, sizeof(*values) * count);
	file_size_script_len = count;
	file_size_script_pos = 0;
	file_size_script_repeat_last = repeat_last;
	file_size_call_count = 0;
	pg_usleep_call_count = 0;
}

static void
clear_file_size_script(void)
{
	file_size_script_len = 0;
	file_size_script_pos = 0;
	file_size_script_repeat_last = false;
}


/* ============================================================
 * Backend I/O round trip (D1)
 * ============================================================ */

UT_TEST(test_sharedfs_roundtrip_and_owner_agnostic)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24576 };
	ClusterSharedFsHandle *h = NULL;
	ClusterSharedFsHandle *h_other_node = NULL;
	char blk_a[BLCKSZ];
	char blk_b[BLCKSZ];
	char readback[BLCKSZ];
	char literal_path[600];
	struct stat st;

	fresh_root("io");

	memset(blk_a, 0xA5, sizeof(blk_a));
	memset(blk_b, 0x5A, sizeof(blk_b));

	/* exists -> create (parent dirs auto-created under the root). */
	UT_ASSERT(!ops->exists(rl, MAIN_FORKNUM));
	ops->create(rl, MAIN_FORKNUM, false, &h);
	UT_ASSERT_NOT_NULL(h);
	UT_ASSERT(ops->exists(rl, MAIN_FORKNUM));

	/* The on-disk path is literally <root>/base/<db>/<relfile>. */
	snprintf(literal_path, sizeof(literal_path), "%s/base/5/24576", test_root);
	UT_ASSERT_EQ(stat(literal_path, &st), 0);

	/* write 2 blocks -> nblocks -> read back. */
	UT_ASSERT_EQ(ops->write(h, 0, blk_a), BLCKSZ);
	UT_ASSERT_EQ(ops->write(h, 1, blk_b), BLCKSZ);
	UT_ASSERT_EQ((int)ops->nblocks(h), 2);
	memset(readback, 0, sizeof(readback));
	UT_ASSERT_EQ(ops->read(h, 0, readback), BLCKSZ);
	UT_ASSERT_EQ(memcmp(readback, blk_a, BLCKSZ), 0);

	/*
	 * Owner-agnostic: a SECOND handle resolved from the same locator (what
	 * the other node's backend would do) reads the bytes the first wrote.
	 */
	ops->open_existing(rl, MAIN_FORKNUM, &h_other_node);
	UT_ASSERT_NOT_NULL(h_other_node);
	memset(readback, 0, sizeof(readback));
	UT_ASSERT_EQ(ops->read(h_other_node, 1, readback), BLCKSZ);
	UT_ASSERT_EQ(memcmp(readback, blk_b, BLCKSZ), 0);
	ops->close(h_other_node);

	/* truncate -> immedsync -> close -> unlink. */
	ops->truncate(h, 1);
	UT_ASSERT_EQ((int)ops->nblocks(h), 1);
	ops->immedsync(h);
	ops->close(h);
	UT_ASSERT(ops->exists(rl, MAIN_FORKNUM));
	ops->unlink(rl, MAIN_FORKNUM);
	UT_ASSERT(!ops->exists(rl, MAIN_FORKNUM));
}

UT_TEST(test_shared_catalog_create_rejects_existing_main)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 30000 };
	ClusterSharedFsHandle *h = NULL;
	char original[BLCKSZ], actual[BLCKSZ];
	pid_t child;
	int status = 0;

	fresh_root("exclusive_create");
	cluster_shared_catalog = true;
	ops->create(rl, MAIN_FORKNUM, false, &h);
	memset(original, 0x5A, sizeof(original));
	UT_ASSERT_EQ(ops->write(h, 0, original), BLCKSZ);
	ops->close(h);
	h = NULL;
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		ops->create(rl, MAIN_FORKNUM, false, &h);
		_exit(0);
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFSIGNALED(status));
	if (WIFSIGNALED(status))
		UT_ASSERT_EQ(WTERMSIG(status), SIGABRT);
	/* Redo keeps its existing contract; failed CREATE changed no old bytes. */
	ops->create(rl, MAIN_FORKNUM, true, &h);
	UT_ASSERT_EQ(ops->read(h, 0, actual), BLCKSZ);
	UT_ASSERT_EQ(memcmp(original, actual, BLCKSZ), 0);
	ops->close(h);
	h = NULL;
	/* Advisory FSM creation is for this same relation, not a new identity. */
	ops->create(rl, FSM_FORKNUM, false, &h);
	UT_ASSERT_EQ(ops->write(h, 0, original), BLCKSZ);
	ops->close(h);
	h = NULL;
	ops->create(rl, FSM_FORKNUM, false, &h);
	UT_ASSERT_EQ(ops->read(h, 0, actual), BLCKSZ);
	UT_ASSERT_EQ(memcmp(original, actual, BLCKSZ), 0);
	ops->close(h);
	ops->unlink(rl, FSM_FORKNUM);
	h = NULL;
	cluster_shared_catalog = false;
	ops->create(rl, MAIN_FORKNUM, false, &h);
	UT_ASSERT_EQ(ops->read(h, 0, actual), BLCKSZ);
	UT_ASSERT_EQ(memcmp(original, actual, BLCKSZ), 0);
	ops->close(h);
	ops->unlink(rl, MAIN_FORKNUM);
}

UT_TEST(test_sharedfs_extend_zero_fills)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24577 };
	ClusterSharedFsHandle *h = NULL;
	char readback[BLCKSZ];
	char zero[BLCKSZ];
	int i;

	memset(zero, 0, sizeof(zero));
	ops->create(rl, MAIN_FORKNUM, false, &h);
	for (i = 0; i < 3; i++)
		ops->extend(h, i);
	UT_ASSERT_EQ((int)ops->nblocks(h), 3);
	memset(readback, 0xFF, sizeof(readback));
	UT_ASSERT_EQ(ops->read(h, 2, readback), BLCKSZ);
	UT_ASSERT_EQ(memcmp(readback, zero, BLCKSZ), 0);
	ops->close(h);
	ops->unlink(rl, MAIN_FORKNUM);
}

/* An 8 KiB pwrite that extends a regular file can expose its first 4 KiB
 * page through i_size while the syscall is still completing.  nblocks must
 * re-sample that in-flight tail rather than classify it as durable damage. */
UT_TEST(test_nblocks_rechecks_transient_partial_extend)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24578 };
	ClusterSharedFsHandle *h = NULL;
	const off_t sizes[] = { BLCKSZ + BLCKSZ / 2, 2 * BLCKSZ };
	BlockNumber nblocks;

	fresh_root("nblocks_transient");
	ops->create(rl, MAIN_FORKNUM, false, &h);
	set_file_size_script(sizes, lengthof(sizes), false);
	nblocks = ops->nblocks(h);

	UT_ASSERT_EQ((int)nblocks, 2);
	UT_ASSERT_EQ(file_size_call_count, 2);
	UT_ASSERT_EQ(pg_usleep_call_count, 1);
	clear_file_size_script();
	ops->close(h);
	ops->unlink(rl, MAIN_FORKNUM);
}

/* Revalidation is bounded and cannot turn a persistent partial block into a
 * rounded-down success.  The test error shim records the production ERROR and
 * lets this standalone process inspect the exhausted retry surface. */
UT_TEST(test_nblocks_persistent_partial_tail_stays_failclosed)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24579 };
	ClusterSharedFsHandle *h = NULL;
	const off_t sizes[] = { BLCKSZ + BLCKSZ / 2 };
	pid_t child;
	int status = 0;

	fresh_root("nblocks_persistent");
	ops->create(rl, MAIN_FORKNUM, false, &h);
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		set_file_size_script(sizes, lengthof(sizes), true);
		(void)ops->nblocks(h);
		_exit(0);
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFSIGNALED(status));
	UT_ASSERT_EQ(WTERMSIG(status), SIGABRT);
	clear_file_size_script();
	ops->close(h);
	ops->unlink(rl, MAIN_FORKNUM);
}


/* ============================================================
 * Cross-node shared-root sentinel (D2)
 * ============================================================ */

UT_TEST(test_sentinel_attach_records_self)
{
	fresh_root("sent");
	cluster_node_id = 3;
	cluster_shared_storage_uuid = NULL;

	cluster_shared_fs_sentinel_attach();

	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(3));
	UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(7));
}

UT_TEST(test_sentinel_attach_idempotent)
{
	MirrorSharedControl m;

	/* Same node attaches again: still one participant entry. */
	cluster_node_id = 3;
	cluster_shared_fs_sentinel_attach();

	UT_ASSERT(read_mirror(&m));
	UT_ASSERT_EQ(m.magic, 0x50475343);
	UT_ASSERT_EQ(m.layout_version, 1);
	UT_ASSERT_EQ((int)m.participant_count, 1);
	UT_ASSERT_EQ(m.participant_node_ids[0], 3);
}

UT_TEST(test_sentinel_second_node_joins)
{
	MirrorSharedControl m;

	cluster_node_id = 7; /* "the other node" attaches to the same root */
	cluster_shared_fs_sentinel_attach();

	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(3));
	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(7));
	UT_ASSERT(read_mirror(&m));
	UT_ASSERT_EQ((int)m.participant_count, 2);
}

UT_TEST(test_sentinel_corrupt_fails_closed)
{
	int fd;
	unsigned char byte;

	/* Flip one uuid byte: CRC breaks, participation can no longer be
	 * proven, has_participant must say NO (fail-closed) -- without
	 * raising any error (the capability gate turns this into a 53RA3
	 * blocker, not a crash). */
	fd = open(sentinel_path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ((int)pread(fd, &byte, 1, 8), 1);
	byte ^= 0xFF;
	UT_ASSERT_EQ((int)pwrite(fd, &byte, 1, 8), 1);
	close(fd);

	UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(3));
	UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(7));

	/* Restore the byte: participation is provable again. */
	fd = open(sentinel_path, O_RDWR);
	byte ^= 0xFF;
	UT_ASSERT_EQ((int)pwrite(fd, &byte, 1, 8), 1);
	close(fd);
	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(3));
}

UT_TEST(test_sentinel_preset_uuid_recorded)
{
	MirrorSharedControl m;
	static char preset[] = "cafebabe00112233445566778899aabb";

	fresh_root("uuid");
	cluster_node_id = 2;
	cluster_shared_storage_uuid = preset;

	cluster_shared_fs_sentinel_attach();

	UT_ASSERT(read_mirror(&m));
	UT_ASSERT_STR_EQ(m.storage_uuid, preset);
	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(2));
	cluster_shared_storage_uuid = NULL;
}

UT_TEST(test_sentinel_missing_file_fails_closed)
{
	fresh_root("none");
	/* No attach ever happened on this root: nobody is a participant. */
	UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(0));
	UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(3));
}

UT_TEST(test_sentinel_uuid_from_shared_configuration)
{
	MirrorSharedControl m;
	static char formatted[] = "01234567-89ab-cdef-0123-456789abcdef";
	static char compact[] = "0123456789abcdef0123456789abcdef";

	fresh_root("shared_config_uuid");
	cluster_node_id = 0;
	cluster_shared_storage_uuid = formatted;
	cluster_shared_fs_sentinel_attach();
	UT_ASSERT(read_mirror(&m));
	UT_ASSERT_STR_EQ(m.storage_uuid, compact);
	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(0));
	cluster_node_id = 1;
	cluster_shared_storage_uuid = compact;
	cluster_shared_fs_sentinel_attach();
	UT_ASSERT(read_mirror(&m));
	UT_ASSERT_STR_EQ(m.storage_uuid, compact);
	UT_ASSERT_EQ(m.participant_count, 2);
	cluster_shared_storage_uuid = NULL;
}

UT_TEST(test_sentinel_uuid_refusal_preserves_identity)
{
	static const char *bad[] = {
		"0123456789abc-def-0123-456789abcdef",
		"01234567_89ab-cdef-0123-456789abcdef",
		"00000000-0000-0000-0000-000000000000",
		"11234567-89ab-cdef-0123-456789abcdef"
	};
	MirrorSharedControl before, after;

	for (unsigned i = 0; i < lengthof(bad); i++) {
		pid_t child;
		int status;

		fresh_root("bad_uuid");
		cluster_node_id = 0;
		cluster_shared_storage_uuid = "01234567-89ab-cdef-0123-456789abcdef";
		cluster_shared_fs_sentinel_attach();
		UT_ASSERT(read_mirror(&before));
		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			cluster_node_id = 1;
			cluster_shared_storage_uuid = (char *)bad[i];
			cluster_shared_fs_sentinel_attach();
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
		UT_ASSERT(read_mirror(&after));
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT(!cluster_shared_fs_sentinel_has_participant(1));
	}
	cluster_shared_storage_uuid = NULL;
}


/* Native descriptors enforce direct alignment on Linux. The explicit check in
 * the fd boundary also exercises that requirement on F_NOCACHE platforms. */
UT_TEST(test_direct_flags_and_all_relation_forks)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	const int modes[] = { 0, IO_DIRECT_WAL, IO_DIRECT_DATA, IO_DIRECT_DATA | IO_DIRECT_WAL };
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24600 };
	PGIOAlignedBlock input[2], output[2];

	fresh_root("direct_forks");
	UT_ASSERT(PG_O_DIRECT != 0 && ops->caps->supports_odirect);
	UT_ASSERT_EQ(ops->caps->required_io_alignment, 0);
	UT_ASSERT_EQ(ops->caps->durability_class, CLUSTER_DURABILITY_BUFFERED);
	for (int mode = 0; mode < lengthof(modes); mode++) {
		io_direct_flags = modes[mode];
		for (ForkNumber forknum = MAIN_FORKNUM; forknum <= SPACE_FORKNUM; forknum++) {
			ClusterSharedFsHandle *handle = NULL;
			bool direct = (io_direct_flags & IO_DIRECT_DATA) != 0;

			ops->create(rl, forknum, false, &handle);
			UT_ASSERT_EQ((last_open_flags & PG_O_DIRECT) != 0, direct);
#ifdef O_DIRECT
			UT_ASSERT_EQ((fcntl(last_open_fd, F_GETFL) & O_DIRECT) != 0, direct);
#endif
			UT_ASSERT_EQ(ops->nblocks(handle), 0);
			memset(input[0].data + 1, 0x83 + forknum, BLCKSZ);
			UT_ASSERT_EQ(ops->write(handle, 0, input[0].data + 1), BLCKSZ);
			UT_ASSERT_EQ(ops->read(handle, 0, output[0].data + 1), BLCKSZ);
			UT_ASSERT_EQ(memcmp(input[0].data + 1, output[0].data + 1, BLCKSZ), 0);
			ops->close(handle);
			ops->open_existing(rl, forknum, &handle);
			UT_ASSERT_EQ((last_open_flags & PG_O_DIRECT) != 0, direct);
			UT_ASSERT_EQ(ops->read(handle, 0, output[0].data), BLCKSZ);
			UT_ASSERT_EQ((unsigned char)output[0].data[0], 0x83 + forknum);
			ops->close(handle);
			ops->create(rl, forknum, true, &handle);
			UT_ASSERT_EQ((last_open_flags & PG_O_DIRECT) != 0, direct);
			UT_ASSERT_EQ(ops->nblocks(handle), 1);
			ops->close(handle);
			ops->unlink(rl, forknum);
		}
	}
	io_direct_flags = 0;
}

UT_TEST(test_direct_alignment_extension_and_sync)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24601 };
	ClusterSharedFsHandle *handle = NULL;
	PGIOAlignedBlock input, output;

	fresh_root("direct_extend");
	io_direct_flags = IO_DIRECT_DATA;
	ops->create(rl, MAIN_FORKNUM, false, &handle);
	memset(input.data, 0x52, BLCKSZ);
	UT_ASSERT_EQ(ops->write(handle, 0, input.data), BLCKSZ);
	UT_ASSERT(last_io_buffer == input.data);
	UT_ASSERT_EQ(ops->read(handle, 0, output.data), BLCKSZ);
	UT_ASSERT(last_io_buffer == output.data);
	UT_ASSERT_EQ(memcmp(input.data, output.data, BLCKSZ), 0);
	for (BlockNumber block = 1; block < 4; block++) {
		UT_ASSERT_EQ(ops->nblocks(handle), block);
		ops->extend(handle, block);
		UT_ASSERT_EQ((uintptr_t)last_io_buffer % PG_IO_ALIGN_SIZE, 0);
		UT_ASSERT_EQ(ops->nblocks(handle), block + 1);
		UT_ASSERT_EQ(ops->read(handle, block, output.data), BLCKSZ);
		memset(input.data, 0, BLCKSZ);
		UT_ASSERT_EQ(memcmp(input.data, output.data, BLCKSZ), 0);
	}
	sync_calls = 0;
	ops->immedsync(handle);
	ops->barrier_sync(handle);
	UT_ASSERT_EQ(sync_calls, 2);
	ops->truncate(handle, 1);
	UT_ASSERT_EQ(ops->nblocks(handle), 1);
	ops->barrier_sync(handle);
	UT_ASSERT_EQ(sync_calls, 3);
	ops->close(handle);
	ops->unlink(rl, MAIN_FORKNUM);
	io_direct_flags = 0;
}

UT_TEST(test_direct_prefetch_and_writeback_are_not_buffered)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24602 };
	ClusterSharedFsHandle *handle = NULL;

	fresh_root("direct_prefetch");
	for (int direct = 0; direct <= 1; direct++) {
		io_direct_flags = direct ? IO_DIRECT_DATA : IO_DIRECT_WAL;
		ops->create(rl, MAIN_FORKNUM, false, &handle);
		prefetch_calls = writeback_calls = 0;
		UT_ASSERT_EQ(ops->prefetch(handle, 0), !direct);
		ops->writeback(handle, 0, 1);
		UT_ASSERT_EQ(prefetch_calls, !direct);
		UT_ASSERT_EQ(writeback_calls, !direct);
		ops->close(handle);
		ops->unlink(rl, MAIN_FORKNUM);
	}
	io_direct_flags = 0;
}

UT_TEST(test_direct_errors_and_partial_read_do_not_publish_success)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24603 };
	ClusterSharedFsHandle *handle = NULL;
	char *shared = mmap(NULL, 2 * BLCKSZ, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	PGIOAlignedBlock input;

	UT_ASSERT(shared != MAP_FAILED);
	fresh_root("direct_error");
	io_direct_flags = IO_DIRECT_DATA;
	ops->create(rl, MAIN_FORKNUM, false, &handle);
	memset(input.data, 0x6d, BLCKSZ);
	ops->write(handle, 0, input.data);
	for (int failure = 1; failure <= 6; failure++) {
		pid_t child;
		int status;

		memset(shared, 0x6d, 2 * BLCKSZ);
		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			io_fault = failure == 6 ? 5 : failure;
			if (failure <= 2)
				ops->read(handle, 0, shared + 1);
			else if (failure <= 4)
				ops->write(handle, 0, input.data);
			else if (failure == 5)
				ops->immedsync(handle);
			else
				ops->barrier_sync(handle);
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
		UT_ASSERT_EQ(memcmp(shared + 1, input.data, BLCKSZ), 0);
	}
	munmap(shared, 2 * BLCKSZ);
	ops->close(handle);
	ops->unlink(rl, MAIN_FORKNUM);
	io_direct_flags = 0;
}

UT_TEST(test_direct_open_failure_cannot_fall_back)
{
	const ClusterSharedFsOps *ops = &cluster_shared_fs_sharedfs_ops;
	RelFileLocator rl = { .spcOid = 1663, .dbOid = 5, .relNumber = 24604 };
	ClusterSharedFsHandle *handle = NULL;

	fresh_root("direct_refused");
	io_direct_flags = 0;
	ops->create(rl, MAIN_FORKNUM, false, &handle);
	ops->close(handle);
	for (int action = 0; action < 3; action++) {
		pid_t child;
		int status;

		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			io_direct_flags = IO_DIRECT_DATA;
			reject_direct_open = true;
			if (action == 0)
				ops->open_existing(rl, MAIN_FORKNUM, &handle);
			else
				ops->create(rl, action == 1 ? MAIN_FORKNUM : FSM_FORKNUM, true, &handle);
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
	}
	ops->unlink(rl, MAIN_FORKNUM);
}

UT_TEST(test_direct_data_leaves_small_sentinel_buffered)
{
	MirrorSharedControl control;
	int before;

	fresh_root("direct_sentinel");
	io_direct_flags = IO_DIRECT_DATA;
	before = open_calls;
	cluster_shared_fs_sharedfs_ops.init();
	UT_ASSERT(read_mirror(&control));
	UT_ASSERT(cluster_shared_fs_sentinel_has_participant(cluster_node_id));
	UT_ASSERT_EQ(open_calls, before);
	UT_ASSERT(sizeof(control) < BLCKSZ);
	io_direct_flags = 0;
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_sharedfs_roundtrip_and_owner_agnostic);
	UT_RUN(test_sharedfs_extend_zero_fills);
	UT_RUN(test_shared_catalog_create_rejects_existing_main);
	UT_RUN(test_nblocks_rechecks_transient_partial_extend);
	UT_RUN(test_nblocks_persistent_partial_tail_stays_failclosed);
	UT_RUN(test_sentinel_attach_records_self);
	UT_RUN(test_sentinel_attach_idempotent);
	UT_RUN(test_sentinel_second_node_joins);
	UT_RUN(test_sentinel_corrupt_fails_closed);
	UT_RUN(test_sentinel_preset_uuid_recorded);
	UT_RUN(test_sentinel_uuid_from_shared_configuration);
	UT_RUN(test_sentinel_uuid_refusal_preserves_identity);
	UT_RUN(test_sentinel_missing_file_fails_closed);
	UT_RUN(test_direct_flags_and_all_relation_forks);
	UT_RUN(test_direct_alignment_extension_and_sync);
	UT_RUN(test_direct_prefetch_and_writeback_are_not_buffered);
	UT_RUN(test_direct_errors_and_partial_read_do_not_publish_success);
	UT_RUN(test_direct_open_failure_cannot_fall_back);
	UT_RUN(test_direct_data_leaves_small_sentinel_buffered);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
