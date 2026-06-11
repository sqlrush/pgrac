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
#include <sys/stat.h>
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


/* ----------
 * GUC storages + globals read by cluster_shared_fs_sharedfs.o.
 * ----------
 */
char *cluster_shared_data_dir = NULL;
char *cluster_shared_storage_uuid = NULL;
int cluster_node_id = 0;
bool IsUnderPostmaster = false;

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
 * relpathperm -> GetRelationPath.  The tests use only permanent MAIN_FORK
 * relations in the default tablespace, so the "base/<db>/<rel>" shape is
 * the full contract exercised (mirrors common/relpath.c for that case).
 */
char *
GetRelationPath(Oid dbOid, Oid spcOid pg_attribute_unused(), RelFileNumber relNumber,
				int backendId pg_attribute_unused(), ForkNumber forkNumber)
{
	if (forkNumber != MAIN_FORKNUM)
		abort();
	return psprintf("base/%u/%u", dbOid, relNumber);
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


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_sharedfs_roundtrip_and_owner_agnostic);
	UT_RUN(test_sharedfs_extend_zero_fills);
	UT_RUN(test_sentinel_attach_records_self);
	UT_RUN(test_sentinel_attach_idempotent);
	UT_RUN(test_sentinel_second_node_joins);
	UT_RUN(test_sentinel_corrupt_fails_closed);
	UT_RUN(test_sentinel_preset_uuid_recorded);
	UT_RUN(test_sentinel_missing_file_fails_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
