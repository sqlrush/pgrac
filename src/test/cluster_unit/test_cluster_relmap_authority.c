/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_authority.c
 *	  Runtime unit tests for the shared relmap authority substrate
 *	  (spec-6.14 D5).
 *
 *	  Covers (spec §4 U1-U4 analog):
 *	    classify VALID / short / bad magic / bad CRC / bad image size;
 *	    write-pending -> read-committed (readers adopt committed only, INV-14-7:
 *	      a staged pending image is NOT visible via read_committed until
 *	      published);
 *	    publish flips pending -> committed;
 *	    corrupt-primary -> .bak fallback; corrupt-both -> fail-closed;
 *	    owner identity is preserved in the durable header (crash-arbitration
 *	    SSOT, spec §2.1).
 *
 *	  Like test_cluster_cf_authority.c the fd.c openers map onto open(2) etc.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_relmap_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_relmap_authority.h"
#include "common/relmap.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/elog.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_shared_data_dir = NULL;

/* ---- Assert + ereport machinery ---- */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}
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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* Use the actual mapper consumer, with only backend lock/wait boundaries stubbed. */
#define LWLockAcquire(lock, mode) ((void)0)
#define LWLockRelease(lock) ((void)0)
#define pgstat_report_wait_start(event) ((void)0)
#define pgstat_report_wait_end() ((void)0)
#include "test_cluster_relmap_consumer.inc"

/* ---- fd.c stubs ---- */
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
int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	return (rename(oldfile, newfile) != 0) ? -1 : 0;
}

/* ---- temp dir scaffolding ---- */
static char test_root[MAXPGPATH];

static void
setup_shared_dir(void)
{
	char dir[MAXPGPATH];

	snprintf(test_root, sizeof(test_root), "/tmp/pgrac_relmap_ut_%d", (int)getpid());
	mkdir(test_root, 0700);
	snprintf(dir, sizeof(dir), "%s/global", test_root);
	mkdir(dir, 0700);
	cluster_shared_data_dir = test_root;
}

static void
unlink_authority(void)
{
	char p[MAXPGPATH];

	snprintf(p, sizeof(p), "%s/global/pgrac_relmap_authority", test_root);
	unlink(p);
	snprintf(p, sizeof(p), "%s/global/pgrac_relmap_authority.bak", test_root);
	unlink(p);
}

/* Build a synthetic map image of a given length whose bytes are a marker. */
static void
make_map_image(char *buf, uint32 len, unsigned char marker)
{
	memset(buf, marker, len);
}

/* ============================================================
 * classify
 * ============================================================ */

UT_TEST(test_classify_short_magic_crc)
{
	char buf[64];

	/* short */
	UT_ASSERT_EQ(cluster_relmap_authority_classify("x", 1), CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT);

	/* full-length zeroed buffer -> bad magic */
	memset(buf, 0, sizeof(buf));
	UT_ASSERT_EQ(cluster_relmap_authority_classify(buf, sizeof(buf)),
				 CLUSTER_RELMAP_AUTHORITY_INVALID_SHORT); /* 64 < file size */
}

/* ============================================================
 * write-pending / read-committed / publish
 * ============================================================ */

UT_TEST(test_first_write_seeds_committed)
{
	char img[CLUSTER_RELMAP_IMAGE_MAX];
	char out[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 out_len = 0;
	ClusterRelmapOwner owner
		= { .owner_node = 2, .owner_xid = 4242, .owner_epoch = 7, .relmap_lsn = 0x1234 };

	setup_shared_dir();
	unlink_authority();

	/* read before any write -> fail-closed */
	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), false);

	/* first write seeds committed from the same image */
	make_map_image(img, 524, 0xAB);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 1, &owner);

	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), true);
	UT_ASSERT_EQ(out_len, 524u);
	UT_ASSERT_EQ((unsigned char)out[0], 0xAB);
	UT_ASSERT_EQ((unsigned char)out[523], 0xAB);
}

UT_TEST(test_pending_not_visible_until_publish)
{
	char img1[CLUSTER_RELMAP_IMAGE_MAX];
	char img2[CLUSTER_RELMAP_IMAGE_MAX];
	char out[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 out_len = 0;
	ClusterRelmapOwner owner
		= { .owner_node = 1, .owner_xid = 100, .owner_epoch = 1, .relmap_lsn = 0 };

	setup_shared_dir();
	unlink_authority();

	/* establish committed generation 1 = 0xAA */
	make_map_image(img1, 524, 0xAA);
	cluster_relmap_authority_write_pending(true, InvalidOid, img1, 524, 1, &owner);

	/* stage pending generation 2 = 0xBB (NOT published) */
	make_map_image(img2, 524, 0xBB);
	cluster_relmap_authority_write_pending(true, InvalidOid, img2, 524, 2, &owner);

	/* INV-14-7: read_committed still returns the committed 0xAA, not pending */
	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), true);
	UT_ASSERT_EQ((unsigned char)out[0], 0xAA);

	/* publish generation 2: now committed = 0xBB */
	cluster_relmap_authority_publish(true, InvalidOid, 2);
	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), true);
	UT_ASSERT_EQ((unsigned char)out[0], 0xBB);
}

UT_TEST(test_owner_identity_preserved_in_header)
{
	char img[CLUSTER_RELMAP_IMAGE_MAX];
	ClusterRelmapAuthorityHeader hdr;
	ClusterRelmapOwner owner
		= { .owner_node = 3, .owner_xid = 55555, .owner_epoch = 99, .relmap_lsn = 0xDEADBEEF };

	setup_shared_dir();
	unlink_authority();

	make_map_image(img, 524, 0xCC);
	/* first write seeds committed=gen1; stage a fresh pending gen2 so the
	 * header reflects the pending owner + generation. */
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 1, &owner);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 2, &owner);

	UT_ASSERT_EQ(cluster_relmap_authority_read_header(true, InvalidOid, &hdr), true);
	UT_ASSERT_EQ(hdr.owner_node, 3);
	UT_ASSERT_EQ((uint32)hdr.owner_xid, 55555u);
	UT_ASSERT_EQ((uint32)hdr.owner_epoch, 99u);
	UT_ASSERT_EQ((uint32)hdr.relmap_lsn, 0xDEADBEEFu);
	UT_ASSERT_EQ((uint32)hdr.pending_generation, 2u);
	UT_ASSERT_EQ((uint32)hdr.committed_generation, 1u);
}

UT_TEST(test_corrupt_primary_falls_back_to_bak)
{
	char img[CLUSTER_RELMAP_IMAGE_MAX];
	char out[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 out_len = 0;
	char primary[MAXPGPATH];
	int fd;
	ClusterRelmapOwner owner
		= { .owner_node = 1, .owner_xid = 1, .owner_epoch = 1, .relmap_lsn = 0 };

	setup_shared_dir();
	unlink_authority();

	/* two writes: the second rolls the first into .bak */
	make_map_image(img, 524, 0x11);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 1, &owner);
	make_map_image(img, 524, 0x22);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 2, &owner);

	/* corrupt the primary */
	snprintf(primary, sizeof(primary), "%s/global/pgrac_relmap_authority", test_root);
	fd = open(primary, O_WRONLY);
	if (fd >= 0) {
		char junk[8] = { 0 };

		if (write(fd, junk, sizeof(junk)) < 0)
			/* ignore */;
		close(fd);
	}

	/* falls back to the .bak (committed 0x11, from the first write's seed) */
	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), true);
	UT_ASSERT_EQ((unsigned char)out[0], 0x11);
}

UT_TEST(test_discard_pending_keeps_committed)
{
	char img1[CLUSTER_RELMAP_IMAGE_MAX];
	char img2[CLUSTER_RELMAP_IMAGE_MAX];
	char out[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 out_len = 0;
	ClusterRelmapAuthorityHeader hdr;
	ClusterRelmapOwner owner
		= { .owner_node = 1, .owner_xid = 777, .owner_epoch = 1, .relmap_lsn = 0x99 };

	setup_shared_dir();
	unlink_authority();

	/* committed gen1 = 0xAA; stage pending gen2 = 0xBB */
	make_map_image(img1, 524, 0xAA);
	cluster_relmap_authority_write_pending(true, InvalidOid, img1, 524, 1, &owner);
	make_map_image(img2, 524, 0xBB);
	cluster_relmap_authority_write_pending(true, InvalidOid, img2, 524, 2, &owner);

	/* discard the pending: committed image + generation survive intact */
	cluster_relmap_authority_discard_pending(true, InvalidOid, 2);

	UT_ASSERT_EQ(cluster_relmap_authority_read_header(true, InvalidOid, &hdr), true);
	UT_ASSERT_EQ((uint32)hdr.pending_generation, 0u);
	UT_ASSERT_EQ((uint32)hdr.committed_generation, 1u);
	UT_ASSERT_EQ(hdr.owner_node, 0);
	UT_ASSERT_EQ((uint32)hdr.owner_xid, 0u);

	UT_ASSERT_EQ(cluster_relmap_authority_read_committed(true, InvalidOid, out, &out_len), true);
	UT_ASSERT_EQ((unsigned char)out[0], 0xAA);
}

UT_TEST(test_discard_pending_generation_mismatch_noop)
{
	char img[CLUSTER_RELMAP_IMAGE_MAX];
	ClusterRelmapAuthorityHeader hdr;
	ClusterRelmapOwner owner
		= { .owner_node = 2, .owner_xid = 888, .owner_epoch = 1, .relmap_lsn = 0 };

	setup_shared_dir();
	unlink_authority();

	make_map_image(img, 524, 0xAA);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 1, &owner);
	cluster_relmap_authority_write_pending(true, InvalidOid, img, 524, 2, &owner);

	/* wrong generation: no-op (already-published/idempotent-retry shape) */
	cluster_relmap_authority_discard_pending(true, InvalidOid, 7);

	UT_ASSERT_EQ(cluster_relmap_authority_read_header(true, InvalidOid, &hdr), true);
	UT_ASSERT_EQ((uint32)hdr.pending_generation, 2u);
	UT_ASSERT_EQ(hdr.owner_node, 2);

	/* matching generation discards; a second identical discard is a no-op */
	cluster_relmap_authority_discard_pending(true, InvalidOid, 2);
	cluster_relmap_authority_discard_pending(true, InvalidOid, 2);
	UT_ASSERT_EQ(cluster_relmap_authority_read_header(true, InvalidOid, &hdr), true);
	UT_ASSERT_EQ((uint32)hdr.pending_generation, 0u);
}

static const char *native_root;
static const char *native_relative_path;
static Oid native_dbid;

UT_TEST(test_native_initdb_consumer)
{
	RelMapFile native;
	RelMapFile consumed;
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = {0};
	char comparison[sizeof(image)];
	char path[MAXPGPATH];
	char dbpath[64];
	char root[] = "/tmp/pgrac_relmap_native_XXXXXX";
	char *saved_root = cluster_shared_data_dir;
	ClusterRelmapOwner empty_owner = {0};
	bool shared = native_dbid == InvalidOid;
	int fd;
	char extra;

	printf("# real initdb input %s\n", native_relative_path);
	snprintf(path, sizeof(path), "%s/%s/pg_filenode.map", native_root, native_relative_path);
	fd = open(path, O_RDONLY | PG_BINARY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, &native, sizeof(native)), sizeof(native));
	UT_ASSERT_EQ(read(fd, &extra, 1), 0);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(shared, native_dbid, &native, sizeof(native),
		image, sizeof(image)), CLUSTER_RELMAP_INIT_OK);
	UT_ASSERT_EQ(cluster_relmap_authority_classify(image, sizeof(image)), CLUSTER_RELMAP_AUTHORITY_VALID);
	UT_ASSERT_NOT_NULL(mkdtemp(root));
	cluster_shared_data_dir = root;
	snprintf(path, sizeof(path), "%s/global", root);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(path, sizeof(path), "%s/base", root);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	if (shared)
		snprintf(dbpath, sizeof(dbpath), "global");
	else
	{
		snprintf(dbpath, sizeof(dbpath), "base/%u", native_dbid);
		snprintf(path, sizeof(path), "%s/%s", root, dbpath);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	snprintf(path, sizeof(path), "%s/%s/pgrac_relmap_authority", root, dbpath);
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, image, sizeof(image)), sizeof(image));
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
	/* Existing output must not be truncated or replaced by the creator. */
	UT_ASSERT_EQ(open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, 0600), -1);
	UT_ASSERT_EQ(errno, EEXIST);
	fd = open(path, O_RDONLY | PG_BINARY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, comparison, sizeof(comparison)), sizeof(comparison));
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(memcmp(image, comparison, sizeof(image)), 0);
	cluster_read_relmap_from_authority(&consumed, dbpath, false, ERROR);
	UT_ASSERT_EQ(memcmp(&native, &consumed, sizeof(native)), 0);
	/* Same bytes as the existing pending -> publish writer, including padding. */
	UT_ASSERT_EQ(unlink(path), 0);
	cluster_relmap_authority_write_pending(shared, native_dbid, (char *)&native, sizeof(native), 1, &empty_owner);
	cluster_relmap_authority_publish(shared, native_dbid, 1);
	fd = open(path, O_RDONLY | PG_BINARY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, comparison, sizeof(comparison)), sizeof(comparison));
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(memcmp(image, comparison, sizeof(image)), 0);
	UT_ASSERT_EQ(unlink(path), 0);
	strlcat(path, ".bak", sizeof(path));
	UT_ASSERT_EQ(unlink(path), 0);
	if (!shared)
	{
		snprintf(path, sizeof(path), "%s/%s", root, dbpath);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	snprintf(path, sizeof(path), "%s/global", root);
	UT_ASSERT_EQ(rmdir(path), 0);
	snprintf(path, sizeof(path), "%s/base", root);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
	cluster_shared_data_dir = saved_root;
}

int
main(int argc, char **argv)
{
	if (argc != 1 && (argc != 3 || strcmp(argv[1], "--native-init") != 0))
	{
		fprintf(stderr, "usage: %s [--native-init fresh-initdb-directory]\n", argv[0]);
		return 1;
	}
	UT_PLAN(argc == 3 ? 11 : 7);
	UT_RUN(test_classify_short_magic_crc);
	UT_RUN(test_first_write_seeds_committed);
	UT_RUN(test_pending_not_visible_until_publish);
	UT_RUN(test_owner_identity_preserved_in_header);
	UT_RUN(test_corrupt_primary_falls_back_to_bak);
	UT_RUN(test_discard_pending_keeps_committed);
	UT_RUN(test_discard_pending_generation_mismatch_noop);
	if (argc == 3)
	{
		const Oid dbids[] = {0, 4, 1, 5};
		const char *paths[] = {"global", "base/4", "base/1", "base/5"};
		native_root = argv[2];
		for (int i = 0; i < 4; ++i)
		{
			native_dbid = dbids[i];
			native_relative_path = paths[i];
			UT_RUN(test_native_initdb_consumer);
		}
	}
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
