/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_authority.c
 *	  Runtime unit tests for the shared XID authority + native-era
 *	  prehistory module (spec-6.15b D1/D2): on-disk layout lock, the pure
 *	  classification and payload-sizing logic, the torn-safe read/write
 *	  round trip with .bak fallback, seal/flag monotonicity, and a real
 *	  prehistory publish -> adopt byte round trip between two fake PGDATA
 *	  pg_xact trees.
 *
 *	  Like test_cluster_oid_lease.c this exercises the module for REAL
 *	  against temp directories: fd.c openers map onto open(2)/close(2),
 *	  pg_fsync onto fsync(2), durable_rename onto rename(2).  The errstart
 *	  stub aborts on elevel >= ERROR, so every FATAL/PANIC leg (corrupt
 *	  authority at adopt, cap refusal, era re-entry) is TAP territory
 *	  (t/361), not unit territory; reaching an abort here is a real bug.
 *
 *	  Covers (spec §4):
 *	    U1  classify truth table (short / magic / CRC / valid), both files
 *	    U2  publish monotonicity (native_hw never lowered)
 *	    U3  flags one-way (SEALED / CLUSTER_ERA never cleared)
 *	    U4  prehistory payload sizing + publish/adopt byte round trip
 *	    U5  header layout locked by offset assertions
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_xid_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.15b-xid-authority-native-era.md (D1/D2, §4 U1-U5)
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

#include "access/clog.h"		/* GCS-race round-2 RC-E stub signatures */
#include "access/transam.h"		/* VariableCache stub */
#include "cluster/cluster_cr.h" /* native-prehistory latch stub signature */
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe_boot.h" /* lazy-latch stub signature (F1 link) */
#include "port/pg_crc32c.h"
#include "storage/fd.h"
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

UT_DEFINE_GLOBALS();

/* Global read by cluster_xid_authority.o's file I/O. */
char *cluster_shared_data_dir = NULL;

/* ---- GCS-race round-2 RC-E: stubs for the post-recovery verify's backend
 * deps.  The unit layer exercises the PURE provable_full judge plus the
 * publish/adopt/prefix file paths; the verify orchestration (SLRU reads,
 * repair writes, latch) is TAP t/361 territory and is never invoked here,
 * but the standalone link still needs the symbols resolved. ---- */
bool cluster_enabled = false;
bool cluster_shared_catalog = false;
VariableCache ShmemVariableCache = NULL;
XidStatus
TransactionIdGetStatus(TransactionId xid pg_attribute_unused(),
					   XLogRecPtr *lsn pg_attribute_unused())
{
	printf("# unexpected TransactionIdGetStatus call in unit context -- aborting\n");
	abort();
}
void
ClusterClogAdoptNativeStatus(TransactionId xid pg_attribute_unused(),
							 XidStatus status pg_attribute_unused())
{
	printf("# unexpected ClusterClogAdoptNativeStatus call in unit context -- aborting\n");
	abort();
}
void
cluster_cr_native_prehistory_latch(uint64 native_hw_full pg_attribute_unused())
{
	printf("# unexpected cluster_cr_native_prehistory_latch call in unit context -- aborting\n");
	abort();
}

/* ---- GCS-race round-2 review F1: provable_full widens through
 * cluster_xid_widen, so cluster_xid_stripe.o joins the link.  Only the
 * pure widen path runs here; the stripe runtime wrappers and their
 * backend deps must never be reached. ---- */
int cluster_node_id = -1;
bool cluster_xid_striping = false;
FullTransactionId
ReadNextFullTransactionId(void)
{
	printf("# unexpected ReadNextFullTransactionId call in unit context -- aborting\n");
	abort();
}
void
cluster_xid_stripe_lazy_latch(void)
{
	printf("# unexpected cluster_xid_stripe_lazy_latch call in unit context -- aborting\n");
	abort();
}

/* ---- Assert + ereport machinery (aborts on ERROR; the read paths never
 * ereport, the write paths only PANIC on real I/O failure). ---- */
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
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* ---- functional fd.c stubs: map onto the kernel ---- */
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
	char globaldir[MAXPGPATH];

	snprintf(test_root, sizeof(test_root), "/tmp/pgrac_xid_auth_ut_%d", (int)getpid());
	mkdir(test_root, 0700);
	snprintf(globaldir, sizeof(globaldir), "%s/global", test_root);
	mkdir(globaldir, 0700);
	cluster_shared_data_dir = test_root;
}

static void
unlink_files(void)
{
	const char *rels[] = { CLUSTER_XID_AUTHORITY_REL_PATH, CLUSTER_XID_AUTHORITY_BAK_REL_PATH,
						   CLUSTER_XID_PREHISTORY_REL_PATH, CLUSTER_XID_PREHISTORY_BAK_REL_PATH };
	char p[MAXPGPATH];
	int i;

	for (i = 0; i < (int)lengthof(rels); i++) {
		snprintf(p, sizeof(p), "%s/%s", test_root, rels[i]);
		unlink(p);
	}
}

/* Build a fake PGDATA with a pg_xact/0000 of n_pages, filled with byte. */
static void
make_fake_pgdata(char *pgdata, size_t len, const char *tag, int n_pages, unsigned char byte)
{
	char dir[MAXPGPATH];
	char seg[MAXPGPATH];
	char page[BLCKSZ];
	int fd;
	int i;

	snprintf(pgdata, len, "%s/pgdata_%s", test_root, tag);
	mkdir(pgdata, 0700);
	snprintf(dir, sizeof(dir), "%s/pg_xact", pgdata);
	mkdir(dir, 0700);
	snprintf(seg, sizeof(seg), "%s/0000", dir);
	fd = open(seg, O_RDWR | O_CREAT | O_TRUNC, 0600);
	memset(page, byte, sizeof(page));
	for (i = 0; i < n_pages; i++)
		(void)!write(fd, page, sizeof(page));
	close(fd);
}

/* Build a fake PGDATA whose pg_xact spans SLRU segments: n_pages whole CLOG
 * pages, 32 per segment file (0000, 0001, ...), page p filled with byte
 * (p & 0xFF) so cross-segment ordering is byte-verifiable. */
static void
make_fake_pgdata_multiseg(char *pgdata, size_t len, const char *tag, int n_pages)
{
	char dir[MAXPGPATH];
	char seg[MAXPGPATH];
	char page[BLCKSZ];
	int fd = -1;
	int cur_seg = -1;
	int p;

	snprintf(pgdata, len, "%s/pgdata_%s", test_root, tag);
	mkdir(pgdata, 0700);
	snprintf(dir, sizeof(dir), "%s/pg_xact", pgdata);
	mkdir(dir, 0700);
	for (p = 0; p < n_pages; p++) {
		int segno = p / 32;

		if (segno != cur_seg) {
			if (fd >= 0)
				close(fd);
			snprintf(seg, sizeof(seg), "%s/%04X", dir, segno);
			fd = open(seg, O_RDWR | O_CREAT | O_TRUNC, 0600);
			cur_seg = segno;
		}
		memset(page, p & 0xFF, sizeof(page));
		(void)!write(fd, page, sizeof(page));
	}
	if (fd >= 0)
		close(fd);
}

/* ============================================================
 * U5: on-disk layout locked
 * ============================================================ */

UT_TEST(test_layout_offsets_locked)
{
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, magic), 0);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, version), 4);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, flags), 8);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, reserved), 12);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, native_hw_full), 16);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, next_multi), 24);
	UT_ASSERT_EQ(offsetof(ClusterXidAuthorityHeader, crc), 32);

	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, magic), 0);
	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, version), 4);
	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, native_hw_full), 8);
	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, payload_len), 16);
	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, reserved), 20);
	UT_ASSERT_EQ(offsetof(ClusterXidPrehistoryHeader, crc), 24);
}

/* ============================================================
 * U1: classify truth table
 * ============================================================ */

UT_TEST(test_classify_short_magic_crc_valid)
{
	ClusterXidAuthorityHeader hdr;
	char buf[sizeof(ClusterXidAuthorityHeader)];

	memset(buf, 0, sizeof(buf));
	UT_ASSERT_EQ(cluster_xid_authority_classify(NULL, 0), CLUSTER_XID_AUTHORITY_INVALID_SHORT);
	UT_ASSERT_EQ(cluster_xid_authority_classify(buf, 3), CLUSTER_XID_AUTHORITY_INVALID_SHORT);

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = 0xDEADBEEF;
	memcpy(buf, &hdr, sizeof(hdr));
	UT_ASSERT_EQ(cluster_xid_authority_classify(buf, sizeof(buf)),
				 CLUSTER_XID_AUTHORITY_INVALID_MAGIC);

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_XID_AUTHORITY_MAGIC;
	hdr.version = CLUSTER_XID_AUTHORITY_VERSION;
	hdr.native_hw_full = 816;
	INIT_CRC32C(hdr.crc);
	COMP_CRC32C(hdr.crc, (char *)&hdr, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(hdr.crc);
	memcpy(buf, &hdr, sizeof(hdr));
	UT_ASSERT_EQ(cluster_xid_authority_classify(buf, sizeof(buf)), CLUSTER_XID_AUTHORITY_VALID);

	buf[16] ^= 0x01; /* flip a native_hw bit -> CRC mismatch */
	UT_ASSERT_EQ(cluster_xid_authority_classify(buf, sizeof(buf)),
				 CLUSTER_XID_AUTHORITY_INVALID_CRC);
}

/* ============================================================
 * U4: prehistory payload sizing
 * ============================================================ */

UT_TEST(test_payload_bytes_boundaries)
{
	const uint32 xids_per_page = BLCKSZ * 4; /* CLOG: 2 bits/xact */

	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(0), 0);
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(1), (uint32)BLCKSZ);
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(816), (uint32)BLCKSZ);
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(xids_per_page), (uint32)BLCKSZ);
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(xids_per_page + 1), (uint32)(2 * BLCKSZ));
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(CLUSTER_XID_PREHISTORY_MAX_XID),
				 (uint32)(CLUSTER_XID_PREHISTORY_MAX_XID / 4));
	/* over the refusal cap: 0 = "no valid payload" (publish FATALs) */
	UT_ASSERT_EQ(cluster_xid_prehistory_payload_bytes(CLUSTER_XID_PREHISTORY_MAX_XID + 1), 0);
}

/* ============================================================
 * torn-safe authority round trips
 * ============================================================ */

UT_TEST(test_seed_if_absent_creates_unsealed)
{
	ClusterXidAuthorityHeader got;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_present(), false);
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	UT_ASSERT_EQ(cluster_xid_authority_present(), true);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 791);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA, 0);

	/* second seed is a no-op and keeps the existing value */
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(123456), false);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 791);
}

UT_TEST(test_publish_monotone_and_seal)
{
	ClusterXidAuthorityHeader got;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);

	/* interim (unsealed) publish raises the high-water */
	cluster_xid_authority_publish_native(810, 1, false);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 810);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);

	/* sealing publish raises further and stamps SEALED */
	cluster_xid_authority_publish_native(816, 1, true);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 816);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, CLUSTER_XID_AUTHORITY_FLAG_SEALED);

	/* a lower publish never lowers, and never clears SEALED */
	cluster_xid_authority_publish_native(500, 1, false);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 816);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, CLUSTER_XID_AUTHORITY_FLAG_SEALED);
}

UT_TEST(test_mark_cluster_era_one_way)
{
	ClusterXidAuthorityHeader got;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);

	cluster_xid_authority_mark_cluster_era();
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.flags,
				 CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);

	/* later publishes keep both flags (never cleared) */
	cluster_xid_authority_publish_native(900, 1, false);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 900);
	UT_ASSERT_EQ(got.flags,
				 CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);
}

UT_TEST(test_primary_corrupt_falls_back_to_bak)
{
	ClusterXidAuthorityHeader got;
	char p[MAXPGPATH];
	int fd;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	/* second write rolls the 791 image into .bak */
	cluster_xid_authority_publish_native(816, 1, true);

	/* corrupt the primary in place */
	snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	fd = open(p, O_RDWR, 0600);
	(void)!write(fd, "garbage!", 8);
	close(fd);

	/* falls back to the rolled 791 image */
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 791);
}

UT_TEST(test_present_distinguishes_corrupt_from_absent)
{
	ClusterXidAuthorityHeader got;
	char p[MAXPGPATH];
	int fd;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_present(), false);

	/* a lone corrupt primary: present=true, read=false (fail-closed) */
	snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0600);
	(void)!write(fd, "garbage!", 8);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_authority_present(), true);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), false);
}

/* ============================================================
 * U4: prehistory publish -> adopt byte round trip
 * ============================================================ */

UT_TEST(test_prehistory_publish_adopt_round_trip)
{
	char pgdata_seed[MAXPGPATH];
	char pgdata_join[MAXPGPATH];
	char seg[MAXPGPATH];
	char seed_page[BLCKSZ];
	char join_page[BLCKSZ];
	int fd;

	unlink_files();

	/* seed: one clog page of 0xAA; joiner: same-size page of zeros */
	make_fake_pgdata(pgdata_seed, sizeof(pgdata_seed), "seed", 1, 0xAA);
	make_fake_pgdata(pgdata_join, sizeof(pgdata_join), "join", 1, 0x00);

	cluster_xid_prehistory_publish(pgdata_seed, 816);
	UT_ASSERT_EQ(cluster_xid_prehistory_was_adopted(), false);
	cluster_xid_prehistory_adopt(pgdata_join, 816);
	UT_ASSERT_EQ(cluster_xid_prehistory_was_adopted(), true);

	snprintf(seg, sizeof(seg), "%s/pg_xact/0000", pgdata_seed);
	fd = open(seg, O_RDONLY, 0);
	UT_ASSERT_EQ(read(fd, seed_page, BLCKSZ), BLCKSZ);
	close(fd);
	snprintf(seg, sizeof(seg), "%s/pg_xact/0000", pgdata_join);
	fd = open(seg, O_RDONLY, 0);
	UT_ASSERT_EQ(read(fd, join_page, BLCKSZ), BLCKSZ);
	close(fd);
	UT_ASSERT_EQ(memcmp(seed_page, join_page, BLCKSZ), 0);

	/* adopt is idempotent: run again, bytes unchanged */
	cluster_xid_prehistory_adopt(pgdata_join, 816);
	fd = open(seg, O_RDONLY, 0);
	UT_ASSERT_EQ(read(fd, join_page, BLCKSZ), BLCKSZ);
	close(fd);
	UT_ASSERT_EQ(memcmp(seed_page, join_page, BLCKSZ), 0);
}

UT_TEST(test_prehistory_multi_segment_round_trip)
{
	const uint64 xids_per_page = (uint64)BLCKSZ * 4;
	const int n_pages = 33; /* segment 0000 full (32 pages) + 0001 (1 page) */
	const uint64 hw = (uint64)n_pages * xids_per_page;
	char pgdata_seed[MAXPGPATH];
	char pgdata_join[MAXPGPATH];
	char seg[MAXPGPATH];
	char want[BLCKSZ];
	char got[BLCKSZ];
	int p;

	unlink_files();
	make_fake_pgdata_multiseg(pgdata_seed, sizeof(pgdata_seed), "seedms", n_pages);
	/* short pre-seed clone: only one zeroed page in segment 0000 */
	make_fake_pgdata_multiseg(pgdata_join, sizeof(pgdata_join), "joinms", 1);

	cluster_xid_prehistory_publish(pgdata_seed, hw);
	cluster_xid_prehistory_adopt(pgdata_join, hw);

	for (p = 0; p < n_pages; p++) {
		int fd;

		snprintf(seg, sizeof(seg), "%s/pg_xact/%04X", pgdata_join, p / 32);
		fd = open(seg, O_RDONLY, 0);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(lseek(fd, (off_t)(p % 32) * BLCKSZ, SEEK_SET), (off_t)(p % 32) * BLCKSZ);
		UT_ASSERT_EQ(read(fd, got, BLCKSZ), BLCKSZ);
		close(fd);
		memset(want, p & 0xFF, sizeof(want));
		UT_ASSERT_EQ(memcmp(got, want, BLCKSZ), 0);
	}
}

UT_TEST(test_unseal_survives_primary_corruption_via_bak)
{
	ClusterXidAuthorityHeader got;
	char path[MAXPGPATH];
	int fd;

	/* F1: after begin_native_run, NO on-disk copy may still say SEALED --
	 * a corrupt primary falling back to a stale SEALED .bak would hand a
	 * joiner the previous pass's high-water (8.A false-invisible). */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);
	cluster_xid_authority_begin_native_run();

	snprintf(path, sizeof(path), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	fd = open(path, O_RDWR, 0600);
	(void)!write(fd, "garbage!", 8);
	close(fd);

	if (cluster_xid_authority_read(&got))
		UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);
	/* read failing entirely (no trustworthy copy) is also acceptable:
	 * joiners fail closed on unavailable exactly like on unsealed. */
}

UT_TEST(test_mark_cluster_era_survives_primary_corruption_via_bak)
{
	ClusterXidAuthorityHeader got;
	char path[MAXPGPATH];
	int fd;

	/* F1 variant: after mark_cluster_era, no on-disk copy may lack the
	 * one-way CLUSTER_ERA flag -- a stale .bak without it would let an
	 * enabled=off boot re-enter the native era on a formed tree. */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);
	cluster_xid_authority_mark_cluster_era();

	snprintf(path, sizeof(path), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	fd = open(path, O_RDWR, 0600);
	(void)!write(fd, "garbage!", 8);
	close(fd);

	if (cluster_xid_authority_read(&got))
		UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA,
					 CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);
}

/*
 * write_raw_image / read_raw_image -- install / fetch one fixed-size
 * authority image at `path` (raw open/write, no CRC games: the images
 * moved around here are byte copies of real, valid on-disk images).
 */
static void
write_raw_image(const char *path, const char *image, size_t len)
{
	int fd;

	fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, image, len), (long long)len);
	close(fd);
}

static void
read_raw_image(const char *path, char *image, size_t len)
{
	int fd;

	fd = open(path, O_RDONLY, 0);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, image, len), (long long)len);
	close(fd);
}

UT_TEST(test_unseal_torn_crash_repairs_stale_sealed_bak)
{
	ClusterXidAuthorityHeader got;
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	char old_image[sizeof(ClusterXidAuthorityHeader)];
	char bak_image[sizeof(ClusterXidAuthorityHeader)];
	int fd;

	/* review r3-X1: a crash BETWEEN write_header_both's two renames leaves
	 * primary=UNSEALED(new) / .bak=SEALED(old high-water).  Every native-era
	 * boot re-runs the transition, which must re-assert BOTH copies; before
	 * the fix it early-returned on the already-unsealed primary and the
	 * stale SEALED .bak survived the whole run -- any later primary read
	 * failure resurrected the previous pass's high-water (8.A). */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);

	/* capture the pre-transition (SEALED) primary image */
	snprintf(primary_path, sizeof(primary_path), "%s/%s", test_root,
			 CLUSTER_XID_AUTHORITY_REL_PATH);
	snprintf(bak_path, sizeof(bak_path), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_BAK_REL_PATH);
	read_raw_image(primary_path, old_image, sizeof(old_image));

	/* complete the transition, then re-install the old SEALED image as
	 * .bak, simulating the crash window between the two durable renames */
	cluster_xid_authority_begin_native_run();
	write_raw_image(bak_path, old_image, sizeof(old_image));

	/* restart repair path: the next native-era boot re-runs the transition */
	cluster_xid_authority_begin_native_run();

	/* the .bak copy itself must have been repaired to an unsealed image */
	read_raw_image(bak_path, bak_image, sizeof(bak_image));
	UT_ASSERT_EQ(cluster_xid_authority_classify(bak_image, sizeof(bak_image)),
				 CLUSTER_XID_AUTHORITY_VALID);
	memcpy(&got, bak_image, sizeof(got));
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);

	/* a later primary read failure must fall back to the REPAIRED image,
	 * not resurrect the previous pass's sealed high-water */
	fd = open(primary_path, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	(void)!write(fd, "garbage!", 8);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 816);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);
}

UT_TEST(test_mark_cluster_era_torn_crash_repairs_stale_bak)
{
	ClusterXidAuthorityHeader got;
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	char old_image[sizeof(ClusterXidAuthorityHeader)];
	char bak_image[sizeof(ClusterXidAuthorityHeader)];
	int fd;

	/* review r3-X1 variant: the same torn-crash window across the
	 * CLUSTER_ERA stamp leaves a .bak without the one-way flag; a .bak
	 * fallback would then let an enabled=off boot re-enter the native era
	 * on a formed tree.  The stamp is re-run by every cluster boot and
	 * must repair the lagging copy. */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);

	snprintf(primary_path, sizeof(primary_path), "%s/%s", test_root,
			 CLUSTER_XID_AUTHORITY_REL_PATH);
	snprintf(bak_path, sizeof(bak_path), "%s/%s", test_root, CLUSTER_XID_AUTHORITY_BAK_REL_PATH);
	read_raw_image(primary_path, old_image, sizeof(old_image));

	cluster_xid_authority_mark_cluster_era();
	write_raw_image(bak_path, old_image, sizeof(old_image));

	/* restart repair path: the next cluster boot re-runs the stamp */
	cluster_xid_authority_mark_cluster_era();

	read_raw_image(bak_path, bak_image, sizeof(bak_image));
	UT_ASSERT_EQ(cluster_xid_authority_classify(bak_image, sizeof(bak_image)),
				 CLUSTER_XID_AUTHORITY_VALID);
	memcpy(&got, bak_image, sizeof(got));
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA,
				 CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);

	fd = open(primary_path, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	(void)!write(fd, "garbage!", 8);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA,
				 CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);
}

UT_TEST(test_prefix_check_divergence_truth_table)
{
	const uint64 xids_per_page = (uint64)BLCKSZ * 4;
	char pgdata_seed[MAXPGPATH];
	char pgdata_join[MAXPGPATH];
	char seg[MAXPGPATH];
	int fd;

	/* F2: identical prefix -> CONSISTENT (both trees one page of 0xAA) */
	unlink_files();
	make_fake_pgdata(pgdata_seed, sizeof(pgdata_seed), "f2seed", 1, 0xAA);
	make_fake_pgdata(pgdata_join, sizeof(pgdata_join), "f2join", 1, 0xAA);
	cluster_xid_prehistory_publish(pgdata_seed, 816);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816, 0),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* flip one status byte INSIDE the limit -> DIVERGED */
	snprintf(seg, sizeof(seg), "%s/pg_xact/0000", pgdata_join);
	fd = open(seg, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(lseek(fd, 100, SEEK_SET), 100); /* byte 100 = xids 400..403 */
	(void)!write(fd, "X", 1);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816, 0),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* the same flipped byte OUTSIDE the limit -> CONSISTENT (adopt arm:
	 * bytes at/after own_next belong to the seed alone) */
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 400, 0),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* partial-byte boundary: xid 400's 2-bit slot enters scope at limit 401 */
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 401, 0),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* missing local page inside the comparable range -> fail-closed
	 * UNAVAILABLE, never CONSISTENT: with oldestXid=0 nothing below the
	 * hole is frozen, so "shorter clone" is not a possible innocent
	 * explanation (review r3-X2) */
	unlink(seg);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816, 0),
				 CLUSTER_XID_PREFIX_UNAVAILABLE);

	/* no trustworthy blob -> UNAVAILABLE */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816, 0),
				 CLUSTER_XID_PREFIX_UNAVAILABLE);

	/* multi-segment divergence: 33-page trees, flip a byte in segment 0001 */
	{
		const int n_pages = 33;
		const uint64 hw = (uint64)n_pages * xids_per_page;

		unlink_files();
		make_fake_pgdata_multiseg(pgdata_seed, sizeof(pgdata_seed), "f2seedms", n_pages);
		make_fake_pgdata_multiseg(pgdata_join, sizeof(pgdata_join), "f2joinms", n_pages);
		cluster_xid_prehistory_publish(pgdata_seed, hw);
		UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, 0),
					 CLUSTER_XID_PREFIX_CONSISTENT);
		snprintf(seg, sizeof(seg), "%s/pg_xact/0001", pgdata_join);
		fd = open(seg, O_RDWR, 0600);
		UT_ASSERT(fd >= 0);
		(void)!write(fd, "Z", 1);
		close(fd);
		UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, 0),
					 CLUSTER_XID_PREFIX_DIVERGED);
	}
}

UT_TEST(test_prefix_check_front_truncation)
{
	const uint64 xids_per_page = (uint64)BLCKSZ * 4;
	const int n_pages = 33;
	const uint64 hw = (uint64)n_pages * xids_per_page;
	const uint64 page32_first = 32 * xids_per_page;
	char pgdata_seed[MAXPGPATH];
	char pgdata_join[MAXPGPATH];
	char seg[MAXPGPATH];
	unsigned char b;
	int fd;

	/* review r3-X2 (a): front truncation is no alibi.  SimpleLruTruncate
	 * removed the joiner's segment 0000 (oldestXid frozen past it), but the
	 * surviving page 32 diverges inside the comparable range -> DIVERGED.
	 * Pre-fix, break-on-first-missing-local-page returned CONSISTENT and
	 * the adopt arm then overwrote the joiner's own live-xid outcomes. */
	unlink_files();
	make_fake_pgdata_multiseg(pgdata_seed, sizeof(pgdata_seed), "ftseed", n_pages);
	make_fake_pgdata_multiseg(pgdata_join, sizeof(pgdata_join), "ftjoin", n_pages);
	cluster_xid_prehistory_publish(pgdata_seed, hw);
	snprintf(seg, sizeof(seg), "%s/pg_xact/0000", pgdata_join);
	UT_ASSERT_EQ(unlink(seg), 0);
	snprintf(seg, sizeof(seg), "%s/pg_xact/0001", pgdata_join);
	fd = open(seg, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(lseek(fd, 600, SEEK_SET), 600); /* byte 600 = slots 2400..2403 */
	(void)!write(fd, "X", 1);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, page32_first),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* (b) frozen exemption: restore byte 600 (page fill 0x20), then flip
	 * ONLY slot 2400's two low bits (0x20 -> 0x21).  With oldestXid at slot
	 * 2401 the divergence sits strictly below the boundary, mid-byte: the
	 * lead mask must exclude it -> CONSISTENT (missing segment 0000 below
	 * page(oldestXid) is equally exempt). */
	fd = open(seg, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(lseek(fd, 600, SEEK_SET), 600);
	b = 0x21;
	(void)!write(fd, &b, 1);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, page32_first + 2401),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* same bytes with oldestXid AT the divergent slot -> DIVERGED (the
	 * boundary slot itself is comparable) */
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, page32_first + 2400),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* review r3-X2 (c): a hole AT/ABOVE page(oldestXid) inside the
	 * comparable range is an anomaly -> UNAVAILABLE (fail-closed), never
	 * CONSISTENT */
	UT_ASSERT_EQ(unlink(seg), 0);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw, page32_first + 2400),
				 CLUSTER_XID_PREFIX_UNAVAILABLE);
}

UT_TEST(test_prehistory_classify_corrupt)
{
	char buf[64];

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_prehistory_classify(NULL, 0), CLUSTER_XID_AUTHORITY_INVALID_SHORT);

	/* publish a real blob, then flip a payload bit -> CRC catches it */
	{
		char pgdata_seed[MAXPGPATH];
		char p[MAXPGPATH];
		char *image;
		struct stat st;
		int fd;
		int r;

		make_fake_pgdata(pgdata_seed, sizeof(pgdata_seed), "seed2", 1, 0x55);
		cluster_xid_prehistory_publish(pgdata_seed, 816);

		snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_XID_PREHISTORY_REL_PATH);
		UT_ASSERT_EQ(stat(p, &st), 0);
		image = malloc(st.st_size);
		fd = open(p, O_RDONLY, 0);
		r = (int)read(fd, image, st.st_size);
		close(fd);
		UT_ASSERT_EQ(r, (int)st.st_size);
		UT_ASSERT_EQ(cluster_xid_prehistory_classify(image, st.st_size),
					 CLUSTER_XID_AUTHORITY_VALID);

		image[sizeof(ClusterXidPrehistoryHeader) + 100] ^= 0x01;
		UT_ASSERT_EQ(cluster_xid_prehistory_classify(image, st.st_size),
					 CLUSTER_XID_AUTHORITY_INVALID_CRC);
		free(image);
	}

	memset(buf, 0, sizeof(buf));
	UT_ASSERT_EQ(cluster_xid_prehistory_classify(buf, sizeof(buf)),
				 CLUSTER_XID_AUTHORITY_INVALID_MAGIC);
}

UT_TEST(test_begin_native_run_unseals_before_cluster_era)
{
	ClusterXidAuthorityHeader got;

	unlink_files();
	UT_ASSERT_EQ(cluster_xid_authority_seed_if_absent(791), true);
	cluster_xid_authority_publish_native(816, 1, true);

	/* a follow-up native run clears SEALED but keeps the high-water */
	cluster_xid_authority_begin_native_run();
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 816);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);

	/* idempotent while open */
	cluster_xid_authority_begin_native_run();
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, 0);

	/* the run's clean shutdown re-publishes and re-seals monotonically */
	cluster_xid_authority_publish_native(900, 1, true);
	UT_ASSERT_EQ(cluster_xid_authority_read(&got), true);
	UT_ASSERT_EQ(got.native_hw_full, 900);
	UT_ASSERT_EQ(got.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED, CLUSTER_XID_AUTHORITY_FLAG_SEALED);
}

/*
 * GCS-race round-2 RC-E: pure provable_full truth table.  hw is the FIRST
 * xid the native era did NOT consume, so hw itself is never native.  The
 * judge widens the bare tuple value to its full 64-bit identity in the
 * signed +/- 2^31 window around the live counter (cluster_xid_widen) and
 * compares full-vs-full against hw -- both halfspaces near an epoch
 * boundary must resolve to the WIDENED identity, never to the bare value
 * (round-2 review F1: the bare compare + local wrap sentinel misjudged
 * exactly there).  Unset latch (hw = 0) and widen failure stay false.
 */
static void
test_native_prehistory_provable_truth_table(void)
{
	const uint64 hw = 816;
	const uint64 no_wrap_next = 100000;				 /* epoch 0 */
	const uint64 epoch0_max = (uint64)PG_UINT32_MAX; /* last epoch-0 counter value */
	const uint64 wrapped_next = ((uint64)1 << 32);	 /* first epoch-1 value */

	/* unlatched (hw = 0): never provable, whatever the value */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, 0, 815));

	/* invalid live counter: widen cannot anchor -> never provable */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(0, hw, 815));

	/* latched + widened identity below hw (plain epoch 0): provable */
	UT_ASSERT(cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 815));
	UT_ASSERT(cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 3));

	/* hw boundary: hw is the first unconsumed xid -> not native */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 816));
	/* [native_hw, stripe floor) gap and cluster-era values: not native */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 90000));

	/* special xids (Invalid / Bootstrap / Frozen): never provable */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 0));
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 1));
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(no_wrap_next, hw, 2));

	/*
	 * FUTURE halfspace at the epoch-0 ceiling: with the counter one shy of
	 * wrapping, a small raw value widens FORWARD to the next epoch's
	 * upcoming allocation (2^32 + 815), never to native 815.  The round-1
	 * bare compare + "next <= 2^32-1" sentinel wrongly held this provable.
	 */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(epoch0_max, hw, 815));

	/* wrap recurrence: past the boundary the widened value carries the
	 * epoch and can never fall below an epoch-0 hw */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(wrapped_next, hw, 815));
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(wrapped_next + 5, hw, 815));

	/* PAST halfspace across the boundary: a raw value just below the
	 * ceiling widens BACKWARD to its epoch-0 identity; that identity is
	 * >= hw here, so still not provable */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(wrapped_next + 5, hw,
														   (TransactionId)(PG_UINT32_MAX - 10)));

	/* ... and when the epoch-0 identity IS below a (near-ceiling) native
	 * hw, the past-halfspace widening proves it native -- the bare-value
	 * wrap sentinel could only deny this leg wholesale */
	UT_ASSERT(cluster_xid_native_prehistory_provable_full(
		wrapped_next + 5, (uint64)PG_UINT32_MAX - 5, (TransactionId)(PG_UINT32_MAX - 10)));

	/* behind-window underflow: an interpretation preceding full xid 0 is
	 * impossible -- widen fails closed */
	UT_ASSERT(
		!cluster_xid_native_prehistory_provable_full((uint64)500, hw, (TransactionId)3000000000U));

	/* deep epoch skew: whatever epoch the local counter sits in, a bare
	 * value widens into that epoch's window, far above any epoch-0 hw */
	UT_ASSERT(!cluster_xid_native_prehistory_provable_full(((uint64)5 << 32) + 100000, hw, 815));
}

/*
 * Round-2 review F3: pre-migrate epoch witness round trip.  Absent, torn
 * (short), and corrupt (CRC) reads all fail closed; a valid write reads
 * back the exact nextFullXid, including an epoch-carrying one; rewrite
 * (crash-retry of the migrate arm) is idempotent.
 */
UT_TEST(test_epoch_witness_round_trip)
{
	uint64 got = 0;
	char p[MAXPGPATH];
	int fd;

	snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_XID_EPOCH_WITNESS_REL_PATH);
	unlink(p);

	/* absent: no proof */
	UT_ASSERT(!cluster_xid_epoch_witness_read(test_root, &got));

	/* epoch-0 value round-trips exactly */
	UT_ASSERT(cluster_xid_epoch_witness_write(test_root, 815));
	UT_ASSERT(cluster_xid_epoch_witness_read(test_root, &got));
	UT_ASSERT_EQ(got, 815);

	/* rewrite is idempotent and epoch-carrying values survive */
	UT_ASSERT(cluster_xid_epoch_witness_write(test_root, ((uint64)3 << 32) + 42));
	UT_ASSERT(cluster_xid_epoch_witness_read(test_root, &got));
	UT_ASSERT_EQ(got, ((uint64)3 << 32) + 42);

	/* corrupt payload byte: CRC rejects */
	fd = open(p, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(lseek(fd, (off_t)offsetof(ClusterXidEpochWitness, next_full_xid), SEEK_SET),
				 (off_t)offsetof(ClusterXidEpochWitness, next_full_xid));
	UT_ASSERT_EQ(write(fd, "!", 1), 1);
	close(fd);
	UT_ASSERT(!cluster_xid_epoch_witness_read(test_root, &got));

	/* short (torn) file: rejected */
	UT_ASSERT(cluster_xid_epoch_witness_write(test_root, 815));
	UT_ASSERT_EQ(truncate(p, (off_t)sizeof(ClusterXidEpochWitness) - 4), 0);
	UT_ASSERT(!cluster_xid_epoch_witness_read(test_root, &got));

	unlink(p);
}

int
main(void)
{
	setup_shared_dir();

	UT_PLAN(20);
	UT_RUN(test_layout_offsets_locked);
	UT_RUN(test_classify_short_magic_crc_valid);
	UT_RUN(test_payload_bytes_boundaries);
	UT_RUN(test_seed_if_absent_creates_unsealed);
	UT_RUN(test_publish_monotone_and_seal);
	UT_RUN(test_mark_cluster_era_one_way);
	UT_RUN(test_begin_native_run_unseals_before_cluster_era);
	UT_RUN(test_unseal_survives_primary_corruption_via_bak);
	UT_RUN(test_mark_cluster_era_survives_primary_corruption_via_bak);
	UT_RUN(test_unseal_torn_crash_repairs_stale_sealed_bak);
	UT_RUN(test_mark_cluster_era_torn_crash_repairs_stale_bak);
	UT_RUN(test_primary_corrupt_falls_back_to_bak);
	UT_RUN(test_present_distinguishes_corrupt_from_absent);
	UT_RUN(test_prehistory_publish_adopt_round_trip);
	UT_RUN(test_prehistory_multi_segment_round_trip);
	UT_RUN(test_prefix_check_divergence_truth_table);
	UT_RUN(test_prefix_check_front_truncation);
	UT_RUN(test_prehistory_classify_corrupt);
	UT_RUN(test_native_prehistory_provable_truth_table);
	UT_RUN(test_epoch_witness_round_trip);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
