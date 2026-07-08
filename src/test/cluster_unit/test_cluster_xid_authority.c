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

#include "cluster/cluster_xid_authority.h"
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
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* flip one status byte INSIDE the limit -> DIVERGED */
	snprintf(seg, sizeof(seg), "%s/pg_xact/0000", pgdata_join);
	fd = open(seg, O_RDWR, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(lseek(fd, 100, SEEK_SET), 100); /* byte 100 = xids 400..403 */
	(void)!write(fd, "X", 1);
	close(fd);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* the same flipped byte OUTSIDE the limit -> CONSISTENT (adopt arm:
	 * bytes at/after own_next belong to the seed alone) */
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 400),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* partial-byte boundary: xid 400's 2-bit slot enters scope at limit 401 */
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 401),
				 CLUSTER_XID_PREFIX_DIVERGED);

	/* missing local pg_xact segment -> comparable prefix is empty ->
	 * CONSISTENT (a shorter clone has no bits to contradict) */
	unlink(seg);
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816),
				 CLUSTER_XID_PREFIX_CONSISTENT);

	/* no trustworthy blob -> UNAVAILABLE */
	unlink_files();
	UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, 816, 816),
				 CLUSTER_XID_PREFIX_UNAVAILABLE);

	/* multi-segment divergence: 33-page trees, flip a byte in segment 0001 */
	{
		const int n_pages = 33;
		const uint64 hw = (uint64)n_pages * xids_per_page;

		unlink_files();
		make_fake_pgdata_multiseg(pgdata_seed, sizeof(pgdata_seed), "f2seedms", n_pages);
		make_fake_pgdata_multiseg(pgdata_join, sizeof(pgdata_join), "f2joinms", n_pages);
		cluster_xid_prehistory_publish(pgdata_seed, hw);
		UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw),
					 CLUSTER_XID_PREFIX_CONSISTENT);
		snprintf(seg, sizeof(seg), "%s/pg_xact/0001", pgdata_join);
		fd = open(seg, O_RDWR, 0600);
		UT_ASSERT(fd >= 0);
		(void)!write(fd, "Z", 1);
		close(fd);
		UT_ASSERT_EQ(cluster_xid_prehistory_prefix_check(pgdata_join, hw, hw),
					 CLUSTER_XID_PREFIX_DIVERGED);
	}
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

int
main(void)
{
	setup_shared_dir();

	UT_PLAN(15);
	UT_RUN(test_layout_offsets_locked);
	UT_RUN(test_classify_short_magic_crc_valid);
	UT_RUN(test_payload_bytes_boundaries);
	UT_RUN(test_seed_if_absent_creates_unsealed);
	UT_RUN(test_publish_monotone_and_seal);
	UT_RUN(test_mark_cluster_era_one_way);
	UT_RUN(test_begin_native_run_unseals_before_cluster_era);
	UT_RUN(test_unseal_survives_primary_corruption_via_bak);
	UT_RUN(test_mark_cluster_era_survives_primary_corruption_via_bak);
	UT_RUN(test_primary_corrupt_falls_back_to_bak);
	UT_RUN(test_present_distinguishes_corrupt_from_absent);
	UT_RUN(test_prehistory_publish_adopt_round_trip);
	UT_RUN(test_prehistory_multi_segment_round_trip);
	UT_RUN(test_prefix_check_divergence_truth_table);
	UT_RUN(test_prehistory_classify_corrupt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
