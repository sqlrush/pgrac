/*-------------------------------------------------------------------------
 *
 * test_cluster_oid_lease.c
 *	  Runtime unit tests for the shared OID authority + lease math
 *	  (spec-6.14 D6).
 *
 *	  Covers (spec §4):
 *	    U5-U7 pure lease math: carve produces pairwise-disjoint blocks from a
 *	          monotonic high-water, consume advances a block, normalize forces
 *	          past the reserved range, and wrap restarts normal OID candidates
 *	    authority I/O: classify (valid / short / bad magic / bad CRC), a real
 *	          write -> read round trip against a temp dir, and fail-closed read
 *	          when the current primary cannot be validated
 *
 *	  Like test_cluster_cf_authority.c the fd.c openers map onto open(2) etc.,
 *	  so file replacement and stale-backup rejection exercise real I/O.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_oid_lease.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D6, U5-U7)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h" /* FirstNormalObjectId */
#include "cluster/cluster_oid_lease.h"
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

/* Global read by cluster_oid_lease.o's authority I/O. */
char *cluster_shared_data_dir = NULL;
static bool expect_authority_error = false;
static jmp_buf authority_error_jmp;

/* ---- Assert + ereport machinery (aborts on ERROR; the read path never
 * ereports, the write path only PANICs on real I/O failure). ---- */
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
		if (expect_authority_error)
			longjmp(authority_error_jmp, 1);
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

	snprintf(test_root, sizeof(test_root), "/tmp/pgrac_oid_lease_ut_%d", (int)getpid());
	mkdir(test_root, 0700);
	snprintf(globaldir, sizeof(globaldir), "%s/global", test_root);
	mkdir(globaldir, 0700);
	cluster_shared_data_dir = test_root;
}

static void
unlink_authority(void)
{
	char p[MAXPGPATH];

	snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_REL_PATH);
	unlink(p);
	snprintf(p, sizeof(p), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_BAK_REL_PATH);
	unlink(p);
}

/* ============================================================
 * U5-U7: pure lease math
 * ============================================================ */

UT_TEST(test_normalize_forces_reserved_up)
{
	UT_ASSERT_EQ(cluster_oid_lease_normalize_start(0), (Oid)FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_normalize_start(100), (Oid)FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_normalize_start((Oid)FirstNormalObjectId),
				 (Oid)FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_normalize_start(1000000), 1000000);
}

UT_TEST(test_carve_basic_block)
{
	Oid start, end, newauth;

	cluster_oid_lease_carve(100000, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, 100000);
	UT_ASSERT_EQ(end, 100000 + 8192);
	UT_ASSERT_EQ(newauth, 100000 + 8192);
}

UT_TEST(test_carve_rejects_reserved_hw)
{
	Oid start, end, newauth;

	/* Only first-seed normalization may move a reserved value upward. */
	cluster_oid_lease_carve(5, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, InvalidOid);
	UT_ASSERT_EQ(end, InvalidOid);
	UT_ASSERT_EQ(newauth, InvalidOid);
}

UT_TEST(test_carve_disjoint_from_monotonic_hw)
{
	Oid s1, e1, a1, s2, e2, a2;

	/* Two consecutive carves from the advancing authority are disjoint:
	 * the second block starts exactly where the first's authority left off. */
	cluster_oid_lease_carve(200000, 4096, &s1, &e1, &a1);
	cluster_oid_lease_carve(a1, 4096, &s2, &e2, &a2);
	UT_ASSERT_EQ(e1, s2); /* adjacent, non-overlapping */
	UT_ASSERT(s2 >= e1);
}

UT_TEST(test_carve_wrap_restarts_after_tail)
{
	ClusterOidLease tail;
	Oid start, end, newauth;
	Oid expected;

	/* Keep the tail separate from the next cycle, without reserved OIDs. */
	cluster_oid_lease_carve(0xFFFFF000u, 8192, &tail.next, &tail.end, &newauth);
	UT_ASSERT_EQ(tail.next, 0xFFFFF000u);
	UT_ASSERT_EQ(tail.end, 0);
	UT_ASSERT_EQ(newauth, FirstNormalObjectId);
	cluster_oid_lease_carve(newauth, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, FirstNormalObjectId);
	UT_ASSERT_EQ(end, FirstNormalObjectId + 8192);
	UT_ASSERT_EQ(newauth, end);
	cluster_oid_lease_carve(newauth, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, FirstNormalObjectId + 8192);
	UT_ASSERT_EQ(newauth, FirstNormalObjectId + 16384);
	for (expected = 0xFFFFF000u; expected != 0; expected++)
		UT_ASSERT_EQ(cluster_oid_lease_consume(&tail), expected);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&tail), InvalidOid);
}

UT_TEST(test_wrap_can_overlap_an_old_candidate_lease)
{
	ClusterOidLease old, tail, wrapped;
	Oid authority;

	/* A slow node may still hold candidates from a previous cycle.  The
	 * callers' catalog/file collision checks, not leases, establish identity. */
	cluster_oid_lease_carve(FirstNormalObjectId, 2, &old.next, &old.end, &authority);
	cluster_oid_lease_carve(UINT32_MAX, 1, &tail.next, &tail.end, &authority);
	cluster_oid_lease_carve(authority, 2, &wrapped.next, &wrapped.end, &authority);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&old), FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&wrapped), FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&tail), UINT32_MAX);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&tail), InvalidOid);
}

UT_TEST(test_last_oid_consumed_once)
{
	ClusterOidLease lease;
	Oid newauth;

	cluster_oid_lease_carve(UINT32_MAX, 1, &lease.next, &lease.end, &newauth);
	UT_ASSERT_EQ(newauth, FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&lease), UINT32_MAX);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&lease), InvalidOid);
	cluster_oid_lease_carve(50000, 0, &lease.next, &lease.end, &newauth);
	UT_ASSERT_EQ(lease.next, InvalidOid);
	UT_ASSERT_EQ(lease.end, InvalidOid);
}

UT_TEST(test_consume_advances_and_exhausts)
{
	ClusterOidLease lease;
	Oid a, b;

	lease.next = 500000;
	lease.end = 500002;
	a = cluster_oid_lease_consume(&lease);
	b = cluster_oid_lease_consume(&lease);
	UT_ASSERT_EQ(a, 500000);
	UT_ASSERT_EQ(b, 500001);
	UT_ASSERT_EQ(lease.next, 500002);
	/* now exhausted */
	UT_ASSERT_EQ(cluster_oid_lease_consume(&lease), InvalidOid);
}

UT_TEST(test_consume_no_overlap_between_two_leases)
{
	ClusterOidLease l1, l2;
	Oid seen1_last;

	/* carve two adjacent blocks and drain them; no value repeats. */
	l1.next = 300000;
	l1.end = 300003;
	l2.next = 300003;
	l2.end = 300006;

	seen1_last = 0;
	while (l1.next != l1.end)
		seen1_last = cluster_oid_lease_consume(&l1);
	/* first value of l2 is strictly greater than last of l1 */
	UT_ASSERT(l2.next > seen1_last);
	UT_ASSERT_EQ(cluster_oid_lease_consume(&l2), 300003);
}

/* ============================================================
 * Authority classify + round trip
 * ============================================================ */

UT_TEST(test_classify_short_and_magic_and_crc)
{
	ClusterOidAuthorityHeader hdr;
	char buf[sizeof(ClusterOidAuthorityHeader)];

	/* short */
	UT_ASSERT_EQ(cluster_oid_authority_classify("x", 1), CLUSTER_OID_AUTHORITY_INVALID_SHORT);

	/* bad magic */
	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = 0xdeadbeef;
	memcpy(buf, &hdr, sizeof(hdr));
	UT_ASSERT_EQ(cluster_oid_authority_classify(buf, sizeof(buf)),
				 CLUSTER_OID_AUTHORITY_INVALID_MAGIC);

	/* good magic, bad CRC */
	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_OID_AUTHORITY_MAGIC;
	hdr.version = CLUSTER_OID_AUTHORITY_VERSION;
	hdr.next_oid = 12345;
	hdr.crc = 0; /* deliberately wrong */
	memcpy(buf, &hdr, sizeof(hdr));
	UT_ASSERT_EQ(cluster_oid_authority_classify(buf, sizeof(buf)),
				 CLUSTER_OID_AUTHORITY_INVALID_CRC);
}

UT_TEST(test_authority_write_read_round_trip)
{
	Oid got = 0;

	setup_shared_dir();
	unlink_authority();

	/* read before any write -> fail-closed */
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);

	cluster_oid_authority_write(777777);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, 777777u);

	/* overwrite advances the high-water */
	cluster_oid_authority_write(888888);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, 888888u);
}

UT_TEST(test_authority_primary_corrupt_rejects_stale_bak)
{
	char primary[MAXPGPATH];
	Oid got = 0;
	int fd;

	setup_shared_dir();
	unlink_authority();

	/* first write: no .bak yet.  second write rolls the first into .bak. */
	cluster_oid_authority_write(111111);
	cluster_oid_authority_write(222222); /* rolls 111111 into .bak */

	/* corrupt the primary in place */
	snprintf(primary, sizeof(primary), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_REL_PATH);
	fd = open(primary, O_WRONLY);
	if (fd >= 0) {
		char junk[4] = { 0, 0, 0, 0 };

		if (write(fd, junk, sizeof(junk)) < 0)
			/* ignore: test aborts below if the read misbehaves */;
		close(fd);
	}

	/* A valid older backup cannot prove the already-issued high-water. */
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);
	UT_ASSERT_EQ(got, 0);
	UT_ASSERT_EQ(unlink(primary), 0);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);
	UT_ASSERT_EQ(cluster_oid_authority_present(), true);
}

UT_TEST(test_authority_rejects_unknown_version_and_reserved_state)
{
	ClusterOidAuthorityHeader hdr;
	int field;

	for (field = 0; field < 3; field++) {
		memset(&hdr, 0, sizeof(hdr));
		hdr.magic = CLUSTER_OID_AUTHORITY_MAGIC;
		hdr.version = CLUSTER_OID_AUTHORITY_VERSION;
		hdr.next_oid = 50000;
		if (field == 0)
			hdr.version++;
		else if (field == 1)
			hdr.reserved = 1;
		else
			hdr.next_oid = 5;
		INIT_CRC32C(hdr.crc);
		COMP_CRC32C(hdr.crc, (char *)&hdr, offsetof(ClusterOidAuthorityHeader, crc));
		FIN_CRC32C(hdr.crc);
		UT_ASSERT(cluster_oid_authority_classify((char *)&hdr, sizeof(hdr))
				  != CLUSTER_OID_AUTHORITY_VALID);
	}
}

UT_TEST(test_authority_legacy_end_marker_wraps)
{
	Oid got = 1;
	Oid start, end, newauth;

	setup_shared_dir();
	unlink_authority();
	cluster_oid_authority_write(InvalidOid);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, InvalidOid);
	UT_ASSERT_EQ(cluster_oid_authority_seed_if_absent(50000), false);
	cluster_oid_lease_carve(got, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, FirstNormalObjectId);
	UT_ASSERT_EQ(end, FirstNormalObjectId + 8192);
	UT_ASSERT_EQ(newauth, end);
}

UT_TEST(test_authority_wrap_survives_reopen)
{
	Oid got = InvalidOid;
	Oid start, end, newauth;

	setup_shared_dir();
	unlink_authority();
	cluster_oid_authority_write(UINT32_MAX - 2);
	UT_ASSERT(cluster_oid_authority_read(&got));
	cluster_oid_lease_carve(got, 8192, &start, &end, &newauth);
	UT_ASSERT_EQ(start, UINT32_MAX - 2);
	UT_ASSERT_EQ(end, InvalidOid);
	UT_ASSERT_EQ(newauth, FirstNormalObjectId);
	cluster_oid_authority_write(newauth);
	/* Losing the unused tail must not restore it from the older .bak. */
	UT_ASSERT(cluster_oid_authority_read(&got));
	UT_ASSERT_EQ(got, FirstNormalObjectId);
	UT_ASSERT_EQ(cluster_oid_authority_seed_if_absent(UINT32_MAX - 2), false);
	cluster_oid_lease_carve(got, 8192, &start, &end, &newauth);
	cluster_oid_authority_write(newauth);
	UT_ASSERT(cluster_oid_authority_read(&got));
	UT_ASSERT_EQ(got, FirstNormalObjectId + 8192);
}

UT_TEST(test_seed_never_replaces_unreadable_existing_authority)
{
	char primary[MAXPGPATH];
	Oid got = 0;
	int fd;
	volatile bool rejected = false;

	setup_shared_dir();
	unlink_authority();
	cluster_oid_authority_write(70000);
	cluster_oid_authority_write(80000);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	snprintf(primary, sizeof(primary), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_REL_PATH);
	fd = open(primary, O_WRONLY | O_TRUNC);
	UT_ASSERT(fd >= 0);
	if (fd >= 0)
		close(fd);
	expect_authority_error = true;
	if (setjmp(authority_error_jmp) == 0)
		(void)cluster_oid_authority_seed_if_absent(50000);
	else
		rejected = true;
	expect_authority_error = false;
	UT_ASSERT(rejected);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);
}

UT_TEST(test_authority_seed_if_absent)
{
	Oid got = 0;

	setup_shared_dir();
	unlink_authority();

	/* absent -> seeds with normalized initial (a low value is forced up). */
	UT_ASSERT_EQ(cluster_oid_authority_seed_if_absent(5), true);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, (Oid)FirstNormalObjectId);

	/* present -> no-op (does not lower or change the existing high-water). */
	UT_ASSERT_EQ(cluster_oid_authority_seed_if_absent(1000000), false);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, (Oid)FirstNormalObjectId); /* unchanged */

	/* a higher initial on a fresh authority seeds that value. */
	unlink_authority();
	UT_ASSERT_EQ(cluster_oid_authority_seed_if_absent(500000), true);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, 500000u);
}

UT_TEST(test_authority_present_distinguishes_corrupt_from_absent)
{
	char path[MAXPGPATH];
	char junk[16];
	Oid got = 0;
	int fd;

	setup_shared_dir();
	unlink_authority();

	/* absent: no image at all -> present=false (genuine first seed) */
	UT_ASSERT_EQ(cluster_oid_authority_present(), false);

	/* corrupt BOTH primary and .bak -> read fail-closed but present=true:
	 * the distinction the catalog bootstrap FATALs on instead of silently
	 * re-seeding over a damaged authority (spec-6.14 §3.6). */
	cluster_oid_authority_write(111111);
	cluster_oid_authority_write(222222); /* rolls 111111 into .bak */

	memset(junk, 0, sizeof(junk));
	snprintf(path, sizeof(path), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_REL_PATH);
	fd = open(path, O_WRONLY);
	if (fd >= 0) {
		if (write(fd, junk, sizeof(junk)) < 0)
			/* ignore: the asserts below catch a misbehaving read */;
		close(fd);
	}
	snprintf(path, sizeof(path), "%s/%s", test_root, CLUSTER_OID_AUTHORITY_BAK_REL_PATH);
	fd = open(path, O_WRONLY);
	if (fd >= 0) {
		if (write(fd, junk, sizeof(junk)) < 0)
			/* ignore: the asserts below catch a misbehaving read */;
		close(fd);
	}

	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);
	UT_ASSERT_EQ(cluster_oid_authority_present(), true);
}

UT_TEST(test_authority_present_fails_closed_on_unreadable)
{
	char globaldir[MAXPGPATH];
	Oid got = 0;

	setup_shared_dir();
	unlink_authority();

	/* A REAL authority behind a stat() that fails non-ENOENT (EACCES via an
	 * unsearchable parent) must read as PRESENT: answering "absent" would
	 * let the bootstrap re-seed over it (opus review r1 P1). */
	cluster_oid_authority_write(424242);
	snprintf(globaldir, sizeof(globaldir), "%s/global", test_root);
	if (chmod(globaldir, 0000) != 0) {
		printf("# chmod 0000 failed; skipping unreadable-dir probe\n");
		return;
	}

	UT_ASSERT_EQ(cluster_oid_authority_read(&got), false);
	UT_ASSERT_EQ(cluster_oid_authority_present(), true);

	chmod(globaldir, 0700);
	UT_ASSERT_EQ(cluster_oid_authority_read(&got), true);
	UT_ASSERT_EQ(got, 424242u);
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_normalize_forces_reserved_up);
	UT_RUN(test_carve_basic_block);
	UT_RUN(test_carve_rejects_reserved_hw);
	UT_RUN(test_carve_disjoint_from_monotonic_hw);
	UT_RUN(test_carve_wrap_restarts_after_tail);
	UT_RUN(test_wrap_can_overlap_an_old_candidate_lease);
	UT_RUN(test_consume_advances_and_exhausts);
	UT_RUN(test_consume_no_overlap_between_two_leases);
	UT_RUN(test_classify_short_and_magic_and_crc);
	UT_RUN(test_authority_write_read_round_trip);
	UT_RUN(test_authority_primary_corrupt_rejects_stale_bak);
	UT_RUN(test_authority_rejects_unknown_version_and_reserved_state);
	UT_RUN(test_authority_legacy_end_marker_wraps);
	UT_RUN(test_authority_wrap_survives_reopen);
	UT_RUN(test_authority_seed_if_absent);
	UT_RUN(test_authority_present_distinguishes_corrupt_from_absent);
	UT_RUN(test_authority_present_fails_closed_on_unreadable);
	UT_RUN(test_last_oid_consumed_once);
	UT_RUN(test_seed_never_replaces_unreadable_existing_authority);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
