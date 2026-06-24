/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_snapshot.c
 *	  Unit tests for the HW authority durable-snapshot module (spec-5.7 D3
 *	  S1/S2/S3a, §3.1b R1/R3/R6/R10/R11/R12).
 *
 *	  The pure envelope (serialize / deserialize+validate / identity /
 *	  generation-supersede / rebuild merge) runs against the real CRC32C from
 *	  libpgport; the durable torn-safe file round trip (temp + durable_rename)
 *	  runs for REAL against a temp dir with functional fd.c / palloc stubs
 *	  (mirror test_cluster_cf_authority).  Pins the single unified envelope
 *	  (R12): one parse/validate path shared by the checkpoint and the adoption
 *	  snapshot.
 *
 *	  Covers (spec §4.1 U2 extension):
 *	    - serialized size math (header + 20*n + crc)
 *	    - serialize -> deserialize round trip (header fields + entries)
 *	    - zero-entry snapshot is VALID (distinguishes "no entries" from "missing")
 *	    - CRC / magic / short / capacity reject
 *	    - identity match / mismatch (system_id / owner / shard)
 *	    - generation supersede (R10): newer wins, same/older/foreign does not
 *	    - rebuild merge (R3/R11): max, conservative hole, never lower
 *	    - durable write -> read round trip; missing / torn / foreign reject (R6)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hw_snapshot.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D3 S1, §3.1b R1/R10/R12)
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

#include "cluster/cluster_hw_snapshot.h"
#include "storage/fd.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Global read by cluster_hw_snapshot.o for the shared-storage path. */
char *cluster_shared_data_dir = NULL;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ----------
 * ereport machinery.  The snapshot write PANICs on I/O failure and the read
 * never ereports (it returns a non-VALID outcome), so reaching the abort would
 * be a real bug in the success/round-trip paths these tests drive.
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

/* ----------
 * Functional fd.c + palloc stubs: map straight onto the kernel / heap.
 * ----------
 */
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
	return rename(oldfile, newfile);
}

int
MakePGDirectory(const char *directoryName)
{
	return mkdir(directoryName, 0700);
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *ptr)
{
	free(ptr);
}

/* ----------
 * Test fixture: a temp shared root with a global/ subdir; flip a file byte.
 * ----------
 */
static char shared_root[MAXPGPATH];

static void
setup_shared_root(void)
{
	char tmpl[MAXPGPATH];
	char globaldir[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_hw_snapshot_XXXXXX", sizeof(tmpl));
	if (mkdtemp(tmpl) == NULL) {
		printf("# mkdtemp failed: %s\n", strerror(errno));
		abort();
	}
	strlcpy(shared_root, tmpl, sizeof(shared_root));
	cluster_shared_data_dir = shared_root;

	snprintf(globaldir, sizeof(globaldir), "%s/global", shared_root);
	if (mkdir(globaldir, 0700) != 0 && errno != EEXIST) {
		printf("# mkdir global failed: %s\n", strerror(errno));
		abort();
	}
}

static void
flip_byte(const char *path, off_t off)
{
	int fd = open(path, O_RDWR);
	unsigned char b;

	if (fd < 0) {
		printf("# flip_byte open %s failed: %s\n", path, strerror(errno));
		abort();
	}
	if (pread(fd, &b, 1, off) != 1) {
		printf("# flip_byte pread failed\n");
		abort();
	}
	b ^= 0xFF;
	if (pwrite(fd, &b, 1, off) != 1) {
		printf("# flip_byte pwrite failed\n");
		abort();
	}
	close(fd);
}

/* ----------
 * Build a representative header + entries for the round-trip tests.
 * ----------
 */
static void
make_header(ClusterHwSnapshotHeader *h, uint32 n_entries)
{
	memset(h, 0, sizeof(*h));
	h->magic = CLUSTER_HW_SNAPSHOT_MAGIC;
	h->version = CLUSTER_HW_SNAPSHOT_VERSION;
	h->system_id = UINT64CONST(0x0123456789abcdef);
	h->owner_node_id = 2;
	h->shard_partition = 7;
	h->snapshot_kind = CLUSTER_HW_SNAPSHOT_CHECKPOINT;
	h->generation = 5;
	h->snapshot_lsn = UINT64CONST(0x00000001abcd0000);
	h->n_entries = n_entries;
	h->reserved = 0;
}

static void
make_entries(ClusterHwSnapshotEntry *e, uint32 n)
{
	uint32 i;

	for (i = 0; i < n; i++) {
		memset(&e[i], 0, sizeof(e[i]));
		e[i].resid.field1 = 100 + i;   /* dbOid */
		e[i].resid.field2 = 16384 + i; /* relNumber */
		e[i].resid.field3 = 1663;	   /* spcOid */
		e[i].resid.field4 = 0;		   /* MAIN_FORKNUM */
		e[i].resid.type = 0xF2;		   /* CLUSTER_HW_RESID_TYPE */
		e[i].resid.lockmethodid = 1;
		e[i].next_hwm = (i + 1) * 1000;
	}
}

/* ======================================================================
 * serialized-size math (header 48 + 20*n + crc 4)
 * ====================================================================== */
UT_TEST(test_hw_snapshot_serialized_size)
{
	UT_ASSERT_EQ(cluster_hw_snapshot_serialized_size(0), 52);  /* 48 + 0 + 4 */
	UT_ASSERT_EQ(cluster_hw_snapshot_serialized_size(1), 72);  /* 48 + 20 + 4 */
	UT_ASSERT_EQ(cluster_hw_snapshot_serialized_size(3), 112); /* 48 + 60 + 4 */
}

/* ======================================================================
 * serialize -> deserialize round trip (R1 + R12): header + entries recover
 * ====================================================================== */
UT_TEST(test_hw_snapshot_roundtrip)
{
	ClusterHwSnapshotHeader h, out;
	ClusterHwSnapshotEntry e[3], out_e[3];
	char buf[256];
	size_t n;
	ClusterHwSnapshotValidity v;
	uint32 i;

	make_header(&h, 3);
	make_entries(e, 3);

	n = cluster_hw_snapshot_serialize(&h, e, buf, sizeof(buf));
	UT_ASSERT_EQ(n, cluster_hw_snapshot_serialized_size(3));

	v = cluster_hw_snapshot_deserialize(buf, n, &out, out_e, 3);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_VALID);

	UT_ASSERT_EQ(out.magic, CLUSTER_HW_SNAPSHOT_MAGIC);
	UT_ASSERT_EQ(out.version, CLUSTER_HW_SNAPSHOT_VERSION);
	UT_ASSERT_EQ(out.system_id, h.system_id);
	UT_ASSERT_EQ(out.owner_node_id, 2);
	UT_ASSERT_EQ(out.shard_partition, 7);
	UT_ASSERT_EQ(out.snapshot_kind, CLUSTER_HW_SNAPSHOT_CHECKPOINT);
	UT_ASSERT_EQ(out.generation, 5);
	UT_ASSERT_EQ(out.snapshot_lsn, h.snapshot_lsn);
	UT_ASSERT_EQ(out.n_entries, 3);

	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ(out_e[i].resid.field1, 100 + i);
		UT_ASSERT_EQ(out_e[i].resid.field2, 16384 + i);
		UT_ASSERT_EQ(out_e[i].resid.type, 0xF2);
		UT_ASSERT_EQ(out_e[i].next_hwm, (i + 1) * 1000);
	}
}

/* a zero-entry snapshot is VALID -- an empty hw_htab writes a real snapshot so
 * recovery can distinguish "owner had no entries" from "snapshot missing" */
UT_TEST(test_hw_snapshot_zero_entries)
{
	ClusterHwSnapshotHeader h, out;
	char buf[64];
	size_t n;
	ClusterHwSnapshotValidity v;

	make_header(&h, 0);
	n = cluster_hw_snapshot_serialize(&h, NULL, buf, sizeof(buf));
	UT_ASSERT_EQ(n, 52);

	v = cluster_hw_snapshot_deserialize(buf, n, &out, NULL, 0);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_VALID);
	UT_ASSERT_EQ(out.n_entries, 0);
}

/* a flipped byte in the entry region is caught by the trailing CRC */
UT_TEST(test_hw_snapshot_crc_corrupt)
{
	ClusterHwSnapshotHeader h, out;
	ClusterHwSnapshotEntry e[2], out_e[2];
	char buf[256];
	size_t n;
	ClusterHwSnapshotValidity v;

	make_header(&h, 2);
	make_entries(e, 2);
	n = cluster_hw_snapshot_serialize(&h, e, buf, sizeof(buf));

	buf[CLUSTER_HW_SNAPSHOT_HEADER_SIZE + 2] ^= 0xFF; /* corrupt an entry byte */

	v = cluster_hw_snapshot_deserialize(buf, n, &out, out_e, 2);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_CRC);
}

/* a corrupt magic is rejected structurally, before any CRC work */
UT_TEST(test_hw_snapshot_magic_corrupt)
{
	ClusterHwSnapshotHeader h, out;
	ClusterHwSnapshotEntry e[1], out_e[1];
	char buf[128];
	size_t n;
	ClusterHwSnapshotValidity v;

	make_header(&h, 1);
	make_entries(e, 1);
	n = cluster_hw_snapshot_serialize(&h, e, buf, sizeof(buf));

	buf[0] ^= 0xFF; /* corrupt magic */

	v = cluster_hw_snapshot_deserialize(buf, n, &out, out_e, 1);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_MAGIC);
}

/* a buffer shorter than the header fails closed as SHORT */
UT_TEST(test_hw_snapshot_short)
{
	ClusterHwSnapshotHeader out;
	char buf[10] = { 0 };
	ClusterHwSnapshotValidity v;

	v = cluster_hw_snapshot_deserialize(buf, sizeof(buf), &out, NULL, 0);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_SHORT);
}

/* a declared n_entries beyond the caller's capacity fails closed (R6) */
UT_TEST(test_hw_snapshot_capacity_exceeded)
{
	ClusterHwSnapshotHeader h, out;
	ClusterHwSnapshotEntry e[3], out_e[2];
	char buf[256];
	size_t n;
	ClusterHwSnapshotValidity v;

	make_header(&h, 3);
	make_entries(e, 3);
	n = cluster_hw_snapshot_serialize(&h, e, buf, sizeof(buf));

	/* caller only provisioned room for 2 -> must not silently truncate */
	v = cluster_hw_snapshot_deserialize(buf, n, &out, out_e, 2);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_SHORT);
}

/* identity gate (R12): cluster + owner + shard must all match */
UT_TEST(test_hw_snapshot_identity_ok)
{
	ClusterHwSnapshotHeader h;

	make_header(&h, 0); /* system_id 0x0123..., owner 2, shard 7 */

	UT_ASSERT_EQ(cluster_hw_snapshot_identity_ok(&h, h.system_id, 2, 7), true);
	UT_ASSERT_EQ(cluster_hw_snapshot_identity_ok(&h, h.system_id + 1, 2, 7), false); /* sysid */
	UT_ASSERT_EQ(cluster_hw_snapshot_identity_ok(&h, h.system_id, 3, 7), false);	 /* owner */
	UT_ASSERT_EQ(cluster_hw_snapshot_identity_ok(&h, h.system_id, 2, 8), false);	 /* shard */
	/* expected_sysid 0 skips the cluster check (bootstrap) */
	UT_ASSERT_EQ(cluster_hw_snapshot_identity_ok(&h, 0, 2, 7), true);
}

/* generation supersede (R10): newer same-identity wins; stale/foreign does not */
UT_TEST(test_hw_snapshot_supersedes)
{
	ClusterHwSnapshotHeader cur, cand;

	make_header(&cur, 0);  /* gen 5 */
	make_header(&cand, 0); /* gen 5 */

	cand.generation = 6;
	UT_ASSERT_EQ(cluster_hw_snapshot_supersedes(&cand, &cur), true); /* newer */

	cand.generation = 5;
	UT_ASSERT_EQ(cluster_hw_snapshot_supersedes(&cand, &cur), false); /* same */

	cand.generation = 4;
	UT_ASSERT_EQ(cluster_hw_snapshot_supersedes(&cand, &cur), false); /* older */

	/* a higher generation with a different identity never supersedes */
	cand.generation = 99;
	cand.owner_node_id = 3;
	UT_ASSERT_EQ(cluster_hw_snapshot_supersedes(&cand, &cur), false);
}

/* R3/R11 rebuild merge: max + conservative hole (never lower than current) */
UT_TEST(test_hw_snapshot_rebuild_value)
{
	/* R3 max: a higher candidate (e.g. a WAL-tail HW_RESERVE) raises the HWM */
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(100, 108), 108);
	/* R11 conservative hole: a LOWER candidate (a tail value below the snapshot)
	 * never lowers it -- the gap is an unused hole, not a re-handed range */
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(108, 100), 108);
	/* idempotent at equality */
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(108, 108), 108);
	/* first sight from zero, and a zero candidate never lowers */
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(0, 5), 5);
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(5, 0), 5);
	/* a large snapshot is preserved against a smaller replayed tail (R11:
	 * "never < any replied end" -- the snapshot already dominates that end) */
	UT_ASSERT_EQ(cluster_hw_snapshot_rebuild_value(200, 150), 200);
}

/* ======================================================================
 * S3a -- durable torn-safe file I/O round trip (R1/R2/R6)
 * ====================================================================== */

/* per-master path scheme: <shared>/global/pg_hw_snapshot.<owner> */
UT_TEST(test_hw_snapshot_path)
{
	char path[MAXPGPATH];
	char expect[MAXPGPATH];

	UT_ASSERT_EQ(cluster_hw_snapshot_path(3, path, sizeof(path)), true);
	snprintf(expect, sizeof(expect), "%s/global/pg_hw_snapshot.3", shared_root);
	UT_ASSERT_STR_EQ(path, expect);
}

/* write -> read recovers the envelope and the entries through a real file */
UT_TEST(test_hw_snapshot_write_read_roundtrip)
{
	ClusterHwSnapshotEntry e[3], out_e[8];
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotValidity v;
	uint32 i;

	make_entries(e, 3);
	cluster_hw_snapshot_write(2, 7, 5, UINT64CONST(0x00000000abcd0000),
							  UINT64CONST(0x0123456789abcdef), CLUSTER_HW_SNAPSHOT_CHECKPOINT, e,
							  3);

	v = cluster_hw_snapshot_read(2, UINT64CONST(0x0123456789abcdef), 2, 7, &hdr, out_e, 8);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_VALID);
	UT_ASSERT_EQ(hdr.owner_node_id, 2);
	UT_ASSERT_EQ(hdr.shard_partition, 7);
	UT_ASSERT_EQ(hdr.generation, 5);
	UT_ASSERT_EQ(hdr.snapshot_lsn, UINT64CONST(0x00000000abcd0000));
	UT_ASSERT_EQ(hdr.snapshot_kind, CLUSTER_HW_SNAPSHOT_CHECKPOINT);
	UT_ASSERT_EQ(hdr.n_entries, 3);
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ(out_e[i].resid.field1, 100 + i);
		UT_ASSERT_EQ(out_e[i].next_hwm, (i + 1) * 1000);
	}
}

/* a missing per-master file fails closed as SHORT (never rebuild from zero) */
UT_TEST(test_hw_snapshot_read_missing)
{
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotEntry out_e[8];
	ClusterHwSnapshotValidity v;

	v = cluster_hw_snapshot_read(99, 0, 99, 0, &hdr, out_e, 8);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_SHORT);
}

/* a torn on-disk image is caught by the CRC */
UT_TEST(test_hw_snapshot_read_crc_corrupt)
{
	ClusterHwSnapshotEntry e[2], out_e[8];
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotValidity v;
	char path[MAXPGPATH];

	make_entries(e, 2);
	cluster_hw_snapshot_write(4, 0, 1, 0, UINT64CONST(0xDEAD), CLUSTER_HW_SNAPSHOT_CHECKPOINT, e,
							  2);

	cluster_hw_snapshot_path(4, path, sizeof(path));
	flip_byte(path, CLUSTER_HW_SNAPSHOT_HEADER_SIZE + 2);

	v = cluster_hw_snapshot_read(4, UINT64CONST(0xDEAD), 4, 0, &hdr, out_e, 8);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_CRC);
}

/* a CRC-valid but foreign / mis-bound image is rejected on identity (R6) */
UT_TEST(test_hw_snapshot_read_identity_mismatch)
{
	ClusterHwSnapshotEntry e[1], out_e[8];
	ClusterHwSnapshotHeader hdr;
	ClusterHwSnapshotValidity v;

	make_entries(e, 1);
	cluster_hw_snapshot_write(5, 7, 1, 0, UINT64CONST(0xBEEF), CLUSTER_HW_SNAPSHOT_CHECKPOINT, e,
							  1);

	/* file is owner 5 / shard 7; a reader expecting owner 6 rejects it */
	v = cluster_hw_snapshot_read(5, UINT64CONST(0xBEEF), 6, 7, &hdr, out_e, 8);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_IDENTITY);
	/* a foreign system_id likewise rejects */
	v = cluster_hw_snapshot_read(5, UINT64CONST(0x9999), 5, 7, &hdr, out_e, 8);
	UT_ASSERT_EQ(v, CLUSTER_HW_SNAPSHOT_INVALID_IDENTITY);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_hw_snapshot_serialized_size);
	UT_RUN(test_hw_snapshot_roundtrip);
	UT_RUN(test_hw_snapshot_zero_entries);
	UT_RUN(test_hw_snapshot_crc_corrupt);
	UT_RUN(test_hw_snapshot_magic_corrupt);
	UT_RUN(test_hw_snapshot_short);
	UT_RUN(test_hw_snapshot_capacity_exceeded);
	UT_RUN(test_hw_snapshot_identity_ok);
	UT_RUN(test_hw_snapshot_supersedes);
	UT_RUN(test_hw_snapshot_rebuild_value);

	setup_shared_root();
	UT_RUN(test_hw_snapshot_path);
	UT_RUN(test_hw_snapshot_write_read_roundtrip);
	UT_RUN(test_hw_snapshot_read_missing);
	UT_RUN(test_hw_snapshot_read_crc_corrupt);
	UT_RUN(test_hw_snapshot_read_identity_mismatch);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
