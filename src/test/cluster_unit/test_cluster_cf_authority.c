/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_authority.c
 *	  Runtime unit tests for the shared pg_control authority module
 *	  (spec-5.6 Da1): path resolution, the pure buffer-classification and
 *	  read-source decision logic, and the real durable_rename read/write
 *	  round trip with .bak corruption fallback.
 *
 *	  Like test_cluster_shared_fs_sharedfs.c this exercises the module for
 *	  REAL against a temp directory: the fd.c openers map onto open(2)/
 *	  close(2), pg_fsync onto fsync(2), and durable_rename onto rename(2)
 *	  plus a best-effort directory fsync, so the atomic-replacement and
 *	  .bak-fallback behaviour is verified end to end without a server.
 *	  The errstart stub aborts on elevel >= ERROR so any unexpected error
 *	  path fails this binary loudly.
 *
 *	  Covers (spec §4.1):
 *	    U2  cluster_cf_shared_path() / cluster_cf_bak_path()
 *	    U3  classify VALID / short / CRC / byte-order / identity;
 *	        decide PRIMARY / BAK / FAILCLOSED (incl. strict-gate);
 *	        write -> read primary round trip; corrupt-primary -> .bak
 *	        fallback; corrupt-both -> fail-closed (read returns false)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da1, U2/U3/U9)
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

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_stats.h"
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

/* ----------
 * Globals read by cluster_cf_authority.o.
 * ----------
 */
char	   *cluster_shared_data_dir = NULL;

/* ----------
 * Assert + ereport machinery.  ereport(ERROR/FATAL) must not return, so the
 * stub aborts; cluster_cf_authority_read() never ereports (it returns false
 * to fail-closed), so reaching the abort would be a real bug.
 * ----------
 */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
	{
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

int errcode(int sqlerrcode pg_attribute_unused()) { return 0; }
int errcode_for_file_access(void) { return 0; }
int errmsg(const char *fmt pg_attribute_unused(),...) { return 0; }
int errmsg_internal(const char *fmt pg_attribute_unused(),...) { return 0; }
int errdetail(const char *fmt pg_attribute_unused(),...) { return 0; }
int errhint(const char *fmt pg_attribute_unused(),...) { return 0; }

/* ----------
 * Functional fd.c stubs: map straight onto the kernel.
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
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	if (rename(oldfile, newfile) != 0)
		return -1;
	return 0;
}

/* ----------
 * Dc2: test-controlled stub for the .bak checkpoint-recoverable probe.
 * cluster_cf_authority_read() consults this when it falls back to a .bak; the
 * real backend definition (cluster_cf_storage.c) stats the redo WAL segment
 * under pg_wal (e2e L14).  Driving it from a global lets a test prove that a
 * CRC-valid .bak with an unrecoverable checkpoint is still rejected.
 * ----------
 */
static bool stub_recoverable = true;

bool
cluster_cf_bak_checkpoint_recoverable(const ControlFileData *bak pg_attribute_unused())
{
	return stub_recoverable;
}

/* spec-5.6 Dc4: cluster_cf_authority.o bumps the .bak-fallback CF counter;
 * cluster_cf_stats.o is not linked here, so a no-op stub satisfies the link
 * (the counter mechanism is covered by test_cluster_cf_stats). */
void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

/* ----------
 * Test fixture: a temp shared root with a global/ subdir.
 * ----------
 */
static char shared_root[MAXPGPATH];

static void
setup_shared_root(void)
{
	char		tmpl[MAXPGPATH];
	char		globaldir[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_cf_authority_XXXXXX", sizeof(tmpl));
	if (mkdtemp(tmpl) == NULL)
	{
		printf("# mkdtemp failed: %s\n", strerror(errno));
		abort();
	}
	strlcpy(shared_root, tmpl, sizeof(shared_root));
	cluster_shared_data_dir = shared_root;

	snprintf(globaldir, sizeof(globaldir), "%s/global", shared_root);
	if (mkdir(globaldir, 0700) != 0 && errno != EEXIST)
	{
		printf("# mkdir global failed: %s\n", strerror(errno));
		abort();
	}
}

/* Build a syntactically valid ControlFileData with the given identity. */
static void
build_cf(ControlFileData *cf, uint64 sysid)
{
	memset(cf, 0, sizeof(*cf));
	cf->pg_control_version = PG_CONTROL_VERSION;
	cf->catalog_version_no = 0;
	cf->system_identifier = sysid;
	cf->state = DB_IN_PRODUCTION;
}

/* Recompute the embedded CRC the same way update_controlfile does. */
static void
finalize_crc(ControlFileData *cf)
{
	INIT_CRC32C(cf->crc);
	COMP_CRC32C(cf->crc, (char *) cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf->crc);
}

/* Flip one byte at the given offset of a file (corrupts its CRC). */
static void
flip_byte(const char *path, off_t off)
{
	int			fd = open(path, O_RDWR);
	unsigned char b;

	if (fd < 0)
	{
		printf("# flip_byte open %s failed: %s\n", path, strerror(errno));
		abort();
	}
	if (pread(fd, &b, 1, off) != 1)
	{
		printf("# flip_byte pread failed\n");
		abort();
	}
	b ^= 0xFF;
	if (pwrite(fd, &b, 1, off) != 1)
	{
		printf("# flip_byte pwrite failed\n");
		abort();
	}
	close(fd);
}

/* ======================================================================
 * U2 -- path accessors
 * ====================================================================== */
UT_TEST(test_paths)
{
	char		expect_primary[MAXPGPATH];
	char		expect_bak[MAXPGPATH];

	snprintf(expect_primary, sizeof(expect_primary), "%s/global/pg_control", shared_root);
	snprintf(expect_bak, sizeof(expect_bak), "%s/global/pg_control.bak", shared_root);

	UT_ASSERT_STR_EQ(cluster_cf_shared_path(), expect_primary);
	UT_ASSERT_STR_EQ(cluster_cf_bak_path(), expect_bak);
}

/* ======================================================================
 * U3 (pure) -- classify_buffer
 * ====================================================================== */
UT_TEST(test_classify_buffer)
{
	ControlFileData cf;

	build_cf(&cf, 0xABCDEF0123456789ULL);
	finalize_crc(&cf);

	/* good image, no identity expectation */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf), 0),
				 CLUSTER_CF_VALID);
	/* good image, matching identity */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf),
											0xABCDEF0123456789ULL),
				 CLUSTER_CF_VALID);
	/* good CRC but foreign identity */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf),
											0x1111111111111111ULL),
				 CLUSTER_CF_INVALID_IDENTITY);
	/* short buffer */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf) - 1, 0),
				 CLUSTER_CF_INVALID_SHORT);

	/* torn CRC */
	((char *) &cf)[4] ^= 0xFF;
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf), 0),
				 CLUSTER_CF_INVALID_CRC);

	/* foreign byte order: version a nonzero multiple of 65536 */
	build_cf(&cf, 1);
	cf.pg_control_version = 65536;
	finalize_crc(&cf);
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *) &cf, sizeof(cf), 0),
				 CLUSTER_CF_INVALID_BYTE_ORDER);
}

/* ======================================================================
 * U3/U9 (pure) -- decide_source
 * ====================================================================== */
UT_TEST(test_decide_source)
{
	/* primary valid always wins */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_VALID,
										  CLUSTER_CF_INVALID_CRC, false),
				 CLUSTER_CF_SOURCE_PRIMARY);
	/* primary bad, bak valid AND strict-ok -> use bak */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC,
										  CLUSTER_CF_VALID, true),
				 CLUSTER_CF_SOURCE_BAK);
	/* primary bad, bak CRC-valid but strict NOT ok -> fail closed */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC,
										  CLUSTER_CF_VALID, false),
				 CLUSTER_CF_SOURCE_FAILCLOSED);
	/* primary bad, bak also bad -> fail closed */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC,
										  CLUSTER_CF_INVALID_CRC, true),
				 CLUSTER_CF_SOURCE_FAILCLOSED);
	/* identity mismatch on primary is never silently trusted */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_IDENTITY,
										  CLUSTER_CF_INVALID_IDENTITY, true),
				 CLUSTER_CF_SOURCE_FAILCLOSED);
}

/* ======================================================================
 * U3 -- write -> read primary round trip
 * ====================================================================== */
UT_TEST(test_write_read_roundtrip)
{
	ControlFileData in;
	ControlFileData out;

	build_cf(&in, 0x0102030405060708ULL);
	cluster_cf_authority_write(&in);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.system_identifier, 0x0102030405060708ULL);
	UT_ASSERT_EQ(out.state, DB_IN_PRODUCTION);
}

/* ======================================================================
 * U3 -- corrupt primary -> .bak fallback
 * ====================================================================== */
UT_TEST(test_bak_fallback)
{
	ControlFileData v1;
	ControlFileData v2;
	ControlFileData out;

	/* first write establishes the primary (no prior .bak) */
	build_cf(&v1, 0x1111000011110000ULL);
	cluster_cf_authority_write(&v1);
	/* second write rolls the primary into .bak, installs v2 as primary */
	build_cf(&v2, 0x2222000022220000ULL);
	cluster_cf_authority_write(&v2);

	/* corrupt the primary -> read must fall back to the (valid) .bak = v1 */
	flip_byte(cluster_cf_shared_path(), 8);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.system_identifier, 0x1111000011110000ULL);
}

/* ======================================================================
 * U3 -- corrupt primary AND .bak -> fail-closed (read returns false)
 * ====================================================================== */
UT_TEST(test_both_bad_failclosed)
{
	ControlFileData v1;
	ControlFileData v2;
	ControlFileData out;

	build_cf(&v1, 0x3333000033330000ULL);
	cluster_cf_authority_write(&v1);
	build_cf(&v2, 0x4444000044440000ULL);
	cluster_cf_authority_write(&v2);

	flip_byte(cluster_cf_shared_path(), 8);
	flip_byte(cluster_cf_bak_path(), 8);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(!cluster_cf_authority_read(&out));
}

/* ======================================================================
 * U10 (pure) -- strict .bak acceptance logic
 *
 * A .bak is acceptable only when it is structurally valid AND its
 * checkpoint is recoverable AND (when an expected identity is supplied)
 * its system_identifier matches.  A CRC-valid-but-stale/unreplayable .bak
 * must be rejected (spec §3.9 T3): silently replaying from an unreachable
 * checkpoint would corrupt recovery.
 * ====================================================================== */
UT_TEST(test_bak_strict_ok)
{
	ControlFileData bak;

	build_cf(&bak, 0xAAAA0000AAAA0000ULL);
	finalize_crc(&bak);

	/* all conditions met -> accept (no identity expectation) */
	UT_ASSERT(cluster_cf_bak_strict_ok(&bak, 0, true));
	/* matching expected identity -> accept */
	UT_ASSERT(cluster_cf_bak_strict_ok(&bak, 0xAAAA0000AAAA0000ULL, true));
	/* checkpoint not reachable/replayable -> reject (U10 core) */
	UT_ASSERT(!cluster_cf_bak_strict_ok(&bak, 0, false));
	/* foreign expected identity -> reject */
	UT_ASSERT(!cluster_cf_bak_strict_ok(&bak, 0x1111111111111111ULL, true));
	/* NULL image -> reject */
	UT_ASSERT(!cluster_cf_bak_strict_ok(NULL, 0, true));
}

/* ======================================================================
 * U10 -- corrupt primary + CRC-valid .bak whose checkpoint is NOT
 * recoverable -> authority read fails closed (does not return the .bak).
 * ====================================================================== */
UT_TEST(test_bak_fallback_unrecoverable)
{
	ControlFileData v1;
	ControlFileData v2;
	ControlFileData out;

	build_cf(&v1, 0x5555000055550000ULL);
	cluster_cf_authority_write(&v1);
	build_cf(&v2, 0x6666000066660000ULL);
	cluster_cf_authority_write(&v2);	/* rolls v1 into .bak */

	/* corrupt the primary; the .bak (v1) is CRC-valid but its checkpoint is
	 * declared unreachable -> the read must fail-closed, not return v1. */
	flip_byte(cluster_cf_shared_path(), 8);
	stub_recoverable = false;

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(!cluster_cf_authority_read(&out));

	stub_recoverable = true;			/* restore for any later test */
}

int
main(void)
{
	setup_shared_root();

	UT_PLAN(8);
	UT_RUN(test_paths);
	UT_RUN(test_classify_buffer);
	UT_RUN(test_decide_source);
	UT_RUN(test_write_read_roundtrip);
	UT_RUN(test_bak_fallback);
	UT_RUN(test_both_bad_failclosed);
	UT_RUN(test_bak_strict_ok);
	UT_RUN(test_bak_fallback_unrecoverable);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
