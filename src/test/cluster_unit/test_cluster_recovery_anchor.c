/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_anchor.c
 *	  Runtime unit tests for the per-node recovery anchor module
 *	  (spec-5.6a D1): on-disk layout, the pure image classification, and
 *	  the real torn-safe read/write round trip with .bak fallback,
 *	  exercised against a temp directory exactly like
 *	  test_cluster_cf_authority.c (fd.c openers map onto open(2)/close(2),
 *	  durable_rename onto rename(2)).
 *
 *	  Covers (spec §4):
 *	    U1  layout: field offsets and total size (mirrors the compile-time
 *	        StaticAssertDecl set at runtime)
 *	    U2  write -> read primary round trip + path accessors
 *	    U3  CRC corruption -> INVALID_CRC; magic/version -> INVALID_MAGIC
 *	    U4  foreign system_identifier -> INVALID_IDENTITY
 *	    U5  foreign node_id -> INVALID_IDENTITY
 *	    U6  short image -> INVALID_SHORT
 *	    U7  corrupt primary -> .bak adoption (used_bak reported)
 *	    U8  corrupt primary AND .bak -> read fails closed
 *	    U9  build_from_controlfile field mapping
 *	    U10 state carrier round trip (uint32 <-> DBState)
 *	    U11 publish_checkpoint -> read round trip
 *	    U12 refresh_state: no-op without an anchor, state-only update on a
 *	        valid one (checkpoint fields preserved)
 *	    U13 boot-time load/active/get adoption statics
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_recovery_anchor.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6a-per-node-recovery-anchor.md (D1, §4 U1-U10)
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

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
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
 * Globals read by cluster_recovery_anchor.o.
 * ----------
 */
char *cluster_shared_data_dir = NULL;
int cluster_node_id = 3;
bool cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
static ClusterMembershipState test_membership_state = CLUSTER_MEMBER_MEMBER;
static uint64 test_self_incarnation = UINT64_C(77);
static uint64 test_admitted_incarnation = UINT64_C(77);
static bool test_owner_eor_active;
static bool test_exactly_one_declared_node = true;
static bool test_cf_local_x_held = true;
static int test_write_fence_calls;
static ClusterStartupPhase test_startup_phase = CLUSTER_PHASE_RUNNING;
static TimestampTz test_phase4_started_at = 1000;
static ClusterStatsStatus test_stats_status = CLUSTER_STATS_READY;
static TimestampTz test_stats_spawned_at = 2000;
static uint16 test_wal_thread_id = 4;
static uint16 test_dump_thread_id = 4;
static bool test_wal_thread_dir_configured = true;
static bool test_wal_thread_dir_validated = true;
static bool test_wal_registry_ready = true;
static ClusterWalSlotVerdict test_wal_slot_verdict = CLUSTER_WAL_SLOT_OK;
static ClusterWalStateSlot test_wal_slot;
static int test_wal_slot_read_calls;
static uint16 test_wal_slot_last_read_thread;
static bool test_cf_x_held = true;
static bool test_phase4_drift_on_second_fence;
static bool test_write_fence_allowed = true;
static bool test_expect_panic;
static jmp_buf test_panic_jump;

ClusterMembershipState
cluster_membership_get_state(int32 node_id pg_attribute_unused())
{
	return test_membership_state;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id pg_attribute_unused())
{
	return test_admitted_incarnation;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	return test_self_incarnation;
}

ClusterStartupPhase
cluster_current_phase(void)
{
	return test_startup_phase;
}

TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	return phase == CLUSTER_PHASE_4_NORMAL ? test_phase4_started_at : 0;
}

ClusterStatsStatus
cluster_stats_status(void)
{
	return test_stats_status;
}

TimestampTz
cluster_stats_spawned_at(void)
{
	return test_stats_spawned_at;
}

uint16
cluster_wal_thread_id(void)
{
	return test_wal_thread_id;
}

uint16
cluster_wal_thread_dump_thread_id(void)
{
	return test_dump_thread_id;
}

bool
cluster_wal_thread_dir_configured(void)
{
	return test_wal_thread_dir_configured;
}

bool
cluster_wal_thread_dir_validated(void)
{
	return test_wal_thread_dir_validated;
}

bool
cluster_wal_state_registry_ready(void)
{
	return test_wal_registry_ready;
}

ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id, ClusterWalStateSlot *slot_out)
{
	test_wal_slot_read_calls++;
	test_wal_slot_last_read_thread = thread_id;
	if (slot_out != NULL)
		*slot_out = test_wal_slot;
	return test_wal_slot_verdict;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return mode == ExclusiveLock && test_cf_x_held;
}

bool
cluster_cf_held(LOCKMODE mode)
{
	return mode == ExclusiveLock && test_cf_local_x_held;
}

bool
cluster_cf_exactly_one_declared_node(void)
{
	return test_exactly_one_declared_node;
}

void
cluster_write_fence_reject_if_fenced(const char *op pg_attribute_unused())
{
	/* RF-ROOT P7 G1b (contract): only the post-publication reject
	 * remains (the entry pre-check skips via allowed); the drift fixture
	 * flips the slot on that single call. */
	test_write_fence_calls++;
	if (test_phase4_drift_on_second_fence && test_write_fence_calls == 1)
		test_wal_slot.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
}

/* RF-ROOT P7 G1b (contract): the entry pre-check skips publication
 * when fenced.  test_write_fence_allowed drives the entry gate; the
 * post-publication reject (still PANIC-capable) governs the drift case. */
bool
cluster_write_fence_allowed(void)
{
	return test_write_fence_allowed;
}

bool
cluster_cf_owner_eor_local_active(void)
{
	return test_owner_eor_active;
}

/* ----------
 * Assert + ereport machinery.  The anchor read path never ereports (it
 * returns false to fail-closed) and the write path PANICs only on real I/O
 * failure, so reaching the abort in this stub means a real bug.
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
	if (elevel >= ERROR) {
		if (test_expect_panic)
			longjmp(test_panic_jump, 1);
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

/* spec-5.6a D5: the module bumps CF observability counters; cluster_cf_stats.o
 * is not linked here, so a no-op stub satisfies the link (the counter
 * mechanism itself is covered by test_cluster_cf_stats). */
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
	char tmpl[MAXPGPATH];
	char globaldir[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_recovery_anchor_XXXXXX", sizeof(tmpl));
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

/* Remove the anchor file family so each leg starts from a clean slate. */
static void
wipe_anchor_files(void)
{
	char path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/global/pgrac_recovery_anchor_n%d", shared_root,
			 cluster_node_id);
	unlink(path);
	snprintf(path, sizeof(path), "%s/global/pgrac_recovery_anchor_n%d.bak", shared_root,
			 cluster_node_id);
	unlink(path);
	test_membership_state = CLUSTER_MEMBER_MEMBER;
	test_self_incarnation = UINT64_C(77);
	test_admitted_incarnation = UINT64_C(77);
	test_owner_eor_active = false;
	cluster_enabled = true;
	cluster_controlfile_shared_authority = true;
	test_exactly_one_declared_node = true;
	test_cf_local_x_held = true;
	test_write_fence_calls = 0;
	test_startup_phase = CLUSTER_PHASE_RUNNING;
	test_phase4_started_at = 1000;
	test_stats_status = CLUSTER_STATS_READY;
	test_stats_spawned_at = 2000;
	test_wal_thread_id = 4;
	test_dump_thread_id = 4;
	test_wal_thread_dir_configured = true;
	test_wal_thread_dir_validated = true;
	test_wal_registry_ready = true;
	test_wal_slot_verdict = CLUSTER_WAL_SLOT_OK;
	cluster_wal_state_slot_fill(&test_wal_slot, test_wal_thread_id, cluster_node_id,
								CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, test_stats_spawned_at,
								test_stats_spawned_at + 1, 0x1000, 0x2000);
	test_wal_slot_read_calls = 0;
	test_wal_slot_last_read_thread = 0;
	test_cf_x_held = true;
	test_phase4_drift_on_second_fence = false;
	test_expect_panic = false;
}

#define TEST_SYSID 0xABCDEF0123456789ULL

/* Build a syntactically valid anchor owned by cluster_node_id. */
static void
build_anchor(ClusterRecoveryAnchor *ra, XLogRecPtr lsn)
{
	memset(ra, 0, sizeof(*ra));
	ra->magic = CLUSTER_RECOVERY_ANCHOR_MAGIC;
	ra->version = CLUSTER_RECOVERY_ANCHOR_VERSION;
	ra->node_id = cluster_node_id;
	ra->state = DB_IN_PRODUCTION;
	ra->system_identifier = TEST_SYSID;
	ra->checkPoint = lsn;
	ra->checkPointCopy.redo = lsn - 8;
	ra->checkPointCopy.ThisTimeLineID = 1;
	ra->checkPointCopy.PrevTimeLineID = 1;
}

/* Recompute the embedded CRC the same way the module's writer does. */
static void
finalize_crc(ClusterRecoveryAnchor *ra)
{
	INIT_CRC32C(ra->crc);
	COMP_CRC32C(ra->crc, (char *)ra, offsetof(ClusterRecoveryAnchor, crc));
	FIN_CRC32C(ra->crc);
}

/* Flip one byte at the given offset of a file (corrupts its CRC). */
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

/* ======================================================================
 * U1 -- on-disk layout (runtime mirror of the StaticAssertDecl set)
 * ====================================================================== */
UT_TEST(test_layout)
{
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, magic), 0);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, version), 4);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, node_id), 8);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, state), 12);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, system_identifier), 16);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, checkPoint), 24);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, write_time), 32);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, checkPointCopy), 40);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, unloggedLSN), 128);
	UT_ASSERT_EQ(offsetof(ClusterRecoveryAnchor, crc), 508);
	UT_ASSERT_EQ(sizeof(ClusterRecoveryAnchor), CLUSTER_RECOVERY_ANCHOR_SIZE);
}

/* ======================================================================
 * U2 -- path accessors + write -> read primary round trip
 * ====================================================================== */
UT_TEST(test_write_read_roundtrip)
{
	ClusterRecoveryAnchor in;
	ClusterRecoveryAnchor out;
	bool used_bak = true;
	char expect_primary[MAXPGPATH];
	char expect_bak[MAXPGPATH];

	wipe_anchor_files();

	snprintf(expect_primary, sizeof(expect_primary), "%s/global/pgrac_recovery_anchor_n%d",
			 shared_root, cluster_node_id);
	snprintf(expect_bak, sizeof(expect_bak), "%s/global/pgrac_recovery_anchor_n%d.bak", shared_root,
			 cluster_node_id);
	UT_ASSERT_STR_EQ(cluster_recovery_anchor_path(), expect_primary);
	UT_ASSERT_STR_EQ(cluster_recovery_anchor_bak_path(), expect_bak);

	build_anchor(&in, 0x0000000112345678ULL);
	cluster_recovery_anchor_write(&in);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT(!used_bak);
	UT_ASSERT_EQ(out.checkPoint, 0x0000000112345678ULL);
	UT_ASSERT_EQ(out.checkPointCopy.redo, 0x0000000112345670ULL);
	UT_ASSERT_EQ(out.node_id, cluster_node_id);
	UT_ASSERT_EQ(out.system_identifier, TEST_SYSID);
	UT_ASSERT_EQ(out.state, (uint32)DB_IN_PRODUCTION);
}

/* ======================================================================
 * U3/U4/U5/U6 -- pure classifier legs
 * ====================================================================== */
UT_TEST(test_classify)
{
	ClusterRecoveryAnchor ra;

	/* valid image */
	build_anchor(&ra, 0x1000);
	finalize_crc(&ra);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id),
		CLUSTER_RA_VALID);

	/* U6: short image */
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra) - 1, TEST_SYSID, cluster_node_id),
		CLUSTER_RA_INVALID_SHORT);
	UT_ASSERT_EQ(cluster_recovery_anchor_classify(NULL, 0, TEST_SYSID, cluster_node_id),
				 CLUSTER_RA_INVALID_SHORT);

	/* U3: torn CRC */
	((char *)&ra)[24] ^= 0xFF;
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id),
		CLUSTER_RA_INVALID_CRC);

	/* U3: CRC-valid but foreign magic */
	build_anchor(&ra, 0x1000);
	ra.magic = 0x11111111;
	finalize_crc(&ra);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id),
		CLUSTER_RA_INVALID_MAGIC);

	/* U3: CRC-valid but foreign version */
	build_anchor(&ra, 0x1000);
	ra.version = CLUSTER_RECOVERY_ANCHOR_VERSION + 1;
	finalize_crc(&ra);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id),
		CLUSTER_RA_INVALID_MAGIC);

	/* U4: foreign system identifier */
	build_anchor(&ra, 0x1000);
	finalize_crc(&ra);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID + 1, cluster_node_id),
		CLUSTER_RA_INVALID_IDENTITY);

	/* U5: foreign node id (another node's anchor behind this path) */
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id + 1),
		CLUSTER_RA_INVALID_IDENTITY);
}

/* ======================================================================
 * U7 -- corrupt primary -> .bak adoption
 * ====================================================================== */
UT_TEST(test_bak_fallback)
{
	ClusterRecoveryAnchor v1;
	ClusterRecoveryAnchor v2;
	ClusterRecoveryAnchor out;
	bool used_bak = false;

	wipe_anchor_files();

	/* first write establishes the primary (no prior .bak) */
	build_anchor(&v1, 0x1111000011110000ULL);
	cluster_recovery_anchor_write(&v1);
	/* second write rolls the primary into .bak, installs v2 as primary */
	build_anchor(&v2, 0x2222000022220000ULL);
	cluster_recovery_anchor_write(&v2);

	/* corrupt the primary -> read must adopt the (valid) .bak = v1 */
	flip_byte(cluster_recovery_anchor_path(), 24);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT(used_bak);
	UT_ASSERT_EQ(out.checkPoint, 0x1111000011110000ULL);
}

/* ======================================================================
 * U8 -- corrupt primary AND .bak -> fail-closed (read returns false)
 * ====================================================================== */
UT_TEST(test_both_bad_failclosed)
{
	ClusterRecoveryAnchor v1;
	ClusterRecoveryAnchor v2;
	ClusterRecoveryAnchor out;
	bool used_bak = false;

	wipe_anchor_files();

	build_anchor(&v1, 0x3333000033330000ULL);
	cluster_recovery_anchor_write(&v1);
	build_anchor(&v2, 0x4444000044440000ULL);
	cluster_recovery_anchor_write(&v2);

	flip_byte(cluster_recovery_anchor_path(), 24);
	flip_byte(cluster_recovery_anchor_bak_path(), 24);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	/* missing entirely is likewise fail-closed */
	wipe_anchor_files();
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	/* a valid file behind a foreign expected sysid is fail-closed too */
	build_anchor(&v1, 0x5555000055550000ULL);
	cluster_recovery_anchor_write(&v1);
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID + 7, &out, &used_bak));
}

/* ======================================================================
 * U9 -- build_from_controlfile field mapping
 * ====================================================================== */
UT_TEST(test_build_from_controlfile)
{
	ControlFileData cf;
	ClusterRecoveryAnchor ra;

	memset(&cf, 0, sizeof(cf));
	cf.system_identifier = TEST_SYSID;
	cf.state = DB_SHUTDOWNED;
	cf.checkPoint = 0x0000000212340000ULL;
	cf.checkPointCopy.redo = 0x0000000212330000ULL;
	cf.checkPointCopy.ThisTimeLineID = 5;
	cf.checkPointCopy.nextOid = 24576;
	cf.unloggedLSN = 0x0000000000004321ULL;

	memset(&ra, 0xEE, sizeof(ra));
	cluster_recovery_anchor_build_from_controlfile(&cf, &ra);

	UT_ASSERT_EQ(ra.magic, CLUSTER_RECOVERY_ANCHOR_MAGIC);
	UT_ASSERT_EQ(ra.version, CLUSTER_RECOVERY_ANCHOR_VERSION);
	UT_ASSERT_EQ(ra.node_id, cluster_node_id);
	UT_ASSERT_EQ(ra.state, (uint32)DB_SHUTDOWNED);
	UT_ASSERT_EQ(ra.system_identifier, TEST_SYSID);
	UT_ASSERT_EQ(ra.checkPoint, 0x0000000212340000ULL);
	UT_ASSERT_EQ(ra.checkPointCopy.redo, 0x0000000212330000ULL);
	UT_ASSERT_EQ(ra.checkPointCopy.ThisTimeLineID, 5);
	UT_ASSERT_EQ(ra.checkPointCopy.nextOid, 24576);
	UT_ASSERT_EQ(ra.unloggedLSN, 0x0000000000004321ULL);
	/* the built image classifies VALID once CRC'd (writer does that) */
	finalize_crc(&ra);
	UT_ASSERT_EQ(
		cluster_recovery_anchor_classify((char *)&ra, sizeof(ra), TEST_SYSID, cluster_node_id),
		CLUSTER_RA_VALID);
}

/* ======================================================================
 * U10 -- state carrier round trip (uint32 <-> DBState)
 * ====================================================================== */
UT_TEST(test_state_carrier)
{
	ClusterRecoveryAnchor ra;
	ClusterRecoveryAnchor out;
	bool used_bak;
	static const DBState states[] = { DB_SHUTDOWNED, DB_IN_PRODUCTION };
	int i;

	for (i = 0; i < (int)lengthof(states); i++) {
		wipe_anchor_files();
		build_anchor(&ra, 0x9000 + i);
		ra.state = (uint32)states[i];
		cluster_recovery_anchor_write(&ra);

		memset(&out, 0xEE, sizeof(out));
		UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
		UT_ASSERT_EQ((DBState)out.state, states[i]);
	}
}

/* ======================================================================
 * U11 -- publish_checkpoint -> read round trip
 * ====================================================================== */
UT_TEST(test_publish_checkpoint)
{
	CheckPoint cp;
	ClusterRecoveryAnchor out;
	bool used_bak = true;

	wipe_anchor_files();

	memset(&cp, 0, sizeof(cp));
	cp.redo = 0x0000000398760000ULL;
	cp.ThisTimeLineID = 1;
	cp.PrevTimeLineID = 1;
	cp.nextOid = 40960;

	cluster_recovery_anchor_publish_checkpoint(0x0000000398770000ULL, &cp, TEST_SYSID,
											   (uint32)DB_SHUTDOWNED, 0x0000000000009999ULL);

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT(!used_bak);
	UT_ASSERT_EQ(out.checkPoint, 0x0000000398770000ULL);
	UT_ASSERT_EQ(out.checkPointCopy.redo, 0x0000000398760000ULL);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, 40960);
	UT_ASSERT_EQ(out.state, (uint32)DB_SHUTDOWNED);
	UT_ASSERT_EQ(out.node_id, cluster_node_id);
	UT_ASSERT_EQ(out.unloggedLSN, 0x0000000000009999ULL);
}

static bool
publish_checkpoint_panics(const CheckPoint *cp)
{
	test_expect_panic = true;
	if (setjmp(test_panic_jump) == 0) {
		cluster_recovery_anchor_publish_checkpoint(0x0000000398770000ULL, cp, TEST_SYSID,
												   (uint32)DB_IN_PRODUCTION, InvalidXLogRecPtr);
		test_expect_panic = false;
		return false;
	}
	test_expect_panic = false;
	return true;
}

static void
configure_exact_phase4_publisher(void)
{
	wipe_anchor_files();
	test_membership_state = CLUSTER_MEMBER_ABSENT;
	test_admitted_incarnation = 0;
	test_startup_phase = CLUSTER_PHASE_4_NORMAL;
	test_stats_status = CLUSTER_STATS_SPAWNING;
}

static void
configure_exact_native_seed_publisher(void)
{
	wipe_anchor_files();
	cluster_enabled = false;
	test_membership_state = CLUSTER_MEMBER_ABSENT;
	test_self_incarnation = 0;
	test_admitted_incarnation = 0;
}

static void
assert_phase4_publish_rejected_before_io(const CheckPoint *cp)
{
	ClusterRecoveryAnchor out;
	bool used_bak;

	/* RF-ROOT P7 G1b (contract): the entry pre-check (allowed) no
	 * longer counts a reject call; these rejections come from the
	 * publisher-is-current predicate (PANIC, pre-write), so the fence
	 * counter stays 0. */
	UT_ASSERT(publish_checkpoint_panics(cp));
	UT_ASSERT_EQ(test_write_fence_calls, 0);
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
}

UT_TEST(test_checkpoint_publish_requires_current_owner_before_io)
{
	CheckPoint cp;
	ClusterRecoveryAnchor out;
	bool used_bak;

	memset(&cp, 0, sizeof(cp));
	cp.redo = 0x0000000398760000ULL;
	cp.ThisTimeLineID = 1;
	cp.PrevTimeLineID = 1;

	wipe_anchor_files();
	test_membership_state = CLUSTER_MEMBER_DEAD;
	UT_ASSERT(publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 0);
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	wipe_anchor_files();
	test_admitted_incarnation = test_self_incarnation + 1;
	UT_ASSERT(publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 0);
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	wipe_anchor_files();
	test_membership_state = CLUSTER_MEMBER_ABSENT;
	test_self_incarnation = 0;
	test_admitted_incarnation = 0;
	test_owner_eor_active = true;
	UT_ASSERT(!publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 1);
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT_EQ(out.checkPoint, 0x0000000398770000ULL);

	wipe_anchor_files();
	cluster_recovery_anchor_publish_checkpoint(0x0000000398770000ULL, &cp, TEST_SYSID,
											   (uint32)DB_IN_PRODUCTION, InvalidXLogRecPtr);
	UT_ASSERT_EQ(test_write_fence_calls, 1);
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT_EQ(out.checkPoint, 0x0000000398770000ULL);
}

/* ======================================================================
 * RF-ROOT P5 regression: the frozen shared-catalog bootstrap contract uses
 * an exact one-node, cluster-disabled native seed.  Its clean shutdown must
 * seal the XID authority and publish the recovery anchor while holding local
 * CF(X); no formed-cluster membership or phase4 identity may be invented.
 * ====================================================================== */
UT_TEST(test_checkpoint_publish_native_seed_proof_is_exact)
{
	CheckPoint cp;
	ClusterRecoveryAnchor out;
	bool used_bak;

	memset(&cp, 0, sizeof(cp));
	cp.redo = 0x0000000398760000ULL;
	cp.ThisTimeLineID = 1;
	cp.PrevTimeLineID = 1;

	configure_exact_native_seed_publisher();
	UT_ASSERT(!publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 1);
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	configure_exact_native_seed_publisher();
	cluster_enabled = true;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	cluster_controlfile_shared_authority = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	test_exactly_one_declared_node = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	test_cf_local_x_held = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	test_self_incarnation = UINT64_C(77);
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	test_membership_state = CLUSTER_MEMBER_MEMBER;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_native_seed_publisher();
	test_admitted_incarnation = UINT64_C(77);
	assert_phase4_publish_rejected_before_io(&cp);
}

/* ======================================================================
 * RF-ROOT P6: the mandatory initial phase4 checkpoint precedes ordinary
 * membership admission.  Its sole additional publisher proof is the exact
 * current Cluster Stats SPAWNING incarnation, own validated WAL thread and
 * ACTIVE slot, under a real coordinated CF(X).  Every mismatch fails before
 * I/O, and the complete predicate is repeated after durable publication.
 * ====================================================================== */
UT_TEST(test_checkpoint_publish_phase4_boot_proof_is_exact)
{
	CheckPoint cp;
	ClusterRecoveryAnchor out;
	bool used_bak;

	memset(&cp, 0, sizeof(cp));
	cp.redo = 0x0000000398760000ULL;
	cp.ThisTimeLineID = 1;
	cp.PrevTimeLineID = 1;

	configure_exact_phase4_publisher();
	UT_ASSERT(!publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 1);
	UT_ASSERT_EQ(test_wal_slot_read_calls, 2);
	UT_ASSERT_EQ(test_wal_slot_last_read_thread, test_wal_thread_id);
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	configure_exact_phase4_publisher();
	test_startup_phase = CLUSTER_PHASE_RUNNING;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_stats_status = CLUSTER_STATS_READY;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_self_incarnation = 0;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_thread_dir_configured = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_thread_dir_validated = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_dump_thread_id++;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_thread_id = XLP_THREAD_ID_LEGACY;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_registry_ready = false;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_slot_verdict = CLUSTER_WAL_SLOT_CORRUPT;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_slot.thread_id++;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_slot.node_id++;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_slot.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_stats_spawned_at = 0;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_phase4_started_at = 0;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_stats_spawned_at = test_phase4_started_at - 1;
	test_wal_slot.started_at = test_stats_spawned_at;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_wal_slot.started_at++;
	assert_phase4_publish_rejected_before_io(&cp);

	configure_exact_phase4_publisher();
	test_cf_x_held = false;
	assert_phase4_publish_rejected_before_io(&cp);

	/* The second fence call is between durable write and the repeated
	 * publisher predicate.  Drift there must PANIC before WAL reuse. */
	configure_exact_phase4_publisher();
	test_phase4_drift_on_second_fence = true;
	UT_ASSERT(publish_checkpoint_panics(&cp));
	UT_ASSERT_EQ(test_write_fence_calls, 1);
	UT_ASSERT_EQ(test_wal_slot_read_calls, 2);
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
}

/* ======================================================================
 * U12 -- refresh_state: no-op without an anchor; state-only on a valid one
 * ====================================================================== */
UT_TEST(test_refresh_state)
{
	ClusterRecoveryAnchor ra;
	ClusterRecoveryAnchor out;
	bool used_bak;

	wipe_anchor_files();

	/* no anchor -> no-op, nothing created */
	UT_ASSERT(!cluster_recovery_anchor_refresh_state(TEST_SYSID, (uint32)DB_IN_PRODUCTION));
	UT_ASSERT(!cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));

	/* valid anchor in SHUTDOWNED -> refresh flips only the state */
	build_anchor(&ra, 0x0000000456780000ULL);
	ra.state = (uint32)DB_SHUTDOWNED;
	cluster_recovery_anchor_write(&ra);
	UT_ASSERT(cluster_recovery_anchor_refresh_state(TEST_SYSID, (uint32)DB_IN_PRODUCTION));

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_recovery_anchor_read(TEST_SYSID, &out, &used_bak));
	UT_ASSERT_EQ(out.state, (uint32)DB_IN_PRODUCTION);
	UT_ASSERT_EQ(out.checkPoint, 0x0000000456780000ULL);
	UT_ASSERT_EQ(out.checkPointCopy.redo, 0x0000000456780000ULL - 8);

	/* corrupt anchor -> refresh is a no-op (creation stays with the
	 * checkpoint hook and the seed path; a later boot fails closed) */
	flip_byte(cluster_recovery_anchor_path(), 24);
	flip_byte(cluster_recovery_anchor_bak_path(), 24);
	UT_ASSERT(!cluster_recovery_anchor_refresh_state(TEST_SYSID, (uint32)DB_SHUTDOWNED));
}

/* ======================================================================
 * U13 -- boot-time adoption statics (load / active / get)
 * ====================================================================== */
UT_TEST(test_load_adoption)
{
	ClusterRecoveryAnchor ra;
	bool used_bak = true;

	wipe_anchor_files();

	/* nothing on disk -> load fails, statics stay inactive */
	UT_ASSERT(!cluster_recovery_anchor_load(TEST_SYSID, &used_bak));
	UT_ASSERT(!cluster_recovery_anchor_active());

	build_anchor(&ra, 0x0000000567890000ULL);
	cluster_recovery_anchor_write(&ra);

	UT_ASSERT(cluster_recovery_anchor_load(TEST_SYSID, &used_bak));
	UT_ASSERT(!used_bak);
	UT_ASSERT(cluster_recovery_anchor_active());
	UT_ASSERT_EQ(cluster_recovery_anchor_get()->checkPoint, 0x0000000567890000ULL);
	UT_ASSERT_EQ(cluster_recovery_anchor_get()->node_id, cluster_node_id);
}

int
main(void)
{
	setup_shared_root();

	UT_PLAN(13);
	UT_RUN(test_layout);
	UT_RUN(test_write_read_roundtrip);
	UT_RUN(test_classify);
	UT_RUN(test_bak_fallback);
	UT_RUN(test_both_bad_failclosed);
	UT_RUN(test_build_from_controlfile);
	UT_RUN(test_state_carrier);
	UT_RUN(test_publish_checkpoint);
	UT_RUN(test_checkpoint_publish_requires_current_owner_before_io);
	UT_RUN(test_checkpoint_publish_native_seed_proof_is_exact);
	UT_RUN(test_checkpoint_publish_phase4_boot_proof_is_exact);
	UT_RUN(test_refresh_state);
	UT_RUN(test_load_adoption);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
