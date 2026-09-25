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

#include "catalog/catversion.h"
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
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/elog.h"

#include "../../backend/cluster/cluster_recovery_anchor_private.h"

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
bool enableFsync = true;
static int test_fsync_calls;
static int test_fsync_fail_at;
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
	test_fsync_calls++;
	if (test_fsync_calls == test_fsync_fail_at) {
		errno = EIO;
		return -1;
	}
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

/* PGRAC: independent LE fixtures for exact root-selected anchor v2. These
 * tests link the production codec/reader; no fixture calls its encoder to
 * construct decoder input. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_put(uint8 *bytes, size_t offset, uint64 value, size_t width)
{
	size_t i;

	for (i = 0; i < width; ++i)
		bytes[offset + i] = (uint8)(value >> (8 * i));
}

static bool
v2_zero(const void *ptr, size_t size)
{
	const uint8 *bytes = ptr;
	size_t i;

	for (i = 0; i < size; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static void
v2_fix_crc_hash(uint8 bytes[512], ClusterRecoveryAnchorRefV2 *ref)
{
	pg_crc32c crc;
	pg_cryptohash_ctx *ctx;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 508);
	FIN_CRC32C(crc);
	v2_put(bytes, 508, crc, 4);
	ctx = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT(ctx != NULL);
	UT_ASSERT_EQ(pg_cryptohash_init(ctx), 0);
	UT_ASSERT_EQ(pg_cryptohash_update(ctx, bytes, 512), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(ctx, ref->anchor_sha256, 32), 0);
	pg_cryptohash_free(ctx);
}

static void
v2_fixture(uint8 bytes[512], ClusterRecoveryAnchorRefV2 *ref)
{
	size_t i;

	memset(bytes, 0, 512);
	memset(ref, 0, sizeof(*ref));
	ref->identity.system_identifier = TEST_SYSID;
	ref->identity.origin_node_id = 3;
	ref->identity.origin_thread_id = 4;
	ref->identity.thread_claim_created_at = 9001;
	ref->identity.thread_claim_crc32c = 0xa1b2c3d4;
	ref->identity.origin_owner_incarnation = 77;
	ref->identity.root_lineage_seq = 13;
	ref->database_incarnation = 19;
	ref->max_config_generation = 8;
	ref->anchor_generation = 23;
	for (i = 0; i < 16; ++i) {
		ref->identity.storage_uuid[i] = i + 1;
		ref->identity.authority_uuid[i] = i + 33;
	}
	for (i = 0; i < 32; ++i)
		ref->claim_sha256[i] = i + 65;
	v2_put(bytes, 0, CLUSTER_RECOVERY_ANCHOR_MAGIC, 4);
	v2_put(bytes, 4, 2, 2);
	v2_put(bytes, 8, 3, 4);
	v2_put(bytes, 12, DB_SHUTDOWNED, 4);
	v2_put(bytes, 16, TEST_SYSID, 8);
	v2_put(bytes, 24, UINT64_C(0x1234567890), 8);
	v2_put(bytes, 32, 1001, 8);
	v2_put(bytes, 40, UINT64_C(0x1234567000), 8);
	v2_put(bytes, 48, 7, 4);
	v2_put(bytes, 52, 6, 4);
	bytes[56] = 1;
	v2_put(bytes, 64, UINT64_C(0x1200000034), 8);
	for (i = 0; i < 7; ++i)
		v2_put(bytes, 72 + 4 * i, 101 + i, 4);
	v2_put(bytes, 104, 1002, 8);
	v2_put(bytes, 112, 201, 4);
	v2_put(bytes, 116, 202, 4);
	v2_put(bytes, 120, 203, 4);
	v2_put(bytes, 128, UINT64_C(0x4500000067), 8);
	v2_put(bytes, 136, 19, 8);
	v2_put(bytes, 144, 77, 8);
	memcpy(bytes + 152, ref->identity.storage_uuid, 16);
	memcpy(bytes + 168, ref->identity.authority_uuid, 16);
	v2_put(bytes, 184, 13, 8);
	v2_put(bytes, 192, 5, 8);
	v2_put(bytes, 200, 23, 8);
	v2_put(bytes, 208, 4, 2);
	v2_put(bytes, 212, UINT64_C(0x01020304), 4);
	memcpy(bytes + 216, ref->claim_sha256, 32);
	v2_put(bytes, 248, 9001, 8);
	v2_put(bytes, 256, UINT64_C(0xa1b2c3d4), 4);
	v2_put(bytes, 260, UINT64_C(0x1234568800), 8);
	v2_put(bytes, 268, 7, 4);
	bytes[289] = 1;
	v2_put(bytes, 292, 2, 4);
	v2_put(bytes, 296, 311, 4);
	v2_put(bytes, 300, 24, 4);
	v2_put(bytes, 304, 12, 4);
	v2_put(bytes, 308, 11, 4);
	v2_put(bytes, 312, 129, 4);
	v2_fix_crc_hash(bytes, ref);
}

static void
v2_common(ControlFileData *cf)
{
	memset(cf, 0, sizeof(*cf));
	cf->system_identifier = TEST_SYSID;
	cf->pg_control_version = PG_CONTROL_VERSION;
	cf->catalog_version_no = CATALOG_VERSION_NO;
	cf->state = DB_IN_PRODUCTION;
	cf->time = 999;
	cf->checkPoint = 888;
	cf->checkPointCopy.redo = 777;
	cf->checkPointCopy.ThisTimeLineID = 22;
	cf->checkPointCopy.PrevTimeLineID = 21;
	cf->checkPointCopy.nextXid = FullTransactionIdFromU64(UINT64_C(0x3400000056));
	cf->checkPointCopy.nextOid = 3001;
	cf->checkPointCopy.nextMulti = 3002;
	cf->checkPointCopy.nextMultiOffset = 3003;
	cf->checkPointCopy.oldestXid = 3004;
	cf->checkPointCopy.oldestXidDB = 3005;
	cf->checkPointCopy.oldestMulti = 3006;
	cf->checkPointCopy.oldestMultiDB = 3007;
	cf->checkPointCopy.oldestActiveXid = 3008;
	cf->MaxConnections = 200;
	cf->wal_level = 1;
	cf->max_worker_processes = 16;
	cf->max_wal_senders = 10;
	cf->max_locks_per_xact = 128;
	cf->track_commit_timestamp = true;
	cf->blcksz = BLCKSZ;
	cf->xlog_blcksz = XLOG_BLCKSZ;
	memset(cf->mock_authentication_nonce, 0x5b, MOCK_AUTH_NONCE_LEN);
	INIT_CRC32C(cf->crc);
	COMP_CRC32C(cf->crc, cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf->crc);
}

UT_TEST(test_v2_exact_fields_and_canonical_roundtrip)
{
	uint8 bytes[512], encoded[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;

	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out), 0);
	UT_ASSERT_EQ(out.identity.system_identifier, TEST_SYSID);
	UT_ASSERT_EQ(out.identity.origin_node_id, 3);
	UT_ASSERT_EQ(out.identity.origin_thread_id, 4);
	UT_ASSERT_EQ(out.database_incarnation, 19);
	UT_ASSERT_EQ(out.config_generation, 5);
	UT_ASSERT_EQ(out.anchor_generation, 23);
	UT_ASSERT_EQ(out.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(out.checkpoint, UINT64_C(0x1234567890));
	UT_ASSERT_EQ(out.write_time, 1001);
	UT_ASSERT_EQ(out.checkpoint_copy.redo, UINT64_C(0x1234567000));
	UT_ASSERT_EQ(out.checkpoint_copy.ThisTimeLineID, 7);
	UT_ASSERT_EQ(out.checkpoint_copy.PrevTimeLineID, 6);
	UT_ASSERT(out.checkpoint_copy.fullPageWrites);
	UT_ASSERT_EQ(U64FromFullTransactionId(out.checkpoint_copy.nextXid), UINT64_C(0x1200000034));
	UT_ASSERT_EQ(out.checkpoint_copy.nextOid, 101);
	UT_ASSERT_EQ(out.checkpoint_copy.nextMulti, 102);
	UT_ASSERT_EQ(out.checkpoint_copy.nextMultiOffset, 103);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestXid, 104);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestXidDB, 105);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestMulti, 106);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestMultiDB, 107);
	UT_ASSERT_EQ(out.checkpoint_copy.time, 1002);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestCommitTsXid, 201);
	UT_ASSERT_EQ(out.checkpoint_copy.newestCommitTsXid, 202);
	UT_ASSERT_EQ(out.checkpoint_copy.oldestActiveXid, 203);
	UT_ASSERT_EQ(out.unlogged_lsn, UINT64_C(0x4500000067));
	UT_ASSERT_EQ(out.min_recovery_point, UINT64_C(0x1234568800));
	UT_ASSERT_EQ(out.min_recovery_tli, 7);
	UT_ASSERT_EQ(out.backup_start, 0);
	UT_ASSERT_EQ(out.backup_end, 0);
	UT_ASSERT(!out.backup_end_required);
	UT_ASSERT(out.wal_log_hints);
	UT_ASSERT(!out.track_commit_timestamp);
	UT_ASSERT_EQ(out.wal_level, 2);
	UT_ASSERT_EQ(out.max_connections, 311);
	UT_ASSERT_EQ(out.max_worker_processes, 24);
	UT_ASSERT_EQ(out.max_wal_senders, 12);
	UT_ASSERT_EQ(out.max_prepared_xacts, 11);
	UT_ASSERT_EQ(out.max_locks_per_xact, 129);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&out, encoded), 0);
	UT_ASSERT(memcmp(bytes, encoded, 512) == 0);
	/* Logical struct padding must not become part of the persistent image. */
	memset((uint8 *)&out.checkpoint_copy + 17, 0xfe, 7);
	memset((uint8 *)&out.checkpoint_copy + 60, 0xfe, 4);
	memset((uint8 *)&out.checkpoint_copy + 84, 0xfe, 4);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&out, encoded), 0);
	UT_ASSERT(memcmp(bytes, encoded, 512) == 0);
}

UT_TEST(test_v2_reserved_and_booleans)
{
	const size_t offsets[] = { 6, 7, 57, 63, 100, 103, 124, 127, 210, 211, 291, 316, 507 };
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes, &ref);
		bytes[offsets[i]] = 1;
		v2_fix_crc_hash(bytes, &ref);
		memset(&out, 0xfe, sizeof(out));
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
					 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	for (i = 0; i < 2; ++i) {
		v2_fixture(bytes, &ref);
		bytes[i == 0 ? 56 : 288] = 2;
		v2_fix_crc_hash(bytes, &ref);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
					 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	}
}

UT_TEST(test_v2_identity_and_hash_refuse)
{
	const size_t offsets[] = { 8, 16, 136, 144, 152, 168, 184, 200, 208, 216, 248, 256 };
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes, &ref);
		bytes[offsets[i]] ^= 1;
		v2_fix_crc_hash(bytes, &ref);
		memset(&out, 0xfe, sizeof(out));
		UT_ASSERT(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	v2_fixture(bytes, &ref);
	ref.anchor_sha256[0] ^= 1;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v2_size_crc_version_endian)
{
	uint8 bytes[513];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;

	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 511, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 513, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_SIZE);
	bytes[40] ^= 1;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	v2_fixture(bytes, &ref);
	bytes[4] = 1;
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	v2_fixture(bytes, &ref);
	bytes[212] ^= 1;
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_ENDIAN);
	v2_fixture(bytes, &ref);
	bytes[0] ^= 1;
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_BAD_MAGIC);
}

UT_TEST(test_v2_config_history_and_invalid_inputs)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;

	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out), 0);
	ref.max_config_generation = 5;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out), 0);
	ref.max_config_generation = 4;
	UT_ASSERT(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out) != 0);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	v2_fixture(bytes, &ref);
	v2_put(bytes, 192, 0, 8);
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out) != 0);
	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out), 0);
	out.identity.reserved42 = 1;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&out, bytes), CLUSTER_CONTROL_ROOT_BAD_RESERVED);
	UT_ASSERT(v2_zero(bytes, 512));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(NULL, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(NULL, bytes),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(bytes, 512));
}

UT_TEST(test_v2_projection_field_ownership)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ControlFileData common, projected, expected;

	v2_fixture(bytes, &ref);
	v2_common(&common);
	expected = common;
	expected.state = DB_SHUTDOWNED;
	expected.time = 1001;
	expected.checkPoint = UINT64_C(0x1234567890);
	expected.checkPointCopy.redo = UINT64_C(0x1234567000);
	expected.checkPointCopy.ThisTimeLineID = 7;
	expected.checkPointCopy.PrevTimeLineID = 6;
	expected.checkPointCopy.fullPageWrites = true;
	expected.checkPointCopy.time = 1002;
	expected.checkPointCopy.oldestCommitTsXid = 201;
	expected.checkPointCopy.newestCommitTsXid = 202;
	expected.unloggedLSN = UINT64_C(0x4500000067);
	expected.minRecoveryPoint = UINT64_C(0x1234568800);
	expected.minRecoveryPointTLI = 7;
	expected.wal_log_hints = true;
	expected.track_commit_timestamp = false;
	expected.wal_level = 2;
	expected.MaxConnections = 311;
	expected.max_worker_processes = 24;
	expected.max_wal_senders = 12;
	expected.max_prepared_xacts = 11;
	expected.max_locks_per_xact = 129;
	INIT_CRC32C(expected.crc);
	COMP_CRC32C(expected.crc, &expected, offsetof(ControlFileData, crc));
	FIN_CRC32C(expected.crc);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &projected), 0);
	UT_ASSERT(memcmp(&projected, &expected, sizeof(expected)) == 0);
	common.system_identifier++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &projected),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_zero(&projected, sizeof(projected)));
	v2_common(&common);
	common.MaxConnections++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &projected),
				 CLUSTER_CONTROL_ROOT_BAD_BODY_CRC);
	UT_ASSERT(v2_zero(&projected, sizeof(projected)));
}

UT_TEST(test_v2_current_thread_state_matrix_preserves_other_fields)
{
	/* Literal expected PG16 DBState, columns DB_STARTUP..DB_IN_PRODUCTION;
	 * -1 means no supported current-thread projection. */
	static const int expected[5][7] = { { -1, 6, -1, 6, -1, -1, 6 },
										{ -1, 4, -1, 4, 4, -1, 4 },
										{ -1, 4, -1, 4, 4, -1, 4 },
										{ -1, 1, -1, -1, -1, -1, -1 },
										{ -1, -1, -1, -1, -1, -1, -1 } };
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterControlRootSnapshot record;
	ControlFileData common, input, view, want;
	v2_fixture(bytes, &ref);
	v2_common(&common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &input), 0);
	memset(&record, 0, sizeof(record));
	record.identity = ref.identity;
	record.checkpoint_lower_lsn = input.checkPointCopy.redo;
	record.checkpoint_tli = input.checkPointCopy.ThisTimeLineID;
	input.minRecoveryPoint = 0;
	input.minRecoveryPointTLI = 0;
	for (int life = 1; life <= 5; ++life) {
		for (int state = 0; state <= 6; ++state) {
			record.lifecycle = life;
			input.state = (DBState)state;
			INIT_CRC32C(input.crc);
			COMP_CRC32C(input.crc, &input, offsetof(ControlFileData, crc));
			FIN_CRC32C(input.crc);
			view = input;
			if (expected[life - 1][state] < 0) {
				UT_ASSERT_EQ(cluster_recovery_anchor_v2_thread_state(&ref, &record, &view),
							 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
				UT_ASSERT(v2_zero(&view, sizeof(view)));
			} else {
				UT_ASSERT_EQ(cluster_recovery_anchor_v2_thread_state(&ref, &record, &view), 0);
				want = input;
				want.state = (DBState)expected[life - 1][state];
				INIT_CRC32C(want.crc);
				COMP_CRC32C(want.crc, &want, offsetof(ControlFileData, crc));
				FIN_CRC32C(want.crc);
				UT_ASSERT(memcmp(&want, &view, sizeof(view)) == 0);
			}
		}
	}
}

UT_TEST(test_v2_current_thread_state_rejects_wrong_identity_or_crc)
{
	for (int fault = 0; fault < 8; ++fault) {
		uint8 bytes[512];
		ClusterRecoveryAnchorRefV2 ref;
		ClusterControlRootSnapshot record;
		ControlFileData common, view;
		v2_fixture(bytes, &ref);
		v2_common(&common);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &view), 0);
		memset(&record, 0, sizeof(record));
		record.identity = ref.identity;
		record.checkpoint_lower_lsn = view.checkPointCopy.redo;
		record.checkpoint_tli = view.checkPointCopy.ThisTimeLineID;
		record.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		if (fault == 0)
			record.identity.origin_owner_incarnation++;
		if (fault == 1)
			record.checkpoint_tli++;
		if (fault == 2)
			record.checkpoint_lower_lsn++;
		if (fault == 3)
			view.system_identifier++;
		if (fault == 4)
			view.crc ^= 1;
		if (fault == 5)
			record.lifecycle = 0;
		UT_ASSERT(cluster_recovery_anchor_v2_thread_state(fault == 6 ? NULL : &ref,
														  fault == 7 ? NULL : &record, &view)
				  != 0);
		UT_ASSERT(v2_zero(&view, sizeof(view)));
	}
}

UT_TEST(test_v2_projection_backup_and_state_refuse)
{
	const size_t offsets[] = { 272, 280, 288 };
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ControlFileData common, projected;
	size_t i;

	v2_common(&common);
	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes, &ref);
		bytes[offsets[i]] = 1;
		v2_fix_crc_hash(bytes, &ref);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &projected),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&projected, sizeof(projected)));
	}
	v2_fixture(bytes, &ref);
	v2_put(bytes, 12, DB_IN_PRODUCTION + 1, 4);
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &projected),
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	UT_ASSERT(v2_zero(&projected, sizeof(projected)));
}

static void
v2_selected_file(const uint8 bytes[512], const ClusterRecoveryAnchorRefV2 *ref,
				 char path[MAXPGPATH], char directory[MAXPGPATH])
{
	char hash[65];
	size_t i;
	int fd;

	for (i = 0; i < 32; ++i)
		snprintf(hash + 2 * i, 3, "%02x", ref->anchor_sha256[i]);
	snprintf(directory, MAXPGPATH, "%s/global/anchor_images", cluster_shared_data_dir);
	UT_ASSERT(mkdir(directory, 0700) == 0 || errno == EEXIST);
	snprintf(directory, MAXPGPATH, "%s/global/anchor_images/thread_4", cluster_shared_data_dir);
	UT_ASSERT(mkdir(directory, 0700) == 0 || errno == EEXIST);
	snprintf(directory, MAXPGPATH, "%s/global/anchor_images/thread_4/generation_77",
			 cluster_shared_data_dir);
	UT_ASSERT(mkdir(directory, 0700) == 0 || errno == EEXIST);
	snprintf(path, MAXPGPATH, "%s/anchor_23-%s.bin", directory, hash);
	fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 512), 512);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_v2_read_selected_object_only)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ControlFileData common, out;
	char path[MAXPGPATH], directory[MAXPGPATH];
	ClusterRecoveryAnchor legacy;

	v2_fixture(bytes, &ref);
	v2_common(&common);
	test_cf_x_held = true;
	v2_selected_file(bytes, &ref, path, directory);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out), 0);
	UT_ASSERT_EQ(out.checkPoint, UINT64_C(0x1234567890));
	test_cf_x_held = false;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	test_cf_x_held = true;
	UT_ASSERT_EQ(unlink(path), 0);
	build_anchor(&legacy, UINT64_C(0x1234567890));
	cluster_recovery_anchor_write(&legacy);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	/* Same generation, different hash is not a selected object. */
	bytes[32]++;
	v2_fix_crc_hash(bytes, &ref);
	v2_selected_file(bytes, &ref, path, directory);
	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(unlink(path), 0);
}

UT_TEST(test_v2_object_type_size_and_permissions)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ControlFileData common, out;
	char path[MAXPGPATH], directory[MAXPGPATH];
	int fd;

	v2_fixture(bytes, &ref);
	v2_common(&common);
	v2_selected_file(bytes, &ref, path, directory);
	UT_ASSERT_EQ(chmod(path, 0660), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(path, 0600), 0);
	UT_ASSERT_EQ(chmod(directory, 0770), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(directory, 0700), 0);
	fd = open(path, O_WRONLY | O_APPEND);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 1), 1);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(symlink(cluster_recovery_anchor_path(), path), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(mkfifo(path, 0600), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(unlink(path), 0);
}

/* PGRAC: actual immutable-anchor lifecycle including durability cuts.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_stage_fixture(ClusterRecoveryAnchorV2 *anchor, uint8 uuid[16], ControlFileData *common)
{
	static uint64 ordinal = 100;
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	char path[MAXPGPATH];
	const char *dirs[] = { "global/anchor_images", "global/anchor_images/thread_4",
						   "global/anchor_images/thread_4/generation_77",
						   "global/anchor_images/thread_4/generation_77/.staging" };
	size_t i;

	v2_fixture(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, anchor), 0);
	anchor->anchor_generation = ++ordinal;
	anchor->write_time += ordinal;
	memset(uuid, 0, 16);
	v2_put(uuid, 0, ordinal, 4);
	uuid[6] = 0x40;
	uuid[8] = 0x80;
	v2_common(common);
	for (i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir, dirs[i]);
		UT_ASSERT(mkdir(path, 0700) == 0 || errno == EEXIST);
	}
	test_fsync_calls = test_fsync_fail_at = 0;
	enableFsync = true;
	test_cf_x_held = false;
}

static void
v2_stage_paths(const ClusterRecoveryAnchorStageV2 *stage, char final[MAXPGPATH],
			   char temp[MAXPGPATH])
{
	char hash[65], uuid[33];
	size_t i;

	for (i = 0; i < 32; ++i)
		snprintf(hash + 2 * i, 3, "%02x", stage->ref.anchor_sha256[i]);
	for (i = 0; i < 16; ++i)
		snprintf(uuid + 2 * i, 3, "%02x", stage->operation_uuid[i]);
	snprintf(final, MAXPGPATH,
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 cluster_shared_data_dir, stage->ref.identity.origin_thread_id,
			 stage->ref.identity.origin_owner_incarnation, stage->ref.anchor_generation, hash);
	snprintf(temp, MAXPGPATH,
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/.staging/%s.tmp",
			 cluster_shared_data_dir, stage->ref.identity.origin_thread_id,
			 stage->ref.identity.origin_owner_incarnation, uuid);
}

UT_TEST(test_v2_stage_install_read_and_discard)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ClusterRecoveryAnchorRefV2 ref;
	ControlFileData common, out;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	UT_ASSERT_EQ(stage.owner_pid, getpid());
	UT_ASSERT_EQ(test_fsync_calls, 2);
	v2_stage_paths(&stage, final, temp);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT(access(final, F_OK) != 0);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), 0);
	UT_ASSERT_EQ(access(final, F_OK), 0);
	UT_ASSERT(access(temp, F_OK) != 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&stage.ref, &common, &out), 0);
	UT_ASSERT_EQ(out.checkPoint, anchor.checkpoint);
	ref = stage.ref;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&ref, &common, &out), 0);
}

UT_TEST(test_v2_same_generation_no_overwrite_and_exact_reuse)
{
	ClusterRecoveryAnchorV2 first, second;
	ClusterRecoveryAnchorStageV2 a, b, reuse;
	ControlFileData common, out;
	uint8 uuid[16];

	v2_stage_fixture(&first, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&first, uuid, &a), 0);
	second = first;
	second.write_time++;
	uuid[0]++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&second, uuid, &b), 0);
	UT_ASSERT(memcmp(a.ref.anchor_sha256, b.ref.anchor_sha256, 32) != 0);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&a), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&b), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&a.ref, &common, &out), 0);
	UT_ASSERT_EQ(out.time, first.write_time);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&b.ref, &common, &out), 0);
	UT_ASSERT_EQ(out.time, second.write_time);
	test_cf_x_held = false;
	uuid[0]++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&first, uuid, &reuse), 0);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&reuse), 0);
	UT_ASSERT(memcmp(a.ref.anchor_sha256, reuse.ref.anchor_sha256, 32) == 0);
}

UT_TEST(test_v2_install_refuses_corrupt_existing_destination)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ControlFileData common;
	uint8 uuid[16], bytes[512];
	char final[MAXPGPATH], temp[MAXPGPATH];
	int fd;

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	v2_stage_paths(&stage, final, temp);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&anchor, bytes), 0);
	bytes[40] ^= 1;
	fd = open(final, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 512), 512);
	UT_ASSERT_EQ(close(fd), 0);
	test_cf_x_held = true;
	UT_ASSERT(cluster_recovery_anchor_v2_install(&stage) != 0);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
	UT_ASSERT_EQ(access(final, F_OK), 0);
	UT_ASSERT_EQ(unlink(final), 0);
}

UT_TEST(test_v2_prepare_and_install_fsync_cuts)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ControlFileData common, out;
	uint8 uuid[16];
	int cut;

	for (cut = 1; cut <= 2; ++cut) {
		v2_stage_fixture(&anchor, uuid, &common);
		test_fsync_fail_at = cut;
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	}
	for (cut = 1; cut <= 2; ++cut) {
		v2_stage_fixture(&anchor, uuid, &common);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
		test_cf_x_held = true;
		test_fsync_calls = 0;
		test_fsync_fail_at = cut;
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
		test_fsync_fail_at = 0;
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), 0);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_read_locked(&stage.ref, &common, &out), 0);
	}
}

UT_TEST(test_v2_discard_sync_retry_cannot_resurrect)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ControlFileData common;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	v2_stage_paths(&stage, final, temp);
	test_fsync_calls = 0;
	test_fsync_fail_at = 1;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(access(final, F_OK) != 0);
	UT_ASSERT(access(temp, F_OK) != 0);
	test_fsync_fail_at = 0;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));

	/* Cancel an install that linked its object but failed the last sync. */
	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	v2_stage_paths(&stage, final, temp);
	test_cf_x_held = true;
	test_fsync_calls = 0;
	test_fsync_fail_at = 2;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	test_fsync_calls = 0;
	test_fsync_fail_at = 1;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	test_fsync_fail_at = 0;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(access(final, F_OK), 0);
	UT_ASSERT(access(temp, F_OK) != 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
	UT_ASSERT(v2_zero(&stage, sizeof(stage)));
	UT_ASSERT_EQ(access(final, F_OK), 0);
}

UT_TEST(test_v2_stage_owner_and_inode_are_exact)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage;
	ControlFileData common;
	uint8 uuid[16], bytes[512];
	char final[MAXPGPATH], temp[MAXPGPATH], saved[MAXPGPATH];
	int fd;

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	test_cf_x_held = true;
	stage.owner_pid++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	stage.owner_pid--;
	v2_stage_paths(&stage, final, temp);
	snprintf(saved, sizeof(saved), "%s.saved", temp);
	UT_ASSERT_EQ(rename(temp, saved), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&anchor, bytes), 0);
	fd = open(temp, O_CREAT | O_WRONLY | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, bytes, 512), 512);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(access(temp, F_OK), 0);
	UT_ASSERT_EQ(unlink(temp), 0);
	UT_ASSERT_EQ(rename(saved, temp), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_discard(&stage), 0);
}

UT_TEST(test_v2_stage_directory_replaced_or_symlinked)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage, refused;
	ControlFileData common;
	uint8 uuid[16];
	char dir[MAXPGPATH], saved[MAXPGPATH];

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_4/generation_77/.staging",
			 cluster_shared_data_dir);
	snprintf(saved, sizeof(saved), "%s.saved", dir);
	UT_ASSERT_EQ(rename(dir, saved), 0);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
	test_cf_x_held = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(rmdir(dir), 0);
	UT_ASSERT_EQ(symlink(".staging.saved", dir), 0);
	uuid[0]++;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &refused),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&refused, sizeof(refused)));
	UT_ASSERT_EQ(unlink(dir), 0);
	UT_ASSERT_EQ(rename(saved, dir), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), 0);
}

UT_TEST(test_v2_stage_requires_fsync_and_clusterwide_x)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterRecoveryAnchorStageV2 stage, refused;
	ControlFileData common;
	uint8 uuid[16], zero[16] = { 0 };

	v2_stage_fixture(&anchor, uuid, &common);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, zero, &refused),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = false;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &refused),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&refused, sizeof(refused)));
	enableFsync = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_prepare(&anchor, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	test_cf_x_held = true;
	enableFsync = false;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	enableFsync = true;
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_install(&stage), 0);
}

/* PGRAC: historical requirements belong to the selected WAL thread, not the
 * current common configuration. Literal offsets are independent of the codec.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_parameter_bytes(uint8 bytes[512], ClusterRecoveryAnchorRefV2 *ref)
{
	v2_fixture(bytes, ref);
	bytes[289] = 1;
	bytes[290] = 0;
	v2_put(bytes, 292, 2, 4);
	v2_put(bytes, 296, 811, 4);
	v2_put(bytes, 300, 28, 4);
	v2_put(bytes, 304, 19, 4);
	v2_put(bytes, 308, 13, 4);
	v2_put(bytes, 312, 259, 4);
	v2_fix_crc_hash(bytes, ref);
}

UT_TEST(test_v2_historical_parameters_not_current_common)
{
	uint8 bytes[512], encoded[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 anchor;
	ControlFileData common, out;
	ClusterControlRootResult result;

	v2_parameter_bytes(bytes, &ref);
	v2_common(&common);
	result = cluster_recovery_anchor_v2_project(bytes, 512, &ref, &common, &out);
	UT_ASSERT_EQ(result, 0);
	if (result != 0)
		return;
	UT_ASSERT_EQ(out.wal_level, 2);
	UT_ASSERT(out.wal_log_hints);
	UT_ASSERT(!out.track_commit_timestamp);
	UT_ASSERT_EQ(out.MaxConnections, 811);
	UT_ASSERT_EQ(out.max_worker_processes, 28);
	UT_ASSERT_EQ(out.max_wal_senders, 19);
	UT_ASSERT_EQ(out.max_prepared_xacts, 13);
	UT_ASSERT_EQ(out.max_locks_per_xact, 259);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, common.checkPointCopy.nextOid);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &anchor), 0);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&anchor, encoded), 0);
	UT_ASSERT(memcmp(bytes, encoded, 512) == 0);
}

UT_TEST(test_v2_parameter_encoding_cannot_be_absent_or_overflow)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;
	const size_t capacities[] = { 296, 300, 304, 308, 312 };

	v2_fixture(bytes, &ref);
	memset(bytes + 289, 0, 27);
	v2_fix_crc_hash(bytes, &ref);
	UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	for (size_t i = 0; i < lengthof(capacities); ++i) {
		v2_parameter_bytes(bytes, &ref);
		v2_put(bytes, capacities[i], UINT32_C(0x80000000), 4);
		v2_fix_crc_hash(bytes, &ref);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	for (int fault = 0; fault < 3; ++fault) {
		v2_parameter_bytes(bytes, &ref);
		v2_put(bytes, fault == 0 ? 292 : fault == 1 ? 296 : 312, fault == 0 ? 3 : 0, 4);
		v2_fix_crc_hash(bytes, &ref);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_parameter_boolean_and_reserved_bytes)
{
	uint8 bytes[512];
	ClusterRecoveryAnchorRefV2 ref;
	ClusterRecoveryAnchorV2 out;
	const size_t offsets[] = { 289, 290, 291, 316, 507 };

	for (size_t i = 0; i < lengthof(offsets); ++i) {
		v2_parameter_bytes(bytes, &ref);
		bytes[offsets[i]] = 2;
		v2_fix_crc_hash(bytes, &ref);
		UT_ASSERT_EQ(cluster_recovery_anchor_v2_decode(bytes, 512, &ref, &out),
					 CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

int
main(void)
{
	setup_shared_root();

	UT_PLAN(35);
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
	UT_RUN(test_v2_exact_fields_and_canonical_roundtrip);
	UT_RUN(test_v2_reserved_and_booleans);
	UT_RUN(test_v2_identity_and_hash_refuse);
	UT_RUN(test_v2_size_crc_version_endian);
	UT_RUN(test_v2_config_history_and_invalid_inputs);
	UT_RUN(test_v2_projection_field_ownership);
	UT_RUN(test_v2_current_thread_state_matrix_preserves_other_fields);
	UT_RUN(test_v2_current_thread_state_rejects_wrong_identity_or_crc);
	UT_RUN(test_v2_projection_backup_and_state_refuse);
	UT_RUN(test_v2_read_selected_object_only);
	UT_RUN(test_v2_object_type_size_and_permissions);
	UT_RUN(test_v2_stage_install_read_and_discard);
	UT_RUN(test_v2_same_generation_no_overwrite_and_exact_reuse);
	UT_RUN(test_v2_install_refuses_corrupt_existing_destination);
	UT_RUN(test_v2_prepare_and_install_fsync_cuts);
	UT_RUN(test_v2_discard_sync_retry_cannot_resurrect);
	UT_RUN(test_v2_stage_owner_and_inode_are_exact);
	UT_RUN(test_v2_stage_directory_replaced_or_symlinked);
	UT_RUN(test_v2_stage_requires_fsync_and_clusterwide_x);
	UT_RUN(test_v2_historical_parameters_not_current_common);
	UT_RUN(test_v2_parameter_encoding_cannot_be_absent_or_overflow);
	UT_RUN(test_v2_parameter_boolean_and_reserved_bytes);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
