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
#include <sys/wait.h>
#include <unistd.h>
#include <dirent.h>

#include "catalog/pg_control.h"
#include "catalog/catversion.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "../../backend/cluster/cluster_control_root_private.h"

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
char *cluster_shared_data_dir = NULL;
bool enableFsync = true;
bool cluster_shared_config = false;
static bool runtime_guard_error_expected;
static unsigned runtime_read_calls;

/* The root test links the actual adapter; this leaf test only checks routing. */
ClusterControlRootResult
cluster_control_root_v2_read_runtime_local_locked(ControlFileData *out)
{
	++runtime_read_calls;
	memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
}

/* PGRAC: only lock facts and fsync failures are controlled; file operations
 * exercise the real immutable image implementation. Author: SqlRush <sqlrush@gmail.com>
 */
static bool image_cf_x;
static bool image_cf_s;
static bool image_cf_global;
static unsigned image_fsync_calls;
static unsigned image_fsync_fail_at;
static void (*image_fsync_hook)(int fd);
static unsigned image_fsync_hook_at;
static bool projection_random_collision;
static char projection_random_name[64];

bool
pg_strong_random(void *bytes, size_t len)
{
	static uint8 next = 1;
	UT_ASSERT_EQ(len, 16);
	memset(bytes, next++, len);
	strlcpy(projection_random_name, ".pg_control-", sizeof(projection_random_name));
	for (size_t i = 0; i < len; i++)
		snprintf(projection_random_name + 12 + 2 * i, 3, "%02x", ((uint8 *)bytes)[i]);
	strlcat(projection_random_name, ".tmp", sizeof(projection_random_name));
	if (projection_random_collision) {
		char path[MAXPGPATH];
		int fd;
		snprintf(path, sizeof(path), "%s/global/%s", cluster_shared_data_dir,
				 projection_random_name);
		fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			UT_ASSERT_EQ(write(fd, "other", 5), 5);
			UT_ASSERT_EQ(close(fd), 0);
		}
	}
	return true;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return image_cf_global && (mode == ExclusiveLock ? image_cf_x : image_cf_s);
}

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
	if (elevel >= ERROR) {
		if (runtime_guard_error_expected)
			_exit(90);
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
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
pg_fsync(int fd)
{
	++image_fsync_calls;
	if (image_fsync_hook != NULL && image_fsync_calls == image_fsync_hook_at) {
		void (*hook)(int) = image_fsync_hook;
		image_fsync_hook = NULL;
		hook(fd);
	}
	if (image_fsync_calls == image_fsync_fail_at) {
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
	char tmpl[MAXPGPATH];
	char globaldir[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_cf_authority_XXXXXX", sizeof(tmpl));
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
	snprintf(globaldir, sizeof(globaldir), "%s/global/control_images", shared_root);
	if (mkdir(globaldir, 0700) != 0)
		abort();
	snprintf(globaldir, sizeof(globaldir), "%s/global/control_images/.staging", shared_root);
	if (mkdir(globaldir, 0700) != 0)
		abort();
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
	COMP_CRC32C(cf->crc, (char *)cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf->crc);
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
 * U2 -- path accessors
 * ====================================================================== */
UT_TEST(test_paths)
{
	char expect_primary[MAXPGPATH];
	char expect_bak[MAXPGPATH];

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
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf), 0), CLUSTER_CF_VALID);
	/* good image, matching identity */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf), 0xABCDEF0123456789ULL),
				 CLUSTER_CF_VALID);
	/* good CRC but foreign identity */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf), 0x1111111111111111ULL),
				 CLUSTER_CF_INVALID_IDENTITY);
	/* short buffer */
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf) - 1, 0),
				 CLUSTER_CF_INVALID_SHORT);

	/* torn CRC */
	((char *)&cf)[4] ^= 0xFF;
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf), 0), CLUSTER_CF_INVALID_CRC);

	/* foreign byte order: version a nonzero multiple of 65536 */
	build_cf(&cf, 1);
	cf.pg_control_version = 65536;
	finalize_crc(&cf);
	UT_ASSERT_EQ(cluster_cf_classify_buffer((char *)&cf, sizeof(cf), 0),
				 CLUSTER_CF_INVALID_BYTE_ORDER);
}

/* ======================================================================
 * U3/U9 (pure) -- decide_source
 * ====================================================================== */
UT_TEST(test_decide_source)
{
	/* primary valid always wins */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_VALID, CLUSTER_CF_INVALID_CRC, false),
				 CLUSTER_CF_SOURCE_PRIMARY);
	/* primary bad, bak valid AND strict-ok -> use bak */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC, CLUSTER_CF_VALID, true),
				 CLUSTER_CF_SOURCE_BAK);
	/* primary bad, bak CRC-valid but strict NOT ok -> fail closed */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC, CLUSTER_CF_VALID, false),
				 CLUSTER_CF_SOURCE_FAILCLOSED);
	/* primary bad, bak also bad -> fail closed */
	UT_ASSERT_EQ(cluster_cf_decide_source(CLUSTER_CF_INVALID_CRC, CLUSTER_CF_INVALID_CRC, true),
				 CLUSTER_CF_SOURCE_FAILCLOSED);
	/* identity mismatch on primary is never silently trusted */
	UT_ASSERT_EQ(
		cluster_cf_decide_source(CLUSTER_CF_INVALID_IDENTITY, CLUSTER_CF_INVALID_IDENTITY, true),
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
	cluster_cf_authority_write(&v2); /* rolls v1 into .bak */

	/* corrupt the primary; the .bak (v1) is CRC-valid but its checkpoint is
	 * declared unreachable -> the read must fail-closed, not return v1. */
	flip_byte(cluster_cf_shared_path(), 8);
	stub_recoverable = false;

	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(!cluster_cf_authority_read(&out));

	stub_recoverable = true; /* restore for any later test */
}

/* Independent named-field mask.  An omitted production copy loses a marked
 * byte; copying the entire input struct leaks one of the unmarked poison bytes.
 */
static void
native_field_mask(uint8 mask[sizeof(ControlFileData)])
{
	memset(mask, 0, sizeof(ControlFileData));
#define MARK_CF(f) memset(mask + offsetof(ControlFileData, f), 1, sizeof(((ControlFileData *)0)->f))
#define MARK_CP(f) MARK_CF(checkPointCopy.f)
	MARK_CF(system_identifier);
	MARK_CF(pg_control_version);
	MARK_CF(catalog_version_no);
	MARK_CF(state);
	MARK_CF(time);
	MARK_CF(checkPoint);
	MARK_CP(redo);
	MARK_CP(ThisTimeLineID);
	MARK_CP(PrevTimeLineID);
	MARK_CP(fullPageWrites);
	MARK_CP(nextXid);
	MARK_CP(nextOid);
	MARK_CP(nextMulti);
	MARK_CP(nextMultiOffset);
	MARK_CP(oldestXid);
	MARK_CP(oldestXidDB);
	MARK_CP(oldestMulti);
	MARK_CP(oldestMultiDB);
	MARK_CP(time);
	MARK_CP(oldestCommitTsXid);
	MARK_CP(newestCommitTsXid);
	MARK_CP(oldestActiveXid);
	MARK_CF(unloggedLSN);
	MARK_CF(minRecoveryPoint);
	MARK_CF(minRecoveryPointTLI);
	MARK_CF(backupStartPoint);
	MARK_CF(backupEndPoint);
	MARK_CF(backupEndRequired);
	MARK_CF(wal_level);
	MARK_CF(wal_log_hints);
	MARK_CF(MaxConnections);
	MARK_CF(max_worker_processes);
	MARK_CF(max_wal_senders);
	MARK_CF(max_prepared_xacts);
	MARK_CF(max_locks_per_xact);
	MARK_CF(track_commit_timestamp);
	MARK_CF(maxAlign);
	MARK_CF(floatFormat);
	MARK_CF(blcksz);
	MARK_CF(relseg_size);
	MARK_CF(xlog_blcksz);
	MARK_CF(xlog_seg_size);
	MARK_CF(nameDataLen);
	MARK_CF(indexMaxKeys);
	MARK_CF(toast_max_chunk_size);
	MARK_CF(loblksize);
	MARK_CF(float8ByVal);
	MARK_CF(data_checksum_version);
	MARK_CF(mock_authentication_nonce);
#undef MARK_CP
#undef MARK_CF
}

static void
image_input(ControlFileData *cf)
{
	static unsigned identity = 100;

	memset(cf, 0xa5, sizeof(*cf));
	cf->system_identifier = UINT64_C(0x0102030405060708);
	cf->pg_control_version = PG_CONTROL_VERSION;
	cf->catalog_version_no = CATALOG_VERSION_NO;
	cf->state = DB_IN_PRODUCTION;
	cf->time = ++identity;
	cf->checkPointCopy.fullPageWrites = true;
	cf->backupEndRequired = true;
	cf->wal_log_hints = true;
	cf->track_commit_timestamp = true;
	cf->float8ByVal = true;
	cf->floatFormat = FLOATFORMAT_VALUE;
	image_cf_global = true;
	image_cf_x = true;
	image_cf_s = false;
	image_fsync_calls = 0;
	image_fsync_fail_at = 0;
}

static void
image_uuid(uint8 uuid[16])
{
	static unsigned next = 1;

	memset(uuid, 0x22, 16);
	uuid[0] = next++;
	uuid[6] = 0x42;
	uuid[8] = 0x82;
}

static void
image_paths(const ClusterCfImageStage *stage, char final[MAXPGPATH], char temp[MAXPGPATH])
{
	char hash[65];
	char uuid[33];
	int i;

	for (i = 0; i < 32; ++i)
		snprintf(hash + i * 2, 3, "%02x", stage->image_sha256[i]);
	for (i = 0; i < 16; ++i)
		snprintf(uuid + i * 2, 3, "%02x", stage->operation_uuid[i]);
	snprintf(final, MAXPGPATH, "%s/global/control_images/%llu-%s.bin", shared_root,
			 (unsigned long long)stage->generation, hash);
	snprintf(temp, MAXPGPATH, "%s/global/control_images/.staging/%s.tmp", shared_root, uuid);
}

static bool
image_zero(const void *ptr, size_t len)
{
	const uint8 *p = ptr;
	size_t i;

	for (i = 0; i < len; ++i)
		if (p[i])
			return false;
	return true;
}

#define REQUIRE_IMAGE_OK(call)                                                                     \
	do {                                                                                           \
		ClusterControlRootResult r_ = (call);                                                      \
		UT_ASSERT_EQ(r_, CLUSTER_CONTROL_ROOT_OK_PRIMARY);                                         \
		if (r_ != CLUSTER_CONTROL_ROOT_OK_PRIMARY)                                                 \
			return;                                                                                \
	} while (0)

UT_TEST(test_immutable_native_named_fields_and_padding)
{
	ControlFileData in;
	ControlFileData other;
	uint8 mask[sizeof(in)];
	uint8 first[PG_CONTROL_FILE_SIZE];
	uint8 second[PG_CONTROL_FILE_SIZE];
	size_t i;
	pg_crc32c crc;

	image_input(&in);
	native_field_mask(mask);
	memset(&other, 0x3c, sizeof(other));
	for (i = 0; i < sizeof(in); ++i)
		if (mask[i])
			((uint8 *)&other)[i] = ((uint8 *)&in)[i];
	REQUIRE_IMAGE_OK(cluster_cf_control_image_encode(&in, first));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_encode(&other, second));
	UT_ASSERT(memcmp(first, second, sizeof(first)) == 0);
	for (i = 0; i < offsetof(ControlFileData, crc); ++i)
		UT_ASSERT_EQ(first[i], mask[i] ? ((uint8 *)&in)[i] : 0);
	UT_ASSERT(image_zero(first + offsetof(ControlFileData, crc) + sizeof(pg_crc32c),
						 sizeof(first) - offsetof(ControlFileData, crc) - sizeof(pg_crc32c)));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, first, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	UT_ASSERT(memcmp(first + offsetof(ControlFileData, crc), &crc, sizeof(crc)) == 0);
}

UT_TEST(test_immutable_prepare_install_read_and_discard)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 17, uuid, &stage));
	image_paths(&stage, final, temp);
	UT_ASSERT(lstat(temp, &st) == 0 && S_ISREG(st.st_mode));
	UT_ASSERT(lstat(final, &st) != 0 && errno == ENOENT);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	UT_ASSERT(lstat(temp, &st) != 0 && errno == ENOENT);
	UT_ASSERT(lstat(final, &st) == 0 && st.st_size == PG_CONTROL_FILE_SIZE);
	REQUIRE_IMAGE_OK(
		cluster_cf_control_image_read_locked(17, stage.image_sha256, in.system_identifier, &out));
	UT_ASSERT_EQ(out.time, in.time);
	UT_ASSERT_EQ(out.minRecoveryPoint, in.minRecoveryPoint);
	UT_ASSERT_EQ(out.backupEndPoint, in.backupEndPoint);
	UT_ASSERT_EQ(out.backupEndRequired, true);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
	UT_ASSERT(image_zero(&stage, sizeof(stage)));
	UT_ASSERT(lstat(final, &st) == 0);
}

UT_TEST(test_immutable_requires_actual_clusterwide_lock)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	int mode;

	image_input(&in);
	image_uuid(uuid);
	image_cf_x = false;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 18, uuid, &stage));
	for (mode = 0; mode < 3; ++mode) {
		image_cf_x = mode == 2;
		image_cf_s = mode == 1;
		image_cf_global = mode != 2;
		UT_ASSERT_EQ(cluster_cf_control_image_install(&stage),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	}
	image_cf_x = true;
	image_cf_global = true;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	image_cf_global = false;
	memset(&out, 0xee, sizeof(out));
	UT_ASSERT_EQ(
		cluster_cf_control_image_read_locked(18, stage.image_sha256, in.system_identifier, &out),
		CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(image_zero(&out, sizeof(out)));
	image_cf_x = false;
	image_cf_s = true;
	image_cf_global = true;
	REQUIRE_IMAGE_OK(
		cluster_cf_control_image_read_locked(18, stage.image_sha256, in.system_identifier, &out));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
}

UT_TEST(test_immutable_same_generation_cannot_clobber)
{
	ControlFileData a, b, out;
	ClusterCfImageStage sa, sb, duplicate;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat before, after;

	image_input(&a);
	image_input(&b);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&a, 19, uuid, &sa));
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&b, 19, uuid, &sb));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&sa));
	image_paths(&sa, final, temp);
	UT_ASSERT(stat(final, &before) == 0);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&sb));
	UT_ASSERT(memcmp(sa.image_sha256, sb.image_sha256, 32) != 0);
	REQUIRE_IMAGE_OK(
		cluster_cf_control_image_read_locked(19, sa.image_sha256, a.system_identifier, &out));
	UT_ASSERT_EQ(out.time, a.time);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&a, 19, uuid, &duplicate));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&duplicate));
	UT_ASSERT(stat(final, &after) == 0 && before.st_ino == after.st_ino);
}

UT_TEST(test_immutable_corrupt_existing_destination_is_not_replaced)
{
	ControlFileData in, out;
	ClusterCfImageStage first, retry;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat before, after;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 20, uuid, &first));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&first));
	image_paths(&first, final, temp);
	UT_ASSERT(stat(final, &before) == 0);
	flip_byte(final, 0);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 20, uuid, &retry));
	UT_ASSERT_EQ(cluster_cf_control_image_install(&retry), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(stat(final, &after) == 0 && before.st_ino == after.st_ino);
	UT_ASSERT_EQ(
		cluster_cf_control_image_read_locked(20, first.image_sha256, in.system_identifier, &out),
		CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(image_zero(&out, sizeof(out)));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&retry));
}

UT_TEST(test_immutable_linked_but_unsynced_is_not_success)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 21, uuid, &stage));
	image_paths(&stage, final, temp);
	image_fsync_calls = 0;
	image_fsync_fail_at = 1;
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(lstat(final, &st) == 0 && lstat(temp, &st) == 0);
	image_fsync_fail_at = 0;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	REQUIRE_IMAGE_OK(
		cluster_cf_control_image_read_locked(21, stage.image_sha256, in.system_identifier, &out));
	UT_ASSERT(lstat(temp, &st) != 0 && errno == ENOENT);
	UT_ASSERT_EQ(out.time, in.time);
}

UT_TEST(test_immutable_uuid_collision_and_stale_discard_do_not_delete)
{
	ControlFileData in;
	ClusterCfImageStage stage, other;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 22, uuid, &stage));
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 22, uuid, &other)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&other, sizeof(other)));
	image_paths(&stage, final, temp);
	other = stage;
	other.file_ino++;
	UT_ASSERT(cluster_cf_control_image_discard(&other) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(lstat(temp, &st) == 0);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
	UT_ASSERT(lstat(temp, &st) != 0 && errno == ENOENT);
	UT_ASSERT(lstat(final, &st) != 0 && errno == ENOENT);
}

UT_TEST(test_immutable_symlink_and_wrong_owner_are_rejected)
{
	ControlFileData in;
	ClusterCfImageStage stage, other;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 23, uuid, &stage));
	image_paths(&stage, final, temp);
	other = stage;
	other.owner_pid++;
	UT_ASSERT(cluster_cf_control_image_install(&other) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(symlink(temp, final) == 0);
	UT_ASSERT(cluster_cf_control_image_install(&stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(lstat(final, &st) == 0 && S_ISLNK(st.st_mode));
	UT_ASSERT(unlink(final) == 0);
	UT_ASSERT(unlink(temp) == 0 && symlink(cluster_cf_shared_path(), temp) == 0);
	UT_ASSERT(cluster_cf_control_image_install(&stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_control_image_discard(&stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(lstat(temp, &st) == 0 && S_ISLNK(st.st_mode));
	UT_ASSERT(unlink(temp) == 0);
}

UT_TEST(test_immutable_invalid_input_and_wrong_read_identity)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16], zero[32] = { 0 };
	uint8 bytes[PG_CONTROL_FILE_SIZE];

	image_input(&in);
	image_uuid(uuid);
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 0, uuid, &stage)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&stage, sizeof(stage)));
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 24, zero, &stage)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_control_image_encode(NULL, bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(bytes, sizeof(bytes)));
	in.catalog_version_no++;
	UT_ASSERT(cluster_cf_control_image_encode(&in, bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(bytes, sizeof(bytes)));
	in.catalog_version_no--;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 24, uuid, &stage));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	UT_ASSERT_EQ(cluster_cf_control_image_read_locked(24, stage.image_sha256,
													  in.system_identifier + 1, &out),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(image_zero(&out, sizeof(out)));
	UT_ASSERT(cluster_cf_control_image_read_locked(24, zero, in.system_identifier, &out)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(test_immutable_prepare_sync_failure_does_not_publish)
{
	ControlFileData in;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	unsigned step;

	for (step = 1; step <= 2; ++step) {
		image_input(&in);
		image_uuid(uuid);
		image_fsync_fail_at = step;
		UT_ASSERT_EQ(cluster_cf_control_image_prepare(&in, 25, uuid, &stage),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(image_zero(&stage, sizeof(stage)));
		image_fsync_fail_at = 0;
		/* Exact failure cleanup permits the same UUID without clobbering. */
		REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 25, uuid, &stage));
		REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
	}
}

UT_TEST(test_immutable_unlinked_but_unsynced_retries_install)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 26, uuid, &stage));
	image_paths(&stage, final, temp);
	image_fsync_calls = 0;
	image_fsync_fail_at = 2;
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(lstat(temp, &st) != 0 && errno == ENOENT);
	UT_ASSERT(lstat(final, &st) == 0);
	image_fsync_fail_at = 0;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	REQUIRE_IMAGE_OK(
		cluster_cf_control_image_read_locked(26, stage.image_sha256, in.system_identifier, &out));
	UT_ASSERT_EQ(out.time, in.time);
}

UT_TEST(test_immutable_discard_sync_failure_cannot_resume_install)
{
	ControlFileData in;
	ClusterCfImageStage first, discarded;
	uint8 uuid[16];

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 27, uuid, &first));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&first));
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 27, uuid, &discarded));
	image_fsync_calls = 0;
	image_fsync_fail_at = 1;
	UT_ASSERT_EQ(cluster_cf_control_image_discard(&discarded), CLUSTER_CONTROL_ROOT_IO_ERROR);
	image_fsync_fail_at = 0;
	/* Identical content exists, but discard has retired THIS operation. */
	UT_ASSERT(cluster_cf_control_image_install(&discarded) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&discarded));
	UT_ASSERT(image_zero(&discarded, sizeof(discarded)));
	/* Already-installed operations must also retire before a fallible sync. */
	image_fsync_calls = 0;
	image_fsync_fail_at = 1;
	UT_ASSERT_EQ(cluster_cf_control_image_discard(&first), CLUSTER_CONTROL_ROOT_IO_ERROR);
	image_fsync_fail_at = 0;
	UT_ASSERT_EQ(cluster_cf_control_image_install(&first), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&first));
	UT_ASSERT(image_zero(&first, sizeof(first)));
}

UT_TEST(test_immutable_directory_symlink_and_writable_paths_refuse)
{
	ControlFileData in;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char staging[MAXPGPATH], held[MAXPGPATH];

	image_input(&in);
	image_uuid(uuid);
	snprintf(staging, sizeof(staging), "%s/global/control_images/.staging", shared_root);
	snprintf(held, sizeof(held), "%s/global/control_images/.staging-held", shared_root);
	UT_ASSERT(chmod(staging, 0770) == 0);
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 28, uuid, &stage)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&stage, sizeof(stage)));
	UT_ASSERT(chmod(staging, 0700) == 0);
	UT_ASSERT(rename(staging, held) == 0 && symlink(held, staging) == 0);
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 28, uuid, &stage)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&stage, sizeof(stage)));
	UT_ASSERT(unlink(staging) == 0 && rename(held, staging) == 0);
}

UT_TEST(test_immutable_staging_directory_identity_cannot_change)
{
	ControlFileData in;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char staging[MAXPGPATH], held[MAXPGPATH];

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 29, uuid, &stage));
	snprintf(staging, sizeof(staging), "%s/global/control_images/.staging", shared_root);
	snprintf(held, sizeof(held), "%s/global/control_images/.staging-held", shared_root);
	UT_ASSERT(rename(staging, held) == 0 && mkdir(staging, 0700) == 0);
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(cluster_cf_control_image_discard(&stage), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(rmdir(staging) == 0 && rename(held, staging) == 0);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
}

UT_TEST(test_immutable_changed_staging_bytes_and_size_refuse)
{
	ControlFileData in;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];
	struct stat st;

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 30, uuid, &stage));
	image_paths(&stage, final, temp);
	flip_byte(temp, 0);
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(truncate(temp, PG_CONTROL_FILE_SIZE - 1) == 0);
	UT_ASSERT_EQ(cluster_cf_control_image_install(&stage), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	UT_ASSERT(lstat(final, &st) != 0 && errno == ENOENT);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
}

UT_TEST(test_immutable_missing_exact_object_never_falls_back)
{
	ControlFileData in, out;
	ClusterCfImageStage stage;
	uint8 uuid[16];
	char final[MAXPGPATH], temp[MAXPGPATH];

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 31, uuid, &stage));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&stage));
	image_paths(&stage, final, temp);
	cluster_cf_authority_write(&in);
	UT_ASSERT(unlink(final) == 0);
	UT_ASSERT_EQ(
		cluster_cf_control_image_read_locked(31, stage.image_sha256, in.system_identifier, &out),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(image_zero(&out, sizeof(out)));
	/* Nor may a malformed special file turn an exact read into a wait. */
	UT_ASSERT(mkfifo(final, 0600) == 0);
	UT_ASSERT(
		cluster_cf_control_image_read_locked(31, stage.image_sha256, in.system_identifier, &out)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&out, sizeof(out)));
	UT_ASSERT(unlink(final) == 0);
}

UT_TEST(test_immutable_fsync_disabled_cannot_claim_durable_success)
{
	ControlFileData in;
	ClusterCfImageStage stage, other;
	uint8 uuid[16];

	image_input(&in);
	image_uuid(uuid);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&in, 32, uuid, &stage));
	enableFsync = false;
	UT_ASSERT(cluster_cf_control_image_install(&stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_cf_control_image_discard(&stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	image_uuid(uuid);
	UT_ASSERT(cluster_cf_control_image_prepare(&in, 32, uuid, &other)
			  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(image_zero(&other, sizeof(other)));
	enableFsync = true;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_discard(&stage));
}

UT_TEST(test_shared_config_dispatches_without_legacy_fallback)
{
	ControlFileData in, out;

	image_input(&in);
	cluster_cf_authority_write(&in);
	cluster_shared_config = true;
	runtime_read_calls = 0;
	out = in;
	UT_ASSERT(!cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(runtime_read_calls, 1);
	UT_ASSERT_EQ(memcmp(&out, &in, sizeof(out)), 0);
	cluster_shared_config = false;
}

UT_TEST(test_shared_config_untyped_writer_cannot_modify_projection)
{
	ControlFileData before, candidate, after;
	pid_t child;
	int status = 0;

	image_input(&before);
	cluster_cf_authority_write(&before);
	UT_ASSERT(cluster_cf_authority_read(&before));
	candidate = before;
	candidate.checkPoint += 8192;
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		cluster_shared_config = true;
		runtime_guard_error_expected = true;
		cluster_cf_authority_write(&candidate);
		_exit(0);
	}
	if (child > 0) {
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 90);
	}
	UT_ASSERT(cluster_cf_authority_read(&after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
}

static void
projection_read(uint8 bytes[PG_CONTROL_FILE_SIZE])
{
	int fd = open(cluster_cf_shared_path(), O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(read(fd, bytes, PG_CONTROL_FILE_SIZE), PG_CONTROL_FILE_SIZE);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(test_projection_writes_canonical_selected_view)
{
	ControlFileData in;
	uint8 expected[PG_CONTROL_FILE_SIZE], actual[PG_CONTROL_FILE_SIZE];
	image_input(&in);
	finalize_crc(&in);
	cluster_shared_config = true;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_encode(&in, expected));
	REQUIRE_IMAGE_OK(cluster_cf_control_projection_write_locked(&in));
	projection_read(actual);
	UT_ASSERT(memcmp(expected, actual, sizeof(actual)) == 0);
	UT_ASSERT_EQ(image_fsync_calls, 2);
	cluster_shared_config = false;
}

UT_TEST(test_projection_refuses_without_permission_or_valid_input)
{
	ControlFileData in;
	uint8 before[PG_CONTROL_FILE_SIZE], after[PG_CONTROL_FILE_SIZE];
	/* Seed via the existing writer; the new writer is independently tested. */
	cluster_shared_config = false;
	image_input(&in);
	finalize_crc(&in);
	cluster_cf_authority_write(&in);
	projection_read(before);
	for (int fault = 0; fault < 5; fault++) {
		image_input(&in);
		finalize_crc(&in);
		cluster_shared_config = fault != 0;
		image_cf_x = fault != 1;
		image_cf_global = fault != 2;
		enableFsync = fault != 3;
		if (fault == 4)
			in.crc ^= 1;
		UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
		UT_ASSERT_EQ(image_fsync_calls, 0);
		projection_read(after);
		UT_ASSERT(memcmp(before, after, sizeof(after)) == 0);
	}
	enableFsync = true;
	cluster_shared_config = false;
}

UT_TEST(test_projection_fsync_failure_preserves_publication_boundary)
{
	ControlFileData old, in;
	uint8 before[PG_CONTROL_FILE_SIZE], expected[PG_CONTROL_FILE_SIZE], after[PG_CONTROL_FILE_SIZE];
	for (unsigned fault = 1; fault <= 2; fault++) {
		cluster_shared_config = false;
		image_input(&old);
		cluster_cf_authority_write(&old);
		projection_read(before);
		image_input(&in);
		finalize_crc(&in);
		REQUIRE_IMAGE_OK(cluster_cf_control_image_encode(&in, expected));
		cluster_shared_config = true;
		image_fsync_fail_at = fault;
		UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
		UT_ASSERT_EQ(image_fsync_calls, fault);
		projection_read(after);
		/* File fsync failed: old remains. Parent fsync failed: new is not undone. */
		UT_ASSERT(memcmp(after, fault == 1 ? before : expected, sizeof(after)) == 0);
		image_fsync_fail_at = 0;
		REQUIRE_IMAGE_OK(cluster_cf_control_projection_write_locked(&in));
		projection_read(after);
		UT_ASSERT(memcmp(after, expected, sizeof(after)) == 0);
	}
	cluster_shared_config = false;
}

UT_TEST(test_projection_rejects_unsafe_existing_target)
{
	ControlFileData in;
	char path[MAXPGPATH], saved[MAXPGPATH];
	cluster_shared_config = false;
	image_input(&in);
	finalize_crc(&in);
	cluster_cf_authority_write(&in);
	strlcpy(path, cluster_cf_shared_path(), sizeof(path));
	snprintf(saved, sizeof(saved), "%s/projection-saved", shared_root);
	UT_ASSERT_EQ(rename(path, saved), 0);
	cluster_shared_config = true;
	UT_ASSERT_EQ(symlink(saved, path), 0);
	UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(link(saved, path), 0);
	UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rename(saved, path), 0);
	UT_ASSERT_EQ(chmod(path, 0666), 0);
	UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
	UT_ASSERT_EQ(chmod(path, 0600), 0);
	REQUIRE_IMAGE_OK(cluster_cf_control_projection_write_locked(&in));
	cluster_shared_config = false;
}

static int projection_race;
static void
projection_race_at_sync(int fd pg_attribute_unused())
{
	char path[MAXPGPATH], saved[MAXPGPATH];
	int replacement;
	if (projection_race == 1) {
		snprintf(path, sizeof(path), "%s/global", shared_root);
		snprintf(saved, sizeof(saved), "%s/global.projection-old", shared_root);
		UT_ASSERT_EQ(rename(path, saved), 0);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
		return;
	}
	if (projection_race == 2) {
		snprintf(saved, sizeof(saved), "%s.projection-old", shared_root);
		UT_ASSERT_EQ(rename(shared_root, saved), 0);
		UT_ASSERT_EQ(mkdir(shared_root, 0700), 0);
		return;
	}
	snprintf(path, sizeof(path), "%s/global/%s", shared_root,
			 projection_race == 0 ? projection_random_name : "pg_control");
	UT_ASSERT_EQ(unlink(path), 0);
	replacement = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(replacement >= 0);
	if (replacement >= 0) {
		UT_ASSERT_EQ(write(replacement, "other", 5), 5);
		UT_ASSERT_EQ(close(replacement), 0);
	}
}

UT_TEST(test_projection_replaced_names_cannot_claim_success_or_delete_others)
{
	ControlFileData in;
	char path[MAXPGPATH], saved[MAXPGPATH], mark[5];
	for (projection_race = 0; projection_race < 4; projection_race++) {
		int before = 0, after = 0, fd;
		cluster_shared_config = false;
		image_input(&in);
		finalize_crc(&in);
		cluster_cf_authority_write(&in);
		cluster_shared_config = true;
		image_fsync_calls = 0;
		image_fsync_hook_at = projection_race == 3 ? 2 : 1;
		image_fsync_hook = projection_race_at_sync;
		for (fd = 0; fd < 256; fd++)
			if (fcntl(fd, F_GETFD) >= 0)
				before++;
		UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
		UT_ASSERT(image_fsync_hook == NULL);
		for (fd = 0; fd < 256; fd++)
			if (fcntl(fd, F_GETFD) >= 0)
				after++;
		UT_ASSERT_EQ(before, after);
		if (projection_race == 1) {
			snprintf(path, sizeof(path), "%s/global", shared_root);
			snprintf(saved, sizeof(saved), "%s/global.projection-old", shared_root);
			UT_ASSERT_EQ(rmdir(path), 0);
			UT_ASSERT_EQ(rename(saved, path), 0);
		} else if (projection_race == 2) {
			snprintf(saved, sizeof(saved), "%s.projection-old", shared_root);
			UT_ASSERT_EQ(rmdir(shared_root), 0);
			UT_ASSERT_EQ(rename(saved, shared_root), 0);
		} else {
			snprintf(path, sizeof(path), "%s/global/%s", shared_root,
					 projection_race == 0 ? projection_random_name : "pg_control");
			fd = open(path, O_RDONLY | O_NOFOLLOW);
			UT_ASSERT(fd >= 0);
			if (fd >= 0) {
				UT_ASSERT_EQ(read(fd, mark, sizeof(mark)), sizeof(mark));
				UT_ASSERT(memcmp(mark, "other", sizeof(mark)) == 0);
				UT_ASSERT_EQ(close(fd), 0);
			}
			UT_ASSERT_EQ(unlink(path), 0);
		}
	}
	cluster_shared_config = false;
}

UT_TEST(test_projection_collision_does_not_remove_preexisting_temporary)
{
	ControlFileData in;
	char path[MAXPGPATH], bytes[5];
	int fd;
	image_input(&in);
	finalize_crc(&in);
	cluster_shared_config = true;
	projection_random_collision = true;
	UT_ASSERT(cluster_cf_control_projection_write_locked(&in) != 0);
	projection_random_collision = false;
	snprintf(path, sizeof(path), "%s/global/%s", shared_root, projection_random_name);
	fd = open(path, O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(read(fd, bytes, sizeof(bytes)), sizeof(bytes));
		UT_ASSERT(memcmp(bytes, "other", sizeof(bytes)) == 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	UT_ASSERT_EQ(unlink(path), 0);
	cluster_shared_config = false;
}

int
main(void)
{
	setup_shared_root();

	UT_PLAN(33);
	UT_RUN(test_paths);
	UT_RUN(test_classify_buffer);
	UT_RUN(test_decide_source);
	UT_RUN(test_write_read_roundtrip);
	UT_RUN(test_bak_fallback);
	UT_RUN(test_both_bad_failclosed);
	UT_RUN(test_bak_strict_ok);
	UT_RUN(test_bak_fallback_unrecoverable);
	UT_RUN(test_immutable_native_named_fields_and_padding);
	UT_RUN(test_immutable_prepare_install_read_and_discard);
	UT_RUN(test_immutable_requires_actual_clusterwide_lock);
	UT_RUN(test_immutable_same_generation_cannot_clobber);
	UT_RUN(test_immutable_corrupt_existing_destination_is_not_replaced);
	UT_RUN(test_immutable_linked_but_unsynced_is_not_success);
	UT_RUN(test_immutable_uuid_collision_and_stale_discard_do_not_delete);
	UT_RUN(test_immutable_symlink_and_wrong_owner_are_rejected);
	UT_RUN(test_immutable_invalid_input_and_wrong_read_identity);
	UT_RUN(test_immutable_prepare_sync_failure_does_not_publish);
	UT_RUN(test_immutable_unlinked_but_unsynced_retries_install);
	UT_RUN(test_immutable_discard_sync_failure_cannot_resume_install);
	UT_RUN(test_immutable_directory_symlink_and_writable_paths_refuse);
	UT_RUN(test_immutable_staging_directory_identity_cannot_change);
	UT_RUN(test_immutable_changed_staging_bytes_and_size_refuse);
	UT_RUN(test_immutable_missing_exact_object_never_falls_back);
	UT_RUN(test_immutable_fsync_disabled_cannot_claim_durable_success);
	UT_RUN(test_shared_config_dispatches_without_legacy_fallback);
	UT_RUN(test_shared_config_untyped_writer_cannot_modify_projection);
	UT_RUN(test_projection_writes_canonical_selected_view);
	UT_RUN(test_projection_refuses_without_permission_or_valid_input);
	UT_RUN(test_projection_fsync_failure_preserves_publication_boundary);
	UT_RUN(test_projection_rejects_unsafe_existing_target);
	UT_RUN(test_projection_replaced_names_cannot_claim_success_or_delete_others);
	UT_RUN(test_projection_collision_does_not_remove_preexisting_temporary);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
