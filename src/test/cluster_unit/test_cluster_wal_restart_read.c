/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_restart_read.c
 *    Native startup WAL selection against retained generation files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_wal_restart_read.c
 *
 * NOTES
 *    PGRAC-original fixture. XLogFileRead is extracted verbatim; filesystem,
 *    claim encoding and SHA are real. Only process/error/runtime boundaries
 *    are supplied here. A wrong pg_wal fallback returns different bytes.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_restart_read.h"
#include "common/cryptohash.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "utils/timestamp.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

typedef enum {
	XLOG_FROM_ANY = 0,
	XLOG_FROM_ARCHIVE = 1,
	XLOG_FROM_PG_WAL = 2,
	XLOG_FROM_STREAM = 4
} XLogSource;

bool cluster_enabled = true, cluster_shared_config = true;
char *cluster_wal_threads_dir;
static char *DataDir;
static int cluster_node_id = 1;
int wal_segment_size = 1024 * 1024;
static TimeLineID curFileTLI;
static XLogSource readSource, XLogReceiptSource;
static TimestampTz XLogReceiptTime;
static bool InRedo;
static ClusterWalDurablePrefixRef input;
static bool error_expected;
static sigjmp_buf error_jmp;
static int archive_calls, error_level;
static char scratch[MAXPGPATH], old_dir[MAXPGPATH], old_file[MAXPGPATH];
static char claim_path[MAXPGPATH], new_file[MAXPGPATH];
static int original_dir = -1;
static int open_action;
static bool fail_directory_close;
static int intercepted_openat(int dir, const char *path, int flags);
static int intercepted_close(int fd);

/* Only the syscall boundary changes; the product opener and claim reader
 * run on real descriptors. Mutations occur after actual segment open. */
#define openat intercepted_openat
#define close intercepted_close
#include "../../backend/cluster/cluster_wal_restart_read.c"
#undef openat
#undef close

/* The root validator boundary supplies a qualified input. Actual production
 * initialization/accessors must preserve it independently of the writer. */
static void
cluster_control_bootstrap_wal_recheck(const char *dir pg_attribute_unused(),
									  ClusterWalDurablePrefixRef *out)
{
	*out = input;
}

uint16
cluster_wal_thread_id(void)
{
	return cluster_wal_thread_id_for(cluster_enabled, cluster_node_id);
}
#include "test_cluster_wal_restart_mirror.inc"
static ClusterWalThreadShmemData mirror;

int
BasicOpenFile(const char *path, int flags)
{
	return open(path, flags, 0600);
}

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	if (!error_expected)
		abort();
	error_level = level;
	siglongjmp(error_jmp, 1);
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code pg_attribute_unused())
{
	abort();
}
int
errcode_for_file_access(void)
{
	abort();
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	abort();
}

static void
set_ps_display(const char *activity pg_attribute_unused())
{}
static bool
RestoreArchivedFile(char *path pg_attribute_unused(), const char *name pg_attribute_unused(),
					const char *temp pg_attribute_unused(), off_t size pg_attribute_unused(),
					bool redo pg_attribute_unused())
{
	archive_calls++;
	return false;
}
static bool
IsInstallXLogFileSegmentActive(void)
{
	return false;
}
static void
KeepFileRestoredFromArchive(const char *p pg_attribute_unused(),
							const char *n pg_attribute_unused())
{
	abort();
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 123;
}

#include "test_cluster_native_wal_read.inc"

static void
write_bytes(const char *path, const void *bytes, size_t count, off_t size)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, count) != (ssize_t)count || ftruncate(fd, size) || close(fd))
		abort();
}

static void
fixture(void)
{
	char path[MAXPGPATH], name[MAXFNAMELEN];
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	ClusterWalThreadClaimV2 claim = { 0 };
	pg_cryptohash_ctx *hash;
	if (original_dir < 0)
		original_dir = open(".", O_RDONLY | O_DIRECTORY);
	if (original_dir < 0 || fchdir(original_dir))
		abort();
	if (scratch[0] && !rmtree(scratch, true))
		abort();
	strlcpy(scratch, "/tmp/pgrac-wal-restart-XXXXXX", sizeof(scratch));
	if (!mkdtemp(scratch) || chdir(scratch) || mkdir("thread_2", 0700)
		|| mkdir("thread_2/generation_99", 0700) || mkdir("successor", 0700)
		|| symlink("successor", "pg_wal"))
		abort();
	cluster_wal_threads_dir = scratch;
	DataDir = scratch;
	snprintf(old_dir, sizeof(old_dir), "%s/thread_2/generation_99", scratch);
	snprintf(claim_path, sizeof(claim_path), "%s/pgrac_thread.claim", old_dir);
	claim.identity.system_identifier = UINT64CONST(0x1122334455667788);
	memset(claim.identity.storage_uuid, 3, 16);
	memset(claim.identity.authority_uuid, 5, 16);
	claim.identity.origin_node_id = 1;
	claim.identity.origin_thread_id = 2;
	claim.identity.origin_owner_incarnation = 99;
	claim.identity.root_lineage_seq = 9;
	claim.identity.thread_claim_created_at = 23;
	claim.database_incarnation = 7;
	claim.config_generation = claim.claim_generation = 1;
	if (cluster_wal_claim_v2_encode(&claim, bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		abort();
	memset(&input, 0, sizeof(input));
	input.claim.identity = claim.identity;
	for (int i = 0; i < 4; ++i)
		input.claim.identity.thread_claim_crc32c |= (uint32)bytes[104 + i] << (8 * i);
	input.claim.database_incarnation = 7;
	input.claim.max_config_generation = 1;
	input.timeline = 7;
	hash = pg_cryptohash_create(PG_SHA256);
	if (!hash || pg_cryptohash_init(hash) || pg_cryptohash_update(hash, bytes, sizeof(bytes))
		|| pg_cryptohash_final(hash, input.claim.claim_sha256, 32))
		abort();
	pg_cryptohash_free(hash);
	write_bytes(claim_path, bytes, sizeof(bytes), sizeof(bytes));
	XLogFileName(name, 7, 1, wal_segment_size);
	snprintf(old_file, sizeof(old_file), "%s/%s", old_dir, name);
	snprintf(new_file, sizeof(new_file), "%s/successor/%s", scratch, name);
	write_bytes(old_file, "A", 1, wal_segment_size);
	write_bytes(new_file, "B", 1, wal_segment_size);
	XLogFileName(name, 8, 1, wal_segment_size);
	snprintf(path, sizeof(path), "%s/successor/%s", scratch, name);
	write_bytes(path, "C", 1, wal_segment_size);
	cluster_enabled = cluster_shared_config = true;
	cluster_node_id = 1;
	memset(&mirror, 0, sizeof(mirror));
	cluster_wal_thread_shmem = &mirror;
	actual_select_input();
	error_expected = false;
	error_level = archive_calls = 0;
	open_action = 0;
	fail_directory_close = false;
}

static int
intercepted_close(int fd)
{
	struct stat st;
	bool fail = fail_directory_close && fstat(fd, &st) == 0 && S_ISDIR(st.st_mode);
	int result = close(fd);
	if (fail) {
		fail_directory_close = false;
		errno = EIO;
		return -1;
	}
	return result;
}

static int
intercepted_openat(int dir, const char *path, int flags)
{
	int fd = openat(dir, path, flags);
	if (fd >= 0 && strlen(path) == 24 && path[0] == '0' && open_action != 0) {
		char backup[MAXPGPATH];
		int action = open_action;
		open_action = 0;
		if (action == 1) {
			snprintf(backup, sizeof(backup), "%s.saved", old_file);
			if (rename(old_file, backup))
				abort();
			write_bytes(old_file, "D", 1, wal_segment_size);
		} else if (action == 2) {
			snprintf(backup, sizeof(backup), "%s.saved", old_dir);
			if (rename(old_dir, backup) || mkdir(old_dir, 0700))
				abort();
		} else if (action == 3) {
			write_bytes(claim_path, "bad", 3, 112);
		} else
			abort();
	}
	return fd;
}

static int
open_count(void)
{
	int count = 0;
	for (int fd = 0; fd < 1024; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			count++;
	return count;
}

static void
expect_open_result(ClusterControlRootResult expected)
{
	int fd = 777;
	int before = open_count();
	ClusterControlRootResult result
		= cluster_wal_restart_segment_open(scratch, &input, 7, 1, wal_segment_size, &fd);
	UT_ASSERT_EQ(result, expected);
	UT_ASSERT_EQ(fd, -1);
	if (fd >= 0 && fd != 777)
		close(fd);
	UT_ASSERT_EQ(open_count(), before);
}

static void
expect_native_refusal(TimeLineID tli, XLogSource source)
{
	error_expected = true;
	if (sigsetjmp(error_jmp, 1) == 0) {
		int fd = XLogFileRead(1, ERROR, tli, source, true);
		if (fd >= 0)
			close(fd);
		UT_ASSERT(false);
	}
	UT_ASSERT(error_level >= ERROR);
	error_expected = false;
}

UT_TEST(native_reads_retained_input_not_pg_wal)
{
	char value = 0;
	int fd;
	fixture();
	fd = XLogFileRead(1, ERROR, 7, XLOG_FROM_PG_WAL, false);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(read(fd, &value, 1), 1);
		UT_ASSERT_EQ(value, 'A');
		UT_ASSERT_EQ(fcntl(fd, F_GETFL) & O_ACCMODE, O_RDONLY);
		UT_ASSERT_EQ(write(fd, "X", 1), -1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	UT_ASSERT_EQ(curFileTLI, 7);
	UT_ASSERT_EQ(readSource, XLOG_FROM_PG_WAL);
}

UT_TEST(native_bad_input_never_falls_back)
{
	fixture();
	mirror.restart_ref_valid = false;
	expect_native_refusal(7, XLOG_FROM_PG_WAL);
	fixture();
	UT_ASSERT_EQ(unlink(claim_path), 0);
	expect_native_refusal(7, XLOG_FROM_PG_WAL);
	fixture();
	expect_native_refusal(8, XLOG_FROM_PG_WAL);
	fixture();
	expect_native_refusal(7, XLOG_FROM_ARCHIVE);
	UT_ASSERT_EQ(archive_calls, 0);
	fixture();
	expect_native_refusal(7, XLOG_FROM_STREAM);
}

UT_TEST(legacy_keeps_native_route)
{
	int fd;
	char value = 0;
	fixture();
	cluster_shared_config = false;
	fd = XLogFileRead(1, ERROR, 7, XLOG_FROM_PG_WAL, false);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(read(fd, &value, 1), 1);
		UT_ASSERT_EQ(value, 'B');
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(missing_segment_is_not_missing_identity)
{
	fixture();
	UT_ASSERT_EQ(unlink(old_file), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(XLogFileRead(1, ERROR, 7, XLOG_FROM_PG_WAL, true), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(unlink(claim_path), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	expect_native_refusal(7, XLOG_FROM_PG_WAL);
}

UT_TEST(corrupt_and_wrong_claims_refused)
{
	fixture();
	write_bytes(claim_path, "bad", 3, 3);
	expect_open_result(CLUSTER_CONTROL_ROOT_BAD_SIZE);
	fixture();
	write_bytes(claim_path, "bad", 3, 112);
	expect_open_result(CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	fixture();
	input.claim.identity.origin_owner_incarnation = 100;
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	input.claim.identity.root_lineage_seq++;
	expect_open_result(CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	fixture();
	input.claim.claim_sha256[0] ^= 1;
	expect_open_result(CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
}

UT_TEST(unsafe_files_and_directories_refused)
{
	char backup[MAXPGPATH];
	fixture();
	UT_ASSERT_EQ(unlink(old_file), 0);
	UT_ASSERT_EQ(symlink(new_file, old_file), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	UT_ASSERT_EQ(unlink(old_file), 0);
	UT_ASSERT_EQ(mkfifo(old_file, 0600), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	UT_ASSERT_EQ(unlink(old_file), 0);
	UT_ASSERT_EQ(mkdir(old_file, 0700), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	UT_ASSERT_EQ(chmod(old_file, 0660), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	UT_ASSERT_EQ(chmod(old_dir, 0770), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	snprintf(backup, sizeof(backup), "%s.saved", old_dir);
	UT_ASSERT_EQ(rename(old_dir, backup), 0);
	UT_ASSERT_EQ(symlink(backup, old_dir), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	snprintf(backup, sizeof(backup), "%s.saved", claim_path);
	UT_ASSERT_EQ(rename(claim_path, backup), 0);
	UT_ASSERT_EQ(symlink(backup, claim_path), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	snprintf(backup, sizeof(backup), "%s.link", old_file);
	UT_ASSERT_EQ(link(old_file, backup), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	fixture();
	UT_ASSERT_EQ(truncate(old_file, 3), 0);
	expect_open_result(CLUSTER_CONTROL_ROOT_BAD_SIZE);
}

UT_TEST(namespace_replacement_after_open_refused)
{
	for (int action = 1; action <= 3; ++action) {
		fixture();
		open_action = action;
		expect_open_result(CLUSTER_CONTROL_ROOT_POSTREAD_FAILED);
		UT_ASSERT_EQ(open_action, 0);
	}
}

UT_TEST(invalid_inputs_clear_descriptor)
{
	const char *paths[] = { NULL, "", "/", ".", "/tmp/../tmp", "/tmp//x", "/tmp/", "/tmp/./x" };
	int fd;
	fixture();
	for (size_t i = 0; i < lengthof(paths); ++i) {
		fd = 777;
		UT_ASSERT_EQ(
			cluster_wal_restart_segment_open(paths[i], &input, 7, 1, wal_segment_size, &fd),
			CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(fd, -1);
	}
	fd = 777;
	UT_ASSERT_EQ(cluster_wal_restart_segment_open(scratch, NULL, 7, 1, wal_segment_size, &fd),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT_EQ(cluster_wal_restart_segment_open(scratch, &input, 7, 1, wal_segment_size, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_restart_segment_open(scratch, &input, 7, 0, wal_segment_size, &fd),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT_EQ(cluster_wal_restart_segment_open(scratch, &input, 7, 1, 0, &fd),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(fd, -1);
	UT_ASSERT_EQ(
		cluster_wal_restart_segment_open(scratch, &input, 7, PG_UINT64_MAX, wal_segment_size, &fd),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(fd, -1);
}

UT_TEST(repeated_refusals_do_not_leak_descriptors)
{
	fixture();
	UT_ASSERT_EQ(chmod(old_file, 0660), 0);
	for (int i = 0; i < 50; ++i)
		expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
}

UT_TEST(restart_mirror_is_independent_and_profile_bound)
{
	ClusterWalDurablePrefixRef observed;
	fixture();
	mirror.v2_ref.claim.identity.origin_owner_incarnation = 100;
	UT_ASSERT(cluster_wal_thread_current_v2_ref(&observed));
	UT_ASSERT_EQ(observed.claim.identity.origin_owner_incarnation, 100);
	UT_ASSERT(cluster_wal_thread_restart_v2_ref(&observed));
	UT_ASSERT_EQ(observed.claim.identity.origin_owner_incarnation, 99);
	for (int fault = 0; fault < 5; ++fault) {
		fixture();
		if (fault == 0)
			cluster_enabled = false;
		if (fault == 1)
			cluster_shared_config = false;
		if (fault == 2)
			cluster_node_id = 2;
		if (fault == 3)
			mirror.dir_validated = 0;
		if (fault == 4)
			cluster_wal_thread_shmem = NULL;
		memset(&observed, 0xff, sizeof(observed));
		UT_ASSERT(!cluster_wal_thread_restart_v2_ref(&observed));
		for (size_t i = 0; i < sizeof(observed); ++i)
			UT_ASSERT_EQ(((uint8 *)&observed)[i], 0);
	}
	UT_ASSERT(!cluster_wal_thread_restart_v2_ref(NULL));
}

UT_TEST(close_failure_cannot_leak_successful_segment)
{
	fixture();
	fail_directory_close = true;
	expect_open_result(CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(!fail_directory_close);
}

static void
assert_segment_unchanged(const char *path, char first)
{
	char buf[8192];
	off_t total = 0;
	int fd = open(path, O_RDONLY);
	ssize_t n;
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	while ((n = read(fd, buf, sizeof(buf))) > 0) {
		for (ssize_t i = 0; i < n; ++i)
			UT_ASSERT_EQ(buf[i], total == 0 && i == 0 ? first : 0);
		total += n;
	}
	UT_ASSERT_EQ(n, 0);
	UT_ASSERT_EQ(total, wal_segment_size);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(reading_preserves_both_generations)
{
	int fd;
	struct stat st;
	ClusterWalThreadClaimV2 claim;
	fixture();
	fd = XLogFileRead(1, ERROR, 7, XLOG_FROM_PG_WAL, false);
	UT_ASSERT(fd >= 0);
	if (fd >= 0)
		UT_ASSERT_EQ(close(fd), 0);
	assert_segment_unchanged(old_file, 'A');
	assert_segment_unchanged(new_file, 'B');
	UT_ASSERT_EQ(cluster_wal_claim_v2_read(scratch, &input.claim, &claim),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	/* Ownership policy also refuses a foreign uid, without a privileged chown. */
	UT_ASSERT_EQ(stat(old_file, &st), 0);
	st.st_uid = geteuid() + 1;
	UT_ASSERT(!restart_owned(&st, false));
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(native_reads_retained_input_not_pg_wal);
	UT_RUN(native_bad_input_never_falls_back);
	UT_RUN(legacy_keeps_native_route);
	UT_RUN(missing_segment_is_not_missing_identity);
	UT_RUN(corrupt_and_wrong_claims_refused);
	UT_RUN(unsafe_files_and_directories_refused);
	UT_RUN(namespace_replacement_after_open_refused);
	UT_RUN(invalid_inputs_clear_descriptor);
	UT_RUN(repeated_refusals_do_not_leak_descriptors);
	UT_RUN(restart_mirror_is_independent_and_profile_bound);
	UT_RUN(close_failure_cannot_leak_successful_segment);
	UT_RUN(reading_preserves_both_generations);
	if (original_dir < 0 || fchdir(original_dir) || close(original_dir) || !rmtree(scratch, true))
		abort();
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
