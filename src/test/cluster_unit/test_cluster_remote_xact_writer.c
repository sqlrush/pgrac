/*-------------------------------------------------------------------------
 * Real remote-outcome mutation entrypoints, including release-build guards.
 * Only process roles and SLRU/lock/storage boundaries are fixtures. This is
 * not a shared-storage recovery or fencing qualification.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
/* Use c.h's exact release definitions even in the cassert test build. */
#undef Assert
#undef AssertMacro
#define Assert(condition) ((void)true)
#define AssertMacro(condition) ((void)true)
#include "access/slru.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_undo_recovery.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/sync.h"
#include <setjmp.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool IsUnderPostmaster;
bool cluster_shared_config;
char *cluster_shared_data_dir;
int pg_dir_create_mode = S_IRWXU;
static bool fixture_startup;
static int fixture_authorized_origin = -1;

bool
cluster_undo_recovery_origin_authorized_v1(int origin)
{
	return origin == fixture_authorized_origin;
}
#undef AmStartupProcess
#define AmStartupProcess() fixture_startup

static SlruCtlData ClusterRemoteXactCtlData;
#define ClusterRemoteXactCtl (&ClusterRemoteXactCtlData)
static SlruCtlData ClusterRemoteXactOriginCtlData[128];
static SlruCtl fixture_expected_ctl;
static LWLock *RemoteXactAccessLocks[128];
static LWLock fixture_access_lock;
static int access_lock_depth;
static bool fixture_directory_ok = true;
static bool fixture_revoke_on_lock;
static struct {
	pg_atomic_uint64 outcome_indoubt_count;
} fixture_stats, *RemoteXactShared;
static int remote_xact_online_writer_depth_v;
static SlruSharedData fixture_shared;
static LWLock fixture_lock;
static PGAlignedBlock fixture_page;
static PGAlignedBlock fixture_disk;
static bool fixture_physical_exists;
static bool fixture_files;
static unsigned physical_reads;
static unsigned directory_checks;
static char *fixture_buffers[1];
static SlruPageStatus fixture_status[1];
static bool fixture_dirty[1];
static int fixture_number[1];
static unsigned locks, writes, syncs, dirsyncs, scans, deletes;
static int lock_depth, deleted_segments[4];
static int sync_error;
static bool expect_error;
static jmp_buf error_boundary;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool fixture_native_directory_ready(SlruCtl ctl, int origin, bool write);

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

static int
fixture_segment_open(SlruCtl ctl, int segno, int flags)
{
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, segno);
	return open(path, flags, 0600);
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	longjmp(error_boundary, 1);
}

static bool
remote_xact_directory_ready(SlruCtl ctl, int origin, bool write)
{
	directory_checks++;
	UT_ASSERT(ctl == fixture_expected_ctl && origin == 1);
	UT_ASSERT_EQ(access_lock_depth, 1);
	(void)write;
	if (fixture_files)
		return fixture_native_directory_ready(ctl, origin, write);
	return fixture_directory_ok;
}

static void
fixture_report(int elevel)
{
	UT_ASSERT_EQ(elevel, ERROR);
	if (!expect_error)
		abort();
	pg_re_throw();
}
#undef ereport
#define ereport(elevel, args) fixture_report(elevel)

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == &fixture_access_lock) {
		UT_ASSERT(mode == LW_EXCLUSIVE && access_lock_depth == 0 && lock_depth == 0);
		access_lock_depth++;
		if (fixture_revoke_on_lock)
			fixture_authorized_origin = -1;
		return true;
	}
	UT_ASSERT(lock == &fixture_lock && mode == LW_EXCLUSIVE && lock_depth == 0);
	lock_depth++;
	locks++;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	if (lock == &fixture_access_lock) {
		UT_ASSERT(access_lock_depth == 1 && lock_depth == 0);
		access_lock_depth--;
		return;
	}
	UT_ASSERT(lock == &fixture_lock && lock_depth == 1);
	lock_depth--;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == &fixture_lock ? lock_depth != 0
		: lock == &fixture_access_lock && access_lock_depth != 0;
}
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == fixture_expected_ctl);
	if (fixture_files) {
		int fd = fixture_segment_open(ctl, pageno / SLRU_PAGES_PER_SEGMENT, O_RDONLY);
		struct stat st;
		bool exists;
		if (fd < 0) {
			UT_ASSERT_EQ(errno, ENOENT);
			return false;
		}
		exists = fstat(fd, &st) == 0
			&& st.st_size >= (off_t)((pageno % SLRU_PAGES_PER_SEGMENT) + 1) * BLCKSZ;
		UT_ASSERT_EQ(close(fd), 0);
		return exists;
	}
	(void)pageno;
	return fixture_physical_exists;
}
int
SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	UT_ASSERT(ctl == fixture_expected_ctl && lock_depth == 1);
	UT_ASSERT_EQ(fixture_status[0], SLRU_PAGE_EMPTY);
	memset(fixture_page.data, 0, BLCKSZ);
	fixture_number[0] = pageno;
	fixture_status[0] = SLRU_PAGE_VALID;
	fixture_dirty[0] = true;
	return 0;
}
int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok, TransactionId xid)
{
	UT_ASSERT(ctl == fixture_expected_ctl && lock_depth == 1 && write_ok);
	if (fixture_files && fixture_status[0] == SLRU_PAGE_EMPTY) {
		int fd = fixture_segment_open(ctl, pageno / SLRU_PAGES_PER_SEGMENT, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, fixture_page.data, BLCKSZ,
			(off_t)(pageno % SLRU_PAGES_PER_SEGMENT) * BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		fixture_status[0] = SLRU_PAGE_VALID;
		fixture_number[0] = pageno;
		physical_reads++;
	}
	if (fixture_status[0] == SLRU_PAGE_EMPTY && fixture_physical_exists) {
		fixture_page = fixture_disk;
		fixture_status[0] = SLRU_PAGE_VALID;
		fixture_number[0] = pageno;
	}
	UT_ASSERT_EQ(fixture_number[0], pageno);
	UT_ASSERT_EQ(xid, 1000);
	return 0;
}
void
SimpleLruWriteAll(SlruCtl ctl, bool allow_redirtied)
{
	UT_ASSERT(ctl == fixture_expected_ctl && lock_depth == 0 && allow_redirtied);
	writes++;
	if (fixture_files && fixture_dirty[0]) {
		int page = fixture_number[0];
		int fd = fixture_segment_open(ctl, page / SLRU_PAGES_PER_SEGMENT, O_RDWR | O_CREAT);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, fixture_page.data, BLCKSZ,
			(off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
	}
	fixture_disk = fixture_page;
	fixture_physical_exists = true;
	fixture_dirty[0] = false;
}
int
SlruSyncFileTag(SlruCtl ctl, const FileTag *tag, char *path)
{
	UT_ASSERT(ctl == fixture_expected_ctl && lock_depth == 0);
	UT_ASSERT_EQ(tag->segno, 524288); /* origin 1, xid 1000 */
	strcpy(path, "fixture-segment");
	syncs++;
	if (fixture_files && !sync_error) {
		int fd = fixture_segment_open(ctl, tag->segno, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(fsync(fd), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	return sync_error;
}
void
fsync_fname(const char *path, bool isdir)
{
	UT_ASSERT(isdir && lock_depth == 0);
	UT_ASSERT(strcmp(path, fixture_expected_ctl->Dir) == 0);
	dirsyncs++;
	if (fixture_files) {
		int fd = open(path, O_RDONLY | O_DIRECTORY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(fsync(fd), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
}
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data)
{
	/* Two origin-1 segments below cutoff, cutoff itself, foreign origin. */
	const int pages[] = { 16777216, 16777248, 16777280, 0 };
	scans++;
	for (unsigned i = 0; i < lengthof(pages); i++)
		(void)callback(ctl, "fixture", pages[i], data);
	return false;
}
void
SlruDeleteSegment(SlruCtl ctl, int segno)
{
	UT_ASSERT(ctl == fixture_expected_ctl && lock_depth == 0);
	if (deletes < lengthof(deleted_segments))
		deleted_segments[deletes] = segno;
	deletes++;
	fixture_physical_exists = false;
	if (fixture_files) {
		char path[MAXPGPATH];
		snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, segno);
		UT_ASSERT(unlink(path) == 0 || errno == ENOENT);
	}
}

#include "test_cluster_remote_xact_writer.inc"
#define remote_xact_directory_ready fixture_native_directory_ready
#include "test_cluster_remote_xact_directory.inc"
#undef remote_xact_directory_ready

static void
reset_fixture(void)
{
	memset(&fixture_page, 0, sizeof(fixture_page));
	memset(&fixture_shared, 0, sizeof(fixture_shared));
	memset(&ClusterRemoteXactCtlData, 0, sizeof(ClusterRemoteXactCtlData));
	fixture_buffers[0] = fixture_page.data;
	fixture_dirty[0] = false;
	fixture_status[0] = SLRU_PAGE_EMPTY;
	fixture_number[0] = -1;
	fixture_shared.ControlLock = &fixture_lock;
	fixture_shared.num_slots = 1;
	fixture_shared.page_buffer = fixture_buffers;
	fixture_shared.page_dirty = fixture_dirty;
	fixture_shared.page_status = fixture_status;
	fixture_shared.page_number = fixture_number;
	ClusterRemoteXactCtl->shared = &fixture_shared;
	strcpy(ClusterRemoteXactCtl->Dir, "pg_xact_remote_v2");
	fixture_expected_ctl = ClusterRemoteXactCtl;
	memset(ClusterRemoteXactOriginCtlData, 0, sizeof(ClusterRemoteXactOriginCtlData));
	ClusterRemoteXactOriginCtlData[1].shared = &fixture_shared;
	RemoteXactAccessLocks[1] = &fixture_access_lock;
	strcpy(ClusterRemoteXactOriginCtlData[1].Dir,
		"/shared/native_side/origin_1/pg_xact_remote_v2");
	RemoteXactShared = &fixture_stats;
	pg_atomic_init_u64(&fixture_stats.outcome_indoubt_count, 0);
	remote_xact_online_writer_depth_v = 0;
	IsUnderPostmaster = true;
	fixture_startup = false;
	cluster_shared_config = false;
	fixture_authorized_origin = -1;
	fixture_physical_exists = false;
	fixture_files = false;
	physical_reads = 0;
	directory_checks = 0;
	fixture_directory_ok = true;
	fixture_revoke_on_lock = false;
	access_lock_depth = 0;
	locks = writes = syncs = dirsyncs = scans = deletes = 0;
	lock_depth = sync_error = 0;
	expect_error = false;
}

static void
assert_no_effect(const PGAlignedBlock *before)
{
	UT_ASSERT(memcmp(before->data, fixture_page.data, BLCKSZ) == 0);
	UT_ASSERT(!fixture_dirty[0]);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(locks + writes + syncs + dirsyncs + scans + deletes, 0);
}

UT_TEST(ordinary_backend_cannot_prepare)
{
	const uint8 digest[28] = { 1 };
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_publish_terminal)
{
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_reset_or_truncate)
{
	PGAlignedBlock before;
	ClusterRemoteXactEntryV2 *entry;
	reset_fixture();
	fixture_status[0] = SLRU_PAGE_VALID;
	fixture_number[0] = 16777219; /* origin 1 + floor(1000/256) */
	entry = &((ClusterRemoteXactEntryV2 *)fixture_page.data)[232];
	entry->format_version = 2;
	entry->status = 2; /* an existing ABORTED projection */
	before = fixture_page;
	UT_ASSERT(!cluster_remote_xact_reset_range_v2(1, 1000, 1));
	assert_no_effect(&before);
	UT_ASSERT(!cluster_remote_xact_truncate_before_v2(1, 16384));
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_cannot_flush)
{
	PGAlignedBlock before;
	reset_fixture();
	before = fixture_page;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		cluster_remote_xact_flush();
		UT_ASSERT(false);
	}
	expect_error = false;
	assert_no_effect(&before);
}

static void
exercise_allowed_writer(void)
{
	const uint8 digest[28] = { 1 };
	ClusterRemoteXactEntryDecodedV2 decoded;
	ClusterRemoteXactEntryV2 *entry;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	if (ut_current_failed)
		return;
	UT_ASSERT(cluster_remote_xact_pending_matches_v2(1, 1000, digest));
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	entry = &((ClusterRemoteXactEntryV2 *)fixture_page.data)[232];
	UT_ASSERT(cluster_remote_xact_entry_decode_terminal_v2(entry, &decoded));
	UT_ASSERT_EQ(decoded.commit_scn, 123);
	UT_ASSERT_EQ(decoded.commit_timestamp, 456);
	UT_ASSERT_EQ(decoded.wrap, 2);
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT_EQ(dirsyncs, 2);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, true, digest, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED);
	UT_ASSERT_EQ(writes, 3); /* duplicate must still prove durability */
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_CONFLICT);
	UT_ASSERT_EQ(writes, 3);
	UT_ASSERT(cluster_remote_xact_reset_range_v2(1, 1000, 1));
	UT_ASSERT_EQ(entry->status, 0);
	UT_ASSERT_EQ(writes, 4);
	UT_ASSERT_EQ(syncs, 4);
	UT_ASSERT(cluster_remote_xact_truncate_before_v2(1, 16384));
	UT_ASSERT_EQ(deletes, 2);
	UT_ASSERT_EQ(deleted_segments[0], 524288);
	UT_ASSERT_EQ(deleted_segments[1], 524289);
	cluster_remote_xact_flush();
	UT_ASSERT_EQ(writes, 5);
	UT_ASSERT_EQ(lock_depth, 0);
}
UT_TEST(startup_writer_preserves_transitions)
{
	reset_fixture();
	fixture_startup = true;
	exercise_allowed_writer();
}
UT_TEST(standalone_writer_preserves_transitions)
{
	reset_fixture();
	IsUnderPostmaster = false;
	exercise_allowed_writer();
}
UT_TEST(online_writer_permission_ends_at_final_pop)
{
	const uint8 digest[28] = { 1 };
	PGAlignedBlock before;
	reset_fixture();
	cluster_remote_xact_online_writer_push();
	cluster_remote_xact_online_writer_push();
	cluster_remote_xact_online_writer_pop();
	UT_ASSERT_EQ(cluster_remote_xact_online_writer_depth(), 1);
	exercise_allowed_writer();
	cluster_remote_xact_online_writer_pop();
	UT_ASSERT_EQ(cluster_remote_xact_online_writer_depth(), 0);
	before = fixture_page;
	locks = writes = syncs = dirsyncs = scans = deletes = 0;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
}
UT_TEST(ordinary_backend_retains_read_only_queries)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	fixture_startup = true;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	fixture_startup = false;
	writes = syncs = dirsyncs = 0;
	UT_ASSERT(cluster_remote_xact_pending_matches_v2(1, 1000, digest));
	UT_ASSERT(!cluster_remote_xact_range_empty_v2(1, 1000, 1));
	UT_ASSERT_EQ(writes + syncs + dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}
UT_TEST(durability_error_is_not_reported_as_stored)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	fixture_startup = true;
	sync_error = -1;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		(void)cluster_remote_xact_store_prepared_v2(1, 1000, digest);
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(shared_projection_writer_requires_original_origin_authority)
{
	const uint8 digest[28] = { 1 };
	PGAlignedBlock before;
	reset_fixture();
	cluster_shared_config = true;
	fixture_startup = true;
	cluster_remote_xact_online_writer_push();
	before = fixture_page;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
		CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	assert_no_effect(&before);
	fixture_authorized_origin = 2;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
		1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2),
		CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT(!cluster_remote_xact_reset_range_v2(1, 1000, 1));
	UT_ASSERT(!cluster_remote_xact_truncate_before_v2(1, 16384));
	assert_no_effect(&before);
	cluster_remote_xact_online_writer_pop();
}

UT_TEST(projection_durability_uses_selected_slru_directory)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	fixture_startup = true;
	strcpy(ClusterRemoteXactCtl->Dir, "/shared/native_side/origin_1/pg_xact_remote_v2");
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
		CLUSTER_REMOTE_XACT_MUTATION_STORED);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(dirsyncs, 1);
}

UT_TEST(shared_projection_routes_to_original_origin_control)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	cluster_shared_config = true;
	fixture_startup = true;
	fixture_authorized_origin = 1;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
		CLUSTER_REMOTE_XACT_MUTATION_STORED);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(dirsyncs, 1);
}

UT_TEST(shared_projection_never_reuses_a_prior_recoverer_cache)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	cluster_shared_config = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	fixture_number[0] = cluster_remote_xact_pageno(1, 1000);
	fixture_status[0] = SLRU_PAGE_VALID;
	UT_ASSERT(cluster_remote_xact_entry_encode_pending_v2(
		&((ClusterRemoteXactEntryV2 *)fixture_page.data)[232], digest));
	UT_ASSERT(!cluster_remote_xact_pending_matches_v2(1, 1000, digest));
	UT_ASSERT_EQ(writes + syncs + dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
}

UT_TEST(shared_projection_sync_error_clears_cache_and_access_lock)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	cluster_shared_config = true;
	fixture_startup = true;
	fixture_authorized_origin = 1;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	sync_error = -1;
	expect_error = true;
	if (setjmp(error_boundary) == 0) {
		(void)cluster_remote_xact_store_prepared_v2(1, 1000, digest);
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
	UT_ASSERT_EQ(fixture_status[0], SLRU_PAGE_EMPTY);
	UT_ASSERT(!fixture_dirty[0]);
	sync_error = 0;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
		CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED);
}

UT_TEST(shared_invalid_arguments_and_revoked_owner_have_no_directory_effect)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	cluster_shared_config = fixture_startup = true;
	fixture_authorized_origin = 1;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, NULL),
		CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
		1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, InvalidScn, 456, true, 2),
		CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT(!cluster_remote_xact_reset_range_v2(1, 1000, 0));
	UT_ASSERT(!cluster_remote_xact_truncate_before_v2(1, InvalidTransactionId));
	UT_ASSERT_EQ(directory_checks, 0);
	fixture_revoke_on_lock = true;
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
		CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(directory_checks, 0);
	UT_ASSERT_EQ(writes + syncs + dirsyncs, 0);
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
}

UT_TEST(shared_origin_projection_survives_writer_exit_and_local_directory_change)
{
	char root[] = "/tmp/pre2-projection-writer-XXXXXX";
	char native[MAXPGPATH], origin[MAXPGPATH], recoverer[MAXPGPATH];
	int cwd_fd = open(".", O_RDONLY | O_DIRECTORY);
	pid_t writer;
	int status;
	SCN scn;
	uint16 wrap;
	bool wrap_valid;
	TimestampTz timestamp;

	reset_fixture();
	UT_ASSERT(cwd_fd >= 0 && mkdtemp(root) != NULL);
	snprintf(native, sizeof(native), "%s/native_side", root);
	snprintf(origin, sizeof(origin), "%s/origin_1", native);
	snprintf(recoverer, sizeof(recoverer), "%s/other-recoverer", root);
	UT_ASSERT_EQ(mkdir(native, 0700), 0);
	UT_ASSERT_EQ(mkdir(origin, 0700), 0);
	UT_ASSERT_EQ(mkdir(recoverer, 0700), 0);
	fixture_files = cluster_shared_config = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	cluster_shared_data_dir = root;
	snprintf(fixture_expected_ctl->Dir, sizeof(fixture_expected_ctl->Dir),
		"%s/pg_xact_remote_v2", origin);
	fflush(NULL);
	writer = fork();
	if (writer == 0) {
		ClusterRemoteXactMutationV2 result;
		fixture_startup = true;
		fixture_authorized_origin = 1;
		result = cluster_remote_xact_store_terminal_v2(
			1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 123, 456, true, 2);
		_exit(result == CLUSTER_REMOTE_XACT_MUTATION_STORED && !ut_current_failed ? 0 : 1);
	}
	UT_ASSERT(writer > 0);
	UT_ASSERT_EQ(waitpid(writer, &status, 0), writer);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	/* The writer has exited; neither its process cache nor cwd is needed. */
	UT_ASSERT_EQ(chdir(recoverer), 0);
	fixture_status[0] = SLRU_PAGE_VALID;
	fixture_number[0] = cluster_remote_xact_pageno(1, 1000);
	memset(fixture_page.data, 0xAA, BLCKSZ);
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, &wrap, &wrap_valid),
		CLUSTER_REMOTE_XACT_COMMITTED);
	UT_ASSERT_EQ(scn, 123);
	UT_ASSERT(wrap_valid && wrap == 2);
	UT_ASSERT(cluster_remote_commit_timestamp(1, 1000, &timestamp));
	UT_ASSERT_EQ(timestamp, 456);
	UT_ASSERT_EQ(physical_reads, 2);
	UT_ASSERT_EQ(writes + syncs + dirsyncs, 0);
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(2, 1000, &scn, &wrap, &wrap_valid),
		CLUSTER_REMOTE_XACT_INDOUBT);
	UT_ASSERT_EQ(scn, InvalidScn);
	fixture_startup = true;
	fixture_authorized_origin = 1;
	UT_ASSERT(cluster_remote_xact_reset_range_v2(1, 1000, 1));
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, NULL, NULL),
		CLUSTER_REMOTE_XACT_INDOUBT);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
		1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 124, 457, true, 3),
		CLUSTER_REMOTE_XACT_MUTATION_STORED);
	UT_ASSERT(cluster_remote_xact_truncate_before_v2(1, 16384));
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, NULL, NULL),
		CLUSTER_REMOTE_XACT_INDOUBT);
	cluster_remote_xact_flush();
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
	UT_ASSERT_EQ(fchdir(cwd_fd), 0);
	UT_ASSERT_EQ(close(cwd_fd), 0);
	UT_ASSERT_EQ(rmdir(fixture_expected_ctl->Dir), 0);
	UT_ASSERT_EQ(rmdir(recoverer), 0);
	UT_ASSERT_EQ(rmdir(origin), 0);
	UT_ASSERT_EQ(rmdir(native), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(ordinary_backend_cannot_prepare);
	UT_RUN(ordinary_backend_cannot_publish_terminal);
	UT_RUN(ordinary_backend_cannot_reset_or_truncate);
	UT_RUN(ordinary_backend_cannot_flush);
	UT_RUN(startup_writer_preserves_transitions);
	UT_RUN(standalone_writer_preserves_transitions);
	UT_RUN(online_writer_permission_ends_at_final_pop);
	UT_RUN(ordinary_backend_retains_read_only_queries);
	UT_RUN(durability_error_is_not_reported_as_stored);
	UT_RUN(shared_projection_writer_requires_original_origin_authority);
	UT_RUN(projection_durability_uses_selected_slru_directory);
	UT_RUN(shared_projection_routes_to_original_origin_control);
	UT_RUN(shared_projection_never_reuses_a_prior_recoverer_cache);
	UT_RUN(shared_projection_sync_error_clears_cache_and_access_lock);
	UT_RUN(shared_invalid_arguments_and_revoked_owner_have_no_directory_effect);
	UT_RUN(shared_origin_projection_survives_writer_exit_and_local_directory_change);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
