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
#include "utils/wait_event.h"
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
static bool fixture_c0_projection;

bool
cluster_undo_recovery_origin_authorized_v1(int origin)
{
	return origin == fixture_authorized_origin;
}

bool
cluster_undo_recovery_abort_projection_authorized_v1(int origin, TransactionId xid)
{
	return fixture_c0_projection && origin == 1 && xid == 1000;
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
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
enum SyncFault { SYNC_OK, SYNC_OPEN, SYNC_FSYNC, SYNC_CLOSE, SYNC_ERROR };
static enum SyncFault sync_fault;
static bool fault_directory, sync_fd_directory;
static int fault_errno, sync_fd = -1, transient_fds;
static unsigned fault_hits;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool fixture_native_directory_ready(SlruCtl ctl, int origin, bool write,
										   TransactionId abort_xid);

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

int
pg_fsync(int fd)
{
	if (fd != sync_fd)
		return fsync(fd); /* Original directory qualification, before write. */
	if (sync_fd_directory)
		dirsyncs++;
	else
		syncs++;
	if (sync_fd_directory == fault_directory
		&& (sync_fault == SYNC_FSYNC || sync_fault == SYNC_ERROR)) {
		fault_hits++;
		errno = fault_errno;
		if (sync_fault == SYNC_ERROR)
			pg_re_throw();
		return -1;
	}
	if (sync_error) {
		errno = EIO;
		return -1;
	}
	return fixture_files ? fsync(fd) : 0;
}

int
OpenTransientFile(const char *path, int flags)
{
	bool directory = strcmp(path, fixture_expected_ctl->Dir) == 0;
	UT_ASSERT_EQ(sync_fd, -1);
	if (sync_fault == SYNC_OPEN && directory == fault_directory) {
		fault_hits++;
		errno = fault_errno;
		return -1;
	}
	sync_fd = fixture_files ? open(path, flags, 0600) : 10000;
	if (sync_fd >= 0) {
		sync_fd_directory = directory;
		transient_fds++;
	}
	return sync_fd;
}

int
CloseTransientFile(int fd)
{
	bool directory = sync_fd_directory;
	int result;
	UT_ASSERT_EQ(fd, sync_fd);
	result = fixture_files ? close(fd) : 0;
	sync_fd = -1;
	transient_fds--;
	if (sync_fault == SYNC_CLOSE && directory == fault_directory) {
		fault_hits++;
		errno = fault_errno;
		return -1;
	}
	return result;
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
remote_xact_directory_ready(SlruCtl ctl, int origin, bool write, TransactionId abort_xid)
{
	directory_checks++;
	UT_ASSERT(ctl == fixture_expected_ctl && origin == 1);
	UT_ASSERT_EQ(access_lock_depth, 1);
	(void)write;
	if (fixture_files)
		return fixture_native_directory_ready(ctl, origin, write, abort_xid);
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

/* Execute the original permissive helpers in real-file cases. Faults live
 * below these bodies, at open/fsync/close, rather than replacing their result. */
#define SlruFileName(ctl, path, seg) snprintf(path, MAXPGPATH, "%s/%04X", (ctl)->Dir, seg)
#define SlruSyncFileTag fixture_native_slru_sync
#define fsync_fname_ext fixture_native_fsync_fname_ext
#include "test_cluster_remote_xact_sync.inc"
#undef fsync_fname_ext
#undef SlruSyncFileTag
#undef SlruFileName

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == &fixture_access_lock) {
		UT_ASSERT(mode == LW_EXCLUSIVE && access_lock_depth == 0 && lock_depth == 0);
		access_lock_depth++;
		if (fixture_revoke_on_lock) {
			fixture_authorized_origin = -1;
			fixture_c0_projection = false;
		}
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
		UT_ASSERT_EQ(
			pread(fd, fixture_page.data, BLCKSZ, (off_t)(pageno % SLRU_PAGES_PER_SEGMENT) * BLCKSZ),
			BLCKSZ);
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
		UT_ASSERT_EQ(
			pwrite(fd, fixture_page.data, BLCKSZ, (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ),
			BLCKSZ);
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
	if (fixture_files)
		return fixture_native_slru_sync(ctl, tag, path);
	strcpy(path, "fixture-segment");
	syncs++;
	return sync_error;
}
void
fsync_fname(const char *path, bool isdir)
{
	UT_ASSERT(isdir && lock_depth == 0);
	UT_ASSERT(strcmp(path, fixture_expected_ctl->Dir) == 0);
	if (fixture_files)
		(void)fixture_native_fsync_fname_ext(path, isdir, false, ERROR);
	else
		dirsyncs++;
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
	strcpy(ClusterRemoteXactOriginCtlData[1].Dir, "/shared/native_side/origin_1/pg_xact_remote_v2");
	RemoteXactShared = &fixture_stats;
	pg_atomic_init_u64(&fixture_stats.outcome_indoubt_count, 0);
	remote_xact_online_writer_depth_v = 0;
	IsUnderPostmaster = true;
	fixture_startup = false;
	cluster_shared_config = false;
	fixture_authorized_origin = -1;
	fixture_c0_projection = false;
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
	sync_fault = SYNC_OK;
	fault_directory = false;
	fault_errno = EIO;
	sync_fd = -1;
	transient_fds = 0;
	fault_hits = wait_event = 0;
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
	snprintf(fixture_expected_ctl->Dir, sizeof(fixture_expected_ctl->Dir), "%s/pg_xact_remote_v2",
			 origin);
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

UT_TEST(c0_abort_permission_is_exact_and_does_not_grant_an_origin)
{
	const uint8 digest[28] = { 1 };
	reset_fixture();
	cluster_shared_config = fixture_startup = fixture_c0_projection = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1001, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 2, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 12, 34, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, true, digest, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, digest, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(cluster_remote_xact_store_prepared_v2(1, 1000, digest),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT(!cluster_remote_xact_reset_range_v2(1, 1000, 1));
	UT_ASSERT(!cluster_remote_xact_truncate_before_v2(1, 16384));
	UT_ASSERT_EQ(writes + syncs + dirsyncs + directory_checks + deletes, 0);
	fixture_startup = false;
	cluster_remote_xact_online_writer_push();
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	cluster_remote_xact_online_writer_pop();
	fixture_startup = true;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(dirsyncs, 1);
	fixture_c0_projection = false;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
}

UT_TEST(c0_projection_retains_conflict_revocation_and_error_cleanup)
{
	reset_fixture();
	cluster_shared_config = fixture_startup = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	fixture_authorized_origin = 1;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED, 12, 34, true, 2),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	fixture_authorized_origin = -1;
	fixture_c0_projection = true;
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_CONFLICT);
	UT_ASSERT_EQ(writes, 1);
	for (int fault = 0; fault < 2; fault++) {
		reset_fixture();
		cluster_shared_config = fixture_startup = fixture_c0_projection = true;
		fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
		fixture_revoke_on_lock = fault == 0;
		sync_error = fault == 1 ? -1 : 0;
		expect_error = true;
		if (setjmp(error_boundary) == 0) {
			UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(1, 1000, false, NULL,
															   CLUSTER_REMOTE_XACT_ABORTED,
															   InvalidScn, 0, false, 0),
						 CLUSTER_REMOTE_XACT_MUTATION_INVALID);
			UT_ASSERT_EQ(fault, 0);
		}
		expect_error = false;
		UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
		UT_ASSERT(!fixture_dirty[0]);
		UT_ASSERT_EQ(fixture_status[0], SLRU_PAGE_EMPTY);
		if (fault == 0)
			UT_ASSERT_EQ(writes + directory_checks, 0);
	}
}

UT_TEST(c0_projection_reads_back_original_target_files_without_cached_success)
{
	char root[] = "/tmp/pre2-c0-projection-XXXXXX";
	char native[MAXPGPATH], origin[MAXPGPATH], segment[MAXPGPATH];
	SCN scn;
	uint16 wrap;
	bool wrap_valid;
	reset_fixture();
	UT_ASSERT(mkdtemp(root) != NULL);
	snprintf(native, sizeof(native), "%s/native_side", root);
	snprintf(origin, sizeof(origin), "%s/origin_1", native);
	UT_ASSERT_EQ(mkdir(native, 0700), 0);
	UT_ASSERT_EQ(mkdir(origin, 0700), 0);
	fixture_files = cluster_shared_config = fixture_startup = fixture_c0_projection = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	cluster_shared_data_dir = root;
	snprintf(fixture_expected_ctl->Dir, sizeof(fixture_expected_ctl->Dir), "%s/pg_xact_remote_v2",
			 origin);
	UT_ASSERT_EQ(cluster_remote_xact_store_terminal_v2(
					 1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0),
				 CLUSTER_REMOTE_XACT_MUTATION_STORED);
	fixture_c0_projection = false;
	memset(fixture_page.data, 0xAA, BLCKSZ);
	fixture_number[0] = cluster_remote_xact_pageno(1, 1000);
	fixture_status[0] = SLRU_PAGE_VALID;
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, &wrap, &wrap_valid),
				 CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT(scn == InvalidScn && !wrap_valid && wrap == 0);
	UT_ASSERT_EQ(physical_reads, 1);
	snprintf(segment, sizeof(segment), "%s/%04X", fixture_expected_ctl->Dir,
			 cluster_remote_xact_pageno(1, 1000) / SLRU_PAGES_PER_SEGMENT);
	UT_ASSERT_EQ(unlink(segment), 0);
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, &wrap, &wrap_valid),
				 CLUSTER_REMOTE_XACT_INDOUBT);
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
	UT_ASSERT_EQ(rmdir(fixture_expected_ctl->Dir), 0);
	UT_ASSERT_EQ(rmdir(origin), 0);
	UT_ASSERT_EQ(rmdir(native), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
}

static void
projection_sync_fixture(char *root)
{
	char native[MAXPGPATH], origin[MAXPGPATH];
	reset_fixture();
	strlcpy(root, "/tmp/pre2-c0-sync-XXXXXX", MAXPGPATH);
	UT_ASSERT(mkdtemp(root) != NULL);
	snprintf(native, sizeof(native), "%s/native_side", root);
	snprintf(origin, sizeof(origin), "%s/origin_1", native);
	UT_ASSERT_EQ(mkdir(native, 0700), 0);
	UT_ASSERT_EQ(mkdir(origin, 0700), 0);
	fixture_files = cluster_shared_config = fixture_startup = fixture_c0_projection = true;
	fixture_expected_ctl = &ClusterRemoteXactOriginCtlData[1];
	cluster_shared_data_dir = root;
	snprintf(fixture_expected_ctl->Dir, sizeof(fixture_expected_ctl->Dir), "%s/pg_xact_remote_v2",
			 origin);
}

static ClusterRemoteXactMutationV2
projection_abort(void)
{
	return cluster_remote_xact_store_terminal_v2(1, 1000, false, NULL, CLUSTER_REMOTE_XACT_ABORTED,
												 InvalidScn, 0, false, 0);
}

static void
projection_sync_case(enum SyncFault fault, bool directory, int error, bool already)
{
	char root[MAXPGPATH], path[MAXPGPATH];
	volatile bool caught = false;
	volatile ClusterRemoteXactMutationV2 result = CLUSTER_REMOTE_XACT_MUTATION_INVALID;
	SCN scn;
	projection_sync_fixture(root);
	if (already)
		UT_ASSERT_EQ(projection_abort(), CLUSTER_REMOTE_XACT_MUTATION_STORED);
	sync_fault = fault;
	fault_directory = directory;
	fault_errno = error;
	expect_error = true;
	if (setjmp(error_boundary) == 0)
		result = projection_abort();
	else
		caught = true;
	expect_error = false;
	UT_ASSERT_EQ(fault_hits, 1);
	UT_ASSERT(caught);
	UT_ASSERT(result != CLUSTER_REMOTE_XACT_MUTATION_STORED
			  && result != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED);
	UT_ASSERT_EQ(lock_depth + access_lock_depth, 0);
	UT_ASSERT_EQ(fixture_status[0], SLRU_PAGE_EMPTY);
	UT_ASSERT(!fixture_dirty[0]);
	UT_ASSERT_EQ(transient_fds, 0);
	UT_ASSERT_EQ(wait_event, 0);

	/* Test cleanup also permits inspecting the pre-fix leaked-FD negative. */
	sync_fault = SYNC_OK;
	if (sync_fd >= 0)
		UT_ASSERT_EQ(CloseTransientFile(sync_fd), 0);
	wait_event = 0;
	/* The failed call wrote bytes, but only this full retry may succeed. */
	UT_ASSERT_EQ(projection_abort(), CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED);
	fixture_c0_projection = false;
	UT_ASSERT_EQ(cluster_remote_commit_outcome_ex(1, 1000, &scn, NULL, NULL),
				 CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT_EQ(scn, InvalidScn);
	UT_ASSERT_EQ(transient_fds + lock_depth + access_lock_depth, 0);
	snprintf(path, sizeof(path), "%s/80000", fixture_expected_ctl->Dir);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rmdir(fixture_expected_ctl->Dir), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_1", root);
	UT_ASSERT_EQ(rmdir(path), 0);
	snprintf(path, sizeof(path), "%s/native_side", root);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
}

UT_TEST(c0_projection_rejects_segment_and_directory_sync_failures)
{
	static const struct {
		enum SyncFault fault;
		bool directory;
		int error;
	} cases[]
		= { { SYNC_OPEN, false, EACCES }, { SYNC_FSYNC, false, EIO },	{ SYNC_CLOSE, false, EIO },
			{ SYNC_OPEN, true, EACCES },  { SYNC_OPEN, true, EISDIR },	{ SYNC_OPEN, true, EIO },
			{ SYNC_FSYNC, true, EBADF },  { SYNC_FSYNC, true, EINVAL }, { SYNC_FSYNC, true, EIO },
			{ SYNC_CLOSE, true, EIO } };
	for (unsigned i = 0; i < lengthof(cases); i++)
		for (int already = 0; already < 2; already++)
			projection_sync_case(cases[i].fault, cases[i].directory, cases[i].error, already);
}

UT_TEST(c0_projection_sync_error_closes_descriptor_and_wait)
{
	for (int directory = 0; directory < 2; directory++)
		for (int already = 0; already < 2; already++)
			projection_sync_case(SYNC_ERROR, directory, EIO, already);
}

int
main(void)
{
	UT_PLAN(21);
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
	UT_RUN(c0_abort_permission_is_exact_and_does_not_grant_an_origin);
	UT_RUN(c0_projection_retains_conflict_revocation_and_error_cleanup);
	UT_RUN(c0_projection_reads_back_original_target_files_without_cached_success);
	UT_RUN(c0_projection_rejects_segment_and_directory_sync_failures);
	UT_RUN(c0_projection_sync_error_closes_descriptor_and_wait);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
