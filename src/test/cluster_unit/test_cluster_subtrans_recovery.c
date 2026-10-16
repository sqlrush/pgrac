/*-------------------------------------------------------------------------
 * test_cluster_subtrans_recovery.c
 *    Original-origin assignment recovery through native SUBTRANS and SLRU file I/O.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_recovery.c
 * NOTES
 *    PGRAC test; only shmem, locks, original authority and fault delivery are
 *    fixtures. Native SLRU lookup/read/write and strict file/directory sync execute.
 *    This is not a cluster fencing or native startup qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <sys/wait.h>
#include <unistd.h>
#include "storage/shmem.h"
#include "miscadmin.h"
#include "../../backend/access/transam/subtrans.c"
#include "../../backend/access/transam/slru.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();


bool cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_shared_data_dir;
bool InRecovery = true;
bool IsUnderPostmaster;
BackendType MyBackendType;
AuxProcType MyAuxProcType = StartupProcess;
volatile uint32 CritSectionCount;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
int pg_file_create_mode = 0600;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
CheckpointStatsData CheckpointStats;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static char fixture_root[MAXPGPATH];
static bool authority = true, revoke_on_lock, expect_error, fail_open, fail_sync, fail_dirsync;
static bool revoke_after_read, revoke_after_sync;
static bool fail_read;
static bool source_durable = true;
static bool fail_sync_close;
static int directory_sync_error, synced_fd = -1;
static int transient_fds;
static unsigned writes, syncs, dirsyncs, reads;
static int change_after_read;
static void fixture_path(int page, char *path);
static TransactionId fixture_status(TransactionId xid);
static struct {
	LWLock *lock;
	bool held;
} fixture_locks[6000];
static unsigned fixture_lock_count;
static void *allocations[300];
static unsigned allocation_count;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	if (expect_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	abort();
}
void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
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
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
data_sync_elevel(int level)
{
	return level;
}
void
ProcessInterrupts(void)
{
	abort();
}

/* PGRAC: native recovery is not the checkpointer queue consumer. */
void
AbsorbSyncRequests(void)
{
	abort();
}

bool
cluster_undo_recovery_cold_origin_authorized_v1(int origin)
{
	return authority && origin == 1;
}
bool
cluster_undo_recovery_cold_source_synced_v1(int origin)
{
	return source_durable && authority && origin == 1;
}
bool
TransactionIdPrecedes(TransactionId a, TransactionId b)
{
	return (int32)(a - b) < 0;
}
bool
TransactionIdFollowsOrEquals(TransactionId a, TransactionId b)
{
	return (int32)(a - b) >= 0;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	void *p = calloc(1, size);
	if (p == NULL || allocation_count >= lengthof(allocations))
		abort();
	allocations[allocation_count++] = p;
	*found = false;
	return p;
}
void
LWLockInitialize(LWLock *lock, int tranche pg_attribute_unused())
{
	if (fixture_lock_count >= lengthof(fixture_locks))
		abort();
	fixture_locks[fixture_lock_count++].lock = lock;
}
static unsigned
fixture_lock_index(LWLock *lock)
{
	for (unsigned i = 0; i < fixture_lock_count; i++)
		if (fixture_locks[i].lock == lock)
			return i;
	abort();
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(!fixture_locks[i].held);
	fixture_locks[i].held = true;
	if (revoke_on_lock && lock == &SubtransRecoveryLocks[3].lock)
		authority = false;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(fixture_locks[i].held);
	fixture_locks[i].held = false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return fixture_locks[fixture_lock_index(lock)].held;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return LWLockAcquire(lock, mode);
}

int
pgstat_get_slru_index(const char *name pg_attribute_unused())
{
	return 0;
}
void
pgstat_count_slru_page_zeroed(int i pg_attribute_unused())
{}
void
pgstat_count_slru_page_hit(int i pg_attribute_unused())
{}
void
pgstat_count_slru_page_read(int i pg_attribute_unused())
{
	reads++;
	if (revoke_after_read)
		authority = false;
	if (change_after_read) {
		char path[MAXPGPATH], replacement[MAXPGPATH];
		int mutation = change_after_read;
		change_after_read = 0;
		fixture_path(0, path);
		if (mutation == 1)
			UT_ASSERT_EQ(truncate(path, BLCKSZ + 1), 0);
		else if (mutation == 2) {
			PGAlignedBlock bytes = { 0 };
			int fd;
			snprintf(replacement, sizeof(replacement), "%s.replacement", path);
			fd = open(replacement, O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0);
			if (fd >= 0) {
				UT_ASSERT_EQ(write(fd, bytes.data, BLCKSZ), BLCKSZ);
				UT_ASSERT_EQ(close(fd), 0);
				UT_ASSERT_EQ(rename(replacement, path), 0);
			}
		} else
			UT_ASSERT_EQ(unlink(path), 0);
	}
}
void
pgstat_count_slru_page_written(int i pg_attribute_unused())
{
	writes++;
}
void
pgstat_count_slru_flush(int i pg_attribute_unused())
{}
void
pgstat_count_slru_truncate(int i pg_attribute_unused())
{}
void
XLogFlush(XLogRecPtr lsn pg_attribute_unused())
{
	abort();
}
bool
RegisterSyncRequest(const FileTag *tag pg_attribute_unused(),
					SyncRequestType type pg_attribute_unused(), bool retry pg_attribute_unused())
{
	abort();
}

int
OpenTransientFile(const char *path, int flags)
{
	int fd;
	if (directory_sync_error == EACCES && (flags & O_DIRECTORY)) {
		errno = EACCES;
		return -1;
	}
	if ((fail_open && (flags & O_RDWR)) || (fail_read && !(flags & O_RDWR))) {
		errno = EIO;
		return -1;
	}
	fd = open(path, flags, pg_file_create_mode);
	if (fd >= 0)
		transient_fds++;
	return fd;
}
int
CloseTransientFile(int fd)
{
	bool fail = fail_sync_close && fd == synced_fd;
	int rc;
	transient_fds--;
	synced_fd = -1;
	rc = close(fd);
	if (fail) {
		errno = EIO;
		return -1;
	}
	return rc;
}
int
pg_fsync(int fd)
{
	struct stat st;
	syncs++;
	synced_fd = fd;
	if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
		dirsyncs++;
		if (directory_sync_error || fail_dirsync) {
			errno = directory_sync_error ? directory_sync_error : EIO;
			return -1;
		}
	}
	if (fail_sync) {
		errno = EIO;
		return -1;
	}
	if (revoke_after_sync)
		authority = false;
	return fsync(fd);
}
void
fsync_fname(const char *path, bool isdir)
{
	int fd, rc;
	UT_ASSERT(isdir);
	dirsyncs++;
	/* Match the existing generic helper's documented ignored directory errors;
	 * the new strict adapter must use checked syscalls instead. */
	if (directory_sync_error)
		return;
	if (fail_dirsync) {
		errno = EIO;
		(void)errstart(ERROR, NULL);
	}
	fd = open(path, O_RDONLY | O_DIRECTORY);
	if (fd < 0)
		abort();
	rc = fsync(fd);
	if (close(fd) != 0)
		rc = -1;
	UT_ASSERT_EQ(rc, 0);
}


DIR *
AllocateDir(const char *path)
{
	DIR *dir = opendir(path);
	if (dir == NULL)
		(void)errstart(ERROR, NULL);
	return dir;
}
struct dirent *
ReadDir(DIR *dir, const char *path pg_attribute_unused())
{
	struct dirent *entry;
	errno = 0;
	entry = readdir(dir);
	if (entry == NULL && errno != 0)
		(void)errstart(ERROR, NULL);
	return entry;
}
int
FreeDir(DIR *dir)
{
	return closedir(dir);
}

static void
fixture_idle(void)
{
	UT_ASSERT_EQ(transient_fds, 0);
	for (unsigned i = 0; i < fixture_lock_count; i++)
		UT_ASSERT(!fixture_locks[i].held);
	if (SubtransRecoveryCtl[1].shared != NULL) {
		SlruShared s = SubtransRecoveryCtl[1].shared;
		for (int i = 0; i < s->num_slots; i++) {
			UT_ASSERT_EQ(s->page_status[i], SLRU_PAGE_EMPTY);
			UT_ASSERT(!s->page_dirty[i]);
		}
	}
}

static void
fixture_path(int page, char *path)
{
	snprintf(path, MAXPGPATH, "%s/native_side/origin_1/pg_subtrans/%04X", fixture_root,
			 page / SLRU_PAGES_PER_SEGMENT);
}

static void
fixture_page(int page, const char *bytes)
{
	char path[MAXPGPATH];
	int fd;
	fixture_path(page, path);
	fd = open(path, O_RDWR | O_CREAT, 0600);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pwrite(fd, bytes, BLCKSZ, (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ),
					 BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

static void
fixture_zero(int page)
{
	PGAlignedBlock bytes = { 0 };
	fixture_page(page, bytes.data);
}

static TransactionId
fixture_status(TransactionId xid)
{
	char path[MAXPGPATH];
	TransactionId value = 0;
	int page = TransactionIdToPage(xid), fd;
	fixture_path(page, path);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pread(fd, &value, sizeof(value),
						   (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ
							   + TransactionIdToEntry(xid) * sizeof(value)),
					 sizeof(value));
		UT_ASSERT_EQ(close(fd), 0);
	}
	return value;
}

static void
fixture_parent(TransactionId xid, TransactionId parent)
{
	char path[MAXPGPATH];
	int page = TransactionIdToPage(xid), fd;
	fixture_path(page, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pwrite(fd, &parent, sizeof(parent),
							(off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ
								+ TransactionIdToEntry(xid) * sizeof(parent)),
					 sizeof(parent));
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(original_origin_controls_are_separate)
{
	UT_ASSERT(SubtransRecoveryCtl[1].shared != NULL);
	UT_ASSERT(SubtransRecoveryCtl[1].shared != SubTransCtl->shared);
	UT_ASSERT(SubtransRecoveryCtl[1].shared != SubtransRecoveryCtl[2].shared);
	UT_ASSERT(strstr(SubtransRecoveryCtl[1].Dir, "/native_side/origin_1/pg_subtrans"));
	fixture_idle();
}

UT_TEST(assignment_persists_all_children_and_repeats_without_terminal_changes)
{
	TransactionId children[]
		= { 101, SUBTRANS_XACTS_PER_PAGE + 17, 32 * SUBTRANS_XACTS_PER_PAGE + 19 };
	unsigned before = writes;
	fixture_zero(0);
	fixture_zero(1);
	fixture_zero(32);
	fixture_parent(99, 90);
	UT_ASSERT(cluster_subtrans_native_recovery_preflight_v1(1, 100, 3, children));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT(!cluster_subtrans_native_recovery_verify_v1(1, 100, 3, children));
	UT_ASSERT(cluster_subtrans_native_recovery_apply_v1(1, 100, 3, children));
	UT_ASSERT(cluster_subtrans_native_recovery_verify_v1(1, 100, 3, children));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(fixture_status(children[i]), 100);
	UT_ASSERT_EQ(fixture_status(99), 90);
	UT_ASSERT_EQ(fixture_status(100), 0);
	UT_ASSERT(cluster_subtrans_native_recovery_apply_v1(1, 100, 3, children));
	UT_ASSERT(syncs > 0 && dirsyncs > 0);
	fixture_idle();
}

UT_TEST(later_conflict_refuses_the_entire_assignment_before_write)
{
	TransactionId children[] = { 101, SUBTRANS_XACTS_PER_PAGE + 17 };
	unsigned before = writes;
	fixture_zero(0);
	fixture_zero(1);
	fixture_parent(children[1], 99);
	UT_ASSERT(!cluster_subtrans_native_recovery_preflight_v1(1, 100, 2, children));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 2, children));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT_EQ(fixture_status(101), 0);
	UT_ASSERT_EQ(fixture_status(children[1]), 99);
	fixture_idle();
}

UT_TEST(missing_short_and_alias_are_not_zero_bases)
{
	TransactionId child = 64 * SUBTRANS_XACTS_PER_PAGE + 1;
	char path[MAXPGPATH], target[MAXPGPATH];
	unsigned before = writes;
	fixture_path(64, path);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	fixture_zero(64);
	UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	UT_ASSERT_EQ(unlink(path), 0);
	fixture_zero(0);
	fixture_path(0, target);
	UT_ASSERT_EQ(symlink(target, path), 0);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(link(target, path), 0);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(scope_role_and_origin_loss_refuse)
{
	TransactionId child = 101;
	unsigned before = writes;
	fixture_zero(0);
	for (int fault = 0; fault < 8; fault++) {
		int origin = 1;
		if (fault == 0)
			cluster_shared_config = false;
		if (fault == 1)
			InRecovery = false;
		if (fault == 2)
			MyAuxProcType = BgWriterProcess;
		if (fault == 3)
			origin = 0;
		if (fault == 4)
			origin = 128;
		if (fault == 5)
			authority = false;
		if (fault == 6)
			revoke_on_lock = true;
		if (fault == 7)
			source_durable = false;
		UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(origin, 100, 1, &child));
		cluster_shared_config = InRecovery = authority = true;
		MyAuxProcType = StartupProcess;
		revoke_on_lock = false;
		source_durable = true;
	}
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(bad_or_unordered_assignment_never_writes)
{
	TransactionId children[] = { 101, 102 };
	unsigned before = writes;
	fixture_zero(0);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 2, 2, children));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, -1, NULL));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, NULL));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, PGPROC_MAX_CACHED_SUBXIDS + 1,
														 children));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 0, NULL));
	children[1] = 101;
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 2, children));
	children[1] = 99;
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 2, children));
	children[1] = 100 + UINT32_C(0x80000000);
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 2, children));
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(io_errors_release_locks_and_discard_private_dirty_cache)
{
	TransactionId child = 101;
	for (int fault = 0; fault < 4; fault++) {
		volatile bool caught = false;
		fixture_zero(0);
		fail_open = fault == 0;
		fail_read = fault == 1;
		fail_sync = fault == 2;
		fail_dirsync = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			bool ok = cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child);
			UT_ASSERT(!ok);
			caught = !ok;
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		fail_open = fail_read = fail_sync = fail_dirsync = expect_error = false;
		fixture_idle();
		UT_ASSERT(cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
		UT_ASSERT(cluster_subtrans_native_recovery_verify_v1(1, 100, 1, &child));
	}
}

UT_TEST(verify_reads_disk_and_preserves_conflicts)
{
	TransactionId child = 101;
	fixture_zero(0);
	UT_ASSERT(cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	fixture_parent(child, 99);
	UT_ASSERT(!cluster_subtrans_native_recovery_verify_v1(1, 100, 1, &child));
	UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
	UT_ASSERT_EQ(fixture_status(child), 99);
	fixture_idle();
}

UT_TEST(wrap_assignment_keeps_original_xid_geometry)
{
	TransactionId children[] = { UINT32_MAX, 3, 4 };
	fixture_zero(TransactionIdToPage(UINT32_MAX));
	fixture_zero(0);
	UT_ASSERT(cluster_subtrans_native_recovery_apply_v1(1, UINT32_MAX - 1, 3, children));
	UT_ASSERT(cluster_subtrans_native_recovery_verify_v1(1, UINT32_MAX - 1, 3, children));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(fixture_status(children[i]), UINT32_MAX - 1);
	fixture_idle();
}

UT_TEST(scope_loss_after_read_or_sync_never_completes)
{
	TransactionId child = 101;
	for (int fault = 0; fault < 2; fault++) {
		fixture_zero(0);
		revoke_after_read = fault == 0;
		revoke_after_sync = fault == 1;
		UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
		revoke_after_read = revoke_after_sync = false;
		authority = true;
		fixture_idle();
	}
}

UT_TEST(sync_close_and_unsupported_directory_sync_cannot_complete)
{
	TransactionId child = 101;
	for (int fault = 0; fault < 4; fault++) {
		fixture_zero(0);
		fail_sync_close = fault == 0;
		directory_sync_error = fault == 1 ? EINVAL : fault == 2 ? EBADF : fault == 3 ? EACCES : 0;
		UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
		fail_sync_close = false;
		directory_sync_error = 0;
		fixture_idle();
	}
}

UT_TEST(segment_replacement_and_size_change_refuse_before_write)
{
	TransactionId child = 101;
	for (int mutation = 1; mutation <= 2; mutation++) {
		unsigned before = writes;
		fixture_zero(0);
		change_after_read = mutation;
		UT_ASSERT(!cluster_subtrans_native_recovery_apply_v1(1, 100, 1, &child));
		UT_ASSERT_EQ(writes, before);
		change_after_read = 0;
		fixture_idle();
	}
}

static void
fixture_cleanup(void)
{
	char path[MAXPGPATH], dirpath[MAXPGPATH];
	const char *parents[]
		= { "/native_side/origin_1/pg_subtrans", "/native_side/origin_1", "/native_side", "" };
	DIR *dir;
	struct dirent *entry;
	snprintf(dirpath, sizeof(dirpath), "%s%s", fixture_root, parents[0]);
	dir = opendir(dirpath);
	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] == '.')
			continue;
		snprintf(path, sizeof(path), "%s/%s", dirpath, entry->d_name);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	for (unsigned i = 0; i < lengthof(parents); i++) {
		snprintf(path, sizeof(path), "%s%s", fixture_root, parents[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
}

int
main(void)
{
	char path[MAXPGPATH];
	const char *dirs[]
		= { "native_side", "native_side/origin_1", "native_side/origin_1/pg_subtrans" };
	strlcpy(fixture_root, "/tmp/pgrac-subtrans-recovery.XXXXXX", sizeof(fixture_root));
	if (!mkdtemp(fixture_root))
		return 2;
	cluster_shared_data_dir = fixture_root;
	for (unsigned i = 0; i < lengthof(dirs); i++) {
		snprintf(path, sizeof(path), "%s/%s", fixture_root, dirs[i]);
		if (mkdir(path, 0700) != 0)
			return 2;
	}
	subtrans_recovery_shmem_init();
	UT_PLAN(12);
	UT_RUN(original_origin_controls_are_separate);
	UT_RUN(assignment_persists_all_children_and_repeats_without_terminal_changes);
	UT_RUN(later_conflict_refuses_the_entire_assignment_before_write);
	UT_RUN(missing_short_and_alias_are_not_zero_bases);
	UT_RUN(scope_role_and_origin_loss_refuse);
	UT_RUN(bad_or_unordered_assignment_never_writes);
	UT_RUN(io_errors_release_locks_and_discard_private_dirty_cache);
	UT_RUN(verify_reads_disk_and_preserves_conflicts);
	UT_RUN(wrap_assignment_keeps_original_xid_geometry);
	UT_RUN(scope_loss_after_read_or_sync_never_completes);
	UT_RUN(sync_close_and_unsupported_directory_sync_cannot_complete);
	UT_RUN(segment_replacement_and_size_change_refuse_before_write);
	fixture_cleanup();
	return ut_failed_count != 0;
}
