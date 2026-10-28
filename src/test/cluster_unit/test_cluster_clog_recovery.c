/*-------------------------------------------------------------------------
 * test_cluster_clog_recovery.c
 *    Original-origin terminal recovery through native CLOG and SLRU file I/O.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_clog_recovery.c
 * NOTES
 *    PGRAC test; only shmem, locks, original authority and fault delivery are
 *    fixtures. Native SLRU lookup, read, write, delete and sync bodies execute.
 *    This is not a cluster fencing or native startup qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
static int fixture_unlink(const char *path);
#define unlink fixture_unlink
#include "../../backend/access/transam/clog.c"
#include "../../backend/access/transam/slru.c"

#undef unlink
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
#ifndef TEST_CLUSTER_EPISODE_CLOG
UT_DEFINE_GLOBALS();
#endif

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
#ifndef TEST_CLUSTER_EPISODE_CLOG
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
#endif
CheckpointStatsData CheckpointStats;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static char fixture_root[MAXPGPATH];
static char fixture_cwd[MAXPGPATH];
static int fixture_origin = 1, c0_origin = 1;
LWLockPadded *MainLWLockArray;
static LWLockPadded own_main_locks[NUM_INDIVIDUAL_LWLOCKS];
static bool authority = true, revoke_on_lock, expect_error, fail_open, fail_sync, fail_dirsync;
static bool c0_authority;
static bool relmap_base_interrupts;
static bool revoke_after_read, revoke_after_sync;
static bool fail_read, fail_unlink;
static bool fail_sync_close;
static int synced_fd = -1, directory_sync_error, directory_open_error;
static unsigned durability_faults;
static unsigned deleted;
static int transient_fds;
static unsigned writes, syncs, dirsyncs, reads;
static bool observe_sync_order;
static XidStatus sync_states[16][3];
static unsigned sync_state_count;
static int change_after_read;
static void fixture_path(int page, char *path);
static XidStatus fixture_status(TransactionId xid);
static struct {
	LWLock *lock;
	bool held;
} fixture_locks[6000];
static unsigned fixture_lock_count;
static void *allocations[300];
static unsigned allocation_count;
static void *fixture_shared_mutation;
static Size fixture_shared_mutation_size;
static void (*fixture_native_sync_hook)(void);

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
	if (expect_error && PG_exception_stack != NULL) {
		/* Match elog ERROR after it clears the original lock holdoff. */
		if (relmap_base_interrupts)
			InterruptHoldoffCount = 0;
		siglongjmp(*PG_exception_stack, 1);
	}
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

#ifndef TEST_CLUSTER_SIDE_NATIVE_CLOG
bool
cluster_undo_recovery_cold_origin_authorized_v1(int origin)
{
	return authority && origin == 1;
}
#endif
bool
cluster_undo_recovery_abort_clog_authorized_v1(int origin, TransactionId xid)
{
	return c0_authority && origin == c0_origin && xid == 100;
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
ShmemInitStruct(const char *name, Size size, bool *found)
{
	void *p = calloc(1, size);
	if (strcmp(name, "CLOG relmap mutation") == 0) {
		free(p);
		if (fixture_shared_mutation != NULL) {
			if (size != fixture_shared_mutation_size) abort();
			*found = true;
			return fixture_shared_mutation;
		}
		p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
		if (p == MAP_FAILED) abort();
		fixture_shared_mutation = p;
		fixture_shared_mutation_size = size;
		*found = false;
		return p;
	}
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
	if (relmap_base_interrupts)
		HOLD_INTERRUPTS();
	if (revoke_on_lock && (lock == &ClogRecoveryLocks[3].lock
		|| (c0_origin == 0 && lock == XactCtl->shared->ControlLock)))
		authority = c0_authority = false;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(fixture_locks[i].held);
	fixture_locks[i].held = false;
	if (relmap_base_interrupts)
		RESUME_INTERRUPTS();
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
		authority = c0_authority = false;
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
	if (directory_open_error && (flags & O_DIRECTORY)) {
		durability_faults++;
		errno = directory_open_error;
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
	if (fd == synced_fd)
		synced_fd = -1;
	transient_fds--;
	rc = close(fd);
	if (fail) {
		durability_faults++;
		errno = EIO;
		return -1;
	}
	return rc;
}
int
pg_fsync(int fd)
{
	struct stat st;
	if (fixture_native_sync_hook != NULL)
		fixture_native_sync_hook();
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	synced_fd = fd;
	if (S_ISDIR(st.st_mode)) {
		dirsyncs++;
		if (fail_dirsync) {
			errno = EIO;
			(void)errstart(ERROR, NULL);
		}
		if (directory_sync_error) {
			durability_faults++;
			errno = directory_sync_error;
			return -1;
		}
		return fsync(fd);
	}
	syncs++;
	if (observe_sync_order && sync_state_count < lengthof(sync_states)) {
		sync_states[sync_state_count][0] = fixture_status(100);
		sync_states[sync_state_count][1] = fixture_status(CLOG_XACTS_PER_PAGE + 17);
		sync_states[sync_state_count][2] = fixture_status(32 * CLOG_XACTS_PER_PAGE + 19);
		sync_state_count++;
	}
	if (fail_sync) {
		errno = EIO;
		return -1;
	}
	if (revoke_after_sync)
		authority = c0_authority = false;
	return fsync(fd);
}
void
fsync_fname(const char *path, bool isdir)
{
	int fd, rc;
	UT_ASSERT(isdir);
	dirsyncs++;
	/* Match fd.c's generic directory exceptions; cold completion must not
	 * inherit them. Actual CLOG/SLRU and their return handling run above. */
	if (directory_open_error == EACCES || directory_open_error == EISDIR
		|| directory_sync_error == EBADF || directory_sync_error == EINVAL) {
		durability_faults++;
		return;
	}
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


/* Only syscall failure delivery is replaced; the native deletion body runs. */
static int
fixture_unlink(const char *path)
{
	if (fail_unlink) {
		errno = EIO;
		return -1;
	}
	deleted++;
	return unlink(path);
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
	if (ClogRecoveryCtl[1].shared != NULL) {
		SlruShared s = ClogRecoveryCtl[1].shared;
		for (int i = 0; i < s->num_slots; i++) {
			UT_ASSERT_EQ(s->page_status[i], SLRU_PAGE_EMPTY);
			UT_ASSERT(!s->page_dirty[i]);
		}
	}
}

static void
fixture_path(int page, char *path)
{
	snprintf(path, MAXPGPATH, "%s/native_side/origin_%d/pg_xact/%04X", fixture_root,
			 fixture_origin, page / SLRU_PAGES_PER_SEGMENT);
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

static XidStatus
fixture_status(TransactionId xid)
{
	char path[MAXPGPATH];
	uint8 byte = 0;
	int page = TransactionIdToPage(xid);
	int fd;
	fixture_path(page, path);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(
			pread(fd, &byte, 1,
				  (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ + TransactionIdToByte(xid)),
			1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	return (byte >> (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT)) & CLOG_XACT_BITMASK;
}

static void
fixture_bit(TransactionId xid, XidStatus status)
{
	char path[MAXPGPATH];
	uint8 byte;
	int page = TransactionIdToPage(xid);
	off_t offset = (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ + TransactionIdToByte(xid);
	int fd;
	fixture_path(page, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pread(fd, &byte, 1, offset), 1);
		byte &= ~(CLOG_XACT_BITMASK << (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT));
		byte |= status << (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT);
		UT_ASSERT_EQ(pwrite(fd, &byte, 1, offset), 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(separate_original_origin_controls)
{
	UT_ASSERT(ClogRecoveryCtl[1].shared != NULL);
	UT_ASSERT(ClogRecoveryCtl[1].shared != XactCtl->shared);
	UT_ASSERT(ClogRecoveryCtl[1].shared != ClogRecoveryCtl[2].shared);
	UT_ASSERT(strstr(ClogRecoveryCtl[1].Dir, "/native_side/origin_1/pg_xact"));
	UT_ASSERT_EQ(ClogRecoveryCtl[1].sync_handler, SYNC_HANDLER_NONE);
	fixture_idle();
}

UT_TEST(commit_and_abort_persist_full_tree_without_local_pollution)
{
	TransactionId children[] = { 101, CLOG_XACTS_PER_PAGE + 17, 32 * CLOG_XACTS_PER_PAGE + 19 };
	fixture_zero(0);
	fixture_zero(1);
	fixture_zero(32);
	fixture_bit(99, TRANSACTION_STATUS_ABORTED);
	observe_sync_order = true;
	sync_state_count = 0;
	UT_ASSERT(
		cluster_clog_native_recovery_apply_v1(1, 100, 3, children, TRANSACTION_STATUS_COMMITTED));
	observe_sync_order = false;
	UT_ASSERT_EQ(sync_state_count, 5);
	UT_ASSERT_EQ(sync_states[0][0], TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(sync_states[0][1], TRANSACTION_STATUS_SUB_COMMITTED);
	UT_ASSERT_EQ(sync_states[1][0], TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(sync_states[1][2], TRANSACTION_STATUS_SUB_COMMITTED);
	UT_ASSERT_EQ(sync_states[2][0], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(sync_states[2][1], TRANSACTION_STATUS_SUB_COMMITTED);
	UT_ASSERT_EQ(sync_states[2][2], TRANSACTION_STATUS_SUB_COMMITTED);
	UT_ASSERT_EQ(sync_states[4][1], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(sync_states[4][2], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(
		cluster_clog_native_recovery_verify_v1(1, 100, 3, children, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_COMMITTED);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(fixture_status(children[i]), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(fixture_status(99), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(
		cluster_clog_native_recovery_apply_v1(1, 100, 3, children, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 120, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 120, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(syncs > 0 && dirsyncs > 0);
	fixture_idle();
}

UT_TEST(conflicting_late_child_refuses_before_any_tree_write)
{
	TransactionId children[] = { CLOG_XACTS_PER_PAGE + 1, 32 * CLOG_XACTS_PER_PAGE + 1 };
	unsigned before = writes;
	fixture_zero(0);
	fixture_zero(1);
	fixture_zero(32);
	fixture_bit(children[1], TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 2, children, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(fixture_status(children[0]), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT(
		!cluster_clog_native_recovery_verify_v1(1, 100, 2, children, TRANSACTION_STATUS_COMMITTED));
	fixture_idle();
}

UT_TEST(missing_short_and_alias_files_never_become_zero_bases)
{
	char path[MAXPGPATH], alias[MAXPGPATH];
	TransactionId xid = 64 * CLOG_XACTS_PER_PAGE + 1;
	unsigned before = writes;
	fixture_path(64, path);
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, xid, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	fixture_zero(64);
	UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, xid, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	fixture_zero(64);
	snprintf(alias, sizeof(alias), "%s.alias", path);
	UT_ASSERT_EQ(link(path, alias), 0);
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, xid, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(unlink(alias), 0);
	UT_ASSERT_EQ(rename(path, alias), 0);
	UT_ASSERT_EQ(symlink(alias, path), 0);
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, xid, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(rename(alias, path), 0);
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(wrong_scope_local_origin_and_revocation_refuse)
{
	unsigned before = writes;
	fixture_zero(0);
	authority = false;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	authority = true;
	for (int origin = -1; origin <= 128; origin++)
		if (origin != 1)
			UT_ASSERT(!cluster_clog_native_recovery_apply_v1(origin, 100, 0, NULL,
															 TRANSACTION_STATUS_COMMITTED));
	InRecovery = false;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	InRecovery = true;
	MyAuxProcType = BgWriterProcess;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	MyAuxProcType = StartupProcess;
	revoke_on_lock = true;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	revoke_on_lock = false;
	authority = true;
	revoke_after_read = true;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	revoke_after_read = false;
	authority = true;
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(invalid_tree_and_nonterminal_status_refuse)
{
	TransactionId children[] = { 102, 101 };
	unsigned before = writes;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 2, children, TRANSACTION_STATUS_COMMITTED));
	children[1] = children[0];
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 2, children, TRANSACTION_STATUS_COMMITTED));
	children[0] = 100;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 1, children, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, -1, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 1, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, FrozenTransactionId, 0, NULL,
													 TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_SUB_COMMITTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_IN_PROGRESS));
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(native_io_and_sync_errors_discard_private_dirty_state)
{
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
			bool ok = cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
															TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT(!ok);
			caught = !ok;
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expect_error = fail_open = fail_read = fail_sync = fail_dirsync = false;
		UT_ASSERT(caught);
		fixture_idle();
		UT_ASSERT(
			cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
		UT_ASSERT(
			cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
		fixture_idle();
	}
}

UT_TEST(postread_uses_real_disk_not_successful_cache)
{
	fixture_zero(0);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	fixture_bit(100, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(
		!cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	fixture_idle();
}

UT_TEST(wrap_tree_retains_native_page_and_commit_order)
{
	TransactionId children[] = { UINT32_MAX, 17, 33 };
	fixture_zero(TransactionIdToPage(UINT32_MAX));
	fixture_zero(0);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, UINT32_MAX - 1, 3, children,
													TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, UINT32_MAX - 1, 3, children,
													 TRANSACTION_STATUS_COMMITTED));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(fixture_status(children[i]), TRANSACTION_STATUS_COMMITTED);
	fixture_idle();
}

UT_TEST(authority_loss_after_sync_does_not_report_completion)
{
	fixture_zero(0);
	revoke_after_sync = true;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	revoke_after_sync = false;
	authority = true;
	fixture_idle();
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	fixture_idle();
}

UT_TEST(base_change_after_native_read_refuses_without_writes)
{
	for (int mutation = 1; mutation <= 3; mutation++) {
		char path[MAXPGPATH];
		unsigned before = writes;
		fixture_path(0, path);
		fixture_zero(0);
		UT_ASSERT_EQ(truncate(path, BLCKSZ), 0);
		change_after_read = mutation;
		UT_ASSERT(
			!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
		UT_ASSERT_EQ(change_after_read, 0);
		UT_ASSERT_EQ(writes, before);
		fixture_idle();
	}
}

UT_TEST(interrupted_subcommit_can_resume_from_original_terminal)
{
	TransactionId child = CLOG_XACTS_PER_PAGE + 17;
	fixture_zero(0);
	fixture_zero(1);
	fail_sync = true;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 1, &child, TRANSACTION_STATUS_COMMITTED));
	fail_sync = false;
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(fixture_status(child), TRANSACTION_STATUS_SUB_COMMITTED);
	fixture_idle();
	UT_ASSERT(
		cluster_clog_native_recovery_apply_v1(1, 100, 1, &child, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(
		cluster_clog_native_recovery_verify_v1(1, 100, 1, &child, TRANSACTION_STATUS_COMMITTED));
	fixture_idle();
}

static ClusterSideProjectionOperationV1
fixture_clog_record(bool truncate, int page)
{
	ClusterSideProjectionOperationV1 op = { 0 };
	op.kind = CLUSTER_SIDE_PROJECTION_CLOG;
	op.action = truncate ? CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE
						 : CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	op.normalized_info = truncate ? CLOG_TRUNCATE : CLOG_ZEROPAGE;
	op.page_number = page;
	if (truncate) {
		op.oldest_xid = (uint32)page * CLOG_XACTS_PER_PAGE + 1;
		op.oldest_database = 5;
	}
	return op;
}
static bool
fixture_exists(int page)
{
	char path[MAXPGPATH];
	struct stat st;
	fixture_path(page, path);
	return lstat(path, &st) == 0;
}
UT_TEST(native_zero_record_creates_and_reads_back_only_its_exact_page)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 97);
	TransactionId xid = 97 * CLOG_XACTS_PER_PAGE + 1;
	unsigned before_sync = syncs, before_dirsync = dirsyncs;
	fixture_zero(96);
	fixture_bit(96 * CLOG_XACTS_PER_PAGE + 7, TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT_EQ(fixture_status(xid), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(fixture_status(96 * CLOG_XACTS_PER_PAGE + 7), TRANSACTION_STATUS_COMMITTED);
	fixture_bit(xid, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT_EQ(fixture_status(xid), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT(syncs > before_sync && dirsyncs > before_dirsync);
	op = fixture_clog_record(false, 128);
	UT_ASSERT(!fixture_exists(128));
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_verify_v1(1, &op));
	fixture_idle();
}
UT_TEST(native_truncate_keeps_partial_segment_and_wrap_boundary)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(true, 97);
	unsigned before = deleted;
	fixture_zero(0);
	fixture_zero(32);
	fixture_zero(64);
	fixture_zero(96);
	fixture_zero(128);
	UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT(!fixture_exists(0) && !fixture_exists(32) && !fixture_exists(64));
	UT_ASSERT(fixture_exists(96) && fixture_exists(128));
	UT_ASSERT(deleted >= before + 3);
	before = deleted;
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT_EQ(deleted, before);
	/* Across uint32 wrap the last segment is old; the half-range is not. */
	fixture_zero(131040);
	fixture_zero(0);
	fixture_zero(32);
	fixture_zero(65568);
	op = fixture_clog_record(true, 32);
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT(!fixture_exists(131040) && !fixture_exists(0));
	UT_ASSERT(fixture_exists(32) && fixture_exists(65568));
	fixture_idle();
}
UT_TEST(native_clog_record_bad_scope_and_shape_do_not_write_or_delete)
{
	unsigned before_writes = writes, before_deleted = deleted;
	ClusterSideProjectionOperationV1 op;
	for (int fault = 0; fault < 10; fault++) {
		op = fixture_clog_record(fault >= 4, 97);
		if (fault == 0)
			op.kind = CLUSTER_SIDE_PROJECTION_COMMIT_TS;
		if (fault == 1)
			op.action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
		if (fault == 2)
			op.normalized_info = CLOG_TRUNCATE;
		if (fault == 3)
			op.page_number = 131072;
		if (fault == 4)
			op.oldest_xid = InvalidTransactionId;
		if (fault == 5)
			op.oldest_database = InvalidOid;
		if (fault == 6)
			op.page_number++;
		if (fault == 7)
			op.page_number = -1;
		if (fault == 8)
			authority = false;
		if (fault == 9)
			InRecovery = false;
		UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &op));
		authority = InRecovery = true;
	}
	op = fixture_clog_record(false, 97);
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(0, &op));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(2, &op));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, NULL));
	UT_ASSERT_EQ(writes, before_writes);
	UT_ASSERT_EQ(deleted, before_deleted);
	fixture_idle();
}
UT_TEST(native_clog_record_alias_and_unlink_failure_remain_incomplete)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 160);
	char path[MAXPGPATH], alias[MAXPGPATH];
	unsigned before = writes;
	fixture_zero(160);
	fixture_path(160, path);
	snprintf(alias, sizeof(alias), "%s.alias", path);
	UT_ASSERT_EQ(link(path, alias), 0);
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT_EQ(unlink(alias), 0);
	/* All eligible files must preflight before even the first deletion. */
	fixture_zero(0);
	fixture_zero(32);
	fixture_path(32, path);
	snprintf(alias, sizeof(alias), "%s.alias", path);
	UT_ASSERT_EQ(link(path, alias), 0);
	op = fixture_clog_record(true, 97);
	before = deleted;
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT_EQ(deleted, before);
	UT_ASSERT(fixture_exists(0) && fixture_exists(32));
	UT_ASSERT_EQ(unlink(alias), 0);
	fixture_zero(0);
	op = fixture_clog_record(true, 32);
	fail_unlink = true;
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &op));
	fail_unlink = false;
	UT_ASSERT(fixture_exists(0));
	UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &op));
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT(!fixture_exists(0));
	fixture_idle();
}
UT_TEST(native_clog_record_io_error_releases_original_locks_and_private_cache)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterSideProjectionOperationV1 op
			= fixture_clog_record(fault == 4, fault == 4 ? 32 : 160);
		volatile bool caught = false;
		bool result = false;
		if (fault == 4)
			fixture_zero(0);
		fail_open = fault == 0;
		fail_sync = fault == 1;
		fail_dirsync = fault == 2 || fault == 4;
		revoke_after_sync = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_record_apply_v1(1, &op);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		UT_ASSERT_EQ(caught, fault == 0 || fault == 2 || fault == 4);
		expect_error = fail_open = fail_sync = fail_dirsync = revoke_after_sync = false;
		authority = true;
		fixture_idle();
		UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT(cluster_clog_native_recovery_record_verify_v1(1, &op));
	}
}

UT_TEST(native_clog_durability_failures_refuse_completion_and_allow_exact_retry)
{
	/* All four consumers must fail even if native bytes already match. */
	for (int operation = 0; operation < 4; operation++) {
		for (int fault = operation == 3 ? 1 : 0; fault < 5; fault++) {
			ClusterSideProjectionOperationV1 op
				= fixture_clog_record(operation == 3, operation == 3 ? 32 : 160);
			XidStatus status
				= operation == 1 ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED;
			unsigned before = durability_faults;
			bool result;

			fixture_zero(0);
			fixture_zero(32);
			fixture_zero(160);
			fail_sync_close = fault == 0;
			directory_sync_error = fault == 1 ? EINVAL : fault == 2 ? EBADF : 0;
			directory_open_error = fault == 3 ? EACCES : fault == 4 ? EISDIR : 0;
			result = operation < 2 ? cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, status)
								   : cluster_clog_native_recovery_record_apply_v1(1, &op);
			UT_ASSERT(!result);
			UT_ASSERT(durability_faults > before);
			fail_sync_close = false;
			directory_sync_error = directory_open_error = 0;
			fixture_idle();
			if (operation < 2) {
				UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, status));
				UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, status));
			} else {
				UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
				UT_ASSERT(cluster_clog_native_recovery_record_verify_v1(1, &op));
			}
			fixture_idle();
		}
	}
}

UT_TEST(c0_exact_native_abort_does_not_authorize_an_origin)
{
	TransactionId child = 101;
	ClusterSideProjectionOperationV1 zero = fixture_clog_record(false, 160);
	ClusterSideProjectionOperationV1 truncate = fixture_clog_record(true, 32);
	unsigned before;
	fixture_zero(0);
	authority = false;
	c0_authority = true;
	before = writes;
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 101, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(
		!cluster_clog_native_recovery_apply_v1(1, 100, 1, &child, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(2, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &zero));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &truncate));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(fixture_status(101), TRANSACTION_STATUS_IN_PROGRESS);
	c0_authority = false;
	UT_ASSERT(!cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	authority = true;
	fixture_idle();
}

UT_TEST(c0_native_abort_retains_conflict_and_durability_refusal)
{
	for (int fault = 0; fault < 4; fault++) {
		unsigned before;
		fixture_zero(0);
		authority = false;
		c0_authority = true;
		if (fault == 0)
			fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		fail_sync = fault == 1;
		fail_sync_close = fault == 2;
		directory_sync_error = fault == 3 ? EINVAL : 0;
		before = writes;
		UT_ASSERT(
			!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
		if (fault == 0) {
			UT_ASSERT_EQ(writes, before);
			UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_COMMITTED);
		} else
			UT_ASSERT(writes > before);
		fail_sync = fail_sync_close = false;
		directory_sync_error = 0;
		c0_authority = false;
		authority = true;
		fixture_idle();
	}
}

UT_TEST(c0_native_abort_revocation_and_error_keep_original_cleanup)
{
	for (int fault = 0; fault < 6; fault++) {
		volatile bool caught = false;
		volatile bool result = false;
		unsigned before;
		fixture_zero(0);
		authority = false;
		c0_authority = true;
		revoke_on_lock = fault == 0;
		revoke_after_read = fault == 1;
		revoke_after_sync = fault == 2;
		fail_read = fault == 3;
		fail_open = fault == 4;
		fail_dirsync = fault == 5;
		expect_error = true;
		before = writes;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
														   TRANSACTION_STATUS_ABORTED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		if (fault <= 1)
			UT_ASSERT_EQ(writes, before);
		expect_error = revoke_on_lock = revoke_after_read = revoke_after_sync = false;
		fail_read = fail_open = fail_dirsync = false;
		fixture_idle();
		c0_authority = true;
		UT_ASSERT(
			cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
		UT_ASSERT(
			cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
		c0_authority = false;
		authority = true;
		fixture_idle();
	}
}

UT_TEST(post_eor_c0_only_restores_the_exact_foreign_top_abort)
{
	TransactionId child = 101;
	ClusterSideProjectionOperationV1 zero = fixture_clog_record(false, 160);
	ClusterSideProjectionOperationV1 truncate = fixture_clog_record(true, 32);
	unsigned before;

	fixture_zero(0);
	InRecovery = false;
	authority = c0_authority = true;
	before = writes;
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 101, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, 1, &child,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &zero));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &truncate));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(fixture_status(101), TRANSACTION_STATUS_IN_PROGRESS);
	c0_authority = false;
	UT_ASSERT(!cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	InRecovery = true;
	fixture_idle();
}

static void
fixture_own_reset(void)
{
	fixture_origin = c0_origin = 0;
	InRecovery = authority = false;
	c0_authority = true;
	for (int i = 0; i < XactCtl->shared->num_slots; i++) {
		XactCtl->shared->page_status[i] = SLRU_PAGE_EMPTY;
		XactCtl->shared->page_dirty[i] = false;
	}
	fixture_zero(0);
}

UT_TEST(own_c0_uses_original_cache_and_preserves_other_transactions)
{
	int slot, other;
	XLogRecPtr lsn;
	fixture_own_reset();
	fixture_zero(1);
	LWLockAcquire(XactCtl->shared->ControlLock, LW_EXCLUSIVE);
	slot = SimpleLruReadPage(XactCtl, 0, true, 100);
	UT_ASSERT(clog_recovery_status(XactCtl, slot, 101, TRANSACTION_STATUS_COMMITTED,
		CLOG_RECOVERY_WRITE));
	other = SimpleLruReadPage(XactCtl, 1, true, CLOG_XACTS_PER_PAGE + 17);
	UT_ASSERT(clog_recovery_status(XactCtl, other, CLOG_XACTS_PER_PAGE + 17,
		TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
	LWLockRelease(XactCtl->shared->ControlLock);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(fixture_status(101), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(TransactionIdGetStatus(100, &lsn), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(TransactionIdGetStatus(101, &lsn), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(XactCtl->shared->page_status[other], SLRU_PAGE_VALID);
	UT_ASSERT(XactCtl->shared->page_dirty[other]);
	UT_ASSERT_EQ(fixture_status(CLOG_XACTS_PER_PAGE + 17), TRANSACTION_STATUS_IN_PROGRESS);
	for (int i = 0; i < ClogRecoveryCtl[0].shared->num_slots; i++)
		UT_ASSERT_EQ(ClogRecoveryCtl[0].shared->page_status[i], SLRU_PAGE_EMPTY);
	fixture_idle();
}

UT_TEST(own_c0_rejects_committed_disk_or_cache_and_requires_disk_readback)
{
	int slot;
	unsigned before;
	for (int fault = 0; fault < 3; fault++) {
		fixture_own_reset();
		LWLockAcquire(XactCtl->shared->ControlLock, LW_EXCLUSIVE);
		slot = SimpleLruReadPage(XactCtl, 0, true, 100);
		if (fault != 0)
			UT_ASSERT(clog_recovery_status(XactCtl, slot, 100,
				fault == 1 ? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED,
				CLOG_RECOVERY_WRITE));
		LWLockRelease(XactCtl->shared->ControlLock);
		if (fault == 0)
			fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		before = writes;
		UT_ASSERT(!(fault == 2
			? cluster_clog_native_recovery_verify_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED)
			: cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED)));
		UT_ASSERT_EQ(writes, before);
		UT_ASSERT_EQ(fixture_status(100), fault == 0 ? TRANSACTION_STATUS_COMMITTED
			: TRANSACTION_STATUS_IN_PROGRESS);
		fixture_idle();
	}
}

UT_TEST(own_c0_keeps_exact_scope_and_original_directory_route)
{
	char expected[MAXPGPATH];
	unsigned before;
	fixture_own_reset();
	snprintf(expected, sizeof(expected), "%s/native_side/origin_1/pg_xact", fixture_root);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(symlink(expected, "pg_xact"), 0);
	before = writes;
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT_EQ(writes, before);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	snprintf(expected, sizeof(expected), "%s/native_side/origin_0/pg_xact", fixture_root);
	UT_ASSERT_EQ(symlink(expected, "pg_xact"), 0);
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 101, 0, NULL, TRANSACTION_STATUS_ABORTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_COMMITTED));
	c0_authority = false;
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	c0_authority = true;
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	fixture_idle();
}

UT_TEST(own_c0_native_error_and_revocation_preserve_cache_ownership)
{
	volatile bool caught = false;
	fixture_own_reset();
	fail_open = expect_error = true;
	PG_TRY();
	{
		(void)cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fail_open = expect_error = false;
	UT_ASSERT(caught);
	fixture_idle();
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	fixture_own_reset();
	revoke_on_lock = true;
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	revoke_on_lock = false;
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
	fixture_idle();
	fixture_own_reset();
	revoke_after_sync = true;
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(0, 100, 0, NULL, TRANSACTION_STATUS_ABORTED));
	revoke_after_sync = false;
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	fixture_idle();
	fixture_origin = c0_origin = 1;
	InRecovery = authority = true;
	c0_authority = false;
}

#include "test_cluster_clog_relmap_participant.inc"
#include "test_cluster_clog_page_birth.inc"
#include "test_cluster_clog_relmap_mutation.inc"
#include "test_cluster_clog_relmap_shared_mutation.inc"

static void
fixture_cleanup(void)
{
	char path[MAXPGPATH], dirpath[MAXPGPATH];
	const char *parents[] = { "/native_side/origin_1/pg_xact", "/native_side/origin_1",
		"/native_side/origin_0/pg_xact", "/native_side/origin_0", "/native_side", "" };
	DIR *dir;
	struct dirent *entry;
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(fixture_cwd), 0);
	for (int origin = 0; origin < 2; origin++) {
		snprintf(dirpath, sizeof(dirpath), "%s/native_side/origin_%d/pg_xact", fixture_root, origin);
		dir = opendir(dirpath);
		while ((entry = readdir(dir)) != NULL) {
			if (entry->d_name[0] == '.')
				continue;
			snprintf(path, sizeof(path), "%s/%s", dirpath, entry->d_name);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT_EQ(closedir(dir), 0);
	}
	for (unsigned i = 0; i < lengthof(parents); i++) {
		snprintf(path, sizeof(path), "%s%s", fixture_root, parents[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	if (fixture_shared_mutation != NULL)
		UT_ASSERT_EQ(munmap(fixture_shared_mutation, fixture_shared_mutation_size), 0);
}

#include "test_cluster_clog_full_consumer.inc"
#include "test_cluster_clog_own_full.inc"

int
main(void)
{
	char path[MAXPGPATH];
	const char *dirs[] = { "native_side", "native_side/origin_1", "native_side/origin_1/pg_xact",
		"native_side/origin_0", "native_side/origin_0/pg_xact" };
	if (!getcwd(fixture_cwd, sizeof(fixture_cwd)))
		return 2;
	strlcpy(fixture_root, "/tmp/pgrac-clog-recovery.XXXXXX", sizeof(fixture_root));
	if (!mkdtemp(fixture_root))
		return 2;
	cluster_shared_data_dir = fixture_root;
	for (unsigned i = 0; i < lengthof(dirs); i++) {
		snprintf(path, sizeof(path), "%s/%s", fixture_root, dirs[i]);
		if (mkdir(path, 0700) != 0)
			return 2;
	}
	clog_recovery_shmem_init();
	MainLWLockArray = own_main_locks;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(XactCtl, "Xact own fixture", 4, CLOG_LSNS_PER_PAGE, XactSLRULock,
		"pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	if (chdir(fixture_root) != 0)
		return 2;
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", fixture_root);
	if (symlink(path, "pg_xact") != 0)
		return 2;
	if (getenv("PGRAC_RELMAP_MUTATION_ONLY") != NULL) {
		UT_PLAN(6);
		UT_RUN(relmap_native_terminal_aba_does_not_revive_held_observation);
		UT_RUN(relmap_native_record_mutations_invalidate_even_unchanged_bytes);
		UT_RUN(relmap_local_native_status_zero_and_redo_invalidate);
		UT_RUN(relmap_verify_refusal_other_origin_and_release_preserve_custody);
		UT_RUN(relmap_failed_native_mutation_cannot_revalidate_old_bytes);
		UT_RUN(relmap_native_mutation_during_first_or_final_root_refuses);
		goto mutation_done;
	}
	if (getenv("PGRAC_RELMAP_SHARED_MUTATION_ONLY") != NULL) {
		UT_PLAN(6);
		UT_RUN(relmap_other_process_terminal_aba_revokes_original_custody);
		UT_RUN(relmap_other_process_zero_full_and_truncate_revoke);
		UT_RUN(relmap_other_process_local_status_zero_and_redo_revoke);
		UT_RUN(relmap_other_process_verify_refusal_and_origin_isolation);
		UT_RUN(relmap_other_process_inflight_write_refuses_new_and_old_reads);
		UT_RUN(relmap_other_process_mutation_during_root_cannot_publish);
		goto mutation_done;
	}
	if (getenv("PGRAC_OWN_FULL_CLOG_ONLY") != NULL) {
		UT_PLAN(4);
		UT_RUN(own_full_cold_replay_preserves_native_cache_and_reads_disk);
		UT_RUN(own_full_cold_replay_requires_exact_cold_role_shape_and_route);
		UT_RUN(own_full_cold_replay_errors_keep_original_cache_and_cleanup);
		UT_RUN(own_full_cold_replay_revokes_equal_byte_held_observation);
		goto mutation_done;
	}
	UT_PLAN(58);
	UT_RUN(separate_original_origin_controls);
	UT_RUN(commit_and_abort_persist_full_tree_without_local_pollution);
	UT_RUN(conflicting_late_child_refuses_before_any_tree_write);
	UT_RUN(missing_short_and_alias_files_never_become_zero_bases);
	UT_RUN(wrong_scope_local_origin_and_revocation_refuse);
	UT_RUN(invalid_tree_and_nonterminal_status_refuse);
	UT_RUN(native_io_and_sync_errors_discard_private_dirty_state);
	UT_RUN(postread_uses_real_disk_not_successful_cache);
	UT_RUN(wrap_tree_retains_native_page_and_commit_order);
	UT_RUN(authority_loss_after_sync_does_not_report_completion);
	UT_RUN(base_change_after_native_read_refuses_without_writes);
	UT_RUN(interrupted_subcommit_can_resume_from_original_terminal);
	UT_RUN(native_zero_record_creates_and_reads_back_only_its_exact_page);
	UT_RUN(native_truncate_keeps_partial_segment_and_wrap_boundary);
	UT_RUN(native_clog_record_bad_scope_and_shape_do_not_write_or_delete);
	UT_RUN(native_clog_record_alias_and_unlink_failure_remain_incomplete);
	UT_RUN(native_clog_record_io_error_releases_original_locks_and_private_cache);
	UT_RUN(native_clog_durability_failures_refuse_completion_and_allow_exact_retry);
	UT_RUN(c0_exact_native_abort_does_not_authorize_an_origin);
	UT_RUN(c0_native_abort_retains_conflict_and_durability_refusal);
	UT_RUN(c0_native_abort_revocation_and_error_keep_original_cleanup);
	UT_RUN(post_eor_c0_only_restores_the_exact_foreign_top_abort);
	UT_RUN(own_c0_uses_original_cache_and_preserves_other_transactions);
	UT_RUN(own_c0_rejects_committed_disk_or_cache_and_requires_disk_readback);
	UT_RUN(own_c0_keeps_exact_scope_and_original_directory_route);
	UT_RUN(own_c0_native_error_and_revocation_preserve_cache_ownership);
	UT_RUN(relmap_episode_base_all_statuses_and_original_origin_routes);
	UT_RUN(relmap_episode_preconditions_keep_caller_locks_and_original_debt);
	UT_RUN(relmap_episode_same_fd_replacement_and_full_image_change_are_sticky);
	UT_RUN(relmap_episode_native_failures_keep_unknown_and_original_protection);
	UT_RUN(relmap_episode_late_holder_loss_revokes_after_io);
	UT_RUN(relmap_episode_error_keeps_held_fd_until_local_cleanup);
	UT_RUN(relmap_episode_own_route_and_cache_are_not_substituted);
	UT_RUN(relmap_episode_exact_source_history_and_root_recheck);
	UT_RUN(clog_full_birth_uses_actual_native_zero_and_full_wal);
	UT_RUN(clog_full_birth_requires_exclusive_native_issuer);
	UT_RUN(clog_full_birth_redo_checks_complete_native_image);
	UT_RUN(clog_full_birth_error_keeps_native_dirty_state);
	UT_RUN(native_full_clog_consumer_preserves_original_page_and_neighbors);
	UT_RUN(native_full_clog_consumer_refuses_shape_and_scope_before_io);
	UT_RUN(native_full_clog_consumer_keeps_late_failure_incomplete);
	UT_RUN(relmap_native_terminal_aba_does_not_revive_held_observation);
	UT_RUN(relmap_native_record_mutations_invalidate_even_unchanged_bytes);
	UT_RUN(relmap_local_native_status_zero_and_redo_invalidate);
	UT_RUN(relmap_verify_refusal_other_origin_and_release_preserve_custody);
	UT_RUN(relmap_failed_native_mutation_cannot_revalidate_old_bytes);
	UT_RUN(relmap_native_mutation_during_first_or_final_root_refuses);
	UT_RUN(relmap_other_process_terminal_aba_revokes_original_custody);
	UT_RUN(relmap_other_process_zero_full_and_truncate_revoke);
	UT_RUN(relmap_other_process_local_status_zero_and_redo_revoke);
	UT_RUN(relmap_other_process_verify_refusal_and_origin_isolation);
	UT_RUN(relmap_other_process_inflight_write_refuses_new_and_old_reads);
	UT_RUN(relmap_other_process_mutation_during_root_cannot_publish);
	UT_RUN(relmap_shared_mutation_saturation_and_missing_state_never_grant);
	UT_RUN(own_full_cold_replay_preserves_native_cache_and_reads_disk);
	UT_RUN(own_full_cold_replay_requires_exact_cold_role_shape_and_route);
	UT_RUN(own_full_cold_replay_errors_keep_original_cache_and_cleanup);
	UT_RUN(own_full_cold_replay_revokes_equal_byte_held_observation);
mutation_done:
	fixture_cleanup();
	UT_DONE();
	return ut_failed_count != 0;
}
