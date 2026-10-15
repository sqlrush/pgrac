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
#include <sys/wait.h>
#include "../../backend/access/transam/clog.c"
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
	if (revoke_on_lock && lock == &ClogRecoveryLocks[3].lock)
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
	transient_fds--;
	return close(fd);
}
int
pg_fsync(int fd)
{
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
		authority = false;
	return fsync(fd);
}
void
fsync_fname(const char *path, bool isdir)
{
	int fd, rc;
	UT_ASSERT(isdir);
	dirsyncs++;
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
	snprintf(path, MAXPGPATH, "%s/native_side/origin_1/pg_xact/%04X", fixture_root,
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

static void
fixture_cleanup(void)
{
	char path[MAXPGPATH], dirpath[MAXPGPATH];
	const char *parents[]
		= { "/native_side/origin_1/pg_xact", "/native_side/origin_1", "/native_side", "" };
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
	const char *dirs[] = { "native_side", "native_side/origin_1", "native_side/origin_1/pg_xact" };
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
	UT_PLAN(12);
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
	fixture_cleanup();
	UT_DONE();
	return ut_failed_count != 0;
}
