/*-------------------------------------------------------------------------
 * test_cluster_subtrans_order.c
 *    Native SUBTRANS deletion and queued file synchronization.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_order.c
 * NOTES
 *    PGRAC test. Actual SLRU, SUBTRANS, sync, checkpointer queue and hash
 *    bodies operate on temporary files. Process allocation, LWLocks and
 *    error delivery are fixtures; forked writers share real mapped buffers.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <poll.h>
#include "../../backend/access/transam/subtrans.c"
#ifdef SLRU_TEST_SOURCE
#include SLRU_TEST_SOURCE
#else
#include "../../backend/access/transam/slru.c"
#endif
#include "../../backend/storage/sync/sync.c"
#include "../../backend/postmaster/checkpointer.c"
#include "../../backend/utils/hash/dynahash.c"
#include "../../backend/nodes/list.c"
#include "../../common/hashfn.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
bool IsUnderPostmaster = true;
bool InRecovery;
bool enableFsync = true;
BackendType MyBackendType = B_CHECKPOINTER;
AuxProcType MyAuxProcType = CheckpointerProcess;
PgStat_CheckpointerStats PendingCheckpointerStats;
TransactionId TransactionXmin = FirstNormalTransactionId;
volatile uint32 CritSectionCount;
volatile uint32 InterruptHoldoffCount;
volatile sig_atomic_t InterruptPending;
int pg_file_create_mode = 0600;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
CheckpointStatsData CheckpointStats;
static LWLockPadded fixture_locks[128];
LWLockPadded *MainLWLockArray = fixture_locks;
static PROC_HDR fixture_proc;
PROC_HDR *ProcGlobal = &fixture_proc;
static uint32 fixture_wait;
uint32 *my_wait_event_info = &fixture_wait;
static MemoryContextData fixture_context;
MemoryContext TopMemoryContext = &fixture_context;
MemoryContext CurrentMemoryContext = &fixture_context;
static char fixture_path[MAXPGPATH];
static int lock_fd, error_level, error_errno;
static bool catch_error, sync_fails, fail_copy_alloc, fail_critical_alloc;
static unsigned sync_calls, file_reads;
static int pause_writer[2], resume_writer[2], wait_reader[2];
static int writer_pause;
static bool reader_notify;
static struct {
	LWLock *ptr;
	bool held;
} lock_map[256];
static unsigned lock_count;
static struct {
	void *ptr;
	Size size;
} mappings[8];
static unsigned mapping_count;

static void fixture_panic(void) pg_attribute_noreturn();
static void
fixture_panic(void)
{
	abort();
}
void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	fixture_panic();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	error_level = CritSectionCount > 0 ? PANIC : level;
	error_errno = errno;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	if (catch_error && PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	fixture_panic();
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
bool data_sync_retry = false;
#include "test_cluster_subtrans_sync_elevel.inc"
void
pg_re_throw(void)
{
	fixture_panic();
}
void
ProcessInterrupts(void)
{
	fixture_panic();
}
int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}
MemoryContext
GetMemoryChunkContext(void *p pg_attribute_unused())
{
	return &fixture_context;
}
int
s_lock(volatile slock_t *l pg_attribute_unused(), const char *f pg_attribute_unused(),
	   int n pg_attribute_unused(), const char *fn pg_attribute_unused())
{
	fixture_panic();
}

static void *
fixture_allocate(Size size)
{
	void *p;
	if (fail_copy_alloc || (fail_critical_alloc && CritSectionCount > 0)) {
		fail_copy_alloc = false;
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("test allocation fault")));
	}
	p = calloc(1, size);
	if (!p)
		fixture_panic();
	return p;
}
void *
palloc(Size size)
{
	return fixture_allocate(size);
}
void *
palloc0(Size size)
{
	return fixture_allocate(size);
}
void *
repalloc(void *p, Size size)
{
	return realloc(p, size);
}
void
pfree(void *p)
{
	free(p);
}
void *
MemoryContextAlloc(MemoryContext c pg_attribute_unused(), Size s)
{
	return fixture_allocate(s);
}
void *
MemoryContextAllocZero(MemoryContext c pg_attribute_unused(), Size s)
{
	return fixture_allocate(s);
}
void *
MemoryContextAllocExtended(MemoryContext c pg_attribute_unused(), Size s,
						   int flags pg_attribute_unused())
{
	return fixture_allocate(s);
}
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent pg_attribute_unused(),
							  const char *name pg_attribute_unused(), Size a pg_attribute_unused(),
							  Size b pg_attribute_unused(), Size c pg_attribute_unused())
{
	fixture_context.type = T_AllocSetContext;
	return &fixture_context;
}
void
MemoryContextSetIdentifier(MemoryContext c pg_attribute_unused(),
						   const char *id pg_attribute_unused())
{}
void
MemoryContextAllowInCriticalSection(MemoryContext c pg_attribute_unused(),
									bool allow pg_attribute_unused())
{}
void
MemoryContextDelete(MemoryContext c pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (p == MAP_FAILED || mapping_count >= lengthof(mappings))
		fixture_panic();
	mappings[mapping_count].ptr = p;
	mappings[mapping_count++].size = size;
	*found = false;
	return p;
}
void
LWLockInitialize(LWLock *lock, int tranche pg_attribute_unused())
{
	if (lock_count >= lengthof(lock_map))
		fixture_panic();
	lock_map[lock_count++].ptr = lock;
}
static unsigned
fixture_lock_index(LWLock *lock)
{
	for (unsigned i = 0; i < lock_count; ++i)
		if (lock_map[i].ptr == lock)
			return i;
	fixture_panic();
	return 0;
}
static void
pipe_send(int fd)
{
	char value = 'x';
	if (write(fd, &value, 1) != 1)
		fixture_panic();
}
static void
pipe_receive(int fd)
{
	char value;
	struct pollfd p = { fd, POLLIN, 0 };
	if (poll(&p, 1, 5000) != 1 || read(fd, &value, 1) != 1)
		fixture_panic();
}
static bool
fixture_lock(LWLock *lock, LWLockMode mode, bool wait)
{
	unsigned i = fixture_lock_index(lock);
	struct flock f = { 0 };
	UT_ASSERT(!lock_map[i].held);
	if ((writer_pause == 1 && lock == CheckpointerCommLock)
		|| (writer_pause == 2 && lock == SubtransSLRULock
			&& SubTransCtl->shared->page_status[0] == SLRU_PAGE_WRITE_IN_PROGRESS
			&& CheckpointerShmem->num_requests > 0)) {
		writer_pause = false;
		pipe_send(pause_writer[1]);
		pipe_receive(resume_writer[0]);
	}
	if (reader_notify && mode == LW_SHARED && lock != SubtransSLRULock
		&& lock != CheckpointerCommLock) {
		reader_notify = false;
		pipe_send(wait_reader[1]);
	}
	f.l_type = mode == LW_EXCLUSIVE ? F_WRLCK : F_RDLCK;
	f.l_whence = SEEK_SET;
	f.l_start = i;
	f.l_len = 1;
	if (fcntl(lock_fd, wait ? F_SETLKW : F_SETLK, &f) != 0) {
		if (!wait && (errno == EAGAIN || errno == EACCES))
			return false;
		fixture_panic();
	}
	lock_map[i].held = true;
	return true;
}
bool
LWLockAcquire(LWLock *l, LWLockMode m)
{
	return fixture_lock(l, m, true);
}
bool
LWLockConditionalAcquire(LWLock *l, LWLockMode m)
{
	return fixture_lock(l, m, false);
}
bool
LWLockHeldByMe(LWLock *l)
{
	return lock_map[fixture_lock_index(l)].held;
}
void
LWLockRelease(LWLock *lock)
{
	unsigned i = fixture_lock_index(lock);
	struct flock f = { 0 };
	UT_ASSERT(lock_map[i].held);
	f.l_type = F_UNLCK;
	f.l_whence = SEEK_SET;
	f.l_start = i;
	f.l_len = 1;
	if (fcntl(lock_fd, F_SETLK, &f) != 0)
		fixture_panic();
	lock_map[i].held = false;
}
void
SetLatch(Latch *l pg_attribute_unused())
{}
int
WaitLatch(Latch *l pg_attribute_unused(), int events pg_attribute_unused(),
		  long ms pg_attribute_unused(), uint32 w pg_attribute_unused())
{
	fixture_panic();
	return 0;
}

bool
TransactionIdPrecedes(TransactionId a, TransactionId b)
{
	return (!TransactionIdIsNormal(a) || !TransactionIdIsNormal(b)) ? a < b : (int32)(a - b) < 0;
}
bool
TransactionIdFollowsOrEquals(TransactionId a, TransactionId b)
{
	return !TransactionIdPrecedes(a, b);
}
int
pgstat_get_slru_index(const char *s pg_attribute_unused())
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
	file_reads++;
}
void
pgstat_count_slru_page_exists(int i pg_attribute_unused())
{}
void
pgstat_count_slru_page_written(int i pg_attribute_unused())
{}
void
pgstat_count_slru_flush(int i pg_attribute_unused())
{}
void
pgstat_count_slru_truncate(int i pg_attribute_unused())
{}
void
XLogFlush(XLogRecPtr p pg_attribute_unused())
{
	fixture_panic();
}
DIR *
AllocateDir(const char *p)
{
	return opendir(p);
}
struct dirent *
ReadDir(DIR *d, const char *p pg_attribute_unused())
{
	return readdir(d);
}
int
FreeDir(DIR *d)
{
	return closedir(d);
}
int
OpenTransientFile(const char *p, int flags)
{
	return open(p, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
int
pg_fsync(int fd)
{
	sync_calls++;
	if (sync_fails) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
void
fsync_fname(const char *p, bool dir pg_attribute_unused())
{
	int fd = open(p, O_RDONLY);
	if (fd < 0 || fsync(fd) || close(fd))
		fixture_panic();
}
int
mdsyncfiletag(const FileTag *t, char *p)
{
	return SlruSyncFileTag(SubTransCtl, t, p);
}
int
mdunlinkfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
bool
mdfiletagmatches(const FileTag *t pg_attribute_unused(), const FileTag *c pg_attribute_unused())
{
	fixture_panic();
	return false;
}
int
clogsyncfiletag(const FileTag *t, char *p)
{
	return SlruSyncFileTag(SubTransCtl, t, p);
}
int
committssyncfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
int
multixactoffsetssyncfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
int
multixactmemberssyncfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
int
cluster_smgr_syncfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
int
cluster_smgr_unlinkfiletag(const FileTag *t pg_attribute_unused(), char *p pg_attribute_unused())
{
	fixture_panic();
	return 0;
}
bool
cluster_smgr_filetagmatches(const FileTag *t pg_attribute_unused(),
							const FileTag *c pg_attribute_unused())
{
	fixture_panic();
	return false;
}

static bool
fixture_process_sync(void)
{
	sigjmp_buf local;
	PG_exception_stack = &local;
	catch_error = true;
	error_level = error_errno = 0;
	if (sigsetjmp(local, 1) == 0)
		ProcessSyncRequests();
	catch_error = false;
	PG_exception_stack = NULL;
	return error_level == 0;
}
static void
fixture_write(int pageno)
{
	int slot;
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
	slot = SimpleLruZeroPage(SubTransCtl, pageno);
	((TransactionId *)SubTransCtl->shared->page_buffer[slot])[10] = 123;
	SubTransCtl->shared->latest_page_number = pageno;
	SimpleLruWritePage(SubTransCtl, slot);
	LWLockRelease(SubtransSLRULock);
}
static pid_t
fixture_backend(int page, BackendType role, int pause)
{
	pid_t pid;
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		ut_current_failed = 0;
		MyBackendType = role;
		MyAuxProcType = role == B_STARTUP ? StartupProcess : NotAnAuxProcess;
		pendingOps = NULL;
		writer_pause = pause;
		fixture_write(page);
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	return pid;
}
static void
fixture_wait_child(pid_t pid)
{
	int status;
	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}
static bool
fixture_exists(int seg)
{
	char path[MAXPGPATH];
	SlruFileName(SubTransCtl, path, seg);
	return access(path, F_OK) == 0;
}
static void
fixture_delete(bool truncate)
{
	if (truncate) {
		SubTransCtl->shared->latest_page_number = 2 * SLRU_PAGES_PER_SEGMENT;
		TruncateSUBTRANS(2 * SLRU_PAGES_PER_SEGMENT * SUBTRANS_XACTS_PER_PAGE + 2);
	} else
		SlruDeleteSegment(SubTransCtl, 0);
}
static void
fixture_setup(void)
{
	bool found;
	strlcpy(fixture_path, "/tmp/pgrac-subtrans-order.XXXXXX", sizeof(fixture_path));
	if (!mkdtemp(fixture_path) || chdir(fixture_path) || mkdir("pg_subtrans", 0700))
		fixture_panic();
	lock_fd = open("locks", O_CREAT | O_RDWR, 0600);
	if (lock_fd < 0 || pipe(pause_writer) || pipe(resume_writer) || pipe(wait_reader))
		fixture_panic();
	LWLockInitialize(SubtransSLRULock, 0);
	LWLockInitialize(CheckpointerCommLock, 0);
	IsUnderPostmaster = false;
	SUBTRANSShmemInit();
	IsUnderPostmaster = true;
	CheckpointerShmem = ShmemInitStruct(
		"queue", offsetof(CheckpointerShmemStruct, requests) + 1024 * sizeof(CheckpointerRequest),
		&found);
	CheckpointerShmem->max_requests = 1024;
	CheckpointerShmem->checkpointer_pid = getpid();
	InitSync();
}
static void
fixture_cleanup(void)
{
	DIR *d = opendir("pg_subtrans");
	struct dirent *e;
	char path[MAXPGPATH];
	if (!d)
		fixture_panic();
	while ((e = readdir(d)) != NULL)
		if (e->d_name[0] != '.') {
			snprintf(path, sizeof(path), "pg_subtrans/%s", e->d_name);
			if (unlink(path))
				fixture_panic();
		}
	closedir(d);
	rmdir("pg_subtrans");
	close(lock_fd);
	unlink("locks");
	if (chdir("/") || rmdir(fixture_path))
		fixture_panic();
}

static void
queued_delete_case(bool truncate)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT(fixture_exists(0));
	fixture_delete(truncate);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(error_level, 0);
	UT_ASSERT_EQ(error_errno, 0);
	UT_ASSERT_EQ(sync_calls, 0);
}
UT_TEST(queued_sync_then_checkpointer_forget)
{
	queued_delete_case(false);
}
UT_TEST(queued_sync_then_subtrans_truncate)
{
	queued_delete_case(true);
}

static void
inflight_case(bool truncate, int stage)
{
	pid_t writer = fixture_backend(0, B_BACKEND, stage), deleter;
	pipe_receive(pause_writer[0]);
	UT_ASSERT_EQ(SubTransCtl->shared->page_status[0], SLRU_PAGE_WRITE_IN_PROGRESS);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, stage == 2 ? 1 : 0);
	fflush(NULL);
	deleter = fork();
	if (deleter < 0)
		fixture_panic();
	if (deleter == 0) {
		reader_notify = true;
		fixture_delete(truncate);
		UT_ASSERT(!fixture_exists(0));
		UT_ASSERT(fixture_process_sync());
		UT_ASSERT_EQ(error_level, 0);
		UT_ASSERT_EQ(error_errno, 0);
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	pipe_receive(wait_reader[0]);
	UT_ASSERT(fixture_exists(0));
	pipe_send(resume_writer[1]);
	fixture_wait_child(writer);
	fixture_wait_child(deleter);
}
UT_TEST(delete_waits_for_original_writer_to_queue_sync)
{
	inflight_case(false, 1);
}
UT_TEST(truncate_waits_for_original_writer_to_queue_sync)
{
	inflight_case(true, 1);
}
UT_TEST(same_name_new_segment_is_synced)
{
	FileTag tag;
	PendingFsyncEntry *entry;

	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT(!fixture_exists(0));
	INIT_SLRUFILETAG(tag, SYNC_HANDLER_CLUSTER_SUBTRANS, 0);
	entry = hash_search(pendingOps, &tag, HASH_FIND, NULL);
	UT_ASSERT(entry != NULL);
	if (entry == NULL)
		return;
	UT_ASSERT(entry->canceled);

	/* PGRAC: a new SYNC must reactivate the still-present canceled entry. */
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	UT_ASSERT(fixture_exists(0));
	AbsorbSyncRequests();
	UT_ASSERT(hash_search(pendingOps, &tag, HASH_FIND, NULL) == entry);
	UT_ASSERT(!entry->canceled);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
UT_TEST(startup_forget_uses_original_queue_order)
{
	pid_t pid;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		MyBackendType = B_STARTUP;
		MyAuxProcType = StartupProcess;
		pendingOps = NULL;
		fixture_delete(false);
		UT_ASSERT_EQ(CheckpointerShmem->num_requests, 2);
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	fixture_wait_child(pid);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 0);
}
UT_TEST(retained_parent_is_not_deleted)
{
	int page = 2 * SLRU_PAGES_PER_SEGMENT;
	TransactionId child = page * SUBTRANS_XACTS_PER_PAGE + 11;
	fixture_wait_child(fixture_backend(page, B_BACKEND, false));
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
	SubTransCtl->shared->page_status[0] = SLRU_PAGE_EMPTY;
	LWLockRelease(SubtransSLRULock);
	TruncateSUBTRANS(child - 1);
	UT_ASSERT(fixture_exists(2));
	UT_ASSERT_EQ(SubTransGetParent(child - 1), 123);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
UT_TEST(missing_retained_file_keeps_fsync_failure)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	unlink("pg_subtrans/0000");
	UT_ASSERT(!fixture_process_sync());
	UT_ASSERT_EQ(error_level, PANIC);
	UT_ASSERT_EQ(error_errno, ENOENT);
}
UT_TEST(non_subtrans_handler_does_not_absorb)
{
	SubTransCtl->sync_handler = SYNC_HANDLER_CLOG;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT(!fixture_process_sync());
	UT_ASSERT_EQ(error_errno, ENOENT);
}

UT_TEST(delete_waits_for_queued_writer_before_valid)
{
	inflight_case(false, 2);
}
UT_TEST(truncate_waits_for_queued_writer_before_valid)
{
	inflight_case(true, 2);
}
static void
dirty_case(bool truncate)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
	SubTransCtl->shared->page_dirty[0] = true;
	LWLockRelease(SubtransSLRULock);
	fixture_delete(truncate);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(error_level, 0);
	UT_ASSERT_EQ(error_errno, 0);
}
UT_TEST(delete_drains_dirty_buffer_before_forget)
{
	dirty_case(false);
}
UT_TEST(truncate_drains_dirty_buffer_before_forget)
{
	dirty_case(true);
}
static void
full_queue_case(bool fault)
{
	pid_t pid;
	fixture_wait_child(fixture_backend(SLRU_PAGES_PER_SEGMENT, B_BACKEND, false));
	CheckpointerShmem->max_requests = 1;
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		sigjmp_buf local;
		ut_current_failed = 0;
		MyBackendType = B_BACKEND;
		MyAuxProcType = NotAnAuxProcess;
		pendingOps = NULL;
		sync_fails = fault;
		PG_exception_stack = &local;
		catch_error = true;
		if (sigsetjmp(local, 1) == 0)
			fixture_write(0);
		catch_error = false;
		PG_exception_stack = NULL;
		UT_ASSERT_EQ(sync_calls, 1);
		UT_ASSERT_EQ(error_level, fault ? PANIC : 0);
		if (fault) {
			UT_ASSERT_EQ(error_errno, EIO);
			UT_ASSERT_EQ(slru_errcause, SLRU_FSYNC_FAILED);
		}
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	fixture_wait_child(pid);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT_EQ(CheckpointerShmem->num_backend_fsync, 1);
	UT_ASSERT(fixture_exists(0));
	if (!fault) {
		fixture_delete(false);
		UT_ASSERT(fixture_process_sync());
		UT_ASSERT_EQ(sync_calls, 1);
	}
}
UT_TEST(full_queue_uses_original_fsync_fallback)
{
	full_queue_case(false);
}
UT_TEST(full_queue_fsync_eio_remains_fatal)
{
	full_queue_case(true);
}
UT_TEST(wrapped_old_segment_keeps_current_parent)
{
	int last_page = (int)(UINT32_MAX / SUBTRANS_XACTS_PER_PAGE);
	int current_page = 2 * SLRU_PAGES_PER_SEGMENT;
	fixture_wait_child(fixture_backend(last_page, B_BACKEND, false));
	fixture_wait_child(fixture_backend(current_page, B_BACKEND, false));
	TruncateSUBTRANS(current_page * SUBTRANS_XACTS_PER_PAGE + 11);
	UT_ASSERT(!fixture_exists(last_page / SLRU_PAGES_PER_SEGMENT));
	UT_ASSERT(fixture_exists(2));
	UT_ASSERT_EQ(SubTransGetParent(current_page * SUBTRANS_XACTS_PER_PAGE + 10), 123);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
static void
allocation_fault_case(bool critical)
{
	sigjmp_buf local;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	if (critical) {
		pid_t pid;
		fflush(NULL);
		pid = fork();
		if (pid < 0)
			fixture_panic();
		if (pid == 0) {
			FileTag tag;
			pendingOps = NULL;
			MyAuxProcType = NotAnAuxProcess;
			MyBackendType = B_BACKEND;
			for (int i = 1; i < 512; i++) {
				INIT_SLRUFILETAG(tag, SYNC_HANDLER_CLUSTER_SUBTRANS, i);
				UT_ASSERT(RegisterSyncRequest(&tag, SYNC_REQUEST, false));
			}
			fflush(NULL);
			_exit(ut_current_failed ? 1 : 0);
		}
		fixture_wait_child(pid);
	}
	fail_copy_alloc = !critical;
	fail_critical_alloc = critical;
	PG_exception_stack = &local;
	catch_error = true;
	if (sigsetjmp(local, 1) == 0)
		fixture_delete(false);
	catch_error = false;
	PG_exception_stack = NULL;
	UT_ASSERT_EQ(error_level, critical ? PANIC : ERROR);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, critical ? 0 : 1);
	UT_ASSERT(fixture_exists(0));
	UT_ASSERT_EQ(CritSectionCount, critical ? 1 : 0);
}
UT_TEST(absorb_copy_failure_does_not_delete_or_clear_queue)
{
	allocation_fault_case(false);
}
UT_TEST(absorb_hash_failure_after_transfer_remains_panic)
{
	allocation_fault_case(true);
}
UT_TEST(nonshared_subtrans_has_no_sync_work)
{
	SubTransCtl->sync_handler = SYNC_HANDLER_NONE;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 0);
	UT_ASSERT(fixture_process_sync());
}

static void
run_case(void (*fn)(void))
{
	pid_t pid;
	int status;
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		alarm(15);
		fixture_setup();
		fn();
		fixture_cleanup();
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}
#define WRAPPED_CASE(name)                                                                         \
	static void run_##name(void)                                                                   \
	{                                                                                              \
		run_case(name);                                                                            \
	}
WRAPPED_CASE(queued_sync_then_checkpointer_forget)
WRAPPED_CASE(queued_sync_then_subtrans_truncate)
WRAPPED_CASE(delete_waits_for_original_writer_to_queue_sync)
WRAPPED_CASE(truncate_waits_for_original_writer_to_queue_sync)
WRAPPED_CASE(same_name_new_segment_is_synced)
WRAPPED_CASE(startup_forget_uses_original_queue_order)
WRAPPED_CASE(retained_parent_is_not_deleted)
WRAPPED_CASE(missing_retained_file_keeps_fsync_failure)
WRAPPED_CASE(non_subtrans_handler_does_not_absorb)
WRAPPED_CASE(delete_waits_for_queued_writer_before_valid)
WRAPPED_CASE(truncate_waits_for_queued_writer_before_valid)
WRAPPED_CASE(delete_drains_dirty_buffer_before_forget)
WRAPPED_CASE(truncate_drains_dirty_buffer_before_forget)
WRAPPED_CASE(full_queue_uses_original_fsync_fallback)
WRAPPED_CASE(full_queue_fsync_eio_remains_fatal)
WRAPPED_CASE(wrapped_old_segment_keeps_current_parent)
WRAPPED_CASE(absorb_copy_failure_does_not_delete_or_clear_queue)
WRAPPED_CASE(absorb_hash_failure_after_transfer_remains_panic)
WRAPPED_CASE(nonshared_subtrans_has_no_sync_work)

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(run_queued_sync_then_checkpointer_forget);
	UT_RUN(run_queued_sync_then_subtrans_truncate);
	UT_RUN(run_delete_waits_for_original_writer_to_queue_sync);
	UT_RUN(run_truncate_waits_for_original_writer_to_queue_sync);
	UT_RUN(run_same_name_new_segment_is_synced);
	UT_RUN(run_startup_forget_uses_original_queue_order);
	UT_RUN(run_retained_parent_is_not_deleted);
	UT_RUN(run_missing_retained_file_keeps_fsync_failure);
	UT_RUN(run_non_subtrans_handler_does_not_absorb);
	UT_RUN(run_delete_waits_for_queued_writer_before_valid);
	UT_RUN(run_truncate_waits_for_queued_writer_before_valid);
	UT_RUN(run_delete_drains_dirty_buffer_before_forget);
	UT_RUN(run_truncate_drains_dirty_buffer_before_forget);
	UT_RUN(run_full_queue_uses_original_fsync_fallback);
	UT_RUN(run_full_queue_fsync_eio_remains_fatal);
	UT_RUN(run_wrapped_old_segment_keeps_current_parent);
	UT_RUN(run_absorb_copy_failure_does_not_delete_or_clear_queue);
	UT_RUN(run_absorb_hash_failure_after_transfer_remains_panic);
	UT_RUN(run_nonshared_subtrans_has_no_sync_work);
	UT_DONE();
	return ut_failed_count != 0;
}
