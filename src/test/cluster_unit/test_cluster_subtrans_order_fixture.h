/*-------------------------------------------------------------------------
 * test_cluster_subtrans_order_fixture.h
 *    Process, memory, error and file fixtures for SUBTRANS ordering tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_order_fixture.h
 * NOTES
 *    PGRAC test-only definitions. Include after the native implementation
 *    bodies and unit_test.h; the race assertions remain in the test source.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_SUBTRANS_ORDER_FIXTURE_H
#define TEST_CLUSTER_SUBTRANS_ORDER_FIXTURE_H

bool cluster_shared_config = true;
char *cluster_shared_data_dir;
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
/* Only the shared-file callback result is a boundary in these native
 * pendingOps tests; the real KO/callback pair has its own unit target. */
static int shared_sync_result, shared_sync_errno;
static unsigned shared_sync_calls;
static int pause_writer[2], resume_writer[2], wait_reader[2];
static int writer_pause;
static bool reader_notify;
static struct {
	LWLock *ptr;
	bool held;
} lock_map[256 + 4 * SUBTRANS_RECOVERY_ORIGINS];
static unsigned lock_count;
static struct {
	void *ptr;
	Size size;
} mappings[8 + SUBTRANS_RECOVERY_ORIGINS];
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
	shared_sync_calls++;
	strlcpy(p, "shared-sync-fixture", MAXPGPATH);
	errno = shared_sync_errno;
	return shared_sync_result;
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
	cluster_shared_data_dir = fixture_path;
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

#endif /* TEST_CLUSTER_SUBTRANS_ORDER_FIXTURE_H */
