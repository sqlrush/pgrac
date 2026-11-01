/* Author: SqlRush <sqlrush@gmail.com>
 * Actual SLRU writer with temporary files; lock scheduling/WAL/sync queue are
 * finite test boundaries. No first-image WAL or recovery qualification. */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>

static ssize_t fixture_pwrite(int fd, const void *bytes, size_t length, off_t offset);
#undef pg_pwrite
#define pg_pwrite fixture_pwrite
#ifdef SLRU_TEST_SOURCE
#include SLRU_TEST_SOURCE
#else
#include "../../backend/access/transam/slru.c"
#endif
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static SlruCtlData ctl;
static SlruSharedData shared;
static LWLock control;
static LWLockPadded locks[1];
static PGAlignedBlock page, disk;
static char *buffers[1];
static SlruPageStatus states[1];
static bool dirty[1];
static int numbers[1], ages[1];
static XLogRecPtr lsns[2], flushed;
static ClusterClogPageIdentityV2 identities[1];
bool IsUnderPostmaster;
static void *allocated_shared;
static Size allocated_size;
static bool control_held, io_held, inject_update, fail_write, short_write, fail_sync;
static bool fixture_io_wait_mode;
static void (*fixture_io_wait) (void);
static bool queue_full;
static unsigned writes, flushes, closes, syncs, queues, stats_writes;
static FileTag queued_tag;
static char directory[MAXPGPATH], filename[MAXPGPATH], events[64];
static unsigned event_count;
static off_t written_offset;
static size_t written_length;
static int error_code;

bool InRecovery;
void LWLockInitialize(LWLock *lock pg_attribute_unused(), int id pg_attribute_unused()) {}
int pgstat_get_slru_index(const char *name pg_attribute_unused()) { return 0; }
void *ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	if (IsUnderPostmaster) { *found = true; return allocated_shared; }
	*found = false; allocated_size = size;
	allocated_shared = calloc(1, size);
	if (allocated_shared == NULL) abort();
	return allocated_shared;
}
void pgstat_count_slru_page_zeroed(int i pg_attribute_unused()) {}
void pgstat_count_slru_page_hit(int i pg_attribute_unused()) {}
void pgstat_count_slru_page_read(int i pg_attribute_unused()) {}
void pgstat_count_slru_page_exists(int i pg_attribute_unused()) {}
volatile uint32 CritSectionCount;
volatile uint32 InterruptHoldoffCount;
volatile sig_atomic_t InterruptPending;
uint32 *my_wait_event_info;
static uint32 wait_event;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
CheckpointStatsData CheckpointStats;

static void
record_event(char c)
{
	if (event_count >= sizeof(events)) abort();
	events[event_count++] = c;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(mode == LW_EXCLUSIVE || mode == LW_SHARED);
	if (lock == &control) {
		UT_ASSERT(!control_held);
		control_held = true;
	} else {
		UT_ASSERT(lock == &locks[0].lock);
		if (mode == LW_SHARED) {
			UT_ASSERT(!control_held && !io_held && fixture_io_wait != NULL);
			if (fixture_io_wait == NULL) abort();
			fixture_io_wait();
			fixture_io_wait_mode = true;
		} else UT_ASSERT(control_held && !io_held);
		io_held = true;
	}
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == &control) {
		UT_ASSERT(control_held);
		control_held = false;
		if (inject_update && io_held) {
			/* One other updater owns the original control lock in this gap. */
			inject_update = false;
			UT_ASSERT(io_held);
			UT_ASSERT_EQ(states[0], SLRU_PAGE_WRITE_IN_PROGRESS);
			UT_ASSERT(!dirty[0]);
			control_held = true;
			memset(page.data, 0x42, BLCKSZ);
			lsns[0] = 30;
			lsns[1] = 40;
			dirty[0] = true;
			control_held = false;
		}
	} else {
		UT_ASSERT(lock == &locks[0].lock);
		UT_ASSERT(io_held && (fixture_io_wait_mode ? !control_held : control_held));
		fixture_io_wait_mode = false;
		io_held = false;
	}
}

bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == &control ? control_held : io_held;
}
bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	/* No competing I/O owner is scheduled by these cases. */
	abort();
}

void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT(!control_held && io_held);
	UT_ASSERT_EQ(CritSectionCount, 1);
	flushed = lsn;
	flushes++;
	record_event('F');
}

#ifndef SLRU_FIXTURE_OPEN
#define SLRU_FIXTURE_OPEN OpenTransientFile
#endif
int
SLRU_FIXTURE_OPEN(const char *path, int flags)
{
	UT_ASSERT(!control_held && io_held);
	record_event('O');
	return open(path, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	closes++;
	return close(fd);
}
static ssize_t
fixture_pwrite(int fd, const void *bytes, size_t length, off_t offset)
{
	UT_ASSERT(!control_held && io_held);
	writes++;
	written_offset = offset;
	written_length = length;
	record_event('W');
	if (fail_write) { errno = ENOSPC; return -1; }
	return pwrite(fd, bytes, short_write ? length - 1 : length, offset);
}

bool
RegisterSyncRequest(const FileTag *tag, SyncRequestType type, bool retry)
{
	UT_ASSERT(!control_held && io_held);
	UT_ASSERT_EQ(type, SYNC_REQUEST);
	UT_ASSERT(!retry);
	queued_tag = *tag;
	queues++;
	record_event('Q');
	return !queue_full;
}
int
pg_fsync(int fd)
{
	UT_ASSERT(!control_held && io_held);
	syncs++;
	record_event('S');
	if (fail_sync) { errno = EIO; return -1; }
	return fsync(fd);
}
int
data_sync_elevel(int level) { return level; }
void
pgstat_count_slru_page_written(int index pg_attribute_unused()) { stats_writes++; }

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	UT_ASSERT(level >= ERROR);
	UT_ASSERT(control_held && !io_held);
	return true;
}
bool
errstart_cold(int level, const char *domain) { return errstart(level, domain); }
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}
int
errcode(int code) { error_code = code; return 0; }
int
errcode_for_file_access(void) { return errcode(ERRCODE_IO_ERROR); }
int
errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...) { return 0; }
int
errdetail(const char *fmt pg_attribute_unused(), ...) { return 0; }
void
ProcessInterrupts(void) { abort(); }

static void
reset(SyncRequestHandler handler)
{
	memset(&ctl, 0, sizeof(ctl));
	memset(&shared, 0, sizeof(shared));
	memset(&CheckpointStats, 0, sizeof(CheckpointStats));
	memset(page.data, 0x41, BLCKSZ);
	buffers[0] = page.data;
	states[0] = SLRU_PAGE_VALID;
	dirty[0] = true;
	numbers[0] = 3;
	ages[0] = 0;
	lsns[0] = 20;
	lsns[1] = 10;
	shared.ControlLock = &control;
	shared.num_slots = 1;
	shared.page_buffer = buffers;
	shared.page_status = states;
	shared.page_dirty = dirty;
	shared.page_number = numbers;
	shared.page_lru_count = ages;
	shared.buffer_locks = locks;
	shared.group_lsn = lsns;
	memset(identities, 0, sizeof(identities));
	shared.clog_page_identity = identities;
	shared.lsn_groups_per_page = 2;
	ctl.shared = &shared;
	ctl.sync_handler = handler;
	strlcpy(ctl.Dir, directory, sizeof(ctl.Dir));
	control_held = true;
	io_held = inject_update = fail_write = short_write = fail_sync = queue_full = false;
	fixture_io_wait_mode = false; fixture_io_wait = NULL;
	writes = flushes = closes = syncs = queues = stats_writes = event_count = 0;
	written_offset = -1;
	written_length = 0;
	flushed = 0;
	error_code = 0;
	CritSectionCount = InterruptHoldoffCount = 0;
	my_wait_event_info = &wait_event;
	unlink(filename);
}

static void
assert_disk(uint8 value)
{
	PGAlignedBlock expected;
	int fd = open(filename, O_RDONLY);
	ssize_t got;
	if (fd < 0) abort();
	got = pread(fd, disk.data, BLCKSZ, 3 * BLCKSZ);
	close(fd);
	UT_ASSERT_EQ(got, BLCKSZ);
	memset(expected.data, value, BLCKSZ);
	UT_ASSERT(memcmp(disk.data, expected.data, BLCKSZ) == 0);
}

static bool
write_raises(SlruWriteAll fdata)
{
	sigjmp_buf stack;
	bool caught = false;
	PG_exception_stack = &stack;
	if (sigsetjmp(stack, 0) == 0) SlruInternalWritePage(&ctl, 0, fdata);
	else caught = true;
	PG_exception_stack = NULL;
	return caught;
}

UT_TEST(clog_snapshot_precedes_unlock_and_preserves_redirty)
{
	reset(SYNC_HANDLER_CLOG);
	inject_update = true;
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT_EQ(flushed, 20);
	assert_disk(0x41);
	UT_ASSERT(control_held && !io_held && dirty[0]);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
	UT_ASSERT_EQ(lsns[0], 30);
	UT_ASSERT_EQ(lsns[1], 40);
	UT_ASSERT_EQ(written_offset, 3 * BLCKSZ);
	UT_ASSERT_EQ(written_length, BLCKSZ);
	UT_ASSERT_EQ(queued_tag.handler, SYNC_HANDLER_CLOG);
	UT_ASSERT_EQ(queued_tag.segno, 0);
	UT_ASSERT(memcmp(events, "FOWQ", 4) == 0);
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT_EQ(flushed, 40);
	assert_disk(0x42);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(flushes, 2);
	UT_ASSERT_EQ(closes, 2);
}

UT_TEST(non_clog_keeps_original_live_buffer_path)
{
	reset(SYNC_HANDLER_NONE);
	inject_update = true;
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT_EQ(flushed, 40);
	assert_disk(0x42);
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(queues, 0);
}

UT_TEST(failed_clog_write_restores_original_state_before_error)
{
	reset(SYNC_HANDLER_CLOG);
	fail_write = true;
	UT_ASSERT(write_raises(NULL));
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(queues, 0);
	UT_ASSERT_EQ(flushed, 20);
	fail_write = false;
	SimpleLruWritePage(&ctl, 0);
	assert_disk(0x41);
	UT_ASSERT(!dirty[0]);
}

UT_TEST(checkpoint_keeps_fd_and_redirty_after_snapshot)
{
	SlruWriteAllData fdata = {0};
	reset(SYNC_HANDLER_CLOG);
	inject_update = true;
	SlruInternalWritePage(&ctl, 0, &fdata);
	UT_ASSERT_EQ(flushed, 20);
	assert_disk(0x41);
	UT_ASSERT_EQ(fdata.num_files, 1);
	UT_ASSERT_EQ(closes, 0);
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(CheckpointStats.ckpt_bufs_written, 1);
	if (fdata.num_files == 1) close(fdata.fd[0]);
}

UT_TEST(short_write_and_sync_failure_keep_retryable_dirty_page)
{
	reset(SYNC_HANDLER_CLOG);
	short_write = true;
	UT_ASSERT(write_raises(NULL));
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(queues, 0);
	reset(SYNC_HANDLER_CLOG);
	queue_full = true;
	fail_sync = true;
	UT_ASSERT(write_raises(NULL));
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(closes, 1);
}

UT_TEST(clean_slot_does_not_flush_or_write)
{
	reset(SYNC_HANDLER_CLOG);
	dirty[0] = false;
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT(control_held && !io_held);
}

static bool
page_precedes(int a, int b) { return a < b; }

UT_TEST(clog_identity_is_private_in_same_control_lock_snapshot)
{
	SlruWriteSnapshot captured;
	ClusterClogPageIdentityV2 before;
	reset(SYNC_HANDLER_CLOG);
	identities[0].identity.logical_page = 3;
	identities[0].identity.page_generation = 77;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 3 * 32768);
	identities[0].identity.birth_observed = true;
	identities[0].identity.page_lsn = 15;
	identities[0].image_lsn = 12;
	before = identities[0];
	SlruCaptureWriteSnapshot(&ctl, 0, &captured);
	UT_ASSERT_EQ(captured.flush_lsn, 20);
	UT_ASSERT_EQ(captured.clog_page.identity.page_lsn, 20);
	UT_ASSERT_EQ(captured.clog_page.image_lsn, 12);
	UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
	memset(page.data, 0x42, BLCKSZ); lsns[0] = 40;
	memset(identities, 0, sizeof(identities));
	UT_ASSERT_EQ((unsigned char)captured.page.data[0], 0x41);
	UT_ASSERT(captured.clog_page.identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(captured.clog_page.identity.birth_full_xid), U64FromFullTransactionId(before.identity.birth_full_xid));
	UT_ASSERT_EQ(captured.clog_page.identity.page_generation, 77);
	UT_ASSERT_EQ(captured.flush_lsn, 20);
	UT_ASSERT_EQ(captured.clog_page.identity.page_lsn, 20);
	/* Decoded watermark may exceed the native groups after reload. */
	identities[0] = before; identities[0].identity.page_lsn = 50;
	SlruCaptureWriteSnapshot(&ctl, 0, &captured);
	UT_ASSERT_EQ(captured.clog_page.identity.page_lsn, 50);
}

UT_TEST(unknown_birth_is_never_reconstructed_from_current_group_lsn)
{
	SlruWriteSnapshot captured;
	reset(SYNC_HANDLER_CLOG);
	SlruCaptureWriteSnapshot(&ctl, 0, &captured);
	UT_ASSERT(!captured.clog_page.identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(captured.clog_page.identity.birth_full_xid), 0);
	shared.clog_page_identity = NULL;
	memset(&captured, 0xff, sizeof(captured));
	SlruCaptureWriteSnapshot(&ctl, 0, &captured);
	UT_ASSERT(!captured.clog_page.identity.birth_observed);
	UT_ASSERT_EQ(captured.clog_page.identity.page_generation, 0);
	UT_ASSERT_EQ(captured.clog_page.image_lsn, 0);
}

UT_TEST(original_zero_and_raw_reload_clear_old_birth)
{
	int fd;
	reset(SYNC_HANDLER_CLOG);
	ctl.PagePrecedes = page_precedes;
	identities[0].identity.birth_observed = true;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64(3);
	UT_ASSERT_EQ(SimpleLruZeroPage(&ctl, 3), 0);
	UT_ASSERT(!identities[0].identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), 0);
	UT_ASSERT_EQ(lsns[0], 0);
	/* Native cache hit keeps original provenance, miss must clear it. */
	identities[0].identity.birth_observed = true;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64(123);
	UT_ASSERT_EQ(SimpleLruReadPage(&ctl, 3, true, 0), 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), 123);
	fd = open(filename, O_CREAT | O_RDWR, 0600);
	if (fd < 0 || ftruncate(fd, 5 * BLCKSZ) != 0) { perror(filename); abort(); }
	close(fd);
	dirty[0] = false; shared.latest_page_number = 99;
	UT_ASSERT_EQ(SimpleLruReadPage(&ctl, 4, true, 0), 0);
	UT_ASSERT(!identities[0].identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), 0);
	UT_ASSERT_EQ(identities[0].identity.page_generation, 0);
	UT_ASSERT_EQ(lsns[0], 0);
	UT_ASSERT(control_held && !io_held);
}

UT_TEST(raw_read_error_forgets_previous_birth_before_unlock)
{
	sigjmp_buf stack;
	volatile bool caught = false;

	reset(SYNC_HANDLER_CLOG);
	ctl.PagePrecedes = page_precedes;
	dirty[0] = false; shared.latest_page_number = 99;
	identities[0].identity.birth_observed = true;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64(123);
	PG_exception_stack = &stack;
	if (sigsetjmp(stack, 0) == 0) (void) SimpleLruReadPage(&ctl, 4, true, 0);
	else caught = true;
	PG_exception_stack = NULL;
	UT_ASSERT(caught);
	UT_ASSERT(!identities[0].identity.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(identities[0].identity.birth_full_xid), 0);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY);
	UT_ASSERT(control_held && !io_held && !dirty[0]);
}

UT_TEST(shmem_initializes_optional_identity_array_and_preserves_attachment)
{
	SlruCtlData initialized = {0}, attached = {0};
	for (unsigned groups = 0; groups <= 2; groups += 2) {
		IsUnderPostmaster = false;
		SimpleLruInit(&initialized, "test", 3, groups, &control, directory, 0,
			groups ? SYNC_HANDLER_CLOG : SYNC_HANDLER_NONE);
		UT_ASSERT_EQ(allocated_size, SimpleLruShmemSize(3, groups));
		UT_ASSERT_EQ(initialized.shared->num_slots, 3);
		UT_ASSERT((initialized.shared->clog_page_identity != NULL) == (groups != 0));
		if (groups && initialized.shared->clog_page_identity != NULL) {
			ClusterClogPageIdentityV2 zero = {0};
			for (unsigned i = 0; i < 3; i++)
				UT_ASSERT(memcmp(&zero, &initialized.shared->clog_page_identity[i], sizeof(zero)) == 0);
			UT_ASSERT((char *)(initialized.shared->clog_page_identity + 3) <= initialized.shared->page_buffer[0]);
			initialized.shared->clog_page_identity[2].identity.birth_full_xid = FullTransactionIdFromU64(123);
		} else UT_ASSERT(initialized.shared->clog_page_identity == NULL);
		IsUnderPostmaster = true;
		SimpleLruInit(&attached, "test", 3, groups, &control, directory, 0,
			groups ? SYNC_HANDLER_CLOG : SYNC_HANDLER_NONE);
		UT_ASSERT(attached.shared == initialized.shared);
		if (groups && attached.shared->clog_page_identity != NULL)
			UT_ASSERT_EQ(U64FromFullTransactionId(attached.shared->clog_page_identity[2].identity.birth_full_xid), 123);
		free(allocated_shared); allocated_shared = NULL;
	}
	IsUnderPostmaster = false;
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-snapshot-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(11);
	UT_RUN(clog_snapshot_precedes_unlock_and_preserves_redirty);
	UT_RUN(non_clog_keeps_original_live_buffer_path);
	UT_RUN(failed_clog_write_restores_original_state_before_error);
	UT_RUN(checkpoint_keeps_fd_and_redirty_after_snapshot);
	UT_RUN(short_write_and_sync_failure_keep_retryable_dirty_page);
	UT_RUN(clean_slot_does_not_flush_or_write);
	UT_RUN(clog_identity_is_private_in_same_control_lock_snapshot);
	UT_RUN(unknown_birth_is_never_reconstructed_from_current_group_lsn);
	UT_RUN(original_zero_and_raw_reload_clear_old_birth);
	UT_RUN(raw_read_error_forgets_previous_birth_before_unlock);
	UT_RUN(shmem_initializes_optional_identity_array_and_preserves_attachment);
	unlink(filename);
	rmdir(directory);
	UT_DONE();
	return ut_failed_count != 0;
}
