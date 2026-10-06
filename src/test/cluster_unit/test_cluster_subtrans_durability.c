/*-------------------------------------------------------------------------
 *
 * test_cluster_subtrans_durability.c
 *    Native SUBTRANS selection and actual SLRU write/sync bodies.
 *
 * Shared-memory allocation, sync-queue delivery and fsync faults are explicit
 * fixture boundaries. File creation, write and readback are real. This is not
 * shared-storage certification, retention authority or live startup admission.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_durability.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "cluster/cluster_guc.h"
#include "storage/fd.h"

static void fixture_lru_init(SlruCtl ctl, const char *name, int slots, int lsn_groups, LWLock *lock,
							 const char *dir, int tranche, SyncRequestHandler handler);
#define SimpleLruInit fixture_lru_init
#include "../../backend/access/transam/subtrans.c"
#undef SimpleLruInit
#include "../../backend/access/transam/slru.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config;
volatile uint32 CritSectionCount;
int pg_file_create_mode = 0600;
static LWLockPadded locks[128];
LWLockPadded *MainLWLockArray = locks;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static SlruSharedData fixture_shared;
static char page[BLCKSZ];
static char *buffers[] = { page };
static unsigned queue_calls, sync_calls, write_calls;
static int transient_fds;
static bool queue_accepts, sync_fails;
static FileTag queued;
static char fixture_dir[MAXPGPATH];

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion %s (%s:%d)\n", condition, file, line);
	abort();
}
bool
errstart(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	abort();
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
	abort();
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}

bool
TransactionIdPrecedes(TransactionId a, TransactionId b)
{
	if (!TransactionIdIsNormal(a) || !TransactionIdIsNormal(b))
		return a < b;
	return (int32)(a - b) < 0;
}
bool
TransactionIdFollowsOrEquals(TransactionId a, TransactionId b)
{
	if (!TransactionIdIsNormal(a) || !TransactionIdIsNormal(b))
		return a >= b;
	return (int32)(a - b) >= 0;
}

static void
fixture_lru_init(SlruCtl ctl, const char *name, int slots, int lsn_groups, LWLock *lock,
				 const char *dir, int tranche, SyncRequestHandler handler)
{
	UT_ASSERT(ctl == SubTransCtl);
	UT_ASSERT(strcmp(name, "Subtrans") == 0 && strcmp(dir, "pg_subtrans") == 0);
	UT_ASSERT_EQ(slots, NUM_SUBTRANS_BUFFERS);
	UT_ASSERT_EQ(lsn_groups, 0);
	UT_ASSERT(lock == SubtransSLRULock && tranche == LWTRANCHE_SUBTRANS_BUFFER);
	ctl->shared = &fixture_shared;
	ctl->sync_handler = handler;
	strlcpy(ctl->Dir, dir, sizeof(ctl->Dir));
	fixture_shared.page_buffer = buffers;
}

int
OpenTransientFile(const char *path, int flags)
{
	int fd = open(path, flags, pg_file_create_mode);
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
	sync_calls++;
	if (sync_fails) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
bool
RegisterSyncRequest(const FileTag *tag, SyncRequestType type, bool retryOnError)
{
	UT_ASSERT_EQ(type, SYNC_REQUEST);
	UT_ASSERT(!retryOnError);
	queue_calls++;
	queued = *tag;
	return queue_accepts;
}
void
pgstat_count_slru_page_written(int index pg_attribute_unused())
{
	write_calls++;
}
void
pgstat_count_slru_flush(int index pg_attribute_unused())
{}
void
XLogFlush(XLogRecPtr ptr pg_attribute_unused())
{
	abort();
}
bool
LWLockHeldByMe(LWLock *lock pg_attribute_unused())
{
	return true;
}

static void
reset_fixture(bool clustered)
{
	UT_ASSERT_EQ(transient_fds, 0);
	memset(&fixture_shared, 0, sizeof(fixture_shared));
	memset(page, 0x52, sizeof(page));
	memset(&queued, 0, sizeof(queued));
	cluster_shared_config = clustered;
	queue_calls = sync_calls = write_calls = 0;
	queue_accepts = true;
	sync_fails = false;
	SUBTRANSShmemInit();
}

UT_TEST(shared_subtrans_write_is_registered_for_sync)
{
	char readback[BLCKSZ];
	int fd;
	reset_fixture(true);
	UT_ASSERT(SubTransCtl->sync_handler != SYNC_HANDLER_NONE);
	UT_ASSERT_EQ(SubTransCtl->sync_handler, SYNC_HANDLER_CLUSTER_SUBTRANS);
	UT_ASSERT(SlruPhysicalWritePage(SubTransCtl, 3 * SLRU_PAGES_PER_SEGMENT, 0, NULL));
	UT_ASSERT_EQ(queue_calls, 1);
	UT_ASSERT_EQ(sync_calls, 0);
	UT_ASSERT_EQ(write_calls, 1);
	UT_ASSERT_EQ(queued.handler, SubTransCtl->sync_handler);
	UT_ASSERT_EQ(queued.segno, 3);
	fd = open("pg_subtrans/0003", O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, readback, sizeof(readback)), sizeof(readback));
	UT_ASSERT(memcmp(readback, page, sizeof(page)) == 0);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(full_queue_requires_synchronous_flush_and_propagates_failure)
{
	reset_fixture(true);
	queue_accepts = false;
	UT_ASSERT(SlruPhysicalWritePage(SubTransCtl, 0, 0, NULL));
	UT_ASSERT_EQ(queue_calls, 1);
	UT_ASSERT_EQ(sync_calls, 1);
	sync_fails = true;
	UT_ASSERT(!SlruPhysicalWritePage(SubTransCtl, 0, 0, NULL));
	UT_ASSERT_EQ(queue_calls, 2);
	UT_ASSERT_EQ(sync_calls, 2);
	UT_ASSERT_EQ(slru_errcause, SLRU_FSYNC_FAILED);
	UT_ASSERT_EQ(slru_errno, EIO);
}

UT_TEST(ordinary_subtrans_keeps_native_nondurable_selection)
{
	reset_fixture(false);
	UT_ASSERT_EQ(SubTransCtl->sync_handler, SYNC_HANDLER_NONE);
	UT_ASSERT(SlruPhysicalWritePage(SubTransCtl, 0, 0, NULL));
	UT_ASSERT_EQ(queue_calls, 0);
	UT_ASSERT_EQ(sync_calls, 0);
}

UT_TEST(queued_consumer_syncs_exact_segment_and_returns_real_errors)
{
	char path[MAXPGPATH];
	reset_fixture(true);
	UT_ASSERT(SlruPhysicalWritePage(SubTransCtl, 3 * SLRU_PAGES_PER_SEGMENT, 0, NULL));
	UT_ASSERT_EQ(subtranssyncfiletag(&queued, path), 0);
	UT_ASSERT(strcmp(path, "pg_subtrans/0003") == 0);
	UT_ASSERT_EQ(sync_calls, 1);
	UT_ASSERT_EQ(transient_fds, 0);
	sync_fails = true;
	UT_ASSERT_EQ(subtranssyncfiletag(&queued, path), -1);
	UT_ASSERT_EQ(errno, EIO);
	UT_ASSERT_EQ(sync_calls, 2);
	UT_ASSERT_EQ(transient_fds, 0);
	queued.segno = 7;
	UT_ASSERT_EQ(subtranssyncfiletag(&queued, path), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(sync_calls, 2);
	UT_ASSERT_EQ(transient_fds, 0);
}

int
main(void)
{
	char original[MAXPGPATH];
	UT_PLAN(4);
	if (!getcwd(original, sizeof(original)))
		return 2;
	strlcpy(fixture_dir, "/tmp/pgrac-subtrans-sync.XXXXXX", sizeof(fixture_dir));
	if (!mkdtemp(fixture_dir) || chdir(fixture_dir) || mkdir("pg_subtrans", 0700))
		return 2;
	UT_RUN(shared_subtrans_write_is_registered_for_sync);
	UT_RUN(full_queue_requires_synchronous_flush_and_propagates_failure);
	UT_RUN(ordinary_subtrans_keeps_native_nondurable_selection);
	UT_RUN(queued_consumer_syncs_exact_segment_and_returns_real_errors);
	/* Exact files created by this test, never a broad cleanup. */
	unlink("pg_subtrans/0000");
	unlink("pg_subtrans/0003");
	rmdir("pg_subtrans");
	if (chdir(original) || rmdir(fixture_dir))
		return 2;
	UT_DONE();
	return ut_failed_count != 0;
}
