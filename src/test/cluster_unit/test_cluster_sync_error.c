/* Native FlushBuffer -> smgr dirty registration -> provider sync polarity.
 * Mapping, grants and sync-queue capacity are explicit unit boundaries. Real
 * first slots, full FlushBuffer and original provider functions execute here.
 * PANIC exits the child before PG_CATCH; the backend TAP covers native elog.
 * Author: SqlRush <sqlrush@gmail.com> */
int page_data_fixture_main(void);
#define PGRAC_TEST_SYNC_ERROR
#define main page_data_fixture_main
#include "test_cluster_page_data.c"
#undef main

#include <errno.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/cluster_mrp.h"
#include "storage/fd.h"
#include "storage/sync.h"

struct ClusterSharedFsHandle {
	File vfd;
	bool opened;
};
typedef struct ClusterSmgrRelationState {
	int unused;
} ClusterSmgrRelationState;
static ClusterSmgrRelationState relation_state;
static ClusterSharedFsHandle native_handle;
static ClusterSharedFsOps ops;
static const ClusterSharedFsOps *cluster_shared_fs_active_ops = &ops;
static int cluster_raw_device_fd;
int io_direct_flags;
static bool queue_accepts, fail_sync, fail_write;
static int reported_level;
static ClusterPageWalRefV1 first_before;
static struct {
	int level, caught, completed, queued, synced, first_unchanged, dirty;
} *observed;

static bool
first_unchanged(void)
{
	ClusterPageWalRefV1 current = { 0 };
	BufferDesc *buf = &descriptors[1].bufferdesc;
	uint32 state = LockBufHdr(buf);
	ClusterPageWalFirstResultV1 result = cluster_page_wal_first_observe_locked_v1(buf, &current);
	UnlockBufHdr(buf, state);
	return result == CLUSTER_PAGE_WAL_FIRST_PRESENT
		   && memcmp(&current, &first_before, sizeof(current)) == 0;
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	reported_level = level;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
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
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	observed->level = reported_level;
	observed->first_unchanged = first_unchanged();
	observed->dirty = (pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY) != 0;
	if (reported_level >= PANIC)
		_exit(86);
	if (reported_level >= ERROR)
		pg_re_throw();
}

int
FileWrite(File fd, const void *data, size_t amount, off_t offset,
		  uint32 event pg_attribute_unused())
{
	if (fail_write) {
		errno = EIO;
		return -1;
	}
	writes++;
	return pwrite(fd, data, amount, offset);
}
int
pg_fsync(int fd)
{
	observed->synced++;
	if (fail_sync) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
int
FileSync(File fd, uint32 event pg_attribute_unused())
{
	return pg_fsync(fd);
}

bool
RegisterSyncRequest(const FileTag *tag, SyncRequestType type, bool retry)
{
	UT_ASSERT_EQ(tag->handler, SYNC_HANDLER_CLUSTER_SHARED);
	UT_ASSERT_EQ(type, SYNC_REQUEST);
	UT_ASSERT(!retry);
	UT_ASSERT(first_unchanged());
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	observed->queued++;
	return queue_accepts;
}
void
cluster_write_fence_reject_if_fenced(const char *op pg_attribute_unused())
{}
void
cluster_mrp_standby_shared_write_gate(const char *op pg_attribute_unused())
{}
static ClusterSmgrRelationState *
cluster_smgr_state_lookup(SMgrRelation r, bool create)
{
	UT_ASSERT(r == &relation && create);
	return &relation_state;
}
static ClusterSharedFsHandle *
cluster_smgr_ensure_handle(ClusterSmgrRelationState *state, ForkNumber fork)
{
	UT_ASSERT(state == &relation_state && fork == MAIN_FORKNUM);
	return &native_handle;
}

#include "test_cluster_data_sync_policy.inc"
#include "test_cluster_sync_providers.inc"
#define ENSURE_ACTIVE() Assert(cluster_shared_fs_active_ops != NULL)
#include "test_cluster_sync_dispatch.inc"
#undef ENSURE_ACTIVE
#include "test_cluster_sync_smgr.inc"

static void
setup(int provider)
{
	BufferDesc *buf;
	uint32 state;
	reset();
	memset(observed, 0, sizeof(*observed));
	memset(&first_before, 0, sizeof(first_before));
	relation.smgr_rlocator.backend = InvalidBackendId;
	native_handle.vfd = cluster_raw_device_fd = fileno(file);
	native_handle.opened = true;
	ops.write = provider == 1 ? cluster_shared_fs_sharedfs_write : cluster_shared_fs_local_write;
	/* Raw layout/data placement is not under test here; its complete provider
	 * is exercised separately by test_cluster_shared_fs_block_device. */
	ops.immedsync = provider == 0	? cluster_shared_fs_local_immedsync
					: provider == 1 ? cluster_shared_fs_sharedfs_immedsync
									: cluster_shared_fs_block_device_immedsync;
	ops.barrier_sync = provider == 0   ? cluster_shared_fs_local_barrier_sync
					   : provider == 1 ? cluster_shared_fs_sharedfs_barrier_sync
									   : cluster_shared_fs_block_device_barrier_sync;
	storage_write_hook = cluster_smgr_write;
	queue_accepts = fail_sync = fail_write = data_sync_retry = false;
	buf = &descriptors[1].bufferdesc;
	pins[1] = 1;
	locks[0] = locks[1] = true;
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_page_wal_first_observe_locked_v1(buf, &first_before),
				 CLUSTER_PAGE_WAL_FIRST_PRESENT);
	UnlockBufHdr(buf, state);
}

static bool
flush(void)
{
	volatile bool caught = false;
	PG_TRY();
	{
		FlushBuffer(&descriptors[1].bufferdesc, &relation, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
		observed->completed++;
	}
	PG_CATCH();
	{
		caught = true;
		observed->caught++;
		AbortBufferIO(2);
		error_context_stack = NULL;
	}
	PG_END_TRY();
	return !caught;
}

UT_TEST(sync_error_bypasses_retry_and_retains_first)
{
	for (int provider = 0; provider < 3; provider++) {
		pid_t child;
		int status;
		setup(provider);
		fail_sync = true;
		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			(void)flush();
			/* A later successful fsync must never be reached after default PANIC. */
			fail_sync = false;
			(void)flush();
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 86);
		UT_ASSERT_EQ(observed->level, PANIC);
		UT_ASSERT_EQ(observed->caught + observed->completed, 0);
		UT_ASSERT_EQ(observed->queued, 1);
		UT_ASSERT_EQ(observed->synced, 1);
		UT_ASSERT(observed->first_unchanged && observed->dirty);
	}
}

UT_TEST(registration_or_sync_precedes_clean_and_first_retirement)
{
	for (int provider = 0; provider < 3; provider++)
		for (int queued = 0; queued < 2; queued++) {
			setup(provider);
			queue_accepts = queued;
			UT_ASSERT(flush());
			UT_ASSERT_EQ(observed->queued, 1);
			UT_ASSERT_EQ(observed->synced, !queued);
			UT_ASSERT(!first_unchanged());
			UT_ASSERT(!(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY));
		}
}

UT_TEST(write_error_remains_error_and_sync_retry_uses_native_policy)
{
	for (int provider = 0; provider < 3; provider++)
		for (int sync = 0; sync < 2; sync++) {
			setup(provider);
			fail_write = !sync;
			fail_sync = data_sync_retry = sync;
			UT_ASSERT(!flush());
			UT_ASSERT_EQ(observed->level, ERROR);
			UT_ASSERT(observed->first_unchanged && observed->dirty);
			UT_ASSERT_EQ(observed->queued, sync);
			UT_ASSERT_EQ(observed->synced, sync);
			fail_write = fail_sync = false;
			UT_ASSERT(flush());
			UT_ASSERT(!first_unchanged());
		}
}

int
main(void)
{
	observed = mmap(NULL, sizeof(*observed), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	if (observed == MAP_FAILED)
		return 2;
	UT_PLAN(3);
	UT_RUN(sync_error_bypasses_retry_and_retains_first);
	UT_RUN(registration_or_sync_precedes_clean_and_first_retirement);
	UT_RUN(write_error_remains_error_and_sync_retry_uses_native_policy);
	munmap(observed, sizeof(*observed));
	if (file)
		fclose(file);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
