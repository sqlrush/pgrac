/* Native step storage transport leaf. Actual namespace/FD implementation is
 * tested separately in test_cluster_shared_fs_page, using real named files.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/storage/cluster_shared_fs_page.h"

static bool ctrc_page_fd_enabled, ctrc_page_fd_live;
static unsigned ctrc_page_fd_fault, ctrc_page_fd_opens, ctrc_page_fd_closes;
static bool ctrc_page_fd_invalid;
static ResourceOwner ctrc_page_fd_owner;
static int ctrc_page_fd_pid;
static MemoryContext ctrc_page_fd_context;
static void (*ctrc_page_fd_read_transport)(SMgrRelation, ForkNumber, BlockNumber, void *) = smgrread;

static void
ctrc_page_fd_count_lock(LWLock *lock, LWLockMode mode, void *context)
{
    (void)lock; (void)mode; (*(unsigned *)context)++;
}

Size
cluster_shared_fs_page_bytes_v1(void)
{
	return 16384; /* bounded transport fixture charge, not the product layout */
}

ClusterSharedFsPageV1 *
cluster_shared_fs_page_open_v1(RelFileLocator locator, BlockNumber block, Size memory_budget)
{
	unsigned held = 0;

	UT_ASSERT(!ctrc_page_fd_live);
	ForEachLWLockHeldByMe(ctrc_page_fd_count_lock, &held);
	UT_ASSERT_EQ(held, 0);
	UT_ASSERT_EQ(block, 7);
	UT_ASSERT(ctrc_read_native_live != NULL && memory_budget >= cluster_shared_fs_page_bytes_v1());
	UT_ASSERT(memcmp(&locator, &ctrc_read_native_live->space.key.locator, sizeof(locator)) == 0);
	ctrc_page_fd_opens++;
	ctrc_page_fd_context = CurrentMemoryContext;
	if (ctrc_page_fd_fault == 8) pg_re_throw();
	if (!ctrc_page_fd_enabled || ctrc_page_fd_fault == 1) return NULL;
	ctrc_page_fd_owner = CurrentResourceOwner; ctrc_page_fd_pid = MyProcPid;
	ctrc_page_fd_live = true;
	return (void *)&ctrc_page_fd_live;
}

bool
cluster_shared_fs_page_current_v1(ClusterSharedFsPageV1 *target)
{
	return target == (void *)&ctrc_page_fd_live && ctrc_page_fd_live && !ctrc_page_fd_invalid
		&& CurrentResourceOwner == ctrc_page_fd_owner && MyProcPid == ctrc_page_fd_pid;
}

bool
cluster_shared_fs_page_matches_v1(ClusterSharedFsPageV1 *target,
	RelFileLocator locator, BlockNumber block)
{
	return cluster_shared_fs_page_current_v1(target) && block == 7
		&& RelFileLocatorEquals(locator, ctrc_read_native_live->space.key.locator);
}

bool
cluster_shared_fs_page_read_v1(ClusterSharedFsPageV1 *target, ForkNumber fork, BlockNumber block, char *out)
{
	UT_ASSERT(cluster_shared_fs_page_current_v1(target));
	if (ctrc_page_fd_fault == 5) pg_re_throw();
	ctrc_page_fd_read_transport(&ctrc_read_native_relation, fork, block, out);
	return cluster_shared_fs_page_current_v1(target);
}

bool
cluster_shared_fs_page_write_v1(ClusterSharedFsPageV1 *target, const char *image)
{
	UT_ASSERT(cluster_shared_fs_page_current_v1(target));
	if (ctrc_page_fd_fault == 3) return false;
	smgrwrite(&ctrc_read_native_relation, MAIN_FORKNUM, 7, image, false);
	return cluster_shared_fs_page_current_v1(target);
}

bool
cluster_shared_fs_page_sync_v1(ClusterSharedFsPageV1 *target)
{
	UT_ASSERT(cluster_shared_fs_page_current_v1(target));
	if (ctrc_page_fd_fault == 4) return false;
	if (ctrc_page_fd_fault == 6) pg_re_throw();
	smgrimmedsync(&ctrc_read_native_relation, MAIN_FORKNUM);
	return cluster_shared_fs_page_current_v1(target);
}

bool
cluster_shared_fs_page_close_v1(ClusterSharedFsPageV1 *target)
{
	if (target == NULL) return true;
	UT_ASSERT(target == (void *)&ctrc_page_fd_live && ctrc_page_fd_live);
	ctrc_page_fd_live = false; ctrc_page_fd_closes++;
	return ctrc_page_fd_fault != 7;
}
