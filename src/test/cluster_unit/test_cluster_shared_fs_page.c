/* Actual held PAGE descriptors and POSIX namespace, no server.
 * Author: SqlRush <sqlrush@gmail.com> */
int sharedfs_page_old_main(void);
#define main sharedfs_page_old_main
#define AcquireExternalFD page_unused_acquire
#define ReleaseExternalFD page_unused_release
#include "test_cluster_shared_fs_sharedfs.c"
#undef main
#undef AcquireExternalFD
#undef ReleaseExternalFD

#include "cluster/storage/cluster_shared_fs_page.h"
#include "utils/resowner.h"
#include <setjmp.h>
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_write_fence.h"

extern bool AcquireExternalFD(void);
extern void ReleaseExternalFD(void);

ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
int MyProcPid = 12345;
static unsigned page_external, page_limit = 100, page_writes, page_syncs;
static int page_fault;
static bool page_drift;
static sigjmp_buf page_error_jump;
static bool page_error_expected;
static int page_error_level;
static bool page_backend = true;
static const ClusterSharedFsOps page_ops = {.id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS};
static RelFileLocator page_locator = {1663, 5, 16384};
static char page_main[MAXPGPATH], page_space[MAXPGPATH], page_parent[MAXPGPATH];

const ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return page_backend ? &page_ops : NULL;
}

bool
AcquireExternalFD(void)
{
	if (page_external >= page_limit) { errno = EMFILE; return false; }
	page_external++; return true;
}

void
ReleaseExternalFD(void)
{
	if (page_external == 0) abort();
	page_external--;
}

void
cluster_write_fence_reject_if_fenced(const char *op)
{
	if (strcmp(op, "write") != 0) abort();
	page_writes++;
}

void
cluster_mrp_standby_shared_write_gate(const char *op)
{
	if (strcmp(op, "write") != 0 || page_writes == 0) abort();
}

static ssize_t
page_pread(int fd, void *out, size_t size, off_t offset)
{
	ssize_t count;
	char moved[MAXPGPATH];

	if (page_fault == 1) { errno = EIO; return -1; }
	count = pread(fd, out, size, offset);
	if (page_fault == 4 && !page_drift) {
		page_drift = true;
		snprintf(moved, sizeof(moved), "%s.old", page_main);
		if (rename(page_main, moved) != 0 || link(moved, page_main) != 0) abort();
	}
	return count;
}

static ssize_t
page_pwrite(int fd, const void *image, size_t size, off_t offset)
{
	if (page_fault == 2) { errno = ENOSPC; return -1; }
	return pwrite(fd, image, size, offset);
}

static int
page_fsync(int fd)
{
	page_syncs++;
	if (page_fault == 3) { errno = EIO; return -1; }
	return fsync(fd);
}

static bool
page_error_start(int level, const char *domain)
{
	if (!page_error_expected) return errstart(level, domain);
	page_error_level = level;
	siglongjmp(page_error_jump, 1);
	return false;
}

#undef pg_pread
#undef pg_pwrite
#define pg_pread page_pread
#define pg_pwrite page_pwrite
#define pg_fsync page_fsync
#define errstart page_error_start
#define errstart_cold page_error_start
#include "../../backend/cluster/storage/cluster_shared_fs_page.c"
#undef errstart
#undef errstart_cold
#undef pg_pread
#undef pg_pwrite
#undef pg_fsync

static void
page_file(const char *path, unsigned blocks)
{
	PGAlignedBlock image;
	int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);

	if (fd < 0) abort();
	for (unsigned i = 0; i < blocks; i++) {
		memset(image.data, 0x31 + i, BLCKSZ);
		if (write(fd, image.data, BLCKSZ) != BLCKSZ) abort();
	}
	if (close(fd) != 0) abort();
}

static void
page_setup(const char *tag)
{
	if (page_external != 0) abort();
	{
		char unique[80]; static unsigned sequence;
		snprintf(unique, sizeof(unique), "%s_%u", tag, sequence++); fresh_root(unique);
	}
	snprintf(page_parent, sizeof(page_parent), "%s/base/5", test_root);
	if (pg_mkdir_p(page_parent, 0700) != 0) abort();
	snprintf(page_main, sizeof(page_main), "%s/16384", page_parent);
	snprintf(page_space, sizeof(page_space), "%s/16384_space", page_parent);
	page_file(page_main, 3); page_file(page_space, 2);
	page_fault = 0; page_limit = 100; page_backend = true;
	page_drift = false;
	page_error_expected = false; page_error_level = 0; data_sync_retry = false;
	page_writes = page_syncs = 0; enableFsync = true; io_direct_flags = 0;
	MyProcPid = 12345; CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
}

static ClusterSharedFsPageV1 *
page_open(void)
{
	return cluster_shared_fs_page_open_v1(page_locator, 1, cluster_shared_fs_page_bytes_v1());
}

UT_TEST(test_page_fd_existing_extent_roundtrip_and_explicit_close)
{
	ClusterSharedFsPageV1 *target;
	PGAlignedBlock image, out;
	struct stat before, after;

	page_setup("page_fd_ok");
	UT_ASSERT_EQ(stat(page_main, &before), 0);
	target = page_open(); UT_ASSERT_NOT_NULL(target);
	UT_ASSERT_EQ(page_external, 5); /* root/base/db + MAIN + SPACE */
	UT_ASSERT(cluster_shared_fs_page_read_v1(target, SPACE_FORKNUM, 0, out.data));
	UT_ASSERT_EQ((unsigned char)out.data[0], 0x31);
	UT_ASSERT(cluster_shared_fs_page_read_v1(target, SPACE_FORKNUM, 1, out.data));
	UT_ASSERT_EQ((unsigned char)out.data[0], 0x32);
	memset(image.data, 0x73, BLCKSZ);
	UT_ASSERT(cluster_shared_fs_page_write_v1(target, image.data));
	UT_ASSERT_EQ(page_writes, 1);
	UT_ASSERT(cluster_shared_fs_page_sync_v1(target));
	UT_ASSERT(cluster_shared_fs_page_read_v1(target, MAIN_FORKNUM, 1, out.data));
	UT_ASSERT(memcmp(image.data, out.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(stat(page_main, &after), 0);
	UT_ASSERT_EQ(after.st_ino, before.st_ino); UT_ASSERT_EQ(after.st_size, before.st_size);
	UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
}

UT_TEST(test_page_fd_replacement_and_ancestor_drift_stay_refused)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterSharedFsPageV1 *target;
		PGAlignedBlock out;
		char moved[MAXPGPATH];
		const char *path;

		page_setup("page_fd_replace"); target = page_open(); UT_ASSERT_NOT_NULL(target);
		path = variant == 0 ? page_main : variant == 1 ? page_space : page_parent;
		snprintf(moved, sizeof(moved), "%s.old", path);
		UT_ASSERT_EQ(rename(path, moved), 0);
		if (variant < 2) page_file(path, variant == 0 ? 3 : 2);
		else if (variant == 2) UT_ASSERT_EQ(mkdir(path, 0700), 0);
		else UT_ASSERT_EQ(symlink(moved, path), 0);
		UT_ASSERT(!cluster_shared_fs_page_current_v1(target));
		UT_ASSERT(!cluster_shared_fs_page_read_v1(target, MAIN_FORKNUM, 1, out.data));
		UT_ASSERT(!cluster_shared_fs_page_write_v1(target, out.data));
		UT_ASSERT_EQ(page_writes, 0);
		UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
	}
}

UT_TEST(test_page_fd_extent_actor_and_owner_drift_stay_refused)
{
	for (unsigned variant = 0; variant < 6; variant++) {
		ClusterSharedFsPageV1 *target;

		page_setup("page_fd_drift"); target = page_open(); UT_ASSERT_NOT_NULL(target);
		if (variant < 3) UT_ASSERT_EQ(truncate(variant == 2 ? page_space : page_main,
			variant == 0 ? BLCKSZ : 4 * BLCKSZ), 0);
		else if (variant == 3) MyProcPid++;
		else if (variant == 4) CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
		else enableFsync = false;
		UT_ASSERT(!cluster_shared_fs_page_current_v1(target));
		MyProcPid = 12345; CurrentResourceOwner = (ResourceOwner)(uintptr_t)1; enableFsync = true;
		UT_ASSERT(!cluster_shared_fs_page_current_v1(target)); /* no rebase */
		UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
	}
}

UT_TEST(test_page_fd_budget_missing_symlink_hardlink_and_partial_fd_cleanup)
{
	char extra[MAXPGPATH];

	page_setup("page_fd_limits");
	UT_ASSERT_NULL(cluster_shared_fs_page_open_v1(page_locator, 1, cluster_shared_fs_page_bytes_v1() - 1));
	UT_ASSERT_NULL(cluster_shared_fs_page_open_v1(page_locator, 3, cluster_shared_fs_page_bytes_v1()));
	for (unsigned limit = 0; limit < 5; limit++) {
		page_limit = limit; UT_ASSERT_NULL(page_open()); UT_ASSERT_EQ(page_external, 0);
	}
	page_limit = 100; page_backend = false; UT_ASSERT_NULL(page_open()); page_backend = true;
	UT_ASSERT_EQ(unlink(page_space), 0); UT_ASSERT_NULL(page_open()); UT_ASSERT_EQ(page_external, 0);
	UT_ASSERT_EQ(symlink(page_main, page_space), 0); UT_ASSERT_NULL(page_open());
	UT_ASSERT_EQ(unlink(page_space), 0); page_file(page_space, 2);
	snprintf(extra, sizeof(extra), "%s/alias", page_parent);
	UT_ASSERT_EQ(link(page_main, extra), 0); UT_ASSERT_NULL(page_open()); UT_ASSERT_EQ(page_external, 0);
}

UT_TEST(test_page_fd_io_failure_retains_custody_until_close)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		ClusterSharedFsPageV1 *target;
		PGAlignedBlock image;

		page_setup("page_fd_io"); target = page_open(); UT_ASSERT_NOT_NULL(target);
		memset(image.data, 0x42, BLCKSZ); page_fault = Min(fault, 3);
		data_sync_retry = fault == 4; page_error_expected = true;
		if (sigsetjmp(page_error_jump, 1) == 0) {
			if (fault == 1) (void)cluster_shared_fs_page_read_v1(target, MAIN_FORKNUM, 1, image.data);
			else if (fault == 2) (void)cluster_shared_fs_page_write_v1(target, image.data);
			else (void)cluster_shared_fs_page_sync_v1(target);
		}
		page_error_expected = false;
		UT_ASSERT_EQ(page_error_level, fault == 3 ? PANIC : ERROR);
		UT_ASSERT_EQ(page_external, 5);
		page_fault = 0; UT_ASSERT(!cluster_shared_fs_page_current_v1(target));
		UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
	}
}

UT_TEST(test_page_fd_cannot_read_another_fork_or_block)
{
	ClusterSharedFsPageV1 *target;
	PGAlignedBlock image;

	page_setup("page_fd_bounds"); target = page_open(); UT_ASSERT_NOT_NULL(target);
	UT_ASSERT(!cluster_shared_fs_page_read_v1(target, MAIN_FORKNUM, 0, image.data));
	UT_ASSERT(!cluster_shared_fs_page_read_v1(target, SPACE_FORKNUM, 2, image.data));
	UT_ASSERT(!cluster_shared_fs_page_read_v1(target, FSM_FORKNUM, 0, image.data));
	UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
}

UT_TEST(test_page_fd_postread_namespace_loss_keeps_output_unchanged)
{
	ClusterSharedFsPageV1 *target;
	PGAlignedBlock image, original;

	page_setup("page_fd_postread"); target = page_open(); UT_ASSERT_NOT_NULL(target);
	memset(image.data, 0x19, BLCKSZ); original = image; page_fault = 4;
	UT_ASSERT(!cluster_shared_fs_page_read_v1(target, MAIN_FORKNUM, 1, image.data));
	UT_ASSERT(memcmp(image.data, original.data, BLCKSZ) == 0);
	UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
}

UT_TEST(test_page_fd_prepare_sync_failure_and_configured_root_spelling)
{
	ClusterSharedFsPageV1 *target;
	char configured[MAXPGPATH];

	page_setup("page_fd_prepare"); page_fault = 3;
	page_error_expected = true;
	if (sigsetjmp(page_error_jump, 1) == 0) (void)page_open();
	page_error_expected = false;
	UT_ASSERT_EQ(page_error_level, PANIC); UT_ASSERT_EQ(page_external, 0);
	page_fault = 0;
	snprintf(configured, sizeof(configured), "%s/", test_root);
	cluster_shared_data_dir = configured;
	target = page_open(); UT_ASSERT_NOT_NULL(target);
	UT_ASSERT(cluster_shared_fs_page_current_v1(target));
	cluster_shared_data_dir = test_root; /* same inode, changed configured value */
	UT_ASSERT(!cluster_shared_fs_page_current_v1(target));
	UT_ASSERT(cluster_shared_fs_page_close_v1(target)); UT_ASSERT_EQ(page_external, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_page_fd_existing_extent_roundtrip_and_explicit_close);
	UT_RUN(test_page_fd_replacement_and_ancestor_drift_stay_refused);
	UT_RUN(test_page_fd_extent_actor_and_owner_drift_stay_refused);
	UT_RUN(test_page_fd_budget_missing_symlink_hardlink_and_partial_fd_cleanup);
	UT_RUN(test_page_fd_io_failure_retains_custody_until_close);
	UT_RUN(test_page_fd_cannot_read_another_fork_or_block);
	UT_RUN(test_page_fd_postread_namespace_loss_keeps_output_unchanged);
	UT_RUN(test_page_fd_prepare_sync_failure_and_configured_root_spelling);
	UT_DONE(); return ut_failed_count != 0;
}
