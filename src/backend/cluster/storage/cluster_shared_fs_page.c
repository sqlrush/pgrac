/* Existing cluster_fs PAGE target held across the original FPI executor.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_shared_fs_page.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/resowner.h"

/* Native permanent locator paths have at most four directory components.
 * Refuse a longer path rather than allocating after any descriptor opens. */
#define SHAREDFS_PAGE_DIRS 5

struct ClusterSharedFsPageV1 {
	RelFileLocator locator;
	BlockNumber block;
	ResourceOwner owner;
	int pid, direct_flags;
	bool failed;
	unsigned dirs;
	int directory[SHAREDFS_PAGE_DIRS], main_fd, space_fd;
	struct stat directory_identity[SHAREDFS_PAGE_DIRS], main_identity, space_identity;
	char configured[MAXPGPATH], root[MAXPGPATH], relative[MAXPGPATH], space[MAXPGPATH];
	char *directory_name[SHAREDFS_PAGE_DIRS], *main_name, *space_name;
};

Size
cluster_shared_fs_page_bytes_v1(void)
{
	return sizeof(ClusterSharedFsPageV1) + 2 * MAXPGPATH;
}

static bool
page_fd_same(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_mode == b->st_mode && a->st_uid == b->st_uid
		&& a->st_gid == b->st_gid && a->st_nlink == b->st_nlink;
}

static bool
page_fd_file(int fd, int directory, const char *name, const struct stat *identity)
{
	struct stat opened, named;

	return fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode) && opened.st_nlink == 1
		&& page_fd_same(identity, &opened) && opened.st_size == identity->st_size
		&& fstatat(directory, name, &named, AT_SYMLINK_NOFOLLOW) == 0
		&& page_fd_same(&opened, &named) && named.st_size == opened.st_size;
}

bool
cluster_shared_fs_page_current_v1(ClusterSharedFsPageV1 *target)
{
	const ClusterSharedFsOps *ops = cluster_shared_fs_get_active_ops();
	struct stat named, opened;

	if (target == NULL || target->failed) return false;
	if (!enableFsync || MyProcPid != target->pid || CurrentResourceOwner != target->owner
		|| io_direct_flags != target->direct_flags || ops == NULL
		|| ops->id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS || cluster_shared_data_dir == NULL
		|| strcmp(cluster_shared_data_dir, target->configured) != 0)
		goto failed;
	for (unsigned i = 0; i < target->dirs; i++) {
		int rc = i == 0 ? lstat(target->root, &named)
			: fstatat(target->directory[i - 1], target->directory_name[i], &named, AT_SYMLINK_NOFOLLOW);

		if (rc != 0 || !S_ISDIR(named.st_mode)
			|| fstat(target->directory[i], &opened) != 0
			|| !page_fd_same(&target->directory_identity[i], &named)
			|| !page_fd_same(&target->directory_identity[i], &opened))
			goto failed;
	}
	if (page_fd_file(target->main_fd, target->directory[target->dirs - 1],
			target->main_name, &target->main_identity)
		&& page_fd_file(target->space_fd, target->directory[target->dirs - 1],
			target->space_name, &target->space_identity))
		return true;
failed:
	target->failed = true;
	return false;
}

bool
cluster_shared_fs_page_matches_v1(ClusterSharedFsPageV1 *target,
	RelFileLocator locator, BlockNumber block)
{
	return target != NULL && RelFileLocatorEquals(target->locator, locator)
		&& target->block == block && cluster_shared_fs_page_current_v1(target);
}

bool
cluster_shared_fs_page_close_v1(ClusterSharedFsPageV1 *target)
{
	int saved_errno = errno;
	bool closed = true;

	if (target == NULL) return true;
	if (target->space_fd >= 0) {
		if (close(target->space_fd) != 0) closed = false;
		ReleaseExternalFD();
	}
	if (target->main_fd >= 0) {
		if (close(target->main_fd) != 0) closed = false;
		ReleaseExternalFD();
	}
	for (unsigned i = target->dirs; i > 0; i--)
		if (target->directory[i - 1] >= 0) {
			if (close(target->directory[i - 1]) != 0) closed = false;
			ReleaseExternalFD();
		}
	pfree(target);
	errno = saved_errno;
	return closed;
}

/* Every external reservation immediately either acquires a raw descriptor
 * or is refunded. No ereport/allocation follows a successful reservation. */
static int
page_fd_open(int parent, const char *name, int flags)
{
	int fd;

	if (!AcquireExternalFD()) return -1;
	fd = parent < 0 ? open(name, flags) : openat(parent, name, flags);
	if (fd < 0) ReleaseExternalFD();
	return fd;
}

ClusterSharedFsPageV1 *
cluster_shared_fs_page_open_v1(RelFileLocator locator, BlockNumber block, Size memory_budget)
{
	const ClusterSharedFsOps *ops = cluster_shared_fs_get_active_ops();
	ClusterSharedFsPageV1 *target;
	char *relative, *space, *name, *next;
	int flags = O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY;
	Size length, parent_length;

	if (memory_budget < cluster_shared_fs_page_bytes_v1() || !enableFsync || CurrentResourceOwner == NULL
		|| MyProcPid <= 0 || !OidIsValid(locator.relNumber) || block == InvalidBlockNumber
		|| cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/'
		|| strlen(cluster_shared_data_dir) >= MAXPGPATH || ops == NULL
		|| ops->id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS
		|| ((io_direct_flags & IO_DIRECT_DATA)
			&& (PG_O_DIRECT == 0 || BLCKSZ % PG_IO_ALIGN_SIZE != 0)))
		return NULL;
	/* Resolve and copy both native fork paths before opening anything. */
	relative = relpathperm(locator, MAIN_FORKNUM);
	space = relpathperm(locator, SPACE_FORKNUM);
	if (strlen(relative) >= MAXPGPATH || strlen(space) >= MAXPGPATH) {
		pfree(relative); pfree(space); return NULL;
	}
	target = palloc0(sizeof(*target));
	strlcpy(target->configured, cluster_shared_data_dir, sizeof(target->configured));
	strlcpy(target->root, cluster_shared_data_dir, sizeof(target->root));
	length = strlen(target->root);
	while (length > 1 && target->root[length - 1] == '/') target->root[--length] = '\0';
	strlcpy(target->relative, relative, sizeof(target->relative));
	strlcpy(target->space, space, sizeof(target->space));
	pfree(relative); pfree(space);
	target->pid = MyProcPid; target->owner = CurrentResourceOwner;
	target->locator = locator; target->block = block; target->direct_flags = io_direct_flags;
	target->main_fd = target->space_fd = -1;
	for (unsigned i = 0; i < SHAREDFS_PAGE_DIRS; i++) target->directory[i] = -1;
	target->dirs = 1; target->directory_name[0] = target->root;
	name = target->relative;
	while ((next = strchr(name, '/')) != NULL) {
		if (target->dirs == SHAREDFS_PAGE_DIRS) goto failed;
		*next = '\0'; target->directory_name[target->dirs++] = name; name = next + 1;
	}
	target->main_name = name;
	parent_length = name - target->relative;
	/* Both native fork names must name the same original directory. */
	if (parent_length > 0) {
		space = target->space + parent_length;
		for (Size i = 0; i < parent_length; i++) {
			char expected = target->relative[i] == '\0' ? '/' : target->relative[i];
			if (target->space[i] != expected) goto failed;
		}
	} else space = target->space;
	target->space_name = space;
	if (*name == '\0' || *space == '\0' || strchr(space, '/') != NULL
		|| strcmp(name, ".") == 0 || strcmp(name, "..") == 0
		|| strcmp(space, ".") == 0 || strcmp(space, "..") == 0)
		goto failed;
	for (unsigned i = 0; i < target->dirs; i++) {
		if (*target->directory_name[i] == '\0'
			|| (i != 0 && (strcmp(target->directory_name[i], ".") == 0
				|| strcmp(target->directory_name[i], "..") == 0)))
			goto failed;
		target->directory[i] = page_fd_open(i == 0 ? -1 : target->directory[i - 1],
			target->directory_name[i], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
		if (target->directory[i] < 0
			|| fstat(target->directory[i], &target->directory_identity[i]) != 0
			|| !S_ISDIR(target->directory_identity[i].st_mode))
			goto failed;
	}
	if (io_direct_flags & IO_DIRECT_DATA) flags |= PG_O_DIRECT;
#ifdef PG_O_DIRECT_USE_F_NOCACHE
	flags &= ~PG_O_DIRECT;
#endif
	target->main_fd = page_fd_open(target->directory[target->dirs - 1], name, O_RDWR | flags);
	if (target->main_fd < 0 || fstat(target->main_fd, &target->main_identity) != 0
		|| target->main_identity.st_size <= 0 || target->main_identity.st_size % BLCKSZ != 0
		|| (uint64)block >= (uint64)target->main_identity.st_size / BLCKSZ)
		goto failed;
	target->space_fd = page_fd_open(target->directory[target->dirs - 1], space, O_RDONLY | flags);
	if (target->space_fd < 0 || fstat(target->space_fd, &target->space_identity) != 0
		|| target->space_identity.st_size != 2 * BLCKSZ)
		goto failed;
#ifdef PG_O_DIRECT_USE_F_NOCACHE
	if ((io_direct_flags & IO_DIRECT_DATA)
		&& (fcntl(target->main_fd, F_NOCACHE, 1) != 0 || fcntl(target->space_fd, F_NOCACHE, 1) != 0))
		goto failed;
#endif
	if (!cluster_shared_fs_page_current_v1(target)) goto failed;
	/* Existing target and namespace only: no mkdir, create or extension. */
	for (unsigned i = target->dirs; i > 0; i--) {
		if (pg_fsync(target->directory[i - 1]) != 0) {
			int saved_errno = errno;

			/* Preparation has not returned its handle to the executor yet. */
			(void)cluster_shared_fs_page_close_v1(target);
			errno = saved_errno;
			ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
				errmsg("cluster_fs: could not sync recovery PAGE directory: %m")));
			return NULL;
		}
		if (!cluster_shared_fs_page_current_v1(target))
			goto failed;
	}
	return target;
failed:
	(void)cluster_shared_fs_page_close_v1(target);
	return NULL;
}

bool
cluster_shared_fs_page_read_v1(ClusterSharedFsPageV1 *target, ForkNumber fork,
	BlockNumber block, char *out)
{
	PGIOAlignedBlock image;
	ssize_t count;
	int fd;

	if (out == NULL || !cluster_shared_fs_page_current_v1(target)
		|| !((fork == MAIN_FORKNUM && block == target->block)
			|| (fork == SPACE_FORKNUM && block < 2)))
		return false;
	fd = fork == MAIN_FORKNUM ? target->main_fd : target->space_fd;
	do { count = pg_pread(fd, image.data, BLCKSZ, (off_t)block * BLCKSZ); } while (count < 0 && errno == EINTR);
	if (count < 0) {
		target->failed = true;
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("cluster_fs: could not read recovery PAGE block %u: %m", block)));
	}
	if (count != BLCKSZ) {
		target->failed = true;
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			errmsg("cluster_fs: short read of recovery PAGE block %u", block)));
	}
	if (!cluster_shared_fs_page_current_v1(target)) return false;
	memcpy(out, image.data, BLCKSZ);
	return true;
}

bool
cluster_shared_fs_page_write_v1(ClusterSharedFsPageV1 *target, const char *image)
{
	PGIOAlignedBlock copy;
	ssize_t count;

	if (image == NULL || !cluster_shared_fs_page_current_v1(target)) return false;
	memcpy(copy.data, image, BLCKSZ);
	/* Same original shared SMGR write guards; custody alone is not permission. */
	cluster_write_fence_reject_if_fenced("write");
	cluster_mrp_standby_shared_write_gate("write");
	if (!cluster_shared_fs_page_current_v1(target)) return false;
	do { count = pg_pwrite(target->main_fd, copy.data, BLCKSZ, (off_t)target->block * BLCKSZ); }
	while (count < 0 && errno == EINTR);
	if (count < 0) {
		target->failed = true;
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("cluster_fs: could not write recovery PAGE block %u: %m", target->block)));
	}
	if (count != BLCKSZ) {
		target->failed = true;
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
			errmsg("cluster_fs: short write of recovery PAGE block %u", target->block)));
	}
	return cluster_shared_fs_page_current_v1(target);
}

bool
cluster_shared_fs_page_sync_v1(ClusterSharedFsPageV1 *target)
{
	if (!cluster_shared_fs_page_current_v1(target)) return false;
	if (pg_fsync(target->main_fd) != 0) {
		target->failed = true;
		ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
			errmsg("cluster_fs: could not sync recovery PAGE target: %m")));
	}
	return cluster_shared_fs_page_current_v1(target);
}
#endif /* USE_PGRAC_CLUSTER */
