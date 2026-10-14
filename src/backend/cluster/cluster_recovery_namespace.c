/*-------------------------------------------------------------------------
 * cluster_recovery_namespace.c
 *   Read-only physical namespace discovery for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_namespace.h"

#define RECOVERY_NAMESPACE_DEPTH 64
#define RECOVERY_NAMESPACE_ENTRIES 65536
#define RECOVERY_NAMESPACE_STEP 128

typedef struct RecoveryDirectory {
	DIR *stream;
	struct stat observed;
	char path[MAXPGPATH];
} RecoveryDirectory;

struct ClusterRecoveryNamespace {
	RecoveryDirectory dirs[RECOVERY_NAMESPACE_DEPTH];
	unsigned depth;
	uint32 entries;
	uint32 max_entries;
	ClusterRecoveryNamespaceResult state;
	int current_fd;
	struct stat current_stat;
	char current_name[MAXPGPATH];
};

static bool
namespace_number(const char **cursor, uint32 *out)
{
	const char *p = *cursor;
	uint32 number = 0;

	if (*p < '1' || *p > '9')
		return false;
	do {
		unsigned digit = *p - '0';

		if (number > (UINT32_MAX - digit) / 10)
			return false;
		number = number * 10 + digit;
		p++;
	} while (*p >= '0' && *p <= '9');
	*cursor = p;
	*out = number;
	return true;
}

ClusterRecoveryFileKind
cluster_recovery_namespace_classify(const char *path, RelFileLocator *locator, ForkNumber *forknum)
{
	RelFileLocator result = { 0 };
	ClusterRecoveryFileKind kind = CLUSTER_RECOVERY_FILE_UNCLASSIFIED;
	ForkNumber fork = InvalidForkNumber;
	const char *name;
	const char *suffix;

	if (locator != NULL)
		memset(locator, 0, sizeof(*locator));
	if (forknum != NULL)
		*forknum = InvalidForkNumber;
	if (path == NULL || locator == NULL || forknum == NULL || strnlen(path, MAXPGPATH) == MAXPGPATH)
		return kind;
	if (strncmp(path, "global/", 7) == 0) {
		result.spcOid = GLOBALTABLESPACE_OID;
		name = path + 7;
	} else if (strncmp(path, "base/", 5) == 0) {
		result.spcOid = DEFAULTTABLESPACE_OID;
		name = path + 5;
		if (!namespace_number(&name, &result.dbOid) || *name++ != '/')
			return kind;
	} else
		return kind;
	if (strchr(name, '/') != NULL)
		return kind;
	if (strcmp(name, "pg_filenode.map") == 0)
		kind = CLUSTER_RECOVERY_FILE_RELMAP;
	else if (strcmp(name, "pg_filenode.map.bak") == 0)
		kind = CLUSTER_RECOVERY_FILE_RELMAP_BACKUP;
	else if (strcmp(name, "pgrac_relmap_authority") == 0)
		kind = CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY;
	else if (strcmp(name, "pgrac_relmap_authority.bak") == 0)
		kind = CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP;
	else {
		suffix = name;
		if (!namespace_number(&suffix, &result.relNumber))
			return kind;
		if (*suffix == '\0')
			fork = MAIN_FORKNUM;
		else if (strcmp(suffix, "_fsm") == 0)
			fork = FSM_FORKNUM;
		else if (strcmp(suffix, "_vm") == 0)
			fork = VISIBILITYMAP_FORKNUM;
		else if (strcmp(suffix, "_space") == 0)
			fork = SPACE_FORKNUM;
		else
			return kind;
		kind = CLUSTER_RECOVERY_FILE_RELATION;
	}
	*locator = result;
	*forknum = fork;
	return kind;
}

static bool
namespace_same(const struct stat *before, const struct stat *after)
{
	if (before->st_dev != after->st_dev || before->st_ino != after->st_ino
		|| before->st_mode != after->st_mode || before->st_uid != after->st_uid
		|| before->st_gid != after->st_gid || before->st_nlink != after->st_nlink
		|| before->st_size != after->st_size)
		return false;
#ifdef __APPLE__
	return before->st_mtimespec.tv_sec == after->st_mtimespec.tv_sec
		   && before->st_mtimespec.tv_nsec == after->st_mtimespec.tv_nsec
		   && before->st_ctimespec.tv_sec == after->st_ctimespec.tv_sec
		   && before->st_ctimespec.tv_nsec == after->st_ctimespec.tv_nsec;
#else
	return before->st_mtim.tv_sec == after->st_mtim.tv_sec
		   && before->st_mtim.tv_nsec == after->st_mtim.tv_nsec
		   && before->st_ctim.tv_sec == after->st_ctim.tv_sec
		   && before->st_ctim.tv_nsec == after->st_ctim.tv_nsec;
#endif
}

/* Open a new directory description: dup alone shares the caller's cursor. */
static DIR *
namespace_directory(int parent, const char *name, const struct stat *expected)
{
	int fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	struct stat held;
	DIR *stream;

	if (fd < 0)
		return NULL;
	if (fstat(fd, &held) != 0) {
		int saved_errno = errno;

		close(fd);
		errno = saved_errno;
		return NULL;
	}
	if (!namespace_same(expected, &held)) {
		close(fd);
		errno = ESTALE;
		return NULL;
	}
	stream = fdopendir(fd);
	if (stream == NULL)
		close(fd);
	return stream;
}

static ClusterRecoveryNamespaceResult
namespace_io_result(void)
{
	return errno == ENOENT || errno == ESTALE || errno == ELOOP || errno == ENOTDIR
			   ? CLUSTER_RECOVERY_NAMESPACE_CHANGED
			   : CLUSTER_RECOVERY_NAMESPACE_IO;
}

ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin(int root_fd, uint32 max_entries, ClusterRecoveryNamespace **out)
{
	struct stat root;
	ClusterRecoveryNamespace *scan;
	DIR *dir;

	if (out != NULL)
		*out = NULL;
	if (root_fd < 0 || out == NULL || max_entries == 0 || max_entries > RECOVERY_NAMESPACE_ENTRIES)
		return CLUSTER_RECOVERY_NAMESPACE_INVALID;
	if (fstat(root_fd, &root) != 0)
		return CLUSTER_RECOVERY_NAMESPACE_IO;
	if (!S_ISDIR(root.st_mode))
		return CLUSTER_RECOVERY_NAMESPACE_UNSAFE;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		return CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
	dir = namespace_directory(root_fd, ".", &root);
	if (dir == NULL) {
		ClusterRecoveryNamespaceResult result = namespace_io_result();

		free(scan);
		return result;
	}
	scan->dirs[0].stream = dir;
	scan->dirs[0].observed = root;
	scan->depth = 1;
	scan->max_entries = max_entries;
	scan->current_fd = -1;
	scan->state = CLUSTER_RECOVERY_NAMESPACE_MORE;
	*out = scan;
	return scan->state;
}

static ClusterRecoveryNamespaceResult
namespace_recheck_file(ClusterRecoveryNamespace *scan)
{
	struct stat held, named;
	int parent = dirfd(scan->dirs[scan->depth - 1].stream);

	if (fstat(scan->current_fd, &held) != 0
		|| fstatat(parent, scan->current_name, &named, AT_SYMLINK_NOFOLLOW) != 0)
		return namespace_io_result();
	if (!namespace_same(&scan->current_stat, &held) || !namespace_same(&scan->current_stat, &named))
		return CLUSTER_RECOVERY_NAMESPACE_CHANGED;
	return CLUSTER_RECOVERY_NAMESPACE_MORE;
}

static ClusterRecoveryNamespaceResult
namespace_finish_file(ClusterRecoveryNamespace *scan)
{
	ClusterRecoveryNamespaceResult result = namespace_recheck_file(scan);

	if (close(scan->current_fd) != 0 && result == CLUSTER_RECOVERY_NAMESPACE_MORE)
		result = CLUSTER_RECOVERY_NAMESPACE_IO;
	scan->current_fd = -1;
	return result;
}

static ClusterRecoveryNamespaceResult
namespace_finish_directory(ClusterRecoveryNamespace *scan)
{
	RecoveryDirectory *dir = &scan->dirs[scan->depth - 1];
	struct stat held, named;

	if (fstat(dirfd(dir->stream), &held) != 0)
		return CLUSTER_RECOVERY_NAMESPACE_IO;
	if (!namespace_same(&dir->observed, &held))
		return CLUSTER_RECOVERY_NAMESPACE_CHANGED;
	if (scan->depth > 1) {
		const char *name = strrchr(dir->path, '/');
		int parent = dirfd(scan->dirs[scan->depth - 2].stream);

		name = name == NULL ? dir->path : name + 1;
		if (fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) != 0)
			return namespace_io_result();
		if (!namespace_same(&dir->observed, &named))
			return CLUSTER_RECOVERY_NAMESPACE_CHANGED;
	}
	if (closedir(dir->stream) != 0) {
		dir->stream = NULL;
		scan->depth--;
		return CLUSTER_RECOVERY_NAMESPACE_IO;
	}
	dir->stream = NULL;
	scan->depth--;
	return scan->depth == 0 ? CLUSTER_RECOVERY_NAMESPACE_END : CLUSTER_RECOVERY_NAMESPACE_MORE;
}

ClusterRecoveryNamespaceResult
cluster_recovery_namespace_next(ClusterRecoveryNamespace *scan, ClusterRecoveryNamespaceEntry *out,
								int *file_fd)
{
	ClusterRecoveryNamespaceEntry entry;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (file_fd != NULL)
		*file_fd = -1;
	if (scan == NULL || out == NULL || file_fd == NULL)
		return CLUSTER_RECOVERY_NAMESPACE_INVALID;
	if (scan->state >= CLUSTER_RECOVERY_NAMESPACE_END)
		return scan->state;
	if (scan->current_fd >= 0) {
		scan->state = namespace_finish_file(scan);
		if (scan->state != CLUSTER_RECOVERY_NAMESPACE_MORE)
			return scan->state;
	}
	for (unsigned work = 0; work < RECOVERY_NAMESPACE_STEP; work++) {
		RecoveryDirectory *dir = &scan->dirs[scan->depth - 1];
		struct dirent *item;
		struct stat named, held;
		int parent = dirfd(dir->stream);
		int written;

		errno = 0;
		item = readdir(dir->stream);
		if (item == NULL) {
			if (errno != 0)
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_IO;
			scan->state = namespace_finish_directory(scan);
			if (scan->state != CLUSTER_RECOVERY_NAMESPACE_MORE)
				return scan->state;
			continue;
		}
		if (strcmp(item->d_name, ".") == 0 || strcmp(item->d_name, "..") == 0)
			continue;
		if (scan->entries == scan->max_entries)
			return scan->state = CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
		memset(&entry, 0, sizeof(entry));
		written = snprintf(entry.path, sizeof(entry.path), "%s%s%s", dir->path,
						   dir->path[0] == '\0' ? "" : "/", item->d_name);
		if (written < 0 || written >= sizeof(entry.path))
			return scan->state = CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
		if (fstatat(parent, item->d_name, &named, AT_SYMLINK_NOFOLLOW) != 0)
			return scan->state = namespace_io_result();
		if (S_ISDIR(named.st_mode)) {
			RecoveryDirectory *child;

			if (scan->depth == RECOVERY_NAMESPACE_DEPTH)
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
			child = &scan->dirs[scan->depth];
			child->stream = namespace_directory(parent, item->d_name, &named);
			if (child->stream == NULL)
				return scan->state = namespace_io_result();
			child->observed = named;
			memcpy(child->path, entry.path, sizeof(child->path));
			scan->depth++;
			entry.kind = CLUSTER_RECOVERY_FILE_DIRECTORY;
			entry.forknum = InvalidForkNumber;
		} else {
			int fd;

			if (!S_ISREG(named.st_mode) || named.st_nlink != 1 || named.st_size < 0)
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_UNSAFE;
			fd = openat(parent, item->d_name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
			if (fd < 0)
				return scan->state = namespace_io_result();
			if (fstat(fd, &held) != 0) {
				close(fd);
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_IO;
			}
			if (!namespace_same(&named, &held)) {
				close(fd);
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_CHANGED;
			}
			scan->current_fd = fd;
			scan->current_stat = held;
			strlcpy(scan->current_name, item->d_name, sizeof(scan->current_name));
			entry.kind
				= cluster_recovery_namespace_classify(entry.path, &entry.locator, &entry.forknum);
			entry.size = held.st_size;
			*file_fd = fd;
		}
		scan->entries++;
		*out = entry;
		return CLUSTER_RECOVERY_NAMESPACE_ENTRY;
	}
	return scan->state = CLUSTER_RECOVERY_NAMESPACE_MORE;
}

ClusterRecoveryNamespaceResult
cluster_recovery_namespace_read_current(ClusterRecoveryNamespace *scan, uint64 offset, void *buffer,
										size_t length)
{
	ClusterRecoveryNamespaceResult result;

	if (buffer != NULL && length > 0 && length <= BLCKSZ)
		memset(buffer, 0, length);
	if (scan == NULL)
		return CLUSTER_RECOVERY_NAMESPACE_INVALID;
	if (scan->state > CLUSTER_RECOVERY_NAMESPACE_END)
		return scan->state;
	if (buffer == NULL || length == 0 || length > BLCKSZ || scan->current_fd < 0 || scan->depth == 0
		|| offset > (uint64)scan->current_stat.st_size
		|| length > (uint64)scan->current_stat.st_size - offset)
		return scan->state = CLUSTER_RECOVERY_NAMESPACE_INVALID;

	result = namespace_recheck_file(scan);
	if (result != CLUSTER_RECOVERY_NAMESPACE_MORE)
		return scan->state = result;
	/* The range already fits the original nonnegative off_t file size.
	 * A short read, including EOF, cannot discharge any page obligation. */
	if (pread(scan->current_fd, buffer, length, (off_t)offset) != (ssize_t)length)
		result = CLUSTER_RECOVERY_NAMESPACE_IO;
	else
		result = namespace_recheck_file(scan);
	if (result != CLUSTER_RECOVERY_NAMESPACE_MORE) {
		memset(buffer, 0, length);
		return scan->state = result;
	}
	return CLUSTER_RECOVERY_NAMESPACE_ENTRY;
}

void
cluster_recovery_namespace_destroy(ClusterRecoveryNamespace *scan)
{
	if (scan == NULL)
		return;
	if (scan->current_fd >= 0)
		close(scan->current_fd);
	for (unsigned i = 0; i < scan->depth; i++)
		if (scan->dirs[i].stream != NULL)
			closedir(scan->dirs[i].stream);
	free(scan);
}
