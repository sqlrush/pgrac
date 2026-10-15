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
#ifdef __APPLE__
#include <malloc/malloc.h>
#include <sys/mount.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#endif
#include "access/slru.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_namespace.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"

#define RECOVERY_NAMESPACE_DEPTH 64
#define RECOVERY_NAMESPACE_ENTRIES 65536
#define RECOVERY_NAMESPACE_STEP 128

typedef struct RecoveryDirectory {
	DIR *stream;
	size_t reservation;
	struct stat observed;
	char path[MAXPGPATH];
} RecoveryDirectory;

struct ClusterRecoveryNamespace {
	RecoveryDirectory dirs[RECOVERY_NAMESPACE_DEPTH];
	unsigned depth;
	uint32 entries;
	uint32 max_entries;
	size_t memory_budget, memory_reserved;
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

static bool
namespace_undo_key(const char *path, uint32 *segment, uint8 *owner)
{
	const char *name;
	uint32 node = 0, id;

	if (strncmp(path, "pg_undo/instance_", 17) != 0)
		return false;
	name = path + 17;
	if (*name == '0')
		name++;
	else if (!namespace_number(&name, &node))
		return false;
	if (node >= UNDO_OWNER_INSTANCE_MAX || strncmp(name, "/seg_", 5) != 0)
		return false;
	name += 5;
	if (!namespace_number(&name, &id) || strcmp(name, ".dat") != 0
		|| (id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != node)
		return false;
	*segment = id;
	*owner = node + 1;
	return true;
}

/* Preserve canonical native segment names, including the five hex digits
 * required by the last members segments. A name is a candidate, never a
 * source/epoch or horizon proof. Origin-side files get a distinct kind. */
static bool
namespace_native_key(const char *path, uint8 *family, uint32 *segment)
{
	static const struct {
		const char *prefix;
		uint8 family;
		uint32 per_page;
	} prefixes[] = {
		{ "pg_xact/", CLUSTER_RECOVERY_NATIVE_FAMILY_XACT, CLUSTER_NATIVE_CLOG_PER_PAGE },
		{ "pg_subtrans/", CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS,
		  CLUSTER_NATIVE_SUBTRANS_PER_PAGE },
		{ "pg_commit_ts/", CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS,
		  CLUSTER_NATIVE_COMMIT_TS_PER_PAGE },
		{ "pg_multixact/offsets/", CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS,
		  CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE },
		{ "pg_multixact/members/", CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS,
		  CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE },
	};

	if (path == NULL || family == NULL || segment == NULL)
		return false;
	for (unsigned i = 0; i < lengthof(prefixes); i++) {
		size_t prefix_length = strlen(prefixes[i].prefix);
		const char *name;
		uint32 value = 0;
		char canonical[9];
		size_t digits;

		if (strncmp(path, prefixes[i].prefix, prefix_length) != 0)
			continue;
		name = path + prefix_length;
		digits = strlen(name);
		if (digits < 4 || digits > 8)
			continue;
		for (unsigned digit = 0; digit < digits; digit++) {
			unsigned nibble;

			if (name[digit] >= '0' && name[digit] <= '9')
				nibble = (unsigned)(name[digit] - '0');
			else if (name[digit] >= 'A' && name[digit] <= 'F')
				nibble = (unsigned)(name[digit] - 'A') + 10;
			else
				return false;
			value = (value << 4) | nibble;
		}
		snprintf(canonical, sizeof(canonical), "%04X", value);
		if (strcmp(canonical, name) != 0
			|| value > UINT32_MAX / prefixes[i].per_page / SLRU_PAGES_PER_SEGMENT)
			return false;
		*family = prefixes[i].family;
		*segment = value;
		return true;
	}
	return false;
}

/* Use exactly the existing failed-origin native-side directory. Decimal
 * aliases and out-of-range origins cannot fall back to the local SLRU. */
static bool
namespace_origin_native_key(const char *path, uint8 *family, uint32 *segment, uint32 *origin)
{
	const char *name;
	uint32 node = 0;
	const char *prefix = "native_side/origin_";
	size_t length = strlen(prefix);

	if (path == NULL || family == NULL || segment == NULL || origin == NULL
		|| strncmp(path, prefix, length) != 0)
		return false;
	name = path + length;
	if (*name < '0' || *name > '9' || (*name == '0' && name[1] != '/'))
		return false;
	while (*name >= '0' && *name <= '9') {
		node = node * 10 + (unsigned)(*name++ - '0');
		if (node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
			return false;
	}
	if (*name != '/' || !namespace_native_key(name + 1, family, segment))
		return false;
	*origin = node;
	return true;
}

/* PostgreSQL's durable two-phase state files are named by one canonical
 * eight-digit uppercase xid.  A name match is only a physical obligation;
 * the file contents and its matching PREPARE/terminal WAL still need the
 * recovery owner's exact proof before any state is consumed. */
static bool
namespace_twophase_key(const char *path, uint32 *xid)
{
	const char *prefix = "pg_twophase/";
	const char *name;
	uint32 value = 0;
	char canonical[9];

	if (path == NULL || xid == NULL || strncmp(path, prefix, strlen(prefix)) != 0)
		return false;
	name = path + strlen(prefix);
	if (strlen(name) != 8)
		return false;
	for (unsigned digit = 0; digit < 8; digit++) {
		unsigned nibble;

		if (name[digit] >= '0' && name[digit] <= '9')
			nibble = (unsigned)(name[digit] - '0');
		else if (name[digit] >= 'A' && name[digit] <= 'F')
			nibble = (unsigned)(name[digit] - 'A') + 10;
		else
			return false;
		value = (value << 4) | nibble;
	}
	snprintf(canonical, sizeof(canonical), "%08X", value);
	if (memcmp(canonical, name, sizeof(canonical) - 1) != 0)
		return false;
	*xid = value;
	return true;
}

ClusterRecoveryFileKind
cluster_recovery_namespace_classify_ex(const char *path, RelFileLocator *locator,
									   ForkNumber *forknum, uint8 *native_family,
									   uint32 *native_segment)
{
	RelFileLocator result = { 0 };
	ClusterRecoveryFileKind kind = CLUSTER_RECOVERY_FILE_UNCLASSIFIED;
	ForkNumber fork = InvalidForkNumber;
	const char *name;
	const char *suffix;
	uint32 undo_segment;
	uint8 undo_owner;
	uint8 observed_native_family = CLUSTER_RECOVERY_NATIVE_FAMILY_NONE;
	uint32 observed_native_segment = UINT32_MAX;
	uint32 observed_twophase_xid = 0;
	uint32 native_origin = 0;

	if (locator != NULL)
		memset(locator, 0, sizeof(*locator));
	if (forknum != NULL)
		*forknum = InvalidForkNumber;
	if (native_family != NULL)
		*native_family = CLUSTER_RECOVERY_NATIVE_FAMILY_NONE;
	if (native_segment != NULL)
		*native_segment = UINT32_MAX;
	if (path == NULL || locator == NULL || forknum == NULL || strnlen(path, MAXPGPATH) == MAXPGPATH)
		return kind;
	if (namespace_native_key(path, &observed_native_family, &observed_native_segment)) {
		if (native_family != NULL)
			*native_family = observed_native_family;
		if (native_segment != NULL)
			*native_segment = observed_native_segment;
		return CLUSTER_RECOVERY_FILE_NATIVE_SLRU;
	}
	if (namespace_origin_native_key(path, &observed_native_family, &observed_native_segment,
									&native_origin)) {
		if (native_family != NULL)
			*native_family = observed_native_family;
		if (native_segment != NULL)
			*native_segment = observed_native_segment;
		return CLUSTER_RECOVERY_FILE_ORIGIN_SLRU;
	}
	if (namespace_undo_key(path, &undo_segment, &undo_owner))
		return CLUSTER_RECOVERY_FILE_UNDO_SEGMENT;
	if (namespace_twophase_key(path, &observed_twophase_xid))
		return CLUSTER_RECOVERY_FILE_TWOPHASE;
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

ClusterRecoveryFileKind
cluster_recovery_namespace_classify(const char *path, RelFileLocator *locator, ForkNumber *forknum)
{
	return cluster_recovery_namespace_classify_ex(path, locator, forknum, NULL, NULL);
}

bool
cluster_recovery_namespace_stat_same(const struct stat *before, const struct stat *after)
{
	if (before == NULL || after == NULL)
		return false;
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

/* Reserve libc's directory storage before opening it, including a possible
 * readdir buffer enlargement. Darwin union directories may cache all entries
 * in fdopendir, so cannot enter a bounded scan. Neither scanner uses telldir.
 * Check the actual allocations too, rather than trusting a libc size guess. */
static size_t
namespace_directory_reservation(int fd, const struct stat *held)
{
#ifdef __APPLE__
	struct statfs filesystem;

	if (fstatfs(fd, &filesystem) != 0)
		return 0;
	if ((filesystem.f_flags & MNT_UNION) != 0) {
		errno = ENOTSUP;
		return 0;
	}
	/* DIR/telldir plus 2 KiB initial and 8 KiB enlarged buffer, including
	 * the brief overlap during readdir's replacement allocation. */
	return 16 * 1024;
#elif defined(__GLIBC__)
	/* glibc allocates DIR and a 32 KiB..1 MiB stat-hinted buffer together.
	 * The extra page covers DIR and allocator rounding. */
	return Min(Max((size_t)held->st_blksize, (size_t)32768), (size_t)1048576) + 4096;
#else
	errno = ENOTSUP;
	return 0;
#endif
}

static size_t
namespace_directory_allocated(DIR *stream)
{
#ifdef __APPLE__
	size_t object = malloc_size(stream);
	size_t buffer = malloc_size(stream->__dd_buf);

	return object == 0 || buffer == 0 || object > SIZE_MAX - buffer ? SIZE_MAX : object + buffer;
#elif defined(__GLIBC__)
	size_t size = malloc_usable_size(stream);

	return size == 0 ? SIZE_MAX : size;
#else
	return SIZE_MAX;
#endif
}

/* Open a new directory description: dup alone shares the caller's cursor. */
static DIR *
namespace_directory(ClusterRecoveryNamespace *scan, int parent, const char *name,
					const struct stat *expected, size_t *reservation)
{
	int fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	struct stat held;
	DIR *stream;
	size_t charge;

	if (fd < 0)
		return NULL;
	if (fstat(fd, &held) != 0) {
		int saved_errno = errno;

		close(fd);
		errno = saved_errno;
		return NULL;
	}
	if (!cluster_recovery_namespace_stat_same(expected, &held)) {
		close(fd);
		errno = ESTALE;
		return NULL;
	}
	charge = namespace_directory_reservation(fd, &held);
	if (charge == 0 || charge > scan->memory_budget - scan->memory_reserved) {
		int saved_errno = charge == 0 ? errno : ENOMEM;

		close(fd);
		errno = saved_errno;
		return NULL;
	}
	stream = fdopendir(fd);
	if (stream == NULL) {
		int saved_errno = errno;

		close(fd);
		errno = saved_errno;
		return NULL;
	}
	if (namespace_directory_allocated(stream) > charge) {
		closedir(stream);
		errno = ENOMEM;
		return NULL;
	}
	scan->memory_reserved += charge;
	*reservation = charge;
	return stream;
}

static ClusterRecoveryNamespaceResult
namespace_io_result(void)
{
	if (errno == ENOMEM || errno == ENOTSUP)
		return CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
	return errno == ENOENT || errno == ESTALE || errno == ELOOP || errno == ENOTDIR
			   ? CLUSTER_RECOVERY_NAMESPACE_CHANGED
			   : CLUSTER_RECOVERY_NAMESPACE_IO;
}

ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin(int root_fd, uint32 max_entries, ClusterRecoveryNamespace **out)
{
	return cluster_recovery_namespace_begin_bounded(root_fd, max_entries, SIZE_MAX, out);
}

ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin_bounded(int root_fd, uint32 max_entries, size_t memory_budget,
										 ClusterRecoveryNamespace **out)
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
	if (memory_budget < sizeof(*scan))
		return CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
	scan = calloc(1, sizeof(*scan));
	if (scan == NULL)
		return CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
	scan->memory_budget = memory_budget;
	scan->memory_reserved = sizeof(*scan);
	dir = namespace_directory(scan, root_fd, ".", &root, &scan->dirs[0].reservation);
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
	if (!cluster_recovery_namespace_stat_same(&scan->current_stat, &held)
		|| !cluster_recovery_namespace_stat_same(&scan->current_stat, &named))
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
	if (!cluster_recovery_namespace_stat_same(&dir->observed, &held))
		return CLUSTER_RECOVERY_NAMESPACE_CHANGED;
	if (scan->depth > 1) {
		const char *name = strrchr(dir->path, '/');
		int parent = dirfd(scan->dirs[scan->depth - 2].stream);

		name = name == NULL ? dir->path : name + 1;
		if (fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) != 0)
			return namespace_io_result();
		if (!cluster_recovery_namespace_stat_same(&dir->observed, &named))
			return CLUSTER_RECOVERY_NAMESPACE_CHANGED;
	}
	if (closedir(dir->stream) != 0) {
		scan->memory_reserved -= dir->reservation;
		dir->stream = NULL;
		scan->depth--;
		return CLUSTER_RECOVERY_NAMESPACE_IO;
	}
	scan->memory_reserved -= dir->reservation;
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
		int read_errno;

		errno = 0;
		item = readdir(dir->stream);
		read_errno = errno;
		if (namespace_directory_allocated(dir->stream) > dir->reservation)
			return scan->state = CLUSTER_RECOVERY_NAMESPACE_CAPACITY;
		if (item == NULL) {
			if (read_errno != 0)
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
			child->stream
				= namespace_directory(scan, parent, item->d_name, &named, &child->reservation);
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
			if (!cluster_recovery_namespace_stat_same(&named, &held)) {
				close(fd);
				return scan->state = CLUSTER_RECOVERY_NAMESPACE_CHANGED;
			}
			scan->current_fd = fd;
			scan->current_stat = held;
			strlcpy(scan->current_name, item->d_name, sizeof(scan->current_name));
			entry.kind = cluster_recovery_namespace_classify_ex(
				entry.path, &entry.locator, &entry.forknum, &entry.native_family,
				&entry.native_segment);
			if (entry.kind == CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
				(void)namespace_origin_native_key(entry.path, &entry.native_family,
												  &entry.native_segment, &entry.native_origin_node);
			else if (entry.kind == CLUSTER_RECOVERY_FILE_UNDO_SEGMENT)
				(void)namespace_undo_key(entry.path, &entry.undo_segment,
										 &entry.undo_owner_instance);
			else if (entry.kind == CLUSTER_RECOVERY_FILE_TWOPHASE)
				(void)namespace_twophase_key(entry.path, &entry.twophase_xid);
			entry.size = held.st_size;
			*file_fd = fd;
		}
		scan->entries++;
		entry.observed = named;
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
