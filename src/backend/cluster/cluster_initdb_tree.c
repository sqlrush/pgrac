/* Original cohort file-tree observation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/cryptohash.h"
#include "cluster_initdb_tree_private.h"

#define TREE_ENTRIES 65536
#define TREE_PATH_BYTES (16 * 1024 * 1024)
#define TREE_DEPTH 64

typedef struct TreeEntry
{
	char *path;
	struct stat observed;
	uint8 hash[32];
} TreeEntry;

typedef struct TreeScan
{
	TreeEntry *entries;
	unsigned count, capacity;
	size_t paths;
	bool recheck;
} TreeScan;

static bool
tree_owned(const struct stat *st)
{
	return st->st_uid == geteuid() && (st->st_mode & 0022) == 0
		&& (S_ISDIR(st->st_mode) || (S_ISREG(st->st_mode) && st->st_nlink == 1 && st->st_size >= 0));
}

static bool
tree_same(const struct stat *a, const struct stat *b)
{
	if (a->st_dev != b->st_dev || a->st_ino != b->st_ino || a->st_mode != b->st_mode
		|| a->st_uid != b->st_uid || a->st_nlink != b->st_nlink || a->st_size != b->st_size)
		return false;
#ifdef __APPLE__
	return a->st_mtimespec.tv_sec == b->st_mtimespec.tv_sec
		&& a->st_mtimespec.tv_nsec == b->st_mtimespec.tv_nsec
		&& a->st_ctimespec.tv_sec == b->st_ctimespec.tv_sec
		&& a->st_ctimespec.tv_nsec == b->st_ctimespec.tv_nsec;
#else
	return a->st_mtim.tv_sec == b->st_mtim.tv_sec && a->st_mtim.tv_nsec == b->st_mtim.tv_nsec
		&& a->st_ctim.tv_sec == b->st_ctim.tv_sec && a->st_ctim.tv_nsec == b->st_ctim.tv_nsec;
#endif
}

static bool
tree_number(pg_cryptohash_ctx *ctx, uint64 value, unsigned width)
{
	uint8 bytes[8];
	for (unsigned i = 0; i < width; i++) bytes[i] = value >> (8 * i);
	return pg_cryptohash_update(ctx, bytes, width) >= 0;
}

static bool
tree_file(int fd, const struct stat *before, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	uint8 bytes[65536];
	struct stat after;
	off_t offset = 0;
	bool ok = false;
	if (ctx == NULL) return false;
	if (pg_cryptohash_init(ctx) < 0) goto done;
	for (;;)
	{
		ssize_t n = pread(fd, bytes, sizeof(bytes), offset);
		if (n < 0 && errno == EINTR) continue;
		if (n < 0 || n > before->st_size - offset) goto done;
		if (n == 0) break;
		if (pg_cryptohash_update(ctx, bytes, n) < 0) goto done;
		offset += n;
	}
	if (offset != before->st_size || fstat(fd, &after) != 0 || !tree_same(before, &after)) goto done;
	ok = pg_cryptohash_final(ctx, hash, 32) >= 0;
done:
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
tree_derived(const char *path)
{
	return strcmp(path, "global/wal_startup") == 0
		|| strcmp(path, "global/pgrac_control_root") == 0
		|| strcmp(path, "global/pgrac_control_root.bak") == 0;
}

/* Names are UTF-8 bytes, independent of server encoding/locale. */
static bool
tree_name(const unsigned char *name)
{
	while (*name)
	{
		uint32 value = *name++;
		unsigned continuation;
		uint32 minimum;
		if (value < 0x80) continue;
		if (value >= 0xc2 && value <= 0xdf) { continuation = 1; minimum = 0x80; value &= 0x1f; }
		else if (value >= 0xe0 && value <= 0xef) { continuation = 2; minimum = 0x800; value &= 0x0f; }
		else if (value >= 0xf0 && value <= 0xf4) { continuation = 3; minimum = 0x10000; value &= 7; }
		else return false;
		for (unsigned i = 0; i < continuation; i++)
		{
			if ((*name & 0xc0) != 0x80) return false;
			value = (value << 6) | (*name++ & 0x3f);
		}
		if (value < minimum || value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff)) return false;
	}
	return true;
}

static bool
tree_scan(TreeScan *scan, int parent, const char *prefix, unsigned depth)
{
	struct stat before, after, named;
	DIR *dir = NULL;
	struct dirent *item;
	int fd = -1;
	bool ok = false;

	if (depth > TREE_DEPTH || fstat(parent, &before) != 0 || !tree_owned(&before)
		|| !S_ISDIR(before.st_mode)) return false;
	fd = openat(parent, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0) return false;
	dir = fdopendir(fd);
	if (dir == NULL) { close(fd); return false; }
	for (;;)
	{
		char path[MAXPGPATH];
		TreeEntry entry = {0};
		int child;
		unsigned slot;
		size_t length;
		errno = 0;
		item = readdir(dir);
		if (item == NULL) { if (errno == 0) ok = true; break; }
		if (strcmp(item->d_name, ".") == 0 || strcmp(item->d_name, "..") == 0) continue;
		if (!tree_name((const unsigned char *)item->d_name)
			|| snprintf(path, sizeof(path), "%s%s%s", prefix, *prefix ? "/" : "", item->d_name) >= sizeof(path)
			|| fstatat(parent, item->d_name, &entry.observed, AT_SYMLINK_NOFOLLOW) != 0
			|| !tree_owned(&entry.observed)) break;
		if (scan->recheck && tree_derived(path)) {
			if ((strcmp(path, "global/wal_startup") == 0) != S_ISDIR(entry.observed.st_mode)) break;
			continue;
		}
		length = strlen(path) + 1;
		if (scan->count == TREE_ENTRIES || length > TREE_PATH_BYTES - scan->paths) break;
		if (scan->count == scan->capacity)
		{
			unsigned capacity = scan->capacity ? scan->capacity * 2 : 64;
			TreeEntry *entries = realloc(scan->entries, capacity * sizeof(*entries));
			if (entries == NULL) break;
			scan->entries = entries; scan->capacity = capacity;
		}
		entry.path = strdup(path);
		if (entry.path == NULL) break;
		scan->paths += length;
		slot = scan->count++;
		scan->entries[slot] = entry;
		child = openat(parent, item->d_name, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC
			| (S_ISDIR(entry.observed.st_mode) ? O_DIRECTORY : 0));
		if (child < 0) break;
		ok = fstat(child, &after) == 0 && tree_same(&entry.observed, &after);
		if (ok && S_ISDIR(entry.observed.st_mode)) ok = tree_scan(scan, child, path, depth + 1);
		else if (ok) ok = tree_file(child, &entry.observed, entry.hash);
		if (fstatat(parent, item->d_name, &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !tree_same(&entry.observed, &named)) ok = false;
		if (close(child) != 0) ok = false;
		if (!ok) break;
		/* Recursion may have grown entries; use the reserved index. */
		scan->entries[slot] = entry;
		ok = false;
	}
	if (fstat(parent, &after) != 0 || !tree_same(&before, &after)) ok = false;
	if (closedir(dir) != 0) ok = false;
	return ok;
}

static int
tree_compare(const void *left, const void *right)
{
	return strcmp(((const TreeEntry *)left)->path, ((const TreeEntry *)right)->path);
}

static bool
tree_identity(pg_cryptohash_ctx *ctx, const struct stat *st)
{
	if (!tree_number(ctx, st->st_dev, 8) || !tree_number(ctx, st->st_ino, 8)
		|| !tree_number(ctx, st->st_mode, 8) || !tree_number(ctx, st->st_uid, 8)) return false;
	/* Adding the creator's derived files changes directory metadata only. */
	if (S_ISDIR(st->st_mode)) return true;
	if (!tree_number(ctx, st->st_nlink, 8) || !tree_number(ctx, st->st_size, 8)) return false;
#ifdef __APPLE__
	return tree_number(ctx, st->st_mtimespec.tv_sec, 8) && tree_number(ctx, st->st_mtimespec.tv_nsec, 8)
		&& tree_number(ctx, st->st_ctimespec.tv_sec, 8) && tree_number(ctx, st->st_ctimespec.tv_nsec, 8);
#else
	return tree_number(ctx, st->st_mtim.tv_sec, 8) && tree_number(ctx, st->st_mtim.tv_nsec, 8)
		&& tree_number(ctx, st->st_ctim.tv_sec, 8) && tree_number(ctx, st->st_ctim.tv_nsec, 8);
#endif
}

static bool
tree_recheck(int root, const TreeEntry *entry)
{
	char path[MAXPGPATH], *part, *next;
	struct stat now;
	int parent = openat(root, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	bool ok = false;
	if (parent < 0) return false;
	strlcpy(path, entry->path, sizeof(path));
	part = path;
	while ((next = strchr(part, '/')) != NULL)
	{
		int child;
		*next = 0;
		child = openat(parent, part, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (close(parent) != 0) { if (child >= 0) close(child); return false; }
		if (child < 0) return false;
		parent = child; part = next + 1;
	}
	ok = fstatat(parent, part, &now, AT_SYMLINK_NOFOLLOW) == 0
		&& tree_same(&entry->observed, &now);
	if (close(parent) != 0) ok = false;
	return ok;
}

bool
cluster_initdb_tree_read(int fd, bool recheck, ClusterInitdbTree *out)
{
	static const char domain[] = "PGRAC-CREATION-TREE-V1";
	static const char identity_domain[] = "PGRAC-CREATION-OBSERVATION-V1";
	pg_cryptohash_ctx *hash = NULL, *identity = NULL;
	TreeScan scan = {0};
	struct stat root, after;
	ClusterInitdbTree result;
	bool ok = false;
	if (out == NULL) return false;
	memset(out, 0, sizeof(*out));
	scan.recheck = recheck;
	if (fstat(fd, &root) != 0 || !tree_scan(&scan, fd, "", 0)) goto done;
	if (scan.count > 1) qsort(scan.entries, scan.count, sizeof(*scan.entries), tree_compare);
	hash = pg_cryptohash_create(PG_SHA256); identity = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL || identity == NULL || pg_cryptohash_init(hash) < 0
		|| pg_cryptohash_init(identity) < 0 || pg_cryptohash_update(hash, (const uint8 *)domain, sizeof(domain)) < 0
		|| pg_cryptohash_update(identity, (const uint8 *)identity_domain, sizeof(identity_domain)) < 0
		|| !tree_identity(identity, &root)) goto done;
	for (unsigned i = 0; i < scan.count; i++)
	{
		const TreeEntry *entry = &scan.entries[i];
		size_t length = strlen(entry->path);
		bool directory = S_ISDIR(entry->observed.st_mode);
		if (!tree_recheck(fd, entry)) goto done;
		if (!tree_number(hash, directory ? 1 : 2, 1) || !tree_number(hash, length, 4)
			|| pg_cryptohash_update(hash, (const uint8 *)entry->path, length) < 0
			|| !tree_number(hash, directory ? 0 : entry->observed.st_size, 8)
			|| pg_cryptohash_update(hash, entry->hash, 32) < 0
			|| !tree_number(identity, length, 4)
			|| pg_cryptohash_update(identity, (const uint8 *)entry->path, length) < 0
			|| !tree_identity(identity, &entry->observed)) goto done;
	}
	if (fstat(fd, &after) != 0 || !tree_same(&root, &after)) goto done;
	if (pg_cryptohash_final(hash, result.content, 32) < 0
		|| pg_cryptohash_final(identity, result.identity, 32) < 0) goto done;
	*out = result; ok = true;
done:
	if (hash != NULL) pg_cryptohash_free(hash);
	if (identity != NULL) pg_cryptohash_free(identity);
	for (unsigned i = 0; i < scan.count; i++) free(scan.entries[i].path);
	free(scan.entries);
	return ok;
}
