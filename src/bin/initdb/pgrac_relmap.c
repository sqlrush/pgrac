/* Original initdb's durable shared relmap files, after its child has exited.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_database_d.h"
#include "cluster/cluster_relmap_authority.h"
#include "common/file_perm.h"
#include "common/relmap.h"
#include "pgrac_relmap.h"

#define AUTHORITY_NAME "pgrac_relmap_authority"
#define MAP_NAME "pg_filenode.map"
static const char *const authority_names[] = { AUTHORITY_NAME, AUTHORITY_NAME ".bak" };

/* Root, global, base, and the three databases made by this initdb. */
static const unsigned map_directories[] = {1, 3, 4, 5};
static const Oid map_databases[] = {InvalidOid, Template1DbOid, Template0DbOid, PostgresDbOid};
static const unsigned parents[] = {0, 0, 2, 2, 2};
static const char *const directories[] = {
	"global", "base", CppAsString2(Template1DbOid),
	CppAsString2(Template0DbOid), CppAsString2(PostgresDbOid)
};

typedef struct InitialMap
{
	int fd;
	struct stat identity;
	RelMapFile native;
	size_t image_size;
	char image[CLUSTER_RELMAP_V2_FILE_SIZE];
	struct stat created[2];
} InitialMap;

static bool
same_file(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_size == b->st_size && a->st_mode == b->st_mode
		&& a->st_uid == b->st_uid && a->st_nlink == b->st_nlink
		&& a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}

static bool
owned_directory(int fd, struct stat *st)
{
	return fstat(fd, st) == 0 && S_ISDIR(st->st_mode)
		&& st->st_uid == geteuid() && (st->st_mode & 0022) == 0;
}

static bool
directories_current(int dirs[6])
{
	struct stat held, named;
	if (!owned_directory(dirs[0], &held)) return false;
	for (unsigned i = 0; i < lengthof(directories); ++i)
		if (!owned_directory(dirs[i + 1], &held)
			|| fstatat(dirs[parents[i]], directories[i], &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !same_file(&held, &named)) return false;
	return true;
}

static bool
open_directories(int dirs[6])
{
	for (unsigned i = 0; i < lengthof(directories); ++i)
	{
		dirs[i + 1] = openat(dirs[parents[i]], directories[i],
			O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (dirs[i + 1] < 0) return false;
	}
	return directories_current(dirs);
}

static bool
database_set_exact(int base)
{
	int fd = openat(base, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	DIR *stream;
	const struct dirent *entry;
	unsigned found = 0;
	bool ok = true;

	if (fd < 0) return false;
	stream = fdopendir(fd);
	if (stream == NULL) { close(fd); return false; }
	while (errno = 0, (entry = readdir(stream)) != NULL)
	{
		bool recognized = false;
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
		for (unsigned i = 2; i < lengthof(directories); ++i)
			if (!strcmp(entry->d_name, directories[i]))
			{
				if (found & (1U << i)) { ok = false; break; }
				found |= 1U << i; recognized = true; break;
			}
		if (!recognized) { ok = false; break; }
	}
	if (errno != 0 || found != ((1U << 2) | (1U << 3) | (1U << 4))) ok = false;
	if (closedir(stream) != 0) ok = false;
	return ok;
}

static bool
absent_authority(int directory)
{
	const char *const names[] = {AUTHORITY_NAME, AUTHORITY_NAME ".bak",
		AUTHORITY_NAME ".tmp", AUTHORITY_NAME ".bak.tmp"};
	struct stat st;
	for (unsigned i = 0; i < lengthof(names); ++i)
		if (fstatat(directory, names[i], &st, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
			return false;
	return true;
}

static bool
source_current(int directory, const InitialMap *map)
{
	struct stat held, named;
	RelMapFile actual;
	ssize_t n;
	do { n = pread(map->fd, &actual, sizeof(actual), 0); } while (n < 0 && errno == EINTR);
	return n == sizeof(actual) && memcmp(&actual, &map->native, sizeof(actual)) == 0
		&& fstat(map->fd, &held) == 0
		&& fstatat(directory, MAP_NAME, &named, AT_SYMLINK_NOFOLLOW) == 0
		&& same_file(&held, &named) && same_file(&held, &map->identity);
}

static bool
persist_image(int directory, InitialMap *map, unsigned copy)
{
	char actual[CLUSTER_RELMAP_V2_FILE_SIZE];
	struct stat held, named;
	int fd = openat(directory, authority_names[copy],
		O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, pg_file_create_mode);
	ssize_t n;
	bool ok = false;
	if (fd < 0) return false;
	do { n = pwrite(fd, map->image, map->image_size, 0); } while (n < 0 && errno == EINTR);
	if (n != map->image_size || fsync(fd) != 0) goto done;
	do { n = pread(fd, actual, map->image_size, 0); } while (n < 0 && errno == EINTR);
	if (n != map->image_size || memcmp(actual, map->image, map->image_size) != 0
		|| fstat(fd, &held) != 0
		|| fstatat(directory, authority_names[copy], &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_file(&held, &named) || !S_ISREG(held.st_mode)
		|| held.st_size != map->image_size || held.st_uid != geteuid()
		|| held.st_nlink != 1 || (held.st_mode & 0022) != 0
		|| fsync(directory) != 0) goto done;
	map->created[copy] = held;
	ok = true;
done:
	if (close(fd) != 0) ok = false;
	return ok;
}

/* Reopen every created copy after ALL writes, retaining its original inode. */
static bool
created_pair_current(int directory, const InitialMap *map,
					 const ClusterRelmapV2Namespace *identity)
{
	char bytes[2][CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapV2View view;
	for (unsigned c = 0; c < lengthof(authority_names); ++c)
	{
		struct stat held, named, after;
		ssize_t n;
		bool ok = false;
		int fd = openat(directory, authority_names[c], O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
		if (fd < 0) return false;
		if (fstat(fd, &held) != 0 || !same_file(&held, &map->created[c])) goto done;
		do { n = pread(fd, bytes[c], sizeof(bytes[c]), 0); } while (n < 0 && errno == EINTR);
		if (n != sizeof(bytes[c]) || memcmp(bytes[c], map->image, sizeof(bytes[c])) != 0
			|| fstat(fd, &after) != 0 || !same_file(&held, &after)
			|| fstatat(directory, authority_names[c], &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !same_file(&held, &named)) goto done;
		ok = true;
	done:
		if (close(fd) != 0) ok = false;
		if (!ok) return false;
	}
	return cluster_relmap_v2_read_pair(bytes[0], sizeof(bytes[0]), bytes[1], sizeof(bytes[1]),
			identity, &view) == CLUSTER_RELMAP_V2_PAIR_VALID
		&& view.pending_complete && !view.has_pending && view.committed_generation == 1
		&& memcmp(&view.committed, &map->native, sizeof(map->native)) == 0;
}

static bool
create_initial_maps(int source_fd, int shared_fd, bool v2,
					uint64 system_identifier, uint64 database_incarnation)
{
	int source[6] = {source_fd, -1, -1, -1, -1, -1};
	int target[6] = {shared_fd, -1, -1, -1, -1, -1};
	InitialMap maps[4] = {0};
	struct stat a, b;
	bool ok = false;

	for (unsigned i = 0; i < lengthof(maps); ++i) maps[i].fd = -1;
	if ((v2 && (system_identifier == 0 || database_incarnation == 0))
		|| !owned_directory(source_fd, &a) || !owned_directory(shared_fd, &b)
		|| (a.st_dev == b.st_dev && a.st_ino == b.st_ino)
		|| !open_directories(source) || !open_directories(target)
		|| !database_set_exact(source[2]) || !database_set_exact(target[2])) goto done;
	/* Validate every input and every destination before the first creation. */
	for (unsigned i = 0; i < lengthof(maps); ++i)
	{
		unsigned d = map_directories[i];
		InitialMap *map = &maps[i];
		ClusterRelmapV2Namespace identity = { system_identifier, database_incarnation,
			map_databases[i], i == 0 };
		ssize_t n;
		if (!owned_directory(source[d], &a) || !owned_directory(target[d], &b)
			|| (a.st_dev == b.st_dev && a.st_ino == b.st_ino)
			|| !absent_authority(target[d])) goto done;
		map->fd = openat(source[d], MAP_NAME, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
		if (map->fd < 0 || fstat(map->fd, &map->identity) != 0
			|| !S_ISREG(map->identity.st_mode) || map->identity.st_uid != geteuid()
			|| map->identity.st_nlink != 1 || (map->identity.st_mode & 0022) != 0
			|| map->identity.st_size != sizeof(map->native)) goto done;
		do { n = pread(map->fd, &map->native, sizeof(map->native), 0); } while (n < 0 && errno == EINTR);
		map->image_size = v2 ? CLUSTER_RELMAP_V2_FILE_SIZE : CLUSTER_RELMAP_AUTHORITY_FILE_SIZE;
		if (n != sizeof(map->native) || !source_current(source[d], map)) goto done;
		if ((v2 ? cluster_relmap_authority_v2_init_image(&identity, &map->native,
					  sizeof(map->native), map->image, map->image_size)
				: cluster_relmap_authority_init_image(i == 0, map_databases[i], &map->native,
					  sizeof(map->native), map->image, map->image_size)) != CLUSTER_RELMAP_INIT_OK) goto done;
	}
	for (unsigned i = 0; i < lengthof(maps); ++i)
	{
		for (unsigned c = 0; c < (v2 ? 2 : 1); ++c)
			if (!directories_current(source) || !directories_current(target)
				|| !source_current(source[map_directories[i]], &maps[i])
				|| !persist_image(target[map_directories[i]], &maps[i], c)) goto done;
	}
	for (unsigned i = 0; i < lengthof(maps); ++i)
		if (!source_current(source[map_directories[i]], &maps[i])) goto done;
	if (!database_set_exact(source[2]) || !database_set_exact(target[2])
		|| !directories_current(source) || !directories_current(target)
		|| fsync(target[2]) != 0 || fsync(shared_fd) != 0
		|| !directories_current(source) || !directories_current(target)) goto done;
	if (v2)
	{
		for (unsigned i = 0; i < lengthof(maps); ++i)
		{
			ClusterRelmapV2Namespace identity = { system_identifier, database_incarnation,
				map_databases[i], i == 0 };
			if (!created_pair_current(target[map_directories[i]], &maps[i], &identity)) goto done;
		}
		for (unsigned i = 0; i < lengthof(maps); ++i)
			if (!source_current(source[map_directories[i]], &maps[i])) goto done;
		if (!database_set_exact(source[2]) || !database_set_exact(target[2])
			|| !directories_current(source) || !directories_current(target)) goto done;
	}
	ok = true;
done:
	for (unsigned i = 0; i < lengthof(maps); ++i)
		if (maps[i].fd >= 0 && close(maps[i].fd) != 0) ok = false;
	for (unsigned i = 1; i < lengthof(source); ++i)
	{
		if (source[i] >= 0 && close(source[i]) != 0) ok = false;
		if (target[i] >= 0 && close(target[i]) != 0) ok = false;
	}
	return ok;
}

bool
pgrac_initdb_relmap_create(int source_fd, int shared_fd)
{
	return create_initial_maps(source_fd, shared_fd, false, 0, 0);
}

bool
pgrac_initdb_relmap_v2_create(int source_fd, int shared_fd,
								uint64 system_identifier, uint64 database_incarnation)
{
	return create_initial_maps(source_fd, shared_fd, true, system_identifier, database_incarnation);
}
