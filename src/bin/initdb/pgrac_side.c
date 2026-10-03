/* Original initdb's per-origin native state, after its child has exited.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/slrudefs.h"
#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "pgrac_side.h"

#define SIDE_FILES_MAX 128

static const char *const directories[] = {
	"pg_xact", "pg_subtrans", "pg_multixact", "pg_commit_ts", "offsets", "members"
};
static const unsigned parents[] = {0, 0, 0, 0, 3, 3};

typedef struct SideFile
{
	unsigned directory;
	char name[9];
	struct stat identity;
} SideFile;

static bool
same_file(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_size == b->st_size && a->st_mode == b->st_mode
		&& a->st_uid == b->st_uid && a->st_nlink == b->st_nlink
		&& a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}

static int
open_directory(int parent, const char *name)
{
	struct stat held, named;
	int fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);

	if (fd >= 0 && fstat(fd, &held) == 0
		&& fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) == 0
		&& same_file(&held, &named) && S_ISDIR(held.st_mode)
		&& held.st_uid == geteuid() && (held.st_mode & 0022) == 0)
		return fd;
	if (fd >= 0) close(fd);
	return -1;
}

static bool
directory_current(int parent, const char *name, int fd)
{
	struct stat held, named;
	return fstat(fd, &held) == 0
		&& fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) == 0
		&& same_file(&held, &named) && S_ISDIR(held.st_mode)
		&& held.st_uid == geteuid() && (held.st_mode & 0022) == 0;
}

static bool
segment_name(const char *name)
{
	char *end, canonical[9];
	unsigned long segment;
	size_t length = strlen(name);

	if (length < 4 || length > 8 || strspn(name, "0123456789ABCDEF") != length)
		return false;
	errno = 0;
	segment = strtoul(name, &end, 16);
	if (errno != 0 || *end != '\0' || segment > UINT32_MAX)
		return false;
	snprintf(canonical, sizeof(canonical), "%04X", (uint32) segment);
	return strcmp(canonical, name) == 0;
}

static bool
collect(int dirs[7], SideFile *files, unsigned *count)
{
	for (unsigned d = 1; d <= lengthof(directories); ++d)
	{
		int fd = open_directory(dirs[d], ".");
		DIR *stream;
		struct dirent *entry;
		bool valid = true;

		if (fd < 0) return false;
		stream = fdopendir(fd);
		if (stream == NULL) { close(fd); return false; }
		errno = 0;
		while ((entry = readdir(stream)) != NULL)
		{
			SideFile *file;
			if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
				continue;
			if (d == 3)
			{
				if (strcmp(entry->d_name, "offsets") == 0 || strcmp(entry->d_name, "members") == 0)
					continue;
				valid = false; break;
			}
			/* Shared commit timestamps are unsupported, including old input. */
			if (d == 4 || *count == SIDE_FILES_MAX || !segment_name(entry->d_name))
			{ valid = false; break; }
			file = &files[(*count)++];
			file->directory = d;
			strlcpy(file->name, entry->d_name, sizeof(file->name));
			if (fstatat(dirs[d], file->name, &file->identity, AT_SYMLINK_NOFOLLOW) != 0
				|| !S_ISREG(file->identity.st_mode) || file->identity.st_uid != geteuid()
				|| file->identity.st_nlink != 1 || (file->identity.st_mode & 0022) != 0
				|| file->identity.st_size <= 0 || file->identity.st_size % BLCKSZ != 0
				|| file->identity.st_size > BLCKSZ * SLRU_PAGES_PER_SEGMENT)
			{ valid = false; break; }
			errno = 0;
		}
		if (errno != 0) valid = false;
		if (closedir(stream) != 0) valid = false;
		if (!valid) return false;
	}
	return *count > 0;
}

static bool
copy_file(int source, int target, const SideFile *file)
{
	int input = -1, output = -1;
	pg_cryptohash_ctx *written = NULL, *actual = NULL;
	uint8 expected[32], observed[32];
	struct stat held, named;
	PGAlignedBlock page;
	bool ok = false;

	input = openat(source, file->name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (input < 0 || fstat(input, &held) != 0 || !same_file(&held, &file->identity)) goto done;
	output = openat(target, file->name,
		O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, pg_file_create_mode);
	if (output < 0) goto done;
	written = pg_cryptohash_create(PG_SHA256);
	actual = pg_cryptohash_create(PG_SHA256);
	if (written == NULL || actual == NULL
		|| pg_cryptohash_init(written) < 0 || pg_cryptohash_init(actual) < 0) goto done;
	for (off_t offset = 0; offset < file->identity.st_size; offset += BLCKSZ)
	{
		ssize_t n;
		do { n = pread(input, page.data, BLCKSZ, offset); } while (n < 0 && errno == EINTR);
		if (n != BLCKSZ || pg_cryptohash_update(written, (uint8 *) page.data, BLCKSZ) < 0) goto done;
		do { n = pwrite(output, page.data, BLCKSZ, offset); } while (n < 0 && errno == EINTR);
		if (n != BLCKSZ) goto done;
	}
	if (fstat(input, &held) != 0 || fstatat(source, file->name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_file(&held, &named) || !same_file(&held, &file->identity) || fsync(output) != 0) goto done;
	for (off_t offset = 0; offset < file->identity.st_size; offset += BLCKSZ)
	{
		ssize_t n;
		do { n = pread(output, page.data, BLCKSZ, offset); } while (n < 0 && errno == EINTR);
		if (n != BLCKSZ || pg_cryptohash_update(actual, (uint8 *) page.data, BLCKSZ) < 0) goto done;
	}
	if (pg_cryptohash_final(written, expected, sizeof(expected)) < 0
		|| pg_cryptohash_final(actual, observed, sizeof(observed)) < 0
		|| memcmp(expected, observed, sizeof(expected)) != 0
		|| fstat(output, &held) != 0 || fstatat(target, file->name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_file(&held, &named) || held.st_size != file->identity.st_size
		|| held.st_uid != geteuid() || held.st_nlink != 1 || (held.st_mode & 0022) != 0) goto done;
	ok = true;
done:
	if (input >= 0 && close(input) != 0) ok = false;
	if (output >= 0 && close(output) != 0) ok = false;
	pg_cryptohash_free(actual);
	pg_cryptohash_free(written);
	return ok;
}

bool
pgrac_initdb_side_create(int source_fd, int shared_fd)
{
	int source[7], target[7], native = -1;
	SideFile *files = pg_malloc0(sizeof(*files) * SIDE_FILES_MAX);
	unsigned count = 0;
	bool ok = false;
	struct stat a, b;

	for (unsigned i = 0; i < lengthof(source); ++i) source[i] = target[i] = -1;
	source[0] = source_fd;
	if (fstat(source_fd, &a) != 0 || fstat(shared_fd, &b) != 0
		|| !S_ISDIR(a.st_mode) || !S_ISDIR(b.st_mode) || a.st_uid != geteuid() || b.st_uid != geteuid()
		|| ((a.st_mode | b.st_mode) & 0022) != 0 || (a.st_dev == b.st_dev && a.st_ino == b.st_ino)) goto done;
	for (unsigned i = 0; i < lengthof(directories); ++i)
	{
		source[i + 1] = open_directory(source[parents[i]], directories[i]);
		if (source[i + 1] < 0) goto done;
	}
	if (!collect(source, files, &count)) goto done;
	/* The creator can only make a new namespace. Failure never adopts one. */
	if (mkdirat(shared_fd, "native_side", pg_dir_create_mode) != 0) goto done;
	native = open_directory(shared_fd, "native_side");
	if (native < 0 || mkdirat(native, "origin_0", pg_dir_create_mode) != 0) goto done;
	target[0] = open_directory(native, "origin_0");
	if (target[0] < 0) goto done;
	for (unsigned i = 0; i < lengthof(directories); ++i)
	{
		if (mkdirat(target[parents[i]], directories[i], pg_dir_create_mode) != 0) goto done;
		target[i + 1] = open_directory(target[parents[i]], directories[i]);
		if (target[i + 1] < 0) goto done;
	}
	for (unsigned i = 0; i < count; ++i)
		if (!copy_file(source[files[i].directory], target[files[i].directory], &files[i])) goto done;
	for (int i = lengthof(directories) - 1; i >= 0; --i)
		if (!directory_current(source[parents[i]], directories[i], source[i + 1])
			|| fsync(target[i + 1]) != 0
			|| !directory_current(target[parents[i]], directories[i], target[i + 1])) goto done;
	if (fsync(target[0]) != 0 || !directory_current(native, "origin_0", target[0])
		|| fsync(native) != 0 || !directory_current(shared_fd, "native_side", native)
		|| fsync(shared_fd) != 0) goto done;
	ok = true;
done:
	for (unsigned i = 0; i < lengthof(target); ++i)
	{
		if (target[i] >= 0 && close(target[i]) != 0) ok = false;
		if (i > 0 && source[i] >= 0 && close(source[i]) != 0) ok = false;
	}
	if (native >= 0 && close(native) != 0) ok = false;
	free(files);
	return ok;
}
