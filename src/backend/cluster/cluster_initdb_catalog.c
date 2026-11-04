/* Original catalog source reading and exclusive allocator publication.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "access/clog_file.h"
#include "cluster/cluster_xid_authority.h"
#include "cluster_initdb_catalog_private.h"
#include "cluster_initdb_origin_private.h"

static bool
catalog_directory(int fd, struct stat *st)
{
	return fstat(fd, st) == 0 && S_ISDIR(st->st_mode) && st->st_uid == geteuid()
		   && (st->st_mode & 0022) == 0;
}

static bool
catalog_directory_current(int fd, const struct stat *expected)
{
	struct stat now;
	return catalog_directory(fd, &now) && now.st_dev == expected->st_dev
		   && now.st_ino == expected->st_ino && now.st_mode == expected->st_mode;
}

bool
cluster_initdb_catalog_read_clog(int directory, uint64 native_hw, uint8 **bytes, Size *length)
{
	struct stat parent;
	uint8 *result;
	Size needed, offset = 0;
	bool ok = false;
	int fd = -1;

	if (bytes == NULL || length == NULL)
		return false;
	*bytes = NULL;
	*length = 0;
	if (native_hw < FirstNormalTransactionId || native_hw > CLUSTER_XID_PREHISTORY_MAX_XID
		|| !catalog_directory(directory, &parent))
		return false;
	needed = ((native_hw + BLCKSZ * 4 - 1) / (BLCKSZ * 4)) * BLCKSZ;
	result = malloc(needed);
	if (result == NULL)
		return false;
	while (offset < needed) {
		char name[16];
		struct stat file;
		Size part = Min(needed - offset, SLRU_PAGES_PER_SEGMENT * BLCKSZ), used = 0;
		snprintf(name, sizeof(name), "%04X",
				 (unsigned)(offset / (SLRU_PAGES_PER_SEGMENT * BLCKSZ)));
		fd = openat(directory, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
		if (fd < 0 || fstat(fd, &file) != 0 || !S_ISREG(file.st_mode) || file.st_uid != geteuid()
			|| file.st_nlink != 1 || (file.st_mode & 0022) != 0 || file.st_size != part)
			goto done;
		while (used < part) {
			ssize_t n = pread(fd, result + offset + used, part - used, used);
			if (n < 0 && errno == EINTR)
				continue;
			if (n <= 0)
				goto done;
			used += n;
		}
		if (close(fd) != 0) {
			fd = -1;
			goto done;
		}
		fd = -1;
		if (!cluster_initdb_object_recheck(directory, name, result + offset, part, &file))
			goto done;
		offset += part;
	}
	if (!catalog_directory_current(directory, &parent))
		goto done;
	ok = true;
done:
	if (fd >= 0 && close(fd) != 0)
		ok = false;
	if (!ok) {
		free(result);
		return false;
	}
	*bytes = result;
	*length = needed;
	return true;
}

bool
cluster_initdb_catalog_read_managed_clog_v2(
	int directory, const ClusterRecoveryFileIdentityV2 *expected, uint64 native_hw,
	uint8 **bytes, Size *length)
{
	struct stat parent, file;
	ClusterClogFileBindingV2 binding = {0};
	ClusterClogPageIdentityV2 page;
	uint8 image[CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES];
	uint8 *result = NULL;
	Size used = 0;
	int fd = -1;
	bool ok = false;

	if (bytes == NULL || length == NULL)
		return false;
	*bytes = NULL;
	*length = 0;
	if (expected == NULL || expected->kind != CLUSTER_RECOVERY_FILE_CLOG_V2
		|| expected->object_id != 0 || expected->database_oid != 0
		|| expected->tablespace_oid != 0 || native_hw < FirstNormalTransactionId
		|| native_hw > CLUSTER_CLOG_PAGE_BYTES * 4
		|| !catalog_directory(directory, &parent))
		return false;
	binding.identity = *expected;
	result = malloc(CLUSTER_CLOG_PAGE_BYTES);
	if (result == NULL)
		return false;
	fd = openat(directory, "0000", O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &file) != 0 || !S_ISREG(file.st_mode)
		|| file.st_uid != geteuid() || file.st_nlink != 1 || (file.st_mode & 0022) != 0
		|| file.st_size != sizeof(image))
		goto done;
	while (used < sizeof(image)) {
		ssize_t n = pread(fd, image + used, sizeof(image) - used, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	if (!cluster_clog_file_header_matches_v2(image, CLUSTER_CLOG_FILE_HEADER_BYTES, &binding)
		|| !cluster_clog_file_page_decode_v2(&binding, 0,
			image + CLUSTER_CLOG_FILE_HEADER_BYTES, CLUSTER_CLOG_FRAME_BYTES,
			&page, result, CLUSTER_CLOG_PAGE_BYTES)
		|| page.identity.page_generation != FirstNormalTransactionId
		|| !FullTransactionIdEquals(page.identity.birth_full_xid, FirstNormalFullTransactionId))
		goto done;
	if (close(fd) != 0) {
		fd = -1;
		goto done;
	}
	fd = -1;
	if (!cluster_initdb_object_recheck(directory, "0000", image, sizeof(image), &file)
		|| !catalog_directory_current(directory, &parent))
		goto done;
	ok = true;
done:
	if (fd >= 0 && close(fd) != 0)
		ok = false;
	if (!ok) {
		free(result);
		return false;
	}
	*bytes = result;
	*length = CLUSTER_CLOG_PAGE_BYTES;
	return true;
}

bool
cluster_initdb_catalog_create(int global, const ClusterCatalogInitialInput *input)
{
	static const struct {
		const char *name;
		ClusterCatalogInitialKind kind;
	} objects[] = { { "pgrac_oid_authority", CLUSTER_CATALOG_INITIAL_OID },
					{ "pgrac_catalog_authority", CLUSTER_CATALOG_INITIAL_MARKER },
					{ "pgrac_xid_authority", CLUSTER_CATALOG_INITIAL_XID },
					{ "pgrac_xid_authority.bak", CLUSTER_CATALOG_INITIAL_XID },
					{ "pgrac_xid_prehistory", CLUSTER_CATALOG_INITIAL_PREHISTORY },
					{ "pgrac_xid_prehistory.bak", CLUSTER_CATALOG_INITIAL_PREHISTORY } };
	uint8 *images[4] = { 0 };
	Size sizes[4] = { 0 };
	struct stat parent, st;
	bool ok = false;

	if (!catalog_directory(global, &parent))
		return false;
	for (unsigned kind = 0; kind < lengthof(images); kind++) {
		sizes[kind] = cluster_catalog_initial_image_size(kind, input);
		if (sizes[kind] == 0 || (images[kind] = calloc(1, sizes[kind])) == NULL
			|| !cluster_catalog_initial_image(kind, input, images[kind], sizes[kind]))
			goto done;
	}
	for (unsigned i = 0; i < lengthof(objects); i++)
		if (fstatat(global, objects[i].name, &st, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
			goto done;
	for (unsigned i = 0; i < lengthof(objects); i++) {
		unsigned kind = objects[i].kind;
		if (!catalog_directory_current(global, &parent)
			|| !cluster_initdb_object_write_new(global, objects[i].name, images[kind], sizes[kind]))
			goto done;
	}
	ok = catalog_directory_current(global, &parent);
done:
	for (unsigned kind = 0; kind < lengthof(images); kind++)
		free(images[kind]);
	return ok;
}
