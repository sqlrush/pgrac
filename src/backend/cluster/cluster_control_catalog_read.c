/* Exact catalog inputs selected by the postmaster's early ROOT observation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "cluster/cluster_xid_authority.h"
#include "common/cryptohash.h"
#include "cluster_control_catalog_private.h"
#include "cluster_control_root_private.h"
#include "cluster_initdb_catalog_private.h"
#include "cluster_initdb_origin_private.h"

typedef struct CatalogReadObject {
	char name[112];
	unsigned directory;
	uint8 *bytes;
	Size length;
	struct stat identity;
} CatalogReadObject;

struct ClusterControlCatalogRead {
	char local[MAXPGPATH], shared[MAXPGPATH];
	ClusterControlBootstrapSnapshot expected;
	struct stat directories[8];
	CatalogReadObject objects[8];
	uint8 *clog;
	Size clog_length;
	struct stat clog_files[CLUSTER_XID_PREHISTORY_MAX_XID / (4 * BLCKSZ * SLRU_PAGES_PER_SEGMENT)];
	unsigned clog_count;
	ControlRootImage root;
	bool valid;
};

static bool
catalog_read_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & 0022) == 0 && (directory || st->st_nlink == 1);
}

static bool
catalog_read_dirs(ClusterControlCatalogRead *read, int dirs[8], bool remember)
{
	const char *names[] = { read->local,
							read->shared,
							"global",
							"control_images",
							"catalog_checkpoints",
							"pgrac_initdb_native_side",
							"pg_xact",
							"global" };
	const int parents[] = { -1, -1, 1, 2, 2, 0, 5, 0 };
	for (unsigned i = 0; i < 8; i++)
		dirs[i] = -1;
	for (unsigned i = 0; i < 8; i++) {
		struct stat st, named;
		int parent = parents[i] < 0 ? AT_FDCWD : dirs[parents[i]];
		dirs[i] = openat(parent, names[i], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (dirs[i] < 0 || fstat(dirs[i], &st) != 0 || !catalog_read_owned(&st, true)
			|| fstatat(parent, names[i], &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !catalog_read_owned(&named, true) || named.st_dev != st.st_dev
			|| named.st_ino != st.st_ino)
			return false;
		if (remember)
			read->directories[i] = st;
		else if (st.st_dev != read->directories[i].st_dev
				 || st.st_ino != read->directories[i].st_ino
				 || st.st_mode != read->directories[i].st_mode)
			return false;
	}
	return true;
}

static bool
catalog_read_close(int dirs[8], bool valid)
{
	for (unsigned i = 0; i < 8; i++)
		if (dirs[i] >= 0 && close(dirs[i]) != 0)
			valid = false;
	return valid;
}

static bool
catalog_read_dirs_current(ClusterControlCatalogRead *read, const int dirs[8])
{
	const char *names[] = { read->local,
							read->shared,
							"global",
							"control_images",
							"catalog_checkpoints",
							"pgrac_initdb_native_side",
							"pg_xact",
							"global" };
	const int parents[] = { -1, -1, 1, 2, 2, 0, 5, 0 };
	for (unsigned i = 0; i < 8; i++) {
		struct stat st;
		int parent = parents[i] < 0 ? AT_FDCWD : dirs[parents[i]];
		if (fstatat(parent, names[i], &st, AT_SYMLINK_NOFOLLOW) != 0
			|| !catalog_read_owned(&st, true) || st.st_dev != read->directories[i].st_dev
			|| st.st_ino != read->directories[i].st_ino
			|| st.st_mode != read->directories[i].st_mode)
			return false;
	}
	return true;
}

static bool
catalog_original_current(ClusterControlCatalogRead *read, int clogdir)
{
	Size offset = 0;
	for (unsigned i = 0; i < read->clog_count; i++) {
		char name[16];
		Size part = Min(read->clog_length - offset, SLRU_PAGES_PER_SEGMENT * BLCKSZ);
		snprintf(name, sizeof(name), "%04X", i);
		if (!cluster_initdb_object_recheck(clogdir, name, read->clog + offset, part,
										   &read->clog_files[i]))
			return false;
		offset += part;
	}
	return offset == read->clog_length && offset > 0;
}

/* Only malloc/syscalls while a raw descriptor is held. */
static bool
catalog_read_object(int parent, CatalogReadObject *object, Size minimum, Size maximum)
{
	int fd = -1;
	Size used = 0;
	bool ok = false;
	fd = openat(parent, object->name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &object->identity) != 0 || !catalog_read_owned(&object->identity, false)
		|| object->identity.st_size < minimum || object->identity.st_size > maximum)
		goto done;
	object->length = object->identity.st_size;
	object->bytes = malloc(object->length);
	if (object->bytes == NULL)
		goto done;
	while (used < object->length) {
		ssize_t n = pread(fd, object->bytes + used, object->length - used, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	ok = cluster_initdb_object_recheck(parent, object->name, object->bytes, object->length,
									   &object->identity);
done:
	if (fd >= 0 && close(fd) != 0)
		ok = false;
	return ok;
}

static bool
catalog_read_hash(const void *bytes, Size length, const uint8 expected[32])
{
	uint8 actual[32];
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) >= 0 && pg_cryptohash_update(hash, bytes, length) >= 0
		 && pg_cryptohash_final(hash, actual, sizeof(actual)) >= 0
		 && memcmp(actual, expected, sizeof(actual)) == 0;
	pg_cryptohash_free(hash);
	return ok;
}

static void
catalog_read_name(CatalogReadObject *object, unsigned directory, const char *name)
{
	object->directory = directory;
	strlcpy(object->name, name, sizeof(object->name));
}

bool
cluster_control_catalog_read(const char *pgdata, const char *shared,
							 const ClusterControlBootstrapSnapshot *expected,
							 ClusterControlCatalogRead **readp, ClusterCatalogStartupInput *out)
{
	ClusterControlCatalogRead *read;
	ClusterCatalogStartupInput input = { 0 };
	ControlFileData native;
	PgracControlBinding binding;
	int dirs[8];
	char hex[65], name[112];
	bool ok;
	const Size maximum = sizeof(ClusterXidPrehistoryHeader) + CLUSTER_XID_PREHISTORY_MAX_XID / 4;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (readp == NULL || *readp != NULL || expected == NULL || pgdata == NULL || shared == NULL
		|| pgdata[0] != '/' || shared[0] != '/' || strlen(pgdata) >= MAXPGPATH
		|| strlen(shared) >= MAXPGPATH
		|| expected->binding.lineage_kind != PGRAC_CONTROL_LINEAGE_CREATION_V1)
		return false;
	read = calloc(1, sizeof(*read));
	if (read == NULL)
		return false;
	*readp = read;
	strlcpy(read->local, pgdata, sizeof(read->local));
	strlcpy(read->shared, shared, sizeof(read->shared));
	read->expected = *expected;
	if (pgrac_control_binding_read(pgdata, &binding) != PGRAC_CONTROL_BINDING_OK
		|| memcmp(&binding, &expected->binding, sizeof(binding)) != 0)
		return false;
	catalog_read_name(&read->objects[0], 2, "pgrac_control_root");
	ok = catalog_read_dirs(read, dirs, true);
	if (ok)
		ok = catalog_read_object(dirs[2], &read->objects[0], CLUSTER_CONTROL_ROOT_FILE_BYTES,
								 CLUSTER_CONTROL_ROOT_FILE_BYTES);
	if (ok)
		ok = catalog_read_dirs_current(read, dirs);
	if (!catalog_read_close(dirs, ok))
		return false;
	if (!catalog_read_hash(read->objects[0].bytes, read->objects[0].length, expected->root_sha256)
		|| cluster_control_bootstrap_root_decode(read->objects[0].bytes, read->objects[0].length,
												 binding.storage_uuid, binding.system_identifier,
												 &read->root)
			   != 0
		|| cluster_control_bootstrap_root_bound(&binding, &read->root) != 0
		|| read->root.header.file_txn_seq != expected->root_sequence
		|| read->root.header.v2.control_image_generation != 1)
		return false;
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + 2 * i, 3, "%02x", read->root.header.v2.control_image_sha256[i]);
	snprintf(name, sizeof(name), "1-%s.bin", hex);
	catalog_read_name(&read->objects[1], 3, name);
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + 2 * i, 3, "%02x", read->root.header.v2.catalog_manifest_sha256[i]);
	snprintf(name, sizeof(name), UINT64_FORMAT "-%s.json",
			 read->root.header.v2.catalog_manifest_generation, hex);
	catalog_read_name(&read->objects[2], 4, name);
	catalog_read_name(&read->objects[3], 2, "pgrac_oid_authority");
	catalog_read_name(&read->objects[4], 2, "pgrac_catalog_authority");
	catalog_read_name(&read->objects[5], 2, "pgrac_xid_authority");
	catalog_read_name(&read->objects[6], 2, "pgrac_xid_authority.bak");
	catalog_read_name(&read->objects[7], 2, "pgrac_xid_prehistory");
	ok = catalog_read_dirs(read, dirs, false);
	for (unsigned i = 1; ok && i < 8; i++) {
		Size limit = i == 1	  ? PG_CONTROL_FILE_SIZE
					 : i == 2 ? CLUSTER_CATALOG_MANIFEST_MAX_BYTES
					 : i == 7 ? maximum
							  : 128;
		ok = catalog_read_object(dirs[read->objects[i].directory], &read->objects[i], 1, limit);
	}
	if (ok && read->objects[1].length != PG_CONTROL_FILE_SIZE)
		ok = false;
	if (ok) {
		uint64 hw;
		memcpy(&native, read->objects[1].bytes, sizeof(native));
		hw = U64FromFullTransactionId(native.checkPointCopy.nextXid);
		ok = hw >= FirstNormalTransactionId && hw <= CLUSTER_XID_PREHISTORY_MAX_XID;
		if (ok)
			read->clog_count = (hw + 4 * BLCKSZ * SLRU_PAGES_PER_SEGMENT - 1)
							   / (4 * BLCKSZ * SLRU_PAGES_PER_SEGMENT);
		for (unsigned i = 0; ok && i < read->clog_count; i++) {
			snprintf(name, sizeof(name), "%04X", i);
			ok = fstatat(dirs[6], name, &read->clog_files[i], AT_SYMLINK_NOFOLLOW) == 0
				 && catalog_read_owned(&read->clog_files[i], false);
		}
		if (ok)
			ok = cluster_initdb_catalog_read_clog(dirs[6], hw, &read->clog, &read->clog_length)
				 && catalog_original_current(read, dirs[6]);
	}
	if (ok)
		ok = catalog_read_dirs_current(read, dirs);
	if (!catalog_read_close(dirs, ok))
		return false;
	if (!catalog_read_hash(read->objects[1].bytes, read->objects[1].length,
						   read->root.header.v2.control_image_sha256))
		return false;
	input.original.identity.system_identifier = binding.system_identifier;
	input.original.identity.database_incarnation = binding.database_incarnation;
	memcpy(input.original.identity.storage_uuid, binding.storage_uuid, 16);
	memcpy(input.original.identity.authority_uuid, binding.authority_uuid, 16);
	input.original.native_control = read->objects[1].bytes;
	input.original.native_control_length = sizeof(ControlFileData);
	input.original.native_clog = read->clog;
	input.original.native_clog_length = read->clog_length;
	input.catalog_generation = read->root.header.v2.catalog_manifest_generation;
	memcpy(input.catalog_sha256, read->root.header.v2.catalog_manifest_sha256, 32);
#define CATALOG_IMAGE(field, i)                                                                    \
	input.field.bytes = read->objects[i].bytes;                                                    \
	input.field.length = read->objects[i].length
	CATALOG_IMAGE(manifest, 2);
	CATALOG_IMAGE(oid, 3);
	CATALOG_IMAGE(marker, 4);
	CATALOG_IMAGE(xid, 5);
	CATALOG_IMAGE(xid_backup, 6);
	CATALOG_IMAGE(prehistory, 7);
#undef CATALOG_IMAGE
	read->valid = true;
	*out = input;
	return true;
}

bool
cluster_control_catalog_current(ClusterControlCatalogRead *read)
{
	PgracControlBinding binding;
	int dirs[8];
	bool ok;
	if (read == NULL || !read->valid)
		return false;
	if (pgrac_control_binding_read(read->local, &binding) != PGRAC_CONTROL_BINDING_OK
		|| memcmp(&binding, &read->expected.binding, sizeof(binding)) != 0)
		return false;
	ok = catalog_read_dirs(read, dirs, false);
	for (unsigned i = 1; ok && i < 8; i++) {
		const CatalogReadObject *object = &read->objects[i];
		ok = cluster_initdb_object_recheck(dirs[object->directory], object->name, object->bytes,
										   object->length, &object->identity);
	}
	if (ok)
		ok = catalog_original_current(read, dirs[6]) && catalog_read_dirs_current(read, dirs);
	if (ok)
		ok = cluster_initdb_object_recheck(dirs[2], read->objects[0].name, read->objects[0].bytes,
										   read->objects[0].length, &read->objects[0].identity);
	return catalog_read_close(dirs, ok);
}

void
cluster_control_catalog_release(ClusterControlCatalogRead **readp)
{
	ClusterControlCatalogRead *read;
	if (readp == NULL || *readp == NULL)
		return;
	read = *readp;
	*readp = NULL;
	for (unsigned i = 0; i < 8; i++)
		free(read->objects[i].bytes);
	free(read->clog);
	free(read);
}
