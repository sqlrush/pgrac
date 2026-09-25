/*-------------------------------------------------------------------------
 * PGRAC: bounded read-only early bootstrap observation, never admission.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#ifndef WIN32
#include <unistd.h>
#endif

#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster_control_bootstrap_private.h"
#include "cluster_control_root_private.h"
#include "cluster_recovery_anchor_private.h"

#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_SYMLINK_NOFOLLOW)

typedef struct BootstrapReadWork {
	ControlRootImage before;
	ControlRootImage after;
	uint8 root_before[CLUSTER_CONTROL_ROOT_FILE_BYTES + 1];
	uint8 root_after[CLUSTER_CONTROL_ROOT_FILE_BYTES + 1];
	uint8 common[PG_CONTROL_FILE_SIZE + 1];
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES + 1];
	uint8 anchor[CLUSTER_RECOVERY_ANCHOR_SIZE + 1];
} BootstrapReadWork;

static bool
read_path_valid(const char *path, const ClusterControlBootstrapObservation *out)
{
	size_t length = path != NULL ? strnlen(path, MAXPGPATH) : 0;
	size_t start = 1;
	uintptr_t a = (uintptr_t)path, b = (uintptr_t)out;

	if (path == NULL || length < 2 || length >= MAXPGPATH || path[0] != '/')
		return false;
	if (a <= b ? b - a < length + 1 : a - b < sizeof(*out))
		return false;
	for (size_t i = 1; i <= length; i++) {
		if (i == length || path[i] == '/') {
			size_t size = i - start;
			if (size == 0 || (size == 1 && path[start] == '.')
				|| (size == 2 && path[start] == '.' && path[start + 1] == '.'))
				return false;
			start = i + 1;
		}
	}
	return true;
}

static bool
read_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static ClusterControlRootResult
read_error(void)
{
	return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static void
read_close(int fd, ClusterControlRootResult *result)
{
	if (fd >= 0 && close(fd) != 0)
		*result = CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static ClusterControlRootResult
read_dir(int parent, const char *name, int *out)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	struct stat st;

	*out = parent < 0 ? open(name, flags) : openat(parent, name, flags);
	if (*out < 0)
		return read_error();
	if (fstat(*out, &st) != 0 || !read_owned(&st, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* bytes has capacity maximum + 1. No backend error-capable call while open. */
static ClusterControlRootResult
read_bytes(int dir, const char *name, size_t minimum, size_t maximum, uint8 *bytes, size_t *length)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	struct stat st;
	size_t used = 0;
	int fd;

	*length = 0;
	fd = openat(dir, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0)
		return read_error();
	if (fstat(fd, &st) != 0 || !read_owned(&st, false))
		goto done;
	if (st.st_size < 0 || (uint64)st.st_size < minimum || (uint64)st.st_size > maximum) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used <= (size_t)st.st_size) {
		ssize_t n = read(fd, bytes + used, (size_t)st.st_size + 1 - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	if (used != (size_t)st.st_size) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	*length = used;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	read_close(fd, &result);
	return result;
}

static ClusterControlRootResult
read_object(int base, const char *const *parts, size_t count, const char *name, size_t minimum,
			size_t maximum, uint8 *bytes, size_t *length)
{
	int dirs[3] = { -1, -1, -1 };
	int current = base;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

	if (count > lengthof(dirs))
		return result;
	for (size_t i = 0; i < count; i++) {
		result = read_dir(current, parts[i], &dirs[i]);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		current = dirs[i];
	}
	result = read_bytes(current, name, minimum, maximum, bytes, length);
done:
	for (size_t i = 0; i < lengthof(dirs); i++)
		read_close(dirs[i], &result);
	return result;
}

static ClusterControlRootResult
read_same_dir(int parent, const char *name, int fd)
{
	struct stat opened, current;

	if (fstat(fd, &opened) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (fstatat(parent, name, &current, AT_SYMLINK_NOFOLLOW) != 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_STALE_TOKEN : CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!read_owned(&current, true) || !read_owned(&opened, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return opened.st_dev == current.st_dev && opened.st_ino == current.st_ino
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
read_binding(const char *pgdata, PgracControlBinding *binding, uint8 bytes[256])
{
	PgracControlBindingResult result = pgrac_control_binding_read(pgdata, binding);

	if (result == PGRAC_CONTROL_BINDING_MISSING)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (result == PGRAC_CONTROL_BINDING_INVALID)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (result != PGRAC_CONTROL_BINDING_OK)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return pgrac_control_binding_encode(binding, bytes, 256)
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}

static void
read_hex(const uint8 hash[32], char hex[65])
{
	for (size_t i = 0; i < 32; i++)
		snprintf(hex + 2 * i, 3, "%02x", hash[i]);
}

static ClusterControlRootResult
read_selected(BootstrapReadWork *work, int global, int wal, uint32 node, uint8 *config,
			  size_t *config_len)
{
	const ControlRootImage *root = &work->before;
	const ClusterControlRootIdentity *self = &root->records[node].identity;
	ClusterControlRootResult result;
	char hex[65], name[128], thread[32], generation[48];
	const char *parts[3];
	size_t length;

	read_hex(root->header.v2.control_image_sha256, hex);
	snprintf(name, sizeof(name), UINT64_FORMAT "-%s.bin", root->header.v2.control_image_generation,
			 hex);
	parts[0] = "control_images";
	result = read_object(global, parts, 1, name, PG_CONTROL_FILE_SIZE, PG_CONTROL_FILE_SIZE,
						 work->common, &length);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	read_hex(root->header.v2.config_sha256, hex);
	snprintf(name, sizeof(name), UINT64_FORMAT "-%s.conf", root->header.v2.config_generation, hex);
	parts[0] = "config_images";
	result = read_object(global, parts, 1, name, 1, CLUSTER_SHARED_CONFIG_MAX_BYTES, config,
						 config_len);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	snprintf(thread, sizeof(thread), "thread_%u", self->origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 self->origin_owner_incarnation);
	parts[0] = thread;
	parts[1] = generation;
	result
		= read_object(wal, parts, 2, CLUSTER_WAL_THREAD_CLAIM_FILENAME, CLUSTER_WAL_CLAIM_V2_BYTES,
					  CLUSTER_WAL_CLAIM_V2_BYTES, work->claim, &length);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	parts[0] = "anchor_images";
	parts[1] = thread;
	parts[2] = generation;
	read_hex(root->refs[node].anchor_sha256, hex);
	snprintf(name, sizeof(name), "anchor_" UINT64_FORMAT "-%s.bin",
			 root->refs[node].anchor_generation, hex);
	return read_object(global, parts, 3, name, CLUSTER_RECOVERY_ANCHOR_SIZE,
					   CLUSTER_RECOVERY_ANCHOR_SIZE, work->anchor, &length);
}
#endif

ClusterControlRootResult
cluster_control_bootstrap_read(const char *pgdata, const char *shared_root, const char *wal_root,
							   uint32 node_id, ClusterControlBootstrapObservation *out)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_SYMLINK_NOFOLLOW)
	ClusterControlBootstrapInput input;
	ClusterControlBootstrapSnapshot snapshot;
	PgracControlBinding binding, later;
	uint8 binding_bytes[256], later_bytes[256];
	BootstrapReadWork *work;
	char *config;
	size_t length, config_len = 0;
	int shared = -1, global = -1, wal = -1;
	ClusterControlRootResult objects;
	bool valid = out != NULL && node_id < PGRAC_CONTROL_BINDING_MAX_NODES
				 && read_path_valid(pgdata, out) && read_path_valid(shared_root, out)
				 && read_path_valid(wal_root, out);

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!valid)
		return result;
	result = read_binding(pgdata, &binding, binding_bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (binding.node_id != node_id)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	/* Every fallible allocation precedes raw FDs. Root codecs are memory-only. */
	work = palloc(sizeof(*work));
	config = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
	result = read_dir(-1, shared_root, &shared);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = read_dir(shared, "global", &global);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = read_bytes(global, "pgrac_control_root", CLUSTER_CONTROL_ROOT_FILE_BYTES,
						CLUSTER_CONTROL_ROOT_FILE_BYTES, work->root_before, &length);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = cluster_control_root_v2_decode(work->root_before, length, binding.storage_uuid,
											binding.system_identifier, &work->before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = cluster_control_bootstrap_root_bound(&binding, &work->before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	objects = read_dir(-1, wal_root, &wal);
	if (objects == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		objects = read_selected(work, global, wal, node_id, (uint8 *)config, &config_len);
	/* Reobserve even after a missing object: a publisher/GC may have advanced. */
	result = read_bytes(global, "pgrac_control_root", CLUSTER_CONTROL_ROOT_FILE_BYTES,
						CLUSTER_CONTROL_ROOT_FILE_BYTES, work->root_after, &length);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = cluster_control_root_v2_decode(work->root_after, length, binding.storage_uuid,
											binding.system_identifier, &work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = cluster_control_bootstrap_root_bound(&binding, &work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	if (memcmp(work->root_before, work->root_after, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto close_dirs;
	}
	result = objects;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto close_dirs;
	result = read_same_dir(AT_FDCWD, shared_root, shared);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = read_same_dir(shared, "global", global);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = read_same_dir(AT_FDCWD, wal_root, wal);
close_dirs:
	read_close(wal, &result);
	read_close(global, &result);
	read_close(shared, &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = read_binding(pgdata, &later, later_bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (memcmp(binding_bytes, later_bytes, sizeof(binding_bytes)) != 0) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	memset(&input, 0, sizeof(input));
	input.node_id = node_id;
#define READ_INPUT(field, ptr, size)                                                               \
	input.field.data = ptr;                                                                        \
	input.field.len = size
	READ_INPUT(binding, binding_bytes, sizeof(binding_bytes));
	READ_INPUT(root_before, work->root_before, CLUSTER_CONTROL_ROOT_FILE_BYTES);
	READ_INPUT(root_after, work->root_after, CLUSTER_CONTROL_ROOT_FILE_BYTES);
	READ_INPUT(common, work->common, PG_CONTROL_FILE_SIZE);
	READ_INPUT(config, (const uint8 *)config, config_len);
	READ_INPUT(claim, work->claim, CLUSTER_WAL_CLAIM_V2_BYTES);
	READ_INPUT(anchor, work->anchor, CLUSTER_RECOVERY_ANCHOR_SIZE);
#undef READ_INPUT
	result = cluster_control_bootstrap_decode(&input, &snapshot);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		config[config_len] = '\0';
		out->snapshot = snapshot;
		out->config_bytes = config;
		out->config_len = config_len;
		config = NULL;
	}
done:
	if (config != NULL)
		pfree(config);
	pfree(work);
#else
	if (out != NULL)
		memset(out, 0, sizeof(*out));
#endif
	return result;
}
