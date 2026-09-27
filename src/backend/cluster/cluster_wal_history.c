/*-------------------------------------------------------------------------
 * PGRAC: retained writer history immutable file ownership.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "cluster_control_root_private.h"

#define HISTORY_STAGED 1
#define HISTORY_INSTALLED 2
#define HISTORY_DISCARDED 3

typedef struct HistoryDirs {
	int objects;
	int staging;
	struct stat objects_stat;
	struct stat staging_stat;
} HistoryDirs;

static bool
history_nonzero(const uint8 *bytes, size_t len)
{
	for (size_t i = 0; i < len; i++)
		if (bytes[i] != 0)
			return true;
	return false;
}

static bool
history_overlaps_stage(const void *input, size_t len, const ClusterWalHistoryStage *out)
{
	uintptr_t a = (uintptr_t)input, b = (uintptr_t)out;
	return input != NULL && (a <= b ? b - a < len : a - b < sizeof(*out));
}

static bool
history_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static void
history_close(int fd, ClusterControlRootResult *result)
{
	if (fd >= 0 && close(fd) != 0)
		*result = CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static void
history_close_dirs(HistoryDirs *dirs, ClusterControlRootResult *result)
{
	history_close(dirs->staging, result);
	history_close(dirs->objects, result);
	dirs->staging = dirs->objects = -1;
}

/* No allocation, mkdir, path traversal through symlinks or authority claim. */
static ClusterControlRootResult
history_open_dirs(uint32 node, bool staging, HistoryDirs *out)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	int fds[4] = { -1, -1, -1, -1 };
	char thread[32];
	const char *parts[] = { "global", "wal_history", thread };
	struct stat st;

	memset(out, 0, sizeof(*out));
	out->objects = out->staging = -1;
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0'
		|| node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	snprintf(thread, sizeof(thread), "thread_%u", node + 1);
	fds[0] = open(cluster_shared_data_dir, flags);
	if (fds[0] < 0 || fstat(fds[0], &st) != 0 || !history_owned(&st, true))
		goto done;
	for (unsigned i = 0; i < lengthof(parts); i++) {
		fds[i + 1] = openat(fds[i], parts[i], flags);
		if (fds[i + 1] < 0) {
			if (errno == ENOENT)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
			goto done;
		}
		if (fstat(fds[i + 1], &st) != 0 || !history_owned(&st, true))
			goto done;
	}
	out->objects = fds[3];
	fds[3] = -1;
	out->objects_stat = st;
	if (staging) {
		out->staging = openat(out->objects, ".staging", flags);
		if (out->staging < 0 || fstat(out->staging, &out->staging_stat) != 0
			|| !history_owned(&out->staging_stat, true))
			goto done;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	for (unsigned i = 0; i < lengthof(fds); i++)
		history_close(fds[i], &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		history_close_dirs(out, &result);
	return result;
}

static void
history_names(const ClusterWalHistoryStage *stage, char formal[112], char temp[40])
{
	char hex[65];
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", stage->sha256[i]);
	snprintf(formal, 112, "history_" UINT64_FORMAT "-%s.bin", stage->generation, hex);
	for (unsigned i = 0; i < 16; i++)
		snprintf(hex + i * 2, 3, "%02x", stage->operation_uuid[i]);
	snprintf(temp, 40, "%s.tmp", hex);
}

static bool
history_stage_valid(const ClusterWalHistoryStage *stage)
{
	return stage != NULL && stage->generation != 0 && stage->system_identifier != 0
		   && stage->current_owner_incarnation != 0
		   && stage->origin_node < CLUSTER_CONTROL_ROOT_RECORD_COUNT
		   && stage->length >= CLUSTER_WAL_HISTORY_HEADER_BYTES + 4
		   && stage->length <= CLUSTER_WAL_HISTORY_MAX_BYTES
		   && (stage->length - CLUSTER_WAL_HISTORY_HEADER_BYTES - 4)
					  % CLUSTER_CONTROL_ROOT_RECORD_BYTES
				  == 0
		   && stage->owner_pid == (uint32)getpid() && history_nonzero(stage->storage_uuid, 16)
		   && history_nonzero(stage->authority_uuid, 16) && history_nonzero(stage->sha256, 32)
		   && history_nonzero(stage->operation_uuid, 16) && stage->state >= HISTORY_STAGED
		   && stage->state <= HISTORY_DISCARDED;
}

static bool
history_dirs_match(const HistoryDirs *dirs, const ClusterWalHistoryStage *stage)
{
	return (uint64)dirs->objects_stat.st_dev == stage->object_dir_dev
		   && (uint64)dirs->objects_stat.st_ino == stage->object_dir_ino
		   && (uint64)dirs->staging_stat.st_dev == stage->staging_dir_dev
		   && (uint64)dirs->staging_stat.st_ino == stage->staging_dir_ino;
}

static bool
history_exact_entry(int dir, const char *name, const ClusterWalHistoryStage *stage)
{
	struct stat st;
	return fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW) == 0 && history_owned(&st, false)
		   && (uint64)st.st_dev == stage->file_dev && (uint64)st.st_ino == stage->file_ino;
}

static bool
history_hash(pg_cryptohash_ctx *ctx, const uint8 *bytes, size_t len, uint8 hash[32])
{
	return pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
		   && pg_cryptohash_final(ctx, hash, 32) >= 0;
}

static ClusterControlRootResult
history_read_at(int dir, const char *name, const ClusterWalHistoryStage *stage, bool check_inode,
				bool sync_file, uint8 *bytes, pg_cryptohash_ctx *ctx)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	struct stat st;
	uint8 hash[32], extra;
	size_t used = 0;
	ssize_t n;
	/* Native pg_fsync requires a writable regular-file descriptor. Only the
	 * installer reestablishes durability; evidence readers never write. */
	int fd
		= openat(dir, name,
				 (sync_file ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);

	if (fd < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fd, &st) != 0 || !history_owned(&st, false))
		goto done;
	if (check_inode
		&& ((uint64)st.st_dev != stage->file_dev || (uint64)st.st_ino != stage->file_ino)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (st.st_size != stage->length) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < stage->length) {
		n = read(fd, bytes + used, stage->length - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	do {
		n = read(fd, &extra, 1);
	} while (n < 0 && errno == EINTR);
	if (n != 0) {
		result = n < 0 ? CLUSTER_CONTROL_ROOT_IO_ERROR : CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	if (!history_hash(ctx, bytes, stage->length, hash))
		goto done;
	if (memcmp(hash, stage->sha256, 32) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	/* Equal bytes at an existing path do not prove an earlier operation synced
	 * them. Reestablish file durability before certifying its directory entry. */
	if (sync_file && pg_fsync(fd) != 0)
		goto done;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	history_close(fd, &result);
	return result;
}

ClusterControlRootResult
cluster_wal_history_prepare(const ControlRootImage *root, uint32 origin_node,
							const ClusterWalHistoryImage *history, uint64 generation,
							const uint8 operation_uuid[16], ClusterWalHistoryStage *out)
{
	ClusterControlRootResult result;
	ClusterWalHistoryStage stage;
	HistoryDirs dirs;
	uint8 *bytes;
	pg_cryptohash_ctx *ctx;
	char formal[112], temp[40];
	struct stat st;
	size_t len = 0, used = 0;
	int fd = -1;
	bool alias = history_overlaps_stage(root, sizeof(*root), out)
				 || history_overlaps_stage(history, sizeof(*history), out)
				 || history_overlaps_stage(operation_uuid, 16, out);

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || !enableFsync || generation == 0 || operation_uuid == NULL
		|| !history_nonzero(operation_uuid, 16))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	bytes = palloc(CLUSTER_WAL_HISTORY_MAX_BYTES);
	result = cluster_control_root_v2_history_encode(root, origin_node, history, bytes, &len);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		pfree(bytes);
		return result;
	}
	memset(&stage, 0, sizeof(stage));
	stage.generation = generation;
	stage.system_identifier = root->header.system_identifier;
	stage.current_owner_incarnation = root->records[origin_node].identity.origin_owner_incarnation;
	memcpy(stage.storage_uuid, root->header.storage_uuid, 16);
	memcpy(stage.authority_uuid, root->header.authority_uuid, 16);
	memcpy(stage.operation_uuid, operation_uuid, 16);
	stage.origin_node = origin_node;
	stage.length = len;
	stage.owner_pid = getpid();
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL || !history_hash(ctx, bytes, len, stage.sha256)) {
		pg_cryptohash_free(ctx);
		pfree(bytes);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	pg_cryptohash_free(ctx);
	history_names(&stage, formal, temp);
	result = history_open_dirs(origin_node, true, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		pfree(bytes);
		return result;
	}
	stage.object_dir_dev = dirs.objects_stat.st_dev;
	stage.object_dir_ino = dirs.objects_stat.st_ino;
	stage.staging_dir_dev = dirs.staging_stat.st_dev;
	stage.staging_dir_ino = dirs.staging_stat.st_ino;
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	fd = openat(dirs.staging, temp,
				O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY, 0600);
	if (fd < 0 || fstat(fd, &st) != 0 || !history_owned(&st, false))
		goto done;
	stage.file_dev = st.st_dev;
	stage.file_ino = st.st_ino;
	stage.state = HISTORY_STAGED;
	while (used < len) {
		ssize_t n = write(fd, bytes + used, len - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	if (pg_fsync(fd) != 0 || pg_fsync(dirs.staging) != 0)
		goto done;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	history_close(fd, &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && stage.state == HISTORY_STAGED
		&& history_exact_entry(dirs.staging, temp, &stage)) {
		if (unlinkat(dirs.staging, temp, 0) == 0)
			(void)pg_fsync(dirs.staging);
	}
	history_close_dirs(&dirs, &result);
	pfree(bytes);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = stage;
	return result;
}

ClusterControlRootResult
cluster_wal_history_install(ClusterWalHistoryStage *stage)
{
	ClusterControlRootResult result;
	HistoryDirs dirs;
	pg_cryptohash_ctx *ctx;
	uint8 *staged, *installed;
	char formal[112], temp[40];

	if (!enableFsync || !history_stage_valid(stage) || stage->state == HISTORY_DISCARDED)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	staged = palloc(2 * stage->length);
	installed = staged + stage->length;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL) {
		pfree(staged);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	history_names(stage, formal, temp);
	result = history_open_dirs(stage->origin_node, true, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (!history_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == HISTORY_STAGED) {
		result = history_read_at(dirs.staging, temp, stage, true, false, staged, ctx);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		if (linkat(dirs.staging, temp, dirs.objects, formal, 0) != 0 && errno != EEXIST) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	result = history_read_at(dirs.objects, formal, stage, false, true, installed, ctx);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (stage->state == HISTORY_STAGED && memcmp(staged, installed, stage->length) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	if (pg_fsync(dirs.objects) != 0) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	if (stage->state == HISTORY_STAGED) {
		if (!history_exact_entry(dirs.staging, temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.staging, temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		stage->state = HISTORY_INSTALLED;
	}
	if (pg_fsync(dirs.staging) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	history_close_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	pfree(staged);
	return result;
}

ClusterControlRootResult
cluster_wal_history_discard(ClusterWalHistoryStage *stage)
{
	ClusterControlRootResult result;
	HistoryDirs dirs;
	char formal[112], temp[40];

	if (!enableFsync || !history_stage_valid(stage))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	history_names(stage, formal, temp);
	result = history_open_dirs(stage->origin_node, true, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!history_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == HISTORY_STAGED) {
		if (!history_exact_entry(dirs.staging, temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.staging, temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	stage->state = HISTORY_DISCARDED;
	if (pg_fsync(dirs.staging) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	history_close_dirs(&dirs, &result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(stage, 0, sizeof(*stage));
	return result;
}

/* PGRAC: only the selected immutable object is consumed. This is evidence,
 * not publication, recovery completion, old-writer isolation or WAL reuse.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_wal_history_read_locked(const ControlRootImage *root, uint32 node,
								ClusterWalHistoryImage *out)
{
	ClusterControlRootResult result;
	ClusterWalHistoryStage selected = { 0 };
	HistoryDirs dirs, current;
	pg_cryptohash_ctx *ctx;
	struct stat st;
	uint8 *bytes;
	char formal[112], unused[40];
	uintptr_t a = (uintptr_t)root, b = (uintptr_t)out;
	bool alias
		= root != NULL && out != NULL && (a <= b ? b - a < sizeof(*root) : a - b < sizeof(*out));

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || root == NULL || node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (root->header.format_version != 2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node] || root->refs[node].history_generation == 0)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (!history_nonzero(root->refs[node].history_sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	selected.generation = root->refs[node].history_generation;
	memcpy(selected.sha256, root->refs[node].history_sha256, 32);
	history_names(&selected, formal, unused);
	bytes = palloc(CLUSTER_WAL_HISTORY_MAX_BYTES);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL) {
		pfree(bytes);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	result = history_open_dirs(node, false, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (fstatat(dirs.objects, formal, &st, AT_SYMLINK_NOFOLLOW) != 0) {
		result = errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	if (!history_owned(&st, false)) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	if (st.st_size < CLUSTER_WAL_HISTORY_HEADER_BYTES + 4
		|| st.st_size > CLUSTER_WAL_HISTORY_MAX_BYTES) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	selected.length = st.st_size;
	selected.file_dev = st.st_dev;
	selected.file_ino = st.st_ino;
	result = history_read_at(dirs.objects, formal, &selected, true, false, bytes, ctx);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = history_open_dirs(node, false, &current);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (dirs.objects_stat.st_dev != current.objects_stat.st_dev
			|| dirs.objects_stat.st_ino != current.objects_stat.st_ino
			|| !history_exact_entry(current.objects, formal, &selected)))
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	history_close_dirs(&current, &result);
done:
	history_close_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	/* No open descriptor survives a decoder allocation/error. */
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_control_root_v2_history_decode(bytes, selected.length, root, node, out);
	pfree(bytes);
	return result;
}
