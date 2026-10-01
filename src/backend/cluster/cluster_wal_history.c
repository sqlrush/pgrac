/*-------------------------------------------------------------------------
 * PGRAC: retained history and pending writer startup immutable file ownership.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "cluster_control_bootstrap_private.h"
#include "cluster_control_root_private.h"

#define HISTORY_STAGED 1
#define HISTORY_INSTALLED 2
#define HISTORY_DISCARDED 3

/* Two fixed backend-private families share the same no-clobber/owned-file
 * discipline. The separate codecs and public entrypoints decide semantics. */
typedef enum WalObjectKind {
	WAL_OBJECT_HISTORY,
	WAL_OBJECT_STARTUP,
	WAL_OBJECT_TERMINAL
} WalObjectKind;

typedef struct HistoryDirs {
	int objects;
	int staging;
	struct stat objects_stat;
	struct stat staging_stat;
} HistoryDirs;

static bool history_hash(pg_cryptohash_ctx *ctx, const uint8 *bytes, size_t len, uint8 hash[32]);

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
history_open_dirs(uint32 node, bool staging, WalObjectKind kind, HistoryDirs *out)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	int fds[4] = { -1, -1, -1, -1 };
	char thread[32];
	const char *parts[]
		= { "global", kind == WAL_OBJECT_HISTORY ? "wal_history" : "wal_startup", thread };
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

/* PGRAC: RESERVED creates metadata only. No guessed thread parent, overwrite,
 * WAL segment or native mutation. Selected complete metadata can be fsynced
 * and reobserved after an uncertain result; malformed partial files remain
 * refused for the operation's recovery/retirement owner, never repaired here.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
startup_empty_entries(int fd, const char *a, const char *b, const char *c)
{
	int copy = dup(fd);
	DIR *dir;
	struct dirent *entry;
	bool valid = true;
	if (copy < 0)
		return false;
	dir = fdopendir(copy);
	if (dir == NULL) {
		close(copy);
		return false;
	}
	rewinddir(dir);
	errno = 0;
	while ((entry = readdir(dir)) != NULL) {
		const char *name = entry->d_name;
		if (strcmp(name, ".") != 0 && strcmp(name, "..") != 0 && (a == NULL || strcmp(name, a) != 0)
			&& (b == NULL || strcmp(name, b) != 0) && (c == NULL || strcmp(name, c) != 0)) {
			valid = false;
			break;
		}
	}
	if (errno != 0)
		valid = false;
	return closedir(dir) == 0 && valid;
}

static int
startup_empty_dir(int parent, const char *name, bool create, bool *created)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	struct stat before, after;
	int fd;
	*created = false;
	if (create) {
		if (mkdirat(parent, name, 0700) == 0)
			*created = true;
		else if (errno != EEXIST)
			return -1;
	}
	if (fstatat(parent, name, &before, AT_SYMLINK_NOFOLLOW) != 0 || !history_owned(&before, true))
		return -1;
	fd = openat(parent, name, flags);
	if (fd >= 0
		&& (fstat(fd, &after) != 0 || after.st_dev != before.st_dev || after.st_ino != before.st_ino
			|| !history_owned(&after, true))) {
		close(fd);
		return -1;
	}
	return fd;
}

static ClusterControlRootResult
startup_empty_file(int dir, const char *name, const uint8 *expected, Size length, bool create,
				   bool sync)
{
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES + 1];
	struct stat st, named;
	int flags = O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY;
	int fd = -1;
	Size used = 0;
	bool fresh = false;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (create) {
		fd = openat(dir, name, flags | O_RDWR | O_CREAT | O_EXCL, 0600);
		fresh = fd >= 0;
		if (fd < 0 && errno != EEXIST)
			return result;
	}
	if (fd < 0)
		fd = openat(dir, name, flags | (sync ? O_RDWR : O_RDONLY));
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : result;
	if (fstat(fd, &st) != 0 || !history_owned(&st, false) || st.st_nlink != 1)
		goto done;
	if (fresh) {
		while (used < length) {
			ssize_t n = write(fd, expected + used, length - used);
			if (n < 0 && errno == EINTR)
				continue;
			if (n <= 0)
				goto done;
			used += n;
		}
	}
	if (lseek(fd, 0, SEEK_SET) != 0)
		goto done;
	used = 0;
	while (used < length + 1) {
		ssize_t n = read(fd, bytes + used, length + 1 - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	if (used != length || memcmp(bytes, expected, length) != 0) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	if (sync && pg_fsync(fd) != 0)
		goto done;
	if (fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) != 0 || !history_owned(&named, false)
		|| named.st_nlink != 1 || st.st_dev != named.st_dev || st.st_ino != named.st_ino) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	history_close(fd, &result);
	return result;
}

ClusterControlRootResult
cluster_wal_startup_empty_locked(const ControlRootImage *root, uint32 node, bool create, bool sync)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterWalStartupImage op;
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];
	char thread[32], generation[48];
	/* WAL root/thread/generation/archive; shared/global/anchors/thread/generation/stage. */
	int fds[10];
	int parents[] = { -1, 0, 1, 2, -1, 4, 5, 6, 7, 8 };
	const char *names[] = { cluster_wal_threads_dir,
							thread,
							generation,
							"archive_status",
							cluster_shared_data_dir,
							"global",
							"anchor_images",
							thread,
							generation,
							".staging" };
	struct stat opened[10], named;
	bool created;
	ClusterControlRootResult result;

	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if ((create && !sync) || (sync && !enableFsync))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_wal_startup_read_locked(root, node, &op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if ((op.phase != CLUSTER_WAL_STARTUP_RESERVED
		 && !(op.phase == CLUSTER_WAL_STARTUP_INITIALIZING && !create && !sync))
		|| cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	result = cluster_wal_claim_v2_encode(&op.claim, claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	snprintf(thread, sizeof(thread), "thread_%u", node + 1);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 op.claim.identity.origin_owner_incarnation);
	for (unsigned i = 0; i < lengthof(fds); ++i)
		fds[i] = -1;
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (unsigned i = 0; i < lengthof(fds); ++i) {
		created = false;
		if (parents[i] < 0)
			fds[i] = open(names[i], flags);
		else
			fds[i] = startup_empty_dir(fds[parents[i]], names[i],
									   create && (i == 2 || i == 3 || i == 8 || i == 9),
									   &created);
		if (fds[i] < 0) {
			result = errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_IO_ERROR;
			/* PGRAC: only a not-yet-created target generation is waitable
			 * at the coordinator's RESERVED durability barrier. Missing
			 * selected input, required parents or a partial target is not. */
			if (result == CLUSTER_CONTROL_ROOT_ABSENT && i == 2 && !create && sync
				&& op.phase == CLUSTER_WAL_STARTUP_RESERVED)
				result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
			goto done;
		}
		if (fstat(fds[i], &opened[i]) != 0 || !history_owned(&opened[i], true)) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		if (i == 2) {
			/* An existing unclaimed directory is not ours to initialize. */
			result = startup_empty_file(fds[2], CLUSTER_WAL_THREAD_CLAIM_FILENAME, claim,
										sizeof(claim), create && created, sync);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				goto done;
			if (!startup_empty_entries(fds[2], CLUSTER_WAL_THREAD_CLAIM_FILENAME,
									   "archive_status", NULL)) {
				result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
				goto done;
			}
		}
		if ((i == 3 || i == 9) && !startup_empty_entries(fds[i], NULL, NULL, NULL)) {
			result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			goto done;
		}
		if (i == 8 && !startup_empty_entries(fds[i], ".staging", NULL, NULL)) {
			result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			goto done;
		}
	}
	/* Persist descendants before parents, then ensure the named tree is still
	 * exactly the tree just inspected. No allocations/ERROR while FDs are open. */
	for (int i = lengthof(fds) - 1; i >= 0; --i) {
		if (sync && pg_fsync(fds[i]) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		if ((parents[i] < 0 ? lstat(names[i], &named)
							: fstatat(fds[parents[i]], names[i], &named, AT_SYMLINK_NOFOLLOW))
				!= 0
			|| !history_owned(&named, true) || named.st_dev != opened[i].st_dev
			|| named.st_ino != opened[i].st_ino) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	for (unsigned i = 0; i < lengthof(fds); ++i)
		history_close(fds[i], &result);
	return result;
}

/* PGRAC: only an owned symlink may be exchanged. Directory identities select
 * the old/new case; a failed claim check never falls back to another route.
 * No fallible allocation/ERROR call is made while raw descriptors are open.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
startup_route_path(const char *path)
{
	size_t len = path != NULL ? strnlen(path, MAXPGPATH) : 0;
	size_t start = 1;
	if (len < 2 || len >= MAXPGPATH || path[0] != '/')
		return false;
	for (size_t i = 1; i <= len; ++i) {
		if (i == len || path[i] == '/') {
			size_t part = i - start;
			if (part == 0 || (part == 1 && path[start] == '.')
				|| (part == 2 && path[start] == '.' && path[start + 1] == '.'))
				return false;
			start = i + 1;
		}
	}
	return true;
}

static bool
startup_route_same(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino;
}

static bool
startup_route_link(int dir, const char *name, struct stat *st)
{
	return fstatat(dir, name, st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISLNK(st->st_mode)
		   && st->st_uid == geteuid() && st->st_nlink == 1;
}

ClusterControlRootResult
cluster_wal_startup_route_locked(const ControlRootImage *root, uint32 node, const char *pgdata,
								 const ClusterWalSourceRef *restart)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterWalStartupImage op;
	ClusterWalSourceRef next = { 0 };
	ClusterWalThreadClaimV2 old_claim;
	ClusterWalTailObservation old_tail, fresh_tail;
	ClusterControlRootResult result;
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES], random[16];
	pg_cryptohash_ctx *ctx;
	char thread[32], oldgen[48], newgen[48], target[MAXPGPATH], temp[64];
	int fds[] = { -1, -1, -1, -1, -1 };
	int parents[] = { -1, -1, 1, 2, 2 };
	const char *names[] = { pgdata, cluster_wal_threads_dir, thread, oldgen, newgen };
	struct stat opened[5], named, leaf, temporary, routed;
	bool created = false, renamed = false, already_new, hashed;

	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!enableFsync || restart == NULL || !startup_route_path(pgdata)
		|| !startup_route_path(cluster_wal_threads_dir))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_wal_startup_read_locked(root, node, &op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (op.phase != CLUSTER_WAL_STARTUP_INITIALIZING
		|| !cluster_control_root_identity_equal(&restart->claim.identity,
												&op.predecessor.snapshot.identity)
		|| restart->claim.database_incarnation != op.database_incarnation
		|| restart->claim.max_config_generation != op.config_generation
		|| restart->timeline != op.predecessor.snapshot.checkpoint_tli
		|| memcmp(restart->claim.claim_sha256, op.predecessor.refs.claim_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &restart->claim, &old_claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_tail_observe_checkpoint(cluster_wal_threads_dir, restart, op.segment_size,
												 op.predecessor.snapshot.checkpoint_lower_lsn,
												 op.sealed_input_end, op.input_record_start,
												 op.input_record_crc, &old_tail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (old_tail.complete_end != op.sealed_input_end
		|| old_tail.last_record_start != op.predecessor.snapshot.tail_last_record_lsn
		|| old_tail.last_record_crc != op.predecessor.snapshot.tail_last_record_crc32c)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = cluster_wal_startup_empty_locked(root, node, false, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_claim_v2_encode(&op.claim, claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hashed = history_hash(ctx, claim, sizeof(claim), next.claim.claim_sha256);
	pg_cryptohash_free(ctx);
	if (!hashed || !pg_strong_random(random, sizeof(random)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	next.claim.identity = op.claim.identity;
	next.claim.database_incarnation = op.database_incarnation;
	next.claim.max_config_generation = op.config_generation;
	next.timeline = op.timeline;
	snprintf(thread, sizeof(thread), "thread_%u", node + 1);
	snprintf(oldgen, sizeof(oldgen), "generation_" UINT64_FORMAT,
			 restart->claim.identity.origin_owner_incarnation);
	snprintf(newgen, sizeof(newgen), "generation_" UINT64_FORMAT,
			 op.claim.identity.origin_owner_incarnation);
	if (snprintf(target, sizeof(target), "%s/%s/%s", cluster_wal_threads_dir, thread, newgen)
		>= sizeof(target))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	strlcpy(temp, ".pgrac-wal-route-", sizeof(temp));
	for (unsigned i = 0; i < sizeof(random); ++i)
		snprintf(temp + 17 + i * 2, 3, "%02x", random[i]);
	/* Everything below, until close, is bounded raw file I/O only. */
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (unsigned i = 0; i < lengthof(fds); ++i) {
		fds[i] = parents[i] < 0 ? open(names[i], flags) : openat(fds[parents[i]], names[i], flags);
		if (fds[i] < 0 || fstat(fds[i], &opened[i]) != 0 || !history_owned(&opened[i], true))
			goto done;
	}
	if (!startup_route_link(fds[0], "pg_wal", &leaf) || fstatat(fds[0], "pg_wal", &routed, 0) != 0
		|| !history_owned(&routed, true))
		goto done;
	already_new = startup_route_same(&routed, &opened[4]);
	if (!already_new && !startup_route_same(&routed, &opened[3])) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	if (!already_new) {
		if (symlinkat(target, fds[0], temp) != 0)
			goto done;
		/* Do not unlink an unobserved/replaced temporary name on failure. */
		if (!startup_route_link(fds[0], temp, &temporary))
			goto done;
		created = true;
	}
	/* Authenticate all pinned directories both before and after the exchange. */
	for (unsigned pass = 0; pass < 2; ++pass) {
		for (unsigned i = 0; i < lengthof(fds); ++i) {
			if ((parents[i] < 0 ? lstat(names[i], &named)
								: fstatat(fds[parents[i]], names[i], &named, AT_SYMLINK_NOFOLLOW))
					!= 0
				|| !history_owned(&named, true) || !startup_route_same(&named, &opened[i])) {
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
				goto done;
			}
		}
		if (!startup_route_link(fds[0], "pg_wal", &named)
			|| !startup_route_same(&named, renamed ? &temporary : &leaf)
			|| fstatat(fds[0], "pg_wal", &routed, 0) != 0
			|| !startup_route_same(&routed, (already_new || renamed) ? &opened[4] : &opened[3])) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (pass == 0) {
			if (!already_new) {
				if (!startup_route_link(fds[0], temp, &named)
					|| !startup_route_same(&named, &temporary)
					|| renameat(fds[0], temp, fds[0], "pg_wal") != 0)
					goto done;
				renamed = true;
			}
			/* Even an already visible successor needs this confirmation. */
			if (pg_fsync(fds[0]) != 0)
				goto done;
		}
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	if (created && !renamed && startup_route_link(fds[0], temp, &named)
		&& startup_route_same(&named, &temporary) && unlinkat(fds[0], temp, 0) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (unsigned i = 0; i < lengthof(fds); ++i)
		history_close(fds[i], &result);
	/* The old stream remains an input; neither success nor refusal erases it. */
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &restart->claim, &old_claim);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_tail_observe_checkpoint(
			cluster_wal_threads_dir, restart, op.segment_size,
			op.predecessor.snapshot.checkpoint_lower_lsn, op.sealed_input_end,
			op.input_record_start, op.input_record_crc, &fresh_tail);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& memcmp(&fresh_tail, &old_tail, sizeof(old_tail)) != 0)
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_control_bootstrap_wal_route(pgdata, cluster_wal_threads_dir, &next);
	return result;
}

static void
history_names(const ClusterWalHistoryStage *stage, WalObjectKind kind, char formal[112],
			  char temp[40])
{
	char hex[65];
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", stage->sha256[i]);
	snprintf(formal, 112, "%s_" UINT64_FORMAT "-%s.bin",
			 kind == WAL_OBJECT_HISTORY ? "history" : "startup", stage->generation, hex);
	for (unsigned i = 0; i < 16; i++)
		snprintf(hex + i * 2, 3, "%02x", stage->operation_uuid[i]);
	snprintf(temp, 40, "%s.tmp", hex);
}

static bool
history_length_valid(uint32 length)
{
	if (length >= CLUSTER_WAL_HISTORY_HEADER_BYTES + 4
		&& length <= CLUSTER_WAL_HISTORY_HEADER_BYTES + CLUSTER_WAL_HISTORY_MAX_RECORDS * 512 + 4
		&& (length - CLUSTER_WAL_HISTORY_HEADER_BYTES - 4) % 512 == 0)
		return true;
	for (uint32 terminals = 1; terminals <= CLUSTER_WAL_HISTORY_MAX_RECORDS; ++terminals) {
		uint32 base
			= CLUSTER_WAL_HISTORY_V2_HEADER_BYTES + terminals * CLUSTER_WAL_TERMINAL_REF_BYTES + 4;
		if (length >= base && (length - base) % 512 == 0
			&& (length - base) / 512 <= CLUSTER_WAL_HISTORY_MAX_RECORDS - terminals)
			return true;
	}
	return false;
}

static bool
history_stage_valid(const ClusterWalHistoryStage *stage, WalObjectKind kind)
{
	return stage != NULL && stage->generation != 0 && stage->system_identifier != 0
		   && stage->current_owner_incarnation != 0
		   && stage->origin_node < CLUSTER_CONTROL_ROOT_RECORD_COUNT
		   && (kind == WAL_OBJECT_HISTORY
				   ? history_length_valid(stage->length)
				   : stage->length
						 == (kind == WAL_OBJECT_STARTUP ? CLUSTER_WAL_STARTUP_BYTES
														: CLUSTER_WAL_TERMINAL_BYTES))
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

/* Inputs were accepted by the family-specific codec before entering here. */
static ClusterControlRootResult
history_prepare_encoded(const ControlRootImage *root, uint32 origin_node, const uint8 *bytes,
						size_t len, uint64 generation, const uint8 operation_uuid[16],
						WalObjectKind kind, ClusterWalHistoryStage *out)
{
	ClusterControlRootResult result;
	ClusterWalHistoryStage stage;
	HistoryDirs dirs;
	pg_cryptohash_ctx *ctx;
	char formal[112], temp[40];
	struct stat st;
	size_t used = 0;
	int fd = -1;
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
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	pg_cryptohash_free(ctx);
	history_names(&stage, kind, formal, temp);
	result = history_open_dirs(origin_node, true, kind, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
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
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = stage;
	return result;
}

ClusterControlRootResult
cluster_wal_history_prepare(const ControlRootImage *root, uint32 origin_node,
							const ClusterWalHistoryImage *history, uint64 generation,
							const uint8 operation_uuid[16], ClusterWalHistoryStage *out)
{
	ClusterControlRootResult result;
	uint8 *bytes;
	size_t len = 0;
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
	/* Flat history keeps record-v2 encoding. Select its root contract explicitly;
	 * this never converts a root or grants permission to ignore pending startup. */
	result = root != NULL && root->header.format_version == 3
				 ? cluster_control_root_v3_history_encode(root, origin_node, history, bytes, &len)
				 : cluster_control_root_v2_history_encode(root, origin_node, history, bytes, &len);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = history_prepare_encoded(root, origin_node, bytes, len, generation, operation_uuid,
										 WAL_OBJECT_HISTORY, out);
	pfree(bytes);
	return result;
}

ClusterControlRootResult
cluster_wal_terminal_prepare(const ControlRootImage *root, uint32 node,
							 const ClusterWalHistoryImage *history,
							 const ClusterWalTerminalImage *terminal, ClusterWalStartupStage *out)
{
	ClusterControlRootResult result;
	ClusterWalTerminalRef ref;
	uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES];
	bool alias = history_overlaps_stage(root, sizeof(*root), out)
				 || history_overlaps_stage(history, sizeof(*history), out)
				 || history_overlaps_stage(terminal, sizeof(*terminal), out);
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_wal_terminal_encode(root, node, history, terminal, bytes, &ref);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = history_prepare_encoded(root, node, bytes, sizeof(bytes), ref.generation,
										 terminal->operation_uuid, WAL_OBJECT_TERMINAL, out);
	return result;
}

ClusterControlRootResult
cluster_wal_startup_prepare(const ControlRootImage *root, uint32 origin_node,
							const ClusterWalStartupImage *startup, ClusterWalStartupStage *out)
{
	ClusterControlRootResult result;
	ControlRootStartupRefV3 ref;
	uint8 bytes[CLUSTER_WAL_STARTUP_BYTES];
	bool alias = history_overlaps_stage(root, sizeof(*root), out)
				 || history_overlaps_stage(startup, sizeof(*startup), out);

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || !enableFsync)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_v3_startup_encode(root, origin_node, startup, bytes, &ref);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = history_prepare_encoded(root, origin_node, bytes, sizeof(bytes), ref.generation,
										 startup->operation_uuid, WAL_OBJECT_STARTUP, out);
	return result;
}

static ClusterControlRootResult
history_install(ClusterWalHistoryStage *stage, WalObjectKind kind)
{
	ClusterControlRootResult result;
	HistoryDirs dirs;
	pg_cryptohash_ctx *ctx;
	uint8 *staged, *installed;
	char formal[112], temp[40];

	if (!enableFsync || !history_stage_valid(stage, kind) || stage->state == HISTORY_DISCARDED)
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
	history_names(stage, kind, formal, temp);
	result = history_open_dirs(stage->origin_node, true, kind, &dirs);
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

static ClusterControlRootResult
history_discard(ClusterWalHistoryStage *stage, WalObjectKind kind)
{
	ClusterControlRootResult result;
	HistoryDirs dirs;
	char formal[112], temp[40];

	if (!enableFsync || !history_stage_valid(stage, kind))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	history_names(stage, kind, formal, temp);
	result = history_open_dirs(stage->origin_node, true, kind, &dirs);
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

ClusterControlRootResult
cluster_wal_history_install(ClusterWalHistoryStage *stage)
{
	return history_install(stage, WAL_OBJECT_HISTORY);
}

ClusterControlRootResult
cluster_wal_history_discard(ClusterWalHistoryStage *stage)
{
	return history_discard(stage, WAL_OBJECT_HISTORY);
}

ClusterControlRootResult
cluster_wal_startup_install(ClusterWalStartupStage *stage)
{
	return history_install(stage, WAL_OBJECT_STARTUP);
}

ClusterControlRootResult
cluster_wal_startup_discard(ClusterWalStartupStage *stage)
{
	return history_discard(stage, WAL_OBJECT_STARTUP);
}

ClusterControlRootResult
cluster_wal_terminal_install(ClusterWalStartupStage *stage)
{
	return history_install(stage, WAL_OBJECT_TERMINAL);
}

ClusterControlRootResult
cluster_wal_terminal_discard(ClusterWalStartupStage *stage)
{
	return history_discard(stage, WAL_OBJECT_TERMINAL);
}

/* PGRAC: only the selected immutable object is consumed. This is evidence,
 * not publication, recovery completion, old-writer isolation or WAL reuse.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
history_read_selected(uint32 node, WalObjectKind kind, ClusterWalHistoryStage *selected,
					  uint8 *bytes)
{
	ClusterControlRootResult result;
	HistoryDirs dirs, current;
	pg_cryptohash_ctx *ctx;
	struct stat st;
	char formal[112], unused[40];

	history_names(selected, kind, formal, unused);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = history_open_dirs(node, false, kind, &dirs);
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
	if (kind != WAL_OBJECT_HISTORY
			? st.st_size
				  != (kind == WAL_OBJECT_STARTUP ? CLUSTER_WAL_STARTUP_BYTES
												 : CLUSTER_WAL_TERMINAL_BYTES)
			: (st.st_size < CLUSTER_WAL_HISTORY_HEADER_BYTES + 4
			   || st.st_size > CLUSTER_WAL_HISTORY_MAX_BYTES)) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	selected->length = st.st_size;
	selected->file_dev = st.st_dev;
	selected->file_ino = st.st_ino;
	result = history_read_at(dirs.objects, formal, selected, true, false, bytes, ctx);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = history_open_dirs(node, false, kind, &current);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (dirs.objects_stat.st_dev != current.objects_stat.st_dev
			|| dirs.objects_stat.st_ino != current.objects_stat.st_ino
			|| !history_exact_entry(current.objects, formal, selected)))
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	history_close_dirs(&current, &result);
done:
	history_close_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	/* No open descriptor survives a decoder allocation/error. */
	return result;
}

ClusterControlRootResult
cluster_wal_history_read_locked(const ControlRootImage *root, uint32 node,
								ClusterWalHistoryImage *out)
{
	ClusterControlRootResult result;
	ClusterWalHistoryStage selected = { 0 };
	uint8 *bytes;
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
	if (root->header.format_version != 2 && root->header.format_version != 3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node] || root->refs[node].history_generation == 0)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (!history_nonzero(root->refs[node].history_sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	selected.generation = root->refs[node].history_generation;
	memcpy(selected.sha256, root->refs[node].history_sha256, 32);
	bytes = palloc(CLUSTER_WAL_HISTORY_MAX_BYTES);
	result = history_read_selected(node, WAL_OBJECT_HISTORY, &selected, bytes);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result
			= root->header.format_version == 3
				  ? cluster_control_root_v3_history_decode(bytes, selected.length, root, node, out)
				  : cluster_control_root_v2_history_decode(bytes, selected.length, root, node, out);
	pfree(bytes);
	return result;
}

ClusterControlRootResult
cluster_wal_startup_read_locked(const ControlRootImage *root, uint32 node,
								ClusterWalStartupImage *out)
{
	ClusterControlRootResult result;
	ClusterWalStartupStage selected = { 0 };
	uint8 bytes[CLUSTER_WAL_STARTUP_BYTES];
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
	if (root->header.format_version != 3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->startup[node].generation == 0 && !history_nonzero(root->startup[node].sha256, 32))
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->startup[node].generation == 0 || !history_nonzero(root->startup[node].sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	selected.generation = root->startup[node].generation;
	memcpy(selected.sha256, root->startup[node].sha256, 32);
	result = history_read_selected(node, WAL_OBJECT_STARTUP, &selected, bytes);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), root, node, out);
	return result;
}

ClusterControlRootResult
cluster_wal_terminal_read_locked(const ControlRootImage *root, uint32 node, uint32 terminal_index,
								 ClusterWalTerminalImage *out)
{
	ClusterControlRootResult result;
	ClusterWalStartupStage selected = { 0 };
	ClusterWalHistoryImage *history;
	uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES];
	uintptr_t a = (uintptr_t)root, b = (uintptr_t)out;
	bool alias
		= root != NULL && out != NULL && (a <= b ? b - a < sizeof(*root) : a - b < sizeof(*out));
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || root == NULL || node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	history = palloc0(sizeof(*history));
	result = cluster_wal_history_read_locked(root, node, history);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (terminal_index >= history->terminal_count)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		else {
			const ClusterWalTerminalRef *ref = &history->terminals[terminal_index];
			selected.generation = ref->generation;
			memcpy(selected.sha256, ref->sha256, 32);
			result = history_read_selected(node, WAL_OBJECT_TERMINAL, &selected, bytes);
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				result = cluster_wal_terminal_decode(bytes, sizeof(bytes), root, node, history, ref,
													 out);
		}
	}
	pfree(history);
	return result;
}

ClusterControlRootResult
cluster_wal_origin_inputs_read_locked(const ControlRootImage *root, uint32 node,
									  ClusterWalOriginInputs *out)
{
	ClusterWalOriginInputs *inputs;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	if (root->header.format_version != 3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->records[node].identity.origin_node_id != node
		|| root->records[node].identity.origin_thread_id != node + 1)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if ((root->refs[node].history_generation == 0)
			!= !history_nonzero(root->refs[node].history_sha256, 32)
		|| (root->startup[node].generation == 0)
			   != !history_nonzero(root->startup[node].sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	/* Nothing partially collected escapes, including when a decoder throws.
	 * A missing selected object is an error, never an empty retained set. */
	inputs = palloc0(sizeof(*inputs));
	inputs->current.snapshot = root->records[node];
	inputs->current.refs = root->refs[node];
	inputs->current.publisher_incarnation = root->publisher_incarnation[node];
	inputs->current.publisher_node = root->publisher_node[node];
	inputs->current.record_crc32c = root->record_crc32c[node];
	if (root->refs[node].history_generation != 0)
		result = cluster_wal_history_read_locked(root, node, &inputs->history);
	for (uint32 i = 0;
		 result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && i < inputs->history.terminal_count; i++) {
		ClusterWalTerminalImage terminal;
		result = cluster_wal_terminal_read_locked(root, node, i, &terminal);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && root->startup[node].generation != 0) {
		result = cluster_wal_startup_read_locked(root, node, &inputs->pending);
		inputs->has_pending = result == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = *inputs;
	pfree(inputs);
	return result;
}
