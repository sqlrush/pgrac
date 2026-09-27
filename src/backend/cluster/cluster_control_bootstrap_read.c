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
#include "cluster/cluster_wal_tail.h"
#include "common/cryptohash.h"
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
	uint8 history[CLUSTER_WAL_HISTORY_MAX_BYTES + 1];
	uint8 startup[CLUSTER_WAL_STARTUP_BYTES + 1];
	ClusterWalHistoryImage retained;
	ClusterWalStartupImage pending;
	struct stat shared_dir;
	struct stat global_dir;
	struct stat wal_dir;
	size_t history_len;
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

static ClusterControlRootResult read_same_dir(int parent, const char *name, int fd);
#endif

ClusterControlRootResult
cluster_control_bootstrap_wal_route(const char *pgdata, const char *wal_root,
									const ClusterWalDurablePrefixRef *ref)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_SYMLINK_NOFOLLOW)
	int dirs[5] = { -1, -1, -1, -1, -1 };
	char thread[32], generation[48];
	struct stat expected, routed, current;
	ClusterWalDurablePrefix prefix;
	ClusterWalDurablePrefix empty = { .sequence = 1 };
	ClusterWalThreadClaimV2 claim;
	uint8 check[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	uint8 claim_bytes[CLUSTER_WAL_CLAIM_V2_BYTES + 1];
	size_t claim_len = 0;

	if (!read_path_valid(pgdata, NULL) || !read_path_valid(wal_root, NULL)
		|| cluster_wal_durable_prefix_encode(ref, &empty, check) != 0)
		return result;
	snprintf(thread, sizeof(thread), "thread_%u", ref->claim.identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 ref->claim.identity.origin_owner_incarnation);
	result = read_dir(-1, pgdata, &dirs[0]);
	if (result != 0)
		goto done;
	result = read_dir(-1, wal_root, &dirs[1]);
	if (result != 0)
		goto done;
	result = read_dir(dirs[1], thread, &dirs[2]);
	if (result != 0)
		goto done;
	result = read_dir(dirs[2], generation, &dirs[3]);
	if (result != 0)
		goto done;
	/* Only the native pg_wal leaf may follow a symlink. Its resolved inode,
	 * not the spelling of the symlink, must match the root-selected directory. */
	dirs[4] = openat(dirs[0], "pg_wal", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (dirs[4] < 0 || fstat(dirs[3], &expected) != 0 || fstat(dirs[4], &routed) != 0) {
		result = read_error();
		goto done;
	}
	if (!read_owned(&routed, true) || expected.st_dev != routed.st_dev
		|| expected.st_ino != routed.st_ino) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	result = read_bytes(dirs[3], CLUSTER_WAL_THREAD_CLAIM_FILENAME, CLUSTER_WAL_CLAIM_V2_BYTES,
						CLUSTER_WAL_CLAIM_V2_BYTES, claim_bytes, &claim_len);
	if (result != 0)
		goto done;
	/* Allocation-free exact current reader; all its descriptors close locally. */
	result = cluster_wal_durable_prefix_read(wal_root, ref, &prefix);
	if (result != 0)
		goto done;
	result = read_same_dir(AT_FDCWD, pgdata, dirs[0]);
	if (result == 0)
		result = read_same_dir(AT_FDCWD, wal_root, dirs[1]);
	if (result == 0)
		result = read_same_dir(dirs[1], thread, dirs[2]);
	if (result == 0)
		result = read_same_dir(dirs[2], generation, dirs[3]);
	if (result != 0)
		goto done;
	if (fstatat(dirs[0], "pg_wal", &current, 0) != 0 || !read_owned(&current, true)
		|| current.st_dev != expected.st_dev || current.st_ino != expected.st_ino)
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
done:
	for (size_t i = 0; i < lengthof(dirs); i++)
		read_close(dirs[i], &result);
	/* Hashing may allocate/use a resource owner: no raw fd remains open. */
	if (result == 0)
		result = cluster_wal_claim_v2_decode(claim_bytes, claim_len, &ref->claim, &claim);
#endif
	return result;
}

#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_SYMLINK_NOFOLLOW)
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
read_source_files(BootstrapReadWork *work, int global, int wal,
				  const ClusterControlRootSnapshot *source, const ControlRootRecordRefsV2 *refs)
{
	const ClusterControlRootIdentity *self = &source->identity;
	ClusterControlRootResult result;
	char hex[65], name[128], thread[32], generation[48];
	const char *parts[3];
	size_t length;

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
	read_hex(refs->anchor_sha256, hex);
	snprintf(name, sizeof(name), "anchor_" UINT64_FORMAT "-%s.bin", refs->anchor_generation, hex);
	return read_object(global, parts, 3, name, CLUSTER_RECOVERY_ANCHOR_SIZE,
					   CLUSTER_RECOVERY_ANCHOR_SIZE, work->anchor, &length);
}

static ClusterControlRootResult
read_selected(BootstrapReadWork *work, int global, int wal, uint32 node, uint8 *config,
			  size_t *config_len)
{
	const ControlRootImage *root = &work->before;
	ClusterControlRootResult result;
	char hex[65], name[128];
	const char *parts[1];
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
	return read_source_files(work, global, wal, &root->records[node], &root->refs[node]);
}

static ClusterControlRootResult
read_dir_identity(int fd, const struct stat *expected)
{
	struct stat current;
	if (fstat(fd, &current) != 0 || !read_owned(&current, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return current.st_dev == expected->st_dev && current.st_ino == expected->st_ino
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

typedef enum BootstrapReadKind {
	BOOTSTRAP_SOURCE,
	BOOTSTRAP_HISTORY,
	BOOTSTRAP_STARTUP,
	BOOTSTRAP_FINAL_ROOT
} BootstrapReadKind;

/* Reopen exact initial roots for bounded reads, then close before any hashing
 * or backend allocation. No FD survives a decoder or caller interrupt. */
static ClusterControlRootResult
read_again(BootstrapReadWork *work, const char *shared_root, const char *wal_root,
		   BootstrapReadKind kind, const ClusterControlRootSnapshot *source,
		   const ControlRootRecordRefsV2 *refs)
{
	int shared = -1, global = -1, wal = -1;
	ClusterControlRootResult result;
	size_t length;

	result = read_dir(-1, shared_root, &shared);
	if (result == 0)
		result = read_dir_identity(shared, &work->shared_dir);
	if (result == 0)
		result = read_dir(shared, "global", &global);
	if (result == 0)
		result = read_dir_identity(global, &work->global_dir);
	if (result == 0)
		result = read_dir(-1, wal_root, &wal);
	if (result == 0)
		result = read_dir_identity(wal, &work->wal_dir);
	if (result != 0)
		goto done;
	if (kind == BOOTSTRAP_SOURCE)
		result = read_source_files(work, global, wal, source, refs);
	else if (kind == BOOTSTRAP_HISTORY) {
		char thread[32], hex[65], name[128];
		const char *parts[] = { "wal_history", thread };
		snprintf(thread, sizeof(thread), "thread_%u", source->identity.origin_thread_id);
		read_hex(refs->history_sha256, hex);
		snprintf(name, sizeof(name), "history_" UINT64_FORMAT "-%s.bin", refs->history_generation,
				 hex);
		result = read_object(global, parts, lengthof(parts), name,
							 CLUSTER_WAL_HISTORY_HEADER_BYTES + 4, CLUSTER_WAL_HISTORY_MAX_BYTES,
							 work->history, &work->history_len);
	} else if (kind == BOOTSTRAP_STARTUP) {
		const ControlRootStartupRefV3 *ref = &work->before.startup[source->identity.origin_node_id];
		char thread[32], hex[65], name[128];
		const char *parts[] = { "wal_startup", thread };
		snprintf(thread, sizeof(thread), "thread_%u", source->identity.origin_thread_id);
		read_hex(ref->sha256, hex);
		snprintf(name, sizeof(name), "startup_" UINT64_FORMAT "-%s.bin", ref->generation, hex);
		result = read_object(global, parts, lengthof(parts), name, CLUSTER_WAL_STARTUP_BYTES,
							 CLUSTER_WAL_STARTUP_BYTES, work->startup, &length);
	} else
		result = read_bytes(global, "pgrac_control_root", CLUSTER_CONTROL_ROOT_FILE_BYTES,
							CLUSTER_CONTROL_ROOT_FILE_BYTES, work->root_after, &length);
	if (result == 0)
		result = read_same_dir(AT_FDCWD, shared_root, shared);
	if (result == 0)
		result = read_same_dir(shared, "global", global);
	if (result == 0)
		result = read_same_dir(AT_FDCWD, wal_root, wal);
done:
	read_close(wal, &result);
	read_close(global, &result);
	read_close(shared, &result);
	return result;
}

static ClusterControlRootResult
read_source_capacity(BootstrapReadWork *work, const char *shared_root, const char *wal_root,
					 const ClusterControlRootSnapshot *source, const ControlRootRecordRefsV2 *refs,
					 ClusterControlRecoveryCapacity *required)
{
	const ControlRootHeader *header = &work->before.header;
	ClusterWalThreadClaimRefV2 claim_ref = { 0 };
	ClusterWalThreadClaimV2 claim;
	ClusterRecoveryAnchorRefV2 anchor_ref = { 0 };
	ClusterRecoveryAnchorV2 anchor;
	ClusterControlRootResult result;

	result = read_again(work, shared_root, wal_root, BOOTSTRAP_SOURCE, source, refs);
	if (result != 0)
		return result;
	claim_ref.identity = source->identity;
	claim_ref.database_incarnation = header->v2.database_incarnation;
	claim_ref.max_config_generation = header->v2.config_generation;
	memcpy(claim_ref.claim_sha256, refs->claim_sha256, 32);
	result
		= cluster_wal_claim_v2_decode(work->claim, CLUSTER_WAL_CLAIM_V2_BYTES, &claim_ref, &claim);
	if (result != 0)
		return result;
	anchor_ref.identity = source->identity;
	anchor_ref.database_incarnation = header->v2.database_incarnation;
	anchor_ref.max_config_generation = header->v2.config_generation;
	anchor_ref.anchor_generation = refs->anchor_generation;
	memcpy(anchor_ref.anchor_sha256, refs->anchor_sha256, 32);
	memcpy(anchor_ref.claim_sha256, refs->claim_sha256, 32);
	result = cluster_recovery_anchor_v2_decode(work->anchor, CLUSTER_RECOVERY_ANCHOR_SIZE,
											   &anchor_ref, &anchor);
	if (result != 0)
		return result;
	if (anchor.backup_start != InvalidXLogRecPtr || anchor.backup_end != InvalidXLogRecPtr
		|| anchor.backup_end_required)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (anchor.checkpoint_copy.redo != source->checkpoint_lower_lsn
		|| anchor.checkpoint_copy.ThisTimeLineID != source->checkpoint_tli)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
#define RECOVERY_MAX(field) required->field = Max(required->field, anchor.field)
	RECOVERY_MAX(max_connections);
	RECOVERY_MAX(max_worker_processes);
	RECOVERY_MAX(max_wal_senders);
	RECOVERY_MAX(max_prepared_xacts);
	RECOVERY_MAX(max_locks_per_xact);
#undef RECOVERY_MAX
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
read_initializing_capacity(BootstrapReadWork *work, const char *wal_root,
						   ClusterControlRecoveryCapacity *required)
{
	ClusterWalDurablePrefixRef ref = { 0 };
	ClusterWalStartupObservation input;
	ClusterControlRootResult result;
	pg_cryptohash_ctx *ctx;
	bool hashed;

	ref.claim.identity = work->pending.claim.identity;
	ref.claim.database_incarnation = work->pending.database_incarnation;
	ref.claim.max_config_generation = work->pending.config_generation;
	ref.timeline = work->pending.timeline;
	/* These exact embedded claim bytes were authenticated with the selected
	 * PGWG. Hash them before opening raw WAL descriptors; never derive the
	 * expected claim from whatever happens to be in the generation directory. */
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hashed
		= pg_cryptohash_init(ctx) >= 0
		  && pg_cryptohash_update(ctx, work->startup + 1280, CLUSTER_WAL_CLAIM_V2_BYTES) >= 0
		  && pg_cryptohash_final(ctx, ref.claim.claim_sha256, sizeof(ref.claim.claim_sha256)) >= 0;
	pg_cryptohash_free(ctx);
	if (!hashed)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_wal_startup_observe(wal_root, &ref, work->pending.segment_size,
										 work->pending.first_segment_lsn, &input);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
#define STARTUP_MAX(field) required->field = Max(required->field, input.field)
	STARTUP_MAX(max_connections);
	STARTUP_MAX(max_worker_processes);
	STARTUP_MAX(max_wal_senders);
	STARTUP_MAX(max_prepared_xacts);
	STARTUP_MAX(max_locks_per_xact);
#undef STARTUP_MAX
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
read_capacities(BootstrapReadWork *work, const char *shared_root, const char *wal_root,
				ClusterControlRecoveryCapacity *required)
{
	const ControlRootImage *root = &work->before;
	ClusterControlRootResult result;

	memset(required, 0, sizeof(*required));
	for (uint32 node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!root->present[node])
			continue;
		result = read_source_capacity(work, shared_root, wal_root, &root->records[node],
									  &root->refs[node], required);
		if (result != 0)
			return result;
		required->current_sources++;
		if (root->refs[node].history_generation != 0) {
			result = read_again(work, shared_root, wal_root, BOOTSTRAP_HISTORY,
								&root->records[node], &root->refs[node]);
			if (result != 0)
				return result;
			result = root->header.format_version == 3
						 ? cluster_control_root_v3_history_decode(work->history, work->history_len,
																  root, node, &work->retained)
						 : cluster_control_root_v2_history_decode(work->history, work->history_len,
																  root, node, &work->retained);
			if (result != 0)
				return result;
			for (uint32 i = 0; i < work->retained.count; i++) {
				const ClusterWalHistoryRecord *old = &work->retained.records[i];
				result = read_source_capacity(work, shared_root, wal_root, &old->snapshot,
											  &old->refs, required);
				if (result != 0)
					return result;
				required->history_sources++;
			}
		}
		if (root->header.format_version == 3 && root->startup[node].generation != 0) {
			result = read_again(work, shared_root, wal_root, BOOTSTRAP_STARTUP,
								&root->records[node], NULL);
			if (result != 0)
				return result;
			result = cluster_control_root_v3_startup_decode(
				work->startup, CLUSTER_WAL_STARTUP_BYTES, root, node, &work->pending);
			if (result != 0)
				return result;
			/* A pending generation is not current and need not be serving. Its
			 * published checkpoint still imposes physical recovery requirements.
			 * Before that checkpoint, an INITIALIZING WAL scan is required; never
			 * substitute the predecessor anchor or count it as clean/empty. */
			if (work->pending.phase == CLUSTER_WAL_STARTUP_INITIALIZING) {
				result = read_initializing_capacity(work, wal_root, required);
				if (result != 0)
					return result;
			}
			if (work->pending.phase == CLUSTER_WAL_STARTUP_DURABLE) {
				result = read_source_capacity(work, shared_root, wal_root,
											  &work->pending.successor.snapshot,
											  &work->pending.successor.refs, required);
				if (result != 0)
					return result;
			}
			required->pending_sources++;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	ClusterControlRecoveryCapacity required;
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
	result = cluster_control_bootstrap_root_decode(work->root_before, length, binding.storage_uuid,
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
	result = cluster_control_bootstrap_root_decode(work->root_after, length, binding.storage_uuid,
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
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (fstat(shared, &work->shared_dir) != 0 || fstat(global, &work->global_dir) != 0
			|| fstat(wal, &work->wal_dir) != 0))
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
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
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	objects = read_capacities(work, shared_root, wal_root, &required);
	/* A later root may legitimately retire an object while this provisional
	 * observation is reading it. Never blame the old object before reobserving. */
	result = read_again(work, shared_root, wal_root, BOOTSTRAP_FINAL_ROOT, NULL, NULL);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = cluster_control_bootstrap_root_decode(
		work->root_after, CLUSTER_CONTROL_ROOT_FILE_BYTES, binding.storage_uuid,
		binding.system_identifier, &work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = cluster_control_bootstrap_root_bound(&binding, &work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (memcmp(work->root_before, work->root_after, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	result = read_binding(pgdata, &later, later_bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (memcmp(binding_bytes, later_bytes, sizeof(binding_bytes)) != 0) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	result = objects;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		config[config_len] = '\0';
		out->snapshot = snapshot;
		out->required = required;
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
