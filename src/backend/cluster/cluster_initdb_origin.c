/* Original creator's immutable claim and native checkpoint anchor.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "cluster_initdb_origin_private.h"
#include "cluster_recovery_anchor_private.h"
#include "../../bin/initdb/pgrac_wal.h"

static bool
origin_hash(const uint8 *bytes, Size length, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (ctx == NULL) return false;
	ok = pg_cryptohash_init(ctx) >= 0
		&& pg_cryptohash_update(ctx, bytes, length) >= 0
		&& pg_cryptohash_final(ctx, hash, 32) >= 0;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
origin_same(const struct stat *left, const struct stat *right)
{
	return left->st_dev == right->st_dev && left->st_ino == right->st_ino
		&& left->st_mode == right->st_mode && left->st_uid == right->st_uid
		&& left->st_nlink == right->st_nlink && left->st_size == right->st_size;
}

/* No overwrite, rename or cleanup of an unselected partial result. Reopen
 * the name after fsync, compare actual bytes and persist its directory entry. */
static bool
origin_write_new(int directory, const char *name, const uint8 *bytes, Size length)
{
	struct stat parent, current, file, named;
	uint8 readback[CLUSTER_RECOVERY_ANCHOR_SIZE + 1];
	Size used = 0;
	ssize_t n;
	int fd = -1;
	bool ok = false;

	if (length == 0 || length >= sizeof(readback) || fstat(directory, &parent) != 0
		|| !S_ISDIR(parent.st_mode) || parent.st_uid != geteuid()
		|| (parent.st_mode & 0022) != 0)
		return false;
	fd = openat(directory, name, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
				pg_file_create_mode);
	if (fd < 0) return false;
	while (used < length)
	{
		n = pwrite(fd, bytes + used, length - used, used);
		if (n < 0 && errno == EINTR) continue;
		if (n <= 0) goto done;
		used += n;
	}
	if (fsync(fd) != 0 || fstat(fd, &file) != 0 || !S_ISREG(file.st_mode)
		|| file.st_uid != geteuid() || file.st_nlink != 1 || (file.st_mode & 0022) != 0
		|| file.st_size != length)
		goto done;
	if (close(fd) != 0) { fd = -1; goto done; }
	fd = openat(directory, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &named) != 0 || !origin_same(&file, &named)) goto done;
	used = 0;
	while (used < length + 1)
	{
		n = pread(fd, readback + used, length + 1 - used, used);
		if (n < 0 && errno == EINTR) continue;
		if (n < 0) goto done;
		if (n == 0) break;
		used += n;
	}
	if (used != length || memcmp(readback, bytes, length) != 0
		|| fstat(fd, &current) != 0 || !origin_same(&file, &current)
		|| fstatat(directory, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !origin_same(&file, &named))
		goto done;
	if (close(fd) != 0) { fd = -1; goto done; }
	fd = -1;
	if (fsync(directory) != 0 || fstat(directory, &current) != 0
		|| parent.st_dev != current.st_dev || parent.st_ino != current.st_ino
		|| parent.st_uid != current.st_uid || parent.st_mode != current.st_mode)
		goto done;
	ok = true;
done:
	if (fd >= 0 && close(fd) != 0) ok = false;
	return ok;
}

bool
cluster_initdb_origin_create(const ClusterSharedConfigRef *config, uint32 node,
	int64 created_at, int wal_fd, int anchor_fd, const ControlFileData *control,
	ClusterWalHistoryRecord *out)
{
	ClusterWalHistoryRecord result = {0};
	ClusterControlRootSnapshot *record = &result.snapshot;
	ClusterWalThreadClaimV2 claim = {0}, decoded_claim;
	ClusterWalThreadClaimRefV2 claim_ref = {0};
	ClusterRecoveryAnchorV2 anchor = {0}, decoded_anchor;
	ClusterRecoveryAnchorRefV2 anchor_ref = {0};
	PgracInitdbWalObservation observed;
	uint8 claim_bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	uint8 anchor_bytes[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char name[112], hex[65];

	if (out == NULL) return false;
	memset(out, 0, sizeof(*out));
	if (IsUnderPostmaster || config == NULL || control == NULL
		|| node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT || created_at <= 0
		|| config->identity.generation != 1 || config->identity.database_incarnation == 0
		|| !(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))
		|| control->system_identifier != config->identity.system_identifier
		|| control->minRecoveryPoint != 0 || control->minRecoveryPointTLI != 0
		|| control->backupStartPoint != 0 || control->backupEndPoint != 0
		|| control->backupEndRequired
		|| !pgrac_initdb_wal_observe(wal_fd, control, node + 1, &observed))
		return false;
	claim.identity.system_identifier = config->identity.system_identifier;
	memcpy(claim.identity.storage_uuid, config->identity.storage_uuid, 16);
	memcpy(claim.identity.authority_uuid, config->identity.authority_uuid, 16);
	claim.identity.origin_node_id = node;
	claim.identity.origin_thread_id = node + 1;
	claim.identity.origin_owner_incarnation = created_at;
	claim.identity.root_lineage_seq = 1;
	claim.identity.thread_claim_created_at = created_at;
	claim.database_incarnation = config->identity.database_incarnation;
	claim.config_generation = claim.claim_generation = 1;
	if (cluster_wal_claim_v2_encode(&claim, claim_bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !origin_hash(claim_bytes, sizeof(claim_bytes), claim_ref.claim_sha256))
		return false;
	/* CRC is part of the complete identity selected by both original codecs. */
	for (unsigned i = 0; i < 4; i++)
		claim.identity.thread_claim_crc32c |= (uint32)claim_bytes[104 + i] << (i * 8);
	claim_ref.identity = claim.identity;
	claim_ref.database_incarnation = claim.database_incarnation;
	claim_ref.max_config_generation = 1;
	if (cluster_wal_claim_v2_decode(claim_bytes, sizeof(claim_bytes), &claim_ref,
								&decoded_claim) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	anchor.identity = claim.identity;
	anchor.database_incarnation = claim.database_incarnation;
	anchor.config_generation = anchor.anchor_generation = 1;
	memcpy(anchor.claim_sha256, claim_ref.claim_sha256, 32);
	anchor.state = control->state;
	anchor.checkpoint = control->checkPoint;
	anchor.write_time = control->time;
	anchor.checkpoint_copy = control->checkPointCopy;
	anchor.unlogged_lsn = control->unloggedLSN;
	anchor.wal_log_hints = control->wal_log_hints;
	anchor.track_commit_timestamp = control->track_commit_timestamp;
	anchor.wal_level = control->wal_level;
	anchor.max_connections = control->MaxConnections;
	anchor.max_worker_processes = control->max_worker_processes;
	anchor.max_wal_senders = control->max_wal_senders;
	anchor.max_prepared_xacts = control->max_prepared_xacts;
	anchor.max_locks_per_xact = control->max_locks_per_xact;
	anchor_ref.identity = claim.identity;
	anchor_ref.database_incarnation = claim.database_incarnation;
	anchor_ref.max_config_generation = anchor_ref.anchor_generation = 1;
	memcpy(anchor_ref.claim_sha256, claim_ref.claim_sha256, 32);
	if (cluster_recovery_anchor_v2_encode(&anchor, anchor_bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !origin_hash(anchor_bytes, sizeof(anchor_bytes), anchor_ref.anchor_sha256)
		|| cluster_recovery_anchor_v2_decode(anchor_bytes, sizeof(anchor_bytes), &anchor_ref,
										 &decoded_anchor) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	for (unsigned i = 0; i < 32; i++)
		snprintf(hex + i * 2, 3, "%02x", anchor_ref.anchor_sha256[i]);
	snprintf(name, sizeof(name), "anchor_1-%s.bin", hex);
	if (!origin_write_new(wal_fd, CLUSTER_WAL_THREAD_CLAIM_FILENAME, claim_bytes, sizeof(claim_bytes))
		|| !origin_write_new(anchor_fd, name, anchor_bytes, sizeof(anchor_bytes)))
		return false;
	record->identity = claim.identity;
	record->root_publish_seq = 1;
	record->published_at_usec = created_at;
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE;
	record->checkpoint_tli = record->tail_tli = control->checkPointCopy.ThisTimeLineID;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->checkpoint_lower_lsn = observed.checkpoint_start;
	record->checkpoint_record_crc32c = observed.checkpoint_crc;
	record->root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->validated_tail_lsn_exclusive = observed.checkpoint_end;
	record->tail_last_record_lsn = observed.checkpoint_start;
	record->tail_last_record_crc32c = observed.checkpoint_crc;
	result.refs.anchor_generation = 1;
	memcpy(result.refs.anchor_sha256, anchor_ref.anchor_sha256, 32);
	memcpy(result.refs.claim_sha256, claim_ref.claim_sha256, 32);
	result.publisher_node = node;
	result.publisher_incarnation = created_at;
	*out = result;
	return true;
}
