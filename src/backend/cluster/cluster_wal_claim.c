/*-------------------------------------------------------------------------
 * PGRAC: root-selected WAL generation claims.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"

static uint64
claim_get(const uint8 *bytes, size_t offset, size_t width)
{
	uint64 value = 0;

	for (size_t i = 0; i < width; ++i)
		value |= (uint64)bytes[offset + i] << (8 * i);
	return value;
}

static void
claim_put(uint8 *bytes, size_t offset, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; ++i)
		bytes[offset + i] = value >> (8 * i);
}

static bool
claim_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;

	for (size_t i = 0; i < len; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
claim_identity_valid(const ClusterControlRootIdentity *id)
{
	return id->system_identifier != 0 && id->origin_thread_id > 0
		   && id->origin_thread_id <= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		   && id->origin_node_id == (int32)id->origin_thread_id - 1 && id->reserved42 == 0
		   && id->reserved60 == 0 && id->thread_claim_created_at > 0
		   && id->origin_owner_incarnation != 0 && id->root_lineage_seq != 0
		   && !claim_zero(id->storage_uuid, 16) && !claim_zero(id->authority_uuid, 16);
}

bool
cluster_wal_claim_v2_ref_valid(const ClusterWalThreadClaimRefV2 *ref)
{
	return ref != NULL && claim_identity_valid(&ref->identity) && ref->database_incarnation != 0
		   && ref->max_config_generation != 0 && !claim_zero(ref->claim_sha256, 32);
}

static ClusterControlRootResult
claim_fields_valid(const ClusterWalThreadClaimV2 *claim)
{
	if (claim->identity.reserved42 != 0 || claim->identity.reserved60 != 0)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (!claim_identity_valid(&claim->identity))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (claim->database_incarnation == 0 || claim->config_generation == 0
		|| claim->claim_generation == 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static uint32
claim_crc(const uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 104);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

ClusterControlRootResult
cluster_wal_claim_v2_encode(const ClusterWalThreadClaimV2 *claim,
							uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES])
{
	ClusterControlRootResult result;
	uint32 crc;

	if (bytes == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(bytes, 0, CLUSTER_WAL_CLAIM_V2_BYTES);
	if (claim == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = claim_fields_valid(claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
#define CP(offset, field, width) claim_put(bytes, offset, claim->field, width)
	claim_put(bytes, 0, CLUSTER_WAL_THREAD_CLAIM_MAGIC, 4);
	claim_put(bytes, 4, 2, 2);
	CP(6, identity.origin_thread_id, 2);
	CP(8, identity.origin_node_id, 4);
	CP(16, identity.system_identifier, 8);
	CP(24, database_incarnation, 8);
	CP(32, identity.origin_owner_incarnation, 8);
	memcpy(bytes + 40, claim->identity.storage_uuid, 16);
	memcpy(bytes + 56, claim->identity.authority_uuid, 16);
	CP(72, identity.root_lineage_seq, 8);
	CP(80, identity.thread_claim_created_at, 8);
	CP(88, config_generation, 8);
	CP(96, claim_generation, 8);
#undef CP
	crc = claim_crc(bytes);
	if (claim->identity.thread_claim_crc32c != 0 && claim->identity.thread_claim_crc32c != crc) {
		memset(bytes, 0, CLUSTER_WAL_CLAIM_V2_BYTES);
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	}
	claim_put(bytes, 104, crc, 4);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_claim_v2_decode(const uint8 *bytes, size_t len, const ClusterWalThreadClaimRefV2 *ref,
							ClusterWalThreadClaimV2 *out)
{
	ClusterWalThreadClaimV2 claim;
	ClusterControlRootResult result;
	pg_cryptohash_ctx *ctx;
	uint8 hash[32];
	bool hash_ok;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || !cluster_wal_claim_v2_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (len != CLUSTER_WAL_CLAIM_V2_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (claim_get(bytes, 104, 4) != claim_crc(bytes))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (claim_get(bytes, 0, 4) != CLUSTER_WAL_THREAD_CLAIM_MAGIC)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (claim_get(bytes, 4, 2) != 2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!claim_zero(bytes + 12, 4) || !claim_zero(bytes + 108, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	memset(&claim, 0, sizeof(claim));
#define CG(offset, field, width) claim.field = claim_get(bytes, offset, width)
	CG(6, identity.origin_thread_id, 2);
	CG(8, identity.origin_node_id, 4);
	CG(16, identity.system_identifier, 8);
	CG(24, database_incarnation, 8);
	CG(32, identity.origin_owner_incarnation, 8);
	memcpy(claim.identity.storage_uuid, bytes + 40, 16);
	memcpy(claim.identity.authority_uuid, bytes + 56, 16);
	CG(72, identity.root_lineage_seq, 8);
	CG(80, identity.thread_claim_created_at, 8);
	CG(88, config_generation, 8);
	CG(96, claim_generation, 8);
	CG(104, identity.thread_claim_crc32c, 4);
#undef CG
	result = claim_fields_valid(&claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (claim.config_generation > ref->max_config_generation)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* Identity's frozen 80-byte layout has no padding; reserved bytes were
	 * validated on both sides. Include claim CRC as well as every namespace.
	 */
	if (memcmp(&claim.identity, &ref->identity, sizeof(claim.identity)) != 0
		|| claim.database_incarnation != ref->database_incarnation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hash_ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
			  && pg_cryptohash_final(ctx, hash, sizeof(hash)) >= 0;
	pg_cryptohash_free(ctx);
	if (!hash_ok)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, ref->claim_sha256, sizeof(hash)) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	*out = claim;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
claim_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

ClusterControlRootResult
cluster_wal_claim_v2_read(const char *wal_root, const ClusterWalThreadClaimRefV2 *ref,
						  ClusterWalThreadClaimV2 *out)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	int fds[4] = { -1, -1, -1, -1 };
	char thread[32], generation[48];
	const char *parts[2];
	struct stat st;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES + 1];
	size_t used = 0;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (wal_root == NULL || wal_root[0] == '\0' || !cluster_wal_claim_v2_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	snprintf(thread, sizeof(thread), "thread_%u", ref->identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 ref->identity.origin_owner_incarnation);
	parts[0] = thread;
	parts[1] = generation;
	/* No allocation or error-throwing backend call while raw FDs are open. */
	fds[0] = open(wal_root, dirflags);
	if (fds[0] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[0], &st) != 0 || !claim_owned(&st, true))
		goto done;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		fds[i + 1] = openat(fds[i], parts[i], dirflags);
		if (fds[i + 1] < 0) {
			if (errno == ENOENT)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
			goto done;
		}
		if (fstat(fds[i + 1], &st) != 0 || !claim_owned(&st, true))
			goto done;
	}
	fds[3] = openat(fds[2], CLUSTER_WAL_THREAD_CLAIM_FILENAME,
					O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fds[3] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[3], &st) != 0 || !claim_owned(&st, false))
		goto done;
	if (st.st_size != CLUSTER_WAL_CLAIM_V2_BYTES) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	/* Read one extra byte to reject concurrent extension, not just stat size. */
	while (used < sizeof(bytes)) {
		ssize_t n = read(fds[3], bytes + used, sizeof(bytes) - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	result = used == CLUSTER_WAL_CLAIM_V2_BYTES ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
												: CLUSTER_CONTROL_ROOT_BAD_SIZE;
done:
	for (size_t i = 0; i < lengthof(fds); ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_claim_v2_decode(bytes, used, ref, out);
	return result;
}
