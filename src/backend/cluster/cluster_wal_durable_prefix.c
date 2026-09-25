/*-------------------------------------------------------------------------
 * PGRAC: per-thread durable WAL prefix input.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_wal_durable_prefix.h"

static uint64
prefix_get(const uint8 *bytes, size_t offset, size_t width)
{
	uint64 value = 0;
	for (size_t i = 0; i < width; ++i)
		value |= (uint64)bytes[offset + i] << (8 * i);
	return value;
}

static void
prefix_put(uint8 *bytes, size_t offset, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; ++i)
		bytes[offset + i] = value >> (8 * i);
}

static bool
prefix_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;
	for (size_t i = 0; i < len; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
prefix_ref_valid(const ClusterWalDurablePrefixRef *ref)
{
	const ClusterControlRootIdentity *id;
	if (ref == NULL)
		return false;
	id = &ref->claim.identity;
	return ref->timeline != 0 && ref->claim.database_incarnation != 0
		   && ref->claim.max_config_generation != 0 && id->system_identifier != 0
		   && id->origin_thread_id > 0 && id->origin_thread_id <= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		   && id->origin_node_id == (int32)id->origin_thread_id - 1
		   && id->origin_owner_incarnation != 0 && id->root_lineage_seq != 0
		   && id->thread_claim_created_at > 0 && id->reserved42 == 0 && id->reserved60 == 0
		   && !prefix_zero(id->storage_uuid, 16) && !prefix_zero(id->authority_uuid, 16)
		   && !prefix_zero(ref->claim.claim_sha256, 32);
}

static bool
prefix_valid(const ClusterWalDurablePrefix *prefix)
{
	if (prefix->sequence == 0)
		return false;
	if (prefix->exclusive_end == 0)
		return prefix->sequence == 1 && prefix->record_start == 0 && prefix->record_crc == 0;
	return prefix->record_start != 0 && prefix->record_start < prefix->exclusive_end;
}

static uint32
prefix_crc(const uint8 *bytes)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 252);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

ClusterControlRootResult
cluster_wal_durable_prefix_encode(const ClusterWalDurablePrefixRef *ref,
								  const ClusterWalDurablePrefix *prefix,
								  uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES])
{
	const ClusterControlRootIdentity *id;
	if (bytes == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(bytes, 0, CLUSTER_WAL_DURABLE_PREFIX_BYTES);
	if (!prefix_ref_valid(ref) || prefix == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!prefix_valid(prefix))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	id = &ref->claim.identity;
	memcpy(bytes, "PGWP", 4);
	prefix_put(bytes, 4, 1, 2);
	prefix_put(bytes, 6, CLUSTER_WAL_DURABLE_PREFIX_BYTES, 2);
	prefix_put(bytes, 8, id->system_identifier, 8);
	prefix_put(bytes, 16, ref->claim.database_incarnation, 8);
	memcpy(bytes + 24, id->storage_uuid, 16);
	memcpy(bytes + 40, id->authority_uuid, 16);
	prefix_put(bytes, 56, id->origin_node_id, 4);
	prefix_put(bytes, 60, id->origin_thread_id, 4);
	prefix_put(bytes, 64, id->origin_owner_incarnation, 8);
	prefix_put(bytes, 72, ref->timeline, 4);
	memcpy(bytes + 80, ref->claim.claim_sha256, 32);
	prefix_put(bytes, 112, prefix->sequence, 8);
	prefix_put(bytes, 120, prefix->exclusive_end, 8);
	prefix_put(bytes, 128, prefix->record_start, 8);
	prefix_put(bytes, 136, prefix->record_crc, 4);
	prefix_put(bytes, 252, prefix_crc(bytes), 4);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_durable_prefix_decode(const uint8 *bytes, size_t len,
								  const ClusterWalDurablePrefixRef *ref,
								  ClusterWalDurablePrefix *out)
{
	const ClusterControlRootIdentity *id;
	ClusterWalDurablePrefix prefix;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || !prefix_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (len != CLUSTER_WAL_DURABLE_PREFIX_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (prefix_get(bytes, 252, 4) != prefix_crc(bytes))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (memcmp(bytes, "PGWP", 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (prefix_get(bytes, 4, 2) != 1)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (prefix_get(bytes, 6, 2) != CLUSTER_WAL_DURABLE_PREFIX_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (!prefix_zero(bytes + 76, 4) || !prefix_zero(bytes + 140, 112))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	id = &ref->claim.identity;
	if (prefix_get(bytes, 8, 8) != id->system_identifier
		|| prefix_get(bytes, 16, 8) != ref->claim.database_incarnation
		|| memcmp(bytes + 24, id->storage_uuid, 16) != 0
		|| memcmp(bytes + 40, id->authority_uuid, 16) != 0
		|| prefix_get(bytes, 56, 4) != (uint32)id->origin_node_id
		|| prefix_get(bytes, 60, 4) != id->origin_thread_id
		|| prefix_get(bytes, 64, 8) != id->origin_owner_incarnation
		|| prefix_get(bytes, 72, 4) != ref->timeline
		|| memcmp(bytes + 80, ref->claim.claim_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	memset(&prefix, 0, sizeof(prefix));
	prefix.sequence = prefix_get(bytes, 112, 8);
	prefix.exclusive_end = prefix_get(bytes, 120, 8);
	prefix.record_start = prefix_get(bytes, 128, 8);
	prefix.record_crc = prefix_get(bytes, 136, 4);
	if (!prefix_valid(&prefix))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	*out = prefix;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_durable_prefix_successor(const ClusterWalDurablePrefixRef *ref,
									 const ClusterWalDurablePrefix *previous,
									 const ClusterWalDurablePrefix *next)
{
	if (!prefix_ref_valid(ref) || previous == NULL || next == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!prefix_valid(previous) || !prefix_valid(next))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (previous->sequence == next->sequence) {
		if (previous->exclusive_end == next->exclusive_end
			&& previous->record_start == next->record_start
			&& previous->record_crc == next->record_crc)
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		return CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
	}
	if (previous->sequence == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	if (next->sequence != previous->sequence + 1 || next->exclusive_end <= previous->exclusive_end
		|| next->record_start < previous->exclusive_end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
prefix_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode) && st->st_nlink == 1)
		   && st->st_uid == geteuid() && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

ClusterControlRootResult
cluster_wal_durable_prefix_read(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
								ClusterWalDurablePrefix *out)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	int fds[5] = { -1, -1, -1, -1, -1 };
	char thread[32], generation[48];
	const char *parts[] = { thread, generation, "durable_prefix" };
	struct stat st;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES + 1];
	size_t used = 0;
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (wal_root == NULL || wal_root[0] == '\0' || !prefix_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	snprintf(thread, sizeof(thread), "thread_%u", ref->claim.identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 ref->claim.identity.origin_owner_incarnation);
	/* No backend allocations/ERROR-capable calls while raw descriptors are open. */
	fds[0] = open(wal_root, dirflags);
	if (fds[0] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[0], &st) != 0 || !prefix_owned(&st, true))
		goto done;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		fds[i + 1] = openat(fds[i], parts[i], dirflags);
		if (fds[i + 1] < 0) {
			if (errno == ENOENT)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
			goto done;
		}
		if (fstat(fds[i + 1], &st) != 0 || !prefix_owned(&st, true))
			goto done;
	}
	fds[4] = openat(fds[3], "current", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fds[4] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[4], &st) != 0 || !prefix_owned(&st, false))
		goto done;
	if (st.st_size != CLUSTER_WAL_DURABLE_PREFIX_BYTES) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < sizeof(bytes)) {
		ssize_t n = read(fds[4], bytes + used, sizeof(bytes) - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	result = used == CLUSTER_WAL_DURABLE_PREFIX_BYTES ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
													  : CLUSTER_CONTROL_ROOT_BAD_SIZE;
done:
	for (size_t i = 0; i < lengthof(fds); ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_durable_prefix_decode(bytes, used, ref, out);
	return result;
}
