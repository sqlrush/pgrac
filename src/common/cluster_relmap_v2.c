/*-------------------------------------------------------------------------
 *
 * cluster_relmap_v2.c
 *    Relmap v2 codec and double-copy committed/pending reader.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/common/cluster_relmap_v2.c
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "common/cluster_relmap_v2.h"

/* Header offsets are wire constants, never offsetof an in-memory struct. */
static uint64
read_le(const unsigned char *p, size_t width)
{
	uint64 value = 0;

	for (size_t i = 0; i < width; i++)
		value |= (uint64)p[i] << (8 * i);
	return value;
}

static void
write_le(unsigned char *p, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; i++)
		p[i] = (unsigned char)(value >> (8 * i));
}

static pg_crc32c
image_crc(const void *bytes, size_t length)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return crc;
}

static bool
all_zero(const void *bytes, size_t length)
{
	const unsigned char *p = bytes;

	for (size_t i = 0; i < length; i++)
		if (p[i] != 0)
			return false;
	return true;
}

static bool
valid_namespace(const ClusterRelmapV2Namespace *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && identity->database_incarnation != 0
		   && identity->shared == (identity->dbid == InvalidOid);
}

/* Same native shape accepted by the cold relmap scanner, without I/O. */
static bool
valid_native_map(const unsigned char *slot, RelMapFile *map)
{
	memcpy(map, slot, sizeof(*map));
	if (map->magic != RELMAPPER_FILEMAGIC || map->num_mappings < 0
		|| map->num_mappings > MAX_MAPPINGS
		|| !EQ_CRC32C(map->crc, image_crc(map, offsetof(RelMapFile, crc))))
		return false;
	for (int i = 0; i < map->num_mappings; i++)
		if (map->mappings[i].mapoid == InvalidOid
			|| map->mappings[i].mapfilenumber == InvalidRelFileNumber)
			return false;
	return all_zero(slot + sizeof(*map), CLUSTER_RELMAP_V2_SLOT_SIZE - sizeof(*map));
}

static bool
valid_wire_format(const unsigned char *p)
{
	return read_le(p, 4) == CLUSTER_RELMAP_V2_MAGIC
		   && read_le(p + 4, 2) == CLUSTER_RELMAP_V2_VERSION
		   && read_le(p + 6, 2) == CLUSTER_RELMAP_V2_HEADER_SIZE
		   && read_le(p + 8, 4) == CLUSTER_RELMAP_V2_FILE_SIZE
		   && read_le(p + 12, 4) == sizeof(RelMapFile) && p[52] <= 1 && p[53] == 0
		   && all_zero(p + 96, 20);
}

static ClusterRelmapV2Validity
decode_header(const unsigned char *p, const ClusterRelmapV2Namespace *expected,
			  ClusterRelmapV2Image *image)
{
	image->identity.system_identifier = read_le(p + 16, 8);
	image->identity.database_incarnation = read_le(p + 24, 8);
	image->identity.dbid = (Oid)read_le(p + 48, 4);
	image->identity.shared = p[52] != 0;
	if (!valid_namespace(&image->identity)
		|| image->identity.system_identifier != expected->system_identifier
		|| image->identity.database_incarnation != expected->database_incarnation
		|| image->identity.dbid != expected->dbid || image->identity.shared != expected->shared)
		return CLUSTER_RELMAP_V2_INVALID_IDENTITY;
	image->committed_generation = read_le(p + 32, 8);
	image->pending_generation = read_le(p + 40, 8);
	if (image->committed_generation == 0
		|| (image->pending_generation != 0
			&& (image->committed_generation == PG_UINT64_MAX
				|| image->pending_generation != image->committed_generation + 1)))
		return CLUSTER_RELMAP_V2_INVALID_GENERATION;
	image->pending_owner.node = (uint16)read_le(p + 54, 2);
	image->pending_owner.top_full_xid = read_le(p + 56, 8);
	image->pending_owner.membership_epoch = read_le(p + 64, 8);
	image->pending_owner.admitted_incarnation = read_le(p + 72, 8);
	image->pending_owner.wal_end = read_le(p + 80, 8);
	image->pending_owner.boot_incarnation = read_le(p + 88, 8);
	if (image->pending_generation == 0)
		return all_zero(p + 54, 42) ? CLUSTER_RELMAP_V2_VALID : CLUSTER_RELMAP_V2_INVALID_OWNER;
	/* node_id is 0..127; a FullXid must also have a normal low 32-bit xid. */
	if (image->pending_owner.node >= 128 || (uint32)image->pending_owner.top_full_xid < 3
		|| image->pending_owner.admitted_incarnation == 0 || image->pending_owner.wal_end == 0
		|| image->pending_owner.boot_incarnation == 0)
		return CLUSTER_RELMAP_V2_INVALID_OWNER;
	return CLUSTER_RELMAP_V2_VALID;
}

ClusterRelmapV2Validity
cluster_relmap_v2_decode(const void *bytes, size_t length, const ClusterRelmapV2Namespace *expected,
						 ClusterRelmapV2Image *output)
{
	const unsigned char *p = bytes;
	ClusterRelmapV2Image image;
	ClusterRelmapV2Validity validity;

	if (output != NULL)
		memset(output, 0, sizeof(*output));
	if (output == NULL || !valid_namespace(expected))
		return CLUSTER_RELMAP_V2_INVALID_ARGUMENT;
	if (bytes == NULL || length != CLUSTER_RELMAP_V2_FILE_SIZE)
		return CLUSTER_RELMAP_V2_INVALID_LENGTH;
	if (!valid_wire_format(p))
		return CLUSTER_RELMAP_V2_INVALID_FORMAT;
	if (read_le(p + 124, 4) != image_crc(p, 124)
		|| read_le(p + 116, 4)
			   != image_crc(p + CLUSTER_RELMAP_V2_COMMITTED_OFFSET, CLUSTER_RELMAP_V2_SLOT_SIZE)
		|| read_le(p + 120, 4)
			   != image_crc(p + CLUSTER_RELMAP_V2_PENDING_OFFSET, CLUSTER_RELMAP_V2_SLOT_SIZE))
		return CLUSTER_RELMAP_V2_INVALID_CRC;
	memset(&image, 0, sizeof(image));
	validity = decode_header(p, expected, &image);
	if (validity != CLUSTER_RELMAP_V2_VALID)
		return validity;
	if (!valid_native_map(p + CLUSTER_RELMAP_V2_COMMITTED_OFFSET, &image.committed))
		return CLUSTER_RELMAP_V2_INVALID_IMAGE;
	if (image.pending_generation != 0) {
		if (!valid_native_map(p + CLUSTER_RELMAP_V2_PENDING_OFFSET, &image.pending))
			return CLUSTER_RELMAP_V2_INVALID_IMAGE;
	} else if (!all_zero(p + CLUSTER_RELMAP_V2_PENDING_OFFSET, CLUSTER_RELMAP_V2_SLOT_SIZE))
		return CLUSTER_RELMAP_V2_INVALID_IMAGE;
	*output = image;
	return CLUSTER_RELMAP_V2_VALID;
}

ClusterRelmapV2Validity
cluster_relmap_v2_encode(const ClusterRelmapV2Image *image, void *bytes, size_t length)
{
	unsigned char wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapV2Image checked;
	ClusterRelmapV2Validity validity;

	if (image == NULL || bytes == NULL)
		return CLUSTER_RELMAP_V2_INVALID_ARGUMENT;
	if (length != sizeof(wire))
		return CLUSTER_RELMAP_V2_INVALID_LENGTH;
	memset(wire, 0, sizeof(wire));
	write_le(wire, CLUSTER_RELMAP_V2_MAGIC, 4);
	write_le(wire + 4, CLUSTER_RELMAP_V2_VERSION, 2);
	write_le(wire + 6, CLUSTER_RELMAP_V2_HEADER_SIZE, 2);
	write_le(wire + 8, CLUSTER_RELMAP_V2_FILE_SIZE, 4);
	write_le(wire + 12, sizeof(RelMapFile), 4);
	write_le(wire + 16, image->identity.system_identifier, 8);
	write_le(wire + 24, image->identity.database_incarnation, 8);
	write_le(wire + 32, image->committed_generation, 8);
	write_le(wire + 40, image->pending_generation, 8);
	write_le(wire + 48, image->identity.dbid, 4);
	wire[52] = image->identity.shared ? 1 : 0;
	write_le(wire + 54, image->pending_owner.node, 2);
	write_le(wire + 56, image->pending_owner.top_full_xid, 8);
	write_le(wire + 64, image->pending_owner.membership_epoch, 8);
	write_le(wire + 72, image->pending_owner.admitted_incarnation, 8);
	write_le(wire + 80, image->pending_owner.wal_end, 8);
	write_le(wire + 88, image->pending_owner.boot_incarnation, 8);
	memcpy(wire + CLUSTER_RELMAP_V2_COMMITTED_OFFSET, &image->committed, sizeof(RelMapFile));
	memcpy(wire + CLUSTER_RELMAP_V2_PENDING_OFFSET, &image->pending, sizeof(RelMapFile));
	write_le(wire + 116,
			 image_crc(wire + CLUSTER_RELMAP_V2_COMMITTED_OFFSET, CLUSTER_RELMAP_V2_SLOT_SIZE), 4);
	write_le(wire + 120,
			 image_crc(wire + CLUSTER_RELMAP_V2_PENDING_OFFSET, CLUSTER_RELMAP_V2_SLOT_SIZE), 4);
	write_le(wire + 124, image_crc(wire, 124), 4);
	validity = cluster_relmap_v2_decode(wire, sizeof(wire), &image->identity, &checked);
	if (validity == CLUSTER_RELMAP_V2_VALID)
		memcpy(bytes, wire, sizeof(wire));
	return validity;
}

static bool
same_pending(const ClusterRelmapV2Image *a, const ClusterRelmapV2Image *b)
{
	const ClusterRelmapV2Owner *x = &a->pending_owner;
	const ClusterRelmapV2Owner *y = &b->pending_owner;

	return a->pending_generation == b->pending_generation && x->node == y->node
		   && x->top_full_xid == y->top_full_xid && x->membership_epoch == y->membership_epoch
		   && x->admitted_incarnation == y->admitted_incarnation && x->wal_end == y->wal_end
		   && x->boot_incarnation == y->boot_incarnation
		   && memcmp(&a->pending, &b->pending, sizeof(RelMapFile)) == 0;
}

static bool
compatible_copies(const ClusterRelmapV2Image *primary, const ClusterRelmapV2Image *backup)
{
	if (primary->committed_generation == backup->committed_generation) {
		if (memcmp(&primary->committed, &backup->committed, sizeof(RelMapFile)) != 0)
			return false;
		if (primary->pending_generation != 0 && backup->pending_generation != 0)
			return same_pending(primary, backup);
		/* Stage and discard can leave exactly one copy with pending. */
		return true;
	}
	/* Only publish advances the primary while the backup still retains pending. */
	return backup->committed_generation != PG_UINT64_MAX
		   && primary->committed_generation == backup->committed_generation + 1
		   && primary->pending_generation == 0
		   && backup->pending_generation == primary->committed_generation
		   && memcmp(&primary->committed, &backup->pending, sizeof(RelMapFile)) == 0;
}

static bool
recoverable_copy_error(ClusterRelmapV2Validity validity)
{
	/* Foreign formats/identities are refusals, not permission to adopt another copy. */
	return validity == CLUSTER_RELMAP_V2_INVALID_LENGTH
		   || validity == CLUSTER_RELMAP_V2_INVALID_CRC;
}

ClusterRelmapV2PairResult
cluster_relmap_v2_read_pair(const void *primary, size_t primary_length, const void *backup,
							size_t backup_length, const ClusterRelmapV2Namespace *expected,
							ClusterRelmapV2View *output)
{
	ClusterRelmapV2Image first;
	ClusterRelmapV2Image second;
	ClusterRelmapV2Validity p;
	ClusterRelmapV2Validity b;
	const ClusterRelmapV2Image *chosen;
	const ClusterRelmapV2Image *pending;
	bool complete;

	if (output == NULL)
		return CLUSTER_RELMAP_V2_PAIR_INVALID;
	memset(output, 0, sizeof(*output));
	p = cluster_relmap_v2_decode(primary, primary_length, expected, &first);
	b = cluster_relmap_v2_decode(backup, backup_length, expected, &second);
	complete = p == CLUSTER_RELMAP_V2_VALID && b == CLUSTER_RELMAP_V2_VALID;
	if (complete) {
		if (!compatible_copies(&first, &second))
			return CLUSTER_RELMAP_V2_PAIR_CONFLICT;
		chosen = &first;
		pending = first.pending_generation != 0 ? &first : &second;
	} else if (p == CLUSTER_RELMAP_V2_VALID && recoverable_copy_error(b))
		chosen = pending = &first;
	else if (b == CLUSTER_RELMAP_V2_VALID && recoverable_copy_error(p)) {
		chosen = pending = &second;
		output->used_backup = true;
	} else
		return CLUSTER_RELMAP_V2_PAIR_INVALID;
	output->committed = chosen->committed;
	output->committed_generation = chosen->committed_generation;
	output->pending_complete = complete;
	output->has_pending = pending->pending_generation != 0;
	if (output->has_pending) {
		output->pending_generation = pending->pending_generation;
		output->pending_owner = pending->pending_owner;
		output->pending = pending->pending;
		output->pending_committed_generation = pending->committed_generation;
		output->pending_committed = pending->committed;
	}
	return complete ? CLUSTER_RELMAP_V2_PAIR_VALID : CLUSTER_RELMAP_V2_PAIR_DEGRADED;
}
