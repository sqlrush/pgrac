/*-------------------------------------------------------------------------
 *
 * cluster_undo_root_descriptor.c
 *    PGRD V1 persistent undo-root descriptor pure codec and identity math.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_undo_root_descriptor.h"
#include "port/pg_crc32c.h"


#define PGRD_OFF_MAGIC 0
#define PGRD_OFF_VERSION 4
#define PGRD_OFF_HEADER_LEN 6
#define PGRD_OFF_INCARNATION 8
#define PGRD_OFF_ROOT_KIND 16
#define PGRD_OFF_RESERVED0 17
#define PGRD_OFF_OWNER_NODE 20
#define PGRD_OFF_ROOT_ORDINAL 24
#define PGRD_OFF_RESERVED1 28
#define PGRD_OFF_ROOT_UUID 32
#define PGRD_OFF_NAMESPACE_ID 48
#define PGRD_OFF_SYSTEM_IDENTIFIER 56
#define PGRD_OFF_RESERVED2 64


static uint16
pgrd_get_le16(const uint8 *in)
{
	return (uint16)in[0] | ((uint16)in[1] << 8);
}


static uint32
pgrd_get_le32(const uint8 *in)
{
	return (uint32)in[0] | ((uint32)in[1] << 8) | ((uint32)in[2] << 16) | ((uint32)in[3] << 24);
}


static uint64
pgrd_get_le64(const uint8 *in)
{
	return (uint64)pgrd_get_le32(in) | ((uint64)pgrd_get_le32(in + 4) << 32);
}


static void
pgrd_put_le16(uint8 *out, uint16 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
}


static void
pgrd_put_le32(uint8 *out, uint32 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
	out[2] = (uint8)(value >> 16);
	out[3] = (uint8)(value >> 24);
}


static void
pgrd_put_le64(uint8 *out, uint64 value)
{
	pgrd_put_le32(out, (uint32)value);
	pgrd_put_le32(out + 4, (uint32)(value >> 32));
}


static bool
pgrd_bytes_zero(const uint8 *bytes, Size length)
{
	Size i;

	for (i = 0; i < length; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}


static uint32
pgrd_crc32c(const uint8 bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


bool
cluster_undo_root_namespace_id(uint64 descriptor_incarnation, uint32 root_ordinal,
							   uint64 *namespace_id)
{
	uint64 ordinal = (uint64)root_ordinal + 1;
	uint64 incarnation_index;
	uint64 value;

	if (namespace_id == NULL || descriptor_incarnation == 0
		|| root_ordinal >= CLUSTER_UNDO_ROOT_COUNT)
		return false;
	incarnation_index = descriptor_incarnation - 1;
	if (incarnation_index > (CLUSTER_UNDO_ROOT_MAX_NAMESPACE - ordinal) / CLUSTER_UNDO_ROOT_COUNT)
		return false;
	value = incarnation_index * CLUSTER_UNDO_ROOT_COUNT + ordinal;
	if (value == 0 || value > CLUSTER_UNDO_ROOT_MAX_NAMESPACE)
		return false;
	*namespace_id = value;
	return true;
}


bool
cluster_undo_root_file_slot(uint32 owner_instance, uint32 segment_slot, uint32 *file_slot)
{
	if (file_slot == NULL || owner_instance == 0 || owner_instance > CLUSTER_MAX_NODES
		|| segment_slot >= 256)
		return false;
	*file_slot = (owner_instance - 1) * 256 + segment_slot;
	return true;
}


bool
cluster_undo_root_id(uint64 namespace_id, uint32 file_slot, uint64 *root_id)
{
	if (root_id == NULL || namespace_id == 0 || namespace_id > CLUSTER_UNDO_ROOT_MAX_NAMESPACE
		|| file_slot >= CLUSTER_UNDO_ROOT_FILE_SLOTS)
		return false;
	*root_id = (namespace_id << 15) | file_slot;
	return true;
}


static bool
pgrd_descriptor_valid(const ClusterUndoRootDescriptorV1 *descriptor)
{
	uint64 expected_namespace;

	if (descriptor == NULL || descriptor->descriptor_incarnation == 0
		|| descriptor->system_identifier == 0
		|| pgrd_bytes_zero(descriptor->root_uuid, CLUSTER_UNDO_ROOT_UUID_BYTES))
		return false;
	if (descriptor->root_kind == CLUSTER_UNDO_ROOT_KIND_SHARED) {
		if (descriptor->owner_node != -1 || descriptor->root_ordinal != 0)
			return false;
	} else if (descriptor->root_kind == CLUSTER_UNDO_ROOT_KIND_LOCAL) {
		if (descriptor->owner_node < 0 || descriptor->owner_node >= CLUSTER_MAX_NODES
			|| descriptor->root_ordinal != (uint32)descriptor->owner_node + 1)
			return false;
	} else
		return false;
	return cluster_undo_root_namespace_id(descriptor->descriptor_incarnation,
										  descriptor->root_ordinal, &expected_namespace)
		   && descriptor->namespace_id == expected_namespace;
}


bool
cluster_undo_root_descriptor_encode(const ClusterUndoRootDescriptorV1 *descriptor,
									uint8 out[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (out == NULL || !pgrd_descriptor_valid(descriptor))
		return false;
	memset(image, 0, sizeof(image));
	pgrd_put_le32(image + PGRD_OFF_MAGIC, CLUSTER_UNDO_ROOT_DESCRIPTOR_MAGIC);
	pgrd_put_le16(image + PGRD_OFF_VERSION, CLUSTER_UNDO_ROOT_DESCRIPTOR_VERSION);
	pgrd_put_le16(image + PGRD_OFF_HEADER_LEN, CLUSTER_UNDO_ROOT_DESCRIPTOR_HEADER_LEN);
	pgrd_put_le64(image + PGRD_OFF_INCARNATION, descriptor->descriptor_incarnation);
	image[PGRD_OFF_ROOT_KIND] = descriptor->root_kind;
	pgrd_put_le32(image + PGRD_OFF_OWNER_NODE, (uint32)descriptor->owner_node);
	pgrd_put_le32(image + PGRD_OFF_ROOT_ORDINAL, descriptor->root_ordinal);
	memcpy(image + PGRD_OFF_ROOT_UUID, descriptor->root_uuid, CLUSTER_UNDO_ROOT_UUID_BYTES);
	pgrd_put_le64(image + PGRD_OFF_NAMESPACE_ID, descriptor->namespace_id);
	pgrd_put_le64(image + PGRD_OFF_SYSTEM_IDENTIFIER, descriptor->system_identifier);
	pgrd_put_le32(image + CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET, pgrd_crc32c(image));
	memcpy(out, image, sizeof(image));
	return true;
}


ClusterUndoRootDescriptorState
cluster_undo_root_descriptor_decode(const uint8 bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
									uint64 expected_system_identifier,
									ClusterUndoRootDescriptorV1 *out)
{
	ClusterUndoRootDescriptorV1 decoded;

	if (bytes == NULL || out == NULL || expected_system_identifier == 0)
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
	if (pgrd_bytes_zero(bytes, CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES))
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED;
	if (pgrd_get_le32(bytes + PGRD_OFF_MAGIC) != CLUSTER_UNDO_ROOT_DESCRIPTOR_MAGIC
		|| pgrd_get_le16(bytes + PGRD_OFF_VERSION) != CLUSTER_UNDO_ROOT_DESCRIPTOR_VERSION
		|| pgrd_get_le16(bytes + PGRD_OFF_HEADER_LEN) != CLUSTER_UNDO_ROOT_DESCRIPTOR_HEADER_LEN
		|| !pgrd_bytes_zero(bytes + PGRD_OFF_RESERVED0, 3)
		|| pgrd_get_le32(bytes + PGRD_OFF_RESERVED1) != 0
		|| !pgrd_bytes_zero(bytes + PGRD_OFF_RESERVED2,
							CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET - PGRD_OFF_RESERVED2)
		|| pgrd_get_le32(bytes + CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET) != pgrd_crc32c(bytes))
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;

	memset(&decoded, 0, sizeof(decoded));
	decoded.descriptor_incarnation = pgrd_get_le64(bytes + PGRD_OFF_INCARNATION);
	decoded.root_kind = bytes[PGRD_OFF_ROOT_KIND];
	decoded.owner_node = (int32)pgrd_get_le32(bytes + PGRD_OFF_OWNER_NODE);
	decoded.root_ordinal = pgrd_get_le32(bytes + PGRD_OFF_ROOT_ORDINAL);
	memcpy(decoded.root_uuid, bytes + PGRD_OFF_ROOT_UUID, CLUSTER_UNDO_ROOT_UUID_BYTES);
	decoded.namespace_id = pgrd_get_le64(bytes + PGRD_OFF_NAMESPACE_ID);
	decoded.system_identifier = pgrd_get_le64(bytes + PGRD_OFF_SYSTEM_IDENTIFIER);
	if (!pgrd_descriptor_valid(&decoded) || decoded.system_identifier != expected_system_identifier)
		return CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD;
	*out = decoded;
	return CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID;
}


bool
cluster_undo_root_descriptor_resolve(const ClusterUndoRootDescriptorV1 *descriptor,
									 ClusterUndoPathIntent intent, uint32 owner_instance,
									 uint32 segment_id, ClusterUndoBlock0ResolvedRoot *out)
{
	ClusterUndoBlock0ResolvedRoot resolved;
	uint32 file_slot;
	uint32 first_segment;
	uint32 segment_slot;

	if (out == NULL || !pgrd_descriptor_valid(descriptor) || owner_instance == 0
		|| owner_instance > CLUSTER_MAX_NODES)
		return false;
	if (descriptor->root_kind == CLUSTER_UNDO_ROOT_KIND_SHARED) {
		if (intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
			&& intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0)
			return false;
	} else if (descriptor->root_kind == CLUSTER_UNDO_ROOT_KIND_LOCAL) {
		if (intent != CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL)
			return false;
	} else
		return false;

	first_segment = (owner_instance - 1) * 256 + 1;
	if (segment_id < first_segment || segment_id >= first_segment + 256)
		return false;
	segment_slot = segment_id - first_segment;
	if (!cluster_undo_root_file_slot(owner_instance, segment_slot, &file_slot)
		|| !cluster_undo_root_id(descriptor->namespace_id, file_slot, &resolved.root_id))
		return false;

	resolved.intent = intent;
	resolved.root_generation = descriptor->descriptor_incarnation;
	*out = resolved;
	return true;
}
