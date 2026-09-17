/*-------------------------------------------------------------------------
 *
 * cluster_replacement_request.c
 *    Spec-5.15A exact 64-byte little-endian RPLM codec.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_replacement_request.h"
#include "port/pg_crc32c.h"


#define RPLM_OFF_MAGIC 0
#define RPLM_OFF_VERSION 4
#define RPLM_OFF_PHASE 6
#define RPLM_OFF_RESERVED0 7
#define RPLM_OFF_TARGET 8
#define RPLM_OFF_RESERVED1 12
#define RPLM_OFF_BASELINE_EPOCH 16
#define RPLM_OFF_OLD_INCARNATION 24
#define RPLM_OFF_FRESH_INCARNATION 32
#define RPLM_OFF_REQUEST_NONCE 40
#define RPLM_OFF_GRAMMAR_FINGERPRINT 48
#define RPLM_OFF_CRC32C 56
#define RPLM_OFF_RESERVED2 60


static void
rplm_put_le16(uint8 *out, uint16 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
}


static void
rplm_put_le32(uint8 *out, uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		out[i] = (uint8)(value >> (i * 8));
}


static void
rplm_put_le64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> (i * 8));
}


static uint16
rplm_get_le16(const uint8 *in)
{
	return (uint16)in[0] | ((uint16)in[1] << 8);
}


static uint32
rplm_get_le32(const uint8 *in)
{
	uint32 value = 0;
	int i;

	for (i = 0; i < 4; i++)
		value |= (uint32)in[i] << (i * 8);
	return value;
}


static uint64
rplm_get_le64(const uint8 *in)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= (uint64)in[i] << (i * 8);
	return value;
}


static uint32
rplm_crc32c(const uint8 bytes[CLUSTER_REPLACEMENT_MARKER_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, RPLM_OFF_CRC32C);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


static bool
rplm_marker_valid(const ClusterReplacementRequestMarker *marker, int32 expected_target_node)
{
	return marker != NULL && marker->magic == CLUSTER_REPLACEMENT_MARKER_MAGIC
		   && marker->version == CLUSTER_REPLACEMENT_MARKER_VERSION
		   && marker->phase == CLUSTER_REPLACEMENT_MARKER_PHASE_REQUESTED && marker->reserved0 == 0
		   && marker->reserved1 == 0 && marker->reserved2 == 0 && marker->target_node_id >= 0
		   && marker->target_node_id < CLUSTER_MAX_NODES
		   && (expected_target_node < 0 || marker->target_node_id == expected_target_node)
		   && marker->old_admitted_incarnation != 0
		   && marker->fresh_incarnation > marker->old_admitted_incarnation
		   && marker->request_nonce != 0
		   && marker->grammar_fingerprint == CLUSTER_REPLACEMENT_EPISODE_GRAMMAR_FINGERPRINT;
}


bool
cluster_replacement_request_encode(const ClusterReplacementRequestMarker *marker,
								   uint8 out[CLUSTER_REPLACEMENT_MARKER_BYTES])
{
	uint8 image[CLUSTER_REPLACEMENT_MARKER_BYTES];

	if (out == NULL || !rplm_marker_valid(marker, -1))
		return false;
	memset(image, 0, sizeof(image));
	rplm_put_le32(image + RPLM_OFF_MAGIC, marker->magic);
	rplm_put_le16(image + RPLM_OFF_VERSION, marker->version);
	image[RPLM_OFF_PHASE] = marker->phase;
	rplm_put_le32(image + RPLM_OFF_TARGET, (uint32)marker->target_node_id);
	rplm_put_le64(image + RPLM_OFF_BASELINE_EPOCH, marker->baseline_epoch);
	rplm_put_le64(image + RPLM_OFF_OLD_INCARNATION, marker->old_admitted_incarnation);
	rplm_put_le64(image + RPLM_OFF_FRESH_INCARNATION, marker->fresh_incarnation);
	rplm_put_le64(image + RPLM_OFF_REQUEST_NONCE, marker->request_nonce);
	rplm_put_le64(image + RPLM_OFF_GRAMMAR_FINGERPRINT, marker->grammar_fingerprint);
	rplm_put_le32(image + RPLM_OFF_CRC32C, rplm_crc32c(image));
	memcpy(out, image, sizeof(image));
	return true;
}


bool
cluster_replacement_request_decode(const uint8 bytes[CLUSTER_REPLACEMENT_MARKER_BYTES],
								   int32 expected_target_node, ClusterReplacementRequestMarker *out)
{
	ClusterReplacementRequestMarker decoded;

	if (bytes == NULL || out == NULL || expected_target_node < 0
		|| expected_target_node >= CLUSTER_MAX_NODES || bytes[RPLM_OFF_RESERVED0] != 0
		|| rplm_get_le32(bytes + RPLM_OFF_RESERVED1) != 0
		|| rplm_get_le32(bytes + RPLM_OFF_RESERVED2) != 0
		|| rplm_get_le32(bytes + RPLM_OFF_CRC32C) != rplm_crc32c(bytes))
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.magic = rplm_get_le32(bytes + RPLM_OFF_MAGIC);
	decoded.version = rplm_get_le16(bytes + RPLM_OFF_VERSION);
	decoded.phase = bytes[RPLM_OFF_PHASE];
	decoded.target_node_id = (int32)rplm_get_le32(bytes + RPLM_OFF_TARGET);
	decoded.baseline_epoch = rplm_get_le64(bytes + RPLM_OFF_BASELINE_EPOCH);
	decoded.old_admitted_incarnation = rplm_get_le64(bytes + RPLM_OFF_OLD_INCARNATION);
	decoded.fresh_incarnation = rplm_get_le64(bytes + RPLM_OFF_FRESH_INCARNATION);
	decoded.request_nonce = rplm_get_le64(bytes + RPLM_OFF_REQUEST_NONCE);
	decoded.grammar_fingerprint = rplm_get_le64(bytes + RPLM_OFF_GRAMMAR_FINGERPRINT);
	decoded.crc32c = rplm_get_le32(bytes + RPLM_OFF_CRC32C);
	if (!rplm_marker_valid(&decoded, expected_target_node))
		return false;
	*out = decoded;
	return true;
}


bool
cluster_replacement_request_pack(uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
								 const ClusterReplacementRequestMarker *marker)
{
	uint8 image[CLUSTER_REPLACEMENT_MARKER_BYTES];

	if (reserved1 == NULL || !cluster_replacement_request_encode(marker, image))
		return false;
	memcpy(reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET, image, sizeof(image));
	return true;
}


bool
cluster_replacement_request_unpack(
	const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES], int32 expected_target_node,
	ClusterReplacementRequestMarker *out)
{
	return reserved1 != NULL
		   && cluster_replacement_request_decode(
			   reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET, expected_target_node, out);
}


void
cluster_replacement_request_clear(uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES])
{
	if (reserved1 != NULL)
		memset(reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET, 0,
			   CLUSTER_REPLACEMENT_MARKER_BYTES);
}


bool
cluster_replacement_request_is_clear(
	const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES])
{
	int i;

	if (reserved1 == NULL)
		return false;
	for (i = 0; i < CLUSTER_REPLACEMENT_MARKER_BYTES; i++) {
		if (reserved1[CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET + i] != 0)
			return false;
	}
	return true;
}


ClusterReplacementRequestSlotState
cluster_replacement_request_slot_state(
	uint64 flags, const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
	int32 expected_target_node, uint64 expected_slot_incarnation,
	ClusterReplacementRequestMarker *out)
{
	ClusterReplacementRequestMarker decoded;
	bool requested = (flags & CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED) != 0;

	if (reserved1 == NULL || expected_slot_incarnation == 0)
		return CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	if (!requested)
		return cluster_replacement_request_is_clear(reserved1)
				   ? CLUSTER_REPLACEMENT_REQUEST_SLOT_CLEAR
				   : CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	if (!cluster_replacement_request_unpack(reserved1, expected_target_node, &decoded)
		|| decoded.fresh_incarnation != expected_slot_incarnation)
		return CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	if (out != NULL)
		*out = decoded;
	return CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID;
}


ClusterReplacementRequestSlotState
cluster_replacement_request_preserve_per_disk(
	uint64 prior_flags, const uint8 prior_reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
	int32 expected_target_node, uint64 expected_slot_incarnation, uint64 *new_flags,
	uint8 new_reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES])
{
	ClusterReplacementRequestSlotState state;

	if (new_flags == NULL || new_reserved1 == NULL)
		return CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD;
	state = cluster_replacement_request_slot_state(
		prior_flags, prior_reserved1, expected_target_node, expected_slot_incarnation, NULL);
	if (state != CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID)
		return state;

	memcpy(new_reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
		   prior_reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
		   CLUSTER_REPLACEMENT_MARKER_BYTES);
	*new_flags |= CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED;
	return CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID;
}
