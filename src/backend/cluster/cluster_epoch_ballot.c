/*-------------------------------------------------------------------------
 *
 * cluster_epoch_ballot.c
 *    Common next-epoch ballot pure codecs (spec-5.15A §2.1A/§2.1A.1).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch_ballot.h"
#include "port/pg_crc32c.h"


#define EPOCH_BALLOT_OFF_PROMISED 48
#define EPOCH_BALLOT_OFF_ACCEPTED 80
#define EPOCH_BALLOT_OFF_ACCEPTED_VALUE 112
#define EPOCH_BALLOT_OFF_SETTLED 240
#define EPOCH_BALLOT_OFF_SETTLED_VALUE 272
#define EPOCH_BALLOT_OFF_RESERVED 400
#define EPOCH_BALLOT_OFF_CRC 508


static uint16
epoch_ballot_get_le16(const uint8 *in)
{
	return (uint16)in[0] | ((uint16)in[1] << 8);
}


static uint32
epoch_ballot_get_le32(const uint8 *in)
{
	return (uint32)in[0] | ((uint32)in[1] << 8) | ((uint32)in[2] << 16) | ((uint32)in[3] << 24);
}


static uint64
epoch_ballot_get_le64(const uint8 *in)
{
	return (uint64)epoch_ballot_get_le32(in) | ((uint64)epoch_ballot_get_le32(in + 4) << 32);
}


static void
epoch_ballot_put_le16(uint8 *out, uint16 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
}


static void
epoch_ballot_put_le32(uint8 *out, uint32 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
	out[2] = (uint8)(value >> 16);
	out[3] = (uint8)(value >> 24);
}


static void
epoch_ballot_put_le64(uint8 *out, uint64 value)
{
	epoch_ballot_put_le32(out, (uint32)value);
	epoch_ballot_put_le32(out + 4, (uint32)(value >> 32));
}


static bool
epoch_ballot_node_valid(int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES;
}


static bool
epoch_ballot_bytes_zero(const void *bytes, Size size)
{
	const uint8 *p = (const uint8 *)bytes;
	Size i;

	for (i = 0; i < size; i++) {
		if (p[i] != 0)
			return false;
	}
	return true;
}


static bool
epoch_ballot_bitmap_empty(const uint8 bitmap[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES])
{
	return epoch_ballot_bytes_zero(bitmap, CLUSTER_EPOCH_BALLOT_BITMAP_BYTES);
}


static int
epoch_ballot_bitmap_count(const uint8 bitmap[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES])
{
	int count = 0;
	int i;

	for (i = 0; i < CLUSTER_EPOCH_BALLOT_BITMAP_BYTES; i++) {
		uint8 value = bitmap[i];

		while (value != 0) {
			count += value & 1u;
			value >>= 1;
		}
	}
	return count;
}


static bool
epoch_ballot_bitmap_has(const uint8 bitmap[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES], int32 node_id)
{
	return epoch_ballot_node_valid(node_id)
		   && (bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}


static bool
epoch_ballot_disk_count_valid(uint32 count)
{
	return count == 1 || count == 3 || count == 5 || count == 7;
}


static uint32
epoch_ballot_lane_crc(const uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, EPOCH_BALLOT_OFF_CRC);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


bool
cluster_epoch_ballot_id_is_valid(const ClusterEpochBallotId *ballot)
{
	return ballot != NULL && ballot->counter != 0
		   && epoch_ballot_node_valid(ballot->proposer_node_id) && ballot->reserved == 0
		   && ballot->proposer_admitted_incarnation != 0 && ballot->nonce != 0;
}


bool
cluster_epoch_ballot_id_encode(const ClusterEpochBallotId *ballot,
							   uint8 out[CLUSTER_EPOCH_BALLOT_ID_BYTES])
{
	uint8 image[CLUSTER_EPOCH_BALLOT_ID_BYTES];

	if (out == NULL || !cluster_epoch_ballot_id_is_valid(ballot))
		return false;
	memset(image, 0, sizeof(image));
	epoch_ballot_put_le64(image + 0, ballot->counter);
	epoch_ballot_put_le32(image + 8, (uint32)ballot->proposer_node_id);
	epoch_ballot_put_le64(image + 16, ballot->proposer_admitted_incarnation);
	epoch_ballot_put_le64(image + 24, ballot->nonce);
	memcpy(out, image, sizeof(image));
	return true;
}


bool
cluster_epoch_ballot_id_decode(const uint8 bytes[CLUSTER_EPOCH_BALLOT_ID_BYTES],
							   ClusterEpochBallotId *out)
{
	ClusterEpochBallotId decoded;

	if (bytes == NULL || out == NULL)
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.counter = epoch_ballot_get_le64(bytes + 0);
	decoded.proposer_node_id = (int32)epoch_ballot_get_le32(bytes + 8);
	decoded.reserved = epoch_ballot_get_le32(bytes + 12);
	decoded.proposer_admitted_incarnation = epoch_ballot_get_le64(bytes + 16);
	decoded.nonce = epoch_ballot_get_le64(bytes + 24);
	if (!cluster_epoch_ballot_id_is_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}


int
cluster_epoch_ballot_id_compare(const ClusterEpochBallotId *a, const ClusterEpochBallotId *b)
{
#define EPOCH_BALLOT_CMP_FIELD(field)                                                              \
	do {                                                                                           \
		if (a->field < b->field)                                                                   \
			return -1;                                                                             \
		if (a->field > b->field)                                                                   \
			return 1;                                                                              \
	} while (0)

	Assert(a != NULL);
	Assert(b != NULL);
	EPOCH_BALLOT_CMP_FIELD(counter);
	EPOCH_BALLOT_CMP_FIELD(proposer_node_id);
	EPOCH_BALLOT_CMP_FIELD(proposer_admitted_incarnation);
	EPOCH_BALLOT_CMP_FIELD(nonce);
	return 0;

#undef EPOCH_BALLOT_CMP_FIELD
}


bool
cluster_epoch_ballot_next_counter(uint64 current, uint64 *next)
{
	if (next == NULL || current == UINT64_MAX)
		return false;
	*next = current + 1;
	return true;
}


static bool
epoch_authority_transition_valid(uint8 transition)
{
	return transition >= CLUSTER_EPOCH_AUTHORITY_GENESIS
		   && transition <= CLUSTER_EPOCH_AUTHORITY_ABORT_CLOSED;
}


static bool
epoch_authority_event_kind_valid(uint8 event_kind)
{
	return event_kind <= CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
}


static bool
epoch_authority_target_matches_subject(const ClusterEpochAuthorityValue *value)
{
	int subjects = epoch_ballot_bitmap_count(value->event_subject_bitmap);

	if (subjects == 1)
		return epoch_ballot_bitmap_has(value->event_subject_bitmap, value->target_node_id);
	if (subjects > 1)
		return value->target_node_id == -1;
	return false;
}


static bool
epoch_authority_event_identity_valid(const ClusterEpochAuthorityValue *value)
{
	switch (value->event_kind) {
	case CLUSTER_EPOCH_EVENT_FAIL_STOP:
		return true;
	case CLUSTER_EPOCH_EVENT_CLEAN_LEAVE:
		return value->old_incarnation == 0 && value->fresh_incarnation == 0;
	case CLUSTER_EPOCH_EVENT_ORDINARY_JOIN:
		return value->fresh_incarnation > value->old_incarnation;
	case CLUSTER_EPOCH_EVENT_NODE_REMOVE:
		return value->old_incarnation != 0 && value->fresh_incarnation == 0;
	case CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT:
		return value->old_incarnation != 0 && value->fresh_incarnation > value->old_incarnation;
	default:
		return false;
	}
}


bool
cluster_epoch_authority_value_is_valid(const ClusterEpochAuthorityValue *value,
									   uint64 expected_grammar_fingerprint)
{
	bool genesis;

	if (value == NULL || expected_grammar_fingerprint != CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT
		|| value->value_version != CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION
		|| !epoch_authority_transition_valid(value->transition)
		|| !epoch_authority_event_kind_valid(value->event_kind)
		|| !epoch_ballot_node_valid(value->request_origin_node)
		|| (value->target_node_id != -1 && !epoch_ballot_node_valid(value->target_node_id))
		|| value->reserved0 != 0 || value->authority_generation == 0
		|| epoch_ballot_bitmap_empty(value->authority_member_bitmap)
		|| value->grammar_fingerprint != expected_grammar_fingerprint
		|| !epoch_ballot_bytes_zero(value->reserved1, sizeof(value->reserved1)))
		return false;

	genesis = value->transition == CLUSTER_EPOCH_AUTHORITY_GENESIS;
	if (genesis != (value->event_kind == CLUSTER_EPOCH_EVENT_GENESIS))
		return false;
	if (genesis) {
		return value->authority_generation == 1 && value->baseline_epoch == value->reserved_epoch
			   && value->target_node_id == 0 && value->old_incarnation == 0
			   && value->fresh_incarnation == 0 && value->request_nonce == 0
			   && epoch_ballot_bitmap_empty(value->event_subject_bitmap)
			   && epoch_ballot_bytes_zero(value->predecessor_digest,
										  sizeof(value->predecessor_digest));
	}

	if (value->request_nonce == 0 || value->baseline_epoch == UINT64_MAX
		|| value->reserved_epoch != value->baseline_epoch + 1
		|| !epoch_authority_target_matches_subject(value)
		|| !epoch_authority_event_identity_valid(value))
		return false;
	return true;
}


static void
epoch_authority_value_pack(const ClusterEpochAuthorityValue *value,
						   uint8 image[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES])
{
	memset(image, 0, CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES);
	epoch_ballot_put_le16(image + 0, value->value_version);
	image[2] = value->transition;
	image[3] = value->event_kind;
	epoch_ballot_put_le32(image + 4, (uint32)value->request_origin_node);
	epoch_ballot_put_le32(image + 8, (uint32)value->target_node_id);
	epoch_ballot_put_le64(image + 16, value->authority_generation);
	epoch_ballot_put_le64(image + 24, value->baseline_epoch);
	epoch_ballot_put_le64(image + 32, value->reserved_epoch);
	epoch_ballot_put_le64(image + 40, value->old_incarnation);
	epoch_ballot_put_le64(image + 48, value->fresh_incarnation);
	epoch_ballot_put_le64(image + 56, value->request_nonce);
	memcpy(image + 64, value->authority_member_bitmap, CLUSTER_EPOCH_BALLOT_BITMAP_BYTES);
	memcpy(image + 80, value->event_subject_bitmap, CLUSTER_EPOCH_BALLOT_BITMAP_BYTES);
	epoch_ballot_put_le64(image + 96, value->grammar_fingerprint);
	memcpy(image + 104, value->predecessor_digest, CLUSTER_EPOCH_BALLOT_DIGEST_BYTES);
}


bool
cluster_epoch_authority_value_encode(const ClusterEpochAuthorityValue *value,
									 uint64 expected_grammar_fingerprint,
									 uint8 out[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES])
{
	uint8 image[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];

	if (out == NULL || !cluster_epoch_authority_value_is_valid(value, expected_grammar_fingerprint))
		return false;
	epoch_authority_value_pack(value, image);
	memcpy(out, image, sizeof(image));
	return true;
}


bool
cluster_epoch_authority_value_decode(const uint8 bytes[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES],
									 uint64 expected_grammar_fingerprint,
									 ClusterEpochAuthorityValue *out)
{
	ClusterEpochAuthorityValue decoded;

	if (bytes == NULL || out == NULL)
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.value_version = epoch_ballot_get_le16(bytes + 0);
	decoded.transition = bytes[2];
	decoded.event_kind = bytes[3];
	decoded.request_origin_node = (int32)epoch_ballot_get_le32(bytes + 4);
	decoded.target_node_id = (int32)epoch_ballot_get_le32(bytes + 8);
	decoded.reserved0 = epoch_ballot_get_le32(bytes + 12);
	decoded.authority_generation = epoch_ballot_get_le64(bytes + 16);
	decoded.baseline_epoch = epoch_ballot_get_le64(bytes + 24);
	decoded.reserved_epoch = epoch_ballot_get_le64(bytes + 32);
	decoded.old_incarnation = epoch_ballot_get_le64(bytes + 40);
	decoded.fresh_incarnation = epoch_ballot_get_le64(bytes + 48);
	decoded.request_nonce = epoch_ballot_get_le64(bytes + 56);
	memcpy(decoded.authority_member_bitmap, bytes + 64, CLUSTER_EPOCH_BALLOT_BITMAP_BYTES);
	memcpy(decoded.event_subject_bitmap, bytes + 80, CLUSTER_EPOCH_BALLOT_BITMAP_BYTES);
	decoded.grammar_fingerprint = epoch_ballot_get_le64(bytes + 96);
	memcpy(decoded.predecessor_digest, bytes + 104, CLUSTER_EPOCH_BALLOT_DIGEST_BYTES);
	memcpy(decoded.reserved1, bytes + 120, sizeof(decoded.reserved1));
	if (!cluster_epoch_authority_value_is_valid(&decoded, expected_grammar_fingerprint))
		return false;
	*out = decoded;
	return true;
}


static bool
epoch_ballot_id_zero(const ClusterEpochBallotId *ballot)
{
	return epoch_ballot_bytes_zero(ballot, sizeof(*ballot));
}


static bool
epoch_authority_value_zero(const ClusterEpochAuthorityValue *value)
{
	return epoch_ballot_bytes_zero(value, sizeof(*value));
}


static bool
epoch_ballot_pair_valid(const ClusterEpochBallotId *ballot, const ClusterEpochAuthorityValue *value,
						uint64 expected_grammar_fingerprint)
{
	bool ballot_zero = epoch_ballot_id_zero(ballot);
	bool value_zero = epoch_authority_value_zero(value);

	if (ballot_zero != value_zero)
		return false;
	if (ballot_zero)
		return true;
	return cluster_epoch_ballot_id_is_valid(ballot)
		   && cluster_epoch_authority_value_is_valid(value, expected_grammar_fingerprint);
}


static bool
epoch_ballot_same_id(const ClusterEpochBallotId *a, const ClusterEpochBallotId *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}


static bool
epoch_authority_same_value(const ClusterEpochAuthorityValue *a, const ClusterEpochAuthorityValue *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}


static bool
epoch_ballot_lane_history_valid(const ClusterEpochBallotLane *lane,
								uint64 expected_grammar_fingerprint)
{
	bool accepted_zero = epoch_ballot_id_zero(&lane->accepted_ballot);
	bool settled_zero = epoch_ballot_id_zero(&lane->settled_ballot);

	if (!cluster_epoch_ballot_id_is_valid(&lane->promised_ballot)
		|| lane->promised_ballot.proposer_node_id != lane->proposer_node_id
		|| lane->promised_ballot.proposer_admitted_incarnation
			   != lane->proposer_admitted_incarnation
		|| !epoch_ballot_pair_valid(&lane->accepted_ballot, &lane->accepted_value,
									expected_grammar_fingerprint)
		|| !epoch_ballot_pair_valid(&lane->settled_ballot, &lane->settled_value,
									expected_grammar_fingerprint))
		return false;
	if (!accepted_zero && lane->accepted_ballot.proposer_node_id != lane->proposer_node_id)
		return false;
	if (!settled_zero && lane->settled_ballot.proposer_node_id != lane->proposer_node_id)
		return false;
	if (!accepted_zero
		&& cluster_epoch_ballot_id_compare(&lane->promised_ballot, &lane->accepted_ballot) < 0)
		return false;
	if (!accepted_zero && !settled_zero
		&& cluster_epoch_ballot_id_compare(&lane->accepted_ballot, &lane->settled_ballot) < 0)
		return false;
	if (!accepted_zero && !settled_zero) {
		if (epoch_ballot_same_id(&lane->accepted_ballot, &lane->settled_ballot)
			&& !epoch_authority_same_value(&lane->accepted_value, &lane->settled_value))
			return false;
		if (lane->accepted_value.authority_generation < lane->settled_value.authority_generation)
			return false;
		if (lane->accepted_value.authority_generation == lane->settled_value.authority_generation
			&& !epoch_authority_same_value(&lane->accepted_value, &lane->settled_value))
			return false;
	}

	switch (lane->last_write_phase) {
	case CLUSTER_EPOCH_BALLOT_PHASE_PROMISED:
		return true;
	case CLUSTER_EPOCH_BALLOT_PHASE_ACCEPTED:
		return !accepted_zero
			   && epoch_ballot_same_id(&lane->promised_ballot, &lane->accepted_ballot);
	case CLUSTER_EPOCH_BALLOT_PHASE_SETTLED:
		return !accepted_zero && !settled_zero
			   && epoch_ballot_same_id(&lane->promised_ballot, &lane->accepted_ballot)
			   && epoch_ballot_same_id(&lane->accepted_ballot, &lane->settled_ballot)
			   && epoch_authority_same_value(&lane->accepted_value, &lane->settled_value);
	default:
		return false;
	}
}


bool
cluster_epoch_ballot_lane_is_valid(const ClusterEpochBallotLane *lane,
								   int32 expected_proposer_node_id,
								   uint32 expected_configured_disk_count,
								   uint64 expected_proposer_admitted_incarnation,
								   uint64 expected_system_identifier,
								   uint64 expected_grammar_fingerprint)
{
	if (lane == NULL || !epoch_ballot_node_valid(expected_proposer_node_id)
		|| !epoch_ballot_disk_count_valid(expected_configured_disk_count)
		|| expected_proposer_admitted_incarnation == 0 || expected_system_identifier == 0
		|| expected_grammar_fingerprint != CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT
		|| lane->magic != CLUSTER_EPOCH_BALLOT_MAGIC
		|| lane->version != CLUSTER_EPOCH_BALLOT_VERSION
		|| lane->last_write_phase < CLUSTER_EPOCH_BALLOT_PHASE_PROMISED
		|| lane->last_write_phase > CLUSTER_EPOCH_BALLOT_PHASE_SETTLED || lane->flags != 0
		|| lane->proposer_node_id != expected_proposer_node_id
		|| lane->configured_disk_count != expected_configured_disk_count
		|| lane->proposer_admitted_incarnation != expected_proposer_admitted_incarnation
		|| lane->lane_generation == 0 || lane->system_identifier != expected_system_identifier
		|| lane->grammar_fingerprint != expected_grammar_fingerprint
		|| !epoch_ballot_bytes_zero(lane->reserved, sizeof(lane->reserved)))
		return false;
	return epoch_ballot_lane_history_valid(lane, expected_grammar_fingerprint);
}


static bool
epoch_ballot_id_pack(const ClusterEpochBallotId *ballot, uint8 image[CLUSTER_EPOCH_BALLOT_ID_BYTES])
{
	return cluster_epoch_ballot_id_encode(ballot, image);
}


bool
cluster_epoch_ballot_lane_encode(const ClusterEpochBallotLane *lane,
								 int32 expected_proposer_node_id,
								 uint32 expected_configured_disk_count,
								 uint64 expected_proposer_admitted_incarnation,
								 uint64 expected_system_identifier,
								 uint64 expected_grammar_fingerprint,
								 uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES])
{
	uint8 image[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	if (out == NULL
		|| !cluster_epoch_ballot_lane_is_valid(
			lane, expected_proposer_node_id, expected_configured_disk_count,
			expected_proposer_admitted_incarnation, expected_system_identifier,
			expected_grammar_fingerprint))
		return false;
	memset(image, 0, sizeof(image));
	epoch_ballot_put_le32(image + 0, lane->magic);
	epoch_ballot_put_le16(image + 4, lane->version);
	image[6] = lane->last_write_phase;
	epoch_ballot_put_le32(image + 8, (uint32)lane->proposer_node_id);
	epoch_ballot_put_le32(image + 12, lane->configured_disk_count);
	epoch_ballot_put_le64(image + 16, lane->proposer_admitted_incarnation);
	epoch_ballot_put_le64(image + 24, lane->lane_generation);
	epoch_ballot_put_le64(image + 32, lane->system_identifier);
	epoch_ballot_put_le64(image + 40, lane->grammar_fingerprint);
	if (!epoch_ballot_id_pack(&lane->promised_ballot, image + EPOCH_BALLOT_OFF_PROMISED))
		return false;
	if (!epoch_ballot_id_zero(&lane->accepted_ballot)) {
		if (!epoch_ballot_id_pack(&lane->accepted_ballot, image + EPOCH_BALLOT_OFF_ACCEPTED))
			return false;
		epoch_authority_value_pack(&lane->accepted_value, image + EPOCH_BALLOT_OFF_ACCEPTED_VALUE);
	}
	if (!epoch_ballot_id_zero(&lane->settled_ballot)) {
		if (!epoch_ballot_id_pack(&lane->settled_ballot, image + EPOCH_BALLOT_OFF_SETTLED))
			return false;
		epoch_authority_value_pack(&lane->settled_value, image + EPOCH_BALLOT_OFF_SETTLED_VALUE);
	}
	epoch_ballot_put_le32(image + EPOCH_BALLOT_OFF_CRC, epoch_ballot_lane_crc(image));
	memcpy(out, image, sizeof(image));
	return true;
}


static bool
epoch_ballot_id_decode_pair(const uint8 bytes[CLUSTER_EPOCH_BALLOT_ID_BYTES],
							ClusterEpochBallotId *out)
{
	if (epoch_ballot_bytes_zero(bytes, CLUSTER_EPOCH_BALLOT_ID_BYTES)) {
		memset(out, 0, sizeof(*out));
		return true;
	}
	return cluster_epoch_ballot_id_decode(bytes, out);
}


static bool
epoch_authority_value_decode_pair(const uint8 bytes[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES],
								  uint64 expected_grammar_fingerprint,
								  ClusterEpochAuthorityValue *out)
{
	if (epoch_ballot_bytes_zero(bytes, CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES)) {
		memset(out, 0, sizeof(*out));
		return true;
	}
	return cluster_epoch_authority_value_decode(bytes, expected_grammar_fingerprint, out);
}


bool
cluster_epoch_ballot_lane_decode(const uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES],
								 int32 expected_proposer_node_id,
								 uint32 expected_configured_disk_count,
								 uint64 expected_proposer_admitted_incarnation,
								 uint64 expected_system_identifier,
								 uint64 expected_grammar_fingerprint, ClusterEpochBallotLane *out)
{
	ClusterEpochBallotLane decoded;

	if (bytes == NULL || out == NULL
		|| epoch_ballot_lane_crc(bytes) != epoch_ballot_get_le32(bytes + EPOCH_BALLOT_OFF_CRC))
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.magic = epoch_ballot_get_le32(bytes + 0);
	decoded.version = epoch_ballot_get_le16(bytes + 4);
	decoded.last_write_phase = bytes[6];
	decoded.flags = bytes[7];
	decoded.proposer_node_id = (int32)epoch_ballot_get_le32(bytes + 8);
	decoded.configured_disk_count = epoch_ballot_get_le32(bytes + 12);
	decoded.proposer_admitted_incarnation = epoch_ballot_get_le64(bytes + 16);
	decoded.lane_generation = epoch_ballot_get_le64(bytes + 24);
	decoded.system_identifier = epoch_ballot_get_le64(bytes + 32);
	decoded.grammar_fingerprint = epoch_ballot_get_le64(bytes + 40);
	if (!epoch_ballot_id_decode_pair(bytes + EPOCH_BALLOT_OFF_PROMISED, &decoded.promised_ballot)
		|| !epoch_ballot_id_decode_pair(bytes + EPOCH_BALLOT_OFF_ACCEPTED, &decoded.accepted_ballot)
		|| !epoch_authority_value_decode_pair(bytes + EPOCH_BALLOT_OFF_ACCEPTED_VALUE,
											  expected_grammar_fingerprint, &decoded.accepted_value)
		|| !epoch_ballot_id_decode_pair(bytes + EPOCH_BALLOT_OFF_SETTLED, &decoded.settled_ballot)
		|| !epoch_authority_value_decode_pair(bytes + EPOCH_BALLOT_OFF_SETTLED_VALUE,
											  expected_grammar_fingerprint, &decoded.settled_value))
		return false;
	memcpy(decoded.reserved, bytes + EPOCH_BALLOT_OFF_RESERVED, sizeof(decoded.reserved));
	decoded.crc32c = epoch_ballot_get_le32(bytes + EPOCH_BALLOT_OFF_CRC);
	if (!cluster_epoch_ballot_lane_is_valid(
			&decoded, expected_proposer_node_id, expected_configured_disk_count,
			expected_proposer_admitted_incarnation, expected_system_identifier,
			expected_grammar_fingerprint))
		return false;
	*out = decoded;
	return true;
}
