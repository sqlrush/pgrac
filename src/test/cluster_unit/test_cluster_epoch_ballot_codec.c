/*-------------------------------------------------------------------------
 *
 * test_cluster_epoch_ballot_codec.c
 *    Spec-5.15A common epoch-ballot pure codec tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_epoch_ballot.h"
#include "port/pg_crc32c.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}


static ClusterEpochBallotId
make_ballot(void)
{
	ClusterEpochBallotId ballot;

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(0x0102030405060708);
	ballot.proposer_node_id = 7;
	ballot.proposer_admitted_incarnation = UINT64_C(0x1112131415161718);
	ballot.nonce = UINT64_C(0x2122232425262728);
	return ballot;
}


static ClusterEpochAuthorityValue
make_value(void)
{
	ClusterEpochAuthorityValue value;
	int i;

	memset(&value, 0, sizeof(value));
	value.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	value.transition = CLUSTER_EPOCH_AUTHORITY_RESERVE;
	value.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	value.request_origin_node = 2;
	value.target_node_id = 3;
	value.authority_generation = UINT64_C(0x3132333435363738);
	value.baseline_epoch = UINT64_C(0x4142434445464748);
	value.reserved_epoch = UINT64_C(0x4142434445464749);
	value.old_incarnation = UINT64_C(0x5152535455565758);
	value.fresh_incarnation = UINT64_C(0x6162636465666768);
	value.request_nonce = UINT64_C(0x7172737475767778);
	value.authority_member_bitmap[0] = UINT8_C(0x0f);
	value.event_subject_bitmap[0] = UINT8_C(0x08);
	value.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	for (i = 0; i < CLUSTER_EPOCH_BALLOT_DIGEST_BYTES; i++)
		value.predecessor_digest[i] = (uint8)(0x80 + i);
	return value;
}


static ClusterEpochBallotLane
make_lane(void)
{
	ClusterEpochBallotLane lane;

	memset(&lane, 0, sizeof(lane));
	lane.magic = CLUSTER_EPOCH_BALLOT_MAGIC;
	lane.version = CLUSTER_EPOCH_BALLOT_VERSION;
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_SETTLED;
	lane.proposer_node_id = 7;
	lane.configured_disk_count = 3;
	lane.proposer_admitted_incarnation = UINT64_C(0x1112131415161718);
	lane.lane_generation = UINT64_C(0x0a0b0c0d0e0f1011);
	lane.system_identifier = UINT64_C(0x3132333435363738);
	lane.grammar_fingerprint = CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT;
	lane.promised_ballot = make_ballot();
	lane.accepted_ballot = lane.promised_ballot;
	lane.accepted_value = make_value();
	lane.settled_ballot = lane.accepted_ballot;
	lane.settled_value = lane.accepted_value;
	return lane;
}


static void
expected_ballot_bytes(uint8 out[CLUSTER_EPOCH_BALLOT_ID_BYTES])
{
	static const uint8 bytes[CLUSTER_EPOCH_BALLOT_ID_BYTES] = {
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x07, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13,
		0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21,
	};

	memcpy(out, bytes, sizeof(bytes));
}


static void
expected_value_bytes(uint8 out[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES])
{
	static const uint8 bytes[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES] = {
		0x01, 0x00, 0x02, 0x05, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x48, 0x47, 0x46, 0x45, 0x44, 0x43,
		0x42, 0x41, 0x49, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, 0x58, 0x57, 0x56, 0x55, 0x54,
		0x53, 0x52, 0x51, 0x68, 0x67, 0x66, 0x65, 0x64, 0x63, 0x62, 0x61, 0x78, 0x77, 0x76, 0x75,
		0x74, 0x73, 0x72, 0x71, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe4, 0x05, 0x89, 0x42, 0x5b, 0xae, 0x0d, 0x8e, 0x80,
		0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	};

	memcpy(out, bytes, sizeof(bytes));
}


static void
expected_lane_bytes(uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES])
{
	static const uint8 header[48] = {
		0x45, 0x50, 0x42, 0x4c, 0x01, 0x00, 0x03, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
		0x11, 0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x38, 0x37, 0x36, 0x35,
		0x34, 0x33, 0x32, 0x31, 0xe4, 0x05, 0x89, 0x42, 0x5b, 0xae, 0x0d, 0x8e,
	};
	uint8 ballot[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 value[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];

	memset(out, 0, CLUSTER_EPOCH_BALLOT_LANE_BYTES);
	memcpy(out, header, sizeof(header));
	expected_ballot_bytes(ballot);
	expected_value_bytes(value);
	memcpy(out + 48, ballot, sizeof(ballot));
	memcpy(out + 80, ballot, sizeof(ballot));
	memcpy(out + 112, value, sizeof(value));
	memcpy(out + 240, ballot, sizeof(ballot));
	memcpy(out + 272, value, sizeof(value));
	out[508] = UINT8_C(0xfe);
	out[509] = UINT8_C(0xf5);
	out[510] = UINT8_C(0x6e);
	out[511] = UINT8_C(0x88);
}


static void
test_lane_recrc(uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 508);
	FIN_CRC32C(crc);
	bytes[508] = (uint8)crc;
	bytes[509] = (uint8)(crc >> 8);
	bytes[510] = (uint8)(crc >> 16);
	bytes[511] = (uint8)(crc >> 24);
}


static bool
encode_lane(const ClusterEpochBallotLane *lane, uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES])
{
	return cluster_epoch_ballot_lane_encode(lane, 7, 3, UINT64_C(0x1112131415161718),
											UINT64_C(0x3132333435363738),
											CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, out);
}


static bool
decode_lane(const uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES], ClusterEpochBallotLane *out)
{
	return cluster_epoch_ballot_lane_decode(bytes, 7, 3, UINT64_C(0x1112131415161718),
											UINT64_C(0x3132333435363738),
											CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, out);
}


UT_TEST(test_exact_host_layouts)
{
	UT_ASSERT_EQ((int)sizeof(ClusterEpochBallotId), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotId, counter), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotId, proposer_node_id), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotId, reserved), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotId, proposer_admitted_incarnation), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotId, nonce), 24);
	UT_ASSERT_EQ((int)sizeof(ClusterEpochAuthorityValue), 128);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochAuthorityValue, authority_generation), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochAuthorityValue, authority_member_bitmap), 64);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochAuthorityValue, predecessor_digest), 104);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochAuthorityValue, reserved1), 120);
	UT_ASSERT_EQ((int)sizeof(ClusterEpochBallotLane), 512);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, promised_ballot), 48);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, accepted_ballot), 80);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, accepted_value), 112);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, settled_ballot), 240);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, settled_value), 272);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, reserved), 400);
	UT_ASSERT_EQ((int)offsetof(ClusterEpochBallotLane, crc32c), 508);
}


UT_TEST(test_ballot_id_exact_little_endian_golden_and_roundtrip)
{
	ClusterEpochBallotId ballot = make_ballot();
	ClusterEpochBallotId decoded;
	uint8 expected[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 actual[CLUSTER_EPOCH_BALLOT_ID_BYTES];

	expected_ballot_bytes(expected);
	UT_ASSERT(cluster_epoch_ballot_id_encode(&ballot, actual));
	UT_ASSERT_EQ(memcmp(actual, expected, sizeof(actual)), 0);
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(cluster_epoch_ballot_id_decode(expected, &decoded));
	UT_ASSERT_EQ(memcmp(&decoded, &ballot, sizeof(ballot)), 0);
}


UT_TEST(test_ballot_id_validation_order_and_checked_counter)
{
	ClusterEpochBallotId a = make_ballot();
	ClusterEpochBallotId b = a;
	uint64 next = UINT64_C(77);

	UT_ASSERT(cluster_epoch_ballot_id_is_valid(&a));
	b.counter++;
	UT_ASSERT(cluster_epoch_ballot_id_compare(&a, &b) < 0);
	b = a;
	b.proposer_node_id++;
	UT_ASSERT(cluster_epoch_ballot_id_compare(&a, &b) < 0);
	b = a;
	b.proposer_admitted_incarnation++;
	UT_ASSERT(cluster_epoch_ballot_id_compare(&a, &b) < 0);
	b = a;
	b.nonce++;
	UT_ASSERT(cluster_epoch_ballot_id_compare(&a, &b) < 0);
	UT_ASSERT_EQ(cluster_epoch_ballot_id_compare(&a, &a), 0);

	UT_ASSERT(cluster_epoch_ballot_next_counter(0, &next));
	UT_ASSERT(next == UINT64_C(1));
	UT_ASSERT(cluster_epoch_ballot_next_counter(UINT64_MAX - 1, &next));
	UT_ASSERT(next == UINT64_MAX);
	next = UINT64_C(77);
	UT_ASSERT(!cluster_epoch_ballot_next_counter(UINT64_MAX, &next));
	UT_ASSERT(next == UINT64_C(77));

	a.counter = 0;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
	a = make_ballot();
	a.proposer_node_id = -1;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
	a = make_ballot();
	a.proposer_node_id = CLUSTER_MAX_NODES;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
	a = make_ballot();
	a.reserved = 1;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
	a = make_ballot();
	a.proposer_admitted_incarnation = 0;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
	a = make_ballot();
	a.nonce = 0;
	UT_ASSERT(!cluster_epoch_ballot_id_is_valid(&a));
}


UT_TEST(test_authority_value_exact_little_endian_golden_and_roundtrip)
{
	ClusterEpochAuthorityValue value = make_value();
	ClusterEpochAuthorityValue decoded;
	uint8 expected[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];
	uint8 actual[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];

	expected_value_bytes(expected);
	UT_ASSERT(cluster_epoch_authority_value_encode(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
												   actual));
	UT_ASSERT_EQ(memcmp(actual, expected, sizeof(actual)), 0);
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(cluster_epoch_authority_value_decode(
		expected, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &decoded));
	UT_ASSERT_EQ(memcmp(&decoded, &value, sizeof(value)), 0);
}


UT_TEST(test_authority_value_rejects_identity_phase_and_reserved_drift)
{
	ClusterEpochAuthorityValue value = make_value();

	UT_ASSERT(
		cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value.value_version++;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.transition = 0;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.event_kind = 6;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.request_origin_node = -1;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.target_node_id = -1;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.event_subject_bitmap[0] = UINT8_C(0x0c);
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value.target_node_id = -1;
	UT_ASSERT(
		cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.reserved0 = 1;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.authority_generation = 0;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.reserved_epoch++;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.baseline_epoch = UINT64_MAX;
	value.reserved_epoch = 0;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.request_nonce = 0;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	memset(value.authority_member_bitmap, 0, sizeof(value.authority_member_bitmap));
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.grammar_fingerprint ^= UINT64_C(1);
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.reserved1[3] = 1;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
}


UT_TEST(test_authority_genesis_has_the_exact_zero_polarity)
{
	ClusterEpochAuthorityValue value = make_value();

	value.transition = CLUSTER_EPOCH_AUTHORITY_GENESIS;
	value.event_kind = CLUSTER_EPOCH_EVENT_GENESIS;
	value.target_node_id = 0;
	value.authority_generation = 1;
	value.reserved_epoch = value.baseline_epoch;
	value.old_incarnation = 0;
	value.fresh_incarnation = 0;
	value.request_nonce = 0;
	memset(value.event_subject_bitmap, 0, sizeof(value.event_subject_bitmap));
	memset(value.predecessor_digest, 0, sizeof(value.predecessor_digest));
	UT_ASSERT(
		cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));

	value.authority_generation = 2;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value.authority_generation = 1;
	value.request_nonce = 1;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
	value = make_value();
	value.event_kind = CLUSTER_EPOCH_EVENT_GENESIS;
	UT_ASSERT(
		!cluster_epoch_authority_value_is_valid(&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT));
}


UT_TEST(test_lane_exact_512_byte_golden_crc_and_roundtrip)
{
	ClusterEpochBallotLane lane = make_lane();
	ClusterEpochBallotLane decoded;
	uint8 expected[CLUSTER_EPOCH_BALLOT_LANE_BYTES];
	uint8 actual[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	expected_lane_bytes(expected);
	UT_ASSERT(encode_lane(&lane, actual));
	UT_ASSERT_EQ(memcmp(actual, expected, sizeof(actual)), 0);
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(decode_lane(expected, &decoded));
	UT_ASSERT_EQ(memcmp(&decoded, &lane, offsetof(ClusterEpochBallotLane, crc32c)), 0);
	UT_ASSERT(decoded.crc32c == UINT32_C(0x886ef5fe));
}


UT_TEST(test_lane_rejects_header_identity_reserved_and_crc_drift)
{
	ClusterEpochBallotLane lane = make_lane();
	ClusterEpochBallotLane before;
	uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	UT_ASSERT(encode_lane(&lane, bytes));
	before = lane;
	bytes[0] ^= UINT8_C(1);
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT_EQ(memcmp(&lane, &before, sizeof(lane)), 0);

	UT_ASSERT(encode_lane(&before, bytes));
	bytes[6] = 4;
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[7] = 1;
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[8] = 6;
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[12] = 5;
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[16] ^= UINT8_C(1);
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[32] ^= UINT8_C(1);
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[40] ^= UINT8_C(1);
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[400] = 1;
	test_lane_recrc(bytes);
	UT_ASSERT(!decode_lane(bytes, &lane));
	UT_ASSERT(encode_lane(&before, bytes));
	bytes[508] ^= UINT8_C(1);
	UT_ASSERT(!decode_lane(bytes, &lane));
}


UT_TEST(test_lane_phase_and_zero_pair_polarity_is_exact)
{
	ClusterEpochBallotLane lane = make_lane();
	ClusterEpochAuthorityValue zero_value;
	ClusterEpochBallotId zero_ballot;
	uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	memset(&zero_value, 0, sizeof(zero_value));
	memset(&zero_ballot, 0, sizeof(zero_ballot));
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_PROMISED;
	lane.accepted_ballot = zero_ballot;
	lane.accepted_value = zero_value;
	lane.settled_ballot = zero_ballot;
	lane.settled_value = zero_value;
	UT_ASSERT(encode_lane(&lane, out));

	lane = make_lane();
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_ACCEPTED;
	lane.settled_ballot = zero_ballot;
	lane.settled_value = zero_value;
	UT_ASSERT(encode_lane(&lane, out));

	lane = make_lane();
	lane.accepted_value = zero_value;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.accepted_ballot = zero_ballot;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.settled_value = zero_value;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.settled_ballot = zero_ballot;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_EMPTY;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.last_write_phase = 4;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.lane_generation = 0;
	UT_ASSERT(!encode_lane(&lane, out));
}


UT_TEST(test_lane_rejects_ballot_and_authority_history_regression)
{
	ClusterEpochBallotLane lane = make_lane();
	uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	lane.promised_ballot.counter--;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.accepted_ballot.counter--;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.accepted_value.authority_generation--;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.settled_value.predecessor_digest[0] ^= UINT8_C(1);
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_ACCEPTED;
	lane.accepted_ballot.nonce++;
	UT_ASSERT(!encode_lane(&lane, out));
	lane = make_lane();
	lane.last_write_phase = CLUSTER_EPOCH_BALLOT_PHASE_SETTLED;
	lane.settled_ballot.nonce++;
	UT_ASSERT(!encode_lane(&lane, out));
}


UT_TEST(test_failed_codecs_preserve_outputs)
{
	ClusterEpochBallotId ballot = make_ballot();
	ClusterEpochBallotId ballot_out;
	ClusterEpochBallotId ballot_before;
	ClusterEpochAuthorityValue value = make_value();
	ClusterEpochAuthorityValue value_out;
	ClusterEpochAuthorityValue value_before;
	ClusterEpochBallotLane lane = make_lane();
	uint8 ballot_bytes[CLUSTER_EPOCH_BALLOT_ID_BYTES];
	uint8 value_bytes[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES];
	uint8 lane_bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES];
	uint8 bytes_before[CLUSTER_EPOCH_BALLOT_LANE_BYTES];

	expected_ballot_bytes(ballot_bytes);
	memset(&ballot_out, 0xa5, sizeof(ballot_out));
	ballot_before = ballot_out;
	ballot_bytes[12] = 1;
	UT_ASSERT(!cluster_epoch_ballot_id_decode(ballot_bytes, &ballot_out));
	UT_ASSERT_EQ(memcmp(&ballot_out, &ballot_before, sizeof(ballot_out)), 0);

	expected_value_bytes(value_bytes);
	memset(&value_out, 0xa5, sizeof(value_out));
	value_before = value_out;
	value_bytes[120] = 1;
	UT_ASSERT(!cluster_epoch_authority_value_decode(
		value_bytes, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &value_out));
	UT_ASSERT_EQ(memcmp(&value_out, &value_before, sizeof(value_out)), 0);

	memset(lane_bytes, 0xa5, sizeof(lane_bytes));
	memcpy(bytes_before, lane_bytes, sizeof(lane_bytes));
	ballot.counter = 0;
	UT_ASSERT(!cluster_epoch_ballot_id_encode(&ballot, lane_bytes));
	UT_ASSERT_EQ(memcmp(lane_bytes, bytes_before, sizeof(lane_bytes)), 0);
	value.reserved1[0] = 1;
	UT_ASSERT(!cluster_epoch_authority_value_encode(
		&value, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, lane_bytes));
	UT_ASSERT_EQ(memcmp(lane_bytes, bytes_before, sizeof(lane_bytes)), 0);
	lane.flags = 1;
	UT_ASSERT(!encode_lane(&lane, lane_bytes));
	UT_ASSERT_EQ(memcmp(lane_bytes, bytes_before, sizeof(lane_bytes)), 0);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_exact_host_layouts);
	UT_RUN(test_ballot_id_exact_little_endian_golden_and_roundtrip);
	UT_RUN(test_ballot_id_validation_order_and_checked_counter);
	UT_RUN(test_authority_value_exact_little_endian_golden_and_roundtrip);
	UT_RUN(test_authority_value_rejects_identity_phase_and_reserved_drift);
	UT_RUN(test_authority_genesis_has_the_exact_zero_polarity);
	UT_RUN(test_lane_exact_512_byte_golden_crc_and_roundtrip);
	UT_RUN(test_lane_rejects_header_identity_reserved_and_crc_drift);
	UT_RUN(test_lane_phase_and_zero_pair_polarity_is_exact);
	UT_RUN(test_lane_rejects_ballot_and_authority_history_regression);
	UT_RUN(test_failed_codecs_preserve_outputs);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
