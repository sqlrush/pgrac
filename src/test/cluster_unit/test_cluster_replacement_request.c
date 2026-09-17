/*-------------------------------------------------------------------------
 *
 * test_cluster_replacement_request.c
 *    Spec-5.15A RPLM exact 64-byte request-marker codec tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_replacement_request.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static ClusterReplacementRequestMarker
valid_marker(void)
{
	ClusterReplacementRequestMarker marker;

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_REPLACEMENT_MARKER_MAGIC;
	marker.version = CLUSTER_REPLACEMENT_MARKER_VERSION;
	marker.phase = CLUSTER_REPLACEMENT_MARKER_PHASE_REQUESTED;
	marker.target_node_id = 3;
	marker.baseline_epoch = UINT64_C(0x0102030405060708);
	marker.old_admitted_incarnation = UINT64_C(0x1112131415161718);
	marker.fresh_incarnation = UINT64_C(0x2122232425262728);
	marker.request_nonce = UINT64_C(0x3132333435363738);
	marker.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	return marker;
}

UT_TEST(test_replacement_request_exact_layout_and_roundtrip)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	ClusterReplacementRequestMarker decoded;
	uint8 image[CLUSTER_REPLACEMENT_MARKER_BYTES];
	static const uint8 expected_prefix[56]
		= { 0x4d, 0x4c, 0x50, 0x52, 0x01, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15,
			0x14, 0x13, 0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x38, 0x37,
			0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0xe4, 0x05, 0x89, 0x42, 0x5b, 0xae, 0x0d, 0x8e };

	memset(image, 0xa5, sizeof(image));
	UT_ASSERT(cluster_replacement_request_encode(&marker, image));
	UT_ASSERT_EQ(memcmp(image, expected_prefix, sizeof(expected_prefix)), 0);
	UT_ASSERT_EQ(image[60], 0);
	UT_ASSERT_EQ(image[61], 0);
	UT_ASSERT_EQ(image[62], 0);
	UT_ASSERT_EQ(image[63], 0);
	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT(cluster_replacement_request_decode(image, 3, &decoded));
	UT_ASSERT_EQ(decoded.target_node_id, 3);
	UT_ASSERT_EQ(decoded.baseline_epoch, marker.baseline_epoch);
	UT_ASSERT_EQ(decoded.old_admitted_incarnation, marker.old_admitted_incarnation);
	UT_ASSERT_EQ(decoded.fresh_incarnation, marker.fresh_incarnation);
	UT_ASSERT_EQ(decoded.request_nonce, marker.request_nonce);
	UT_ASSERT_EQ(decoded.grammar_fingerprint, marker.grammar_fingerprint);
}

UT_TEST(test_replacement_request_crc_and_identity_fail_closed)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	ClusterReplacementRequestMarker sentinel;
	ClusterReplacementRequestMarker out;
	uint8 image[CLUSTER_REPLACEMENT_MARKER_BYTES];

	memset(&sentinel, 0x5a, sizeof(sentinel));
	UT_ASSERT(cluster_replacement_request_encode(&marker, image));
	out = sentinel;
	image[24] ^= UINT8_C(0x01);
	UT_ASSERT(!cluster_replacement_request_decode(image, 3, &out));
	UT_ASSERT_EQ(memcmp(&out, &sentinel, sizeof(out)), 0);
	image[24] ^= UINT8_C(0x01);
	out = sentinel;
	UT_ASSERT(!cluster_replacement_request_decode(image, 2, &out));
	UT_ASSERT_EQ(memcmp(&out, &sentinel, sizeof(out)), 0);
	image[60] = 1;
	out = sentinel;
	UT_ASSERT(!cluster_replacement_request_decode(image, 3, &out));
	UT_ASSERT_EQ(memcmp(&out, &sentinel, sizeof(out)), 0);
}

UT_TEST(test_replacement_request_rejects_invalid_marker_without_output)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	uint8 sentinel[CLUSTER_REPLACEMENT_MARKER_BYTES];
	uint8 image[CLUSTER_REPLACEMENT_MARKER_BYTES];

	memset(sentinel, 0xa5, sizeof(sentinel));
	memcpy(image, sentinel, sizeof(image));
	marker.request_nonce = 0;
	UT_ASSERT(!cluster_replacement_request_encode(&marker, image));
	UT_ASSERT_EQ(memcmp(image, sentinel, sizeof(image)), 0);
	marker = valid_marker();
	marker.fresh_incarnation = marker.old_admitted_incarnation;
	UT_ASSERT(!cluster_replacement_request_encode(&marker, image));
	UT_ASSERT_EQ(memcmp(image, sentinel, sizeof(image)), 0);
	marker = valid_marker();
	marker.grammar_fingerprint++;
	UT_ASSERT(!cluster_replacement_request_encode(&marker, image));
	UT_ASSERT_EQ(memcmp(image, sentinel, sizeof(image)), 0);
}

UT_TEST(test_replacement_request_reserved1_pack_clear_is_exact)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	ClusterReplacementRequestMarker decoded;
	uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];
	uint8 before[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];

	memset(reserved1, 0x7c, sizeof(reserved1));
	memcpy(before, reserved1, sizeof(before));
	UT_ASSERT(cluster_replacement_request_pack(reserved1, &marker));
	UT_ASSERT_EQ(memcmp(reserved1, before, CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET), 0);
	UT_ASSERT_EQ(memcmp(reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							+ CLUSTER_REPLACEMENT_MARKER_BYTES,
						before + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							+ CLUSTER_REPLACEMENT_MARKER_BYTES,
						sizeof(reserved1) - CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							- CLUSTER_REPLACEMENT_MARKER_BYTES),
				 0);
	UT_ASSERT(cluster_replacement_request_unpack(reserved1, 3, &decoded));
	UT_ASSERT_EQ(decoded.request_nonce, marker.request_nonce);
	cluster_replacement_request_clear(reserved1);
	UT_ASSERT(cluster_replacement_request_is_clear(reserved1));
	UT_ASSERT_EQ(memcmp(reserved1, before, CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET), 0);
	UT_ASSERT_EQ(memcmp(reserved1 + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							+ CLUSTER_REPLACEMENT_MARKER_BYTES,
						before + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							+ CLUSTER_REPLACEMENT_MARKER_BYTES,
						sizeof(reserved1) - CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
							- CLUSTER_REPLACEMENT_MARKER_BYTES),
				 0);
}

UT_TEST(test_replacement_request_slot_state_requires_exact_pair)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	ClusterReplacementRequestMarker sentinel;
	ClusterReplacementRequestMarker decoded;
	uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];

	memset(reserved1, 0, sizeof(reserved1));
	memset(&sentinel, 0x5a, sizeof(sentinel));
	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_replacement_request_slot_state(0, reserved1, 3, marker.fresh_incarnation, &decoded),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_CLEAR);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_replacement_request_pack(reserved1, &marker));
	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_replacement_request_slot_state(CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED,
											   reserved1, 3, marker.fresh_incarnation, &decoded),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID);
	UT_ASSERT_EQ(decoded.request_nonce, marker.request_nonce);

	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_replacement_request_slot_state(0, reserved1, 3, marker.fresh_incarnation, &decoded),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	decoded = sentinel;
	UT_ASSERT_EQ(cluster_replacement_request_slot_state(
					 CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED, reserved1, 3,
					 marker.fresh_incarnation + 1, &decoded),
				 CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	cluster_replacement_request_clear(reserved1);
	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_replacement_request_slot_state(CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED,
											   reserved1, 3, marker.fresh_incarnation, &decoded),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);
}

UT_TEST(test_replacement_request_slot_state_rejects_corrupt_pair)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	ClusterReplacementRequestMarker sentinel;
	ClusterReplacementRequestMarker decoded;
	uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];

	memset(reserved1, 0, sizeof(reserved1));
	UT_ASSERT(cluster_replacement_request_pack(reserved1, &marker));
	reserved1[CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET + 24] ^= UINT8_C(1);
	memset(&sentinel, 0xa5, sizeof(sentinel));
	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_replacement_request_slot_state(CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED,
											   reserved1, 3, marker.fresh_incarnation, &decoded),
		CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);
}

UT_TEST(test_replacement_request_preserve_is_per_disk_and_exact)
{
	ClusterReplacementRequestMarker marker = valid_marker();
	uint8 prior[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];
	uint8 next[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];
	uint8 before[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES];
	uint64 flags;

	memset(prior, 0, sizeof(prior));
	UT_ASSERT(cluster_replacement_request_pack(prior, &marker));
	memset(next, 0x7c, sizeof(next));
	memcpy(before, next, sizeof(before));
	flags = UINT64_C(0x1);
	UT_ASSERT_EQ(cluster_replacement_request_preserve_per_disk(
					 CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED, prior, 3,
					 marker.fresh_incarnation, &flags, next),
				 CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID);
	UT_ASSERT_EQ(flags, UINT64_C(0x1) | CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED);
	UT_ASSERT_EQ(memcmp(next, before, CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET), 0);
	UT_ASSERT_EQ(memcmp(next + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
						prior + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET,
						CLUSTER_REPLACEMENT_MARKER_BYTES),
				 0);
	UT_ASSERT_EQ(
		memcmp(
			next + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET + CLUSTER_REPLACEMENT_MARKER_BYTES,
			before + CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET + CLUSTER_REPLACEMENT_MARKER_BYTES,
			sizeof(next) - CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET
				- CLUSTER_REPLACEMENT_MARKER_BYTES),
		0);

	memset(prior, 0, sizeof(prior));
	memset(next, 0x3d, sizeof(next));
	memcpy(before, next, sizeof(before));
	flags = UINT64_C(0x1);
	UT_ASSERT_EQ(cluster_replacement_request_preserve_per_disk(
					 0, prior, 3, marker.fresh_incarnation, &flags, next),
				 CLUSTER_REPLACEMENT_REQUEST_SLOT_CLEAR);
	UT_ASSERT_EQ(flags, UINT64_C(0x1));
	UT_ASSERT_EQ(memcmp(next, before, sizeof(next)), 0);

	UT_ASSERT(cluster_replacement_request_pack(prior, &marker));
	UT_ASSERT_EQ(cluster_replacement_request_preserve_per_disk(
					 0, prior, 3, marker.fresh_incarnation, &flags, next),
				 CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD);
	UT_ASSERT_EQ(flags, UINT64_C(0x1));
	UT_ASSERT_EQ(memcmp(next, before, sizeof(next)), 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_replacement_request_exact_layout_and_roundtrip);
	UT_RUN(test_replacement_request_crc_and_identity_fail_closed);
	UT_RUN(test_replacement_request_rejects_invalid_marker_without_output);
	UT_RUN(test_replacement_request_reserved1_pack_clear_is_exact);
	UT_RUN(test_replacement_request_slot_state_requires_exact_pair);
	UT_RUN(test_replacement_request_slot_state_rejects_corrupt_pair);
	UT_RUN(test_replacement_request_preserve_is_per_disk_and_exact);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
