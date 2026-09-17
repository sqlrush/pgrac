/*-------------------------------------------------------------------------
 *
 * test_pgrac_external_fence_protocol.c
 *	  RF-ROOT P4 exact PFRQ manual-codec golden tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "common/pgrac_external_fence_protocol.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static const uint8 pfrq_golden[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES]
	= { 0x50, 0x46, 0x52, 0x51, 0x01, 0x00, 0x01, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
		0x0f, 0x10, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x20, 0x21, 0x22, 0x23, 0x24,
		0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0xd6, 0xff, 0xff,
		0xff, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x80, 0x81,
		0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90,
		0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
		0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xc0, 0x27, 0x09, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x4f, 0x1d, 0xa4 };

static const uint8 pfrs_golden[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES] = {
	0x50, 0x46, 0x52, 0x53, 0x01, 0x00, 0x02, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
	0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
	0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0xd6, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
	0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
	0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
	0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
	0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7,
	0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
	0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x18, 0x13, 0x0b, 0x3f, 0x15, 0x13, 0x12, 0x11,
	0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
	0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
	0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
	0xf9, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50, 0x46, 0x52, 0x45, 0x98, 0xa8, 0xdd, 0xc3
};

static const uint8 pfrj_prepare_golden[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES] = {
	0x50, 0x46, 0x52, 0x4a, 0x01, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd6, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
	0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
	0xc0, 0x27, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50, 0x46, 0x52, 0x5a, 0xf4, 0x5c, 0x94, 0x79
};

static const uint8 protected_set_digest_golden[32]
	= { 0x2f, 0x1f, 0xff, 0xca, 0xee, 0x84, 0x6e, 0x21, 0xb6, 0x81, 0x2a,
		0xab, 0xb0, 0x8d, 0x20, 0xc8, 0x95, 0xf3, 0xff, 0xbe, 0x1f, 0x2f,
		0x7b, 0x1a, 0xb9, 0x60, 0xd0, 0x65, 0xfe, 0x99, 0x9f, 0xc0 };

static const uint8 target_state_digest_golden[32]
	= { 0x93, 0x15, 0xb3, 0x24, 0x80, 0x0b, 0xec, 0xa8, 0x58, 0xb9, 0x12,
		0x45, 0x4d, 0x0d, 0x60, 0x7a, 0x34, 0xdb, 0x67, 0x0f, 0xcb, 0xa3,
		0x21, 0xff, 0x71, 0x92, 0xaa, 0xde, 0x71, 0xdd, 0xba, 0x54 };

static const uint8 binding_digest_golden[32]
	= { 0xef, 0x4e, 0xc3, 0x41, 0x1c, 0xee, 0x9a, 0x3c, 0x73, 0x1d, 0x8f,
		0x2e, 0x53, 0x4d, 0x4f, 0x58, 0x89, 0x04, 0x46, 0x20, 0x4a, 0xea,
		0xfe, 0x49, 0x66, 0x19, 0x44, 0x0a, 0x67, 0x6e, 0x53, 0x65 };

static const uint8 rejoin_binding_digest_golden[32]
	= { 0xe0, 0x26, 0xaa, 0x2f, 0xb0, 0x55, 0x4e, 0xc0, 0x6a, 0x05, 0xd6,
		0xc3, 0x17, 0x55, 0x07, 0x8c, 0xa5, 0x88, 0xe1, 0x51, 0x50, 0xc7,
		0x33, 0xe6, 0x11, 0x6d, 0x2d, 0x81, 0xfc, 0xfe, 0x9c, 0xef };

static void
fill_request(PgracExternalFenceProtocolRequestV1 *request)
{
	int i;

	memset(request, 0, sizeof(*request));
	for (i = 0; i < 16; i++)
		request->request_nonce[i] = (uint8)(i + 1);
	request->need.system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 32; i++) {
		request->need.canonical_duty_digest[i] = (uint8)(0x20 + i);
		request->need.protected_set_digest[i] = (uint8)(0x80 + i);
	}
	request->need.victim_node_id = -42;
	request->need.victim_incarnation = UINT64_C(0xfedcba9876543210);
	request->need.predicate_id = 1;
	request->need.predicate_version = 1;
	request->timeout_ms = 600000;
}

static void
fill_response(PgracExternalFenceProtocolResponseV1 *response)
{
	int i;

	memset(response, 0, sizeof(*response));
	response->verdict = 1;
	for (i = 0; i < 16; i++) {
		response->request_nonce[i] = (uint8)(i + 1);
		response->daemon_boot_id[i] = (uint8)(0xa0 + i);
	}
	response->binding.system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 32; i++) {
		response->binding.canonical_duty_digest[i] = (uint8)(0x20 + i);
		response->binding.protected_set_digest[i] = (uint8)(0x80 + i);
		response->target_state_digest[i] = (uint8)(0xc0 + i);
	}
	response->binding.victim_node_id = -42;
	response->binding.victim_incarnation = UINT64_C(0xfedcba9876543210);
	response->binding.target_mapping_generation = UINT64_C(0x1122334455667788);
	response->binding.predicate_id = 1;
	response->binding.predicate_version = 1;
	response->journal_seq = UINT64_C(0x0102030405060708);
	response->verified_mono_ns = UINT64_C(0x1112131415161718);
	response->fresh_until_mono_ns = UINT64_C(0x111213153f0b1318);
	response->proof_generation = UINT64_C(0x2122232425262728);
	response->provider_id = UINT16_C(0x0100);
	response->provider_abi_version = 1;
	response->provider_result = 0;
	response->provider_native_status = -7;
	response->deny_reason = 0;
}

static void
fill_rejoin_prepare(PgracExternalFenceProtocolRejoinFrameV1 *frame)
{
	int i;

	memset(frame, 0, sizeof(*frame));
	frame->opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE;
	for (i = 0; i < 16; i++)
		frame->transport_nonce[i] = (uint8)(i + 1);
	frame->old_node_id = -42;
	frame->old_incarnation = UINT64_C(0xfedcba9876543210);
	frame->candidate_incarnation = UINT64_C(0x0102030405060708);
	frame->timeout_ms = 600000;
}

UT_TEST(test_pfrq_exact_golden_and_round_trip)
{
	PgracExternalFenceProtocolRequestV1 input;
	PgracExternalFenceProtocolRequestV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	fill_request(&input);
	UT_ASSERT(pgrac_external_fence_request_v1_encode(&input, frame));
	UT_ASSERT(memcmp(frame, pfrq_golden, sizeof(frame)) == 0);
	UT_ASSERT(pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT(memcmp(&decoded, &input, sizeof(input)) == 0);
}

UT_TEST(test_pfrq_rejects_bad_crc_reserved_and_nonce)
{
	PgracExternalFenceProtocolRequestV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	memcpy(frame, pfrq_golden, sizeof(frame));
	frame[40] ^= 0x01;
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrq_golden, sizeof(frame));
	frame[132] = 1;
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrq_golden, sizeof(frame));
	memset(frame + 16, 0, 16);
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));
}

UT_TEST(test_pfrq_rejects_wrong_length_version_and_type)
{
	PgracExternalFenceProtocolRequestV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	UT_ASSERT(
		!pgrac_external_fence_request_v1_decode(pfrq_golden, sizeof(pfrq_golden) - 1, &decoded));

	memcpy(frame, pfrq_golden, sizeof(frame));
	frame[4] = 2;
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrq_golden, sizeof(frame));
	frame[6] = 2;
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrq_golden, sizeof(frame));
	frame[8] = 0x9f;
	UT_ASSERT(!pgrac_external_fence_request_v1_decode(frame, sizeof(frame), &decoded));
}

UT_TEST(test_pfrq_encoder_rejects_noncanonical_input)
{
	PgracExternalFenceProtocolRequestV1 input;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];

	fill_request(&input);
	memset(input.request_nonce, 0, sizeof(input.request_nonce));
	UT_ASSERT(!pgrac_external_fence_request_v1_encode(&input, frame));

	fill_request(&input);
	input.timeout_ms = 0;
	UT_ASSERT(!pgrac_external_fence_request_v1_encode(&input, frame));

	fill_request(&input);
	input.timeout_ms = 600001;
	UT_ASSERT(!pgrac_external_fence_request_v1_encode(&input, frame));
}

UT_TEST(test_pfrs_exact_golden_and_round_trip)
{
	PgracExternalFenceProtocolResponseV1 input;
	PgracExternalFenceProtocolResponseV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];

	fill_response(&input);
	UT_ASSERT(pgrac_external_fence_response_v1_encode(&input, frame));
	UT_ASSERT(memcmp(frame, pfrs_golden, sizeof(frame)) == 0);
	UT_ASSERT(pgrac_external_fence_response_v1_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT(memcmp(&decoded, &input, sizeof(input)) == 0);
}

UT_TEST(test_pfrs_rejects_crc_reserved_and_trailer_corruption)
{
	PgracExternalFenceProtocolResponseV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];

	memcpy(frame, pfrs_golden, sizeof(frame));
	frame[100] ^= 0x01;
	UT_ASSERT(!pgrac_external_fence_response_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrs_golden, sizeof(frame));
	frame[232] = 1;
	UT_ASSERT(!pgrac_external_fence_response_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrs_golden, sizeof(frame));
	frame[248] = 'X';
	UT_ASSERT(!pgrac_external_fence_response_v1_decode(frame, sizeof(frame), &decoded));
}

UT_TEST(test_pfrs_negative_preserves_diagnostic_boot_and_journal)
{
	PgracExternalFenceProtocolResponseV1 input;
	PgracExternalFenceProtocolResponseV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	int i;

	memset(&input, 0, sizeof(input));
	input.verdict = 4;
	for (i = 0; i < 16; i++) {
		input.request_nonce[i] = (uint8)(i + 1);
		input.daemon_boot_id[i] = (uint8)(0xe0 + i);
	}
	input.journal_seq = 9;
	input.provider_abi_version = 1;
	input.provider_result = 4;
	input.provider_native_status = -9;
	input.deny_reason = 10;
	UT_ASSERT(pgrac_external_fence_response_v1_encode(&input, frame));
	UT_ASSERT(pgrac_external_fence_response_v1_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT(memcmp(&decoded, &input, sizeof(input)) == 0);
}

UT_TEST(test_pfrj_prepare_exact_golden_and_round_trip)
{
	PgracExternalFenceProtocolRejoinFrameV1 input;
	PgracExternalFenceProtocolRejoinFrameV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	fill_rejoin_prepare(&input);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&input, frame));
	UT_ASSERT(memcmp(frame, pfrj_prepare_golden, sizeof(frame)) == 0);
	UT_ASSERT(pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &decoded));
	UT_ASSERT(memcmp(&decoded, &input, sizeof(input)) == 0);
}

UT_TEST(test_pfrj_rejects_reserved_crc_and_unknown_opcode)
{
	PgracExternalFenceProtocolRejoinFrameV1 decoded;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	memcpy(frame, pfrj_prepare_golden, sizeof(frame));
	frame[124] = 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrj_prepare_golden, sizeof(frame));
	frame[140] ^= 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &decoded));

	memcpy(frame, pfrj_prepare_golden, sizeof(frame));
	frame[6] = 10;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &decoded));
}

UT_TEST(test_pfrj_enforces_opcode_specific_zero_and_proof_rules)
{
	PgracExternalFenceProtocolRejoinFrameV1 rejoin;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	fill_rejoin_prepare(&rejoin);
	rejoin.system_identifier = 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));

	memset(&rejoin, 0, sizeof(rejoin));
	rejoin.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memset(rejoin.transport_nonce, 0x71, sizeof(rejoin.transport_nonce));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));
	rejoin.old_node_id = 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));

	memset(&rejoin, 0, sizeof(rejoin));
	rejoin.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL;
	memset(rejoin.transport_nonce, 0x72, sizeof(rejoin.transport_nonce));
	memset(rejoin.operation_id, 0x73, sizeof(rejoin.operation_id));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));
	rejoin.old_incarnation = 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));

	memset(&rejoin, 0, sizeof(rejoin));
	rejoin.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER;
	memset(rejoin.transport_nonce, 0x74, sizeof(rejoin.transport_nonce));
	memset(rejoin.operation_id, 0x75, sizeof(rejoin.operation_id));
	rejoin.system_identifier = 9001;
	memset(rejoin.protected_set_digest, 0x76, sizeof(rejoin.protected_set_digest));
	rejoin.old_node_id = 3;
	rejoin.old_incarnation = 70;
	rejoin.candidate_incarnation = 77;
	rejoin.provider_id = 1;
	rejoin.provider_abi_version = 1;
	rejoin.target_mapping_generation = 17;
	memset(rejoin.daemon_boot_id, 0x77, sizeof(rejoin.daemon_boot_id));
	rejoin.journal_seq = 4;
	rejoin.verified_mono_ns = 10;
	rejoin.fresh_until_mono_ns = 20;
	rejoin.proof_generation = 5;
	memset(rejoin.target_state_digest, 0x78, sizeof(rejoin.target_state_digest));
	rejoin.status = 1;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));
	rejoin.rejoin_gate_digest[0] = 1;
	UT_ASSERT(!pgrac_external_fence_rejoin_v1_encode(&rejoin, frame));
}

UT_TEST(test_protected_set_digest_exact_domain)
{
	uint8 storage_uuid[16];
	uint8 digest[32];
	int i;

	for (i = 0; i < 16; i++)
		storage_uuid[i] = (uint8)i;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(UINT32_C(3), storage_uuid, digest));
	UT_ASSERT(memcmp(digest, protected_set_digest_golden, sizeof(digest)) == 0);
}

UT_TEST(test_target_state_digest_exact_domain_and_closed_values)
{
	uint8 target_uuid[16];
	uint8 digest[32];
	int i;

	for (i = 0; i < 16; i++)
		target_uuid[i] = (uint8)i;
	UT_ASSERT(pgrac_external_fence_target_state_digest_v1(
		target_uuid, 1, 1, UINT64_C(0x1122334455667788), UINT64_C(0x2122232425262728), digest));
	UT_ASSERT(memcmp(digest, target_state_digest_golden, sizeof(digest)) == 0);
	UT_ASSERT(!pgrac_external_fence_target_state_digest_v1(target_uuid, 3, 1, 1, 1, digest));
	UT_ASSERT(!pgrac_external_fence_target_state_digest_v1(target_uuid, 1, 0, 1, 1, digest));
	UT_ASSERT(!pgrac_external_fence_target_state_digest_v1(target_uuid, 1, 1, 0, 1, digest));
}

UT_TEST(test_binding_digest_exact_manual_encoding)
{
	PgracExternalFenceProtocolResponseV1 response;
	uint8 digest[32];

	fill_response(&response);
	UT_ASSERT(pgrac_external_fence_binding_digest_v1(&response.binding, digest));
	UT_ASSERT(memcmp(digest, binding_digest_golden, sizeof(digest)) == 0);
}

UT_TEST(test_rejoin_binding_digest_exact_manual_encoding)
{
	PgracExternalFenceProtocolRejoinBindingV1 binding;
	uint8 digest[32];
	int i;

	memset(&binding, 0, sizeof(binding));
	binding.system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 32; i++) {
		binding.rejoin_gate_digest[i] = (uint8)(0x40 + i);
		binding.protected_set_digest[i] = (uint8)(0x80 + i);
	}
	binding.old_node_id = -42;
	binding.old_incarnation = UINT64_C(0xfedcba9876543210);
	binding.candidate_incarnation = UINT64_C(0x0102030405060708);
	binding.target_mapping_generation = UINT64_C(0x1122334455667788);
	binding.predicate_id = 2;
	binding.predicate_version = 1;
	UT_ASSERT(pgrac_external_fence_rejoin_binding_digest_v1(&binding, digest));
	UT_ASSERT(memcmp(digest, rejoin_binding_digest_golden, sizeof(digest)) == 0);
}

UT_TEST(test_acquire_echo_binding_rejects_every_identity_mutation)
{
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	PgracExternalFenceProtocolBindingV1 binding;
	PgracExternalFenceProtocolResponseV1 changed;

	fill_request(&request);
	request.need.victim_node_id = 42;
	fill_response(&response);
	response.binding.victim_node_id = 42;
	UT_ASSERT(pgrac_external_fence_need_v1_valid(&request.need));
	UT_ASSERT(pgrac_external_fence_binding_from_request_v1(
		&request.need, response.binding.target_mapping_generation, &binding));
	response.binding = binding;
	UT_ASSERT(pgrac_external_fence_affirmative_response_matches_request_v1(&request, &response));

	changed = response;
	changed.request_nonce[0] ^= 1;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.system_identifier++;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.canonical_duty_digest[0] ^= 1;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.victim_node_id++;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.victim_incarnation++;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.target_mapping_generation = 0;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.protected_set_digest[0] ^= 1;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.predicate_id++;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));
	changed = response;
	changed.binding.predicate_version++;
	UT_ASSERT(!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &changed));

	request.need.victim_node_id = -1;
	UT_ASSERT(!pgrac_external_fence_need_v1_valid(&request.need));
	UT_ASSERT(!pgrac_external_fence_binding_from_request_v1(&request.need, 1, &binding));
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_pfrq_exact_golden_and_round_trip);
	UT_RUN(test_pfrq_rejects_bad_crc_reserved_and_nonce);
	UT_RUN(test_pfrq_rejects_wrong_length_version_and_type);
	UT_RUN(test_pfrq_encoder_rejects_noncanonical_input);
	UT_RUN(test_pfrs_exact_golden_and_round_trip);
	UT_RUN(test_pfrs_rejects_crc_reserved_and_trailer_corruption);
	UT_RUN(test_pfrs_negative_preserves_diagnostic_boot_and_journal);
	UT_RUN(test_pfrj_prepare_exact_golden_and_round_trip);
	UT_RUN(test_pfrj_rejects_reserved_crc_and_unknown_opcode);
	UT_RUN(test_pfrj_enforces_opcode_specific_zero_and_proof_rules);
	UT_RUN(test_protected_set_digest_exact_domain);
	UT_RUN(test_target_state_digest_exact_domain_and_closed_values);
	UT_RUN(test_binding_digest_exact_manual_encoding);
	UT_RUN(test_rejoin_binding_digest_exact_manual_encoding);
	UT_RUN(test_acquire_echo_binding_rejects_every_identity_mutation);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
