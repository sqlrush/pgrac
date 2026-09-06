/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_node_wire.c
 *    Resource-X final node-wire layout -- spec-8.10 D10-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "port/pg_crc32c.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* The Linux ARM CRC chooser uses backend logging. Never hide a CRC error. */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

void
ExceptionalCondition(const char *condition_name, const char *file_name,
				 int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name,
		   line_number);
	abort();
}

UT_TEST(test_wire_kind_and_proof_domains_are_closed)
{
	UT_ASSERT_EQ(RESOURCE_X_WIRE_VERSION, 1);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_ASSERT_X, 1);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_BLOCK_TO_N, 2);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_BLOCKED_TO_N, 3);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_RELEASE_X, 4);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_IMAGE_ENVELOPE, 5);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_AUTHORITY_GRANT, 6);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_INSTALL_SETTLEMENT, 7);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION, 8);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP, 9);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2, 10);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2, 11);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_REMOTE_CARRIER, 1);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_LOCAL_IMAGE, 2);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_DURABLE_STORAGE, 3);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_KIND_MIN, RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_KIND_MAX,
				 RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_KIND_MIN, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_KIND_MAX,
				 RESOURCE_X_PROOF_DURABLE_STORAGE);
}

UT_TEST(test_reused_message_numbers_remain_exact)
{
	UT_ASSERT_EQ(RESOURCE_X_MSG_ASSERT_X,
				 PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ(RESOURCE_X_MSG_IMAGE_OR_GRANT,
				 PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(RESOURCE_X_MSG_BLOCK_TO_N,
				 PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE);
	UT_ASSERT_EQ(RESOURCE_X_MSG_BLOCKED_TO_N,
				 PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
	UT_ASSERT_EQ(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
				 PGRAC_IC_MSG_GCS_BLOCK_DONE);
}

UT_TEST(test_common_wire_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXWireCommonV1), 96);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, wire_version), 0);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, kind), 1);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, header_bytes), 2);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, semantic_crc32c), 4);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, logical_assertion), 8);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, base_authority_generation), 32);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, resource_formation), 40);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, master_session_incarnation), 48);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, assertion_sequence), 56);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, ordered_lane), 64);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, action_node), 68);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, observed_mode), 72);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, target_mode), 73);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, source_candidate), 74);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, retain_pi_if_dirty), 75);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, sender_connection_generation), 76);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, outcome), 80);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, flags), 81);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, zero_reserved), 82);
	UT_ASSERT_EQ(offsetof(ResourceXWireCommonV1, authority_generation), 88);
	UT_ASSERT_EQ(sizeof(ResourceXControlV1), RESOURCE_X_CONTROL_V1_BYTES);
}

UT_TEST(test_local_proof_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXLocalProofDeclarationV1), 152);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  local_holder_authority_generation), 96);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  requester_target_generation), 104);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, page_scn_lsn), 112);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, dependency_count), 120);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  dependency_vector_crc32c), 124);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, page_checksum), 128);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  local_image_proof_crc32c), 132);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  requester_connection_generation), 136);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1,
						  local_proof_generation), 144);
}

UT_TEST(test_blocked_to_n_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXBlockedToNProofV1), 312);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_fence), 96);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1,
						  source_carrier_generation), 130);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1,
						  requester_target_generation), 138);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, page_scn_lsn), 146);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, dependency_count), 154);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, dependencies), 156);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_proof_crc32c), 284);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, page_checksum), 288);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_disposition), 292);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, proof_kind), 293);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, proof_flags), 294);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1,
						  holder_connection_generation), 296);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, acting_formation), 304);
}

UT_TEST(test_authority_grant_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXAuthorityGrantV1), 312);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_fence), 96);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1,
						  final_authority_generation), 130);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1,
						  source_carrier_generation), 138);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1,
						  requester_target_generation), 146);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, page_scn_lsn), 154);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, dependency_count), 162);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, dependencies), 164);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_proof_crc32c), 292);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, page_checksum), 296);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, proof_kind), 300);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_disposition), 301);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, grant_flags), 302);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1,
						  requester_connection_generation), 304);
}

UT_TEST(test_image_envelope_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXImageEnvelopeV1), 8520);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, request_tail), 96);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1,
						  conversion_base_generation), 116);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, source_fence), 124);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1,
						  source_carrier_generation), 158);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1,
						  requester_target_generation), 166);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, page_scn_lsn), 174);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, dependency_count), 182);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, dependencies), 184);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1,
						  dependency_vector_crc32c), 312);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, page_checksum), 316);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, image_length), 320);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, source_disposition), 324);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, proof_kind), 325);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, image_flags), 326);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV1, page_bytes), 328);
	UT_ASSERT_EQ(sizeof(((ResourceXImageEnvelopeV1 *)0)->page_bytes), BLCKSZ);
}

UT_TEST(test_install_settlement_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXInstallSettlementV1), 152);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1,
						  conversion_base_generation), 96);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1,
						  final_authority_generation), 104);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1,
						  requester_connection_generation), 112);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1,
						  requester_target_generation), 120);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, page_scn_lsn), 128);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, page_checksum), 136);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1,
						  source_proof_crc32c), 140);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, installed_mode), 144);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, requester_role), 145);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, terminal_outcome), 146);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, terminal_state), 147);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, settlement_flags), 148);
}

static ResourceXDecodedFrame
make_control_frame(ResourceXWireKind kind)
{
	ResourceXDecodedFrame frame;
	BufferTag tag;

	memset(&frame, 0, sizeof(frame));
	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9001;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 42;
	UT_ASSERT(resource_x_assertion_init(&tag, 3, &frame.common.logical_assertion));
	frame.kind = kind;
	frame.common.base_authority_generation = UINT64_C(0x0102030405060708);
	frame.common.resource_formation = UINT64_C(0x1112131415161718);
	frame.common.master_session_incarnation = UINT64_C(0x2122232425262728);
	frame.common.assertion_sequence = UINT64_C(0x3132333435363738);
	frame.common.ordered_lane = UINT32_C(0x41424344);
	frame.common.action_node = 3;
	frame.common.observed_mode = PCM_STATE_N;
	frame.common.target_mode = PCM_STATE_X;
	frame.common.sender_connection_generation = UINT32_C(0x51525354);
	frame.common.authority_generation = UINT64_C(0x6162636465666768);
	frame.common.outcome = RESOURCE_X_OUTCOME_NONE;
	if (kind == RESOURCE_X_WIRE_BLOCK_TO_N) {
		frame.common.action_node = 7;
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.source_candidate = 1;
		frame.common.retain_pi_if_dirty = 1;
	} else if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N) {
		frame.common.action_node = 7;
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	} else if (kind == RESOURCE_X_WIRE_RELEASE_X) {
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	}
	return frame;
}

static ResourceXDecodedFrame make_typed_frame(ResourceXWireKind kind);
static void test_reseal(uint8 *bytes, uint16 len);

static ResourceXDecodedFrame
make_bootstrap_frame(bool ack)
{
	ResourceXDecodedFrame frame
		= make_control_frame(RESOURCE_X_WIRE_ASSERT_X);

	frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	frame.common.base_authority_generation
		= ack ? UINT64_C(0x0102030405060708) : 0;
	frame.common.authority_generation = 0;
	frame.common.ordered_lane = 0;
	frame.common.observed_mode = PCM_STATE_N;
	frame.common.target_mode = PCM_STATE_X;
	frame.common.source_candidate = 0;
	frame.common.retain_pi_if_dirty = 0;
	frame.common.outcome
		= ack ? RESOURCE_X_OUTCOME_OK : RESOURCE_X_OUTCOME_NONE;
	frame.common.flags = 0;
	return frame;
}

static void
assert_bootstrap_encode_rejected(uint8 msg_type,
								 ResourceXDecodedFrame *frame)
{
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(!cluster_resource_x_wire_encode(
		msg_type, frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(reject != RESOURCE_X_WIRE_REJECT_NONE);
}

UT_TEST(test_preassert_bootstrap_request_and_ack_are_direction_exact)
{
	ResourceXDecodedFrame request = make_bootstrap_frame(false);
	ResourceXDecodedFrame ack = make_bootstrap_frame(true);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_ASSERT_X, &request, bytes, sizeof(bytes), &len,
		&reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(bytes[1], RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_ASSERT_X, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_NONE);

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &ack, bytes, sizeof(bytes), &len,
		&reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.base_authority_generation,
		UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);

	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, &request);
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &ack);
}

UT_TEST(test_preassert_bootstrap_truth_tables_fail_closed)
{
	ResourceXDecodedFrame frame;

	frame = make_bootstrap_frame(false);
	frame.common.base_authority_generation = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(true);
	frame.common.base_authority_generation = 0;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.authority_generation = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.resource_formation = 0;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.master_session_incarnation = 0;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.assertion_sequence = 0;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.ordered_lane = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.action_node++;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.observed_mode = PCM_STATE_S;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.target_mode = PCM_STATE_N;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.source_candidate = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.retain_pi_if_dirty = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.sender_connection_generation = 0;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
	frame = make_bootstrap_frame(true);
	frame.common.outcome = RESOURCE_X_OUTCOME_NONE;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame);
	frame = make_bootstrap_frame(false);
	frame.common.flags = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &frame);
}

UT_TEST(test_preassert_bootstrap_decode_rejects_reserved_length_and_pair_drift)
{
	ResourceXDecodedFrame request = make_bootstrap_frame(false);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_ASSERT_X, &request, bytes, sizeof(bytes), &len,
		&reject));
	bytes[82] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_ASSERT_X, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
	UT_ASSERT(!cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_ASSERT_X, bytes, len - 1, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
	UT_ASSERT(!cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
}

UT_TEST(test_control_codec_is_network_order_and_crc_exact)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame,
		bytes, sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(bytes[0], RESOURCE_X_WIRE_VERSION);
	UT_ASSERT_EQ(bytes[1], RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT_EQ(bytes[2], 0);
	UT_ASSERT_EQ(bytes[3], RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(bytes[32], 0x01);
	UT_ASSERT_EQ(bytes[39], 0x08);
	UT_ASSERT_EQ(bytes[64], 0x41);
	UT_ASSERT_EQ(bytes[67], 0x44);
	UT_ASSERT(memcmp(bytes + 4, "\0\0\0\0", 4) != 0);

	memset(&decoded, 0xa5, sizeof(decoded));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes,
		encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT_EQ(decoded.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
		&frame.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.base_authority_generation,
		frame.common.base_authority_generation);
	UT_ASSERT_EQ(decoded.common.ordered_lane, frame.common.ordered_lane);
	UT_ASSERT_EQ(decoded.common.authority_generation,
		frame.common.authority_generation);
}

UT_TEST(test_physical_sender_generation_rebind_preserves_block_semantics)
{
	ResourceXDecodedFrame frame
		= make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes, sizeof(bytes),
		&encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len, 77, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, 77);
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
		&frame.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.base_authority_generation,
		frame.common.base_authority_generation);
	UT_ASSERT_EQ(decoded.common.assertion_sequence,
		frame.common.assertion_sequence);
	UT_ASSERT_EQ(decoded.common.action_node, frame.common.action_node);
	UT_ASSERT_EQ(decoded.common.observed_mode, frame.common.observed_mode);
	UT_ASSERT_EQ(decoded.common.target_mode, frame.common.target_mode);
	UT_ASSERT_EQ(decoded.common.source_candidate,
		frame.common.source_candidate);
	UT_ASSERT_EQ(decoded.common.retain_pi_if_dirty,
		frame.common.retain_pi_if_dirty);
	UT_ASSERT_EQ(decoded.common.authority_generation,
		frame.common.authority_generation);
	UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len, 0, &reject));
}

UT_TEST(test_shared_s_carrier_block_to_n_round_trip_is_exact)
{
	ResourceXDecodedFrame frame
		= make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	frame.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes, sizeof(bytes),
		&encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(decoded.common.source_candidate, 1);
	UT_ASSERT_EQ(decoded.common.retain_pi_if_dirty, 1);
}

UT_TEST(test_shared_s_carrier_proof_and_image_round_trip_preserve_mode)
{
	ResourceXDecodedFrame proof
		= make_typed_frame(RESOURCE_X_WIRE_BLOCKED_TO_N);
	ResourceXDecodedFrame image
		= make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 encoded_len = 0;

	proof.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCKED_TO_N, &proof, bytes, sizeof(bytes),
		&encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, bytes, encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT(decoded.blocked_has_remote_proof);

	image.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &image, bytes, sizeof(bytes),
		&encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_X);
}

UT_TEST(test_block_to_n_source_polarity_truth_table_is_closed)
{
	ResourceXDecodedFrame frame
		= make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	frame.common.observed_mode = PCM_STATE_S;
	frame.common.source_candidate = 1;
	frame.common.retain_pi_if_dirty = 0;
	UT_ASSERT(!cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes, sizeof(bytes),
		&encoded_len, &reject));

	frame.common.source_candidate = 0;
	frame.common.retain_pi_if_dirty = 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes, sizeof(bytes),
		&encoded_len, &reject));
}

UT_TEST(test_all_control_kind_type_pairs_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = {
		{RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_ASSERT_X},
		{RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_WIRE_BLOCK_TO_N},
		{RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N},
		{RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_WIRE_RELEASE_X}
	};
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_control_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 encoded_len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame,
			bytes, sizeof(bytes), &encoded_len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes,
			encoded_len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.kind, cases[i].kind);
	}
}

UT_TEST(test_control_codec_rejects_pair_identity_crc_and_legacy_length)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	ResourceXDecodedFrame decoded;
	ResourceXDecodedFrame sentinel;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len;

	memset(&sentinel, 0xa5, sizeof(sentinel));
	decoded = sentinel;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N,
		&frame, bytes, sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);

	frame.common.logical_assertion.requester_node = -1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X,
		&frame, bytes, sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
	frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame,
		bytes, sizeof(bytes), &encoded_len, &reject));
	bytes[20] ^= 0x01;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes,
		encoded_len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_CRC);
	UT_ASSERT(memcmp(&decoded, &sentinel, sizeof(decoded)) == 0);

	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes,
		RESOURCE_X_CONTROL_V1_BYTES - 1, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
}

static ResourceXDecodedFrame
make_typed_frame(ResourceXWireKind kind)
{
	ResourceXDecodedFrame frame = make_control_frame(kind);
	int i;

	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	if (kind == RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION) {
		frame.body.local_proof.local_holder_authority_generation = 71;
		frame.body.local_proof.requester_target_generation
			= frame.common.assertion_sequence;
		frame.body.local_proof.page_scn_lsn = 73;
		frame.body.local_proof.dependency_count = 2;
		frame.body.local_proof.dependency_vector_crc32c = UINT32_C(0x01020304);
		frame.body.local_proof.page_checksum = UINT32_C(0x11121314);
		frame.body.local_proof.local_image_proof_crc32c = UINT32_C(0x21222324);
		frame.body.local_proof.requester_connection_generation = 74;
		frame.body.local_proof.local_proof_generation = 75;
	} else if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N
			   || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			   || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2) {
		frame.blocked_has_remote_proof = true;
		frame.common.action_node = 7;
		frame.common.observed_mode
			= kind == RESOURCE_X_WIRE_BLOCKED_TO_N
				? PCM_STATE_X : PCM_STATE_N;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.flags
			= kind == RESOURCE_X_WIRE_BLOCKED_TO_N
				? RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED : 0;
		if (kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			|| kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2) {
			frame.common.ordered_lane = 0;
			frame.common.source_candidate = 1;
			frame.common.retain_pi_if_dirty = 1;
			frame.common.outcome
				= kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
					? RESOURCE_X_OUTCOME_NONE : RESOURCE_X_OUTCOME_OK;
		}
		for (i = 0; i < RESOURCE_X_SOURCE_FENCE_BYTES; i++)
			frame.body.blocked_to_n.source_fence[i] = (uint8)(i + 1);
		frame.body.blocked_to_n.source_carrier_generation = 81;
		frame.body.blocked_to_n.requester_target_generation
			= frame.common.assertion_sequence;
		frame.body.blocked_to_n.page_scn_lsn = 83;
		frame.body.blocked_to_n.dependency_count = 2;
		frame.body.blocked_to_n.dependencies[0] = 84;
		frame.body.blocked_to_n.dependencies[1] = 85;
		frame.body.blocked_to_n.source_proof_crc32c = UINT32_C(0x31323334);
		frame.body.blocked_to_n.page_checksum = UINT32_C(0x41424344);
		frame.body.blocked_to_n.source_disposition
			= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		frame.body.blocked_to_n.holder_connection_generation = 86;
		frame.body.blocked_to_n.acting_formation
			= frame.common.resource_formation;
	} else if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT) {
		frame.common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED;
		for (i = 0; i < RESOURCE_X_SOURCE_FENCE_BYTES; i++)
			frame.body.authority_grant.source_fence[i] = (uint8)(0x40 + i);
		frame.body.authority_grant.final_authority_generation
			= frame.common.authority_generation;
		frame.body.authority_grant.source_carrier_generation = 92;
		frame.body.authority_grant.requester_target_generation
			= frame.common.assertion_sequence;
		frame.body.authority_grant.page_scn_lsn = 94;
		frame.body.authority_grant.dependency_count = 2;
		frame.body.authority_grant.dependencies[0] = 95;
		frame.body.authority_grant.dependencies[1] = 96;
		frame.body.authority_grant.source_proof_crc32c = UINT32_C(0x51525354);
		frame.body.authority_grant.page_checksum = UINT32_C(0x61626364);
		frame.body.authority_grant.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		frame.body.authority_grant.source_disposition
			= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.authority_grant.requester_connection_generation = 97;
	} else if (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		frame.common.action_node = 7;
		for (i = 0; i < RESOURCE_X_REQUEST_TAIL_BYTES; i++)
			frame.body.image_envelope.request_tail[i] = (uint8)(0x20 + i);
		frame.body.image_envelope.conversion_base_generation
			= frame.common.base_authority_generation;
		for (i = 0; i < RESOURCE_X_SOURCE_FENCE_BYTES; i++)
			frame.body.image_envelope.source_fence[i] = (uint8)(0x60 + i);
		frame.body.image_envelope.source_carrier_generation = 102;
		frame.body.image_envelope.requester_target_generation
			= frame.common.assertion_sequence;
		frame.body.image_envelope.page_scn_lsn = 104;
		frame.body.image_envelope.dependency_count = 2;
		frame.body.image_envelope.dependencies[0] = 105;
		frame.body.image_envelope.dependencies[1] = 106;
		frame.body.image_envelope.dependency_vector_crc32c = UINT32_C(0x71727374);
		frame.body.image_envelope.page_checksum = UINT32_C(0x81828384);
		frame.body.image_envelope.image_length = RESOURCE_X_PAGE_BYTES;
		frame.body.image_envelope.source_disposition
			= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.image_envelope.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		for (i = 0; i < RESOURCE_X_PAGE_BYTES; i++)
			frame.body.image_envelope.page_bytes[i] = (uint8)(i * 37);
	} else if (kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT) {
		frame.body.install_settlement.conversion_base_generation
			= frame.common.base_authority_generation;
		frame.body.install_settlement.final_authority_generation
			= frame.common.authority_generation;
		frame.body.install_settlement.requester_connection_generation = 113;
		frame.body.install_settlement.requester_target_generation
			= frame.common.assertion_sequence;
		frame.body.install_settlement.page_scn_lsn = 115;
		frame.body.install_settlement.page_checksum = UINT32_C(0x91929394);
		frame.body.install_settlement.source_proof_crc32c = UINT32_C(0xa1a2a3a4);
		frame.body.install_settlement.installed_mode = PCM_STATE_X;
		frame.body.install_settlement.requester_role
			= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
		frame.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.install_settlement.terminal_state
			= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	}
	return frame;
}

static void
assert_source_settlement_encode_rejected(uint8 msg_type,
									 ResourceXDecodedFrame *frame)
{
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(!cluster_resource_x_wire_encode(
		msg_type, frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(reject != RESOURCE_X_WIRE_REJECT_NONE);
}

UT_TEST(test_source_settlement_request_and_ack_are_direction_exact)
{
	ResourceXDecodedFrame request
		= make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	ResourceXDecodedFrame ack
		= make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCK_TO_N, &request, bytes, sizeof(bytes),
		&len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	UT_ASSERT_EQ(decoded.common.action_node, 7);
	UT_ASSERT_EQ(decoded.common.authority_generation,
		request.common.authority_generation);
	UT_ASSERT_EQ(decoded.body.blocked_to_n.source_carrier_generation,
		request.body.blocked_to_n.source_carrier_generation);
	UT_ASSERT(!cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, &decoded, &reject));

	UT_ASSERT(cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_BLOCKED_TO_N, &ack, bytes, sizeof(bytes),
		&len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT(!cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
}

UT_TEST(test_source_settlement_truth_tables_fail_closed)
{
	ResourceXDecodedFrame frame
		= make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);

	frame.common.ordered_lane = 1;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.authority_generation = 0;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.observed_mode = PCM_STATE_X;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.target_mode = PCM_STATE_X;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.source_candidate = 0;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.retain_pi_if_dirty = 0;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	frame.common.outcome = RESOURCE_X_OUTCOME_NONE;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCKED_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	frame.body.blocked_to_n.source_carrier_generation = 0;
	assert_source_settlement_encode_rejected(
		RESOURCE_X_MSG_BLOCKED_TO_N, &frame);
}

UT_TEST(test_short_typed_frames_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = {
		{RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION},
		{RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		 RESOURCE_X_WIRE_INSTALL_SETTLEMENT}
	};
	uint8 bytes[RESOURCE_X_SHORT_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_typed_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame,
			bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT_EQ(len, RESOURCE_X_SHORT_V1_BYTES);
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes,
			len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.kind, cases[i].kind);
	}
}

UT_TEST(test_proof_typed_frames_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = {
		{RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N},
		{RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_WIRE_AUTHORITY_GRANT}
	};
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_typed_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame,
			bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes,
			len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.kind, cases[i].kind);
		if (cases[i].kind == RESOURCE_X_WIRE_BLOCKED_TO_N) {
			UT_ASSERT(decoded.blocked_has_remote_proof);
			UT_ASSERT_EQ(decoded.body.blocked_to_n.dependencies[1], 85);
		} else
			UT_ASSERT_EQ(decoded.body.authority_grant.dependencies[1], 96);
	}
}

UT_TEST(test_image_envelope_round_trip_preserves_exact_page)
{
	ResourceXDecodedFrame frame = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 len;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_IMAGE_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.body.image_envelope.image_length,
		RESOURCE_X_PAGE_BYTES);
	UT_ASSERT(memcmp(decoded.body.image_envelope.page_bytes,
		frame.body.image_envelope.page_bytes, RESOURCE_X_PAGE_BYTES) == 0);
}

UT_TEST(test_delegated_block_and_image_keep_exact_wire_layout)
{
	ResourceXWireKind kinds[] = { RESOURCE_X_WIRE_BLOCK_TO_N, RESOURCE_X_WIRE_IMAGE_ENVELOPE };
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(kinds); i++) {
		ResourceXDecodedFrame frame = kinds[i] == RESOURCE_X_WIRE_BLOCK_TO_N
										  ? make_control_frame(kinds[i])
										  : make_typed_frame(kinds[i]);
		ResourceXDecodedFrame decoded = { 0 };
		ResourceXWireReject reject;
		uint16 len = 0;
		uint8 msg = kinds[i] == RESOURCE_X_WIRE_BLOCK_TO_N ? RESOURCE_X_MSG_BLOCK_TO_N
														   : RESOURCE_X_MSG_IMAGE_OR_GRANT;

		frame.common.flags = UINT8_C(0x04);
		frame.common.observed_mode = PCM_STATE_X;
		if (i != 0)
			frame.common.action_node = frame.common.logical_assertion.requester_node;
		frame.common.authority_generation = frame.common.base_authority_generation + 2;
		UT_ASSERT(cluster_resource_x_wire_encode(msg, &frame, bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT_EQ(len, i == 0 ? RESOURCE_X_CONTROL_V1_BYTES : RESOURCE_X_IMAGE_V1_BYTES);
		UT_ASSERT(cluster_resource_x_wire_decode(msg, bytes, len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.common.flags, UINT8_C(0x04));
		UT_ASSERT_EQ(decoded.common.authority_generation, frame.common.authority_generation);
		/* A selected SCUR source is also legal, after the master barrier. */
		frame.common.observed_mode = PCM_STATE_S;
		UT_ASSERT(cluster_resource_x_wire_encode(msg, &frame, bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(msg, bytes, len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	}
}

UT_TEST(test_delegation_flag_never_grants_other_kind_or_nonsource)
{
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 len;

	frame = make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	frame.common.flags = UINT8_C(0x04);
	frame.common.observed_mode = PCM_STATE_S;
	frame.common.source_candidate = frame.common.retain_pi_if_dirty = 0;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											  sizeof(bytes), &len, &reject));
	frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	frame.common.flags |= UINT8_C(0x04);
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											  sizeof(bytes), &len, &reject));
	frame = make_typed_frame(RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	frame.common.flags = UINT8_C(0x04);
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &frame, bytes,
											  sizeof(bytes), &len, &reject));
}

UT_TEST(test_delegation_generations_rejected_even_with_valid_crc)
{
	ResourceXDecodedFrame frame = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 len;
	uint64 finals[] = { 0, frame.common.base_authority_generation, UINT64_MAX };
	Size i;

	frame.common.flags = UINT8_C(0x04);
	frame.common.observed_mode = PCM_STATE_X;
	frame.common.action_node = frame.common.logical_assertion.requester_node;
	for (i = 0; i < lengthof(finals); i++) {
		frame.common.authority_generation = finals[i];
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
												  sizeof(bytes), &len, &reject));
	}
	frame.common.authority_generation = frame.common.base_authority_generation + 2;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											 sizeof(bytes), &len, &reject));
	memcpy(bytes + 88, bytes + 32, 8); /* valid CRC, F == B is still no delegation */
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
}

UT_TEST(test_typed_body_validation_is_fail_closed)
{
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 len;

	frame = make_typed_frame(RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION);
	frame.body.local_proof.dependency_count = RESOURCE_X_DEPENDENCY_MAX + 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame,
		bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_BLOCKED_TO_N);
	frame.body.blocked_to_n.dependencies[2] = 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	frame.body.authority_grant.proof_kind = 0;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	frame.body.image_envelope.image_length--;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	frame.body.authority_grant.requester_target_generation++;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);
}

static void
test_put_u32_be(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)(value >> 24);
	bytes[1] = (uint8)(value >> 16);
	bytes[2] = (uint8)(value >> 8);
	bytes[3] = (uint8)value;
}

static void
test_reseal(uint8 *bytes, uint16 len)
{
	static const uint8 zero_crc[4] = {0, 0, 0, 0};
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 4);
	COMP_CRC32C(crc, zero_crc, sizeof(zero_crc));
	COMP_CRC32C(crc, bytes + 8, len - 8);
	FIN_CRC32C(crc);
	test_put_u32_be(bytes + 4, (uint32)crc);
}

UT_TEST(test_ingress_semantic_mutations_with_valid_crc_are_rejected)
{
	ResourceXDecodedFrame frame
		= make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 original[RESOURCE_X_PROOF_V1_BYTES];
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&frame, original, sizeof(original), &len, &reject));

	memcpy(bytes, original, len);
	bytes[0]++;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_VERSION);

	memcpy(bytes, original, len);
	bytes[3]--;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_DECLARED_LENGTH);

	memcpy(bytes, original, len);
	test_put_u32_be(bytes + 68, UINT32_MAX);
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_ROLE);

	memcpy(bytes, original, len);
	test_put_u32_be(bytes + 20, MAX_FORKNUM + 1);
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);

	memcpy(bytes, original, len);
	bytes[72] = PCM_STATE_X + 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_ENUM);

	memcpy(bytes, original, len);
	bytes[81] |= 0x80;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_FLAGS);

	memcpy(bytes, original, len);
	bytes[82] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_RESERVED);

	memcpy(bytes, original, len);
	bytes[300] = 0;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	memcpy(bytes, original, len);
	bytes[180] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	memcpy(bytes, original, len);
	bytes[302] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);
}

UT_TEST(test_resource_x_capability_has_complete_collision_census)
{
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
		UINT32_C(0x00020000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1,
		UINT32_C(0x00400000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_COUNT, 21);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_MASK, UINT32_C(0x007BBFFF));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_SUM,
		PGRAC_IC_HELLO_CAP_DEFINED_MASK);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_MASK & UINT32_C(0x00044000), 0);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_wire_kind_and_proof_domains_are_closed);
	UT_RUN(test_reused_message_numbers_remain_exact);
	UT_RUN(test_common_wire_layout_is_exact);
	UT_RUN(test_local_proof_layout_is_exact);
	UT_RUN(test_blocked_to_n_layout_is_exact);
	UT_RUN(test_authority_grant_layout_is_exact);
	UT_RUN(test_image_envelope_layout_is_exact);
	UT_RUN(test_install_settlement_layout_is_exact);
	UT_RUN(test_preassert_bootstrap_request_and_ack_are_direction_exact);
	UT_RUN(test_preassert_bootstrap_truth_tables_fail_closed);
	UT_RUN(test_preassert_bootstrap_decode_rejects_reserved_length_and_pair_drift);
	UT_RUN(test_control_codec_is_network_order_and_crc_exact);
	UT_RUN(test_physical_sender_generation_rebind_preserves_block_semantics);
	UT_RUN(test_shared_s_carrier_block_to_n_round_trip_is_exact);
	UT_RUN(test_shared_s_carrier_proof_and_image_round_trip_preserve_mode);
	UT_RUN(test_block_to_n_source_polarity_truth_table_is_closed);
	UT_RUN(test_all_control_kind_type_pairs_round_trip);
	UT_RUN(test_control_codec_rejects_pair_identity_crc_and_legacy_length);
	UT_RUN(test_short_typed_frames_round_trip);
	UT_RUN(test_proof_typed_frames_round_trip);
	UT_RUN(test_image_envelope_round_trip_preserves_exact_page);
	UT_RUN(test_delegated_block_and_image_keep_exact_wire_layout);
	UT_RUN(test_delegation_flag_never_grants_other_kind_or_nonsource);
	UT_RUN(test_delegation_generations_rejected_even_with_valid_crc);
	UT_RUN(test_typed_body_validation_is_fail_closed);
	UT_RUN(test_ingress_semantic_mutations_with_valid_crc_are_rejected);
	UT_RUN(test_resource_x_capability_has_complete_collision_census);
	UT_RUN(test_source_settlement_request_and_ack_are_direction_exact);
	UT_RUN(test_source_settlement_truth_tables_fail_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
