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
#include "storage/bufpage.h"

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
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
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
	UT_ASSERT_EQ(RESOURCE_X_WIRE_KIND_MAX, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_KIND_MIN, RESOURCE_X_PROOF_REMOTE_CARRIER);
	UT_ASSERT_EQ(RESOURCE_X_PROOF_KIND_MAX, RESOURCE_X_PROOF_DURABLE_STORAGE);
}

UT_TEST(test_reused_message_numbers_remain_exact)
{
	UT_ASSERT_EQ(RESOURCE_X_MSG_ASSERT_X, PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ(RESOURCE_X_MSG_IMAGE_OR_GRANT, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(RESOURCE_X_MSG_BLOCK_TO_N, PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE);
	UT_ASSERT_EQ(RESOURCE_X_MSG_BLOCKED_TO_N, PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
	UT_ASSERT_EQ(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, PGRAC_IC_MSG_GCS_BLOCK_DONE);
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
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, local_holder_authority_generation), 96);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, requester_target_generation), 104);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, page_scn_lsn), 112);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, dependency_count), 120);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, dependency_vector_crc32c), 124);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, page_checksum), 128);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, local_image_proof_crc32c), 132);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, requester_connection_generation), 136);
	UT_ASSERT_EQ(offsetof(ResourceXLocalProofDeclarationV1, local_proof_generation), 144);
}

UT_TEST(test_blocked_to_n_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXBlockedToNProofV1), 312);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_fence), 96);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_carrier_generation), 130);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, requester_target_generation), 138);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, page_scn_lsn), 146);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, dependency_count), 154);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, dependencies), 156);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_proof_crc32c), 284);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, page_checksum), 288);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, source_disposition), 292);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, proof_kind), 293);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, proof_flags), 294);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, holder_connection_generation), 296);
	UT_ASSERT_EQ(offsetof(ResourceXBlockedToNProofV1, acting_formation), 304);
}

UT_TEST(test_authority_grant_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXAuthorityGrantV1), 312);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_fence), 96);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, final_authority_generation), 130);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_carrier_generation), 138);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, requester_target_generation), 146);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, page_scn_lsn), 154);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, dependency_count), 162);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, dependencies), 164);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_proof_crc32c), 292);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, page_checksum), 296);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, proof_kind), 300);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, source_disposition), 301);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, grant_flags), 302);
	UT_ASSERT_EQ(offsetof(ResourceXAuthorityGrantV1, requester_connection_generation), 304);
}

UT_TEST(test_image_envelope_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXImageEnvelopeV2), 8752);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, page_wal), 8520);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, request_tail), 96);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, conversion_base_generation), 116);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, source_fence), 124);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, source_carrier_generation), 158);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, requester_target_generation), 166);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, page_scn_lsn), 174);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, dependency_count), 182);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, dependencies), 184);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, dependency_vector_crc32c), 312);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, page_checksum), 316);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, image_length), 320);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, source_disposition), 324);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, proof_kind), 325);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, image_flags), 326);
	UT_ASSERT_EQ(offsetof(ResourceXImageEnvelopeV2, page_bytes), 328);
	UT_ASSERT_EQ(sizeof(((ResourceXImageEnvelopeV2 *)0)->page_bytes), BLCKSZ);
}

UT_TEST(test_install_settlement_layout_is_exact)
{
	UT_ASSERT_EQ(sizeof(ResourceXInstallSettlementV1), 152);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, conversion_base_generation), 96);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, final_authority_generation), 104);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, requester_connection_generation), 112);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, requester_target_generation), 120);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, page_scn_lsn), 128);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, page_checksum), 136);
	UT_ASSERT_EQ(offsetof(ResourceXInstallSettlementV1, source_proof_crc32c), 140);
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
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);

	frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	frame.common.base_authority_generation = ack ? UINT64_C(0x0102030405060708) : 0;
	frame.common.authority_generation = 0;
	frame.common.ordered_lane = 0;
	frame.common.observed_mode = PCM_STATE_N;
	frame.common.target_mode = PCM_STATE_X;
	frame.common.source_candidate = 0;
	frame.common.retain_pi_if_dirty = 0;
	frame.common.outcome = ack ? RESOURCE_X_OUTCOME_OK : RESOURCE_X_OUTCOME_NONE;
	frame.common.flags = 0;
	return frame;
}

static void
assert_bootstrap_encode_rejected(uint8 msg_type, ResourceXDecodedFrame *frame)
{
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(
		!cluster_resource_x_wire_encode(msg_type, frame, bytes, sizeof(bytes), &len, &reject));
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

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &request, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT_EQ(bytes[1], RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP);
	UT_ASSERT(
		cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_NONE);

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &ack, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											 &reject));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, UINT64_C(0x0102030405060708));
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);

	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, &request);
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &ack);
}

UT_TEST(test_remote_admission_is_only_a_zero_base_request_flag)
{
	ResourceXDecodedFrame request = make_bootstrap_frame(false);
	ResourceXDecodedFrame decoded;
	ResourceXDecodedFrame wrong;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 len = 0;

	request.common.flags = UINT8_C(0x08);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &request, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(
		cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.flags, UINT8_C(0x08));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, 0);
	UT_ASSERT_EQ(decoded.common.authority_generation, 0);
	wrong = make_bootstrap_frame(true);
	wrong.common.flags = UINT8_C(0x08);
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, &wrong);
	wrong = request;
	wrong.common.flags |= RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &wrong);
	wrong = request;
	wrong.common.base_authority_generation = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &wrong);
	wrong = request;
	wrong.kind = RESOURCE_X_WIRE_ASSERT_X;
	wrong.common.base_authority_generation = 1;
	wrong.common.authority_generation = 1;
	assert_bootstrap_encode_rejected(RESOURCE_X_MSG_ASSERT_X, &wrong);
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

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &request, bytes,
											 sizeof(bytes), &len, &reject));
	bytes[82] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(
		!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, len - 1, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
	UT_ASSERT(
		!cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
}

UT_TEST(test_control_codec_is_network_order_and_crc_exact)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, bytes, sizeof(bytes),
											 &encoded_len, &reject));
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
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, encoded_len, &decoded,
											 &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT_EQ(decoded.payload_bytes, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
										 &frame.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, frame.common.base_authority_generation);
	UT_ASSERT_EQ(decoded.common.ordered_lane, frame.common.ordered_lane);
	UT_ASSERT_EQ(decoded.common.authority_generation, frame.common.authority_generation);
}

UT_TEST(test_physical_sender_generation_rebind_preserves_block_semantics)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											 sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_BLOCK_TO_N, bytes,
															   encoded_len, 77, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len,
											 &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, 77);
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_BLOCK_TO_N);
	UT_ASSERT(resource_x_assertion_equal(&decoded.common.logical_assertion,
										 &frame.common.logical_assertion));
	UT_ASSERT_EQ(decoded.common.base_authority_generation, frame.common.base_authority_generation);
	UT_ASSERT_EQ(decoded.common.assertion_sequence, frame.common.assertion_sequence);
	UT_ASSERT_EQ(decoded.common.action_node, frame.common.action_node);
	UT_ASSERT_EQ(decoded.common.observed_mode, frame.common.observed_mode);
	UT_ASSERT_EQ(decoded.common.target_mode, frame.common.target_mode);
	UT_ASSERT_EQ(decoded.common.source_candidate, frame.common.source_candidate);
	UT_ASSERT_EQ(decoded.common.retain_pi_if_dirty, frame.common.retain_pi_if_dirty);
	UT_ASSERT_EQ(decoded.common.authority_generation, frame.common.authority_generation);
	UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_BLOCK_TO_N, bytes,
																encoded_len, 0, &reject));
}

UT_TEST(test_shared_s_carrier_block_to_n_round_trip_is_exact)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	frame.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											 sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_CONTROL_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, encoded_len,
											 &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT_EQ(decoded.common.source_candidate, 1);
	UT_ASSERT_EQ(decoded.common.retain_pi_if_dirty, 1);
}

UT_TEST(test_shared_s_carrier_proof_and_image_round_trip_preserve_mode)
{
	ResourceXDecodedFrame proof = make_typed_frame(RESOURCE_X_WIRE_BLOCKED_TO_N);
	ResourceXDecodedFrame image = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
	uint16 encoded_len = 0;

	proof.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &proof, bytes,
											 sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, encoded_len,
											 &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_N);
	UT_ASSERT(decoded.blocked_has_remote_proof);

	image.common.observed_mode = PCM_STATE_S;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &image, bytes,
											 sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(encoded_len, RESOURCE_X_IMAGE_V2_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, encoded_len,
											 &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.observed_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_X);
}

UT_TEST(test_block_to_n_source_polarity_truth_table_is_closed)
{
	ResourceXDecodedFrame frame = make_control_frame(RESOURCE_X_WIRE_BLOCK_TO_N);
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 encoded_len = 0;

	frame.common.observed_mode = PCM_STATE_S;
	frame.common.source_candidate = 1;
	frame.common.retain_pi_if_dirty = 0;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											  sizeof(bytes), &encoded_len, &reject));

	frame.common.source_candidate = 0;
	frame.common.retain_pi_if_dirty = 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											  sizeof(bytes), &encoded_len, &reject));
}

UT_TEST(test_all_control_kind_type_pairs_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = { { RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_ASSERT_X },
				  { RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_WIRE_BLOCK_TO_N },
				  { RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N },
				  { RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_WIRE_RELEASE_X } };
	uint8 bytes[RESOURCE_X_CONTROL_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_control_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 encoded_len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame, bytes, sizeof(bytes),
												 &encoded_len, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes, encoded_len, &decoded,
												 &reject));
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
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &frame, bytes,
											  sizeof(bytes), &encoded_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);

	frame.common.logical_assertion.requester_node = -1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, bytes, sizeof(bytes),
											  &encoded_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
	frame = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, bytes, sizeof(bytes),
											 &encoded_len, &reject));
	bytes[20] ^= 0x01;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, encoded_len, &decoded,
											  &reject));
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
		frame.body.local_proof.requester_target_generation = frame.common.assertion_sequence;
		frame.body.local_proof.page_scn_lsn = 73;
		frame.body.local_proof.dependency_count = 2;
		frame.body.local_proof.dependency_vector_crc32c = UINT32_C(0x01020304);
		frame.body.local_proof.page_checksum = UINT32_C(0x11121314);
		frame.body.local_proof.local_image_proof_crc32c = UINT32_C(0x21222324);
		frame.body.local_proof.requester_connection_generation = 74;
		frame.body.local_proof.local_proof_generation = 75;
	} else if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			   || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2) {
		frame.blocked_has_remote_proof = true;
		frame.common.action_node = 7;
		frame.common.observed_mode
			= kind == RESOURCE_X_WIRE_BLOCKED_TO_N ? PCM_STATE_X : PCM_STATE_N;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.flags
			= kind == RESOURCE_X_WIRE_BLOCKED_TO_N ? RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED : 0;
		if (kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			|| kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2) {
			frame.common.ordered_lane = 0;
			frame.common.source_candidate = 1;
			frame.common.retain_pi_if_dirty = 1;
			frame.common.outcome = kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
									   ? RESOURCE_X_OUTCOME_NONE
									   : RESOURCE_X_OUTCOME_OK;
		}
		for (i = 0; i < RESOURCE_X_SOURCE_FENCE_BYTES; i++)
			frame.body.blocked_to_n.source_fence[i] = (uint8)(i + 1);
		frame.body.blocked_to_n.source_carrier_generation = 81;
		frame.body.blocked_to_n.requester_target_generation = frame.common.assertion_sequence;
		frame.body.blocked_to_n.page_scn_lsn = 83;
		frame.body.blocked_to_n.dependency_count = 2;
		frame.body.blocked_to_n.dependencies[0] = 84;
		frame.body.blocked_to_n.dependencies[1] = 85;
		frame.body.blocked_to_n.source_proof_crc32c = UINT32_C(0x31323334);
		frame.body.blocked_to_n.page_checksum = UINT32_C(0x41424344);
		frame.body.blocked_to_n.source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		frame.body.blocked_to_n.holder_connection_generation = 86;
		frame.body.blocked_to_n.acting_formation = frame.common.resource_formation;
	} else if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT) {
		frame.common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED;
		for (i = 0; i < RESOURCE_X_SOURCE_FENCE_BYTES; i++)
			frame.body.authority_grant.source_fence[i] = (uint8)(0x40 + i);
		frame.body.authority_grant.final_authority_generation = frame.common.authority_generation;
		frame.body.authority_grant.source_carrier_generation = 92;
		frame.body.authority_grant.requester_target_generation = frame.common.assertion_sequence;
		frame.body.authority_grant.page_scn_lsn = 94;
		frame.body.authority_grant.dependency_count = 2;
		frame.body.authority_grant.dependencies[0] = 95;
		frame.body.authority_grant.dependencies[1] = 96;
		frame.body.authority_grant.source_proof_crc32c = UINT32_C(0x51525354);
		frame.body.authority_grant.page_checksum = UINT32_C(0x61626364);
		frame.body.authority_grant.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		frame.body.authority_grant.source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
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
		frame.body.image_envelope.requester_target_generation = frame.common.assertion_sequence;
		frame.body.image_envelope.page_scn_lsn = 104;
		frame.body.image_envelope.dependency_count = 2;
		frame.body.image_envelope.dependencies[0] = 105;
		frame.body.image_envelope.dependencies[1] = 106;
		frame.body.image_envelope.dependency_vector_crc32c = UINT32_C(0x71727374);
		frame.body.image_envelope.page_checksum = UINT32_C(0x81828384);
		frame.body.image_envelope.image_length = RESOURCE_X_PAGE_BYTES;
		frame.body.image_envelope.source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.image_envelope.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		for (i = 0; i < RESOURCE_X_PAGE_BYTES; i++)
			frame.body.image_envelope.page_bytes[i] = (uint8)(i * 37);
	} else if (kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT) {
		frame.body.install_settlement.conversion_base_generation
			= frame.common.base_authority_generation;
		frame.body.install_settlement.final_authority_generation
			= frame.common.authority_generation;
		frame.body.install_settlement.requester_connection_generation = 113;
		frame.body.install_settlement.requester_target_generation = frame.common.assertion_sequence;
		frame.body.install_settlement.page_scn_lsn = 115;
		frame.body.install_settlement.page_checksum = UINT32_C(0x91929394);
		frame.body.install_settlement.source_proof_crc32c = UINT32_C(0xa1a2a3a4);
		frame.body.install_settlement.installed_mode = PCM_STATE_X;
		frame.body.install_settlement.requester_role = RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
		frame.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.install_settlement.terminal_state = RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	}
	return frame;
}

static void
assert_source_settlement_encode_rejected(uint8 msg_type, ResourceXDecodedFrame *frame)
{
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(
		!cluster_resource_x_wire_encode(msg_type, frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(reject != RESOURCE_X_WIRE_REJECT_NONE);
}

UT_TEST(test_source_settlement_request_and_ack_are_direction_exact)
{
	ResourceXDecodedFrame request = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	ResourceXDecodedFrame ack = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len = 0;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &request, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(
		cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	UT_ASSERT_EQ(decoded.common.action_node, 7);
	UT_ASSERT_EQ(decoded.common.authority_generation, request.common.authority_generation);
	UT_ASSERT_EQ(decoded.body.blocked_to_n.source_carrier_generation,
				 request.body.blocked_to_n.source_carrier_generation);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, &decoded,
											  &reject));

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &ack, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
	UT_ASSERT(
		cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, &decoded, &reject));
	UT_ASSERT_EQ(decoded.kind, RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	UT_ASSERT_EQ(decoded.common.outcome, RESOURCE_X_OUTCOME_OK);
	UT_ASSERT(
		!cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, len, &decoded, &reject));
}

UT_TEST(test_source_settlement_truth_tables_fail_closed)
{
	ResourceXDecodedFrame frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);

	frame.common.ordered_lane = 1;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.authority_generation = 0;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.observed_mode = PCM_STATE_X;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.target_mode = PCM_STATE_X;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.source_candidate = 0;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.retain_pi_if_dirty = 0;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2);
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCK_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	frame.common.outcome = RESOURCE_X_OUTCOME_NONE;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, &frame);
	frame = make_typed_frame(RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2);
	frame.body.blocked_to_n.source_carrier_generation = 0;
	assert_source_settlement_encode_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, &frame);
}

UT_TEST(test_short_typed_frames_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = { { RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION },
				  { RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_WIRE_INSTALL_SETTLEMENT } };
	uint8 bytes[RESOURCE_X_SHORT_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_typed_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame, bytes, sizeof(bytes),
												 &len, &reject));
		UT_ASSERT_EQ(len, RESOURCE_X_SHORT_V1_BYTES);
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes, len, &decoded, &reject));
		UT_ASSERT_EQ(decoded.kind, cases[i].kind);
	}
}

UT_TEST(test_proof_typed_frames_round_trip)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
	} cases[] = { { RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N },
				  { RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_WIRE_AUTHORITY_GRANT } };
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		ResourceXDecodedFrame frame = make_typed_frame(cases[i].kind);
		ResourceXDecodedFrame decoded;
		ResourceXWireReject reject;
		uint16 len;

		UT_ASSERT(cluster_resource_x_wire_encode(cases[i].msg_type, &frame, bytes, sizeof(bytes),
												 &len, &reject));
		UT_ASSERT_EQ(len, RESOURCE_X_PROOF_V1_BYTES);
		UT_ASSERT(cluster_resource_x_wire_decode(cases[i].msg_type, bytes, len, &decoded, &reject));
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
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
	uint16 len;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_IMAGE_V2_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											 &reject));
	UT_ASSERT_EQ(decoded.body.image_envelope.image_length, RESOURCE_X_PAGE_BYTES);
	UT_ASSERT(memcmp(decoded.body.image_envelope.page_bytes, frame.body.image_envelope.page_bytes,
					 RESOURCE_X_PAGE_BYTES)
			  == 0);
}

static ResourceXDecodedFrame
make_wal_image(void)
{
	ResourceXDecodedFrame frame = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ClusterPageWalBindingV1 *wal = &frame.body.image_envelope.page_wal;
	BufferTag *tag = &frame.common.logical_assertion.resource;
	PGAlignedBlock page;
	memset(&page, 0, sizeof(page));
	((PageHeader)page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)page.data)->pd_upper = BLCKSZ;
	((PageHeader)page.data)->pd_special = BLCKSZ;
	((PageHeader)page.data)->pd_block_scn = 41;
	PageSetLSNPreserveOrigin(page.data, 0x800);
	UT_ASSERT(PageSetLSNOrigin(page.data, 1));
	memcpy(frame.body.image_envelope.page_bytes, page.data, BLCKSZ);
	frame.body.image_envelope.page_scn_lsn = 41;
	frame.body.image_envelope.image_flags = RESOURCE_X_IMAGE_HAS_WAL;
	tag->forkNum = MAIN_FORKNUM;
	wal->source.claim.identity.system_identifier = 17;
	wal->source.claim.identity.storage_uuid[0] = 3;
	wal->source.claim.identity.authority_uuid[0] = 5;
	wal->source.claim.identity.origin_thread_id = 2;
	wal->source.claim.identity.origin_node_id = 1;
	wal->source.claim.identity.thread_claim_created_at = 100;
	wal->source.claim.identity.thread_claim_crc32c = 0x1234;
	wal->source.claim.identity.origin_owner_incarnation = 4;
	wal->source.claim.identity.root_lineage_seq = 7;
	wal->source.claim.database_incarnation = 9;
	wal->source.claim.max_config_generation = 11;
	wal->source.claim.claim_sha256[0] = 13;
	wal->source.timeline = 2;
	wal->identity.system_identifier = 17;
	wal->identity.storage_uuid[0] = 3;
	wal->identity.locator = BufTagGetRelFileLocator(tag);
	wal->identity.forknum = tag->forkNum;
	wal->identity.blockno = tag->blockNum;
	wal->version.segment_incarnation[0] = 19;
	wal->version.mutation_token = 41;
	wal->record_start = 0x600;
	wal->record_end = 0x800;
	wal->record_crc = 0x11223344;
	wal->rmid = 10;
	wal->info = 0x12;
	wal->flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	return frame;
}

UT_TEST(test_image_preserves_full_native_source_and_rejects_legacy)
{
	ResourceXDecodedFrame frame = make_wal_image(), decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
	uint16 len = 0;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, 8752);
	UT_ASSERT_EQ(bytes[0], 2);
	UT_ASSERT_EQ(bytes[8520 + 224], 0x11);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											 &reject));
	UT_ASSERT_EQ(memcmp(&decoded.body.image_envelope.page_wal, &frame.body.image_envelope.page_wal,
						sizeof(ClusterPageWalBindingV1)),
				 0);
	frame.body.image_envelope.page_wal.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											 sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(bytes[8520 + 230], 0);
	UT_ASSERT_EQ(bytes[8520 + 231], CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											 &reject));
	UT_ASSERT_EQ(decoded.body.image_envelope.page_wal.flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	memset(&decoded, 0x59, sizeof(decoded));
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, 8520, &decoded,
											  &reject));
	bytes[0] = 1;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(((uint8 *)&decoded)[0], 0x59);
}

UT_TEST(test_image_source_conflicts_refuse_before_output)
{
	for (int i = 0; i < 13; i++) {
		ResourceXDecodedFrame frame = make_wal_image();
		ClusterPageWalBindingV1 *wal = &frame.body.image_envelope.page_wal;
		ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
		uint16 len = 91;
		switch (i) {
		case 0:
			wal->record_end++;
			break;
		case 1:
			wal->version.mutation_token++;
			break;
		case 2:
			wal->source.claim.identity.origin_thread_id++;
			break;
		case 3:
			wal->identity.locator.relNumber++;
			break;
		case 4:
			wal->identity.storage_uuid[0]++;
			break;
		case 5:
			memset(wal->version.segment_incarnation, 0, 16);
			break;
		case 6:
			memset(wal->source.claim.claim_sha256, 0, 32);
			break;
		case 7:
			wal->flags = 2;
			break;
		case 8:
			wal->source.claim.identity.reserved60 = 1;
			break;
		case 9:
			frame.body.image_envelope.image_flags = 0;
			break;
		case 10:
			frame.body.image_envelope.image_flags = 2;
			break;
		case 11:
			wal->record_start = wal->record_end;
			break;
		case 12:
			wal->flags = 0;
			break;
		}
		memset(bytes, 0x59, sizeof(bytes));
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
												  sizeof(bytes), &len, &reject));
		UT_ASSERT_EQ(bytes[0], 0x59);
		UT_ASSERT_EQ(len, 91);
	}
}

UT_TEST(test_image_wal_corruption_rejected_with_valid_wire_crc)
{
	const int offsets[] = { 8520 + 132, 8520 + 180, 8520 + 208, 8520 + 40, 326, 8520 + 231 };
	for (int i = 0; i < lengthof(offsets); i++) {
		ResourceXDecodedFrame frame = make_wal_image(), decoded;
		ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
		uint16 len = 0;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
												 sizeof(bytes), &len, &reject));
		bytes[offsets[i]] = offsets[i] == 8520 + 231 ? 0 : 1;
		test_reseal(bytes, len);
		memset(&decoded, 0x59, sizeof(decoded));
		UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len,
												  &decoded, &reject));
		UT_ASSERT_EQ(((uint8 *)&decoded)[0], 0x59);
	}
}

UT_TEST(test_delegated_block_and_image_keep_exact_wire_layout)
{
	ResourceXWireKind kinds[] = { RESOURCE_X_WIRE_BLOCK_TO_N, RESOURCE_X_WIRE_IMAGE_ENVELOPE };
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
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
		UT_ASSERT_EQ(len, i == 0 ? RESOURCE_X_CONTROL_V1_BYTES : RESOURCE_X_IMAGE_V2_BYTES);
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
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
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
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
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
	uint8 bytes[RESOURCE_X_IMAGE_V2_BYTES];
	uint16 len;

	frame = make_typed_frame(RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION);
	frame.body.local_proof.dependency_count = RESOURCE_X_DEPENDENCY_MAX + 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &frame, bytes, sizeof(bytes),
											  &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_BLOCKED_TO_N);
	frame.body.blocked_to_n.dependencies[2] = 1;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &frame, bytes,
											  sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	frame.body.authority_grant.proof_kind = 0;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											  sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	frame.body.image_envelope.image_length--;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											  sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	frame.body.authority_grant.requester_target_generation++;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, bytes,
											  sizeof(bytes), &len, &reject));
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
	static const uint8 zero_crc[4] = { 0, 0, 0, 0 };
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
	ResourceXDecodedFrame frame = make_typed_frame(RESOURCE_X_WIRE_AUTHORITY_GRANT);
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	uint8 original[RESOURCE_X_PROOF_V1_BYTES];
	uint8 bytes[RESOURCE_X_PROOF_V1_BYTES];
	uint16 len;

	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &frame, original,
											 sizeof(original), &len, &reject));

	memcpy(bytes, original, len);
	bytes[0]++;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_VERSION);

	memcpy(bytes, original, len);
	bytes[3]--;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_DECLARED_LENGTH);

	memcpy(bytes, original, len);
	test_put_u32_be(bytes + 68, UINT32_MAX);
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_ROLE);

	memcpy(bytes, original, len);
	test_put_u32_be(bytes + 20, MAX_FORKNUM + 1);
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);

	memcpy(bytes, original, len);
	bytes[72] = PCM_STATE_X + 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_ENUM);

	memcpy(bytes, original, len);
	bytes[81] |= 0x80;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_FLAGS);

	memcpy(bytes, original, len);
	bytes[82] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_RESERVED);

	memcpy(bytes, original, len);
	bytes[300] = 0;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	memcpy(bytes, original, len);
	bytes[180] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);

	memcpy(bytes, original, len);
	bytes[302] = 1;
	test_reseal(bytes, len);
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, len, &decoded,
											  &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_BODY);
}

UT_TEST(test_resource_x_capability_has_complete_collision_census)
{
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1, UINT32_C(0x00020000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1, UINT32_C(0x00400000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_CONFIG_MEMBERS_V3, UINT32_C(0x00800000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_KO_SHARED_V2, UINT32_C(0x01000000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2, UINT32_C(0x02000000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3, UINT32_C(0x04000000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1, UINT32_C(0x08000000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_COUNT, 27);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_MASK, UINT32_C(0x1FFBBFFF));
	/* The collision census must include the new allocation even while the
	 * runtime consumer is deliberately unadvertised. */
#ifdef PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1, UINT32_C(0x10000000));
#else
	UT_ASSERT(false);
#endif
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_SUM, PGRAC_IC_HELLO_CAP_DEFINED_MASK);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_DEFINED_MASK & UINT32_C(0x00044000), 0);
}

/* Build the acquisition ABI independently of its encoder. The old control
 * encoder supplies only the original common header; all new bytes and the
 * request-only polarity are set here before resealing the actual wire CRC. */
static void
test_put_u64_be(uint8 *bytes, uint64 value)
{
	for (int i = 0; i < 8; i++)
		bytes[i] = (uint8)(value >> ((7 - i) * 8));
}

static void
make_acquire_bytes(uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES])
{
	ResourceXDecodedFrame control = make_control_frame(RESOURCE_X_WIRE_ASSERT_X);
	ResourceXWireReject reject;
	uint16 len = 0;

	memset(bytes, 0, RESOURCE_X_ACQUIRE_V3_BYTES);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, &control, bytes,
		RESOURCE_X_ACQUIRE_V3_BYTES, &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_CONTROL_V1_BYTES);
	bytes[0] = 3;
	bytes[2] = 0;
	bytes[3] = 160;
	memset(bytes + 32, 0, 8); /* Only master selects base/final authority. */
	memset(bytes + 64, 0, 4);
	memset(bytes + 88, 0, 8);
	test_put_u64_be(bytes + 96, UINT64_C(0x7172737475767778));
	test_put_u64_be(bytes + 104, UINT64_C(0x8182838485868788));
	test_put_u64_be(bytes + 112, UINT64_C(0x9192939495969798));
	test_put_u64_be(bytes + 120, UINT64_C(0xa1a2a3a4a5a6a7a8));
	test_put_u64_be(bytes + 128, UINT64_C(0xb1b2b3b4b5b6b7b8));
	test_put_u64_be(bytes + 136, UINT64_C(0xc1c2c3c4c5c6c7c8));
	test_put_u32_be(bytes + 144, 17);
	test_put_u32_be(bytes + 148, 29);
	bytes[152] = 1;
	test_reseal(bytes, RESOURCE_X_ACQUIRE_V3_BYTES);
}

static bool
test_acquire_round_trip(uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES],
						ResourceXDecodedFrame *decoded)
{
	ResourceXWireReject reject;
	uint8 encoded[RESOURCE_X_ACQUIRE_V3_BYTES];
	uint16 len = 0;
	bool ok;

	test_reseal(bytes, RESOURCE_X_ACQUIRE_V3_BYTES);
	ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes,
		RESOURCE_X_ACQUIRE_V3_BYTES, decoded, &reject);
	UT_ASSERT(ok);
	if (!ok)
		return false;
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT(decoded->has_acquire_intent);
	UT_ASSERT_EQ(decoded->common.base_authority_generation, 0);
	UT_ASSERT_EQ(decoded->common.authority_generation, 0);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, decoded, encoded,
		sizeof(encoded), &len, &reject));
	UT_ASSERT_EQ(len, sizeof(encoded));
	UT_ASSERT(memcmp(bytes, encoded, sizeof(encoded)) == 0);
	return true;
}

static void
test_acquire_rejected(uint8 msg_type, const uint8 *bytes, uint16 len)
{
	ResourceXDecodedFrame out, original;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	memset(&out, 0xa5, sizeof(out));
	original = out;
	UT_ASSERT(!cluster_resource_x_wire_decode(msg_type, bytes, len, &out, &reject));
	UT_ASSERT_NE(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT(memcmp(&out, &original, sizeof(out)) == 0);
}

UT_TEST(test_acquire_layout_reuses_request_family_without_new_kind)
{
	UT_ASSERT_EQ(RESOURCE_X_ACQUIRE_WIRE_VERSION, 3);
	UT_ASSERT_EQ(RESOURCE_X_ACQUIRE_V3_BYTES, 160);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireRequestV3), 160);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, requester_incarnation), 96);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, backend_generation), 104);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, base_generation), 112);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, reservation_token), 120);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, cluster_epoch), 128);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, r4_generation), 136);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, backend_id), 144);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, buffer_id), 148);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, path), 152);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireRequestV3, zero_reserved), 153);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_KIND_MAX, 11);
}

UT_TEST(test_acquire_first_fill_read_and_known_write_round_trip)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded;

	for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		make_acquire_bytes(bytes);
		bytes[73] = mode;
		if (!test_acquire_round_trip(bytes, &decoded))
			return;
		UT_ASSERT_EQ(decoded.common.target_mode, mode);
		UT_ASSERT_EQ(decoded.body.acquire_intent.path, RESOURCE_X_ACQUIRE_FIRST_FILL);
		UT_ASSERT_EQ(decoded.body.acquire_intent.requester_incarnation,
			UINT64_C(0x7172737475767778));
		UT_ASSERT_EQ(decoded.body.acquire_intent.backend_generation,
			UINT64_C(0x8182838485868788));
		UT_ASSERT_EQ(decoded.body.acquire_intent.base_generation,
			UINT64_C(0x9192939495969798));
		UT_ASSERT_EQ(decoded.body.acquire_intent.reservation_token,
			UINT64_C(0xa1a2a3a4a5a6a7a8));
		UT_ASSERT_EQ(decoded.body.acquire_intent.cluster_epoch,
			UINT64_C(0xb1b2b3b4b5b6b7b8));
		UT_ASSERT_EQ(decoded.body.acquire_intent.r4_generation,
			UINT64_C(0xc1c2c3c4c5c6c7c8));
		UT_ASSERT_EQ(decoded.body.acquire_intent.backend_id, 17);
		UT_ASSERT_EQ(decoded.body.acquire_intent.buffer_id, 29);
	}
}

UT_TEST(test_acquire_resident_n_s_x_preserves_full_episode)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded;

	for (int observed = PCM_STATE_N; observed <= PCM_STATE_X; observed++)
		for (int wanted = PCM_STATE_S; wanted <= PCM_STATE_X; wanted++) {
			make_acquire_bytes(bytes);
			bytes[72] = observed;
			bytes[73] = wanted;
			bytes[152] = 2;
			if (!test_acquire_round_trip(bytes, &decoded))
				return;
			UT_ASSERT_EQ(decoded.common.observed_mode, observed);
			UT_ASSERT_EQ(decoded.common.target_mode, wanted);
			UT_ASSERT_EQ(decoded.body.acquire_intent.path, RESOURCE_X_ACQUIRE_RESIDENT);
		}
}

UT_TEST(test_acquire_every_prefix_and_extra_byte_is_rejected)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES + 1];

	memset(bytes, 0, sizeof(bytes));
	make_acquire_bytes(bytes);
	for (uint16 len = 0; len < RESOURCE_X_ACQUIRE_V3_BYTES; len++)
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, len);
	test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	bytes[3]--;
	test_reseal(bytes, RESOURCE_X_ACQUIRE_V3_BYTES);
	test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, RESOURCE_X_ACQUIRE_V3_BYTES);
}

UT_TEST(test_acquire_version_and_kind_length_pairs_are_closed)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	for (int version = 0; version <= UINT8_MAX; version++) {
		if (version == 3)
			continue;
		make_acquire_bytes(bytes);
		bytes[0] = version;
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
	for (int kind = 0; kind <= UINT8_MAX; kind++) {
		if (kind == RESOURCE_X_WIRE_ASSERT_X)
			continue;
		make_acquire_bytes(bytes);
		bytes[1] = kind;
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
}

UT_TEST(test_acquire_all_nonrequest_message_types_are_rejected)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	make_acquire_bytes(bytes);
	for (int msg = 0; msg <= UINT8_MAX; msg++)
		if (msg != RESOURCE_X_MSG_ASSERT_X)
			test_acquire_rejected((uint8)msg, bytes, sizeof(bytes));
}

UT_TEST(test_acquire_reserved_bytes_fail_with_valid_crc)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	for (int offset = 82; offset < RESOURCE_X_ACQUIRE_V3_BYTES; offset++) {
		if (offset >= 88 && offset < 153)
			continue;
		for (int bit = 0; bit < 8; bit++) {
			make_acquire_bytes(bytes);
			bytes[offset] = 1 << bit;
			test_reseal(bytes, sizeof(bytes));
			test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
		}
	}
}

UT_TEST(test_acquire_zero_or_exhausted_provenance_is_rejected)
{
	const int required64[] = { 40, 48, 56, 96, 104, 120, 128, 136 };
	const uint64 bad[] = { 0, UINT64_MAX };
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	for (unsigned i = 0; i < lengthof(required64); i++)
		for (unsigned j = 0; j < lengthof(bad); j++) {
			make_acquire_bytes(bytes);
			test_put_u64_be(bytes + required64[i], bad[j]);
			test_reseal(bytes, sizeof(bytes));
			test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
		}
	for (unsigned j = 0; j < lengthof(bad); j++) {
		make_acquire_bytes(bytes);
		test_put_u32_be(bytes + 76, (uint32)bad[j]);
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
}

UT_TEST(test_acquire_native_zero_generation_and_slot_boundaries)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded;

	make_acquire_bytes(bytes);
	test_put_u64_be(bytes + 112, 0); /* Fresh native base is legitimately zero. */
	test_put_u32_be(bytes + 144, 0); /* PGPROC and buffer indexes are zero-based. */
	test_put_u32_be(bytes + 148, 0);
	if (!test_acquire_round_trip(bytes, &decoded))
		return;
	UT_ASSERT_EQ(decoded.body.acquire_intent.base_generation, 0);
	UT_ASSERT_EQ(decoded.body.acquire_intent.backend_id, 0);
	UT_ASSERT_EQ(decoded.body.acquire_intent.buffer_id, 0);
	test_put_u32_be(bytes + 144, INT32_MAX);
	test_put_u32_be(bytes + 148, INT32_MAX);
	if (!test_acquire_round_trip(bytes, &decoded))
		return;
	for (int offset = 144; offset <= 148; offset += 4) {
		make_acquire_bytes(bytes);
		test_put_u32_be(bytes + offset, UINT32_C(0x80000000));
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
	make_acquire_bytes(bytes);
	test_put_u64_be(bytes + 112, UINT64_MAX);
	test_reseal(bytes, sizeof(bytes));
	test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
}

UT_TEST(test_acquire_cannot_claim_authority_source_or_retry_outcome)
{
	const int forbidden[] = { 32, 64, 74, 75, 80, 81, 88 };
	const int invalid_modes[][2] = { { 72, 3 }, { 73, 0 }, { 73, 3 }, { 152, 0 },
		{ 152, 3 }, { 152, 255 }, { 72, PCM_STATE_S }, { 72, PCM_STATE_X } };
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	for (unsigned i = 0; i < lengthof(forbidden); i++) {
		make_acquire_bytes(bytes);
		bytes[forbidden[i]] = 1;
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
	for (int outcome = RESOURCE_X_OUTCOME_RETRY; outcome <= RESOURCE_X_OUTCOME_CORRUPT;
		 outcome++) {
		make_acquire_bytes(bytes);
		bytes[80] = outcome;
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
	for (unsigned i = 0; i < lengthof(invalid_modes); i++) {
		make_acquire_bytes(bytes);
		bytes[invalid_modes[i][0]] = invalid_modes[i][1];
		test_reseal(bytes, sizeof(bytes));
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
	make_acquire_bytes(bytes);
	test_put_u32_be(bytes + 68, 4); /* Original requester is node3. */
	test_reseal(bytes, sizeof(bytes));
	test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
}

static void
test_acquire_encode_rejected(const ResourceXDecodedFrame *frame, uint16 capacity)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES], original[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint16 len = 0xeeee;

	memset(bytes, 0xa5, sizeof(bytes));
	memcpy(original, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, frame, bytes,
		capacity, &len, &reject));
	UT_ASSERT_NE(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT_EQ(len, 0xeeee);
	UT_ASSERT(memcmp(bytes, original, sizeof(bytes)) == 0);
}

UT_TEST(test_acquire_encoder_failure_preserves_output_and_length)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded, wrong;

	make_acquire_bytes(bytes);
	if (!test_acquire_round_trip(bytes, &decoded))
		return;
#define ACQUIRE_BAD(change) \
	do { wrong = decoded; change; test_acquire_encode_rejected(&wrong, sizeof(bytes)); } while (0)
	ACQUIRE_BAD(wrong.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP);
	ACQUIRE_BAD(wrong.has_acquire_intent = false);
	ACQUIRE_BAD(wrong.common.target_mode = PCM_STATE_N);
	ACQUIRE_BAD(wrong.common.authority_generation = 1);
	ACQUIRE_BAD(wrong.common.outcome = RESOURCE_X_OUTCOME_RETRY);
	ACQUIRE_BAD(wrong.body.acquire_intent.requester_incarnation = 0);
	ACQUIRE_BAD(wrong.body.acquire_intent.backend_generation = UINT64_MAX);
	ACQUIRE_BAD(wrong.body.acquire_intent.base_generation = UINT64_MAX);
	ACQUIRE_BAD(wrong.body.acquire_intent.reservation_token = 0);
	ACQUIRE_BAD(wrong.body.acquire_intent.cluster_epoch = 0);
	ACQUIRE_BAD(wrong.body.acquire_intent.r4_generation = 0);
	ACQUIRE_BAD(wrong.body.acquire_intent.backend_id = -1);
	ACQUIRE_BAD(wrong.body.acquire_intent.buffer_id = -1);
	ACQUIRE_BAD(wrong.body.acquire_intent.path = 0);
#undef ACQUIRE_BAD
}

UT_TEST(test_acquire_crc_failure_preserves_decoded_output)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];

	for (int offset = 0; offset < RESOURCE_X_ACQUIRE_V3_BYTES; offset++) {
		make_acquire_bytes(bytes);
		bytes[offset] ^= 1;
		test_acquire_rejected(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes));
	}
}

UT_TEST(test_acquire_sender_rebind_preserves_native_episode)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES], before[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;

	make_acquire_bytes(bytes);
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_ASSERT_X,
		bytes, sizeof(bytes), 7, &reject));
	for (int offset = 0; offset < RESOURCE_X_ACQUIRE_V3_BYTES; offset++)
		if ((offset < 4 || offset >= 8) && (offset < 76 || offset >= 80))
			UT_ASSERT_EQ(bytes[offset], before[offset]);
	if (!test_acquire_round_trip(bytes, &decoded))
		return;
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, 7);
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_ASSERT_X,
		bytes, sizeof(bytes), UINT32_MAX, &reject));
	UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
	bytes[153] = 1;
	test_reseal(bytes, sizeof(bytes));
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_ASSERT_X,
		bytes, sizeof(bytes), 9, &reject));
	UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
}

UT_TEST(test_acquire_insufficient_encode_capacity_is_atomic)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_V3_BYTES];
	ResourceXDecodedFrame decoded;

	make_acquire_bytes(bytes);
	if (!test_acquire_round_trip(bytes, &decoded))
		return;
	for (uint16 cap = 0; cap < RESOURCE_X_ACQUIRE_V3_BYTES; cap++)
		test_acquire_encode_rejected(&decoded, cap);
}

/* Construct the conditional ABI independently of the new encoder. */
static void
make_decision_bytes(uint8 *bytes, bool forward, uint8 directory_mode,
					uint32 holders, uint8 target_mode, int32 source_node,
					uint8 source_kind)
{
	uint32 required;

	memset(bytes, 0, RESOURCE_X_DECISION_V4_BYTES);
	make_acquire_bytes(bytes);
	bytes[0] = RESOURCE_X_DECISION_WIRE_VERSION;
	bytes[1] = forward ? RESOURCE_X_WIRE_BLOCK_TO_N : RESOURCE_X_WIRE_AUTHORITY_GRANT;
	bytes[2] = 2;
	bytes[3] = 224;
	test_put_u64_be(bytes + 32, 10000);
	test_put_u64_be(bytes + 88, 10001);
	bytes[73] = target_mode;
	test_put_u64_be(bytes + 160, UINT64_C(0x0102030405060708));
	test_put_u64_be(bytes + 168, UINT64_C(0x1112131415161718));
	test_put_u64_be(bytes + 176, source_node < 0 ? 0 : 55);
	test_put_u32_be(bytes + 184, 2);
	test_put_u32_be(bytes + 188, (uint32)source_node);
	test_put_u32_be(bytes + 192, holders);
	required = target_mode == PCM_STATE_X ? holders & ~UINT32_C(8)
		: directory_mode == PCM_STATE_X ? holders & ~UINT32_C(8) : 0;
	test_put_u32_be(bytes + 196, required);
	test_put_u32_be(bytes + 204, 17);
	test_put_u32_be(bytes + 208, 19);
	bytes[212] = directory_mode;
	bytes[213] = source_kind;
	for (int node = 0; node < 32; node++) {
		if (!(holders & (UINT32_C(1) << node)))
			continue;
		test_put_u64_be(bytes + 224 + node * 16,
			node == 3 ? UINT64_C(0x7172737475767778) : (uint64)(10 + node));
		test_put_u64_be(bytes + 232 + node * 16, (uint64)(50 + node));
	}
	if (forward)
		test_put_u32_be(bytes + 68, (uint32)source_node);
	test_reseal(bytes, RESOURCE_X_DECISION_V4_BYTES);
}

static bool
decision_round_trip(uint8 *bytes, bool forward, ResourceXDecodedFrame *decoded)
{
	uint8 encoded[RESOURCE_X_DECISION_V4_BYTES];
	uint8 msg = forward ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
	ResourceXWireReject reject;
	uint16 len = 0;
	bool ok;

	test_reseal(bytes, RESOURCE_X_DECISION_V4_BYTES);
	ok = cluster_resource_x_wire_decode(msg, bytes, RESOURCE_X_DECISION_V4_BYTES,
		decoded, &reject);
	UT_ASSERT(ok);
	if (!ok)
		return false;
	UT_ASSERT(decoded->has_acquire_decision);
	UT_ASSERT(!decoded->has_acquire_intent);
	UT_ASSERT(!decoded->blocked_has_remote_proof);
	UT_ASSERT_EQ(decoded->common.outcome, RESOURCE_X_OUTCOME_NONE);
	UT_ASSERT(cluster_resource_x_wire_encode(msg, decoded, encoded, sizeof(encoded), &len, &reject));
	UT_ASSERT_EQ(len, sizeof(encoded));
	UT_ASSERT(memcmp(bytes, encoded, sizeof(encoded)) == 0);
	return true;
}

static void
decision_reject_bytes(uint8 *bytes, bool forward)
{
	test_reseal(bytes, RESOURCE_X_DECISION_V4_BYTES);
	test_acquire_rejected(forward ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT,
		bytes, RESOURCE_X_DECISION_V4_BYTES);
}

UT_TEST(test_decision_layout_and_existing_families_are_exact)
{
	UT_ASSERT_EQ(RESOURCE_X_DECISION_V4_BYTES, 736);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireDecisionV4), 736);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireHolderV3), 16);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, resource_binding), 160);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, source_service_generation), 176);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, master_node), 184);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, holders_bitmap), 192);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, requester_ingress_generation), 204);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, directory_mode), 212);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, zero_reserved), 214);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, holders), 224);
	UT_ASSERT_EQ(RESOURCE_X_WIRE_KIND_MAX, 11);
	UT_ASSERT_EQ(RESOURCE_X_MSG_IMAGE_OR_GRANT, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(RESOURCE_X_MSG_BLOCK_TO_N, PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE);
}

UT_TEST(test_decision_disk_s_and_x_preserve_complete_episode)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		make_decision_bytes(bytes, false, PCM_STATE_N, 0, mode, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
		if (!decision_round_trip(bytes, false, &decoded))
			continue;
		UT_ASSERT_EQ(decoded.body.acquire_decision.episode.backend_id, 17);
		UT_ASSERT_EQ(decoded.body.acquire_decision.episode.buffer_id, 29);
		UT_ASSERT_EQ(decoded.body.acquire_decision.episode.reservation_token,
			UINT64_C(0xa1a2a3a4a5a6a7a8));
		UT_ASSERT_EQ(decoded.body.acquire_decision.episode.base_generation,
			UINT64_C(0x9192939495969798));
		UT_ASSERT_EQ(decoded.body.acquire_decision.source_node, -1);
		UT_ASSERT_EQ(decoded.body.acquire_decision.resource_binding, UINT64_C(0x0102030405060708));
		UT_ASSERT_EQ(decoded.body.acquire_decision.enqueue_order, UINT64_C(0x1112131415161718));
		UT_ASSERT_EQ(decoded.body.acquire_decision.required_holders_bitmap, 0);
	}
}

UT_TEST(test_decision_remote_x_source_and_downgrade_require_exact_holder)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	for (int forward = 0; forward <= 1; forward++)
		for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
			make_decision_bytes(bytes, forward, PCM_STATE_X, 2, mode, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
			test_put_u32_be(bytes + 200, 6); /* PI responsibility is not current authority. */
			if (!decision_round_trip(bytes, forward, &decoded))
				continue;
			UT_ASSERT_EQ(decoded.body.acquire_decision.required_holders_bitmap, 2);
			UT_ASSERT_EQ(decoded.body.acquire_decision.pi_holders_bitmap, 6);
			UT_ASSERT_EQ(decoded.body.acquire_decision.holders[1].boot_incarnation, 11);
			UT_ASSERT_EQ(decoded.body.acquire_decision.holders[1].grant_generation, 51);
			UT_ASSERT_EQ(decoded.common.authority_generation, 10001);
		}
}

UT_TEST(test_decision_shared_holders_keep_every_required_identity)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		make_decision_bytes(bytes, false, PCM_STATE_S, UINT32_MAX, mode, 31,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		if (!decision_round_trip(bytes, false, &decoded))
			continue;
		UT_ASSERT_EQ(decoded.body.acquire_decision.required_holders_bitmap,
			mode == PCM_STATE_X ? UINT32_MAX & ~UINT32_C(8) : 0);
		for (int node = 0; node < 32; node++)
			UT_ASSERT_EQ(decoded.body.acquire_decision.holders[node].grant_generation, 50 + node);
	}
}

UT_TEST(test_decision_local_current_is_bound_to_requester_boot)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	for (int dir = PCM_STATE_S; dir <= PCM_STATE_X; dir++)
		for (int mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
			make_decision_bytes(bytes, false, dir, 8, mode, 3, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
			bytes[152] = RESOURCE_X_ACQUIRE_RESIDENT;
			bytes[72] = dir;
			if (!decision_round_trip(bytes, false, &decoded))
				continue;
			UT_ASSERT_EQ(decoded.body.acquire_decision.holders[3].boot_incarnation,
				decoded.body.acquire_decision.episode.requester_incarnation);
			UT_ASSERT_EQ(decoded.body.acquire_decision.required_holders_bitmap, 0);
			test_put_u32_be(bytes + 196, 8); /* R cannot be its own external ACK. */
			test_reseal(bytes, RESOURCE_X_DECISION_V4_BYTES);
			test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, sizeof(bytes));
		}
}

UT_TEST(test_decision_forward_may_address_required_nonsource_only)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	make_decision_bytes(bytes, true, PCM_STATE_S, 6, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	test_put_u32_be(bytes + 68, 2);
	(void)decision_round_trip(bytes, true, &decoded);
	test_put_u32_be(bytes + 68, 0);
	decision_reject_bytes(bytes, true);
	make_decision_bytes(bytes, true, PCM_STATE_N, 0, PCM_STATE_S, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u32_be(bytes + 68, 3);
	decision_reject_bytes(bytes, true);
}

UT_TEST(test_decision_every_prefix_extra_length_and_wrong_pair_is_rejected)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES + 1];

	for (int forward = 0; forward <= 1; forward++) {
		uint8 msg = forward ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
		make_decision_bytes(bytes, forward, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		for (int len = 0; len < RESOURCE_X_DECISION_V4_BYTES; len++)
			test_acquire_rejected(msg, bytes, len);
		bytes[RESOURCE_X_DECISION_V4_BYTES] = 0;
		test_acquire_rejected(msg, bytes, sizeof(bytes));
		for (int type = 0; type < 256; type++)
			if (type != msg)
				test_acquire_rejected(type, bytes, RESOURCE_X_DECISION_V4_BYTES);
	}
}

UT_TEST(test_decision_version_declared_length_kind_and_crc_are_strict)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES], before[RESOURCE_X_DECISION_V4_BYTES];

	make_decision_bytes(before, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	for (int version = 0; version < 256; version++) {
		if (version == RESOURCE_X_DECISION_WIRE_VERSION)
			continue;
		memcpy(bytes, before, sizeof(bytes)); bytes[0] = version;
		decision_reject_bytes(bytes, false);
	}
	for (int kind = 0; kind < 256; kind++) {
		if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT)
			continue;
		memcpy(bytes, before, sizeof(bytes)); bytes[1] = kind;
		decision_reject_bytes(bytes, false);
	}
	memcpy(bytes, before, sizeof(bytes)); bytes[3]--;
	decision_reject_bytes(bytes, false);
	memcpy(bytes, before, sizeof(bytes)); bytes[224] ^= 1;
	test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, sizeof(bytes));
}

UT_TEST(test_decision_reserved_ranges_are_zero_even_with_valid_crc)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const int starts[] = {82, 153, 214}, lengths[] = {6, 7, 10};

	for (int range = 0; range < 3; range++)
		for (int pos = starts[range]; pos < starts[range] + lengths[range]; pos++) {
			make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
			bytes[pos] = 1;
			decision_reject_bytes(bytes, false);
		}
}

UT_TEST(test_decision_full_generations_reject_zero_and_exhaustion)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const int offsets[] = {32, 40, 48, 56, 88, 96, 104, 120, 128, 136, 160, 168, 176, 240, 248};

	for (unsigned i = 0; i < lengthof(offsets); i++)
		for (int exhausted = 0; exhausted <= 1; exhausted++) {
			make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
			test_put_u64_be(bytes + offsets[i], exhausted ? UINT64_MAX : 0);
			decision_reject_bytes(bytes, false);
		}
	for (int exhausted = 0; exhausted <= 1; exhausted++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		test_put_u64_be(bytes + 88, exhausted ? 9999 : 10000);
		decision_reject_bytes(bytes, false);
	}
}

UT_TEST(test_decision_native_base_zero_is_valid_but_bad_slots_or_path_are_not)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	const int offsets[] = {144, 148};

	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_X, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u64_be(bytes + 112, 0);
	test_put_u32_be(bytes + 144, 0);
	test_put_u32_be(bytes + 148, 0);
	(void)decision_round_trip(bytes, false, &decoded);
	test_put_u64_be(bytes + 112, UINT64_MAX);
	decision_reject_bytes(bytes, false);
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_X, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
		test_put_u32_be(bytes + offsets[i], UINT32_MAX);
		decision_reject_bytes(bytes, false);
	}
	for (int path = 0; path <= 3; path++) {
		if (path == RESOURCE_X_ACQUIRE_FIRST_FILL || path == RESOURCE_X_ACQUIRE_RESIDENT)
			continue;
		make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_X, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
		bytes[152] = path;
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_X, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	bytes[72] = PCM_STATE_S; /* first fill cannot claim a preexisting S */
	decision_reject_bytes(bytes, false);
}

UT_TEST(test_decision_connection_domains_and_master_roles_are_bounded)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const int connections[] = {76, 204, 208};

	for (unsigned i = 0; i < lengthof(connections); i++)
		for (int exhausted = 0; exhausted <= 1; exhausted++) {
			make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
			test_put_u32_be(bytes + connections[i], exhausted ? UINT32_MAX : 0);
			decision_reject_bytes(bytes, false);
		}
	for (int node = -1; node <= 32; node += 33) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		test_put_u32_be(bytes + 184, (uint32)node);
		decision_reject_bytes(bytes, false);
	}
}

UT_TEST(test_decision_holder_set_has_no_missing_stray_or_future_grant)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const int offsets[] = {224, 232, 256, 264};

	for (unsigned i = 0; i < lengthof(offsets); i++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		test_put_u64_be(bytes + offsets[i], 1);
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	test_put_u64_be(bytes + 248, 10001);
	decision_reject_bytes(bytes, false);
}

UT_TEST(test_decision_directory_and_required_set_cannot_omit_blockers)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const uint32 required[] = {0, 1, 4, 8, UINT32_MAX};

	for (unsigned i = 0; i < lengthof(required); i++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		test_put_u32_be(bytes + 196, required[i]);
		decision_reject_bytes(bytes, false);
	}
	for (int mode = PCM_STATE_N; mode <= 3; mode++) {
		if (mode == PCM_STATE_X || mode == PCM_STATE_S)
			continue;
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		bytes[212] = mode;
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_S, 6, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	bytes[212] = PCM_STATE_X; /* two X holders */
	decision_reject_bytes(bytes, false);
}

UT_TEST(test_decision_disk_never_covers_current_pi_or_service_debt)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];

	for (int mutation = 0; mutation < 4; mutation++) {
		make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
		if (mutation == 0) test_put_u32_be(bytes + 200, 1);
		if (mutation == 1) test_put_u64_be(bytes + 176, 1);
		if (mutation == 2) test_put_u32_be(bytes + 188, 0);
		if (mutation == 3) bytes[212] = PCM_STATE_S;
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_S, 1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	decision_reject_bytes(bytes, false);
}

UT_TEST(test_decision_remote_local_source_identity_cannot_be_substituted)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];

	for (int mutation = 0; mutation < 5; mutation++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		if (mutation == 0) test_put_u32_be(bytes + 188, 3);
		if (mutation == 1) test_put_u32_be(bytes + 188, 32);
		if (mutation == 2) test_put_u32_be(bytes + 188, UINT32_MAX);
		if (mutation == 3) bytes[213] = RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT;
		if (mutation == 4) bytes[213] = 4;
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_X, 8, PCM_STATE_X, 3,
		RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	test_put_u64_be(bytes + 224 + 3 * 16, 42);
	decision_reject_bytes(bytes, false);
	/* The requester may be a compatible holder even when another node sends
	 * the image. It must not inherit an earlier boot's local grant. */
	make_decision_bytes(bytes, false, PCM_STATE_S, 10, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	test_put_u64_be(bytes + 224 + 3 * 16, 42);
	decision_reject_bytes(bytes, false);
}

UT_TEST(test_decision_never_encodes_terminal_outcome_or_legacy_source_flags)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	const int offsets[] = {64, 74, 75, 80, 81};

	for (unsigned i = 0; i < lengthof(offsets); i++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		bytes[offsets[i]] = 1;
		decision_reject_bytes(bytes, false);
	}
	for (int outcome = RESOURCE_X_OUTCOME_OK; outcome <= RESOURCE_X_OUTCOME_CORRUPT; outcome++) {
		make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		bytes[80] = outcome;
		decision_reject_bytes(bytes, false);
	}
	make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	test_put_u32_be(bytes + 68, 1);
	decision_reject_bytes(bytes, false);
	bytes[68] = bytes[69] = bytes[70] = 0; bytes[71] = 3;
	bytes[73] = PCM_STATE_N;
	decision_reject_bytes(bytes, false);
}

static void
decision_encode_rejected(ResourceXDecodedFrame *decoded, uint16 capacity)
{
	uint8 output[RESOURCE_X_DECISION_V4_BYTES], original[RESOURCE_X_DECISION_V4_BYTES];
	uint16 len = 77;
	ResourceXWireReject reject;

	memset(output, 0x5a, sizeof(output));
	memcpy(original, output, sizeof(output));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, decoded,
		output, capacity, &len, &reject));
	UT_ASSERT_NE(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT_EQ(len, 77);
	UT_ASSERT(memcmp(output, original, sizeof(output)) == 0);
}

UT_TEST(test_decision_encoder_checks_typed_body_and_discriminator_before_output)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded, invalid;

	make_decision_bytes(bytes, false, PCM_STATE_X, 2, PCM_STATE_X, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	for (int mutation = 0; mutation < 8; mutation++) {
		invalid = decoded;
		if (mutation == 0) invalid.has_acquire_intent = true;
		if (mutation == 1) invalid.blocked_has_remote_proof = true;
		if (mutation == 2) invalid.kind = RESOURCE_X_WIRE_ASSERT_X;
		if (mutation == 3) invalid.body.acquire_decision.required_holders_bitmap = 0;
		if (mutation == 4) invalid.body.acquire_decision.holders[0].boot_incarnation = 1;
		if (mutation == 5) invalid.body.acquire_decision.episode.reservation_token = 0;
		if (mutation == 6) invalid.common.outcome = RESOURCE_X_OUTCOME_OK;
		if (mutation == 7) invalid.body.acquire_decision.source_node = 32;
		decision_encode_rejected(&invalid, sizeof(bytes));
	}
}

UT_TEST(test_decision_every_insufficient_capacity_is_atomic)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;

	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1, RESOURCE_X_ACQUIRE_SOURCE_DISK);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	for (uint16 cap = 0; cap < RESOURCE_X_DECISION_V4_BYTES; cap++)
		decision_encode_rejected(&decoded, cap);
}

UT_TEST(test_decision_sender_rebind_preserves_master_episode_and_holder_bytes)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES], before[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;

	for (int forward = 0; forward <= 1; forward++) {
		uint8 msg = forward ? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT;
		make_decision_bytes(bytes, forward, PCM_STATE_S, 6, PCM_STATE_X, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
		memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(msg, bytes, sizeof(bytes), 7, &reject));
		for (int offset = 0; offset < RESOURCE_X_DECISION_V4_BYTES; offset++)
			if ((offset < 4 || offset >= 8) && (offset < 76 || offset >= 80))
				UT_ASSERT_EQ(bytes[offset], before[offset]);
		if (!decision_round_trip(bytes, forward, &decoded))
			continue;
		UT_ASSERT_EQ(decoded.common.sender_connection_generation, 7);
		memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(msg, bytes, sizeof(bytes), UINT32_MAX, &reject));
		UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
		bytes[214] = 1;
		test_reseal(bytes, sizeof(bytes));
		memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(msg, bytes, sizeof(bytes), 9, &reject));
		UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
	}
}

/* A and the original ACK are built independently of the witness encoder. */
static ResourceXDecodedFrame
witness_attach(uint8 *bytes, int holder, bool source_proof)
{
	ResourceXDecodedFrame decision, ack, witness = {0};
	ResourceXWireReject reject;
	uint16 inner_len = 0;
	uint16 len = source_proof ? RESOURCE_X_SOURCE_WITNESS_V4_BYTES : RESOURCE_X_WITNESS_V4_BYTES;
	test_put_u32_be(bytes + 68, holder);
	test_reseal(bytes, RESOURCE_X_DECISION_V4_BYTES);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes,
		RESOURCE_X_DECISION_V4_BYTES, &decision, &reject));
	ack = make_typed_frame(RESOURCE_X_WIRE_BLOCKED_TO_N);
	ack.common = decision.common;
	ack.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	ack.has_acquire_decision = ack.has_acquire_intent = false;
	ack.blocked_has_remote_proof = source_proof;
	ack.common.authority_generation = ack.common.base_authority_generation;
	ack.common.observed_mode = decision.body.acquire_decision.directory_mode;
	ack.common.target_mode = decision.common.target_mode == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N;
	ack.common.outcome = RESOURCE_X_OUTCOME_OK;
	ack.common.flags = source_proof ? RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED : 0;
	ack.common.sender_connection_generation = 83;
	if (source_proof) {
		ResourceXDecodedBlockedToN *proof = &ack.body.blocked_to_n;
		memset(proof->source_fence, 0, sizeof(proof->source_fence));
		test_put_u32_be(proof->source_fence, holder);
		test_put_u64_be(proof->source_fence + 4, decision.body.acquire_decision.holders[holder].boot_incarnation);
		test_put_u64_be(proof->source_fence + 12, 71);
		test_put_u64_be(proof->source_fence + 20, 77);
		proof->source_fence[28] = ack.common.observed_mode;
		proof->source_fence[29] = 1;
		proof->source_carrier_generation = 78;
		proof->requester_target_generation = ack.common.assertion_sequence;
		proof->acting_formation = ack.common.resource_formation;
	}
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &ack,
		bytes + RESOURCE_X_DECISION_V4_BYTES, RESOURCE_X_PROOF_V1_BYTES, &inner_len, &reject));
	UT_ASSERT_EQ(inner_len, len - RESOURCE_X_DECISION_V4_BYTES);
	witness.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	witness.payload_bytes = len;
	witness.has_acquire_decision = true;
	witness.common = decision.common;
	witness.common.outcome = RESOURCE_X_OUTCOME_OK;
	witness.body.acquire_witness.decision = decision.body.acquire_decision;
	witness.body.acquire_witness.original_ack = ack.common;
	if (source_proof) witness.body.acquire_witness.original_proof = ack.body.blocked_to_n;
	witness.body.acquire_witness.has_source_proof = source_proof;
	bytes[1] = RESOURCE_X_WIRE_BLOCKED_TO_N;
	bytes[2] = len >> 8; bytes[3] = len;
	bytes[80] = RESOURCE_X_OUTCOME_OK;
	test_reseal(bytes, len);
	return witness;
}

static ResourceXDecodedFrame
make_witness_bytes(uint8 *bytes, bool source, PcmState directory, PcmState target)
{
	make_decision_bytes(bytes, true, directory, directory == PCM_STATE_X ? 2 : 6,
		target, 1, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	return witness_attach(bytes, source ? 1 : 2, source);
}

static bool
witness_round_trip(const uint8 *bytes, uint16 len, ResourceXDecodedFrame *frame)
{
	ResourceXWireReject reject;
	uint8 encoded[RESOURCE_X_SOURCE_WITNESS_V4_BYTES];
	uint16 actual = 0;
	bool ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, frame, &reject);
	UT_ASSERT(ok);
	if (!ok) return false;
	UT_ASSERT(frame->has_acquire_decision && !frame->has_acquire_intent && !frame->blocked_has_remote_proof);
	UT_ASSERT_EQ(frame->kind, RESOURCE_X_WIRE_BLOCKED_TO_N);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, frame,
		encoded, sizeof(encoded), &actual, &reject));
	UT_ASSERT_EQ(actual, len);
	UT_ASSERT(memcmp(encoded, bytes, len) == 0);
	return true;
}

static void
witness_reject(uint8 *bytes, uint16 len, bool reseal_inner)
{
	if (reseal_inner) test_reseal(bytes + RESOURCE_X_DECISION_V4_BYTES, len - RESOURCE_X_DECISION_V4_BYTES);
	test_reseal(bytes, len);
	test_acquire_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len);
}

static void
witness_encode_rejected(ResourceXDecodedFrame *frame, uint16 capacity)
{
	uint8 output[RESOURCE_X_SOURCE_WITNESS_V4_BYTES], original[RESOURCE_X_SOURCE_WITNESS_V4_BYTES];
	uint16 len = 77;
	ResourceXWireReject reject;
	memset(output, 0xa5, sizeof(output)); memcpy(original, output, sizeof(output));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, frame,
		output, capacity, &len, &reject));
	UT_ASSERT_EQ(len, 77);
	UT_ASSERT(memcmp(output, original, sizeof(output)) == 0);
}

UT_TEST(test_witness_layout_retains_complete_a_and_original_ack)
{
	UT_ASSERT_EQ(RESOURCE_X_WITNESS_V4_BYTES, 832);
	UT_ASSERT_EQ(RESOURCE_X_SOURCE_WITNESS_V4_BYTES, 1048);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireWitnessV4), 832);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireSourceWitnessV4), 1048);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireSourceWitnessV4, original_ack), 736);
	UT_ASSERT(sizeof(ResourceXDecodedAcquireWitness) <= sizeof(ResourceXDecodedImageEnvelope));
	printf("# decoded_frame_bytes=%zu witness_body_bytes=%zu\n", sizeof(ResourceXDecodedFrame), sizeof(ResourceXDecodedAcquireWitness));
}

UT_TEST(test_witness_plain_required_holder_round_trips_original_ack)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	(void)make_witness_bytes(bytes, false, PCM_STATE_S, PCM_STATE_X);
	if (witness_round_trip(bytes, 832, &frame)) {
		UT_ASSERT(!frame.body.acquire_witness.has_source_proof);
		UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.action_node, 2);
		UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.target_mode, PCM_STATE_N);
	}
}

UT_TEST(test_witness_shared_source_retains_original_proof)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, target);
		if (witness_round_trip(bytes, 1048, &frame)) {
			UT_ASSERT(frame.body.acquire_witness.has_source_proof);
			UT_ASSERT_EQ(frame.body.acquire_witness.original_proof.source_carrier_generation, 78);
			UT_ASSERT_EQ(frame.body.acquire_witness.decision.holders[1].grant_generation, 51);
			UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.target_mode,
				target == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N);
		}
	}
}

UT_TEST(test_witness_x_source_keeps_downgrade_and_revoke_distinct)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_X, target);
		if (witness_round_trip(bytes, 1048, &frame)) {
			UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.observed_mode, PCM_STATE_X);
			UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.target_mode, target == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N);
		}
	}
}

UT_TEST(test_witness_local_source_self_downgrade_has_no_external_ack)
{
	uint8 bytes[1048]; ResourceXDecodedFrame decision, witness = {0};
	ResourceXWireReject reject; uint16 length = 0;
	make_decision_bytes(bytes, false, PCM_STATE_X, 8, PCM_STATE_S, 3, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
	if (!decision_round_trip(bytes, false, &decision)) return;
	UT_ASSERT_EQ(decision.body.acquire_decision.required_holders_bitmap, 0);
	witness.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	witness.payload_bytes = RESOURCE_X_WITNESS_V4_BYTES;
	witness.has_acquire_decision = true;
	witness.common = decision.common;
	witness.common.action_node = 3; witness.common.outcome = RESOURCE_X_OUTCOME_OK;
	witness.body.acquire_witness.decision = decision.body.acquire_decision;
	witness.body.acquire_witness.original_ack = witness.common;
	witness.body.acquire_witness.original_ack.authority_generation = decision.common.base_authority_generation;
	witness.body.acquire_witness.original_ack.observed_mode = PCM_STATE_X;
	witness.body.acquire_witness.original_ack.target_mode = PCM_STATE_S;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &witness,
		bytes, sizeof(bytes), &length, &reject));
}

UT_TEST(test_witness_required_holder_bit31_is_not_lost)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	make_decision_bytes(bytes, true, PCM_STATE_S, UINT32_C(0x80000002), PCM_STATE_X, 1, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	(void)witness_attach(bytes, 31, false);
	if (witness_round_trip(bytes, 832, &frame)) UT_ASSERT_EQ(frame.common.action_node, 31);
}

UT_TEST(test_witness_every_prefix_and_extra_byte_is_rejected)
{
	uint8 bytes[1049];
	for (int source = 0; source <= 1; source++) {
		uint16 len = source ? 1048 : 832;
		(void)make_witness_bytes(bytes, source, PCM_STATE_S, PCM_STATE_X);
		for (uint16 n = 0; n < len; n++) test_acquire_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, n);
		bytes[len] = 0; test_acquire_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len + 1);
	}
}

UT_TEST(test_witness_other_message_types_and_kinds_are_rejected)
{
	uint8 bytes[1048];
	for (int source = 0; source <= 1; source++) {
		uint16 len = source ? 1048 : 832;
		(void)make_witness_bytes(bytes, source, PCM_STATE_S, PCM_STATE_X);
		for (int type = 0; type < 256; type++)
			if (type != RESOURCE_X_MSG_BLOCKED_TO_N) test_acquire_rejected(type, bytes, len);
		for (int kind = 0; kind <= 12; kind++) {
			(void)make_witness_bytes(bytes, source, PCM_STATE_S, PCM_STATE_X);
			if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N) continue;
			bytes[1] = kind; witness_reject(bytes, len, false);
		}
	}
}

UT_TEST(test_witness_version_declared_length_and_crc_are_strict)
{
	uint8 bytes[1048];
	for (int variant = 0; variant < 4; variant++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) bytes[0] = 1;
		if (variant == 1) bytes[0] = 2;
		if (variant == 2) bytes[3]--;
		if (variant == 3) {bytes[744] ^= 1; test_acquire_rejected(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, 1048);continue;}
		witness_reject(bytes, 1048, false);
	}
}

UT_TEST(test_witness_both_reserved_domains_are_zero)
{
	uint8 bytes[1048];
	const int offsets[] = {82,83,87,153,159,214,223,736+82,736+87};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		bytes[offsets[n]] = 1; witness_reject(bytes, 1048, true);
	}
}

UT_TEST(test_witness_inner_crc_cannot_be_repaired_by_outer_reseal)
{
	uint8 bytes[1048];
	(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
	bytes[736+284] ^= 1;
	witness_reject(bytes, 1048, false);
}

UT_TEST(test_witness_original_ack_key_must_match_complete_a)
{
	uint8 bytes[1048];
	const int offsets[] = {8,32,40,48,56,64,68,72,73,88};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		bytes[736+offsets[n]] ^= 1; witness_reject(bytes, 1048, true);
	}
}

UT_TEST(test_witness_only_completed_ok_is_accepted)
{
	uint8 bytes[1048];
	for (int outcome = RESOURCE_X_OUTCOME_NONE; outcome <= RESOURCE_X_OUTCOME_CORRUPT; outcome++) {
		if (outcome == RESOURCE_X_OUTCOME_OK) continue;
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		bytes[80] = outcome; witness_reject(bytes, 1048, false);
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		bytes[736+80] = outcome; witness_reject(bytes, 1048, true);
	}
}

UT_TEST(test_witness_source_proof_cannot_be_omitted_or_substituted)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	frame = make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
	frame.body.acquire_witness.has_source_proof = false;
	memset(&frame.body.acquire_witness.original_proof, 0, sizeof(frame.body.acquire_witness.original_proof));
	witness_encode_rejected(&frame, 1048);
	frame = make_witness_bytes(bytes, false, PCM_STATE_S, PCM_STATE_X);
	frame.body.acquire_witness.has_source_proof = true;
	witness_encode_rejected(&frame, 1048);
}

UT_TEST(test_witness_original_proof_dependencies_remain_strict)
{
	uint8 bytes[1048];
	for (int variant = 0; variant < 3; variant++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) {bytes[736+154] = 0;bytes[736+155] = 17;}
		if (variant == 1) test_put_u64_be(bytes+736+156+15*8, 1);
		if (variant == 2) bytes[736+294] = 1;
		witness_reject(bytes, 1048, true);
	}
}

UT_TEST(test_witness_source_fence_binds_original_holder_boot_node_and_mode)
{
	uint8 bytes[1048];
	const int offsets[] = {736+96+3,736+96+4,736+96+28};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		bytes[offsets[n]] ^= 1; witness_reject(bytes, 1048, true);
	}
}

UT_TEST(test_witness_native_carrier_is_not_master_grant_generation)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
	if (!witness_round_trip(bytes, 1048, &frame)) return;
	UT_ASSERT_NE(frame.body.acquire_witness.original_proof.source_carrier_generation,
		frame.body.acquire_witness.decision.holders[1].grant_generation);
	for (int variant = 0; variant < 4; variant++) {
		frame = make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) frame.body.acquire_witness.original_proof.source_carrier_generation = 0;
		if (variant == 1) frame.body.acquire_witness.original_proof.source_carrier_generation = UINT64_MAX;
		if (variant == 2) test_put_u64_be(frame.body.acquire_witness.original_proof.source_fence + 20, 0);
		if (variant == 3) test_put_u64_be(frame.body.acquire_witness.original_proof.source_fence + 20, UINT64_MAX);
		witness_encode_rejected(&frame, 1048);
	}
}

UT_TEST(test_witness_required_set_cannot_omit_or_add_an_incumbent)
{
	uint8 bytes[1048];
	for (int variant = 0; variant < 3; variant++) {
		(void)make_witness_bytes(bytes, false, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) test_put_u32_be(bytes+196, 2);
		if (variant == 1) test_put_u32_be(bytes+196, 14);
		if (variant == 2) test_put_u64_be(bytes+224+2*16+8, 0);
		witness_reject(bytes, 832, false);
	}
}

UT_TEST(test_witness_complete_episode_and_receiving_holder_remain_bound)
{
	uint8 bytes[1048];
	const int offsets[] = {96,104,120,136,144,148,152,160,184,204,208};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		/* Invalid/exhausted fields, not arbitrary valid A choices. Full expected
			* A equality is the receiver's duty, not inferred by a stateless codec. */
		if (offsets[n] == 152) bytes[152] = 9;
		else if (offsets[n] == 144 || offsets[n] == 148 || offsets[n] == 184)
			test_put_u32_be(bytes+offsets[n], UINT32_MAX);
		else if (offsets[n] == 204 || offsets[n] == 208) test_put_u32_be(bytes+offsets[n], 0);
		else test_put_u64_be(bytes+offsets[n], 0);
		witness_reject(bytes, 1048, false);
	}
}

UT_TEST(test_witness_encoder_preserves_every_insufficient_output)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	for (int source = 0; source <= 1; source++) {
		uint16 len = source ? 1048 : 832;
		frame = make_witness_bytes(bytes, source, PCM_STATE_S, PCM_STATE_X);
		for (uint16 capacity = 0; capacity < len; capacity++) witness_encode_rejected(&frame, capacity);
	}
}

UT_TEST(test_witness_typed_discriminators_and_unused_proof_are_closed)
{
	uint8 bytes[1048]; ResourceXDecodedFrame frame;
	for (int variant = 0; variant < 4; variant++) {
		frame = make_witness_bytes(bytes, false, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) frame.has_acquire_intent = true;
		if (variant == 1) frame.blocked_has_remote_proof = true;
		if (variant == 2) frame.body.acquire_witness.original_proof.source_carrier_generation = 1;
		if (variant == 3) frame.body.acquire_witness.original_ack.sender_connection_generation = UINT32_MAX;
		witness_encode_rejected(&frame, 1048);
	}
}

UT_TEST(test_witness_sender_rebind_changes_only_outer_transport_and_crc)
{
	uint8 bytes[1048], original[1048]; ResourceXDecodedFrame frame; ResourceXWireReject reject;
	for (int source = 0; source <= 1; source++) {
		uint16 len = source ? 1048 : 832;
		(void)make_witness_bytes(bytes, source, PCM_STATE_S, PCM_STATE_X); memcpy(original, bytes, len);
		UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_BLOCKED_TO_N, bytes, len, 99, &reject));
		for (int offset = 0; offset < len; offset++)
			if ((offset < 4 || offset >= 8) && (offset < 76 || offset >= 80)) UT_ASSERT_EQ(bytes[offset], original[offset]);
		if (witness_round_trip(bytes, len, &frame)) {
			UT_ASSERT_EQ(frame.common.sender_connection_generation, 99);
			UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.sender_connection_generation, 83);
		}
	}
}

UT_TEST(test_witness_rebind_refuses_malformed_nested_or_exhausted_sender)
{
	uint8 bytes[1048], original[1048]; ResourceXWireReject reject;
	for (int variant = 0; variant < 3; variant++) {
		(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
		if (variant == 0) {bytes[736+68] ^= 1;test_reseal(bytes+736,312);test_reseal(bytes,1048);}
		memcpy(original, bytes, 1048);
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_BLOCKED_TO_N,
			bytes, 1048, variant == 1 ? 0 : variant == 2 ? UINT32_MAX : 99, &reject));
		UT_ASSERT(memcmp(bytes, original, 1048) == 0);
	}
}

UT_TEST(test_witness_original_pi_proof_and_native_bytes_are_never_synthesized)
{
	uint8 bytes[1048], encoded[1048]; ResourceXDecodedFrame frame; ResourceXWireReject reject; uint16 len;
	(void)make_witness_bytes(bytes, true, PCM_STATE_S, PCM_STATE_X);
	if (!witness_round_trip(bytes, 1048, &frame)) return;
	UT_ASSERT_EQ(frame.body.acquire_witness.original_ack.flags, RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED);
	UT_ASSERT_EQ(frame.body.acquire_witness.original_proof.dependencies[1], 85);
	UT_ASSERT_EQ(frame.body.acquire_witness.original_proof.holder_connection_generation, 86);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &frame, encoded, sizeof(encoded), &len, &reject));
	UT_ASSERT(memcmp(bytes+736, encoded+736, 312) == 0);
}

/* The original image is constructed by the old codec independently of SHIP.
	* S is a new encapsulated component mode; a bare S image still rejects. */
static uint32
ship_crc(const void *data, Size len)
{
	pg_crc32c crc;
	INIT_CRC32C(crc); COMP_CRC32C(crc, data, len); FIN_CRC32C(crc);
	return (uint32)crc;
}
static uint32
ship_get_u32(const uint8 *b)
{
	return ((uint32)b[0] << 24) | ((uint32)b[1] << 16) | ((uint32)b[2] << 8) | b[3];
}
static ResourceXDecodedFrame
make_ship_bytes(uint8 *bytes, PcmState directory, PcmState target, bool wal)
{
	ResourceXDecodedFrame source = make_witness_bytes(bytes, true, directory, target);
	ResourceXDecodedFrame original = wal ? make_wal_image() : make_typed_frame(RESOURCE_X_WIRE_IMAGE_ENVELOPE);
	ResourceXDecodedFrame ship = {0};
	ResourceXDecodedImageEnvelope *image = &original.body.image_envelope;
	ResourceXDecodedBlockedToN *proof = &source.body.acquire_witness.original_proof;
	ResourceXWireReject reject;
	uint8 deps[128]; uint16 len = 0;
	original.common = source.common;
	original.common.action_node = original.common.logical_assertion.requester_node;
	original.common.observed_mode = directory;
	original.common.target_mode = PCM_STATE_X;
	original.common.sender_connection_generation = 71;
	original.common.flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
	memset(image->request_tail, 0, sizeof(image->request_tail));
	test_put_u64_be(image->request_tail, 71);
	test_put_u64_be(image->request_tail + 8, original.common.assertion_sequence);
	image->conversion_base_generation = original.common.base_authority_generation;
	memcpy(image->source_fence, proof->source_fence, sizeof(image->source_fence));
	image->source_carrier_generation = proof->source_carrier_generation;
	image->requester_target_generation = original.common.assertion_sequence;
	for (int i = 0; i < 16; i++) test_put_u64_be(deps + i * 8, image->dependencies[i]);
	image->dependency_vector_crc32c = ship_crc(deps, sizeof(deps));
	image->page_checksum = ship_crc(image->page_bytes, sizeof(image->page_bytes));
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &original,
		bytes + 1048, RESOURCE_X_IMAGE_V2_BYTES, &len, &reject));
	UT_ASSERT_EQ(len, RESOURCE_X_IMAGE_V2_BYTES);
	bytes[1048 + 73] = target;
	test_reseal(bytes + 1048, RESOURCE_X_IMAGE_V2_BYTES);
	original.common.target_mode = target;
	original.common.semantic_crc32c = ship_get_u32(bytes + 1048 + 4);
	proof->page_scn_lsn = image->page_scn_lsn;
	proof->dependency_count = image->dependency_count;
	memcpy(proof->dependencies, image->dependencies, sizeof(proof->dependencies));
	proof->page_checksum = image->page_checksum;
	proof->source_proof_crc32c = original.common.semantic_crc32c;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &source,
		bytes, RESOURCE_X_SOURCE_WITNESS_V4_BYTES, &len, &reject));
	UT_ASSERT_EQ(len, 1048);
	ship.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	ship.payload_bytes = RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES;
	ship.has_acquire_decision = true;
	ship.common = source.common;
	ship.body.acquire_image.source = source.body.acquire_witness;
	ship.body.acquire_image.original_common = original.common;
	ship.body.acquire_image.original_image = *image;
	bytes[1] = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	bytes[2] = RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES >> 8;
	bytes[3] = RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES & 255;
	test_reseal(bytes, RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES);
	return ship;
}
static bool
ship_round_trip(const uint8 *bytes, ResourceXDecodedFrame *decoded)
{
	uint8 encoded[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint16 len = 0;
	ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes,
		RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES, decoded, &reject);
	UT_ASSERT(ok); if (!ok) return false;
	UT_ASSERT(decoded->has_acquire_decision && !decoded->has_acquire_intent && !decoded->blocked_has_remote_proof);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, decoded,
		encoded, sizeof(encoded), &len, &reject));
	UT_ASSERT_EQ(len, sizeof(encoded)); UT_ASSERT(memcmp(bytes, encoded, sizeof(encoded)) == 0);
	return true;
}
static void
ship_encode_rejected(const ResourceXDecodedFrame *frame, uint16 capacity)
{
	uint8 encoded[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES], original[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	ResourceXWireReject reject; uint16 len = 77;
	memset(encoded, 0xa6, sizeof(encoded)); memcpy(original, encoded, sizeof(original));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, frame, encoded,
		capacity, &len, &reject));
	UT_ASSERT_EQ(len, 77); UT_ASSERT(memcmp(encoded, original, sizeof(encoded)) == 0);
}
static void
ship_reject(uint8 *bytes)
{
	test_reseal(bytes, RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES);
	test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES);
}
UT_TEST(test_ship_layout_has_one_complete_a_source_ack_and_image)
{
	UT_ASSERT_EQ(RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES, 9800);
	UT_ASSERT_EQ(sizeof(ResourceXAcquireImageV4), 9800);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireImageV4, original_image), 1048);
	printf("# ship_frame_bytes=%zu ship_body_bytes=%zu\n", sizeof(ResourceXDecodedFrame), sizeof(ResourceXDecodedAcquireImage));
}
UT_TEST(test_ship_s_and_x_sources_support_both_target_modes)
{
	uint8 bytes[9800]; ResourceXDecodedFrame decoded;
	for (int source = PCM_STATE_S; source <= PCM_STATE_X; source++)
		for (int target = PCM_STATE_S; target <= PCM_STATE_X; target++) {
			(void)make_ship_bytes(bytes, source, target, false);
			if (ship_round_trip(bytes, &decoded)) {
				UT_ASSERT_EQ(decoded.body.acquire_image.original_common.target_mode, target);
				UT_ASSERT_EQ(decoded.body.acquire_image.source.original_ack.target_mode, target == PCM_STATE_S ? PCM_STATE_S : PCM_STATE_N);
				UT_ASSERT(memcmp(bytes + 1048 + 328, decoded.body.acquire_image.original_image.page_bytes, 8192) == 0);
				if (target == PCM_STATE_S)
					test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes + 1048, 8752);
			}
		}
}
UT_TEST(test_ship_full_wal_proof_is_preserved)
{
	uint8 bytes[9800]; ResourceXDecodedFrame decoded;
	(void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, true);
	if (ship_round_trip(bytes, &decoded)) {
		UT_ASSERT_EQ(decoded.body.acquire_image.original_image.image_flags, RESOURCE_X_IMAGE_HAS_WAL);
		UT_ASSERT_EQ(decoded.body.acquire_image.original_image.page_wal.flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	}
}
UT_TEST(test_ship_every_prefix_and_extra_byte_is_rejected)
{
	uint8 bytes[9801]; (void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
	for (uint16 n = 0; n < 9800; n++) test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, n);
	test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, 9801);
}
UT_TEST(test_ship_only_image_family_and_kind_are_accepted)
{
	uint8 bytes[9800]; (void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
	for (int type = 14; type <= 18; type++) if (type != RESOURCE_X_MSG_IMAGE_OR_GRANT)
		test_acquire_rejected(type, bytes, sizeof(bytes));
	for (int kind = 1; kind <= 11; kind++) if (kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		bytes[1] = kind; ship_reject(bytes);
	}
}
UT_TEST(test_ship_version_length_and_outer_crc_are_exact)
{
	uint8 bytes[9800]; const int offsets[] = {0,2,4,8000};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
		bytes[offsets[n]] ^= 1;
		test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, sizeof(bytes));
	}
}
UT_TEST(test_ship_reserved_bytes_at_all_three_layers_reject)
{
	uint8 bytes[9800]; const int offsets[] = {82,153,214,736+82,1048+82,1048+8520+132};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		(void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false); bytes[offsets[n]] = 1;
		test_reseal(bytes+736,312); test_reseal(bytes+1048,8752); ship_reject(bytes);
	}
}
UT_TEST(test_ship_inner_image_crc_is_independent_of_outer_reseal)
{
	uint8 bytes[9800]; (void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
	bytes[1048+400] ^= 1; ship_reject(bytes);
}
UT_TEST(test_ship_original_ack_crc_is_independent_of_outer_reseal)
{
	uint8 bytes[9800]; (void)make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
	bytes[736+296] ^= 1; ship_reject(bytes);
}
UT_TEST(test_ship_ack_must_name_the_original_image_crc)
{
	uint8 bytes[9800]; ResourceXDecodedFrame frame = make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
	frame.body.acquire_image.source.original_proof.source_proof_crc32c ^= 1; ship_encode_rejected(&frame, 9800);
}
UT_TEST(test_ship_original_image_key_and_domains_match_a)
{
	uint8 bytes[9800];
	for (int n = 0; n < 8; n++) {
		ResourceXDecodedFrame frame = make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false);
		ResourceXDecodedCommon *c = &frame.body.acquire_image.original_common;
		if (n==0) c->logical_assertion.resource.blockNum++;
		if (n==1) c->logical_assertion.requester_node++;
		if (n==2) c->base_authority_generation++;
		if (n==3) c->authority_generation++;
		if (n==4) c->resource_formation++;
		if (n==5) c->master_session_incarnation++;
		if (n==6) c->assertion_sequence++;
		if (n==7) c->ordered_lane++;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_original_image_target_source_and_requester_are_exact)
{
	uint8 bytes[9800];
	for (int n = 0; n < 6; n++) {
		ResourceXDecodedFrame frame = make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_S, false);
		ResourceXDecodedCommon *c = &frame.body.acquire_image.original_common;
		if(n==0)c->target_mode=PCM_STATE_X; if(n==1)c->observed_mode=PCM_STATE_S;
		if(n==2)c->action_node=1; if(n==3)c->outcome=RESOURCE_X_OUTCOME_RETRY;
		if(n==4)c->source_candidate=1; if(n==5)c->retain_pi_if_dirty=1;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_image_request_tail_and_source_connection_remain_bound)
{
	uint8 bytes[9800];
	for(int n=0;n<4;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		ResourceXDecodedImageEnvelope *i=&frame.body.acquire_image.original_image;
		if(n==0)i->request_tail[7]^=1; if(n==1)i->request_tail[15]^=1;
		if(n==2)i->request_tail[19]=1; if(n==3)i->source_fence[19]^=1;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_source_proof_and_image_scalars_must_agree)
{
	uint8 bytes[9800];
	for(int n=0;n<8;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		ResourceXDecodedBlockedToN *p=&frame.body.acquire_image.source.original_proof;
		if(n==0)p->source_carrier_generation++; if(n==1)p->page_scn_lsn++;
		if(n==2)p->page_checksum++; if(n==3)p->dependency_count=0;
		if(n==4)p->dependencies[0]++; if(n==5)p->source_fence[12]^=1;
		if(n==6)p->source_disposition++; if(n==7)p->proof_kind++;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_page_checksum_cannot_be_hidden_by_wire_crc)
{
	uint8 bytes[9800]; ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
	frame.body.acquire_image.original_image.page_bytes[511]^=1; ship_encode_rejected(&frame,9800);
	/* All three wire CRCs can be valid while the source page checksum is
	 * stale. The image remains unusable, even with a matching ACK reference. */
	bytes[1048+328+511]^=1;
	test_reseal(bytes+1048,8752);
	test_put_u32_be(bytes+736+284,ship_get_u32(bytes+1048+4));
	test_reseal(bytes+736,312);
	ship_reject(bytes);
}
UT_TEST(test_ship_dependency_vector_crc_is_canonical)
{
	uint8 bytes[9800]; ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
	frame.body.acquire_image.original_image.dependency_vector_crc32c^=1; ship_encode_rejected(&frame,9800);
}
UT_TEST(test_ship_wal_binding_shape_and_page_stay_strict)
{
	uint8 bytes[9800];
	for(int n=0;n<4;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,true);
		ResourceXDecodedImageEnvelope *i=&frame.body.acquire_image.original_image;
		if(n==0)i->page_wal.identity.blockno++; if(n==1)i->page_wal.version.mutation_token++;
		if(n==2)i->page_wal.flags=0; if(n==3)i->image_flags=0;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_unused_wal_body_must_be_empty)
{
	uint8 bytes[9800]; ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
	frame.body.acquire_image.original_image.page_wal.record_end=42; ship_encode_rejected(&frame,9800);
}
UT_TEST(test_ship_source_is_remote_and_selected_by_master)
{
	uint8 bytes[9800];
	for(int n=0;n<4;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_S,PCM_STATE_X,false);
		ResourceXDecodedAcquireWitness *w=&frame.body.acquire_image.source;
		if(n==0)w->decision.source_kind=RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT;
		if(n==1)w->decision.source_kind=RESOURCE_X_ACQUIRE_SOURCE_DISK;
		if(n==2)frame.common.action_node=2; if(n==3)w->has_source_proof=false;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_wrong_discriminators_and_legacy_flags_reject)
{
	uint8 bytes[9800];
	for(int n=0;n<5;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		if(n==0)frame.has_acquire_decision=false; if(n==1)frame.has_acquire_intent=true;
		if(n==2)frame.blocked_has_remote_proof=true; if(n==3)frame.common.flags=4;
		if(n==4)frame.body.acquire_image.original_common.flags=RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_zero_exhausted_and_mismatched_generations_reject)
{
	uint8 bytes[9800];
	for(int n=0;n<5;n++) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		if(n==0)frame.common.sender_connection_generation=UINT32_MAX;
		if(n==1)frame.body.acquire_image.original_common.sender_connection_generation=UINT32_MAX;
		if(n==2)frame.body.acquire_image.original_common.sender_connection_generation=0;
		if(n==3)frame.body.acquire_image.source.decision.holders[1].boot_incarnation++;
		if(n==4)frame.body.acquire_image.source.decision.episode.reservation_token=0;
		ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_only_ok_terminal_results_are_valid)
{
	uint8 bytes[9800];
	for(int outcome=0;outcome<=RESOURCE_X_OUTCOME_CORRUPT;outcome++) if(outcome!=RESOURCE_X_OUTCOME_OK) {
		ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		frame.common.outcome=outcome; ship_encode_rejected(&frame,9800);
	}
}
UT_TEST(test_ship_every_insufficient_capacity_preserves_output)
{
	uint8 bytes[9800]; ResourceXDecodedFrame frame=make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
	for(uint16 n=0;n<9800;n++)ship_encode_rejected(&frame,n);
}
UT_TEST(test_ship_sender_rebind_only_changes_outer_header)
{
	uint8 bytes[9800],before[9800]; ResourceXDecodedFrame decoded; ResourceXWireReject reject;
	(void)make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false); memcpy(before,bytes,sizeof(bytes));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_IMAGE_OR_GRANT,bytes,9800,999,&reject));
	if(ship_round_trip(bytes,&decoded))UT_ASSERT_EQ(decoded.common.sender_connection_generation,999);
	UT_ASSERT(memcmp(before+96,bytes+96,9800-96)==0);
	for(int i=0;i<96;i++) if((i<4||i>7)&&(i<76||i>79))UT_ASSERT_EQ(before[i],bytes[i]);
}
UT_TEST(test_ship_bad_inner_frame_or_exhausted_rebind_is_atomic)
{
	uint8 bytes[9800],before[9800]; ResourceXWireReject reject;
	for(int n=0;n<2;n++) {
		(void)make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);
		if(n==0){bytes[1048+200]^=1;test_reseal(bytes,9800);} memcpy(before,bytes,sizeof(bytes));
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_IMAGE_OR_GRANT,bytes,9800,n?UINT32_MAX:999,&reject));
		UT_ASSERT(memcmp(before,bytes,sizeof(bytes))==0);
	}
}
UT_TEST(test_ship_original_image_version_length_and_kind_are_strict)
{
	uint8 bytes[9800]; const int offsets[]={1048,1049,1050};
	for(unsigned n=0;n<lengthof(offsets);n++) {
		(void)make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,false);bytes[offsets[n]]^=1;
		test_reseal(bytes+1048,8752);ship_reject(bytes);
	}
}
UT_TEST(test_ship_native_and_master_generation_axes_remain_independent)
{
	uint8 bytes[9800]; ResourceXDecodedFrame decoded;
	(void)make_ship_bytes(bytes,PCM_STATE_S,PCM_STATE_S,false);
	if(ship_round_trip(bytes,&decoded)) {
		UT_ASSERT_EQ(decoded.body.acquire_image.source.decision.holders[1].grant_generation,51);
		UT_ASSERT_EQ(decoded.body.acquire_image.original_image.source_carrier_generation,78);
	}
}
UT_TEST(test_ship_source_ack_and_image_are_byte_exact_after_round_trip)
{
	uint8 bytes[9800],before[9800]; ResourceXDecodedFrame decoded;
	(void)make_ship_bytes(bytes,PCM_STATE_X,PCM_STATE_X,true);memcpy(before,bytes,sizeof(bytes));
	if(ship_round_trip(bytes,&decoded)) {
		UT_ASSERT_EQ(decoded.body.acquire_image.source.original_proof.source_proof_crc32c,ship_get_u32(before+1052));
		UT_ASSERT_EQ(decoded.body.acquire_image.source.original_ack.sender_connection_generation,83);
		UT_ASSERT_EQ(decoded.body.acquire_image.original_common.sender_connection_generation,71);
	}
}


UT_TEST(test_decision_watermark_layout_is_fixed_and_network_ordered)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	const uint64 wm = UINT64_C(0x0102030405060708);
	UT_ASSERT_EQ(RESOURCE_X_DECISION_WIRE_VERSION, 4);
	UT_ASSERT_EQ(offsetof(ResourceXAcquireDecisionV4, disk_watermark_scn), 216);
	UT_ASSERT_EQ(sizeof(((ResourceXAcquireDecisionV4 *)0)->zero_reserved), 2);
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1,
						RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u64_be(bytes + 216, wm);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	UT_ASSERT_EQ(decoded.body.acquire_decision.disk_watermark_scn, wm);
	UT_ASSERT_EQ(bytes[0], 4);
	for (unsigned n = 0; n < 8; n++)
		UT_ASSERT_EQ(bytes[216 + n], (wm >> ((7 - n) * 8)) & 255);
}
UT_TEST(test_decision_old_conditional_version_is_not_an_untracked_disk_grant)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1,
						RESOURCE_X_ACQUIRE_SOURCE_DISK);
	bytes[0] = 3;
	decision_reject_bytes(bytes, false);
}
UT_TEST(test_decision_current_source_cannot_smuggle_a_disk_watermark)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	make_decision_bytes(bytes, false, PCM_STATE_S, 2, PCM_STATE_S, 1,
						RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	decoded.body.acquire_decision.disk_watermark_scn = 91;
	decision_encode_rejected(&decoded, sizeof(bytes));
	test_put_u64_be(bytes + 216, 91);
	decision_reject_bytes(bytes, false);
}
UT_TEST(test_decision_disk_watermark_is_covered_by_original_crc)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_X, -1,
						RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u64_be(bytes + 216, 93);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	for (unsigned n = 216; n < 224; n++) {
		bytes[n] ^= 1;
		test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, sizeof(bytes));
		bytes[n] ^= 1;
	}
}
UT_TEST(test_decision_sender_rebind_preserves_the_original_disk_watermark)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES], before[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1,
						RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u64_be(bytes + 216, UINT64_C(0x070000000000005d));
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes,
															   sizeof(bytes), 89, &reject));
	for (unsigned n = 0; n < sizeof(bytes); n++)
		if ((n < 4 || n >= 8) && (n < 76 || n >= 80))
			UT_ASSERT_EQ(bytes[n], before[n]);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, sizeof(bytes),
											 &decoded, &reject));
	UT_ASSERT_EQ(decoded.body.acquire_decision.disk_watermark_scn, UINT64_C(0x070000000000005d));
}
UT_TEST(test_decision_disk_reserved_bytes_and_short_frames_remain_rejected)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ResourceXDecodedFrame decoded;
	make_decision_bytes(bytes, false, PCM_STATE_N, 0, PCM_STATE_S, -1,
						RESOURCE_X_ACQUIRE_SOURCE_DISK);
	test_put_u64_be(bytes + 216, 93);
	if (!decision_round_trip(bytes, false, &decoded))
		return;
	for (unsigned n = 214; n < 216; n++) {
		bytes[n] = 1;
		decision_reject_bytes(bytes, false);
		bytes[n] = 0;
	}
	test_reseal(bytes, sizeof(bytes));
	for (unsigned n = 0; n < sizeof(bytes); n++)
		test_acquire_rejected(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, n);
}
UT_TEST(test_decision_witness_and_ship_upgrade_without_changing_inner_proofs)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	ResourceXWireReject reject;
	ResourceXDecodedFrame frame, decoded;
	uint16 len;
	for (unsigned profile = 0; profile < 3; profile++) {
		uint8 type = profile == 2 ? RESOURCE_X_MSG_IMAGE_OR_GRANT : RESOURCE_X_MSG_BLOCKED_TO_N;
		frame = profile == 2
					? make_ship_bytes(bytes, PCM_STATE_X, PCM_STATE_X, false)
					: make_witness_bytes(bytes, profile == 1,
										 profile == 0 ? PCM_STATE_S : PCM_STATE_X, PCM_STATE_X);
		len = frame.payload_bytes;
		UT_ASSERT_EQ(bytes[0], 4);
		UT_ASSERT_EQ(bytes[736], RESOURCE_X_WIRE_VERSION);
		if (profile == 2)
			UT_ASSERT_EQ(bytes[1048], RESOURCE_X_IMAGE_WIRE_VERSION);
		bytes[0] = 3;
		test_reseal(bytes, len);
		test_acquire_rejected(type, bytes, len);
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(type, bytes, len, 91, &reject));
		bytes[0] = 4;
		test_reseal(bytes, len);
		UT_ASSERT(cluster_resource_x_wire_decode(type, bytes, len, &decoded, &reject));
	}
}


/* Independent full-source receipt bytes. Codec success supplies no native
 * completion, directory grant, transport owner or source/WAL authority. */
#define TEST_SOURCE_COMPLETE_BYTES 776
static const uint8 source_complete_outcomes[] = {
	RESOURCE_X_OUTCOME_OK, RESOURCE_X_OUTCOME_DUPLICATE,
	RESOURCE_X_OUTCOME_QUIET, RESOURCE_X_OUTCOME_QUIET_RETIRED,
	RESOURCE_X_OUTCOME_QUIET_NO_IMAGE, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED
};
static bool
source_complete_quiet(unsigned result)
{
	return result >= 2;
}
static void
source_complete_make(uint8 *bytes, uint8 directory, uint32 holders, int source,
	uint8 source_kind, bool resident, unsigned result)
{
	make_decision_bytes(bytes, false, directory, holders, PCM_STATE_S, source, source_kind);
	memset(bytes + 736, 0, 40);
	if (resident) {
		bytes[72] = directory;
		bytes[152] = RESOURCE_X_ACQUIRE_RESIDENT;
	}
	/* Retain the original complete A checksum separately from the receipt. */
	test_reseal(bytes, 736);
	test_put_u32_be(bytes + 772, ship_get_u32(bytes + 4));
	bytes[0] = (uint8)(11 + result);
	bytes[1] = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	bytes[2] = 3; bytes[3] = 8;
	bytes[80] = source_complete_outcomes[result];
	test_put_u32_be(bytes + 68, result % 2 ? 2 : 3);
	test_put_u64_be(bytes + 736, UINT64_C(0x9192939495969798)
		+ (source_complete_quiet(result) ? 0 : 1));
	test_put_u64_be(bytes + 744, UINT64_C(0x0102030405060708));
	if (result < 4) {
		test_put_u64_be(bytes + 752, UINT64_C(0x1122334455667788));
		test_put_u64_be(bytes + 760, UINT64_C(0x0203040506070809));
		test_put_u32_be(bytes + 768, UINT32_C(0x12345678));
	}
	test_reseal(bytes, TEST_SOURCE_COMPLETE_BYTES);
}
static bool
source_complete_round_trip(uint8 *bytes, ResourceXDecodedFrame *decoded)
{
	uint8 encoded[TEST_SOURCE_COMPLETE_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	bool ok;
	test_reseal(bytes, TEST_SOURCE_COMPLETE_BYTES);
	ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, TEST_SOURCE_COMPLETE_BYTES, decoded, &reject);
	UT_ASSERT(ok); if (!ok) return false;
	UT_ASSERT(decoded->has_acquire_decision && !decoded->has_acquire_intent
		&& !decoded->blocked_has_remote_proof);
	UT_ASSERT_EQ(decoded->kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		decoded, encoded, sizeof(encoded), &len, &reject));
	UT_ASSERT_EQ(len, sizeof(encoded));
	UT_ASSERT(memcmp(encoded, bytes, sizeof(encoded)) == 0);
	return true;
}
static void
source_complete_reject(uint8 *bytes)
{
	test_reseal(bytes, TEST_SOURCE_COMPLETE_BYTES);
	test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, TEST_SOURCE_COMPLETE_BYTES);
}
static void
source_complete_encode_reject(const ResourceXDecodedFrame *frame, uint16 capacity)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES], before[sizeof(bytes)];
	uint16 len = 971;
	ResourceXWireReject reject;
	memset(bytes, 0xa5, sizeof(bytes)); memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, capacity, &len, &reject));
	UT_ASSERT_EQ(len, 971);
	UT_ASSERT_NE(reject, RESOURCE_X_WIRE_REJECT_NONE);
	UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
}
UT_TEST(test_source_complete_all_six_results_keep_complete_a)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	ResourceXDecodedFrame frame;
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++)
		for (int directory = PCM_STATE_S; directory <= PCM_STATE_X; directory++) {
			source_complete_make(bytes, directory, directory == PCM_STATE_S ? 6 : 2,
				1, RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
			if (!source_complete_round_trip(bytes, &frame)) continue;
			UT_ASSERT_EQ(frame.common.outcome, source_complete_outcomes[result]);
			UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders[1].boot_incarnation, 11);
			UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders[1].grant_generation, 51);
			UT_ASSERT_EQ(frame.body.acquire_completion.decision.required_holders_bitmap,
				directory == PCM_STATE_X ? 2 : 0);
			UT_ASSERT_EQ(frame.body.acquire_completion.native_generation,
				UINT64_C(0x9192939495969798) + (source_complete_quiet(result) ? 0 : 1));
		}
}
UT_TEST(test_source_complete_full_holder_set_and_high_node_are_lossless)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES]; ResourceXDecodedFrame frame;
	source_complete_make(bytes, PCM_STATE_S, UINT32_MAX, 31,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	if (!source_complete_round_trip(bytes, &frame)) return;
	UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders_bitmap, UINT32_MAX);
	for (int n = 0; n < 32; n++)
		UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders[n].grant_generation, 50 + n);
	UT_ASSERT_EQ(frame.body.acquire_completion.page_lsn, UINT64_C(0x1122334455667788));
	UT_ASSERT_EQ(frame.body.acquire_completion.page_scn, UINT64_C(0x0203040506070809));
	UT_ASSERT_EQ(frame.body.acquire_completion.page_crc32c, UINT32_C(0x12345678));
}
UT_TEST(test_source_complete_resident_local_current_retains_requester_boot)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES]; ResourceXDecodedFrame frame;
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		source_complete_make(bytes, PCM_STATE_S, 8, 3,
			RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT, true, result);
		if (!source_complete_round_trip(bytes, &frame)) continue;
		UT_ASSERT_EQ(frame.body.acquire_completion.decision.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
		UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders[3].boot_incarnation,
			frame.body.acquire_completion.decision.episode.requester_incarnation);
	}
}
UT_TEST(test_source_complete_every_prefix_extra_length_and_wrong_family_refused)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES + 1];
	source_complete_make(bytes, PCM_STATE_X, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	for (uint16 n = 0; n < TEST_SOURCE_COMPLETE_BYTES; n++)
		test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, n);
	bytes[TEST_SOURCE_COMPLETE_BYTES] = 0;
	test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, sizeof(bytes));
	for (int n = 0; n < 256; n++)
		if (n != RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE)
			test_acquire_rejected(n, bytes, TEST_SOURCE_COMPLETE_BYTES);
}
UT_TEST(test_source_complete_each_result_requires_its_exact_version)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		source_complete_make(bytes, PCM_STATE_S, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
		for (int version = 0; version < 256; version++) {
			if (version == 11 + result) continue;
			bytes[0] = version;
			source_complete_reject(bytes);
		}
	}
}
UT_TEST(test_source_complete_direction_and_result_do_not_alias)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		source_complete_make(bytes, PCM_STATE_X, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
		test_put_u32_be(bytes + 68, result % 2 ? 3 : 2);
		source_complete_reject(bytes);
		source_complete_make(bytes, PCM_STATE_X, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
		bytes[80] = RESOURCE_X_OUTCOME_NONE;
		source_complete_reject(bytes);
	}
}
UT_TEST(test_source_complete_native_generation_matches_completion_polarity)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++)
		for (unsigned n = 0; n < 3; n++) {
			source_complete_make(bytes, PCM_STATE_X, 2, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
			test_put_u64_be(bytes + 736, n == 0 ? 0 : n == 1 ? UINT64_MAX
				: UINT64_C(0x9192939495969798) + (source_complete_quiet(result) ? 1 : 0));
			source_complete_reject(bytes);
		}
}
UT_TEST(test_source_complete_odd_write_io_and_no_image_bytes_are_refused)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	const unsigned offsets[] = {752, 760, 768};
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		source_complete_make(bytes, PCM_STATE_S, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
		bytes[751] |= 1; source_complete_reject(bytes);
		if (result < 4) continue;
		for (unsigned n = 0; n < lengthof(offsets); n++) {
			source_complete_make(bytes, PCM_STATE_S, 2, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
			bytes[offsets[n]] = 1; source_complete_reject(bytes);
		}
	}
}
UT_TEST(test_source_complete_holder_proof_cannot_be_missing_or_out_of_set)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	const unsigned offsets[] = {240, 248, 224};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		source_complete_make(bytes, PCM_STATE_X, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
		test_put_u64_be(bytes + offsets[n], n == 2 ? 1 : 0);
		source_complete_reject(bytes);
	}
	source_complete_make(bytes, PCM_STATE_X, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	test_put_u32_be(bytes + 196, 0); source_complete_reject(bytes);
}
UT_TEST(test_source_complete_reserved_full_key_and_source_checks_remain_strict)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	const unsigned offsets[] = {82, 153, 214};
	const unsigned lengths[] = {6, 7, 2};
	const unsigned generations[] = {32, 40, 48, 56, 88, 96, 104, 120, 128, 136, 160, 168, 176, 240, 248};
	for (unsigned n = 0; n < lengthof(offsets); n++)
		for (unsigned i = 0; i < lengths[n]; i++) {
			source_complete_make(bytes, PCM_STATE_S, 2, 1,
				RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
			bytes[offsets[n] + i] = 1; source_complete_reject(bytes);
		}
	for (unsigned n = 0; n < lengthof(generations); n++) {
		source_complete_make(bytes, PCM_STATE_X, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
		test_put_u64_be(bytes + generations[n], UINT64_MAX);
		source_complete_reject(bytes);
	}
	source_complete_make(bytes, PCM_STATE_S, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	test_put_u32_be(bytes + 188, 0); source_complete_reject(bytes);
	source_complete_make(bytes, PCM_STATE_S, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	test_put_u64_be(bytes + 216, 91); source_complete_reject(bytes);
}
UT_TEST(test_source_complete_crc_covers_a_and_native_evidence)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	source_complete_make(bytes, PCM_STATE_S, 6, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	for (unsigned n = 0; n < sizeof(bytes); n++) {
		bytes[n] ^= 1;
		test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, sizeof(bytes));
		bytes[n] ^= 1;
	}
}
UT_TEST(test_source_complete_encoder_capacity_and_rejection_are_atomic)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES]; ResourceXDecodedFrame frame;
	source_complete_make(bytes, PCM_STATE_X, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	if (!source_complete_round_trip(bytes, &frame)) return;
	for (unsigned n = 0; n < sizeof(bytes); n++) source_complete_encode_reject(&frame, n);
	frame.body.acquire_completion.decision.holders[0].boot_incarnation = 1;
	source_complete_encode_reject(&frame, sizeof(bytes));
}
UT_TEST(test_source_complete_sender_rebind_changes_only_physical_envelope)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES], before[sizeof(bytes)];
	ResourceXWireReject reject; ResourceXDecodedFrame frame;
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		source_complete_make(bytes, PCM_STATE_S, UINT32_MAX, 31,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, result);
		memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, sizeof(bytes), 89, &reject));
		for (unsigned n = 0; n < sizeof(bytes); n++)
			if ((n < 4 || n >= 8) && (n < 76 || n >= 80))
				UT_ASSERT_EQ(bytes[n], before[n]);
		if (!source_complete_round_trip(bytes, &frame)) continue;
		UT_ASSERT_EQ(frame.common.sender_connection_generation, 89);
	}
}
UT_TEST(test_source_complete_invalid_rebind_preserves_all_bytes)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES], before[sizeof(bytes)]; ResourceXWireReject reject;
	for (unsigned n = 0; n < 3; n++) {
		source_complete_make(bytes, PCM_STATE_X, 2, 1,
			RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
		if (n == 0) bytes[744] ^= 1;
		memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, sizeof(bytes), n == 1 ? 0 : n == 2 ? UINT32_MAX : 89, &reject));
		UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
	}
}
UT_TEST(test_source_complete_disk_and_source_formats_cannot_alias)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES];
	ResourceXDecodedFrame frame; ResourceXWireReject reject; uint16 len;
	source_complete_make(bytes, PCM_STATE_N, 0, -1,
		RESOURCE_X_ACQUIRE_SOURCE_DISK, false, 0);
	source_complete_reject(bytes);
	source_complete_make(bytes, PCM_STATE_S, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	/* A valid legacy length and recomputed CRC cannot omit holder identities. */
	bytes[0] = 5; bytes[2] = 1; bytes[3] = 8;
	test_reseal(bytes, 264);
	test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, 264);
	source_complete_make(bytes, PCM_STATE_N, 0, -1,
		RESOURCE_X_ACQUIRE_SOURCE_DISK, false, 0);
	memcpy(bytes + 224, bytes + 736, 40);
	bytes[0] = 5; bytes[2] = 1; bytes[3] = 8; test_reseal(bytes, 264);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, 264, &frame, &reject));
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		&frame, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT_EQ(len, 264);
}
UT_TEST(test_source_complete_known_x_preserves_full_a_and_invalid_kind_is_refused)
{
	uint8 bytes[TEST_SOURCE_COMPLETE_BYTES]; ResourceXDecodedFrame frame;
	source_complete_make(bytes, PCM_STATE_X, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	bytes[73] = PCM_STATE_X;
	if (!source_complete_round_trip(bytes, &frame)) return;
	UT_ASSERT_EQ(frame.common.target_mode, PCM_STATE_X);
	UT_ASSERT_EQ(frame.body.acquire_completion.decision.holders_bitmap, 2);
	UT_ASSERT_EQ(frame.body.acquire_completion.decision.source_node, 1);
	source_complete_make(bytes, PCM_STATE_X, 2, 1,
		RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT, false, 0);
	if (!source_complete_round_trip(bytes, &frame)) return;
	frame.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	source_complete_encode_reject(&frame, sizeof(bytes));
	frame.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT; frame.has_acquire_intent = true;
	source_complete_encode_reject(&frame, sizeof(bytes));
}

/* Independent compact disk completion bytes. Codec success does not supply
 * native X installation, T1--T3 or writer activation evidence. */
static void
disk_x_complete_make(uint8 *bytes, bool resident, unsigned result)
{
	uint8 grant[RESOURCE_X_DECISION_V4_BYTES];
	make_decision_bytes(grant, false, PCM_STATE_N, 0, PCM_STATE_X, -1,
		RESOURCE_X_ACQUIRE_SOURCE_DISK);
	if (resident) grant[152] = RESOURCE_X_ACQUIRE_RESIDENT;
	test_put_u64_be(grant + 216, UINT64_C(0x0203040506070809));
	test_reseal(grant, sizeof(grant));
	memcpy(bytes, grant, 224); memset(bytes + 224, 0, 40);
	bytes[0] = (uint8)(5 + result); bytes[1] = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	bytes[2] = 1; bytes[3] = 8; bytes[80] = source_complete_outcomes[result];
	test_put_u32_be(bytes + 68, result % 2 ? 2 : 3);
	test_put_u64_be(bytes + 224, UINT64_C(0x9192939495969798) + (result < 2 ? 1 : 0));
	test_put_u64_be(bytes + 232, UINT64_C(0x0102030405060708));
	if (result < 4) {
		test_put_u64_be(bytes + 240, UINT64_C(0x1122334455667788));
		test_put_u64_be(bytes + 248, UINT64_C(0x0203040506070809));
		test_put_u32_be(bytes + 256, UINT32_C(0x12345678));
	}
	test_put_u32_be(bytes + 260, ship_get_u32(grant + 4));
	test_reseal(bytes, RESOURCE_X_COMPLETE_V5_BYTES);
}

static bool
disk_x_complete_round_trip(uint8 *bytes, ResourceXDecodedFrame *frame)
{
	uint8 encoded[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len = 0;
	ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(encoded), frame, &reject);
	UT_ASSERT(ok); if (!ok) return false;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, encoded, sizeof(encoded), &len, &reject));
	UT_ASSERT_EQ(len, sizeof(encoded));
	UT_ASSERT(memcmp(bytes, encoded, sizeof(encoded)) == 0);
	return true;
}

static void
disk_x_complete_reject(uint8 *bytes)
{
	test_reseal(bytes, RESOURCE_X_COMPLETE_V5_BYTES);
	test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, RESOURCE_X_COMPLETE_V5_BYTES);
}

UT_TEST(test_disk_x_complete_first_fill_resident_and_all_six_results)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; ResourceXDecodedFrame frame;
	for (int resident = 0; resident < 2; resident++)
		for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
			disk_x_complete_make(bytes, resident, result);
			if (!disk_x_complete_round_trip(bytes, &frame)) continue;
			UT_ASSERT_EQ(frame.common.target_mode, PCM_STATE_X);
			UT_ASSERT_EQ(frame.common.outcome, source_complete_outcomes[result]);
			UT_ASSERT_EQ(frame.body.acquire_completion.decision.episode.path,
				resident ? RESOURCE_X_ACQUIRE_RESIDENT : RESOURCE_X_ACQUIRE_FIRST_FILL);
			UT_ASSERT_EQ(frame.body.acquire_completion.decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_DISK);
			UT_ASSERT_EQ(frame.body.acquire_completion.native_generation,
				UINT64_C(0x9192939495969798) + (result < 2 ? 1 : 0));
		}
}
UT_TEST(test_disk_x_complete_result_version_and_direction_cannot_alias)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		for (int version = 1; version <= 16; version++) {
			if (version == 5 + result) continue;
			disk_x_complete_make(bytes, false, result); bytes[0] = version;
			disk_x_complete_reject(bytes);
		}
		disk_x_complete_make(bytes, false, result);
		test_put_u32_be(bytes + 68, result % 2 ? 3 : 2); disk_x_complete_reject(bytes);
	}
}
UT_TEST(test_disk_x_complete_native_generation_and_write_IO_remain_strict)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		disk_x_complete_make(bytes, true, result);
		test_put_u64_be(bytes + 224, UINT64_C(0x9192939495969798) + (result < 2 ? 0 : 1));
		disk_x_complete_reject(bytes);
		disk_x_complete_make(bytes, true, result); bytes[239] |= 1; disk_x_complete_reject(bytes);
	}
}
UT_TEST(test_disk_x_complete_watermark_and_absent_image_are_not_relaxed)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		if (result < 4) {
			disk_x_complete_make(bytes, false, result);
			test_put_u64_be(bytes + 248, UINT64_C(0x0203040506070808)); disk_x_complete_reject(bytes);
		} else {
			const unsigned offsets[] = {240, 248, 256};
			for (unsigned n = 0; n < lengthof(offsets); n++) {
				disk_x_complete_make(bytes, false, result); bytes[offsets[n]] = 1; disk_x_complete_reject(bytes);
			}
		}
	}
}
UT_TEST(test_disk_x_complete_cannot_omit_holders_or_claim_resident_S)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	const unsigned fields[] = {176, 192, 196, 200, 212};
	for (unsigned n = 0; n < lengthof(fields); n++) {
		disk_x_complete_make(bytes, true, 0); bytes[fields[n]] = 1; disk_x_complete_reject(bytes);
	}
	disk_x_complete_make(bytes, true, 0); test_put_u32_be(bytes + 188, 1); disk_x_complete_reject(bytes);
	disk_x_complete_make(bytes, true, 0); bytes[213] = RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT; disk_x_complete_reject(bytes);
	disk_x_complete_make(bytes, true, 0); bytes[72] = PCM_STATE_S; disk_x_complete_reject(bytes);
	disk_x_complete_make(bytes, false, 0); bytes[73] = PCM_STATE_N; disk_x_complete_reject(bytes);
}
UT_TEST(test_disk_x_complete_prefix_extra_length_and_crc_remain_strict)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES + 1];
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		disk_x_complete_make(bytes, false, result); bytes[sizeof(bytes) - 1] = 0;
		for (unsigned len = 0; len < sizeof(bytes) - 1; len++)
			test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, len);
		test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, sizeof(bytes));
		bytes[256] ^= 1;
		test_acquire_rejected(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, sizeof(bytes) - 1);
	}
}
UT_TEST(test_disk_x_complete_reserved_episode_and_encoder_refusal_are_atomic)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; ResourceXDecodedFrame frame;
	const unsigned offsets[] = {153, 154, 155, 156, 214, 215};
	for (unsigned n = 0; n < lengthof(offsets); n++) {
		disk_x_complete_make(bytes, true, 0); bytes[offsets[n]] = 1; disk_x_complete_reject(bytes);
	}
	disk_x_complete_make(bytes, false, 0);
	if (!disk_x_complete_round_trip(bytes, &frame)) return;
	for (unsigned cap = 0; cap < sizeof(bytes); cap++) source_complete_encode_reject(&frame, cap);
	frame.body.acquire_completion.decision.episode.reservation_token = 0;
	source_complete_encode_reject(&frame, sizeof(bytes));
}
UT_TEST(test_disk_x_complete_rebind_preserves_full_A_and_native_I)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; ResourceXDecodedFrame before, after;
	ResourceXWireReject reject;
	for (unsigned result = 0; result < lengthof(source_complete_outcomes); result++) {
		disk_x_complete_make(bytes, true, result);
		if (!disk_x_complete_round_trip(bytes, &before)) continue;
		UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, sizeof(bytes), 91, &reject));
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, sizeof(bytes), &after, &reject));
		UT_ASSERT_EQ(after.common.sender_connection_generation, 91);
		UT_ASSERT(memcmp(&before.body.acquire_completion, &after.body.acquire_completion,
			sizeof(before.body.acquire_completion)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(150);
	UT_RUN(test_wire_kind_and_proof_domains_are_closed);
	UT_RUN(test_reused_message_numbers_remain_exact);
	UT_RUN(test_common_wire_layout_is_exact);
	UT_RUN(test_local_proof_layout_is_exact);
	UT_RUN(test_blocked_to_n_layout_is_exact);
	UT_RUN(test_authority_grant_layout_is_exact);
	UT_RUN(test_image_envelope_layout_is_exact);
	UT_RUN(test_install_settlement_layout_is_exact);
	UT_RUN(test_preassert_bootstrap_request_and_ack_are_direction_exact);
	UT_RUN(test_remote_admission_is_only_a_zero_base_request_flag);
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
	UT_RUN(test_image_preserves_full_native_source_and_rejects_legacy);
	UT_RUN(test_image_source_conflicts_refuse_before_output);
	UT_RUN(test_image_wal_corruption_rejected_with_valid_wire_crc);
	UT_RUN(test_delegated_block_and_image_keep_exact_wire_layout);
	UT_RUN(test_delegation_flag_never_grants_other_kind_or_nonsource);
	UT_RUN(test_delegation_generations_rejected_even_with_valid_crc);
	UT_RUN(test_typed_body_validation_is_fail_closed);
	UT_RUN(test_ingress_semantic_mutations_with_valid_crc_are_rejected);
	UT_RUN(test_resource_x_capability_has_complete_collision_census);
	UT_RUN(test_source_settlement_request_and_ack_are_direction_exact);
	UT_RUN(test_source_settlement_truth_tables_fail_closed);
	UT_RUN(test_acquire_layout_reuses_request_family_without_new_kind);
	UT_RUN(test_acquire_first_fill_read_and_known_write_round_trip);
	UT_RUN(test_acquire_resident_n_s_x_preserves_full_episode);
	UT_RUN(test_acquire_every_prefix_and_extra_byte_is_rejected);
	UT_RUN(test_acquire_version_and_kind_length_pairs_are_closed);
	UT_RUN(test_acquire_all_nonrequest_message_types_are_rejected);
	UT_RUN(test_acquire_reserved_bytes_fail_with_valid_crc);
	UT_RUN(test_acquire_zero_or_exhausted_provenance_is_rejected);
	UT_RUN(test_acquire_native_zero_generation_and_slot_boundaries);
	UT_RUN(test_acquire_cannot_claim_authority_source_or_retry_outcome);
	UT_RUN(test_acquire_encoder_failure_preserves_output_and_length);
	UT_RUN(test_acquire_crc_failure_preserves_decoded_output);
	UT_RUN(test_acquire_sender_rebind_preserves_native_episode);
	UT_RUN(test_acquire_insufficient_encode_capacity_is_atomic);
	UT_RUN(test_decision_layout_and_existing_families_are_exact);
	UT_RUN(test_decision_disk_s_and_x_preserve_complete_episode);
	UT_RUN(test_decision_remote_x_source_and_downgrade_require_exact_holder);
	UT_RUN(test_decision_shared_holders_keep_every_required_identity);
	UT_RUN(test_decision_local_current_is_bound_to_requester_boot);
	UT_RUN(test_decision_forward_may_address_required_nonsource_only);
	UT_RUN(test_decision_every_prefix_extra_length_and_wrong_pair_is_rejected);
	UT_RUN(test_decision_version_declared_length_kind_and_crc_are_strict);
	UT_RUN(test_decision_reserved_ranges_are_zero_even_with_valid_crc);
	UT_RUN(test_decision_full_generations_reject_zero_and_exhaustion);
	UT_RUN(test_decision_native_base_zero_is_valid_but_bad_slots_or_path_are_not);
	UT_RUN(test_decision_connection_domains_and_master_roles_are_bounded);
	UT_RUN(test_decision_holder_set_has_no_missing_stray_or_future_grant);
	UT_RUN(test_decision_directory_and_required_set_cannot_omit_blockers);
	UT_RUN(test_decision_disk_never_covers_current_pi_or_service_debt);
	UT_RUN(test_decision_remote_local_source_identity_cannot_be_substituted);
	UT_RUN(test_decision_never_encodes_terminal_outcome_or_legacy_source_flags);
	UT_RUN(test_decision_encoder_checks_typed_body_and_discriminator_before_output);
	UT_RUN(test_decision_every_insufficient_capacity_is_atomic);
	UT_RUN(test_decision_sender_rebind_preserves_master_episode_and_holder_bytes);
	UT_RUN(test_witness_layout_retains_complete_a_and_original_ack);
	UT_RUN(test_witness_plain_required_holder_round_trips_original_ack);
	UT_RUN(test_witness_shared_source_retains_original_proof);
	UT_RUN(test_witness_x_source_keeps_downgrade_and_revoke_distinct);
	UT_RUN(test_witness_local_source_self_downgrade_has_no_external_ack);
	UT_RUN(test_witness_required_holder_bit31_is_not_lost);
	UT_RUN(test_witness_every_prefix_and_extra_byte_is_rejected);
	UT_RUN(test_witness_other_message_types_and_kinds_are_rejected);
	UT_RUN(test_witness_version_declared_length_and_crc_are_strict);
	UT_RUN(test_witness_both_reserved_domains_are_zero);
	UT_RUN(test_witness_inner_crc_cannot_be_repaired_by_outer_reseal);
	UT_RUN(test_witness_original_ack_key_must_match_complete_a);
	UT_RUN(test_witness_only_completed_ok_is_accepted);
	UT_RUN(test_witness_source_proof_cannot_be_omitted_or_substituted);
	UT_RUN(test_witness_original_proof_dependencies_remain_strict);
	UT_RUN(test_witness_source_fence_binds_original_holder_boot_node_and_mode);
	UT_RUN(test_witness_native_carrier_is_not_master_grant_generation);
	UT_RUN(test_witness_required_set_cannot_omit_or_add_an_incumbent);
	UT_RUN(test_witness_complete_episode_and_receiving_holder_remain_bound);
	UT_RUN(test_witness_encoder_preserves_every_insufficient_output);
	UT_RUN(test_witness_typed_discriminators_and_unused_proof_are_closed);
	UT_RUN(test_witness_sender_rebind_changes_only_outer_transport_and_crc);
	UT_RUN(test_witness_rebind_refuses_malformed_nested_or_exhausted_sender);
	UT_RUN(test_witness_original_pi_proof_and_native_bytes_are_never_synthesized);
	UT_RUN(test_ship_layout_has_one_complete_a_source_ack_and_image);
	UT_RUN(test_ship_s_and_x_sources_support_both_target_modes);
	UT_RUN(test_ship_full_wal_proof_is_preserved);
	UT_RUN(test_ship_every_prefix_and_extra_byte_is_rejected);
	UT_RUN(test_ship_only_image_family_and_kind_are_accepted);
	UT_RUN(test_ship_version_length_and_outer_crc_are_exact);
	UT_RUN(test_ship_reserved_bytes_at_all_three_layers_reject);
	UT_RUN(test_ship_inner_image_crc_is_independent_of_outer_reseal);
	UT_RUN(test_ship_original_ack_crc_is_independent_of_outer_reseal);
	UT_RUN(test_ship_ack_must_name_the_original_image_crc);
	UT_RUN(test_ship_original_image_key_and_domains_match_a);
	UT_RUN(test_ship_original_image_target_source_and_requester_are_exact);
	UT_RUN(test_ship_image_request_tail_and_source_connection_remain_bound);
	UT_RUN(test_ship_source_proof_and_image_scalars_must_agree);
	UT_RUN(test_ship_page_checksum_cannot_be_hidden_by_wire_crc);
	UT_RUN(test_ship_dependency_vector_crc_is_canonical);
	UT_RUN(test_ship_wal_binding_shape_and_page_stay_strict);
	UT_RUN(test_ship_unused_wal_body_must_be_empty);
	UT_RUN(test_ship_source_is_remote_and_selected_by_master);
	UT_RUN(test_ship_wrong_discriminators_and_legacy_flags_reject);
	UT_RUN(test_ship_zero_exhausted_and_mismatched_generations_reject);
	UT_RUN(test_ship_only_ok_terminal_results_are_valid);
	UT_RUN(test_ship_every_insufficient_capacity_preserves_output);
	UT_RUN(test_ship_sender_rebind_only_changes_outer_header);
	UT_RUN(test_ship_bad_inner_frame_or_exhausted_rebind_is_atomic);
	UT_RUN(test_ship_original_image_version_length_and_kind_are_strict);
	UT_RUN(test_ship_native_and_master_generation_axes_remain_independent);
	UT_RUN(test_ship_source_ack_and_image_are_byte_exact_after_round_trip);
	UT_RUN(test_decision_watermark_layout_is_fixed_and_network_ordered);
	UT_RUN(test_decision_old_conditional_version_is_not_an_untracked_disk_grant);
	UT_RUN(test_decision_current_source_cannot_smuggle_a_disk_watermark);
	UT_RUN(test_decision_disk_watermark_is_covered_by_original_crc);
	UT_RUN(test_decision_sender_rebind_preserves_the_original_disk_watermark);
	UT_RUN(test_decision_disk_reserved_bytes_and_short_frames_remain_rejected);
	UT_RUN(test_decision_witness_and_ship_upgrade_without_changing_inner_proofs);
	UT_RUN(test_source_complete_all_six_results_keep_complete_a);
	UT_RUN(test_source_complete_full_holder_set_and_high_node_are_lossless);
	UT_RUN(test_source_complete_resident_local_current_retains_requester_boot);
	UT_RUN(test_source_complete_every_prefix_extra_length_and_wrong_family_refused);
	UT_RUN(test_source_complete_each_result_requires_its_exact_version);
	UT_RUN(test_source_complete_direction_and_result_do_not_alias);
	UT_RUN(test_source_complete_native_generation_matches_completion_polarity);
	UT_RUN(test_source_complete_odd_write_io_and_no_image_bytes_are_refused);
	UT_RUN(test_source_complete_holder_proof_cannot_be_missing_or_out_of_set);
	UT_RUN(test_source_complete_reserved_full_key_and_source_checks_remain_strict);
	UT_RUN(test_source_complete_crc_covers_a_and_native_evidence);
	UT_RUN(test_source_complete_encoder_capacity_and_rejection_are_atomic);
	UT_RUN(test_source_complete_sender_rebind_changes_only_physical_envelope);
	UT_RUN(test_source_complete_invalid_rebind_preserves_all_bytes);
	UT_RUN(test_source_complete_disk_and_source_formats_cannot_alias);
	UT_RUN(test_source_complete_known_x_preserves_full_a_and_invalid_kind_is_refused);
	UT_RUN(test_disk_x_complete_first_fill_resident_and_all_six_results);
	UT_RUN(test_disk_x_complete_result_version_and_direction_cannot_alias);
	UT_RUN(test_disk_x_complete_native_generation_and_write_IO_remain_strict);
	UT_RUN(test_disk_x_complete_watermark_and_absent_image_are_not_relaxed);
	UT_RUN(test_disk_x_complete_cannot_omit_holders_or_claim_resident_S);
	UT_RUN(test_disk_x_complete_prefix_extra_length_and_crc_remain_strict);
	UT_RUN(test_disk_x_complete_reserved_episode_and_encoder_refusal_are_atomic);
	UT_RUN(test_disk_x_complete_rebind_preserves_full_A_and_native_I);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
