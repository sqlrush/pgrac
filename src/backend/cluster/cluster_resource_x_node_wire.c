/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_node_wire.c
 *    Resource-X final node-wire strict codec -- spec-8.10 D10-02.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_node_wire.h"
#include "port/pg_crc32c.h"


static void
resource_x_wire_reject(ResourceXWireReject *reject, ResourceXWireReject value)
{
	if (reject != NULL)
		*reject = value;
}


static void
resource_x_put_u16(uint8 *out, uint16 value)
{
	out[0] = (uint8)(value >> 8);
	out[1] = (uint8)value;
}


static void
resource_x_put_u32(uint8 *out, uint32 value)
{
	out[0] = (uint8)(value >> 24);
	out[1] = (uint8)(value >> 16);
	out[2] = (uint8)(value >> 8);
	out[3] = (uint8)value;
}


static void
resource_x_put_u64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> ((7 - i) * 8));
}


static uint16
resource_x_get_u16(const uint8 *in)
{
	return ((uint16)in[0] << 8) | (uint16)in[1];
}


static uint32
resource_x_get_u32(const uint8 *in)
{
	return ((uint32)in[0] << 24) | ((uint32)in[1] << 16) | ((uint32)in[2] << 8) | (uint32)in[3];
}


static uint64
resource_x_get_u64(const uint8 *in)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value = (value << 8) | (uint64)in[i];
	return value;
}


static bool
resource_x_bytes_zero(const uint8 *bytes, Size len)
{
	Size i;

	for (i = 0; i < len; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}


static uint32
resource_x_wire_crc(const uint8 *bytes, uint16 len)
{
	static const uint8 zero_crc[4] = { 0, 0, 0, 0 };
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 4);
	COMP_CRC32C(crc, zero_crc, sizeof(zero_crc));
	COMP_CRC32C(crc, bytes + 8, len - 8);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


static void
resource_x_assertion_encode(uint8 out[24], const ResourceXAssertion *assertion)
{
	memset(out, 0, 24);
	resource_x_put_u32(out, assertion->resource.spcOid);
	resource_x_put_u32(out + 4, assertion->resource.dbOid);
	resource_x_put_u32(out + 8, assertion->resource.relNumber);
	resource_x_put_u32(out + 12, (uint32)assertion->resource.forkNum);
	resource_x_put_u32(out + 16, assertion->resource.blockNum);
	resource_x_put_u32(out + 20, (uint32)assertion->requester_node);
}


static void
resource_x_assertion_decode(const uint8 in[24], ResourceXAssertion *assertion)
{
	memset(assertion, 0, sizeof(*assertion));
	assertion->resource.spcOid = resource_x_get_u32(in);
	assertion->resource.dbOid = resource_x_get_u32(in + 4);
	assertion->resource.relNumber = resource_x_get_u32(in + 8);
	assertion->resource.forkNum = (ForkNumber)resource_x_get_u32(in + 12);
	assertion->resource.blockNum = resource_x_get_u32(in + 16);
	assertion->requester_node = (int32)resource_x_get_u32(in + 20);
}


static bool
resource_x_pair_length(uint8 msg_type, ResourceXWireKind kind, uint16 len)
{
	if (kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP)
		return (msg_type == RESOURCE_X_MSG_ASSERT_X || msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT)
			   && len == RESOURCE_X_CONTROL_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_ASSERT_X)
		return msg_type == RESOURCE_X_MSG_ASSERT_X && len == RESOURCE_X_CONTROL_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION)
		return msg_type == RESOURCE_X_MSG_ASSERT_X && len == RESOURCE_X_SHORT_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_BLOCK_TO_N)
		return msg_type == RESOURCE_X_MSG_BLOCK_TO_N && len == RESOURCE_X_CONTROL_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N)
		return msg_type == RESOURCE_X_MSG_BLOCKED_TO_N
			   && (len == RESOURCE_X_CONTROL_V1_BYTES || len == RESOURCE_X_PROOF_V1_BYTES);
	if (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE)
		return msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT && len == RESOURCE_X_IMAGE_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT)
		return msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT && len == RESOURCE_X_PROOF_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_RELEASE_X)
		return msg_type == RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE
			   && len == RESOURCE_X_CONTROL_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT)
		return msg_type == RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE && len == RESOURCE_X_SHORT_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2)
		return msg_type == RESOURCE_X_MSG_BLOCK_TO_N && len == RESOURCE_X_PROOF_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2)
		return msg_type == RESOURCE_X_MSG_BLOCKED_TO_N && len == RESOURCE_X_PROOF_V1_BYTES;
	return false;
}


static bool
resource_x_frame_length(const ResourceXDecodedFrame *frame, uint16 *len)
{
	switch (frame->kind) {
	case RESOURCE_X_WIRE_ASSERT_X:
	case RESOURCE_X_WIRE_BLOCK_TO_N:
	case RESOURCE_X_WIRE_RELEASE_X:
	case RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP:
		*len = RESOURCE_X_CONTROL_V1_BYTES;
		return true;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
		*len = frame->blocked_has_remote_proof ? RESOURCE_X_PROOF_V1_BYTES
											   : RESOURCE_X_CONTROL_V1_BYTES;
		return true;
	case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:
		*len = RESOURCE_X_SHORT_V1_BYTES;
		return true;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:
		*len = RESOURCE_X_PROOF_V1_BYTES;
		return true;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE:
		*len = RESOURCE_X_IMAGE_V1_BYTES;
		return true;
	default:
		return false;
	}
}


static bool
resource_x_bootstrap_common_valid(uint8 msg_type, const ResourceXDecodedCommon *common,
								  ResourceXWireReject *reject)
{
	bool request = msg_type == RESOURCE_X_MSG_ASSERT_X;
	bool ack = msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT;

	if (!request && !ack) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
		return false;
	}
	if (!resource_x_assertion_valid(&common->logical_assertion)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
		return false;
	}
	if ((request && common->base_authority_generation != 0)
		|| (ack && common->base_authority_generation == 0) || common->resource_formation == 0
		|| common->master_session_incarnation == 0 || common->assertion_sequence == 0
		|| common->sender_connection_generation == 0 || common->authority_generation != 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	if (common->ordered_lane != 0 || common->action_node != common->logical_assertion.requester_node
		|| common->observed_mode != (uint8)PCM_STATE_N || common->target_mode != (uint8)PCM_STATE_X
		|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
		return false;
	}
	if (common->outcome != (request ? RESOURCE_X_OUTCOME_NONE : RESOURCE_X_OUTCOME_OK)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ENUM);
		return false;
	}
	if (common->flags != 0
		&& (!request || common->flags != RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_FLAGS);
		return false;
	}
	return true;
}


static bool
resource_x_source_settlement_common_valid(uint8 msg_type, ResourceXWireKind kind,
										  const ResourceXDecodedCommon *common,
										  ResourceXWireReject *reject)
{
	bool request = kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	bool ack = kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2;

	if ((!request && !ack) || (request && msg_type != RESOURCE_X_MSG_BLOCK_TO_N)
		|| (ack && msg_type != RESOURCE_X_MSG_BLOCKED_TO_N)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
		return false;
	}
	if (!resource_x_assertion_valid(&common->logical_assertion)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
		return false;
	}
	if (common->base_authority_generation == 0 || common->base_authority_generation == UINT64_MAX
		|| common->authority_generation <= common->base_authority_generation
		|| common->authority_generation == UINT64_MAX || common->resource_formation == 0
		|| common->master_session_incarnation == 0 || common->assertion_sequence == 0
		|| common->sender_connection_generation == 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	if (common->ordered_lane != 0 || common->action_node < 0
		|| common->action_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| common->action_node == common->logical_assertion.requester_node
		|| common->observed_mode != (uint8)PCM_STATE_N || common->target_mode != (uint8)PCM_STATE_N
		|| common->source_candidate != 1 || common->retain_pi_if_dirty != 1) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
		return false;
	}
	if (common->outcome != (request ? RESOURCE_X_OUTCOME_NONE : RESOURCE_X_OUTCOME_OK)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ENUM);
		return false;
	}
	if (common->flags != 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_FLAGS);
		return false;
	}
	return true;
}


static bool
resource_x_common_valid(ResourceXWireKind kind, const ResourceXDecodedCommon *common,
						ResourceXWireReject *reject)
{
	uint8 allowed_flags = 0;

	if (!resource_x_assertion_valid(&common->logical_assertion)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
		return false;
	}
	if (common->base_authority_generation == 0 || common->resource_formation == 0
		|| common->master_session_incarnation == 0 || common->assertion_sequence == 0
		|| common->sender_connection_generation == 0 || common->authority_generation == 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	if (common->action_node < 0 || common->action_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
		return false;
	}
	if (common->observed_mode > (uint8)PCM_STATE_X || common->target_mode > (uint8)PCM_STATE_X
		|| common->source_candidate > 1 || common->retain_pi_if_dirty > 1
		|| common->outcome > RESOURCE_X_OUTCOME_CORRUPT) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ENUM);
		return false;
	}
	if (kind == RESOURCE_X_WIRE_ASSERT_X) {
		if (common->action_node != common->logical_assertion.requester_node
			|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->outcome != RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_BLOCK_TO_N) {
		bool direct_x_source = common->observed_mode == (uint8)PCM_STATE_X
							   && common->source_candidate == 1 && common->retain_pi_if_dirty == 1;
		bool shared_s_holder = common->observed_mode == (uint8)PCM_STATE_S
							   && common->source_candidate == common->retain_pi_if_dirty;

		allowed_flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;

		if (common->target_mode != (uint8)PCM_STATE_N || (!direct_x_source && !shared_s_holder)) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
		if (common->outcome != RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ENUM);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N) {
		allowed_flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
		if (common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->outcome == RESOURCE_X_OUTCOME_NONE
			|| ((common->flags & RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED) != 0
				&& common->outcome != RESOURCE_X_OUTCOME_OK)) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_FLAGS);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_RELEASE_X) {
		if (common->action_node != common->logical_assertion.requester_node
			|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->outcome == RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT) {
		allowed_flags = RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED;
		if (common->action_node != common->logical_assertion.requester_node
			|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->target_mode != (uint8)PCM_STATE_X
			|| common->outcome == RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION
			   || kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT) {
		if (common->action_node != common->logical_assertion.requester_node
			|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->target_mode != (uint8)PCM_STATE_X
			|| common->outcome == RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
	} else if (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		allowed_flags = RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE;
		if (common->source_candidate != 0 || common->retain_pi_if_dirty != 0
			|| common->target_mode != (uint8)PCM_STATE_X
			|| common->outcome == RESOURCE_X_OUTCOME_NONE) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
	}
	if ((common->flags & ~allowed_flags) != 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_FLAGS);
		return false;
	}
	if ((common->flags & RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE) != 0) {
		bool selected_block = kind == RESOURCE_X_WIRE_BLOCK_TO_N && common->source_candidate == 1
							  && common->retain_pi_if_dirty == 1
							  && common->action_node != common->logical_assertion.requester_node;
		bool target_image = kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
							&& common->action_node == common->logical_assertion.requester_node
							&& common->outcome == RESOURCE_X_OUTCOME_OK;

		if ((!selected_block && !target_image)
			|| (common->observed_mode != (uint8)PCM_STATE_X
				&& common->observed_mode != (uint8)PCM_STATE_S)) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
			return false;
		}
		if (common->authority_generation <= common->base_authority_generation
			|| common->authority_generation == UINT64_MAX) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
			return false;
		}
	}
	return true;
}


static bool
resource_x_pair_common_valid(uint8 msg_type, ResourceXWireKind kind,
							 const ResourceXDecodedCommon *common, ResourceXWireReject *reject)
{
	if (kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP)
		return resource_x_bootstrap_common_valid(msg_type, common, reject);
	if (kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2)
		return resource_x_source_settlement_common_valid(msg_type, kind, common, reject);
	return resource_x_common_valid(kind, common, reject);
}


static void
resource_x_common_encode(uint8 *bytes, ResourceXWireKind kind, const ResourceXDecodedCommon *common,
						 uint16 len)
{
	bytes[0] = RESOURCE_X_WIRE_VERSION;
	bytes[1] = (uint8)kind;
	resource_x_put_u16(bytes + 2, len);
	resource_x_assertion_encode(bytes + 8, &common->logical_assertion);
	resource_x_put_u64(bytes + 32, common->base_authority_generation);
	resource_x_put_u64(bytes + 40, common->resource_formation);
	resource_x_put_u64(bytes + 48, common->master_session_incarnation);
	resource_x_put_u64(bytes + 56, common->assertion_sequence);
	resource_x_put_u32(bytes + 64, common->ordered_lane);
	resource_x_put_u32(bytes + 68, (uint32)common->action_node);
	bytes[72] = common->observed_mode;
	bytes[73] = common->target_mode;
	bytes[74] = common->source_candidate;
	bytes[75] = common->retain_pi_if_dirty;
	resource_x_put_u32(bytes + 76, common->sender_connection_generation);
	bytes[80] = common->outcome;
	bytes[81] = common->flags;
	resource_x_put_u64(bytes + 88, common->authority_generation);
}


static bool
resource_x_common_decode(uint8 msg_type, const uint8 *bytes, ResourceXWireKind kind,
						 ResourceXDecodedCommon *common, ResourceXWireReject *reject)
{
	memset(common, 0, sizeof(*common));
	if (!resource_x_bytes_zero(bytes + 82, 6)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
		return false;
	}
	resource_x_assertion_decode(bytes + 8, &common->logical_assertion);
	common->base_authority_generation = resource_x_get_u64(bytes + 32);
	common->resource_formation = resource_x_get_u64(bytes + 40);
	common->master_session_incarnation = resource_x_get_u64(bytes + 48);
	common->assertion_sequence = resource_x_get_u64(bytes + 56);
	common->ordered_lane = resource_x_get_u32(bytes + 64);
	common->action_node = (int32)resource_x_get_u32(bytes + 68);
	common->observed_mode = bytes[72];
	common->target_mode = bytes[73];
	common->source_candidate = bytes[74];
	common->retain_pi_if_dirty = bytes[75];
	common->sender_connection_generation = resource_x_get_u32(bytes + 76);
	common->outcome = bytes[80];
	common->flags = bytes[81];
	common->authority_generation = resource_x_get_u64(bytes + 88);
	return resource_x_pair_common_valid(msg_type, kind, common, reject);
}


static bool
resource_x_dependencies_valid(const uint64 *dependencies, uint16 count)
{
	int i;

	if (count > RESOURCE_X_DEPENDENCY_MAX)
		return false;
	for (i = count; i < RESOURCE_X_DEPENDENCY_MAX; i++)
		if (dependencies[i] != 0)
			return false;
	return true;
}


static bool
resource_x_disposition_matches(uint8 proof_kind, uint8 disposition)
{
	return (proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
			&& disposition == RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE)
		   || (proof_kind == RESOURCE_X_PROOF_LOCAL_IMAGE
			   && disposition == RESOURCE_X_DISPOSITION_LOCAL_IMAGE)
		   || (proof_kind == RESOURCE_X_PROOF_DURABLE_STORAGE
			   && disposition == RESOURCE_X_DISPOSITION_DURABLE_STORAGE);
}


static bool
resource_x_body_valid(const ResourceXDecodedFrame *frame, ResourceXWireReject *reject)
{
	bool valid = true;

	switch (frame->kind) {
	case RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP:
		valid = true;
		break;
	case RESOURCE_X_WIRE_ASSERT_X:
	case RESOURCE_X_WIRE_BLOCK_TO_N:
	case RESOURCE_X_WIRE_RELEASE_X:
		break;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
		if (frame->blocked_has_remote_proof) {
			const ResourceXDecodedBlockedToN *body = &frame->body.blocked_to_n;

			valid = body->source_carrier_generation != 0
					&& body->requester_target_generation == frame->common.assertion_sequence
					&& body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
					&& body->source_disposition == RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
					&& body->proof_flags == 0 && body->holder_connection_generation != 0
					&& body->acting_formation == frame->common.resource_formation
					&& resource_x_dependencies_valid(body->dependencies, body->dependency_count);
		}
		break;
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2: {
		const ResourceXDecodedBlockedToN *body = &frame->body.blocked_to_n;

		valid = frame->blocked_has_remote_proof && body->source_carrier_generation != 0
				&& body->requester_target_generation == frame->common.assertion_sequence
				&& body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
				&& body->source_disposition == RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
				&& body->proof_flags == 0 && body->holder_connection_generation != 0
				&& body->acting_formation == frame->common.resource_formation
				&& resource_x_dependencies_valid(body->dependencies, body->dependency_count);
	} break;
	case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
		valid = frame->body.local_proof.local_holder_authority_generation != 0
				&& frame->body.local_proof.requester_target_generation
					   == frame->common.assertion_sequence
				&& frame->body.local_proof.dependency_count <= RESOURCE_X_DEPENDENCY_MAX
				&& frame->body.local_proof.requester_connection_generation != 0
				&& frame->body.local_proof.local_proof_generation != 0;
		break;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT: {
		const ResourceXDecodedAuthorityGrant *body = &frame->body.authority_grant;
		bool remote = body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER;

		valid = body->final_authority_generation == frame->common.authority_generation
				&& body->requester_target_generation == frame->common.assertion_sequence
				&& body->proof_kind >= RESOURCE_X_PROOF_KIND_MIN
				&& body->proof_kind <= RESOURCE_X_PROOF_KIND_MAX
				&& resource_x_disposition_matches(body->proof_kind, body->source_disposition)
				&& body->grant_flags == 0 && body->requester_connection_generation != 0
				&& resource_x_dependencies_valid(body->dependencies, body->dependency_count)
				&& (((frame->common.flags & RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED) != 0)
					== remote)
				&& (!remote || body->source_carrier_generation != 0);
	} break;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE: {
		const ResourceXDecodedImageEnvelope *body = &frame->body.image_envelope;

		valid = body->conversion_base_generation == frame->common.base_authority_generation
				&& body->source_carrier_generation != 0
				&& body->requester_target_generation == frame->common.assertion_sequence
				&& body->image_length == RESOURCE_X_PAGE_BYTES
				&& body->source_disposition == RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
				&& body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER && body->image_flags == 0
				&& resource_x_dependencies_valid(body->dependencies, body->dependency_count);
	} break;
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT: {
		const ResourceXDecodedInstallSettlement *body = &frame->body.install_settlement;

		valid = body->conversion_base_generation == frame->common.base_authority_generation
				&& body->final_authority_generation == frame->common.authority_generation
				&& body->requester_connection_generation != 0
				&& body->requester_target_generation == frame->common.assertion_sequence
				&& body->installed_mode == (uint8)PCM_STATE_X
				&& body->requester_role == RESOURCE_X_REQUESTER_ROLE_ACQUIRER
				&& body->terminal_outcome > RESOURCE_X_OUTCOME_NONE
				&& body->terminal_outcome <= RESOURCE_X_OUTCOME_CORRUPT
				&& body->terminal_state >= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED
				&& body->terminal_state <= RESOURCE_X_SETTLEMENT_TERMINAL_EXCLUDED
				&& body->settlement_flags == 0;
	} break;
	default:
		valid = false;
		break;
	}
	if (!valid)
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BODY);
	return valid;
}


static void
resource_x_dependencies_encode(uint8 *bytes, const uint64 *dependencies)
{
	int i;

	for (i = 0; i < RESOURCE_X_DEPENDENCY_MAX; i++)
		resource_x_put_u64(bytes + i * 8, dependencies[i]);
}


static void
resource_x_dependencies_decode(const uint8 *bytes, uint64 *dependencies)
{
	int i;

	for (i = 0; i < RESOURCE_X_DEPENDENCY_MAX; i++)
		dependencies[i] = resource_x_get_u64(bytes + i * 8);
}


static void
resource_x_body_encode(uint8 *bytes, const ResourceXDecodedFrame *frame)
{
	switch (frame->kind) {
	case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
		resource_x_put_u64(bytes + 96, frame->body.local_proof.local_holder_authority_generation);
		resource_x_put_u64(bytes + 104, frame->body.local_proof.requester_target_generation);
		resource_x_put_u64(bytes + 112, frame->body.local_proof.page_scn_lsn);
		resource_x_put_u16(bytes + 120, frame->body.local_proof.dependency_count);
		resource_x_put_u32(bytes + 124, frame->body.local_proof.dependency_vector_crc32c);
		resource_x_put_u32(bytes + 128, frame->body.local_proof.page_checksum);
		resource_x_put_u32(bytes + 132, frame->body.local_proof.local_image_proof_crc32c);
		resource_x_put_u64(bytes + 136, frame->body.local_proof.requester_connection_generation);
		resource_x_put_u64(bytes + 144, frame->body.local_proof.local_proof_generation);
		break;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:
		if (frame->blocked_has_remote_proof) {
			const ResourceXDecodedBlockedToN *body = &frame->body.blocked_to_n;

			memcpy(bytes + 96, body->source_fence, RESOURCE_X_SOURCE_FENCE_BYTES);
			resource_x_put_u64(bytes + 130, body->source_carrier_generation);
			resource_x_put_u64(bytes + 138, body->requester_target_generation);
			resource_x_put_u64(bytes + 146, body->page_scn_lsn);
			resource_x_put_u16(bytes + 154, body->dependency_count);
			resource_x_dependencies_encode(bytes + 156, body->dependencies);
			resource_x_put_u32(bytes + 284, body->source_proof_crc32c);
			resource_x_put_u32(bytes + 288, body->page_checksum);
			bytes[292] = body->source_disposition;
			bytes[293] = body->proof_kind;
			resource_x_put_u16(bytes + 294, body->proof_flags);
			resource_x_put_u64(bytes + 296, body->holder_connection_generation);
			resource_x_put_u64(bytes + 304, body->acting_formation);
		}
		break;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT: {
		const ResourceXDecodedAuthorityGrant *body = &frame->body.authority_grant;

		memcpy(bytes + 96, body->source_fence, RESOURCE_X_SOURCE_FENCE_BYTES);
		resource_x_put_u64(bytes + 130, body->final_authority_generation);
		resource_x_put_u64(bytes + 138, body->source_carrier_generation);
		resource_x_put_u64(bytes + 146, body->requester_target_generation);
		resource_x_put_u64(bytes + 154, body->page_scn_lsn);
		resource_x_put_u16(bytes + 162, body->dependency_count);
		resource_x_dependencies_encode(bytes + 164, body->dependencies);
		resource_x_put_u32(bytes + 292, body->source_proof_crc32c);
		resource_x_put_u32(bytes + 296, body->page_checksum);
		bytes[300] = body->proof_kind;
		bytes[301] = body->source_disposition;
		resource_x_put_u16(bytes + 302, body->grant_flags);
		resource_x_put_u64(bytes + 304, body->requester_connection_generation);
	} break;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE: {
		const ResourceXDecodedImageEnvelope *body = &frame->body.image_envelope;

		memcpy(bytes + 96, body->request_tail, RESOURCE_X_REQUEST_TAIL_BYTES);
		resource_x_put_u64(bytes + 116, body->conversion_base_generation);
		memcpy(bytes + 124, body->source_fence, RESOURCE_X_SOURCE_FENCE_BYTES);
		resource_x_put_u64(bytes + 158, body->source_carrier_generation);
		resource_x_put_u64(bytes + 166, body->requester_target_generation);
		resource_x_put_u64(bytes + 174, body->page_scn_lsn);
		resource_x_put_u16(bytes + 182, body->dependency_count);
		resource_x_dependencies_encode(bytes + 184, body->dependencies);
		resource_x_put_u32(bytes + 312, body->dependency_vector_crc32c);
		resource_x_put_u32(bytes + 316, body->page_checksum);
		resource_x_put_u32(bytes + 320, body->image_length);
		bytes[324] = body->source_disposition;
		bytes[325] = body->proof_kind;
		resource_x_put_u16(bytes + 326, body->image_flags);
		memcpy(bytes + 328, body->page_bytes, RESOURCE_X_PAGE_BYTES);
	} break;
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT: {
		const ResourceXDecodedInstallSettlement *body = &frame->body.install_settlement;

		resource_x_put_u64(bytes + 96, body->conversion_base_generation);
		resource_x_put_u64(bytes + 104, body->final_authority_generation);
		resource_x_put_u64(bytes + 112, body->requester_connection_generation);
		resource_x_put_u64(bytes + 120, body->requester_target_generation);
		resource_x_put_u64(bytes + 128, body->page_scn_lsn);
		resource_x_put_u32(bytes + 136, body->page_checksum);
		resource_x_put_u32(bytes + 140, body->source_proof_crc32c);
		bytes[144] = body->installed_mode;
		bytes[145] = body->requester_role;
		bytes[146] = body->terminal_outcome;
		bytes[147] = body->terminal_state;
		resource_x_put_u32(bytes + 148, body->settlement_flags);
	} break;
	default:
		break;
	}
}


static bool
resource_x_body_decode(const uint8 *bytes, ResourceXDecodedFrame *frame,
					   ResourceXWireReject *reject)
{
	switch (frame->kind) {
	case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
		if (!resource_x_bytes_zero(bytes + 122, 2)) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
			return false;
		}
		frame->body.local_proof.local_holder_authority_generation = resource_x_get_u64(bytes + 96);
		frame->body.local_proof.requester_target_generation = resource_x_get_u64(bytes + 104);
		frame->body.local_proof.page_scn_lsn = resource_x_get_u64(bytes + 112);
		frame->body.local_proof.dependency_count = resource_x_get_u16(bytes + 120);
		frame->body.local_proof.dependency_vector_crc32c = resource_x_get_u32(bytes + 124);
		frame->body.local_proof.page_checksum = resource_x_get_u32(bytes + 128);
		frame->body.local_proof.local_image_proof_crc32c = resource_x_get_u32(bytes + 132);
		frame->body.local_proof.requester_connection_generation = resource_x_get_u64(bytes + 136);
		frame->body.local_proof.local_proof_generation = resource_x_get_u64(bytes + 144);
		break;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:
		if (frame->blocked_has_remote_proof) {
			ResourceXDecodedBlockedToN *body = &frame->body.blocked_to_n;

			memcpy(body->source_fence, bytes + 96, RESOURCE_X_SOURCE_FENCE_BYTES);
			body->source_carrier_generation = resource_x_get_u64(bytes + 130);
			body->requester_target_generation = resource_x_get_u64(bytes + 138);
			body->page_scn_lsn = resource_x_get_u64(bytes + 146);
			body->dependency_count = resource_x_get_u16(bytes + 154);
			resource_x_dependencies_decode(bytes + 156, body->dependencies);
			body->source_proof_crc32c = resource_x_get_u32(bytes + 284);
			body->page_checksum = resource_x_get_u32(bytes + 288);
			body->source_disposition = bytes[292];
			body->proof_kind = bytes[293];
			body->proof_flags = resource_x_get_u16(bytes + 294);
			body->holder_connection_generation = resource_x_get_u64(bytes + 296);
			body->acting_formation = resource_x_get_u64(bytes + 304);
		}
		break;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT: {
		ResourceXDecodedAuthorityGrant *body = &frame->body.authority_grant;

		memcpy(body->source_fence, bytes + 96, RESOURCE_X_SOURCE_FENCE_BYTES);
		body->final_authority_generation = resource_x_get_u64(bytes + 130);
		body->source_carrier_generation = resource_x_get_u64(bytes + 138);
		body->requester_target_generation = resource_x_get_u64(bytes + 146);
		body->page_scn_lsn = resource_x_get_u64(bytes + 154);
		body->dependency_count = resource_x_get_u16(bytes + 162);
		resource_x_dependencies_decode(bytes + 164, body->dependencies);
		body->source_proof_crc32c = resource_x_get_u32(bytes + 292);
		body->page_checksum = resource_x_get_u32(bytes + 296);
		body->proof_kind = bytes[300];
		body->source_disposition = bytes[301];
		body->grant_flags = resource_x_get_u16(bytes + 302);
		body->requester_connection_generation = resource_x_get_u64(bytes + 304);
	} break;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE: {
		ResourceXDecodedImageEnvelope *body = &frame->body.image_envelope;

		memcpy(body->request_tail, bytes + 96, RESOURCE_X_REQUEST_TAIL_BYTES);
		body->conversion_base_generation = resource_x_get_u64(bytes + 116);
		memcpy(body->source_fence, bytes + 124, RESOURCE_X_SOURCE_FENCE_BYTES);
		body->source_carrier_generation = resource_x_get_u64(bytes + 158);
		body->requester_target_generation = resource_x_get_u64(bytes + 166);
		body->page_scn_lsn = resource_x_get_u64(bytes + 174);
		body->dependency_count = resource_x_get_u16(bytes + 182);
		resource_x_dependencies_decode(bytes + 184, body->dependencies);
		body->dependency_vector_crc32c = resource_x_get_u32(bytes + 312);
		body->page_checksum = resource_x_get_u32(bytes + 316);
		body->image_length = resource_x_get_u32(bytes + 320);
		body->source_disposition = bytes[324];
		body->proof_kind = bytes[325];
		body->image_flags = resource_x_get_u16(bytes + 326);
		memcpy(body->page_bytes, bytes + 328, RESOURCE_X_PAGE_BYTES);
	} break;
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT: {
		ResourceXDecodedInstallSettlement *body = &frame->body.install_settlement;

		body->conversion_base_generation = resource_x_get_u64(bytes + 96);
		body->final_authority_generation = resource_x_get_u64(bytes + 104);
		body->requester_connection_generation = resource_x_get_u64(bytes + 112);
		body->requester_target_generation = resource_x_get_u64(bytes + 120);
		body->page_scn_lsn = resource_x_get_u64(bytes + 128);
		body->page_checksum = resource_x_get_u32(bytes + 136);
		body->source_proof_crc32c = resource_x_get_u32(bytes + 140);
		body->installed_mode = bytes[144];
		body->requester_role = bytes[145];
		body->terminal_outcome = bytes[146];
		body->terminal_state = bytes[147];
		body->settlement_flags = resource_x_get_u32(bytes + 148);
	} break;
	default:
		break;
	}
	return resource_x_body_valid(frame, reject);
}


bool
cluster_resource_x_wire_encode(uint8 msg_type, const ResourceXDecodedFrame *frame, void *payload,
							   uint16 payload_capacity, uint16 *payload_len_out,
							   ResourceXWireReject *reject)
{
	uint8 encoded[RESOURCE_X_IMAGE_V1_BYTES];
	uint32 crc;
	uint16 len;

	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT);
	if (frame == NULL || payload == NULL || payload_len_out == NULL)
		return false;
	if (!resource_x_frame_length(frame, &len)
		|| !resource_x_pair_length(msg_type, frame->kind, len)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
		return false;
	}
	if (payload_capacity < len) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_DECLARED_LENGTH);
		return false;
	}
	if (!resource_x_pair_common_valid(msg_type, frame->kind, &frame->common, reject))
		return false;
	if (!resource_x_body_valid(frame, reject))
		return false;

	memset(encoded, 0, sizeof(encoded));
	resource_x_common_encode(encoded, frame->kind, &frame->common, len);
	resource_x_body_encode(encoded, frame);
	crc = resource_x_wire_crc(encoded, len);
	resource_x_put_u32(encoded + 4, crc);
	memcpy(payload, encoded, len);
	*payload_len_out = len;
	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_NONE);
	return true;
}


bool
cluster_resource_x_wire_decode(uint8 msg_type, const void *payload, uint16 payload_len,
							   ResourceXDecodedFrame *out, ResourceXWireReject *reject)
{
	const uint8 *bytes = (const uint8 *)payload;
	ResourceXDecodedFrame decoded;
	ResourceXWireKind kind;

	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT);
	if (bytes == NULL || out == NULL)
		return false;
	if (payload_len != RESOURCE_X_CONTROL_V1_BYTES && payload_len != RESOURCE_X_SHORT_V1_BYTES
		&& payload_len != RESOURCE_X_PROOF_V1_BYTES && payload_len != RESOURCE_X_IMAGE_V1_BYTES) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
		return false;
	}
	kind = (ResourceXWireKind)bytes[1];
	if (!resource_x_pair_length(msg_type, kind, payload_len)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
		return false;
	}
	if (bytes[0] != RESOURCE_X_WIRE_VERSION) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_VERSION);
		return false;
	}
	if (resource_x_get_u16(bytes + 2) != payload_len) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_DECLARED_LENGTH);
		return false;
	}
	if (resource_x_get_u32(bytes + 4) != resource_x_wire_crc(bytes, payload_len)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_CRC);
		return false;
	}
	memset(&decoded, 0, sizeof(decoded));
	decoded.kind = kind;
	decoded.payload_bytes = payload_len;
	decoded.blocked_has_remote_proof
		= (kind == RESOURCE_X_WIRE_BLOCKED_TO_N || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		   || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2)
		  && payload_len == RESOURCE_X_PROOF_V1_BYTES;
	if (!resource_x_common_decode(msg_type, bytes, kind, &decoded.common, reject))
		return false;
	decoded.common.semantic_crc32c = resource_x_get_u32(bytes + 4);
	if (!resource_x_body_decode(bytes, &decoded, reject))
		return false;
	*out = decoded;
	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_NONE);
	return true;
}


/* sender_connection_generation is transport freshness, not logical
 * assertion identity.  Retained owner bytes therefore bind it only when a
 * physical copy is staged for one exact HELLO-authenticated DATA session.
 * Decode first so malformed retained bytes can never be repaired into a
 * sendable frame merely by resealing their CRC. */
bool
cluster_resource_x_wire_rebind_sender_generation(uint8 msg_type, void *payload, uint16 payload_len,
												 uint32 sender_connection_generation,
												 ResourceXWireReject *reject)
{
	ResourceXDecodedFrame decoded;
	uint8 *bytes = (uint8 *)payload;
	uint32 crc;

	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT);
	if (bytes == NULL || sender_connection_generation == 0
		|| !cluster_resource_x_wire_decode(msg_type, bytes, payload_len, &decoded, reject))
		return false;
	resource_x_put_u32(bytes + 76, sender_connection_generation);
	resource_x_put_u32(bytes + 4, 0);
	crc = resource_x_wire_crc(bytes, payload_len);
	resource_x_put_u32(bytes + 4, crc);
	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_NONE);
	return true;
}
