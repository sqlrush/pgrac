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


/* Fixed IMAGE extension; every integer is network order. The four bytes at
 * 132 are reserved, not the native source structure's padding. */
static void
resource_x_page_wal_encode(uint8 *p, const ClusterPageWalBindingV1 *b)
{
	memset(p, 0, 232);
	resource_x_put_u64(p + 0, b->source.claim.identity.system_identifier);
	memcpy(p + 8, b->source.claim.identity.storage_uuid, 16);
	memcpy(p + 24, b->source.claim.identity.authority_uuid, 16);
	resource_x_put_u16(p + 40, b->source.claim.identity.origin_thread_id);
	resource_x_put_u16(p + 42, b->source.claim.identity.reserved42);
	resource_x_put_u32(p + 44, b->source.claim.identity.origin_node_id);
	resource_x_put_u64(p + 48, b->source.claim.identity.thread_claim_created_at);
	resource_x_put_u32(p + 56, b->source.claim.identity.thread_claim_crc32c);
	resource_x_put_u32(p + 60, b->source.claim.identity.reserved60);
	resource_x_put_u64(p + 64, b->source.claim.identity.origin_owner_incarnation);
	resource_x_put_u64(p + 72, b->source.claim.identity.root_lineage_seq);
	resource_x_put_u64(p + 80, b->source.claim.database_incarnation);
	resource_x_put_u64(p + 88, b->source.claim.max_config_generation);
	memcpy(p + 96, b->source.claim.claim_sha256, 32);
	resource_x_put_u32(p + 128, b->source.timeline);
	resource_x_put_u64(p + 136, b->identity.system_identifier);
	memcpy(p + 144, b->identity.storage_uuid, 16);
	resource_x_put_u32(p + 160, b->identity.locator.spcOid);
	resource_x_put_u32(p + 164, b->identity.locator.dbOid);
	resource_x_put_u32(p + 168, b->identity.locator.relNumber);
	resource_x_put_u32(p + 172, b->identity.forknum);
	resource_x_put_u32(p + 176, b->identity.blockno);
	resource_x_put_u32(p + 180, b->identity.reserved_zero);
	memcpy(p + 184, b->version.segment_incarnation, 16);
	resource_x_put_u64(p + 200, b->version.mutation_token);
	resource_x_put_u64(p + 208, b->record_start);
	resource_x_put_u64(p + 216, b->record_end);
	resource_x_put_u32(p + 224, b->record_crc);
	p[228] = b->rmid;
	p[229] = b->info;
	resource_x_put_u16(p + 230, b->flags);
}

static void
resource_x_page_wal_decode(const uint8 *p, ClusterPageWalBindingV1 *b)
{
	memset(b, 0, sizeof(*b));
	b->source.claim.identity.system_identifier = resource_x_get_u64(p + 0);
	memcpy(b->source.claim.identity.storage_uuid, p + 8, 16);
	memcpy(b->source.claim.identity.authority_uuid, p + 24, 16);
	b->source.claim.identity.origin_thread_id = resource_x_get_u16(p + 40);
	b->source.claim.identity.reserved42 = resource_x_get_u16(p + 42);
	b->source.claim.identity.origin_node_id = (int32)resource_x_get_u32(p + 44);
	b->source.claim.identity.thread_claim_created_at = (int64)resource_x_get_u64(p + 48);
	b->source.claim.identity.thread_claim_crc32c = resource_x_get_u32(p + 56);
	b->source.claim.identity.reserved60 = resource_x_get_u32(p + 60);
	b->source.claim.identity.origin_owner_incarnation = resource_x_get_u64(p + 64);
	b->source.claim.identity.root_lineage_seq = resource_x_get_u64(p + 72);
	b->source.claim.database_incarnation = resource_x_get_u64(p + 80);
	b->source.claim.max_config_generation = resource_x_get_u64(p + 88);
	memcpy(b->source.claim.claim_sha256, p + 96, 32);
	b->source.timeline = resource_x_get_u32(p + 128);
	b->identity.system_identifier = resource_x_get_u64(p + 136);
	memcpy(b->identity.storage_uuid, p + 144, 16);
	b->identity.locator.spcOid = resource_x_get_u32(p + 160);
	b->identity.locator.dbOid = resource_x_get_u32(p + 164);
	b->identity.locator.relNumber = resource_x_get_u32(p + 168);
	b->identity.forknum = resource_x_get_u32(p + 172);
	b->identity.blockno = resource_x_get_u32(p + 176);
	b->identity.reserved_zero = resource_x_get_u32(p + 180);
	memcpy(b->version.segment_incarnation, p + 184, 16);
	b->version.mutation_token = resource_x_get_u64(p + 200);
	b->record_start = resource_x_get_u64(p + 208);
	b->record_end = resource_x_get_u64(p + 216);
	b->record_crc = resource_x_get_u32(p + 224);
	b->rmid = p[228];
	b->info = p[229];
	b->flags = resource_x_get_u16(p + 230);
}

static bool
resource_x_image_wal_valid(const ResourceXDecodedFrame *frame)
{
	const ResourceXDecodedImageEnvelope *body = &frame->body.image_envelope;
	const BufferTag *tag = &frame->common.logical_assertion.resource;
	PGAlignedBlock page;
	if (body->image_flags == 0)
		return resource_x_bytes_zero((const uint8 *)&body->page_wal, sizeof(body->page_wal));
	if (body->image_flags != RESOURCE_X_IMAGE_HAS_WAL
		|| body->page_wal.flags != CLUSTER_PAGE_WAL_NATIVE_FLUSHED)
		return false;
	memcpy(page.data, body->page_bytes, BLCKSZ);
	return body->page_scn_lsn == body->page_wal.version.mutation_token
		   && cluster_page_wal_binding_matches_v1(&body->page_wal, BufTagGetRelFileLocator(tag),
												  tag->forkNum, tag->blockNum, page.data);
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
		return msg_type == RESOURCE_X_MSG_ASSERT_X
			   && (len == RESOURCE_X_CONTROL_V1_BYTES || len == RESOURCE_X_ACQUIRE_V3_BYTES);
	if (kind == RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION)
		return msg_type == RESOURCE_X_MSG_ASSERT_X && len == RESOURCE_X_SHORT_V1_BYTES;
	if (kind == RESOURCE_X_WIRE_BLOCK_TO_N)
		return msg_type == RESOURCE_X_MSG_BLOCK_TO_N
			&& (len == RESOURCE_X_CONTROL_V1_BYTES || len == RESOURCE_X_DECISION_V3_BYTES);
	if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N)
		return msg_type == RESOURCE_X_MSG_BLOCKED_TO_N
			   && (len == RESOURCE_X_CONTROL_V1_BYTES || len == RESOURCE_X_PROOF_V1_BYTES
				   || len == RESOURCE_X_WITNESS_V3_BYTES || len == RESOURCE_X_SOURCE_WITNESS_V3_BYTES);
	if (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE)
		return msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT
			&& (len == RESOURCE_X_IMAGE_V2_BYTES || len == RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES);
	if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT)
		return msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT
			&& (len == RESOURCE_X_PROOF_V1_BYTES || len == RESOURCE_X_DECISION_V3_BYTES);
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
	if (frame->has_acquire_decision) {
		if (frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
			if (frame->has_acquire_intent || frame->blocked_has_remote_proof)
				return false;
			*len = RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES;
			return true;
		}
		if (frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N) {
			if (frame->has_acquire_intent || frame->blocked_has_remote_proof)
				return false;
			*len = frame->body.acquire_witness.has_source_proof
				? RESOURCE_X_SOURCE_WITNESS_V3_BYTES : RESOURCE_X_WITNESS_V3_BYTES;
			return true;
		}
		if (frame->has_acquire_intent
			|| (frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
				&& frame->kind != RESOURCE_X_WIRE_BLOCK_TO_N))
			return false;
		*len = RESOURCE_X_DECISION_V3_BYTES;
		return true;
	}
	if (frame->has_acquire_intent) {
		if (frame->kind != RESOURCE_X_WIRE_ASSERT_X)
			return false;
		*len = RESOURCE_X_ACQUIRE_V3_BYTES;
		return true;
	}
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
		*len = RESOURCE_X_IMAGE_V2_BYTES;
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
resource_x_acquire_common_valid(const ResourceXDecodedCommon *common,
								ResourceXWireReject *reject)
{
	if (!resource_x_assertion_valid(&common->logical_assertion)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
		return false;
	}
	if (common->resource_formation == 0 || common->resource_formation == UINT64_MAX
		|| common->master_session_incarnation == 0
		|| common->master_session_incarnation == UINT64_MAX
		|| common->assertion_sequence == 0 || common->assertion_sequence == UINT64_MAX
		|| common->sender_connection_generation == 0
		|| common->sender_connection_generation == UINT32_MAX) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	/* Native base/token live in the typed tail. The requester does not select
	 * the master canonical base, queue order, source or final authority. */
	if (common->base_authority_generation != 0 || common->authority_generation != 0
		|| common->ordered_lane != 0
		|| common->action_node != common->logical_assertion.requester_node
		|| common->source_candidate != 0 || common->retain_pi_if_dirty != 0) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
		return false;
	}
	if (common->observed_mode > (uint8)PCM_STATE_X
		|| (common->target_mode != (uint8)PCM_STATE_S && common->target_mode != (uint8)PCM_STATE_X)
		|| common->outcome != RESOURCE_X_OUTCOME_NONE) {
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
resource_x_decision_common_valid(const ResourceXDecodedCommon *common,
								 ResourceXWireReject *reject)
{
	if (!resource_x_assertion_valid(&common->logical_assertion)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_IDENTITY);
		return false;
	}
	if (common->base_authority_generation == 0
		|| common->authority_generation <= common->base_authority_generation
		|| common->authority_generation == UINT64_MAX
		|| common->resource_formation == 0 || common->resource_formation == UINT64_MAX
		|| common->master_session_incarnation == 0
		|| common->master_session_incarnation == UINT64_MAX
		|| common->assertion_sequence == 0 || common->assertion_sequence == UINT64_MAX
		|| common->sender_connection_generation == 0
		|| common->sender_connection_generation == UINT32_MAX) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	if (common->ordered_lane != 0 || common->source_candidate != 0
		|| common->retain_pi_if_dirty != 0 || common->action_node < 0
		|| common->action_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ROLE);
		return false;
	}
	if (common->observed_mode > (uint8)PCM_STATE_X
		|| (common->target_mode != (uint8)PCM_STATE_S && common->target_mode != (uint8)PCM_STATE_X)
		|| common->outcome != RESOURCE_X_OUTCOME_NONE) {
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
resource_x_pair_common_valid(uint8 msg_type, ResourceXWireKind kind,
							 bool acquire_intent, bool acquire_decision,
							 const ResourceXDecodedCommon *common,
							 ResourceXWireReject *reject)
{
	if (acquire_decision) {
		if (((kind == RESOURCE_X_WIRE_BLOCKED_TO_N && msg_type == RESOURCE_X_MSG_BLOCKED_TO_N)
			 || (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE && msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT))
			&& !acquire_intent) {
			ResourceXDecodedCommon decision = *common;
			if (common->outcome != RESOURCE_X_OUTCOME_OK) {
				resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_ENUM);
				return false;
			}
			decision.outcome = RESOURCE_X_OUTCOME_NONE;
			return resource_x_decision_common_valid(&decision, reject);
		}
		if (acquire_intent
			|| !((kind == RESOURCE_X_WIRE_AUTHORITY_GRANT && msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT)
				 || (kind == RESOURCE_X_WIRE_BLOCK_TO_N && msg_type == RESOURCE_X_MSG_BLOCK_TO_N))) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
			return false;
		}
		return resource_x_decision_common_valid(common, reject);
	}
	if (acquire_intent) {
		if (msg_type != RESOURCE_X_MSG_ASSERT_X || kind != RESOURCE_X_WIRE_ASSERT_X) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
			return false;
		}
		return resource_x_acquire_common_valid(common, reject);
	}
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
	bytes[0] = (len == RESOURCE_X_ACQUIRE_V3_BYTES || len == RESOURCE_X_DECISION_V3_BYTES
		|| len == RESOURCE_X_WITNESS_V3_BYTES || len == RESOURCE_X_SOURCE_WITNESS_V3_BYTES
		|| len == RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES)
		? RESOURCE_X_ACQUIRE_WIRE_VERSION
		: kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE ? RESOURCE_X_IMAGE_WIRE_VERSION
													  : RESOURCE_X_WIRE_VERSION;
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
resource_x_common_fields_decode(const uint8 *bytes, ResourceXDecodedCommon *common,
							   ResourceXWireReject *reject)
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
	return true;
}

static bool
resource_x_common_decode(uint8 msg_type, const uint8 *bytes, ResourceXWireKind kind,
						 bool acquire_intent, bool acquire_decision, ResourceXDecodedCommon *common,
						 ResourceXWireReject *reject)
{
	if (!resource_x_common_fields_decode(bytes, common, reject))
		return false;
	return resource_x_pair_common_valid(msg_type, kind, acquire_intent, acquire_decision, common, reject);
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
resource_x_acquire_native_valid(const ResourceXDecodedAcquireIntent *body, uint8 observed_mode)
{
	return body->requester_incarnation != 0 && body->requester_incarnation != UINT64_MAX
		&& body->backend_generation != 0 && body->backend_generation != UINT64_MAX
		&& body->base_generation != UINT64_MAX
		&& body->reservation_token != 0 && body->reservation_token != UINT64_MAX
		&& body->cluster_epoch != 0 && body->cluster_epoch != UINT64_MAX
		&& body->r4_generation != 0 && body->r4_generation != UINT64_MAX
		&& body->backend_id >= 0 && body->buffer_id >= 0
		&& (body->path == RESOURCE_X_ACQUIRE_RESIDENT
			|| (body->path == RESOURCE_X_ACQUIRE_FIRST_FILL && observed_mode == (uint8)PCM_STATE_N));
}

static bool
resource_x_decision_body_valid(const ResourceXDecodedFrame *frame)
{
	const ResourceXDecodedAcquireDecision *body = &frame->body.acquire_decision;
	uint32 requester_bit = UINT32_C(1) << frame->common.logical_assertion.requester_node;
	uint32 required;
	uint32 source_bit;

	if (frame->has_acquire_intent || frame->blocked_has_remote_proof
		|| !resource_x_acquire_native_valid(&body->episode, frame->common.observed_mode)
		|| body->resource_binding == 0 || body->resource_binding == UINT64_MAX
		|| body->enqueue_order == 0 || body->enqueue_order == UINT64_MAX
		|| body->master_node < 0 || body->master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| body->requester_ingress_generation == 0 || body->requester_ingress_generation == UINT32_MAX
		|| body->master_ingress_generation == 0 || body->master_ingress_generation == UINT32_MAX
		|| body->directory_mode > (uint8)PCM_STATE_X
		|| ((body->directory_mode == (uint8)PCM_STATE_N) != (body->holders_bitmap == 0))
		|| (body->directory_mode == (uint8)PCM_STATE_X
			&& (body->holders_bitmap & (body->holders_bitmap - 1)) != 0))
		return false;
	required = frame->common.target_mode == (uint8)PCM_STATE_X
		? body->holders_bitmap & ~requester_bit
		: body->directory_mode == (uint8)PCM_STATE_X ? body->holders_bitmap : 0;
	if (body->required_holders_bitmap != required)
		return false;
	for (int node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
		const ResourceXDecodedAcquireHolder *holder = &body->holders[node];

		if (body->holders_bitmap & (UINT32_C(1) << node)) {
			if (holder->boot_incarnation == 0 || holder->boot_incarnation == UINT64_MAX
				|| holder->grant_generation == 0
				|| holder->grant_generation > frame->common.base_authority_generation
				|| (node == frame->common.logical_assertion.requester_node
					&& holder->boot_incarnation != body->episode.requester_incarnation))
				return false;
		} else if (holder->boot_incarnation != 0 || holder->grant_generation != 0)
			return false;
	}
	if (body->source_kind == RESOURCE_X_ACQUIRE_SOURCE_DISK) {
		if (body->source_node != -1 || body->source_service_generation != 0
			|| body->directory_mode != (uint8)PCM_STATE_N || body->pi_holders_bitmap != 0)
			return false;
	} else {
		if (body->source_node < 0 || body->source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| body->source_service_generation == 0 || body->source_service_generation == UINT64_MAX)
			return false;
		source_bit = UINT32_C(1) << body->source_node;
		if (!(body->holders_bitmap & source_bit))
			return false;
		if (body->source_kind == RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT) {
			if (source_bit != requester_bit
				|| body->holders[body->source_node].boot_incarnation != body->episode.requester_incarnation)
				return false;
		} else if (body->source_kind != RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT
				   || source_bit == requester_bit)
			return false;
	}
	if (frame->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT)
		return frame->common.action_node == frame->common.logical_assertion.requester_node;
	if (frame->kind != RESOURCE_X_WIRE_BLOCK_TO_N || body->source_kind == RESOURCE_X_ACQUIRE_SOURCE_DISK)
		return false;
	return frame->common.action_node == body->source_node
		|| (required & (UINT32_C(1) << frame->common.action_node)) != 0;
}

static bool resource_x_body_valid(const ResourceXDecodedFrame *frame, ResourceXWireReject *reject);
static bool resource_x_body_encode(uint8 *bytes, const ResourceXDecodedFrame *frame,
								   ResourceXWireReject *reject);

static const ResourceXDecodedAcquireWitness *
resource_x_frame_witness(const ResourceXDecodedFrame *frame)
{
	return frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
		? &frame->body.acquire_image.source : &frame->body.acquire_witness;
}

static void
resource_x_witness_original_ack(const ResourceXDecodedFrame *frame, ResourceXDecodedFrame *ack)
{
	const ResourceXDecodedAcquireWitness *w = resource_x_frame_witness(frame);
	memset(ack, 0, sizeof(*ack));
	ack->kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	ack->payload_bytes = w->has_source_proof ? RESOURCE_X_PROOF_V1_BYTES : RESOURCE_X_CONTROL_V1_BYTES;
	ack->common = w->original_ack;
	ack->blocked_has_remote_proof = w->has_source_proof;
	if (w->has_source_proof)
		ack->body.blocked_to_n = w->original_proof;
}

static bool
resource_x_witness_body_valid(const ResourceXDecodedFrame *frame, ResourceXWireReject *reject)
{
	const ResourceXDecodedAcquireWitness *w = resource_x_frame_witness(frame);
	const ResourceXDecodedAcquireDecision *a = &w->decision;
	const ResourceXDecodedCommon *c = &frame->common;
	const ResourceXDecodedCommon *ack = &w->original_ack;
	const ResourceXDecodedBlockedToN *proof = &w->original_proof;
	ResourceXDecodedFrame component;
	uint32 services;
	bool source;

	if (frame->has_acquire_intent || frame->blocked_has_remote_proof)
		return false;
	/* Reuse the complete A validator on the original forward semantics.
	 * The enclosing ACK is a completion, not another master decision. */
	memset(&component, 0, sizeof(component));
	component.kind = RESOURCE_X_WIRE_BLOCK_TO_N;
	component.has_acquire_decision = true;
	component.common = *c;
	component.common.outcome = RESOURCE_X_OUTCOME_NONE;
	component.body.acquire_decision = *a;
	if (!resource_x_pair_common_valid(RESOURCE_X_MSG_BLOCK_TO_N, component.kind, false, true,
		&component.common, reject) || !resource_x_decision_body_valid(&component))
		return false;
	services = a->required_holders_bitmap;
	if (a->source_kind == RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT)
		services |= UINT32_C(1) << a->source_node;
	if ((services & (UINT32_C(1) << c->action_node)) == 0)
		return false;
	source = a->source_kind == RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT && c->action_node == a->source_node;
	if (w->has_source_proof != source
		|| !resource_x_assertion_equal(&ack->logical_assertion, &c->logical_assertion)
		|| ack->base_authority_generation != c->base_authority_generation
		|| ack->authority_generation != c->base_authority_generation
		|| ack->resource_formation != c->resource_formation
		|| ack->master_session_incarnation != c->master_session_incarnation
		|| ack->assertion_sequence != c->assertion_sequence || ack->ordered_lane != 0
		|| ack->action_node != c->action_node || ack->observed_mode != a->directory_mode
		|| ack->target_mode != (c->target_mode == (uint8)PCM_STATE_S ? (uint8)PCM_STATE_S : (uint8)PCM_STATE_N)
		|| ack->outcome != RESOURCE_X_OUTCOME_OK || ack->sender_connection_generation == UINT32_MAX)
		return false;
	resource_x_witness_original_ack(frame, &component);
	if (!resource_x_common_valid(component.kind, &component.common, reject)
		|| !resource_x_body_valid(&component, reject))
		return false;
	if (source) {
		/* Preserve the original native fence and its independent generation
		 * axis. Actual native lineage/quiescence is still proved by the source
		 * owner; neither this CRC nor A substitutes for that proof. */
		return resource_x_get_u32(proof->source_fence) == (uint32)c->action_node
			&& resource_x_get_u64(proof->source_fence + 4) == a->holders[c->action_node].boot_incarnation
			&& resource_x_get_u64(proof->source_fence + 20) != 0
			&& resource_x_get_u64(proof->source_fence + 20) != UINT64_MAX
			&& proof->source_fence[28] == ack->observed_mode
			&& proof->source_carrier_generation != UINT64_MAX
			&& proof->holder_connection_generation != UINT64_MAX;
	}
	/* Unused typed proof fields cannot smuggle an omitted source proof.
	 * Host struct padding is never a wire field or an identity predicate. */
	return resource_x_bytes_zero(proof->source_fence, sizeof(proof->source_fence))
		&& proof->source_carrier_generation == 0 && proof->requester_target_generation == 0
		&& proof->page_scn_lsn == 0 && proof->dependency_count == 0
		&& resource_x_dependencies_valid(proof->dependencies, 0)
		&& proof->source_proof_crc32c == 0 && proof->page_checksum == 0
		&& proof->source_disposition == 0 && proof->proof_kind == 0 && proof->proof_flags == 0
		&& proof->holder_connection_generation == 0 && proof->acting_formation == 0;
}

/* The enclosed image is a source artifact, not a standalone grant. S/X
 * target semantics are checked against complete A only by this envelope;
 * the legacy V2 entry point keeps its original X-only common validation. */
static bool
resource_x_image_component_encode(uint8 *bytes, const ResourceXDecodedAcquireImage *body,
								  ResourceXWireReject *reject)
{
	ResourceXDecodedFrame image;
	ResourceXDecodedCommon legacy_common = body->original_common;

	if (legacy_common.target_mode != PCM_STATE_S && legacy_common.target_mode != PCM_STATE_X)
		return false;
	legacy_common.target_mode = PCM_STATE_X;
	if (!resource_x_common_valid(RESOURCE_X_WIRE_IMAGE_ENVELOPE, &legacy_common, reject))
		return false;
	memset(&image, 0, sizeof(image));
	image.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	image.common = body->original_common;
	image.body.image_envelope = body->original_image;
	if (!resource_x_body_valid(&image, reject))
		return false;
	memset(bytes, 0, RESOURCE_X_IMAGE_V2_BYTES);
	resource_x_common_encode(bytes, image.kind, &image.common, RESOURCE_X_IMAGE_V2_BYTES);
	if (!resource_x_body_encode(bytes, &image, reject))
		return false;
	resource_x_put_u32(bytes + 4, resource_x_wire_crc(bytes, RESOURCE_X_IMAGE_V2_BYTES));
	return true;
}

static uint32
resource_x_data_crc(const uint8 *bytes, Size len)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
resource_x_acquire_image_valid(const ResourceXDecodedFrame *frame, ResourceXWireReject *reject)
{
	const ResourceXDecodedAcquireImage *body = &frame->body.acquire_image;
	const ResourceXDecodedAcquireWitness *w = &body->source;
	const ResourceXDecodedCommon *c = &body->original_common;
	const ResourceXDecodedCommon *a = &frame->common;
	const ResourceXDecodedBlockedToN *p = &w->original_proof;
	const ResourceXDecodedImageEnvelope *i = &body->original_image;
	uint8 encoded[RESOURCE_X_IMAGE_V2_BYTES];
	uint8 dependencies[RESOURCE_X_DEPENDENCY_VECTOR_BYTES];

	if (!w->has_source_proof || w->decision.source_kind != RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT
		|| a->action_node != w->decision.source_node || !resource_x_witness_body_valid(frame, reject)
		|| !resource_x_assertion_equal(&c->logical_assertion, &a->logical_assertion)
		|| c->base_authority_generation != a->base_authority_generation
		|| c->authority_generation != a->authority_generation
		|| c->resource_formation != a->resource_formation
		|| c->master_session_incarnation != a->master_session_incarnation
		|| c->assertion_sequence != a->assertion_sequence || c->ordered_lane != 0
		|| c->action_node != a->logical_assertion.requester_node
		|| c->observed_mode != w->decision.directory_mode || c->target_mode != a->target_mode
		|| c->sender_connection_generation == UINT32_MAX || c->outcome != RESOURCE_X_OUTCOME_OK
		|| resource_x_get_u64(i->request_tail) != c->sender_connection_generation
		|| resource_x_get_u64(i->request_tail + 8) != c->assertion_sequence
		|| !resource_x_bytes_zero(i->request_tail + 16, 4)
		|| resource_x_get_u64(i->source_fence + 12) != c->sender_connection_generation
		|| memcmp(i->source_fence, p->source_fence, sizeof(i->source_fence)) != 0
		|| i->source_carrier_generation != p->source_carrier_generation
		|| i->requester_target_generation != p->requester_target_generation
		|| i->page_scn_lsn != p->page_scn_lsn || i->page_checksum != p->page_checksum
		|| i->dependency_count != p->dependency_count
		|| memcmp(i->dependencies, p->dependencies, sizeof(i->dependencies)) != 0
		|| i->source_disposition != p->source_disposition || i->proof_kind != p->proof_kind
		|| i->page_checksum != resource_x_data_crc(i->page_bytes, sizeof(i->page_bytes)))
		return false;
	for (int n = 0; n < RESOURCE_X_DEPENDENCY_MAX; n++)
		resource_x_put_u64(dependencies + n * 8, i->dependencies[n]);
	return i->dependency_vector_crc32c == resource_x_data_crc(dependencies, sizeof(dependencies))
		&& resource_x_image_component_encode(encoded, body, reject)
		&& p->source_proof_crc32c == resource_x_get_u32(encoded + 4);
}

static bool
resource_x_body_valid(const ResourceXDecodedFrame *frame, ResourceXWireReject *reject)
{
	bool valid = true;
	if (frame->has_acquire_decision) {
		valid = frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			? resource_x_acquire_image_valid(frame, reject)
			: frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
				? resource_x_witness_body_valid(frame, reject) : resource_x_decision_body_valid(frame);
		if (!valid)
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BODY);
		return valid;
	}
	if (frame->has_acquire_intent) {
		const ResourceXDecodedAcquireIntent *body = &frame->body.acquire_intent;

		valid = frame->kind == RESOURCE_X_WIRE_ASSERT_X && !frame->blocked_has_remote_proof
			&& resource_x_acquire_native_valid(body, frame->common.observed_mode);
		if (!valid)
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BODY);
		return valid;
	}

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
				&& body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
				&& resource_x_image_wal_valid(frame)
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


static bool
resource_x_body_encode(uint8 *bytes, const ResourceXDecodedFrame *frame, ResourceXWireReject *reject)
{
	if (frame->has_acquire_intent || frame->has_acquire_decision) {
		bool image = frame->has_acquire_decision && frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		bool witness = frame->has_acquire_decision && (image || frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N);
		const ResourceXDecodedAcquireDecision *decision = witness
			? &resource_x_frame_witness(frame)->decision : &frame->body.acquire_decision;
		const ResourceXDecodedAcquireIntent *body = frame->has_acquire_decision
			? &decision->episode : &frame->body.acquire_intent;

		resource_x_put_u64(bytes + 96, body->requester_incarnation);
		resource_x_put_u64(bytes + 104, body->backend_generation);
		resource_x_put_u64(bytes + 112, body->base_generation);
		resource_x_put_u64(bytes + 120, body->reservation_token);
		resource_x_put_u64(bytes + 128, body->cluster_epoch);
		resource_x_put_u64(bytes + 136, body->r4_generation);
		resource_x_put_u32(bytes + 144, (uint32)body->backend_id);
		resource_x_put_u32(bytes + 148, (uint32)body->buffer_id);
		bytes[152] = body->path;
		if (frame->has_acquire_decision) {
			resource_x_put_u64(bytes + 160, decision->resource_binding);
			resource_x_put_u64(bytes + 168, decision->enqueue_order);
			resource_x_put_u64(bytes + 176, decision->source_service_generation);
			resource_x_put_u32(bytes + 184, (uint32)decision->master_node);
			resource_x_put_u32(bytes + 188, (uint32)decision->source_node);
			resource_x_put_u32(bytes + 192, decision->holders_bitmap);
			resource_x_put_u32(bytes + 196, decision->required_holders_bitmap);
			resource_x_put_u32(bytes + 200, decision->pi_holders_bitmap);
			resource_x_put_u32(bytes + 204, decision->requester_ingress_generation);
			resource_x_put_u32(bytes + 208, decision->master_ingress_generation);
			bytes[212] = decision->directory_mode;
			bytes[213] = decision->source_kind;
			for (int node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
				resource_x_put_u64(bytes + 224 + node * 16, decision->holders[node].boot_incarnation);
				resource_x_put_u64(bytes + 232 + node * 16, decision->holders[node].grant_generation);
			}
		}
		if (witness) {
			ResourceXDecodedFrame ack;
			uint16 len = 0;
			uint16 expected = resource_x_frame_witness(frame)->has_source_proof
				? RESOURCE_X_PROOF_V1_BYTES : RESOURCE_X_CONTROL_V1_BYTES;

			resource_x_witness_original_ack(frame, &ack);
			/* This bounded inner call accepts only the original V1 ACK, with
			 * its own full key and CRC. It cannot encode another witness. */
			if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCKED_TO_N, &ack,
				bytes + RESOURCE_X_DECISION_V3_BYTES, expected, &len, reject) || len != expected)
				return false;
		}
		if (image && !resource_x_image_component_encode(bytes + RESOURCE_X_SOURCE_WITNESS_V3_BYTES,
				&frame->body.acquire_image, reject))
			return false;
		return true;
	}
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
		resource_x_page_wal_encode(bytes + 8520, &body->page_wal);
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
	return true;
}


static bool
resource_x_body_decode(const uint8 *bytes, ResourceXDecodedFrame *frame,
					   ResourceXWireReject *reject)
{
	if (frame->has_acquire_intent || frame->has_acquire_decision) {
		bool image = frame->has_acquire_decision && frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE;
		bool witness = frame->has_acquire_decision && (image || frame->kind == RESOURCE_X_WIRE_BLOCKED_TO_N);
		ResourceXDecodedAcquireWitness *w = image
			? &frame->body.acquire_image.source : &frame->body.acquire_witness;
		ResourceXDecodedAcquireDecision *decision = witness
			? &w->decision : &frame->body.acquire_decision;
		ResourceXDecodedAcquireIntent *body = frame->has_acquire_decision
			? &decision->episode : &frame->body.acquire_intent;

		if (!resource_x_bytes_zero(bytes + 153, 7)
			|| (frame->has_acquire_decision && !resource_x_bytes_zero(bytes + 214, 10))) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
			return false;
		}
		body->requester_incarnation = resource_x_get_u64(bytes + 96);
		body->backend_generation = resource_x_get_u64(bytes + 104);
		body->base_generation = resource_x_get_u64(bytes + 112);
		body->reservation_token = resource_x_get_u64(bytes + 120);
		body->cluster_epoch = resource_x_get_u64(bytes + 128);
		body->r4_generation = resource_x_get_u64(bytes + 136);
		body->backend_id = (int32)resource_x_get_u32(bytes + 144);
		body->buffer_id = (int32)resource_x_get_u32(bytes + 148);
		body->path = bytes[152];
		if (frame->has_acquire_decision) {
			decision->resource_binding = resource_x_get_u64(bytes + 160);
			decision->enqueue_order = resource_x_get_u64(bytes + 168);
			decision->source_service_generation = resource_x_get_u64(bytes + 176);
			decision->master_node = (int32)resource_x_get_u32(bytes + 184);
			decision->source_node = (int32)resource_x_get_u32(bytes + 188);
			decision->holders_bitmap = resource_x_get_u32(bytes + 192);
			decision->required_holders_bitmap = resource_x_get_u32(bytes + 196);
			decision->pi_holders_bitmap = resource_x_get_u32(bytes + 200);
			decision->requester_ingress_generation = resource_x_get_u32(bytes + 204);
			decision->master_ingress_generation = resource_x_get_u32(bytes + 208);
			decision->directory_mode = bytes[212];
			decision->source_kind = bytes[213];
			for (int node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
				decision->holders[node].boot_incarnation = resource_x_get_u64(bytes + 224 + node * 16);
				decision->holders[node].grant_generation = resource_x_get_u64(bytes + 232 + node * 16);
			}
		}
		if (witness) {
			ResourceXDecodedFrame ack;
			const uint8 *original = bytes + RESOURCE_X_DECISION_V3_BYTES;
			uint16 len = image ? RESOURCE_X_PROOF_V1_BYTES : frame->payload_bytes - RESOURCE_X_DECISION_V3_BYTES;

			/* Reject nested V3 envelopes before the bounded original ACK
			 * decoder, rather than treating a copied A as native completion. */
			if (original[0] != RESOURCE_X_WIRE_VERSION
				|| original[1] != RESOURCE_X_WIRE_BLOCKED_TO_N
				|| resource_x_get_u16(original + 2) != len) {
				resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BODY);
				return false;
			}
			if (!cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCKED_TO_N, original, len, &ack, reject))
				return false;
			w->original_ack = ack.common;
			w->has_source_proof = ack.blocked_has_remote_proof;
			if (ack.blocked_has_remote_proof)
				w->original_proof = ack.body.blocked_to_n;
		}
		if (image) {
			ResourceXDecodedFrame original;
			const uint8 *component = bytes + RESOURCE_X_SOURCE_WITNESS_V3_BYTES;

			if (component[0] != RESOURCE_X_IMAGE_WIRE_VERSION
				|| component[1] != RESOURCE_X_WIRE_IMAGE_ENVELOPE
				|| resource_x_get_u16(component + 2) != RESOURCE_X_IMAGE_V2_BYTES
				|| resource_x_get_u32(component + 4) != resource_x_wire_crc(component, RESOURCE_X_IMAGE_V2_BYTES)) {
				resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_BODY);
				return false;
			}
			memset(&original, 0, sizeof(original));
			original.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
			if (!resource_x_common_fields_decode(component, &original.common, reject)
				|| !resource_x_body_decode(component, &original, reject))
				return false;
			original.common.semantic_crc32c = resource_x_get_u32(component + 4);
			frame->body.acquire_image.original_common = original.common;
			frame->body.acquire_image.original_image = original.body.image_envelope;
		}
		return resource_x_body_valid(frame, reject);
	}
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
		if (!resource_x_bytes_zero(bytes + 8520 + 132, 4)) {
			resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_RESERVED);
			return false;
		}
		resource_x_page_wal_decode(bytes + 8520, &body->page_wal);
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
	uint8 encoded[RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES];
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
	if (!resource_x_pair_common_valid(msg_type, frame->kind, frame->has_acquire_intent,
									frame->has_acquire_decision, &frame->common, reject))
		return false;
	if (!resource_x_body_valid(frame, reject))
		return false;

	memset(encoded, 0, len);
	resource_x_common_encode(encoded, frame->kind, &frame->common, len);
	if (!resource_x_body_encode(encoded, frame, reject))
		return false;
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
		&& payload_len != RESOURCE_X_PROOF_V1_BYTES && payload_len != RESOURCE_X_IMAGE_V2_BYTES
		&& payload_len != RESOURCE_X_ACQUIRE_V3_BYTES && payload_len != RESOURCE_X_DECISION_V3_BYTES
		&& payload_len != RESOURCE_X_WITNESS_V3_BYTES && payload_len != RESOURCE_X_SOURCE_WITNESS_V3_BYTES
		&& payload_len != RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
		return false;
	}
	kind = (ResourceXWireKind)bytes[1];
	if (!resource_x_pair_length(msg_type, kind, payload_len)) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
		return false;
	}
	if (bytes[0]
		!= ((payload_len == RESOURCE_X_ACQUIRE_V3_BYTES || payload_len == RESOURCE_X_DECISION_V3_BYTES
			 || payload_len == RESOURCE_X_WITNESS_V3_BYTES || payload_len == RESOURCE_X_SOURCE_WITNESS_V3_BYTES
			 || payload_len == RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES)
			? RESOURCE_X_ACQUIRE_WIRE_VERSION
			: kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE ? RESOURCE_X_IMAGE_WIRE_VERSION
												   : RESOURCE_X_WIRE_VERSION)) {
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
	decoded.has_acquire_intent = payload_len == RESOURCE_X_ACQUIRE_V3_BYTES;
	decoded.has_acquire_decision = payload_len == RESOURCE_X_DECISION_V3_BYTES
		|| payload_len == RESOURCE_X_WITNESS_V3_BYTES || payload_len == RESOURCE_X_SOURCE_WITNESS_V3_BYTES
		|| payload_len == RESOURCE_X_ACQUIRE_IMAGE_V3_BYTES;
	decoded.blocked_has_remote_proof
		= (kind == RESOURCE_X_WIRE_BLOCKED_TO_N || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		   || kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2)
		  && payload_len == RESOURCE_X_PROOF_V1_BYTES;
	if (!resource_x_common_decode(msg_type, bytes, kind, decoded.has_acquire_intent,
								 decoded.has_acquire_decision, &decoded.common, reject))
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
	if ((decoded.has_acquire_intent || decoded.has_acquire_decision)
		&& sender_connection_generation == UINT32_MAX) {
		resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_GENERATION);
		return false;
	}
	resource_x_put_u32(bytes + 76, sender_connection_generation);
	resource_x_put_u32(bytes + 4, 0);
	crc = resource_x_wire_crc(bytes, payload_len);
	resource_x_put_u32(bytes + 4, crc);
	resource_x_wire_reject(reject, RESOURCE_X_WIRE_REJECT_NONE);
	return true;
}
