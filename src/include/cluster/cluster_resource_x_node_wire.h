/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_node_wire.h
 *    Resource-X final node-wire byte layouts -- spec-8.10 D10-01.
 *
 * Every transmitted integer is an explicitly sized byte array.  The codec
 * owns network-byte-order conversion; no native C integer or implicit
 * padding is part of this ABI.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RESOURCE_X_NODE_WIRE_H
#define CLUSTER_RESOURCE_X_NODE_WIRE_H

#include "cluster/cluster_buffer_desc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_resource_x_identity.h"

#define RESOURCE_X_WIRE_VERSION 1

#define RESOURCE_X_CONTROL_V1_BYTES 96
#define RESOURCE_X_SHORT_V1_BYTES 152
#define RESOURCE_X_PROOF_V1_BYTES 312
#define RESOURCE_X_IMAGE_V1_BYTES 8520
#define RESOURCE_X_PAGE_BYTES 8192
#define RESOURCE_X_DEPENDENCY_MAX 16
#define RESOURCE_X_DEPENDENCY_VECTOR_BYTES 128
#define RESOURCE_X_SOURCE_FENCE_BYTES 34
#define RESOURCE_X_REQUEST_TAIL_BYTES 20

#define RESOURCE_X_MSG_ASSERT_X PGRAC_IC_MSG_GCS_BLOCK_REQUEST
#define RESOURCE_X_MSG_IMAGE_OR_GRANT PGRAC_IC_MSG_GCS_BLOCK_REPLY
#define RESOURCE_X_MSG_BLOCK_TO_N PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE
#define RESOURCE_X_MSG_BLOCKED_TO_N PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK
#define RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE PGRAC_IC_MSG_GCS_BLOCK_DONE

typedef enum ResourceXWireKind
{
	RESOURCE_X_WIRE_ASSERT_X = 1,
	RESOURCE_X_WIRE_BLOCK_TO_N = 2,
	RESOURCE_X_WIRE_BLOCKED_TO_N = 3,
	RESOURCE_X_WIRE_RELEASE_X = 4,
	RESOURCE_X_WIRE_IMAGE_ENVELOPE = 5,
	RESOURCE_X_WIRE_AUTHORITY_GRANT = 6,
	RESOURCE_X_WIRE_INSTALL_SETTLEMENT = 7,
	RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION = 8,
	RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP = 9,
	RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2 = 10,
	RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2 = 11
} ResourceXWireKind;

#define RESOURCE_X_WIRE_KIND_MIN RESOURCE_X_WIRE_ASSERT_X
#define RESOURCE_X_WIRE_KIND_MAX RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2

typedef enum ResourceXSourceProofKind
{
	RESOURCE_X_PROOF_REMOTE_CARRIER = 1,
	RESOURCE_X_PROOF_LOCAL_IMAGE = 2,
	RESOURCE_X_PROOF_DURABLE_STORAGE = 3
} ResourceXSourceProofKind;

#define RESOURCE_X_PROOF_KIND_MIN RESOURCE_X_PROOF_REMOTE_CARRIER
#define RESOURCE_X_PROOF_KIND_MAX RESOURCE_X_PROOF_DURABLE_STORAGE

#define RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED UINT8_C(0x01)
#define RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED UINT8_C(0x02)
/* An exact master-directed source transfers X permission with the image.
 * Valid only on a selected-source BLOCK_TO_N and its IMAGE_ENVELOPE. */
#define RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE UINT8_C(0x04)
/* Only a zero-base type14/kind9 request may permit remote-source admission. */
#define RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION UINT8_C(0x08)

typedef enum ResourceXWireOutcome
{
	RESOURCE_X_OUTCOME_NONE = 0,
	RESOURCE_X_OUTCOME_OK = 1,
	RESOURCE_X_OUTCOME_DUPLICATE = 2,
	RESOURCE_X_OUTCOME_STALE = 3,
	RESOURCE_X_OUTCOME_RETRY = 4,
	RESOURCE_X_OUTCOME_QUEUE_FULL = 5,
	RESOURCE_X_OUTCOME_CANCELLED = 6,
	RESOURCE_X_OUTCOME_ROLL_FORWARD = 7,
	RESOURCE_X_OUTCOME_REPLAY_REQUIRED = 8,
	RESOURCE_X_OUTCOME_RECOVERY_BLOCKED = 9,
	RESOURCE_X_OUTCOME_GENERATION_EXHAUSTED = 10,
	RESOURCE_X_OUTCOME_CORRUPT = 11
} ResourceXWireOutcome;

typedef enum ResourceXSourceDisposition
{
	RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE = 1,
	RESOURCE_X_DISPOSITION_LOCAL_IMAGE = 2,
	RESOURCE_X_DISPOSITION_DURABLE_STORAGE = 3
} ResourceXSourceDisposition;

typedef enum ResourceXRequesterRole
{
	RESOURCE_X_REQUESTER_ROLE_ACQUIRER = 1
} ResourceXRequesterRole;

typedef enum ResourceXSettlementTerminalState
{
	RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED = 1,
	RESOURCE_X_SETTLEMENT_TERMINAL_EXCLUDED = 2
} ResourceXSettlementTerminalState;

typedef enum ResourceXWireReject
{
	RESOURCE_X_WIRE_REJECT_NONE = 0,
	RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT,
	RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH,
	RESOURCE_X_WIRE_REJECT_TYPE_KIND,
	RESOURCE_X_WIRE_REJECT_VERSION,
	RESOURCE_X_WIRE_REJECT_DECLARED_LENGTH,
	RESOURCE_X_WIRE_REJECT_CRC,
	RESOURCE_X_WIRE_REJECT_RESERVED,
	RESOURCE_X_WIRE_REJECT_IDENTITY,
	RESOURCE_X_WIRE_REJECT_GENERATION,
	RESOURCE_X_WIRE_REJECT_ENUM,
	RESOURCE_X_WIRE_REJECT_ROLE,
	RESOURCE_X_WIRE_REJECT_FLAGS,
	RESOURCE_X_WIRE_REJECT_BODY
} ResourceXWireReject;

typedef struct ResourceXWireCommonV1
{
	uint8		wire_version;
	uint8		kind;
	uint8		header_bytes[2];
	uint8		semantic_crc32c[4];
	uint8		logical_assertion[24];
	uint8		base_authority_generation[8];
	uint8		resource_formation[8];
	uint8		master_session_incarnation[8];
	uint8		assertion_sequence[8];
	uint8		ordered_lane[4];
	uint8		action_node[4];
	uint8		observed_mode;
	uint8		target_mode;
	uint8		source_candidate;
	uint8		retain_pi_if_dirty;
	uint8		sender_connection_generation[4];
	uint8		outcome;
	uint8		flags;
	uint8		zero_reserved[6];
	uint8		authority_generation[8];
} ResourceXWireCommonV1;

typedef ResourceXWireCommonV1 ResourceXControlV1;

typedef struct ResourceXLocalProofDeclarationV1
{
	ResourceXWireCommonV1 common;
	uint8		local_holder_authority_generation[8];
	uint8		requester_target_generation[8];
	uint8		page_scn_lsn[8];
	uint8		dependency_count[2];
	uint8		zero_reserved[2];
	uint8		dependency_vector_crc32c[4];
	uint8		page_checksum[4];
	uint8		local_image_proof_crc32c[4];
	uint8		requester_connection_generation[8];
	uint8		local_proof_generation[8];
} ResourceXLocalProofDeclarationV1;

typedef struct ResourceXBlockedToNProofV1
{
	ResourceXWireCommonV1 common;
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint8		source_carrier_generation[8];
	uint8		requester_target_generation[8];
	uint8		page_scn_lsn[8];
	uint8		dependency_count[2];
	uint8		dependencies[RESOURCE_X_DEPENDENCY_VECTOR_BYTES];
	uint8		source_proof_crc32c[4];
	uint8		page_checksum[4];
	uint8		source_disposition;
	uint8		proof_kind;
	uint8		proof_flags[2];
	uint8		holder_connection_generation[8];
	uint8		acting_formation[8];
} ResourceXBlockedToNProofV1;

typedef struct ResourceXAuthorityGrantV1
{
	ResourceXWireCommonV1 common;
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint8		final_authority_generation[8];
	uint8		source_carrier_generation[8];
	uint8		requester_target_generation[8];
	uint8		page_scn_lsn[8];
	uint8		dependency_count[2];
	uint8		dependencies[RESOURCE_X_DEPENDENCY_VECTOR_BYTES];
	uint8		source_proof_crc32c[4];
	uint8		page_checksum[4];
	uint8		proof_kind;
	uint8		source_disposition;
	uint8		grant_flags[2];
	uint8		requester_connection_generation[8];
} ResourceXAuthorityGrantV1;

typedef struct ResourceXImageEnvelopeV1
{
	ResourceXWireCommonV1 common;
	uint8		request_tail[RESOURCE_X_REQUEST_TAIL_BYTES];
	uint8		conversion_base_generation[8];
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint8		source_carrier_generation[8];
	uint8		requester_target_generation[8];
	uint8		page_scn_lsn[8];
	uint8		dependency_count[2];
	uint8		dependencies[RESOURCE_X_DEPENDENCY_VECTOR_BYTES];
	uint8		dependency_vector_crc32c[4];
	uint8		page_checksum[4];
	uint8		image_length[4];
	uint8		source_disposition;
	uint8		proof_kind;
	uint8		image_flags[2];
	uint8		page_bytes[RESOURCE_X_PAGE_BYTES];
} ResourceXImageEnvelopeV1;

typedef struct ResourceXInstallSettlementV1
{
	ResourceXWireCommonV1 common;
	uint8		conversion_base_generation[8];
	uint8		final_authority_generation[8];
	uint8		requester_connection_generation[8];
	uint8		requester_target_generation[8];
	uint8		page_scn_lsn[8];
	uint8		page_checksum[4];
	uint8		source_proof_crc32c[4];
	uint8		installed_mode;
	uint8		requester_role;
	uint8		terminal_outcome;
	uint8		terminal_state;
	uint8		settlement_flags[4];
} ResourceXInstallSettlementV1;

/* Host-order values.  These are never copied directly to the wire. */
typedef struct ResourceXDecodedCommon
{
	ResourceXAssertion logical_assertion;
	uint64		base_authority_generation;
	uint64		resource_formation;
	uint64		master_session_incarnation;
	uint64		assertion_sequence;
	uint32		ordered_lane;
	int32		action_node;
	uint8		observed_mode;
	uint8		target_mode;
	uint8		source_candidate;
	uint8		retain_pi_if_dirty;
	uint32		sender_connection_generation;
	uint8		outcome;
	uint8		flags;
	uint64		authority_generation;
	/* Decoder-verified CRC from wire bytes.  The encoder computes this field;
	 * callers do not provide it as input. */
	uint32		semantic_crc32c;
} ResourceXDecodedCommon;

typedef struct ResourceXDecodedLocalProof
{
	uint64		local_holder_authority_generation;
	uint64		requester_target_generation;
	uint64		page_scn_lsn;
	uint16		dependency_count;
	uint32		dependency_vector_crc32c;
	uint32		page_checksum;
	uint32		local_image_proof_crc32c;
	uint64		requester_connection_generation;
	uint64		local_proof_generation;
} ResourceXDecodedLocalProof;

typedef struct ResourceXDecodedBlockedToN
{
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint64		source_carrier_generation;
	uint64		requester_target_generation;
	uint64		page_scn_lsn;
	uint16		dependency_count;
	uint64		dependencies[RESOURCE_X_DEPENDENCY_MAX];
	uint32		source_proof_crc32c;
	uint32		page_checksum;
	uint8		source_disposition;
	uint8		proof_kind;
	uint16		proof_flags;
	uint64		holder_connection_generation;
	uint64		acting_formation;
} ResourceXDecodedBlockedToN;

typedef struct ResourceXDecodedAuthorityGrant
{
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint64		final_authority_generation;
	uint64		source_carrier_generation;
	uint64		requester_target_generation;
	uint64		page_scn_lsn;
	uint16		dependency_count;
	uint64		dependencies[RESOURCE_X_DEPENDENCY_MAX];
	uint32		source_proof_crc32c;
	uint32		page_checksum;
	uint8		proof_kind;
	uint8		source_disposition;
	uint16		grant_flags;
	uint64		requester_connection_generation;
} ResourceXDecodedAuthorityGrant;

typedef struct ResourceXDecodedImageEnvelope
{
	uint8		request_tail[RESOURCE_X_REQUEST_TAIL_BYTES];
	uint64		conversion_base_generation;
	uint8		source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	uint64		source_carrier_generation;
	uint64		requester_target_generation;
	uint64		page_scn_lsn;
	uint16		dependency_count;
	uint64		dependencies[RESOURCE_X_DEPENDENCY_MAX];
	uint32		dependency_vector_crc32c;
	uint32		page_checksum;
	uint32		image_length;
	uint8		source_disposition;
	uint8		proof_kind;
	uint16		image_flags;
	uint8		page_bytes[RESOURCE_X_PAGE_BYTES];
} ResourceXDecodedImageEnvelope;

typedef struct ResourceXDecodedInstallSettlement
{
	uint64		conversion_base_generation;
	uint64		final_authority_generation;
	uint64		requester_connection_generation;
	uint64		requester_target_generation;
	uint64		page_scn_lsn;
	uint32		page_checksum;
	uint32		source_proof_crc32c;
	uint8		installed_mode;
	uint8		requester_role;
	uint8		terminal_outcome;
	uint8		terminal_state;
	uint32		settlement_flags;
} ResourceXDecodedInstallSettlement;

typedef struct ResourceXDecodedFrame
{
	ResourceXWireKind kind;
	uint16		payload_bytes;
	bool		blocked_has_remote_proof;
	ResourceXDecodedCommon common;
	union
	{
		ResourceXDecodedLocalProof local_proof;
		ResourceXDecodedBlockedToN blocked_to_n;
		ResourceXDecodedAuthorityGrant authority_grant;
		ResourceXDecodedImageEnvelope image_envelope;
		ResourceXDecodedInstallSettlement install_settlement;
	} body;
} ResourceXDecodedFrame;

StaticAssertDecl(BLCKSZ == RESOURCE_X_PAGE_BYTES,
				 "Resource-X V1 requires the current 8192-byte page ABI");
StaticAssertDecl(sizeof(ResourceXWireCommonV1) == RESOURCE_X_CONTROL_V1_BYTES,
				 "Resource-X common V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXWireCommonV1, logical_assertion) == 8
					 && offsetof(ResourceXWireCommonV1, authority_generation) == 88,
				 "Resource-X common V1 offsets changed");
StaticAssertDecl(sizeof(ResourceXLocalProofDeclarationV1) == RESOURCE_X_SHORT_V1_BYTES,
				 "Resource-X local proof V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXLocalProofDeclarationV1,
						   local_holder_authority_generation) == 96
					 && offsetof(ResourceXLocalProofDeclarationV1,
								 local_proof_generation) == 144,
				 "Resource-X local proof V1 offsets changed");
StaticAssertDecl(sizeof(ResourceXBlockedToNProofV1) == RESOURCE_X_PROOF_V1_BYTES,
				 "Resource-X blocked-to-N V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXBlockedToNProofV1, source_fence) == 96
					 && offsetof(ResourceXBlockedToNProofV1, acting_formation) == 304,
				 "Resource-X blocked-to-N V1 offsets changed");
StaticAssertDecl(sizeof(ResourceXAuthorityGrantV1) == RESOURCE_X_PROOF_V1_BYTES,
				 "Resource-X authority-grant V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXAuthorityGrantV1, source_fence) == 96
					 && offsetof(ResourceXAuthorityGrantV1,
								 requester_connection_generation) == 304,
				 "Resource-X authority-grant V1 offsets changed");
StaticAssertDecl(sizeof(ResourceXImageEnvelopeV1) == RESOURCE_X_IMAGE_V1_BYTES,
				 "Resource-X image-envelope V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXImageEnvelopeV1, request_tail) == 96
					 && offsetof(ResourceXImageEnvelopeV1, page_bytes) == 328,
				 "Resource-X image-envelope V1 offsets changed");
StaticAssertDecl(sizeof(ResourceXInstallSettlementV1) == RESOURCE_X_SHORT_V1_BYTES,
				 "Resource-X install-settlement V1 wire size changed");
StaticAssertDecl(offsetof(ResourceXInstallSettlementV1,
						   conversion_base_generation) == 96
					 && offsetof(ResourceXInstallSettlementV1, settlement_flags) == 148,
				 "Resource-X install-settlement V1 offsets changed");

extern bool cluster_resource_x_wire_encode(uint8 msg_type,
	const ResourceXDecodedFrame *frame, void *payload, uint16 payload_capacity,
	uint16 *payload_len_out, ResourceXWireReject *reject);
extern bool cluster_resource_x_wire_decode(uint8 msg_type, const void *payload,
	uint16 payload_len, ResourceXDecodedFrame *out,
	ResourceXWireReject *reject);
extern bool cluster_resource_x_wire_rebind_sender_generation(uint8 msg_type,
	void *payload, uint16 payload_len, uint32 sender_connection_generation,
	ResourceXWireReject *reject);

#endif /* CLUSTER_RESOURCE_X_NODE_WIRE_H */
