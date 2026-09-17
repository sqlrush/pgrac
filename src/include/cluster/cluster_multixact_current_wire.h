/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_wire.h
 *	  Strict descriptor wire ABI for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_multixact_current_wire.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_CURRENT_WIRE_H
#define CLUSTER_MULTIXACT_CURRENT_WIRE_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_multixact_current.h"

#define CLUSTER_CURRENT_MX_WIRE_MAGIC ((uint32)0x5047434d)
#define CLUSTER_CURRENT_MX_WIRE_VERSION 5
#define CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE 0
#define CLUSTER_CURRENT_MX_DESCRIBE_FORWARD_SIZE 128
#define CLUSTER_CURRENT_MX_PROOF_FORWARD_SIZE 128
#define CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME 7

typedef enum ClusterCurrentMxProofBodyKind {
	CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS = 1,
	CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE = 2
} ClusterCurrentMxProofBodyKind;

typedef struct ClusterCurrentMxDescribePrefixWire {
	uint64 request_id;
	uint64 epoch;
	ClusterCurrentMxKey mxkey;
	uint8 reserved_a[4];
	int32 original_requester_node;
	int32 requester_backend_id;
	uint8 reserved_b[19];
	uint8 kind;
} ClusterCurrentMxDescribePrefixWire;

typedef struct ClusterCurrentMxDescribeTrailerWire {
	uint32 magic;
	uint16 version;
	uint16 flags;
	uint8 reserved[56];
} ClusterCurrentMxDescribeTrailerWire;

typedef struct ClusterCurrentMxDescribeForwardV2 {
	ClusterCurrentMxDescribePrefixWire prefix;
	ClusterCurrentMxDescribeTrailerWire trailer;
} ClusterCurrentMxDescribeForwardV2;

typedef struct ClusterCurrentMxProofAskWire {
	TransactionId xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 reserved8;
} ClusterCurrentMxProofAskWire;

typedef struct ClusterCurrentMxUpdaterChallengeWire {
	ClusterCurrentMxSuccessorAlias candidate_next_xmin_alias;
	ClusterTxLocator candidate_next_xmin_locator;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 reserved8;
} ClusterCurrentMxUpdaterChallengeWire;

typedef struct ClusterCurrentMxUpdaterChallengeBodyWire {
	ClusterCurrentMxUpdaterChallengeWire challenge;
} ClusterCurrentMxUpdaterChallengeBodyWire;

typedef union ClusterCurrentMxProofRequestBodyWire {
	ClusterCurrentMxProofAskWire asks[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentMxUpdaterChallengeBodyWire updater;
	uint8 raw[56];
} ClusterCurrentMxProofRequestBodyWire;

typedef struct ClusterCurrentMxProofPrefixWire {
	uint64 request_id;
	uint64 epoch;
	ClusterCurrentMxKey mxkey;
	uint8 reserved_a[4];
	int32 original_requester_node;
	int32 requester_backend_id;
	uint32 total_count;
	uint8 chunk_ordinal;
	uint8 descriptor_hash_bytes[8];
	uint8 chunk_count_minus_one;
	uint8 entry_count;
	uint8 body_kind;
	uint8 flags;
	uint8 reserved_b[2];
	uint8 kind;
} ClusterCurrentMxProofPrefixWire;

typedef struct ClusterCurrentMxProofTrailerWire {
	uint32 magic;
	uint16 version;
	uint16 reserved16;
	ClusterCurrentMxProofRequestBodyWire body;
} ClusterCurrentMxProofTrailerWire;

typedef struct ClusterCurrentMxProofForwardV2 {
	ClusterCurrentMxProofPrefixWire prefix;
	ClusterCurrentMxProofTrailerWire trailer;
} ClusterCurrentMxProofForwardV2;

typedef struct ClusterCurrentMxProofRequestPlan {
	uint16 destination_node_id;
	uint16 reserved16;
	ClusterCurrentMxProofForwardV2 request;
} ClusterCurrentMxProofRequestPlan;

typedef struct ClusterCurrentMxDescribeReplyHeader {
	uint32 magic;
	uint16 version;
	uint8 kind;
	uint8 result;
	uint32 flags;
	uint32 source_node_id;
	uint64 request_id;
	ClusterCurrentMxKey mxkey;
	uint64 descriptor_hash;
	uint32 total_count;
	uint16 entry_count;
	uint8 chunk_ordinal;
	uint8 chunk_count_minus_one;
	uint16 wire_length;
	uint16 reserved16;
	uint32 reserved32;
} ClusterCurrentMxDescribeReplyHeader;

#define CLUSTER_CURRENT_MX_DESCRIBE_REPLY_RESERVED_SIZE                                            \
	(BLCKSZ - sizeof(ClusterCurrentMxDescribeReplyHeader)                                          \
	 - CLUSTER_CURRENT_MX_MAX_MEMBERS * sizeof(ClusterCurrentMxMemberDesc))

typedef struct ClusterCurrentMxDescribeReplyPage {
	ClusterCurrentMxDescribeReplyHeader header;
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint8 reserved[CLUSTER_CURRENT_MX_DESCRIBE_REPLY_RESERVED_SIZE];
} ClusterCurrentMxDescribeReplyPage;

typedef struct ClusterCurrentMxProofReplyHeader {
	uint32 magic;
	uint16 version;
	uint8 kind;
	uint8 result;
	uint32 flags;
	uint32 source_node_id;
	uint64 request_id;
	ClusterCurrentMxKey mxkey;
	uint64 descriptor_hash;
	uint32 total_count;
	uint16 entry_count;
	uint8 chunk_ordinal;
	uint8 chunk_count_minus_one;
	uint16 wire_length;
	uint16 reserved16;
	uint32 requester_capability_generation;
} ClusterCurrentMxProofReplyHeader;

typedef struct ClusterCurrentMxUpdaterProofReplyBodyWire {
	ClusterCurrentMemberProof member_proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint8 reserved[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME * sizeof(ClusterCurrentMemberProof)
				   - sizeof(ClusterCurrentMemberProof) - sizeof(ClusterCurrentUpdaterProof)];
} ClusterCurrentMxUpdaterProofReplyBodyWire;

typedef union ClusterCurrentMxProofReplyBodyWire {
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentMxUpdaterProofReplyBodyWire updater;
	uint8 raw[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME * sizeof(ClusterCurrentMemberProof)];
} ClusterCurrentMxProofReplyBodyWire;

#define CLUSTER_CURRENT_MX_PROOF_REPLY_RESERVED_SIZE                                               \
	(BLCKSZ - sizeof(ClusterCurrentMxProofReplyHeader) - sizeof(ClusterCurrentMxProofReplyBodyWire))

typedef struct ClusterCurrentMxProofReplyPage {
	ClusterCurrentMxProofReplyHeader header;
	ClusterCurrentMxProofReplyBodyWire body;
	uint8 reserved[CLUSTER_CURRENT_MX_PROOF_REPLY_RESERVED_SIZE];
} ClusterCurrentMxProofReplyPage;

StaticAssertDecl(sizeof(ClusterCurrentMxDescribePrefixWire) == sizeof(GcsBlockForwardPayload),
				 "current MX describe prefix must preserve the shipped 64-byte frame");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribePrefixWire, request_id)
						 == offsetof(GcsBlockForwardPayload, request_id)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, epoch)
							== offsetof(GcsBlockForwardPayload, epoch)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, original_requester_node)
							== offsetof(GcsBlockForwardPayload, original_requester_node)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, requester_backend_id)
							== offsetof(GcsBlockForwardPayload, requester_backend_id)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, kind)
							== offsetof(GcsBlockForwardPayload, reserved_0) + 6,
				 "current MX describe routing offsets must preserve the shipped prefix");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeTrailerWire) == 64,
				 "current MX describe trailer must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeForwardV2)
					 == CLUSTER_CURRENT_MX_DESCRIBE_FORWARD_SIZE,
				 "current MX describe forward must remain 128 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofAskWire) == 8,
				 "current MX proof ask must remain 8 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterChallengeWire) == 56,
				 "current MX updater challenge wire must remain 56 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterChallengeBodyWire) == 56,
				 "current MX updater challenge body must remain 56 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofRequestBodyWire) == 56,
				 "current MX proof request body must remain 56 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofPrefixWire) == 64,
				 "current MX proof prefix must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, request_id)
						 == offsetof(GcsBlockForwardPayload, request_id)
					 && offsetof(ClusterCurrentMxProofPrefixWire, epoch)
							== offsetof(GcsBlockForwardPayload, epoch)
					 && offsetof(ClusterCurrentMxProofPrefixWire, original_requester_node)
							== offsetof(GcsBlockForwardPayload, original_requester_node)
					 && offsetof(ClusterCurrentMxProofPrefixWire, requester_backend_id)
							== offsetof(GcsBlockForwardPayload, requester_backend_id)
					 && offsetof(ClusterCurrentMxProofPrefixWire, kind)
							== offsetof(GcsBlockForwardPayload, reserved_0) + 6,
				 "current MX proof routing offsets must preserve the shipped prefix");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, mxkey) == 16
					 && offsetof(ClusterCurrentMxProofPrefixWire, total_count) == 44
					 && offsetof(ClusterCurrentMxProofPrefixWire, chunk_ordinal) == 48
					 && offsetof(ClusterCurrentMxProofPrefixWire, descriptor_hash_bytes) == 49
					 && offsetof(ClusterCurrentMxProofPrefixWire, chunk_count_minus_one) == 57
					 && offsetof(ClusterCurrentMxProofPrefixWire, kind) == 63,
				 "current MX proof overlay offsets changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofTrailerWire) == 64,
				 "current MX proof trailer must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofForwardV2) == CLUSTER_CURRENT_MX_PROOF_FORWARD_SIZE,
				 "current MX proof forward must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofRequestPlan, request) == 8,
				 "current MX proof plan request alignment changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyHeader) == 64,
				 "current MX describe reply header must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyPage, members) == 64,
				 "current MX describe members must begin at byte 64");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyPage) == BLCKSZ,
				 "current MX describe reply must fill one GCS page");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyHeader) == 64,
				 "current MX proof reply header must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyHeader, requester_capability_generation) == 60,
				 "current MX requester capability carrier offset must remain 60");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterProofReplyBodyWire)
					 == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
							* sizeof(ClusterCurrentMemberProof),
				 "current MX updater proof reply body size changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyBodyWire)
					 == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
							* sizeof(ClusterCurrentMemberProof),
				 "current MX proof reply body size changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyPage, body) == 64,
				 "current MX proof reply body must begin at byte 64");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyPage) == BLCKSZ,
				 "current MX proof reply must fill one GCS page");

static inline void
ClusterCurrentMxProofPrefixSetDescriptorHash(ClusterCurrentMxProofPrefixWire *prefix, uint64 hash)
{
	prefix->descriptor_hash_bytes[0] = (uint8)(hash & 0xff);
	prefix->descriptor_hash_bytes[1] = (uint8)((hash >> 8) & 0xff);
	prefix->descriptor_hash_bytes[2] = (uint8)((hash >> 16) & 0xff);
	prefix->descriptor_hash_bytes[3] = (uint8)((hash >> 24) & 0xff);
	prefix->descriptor_hash_bytes[4] = (uint8)((hash >> 32) & 0xff);
	prefix->descriptor_hash_bytes[5] = (uint8)((hash >> 40) & 0xff);
	prefix->descriptor_hash_bytes[6] = (uint8)((hash >> 48) & 0xff);
	prefix->descriptor_hash_bytes[7] = (uint8)((hash >> 56) & 0xff);
}

static inline uint64
ClusterCurrentMxProofPrefixGetDescriptorHash(const ClusterCurrentMxProofPrefixWire *prefix)
{
	uint64 hash = 0;

	hash |= (uint64)prefix->descriptor_hash_bytes[0];
	hash |= (uint64)prefix->descriptor_hash_bytes[1] << 8;
	hash |= (uint64)prefix->descriptor_hash_bytes[2] << 16;
	hash |= (uint64)prefix->descriptor_hash_bytes[3] << 24;
	hash |= (uint64)prefix->descriptor_hash_bytes[4] << 32;
	hash |= (uint64)prefix->descriptor_hash_bytes[5] << 40;
	hash |= (uint64)prefix->descriptor_hash_bytes[6] << 48;
	hash |= (uint64)prefix->descriptor_hash_bytes[7] << 56;
	return hash;
}

extern bool cluster_multixact_current_wire_validate_describe_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxDescribeForwardV2 *decoded);
extern ClusterMxDescribeResult cluster_multixact_current_wire_validate_describe_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	uint64 expected_request_id, const ClusterCurrentMxKey *expected_key,
	ClusterCurrentMxMemberDesc *members, uint16 members_cap, uint16 *members_count,
	uint32 *reported_total_members);
extern bool cluster_multixact_current_wire_validate_proof_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxProofForwardV2 *decoded);
extern ClusterMxResolveResult cluster_multixact_current_wire_build_proof_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 descriptor_hash,
	const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id, uint64 current_epoch,
	int32 requester_node, int32 requester_backend_id, ClusterCurrentMxProofRequestPlan *plans,
	uint16 plans_cap, uint16 *plan_count);
extern ClusterMxResolveResult cluster_multixact_current_wire_validate_proof_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request, ClusterCurrentMemberProof *proofs,
	uint16 proofs_cap, uint16 *proof_count, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out);
extern bool cluster_multixact_current_wire_validate_proof_reply_frame(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request, ClusterMxResolveResult *result,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof, uint32 *requester_capability_generation_out);

#endif /* CLUSTER_MULTIXACT_CURRENT_WIRE_H */
