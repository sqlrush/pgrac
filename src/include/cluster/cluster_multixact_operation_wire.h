/*-------------------------------------------------------------------------
 * cluster_multixact_operation_wire.h
 *    Exact-operation codecs staged for the complete current-MX wire family.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_OPERATION_WIRE_H
#define CLUSTER_MULTIXACT_OPERATION_WIRE_H

#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_savepoint_reader.h"

/* These codecs do not activate a transport, grant authority, or change the
 * deployed wire version. All queue/origin/consumer legs must switch together. */
#define CLUSTER_CURRENT_MX_OPERATION_WIRE_VERSION 6
#define CLUSTER_CURRENT_MX_OPERATION_ASK_SIZE 96
#define CLUSTER_CURRENT_MX_OPERATION_BODY_SIZE 672

typedef struct ClusterCurrentMxOperationForwardV6 {
	ClusterCurrentMxProofPrefixWire prefix;
	uint32 magic;
	uint16 version;
	uint16 reserved16;
	uint8 body[CLUSTER_CURRENT_MX_OPERATION_BODY_SIZE];
	uint8 reserved[24];
} ClusterCurrentMxOperationForwardV6;

typedef struct ClusterCurrentMxOperationRequestPlan {
	uint16 destination_node_id;
	uint16 reserved16;
	ClusterCurrentMxOperationForwardV6 request;
} ClusterCurrentMxOperationRequestPlan;

typedef struct ClusterCurrentMxOperationAsk {
	ClusterCurrentMxProofAskWire member;
	bool exact;
	ClusterSavepointOperationKey operation;
} ClusterCurrentMxOperationAsk;

typedef struct ClusterCurrentMxOperationReplyPageV6 {
	ClusterCurrentMxProofReplyHeader header;
	uint8 body[616];
	uint8 reserved[7512];
} ClusterCurrentMxOperationReplyPageV6;

StaticAssertDecl(sizeof(ClusterCurrentMxOperationForwardV6) == 768,
				 "operation request must be exactly 768 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxOperationForwardV6, body) == 72,
				 "operation entries must start at byte 72");
StaticAssertDecl(offsetof(ClusterCurrentMxOperationRequestPlan, request) == 8,
				 "operation plan must align the request");
StaticAssertDecl(sizeof(ClusterCurrentMxOperationReplyPageV6) == BLCKSZ,
				 "operation reply must remain one page");

/* A NULL operations array or an entirely zero key means canonical top-only.
 * A nonzero incomplete key is rejected, never downgraded to a raw lookup. */
extern ClusterMxResolveResult cluster_multixact_operation_wire_build_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *origins, const ClusterSavepointOperationKey *operations, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id,
	uint64 epoch, int32 requester, int32 backend, ClusterCurrentMxOperationRequestPlan *plans,
	uint16 capacity, uint16 *count);
extern bool
cluster_multixact_operation_wire_validate_forward(const void *payload, uint32 length, int32 source,
												  int32 local, uint64 epoch,
												  ClusterCurrentMxOperationForwardV6 *out);
/* Use only a frame accepted by validate_forward. This checks the entry again
 * and returns compare input, not an origin or retention proof. */
extern bool
cluster_multixact_operation_wire_get_ask(const ClusterCurrentMxOperationForwardV6 *request,
										 uint16 index, ClusterCurrentMxOperationAsk *out);
extern bool cluster_multixact_operation_wire_encode_reply(
	const ClusterCurrentMxOperationForwardV6 *request, int32 source, uint64 epoch,
	ClusterMxResolveResult result, const ClusterCurrentMemberOperationProof *proofs, uint16 count,
	const ClusterCurrentUpdaterProof *updater, uint32 capability_generation,
	ClusterCurrentMxOperationReplyPageV6 *out);
/* false is malformed/UNKNOWN, with every output reset. A valid typed UNKNOWN
 * frame returns true with result UNKNOWN and no proofs or capability. */
extern bool cluster_multixact_operation_wire_validate_reply(
	const void *payload, uint32 length, int32 source, uint64 epoch,
	const ClusterCurrentMxOperationForwardV6 *request, ClusterMxResolveResult *result,
	ClusterCurrentMemberOperationProof *proofs, uint16 capacity, uint16 *count,
	ClusterCurrentUpdaterProof *updater, uint32 *capability_generation);

#endif
