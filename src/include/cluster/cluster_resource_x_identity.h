/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_identity.h
 *    Resource-X logical assertion identity — spec-8.6 D6-01.
 *
 * The global assertion is exactly (BufferTag, requester node).  Attempt and
 * transport fields are witnesses only: neither expands logical equality nor
 * grants queue/ownership authority.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RESOURCE_X_IDENTITY_H
#define CLUSTER_RESOURCE_X_IDENTITY_H

#include "storage/buf_internals.h"

#define RESOURCE_X_PROTOCOL_NODE_LIMIT 32
#define RESOURCE_X_PROOF_READINESS_UNAVAILABLE "UNAVAILABLE_PROOF_KIND"
#define RESOURCE_X_PROOF_READINESS_AVAILABLE "AVAILABLE_PROOF_KIND"

typedef struct ResourceXAssertion {
	BufferTag resource;
	int32 requester_node;
} ResourceXAssertion;

typedef struct ResourceXAttemptWitness {
	ResourceXAssertion assertion;
	uint64 base_authority_generation;
} ResourceXAttemptWitness;

typedef struct ResourceXTransportWitness {
	uint64 cluster_epoch;
	uint64 peer_session_incarnation;
	uint32 connection_generation;
	uint16 lane_id;
	uint16 flags;
} ResourceXTransportWitness;

/* Process-local D6-03 outcome.  These values are never persisted or sent on
 * the legacy 41--64 wire family. */
typedef enum ResourceXLocalJoinResult {
	RESOURCE_X_LOCAL_JOIN_NONE = 0,
	RESOURCE_X_LOCAL_LEADER_MUST_SUBMIT,
	RESOURCE_X_LOCAL_JOINED_LOCAL_ASSERTION,
	RESOURCE_X_LOCAL_WAIT_SUCCESSOR_ROUND
} ResourceXLocalJoinResult;

StaticAssertDecl(sizeof(BufferTag) == 20,
				 "Resource-X requires the frozen 20-byte BufferTag layout");
StaticAssertDecl(sizeof(ResourceXAssertion) == 24,
				 "Resource-X assertion layout must remain 24 bytes");
StaticAssertDecl(offsetof(ResourceXAssertion, resource) == 0
					 && offsetof(ResourceXAssertion, requester_node) == 20,
				 "Resource-X assertion offsets changed");
StaticAssertDecl(sizeof(ResourceXAttemptWitness) == 32,
				 "Resource-X attempt witness layout must remain 32 bytes");
StaticAssertDecl(offsetof(ResourceXAttemptWitness, assertion) == 0
					 && offsetof(ResourceXAttemptWitness, base_authority_generation) == 24,
				 "Resource-X attempt witness offsets changed");
StaticAssertDecl(sizeof(ResourceXTransportWitness) == 24,
				 "Resource-X transport witness layout must remain 24 bytes");
StaticAssertDecl(offsetof(ResourceXTransportWitness, cluster_epoch) == 0
					 && offsetof(ResourceXTransportWitness, peer_session_incarnation) == 8
					 && offsetof(ResourceXTransportWitness, connection_generation) == 16
					 && offsetof(ResourceXTransportWitness, lane_id) == 20
					 && offsetof(ResourceXTransportWitness, flags) == 22,
				 "Resource-X transport witness offsets changed");

extern bool resource_x_assertion_init(const BufferTag *tag, int32 requester_node,
									  ResourceXAssertion *out);
extern bool resource_x_assertion_valid(const ResourceXAssertion *assertion);
extern bool resource_x_assertion_equal(const ResourceXAssertion *left,
									   const ResourceXAssertion *right);
extern uint32 resource_x_assertion_hash(const ResourceXAssertion *assertion);
extern bool resource_x_attempt_init(const ResourceXAssertion *assertion,
									uint64 base_authority_generation, ResourceXAttemptWitness *out);
extern bool resource_x_attempt_matches(const ResourceXAttemptWitness *left,
									   const ResourceXAttemptWitness *right);
/* R6 readiness only: no proof enum, proof value, producer, or proof-bearing key. */
extern const char *resource_x_proof_readiness_status(void);

#endif /* CLUSTER_RESOURCE_X_IDENTITY_H */
