/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_identity.c
 *    Resource-X logical assertion identity — spec-8.6 D6-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_identity.h"

#define RESOURCE_X_HASH_OFFSET UINT64CONST(1469598103934665603)
#define RESOURCE_X_HASH_PRIME UINT64CONST(1099511628211)

static uint64
resource_x_hash_scalar(uint64 hash, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		hash ^= (uint8)(value & UINT64CONST(0xff));
		hash *= RESOURCE_X_HASH_PRIME;
		value >>= 8;
	}
	return hash;
}

bool
resource_x_assertion_valid(const ResourceXAssertion *assertion)
{
	if (assertion == NULL)
		return false;
	if (!RelFileNumberIsValid(assertion->resource.relNumber)
		|| assertion->resource.blockNum == InvalidBlockNumber
		|| assertion->resource.forkNum < MAIN_FORKNUM || assertion->resource.forkNum > MAX_FORKNUM)
		return false;
	return assertion->requester_node >= 0
		   && assertion->requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
}

bool
resource_x_assertion_init(const BufferTag *tag, int32 requester_node, ResourceXAssertion *out)
{
	ResourceXAssertion candidate;

	if (tag == NULL || out == NULL)
		return false;
	candidate.resource = *tag;
	candidate.requester_node = requester_node;
	if (!resource_x_assertion_valid(&candidate))
		return false;
	*out = candidate;
	return true;
}

bool
resource_x_assertion_equal(const ResourceXAssertion *left, const ResourceXAssertion *right)
{
	return resource_x_assertion_valid(left) && resource_x_assertion_valid(right)
		   && BufferTagsEqual(&left->resource, &right->resource)
		   && left->requester_node == right->requester_node;
}

uint32
resource_x_assertion_hash(const ResourceXAssertion *assertion)
{
	uint64 hash = RESOURCE_X_HASH_OFFSET;

	if (!resource_x_assertion_valid(assertion))
		return 0;
	hash = resource_x_hash_scalar(hash, (uint64)assertion->resource.spcOid);
	hash = resource_x_hash_scalar(hash, (uint64)assertion->resource.dbOid);
	hash = resource_x_hash_scalar(hash, (uint64)assertion->resource.relNumber);
	hash = resource_x_hash_scalar(hash, (uint64)(uint32)assertion->resource.forkNum);
	hash = resource_x_hash_scalar(hash, (uint64)assertion->resource.blockNum);
	hash = resource_x_hash_scalar(hash, (uint64)(uint32)assertion->requester_node);
	return (uint32)(hash ^ (hash >> 32));
}

bool
resource_x_attempt_init(const ResourceXAssertion *assertion, uint64 base_authority_generation,
						ResourceXAttemptWitness *out)
{
	ResourceXAttemptWitness candidate;

	if (!resource_x_assertion_valid(assertion) || out == NULL)
		return false;
	candidate.assertion = *assertion;
	candidate.base_authority_generation = base_authority_generation;
	*out = candidate;
	return true;
}

bool
resource_x_attempt_matches(const ResourceXAttemptWitness *left,
						   const ResourceXAttemptWitness *right)
{
	return left != NULL && right != NULL
		   && resource_x_assertion_equal(&left->assertion, &right->assertion)
		   && left->base_authority_generation == right->base_authority_generation;
}

const char *
resource_x_proof_readiness_status(void)
{
	return RESOURCE_X_PROOF_READINESS_AVAILABLE;
}
