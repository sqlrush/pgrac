/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_identity.c
 *    Resource-X logical assertion and attempt identity — spec-8.6 D6-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_identity.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static BufferTag
make_tag(void)
{
	BufferTag tag;

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9001;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 42;
	return tag;
}

UT_TEST(test_canonical_layout)
{
	UT_ASSERT_EQ(sizeof(BufferTag), 20);
	UT_ASSERT_EQ(sizeof(ResourceXAssertion), 24);
	UT_ASSERT_EQ(offsetof(ResourceXAssertion, resource), 0);
	UT_ASSERT_EQ(offsetof(ResourceXAssertion, requester_node), 20);
	UT_ASSERT_EQ(sizeof(ResourceXAttemptWitness), 32);
	UT_ASSERT_EQ(offsetof(ResourceXAttemptWitness, assertion), 0);
	UT_ASSERT_EQ(offsetof(ResourceXAttemptWitness, base_authority_generation), 24);
	UT_ASSERT_EQ(sizeof(ResourceXTransportWitness), 24);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, cluster_epoch), 0);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, peer_session_incarnation), 8);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, connection_generation), 16);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, lane_id), 20);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, flags), 22);
}

UT_TEST(test_init_accepts_shared_catalog_identity)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	tag.dbOid = InvalidOid;
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &assertion));
	UT_ASSERT(resource_x_assertion_valid(&assertion));
	UT_ASSERT(BufferTagsEqual(&assertion.resource, &tag));
	UT_ASSERT_EQ(assertion.requester_node, 0);
}

UT_TEST(test_validation_rejects_invalid_native_tag_fields)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	UT_ASSERT(resource_x_assertion_init(&tag, RESOURCE_X_PROTOCOL_NODE_LIMIT - 1, &assertion));
	assertion.resource.relNumber = InvalidRelFileNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.resource.blockNum = InvalidBlockNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.resource.forkNum = InvalidForkNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource.forkNum = (ForkNumber)(MAX_FORKNUM + 1);
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.requester_node = -1;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.requester_node = RESOURCE_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
}

UT_TEST(test_init_rejects_null_and_invalid_inputs)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	UT_ASSERT(!resource_x_assertion_init(NULL, 1, &assertion));
	UT_ASSERT(!resource_x_assertion_init(&tag, 1, NULL));
	tag.relNumber = InvalidRelFileNumber;
	UT_ASSERT(!resource_x_assertion_init(&tag, 1, &assertion));
}

UT_TEST(test_equality_is_exactly_resource_and_requester_node)
{
	BufferTag tag = make_tag();
	ResourceXAssertion left;
	ResourceXAssertion right;

	UT_ASSERT(resource_x_assertion_init(&tag, 7, &left));
	UT_ASSERT(resource_x_assertion_init(&tag, 7, &right));
	UT_ASSERT(resource_x_assertion_equal(&left, &right));
	UT_ASSERT_EQ(resource_x_assertion_hash(&left), resource_x_assertion_hash(&right));
	right.requester_node++;
	UT_ASSERT(!resource_x_assertion_equal(&left, &right));
	right = left;
	right.resource.blockNum++;
	UT_ASSERT(!resource_x_assertion_equal(&left, &right));
}

UT_TEST(test_attempt_match_adds_only_base_generation)
{
	BufferTag tag = make_tag();
	ResourceXAttemptWitness left;
	ResourceXAttemptWitness right;

	UT_ASSERT(resource_x_assertion_init(&tag, 7, &left.assertion));
	left.base_authority_generation = 100;
	right = left;
	UT_ASSERT(resource_x_attempt_matches(&left, &right));
	right.base_authority_generation++;
	UT_ASSERT(!resource_x_attempt_matches(&left, &right));
	right = left;
	right.assertion.requester_node++;
	UT_ASSERT(!resource_x_attempt_matches(&left, &right));
}

UT_TEST(test_null_comparisons_fail_closed)
{
	BufferTag tag = make_tag();
	ResourceXAttemptWitness attempt;

	UT_ASSERT(resource_x_assertion_init(&tag, 1, &attempt.assertion));
	attempt.base_authority_generation = 1;
	UT_ASSERT(!resource_x_assertion_valid(NULL));
	UT_ASSERT(!resource_x_assertion_equal(NULL, &attempt.assertion));
	UT_ASSERT(!resource_x_assertion_equal(&attempt.assertion, NULL));
	UT_ASSERT(!resource_x_attempt_matches(NULL, &attempt));
	UT_ASSERT(!resource_x_attempt_matches(&attempt, NULL));
}

UT_TEST(test_proof_readiness_is_explicitly_available)
{
	UT_ASSERT_STR_EQ(resource_x_proof_readiness_status(), "AVAILABLE_PROOF_KIND");
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_canonical_layout);
	UT_RUN(test_init_accepts_shared_catalog_identity);
	UT_RUN(test_validation_rejects_invalid_native_tag_fields);
	UT_RUN(test_init_rejects_null_and_invalid_inputs);
	UT_RUN(test_equality_is_exactly_resource_and_requester_node);
	UT_RUN(test_attempt_match_adds_only_base_generation);
	UT_RUN(test_null_comparisons_fail_closed);
	UT_RUN(test_proof_readiness_is_explicitly_available);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
