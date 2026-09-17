/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_handoff.c
 *    Standalone downstream compile contract for Resource-X semantics.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_identity.h"

#ifdef CLUSTER_PCM_X_CONVERT_H
#error "Resource-X semantic handoff must not import the PCM-X ticket adapter"
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static ResourceXAssertion
make_assertion(void)
{
	BufferTag tag;
	ResourceXAssertion assertion;

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9101;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 64;
	UT_ASSERT(resource_x_assertion_init(&tag, 3, &assertion));
	return assertion;
}

UT_TEST(test_downstream_handoff_is_exact_attempt_semantics)
{
	ResourceXAssertion assertion = make_assertion();
	ResourceXAttemptWitness attempt;

	UT_ASSERT(resource_x_attempt_init(&assertion, UINT64_C(101), &attempt));
	UT_ASSERT(resource_x_assertion_equal(&attempt.assertion, &assertion));
	UT_ASSERT_EQ(attempt.base_authority_generation, UINT64_C(101));

	assertion.resource.blockNum++;
	UT_ASSERT_EQ(attempt.assertion.resource.blockNum, 64);
	UT_ASSERT_EQ(attempt.assertion.requester_node, 3);
}

UT_TEST(test_downstream_handoff_rejects_invalid_without_output_mutation)
{
	ResourceXAssertion assertion = make_assertion();
	ResourceXAttemptWitness attempt;
	ResourceXAttemptWitness before;

	MemSet(&attempt, 0xa5, sizeof(attempt));
	before = attempt;
	UT_ASSERT(!resource_x_attempt_init(NULL, 1, &attempt));
	UT_ASSERT(memcmp(&attempt, &before, sizeof(attempt)) == 0);
	UT_ASSERT(!resource_x_attempt_init(&assertion, 1, NULL));
	assertion.requester_node = RESOURCE_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT(!resource_x_attempt_init(&assertion, 1, &attempt));
	UT_ASSERT(memcmp(&attempt, &before, sizeof(attempt)) == 0);

	assertion = make_assertion();
	UT_ASSERT(resource_x_attempt_init(&assertion, 0, &attempt));
	UT_ASSERT_EQ(attempt.base_authority_generation, UINT64_C(0));
}

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_downstream_handoff_is_exact_attempt_semantics);
	UT_RUN(test_downstream_handoff_rejects_invalid_without_output_mutation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
