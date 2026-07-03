/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_server_policy.c
 *	  Standalone unit tests for the spec-6.12b CR-server split policy
 *	  (cluster_cr_server_split_classify): FULL / PARTIAL / DENY over
 *	  write_scn-DESC chain-origin sequences, including the malformed-input
 *	  fail-closed and the empty-chain FULL.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Links cluster_cr_server_policy.o
 *	  only; the policy is pure (no shmem / locks / elog).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_server.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Assert hook stub so the cassert libpgport links standalone. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

UT_TEST(test_split_empty_is_full_prefix_zero)
{
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 0, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_all_self_is_full)
{
	int32 origins[3] = { 0, 0, 0 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 3);
}

UT_TEST(test_split_self_prefix_foreign_suffix_is_partial)
{
	int32 origins[4] = { 0, 0, 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 4, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 2);
}

UT_TEST(test_split_all_foreign_is_partial_prefix_zero)
{
	/* Serving nothing is still a legal one-hop reply: the shipped current
	 * copy lets the requester do the whole peel (equivalent to a plain
	 * read image + local construction). */
	int32 origins[2] = { 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 2, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_interleave_is_deny)
{
	int32 self_after_foreign[3] = { 0, 1, 0 };
	int32 foreign_then_self[2] = { 1, 0 };
	int prefix = 77;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(self_after_foreign, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(foreign_then_self, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

UT_TEST(test_split_third_party_suffix_stays_partial)
{
	/* A >=3-node foreign suffix mixing OTHER nodes is still a clean
	 * self-prefix cut here; the REQUESTER's continue-run hits its own
	 * class-(3) walk backstop for the third-party chains (53R9G). */
	int32 origins[3] = { 0, 1, 2 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 1);
}

UT_TEST(test_split_malformed_is_deny)
{
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, -1, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_split_empty_is_full_prefix_zero);
	UT_RUN(test_split_all_self_is_full);
	UT_RUN(test_split_self_prefix_foreign_suffix_is_partial);
	UT_RUN(test_split_all_foreign_is_partial_prefix_zero);
	UT_RUN(test_split_interleave_is_deny);
	UT_RUN(test_split_third_party_suffix_stays_partial);
	UT_RUN(test_split_malformed_is_deny);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
