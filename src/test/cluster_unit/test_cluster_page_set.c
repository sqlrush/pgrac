/*-------------------------------------------------------------------------
 *
 * test_cluster_page_set.c
 *    RF-PAGE PGDEL-05 focused unit tests: the §6.3 contributor closure
 *    and the §3.3 shape-2 trusted chain skip.
 *
 *    RED mapping (spec §10.1 / §3.4 / §6.3):
 *      - closed exact chain            -> OK (PU-02 chain form);
 *      - chain gap (missing version)   -> GAP (PU-05: numeric-higher is
 *        not coverage; PU-25 STORAGE contributor gap);
 *      - unknown-class contributor     -> UNKNOWN_CLASS (PU-07);
 *      - incarnation boundary crossing -> INCARNATION_CROSS (PU-06/11:
 *        old-incarnation redo never applies to the new incarnation);
 *      - terminal mismatch             -> TERMINAL_MISMATCH (no guessing);
 *      - empty chain source==terminal  -> OK; source!=terminal -> GAP
 *        (PU-22: an empty set is a conclusion only with the witness the
 *        PGDEL-04 CURRENT validator carries);
 *      - invalid inputs                -> INVALID_INPUT;
 *      - shape-2 skip: chain_covers true exactly when a closed chain
 *        from from_version reaches the terminal; a numeric high-water
 *        never covers (PU-05).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_set.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

static ClusterPageIdentity ut_id;
static ClusterPageVersion ut_v[6]; /* v0..v5 = token 100..105 */
static ClusterPageRedoChange ut_changes[5];

static void
ut_setup_globals(void)
{
	int i;

	memset(&ut_id, 0, sizeof(ut_id));
	ut_id.rlocator.spcOid = 1;
	ut_id.rlocator.dbOid = 2;
	ut_id.rlocator.relNumber = 3;
	ut_id.forknum = MAIN_FORKNUM;
	ut_id.blocknum = 42;

	for (i = 0; i < 6; i++) {
		memset(&ut_v[i], 0, sizeof(ut_v[i]));
		ut_v[i].identity = ut_id;
		ut_v[i].incarnation = 7;
		ut_v[i].token = (uint64)(100 + i);
	}

	for (i = 0; i < 5; i++) {
		memset(&ut_changes[i], 0, sizeof(ut_changes[i]));
		ut_changes[i].identity = ut_id;
		ut_changes[i].page_class = CLUSTER_PAGE_CLASS_NORMAL;
		ut_changes[i].failed_origin_thread = 2;
		ut_changes[i].change_identity = (uint64)(i + 1);
	}
}

static ClusterBlockRecoverySet
ut_set(int n_changes)
{
	ClusterBlockRecoverySet set;

	memset(&set, 0, sizeof(set));
	set.failed_origin_thread = 2;
	set.identity = ut_id;
	set.page_class = CLUSTER_PAGE_CLASS_NORMAL;
	set.source_kind = CLUSTER_PAGE_SOURCE_CURRENT;
	set.source_version = ut_v[0];
	set.terminal_version = ut_v[n_changes]; /* chain v0 -> v1 -> ... -> vn */
	/* An empty chain is a NULL contributor list (a non-NULL pointer with
	 * n==0 is a contradictory input). */
	set.contributors = (n_changes > 0) ? ut_changes : NULL;
	set.n_contributors = n_changes;
	return set;
}

/* Build a closed chain v[i-1] -> v[i] for changes 0..n-1.  Resets
 * every chain-relevant field so earlier tests cannot pollute later ones. */
static void
ut_chain(int n)
{
	int i;

	for (i = 0; i < n; i++) {
		ut_changes[i].identity = ut_id;
		ut_changes[i].page_class = CLUSTER_PAGE_CLASS_NORMAL;
		ut_changes[i].failed_origin_thread = 2;
		ut_changes[i].change_identity = (uint64)(i + 1);
		ut_changes[i].expected_before = ut_v[i];
		ut_changes[i].result_version = ut_v[i + 1];
	}
}

UT_TEST(test_closure_closed_chain_ok)
{
	ClusterBlockRecoverySet set;

	ut_chain(3);
	set = ut_set(3);
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_OK);
	/* Rerun determinism (§6.3-9): same inputs, same verdict. */
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_OK);
}

UT_TEST(test_closure_gap_and_terminal_mismatch)
{
	ClusterBlockRecoverySet set;

	/* Gap: change 1's expected_before is not change 0's result. */
	ut_chain(3);
	ut_changes[1].expected_before = ut_v[0]; /* skips v1 -> wrong join */
	ut_changes[1].result_version = ut_v[2];
	set = ut_set(3);
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_GAP);

	/* Gap: invalid version in a contributor (PU-05: numeric-higher or
	 * missing is never coverage). */
	ut_chain(3);
	memset(&ut_changes[1].expected_before, 0, sizeof(ut_changes[1].expected_before));
	set = ut_set(3);
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_GAP);

	/* Terminal mismatch: chain ends at v3 but the set requires v4. */
	ut_chain(3);
	set = ut_set(3);
	set.terminal_version = ut_v[4];
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
				 (int)CLUSTER_PAGE_CLOSURE_TERMINAL_MISMATCH);
}

UT_TEST(test_closure_unknown_class_and_incarnation_cross)
{
	ClusterBlockRecoverySet set;

	/* Unknown-class contributor -> UNKNOWN_CLASS (PU-07). */
	ut_chain(2);
	ut_changes[1].page_class = CLUSTER_PAGE_CLASS_UNKNOWN;
	set = ut_set(2);
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
				 (int)CLUSTER_PAGE_CLOSURE_UNKNOWN_CLASS);

	/* Incarnation boundary: equal-SCN-looking versions with a different
	 * incarnation are NOT adjacency (PU-06/11) — old-incarnation redo
	 * never applies to the new-incarnation page. */
	ut_chain(2);
	ut_changes[1].expected_before = ut_v[1];
	ut_changes[1].expected_before.incarnation = 8; /* new incarnation */
	ut_changes[1].result_version = ut_v[2];
	ut_changes[1].result_version.incarnation = 8;
	set = ut_set(2);
	set.terminal_version = ut_v[2];
	set.terminal_version.incarnation = 8;
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
				 (int)CLUSTER_PAGE_CLOSURE_INCARNATION_CROSS);
}

UT_TEST(test_closure_empty_chain)
{
	ClusterBlockRecoverySet set;

	/* Empty chain, source already at the terminal: OK — but the "empty
	 * replay set" conclusion additionally needs the production GCS
	 * stability witness that the PGDEL-04 CURRENT validator carries. */
	set = ut_set(0);
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_OK);

	/* Empty chain, source != terminal: a gap, never a skip. */
	set = ut_set(0);
	set.terminal_version = ut_v[1];
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set), (int)CLUSTER_PAGE_CLOSURE_GAP);
}

UT_TEST(test_closure_invalid_inputs)
{
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(NULL),
				 (int)CLUSTER_PAGE_CLOSURE_INVALID_INPUT);
	{
		ClusterBlockRecoverySet set = ut_set(1);
		ClusterPageVersion invalid;

		memset(&invalid, 0, sizeof(invalid));
		set.source_version = invalid;
		UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
					 (int)CLUSTER_PAGE_CLOSURE_INVALID_INPUT);
	}
}

UT_TEST(test_chain_covers_shape2_skip)
{
	ClusterBlockRecoverySet set;

	/* Closed chain v0 -> v1 -> v2 -> v3.  from v1: the sub-chain v1 ->
	 * v2 -> v3 is closed to the terminal -> covered.  from v2: closed.
	 * from v3: the change producing v3 is the last; its result IS the
	 * terminal -> covered. */
	ut_chain(3);
	set = ut_set(3);
	UT_ASSERT(cluster_page_contributor_chain_covers(&set, &ut_v[1]));
	UT_ASSERT(cluster_page_contributor_chain_covers(&set, &ut_v[2]));
	UT_ASSERT(cluster_page_contributor_chain_covers(&set, &ut_v[3]));

	/* from v0 (the source, produced by NO change): not covered by shape 2
	 * (shape 1 — source == terminal — is the PGDEL-01 gate's). */
	UT_ASSERT(!cluster_page_contributor_chain_covers(&set, &ut_v[0]));

	/* A numeric high-water (token 999) is never a covered version. */
	{
		ClusterPageVersion high = ut_v[3];

		high.token = 999;
		UT_ASSERT(!cluster_page_contributor_chain_covers(&set, &high));
	}

	/* A version produced by a change whose onward chain is BROKEN is not
	 * covered. */
	ut_chain(3);
	ut_changes[2].expected_before = ut_v[0]; /* breaks the v2->v3 join */
	set = ut_set(3);
	UT_ASSERT(!cluster_page_contributor_chain_covers(&set, &ut_v[1]));

	/* NULL / invalid inputs. */
	UT_ASSERT(!cluster_page_contributor_chain_covers(NULL, &ut_v[1]));
	{
		ClusterPageVersion invalid;

		memset(&invalid, 0, sizeof(invalid));
		UT_ASSERT(!cluster_page_contributor_chain_covers(&set, &invalid));
	}
}

int
main(void)
{
	UT_PLAN(7);

	ut_setup_globals();

	UT_RUN(test_closure_closed_chain_ok);
	UT_RUN(test_closure_gap_and_terminal_mismatch);
	UT_RUN(test_closure_unknown_class_and_incarnation_cross);
	UT_RUN(test_closure_empty_chain);
	UT_RUN(test_closure_invalid_inputs);
	UT_RUN(test_chain_covers_shape2_skip);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
