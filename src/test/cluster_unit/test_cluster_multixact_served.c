/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact_served.c
 *	  Standalone unit tests for the spec-7.1 D3-b served-verdict multixact
 *	  combination resolver (cluster_multixact_resolve_visibility_served):
 *	  the visibility truth table over origin-served member terminals, the
 *	  lock-only skip (A2), and the 8.A fail-closed on any unproven updater.
 *
 *	  Links cluster_multixact_served.o only (pure); scn_time_cmp is stubbed
 *	  locally (as in test_cluster_visibility_decide_scn) to avoid dragging
 *	  cluster_scn.o's PG-core deps.  The resolver's per-COMMITTED decision
 *	  mirrors cluster_multixact_resolve_visibility verbatim, so these cases
 *	  encode the SAME polarity as the shipped local-TT resolver (TAP t/212).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_multixact_served.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.1-cross-instance-positive-interread.md (D3-b, A1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_scn.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* Local scn_time_cmp mirroring cluster_scn.c (local_scn-only contract). */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

static SCN
make_scn(NodeId node_id, uint64 local_scn)
{
	return scn_encode(node_id, local_scn);
}

/* Build one served member. */
static ClusterMultiXactServedMember
mk(uint8 status, uint8 verdict, SCN commit_scn, SCN horizon_scn)
{
	ClusterMultiXactServedMember m;

	memset(&m, 0, sizeof(m));
	m.member_status = status;
	m.verdict = verdict;
	m.commit_scn = commit_scn;
	m.horizon_scn = horizon_scn;
	m.xid = 1234;
	return m;
}

#define LOCK_ONLY MultiXactStatusForShare /* 1 */
#define UPDATER MultiXactStatusUpdate	  /* 5 */
#define V_EXACT ((uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT)
#define V_BELOW ((uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON)
#define V_ABORT ((uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED)

UT_TEST(t1_empty_is_visible)
{
	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(NULL, 0, make_scn(0, 100)),
				 (int)CLUSTER_VISIBILITY_UNKNOWN); /* NULL members -> fail closed */
}

UT_TEST(t2_zero_count_is_visible)
{
	ClusterMultiXactServedMember m = mk(UPDATER, V_ABORT, InvalidScn, InvalidScn);

	/* member_count 0 -> no updater hid the tuple -> visible. */
	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 0, make_scn(0, 100)),
				 (int)CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(t3_all_lock_only_is_visible)
{
	ClusterMultiXactServedMember ms[2]
		= { mk(LOCK_ONLY, 0, InvalidScn, InvalidScn),
			mk(MultiXactStatusForKeyShare, 0, InvalidScn, InvalidScn) };

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(ms, 2, make_scn(0, 100)),
				 (int)CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(t4_updater_aborted_is_visible)
{
	ClusterMultiXactServedMember m = mk(UPDATER, V_ABORT, InvalidScn, InvalidScn);

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(t5_updater_committed_before_snapshot_is_invisible)
{
	/* Committed-updater leg #1 (P0 polarity SSOT): commit_scn 100 <= read_scn
	 * 200 -- the delete is visible at the snapshot -> the tuple is gone. */
	ClusterMultiXactServedMember m = mk(UPDATER, V_EXACT, make_scn(0, 100), InvalidScn);

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(t6_updater_committed_after_snapshot_is_visible)
{
	/* Committed-updater leg #2 (P0 polarity SSOT): commit_scn 300 > read_scn
	 * 200 -- the delete committed AFTER the snapshot -> the row stays live. */
	ClusterMultiXactServedMember m = mk(UPDATER, V_EXACT, make_scn(0, 300), InvalidScn);

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(t7_updater_committed_invalid_scn_is_unknown)
{
	ClusterMultiXactServedMember m = mk(UPDATER, V_EXACT, InvalidScn, InvalidScn);

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(t8_updater_no_verdict_is_unprovable)
{
	/* verdict 0 on an updater (origin could not resolve / in-progress) -> 8.A UNKNOWN. */
	ClusterMultiXactServedMember m = mk(UPDATER, 0, InvalidScn, InvalidScn);

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(t9_updater_below_horizon_admissible_is_invisible)
{
	/* horizon 100 <= read 200 -> admissible bound; the committed delete is
	 * at/below the horizon <= read_scn -> delete visible -> tuple gone. */
	ClusterMultiXactServedMember m = mk(UPDATER, V_BELOW, InvalidScn, make_scn(0, 100));

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(t10_updater_below_horizon_inadmissible_is_unknown)
{
	/* horizon 300 > read 200 -> inadmissible bound -> fail closed. */
	ClusterMultiXactServedMember m = mk(UPDATER, V_BELOW, InvalidScn, make_scn(0, 300));

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(t11_below_horizon_invalid_readscn_is_unknown)
{
	ClusterMultiXactServedMember m = mk(UPDATER, V_BELOW, InvalidScn, make_scn(0, 100));

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(&m, 1, InvalidScn),
				 (int)CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(t12_lock_only_prefix_plus_hiding_updater_is_invisible)
{
	/* lock-only skipped; updater commit 100 <= read 200 -> delete visible -> hidden. */
	ClusterMultiXactServedMember ms[2] = { mk(LOCK_ONLY, 0, InvalidScn, InvalidScn),
										   mk(UPDATER, V_EXACT, make_scn(0, 100), InvalidScn) };

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(ms, 2, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(t13_aborted_updater_then_hiding_updater_is_invisible)
{
	/* aborted updater continues; committed updater commit 100 <= read 200 hides. */
	ClusterMultiXactServedMember ms[2] = { mk(UPDATER, V_ABORT, InvalidScn, InvalidScn),
										   mk(UPDATER, V_EXACT, make_scn(0, 100), InvalidScn) };

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(ms, 2, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(t14_unprovable_updater_short_circuits_over_later_visible)
{
	/* First updater has no verdict -> UNKNOWN immediately, even though a later
	 * member would be benign.  8.A: any unproven updater fails the whole multi. */
	ClusterMultiXactServedMember ms[2]
		= { mk(UPDATER, 0, InvalidScn, InvalidScn), mk(UPDATER, V_ABORT, InvalidScn, InvalidScn) };

	UT_ASSERT_EQ((int)cluster_multixact_resolve_visibility_served(ms, 2, make_scn(0, 200)),
				 (int)CLUSTER_VISIBILITY_UNKNOWN);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(t1_empty_is_visible);
	UT_RUN(t2_zero_count_is_visible);
	UT_RUN(t3_all_lock_only_is_visible);
	UT_RUN(t4_updater_aborted_is_visible);
	UT_RUN(t5_updater_committed_before_snapshot_is_invisible);
	UT_RUN(t6_updater_committed_after_snapshot_is_visible);
	UT_RUN(t7_updater_committed_invalid_scn_is_unknown);
	UT_RUN(t8_updater_no_verdict_is_unprovable);
	UT_RUN(t9_updater_below_horizon_admissible_is_invisible);
	UT_RUN(t10_updater_below_horizon_inadmissible_is_unknown);
	UT_RUN(t11_below_horizon_invalid_readscn_is_unknown);
	UT_RUN(t12_lock_only_prefix_plus_hiding_updater_is_invisible);
	UT_RUN(t13_aborted_updater_then_hiding_updater_is_invisible);
	UT_RUN(t14_unprovable_updater_short_circuits_over_later_visible);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
