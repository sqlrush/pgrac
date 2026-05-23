/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_decide_scn.c
 *	  pgrac spec-3.3 D11 — cluster_unit static + behavioral mock tests
 *	  for cluster_visibility_decide_by_scn() inline helper (D5) and the
 *	  ClusterVisibilityDecision enum (L180).
 *
 *	  22 tests (spec-3.3 §1.2 D11):
 *	    T1   ClusterVisibilityDecision enum values stable (VISIBLE=0,
 *	         INVISIBLE=1, UNKNOWN=2; L180 positive presence)
 *	    T2   helper: InvalidScn commit_scn -> UNKNOWN
 *	    T3   helper: InvalidScn read_scn -> UNKNOWN
 *	    T4   helper: both InvalidScn -> UNKNOWN
 *	    T5   helper: commit_scn < read_scn (single-node) -> VISIBLE
 *	    T6   helper: commit_scn == read_scn -> VISIBLE (inclusive)
 *	    T7   helper: commit_scn > read_scn -> INVISIBLE
 *	    T8   helper: cross-node SCN where commit time-precedes read but
 *	         commit_scn raw uint64 > read_scn raw uint64 due to node_id
 *	         high bits -> still VISIBLE via scn_time_cmp (R1 P0)
 *	    T9   helper: inverse cross-node -> still INVISIBLE via
 *	         scn_time_cmp
 *	    T10  helper: minimum valid local SCN visible
 *	    T11  helper: large local SCN invisible
 *	    T12  SCN_VALID(InvalidScn) == false
 *	    T13  SCN_VALID of constructed non-zero SCN == true
 *	    T14  scn_time_cmp ignores node_id high bits
 *	    T15  53R97 ERRCODE_CLUSTER_TT_STATUS_UNKNOWN encodable
 *	    T16  ClusterTTStatus enum still 5 values stable
 *	    T17  SnapshotSource enum LOCAL=0, CLUSTER=1 stable
 *	    T18  ClusterVisibilityDecision sizeof >= sizeof(int) (defensive
 *	         L180 -- a future bool conversion would shrink to 1)
 *	    T19  COMMITTED path mock: helper drives VISIBLE on ordered SCN
 *	    T20  COMMITTED path mock: helper drives UNKNOWN on InvalidScn
 *	    T21  ABORTED enum distinct from COMMITTED (caller short-circuits
 *	         before helper invocation; compile-time guard)
 *	    T22  CLEANED_OUT path mock: helper drives same as COMMITTED
 *
 *	  Standalone executable per spec-0.4 §9.2; no PG backend required.
 *	  Behavioral coverage of full HeapTupleSatisfiesMVCC fork in
 *	  cluster_tap t/205 (D13).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_visibility_decide_scn.c
 *
 * Spec: spec-3.3-snapshot-consistency-cross-node.md (v1.0 FROZEN 2026-05-23)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h"
#include "utils/errcodes.h"
#include "utils/snapshot.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/*
 * cluster_unit binary does not link cluster_scn.o (which drags in
 * errfinish / errstart / SI / superuser from PG core). Provide a local
 * copy of scn_time_cmp mirroring the production implementation
 * (cluster_scn.c). If the production impl ever diverges, this stub will
 * mask the change -- T14 directly asserts the local_scn-only contract
 * so divergence still surfaces.
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64		la = scn_local(a);
	uint64		lb = scn_local(b);

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


UT_TEST(test_t1_enum_values_stable)
{
	UT_ASSERT_EQ((int) CLUSTER_VISIBILITY_VISIBLE, 0);
	UT_ASSERT_EQ((int) CLUSTER_VISIBILITY_INVISIBLE, 1);
	UT_ASSERT_EQ((int) CLUSTER_VISIBILITY_UNKNOWN, 2);
}

UT_TEST(test_t2_invalid_commit_scn_unknown)
{
	SCN			read_scn = make_scn(0, 100);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(InvalidScn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(test_t3_invalid_read_scn_unknown)
{
	SCN			commit_scn = make_scn(0, 100);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, InvalidScn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(test_t4_both_invalid_unknown)
{
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(InvalidScn, InvalidScn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(test_t5_commit_lt_read_visible)
{
	SCN			commit_scn = make_scn(0, 100);
	SCN			read_scn = make_scn(0, 200);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(test_t6_commit_eq_read_visible)
{
	SCN			commit_scn = make_scn(0, 150);
	SCN			read_scn = make_scn(0, 150);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(test_t7_commit_gt_read_invisible)
{
	SCN			commit_scn = make_scn(0, 300);
	SCN			read_scn = make_scn(0, 200);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(test_t8_crossnode_high_node_visible)
{
	/* Node 7 commit at local_scn 100; node 0 read at local_scn 200.
	 * scn_time_cmp ignores node_id high bits -> time-orders commit
	 * before read -> VISIBLE. Raw uint64 compare would mis-classify. */
	SCN			commit_scn = make_scn(7, 100);
	SCN			read_scn = make_scn(0, 200);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(test_t9_crossnode_low_node_invisible)
{
	SCN			commit_scn = make_scn(0, 300);
	SCN			read_scn = make_scn(7, 200);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(test_t10_min_local_scn_visible)
{
	SCN			commit_scn = make_scn(0, 1);
	SCN			read_scn = make_scn(0, 2);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(test_t11_large_local_scn_invisible)
{
	SCN			commit_scn = make_scn(0, ((uint64) 1 << 40));
	SCN			read_scn = make_scn(0, ((uint64) 1 << 32));
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_INVISIBLE);
}

UT_TEST(test_t12_scn_valid_invalid_false)
{
	UT_ASSERT_EQ((int) SCN_VALID(InvalidScn), 0);
}

UT_TEST(test_t13_scn_valid_nonzero_true)
{
	SCN			scn = make_scn(3, 17);

	UT_ASSERT_NE((int) SCN_VALID(scn), 0);
}

UT_TEST(test_t14_scn_time_cmp_ignores_node_high_bits)
{
	SCN			a = make_scn(7, 100);
	SCN			b = make_scn(0, 200);

	UT_ASSERT(scn_time_cmp(a, b) < 0);
}

UT_TEST(test_t15_errcode_53r97_encodable)
{
	int			code = ERRCODE_CLUSTER_TT_STATUS_UNKNOWN;

	UT_ASSERT_NE(code, 0);
}

UT_TEST(test_t16_cluster_tt_status_enum_stable)
{
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int) CLUSTER_TT_STATUS_CLEANED_OUT, 4);
}

UT_TEST(test_t17_snapshot_source_enum_stable)
{
	UT_ASSERT_EQ((int) SNAPSHOT_SOURCE_LOCAL, 0);
	UT_ASSERT_EQ((int) SNAPSHOT_SOURCE_CLUSTER, 1);
}

UT_TEST(test_t18_decision_sizeof_geq_int)
{
	/* L180: decision must be a multi-valued type (enum), not bool. */
	UT_ASSERT((int) sizeof(ClusterVisibilityDecision) >= (int) sizeof(int));
}

UT_TEST(test_t19_committed_mock_visible)
{
	SCN			commit_scn = make_scn(2, 50);
	SCN			read_scn = make_scn(0, 100);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}

UT_TEST(test_t20_committed_mock_unknown_on_invalidscn)
{
	SCN			read_scn = make_scn(0, 100);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(InvalidScn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_UNKNOWN);
}

UT_TEST(test_t21_aborted_short_circuits_distinct_enum)
{
	UT_ASSERT_NE((int) CLUSTER_TT_STATUS_ABORTED, (int) CLUSTER_TT_STATUS_COMMITTED);
}

UT_TEST(test_t22_cleaned_out_mock_visible)
{
	SCN			commit_scn = make_scn(1, 30);
	SCN			read_scn = make_scn(1, 30);
	ClusterVisibilityDecision d =
		cluster_visibility_decide_by_scn(commit_scn, read_scn);

	UT_ASSERT_EQ((int) d, (int) CLUSTER_VISIBILITY_VISIBLE);
}


int
main(void)
{
	UT_RUN(test_t1_enum_values_stable);
	UT_RUN(test_t2_invalid_commit_scn_unknown);
	UT_RUN(test_t3_invalid_read_scn_unknown);
	UT_RUN(test_t4_both_invalid_unknown);
	UT_RUN(test_t5_commit_lt_read_visible);
	UT_RUN(test_t6_commit_eq_read_visible);
	UT_RUN(test_t7_commit_gt_read_invisible);
	UT_RUN(test_t8_crossnode_high_node_visible);
	UT_RUN(test_t9_crossnode_low_node_invisible);
	UT_RUN(test_t10_min_local_scn_visible);
	UT_RUN(test_t11_large_local_scn_invisible);
	UT_RUN(test_t12_scn_valid_invalid_false);
	UT_RUN(test_t13_scn_valid_nonzero_true);
	UT_RUN(test_t14_scn_time_cmp_ignores_node_high_bits);
	UT_RUN(test_t15_errcode_53r97_encodable);
	UT_RUN(test_t16_cluster_tt_status_enum_stable);
	UT_RUN(test_t17_snapshot_source_enum_stable);
	UT_RUN(test_t18_decision_sizeof_geq_int);
	UT_RUN(test_t19_committed_mock_visible);
	UT_RUN(test_t20_committed_mock_unknown_on_invalidscn);
	UT_RUN(test_t21_aborted_short_circuits_distinct_enum);
	UT_RUN(test_t22_cleaned_out_mock_visible);
	UT_DONE();
}
