/*-------------------------------------------------------------------------
 *
 * test_cluster_side_stats.c
 *    RF-SIDE D-SIDE-10 focused unit tests: the observability counters
 *    and the U-SIDE-16 rule that counters can never change a verdict.
 *
 *    RED mapping (spec §5.1 U-SIDE-16 + §1.2 D-SIDE-10):
 *      - every counter has a single producer + a G2 vocabulary entry;
 *      - unknown vocabulary names fail closed;
 *      - a counter INFLATED to a huge value never changes any verdict
 *        (the verdict judgements are pure over their typed facts and
 *        never read a counter).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_recovery.h"
#include "cluster/cluster_side_route.h"
#include "cluster/cluster_side_stats.h"

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

static ClusterSideStats ut_stats;

UT_TEST(test_counters_and_vocabulary)
{
	static const char *names[]
		= { "cluster.side.route_applies",		  "cluster.side.route_noops",
			"cluster.side.route_blocked",		  "cluster.side.domain_tt_undo",
			"cluster.side.domain_projection",	  "cluster.side.domain_storage",
			"cluster.side.blocked_unknown_class", "cluster.side.blocked_authority",
			"cluster.side.rebuild_events",		  "cluster.side.durability_events" };
	int i;
	int kind;

	cluster_side_stats_init(&ut_stats);

	cluster_side_stats_route(&ut_stats, CLUSTER_SIDE_ROUTE_VERDICT_APPLY);
	cluster_side_stats_route(&ut_stats, CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP);
	cluster_side_stats_route(&ut_stats, CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED);
	cluster_side_stats_domain(&ut_stats, CLUSTER_SIDE_ROUTE_TT_UNDO);
	cluster_side_stats_domain(&ut_stats, CLUSTER_SIDE_ROUTE_PROJECTION);
	cluster_side_stats_domain(&ut_stats, CLUSTER_SIDE_ROUTE_STORAGE);
	cluster_side_stats_blocked(&ut_stats, true);
	cluster_side_stats_blocked(&ut_stats, false);
	cluster_side_stats_rebuild(&ut_stats);
	cluster_side_stats_durability(&ut_stats);

	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.route_applies), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.route_noops), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.route_blocked), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.domain_tt_undo), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.domain_projection), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.domain_storage), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.blocked_unknown_class), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.blocked_authority), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.rebuild_events), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.durability_events), UINT64_C(1));

	/* G2 vocabulary: every name maps to exactly one kind; unknown names
	 * fail closed. */
	for (i = 0; i < (int)lengthof(names); i++)
		UT_ASSERT(cluster_side_stats_describe(names[i], &kind));
	UT_ASSERT(!cluster_side_stats_describe("cluster.side.no_such", &kind));
	UT_ASSERT(!cluster_side_stats_describe(NULL, &kind));

	/* NULL safety. */
	cluster_side_stats_route(NULL, 0);
}

UT_TEST(test_counter_never_changes_verdict)
{
	ClusterSideRouteRow row;
	ClusterSideReadinessInput ready;

	/* U-SIDE-16: inflate every counter to a huge value; the verdict
	 * judgements are pure over their typed facts and never read a
	 * counter, so every verdict must be identical. */
	{
		int i;
		pg_atomic_uint64 *fields = (pg_atomic_uint64 *)&ut_stats;
		int nfields = sizeof(ClusterSideStats) / sizeof(pg_atomic_uint64);

		for (i = 0; i < nfields; i++)
			pg_atomic_write_u64(&fields[i], UINT64_MAX / 2);
	}

	UT_ASSERT(cluster_side_route_lookup(10 /* RM_HEAP_ID */, 0x10, &row));
	UT_ASSERT_EQ((int)cluster_side_route_verdict(&row), (int)CLUSTER_SIDE_ROUTE_VERDICT_APPLY);

	memset(&ready, 0, sizeof(ready));
	ready.resource_id = 1;
	ready.page_proof_ok = true;
	ready.side_proof_ok = true;
	ready.authority_fresh = true;
	UT_ASSERT(cluster_side_resource_readiness(&ready));
	/* U-SIDE-14: a stale fresh-authority proof keeps the resource
	 * closed even when every other readiness fact holds. */
	ready.authority_fresh = false;
	UT_ASSERT(!cluster_side_resource_readiness(&ready));
}

int
main(void)
{
	UT_PLAN(2);

	UT_RUN(test_counters_and_vocabulary);
	UT_RUN(test_counter_never_changes_verdict);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
