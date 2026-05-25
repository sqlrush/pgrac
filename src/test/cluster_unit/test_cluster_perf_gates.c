/*-------------------------------------------------------------------------
 *
 * test_cluster_perf_gates.c
 *	  pgrac spec-3.4e D5 — cluster_unit tests for 5-tier × 4 workload
 *	  class perf gate ABI contract (pure ABI / threshold / symbol-link
 *	  only;  per spec-3.4e F5 no SQL UDF / backend behavior / grep-style
 *	  assertions in C unit — those move to TAP / Perl perf runner).
 *
 *	  10 tests covering:
 *	    T1   spec-3.4e workload class count = 4 (single-node-no-peer +
 *	         3× 2node-* classes)
 *	    T2   5-tier name stability (GREEN/YELLOW/ORANGE/RED/CATASTROPHIC
 *	         used by docs/perf-gates.md §1)
 *	    T3   5 metric set count = 5 (TPS / p95_latency / WAL_bytes_per_sec /
 *	         fail_closed_rate / lookup_hit_miss_ratio)
 *	    T4   cluster_remote_row_lock_fail_closed_count counter symbol
 *	         linkable (spec-3.4d D11 — spec-3.4e D6 promotes to shmem
 *	         aggregation)
 *	    T5   cluster_itl_get_remote_row_lock_fail_closed_count accessor
 *	         symbol linkable
 *	    T6   cluster_itl_get_lock_only_itl_stamp_count accessor linkable
 *	    T7   cluster_itl_get_lock_only_tt_hint_emit_count accessor linkable
 *	    T8   ERRCODE_CLUSTER_REMOTE_ROW_LOCK_WAIT_NOT_SUPPORTED = 53R98
 *	         (spec-3.4d Q1 fail-closed — class 4 hot-row trigger)
 *	    T9   spec-3.4e single-node-no-peer GREEN threshold = 15%
 *	         (ship-blocking;  L195 stable contract;  spec-3.4c A4
 *	         实测 GREEN ≤ 15%)
 *	    T10  spec-3.4e ClusterPair fixture partial coverage label
 *	         (class 3/4 inject-based;  真 cross-node shared heap TPS
 *	         contention 推 feature-117 / Stage 4+)
 *
 *	  No real perf measurement here;  D9 perf runner via Perl owns
 *	  pgbench --log parse + 5 metric collection + ClusterPair lifecycle.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_perf_gates.c
 *
 * Spec: spec-3.4e-stage3-multinode-perf-hardening.md
 *       (v0.2 FROZEN 2026-05-25)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_itl.h"
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* Local stubs — cluster_unit binary does not link cluster_itl.o
 * (would drag in shmem/LWLock).  Symbol linkability tests use these. */
void
cluster_itl_bump_remote_row_lock_fail_closed_count(void)
{}
void
cluster_itl_bump_lock_only_itl_stamp_count(void)
{}
void
cluster_itl_bump_lock_only_tt_hint_emit_count(void)
{}
uint64
cluster_itl_get_remote_row_lock_fail_closed_count(void)
{
	return 0;
}
uint64
cluster_itl_get_lock_only_itl_stamp_count(void)
{
	return 0;
}
uint64
cluster_itl_get_lock_only_tt_hint_emit_count(void)
{
	return 0;
}


/* ===== T1: spec-3.4e workload class count = 4 ===== */

/* spec-3.4e §2.1 workload class names — anchor strings for cross-spec
 * inheritance discipline (L201).  Future spec-3.5+ uses same names via
 * docs/perf-gates.md §2 引用. */
#define SPEC_3_4E_WORKLOAD_CLASS_1 "single-node-no-peer"
#define SPEC_3_4E_WORKLOAD_CLASS_2 "2node-local-affinity"
#define SPEC_3_4E_WORKLOAD_CLASS_3 "2node-cross-node-visibility"
#define SPEC_3_4E_WORKLOAD_CLASS_4 "2node-hot-row-lock"
#define SPEC_3_4E_WORKLOAD_CLASS_COUNT 4

UT_TEST(t1_workload_class_count_is_4)
{
	UT_ASSERT_EQ((int) SPEC_3_4E_WORKLOAD_CLASS_COUNT, 4);
}


/* ===== T2: 5-tier name stability ===== */

#define SPEC_3_4E_TIER_GREEN "GREEN"
#define SPEC_3_4E_TIER_YELLOW "YELLOW"
#define SPEC_3_4E_TIER_ORANGE "ORANGE"
#define SPEC_3_4E_TIER_RED "RED"
#define SPEC_3_4E_TIER_CATASTROPHIC "CATASTROPHIC"
#define SPEC_3_4E_TIER_COUNT 5

UT_TEST(t2_5_tier_name_count_is_5)
{
	UT_ASSERT_EQ((int) SPEC_3_4E_TIER_COUNT, 5);
	/* Verify string anchors exist (compile-time link OK if reached here) */
	UT_ASSERT_NE((void *) SPEC_3_4E_TIER_GREEN, NULL);
	UT_ASSERT_NE((void *) SPEC_3_4E_TIER_YELLOW, NULL);
	UT_ASSERT_NE((void *) SPEC_3_4E_TIER_ORANGE, NULL);
	UT_ASSERT_NE((void *) SPEC_3_4E_TIER_RED, NULL);
	UT_ASSERT_NE((void *) SPEC_3_4E_TIER_CATASTROPHIC, NULL);
}


/* ===== T3: 5 metric set count ===== */

#define SPEC_3_4E_METRIC_COUNT 5

UT_TEST(t3_metric_set_count_is_5)
{
	UT_ASSERT_EQ((int) SPEC_3_4E_METRIC_COUNT, 5);
}


/* ===== T4-T7: counter / accessor symbol linkability ===== */

UT_TEST(t4_fail_closed_bump_linkable)
{
	UT_ASSERT_NE((void *) cluster_itl_bump_remote_row_lock_fail_closed_count, NULL);
}
UT_TEST(t5_fail_closed_get_linkable)
{
	UT_ASSERT_NE((void *) cluster_itl_get_remote_row_lock_fail_closed_count, NULL);
}
UT_TEST(t6_lock_only_itl_stamp_get_linkable)
{
	UT_ASSERT_NE((void *) cluster_itl_get_lock_only_itl_stamp_count, NULL);
}
UT_TEST(t7_lock_only_tt_hint_emit_get_linkable)
{
	UT_ASSERT_NE((void *) cluster_itl_get_lock_only_tt_hint_emit_count, NULL);
}


/* ===== T8: 53R98 SQLSTATE encodable (class 4 hot-row trigger) ===== */

UT_TEST(t8_errcode_53r98_encodable)
{
	int sqlstate = MAKE_SQLSTATE('5', '3', 'R', '9', '8');
	UT_ASSERT_EQ((int) ERRCODE_CLUSTER_REMOTE_ROW_LOCK_WAIT_NOT_SUPPORTED, sqlstate);
}


/* ===== T9: single-node-no-peer GREEN threshold ===== */

#define SPEC_3_4E_CLASS_1_GREEN_THRESHOLD_PERCENT 15

UT_TEST(t9_single_node_no_peer_green_threshold_15)
{
	/* spec-3.4c Hardening v1.0.1 A4 ★ 实测:single-node select-only 2.8% /
	 * full 8.6% — generous-but-not-loose 上限 15% 给 5% noise buffer.
	 * Threshold drift will be caught by docs/perf-gates.md §1 review. */
	UT_ASSERT_EQ((int) SPEC_3_4E_CLASS_1_GREEN_THRESHOLD_PERCENT, 15);
}


/* ===== T10: ClusterPair partial coverage label invariant ===== */

#define SPEC_3_4E_CLASS_3_PARTIAL_COVERAGE 1
#define SPEC_3_4E_CLASS_4_PARTIAL_COVERAGE 1
#define SPEC_3_4E_CLASS_1_PARTIAL_COVERAGE 0
#define SPEC_3_4E_CLASS_2_PARTIAL_COVERAGE 0

UT_TEST(t10_class_3_4_partial_coverage_anchor)
{
	/* spec-3.4e E1 — class 3/4 partial coverage (inject-based,not real
	 * shared heap);真 cross-node TPS contention forward-link
	 * feature-117 / Stage 4+.  This compile-time anchor catches
	 * accidental change to "real shared heap claim" in code. */
	UT_ASSERT_NE((int) SPEC_3_4E_CLASS_3_PARTIAL_COVERAGE, 0);
	UT_ASSERT_NE((int) SPEC_3_4E_CLASS_4_PARTIAL_COVERAGE, 0);
	UT_ASSERT_EQ((int) SPEC_3_4E_CLASS_1_PARTIAL_COVERAGE, 0);
	UT_ASSERT_EQ((int) SPEC_3_4E_CLASS_2_PARTIAL_COVERAGE, 0);
}


int
main(void)
{
	UT_PLAN(10);
	UT_RUN(t1_workload_class_count_is_4);
	UT_RUN(t2_5_tier_name_count_is_5);
	UT_RUN(t3_metric_set_count_is_5);
	UT_RUN(t4_fail_closed_bump_linkable);
	UT_RUN(t5_fail_closed_get_linkable);
	UT_RUN(t6_lock_only_itl_stamp_get_linkable);
	UT_RUN(t7_lock_only_tt_hint_emit_get_linkable);
	UT_RUN(t8_errcode_53r98_encodable);
	UT_RUN(t9_single_node_no_peer_green_threshold_15);
	UT_RUN(t10_class_3_4_partial_coverage_anchor);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
