/*-------------------------------------------------------------------------
 *
 * test_cluster_sequence.c
 *	  Pure-layer contract tests for the SQ sequence lock allocation math
 *	  and resource-id encoding.
 *
 *	  Exercises the pure layer of cluster_sequence.c standalone (no PG
 *	  backend): SQ resource-id encoding (cluster_sq_resid_encode), the
 *	  direction-aware authority batch allocator (cluster_sq_alloc_segment),
 *	  and the instance-cache slicing predicate (cluster_sq_cache_has_value).
 *	  The shmem instance cache, the GES(X) enqueue refill protocol, the
 *	  cross-node boundary writeback, and the failover fail-closed paths run
 *	  backend-side (cluster_tap t/<NNN>_sq_sequence.pl), not here.
 *
 *	  Spec: spec-5.4-sq-sequence-lock.md  (U1/U2/U3/U4/U7/U10)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_sequence.c
 *
 * NOTES
 *	  Pure layer only; links cluster_sequence.o with no PG-backend stubs
 *	  (apart from the Assert ExceptionalCondition hook below).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_sequence.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Assert() in cluster_sequence.c expands to ExceptionalCondition() in
 * --enable-cassert builds; provide the stub so the pure layer links
 * standalone.  Valid-input tests never trigger it.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ---- helpers -------------------------------------------------------- */

static bool
resid_eq(const ClusterResId *a, const ClusterResId *b)
{
	return memcmp(a, b, sizeof(ClusterResId)) == 0;
}

/* ---- U2: SQ resid encoding (distinct + cross-DB isolation) ---------- */

UT_TEST(test_resid_encode_deterministic_and_distinct)
{
	ClusterResId a, b, c;

	/* same (db, seq, gen) -> identical resid */
	cluster_sq_resid_encode(100, 200, 1, &a);
	cluster_sq_resid_encode(100, 200, 1, &b);
	UT_ASSERT(resid_eq(&a, &b));

	/* fields land where the SQ namespace expects them */
	UT_ASSERT_EQ(a.field1, 100u); /* database_oid */
	UT_ASSERT_EQ(a.field2, 200u); /* sequence_oid */
	UT_ASSERT_EQ(a.field3, 1u);	  /* generation (relfilenode) */
	UT_ASSERT_EQ(a.type, CLUSTER_SQ_RESID_TYPE);

	/* SQ namespace must not collide with the PG relation-lock namespace */
	UT_ASSERT_NE(a.type, 0 /* LOCKTAG_RELATION */);

	/* different sequence oid -> different resid */
	cluster_sq_resid_encode(100, 201, 1, &c);
	UT_ASSERT(!resid_eq(&a, &c));
}

UT_TEST(test_resid_encode_cross_db_isolation)
{
	ClusterResId db1, db2;

	/* same sequence oid, different database -> distinct resources */
	cluster_sq_resid_encode(100, 200, 1, &db1);
	cluster_sq_resid_encode(101, 200, 1, &db2);
	UT_ASSERT(!resid_eq(&db1, &db2));
}

/* ---- U3: generation ABA (drop/recreate) ---------------------------- */

UT_TEST(test_resid_encode_generation_aba)
{
	ClusterResId gen1, gen2;

	/* same (db, seq) but a new relfilenode after DROP+CREATE -> different
	 * resource id, so a stale in-flight segment can never be reused. */
	cluster_sq_resid_encode(100, 200, 1, &gen1);
	cluster_sq_resid_encode(100, 200, 2, &gen2);
	UT_ASSERT(!resid_eq(&gen1, &gen2));
}

/* ---- U1: instance-cache slicing predicate (direction-aware) -------- */

UT_TEST(test_cache_has_value_ascending)
{
	/* [next=5 .. end=9] inc +1 -> values remain */
	UT_ASSERT(cluster_sq_cache_has_value(5, 9, 1));
	/* next == end -> the last value is still available */
	UT_ASSERT(cluster_sq_cache_has_value(9, 9, 1));
	/* next past end -> empty, needs refill */
	UT_ASSERT(!cluster_sq_cache_has_value(10, 9, 1));
}

UT_TEST(test_cache_has_value_descending)
{
	/* [next=100 .. end=91] inc -1 -> values remain */
	UT_ASSERT(cluster_sq_cache_has_value(100, 91, -1));
	UT_ASSERT(cluster_sq_cache_has_value(91, 91, -1));
	/* next below end -> empty */
	UT_ASSERT(!cluster_sq_cache_has_value(90, 91, -1));
}

/* ---- U4: authority batch allocation, monotone + non-overlapping ---- */

UT_TEST(test_alloc_first_segment_is_called_false_starts_at_start)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* CREATE ... START 1 INCREMENT 1 (is_called=false) -> first value is
	 * START (1) itself, then 1..10 for a 10-count segment. */
	st = cluster_sq_alloc_segment(1 /*boundary=START*/, false /*is_called*/, 1 /*inc*/, 1 /*min*/,
								  INT64_MAX /*max*/, 10 /*want*/, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_OK);
	UT_ASSERT_EQ(start, 1);
	UT_ASSERT_EQ(end, 10);
	UT_ASSERT_EQ(cnt, 10);
	UT_ASSERT_EQ(newb, 10); /* new allocation boundary = last value */
}

UT_TEST(test_alloc_consecutive_segments_monotone_nonoverlap)
{
	int64 s1 = 0, e1 = 0, c1 = 0, b1 = 0;
	int64 s2 = 0, e2 = 0, c2 = 0, b2 = 0;
	ClusterSqAllocStatus st;

	/* first refill from START (is_called=false) */
	st = cluster_sq_alloc_segment(1, false, 1, 1, INT64_MAX, 10, &s1, &e1, &c1, &b1);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_OK);
	UT_ASSERT_EQ(b1, 10);

	/* second refill continues from the durable boundary (is_called=true) */
	st = cluster_sq_alloc_segment(b1, true, 1, 1, INT64_MAX, 10, &s2, &e2, &c2, &b2);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_OK);
	UT_ASSERT_EQ(s2, 11); /* strictly past the first segment, no overlap */
	UT_ASSERT_EQ(e2, 20);
	UT_ASSERT_EQ(b2, 20);
	UT_ASSERT(e1 < s2); /* monotone, non-overlapping */
}

/* ---- U7: allocation semantics (increment, direction, min/max) ------ */

UT_TEST(test_alloc_increment_ten)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* boundary=0 is_called=true inc=10 -> 10,20,...,50 for want=5 */
	st = cluster_sq_alloc_segment(0, true, 10, 1, INT64_MAX, 5, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_OK);
	UT_ASSERT_EQ(start, 10);
	UT_ASSERT_EQ(end, 50);
	UT_ASSERT_EQ(cnt, 5);
	UT_ASSERT_EQ(newb, 50);
}

UT_TEST(test_alloc_descending)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* START 100 INCREMENT -1 (is_called=false) CACHE 10 -> 100..91, the new
	 * boundary is the smallest allocated value (91), not the largest. */
	st = cluster_sq_alloc_segment(100, false, -1, INT64_MIN, 100, 10, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_OK);
	UT_ASSERT_EQ(start, 100);
	UT_ASSERT_EQ(end, 91);
	UT_ASSERT_EQ(cnt, 10);
	UT_ASSERT_EQ(newb, 91); /* direction-aware boundary = min allocated */
}

UT_TEST(test_alloc_truncated_at_maxvalue)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* boundary=95 is_called=true inc=1 maxv=100 want=10 -> only 96..100 (5) */
	st = cluster_sq_alloc_segment(95, true, 1, 1, 100, 10, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_TRUNCATED);
	UT_ASSERT_EQ(start, 96);
	UT_ASSERT_EQ(end, 100);
	UT_ASSERT_EQ(cnt, 5);
	UT_ASSERT_EQ(newb, 100);
}

UT_TEST(test_alloc_exhausted_at_maxvalue)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* boundary already at maxv, is_called=true -> nothing left to grant */
	st = cluster_sq_alloc_segment(100, true, 1, 1, 100, 10, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_EXHAUSTED);
	UT_ASSERT_EQ(cnt, 0);
}

UT_TEST(test_alloc_truncated_at_minvalue_descending)
{
	int64 start = 0, end = 0, cnt = 0, newb = 0;
	ClusterSqAllocStatus st;

	/* descending toward minv=1: boundary=5 is_called=true inc=-1 want=10 ->
	 * only 4,3,2,1 (4 values). */
	st = cluster_sq_alloc_segment(5, true, -1, 1, 100, 10, &start, &end, &cnt, &newb);
	UT_ASSERT_EQ(st, CLUSTER_SQ_ALLOC_TRUNCATED);
	UT_ASSERT_EQ(start, 4);
	UT_ASSERT_EQ(end, 1);
	UT_ASSERT_EQ(cnt, 4);
	UT_ASSERT_EQ(newb, 1);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_resid_encode_deterministic_and_distinct);
	UT_RUN(test_resid_encode_cross_db_isolation);
	UT_RUN(test_resid_encode_generation_aba);
	UT_RUN(test_cache_has_value_ascending);
	UT_RUN(test_cache_has_value_descending);
	UT_RUN(test_alloc_first_segment_is_called_false_starts_at_start);
	UT_RUN(test_alloc_consecutive_segments_monotone_nonoverlap);
	UT_RUN(test_alloc_increment_ten);
	UT_RUN(test_alloc_descending);
	UT_RUN(test_alloc_truncated_at_maxvalue);
	UT_RUN(test_alloc_exhausted_at_maxvalue);
	UT_RUN(test_alloc_truncated_at_minvalue_descending);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
