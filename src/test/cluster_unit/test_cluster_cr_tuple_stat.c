/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_tuple_stat.c
 *	  pgrac spec-5.54 D5 — cluster_unit tests for the independent tuple-level
 *	  CR fast-path outcome counter shmem region (cluster_cr_tuple_stat.c):
 *	  init-to-zero, per-outcome atomic bump, and out-of-range guard.
 *
 *	  Links cluster_cr_tuple_stat.o standalone; stubs ShmemInitStruct
 *	  (malloc-backed) and cluster_shmem_register_region (no-op).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_tuple_stat.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_tuple.h"
#include "cluster/cluster_shmem.h"
#include "storage/shmem.h"

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

/* ---- stubs ---- *
 * ShmemInitStruct returns a fresh malloc each call (found=false) for isolation;
 * the region registration is a no-op (the test drives init directly). */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	void *p = malloc(size);

	if (p != NULL)
		memset(p, 0, size);
	if (foundPtr != NULL)
		*foundPtr = false;
	return p;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{
	/* link-only stub; the test calls cluster_cr_tuple_stat_shmem_init() directly */
}

/* ===== tests ===== */

UT_TEST(test_tuple_stat_init_zero)
{
	int i;

	cluster_cr_tuple_stat_shmem_init();
	for (i = 0; i < CR_TUPLE_OUTCOME__COUNT; i++)
		UT_ASSERT_EQ(cluster_cr_tuple_stat_count((ClusterCRTupleOutcome)i), 0);
}

UT_TEST(test_tuple_stat_bump_counts)
{
	cluster_cr_tuple_stat_shmem_init();

	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_VERDICT);
	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_VERDICT);
	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN);
	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN);
	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN);
	cluster_cr_tuple_stat_bump(CR_TUPLE_OUTCOME_FALLBACK_IDENTITY);

	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_VERDICT), 2);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN), 3);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_IDENTITY), 1);
	/* untouched outcomes stay zero (per-outcome isolation) */
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_REMOTE), 0);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_CROSS_BLOCK), 0);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN), 0);
}

UT_TEST(test_tuple_stat_bump_invalid_outcome_noop)
{
	cluster_cr_tuple_stat_shmem_init();

	/* out-of-range bumps must not crash or corrupt neighboring counters */
	cluster_cr_tuple_stat_bump((ClusterCRTupleOutcome)-1);
	cluster_cr_tuple_stat_bump((ClusterCRTupleOutcome)CR_TUPLE_OUTCOME__COUNT);
	cluster_cr_tuple_stat_bump((ClusterCRTupleOutcome)(CR_TUPLE_OUTCOME__COUNT + 7));

	UT_ASSERT_EQ(cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_VERDICT), 0);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count((ClusterCRTupleOutcome)-1), 0);
	UT_ASSERT_EQ(cluster_cr_tuple_stat_count((ClusterCRTupleOutcome)CR_TUPLE_OUTCOME__COUNT), 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_tuple_stat_init_zero);
	UT_RUN(test_tuple_stat_bump_counts);
	UT_RUN(test_tuple_stat_bump_invalid_outcome_noop);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
