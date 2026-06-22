/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_stats.c
 *	  Standalone unit tests for the spec-5.6 Dc4 CF shared-authority
 *	  observability counters: inc / read / per-counter independence / bounds
 *	  guard / NULL-safety before the shmem region is initialised.
 *
 *	  Links cluster_cf_stats.o; ShmemInitStruct + region register are stubbed
 *	  locally (a static atomic array backs the region).  The counters' real
 *	  call sites (CF X/S acquire, fail-closed, single-node authority, .bak
 *	  fallback) are exercised by the cluster_tap CF harness.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Dc4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_stats.h"
#include "port/atomics.h"

#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"


/* ============================================================
 * PG runtime stubs.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * ShmemInitStruct stub -- a static pg_atomic_uint64 array is naturally 8-byte
 * aligned and matches the ClusterCfStatsSharedState layout (counters[]), so it
 * backs the region without a force-align union.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster cf stats") == 0) {
		/* counters[] (uint64) + the join_readonly uint32 flag; one extra
		 * pg_atomic_uint64 slot covers the uint32 + padding, 8-byte aligned. */
		static pg_atomic_uint64 cf_buf[CLUSTER_CF_COUNTER_COUNT + 1];
		static bool cf_initialized = false;

		Assert(size <= sizeof(cf_buf)); /* catch shmem layout growth */
		*foundPtr = cf_initialized;
		cf_initialized = true;
		return cf_buf;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * U -- CF observability counters: NULL-safe before init.
 * ============================================================ */
UT_TEST(test_cf_counters_null_safe_before_init)
{
	/* Before cluster_cf_stats_shmem_init(), the state pointer is NULL; inc is
	 * a no-op and read returns 0 rather than dereferencing NULL. */
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 0);
}


/* ============================================================
 * U -- CF observability counters: inc / read / independence / bounds.
 * ============================================================ */
UT_TEST(test_cf_counters_inc_read_bounds)
{
	cluster_cf_stats_shmem_init();

	/* All counters start at zero. */
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 0);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK), 0);

	/* Independent accumulation per counter. */
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_S_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 2);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_S_ACQUIRE), 1);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_FAILCLOSED), 1);
	/* Untouched counters stay zero. */
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_SINGLE_NODE_AUTHORITY), 0);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK), 0);

	/* Out-of-range index is a no-op / zero (bounds guard). */
	cluster_cf_counter_inc(CLUSTER_CF_COUNTER_COUNT);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_COUNTER_COUNT), 0);
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(2);

	UT_RUN(test_cf_counters_null_safe_before_init);
	UT_RUN(test_cf_counters_inc_read_bounds);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
