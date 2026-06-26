/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_admit_stat.c
 *	  pgrac spec-5.52 D9 — cluster_unit tests for the independent admission
 *	  reason counter shmem region (cluster_cr_admit_stat.c): init-to-zero,
 *	  per-reason atomic bump, and out-of-range guard.
 *
 *	  Links cluster_cr_admit_stat.o standalone; stubs ShmemInitStruct
 *	  (malloc-backed) and cluster_shmem_register_region (no-op).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.52-cr-cache-admission-policy.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_admit_stat.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_admit.h"
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
	/* link-only stub; the test calls cluster_cr_admit_shmem_init() directly */
}

/* ===== tests ===== */

UT_TEST(test_stat_init_zero)
{
	int i;

	cluster_cr_admit_shmem_init();
	for (i = 0; i < CR_ADMIT_REASON__COUNT; i++)
		UT_ASSERT_EQ(cluster_cr_admit_stat_count((ClusterCRAdmitReason)i), 0);
}

UT_TEST(test_stat_bump_counts)
{
	cluster_cr_admit_shmem_init();

	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_ADMITTED);
	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_ADMITTED);
	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_REJECT_BULK);
	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_REJECT_BULK);
	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_REJECT_BULK);
	cluster_cr_admit_stat_bump(CR_ADMIT_REASON_REJECT_VOLATILE);

	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_ADMITTED), 2);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_BULK), 3);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_VOLATILE), 1);
	/* untouched reasons stay zero (per-reason isolation) */
	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_PARALLEL), 0);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_RELCAP), 0);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_NO_ADMIT), 0);
}

UT_TEST(test_stat_bump_invalid_reason_noop)
{
	cluster_cr_admit_shmem_init();

	/* out-of-range bumps must not crash or corrupt neighboring counters */
	cluster_cr_admit_stat_bump((ClusterCRAdmitReason)-1);
	cluster_cr_admit_stat_bump((ClusterCRAdmitReason)CR_ADMIT_REASON__COUNT);
	cluster_cr_admit_stat_bump((ClusterCRAdmitReason)(CR_ADMIT_REASON__COUNT + 5));

	UT_ASSERT_EQ(cluster_cr_admit_stat_count(CR_ADMIT_REASON_ADMITTED), 0);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count((ClusterCRAdmitReason)-1), 0);
	UT_ASSERT_EQ(cluster_cr_admit_stat_count((ClusterCRAdmitReason)CR_ADMIT_REASON__COUNT), 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_stat_init_zero);
	UT_RUN(test_stat_bump_counts);
	UT_RUN(test_stat_bump_invalid_reason_noop);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
