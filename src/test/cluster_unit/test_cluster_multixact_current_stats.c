/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact_current_stats.c
 *	  Standalone tests for current-DML MultiXact shared observability.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_multixact_current_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

ProcessingMode Mode = NormalProcessing;
bool cluster_enabled = true;
int cluster_node_id = 1;

static TimestampTz test_now = UINT64CONST(946684800000000);

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return test_now;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	static union {
		/* Enforces alignment of the byte-backed shared-memory fixture. */
		/* cppcheck-suppress unusedStructMember */
		uint64 align;
		unsigned char bytes[4096];
	} shmem;
	static bool initialized = false;

	UT_ASSERT_STR_EQ(name, "pgrac current multixact stats");
	UT_ASSERT(size <= sizeof(shmem.bytes));
	*found = initialized;
	initialized = true;
	return shmem.bytes;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

UT_TEST(test_current_mx_stats_null_safe)
{
	cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_LOCAL);
	cluster_multixact_current_stats_record_restarts(3);

	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_DESCRIBE_LOCAL), 0);
	UT_ASSERT_EQ(cluster_multixact_current_stats_since(), 0);
}

UT_TEST(test_current_mx_restart_bucket_boundaries)
{
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(0), CMX_STAT_RESTART_BUCKET_0);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(1), CMX_STAT_RESTART_BUCKET_1);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(2), CMX_STAT_RESTART_BUCKET_2_3);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(3), CMX_STAT_RESTART_BUCKET_2_3);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(4), CMX_STAT_RESTART_BUCKET_4_7);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(7), CMX_STAT_RESTART_BUCKET_4_7);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(8), CMX_STAT_RESTART_BUCKET_8_PLUS);
	UT_ASSERT_EQ(cluster_multixact_current_restart_bucket(UINT32_MAX),
				 CMX_STAT_RESTART_BUCKET_8_PLUS);
}

UT_TEST(test_current_mx_stats_restart_histogram_and_max)
{
	cluster_multixact_current_stats_shmem_init();

	UT_ASSERT_EQ(cluster_multixact_current_stats_since(), test_now);
	cluster_multixact_current_stats_record_restarts(0);
	cluster_multixact_current_stats_record_restarts(3);
	cluster_multixact_current_stats_record_restarts(2);
	cluster_multixact_current_stats_record_restarts(9);
	cluster_multixact_current_stats_record_restarts(4);

	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_0), 1);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_1), 0);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_2_3), 2);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_4_7), 1);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_8_PLUS), 1);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_MAX), 9);

	cluster_multixact_current_stats_record_restarts(1);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_BUCKET_1), 1);
	UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RESTART_MAX), 9);
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(3);

	UT_RUN(test_current_mx_stats_null_safe);
	UT_RUN(test_current_mx_restart_bucket_boundaries);
	UT_RUN(test_current_mx_stats_restart_histogram_and_max);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
