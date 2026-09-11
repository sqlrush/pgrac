/*-------------------------------------------------------------------------
 * test_cluster_r4_production_reachability.c
 *    Compile the production HOT hook without the unit-only override.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_production_reachability.c
 *
 * The fetch wrapper, not this hook, still owns semantic admission.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#undef printf
#include "unit_test.h"

#ifdef USE_CLUSTER_UNIT
#error "This test must compile the production branch"
#endif

#include "test_cluster_r4_hot_reachability.inc"

UT_DEFINE_GLOBALS();

UT_TEST(production_hot_can_reach_the_existing_admission_owner)
{
	UT_ASSERT(heap_hot_r4_target_reachable());
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(production_hot_can_reach_the_existing_admission_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
