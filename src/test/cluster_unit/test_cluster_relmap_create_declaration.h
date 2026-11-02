/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_declaration.h
 *    Synthetic independent registrations for pure planner fixtures.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_declaration.h
 * NOTES
 *    Production declarations come from Start control registration, not WAL.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RELMAP_CREATE_DECLARATION_H
#define TEST_CLUSTER_RELMAP_CREATE_DECLARATION_H
static ClusterColdDetailV1
test_create_predeclare(ClusterColdPlanV1 *plan,
	const ClusterColdRelmapCreateInputV1 *inputs, uint32 count)
{
	ClusterColdRelmapCreateExpectedV1 *expected = calloc(count, sizeof(*expected));
	ClusterColdDetailV1 detail;
	if (expected == NULL) return CLUSTER_COLD_OOM;
	for (uint32 i = 0; i < count; ++i) {
		expected[i].expected = inputs[i].creation.identity;
		memcpy(expected[i].batch_uuid, inputs[i].creation.batch_uuid, 16);
	}
	detail = cluster_cold_plan_relmap_create_predeclare_v1(plan, expected, count);
	free(expected);
	return detail;
}
#endif
