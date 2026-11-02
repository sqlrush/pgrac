/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_plan_original.c
 *    Original cold planner beside the existing recovery-frame fixture.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_plan_original.c
 * NOTES
 *    Only the two original frame getters use a private fixture namespace.
 *-------------------------------------------------------------------------
 */
#define cluster_cold_plan_input_frame_count_v1 create_owner_original_frame_count
#define cluster_cold_plan_input_frame_v1 create_owner_original_frame_at
#include "../../backend/cluster/cluster_cold_recovery_plan.c"
