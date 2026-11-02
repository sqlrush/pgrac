/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_side_original.c
 *    Original SIDE plan with four fixture provider names isolated.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_side_original.c
 *-------------------------------------------------------------------------
 */
#define cluster_wal_claim_v2_read create_side_original_claim_read
#include "../../backend/cluster/cluster_wal_claim.c"
#undef cluster_wal_claim_v2_read
#define rf_side_online_plan_recovery_sources_match_v1 create_side_original_sources
#define rf_side_online_plan_relmap_create_count_v1 create_side_original_count
#define rf_side_online_plan_relmap_create_input_matches_v1 create_side_original_input
#define rf_side_online_plan_relmap_create_matches_v1 create_side_original_record
#include "../../backend/cluster/cluster_side_online_plan.c"
