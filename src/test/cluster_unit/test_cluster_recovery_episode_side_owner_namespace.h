/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_side_owner_namespace.h
 *     Isolate complete original SIDE fixture providers and owner.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_side_owner_namespace.h
 * NOTES
 *     Original episode/plans/readers execute; cold authority, native mutation
 *     and projection leaves remain controlled. No lifetime or COMPLETE grant.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_EPISODE_SIDE_OWNER_NAMESPACE_H
#define TEST_CLUSTER_RECOVERY_EPISODE_SIDE_OWNER_NAMESPACE_H
#define ExceptionalCondition d3_episode_side_ExceptionalCondition
#define GetSystemIdentifier d3_episode_side_GetSystemIdentifier
#define cluster_node_id d3_episode_side_cluster_node_id
#define main d3_episode_side_main
#define pg_re_throw d3_episode_side_pg_re_throw
#define rf_side_online_production_apply_record_v1                                                  \
	d3_episode_side_rf_side_online_production_apply_record_v1
#define rf_side_online_production_apply_v1 d3_episode_side_rf_side_online_production_apply_v1
#define rf_side_online_production_bind_undo_v1                                                     \
	d3_episode_side_rf_side_online_production_bind_undo_v1
#define rf_side_online_production_owner_init_v1                                                    \
	d3_episode_side_rf_side_online_production_owner_init_v1
#define rf_side_online_production_preflight_v1                                                     \
	d3_episode_side_rf_side_online_production_preflight_v1
#define scn_time_cmp d3_episode_side_scn_time_cmp
#endif
