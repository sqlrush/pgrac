/* Unbound role modules are explicit negative host inputs, never success.
 * Existing role bindings and actual service/actor implementations are retained.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define COLLECT_REFUSED_SIMPLE(name, type) \
ClusterNormalStopPollResult name(type *key pg_attribute_unused(), const char **reason) { \
	*reason = "UNINITIALIZED_COLLECTION_FIXTURE_OWNER"; return CLUSTER_NORMAL_STOP_INVALID; }
#define COLLECT_REFUSED_DOMAIN(name) \
ClusterNormalStopPollResult name(const char **domain pg_attribute_unused(), \
	uint64 *key pg_attribute_unused(), const char **reason) { \
	*reason = "UNINITIALIZED_COLLECTION_FIXTURE_OWNER"; return CLUSTER_NORMAL_STOP_INVALID; }
#define COLLECT_REFUSED_REASON(name) \
ClusterNormalStopPollResult name(const char **reason) { \
	*reason = "UNINITIALIZED_COLLECTION_FIXTURE_OWNER"; return CLUSTER_NORMAL_STOP_INVALID; }
COLLECT_REFUSED_DOMAIN(cluster_backup_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_fence_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_gcs_dedup_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_ges_dedup_normal_stop_poll)
COLLECT_REFUSED_SIMPLE(cluster_grd_outbound_normal_stop_poll, uint32)
COLLECT_REFUSED_SIMPLE(cluster_grd_work_queue_normal_stop_poll, uint32)
COLLECT_REFUSED_SIMPLE(cluster_lmd_graph_normal_stop_poll, uint64)
COLLECT_REFUSED_DOMAIN(cluster_lmd_normal_stop_poll)
COLLECT_REFUSED_SIMPLE(cluster_lmd_pending_normal_stop_poll, uint64)
COLLECT_REFUSED_SIMPLE(cluster_lmd_probe_normal_stop_poll, uint64)
COLLECT_REFUSED_DOMAIN(cluster_mrp_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_node_remove_normal_stop_poll)
COLLECT_REFUSED_REASON(cluster_pi_data_normal_stop_poll_v1)
COLLECT_REFUSED_REASON(cluster_pi_writeback_normal_stop_poll_v1)
COLLECT_REFUSED_DOMAIN(cluster_reconfig_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_recovery_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_scn_normal_stop_poll)
COLLECT_REFUSED_DOMAIN(cluster_semantic_normal_stop_poll)
COLLECT_REFUSED_REASON(cluster_wal_cut_normal_stop_poll_v1)
COLLECT_REFUSED_DOMAIN(cluster_write_fence_normal_stop_poll)
/* This host input is explicitly a retained CONTROL request, not empty. */
bool cluster_control_request_empty(void) { return false; }
#undef COLLECT_REFUSED_SIMPLE
#undef COLLECT_REFUSED_DOMAIN
#undef COLLECT_REFUSED_REASON
