/*-------------------------------------------------------------------------
 *
 * cluster_unit_service_stubs.c
 *    Reject unprovided service-module boundaries in scoped native-loop tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/cluster_unit_service_stubs.c
 *
 * NOTES
 *    A role-specific test supplies strong definitions for every module that
 *    its actual role uses. Other roles' uncalled boundaries abort, never pass.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_control_request.h"
#include "cluster/cluster_xid_stripe_boot.h"

#define UNUSED_SIMPLE(name, type)                                                                  \
	ClusterNormalStopPollResult __attribute__((weak)) name(                                        \
		type *key pg_attribute_unused(), const char **reason pg_attribute_unused())                \
	{                                                                                              \
		abort();                                                                                   \
	}
#define UNUSED_DOMAIN(name)                                                                        \
	ClusterNormalStopPollResult __attribute__((weak)) name(                                        \
		const char **domain pg_attribute_unused(), uint64 *key pg_attribute_unused(),              \
		const char **reason pg_attribute_unused())                                                 \
	{                                                                                              \
		abort();                                                                                   \
	}

UNUSED_SIMPLE(cluster_grd_work_queue_normal_stop_poll, uint32)
UNUSED_SIMPLE(cluster_grd_outbound_normal_stop_poll, uint32)
UNUSED_SIMPLE(cluster_cr_server_normal_stop_poll, int)
UNUSED_SIMPLE(cluster_lms_native_probe_normal_stop_poll, int)
UNUSED_SIMPLE(cluster_gcs_block_normal_stop_local_poll, int)
UNUSED_SIMPLE(cluster_clean_leave_normal_stop_local_poll, int)
UNUSED_SIMPLE(cluster_clean_leave_service_poll, int)
UNUSED_SIMPLE(cluster_lmd_probe_normal_stop_poll, uint64)
UNUSED_SIMPLE(cluster_lmd_pending_normal_stop_poll, uint64)
UNUSED_SIMPLE(cluster_lmd_graph_normal_stop_poll, uint64)
UNUSED_SIMPLE(cluster_ko_normal_stop_poll, uint32)
UNUSED_DOMAIN(cluster_semantic_normal_stop_poll)
UNUSED_DOMAIN(cluster_scn_normal_stop_poll)
UNUSED_DOMAIN(cluster_reconfig_normal_stop_poll)
UNUSED_DOMAIN(cluster_node_remove_normal_stop_poll)
UNUSED_DOMAIN(cluster_fence_normal_stop_poll)
UNUSED_DOMAIN(cluster_write_fence_normal_stop_poll)
UNUSED_DOMAIN(cluster_recovery_normal_stop_poll)
UNUSED_DOMAIN(cluster_backup_normal_stop_poll)
UNUSED_DOMAIN(cluster_mrp_normal_stop_poll)
UNUSED_DOMAIN(cluster_gcs_dedup_normal_stop_poll)
UNUSED_DOMAIN(cluster_ges_dedup_normal_stop_poll)
UNUSED_DOMAIN(cluster_lmd_normal_stop_poll)
UNUSED_DOMAIN(cluster_sinval_normal_stop_poll)

ClusterNormalStopPollResult __attribute__((weak))
cluster_ic_normal_stop_poll(const char **domain pg_attribute_unused(),
							int *peer pg_attribute_unused(), uint32 *sequence pg_attribute_unused(),
							const char **reason pg_attribute_unused())
{
	abort();
}
ClusterNormalStopPollResult __attribute__((weak))
cluster_lms_outbound_normal_stop_poll(int *worker pg_attribute_unused(),
									  uint32 *position pg_attribute_unused(),
									  const char **reason pg_attribute_unused())
{
	abort();
}
ClusterNormalStopPollResult __attribute__((weak))
cluster_cf_normal_stop_poll(bool post_checkpoint pg_attribute_unused(),
							const char **reason pg_attribute_unused())
{
	abort();
}
bool __attribute__((weak))
cluster_control_request_empty(void)
{
	abort();
}

bool __attribute__((weak))
cluster_xid_wrap_barrier_observe(bool *pending pg_attribute_unused())
{
	abort();
}
