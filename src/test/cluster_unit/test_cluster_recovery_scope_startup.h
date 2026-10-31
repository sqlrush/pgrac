/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_startup.h
 *     Test access to the original Startup owner under an existing scope.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_startup.h
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_SCOPE_STARTUP_H
#define TEST_CLUSTER_RECOVERY_SCOPE_STARTUP_H

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_wal_inputs.h"

extern bool test_scope_startup_attach(ClusterColdTypedV1 *typed,
	const ClusterRecoveryRelmapSourceV1 *sources);
extern bool test_scope_startup_attach_cold(ClusterColdTypedV1 *typed,
	const ClusterWalInputV1 *sources);
extern ClusterColdDetailV1 test_scope_startup_prepare_plan(ClusterColdTypedV1 *typed,
	const ClusterWalInputV1 *sources, Size budget);
extern bool test_scope_startup_sources_current(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence);
extern bool test_scope_startup_origin(uint16 index, uint16 *thread,
	ClusterControlRootSnapshot *root, ClusterControlRootReadToken *token);
extern void test_scope_startup_detach(ClusterColdTypedV1 *typed);

#endif
