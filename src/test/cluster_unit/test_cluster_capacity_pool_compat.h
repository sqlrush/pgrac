/*-------------------------------------------------------------------------
 *
 * test_cluster_capacity_pool_compat.h
 *    Capacity observation interface for behavioral tests on older product C.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_capacity_pool_compat.h
 *
 * NOTES
 *    This pgrac-original test header supplies only capacity interface types
 *    when the product API is absent. It defines no runtime capacity owner.
 *
 *-------------------------------------------------------------------------
 */
/* Frozen-interface compatibility for behavioral RED on older product C.
 * This test-only fallback never supplies a positive capacity observation. */
#ifndef CLUSTER_CAPACITY_POOL_TEST_COMPAT_H
#define CLUSTER_CAPACITY_POOL_TEST_COMPAT_H
#ifndef CLUSTER_CTRC_CAPACITY_POOL_V1
#define CLUSTER_CAPACITY_POOL_TEST_API_ABSENT 1
typedef enum ClusterCapacityReasonV1 {
	CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER = 0,
	CLUSTER_CAPACITY_SUPPLY_RECHECK,
	CLUSTER_CAPACITY_OWNER_PROGRESS_POSSIBLE,
	CLUSTER_CAPACITY_PROTECTED_HISTORY,
	CLUSTER_CAPACITY_ACTIVE_ONLY
} ClusterCapacityReasonV1;
typedef enum ClusterCapacityScopeV1 {
	CLUSTER_CAPACITY_SCOPE_SLOT = 0,
	CLUSTER_CAPACITY_SCOPE_SEGMENT
} ClusterCapacityScopeV1;
#define CLUSTER_CAPACITY_UNPROVEN_IDENTITY UINT32_C(0x01)
#define CLUSTER_CAPACITY_UNPROVEN_PHASE UINT32_C(0x02)
#define CLUSTER_CAPACITY_UNPROVEN_REQUIRED_ACK UINT32_C(0x04)
#define CLUSTER_CAPACITY_UNPROVEN_CANONICAL UINT32_C(0x08)
#define CLUSTER_CAPACITY_UNPROVEN_OWNER UINT32_C(0x10)
typedef struct ClusterCapacityObservationV1 {
	ClusterCapacityReasonV1 reason;
	ClusterCapacityScopeV1 scope;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginState phase;
	unsigned owner_worker;
	uint64 owner_generation;
	uint64 coordinator_generation;
	uint64 observed_change_sequence;
	uint64 pool_positions;
	uint32 unproven_reasons;
} ClusterCapacityObservationV1;
#endif
#endif
