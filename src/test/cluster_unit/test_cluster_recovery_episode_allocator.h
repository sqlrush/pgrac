/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_allocator.h
 *     Original allocator observations for the complete recovery episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_allocator.h
 * NOTES
 *     Test-only composition; no native era, durability or completion grant.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_EPISODE_ALLOCATOR_H
#define TEST_CLUSTER_RECOVERY_EPISODE_ALLOCATOR_H
#include "test_cluster_xid_native_clog.h"
typedef struct TestEpisodeIssuedClogV1 {
	uint64 first_issued, queried_full_xid, next_full_xid;
	XidNativeClogState native;
} TestEpisodeIssuedClogV1;
typedef struct TestEpisodeAllocatorProbeV1 {
	uint64 next_full_xid;
	TransactionId published_xid;
	unsigned allocations;
	bool allocator_lock_held;
	XidNativeClogState native;
} TestEpisodeAllocatorProbeV1;
extern bool test_episode_allocator_produce(bool full, bool fail,
	TestEpisodeIssuedClogV1 *out, TestEpisodeAllocatorProbeV1 *probe);
#endif
