/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_side_owner.h
 *     Original saved SIDE plan execution in the input episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_side_owner.h
 * NOTES
 *     Original episode/plans/readers execute; cold authority, native mutation
 *     and projection leaves remain controlled. No lifetime or COMPLETE grant.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_EPISODE_SIDE_OWNER_H
#define TEST_CLUSTER_RECOVERY_EPISODE_SIDE_OWNER_H
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_wal_inputs.h"
typedef struct EpisodeSideOwnerStats
{
	unsigned scopes, native_applies, native_verifies, projections;
} EpisodeSideOwnerStats;
extern RfPageProofDetailV1
test_episode_side_owner_apply(const RfSideOnlinePlanV1 *plan, uint32 index,
							  const ClusterRecoveryRelmapSourceV1 *source, unsigned fault,
							  EpisodeSideOwnerStats *stats);
#endif
