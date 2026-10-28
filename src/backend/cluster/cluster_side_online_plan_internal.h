/*-------------------------------------------------------------------------
 * cluster_side_online_plan_internal.h
 *    Private original native CLOG record execution state.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_side_online_plan_internal.h
 * NOTES
 *    Only the original production record executor writes this local state.
 *    It grants no source, lifetime, participant completion or release right.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_ONLINE_PLAN_INTERNAL_H
#define CLUSTER_SIDE_ONLINE_PLAN_INTERNAL_H

#include "cluster/cluster_side_online_plan.h"

typedef struct SideNativeClogReplay {
	bool attempted;
	bool applied;
} SideNativeClogReplay;

/* Exact original physical RECOVERY operation only; NULL is not a grant. */
extern SideNativeClogReplay *side_plan_clog_replay_state(
	const RfSideOnlinePlanV1 *plan, uint32 index, const RfPageOnlineRecordIdentityV1 *identity);

#endif
