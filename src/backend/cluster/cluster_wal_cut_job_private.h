/* Allocation lifetime only; original cut poll retains every authority gate.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_CUT_JOB_PRIVATE_H
#define CLUSTER_WAL_CUT_JOB_PRIVATE_H

#include "cluster/cluster_wal_cut.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

extern ClusterControlRootResult cluster_wal_cut_begin_in_context_v1(
	const ClusterWalSourceRef *source, MemoryContext context, ClusterWalCutV1 **out);
extern bool cluster_wal_cut_context_binding_v1(const ClusterWalCutV1 *cut,
	MemoryContext context, ResourceOwner owner, int32 pid, const ClusterWalSourceRef *source);
extern bool cluster_wal_cut_cancel_in_context_v1(ClusterWalCutV1 *cut,
	MemoryContext context, ResourceOwner owner, int32 pid, const ClusterWalSourceRef *source);
extern bool cluster_wal_cut_release_in_context_v1(ClusterWalCutV1 **cut,
	MemoryContext context, ResourceOwner owner, int32 pid, const ClusterWalSourceRef *source);

#endif
