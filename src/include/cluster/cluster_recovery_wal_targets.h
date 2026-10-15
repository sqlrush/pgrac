/*-------------------------------------------------------------------------
 * cluster_recovery_wal_targets.h
 *    Retained WAL file targets in the physical recovery root union.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_WAL_TARGETS_H
#define CLUSTER_RECOVERY_WAL_TARGETS_H

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_recovery_root_set.h"

typedef struct ClusterRecoveryWalTargetsDiag {
	RfPageProofDetailV1 route_detail;
	ClusterRecoveryNativeResult native_result;
	ClusterRecoveryRootsResult roots_result;
	uint32 page_targets;
	uint32 rebuildable_targets;
	uint32 structural_effects;
	uint32 native_effects;
	uint32 other_native_effects;
	uint32 unowned_records;
} ClusterRecoveryWalTargetsDiag;

/* One original cold-reader record, before the physical root scan begins.
 * The original full source/cut/ROOT validation remains with the input owner.
 * Reuse the existing closed route, page preflight and native/SPACE parsers.
 * Include FSM and missing structural forks; never apply or discharge them.
 *
 * On success its supported PAGE/native-storage/SPACE targets are indexed.
 * An unowned record may also contain targets not interpreted here. Its
 * original owner MUST enumerate those before the full graph can be sealed.
 * The complete record and its non-file effects remain graph obligations:
 * unowned_records/other_native_effects are not ignorable no-ops.
 * No whole-graph, replay, ROOT, OPEN, PI or GC authority is created here.
 * This synchronous visitor handles one finite record; memory_budget bounds
 * its owned native payload, not elapsed time or the surrounding root set.
 * Refusal invalidates roots, including earlier provisional additions. */
extern bool cluster_recovery_wal_targets_collect(
	ClusterRecoveryRootSet *roots, XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, size_t memory_budget, ClusterRecoveryWalTargetsDiag *diag);

#endif
#endif
