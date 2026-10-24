/*-------------------------------------------------------------------------
 * cluster_recovery_xact_join.h
 *   Retained transaction effects and original-origin physical SLRU slots.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_XACT_JOIN_H
#define CLUSTER_RECOVERY_XACT_JOIN_H

#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_recovery_root_set.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryXactJoin ClusterRecoveryXactJoin;

typedef enum ClusterRecoveryXactJoinResult {
	CLUSTER_RECOVERY_XACT_JOIN_MORE = 0,
	CLUSTER_RECOVERY_XACT_JOIN_ENTRY,
	CLUSTER_RECOVERY_XACT_JOIN_DRAINED,
	CLUSTER_RECOVERY_XACT_JOIN_INVALID,
	CLUSTER_RECOVERY_XACT_JOIN_CHANGED,
	CLUSTER_RECOVERY_XACT_JOIN_IO,
	CLUSTER_RECOVERY_XACT_JOIN_BASE_REQUIRED,
	CLUSTER_RECOVERY_XACT_JOIN_CAPACITY
} ClusterRecoveryXactJoinResult;

typedef struct ClusterRecoveryXactJoinEntry {
	const ClusterRecoveryNativeRecord *record;
	TransactionId xid;
	uint32 ordinal; /* child ordinal; zero for the separately marked top */
	uint32 root_index;
	uint64 page_number;
	uint32 expected_value, observed_value; /* raw CLOG status or SUBTRANS parent */
	bool is_top, is_parent;
	bool equal; /* raw comparison, not era/status/parent-chain authority */
} ClusterRecoveryXactJoinEntry;

/* Borrow an owned terminal/assignment scan and OBSERVED roots from the
 * original isolated cold scope. Both owners outlive the join. The fixed
 * budget includes the cursor and two pages, not its owners. No local SLRU
 * fallback, current epoch guess, apply or visibility lookup is permitted. */
extern ClusterRecoveryXactJoinResult
cluster_recovery_xact_join_begin(ClusterRecoveryRootSet *roots, ClusterRecoveryNativeScan *scan,
								 size_t memory_budget, ClusterRecoveryXactJoin **out);

/* At most128 root candidates, one page read/reread or one slot per call.
 * COMMIT/ABORT compare top and all children; ASSIGNMENT only children,
 * against the native redo top parent, not each original immediate parent.
 * All physical values/mismatches remain explicit. Failure invalidates roots;
 * record/root invalidation also revokes DRAINED. Entries borrow the original
 * owned record. Final whole TT/ROOT/WAL rereads remain the caller's duty.
 * DRAINED does not authorize terminal, parent, replay, OPEN or GC. */
extern ClusterRecoveryXactJoinResult
cluster_recovery_xact_join_next(ClusterRecoveryXactJoin *join, ClusterRecoveryXactJoinEntry *out);
extern void cluster_recovery_xact_join_destroy(ClusterRecoveryXactJoin *join);
#endif
#endif
