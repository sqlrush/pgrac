/*-------------------------------------------------------------------------
 * cluster_recovery_multixact_join.h
 *   Retained MultiXact CREATE bytes and their physical origin candidates.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_MULTIXACT_JOIN_H
#define CLUSTER_RECOVERY_MULTIXACT_JOIN_H

#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_recovery_root_set.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryMultixactJoin ClusterRecoveryMultixactJoin;

typedef enum ClusterRecoveryMultixactJoinResult {
	CLUSTER_RECOVERY_MX_JOIN_MORE = 0,
	CLUSTER_RECOVERY_MX_JOIN_ENTRY,
	CLUSTER_RECOVERY_MX_JOIN_DRAINED,
	CLUSTER_RECOVERY_MX_JOIN_INVALID,
	CLUSTER_RECOVERY_MX_JOIN_CHANGED,
	CLUSTER_RECOVERY_MX_JOIN_IO,
	CLUSTER_RECOVERY_MX_JOIN_BASE_REQUIRED,
	CLUSTER_RECOVERY_MX_JOIN_CAPACITY
} ClusterRecoveryMultixactJoinResult;

typedef struct ClusterRecoveryMultixactJoinEntry {
	const ClusterRecoveryNativeRecord *record;
	bool is_offset;
	uint32 mid;
	uint32 ordinal; /* member ordinal, zero for the start offset */
	uint32 address; /* original raw MX id or member offset, with native wrap */
	uint32 root_index;
	uint64 page_number;
	uint32 expected_value, observed_value;
	uint8 expected_status, observed_status;
	bool equal; /* byte comparison only, never membership/terminal authority */
} ClusterRecoveryMultixactJoinEntry;

/* Borrow an original owned CREATE scan and OBSERVED roots. Both must outlive
 * the join. The caller retains the original cold scope/isolation and performs
 * final whole-input/root rechecks. No local SLRU/cache or inferred endpoint.
 * memory_budget covers this cursor and its two page buffers, not its owners. */
extern ClusterRecoveryMultixactJoinResult
cluster_recovery_multixact_join_begin(ClusterRecoveryRootSet *roots,
									  ClusterRecoveryNativeScan *create, size_t memory_budget,
									  ClusterRecoveryMultixactJoin **out);

/* At most 128 root candidates, one page read/re-read, or one comparison per
 * call. Emit the start offset and every original WAL member, including raw
 * physical zero/unknown/mismatching bytes; these remain owner obligations.
 * A match does not prove the current tuple's retained range or source era.
 * DRAINED is this record only, no apply/terminal/truncate/OPEN/GC permission.
 * Failure invalidates roots; later root/record invalidation revokes DRAINED.
 * Entries borrow the original record; discard them on owner invalidation. */
extern ClusterRecoveryMultixactJoinResult
cluster_recovery_multixact_join_next(ClusterRecoveryMultixactJoin *join,
									 ClusterRecoveryMultixactJoinEntry *out);
extern void cluster_recovery_multixact_join_destroy(ClusterRecoveryMultixactJoin *join);
#endif
#endif
