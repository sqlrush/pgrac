/*-------------------------------------------------------------------------
 * cluster_recovery_relation_scan.h
 *   SPACE-bound physical relation candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_RELATION_SCAN_H
#define CLUSTER_RECOVERY_RELATION_SCAN_H
#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_space_reservation.h"

#ifdef USE_PGRAC_CLUSTER
typedef enum ClusterRecoveryRelationResult {
	CLUSTER_RECOVERY_RELATION_MORE = 0,
	CLUSTER_RECOVERY_RELATION_PAGE,
	CLUSTER_RECOVERY_RELATION_DRAINED,
	CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED,
	CLUSTER_RECOVERY_RELATION_INVALID,
	CLUSTER_RECOVERY_RELATION_IO,
	CLUSTER_RECOVERY_RELATION_CHANGED,
	CLUSTER_RECOVERY_RELATION_CORRUPT,
	CLUSTER_RECOVERY_RELATION_CAPACITY
} ClusterRecoveryRelationResult;

typedef struct ClusterRecoveryRelationPage {
	ClusterSpaceReservation reservation;
	uint64 identity_token;
	uint64 reservation_token;
	ForkNumber forknum;
	BlockNumber block;
	const char *page;
} ClusterRecoveryRelationPage;

typedef struct ClusterRecoveryRelationScan ClusterRecoveryRelationScan;

/* Borrows an OBSERVED namespace and one relation candidate. The original
 * recovery owner supplies isolation/cuts; matching SPACE bytes establishes
 * a candidate identity, not authority or structural replay completion.
 * Missing/half-installed SPACE and WAL targets beyond EOF require the typed
 * structure owner, never an empty obligation set. */
extern ClusterRecoveryRelationResult
cluster_recovery_relation_begin(ClusterRecoveryRootSet *roots, uint32 root_index,
								size_t memory_budget, ClusterRecoveryRelationScan **out);

/* At most one original bounded block read per call. Enumerates every block
 * of MAIN/VM/FSM/SPACE, including untouched pages. No hint, page flag or
 * TOMBSTONE suppresses a block. Caller decodes each page using its real
 * producer, including heap/history/MX. Recheck both complete SPACE pages
 * after enumeration. DRAINED covers this fork only, not D1/apply/OPEN/GC. */
extern ClusterRecoveryRelationResult
cluster_recovery_relation_next(ClusterRecoveryRelationScan *scan, ClusterRecoveryRelationPage *out);
extern const ClusterSpaceReservation *
cluster_recovery_relation_identity(const ClusterRecoveryRelationScan *scan);
extern void cluster_recovery_relation_destroy(ClusterRecoveryRelationScan *scan);
#endif
#endif
