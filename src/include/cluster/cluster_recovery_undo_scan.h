/*-------------------------------------------------------------------------
 * cluster_recovery_undo_scan.h
 *   Read-only undo header and TT candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_UNDO_SCAN_H
#define CLUSTER_RECOVERY_UNDO_SCAN_H

#include "cluster/cluster_undo_segment.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterRecoveryUndoResult {
	CLUSTER_RECOVERY_UNDO_MORE = 0,
	CLUSTER_RECOVERY_UNDO_ENTRY,
	CLUSTER_RECOVERY_UNDO_END,
	CLUSTER_RECOVERY_UNDO_INVALID,
	CLUSTER_RECOVERY_UNDO_UNSUPPORTED,
	CLUSTER_RECOVERY_UNDO_CORRUPT,
	CLUSTER_RECOVERY_UNDO_IDENTITY,
	CLUSTER_RECOVERY_UNDO_CAPACITY
} ClusterRecoveryUndoResult;

typedef struct ClusterRecoveryUndoEntry {
	uint32 segment_id;
	uint32 segment_generation;
	uint8 owner_instance;
	uint16 slot_offset;
	TTSlot slot;
} ClusterRecoveryUndoEntry;

typedef struct ClusterRecoveryUndoScan ClusterRecoveryUndoScan;

/* Copy a complete block zero already obtained under the cold owner's exact
 * namespace, isolation and checksum checks. expected_* come from that owner's
 * candidate key, not the page itself. This validates layout and key only:
 * neither the status/SCN nor a release flag supplies terminal authority. */
extern ClusterRecoveryUndoResult cluster_recovery_undo_scan_begin(const void *page, size_t length,
																  uint32 expected_segment,
																  uint8 expected_owner,
																  ClusterRecoveryUndoScan **out);

/* Borrow the complete immutable header, including bitmap, retention fields
 * and counters. Counts and segment lifecycle never shorten the slot scan. */
extern const UndoSegmentHeaderData *
cluster_recovery_undo_scan_header(const ClusterRecoveryUndoScan *scan);

/* All 48 slots, including UNUSED and RECYCLABLE, one per call. Raw incomplete
 * terminal fields remain candidates; no CLOG or online liveness is consulted.
 * END completes this header only, not the segment/record graph or recovery. */
extern ClusterRecoveryUndoResult cluster_recovery_undo_scan_next(ClusterRecoveryUndoScan *scan,
																 ClusterRecoveryUndoEntry *out);
extern void cluster_recovery_undo_scan_destroy(ClusterRecoveryUndoScan *scan);

#endif
#endif
