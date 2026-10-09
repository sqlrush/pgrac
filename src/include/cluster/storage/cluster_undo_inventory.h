/*-------------------------------------------------------------------------
 *
 * cluster_undo_inventory.h
 *    Complete, positive inventory of this node's published undo segments.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/storage/cluster_undo_inventory.h
 *
 * NOTES
 *    Volatile storage inventory only; never a transaction-status cache.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_INVENTORY_H
#define CLUSTER_UNDO_INVENTORY_H

#include "cluster/cluster_undo_gcs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "port/atomics.h"
#include "storage/spin.h"

#define CLUSTER_UNDO_INVENTORY_WORDS (CLUSTER_UNDO_SEGS_PER_INSTANCE / 64)

typedef struct ClusterUndoInventory {
	slock_t lock;
	uint64 serial;
	uint64 published[CLUSTER_UNDO_INVENTORY_WORDS];
	uint32 publishers;
	bool complete;
	bool disabled;
	uint8 owner;
	char path[MAXPGPATH];
	pg_atomic_uint64 bitmap_hit_count;
	pg_atomic_uint64 full_scan_count;
	pg_atomic_uint64 disable_count;
} ClusterUndoInventory;

typedef struct ClusterUndoInventorySnapshot {
	const ClusterUndoInventory *identity;
	uint64 serial;
	uint64 published[CLUSTER_UNDO_INVENTORY_WORDS];
	bool tracked;
	bool usable;
} ClusterUndoInventorySnapshot;

/* Cumulative per-node scan passes, not transaction matches or verdicts.
 * Full scans include cold/disabled/foreign scans and limited-scan retries.
 * Disable counts false-to-true transitions, once per shared-memory lifetime. */
typedef struct ClusterUndoInventoryStats {
	uint64 bitmap_hit_count;
	uint64 full_scan_count;
	uint64 disable_count;
} ClusterUndoInventoryStats;

extern void cluster_undo_inventory_count_scan(bool bitmap);
/* Each field is sampled independently.  False means no shared inventory. */
extern bool cluster_undo_inventory_read_stats(ClusterUndoInventoryStats *out);

extern void cluster_undo_inventory_attach(ClusterUndoInventory *state, bool initialize);
extern void cluster_undo_inventory_snapshot(ClusterUndoPathIntent intent, uint8 owner,
											ClusterUndoInventorySnapshot *out);
extern bool cluster_undo_inventory_finish(const ClusterUndoInventorySnapshot *snapshot,
										  const uint64 seen[CLUSTER_UNDO_INVENTORY_WORDS],
										  bool complete);
extern bool cluster_undo_inventory_publish_begin(ClusterUndoPathIntent intent, uint8 owner);
/* Paired with a true begin, including on ERROR.  Success means readable final file. */
extern void cluster_undo_inventory_publish_end(uint32 segment, bool success);
extern void cluster_undo_inventory_disable(uint8 owner);

#endif /* CLUSTER_UNDO_INVENTORY_H */
