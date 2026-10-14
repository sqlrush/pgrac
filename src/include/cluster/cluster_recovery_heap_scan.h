/*-------------------------------------------------------------------------
 * cluster_recovery_heap_scan.h
 *   Read-only heap-page candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_HEAP_SCAN_H
#define CLUSTER_RECOVERY_HEAP_SCAN_H

#include "storage/bufpage.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterRecoveryHeapResult {
	CLUSTER_RECOVERY_HEAP_MORE = 0,
	CLUSTER_RECOVERY_HEAP_ENTRY,
	CLUSTER_RECOVERY_HEAP_END,
	CLUSTER_RECOVERY_HEAP_INVALID,
	CLUSTER_RECOVERY_HEAP_UNSUPPORTED,
	CLUSTER_RECOVERY_HEAP_CORRUPT,
	CLUSTER_RECOVERY_HEAP_CAPACITY
} ClusterRecoveryHeapResult;

typedef enum ClusterRecoveryHeapEntryKind {
	CLUSTER_RECOVERY_HEAP_ITL = 1,
	CLUSTER_RECOVERY_HEAP_ITEM
} ClusterRecoveryHeapEntryKind;

typedef struct ClusterRecoveryHeapEntry {
	ClusterRecoveryHeapEntryKind kind;
	uint16 index; /* zero-based ITL slot, or one-based line pointer */
	ClusterItlSlotData itl;
	ItemIdData item;
	const char *tuple_bytes; /* owned by the scan; NULL for no-storage items */
	uint16 tuple_length;
} ClusterRecoveryHeapEntry;

typedef struct ClusterRecoveryHeapScan ClusterRecoveryHeapScan;

/* Copies one page already read under the original cold owner's namespace,
 * checksum, isolation and storage-identity checks. It validates the local
 * heap layout, not those authorities. A non-heap or unknown page is not an
 * empty reference set. No online visibility, WAL or buffer access occurs. */
extern ClusterRecoveryHeapResult cluster_recovery_heap_scan_begin(const void *page, size_t length,
																  ClusterRecoveryHeapScan **out);

/* Emits every ITL (including FREE) and every line pointer (including DEAD,
 * REDIRECT and UNUSED). Raw tuple bytes retain MX, HOT and hint information;
 * none of it is interpreted as a terminal proof. At most one item per call.
 * END completes only this page, never the recovery graph or service gate. */
extern ClusterRecoveryHeapResult cluster_recovery_heap_scan_next(ClusterRecoveryHeapScan *scan,
																 ClusterRecoveryHeapEntry *out);
extern void cluster_recovery_heap_scan_destroy(ClusterRecoveryHeapScan *scan);

#endif
#endif
