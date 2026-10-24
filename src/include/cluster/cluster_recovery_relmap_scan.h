/*-------------------------------------------------------------------------
 * cluster_recovery_relmap_scan.h
 *   Physical native map observations and their original ROOT reference edges.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_RELMAP_SCAN_H
#define CLUSTER_RECOVERY_RELMAP_SCAN_H

#include "cluster/cluster_recovery_root_set.h"
#include "common/relmap.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryRelmapScan ClusterRecoveryRelmapScan;
typedef enum ClusterRecoveryRelmapResult {
	CLUSTER_RECOVERY_RELMAP_MORE = 0,
	CLUSTER_RECOVERY_RELMAP_ENTRY,
	CLUSTER_RECOVERY_RELMAP_DRAINED,
	CLUSTER_RECOVERY_RELMAP_INVALID,
	CLUSTER_RECOVERY_RELMAP_IO,
	CLUSTER_RECOVERY_RELMAP_CHANGED,
	CLUSTER_RECOVERY_RELMAP_CORRUPT,
	CLUSTER_RECOVERY_RELMAP_CAPACITY,
	CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED
} ClusterRecoveryRelmapResult;

typedef enum ClusterRecoveryRelmapKind {
	CLUSTER_RECOVERY_RELMAP_IMAGE = 1,
	CLUSTER_RECOVERY_RELMAP_MAPPING,
	CLUSTER_RECOVERY_RELMAP_TARGET
} ClusterRecoveryRelmapKind;

typedef struct ClusterRecoveryRelmapEntry {
	ClusterRecoveryRelmapKind kind;
	uint32 map_index, ordinal;
	ClusterRecoveryFileKind file_kind;
	const RelMapFile *image; /* complete borrowed bytes, including inactive slots */
	RelMapping mapping;
	ClusterSpaceIdentityKey key; /* observed namespace/address, not incarnation */
	ForkNumber forknum;
	uint32 target_index; /* UINT32_MAX: no candidate, never deletion proof */
	const ClusterRecoveryRootEntry *target;
	bool physical, extent_present;
} ClusterRecoveryRelmapEntry;

typedef struct ClusterRecoveryRelmapDiag {
	uint64 skipped_roots, images, mappings, targets, missing_targets, short_targets, rereads;
} ClusterRecoveryRelmapDiag;

/* Borrow the original OBSERVED roots and namespace, keeping the caller's
 * isolation/retention ownership. One fixed-budget scratch cursor. Primary
 * and backup native maps remain separate observations; neither is selected
 * as authoritative. Authority files/pending publication need their original
 * consumers. This reader cannot publish a map or authorize recovery. */
extern ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin(ClusterRecoveryRootSet *roots, uint64 system_identifier,
								   uint64 database_incarnation, const uint8 storage_uuid[16],
								   size_t memory_budget, ClusterRecoveryRelmapScan **out);

/* At most 128 metadata candidates, one original file read, or one mapping/
 * exact bounded fork lookup per call. No new roots or revised baseline.
 * Every image is reread after its entries; all outputs remain provisional
 * until the cold owner's final whole ROOT/TT/WAL checks. Missing/short fork
 * edges remain obligations; a missing/short map or invalid shape refuses.
 * Any refusal revokes the roots; outputs clear and failure stays sticky.
 * DRAINED gives no authority/terminal/apply/OPEN/GC permission. */
extern ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_next(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRelmapEntry *out);
extern bool cluster_recovery_relmap_scan_diag(const ClusterRecoveryRelmapScan *scan,
											  ClusterRecoveryRelmapDiag *out);
extern void cluster_recovery_relmap_scan_destroy(ClusterRecoveryRelmapScan *scan);
#endif
#endif
