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
#include "cluster/cluster_control_file_identity.h"
#include "cluster/cluster_control_native_files.h"
#include "common/cluster_relmap_file.h"
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
	/* Managed observations borrow the independently selected complete identity
	 * and this copy's whole image; valid only until next step/destroy. */
	const ClusterRecoveryFileIdentityV2 *file_identity;
	const ClusterRelmapV2Image *managed_image;
	bool pending;
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

/* Explicit managed companion. Borrow the SAME continuously held CF selection
 * and OBSERVED roots; never acquire CF or reconstruct a registration from a
 * file. Budget includes fixed scratch independent of namespace count. Every
 * registered namespace must have both physical, compatible copies. Preserve
 * each copy's committed/pending images separately, with all fork obligations.
 * Raw files refuse in this mode. The raw entry refuses managed-sized authority
 * files and never grants managed coverage. Fresh *out must be NULL and caller
 * output must not alias the borrowed roots/selection. Failure/ERROR revokes
 * roots but never destroys the borrowed selection or releases protection.
 * Full original control rechecks bracket each file read and final drain;
 * mapping ticks check only the original live reference, with no control I/O.
 * Final whole ROOT/source/owner qualification remains the cold owner's duty. */
extern ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin_managed_v2(ClusterRecoveryRootSet *roots,
	ClusterControlRelmapSelectionV2 *selection, size_t memory_budget,
	ClusterRecoveryRelmapScan **out);

/* Original Start's complete published table, copied under its narrow CF-X.
 * Caller has released CF before traversal; all original episode/IR/WALR/ROOT
 * duties remain. Pin no new owner: borrow each unique AVAILABLE address from
 * this exact table generation, through both copies and their rereads. Retire
 * that local borrow before the next namespace, or at destroy. A changed table
 * invalidates the cursor even between borrows. Same complete-pair/count/fork
 * checks as the explicit scanner; no raw fallback or managed permission.
 * A failed borrow release is an unretired duty and PANICs without retry/free.
 * Table input is copied, not borrowed. Output aliases refuse before writes. */
extern ClusterRecoveryRelmapResult
cluster_recovery_relmap_scan_begin_native_v2(ClusterRecoveryRootSet *roots,
											 const ClusterControlNativeTableInfoV2 *table,
											 size_t memory_budget, ClusterRecoveryRelmapScan **out);
/* Pure bounds check for composite readers, including exported image storage.
 * No current/CF/I/O call; valid even after cursor failure. */
extern bool cluster_recovery_relmap_scan_output_distinct_v2(const ClusterRecoveryRelmapScan *,
															const void *out, Size length);

/* At most 128 metadata candidates, one target file read (plus managed control
 * rechecks described above), or one mapping/
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
