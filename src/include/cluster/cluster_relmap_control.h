/*-------------------------------------------------------------------------
 * cluster_relmap_control.h
 *    Control-selected relmap cache and checkpoint observations.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/cluster/cluster_relmap_control.h
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_CONTROL_H
#define CLUSTER_RELMAP_CONTROL_H

#include "cluster/cluster_control_file_identity.h"
#include "common/cluster_relmap_file.h"

typedef struct ClusterRelmapCheckpointReceiptV2 ClusterRelmapCheckpointReceiptV2;
typedef struct ClusterRelmapCheckpointSummaryV2
{
	ClusterControlRelmapReferenceV2 control;
	uint8 files_sha256[32]; /* complete ordered identities and both whole copies */
} ClusterRelmapCheckpointSummaryV2;

/* Explicit managed-format companions. Original owner supplies the independent
 * configuration and retains its SAME CF-S/X throughout; no CF acquisition,
 * reentry, format inference or legacy fallback. Enter outside critical sections
 * and ControlFileLock, before MappingLock.
 * Acquire/release only the original MappingLock around actual pair I/O, then
 * recheck original complete control inputs before publishing local outputs.
 * Failure/ERROR preserves caller outputs, original CF/FD/pins and all debt. */
extern bool cluster_relmap_control_read_v2(const ClusterSharedConfigRef *expected,
	Size registry_byte_budget, Oid database_oid, Oid tablespace_oid, RelMapFile *out);
extern bool cluster_relmap_control_checkpoint_v2(const ClusterSharedConfigRef *expected,
	Size registry_byte_budget, Size receipt_byte_budget,
	ClusterRelmapCheckpointReceiptV2 **fresh);

/* Original checkpoint-local copies, never CF custody or a coverage grant.
 * The later original publisher reads a NEW selection under its own CF interval
 * and checks every saved reference/key/source before its ALL-family/source-cut,
 * actual native WAL insert/flush and original CAS conjunction. A copied digest,
 * per-file success or this recheck cannot replace that conjunction. False or
 * ERROR poisons the local receipt; it cannot be retried or made current again. */
extern bool cluster_relmap_checkpoint_recheck_v2(ClusterRelmapCheckpointReceiptV2 *,
	ClusterControlRelmapSelectionV2 *fresh_selection);
extern bool cluster_relmap_checkpoint_summary_v2(const ClusterRelmapCheckpointReceiptV2 *,
	ClusterRelmapCheckpointSummaryV2 *out);
extern bool cluster_relmap_checkpoint_file_at_v2(const ClusterRelmapCheckpointReceiptV2 *,
	uint64 index, ClusterRecoveryFileIdentityV2 *identity, ClusterRelmapFilePairV2 *pair);
extern bool cluster_relmap_checkpoint_source_at_v2(const ClusterRelmapCheckpointReceiptV2 *,
	uint64 index, ClusterRecoveryCheckpointSourceV2 *out);
/* Only local allocations are destroyed; the original owner keeps all duties. */
extern void cluster_relmap_checkpoint_destroy_v2(ClusterRelmapCheckpointReceiptV2 **);

#endif
