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
#include "cluster/cluster_control_native_files.h"
#include "common/cluster_relmap_file.h"
#include "common/cluster_relmap_create.h"
#include "access/xlogreader.h"

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

/* Original CREATE DATABASE owner companion, not an activation entry. Caller
 * retains SAME global relmap X -> CF-X plus target creation/file duty. Enter
 * before MappingLock/ControlFile/XidGen/SLRU/truncation locks. Source must be a
 * unique AVAILABLE default-tablespace database; target is the exact independent
 * reserved carrier. Locks the carrier's local copy attempt before any fallible
 * work. No retry after false/ERROR; partial files/reservation remain debt.
 * Writes and flushes the independent creation WAL at its actual insert end,
 * then creates those exact clean generation-one copies, syncs/full-rereads, and retires
 * only native borrows under continuous original protection and calls the
 * original one-shot control completion. No whole-database admission,
 * retention release or OPEN grant. Original live shared CREATE DATABASE still
 * refuses until its complete native caller/recovery contract is connected. */
extern bool cluster_relmap_control_copy_create_v2(ClusterControlNativeRelmapCreateV2 *,
	Oid source_database, Oid source_tablespace);

/* Read-only native redo input adapter: require the original decoded CREATE
 * record, no block/PAGE edge, exact main payload and actual current EndRecPtr.
 * Complete expected identity and batch come from the caller's independent
 * episode/RESERVED registration. The adapter performs no I/O or completion and
 * does not replace that authorization, native WAL/source validation or original
 * protection. Only its successful private result can feed the file replay step.
 * Refusal preserves out; every overlap with the original reader is refused. */
extern bool cluster_relmap_control_create_redo_input_v1(XLogReaderState *,
	const ClusterRecoveryFileIdentityV2 *, const uint8 batch_uuid[16],
	ClusterRelmapCreateReplayV1 *out);

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
