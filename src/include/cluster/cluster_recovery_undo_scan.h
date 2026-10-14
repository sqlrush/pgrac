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
#include "cluster/cluster_undo_record.h"

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

typedef struct ClusterRecoveryUndoRecord {
	UBA address;
	UndoBlockHeader block_header;
	UndoSlotDirEntry directory;
	UndoRecordHeader header;
	uint16 body_length;
	UndoItlHistoryTrailer history;
	uint8 payload[BLCKSZ - sizeof(UndoBlockHeader) - sizeof(UndoSlotDirEntry)
				  - sizeof(UndoRecordHeader)];
} ClusterRecoveryUndoRecord;

/* Read one candidate from an immutable whole block. expected_* identify the
 * independently read file/block, not fields inferred from the record. Output
 * owns the original bytes, including the transaction-wide predecessor and
 * separate page history. No terminal, checksum or generation authority is
 * inferred here: the cold owner must bind the retained source and segment
 * generation before graph traversal, and revalidate them before completion.
 * page and out must not overlap. Any refusal clears out in full. */
extern ClusterRecoveryUndoResult
cluster_recovery_undo_record_read(const void *page, size_t length, uint32 expected_segment,
								  uint32 expected_block, uint8 expected_owner, UBA address,
								  ClusterRecoveryUndoRecord *out);

#endif
#endif
