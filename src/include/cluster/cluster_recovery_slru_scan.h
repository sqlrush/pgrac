/*-------------------------------------------------------------------------
 * cluster_recovery_slru_scan.h
 *   Raw native transaction slots in the original cold root observation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_SLRU_SCAN_H
#define CLUSTER_RECOVERY_SLRU_SCAN_H

#include "cluster/cluster_recovery_root_set.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoverySlruScan ClusterRecoverySlruScan;
typedef enum ClusterRecoverySlruResult {
	CLUSTER_RECOVERY_SLRU_MORE = 0,
	CLUSTER_RECOVERY_SLRU_ENTRY,
	CLUSTER_RECOVERY_SLRU_DRAINED,
	CLUSTER_RECOVERY_SLRU_INVALID,
	CLUSTER_RECOVERY_SLRU_IO,
	CLUSTER_RECOVERY_SLRU_CHANGED,
	CLUSTER_RECOVERY_SLRU_CORRUPT,
	CLUSTER_RECOVERY_SLRU_CAPACITY,
	CLUSTER_RECOVERY_SLRU_BASE_REQUIRED
} ClusterRecoverySlruResult;

typedef enum ClusterRecoverySlruKind {
	CLUSTER_RECOVERY_SLRU_FILE = 1,
	CLUSTER_RECOVERY_SLRU_CLOG,
	CLUSTER_RECOVERY_SLRU_SUBTRANS,
	CLUSTER_RECOVERY_SLRU_COMMIT_TS,
	CLUSTER_RECOVERY_SLRU_PADDING
} ClusterRecoverySlruKind;

typedef struct ClusterRecoverySlruEntry {
	ClusterRecoverySlruKind kind;
	uint32 root_index;
	ClusterRecoveryFileKind file_kind;
	uint8 family;
	uint32 origin_node; /* UINT32_MAX for local; never infer a failed origin */
	uint32 segment;
	uint64 file_size, page_number;
	uint32 slot;
	uint64 raw_xid; /* no epoch inference or uint32 wrap */
	bool addressable;
	uint8 raw_status;
	uint32 raw_parent;
	int64 raw_timestamp;
	uint16 raw_replication_origin;
	const char *bytes; /* borrowed full page/padding until next/destroy */
	size_t length;
} ClusterRecoverySlruEntry;

typedef struct ClusterRecoverySlruDiag {
	uint64 files, pages, clog_slots, subtrans_slots, commit_ts_slots;
	uint64 unaddressable_slots, padding_bytes, reread_pages, skipped_roots;
} ClusterRecoverySlruDiag;

/* Borrow the original OBSERVED roots under caller-held cold isolation/cuts.
 * Own two page buffers within memory_budget. Roots must outlive this cursor;
 * destroy on every caller exit, including ERROR/cancel/stop. Refusal revokes
 * roots; later roots revocation also invalidates a drained cursor. */
extern ClusterRecoverySlruResult cluster_recovery_slru_scan_begin(ClusterRecoveryRootSet *roots,
																  size_t memory_budget,
																  ClusterRecoverySlruScan **out);

/* One raw slot, one page read/reread or at most128 metadata candidates.
 * Include zero/special xids, all raw values, file boundaries and padding.
 * Native status/parent/timestamp are observations, not terminal, parent-chain,
 * retained-range, era or visibility proofs. Original final whole-root/WAL
 * rechecks and semantic owners remain required; no apply/OPEN/GC authority. */
extern ClusterRecoverySlruResult cluster_recovery_slru_scan_next(ClusterRecoverySlruScan *scan,
																 ClusterRecoverySlruEntry *out);
extern bool cluster_recovery_slru_scan_diag(const ClusterRecoverySlruScan *scan,
											ClusterRecoverySlruDiag *out);
extern void cluster_recovery_slru_scan_destroy(ClusterRecoverySlruScan *scan);
#endif
#endif
