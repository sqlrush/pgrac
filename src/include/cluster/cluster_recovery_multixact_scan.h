/*-------------------------------------------------------------------------
 * cluster_recovery_multixact_scan.h
 *   Physical MultiXact candidates under the original cold root observation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/include/cluster/cluster_recovery_multixact_scan.h
 * NOTES
 *   Raw slots are obligations, never membership or visibility proofs.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_MULTIXACT_SCAN_H
#define CLUSTER_RECOVERY_MULTIXACT_SCAN_H

#include "cluster/cluster_recovery_root_set.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryMultixactScan ClusterRecoveryMultixactScan;

typedef enum ClusterRecoveryMultixactResult {
	CLUSTER_RECOVERY_MX_MORE = 0,
	CLUSTER_RECOVERY_MX_ENTRY,
	CLUSTER_RECOVERY_MX_DRAINED,
	CLUSTER_RECOVERY_MX_INVALID,
	CLUSTER_RECOVERY_MX_IO,
	CLUSTER_RECOVERY_MX_CHANGED,
	CLUSTER_RECOVERY_MX_CORRUPT,
	CLUSTER_RECOVERY_MX_CAPACITY,
	CLUSTER_RECOVERY_MX_BASE_REQUIRED
} ClusterRecoveryMultixactResult;

typedef enum ClusterRecoveryMultixactEntryKind {
	CLUSTER_RECOVERY_MX_FILE = 1,
	CLUSTER_RECOVERY_MX_OFFSET,
	CLUSTER_RECOVERY_MX_MEMBER,
	CLUSTER_RECOVERY_MX_PADDING
} ClusterRecoveryMultixactEntryKind;

typedef struct ClusterRecoveryMultixactEntry {
	ClusterRecoveryMultixactEntryKind kind;
	uint32 root_index;
	ClusterRecoveryFileKind file_kind;
	uint8 family;
	uint32 origin_node; /* UINT32_MAX for local: never infer a failed origin */
	uint32 segment;
	uint64 file_size;
	uint64 page_number;
	uint32 slot;
	uint64 raw_index; /* do not wrap unused final-page slots into member zero */
	bool addressable;
	uint32 value; /* raw offset or raw member xid; zero is not an absent proof */
	uint8 raw_status;
	const char *bytes; /* immutable current page/padding, until next/destroy */
	size_t length;
} ClusterRecoveryMultixactEntry;

typedef struct ClusterRecoveryMultixactDiag {
	uint64 files, pages, offset_slots, member_slots, unaddressable_slots;
	uint64 padding_bytes, reread_pages, skipped_roots;
} ClusterRecoveryMultixactDiag;

/* Borrow an OBSERVED root union, which must outlive this scan. Includes all
 * local and original-origin MX files, keeping their namespaces distinct.
 * The cold owner retains isolation/cuts and performs final whole-graph
 * revalidation. memory_budget covers this scan and both owned page buffers.
 * Destroy on every exit, including PG_FINALLY. Failure invalidates roots. */
extern ClusterRecoveryMultixactResult
cluster_recovery_multixact_scan_begin(ClusterRecoveryRootSet *roots, size_t memory_budget,
									  ClusterRecoveryMultixactScan **out);

/* At most128 metadata candidates, one block read/re-read, or one raw slot.
 * Emit every slot, including zero/unknown status and unaddressable tail
 * slots, plus file boundaries and raw padding. Per-page rereads and later
 * root invalidation revoke even DRAINED. No retained range, origin epoch,
 * offset/member/heap join, terminal, apply, truncate or OPEN/GC authority
 * follows. DRAINED means only this physical subset was enumerated. */
extern ClusterRecoveryMultixactResult
cluster_recovery_multixact_scan_next(ClusterRecoveryMultixactScan *scan,
									 ClusterRecoveryMultixactEntry *out);
extern bool cluster_recovery_multixact_scan_diag(const ClusterRecoveryMultixactScan *scan,
												 ClusterRecoveryMultixactDiag *out);
extern void cluster_recovery_multixact_scan_destroy(ClusterRecoveryMultixactScan *scan);
#endif
#endif
