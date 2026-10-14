/*-------------------------------------------------------------------------
 * cluster_recovery_page_history.h
 *   Private page versions reachable through exact retained ITL history.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_PAGE_HISTORY_H
#define CLUSTER_RECOVERY_PAGE_HISTORY_H

#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_space_identity.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterRecoveryHistoryResult {
	CLUSTER_RECOVERY_HISTORY_MORE = 0,
	CLUSTER_RECOVERY_HISTORY_PAGE,
	CLUSTER_RECOVERY_HISTORY_NEED_RECORD,
	CLUSTER_RECOVERY_HISTORY_DRAINED,
	CLUSTER_RECOVERY_HISTORY_INVALID,
	CLUSTER_RECOVERY_HISTORY_IDENTITY,
	CLUSTER_RECOVERY_HISTORY_CORRUPT,
	CLUSTER_RECOVERY_HISTORY_CYCLE,
	CLUSTER_RECOVERY_HISTORY_CAPACITY,
	CLUSTER_RECOVERY_HISTORY_BASE_REQUIRED
} ClusterRecoveryHistoryResult;

typedef struct ClusterRecoveryHistoryEvent {
	const char *page;		  /* immutable, owned by the walker; valid until accept/destroy */
	ClusterTxLocator locator; /* page-slot wrap is not the canonical TT wrap */
} ClusterRecoveryHistoryEvent;

typedef struct ClusterRecoveryPageHistory ClusterRecoveryPageHistory;

/* identity and bytes must already belong to the cold owner's retained
 * namespace. Structural validity grants no SPACE, terminal or read authority.
 * The walker owns its copies; input bytes and live buffers are never changed. */
extern ClusterRecoveryHistoryResult
cluster_recovery_page_history_begin(const ClusterSpaceIdentity *identity, ForkNumber forknum,
									BlockNumber block, const void *page, size_t length,
									size_t memory_budget, ClusterRecoveryPageHistory **out);

/* Emit the original page and every reconstructed page, so callers enumerate
 * all tuple/ITL/MX/HOT candidates with cluster_recovery_heap_scan. Then request
 * only the newest remaining page-local history head. No transaction-global
 * prev chain, write_scn/H cutoff, online resolver or shared-buffer access.
 * BASE_REQUIRED is unproven history, never an empty graph or completion. */
extern ClusterRecoveryHistoryResult
cluster_recovery_page_history_next(ClusterRecoveryPageHistory *history,
								   ClusterRecoveryHistoryEvent *out);

/* Accept only the pending exact record under its separately established TT
 * identity and physical segment generation. Reuse the original pure CR
 * inverse, including its target/role/wrap/SCN/prior checks. Failure is sticky
 * and never exports a partially mutated image. The caller also registers
 * this record's TX_RECORD root; its prev may address a different relation.
 * DRAINED covers this history path only, not D1/terminal/apply/OPEN/GC. */
extern ClusterRecoveryHistoryResult cluster_recovery_page_history_accept(
	ClusterRecoveryPageHistory *history, const ClusterRecoveryTxnKey *transaction,
	uint32 record_segment_generation, const ClusterRecoveryUndoRecord *record);
extern void cluster_recovery_page_history_destroy(ClusterRecoveryPageHistory *history);

#endif
#endif
