/*-------------------------------------------------------------------------
 * cluster_recovery_page_census.h
 *   Cold heap/history references joined to the observed transaction forest.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_PAGE_CENSUS_H
#define CLUSTER_RECOVERY_PAGE_CENSUS_H

#include "cluster/cluster_recovery_heap_scan.h"
#include "cluster/cluster_recovery_page_history.h"
#include "cluster/cluster_recovery_relation_scan.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryPageCensus ClusterRecoveryPageCensus;

typedef enum ClusterRecoveryPageCensusResult {
	CLUSTER_RECOVERY_PAGE_MORE = 0,
	CLUSTER_RECOVERY_PAGE_ENTRY,
	CLUSTER_RECOVERY_PAGE_DRAINED,
	CLUSTER_RECOVERY_PAGE_INVALID,
	CLUSTER_RECOVERY_PAGE_CHANGED,
	CLUSTER_RECOVERY_PAGE_IDENTITY,
	CLUSTER_RECOVERY_PAGE_CORRUPT,
	CLUSTER_RECOVERY_PAGE_CAPACITY,
	CLUSTER_RECOVERY_PAGE_BASE_REQUIRED
} ClusterRecoveryPageCensusResult;

typedef struct ClusterRecoveryPageReference {
	uint64 version; /* original physical page is zero; no SCN ordering claim */
	ClusterRecoveryHeapEntry entry;
	/* Non-FREE ITL only. Borrowed from the graph, no terminal authority. */
	ClusterRecoveryGraphRecord transaction;
	/* Same complete history version, valid through this entry until next().
	 * Borrowed bytes, never a reconstructed image from tuple/reference data. */
	const char *page;
} ClusterRecoveryPageReference;

/* Copies a MAIN heap page produced by the original SPACE-bound relation
 * scanner. The cold owner retains namespace/isolation and the observed TX
 * forest; seed every retained TT/WAL root before walking that forest.
 * All eight ITLs and all raw tuple/MX/HOT/line-pointer candidates are emitted
 * on each version before advancing original page history. A missing history
 * transaction is BASE_REQUIRED, never inferred from a current TT generation.
 * memory_budget includes this consumer, its heap scan, lookup and history;
 * the caller-owned graph and relation scanner have separate budgets. */
extern ClusterRecoveryPageCensusResult
cluster_recovery_page_census_begin(const ClusterRecoveryRelationPage *page,
								   ClusterRecoveryReferenceGraph *graph, size_t memory_budget,
								   ClusterRecoveryPageCensus **out);

/* One heap entry, one inverse, or at most 128 saved TX nodes per call.
 * Tuple bytes live until the next call/destroy; graph records until graph
 * invalidation/destroy. All results are provisional until the owner's full
 * roots/TT/input rechecks. Any refusal invalidates the forest, including a
 * previously drained one. DRAINED is this page/history path only: native MX,
 * HOT/tuple obligations, missing base proofs and the full graph still need
 * their consumers; it grants no apply, service or retirement permission. */
extern ClusterRecoveryPageCensusResult
cluster_recovery_page_census_next(ClusterRecoveryPageCensus *census,
								  ClusterRecoveryPageReference *out);
extern void cluster_recovery_page_census_destroy(ClusterRecoveryPageCensus *census);
#endif
#endif
