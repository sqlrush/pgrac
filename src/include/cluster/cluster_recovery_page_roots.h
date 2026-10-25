/*-------------------------------------------------------------------------
 * cluster_recovery_page_roots.h
 *   PAGE/history root discovery before the original TX graph traversal.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_PAGE_ROOTS_H
#define CLUSTER_RECOVERY_PAGE_ROOTS_H

#include "cluster/cluster_recovery_undo_store.h"
#include "cluster/cluster_recovery_page_history.h"
#include "cluster/cluster_recovery_relation_scan.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryPageRoots ClusterRecoveryPageRoots;

typedef struct ClusterRecoveryPageRootsDiag {
	uint64 pages, versions, references;
	uint64 deferred_references, deferred_histories, deferred_relations;
	ClusterRecoveryRelationResult relation_result;
	ClusterRecoveryHistoryResult history_result;
	ClusterRecoveryGraphResult graph_result;
} ClusterRecoveryPageRootsDiag;

/* Borrow the store's complete original ROOT/TT namespace and unstarted TX
 * graph. Scan every physical MAIN heap page and the private versions reached
 * through the original history inverse, adding exact canonical-base roots.
 * Reused/missing canonical bindings remain explicit deferred obligations;
 * an independently seeded WAL forest can still supply them to the original
 * observed-graph PAGE consumer. A deferred inverse stops only that private
 * history branch; it never proves an empty or fully covered history.
 *
 * The caller MUST subsequently run the original complete TX traversal and
 * rereads, observed-graph relation/PAGE/history census and final TT/ROOT/WAL
 * checks. DRAINED here is discovery only, never coverage, a semantic seal,
 * terminal, apply, service or retirement authority. No bytes are mutated.
 * memory_budget covers this cursor and its concurrent relation/heap/history
 * scratch, separately from the original store/graph/ROOT reservations. */
extern ClusterRecoveryGraphResult cluster_recovery_page_roots_begin(
	ClusterRecoveryUndoStore *store, size_t memory_budget, ClusterRecoveryPageRoots **out);
/* At most one original relation block read or one undo block read per call,
 * or128 local root entries. Failures revoke the graph and namespace; diag is
 * value-owned and retained on failure. Destroy before starting the graph;
 * graph/store/ROOT retain every source and must outlive this cursor. */
extern ClusterRecoveryGraphResult cluster_recovery_page_roots_step(
	ClusterRecoveryPageRoots *cursor, ClusterRecoveryPageRootsDiag *diag);
extern void cluster_recovery_page_roots_destroy(ClusterRecoveryPageRoots *cursor);
#endif
#endif
