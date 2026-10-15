/*-------------------------------------------------------------------------
 * cluster_recovery_reference_graph.h
 *   Bounded, read-only transaction-record dependencies for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_REFERENCE_GRAPH_H
#define CLUSTER_RECOVERY_REFERENCE_GRAPH_H

#include "cluster/cluster_recovery_undo_scan.h"

#ifdef USE_PGRAC_CLUSTER

/* A persistent TT address, not an online grant or a terminal verdict. The
 * cold owner must obtain the generation from its exact retained namespace.
 * Zero segment generation is legal. A raw xid alone never identifies a node. */
typedef struct ClusterRecoveryTxnKey {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint32 segment_id;
	uint32 segment_generation;
	TransactionId xid;
	uint16 origin_node_id;
	uint16 slot_offset;
	uint16 slot_wrap;
} ClusterRecoveryTxnKey;

typedef enum ClusterRecoveryGraphResult {
	CLUSTER_RECOVERY_GRAPH_MORE = 0,
	CLUSTER_RECOVERY_GRAPH_RECORD,
	CLUSTER_RECOVERY_GRAPH_PENDING,
	CLUSTER_RECOVERY_GRAPH_DRAINED,
	CLUSTER_RECOVERY_GRAPH_INVALID,
	CLUSTER_RECOVERY_GRAPH_IO,
	CLUSTER_RECOVERY_GRAPH_IDENTITY,
	CLUSTER_RECOVERY_GRAPH_CHANGED,
	CLUSTER_RECOVERY_GRAPH_CYCLE,
	CLUSTER_RECOVERY_GRAPH_CAPACITY,
	CLUSTER_RECOVERY_GRAPH_CORRUPT
} ClusterRecoveryGraphResult;

/* Read exactly the requested record through the original cold namespace
 * owner and cluster_recovery_undo_record_read. RECORD returns the immutable
 * bytes and the physical record segment's generation, independently of the
 * canonical TT segment. PENDING may be retried; every other result refuses
 * this graph. No missing/short/unknown input is an empty transaction chain. */
typedef ClusterRecoveryGraphResult (*ClusterRecoveryGraphRead)(
	void *arg, const ClusterRecoveryTxnKey *transaction, UBA address,
	ClusterRecoveryUndoRecord *record, uint32 *record_segment_generation);

typedef struct ClusterRecoveryGraphRecord {
	ClusterRecoveryTxnKey transaction;
	uint32 record_segment_generation;
	const ClusterRecoveryUndoRecord *record; /* borrowed until graph destroy */
} ClusterRecoveryGraphRecord;

typedef struct ClusterRecoveryReferenceGraph ClusterRecoveryReferenceGraph;
typedef struct ClusterRecoveryGraphLookup ClusterRecoveryGraphLookup;

typedef enum ClusterRecoveryGraphLookupResult {
	CLUSTER_RECOVERY_LOOKUP_MORE = 0,
	CLUSTER_RECOVERY_LOOKUP_RECORD,
	CLUSTER_RECOVERY_LOOKUP_MISSING,
	CLUSTER_RECOVERY_LOOKUP_INVALID,
	CLUSTER_RECOVERY_LOOKUP_CHANGED,
	CLUSTER_RECOVERY_LOOKUP_IDENTITY,
	CLUSTER_RECOVERY_LOOKUP_CAPACITY
} ClusterRecoveryGraphLookupResult;

/* Pure byte/identity validation shared with PAGE_VERSION. It neither reads
 * storage nor establishes the caller's generation/namespace authority. */
extern ClusterRecoveryGraphResult
cluster_recovery_tx_record_check(const ClusterRecoveryTxnKey *transaction, UBA address,
								 const ClusterRecoveryUndoRecord *record,
								 uint32 record_segment_generation);

extern ClusterRecoveryGraphResult
cluster_recovery_reference_graph_begin(size_t memory_budget, ClusterRecoveryGraphRead read,
									   void *arg, ClusterRecoveryReferenceGraph **out);

/* Roots are accepted only before the first step. The caller enumerates all
 * TT slots and page references; an invalid UBA is not an empty-chain root. */
extern ClusterRecoveryGraphResult
cluster_recovery_reference_graph_add(ClusterRecoveryReferenceGraph *graph,
									 const ClusterRecoveryTxnKey *transaction, UBA head);
/* Root-producer lifecycle check only, never proof of producer coverage. */
extern bool
cluster_recovery_reference_graph_accepts_roots(const ClusterRecoveryReferenceGraph *graph);

/* At most one record read and 128 graph transitions per call. Every record
 * is exposed once with all payload/history bytes and its actual target;
 * cross-relation prev edges are followed, never inverse-applied to a page.
 * After traversal, a second read of every node verifies the saved physical
 * generation and bytes. DRAINED means this supplied TX forest is stable,
 * not full producer coverage, transaction completion, apply, OPEN or GC.
 * PAGE_VERSION dependencies have a separate inverse and must not be replaced
 * by walking this transaction-global prev chain. Errors are sticky. */
extern ClusterRecoveryGraphResult
cluster_recovery_reference_graph_step(ClusterRecoveryReferenceGraph *graph,
									  ClusterRecoveryGraphRecord *out);
extern uint32 cluster_recovery_reference_graph_count(const ClusterRecoveryReferenceGraph *graph);
/* Only the supplied TX forest has completed its second record reads. This
 * does not assert that all page/native/structural producers were supplied. */
extern bool cluster_recovery_reference_graph_observed(const ClusterRecoveryReferenceGraph *graph);

/* Match a page locator against saved, fully re-read records, never against
 * the current TT slot generation guessed from the UBA. The forest must be
 * observed and outlive this lookup and all returned borrowed records.
 * At most 128 nodes per next(); a match is exported only after the whole
 * forest rules out ambiguous canonical identities. MISSING is an unresolved
 * dependency, not an empty history or terminal verdict. Unknown page wrap
 * is permitted; the original record provides it. No online lookup or I/O. */
extern ClusterRecoveryGraphLookupResult
cluster_recovery_reference_lookup_begin(const ClusterRecoveryReferenceGraph *graph,
										uint64 system_identifier, uint64 database_incarnation,
										const ClusterTxLocator *locator, size_t memory_budget,
										ClusterRecoveryGraphLookup **out);
extern ClusterRecoveryGraphLookupResult
cluster_recovery_reference_lookup_next(ClusterRecoveryGraphLookup *lookup,
									   ClusterRecoveryGraphRecord *out);
extern void cluster_recovery_reference_lookup_destroy(ClusterRecoveryGraphLookup *lookup);
/* A producer's later observation failed. Revoke even a drained TX forest;
 * this never reopens it for new roots or turns an earlier refusal into MORE. */
extern void cluster_recovery_reference_graph_invalidate(ClusterRecoveryReferenceGraph *graph);
extern void cluster_recovery_reference_graph_destroy(ClusterRecoveryReferenceGraph *graph);

#endif
#endif
