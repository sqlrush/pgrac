/*-------------------------------------------------------------------------
 * cluster_recovery_undo_store.h
 *   Cold namespace adapter for TT enumeration and exact UBA graph reads.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_UNDO_STORE_H
#define CLUSTER_RECOVERY_UNDO_STORE_H

#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_recovery_root_set.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryUndoStore ClusterRecoveryUndoStore;

/* Borrows the complete observed roots; the cold owner holds their isolation
 * and cuts for the store's entire lifetime. Enumerate every physical undo
 * segment, including unused/recyclable slots and generation zero. No online
 * segment bitmap, CLOG, service or transaction verdict is consulted. */
extern ClusterRecoveryGraphResult cluster_recovery_undo_store_begin(ClusterRecoveryRootSet *roots,
																	size_t memory_budget,
																	ClusterRecoveryUndoStore **out);

/* MORE loads one header; RECORD returns one of its 48 raw TT slots. DRAINED
 * completes only this physical TT inventory, never the recovery graph. */
extern ClusterRecoveryGraphResult cluster_recovery_undo_store_next(ClusterRecoveryUndoStore *store,
																   ClusterRecoveryUndoEntry *out);
extern const UndoSegmentHeaderData *
cluster_recovery_undo_store_header(const ClusterRecoveryUndoStore *store, uint32 segment);

/* ClusterRecoveryGraphRead adapter: exact canonical TT identity and physical
 * UBA segment are distinct. Read a single whole block through the observed
 * namespace and original undo parser; missing/short/changed data is refusal.
 * Available only after the full TT inventory. The caller separately adds
 * all page/history/MX roots before starting its transaction graph. */
extern ClusterRecoveryGraphResult
cluster_recovery_undo_store_read(void *arg, const ClusterRecoveryTxnKey *transaction, UBA address,
								 ClusterRecoveryUndoRecord *record,
								 uint32 *record_segment_generation);

/* Re-read every entire saved TT header, one per call, after graph traversal.
 * DRAINED means byte-stable headers, not transaction or retirement proof. */
extern ClusterRecoveryGraphResult
cluster_recovery_undo_store_recheck(ClusterRecoveryUndoStore *store);
extern void cluster_recovery_undo_store_destroy(ClusterRecoveryUndoStore *store);
#endif
#endif
