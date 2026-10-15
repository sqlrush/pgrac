/*-------------------------------------------------------------------------
 * cluster_recovery_census.h
 *   Owned observations of the original isolated cold recovery inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_CENSUS_H
#define CLUSTER_RECOVERY_CENSUS_H

#include "cluster/cluster_recovery_native_inventory.h"
#include "cluster/cluster_recovery_undo_store.h"
#include "cluster/cluster_recovery_relation_census.h"
#include "cluster/cluster_recovery_multixact_scan.h"
#include "cluster/cluster_recovery_slru_scan.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryCensus ClusterRecoveryCensus;

typedef enum ClusterRecoveryCensusResult {
	CLUSTER_RECOVERY_CENSUS_MORE = 0,
	CLUSTER_RECOVERY_CENSUS_ENTRY,
	CLUSTER_RECOVERY_CENSUS_DRAINED,
	CLUSTER_RECOVERY_CENSUS_INVALID,
	CLUSTER_RECOVERY_CENSUS_REFUSED,
	CLUSTER_RECOVERY_CENSUS_CAPACITY
} ClusterRecoveryCensusResult;

typedef enum ClusterRecoveryCensusPhase {
	CLUSTER_RECOVERY_CENSUS_WAL = 1,
	CLUSTER_RECOVERY_CENSUS_ROOTS,
	CLUSTER_RECOVERY_CENSUS_TT,
	CLUSTER_RECOVERY_CENSUS_WAL_HEADS,
	CLUSTER_RECOVERY_CENSUS_TX,
	CLUSTER_RECOVERY_CENSUS_TX_TARGETS,
	CLUSTER_RECOVERY_CENSUS_RELATIONS,
	CLUSTER_RECOVERY_CENSUS_SLRU_PHYSICAL,
	CLUSTER_RECOVERY_CENSUS_MX_PHYSICAL,
	CLUSTER_RECOVERY_CENSUS_MX_COMPARE,
	CLUSTER_RECOVERY_CENSUS_EFFECTS,
	CLUSTER_RECOVERY_CENSUS_FINAL_TT,
	CLUSTER_RECOVERY_CENSUS_FINAL_ROOTS,
	CLUSTER_RECOVERY_CENSUS_FINAL_WAL,
	CLUSTER_RECOVERY_CENSUS_DONE
} ClusterRecoveryCensusPhase;

typedef enum ClusterRecoveryCensusKind {
	CLUSTER_RECOVERY_CENSUS_TT_SLOT = 1,
	CLUSTER_RECOVERY_CENSUS_TX_RECORD,
	CLUSTER_RECOVERY_CENSUS_RELATION,
	CLUSTER_RECOVERY_CENSUS_MX_SLOT,
	CLUSTER_RECOVERY_CENSUS_MX_MATCH,
	CLUSTER_RECOVERY_CENSUS_NATIVE_EFFECT,
	CLUSTER_RECOVERY_CENSUS_TX_TARGET,
	CLUSTER_RECOVERY_CENSUS_SLRU_SLOT
} ClusterRecoveryCensusKind;

/* An edge from an observed TX record to the original physical/WAL union.
 * Missing indices/pointers and absent extents remain explicit obligations.
 * These flags describe files only, not SPACE identity or lifecycle proof. */
typedef struct ClusterRecoveryCensusTarget {
	ClusterRecoveryGraphRecord transaction;
	ClusterSpaceIdentityKey key;
	ForkNumber forknum;
	BlockNumber blockno;
	uint64 required_blocks;
	uint32 root_index, space_index; /* UINT32_MAX if no candidate */
	const ClusterRecoveryRootEntry *root, *space;
	bool target_physical, target_extent_present, space_physical;
} ClusterRecoveryCensusTarget;

typedef struct ClusterRecoveryCensusEntry {
	ClusterRecoveryCensusKind kind;
	union {
		ClusterRecoveryUndoEntry tt;
		ClusterRecoveryGraphRecord tx;
		ClusterRecoveryRelationCensusEntry relation;
		ClusterRecoveryMultixactEntry mx;
		ClusterRecoveryMultixactJoinEntry compare;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryCensusTarget target;
		ClusterRecoverySlruEntry slru;
	} value;
} ClusterRecoveryCensusEntry;

/* Diagnostic observations only. On refusal these may describe an incomplete
 * prefix; neither counts nor DRAINED are a graph seal or recovery authority. */
typedef struct ClusterRecoveryCensusDiag {
	ClusterRecoveryCensusPhase phase;
	ClusterControlRootResult input_result;
	ClusterRecoveryRootsResult roots_result;
	ClusterRecoveryGraphResult graph_result;
	ClusterRecoveryRelationCensusResult relation_result;
	ClusterRecoveryMultixactResult mx_result;
	ClusterRecoverySlruResult slru_result;
	ClusterRecoveryMultixactJoinResult join_result;
	ClusterRecoveryNativeInventoryResult inventory_result;
	ClusterRecoveryNativeInventoryDiag inventory;
	uint64 tt_slots, tx_records, relation_entries, mx_entries, mx_comparisons, effects;
	uint64 slru_entries;
	uint64 tx_targets, tx_target_missing, tx_target_short, tx_space_missing;
} ClusterRecoveryCensusDiag;

/* Borrow the original live cold inputs and their ROOT-selected isolated
 * namespace fd. The caller holds isolation, retention and these owners for
 * the entire cursor lifetime; this API establishes none of those premises.
 * Derive namespace identity from the original source, never a caller flag.
 * Own all child cursors under a total memory reservation (external inputs,
 * original WAL reader and cryptohash service allocations are not included).
 * Destroy on every caller exit, including ERROR/cancellation/stop.
 */
extern ClusterRecoveryCensusResult cluster_recovery_census_begin(ClusterWalInputsV1 *inputs,
																 int root_fd, size_t memory_budget,
																 ClusterRecoveryCensus **out);

/* Run the existing ROOT/WAL union, all TT slots and seeded TX records,
 * all TX record target/SPACE candidates, relation/page/history, raw physical
 * CLOG/SUBTRANS/commit_ts/MX, retained MX comparisons and native effects.
 * Then repeat saved TT headers, the entire physical ROOT and
 * original cold WAL census. Each entry is borrowed until next/destroy and
 * provisional until all final rereads; later refusal revokes every child.
 * Ordinary refusals and ERROR clear the output and release owned children.
 * The failed cursor remains sticky and diagnosable until destroyed.
 *
 * Target joins examine one saved node and at most two 128-probe lookups,
 * with no I/O, allocation or changes to the ROOT baseline. Missing/short
 * targets remain explicit obligations and do not complete recovery.
 * Other calls invoke one existing component step. WAL collection/recheck
 * retain their original synchronous complete census; this API does NOT
 * promise a one-page, 128-operation or elapsed-time bound for those calls.
 *
 * DRAINED only joins these observations. Unknown/native/HOT/MX/relmap,
 * missing historical roots, structural and lifecycle effects still require
 * semantic ownership and final full graph sealing. No apply, terminal,
 * recovery completion, OPEN or GC permission is created here.
 */
extern ClusterRecoveryCensusResult cluster_recovery_census_next(ClusterRecoveryCensus *census,
																ClusterRecoveryCensusEntry *out);
extern bool cluster_recovery_census_diag(const ClusterRecoveryCensus *census,
										 ClusterRecoveryCensusDiag *out);
extern void cluster_recovery_census_destroy(ClusterRecoveryCensus *census);
#endif
#endif
