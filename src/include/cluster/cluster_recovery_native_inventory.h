/*-------------------------------------------------------------------------
 * cluster_recovery_native_inventory.h
 *    Native and undo effects observed through the original cold inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_NATIVE_INVENTORY_H
#define CLUSTER_RECOVERY_NATIVE_INVENTORY_H

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_recovery_multixact_join.h"
#include "cluster/cluster_recovery_xact_join.h"
#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_wal_inputs.h"

typedef enum ClusterRecoveryNativeInventoryResult {
	CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED = 0,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_END,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_INPUT,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_UNSUPPORTED,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED,
	CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY
} ClusterRecoveryNativeInventoryResult;

/* Provisional terminal edges retain their exact producer, not a visibility
 * or completion verdict. Legacy identities need a prior same-source BIND. */
typedef enum ClusterRecoveryTerminalIdentity {
	CLUSTER_RECOVERY_TERMINAL_NATIVE_ONLY = 1,
	CLUSTER_RECOVERY_TERMINAL_EXACT,
	CLUSTER_RECOVERY_TERMINAL_BOUND,
	CLUSTER_RECOVERY_TERMINAL_BINDING_MISSING,
	CLUSTER_RECOVERY_TERMINAL_BINDING_MISMATCH
} ClusterRecoveryTerminalIdentity;

typedef enum ClusterRecoveryTerminalResult {
	CLUSTER_RECOVERY_TERMINAL_MORE = 0,
	CLUSTER_RECOVERY_TERMINAL_ENTRY,
	CLUSTER_RECOVERY_TERMINAL_DRAINED,
	CLUSTER_RECOVERY_TERMINAL_INVALID,
	CLUSTER_RECOVERY_TERMINAL_CHANGED,
	CLUSTER_RECOVERY_TERMINAL_CAPACITY
} ClusterRecoveryTerminalResult;

typedef struct ClusterRecoveryTerminalEntry {
	const ClusterRecoveryNativeRecord *record;
	const ClusterRecoveryNativeRecord *binding_record;
	ClusterRecoveryTxnKey transaction; /* generation UINT32_MAX unless EXACT/BOUND */
	SCN scn;
	uint8 status; /* terminal value recorded by this producer, not applied */
	ClusterRecoveryTerminalIdentity identity;
} ClusterRecoveryTerminalEntry;

typedef struct ClusterRecoveryNativeInventoryDiag {
	ClusterControlRootResult input_result;
	RfPageProofDetailV1 input_detail;
	ClusterRecoveryNativeResult native_result;
	uint32 sources;
	uint64 records;
	uint64 native_records;
	uint64 multixact_records;				/* subset of native_records; no member/status proof */
	uint64 clog_records, commit_ts_records; /* native observations, not lifecycle proofs */
	uint64 prepare_records; /* raw pending obligations, not prepared/terminal authority */
	uint64 relmap_records;	/* image observations, not publication proofs */
	uint64 undo_records;	/* retained observations, not executed undo effects */
	uint64 other_records;	/* families not yet enumerated by this inventory */
	uint64 page_targets;
	uint64 rebuildable_targets;
	uint64 structural_effects;
	uint64 undo_targets;
	uint64 unowned_target_records;
	uint64 tt_roots_added; /* successful root additions, before graph deduplication */
	uint64 tt_binding_records_examined;
	uint64 tt_head_binding_missing;
	uint64 terminal_records, terminal_bound, terminal_unbound, terminal_binding_examined;
	uint64 mx_compared_offsets, mx_compared_members, mx_physical_mismatches;
	uint64 xact_clog_compared, xact_parents_compared, xact_physical_mismatches;
	ClusterRecoveryGraphResult graph_result;
	size_t owned_bytes;
} ClusterRecoveryNativeInventoryDiag;

typedef struct ClusterRecoveryNativeInventory ClusterRecoveryNativeInventory;

/* Borrow the original startup cold scope for two complete physical passes.
 * Retain native effects and original undo WAL with complete owned payloads.
 * Every source (including explicit empty terminals), exact cut, record
 * position and full decoded bytes of every family must agree. The original
 * census validates ROOT/claim/WALR and the physical stream after each pass.
 * Failure/ERROR exposes no partial inventory. Destroy on every caller exit.
 *
 * These are owned observations, not a replay plan or terminal/OPEN/GC
 * authority. Other record families remain separate obligations. The caller
 * retains isolation/retention and revalidates the full graph before use.
 * This calls the original synchronous physical visitors; memory_budget is
 * an owned-allocation bound, not a one-page or elapsed-time bound. */
extern ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_build(ClusterWalInputsV1 *inputs, size_t memory_budget,
										ClusterRecoveryNativeInventory **out,
										ClusterRecoveryNativeInventoryDiag *diag);
/* Same two cold passes, also collecting PAGE/FSM/native storage/SPACE/undo
 * targets into an unstarted root set. Both outputs remain provisional
 * until all source records (including non-native bytes) agree. Failure
 * or ERROR invalidates roots. Root storage has its own caller-set budget;
 * the original reader and one cryptohash context are not inventory storage.
 * Other owners and the full physical/graph re-observation remain required. */
extern ClusterRecoveryNativeInventoryResult cluster_recovery_native_inventory_collect_roots(
	ClusterWalInputsV1 *inputs, ClusterRecoveryRootSet *roots, size_t memory_budget,
	ClusterRecoveryNativeInventory **out, ClusterRecoveryNativeInventoryDiag *diag);
/* Repeat the original complete cold census after dependent graph traversal.
 * Compare to the saved source/cut/all-record transcript, never rebase it.
 * No already-consumed effect is emitted again; failure/ERROR makes this
 * inventory unusable. Success is only input stability, not graph completion.
 * This has the same synchronous-reader bound as build(). */
extern ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_recheck(ClusterWalInputsV1 *inputs,
										  ClusterRecoveryNativeInventory *inventory,
										  ClusterRecoveryNativeInventoryDiag *diag);
/* At most one native or original undo effect per call after both passes.
 * END drains only this observed subset. It never discharges these effects,
 * other_records, historical identity binding or physical roots. */
extern ClusterRecoveryNativeInventoryResult
cluster_recovery_native_inventory_next(ClusterRecoveryNativeInventory *inventory,
									   ClusterRecoveryNativeEffect *out);
/* Join each retained SET_HEAD with its last preceding TT_BIND in the same
 * source/cut, with no intervening segment lifecycle change. Missing or
 * overwritten identity refuses; current TT generations are never guessed.
 * At most 128 saved records per call, without consuming the effect cursor.
 * The graph must be unstarted and outlive this inventory. Failure, failed
 * input recheck or inventory destruction invalidates it. DRAINED only means
 * these heads were added: physical TT/page/native producers, rereads and
 * lifecycle/terminal proofs remain separate, with no apply/OPEN/GC authority.
 * Original graph memory is budgeted separately; no per-record allocation. */
extern ClusterRecoveryGraphResult
cluster_recovery_native_inventory_seed_graph_step(ClusterRecoveryNativeInventory *inventory,
												  ClusterRecoveryReferenceGraph *graph,
												  ClusterRecoveryNativeInventoryDiag *diag);
/* Compare every saved MX CREATE to its original-origin physical candidates.
 * Independent of both effect and TT-seed cursors; no member/range/era proof.
 * Roots must outlive this inventory. Failure/final input drift/destruction
 * revoke them. At most 128 saved records, or one join step, per call. The
 * fixed join budget is separate from the inventory's owned allocation.
 * Physical mismatch is emitted and counted, never silently discharged. */
extern ClusterRecoveryMultixactJoinResult cluster_recovery_native_inventory_multixact_step(
	ClusterRecoveryNativeInventory *inventory, ClusterRecoveryRootSet *roots,
	size_t join_memory_budget, ClusterRecoveryMultixactJoinEntry *out,
	ClusterRecoveryNativeInventoryDiag *diag);
/* Full SHA-256 of every original ordered source, cut, completed tail and
 * all-record transcript (including empty sources/non-native records).
 * Scalars are LE64; fixed arrays are whole; domains include their NUL.
 * Available only on a complete, non-refused inventory. No I/O, cursor reset
 * or authority: the original cold owner must perform its final recheck. */
extern bool
cluster_recovery_native_inventory_digest(const ClusterRecoveryNativeInventory *inventory,
										 uint8 out[32]);
/* Independent complete terminal cursor. At most 128 saved-record inspections
 * per step, with no I/O or new allocation. Exact records retain their full
 * key; legacy COMMIT/ABORT use only the last prior same-source BIND, with
 * INIT/RECYCLE/REUSE barriers. Missing/mismatched bindings and native-only
 * terminals remain explicit unresolved obligations. Every effect and full
 * payload stays owned; none is applied, discarded or deemed completed.
 * Output borrows this inventory and is provisional until the original final
 * scope/TT/ROOT/WAL checks. Refusal is sticky and revokes dependents. */
extern ClusterRecoveryTerminalResult
cluster_recovery_native_inventory_terminal_step(ClusterRecoveryNativeInventory *inventory,
												ClusterRecoveryTerminalEntry *out,
												ClusterRecoveryNativeInventoryDiag *diag);
extern void cluster_recovery_native_inventory_destroy(ClusterRecoveryNativeInventory *inventory);
/* Independent retained terminal/assignment cursor, under the same borrowed
 * ROOT lifetime and failure rules as MX. At most 128 saved records or one
 * bounded physical join step. Mismatches are observations, not permission
 * to overwrite a status or parent. The fixed scratch budget is separate. */
extern ClusterRecoveryXactJoinResult cluster_recovery_native_inventory_xact_step(
	ClusterRecoveryNativeInventory *inventory, ClusterRecoveryRootSet *roots,
	size_t join_memory_budget, ClusterRecoveryXactJoinEntry *out,
	ClusterRecoveryNativeInventoryDiag *diag);
#endif
#endif
