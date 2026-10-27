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
#include "access/clog.h"
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_recovery_multixact_join.h"
#include "cluster/cluster_recovery_xact_join.h"
#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_wal_inputs.h"
#include "common/cluster_relmap_v2.h"
#include "cluster/cluster_relmap_checkpoint.h"

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

/* Whole-retained-input comparison for one exact TT key. A missing or
 * unresolved record is not an abort; agreement is not effect completion. */
typedef struct ClusterRecoveryTerminalSummary {
	ClusterRecoveryTxnKey transaction;
	uint64 exact_records, unresolved_records;
	const ClusterRecoveryNativeRecord *first_exact;
	SCN commit_scn;
	bool has_commit, has_abort, conflict;
} ClusterRecoveryTerminalSummary;

/* Raw carriers for one complete pending image, never a FullXid/outcome proof.
 * All selected generations of the physical origin participate. Source LSNs
 * from different generations are not ordered or treated as interchangeable. */
typedef enum ClusterRecoveryRelmapFoldResult {
	CLUSTER_RECOVERY_RELMAP_FOLD_MORE = 0,
	CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY,
	CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED,
	CLUSTER_RECOVERY_RELMAP_FOLD_INVALID,
	CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED,
	CLUSTER_RECOVERY_RELMAP_FOLD_CAPACITY
} ClusterRecoveryRelmapFoldResult;

#define CLUSTER_RECOVERY_RELMAP_FOLD_HEADER_XID (1U << 0)
#define CLUSTER_RECOVERY_RELMAP_FOLD_WAL_MATCH (1U << 1)
#define CLUSTER_RECOVERY_RELMAP_FOLD_WAL_ALIAS (1U << 2)
#define CLUSTER_RECOVERY_RELMAP_FOLD_COMMIT (1U << 3)
#define CLUSTER_RECOVERY_RELMAP_FOLD_ABORT (1U << 4)
#define CLUSTER_RECOVERY_RELMAP_FOLD_ASSIGNMENT (1U << 5)
#define CLUSTER_RECOVERY_RELMAP_FOLD_PREPARE (1U << 6)
#define CLUSTER_RECOVERY_RELMAP_FOLD_TOP_XID (1U << 7)
#define CLUSTER_RECOVERY_RELMAP_FOLD_SUBXID (1U << 8)
#define CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_ZERO (1U << 9)
#define CLUSTER_RECOVERY_RELMAP_FOLD_CLOG_TRUNCATE (1U << 10)
#define CLUSTER_RECOVERY_RELMAP_FOLD_RECORD_TOP_XID (1U << 11)

/* Owned original selected input, including history and empty alternatives.
 * The flags describe actual complete checkpoint bytes/addresses observed in
 * both original physical passes. A v2 window is an issuance observation, not
 * a record's FullXid era, suffix upper bound, native base or lifetime proof. */
typedef struct ClusterRecoveryRelmapHistorySource {
	ClusterWalInputV1 input;
	uint32 input_index; /* original selector index, not the census's sorted ordinal */
	XLogRecPtr checkpoint_end;
	ClusterRelmapCheckpointWindow window;
	uint64 post_flush_issued;
	bool checkpoint_observed, window_observed;
} ClusterRecoveryRelmapHistorySource;

typedef struct ClusterRecoveryRelmapFoldEntry {
	const ClusterRecoveryNativeRecord *record; /* full original source/cut/payload */
	const ClusterRecoveryRelmapHistorySource *history; /* same owned source, NULL if unavailable */
	ClusterRecoveryNativeKind kind;
	uint32 source_index;
	uint64 record_ordinal; /* original all-family ordinal within this source */
	uint32 flags;
	uint32 subxid_references; /* every occurrence, not an immediate-parent proof */
} ClusterRecoveryRelmapFoldEntry;

typedef struct ClusterRecoveryRelmapFoldSummary {
	uint32 sources, origin_sources; /* includes original explicit empty sources */
	uint64 header_xid_records, record_top_xid_records;
	uint64 wal_matches, wal_aliases;
	uint64 top_commits, top_aborts, assignments, prepares;
	uint64 child_records, child_references;
	uint64 clog_zeroes, clog_truncates;
	/* Same physical origin only. Counts compare the FULL query to the v2
	 * persisted half-space window, never infer an era from a raw carrier.
	 * owner_window_sources additionally matches epoch/admitted/boot. */
	uint32 checkpoint_sources, window_sources, query_window_sources, owner_window_sources;
	/* Every query-page zero and every truncate remains an obligation. Order
	 * only within the UNIQUE exact native map source. Other generations,
	 * missing/duplicate maps or an overlapping record remain unordered.
	 * Exact here means WAL_MATCH's represented fields, not certified FullXid.
	 * Even a before count proves no cutoff era or base retention lifetime. */
	uint64 clog_before_wal, clog_after_wal, clog_unordered;
} ClusterRecoveryRelmapFoldSummary;

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
	uint64 terminal_lookup_examined;
	uint64 relmap_examined; /* saved records plus individual subxid inspections */
	uint64 mx_compared_offsets, mx_compared_members, mx_physical_mismatches;
	uint64 xact_clog_compared, xact_parents_compared, xact_physical_mismatches;
	ClusterRecoveryGraphResult graph_result;
	size_t owned_bytes;
} ClusterRecoveryNativeInventoryDiag;

typedef struct ClusterRecoveryNativeInventory ClusterRecoveryNativeInventory;

typedef struct ClusterRecoveryPagePlanBuildV1 {
	RfPageOnlinePlanV1 *plan;
	RfPageProofDetailV1 detail;
} ClusterRecoveryPagePlanBuildV1;

/* Before PAGE/content locks, build a caller-owned immutable PAGE input graph
 * by rereading every original physical source/cut and comparing its complete
 * saved metadata, tail and transcript. All generations and explicit empty
 * sources remain bound; native redo comes from the same original checkpoint.
 * Non-PAGE records advance full-cut coverage only, never SIDE qualification.
 * memory_budget bounds the additional private build/plan allocation; zero
 * uses RF_PAGE_ONLINE_PLAN_MAX_BYTES. Existing inventory memory is separate.
 * Caller-held LWLocks refuse before allocation or I/O, preserving the input.
 * Destroy the returned plan with rf_page_online_plan_destroy_v1. Retain and
 * revalidate original inventory/scope/native owners before any authority use.
 * Refusal/ERROR after entry invalidates the inventory and frees only private
 * output; it never releases original custody or supplies PAGE/DATA rights,
 * physical-target coverage, discharge, COMPLETE or OPEN. */
extern ClusterRecoveryPagePlanBuildV1 cluster_recovery_native_inventory_page_plan_v1(
	ClusterWalInputsV1 *inputs, ClusterRecoveryNativeInventory *inventory, Size memory_budget);

/* Original complete two-pass observation, including sources with no PAGE
 * records and explicit empty terminals. The transcript covers every record
 * family in the exact full cut. source_index is the physical census/PAGE WAL
 * order, not the input-directory or replay-set index; match complete source
 * identities across those views. By-value metadata grants no replay authority. */
typedef struct ClusterRecoveryWalSourceObservationV1 {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	ClusterWalTailObservation observed;
	uint8 transcript_sha256[32];
	uint32 source_index;
} ClusterRecoveryWalSourceObservationV1;

/* Same complete-inventory/snapshot gate as PAGE inputs; count is the entire
 * original selected set, never the number of sources containing PAGE WAL.
 * No file I/O or cursor reset; inherited snapshot hashing may allocate scratch.
 * Call before PAGE acquisition. Outputs are cleared on refusal. */
extern bool cluster_recovery_native_inventory_source_count_v1(
	ClusterRecoveryNativeInventory *inventory, uint32 *out_count);
extern bool cluster_recovery_native_inventory_source_at_v1(
	ClusterRecoveryNativeInventory *inventory, uint32 index,
	ClusterRecoveryWalSourceObservationV1 *out);

/* Full decoded PageVersion-bearing input, not a contribution-graph verdict.
 * reader owns no files/callbacks and borrows this inventory's immutable copy.
 * All components, payloads and original source generations are retained. */
typedef struct ClusterRecoveryPageWalV1 {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	uint32 source_index;
	uint64 ordinal;
	const XLogReaderState *reader;
} ClusterRecoveryPageWalV1;

/* Complete two-pass observations only. Count zero is a complete empty set,
 * not missing coverage. No I/O or cursor reset; snapshots are checked before
 * exposure. at() clears out on refusal/out-of-range. Borrowed readers expire
 * with the inventory. Original full input and physical rechecks, codec/image
 * validation and native custody remain necessary; no replay/PAGE/OPEN grant. */
extern bool cluster_recovery_native_inventory_page_count_v1(
	ClusterRecoveryNativeInventory *inventory, uint32 *out_count);
extern bool cluster_recovery_native_inventory_page_at_v1(
	ClusterRecoveryNativeInventory *inventory, uint32 index, ClusterRecoveryPageWalV1 *out);

/* Complete generation inputs from the original declared relmap episode.
 * source.selected remains the full original selector value; census ordinal
 * and source.input_index need not agree. Flags describe observed bytes,
 * never a record's FullXid era, CLOG lifetime or terminal qualification. */
typedef struct ClusterRecoveryRelmapHistoryEntryV1 {
	ClusterRecoveryRelmapSourceV1 source;
	ClusterRecoveryWalSourceObservationV1 observation;
	XLogRecPtr checkpoint_end;
	ClusterRelmapCheckpointWindow window;
	uint64 post_flush_issued;
	bool checkpoint_observed, window_observed;
} ClusterRecoveryRelmapHistoryEntryV1;

/* Actual original-source/base observations. Full raw pending obligations stay
 * visible; IN_PROGRESS and SUB_COMMITTED are never promoted to terminals. */
typedef struct ClusterRecoveryRelmapPendingBaseV1 {
	ClusterRecoveryRelmapHistoryEntryV1 history;
	ClusterRecoveryRelmapFoldSummary fold;
	ClusterRelmapClogBaseV1 native;
} ClusterRecoveryRelmapPendingBaseV1;

typedef struct ClusterRecoveryRelmapHistory ClusterRecoveryRelmapHistory;

/* Borrow the same all-replay IR/WALR scope, not a new per-origin owner.
 * The budget reserves this handle, the complete two-pass inventory and one
 * fixed original native base custody (charged once actually allocated).
 * Missing/legacy windows stay explicit unqualified inputs. Entered refusal
 * or ERROR revokes the original episode, retaining its protections/debt.
 * Destroy local custody before the original scope/ResourceOwner ends.
 * No native allocation, CLOG truncation, COMPLETE, OPEN or release right. */
extern ClusterRecoveryNativeInventoryResult cluster_recovery_relmap_history_build_v1(
	const ClusterRecoveryRelmapSourceV1 *source, size_t memory_budget,
	ClusterRecoveryRelmapHistory **out,
	ClusterRecoveryNativeInventoryDiag *diag);
/* One original generation in census order, including empty/history sources.
 * Complete cut/tail/all-record transcript is copied from the SAME inventory,
 * not a PAGE or native-map prefix. source.input_index and observation's
 * source_index identify separate views; join them by complete generation.
 * No I/O/cursor changes. Current checks bracket publication of copied bytes.
 * Invalid index/overlapping output is a precondition refusal; other failure
 * is sticky. All outputs remain provisional until the full final recheck. */
extern bool cluster_recovery_relmap_history_source_v1(ClusterRecoveryRelmapHistory *history,
	uint32 ordinal, ClusterRecoveryRelmapHistoryEntryV1 *out);
/* Reread every original physical input and full selected metadata, comparing
 * to the saved baseline under the same original episode. Never rebase.
 * Also reread any bound native base on its SAME held fd/full-page baseline.
 * One full synchronous census plus native base read, not a bounded tick. */
extern ClusterRecoveryNativeInventoryResult cluster_recovery_relmap_history_recheck_v1(
	ClusterRecoveryRelmapHistory *history, ClusterRecoveryNativeInventoryDiag *diag);
/* Fold one complete canonical pending image under this SAME original episode.
 * At most128 saved record/child inspections plus two complete saved-source
 * metadata comparisons per tick; no I/O, allocation, new IR or release.
 * ENTRY borrows the history's immutable record and selected metadata until
 * local destroy; MORE/refusal clears entry/summary. Buffers must be disjoint
 * from each other, the query and all borrowed history storage.
 * Ordinary caller-lock/output preconditions do not revoke custody. Once
 * entered, any input/query drift or ERROR is sticky and fails the original
 * episode, retaining protections/debt. Full final physical recheck remains
 * mandatory. Once a native base is bound, only the same drained query may
 * be reread; a new query requires separate local custody under the original
 * episode. DRAINED is raw correspondence, never era/lifetime/terminal,
 * COMPLETE, OPEN, native allocation/CLOG truncate or release authority. */
extern ClusterRecoveryRelmapFoldResult cluster_recovery_relmap_history_pending_step_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapFoldEntry *out, ClusterRecoveryRelmapFoldSummary *summary,
	ClusterRecoveryNativeInventoryDiag *diag);
/* Only after a complete pending fold drains: select its unique exact native
 * map source and actual v2 owner/query window internally, then read the
 * original CLOG base through the original held-fd API. Buffers must be
 * exclusive/disjoint from query and borrowed history storage. No caller CF,
 * native lock or critical section. Checked pre-drain/output/CF/critical
 * preconditions do not poison; entered refusal/ERROR does, retaining pins/debt and
 * any held native custody until explicit local destroy. Query is thereafter
 * fixed; repeated/final reads compare the SAME full page, never rebase.
 * This is synchronous native I/O, not a bounded metadata tick, birth-era,
 * no-reuse/truncation/lifetime, native/SIDE terminal or COMPLETE/OPEN/release
 * authority. Destroy before the original scope/ResourceOwner ends. */
extern bool cluster_recovery_relmap_history_pending_base_v1(
	ClusterRecoveryRelmapHistory *history, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapPendingBaseV1 *out, ClusterRecoveryNativeInventoryDiag *diag);
extern void cluster_recovery_relmap_history_destroy_v1(ClusterRecoveryRelmapHistory **history);

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
/* After terminal_step drains, compare all saved terminal edges to key.
 * At most128 saved records per call; MORE clears out and pins this query key
 * until ENTRY. A changed key or invalid input refuses and revokes dependents.
 * ENTRY is a complete comparison (including zero matches), never terminal
 * authority. Same-origin/xid unbound carriers remain unresolved even if exact
 * records agree. Each original record/source/cut/payload remains available;
 * no ordering across source LSNs or physical status overrides are inferred.
 * No I/O, allocation or independent cursor reset. Final full inputs and
 * physical TT/ROOT rereads remain mandatory through the caller's lifetime. */
extern ClusterRecoveryTerminalResult cluster_recovery_native_inventory_terminal_lookup_step(
	ClusterRecoveryNativeInventory *inventory, const ClusterRecoveryTxnKey *key,
	ClusterRecoveryTerminalSummary *out, ClusterRecoveryNativeInventoryDiag *diag);
/* Independent complete pending-image query. At most 128 saved-record/child
 * inspections per call, no I/O/allocation and no consumption of other cursors.
 * ENTRY borrows one original carrier; only DRAINED exports the complete summary.
 * MORE/refusal clears both outputs. Canonical full pending bytes are fixed
 * through DRAINED; a later query may then start. Invalid/query drift or failed
 * final input recheck poisons this inventory and its dependent observations.
 * RECORD_TOP_XID retains the native top-level label on child-xid carriers.
 * WAL_MATCH checks full native map bytes, namespace, raw xid, record end and
 * represented source incarnation only. Membership/boot/FullXid era, reuse,
 * checkpoint/base lifetime, native/SIDE agreement and terminal qualification
 * are NOT encoded by that native carrier. Raw terminals and every same-origin
 * CLOG truncate remain explicit obligations, including historical sources.
 * Original episode/physical holders and final whole-input checks must remain
 * current; DRAINED is no COMPLETE, status override, release or OPEN authority. */
extern ClusterRecoveryRelmapFoldResult cluster_recovery_native_inventory_relmap_step(
	ClusterRecoveryNativeInventory *inventory, const ClusterRelmapV2Image *pending,
	ClusterRecoveryRelmapFoldEntry *out, ClusterRecoveryRelmapFoldSummary *summary,
	ClusterRecoveryNativeInventoryDiag *diag);
/* Copy one original owned source in census order, including empty/history
 * inputs; input_index retains the distinct original selector order.
 * No I/O/allocation or cursor movement. Distinct caller-owned output only;
 * refusal clears it. Its lifetime and final whole-scope/physical rereads are
 * still the original episode's duty. This getter grants no COMPLETE, native
 * base/no-reuse/history qualification, status override, release or OPEN.
 * The existing V1 inventory digest stays source/cut/all-record transcript;
 * it does not independently bind this selected window/metadata. Consumers
 * must retain these full values and perform the original final recheck. */
extern bool
cluster_recovery_native_inventory_relmap_source(const ClusterRecoveryNativeInventory *inventory,
												uint32 index,
												ClusterRecoveryRelmapHistorySource *out);
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
