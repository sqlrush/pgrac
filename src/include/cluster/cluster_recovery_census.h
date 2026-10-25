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
#include "cluster/cluster_recovery_page_roots.h"
#include "cluster/cluster_recovery_relation_census.h"
#include "cluster/cluster_recovery_multixact_scan.h"
#include "cluster/cluster_recovery_slru_scan.h"
#include "cluster/cluster_recovery_relmap_scan.h"

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
	CLUSTER_RECOVERY_CENSUS_PAGE_ROOTS,
	CLUSTER_RECOVERY_CENSUS_TX,
	CLUSTER_RECOVERY_CENSUS_TX_TARGETS,
	CLUSTER_RECOVERY_CENSUS_RELATIONS,
	CLUSTER_RECOVERY_CENSUS_SLRU_PHYSICAL,
	CLUSTER_RECOVERY_CENSUS_MX_PHYSICAL,
	CLUSTER_RECOVERY_CENSUS_MX_COMPARE,
	CLUSTER_RECOVERY_CENSUS_XACT_COMPARE,
	CLUSTER_RECOVERY_CENSUS_TERMINALS,
	CLUSTER_RECOVERY_CENSUS_TT_TERMINALS,
	CLUSTER_RECOVERY_CENSUS_RELMAP_PHYSICAL,
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
	CLUSTER_RECOVERY_CENSUS_SLRU_SLOT,
	CLUSTER_RECOVERY_CENSUS_XACT_MATCH,
	CLUSTER_RECOVERY_CENSUS_RELMAP,
	CLUSTER_RECOVERY_CENSUS_TERMINAL,
	CLUSTER_RECOVERY_CENSUS_TT_TERMINAL
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

/* Raw join to the original physical TT. A reused/missing slot and unequal
 * terminal bytes remain obligations; even equality grants no apply/OPEN. */
typedef struct ClusterRecoveryCensusTerminal {
	ClusterRecoveryTerminalEntry terminal;
	TTSlot physical;
	uint32 physical_generation;
	bool physical_present, identity_matches, raw_terminal_matches;
} ClusterRecoveryCensusTerminal;

/* Revisit all physical slots, including those with no retained terminal.
 * Invalid/empty raw identities remain explicit; no loser is inferred. */
typedef struct ClusterRecoveryCensusTTTerminal {
	ClusterRecoveryUndoEntry physical;
	ClusterRecoveryTerminalSummary retained;
	bool key_usable, raw_terminal_matches;
} ClusterRecoveryCensusTTTerminal;

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
		ClusterRecoveryXactJoinEntry xact_compare;
		ClusterRecoveryRelmapEntry relmap;
		ClusterRecoveryCensusTerminal terminal;
		ClusterRecoveryCensusTTTerminal tt_terminal;
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
	ClusterRecoveryRelmapResult relmap_result;
	ClusterRecoveryMultixactJoinResult join_result;
	ClusterRecoveryXactJoinResult xact_result;
	ClusterRecoveryNativeInventoryResult inventory_result;
	ClusterRecoveryTerminalResult terminal_result;
	ClusterRecoveryNativeInventoryDiag inventory;
	ClusterRecoveryPageRootsDiag page_roots;
	uint64 tt_slots, tx_records, relation_entries, mx_entries, mx_comparisons, effects;
	uint64 slru_entries;
	uint64 xact_comparisons;
	uint64 relmap_entries;
	uint64 terminal_entries, terminal_missing, terminal_identity_mismatches,
		terminal_state_mismatches;
	uint64 tt_terminal_slots, tt_terminal_missing, tt_terminal_unresolved, tt_terminal_conflicts;
	uint64 tx_targets, tx_target_missing, tx_target_short, tx_space_missing;
} ClusterRecoveryCensusDiag;

/* Value-owned identity of the complete observation, not a semantic seal.
 * Full digests bind the original ordered inputs and physical ROOT union.
 * Unowned effects remain obligations even when all counts/digests agree. */
typedef struct ClusterRecoveryCensusManifest {
	uint32 encoding_version;
	uint32 source_count, root_count, tx_record_count;
	uint64 wal_record_count, native_effect_count;
	uint64 other_wal_record_count, unowned_target_record_count;
	uint8 inputs_sha256[32], roots_sha256[32], observation_sha256[32];
} ClusterRecoveryCensusManifest;

/* Raw current TT observations in the original retained work order. A zero
 * count is a valid complete empty view, distinct from missing observation.
 * Borrow only while the original Start/cursor/scope and all caller isolation
 * and freeze premises live. Identity matching is not terminal/history,
 * canonical transaction, semantic coverage, apply or release authority. */
typedef struct ClusterColdCtrcPhysicalV1 {
	uint32 count;
	ClusterRecoveryUndoEntry entries[FLEXIBLE_ARRAY_MEMBER];
} ClusterColdCtrcPhysicalV1;

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

/* Original CTRC scope entry: obtain its selected namespace instead of accepting
 * a caller FD. Borrow scope and descriptor until destroy. Every next/manifest
 * checks the original scope/work/namespace binding; failure revokes the cursor.
 * The original caller must still hold isolation/retention/freeze. This is an
 * observation component, not a service driver, semantic seal or write grant. */
extern ClusterRecoveryCensusResult cluster_recovery_census_begin_ctrc(
	ClusterWalInputsV1 *inputs, size_t memory_budget, ClusterRecoveryCensus **out);

/* Run the existing ROOT/WAL union, all TT slots, physical PAGE/history root
 * discovery and seeded TX records. Discovery adds exact canonical-base roots
 * before the TX walk; deferred bindings/structures remain obligations for
 * the unchanged subsequent observed-graph PAGE/history consumers. Then run
 * all TX record target/SPACE candidates, relation/page/history, raw physical
 * CLOG/SUBTRANS/commit_ts/MX, retained MX/terminal/assignment comparisons,
 * physical native relmap copies and every mapped fork edge, and native
 * effects. Primary/backup map observations never choose an authority or
 * complete publication; missing fork edges remain explicit obligations.
 * Every retained terminal also joins its exact or same-source BIND identity
 * to the original physical TT; unresolved identities and reused/status/SCN
 * differences remain explicit. No terminal record overrides another. Every
 * physical TT slot is then revisited against the complete retained terminal
 * set: absent, unbound and conflicting endpoints remain distinct obligations.
 * Even exact raw agreement grants no transaction/effect completion.
 * Transaction comparisons retain raw status/parent differences and grant
 * no era, terminal or complete parent-chain proof.
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
/* Available only after the original final TT/whole ROOT/full WAL rereads.
 * Revalidate the live original scope and dependent owners before copying.
 * False clears out; failures remain sticky. This does not repeat the DATA
 * or WAL census; caller isolation/retention must last through every use.
 * The value grants no recovery-completion/apply/terminal/OPEN/GC authority. */
extern bool cluster_recovery_census_manifest(ClusterRecoveryCensus *census,
											 ClusterRecoveryCensusManifest *out);
extern bool cluster_recovery_census_diag(const ClusterRecoveryCensus *census,
										 ClusterRecoveryCensusDiag *out);
extern void cluster_recovery_census_destroy(ClusterRecoveryCensus *census);
#endif
#endif
