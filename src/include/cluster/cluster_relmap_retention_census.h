/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_CENSUS_H
#define CLUSTER_RELMAP_RETENTION_CENSUS_H

#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_relmap_pending.h"
#include "cluster/cluster_relmap_pending_side.h"
#include "cluster/cluster_relmap_retention_restore.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRelmapRetentionCensus ClusterRelmapRetentionCensus;

typedef enum ClusterRelmapCensusResult {
	CLUSTER_RELMAP_CENSUS_MORE = 0,
	CLUSTER_RELMAP_CENSUS_DRAINED,
	CLUSTER_RELMAP_CENSUS_INVALID,
	CLUSTER_RELMAP_CENSUS_UNAVAILABLE,
	CLUSTER_RELMAP_CENSUS_CHANGED,
	CLUSTER_RELMAP_CENSUS_CAPACITY
} ClusterRelmapCensusResult;

/* A copied committed image, never the pending slot or a terminal proof. */
typedef struct ClusterRelmapCensusCommitted {
	ClusterRelmapV2Namespace identity;
	uint64 generation;
	RelMapFile map;
	bool present;
} ClusterRelmapCensusCommitted;

/* Borrow the original isolated, OBSERVED ROOT set, not the configured path.
 * Every supplied original-origin directory must be exclusive, unpublished and
 * BUILDING, with native/SIDE floor/last-issued and the complete origin set
 * already qualified by the original cold owner. Copy the pointer array only;
 * keep roots and directories alive and exclusively owned until destroy.
 * The caller's memory budget covers this fixed scratch cursor.
 *
 * This authenticates neither those qualifications nor ROOT/isolation/retention.
 * Any refusal invalidates roots and all directories in a valid bounded input
 * array, preserving pins. An oversized array is never accessed. */
extern ClusterRelmapCensusResult cluster_relmap_retention_census_begin(
	ClusterRecoveryRootSet *roots, uint64 system_identifier, uint64 database_incarnation,
	const uint8 storage_uuid[16], ClusterRelmapRetentionDirectory *const directories[],
	size_t count, size_t memory_budget, ClusterRelmapRetentionCensus **out);

/* At most128 metadata candidates, one <=BLCKSZ original file read, or one
 * existing whole-ROOT recheck step. Require global/base and both exact v2
 * copies for global and every canonical physical base/db directory, including
 * namespaces with no WAL reference. Restore backup-only pending to its original
 * origin. Missing, degraded, conflicting or changed input refuses, never empty.
 *
 * DRAINED requires the original entire ROOT set to pass recheck unchanged.
 * Directories remain BUILDING: no terminal, COMPLETE publication, truncate,
 * CTRC release, allocation or OPEN permission. Destroy before the original
 * owner performs its remaining qualification/publication steps. */
extern ClusterRelmapCensusResult
cluster_relmap_retention_census_next(ClusterRelmapRetentionCensus *census);

/* Same cursor and bounded work as next. Clear output on every call, then set
 * present only on the MORE tick that finishes a namespace's complete legal
 * pair AND restores any pending obligation to its original-origin directory.
 * Copies the selected committed image and exact namespace/generation; no
 * configured-path reread, pending image, TT binding or release permission.
 * NULL output discards that tick's image, as the legacy next API does.
 *
 * Every emitted image remains provisional until this cursor returns DRAINED
 * after the whole ROOT recheck. On any refusal, the caller must discard ALL
 * images from this cursor, including earlier copies; output clearing cannot
 * revoke copies already held by the caller. DRAINED still leaves directories
 * BUILDING and the original cold owner's remaining qualifications unchanged.
 * Use this API throughout a scan when every namespace image is required. */
extern ClusterRelmapCensusResult cluster_relmap_retention_census_next_committed(
	ClusterRelmapRetentionCensus *census, ClusterRelmapCensusCommitted *out);

/* Rebuild structural pending from the actual pair behind the last committed
 * emission, without a configured-path reread or a caller-constructed view.
 * Usable once, immediately after next_committed returned MORE with present;
 * any next call (including legacy/NULL-output next) expires this window.
 * No current emission returns NOT_READY without changing the cursor or set.
 *
 * The original cold owner must supply this namespace's complete, qualified
 * DROP/TRUNCATE descriptors from its exact stream. Nothing is inferred from
 * the pending image. The destination must be freshly initialized and empty;
 * each entry represents MAIN/relNumber obligations with all proof bits clear.
 * Descriptor refusal poisons the census, preserving every retention pin and
 * leaving the destination unchanged. Borrowed qualification drift does too.
 * Work is bounded by the origin limit and pending capacity; no I/O is added.
 *
 * This set, like the emitted map, is provisional until whole-ROOT DRAINED.
 * The caller must discard ALL earlier sets/maps on any later census refusal.
 * It supplies no physical completion, native/TT/SPACE proof, COMPLETE, floor,
 * issued/nonreuse qualification, release, allocation or activation authority. */
extern ClusterRelmapPendingResult cluster_relmap_retention_census_rebuild_pending(
	ClusterRelmapRetentionCensus *census, ClusterRelmapPendingSet *pending_out,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count);

struct ClusterWalInputsV1;

/* Same single-use window and provisional structural result. When either copy
 * retains pending, validate that exact union image through the caller's held
 * original cold WAL scope before rebuilding. Use the pending copy's original
 * committed base even if primary has already published the new generation.
 * The retained visitor scans ALL selected sources and rechecks its ROOT/pins.
 * A clean pair has no pending WAL assertion to check and uses the structural
 * rebuild directly; this does not prove WAL/source coverage for a clean pair.
 *
 * The caller still qualifies the complete descriptor selection, origin set,
 * FullXid era, membership epoch, boot incarnation and floor/issued inputs.
 * Refusal (including WAIT) invalidates the census, retaining pins and leaving
 * the destination unchanged. ERROR does the same before rethrow; the caller
 * owns and must release its original WAL scope on every exit. This synchronous
 * retained scan is not a bounded next_committed tick. Whole physical ROOT
 * DRAINED, BUILDING and all independent owner proof requirements still apply. */
extern ClusterRelmapPendingResult cluster_relmap_retention_census_rebuild_pending_wal(
	ClusterRelmapRetentionCensus *census, ClusterRelmapPendingSet *pending_out,
	struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count);

/* Same single-use window and provisional result, using the original sealed
 * SIDE collector rather than caller-constructed descriptors. The census owns
 * the exact namespace, storage UUID and legal committed pair used together.
 * The caller still supplies ALL selected original plans/participants and owns
 * their final retained-source recheck; this does not certify that selection.
 * Missing input is not an explicitly empty sealed stream. Any SIDE refusal
 * invalidates the whole census, preserving pins and the unchanged destination.
 * This synchronous, read-only call scans retained plan operations (including
 * history) for the current namespace, bounded by the original plan limits and
 * pending capacity; it adds no I/O and is not a next_committed work tick.
 * DRAINED and all independent owner qualifications remain necessary. */
extern ClusterRelmapPendingResult cluster_relmap_retention_census_rebuild_pending_side(
	ClusterRelmapRetentionCensus *census, ClusterRelmapPendingSet *pending_out,
	const ClusterRelmapPendingSideSource *sources, size_t source_count);

/* Interrupted scans invalidate the borrowed build and ROOT set, retaining
 * every restored pin. No directory, pin or ROOT ownership is freed here. */
extern void cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census);
#endif
#endif
