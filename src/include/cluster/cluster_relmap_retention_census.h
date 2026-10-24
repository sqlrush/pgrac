/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_CENSUS_H
#define CLUSTER_RELMAP_RETENTION_CENSUS_H

#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_relmap_pending.h"
#include "cluster/cluster_relmap_pending_side.h"
#include "cluster/cluster_relmap_retention_restore.h"
#include "cluster/cluster_relmap_recovery.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_relmap_authority.h"
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

/* Caller-owned arbitration input, published only after whole-ROOT DRAINED.
 * The full view is the original physical pair, not a caller-built image;
 * handle authenticates it through the original native cold scope. */
typedef struct ClusterRelmapCensusRecovery {
	ClusterRelmapV2Namespace identity;
	ClusterRelmapV2View pending;
	ClusterRelmapRecoveryHandle *handle;
} ClusterRelmapCensusRecovery;

/* Dormant DRAINED-only prepare. Select global for InvalidOid, otherwise the
 * canonical base/dbid namespace in this original ROOT set. Re-read both exact
 * observed copies and require their pending's already-restored origin pin;
 * revalidate every original WAL source's system/incarnation/storage UUID;
 * prepare opaque native custody and recheck borrowed ROOT/origin state before
 * publishing. This synchronous operation is not a bounded census tick.
 *
 * Before DRAINED returns false without consuming the scan. Any actual input
 * refusal/ERROR invalidates the census and its directories, retaining pins.
 * Fresh distinct caller output is cleared on refusal; ERROR propagates after
 * releasing only local custody. Release output.handle on every later exit.
 * Keep this census, its original ROOT/directories and the native inputs alive
 * and exclusively owned until that handle is released or consumed. Original
 * cold-owner isolation/cut qualifications are not established by this join.
 * No target write, terminal decision, COMPLETE, release or activation follows.
 * All census maps/sets are still observations; do not adopt them after a later
 * arbitration mutates the observed authority files. */
extern bool cluster_relmap_retention_census_recovery_prepare(ClusterRelmapRetentionCensus *census,
															 struct ClusterWalInputsV1 *inputs,
															 Oid dbid,
															 ClusterRelmapCensusRecovery *out);

/* Dormant original execution-owner prepare after pass-1 read pins are gone.
 * Require DRAINED and the original sole-merger claim/namespace X/shared
 * catalog. Select the exact original physical pair and restored pending pin;
 * use only the original cold execution prepare, then match its original
 * source/sysid/dbinc/storage/serial. After native I/O, freshly recheck the
 * entire physical ROOT and every original pin before one output publication.
 * No arbitration, target write, COMPLETE, pin release or activation follows.
 *
 * Missing initial ownership/before DRAINED leaves the scan unconsumed. Entered
 * refusal/ERROR invalidates old ROOT/census/directories and retains all pins;
 * only local custody is freed. False is not rollback or retry permission.
 * Output must be fresh exclusive storage, disjoint from all borrowed objects;
 * known census/directory overlaps or an output spanning the ROOT pointer stay
 * unchanged. Distinct refusal clears output. Original isolation and complete
 * origin/native qualification remain caller obligations, never established by
 * this join. Keep census/ROOT/directories/original execution owner alive until
 * the returned handle is released or consumed. */
extern bool
cluster_relmap_retention_census_recovery_prepare_cold(ClusterRelmapRetentionCensus *census,
													  Oid dbid, ClusterRelmapCensusRecovery *out);

/* Dormant original-owner consumer: while holding the original sole-merger
 * claim and namespace X, prepare the selected DRAINED namespace above, then
 * consume its opaque handle through original v2 arbitration. No supplied
 * outcome, map or terminal boolean is accepted. Missing locks/master switch
 * or an unfinished scan returns UNKNOWN without consuming that scan.
 *
 * EVERY attempted arbitration invalidates the old census/ROOT/directories,
 * on success, UNKNOWN and ERROR alike: target bytes may already have changed.
 * All original pins remain. Re-observe under original recovery ownership;
 * never adopt old maps/sets or treat false as rollback/automatic retry.
 * CLEARED_POSTREAD is only this pair's result, not directory COMPLETE,
 * release, global floor/issued, live activation or whole-cold completion. */
extern ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate(ClusterRelmapRetentionCensus *census,
												   struct ClusterWalInputsV1 *inputs, Oid dbid);

/* Dormant original execution-owner single-namespace consumer, after pass-1
 * read pins are gone. Under original merger/X/shared-catalog ownership, use
 * prepare_cold and its fresh whole physical ROOT check, then recheck original
 * source/serial/metadata and every pin before native arbitration. Revoke old
 * census/ROOT/directory observations BEFORE that first native call; original
 * native authority/pins survive. Check identical original source/metadata/pins
 * and ownership after it, without qualifying the now-UNKNOWN directories.
 *
 * Missing initial ownership/before DRAINED leaves the scan unconsumed. Every
 * entered refusal/ERROR invalidates old observations and frees local custody
 * only. ERROR propagates; prior target writes may already be durable. All pins
 * remain, with no rollback/retry or scope replacement. CLEARED_POSTREAD means
 * only this pending pair cleared, never COMPLETE/release, whole-origin/native
 * qualification, allocator/G_ref/owner completion, D0 discharge or activation.
 * Fresh physical re-observation into new storage remains the original owner's
 * responsibility. No old map/set/directory or consumed handle may be reused. */
extern ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate_cold(ClusterRelmapRetentionCensus *census,
														Oid dbid);

/* Original execution-owner follow-on, once after that consumer returns
 * CLEARED_POSTREAD. Retain its exact source/full control ROOT/redo/serial and
 * every old pin; require original merger/X/shared-catalog ownership. Under
 * that same owner, synchronously build a distinct OBSERVED ROOT using the
 * original held-fd two-pass producer. Recheck source, all UNKNOWN directory
 * metadata/pins and ownership before and after physical I/O. UNKNOWN is a
 * required old state, never floor/lifetime or native qualification.
 *
 * Missing initial ownership leaves the follow-on unconsumed. Once entered,
 * success/refusal/ERROR consumes it: no automatic retry, scope replacement or
 * old observation revival. UNKNOWN/ERROR arbitration provides no follow-on.
 * All old observations stay invalid; all pins/debt remain. Refusal/ERROR frees
 * only new local storage; ERROR propagates. Output is published only after
 * complete observation and final original checks. Fresh exclusive output;
 * known census/directory/ROOT-pointer overlap refuses unchanged, distinct
 * refusal sets *out NULL. root_budget covers the new ROOT producer only.
 *
 * Keep the original owner, held fd/cuts/pins and old census/directories alive
 * through the new cursor's lifetime. Destroy it on every exit, including
 * PG_FINALLY. This returns only candidate observation, never fresh qualified
 * census publication, all-origin/native completion, COMPLETE/release, G_ref,
 * D0 discharge, allocator or activation authority. */
extern bool cluster_relmap_retention_census_reobserve_cold(ClusterRelmapRetentionCensus *census,
														   size_t root_budget,
														   ClusterRecoveryRootSet **out);

/* Original-owner restart into new local BUILDING storage, once after the
 * same successful consumer. Only a previous complete single-origin input set
 * matching that consumer's original source is supported. Original native /
 * SIDE / history / lifetime qualifications remain the caller's preconditions;
 * this authenticates checkpoint provenance and unchanged bounds only.
 *
 * Consume the follow-on before allocation or I/O. Use the original execution
 * checkpoint producer for a fresh directory; require its original serial and
 * identical floor / actual-issued bounds. Recheck old source / metadata / all
 * pins and ownership around that I/O and fresh complete ROOT observation.
 * Publish a new census only after all checks. Missing initial ownership or
 * unsupported multiple origins leaves the follow-on unconsumed. Known output
 * aliases stay unchanged; distinct refusal sets *out NULL. Entered refusal /
 * ERROR consumes it, frees only new local storage and preserves all old pins.
 * memory_budget covers the new census plus directory, root_budget its ROOT.
 *
 * The new census owns its new ROOT and directory; destroy it on every exit,
 * before the previous census. Keep the original owner / source / fd / cuts /
 * previous directories and pins alive, unchanged and exclusively held through
 * the new census. Its images / sets remain provisional until whole-ROOT
 * DRAINED; even then the new directory stays BUILDING. No COMPLETE, old pin
 * retirement, mixed-origin qualification, G_ref / owner completion, D0,
 * allocator / live activation or replacement recovery scope is granted. */
extern bool cluster_relmap_retention_census_restart_cold(ClusterRelmapRetentionCensus *census,
														 size_t root_budget, size_t memory_budget,
														 ClusterRelmapRetentionCensus **out);

/* Opaque, local-only preparation of every pending namespace in the original
 * DRAINED physical census, including backup-only pending. No caller namespace
 * list is accepted. Hold original merger claim and namespace X across this
 * synchronous operation and all later uses; keep census/ROOT/directories and
 * inputs alive and exclusively owned. The budget bounds this batch storage,
 * not native prepare's separately owned allocations or synchronous I/O.
 *
 * Re-read every exact pair, prepare every selected original native handle,
 * recheck the whole ROOT and all selected pins/sources, then publish once.
 * Clean namespaces need no handle. Any late refusal/ERROR frees all local
 * handles, invalidates old observations and retains every pin. Before
 * DRAINED or without original ownership, leave the scan unconsumed.
 * A successful (possibly empty) batch is preparation only: no arbitration,
 * target write, directory COMPLETE, global floor/issued or release follows.
 * This local container is not complete semantic G_ref/owner completion.
 * Release cancels only this container/custody; never releases original pins. */
typedef struct ClusterRelmapCensusRecoveryBatch ClusterRelmapCensusRecoveryBatch;
extern bool cluster_relmap_retention_census_recovery_prepare_all(
	ClusterRelmapRetentionCensus *census, struct ClusterWalInputsV1 *inputs, size_t memory_budget,
	ClusterRelmapCensusRecoveryBatch **out);
/* Execution-owner variant after pass-1 read pins are gone. Only an original
 * complete single-origin census is supported. Prepare every actual pending
 * pair through that same original cold source/full control ROOT/redo/serial;
 * retain ALL original pins, including unused slots. No namespace list, new
 * read pin or scope is accepted. A final whole physical ROOT pass follows
 * all native preparations before one opaque output publication.
 *
 * Unsupported multiple origins/missing initial ownership/before DRAINED
 * leave the scan unconsumed. Empty pending refuses, never cold completion.
 * Known census/directory/ROOT-pointer aliases stay unchanged; distinct false
 * clears output. Entered refusal/ERROR frees every new handle, invalidates
 * old observations and preserves original pins/debt; ERROR propagates.
 * Budget covers local batch/provenance/items, not native handle allocations.
 * Original complete qualifications and all borrowed lifetimes above remain
 * caller obligations. No arbitration/COMPLETE/release/live authority follows. */
extern bool
cluster_relmap_retention_census_recovery_prepare_all_cold(ClusterRelmapRetentionCensus *census,
														  size_t memory_budget,
														  ClusterRelmapCensusRecoveryBatch **out);
extern void
cluster_relmap_retention_census_recovery_batch_release(ClusterRelmapCensusRecoveryBatch **batch);

/* Consume the original prepared set once under the same borrowed lifetimes.
 * Always clear *batch and free all local custody, including refusal/ERROR.
 * Missing initial shared-catalog/merger/X ownership leaves observations alone.
 * Otherwise recheck the whole original ROOT, sources and exact pins before
 * targets. Revoke physical census/ROOT/directory observations BEFORE the first
 * native arbiter call; native cold authority and all original pins survive.
 * Check original ownership/source/origin metadata and every saved pin across
 * each call. Stop on UNKNOWN/ERROR: earlier target writes may be durable and
 * must not be rolled back or automatically retried. ERROR propagates.
 * A valid empty batch performs no target write or observation invalidation.
 * CLEARED_POSTREAD concerns only these pairs, never directory COMPLETE,
 * global floor/issued, G_ref/owner completion, pin release or activation. */
extern ClusterRelmapV2ClearResult cluster_relmap_retention_census_recovery_arbitrate_all(
	ClusterRelmapCensusRecoveryBatch **batch);
/* Consume only the execution-owner batch once. Fresh exclusive custody
 * pointer, disjoint from every borrowed object and batch storage; known
 * aliases refuse unchanged. Wrong retained/execution entry cancels local
 * custody only. Missing initial ownership also leaves observations alone.
 * Otherwise freshly recheck whole physical ROOT and original source/full
 * control ROOT/redo/serial/metadata/ALL pins. Revoke old observations before
 * first native attempt; recheck original source/metadata/pins/ownership
 * before/after EVERY normal native return, including UNKNOWN. Stop on the
 * first UNKNOWN/ERROR, release all local custody; old pins/debt remain.
 *
 * Only all native CLEAR plus final original checks and normal cleanup enable
 * the existing one-shot reobserve_cold/restart_cold follow-on. Earlier target
 * writes may be durable; refusal/ERROR permits no retry or scope replacement.
 * This grants no directory COMPLETE, native/all-origin/G_ref/owner completion,
 * old pin retirement, D0, allocator/live or v2 activation. Original owner/
 * source/cuts/fd/census/directories/pins outlive batch and every follow-on. */
extern ClusterRelmapV2ClearResult cluster_relmap_retention_census_recovery_arbitrate_all_cold(
	ClusterRelmapCensusRecoveryBatch **batch);

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

/* Require both original sealed SIDE extraction and the retained native WAL
 * check before consuming ONE committed emission window. No structural set is
 * installed if either refuses. The SIDE and WAL APIs above cannot be called
 * sequentially: each successful rebuild consumes that same private window.
 *
 * For every pair, the original retained scan also matches ALL supplied
 * sealed physical participants to its actual selected full claims and cuts.
 * Clean pairs still require full structural correspondence: DROP/TRUNCATE
 * obligations are independent of whether relmap has a pending generation.
 * The original cold owner supplies immutable plans/inputs and the held scope;
 * full capture/operation equivalence, redo eligibility, final SIDE revalidation
 * and independent era/epoch/boot/floor/issued qualifications remain its duty.
 * Whole physical ROOT DRAINED, provisional results, BUILDING and ERROR/scope
 * ownership follow the APIs above; no completion or release. */
extern ClusterRelmapPendingResult cluster_relmap_retention_census_rebuild_pending_side_wal(
	ClusterRelmapRetentionCensus *census, ClusterRelmapPendingSet *pending_out,
	struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingSideSource *sources, size_t source_count);

/* Interrupted scans invalidate the borrowed build and ROOT set, retaining
 * every restored pin. No directory, pin or ROOT ownership is freed here. */
extern void cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census);
#endif
#endif
