/*-------------------------------------------------------------------------
 *
 * cluster_space_storage.h
 *    Native relation/WAL owners of persistent SPACE identities.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_space_storage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_STORAGE_H
#define CLUSTER_SPACE_STORAGE_H

#include "access/xlogreader.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_thread.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/* Caller is the original relation-create owner, after registering native
 * abort-time deletion (or under CREATE DATABASE's whole-directory cleanup).
 * False MUST abort that operation. No legacy/local
 * relation acquires a SPACE identity, and no existing identity is replaced. */
extern bool cluster_space_relation_create(RelFileLocator locator);

/* Process-local observation produced by the original live CREATE scope.
 * This value does not retain that scope, certify ABORT/COMMIT, or authorize
 * discard, I/O or directory retirement. It has no persistent representation. */
typedef struct ClusterSpaceCreateObservationV1 {
	ClusterSpaceIdentity identity;
	ClusterWalSourceRef namespace_source; /* namespace sample, not WAL attribution */
	uint64 create_serial;
	XLogRecPtr end_rec_ptr;
} ClusterSpaceCreateObservationV1;

/* Native MAIN constructor only, under its existing create serial and abort
 * pending-delete owner. Capture the actual typed CREATE at its WAL publication
 * before releasing buffers. False leaves output unchanged. A post-publication
 * ERROR preserves the observation and the original cleanup obligation. The
 * caller owns that scope and every ERROR cleanup. Never starts a new scope. */
extern bool cluster_space_relation_create_observed_v1(RelFileLocator locator,
	uint64 create_serial, ClusterSpaceCreateObservationV1 *out);

/* Only the original native abort pending-delete for this new storage may
 * call this, after its transaction/users are gone and before unlink/reuse.
 * Match the actual CREATE identity, use the unchanged writer/WAL gates, and
 * recheck identity. A namespace sample/serial never grants death authority.
 * False/ERROR leaves pending-delete responsible; this does not evict/unlink. */
extern bool cluster_space_relation_flush_created_abort_v1(RelFileLocator locator,
													 const ClusterSpaceCreateObservationV1 *created);

/* Original relation owner holds its lifecycle lock. Reads exact LIVE state
 * through native buffers; never creates missing storage. Output unchanged
 * on refusal. Caller must not hold a page content lock across this read. */
extern bool cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out);

/* Pass-based maintenance owner, under its original lifecycle/admission proof.
 * Uses the existing Resource-X current path, including for auxiliary processes
 * without a backend identity. False releases every pin/lock and leaves output
 * unchanged; the caller retains its work for another pass. No content lock
 * may be held on entry. This read grants no cleanup authority. */
extern bool cluster_space_relation_read_maintenance_identity(RelFileLocator locator,
															 ClusterSpaceIdentity *out);

/* Recovery executor, under its existing isolation and selected restart input.
 * Uses restart namespace, never current writer/relcache or identity creation.
 * Same output/locking contract as the runtime raw reader; not admission. */
extern bool cluster_space_relation_read_redo_identity(RelFileLocator locator,
													  ClusterSpaceIdentity *out);

typedef struct ClusterSpaceColdSourceV1 {
	uint16 origin_thread;
	XLogRecPtr end_rec_ptr;
} ClusterSpaceColdSourceV1;

/* Startup typed cold replay only. Inputs contain the complete retained
 * relation chain in its pass-1 canonical order. Install no further than
 * through; preserve components already covered by a qualified successor.
 * shrink_forks names ForkNumber bits (MAIN/FSM/VM), only at a TRUNCATE's own
 * step. Shrink only those forks even if SPACE already covers the result.
 * Zero means no shrink, including final catch-up and intermediate inputs.
 * Sources are record coordinates, never authority: the implementation must
 * borrow every actual source owner before its first modification. The cold
 * consumer handshake stays undefined until all structural/own-source paths
 * and their physical obligations have been delivered. */
extern bool cluster_space_cold_install_v1(const ClusterSpaceIdentityKey *key,
										  const ClusterSpaceRecoveryInput *inputs,
										  const ClusterSpaceColdSourceV1 *sources, uint32 count,
										  uint32 through, uint8 shrink_forks);

/* DML owner holds the original relation lifecycle lock and has consumed native
 * relcache invalidations at its lock/statement boundary. Backend-private
 * relcache adjunct: hits use memory only, misses use the exact reader above.
 * Caller must not hold content locks even on an expected hit. A returned value
 * is a copy, never a cache or SMgr pointer. Cross-node DDL must complete its
 * existing reliable invalidation barrier before releasing lifecycle locks. */
extern bool cluster_space_relation_get_identity(Relation relation, ClusterSpaceIdentity *out);

/* Operation-local copy of the native cache identity and published namespace
 * reference, not lifecycle, retention or publication authority. Retain the
 * original relation lock and invalidation boundary. No hash/SMgr pointer is
 * borrowed by this value. */
typedef struct ClusterSpaceIdentityObservationV1 {
	ClusterSpaceIdentity identity;
	ClusterWalSourceRef namespace_source;
	uint64 invalidations;
	Oid relid;
} ClusterSpaceIdentityObservationV1;

/* Before content locks, sample the published namespace before the original
 * cache/read path. False/ERROR leaves a zero observation; this does not create
 * a missing SPACE identity. */
extern bool cluster_space_relation_identity_capture_v1(Relation relation,
														 ClusterSpaceIdentityObservationV1 *out);
/* Local published-reference/cache/relcache checks only; no namespace service,
 * lock, I/O or cache population. Refill after invalidation never revives the
 * old observation. This reference does not qualify a writer or publication. */
extern bool cluster_space_relation_identity_current_v1(
	Relation relation, const ClusterSpaceIdentityObservationV1 *expected);

typedef enum ClusterSpaceHintResult {
	CLUSTER_SPACE_HINT_NATIVE,
	CLUSTER_SPACE_HINT_SKIPPED,
	CLUSTER_SPACE_HINT_VERSIONED
} ClusterSpaceHintResult;

/* Original hint owner, before its first byte change. Shared permanent MAIN/VM
 * requires content-X, existing write authority and an already populated SPACE
 * cache entry; this never reads SPACE, upgrades a lock or allocates a cache.
 * VERSIONED enters a critical section and stamps a captured before/result.
 * The caller must perform its bounded mutation and finish without releasing
 * the content lock. SKIPPED leaves everything untouched: optional hints may
 * return, but required transaction normalization must refuse the operation.
 * NATIVE keeps the original nonshared/rebuildable hint path. */
extern ClusterSpaceHintResult cluster_space_hint_begin(Buffer buffer, RfPageProducerBatchV1 *batch);
extern void cluster_space_hint_finish(Buffer buffer, bool standard,
									  const RfPageProducerBatchV1 *batch);

/* Memory-only single-component capture for native owners whose WAL batch can
 * span relations. Same locked-buffer/identity contract as prepare below, but
 * no token allocation; the owner prepares one token for the complete batch. */
extern bool cluster_space_buffer_version_component(const ClusterSpaceIdentity *identity,
												   Buffer buffer, uint8 block_id, uint16 ordinal,
												   RfPageProducerComponentV1 *component);

/* Capture the original mutation owner's locked, initialized MAIN/VM pages.
 * Identity was read before acquiring content locks; this call performs no
 * storage I/O or buffer acquisition. It validates the whole batch before
 * allocating one result token and never changes page bytes. Block IDs are
 * those used by the native WAL record, in strictly increasing order. Caller
 * retains all pins/content locks through stamp, native mutation and WAL. */
extern bool cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity *identity,
												  const Buffer *buffers, const uint8 *block_ids,
												  uint8 count, RfPageProducerBatchV1 *batch);

/* New empty-fork copy owner only. A private source page becomes a new
 * ABSENT->PRESENT version under the destination identity, logged as one
 * FPI+version record. FSM has its explicit rebuildable class. The caller
 * flushes returned WAL before DATA and retains its final native fsync. */
extern bool cluster_space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum,
										BlockNumber block, void *page, XLogRecPtr *lsn);

/* Native btree bulk-build owner, under the original new-index lifecycle lock.
 * A NULL before denotes a not-yet-extended target; otherwise caller has read
 * the exact previously zero-filled target. Validate all zero bytes before
 * publishing its UNFORMATTED edge. Source/result are private construction
 * bytes; caller flushes returned WAL before DATA and retains final fsync. */
extern bool cluster_space_btree_build_page_wal(const ClusterSpaceIdentity *identity,
											   BlockNumber block, void *page,
											   const void *zero_before, XLogRecPtr *lsn);

/* Original new-fork buffer-copy owner only: source content is share-locked,
 * destination is pinned/exclusively content-locked and must be entirely zero
 * after native extension. Captures UNFORMATTED before copying; emits the same
 * FPI+version record and publishes dirty bytes atomically. Native buffer
 * writeback owns WAL-before-DATA. Refusal leaves destination unchanged. */
extern bool cluster_space_copy_buffer_wal(const ClusterSpaceIdentity *identity, const void *source,
										  Buffer destination);

/* Native heap allocation only. The identity was read before content locks;
 * the locked MAIN target must still be entirely zero. Publish the original
 * heap/ITL layout with its own UNFORMATTED edge before exposing it to callers. */
extern bool cluster_space_init_heap_buffer_wal(const ClusterSpaceIdentity *identity,
											   Buffer destination);

/* Original VM write-pin initializer, with the same exact zero-before and WAL
 * ownership contract. Publishes native VM layout, never heap/ITL layout. */
extern bool cluster_space_init_vm_buffer_wal(const ClusterSpaceIdentity *identity,
											 Buffer destination);

/* Original recovery executor only, with its existing isolation and selected
 * WAL inputs. Decode/identity success is not recovery admission. False MUST
 * reject replay; an unsupported structural action is never silently skipped. */
extern bool cluster_space_relation_redo(XLogReaderState *record);

struct HwLock;

/* Original extension owner holds relation lifecycle protection and this
 * exact HW(X). The identity was resolved before ordinary content locks.
 * Uses the existing GCS current-X SPACE page, never FileSize or a second
 * master counter. WAL is flushed before a nonempty range is returned.
 * Refusal returns InvalidBlockNumber and zero granted. */
extern BlockNumber cluster_space_reserve(const ClusterSpaceIdentity *identity,
										 const struct HwLock *lock, uint32 want, uint32 *granted);

/* Original unpublished build/copy owner only, under its lifecycle lock.
 * Acquire HW(X) and reserve exactly the requested next range. A mismatched
 * grant is consumed but refused; it never licenses overwriting prior blocks. */
extern bool cluster_space_reserve_exact(const ClusterSpaceIdentity *identity, BlockNumber first,
										uint32 count);

typedef struct ClusterSpaceTruncateState ClusterSpaceTruncateState;

/* Native RelationTruncate owner after its exclusive lifecycle/KO barrier and
 * auxiliary-fork preparation. No content locks on entry. Prepare makes the
 * surviving DATA base durable, then holds both SPACE pages through native
 * critical WAL/flush/shrink/publish. Finish releases pins and invalidates the
 * identity observation. This API does not grant lifecycle authority. */
extern ClusterSpaceTruncateState *cluster_space_truncate_prepare(Relation rel, BlockNumber nblocks);
extern XLogRecPtr cluster_space_truncate_log(ClusterSpaceTruncateState *state);
extern void cluster_space_truncate_publish(ClusterSpaceTruncateState *state);
extern void cluster_space_truncate_finish(ClusterSpaceTruncateState *state, Relation rel);

typedef struct ClusterSpaceDropState ClusterSpaceDropState;

/* Native commit owner of its pending-delete list, after exclusive lifecycle
 * and KO. Prepare validates private results, serializes the optional native
 * COMMIT tail and releases its page references. Authorize separately checks
 * the completed operation, reacquires both pages and revalidates the exact
 * two-page predecessor outside the critical section. A clean refusal releases
 * those references; an ERROR retains the original ResourceOwner cleanup.
 * Publish marks dirty and installs both pages
 * only after that COMMIT is flushed and decided, with the original checkpoint
 * delay still held. Finish unlocks before unlink. */
extern ClusterSpaceDropState *cluster_space_drop_prepare(const RelFileLocator *locators, int count);
extern const char *cluster_space_drop_wal(const ClusterSpaceDropState *state, uint32 *len);
/* Deferred native purpose retains this unpinned preparation through local
 * transaction cleanup. The original commit critical section records its
 * flushed LSN, without publishing either page. No new persistent format. */
extern bool cluster_space_drop_is_deferred_v4(const ClusterSpaceDropState *state);
extern void cluster_space_drop_defer_v4(ClusterSpaceDropState *state, XLogRecPtr commit_lsn);
struct ClusterKoCompletionV2;
extern bool cluster_space_drop_committed_owner_v4(const ClusterSpaceDropState *state,
												 const struct ClusterKoCompletionV2 *completion);
struct ClusterKoDirectoryIoV3;
struct ClusterPageWalBindingV1;
/* Same committed preparation and entered call; writes only its two existing
 * SPACE pages. Output is published after sync/readback and exact revalidation.
 * Refusal/ERROR leaves physical and native recovery responsibility intact.
 * Optional diagnostic_stage is caller-owned observation, never authority. */
extern bool cluster_space_drop_complete_v4(const ClusterSpaceDropState *state,
	const struct ClusterKoCompletionV2 *completion, const struct ClusterKoDirectoryIoV3 *call,
	struct ClusterPageWalBindingV1 *terminal, void *wal, uint32 *diagnostic_stage);
extern bool cluster_space_drop_authorize(ClusterSpaceDropState *state);
extern void cluster_space_drop_publish(ClusterSpaceDropState *state, XLogRecPtr commit_lsn);
extern void cluster_space_drop_finish(ClusterSpaceDropState *state);

/* Native local recovery: all deletion identities are in this COMMIT itself.
 * No previous record or process-local intent is required at a checkpoint cut. */
extern bool cluster_space_drop_replay_commit(XLogReaderState *record, TransactionId xid);

/* Cold native COMMIT postcondition only, after the qualified deletion.
 * Neither replay authority nor a durable completion / missing-SPACE proof. */
extern bool cluster_space_drop_replay_verify_v1(XLogReaderState *record, TransactionId xid);

/* Original native cold COMMIT, after its qualified deletion: bind every
 * native/typed target, sync existing results, then recheck the whole set.
 * Not replay-from-absence authority or the final cold ROOT seal. */
extern bool cluster_space_drop_replay_finish_v1(XLogReaderState *record, TransactionId xid);

/* Executable cold foreign-source owner only, borrowing its immutable,
 * fully decoded selected COMMIT (reader must not advance). Qualifies every original
 * native/typed shared target, syncs that source, then performs the native
 * deletion and strict whole-result sync/readback. Structural effect only:
 * caller still owes canonical TT/CLOG, remaining COMMIT effects and final
 * cold seal. Does not authorize replay from missing SPACE. */
extern bool cluster_space_cold_drop_foreign_v1(XLogReaderState *record, TransactionId xid);
/* Same complete structural proof without mutation, before the selected
 * compound record changes canonical TT/CLOG. Execution rechecks it again. */
extern bool cluster_space_cold_drop_foreign_preflight_v1(XLogReaderState *record,
														 TransactionId xid);

/* Same physical postcondition at the original sequencer's final callback,
 * after all scheduled records executed and every source was fully drained.
 * The sealed final TOMBSTONE binds the target, not execution authority. */
extern bool cluster_space_cold_drop_postcondition_v1(const ClusterSpaceIdentityKey *key,
													 const ClusterSpaceRecoveryInput *input,
													 const ClusterSpaceColdSourceV1 *source);

/* Final executed DROP result: verify, sync MAIN and its directory, then
 * recheck the same cold namespace/source. No deletion or completion publish. */
extern bool cluster_space_cold_drop_foreign_sync_v1(const ClusterSpaceIdentityKey *key,
													const ClusterSpaceRecoveryInput *input,
													const ClusterSpaceColdSourceV1 *source);
extern bool cluster_space_cold_drop_sync_v1(const ClusterSpaceIdentityKey *key,
											const ClusterSpaceRecoveryInput *input,
											const ClusterSpaceColdSourceV1 *source);

#endif /* CLUSTER_SPACE_STORAGE_H */
