/*-------------------------------------------------------------------------
 *
 * cluster_cr_pool.h
 *	  pgrac dedicated shared CR buffer pool, v1 (spec-5.51).
 *
 *	  A per-instance, cross-backend shared-memory pool of full-block CR images,
 *	  layered as L2 behind the backend-local L1 cache (cluster_cr_cache.c,
 *	  spec-3.10).  Physically separate from shared_buffers (no pollution).  Keyed
 *	  by the SAME exact-identity ClusterCRCacheKey as L1 (compared via the
 *	  canonical cluster_cr_cache_key_equal helper; identity contract SSOT =
 *	  spec-5.53 §3.1).  NB: the key is an exact identity, NOT "self-validating":
 *	  safety against relfilenode (Oid) reuse is the provably-complete pool_epoch
 *	  bump floor (property 4 below + spec-5.53 D2b), not an intrinsic property of
 *	  the key — there is no binding-point-independent token at the CR layer
 *	  (spec-5.53 §0.6 / D0 = RED).
 *
 *	  Five safety properties that distinguish a SHARED CR cache from the
 *	  backend-local L1 (spec-5.51 §3, all 8.A):
 *	   1. copy-out: a hit memcpy's BLCKSZ into a caller buffer WHILE the segment
 *	      LWLock is held; a shared slot raw pointer is NEVER returned (else a
 *	      concurrent evict/publish could rewrite it under the caller).
 *	   2. slot generation: reserve bumps a per-slot generation written into the
 *	      handle; publish/abort only act when generation still matches, so a
 *	      stale handle for a reused slot cannot clobber it (ABA fail-closed).
 *	   3. handle carries the key: publish verifies slot.key == handle.key.
 *	   4. pool_epoch (uint64, never wraps): a global lifecycle epoch bumped at
 *	      DROP/TRUNCATE/relfilenode-reuse/sinval; a slot is served only when its
 *	      stamped epoch == the current epoch, so a stale image cannot survive a
 *	      lifecycle event (fine-grained per-relation invalidation = spec-5.56).
 *	   5. exact-key safe-miss: any non-exact-key / RESERVED / epoch-mismatch /
 *	      capacity miss returns "no hit"; the caller falls back to constructing
 *	      (cluster_cr_construct_block_into, fail-closed).  A CR cache may ALWAYS
 *	      miss safely; it must NEVER false-hit.
 *
 *	  Two-phase insert keeps construction (which may ereport, spec-3.9) OUT of
 *	  the LWLock (spec-5.51 §3.2, rule 16):
 *	    reserve(key, &h);                                  // seg EXCL, pick victim
 *	    cluster_cr_construct_block_into(buf, scn, dst);    // LOCK-FREE, may throw
 *	    publish(&h, dst);                                  // seg EXCL, gen/key/epoch
 *	  An exception between reserve and publish must call abort(&h) (PG_CATCH).
 *
 *	  Disabled by default: cluster.shared_cr_pool_size_blocks == 0 means the
 *	  region claims zero bytes (true zero memory) and every op is a safe no-op /
 *	  miss, so the L1-only spec-3.10 path is unchanged.  Enabling requires
 *	  size_blocks > 0 and a restart (both GUCs are PGC_POSTMASTER).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.51-dedicated-shared-cr-buffer-pool-v1.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_pool.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_POOL_H
#define CLUSTER_CR_POOL_H

#ifndef FRONTEND

#include "postgres.h"

#include "cluster/cluster_cr_cache.h" /* ClusterCRCacheKey */

/*
 * GUCs (registered in cluster_guc.c, spec-5.51 D8).  Both PGC_POSTMASTER: the
 * pool size and on/off are fixed at shmem-reservation time.  size_blocks == 0
 * (the default) is true zero memory; the region is still registered (so the
 * shmem-region-count baseline is deterministic) but its size_fn returns 0.
 */
extern int cluster_shared_cr_pool_size_blocks;
extern bool cluster_shared_cr_pool_enabled;

/*
 * cluster.cr_pool_rel_generation_slots (spec-5.56 D4, Part B; PGC_POSTMASTER).
 * 0 (the default) = disabled => the smgrdounlinkall lifecycle bump stays the
 * spec-5.53 unconditional GLOBAL epoch bump (coarse, whole-pool flush; zero
 * behavior change).  > 0 enables fine-grained per-relation invalidation: a
 * bounded per-instance generation table sized at this many locators, GO mode
 * bumps only the dropped relation's generation so unrelated warm CR images
 * survive.  Bounded by the same hard cap as the pool.
 */
extern int cluster_cr_pool_rel_generation_slots;

/*
 * Opaque-ish handle for the two-phase insert.  `valid` is false when reserve
 * declined (pool disabled / no victim); publish/abort on an invalid handle are
 * no-ops.  Carries the key + generation so publish can fail closed on ABA.
 */
typedef struct ClusterCRPoolHandle {
	bool valid;
	int slot_idx;
	int seg;
	uint32 generation;
	uint64 pool_epoch; /* epoch at reserve (for publish recheck) */
	uint64 rel_gen;	   /* spec-5.56 D4: per-relation install generation at reserve
						* (for the publish-side per-relation recheck ⑤; 0 = epoch-only) */
	ClusterCRCacheKey key;
} ClusterCRPoolHandle;

/* ---- shmem region lifecycle (cluster_shmem.c framework) ---- */
extern Size cluster_cr_pool_shmem_size(void);
extern void cluster_cr_pool_shmem_init(void);
extern void cluster_cr_pool_shmem_register(void);
extern void cluster_cr_pool_request_lwlocks(void);

/* ---- pool operations (spec-5.51 D2/D3) ---- */

/*
 * cluster_cr_pool_lookup_copy -- probe by exact key.  On a hit, memcpy BLCKSZ
 *   into `dst` (a caller-owned BLCKSZ buffer) WHILE the segment lock is held and
 *   return true.  On any miss / disabled / epoch-mismatch, return false (caller
 *   constructs).  Never returns a pointer into shared memory.
 */
extern bool cluster_cr_pool_lookup_copy(const ClusterCRCacheKey *key, char *dst);

/*
 * cluster_cr_pool_lookup_copy_gen -- spec-5.56 P1-fix: like cluster_cr_pool_lookup_
 *   copy but ALSO outputs *out_rel_gen = the per-relation generation the copied
 *   bytes are valid for, CAPTURED UNDER THE POOL LOCK at the moment the composite
 *   fence matched.  The caller MUST stamp the L1 entry with this value (never a
 *   fresh re-read of the locator's current gen): a racing unlink between the L2
 *   copy and the L1 commit would otherwise let the caller brand stale gen-N bytes
 *   with gen N+1, bypassing the composite fence (stale L1 hit, 8.A).  *out_rel_gen
 *   = 0 when the gen table is disabled (epoch-only) or on a miss.
 *   cluster_cr_pool_lookup_copy() is the out_rel_gen == NULL wrapper.
 */
extern bool cluster_cr_pool_lookup_copy_gen(const ClusterCRCacheKey *key, char *dst,
											uint64 *out_rel_gen);

/*
 * cluster_cr_pool_reserve -- reserve a victim slot for `key` (RESERVED, clock
 *   eviction skips RESERVED).  Returns true and fills *out when a slot was
 *   reserved; false (handle.valid=false) when disabled or no victim.  The caller
 *   must eventually publish() or abort() a true-returning reservation.
 */
extern bool cluster_cr_pool_reserve(const ClusterCRCacheKey *key, ClusterCRPoolHandle *out);

/*
 * cluster_cr_pool_reserve_gen -- spec-5.56 D4: reserve a victim slot stamping it
 *   with the per-relation install generation `install_gen` (the value returned by
 *   cluster_cr_pool_register_locator).  The handle carries install_gen so publish
 *   can detect a per-relation unlink that occurred during construction (⑤).
 *   cluster_cr_pool_reserve() is the install_gen == 0 wrapper (epoch-only).
 */
extern bool cluster_cr_pool_reserve_gen(const ClusterCRCacheKey *key, uint64 install_gen,
										ClusterCRPoolHandle *out);

/*
 * cluster_cr_pool_publish -- install the constructed page into the reserved
 *   slot, but only if state==RESERVED && slot.key==handle.key &&
 *   slot.generation==handle.generation && slot.pool_epoch==current.  If the
 *   generation still matches but the epoch advanced during construction, the
 *   reservation is released (FREE, generation++) without installing (so the
 *   RESERVED slot does not leak).  A generation mismatch means the slot was
 *   already recycled by someone else: do not touch it.
 */
extern void cluster_cr_pool_publish(const ClusterCRPoolHandle *h, const char *page);

/* cluster_cr_pool_abort -- release a reservation (FREE, generation++) without
 *   installing.  Generation-guarded; safe on an invalid handle. */
extern void cluster_cr_pool_abort(const ClusterCRPoolHandle *h);

/* ============================================================
 * CR pool lifecycle / invalidation contract SSOT (spec-5.56 D1)
 *
 * The shared CR pool (L2) + backend-local CR cache (L1) hold FULLY-MATERIALIZED
 * historical page images (char page[BLCKSZ]); an image is a pure function of its
 * key (AD-006, S-1 GREEN) and serving it references NO live undo / catalog /
 * membership state.  Four lifecycle event classes, each with its "invalidate /
 * survive / fail-closed" state (spec-5.56 §3.1):
 *
 *  class event                     invalidation                survive / fail-closed
 *  C1    DROP / different relid     smgrdounlinkall -> epoch     shipped floor (5.53 D2b)
 *        reuse                      bump (floor) [+ per-rel gen, + different relations'
 *                                   GO]                         entries survive (GO)
 *  C2    TRUNCATE/VACFULL/CLUSTER   same (old relfilenode        shipped floor; same-relid
 *        (same relid, new relfile)  unlink)                     old image MUST MISS
 *  C3    retention / undo-recycle   NONE NEEDED (§3.2 proof:     all cached images survive
 *        (horizon advance)          INV-R1 image-immutability + (image is self-contained);
 *                                   INV-R2 horizon-pinning +    if an image were NOT fully
 *                                   INV-R3 read_scn monotone)   materialized / referenced
 *                                                               live undo => MUST invalidate
 *                                                               (v1 forbids that path, INV-L0)
 *  C4    reconfig / membership /    NONE NEEDED for the two      own + merged-materialized
 *        remaster                   pool-eligible origin        remote images survive
 *                                   classes (§3.3, per-class)   (durable merge_recovered_lsn,
 *                                                               read_scn is a global SCN); a
 *                                                               runtime-warm remote image is
 *                                                               NOT in the pool (tier-3 fall-
 *                                                               through) => class-③ fail-
 *                                                               closed assert + forward 5.57
 *
 *  INV-L0 (total fail-closed): if ANY lifecycle signal is unavailable /
 *    inconsistent / uncertain -> MISS / reconstruct (NEVER stale-serve).  The
 *    pool holds ONLY fully-materialized images (no live-undo serve path); if that
 *    invariant is ever broken, the corresponding invalidation MUST be added — the
 *    image must never be served by default.
 * ============================================================ */

/* ---- lifecycle epoch (spec-5.51 D5) ---- */

/* cluster_cr_pool_bump_epoch -- advance the global pool epoch (O(1) atomic).
 *   Bound to DROP / TRUNCATE / relfilenode reuse / cluster sinval.  No-op when
 *   the pool is disabled. */
extern void cluster_cr_pool_bump_epoch(void);

/* cluster_cr_pool_current_epoch -- read the current epoch (for the L1 entry
 *   reset + commit recheck in cluster_cr_lookup_or_construct, spec-5.51 D4).
 *   Returns 0 when disabled. */
extern uint64 cluster_cr_pool_current_epoch(void);

/* ---- counters (spec-5.51 D8; read lock-free by dump) ---- */
extern uint64 cluster_cr_pool_hit_count(void);
extern uint64 cluster_cr_pool_miss_count(void);
extern uint64 cluster_cr_pool_reserve_count(void);
extern uint64 cluster_cr_pool_publish_count(void);
extern uint64 cluster_cr_pool_abort_count(void);
extern uint64 cluster_cr_pool_evict_count(void);
extern uint64 cluster_cr_pool_epoch_bump_count(void);
extern uint64 cluster_cr_pool_publish_stale_release_count(void);

/* ---- identity/reuse-fence mismatch diagnostics (spec-5.53 D5) ---- */
extern uint64 cluster_cr_pool_key_mismatch_count(void);
extern uint64 cluster_cr_pool_epoch_mismatch_count(void);
extern uint64 cluster_cr_pool_generation_mismatch_count(void);
extern uint64 cluster_cr_pool_base_lsn_mismatch_count(void);
extern uint64 cluster_cr_pool_locator_reuse_reject_count(void);

/* Record an L1-detected (cluster_cr_cache.c, linear-scan = reliable) mismatch
 * into the shared pool counters so all five dump together (spec-5.53 D5):
 *   _l1_epoch_mismatch  -- per-entry epoch fence rejected a stale exact-key entry
 *   _base_lsn_mismatch  -- MISS attributed to a churned page version (F0-14)
 *   _key_mismatch       -- MISS attributed to a diverged read_scn (F0-16)
 * All no-op when the pool is disabled. */
extern void cluster_cr_pool_note_l1_epoch_mismatch(void);
extern void cluster_cr_pool_note_base_lsn_mismatch(void);
extern void cluster_cr_pool_note_key_mismatch(void);

/* ============================================================
 * Per-relation lifecycle generation table (spec-5.56 D4, Part B; value-gated)
 *
 *   A bounded, per-instance shared-memory table mapping a FULL RelFileLocator
 *   (spc/db/rel — P2-a: NOT relNumber-only, which would false-share across
 *   db/tablespace and dilute the value) to a monotonically increasing 1-based
 *   generation.  It is the FINE-GRAINED half of the composite lifecycle fence:
 *   in GO mode (slots > 0) a relfilenode unlink bumps ONLY the dropped relation's
 *   generation, so unrelated warm CR images survive — instead of the coarse
 *   global-epoch bump (spec-5.53 floor) that flushes the whole pool (measured
 *   100% blast radius per unrelated DDL, spec-5.56 D0).
 *
 *   Its OWN shmem region (Q8 exception: the table needs independent sizing; the
 *   lifecycle COUNTERS still fold into ClusterCRPoolShared / the 'cr' dump).
 *   The global epoch (cluster_cr_pool_bump_epoch) is RETAINED as the detected-
 *   failure FALLBACK backstop (gen-table disabled / uncertain / internal error).
 *
 *   INVARIANTS (8.A, ship-blocking; spec-5.56 §3.4):
 *     INV-G1 (completeness): every relfilenode-free/reuse path provably bumps the
 *       per-relation generation OR takes the global fallback — same exhaustive
 *       chokepoint (smgrdounlinkall over ALL nrels, incl. md.c-routed; NON-
 *       filtered).  Tracked-locator correctness RESTS on this completeness (the
 *       global epoch does NOT advance for a tracked unlink), NOT on the global
 *       epoch backstop — an undetected missed per-rel bump is a P0 over-hit.
 *     INV-G2 (eviction safety): the table is APPEND-ONLY in v1 — entries are never
 *       removed; a tracked unlink bumps gen in place (handles relfilenode reuse:
 *       the reused locator's NEXT install reads the bumped gen).  On full, register
 *       fails => serve-but-skip-cache (NO global bump; nothing was cached).  This
 *       is the v0.5 "唯一安全" choice realized as never-delete: a backend-local L1
 *       survivor stays tracked, so its locator's unlink bumps the gen and the L1
 *       entry MISSes.  (A future eviction MUST first global-fallback-bump.)
 *     INV-G3 (register-at-install): ANY L1 OR L2 entry ⟹ its locator is
 *       registered.  BOTH the L2 reserve AND the L1 commit register the locator;
 *       a register failure (table full) skips BOTH caches (serve-but-skip-cache),
 *       so an untracked locator has NO entry and its unlink is a safe no-op.
 * ============================================================ */

/* ---- gen-table shmem region lifecycle (registered/requested together with the
 *      pool by cluster_cr_pool_shmem_register / _request_lwlocks) ---- */
extern Size cluster_cr_pool_relgen_shmem_size(void);
extern void cluster_cr_pool_relgen_shmem_init(void);

/* cluster_cr_pool_rel_generation_enabled -- true iff the gen table is allocated
 *   (slots > 0 AND the pool is enabled).  When false the composite fence degrades
 *   to epoch-only (spec-5.53 equivalence) and smgrdounlinkall does the
 *   unconditional global bump. */
extern bool cluster_cr_pool_rel_generation_enabled(void);

/*
 * cluster_cr_pool_rel_generation -- read the locator's current generation.
 *   Returns true iff the locator is registered; *out_gen = its generation (>= 1).
 *   disabled / unregistered => false (caller uses cur_rel_gen = 0 = no per-rel
 *   fence => epoch-only).  Lock-light (LW_SHARED): called on every CR cache miss
 *   to capture the composite fence's rel_gen half.
 */
extern bool cluster_cr_pool_rel_generation(RelFileLocator rlocator, uint64 *out_gen);

/*
 * cluster_cr_pool_register_locator -- register the locator at install (INV-G3:
 *   called at BOTH L2 reserve AND L1 commit).  *out_gen = the generation to stamp
 *   the entry with (newly registered => 1; already registered => current gen).
 *   Returns:
 *     true,  *out_gen = 0   when disabled (proceed; epoch-only fence)
 *     true,  *out_gen >= 1  when registered/found (cache the image under that gen)
 *     false                 when the table is full (serve-but-skip-cache;
 *                           rel_gen_table_overflow++; NO global bump — nothing
 *                           was cached so nothing needs invalidating, P2-d′)
 */
extern bool cluster_cr_pool_register_locator(RelFileLocator rlocator, uint64 *out_gen);

/*
 * cluster_cr_pool_unlink_locator -- the per-relation lifecycle bump, called at the
 *   smgrdounlinkall chokepoint once per locator over ALL nrels (GO mode; covers
 *   md.c-routed, NON-filtered, INV-G1).  tracked => gen++ (rel_gen_bump++; only
 *   THIS relation's entries are fenced, others stay warm); untracked => no-op
 *   (INV-G3 guarantees no L1/L2 entry exists); internal uncertainty => global
 *   fallback bump.  No-op when the gen table is disabled (the caller does the
 *   unconditional global bump instead).
 */
extern void cluster_cr_pool_unlink_locator(RelFileLocator rlocator);

/*
 * cluster_cr_pool_global_fallback_bump -- the detected-failure backstop: advance
 *   the GLOBAL epoch (whole-pool flush, spec-5.53 coarse) + global_epoch_fallback_
 *   bump++.  Used for gen-table-disabled transitions, internal uncertainty, and
 *   (future) gen-entry eviction.  Degrades safely to coarse.
 */
extern void cluster_cr_pool_global_fallback_bump(void);

/* test/diagnostic: number of registered locators in the gen table. */
extern int cluster_cr_pool_rel_generation_live(void);

/* ---- lifecycle counters (spec-5.56 D5; folded into 'cr' dump, no new region) ---- */
extern uint64 cluster_cr_pool_global_epoch_fallback_bump_count(void);
extern uint64 cluster_cr_pool_rel_gen_bump_count(void);
extern uint64 cluster_cr_pool_rel_gen_table_overflow_count(void);
extern uint64 cluster_cr_pool_retention_horizon_advance_noted_count(void);
extern uint64 cluster_cr_pool_reconfig_intra_survived_count(void);

/* cluster_cr_pool_note_retention_horizon_advance -- spec-5.56 D2 (C3): a CR
 *   lookup observed the undo retention horizon having advanced past a cached
 *   image's read_scn.  PURE OBSERVATION (no invalidation): the materialized image
 *   stays correct (INV-R1); this only quantifies that the retention-recycle
 *   scenario really occurs.  No-op when the pool is disabled. */
extern void cluster_cr_pool_note_retention_horizon_advance(void);

/* cluster_cr_pool_note_reconfig_intra_survived -- spec-5.56 D3 (C4): unit/
 *   injection evidence that an own/merged-materialized image survived a reconfig
 *   epoch advance (INV-C1/C2).  No-op when the pool is disabled. */
extern void cluster_cr_pool_note_reconfig_intra_survived(void);

/* ---- test/diagnostic ---- */
extern int cluster_cr_pool_live_entries(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_POOL_H */
