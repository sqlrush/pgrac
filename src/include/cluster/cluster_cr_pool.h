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
 * cluster_cr_pool_reserve -- reserve a victim slot for `key` (RESERVED, clock
 *   eviction skips RESERVED).  Returns true and fills *out when a slot was
 *   reserved; false (handle.valid=false) when disabled or no victim.  The caller
 *   must eventually publish() or abort() a true-returning reservation.
 */
extern bool cluster_cr_pool_reserve(const ClusterCRCacheKey *key, ClusterCRPoolHandle *out);

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

/* ---- test/diagnostic ---- */
extern int cluster_cr_pool_live_entries(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_POOL_H */
