/*-------------------------------------------------------------------------
 *
 * cluster_cr_cache.h
 *	  pgrac backend-local CR block cache (spec-3.10 D2).
 *
 *	  A fixed-size, backend-local cache of full-block CR images keyed by
 *	  (RelFileLocator, forknum, blockno, read_scn, base_page_lsn).  The CR
 *	  image is a pure function of that key (spec-3.10 §3.2): the content is
 *	  immutable for a given (block, read_scn) and base_page_lsn guards against
 *	  HOT-prune / VACUUM physical relayout desyncing cached offsets from the
 *	  live page.  So there is NO invalidation — only clock eviction for memory.
 *
 *	  Two-phase miss install avoids a scratch->cache double memcpy and keeps
 *	  the cache consistent if construction ereports mid-way:
 *	    slot = cluster_cr_cache_victim_slot(key, &evicted);   // reserve (invalid)
 *	    cluster_cr_construct_block_into(buf, read_scn, slot);  // fill (may throw)
 *	    cluster_cr_cache_commit_slot();                        // mark valid
 *	  If construction throws, commit is never called and the reserved slot stays
 *	  invalid (never served).
 *
 *	  Backend-local: no shmem, no LWLock (spec-3.10 §2.5 / L106).  Disabled when
 *	  cluster.cr_cache_max_blocks == 0: lookup always misses and victim_slot
 *	  returns a single backend-local fallback buffer (equivalent to the spec-3.9
 *	  scratch), so the gate path keeps working with zero caching.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.10-cr-block-cache.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_cache.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_CACHE_H
#define CLUSTER_CR_CACHE_H

#ifndef FRONTEND

#include "postgres.h"

#include "access/xlogdefs.h"		/* XLogRecPtr */
#include "cluster/cluster_scn.h"	/* SCN */
#include "common/relpath.h"			/* ForkNumber */
#include "storage/block.h"			/* BlockNumber */
#include "storage/relfilelocator.h" /* RelFileLocator */

/*
 * cluster.cr_cache_max_blocks -- backend-local cache capacity in 8 KB blocks.
 * 0 disables caching.  Owned by this module (GUC registration in cluster_guc.c
 * points DefineCustomIntVariable at it; spec-3.10 D4).
 */
extern int cluster_cr_cache_max_blocks;

/*
 * ClusterCRCacheKey -- CR image identity contract / SSOT (spec-5.53 §3.1 D1;
 * over-hit P0 命门).  This is the ONE definition of "two CR lookups address the
 * same image"; L1 (cluster_cr_cache.c) and L2 (cluster_cr_pool.c) both key on it
 * and compare via cluster_cr_cache_key_equal().
 *
 *   field             necessity (drop it ⇒ the over-hit reverse-example)   ~Oracle
 *   rlocator.spcOid   diff tablespace, same relNumber ⇒ different file       DBA tbsp
 *   rlocator.dbOid    diff database,   same relNumber ⇒ different object      DBA file#
 *   rlocator.relNumber diff relation ⇒ different data (Oid CAN be reused      DBA object
 *                      ⇒ paired with the reuse fence, see below)
 *   forknum           main/fsm/vm/init are different physical files           block class
 *   blockno           per-block image                                         DBA block#
 *   read_scn          diff snapshot ⇒ diff visible version (even if the       SCN
 *                      physical image bytes coincide)
 *   base_page_lsn     live page physical-version guard: every WAL-logged       block version
 *                      change (HOT-prune / VACUUM relayout / hint-bit) bumps
 *                      pd_lsn ⇒ a relayout forces a MISS, never a stale hit
 *
 *   Joint sufficiency: the five fields uniquely determine the own-instance CR
 *   image (5.50 §17.5 S-1 GREEN: base_page_lsn churn is a MISS, not a stale hit;
 *   read_scn blocks cross-snapshot leakage).
 *
 *   FORBIDDEN (would re-open over-hit; §3.1 / §1.3 "永不做"):
 *     - block-only key: drops read_scn (cross-snapshot leak) + base_page_lsn
 *       (relayout over-hit).
 *     - base_block_scn: pd_block_scn is NOT bumped by HOT-prune, so it is both
 *       redundant (page_lsn bumps whenever it does) and unsafe-if-sole.
 *
 *   relfilenode reuse (relNumber = Oid, 32-bit, REUSABLE — catalog.c
 *   GetNewRelFileNumber collides only against LIVE files; and same-relid rewrite
 *   swaps the physical relNumber: TRUNCATE → RelationSetNewRelfilenumber,
 *   CLUSTER/VACUUM FULL → swap_relation_files) means relNumber alone cannot fence
 *   a stale image.  Safety against reuse is NOT in the key: it is the
 *   provably-complete pool_epoch bump floor (spec-5.53 D2b — every relfilenode
 *   unlink bumps the lifecycle epoch at the smgrdounlinkall chokepoint).  NB:
 *   there is NO binding-point-independent self-validating token at the CR layer
 *   (a catalog-coupled storage-incarnation belt was D0-gated and found infeasible
 *   under the CR content-lock contract; see spec-5.53 §0.6 / D0 = RED).
 *
 *   Build with cr_build_cache_key (memset-0) so padding bytes are zeroed, and
 *   compare with cluster_cr_cache_key_equal (field-wise, never memcmp).
 */
typedef struct ClusterCRCacheKey {
	RelFileLocator rlocator; /* spc / db / rel */
	ForkNumber forknum;
	BlockNumber blockno;
	SCN read_scn;			  /* target snapshot */
	XLogRecPtr base_page_lsn; /* live page version guard */
} ClusterCRCacheKey;

/*
 * spec-5.53 D1: lock the identity contract's field SET.  base_page_lsn is the
 * last identity field; if a sixth field is appended (or the trailing layout
 * grows) sizeof() exceeds its end offset and this fires — re-review the §3.1
 * identity contract (and every over-hit reverse-example above) before changing
 * the CR key.  The per-field type widths are asserted too: a narrower read_scn /
 * base_page_lsn / locator would silently weaken a discriminator.
 */
StaticAssertDecl(sizeof(ClusterCRCacheKey)
					 == offsetof(ClusterCRCacheKey, base_page_lsn) + sizeof(XLogRecPtr),
				 "ClusterCRCacheKey gained a trailing field / padding: re-review the "
				 "spec-5.53 §3.1 identity contract before changing the CR cache key.");
StaticAssertDecl(sizeof(((ClusterCRCacheKey *)0)->read_scn) == sizeof(SCN)
					 && sizeof(((ClusterCRCacheKey *)0)->base_page_lsn) == sizeof(XLogRecPtr)
					 && sizeof(((ClusterCRCacheKey *)0)->rlocator) == sizeof(RelFileLocator),
				 "ClusterCRCacheKey identity field width changed: re-review spec-5.53 §3.1.");

/*
 * cluster_cr_cache_key_equal -- canonical CR cache key equality (spec-5.51 D6).
 *   Field-wise compare (RelFileLocatorEquals + scalar fields); NEVER memcmp the
 *   struct (padding bytes are not part of identity).  Exported so the shared CR
 *   pool (cluster_cr_pool.c, spec-5.51) and the backend-local cache share ONE
 *   equality definition and cannot drift.
 */
extern bool cluster_cr_cache_key_equal(const ClusterCRCacheKey *a, const ClusterCRCacheKey *b);

/*
 * cluster_cr_cache_lookup miss-reason classification (spec-5.53 D5).  On a miss
 * the linear L1 scan records the MOST-SPECIFIC near-miss so a MISS is
 * attributable (feeds the over-miss / SCN-range value analysis and the reuse
 * fence health, §2.4): EPOCH (exact key, stale lifecycle epoch — the L1 reuse
 * fence) > BASE_LSN (same block + read_scn, churned page version, F0-14) > KEY
 * (same block, diverged read_scn, F0-16) > NONE.  NONE on a hit or a no-near
 * match.
 */
#define CR_CACHE_MISS_NONE 0
#define CR_CACHE_MISS_EPOCH 1
#define CR_CACHE_MISS_BASE_LSN 2
#define CR_CACHE_MISS_KEY 3

/*
 * cluster_cr_cache_lookup -- probe under the per-entry lifecycle-epoch fence
 *   (spec-5.53 D2c, the L1 half of "L1+L2 same fence").  Returns the cached CR
 *   page (immutable; valid until the next cluster_cr_cache_* call in this
 *   backend) ONLY when an entry's key matches AND its stamped pool_epoch equals
 *   `cur_epoch`; otherwise NULL (miss / disabled / stale epoch).  An exact-key
 *   match whose stamped epoch is older than `cur_epoch` (a DROP/TRUNCATE/
 *   relfilenode reuse/sinval since the image was cached) is NOT served (rule
 *   8.A: never a stale hit).  *out_miss_reason (may be NULL) classifies a miss
 *   per CR_CACHE_MISS_* so the caller can bump the matching mismatch counter.
 *   Pass cur_epoch == 0 (L2 disabled) to make the epoch fence a no-op (entries
 *   stamped 0; pure exact-key, spec-3.10 behavior).
 */
extern const char *cluster_cr_cache_lookup(const ClusterCRCacheKey *key, uint64 cur_epoch,
										   int *out_miss_reason);

/*
 * cluster_cr_cache_lookup_fenced -- spec-5.56 D4: lookup under the COMPOSITE
 *   lifecycle fence {pool_epoch, rel_gen} (P1-c).  Identical to
 *   cluster_cr_cache_lookup but ALSO requires the entry's stamped per-relation
 *   generation to equal `cur_rel_gen` (the locator's current generation, which
 *   the caller reads from the per-relation generation table).  cur_rel_gen == 0
 *   makes the rel_gen half a no-op (gen table disabled / locator unregistered =>
 *   epoch-only, spec-5.53 equivalence).  cluster_cr_cache_lookup() is the
 *   cur_rel_gen == 0 wrapper.
 */
extern const char *cluster_cr_cache_lookup_fenced(const ClusterCRCacheKey *key, uint64 cur_epoch,
												  uint64 cur_rel_gen, int *out_miss_reason);

/*
 * cluster_cr_cache_victim_slot -- reserve a BLCKSZ slot for `key` to construct
 *   into, stamping it with `cur_epoch` (spec-5.53 D2c).  Clock-evicts a victim
 *   if at capacity (*evicted set true iff a valid entry was displaced).  The
 *   slot is left INVALID until commit, so a throwing construction never leaves a
 *   half-built entry servable.  When disabled, returns the backend-local
 *   fallback buffer and sets *evicted = false.
 */
extern char *cluster_cr_cache_victim_slot(const ClusterCRCacheKey *key, uint64 cur_epoch,
										  bool *evicted);

/*
 * cluster_cr_cache_commit_slot -- mark the slot reserved by the last
 *   victim_slot() call valid + referenced.  No-op when disabled.
 */
extern void cluster_cr_cache_commit_slot(void);

/*
 * cluster_cr_cache_commit_slot_gen -- spec-5.56 D4: mark the reserved slot valid
 *   AND stamp it with the authoritative per-relation generation `rel_gen` (the
 *   install_gen from cluster_cr_pool_register_locator on the construct path, or
 *   the generation an L2 hit matched).  cluster_cr_cache_commit_slot() is the
 *   rel_gen == 0 wrapper (epoch-only).  No-op when disabled.
 */
extern void cluster_cr_cache_commit_slot_gen(uint64 rel_gen);

/*
 * cluster_cr_cache_reset -- drop all entries (GUC change / capacity resize /
 *   backend reset).  The next lookup/victim re-allocates lazily.
 */
extern void cluster_cr_cache_reset(void);

/* Test/diagnostic: number of currently-valid entries (this backend). */
extern int cluster_cr_cache_live_entries(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_CACHE_H */
