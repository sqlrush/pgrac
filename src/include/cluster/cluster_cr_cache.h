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
 * ClusterCRCacheKey -- backend-local CR image identity (spec-3.10 Q1/Q6).
 *   NO base_block_scn: pd_block_scn is not bumped by HOT-prune, so it is both
 *   redundant (page_lsn bumps whenever it does) and unsafe-if-sole.  Build with
 *   the helper so padding bytes are zeroed before compare.
 */
typedef struct ClusterCRCacheKey {
	RelFileLocator rlocator; /* spc / db / rel */
	ForkNumber forknum;
	BlockNumber blockno;
	SCN read_scn;			  /* target snapshot */
	XLogRecPtr base_page_lsn; /* live page version guard */
} ClusterCRCacheKey;

/*
 * cluster_cr_cache_key_equal -- canonical CR cache key equality (spec-5.51 D6).
 *   Field-wise compare (RelFileLocatorEquals + scalar fields); NEVER memcmp the
 *   struct (padding bytes are not part of identity).  Exported so the shared CR
 *   pool (cluster_cr_pool.c, spec-5.51) and the backend-local cache share ONE
 *   equality definition and cannot drift.
 */
extern bool cluster_cr_cache_key_equal(const ClusterCRCacheKey *a, const ClusterCRCacheKey *b);

/*
 * cluster_cr_cache_lookup -- probe.  Returns the cached CR page (immutable;
 *   valid until the next cluster_cr_cache_* call in this backend) or NULL on a
 *   miss / when caching is disabled.
 */
extern const char *cluster_cr_cache_lookup(const ClusterCRCacheKey *key);

/*
 * cluster_cr_cache_victim_slot -- reserve a BLCKSZ slot for `key` to construct
 *   into.  Clock-evicts a victim if at capacity (*evicted set true iff a valid
 *   entry was displaced).  The slot is left INVALID until commit, so a throwing
 *   construction never leaves a half-built entry servable.  When disabled,
 *   returns the backend-local fallback buffer and sets *evicted = false.
 */
extern char *cluster_cr_cache_victim_slot(const ClusterCRCacheKey *key, bool *evicted);

/*
 * cluster_cr_cache_commit_slot -- mark the slot reserved by the last
 *   victim_slot() call valid + referenced.  No-op when disabled.
 */
extern void cluster_cr_cache_commit_slot(void);

/*
 * cluster_cr_cache_reset -- drop all entries (GUC change / capacity resize /
 *   backend reset).  The next lookup/victim re-allocates lazily.
 */
extern void cluster_cr_cache_reset(void);

/* Test/diagnostic: number of currently-valid entries (this backend). */
extern int cluster_cr_cache_live_entries(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_CACHE_H */
