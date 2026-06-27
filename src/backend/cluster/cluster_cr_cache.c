/*-------------------------------------------------------------------------
 *
 * cluster_cr_cache.c
 *	  pgrac backend-local CR block cache (spec-3.10 D2).
 *
 *	  Fixed-size backend-local cache of full-block CR images, clock (second-
 *	  chance) eviction.  See cluster_cr_cache.h for the contract.  No shmem, no
 *	  LWLock: a backend is single-threaded, so the cache is touched only by its
 *	  owner (spec-3.10 §2.5 / L106).  The cached image is a pure function of the
 *	  key, so there is no invalidation — base_page_lsn in the key forces a miss
 *	  after any WAL-logged physical relayout (HOT-prune / VACUUM).
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
 *	  src/backend/cluster/cluster_cr_cache.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_cache.h"
#include "storage/bufpage.h"		/* BLCKSZ */
#include "storage/relfilelocator.h" /* RelFileLocatorEquals */
#include "utils/memutils.h"			/* TopMemoryContext */

/*
 * GUC backing variable (registered in cluster_guc.c, spec-3.10 D4).  Owned here
 * so the cache module is self-contained for cluster_unit.
 */
int cluster_cr_cache_max_blocks = 64;

/* Defensive upper bound mirroring the GUC max (cluster_guc.c). */
#define CR_CACHE_HARD_CAP 4096

typedef struct ClusterCRCacheEntry {
	ClusterCRCacheKey key;
	uint64 pool_epoch; /* spec-5.53 D2c: lifecycle epoch stamped at reserve; the
						* L1 fence serves an entry only at the current epoch, so a
						* relfilenode-reuse (epoch bump) forces a MISS even on an
						* exact key match (L1+L2 same fence). */
	uint64 rel_gen;	   /* spec-5.56 D4: per-relation lifecycle generation stamped
						* at commit (the second half of the composite {pool_epoch,
						* rel_gen} fence, P1-c).  0 = no per-rel fence (gen table
						* disabled => epoch-only, spec-5.53 equivalence).  When the
						* gen table is enabled this is the locator's generation at
						* install; a per-relation unlink bumps the locator's gen so
						* this stale entry MISSes WITHOUT a global epoch bump (the
						* whole point of Part B: unrelated relations stay warm). */
	bool valid;		   /* false until commit / after eviction reserve */
	bool ref;		   /* clock second-chance bit */
	char page[BLCKSZ];
} ClusterCRCacheEntry;

/* Backend-local state (TopMemoryContext, lazily allocated). */
static ClusterCRCacheEntry *cr_cache = NULL;
static int cr_cache_capacity = 0;	   /* allocated entry count */
static int cr_cache_hand = 0;		   /* clock hand */
static int cr_cache_last_victim = -1;  /* slot reserved by victim_slot */
static char *cr_cache_fallback = NULL; /* disabled-mode single buffer */


/*
 * cluster_cr_cache_key_equal -- canonical CR cache key equality (spec-5.51 D6).
 *
 * PGRAC: promoted from the former file-local cr_key_eq so the shared CR pool
 * (cluster_cr_pool.c) reuses the SAME field-wise compare.  Field-wise (never
 * memcmp): ClusterCRCacheKey has alignment padding that is not part of identity.
 */
bool
cluster_cr_cache_key_equal(const ClusterCRCacheKey *a, const ClusterCRCacheKey *b)
{
	return RelFileLocatorEquals(a->rlocator, b->rlocator) && a->forknum == b->forknum
		   && a->blockno == b->blockno && a->read_scn == b->read_scn
		   && a->base_page_lsn == b->base_page_lsn;
}

/*
 * Reconcile the allocated cache with cluster_cr_cache_max_blocks.  A capacity
 * change (GUC SIGHUP / SET) drops all entries and re-allocates lazily.
 */
static void
cr_cache_ensure(void)
{
	int want = cluster_cr_cache_max_blocks;

	if (want < 0)
		want = 0;
	if (want > CR_CACHE_HARD_CAP)
		want = CR_CACHE_HARD_CAP;

	if (want == cr_cache_capacity)
		return;

	/* capacity changed: reset then re-allocate */
	cluster_cr_cache_reset();
	if (want > 0) {
		cr_cache = (ClusterCRCacheEntry *)MemoryContextAllocZero(
			TopMemoryContext, sizeof(ClusterCRCacheEntry) * (Size)want);
		cr_cache_capacity = want;
	}
}

/*
 * Pick a victim slot index.  Prefer a free (invalid) slot so the cache fills
 * before evicting; otherwise run the clock until a slot with a clear ref bit
 * is found.  *evicted is true iff a currently-valid entry is displaced.
 */
static int
cr_cache_pick_victim(bool *evicted)
{
	int i;

	for (i = 0; i < cr_cache_capacity; i++) {
		if (!cr_cache[i].valid) {
			*evicted = false;
			return i;
		}
	}

	/* all valid: second-chance clock sweep (bounded: at most 2*capacity steps) */
	for (;;) {
		int idx = cr_cache_hand;

		cr_cache_hand = (cr_cache_hand + 1) % cr_cache_capacity;
		if (cr_cache[idx].ref) {
			cr_cache[idx].ref = false;
			continue;
		}
		*evicted = true;
		return idx;
	}
}


const char *
cluster_cr_cache_lookup(const ClusterCRCacheKey *key, uint64 cur_epoch, int *out_miss_reason)
{
	/* spec-5.56: epoch-only fence (cur_rel_gen == 0 == "no per-rel fence").  This
	 * preserves the spec-3.10/5.53 behavior for callers that do not (or cannot)
	 * consult the per-relation generation table (cluster_unit substrate tests). */
	return cluster_cr_cache_lookup_fenced(key, cur_epoch, 0, out_miss_reason);
}

const char *
cluster_cr_cache_lookup_fenced(const ClusterCRCacheKey *key, uint64 cur_epoch, uint64 cur_rel_gen,
							   int *out_miss_reason)
{
	int i;
	int reason = CR_CACHE_MISS_NONE;

	if (out_miss_reason != NULL)
		*out_miss_reason = CR_CACHE_MISS_NONE;
	if (key == NULL)
		return NULL;

	cr_cache_ensure();
	for (i = 0; i < cr_cache_capacity; i++) {
		ClusterCRCacheEntry *e = &cr_cache[i];

		if (!e->valid)
			continue;

		if (cluster_cr_cache_key_equal(&e->key, key)) {
			/*
			 * spec-5.53 D2c + spec-5.56 D4: per-entry COMPOSITE lifecycle fence
			 * {pool_epoch, rel_gen} (P1-c).  An exact key match whose stamped
			 * GLOBAL epoch is stale (a coarse DROP/TRUNCATE/sinval since install)
			 * OR whose stamped PER-RELATION generation no longer matches the
			 * locator's current generation (a fine-grained per-relation unlink in
			 * GO mode, which does NOT bump the global epoch) is a stale image from
			 * before a lifecycle event.  Do NOT serve it (rule 8.A: never a stale
			 * hit); report it so the caller can count the fence firing.  The stale
			 * entry is left for the clock to evict (its (epoch, rel_gen) pair can
			 * only be re-hit at values that can no longer occur).  cur_rel_gen == 0
			 * (gen table disabled / locator unregistered) makes the rel_gen half a
			 * no-op (entries stamped 0 ⇒ epoch-only, spec-5.53 equivalence).
			 */
			if (e->pool_epoch != cur_epoch || e->rel_gen != cur_rel_gen) {
				if (out_miss_reason != NULL)
					*out_miss_reason = CR_CACHE_MISS_EPOCH;
				return NULL; /* lifecycle fence (epoch | rel_gen): stop here */
			}
			e->ref = true; /* second chance */
			return e->page;
		}

		/*
		 * spec-5.53 D5: over-miss near-miss classification.  L1 is a linear scan,
		 * so this is reliable (unlike the segmented L2 pool).  Same physical
		 * block address (locator+fork+block) but a churned page version or a
		 * diverged snapshot — a hit-rate miss, not a reuse hazard.  Keep the
		 * most-specific bucket (base_lsn over key); an exact-key stale-epoch match
		 * found later still wins (it returns immediately above).
		 */
		if (reason != CR_CACHE_MISS_BASE_LSN && RelFileLocatorEquals(e->key.rlocator, key->rlocator)
			&& e->key.forknum == key->forknum && e->key.blockno == key->blockno) {
			if (e->key.read_scn == key->read_scn && e->key.base_page_lsn != key->base_page_lsn)
				reason = CR_CACHE_MISS_BASE_LSN; /* churned page version (F0-14) */
			else if (e->key.read_scn != key->read_scn && reason == CR_CACHE_MISS_NONE)
				reason = CR_CACHE_MISS_KEY; /* diverged read_scn (F0-16) */
		}
	}

	if (out_miss_reason != NULL)
		*out_miss_reason = reason;
	return NULL;
}

char *
cluster_cr_cache_victim_slot(const ClusterCRCacheKey *key, uint64 cur_epoch, bool *evicted)
{
	int idx;
	bool ev = false;

	cr_cache_ensure();

	if (cr_cache_capacity == 0) {
		/* disabled: construct into the single fallback buffer */
		if (cr_cache_fallback == NULL)
			cr_cache_fallback = (char *)MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
		cr_cache_last_victim = -1;
		if (evicted != NULL)
			*evicted = false;
		return cr_cache_fallback;
	}

	idx = cr_cache_pick_victim(&ev);

	/* Reserve INVALID until commit: a throwing construction must not leave a
	 * half-built entry servable (spec-3.10 §2.1). */
	cr_cache[idx].valid = false;
	cr_cache[idx].ref = false;
	cr_cache[idx].key = (key != NULL) ? *key : cr_cache[idx].key;
	cr_cache[idx].pool_epoch = cur_epoch; /* spec-5.53 D2c: stamp the fence epoch */
	cr_cache[idx].rel_gen = 0;			  /* spec-5.56 D4: provisional; commit re-stamps
										   * the authoritative install gen.  The slot is
										   * INVALID until commit so this 0 is never served. */
	cr_cache_last_victim = idx;

	if (evicted != NULL)
		*evicted = ev;
	return cr_cache[idx].page;
}

void
cluster_cr_cache_commit_slot(void)
{
	/* spec-5.56: epoch-only callers commit with rel_gen == 0 (no per-rel fence). */
	cluster_cr_cache_commit_slot_gen(0);
}

void
cluster_cr_cache_commit_slot_gen(uint64 rel_gen)
{
	if (cr_cache_last_victim >= 0 && cr_cache_last_victim < cr_cache_capacity) {
		/*
		 * spec-5.56 D4: stamp the AUTHORITATIVE per-relation generation (the
		 * install_gen the caller obtained from cluster_cr_pool_register_locator,
		 * or the gen the L2 hit matched).  This is stamped at commit, not at
		 * victim_slot reserve, because the install gen is known only after the
		 * locator is registered (P2-e′: a first-registration locator's install_gen
		 * is 1 while the captured start_rel_gen was 0 — comparing the entry against
		 * the install_gen at commit-recheck avoids falsely skipping the first cache).
		 */
		cr_cache[cr_cache_last_victim].rel_gen = rel_gen;
		cr_cache[cr_cache_last_victim].valid = true;
		/*
		 * ref stays FALSE on install: an image becomes "referenced" only when
		 * a later lookup HITS it.  This makes the clock favor re-read images
		 * over single-use ones (a CR image installed but never hit again is the
		 * cheapest to evict), which fits the construct-once / hit-many pattern.
		 */
	}
	cr_cache_last_victim = -1;
}

void
cluster_cr_cache_reset(void)
{
	if (cr_cache != NULL) {
		pfree(cr_cache);
		cr_cache = NULL;
	}
	cr_cache_capacity = 0;
	cr_cache_hand = 0;
	cr_cache_last_victim = -1;
	/* fallback buffer is retained: it has no key, only disabled-mode scratch. */
}

int
cluster_cr_cache_live_entries(void)
{
	int i;
	int n = 0;

	for (i = 0; i < cr_cache_capacity; i++) {
		if (cr_cache[i].valid)
			n++;
	}
	return n;
}
