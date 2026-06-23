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
	bool valid; /* false until commit / after eviction reserve */
	bool ref;	/* clock second-chance bit */
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
cluster_cr_cache_lookup(const ClusterCRCacheKey *key)
{
	int i;

	if (key == NULL)
		return NULL;

	cr_cache_ensure();
	for (i = 0; i < cr_cache_capacity; i++) {
		if (cr_cache[i].valid && cluster_cr_cache_key_equal(&cr_cache[i].key, key)) {
			cr_cache[i].ref = true; /* second chance */
			return cr_cache[i].page;
		}
	}
	return NULL;
}

char *
cluster_cr_cache_victim_slot(const ClusterCRCacheKey *key, bool *evicted)
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
	cr_cache_last_victim = idx;

	if (evicted != NULL)
		*evicted = ev;
	return cr_cache[idx].page;
}

void
cluster_cr_cache_commit_slot(void)
{
	if (cr_cache_last_victim >= 0 && cr_cache_last_victim < cr_cache_capacity) {
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
