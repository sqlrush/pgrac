/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_cache.c
 *	  pgrac spec-3.10 D8 — cluster_unit tests for the backend-local CR block
 *	  cache (cluster_cr_cache.c): clock eviction, page_lsn version guard,
 *	  two-phase install, disable, reset.
 *
 *	  Links cluster_cr_cache.o; stubs the MemoryContext allocator (malloc-
 *	  backed) so the backend-local cache is exercisable standalone.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.10-cr-block-cache.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_cache.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_cache.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ---- MemoryContext allocator stubs (malloc-backed) ---- */
MemoryContext TopMemoryContext = (MemoryContext)0x1;

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	void *p = malloc(size);

	if (p != NULL)
		memset(p, 0, size);
	return p;
}

void
pfree(void *pointer)
{
	free(pointer);
}


/* ---- helpers ---- */

static ClusterCRCacheKey
mk_key(Oid rel, BlockNumber blk, SCN read_scn, XLogRecPtr lsn)
{
	ClusterCRCacheKey k;

	memset(&k, 0, sizeof(k));
	k.rlocator.spcOid = 1663;
	k.rlocator.dbOid = 5;
	k.rlocator.relNumber = rel;
	k.forknum = MAIN_FORKNUM;
	k.blockno = blk;
	k.read_scn = read_scn;
	k.base_page_lsn = lsn;
	return k;
}

/* miss -> install marker byte -> commit; returns whether an entry was evicted. */
static bool
install(const ClusterCRCacheKey *key, unsigned char marker)
{
	bool evicted = false;
	char *slot = cluster_cr_cache_victim_slot(key, 0, &evicted);

	slot[0] = (char)marker;
	cluster_cr_cache_commit_slot();
	return evicted;
}

/* install stamped with a specific lifecycle epoch (spec-5.53 D2c). */
static void
install_epoch(const ClusterCRCacheKey *key, unsigned char marker, uint64 epoch)
{
	bool evicted = false;
	char *slot = cluster_cr_cache_victim_slot(key, epoch, &evicted);

	slot[0] = (char)marker;
	cluster_cr_cache_commit_slot();
}


UT_TEST(test_lookup_empty_miss)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) == NULL), 1);
}

UT_TEST(test_install_then_hit_returns_image)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	const char *hit;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&k, 0xAB);
	hit = cluster_cr_cache_lookup(&k, 0, NULL);
	UT_ASSERT_EQ((int)(hit != NULL), 1);
	UT_ASSERT_EQ((int)(unsigned char)hit[0], 0xAB);
}

UT_TEST(test_page_lsn_guard_miss)
{
	/* same block + read_scn but different base_page_lsn -> MISS (the guard) */
	ClusterCRCacheKey k1 = mk_key(100, 7, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey k2 = mk_key(100, 7, (SCN)50, (XLogRecPtr)11);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&k1, 0x11);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k2, 0, NULL) == NULL), 1);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k1, 0, NULL) != NULL), 1);
}

UT_TEST(test_block_and_scn_discriminate)
{
	ClusterCRCacheKey base = mk_key(100, 7, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey diff_blk = mk_key(100, 8, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey diff_scn = mk_key(100, 7, (SCN)60, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&base, 0x22);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&diff_blk, 0, NULL) == NULL), 1);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&diff_scn, 0, NULL) == NULL), 1);
}

UT_TEST(test_eviction_at_capacity)
{
	ClusterCRCacheKey a = mk_key(1, 0, (SCN)1, (XLogRecPtr)1);
	ClusterCRCacheKey b = mk_key(2, 0, (SCN)1, (XLogRecPtr)1);
	ClusterCRCacheKey c = mk_key(3, 0, (SCN)1, (XLogRecPtr)1);

	cluster_cr_cache_max_blocks = 2;
	cluster_cr_cache_reset();
	UT_ASSERT_EQ((int)install(&a, 0xA1), 0); /* free slot -> no evict */
	UT_ASSERT_EQ((int)install(&b, 0xB1), 0); /* free slot -> no evict */
	UT_ASSERT_EQ((int)install(&c, 0xC1), 1); /* full -> evicts one */
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 2);
}

UT_TEST(test_clock_second_chance_keeps_referenced)
{
	/* cap 2: install A,B; touch A (set ref); install C -> evicts B not A */
	ClusterCRCacheKey a = mk_key(1, 0, (SCN)1, (XLogRecPtr)1);
	ClusterCRCacheKey b = mk_key(2, 0, (SCN)1, (XLogRecPtr)1);
	ClusterCRCacheKey c = mk_key(3, 0, (SCN)1, (XLogRecPtr)1);

	cluster_cr_cache_max_blocks = 2;
	cluster_cr_cache_reset();
	install(&a, 0xA2);
	install(&b, 0xB2);
	(void)cluster_cr_cache_lookup(&a, 0, NULL); /* second chance for A */
	install(&c, 0xC2);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&a, 0, NULL) != NULL), 1); /* A survived */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&b, 0, NULL) == NULL), 1); /* B evicted */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&c, 0, NULL) != NULL), 1);
}

UT_TEST(test_disabled_no_caching)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	bool evicted = true;
	const char *slot;

	cluster_cr_cache_max_blocks = 0; /* disabled */
	cluster_cr_cache_reset();
	slot = cluster_cr_cache_victim_slot(&k, 0, &evicted);
	UT_ASSERT_EQ((int)(slot != NULL), 1); /* fallback buffer */
	UT_ASSERT_EQ((int)evicted, 0);
	cluster_cr_cache_commit_slot();										  /* no-op */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) == NULL), 1); /* never caches */
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 0);
}

UT_TEST(test_two_phase_commit_visibility)
{
	/* a reserved-but-not-committed slot must NOT be servable (throw-safety) */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	bool evicted = false;
	char *slot;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	slot = cluster_cr_cache_victim_slot(&k, 0, &evicted);
	slot[0] = 0x33;
	/* before commit: lookup must miss */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) == NULL), 1);
	cluster_cr_cache_commit_slot();
	/* after commit: hit */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) != NULL), 1);
}

UT_TEST(test_reset_drops_all)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&k, 0x44);
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 1);
	cluster_cr_cache_reset();
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 0);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) == NULL), 1);
}

UT_TEST(test_capacity_resize_resets)
{
	/* changing max_blocks between calls resets the cache (GUC SET) */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&k, 0x55);
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 1);
	cluster_cr_cache_max_blocks = 16; /* resize -> ensure() drops + re-allocs */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 0, NULL) == NULL), 1);
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 0);
}


UT_TEST(test_epoch_fence_per_entry)
{
	/*
	 * spec-5.53 D2c: the per-entry lifecycle-epoch fence.  An image cached at
	 * epoch E must HIT at epoch E, but MISS (with out_epoch_stale) once the
	 * epoch advances (a relfilenode lifecycle event) — even though the key is an
	 * exact match.  This is the L1 half of "L1+L2 same fence" (rule 8.A: a
	 * relfilenode reuse can never serve a stale image).
	 */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	int reason;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install_epoch(&k, 0xE0, 100);

	/* same epoch -> HIT, reason NONE */
	reason = CR_CACHE_MISS_EPOCH;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 100, &reason) != NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_NONE);

	/* advanced epoch -> MISS + EPOCH reason (exact key, stale lifecycle) */
	reason = CR_CACHE_MISS_NONE;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 101, &reason) == NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_EPOCH);

	/* the stale lookup did not evict the entry: it still hits at its own epoch */
	reason = CR_CACHE_MISS_EPOCH;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 100, &reason) != NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_NONE);

	/* out_miss_reason may be NULL (caller does not care) */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k, 101, NULL) == NULL), 1);
}

UT_TEST(test_miss_reason_classification)
{
	/*
	 * spec-5.53 D5: a MISS is attributed to the most-specific near-miss so the
	 * over-miss diagnostics are reliable (L1 is a linear scan).  base_lsn (same
	 * block + read_scn, churned page version) > key (same block, diverged
	 * read_scn) > none (no same-block entry).  Epoch is covered by
	 * test_epoch_fence_per_entry.
	 */
	ClusterCRCacheKey base = mk_key(100, 7, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey churn = mk_key(100, 7, (SCN)50, (XLogRecPtr)11);	  /* diff base_lsn */
	ClusterCRCacheKey diff_scn = mk_key(100, 7, (SCN)60, (XLogRecPtr)10); /* diff read_scn */
	ClusterCRCacheKey other = mk_key(200, 7, (SCN)50, (XLogRecPtr)10);	  /* diff block addr */
	int reason;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&base, 0xB0);

	/* same block + read_scn, churned page version -> BASE_LSN */
	reason = CR_CACHE_MISS_NONE;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&churn, 0, &reason) == NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_BASE_LSN);

	/* same block, diverged read_scn -> KEY */
	reason = CR_CACHE_MISS_NONE;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&diff_scn, 0, &reason) == NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_KEY);

	/* different block address -> no near miss */
	reason = CR_CACHE_MISS_BASE_LSN;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&other, 0, &reason) == NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_NONE);

	/* exact hit -> reason NONE */
	reason = CR_CACHE_MISS_BASE_LSN;
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&base, 0, &reason) != NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_NONE);
}

UT_TEST(test_scn_range_negative_probe)
{
	/*
	 * spec-5.53 D6 / U17 — SCN-range "only with proof" negative probe.  A CR
	 * image constructed at read_scn = X answers visibility AT X.  If a commit in
	 * the interval (X, Y] changed the block's visible version, the image for Y
	 * differs — so reusing the X image for a read_scn = Y query would over-hit
	 * (8.A false-visible).  Because read_scn is in the key, the cache MISSes
	 * across read_scns (conservative-correct): the SCN-range relaxation is NOT
	 * implemented in v1 (it would require an airtight undo-chain identity proof
	 * that no commit/undo touched the block in the interval — §3.6).  This probe
	 * locks that exact-key behavior: relaxing it WITHOUT the proof is the over-hit
	 * this MISS prevents.
	 */
	ClusterCRCacheKey at_x = mk_key(100, 7, (SCN)100, (XLogRecPtr)10);
	ClusterCRCacheKey at_y = mk_key(100, 7, (SCN)200, (XLogRecPtr)10); /* Y > X, same page */
	const char *hit;
	int reason;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&at_x, 0x77); /* CR image valid at read_scn = X */

	/* the image at X hits exactly at X */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&at_x, 0, NULL) != NULL), 1);

	/* a later snapshot at Y must NOT be served the X image (no SCN-range reuse):
	 * MISS, classified as a read_scn divergence (key) — reconstruct at Y. */
	reason = CR_CACHE_MISS_NONE;
	hit = cluster_cr_cache_lookup(&at_y, 0, &reason);
	UT_ASSERT_EQ((int)(hit == NULL), 1);
	UT_ASSERT_EQ(reason, CR_CACHE_MISS_KEY);
}

int
main(int argc, char **argv)
{
	UT_PLAN(13);

	UT_RUN(test_lookup_empty_miss);
	UT_RUN(test_install_then_hit_returns_image);
	UT_RUN(test_page_lsn_guard_miss);
	UT_RUN(test_block_and_scn_discriminate);
	UT_RUN(test_eviction_at_capacity);
	UT_RUN(test_clock_second_chance_keeps_referenced);
	UT_RUN(test_disabled_no_caching);
	UT_RUN(test_two_phase_commit_visibility);
	UT_RUN(test_reset_drops_all);
	UT_RUN(test_capacity_resize_resets);
	UT_RUN(test_epoch_fence_per_entry);
	UT_RUN(test_miss_reason_classification);
	UT_RUN(test_scn_range_negative_probe);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
