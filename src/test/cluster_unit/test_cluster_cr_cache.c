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
	char *slot = cluster_cr_cache_victim_slot(key, &evicted);

	slot[0] = (char)marker;
	cluster_cr_cache_commit_slot();
	return evicted;
}


UT_TEST(test_lookup_empty_miss)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) == NULL), 1);
}

UT_TEST(test_install_then_hit_returns_image)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	const char *hit;

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&k, 0xAB);
	hit = cluster_cr_cache_lookup(&k);
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
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k2) == NULL), 1);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k1) != NULL), 1);
}

UT_TEST(test_block_and_scn_discriminate)
{
	ClusterCRCacheKey base = mk_key(100, 7, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey diff_blk = mk_key(100, 8, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey diff_scn = mk_key(100, 7, (SCN)60, (XLogRecPtr)10);

	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	install(&base, 0x22);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&diff_blk) == NULL), 1);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&diff_scn) == NULL), 1);
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
	(void)cluster_cr_cache_lookup(&a); /* second chance for A */
	install(&c, 0xC2);
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&a) != NULL), 1); /* A survived */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&b) == NULL), 1); /* B evicted */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&c) != NULL), 1);
}

UT_TEST(test_disabled_no_caching)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	bool evicted = true;
	const char *slot;

	cluster_cr_cache_max_blocks = 0; /* disabled */
	cluster_cr_cache_reset();
	slot = cluster_cr_cache_victim_slot(&k, &evicted);
	UT_ASSERT_EQ((int)(slot != NULL), 1); /* fallback buffer */
	UT_ASSERT_EQ((int)evicted, 0);
	cluster_cr_cache_commit_slot();								 /* no-op */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) == NULL), 1); /* never caches */
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
	slot = cluster_cr_cache_victim_slot(&k, &evicted);
	slot[0] = 0x33;
	/* before commit: lookup must miss */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) == NULL), 1);
	cluster_cr_cache_commit_slot();
	/* after commit: hit */
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) != NULL), 1);
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
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) == NULL), 1);
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
	UT_ASSERT_EQ((int)(cluster_cr_cache_lookup(&k) == NULL), 1);
	UT_ASSERT_EQ(cluster_cr_cache_live_entries(), 0);
}


int
main(int argc, char **argv)
{
	UT_PLAN(10);

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

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
