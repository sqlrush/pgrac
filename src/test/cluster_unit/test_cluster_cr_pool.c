/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_pool.c
 *	  pgrac spec-5.51 — cluster_unit tests for the dedicated shared CR buffer
 *	  pool substrate (cluster_cr_pool.c): copy-out lookup, two-phase
 *	  reserve/publish/abort with generation+key+epoch guards, lifecycle epoch
 *	  gate, clock eviction, disabled (size=0) zero-memory.
 *
 *	  Links cluster_cr_pool.o + cluster_cr_cache.o (canonical key_equal, D6).
 *	  Stubs ShmemInitStruct (malloc-backed, fresh pool per init for isolation),
 *	  the named LWLock tranche, and LWLock acquire/release (single-threaded).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.51-dedicated-shared-cr-buffer-pool-v1.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_pool.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr_pool.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion */
#include "storage/bufpage.h"	   /* BLCKSZ */
#include "storage/lwlock.h"
#include "storage/shmem.h" /* ShmemInitStruct proto */

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

/* ---- MemoryContext stubs (cluster_cr_cache.o needs them) ---- */
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

/* ---- shmem + LWLock stubs ---- *
 * ShmemInitStruct returns a FRESH malloc each call (found=false) so every
 * cluster_cr_pool_shmem_init() in a test yields an isolated pool.  LWLocks are
 * no-ops (the unit test is single-threaded). */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	void *p = malloc(size);

	if (p != NULL)
		memset(p, 0, size);
	if (foundPtr != NULL)
		*foundPtr = false;
	return p;
}

static LWLockPadded stub_locks[64];

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name pg_attribute_unused())
{
	return stub_locks;
}

void
RequestNamedLWLockTranche(const char *tranche_name pg_attribute_unused(),
						  int num_lwlocks pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *l pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *l pg_attribute_unused())
{}

void
cluster_shmem_register_region(const ClusterShmemRegion *r pg_attribute_unused())
{}

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

/* enable pool with `blocks` slots and (re)init a fresh shared region. */
static void
pool_init(int blocks)
{
	cluster_shared_cr_pool_enabled = (blocks > 0);
	cluster_shared_cr_pool_size_blocks = blocks;
	cluster_cr_pool_shmem_init();
}

/* construct-and-publish a one-marker page for `key`; returns true if published. */
static bool
pool_install(const ClusterCRCacheKey *key, unsigned char marker)
{
	ClusterCRPoolHandle h;
	char page[BLCKSZ];

	if (!cluster_cr_pool_reserve(key, &h))
		return false;
	memset(page, 0, BLCKSZ);
	page[0] = (char)marker;
	cluster_cr_pool_publish(&h, page);
	return true;
}


UT_TEST(test_disabled_zero_memory)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];
	ClusterCRPoolHandle h;

	pool_init(0);												/* disabled */
	UT_ASSERT_EQ((int)cluster_cr_pool_shmem_size(), 0);			/* zero bytes */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* miss */
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h), 0);		/* no reserve */
	UT_ASSERT_EQ((int)h.valid, 0);
	UT_ASSERT_EQ((int)(cluster_cr_pool_current_epoch() == 0), 1);
}

UT_TEST(test_install_then_hit_copy_out)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	pool_init(64);
	memset(dst, 0, BLCKSZ);
	UT_ASSERT_EQ((int)pool_install(&k, 0xAB), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xAB);
}

UT_TEST(test_copy_out_is_independent_of_pool)
{
	/* dst is caller-owned: scribbling on it must not affect the pool / a later
	 * lookup.  Proves lookup_copy memcpy's out, never aliases a shared slot. */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	char dst1[BLCKSZ];
	char dst2[BLCKSZ];

	pool_init(64);
	pool_install(&k, 0x5A);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst1), 1);
	dst1[0] = (char)0xFF;							 /* caller scribbles its copy */
	UT_ASSERT_EQ((int)(unsigned char)dst1[0], 0xFF); /* scribble landed in caller's copy */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst2), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst2[0], 0x5A); /* pool image intact (copy-out independence) */
}

UT_TEST(test_no_false_hit_diff_key)
{
	ClusterCRCacheKey base = mk_key(100, 7, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey dblk = mk_key(100, 8, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey dscn = mk_key(100, 7, (SCN)60, (XLogRecPtr)10);
	ClusterCRCacheKey dlsn = mk_key(100, 7, (SCN)50, (XLogRecPtr)11);
	char dst[BLCKSZ];

	pool_init(64);
	pool_install(&base, 0x22);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&dblk, dst), 0);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&dscn, dst), 0);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&dlsn, dst), 0);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&base, dst), 1); /* exact hits */
}

UT_TEST(test_reserved_not_served)
{
	/* a RESERVED (not-yet-published) slot must MISS (two-phase visibility). */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h;
	char dst[BLCKSZ];

	pool_init(64);
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* reserved -> miss */
	cluster_cr_pool_publish(&h, dst);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1); /* now hit */
}

UT_TEST(test_abort_releases_slot)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h;
	char dst[BLCKSZ];

	pool_init(64);
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h), 1);
	cluster_cr_pool_abort(&h);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* never installed */
	UT_ASSERT_EQ(cluster_cr_pool_live_entries(), 0);
}

UT_TEST(test_stale_handle_generation_aba)
{
	/* Isolate the generation guard (not the state guard): a 1-slot-per-segment
	 * pool (size 16) forces the SAME slot to be reused.  reserve h1 -> abort
	 * (gen++) -> reserve h2 SAME slot (now RESERVED, newer gen).  Publishing the
	 * STALE h1 finds the slot RESERVED with key matching but a DIFFERENT
	 * generation: only the generation guard rejects it.  Without the guard, h1
	 * would clobber h2's reservation. */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h1, h2;
	char page[BLCKSZ];
	char dst[BLCKSZ];

	pool_init(16); /* per_seg == 1 -> same slot reused */
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h1), 1);
	cluster_cr_pool_abort(&h1);								/* slot FREE, gen++ */
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h2), 1); /* SAME slot, RESERVED */
	UT_ASSERT_EQ((int)(h2.generation != h1.generation), 1);

	memset(page, 0, BLCKSZ);
	page[0] = (char)0xA1;
	cluster_cr_pool_publish(&h1, page); /* STALE gen on a RESERVED slot -> ignored */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* still RESERVED */

	page[0] = (char)0xB2;
	cluster_cr_pool_publish(&h2, page); /* the live handle installs */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xB2); /* h1 never clobbered it */
}

UT_TEST(test_publish_key_mismatch_ignored)
{
	/* a handle whose key no longer matches the slot must not install. */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h;
	char page[BLCKSZ];
	char dst[BLCKSZ];

	pool_init(64);
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h), 1);
	h.key = mk_key(999, 0, (SCN)1, (XLogRecPtr)1); /* corrupt handle key */
	memset(page, 0, BLCKSZ);
	page[0] = (char)0xCC;
	cluster_cr_pool_publish(&h, page);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* not installed */
}

UT_TEST(test_epoch_gate_bump_invalidates)
{
	/* publish at epoch E; bump epoch; lookup must MISS the stale-epoch slot. */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	pool_init(64);
	pool_install(&k, 0x77);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1); /* hit at cur epoch */
	cluster_cr_pool_bump_epoch();
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* epoch stale -> miss */
}

UT_TEST(test_publish_epoch_stale_releases)
{
	/* reserve at E; bump epoch during "construction"; publish must release the
	 * RESERVED slot (no install, no leak) + bump publish_stale_release. */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h;
	char page[BLCKSZ];
	char dst[BLCKSZ];
	uint64 before;

	pool_init(64);
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h), 1);
	before = cluster_cr_pool_publish_stale_release_count();
	cluster_cr_pool_bump_epoch(); /* epoch advanced mid-construction */
	memset(page, 0, BLCKSZ);
	page[0] = (char)0x88;
	cluster_cr_pool_publish(&h, page);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* not installed */
	UT_ASSERT_EQ((int)(cluster_cr_pool_publish_stale_release_count() == before + 1), 1);
	/* slot released, not leaked: a fresh reserve+publish at the new epoch works */
	UT_ASSERT_EQ((int)pool_install(&k, 0x99), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0x99);
}

UT_TEST(test_clock_eviction_at_capacity)
{
	/* 16 slots / 16 segments (1 slot per segment): inserting 17 distinct keys
	 * forces at least one segment collision -> at least one eviction. */
	int i;
	uint64 ev0;

	pool_init(16);
	ev0 = cluster_cr_pool_evict_count();
	for (i = 0; i < 17; i++) {
		ClusterCRCacheKey k = mk_key(1, (BlockNumber)i, (SCN)1, (XLogRecPtr)1);

		pool_install(&k, (unsigned char)(0x40 + i));
	}
	UT_ASSERT_EQ((int)(cluster_cr_pool_evict_count() > ev0), 1);
	/* never more live than capacity */
	UT_ASSERT_EQ((int)(cluster_cr_pool_live_entries() <= 16), 1);
}

UT_TEST(test_counters_hit_miss)
{
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey miss = mk_key(200, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];
	uint64 h0, m0;

	pool_init(64);
	pool_install(&k, 0x11);
	h0 = cluster_cr_pool_hit_count();
	m0 = cluster_cr_pool_miss_count();
	cluster_cr_pool_lookup_copy(&k, dst);	 /* hit */
	cluster_cr_pool_lookup_copy(&miss, dst); /* miss */
	UT_ASSERT_EQ((int)(cluster_cr_pool_hit_count() == h0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_miss_count() == m0 + 1), 1);
}


UT_TEST(test_relfilenode_reuse_floor)
{
	/*
	 * spec-5.53 U5/U8 (8.A core, D2b floor): the relfilenode-reuse over-hit
	 * (F0-10).  relNumber=Oid is reusable: rel A (relNumber R) is cached, A is
	 * DROPped (epoch bump), then a NEW rel B reuses relNumber R.  Even in the
	 * WORST case — B's block coincides on the exact same key (block/read_scn/
	 * base_page_lsn) as A's cached image — the lifecycle-epoch floor must force a
	 * MISS (never serve A's stale image for B).  This is the floor working under
	 * a normal bump (the provably-complete bump at smgrdounlinkall); the
	 * relfilenode-reuse MISS is attributed to epoch_mismatch (D0=RED floor-only,
	 * §3.2: locator_reuse_reject is the belt's counter and stays 0).
	 */
	ClusterCRCacheKey ka = mk_key(42, 0, (SCN)50, (XLogRecPtr)10); /* rel A, relNumber 42 */
	char dst[BLCKSZ];
	uint64 epoch0, reject0;

	pool_init(64);
	pool_install(&ka, 0xAA); /* A's CR image cached */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&ka, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xAA);

	epoch0 = cluster_cr_pool_epoch_mismatch_count();
	reject0 = cluster_cr_pool_locator_reuse_reject_count();

	/* DROP A -> relfilenode 42 unlinked -> the provably-complete floor bump. */
	cluster_cr_pool_bump_epoch();

	/*
	 * rel B reuses relNumber 42 and (worst case) hits the identical key.  The
	 * epoch floor forces a MISS: A's image is never served for B (rule 8.A).
	 */
	memset(dst, 0xEE, BLCKSZ); /* poison: a stale hit would overwrite this */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&ka, dst), 0); /* MISS, not A's image */
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xEE);				 /* dst untouched */

	/* the reuse MISS was attributed to the epoch floor, and the belt counter
	 * (catalog-incarnation, D0=RED → absent) stayed 0. */
	UT_ASSERT_EQ((int)(cluster_cr_pool_epoch_mismatch_count() == epoch0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_locator_reuse_reject_count() == reject0), 1);

	/* B can now cache its OWN image at the new epoch and hit it. */
	pool_install(&ka, 0xBB);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&ka, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xBB); /* B's image, never A's */
}

UT_TEST(test_mismatch_generation_counter)
{
	/* spec-5.53 D5/U12: the publish-side ABA (generation moved) bumps
	 * generation_mismatch (distinct from the epoch-stale release). */
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h1, h2;
	char page[BLCKSZ];
	uint64 gen0;

	pool_init(16); /* per_seg == 1 -> same slot reused */
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h1), 1);
	cluster_cr_pool_abort(&h1);								/* gen++ */
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve(&k, &h2), 1); /* SAME slot, newer gen */

	gen0 = cluster_cr_pool_generation_mismatch_count();
	memset(page, 0, BLCKSZ);
	page[0] = (char)0xA1;
	cluster_cr_pool_publish(&h1, page); /* stale gen on RESERVED slot -> ABA reject */
	UT_ASSERT_EQ((int)(cluster_cr_pool_generation_mismatch_count() == gen0 + 1), 1);
}

UT_TEST(test_mismatch_counters_all_five)
{
	/*
	 * spec-5.53 D5/U15: all five mismatch diagnostics increment accurately, and
	 * locator_reuse_reject (the catalog-incarnation belt observable) stays 0 in
	 * this D0=RED floor-only build.  L1-detected near-misses (key / base_lsn /
	 * epoch) record into the shared pool counters via the note helpers (the
	 * classification itself is covered by test_cluster_cr_cache.c); L2 epoch and
	 * publish-ABA are exercised by the dedicated tests above.
	 */
	uint64 k0, e0, b0, g0, r0;
	ClusterCRCacheKey k = mk_key(100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	pool_init(64);
	k0 = cluster_cr_pool_key_mismatch_count();
	e0 = cluster_cr_pool_epoch_mismatch_count();
	b0 = cluster_cr_pool_base_lsn_mismatch_count();
	g0 = cluster_cr_pool_generation_mismatch_count();
	r0 = cluster_cr_pool_locator_reuse_reject_count();

	cluster_cr_pool_note_key_mismatch();
	cluster_cr_pool_note_base_lsn_mismatch();
	cluster_cr_pool_note_l1_epoch_mismatch();

	/* L2 epoch fence: install, bump, lookup stale -> epoch_mismatch (a 2nd one) */
	pool_install(&k, 0x33);
	cluster_cr_pool_bump_epoch();
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0);

	UT_ASSERT_EQ((int)(cluster_cr_pool_key_mismatch_count() == k0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_epoch_mismatch_count() == e0 + 2), 1); /* L1 note + L2 */
	UT_ASSERT_EQ((int)(cluster_cr_pool_base_lsn_mismatch_count() == b0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_generation_mismatch_count() == g0), 1); /* untouched here */
	/* locator_reuse_reject is the belt observable: never bumped in floor-only. */
	UT_ASSERT_EQ((int)(cluster_cr_pool_locator_reuse_reject_count() == r0), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_locator_reuse_reject_count() == 0), 1);
}

int
main(int argc, char **argv)
{
	UT_PLAN(15);

	UT_RUN(test_disabled_zero_memory);
	UT_RUN(test_install_then_hit_copy_out);
	UT_RUN(test_copy_out_is_independent_of_pool);
	UT_RUN(test_no_false_hit_diff_key);
	UT_RUN(test_reserved_not_served);
	UT_RUN(test_abort_releases_slot);
	UT_RUN(test_stale_handle_generation_aba);
	UT_RUN(test_publish_key_mismatch_ignored);
	UT_RUN(test_epoch_gate_bump_invalidates);
	UT_RUN(test_publish_epoch_stale_releases);
	UT_RUN(test_clock_eviction_at_capacity);
	UT_RUN(test_counters_hit_miss);
	UT_RUN(test_relfilenode_reuse_floor);
	UT_RUN(test_mismatch_generation_counter);
	UT_RUN(test_mismatch_counters_all_five);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
