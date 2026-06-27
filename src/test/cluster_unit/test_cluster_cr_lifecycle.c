/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_lifecycle.c
 *	  pgrac spec-5.56 — cluster_unit tests for the CR pool lifecycle /
 *	  invalidation contract (C1-C4) and the per-relation generation table
 *	  (Part B): the composite {pool_epoch, rel_gen} fence (P1-c), register-at-
 *	  install (INV-G3), per-relation unlink completeness (INV-G1), append-only
 *	  eviction safety incl. backend-local L1 survivors (INV-G2), gen-table
 *	  overflow serve-but-skip-cache, disabled equivalence, and the C3/C4
 *	  observation counters.
 *
 *	  Links cluster_cr_pool.o + cluster_cr_cache.o.  Stubs ShmemInitStruct
 *	  (malloc-backed, fresh region per init), the named LWLock tranche, and
 *	  LWLock acquire/release (single-threaded), exactly like test_cluster_cr_pool.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.56-cr-pool-lifecycle-invalidation.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_lifecycle.c
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

/* ---- shmem + LWLock stubs (fresh region per init; locks are no-ops) ---- */
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
mk_key(Oid spc, Oid db, Oid rel, BlockNumber blk, SCN read_scn, XLogRecPtr lsn)
{
	ClusterCRCacheKey k;

	memset(&k, 0, sizeof(k));
	k.rlocator.spcOid = spc;
	k.rlocator.dbOid = db;
	k.rlocator.relNumber = rel;
	k.forknum = MAIN_FORKNUM;
	k.blockno = blk;
	k.read_scn = read_scn;
	k.base_page_lsn = lsn;
	return k;
}

/* Bring up the pool + the per-relation generation table.  relgen_slots == 0
 * leaves the gen table disabled (coarse / spec-5.53 equivalence). */
static void
lifecycle_init(int pool_blocks, int relgen_slots)
{
	cluster_shared_cr_pool_enabled = (pool_blocks > 0);
	cluster_shared_cr_pool_size_blocks = pool_blocks;
	cluster_cr_pool_rel_generation_slots = relgen_slots;
	cluster_cr_cache_max_blocks = 64;
	cluster_cr_cache_reset();
	cluster_cr_pool_shmem_init();
	cluster_cr_pool_relgen_shmem_init();
}

/* Register `key`'s locator, then construct-and-publish a one-marker page at the
 * registered generation.  Returns true iff published (registered + reserved). */
static bool
l2_install(const ClusterCRCacheKey *key, unsigned char marker)
{
	ClusterCRPoolHandle h;
	char page[BLCKSZ];
	uint64 gen = 0;

	if (!cluster_cr_pool_register_locator(key->rlocator, &gen))
		return false; /* gen table full: serve-but-skip-cache */
	if (!cluster_cr_pool_reserve_gen(key, gen, &h))
		return false;
	memset(page, 0, BLCKSZ);
	page[0] = (char)marker;
	cluster_cr_pool_publish(&h, page);
	return true;
}

/* ============================================================ */

/* U1 (C1 floor): coarse global-epoch bump (gen table disabled) invalidates an
 * installed image — the DROP→different-relid-reuse floor (spec-5.53 D2b). */
UT_TEST(test_u1_c1_floor_global_epoch)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	lifecycle_init(64, 0); /* relgen disabled => coarse floor */
	UT_ASSERT_EQ((int)l2_install(&k, 0xA1), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1); /* warm hit */
	cluster_cr_pool_bump_epoch();								/* relid unlink (floor) */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* stale: MISS */
}

/* U2 (C2 floor): a second relfilenode unlink (same relid rewrite) is the same
 * coarse floor — a fresh install at the new epoch hits; the old image stays
 * MISS even on an exact key. */
UT_TEST(test_u2_c2_floor_same_relid_rewrite)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	lifecycle_init(64, 0);
	l2_install(&k, 0x22);
	cluster_cr_pool_bump_epoch(); /* TRUNCATE: old relfilenode unlink */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0);
	/* a fresh image at the new epoch (the new relfilenode) hits */
	l2_install(&k, 0x23);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0x23);
}

/* U3 (C3 image-immutability, INV-R1): a cached image is fully-materialized bytes;
 * retention horizon advancing past its read_scn does NOT change it.  At the
 * substrate level the pool stores immutable bytes and never consults undo, so a
 * later lookup returns the SAME bytes (a horizon advance is a no-op on the pool;
 * cluster_cr.c only OBSERVES it, spec-5.56 D2). */
UT_TEST(test_u3_c3_image_immutability)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 200, 3, (SCN)77, (XLogRecPtr)20);
	char dst1[BLCKSZ], dst2[BLCKSZ];

	lifecycle_init(64, 256);
	UT_ASSERT_EQ((int)l2_install(&k, 0x5C), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst1), 1);
	/* no lifecycle event; "time passes / horizon advances" — bytes are immutable */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst2), 1);
	UT_ASSERT_EQ(memcmp(dst1, dst2, BLCKSZ), 0);
	UT_ASSERT_EQ((int)(unsigned char)dst2[0], 0x5C);
}

/* U5 (C3 retention counter): the pure-observation counter is bumped by the note
 * API (the cluster_cr.c hot-path helper calls it; U5/injection exercises it). */
UT_TEST(test_u5_c3_retention_counter)
{
	uint64 before;

	lifecycle_init(64, 256);
	before = cluster_cr_pool_retention_horizon_advance_noted_count();
	cluster_cr_pool_note_retention_horizon_advance();
	cluster_cr_pool_note_retention_horizon_advance();
	UT_ASSERT_EQ((int)(cluster_cr_pool_retention_horizon_advance_noted_count() == before + 2), 1);
}

/* U7 (C4 reconfig two-class invariant + counter): an image survives a simulated
 * reconfig (membership changes nothing about the materialized bytes); the
 * evidence counter records the invariant was exercised. */
UT_TEST(test_u7_c4_reconfig_survives)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 300, 1, (SCN)90, (XLogRecPtr)30);
	char dst[BLCKSZ];
	uint64 before;

	lifecycle_init(64, 256);
	l2_install(&k, 0x77);
	before = cluster_cr_pool_reconfig_intra_survived_count();
	/* simulate a reconfig epoch advance: the CR layer does NOT invalidate */
	cluster_cr_pool_note_reconfig_intra_survived();
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1); /* image survives */
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0x77);
	UT_ASSERT_EQ((int)(cluster_cr_pool_reconfig_intra_survived_count() == before + 1), 1);
}

/* U9 (INV-L0 fail-closed): an inconsistent per-relation generation forces a MISS,
 * never a stale serve.  Install at gen g, then the locator's gen advances
 * (unlink) — the slot's stale rel_gen no longer matches current => MISS. */
UT_TEST(test_u9_inv_l0_fail_closed)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 400, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	lifecycle_init(64, 256);
	UT_ASSERT_EQ((int)l2_install(&k, 0x9A), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	cluster_cr_pool_unlink_locator(k.rlocator);					/* gen advances */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* fail-closed MISS */
}

/* U10 (Part B fine-grained hit): rel A hot + rel B dropped -> A survives, B
 * MISSes, and ONLY one per-relation bump happened (no global epoch bump). */
UT_TEST(test_u10_fine_grained_survival)
{
	ClusterCRCacheKey a = mk_key(1663, 5, 1001, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey b = mk_key(1663, 5, 1002, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];
	uint64 bump0, epoch0;

	lifecycle_init(256, 256);
	UT_ASSERT_EQ((int)l2_install(&a, 0xAA), 1);
	UT_ASSERT_EQ((int)l2_install(&b, 0xBB), 1);
	bump0 = cluster_cr_pool_rel_gen_bump_count();
	epoch0 = cluster_cr_pool_current_epoch();

	cluster_cr_pool_unlink_locator(b.rlocator); /* DROP rel B only */

	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&a, dst), 1); /* A survives warm */
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0xAA);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&b, dst), 0); /* B invalidated */
	UT_ASSERT_EQ((int)(cluster_cr_pool_rel_gen_bump_count() == bump0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_current_epoch() == epoch0), 1); /* NO global bump */
}

/* U11 (INV-G2/G3): (a) unlinking an UNREGISTERED locator is a no-op (no global
 * bump, no rel_gen bump) so unrelated warm images survive; (b) a backend-local
 * L1 survivor of a dropped locator MISSes (append-only gen entry persists and
 * bumps, so the L1 composite fence rejects it). */
UT_TEST(test_u11_invg2_invg3_l1_survivor)
{
	ClusterCRCacheKey a = mk_key(1663, 5, 2001, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey c = mk_key(1663, 5, 2999, 0, (SCN)50, (XLogRecPtr)10); /* never registered */
	char dst[BLCKSZ];
	char *slot;
	bool ev = false;
	uint64 epoch0, bump0, start_gen = 0;

	lifecycle_init(256, 256);
	UT_ASSERT_EQ((int)l2_install(&a, 0xAA), 1);
	epoch0 = cluster_cr_pool_current_epoch();
	bump0 = cluster_cr_pool_rel_gen_bump_count();

	/* (a) untracked unlink: no-op (INV-G3: no entry exists for c) */
	cluster_cr_pool_unlink_locator(c.rlocator);
	UT_ASSERT_EQ((int)(cluster_cr_pool_current_epoch() == epoch0), 1);	   /* no global bump */
	UT_ASSERT_EQ((int)(cluster_cr_pool_rel_gen_bump_count() == bump0), 1); /* no rel bump */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&a, dst), 1);			   /* A still warm */

	/* (b) L1 survivor: register A's locator (already registered), commit an L1
	 * entry stamped at the current gen, then DROP A -> the L1 entry must MISS. */
	(void)cluster_cr_pool_rel_generation(a.rlocator, &start_gen);
	slot = cluster_cr_cache_victim_slot(&a, cluster_cr_pool_current_epoch(), &ev);
	slot[0] = (char)0xA1;
	cluster_cr_cache_commit_slot_gen(start_gen);
	UT_ASSERT_EQ(
		(int)(cluster_cr_cache_lookup_fenced(&a, cluster_cr_pool_current_epoch(), start_gen, NULL)
			  != NULL),
		1);										/* L1 hit at install gen */
	cluster_cr_pool_unlink_locator(a.rlocator); /* DROP A: gen++ */
	{
		uint64 cur = 0;

		(void)cluster_cr_pool_rel_generation(a.rlocator, &cur);
		UT_ASSERT_EQ((int)(cur == start_gen + 1), 1);
		/* L1 survivor: lookup at the NEW gen rejects the stale entry (no stale-hit) */
		UT_ASSERT_EQ(
			(int)(cluster_cr_cache_lookup_fenced(&a, cluster_cr_pool_current_epoch(), cur, NULL)
				  == NULL),
			1);
	}
}

/* U12 (INV-G1 completeness): every registered locator that is unlinked MISSes;
 * a locator whose bump is SKIPPED (the missed-bump fault) still HITs — proving
 * the per-relation bump is load-bearing (the global epoch does not back it up). */
UT_TEST(test_u12_invg1_completeness)
{
	ClusterCRCacheKey keys[8];
	char dst[BLCKSZ];
	int i;

	lifecycle_init(256, 256);
	for (i = 0; i < 8; i++) {
		keys[i] = mk_key(1663, 5, 3000 + i, 0, (SCN)50, (XLogRecPtr)10);
		UT_ASSERT_EQ((int)l2_install(&keys[i], (unsigned char)(0x30 + i)), 1);
	}
	/* unlink all but index 5 (the "missed bump") */
	for (i = 0; i < 8; i++) {
		if (i != 5)
			cluster_cr_pool_unlink_locator(keys[i].rlocator);
	}
	for (i = 0; i < 8; i++) {
		int hit = cluster_cr_pool_lookup_copy(&keys[i], dst);

		if (i == 5)
			UT_ASSERT_EQ(hit, 1); /* not bumped -> still served (load-bearing) */
		else
			UT_ASSERT_EQ(hit, 0); /* bumped -> MISS (completeness) */
	}
}

/* U13 (register + overflow + first-install gen): the first registration returns
 * gen 1; a full table fails register (serve-but-skip-cache) WITHOUT a global
 * bump, and bumps the overflow counter. */
UT_TEST(test_u13_register_and_overflow)
{
	uint64 gen = 0, epoch0, ov0;
	int i, fails = 0;

	/* per_seg = 16/16 = 1: each segment band holds exactly one locator. */
	lifecycle_init(64, 16);
	UT_ASSERT_EQ(
		(int)cluster_cr_pool_register_locator(mk_key(1663, 5, 4000, 0, 0, 0).rlocator, &gen), 1);
	UT_ASSERT_EQ((int)(gen == 1), 1); /* first install -> gen 1 (P2-e′) */

	epoch0 = cluster_cr_pool_current_epoch();
	ov0 = cluster_cr_pool_rel_gen_table_overflow_count();
	/* register many distinct locators -> some bands overflow (16 slots, 200 keys) */
	for (i = 0; i < 200; i++) {
		uint64 g = 0;

		if (!cluster_cr_pool_register_locator(mk_key(1663, 5, 5000 + i, 0, 0, 0).rlocator, &g))
			fails++;
	}
	UT_ASSERT_EQ((int)(fails > 0), 1); /* overflow happened */
	UT_ASSERT_EQ((int)(cluster_cr_pool_rel_gen_table_overflow_count() == ov0 + (uint64)fails), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_current_epoch() == epoch0), 1); /* NO global bump */
}

/* U13b (INV-G3 L1-only path): register a locator and commit ONLY an L1 entry
 * (admission-reject / L2-disabled path).  A DROP of that locator MISSes the
 * L1-only survivor (tracked because register ran at L1 commit). */
UT_TEST(test_u13b_invg3_l1_only)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 6001, 0, (SCN)50, (XLogRecPtr)10);
	char *slot;
	bool ev = false;
	uint64 gen = 0;

	lifecycle_init(256, 256);
	/* L1-only install: register at L1 commit (INV-G3 covers L1) */
	UT_ASSERT_EQ((int)cluster_cr_pool_register_locator(k.rlocator, &gen), 1);
	slot = cluster_cr_cache_victim_slot(&k, cluster_cr_pool_current_epoch(), &ev);
	slot[0] = (char)0x6B;
	cluster_cr_cache_commit_slot_gen(gen);
	UT_ASSERT_EQ(
		(int)(cluster_cr_cache_lookup_fenced(&k, cluster_cr_pool_current_epoch(), gen, NULL)
			  != NULL),
		1);
	/* DROP: the L1-only entry must MISS (no stale-hit, P1-d) */
	cluster_cr_pool_unlink_locator(k.rlocator);
	{
		uint64 cur = 0;

		(void)cluster_cr_pool_rel_generation(k.rlocator, &cur);
		UT_ASSERT_EQ(
			(int)(cluster_cr_cache_lookup_fenced(&k, cluster_cr_pool_current_epoch(), cur, NULL)
				  == NULL),
			1);
	}
}

/* U14 (disabled equivalence): relgen_slots == 0 reproduces the coarse spec-5.53
 * behavior exactly — install hits, unlink_locator is a no-op (caller would do the
 * global bump), only a global epoch bump invalidates. */
UT_TEST(test_u14_disabled_equivalence)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 7001, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];

	lifecycle_init(64, 0); /* gen table disabled */
	UT_ASSERT_EQ((int)cluster_cr_pool_rel_generation_enabled(), 0);
	UT_ASSERT_EQ((int)l2_install(&k, 0xD0), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1);
	cluster_cr_pool_unlink_locator(k.rlocator);					/* no-op when disabled */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 1); /* still warm */
	cluster_cr_pool_bump_epoch();								/* the coarse floor */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* now MISS */
}

/* U14b (composite fence recheck, P1-c): a per-relation unlink that races a
 * two-phase insert (between reserve and publish) is caught at publish — the
 * stale image is NOT installed (publish_stale_release++); and a lookup_copy after
 * a bump MISSes (lookup reads the current gen fresh). */
UT_TEST(test_u14b_composite_fence_recheck)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 8001, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRPoolHandle h;
	char page[BLCKSZ], dst[BLCKSZ];
	uint64 gen = 0, rel0;

	lifecycle_init(256, 256);
	UT_ASSERT_EQ((int)cluster_cr_pool_register_locator(k.rlocator, &gen), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_reserve_gen(&k, gen, &h), 1);
	/* mid-window per-relation unlink (no global epoch bump) */
	cluster_cr_pool_unlink_locator(k.rlocator);
	rel0 = cluster_cr_pool_publish_stale_release_count();
	memset(page, 0, BLCKSZ);
	page[0] = (char)0x8C;
	cluster_cr_pool_publish(&h, page); /* must release, not install (gen advanced) */
	UT_ASSERT_EQ((int)(cluster_cr_pool_publish_stale_release_count() == rel0 + 1), 1);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0); /* not installed */
}

/* U15 (counters): each spec-5.56 lifecycle counter is bumped by its path. */
UT_TEST(test_u15_counters)
{
	ClusterCRCacheKey a = mk_key(1663, 5, 9001, 0, (SCN)50, (XLogRecPtr)10);
	uint64 fb0, rb0, ov0, rh0, rc0;
	int i;

	lifecycle_init(64, 16);
	fb0 = cluster_cr_pool_global_epoch_fallback_bump_count();
	rb0 = cluster_cr_pool_rel_gen_bump_count();
	ov0 = cluster_cr_pool_rel_gen_table_overflow_count();
	rh0 = cluster_cr_pool_retention_horizon_advance_noted_count();
	rc0 = cluster_cr_pool_reconfig_intra_survived_count();

	cluster_cr_pool_global_fallback_bump(); /* fallback */
	l2_install(&a, 0x01);
	cluster_cr_pool_unlink_locator(a.rlocator); /* rel bump */
	for (i = 0; i < 200; i++)					/* overflow */
		(void)cluster_cr_pool_register_locator(mk_key(1663, 5, 9100 + i, 0, 0, 0).rlocator, NULL);
	cluster_cr_pool_note_retention_horizon_advance(); /* C3 */
	cluster_cr_pool_note_reconfig_intra_survived();	  /* C4 */

	UT_ASSERT_EQ((int)(cluster_cr_pool_global_epoch_fallback_bump_count() == fb0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_rel_gen_bump_count() == rb0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_rel_gen_table_overflow_count() > ov0), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_retention_horizon_advance_noted_count() == rh0 + 1), 1);
	UT_ASSERT_EQ((int)(cluster_cr_pool_reconfig_intra_survived_count() == rc0 + 1), 1);
}

/* U16 (pool disabled): the whole spec-5.56 path is a no-op when the pool is off
 * (the gen table requires the pool); equivalent to pre-5.56. */
UT_TEST(test_u16_pool_disabled)
{
	ClusterCRCacheKey k = mk_key(1663, 5, 100, 0, (SCN)50, (XLogRecPtr)10);
	char dst[BLCKSZ];
	uint64 gen = 123;

	lifecycle_init(0, 256); /* pool off => gen table off too */
	UT_ASSERT_EQ((int)cluster_cr_pool_rel_generation_enabled(), 0);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&k, dst), 0);				  /* disabled: miss */
	UT_ASSERT_EQ((int)cluster_cr_pool_register_locator(k.rlocator, &gen), 1); /* proceed */
	UT_ASSERT_EQ((int)(gen == 0), 1);										/* disabled -> gen 0 */
	UT_ASSERT_EQ((int)cluster_cr_pool_rel_generation(k.rlocator, &gen), 0); /* unregistered */
	cluster_cr_pool_unlink_locator(k.rlocator);								/* no-op, no crash */
	UT_ASSERT_EQ((int)cluster_cr_pool_rel_generation_live(), 0);
}

/* U4b (P2-a full-locator key, no false sharing): two relations with the SAME
 * relNumber but different dbOid have INDEPENDENT generations — dropping one does
 * not invalidate the other (a relNumber-only key would over-invalidate). */
UT_TEST(test_u4b_full_locator_no_false_share)
{
	ClusterCRCacheKey a = mk_key(1663, 5, 12345, 0, (SCN)50, (XLogRecPtr)10);
	ClusterCRCacheKey b = mk_key(1663, 6, 12345, 0, (SCN)50, (XLogRecPtr)10); /* diff dbOid */
	char dst[BLCKSZ];

	lifecycle_init(256, 256);
	UT_ASSERT_EQ((int)l2_install(&a, 0x4A), 1);
	UT_ASSERT_EQ((int)l2_install(&b, 0x4B), 1);
	cluster_cr_pool_unlink_locator(b.rlocator);					/* DROP only the dbOid=6 relation */
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&a, dst), 1); /* dbOid=5 survives */
	UT_ASSERT_EQ((int)(unsigned char)dst[0], 0x4A);
	UT_ASSERT_EQ((int)cluster_cr_pool_lookup_copy(&b, dst), 0); /* dbOid=6 invalidated */
}

int
main(int argc, char **argv)
{
	UT_PLAN(16);

	UT_RUN(test_u1_c1_floor_global_epoch);
	UT_RUN(test_u2_c2_floor_same_relid_rewrite);
	UT_RUN(test_u3_c3_image_immutability);
	UT_RUN(test_u4b_full_locator_no_false_share);
	UT_RUN(test_u5_c3_retention_counter);
	UT_RUN(test_u7_c4_reconfig_survives);
	UT_RUN(test_u9_inv_l0_fail_closed);
	UT_RUN(test_u10_fine_grained_survival);
	UT_RUN(test_u11_invg2_invg3_l1_survivor);
	UT_RUN(test_u12_invg1_completeness);
	UT_RUN(test_u13_register_and_overflow);
	UT_RUN(test_u13b_invg3_l1_only);
	UT_RUN(test_u14_disabled_equivalence);
	UT_RUN(test_u14b_composite_fence_recheck);
	UT_RUN(test_u15_counters);
	UT_RUN(test_u16_pool_disabled);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
