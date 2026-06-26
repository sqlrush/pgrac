/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_key.c
 *	  pgrac spec-5.53 D7 — cluster_unit tests for the CR cache key identity
 *	  contract (cluster_cr_cache_key_equal): per-field necessity, joint
 *	  sufficiency, and field-wise (never-memcmp / padding-safe) equality.
 *
 *	  This file locks the spec-5.53 §3.1 identity contract (the over-hit P0
 *	  命门): every one of the five key fields (db / tablespace / relNumber /
 *	  fork / block / read_scn / base_page_lsn) is necessary, and the five
 *	  together are sufficient.  A regression here means the key field set or
 *	  the equality definition drifted — re-review the contract before changing.
 *
 *	  Links cluster_cr_cache.o; stubs the MemoryContext allocator (malloc-
 *	  backed) so the key primitive is exercisable standalone.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.53-cr-key-and-safe-reuse-evolution.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_key.c
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

/* A fully-specified reference key; tests perturb exactly one field. */
static ClusterCRCacheKey
ref_key(void)
{
	ClusterCRCacheKey k;

	memset(&k, 0, sizeof(k));
	k.rlocator.spcOid = 1663;
	k.rlocator.dbOid = 5;
	k.rlocator.relNumber = 16384;
	k.forknum = MAIN_FORKNUM;
	k.blockno = 7;
	k.read_scn = (SCN)50;
	k.base_page_lsn = (XLogRecPtr)0x1000;
	return k;
}


/*
 * U1 — per-field necessity (locator sub-fields + fork + block).  Removing any
 * single discriminator must make the keys NON-equal (else that field could be
 * dropped → over-hit).
 */
UT_TEST(test_necessity_locator_fork_block)
{
	ClusterCRCacheKey base = ref_key();
	ClusterCRCacheKey k;

	/* all-equal baseline holds (sanity for the perturbations below) */
	k = ref_key();
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 1);

	/* spcOid (tablespace): different tablespace, same relNumber → not equal */
	k = ref_key();
	k.rlocator.spcOid = 1664;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);

	/* dbOid (database): different db, same relNumber → not equal */
	k = ref_key();
	k.rlocator.dbOid = 6;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);

	/* relNumber (relfilenode): different relation → not equal */
	k = ref_key();
	k.rlocator.relNumber = 16385;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);

	/* forknum: main vs fsm is a different physical file → not equal */
	k = ref_key();
	k.forknum = FSM_FORKNUM;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);

	/* blockno: per-block image → not equal */
	k = ref_key();
	k.blockno = 8;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);
}

/*
 * U2 — per-field necessity (snapshot + version guard).  read_scn and
 * base_page_lsn each independently discriminate (different visible version /
 * different live page version → not equal).
 */
UT_TEST(test_necessity_read_scn_base_lsn)
{
	ClusterCRCacheKey base = ref_key();
	ClusterCRCacheKey k;

	/* read_scn: different snapshot → different visible version → not equal */
	k = ref_key();
	k.read_scn = (SCN)60;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);

	/* base_page_lsn: live page version guard (HOT-prune / VACUUM relayout) */
	k = ref_key();
	k.base_page_lsn = (XLogRecPtr)0x1001;
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&base, &k), 0);
}

/*
 * U3 — joint sufficiency.  Two keys with ALL five fields equal compare equal
 * (the cached CR image is a pure function of the key, spec-3.10 §3.2 / 5.50
 * S-1 GREEN).
 */
UT_TEST(test_joint_sufficiency)
{
	ClusterCRCacheKey a = ref_key();
	ClusterCRCacheKey b = ref_key();

	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&a, &b), 1);
	/* symmetric */
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&b, &a), 1);
}

/*
 * U16 — field-wise (never memcmp): two keys whose IDENTITY fields are equal but
 * whose alignment padding bytes differ must still compare EQUAL.  A memcmp of
 * the struct would wrongly return non-equal (padding is not part of identity,
 * F0-6).  Build one key over a 0x00 backing and one over a 0xFF backing so any
 * padding bytes differ, then set the identity fields identically.
 */
UT_TEST(test_field_wise_not_memcmp)
{
	ClusterCRCacheKey a;
	ClusterCRCacheKey b;

	memset(&a, 0x00, sizeof(a));
	memset(&b, 0xFF, sizeof(b));

	/* identical identity fields over differing backings (padding differs) */
	a.rlocator.spcOid = b.rlocator.spcOid = 1663;
	a.rlocator.dbOid = b.rlocator.dbOid = 5;
	a.rlocator.relNumber = b.rlocator.relNumber = 16384;
	a.forknum = b.forknum = MAIN_FORKNUM;
	a.blockno = b.blockno = 7;
	a.read_scn = b.read_scn = (SCN)50;
	a.base_page_lsn = b.base_page_lsn = (XLogRecPtr)0x1000;

	/* field-wise compare ignores the differing padding → equal */
	UT_ASSERT_EQ((int)cluster_cr_cache_key_equal(&a, &b), 1);

	/* sanity: a raw memcmp WOULD differ here (proves the keys are not byte-equal,
	 * so the equal verdict above came from the field-wise path, not luck). */
	UT_ASSERT_NE(memcmp(&a, &b, sizeof(a)), 0);
}


int
main(int argc, char **argv)
{
	UT_PLAN(4);

	UT_RUN(test_necessity_locator_fork_block);
	UT_RUN(test_necessity_read_scn_base_lsn);
	UT_RUN(test_joint_sufficiency);
	UT_RUN(test_field_wise_not_memcmp);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
