/*-------------------------------------------------------------------------
 *
 * test_cluster_dl.c
 *	  Unit tests for the DL (bulk-load lease) pure layer (spec-5.7 D4, §3.2).
 *
 *	  Covers the DL resource-id encoder: per-relation identity (no fork, unlike
 *	  HW), the 0xF3 namespace marker, and non-collision with the SQ (0xF0) / CF
 *	  (0xF1) / HW (0xF2) namespaces.  The cross-node lease acquire/release + the
 *	  BulkInsertState hook are exercised by the 2-node TAP (t/294); this file
 *	  pins the pure encoding that routes a bulk load to its lease.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_dl.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D4, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_sequence.h"
#include "storage/relfilelocator.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ======================================================================
 * U1 -- DL resid encoding: per relation, namespace 0xF3, no fork.
 * ====================================================================== */
UT_TEST(test_dl_resid_encode)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;

	memset(&r, 0xEE, sizeof(r));
	cluster_dl_resid_encode(rloc, &r);

	UT_ASSERT_EQ(r.field1, 5);	   /* dbOid */
	UT_ASSERT_EQ(r.field2, 16384); /* relNumber (ABA defence) */
	UT_ASSERT_EQ(r.field3, 1663);  /* spcOid */
	UT_ASSERT_EQ(r.field4, 0);	   /* DL is per-relation: no fork */
	UT_ASSERT_EQ(r.type, CLUSTER_DL_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF3);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* namespace 0xF3 is distinct from SQ / CF / HW */
UT_TEST(test_dl_resid_namespace_distinct)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_dl_resid_encode(rloc, &r);

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
}

/* a DL resid (per-relation) and an HW resid (per-fork, MAIN) of the SAME
 * relation differ in type AND fork -- they never alias, so a bulk load's DL
 * lease and its extents' HW authority are separate resources (lock order
 * DL -> HW). */
UT_TEST(test_dl_vs_hw_resid_distinct)
{
	RelFileLocator rloc;
	ClusterResId dl, hw;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_dl_resid_encode(rloc, &dl);
	cluster_hw_resid_encode(rloc, MAIN_FORKNUM, &hw);

	UT_ASSERT_NE(dl.type, hw.type);		/* 0xF3 != 0xF2 */
	UT_ASSERT_EQ(dl.field1, hw.field1); /* same db */
	UT_ASSERT_EQ(dl.field2, hw.field2); /* same relNumber */
}

/* two different relations get distinct DL resids (different relNumber) */
UT_TEST(test_dl_resid_per_relation)
{
	RelFileLocator a, b;
	ClusterResId ra, rb;

	a.spcOid = b.spcOid = 1663;
	a.dbOid = b.dbOid = 5;
	a.relNumber = 16384;
	b.relNumber = 16385;
	cluster_dl_resid_encode(a, &ra);
	cluster_dl_resid_encode(b, &rb);

	UT_ASSERT_NE(ra.field2, rb.field2); /* distinct relNumber -> distinct resid */
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_dl_resid_encode);
	UT_RUN(test_dl_resid_namespace_distinct);
	UT_RUN(test_dl_vs_hw_resid_distinct);
	UT_RUN(test_dl_resid_per_relation);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
