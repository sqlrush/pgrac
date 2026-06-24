/*-------------------------------------------------------------------------
 *
 * test_cluster_ko.c
 *	  Unit tests for the KO (object-reuse flush) pure layer (spec-5.7 D6, §3.5).
 *
 *	  Covers the KO resource-id encoder: per-relfilenode identity (no fork --
 *	  a DROP/TRUNCATE removes all forks of the relfilenode), the 0xF6 namespace
 *	  marker, and non-collision with the SQ (0xF0) / CF (0xF1) / HW (0xF2) /
 *	  DL (0xF3) / IR (0xF5) namespaces.  The cross-node apply-after-drop flush
 *	  barrier (KO_FLUSH fanout + ACK, §3.6) is exercised by the 2-node TAP
 *	  (t/297);  this file pins the pure encoding that routes a drop to its KO(X).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ko.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_dl.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_ko.h"
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
 * U1 -- KO resid encoding: per relfilenode, namespace 0xF6, no fork.
 * ====================================================================== */
UT_TEST(test_ko_resid_encode)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;

	memset(&r, 0xEE, sizeof(r));
	cluster_ko_resid_encode(rloc, &r);

	UT_ASSERT_EQ(r.field1, 5);	   /* dbOid */
	UT_ASSERT_EQ(r.field2, 16384); /* relNumber (ABA defence -- relfilenode reuse) */
	UT_ASSERT_EQ(r.field3, 1663);  /* spcOid */
	UT_ASSERT_EQ(r.field4, 0);	   /* KO is per-relfilenode: covers all forks */
	UT_ASSERT_EQ(r.type, CLUSTER_KO_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF6);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* namespace 0xF6 is distinct from SQ / CF / HW / DL / IR */
UT_TEST(test_ko_resid_namespace_distinct)
{
	RelFileLocator rloc;
	ClusterResId r;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_ko_resid_encode(rloc, &r);

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
	UT_ASSERT_NE(r.type, CLUSTER_DL_RESID_TYPE); /* != 0xF3 */
	UT_ASSERT_NE(r.type, CLUSTER_IR_RESID_TYPE); /* != 0xF5 */
}

/* a KO resid (per-relfilenode) and a DL resid (per-relation) of the SAME
 * relation share the relfilenode triple but differ in type -- the drop's
 * flush barrier (KO) and a bulk-load lease (DL) are separate resources. */
UT_TEST(test_ko_vs_dl_resid_distinct)
{
	RelFileLocator rloc;
	ClusterResId ko, dl;

	rloc.spcOid = 1663;
	rloc.dbOid = 5;
	rloc.relNumber = 16384;
	cluster_ko_resid_encode(rloc, &ko);
	cluster_dl_resid_encode(rloc, &dl);

	UT_ASSERT_NE(ko.type, dl.type);		/* 0xF6 != 0xF3 */
	UT_ASSERT_EQ(ko.field1, dl.field1); /* same db */
	UT_ASSERT_EQ(ko.field2, dl.field2); /* same relNumber */
	UT_ASSERT_EQ(ko.field3, dl.field3); /* same spc */
}

/* two different relfilenodes get distinct KO resids (different relNumber):
 * relfilenode reuse (ABA) after a drop maps to a NEW resid, so a stale flush
 * for the old incarnation never aliases the new one. */
UT_TEST(test_ko_resid_per_relfilenode)
{
	RelFileLocator a, b;
	ClusterResId ra, rb;

	a.spcOid = b.spcOid = 1663;
	a.dbOid = b.dbOid = 5;
	a.relNumber = 16384;
	b.relNumber = 16385;
	cluster_ko_resid_encode(a, &ra);
	cluster_ko_resid_encode(b, &rb);

	UT_ASSERT_NE(ra.field2, rb.field2); /* distinct relNumber -> distinct resid */
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_ko_resid_encode);
	UT_RUN(test_ko_resid_namespace_distinct);
	UT_RUN(test_ko_vs_dl_resid_distinct);
	UT_RUN(test_ko_resid_per_relfilenode);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
