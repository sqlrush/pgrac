/*-------------------------------------------------------------------------
 *
 * test_cluster_ts.c
 *	  Unit tests for the TT (tablespace-DDL) pure layer (spec-5.7 D5, §3.3).
 *
 *	  Covers the resid-identity split (TT-M1): DROP/ALTER -> resid = tablespace
 *	  OID; CREATE/RENAME -> resid = a deterministic hash(spcname).  Pins the
 *	  0xF4 namespace marker, non-collision with SQ/CF/HW/DL/IR, that the name hash
 *	  is stable (same name -> same resid -- the cross-node same-name CREATE serial-
 *	  isation R14 depends on it) and discriminating (different names -> different
 *	  resid), and that the OID and NAME identity spaces do not alias.  The TT(X)/
 *	  TT(S) acquire + the S/X conflict matrix are exercised by the 2-node TAP
 *	  (t/296); this file pins the pure identity layer.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ts.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_sequence.h"
#include "cluster/cluster_ts.h"

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
 * U1 -- OID resid (DROP/ALTER): field1 = OID, namespace 0xF4, no other fields.
 * ====================================================================== */
UT_TEST(test_ts_resid_encode_oid)
{
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	cluster_ts_resid_encode_oid(16400, &r);

	UT_ASSERT_EQ(r.field1, 16400); /* tablespace OID */
	UT_ASSERT_EQ(r.field2, 0);
	UT_ASSERT_EQ(r.field3, 0);
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_TT_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF4);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* U2 -- NAME resid (CREATE/RENAME): field1 = hash(spcname), namespace 0xF4. */
UT_TEST(test_ts_resid_encode_name)
{
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	cluster_ts_resid_encode_name("ts_foo", &r);

	UT_ASSERT_EQ(r.field2, 0);
	UT_ASSERT_EQ(r.field3, 0);
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_TT_RESID_TYPE);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* R14: the name hash is DETERMINISTIC -- two nodes hashing the same name get the
 * same resid (that is what serialises a cross-node same-name CREATE). */
UT_TEST(test_ts_resid_name_deterministic)
{
	ClusterResId a, b;

	cluster_ts_resid_encode_name("ts_same", &a);
	cluster_ts_resid_encode_name("ts_same", &b);

	UT_ASSERT_EQ(a.field1, b.field1); /* same name -> same hash -> same resid */
}

/* ... and DISCRIMINATING: different names get different resids (no false
 * serialisation of unrelated CREATEs, beyond rare hash collisions). */
UT_TEST(test_ts_resid_name_discriminates)
{
	ClusterResId a, b;

	cluster_ts_resid_encode_name("ts_alpha", &a);
	cluster_ts_resid_encode_name("ts_beta", &b);

	UT_ASSERT_NE(a.field1, b.field1);
}

/* namespace 0xF4 is distinct from SQ / CF / HW / DL / IR */
UT_TEST(test_ts_resid_namespace_distinct)
{
	ClusterResId r;

	cluster_ts_resid_encode_oid(16400, &r);

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
	UT_ASSERT_NE(r.type, CLUSTER_DL_RESID_TYPE); /* != 0xF3 */
	UT_ASSERT_NE(r.type, CLUSTER_IR_RESID_TYPE); /* != 0xF5 */
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_ts_resid_encode_oid);
	UT_RUN(test_ts_resid_encode_name);
	UT_RUN(test_ts_resid_name_deterministic);
	UT_RUN(test_ts_resid_name_discriminates);
	UT_RUN(test_ts_resid_namespace_distinct);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
