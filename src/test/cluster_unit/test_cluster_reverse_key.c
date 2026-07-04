/*-------------------------------------------------------------------------
 *
 * test_cluster_reverse_key.c
 *	  Standalone unit tests for the spec-6.12 wave-f reverse-key
 *	  encoding core (cluster_reverse_key_encode + width gate).
 *
 *	  U-f coverage:
 *	    1) width gate: exactly typlen 2 / 4 / 8 are supported.
 *	    2) known byte patterns per width (value-level byte swap).
 *	    3) involution: encode(encode(x)) == x for edge values (0, 1,
 *	       -1, MIN, MAX) at every width -- the property that makes one
 *	       transform serve both insert and equality-probe sides.
 *	    4) equality preservation via bijection spot checks (distinct
 *	       inputs stay distinct after encoding).
 *	    5) order is NOT preserved (256 encodes below 1 at width 4) --
 *	       the property that forces the equality-only planner contract
 *	       (Q18-A) and the leaf-scatter benefit itself.
 *
 *	  The scankey cross-type normalization + planner contract are
 *	  covered end-to-end by cluster_regress reverse_key (real index,
 *	  real planner); this binary locks the pure encoding math.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_reverse_key.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_reverse_key.o only; PG backend symbols stubbed locally
 *	  (the ereport / lsyscache / fmgr externs live in the scankey
 *	  transform, which unit tests never call -- it needs a Relation).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_reverse_key.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 * Stubs needed to link cluster_reverse_key.o standalone.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		printf("# unexpected ereport(elevel=%d) -- aborting\n", elevel);
		abort();
	}
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* lsyscache / fmgr externs referenced by the scankey transform only. */
Oid
get_opfamily_member(Oid opfamily pg_attribute_unused(), Oid lefttype pg_attribute_unused(),
					Oid righttype pg_attribute_unused(), int16 strategy pg_attribute_unused())
{
	abort();
}

RegProcedure
get_opcode(Oid opno pg_attribute_unused())
{
	abort();
}

void
fmgr_info(Oid functionId pg_attribute_unused(), FmgrInfo *finfo pg_attribute_unused())
{
	abort();
}


/* ============================================================
 * Tests
 * ============================================================ */

UT_TEST(test_width_gate)
{
	int16 w;

	for (w = -1; w <= 16; w++) {
		bool expected = (w == 2 || w == 4 || w == 8);

		UT_ASSERT(cluster_reverse_key_typlen_supported(w) == expected);
	}
}

UT_TEST(test_known_patterns)
{
	/* width 2: 0x0102 <-> 0x0201 */
	UT_ASSERT_EQ(DatumGetUInt16(cluster_reverse_key_encode(UInt16GetDatum(0x0102), 2)), 0x0201);

	/* width 4: 0x00000001 <-> 0x01000000 */
	UT_ASSERT_EQ(DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(0x00000001), 4)),
				 0x01000000);

	/* width 4: 0x12345678 <-> 0x78563412 */
	UT_ASSERT_EQ(DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(0x12345678), 4)),
				 0x78563412);

	/* width 8: 777 (0x309) <-> 0x0903000000000000 */
	UT_ASSERT(DatumGetUInt64(cluster_reverse_key_encode(UInt64GetDatum(777), 8))
			  == UINT64CONST(0x0903000000000000));
}

UT_TEST(test_involution)
{
	static const int64 edges[] = {
		0,
		1,
		-1,
		2,
		42,
		777,
		256,
		65535,
		PG_INT16_MIN,
		PG_INT16_MAX,
		PG_INT32_MIN,
		PG_INT32_MAX,
		PG_INT64_MIN,
		PG_INT64_MAX,
	};
	int i;

	for (i = 0; i < (int)lengthof(edges); i++) {
		int64 v = edges[i];

		if (v >= PG_INT16_MIN && v <= PG_INT16_MAX) {
			Datum once = cluster_reverse_key_encode(Int16GetDatum((int16)v), 2);

			UT_ASSERT(DatumGetInt16(cluster_reverse_key_encode(once, 2)) == (int16)v);
		}
		if (v >= PG_INT32_MIN && v <= PG_INT32_MAX) {
			Datum once = cluster_reverse_key_encode(Int32GetDatum((int32)v), 4);

			UT_ASSERT(DatumGetInt32(cluster_reverse_key_encode(once, 4)) == (int32)v);
		}
		{
			Datum once = cluster_reverse_key_encode(Int64GetDatum(v), 8);

			UT_ASSERT(DatumGetInt64(cluster_reverse_key_encode(once, 8)) == v);
		}
	}
}

UT_TEST(test_bijection_spot)
{
	/*
	 * Involution already implies bijection; spot-check that near-collision
	 * candidates stay distinct after encoding.
	 */
	UT_ASSERT(DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(1), 4))
			  != DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(2), 4)));
	UT_ASSERT(DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(1), 4))
			  != DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(256), 4)));
}

UT_TEST(test_order_not_preserved)
{
	/*
	 * enc4(256) = 0x00010000 < enc4(1) = 0x01000000: a monotone insert
	 * sequence scatters (the point of the feature), so range scans over
	 * the encoding are meaningless -- locking the reason the planner
	 * contract is equality-only.
	 */
	uint32 e1 = DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(1), 4));
	uint32 e256 = DatumGetUInt32(cluster_reverse_key_encode(UInt32GetDatum(256), 4));

	UT_ASSERT(e256 < e1);
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(5);

	UT_RUN(test_width_gate);
	UT_RUN(test_known_patterns);
	UT_RUN(test_involution);
	UT_RUN(test_bijection_spot);
	UT_RUN(test_order_not_preserved);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
