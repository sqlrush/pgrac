/*-------------------------------------------------------------------------
 *
 * test_cluster_errcodes.c
 *	  Compile-time invariants for the cluster SQLSTATE error codes
 *	  registered in stage 0.12.
 *
 *	  All ERRCODE_CLUSTER_* macros are generated automatically by PG's
 *	  generate-errcodes.pl from src/backend/utils/errcodes.txt.  This
 *	  test asserts that:
 *
 *	  - Each of the 8 PG classes (08, 40, 53, 55, 57, 58, 72, XX)
 *	    contains the expected pgrac extension entries (first + last
 *	    spot-checked, intermediate ones spot-checked too).
 *	  - Each ERRCODE_CLUSTER_* macro encodes the exact SQLSTATE string
 *	    via MAKE_SQLSTATE() (proves the .txt -> .h pipeline produced
 *	    correct values).
 *	  - All checked codes use the 'R' subclass character (pgrac namespace
 *	    discipline; design doc §2.3).
 *	  - The Class 58 pgrac block is dense from 58R01..58R16 (the
 *	    largest pgrac sub-class, anchors the count proof).
 *
 *	  Why compile-time only:
 *
 *	  Stage 0.12 wires no ereport() call sites; the 45 errcodes are
 *	  identifier registrations.  Runtime invocation of plpgsql-friendly
 *	  names (cluster_*) is validated separately by cluster_tap
 *	  t/006_errcodes.pl, which raises exceptions in a real PG instance
 *	  and confirms the expected SQLSTATE comes out.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_errcodes.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Includes postgres.h to pull in
 *	  the basic PG types referenced by errcodes.h, then undoes the
 *	  printf -> pg_printf redirection so the standalone unit test
 *	  binary does not pull in libpgport.
 *
 *	  errcodes.h is auto-generated under $(top_builddir)/src/include,
 *	  but the cluster_unit Makefile already includes both the source
 *	  tree and (transitively, via Makefile.global) the generated
 *	  header path, so #include "utils/errcodes.h" picks up the
 *	  freshly generated file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/errcodes.h"

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
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


/* ----------
 * Helper: extract the i-th character (1-based) of an encoded SQLSTATE.
 *
 *	MAKE_SQLSTATE packs five 6-bit fields, each holding (ch - '0') & 0x3F.
 *	For digits ('0'..'9') and letters ('A'..'Z') used in SQLSTATE codes,
 *	(ch - '0') is in range 0..42 so the mask is a no-op, and we round-trip
 *	the character by adding '0' (not OR-ing, which would corrupt letters
 *	whose bit 4 already overlaps the '0' offset).
 * ----------
 */
static char
sqlstate_char(int code, int idx)
{
	int shift = (idx - 1) * 6;
	int sixbit = (code >> shift) & 0x3F;

	return (char)(sixbit + '0');
}


/* ----------
 * Class boundary spot-checks: first and last entry of every class.
 * Verifies that errcodes.txt ordering produced the correct 5-character
 * SQLSTATE strings for each generated macro.
 * ----------
 */

UT_TEST(test_class_08_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_NODE_UNREACHABLE, MAKE_SQLSTATE('0', '8', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_PROTOCOL_VERSION_MISMATCH, MAKE_SQLSTATE('0', '8', 'R', '0', '5'));
}

UT_TEST(test_class_40_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RECONFIG_ABORT, MAKE_SQLSTATE('4', '0', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_PI_INVALIDATED_RETRY, MAKE_SQLSTATE('4', '0', 'R', '0', '4'));
}

UT_TEST(test_class_53_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_LMS_QUEUE_FULL, MAKE_SQLSTATE('5', '3', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT,
				 MAKE_SQLSTATE('5', '3', 'R', 'A', 'F'));
}

UT_TEST(test_class_55_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_PCM_STATE_INVALID, MAKE_SQLSTATE('5', '5', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_BLOCK_MISSING_TEMPORARY, MAKE_SQLSTATE('5', '5', 'R', '0', '6'));
}

UT_TEST(test_class_57_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_FENCE_TRIGGERED, MAKE_SQLSTATE('5', '7', 'R', '0', '2'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_ADG_APPLY_LAG_EXCESSIVE, MAKE_SQLSTATE('5', '7', 'R', '0', '6'));
}

UT_TEST(test_class_58_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED, MAKE_SQLSTATE('5', '8', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR, MAKE_SQLSTATE('5', '8', 'R', '1', '6'));
}

UT_TEST(test_class_72_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SNAPSHOT_TOO_OLD, MAKE_SQLSTATE('7', '2', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SNAPSHOT_UNAVAILABLE, MAKE_SQLSTATE('7', '2', 'R', '0', '2'));
}

UT_TEST(test_class_xx_first_last)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_ASSERTION_FAILURE, MAKE_SQLSTATE('X', 'X', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RELMAPPER_CONFLICT, MAKE_SQLSTATE('X', 'X', 'R', '0', '3'));
}


/* ----------
 * Class 58 has the largest pgrac sub-class (16 entries).  Verify all
 * 16 are present and correctly encoded.  This anchors the per-class
 * dense-packing claim that the rest of the test only spot-checks.
 * ----------
 */

UT_TEST(test_class_58_complete)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED, MAKE_SQLSTATE('5', '8', 'R', '0', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_HEARTBEAT_LOST, MAKE_SQLSTATE('5', '8', 'R', '0', '2'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_INTERCONNECT_CORRUPTED, MAKE_SQLSTATE('5', '8', 'R', '0', '3'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_MASTER_UNAVAILABLE, MAKE_SQLSTATE('5', '8', 'R', '0', '4'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SCN_DRIFT_EXCESSIVE, MAKE_SQLSTATE('5', '8', 'R', '0', '5'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SCN_UNDERFLOW, MAKE_SQLSTATE('5', '8', 'R', '0', '6'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_GRD_INCONSISTENT, MAKE_SQLSTATE('5', '8', 'R', '0', '7'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_TOPOLOGY_INVALID, MAKE_SQLSTATE('5', '8', 'R', '0', '8'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_TT_INCONSISTENT, MAKE_SQLSTATE('5', '8', 'R', '0', '9'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CATALOG_INCONSISTENT, MAKE_SQLSTATE('5', '8', 'R', '1', '0'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SINVAL_INCONSISTENT, MAKE_SQLSTATE('5', '8', 'R', '1', '1'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RECOVERY_FAILED, MAKE_SQLSTATE('5', '8', 'R', '1', '2'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE,
				 MAKE_SQLSTATE('5', '8', 'R', '1', '3'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT, MAKE_SQLSTATE('5', '8', 'R', '1', '4'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_STORAGE_FENCE_UNAVAILABLE, MAKE_SQLSTATE('5', '8', 'R', '1', '5'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR, MAKE_SQLSTATE('5', '8', 'R', '1', '6'));
}

UT_TEST(test_class_53_rdma_band)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_IC_RDMA_UNAVAILABLE, MAKE_SQLSTATE('5', '3', 'R', '2', '2'));
}

UT_TEST(test_class_53_backup_band)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_BACKUP_IN_PROGRESS, MAKE_SQLSTATE('5', '3', 'R', 'A', 'B'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_PITR_TARGET_UNREACHABLE, MAKE_SQLSTATE('5', '3', 'R', 'A', 'C'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_BACKUP_INCOMPLETE, MAKE_SQLSTATE('5', '3', 'R', 'A', 'D'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RESTORE_INCOMPATIBLE, MAKE_SQLSTATE('5', '3', 'R', 'A', 'E'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT,
				 MAKE_SQLSTATE('5', '3', 'R', 'A', 'F'));
}


/* ----------
 * All 45 cluster errcodes use 'R' as their subclass character
 * (third position of the SQLSTATE).  This proves namespace discipline
 * across the entire pgrac extension; spot-check one entry per class.
 * ----------
 */

UT_TEST(test_all_use_r_subclass)
{
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_NODE_UNREACHABLE, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RECONFIG_ABORT, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_LMS_QUEUE_FULL, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_PCM_STATE_INVALID, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_STORAGE_FENCE_UNAVAILABLE, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SNAPSHOT_TOO_OLD, 3), 'R');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_ASSERTION_FAILURE, 3), 'R');
}


/* ----------
 * pgrac errcodes do not collide with PG-native errcodes in the same
 * class (PG uses no 'R' subclass anywhere).  Spot-check two classes
 * where PG has a lot of subclasses already: 22 and 23 are unaffected
 * but 08, 40, 53, 57, 58 share class with pgrac extensions.
 * ----------
 */

UT_TEST(test_no_overlap_with_pg_native)
{
	/* PG native: 08006 connection_failure */
	UT_ASSERT_NE(ERRCODE_CONNECTION_FAILURE, ERRCODE_CLUSTER_NODE_UNREACHABLE);
	/* PG native: 40001 serialization_failure */
	UT_ASSERT_NE(ERRCODE_T_R_SERIALIZATION_FAILURE, ERRCODE_CLUSTER_RECONFIG_ABORT);
	/* PG native: 53000 insufficient_resources */
	UT_ASSERT_NE(ERRCODE_INSUFFICIENT_RESOURCES, ERRCODE_CLUSTER_LMS_QUEUE_FULL);
}


/* ----------
 * Per-class size sanity (pgrac ranges).  We anchor the upper bound
 * by referencing the last entry of each class; if a future commit
 * drops or renames an entry, this test fails compile (undeclared
 * macro) or value (encoded SQLSTATE shifts).
 * ----------
 */

UT_TEST(test_per_class_anchors)
{
	/* Class 08 has 5 entries: 08R01..08R05 */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_PROTOCOL_VERSION_MISMATCH, 5), '5');
	/* Class 40 has 5 entries: 40R01..40R05 */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SMART_FUSION_RETRY, 5), '5');
	/* Class 53 spans base 53R01..53R07 plus later pgrac bands up to 53RAF. */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT, 4), 'A');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT, 5), 'F');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_CF_TERMINAL_UNRESOLVED, 4), '9');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_CF_TERMINAL_UNRESOLVED, 5), 'O');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SMART_FUSION_DEP_LOST, 4), '9');
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SMART_FUSION_DEP_LOST, 5), 'P');
	/* Class 55 has 6 entries: 55R01..55R06 */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_BLOCK_MISSING_TEMPORARY, 5), '6');
	/* Class 57 keeps operator-intervention cluster codes 57R02..57R06. */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_ADG_APPLY_LAG_EXCESSIVE, 5), '6');
	/* Class 72 has 2 entries: 72R01..72R02 */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_SNAPSHOT_UNAVAILABLE, 5), '2');
	/* Class XX has 3 entries: XXR01..XXR03 */
	UT_ASSERT_EQ(sqlstate_char(ERRCODE_CLUSTER_RELMAPPER_CONFLICT, 5), '3');
}


int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_class_08_first_last);
	UT_RUN(test_class_40_first_last);
	UT_RUN(test_class_53_first_last);
	UT_RUN(test_class_55_first_last);
	UT_RUN(test_class_57_first_last);
	UT_RUN(test_class_58_first_last);
	UT_RUN(test_class_72_first_last);
	UT_RUN(test_class_xx_first_last);
	UT_RUN(test_class_58_complete);
	UT_RUN(test_class_53_rdma_band);
	UT_RUN(test_class_53_backup_band);
	UT_RUN(test_all_use_r_subclass);
	UT_RUN(test_no_overlap_with_pg_native);
	UT_RUN(test_per_class_anchors);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
