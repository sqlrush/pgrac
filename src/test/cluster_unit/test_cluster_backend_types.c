/*-------------------------------------------------------------------------
 *
 * test_cluster_backend_types.c
 *	  Compile-time invariants for the pgrac BackendType extension.
 *
 *	  Stage 0.10 extends PG's BackendType enum with 14 pgrac cluster
 *	  process types (see docs/background-process-design.md §8.2).  This
 *	  test asserts the structural invariants that remain valid across
 *	  the extension:
 *
 *	  - PG ABI preserved: the original 14 PG values (B_INVALID..B_WAL_WRITER)
 *	    keep their numeric positions; the 15 pgrac values are appended
 *	    after B_WAL_WRITER (CSSD added in spec-2.5 Sprint A).
 *	  - BACKEND_NUM_TYPES == 29 (14 PG + 15 pgrac).
 *	  - The 15 new values are pairwise distinct and dense (no holes).
 *	  - B_UNDO_CLEANER == BACKEND_NUM_TYPES - 1 (last value).
 *
 *	  Why compile-time only:
 *
 *	  GetBackendTypeDesc() lives in src/backend/utils/init/miscinit.c,
 *	  which is part of the PG backend and depends on most of the rest
 *	  of PG to link.  cluster_unit deliberately stays PG-free (it only
 *	  links cluster_version.o standalone), so we cannot call
 *	  GetBackendTypeDesc() here.  The runtime mapping enum value ->
 *	  string is validated by:
 *
 *	  - cluster_tap t/004_backend_types.pl (PG running, queries
 *	    pg_stat_activity.backend_type to confirm switch did not break)
 *	  - the compiler's -Wswitch-enum (every enum value must appear in
 *	    every switch on BackendType)
 *	  - manual review against §2.3 of spec-0.10
 *
 *	  When stage 0.13+ wires real fork() paths for the 14 new process
 *	  types, the desc strings will be observable directly via ps and
 *	  pg_stat_activity in cluster_tap; that work is intentionally
 *	  deferred and out of scope for stage 0.10.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_backend_types.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Including miscadmin.h pulls in two PG typedef-only headers
 *	  (datatype/timestamp.h, pgtime.h); both are header-only and do
 *	  not introduce a link-time dependency on PG backend objects.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

/*
 * postgres.h transitively pulls in port.h, which #defines printf and
 * friends to pg_printf etc.  Standalone unit-test binaries do not link
 * libpgport, so undo the redirection before pulling in unit_test.h.
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
 * Structural invariants for the BackendType enum extension (stage 0.10).
 *
 * These tests are deliberately compile-time:  they read enum values that
 * the compiler resolves to integer constants.  No PG runtime functions
 * are called.
 * ----------
 */

UT_TEST(test_backend_num_types_is_29)
{
	/* 14 PG-native (B_INVALID..B_WAL_WRITER) + 15 pgrac = 29
	 * (spec-2.5 Sprint A added B_CSSD) */
	UT_ASSERT_EQ(BACKEND_NUM_TYPES, 29);
}

UT_TEST(test_pgrac_values_appended_after_wal_writer)
{
	/* Append-only ABI policy: every pgrac value sits above B_WAL_WRITER. */
	UT_ASSERT(B_CLUSTER_STATS > B_WAL_WRITER);
	UT_ASSERT(B_CSSD > B_WAL_WRITER);
	UT_ASSERT(B_DIAG > B_WAL_WRITER);
	UT_ASSERT(B_HEARTBEAT > B_WAL_WRITER);
	UT_ASSERT(B_INTERCONNECT > B_WAL_WRITER);
	UT_ASSERT(B_LCK > B_WAL_WRITER);
	UT_ASSERT(B_LMD > B_WAL_WRITER);
	UT_ASSERT(B_LMON > B_WAL_WRITER);
	UT_ASSERT(B_LMS_WORKER > B_WAL_WRITER);
	UT_ASSERT(B_MRP > B_WAL_WRITER);
	UT_ASSERT(B_RECOVERY_COORD > B_WAL_WRITER);
	UT_ASSERT(B_RECOVERY_WORKER > B_WAL_WRITER);
	UT_ASSERT(B_SINVAL_BCAST > B_WAL_WRITER);
	UT_ASSERT(B_TT_GC > B_WAL_WRITER);
	UT_ASSERT(B_UNDO_CLEANER > B_WAL_WRITER);
}

UT_TEST(test_pg_native_values_unchanged)
{
	/*
	 * Spot check the original 14 PG values to catch accidental
	 * reordering.  The full enum is alphabetic per PG convention,
	 * so B_INVALID is 0 and B_WAL_WRITER is 13 (last PG value).
	 */
	UT_ASSERT_EQ(B_INVALID, 0);
	UT_ASSERT_EQ(B_WAL_WRITER, 13); /* last PG-native value */
}

UT_TEST(test_pgrac_values_are_dense_and_distinct)
{
	/*
	 * 15 pgrac values must occupy positions 14..28 with no holes
	 * and no duplicates.  Asserting strict ordering proves both
	 * (alphabetic order matches enum order in §2.2 of spec-0.10).
	 */
	UT_ASSERT(B_CLUSTER_STATS < B_CSSD);
	UT_ASSERT(B_CSSD < B_DIAG);
	UT_ASSERT(B_DIAG < B_HEARTBEAT);
	UT_ASSERT(B_HEARTBEAT < B_INTERCONNECT);
	UT_ASSERT(B_INTERCONNECT < B_LCK);
	UT_ASSERT(B_LCK < B_LMD);
	UT_ASSERT(B_LMD < B_LMON);
	UT_ASSERT(B_LMON < B_LMS_WORKER);
	UT_ASSERT(B_LMS_WORKER < B_MRP);
	UT_ASSERT(B_MRP < B_RECOVERY_COORD);
	UT_ASSERT(B_RECOVERY_COORD < B_RECOVERY_WORKER);
	UT_ASSERT(B_RECOVERY_WORKER < B_SINVAL_BCAST);
	UT_ASSERT(B_SINVAL_BCAST < B_TT_GC);
	UT_ASSERT(B_TT_GC < B_UNDO_CLEANER);
}

UT_TEST(test_undo_cleaner_is_last)
{
	/*
	 * B_UNDO_CLEANER is the last value in the enum, so its index
	 * equals BACKEND_NUM_TYPES - 1.  This anchors the count.
	 */
	UT_ASSERT_EQ(B_UNDO_CLEANER, BACKEND_NUM_TYPES - 1);
}


int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_backend_num_types_is_29);
	UT_RUN(test_pgrac_values_appended_after_wal_writer);
	UT_RUN(test_pg_native_values_unchanged);
	UT_RUN(test_pgrac_values_are_dense_and_distinct);
	UT_RUN(test_undo_cleaner_is_last);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
