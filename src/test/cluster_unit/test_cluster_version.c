/*-------------------------------------------------------------------------
 *
 * test_cluster_version.c
 *	  Unit tests for pgrac_version_string().
 *
 *	  Verifies the format and content of the version string returned
 *	  by cluster_version.c.  This validates CLAUDE.md rule 19 version
 *	  numbering policy is observed in practice.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_version.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "unit_test.h"
#include "cluster/cluster_version.h"
#include "cluster/cluster_version_macros.h"

UT_DEFINE_GLOBALS();


/* ----------
 * Content tests on pgrac_version_string() (legacy, established at 0.5)
 * ----------
 */

UT_TEST(test_returns_non_null)
{
	UT_ASSERT_NOT_NULL(pgrac_version_string());
}

UT_TEST(test_contains_pgrac_prefix)
{
	UT_ASSERT_STR_CONTAINS(pgrac_version_string(), "pgrac");
}

UT_TEST(test_contains_pg_base_version)
{
	/* must declare which PG version this is based on */
	UT_ASSERT_STR_CONTAINS(pgrac_version_string(), "16.13");
}

UT_TEST(test_contains_stage_marker)
{
	/* must declare current development stage (CLAUDE.md rule 19) */
	UT_ASSERT_STR_CONTAINS(pgrac_version_string(), "stage");
}

UT_TEST(test_contains_v_marker)
{
	/* version starts with "v" prefix per semver convention */
	UT_ASSERT_STR_CONTAINS(pgrac_version_string(), "v0.");
}


/* ----------
 * Macro tests (added in 0.8 with cluster_version_macros.h)
 * ----------
 */

UT_TEST(test_macro_version_major)
{
	UT_ASSERT_EQ(PGRAC_VERSION_MAJOR, 0);
}

UT_TEST(test_macro_version_minor)
{
	/* Spec-2.1 D8: bump 2 -> 8 (Q-I versioning ladder Stage 2 entry). */
	UT_ASSERT_EQ(PGRAC_VERSION_MINOR, 8);
}

UT_TEST(test_macro_pg_base_version)
{
	UT_ASSERT_STR_EQ(PGRAC_PG_BASE_VERSION, "16.13");
}

UT_TEST(test_macro_string_matches_function)
{
	/* the function is just a wrapper around the macro */
	UT_ASSERT_STR_EQ(PGRAC_VERSION_STRING, pgrac_version_string());
}


int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_returns_non_null);
	UT_RUN(test_contains_pgrac_prefix);
	UT_RUN(test_contains_pg_base_version);
	UT_RUN(test_contains_stage_marker);
	UT_RUN(test_contains_v_marker);
	UT_RUN(test_macro_version_major);
	UT_RUN(test_macro_version_minor);
	UT_RUN(test_macro_pg_base_version);
	UT_RUN(test_macro_string_matches_function);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
