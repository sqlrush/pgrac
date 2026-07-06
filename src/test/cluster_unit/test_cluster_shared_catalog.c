/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_catalog.c
 *	  Runtime unit tests for the shared_catalog pure decision helpers
 *	  (spec-6.14 D1): the startup dependency vet and the "non-temp = shared"
 *	  routing criterion.
 *
 *	  Covers (spec §4):
 *	    U8-U10  gate criterion three-state (off unchanged is exercised at the
 *	            call sites; here the pure on-mode criterion: non-temp = shared,
 *	            temp = node-local)
 *	    U14-U16 vet predicate: OK when off; first-missing-dependency priority
 *	            order when on; dependency-name mapping for the FATAL errhint
 *
 *	  Links cluster_shared_catalog.o (dependency-light: only c.h).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_catalog.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D1, U8-U10 / U14-U16)
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include "cluster/cluster_shared_catalog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * libpgport's snprintf (linked under --enable-cassert) references
 * ExceptionalCondition; the pure module itself never triggers an Assert.
 * Provide the stub so the test links standalone.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* ----------
 * U14: vet returns OK whenever shared_catalog is off, regardless of the
 * dependency state (the feature imposes no requirements when disabled, and
 * the off path is byte-identical to stock PG).
 * ----------
 */
UT_TEST(test_vet_off_always_ok)
{
	UT_ASSERT_EQ(cluster_shared_catalog_vet(false, false, false, false, false),
				 CLUSTER_SHARED_CATALOG_VET_OK);
	UT_ASSERT_EQ(cluster_shared_catalog_vet(false, true, true, true, true),
				 CLUSTER_SHARED_CATALOG_VET_OK);
	/* mixed dependency state, still off -> still OK */
	UT_ASSERT_EQ(cluster_shared_catalog_vet(false, true, false, true, false),
				 CLUSTER_SHARED_CATALOG_VET_OK);
}

/* ----------
 * U15: with shared_catalog on and all dependencies satisfied -> OK; with any
 * dependency missing -> the FIRST missing one in priority order
 * (smgr_user_relations, then shared_data_dir, then controlfile authority,
 * then merged_recovery -- spec-6.14 D9 amend INV-D9-R).
 * ----------
 */
UT_TEST(test_vet_on_all_present_ok)
{
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, true, true, true),
				 CLUSTER_SHARED_CATALOG_VET_OK);
}

UT_TEST(test_vet_on_missing_smgr_user_relations_first)
{
	/* smgr_user_relations is the highest-priority dependency: reported even
	 * when the other three are ALSO missing. */
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, false, false, false, false),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS);
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, false, true, true, true),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS);
}

UT_TEST(test_vet_on_missing_shared_data_dir_second)
{
	/* smgr present, shared_data_dir missing -> shared_data_dir, even when the
	 * cf authority is also missing. */
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, false, false, false),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR);
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, false, true, true),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR);
}

UT_TEST(test_vet_on_missing_cf_authority_third)
{
	/* smgr + shared_data_dir present, cf authority missing: reported even
	 * when merged_recovery is also missing. */
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, true, false, false),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY);
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, true, false, true),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY);
}

UT_TEST(test_vet_on_missing_merged_recovery_last)
{
	/* All other dependencies present, only merged_recovery missing: cold
	 * crash redo of PCM-tracked catalog pages needs the merged-replay
	 * window (spec-6.14 D9 amend INV-D9-R). */
	UT_ASSERT_EQ(cluster_shared_catalog_vet(true, true, true, true, false),
				 CLUSTER_SHARED_CATALOG_VET_MISSING_MERGED_RECOVERY);
}

/* ----------
 * U16: dependency-name mapping (used in the FATAL errhint).  OK maps to "".
 * ----------
 */
UT_TEST(test_vet_missing_dep_name)
{
	UT_ASSERT_STR_EQ(cluster_shared_catalog_vet_missing_dep_name(CLUSTER_SHARED_CATALOG_VET_OK),
					 "");
	UT_ASSERT_STR_CONTAINS(cluster_shared_catalog_vet_missing_dep_name(
							   CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS),
						   "smgr_user_relations");
	UT_ASSERT_STR_CONTAINS(cluster_shared_catalog_vet_missing_dep_name(
							   CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR),
						   "shared_data_dir");
	UT_ASSERT_STR_CONTAINS(cluster_shared_catalog_vet_missing_dep_name(
							   CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY),
						   "controlfile_shared_authority");
	UT_ASSERT_STR_CONTAINS(cluster_shared_catalog_vet_missing_dep_name(
							   CLUSTER_SHARED_CATALOG_VET_MISSING_MERGED_RECOVERY),
						   "merged_recovery");
}

/* ----------
 * U8-U10 (pure part): the "non-temp = shared" routing criterion.  Under
 * shared_catalog=on a temp relation is always node-local; everything else
 * (catalog and user, permanent) is cluster-shared -- the historic
 * FirstNormalObjectId catalog/user split is gone.
 * ----------
 */
UT_TEST(test_is_shared_rel_temp_is_local)
{
	UT_ASSERT_EQ(cluster_shared_catalog_is_shared_rel(true), false);
}

UT_TEST(test_is_shared_rel_nontemp_is_shared)
{
	UT_ASSERT_EQ(cluster_shared_catalog_is_shared_rel(false), true);
}

/* ----------
 * D13 temp-namespace suffix format / parse round trip.
 * ----------
 */
UT_TEST(test_temp_suffix_format_off_is_stock)
{
	char buf[32];

	cluster_temp_namespace_format_suffix(buf, sizeof(buf), false, 0, 12);
	UT_ASSERT_STR_EQ(buf, "12");
	cluster_temp_namespace_format_suffix(buf, sizeof(buf), false, 3, 99);
	UT_ASSERT_STR_EQ(buf, "99"); /* node ignored in off mode */
}

UT_TEST(test_temp_suffix_format_on_is_node_qualified)
{
	char buf[32];

	cluster_temp_namespace_format_suffix(buf, sizeof(buf), true, 0, 12);
	UT_ASSERT_STR_EQ(buf, "n0_12");
	cluster_temp_namespace_format_suffix(buf, sizeof(buf), true, 2, 7);
	UT_ASSERT_STR_EQ(buf, "n2_7");
}

UT_TEST(test_temp_suffix_parse_stock)
{
	int node = 999;
	int backend;

	backend = cluster_temp_namespace_parse_suffix("12", &node);
	UT_ASSERT_EQ(backend, 12);
	UT_ASSERT_EQ(node, -1); /* stock format: no node */
}

UT_TEST(test_temp_suffix_parse_node_qualified)
{
	int node = 999;
	int backend;

	backend = cluster_temp_namespace_parse_suffix("n2_7", &node);
	UT_ASSERT_EQ(backend, 7);
	UT_ASSERT_EQ(node, 2);

	backend = cluster_temp_namespace_parse_suffix("n0_12", &node);
	UT_ASSERT_EQ(backend, 12);
	UT_ASSERT_EQ(node, 0);
}

UT_TEST(test_temp_suffix_round_trip)
{
	char buf[32];
	int node = 999;
	int backend;

	cluster_temp_namespace_format_suffix(buf, sizeof(buf), true, 5, 42);
	backend = cluster_temp_namespace_parse_suffix(buf, &node);
	UT_ASSERT_EQ(backend, 42);
	UT_ASSERT_EQ(node, 5);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_vet_off_always_ok);
	UT_RUN(test_vet_on_all_present_ok);
	UT_RUN(test_vet_on_missing_smgr_user_relations_first);
	UT_RUN(test_vet_on_missing_shared_data_dir_second);
	UT_RUN(test_vet_on_missing_cf_authority_third);
	UT_RUN(test_vet_on_missing_merged_recovery_last);
	UT_RUN(test_vet_missing_dep_name);
	UT_RUN(test_is_shared_rel_temp_is_local);
	UT_RUN(test_is_shared_rel_nontemp_is_shared);
	UT_RUN(test_temp_suffix_format_off_is_stock);
	UT_RUN(test_temp_suffix_format_on_is_node_qualified);
	UT_RUN(test_temp_suffix_parse_stock);
	UT_RUN(test_temp_suffix_parse_node_qualified);
	UT_RUN(test_temp_suffix_round_trip);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
