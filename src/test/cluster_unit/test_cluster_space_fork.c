/*-------------------------------------------------------------------------
 *
 * test_cluster_space_fork.c
 *    Exercise native relation paths for the persistent SPACE fork.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_fork.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_space_name_and_suffix)
{
	ForkNumber fork = InvalidForkNumber;

	UT_ASSERT_EQ(forkname_to_number("space"), 4);
	UT_ASSERT_EQ(forkname_chars("space.2", &fork), 5);
	UT_ASSERT_EQ(fork, 4);
	UT_ASSERT_EQ(forkname_to_number("spaces"), InvalidForkNumber);
	UT_ASSERT_EQ(forkname_chars("spac", &fork), 0);
	UT_ASSERT_EQ(fork, InvalidForkNumber);
}

UT_TEST(test_space_relation_paths)
{
	ForkNumber fork = forkname_to_number("space");
	char *path;
	char expected[MAXPGPATH];

	UT_ASSERT_EQ(fork, 4); /* Never index forkNames with a missing fork. */
	if (fork != 4)
		return;
	path = GetRelationPath(5, DEFAULTTABLESPACE_OID, 16384, -1, fork);
	UT_ASSERT_STR_EQ(path, "base/5/16384_space");
	pfree(path);
	path = GetRelationPath(0, GLOBALTABLESPACE_OID, 16384, -1, fork);
	UT_ASSERT_STR_EQ(path, "global/16384_space");
	pfree(path);
	path = GetRelationPath(5, 777, 16384, -1, fork);
	snprintf(expected, sizeof(expected), "pg_tblspc/777/%s/5/16384_space",
			 TABLESPACE_VERSION_DIRECTORY);
	UT_ASSERT_STR_EQ(path, expected);
	pfree(path);
}

UT_TEST(test_legacy_paths_and_unknown_names)
{
	char *path = GetRelationPath(5, DEFAULTTABLESPACE_OID, 16384, -1, MAIN_FORKNUM);

	UT_ASSERT_STR_EQ(path, "base/5/16384");
	pfree(path);
	UT_ASSERT_EQ(forkname_to_number("fsm"), 1);
	UT_ASSERT_EQ(forkname_to_number("vm"), 2);
	UT_ASSERT_EQ(forkname_to_number("init"), 3);
	UT_ASSERT_EQ(forkname_to_number("SPACE"), InvalidForkNumber);
	UT_ASSERT_EQ(forkname_to_number(""), InvalidForkNumber);
	path = GetRelationPath(5, DEFAULTTABLESPACE_OID, 16384, -1, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_STR_EQ(path, "base/5/16384_vm");
	pfree(path);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_space_name_and_suffix);
	UT_RUN(test_space_relation_paths);
	UT_RUN(test_legacy_paths_and_unknown_names);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
