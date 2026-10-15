/*-------------------------------------------------------------------------
 *
 * test_cluster_grd_ko_cancel.c
 *    Real cleanup consumer; exact KO snapshot/publication are boundary fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_grd_ko_cancel.c
 *
 * NOTES
 *    PGRAC-original standalone tests. Product APIs retain their cluster_
 *    namespace; dependency fixtures do not establish SQL or fabric results.
 *
 *-------------------------------------------------------------------------
 */
int grd_fixture_main(int argc, char **argv);
#define main grd_fixture_main
#include "test_cluster_grd.c"
#undef main

UT_TEST(test_cleanup_keeps_exact_cancel_holder_only)
{
	ClusterGrdEntry *entry;
	int saved = cluster_node_id;
	cluster_node_id = 1;
	convert_reset();
	entry = convert_make_entry(3101);
	convert_grant(entry, 1, 100, 11, ShareLock);
	convert_grant(entry, 1, 100, 12, ShareLock);
	memset(&ko_cleanup_holder, 0, sizeof(ko_cleanup_holder));
	ko_cleanup_holder.node_id = 1;
	ko_cleanup_holder.procno = 100;
	ko_cleanup_holder.request_id = 11;
	ko_cleanup_retained = true;
	ko_cleanup_drift = false;
	ko_cleanup_finished = 0;
	UT_ASSERT_EQ(cluster_grd_entry_cleanup_guarded(entry, 100, -1), 1);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(entry), 1);
	UT_ASSERT_EQ(ko_cleanup_finished, 1);
	UT_ASSERT_EQ(cluster_grd_entry_cleanup_guarded(entry, 100, -1), 0);
	ko_cleanup_retained = false;
	UT_ASSERT_EQ(cluster_grd_entry_cleanup_guarded(entry, 100, -1), 1);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(entry), 0);
	convert_teardown();
	cluster_node_id = saved;
}
UT_TEST(test_cleanup_version_drift_returns_to_original_sweeper)
{
	ClusterGrdEntry *entry;
	int saved = cluster_node_id;
	cluster_node_id = 1;
	convert_reset();
	entry = convert_make_entry(3102);
	convert_grant(entry, 1, 100, 11, ShareLock);
	ko_cleanup_retained = false;
	ko_cleanup_drift = true;
	ko_cleanup_finished = 0;
	UT_ASSERT_EQ(cluster_grd_entry_cleanup_guarded(entry, 100, -1), 0);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(entry), 1);
	UT_ASSERT_EQ(ko_cleanup_finished, 0);
	ko_cleanup_drift = false;
	UT_ASSERT_EQ(cluster_grd_entry_cleanup_guarded(entry, 100, -1), 1);
	UT_ASSERT_EQ(cluster_grd_entry_ngranted(entry), 0);
	convert_teardown();
	cluster_node_id = saved;
}
int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_cleanup_keeps_exact_cancel_holder_only);
	UT_RUN(test_cleanup_version_drift_returns_to_original_sweeper);
	UT_RUN(test_convert_u12_sweep_on_holder_death);
	UT_DONE();
	return ut_failed_count != 0;
}
