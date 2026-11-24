/* Physical ROOT/census drainage retains its provisional status without the
 * original joint recovery completion owner. No completion receipt is forged.
 * Author: SqlRush <sqlrush@gmail.com> */
#define RELMAP_CENSUS_FIXTURE_ONLY
#include "test_cluster_relmap_retention_census.c"
#undef RELMAP_CENSUS_FIXTURE_ONLY

UT_TEST(k_drained_root_without_joint_owner_cannot_publish_history)
{
	CensusFixture f;
	ClusterRelmapRetentionDirectory before[2];

	census_fixture(&f, false);
	if (census_observe_begin(&f)) {
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		memcpy(before, f.origin, sizeof(before));
		UT_ASSERT(!cluster_relmap_retention_census_publish_history_v1(f.census, NULL));
		UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
		UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(f.origin[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
	}
	census_cleanup(&f);
}

UT_TEST(k_missing_history_qualification_never_retires_pending_duty)
{
	CensusFixture f;
	ClusterRelmapRetentionDirectory before[2];

	census_fixture(&f, true);
	if (census_observe_begin(&f)) {
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		memcpy(before, f.origin, sizeof(before));
		UT_ASSERT(!cluster_relmap_retention_census_publish_history_v1(f.census, NULL));
		UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
	}
	census_cleanup(&f);
}

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(k_drained_root_without_joint_owner_cannot_publish_history);
	UT_RUN(k_missing_history_qualification_never_retires_pending_duty);
	UT_DONE();
	return ut_failed_count != 0;
}
