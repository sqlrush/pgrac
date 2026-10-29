/* Actual original census/root namespace, with interrupt/source fault leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool census_namespace_probe;
static unsigned census_namespace_calls, census_namespace_fault;
static void
census_namespace_interrupts(void)
{
	if (census_namespace_probe && ++census_namespace_calls == 4) {
		if (census_namespace_fault == 1) inputs_pin_current = false;
		if (census_namespace_fault == 2) pg_re_throw();
	}
}
UT_TEST(test_census_namespace_complete_set_keeps_original_pages_and_manifest)
{
	ClusterWalInputsV1 *inputs; int fd, file; char path[MAXPGPATH];
	uint32 at = census_current_setup(&inputs, &fd, path);
	ClusterRecoveryCensus *cursor = census_current_owner;
	ClusterRecoveryCensusManifest saved = cursor->manifest;
	ClusterRecoveryCensusPageV1 page = cursor->pages->entries[at];

	UT_ASSERT(cluster_recovery_census_namespace_current_v1(cursor));
	file = openat(fd, "base/5/16386", O_RDWR); UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(pwrite(file, "x", 1, BLCKSZ-1), 1); UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT(cluster_recovery_census_namespace_current_v1(cursor));
	UT_ASSERT(memcmp(&saved, &cursor->manifest, sizeof(saved)) == 0);
	UT_ASSERT(memcmp(&page, &cursor->pages->entries[at], sizeof(page)) == 0);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	census_current_cleanup(&inputs, fd, path);
}
UT_TEST(test_census_namespace_added_object_revokes_private_census_not_original_pin)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	(void)census_current_setup(&inputs, &fd, path);
	{ int added = openat(fd, "base/5/new-physical", O_CREAT | O_WRONLY, 0600);
		UT_ASSERT(added >= 0); UT_ASSERT_EQ(write(added, "x", 1), 1); UT_ASSERT_EQ(close(added), 0); }
	UT_ASSERT(!cluster_recovery_census_namespace_current_v1(census_current_owner));
	UT_ASSERT_EQ(census_current_owner->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(census_current_owner->roots == NULL && census_current_owner->pages == NULL);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/new-physical", 0), 0);
	census_current_cleanup(&inputs, fd, path);
}
UT_TEST(test_census_namespace_preflight_and_entered_source_or_error_keep_pin)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
		volatile bool caught = false;
		(void)census_current_setup(&inputs, &fd, path);
		census_namespace_probe = true; census_namespace_calls = 0; census_namespace_fault = fault;
		if (fault == 0) {
			inventory_graph_caller_lock = true;
			UT_ASSERT(!cluster_recovery_census_namespace_current_v1(census_current_owner));
			UT_ASSERT_EQ(census_namespace_calls, 0);
			UT_ASSERT_EQ(census_current_owner->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
			inventory_graph_caller_lock = false;
			UT_ASSERT(cluster_recovery_census_namespace_current_v1(census_current_owner));
		} else {
			PG_TRY(); { UT_ASSERT(!cluster_recovery_census_namespace_current_v1(census_current_owner)); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2); UT_ASSERT_EQ(census_namespace_calls, 4);
			UT_ASSERT_EQ(census_current_owner->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
			UT_ASSERT(census_current_owner->roots == NULL);
		}
		census_namespace_probe = false;
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_current_cleanup(&inputs, fd, path);
	}
}
