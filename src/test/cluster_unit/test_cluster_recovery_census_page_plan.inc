/* Original complete census and physical WAL PAGE graph integration.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRecoveryCensus *page_census_changed;
static void page_census_throw(void) { pg_re_throw(); }
static void page_census_drift(void) { page_census_changed->pages->entries[0].page_sha256[0]++; }

static ClusterRecoveryCensus *
page_census_fixture(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH], bool pages)
{
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryCensus *census = NULL;
	PGAlignedBlock zero = {0};
	char name[96];
	*inputs = inventory_graph_fixture(pages);
	roots = inventory_roots(*inputs, path, fd, false);
	cluster_recovery_root_set_destroy(roots);
	owned_census_relation(*fd, &cluster_wal_inputs_at_v1(*inputs, 0)->source, 16385, false);
	owned_census_relation(*fd, &cluster_wal_inputs_at_v1(*inputs, 0)->source, 99999, true);
	if (pages) {
		ClusterSpaceReservation reservation = {0};
		PGAlignedBlock space[2];
		const ClusterWalSourceRef *source = &cluster_wal_inputs_at_v1(*inputs, 0)->source;
		int file = openat(*fd, "base/5/16386", O_CREAT | O_WRONLY, 0600);
		UT_ASSERT(file >= 0);
		for (unsigned block = 0; block < 8; block++)
			UT_ASSERT_EQ(write(file, zero.data, BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(file), 0);
		reservation.identity.key.system_identifier = source->claim.identity.system_identifier;
		reservation.identity.key.database_incarnation = source->claim.database_incarnation;
		memcpy(reservation.identity.key.storage_uuid, source->claim.identity.storage_uuid, 16);
		reservation.identity.key.locator = (RelFileLocator){1663, 5, 16386};
		memset(reservation.identity.incarnation, 0x21, 16);
		reservation.identity.sequence = 2; reservation.identity.operation = 9;
		reservation.identity.state = CLUSTER_SPACE_IDENTITY_LIVE; reservation.next_block = 8;
		UT_ASSERT(cluster_space_identity_page_encode(&reservation.identity, 7, space[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 11, space[1].data, BLCKSZ));
		file = openat(*fd, "base/5/16386_space", O_CREAT | O_WRONLY, 0600); UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(write(file, space, sizeof(space)), sizeof(space)); UT_ASSERT_EQ(close(file), 0);
	}
	UT_ASSERT_EQ(mkdirat(*fd, "pg_undo", 0700), 0);
	UT_ASSERT_EQ(mkdirat(*fd, "native_side", 0700), 0);
	for (unsigned node = 0; node < 2; node++) {
		int file;
		snprintf(name, sizeof(name), "native_side/origin_%u", node);
		UT_ASSERT_EQ(mkdirat(*fd, name, 0700), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact", node);
		UT_ASSERT_EQ(mkdirat(*fd, name, 0700), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact/0000", node);
		file = openat(*fd, name, O_CREAT | O_WRONLY, 0600); UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(write(file, zero.data, BLCKSZ), BLCKSZ); UT_ASSERT_EQ(close(file), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact/0004", node);
		file = openat(*fd, name, O_CREAT | O_WRONLY, 0600); UT_ASSERT(file >= 0);
		UT_ASSERT_EQ(write(file, zero.data, BLCKSZ), BLCKSZ); UT_ASSERT_EQ(close(file), 0);
	}
	UT_ASSERT_EQ(cluster_recovery_census_begin(*inputs, *fd, 8 * 1024 * 1024, &census),
		CLUSTER_RECOVERY_CENSUS_MORE);
	return census;
}

static void
page_census_done(ClusterRecoveryCensus *census, ClusterWalInputsV1 **inputs, int fd, const char *path)
{
	char name[96];
	cluster_recovery_census_destroy(census);
	for (unsigned node = 0; node < 2; node++) {
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact/0000", node);
		UT_ASSERT_EQ(unlinkat(fd, name, 0), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact/0004", node);
		UT_ASSERT_EQ(unlinkat(fd, name, 0), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u/pg_xact", node);
		UT_ASSERT_EQ(unlinkat(fd, name, AT_REMOVEDIR), 0);
		snprintf(name, sizeof(name), "native_side/origin_%u", node);
		UT_ASSERT_EQ(unlinkat(fd, name, AT_REMOVEDIR), 0);
	}
	UT_ASSERT_EQ(unlinkat(fd, "native_side", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "pg_undo", AT_REMOVEDIR), 0);
	if (faccessat(fd, "base/5/16386", F_OK, 0) == 0) {
		UT_ASSERT_EQ(unlinkat(fd, "base/5/16386", 0), 0);
		UT_ASSERT_EQ(unlinkat(fd, "base/5/16386_space", 0), 0);
	}
	UT_ASSERT_EQ(unlinkat(fd, "base/5/16385", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/16385_space", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "base/5/99999_space", 0), 0);
	inventory_roots_done(NULL, fd, path);
	inventory_between_passes = NULL; inputs_pin_current = true;
	cold_census_done(inputs); inventory_extra = 0;
}

UT_TEST(test_census_page_graph_preserves_original_full_manifest_and_owned_plan)
{
	for (unsigned pages = 0; pages < 2; pages++) {
		ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
		ClusterRecoveryCensus *census = page_census_fixture(&inputs, &fd, path, pages);
		ClusterRecoveryCensusManifest before, after;
		ClusterRecoveryPagePlanBuildV1 built;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT(cluster_recovery_census_manifest(census, &before));
		built = cluster_recovery_census_page_plan_v1(census, 1024 * 1024);
		UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.plan != NULL);
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(built.plan), pages);
		for (uint32 i = 0; i < before.source_count; i++) {
			ClusterWalSourceRef source;
			UT_ASSERT(rf_page_online_plan_source_v1(built.plan, i, &source));
		}
		{ ClusterWalSourceRef source;
			UT_ASSERT(!rf_page_online_plan_source_v1(built.plan, before.source_count, &source)); }
		UT_ASSERT(cluster_recovery_census_manifest(census, &after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		/* Private graph owns copied input; census teardown cannot free it. */
		cluster_recovery_census_destroy(census); census = NULL;
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(built.plan), pages);
		rf_page_online_plan_destroy_v1(&built.plan);
		page_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_census_page_graph_never_advances_prefix_or_enters_under_caller_lock)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = page_census_fixture(&inputs, &fd, path, true);
	ClusterRecoveryPagePlanBuildV1 built;
	unsigned calls = inventory_census_calls;
	built = cluster_recovery_census_page_plan_v1(census, 1024 * 1024);
	UT_ASSERT(built.plan == NULL); UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_MORE);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	calls = inventory_census_calls; inventory_graph_caller_lock = true;
	built = cluster_recovery_census_page_plan_v1(census, 1024 * 1024);
	inventory_graph_caller_lock = false;
	UT_ASSERT(built.plan == NULL); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	built = cluster_recovery_census_page_plan_v1(census, 1024 * 1024);
	UT_ASSERT(built.plan != NULL); rf_page_online_plan_destroy_v1(&built.plan);
	page_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_page_graph_factory_failure_and_final_manifest_drift_are_sticky)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
		ClusterRecoveryCensus *census = page_census_fixture(&inputs, &fd, path, true);
		ClusterRecoveryPagePlanBuildV1 built;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		if (fault == 1) { inventory_fault = 0; inventory_drift(); }
		if (fault == 2) {
			page_census_changed = census; inventory_census_calls = 0;
			inventory_between_passes = page_census_drift;
		}
		built = cluster_recovery_census_page_plan_v1(census, fault == 0 ? 1 : 1024 * 1024);
		inventory_between_passes = NULL;
		UT_ASSERT(built.plan == NULL); UT_ASSERT_NE(built.detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs != NULL && inputs_pin_held);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		page_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_census_page_graph_error_invalidates_observation_without_releasing_input)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = page_census_fixture(&inputs, &fd, path, true);
	volatile bool caught = false;
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	inventory_census_calls = 0; inventory_between_passes = page_census_throw;
	PG_TRY(); { (void)cluster_recovery_census_page_plan_v1(census, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	inventory_between_passes = NULL;
	UT_ASSERT(caught); UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(inputs != NULL && inputs_pin_held);
	page_census_done(census, &inputs, fd, path);
}
