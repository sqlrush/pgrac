/* Original native factory plus complete physical pending scan.
 * Namespace/native/authority leaves are controlled; ROOT/files/census are real.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifdef CENSUS_NATIVE_EXECUTE_PARENT_CONTROL
static bool
cluster_relmap_retention_census_execute_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	return cluster_relmap_retention_census_open_native_episode(source, root_budget,
		memory_budget, payload_budget, out) == CLUSTER_RELMAP_CENSUS_MORE;
}
#endif

static unsigned native_execute_late_fault;

static void
native_execute_read(void *bytes, size_t length, off_t offset)
{
	unsigned fault;

	episode_read(bytes, length, offset);
	/* The second original namespace lookup completes the factory. Inject
	 * only during the following physical census, not its ROOT producer. */
	if (native_root_namespace_calls < 2 || native_execute_late_fault == 0)
		return;
	fault = native_execute_late_fault;
	native_execute_late_fault = 0;
	if (fault == 1) episode_sources[0].selected.native_window[159] ^= 1;
	if (fault == 2) episode_error = true;
	if (fault == 3) root_file(native_episode_fd, "base/6/late_target", "late", 4);
}

static bool
native_execute(CensusFixture *f)
{
	return cluster_relmap_retention_census_execute_native_episode(&episode_sources[1],
		4 * 1024 * 1024, 1024 * 1024, 128 * 1024, &f->census);
}

UT_TEST(test_native_execute_publishes_only_complete_clean_census)
{
	CensusFixture f;
	ClusterRelmapRetentionDirectory before[2];

	native_root_setup(&f, false);
	memcpy(before, f.origin, sizeof(before));
	UT_ASSERT(native_execute(&f));
	UT_ASSERT(f.census != NULL);
	if (f.census != NULL) {
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_DRAINED);
		cluster_relmap_retention_census_destroy(f.census);
		f.census = NULL;
	}
	UT_ASSERT_EQ(episode_failures, 0);
	UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
	UT_ASSERT_EQ(native_root_visits, 1);
	UT_ASSERT_EQ(native_episode_calls, 2);
	UT_ASSERT_EQ(census_execute_closes, 0);
	UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
	episode_cleanup(&f);
}

UT_TEST(test_native_execute_pending_in_either_origin_never_publishes_or_arbitrates)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CensusFixture f;
		native_root_setup(&f, false);
		if (fault == 0) census_pair(f.fd, 5, 3, true, true, false);
		if (fault == 1) census_pair(f.fd, 6, 7, false, true, false);
		if (fault == 2) census_pair(f.fd, InvalidOid, 3, false, true, false);
		UT_ASSERT(!native_execute(&f));
		UT_ASSERT(f.census == NULL);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_execute_late_source_root_and_error_keep_original_debt)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		CensusFixture f;
		volatile bool caught = false;
		native_root_setup(&f, false);
		native_execute_late_fault = fault;
		namespace_read_transform = native_execute_read;
		PG_TRY(); { UT_ASSERT(!native_execute(&f)); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(native_execute_late_fault, 0);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT_EQ(native_episode_calls, 2);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		native_execute_late_fault = 0;
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_execute_entry_alias_and_owner_error_publish_nothing)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		volatile bool caught = false;
		native_root_setup(&f, false);
		if (fault == 0) {
			ClusterRecoveryRelmapSourceV1 before = episode_sources[1];
			UT_ASSERT(!cluster_relmap_retention_census_execute_native_episode(&episode_sources[1],
				4 * 1024 * 1024, 1024 * 1024, 128 * 1024,
				(ClusterRelmapRetentionCensus **)&episode_sources[1]));
			UT_ASSERT_EQ(memcmp(&before, &episode_sources[1], sizeof(before)), 0);
			UT_ASSERT_EQ(episode_failures, 0);
		} else if (fault == 1) {
			CritSectionCount = 1;
			UT_ASSERT(!native_execute(&f));
			CritSectionCount = 0;
			UT_ASSERT_EQ(episode_failures, 0);
		} else if (fault == 2) {
			f.census = (void *)1;
			UT_ASSERT(!native_execute(&f));
			UT_ASSERT(f.census == (void *)1);
			f.census = NULL;
			UT_ASSERT_EQ(episode_failures, 0);
		} else {
			episode_error = true;
			PG_TRY(); { (void)native_execute(&f); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT(caught && episode_failures > 0);
		}
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_root_namespace_calls, 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		episode_cleanup(&f);
	}
}

static void
census_native_execute_run(void)
{
	UT_RUN(test_native_execute_publishes_only_complete_clean_census);
	UT_RUN(test_native_execute_pending_in_either_origin_never_publishes_or_arbitrates);
	UT_RUN(test_native_execute_late_source_root_and_error_keep_original_debt);
	UT_RUN(test_native_execute_entry_alias_and_owner_error_publish_nothing);
}
