/*-------------------------------------------------------------------------
 * test_cluster_relmap_census_native_history.inc
 *     All retained checkpoint generations in one original episode census.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_relmap_census_native_history.inc
 *
 * NOTES
 *     Native/source leaves are controlled. Original ROOT, both relmap copies,
 *     the factories and the complete census driver execute unchanged.
 *-------------------------------------------------------------------------
 */
static void
native_history_setup(CensusFixture *f, bool pending)
{
	native_root_setup(f, pending);
	/* Keep the original replay at index2, with a distinct older generation
	 * at index0 of the SAME origin. Physical source order is independent. */
	episode_sources[2] = episode_sources[0];
	native_episode_inputs[2] = native_episode_inputs[0];
	episode_sources[0].selected.current = false;
	episode_sources[0].selected.source.claim.identity.origin_owner_incarnation = 707;
	episode_sources[0].selected.checkpoint.root_publish_seq++;
	episode_sources[0].selected.native_window[159]++;
	episode_count = 3;
	for (unsigned i = 0; i < episode_count; i++) {
		episode_sources[i].input_index = i;
		episode_sources[i].input_count = episode_count;
		episode_sources[i].replay = episode_sources[i == 1 ? 1 : 2].selected;
		native_episode_inputs[i].source = episode_sources[i];
	}
	native_episode_inputs[0].window.native_floor++;
	native_episode_inputs[0].post_flush_issued--;
}

UT_TEST(test_native_history_factory_uses_original_replay_window_without_widening)
{
	CensusFixture f;
	ClusterRelmapRetentionDirectory before[2];

	native_history_setup(&f, true);
	memcpy(before, f.origin, sizeof(before));
	UT_ASSERT_EQ(native_episode_begin(&f, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
	UT_ASSERT_EQ(native_episode_calls, 3);
	if (f.census != NULL)
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
	UT_ASSERT_EQ(episode_failures, 0);
	episode_cleanup(&f);
}

UT_TEST(test_native_history_root_scan_and_driver_keep_every_generation)
{
	for (unsigned execute = 0; execute < 2; execute++) {
		CensusFixture f;

		native_history_setup(&f, false);
		if (execute)
			UT_ASSERT(cluster_relmap_retention_census_execute_native_episode(&episode_sources[0],
				4 * 1024 * 1024, 1024 * 1024, 128 * 1024, &f.census));
		else
			UT_ASSERT_EQ(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
		UT_ASSERT_EQ(native_root_visits, 1);
		UT_ASSERT_EQ(native_root_collects, 3);
		UT_ASSERT_EQ(native_episode_calls, 3);
		if (f.census != NULL)
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT_EQ(episode_failures, 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_history_whole_replay_mapping_precedes_namespace_and_native_io)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;

		native_history_setup(&f, true);
		if (fault == 0) episode_sources[2].selected.source.claim.identity.origin_owner_incarnation++;
		if (fault == 1) episode_sources[0].replay_index = 0;
		if (fault == 2) episode_sources[0].selected = episode_sources[2].selected;
		if (fault == 3) episode_sources[0].selected.kind = CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL;
		UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, fault == 4 ? 1 : 1024 * 1024)
			> CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT_EQ(native_root_namespace_calls, 0);
		UT_ASSERT_EQ(native_episode_calls, 0);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_history_window_refusals_and_error_keep_original_protection)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionDirectory before[2];
		volatile bool caught = false;

		native_history_setup(&f, true);
		memcpy(before, f.origin, sizeof(before));
		if (fault == 0) native_episode_inputs[0].window.native_floor = UINT64CONST(0x100000003);
		if (fault == 1) native_episode_inputs[0].post_flush_issued += 2;
		if (fault == 2) memset(&native_episode_inputs[0], 0, sizeof(native_episode_inputs[0]));
		if (fault == 3) native_episode_fault = 5;
		PG_TRY(); {
			UT_ASSERT(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024)
				> CLUSTER_RELMAP_CENSUS_DRAINED);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT(f.census == NULL);
		UT_ASSERT(episode_failures > 0);
		UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
		UT_ASSERT_EQ(census_execute_closes, 0);
		UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		episode_cleanup(&f);
	}
}

UT_TEST(test_native_history_late_generation_drift_invalidates_whole_cursor)
{
	CensusFixture f;

	native_history_setup(&f, false);
	UT_ASSERT_EQ(native_root_begin(&f, 4 * 1024 * 1024, 1024 * 1024), CLUSTER_RELMAP_CENSUS_MORE);
	if (f.census != NULL) {
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		episode_sources[0].selected.native_window[159]++;
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), CLUSTER_RELMAP_CENSUS_CHANGED);
		UT_ASSERT(episode_failures > 0);
	}
	UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
	episode_cleanup(&f);
}

static void
census_native_history_run(void)
{
	UT_RUN(test_native_history_factory_uses_original_replay_window_without_widening);
	UT_RUN(test_native_history_root_scan_and_driver_keep_every_generation);
	UT_RUN(test_native_history_whole_replay_mapping_precedes_namespace_and_native_io);
	UT_RUN(test_native_history_window_refusals_and_error_keep_original_protection);
	UT_RUN(test_native_history_late_generation_drift_invalidates_whole_cursor);
}
