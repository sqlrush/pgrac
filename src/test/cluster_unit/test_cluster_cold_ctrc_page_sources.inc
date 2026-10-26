/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_page_sources.inc
 *   Original Startup PAGE plan-to-native-generation composition.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Original Start, observation, budget and cleanup execute. Physical census,
 * sealed graph and native generation/holder leaves remain controlled here;
 * native generation custody has its separate real ROOT/input suite.
 *-------------------------------------------------------------------------
 */
static unsigned page_source_fault, page_source_factories;
static uint32 page_source_input_count;
static bool page_source_error, page_source_native_current, page_source_current_error;
static void (*page_source_current_step)(void);
static void (*page_source_step)(void);

static void
page_source_fixture_reset(void)
{
	page_source_fault = page_source_factories = 0;
	page_source_input_count = 1;
	page_source_error = page_source_current_error = false; page_source_native_current = true;
	page_source_current_step = NULL;
	page_source_step = NULL;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&observation_scope_object);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	return page_source_input_count;
}

static ClusterRecoveryCtrcReplaySetSourceV1
page_source_value(ClusterWalInputsV1 *scope, uint32 index)
{
	ClusterRecoveryCtrcReplaySetSourceV1 value = {0};
	uint32 ordinal = page_source_input_count == 3 ? (index + 1) % 3 : index;
	const ClusterRecoveryWalSourceObservationV1 *observed;
	if (index >= page_source_input_count || ordinal >= lengthof(observation_wal_sources))
		return value;
	observed = &observation_wal_sources[ordinal];
	value.scope = scope; value.ir_request_id = 817;
	value.work_generation = native_ctrc_work.generation;
	value.replay_count = 1; value.generation_count = page_source_input_count;
	value.generation_index = index;
	value.generation.source = observed->source;
	value.generation_begin = observed->cut.scan_begin_inclusive;
	value.generation_end = observed->cut.scan_end_exclusive;
	value.generation_flags = observed->cut.flags;
	return value;
}

ClusterRecoveryCtrcReplaySetSourceV1
cluster_wal_inputs_ctrc_generation_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	ClusterRecoveryCtrcReplaySetSourceV1 value;
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&observation_scope_object);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	page_source_factories++;
	if (page_source_error) pg_re_throw();
	value = page_source_value(inputs, index);
	if (page_source_fault == 1) memset(&value, 0, sizeof(value));
	if (page_source_fault == 2) value.generation.source.claim.claim_sha256[31]++;
	if (page_source_fault == 3) value.generation_end++;
	if (page_source_fault == 4) value.generation_index = 0;
	if (page_source_fault == 5) value.generation_flags ^= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	if (page_source_step != NULL) page_source_step();
	return value;
}

bool
cluster_wal_inputs_ctrc_replays_source_current_v1(const ClusterRecoveryCtrcReplaySetSourceV1 *source)
{
	ClusterRecoveryCtrcReplaySetSourceV1 expected;
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	expected = page_source_value((ClusterWalInputsV1 *)&observation_scope_object,
		source->generation_index);
	if (page_source_current_error) pg_re_throw();
	if (page_source_current_step != NULL) page_source_current_step();
	return page_source_native_current && memcmp(source, &expected, sizeof(expected)) == 0;
}

bool
rf_page_online_plan_source_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
	ClusterWalSourceRef *out)
{
	UT_ASSERT(plan == (RfPageOnlinePlanV1 *)&page_graph_token && page_graph_live);
	if (index >= observation_wal_source_count || index >= lengthof(observation_wal_sources))
		return false;
	*out = observation_wal_sources[index].source;
	return true;
}

static void
page_sources_three_ready(void)
{
	observation_two_ready();
	page_source_input_count = observation_wal_source_count = observation_manifest.source_count = 3;
	for (unsigned i = 1; i < 3; i++) {
		observation_wal_sources[i] = observation_wal_sources[0];
		observation_wal_sources[i].source.claim.identity.origin_owner_incarnation = 29 + i;
		observation_wal_sources[i].source.claim.claim_sha256[31] = i;
		observation_wal_sources[i].source_index = i;
		observation_wal_sources[i].cut.origin_owner_incarnation = 29 + i;
	}
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
}

UT_TEST(test_startup_page_sources_refuse_missing_or_changed_binding)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		page_sources_three_ready(); page_source_fault = fault;
		UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_startup_page_sources_reserve_vector_budget_before_graph)
{
	ctrc_replay_request_fixture();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1));
	UT_ASSERT_EQ(page_graph_builds, 0);
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_sources_error_revokes_original_episode)
{
	volatile bool caught = false;
	ctrc_replay_request_fixture();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	page_source_error = true;
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(observation_scope_releases, 1); UT_ASSERT_EQ(observation_census_destroys, 1);
}

UT_TEST(test_startup_page_sources_revalidate_native_owner_on_repeat)
{
	ctrc_replay_request_fixture();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	page_source_native_current = false;
	UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 0));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(page_graph_builds, 1); UT_ASSERT_EQ(observation_scope_releases, 0);
}

UT_TEST(test_startup_page_sources_bind_three_original_indexes_in_graph_order)
{
	const ClusterRecoveryCtrcReplaySetSourceV1 *sources, *again;
	unsigned builds, factories, mutations;
	page_sources_three_ready(); builds = page_graph_builds;
	mutations = wal_inserts + stamp_writes;
	UT_ASSERT(ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3) == NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	sources = ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3);
	UT_ASSERT(sources != NULL);
	if (sources == NULL) return;
	UT_ASSERT_EQ(sources[0].generation_index, 2);
	UT_ASSERT_EQ(sources[1].generation_index, 0);
	UT_ASSERT_EQ(sources[2].generation_index, 1);
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT_EQ(sources[i].generation_count, 3);
		UT_ASSERT_EQ(sources[i].ir_request_id, 817);
		UT_ASSERT(memcmp(&sources[i].generation.source, &observation_wal_sources[i].source,
			sizeof(sources[i].generation.source)) == 0);
	}
	UT_ASSERT_EQ(page_graph_budget, 1024 * 1024 - 3 * sizeof(*sources));
	factories = page_source_factories;
	again = ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3);
	UT_ASSERT(again == sources); UT_ASSERT_EQ(page_source_factories, factories);
	UT_ASSERT_EQ(page_graph_builds, builds + 1);
	UT_ASSERT_EQ(wal_inserts + stamp_writes, mutations);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 2) == NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, (const ClusterColdCtrcWorkV1 *)&typed, 3) == NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcPageSources((const struct ClusterRecoveryStartV1 *)&info, &native_ctrc_work, 3) == NULL);
	UT_ASSERT_EQ(observation_scope_begins, 1); UT_ASSERT_EQ(observation_census_begins, 1);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT(strstr(observation_events, "PDR") != NULL);
	UT_ASSERT(ClusterColdRecoveryCtrcPageSources(NULL, &native_ctrc_work, 3) == NULL);
}

UT_TEST(test_startup_page_sources_reject_changed_full_value_and_original_cut)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		const ClusterRecoveryCtrcReplaySetSourceV1 *saved;
		ClusterRecoveryCtrcReplaySetSourceV1 *changed;
		page_sources_three_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
		saved = ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3);
		UT_ASSERT(saved != NULL); if (saved == NULL) return;
		changed = (ClusterRecoveryCtrcReplaySetSourceV1 *)saved;
		if (fault == 0) changed[0].generation.checkpoint.checkpoint_lower_lsn++;
		if (fault == 1) changed[1].replay_token.file_txn_seq++;
		if (fault == 2) changed[2].ir_request_id++;
		if (fault == 3) changed[1].generation_index = changed[0].generation_index;
		if (fault == 4) changed[2].generation_count--;
		if (fault == 5) observation_wal_sources[1].cut.scan_end_exclusive++;
		if (fault == 6) observation_wal_sources[1].source.claim.claim_sha256[31]++;
		if (fault == 7) observation_wal_sources[2].source_index = 1;
		UT_ASSERT(ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3) == NULL);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(page_graph_live); UT_ASSERT_EQ(page_graph_builds, 1);
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	}
}

static void
page_source_retire(void)
{
	page_source_current_step = page_source_step = NULL;
	cluster_recovery_cold_observation_close(cluster_recovery_cold_start);
}

UT_TEST(test_startup_page_sources_callback_retirement_preserves_pointer_lifetime)
{
	for (unsigned published = 0; published < 2; published++) {
		page_sources_three_ready();
		if (published) {
			UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
			page_source_current_step = page_source_retire;
			UT_ASSERT(ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3) == NULL);
		} else {
			page_source_step = page_source_retire;
			UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
		}
		UT_ASSERT_EQ(page_graph_builds, 1); UT_ASSERT_EQ(page_graph_destroys, 1);
		UT_ASSERT(!page_graph_live); UT_ASSERT_EQ(observation_scope_releases, 1);
		UT_ASSERT_EQ(observation_census_destroys, 1);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		ClusterReleaseColdRecoveryInputs(&info);
		UT_ASSERT_EQ(observation_scope_releases, 1); UT_ASSERT_EQ(observation_census_destroys, 1);
	}
}

UT_TEST(test_startup_page_sources_failure_before_allocation_keeps_original_debt)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		page_sources_three_ready();
		if (fault == 0) page_source_input_count = 2;
		if (fault == 1) allocation_ok = false;
		UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start,
			fault == 2 ? 3 * sizeof(ClusterRecoveryCtrcReplaySetSourceV1) : 1024 * 1024));
		UT_ASSERT_EQ(page_graph_builds, 0);
		UT_ASSERT_EQ(page_source_factories, 0);
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
		allocation_ok = true;
		ClusterReleaseColdRecoveryInputs(&info);
		UT_ASSERT_EQ(observation_scope_releases, 1); UT_ASSERT_EQ(observation_census_destroys, 1);
	}
}

UT_TEST(test_startup_page_sources_repeat_error_revokes_without_early_release)
{
	volatile bool caught = false;
	page_sources_three_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	page_source_current_error = true;
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPageSources(info.cluster_tx_start, &native_ctrc_work, 3); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	UT_ASSERT(page_graph_live);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(page_graph_destroys, 1); UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(observation_census_destroys, 1);
}
