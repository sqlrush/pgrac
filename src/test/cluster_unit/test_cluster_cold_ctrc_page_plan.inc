/* Original Startup graph preparation. Actual Start/boundary; graph factory
 * and native holder scheduling are leaves, covered by native-input tests.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned page_graph_builds, page_graph_destroys;
static unsigned page_graph_token;
static Size page_graph_budget;
static bool page_graph_live, page_graph_error, page_graph_refuse;
static void (*page_graph_step)(void);

static void
page_graph_fixture_reset(void)
{
	if (page_graph_live) abort();
	page_graph_builds = page_graph_destroys = 0; page_graph_budget = 0;
	page_graph_error = page_graph_refuse = false; page_graph_step = NULL;
}

ClusterRecoveryPagePlanBuildV1
cluster_recovery_census_page_plan_v1(ClusterRecoveryCensus *cursor, Size budget)
{
	UT_ASSERT(cursor == (ClusterRecoveryCensus *)&observation_cursor_object);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT(ctrc_driver_authorizations >= 2);
	UT_ASSERT(budget > 0 && budget <= RF_PAGE_ONLINE_PLAN_MAX_BYTES);
	UT_ASSERT(!page_graph_live);
	page_graph_builds++; page_graph_budget = budget;
	if (page_graph_error) pg_re_throw();
	if (page_graph_refuse)
		return (ClusterRecoveryPagePlanBuildV1){NULL, RF_PAGE_PROOF_DETAIL_EDGE_GAP};
	page_graph_live = true;
	if (page_graph_step != NULL) page_graph_step();
	return (ClusterRecoveryPagePlanBuildV1){(RfPageOnlinePlanV1 *)&page_graph_token,
		RF_PAGE_PROOF_DETAIL_OK};
}

void
rf_page_online_plan_destroy_v1(RfPageOnlinePlanV1 **plan)
{
	if (plan == NULL || *plan == NULL) return;
	UT_ASSERT(*plan == (RfPageOnlinePlanV1 *)&page_graph_token && page_graph_live);
	page_graph_destroys++; page_graph_live = false; *plan = NULL;
	observation_event('P');
}

static void
page_graph_service_wait(void)
{
	UT_ASSERT(page_graph_live); UT_ASSERT_EQ(page_graph_builds, 1);
	ctrc_driver_service_wait();
}

UT_TEST(test_actual_startup_prepares_original_page_graph_before_service)
{
	ctrc_handoff_fixture(false, false);
	ctrc_wait_step = page_graph_service_wait;
	actual_startup_finish_tx(&info);
	UT_ASSERT_EQ(page_graph_builds, 1); UT_ASSERT_EQ(page_graph_destroys, 1);
	UT_ASSERT(strstr(observation_events, "PDR") != NULL);
}

UT_TEST(test_startup_page_graph_is_original_bounded_and_never_rebuilt)
{
	const RfPageOnlinePlanV1 *first;
	unsigned mutations;
	ctrc_replay_request_fixture();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	mutations = wal_inserts + stamp_writes;
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	first = ClusterColdRecoveryCtrcPagePlan(info.cluster_tx_start, &native_ctrc_work);
	UT_ASSERT(first == (RfPageOnlinePlanV1 *)&page_graph_token);
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 0));
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlan(info.cluster_tx_start, &native_ctrc_work) == first);
	UT_ASSERT_EQ(page_graph_builds, 1); UT_ASSERT_EQ(page_graph_budget, 1024 * 1024);
	UT_ASSERT_EQ(wal_inserts + stamp_writes, mutations);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT_EQ(observation_scope_begins, 1); UT_ASSERT_EQ(observation_census_begins, 1);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(page_graph_destroys, 1); UT_ASSERT(strstr(observation_events, "PDR") != NULL);
}

UT_TEST(test_startup_page_graph_preconditions_leave_original_custody)
{
	reset(); UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(NULL, 0));
	for (unsigned fault = 0; fault < 8; fault++) {
		const struct ClusterRecoveryStartV1 *start;
		unsigned manifests;
		ctrc_replay_request_fixture(); start = info.cluster_tx_start;
		manifests = observation_manifests;
		if (fault == 0) start = (const struct ClusterRecoveryStartV1 *)&info;
		if (fault == 1) ctrc_driver_lwlocks = 200;
		if (fault == 2) native_recovery = true;
		if (fault == 3) CritSectionCount++;
		if (fault == 4) ShutdownRequestPending = true;
		if (fault == 5) MyBackendType = B_BACKEND;
		if (fault == 6) cluster_recovery_cold_start->ctrc_observation_complete = false;
		if (fault == 7) ctrc_driver_false_authorization = true;
		UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(start, 1024 * 1024));
		UT_ASSERT_EQ(page_graph_builds, 0); UT_ASSERT_EQ(page_graph_destroys, 0);
		if (fault < 7) UT_ASSERT_EQ(observation_manifests, manifests);
		UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
		CritSectionCount = 0; ctrc_driver_lwlocks = 0;
	}
}

static void page_graph_change_owner(void) { owner_boot++; }
static void page_graph_close_observation(void) { cluster_recovery_cold_observation_close(cluster_recovery_cold_start); }
static void page_graph_change_manifest(void) { observation_fault = 6; }

UT_TEST(test_startup_page_graph_late_refusal_discards_only_private_plan)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ctrc_replay_request_fixture();
		UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
		if (fault == 0) page_graph_refuse = true;
		if (fault == 1) page_graph_step = page_graph_change_owner;
		if (fault == 2) page_graph_step = page_graph_close_observation;
		if (fault == 3) page_graph_step = page_graph_change_manifest;
		UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
		UT_ASSERT(!page_graph_live); UT_ASSERT_EQ(page_graph_builds, 1);
		UT_ASSERT_EQ(page_graph_destroys, fault != 0);
		UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
		UT_ASSERT(!ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
		UT_ASSERT_EQ(page_graph_builds, 1);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		if (fault < 2) UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	}
}

UT_TEST(test_startup_page_graph_error_revokes_episode_and_preserves_cleanup)
{
	volatile bool caught = false;
	ctrc_replay_request_fixture();
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	page_graph_error = true;
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	UT_ASSERT(!page_graph_live);
	ClusterReleaseColdRecoveryInputs(&info);
	UT_ASSERT_EQ(observation_scope_releases, 1); UT_ASSERT_EQ(observation_census_destroys, 1);
}
