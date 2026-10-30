/* Original Startup handoff after an independently yielding service wait.
 * PAGE I/O and the private shared component publisher are controlled leaves;
 * this fixture does not produce real relmap+CTRC joint completion authority.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned post_service_path, post_service_fault;
static bool post_service_polled;

static void
post_service_publish(void)
{
	ClusterRecoveryCheckpointSlotV1 *slot = &window_region.checkpoint_owner;

	UT_ASSERT_EQ(tx_waits, 1);
	UT_ASSERT_EQ(final_local_empty_calls, 1);
	UT_ASSERT_EQ(claim_arbitrations, 0);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	post_service_polled = true;
	/* Private component publication only: real joint completion remains
	 * outside this controlled freshness-boundary fixture. Never a product API. */
	slot->relmap_required = false;
	slot->ctrc_result.state = CLUSTER_RECOVERY_CTRC_RESULT_COMPLETE;
	slot->ctrc_result.released_count = slot->ctrc_result.subject_count;
	slot->ctrc_result.unresolved_count = slot->ctrc_result.unqualified_count = 0;
	slot->ctrc_accepted_generation = slot->ctrc_result.result_generation;
	switch (post_service_fault) {
	case 1:
		if (post_service_path == 1) unchanged_readback_mode = 1;
		else completed_readback_mode = 1;
		break;
	case 2:
		if (post_service_path == 1) unchanged_readback_mode = 2;
		else completed_readback_mode = 3;
		break;
	case 3: untouched_fault = 1; untouched_at = untouched_calls + 1; break;
	case 4: untouched_fault = 2; untouched_at = untouched_calls + 1; break;
	case 5: untouched_fault = 4; untouched_at = untouched_calls + 1; break;
	case 6: startup_first_floor.dirty = 1; break;
	case 7: final_local_empty_mode = 2; break;
	case 8: final_local_empty_mode = 3; break;
	case 9: owner_boot++; break;
	case 10: ShutdownRequestPending = true; break;
	case 11: final_local_empty_mode = 1; break;
	}
}

static void
post_service_ready(unsigned path, unsigned fault)
{
	final_local_empty_ready(path);
	post_service_path = path; post_service_fault = fault; post_service_polled = false;
	untouched_probe = true; untouched_calls = untouched_fault = 0;
	untouched_at = 1; untouched_step = NULL;
	ctrc_wait_step = post_service_publish;
}

static void
post_service_done(void)
{
	untouched_probe = false; untouched_step = NULL;
	if (info.cluster_tx_start != NULL) retirement_local_cleanup();
}

static void
post_service_retained(void)
{
	UT_ASSERT(post_service_polled);
	UT_ASSERT_EQ(claim_arbitrations, 0); UT_ASSERT_EQ(claim_releases, 0);
	UT_ASSERT_EQ(input_destroys, tx_wait_destroy_base);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	if (post_service_path != 2) UT_ASSERT(pending_storage_mask == 3);
	if (info.cluster_tx_start != NULL && post_service_fault < 9)
		UT_ASSERT(info.cluster_tx_start->revoked);
	post_service_done();
}

UT_TEST(test_startup_post_service_rechecks_fpi_unchanged_and_zero_page)
{
	for (unsigned path = 0; path < 3; path++) {
		post_service_ready(path, 0); retirement_run_startup();
		UT_ASSERT(post_service_polled);
		UT_ASSERT_EQ(final_local_empty_calls, 2); UT_ASSERT_EQ(startup_first_calls, 4);
		if (path == 0) UT_ASSERT_EQ(completed_readback_calls, 4);
		if (path == 1) UT_ASSERT_EQ(unchanged_readback_calls, 4);
		if (path == 2) UT_ASSERT_EQ(untouched_calls, 4);
		UT_ASSERT_EQ(prefix_steps, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_created, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(pending_storage_closes, path == 2 ? 0 : 2);
		UT_ASSERT_EQ(claim_arbitrations, 1); UT_ASSERT_EQ(claim_releases, 1);
		UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
		post_service_done();
	}
}

UT_TEST(test_startup_post_service_same_fd_readback_drift_and_error_keep_debt)
{
	for (unsigned path = 0; path < 2; path++)
		for (unsigned fault = 1; fault <= 2; fault++) {
			post_service_ready(path, fault); retirement_run_startup();
			post_service_retained();
			UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
			UT_ASSERT_EQ(pending_storage_closes, 2);
		}
}

UT_TEST(test_startup_post_service_rechecks_untouched_census_before_handoff)
{
	for (unsigned fault = 3; fault <= 5; fault++) {
		post_service_ready(2, fault); retirement_run_startup();
		post_service_retained(); UT_ASSERT_EQ(untouched_calls, 3);
		UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(pending_storage_created, 0);
	}
}

UT_TEST(test_startup_post_service_first_directory_owner_and_cancel_keep_debt)
{
	for (unsigned fault = 6; fault <= 10; fault++) {
		post_service_ready(0, fault); retirement_run_startup();
		post_service_retained();
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
	}
}

UT_TEST(test_startup_post_service_local_wait_reuses_exact_results_and_fds)
{
	post_service_ready(0, 11); retirement_run_startup();
	UT_ASSERT(post_service_polled); UT_ASSERT_EQ(tx_waits, 1);
	UT_ASSERT_EQ(ctrc_driver_waits, 1); UT_ASSERT_EQ(final_local_empty_calls, 3);
	UT_ASSERT_EQ(completed_readback_calls, 6); UT_ASSERT_EQ(startup_first_calls, 6);
	UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
	UT_ASSERT_EQ(pending_storage_closes, 2); UT_ASSERT_EQ(claim_releases, 1);
	UT_ASSERT(info.cluster_tx_start == NULL && info.cluster_cold_inputs == NULL);
	post_service_done();
}
