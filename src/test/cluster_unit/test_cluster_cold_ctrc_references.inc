/* Original Start must retain every original census reference, not a prefix.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_ctrc_references_missing_view_refuses_before_observation_publication)
{
	observation_two_ready(); observation_reference_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(observation_reference_calls, 1);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	observation_reference_fault = 0;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_references_cannot_rebind_same_start_to_another_complete_view)
{
	observation_two_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(observation_reference_calls > 0);
	observation_reference_fault = 2;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	observation_reference_fault = 0;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_references_error_uses_original_failure_and_reverse_cleanup)
{
	volatile bool caught = false;
	observation_two_ready(); observation_reference_fault = 3;
	PG_TRY();
	{
		(void)ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	observation_reference_fault = 0;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_references_after_getter_owner_drift_refuses_without_new_scope)
{
	observation_two_ready(); observation_reference_fault = 4;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(observation_reference_calls, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	observation_reference_fault = 0; owner_boot--;
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	ClusterReleaseColdRecoveryInputs(&info);
}
