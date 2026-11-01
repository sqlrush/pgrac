/* Same original Start borrows the complete census image collection. Physical
 * page/history production is exercised by the native-input and undo suites. */
UT_TEST(test_start_images_borrow_full_original_versions_and_complete_empty_view)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		const ClusterRecoveryCensusImagesV1 *view;
		ClusterRecoveryCensusImagesV1 *expected;
		observation_two_ready();
		expected = (ClusterRecoveryCensusImagesV1 *)observation_images_storage[0].bytes;
		expected->count = empty ? 0 : 3;
		for (uint32 i = 0; i < expected->count; i++) {
			expected->entries[i].page_index = i == 2 ? 1 : 0;
			expected->entries[i].version = i == 1 ? 1 : 0;
			memset(expected->entries[i].bytes.data, 0x31+i, BLCKSZ);
		}
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		view = ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work);
		UT_ASSERT(view == expected);
		if (view != NULL) {
			UT_ASSERT_EQ(view->count, empty ? 0 : 3);
			for (uint32 i = 0; i < view->count; i++)
				UT_ASSERT(memcmp(view->entries[i].bytes.data, expected->entries[i].bytes.data, BLCKSZ) == 0);
		}
		UT_ASSERT_EQ(observation_image_begins, 1);
		UT_ASSERT_EQ(observation_census_begins, 1);
		UT_ASSERT_EQ(observation_scope_begins, 1);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		ClusterReleaseColdRecoveryInputs(&info);
		UT_ASSERT(strstr(observation_events, "DR") != NULL);
	}
}

UT_TEST(test_start_images_preconditions_never_enter_or_start_a_replacement)
{
	ClusterColdCtrcWorkV1 other;
	unsigned calls;
	observation_two_ready(); other = native_ctrc_work;
	UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
	UT_ASSERT_EQ(observation_scope_begins, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	calls = observation_image_calls;
	UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(NULL, &native_ctrc_work));
	UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &other));
	CritSectionCount = 1;
	UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
	CritSectionCount = 0; ShutdownRequestPending = true;
	UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
	ShutdownRequestPending = false;
	UT_ASSERT_EQ(observation_image_calls, calls);
	UT_ASSERT_NOT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
	UT_ASSERT_EQ(observation_image_begins, 1);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_images_missing_or_rebound_collection_revokes_same_observation)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		observation_two_ready();
		if (fault == 2)
			UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_image_fault = fault;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_image_fault = 0;
		UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
		UT_ASSERT_EQ(observation_image_begins, 1);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_images_error_or_late_owner_drift_preserve_original_cleanup)
{
	for (unsigned fault = 3; fault <= 4; fault++) {
		volatile bool caught = false;
		observation_two_ready(); observation_image_fault = fault;
		PG_TRY(); {
			UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught == (fault == 3));
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_image_fault = 0;
		if (fault == 4) {
			owner_boot--;
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		UT_ASSERT_NULL(ClusterColdRecoveryCtrcImages(info.cluster_tx_start, &native_ctrc_work));
		UT_ASSERT_EQ(observation_image_begins, 1);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
