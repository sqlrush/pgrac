/* Original Start envelope checks; physical reader/CRC is a separate fixture.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_start_page_raw_preserves_original_whole_bytes_and_pending_service)
{
	ClusterRecoveryPageWalV1 out;
	ClusterRecoveryCtrcResultValueV1 before;
	observation_two_ready(); before = window_region.checkpoint_owner.ctrc_result;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	for (uint32 i = 0; i < 2; i++) {
		uint8 digest[32];
		UT_ASSERT(ClusterColdRecoveryCtrcPageWalAt(info.cluster_tx_start, &native_ctrc_work, i, &out));
		UT_ASSERT(out.raw_record == observation_page_wal_raw[i]);
		UT_ASSERT_EQ(out.raw_length, sizeof(observation_page_wal_raw[i]));
		UT_ASSERT_EQ(out.source_index, i); UT_ASSERT_EQ(out.ordinal, i + 4);
		UT_ASSERT(out.reader == &observation_page_wal_readers[i]);
		UT_ASSERT(cluster_recovery_native_raw_sha256_v1(out.raw_record, out.raw_length, digest));
		UT_ASSERT(memcmp(digest, out.raw_record_sha256, 32) == 0);
	}
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_page_raw_missing_changed_and_wrong_end_revoke_whole_observation)
{
	for (unsigned fault = 6; fault <= 12; fault++) {
		ClusterRecoveryPageWalV1 out, zero = {0};
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_page_wal_fault = fault; memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!ClusterColdRecoveryCtrcPageWalAt(info.cluster_tx_start, &native_ctrc_work, 0, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(!observation_scope_live && !observation_cursor_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_page_wal_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
