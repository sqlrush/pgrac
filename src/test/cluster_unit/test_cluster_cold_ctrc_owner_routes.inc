/* Original Start gate with the lower census carrier controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_start_owner_routes_keep_distinct_mx_duties_without_accepting_completion)
{
	ClusterRecoveryNativeRecordObservationV1 out;
	start_native_records_ready(2);
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 1);
	UT_ASSERT(out.observed && out.owner_families != NULL);
	if (out.owner_families != NULL) {
		UT_ASSERT_EQ(out.owner_families[0], 24); /* offsets + members */
		UT_ASSERT_EQ(out.owner_families[1], 16);
		UT_ASSERT_EQ(out.owner_families[2], 16);
	}
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 2);
	UT_ASSERT(out.observed && out.owner_families == NULL);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_owner_routes_refuse_missing_unknown_partial_extra_and_nonempty_end)
{
	for (unsigned fault = 31; fault <= 36; fault++) {
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		start_native_records_ready(2); start_native_record_fault = fault;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work,
			fault == 36 ? 2 : 1);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		start_native_record_fault = 0;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
