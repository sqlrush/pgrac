/* Original Start/work, with only the lower census carrier controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_start_compound_groups_retain_complete_member_input_and_end)
{
	ClusterRecoveryNativeRecordObservationV1 out;
	start_native_records_ready(2);
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 1);
	UT_ASSERT(out.observed && out.effects == start_native_components[1]);
	UT_ASSERT_EQ(out.effect_count, 3);
	if (out.effects != NULL && out.effect_count == 3) {
		UT_ASSERT_EQ(out.effects[1].kind, CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER);
		UT_ASSERT_EQ(out.effects[2].ordinal, 1);
		UT_ASSERT_EQ(*(const char *)out.effects[2].bytes, start_native_payloads[1][4]);
	}
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 2);
	UT_ASSERT(out.observed && out.effects == NULL && out.effect_count == 0);
	UT_ASSERT_EQ(observation_census_destroys, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_compound_groups_refuse_missing_mixed_or_nonempty_end)
{
	for (unsigned fault = 21; fault <= 26; fault++) {
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		start_native_records_ready(2); start_native_record_fault = fault;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work,
			fault == 26 ? 2 : 1);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		start_native_record_fault = 0;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
