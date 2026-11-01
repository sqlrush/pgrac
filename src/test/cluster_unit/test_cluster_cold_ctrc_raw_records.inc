/* Original Start boundary; the lower census carrier is controlled here.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_start_raw_records_keep_complete_original_bytes_and_empty_end)
{
	ClusterRecoveryNativeRecordObservationV1 out;
	start_native_records_ready(2);
	for (uint64 i = 0; i < 2; i++) {
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, i);
		UT_ASSERT(out.observed && out.raw_record == start_native_raw[i]);
		UT_ASSERT_EQ(out.raw_length, sizeof(start_native_raw[i]));
		UT_ASSERT(memcmp(out.raw_record, &start_native_records[i].header, SizeOfXLogRecord) == 0);
		UT_ASSERT(memcmp(out.raw_record + SizeOfXLogRecord + 2, start_native_payloads[i], 8) == 0);
	}
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 2);
	UT_ASSERT(out.observed && out.raw_record == NULL && out.raw_length == 0);
	UT_ASSERT_EQ(observation_scope_begins, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_raw_records_refuse_incomplete_changed_bytes_hash_header_and_end)
{
	for (unsigned fault = 41; fault <= 48; fault++) {
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		start_native_records_ready(2); start_native_record_fault = fault;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work,
			fault == 48 ? 2 : 1);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		start_native_record_fault = 0;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
