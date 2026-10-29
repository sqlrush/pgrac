/* Original Startup's TX-forest owner boundary; native forest/files separately
 * exercised by test_cluster_recovery_census_tx_records.inc.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned start_tx_record_fault, start_tx_record_calls;
static ClusterRecoveryUndoRecord start_tx_record_bytes;

ClusterRecoveryCensusTxRecordV1
cluster_recovery_census_tx_record_v1(ClusterRecoveryCensus *cursor, uint32 index)
{
	ClusterRecoveryCensusTxRecordV1 out = {0};
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || observation_steps < 3) abort();
	start_tx_record_calls++;
	if (start_tx_record_fault == 1) return out;
	if (start_tx_record_fault == 2) pg_re_throw();
	out.observed = true; out.count = observation_manifest.tx_record_count;
	if (start_tx_record_fault == 3) out.count++;
	if (index < out.count) {
		out.value.transaction.xid = 4195136;
		out.value.transaction.segment_generation = 17;
		out.value.record_segment_generation = 41;
		out.value.record = &start_tx_record_bytes;
	}
	if (start_tx_record_fault == 4) owner_boot++;
	if (start_tx_record_fault == 5) out.value.record = NULL;
	if (start_tx_record_fault == 6) observation_manifest.inputs_sha256[31]++;
	return out;
}

static void
start_tx_records_ready(uint32 count)
{
	observation_two_ready();
	start_tx_record_fault = start_tx_record_calls = 0;
	memset(&start_tx_record_bytes, 0, sizeof(start_tx_record_bytes));
	start_tx_record_bytes.body_length = 7;
	start_tx_record_bytes.payload[6] = 0x73;
	observation_manifest.tx_record_count = count;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
}

UT_TEST(test_start_tx_records_keep_same_full_original_owner_and_empty_end)
{
	for (uint32 count = 0; count <= 3; count += 3) {
		ClusterRecoveryCensusTxRecordV1 out;
		start_tx_records_ready(count);
		for (uint32 i = 0; i <= count; i++) {
			out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, i);
			UT_ASSERT(out.observed); UT_ASSERT_EQ(out.count, count);
			if (i == count) UT_ASSERT(out.value.record == NULL);
			else {
				UT_ASSERT(out.value.record == &start_tx_record_bytes);
				UT_ASSERT_EQ(out.value.transaction.segment_generation, 17);
				UT_ASSERT_EQ(out.value.record_segment_generation, 41);
				UT_ASSERT_EQ(out.value.record->payload[6], 0x73);
			}
		}
		UT_ASSERT_EQ(observation_scope_begins, 1);
		UT_ASSERT_EQ(observation_census_begins, 1);
		UT_ASSERT_EQ(observation_census_destroys, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_tx_records_preconditions_do_not_enter_or_replace_work)
{
	ClusterRecoveryCensusTxRecordV1 out, zero = {0};
	ClusterColdCtrcWorkV1 other;
	start_tx_records_ready(3); other = native_ctrc_work;
	out = ClusterColdRecoveryCtrcTxRecord(NULL, &native_ctrc_work, 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &other, 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	CritSectionCount = 1;
	out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 0);
	CritSectionCount = 0;
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 4);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT_EQ(start_tx_record_calls, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT(out.observed);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_tx_records_late_refusal_count_and_owner_revoke_whole_start)
{
	const unsigned faults[] = {1,3,4,5,6};
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterRecoveryCensusTxRecordV1 out, zero = {0};
		start_tx_records_ready(3); start_tx_record_fault = faults[i];
		out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(start_tx_record_calls, 1);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		start_tx_record_fault = 0;
		out = ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(start_tx_record_calls, 1);
		if (faults[i] == 4) {
			/* A stale incarnation cannot use the normal checkpoint handoff. */
			owner_boot--;
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_tx_records_error_cleans_original_observation_in_order)
{
	volatile bool caught = false;
	start_tx_records_ready(3); start_tx_record_fault = 2;
	PG_TRY(); {
		(void)ClusterColdRecoveryCtrcTxRecord(info.cluster_tx_start, &native_ctrc_work, 0);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	UT_ASSERT(strstr(observation_events, "DR") != NULL);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	start_tx_record_fault = 0;
	ClusterReleaseColdRecoveryInputs(&info);
}
