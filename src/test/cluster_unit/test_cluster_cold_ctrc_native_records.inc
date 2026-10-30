/* Same original Start/census transport; native bytes/files are covered by
 * test_cluster_recovery_census_native_records.inc.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned start_native_record_fault, start_native_record_calls;
static uint16 start_native_owner_families[2][3];
static uint64 start_native_record_count;
static ClusterRecoveryNativeRecord start_native_records[2];
static char start_native_payloads[2][8];
static ClusterRecoveryNativeEffect start_native_components[2][3];

ClusterRecoveryNativeRecordObservationV1
cluster_recovery_census_native_record_v1(ClusterRecoveryCensus *cursor, uint64 index)
{
	ClusterRecoveryNativeRecordObservationV1 out = {0};
	if (cursor != (ClusterRecoveryCensus *)&observation_cursor_object
		|| !observation_cursor_live || !observation_scope_live || observation_steps < 3) abort();
	start_native_record_calls++;
	if (start_native_record_fault == 1) return out;
	if (start_native_record_fault == 2) pg_re_throw();
	out.observed = true; out.index = index; out.count = start_native_record_count;
	out.source_count = observation_wal_source_count;
	if (index < out.count) {
		out.record_ordinal = index + 1;
		out.primary.kind = index == 0 ? CLUSTER_RECOVERY_NATIVE_CLOG_ZERO
			: CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE;
		out.primary.record = &start_native_records[index];
		out.primary.bytes = start_native_payloads[index]; out.primary.length = 8;
		out.effects = start_native_components[index]; out.effect_count = index == 0 ? 1 : 3;
		out.owner_families = start_native_owner_families[index];
	}
	if (start_native_record_fault == 3) out.count = observation_manifest.native_effect_count + 1;
	if (start_native_record_fault == 4) out.source_index = out.source_count;
	if (start_native_record_fault == 5) out.primary.record = NULL;
	if (start_native_record_fault == 6) owner_boot++;
	if (start_native_record_fault == 7) observation_manifest.inputs_sha256[31]++;
	if (start_native_record_fault == 8) start_native_records[0].cut.scan_end_exclusive++;
	if (start_native_record_fault == 9) out.primary.record = &start_native_records[0];
	if (start_native_record_fault == 10) out.source_count++;
	if (start_native_record_fault == 11) out.index++;
	if (start_native_record_fault == 21) out.effect_count = 0;
	if (start_native_record_fault == 22) out.effects = NULL;
	if (start_native_record_fault == 23) out.effect_count = SIZE_MAX;
	if (start_native_record_fault == 24) start_native_components[1][2].record = &start_native_records[0];
	if (start_native_record_fault == 25) start_native_components[1][0].kind = 0;
	if (start_native_record_fault == 26) { out.effects = start_native_components[0]; out.effect_count = 1; }
	if (start_native_record_fault == 31) out.owner_families = NULL;
	if (start_native_record_fault == 32) start_native_owner_families[1][0] = 0;
	if (start_native_record_fault == 33) start_native_owner_families[1][0] = 0x8000;
	if (start_native_record_fault == 34) start_native_owner_families[1][0] = 8;
	if (start_native_record_fault == 35) start_native_owner_families[1][0] = 0x3fff;
	if (start_native_record_fault == 36) out.owner_families = start_native_owner_families[0];
	return out;
}

static void
start_native_records_ready(uint64 count)
{
	start_tx_records_ready(0);
	start_native_record_fault = start_native_record_calls = 0;
	start_native_record_count = count;
	memset(start_native_records, 0, sizeof(start_native_records));
	memset(start_native_payloads, 0, sizeof(start_native_payloads));
	memset(start_native_components, 0, sizeof(start_native_components));
	for (unsigned i = 0; i < lengthof(start_native_records); i++) {
		start_native_records[i].source = observation_wal_sources[0].source;
		start_native_records[i].cut = observation_wal_sources[0].cut;
		start_native_records[i].header.xl_rmid = i == 0 ? RM_CLOG_ID : RM_MULTIXACT_ID;
		start_native_records[i].payload = start_native_payloads[i];
		start_native_records[i].payload_length = sizeof(start_native_payloads[i]);
		start_native_payloads[i][7] = 0x73 + i;
		start_native_owner_families[i][0] = i == 0 ? 1 : 24;
		start_native_owner_families[i][1] = start_native_owner_families[i][2] = 16;
		start_native_components[i][0] = (ClusterRecoveryNativeEffect){
			i == 0 ? CLUSTER_RECOVERY_NATIVE_CLOG_ZERO : CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE,
			0, &start_native_records[i], start_native_payloads[i], 8 };
		for (unsigned j = 1; j < 3; j++)
			start_native_components[i][j] = (ClusterRecoveryNativeEffect){
				CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER, j - 1, &start_native_records[i],
				start_native_payloads[i] + (j - 1) * 4, 4 };
	}
}

UT_TEST(test_start_native_records_complete_unfiltered_input_and_empty_end)
{
	for (uint64 count = 0; count <= 2; count += 2) {
		start_native_records_ready(count);
		for (uint64 i = 0; i <= count; i++) {
			ClusterRecoveryNativeRecordObservationV1 out = ClusterColdRecoveryCtrcNativeRecord(
				info.cluster_tx_start, &native_ctrc_work, i);
			UT_ASSERT(out.observed); UT_ASSERT_EQ(out.index, i); UT_ASSERT_EQ(out.count, count);
			UT_ASSERT_EQ(out.source_count, 1);
			if (i == count) {
				UT_ASSERT(out.primary.record == NULL && out.primary.bytes == NULL);
				UT_ASSERT_EQ(out.primary.kind, 0); UT_ASSERT_EQ(out.record_ordinal, 0);
			} else {
				UT_ASSERT(out.primary.record == &start_native_records[i]);
				UT_ASSERT_EQ(out.primary.record->header.xl_xid, 0);
				UT_ASSERT_EQ(out.primary.record->payload[7], 0x73 + i);
				UT_ASSERT_EQ(out.record_ordinal, i + 1);
			}
		}
		UT_ASSERT_EQ(observation_scope_begins, 1); UT_ASSERT_EQ(observation_census_begins, 1);
		UT_ASSERT_EQ(observation_census_destroys, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_native_records_preconditions_do_not_enter_or_replace_owner)
{
	ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
	ClusterColdCtrcWorkV1 other;
	start_native_records_ready(2); other = native_ctrc_work;
	out = ClusterColdRecoveryCtrcNativeRecord(NULL, &native_ctrc_work, 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &other, 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	for (unsigned mode = 0; mode < 3; mode++) {
		if (mode == 0) CritSectionCount = 1;
		if (mode == 1) ShutdownRequestPending = true;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work,
			mode == 2 ? UINT64_MAX : 0);
		CritSectionCount = 0; ShutdownRequestPending = false;
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	}
	UT_ASSERT_EQ(start_native_record_calls, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT(out.observed); ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_native_records_late_shape_source_owner_and_error_revoke_whole_view)
{
	for (unsigned fault = 1; fault <= 11; fault++) {
		ClusterRecoveryNativeRecordObservationV1 out = {0}, zero = {0};
		volatile bool caught = false;
		start_native_records_ready(2); start_native_record_fault = fault;
		PG_TRY(); {
			out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work,
				fault == 9 ? 2 : 0);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught == (fault == 2)); UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(start_native_record_calls, 1);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(strstr(observation_events, "DR") != NULL);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		start_native_record_fault = 0;
		out = ClusterColdRecoveryCtrcNativeRecord(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(start_native_record_calls, 1);
		if (fault == 6) {
			owner_boot--; release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		ClusterReleaseColdRecoveryInputs(&info);
	}
}
