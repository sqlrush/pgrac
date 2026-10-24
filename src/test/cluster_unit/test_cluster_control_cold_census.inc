/* Cold census over real selected ROOT/claims/anchors and physical WAL.
 * Native transport/ResourceOwner boundaries come from the containing fixture.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
typedef struct ColdCensusTest {
	uint64 records;
	uint64 source_records;
	unsigned begins;
	unsigned ends;
	unsigned empty;
	int fault;
	bool active;
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
} ColdCensusTest;

static RfPageProofDetailV1
cold_census_source(const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
				   const ClusterWalTailObservation *completed, void *arg)
{
	ColdCensusTest *work = arg;

	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT(inputs_pin_held && inputs_pin_current);
	UT_ASSERT_EQ(source->claim.identity.origin_thread_id, cut->failed_thread);
	UT_ASSERT_EQ(source->claim.identity.origin_owner_incarnation, cut->origin_owner_incarnation);
	UT_ASSERT_EQ(source->timeline, cut->timeline_id);
	UT_ASSERT((cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) != 0);
	if (completed == NULL) {
		UT_ASSERT(!work->active);
		work->active = true;
		work->begins++;
		work->source = *source;
		work->cut = *cut;
		work->source_records = 0;
		if (work->fault == 2)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	} else {
		UT_ASSERT(work->active);
		UT_ASSERT_EQ(memcmp(&work->source, source, sizeof(*source)), 0);
		UT_ASSERT_EQ(memcmp(&work->cut, cut, sizeof(*cut)), 0);
		UT_ASSERT_EQ(completed->records, work->source_records);
		if (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) {
			UT_ASSERT_EQ(completed->records, 0);
			UT_ASSERT_EQ(completed->complete_end, 0);
			UT_ASSERT_EQ(cut->scan_begin_inclusive, cut->scan_end_exclusive);
			work->empty++;
		} else {
			UT_ASSERT(completed->records > 0);
			UT_ASSERT_EQ(completed->complete_end, cut->scan_end_exclusive);
		}
		work->active = false;
		work->ends++;
		if (work->fault == 3)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		/* Drift after the last completed source must still invalidate the
		 * whole census, not just the next source's reader. */
		if (work->fault == 4 && work->ends == 2)
			v2_checkpoint_root_race();
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
cold_census_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
				   const RfContributorStreamCutV1 *cut, void *arg)
{
	ColdCensusTest *work = arg;

	UT_ASSERT(work->active);
	UT_ASSERT_EQ(test_actual_cf, NoLock);
	UT_ASSERT(inputs_pin_held && inputs_pin_current);
	UT_ASSERT_EQ(memcmp(&work->source, source, sizeof(*source)), 0);
	UT_ASSERT_EQ(memcmp(&work->cut, cut, sizeof(*cut)), 0);
	UT_ASSERT(reader->ReadRecPtr >= cut->scan_begin_inclusive);
	UT_ASSERT(reader->EndRecPtr <= cut->scan_end_exclusive);
	work->records++;
	work->source_records++;
	if (work->fault == 1)
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	if (work->fault == 5)
		inputs_pin_current = false;
	if (work->fault == 6)
		pg_re_throw();
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
cold_census_done(ClusterWalInputsV1 **inputs)
{
	cluster_wal_inputs_release_v1(inputs);
	MyBackendType = B_INVALID;
	inputs_native_recovery = false;
	test_inputs_startup_cf = false;
	inputs_pin_current = true;
}

UT_TEST(test_cold_census_visits_all_selected_sources_twice)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;

	inputs_closed_wal_fixture(bytes, anchors);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		return;
	for (unsigned pass = 0; pass < 2; pass++) {
		ColdCensusTest work = { 0 };
		uint64 records = 99;
		RfPageProofDetailV1 detail;

		UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(
						 inputs, cold_census_record, cold_census_source, &work, &records, &detail),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(records, 2);
		UT_ASSERT_EQ(work.records, 2);
		UT_ASSERT_EQ(work.begins, 2);
		UT_ASSERT_EQ(work.ends, 2);
		UT_ASSERT(!work.active);
		UT_ASSERT_EQ(work.source.claim.identity.origin_thread_id, 128);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	}
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cold_census_done(&inputs);
}

UT_TEST(test_cold_census_open_source_waits_without_partial_callbacks)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	ColdCensusTest work = { 0 };
	uint64 records = 99;
	RfPageProofDetailV1 detail;

	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(
		cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	root.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record, cold_census_source,
												   &work, &records, &detail),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(work.begins + work.records + work.ends, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cold_census_done(&inputs);
}

UT_TEST(test_cold_census_refusal_error_and_final_drift_never_complete)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		uint8 bytes[66048];
		ClusterRecoveryAnchorV2 anchors[2];
		ClusterWalInputsV1 *inputs = NULL;
		ColdCensusTest work = { 0 };
		uint64 records = 99, calls;
		RfPageProofDetailV1 detail;
		bool threw = false;

		inputs_closed_wal_fixture(bytes, anchors);
		test_inputs_startup_cf = true;
		UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
		if (inputs == NULL)
			return;
		work.fault = fault;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record,
														   cold_census_source, &work, &records,
														   &detail),
						 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 6);
		UT_ASSERT_EQ(records, 0);
		if (fault <= 3)
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
		calls = work.begins + work.records + work.ends;
		work.fault = 0;
		inputs_pin_current = true;
		UT_ASSERT_NE(cluster_wal_inputs_cold_census_v1(
						 inputs, cold_census_record, cold_census_source, &work, &records, &detail),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(work.begins + work.records + work.ends, calls);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_cold_census_keeps_original_scope_and_purpose)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ColdCensusTest work = { 0 };
	uint64 records = 99;
	RfPageProofDetailV1 detail;
	ResourceOwner owner = CurrentResourceOwner;

	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(inputs_begin_for_role(false, &inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record, cold_census_source,
												   &work, &records, &detail),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	cluster_wal_inputs_release_v1(&inputs);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		return;
	/* Online census remains unavailable even with a valid cold read scope. */
	UT_ASSERT_EQ(cluster_wal_inputs_census_v1(inputs, cold_census_record, &work, &records, &detail),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (unsigned fault = 0; fault < 4; fault++) {
		if (fault == 0)
			CurrentResourceOwner = (ResourceOwner)&work;
		if (fault == 1)
			MyBackendType = B_BG_WORKER;
		if (fault == 2)
			inputs_native_recovery = false;
		if (fault == 3)
			UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
		UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(
						 inputs, cold_census_record, cold_census_source, &work, &records, &detail),
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(records, 0);
		CurrentResourceOwner = owner;
		MyBackendType = B_STARTUP;
		inputs_native_recovery = true;
		if (fault == 3)
			UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), 0);
	}
	UT_ASSERT_EQ(work.begins + work.records + work.ends, 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	cold_census_done(&inputs);
}

UT_TEST(test_cold_census_preserves_empty_initializer_source)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalStartupImage op;
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	ColdCensusTest work = { 0 };
	RfPageProofDetailV1 detail;
	uint64 records = 99;
	uint32 selected, empty = 0;

	op = pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed)
		goto cleanup;
	test_worker_pin_held = false;
	test_history_sync_count = 0;
	test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root,
															 &control, &token),
				 0);
	/* Same read-only terminal fixture as the existing physical visitor test:
	 * remove the unrelated uncompleted peer reservation, not its predecessor
	 * or WAL. No runtime admission is inferred from this fixture edit. */
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL)
		goto cleanup;
	selected = cluster_wal_inputs_count_v1(inputs);
	for (uint32 i = 0; i < selected; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
			UT_ASSERT_EQ(item->terminal.tail.records, 0);
			UT_ASSERT(cluster_control_root_identity_equal(&item->source.claim.identity,
														  &op.claim.identity));
			empty++;
		}
	}
	UT_ASSERT_EQ(empty, 1);
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record, cold_census_source,
												   &work, &records, &detail),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(work.begins, selected);
	UT_ASSERT_EQ(work.ends, selected);
	UT_ASSERT_EQ(work.empty, empty);
	UT_ASSERT_EQ(records, work.records);
cleanup:
	cold_census_done(&inputs);
	test_reserve_mode = false;
	cluster_node_id = saved_node;
	test_self_incarnation = saved_incarnation;
}
