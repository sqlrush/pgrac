/* Online source census over selected physical WAL and original fixed cuts.
 * Transport and ResourceOwner boundaries use the containing fixture.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
UT_TEST(test_online_source_census_reuses_complete_physical_cuts)
{
	for (int open_node = -1; open_node <= 1; open_node++) {
		ClusterWalInputsV1 *inputs = inputs_contribution_fixture(open_node);
		ColdCensusTest before = { 0 }, after = { 0 };
		RfPageProofDetailV1 detail;
		uint64 records = 99, plain = 0;
		unsigned begins, flushes, recheck_begins;

		if (inputs == NULL)
			return;
		if (open_node == 1)
			inputs_peer_confirm(cluster_wal_inputs_at_v1(inputs, 1));
		UT_ASSERT_EQ(cluster_wal_inputs_census_v1(inputs, inputs_census_record, &plain,
												  &records, &detail), 0);
		begins = inputs_peer_begins + inputs_native_begin_calls;
		plain = 0;
		UT_ASSERT_EQ(cluster_wal_inputs_census_v1(inputs, inputs_census_record, &plain,
												  &records, &detail), 0);
		recheck_begins = inputs_peer_begins + inputs_native_begin_calls - begins;
		begins = inputs_peer_begins + inputs_native_begin_calls;
		flushes = inputs_native_flush_calls;
		UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
				cold_census_source, &before, &records, &detail), 0);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(records, plain);
		UT_ASSERT_EQ(records, 2);
		UT_ASSERT_EQ(before.records, records);
		UT_ASSERT_EQ(before.begins, cluster_wal_inputs_count_v1(inputs));
		UT_ASSERT_EQ(before.ends, before.begins);
		UT_ASSERT(!before.active);
		UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
				cold_census_source, &after, &records, &detail), 0);
		UT_ASSERT_EQ(after.records, before.records);
		UT_ASSERT_EQ(after.begins, before.begins);
		UT_ASSERT_EQ(after.ends, before.ends);
		UT_ASSERT_EQ(memcmp(&after.source, &before.source, sizeof(before.source)), 0);
		UT_ASSERT_EQ(memcmp(&after.cut, &before.cut, sizeof(before.cut)), 0);
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls, begins + 2 * recheck_begins);
		UT_ASSERT_EQ(inputs_native_flush_calls, flushes);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
		inputs_live_done(&inputs);
	}
}

UT_TEST(test_online_source_census_pending_has_no_partial_callbacks)
{
	ClusterWalInputsV1 *inputs = inputs_contribution_fixture(1);
	ColdCensusTest work = { 0 };
	RfPageProofDetailV1 detail;
	uint64 records = 99;

	if (inputs == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(work.records + work.begins + work.ends, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT(inputs_pin_held);
	inputs_peer_confirm(cluster_wal_inputs_at_v1(inputs, 1));
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &work, &records, &detail), 0);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(work.begins, 2);
	UT_ASSERT_EQ(work.ends, 2);
	UT_ASSERT_EQ(inputs_peer_begins, 1);
	inputs_live_done(&inputs);
	UT_ASSERT_EQ(inputs_peer_releases, 1);
}

UT_TEST(test_online_source_census_refusal_error_and_final_drift_revoke_scope)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterWalInputsV1 *inputs = inputs_contribution_fixture(1);
		struct OnlineCensusFailure {
			ColdCensusTest observation;
			uint64 records;
			RfPageProofDetailV1 detail;
		} *state = palloc0(sizeof(*state));
		ColdCensusTest *work = &state->observation;
		uint64 calls;
		volatile bool threw = false;

		if (inputs == NULL) {
			pfree(state);
			return;
		}
		inputs_peer_confirm(cluster_wal_inputs_at_v1(inputs, 1));
		state->records = 99;
		work->fault = fault;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
					cold_census_source, work, &state->records, &state->detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 6);
		UT_ASSERT_EQ(state->records, 0);
		if (fault <= 3)
			UT_ASSERT_EQ(state->detail, RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		if (fault == 4) {
			UT_ASSERT_EQ(work->records, 2);
			UT_ASSERT_EQ(work->ends, 2);
		}
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
		UT_ASSERT(inputs_pin_held);
		calls = work->begins + work->records + work->ends;
		work->fault = 0;
		inputs_pin_current = true;
		UT_ASSERT_NE(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
				cold_census_source, work, &state->records, &state->detail), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(work->begins + work->records + work->ends, calls);
		inputs_live_done(&inputs);
		UT_ASSERT(!inputs_pin_held);
		pfree(state);
	}
}

UT_TEST(test_online_source_census_keeps_original_owner_role_and_cold_rejection)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ColdCensusTest work = { 0 };
	ResourceOwner owner = CurrentResourceOwner;
	RfPageProofDetailV1 detail;
	uint64 records = 99;

	inputs_closed_wal_fixture(bytes, anchors);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	cold_census_done(&inputs);
	inputs = inputs_contribution_fixture(1);
	if (inputs == NULL)
		return;
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, cold_census_record,
			cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, NULL,
			cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			NULL, &work, &records, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &work, NULL, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &work, &records, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (unsigned fault = 0; fault < 5; fault++) {
		if (fault == 0)
			CurrentResourceOwner = (ResourceOwner)&work;
		if (fault == 1)
			MyBackendType = B_STARTUP;
		if (fault == 2)
			ShutdownRequestPending = true;
		if (fault == 3)
			CritSectionCount++;
		if (fault == 4)
			UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
		UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
				cold_census_source, &work, &records, &detail), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(records, 0);
		CurrentResourceOwner = owner;
		MyBackendType = B_BG_WORKER;
		ShutdownRequestPending = false;
		if (fault == 3)
			CritSectionCount--;
		if (fault == 4)
			UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), 0);
	}
	UT_ASSERT_EQ(work.records + work.begins + work.ends, 0);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	inputs_live_done(&inputs);
}

UT_TEST(test_online_source_census_preserves_empty_initializer_source)
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
	UT_ASSERT_EQ(inputs_begin_for_role(false, &inputs), 0);
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
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record, cold_census_source,
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

static XLogRecPtr
online_census_append_noop(const ClusterWalInputV1 *source, XLogRecPtr at)
{
	XLogRecord record = { 0 };
	uint8 bytes[MAXALIGN(SizeOfXLogRecord)] = { 0 };
	char path[MAXPGPATH];
	int fd;

	record.xl_tot_len = SizeOfXLogRecord;
	record.xl_prev = source->checkpoint.tail_last_record_lsn;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = XLOG_NOOP;
	record.xl_scn = 5907;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	UT_ASSERT(at % XLOG_BLCKSZ + sizeof(bytes) < XLOG_BLCKSZ);
	v2_checkpoint_wal_path(&source->source.claim.identity, at,
		source->source.timeline, path);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return InvalidXLogRecPtr;
	UT_ASSERT_EQ(pwrite(fd, bytes, sizeof(bytes), at % wal_segment_size), sizeof(bytes));
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
	return at + sizeof(bytes);
}

UT_TEST(test_online_source_census_physical_growth_keeps_original_cut)
{
	ClusterWalInputsV1 *inputs = inputs_contribution_fixture(1);
	const ClusterWalInputV1 *peer;
	ColdCensusTest before = { 0 }, after = { 0 };
	InputsVisitTest direct = { 0 };
	ClusterWalTailObservation physical;
	RfPageProofDetailV1 detail;
	uint64 records = 99;
	XLogRecPtr accepted, appended;

	if (inputs == NULL)
		return;
	peer = cluster_wal_inputs_at_v1(inputs, 1);
	inputs_peer_confirm(peer);
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &before, &records, &detail), 0);
	accepted = before.cut.scan_end_exclusive;
	appended = online_census_append_noop(peer, accepted);
	UT_ASSERT(appended > accepted);
	UT_ASSERT_EQ(cluster_wal_flushed_prefix_visit(cluster_wal_threads_dir,
			&peer->source, wal_segment_size, peer->checkpoint.checkpoint_lower_lsn,
			accepted, appended, appended, peer->checkpoint_start,
			peer->checkpoint.checkpoint_record_crc32c, inputs_visit_record,
			&direct, &physical), 0);
	UT_ASSERT_EQ(physical.records, 2);
	UT_ASSERT_EQ(physical.complete_end, appended);
	inputs_peer_flush.complete_end = inputs_peer_flush.flushed_end = appended;
	UT_ASSERT_EQ(cluster_wal_inputs_census_sources_v1(inputs, cold_census_record,
			cold_census_source, &after, &records, &detail), 0);
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(after.records, 2);
	UT_ASSERT_EQ(after.begins, 2);
	UT_ASSERT_EQ(after.ends, 2);
	UT_ASSERT_EQ(memcmp(&after.source, &before.source, sizeof(before.source)), 0);
	UT_ASSERT_EQ(memcmp(&after.cut, &before.cut, sizeof(before.cut)), 0);
	UT_ASSERT_EQ(inputs_peer_begins, 1);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	inputs_live_done(&inputs);
}
