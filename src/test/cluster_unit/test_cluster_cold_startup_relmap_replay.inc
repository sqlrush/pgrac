/*-------------------------------------------------------------------------
 * test_cluster_cold_startup_relmap_replay.inc
 *     Original Startup plan, sealed SIDE and relmap files in one replay.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_startup_relmap_replay.inc
 * NOTES
 *     ROOT, scans, PAGE application and isolation are controlled boundaries.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static struct {
	ClusterColdRelmapInputV1 input;
	ClusterColdTypedV1 *typed;
	XLogReaderState reader;
	char payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	unsigned next[NROOTS];
	unsigned pages;
	unsigned fault;
	DecodedXLogRecord decoded;
} startup_relmap_replay;

/* Build the same decoded frame the original scan and merge must compare.
 * The SIDE implementation copies all bytes before this reader is reused. */
static RfPageProofDetailV1
startup_relmap_feed(RfSideOnlinePlanV1 *side, bool image)
{
	ClusterColdRelmapInputV1 *input = &startup_relmap_replay.input;
	XLogReaderState control = {0};
	DecodedXLogRecord decoded = {0};
	XLogReaderState *reader = &startup_relmap_replay.reader;
	RfPageOnlineRecordIdentityV1 identity = {0};
	RfDetachedRecordPlanV1 record = {0};

	if (!image) {
		control.system_identifier = input->source.claim.identity.system_identifier;
		control.cluster_expected_thread_id = input->cut.thread_id;
		control.seg.ws_tli = input->cut.timeline;
		control.ReadRecPtr = decoded.lsn = input->cut.physical_lower;
		control.EndRecPtr = decoded.next_lsn = input->cut.native_redo;
		control.record = &decoded;
		decoded.header.xl_tot_len = SizeOfXLogRecord;
		decoded.header.xl_rmid = RM_XLOG_ID;
		decoded.header.xl_info = XLOG_NOOP;
		decoded.max_block_id = -1;
		reader = &control;
	}
	identity.record.system_identifier = reader->system_identifier;
	memcpy(identity.record.storage_uuid, input->source.claim.identity.storage_uuid, 16);
	identity.record.origin_thread = reader->cluster_expected_thread_id;
	identity.record.timeline_id = reader->seg.ws_tli;
	identity.record.read_rec_ptr = reader->ReadRecPtr;
	identity.record.end_rec_ptr = reader->EndRecPtr;
	identity.record.record_crc = (uint32)reader->record->header.xl_crc;
	identity.record.rmid = XLogRecGetRmid(reader);
	identity.record.info = XLogRecGetInfo(reader);
	record.source_record = reader;
	record.preflight_complete = true;
	if (rf_opcode_route_lookup_v1(XLogRecGetRmid(reader), XLogRecGetInfo(reader),
		false, false, &record.route) != RF_OPCODE_ROUTE_OK)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	return image ? cold_native_side_rf_side_online_plan_feed_relmap_v1(side, &record,
		&identity, input) : cold_native_side_rf_side_online_plan_feed_record_v1(side,
		&record, &identity);
}

static void
startup_relmap_replay_begin(void)
{
	xl_relmap_update header = {InvalidOid, GLOBALTABLESPACE_OID, sizeof(RelMapFile)};
	ClusterColdRelmapInputV1 *input = &startup_relmap_replay.input;
	XLogReaderState *reader = &startup_relmap_replay.reader;
	DecodedXLogRecord *decoded = &startup_relmap_replay.decoded;

	memset(&startup_relmap_replay, 0, sizeof(startup_relmap_replay));
	startup_relmap_fixture(input);
	test_cold_relmap_storage_begin(input);
	reader->system_identifier = input->staged.identity.system_identifier;
	reader->cluster_expected_thread_id = input->cut.thread_id;
	reader->seg.ws_tli = input->cut.timeline;
	reader->ReadRecPtr = decoded->lsn = input->frame.read_rec_ptr;
	reader->EndRecPtr = decoded->next_lsn = input->frame.end_rec_ptr;
	reader->record = decoded;
	decoded->header.xl_tot_len = SizeOfXLogRecord + 3 + sizeof(startup_relmap_replay.payload);
	decoded->header.xl_prev = input->cut.physical_lower;
	decoded->header.xl_xid = input->frame.source_xid;
	decoded->header.xl_rmid = RM_RELMAP_ID;
	decoded->header.xl_info = XLOG_RELMAP_UPDATE;
	decoded->header.xl_crc = input->frame.record_crc;
	decoded->max_block_id = -1;
	decoded->main_data = startup_relmap_replay.payload;
	decoded->main_data_len = sizeof(startup_relmap_replay.payload);
	memcpy(decoded->main_data, &header, MinSizeOfRelmapUpdate);
	memcpy(decoded->main_data + MinSizeOfRelmapUpdate, &input->staged.pending,
		sizeof(RelMapFile));
	startup_relmap_real_side = true;
	startup_relmap_replay.typed = startup_relmap_prepare(input);
	UT_ASSERT_NOT_NULL(startup_relmap_replay.typed);
	UT_ASSERT_EQ(startup_relmap_replay.typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(startup_relmap_replay.typed->scanned_records, 2 * NROOTS);
	UT_ASSERT_NOT_NULL(startup_relmap_real_plan);
	UT_ASSERT(cold_native_side_rf_side_online_plan_relmap_input_matches_v1(
		startup_relmap_real_plan, input));
	cluster_cold_replay_window_enter_v1();
}

static bool
startup_relmap_next(void *arg, uint32 p, ClusterColdReplayRecordV1 *out)
{
	unsigned index = startup_relmap_replay.next[p]++;
	(void)arg;
	/* The original sequencer starts at native redo, after retained history. */
	if (index != 0) return false;
	memset(out, 0, sizeof(*out));
	out->read_rec_ptr = 0x800;
	out->end_rec_ptr = 0x2000;
	if (p == 0) {
		out->rmid = RM_RELMAP_ID;
		out->info = XLOG_RELMAP_UPDATE;
		out->record_crc = startup_relmap_replay.input.frame.record_crc;
	}
	return true;
}

static bool
startup_relmap_unscheduled(void *arg, uint32 p)
{
	(void)arg;
	(void)p;
	return false; /* This fixture has exactly one scheduled post-redo frame. */
}

static bool
startup_relmap_scheduled(void *arg, uint32 p, const ClusterColdStepV1 *step,
	ClusterColdPageActionV1 action)
{
	(void)arg;
	if (p == 0)
		return action == CLUSTER_COLD_RELMAP_STEP
			&& cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
				(ClusterRecoveryFencePlan *)&fixture_context, step, &startup_relmap_replay.reader);
	if (action != CLUSTER_COLD_PAGE_APPLY
		|| !test_cold_relmap_storage_matches(&startup_relmap_replay.input,
			startup_relmap_replay.pages != 0)) return false;
	++startup_relmap_replay.pages;
	return true;
}

static ClusterColdReplayDetailV1
startup_relmap_run(void)
{
	const ClusterColdReplayOpsV1 ops = {startup_relmap_next,
		startup_relmap_unscheduled, startup_relmap_scheduled};
	ClusterColdReplayResultV1 result;
	return cluster_cold_replay_run_v1(startup_relmap_replay.typed->plan,
		startup_relmap_replay.typed->participants, NROOTS, 0, &ops, NULL, &result);
}

static void
startup_relmap_replay_end(void)
{
	cluster_cold_typed_destroy_v1(&startup_relmap_replay.typed);
	cluster_cold_replay_window_leave_v1();
	UT_ASSERT_NULL(startup_relmap_real_plan);
	startup_relmap_real_side = false;
	UT_ASSERT_EQ(contexts_alive, 0);
	test_cold_relmap_storage_end();
}

UT_TEST(test_startup_relmap_replays_original_sealed_carrier_between_other_pages)
{
	ClusterColdStepV1 step;
	TestColdRelmapStorageStats before, after;
	startup_relmap_replay_begin();
	UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(startup_relmap_replay.pages, 2);
	UT_ASSERT(cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context));
	before = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(before.writes, 2);
	UT_ASSERT(before.pin_retained && before.may_be_staged);
	UT_ASSERT_EQ(before.io_under_retention_lock, 0);
	UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
	UT_ASSERT(cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
		(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
	after = test_cold_relmap_storage_stats();
	UT_ASSERT_EQ(after.writes, before.writes);
	startup_relmap_replay_end();
}

UT_TEST(test_startup_relmap_missing_or_changed_carrier_refuses_before_files)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		RfSideOnlinePlanV1 *saved;
		TestColdRelmapStorageStats stats;
		startup_relmap_replay_begin();
		saved = startup_relmap_replay.typed->side_plans[0];
		if (fault == 0) startup_relmap_replay.typed->side_plans[0] = NULL;
		else startup_relmap_replay.decoded.header.xl_prev++;
		UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_CALLBACK);
		stats = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(stats.writes, 0);
		UT_ASSERT(stats.pin_retained && !stats.may_be_staged);
		startup_relmap_replay.typed->side_plans[0] = saved;
		startup_relmap_replay.decoded.header.xl_prev = 0x100;
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		startup_relmap_replay_end();
	}
}

static void
startup_relmap_late_failure(bool directory)
{
	if (directory) return;
	if (startup_relmap_replay.fault == 1) tokens[3].file_txn_seq++;
	else siglongjmp(*PG_exception_stack, 1);
}

UT_TEST(test_startup_relmap_partial_write_and_late_error_retain_original_debt)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterColdStepV1 step;
		TestColdRelmapStorageStats before, after;
		volatile bool caught = false;
		volatile ClusterColdReplayDetailV1 result = CLUSTER_COLD_REPLAY_OK;
		startup_relmap_replay_begin();
		startup_relmap_replay.fault = fault;
		test_cold_relmap_storage_fault(fault == 0 ? 2 : 0,
			fault == 0 ? NULL : startup_relmap_late_failure);
		PG_TRY(); { result = startup_relmap_run(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(fault == 2 ? caught : result == CLUSTER_COLD_REPLAY_CALLBACK);
		test_cold_relmap_storage_fault(0, NULL);
		before = test_cold_relmap_storage_stats();
		UT_ASSERT(before.writes > 0 && before.pin_retained && before.may_be_staged);
		UT_ASSERT_EQ(before.io_under_retention_lock, 0);
		UT_ASSERT(cluster_cold_plan_step_v1(startup_relmap_replay.typed->plan, 1, &step));
		UT_ASSERT(!cluster_cold_typed_relmap_record_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context, &step, &startup_relmap_replay.reader));
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(after.writes, before.writes);
		startup_relmap_replay_end();
	}
}

UT_TEST(test_startup_relmap_final_receipt_rechecks_original_source_and_side)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		RfSideOnlinePlanV1 *saved;
		TestColdRelmapStorageStats before, after;
		startup_relmap_replay_begin();
		UT_ASSERT_EQ(startup_relmap_run(), CLUSTER_COLD_REPLAY_OK);
		before = test_cold_relmap_storage_stats();
		saved = startup_relmap_replay.typed->side_plans[0];
		if (fault == 0) startup_relmap_replay.typed->side_plans[0] = NULL;
		else tokens[3].file_txn_seq++;
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		startup_relmap_replay.typed->side_plans[0] = saved;
		if (fault != 0) tokens[3].file_txn_seq--;
		UT_ASSERT(!cluster_cold_typed_relmap_ready_v1(startup_relmap_replay.typed,
			(ClusterRecoveryFencePlan *)&fixture_context));
		after = test_cold_relmap_storage_stats();
		UT_ASSERT_EQ(after.writes, before.writes);
		UT_ASSERT(after.pin_retained && after.may_be_staged);
		startup_relmap_replay_end();
	}
}
