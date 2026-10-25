/* Original CTRC scope and actual INSTALL/history/claim/anchor/WAL readers.
 * Only native writer/flush and original Start ownership are fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static unsigned ctrc_local_flushes, ctrc_local_flush_fault;
static XLogRecPtr ctrc_local_requested;
static ClusterWalSourceRef ctrc_local_source;
static XLogRecPtr ctrc_local_checkpoint, ctrc_local_checkpoint_end;

static XLogRecPtr
ctrc_local_append(XLogRecPtr previous, XLogRecPtr at)
{
	v2_wal_append_noop(&ctrc_local_source, previous, at);
	return MAXALIGN(at + SizeOfXLogRecord + 10);
}

void
XLogFlush(XLogRecPtr end)
{
	UT_ASSERT_EQ(MyBackendType, B_STARTUP);
	UT_ASSERT(!RecoveryInProgress() && CritSectionCount == 0 && !ShutdownRequestPending);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(tx_inputs_start));
	UT_ASSERT_EQ(memcmp(&ctrc_inputs_owner.writer, &inputs_native_writer,
		sizeof(inputs_native_writer)), 0);
	UT_ASSERT(end != 0 && end <= test_insert);
	ctrc_local_flushes++;
	ctrc_local_requested = end;
	if (ctrc_local_flush_fault == 1) v2_checkpoint_root_race();
	if (ctrc_local_flush_fault == 2) ctrc_inputs_work.generation++;
	if (ctrc_local_flush_fault == 3) inputs_native_writer.epoch++;
	if (ctrc_local_flush_fault == 4) inputs_pin_current = false;
	if (ctrc_local_flush_fault == 5) pg_re_throw();
	if (ctrc_local_flush_fault == 6) test_flush_tli++;
	if (ctrc_local_flush_fault != 7) test_flush = Max(test_flush, end);
	if (ctrc_local_flush_fault == 8)
		test_insert = ctrc_local_append(ctrc_local_checkpoint_end, end);
}

static void
ctrc_local_fixture(bool foreign_open)
{
	uint8 bytes[66048];
	ControlRootImage root, selected;
	ControlFileData common, candidate;
	ClusterControlRootFileToken token, original;
	ClusterWalStartupImage own, peer, durable;
	ClusterWalSourceRef writer, other;
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;

	inputs_fixture(bytes, anchors);
	own = v3_install_fixture(bytes, &root);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &original), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&own, &writer), 0);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	UT_ASSERT_EQ(cluster_wal_startup_read_locked(&root, 3, &peer), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_read_thread_locked(&peer.predecessor.snapshot.identity,
		&selected, &candidate, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	cluster_node_id = 3;
	test_self_incarnation = test_membership_incarnation = peer.claim.identity.origin_owner_incarnation;
	v3_startup_native_record(&peer, &candidate);
	history_stage_dirs(3);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_checkpoint(&peer.claim.identity,
		peer.operation_uuid, &candidate, test_checkpoint_end, &durable), 0);
	UT_ASSERT_EQ(cluster_control_root_v3_startup_install_writer(&durable, &other), 0);
	cluster_node_id = 0;
	test_self_incarnation = test_membership_incarnation = own.claim.identity.origin_owner_incarnation;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	/* Peer lifecycle is a fixture input; both INSTALL producers above are
	 * real. No test manufactures the local claim, anchor or physical WAL. */
	if (!foreign_open) {
		root.records[3].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
		v2_write_roots(root.bytes);
	}
	ctrc_local_checkpoint = root.records[0].checkpoint_lower_lsn;
	ctrc_local_checkpoint_end = root.records[0].validated_tail_lsn_exclusive;
	ctrc_local_source = writer;
	MyBackendType = B_BG_WORKER;
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &inputs), 0);
	if (inputs == NULL) return;
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 4);
	tx_inputs_capture(inputs, &writer.claim.identity);
	tx_inputs_typed.census_root = original;
	cluster_wal_inputs_release_v1(&inputs);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	memset(&inputs_native_writer, 0, sizeof(inputs_native_writer));
	inputs_native_writer.ref = writer;
	inputs_native_writer.epoch = own.formation_epoch;
	inputs_native_writer.startup_first_lsn = own.first_segment_lsn;
	ctrc_inputs_owner.writer = inputs_native_writer;
	test_checkpoint_mode = true;
	test_flush_tli = writer.timeline;
	test_flush = test_insert = ctrc_local_checkpoint_end;
	ctrc_local_flushes = ctrc_local_flush_fault = 0;
	ctrc_local_requested = 0;
	test_reserve_mode = false;
}

typedef struct CtrcLocalCensus {
	ColdCensusTest census;
	unsigned local_records, local_ends, fault;
	XLogRecPtr local_end;
} CtrcLocalCensus;

static RfPageProofDetailV1
ctrc_local_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	CtrcLocalCensus *work = arg;
	RfPageProofDetailV1 result = cold_census_record(reader, source, cut, &work->census);
	if (cluster_control_root_identity_equal(&source->claim.identity, &ctrc_local_source.claim.identity)) {
		work->local_records++;
		if (work->fault == 1) inputs_native_writer.epoch++;
		if (work->fault == 2) ctrc_inputs_work.generation++;
		if (work->fault == 3) v2_checkpoint_root_race();
		if (work->fault == 4) inputs_pin_current = false;
		if (work->fault == 5) pg_re_throw();
	}
	return result;
}

static RfPageProofDetailV1
ctrc_local_source_event(const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
	const ClusterWalTailObservation *completed, void *arg)
{
	CtrcLocalCensus *work = arg;
	RfPageProofDetailV1 result = cold_census_source(source, cut, completed, &work->census);
	if (completed != NULL
		&& cluster_control_root_identity_equal(&source->claim.identity, &ctrc_local_source.claim.identity)) {
		work->local_ends++;
		work->local_end = cut->scan_end_exclusive;
	}
	if (completed != NULL && work->census.ends == 4 && work->fault == 6)
		inputs_native_writer.epoch++;
	return result;
}

static ClusterControlRootResult
ctrc_local_scan(ClusterWalInputsV1 *inputs, CtrcLocalCensus *work, uint64 *records)
{
	RfPageProofDetailV1 detail;
	return cluster_wal_inputs_cold_census_v1(inputs, ctrc_local_record,
		ctrc_local_source_event, work, records, &detail);
}

static void
ctrc_local_done(ClusterWalInputsV1 **inputs)
{
	cluster_wal_inputs_release_v1(inputs);
	UT_ASSERT(!inputs_pin_held && test_actual_cf == NoLock);
	UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
	inputs_pin_current = true;
	test_inputs_startup_cf = test_checkpoint_mode = false;
	MyBackendType = B_INVALID;
}

UT_TEST(test_ctrc_local_cut_reads_installed_writer_and_all_original_history)
{
	ClusterWalInputsV1 *inputs = NULL;
	XLogRecPtr fixed;

	ctrc_local_fixture(false);
	test_insert = test_flush = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
	fixed = test_insert;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	for (unsigned pass = 0; pass < 2; pass++) {
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		UT_ASSERT_EQ(ctrc_local_scan(inputs, &work, &records), 0);
		UT_ASSERT_EQ(work.census.begins, 4);
		UT_ASSERT_EQ(work.census.ends, 4);
		UT_ASSERT_EQ(work.local_records, 2);
		UT_ASSERT_EQ(work.local_ends, 1);
		UT_ASSERT_EQ(work.local_end, fixed);
		UT_ASSERT_EQ(records, work.census.records);
		UT_ASSERT(records >= 5);
		UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
		if (pass == 0) {
			test_insert = test_flush = ctrc_local_append(ctrc_local_checkpoint_end, fixed);
			UT_ASSERT_EQ(cluster_wal_inputs_suspend_v1(inputs), 0);
			UT_ASSERT_EQ(cluster_wal_inputs_resume_v1(inputs), 0);
		}
	}
	UT_ASSERT_EQ(ctrc_local_flushes, 0);
	for (uint32 i = 0; i < cluster_wal_inputs_count_v1(inputs); i++) {
		XLogRecPtr end = 99;
		UT_ASSERT_EQ(cluster_wal_inputs_prepare_live_v1(inputs, i, &end),
			CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(end, 0);
	}
	ctrc_local_done(&inputs);
}

UT_TEST(test_ctrc_local_cut_flushes_one_original_minimum_despite_later_insert)
{
	ClusterWalInputsV1 *inputs = NULL;
	XLogRecPtr fixed;
	ctrc_local_fixture(false);
	fixed = test_insert = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
	ctrc_local_flush_fault = 8;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	for (unsigned pass = 0; pass < 2; pass++) {
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		UT_ASSERT_EQ(ctrc_local_scan(inputs, &work, &records), 0);
		UT_ASSERT_EQ(work.local_records, 2);
		UT_ASSERT_EQ(work.local_end, fixed);
		UT_ASSERT_EQ(ctrc_local_flushes, 1);
		UT_ASSERT_EQ(ctrc_local_requested, fixed);
		UT_ASSERT(test_insert > fixed);
	}
	ctrc_local_done(&inputs);
}

UT_TEST(test_ctrc_local_cut_foreign_open_waits_before_any_callback_or_flush)
{
	ClusterWalInputsV1 *inputs = NULL;
	CtrcLocalCensus work = { 0 };
	uint64 records = 99;
	ctrc_local_fixture(true);
	test_insert = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	UT_ASSERT_EQ(ctrc_local_scan(inputs, &work, &records), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(records, 0);
	UT_ASSERT_EQ(work.census.begins + work.census.records + work.census.ends, 0);
	UT_ASSERT_EQ(ctrc_local_flushes, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
	ctrc_local_done(&inputs);
}

UT_TEST(test_ctrc_local_cut_cannot_borrow_another_native_writer_or_start)
{
	for (unsigned fault = 0; fault < 23; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		ctrc_local_fixture(false);
		test_insert = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
		switch (fault) {
		case 0: ctrc_inputs_owner.writer.ref.claim.identity.system_identifier++; break;
		case 1: ctrc_inputs_owner.writer.ref.claim.identity.storage_uuid[0]++; break;
		case 2: ctrc_inputs_owner.writer.ref.claim.identity.origin_owner_incarnation++; break;
		case 3: ctrc_inputs_owner.writer.ref.claim.database_incarnation++; break;
		case 4: ctrc_inputs_owner.writer.ref.claim.max_config_generation++; break;
		case 5: ctrc_inputs_owner.writer.ref.claim.claim_sha256[0]++; break;
		case 6: ctrc_inputs_owner.writer.ref.timeline++; break;
		case 7: ctrc_inputs_owner.writer.epoch++; break;
		case 8: ctrc_inputs_owner.writer.startup_first_lsn = 0; break;
		case 9: ctrc_inputs_owner.writer.startup_first_lsn++; break;
		case 10: ctrc_inputs_owner.writer.ref.claim.identity.origin_node_id++; break;
		default: break;
		}
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		if (fault == 11) tx_inputs_serial++;
		if (fault == 12) ctrc_inputs_work.generation++;
		if (fault == 13) CritSectionCount = 1;
		if (fault == 14) ShutdownRequestPending = true;
		if (fault == 15) test_insert = InvalidXLogRecPtr;
		if (fault == 16) test_insert = UINT64_MAX;
		if (fault == 17) test_insert = ctrc_local_checkpoint;
		if (fault == 18) test_insert = ctrc_local_checkpoint_end - 1;
		if (fault == 19) test_flush_tli++;
		if (fault == 20) {
			inputs_native_writer.epoch++;
			inputs_native_reconfigured = true;
		}
		if (fault == 21) inputs_native_recovery = true;
		if (fault == 22) MyBackendType = B_BG_WORKER;
		UT_ASSERT_NE(ctrc_local_scan(inputs, &work, &records), 0);
		UT_ASSERT_EQ(records, 0);
		UT_ASSERT_EQ(work.census.begins + work.census.records + work.census.ends, 0);
		UT_ASSERT_EQ(ctrc_local_flushes, 0);
		CritSectionCount = 0;
		ShutdownRequestPending = false;
		ctrc_local_done(&inputs);
	}
}

UT_TEST(test_ctrc_local_cut_flush_drift_error_and_short_flush_preserve_debt)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		volatile bool caught = false;
		volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		XLogRecPtr fixed;
		ctrc_local_fixture(false);
		fixed = test_insert = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
		ctrc_local_flush_fault = fault;
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		PG_TRY();
		{
			result = ctrc_local_scan(inputs, &work, &records);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		if (!caught) UT_ASSERT_NE(result, 0);
		UT_ASSERT_EQ(records, 0);
		UT_ASSERT_EQ(work.census.begins + work.census.records + work.census.ends, 0);
		UT_ASSERT_EQ(ctrc_local_flushes, 1);
		UT_ASSERT(inputs_pin_held);
		if (fault == 7) {
			/* WAIT preserves this same frozen end; no moving resample. */
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
			UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), 0);
			test_insert = ctrc_local_append(ctrc_local_checkpoint_end, fixed);
			ctrc_local_flush_fault = 0;
			UT_ASSERT_EQ(ctrc_local_scan(inputs, &work, &records), 0);
			UT_ASSERT_EQ(work.local_end, fixed);
			UT_ASSERT_EQ(ctrc_local_requested, fixed);
		} else
			UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), 0);
		ctrc_local_done(&inputs);
	}
}

UT_TEST(test_ctrc_local_cut_record_and_final_owner_checks_refuse_drift)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		volatile bool caught = false;
		volatile ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		ctrc_local_fixture(false);
		work.fault = fault;
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		PG_TRY();
		{
			result = ctrc_local_scan(inputs, &work, &records);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		if (!caught) UT_ASSERT_NE(result, 0);
		UT_ASSERT_EQ(records, 0);
		UT_ASSERT(work.local_records > 0);
		UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), 0);
		UT_ASSERT(inputs_pin_held);
		ctrc_local_done(&inputs);
	}
}

UT_TEST(test_ctrc_local_cut_damage_never_shortens_the_confirmed_native_end)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		CtrcLocalCensus work = { 0 };
		uint64 records = 99;
		char path[MAXPGPATH];
		int fd;
		uint8 bad = 0xff;
		ctrc_local_fixture(false);
		test_flush = test_insert = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
		v2_checkpoint_wal_path(&ctrc_local_source.claim.identity, ctrc_local_checkpoint_end,
			ctrc_local_source.timeline, path);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		if (fault == 0)
			UT_ASSERT_EQ(pwrite(fd, &bad, 1, ctrc_local_checkpoint_end % wal_segment_size), 1);
		if (fault == 1)
			UT_ASSERT_EQ(ftruncate(fd, ctrc_local_checkpoint_end % wal_segment_size), 0);
		UT_ASSERT_EQ(close(fd), 0);
		if (fault == 2)
			v2_incomplete_wal_tail(&ctrc_local_source.claim.identity, ctrc_local_source.timeline,
				wal_segment_size, ctrc_local_checkpoint_end, ctrc_local_checkpoint);
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
		UT_ASSERT_NE(ctrc_local_scan(inputs, &work, &records), 0);
		UT_ASSERT_EQ(records, 0);
		UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), 0);
		UT_ASSERT(inputs_pin_held);
		ctrc_local_done(&inputs);
	}
}
