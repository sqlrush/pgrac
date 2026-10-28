/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_clog_side.inc
 *    Original native CLOG and sealed SIDE bytes in the same physical pass.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_clog_side.inc
 *
 * NOTES
 *    Original ROOT/WAL/scanner/counter and sealed SIDE execute. Original
 *    Startup/GES/native CLOG leaves remain controlled; no lifetime grant.
 *-------------------------------------------------------------------------
 */
static unsigned episode_clog_side_fault;

static WalTestRecord
episode_clog_side_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	if (source->claim.identity.origin_node_id == 0)
		return episode_birth_match_prefix(source, previous);
	{
		int page = 17;
		xl_clog_truncate truncate = {16, 524291, 606};
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
		previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
			RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		return episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
}

static RfPageProofDetailV1
episode_clog_side_census_feed(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	DecodedXLogRecord record;
	XLogReaderState changed;
	char payload[sizeof(xl_clog_zeropage_full)];
	uint8 info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	bool full = source->claim.identity.origin_node_id == 0 && info == CLOG_ZEROPAGE_FULL;
	bool alter = XLogRecGetRmid(reader) == RM_CLOG_ID
		&& ((full && (episode_clog_side_fault == 1 || episode_clog_side_fault == 2))
			|| (!full && episode_clog_side_fault == 3 && info == CLOG_ZEROPAGE)
			|| (!full && episode_clog_side_fault == 4 && info == CLOG_TRUNCATE));

	if (episode_clog_side_fault == 5 && XLogRecGetRmid(reader) == RM_CLOG_ID
		&& info == CLOG_ZEROPAGE && source->claim.identity.origin_node_id != 0)
		return RF_PAGE_PROOF_DETAIL_OK; /* missing sealed native carrier */
	if (episode_clog_side_fault == 6 && XLogRecGetRmid(reader) == RM_RELMAP_ID
		&& source->claim.identity.origin_node_id != 0) {
		int page = 19;
		record = *reader->record; changed = *reader; changed.record = &record;
		record.header.xl_rmid = RM_CLOG_ID; record.header.xl_info = CLOG_ZEROPAGE;
		record.header.xl_tot_len = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(page);
		record.main_data = (char *)&page; record.main_data_len = sizeof(page);
		return episode_terminal_census_feed(&changed, source, cut, arg); /* extra sealed carrier */
	}
	if (!alter)
		return episode_terminal_census_feed(reader, source, cut, arg);
	record = *reader->record; changed = *reader; changed.record = &record;
	if (episode_clog_side_fault == 1 || episode_clog_side_fault == 3)
		record.header.xl_prev ^= 1;
	else {
		UT_ASSERT(XLogRecGetDataLen(reader) <= sizeof(payload));
		if (XLogRecGetDataLen(reader) > sizeof(payload)) return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		memcpy(payload, XLogRecGetData(reader), XLogRecGetDataLen(reader));
		if (full) {
			xl_clog_zeropage_full birth;
			memcpy(&birth, payload, sizeof(birth)); birth.first_full_xid += UINT64_C(1) << 32;
			memcpy(payload, &birth, sizeof(birth));
		} else {
			xl_clog_truncate truncate;
			memcpy(&truncate, payload, sizeof(truncate)); truncate.oldestXactDb++;
			memcpy(payload, &truncate, sizeof(truncate));
		}
		record.main_data = payload;
	}
	return episode_terminal_census_feed(&changed, source, cut, arg);
}

static void
episode_clog_side_plans(ClusterWalInputsV1 *inputs)
{
	uint64 records;
	RfPageProofDetailV1 detail;
	uint32 seen[CLUSTER_COLD_MAX_PARTICIPANTS] = {0};
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p) {
		const ClusterWalSourceRef *source = &tx_inputs_typed.sources[p];
		const ClusterColdParticipantV1 *part = &tx_inputs_typed.participants[p];
		RfSideOnlinePlanRequestV1 request = {0};
		RfContributorStreamCutV1 cut = {0};
		cut.failed_thread = part->thread_id;
		cut.timeline_id = part->timeline;
		cut.origin_owner_incarnation = part->owner_incarnation;
		cut.scan_begin_inclusive = part->physical_lower;
		cut.scan_end_exclusive = part->tail_end;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		request.system_identifier = source->claim.identity.system_identifier;
		memcpy(request.storage_uuid, source->claim.identity.storage_uuid, 16);
		request.physical_cuts = &cut;
		request.participant_count = 1;
		request.redo_starts = &part->native_redo;
		request.purpose = RF_SIDE_PLAN_RECOVERY;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &tx_inputs_typed.side_plans[p]), 0);
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(tx_inputs_typed.side_plans[p],
			tx_inputs_typed.sources, tx_inputs_typed.participant_count));
	}
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, episode_clog_side_census_feed,
		episode_terminal_census_source, seen, &records, &detail), 0);
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p) {
		UT_ASSERT(rf_side_online_plan_bind_database_v1(tx_inputs_typed.side_plans[p],
			tx_inputs_typed.sources[p].claim.database_incarnation));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(tx_inputs_typed.side_plans[p]), 0);
	}
}

static void
episode_clog_side_terminal_begin(EpisodeTerminalFixture mode, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	ClusterWalInputsV1 *raw = relmap_history_fixture(3, 2, false);
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalSourceRef source = {0};
	ClusterControlRootSnapshot *snapshot;
	WalTestRecord tail;
	char payload[TEST_PREPARE_MAX];

	cluster_wal_inputs_release_v1(&raw);
	test_inputs_startup_cf = false;
	MyBackendType = B_BG_WORKER;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	snapshot = &root.records[0];
	/* Complete the other original source after its RELMAP participant so
	 * the sealed SIDE plan observes the real physical tail as well. */
	{
		ClusterControlRootSnapshot *other = &root.records[127];
		ClusterWalSourceRef foreign = {0};
		WalTestRecord previous = {1, other->validated_tail_lsn_exclusive,
			other->tail_last_record_lsn, other->tail_last_record_crc32c};
		foreign.claim.identity = other->identity; foreign.timeline = other->tail_tli;
		previous = episode_terminal_write(&foreign, previous, false, 4196001);
		other->validated_tail_lsn_exclusive = previous.exclusive_end;
		other->tail_last_record_lsn = previous.record_start;
		other->tail_last_record_crc32c = previous.record_crc;
	}
	source.claim.identity = snapshot->identity;
	source.timeline = snapshot->tail_tli;
	tail = (WalTestRecord){1, snapshot->validated_tail_lsn_exclusive,
		snapshot->tail_last_record_lsn, snapshot->tail_last_record_crc32c};
	if (mode != EPISODE_TERMINAL_MISSING) {
		tail = episode_terminal_write(&source, tail, mode == EPISODE_TERMINAL_ABORT, 4195136);
	}
	if (mode == EPISODE_TERMINAL_CONFLICT)
		tail = episode_terminal_write(&source, tail, true, 4195136);
	if (mode == EPISODE_TERMINAL_OTHER_XID)
		tail = episode_terminal_write(&source, tail, false, 4195137);
	if (mode == EPISODE_TERMINAL_CHILD) {
		TransactionId parent = 4196000, child = 4195136;
		int count = 1;
		memcpy(payload, &parent, sizeof(parent));
		memcpy(payload + sizeof(parent), &count, sizeof(count));
		memcpy(payload + MinSizeOfXactAssignment, &child, sizeof(child));
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_XACT_ID, XLOG_XACT_ASSIGNMENT, payload, MinSizeOfXactAssignment + sizeof(child));
	}
	if (mode == EPISODE_TERMINAL_PREPARE) {
		uint32 length = test_prepare_payload(payload, false, 13);
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_XACT_ID, XLOG_XACT_PREPARE, payload, length);
	}
	if (mode == EPISODE_TERMINAL_ALIAS) {
		test_relmap_payload(payload, true, 2);
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_RELMAP_ID, XLOG_RELMAP_UPDATE, payload, TEST_RELMAP_LENGTH);
	}
	if (mode == EPISODE_TERMINAL_ZERO || mode == EPISODE_TERMINAL_OTHER_PAGE) {
		int page = 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE + (mode == EPISODE_TERMINAL_OTHER_PAGE);
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
	}
	if (mode == EPISODE_TERMINAL_TRUNCATE) {
		xl_clog_truncate truncate = {1, 32768, 5};
		tail = inventory_write_record(&source, tail.exclusive_end, tail.record_start,
			RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
	}
	if (episode_terminal_tail_hook != NULL)
		tail = episode_terminal_tail_hook(&source, tail);
	snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	snapshot->tail_last_record_lsn = tail.record_start;
	snapshot->tail_last_record_crc32c = tail.record_crc;
	tx_ir_complete(snapshot);
	tx_ir_complete(&root.records[127]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &raw), 0);
	tx_inputs_capture(raw, NULL);
	cluster_wal_inputs_release_v1(&raw);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs;
	ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = ctrc_set_preflight_retire = 0;
	ctrc_set_after_grant = NULL;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = relmap_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_declared = true;
	relmap_failed = 0;
	relmap_sibling = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	episode_terminal_side_mismatch = mode == EPISODE_TERMINAL_SIDE_MISMATCH;
	/* These deliberate raw-input refusals must fire before native I/O or any
	 * downstream SIDE consultation. Leave those plans absent, rather than
	 * fabricate generic SIDE coverage of PREPARE/alias/lifecycle obligations. */
	if (mode == EPISODE_TERMINAL_COMMIT || mode == EPISODE_TERMINAL_ABORT
		|| mode == EPISODE_TERMINAL_OTHER_PAGE || mode == EPISODE_TERMINAL_OTHER_XID
		|| mode == EPISODE_TERMINAL_SIDE_MISMATCH)
		episode_terminal_plans(*inputs);
	episode_terminal_side_mismatch = false;
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	UT_ASSERT_EQ(episode_history_build(*inputs, 1024 * 1024, history, &diag), 0);
	UT_ASSERT(*history != NULL);
	if (*history != NULL)
		*pending = relmap_fold_pending((*history)->inventory, 0);
}

static void
episode_clog_side_begin(unsigned fault, bool abort, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	episode_base_reset(); episode_birth_match_mode = 0;
	episode_pending_prefix_hook = episode_clog_side_prefix;
	episode_clog_side_terminal_begin(abort ? EPISODE_TERMINAL_ABORT : EPISODE_TERMINAL_COMMIT,
		inputs, history, pending);
	episode_pending_prefix_hook = NULL;
	episode_terminal_status(abort ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; i++)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
	episode_clog_side_fault = fault;
	episode_clog_side_plans(*inputs);
	episode_clog_side_fault = 0;
}

UT_TEST(test_episode_clog_side_matches_all_physical_generations_in_one_original_pass)
{
	for (unsigned abort = 0; abort < 2; abort++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native;
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census, reads, acquires;
		uint32 count = 0;

		episode_clog_side_begin(0, abort, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		for (uint32 i = 0; i < tx_inputs_typed.participant_count; i++) {
			uint32 seen = rf_side_online_plan_native_clog_count_v1(tx_inputs_typed.side_plans[i],
				&tx_inputs_typed.sources[i]);
			UT_ASSERT(seen != UINT32_MAX); count += seen;
		}
		UT_ASSERT_EQ(count, 3);
		census = inventory_census_calls; reads = episode_base_calls; acquires = ctrc_set_calls;
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census + 1); UT_ASSERT_EQ(episode_base_calls, reads + 2);
		UT_ASSERT_EQ(native.birth.inputs.generation_count, 2);
		UT_ASSERT_EQ(native.birth.inputs.clog_count, 1);
		UT_ASSERT_EQ(native.terminal.base.native.status,
			abort ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		UT_ASSERT_EQ(ctrc_set_calls, acquires);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls | relmap_failed, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1 && !history->failed);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_clog_side_refuses_complete_native_header_and_payload_mismatches)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		unsigned census;

		episode_clog_side_begin(fault, false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		held = history->native_base;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base == held);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		census = inventory_census_calls;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(inventory_census_calls, census);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

static void
episode_clog_side_drop_other_plan(void)
{
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; i++)
		if (tx_inputs_typed.sources[i].claim.identity.origin_node_id != 0)
			rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[i]);
}

UT_TEST(test_episode_clog_side_requires_every_original_plan_before_and_after_reread)
{
	for (unsigned late = 0; late < 2; late++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;

		episode_clog_side_begin(0, false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		held = history->native_base;
		inventory_census_calls = 0;
		if (late) inventory_between_passes = episode_clog_side_drop_other_plan;
		else episode_clog_side_drop_other_plan();
		UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
		inventory_between_passes = NULL;
		UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
		UT_ASSERT(history->failed && relmap_failed != 0 && history->native_base == held);
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_clog_side_whole_output_owner_error_retains_original_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;
	unsigned census, reads;

	episode_clog_side_begin(0, false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
	held = history->native_base; saved = pending; census = inventory_census_calls; reads = episode_base_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, (void *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(pending)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, (void *)history, &diag));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, (void *)&native.terminal));
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
	CritSectionCount = 0;
	UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
	UT_ASSERT_EQ(inventory_census_calls, census); UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT(!history->failed && history->native_base == held);
	inventory_census_calls = 0; inventory_between_passes = episode_terminal_recheck_error;
	test_capture_error_level = true;
	PG_TRY(); { (void)cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false; inventory_between_passes = NULL;
	UT_ASSERT(caught && history->failed && relmap_failed != 0 && history->native_base == held);
	UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
	UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && episode_base_live == 1);
	episode_terminal_done(&history, &inputs);
}
