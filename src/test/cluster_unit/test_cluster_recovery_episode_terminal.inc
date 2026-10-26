/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_terminal.inc
 *     Original complete native/SIDE terminal and page-lifecycle inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_terminal.inc
 * NOTES
 *     Real ROOT/claim/anchor/WAL readers and sealed SIDE plans execute.
 *     Existing Startup/GES/native CLOG leaves stay controlled. Agreement is
 *     an observation, never era/lifetime, COMPLETE or release authority.
 *-------------------------------------------------------------------------
 */
typedef enum EpisodeTerminalFixture {
	EPISODE_TERMINAL_COMMIT, EPISODE_TERMINAL_ABORT, EPISODE_TERMINAL_CONFLICT,
	EPISODE_TERMINAL_CHILD, EPISODE_TERMINAL_PREPARE, EPISODE_TERMINAL_ALIAS,
	EPISODE_TERMINAL_MISSING, EPISODE_TERMINAL_OTHER_PAGE,
	EPISODE_TERMINAL_ZERO, EPISODE_TERMINAL_TRUNCATE, EPISODE_TERMINAL_OTHER_XID,
	EPISODE_TERMINAL_SIDE_MISMATCH
} EpisodeTerminalFixture;

typedef struct EpisodeTerminalPlanFeed {
	RfSideOnlinePlanV1 *plan;
	const ClusterWalSourceRef *source;
} EpisodeTerminalPlanFeed;
static bool episode_terminal_side_mismatch;

static bool
episode_terminal_plan_feed(XLogReaderState *reader, void *arg)
{
	EpisodeTerminalPlanFeed *feed = arg;
	RfPageOnlineRecordIdentityV1 identity = {0};
	RfDetachedRecordPlanV1 record = {0};
	DecodedXLogRecord changed;
	XLogReaderState changed_reader;
	char changed_payload[TEST_PREPARE_MAX];
	uint8 info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

	/* RELMAP remains in the original full inventory/cold census, owned by
	 * its separate participant. This fixture only seals native/SIDE terminal
	 * correspondence; the generic SIDE decoder does not own RELMAP. */
	if (XLogRecGetRmid(reader) == RM_RELMAP_ID)
		return true;

	identity.record.system_identifier = feed->source->claim.identity.system_identifier;
	memcpy(identity.record.storage_uuid, feed->source->claim.identity.storage_uuid, 16);
	identity.record.origin_thread = feed->source->claim.identity.origin_thread_id;
	identity.record.timeline_id = feed->source->timeline;
	identity.record.read_rec_ptr = reader->ReadRecPtr;
	identity.record.end_rec_ptr = reader->EndRecPtr;
	identity.record.record_crc = (uint32)reader->record->header.xl_crc;
	identity.record.rmid = XLogRecGetRmid(reader);
	identity.record.info = XLogRecGetInfo(reader);
	record.source_record = reader;
	/* A controlled stale SIDE witness must not match the actual native bytes. */
	if (episode_terminal_side_mismatch && XLogRecGetRmid(reader) == RM_XACT_ID
		&& (info == XLOG_XACT_COMMIT || info == XLOG_XACT_ABORT)) {
		UT_ASSERT(XLogRecGetDataLen(reader) <= sizeof(changed_payload));
		if (XLogRecGetDataLen(reader) > sizeof(changed_payload)) return false;
		memcpy(changed_payload, XLogRecGetData(reader), XLogRecGetDataLen(reader));
		changed_payload[0] ^= 1;
		changed = *reader->record;
		changed.main_data = changed_payload;
		changed_reader = *reader;
		changed_reader.record = &changed;
		record.source_record = &changed_reader;
	}
	record.preflight_complete = true;
	if (rf_opcode_route_lookup_v1(XLogRecGetRmid(reader), XLogRecGetInfo(reader),
		XLogRecHasAnyBlockRefs(reader), true, &record.route) != RF_OPCODE_ROUTE_OK)
		return false;
	return rf_side_online_plan_feed_record_v1(feed->plan, &record, &identity) == 0;
}

static RfPageProofDetailV1
episode_terminal_census_feed(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	uint32 *seen = arg;
	(void)cut;
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p) {
		if (memcmp(source, &tx_inputs_typed.sources[p], sizeof(*source)) == 0) {
			EpisodeTerminalPlanFeed feed = {tx_inputs_typed.side_plans[p], source};
			if (!episode_terminal_plan_feed(reader, &feed))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (XLogRecGetRmid(reader) == RM_XACT_ID
				&& ((XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT
					|| (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT))
				seen[p]++;
			return RF_PAGE_PROOF_DETAIL_OK;
		}
	}
	return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
}

static RfPageProofDetailV1
episode_terminal_census_source(const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, const ClusterWalTailObservation *completed, void *arg)
{
	(void)completed; (void)arg;
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p) {
		const ClusterColdParticipantV1 *part = &tx_inputs_typed.participants[p];
		if (memcmp(source, &tx_inputs_typed.sources[p], sizeof(*source)) == 0)
			return cut->failed_thread == part->thread_id
				&& cut->origin_owner_incarnation == part->owner_incarnation
				&& cut->scan_begin_inclusive == part->physical_lower
				&& cut->scan_end_exclusive == part->tail_end
				? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
}

/* Feed the original complete cold census after simultaneous IR/WALR custody,
 * rather than borrowing an ordinary retained-read permission on recovered
 * inputs. Every plan binds the same complete generation set. */
static void
episode_terminal_plans(ClusterWalInputsV1 *inputs)
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
	UT_ASSERT_EQ(cluster_wal_inputs_cold_census_v1(inputs, episode_terminal_census_feed,
		episode_terminal_census_source, seen, &records, &detail), 0);
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p) {
		/* A source without a native terminal supplies no terminal plan here.
		 * No empty-list completion or SIDE discharge is fabricated. */
		if (seen[p] == 0) {
			rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[p]);
			continue;
		}
		UT_ASSERT(rf_side_online_plan_bind_database_v1(tx_inputs_typed.side_plans[p],
			tx_inputs_typed.sources[p].claim.database_incarnation));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(tx_inputs_typed.side_plans[p]), 0);
	}
}

static WalTestRecord
episode_terminal_write(const ClusterWalSourceRef *source, WalTestRecord previous,
	bool abort, TransactionId raw)
{
	char payload[sizeof(xl_xact_commit) + sizeof(xl_xact_xinfo)
		+ sizeof(xl_xact_scn) + sizeof(xl_xact_tt_commit)] = {0};
	TimestampTz time = 8765;
	uint32 xinfo = XACT_XINFO_HAS_SCN | (abort ? 0 : XACT_XINFO_HAS_TT_COMMIT);
	xl_xact_scn scn = {5906};
	xl_xact_tt_commit tt = {0};
	size_t used = 0;
	WalTestRecord next;

	memcpy(payload + used, &time, sizeof(time)); used += sizeof(time);
	memcpy(payload + used, &xinfo, sizeof(xinfo)); used += sizeof(xinfo);
	memcpy(payload + used, &scn, sizeof(scn)); used += sizeof(scn);
	if (!abort) {
		tt.instance = source->claim.identity.origin_thread_id;
		tt.segment_id = source->claim.identity.origin_node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		tt.segment_generation = 11; tt.slot_offset = 4; tt.wrap = 7;
		tt.xid = raw; tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION; tt.commit_scn = scn.scn;
		memcpy(payload + used, &tt, sizeof(tt)); used += sizeof(tt);
	}
	next = inventory_write_record(source, previous.exclusive_end, previous.record_start,
		RM_XACT_ID, (abort ? XLOG_XACT_ABORT : XLOG_XACT_COMMIT) | XLOG_XACT_HAS_INFO,
		payload, used);
	if (raw != 4195136) {
		char path[MAXPGPATH];
		uint8 bytes[2048];
		XLogRecord header;
		int fd;
		v2_checkpoint_wal_path(&source->claim.identity, next.record_start, source->timeline, path);
		fd = open(path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, &header, SizeOfXLogRecord, next.record_start % wal_segment_size), SizeOfXLogRecord);
		UT_ASSERT(header.xl_tot_len <= sizeof(bytes));
		UT_ASSERT_EQ(pread(fd, bytes, header.xl_tot_len, next.record_start % wal_segment_size), header.xl_tot_len);
		header.xl_xid = raw;
		INIT_CRC32C(header.xl_crc);
		COMP_CRC32C(header.xl_crc, bytes + SizeOfXLogRecord, header.xl_tot_len - SizeOfXLogRecord);
		COMP_CRC32C(header.xl_crc, &header, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(header.xl_crc);
		UT_ASSERT_EQ(pwrite(fd, &header, SizeOfXLogRecord, next.record_start % wal_segment_size), SizeOfXLogRecord);
		UT_ASSERT_EQ(close(fd), 0);
		next.record_crc = header.xl_crc;
	}
	return next;
}

static void
episode_terminal_begin(EpisodeTerminalFixture mode, ClusterWalInputsV1 **inputs,
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
episode_terminal_done(ClusterRecoveryRelmapHistory **history, ClusterWalInputsV1 **inputs)
{
	for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p)
		rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[p]);
	episode_history_done(history, inputs);
	episode_base_enabled = false;
}

static void
episode_terminal_status(XidStatus status)
{
	uint32 raw = 4195136;
	episode_base_page[(raw % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4]
		= status << ((raw % 4) * 2);
}

UT_TEST(test_episode_terminal_matches_actual_sealed_side_and_same_held_page)
{
	for (unsigned outcome = 0; outcome < 4; ++outcome) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingTerminalV1 terminal, again;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		unsigned acquires;
		episode_base_reset();
		episode_terminal_begin(outcome == 1 ? EPISODE_TERMINAL_ABORT
			: outcome == 2 ? EPISODE_TERMINAL_OTHER_PAGE
			: outcome == 3 ? EPISODE_TERMINAL_OTHER_XID : EPISODE_TERMINAL_COMMIT,
			&inputs, &history, &pending);
		if (history == NULL) return;
		episode_terminal_status(outcome == 1 ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		acquires = ctrc_set_calls;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
		held = history->native_base;
		UT_ASSERT(held != NULL && terminal.terminal_start >= pending.pending_owner.wal_end);
		UT_ASSERT(terminal.terminal_end > terminal.terminal_start);
		UT_ASSERT_EQ(terminal.terminal_source_index, terminal.base.history.observation.source_index);
		UT_ASSERT(terminal.terminal_ordinal > 1);
		UT_ASSERT_EQ(terminal.base.fold.top_commits, outcome != 1);
		UT_ASSERT_EQ(terminal.base.fold.top_aborts, outcome == 1);
		UT_ASSERT_EQ(cluster_wal_inputs_relmap_terminal_count_v1(&terminal.base.history.source), outcome == 3 ? 2 : 1);
		UT_ASSERT_EQ(episode_base_calls, 2);
		UT_ASSERT(cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &again, &diag));
		UT_ASSERT_EQ(memcmp(&terminal, &again, sizeof(terminal)), 0);
		UT_ASSERT(history->native_base == held && !history->failed);
		UT_ASSERT_EQ(ctrc_set_calls, acquires);
		UT_ASSERT_EQ(ctrc_set_release_calls | relmap_failed, 0);
		episode_terminal_done(&history, &inputs);
		UT_ASSERT_EQ(episode_base_releases, 1);
	}
}

UT_TEST(test_episode_terminal_rejects_lifecycle_discontinuity_before_new_native_io)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingTerminalV1 terminal, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		episode_base_reset();
		episode_terminal_begin(fault == 0 ? EPISODE_TERMINAL_ZERO : EPISODE_TERMINAL_TRUNCATE,
			&inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (fault == 2) { /* Unordered generation/overlap must not use numeric LSN order. */
			history->inventory->relmap_summary.clog_after_wal = 0;
			history->inventory->relmap_summary.clog_unordered = 1;
		}
		memset(&terminal, 0x5a, sizeof(terminal));
		UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
		UT_ASSERT_EQ(memcmp(&terminal, &zero, sizeof(terminal)), 0);
		UT_ASSERT_EQ(episode_base_calls | episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(history->failed && relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_terminal_missing_conflicting_child_prepare_or_alias_is_not_terminal)
{
	const EpisodeTerminalFixture modes[] = {EPISODE_TERMINAL_CONFLICT, EPISODE_TERMINAL_CHILD,
		EPISODE_TERMINAL_PREPARE, EPISODE_TERMINAL_ALIAS, EPISODE_TERMINAL_MISSING};
	for (unsigned i = 0; i < lengthof(modes); ++i) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingTerminalV1 terminal;
		ClusterRecoveryNativeInventoryDiag diag;
		episode_base_reset();
		episode_terminal_begin(modes[i], &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		UT_ASSERT(history->failed && relmap_failed > 0 && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_terminal_raw_status_or_side_failure_retains_observed_native_custody)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingBaseV1 base;
		ClusterRecoveryRelmapPendingTerminalV1 terminal, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		episode_base_reset();
		episode_terminal_begin(fault == 4 ? EPISODE_TERMINAL_SIDE_MISMATCH : EPISODE_TERMINAL_COMMIT,
			&inputs, &history, &pending);
		if (history == NULL) return;
		episode_terminal_status(fault < 2 ? fault == 0 ? TRANSACTION_STATUS_IN_PROGRESS
			: TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		held = history->native_base;
		if (fault == 2) rf_side_online_plan_destroy_v1(&tx_inputs_typed.side_plans[0]);
		if (fault == 3) tx_inputs_typed.side_capture_detail[0] = 1;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
		UT_ASSERT_EQ(memcmp(&terminal, &zero, sizeof(terminal)), 0);
		UT_ASSERT(history->failed && history->native_base == held && episode_base_live == 1);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
		UT_ASSERT_EQ(episode_base_releases, 1);
	}
}

static void
episode_terminal_recheck_error(void)
{
	ereport(ERROR, (errmsg("controlled complete terminal recheck ERROR")));
}

UT_TEST(test_episode_terminal_error_is_sticky_and_clears_only_transient_pass_state)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingTerminalV1 terminal, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	volatile bool caught = false;
	unsigned calls;

	episode_base_reset();
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
	if (history == NULL) return;
	episode_terminal_status(TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	inventory_census_calls = 0;
	inventory_between_passes = episode_terminal_recheck_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	inventory_between_passes = NULL;
	UT_ASSERT(caught && history->failed && history->native_base != NULL);
	UT_ASSERT(history->inventory->relmap_terminal == NULL);
	UT_ASSERT_EQ(memcmp(&terminal, &zero, sizeof(terminal)), 0);
	UT_ASSERT_EQ(episode_base_live, 1);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(relmap_failed > 0 && inputs_pin_held && ctrc_set_held == 2);
	calls = episode_base_calls;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	UT_ASSERT_EQ(episode_base_calls, calls);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_terminal_preconditions_preserve_original_base_and_query)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingTerminalV1 terminal;
	ClusterRecoveryNativeInventoryDiag diag;

	episode_base_reset();
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
	if (history == NULL) return;
	episode_terminal_status(TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending,
		(ClusterRecoveryRelmapPendingTerminalV1 *)history->inventory->sources, &diag));
	inventory_graph_caller_lock = true;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	inventory_graph_caller_lock = false;
	CritSectionCount++;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	CritSectionCount--;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(episode_base_calls | relmap_failed, 0);
	UT_ASSERT(!history->failed);
	UT_ASSERT(cluster_recovery_relmap_history_pending_terminal_v1(history, &pending, &terminal, &diag));
	episode_terminal_done(&history, &inputs);
}
