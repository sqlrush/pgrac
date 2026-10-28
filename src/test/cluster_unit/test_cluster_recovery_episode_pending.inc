/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_pending.inc
 *     Pending native folds under the original complete replay episode.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_pending.inc
 *
 * NOTES
 *     ROOT/claim/anchor/native WAL are actual inputs. Original Startup and
 *     distributed GES remain controlled leaves. No terminal authority.
 *-------------------------------------------------------------------------
 */
static WalTestRecord (*episode_pending_prefix_hook)(const ClusterWalSourceRef *, WalTestRecord);

static ClusterWalInputsV1 *
relmap_history_fixture(uint32 era, unsigned window_version, bool barriers)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	char path[MAXPGPATH];

	inputs_closed_wal_fixture(bytes, anchors);
	for (unsigned i = 0; i < 2; ++i) {
		ClusterRecoveryAnchorV2 *a = &anchors[i];
		ControlFileData native = { 0 };
		ClusterRelmapCheckpointWindow window = { 0 };
		ClusterWalSourceRef source = { 0 };
		WalTestRecord next;
		char map[TEST_RELMAP_LENGTH];
		int page = 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE;
		xl_clog_truncate truncate = { .pageno = 1, .oldestXact = 32768, .oldestXactDb = 5 };

		a->checkpoint_copy.nextXid = FullTransactionIdFromU64(((uint64)era << 32) | 4196000);
		native.state = a->state;
		native.checkPoint = a->checkpoint;
		native.checkPointCopy = a->checkpoint_copy;
		snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
				 "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix/current",
				 cluster_wal_threads_dir, a->identity.origin_thread_id,
				 a->identity.origin_owner_incarnation);
		v2_checkpoint_wal_record(&a->identity, &native, 8);
		window.system_identifier = a->identity.system_identifier;
		window.database_incarnation = a->database_incarnation;
		window.origin = a->identity.origin_node_id;
		window.admitted_incarnation = a->identity.origin_owner_incarnation;
		window.boot_incarnation = 17;
		window.epoch = 9;
		window.native_floor = ((uint64)era << 32) | 4195000;
		window.last_issued = ((uint64)era << 32) | 4195200;
		window.allocation_next = ((uint64)era << 32) | 4195300;
		/* Native CLOG/window supports origins0..63. Keep origin127 as a
		 * complete original source with NO invented native qualification. */
		if (window_version == 1 && i == 0)
			UT_ASSERT(cluster_relmap_checkpoint_window_encode_v1(&window, &a->checkpoint_copy,
																 a->checkpoint, test_checkpoint_end,
																 a->native_window));
		else if (window_version == 2 && i == 0)
			UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(
				&window, &a->checkpoint_copy, a->checkpoint, test_checkpoint_end,
				((uint64)era << 32) | 4195250, a->native_window));
		v2_anchor_object(bytes, a, &a->identity, path);
		UT_ASSERT_EQ(
			cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
		root.records[i == 0 ? 0 : 127].checkpoint_record_crc32c = test_checkpoint_crc;
		source.claim.identity = a->identity;
		source.timeline = a->checkpoint_copy.ThisTimeLineID;
		next = (WalTestRecord){ 1, test_checkpoint_end, a->checkpoint, test_checkpoint_crc };
		if (barriers) {
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		}
		if (episode_pending_prefix_hook != NULL)
			next = episode_pending_prefix_hook(&source, next);
		test_relmap_payload(map, i == 0, 2);
		next = inventory_write_record(&source, next.exclusive_end, next.record_start, RM_RELMAP_ID,
									  XLOG_RELMAP_UPDATE, map, sizeof(map));
		if (barriers) {
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
			next = inventory_write_record(&source, next.exclusive_end, next.record_start,
										  RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		}
		root.records[i == 0 ? 0 : 127].tail_last_record_lsn = next.record_start;
		root.records[i == 0 ? 0 : 127].tail_last_record_crc32c = next.record_crc;
		root.records[i == 0 ? 0 : 127].validated_tail_lsn_exclusive = next.exclusive_end;
		UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
		memcpy(bytes, root.bytes, sizeof(bytes));
	}
	v2_write_roots(bytes);
	test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	return inputs;
}

static void
episode_pending_begin(bool large, ClusterWalInputsV1 **inputs,
	ClusterRecoveryRelmapHistory **history, ClusterRelmapV2Image *pending)
{
	ClusterWalInputsV1 *raw = relmap_history_fixture(3, 2, true);
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterRecoveryNativeInventoryDiag diag;

	cluster_wal_inputs_release_v1(&raw);
	test_inputs_startup_cf = false;
	MyBackendType = B_BG_WORKER;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	test_actual_cf = test_cf_mode = NoLock;
	if (large) {
		ClusterWalSourceRef source = {0};
		ClusterControlRootSnapshot *snapshot = &root.records[0];
		WalTestRecord tail = {1, snapshot->validated_tail_lsn_exclusive,
			snapshot->tail_last_record_lsn, snapshot->tail_last_record_crc32c};
		source.claim.identity = snapshot->identity;
		source.timeline = snapshot->tail_tli;
		tail = inventory_write_relmap_fold_sequence(&source, tail, 0);
		snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
		snapshot->tail_last_record_lsn = tail.record_start;
		snapshot->tail_last_record_crc32c = tail.record_crc;
	}
	/* Original replay completion is a supplied fixture input. The original
	 * full-source native serial gate must still validate before every IR. */
	tx_ir_complete(&root.records[0]);
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
	inventory_census_calls = 0;
	inventory_between_passes = NULL;
	UT_ASSERT_EQ(episode_history_build(*inputs, 1024 * 1024, history, &diag), 0);
	UT_ASSERT(*history != NULL);
	if (*history != NULL)
		*pending = relmap_fold_pending((*history)->inventory, 0);
}

static ClusterRecoveryRelmapFoldResult
episode_pending_drain(ClusterRecoveryRelmapHistory *history,
	const ClusterRelmapV2Image *pending, ClusterRecoveryRelmapFoldSummary *summary)
{
	ClusterRecoveryRelmapFoldEntry entry, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	uint64 examined = history->inventory->diag.relmap_examined;
	for (unsigned i = 0; i < 1000; ++i) {
		ClusterRecoveryRelmapFoldResult result = cluster_recovery_relmap_history_pending_step_v1(
			history, pending, &entry, summary, &diag);
		UT_ASSERT(diag.relmap_examined - examined <= 128);
		examined = diag.relmap_examined;
		if (result == CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY) {
			ClusterRecoveryRelmapHistoryEntryV1 source;
			UT_ASSERT(entry.record != NULL && entry.history != NULL);
			UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, entry.source_index, &source));
			UT_ASSERT_EQ(memcmp(&source.source.selected, &entry.history->input,
				sizeof(entry.history->input)), 0);
		} else {
			UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
			if (result != CLUSTER_RECOVERY_RELMAP_FOLD_MORE)
				return result;
		}
	}
	return CLUSTER_RECOVERY_RELMAP_FOLD_INVALID;
}

UT_TEST(test_episode_pending_keeps_full_history_and_lifecycle_obligations)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	unsigned acquires;
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	acquires = ctrc_set_calls;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.sources, 2);
	UT_ASSERT_EQ(summary.origin_sources, 1);
	UT_ASSERT_EQ(summary.wal_matches, 1);
	UT_ASSERT_EQ(summary.clog_before_wal, 2);
	UT_ASSERT_EQ(summary.clog_after_wal, 2);
	UT_ASSERT_EQ(summary.clog_unordered, 0);
	UT_ASSERT_EQ(summary.owner_window_sources, 1);
	/* FullXid era is never inferred from native raw-XID correspondence. */
	pending.pending_owner.top_full_xid += UINT64_C(1) << 32;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.wal_matches, 1);
	UT_ASSERT_EQ(summary.query_window_sources | summary.owner_window_sources, 0);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(relmap_failed | ctrc_set_release_calls, 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_pending_bounds_ticks_and_rejects_changed_query_or_holder)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldEntry entry;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryRelmapFoldResult result;
		bool changed = false;
		uint64 examined = 0;
		episode_pending_begin(true, &inputs, &history, &pending);
		if (history == NULL) return;
		do {
			result = cluster_recovery_relmap_history_pending_step_v1(history, &pending,
				&entry, &summary, &diag);
			UT_ASSERT(diag.relmap_examined - examined <= 128);
			examined = diag.relmap_examined;
			if (result == CLUSTER_RECOVERY_RELMAP_FOLD_MORE && !changed) {
				changed = true;
				if (fault == 1) pending.pending_owner.boot_incarnation++;
				if (fault == 2) tx_inputs_serial++;
				if (fault == 3) relmap_sibling->release_uncertain = true;
			}
		} while (result <= CLUSTER_RECOVERY_RELMAP_FOLD_ENTRY);
		UT_ASSERT(changed);
		if (fault) {
			UT_ASSERT(result >= CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
			UT_ASSERT(relmap_failed > 0);
			UT_ASSERT(history->failed);
		} else {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
			UT_ASSERT_EQ(summary.child_references, 3);
		}
		UT_ASSERT_EQ(ctrc_set_release_calls, 0);
		if (relmap_sibling != NULL) relmap_sibling->release_uncertain = false;
		episode_history_done(&history, &inputs);
	}
}

UT_TEST(test_episode_pending_first_current_error_retains_original_protection)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldEntry entry, zero = {0};
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeInventoryDiag diag;
	volatile bool caught = false;
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	ctrc_replay_current_override = episode_history_current_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_step_v1(history, &pending,
			&entry, &summary, &diag);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false;
	ctrc_replay_current_override = ctrc_set_original_current;
	UT_ASSERT(caught && history->failed && relmap_failed > 0);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT_EQ(ctrc_set_release_calls, 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_pending_final_reread_drift_never_revives_the_query)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldEntry entry, zero = {0};
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeInventoryDiag diag;
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	episode_history_fault_inputs = inputs;
	episode_history_mutate_window();
	UT_ASSERT_NE(cluster_recovery_relmap_history_recheck_v1(history, &diag), 0);
	UT_ASSERT_EQ(cluster_recovery_relmap_history_pending_step_v1(history, &pending,
		&entry, &summary, &diag), CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED);
	UT_ASSERT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
	UT_ASSERT(relmap_failed > 0);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_pending_output_and_caller_lock_preconditions_preserve_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending, original;
	ClusterRecoveryRelmapFoldEntry entry;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryNativeInventoryDiag diag;
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	original = pending;
	UT_ASSERT_EQ(cluster_recovery_relmap_history_pending_step_v1(history, &pending,
		(ClusterRecoveryRelmapFoldEntry *)&pending, &summary, &diag), CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
	UT_ASSERT_EQ(memcmp(&pending, &original, sizeof(pending)), 0);
	test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_recovery_relmap_history_pending_step_v1(history, &pending,
		&entry, &summary, &diag), CLUSTER_RECOVERY_RELMAP_FOLD_INVALID);
	test_actual_cf = NoLock;
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	episode_history_done(&history, &inputs);
}
