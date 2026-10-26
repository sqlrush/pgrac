/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_clog_history.inc
 *     Complete original native CLOG lifecycle counter inputs.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_clog_history.inc
 * NOTES
 *     Original ROOT/WAL/native decoders run. Startup/GES remain controlled.
 *     Raw lifecycle events are not an era, retention or completion grant.
 *-------------------------------------------------------------------------
 */
static unsigned episode_clog_history_tail_count;
static bool episode_clog_history_mixed;

static WalTestRecord
episode_clog_history_tail(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	for (unsigned i = 0; i < episode_clog_history_tail_count; ++i) {
		int page = 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE + i + 1;
		if (episode_clog_history_mixed && i == 1) {
			xl_clog_truncate truncate = {17, 17 * CLUSTER_NATIVE_CLOG_PER_PAGE + 9, 606};
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_TRUNCATE, &truncate, sizeof(truncate));
		} else
			previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
				RM_CLOG_ID, CLOG_ZEROPAGE, &page, sizeof(page));
		previous = episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
	}
	return previous;
}

UT_TEST(test_episode_clog_history_keeps_every_original_zero_and_truncate)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;
	unsigned census, acquires;

	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	census = inventory_census_calls; acquires = ctrc_set_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.clog_count, 4);
	if (xid.clog_count == 4) for (uint32 i = 0; i < xid.clog_count; ++i) {
		const ClusterRecoveryRelmapClogEventV1 *event = &xid.clog[i];
		ClusterRecoveryRelmapHistoryEntryV1 source;
		UT_ASSERT(cluster_recovery_relmap_history_source_v1(history, event->source_index, &source));
		UT_ASSERT_EQ(source.source.selected.source.claim.identity.origin_node_id, pending.pending_owner.node);
		UT_ASSERT(event->counter_observed);
		UT_ASSERT_EQ(event->counter_before, xid.checkpoint_next);
		UT_ASSERT_EQ(event->counter_after, xid.checkpoint_next);
		UT_ASSERT(event->read_lsn < event->end_lsn && event->record_ordinal < xid.records);
		UT_ASSERT_EQ(event->source_index, xid.history.observation.source_index);
		UT_ASSERT_EQ(event->kind, i % 2 ? CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE : CLUSTER_RECOVERY_NATIVE_CLOG_ZERO);
		UT_ASSERT_EQ(event->page, i % 2 ? 1 : 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE);
		UT_ASSERT_EQ(event->oldest_xact, i % 2 ? 32768 : InvalidTransactionId);
		UT_ASSERT_EQ(event->oldest_database, i % 2 ? 5 : InvalidOid);
		if (i < 2) UT_ASSERT(event->end_lsn <= xid.map_start);
		else UT_ASSERT(event->read_lsn >= xid.map_end);
		if (i != 0) UT_ASSERT(event->record_ordinal > xid.clog[i-1].record_ordinal);
	}
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls | relmap_failed, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	episode_history_done(&history, &inputs);
}

UT_TEST(test_episode_clog_history_keeps_unrelated_page_and_raw_horizon)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingXidV1 xid;
	ClusterRecoveryNativeInventoryDiag diag;

	episode_base_reset();
	episode_clog_history_tail_count = 3; episode_clog_history_mixed = true;
	episode_terminal_tail_hook = episode_clog_history_tail;
	episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
	episode_terminal_tail_hook = NULL;
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(summary.clog_zeroes, 0);
	UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
	UT_ASSERT_EQ(xid.clog_count, 3);
	if (xid.clog_count == 3) {
		UT_ASSERT_EQ(xid.clog[0].page, 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE + 1);
		UT_ASSERT_EQ(xid.clog[1].kind, CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE);
		UT_ASSERT_EQ(xid.clog[1].page, 17);
		UT_ASSERT_EQ(xid.clog[1].oldest_xact, 17 * CLUSTER_NATIVE_CLOG_PER_PAGE + 9);
		UT_ASSERT_EQ(xid.clog[1].oldest_database, 606);
		UT_ASSERT_EQ(xid.clog[2].page, 4195136 / CLUSTER_NATIVE_CLOG_PER_PAGE + 3);
		for (uint32 i = 0; i < xid.clog_count; ++i) {
			UT_ASSERT(xid.clog[i].counter_observed && xid.clog[i].read_lsn >= xid.map_end);
			UT_ASSERT_EQ(xid.clog[i].counter_before, xid.tail_next);
			UT_ASSERT_EQ(xid.clog[i].counter_after, xid.tail_next);
		}
	}
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls | relmap_failed, 0);
	episode_terminal_done(&history, &inputs);
}

UT_TEST(test_episode_clog_history_capacity_refuses_without_partial_output)
{
	for (unsigned extra = 0; extra < 2; ++extra) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		unsigned census;
		episode_base_reset();
		episode_clog_history_tail_count = CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX + extra;
		episode_clog_history_mixed = false;
		episode_terminal_tail_hook = episode_clog_history_tail;
		episode_terminal_begin(EPISODE_TERMINAL_COMMIT, &inputs, &history, &pending);
		episode_terminal_tail_hook = NULL;
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (extra == 0) {
			UT_ASSERT(cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(xid.clog_count, CLUSTER_RECOVERY_RELMAP_CLOG_HISTORY_MAX);
		} else {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
			UT_ASSERT(history->failed && relmap_failed != 0);
			census = inventory_census_calls;
			UT_ASSERT(!cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag));
			UT_ASSERT_EQ(inventory_census_calls, census);
		}
		UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

/* Branch-level coverage only: use the original immutable native scans, but
 * deliberately vary the private counter pass's selector/redo. These are not
 * fabricated generation-ownership or FullXid qualification witnesses. */
UT_TEST(test_episode_clog_history_prefix_and_other_generation_remain_unobserved)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryNativeInventory local;
	NativeInventoryRelmapXid pass = {0};
	ClusterRecoveryRelmapPendingXidV1 output = {0};
	unsigned seen = 0;
	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	local = *history->inventory;
	local.relmap_xid = &pass; local.relmap_query = pending; pass.result = &output;
	for (const NativeInventoryRecord *saved = local.head; saved != NULL; saved = saved->next) {
		ClusterRecoveryNativeEffect effect;
		DecodedXLogRecord record = {0};
		XLogReaderState reader = {0};
		if (cluster_recovery_native_scan_primary(saved->scan, &effect) != CLUSTER_RECOVERY_NATIVE_EFFECT
			|| effect.kind != CLUSTER_RECOVERY_NATIVE_CLOG_ZERO) continue;
		local.source_cursor = saved->source_index;
		record.header = effect.record->header;
		reader.record = &record; reader.ReadRecPtr = effect.record->read_lsn; reader.EndRecPtr = effect.record->end_lsn;
		for (unsigned mode = 0; mode < 2; ++mode) {
			bool same = effect.record->source.claim.identity.origin_node_id == pending.pending_owner.node;
			memset(&output, 0, sizeof(output));
			pass.source_index = mode == 0 ? UINT32_MAX : saved->source_index;
			pass.source.native_redo = reader.EndRecPtr;
			UT_ASSERT(inventory_relmap_xid_record(&local, &reader, saved->ordinal, saved));
			UT_ASSERT_EQ(output.clog_count, same ? 1 : 0);
			if (same && output.clog_count == 1) {
				UT_ASSERT(!output.clog[0].counter_observed);
				UT_ASSERT_EQ(output.clog[0].counter_before | output.clog[0].counter_after, 0);
				UT_ASSERT_EQ(output.clog[0].source_index, saved->source_index);
				UT_ASSERT_EQ(output.clog[0].record_ordinal, saved->ordinal);
				UT_ASSERT_EQ(output.clog[0].record_crc, (uint32)effect.record->header.xl_crc);
			}
		}
		seen++;
	}
	UT_ASSERT_EQ(seen, 4); /* two ZERO records per original physical origin */
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(episode_base_calls | ctrc_set_release_calls, 0);
	episode_history_done(&history, &inputs);
}

static ClusterRecoveryRelmapHistory *episode_clog_history_error_handle;
static unsigned episode_clog_history_entered_count;
static bool
episode_clog_history_current_error(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	NativeInventoryRelmapXid *pass = episode_clog_history_error_handle->inventory->relmap_xid;
	if (pass != NULL && pass->result != NULL && pass->result->clog_count != 0) {
		episode_clog_history_entered_count = pass->result->clog_count;
		ereport(ERROR, (errmsg("controlled original current ERROR after CLOG collection")));
	}
	return ctrc_set_original_current(start, work, index, scope, request);
}

UT_TEST(test_episode_clog_history_late_error_discards_output_keeps_original_custody)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryRelmapPendingXidV1 xid, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	volatile bool caught = false;
	unsigned reads;
	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base; reads = episode_base_calls;
	episode_clog_history_error_handle = history; episode_clog_history_entered_count = 0;
	ctrc_replay_current_override = episode_clog_history_current_error;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_recovery_relmap_history_pending_xid_v1(history, &pending, &xid, &diag);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_capture_error_level = false;
	ctrc_replay_current_override = ctrc_set_original_current;
	episode_clog_history_error_handle = NULL;
	UT_ASSERT(caught && episode_clog_history_entered_count != 0);
	UT_ASSERT_EQ(memcmp(&xid, &zero, sizeof(xid)), 0);
	UT_ASSERT(history->failed && relmap_failed != 0 && history->inventory->relmap_xid == NULL);
	UT_ASSERT(history->native_base == held && held != NULL && episode_base_live == 1);
	UT_ASSERT_EQ(episode_base_calls, reads);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	episode_history_done(&history, &inputs);
}
