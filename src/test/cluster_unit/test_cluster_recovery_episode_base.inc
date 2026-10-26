/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_base.inc
 *     Same-episode pending query and original native base custody.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_base.inc
 * NOTES
 *     Original ROOT/claim/anchor/WAL/history readers execute. Only the native
 *     CLOG base leaf below is controlled, defaults to refusal and retains
 *     exact source/query/full-page identity. Actual native fd/SLRU behavior
 *     remains separately tested by test_cluster_clog_recovery. No lifetime,
 *     terminal or completion qualification is supplied by this composition.
 *-------------------------------------------------------------------------
 */
struct ClusterRelmapClogBaseHandleV1 {
	ClusterRecoveryRelmapSourceV1 source;
	ClusterRelmapClogBaseV1 base;
	bool failed;
};
static bool episode_base_enabled, episode_base_refuse, episode_base_error;
static unsigned episode_base_calls, episode_base_live, episode_base_releases;
static uint8 episode_base_page[BLCKSZ];
static void (*episode_base_after_read)(void);

size_t
cluster_clog_relmap_base_custody_bytes_v1(void)
{
	return sizeof(ClusterRelmapClogBaseHandleV1);
}

bool
cluster_clog_relmap_base_read_v1(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 query, ClusterRelmapClogBaseV1 *out, ClusterRelmapClogBaseHandleV1 **custody)
{
	ClusterRelmapClogBaseHandleV1 *held = *custody;
	ClusterRelmapClogBaseV1 base = {0};
	bool existing = held != NULL;

	episode_base_calls++;
	if (!episode_base_enabled || (existing && held->failed))
		return false;
	if (existing)
		held->failed = true;
	if (episode_base_error)
		ereport(ERROR, (errmsg("controlled original native base ERROR")));
	if (episode_base_refuse || !cluster_wal_inputs_relmap_source_current_v1(source))
		return false;
	base.queried_full_xid = query;
	base.origin = source->selected.source.claim.identity.origin_node_id;
	base.page_number = (uint32)query / CLUSTER_NATIVE_CLOG_PER_PAGE;
	base.device = 17; base.inode = 29; base.size = BLCKSZ * 32;
	memcpy(base.image, episode_base_page, BLCKSZ);
	base.status = (base.image[((uint32)query % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4]
		>> (((uint32)query % 4) * 2)) & 3;
	if (existing && (memcmp(source, &held->source, sizeof(*source)) != 0
		|| memcmp(&base, &held->base, sizeof(base)) != 0))
		return false;
	if (!existing) {
		held = calloc(1, sizeof(*held));
		UT_ASSERT(held != NULL);
		if (held == NULL) return false;
		held->source = *source;
		held->base = base;
		episode_base_live++;
	}
	held->failed = false;
	*custody = held;
	*out = base;
	if (episode_base_after_read != NULL)
		episode_base_after_read();
	return true;
}

void
cluster_clog_relmap_base_release_v1(ClusterRelmapClogBaseHandleV1 **custody)
{
	if (custody != NULL && *custody != NULL) {
		free(*custody);
		*custody = NULL;
		episode_base_live--;
		episode_base_releases++;
	}
}

static void
episode_base_reset(void)
{
	UT_ASSERT_EQ(episode_base_live, 0);
	episode_base_enabled = true;
	episode_base_refuse = episode_base_error = false;
	episode_base_calls = episode_base_releases = 0;
	episode_base_after_read = NULL;
	memset(episode_base_page, 0, sizeof(episode_base_page));
	episode_base_page[BLCKSZ - 1] = 0x31;
}

UT_TEST(test_episode_base_requires_drain_and_retains_actual_query_source)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base, again, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRelmapClogBaseHandleV1 *held;
	size_t before;
	unsigned acquires;

	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	before = history->inventory->diag.owned_bytes + sizeof(*history);
	acquires = ctrc_set_calls;
	memset(&base, 0x5a, sizeof(base));
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
	UT_ASSERT_EQ(episode_base_calls, 0);
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	held = history->native_base;
	UT_ASSERT(held != NULL && episode_base_live == 1);
	UT_ASSERT_EQ(diag.owned_bytes, before + cluster_clog_relmap_base_custody_bytes_v1());
	UT_ASSERT_EQ(base.native.queried_full_xid, pending.pending_owner.top_full_xid);
	UT_ASSERT_EQ(base.native.status, TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(base.native.page_number, (uint32)pending.pending_owner.top_full_xid / CLUSTER_NATIVE_CLOG_PER_PAGE);
	UT_ASSERT_EQ(base.native.image[BLCKSZ - 1], 0x31);
	UT_ASSERT_EQ(base.native.device, 17);
	UT_ASSERT_EQ(base.native.inode, 29);
	UT_ASSERT_EQ(base.fold.wal_matches, 1);
	UT_ASSERT_EQ(base.fold.sources, 2);
	UT_ASSERT_EQ(base.fold.clog_after_wal, 2); /* remains an obligation */
	UT_ASSERT_EQ(memcmp(&base.history.source, &held->source, sizeof(held->source)), 0);
	UT_ASSERT(base.history.source.scope == inputs && base.history.window_observed);
	UT_ASSERT_EQ(base.history.window.epoch, pending.pending_owner.membership_epoch);
	UT_ASSERT_EQ(base.history.post_flush_issued, ((uint64)3 << 32) | 4195250);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &again, &diag));
	UT_ASSERT(history->native_base == held);
	UT_ASSERT_EQ(memcmp(&base, &again, sizeof(base)), 0);
	UT_ASSERT_EQ(diag.owned_bytes, before + cluster_clog_relmap_base_custody_bytes_v1());
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	UT_ASSERT_EQ(memcmp(&base.fold, &summary, sizeof(summary)), 0);
	UT_ASSERT_EQ(episode_base_calls, 2);
	UT_ASSERT_EQ(ctrc_set_calls, acquires);
	UT_ASSERT_EQ(ctrc_set_release_calls | relmap_failed, 0);
	episode_history_done(&history, &inputs);
	UT_ASSERT_EQ(episode_base_live, 0);
	UT_ASSERT_EQ(episode_base_releases, 1);
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_rejects_unqualified_or_ambiguous_native_source)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		episode_base_reset();
		episode_pending_begin(false, &inputs, &history, &pending);
		if (history == NULL) return;
		/* Full era or boot changes still raw-match, but do not match the
		 * actual original v2 query/owner window. Never invoke base I/O. */
		if (fault == 0) pending.pending_owner.top_full_xid += UINT64_C(1) << 32;
		if (fault == 1) pending.pending_owner.boot_incarnation++;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (fault == 2) history->inventory->relmap_summary.wal_matches++;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
		UT_ASSERT_EQ(episode_base_calls, 0);
		UT_ASSERT(history->failed && relmap_failed > 0 && ctrc_set_release_calls == 0);
		episode_history_done(&history, &inputs);
	}
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_bound_query_cannot_restart_or_replace_custody)
{
	for (unsigned operation = 0; operation < 3; ++operation) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldEntry entry;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		episode_base_reset();
		episode_pending_begin(false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		held = history->native_base;
		if (operation == 0) pending.pending_owner.boot_incarnation++;
		else if (operation == 1) pending.pending_owner.top_full_xid++;
		else {
			pending.committed.mappings[0].mapfilenumber++;
			INIT_CRC32C(pending.committed.crc);
			COMP_CRC32C(pending.committed.crc, &pending.committed, offsetof(RelMapFile, crc));
			FIN_CRC32C(pending.committed.crc);
		}
		if (operation == 0)
			UT_ASSERT_EQ(cluster_recovery_relmap_history_pending_step_v1(history, &pending, &entry,
				&summary, &diag), CLUSTER_RECOVERY_RELMAP_FOLD_CHANGED);
		else {
			UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
			UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
		}
		UT_ASSERT(history->failed && history->native_base == held && relmap_failed > 0);
		UT_ASSERT_EQ(episode_base_calls, 1);
		UT_ASSERT_EQ(episode_base_live, 1);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		episode_history_done(&history, &inputs);
		UT_ASSERT_EQ(episode_base_releases, 1);
	}
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_final_physical_reread_keeps_the_original_baseline)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		episode_base_reset();
		episode_pending_begin(false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		held = history->native_base;
		UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(episode_base_calls, 2);
		if (fault == 0) episode_base_page[BLCKSZ - 1] ^= 1; /* unrelated byte */
		else episode_base_refuse = true;
		UT_ASSERT_EQ(cluster_recovery_relmap_history_recheck_v1(history, &diag), CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		UT_ASSERT(history->failed && history->native_base == held && relmap_failed > 0);
		episode_base_page[BLCKSZ - 1] ^= (fault == 0);
		episode_base_refuse = false;
		UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
		UT_ASSERT_EQ(episode_base_calls, 3);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		episode_history_done(&history, &inputs);
		UT_ASSERT_EQ(episode_base_releases, 1);
	}
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_first_and_existing_errors_never_drop_protection)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		volatile bool caught = false;
		episode_base_reset();
		episode_pending_begin(false, &inputs, &history, &pending);
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		if (fault == 2)
			UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
		if (fault == 0) ctrc_replay_current_override = episode_history_current_error;
		else episode_base_error = true;
		memset(&base, 0x5a, sizeof(base));
		test_capture_error_level = true;
		PG_TRY(); {
			(void)cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		test_capture_error_level = false;
		ctrc_replay_current_override = ctrc_set_original_current;
		UT_ASSERT(caught && history->failed && relmap_failed > 0);
		UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
		UT_ASSERT_EQ(episode_base_live, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		episode_history_done(&history, &inputs);
	}
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_after_read_source_drift_discards_only_local_output)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	episode_history_fault_inputs = inputs;
	episode_base_after_read = episode_history_mutate_window;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	UT_ASSERT_EQ(memcmp(&base, &zero, sizeof(base)), 0);
	UT_ASSERT(history->failed && history->native_base != NULL && relmap_failed > 0);
	UT_ASSERT_EQ(episode_base_live, 1);
	UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
	episode_history_done(&history, &inputs);
	UT_ASSERT_EQ(episode_base_releases, 1);
	episode_base_enabled = false;
}

UT_TEST(test_episode_base_output_lock_and_budget_preconditions_do_not_poison)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL, *too_small = NULL;
	ClusterRelmapV2Image pending, saved;
	ClusterRecoveryRelmapFoldSummary summary;
	ClusterRecoveryRelmapPendingBaseV1 base;
	ClusterRecoveryNativeInventoryDiag diag;
	episode_base_reset();
	episode_pending_begin(false, &inputs, &history, &pending);
	if (history == NULL) return;
	UT_ASSERT_EQ(episode_history_build(inputs,
		sizeof(*history) + cluster_clog_relmap_base_custody_bytes_v1() - 1, &too_small, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY);
	UT_ASSERT(too_small == NULL);
	UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
	saved = pending;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending,
		(ClusterRecoveryRelmapPendingBaseV1 *)&pending, &diag));
	UT_ASSERT_EQ(memcmp(&pending, &saved, sizeof(saved)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending,
		(ClusterRecoveryRelmapPendingBaseV1 *)&history->inventory->relmap_query, &diag));
	UT_ASSERT_EQ(memcmp(&history->inventory->relmap_query, &saved, sizeof(saved)), 0);
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base,
		(ClusterRecoveryNativeInventoryDiag *)history->inventory->head->scan));
	test_actual_cf = ShareLock;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	test_actual_cf = NoLock;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	CritSectionCount = 0;
	UT_ASSERT_EQ(episode_base_calls | relmap_failed, 0);
	UT_ASSERT(!history->failed);
	UT_ASSERT(cluster_recovery_relmap_history_pending_base_v1(history, &pending, &base, &diag));
	episode_history_done(&history, &inputs);
	UT_ASSERT_EQ(episode_base_releases, 1);
	episode_base_enabled = false;
}
