/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_unchanged_custody.inc
 *     Original Start retains unchanged targets through final readback.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Actual extracted Start/drain; PAGE, native FD and read transport are leaves.
 *-------------------------------------------------------------------------
 */
static unsigned unchanged_readback_calls, unchanged_readback_mode;
static bool unchanged_cleanup_observed, unchanged_readback_entered;
static bool unchanged_baseline_drift;
static const RfPageCtrcUnchangedV1 *unchanged_observed;

RfPageCtrcHeldTargetV1
rf_page_recovery_ctrc_replays_itl_graph_hold_target_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const ClusterColdCtrcWorkV1 *work,
	const ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const ClusterSpaceIdentity *space, BlockNumber block, const RfPageVersionV1 *before,
	const char canonical[BLCKSZ], const ClusterRecoveryUndoEntry *physical, Size count, Size budget)
{
	RfPageCtrcHeldTargetV1 held = {0};

	held.step = rf_page_recovery_ctrc_replays_itl_graph_hold_smgr_v1(start, work, source,
		authority, space, block, before, canonical, physical, count, budget);
	if (held.step.result.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED && !pending_storage_missing) {
		UT_ASSERT(pending_storage_created < 2);
		pending_storage_mask |= 1U << pending_storage_created;
		held.step.storage = pending_storage_created == 0 ? (void *)&pending_storage_token
			: (void *)&pending_storage_token_second;
		pending_storage_live = true; pending_storage_created++;
		held.unchanged.ready = true; held.unchanged.space = *space;
		held.unchanged.before = *before; held.unchanged.blockno = block;
		memcpy(held.unchanged.page.data, canonical, BLCKSZ);
		held.unchanged.space0.data[100] = 31; held.unchanged.space1.data[100] = 47;
	}
	return held;
}

RfPageProofDetailV1
rf_page_recovery_ctrc_replays_unchanged_readback_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const RfPageCtrcUnchangedV1 *unchanged,
	ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
	const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	uint32 index = unchanged->blockno - 23;

	UT_ASSERT(start == info.cluster_tx_start && work == start->ctrc_observation_work);
	UT_ASSERT(source == &start->ctrc_page_sources[prefix_source_ordinal]);
	UT_ASSERT(authority == (RfPageCtrcAuthorityV1 *)&prefix_page_token);
	UT_ASSERT(unchanged->ready && index < 2 && budget > 0 && count == work->counts.subject_count);
	UT_ASSERT(!progress->pending && progress->next_target == 2);
	UT_ASSERT(storage == progress->completed[index].storage && pending_storage_mask == 3);
	UT_ASSERT_EQ(progress->completed[index].result.phase, RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED);
	UT_ASSERT(memcmp(unchanged->page.data, prefix_images[index].data, BLCKSZ) == 0);
	UT_ASSERT_EQ(unchanged->space0.data[100], 31); UT_ASSERT_EQ(unchanged->space1.data[100], 47);
	unchanged_readback_calls++; unchanged_readback_entered = true;
	unchanged_observed = unchanged;
	if (unchanged_readback_mode == 1) return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
	if (unchanged_readback_mode == 2) pg_re_throw();
	if (unchanged_readback_mode == 3 && unchanged_readback_calls < 3) {
		ctrc_driver_waiting = true;
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
unchanged_custody_ready(void)
{
	retirement_ready(); prefix_unchanged = true; prefix_legacy_unchanged = false;
	unchanged_readback_calls = unchanged_readback_mode = 0;
	unchanged_cleanup_observed = unchanged_readback_entered = false;
	unchanged_baseline_drift = false; unchanged_observed = NULL;
}

UT_TEST(test_startup_unchanged_targets_keep_fds_and_full_readback_before_service)
{
	bool held;
	unchanged_custody_ready(); retirement_run_startup();
	held = pending_storage_mask == 3 && observation_scope_live && observation_cursor_live;
	retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached && held);
	UT_ASSERT_EQ(unchanged_readback_calls, 2); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(prefix_records + pending_readback_calls, 0);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_startup_unchanged_readback_refusal_or_error_keeps_original_cleanup)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		bool held;
		unchanged_custody_ready(); unchanged_readback_mode = mode; retirement_run_startup();
		held = pending_storage_mask == 3 && observation_scope_live && observation_cursor_live;
		retirement_local_cleanup();
		UT_ASSERT(!retirement_service_reached && held);
		UT_ASSERT_EQ(unchanged_readback_calls, 1); UT_ASSERT_EQ(prefix_records, 0);
		UT_ASSERT_EQ(pending_storage_closes, 2);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_startup_unchanged_wait_reuses_same_saved_targets_without_wal)
{
	unchanged_custody_ready(); unchanged_readback_mode = 3; retirement_run_startup();
	retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(unchanged_readback_calls, 4); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(prefix_records, 0); UT_ASSERT_EQ(pending_storage_created, 2);
	UT_ASSERT_EQ(pending_storage_closes, 2);
}

static void
unchanged_custody_cleanup_probe(void)
{
	const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	if (progress->next_target < 2 && pending_storage_live)
		unchanged_cleanup_observed = !progress->pending
			&& progress->completed[progress->next_target].storage != NULL
			&& progress->completed[progress->next_target].result.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED;
}

UT_TEST(test_startup_unchanged_storage_is_saved_before_page_cleanup)
{
	unchanged_custody_ready(); prefix_destroy_callback = unchanged_custody_cleanup_probe;
	retirement_run_startup(); retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached && unchanged_cleanup_observed);
	UT_ASSERT_EQ(pending_storage_closes, 2); UT_ASSERT_EQ(prefix_records, 0);
}

static void
unchanged_custody_late_drift(void)
{
	if (unchanged_readback_entered) {
		if (unchanged_baseline_drift)
			((RfPageCtrcUnchangedV1 *)unchanged_observed)->space0.data[100]++;
		else cluster_recovery_cold_start->ctrc_page_progress->completed[0].result.phase = RF_PAGE_CTRC_STEP_REFUSED;
	}
}

UT_TEST(test_startup_unchanged_late_saved_result_drift_prevents_service)
{
	for (unsigned baseline = 0; baseline < 2; baseline++) {
		bool held;
		unchanged_custody_ready(); prefix_destroy_callback = unchanged_custody_late_drift;
		unchanged_baseline_drift = baseline != 0;
		retirement_run_startup(); held = pending_storage_mask == 3 && observation_scope_live;
		retirement_local_cleanup();
		UT_ASSERT(!retirement_service_reached && held);
		UT_ASSERT_EQ(unchanged_readback_calls, 1); UT_ASSERT_EQ(pending_storage_closes, 2);
		UT_ASSERT_EQ(prefix_records, 0);
	}
}

UT_TEST(test_startup_unchanged_binding_is_original_start_slot_and_same_fd)
{
	ClusterColdCtrcPageProgressV1 *progress;
	const RfPageCtrcUnchangedV1 *original;
	RfPageCtrcUnchangedV1 clone;
	ClusterSharedFsPageV1 *storage;
	const ClusterColdCtrcWorkV1 *work;

	unchanged_custody_ready(); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	progress = cluster_recovery_cold_start->ctrc_page_progress;
	original = &progress->completed[0].unchanged; clone = *original;
	storage = progress->completed[0].storage; work = cluster_recovery_cold_start->ctrc_observation_work;
	UT_ASSERT(ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work, original, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work, &clone, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work, original,
		progress->completed[1].storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent((const void *)(uintptr_t)1, work, original, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, NULL, original, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work,
		(const void *)(uintptr_t)1, storage));
	progress->pending = true;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work, original, storage));
	progress->pending = false;
	retirement_local_cleanup();
	UT_ASSERT(!ClusterColdRecoveryCtrcPageUnchangedCurrent(info.cluster_tx_start, work, original, storage));
	UT_ASSERT_EQ(pending_storage_closes, 2);
}
