/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_completed_readback.inc
 *     Original Startup final readback of retained completed PAGE results.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_completed_readback.inc
 *
 * NOTES
 *     Actual Start/F4/drain/service boundary; PAGE/file transport controlled.
 *-------------------------------------------------------------------------
 */
static unsigned completed_readback_calls, completed_readback_mode;
static bool completed_readback_entered;

extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_completed_readback_v1(
	const struct ClusterRecoveryStartV1 *, const ClusterColdCtrcWorkV1 *,
	const ClusterRecoveryCtrcReplaySetSourceV1 *, RfPageCtrcAuthorityV1 *,
	const RfPageCtrcFpiInsertV1 *, ClusterSharedFsPageV1 *, Size, Size);

RfPageProofDetailV1
rf_page_recovery_ctrc_replays_completed_readback_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
	ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
	const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	uint32 index = inserted->image.block_number - 23;

	UT_ASSERT(start == info.cluster_tx_start && work == start->ctrc_observation_work);
	UT_ASSERT(source == &start->ctrc_page_sources[prefix_source_ordinal]);
	UT_ASSERT(authority == (RfPageCtrcAuthorityV1 *)&prefix_page_token);
	UT_ASSERT(!progress->pending && progress->next_target == prefix_count && index < prefix_count);
	UT_ASSERT(inserted == &progress->completed[index].result.inserted);
	UT_ASSERT(storage == progress->completed[index].storage
		&& pending_storage_mask == (1U << prefix_count) - 1);
	UT_ASSERT_EQ(count, work->counts.subject_count); UT_ASSERT(budget > 0);
	UT_ASSERT_EQ(prefix_steps, prefix_count + (prefix_wait ? 1 : 0));
	UT_ASSERT_EQ(cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.phase,
		CLUSTER_RECOVERY_STAMP_EMPTY);
	completed_readback_calls++; completed_readback_entered = true;
	if (completed_readback_mode == 1 || (completed_readback_mode == 2 && index == 1))
		return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
	if (completed_readback_mode == 3) pg_re_throw();
	if (completed_readback_mode == 4 && completed_readback_calls < 3) {
		ctrc_driver_waiting = true;
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
completed_readback_ready(void)
{
	retirement_ready();
	completed_readback_calls = completed_readback_mode = 0;
	completed_readback_entered = false;
}

UT_TEST(test_startup_completed_pages_are_reread_before_independent_service)
{
	bool held, unaccepted;
	completed_readback_ready(); retirement_run_startup();
	held = pending_storage_mask == 3 && observation_scope_live && observation_cursor_live;
	unaccepted = window_region.checkpoint_owner.ctrc_accepted_generation == 0;
	retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached && held && unaccepted);
	UT_ASSERT_EQ(completed_readback_calls, 2);
	UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_readback_calls, 2);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_completed_page_refusal_keeps_all_fds_and_shared_debt)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		bool held;
		completed_readback_ready(); completed_readback_mode = mode; retirement_run_startup();
		held = pending_storage_mask == 3 && observation_scope_live && observation_cursor_live;
		retirement_local_cleanup();
		UT_ASSERT(!retirement_service_reached && held);
		UT_ASSERT_EQ(completed_readback_calls, mode);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_startup_completed_page_error_preserves_original_cleanup)
{
	bool held;
	completed_readback_ready(); completed_readback_mode = 3; retirement_run_startup();
	held = pending_storage_mask == 3 && observation_scope_live && observation_cursor_live;
	retirement_local_cleanup();
	UT_ASSERT(!retirement_service_reached && held);
	UT_ASSERT_EQ(completed_readback_calls, 1); UT_ASSERT_EQ(pending_storage_closes, 2);
	UT_ASSERT_EQ(prefix_steps, 2);
}

UT_TEST(test_startup_completed_page_wait_reuses_original_result_and_fds)
{
	completed_readback_ready(); completed_readback_mode = 4; retirement_run_startup();
	retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(completed_readback_calls, 4); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
}

static void
completed_readback_cleanup_drift(void)
{
	if (completed_readback_entered)
		cluster_recovery_cold_start->ctrc_page_progress->completed[0].result.inserted.image.image.data[100]++;
}

UT_TEST(test_startup_completed_result_drift_after_page_cleanup_prevents_service)
{
	bool held;
	completed_readback_ready(); prefix_destroy_callback = completed_readback_cleanup_drift;
	retirement_run_startup();
	held = pending_storage_mask == 3 && observation_scope_live;
	retirement_local_cleanup();
	UT_ASSERT(!retirement_service_reached && held);
	UT_ASSERT_EQ(completed_readback_calls, 1); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_completed_binding_refuses_copies_other_fds_and_retired_observation)
{
	ClusterColdCtrcPageProgressV1 *progress;
	RfPageCtrcFpiInsertV1 clone;
	const RfPageCtrcFpiInsertV1 *original;
	ClusterSharedFsPageV1 *storage;
	const ClusterColdCtrcWorkV1 *work;

	completed_readback_ready(); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	progress = cluster_recovery_cold_start->ctrc_page_progress;
	original = &progress->completed[0].result.inserted; clone = *original;
	storage = progress->completed[0].storage; work = cluster_recovery_cold_start->ctrc_observation_work;
	UT_ASSERT(ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, work, original, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, work, &clone, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, work, original,
		progress->completed[1].storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, NULL, original, storage));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent((const struct ClusterRecoveryStartV1 *)(uintptr_t)1,
		work, original, storage));
	progress->pending = true;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, work, original, storage));
	progress->pending = false;
	retirement_local_cleanup();
	UT_ASSERT(!ClusterColdRecoveryCtrcPageCompletedCurrent(info.cluster_tx_start, work, original, storage));
	UT_ASSERT_EQ(pending_storage_closes, 2);
}
