/* Original Startup continuation; native PAGE/readback is a controlled leaf.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned pending_readback_mode, pending_readback_cleanups;

static void
pending_readback_reset(void)
{
	pending_readback_calls = pending_readback_mode = pending_readback_cleanups = 0;
	startup_local_empty_reset();
}

RfPageProofDetailV1
rf_page_recovery_ctrc_replays_fpi_readback_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
	ClusterSharedFsPageV1 *storage, Size count, Size budget)
{
	const ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	UT_ASSERT(start == info.cluster_tx_start && work == start->ctrc_observation_work);
	UT_ASSERT(source == &start->ctrc_page_sources[prefix_source_ordinal]);
	UT_ASSERT(authority == (RfPageCtrcAuthorityV1 *)&prefix_page_token);
	UT_ASSERT(progress->pending && inserted == &progress->result.inserted);
	UT_ASSERT(storage == progress->storage && pending_storage_live);
	UT_ASSERT_EQ(count, work->counts.subject_count); UT_ASSERT(budget > 0);
	UT_ASSERT_EQ(prefix_steps, prefix_wait ? 2 : 1); UT_ASSERT_EQ(progress->next_target, 0);
	pending_readback_calls++;
	if (pending_readback_mode == 1) return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	if (pending_readback_mode == 2) pg_re_throw();
	if (pending_readback_mode == 3 && pending_readback_calls < 3) {
		ctrc_driver_waiting = true;
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
pending_readback_cleanup_drift(void)
{
	if (++pending_readback_cleanups == 2)
		cluster_recovery_cold_start->ctrc_page_progress->result.inserted.image.image.data[100]++;
}

UT_TEST(test_startup_pending_readback_uses_saved_operation_before_service)
{
	prefix_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(pending_readback_calls, 1); UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
	UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
	UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&prefix_window,
		prefix_window.owner.writer.startup_first_lsn + 40000,
		prefix_window.owner.writer.startup_first_lsn + 40128));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_readback_refusal_retains_fd_window_and_graph)
{
	prefix_ready(); pending_readback_mode = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(pending_readback_calls, 1); UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT(pending_storage_live && page_graph_live && observation_scope_live);
	UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_readback_collision_retries_without_reinserting)
{
	prefix_ready(); pending_readback_mode = 3;
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(pending_readback_calls, 3); UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT_EQ(pending_storage_created, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT(!cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.failed);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_readback_error_retains_original_cleanup_duty)
{
	volatile bool caught = false;
	prefix_ready(); pending_readback_mode = 2;
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(pending_readback_calls, 1);
	UT_ASSERT(pending_storage_live); UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_readback_cleanup_cannot_change_saved_operation)
{
	prefix_ready(); prefix_destroy_callback = pending_readback_cleanup_drift;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(pending_readback_calls, 1); UT_ASSERT_EQ(pending_readback_cleanups, 2);
	UT_ASSERT(pending_storage_live); pending_storage_abandon();
}

UT_TEST(test_startup_pending_readback_repeats_protection_without_new_operation)
{
	prefix_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(pending_readback_calls, 2); UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT_EQ(pending_storage_created, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}
