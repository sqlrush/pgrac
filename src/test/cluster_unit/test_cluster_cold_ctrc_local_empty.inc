/* Original Startup branch; the local native directory is an independent leaf.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned local_empty_calls, local_empty_mode;
static unsigned final_local_empty_calls, final_local_empty_mode;

static void
startup_local_empty_reset(void)
{
	local_empty_calls = local_empty_mode = 0;
	final_local_empty_calls = final_local_empty_mode = 0;
}

ClusterCtrcLocalEmptyV1
cluster_ctrc_recovery_local_empty_v1(void)
{
	ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	if (progress == NULL || !progress->pending) {
		UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
		UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
		UT_ASSERT(progress == NULL || progress->next_target == progress->target_count);
		final_local_empty_calls++;
		if (final_local_empty_mode == 1 && final_local_empty_calls < 3) {
			ctrc_driver_waiting = true;
			return CLUSTER_CTRC_LOCAL_WAIT;
		}
		if (final_local_empty_mode == 2) return CLUSTER_CTRC_LOCAL_REFUSED;
		if (final_local_empty_mode == 3) pg_re_throw();
		if (final_local_empty_mode == 4) owner_boot++;
		if (final_local_empty_mode == 5) return (ClusterCtrcLocalEmptyV1)99;
		return CLUSTER_CTRC_LOCAL_EMPTY;
	}
	UT_ASSERT(progress->pending && pending_storage_live);
	UT_ASSERT_EQ(prefix_destroyed + 1, 2 * prefix_pages);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	local_empty_calls++;
	if (local_empty_mode == 1 && local_empty_calls < 3) {
		ctrc_driver_waiting = true;
		return CLUSTER_CTRC_LOCAL_WAIT;
	}
	if (local_empty_mode == 2) return CLUSTER_CTRC_LOCAL_REFUSED;
	if (local_empty_mode == 3) pg_re_throw();
	if (local_empty_mode == 4) progress->result.inserted.image.image.data[100]++;
	if (local_empty_mode == 5) progress->next_target++;
	return CLUSTER_CTRC_LOCAL_EMPTY;
}

UT_TEST(test_startup_local_empty_is_after_page_cleanup_without_discharging)
{
	prefix_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(local_empty_calls, 1); UT_ASSERT_EQ(pending_readback_calls, 1);
	UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
	UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&prefix_window,
		prefix_window.owner.writer.startup_first_lsn + 40000,
		prefix_window.owner.writer.startup_first_lsn + 40128));
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_local_empty_wait_retains_operation_and_rechecks_readback)
{
	prefix_ready(); local_empty_mode = 1;
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(local_empty_calls, 3); UT_ASSERT_EQ(pending_readback_calls, 3);
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_created, 1);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_local_empty_refusal_keeps_original_cleanup_debt)
{
	prefix_ready(); local_empty_mode = 2;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(local_empty_calls, 1);
	UT_ASSERT(pending_storage_live && page_graph_live && observation_scope_live);
	UT_ASSERT_EQ(pending_storage_closes, 0); pending_storage_abandon();
}

UT_TEST(test_startup_local_empty_error_keeps_original_cleanup_debt)
{
	volatile bool caught = false;
	prefix_ready(); local_empty_mode = 3;
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(local_empty_calls, 1);
	UT_ASSERT(pending_storage_live); UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_local_empty_cannot_change_saved_operation_or_target)
{
	for (unsigned mode = 4; mode <= 5; mode++) {
		prefix_ready(); local_empty_mode = mode;
		UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
		UT_ASSERT_EQ(local_empty_calls, 1); UT_ASSERT(pending_storage_live);
		pending_storage_abandon();
	}
}

UT_TEST(test_startup_local_empty_does_not_hide_readback_refusal)
{
	prefix_ready(); pending_readback_mode = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT_EQ(local_empty_calls, 0); UT_ASSERT(pending_storage_live);
	pending_storage_abandon();
}
