/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_fpi_retirement.inc
 *     Original Startup per-operation F4 retirement and retained target custody.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * NOTES
 *     Actual Startup/Start/F4 transitions execute. Native PAGE/readback/FD
 *     transport leaves are controlled; accepted service remains independent.
 *-------------------------------------------------------------------------
 */
static bool retirement_service_reached;
static unsigned retirement_mode;
static uint32 retirement_target, retirement_pending;
static unsigned retirement_steps, retirement_reads, retirement_closes;
static ClusterRecoveryStampPhaseV1 retirement_phase;

static void
retirement_service_probe(void)
{
	ClusterColdCtrcPageProgressV1 *progress = cluster_recovery_cold_start->ctrc_page_progress;
	ClusterRecoveryStampSlotV1 *slot = &cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp;

	retirement_service_reached = true;
	retirement_target = progress->next_target; retirement_pending = progress->pending;
	retirement_phase = slot->phase; retirement_steps = prefix_steps;
	retirement_reads = pending_readback_calls; retirement_closes = pending_storage_closes;
	if (retirement_mode == 1) {
		bool again = ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0);
		retirement_service_reached = again && prefix_steps == retirement_steps
			&& pending_readback_calls == retirement_reads;
	}
	pg_re_throw(); /* Original service stays PENDING; end this controlled call. */
}

static void
retirement_ready(void)
{
	ctrc_handoff_fixture(false, false); prefix_setup_targets();
	retirement_service_reached = false; retirement_mode = 0;
	retirement_target = retirement_pending = retirement_steps = retirement_reads = retirement_closes = 0;
	retirement_phase = CLUSTER_RECOVERY_STAMP_EMPTY;
	ctrc_wait_step = retirement_service_probe;
}

static void
retirement_run_startup(void)
{
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		PG_TRY(); { actual_startup_finish_tx(&info); }
		PG_CATCH(); { } PG_END_TRY();
	}
	fatal_armed = false;
}

static void
retirement_local_cleanup(void)
{
	pending_storage_check_close = true;
	cluster_recovery_cold_observation_close(info.cluster_tx_start);
}

UT_TEST(test_startup_retires_exact_fpi_windows_before_next_target_and_service)
{
	bool held, unaccepted;
	unsigned closed;
	retirement_ready(); retirement_run_startup();
	held = pending_storage_live && observation_scope_live && observation_cursor_live;
	unaccepted = window_region.checkpoint_owner.ctrc_accepted_generation == 0;
	retirement_local_cleanup(); closed = pending_storage_closes;
	UT_ASSERT(retirement_service_reached && held && unaccepted);
	UT_ASSERT_EQ(retirement_target, 2); UT_ASSERT_EQ(retirement_pending, 0);
	UT_ASSERT_EQ(retirement_phase, CLUSTER_RECOVERY_STAMP_EMPTY);
	UT_ASSERT_EQ(retirement_steps, 2); UT_ASSERT_EQ(retirement_reads, 2);
	UT_ASSERT_EQ(retirement_closes, 0); UT_ASSERT_EQ(closed, 2);
}

UT_TEST(test_startup_retired_fpi_progress_does_not_repeat_wal_or_readback)
{
	retirement_ready(); retirement_mode = 1; retirement_run_startup();
	retirement_local_cleanup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(retirement_steps, 2); UT_ASSERT_EQ(retirement_reads, 2);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_retired_fpi_fd_loss_refuses_without_reopening_or_releasing_scope)
{
	bool was_current, lost, retained;
	unsigned steps, reads;
	retirement_ready(); retirement_run_startup();
	was_current = ClusterColdRecoveryTxCurrent(info.cluster_tx_start);
	steps = prefix_steps; reads = pending_readback_calls;
	pending_storage_invalid = true;
	lost = !ClusterColdRecoveryTxCurrent(info.cluster_tx_start);
	retained = pending_storage_live && observation_scope_live && observation_cursor_live;
	retirement_local_cleanup();
	UT_ASSERT(was_current && lost && retained);
	UT_ASSERT_EQ(steps, 2); UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(pending_storage_created, 2); UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_fpi_retirement_refuses_unproved_readback_or_local_directory)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		bool pending, held, unaccepted;
		ClusterRecoveryStampPhaseV1 phase;
		retirement_ready();
		if (mode == 0) pending_readback_mode = 1; else local_empty_mode = 2;
		retirement_run_startup();
		pending = cluster_recovery_cold_start->ctrc_page_progress->pending;
		held = pending_storage_live && observation_scope_live;
		unaccepted = window_region.checkpoint_owner.ctrc_accepted_generation == 0;
		phase = cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.phase;
		retirement_local_cleanup();
		UT_ASSERT(!retirement_service_reached && pending && held && unaccepted);
		UT_ASSERT_EQ(phase, CLUSTER_RECOVERY_STAMP_DURABLE);
		UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_closes, 1);
	}
}

static void
retirement_second_refuses(void)
{
	if (prefix_records == 1) prefix_legacy_synced = true;
}

UT_TEST(test_startup_later_fpi_refusal_keeps_prior_target_custody_and_shared_debt)
{
	bool retained, pending, unaccepted;
	unsigned count;
	retirement_ready(); prefix_callback = retirement_second_refuses;
	retirement_run_startup();
	retained = pending_storage_live && observation_scope_live;
	pending = cluster_recovery_cold_start->ctrc_page_progress->pending;
	count = cluster_recovery_cold_start->ctrc_page_progress->next_target;
	unaccepted = window_region.checkpoint_owner.ctrc_accepted_generation == 0;
	retirement_local_cleanup();
	UT_ASSERT(!retirement_service_reached && retained && pending && unaccepted);
	UT_ASSERT_EQ(count, 1); UT_ASSERT_EQ(prefix_steps, 2);
	UT_ASSERT_EQ(pending_storage_created, 1); UT_ASSERT_EQ(pending_storage_closes, 1);
	UT_ASSERT(cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.failed);
}

UT_TEST(test_startup_fpi_completion_capacity_is_reserved_before_first_window)
{
	bool accepted;
	unsigned steps;
	prefix_ready(); prefix_count = 256;
	accepted = ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024);
	steps = prefix_steps;
	retirement_local_cleanup();
	UT_ASSERT(!accepted); UT_ASSERT_EQ(steps, 0);
	UT_ASSERT_EQ(pending_storage_created, 0); UT_ASSERT_EQ(pending_storage_closes, 0);
}

UT_TEST(test_startup_retained_raw_handle_peak_precedes_first_page_or_window)
{
	bool accepted;
	unsigned steps, pages;
	prefix_ready(); pending_storage_bytes = 512 * 1024;
	accepted = ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024);
	steps = prefix_steps; pages = prefix_pages;
	retirement_local_cleanup();
	UT_ASSERT(!accepted); UT_ASSERT_EQ(steps + pages, 0);
	UT_ASSERT_EQ(pending_storage_created + pending_storage_closes, 0);
}
