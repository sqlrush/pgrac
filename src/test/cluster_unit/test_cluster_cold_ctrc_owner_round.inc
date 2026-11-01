/* Original Start's complete PAGE-owner round. PAGE/FD transport is controlled;
 * the extracted owner, waits, final checks and cleanup execute unchanged.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned owner_round_trace[16], owner_round_visits, owner_round_mode;
static bool owner_round_waited;

static void
owner_round_change_first(void)
{
	ClusterColdCtrcPageCompletedV1 *first =
		&cluster_recovery_cold_start->ctrc_page_progress->completed[0];

	if (first->result.phase == RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED)
		first->unchanged.space0.data[101] ^= 1;
	else
		first->result.inserted.record_end++;
}

static RfPageProofDetailV1
owner_round_observe(uint32 index, RfPageProofDetailV1 detail)
{
	UT_ASSERT(owner_round_visits < lengthof(owner_round_trace));
	owner_round_trace[owner_round_visits++] = index;
	if (detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	if (index == 1 && owner_round_mode == 1) owner_round_change_first();
	if (index == 1 && owner_round_mode == 2) pg_re_throw();
	if (index == 1 && owner_round_mode == 3 && !owner_round_waited) {
		owner_round_waited = true;
		ctrc_driver_waiting = true;
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	}
	return detail;
}

static void
owner_round_ready(bool unchanged)
{
	final_projection_ready(unchanged);
	final_projection_check = false;
	owner_round_visits = owner_round_mode = 0;
	owner_round_waited = false;
	memset(owner_round_trace, 0, sizeof(owner_round_trace));
	startup_image_observer = owner_round_observe;
}

static void
owner_round_cleanup(void)
{
	startup_image_observer = NULL;
	ctrc_driver_wait_step = NULL;
	startup_first_step = NULL;
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
	UT_ASSERT_EQ(pending_storage_closes, 2);
}

UT_TEST(test_startup_owner_round_keeps_complete_fpi_and_unchanged_set)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0);
		retirement_run_startup();
		UT_ASSERT(retirement_service_reached);
		UT_ASSERT_EQ(owner_round_visits, 2);
		UT_ASSERT_EQ(owner_round_trace[0], 0); UT_ASSERT_EQ(owner_round_trace[1], 1);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
		owner_round_cleanup();
	}
}

UT_TEST(test_startup_owner_round_later_target_cannot_change_earlier_operation)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0); owner_round_mode = 1;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_round_cleanup();
			UT_ASSERT(refused);
			UT_ASSERT_EQ(owner_round_visits, 2);
		}
	}
}

static void
owner_round_final_first(void)
{
	if (startup_first_calls == 2) owner_round_change_first();
}

UT_TEST(test_startup_owner_round_final_nonpage_check_cannot_change_completed_set)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0); startup_first_step = owner_round_final_first;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_round_cleanup();
			UT_ASSERT(refused);
			UT_ASSERT_EQ(owner_round_visits, 2); UT_ASSERT_EQ(startup_first_calls, 2);
		}
	}
}

UT_TEST(test_startup_owner_round_wait_restarts_first_target_without_new_operation)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0); owner_round_mode = 3;
		retirement_run_startup();
		{
			bool reached = retirement_service_reached && owner_round_waited;
			owner_round_cleanup();
			UT_ASSERT(reached);
		}
		UT_ASSERT_EQ(ctrc_driver_waits, 1); UT_ASSERT_EQ(owner_round_visits, 4);
		UT_ASSERT_EQ(owner_round_trace[0], 0); UT_ASSERT_EQ(owner_round_trace[1], 1);
		UT_ASSERT_EQ(owner_round_trace[2], 0); UT_ASSERT_EQ(owner_round_trace[3], 1);
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(pending_storage_created, 2);
	}
}

UT_TEST(test_startup_owner_round_wait_cannot_rebase_changed_first_operation)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0); owner_round_mode = 3;
		ctrc_driver_wait_step = owner_round_change_first;
		retirement_run_startup();
		{
			bool refused = !retirement_service_reached && info.cluster_tx_start->revoked;
			owner_round_cleanup();
			UT_ASSERT(refused);
			UT_ASSERT_EQ(ctrc_driver_waits, 1); UT_ASSERT_EQ(owner_round_visits, 2);
		}
	}
}

UT_TEST(test_startup_owner_round_late_error_keeps_original_cleanup)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		owner_round_ready(unchanged != 0); owner_round_mode = 2;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(owner_round_visits, 2);
		owner_round_cleanup();
	}
}

UT_TEST(test_startup_owner_round_reserves_complete_snapshot_before_first_page)
{
	Size bytes, scratch;

	prefix_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	bytes = sizeof(ClusterColdCtrcPageProgressV1) + 2 * sizeof(ClusterColdCtrcPageCompletedV1)
		+ 2 * cluster_shared_fs_page_bytes_v1();
	scratch = native_ctrc_work.counts.subject_count * sizeof(ClusterColdTtSlotInputV1)
		+ 2 * sizeof(PGAlignedBlock) + sizeof(RfPageCtrcOwnerProofV1)
		+ 2 * sizeof(ClusterColdCtrcPageCompletedV1);
	{
		bool refused = !ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, bytes + scratch);
		bool untouched = prefix_pages == 0 && prefix_steps == 0
			&& pending_storage_created == 0 && observation_scope_live;
		retirement_local_cleanup();
		UT_ASSERT(refused && untouched);
	}
}
