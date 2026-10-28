/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_applied_owner.inc
 *     Retain original whole-source refusal and held native pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_applied_owner.inc
 * NOTES
 *     Original episode/plans/readers execute; cold authority, native mutation
 *     and projection leaves remain controlled. No lifetime or COMPLETE grant.
 *-------------------------------------------------------------------------
 */

static bool episode_owner_original(ClusterRecoveryRelmapHistory *history,
								   ClusterRecoveryRelmapSourceV1 *source, RfSideOnlinePlanV1 **plan,
								   uint32 *index)
{
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
	{
		ClusterRecoveryRelmapHistoryEntryV1 saved;
		if (!cluster_recovery_relmap_history_source_v1(history, i, &saved))
			return false;
		if (saved.source.selected.source.claim.identity.origin_node_id != 0)
			continue;
		*source = saved.source;
		for (uint32 p = 0; p < tx_inputs_typed.participant_count; ++p)
		{
			if (memcmp(&tx_inputs_typed.sources[p], &source->selected.source,
					   sizeof(source->selected.source)) != 0)
				continue;
			*plan = tx_inputs_typed.side_plans[p];
			for (uint32 n = 0; n < rf_side_online_plan_operation_count_v1(*plan); ++n)
			{
				RfSideOnlineOperationV1 operation;
				if (!rf_side_online_plan_operation_v1(*plan, n, &operation))
					return false;
				if (operation.kind == RF_SIDE_ONLINE_OPERATION_PROJECTION &&
					operation.projection.kind == CLUSTER_SIDE_PROJECTION_CLOG)
				{
					*index = n;
					return true;
				}
			}
		}
	}
	return false;
}

static int episode_owner_hold(ClusterRecoveryRelmapHistory *history,
							  const ClusterRelmapV2Image *pending)
{
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryRelmapNativePageV1 page;
	ClusterRecoveryNativeInventoryDiag diag;
	int fd = -1;
	UT_ASSERT(
		cluster_recovery_relmap_history_pending_native_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(out.page_count, 3);
	if (cluster_recovery_relmap_history_native_page_v1(history, 0, &page))
		fd = episode_native_pages_find_fd(page.native.inode);
	UT_ASSERT(fd >= 0);
	return fd;
}

static void episode_owner_refused_input(ClusterRecoveryRelmapHistory *history,
										const ClusterRelmapV2Image *pending, int fd)
{
	ClusterRecoveryRelmapPendingNativePagesV1 out, zero = {0};
	ClusterRecoveryNativeInventoryDiag diag;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(
		!cluster_recovery_relmap_history_pending_applied_pages_v1(history, pending, &out, &diag));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(history->failed && relmap_failed > 0 && fcntl(fd, F_GETFD) >= 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
}

UT_TEST(test_episode_applied_owner_whole_source_preflight_precedes_native_mutation)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 source;
	RfSideOnlinePlanV1 *plan = NULL;
	EpisodeSideOwnerStats stats;
	uint32 index = 0;
	int fd;
	episode_applied_reset(0);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	fd = episode_owner_hold(history, &pending);
	UT_ASSERT(episode_owner_original(history, &source, &plan, &index));
	UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes, 1);
	UT_ASSERT_EQ(stats.native_applies + stats.native_verifies + stats.projections, 0);
	UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes + stats.native_applies + stats.native_verifies + stats.projections,
				 0);
	episode_owner_refused_input(history, &pending, fd);
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}

UT_TEST(test_episode_applied_owner_equivalent_plan_does_not_replace_original_attempt)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 source;
	RfSideOnlinePlanV1 *plan = NULL, *clone = NULL;
	RfSideOnlinePlanV1 *original[CLUSTER_COLD_MAX_PARTICIPANTS] = {0};
	RfSideOnlinePlanV1 *clones[CLUSTER_COLD_MAX_PARTICIPANTS] = {0};
	EpisodeSideOwnerStats stats;
	uint32 index = 0;
	int fd;
	episode_applied_reset(0);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	fd = episode_owner_hold(history, &pending);
	UT_ASSERT(episode_owner_original(history, &source, &plan, &index));
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
	{
		original[i] = tx_inputs_typed.side_plans[i];
		tx_inputs_typed.side_plans[i] = NULL;
	}
	episode_clog_side_plans(inputs);
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
	{
		clones[i] = tx_inputs_typed.side_plans[i];
		tx_inputs_typed.side_plans[i] = original[i];
		if (original[i] == plan)
			clone = clones[i];
	}
	UT_ASSERT(clone != NULL && clone != plan);
	UT_ASSERT_NE(test_episode_side_owner_apply(clone, index, &source, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes, 1);
	UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes, 1); /* Clone's attempt cannot retire this plan. */
	UT_ASSERT_EQ(stats.native_applies + stats.native_verifies + stats.projections, 0);
	episode_owner_refused_input(history, &pending, fd);
	for (uint32 i = 0; i < tx_inputs_typed.participant_count; ++i)
		rf_side_online_plan_destroy_v1(&clones[i]);
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}

UT_TEST(test_episode_applied_owner_scope_false_and_error_do_not_retry)
{
	for (unsigned fault = 1; fault <= 2; ++fault)
	{
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapSourceV1 source;
		RfSideOnlinePlanV1 *plan = NULL;
		static EpisodeSideOwnerStats stats;
		uint32 index = 0;
		volatile bool caught = false;
		int fd;
		episode_applied_reset(0);
		episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
		fd = episode_owner_hold(history, &pending);
		UT_ASSERT(episode_owner_original(history, &source, &plan, &index));
		PG_TRY();
		{
			UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, fault, &stats), 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 2);
		UT_ASSERT_EQ(stats.scopes, 1);
		UT_ASSERT_EQ(stats.native_applies + stats.native_verifies + stats.projections, 0);
		UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, 0, &stats), 0);
		UT_ASSERT_EQ(stats.scopes, 0);
		episode_owner_refused_input(history, &pending, fd);
		episode_native_pages_end(&history, &inputs);
		UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
	}
}

UT_TEST(test_episode_applied_owner_stale_source_refuses_before_original_attempt)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapSourceV1 source, changed;
	RfSideOnlinePlanV1 *plan = NULL;
	EpisodeSideOwnerStats stats;
	uint32 index = 0;
	int fd;
	episode_applied_reset(0);
	episode_native_pages_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	fd = episode_owner_hold(history, &pending);
	UT_ASSERT(episode_owner_original(history, &source, &plan, &index));
	changed = source;
	changed.selected.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &changed, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes, 0);
	UT_ASSERT_NE(test_episode_side_owner_apply(plan, index, &source, 0, &stats), 0);
	UT_ASSERT_EQ(stats.scopes, 1); /* Stale precondition did not consume it. */
	UT_ASSERT_EQ(stats.native_applies + stats.native_verifies + stats.projections, 0);
	episode_owner_refused_input(history, &pending, fd);
	episode_native_pages_end(&history, &inputs);
	UT_ASSERT_EQ(fcntl(fd, F_GETFD), -1);
}
