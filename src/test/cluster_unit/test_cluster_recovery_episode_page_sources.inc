/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_page_sources.inc
 *    Whole generation-set checks for held FULL page observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_page_sources.inc
 * NOTES
 *    Start from original physical two-source observations. Additional
 *    by-value generations test consistency only, not new native authority.
 *-------------------------------------------------------------------------
 */
static void
episode_page_sources_check(unsigned mode)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 *out = calloc(1, sizeof(*out));
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRelmapPendingXidV1 *xid;
	ClusterRecoveryRelmapXidGenerationV1 *extra;
	uint32 foreign = UINT32_MAX, count;
	unsigned reads, census;

	UT_ASSERT(out != NULL);
	if (out == NULL) return;
	episode_page_history_mode = 0;
	episode_page_history_begin(2, false, 8 * 1024 * 1024, &inputs, &history, &pending);
	consumed_setup(inputs, history, false, 1);
	UT_ASSERT(cluster_recovery_relmap_history_pending_consumed_pages_v1(history, &pending, out, &diag));
	xid = &out->inputs.query.birth.inputs;
	count = xid->generation_count;
	UT_ASSERT_EQ(count, 2); UT_ASSERT_EQ(out->page_count, 3);
	for (uint32 i = 0; i < count; ++i)
		if (xid->generations[i].observation.source.claim.identity.origin_node_id == 127)
			foreign = i;
	UT_ASSERT(foreign < count && count < CLUSTER_WAL_INPUTS_MAX);
	if (foreign < count && count < CLUSTER_WAL_INPUTS_MAX) {
		/* This is a structural candidate, NOT an acquired third scope or a
		 * valid claim/counter/lifetime certificate for the original episode. */
		extra = &xid->generations[count];
		*extra = xid->generations[foreign];
		extra->observation.source_index = count;
		extra->observation.source.claim.identity.origin_owner_incarnation++;
		extra->observation.cut.origin_owner_incarnation++;
		if (mode == 1)
			extra->counter_observed = false;
		if (mode == 2)
			extra->observation.source.claim.identity.origin_node_id = 126;
		xid->generation_count++;
		/* No event references the additional generation: inspecting only
		 * CLOG events cannot find the same-origin ambiguity. */
		for (uint32 i = 0; i < out->inputs.clog_count; ++i)
			UT_ASSERT(out->inputs.clog[i].source_index != count);
		reads = episode_native_clog_calls; census = inventory_census_calls;
		UT_ASSERT_EQ(history_pending_page_history(history, xid, &out->inputs), mode == 2);
		UT_ASSERT_EQ(episode_native_clog_calls, reads); UT_ASSERT_EQ(inventory_census_calls, census);
		/* Rejecting the local candidate changes no original held observation. */
		xid->generation_count = count;
		UT_ASSERT(history_pending_page_history(history, xid, &out->inputs));
	}
	UT_ASSERT(!history->failed && relmap_failed == 0);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && ctrc_set_release_calls == 0);
	consumed_mode = 0; episode_native_pages_end(&history, &inputs);
	free(out);
}

UT_TEST(test_episode_page_sources_refuse_same_origin_without_clog)
{
	episode_page_sources_check(0);
}
UT_TEST(test_episode_page_sources_refuse_same_origin_without_counter)
{
	episode_page_sources_check(1);
}
UT_TEST(test_episode_page_sources_preserve_unrelated_eventless_generation)
{
	episode_page_sources_check(2);
}
