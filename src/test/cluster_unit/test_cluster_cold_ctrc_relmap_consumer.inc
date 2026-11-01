/* Original Start owner-graph consumer regressions.
 * PAGE/native source leaves use the existing fixture; a missing relmap
 * terminal receipt is never replaced by a successful fixture provider.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static void
relmap_consumer_declare(void)
{
	UT_ASSERT(ClusterColdRecoveryRelmapDeclare(info.cluster_tx_start));
	/* Only the already separate staging leaf is controlled here. There is
	 * deliberately no terminal-capture/recheck success provider. */
	relmap_observe_allowed = true;
}

static void
relmap_consumer_preserved(unsigned destroyed)
{
	UT_ASSERT_EQ(input_destroys, destroyed);
	UT_ASSERT(claim_held && info.cluster_tx_start != NULL);
	UT_ASSERT(info.cluster_cold_inputs == &typed);
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(!cluster_recovery_checkpoint_handoff_v1(info.cluster_tx_start));
}

UT_TEST(test_start_owner_graph_requires_declared_relmap_terminal_input)
{
	RfPageCtrcOwnerGraphV1 out;
	unsigned destroyed;
	owner_scope_after_subject = relmap_consumer_declare;
	projected_graph_ready(true, false, false);
	owner_scope_after_subject = NULL;
	UT_ASSERT(ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	destroyed = input_destroys;
	/* This original Start has PAGE owners and a declared obligation, but no
	 * original-stage terminal capture. The owner graph must call the real
	 * terminal participant rather than treating the declaration as evidence. */
	out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0);
	relmap_consumer_preserved(destroyed);
	/* Discard returned observation storage even on the failing parent. This
	 * does not retire any native descriptor, pin or original shared duty. */
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) owner_scope_free(&out);
	owner_scope_cleanup();
}

UT_TEST(test_start_observation_graph_cannot_bypass_declared_terminal_input)
{
	RfPageCtrcOwnerGraphV1 out;
	unsigned destroyed;
	owner_scope_after_subject = relmap_consumer_declare;
	owner_scope_ready(false);
	owner_scope_after_subject = NULL;
	UT_ASSERT(ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	destroyed = input_destroys;
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0);
	relmap_consumer_preserved(destroyed);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) owner_scope_free(&out);
	owner_scope_cleanup();
}

static void
relmap_consumer_owner_error(void)
{
	pg_re_throw();
}

UT_TEST(test_start_owner_graph_terminal_provider_error_keeps_original_debt)
{
	RfPageCtrcOwnerGraphV1 out = {0};
	unsigned destroyed;
	volatile bool caught = false;
	owner_scope_after_subject = relmap_consumer_declare;
	projected_graph_ready(true, false, false);
	owner_scope_after_subject = NULL;
	destroyed = input_destroys;
	relmap_owner_check_step = relmap_consumer_owner_error;
	PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	relmap_owner_check_step = NULL;
	UT_ASSERT(caught);
	UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0);
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	relmap_consumer_preserved(destroyed);
	owner_scope_cleanup();
}

UT_TEST(test_start_undeclared_page_owner_graph_keeps_original_contribution)
{
	RfPageCtrcOwnerGraphV1 out;
	projected_graph_ready(true, false, false);
	UT_ASSERT(!ClusterColdRecoveryRelmapCurrent(info.cluster_tx_start));
	out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.contribution.node_count, 4);
		UT_ASSERT_EQ(out.contribution.edge_count, 2);
		UT_ASSERT_EQ(out.unresolved_count, 4);
		owner_scope_free(&out);
	}
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	owner_scope_cleanup();
}
