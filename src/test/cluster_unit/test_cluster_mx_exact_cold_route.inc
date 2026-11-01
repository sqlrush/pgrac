/* Actual original origin continuation, codec and release. Pool/runtime authority
 * are controlled leaves, not a native I/O or TT40 integration claim. */
static void
mx_cold_setup(ClusterCurrentMxOperationForwardV6 *request, ClusterICEnvelope *env)
{
	mx_exact_request(request, env, true);
	mx_cold_mask = 15; mx_cold_reads = mx_cold_fault_at = mx_cold_fault = 0;
}
static void
mx_cold_cleanup(int saved)
{
	mx_cold_mask = mx_cold_reads = mx_cold_fault_at = mx_cold_fault = 0;
	mx_origin_sample_enabled = mx_exact_enabled = false; cluster_node_id = saved;
}
static void
mx_cold_reply(const ClusterCurrentMxOperationForwardV6 *request, bool success)
{
	ClusterCurrentMemberOperationProof proofs[7]; ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result; uint16 count; uint32 capability;
	UT_ASSERT_EQ(route_seam.raw_send_calls, 1);
	UT_ASSERT(cluster_multixact_operation_wire_validate_reply(route_seam.raw_send_page,
		BLCKSZ, UT_MASTER_NODE, UT_FORMATION_EPOCH, request, &result, proofs,
		lengthof(proofs), &count, &updater, &capability));
	UT_ASSERT_EQ(result, success ? CMX_RESOLVE_OK : CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(count, success ? 1 : 0);
	UT_ASSERT_EQ(updater.verdict, success ? CUCP_MATCH : CUCP_UNKNOWN);
	UT_ASSERT_EQ(mx_origin_guard_held, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
}
UT_TEST(test_exact_cold_A_and_B_share_one_read_per_drain)
{
	ClusterCurrentMxOperationForwardV6 request; ClusterICEnvelope env;
	int saved = cluster_node_id; cluster_node_id = UT_MASTER_NODE;
	mx_cold_setup(&request, &env);
	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	for (unsigned i = 0; i < 32; ++i) {
		unsigned before = mx_cold_reads;
		cluster_gcs_block_test_r4_tx_origin_drain();
		UT_ASSERT(mx_cold_reads - before <= 1);
	}
	mx_cold_reply(&request, true); UT_ASSERT_EQ(mx_cold_reads, 4);
	UT_ASSERT_EQ(mx_cold_mask, 0); UT_ASSERT_EQ(mx_exact_stage, 3);
	UT_ASSERT_EQ(route_seam.candidate_acquire_begin_calls, 6);
	UT_ASSERT_EQ(route_seam.candidate_release_begin_calls, 6);
	mx_cold_cleanup(saved);
}
UT_TEST(test_exact_cold_failure_and_error_never_publish_A)
{
	int saved = cluster_node_id;
	for (unsigned at = 1; at <= 4; ++at) for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterCurrentMxOperationForwardV6 request; ClusterICEnvelope env;
		cluster_node_id = UT_MASTER_NODE; mx_cold_setup(&request, &env);
		mx_cold_fault_at = at; mx_cold_fault = fault;
		UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
		for (unsigned i = 0; i < 32; ++i) cluster_gcs_block_test_r4_tx_origin_drain();
		mx_cold_reply(&request, false); UT_ASSERT_EQ(mx_cold_reads, at);
		mx_cold_cleanup(saved);
	}
}
UT_TEST(test_exact_cold_post_load_scope_drift_is_unknown)
{
	ClusterCurrentMxOperationForwardV6 request; ClusterICEnvelope env;
	int saved = cluster_node_id; cluster_node_id = UT_MASTER_NODE;
	mx_cold_setup(&request, &env); mx_cold_fault_at = 1; mx_cold_fault = 3;
	UT_ASSERT(cluster_gcs_block_test_current_mx_forward128(&env, &request));
	for (unsigned i = 0; i < 32; ++i) cluster_gcs_block_test_r4_tx_origin_drain();
	/* Lost admission follows the original context retirement path, so no
	 * reply may be admitted. There is no partial A/B result to consume. */
	UT_ASSERT_EQ(route_seam.raw_send_calls, 0); UT_ASSERT_EQ(mx_cold_reads, 1);
	UT_ASSERT_EQ(mx_origin_guard_held, 0);
	UT_ASSERT_EQ(cluster_gcs_block_test_r4_tx_origin_context_count(), 0);
	mx_cold_cleanup(saved);
}
