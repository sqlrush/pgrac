/* SAMPLE proof lifetime through the real handler and LMON duty order.
 * External membership and transport observations use the existing unit seams.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
test_sample_barrier_observation_gap(bool second_read)
{
	ClusterSemanticActivationAckWireV1 request;
	ClusterSemanticActivationAckWireV1 ack;
	ClusterSemanticActivationAckTableV1 complete;
	ClusterICEnvelope envelope = {0};
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	test_first_open_barrier_setup(&request, &complete);
	test_first_open_deliver(&request, 0);
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	ack = request;
	ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	ack.member_node = 2;
	ack.capability_sample_digest = 0;
	ack.boot_id = complete.observed[2].boot_id;
	ack.admitted_incarnation = complete.observed[2].admitted_incarnation;
	ack.capability_word = complete.observed[2].capability_word;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&ack, payload));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = 2;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	if (second_read)
		test_membership_snapshot_fail_at_call = test_membership_snapshot_calls + 2;
	else
		test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(
		&semantic_activation_ack_local_ingress), 1);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 11);
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits), 0);
	test_membership_snapshot_valid = true;
	test_membership_snapshot_fail_at_call = 0;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(semantic_activation_ack_ingress_pending(
		&semantic_activation_ack_local_ingress), 0);
	/* A late duplicate of the previous stage cannot erase the new stage. */
	test_first_open_deliver(&ack, 2);
	test_first_open_deliver(&request, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits), 0);
	test_first_open_finish();
}

UT_TEST(test_sample_barrier_preserves_partial_proof_during_observation_gap)
{
	test_sample_barrier_observation_gap(false);
}

UT_TEST(test_sample_barrier_preserves_partial_proof_at_second_read_gap)
{
	test_sample_barrier_observation_gap(true);
}

UT_TEST(test_sample_fanout_finishes_before_barrier_replaces_owner)
{
	ClusterSemanticActivationAckWireV1 barrier;
	ClusterSemanticActivationAckWireV1 sample;
	ClusterSemanticActivationAckWireV1 sent;
	ClusterSemanticActivationAckTableV1 complete;

	test_first_open_barrier_setup(&barrier, &complete);
	UT_ASSERT(semantic_activation_ack_table_publish(&complete));
	sample = barrier;
	sample.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	sample.capability_sample_digest = 0;
	UT_ASSERT(semantic_activation_ack_pending_send_begin_positive(
		&semantic_activation_ack_local_pending_send, &sample, cluster_node_id,
		&complete.observed[cluster_node_id]));
	test_send_results[0] = test_send_results[1] = CLUSTER_IC_SEND_DONE;
	test_send_results[2] = CLUSTER_IC_SEND_NOT_ADMITTED;
	semantic_activation_ack_lmon_send_pending();
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_lo, 4);
	test_first_open_deliver(&barrier, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 15);
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT(!semantic_activation_ack_local_pending_send.invalidated);
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_lo, 4);
	UT_ASSERT_EQ(test_send_calls[0], 1);
	UT_ASSERT_EQ(test_send_calls[1], 1);
	test_send_results[2] = CLUSTER_IC_SEND_DONE;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(test_send_payloads[2], &sent));
	UT_ASSERT_EQ(sent.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(semantic_activation_ack_local_pending_send.pending_members_lo, 0);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT(!semantic_activation_ack_local_pending_send.invalidated);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits), 0);
	test_first_open_finish();
}
