/*-------------------------------------------------------------------------
 * test_cluster_commit_carrier.h
 *    Member COMMIT ownership across an unavailable observation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_commit_carrier.h
 * NOTES
 *    Included by the production semantic-activation FSM unit harness.
 *-------------------------------------------------------------------------
 */
static void
test_commit_carrier_deliver(const ClusterSemanticActivationAckWireV1 *message, int source)
{
	ClusterICEnvelope envelope = { 0 };
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(message, payload));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = source;
	envelope.dest_node_id = cluster_node_id;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
}

static void
test_commit_carrier_prepare_member(ClusterSemanticActivationAckWireV1 *request, int member)
{
	ClusterSemanticActivationAckTableV1 *table;
	const uint32 caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
						| PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;

	test_gate_reset();
	cluster_node_id = member;
	test_current_epoch = test_membership_snapshot_epoch = 1;
	test_membership_snapshot_valid = true;
	test_membership_snapshot_lo = 15;
	test_membership_snapshot_hi = 0;
	test_local_capability_word = test_peer_capability_word = caps;
	test_peer_capability_word_sample_ok = test_peer_capability_matches = true;
	test_peer_capability_generation = 19;
	table = SemanticActivationAckTable;
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	table->coordinator_node = 0;
	table->round_nonce = 3;
	table->transition_epoch = 1;
	table->record_generation = 4;
	table->expected_members_lo = table->observed_members_lo = 15;
	table->source_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	table->target_feature_bitmap
		= table->source_feature_bitmap | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	table->capability_sample_digest = UINT64_C(0xabc123);
	for (int node = 0; node < 4; node++) {
		SemanticActivationAckTuple *tuple = &table->expected[node];
		test_remote_admitted_incarnations[node] = UINT64_C(0x100) + node;
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
		if (node == cluster_node_id) {
			UT_ASSERT(semantic_activation_ack_self_tuple(node, caps, 1, 4, tuple));
		} else {
			tuple->node_id = node;
			tuple->boot_id = tuple->admitted_incarnation = test_remote_admitted_incarnations[node];
			tuple->control_connection_generation = tuple->capability_generation = 19;
			tuple->capability_word = caps;
			tuple->transition_epoch = 1;
			tuple->record_generation = 4;
		}
		table->observed[node] = *tuple;
	}
	test_gate_publish(12, table->source_feature_bitmap, 4, 1, true);
	memset(request, 0, sizeof(*request));
	request->kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED;
	request->result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request->coordinator_node = 0;
	request->member_node = cluster_node_id;
	request->transition_epoch = 1;
	request->record_generation = 5;
	request->round_nonce = table->round_nonce;
	request->source_feature_bitmap = table->source_feature_bitmap;
	request->target_feature_bitmap = table->target_feature_bitmap;
	request->admitted_members_lo = 15;
	request->capability_sample_digest = table->capability_sample_digest;
}

static void
test_commit_carrier_setup(ClusterSemanticActivationAckWireV1 *request)
{
	ClusterSemanticActivationAckTableV1 *table;

	test_commit_carrier_prepare_member(request, 1);
	table = SemanticActivationAckTable;
	test_commit_carrier_deliver(request, 0);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(table->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_EQ(table->expected_members_lo, 15);
	UT_ASSERT_NE(semantic_activation_lmon_record_read_seq, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
}

UT_TEST(test_member_commit_read_preserves_carrier_during_observation_gap)
{
	ClusterSemanticActivationAckWireV1 request, ack;
	ClusterSemanticActivationAckTableV1 before;
	uint64 read_seq;

	test_commit_carrier_setup(&request);
	/* The coordinator's genuine stage ACK may precede this member's read. */
	ack = request;
	ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	ack.member_node = 0;
	ack.boot_id = ack.admitted_incarnation = test_remote_admitted_incarnations[0];
	ack.capability_word = test_peer_capability_word;
	test_commit_carrier_deliver(&ack, 0);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 1);
	UT_ASSERT(semantic_activation_ack_table_snapshot(&before));
	read_seq = semantic_activation_lmon_record_read_seq;

	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, read_seq);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
	UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 0);
	test_membership_snapshot_valid = true;
	test_gate_reset();
}

static void
test_commit_carrier_complete_read(const ClusterSemanticActivationAckWireV1 *request, int fault)
{
	ClusterSemanticActivationReadRequest read;
	ClusterSemanticActivationRecord commit = { 0 };
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&read));
	UT_ASSERT_EQ(read.request_seq, semantic_activation_lmon_record_read_seq);
	commit.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
	commit.record_generation = request->record_generation;
	commit.transition_epoch = request->transition_epoch;
	commit.coordinator_node = request->coordinator_node;
	commit.coordinator_incarnation = test_remote_admitted_incarnations[0];
	commit.admitted_members_lo = request->admitted_members_lo;
	commit.source_feature_bitmap = request->source_feature_bitmap;
	commit.target_feature_bitmap = request->target_feature_bitmap;
	commit.capability_sample_digest = request->capability_sample_digest;
	switch (fault) {
	case 3:
		commit.record_generation++;
		break;
	case 4:
		commit.transition_epoch++;
		break;
	case 5:
		commit.admitted_members_lo = 7;
		break;
	case 6:
		commit.coordinator_incarnation++;
		break;
	case 7:
		commit.capability_sample_digest++;
		break;
	case 8:
		commit.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
		break;
	}
	UT_ASSERT(cluster_semantic_activation_record_encode(&commit, bytes));
	if (fault == 1)
		memset(bytes, 0, sizeof(bytes)); /* canonical implicit-open input */
	if (fault == 2)
		bytes[sizeof(bytes) - 1] ^= 1;
	UT_ASSERT_EQ(
		cluster_semantic_activation_qvotec_complete_record_read(
			read.request_seq,
			fault == 9 ? CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD : CLUSTER_SEMANTIC_ACTIVATION_OK,
			fault == 1, bytes),
		fault != 2); /* malformed CRC is refused by the producer */
}

static void
test_commit_carrier_apply_proof(void)
{
	/* Only the external PCM proof is supplied; the descriptor callback,
	 * local projection, ACK revalidation and fan-out remain production code. */
	test_resource_x_cutover_digest_valid = true;
	test_resource_x_cutover_token.old_formation = 17;
	test_resource_x_cutover_token.new_formation = 18;
	test_resource_x_cutover_token.freeze_generation = 1;
	test_resource_x_cutover_digest = UINT64_C(0xa55a9911);
}

/* No member number is special: a missing observation after accepting the
 * REQUEST must not discard the three later genuine peer receipts. */
UT_TEST(test_member_commit_gap_before_all_peer_receipts)
{
	for (int member = 1; member < 4; member++) {
		ClusterSemanticActivationAckWireV1 request;
		uint64 read_seq;

		test_commit_carrier_prepare_member(&request, member);
		test_commit_carrier_deliver(&request, 0);
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(SemanticActivationAckTable->stage,
					 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
		read_seq = semantic_activation_lmon_record_read_seq;
		UT_ASSERT_NE(read_seq, 0);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
		test_membership_snapshot_valid = false;
		cluster_semantic_activation_lmon_tick();
		test_membership_snapshot_valid = true;
		for (int peer = 0; peer < 4; peer++) {
			ClusterSemanticActivationAckWireV1 ack = request;

			if (peer == member)
				continue;
			ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
			ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
			ack.member_node = peer;
			ack.boot_id = ack.admitted_incarnation = test_remote_admitted_incarnations[peer];
			ack.capability_word = test_peer_capability_word;
			test_commit_carrier_deliver(&ack, peer);
			cluster_semantic_activation_lmon_tick();
		}
		UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 15);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo,
					 UINT64_C(15) & ~(UINT64_C(1) << member));
		UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, read_seq);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
		for (int peer = 0; peer < 4; peer++)
			UT_ASSERT_EQ(test_send_calls[peer], 0);

		test_commit_carrier_complete_read(&request, 0);
		test_commit_carrier_apply_proof();
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 15);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 5);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
		for (int peer = 0; peer < 4; peer++)
			UT_ASSERT_EQ(test_send_calls[peer], peer == member ? 0 : 1);
	}
	test_gate_reset();
}

UT_TEST(test_member_commit_request_waits_for_real_predecessor_receipts)
{
	ClusterSemanticActivationAckWireV1 request;

	test_commit_carrier_prepare_member(&request, 2);
	SemanticActivationAckTable->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	SemanticActivationAckTable->observed_members_lo = 5;
	memset(&SemanticActivationAckTable->observed[1], 0,
		   sizeof(SemanticActivationAckTable->observed[1]));
	memset(&SemanticActivationAckTable->observed[3], 0,
		   sizeof(SemanticActivationAckTable->observed[3]));
	test_commit_carrier_deliver(&request, 0);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
	test_commit_carrier_deliver(&request, 0); /* one retained owner for duplicates */
	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	test_membership_snapshot_valid = true;
	for (int peer = 1; peer <= 3; peer += 2) {
		ClusterSemanticActivationAckWireV1 ack = request;

		ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
		ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		ack.member_node = peer;
		ack.record_generation = 4;
		ack.boot_id = ack.admitted_incarnation = test_remote_admitted_incarnations[peer];
		ack.capability_word = test_peer_capability_word;
		test_commit_carrier_deliver(&ack, peer);
		cluster_semantic_activation_lmon_tick();
		if (peer == 1) {
			UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
			UT_ASSERT_EQ(SemanticActivationAckTable->stage,
						 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
		}
	}
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
	UT_ASSERT_NE(semantic_activation_lmon_record_read_seq, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	for (int peer = 0; peer < 4; peer++)
		UT_ASSERT_EQ(test_send_calls[peer], 0);
	test_gate_reset();
}

UT_TEST(test_member_commit_request_rejects_old_or_changed_identity)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterSemanticActivationAckWireV1 request;

		test_commit_carrier_prepare_member(&request, 2);
		if (fault == 0)
			request.round_nonce--;
		else if (fault == 1)
			request.transition_epoch++;
		else if (fault == 2)
			request.record_generation++;
		test_commit_carrier_deliver(&request, 0);
		if (fault == 3)
			test_peer_capability_generation++;
		else if (fault == 4)
			test_remote_admitted_incarnations[0]++;
		else if (fault == 5)
			test_local_capability_word = 0;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
		for (int peer = 0; peer < 4; peer++)
			UT_ASSERT_EQ(test_send_calls[peer], 0);
	}
	test_gate_reset();
}

UT_TEST(test_member_commit_resumes_original_read_after_observation_gap)
{
	for (int completed_before_gap = 0; completed_before_gap <= 1; completed_before_gap++) {
		ClusterSemanticActivationAckWireV1 request;
		ClusterSemanticActivationAckTableV1 before;
		uint64 read_seq;

		test_commit_carrier_setup(&request);
		read_seq = semantic_activation_lmon_record_read_seq;
		UT_ASSERT(semantic_activation_ack_table_snapshot(&before));
		if (completed_before_gap)
			test_commit_carrier_complete_read(&request, 0);
		test_membership_snapshot_valid = false;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
		UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, read_seq);
		if (!completed_before_gap)
			test_commit_carrier_complete_read(&request, 0);
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, read_seq);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);

		/* Reading COMMIT cannot substitute for the local closed apply proof. */
		test_membership_snapshot_valid = true;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 5);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
		UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 0);
		test_commit_carrier_apply_proof();
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 2);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
		for (int peer = 0; peer < 4; peer++) {
			ClusterSemanticActivationAckWireV1 ack = { 0 };

			UT_ASSERT_EQ(test_send_calls[peer], peer == cluster_node_id ? 0 : 1);
			if (peer == cluster_node_id)
				continue;
			UT_ASSERT(cluster_semantic_activation_ack_wire_decode(test_send_payloads[peer], &ack));
			UT_ASSERT_EQ(ack.kind, CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK);
			UT_ASSERT_EQ(ack.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED);
			UT_ASSERT_EQ(ack.record_generation, request.record_generation);
			UT_ASSERT_EQ(ack.round_nonce, request.round_nonce);
			UT_ASSERT_EQ(ack.member_node, cluster_node_id);
		}
		/* A second gap after consumption keeps the same-generation carrier
		 * and never resends a completed positive handoff. */
		UT_ASSERT(semantic_activation_ack_table_snapshot(&before));
		test_membership_snapshot_valid = false;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
		test_membership_snapshot_valid = true;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 3);
	}
	test_gate_reset();
}

UT_TEST(test_member_commit_retention_rejects_observable_contradictions)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterSemanticActivationAckWireV1 request;
		ClusterSemanticActivationAckTableV1 *table;

		test_commit_carrier_setup(&request);
		table = SemanticActivationAckTable;
		test_membership_snapshot_valid = false;
		switch (fault) {
		case 0:
			test_current_epoch++;
			break;
		case 1:
			test_remote_admitted_incarnations[2]++;
			break;
		case 2:
			test_qvotec_self_incarnation++;
			break;
		case 3:
			test_peer_capability_generation++;
			break;
		case 4:
			test_local_capability_word = 0;
			break;
		case 5:
			test_terminal_nonmember = 2;
			break;
		case 6:
			test_observed_slot_valid[2] = true;
			test_observed_slot_generation[2] = 1;
			test_observed_slot_epoch[2] = test_current_epoch;
			test_observed_slot_incarnation[2] = test_remote_admitted_incarnations[2] + 1;
			break;
		case 7:
			table->record_generation++;
			for (int node = 0; node < 4; node++)
				table->expected[node].record_generation++;
			break;
		case 8:
			/* No local ACK is legal before the local COMMIT read/apply. */
			table->observed_members_lo = 2;
			table->observed[1] = table->expected[1];
			break;
		case 9:
			table->round_nonce = 0;
			break;
		case 10:
			table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
			break;
		case 11:
			table->coordinator_node = cluster_node_id;
			break;
		}
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(table->flags, 0);
		UT_ASSERT_EQ(table->expected_members_lo, 0);
		UT_ASSERT_EQ(table->observed_members_lo, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
		UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 0);
	}
	test_gate_reset();
}

UT_TEST(test_member_commit_original_read_still_requires_exact_durable_proof)
{
	for (int fault = 1; fault <= 9; fault++) {
		ClusterSemanticActivationAckWireV1 request;

		test_commit_carrier_setup(&request);
		test_commit_carrier_apply_proof();
		test_commit_carrier_complete_read(&request, fault);
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 4);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
		UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
		UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 0);
	}
	test_gate_reset();
}

UT_TEST(test_member_commit_control_without_observation_gap)
{
	ClusterSemanticActivationAckWireV1 request;

	test_commit_carrier_setup(&request);
	test_commit_carrier_apply_proof();
	test_commit_carrier_complete_read(&request, 0);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_lmon_record_read_seq, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 5);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 1);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 2);
	UT_ASSERT_EQ(test_send_calls[0] + test_send_calls[2] + test_send_calls[3], 3);
	test_gate_reset();
}
