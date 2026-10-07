/* Original startup requester + real semantic mailbox/LMON consumer.
 * Fixtures provide external INSTALL, native writer and QVOTEC/IC inputs;
 * they never substitute the request or admission algorithms.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
test_first_open_reset(void)
{
	test_gate_reset();
	cluster_shared_config = true;
	IsUnderPostmaster = false;
	cluster_node_id = 0;
	test_route_node_count = 4;
	test_first_writer_installed = test_first_writer_initialized = true;
	test_first_writer_epoch = test_current_epoch;
	test_first_live_checks = 0;
	test_first_live_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_restart_in_recovery = false;
	test_system_identifier = UINT64_C(0x8070605040302010);
	memset(&test_first_writer, 0, sizeof(test_first_writer));
	test_first_writer.timeline = 1;
	test_first_writer.claim.identity.origin_node_id = cluster_node_id;
	test_first_writer.claim.identity.origin_owner_incarnation = test_qvotec_self_incarnation;
	pg_atomic_write_u32(&NormalStartCompletion->state, CLUSTER_NORMAL_START_SOURCE_ZERO);
	test_gate_publish(2, 0, 0, test_current_epoch, false);
}

static void
test_first_open_finish(void)
{
	cluster_shared_config = false;
	IsUnderPostmaster = true;
	test_route_node_count = 4;
	test_gate_reset();
}

static unsigned test_semantic_completion_wakes;
static uint64 test_semantic_completion_expected_seq;
static ClusterSemanticActivationResult test_semantic_completion_expected_result;
static unsigned test_semantic_submit_wakes;
static uint64 test_semantic_submit_expected_seq;
static ClusterSemanticAuthorityRequestKind test_semantic_submit_expected_kind;
static uint8 test_semantic_submit_expected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

void
cluster_qvotec_wakeup(void)
{
	test_semantic_submit_wakes++;
	if (test_semantic_submit_expected_seq != 0) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq),
					 test_semantic_submit_expected_seq);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind),
					 test_semantic_submit_expected_kind);
		UT_ASSERT_EQ(SemanticActivationShmem->record_cas_expected_generation, 17);
		UT_ASSERT_EQ(SemanticActivationShmem->record_cas_expected_source_feature_bitmap, 23);
		UT_ASSERT(memcmp(SemanticActivationShmem->record_cas_desired_bytes,
						 test_semantic_submit_expected_bytes,
						 sizeof(test_semantic_submit_expected_bytes))
				  == 0);
	}
}

UT_TEST(test_first_open_authority_submit_notifies_after_exact_publication)
{
	const ClusterSemanticAuthorityRequestKind kinds[]
		= { CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS,
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ,
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ };
	uint64 seq = 0;
	unsigned i;

	test_first_open_reset();
	test_semantic_submit_wakes = 0;
	memset(test_semantic_submit_expected_bytes, 0x5a, sizeof(test_semantic_submit_expected_bytes));
	UT_ASSERT(!semantic_activation_authority_mailbox_submit(kinds[0], 17, 23, NULL, &seq));
	UT_ASSERT_EQ(test_semantic_submit_wakes, 0);
	for (i = 0; i < lengthof(kinds); i++) {
		test_semantic_submit_expected_seq = i + 1;
		test_semantic_submit_expected_kind = kinds[i];
		UT_ASSERT(semantic_activation_authority_mailbox_submit(
			kinds[i], 17, 23, test_semantic_submit_expected_bytes, &seq));
		UT_ASSERT_EQ(seq, i + 1);
		UT_ASSERT_EQ(test_semantic_submit_wakes, i + 1);
		UT_ASSERT(!semantic_activation_authority_mailbox_submit(
			kinds[i], 17, 23, test_semantic_submit_expected_bytes, &seq));
		UT_ASSERT(!semantic_activation_authority_mailbox_complete(kinds[i], seq + 1,
																  CLUSTER_SEMANTIC_ACTIVATION_OK));
		UT_ASSERT_EQ(test_semantic_submit_wakes, i + 1);
		UT_ASSERT(semantic_activation_authority_mailbox_complete(kinds[i], seq,
																 CLUSTER_SEMANTIC_ACTIVATION_OK));
	}
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_request_seq, UINT64_MAX);
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_completion_seq, UINT64_MAX);
	UT_ASSERT(!semantic_activation_authority_mailbox_submit(
		kinds[0], 17, 23, test_semantic_submit_expected_bytes, &seq));
	UT_ASSERT_EQ(test_semantic_submit_wakes, lengthof(kinds));
	test_semantic_submit_expected_seq = 0;
	test_first_open_finish();
}

void
cluster_lmon_marker_complete_wakeup(void)
{
	test_semantic_completion_wakes++;
	if (test_semantic_completion_expected_seq != 0) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq),
					 test_semantic_completion_expected_seq);
		UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->record_cas_result),
					 test_semantic_completion_expected_result);
	}
}

UT_TEST(test_first_open_authority_completion_notifies_after_exact_publication)
{
	uint8 empty[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };
	uint64 seq;
	int result;

	test_first_open_reset();
	test_semantic_completion_wakes = 0;
	for (result = 0; result < 2; result++) {
		UT_ASSERT(semantic_activation_record_read_mailbox_submit(&seq));
		test_semantic_completion_expected_seq = seq;
		test_semantic_completion_expected_result = result == 0
													   ? CLUSTER_SEMANTIC_ACTIVATION_OK
													   : CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
		UT_ASSERT_EQ(test_semantic_completion_wakes, (unsigned)result);
		UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
			seq + 1, CLUSTER_SEMANTIC_ACTIVATION_OK, true, empty));
		UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_cas(
			seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
		UT_ASSERT_EQ(test_semantic_completion_wakes, (unsigned)result);
		UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
			seq, test_semantic_completion_expected_result, result == 0, empty));
		UT_ASSERT_EQ(test_semantic_completion_wakes, (unsigned)result + 1);
		UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
			seq, test_semantic_completion_expected_result, result == 0, empty));
		UT_ASSERT_EQ(test_semantic_completion_wakes, (unsigned)result + 1);
	}
	test_semantic_completion_expected_seq = 0;
	test_first_open_finish();
}

/* A BARRIER from the coordinator and a SAMPLE ACK from another member use
 * different connections.  The former is evidence only until the latter
 * completes this receiver's exact initial-round sample. */
static void
test_first_open_barrier_setup(ClusterSemanticActivationAckWireV1 *request,
							  ClusterSemanticActivationAckTableV1 *complete)
{
	ClusterSemanticActivationAckTableV1 *table;
	const uint32 caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	uint64 digest = 0;
	int node;

	test_first_open_reset();
	cluster_node_id = 3;
	test_local_capability_word = caps;
	test_peer_capability_word_sample_ok = true;
	test_peer_capability_word = caps;
	test_peer_capability_generation = 19;
	test_peer_capability_matches = true;
	for (node = 0; node < 4; node++)
		test_remote_admitted_incarnations[node] = UINT64_C(0x100) + (uint64)node;
	table = SemanticActivationAckTable;
	memset(table, 0, sizeof(*table));
	pg_atomic_init_u64(&table->publication_seq, 0);
	table->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	table->coordinator_node = 0;
	table->round_nonce = 79;
	table->expected_members_lo = 15;
	table->observed_members_lo = 11; /* member 2 has not arrived here */
	table->transition_epoch = test_current_epoch;
	table->record_generation = 1;
	table->target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	*complete = *table;
	complete->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
					  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	complete->observed_members_lo = 15;
	for (node = 0; node < 4; node++) {
		SemanticActivationAckTuple *tuple = &complete->observed[node];

		if (node == cluster_node_id)
			UT_ASSERT(semantic_activation_ack_self_tuple(node, caps, test_current_epoch, 1, tuple));
		else {
			tuple->node_id = node;
			tuple->boot_id = test_remote_admitted_incarnations[node];
			tuple->admitted_incarnation = tuple->boot_id;
			tuple->control_connection_generation = 19;
			tuple->capability_word = caps;
			tuple->capability_generation = 19;
			tuple->transition_epoch = test_current_epoch;
			tuple->record_generation = 1;
		}
		complete->expected[node] = *tuple;
		if (node != 2)
			table->observed[node] = *tuple;
	}
	UT_ASSERT(semantic_activation_ack_sample_digest(complete, &digest));
	memset(request, 0, sizeof(*request));
	request->kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	request->stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER;
	request->result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST;
	request->coordinator_node = 0;
	request->member_node = 3;
	request->transition_epoch = test_current_epoch;
	request->record_generation = 1;
	request->round_nonce = 79;
	request->target_feature_bitmap = table->target_feature_bitmap;
	request->admitted_members_lo = 15;
	request->capability_sample_digest = digest;
}

static void
test_first_open_deliver(const ClusterSemanticActivationAckWireV1 *message, int source)
{
	ClusterICEnvelope envelope;
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(message, payload));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	envelope.source_node_id = source;
	envelope.dest_node_id = 3;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&envelope, payload);
	semantic_activation_ack_lmon_drain();
}

UT_TEST(test_first_open_barrier_waits_for_last_sample_ack)
{
	ClusterSemanticActivationAckWireV1 request;
	ClusterSemanticActivationAckWireV1 ack;
	ClusterSemanticActivationAckTableV1 complete;
	int node;

	test_first_open_barrier_setup(&request, &complete);
	test_first_open_deliver(&request, 0);
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	test_first_open_deliver(&request, 0); /* same request, no second owner */
	UT_ASSERT(semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 11);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 0);
	UT_ASSERT_EQ(test_drain_request_calls, 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);

	ack = request;
	ack.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
	ack.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE;
	ack.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
	ack.member_node = 2;
	ack.capability_sample_digest = 0;
	ack.boot_id = complete.observed[2].boot_id;
	ack.admitted_incarnation = complete.observed[2].admitted_incarnation;
	ack.capability_word = complete.observed[2].capability_word;
	test_first_open_deliver(&ack, 2);
	UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
	UT_ASSERT_EQ(SemanticActivationAckTable->stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_BARRIER);
	UT_ASSERT_EQ(SemanticActivationAckTable->flags,
				 CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->capability_sample_digest,
				 request.capability_sample_digest);
	/* Retaining/installing the request is not a positive BARRIER receipt. */
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 0);
	for (node = 0; node < 4; node++)
		UT_ASSERT_EQ(test_send_calls[node], 0);
	test_first_open_finish();
}

UT_TEST(test_first_open_barrier_rejects_changed_authority)
{
	ClusterSemanticActivationAckWireV1 request;
	ClusterSemanticActivationAckTableV1 complete;
	int invalid;

	for (invalid = 0; invalid < 7; invalid++) {
		test_first_open_barrier_setup(&request, &complete);
		switch (invalid) {
		case 0:
			request.round_nonce++;
			break;
		case 1:
			request.transition_epoch++;
			break;
		case 2:
			request.record_generation++;
			break;
		case 3:
			test_peer_capability_matches = false;
			break;
		case 4:
			request.source_feature_bitmap = request.target_feature_bitmap;
			break;
		case 5:
			request.target_feature_bitmap |= CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
			break;
		case 6:
			request.coordinator_node = 1;
			break;
		}
		test_first_open_deliver(&request, 0);
		UT_ASSERT(!semantic_activation_ack_local_request_ahead.valid);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 0);
		test_first_open_finish();
	}
}

UT_TEST(test_first_open_requires_installed_original_input)
{
	ClusterSemanticActivationRefusal refusal;
	int invalid;

	for (invalid = 0; invalid < 10; invalid++) {
		test_first_open_reset();
		switch (invalid) {
		case 0:
			test_first_writer_installed = false;
			break;
		case 1:
			test_first_writer_initialized = false;
			break; /* CLEAN, not kind4 */
		case 2:
			test_first_writer_epoch--;
			break;
		case 3:
			test_first_writer.claim.identity.origin_node_id = 1;
			break;
		case 4:
			test_first_writer.claim.identity.origin_owner_incarnation++;
			break;
		case 5:
			test_first_writer.timeline = 0;
			break;
		case 6:
			IsUnderPostmaster = true;
			break;
		case 7:
			test_route_node_count = 2;
			break;
		case 8:
			pg_atomic_write_u32(&NormalStartCompletion->state, CLUSTER_NORMAL_START_UNCLASSIFIED);
			break;
		case 9:
			pg_atomic_write_u32(&NormalStartCompletion->state, CLUSTER_NORMAL_START_EXISTING_OTHER);
			break;
		}
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationUtilityMailbox->utility_request_seq), 0);
		UT_ASSERT_EQ(test_first_live_checks, 0);
	}
	test_first_open_finish();
}

UT_TEST(test_first_open_request_cannot_publish_admission)
{
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationUtilityRequest request;
	int i;

	test_first_open_reset();
	for (i = 0; i < 3; i++)
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT_EQ(request.request_seq, 1);
	UT_ASSERT_EQ(request.expected_record_generation, 0);
	UT_ASSERT_EQ(request.source_feature_bitmap, 0);
	UT_ASSERT_EQ(request.target_feature_bitmap, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_generation), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits), 0);
	UT_ASSERT_EQ(test_first_live_checks, 0);
	test_first_open_reset();
	cluster_node_id = 1;
	test_first_writer.claim.identity.origin_node_id = 1;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationUtilityMailbox->utility_request_seq), 0);
	test_first_open_finish();
}

UT_TEST(test_first_open_foreign_mailbox_is_not_consumed)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 other;

	test_first_open_reset();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &other));
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(semantic_activation_first_start.request_seq, 0);
	UT_ASSERT(
		semantic_activation_utility_mailbox_complete(other, CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0));
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(semantic_activation_first_start.request_seq, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state),
				 SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE);
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(other, &refusal));
	test_first_open_finish();
}

UT_TEST(test_first_open_exact_reply_is_not_open_proof)
{
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationUtilityRequest request;
	uint64 first;

	test_first_open_reset();
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	first = semantic_activation_first_start.request_seq;
	UT_ASSERT(semantic_activation_utility_mailbox_complete(
		first, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED, 1, 0));
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(semantic_activation_first_start.request_seq, 0);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(semantic_activation_first_start.request_seq, first + 1);
	UT_ASSERT(semantic_activation_utility_mailbox_complete(first + 1,
														   CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0));
	/* The completed R4 gate is an external boundary observation here;
	 * the consumer test below separately exercises the actual R4 path. */
	test_gate_publish(4, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 3, test_current_epoch, false);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT_EQ(request.request_seq, first + 2);
	UT_ASSERT_EQ(request.expected_record_generation, 3);
	UT_ASSERT_EQ(request.source_feature_bitmap, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(request.target_feature_bitmap,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
					 | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1);
	test_first_open_finish();
}

UT_TEST(test_first_open_epoch_or_writer_drift_cannot_rebind)
{
	ClusterSemanticActivationRefusal refusal;
	int drift;

	for (drift = 0; drift < 2; drift++) {
		test_first_open_reset();
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		if (drift == 0) {
			test_current_epoch++;
			test_first_writer_epoch = test_current_epoch;
		} else
			test_first_writer.claim.identity.root_lineage_seq++;
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
		UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationUtilityMailbox->utility_request_seq), 1);
	}
	test_first_open_finish();
}

UT_TEST(test_first_open_requires_both_real_admission_gates)
{
	const uint64 target = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
						  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq;

	test_serving_setup(false);
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	request_seq = semantic_activation_first_start.request_seq;
	test_gate_publish(4, target, 6, test_current_epoch, false);
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	/* Native cutover 1 -> 2 is independent of this membership epoch (7). */
	test_resource_x_gate_snapshot.formation = 2;
	test_resource_x_gate_snapshot.freeze_generation = 1;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal)); /* own reply outstanding */
	UT_ASSERT(semantic_activation_utility_mailbox_complete(request_seq,
														   CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0));
	test_resource_x_gate_snapshot.formation = 0;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_resource_x_gate_snapshot.formation = 2;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_FROZEN;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.freeze_generation = 0;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_resource_x_gate_snapshot.freeze_generation = 1;
	test_gate_publish(6, target, 6, test_current_epoch, true);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_gate_publish(8, target, 5, test_current_epoch, false);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_gate_publish(10, target, 6, test_current_epoch, false);
	test_serving_finish_root();
	UT_ASSERT(cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state),
				 SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
	UT_ASSERT_EQ(test_first_live_checks, 0);
	test_first_open_finish();
}

UT_TEST(test_first_open_lmon_rechecks_live_writer)
{
	ClusterSemanticActivationRefusal refusal;
	SemanticActivationUtilityRequest request;
	int unavailable;

	for (unavailable = 0; unavailable < 3; unavailable++) {
		test_first_open_reset();
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		IsUnderPostmaster = true;
		if (unavailable == 0)
			test_first_writer_initialized = false;
		if (unavailable == 1)
			test_restart_in_recovery = true;
		if (unavailable == 2)
			test_first_live_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		semantic_activation_lmon_consume_utility();
		UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
		UT_ASSERT_EQ(request.request_seq, 1);
		UT_ASSERT_EQ(semantic_activation_lmon_pgrd_request_seq, 0);
		UT_ASSERT_EQ(semantic_activation_lmon_prepare_cas_seq, 0);
		UT_ASSERT(!semantic_activation_first_writer_live());
	}
	test_first_open_finish();
}

UT_TEST(test_first_open_request_drives_original_sample_consumer)
{
	ClusterSemanticActivationRefusal refusal;
	ClusterUndoRootDescriptorV1 descriptor = { 0 };
	ClusterUndoRootDescriptorRequest pgrd;
	ClusterSemanticActivationAckTableV1 table;
	int node;

	test_first_open_reset();
	test_initial_clean_snapshot_valid = true;
	test_initial_clean_snapshot.formation_marker_generation = 3;
	test_initial_clean_snapshot.formation_epoch = test_current_epoch;
	test_initial_clean_snapshot.members_lo = 15;
	test_initial_clean_snapshot.arbiter_node = 0;
	test_initial_clean_snapshot.arbiter_incarnation = test_qvotec_self_incarnation;
	cluster_shared_data_dir = "/cluster-share";
	test_local_capability_word = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
	test_peer_capability_word_sample_ok = test_peer_capability_matches = true;
	test_peer_capability_word = test_local_capability_word;
	test_peer_capability_generation = 19;
	for (node = 0; node < 4; node++) {
		test_remote_admitted_incarnations[node] = test_qvotec_self_incarnation + (uint64)node;
		test_initial_clean_snapshot.admitted_incarnation[node]
			= test_remote_admitted_incarnations[node];
		test_send_results[node] = CLUSTER_IC_SEND_DONE;
	}
	test_last_admitted_incarnation = test_qvotec_self_incarnation;
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	memset(descriptor.root_uuid, 0x6b, sizeof(descriptor.root_uuid));
	descriptor.namespace_id = 1;
	descriptor.system_identifier = test_system_identifier;
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, test_pgrd_candidate));
	test_pgrd_candidate_state = CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
	UT_ASSERT(semantic_activation_pgrd_snapshot_publish(test_pgrd_candidate));
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	IsUnderPostmaster = true;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(&pgrd));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		pgrd.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_ack_table_snapshot(&table));
	UT_ASSERT_EQ(table.stage, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);
	UT_ASSERT_EQ(table.expected_members_lo, 15);
	UT_ASSERT_EQ(table.round_nonce, semantic_activation_first_start.request_seq);
	UT_ASSERT(test_first_live_checks > 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->active_bits), 0);
	IsUnderPostmaster = false;
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	test_first_open_finish();
}
