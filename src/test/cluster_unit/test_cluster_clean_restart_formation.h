/*-------------------------------------------------------------------------
 *
 * test_cluster_clean_restart_formation.h
 *    CLEAN input belongs to the retired boot; service belongs to this one.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_clean_restart_formation.h
 * NOTES
 *    Only native ROOT/INSTALL observations are fixtures. Classification,
 *    loading, publication and ACK consumption use their production owners.
 *-------------------------------------------------------------------------
 */
static ClusterWalStartupCleanInputV1 test_clean_input;
static ClusterControlRootResult test_clean_input_result;
static bool test_clean_installed;

ClusterControlRootResult
cluster_wal_startup_clean_input_v1(ClusterWalStartupCleanInputV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (test_clean_input_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = test_clean_input;
	return test_clean_input_result;
}

bool
cluster_wal_thread_clean_writer_matches(const ClusterWalSourceRef *expected, uint64 epoch)
{
	return test_clean_installed && test_first_writer_installed && epoch == test_first_writer_epoch
		   && memcmp(expected, &test_first_writer, sizeof(*expected)) == 0;
}

static void
test_clean_formation_bind(void)
{
	ClusterSemanticActivationRecord open;
	ClusterControlRootIdentity *id;
	uint8 encoded[CLUSTER_WAL_CLAIM_V2_BYTES];
	cluster_shared_config = true;
	test_current_epoch = test_membership_snapshot_epoch = 2;
	test_gate_publish(2, 0, 0, 2, true);
	test_initial_clean_snapshot.formation_epoch = 2;
	test_initial_clean_snapshot.formation_marker_generation = 2;
	test_initial_clean_snapshot.arbiter_incarnation = 0x100;
	test_serving_formation_valid = true;
	test_serving_formation = 2;
	UT_ASSERT(cluster_semantic_activation_record_decode(test_r4fsm_bootstrap_bytes, &open, NULL));
	open.transition_epoch = 1;
	UT_ASSERT(cluster_semantic_activation_record_encode(&open, test_r4fsm_bootstrap_bytes));
	memset(&test_clean_input, 0, sizeof(test_clean_input));
	id = &test_clean_input.successor.identity;
	id->system_identifier = test_system_identifier;
	memset(id->storage_uuid, 1, sizeof(id->storage_uuid));
	memset(id->authority_uuid, 2, sizeof(id->authority_uuid));
	id->origin_thread_id = cluster_node_id + 1;
	id->origin_node_id = cluster_node_id;
	id->thread_claim_created_at = 8;
	id->origin_owner_incarnation = test_qvotec_self_incarnation;
	id->root_lineage_seq = 1;
	test_clean_input.predecessor = *id;
	id->root_lineage_seq++;
	test_clean_input.predecessor.origin_owner_incarnation = 100 + cluster_node_id;
	memset(test_clean_input.predecessor_claim_sha256, 3, 32);
	test_clean_input.successor.database_incarnation = 1;
	test_clean_input.successor.config_generation = 1;
	test_clean_input.successor.claim_generation = 2;
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&test_clean_input.successor, encoded),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	id->thread_claim_crc32c = semantic_activation_read_u32_le(encoded + 104);
	test_clean_input.formation_epoch = 2;
	test_clean_input.config_generation = 1;
	test_clean_input.predecessor_root_sequence = 12;
	memset(test_clean_input.predecessor_root_sha256, 4, 32);
	memset(test_clean_input.exit_evidence_sha256, 5, 32);
	memset(test_clean_input.operation_uuid, 6, 16);
	test_clean_input.operation_generation = 2;
	test_clean_input.checkpoint_lsn = 0x80;
	test_clean_input.checkpoint_end = 0x100;
	test_clean_input.checkpoint_crc32c = 123;
	test_clean_input.timeline = 1;
	test_clean_input_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_clean_installed = false;
	test_first_writer_installed = false;
}

static void
test_clean_formation_input(void)
{
	ut_normal_start_setup();
	test_clean_formation_bind();
}

static void
test_clean_install(void)
{
	uint8 encoded[CLUSTER_WAL_CLAIM_V2_BYTES];
	pg_cryptohash_ctx *hash;
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&test_clean_input.successor, encoded),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&test_first_writer, 0, sizeof(test_first_writer));
	test_first_writer.claim.identity = test_clean_input.successor.identity;
	test_first_writer.claim.database_incarnation = test_clean_input.successor.database_incarnation;
	test_first_writer.claim.max_config_generation = test_clean_input.successor.config_generation;
	test_first_writer.timeline = test_clean_input.timeline;
	hash = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT_NOT_NULL(hash);
	UT_ASSERT_EQ(pg_cryptohash_init(hash), 0);
	UT_ASSERT_EQ(pg_cryptohash_update(hash, encoded, sizeof(encoded)), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(hash, test_first_writer.claim.claim_sha256, 32), 0);
	pg_cryptohash_free(hash);
	test_first_writer_installed = test_clean_installed = true;
	test_first_writer_initialized = false;
	test_first_writer_epoch = 2;
}

static void
test_clean_ready_for_node(int node)
{
	char oldpath[MAXPGPATH], newpath[MAXPGPATH];
	const char *failure;
	ut_cold_setup(2);
	snprintf(oldpath, sizeof(oldpath), "%s/pg_undo/instance_%d", cold_root_path, cluster_node_id);
	snprintf(newpath, sizeof(newpath), "%s/pg_undo/instance_%d", cold_root_path, node);
	UT_ASSERT_EQ(rename(oldpath, newpath), 0);
	cluster_node_id = node;
	test_qvotec_self_incarnation = test_last_admitted_incarnation = 0x100 + node;
	test_clean_formation_bind();
	ut_cold_file(255, TT_SLOT_ABORTED);
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	test_clean_install();
	UT_ASSERT(cluster_semantic_normal_start_finish(&failure));
	MyAuxProcType = LmonProcess;
}

UT_TEST(test_clean_formation_old_pgsa_classifies_loading_at_current_epoch)
{
	const char *failure;
	test_clean_formation_input();
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	if (failure != NULL)
		printf("# CLEAN prepare refusal: %s\n", failure);
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_TARGET_LOADING);
	UT_ASSERT_EQ(NormalStartCompletion->epoch, 2);
	UT_ASSERT_EQ(memcmp(NormalStartCompletion->pgsa, test_r4fsm_bootstrap_bytes, 512), 0);
	UT_ASSERT(cluster_semantic_normal_start_closed());
	cluster_shared_config = false;
	test_gate_reset();
}

UT_TEST(test_clean_formation_missing_original_input_cannot_classify_ready)
{
	const char *failure;
	test_clean_formation_input();
	test_clean_input_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	UT_ASSERT(!cluster_semantic_normal_start_prepare(true, 0, &failure));
	UT_ASSERT_STR_EQ(failure, "NORMAL_START_CLEAN_INPUT_READ_UNPROVEN");
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_FAILED);
	UT_ASSERT(!semantic_activation_restart.local_ready);
	cluster_shared_config = false;
	test_gate_reset();
}

UT_TEST(test_clean_formation_bridge_contradictions_fail_before_loading)
{
	const char *failure;
	for (int fault = 0; fault < 12; fault++) {
		test_clean_formation_input();
		switch (fault) {
		case 0:
			test_clean_input.formation_epoch++;
			break;
		case 1:
			test_clean_input.predecessor.system_identifier++;
			break;
		case 2:
			test_clean_input.successor.identity.origin_owner_incarnation++;
			break;
		case 3:
			test_clean_input.checkpoint_lsn++;
			break;
		case 4:
			memset(test_clean_input.exit_evidence_sha256, 0, 32);
			break;
		case 5:
			memset(test_clean_input.predecessor_root_sha256, 0, 32);
			break;
		case 6:
			test_clean_input.successor.config_generation++;
			break;
		case 7:
			test_clean_input.operation_generation = 0;
			break;
		case 8:
			test_membership_snapshot_lo = 7;
			break;
		case 9:
			test_serving_formation = 0;
			break;
		case 10:
			test_last_admitted_incarnation++;
			break;
		case 11:
			test_clean_input.predecessor.origin_owner_incarnation = test_qvotec_self_incarnation;
			break;
		}
		UT_ASSERT(!cluster_semantic_normal_start_prepare(true, 0, &failure));
		UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_FAILED);
		UT_ASSERT(!semantic_activation_restart.opened);
		cluster_shared_config = false;
		test_gate_reset();
	}
}

UT_TEST(test_clean_formation_actual_loader_requires_exact_installed_claim)
{
	const char *failure;
	for (int fault = -1; fault < 6; fault++) {
		ut_cold_setup(2);
		test_clean_formation_bind();
		ut_cold_file(255, TT_SLOT_ABORTED);
		UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
		test_clean_install();
		switch (fault) {
		case 0:
			test_first_writer_installed = false;
			break;
		case 1:
			test_clean_installed = false;
			break;
		case 2:
			test_first_writer.claim.claim_sha256[0] ^= 1;
			break;
		case 3:
			test_first_writer_epoch++;
			break;
		case 4:
			test_first_writer.timeline++;
			break;
		case 5:
			test_remote_admitted_incarnations[2]++;
			break;
		}
		UT_ASSERT_EQ(cluster_semantic_normal_start_finish(&failure), fault == -1);
		UT_ASSERT_EQ(cluster_semantic_normal_start_state(),
					 fault == -1 ? CLUSTER_NORMAL_START_TARGET_READY : CLUSTER_NORMAL_START_FAILED);
		UT_ASSERT_EQ(cold_read_count[255], fault == -1 ? 1 : 0);
		UT_ASSERT(!semantic_activation_restart.local_ready);
		cluster_shared_config = false;
		ut_cold_cleanup();
	}
}

UT_TEST(test_clean_formation_ready_enters_original_restart_read_owner)
{
	const char *failure;
	ClusterSemanticActivationReadRequest request;
	ut_cold_setup(2);
	test_clean_formation_bind();
	ut_cold_file(255, TT_SLOT_ABORTED);
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	test_clean_install();
	UT_ASSERT(cluster_semantic_normal_start_finish(&failure));
	MyAuxProcType = LmonProcess;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_restart.read_seq != 0);
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT_EQ(request.request_seq, semantic_activation_restart.read_seq);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	UT_ASSERT(!semantic_activation_restart.opened);
	cluster_shared_config = false;
	ut_cold_cleanup();
}

UT_TEST(test_clean_restart_rejects_previous_ack_wire_version)
{
	ClusterSemanticActivationAckWireV1 message = { 0 }, decoded;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	message.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	message.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	message.transition_epoch = 2;
	message.record_generation = 6;
	message.round_nonce = 23;
	message.source_feature_bitmap = 1;
	message.target_feature_bitmap = 1025;
	message.admitted_members_lo = 15;
	message.capability_sample_digest = 0xabc123;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&message, bytes));
	bytes[4] = 1;
	bytes[5] = 0;
	UT_ASSERT(!cluster_semantic_activation_ack_wire_decode(bytes, &decoded));
}

static uint64
test_clean_round_begin(int node)
{
	uint64 nonce;
	test_clean_ready_for_node(node);
	cluster_semantic_activation_lmon_tick();
	nonce = ut_a142_complete_open_read(0);
	if (node != 0) {
		nonce = 23;
		ut_a142_frame(0, false, nonce, 0);
	}
	ut_a142_local_root(false);
	UT_ASSERT(semantic_activation_restart.local_ready);
	UT_ASSERT(!semantic_activation_restart.failed);
	return nonce;
}

static void
test_clean_round_finish(uint64 nonce)
{
	uint64 generation;
	for (int peer = 0; peer < 4; peer++)
		if (peer != cluster_node_id)
			ut_a142_frame(peer, true, nonce, 0x100 + peer);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 15);
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	test_resource_x_cutover_digest_valid = true;
	test_resource_x_cutover_digest = 99;
	test_resource_x_cutover_token.old_formation = 1;
	test_resource_x_cutover_token.new_formation = 2;
	test_resource_x_cutover_token.freeze_generation = 1;
	cluster_semantic_activation_lmon_tick();
	test_resource_x_cutover_thawed = true;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(semantic_activation_restart.opened);
	UT_ASSERT_EQ(cluster_resource_x_writer_path_snapshot(&generation), RESOURCE_X_WRITER_TARGET);
	UT_ASSERT_EQ(generation, 6);
}

UT_TEST(test_clean_formation_all_nodes_reach_current_open_without_rewriting_pgsa)
{
	for (int node = 0; node < 4; node++) {
		ClusterSemanticActivationRecord old;
		uint64 epoch = UINT64_MAX;
		uint64 nonce = test_clean_round_begin(node);
		UT_ASSERT(
			cluster_semantic_activation_record_decode(NormalStartCompletion->pgsa, &old, NULL));
		UT_ASSERT_EQ(old.transition_epoch, 1);
		UT_ASSERT(
			cluster_semantic_normal_stop_current_epoch(&old, NormalStartCompletion->pgrd, &epoch)
			!= CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(epoch, 0);
		test_clean_round_finish(nonce);
		UT_ASSERT_EQ(
			cluster_semantic_normal_stop_current_epoch(&old, NormalStartCompletion->pgrd, &epoch),
			CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(epoch, 2);
		UT_ASSERT_EQ(memcmp(NormalStartCompletion->pgsa, test_r4fsm_bootstrap_bytes, 512), 0);
		/* ROOT publication is still a separate prerequisite to SQL admission. */
		UT_ASSERT_EQ(test_serving_calls, 0);
		ut_cold_cleanup();
	}
}

UT_TEST(test_clean_formation_current_epoch_refuses_changed_bridge_and_carriers)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterSemanticActivationRecord old;
		uint64 epoch = UINT64_MAX;
		uint64 nonce = test_clean_round_begin(1);
		test_clean_round_finish(nonce);
		UT_ASSERT(
			cluster_semantic_activation_record_decode(NormalStartCompletion->pgsa, &old, NULL));
		switch (fault) {
		case 0:
			SemanticActivationAckTable->restart_binding[0] ^= 1;
			break;
		case 1:
			NormalStartCompletion->clean_input.predecessor_root_sha256[0] ^= 1;
			break;
		case 2:
			NormalStartCompletion->clean_input.exit_evidence_sha256[0] ^= 1;
			break;
		case 3:
			test_first_writer.claim.claim_sha256[0] ^= 1;
			break;
		case 4:
			test_clean_installed = false;
			break;
		case 5:
			test_current_epoch++;
			break;
		case 6:
			test_serving_formation++;
			break;
		case 7:
			test_remote_admitted_incarnations[3]++;
			break;
		case 8:
			SemanticActivationAckTable->observed_members_lo &= ~8;
			break;
		case 9:
			old.transition_epoch = 2;
			break;
		}
		UT_ASSERT(
			cluster_semantic_normal_stop_current_epoch(&old, NormalStartCompletion->pgrd, &epoch)
			!= CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(epoch, 0);
		ut_cold_cleanup();
	}
}

UT_TEST(test_clean_formation_early_late_duplicate_ack_keeps_exact_round)
{
	uint64 nonce = 23;
	test_clean_ready_for_node(3);
	test_restart_in_recovery = true;
	ut_a142_frame(1, true, nonce, 0x101);
	ut_a142_frame(1, true, nonce, 0x101);
	UT_ASSERT_EQ(semantic_activation_restart.early.count, 0);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	UT_ASSERT(!semantic_activation_restart.opened);
	test_restart_in_recovery = false;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_restart.early.count, 1);
	(void)ut_a142_complete_open_read(0);
	ut_a142_frame(0, false, nonce, 0);
	ut_a142_local_root(false);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 10);
	ut_a142_frame(1, true, nonce, 0x101);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 10);
	ut_a142_frame(2, true, nonce + 1, 0x102);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 10);
	UT_ASSERT(!semantic_activation_restart.failed);
	test_clean_round_finish(nonce);
	ut_cold_cleanup();
}

UT_TEST(test_clean_formation_binding_ack_cannot_be_reused_for_another_root)
{
	ClusterSemanticActivationAckWireV1 ack;
	ClusterICEnvelope env = { 0 };
	uint8 payload[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];
	uint64 nonce = test_clean_round_begin(3);
	UT_ASSERT(cluster_semantic_activation_ack_wire_decode(test_send_payloads[1], &ack));
	ack.member_node = 1;
	ack.boot_id = ack.admitted_incarnation = 0x101;
	ack.restart_binding[0] ^= 1;
	UT_ASSERT(cluster_semantic_activation_ack_wire_encode(&ack, payload));
	env.msg_type = PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1;
	env.source_node_id = 1;
	env.dest_node_id = 3;
	env.epoch = 2;
	env.payload_length = sizeof(payload);
	cluster_semantic_activation_ack_handler(&env, payload);
	semantic_activation_ack_lmon_drain();
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 8);
	UT_ASSERT(!semantic_activation_restart.opened);
	ut_a142_frame(1, true, nonce, 0x101);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 10);
	ut_cold_cleanup();
}

UT_TEST(test_clean_formation_first_request_survives_membership_observation_gap)
{
	test_clean_ready_for_node(3);
	test_membership_snapshot_valid = false;
	ut_a142_frame(0, false, 23, 0);
	UT_ASSERT(!semantic_activation_restart.requested);
	test_membership_snapshot_valid = true;
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	UT_ASSERT(semantic_activation_restart.requested);
	UT_ASSERT_EQ(SemanticActivationAckTable->round_nonce, 23);
	ut_a142_local_root(false);
	test_clean_round_finish(23);
	ut_cold_cleanup();
}

UT_TEST(test_clean_formation_stop_terminal_receipt_is_not_partial_open)
{
	ClusterSemanticActivationRecord old;
	uint64 epoch;
	uint64 nonce = test_clean_round_begin(1);
	test_clean_round_finish(nonce);
	UT_ASSERT(cluster_semantic_activation_record_decode(NormalStartCompletion->pgsa, &old, NULL));
	for (int node = 0; node < 4; node++) {
		test_observed_slot_valid[node] = true;
		test_observed_slot_incarnation[node] = 0x100 + node;
		test_observed_slot_generation[node] = 20 + node;
		test_observed_slot_epoch[node] = 2;
	}
	test_stop_real_observation = true;
	test_terminal_peer_open = old;
	memcpy(test_terminal_peer_root, NormalStartCompletion->pgrd, 512);
	test_terminal_peer_record_enabled = test_terminal_peer_eligible = true;
	test_terminal_peer_record
		= (ClusterSfPeerCap){ .valid = true, .bits = test_peer_capability_word, .generation = 19 };
	UT_ASSERT(cluster_sf_peer_cap_invalidate_gen(&test_terminal_peer_record, 19));
	test_stop_not_fresh_peer = 3;
	UT_ASSERT_EQ(
		cluster_semantic_normal_stop_current_epoch(&old, NormalStartCompletion->pgrd, &epoch),
		CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(epoch, 2);
	test_terminal_peer_eligible = false;
	epoch = UINT64_MAX;
	UT_ASSERT(cluster_semantic_normal_stop_current_epoch(&old, NormalStartCompletion->pgrd, &epoch)
			  != CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(epoch, 0);
	test_terminal_peer_record_enabled = test_terminal_peer_eligible = false;
	ut_cold_cleanup();
}

UT_TEST(test_clean_formation_preinstall_input_does_not_borrow_final_admission)
{
	const char *failure;
	ut_cold_setup(2);
	test_clean_formation_bind();
	ut_cold_file(255, TT_SLOT_ABORTED);
	/* Real native ordering: accepted complete PGFM precedes INSTALL; the
	 * final join/stripe admission is still closed at this original call. */
	test_clean_admitted = false;
	test_initial_clean_snapshot_valid = false;
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_TARGET_LOADING);
	test_clean_install();
	UT_ASSERT(cluster_semantic_normal_start_finish(&failure));
	MyAuxProcType = LmonProcess;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(semantic_activation_restart.read_seq, 0);
	UT_ASSERT(!semantic_activation_restart.opened);
	UT_ASSERT_EQ(SemanticActivationAckTable->observed_members_lo, 0);
	test_clean_admitted = true;
	cluster_semantic_activation_lmon_tick();
	(void)ut_a142_complete_open_read(0);
	ut_a142_frame(0, false, 23, 0);
	if (!semantic_activation_restart.have_open) {
		cluster_semantic_activation_lmon_tick();
		(void)ut_a142_complete_open_read(0);
	}
	UT_ASSERT(semantic_activation_restart.have_open);
	ut_a142_local_root(false);
	test_clean_round_finish(23);
	ut_cold_cleanup();
}
