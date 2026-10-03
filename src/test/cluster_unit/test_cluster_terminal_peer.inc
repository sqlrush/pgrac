/* SD98 database identity tests. The real producer/OPEN FSM and observed-slot
 * predicate run here; external QVOTEC/transport samples are explicit inputs.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
ut_terminal_members_and_sessions(void)
{
	test_stop_real_observation = true;
	for (int node = 0; node < 4; node++) {
		test_observed_slot_valid[node] = true;
		test_observed_slot_incarnation[node] = cluster_membership_get_last_admitted_incarnation(node);
		test_observed_slot_generation[node] = 20 + node;
		test_observed_slot_epoch[node] = test_current_epoch;
	}
	test_terminal_sessions.control_stream_generation = 73;
	test_terminal_sessions.data_channels = (uint32)cluster_lms_workers;
	for (int ch = 0; ch < cluster_lms_workers; ch++)
		test_terminal_sessions.data_stream_generation[ch] = 40 + ch;
}

static void
ut_terminal_setup(ClusterSemanticAdmissionToken *token, uint64 epoch)
{
	ClusterSemanticActivationAckTableV1 image;

	ut_resource_x_open_carrier_setup_at_epoch(token, epoch);
	ut_terminal_members_and_sessions();
	image = *SemanticActivationAckTable;
	UT_ASSERT(semantic_activation_ack_table_publish(&image));
	memset(token, 0, sizeof(*token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, token), CLUSTER_SEMANTIC_ADMISSION_OK);
}

UT_TEST(test_terminal_peer_stale_origin_has_only_inquiry_qualification)
{
	for (int epoch = 0; epoch <= 7; epoch += 7) {
		ClusterSemanticAdmissionToken token;
		ClusterSemanticTerminalPeerSnapshot got;
		ClusterSemanticActivationAckTableV1 before;

		ut_terminal_setup(&token, epoch);
		UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 2,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &got));
		test_stop_not_fresh_peer = 2;
		before = *SemanticActivationAckTable;
		UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 2,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &got));
		UT_ASSERT_EQ(got.peer_boot_id, test_remote_admitted_incarnations[2]);
		UT_ASSERT_EQ(got.formation_epoch, epoch);
		UT_ASSERT_EQ(got.peer_data_generation[3], 43);
		UT_ASSERT(cluster_semantic_activation_terminal_peer_current(&token, &got));
		UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&token, 2,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, 19));
		UT_ASSERT(memcmp(&before, SemanticActivationAckTable, sizeof(before)) == 0);
		UT_ASSERT(!cluster_reconfig_get_observed_fresh_alive(2));
		/* A later heartbeat publication is not a different admitted owner. */
		test_observed_slot_generation[2]++;
		UT_ASSERT(cluster_semantic_activation_terminal_peer_current(&token, &got));
		cluster_semantic_activation_leave(&token);
		UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &got));
	}
	test_gate_reset();
}

UT_TEST(test_terminal_peer_missing_authority_clears_output)
{
	for (int fault = 0; fault < 30; fault++) {
		ClusterSemanticAdmissionToken token, candidate;
		ClusterSemanticTerminalPeerSnapshot got, empty = { 0 };
		ClusterSemanticActivationAckTableV1 *table;
		uint32 caps = CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS;
		int peer = 2;

		ut_terminal_setup(&token, 7);
		candidate = token;
		table = SemanticActivationAckTable;
		test_stop_not_fresh_peer = 2;
		switch (fault) {
		case 0: candidate.entered = false; break;
		case 1: candidate.side = CLUSTER_SEMANTIC_SOURCE_SIDE; break;
		case 2: candidate.record_generation++; break;
		case 3: candidate.formation_epoch++; break;
		case 4: test_observed_slot_valid[2] = false; break;
		case 5: test_observed_slot_incarnation[2]++; break;
		case 6: test_observed_slot_epoch[2]++; break;
		case 7: test_terminal_nonmember = 2; break;
		case 8: test_terminal_membership_cut = 0; break;
		case 9: test_qvotec_in_quorum = false; break;
		case 10: test_stop_storage_quorum = false; break;
		case 11: test_terminal_qv_status = CLUSTER_QVOTEC_STARTING; break;
		case 12: test_stop_not_fresh_peer = 3; break;
		case 13: table->observed_members_lo &= ~UINT64_C(4); break;
		case 14: table->expected[2].boot_id++; break;
		case 15: table->flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE; break;
		case 16: table->target_feature_bitmap &= ~token.feature_bit; break;
		case 17: test_capability_missing_peer = 2; break;
		case 18: test_peer_capability_generation++; break;
		case 19: test_peer_capability_word = 0; break;
		case 20: test_terminal_sessions_ok = false; break;
		case 21: test_terminal_fenced = true; break;
		case 22: test_terminal_prebump = true; break;
		case 23: test_terminal_join = true; break;
		case 24: test_terminal_stop = true; break;
		case 25: test_restart_in_recovery = true; break;
		case 26: test_grd_recovery_state = GRD_RECOVERY_WAIT_CLUSTER; break;
		case 27: caps = PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1; break;
		case 28: peer = cluster_node_id; break;
		case 29: table->flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_OPEN_PROOF; break;
		}
		memset(&got, 0xa5, sizeof(got));
		UT_ASSERT(!cluster_semantic_activation_terminal_peer_capture(&candidate, peer, caps, &got));
		UT_ASSERT(memcmp(&got, &empty, sizeof(got)) == 0);
		cluster_semantic_activation_leave(&token);
	}
	test_gate_reset();
}

UT_TEST(test_terminal_peer_aba_invalidates_original_snapshot)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterSemanticAdmissionToken token;
		ClusterSemanticTerminalPeerSnapshot old, current;
		ClusterSemanticActivationAckTableV1 image;

		ut_terminal_setup(&token, 7);
		UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 2,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &old));
		switch (fault) {
		case 0: test_terminal_membership_cut += 4; break;
		case 1:
			image = *SemanticActivationAckTable;
			UT_ASSERT(semantic_activation_ack_table_publish(&image));
			break;
		case 2: test_terminal_sessions.control_stream_generation++; break;
		case 3: test_terminal_sessions.data_stream_generation[2]++; break;
		case 4:
			test_gate_publish(4, SemanticActivationAckTable->target_feature_bitmap, 6, 7, false);
			break;
		}
		UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &old));
		UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 2,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &current));
		UT_ASSERT(cluster_semantic_activation_terminal_peer_current(&token, &current));
		cluster_semantic_activation_leave(&token);
	}
	test_gate_reset();
}

static void
ut_terminal_mid_sample_change(void)
{
	test_terminal_membership_cut += 2;
}
UT_TEST(test_terminal_peer_mid_sample_drift_is_refused)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticTerminalPeerSnapshot got, empty = { 0 };
	ut_terminal_setup(&token, 7);
	test_terminal_session_hook_at = 2;
	test_terminal_session_hook = ut_terminal_mid_sample_change;
	memset(&got, 0xa5, sizeof(got));
	UT_ASSERT(!cluster_semantic_activation_terminal_peer_capture(&token, 2,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &got));
	UT_ASSERT(memcmp(&got, &empty, sizeof(got)) == 0);
	cluster_semantic_activation_leave(&token);
	test_gate_reset();
}

UT_TEST(test_terminal_peer_uses_actual_recovered_open_carrier)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticTerminalPeerSnapshot got;
	ut_open_proof_env_setup(3);
	UT_ASSERT(cluster_semantic_activation_restore_open_proof_if_active());
	test_gate_publish(2, SemanticActivationAckTable->target_feature_bitmap, 3, 7, false);
	ut_terminal_members_and_sessions();
	test_peer_capability_matches = true;
	test_stop_not_fresh_peer = 1;
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &token), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(cluster_semantic_activation_terminal_peer_capture(&token, 1,
		CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS, &got));
	UT_ASSERT(cluster_semantic_activation_terminal_peer_current(&token, &got));
	pg_atomic_write_u32(&SemanticActivationBit22Latch->active, 0);
	UT_ASSERT(!cluster_semantic_activation_terminal_peer_current(&token, &got));
	cluster_semantic_activation_leave(&token);
	test_gate_reset();
}
