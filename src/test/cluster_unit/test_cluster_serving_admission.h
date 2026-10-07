/* Exercise final SQL admission independently of the original semantic gates.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
test_serving_semantic_open(bool restart)
{
	test_first_open_reset();
	if (restart)
		pg_atomic_write_u32(&NormalStartCompletion->state, CLUSTER_NORMAL_START_TARGET_READY);
	test_gate_publish(4,
					  CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
						  | CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
					  6, test_current_epoch, false);
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.formation = 2;
	test_resource_x_gate_snapshot.freeze_generation = 1;
}

UT_TEST(test_first_start_sql_waits_for_root_serving)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_semantic_open(false);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_first_open_finish();
}

UT_TEST(test_clean_restart_sql_waits_for_root_serving)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_semantic_open(true);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_first_open_finish();
}

static ClusterSemanticActivationRecord test_serving_expected_open;
static uint8 test_serving_expected_root[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

static void
test_serving_setup(bool restart)
{
	ut_a148_stop_identity(&test_serving_expected_open, test_serving_expected_root, 7);
	cluster_shared_config = true;
	test_route_node_count = 4;
	test_serving_formation_valid = true;
	test_first_writer_installed = test_first_writer_initialized = true;
	test_first_writer_epoch = test_current_epoch;
	test_first_live_checks = 0;
	memset(&test_first_writer, 0, sizeof(test_first_writer));
	test_first_writer.timeline = 1;
	test_first_writer.claim.identity.origin_node_id = cluster_node_id;
	test_first_writer.claim.identity.origin_owner_incarnation = test_qvotec_self_incarnation;
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.formation = 2;
	test_resource_x_gate_snapshot.freeze_generation = 1;
	pg_atomic_write_u32(&NormalStartCompletion->state, restart ? CLUSTER_NORMAL_START_TARGET_READY
															   : CLUSTER_NORMAL_START_SOURCE_ZERO);
	IsUnderPostmaster = false;
	test_serving_token.file_txn_seq = 10;
	test_serving_token.format_version = 3;
	test_serving_token.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	test_serving_token.record_count = 4;
	test_serving_token.system_identifier = test_system_identifier;
	memset(test_serving_token.authority_uuid, 0x37, sizeof(test_serving_token.authority_uuid));
	memset(test_serving_token.image_sha256, 0x71, sizeof(test_serving_token.image_sha256));
}

static void
test_serving_tick(void)
{
	IsUnderPostmaster = true;
	MyAuxProcType = LmonProcess;
	cluster_semantic_activation_lmon_tick();
	MyAuxProcType = NotAnAuxProcess;
	IsUnderPostmaster = false;
}

static void
test_serving_complete_read(void)
{
	uint64 seq = semantic_serving.read_seq;
	UT_ASSERT(seq != 0);
	UT_ASSERT(cluster_semantic_activation_record_encode(
		&test_serving_expected_open, SemanticActivationShmem->record_cas_desired_bytes));
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 0;
	UT_ASSERT(semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
}

static void
test_serving_finish_root(void)
{
	test_serving_tick();
	test_serving_complete_read();
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_calls, 1);
	UT_ASSERT_EQ(semantic_serving.read_seq, 0);
}

UT_TEST(test_serving_owner_waits_for_exact_root_release_and_frees_mailbox)
{
	for (unsigned restart = 0; restart < 2; restart++) {
		ClusterSemanticActivationRefusal refusal;
		test_serving_setup(restart != 0);
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 0);
		test_serving_complete_read();
		test_serving_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 1);
		UT_ASSERT_EQ(semantic_serving.read_seq, 0);
		UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		test_serving_result = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 2);
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 3);
		UT_ASSERT(memcmp(&test_serving_open, &test_serving_expected_open, sizeof(test_serving_open))
				  == 0);
		UT_ASSERT(memcmp(test_serving_root, test_serving_expected_root, sizeof(test_serving_root))
				  == 0);
		UT_ASSERT(cluster_semantic_activation_startup_poll(&refusal));
		UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 3); /* completed same cut, no repeated disk poll */
		test_first_open_finish();
	}
}

UT_TEST(test_serving_rejects_failed_or_empty_completion)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterSemanticActivationRefusal refusal;
		test_serving_setup(false);
		test_serving_tick();
		test_serving_complete_read();
		test_serving_result
			= fault == 0 ? CLUSTER_CONTROL_ROOT_STALE_TOKEN : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		if (fault == 1)
			memset(&test_serving_token, 0, sizeof(test_serving_token));
		if (fault == 2)
			test_serving_token.system_identifier++;
		test_serving_tick();
		UT_ASSERT_EQ(test_serving_calls, 1);
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
		test_first_open_finish();
	}
}

UT_TEST(test_serving_retries_transient_root_conflict_without_ready)
{
	const ClusterControlRootResult transient[]
		= { CLUSTER_CONTROL_ROOT_STALE_TOKEN, CLUSTER_CONTROL_ROOT_CAS_CONFLICT };
	for (unsigned restart = 0; restart < 2; restart++) {
		for (unsigned fault = 0; fault < lengthof(transient); fault++) {
			ClusterSemanticActivationRefusal refusal;
			test_serving_setup(restart != 0);
			test_serving_tick();
			test_serving_complete_read();
			test_serving_result = transient[fault];
			for (unsigned attempt = 0; attempt < 3; attempt++) {
				test_serving_tick();
				UT_ASSERT_EQ(test_serving_calls, attempt + 1);
				UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
				UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
			}
			test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			test_serving_tick();
			UT_ASSERT_EQ(test_serving_calls, 4);
			UT_ASSERT(cluster_semantic_activation_startup_poll(&refusal));
			test_first_open_finish();
		}
	}
}

UT_TEST(test_serving_transient_retry_still_rejects_changed_epoch)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_setup(false);
	test_serving_tick();
	test_serving_complete_read();
	test_serving_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	test_serving_tick();
	test_current_epoch++;
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_calls, 1);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
	test_first_open_finish();
}

UT_TEST(test_serving_identity_mismatch_stays_terminal_for_same_cut)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_setup(false);
	test_serving_tick();
	test_serving_complete_read();
	test_serving_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	test_serving_tick();
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_calls, 1);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
	test_first_open_finish();
}

UT_TEST(test_serving_old_result_rejected_after_cut_changes)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterSemanticActivationRefusal refusal;
		test_serving_setup(false);
		test_serving_finish_root();
		UT_ASSERT(cluster_semantic_activation_startup_poll(&refusal));
		switch (fault) {
		case 0:
			test_current_epoch++;
			break;
		case 1:
			test_serving_formation++;
			break;
		case 2:
			test_terminal_membership_cut += 2;
			break;
		case 3:
			test_remote_admitted_incarnations[2]++;
			break;
		case 4:
			test_gate_publish(4, test_serving_expected_open.target_feature_bitmap, 6, 7, true);
			break;
		case 5:
			pg_atomic_fetch_add_u64(&SemanticActivationAckTable->publication_seq, 2);
			break;
		case 6:
			pg_atomic_fetch_add_u64(&SemanticActivationPgrdSnapshot->publication_seq, 2);
			break;
		case 7:
			test_terminal_stop = true;
			break;
		case 8:
			test_peer_capability_generation++;
			test_serving_tick(); /* original LMON owns live capability observation */
			break;
		}
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		test_serving_tick();
		UT_ASSERT_EQ(SemanticServingReady->cut.admission_seq, 0);
		test_first_open_finish();
	}
}

static void
test_serving_drift_hook(void)
{
	test_serving_formation++;
}

UT_TEST(test_serving_rechecks_identity_after_root_poll)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_setup(false);
	test_serving_tick();
	test_serving_complete_read();
	test_serving_hook = test_serving_drift_hook;
	test_serving_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_calls, 1);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT_EQ(SemanticServingReady->cut.admission_seq, 0);
	test_first_open_finish();
}

UT_TEST(test_serving_only_original_lmon_consumes_complete_open)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		test_serving_setup(false);
		switch (fault) {
		case 0:
			cluster_shared_config = false;
			break;
		case 1:
			SemanticActivationAckTable->observed_members_lo &= ~UINT64_C(8);
			break;
		case 2:
			SemanticActivationAckTable->flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
			break;
		case 3:
			test_serving_formation_valid = false;
			break;
		case 4:
			break; /* postmaster, never the ROOT caller */
		}
		if (fault == 4) {
			cluster_semantic_activation_lmon_tick();
		} else
			test_serving_tick();
		UT_ASSERT_EQ(semantic_serving.read_seq, 0);
		UT_ASSERT_EQ(test_serving_calls, 0);
		UT_ASSERT_EQ(SemanticServingReady->cut.admission_seq, 0);
		test_first_open_finish();
	}
}

UT_TEST(test_serving_releases_read_before_rejecting_invalid_durable_input)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterSemanticActivationRefusal refusal;
		test_serving_setup(false);
		test_serving_tick();
		test_serving_complete_read();
		if (fault == 0)
			SemanticActivationShmem->record_cas_desired_bytes[0] ^= 1;
		if (fault == 1)
			SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 1;
		if (fault == 2)
			pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result,
								CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		test_serving_tick();
		UT_ASSERT_EQ(semantic_serving.read_seq, 0);
		UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
		UT_ASSERT_EQ(test_serving_calls, 0);
		UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
		test_first_open_finish();
	}
}

UT_TEST(test_serving_respects_other_mailbox_owner_and_cancels_on_stop)
{
	uint64 seq;
	test_serving_setup(false);
	UT_ASSERT(
		semantic_activation_record_read_mailbox_submit(&semantic_activation_lmon_stop_read_seq));
	seq = semantic_activation_lmon_stop_read_seq;
	test_serving_tick();
	UT_ASSERT_EQ(semantic_serving.read_seq, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), seq);
	test_first_open_finish();

	test_serving_setup(false);
	test_serving_tick();
	test_serving_complete_read();
	test_serving_tick(); /* ROOT waiting for the coordinator or its CF release */
	UT_ASSERT_EQ(test_serving_calls, 1);
	test_terminal_stop = true;
	test_serving_tick();
	UT_ASSERT_EQ(test_serving_cancels, 1);
	UT_ASSERT(!semantic_serving.polling);
	UT_ASSERT_EQ(semantic_activation_authority_mailbox_owner(true), -1);
	test_first_open_finish();
}

UT_TEST(test_serving_drift_does_not_orphan_inflight_authority_read)
{
	uint64 seq;
	test_serving_setup(false);
	test_serving_tick();
	seq = semantic_serving.read_seq;
	test_current_epoch++;
	test_serving_tick();
	UT_ASSERT_EQ(semantic_serving.read_seq, seq);
	test_serving_complete_read();
	test_serving_tick();
	UT_ASSERT_EQ(semantic_serving.read_seq, 0);
	UT_ASSERT_EQ(test_serving_calls, 0);
	UT_ASSERT_EQ(SemanticServingReady->cut.admission_seq, 0);
	test_first_open_finish();
}
