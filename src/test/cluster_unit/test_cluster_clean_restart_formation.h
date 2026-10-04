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
	return test_clean_installed && test_first_writer_installed
		&& epoch == test_first_writer_epoch
		&& memcmp(expected, &test_first_writer, sizeof(*expected)) == 0;
}

static void
test_clean_formation_input(void)
{
	ClusterSemanticActivationRecord open;
	ClusterControlRootIdentity *id;
	ut_normal_start_setup();
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
	id->thread_claim_crc32c = 9;
	id->origin_owner_incarnation = test_qvotec_self_incarnation;
	id->root_lineage_seq = 1;
	test_clean_input.predecessor = *id;
	test_clean_input.predecessor.origin_owner_incarnation = 100 + cluster_node_id;
	memset(test_clean_input.predecessor_claim_sha256, 3, 32);
	test_clean_input.successor.database_incarnation = 1;
	test_clean_input.successor.config_generation = 1;
	test_clean_input.successor.claim_generation = 2;
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

UT_TEST(test_clean_formation_old_pgsa_classifies_loading_at_current_epoch)
{
	const char *failure;
	test_clean_formation_input();
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_TARGET_LOADING);
	UT_ASSERT_EQ(NormalStartCompletion->epoch, 2);
	UT_ASSERT_EQ(memcmp(NormalStartCompletion->pgsa, test_r4fsm_bootstrap_bytes, 512), 0);
	UT_ASSERT(cluster_semantic_normal_start_closed());
	test_gate_reset();
}

UT_TEST(test_clean_formation_missing_original_input_cannot_classify_ready)
{
	const char *failure;
	test_clean_formation_input();
	test_clean_input_result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	UT_ASSERT(!cluster_semantic_normal_start_prepare(true, 0, &failure));
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_FAILED);
	UT_ASSERT(!semantic_activation_restart.local_ready);
	test_gate_reset();
}
