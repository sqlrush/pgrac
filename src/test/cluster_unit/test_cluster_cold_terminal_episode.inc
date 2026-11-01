/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_terminal_episode.inc
 *    Original Startup authority for terminal observation after replay.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_terminal_episode.inc
 *
 * NOTES
 *    This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
/* Original Startup authority gate after complete replay. Storage, source and
 * fence providers remain the existing Startup boundary fixture. */
bool
RecoveryInProgress(void)
{
	return InRecovery;
}

static ClusterColdTypedV1 *
terminal_original_episode_fixture(void)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	ClusterColdEffectsProofV1 proof = {0};
	ClusterColdReplayResultV1 result = endpoint_result;

	UT_ASSERT(endpoint_finish(typed));
	control_authority_ok = control_borrow_ok = control_cover_ok = true;
	control_epoch = 7;
	control_duty = roots[2].identity;
	control_root = roots[2];
	control_token = tokens[2];
	memset(&control_serial, 0, sizeof(control_serial));
	control_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	control_serial.held = true;
	control_serial.lock_request.request_id = 23;
	for (uint32 p = 0; p < typed->participant_count; ++p) {
		proof.records_seen[p] = cluster_cold_plan_replay_record_count_v1(typed->plan, p);
		proof.records_routed += proof.records_seen[p];
	}
	for (uint32 i = 0; i < result.steps_done; ++i) {
		ClusterColdStepV1 step;
		UT_ASSERT(cluster_cold_plan_step_v1(typed->plan, i, &step));
		if (step.step_kind == CLUSTER_COLD_STEP_SPACE) result.space_steps++;
	}
	UT_ASSERT(cluster_cold_typed_effects_complete_v1(typed, &proof, &result));
	UT_ASSERT(source_drained_fixture(typed, 1));
	MyBackendType = B_STARTUP;
	InRecovery = true;
	return typed;
}

UT_TEST(test_terminal_original_episode_requires_full_replay_and_held_ir)
{
	for (unsigned fault = 0; fault < 7; ++fault) {
		ClusterColdTypedV1 *typed = terminal_original_episode_fixture();
		ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
		ClusterThreadRecoveryAuthorityV1 authority = {0};
		uint32 participant = 1;

		if (fault == 1) typed->effects_receipt.proof.records_seen[0]++;
		if (fault == 2) typed->source_drained[1] = false;
		if (fault == 3) control_serial.held = false;
		if (fault == 4) control_serial.release_uncertain = true;
		if (fault == 5) control_serial.lock_request.request_id = 0;
		if (fault == 6) participant = typed->replay_count;
		UT_ASSERT_EQ(cluster_cold_typed_terminal_authority_v1(typed, fence, participant, &authority), fault == 0);
		if (fault == 0) {
			UT_ASSERT(authority.serial_guard == &control_serial);
			UT_ASSERT(!cluster_cold_typed_terminal_authority_v1(typed, fence, 0, &authority));
		}
		endpoint_destroy(&typed);
		MyBackendType = B_INVALID;
		InRecovery = false;
	}
}

UT_TEST(test_terminal_original_episode_refuses_changed_whole_source_or_owner)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterColdTypedV1 *typed = terminal_original_episode_fixture();
		ClusterRecoveryFencePlan *fence = (ClusterRecoveryFencePlan *)&fixture_context;
		ClusterThreadRecoveryAuthorityV1 authority = {0};

		UT_ASSERT(cluster_cold_typed_terminal_authority_v1(typed, fence, 1, &authority));
		if (fault == 0) fence_current = false;
		if (fault == 1) typed->sources[typed->replay_count].claim.claim_sha256[31]++;
		if (fault == 2) control_token.file_txn_seq++;
		if (fault == 3) cluster_cold_replay_window_leave_v1();
		UT_ASSERT(!cluster_cold_typed_terminal_authority_v1(typed, fence, 1, &authority));
		endpoint_destroy(&typed);
		MyBackendType = B_INVALID;
		InRecovery = false;
	}
}
