/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_replay_ir.inc
 *     Exercise original replay-origin IR independently of TT occupancy.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_replay_ir.inc
 *
 * NOTES
 *     Original ROOT/claim/WAL readers execute unchanged. Original Start
 *     ownership and distributed GES remain controlled fixture boundaries.
 *     No all-source holder set or PAGE/service permission is exercised.
 *
 *-------------------------------------------------------------------------
 */
bool
ClusterColdRecoveryCtrcReplayRequestCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, uint32 index, ClusterWalInputsV1 *scope,
	const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest expected = ctrc_ir_provisional();
	expected.tx_closure_scope = scope;
	return ctrc_ir_start_current && ClusterColdRecoveryCtrcWorkCurrent(start, work)
		&& index < tx_inputs_typed.replay_count && index == 0
		&& scope == ctrc_ir_original_scope && request != NULL
		&& memcmp(request, &expected, sizeof(expected)) == 0;
}

static void
ctrc_replay_ir_fixture(bool complete, ClusterWalInputsV1 **inputs)
{
	tx_ir_fixture(complete);
	ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs; ctrc_ir_start_current = true;
}

UT_TEST(test_ctrc_replay_ir_grants_original_source_without_a_tt_subject)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcReplayAuthorityV1 authority, empty, zero = {0};
	uint64 generation;
	ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	generation = inputs_pin_generation;
	empty = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	UT_ASSERT(memcmp(&empty, &zero, sizeof(zero)) == 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 1);
	UT_ASSERT(cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
	authority = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	UT_ASSERT(authority.source.retention.scope == inputs);
	UT_ASSERT(authority.ir_request_id != 0 && authority.work_generation != 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_replay_source_current_v1(&authority));
	UT_ASSERT_EQ(inputs_pin_generation, generation);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), 0);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&authority));
	UT_ASSERT(inputs_pin_held && !test_tx_ir_held);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_replay_ir_never_substitutes_for_subject_purpose)
{
	for (unsigned replay = 0; replay < 2; replay++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		ClusterRecoveryCtrcSubjectAuthorityV1 subject, zero = {0};
		ctrc_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
		UT_ASSERT_EQ(replay ? cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request)
			: cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), 0);
		UT_ASSERT_EQ(replay ? cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request)
			: cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(replay ? cluster_wal_inputs_ctrc_release_v1(inputs, 0)
			: cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(replay ? !cluster_wal_inputs_ctrc_authorized_v1(inputs, 0)
			: !cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
		if (replay) {
			subject = cluster_wal_inputs_ctrc_subject_v1(inputs, 0);
			UT_ASSERT(memcmp(&subject, &zero, sizeof(zero)) == 0);
		}
		UT_ASSERT_EQ(test_tx_ir_grants, 1);
		cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
	}
}

UT_TEST(test_ctrc_replay_ir_wait_keeps_original_purpose_and_holder)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	for (unsigned retry = 0; retry < 2; retry++) {
		test_input_acquire = retry ? CLUSTER_RECOVERY_SERIAL_RETRY : CLUSTER_RECOVERY_SERIAL_BUSY;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(!test_tx_ir_held && inputs_pin_held);
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	}
	test_input_acquire = CLUSTER_RECOVERY_SERIAL_GRANTED;
	test_tx_ir_after_grant = ctrc_ir_cf_contention;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(test_tx_ir_held && inputs_pin_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
	test_cf_grant = true; test_tx_ir_after_grant = NULL;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	UT_ASSERT_EQ(test_tx_ir_grants, 1);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_replay_ir_rejects_wrong_original_and_provisional_inputs)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		uint32 index = 0;
		ctrc_replay_ir_fixture(fault != 0, &inputs); request = ctrc_ir_provisional();
		switch (fault) {
		case 0: break;
		case 1: index = 1; break;
		case 2: request.duty.origin_owner_incarnation++; break;
		case 3: request.expected_root_token.root_publish_seq++; break;
		case 4: request.tx_closure_scope = inputs; break;
		case 5: ctrc_ir_start_current = false; break;
		case 6: ctrc_inputs_work.generation++; break;
		case 7: inputs_pin_current = false; break;
		}
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, index, &request), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(test_tx_ir_grants, 0);
		cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
	}
}

UT_TEST(test_ctrc_replay_ir_late_loss_retains_exact_native_cleanup_debt)
{
	for (ctrc_ir_late_fault = 0; ctrc_ir_late_fault < 5; ctrc_ir_late_fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoverySerialRequest request;
		ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
		test_tx_ir_after_grant = ctrc_ir_late_change;
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT(test_tx_ir_held && inputs_pin_held);
		UT_ASSERT(!cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
		UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), 0);
		UT_ASSERT(inputs_pin_held && !test_tx_ir_held);
		cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
	}
}

UT_TEST(test_ctrc_replay_source_freezes_native_values_and_exact_window)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcReplayAuthorityV1 authority;
	XLogRecPtr lower, upper; uint16 thread; unsigned cf;
	ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	for (Size i = 0; i < sizeof(authority); i++) {
		ClusterRecoveryCtrcReplayAuthorityV1 changed = authority;
		((uint8 *)&changed)[i] ^= 1;
		UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&changed));
	}
	lower = authority.source.selected.checkpoint.checkpoint_lower_lsn;
	upper = authority.source.selected.checkpoint.validated_tail_lsn_exclusive;
	thread = authority.source.selected.source.claim.identity.origin_thread_id;
	cf = test_cf_lock_calls;
	UT_ASSERT(cluster_wal_inputs_ctrc_replay_source_covers_window_v1(&authority, thread, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_covers_window_v1(&authority, thread + 1, lower, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_covers_window_v1(&authority, thread, lower - 1, upper));
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_covers_window_v1(&authority, thread, lower, upper + 1));
	UT_ASSERT_EQ(test_cf_lock_calls, cf);
	cluster_wal_inputs_release_v1(&inputs);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&authority));
	test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_replay_ir_uncertain_release_keeps_purpose_request_and_walr)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcReplayAuthorityV1 authority;
	volatile bool caught = false;
	ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	test_input_release = false; test_capture_error_level = true;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY(); test_capture_error_level = false;
	UT_ASSERT(caught && test_last_error_level == FATAL);
	UT_ASSERT(test_tx_ir_held && inputs_pin_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&authority));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(inputs, 0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	test_input_release = true;
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_replay_ir_error_and_reacquire_never_renew_old_source)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcReplayAuthorityV1 original, current;
	volatile bool caught = false;
	ctrc_replay_ir_fixture(true, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	original = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	ctrc_subject_reacquire_guard = original.source.retention.serial;
	ctrc_subject_reacquire_id = original.ir_request_id;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), 0);
	test_tx_ir_after_grant = ctrc_subject_fresh_native_request;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	test_tx_ir_after_grant = NULL;
	current = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_replay_source_current_v1(&current));
	UT_ASSERT(current.ir_request_id > original.ir_request_id);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&original));
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), 0);
	test_tx_ir_after_grant = ctrc_ir_native_error;
	PG_TRY(); { (void)cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY(); test_tx_ir_after_grant = NULL;
	UT_ASSERT(caught && test_tx_ir_held && inputs_pin_held);
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_authorized_v1(inputs, 0));
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}
