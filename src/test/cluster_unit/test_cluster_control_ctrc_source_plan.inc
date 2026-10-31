/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_source_plan.inc
 *     Keep the complete retained source plan immutable across native waits.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_source_plan.inc
 *
 * NOTES
 *     Original ROOT/claim/WAL readers run. Start/GES owners retain existing
 *     controlled leaves; this fixture grants no all-source or PAGE rights.
 *
 *-------------------------------------------------------------------------
 */
static void
ctrc_source_plan_fixture(bool history, ClusterWalInputsV1 **inputs)
{
	tx_ir_fixture(true); ctrc_inputs_activate_work();
	if (history) {
		ClusterColdParticipantV1 *p = &tx_inputs_typed.participants[1];
		tx_inputs_typed.replay_count = 1;
		tx_inputs_typed.history_xid[1].begun = true;
		tx_inputs_typed.history_xid[1].source.thread_id = p->thread_id;
		tx_inputs_typed.history_xid[1].source.owner_incarnation = p->owner_incarnation;
		tx_inputs_typed.history_xid[1].source.timeline = p->timeline;
		tx_inputs_typed.history_xid[1].source.native_redo = p->native_redo;
	}
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs; ctrc_ir_start_current = true;
}

UT_TEST(test_ctrc_source_plan_preserves_unchanged_local_scope)
{
	ClusterWalInputsV1 *inputs = NULL;
	unsigned cf;
	ctrc_source_plan_fixture(false, &inputs); cf = test_cf_lock_calls;
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	UT_ASSERT(cluster_wal_inputs_at_v1(inputs, 1) != NULL);
	UT_ASSERT_EQ(test_cf_lock_calls, cf);
	UT_ASSERT_EQ(test_tx_ir_grants, 0);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_source_plan_rejects_every_source_and_participant_byte)
{
	for (unsigned history = 0; history < 2; history++) {
		ClusterWalInputsV1 *inputs = NULL;
		unsigned cf;
		ctrc_source_plan_fixture(history != 0, &inputs); cf = test_cf_lock_calls;
		for (unsigned kind = 0; kind < 2; kind++)
			for (uint32 p = 0; p < tx_inputs_typed.participant_count; p++) {
				uint8 *bytes = kind ? (uint8 *)&tx_inputs_typed.sources[p]
					: (uint8 *)&tx_inputs_typed.participants[p];
				Size size = kind ? sizeof(tx_inputs_typed.sources[p]) : sizeof(tx_inputs_typed.participants[p]);
				for (Size i = 0; i < size; i++) {
					bytes[i] ^= 1;
					UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
					UT_ASSERT(cluster_wal_inputs_at_v1(inputs, 0) == NULL);
					bytes[i] ^= 1;
					UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
				}
			}
		UT_ASSERT_EQ(test_cf_lock_calls, cf);
		cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
	}
}

UT_TEST(test_ctrc_source_plan_refuses_all_later_count_changes)
{
	ClusterWalInputsV1 *inputs = NULL;
	uint32 participants, replay;
	ctrc_source_plan_fixture(false, &inputs);
	participants = tx_inputs_typed.participant_count; replay = tx_inputs_typed.replay_count;
	for (unsigned fault = 0; fault < 5; fault++) {
		if (fault == 0) tx_inputs_typed.participant_count = 0;
		if (fault == 1) tx_inputs_typed.participant_count = participants - 1;
		if (fault == 2) tx_inputs_typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1;
		if (fault == 3) tx_inputs_typed.replay_count = replay - 1;
		if (fault == 4) tx_inputs_typed.replay_count = participants + 1;
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 0);
		tx_inputs_typed.participant_count = participants; tx_inputs_typed.replay_count = replay;
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(inputs), 2);
	}
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_source_plan_other_origin_drift_keeps_native_cleanup_debt)
{
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoverySerialRequest request;
	ClusterRecoveryCtrcReplayAuthorityV1 authority;
	ctrc_source_plan_fixture(false, &inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_acquire_v1(inputs, 0, &request), 0);
	authority = cluster_wal_inputs_ctrc_replay_source_v1(inputs, 0);
	tx_inputs_typed.participants[1].reserved_zero = 1;
	UT_ASSERT(!cluster_wal_inputs_ctrc_replay_source_current_v1(&authority));
	UT_ASSERT(test_tx_ir_held && inputs_recovery_held);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replay_release_v1(inputs, 0), 0);
	UT_ASSERT(!test_tx_ir_held && inputs_recovery_held);
	cluster_wal_inputs_release_v1(&inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_source_plan_refuses_invalid_initial_bounds_before_owners)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		unsigned cf, grants;
		tx_ir_fixture(true); ctrc_inputs_activate_work();
		if (fault == 0) tx_inputs_typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1;
		else tx_inputs_typed.replay_count = tx_inputs_typed.participant_count + 1;
		cf = test_cf_lock_calls; grants = test_tx_ir_grants;
		UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(inputs == NULL && !inputs_pin_held);
		UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		test_inputs_startup_cf = false;
	}
}
