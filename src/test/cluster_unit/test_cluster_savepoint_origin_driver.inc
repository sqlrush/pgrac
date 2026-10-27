/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_origin_driver.inc
 *   Original SCUR lifecycle around exact operation origin reads.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_origin_driver.inc
 *
 * NOTES
 *   Real runtime orchestration; current/storage/native status are doubles.
 *-------------------------------------------------------------------------
 */
static OperationPlanFixture *operation_driver_output;
static int operation_driver_action;
static int operation_driver_action_release;

/* Observe real orchestration boundaries, including the final release before
 * any caller output may be published. Tail access remains byte aligned. */
static void
operation_driver_release(void)
{
	ClusterTxResolution zero_top = { 0 };
	ClusterSavepointReaderRecord zero_operation = { 0 };
	PGAlignedBlock original;

	memset(original.data, 0xa5, BLCKSZ);
	UT_ASSERT_EQ(memcmp(&operation_driver_output->top, &zero_top, sizeof(zero_top)), 0);
	UT_ASSERT_EQ(
		memcmp(&operation_driver_output->operation, &zero_operation, sizeof(zero_operation)), 0);
	UT_ASSERT_EQ(memcmp(operation_driver_output->output.data, original.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	if (test_candidate_release_calls != operation_driver_action_release)
		return;
	switch (operation_driver_action) {
	case 1:
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		break;
	case 2:
		test_tt_slot.status = TT_SLOT_COMMITTED;
		test_tt_slot.commit_scn = test_commit_scn;
		test_native_status = TRANSACTION_STATUS_COMMITTED;
		break;
	case 3:
		test_candidate_data_generation++;
		break;
	case 4:
		test_admission_revoked = true;
		break;
	case 5:
		test_candidate_root_drift = true;
		break;
	case 6:
		((UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader)))->target_block++;
		break;
	case 7:
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
		break;
	case 8:
		test_candidate_tt_generation++;
		break;
	case 9:
		pg_re_throw();
		break;
	}
}

static void
operation_driver_init(OperationPlanFixture *f, bool same_segment)
{
	operation_plan_init(f, same_segment, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_driver_output = f;
	operation_driver_action = 0;
	operation_driver_action_release = 0;
	memset(&f->top, 0xa5, sizeof(f->top));
	memset(&f->operation, 0xa5, sizeof(f->operation));
	memset(f->output.data, 0xa5, BLCKSZ);
	test_operation_release_observer = operation_driver_release;
}

static ClusterTxOutcome
operation_driver_read(OperationPlanFixture *f, ClusterTxResolveMode mode)
{
	return cluster_runtime_visibility_resolve_operation_origin_admitted(
		&test_origin_locator, &f->expected, mode, &f->admission, &f->top, &f->operation,
		f->output.data, &f->reason);
}

static void
operation_driver_assert_refused(OperationPlanFixture *f)
{
	ClusterTxResolution zero_top = { 0 };
	ClusterSavepointReaderRecord zero_operation = { 0 };
	PGAlignedBlock original;

	memset(original.data, 0xa5, BLCKSZ);
	UT_ASSERT(f->reason != CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(memcmp(&f->top, &zero_top, sizeof(zero_top)), 0);
	UT_ASSERT_EQ(memcmp(&f->operation, &zero_operation, sizeof(zero_operation)), 0);
	UT_ASSERT_EQ(memcmp(f->output.data, original.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_driver_three_current_phases)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;

		operation_driver_init(&f, same);
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_candidate_acquire_segments[0], 1);
		UT_ASSERT_EQ(test_candidate_acquire_segments[1], same ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_acquire_segments[2], 1);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
		UT_ASSERT_EQ(f.top.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
	}
}

UT_TEST(test_operation_driver_post_top_discard)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;

		operation_driver_init(&f, same);
		operation_driver_action = 1;
		operation_driver_action_release = 2;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT_EQ(f.top.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
	}
}

UT_TEST(test_operation_driver_preserves_sampled_active)
{
	OperationPlanFixture f;

	operation_driver_init(&f, false);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	operation_driver_action = 2;
	operation_driver_action_release = 2;
	UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(f.top.commit_scn, InvalidScn);
	UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(test_operation_driver_acquire_failures)
{
	for (int call = 1; call <= 3; call++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		test_operation_acquire_failure_call = call;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_acquire_calls, call);
		UT_ASSERT_EQ(test_candidate_release_calls, call - 1);
		operation_driver_assert_refused(&f);
	}
}

UT_TEST(test_operation_driver_release_failures)
{
	for (int call = 1; call <= 3; call++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		test_candidate_release_failed = true;
		test_candidate_release_fault_call = call;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_acquire_calls, call);
		UT_ASSERT_EQ(test_candidate_release_calls, call);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		operation_driver_assert_refused(&f);
	}
}

UT_TEST(test_operation_driver_release_authority_fences)
{
	for (int call = 1; call <= 3; call++) {
		for (int fault = 0; fault < 2; fault++) {
			OperationPlanFixture f;

			operation_driver_init(&f, false);
			test_candidate_release_fault_call = call;
			test_candidate_release_epoch_drift = fault == 0;
			test_candidate_release_admission_drift = fault == 1;
			UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY),
						 CLUSTER_TX_UNKNOWN);
			operation_driver_assert_refused(&f);
		}
	}
}

UT_TEST(test_operation_driver_changed_data_refuses)
{
	for (int action = 3; action <= 8; action++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		if (action == 7)
			operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		operation_driver_action = action;
		operation_driver_action_release = action == 8 ? 1 : 2;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		operation_driver_assert_refused(&f);
	}
}

UT_TEST(test_operation_driver_cold_reads)
{
	for (int mask = 1; mask <= 3; mask++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		test_candidate_cold_copy_mask = mask;
		operation_driver_action = 1;
		operation_driver_action_release = 2;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, test_candidate_data_unpin_calls);
		UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	}
}

UT_TEST(test_operation_driver_cold_miss_refuses)
{
	for (int mask = 1; mask <= 2; mask++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		test_candidate_cold_copy_mask = mask;
		test_candidate_data_pool_disabled = true;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY), CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 1);
		UT_ASSERT_EQ(test_candidate_data_unpin_calls, 0);
		operation_driver_assert_refused(&f);
	}
}

UT_TEST(test_operation_driver_error_cleanup)
{
	for (int fault = 0; fault < 3; fault++) {
		OperationPlanFixture f;
		volatile bool caught = false;

		operation_driver_init(&f, false);
		if (fault < 2) {
			test_candidate_cold_copy_mask = fault == 0 ? 1 : 2;
			test_candidate_data_io_throw = true;
		} else {
			operation_driver_action = 9;
			operation_driver_action_release = 3;
		}
		PG_TRY();
		{
			(void)operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		/* ERROR propagation has no normal reason result. */
		f.reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		operation_driver_assert_refused(&f);
	}
}

UT_TEST(test_operation_driver_pending_waits)
{
	for (int release = 0; release <= 3; release++) {
		OperationPlanFixture f;

		operation_driver_init(&f, false);
		if (release == 0) {
			test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_after_wait_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
			test_candidate_wakes_before_terminal = 2;
		} else
			test_operation_release_pending_call = release;
		UT_ASSERT_EQ(operation_driver_read(&f, CLUSTER_TX_RESOLVE_VISIBILITY),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(test_candidate_wait_calls, release == 0 ? 2 : 0);
		UT_ASSERT_EQ(test_operation_release_wait_count, release == 0 ? 0 : 1);
		UT_ASSERT_EQ(test_operation_release_poll_count, release == 0 ? 0 : 2);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	}
}

UT_TEST(test_operation_driver_bad_and_foreign_inputs)
{
	for (int fault = 0; fault < 10; fault++) {
		OperationPlanFixture f;
		ClusterTxOutcome outcome;

		operation_driver_init(&f, false);
		if (fault == 0)
			memset(&test_origin_locator.uba, 0, sizeof(UBA));
		if (fault == 1)
			test_origin_locator.uba
				= uba_encode(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 7, TEST_TT_OFFSET, 0);
		if (fault == 2)
			f.admission.entered = false;
		if (fault < 4)
			outcome = operation_driver_read(&f, fault == 3 ? CLUSTER_TX_RESOLVE_ROW_WAIT
														   : CLUSTER_TX_RESOLVE_VISIBILITY);
		else {
			if (fault == 7)
				memset(&f.top, 0, sizeof(f.top));
			if (fault == 8)
				memset(&f.operation, 0, sizeof(f.operation));
			outcome = cluster_runtime_visibility_resolve_operation_origin_admitted(
				fault == 4 ? NULL : &test_origin_locator, fault == 5 ? NULL : &f.expected,
				CLUSTER_TX_RESOLVE_VISIBILITY, fault == 6 ? NULL : &f.admission,
				fault == 7 ? NULL : &f.top, fault == 8 ? NULL : &f.operation,
				fault == 9 ? NULL : f.output.data, &f.reason);
		}
		UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		UT_ASSERT_EQ(test_remote_origin_calls, 0);
		operation_driver_assert_refused(&f);
	}
}
