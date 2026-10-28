/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_origin_poll.inc
 *   Stepwise original operation-origin lifecycle checks.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_origin_poll.inc
 *
 * NOTES
 *   Pgrac-original tests; current/storage/admission are explicit doubles.
 *-------------------------------------------------------------------------
 */

UT_TEST(test_operation_poll_does_not_complete_inline)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read = { 0 };
	ClusterSavepointReaderIdentity identity;

	operation_driver_init(&f, false);
	UT_ASSERT(cluster_runtime_visibility_operation_origin_read_begin(
		&read, &test_origin_locator, &f.expected, NULL, NULL, CLUSTER_TX_RESOLVE_VISIBILITY,
		&f.admission, &f.reason));
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(cluster_runtime_visibility_operation_origin_read_poll(
					 &read, &identity, &f.top, &f.operation, f.output.data, &f.reason),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_release_calls, 0);
	cluster_runtime_visibility_operation_origin_read_cancel(&read);
}

UT_TEST(test_operation_poll_stages_outputs)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read = { 0 };
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolution zero_top = { 0 };
	ClusterSavepointReaderRecord zero_operation = { 0 };
	PGAlignedBlock original;

	operation_driver_init(&f, true);
	memset(original.data, 0xa5, BLCKSZ);
	UT_ASSERT(cluster_runtime_visibility_operation_origin_read_begin(
		&read, &test_origin_locator, &f.expected, NULL, NULL, CLUSTER_TX_RESOLVE_VISIBILITY,
		&f.admission, &f.reason));
	(void)cluster_runtime_visibility_operation_origin_read_poll(
		&read, &identity, &f.top, &f.operation, f.output.data, &f.reason);
	UT_ASSERT_EQ(memcmp(&f.top, &zero_top, sizeof(zero_top)), 0);
	UT_ASSERT_EQ(memcmp(&f.operation, &zero_operation, sizeof(zero_operation)), 0);
	UT_ASSERT_EQ(memcmp(f.output.data, original.data, BLCKSZ), 0);
	cluster_runtime_visibility_operation_origin_read_cancel(&read);
}

static bool
operation_poll_begin_exact(ClusterRuntimeVisibilityOperationRead *read, OperationPlanFixture *f,
						   bool same_segment)
{
	operation_driver_init(f, same_segment);
	memset(read, 0, sizeof(*read));
	return cluster_runtime_visibility_operation_origin_read_begin(
		read, &test_origin_locator, &f->expected, NULL, NULL, CLUSTER_TX_RESOLVE_VISIBILITY,
		&f->admission, &f->reason);
}

static ClusterRuntimeVisibilityOperationStep
operation_poll_step(ClusterRuntimeVisibilityOperationRead *read, OperationPlanFixture *f,
					ClusterSavepointReaderIdentity *identity)
{
	int acquire = test_candidate_acquire_calls;
	int poll = test_candidate_poll_calls;
	int release = test_candidate_release_calls;
	int release_poll = test_operation_release_poll_count;
	ClusterRuntimeVisibilityOperationStep step;
	ClusterTxResolution zero_top = { 0 };
	ClusterSavepointReaderRecord zero_operation = { 0 };
	ClusterSavepointReaderIdentity zero_identity = { 0 };
	PGAlignedBlock original;

	memset(original.data, 0xa5, BLCKSZ);
	step = cluster_runtime_visibility_operation_origin_read_poll(
		read, identity, &f->top, &f->operation, f->output.data, &f->reason);
	UT_ASSERT(test_candidate_acquire_calls - acquire + test_candidate_poll_calls - poll
				  + test_candidate_release_calls - release + test_operation_release_poll_count
				  - release_poll
			  <= 1);
	UT_ASSERT_EQ(test_candidate_wait_calls, 0);
	UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	if (step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE) {
		UT_ASSERT_EQ(memcmp(&f->top, &zero_top, sizeof(zero_top)), 0);
		UT_ASSERT_EQ(memcmp(&f->operation, &zero_operation, sizeof(zero_operation)), 0);
		UT_ASSERT_EQ(memcmp(identity, &zero_identity, sizeof(zero_identity)), 0);
		UT_ASSERT_EQ(memcmp(f->output.data, original.data, BLCKSZ), 0);
	}
	if (step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED)
		UT_ASSERT(f->reason != CLUSTER_TX_RESOLVE_NONE);
	else
		UT_ASSERT_EQ(f->reason, CLUSTER_TX_RESOLVE_NONE);
	return step;
}

static ClusterRuntimeVisibilityOperationStep
operation_poll_finish(ClusterRuntimeVisibilityOperationRead *read, OperationPlanFixture *f,
					  ClusterSavepointReaderIdentity *identity)
{
	ClusterRuntimeVisibilityOperationStep step = CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING;

	for (int i = 0; i < 64 && step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING; i++)
		step = operation_poll_step(read, f, identity);
	UT_ASSERT(step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	return step;
}

UT_TEST(test_operation_poll_same_and_cross_segment_complete)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, same));
		for (int i = 0; i < 9; i++)
			UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
						 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_acquire_segments[1], same ? 1 : 2);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(f.top.outcome, CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(identity.canonical.xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	}
}

UT_TEST(test_operation_poll_pending_acquire_never_waits)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterSavepointReaderIdentity identity;

	UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
	test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
	UT_ASSERT_EQ(test_candidate_poll_calls, 2);
	UT_ASSERT_EQ(test_candidate_release_calls, 0);
	UT_ASSERT_EQ(test_candidate_data_copy_calls, 0);
	test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(test_candidate_poll_calls, 5);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
}

UT_TEST(test_operation_poll_pending_release_is_not_complete)
{
	for (int release = 1; release <= 3; release++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		test_operation_release_pending_call = release;
		UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
		UT_ASSERT_EQ(test_operation_release_poll_count, 2);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	}
}

UT_TEST(test_operation_poll_preserves_active_and_post_top_discard)
{
	for (int active = 0; active < 2; active++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		operation_driver_action_release = 2;
		operation_driver_action = active ? 2 : 1;
		if (active) {
			test_tt_slot.status = TT_SLOT_ACTIVE;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_xid = TEST_ORIGIN_XID;
		}
		UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(f.top.outcome, active ? CLUSTER_TX_IN_PROGRESS : CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(f.operation.operation.effect_state,
					 active ? CLUSTER_SAVEPOINT_EFFECT_LIVE : CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		if (active)
			UT_ASSERT_EQ(f.top.commit_scn, InvalidScn);
	}
}

UT_TEST(test_operation_poll_copies_begin_inputs)
{
	for (int discover = 0; discover < 2; discover++) {
		OperationCrFixture f;
		ClusterRuntimeVisibilityOperationRead read = { 0 };
		ClusterSavepointReaderIdentity identity;
		ClusterTxLocator locator;
		ClusterItlSlotData head;
		BufferTag tag;
		ClusterSavepointOperation expected;
		ClusterSemanticAdmissionToken admission;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		locator = test_origin_locator;
		head = ClusterPageGetItlSlots((Page)f.source.data)[2];
		tag = f.tag;
		expected = f.origin.expected;
		admission = f.origin.admission;
		if (discover)
			locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT(cluster_runtime_visibility_operation_origin_read_begin(
			&read, &locator, discover ? NULL : &expected, discover ? &head : NULL,
			discover ? &tag : NULL, CLUSTER_TX_RESOLVE_VISIBILITY, &admission, &f.origin.reason));
		memset(&locator, 0, sizeof(locator));
		memset(&head, 0, sizeof(head));
		memset(&tag, 0, sizeof(tag));
		memset(&expected, 0, sizeof(expected));
		memset(&admission, 0, sizeof(admission));
		UT_ASSERT_EQ(operation_poll_finish(&read, &f.origin, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
		UT_ASSERT_EQ(f.origin.top.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(identity.canonical.wrap, TEST_ORIGIN_WRAP);
		UT_ASSERT_EQ(f.origin.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	}
}

UT_TEST(test_operation_poll_cancel_every_phase)
{
	for (int stop = 0; stop <= 9; stop++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;
		int expected_cancel = stop % 3 != 0;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		for (int i = 0; i < stop; i++)
			UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
						 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		UT_ASSERT_EQ(test_candidate_cancel_calls, expected_cancel);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_cancel_calls, expected_cancel);
	}
}

UT_TEST(test_operation_poll_cancel_pending_guard)
{
	for (int release = 0; release < 2; release++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		if (release)
			test_operation_release_pending_call = 1;
		else
			test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
		for (int i = 0; i < (release ? 3 : 1); i++)
			UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
						 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_poll_cold_data_is_resident_only)
{
	for (int mask = 1; mask <= 2; mask++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		test_candidate_cold_copy_mask = mask;
		UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
		UT_ASSERT_EQ(test_candidate_release_calls, mask == 1 ? 1 : 3);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_poll_release_and_final_fences)
{
	for (int stop = 0; stop <= 9; stop++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		for (int i = 0; i < stop; i++)
			UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
						 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
		test_admission_revoked = true;
		UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_cancel_calls, stop % 3 != 0);
	}
}

UT_TEST(test_operation_poll_stale_data_and_malformed_record)
{
	for (int action = 3; action <= 8; action++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		if (action == 7)
			operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		operation_driver_action = action;
		operation_driver_action_release = action == 8 ? 1 : 2;
		UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_poll_current_failures)
{
	for (int fault = 0; fault < 3; fault++) {
		for (int phase = 1; phase <= 3; phase++) {
			OperationPlanFixture f;
			ClusterRuntimeVisibilityOperationRead read;
			ClusterSavepointReaderIdentity identity;

			UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
			if (fault == 0)
				test_operation_acquire_failure_call = phase;
			else if (fault == 1) {
				test_candidate_release_failed = true;
				test_candidate_release_fault_call = phase;
			} else {
				for (int i = 0; i < (phase - 1) * 3; i++)
					UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
								 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
				test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
				test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
			}
			UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
						 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_candidate_cancel_calls, fault == 0 ? 0 : 1);
		}
	}
}

UT_TEST(test_operation_poll_error_cleanup_is_terminal)
{
	for (int phase = 1; phase <= 3; phase++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;
		volatile bool caught = false;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		operation_driver_action = 9;
		operation_driver_action_release = phase;
		PG_TRY();
		{
			(void)operation_poll_finish(&read, &f, &identity);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	}
}

UT_TEST(test_operation_poll_rebegin_does_not_lose_guard)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterSavepointReaderIdentity identity;

	UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
	UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT(!cluster_runtime_visibility_operation_origin_read_begin(
		&read, &test_origin_locator, &f.expected, NULL, NULL, CLUSTER_TX_RESOLVE_VISIBILITY,
		&f.admission, &f.reason));
	UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
}

UT_TEST(test_operation_poll_duplicate_completion_cannot_publish)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolution zero_top = { 0 };
	ClusterSavepointReaderRecord zero_operation = { 0 };
	PGAlignedBlock completed;

	UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
	UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE);
	memcpy(completed.data, f.output.data, BLCKSZ);
	UT_ASSERT_EQ(cluster_runtime_visibility_operation_origin_read_poll(
					 &read, &identity, &f.top, &f.operation, f.output.data, &f.reason),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(memcmp(&f.top, &zero_top, sizeof(zero_top)), 0);
	UT_ASSERT_EQ(memcmp(&f.operation, &zero_operation, sizeof(zero_operation)), 0);
	UT_ASSERT_EQ(memcmp(f.output.data, completed.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
}

UT_TEST(test_operation_poll_release_poll_failure_detaches)
{
	for (int phase = 1; phase <= 3; phase++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityOperationRead read;
		ClusterSavepointReaderIdentity identity;

		UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
		test_operation_release_pending_call = phase;
		test_operation_release_poll_failed = true;
		UT_ASSERT_EQ(operation_poll_finish(&read, &f, &identity),
					 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
		UT_ASSERT_EQ(test_candidate_acquire_calls, phase);
		UT_ASSERT_EQ(test_candidate_release_calls, phase);
		UT_ASSERT_EQ(test_operation_release_poll_count, 1);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_poll_invalid_inputs_do_not_acquire)
{
	for (int fault = 0; fault < 7; fault++) {
		OperationCrFixture f;
		ClusterRuntimeVisibilityOperationRead read = { 0 };
		ClusterTxLocator locator;
		ClusterItlSlotData head;
		ClusterTxResolveMode mode = CLUSTER_TX_RESOLVE_VISIBILITY;
		bool expected = fault != 4 && fault != 5;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		locator = test_origin_locator;
		head = ClusterPageGetItlSlots((Page)f.source.data)[2];
		if (fault == 0)
			locator.uba = (UBA)InvalidUba_init;
		else if (fault == 1)
			locator.uba = uba_encode(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 7, TEST_TT_OFFSET, 0);
		else if (fault == 2)
			f.origin.admission.entered = false;
		else if (fault == 3)
			mode = CLUSTER_TX_RESOLVE_ROW_WAIT;
		UT_ASSERT(!cluster_runtime_visibility_operation_origin_read_begin(
			&read, fault == 6 ? NULL : &locator, expected ? &f.origin.expected : NULL,
			fault == 5 ? &head : NULL, fault == 4 ? &f.tag : NULL, mode, &f.origin.admission,
			&f.origin.reason));
		UT_ASSERT(f.origin.reason != CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
		cluster_runtime_visibility_operation_origin_read_cancel(&read);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	}
}

UT_TEST(test_operation_poll_invalid_output_cancels_active)
{
	OperationPlanFixture f;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterSavepointReaderIdentity identity;

	UT_ASSERT(operation_poll_begin_exact(&read, &f, false));
	UT_ASSERT_EQ(operation_poll_step(&read, &f, &identity),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING);
	UT_ASSERT_EQ(cluster_runtime_visibility_operation_origin_read_poll(
					 &read, NULL, &f.top, &f.operation, f.output.data, &f.reason),
				 CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED);
	UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_runtime_visibility_operation_origin_read_cancel(&read);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
}
