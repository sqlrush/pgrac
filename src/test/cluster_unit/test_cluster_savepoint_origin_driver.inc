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
	case 10:
		((UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader)))->target_offset++;
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

/* A V1 link has the caller's exact current top, never a fabricated tail. */
static void
operation_own_head_init(OperationPlanFixture *f, bool same, ClusterItlSlotData *head,
						BufferTag *tag)
{
	UndoRecordHeader *record;
	UndoBlockHeader *block;
	UndoSlotDirEntry *slot;

	operation_driver_init(f, same);
	f->expected.operation_fxid = f->expected.top_fxid;
	f->expected.operation_sequence = 0;
	f->expected.command_id = InvalidCommandId;
	f->expected.subid = TopSubTransactionId;
	test_origin_locator.itl_slot_index = 2;
	*tag = (BufferTag){ .spcOid = 1663,
						.dbOid = 42,
						.relNumber = 77,
						.forkNum = MAIN_FORKNUM,
						.blockNum = 23 };
	*head = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
								  .wrap = 8,
								  .flags = ITL_FLAG_ACTIVE,
								  .write_scn = scn_encode(0, 70),
								  .undo_segment_head = test_origin_locator.uba };
	block = (UndoBlockHeader *)test_operation_page.data;
	record = (UndoRecordHeader *)(test_operation_page.data + sizeof(*block));
	slot = (UndoSlotDirEntry *)(test_operation_page.data + BLCKSZ - sizeof(*slot));
	record->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	record->xid = TEST_ORIGIN_XID;
	record->payload_length -= sizeof(UndoOperationTrailer);
	slot->flags = record->flags;
	slot->record_length -= sizeof(UndoOperationTrailer);
	block->free_offset -= sizeof(UndoOperationTrailer);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
}

static ClusterTxOutcome
operation_own_head_read(OperationPlanFixture *f, const ClusterItlSlotData *head,
						const BufferTag *tag, ClusterSavepointReaderIdentity *identity)
{
	return cluster_runtime_visibility_resolve_own_operation_head_origin_admitted(
		&test_origin_locator, head, tag, &f->expected, CLUSTER_TX_RESOLVE_VISIBILITY, &f->admission,
		identity, &f->top, &f->operation, f->output.data, &f->reason);
}

UT_TEST(test_operation_own_top_head_uses_original_three_current_phases)
{
	for (int same = 0; same < 2; same++)
		for (int commit = 0; commit < 2; commit++) {
			OperationPlanFixture f;
			ClusterItlSlotData head;
			BufferTag tag;
			ClusterSavepointReaderIdentity identity;

			operation_own_head_init(&f, same, &head, &tag);
			operation_driver_action = commit ? 2 : 0;
			operation_driver_action_release = 2;
			UT_ASSERT_EQ(operation_own_head_read(&f, &head, &tag, &identity),
						 CLUSTER_TX_IN_PROGRESS);
			UT_ASSERT_EQ(f.reason, CLUSTER_TX_RESOLVE_NONE);
			UT_ASSERT_EQ(identity.operation_sequence, 0);
			UT_ASSERT_EQ(identity.operation_fxid.value, f.expected.top_fxid.value);
			UT_ASSERT_EQ(identity.canonical_generation, same ? 17 : 23);
			UT_ASSERT_EQ(f.operation.operation.effect_state, 0);
			UT_ASSERT_EQ(f.operation.history.count, 1);
			UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
			UT_ASSERT_EQ(test_candidate_release_calls, 3);
			UT_ASSERT_EQ(test_candidate_acquire_segments[0], 1);
			UT_ASSERT_EQ(test_candidate_acquire_segments[1], same ? 1 : 2);
			UT_ASSERT_EQ(test_candidate_acquire_segments[2], 1);
			UT_ASSERT_EQ(test_candidate_max_held_count, 1);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_operation_pin_depth, 0);
		}
}

UT_TEST(test_operation_own_top_head_keeps_identity_and_fresh_data_fences)
{
	for (int same = 0; same < 2; same++)
		for (int fault = 0; fault < 10; fault++) {
			OperationPlanFixture f;
			ClusterItlSlotData head;
			BufferTag tag;
			ClusterSavepointReaderIdentity identity;
			UndoRecordHeader *record;

			operation_own_head_init(&f, same, &head, &tag);
			record = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
			if (fault == 0)
				record->xid++;
			if (fault == 1)
				head.write_scn++;
			if (fault == 2)
				f.expected.top.segment_generation++;
			if (fault == 3)
				f.expected.top.slot_wrap++;
			if (fault == 4)
				tag.relNumber++;
			if (fault == 5)
				f.expected.top.publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
			if (fault >= 6) {
				const int action[] = { 3, 4, 8, 10 };
				operation_driver_action = fault == 8 && same ? 3 : action[fault - 6];
				operation_driver_action_release = fault == 8 ? 1 : 2;
			}
			UT_ASSERT_EQ(operation_own_head_read(&f, &head, &tag, &identity), CLUSTER_TX_UNKNOWN);
			UT_ASSERT_EQ(identity.operation_sequence, 0);
			operation_driver_assert_refused(&f);
			UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
			UT_ASSERT_EQ(test_operation_pin_depth, 0);
		}
}

UT_TEST(test_operation_own_top_head_error_cleanup_and_old_entry_refusal)
{
	for (int release = 1; release <= 3; release++) {
		OperationPlanFixture f;
		ClusterItlSlotData head;
		BufferTag tag;
		ClusterSavepointReaderIdentity identity;
		volatile bool caught = false;

		operation_own_head_init(&f, false, &head, &tag);
		operation_driver_action = 9;
		operation_driver_action_release = release;
		PG_TRY();
		{
			(void)operation_own_head_read(&f, &head, &tag, &identity);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		f.reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		operation_driver_assert_refused(&f);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
	}
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;
		ClusterItlSlotData head;
		BufferTag tag;
		ClusterSavepointReaderIdentity identity;

		operation_own_head_init(&f, same, &head, &tag);
		UT_ASSERT_EQ(cluster_runtime_visibility_resolve_operation_head_origin_admitted(
						 &test_origin_locator, &head, &tag, CLUSTER_TX_RESOLVE_VISIBILITY,
						 &f.admission, &identity, &f.top, &f.operation, f.output.data, &f.reason),
					 CLUSTER_TX_UNKNOWN);
		operation_driver_assert_refused(&f);
	}
}

static ClusterTxOutcome
operation_retained_head_read(OperationPlanFixture *f, const ClusterItlSlotData *head,
							 const BufferTag *tag, const ClusterSavepointReaderIdentity *retained,
							 ClusterSavepointReaderIdentity *identity)
{
	return cluster_runtime_visibility_resolve_retained_operation_head_origin_admitted(
		&test_origin_locator, head, tag, retained, CLUSTER_TX_RESOLVE_VISIBILITY, &f->admission,
		identity, &f->top, &f->operation, f->output.data, &f->reason);
}

/* First qualify an actual operation. The older V1 occupies another DATA row,
 * and keeps that canonical generation/FullXid challenge without publication. */
static void
operation_retained_head_init(OperationPlanFixture *f, bool same, ClusterItlSlotData *head,
							 BufferTag *tag, ClusterSavepointReaderIdentity *retained)
{
	ClusterSavepointReaderIdentity first;
	UndoBlockHeader *block;
	UndoRecordHeader *record;
	UndoSlotDirEntry *slot;
	UndoItlHistoryTrailer history;

	operation_driver_init(f, same);
	test_origin_locator.itl_slot_index = 2;
	*tag = (BufferTag){ .spcOid = 1663,
						.dbOid = 42,
						.relNumber = 77,
						.forkNum = MAIN_FORKNUM,
						.blockNum = 23 };
	*head = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
								  .wrap = 8,
								  .flags = ITL_FLAG_ACTIVE,
								  .write_scn = scn_encode(0, 70),
								  .undo_segment_head = test_origin_locator.uba };
	UT_ASSERT_EQ(operation_retained_head_read(f, head, tag, NULL, &first), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	*retained = first;
	retained->operation_fxid = first.top_fxid;
	retained->operation_sequence = 0;
	retained->command_id = InvalidCommandId;
	retained->subid = TopSubTransactionId;
	block = (UndoBlockHeader *)test_operation_page.data;
	slot = (UndoSlotDirEntry *)(test_operation_page.data + BLCKSZ - sizeof(*slot));
	memcpy(test_operation_page.data + 1024, test_operation_page.data + sizeof(*block),
		   slot->record_length);
	slot--;
	*slot = *(slot + 1);
	slot->record_offset = 1024;
	slot->record_length -= sizeof(UndoOperationTrailer);
	slot->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
	record = (UndoRecordHeader *)(test_operation_page.data + 1024);
	record->flags = slot->flags;
	record->xid = TEST_ORIGIN_XID;
	record->payload_length -= sizeof(UndoOperationTrailer);
	record->write_scn = scn_encode(0, 69);
	memcpy(&history, (char *)(record + 1) + sizeof(UndoInsertPayload), sizeof(history));
	history.entries[0].after_write_scn = record->write_scn;
	memcpy((char *)(record + 1) + sizeof(UndoInsertPayload), &history, sizeof(history));
	block->slot_count = 2;
	block->free_offset = 1024 + slot->record_length;
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 1);
	test_origin_locator.tt_wrap = retained->canonical.wrap;
	head->undo_segment_head = test_origin_locator.uba;
	head->write_scn = record->write_scn;
	memset(f->output.data, 0xa5, BLCKSZ);
}

UT_TEST(test_operation_retained_blocking_head_repeats_three_current_phases)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;
		ClusterItlSlotData head;
		BufferTag tag;
		ClusterSavepointReaderIdentity retained, identity;

		operation_retained_head_init(&f, same, &head, &tag, &retained);
		UT_ASSERT_EQ(operation_retained_head_read(&f, &head, &tag, &retained, &identity),
					 CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(identity.operation_sequence, 0);
		UT_ASSERT(FullTransactionIdEquals(identity.top_fxid, retained.top_fxid));
		UT_ASSERT_EQ(identity.canonical_generation, same ? 17 : 23);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_release_calls, 6);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_operation_pin_depth, 0);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
		UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
	}
}

UT_TEST(test_operation_retained_blocking_refusal_and_error_cleanup)
{
	for (int same = 0; same < 2; same++)
		for (int fault = 0; fault < 14; fault++) {
			OperationPlanFixture f;
			ClusterItlSlotData head;
			BufferTag tag;
			ClusterSavepointReaderIdentity retained, identity;
			volatile bool caught = false;
			volatile ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;

			operation_retained_head_init(&f, same, &head, &tag, &retained);
			if (fault == 0)
				retained.canonical_generation++;
			if (fault == 1)
				retained.top_fxid.value++, retained.operation_fxid.value++;
			if (fault == 2)
				retained.canonical.wrap++;
			if (fault == 3)
				retained.canonical.xid++;
			if (fault == 4)
				retained.canonical.slot_offset++;
			if (fault == 5)
				retained.command_id = 8;
			if (fault == 6)
				tag.relNumber++;
			if (fault == 7)
				head.write_scn++;
			if (fault >= 8) {
				operation_driver_action = fault == 8 || (fault == 9 && same) ? 3
										  : fault == 9						 ? 8
										  : fault == 10						 ? 4
																			 : 9;
				operation_driver_action_release = fault == 9 ? 4 : fault < 11 ? 5 : fault - 7;
			}
			PG_TRY();
			{
				outcome = operation_retained_head_read(&f, &head, &tag, &retained, &identity);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			if (caught != (fault >= 11) || outcome != CLUSTER_TX_UNKNOWN)
				printf("# retained same=%d fault=%d outcome=%d caught=%d releases=%d\n", same,
					   fault, outcome, caught, test_candidate_release_calls);
			UT_ASSERT_EQ(caught, fault >= 11);
			UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
			if (caught)
				f.reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
			operation_driver_assert_refused(&f);
			UT_ASSERT_EQ(identity.operation_sequence, 0);
			UT_ASSERT_EQ(test_operation_pin_depth, 0);
			if (!caught)
				UT_ASSERT_EQ(test_candidate_acquire_calls, test_candidate_release_calls);
			else
				UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		}
}
