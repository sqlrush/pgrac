/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_origin_plan.inc
 *   Ordered operation reads through the original origin continuation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_origin_plan.inc
 *
 * NOTES
 *   Real runtime/record decode; current, storage, native status are doubles.
 *-------------------------------------------------------------------------
 */
typedef struct OperationPlanFixture {
	ClusterRuntimeVisibilityOriginPlan plan;
	ClusterSavepointOperation expected;
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterUndoBlock0ResolvedRoot data_root;
	ClusterUndoBlock0ResolvedRoot tt_root;
	ClusterTxResolution top;
	ClusterSavepointReaderRecord operation;
	ClusterTxResolveReason reason;
	PGAlignedBlock output;
} OperationPlanFixture;

static char *
operation_plan_tail(void)
{
	UndoRecordHeader *record
		= (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));

	return (char *)(record + 1) + record->payload_length - sizeof(UndoOperationTrailer);
}

static void
operation_plan_effect(uint8 effect)
{
	operation_plan_tail()[offsetof(UndoOperationTrailer, effect_state)] = (char)effect;
}

static void
operation_plan_init(OperationPlanFixture *f, bool same_segment, uint8 effect)
{
	UndoBlockHeader block = { 0 };
	UndoSlotDirEntry slot = { 0 };
	UndoRecordHeader record;
	UndoItlHistoryTrailer history = { 0 };
	UndoOperationTrailer operation = { 0 };
	UndoInsertPayload insert = { 60, 0 };
	char *body;

	reset_exact_origin_fixture();
	memset(f, 0, sizeof(*f));
	test_operation_page_enabled = true;
	test_origin_locator.uba = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 0);
	f->expected.top = test_local_binding;
	if (same_segment) {
		f->expected.top.segment_id = TEST_RECORD_SEGMENT;
		f->expected.top.segment_generation = 17;
	}
	f->expected.top_fxid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID);
	f->expected.operation_fxid = FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 1);
	f->expected.operation_sequence = 12;
	f->expected.command_id = 8;
	f->expected.subid = 4;
	f->admission
		= (ClusterSemanticAdmissionToken){ .feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
										   .record_generation = 5,
										   .formation_epoch = 42,
										   .side = CLUSTER_SEMANTIC_TARGET_SIDE,
										   .entered = true };
	f->data_root = (ClusterUndoBlock0ResolvedRoot){ .intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED,
													.root_id = 91,
													.root_generation = 7 };
	f->tt_root = f->data_root;
	if (!same_segment)
		f->tt_root.root_id = 92;
	record = test_origin_record;
	record.record_type = UNDO_RECORD_INSERT;
	record.flags = UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION;
	record.xid = TEST_ORIGIN_XID + 1;
	record.tt_slot_segment_id = f->expected.top.segment_id;
	record.write_scn = scn_encode(0, 70);
	record.prev_uba = uba_encode(TEST_RECORD_SEGMENT, 6, TEST_TT_OFFSET, 0);
	record.target_locator = (RelFileLocator){ 1663, 42, 77 };
	record.target_fork = MAIN_FORKNUM;
	record.target_block = 23;
	record.target_offset = 3;
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0] = (UndoItlHistoryEntry){ .block = 23,
												.slot_index = 2,
												.after_kind = ITL_FLAG_ACTIVE,
												.after_write_scn = record.write_scn,
												.prior = { .xid = TEST_ORIGIN_XID,
														   .wrap = 8,
														   .flags = ITL_FLAG_ACTIVE,
														   .write_scn = scn_encode(0, 60),
														   .undo_segment_head = record.prev_uba } };
	operation = (UndoOperationTrailer){
		.version = UNDO_OPERATION_TRAILER_VERSION,
		.length = sizeof(operation),
		.effect_state = effect,
		.canonical_top_fxid = U64FromFullTransactionId(f->expected.top_fxid),
		.operation_fxid = U64FromFullTransactionId(f->expected.operation_fxid),
		.operation_sequence = 12,
		.canonical_segment_generation = f->expected.top.segment_generation,
		.command_id = 8,
		.subid = 4
	};
	record.payload_length = sizeof(insert) + sizeof(history) + sizeof(operation);
	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.slot_count = 1;
	block.free_offset = sizeof(block) + sizeof(record) + record.payload_length;
	slot.record_offset = sizeof(block);
	slot.record_length = sizeof(record) + record.payload_length;
	slot.record_type = record.record_type;
	slot.flags = record.flags;
	memset(test_operation_page.data, 0, BLCKSZ);
	memcpy(test_operation_page.data, &block, sizeof(block));
	memcpy(test_operation_page.data + sizeof(block), &record, sizeof(record));
	body = test_operation_page.data + sizeof(block) + sizeof(record);
	memcpy(body, &insert, sizeof(insert));
	memcpy(body + sizeof(insert), &history, sizeof(history));
	memcpy(body + sizeof(insert) + sizeof(history), &operation, sizeof(operation));
	memcpy(test_operation_page.data + BLCKSZ - sizeof(slot), &slot, sizeof(slot));
}

static ClusterRuntimeVisibilityOriginStep
operation_plan_freeze(OperationPlanFixture *f)
{
	return cluster_runtime_visibility_origin_plan_freeze_operation_held(
		&test_origin_locator, &f->expected, CLUSTER_TX_RESOLVE_VISIBILITY, &f->admission, NULL,
		&f->guard, &f->data_root, &f->plan, &f->reason);
}

static ClusterTxOutcome
operation_plan_copy(OperationPlanFixture *f)
{
	return cluster_runtime_visibility_origin_plan_copy_operation_held(
		&f->plan, CLUSTER_TX_RESOLVE_VISIBILITY, &f->admission, &f->guard, &f->data_root, &f->top,
		&f->operation, f->output.data, &f->reason);
}

static void
operation_plan_sample(OperationPlanFixture *f)
{
	UT_ASSERT_EQ(operation_plan_freeze(f), CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
	UT_ASSERT(cluster_runtime_visibility_origin_plan_sample_canonical_held(
		&f->plan, CLUSTER_TX_RESOLVE_VISIBILITY, &f->admission, &f->guard, &f->tt_root,
		&f->reason));
}

static void
operation_plan_refused(OperationPlanFixture *f)
{
	ClusterTxResolution empty = { 0 };
	ClusterSavepointReaderRecord no_operation = { 0 };
	PGAlignedBlock untouched;

	memset(f->output.data, 0xa5, BLCKSZ);
	untouched = f->output;
	memset(&f->top, 0xa5, sizeof(f->top));
	memset(&f->operation, 0xa5, sizeof(f->operation));
	UT_ASSERT_EQ(operation_plan_copy(f), CLUSTER_TX_UNKNOWN);
	UT_ASSERT(f->reason != CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(memcmp(&f->top, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(memcmp(&f->operation, &no_operation, sizeof(no_operation)), 0);
	UT_ASSERT_EQ(memcmp(f->output.data, untouched.data, BLCKSZ), 0);
}

UT_TEST(test_operation_plan_requires_three_phases)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;
		ClusterUndoBlock0LogicalKey logical = { 0 };

		operation_plan_init(&f, same, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(operation_plan_freeze(&f), CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
		UT_ASSERT(cluster_runtime_visibility_origin_plan_canonical_logical(&f.plan, &logical));
		UT_ASSERT_EQ(logical.segment_id, same ? 1 : 2);
		UT_ASSERT_EQ(test_native_status_calls, 0);
		operation_plan_refused(&f);
	}
}

UT_TEST(test_operation_plan_consumes_post_top_discard)
{
	for (int same = 0; same < 2; same++) {
		OperationPlanFixture f;
		ClusterRuntimeVisibilityCounters counters;

		operation_plan_init(&f, same, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		operation_plan_sample(&f);
		operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT_EQ(operation_plan_copy(&f), CLUSTER_TX_COMMITTED);
		UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT_EQ(f.top.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(f.top.local_canonical.segment_id, same ? 1 : 2);
		UT_ASSERT_EQ(f.top.local_canonical_generation, same ? 17 : 23);
		UT_ASSERT_EQ(memcmp(f.output.data, test_operation_page.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_data_copy_calls, 2);
		cluster_runtime_visibility_exact_counters_snapshot(&counters);
		UT_ASSERT_EQ(counters.local_memo_publishes, 0);
		operation_plan_refused(&f); /* Single-use after success. */
	}
}

UT_TEST(test_operation_plan_keeps_sampled_active)
{
	OperationPlanFixture f;

	operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	operation_plan_sample(&f);
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.commit_scn = test_commit_scn;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(operation_plan_copy(&f), CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(f.top.commit_scn, InvalidScn);
}

UT_TEST(test_operation_plan_keeps_exact_abort)
{
	OperationPlanFixture f;

	operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	operation_plan_sample(&f);
	UT_ASSERT_EQ(operation_plan_copy(&f), CLUSTER_TX_ABORTED);
	UT_ASSERT_EQ(f.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(test_operation_plan_preserves_prepared)
{
	for (uint8 effect = 1; effect <= 2; effect++) {
		OperationPlanFixture f;

		operation_plan_init(&f, false, effect);
		test_tt_slot.status = TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_twophase_prepared = true;
		operation_plan_sample(&f);
		UT_ASSERT_EQ(operation_plan_copy(&f), CLUSTER_TX_PREPARED);
		UT_ASSERT_EQ(f.operation.operation.effect_state, effect);
	}
}

UT_TEST(test_operation_plan_rejects_immutable_changes)
{
	for (int fault = 0; fault < 12; fault++) {
		OperationPlanFixture f;
		UndoRecordHeader *record;
		UndoOperationTrailer tail;

		operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		operation_plan_sample(&f);
		record = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
		memcpy(&tail, operation_plan_tail(), sizeof(tail));
		switch (fault) {
		case 0:
			tail.operation_sequence++;
			break;
		case 1:
			tail.canonical_top_fxid += UINT64CONST(1) << 32;
			break;
		case 2:
			tail.operation_fxid += UINT64CONST(1) << 32;
			break;
		case 3:
			tail.canonical_segment_generation++;
			break;
		case 4:
			tail.command_id++;
			break;
		case 5:
			tail.subid++;
			break;
		case 6:
			record->target_block++;
			break;
		case 7:
			record->prev_uba = uba_encode(1, 5, TEST_TT_OFFSET, 0);
			break;
		case 8:
			((UndoInsertPayload *)(record + 1))->inserted_tuple_len++;
			break;
		case 9:
			((UndoBlockHeader *)test_operation_page.data)->free_offset--;
			break;
		case 10:
			tail.effect_state = 0;
			break;
		case 11:
			record->xid++;
			break;
		}
		memcpy(operation_plan_tail(), &tail, sizeof(tail));
		operation_plan_refused(&f);
	}
}

UT_TEST(test_operation_plan_rejects_discard_reversal)
{
	OperationPlanFixture f;

	operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	operation_plan_sample(&f);
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_plan_refused(&f);
}

UT_TEST(test_operation_plan_requires_exact_canonical)
{
	for (int fault = 0; fault < 6; fault++) {
		OperationPlanFixture f;

		operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT_EQ(operation_plan_freeze(&f), CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL);
		switch (fault) {
		case 0:
			test_candidate_tt_generation++;
			break;
		case 1:
			test_tt_slot.xid++;
			break;
		case 2:
			test_tt_slot.wrap++;
			break;
		case 3:
			test_tt_slot.status = 0;
			break;
		case 4:
			test_candidate_copy_physical_slot = false;
			break;
		case 5:
			test_native_status = (XidStatus)99;
			break;
		}
		UT_ASSERT(!cluster_runtime_visibility_origin_plan_sample_canonical_held(
			&f.plan, CLUSTER_TX_RESOLVE_VISIBILITY, &f.admission, &f.guard, &f.tt_root, &f.reason));
		operation_plan_refused(&f);
	}
}

UT_TEST(test_operation_plan_preserves_final_fences)
{
	for (int fault = 0; fault < 5; fault++) {
		OperationPlanFixture f;

		operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		operation_plan_sample(&f);
		switch (fault) {
		case 0:
			test_candidate_data_generation++;
			break;
		case 1:
			test_candidate_root_drift = true;
			break;
		case 2:
			test_formation_epoch++;
			break;
		case 3:
			test_admission_revoked = true;
			break;
		case 4:
			test_candidate_cold_copy_mask = 2;
			break;
		}
		operation_plan_refused(&f);
	}
}

UT_TEST(test_operation_plan_cannot_escape_ordinary_api)
{
	OperationPlanFixture f;

	operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_plan_sample(&f);
	UT_ASSERT_EQ(cluster_runtime_visibility_origin_plan_copy_data_held(
					 &f.plan, CLUSTER_TX_RESOLVE_VISIBILITY, &f.admission, &f.guard, &f.data_root,
					 &f.top, f.output.data, &f.reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(operation_plan_copy(&f), CLUSTER_TX_COMMITTED);
}

UT_TEST(test_operation_plan_rejects_bad_inputs)
{
	for (int fault = 0; fault < 5; fault++) {
		OperationPlanFixture f;

		operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		switch (fault) {
		case 0:
			f.expected.top.xid++;
			break;
		case 1:
			f.expected.operation_sequence++;
			break;
		case 2:
			f.expected.top.publish_state = 0;
			break;
		case 3:
			test_origin_locator.tt_wrap = TT_WRAP_INVALID;
			break;
		case 4:
			test_operation_page_enabled = false;
			break;
		}
		UT_ASSERT_EQ(operation_plan_freeze(&f), CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED);
		operation_plan_refused(&f);
	}
}

UT_TEST(test_operation_plan_rejects_malformed_page)
{
	for (int fault = 0; fault < 16; fault++) {
		OperationPlanFixture f;
		UndoBlockHeader *block = (UndoBlockHeader *)test_operation_page.data;
		UndoSlotDirEntry *slot
			= (UndoSlotDirEntry *)(test_operation_page.data + BLCKSZ - sizeof(UndoSlotDirEntry));
		UndoRecordHeader *record
			= (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));

		operation_plan_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		switch (fault) {
		case 0:
			block->magic++;
			break;
		case 1:
			block->block_version++;
			break;
		case 2:
			block->slot_count = 0;
			break;
		case 3:
			block->slot_count = UINT16_MAX;
			break;
		case 4:
			block->free_offset = sizeof(*block) - 1;
			break;
		case 5:
			block->free_offset = BLCKSZ;
			break;
		case 6:
			slot->record_offset = 0;
			break;
		case 7:
			slot->record_length = sizeof(*record) - 1;
			break;
		case 8:
			slot->record_offset = block->free_offset + 1;
			break;
		case 9:
			slot->record_length = UINT16_MAX;
			break;
		case 10:
			slot->flags ^= UNDO_REC_FLAG_FIRST_IN_TX;
			break;
		case 11:
			slot->record_type = UNDO_RECORD_UPDATE;
			break;
		case 12:
			record->payload_length++;
			break;
		case 13:
			record->prev_uba = test_origin_locator.uba;
			break;
		case 14:
			test_origin_locator.uba = uba_encode(1, 7, TEST_TT_OFFSET, 1);
			break;
		case 15:
			record->origin_node_id++;
			break;
		}
		UT_ASSERT_EQ(operation_plan_freeze(&f), CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED);
		operation_plan_refused(&f);
	}
}
