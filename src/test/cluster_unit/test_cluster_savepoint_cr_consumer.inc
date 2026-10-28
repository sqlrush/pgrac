/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_cr_consumer.inc
 *   Exact operation outcome consumption by the original CR inverse.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_cr_consumer.inc
 *
 * NOTES
 *   Pgrac-original tests; real origin/CR/page bodies, external boundaries doubled.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_cr_history.h"
#include "test_cluster_operation_cr_entry.inc"

typedef struct OperationCrFixture {
	OperationPlanFixture origin;
	PGAlignedBlock source;
	BufferTag tag;
} OperationCrFixture;

static void
operation_cr_init(OperationCrFixture *f, bool same_segment, uint8 effect)
{
	char bytes[60] = { 0 };
	HeapTupleHeader tuple = (HeapTupleHeader)bytes;
	ClusterItlSlotData *slot;

	operation_driver_init(&f->origin, same_segment);
	operation_plan_effect(effect);
	test_origin_locator.itl_slot_index = 2;
	f->tag = (BufferTag){ .spcOid = 1663,
						  .dbOid = 42,
						  .relNumber = 77,
						  .forkNum = MAIN_FORKNUM,
						  .blockNum = 23 };
	PageInit((Page)f->source.data, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	((PageHeader)f->source.data)->pd_flags |= PD_HAS_ITL;
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple->t_infomask = HEAP_XMAX_INVALID;
	for (int i = 1; i <= 3; i++) {
		HeapTupleHeaderSetXmin(tuple, i == 3 ? TEST_ORIGIN_XID + 1 : TEST_ORIGIN_XID - 5);
		bytes[59] = (char)i;
		ItemPointerSet(&tuple->t_ctid, 23, i);
		UT_ASSERT_EQ(PageAddItem((Page)f->source.data, bytes, sizeof(bytes), i, false, true), i);
	}
	slot = &ClusterPageGetItlSlots((Page)f->source.data)[2];
	*slot = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
								  .wrap = 8,
								  .flags = ITL_FLAG_COMMITTED,
								  .undo_segment_head = test_origin_locator.uba,
								  .commit_scn = test_commit_scn,
								  .write_scn = scn_encode(0, 70) };
}

static bool
operation_cr_read(OperationCrFixture *f, SCN read_scn, char *result)
{
	return cluster_cr_apply_operation_origin_admitted(
		f->source.data, &f->tag, read_scn, &test_origin_locator, &f->origin.expected,
		&f->origin.admission, result, &f->origin.top, &f->origin.operation, &f->origin.reason);
}

static bool
operation_cr_has_child(const char *page)
{
	return PageGetMaxOffsetNumber((Page)page) >= 3 && ItemIdIsNormal(PageGetItemId((Page)page, 3));
}

static void
operation_cr_assert_inverse(OperationCrFixture *f)
{
	Page page = (Page)f->origin.output.data;
	ClusterItlSlotData *slot;

	/* A failed read leaves the output sentinel, not a readable page. */
	if (f->origin.top.outcome == CLUSTER_TX_UNKNOWN) {
		UT_ASSERT(false);
		return;
	}
	slot = &ClusterPageGetItlSlots(page)[2];

	UT_ASSERT(!operation_cr_has_child((char *)page));
	UT_ASSERT(operation_cr_has_child(f->source.data));
	for (int i = 1; i <= 2; i++)
		UT_ASSERT_EQ(
			memcmp(PageGetItem(page, PageGetItemId(page, i)),
				   PageGetItem((Page)f->source.data, PageGetItemId((Page)f->source.data, i)), 60),
			0);
	UT_ASSERT_EQ(slot->xid, TEST_ORIGIN_XID);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slot->wrap, 8);
	UT_ASSERT_EQ(slot->write_scn, scn_encode(0, 60));
	UT_ASSERT_EQ(slot->undo_segment_head.raw[0],
				 uba_encode(TEST_RECORD_SEGMENT, 6, TEST_TT_OFFSET, 0).raw[0]);
}

UT_TEST(test_operation_cr_discarded_before_snapshot_is_inverse)
{
	const SCN snapshots[] = { 69, 80, 100 };

	for (int same = 0; same < 2; same++)
		for (unsigned i = 0; i < lengthof(snapshots); i++) {
			OperationCrFixture f;

			operation_cr_init(&f, same, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
			UT_ASSERT(operation_cr_read(&f, snapshots[i], f.origin.output.data));
			UT_ASSERT_EQ(f.origin.top.outcome, CLUSTER_TX_COMMITTED);
			UT_ASSERT_EQ(f.origin.top.top_xid, TEST_ORIGIN_XID);
			UT_ASSERT_EQ(f.origin.operation.operation.effect_state,
						 CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
			operation_cr_assert_inverse(&f);
		}
}

UT_TEST(test_operation_cr_live_uses_top_commit_boundary)
{
	const SCN snapshots[] = { 79, 80, 100 };

	for (unsigned i = 0; i < lengthof(snapshots); i++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		UT_ASSERT(operation_cr_read(&f, snapshots[i], f.origin.output.data));
		UT_ASSERT_EQ(f.origin.top.commit_scn, 80);
		if (i == 0)
			operation_cr_assert_inverse(&f);
		else
			UT_ASSERT_EQ(memcmp(f.origin.output.data, f.source.data, BLCKSZ), 0);
	}
}

UT_TEST(test_operation_cr_active_stays_conservative)
{
	for (int action = 0; action <= 2; action++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.status = TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		operation_driver_action = action;
		operation_driver_action_release = 2;
		UT_ASSERT(operation_cr_read(&f, 100, f.origin.output.data));
		UT_ASSERT_EQ(f.origin.top.outcome, CLUSTER_TX_IN_PROGRESS);
		UT_ASSERT_EQ(f.origin.top.commit_scn, InvalidScn);
		UT_ASSERT_EQ(f.origin.operation.operation.effect_state,
					 action == 1 ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
								 : CLUSTER_SAVEPOINT_EFFECT_LIVE);
		operation_cr_assert_inverse(&f);
	}
}

UT_TEST(test_operation_cr_exact_top_abort_is_inverse)
{
	for (int discarded = 0; discarded < 2; discarded++) {
		OperationCrFixture f;

		operation_cr_init(&f, true,
						  discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
									: CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.status = TT_SLOT_ABORTED;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_ABORTED;
		UT_ASSERT(operation_cr_read(&f, 100, f.origin.output.data));
		UT_ASSERT_EQ(f.origin.top.outcome, CLUSTER_TX_ABORTED);
		UT_ASSERT_EQ(f.origin.top.top_xid, TEST_ORIGIN_XID);
		operation_cr_assert_inverse(&f);
	}
}

UT_TEST(test_operation_cr_prepared_never_publishes_image)
{
	for (int discarded = 0; discarded < 2; discarded++) {
		OperationCrFixture f;

		operation_cr_init(&f, false,
						  discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
									: CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.status = TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_twophase_prepared = true;
		UT_ASSERT(!operation_cr_read(&f, 100, f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
		UT_ASSERT_EQ(f.origin.reason, CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT);
		UT_ASSERT(operation_cr_has_child(f.source.data));
	}
}

UT_TEST(test_operation_cr_unknown_retains_input_and_output)
{
	for (int fault = 0; fault < 3; fault++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		if (fault == 0) {
			test_candidate_release_fault_call = 3;
			test_candidate_release_failed = true;
		} else {
			operation_driver_action = fault == 1 ? 4 : 3;
			operation_driver_action_release = 2;
		}
		UT_ASSERT(!operation_cr_read(&f, 100, f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
		UT_ASSERT(operation_cr_has_child(f.source.data));
	}
}

UT_TEST(test_operation_cr_keep_still_validates_exact_head)
{
	for (int fault = 0; fault < 10; fault++) {
		OperationCrFixture f;
		HeapTupleHeader tuple;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		tuple = (HeapTupleHeader)PageGetItem((Page)f.source.data,
											 PageGetItemId((Page)f.source.data, 3));
		switch (fault) {
		case 0:
			((PageHeader)f.source.data)->pd_flags &= ~PD_HAS_ITL;
			break;
		case 1:
			((PageHeader)f.source.data)->pd_special = BLCKSZ;
			break;
		case 2:
			f.tag.relNumber++;
			break;
		case 3:
			ClusterPageGetItlSlots((Page)f.source.data)[2].xid++;
			break;
		case 4:
			HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID);
			break;
		case 5:
			ClusterPageGetItlSlots((Page)f.source.data)[2].undo_segment_head = (UBA)InvalidUba_init;
			break;
		case 6:
			f.origin.expected.operation_sequence++;
			break;
		case 7:
			tuple->t_hoff = 0;
			break;
		}
		UT_ASSERT(!operation_cr_read(&f, fault == 8 ? InvalidScn : 100,
									 fault == 9 ? NULL : f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
	}
}

UT_TEST(test_operation_cr_source_result_alias)
{
	for (int discarded = 0; discarded < 2; discarded++) {
		OperationCrFixture f;
		PGAlignedBlock before;

		operation_cr_init(&f, false,
						  discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
									: CLUSTER_SAVEPOINT_EFFECT_LIVE);
		memcpy(before.data, f.source.data, BLCKSZ);
		UT_ASSERT(operation_cr_read(&f, 100, f.source.data));
		if (discarded)
			UT_ASSERT(!operation_cr_has_child(f.source.data));
		else
			UT_ASSERT_EQ(memcmp(before.data, f.source.data, BLCKSZ), 0);
	}
}

UT_TEST(test_operation_cr_uses_post_top_discard)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_driver_action = 1;
	operation_driver_action_release = 2;
	UT_ASSERT(operation_cr_read(&f, 100, f.origin.output.data));
	UT_ASSERT_EQ(f.origin.top.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(f.origin.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	operation_cr_assert_inverse(&f);
}

UT_TEST(test_operation_cr_error_does_not_publish)
{
	OperationCrFixture f;
	volatile bool caught = false;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_driver_action = 9;
	operation_driver_action_release = 3;
	PG_TRY();
	{
		(void)operation_cr_read(&f, 100, f.origin.output.data);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	operation_driver_assert_refused(&f.origin);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT(operation_cr_has_child(f.source.data));
}

UT_TEST(test_operation_cr_final_admission_fence)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_operation_cr_revoke_on_decision = true;
	UT_ASSERT(!operation_cr_read(&f, 100, f.origin.output.data));
	operation_driver_assert_refused(&f.origin);
	UT_ASSERT_EQ(f.origin.reason, CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
	UT_ASSERT(operation_cr_has_child(f.source.data));
}

/* A NULL expected receipt asks the real consumer to discover this immutable
 * head. No live-backend binding or native parent lookup supplies its owner. */
static bool
operation_cr_discover(OperationCrFixture *f, char *result)
{
	return cluster_cr_apply_operation_origin_admitted(
		f->source.data, &f->tag, 100, &test_origin_locator, NULL, &f->origin.admission, result,
		&f->origin.top, &f->origin.operation, &f->origin.reason);
}

UT_TEST(test_operation_head_without_backend_binding)
{
	for (int same = 0; same < 2; same++) {
		OperationCrFixture f;

		operation_cr_init(&f, same, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		memset(&f.origin.expected, 0, sizeof(f.origin.expected));
		test_local_binding_available = false;
		test_current_owner_available = false;
		UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
		operation_cr_assert_inverse(&f);
		UT_ASSERT_EQ(f.origin.top.top_xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(test_subtrans_parent_calls, 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_release_calls, 3);
	}
}

UT_TEST(test_operation_head_unknown_tt_wrap)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	test_origin_locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
	UT_ASSERT_EQ(f.origin.top.locator_echo.tt_wrap, TEST_ORIGIN_WRAP);
	operation_cr_assert_inverse(&f);
}

UT_TEST(test_operation_head_prior_owner_and_role_wrap)
{
	for (int role = 0; role < 2; role++) {
		OperationCrFixture f;
		UndoItlHistoryTrailer history;
		ClusterItlSlotData *restored;
		char *bytes;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		bytes = test_operation_page.data + sizeof(UndoBlockHeader) + sizeof(UndoRecordHeader)
				+ sizeof(UndoInsertPayload);
		memcpy(&history, bytes, sizeof(history));
		history.entries[0].prior.wrap = 7;
		if (role)
			history.entries[0].prior.flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		else
			history.entries[0].prior.xid = TEST_ORIGIN_XID + 1;
		memcpy(bytes, &history, sizeof(history));
		UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
		if (f.origin.top.outcome == CLUSTER_TX_UNKNOWN)
			continue;
		restored = &ClusterPageGetItlSlots((Page)f.origin.output.data)[2];
		UT_ASSERT_EQ(restored->wrap, 7);
		UT_ASSERT_EQ(restored->xid, role ? TEST_ORIGIN_XID : TEST_ORIGIN_XID + 1);
		UT_ASSERT_EQ(restored->flags, role ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE);
		UT_ASSERT(!operation_cr_has_child(f.origin.output.data));
	}
}

UT_TEST(test_operation_head_live_keeps_validated_source)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
	UT_ASSERT_EQ(memcmp(f.source.data, f.origin.output.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f.origin.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
}

UT_TEST(test_operation_head_post_top_discard)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_driver_action = 1;
	operation_driver_action_release = 2;
	UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
	UT_ASSERT_EQ(f.origin.operation.operation.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	operation_cr_assert_inverse(&f);
}

UT_TEST(test_operation_head_preserves_sampled_active)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	operation_driver_action = 2;
	operation_driver_action_release = 2;
	UT_ASSERT(operation_cr_discover(&f, f.origin.output.data));
	UT_ASSERT_EQ(f.origin.top.outcome, CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(f.origin.top.commit_scn, InvalidScn);
	operation_cr_assert_inverse(&f);
}

UT_TEST(test_operation_head_prepared_refuses)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_prepared = true;
	test_twophase_xid = TEST_ORIGIN_XID;
	UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
	UT_ASSERT_EQ(f.origin.reason, CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT);
	operation_driver_assert_refused(&f.origin);
}

static void
operation_head_bad_root(OperationCrFixture *f, int fault)
{
	ClusterItlSlotData *head = &ClusterPageGetItlSlots((Page)f->source.data)[2];

	switch (fault) {
	case 0:
		head->xid++;
		break;
	case 1:
		head->wrap++;
		break;
	case 2:
		head->write_scn++;
		break;
	case 3:
		head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 6, TEST_TT_OFFSET, 0);
		break;
	case 4:
		head->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		break;
	case 5:
		f->tag.relNumber++;
		break;
	case 6:
		f->tag.blockNum++;
		break;
	case 7:
		test_origin_locator.tt_wrap++;
		break;
	case 8:
		test_origin_locator.itl_slot_index = CLUSTER_ITL_INITRANS_DEFAULT;
		break;
	case 9:
		test_origin_locator.itl_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
		break;
	}
}

UT_TEST(test_operation_head_exact_root_required)
{
	for (int fault = 0; fault < 10; fault++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		operation_head_bad_root(&f, fault);
		UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
	}
}

UT_TEST(test_operation_head_tail_cannot_choose_other_owner)
{
	OperationCrFixture f;
	UndoOperationTrailer tail;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	memcpy(&tail, operation_plan_tail(), sizeof(tail));
	tail.canonical_top_fxid--;
	memcpy(operation_plan_tail(), &tail, sizeof(tail));
	UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
	operation_driver_assert_refused(&f.origin);
	UT_ASSERT_EQ(test_native_status_calls, 0);
}

UT_TEST(test_operation_head_canonical_not_self_certified)
{
	for (int fault = 0; fault < 3; fault++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		if (fault == 0)
			test_candidate_tt_generation++;
		else if (fault == 1)
			test_tt_slot.wrap++;
		else
			test_tt_slot.xid++;
		UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
	}
}

UT_TEST(test_operation_head_fresh_data_and_release_fences)
{
	for (int action = 3; action <= 8; action++) {
		OperationCrFixture f;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		operation_driver_action = action;
		operation_driver_action_release = action == 8 ? 1 : 2;
		UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
		operation_driver_assert_refused(&f.origin);
	}
}

UT_TEST(test_operation_head_source_result_alias)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT(operation_cr_discover(&f, f.source.data));
	if (f.origin.top.outcome != CLUSTER_TX_UNKNOWN)
		UT_ASSERT(!operation_cr_has_child(f.source.data));
}

UT_TEST(test_operation_head_final_admission_fence)
{
	OperationCrFixture f;

	operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_operation_cr_revoke_on_decision = true;
	UT_ASSERT(!operation_cr_discover(&f, f.origin.output.data));
	operation_driver_assert_refused(&f.origin);
	UT_ASSERT(test_admission_revoked);
}

UT_TEST(test_operation_head_cold_reads_and_missing_data)
{
	for (int missing = 0; missing < 2; missing++) {
		for (int mask = 1; mask <= 3; mask++) {
			OperationCrFixture f;

			operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
			test_candidate_cold_copy_mask = mask;
			test_candidate_data_pool_disabled = missing;
			UT_ASSERT_EQ(operation_cr_discover(&f, f.origin.output.data), !missing);
			if (missing)
				operation_driver_assert_refused(&f.origin);
			else {
				operation_cr_assert_inverse(&f);
				UT_ASSERT_EQ(test_candidate_data_pin_calls, test_candidate_data_unpin_calls);
			}
		}
	}
}

UT_TEST(test_operation_head_error_never_publishes)
{
	for (int fault = 0; fault < 3; fault++) {
		OperationCrFixture f;
		volatile bool caught = false;

		operation_cr_init(&f, false, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		if (fault < 2) {
			test_candidate_cold_copy_mask = fault == 0 ? 1 : 2;
			test_candidate_data_io_throw = true;
		} else {
			operation_driver_action = 9;
			operation_driver_action_release = 3;
		}
		PG_TRY();
		{
			(void)operation_cr_discover(&f, f.origin.output.data);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		operation_driver_assert_refused(&f.origin);
		UT_ASSERT(operation_cr_has_child(f.source.data));
	}
}
