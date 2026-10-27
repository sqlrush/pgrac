/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_savepoint_precommit.inc
 *      Original precommit owner requalifies completed local rollback.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_savepoint_precommit.inc
 *
 * NOTES
 *      Native identity, current, pool and WAL services are fixture inputs.
 *      These tests do not establish TT terminal retention or SQL support.
 *-------------------------------------------------------------------------
 */
static void
precommit_test_enable(ClusterUndoSavepointControl *control)
{
	complete_test_mode = false;
	precommit_test_mode = true;
	precommit_test_level = 1;
	precommit_test_binding_missing = precommit_test_refuse_current = false;
	precommit_test_top = control->operation.top_fxid;
	precommit_test_binding = control->operation.top;
	precommit_test_pins = precommit_test_binding_calls = 0;
	precommit_test_drift_at = precommit_test_drift_kind = 0;
	/* The original child frame has already ended. */
	reserve_test_scope_valid = false;
}

static void
precommit_test_begin(ClusterUndoSavepointControl **control, ClusterUndoSavepointBounds *bounds)
{
	XLogRecPtr cut;

	complete_test_begin(control, bounds);
	UT_ASSERT(complete_test_step(*control, &cut));
	UT_ASSERT_EQ(cut, 603);
	precommit_test_enable(*control);
}

static void
precommit_test_end(void)
{
	precommit_test_mode = false;
	precommit_test_level = 1;
	complete_test_end();
}

/* A read-only qualification cannot add WAL, flush, installs or ref release. */
static void
precommit_test_call(bool expect_panic)
{
	volatile bool caught = false;
	int refs = reserve_test_refs;
	int wal = cursor_test_wal_calls, installs = cursor_test_install_calls;
	int flushes = cursor_test_flush_calls;

	receipt_trap_error = true;
	receipt_error_level = 0;
	PG_TRY();
	{
		cluster_undo_xact_precommit_flush();
	}
	PG_CATCH();
	{
		caught = true;
		if (!expect_panic)
			printf("# Unexpected precommit error: %s\n", receipt_last_log);
	}
	PG_END_TRY();
	receipt_trap_error = false;
	UT_ASSERT_EQ(caught, expect_panic);
	if (expect_panic)
		UT_ASSERT_EQ(receipt_error_level, PANIC);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(reserve_test_refs, refs);
	UT_ASSERT_EQ(cursor_test_wal_calls, wal);
	UT_ASSERT_EQ(cursor_test_install_calls, installs);
	UT_ASSERT_EQ(cursor_test_flush_calls, flushes);
}

static void
precommit_test_after_pin(void)
{
	precommit_test_pins++;
	if (precommit_test_pins != precommit_test_drift_at)
		return;
	switch (precommit_test_drift_kind) {
	case 1:
		receipt_block0_valid = false;
		precommit_test_refuse_current = true;
		break;
	case 2:
		receipt_modifier_valid = false;
		break;
	case 3:
		precommit_test_binding_missing = true;
		break;
	case 4:
		precommit_test_top = FullTransactionIdFromEpochAndXid(6, 700);
		break;
	case 5:
		MyProc->lxid++;
		break;
	case 6:
		ereport(ERROR, (errmsg("fixture precommit read failed")));
		break;
	case 7:
		reserve_test_set_head(head_pipeline_address(3));
		break;
	}
}

UT_TEST(test_precommit_qualifies_complete_after_child_frame_is_gone)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	PGAlignedBlock before[2];

	precommit_test_begin(&control, &bounds);
	memcpy(before, reserve_test_images, sizeof(before));
	cluster_undo_touched_seg_count = 0;
	precommit_test_call(false);
	UT_ASSERT_EQ(precommit_test_pins, 5);
	UT_ASSERT(precommit_test_binding_calls > 0);
	UT_ASSERT_EQ(memcmp(before, reserve_test_images, sizeof(before)), 0);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	UT_ASSERT(!reserve_test_scope_valid);
	precommit_test_end();
}

/* A surviving later operation can precede the completed control. The
 * original control must still be reachable through actual prev edges. */
static void
precommit_test_live_prefix(ClusterUndoSavepointControl *control)
{
	UndoRecordHeader header;
	UndoOperationTrailer operation;
	UndoBlockHeader *block = (UndoBlockHeader *)reserve_test_images[0].data;
	UndoSlotDirEntry *slot = UNDO_SLOT_DIR_PTR(block, 1);
	char *record = reserve_test_images[0].data + 512;
	size_t offset;

	memcpy(&header, head_pipeline_record(3), sizeof(header));
	header.prev_uba = control->uba;
	memcpy(record, &header, sizeof(header));
	memcpy(record + sizeof(header), (char *)head_pipeline_record(3) + sizeof(header),
		   header.payload_length);
	offset = sizeof(header) + header.payload_length - sizeof(operation);
	memcpy(&operation, record + offset, sizeof(operation));
	operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	operation.operation_sequence = 99;
	memcpy(record + offset, &operation, sizeof(operation));
	slot->record_offset = 512;
	slot->record_length = sizeof(header) + header.payload_length;
	slot->record_type = header.record_type;
	slot->flags = header.flags;
	block->slot_count = 2;
	block->free_offset = 512 + slot->record_length;
	block->block_lsn = 604;
	reserve_test_set_head(uba_encode(1, 1, 0, 1));
}

UT_TEST(test_precommit_allows_live_prefix_but_requires_control_reachable)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;

	precommit_test_begin(&control, &bounds);
	precommit_test_live_prefix(control);
	precommit_test_call(false);
	UT_ASSERT_EQ(precommit_test_pins, 6);
	reserve_test_set_head(head_pipeline_address(3));
	precommit_test_call(true);
	precommit_test_end();
}

UT_TEST(test_precommit_refuses_native_or_canonical_identity_drift)
{
	for (unsigned i = 0; i < 10; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		precommit_test_begin(&control, &bounds);
		switch (i) {
		case 0:
			precommit_test_level = 2;
			break;
		case 1:
			precommit_test_top = FullTransactionIdFromEpochAndXid(6, 700);
			break;
		case 2:
			precommit_test_binding_missing = true;
			break;
		case 3:
			precommit_test_binding.segment_generation++;
			break;
		case 4:
			precommit_test_binding.slot_offset++;
			break;
		case 5:
			precommit_test_binding.slot_wrap++;
			break;
		case 6:
			precommit_test_binding.origin_instance++;
			break;
		case 7:
			precommit_test_binding.active_lsn++;
			break;
		case 8:
			precommit_test_binding.publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
			break;
		case 9:
			precommit_test_binding.reserved16 = 1;
			break;
		}
		precommit_test_call(true);
		precommit_test_end();
	}
}

UT_TEST(test_precommit_cannot_commit_pending_or_marking_control)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut;
		bool at_stop;

		cursor_test_begin(&control, &bounds);
		if (i == 1)
			UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
		precommit_test_enable(control);
		precommit_test_call(true);
		UT_ASSERT_EQ(cursor_test_body()->phase, i == 0 ? 2 : 3);
		precommit_test_end();
	}
}

UT_TEST(test_precommit_refuses_staged_unpublished_control)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;

	head_pipeline_begin(&control, &bounds);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
	precommit_test_enable(control);
	precommit_test_call(true);
	UT_ASSERT_EQ(control->pending_lsn, 502);
	UT_ASSERT_EQ(control->head_lsn, 0);
	precommit_test_end();
}

UT_TEST(test_precommit_rechecks_actual_control_and_discarded_range)
{
	for (unsigned i = 0; i < 7; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		precommit_test_begin(&control, &bounds);
		switch (i) {
		case 0:
			*cursor_test_effect(1) = CLUSTER_SAVEPOINT_EFFECT_LIVE;
			break;
		case 1:
			*cursor_test_effect(3) = 0;
			break;
		case 2:
			((UndoSavepointRollbackPayload *)((char *)head_pipeline_record(2) + 64))->phase = 3;
			break;
		case 3:
			cursor_test_body()->rollback_sequence++;
			break;
		case 4:
			((UndoBlockHeader *)reserve_test_images[0].data)->block_lsn = 1;
			break;
		case 5:
			head_pipeline_test_flushed = 602;
			break;
		case 6:
			cursor_test_body()->visited_count++;
			break;
		}
		precommit_test_call(true);
		precommit_test_end();
	}
}

UT_TEST(test_precommit_rechecks_current_native_head_and_binding_at_each_pin)
{
	const int pins[] = { 1, 2, 5 };

	for (unsigned i = 0; i < lengthof(pins); i++)
		for (int kind = 1; kind <= 7; kind++) {
			ClusterUndoSavepointControl *control;
			ClusterUndoSavepointBounds bounds;

			precommit_test_begin(&control, &bounds);
			precommit_test_drift_at = pins[i];
			precommit_test_drift_kind = kind;
			precommit_test_call(true);
			UT_ASSERT_EQ(precommit_test_pins, pins[i]);
			precommit_test_end();
		}
}

UT_TEST(test_precommit_repeat_still_reads_real_disposition)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	int pins;

	precommit_test_begin(&control, &bounds);
	precommit_test_call(false);
	pins = precommit_test_pins;
	precommit_test_call(false);
	UT_ASSERT_EQ(precommit_test_pins, pins + 5);
	*cursor_test_effect(1) = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	precommit_test_call(true);
	precommit_test_end();
}

UT_TEST(test_precommit_unused_reservations_and_empty_list_are_not_completed)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;

	reserve_test_fixture();
	precommit_test_call(false);
	reserve_test_done();
	head_pipeline_begin(&control, &bounds);
	precommit_test_enable(control);
	precommit_test_call(false);
	UT_ASSERT_EQ(precommit_test_pins, 0);
	UT_ASSERT_EQ(precommit_test_binding_calls, 0);
	UT_ASSERT_EQ(cursor_test_body()->phase, 1);
	UT_ASSERT(control->reserved);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	precommit_test_end();
}

UT_TEST(test_precommit_checks_all_controls_in_original_list)
{
	ClusterUndoSavepointControl *controls[2];
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	bool at_stop;

	head_pipeline_begin_controls(controls, &bounds, 2);
	UT_ASSERT(head_pipeline_publish(controls[0], &bounds, &cut));
	cursor_test_enable();
	UT_ASSERT(cursor_test_step(controls[0], &cut, &at_stop));
	complete_test_enable();
	UT_ASSERT(complete_test_step(controls[0], &cut));
	precommit_test_enable(controls[0]);
	precommit_test_call(false);
	UT_ASSERT_EQ(precommit_test_pins, 5);
	UT_ASSERT_EQ(reserve_test_refs, 2);
	precommit_test_mode = cursor_test_mode = false;
	reserve_test_scope_valid = true;
	UT_ASSERT(cluster_undo_record_savepoint_control_bounds(controls[1], &bounds));
	pending_test_record_offset = 256;
	pending_test_slot_offset = 8176;
	pending_test_wal_calls = pending_test_install_calls = 0;
	reserve_test_wal = 105;
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(controls[1], &bounds, 100, &cut));
	UT_ASSERT_EQ(cut, 606);
	cursor_test_mode = true;
	precommit_test_enable(controls[0]);
	precommit_test_call(true);
	UT_ASSERT_EQ(controls[1]->pending_lsn, 606);
	UT_ASSERT_EQ(controls[1]->head_lsn, 0);
	UT_ASSERT_EQ(reserve_test_refs, 2);
	precommit_test_end();
}
