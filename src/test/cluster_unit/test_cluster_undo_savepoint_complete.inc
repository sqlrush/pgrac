/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_savepoint_complete.inc
 *      Original writer COMPLETE publication and current readback tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_savepoint_complete.inc
 *
 * NOTES
 *      Current, pool, WAL and flush services are explicit fixture boundaries.
 *      No native cleanup or transaction completion is authorized here.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SAVEPOINT_COMPLETE_PRESENT
static bool
cluster_undo_record_complete_savepoint(ClusterUndoSavepointControl *control, uint64 deadline,
									   XLogRecPtr *cut)
{
	(void)control;
	(void)deadline;
	if (cut != NULL)
		*cut = InvalidXLogRecPtr;
	return false;
}
#endif

static void
complete_test_enable(void)
{
	complete_test_mode = true;
	complete_test_pins = complete_test_drift_at = complete_test_drift_kind = 0;
	complete_test_flush_drift = 0;
}

static void
complete_test_begin(ClusterUndoSavepointControl **control, ClusterUndoSavepointBounds *bounds)
{
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_begin(control, bounds);
	UT_ASSERT(cursor_test_step(*control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	complete_test_enable();
}

static void
complete_test_end(void)
{
	complete_test_mode = false;
	cursor_test_end();
}

static void
complete_test_after_pin(void)
{
	complete_test_pins++;
	if (complete_test_pins != complete_test_drift_at)
		return;
	switch (complete_test_drift_kind) {
	case 1:
		receipt_block0_valid = false;
		break;
	case 2:
		receipt_modifier_valid = false;
		break;
	case 3:
		reserve_test_scope_valid = false;
		break;
	case 4:
		reserve_test_set_head(head_pipeline_address(3));
		break;
	case 5:
		MyProc->lxid++;
		break;
	case 6:
		ereport(ERROR, (errmsg("fixture completion read failed")));
		break;
	}
}

static void
complete_test_after_flush(void)
{
	switch (complete_test_flush_drift) {
	case 1:
		*cursor_test_effect(3) = 1;
		break;
	case 2:
		cursor_test_body()->rollback_sequence++;
		break;
	case 3:
		receipt_block0_valid = false;
		break;
	case 4:
		MyProc->lxid++;
		break;
	case 5:
		((UndoSavepointRollbackPayload *)((char *)head_pipeline_record(2) + 64))->phase = 3;
		break;
	}
}

static void
complete_test_emit(const char *image, uint32 block, uint16 rec_off, uint16 rec_len, uint16 slot_off,
				   uint16 slot_len)
{
	UndoSavepointRollbackPayload after;

	memcpy(&after, image + rec_off + 64, sizeof(after));
	UT_ASSERT_EQ(block, 1);
	UT_ASSERT_EQ(rec_off, 48);
	UT_ASSERT_EQ(rec_len, 208);
	UT_ASSERT_EQ(slot_off, 8184);
	UT_ASSERT_EQ(slot_len, 8);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	UT_ASSERT_EQ(after.phase, 4);
	UT_ASSERT_EQ(after.last_effect_wal_lsn, cursor_test_body()->last_effect_wal_lsn);
	UT_ASSERT_EQ(after.visited_count, cursor_test_body()->visited_count);
	UT_ASSERT_EQ(cursor_test_install_calls, cursor_test_wal_calls - 1);
	UT_ASSERT(reserve_test_uba_equal(after.next_uba, after.stop_uba));
}

static bool
complete_test_step(ClusterUndoSavepointControl *control, XLogRecPtr *cut)
{
	volatile bool success = false;

	receipt_trap_error = true;
	PG_TRY();
	{
		success = cluster_undo_record_complete_savepoint(control, 100, cut);
	}
	PG_CATCH();
	{
		printf("# Unexpected COMPLETE error: %s\n", receipt_last_log);
	}
	PG_END_TRY();
	receipt_trap_error = false;
	return success;
}

static void
complete_test_expect_panic(ClusterUndoSavepointControl *control)
{
	XLogRecPtr cut = 99;
	volatile bool caught = false;

	receipt_trap_error = true;
	receipt_error_level = 0;
	PG_TRY();
	{
		(void)cluster_undo_record_complete_savepoint(control, 100, &cut);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	receipt_trap_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(receipt_error_level, PANIC);
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
}

UT_TEST(test_complete_publishes_exact_body_after_all_marks_read_back)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	PGAlignedBlock expected_control, data;
	XLogRecPtr cut = 99;

	complete_test_begin(&control, &bounds);
	memcpy(expected_control.data, reserve_test_images[0].data, BLCKSZ);
	memcpy(data.data, reserve_test_images[1].data, BLCKSZ);
	((UndoSavepointRollbackPayload *)(expected_control.data + 112))->phase = 4;
	((UndoBlockHeader *)expected_control.data)->block_lsn = 603;
	MyProc->delayChkptFlags = DELAY_CHKPT_COMPLETE;
	UT_ASSERT(complete_test_step(control, &cut));
	UT_ASSERT_EQ(cut, 603);
	UT_ASSERT_EQ(head_pipeline_test_flushed, 603);
	UT_ASSERT_EQ(memcmp(expected_control.data, reserve_test_images[0].data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(data.data, reserve_test_images[1].data, BLCKSZ), 0);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	UT_ASSERT_EQ(cursor_test_flush_calls, 2);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_COMPLETE);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	complete_test_end();
}

UT_TEST(test_complete_repeat_requalifies_without_new_wal)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	int pins;

	complete_test_begin(&control, &bounds);
	UT_ASSERT(complete_test_step(control, &cut));
	pins = complete_test_pins;
	UT_ASSERT(complete_test_step(control, &cut));
	UT_ASSERT_EQ(cut, 603);
	UT_ASSERT(complete_test_pins > pins);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	UT_ASSERT_EQ(cursor_test_flush_calls, 2);
	*cursor_test_effect(1) = 1;
	complete_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	complete_test_end();
}

UT_TEST(test_complete_rejects_incomplete_published_cursor)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_begin(&control, &bounds);
	complete_test_enable();
	complete_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 0);
	UT_ASSERT_EQ(cursor_test_body()->phase, 2);
	complete_test_end();
	cursor_test_cross_block_begin(&control, &bounds);
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(!at_stop);
	complete_test_enable();
	complete_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 2);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	complete_test_end();
}

UT_TEST(test_complete_refuses_actual_live_unknown_or_bad_chain)
{
	for (unsigned i = 0; i < 8; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UndoRecordHeader *record;

		complete_test_begin(&control, &bounds);
		record = head_pipeline_record(1);
		switch (i) {
		case 0:
			*cursor_test_effect(1) = 1;
			break;
		case 1:
			*cursor_test_effect(3) = 0;
			break;
		case 2:
			((UndoSavepointRollbackPayload *)((char *)head_pipeline_record(2) + 64))->phase = 3;
			break;
		case 3:
			record->flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
			break;
		case 4:
			record->prev_uba = InvalidUba;
			break;
		case 5:
			record->prev_uba = control->uba;
			break;
		case 6:
			record->prev_uba = head_pipeline_address(3);
			break;
		case 7:
			record->xid++;
			break;
		}
		complete_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 2);
		UT_ASSERT_EQ(cursor_test_body()->phase, 3);
		complete_test_end();
	}
}

UT_TEST(test_complete_does_not_use_visited_count_as_stop_proof)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		complete_test_begin(&control, &bounds);
		if (i == 0) {
			control->cursor.visited_count = cursor_test_body()->visited_count = 1;
		} else
			head_pipeline_record(3)->prev_uba = bounds.stop_uba;
		complete_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 2);
		complete_test_end();
	}
}

UT_TEST(test_complete_rechecks_native_current_and_frame)
{
	int stages[] = { 1, 2, 5, 9 };

	for (unsigned i = 0; i < lengthof(stages); i++)
		for (int drift = 1; drift <= 6; drift++) {
			ClusterUndoSavepointControl *control;
			ClusterUndoSavepointBounds bounds;

			complete_test_begin(&control, &bounds);
			complete_test_drift_at = stages[i];
			complete_test_drift_kind = drift;
			complete_test_expect_panic(control);
			UT_ASSERT_EQ(cursor_test_wal_calls, stages[i] == 9 ? 3 : 2);
			complete_test_end();
		}
}

UT_TEST(test_complete_requires_actual_flushed_mark_and_cursor)
{
	for (unsigned i = 0; i < 6; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		complete_test_begin(&control, &bounds);
		switch (i) {
		case 0:
			head_pipeline_test_flushed = 601;
			break;
		case 1:
			((UndoBlockHeader *)reserve_test_images[0].data)->block_lsn = 601;
			break;
		case 2:
			((UndoBlockHeader *)reserve_test_images[1].data)->block_lsn = 0;
			break;
		case 3:
			control->cursor.last_effect_wal_lsn = cursor_test_body()->last_effect_wal_lsn = 503;
			break;
		case 4:
			control->cursor.last_effect_wal_lsn = cursor_test_body()->last_effect_wal_lsn = 602;
			break;
		case 5:
			cursor_test_body()->phase = 4;
			break;
		}
		complete_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 2);
		complete_test_end();
	}
}

UT_TEST(test_complete_wal_install_and_flush_failures_keep_refs)
{
	for (unsigned i = 0; i < 5; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		complete_test_begin(&control, &bounds);
		switch (i) {
		case 0:
			cursor_test_throw_wal_at = 3;
			break;
		case 1:
			cursor_test_throw_install_at = 3;
			break;
		case 2:
			cursor_test_bad_cut_at = 3;
			break;
		case 3:
			cursor_test_flush_throw = true;
			break;
		case 4:
			cursor_test_flush_short = true;
			break;
		}
		complete_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 3);
		UT_ASSERT_EQ(cursor_test_flush_calls, i < 3 ? 1 : 2);
		complete_test_end();
	}
}

UT_TEST(test_complete_post_flush_readback_cannot_trust_cache)
{
	for (int drift = 1; drift <= 5; drift++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		complete_test_begin(&control, &bounds);
		complete_test_flush_drift = drift;
		complete_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 3);
		UT_ASSERT_EQ(cursor_test_flush_calls, 2);
		complete_test_end();
	}
}

UT_TEST(test_complete_unknown_and_unpublished_clear_output)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;

	head_pipeline_begin(&control, &bounds);
	UT_ASSERT(!cluster_undo_record_complete_savepoint(control, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
	UT_ASSERT(!cluster_undo_record_complete_savepoint(control, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	cut = 99;
	UT_ASSERT(!cluster_undo_record_complete_savepoint((ClusterUndoSavepointControl *)1, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(!cluster_undo_record_complete_savepoint(control, 100, NULL));
	head_pipeline_end();
}

UT_TEST(test_complete_cross_block_readback_follows_actual_prev)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_cross_block_begin(&control, &bounds);
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	complete_test_enable();
	UT_ASSERT(complete_test_step(control, &cut));
	UT_ASSERT_EQ(cut, 605);
	UT_ASSERT_EQ(cursor_test_body()->phase, 4);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 4);
	UT_ASSERT_EQ(cursor_test_body()->last_effect_wal_lsn, 603);
	UT_ASSERT_EQ(cursor_test_wal_calls, 5);
	UT_ASSERT_EQ(cursor_test_flush_calls, 3);
	complete_test_end();
}

UT_TEST(test_complete_cannot_be_advanced_as_marking)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;

	complete_test_begin(&control, &bounds);
	UT_ASSERT(complete_test_step(control, &cut));
	complete_test_mode = false;
	cursor_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	complete_test_end();
}
