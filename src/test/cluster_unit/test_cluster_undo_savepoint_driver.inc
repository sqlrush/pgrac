/* Real record-owner rollback composition, with the existing explicit
 * pool/current/WAL/TT boundary services. No native cleanup is simulated.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#ifndef CLUSTER_UNDO_SAVEPOINT_DRIVER_PRESENT
static bool
cluster_undo_record_rollback_savepoint(ClusterUndoSavepointControl *control, UBA expected_head,
									   uint64 deadline, XLogRecPtr *cut)
{
	(void)control;
	(void)expected_head;
	(void)deadline;
	if (cut != NULL)
		*cut = InvalidXLogRecPtr;
	return false;
}
#endif

static void
rollback_driver_test_after_head(void)
{
	cursor_test_enable();
	if (rollback_driver_test_fault <= 3)
		cursor_test_throw_wal_at = rollback_driver_test_fault;
	else if (rollback_driver_test_fault == 4)
		cursor_test_throw_install_at = 1;
	else if (rollback_driver_test_fault == 5)
		cursor_test_flush_throw = true;
	else if (rollback_driver_test_fault == 6)
		cursor_test_flush_short = true;
}

static void
rollback_driver_test_after_cursor(void)
{
	if (reserve_test_uba_equal(cursor_test_body()->next_uba, cursor_test_body()->stop_uba)) {
		complete_test_enable();
		if (rollback_driver_test_fault == 7)
			complete_test_flush_drift = 1;
	}
}

static void
rollback_driver_test_begin(ClusterUndoSavepointControl **control,
						   ClusterUndoSavepointBounds *bounds)
{
	head_pipeline_begin(control, bounds);
	rollback_driver_test_mode = true;
	rollback_driver_test_fault = 0;
	rollback_driver_test_expected = InvalidUba;
}

static void
rollback_driver_test_end(void)
{
	rollback_driver_test_mode = false;
	complete_test_mode = false;
	cursor_test_end();
}

static bool
rollback_driver_test_run(ClusterUndoSavepointControl *control, XLogRecPtr *cut)
{
	volatile bool result = false;

	receipt_trap_error = true;
	PG_TRY();
	{
		result = cluster_undo_record_rollback_savepoint(control, rollback_driver_test_expected, 100,
														cut);
	}
	PG_CATCH();
	{
		printf("# Unexpected rollback driver error: %s\n", receipt_last_log);
	}
	PG_END_TRY();
	receipt_trap_error = false;
	return result;
}

static void
rollback_driver_test_panic(ClusterUndoSavepointControl *control, uint64 deadline)
{
	XLogRecPtr cut = 99;
	volatile bool caught = false;
	int refs = reserve_test_refs;

	receipt_trap_error = true;
	receipt_error_level = 0;
	PG_TRY();
	{
		(void)cluster_undo_record_rollback_savepoint(control, rollback_driver_test_expected,
													 deadline, &cut);
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
	UT_ASSERT_EQ(reserve_test_refs, refs);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	UT_ASSERT(!receipt_buffer_locked && !undo_savepoint_cursor_active);
	UT_ASSERT(control->owns_ref);
}

UT_TEST(test_rollback_driver_publishes_marks_and_reads_complete)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	ClusterUndoExtent extent;
	char stop_record[68];
	XLogRecPtr cut = 99;
	int refs, allocations;

	rollback_driver_test_begin(&control, &bounds);
	extent = cluster_undo_current_extent;
	refs = reserve_test_refs;
	allocations = reserve_test_allocation_count;
	/* The exclusive stop is a 64B legacy header plus its actual 4B body;
	 * it has no operation disposition byte. Preserve every real byte. */
	memcpy(stop_record, head_pipeline_record(0), sizeof(stop_record));
	UT_ASSERT(rollback_driver_test_run(control, &cut));
	UT_ASSERT_EQ(cut, 603);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	UT_ASSERT_EQ(cursor_test_flush_calls, 2);
	UT_ASSERT_EQ(cursor_test_body()->phase, 4);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 3);
	UT_ASSERT_EQ(memcmp(stop_record, head_pipeline_record(0), sizeof(stop_record)), 0);
	UT_ASSERT_EQ(*cursor_test_effect(1), 2);
	UT_ASSERT_EQ(*cursor_test_effect(3), 2);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	UT_ASSERT_EQ(head_pipeline_test_flushed, cut);
	UT_ASSERT_EQ(reserve_test_refs, refs);
	UT_ASSERT_EQ(reserve_test_allocation_count, allocations);
	UT_ASSERT_EQ(memcmp(&extent, &cluster_undo_current_extent, sizeof(extent)), 0);
	rollback_driver_test_end();
}

UT_TEST(test_rollback_driver_empty_range_keeps_unused_reservation)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;
	int refs;

	rollback_driver_test_begin(&control, &bounds);
	reserve_test_set_head(bounds.stop_uba);
	refs = reserve_test_refs;
	UT_ASSERT(rollback_driver_test_run(control, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(control->reserved && control->owns_ref);
	UT_ASSERT_EQ(control->pending_lsn + control->head_lsn + control->cursor_lsn, 0);
	UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
	UT_ASSERT_EQ(reserve_test_refs, refs);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), bounds.stop_uba));
	rollback_driver_test_end();
}

UT_TEST(test_rollback_driver_runs_every_physical_block)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;

	cursor_test_cross_block_begin(&control, &bounds);
	rollback_driver_test_mode = true;
	rollback_driver_test_fault = 0;
	rollback_driver_test_expected = InvalidUba;
	UT_ASSERT(rollback_driver_test_run(control, &cut));
	UT_ASSERT_EQ(cut, 605);
	UT_ASSERT_EQ(cursor_test_wal_calls, 5);
	UT_ASSERT_EQ(cursor_test_flush_calls, 3);
	UT_ASSERT_EQ(cursor_test_body()->phase, 4);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 4);
	UT_ASSERT_EQ((uint8)reserve_test_images[0].data[512 + 208], 2);
	UT_ASSERT_EQ(*cursor_test_effect(1), 2);
	UT_ASSERT_EQ(*cursor_test_effect(3), 2);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	rollback_driver_test_end();
}

UT_TEST(test_rollback_driver_repeat_requires_actual_complete_readback)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;
	int reads;

	rollback_driver_test_begin(&control, &bounds);
	UT_ASSERT(rollback_driver_test_run(control, &cut));
	reads = complete_test_pins;
	UT_ASSERT(rollback_driver_test_run(control, &cut));
	UT_ASSERT_EQ(cut, 603);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	UT_ASSERT_EQ(cursor_test_flush_calls, 2);
	UT_ASSERT(complete_test_pins > reads);
	*cursor_test_effect(3) = 1;
	rollback_driver_test_panic(control, 100);
	rollback_driver_test_end();
}

UT_TEST(test_rollback_driver_unknown_handles_clear_output)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;

	rollback_driver_test_begin(&control, &bounds);
	UT_ASSERT(!cluster_undo_record_rollback_savepoint(NULL, InvalidUba, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	cut = 99;
	UT_ASSERT(!cluster_undo_record_rollback_savepoint((void *)1, InvalidUba, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(!cluster_undo_record_rollback_savepoint(control, InvalidUba, 100, NULL));
	cluster_undo_record_release_savepoint_control(control);
	cut = 99;
	UT_ASSERT(!cluster_undo_record_rollback_savepoint(control, InvalidUba, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
	rollback_driver_test_end();
}

UT_TEST(test_rollback_driver_never_cancels_unsettled_data)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		rollback_driver_test_begin(&control, &bounds);
		switch (fault) {
		case 0:
			cluster_undo_record_reservation.active = true;
			break;
		case 1:
			cluster_undo_record_reservation.owns_ref = true;
			break;
		case 2:
			cluster_undo_record_reservation.consume_locked = true;
			break;
		case 3:
			cluster_undo_record_reservation.receipt.ctrc_applied_mask = 1;
			break;
		case 4:
			cluster_undo_record_reservation.receipt.ctrc_pending_mask = 1;
			break;
		case 5:
			cluster_undo_record_reservation.receipt.modifier_admission.entered = true;
			break;
		}
		rollback_driver_test_panic(control, 100);
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		memset(&cluster_undo_record_reservation, 0, sizeof(cluster_undo_record_reservation));
		cluster_undo_record_reservation.ref_slot = -1;
		rollback_driver_test_end();
	}
}

UT_TEST(test_rollback_driver_prepublication_refusal_cannot_resume_parent)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		rollback_driver_test_begin(&control, &bounds);
		if (fault == 0)
			head_pipeline_record(3)->prev_uba = head_pipeline_address(3);
		else if (fault == 1)
			pending_test_pin_fail = true;
		else if (fault == 2)
			head_pipeline_test_tt_mode = 1;
		rollback_driver_test_panic(control, fault == 3 ? 0 : 100);
		UT_ASSERT_EQ(reserve_test_refs, 1);
		rollback_driver_test_end();
	}
}

UT_TEST(test_rollback_driver_stage_failures_keep_original_responsibility)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		rollback_driver_test_begin(&control, &bounds);
		rollback_driver_test_fault = fault;
		rollback_driver_test_panic(control, 100);
		UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
		UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
		UT_ASSERT_EQ(control->head_lsn, 503);
		rollback_driver_test_end();
	}
}

UT_TEST(test_rollback_driver_post_complete_corruption_is_fatal)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;

	rollback_driver_test_begin(&control, &bounds);
	rollback_driver_test_fault = 7;
	rollback_driver_test_panic(control, 100);
	UT_ASSERT_EQ(cursor_test_body()->phase, 4);
	UT_ASSERT_EQ(*cursor_test_effect(3), 1);
	UT_ASSERT_EQ(cursor_test_wal_calls, 3);
	UT_ASSERT_EQ(head_pipeline_test_flushed, 603);
	rollback_driver_test_end();
}
