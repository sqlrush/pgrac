/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_savepoint_cursor.inc
 *      Real record-owner block marking and cursor publication tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_savepoint_cursor.inc
 *
 * NOTES
 *      PGRAC-original tests with explicit current/pool/WAL/flush services.
 *      Fault injection does not establish native crash recovery or SQL.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SAVEPOINT_CURSOR_PRESENT
static bool
cluster_undo_record_advance_savepoint(ClusterUndoSavepointControl *control, uint64 deadline,
									  XLogRecPtr *cut, bool *at_stop)
{
	(void)control;
	(void)deadline;
	if (cut != NULL)
		*cut = InvalidXLogRecPtr;
	if (at_stop != NULL)
		*at_stop = false;
	return false;
}
#endif

UT_TEST(test_cursor_caches_unaligned_control_without_header_dereference)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	UndoRecordHeader header;
	UndoBlockHeader *block;
	XLogRecPtr cut;
	char record[208];

	pending_test_begin(&control, 1, 0, &bounds);
	memcpy(record, reserve_test_images[0].data + 48, sizeof(record));
	memcpy(&header, record, sizeof(header));
	memcpy(reserve_test_images[0].data + 252, record, sizeof(record));
	block = (UndoBlockHeader *)reserve_test_images[0].data;
	block->free_offset = 252 + sizeof(record);
	UNDO_SLOT_DIR_PTR(block, 0)->record_offset = 252;
	pending_test_record_offset = 252;
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
	UT_ASSERT_EQ(control->record_scn, header.write_scn);
	UT_ASSERT_EQ(control->cursor_lsn, cut);
	UT_ASSERT_EQ(control->cursor.phase, 2);
	pending_test_end();
}

static UndoSavepointRollbackPayload *
cursor_test_body(void)
{
	return (UndoSavepointRollbackPayload *)(reserve_test_images[0].data + 112);
}

static uint8 *
cursor_test_effect(unsigned row)
{
	return (uint8 *)head_pipeline_record(row) + 208;
}

static void
cursor_test_enable(void)
{
	cursor_test_mode = true;
	cursor_test_wal_calls = cursor_test_install_calls = cursor_test_flush_calls = 0;
	cursor_test_throw_wal_at = cursor_test_throw_install_at = cursor_test_bad_cut_at = 0;
	cursor_test_drift_pin = cursor_test_drift_kind = 0;
	cursor_test_flush_throw = cursor_test_flush_short = false;
	cursor_test_last_lsn = 600;
}

static void
cursor_test_begin(ClusterUndoSavepointControl **control, ClusterUndoSavepointBounds *bounds)
{
	XLogRecPtr cut;

	head_pipeline_begin(control, bounds);
	UT_ASSERT(head_pipeline_publish(*control, bounds, &cut));
	UT_ASSERT_EQ(cut, 503);
	cursor_test_enable();
}

static void
cursor_test_end(void)
{
	cursor_test_mode = false;
	head_pipeline_end();
}

static bool
cursor_test_step(ClusterUndoSavepointControl *control, XLogRecPtr *cut, bool *at_stop)
{
	volatile bool success = false;

	receipt_trap_error = true;
	PG_TRY();
	{
		success = cluster_undo_record_advance_savepoint(control, 100, cut, at_stop);
	}
	PG_CATCH();
	{
		printf("# Unexpected cursor error: %s\n", receipt_last_log);
	}
	PG_END_TRY();
	receipt_trap_error = false;
	return success;
}

static void
cursor_test_after_pin(ClusterUndoBufPin *pin)
{
	int kind = pin->mode == CLUSTER_UNDO_BUF_SHARED ? 1 : pin->block_no == 2 ? 2 : 3;

	UT_ASSERT_EQ(reserve_test_pins, 1);
	if (precommit_test_mode) {
		precommit_test_after_pin();
		return;
	}
	if (complete_test_mode) {
		complete_test_after_pin();
		return;
	}
	if (kind != cursor_test_drift_pin)
		return;
	switch (cursor_test_drift_kind) {
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
		ereport(ERROR, (errmsg("fixture current pin failed")));
		break;
	}
}

static XLogRecPtr
cursor_test_emit(uint8 instance, uint32 segment, uint32 block, const char *image,
				 XLogRecPtr old_lsn, uint16 rec_off, uint16 rec_len, uint16 slot_off,
				 uint16 slot_len)
{
	const UndoBlockHeader *before = (const UndoBlockHeader *)reserve_test_images[block - 1].data;
	const UndoBlockHeader *after = (const UndoBlockHeader *)image;
	XLogRecPtr lsn = ++cursor_test_last_lsn;

	UT_ASSERT_EQ(instance, 1);
	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT(block == 1 || block == 2);
	UT_ASSERT_EQ(old_lsn, before->block_lsn);
	UT_ASSERT_EQ(after->block_lsn, old_lsn);
	UT_ASSERT(receipt_buffer_locked && reserve_test_pins == 1);
	UT_ASSERT(CritSectionCount > 0 && (MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);
	UT_ASSERT_EQ(slot_len % 8, 0);
	UT_ASSERT(rec_len > 0 && slot_len > 0);
	UT_ASSERT_EQ(memcmp(image + slot_off, reserve_test_images[block - 1].data + slot_off, slot_len),
				 0);
	cursor_test_wal_calls++;
	if (complete_test_mode)
		complete_test_emit(image, block, rec_off, rec_len, slot_off, slot_len);
	else if (cursor_test_wal_calls % 2 == 1) {
		UT_ASSERT_EQ(cursor_test_body()->phase, cursor_test_wal_calls == 1 ? 2 : 3);
		UT_ASSERT_EQ(cursor_test_install_calls, cursor_test_wal_calls - 1);
	} else {
		const UndoSavepointRollbackPayload *body
			= (const UndoSavepointRollbackPayload *)(image + 112);

		UT_ASSERT_EQ(block, 1);
		UT_ASSERT_EQ(rec_off, 48);
		UT_ASSERT_EQ(rec_len, 208);
		UT_ASSERT_EQ(slot_off, 8184);
		UT_ASSERT_EQ(slot_len, 8);
		UT_ASSERT_EQ(cursor_test_install_calls, cursor_test_wal_calls - 1);
		UT_ASSERT_EQ(cursor_test_body()->phase, cursor_test_wal_calls == 2 ? 2 : 3);
		UT_ASSERT_EQ(body->phase, 3);
		UT_ASSERT_EQ(body->last_effect_wal_lsn, lsn - 1);
	}
	if (cursor_test_wal_calls == cursor_test_throw_wal_at)
		ereport(ERROR, (errmsg("fixture WAL insertion failed")));
	if (cursor_test_wal_calls == cursor_test_bad_cut_at)
		return cursor_test_wal_calls == 1 ? InvalidXLogRecPtr : lsn - 1;
	return lsn;
}

void
XLogFlush(XLogRecPtr cut)
{
	UT_ASSERT(cursor_test_mode);
	UT_ASSERT(!receipt_buffer_locked && reserve_test_pins == 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(cursor_test_body()->phase, complete_test_mode ? 4 : 3);
	UT_ASSERT_EQ(cut, cursor_test_last_lsn);
	cursor_test_flush_calls++;
	if (cursor_test_flush_throw)
		ereport(ERROR, (errmsg("fixture cursor WAL flush failed")));
	if (!cursor_test_flush_short)
		head_pipeline_test_flushed = cut;
	if (complete_test_mode)
		complete_test_after_flush();
	else if (rollback_driver_test_mode)
		rollback_driver_test_after_cursor();
}

UT_TEST(test_cursor_marks_one_block_before_progress_and_flush)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	PGAlignedBlock expected_data, expected_control;
	UndoSavepointRollbackPayload *expected;
	ClusterUndoExtent extent;
	XLogRecPtr cut = 99;
	bool at_stop = false;

	printf("# savepoint_control_bytes=%zu cursor_stage_bytes=%zu control_body_bytes=%zu\n",
		   sizeof(*control), sizeof(UndoSavepointCursorAdvance),
		   sizeof(UndoSavepointRollbackPayload));
	cursor_test_begin(&control, &bounds);
	memcpy(expected_data.data, reserve_test_images[1].data, BLCKSZ);
	memcpy(expected_control.data, reserve_test_images[0].data, BLCKSZ);
	expected_data.data[560 + 208] = expected_data.data[1584 + 208] = 2;
	((UndoBlockHeader *)expected_data.data)->block_lsn = 601;
	expected = (UndoSavepointRollbackPayload *)(expected_control.data + 112);
	expected->phase = 3;
	expected->next_uba = bounds.stop_uba;
	expected->last_effect_wal_lsn = 601;
	expected->visited_count = 3;
	((UndoBlockHeader *)expected_control.data)->block_lsn = 602;
	extent = cluster_undo_current_extent;
	MyProc->delayChkptFlags = DELAY_CHKPT_COMPLETE;
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ(cut, 602);
	UT_ASSERT_EQ(head_pipeline_test_flushed, cut);
	UT_ASSERT_EQ(memcmp(expected_data.data, reserve_test_images[1].data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(expected_control.data, reserve_test_images[0].data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(&extent, &cluster_undo_current_extent, sizeof(extent)), 0);
	UT_ASSERT_EQ(cursor_test_wal_calls, 2);
	UT_ASSERT_EQ(cursor_test_install_calls, 2);
	UT_ASSERT_EQ(cursor_test_flush_calls, 1);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_COMPLETE);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	cursor_test_end();
}

UT_TEST(test_cursor_already_discarded_data_is_idempotent)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_begin(&control, &bounds);
	*cursor_test_effect(1) = *cursor_test_effect(3) = 2;
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 3);
	UT_ASSERT_EQ(*cursor_test_effect(1), 2);
	UT_ASSERT_EQ(*cursor_test_effect(3), 2);
	UT_ASSERT_EQ(cursor_test_wal_calls, 2);
	cursor_test_end();
}

UT_TEST(test_cursor_exact_stop_repeat_is_not_complete)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;
	bool at_stop;

	cursor_test_begin(&control, &bounds);
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ(cut, 602);
	UT_ASSERT_EQ(cursor_test_wal_calls, 2);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	cursor_test_end();
}

UT_TEST(test_cursor_traverses_old_complete_control_without_effect)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	char old[208];
	XLogRecPtr cut;
	bool at_stop;

	head_pipeline_begin(&control, &bounds);
	control->stop_uba = bounds.stop_uba = head_pipeline_address(1);
	bounds.current_head = head_pipeline_address(2);
	reserve_test_set_head(bounds.current_head);
	memcpy(old, head_pipeline_record(2), sizeof(old));
	UT_ASSERT(head_pipeline_publish(control, &bounds, &cut));
	cursor_test_enable();
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 1);
	UT_ASSERT_EQ(memcmp(old, head_pipeline_record(2), sizeof(old)), 0);
	UT_ASSERT_EQ(*cursor_test_effect(1), 1);
	UT_ASSERT_EQ(*cursor_test_effect(3), 1);
	cursor_test_end();
}

static void
cursor_test_expect_panic(ClusterUndoSavepointControl *control)
{
	XLogRecPtr cut = 99;
	bool at_stop = true;
	volatile bool caught = false;

	receipt_trap_error = true;
	receipt_error_level = 0;
	PG_TRY();
	{
		(void)cluster_undo_record_advance_savepoint(control, 100, &cut, &at_stop);
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
	UT_ASSERT(!at_stop);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	UT_ASSERT(!receipt_buffer_locked);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
}

UT_TEST(test_cursor_rejects_legacy_data_without_scope_proof)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;

	head_pipeline_begin(&control, &bounds);
	control->stop_uba = bounds.stop_uba = InvalidUba;
	UT_ASSERT(head_pipeline_publish(control, &bounds, &cut));
	cursor_test_enable();
	cursor_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 0);
	UT_ASSERT_EQ(*cursor_test_effect(1), 1);
	UT_ASSERT_EQ(*cursor_test_effect(3), 1);
	UT_ASSERT_EQ(cursor_test_body()->phase, 2);
	cursor_test_end();
}

UT_TEST(test_cursor_rechecks_actual_control_before_marking)
{
	for (unsigned i = 0; i < 7; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UndoRecordHeader *header;

		cursor_test_begin(&control, &bounds);
		header = (UndoRecordHeader *)(reserve_test_images[0].data + 48);
		switch (i) {
		case 0:
			cursor_test_body()->rollback_sequence++;
			break;
		case 1:
			cursor_test_body()->next_uba = bounds.stop_uba;
			break;
		case 2:
			cursor_test_body()->visited_count++;
			break;
		case 3:
			cursor_test_body()->phase = 4;
			break;
		case 4:
			header->write_scn++;
			break;
		case 5:
			header->tt_wrap_plus1++;
			break;
		case 6:
			((UndoBlockHeader *)reserve_test_images[0].data)->block_lsn = 1;
			break;
		}
		cursor_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 0);
		cursor_test_end();
	}
}

UT_TEST(test_cursor_rejects_malformed_next_before_publication)
{
	for (unsigned i = 0; i < 7; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UndoRecordHeader *record;

		cursor_test_begin(&control, &bounds);
		record = head_pipeline_record(1);
		switch (i) {
		case 0:
			record->xid++;
			break;
		case 1:
			record->tt_wrap_plus1++;
			break;
		case 2:
			*cursor_test_effect(1) = 0;
			break;
		case 3:
			record->prev_uba = head_pipeline_address(3);
			break;
		case 4:
			record->prev_uba = control->uba;
			break;
		case 5:
			((UndoSavepointRollbackPayload *)((char *)head_pipeline_record(2) + 64))->phase = 3;
			break;
		case 6:
			UNDO_SLOT_DIR_PTR(reserve_test_images[1].data, 1)->record_offset = 8191;
			break;
		}
		cursor_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 0);
		UT_ASSERT_EQ(*cursor_test_effect(3), 1);
		cursor_test_end();
	}
}

UT_TEST(test_cursor_rechecks_original_current_native_and_scope)
{
	for (int pin = 1; pin <= 3; pin++)
		for (int drift = 1; drift <= 6; drift++) {
			ClusterUndoSavepointControl *control;
			ClusterUndoSavepointBounds bounds;

			cursor_test_begin(&control, &bounds);
			cursor_test_drift_pin = pin;
			cursor_test_drift_kind = drift;
			cursor_test_expect_panic(control);
			UT_ASSERT_EQ(cursor_test_wal_calls, pin == 3 ? 1 : 0);
			cursor_test_end();
		}
}

UT_TEST(test_cursor_failures_between_mark_and_cursor_keep_pending_owner)
{
	for (unsigned i = 0; i < 6; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		cursor_test_begin(&control, &bounds);
		if (i < 2)
			cursor_test_throw_wal_at = 1 + i;
		else if (i < 4)
			cursor_test_throw_install_at = i - 1;
		else
			cursor_test_bad_cut_at = i - 3;
		cursor_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_flush_calls, 0);
		if (i == 1 || i == 2 || i == 5) {
			UT_ASSERT_EQ(*cursor_test_effect(3), 2);
			UT_ASSERT_EQ(cursor_test_body()->phase, 2);
		}
		cursor_test_end();
	}
}

UT_TEST(test_cursor_flush_failures_cannot_resume_parent)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;

		cursor_test_begin(&control, &bounds);
		cursor_test_flush_throw = i == 0;
		cursor_test_flush_short = i == 1;
		cursor_test_expect_panic(control);
		UT_ASSERT_EQ(cursor_test_wal_calls, 2);
		UT_ASSERT_EQ(cursor_test_body()->phase, 3);
		UT_ASSERT_EQ(cursor_test_body()->visited_count, 3);
		UT_ASSERT_EQ(cursor_test_flush_calls, 1);
		cursor_test_end();
	}
}

UT_TEST(test_cursor_requires_actual_head_publication)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;
	bool at_stop = true;

	head_pipeline_begin(&control, &bounds);
	UT_ASSERT(!cluster_undo_record_advance_savepoint(control, 100, &cut, &at_stop));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(!at_stop);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
	UT_ASSERT(!cluster_undo_record_advance_savepoint(control, 100, &cut, &at_stop));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT(!at_stop);
	UT_ASSERT(!cluster_undo_record_advance_savepoint((ClusterUndoSavepointControl *)1, 100, &cut,
													 &at_stop));
	UT_ASSERT(!cluster_undo_record_advance_savepoint(control, 100, NULL, &at_stop));
	UT_ASSERT(!cluster_undo_record_advance_savepoint(control, 100, &cut, NULL));
	head_pipeline_end();
}

UT_TEST(test_cursor_respects_same_block_control_and_data)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	UndoRecordHeader record;
	UndoBlockHeader *block;
	UndoSlotDirEntry *slot;
	XLogRecPtr cut;
	bool at_stop;
	char payload[256];

	head_pipeline_begin(&control, &bounds);
	memcpy(&record, head_pipeline_record(3), 64);
	memcpy(payload, (char *)head_pipeline_record(3) + 64, record.payload_length);
	record.prev_uba = bounds.stop_uba;
	block = (UndoBlockHeader *)reserve_test_images[0].data;
	memcpy(reserve_test_images[0].data + 512, &record, 64);
	memcpy(reserve_test_images[0].data + 576, payload, record.payload_length);
	slot = UNDO_SLOT_DIR_PTR(block, 1);
	slot->record_offset = 512;
	slot->record_length = 64 + record.payload_length;
	slot->record_type = record.record_type;
	slot->flags = record.flags;
	block->slot_count = 2;
	block->free_offset = 512 + slot->record_length;
	bounds.current_head = uba_encode(1, 1, 0, 1);
	reserve_test_set_head(bounds.current_head);
	UT_ASSERT(
		cluster_undo_record_publish_savepoint_pending(control, &bounds, InvalidUba, 100, &cut));
	cursor_test_enable();
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ((uint8)reserve_test_images[0].data[512 + 208], 2);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 1);
	UT_ASSERT_EQ(cursor_test_wal_calls, 2);
	cursor_test_end();
}

static void
cursor_test_cross_block_begin(ClusterUndoSavepointControl **control,
							  ClusterUndoSavepointBounds *bounds)
{
	UndoRecordHeader record;
	UndoSlotDirEntry *slot;
	UndoBlockHeader *block;
	char payload[256];
	XLogRecPtr cut;
	uint64 sequence = 49;

	head_pipeline_begin(control, bounds);
	memcpy(&record, head_pipeline_record(1), 64);
	memcpy(payload, (char *)head_pipeline_record(1) + 64, record.payload_length);
	memcpy(payload + record.payload_length - 48 + 24, &sequence, sizeof(sequence));
	record.prev_uba = bounds->stop_uba;
	block = (UndoBlockHeader *)reserve_test_images[0].data;
	memcpy(reserve_test_images[0].data + 512, &record, 64);
	memcpy(reserve_test_images[0].data + 576, payload, record.payload_length);
	slot = UNDO_SLOT_DIR_PTR(block, 1);
	slot->record_offset = 512;
	slot->record_length = 64 + record.payload_length;
	slot->record_type = record.record_type;
	slot->flags = record.flags;
	block->slot_count = 2;
	block->free_offset = 512 + slot->record_length;
	head_pipeline_record(1)->prev_uba = uba_encode(1, 1, 0, 1);
	UT_ASSERT(head_pipeline_publish(*control, bounds, &cut));
	cursor_test_enable();
}

UT_TEST(test_cursor_cross_block_progress_follows_actual_prev)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_cross_block_begin(&control, &bounds);
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(!at_stop);
	UT_ASSERT_EQ(cut, 602);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 3);
	UT_ASSERT(reserve_test_uba_equal(cursor_test_body()->next_uba, uba_encode(1, 1, 0, 1)));
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(at_stop);
	UT_ASSERT_EQ(cut, 604);
	UT_ASSERT_EQ(cursor_test_body()->last_effect_wal_lsn, 603);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 4);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	UT_ASSERT_EQ(cursor_test_wal_calls, 4);
	UT_ASSERT_EQ(cursor_test_flush_calls, 2);
	UT_ASSERT_EQ((uint8)reserve_test_images[0].data[512 + 208], 2);
	cursor_test_end();
}

UT_TEST(test_cursor_cross_block_cycle_cannot_reset_at_batch_boundary)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	UndoRecordHeader *extra;
	XLogRecPtr cut;
	bool at_stop;

	cursor_test_cross_block_begin(&control, &bounds);
	extra = (UndoRecordHeader *)(reserve_test_images[0].data + 512);
	extra->prev_uba = head_pipeline_address(2); /* A cycle below the original start. */
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(!at_stop);
	UT_ASSERT(cursor_test_step(control, &cut, &at_stop));
	UT_ASSERT(!at_stop);
	cursor_test_expect_panic(control);
	UT_ASSERT_EQ(cursor_test_wal_calls, 4);
	UT_ASSERT_EQ(cursor_test_body()->visited_count, 4);
	UT_ASSERT_EQ(cursor_test_body()->phase, 3);
	cursor_test_end();
}
