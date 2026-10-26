/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_savepoint_pending.inc
 *      Physical control staging through the real record owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_savepoint_pending.inc
 *
 * NOTES
 *      PGRAC-original tests. Native current/pool/WAL services are explicit
 *      fixture inputs; this does not prove SQL or crash recovery.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SAVEPOINT_PENDING_PRESENT
static bool
cluster_undo_record_stage_savepoint_pending(ClusterUndoSavepointControl *control,
											const ClusterUndoSavepointBounds *expected,
											uint64 deadline, XLogRecPtr *cut_out)
{
	(void)control;
	(void)expected;
	(void)deadline;
	if (cut_out != NULL)
		*cut_out = InvalidXLogRecPtr;
	return false;
}
#endif

static void
pending_test_after_pin(ClusterUndoBufPin *pin)
{
	if (pending_test_wrong_slot)
		pin->slot = 8;
	if (pending_test_scope_drift)
		reserve_test_scope_valid = false;
	if (pending_test_current_drift)
		receipt_block0_valid = false;
	if (pending_test_modifier_drift)
		receipt_modifier_valid = false;
	if (pending_test_head_drift)
		reserve_test_set_head(uba_encode(1, 2, 0, 5));
}

static XLogRecPtr
pending_test_emit(uint8 instance, uint32 segment, uint32 block, const char *image,
				  XLogRecPtr old_lsn, uint16 rec_off, uint16 rec_len, uint16 slot_off,
				  uint16 slot_len)
{
	const UndoBlockHeader *before = (const UndoBlockHeader *)reserve_test_images[block - 1].data;
	UndoSavepointRollbackPayload body;

	UT_ASSERT_EQ(instance, 1);
	UT_ASSERT_EQ(segment, 1);
	UT_ASSERT(block >= 1 && block <= 2);
	UT_ASSERT(receipt_buffer_locked && CritSectionCount > 0);
	UT_ASSERT(MyProc != NULL && MyProc->lxid == 1);
	UT_ASSERT((MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);
	UT_ASSERT_EQ(pending_test_install_calls, 0);
	UT_ASSERT_EQ(old_lsn, before->block_lsn);
	UT_ASSERT_EQ(rec_off, pending_test_record_offset);
	UT_ASSERT_EQ(rec_len, 208);
	UT_ASSERT_EQ(slot_off, pending_test_slot_offset);
	UT_ASSERT_EQ(slot_len, 8);
	memcpy(&body, image + rec_off + 64, 96);
	UT_ASSERT_EQ(body.phase, 2);
	UT_ASSERT_EQ(((const UndoRecordHeader *)(image + rec_off))->flags, 16);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(image, (BLCKSZ - slot_off) / 8 - 1)->flags, 16);
	memcpy(pending_test_wal_image.data, image, BLCKSZ);
	pending_test_wal_calls++;
	reserve_test_wal++;
	if (pending_test_wal_throw)
		siglongjmp(*PG_exception_stack, 1);
	return pending_test_wal_invalid ? InvalidXLogRecPtr : 500 + reserve_test_wal;
}

static void
pending_test_begin(ClusterUndoSavepointControl **controls, uint32 count, unsigned selected,
				   ClusterUndoSavepointBounds *bounds)
{
	UBA stop = uba_encode(2, 9, 0, 1);

	reserve_test_fixture();
	reserve_test_proc.lxid = 1;
	reserve_test_set_head(stop);
	UT_ASSERT(cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, count, 100,
															 controls));
	reserve_test_set_head(uba_encode(1, 2, 0, 3));
	UT_ASSERT(cluster_undo_record_savepoint_control_bounds(controls[selected], bounds));
	pending_test_record_offset = 48 + selected % 37 * 208;
	pending_test_slot_offset = 8192 - 8 * (selected % 37 + 1);
	pending_test_mode = true;
	pending_test_pin_fail = pending_test_wrong_slot = false;
	pending_test_scope_drift = pending_test_current_drift = pending_test_modifier_drift = false;
	pending_test_head_drift = pending_test_wal_throw = pending_test_wal_invalid = false;
	pending_test_install_throw = false;
	pending_test_wal_calls = pending_test_install_calls = 0;
}

static void
pending_test_end(void)
{
	pending_test_mode = false;
	reserve_test_scope_valid = receipt_block0_valid = receipt_modifier_valid = true;
	cluster_undo_pending.active = false;
	CritSectionCount = 0;
	MyProc = &reserve_test_proc;
	reserve_test_done();
}

UT_TEST(test_control_pending_installs_literal_image_before_head_publication)
{
	ClusterUndoSavepointControl *controls[3];
	ClusterUndoSavepointBounds bounds;
	PGAlignedBlock expected;
	UndoRecordHeader *header;
	UndoSavepointRollbackPayload *body;
	XLogRecPtr cut = 99;
	uint64 count;
	int wal;

	pending_test_begin(controls, 3, 1, &bounds);
	memcpy(expected.data, reserve_test_images[0].data, BLCKSZ);
	header = (UndoRecordHeader *)(expected.data + 256);
	body = (UndoSavepointRollbackPayload *)(expected.data + 320);
	header->flags = 16;
	header->prev_uba = bounds.current_head;
	body->phase = 2;
	body->start_uba = body->next_uba = bounds.current_head;
	body->stop_uba = bounds.stop_uba;
	UNDO_SLOT_DIR_PTR(expected.data, 1)->flags = 16;
	wal = reserve_test_wal;
	((UndoBlockHeader *)expected.data)->block_lsn = 501 + wal;
	count = pg_atomic_read_u64(&UndoRecordShared->record_alloc_count);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(controls[1], &bounds, 100, &cut));
	UT_ASSERT_EQ(cut, 501 + wal);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(pending_test_install_calls, 1);
	UT_ASSERT_EQ(memcmp(expected.data, reserve_test_images[0].data, BLCKSZ), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&UndoRecordShared->record_alloc_count), count);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), bounds.current_head));
	UT_ASSERT_EQ(reserve_test_refs, 3);
	UT_ASSERT(!cluster_undo_record_savepoint_control_owned(controls[1]));
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	for (unsigned i = 0; i < 3; i++)
		cluster_undo_record_release_savepoint_control(controls[i]);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	pending_test_end();
}

UT_TEST(test_control_pending_repeat_requires_identical_pending_bytes)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr first = 0, repeat = 0;

	pending_test_begin(&control, 1, 0, &bounds);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &first));
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &repeat));
	UT_ASSERT_EQ(first, repeat);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(pending_test_install_calls, 1);
	((UndoSavepointRollbackPayload *)(reserve_test_images[0].data + 112))->phase = 3;
	repeat = 99;
	UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &repeat));
	UT_ASSERT_EQ(repeat, 0);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	pending_test_end();
}

UT_TEST(test_control_pending_refuses_changed_bounds_without_effects)
{
	for (unsigned i = 0; i < 11; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		PGAlignedBlock before;
		XLogRecPtr cut = 99;

		pending_test_begin(&control, 1, 0, &bounds);
		memcpy(before.data, reserve_test_images[0].data, BLCKSZ);
		switch (i) {
		case 0:
			bounds.operation.top_fxid = FullTransactionIdFromEpochAndXid(6, 700);
			break;
		case 1:
			bounds.operation.operation_fxid = FullTransactionIdFromEpochAndXid(6, 701);
			break;
		case 2:
			bounds.operation.top.segment_generation++;
			break;
		case 3:
			bounds.operation.operation_sequence++;
			break;
		case 4:
			bounds.operation.command_id++;
			break;
		case 5:
			bounds.operation.subid++;
			break;
		case 6:
			bounds.control_uba = uba_encode(1, 2, 0, 1);
			break;
		case 7:
			bounds.stop_uba = InvalidUba;
			break;
		case 8:
			bounds.current_head = uba_encode(1, 2, 0, 5);
			break;
		case 9:
			bounds.operation.top.origin_instance = 2;
			break;
		case 10:
			control = (ClusterUndoSavepointControl *)(uintptr_t)1;
			break;
		}
		UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(memcmp(before.data, reserve_test_images[0].data, BLCKSZ), 0);
		UT_ASSERT_EQ(pending_test_wal_calls + pending_test_install_calls, 0);
		UT_ASSERT_EQ(reserve_test_refs, 1);
		pending_test_end();
	}
}

UT_TEST(test_control_pending_rejects_malformed_original_material)
{
	for (unsigned i = 0; i < 14; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UndoRecordHeader *record;
		UndoOperationTrailer *operation;
		XLogRecPtr cut = 99;

		pending_test_begin(&control, 1, 0, &bounds);
		record = (UndoRecordHeader *)(reserve_test_images[0].data + 48);
		operation = (UndoOperationTrailer *)(reserve_test_images[0].data + 208);
		switch (i) {
		case 0:
			((UndoBlockHeader *)reserve_test_images[0].data)->block_lsn = 0;
			break;
		case 1:
			((UndoBlockHeader *)reserve_test_images[0].data)->magic = 0;
			break;
		case 2:
			((UndoBlockHeader *)reserve_test_images[0].data)->slot_count = 0;
			break;
		case 3:
			UNDO_SLOT_DIR_PTR(reserve_test_images[0].data, 0)->record_offset = BLCKSZ - 1;
			break;
		case 4:
			UNDO_SLOT_DIR_PTR(reserve_test_images[0].data, 0)->record_length--;
			break;
		case 5:
			UNDO_SLOT_DIR_PTR(reserve_test_images[0].data, 0)->flags = 0;
			break;
		case 6:
			record->xid++;
			break;
		case 7:
			record->origin_node_id++;
			break;
		case 8:
			record->tt_wrap_plus1++;
			break;
		case 9:
			operation->canonical_top_fxid += UINT64_C(1) << 32;
			break;
		case 10:
			operation->canonical_segment_generation++;
			break;
		case 11:
			((UndoSavepointRollbackPayload *)(reserve_test_images[0].data + 112))
				->rollback_sequence++;
			break;
		case 12:
			((UndoSavepointRollbackPayload *)(reserve_test_images[0].data + 112))->phase = 2;
			break;
		case 13:
			record->target_block = 1;
			break;
		}
		UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + pending_test_install_calls, 0);
		UT_ASSERT_EQ(reserve_test_pins, 0);
		pending_test_end();
	}
}

UT_TEST(test_control_pending_rechecks_current_scope_and_modifier)
{
	for (unsigned i = 0; i < 11; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;

		pending_test_begin(&control, 1, 0, &bounds);
		switch (i) {
		case 0:
			pending_test_pin_fail = true;
			break;
		case 1:
			pending_test_wrong_slot = true;
			break;
		case 2:
			pending_test_scope_drift = true;
			break;
		case 3:
			pending_test_current_drift = true;
			break;
		case 4:
			pending_test_modifier_drift = true;
			break;
		case 5:
			pending_test_head_drift = true;
			break;
		case 6:
			MyProc = NULL;
			break;
		case 7:
			reserve_test_proc.lxid = 0;
			break;
		case 8:
			cluster_shared_config = false;
			break;
		case 9:
			CritSectionCount = 1;
			break;
		case 10:
			cluster_undo_pending.active = true;
			break;
		}
		UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + pending_test_install_calls, 0);
		UT_ASSERT(!receipt_buffer_locked);
		UT_ASSERT_EQ(reserve_test_pins, 0);
		pending_test_end();
	}
}

UT_TEST(test_control_pending_critical_wal_failures_keep_owner_and_cleanup)
{
	for (unsigned i = 0; i < 3; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;
		volatile bool caught = false;

		pending_test_begin(&control, 1, 0, &bounds);
		MyProc->delayChkptFlags = DELAY_CHKPT_COMPLETE;
		pending_test_wal_throw = i == 0;
		pending_test_wal_invalid = i == 1;
		pending_test_install_throw = i == 2;
		receipt_trap_error = true;
		receipt_error_level = 0;
		PG_TRY();
		{
			(void)cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		receipt_trap_error = false;
		UT_ASSERT(caught);
		if (i == 1)
			UT_ASSERT_EQ(receipt_error_level, PANIC);
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_COMPLETE);
		UT_ASSERT(!receipt_buffer_locked);
		UT_ASSERT_EQ(reserve_test_pins, 0);
		UT_ASSERT_EQ(reserve_test_refs, 1);
		UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), bounds.current_head));
		if (i == 2) {
			cluster_undo_record_release_savepoint_control(control);
			UT_ASSERT_EQ(reserve_test_refs, 1);
		}
		pending_test_end();
	}
}

UT_TEST(test_control_pending_updates_old_block_without_rewinding_allocator)
{
	ClusterUndoSavepointControl *controls[38];
	ClusterUndoSavepointBounds bounds;
	ClusterUndoExtent extent;
	PGAlignedBlock other;
	XLogRecPtr cut;

	pending_test_begin(controls, 38, 0, &bounds);
	extent = cluster_undo_current_extent;
	memcpy(other.data, reserve_test_images[1].data, BLCKSZ);
	UT_ASSERT_EQ(extent.cur_block, 2);
	UT_ASSERT(cluster_undo_record_stage_savepoint_pending(controls[0], &bounds, 100, &cut));
	UT_ASSERT_EQ(memcmp(&extent, &cluster_undo_current_extent, sizeof(extent)), 0);
	UT_ASSERT_EQ(memcmp(other.data, reserve_test_images[1].data, BLCKSZ), 0);
	UT_ASSERT_EQ(reserve_test_refs, 38);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	pending_test_end();
}

UT_TEST(test_control_pending_empty_scope_does_not_claim_completion)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;

	pending_test_begin(&control, 1, 0, &bounds);
	reserve_test_set_head(bounds.stop_uba);
	bounds.current_head = bounds.stop_uba;
	UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT_EQ(pending_test_wal_calls + pending_test_install_calls, 0);
	UT_ASSERT(cluster_undo_record_savepoint_control_owned(control));
	UT_ASSERT_EQ(((UndoSavepointRollbackPayload *)(reserve_test_images[0].data + 112))->phase, 1);
	UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, NULL, 100, &cut));
	UT_ASSERT_EQ(cut, 0);
	cut = 99;
	UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, NULL));
	UT_ASSERT_EQ(cut, 99);
	UT_ASSERT(!cluster_undo_record_stage_savepoint_pending(control, &bounds, 0, &cut));
	UT_ASSERT_EQ(cut, 0);
	pending_test_end();
}

UT_TEST(test_control_pending_preserves_success_diagnostics_and_refuses_unknown_debt)
{
	for (unsigned i = 0; i < 3; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		ClusterUndoRecordReservation before;
		XLogRecPtr cut = 99;

		pending_test_begin(&control, 1, 0, &bounds);
		cluster_undo_record_reservation.apply_attempted = true;
		if (i == 1)
			cluster_undo_record_reservation.receipt.ctrc_applied_mask = 1;
		if (i == 2)
			cluster_undo_record_reservation.active = true;
		before = cluster_undo_record_reservation;
		UT_ASSERT_EQ(cluster_undo_record_stage_savepoint_pending(control, &bounds, 100, &cut),
					 i == 0);
		UT_ASSERT_EQ(memcmp(&before, &cluster_undo_record_reservation, sizeof(before)), 0);
		UT_ASSERT_EQ(pending_test_wal_calls, i == 0 ? 1 : 0);
		cluster_undo_record_reservation.active = false;
		cluster_undo_record_reservation.apply_attempted = false;
		cluster_undo_record_reservation.receipt.ctrc_applied_mask = 0;
		pending_test_end();
	}
}
