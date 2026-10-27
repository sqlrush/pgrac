/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_savepoint_head.inc
 *      Original record owner PENDING-to-head publication tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_undo_savepoint_head.inc
 *
 * NOTES
 *      Current, pool, native WAL and canonical TT services are explicit
 *      fixture inputs. These tests do not establish SQL or crash recovery.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_tt_durable.h"

#ifndef CLUSTER_UNDO_SAVEPOINT_HEAD_PRESENT
static bool
cluster_undo_record_publish_savepoint_pending(ClusterUndoSavepointControl *control,
											  const ClusterUndoSavepointBounds *expected,
											  UBA expected_tt_head, uint64 deadline,
											  XLogRecPtr *cut)
{
	(void)control;
	(void)expected;
	(void)expected_tt_head;
	(void)deadline;
	if (cut != NULL)
		*cut = InvalidXLogRecPtr;
	return false;
}
#endif

volatile sig_atomic_t InterruptPending = false;

void
ProcessInterrupts(void)
{
	ereport(ERROR, (errmsg("fixture cancellation during retained-chain preflight")));
}

static UBA
head_pipeline_address(unsigned row)
{
	return uba_encode(1, 2, 0, row);
}

static UndoRecordHeader *
head_pipeline_record(unsigned row)
{
	return (UndoRecordHeader *)(reserve_test_images[1].data + 48 + 512 * row);
}

static void
head_pipeline_store(unsigned row, const UndoRecordHeader *record, const void *payload)
{
	UndoBlockHeader *block = (UndoBlockHeader *)reserve_test_images[1].data;
	UndoSlotDirEntry *slot = UNDO_SLOT_DIR_PTR(block, row);

	memcpy(head_pipeline_record(row), record, sizeof(*record));
	memcpy((char *)head_pipeline_record(row) + 64, payload, record->payload_length);
	slot->record_offset = 48 + row * 512;
	slot->record_length = 64 + record->payload_length;
	slot->record_type = record->record_type;
	slot->flags = record->flags;
	block->slot_count = row + 1;
	block->free_offset = slot->record_offset + slot->record_length;
}

static void
head_pipeline_data(unsigned row, UBA previous, bool legacy)
{
	OperationCodecFixture fixture;

	operation_codec_init(&fixture, UNDO_RECORD_INSERT);
	fixture.record.prev_uba = previous;
	fixture.record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	if (UBA_is_invalid(previous))
		fixture.record.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
	fixture.record.tt_wrap_plus1 = 8;
	fixture.record.xid = legacy ? 700 : 703;
	fixture.operation.canonical_top_fxid = UINT64CONST(21474837180);
	fixture.operation.operation_fxid = UINT64CONST(21474837183);
	fixture.operation.operation_sequence = 40 + row;
	fixture.operation.subid = 4;
	operation_codec_pack(&fixture);
	if (legacy) {
		fixture.record.flags &= ~(UNDO_REC_FLAG_HAS_OPERATION | UNDO_REC_FLAG_HAS_ITL_HISTORY);
		fixture.record.payload_length = 4;
	}
	head_pipeline_store(row, &fixture.record, fixture.storage + 1);
}

static void
head_pipeline_old_control(void)
{
	char image[208];
	UndoRecordHeader *record = (UndoRecordHeader *)image;
	UndoSavepointRollbackPayload *body = (UndoSavepointRollbackPayload *)(image + 64);
	ClusterUndoSavepointControl old = { 0 };

	old.operation = reserve_test_operations[1];
	UT_ASSERT(undo_savepoint_control_encode(&old, 201, image));
	record->flags = 16;
	record->prev_uba = head_pipeline_address(1);
	body->phase = 4;
	body->start_uba = record->prev_uba;
	body->stop_uba = body->next_uba = head_pipeline_address(0);
	body->visited_count = 1;
	body->last_effect_wal_lsn = 200;
	head_pipeline_store(2, record, image + 64);
}

static void
head_pipeline_begin_controls(ClusterUndoSavepointControl **control,
							 ClusterUndoSavepointBounds *bounds, uint32 count)
{
	UndoBlockHeader *block;

	reserve_test_fixture();
	reserve_test_proc.lxid = 1;
	reserve_test_set_head(head_pipeline_address(0));
	UT_ASSERT(cluster_undo_record_reserve_savepoint_controls(reserve_test_operations, count, 100,
															 control));
	block = (UndoBlockHeader *)reserve_test_images[1].data;
	block->magic = PGRAC_UNDO_BLOCK_MAGIC;
	block->block_version = UNDO_BLOCK_VERSION_1;
	block->block_lsn = 300;
	head_pipeline_data(0, InvalidUba, true);
	head_pipeline_data(1, head_pipeline_address(0), false);
	head_pipeline_old_control();
	head_pipeline_data(3, head_pipeline_address(2), false);
	reserve_test_set_head(head_pipeline_address(3));
	UT_ASSERT(cluster_undo_record_savepoint_control_bounds(*control, bounds));
	pending_test_record_offset = 48;
	pending_test_slot_offset = 8184;
	pending_test_mode = head_pipeline_test_mode = true;
	pending_test_pin_fail = pending_test_wrong_slot = pending_test_scope_drift = false;
	pending_test_current_drift = pending_test_modifier_drift = pending_test_head_drift = false;
	pending_test_wal_throw = pending_test_wal_invalid = pending_test_install_throw = false;
	pending_test_wal_calls = pending_test_install_calls = 0;
	head_pipeline_test_reads = head_pipeline_test_drift = head_pipeline_test_tt_mode = 0;
	head_pipeline_test_tt_calls = 0;
	head_pipeline_test_flushed = 0;
}

static void
head_pipeline_begin(ClusterUndoSavepointControl **control, ClusterUndoSavepointBounds *bounds)
{
	head_pipeline_begin_controls(control, bounds, 1);
}

static void
head_pipeline_end(void)
{
	head_pipeline_test_mode = false;
	pending_test_end();
}

static void
head_pipeline_test_after_pin(ClusterUndoBufPin *pin)
{
	UT_ASSERT(pin->block_no == 1 || pin->block_no == 2);
	head_pipeline_test_reads++;
	switch (head_pipeline_test_drift) {
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
		reserve_test_set_head(head_pipeline_address(1));
		break;
	case 5:
		MyProc->lxid++;
		break;
	}
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *tli)
{
	if (tli != NULL)
		*tli = 1;
	return head_pipeline_test_flushed;
}

XLogRecPtr
cluster_tt_slot_durable_publish_operation_head(const xl_undo_tt_slot_set_head_operation *record,
											   UBA expected_head, XLogRecPtr pending_lsn,
											   const ClusterSemanticAdmissionToken *admission)
{
	const char *installed = reserve_test_images[0].data + 48;
	const UndoRecordHeader *control = (const UndoRecordHeader *)installed;

	UT_ASSERT(head_pipeline_test_mode && admission->entered);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!receipt_buffer_locked && reserve_test_pins == 0);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(pending_test_install_calls, 1);
	UT_ASSERT_EQ(pending_lsn, 500 + reserve_test_wal);
	UT_ASSERT_EQ(record->head.instance, 1);
	UT_ASSERT_EQ(record->head.segment_id, 1);
	UT_ASSERT_EQ(record->head.slot_offset, 0);
	UT_ASSERT_EQ(record->head.wrap, 7);
	UT_ASSERT_EQ(record->head.xid, 700);
	UT_ASSERT_EQ(record->head._pad[0], 1);
	UT_ASSERT_EQ(record->head._pad[1] + record->head._pad[2], 0);
	UT_ASSERT(reserve_test_uba_equal(record->head.first_undo_block, uba_encode(1, 1, 0, 0)));
	UT_ASSERT_EQ(memcmp(&record->operation, installed + 160, 48), 0);
	UT_ASSERT(UBA_is_invalid(expected_head)
			  || reserve_test_uba_equal(expected_head, head_pipeline_address(2)));
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->prev_uba));
	head_pipeline_test_tt_calls++;
	switch (head_pipeline_test_tt_mode) {
	case 1:
		ereport(ERROR, (errmsg("fixture TT admission failed")));
		break;
	case 2:
		return InvalidXLogRecPtr;
	case 3:
		return pending_lsn;
	case 4:
		return pending_lsn + 1;
	case 5:
		reserve_test_scope_valid = false;
		break;
	case 6:
		reserve_test_set_head(head_pipeline_address(1));
		break;
	case 7:
		MyProc->lxid++;
		break;
	case 8:
		receipt_modifier_valid = false;
		break;
	}
	head_pipeline_test_flushed = pending_lsn + 1;
	return pending_lsn + 1;
}

static bool
head_pipeline_publish(ClusterUndoSavepointControl *control,
					  const ClusterUndoSavepointBounds *bounds, XLogRecPtr *cut)
{
	return cluster_undo_record_publish_savepoint_pending(control, bounds, head_pipeline_address(2),
														 100, cut);
}

UT_TEST(test_control_head_pipeline_publishes_after_complete_chain_proof)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	ClusterUndoExtent extent;
	PGAlignedBlock chain;
	XLogRecPtr cut = 99;

	head_pipeline_begin(&control, &bounds);
	extent = cluster_undo_current_extent;
	memcpy(chain.data, reserve_test_images[1].data, BLCKSZ);
	UT_ASSERT(head_pipeline_publish(control, &bounds, &cut));
	UT_ASSERT_EQ(cut, 503);
	UT_ASSERT_EQ(head_pipeline_test_flushed, cut);
	UT_ASSERT(head_pipeline_test_reads >= 4);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT_EQ(memcmp(chain.data, reserve_test_images[1].data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(&extent, &cluster_undo_current_extent, sizeof(extent)), 0);
	UT_ASSERT(reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	UT_ASSERT(!reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), bounds.stop_uba));
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	head_pipeline_end();
}

UT_TEST(test_control_head_pipeline_nil_tt_and_stop_preserve_full_chain)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut;

	head_pipeline_begin(&control, &bounds);
	control->stop_uba = bounds.stop_uba = InvalidUba;
	UT_ASSERT(
		cluster_undo_record_publish_savepoint_pending(control, &bounds, InvalidUba, 100, &cut));
	UT_ASSERT_EQ(cut, 503);
	UT_ASSERT(head_pipeline_test_reads >= 4);
	head_pipeline_end();
}

UT_TEST(test_control_head_pipeline_rejects_malformed_retained_members)
{
	for (unsigned i = 0; i < 16; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UndoRecordHeader *record;
		UndoOperationTrailer *operation;
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		record = head_pipeline_record(3);
		operation = (UndoOperationTrailer *)((char *)record + 64 + record->payload_length - 48);
		switch (i) {
		case 0:
			((UndoBlockHeader *)reserve_test_images[1].data)->magic = 0;
			break;
		case 1:
			((UndoBlockHeader *)reserve_test_images[1].data)->block_version++;
			break;
		case 2:
			((UndoBlockHeader *)reserve_test_images[1].data)->block_lsn = 0;
			break;
		case 3:
			UNDO_SLOT_DIR_PTR(reserve_test_images[1].data, 3)->record_offset = 8191;
			break;
		case 4:
			UNDO_SLOT_DIR_PTR(reserve_test_images[1].data, 3)->record_length--;
			break;
		case 5:
			UNDO_SLOT_DIR_PTR(reserve_test_images[1].data, 3)->flags = 0;
			break;
		case 6:
			record->origin_node_id++;
			break;
		case 7:
			record->tt_slot_segment_id++;
			break;
		case 8:
			record->tt_slot_id++;
			break;
		case 9:
			record->tt_wrap_plus1++;
			break;
		case 10:
			record->write_scn = 0;
			break;
		case 11:
			operation->canonical_top_fxid += UINT64CONST(4294967296);
			break;
		case 12:
			operation->canonical_segment_generation++;
			break;
		case 13:
			operation->effect_state = 0;
			break;
		case 14:
			operation->operation_sequence = 31;
			break;
		case 15:
			record->xid++;
			break;
		}
		UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		UT_ASSERT_EQ(reserve_test_pins, 0);
		head_pipeline_end();
	}
}

UT_TEST(test_control_head_pipeline_requires_stop_and_canonical_ancestry)
{
	for (unsigned i = 0; i < 5; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		UBA expected = head_pipeline_address(2);
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		if (i == 0)
			control->stop_uba = bounds.stop_uba = head_pipeline_address(7);
		else if (i == 1)
			expected = head_pipeline_address(7);
		else if (i == 2)
			head_pipeline_record(1)->prev_uba = head_pipeline_address(3);
		else if (i == 3)
			head_pipeline_record(3)->prev_uba = head_pipeline_address(3);
		else
			expected = head_pipeline_address(1); /* DATA is not a published control. */
		UT_ASSERT(
			!cluster_undo_record_publish_savepoint_pending(control, &bounds, expected, 100, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		head_pipeline_end();
	}
}

UT_TEST(test_control_head_pipeline_validates_below_stop)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		control->stop_uba = bounds.stop_uba = head_pipeline_address(2);
		if (i == 0)
			head_pipeline_record(0)->xid = 703; /* Unextended raw child is UNKNOWN. */
		else
			head_pipeline_record(0)->prev_uba = head_pipeline_address(3);
		UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		head_pipeline_end();
	}
}

UT_TEST(test_control_head_pipeline_rechecks_original_pin_publication)
{
	for (unsigned i = 1; i <= 5; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		head_pipeline_test_drift = i;
		UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		UT_ASSERT_EQ(reserve_test_pins, 0);
		head_pipeline_end();
	}
}

UT_TEST(test_control_head_pipeline_refuses_unknown_preparation_debt)
{
	for (unsigned i = 0; i < 4; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		if (i == 0)
			cluster_undo_pending.active = true;
		else if (i == 1)
			cluster_undo_record_reservation.receipt.ctrc_applied_mask = 2;
		else if (i == 2)
			cluster_shared_config = false;
		else
			MyProc->lxid = 0;
		UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
		UT_ASSERT_EQ(cut, 0);
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		cluster_undo_record_reservation.receipt.ctrc_applied_mask = 0;
		head_pipeline_end();
	}
}

static void
head_pipeline_expect_panic(int mode)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;
	volatile bool caught = false;

	head_pipeline_begin(&control, &bounds);
	head_pipeline_test_tt_mode = mode;
	receipt_trap_error = true;
	receipt_error_level = 0;
	PG_TRY();
	{
		(void)head_pipeline_publish(control, &bounds, &cut);
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
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT_EQ(reserve_test_pins, 0);
	cluster_undo_record_release_savepoint_control(control);
	UT_ASSERT_EQ(reserve_test_refs, 1);
	UT_ASSERT(!reserve_test_uba_equal(cluster_undo_local_head_get(1, 0), control->uba));
	head_pipeline_end();
}

UT_TEST(test_control_head_pipeline_promotes_tt_errors_after_pending)
{
	head_pipeline_expect_panic(1);
}

UT_TEST(test_control_head_pipeline_requires_later_durable_cut)
{
	for (int mode = 2; mode <= 4; mode++)
		head_pipeline_expect_panic(mode);
}

UT_TEST(test_control_head_pipeline_rechecks_scope_and_local_owner_after_tt)
{
	for (int mode = 5; mode <= 8; mode++)
		head_pipeline_expect_panic(mode);
}

UT_TEST(test_control_head_pipeline_refuses_second_publication)
{
	ClusterUndoSavepointControl *control;
	ClusterUndoSavepointBounds bounds;
	XLogRecPtr cut = 99;

	head_pipeline_begin(&control, &bounds);
	UT_ASSERT(head_pipeline_publish(control, &bounds, &cut));
	cut = 99;
	UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
	UT_ASSERT_EQ(cut, 0);
	UT_ASSERT_EQ(pending_test_wal_calls, 1);
	UT_ASSERT_EQ(head_pipeline_test_tt_calls, 1);
	UT_ASSERT(
		!cluster_undo_record_publish_savepoint_pending(control, &bounds, InvalidUba, 100, NULL));
	head_pipeline_end();
}

UT_TEST(test_control_head_pipeline_reserved_controls_are_off_chain)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterUndoSavepointControl *control;
		ClusterUndoSavepointBounds bounds;
		XLogRecPtr cut = 99;

		head_pipeline_begin(&control, &bounds);
		if (i == 0)
			head_pipeline_record(1)->prev_uba = control->uba;
		else {
			char image[208];
			ClusterUndoSavepointControl old = { 0 };

			old.operation = reserve_test_operations[1];
			UT_ASSERT(undo_savepoint_control_encode(&old, 201, image));
			head_pipeline_store(2, (UndoRecordHeader *)image, image + 64);
			((UndoBlockHeader *)reserve_test_images[1].data)->slot_count = 4;
			((UndoBlockHeader *)reserve_test_images[1].data)->free_offset = 2048;
		}
		UT_ASSERT(!head_pipeline_publish(control, &bounds, &cut));
		UT_ASSERT_EQ(pending_test_wal_calls + head_pipeline_test_tt_calls, 0);
		head_pipeline_end();
	}
}
