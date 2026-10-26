/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_live_head.inc
 *    Exercise the linked canonical TT operation-head publisher.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_tt_live_head.inc
 *
 * NOTES
 *    PGRAC-original tests. Current, storage and WAL boundaries are fixtures;
 *    the publisher and header sealing are the production implementations.
 *
 *-------------------------------------------------------------------------
 */

typedef struct TestLiveHeadCall {
	TestUndoHeadOperation record;
	UBA previous;
	XLogRecPtr pending_lsn;
	XLogRecPtr result;
	ClusterSemanticAdmissionToken admission;
} TestLiveHeadCall;

static xl_undo_tt_slot_set_head_operation live_head_emitted;
static int live_head_emit_calls;
static int live_head_emit_flags;
static uint32 live_head_emit_critical;
static XLogRecPtr live_head_emit_lsn;
static bool live_head_short_flush;
static bool live_head_flush_error;
static unsigned live_head_after_flush;
static TestLiveHeadCall *live_head_input;

XLogRecPtr
cluster_undo_emit_tt_slot_set_head_operation(const xl_undo_tt_slot_set_head_operation *record)
{
	live_head_emit_calls++;
	live_head_emit_flags = MyProc->delayChkptFlags;
	live_head_emit_critical = CritSectionCount;
	live_head_emitted = *record;
	UT_ASSERT(!g_current_active);
	UT_ASSERT(g_pin_reserved);
	UT_ASSERT_EQ(g_writer_fd_open, 1);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	return live_head_emit_lsn;
}

static void
live_head_flush(XLogRecPtr cut)
{
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;

	UT_ASSERT_EQ(live_head_emit_calls, 1);
	UT_ASSERT(!g_current_active && g_pin_reserved);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	if (live_head_flush_error)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR), errmsg("injected operation head flush failure")));
	g_flush_ptr = live_head_short_flush ? cut - 1 : cut;
	if (live_head_after_flush == 1)
		g_lock_content_ok = false;
	else if (live_head_after_flush == 2)
		resident->wrap_count++;
	else if (live_head_after_flush == 3)
		resident->tt_slots[7].xid++;
	else if (live_head_after_flush == 4)
		g_write_hdr_ok = false;
	else if (live_head_after_flush == 5)
		g_current_root_ok = false;
	else if (live_head_after_flush == 6)
		resident->owner_instance++;
	else if (live_head_after_flush == 7)
		MyProc->lxid++;
	else if (live_head_after_flush == 8)
		MyProc = NULL;
	else if (live_head_after_flush == 9)
		MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	else if (live_head_after_flush == 10)
		g_current_root.root_generation++;
}

static TestLiveHeadCall
live_head_setup(bool target)
{
	TestLiveHeadCall call = { 0 };

	reset_current_write_mock();
	cluster_shared_config = true;
	seed_current_exact_active(7, 778, 5);
	enable_sealed_header_io();
	call.record = test_head_operation();
	call.pending_lsn = (XLogRecPtr)0x1000;
	call.admission = target ? target_modifier_token() : source_modifier_token();
	live_head_emit_calls = 0;
	live_head_emit_flags = 0;
	live_head_emit_critical = 0;
	live_head_emit_lsn = (XLogRecPtr)0x2000;
	live_head_short_flush = false;
	live_head_flush_error = false;
	live_head_after_flush = 0;
	live_head_input = NULL;
	memset(&live_head_emitted, 0, sizeof(live_head_emitted));
	g_live_head_test_mode = true;
	g_live_head_flush_hook = live_head_flush;
	g_write_requires_flush = live_head_emit_lsn;
	MyProc->delayChkptFlags = DELAY_CHKPT_COMPLETE;
	return call;
}

static bool
live_head_call(TestLiveHeadCall *call, bool null_record, bool null_admission)
{
	volatile bool caught = false;

	call->result = InvalidXLogRecPtr;
	PG_TRY();
	{
#ifdef CLUSTER_TT_LIVE_OPERATION_HEAD_PRESENT
		call->result = cluster_tt_slot_durable_publish_operation_head(
			null_record ? NULL : (const xl_undo_tt_slot_set_head_operation *)&call->record,
			call->previous, call->pending_lsn, null_admission ? NULL : &call->admission);
#else
		/* The parent has no live publisher. Keep RED buildable. */
		(void)null_record;
		(void)null_admission;
#endif
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static void
live_head_resources_released(void)
{
	UT_ASSERT(!g_current_active && !g_pin_reserved);
	UT_ASSERT_EQ(g_writer_fd_open, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(g_fake_proc.delayChkptFlags, DELAY_CHKPT_COMPLETE);
}

static void
live_head_no_publication(void)
{
	UT_ASSERT_EQ(live_head_emit_calls, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(g_flushed_lsn, InvalidXLogRecPtr);
	live_head_resources_released();
}

static void
live_head_set_previous(TestLiveHeadCall *call, UBA previous)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;

	call->previous = previous;
	disk->tt_slots[7].first_undo_block = previous;
	UT_ASSERT(cluster_undo_segment_header_seal(disk));
	memcpy(g_current_resident, disk, BLCKSZ);
}

static void
live_head_change_caller_request(void)
{
	live_head_input->record.operation.operation_sequence++;
	live_head_input->record.head.first_undo_block = uba_encode(1, 19, 7, 0);
}

UT_TEST(test_live_head_target_orders_wal_flush_and_exact_slot)
{
	TestLiveHeadCall call = live_head_setup(true);
	TestUndoHeadOperation original = call.record;
	PGAlignedBlock expected;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)expected.data;

	memcpy(expected.data, g_canned_block, BLCKSZ);
	header->tt_slots[7].first_undo_block = original.head.first_undo_block;
	UT_ASSERT(cluster_undo_segment_header_seal(header));
	live_head_input = &call;
	g_before_current_acquire_hook = live_head_change_caller_request;
	UT_ASSERT(!live_head_call(&call, false, false));
	UT_ASSERT_EQ(call.result, (XLogRecPtr)0x2000);
	UT_ASSERT_EQ(live_head_emit_calls, 1);
	UT_ASSERT_EQ(memcmp(&live_head_emitted, &original, sizeof(original)), 0);
	UT_ASSERT_EQ(g_flushed_lsn, call.result);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT(!g_write_before_flush);
	UT_ASSERT((live_head_emit_flags & DELAY_CHKPT_START) != 0);
	UT_ASSERT(live_head_emit_critical > 0);
	UT_ASSERT((g_write_delay_flags & DELAY_CHKPT_START) != 0);
	UT_ASSERT_EQ(memcmp(expected.data, g_canned_block, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(expected.data, g_current_resident, BLCKSZ), 0);
	UT_ASSERT_EQ(g_current_target_acquire_calls, 1);
	UT_ASSERT_EQ(g_pin_downgrade_calls, 1);
	UT_ASSERT_EQ(g_lock_content_calls, 1);
	UT_ASSERT_EQ(g_current_unpin_calls, 1);
	UT_ASSERT_EQ(g_current_cancel_calls, 1);
	UT_ASSERT_EQ(g_writer_close_calls, 1);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_commit_0x30_emits, 0);
	live_head_resources_released();
}

UT_TEST(test_live_head_source_preserves_prior_retained_head)
{
	TestLiveHeadCall call = live_head_setup(false);
	TTSlot expected;

	live_head_set_previous(&call, uba_encode(1, 8, 7, 3));
	expected = ((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7];
	expected.first_undo_block = call.record.head.first_undo_block;
	UT_ASSERT(!live_head_call(&call, false, false));
	UT_ASSERT_EQ(call.result, live_head_emit_lsn);
	UT_ASSERT_EQ(memcmp(&expected, &g_last_written_slot, sizeof(expected)), 0);
	UT_ASSERT_EQ(g_current_target_acquire_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	assert_sealed_resident_matches_disk();
	live_head_resources_released();
}

UT_TEST(test_live_head_already_present_still_flushes_exact_wal)
{
	TestLiveHeadCall call = live_head_setup(false);
	PGAlignedBlock before;

	live_head_set_previous(&call, call.record.head.first_undo_block);
	memcpy(before.data, g_canned_block, BLCKSZ);
	UT_ASSERT(!live_head_call(&call, false, false));
	UT_ASSERT_EQ(call.result, live_head_emit_lsn);
	UT_ASSERT_EQ(live_head_emit_calls, 1);
	UT_ASSERT_EQ(g_flushed_lsn, live_head_emit_lsn);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
	live_head_resources_released();
}

static void
live_head_bad_request(TestLiveHeadCall *call, unsigned which)
{
	if (which == 0)
		call->record.head._pad[0] = 0;
	else if (which == 1)
		call->record.operation.length--;
	else if (which == 2)
		call->record.operation.canonical_top_fxid++;
	else if (which == 3)
		call->record.operation.canonical_segment_generation = UINT32_MAX;
	else if (which == 4)
		call->record.head.instance = 2;
	else if (which == 5)
		call->record.head.slot_offset = TT_SLOTS_PER_SEGMENT;
	else if (which == 6)
		call->pending_lsn = InvalidXLogRecPtr;
	else if (which == 7)
		call->admission.entered = false;
	else if (which == 8)
		call->admission.side = 99;
	else if (which == 9)
		MyProc = NULL;
	else if (which == 10)
		MyProc->lxid = InvalidLocalTransactionId;
	else if (which == 11)
		cluster_node_id = 1;
	else if (which == 12)
		cluster_shared_config = false;
	else if (which == 13)
		call->record.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
}

UT_TEST(test_live_head_rejects_invalid_requests_before_current)
{
	for (unsigned i = 0; i < 16; i++) {
		TestLiveHeadCall call = live_head_setup(false);

		cluster_shared_config = true;
		live_head_bad_request(&call, i);
		UT_ASSERT(live_head_call(&call, i == 14, i == 15));
		UT_ASSERT_EQ(call.result, InvalidXLogRecPtr);
		UT_ASSERT_EQ(last_ereport_elevel, ERROR);
		UT_ASSERT_EQ(g_current_acquire_calls, 0);
		cluster_node_id = 0;
		MyProc = &g_fake_proc;
		live_head_no_publication();
	}
}

UT_TEST(test_live_head_rejects_invalid_previous_head)
{
	UBA bad[] = { uba_encode(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 1, 7, 0),
				  uba_encode(1, 1, 8, 0),
				  uba_encode(1, 0, 7, 0),
				  { { UINT64_MAX, UINT64_MAX } } };

	for (unsigned i = 0; i < lengthof(bad); i++) {
		TestLiveHeadCall call = live_head_setup(false);

		call.previous = bad[i];
		UT_ASSERT(live_head_call(&call, false, false));
		UT_ASSERT_EQ(g_current_acquire_calls, 0);
		live_head_no_publication();
	}
}

static void
live_head_bad_authority(unsigned which)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;

	if (which == 0)
		g_current_root_ok = false;
	else if (which == 1)
		g_current_acquire_ok = false;
	else if (which == 2)
		g_current_sample_ok = false;
	else if (which == 3)
		g_current_generation.known = false;
	else if (which == 4)
		g_current_generation.value++;
	else if (which == 5)
		g_current_pin_ok = false;
	else if (which == 6)
		g_read_block_ok = false;
	else if (which == 7)
		disk->owner_instance++;
	else if (which == 8)
		resident->tt_slots[7].xid++;
	else if (which == 9)
		disk->tt_slots[7].status = TT_SLOT_ABORTED;
	else if (which == 10)
		disk->tt_slots[7].first_undo_block = uba_encode(1, 7, 7, 0);
	else if (which == 11)
		g_allocator_owner.xid++;
	else if (which == 12)
		disk->tt_slots[7].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	else if (which == 13)
		disk->tt_slots[7].commit_scn = scn_encode(1, 31);
	else if (which == 14)
		((PageHeader)disk)->pd_checksum++;
	if (which == 7 || which == 9 || which == 10 || which == 12 || which == 13) {
		UT_ASSERT(cluster_undo_segment_header_seal(disk));
		memcpy(resident, disk, BLCKSZ);
	}
	if (which == 8)
		UT_ASSERT(cluster_undo_segment_header_seal(resident));
}

UT_TEST(test_live_head_rejects_physical_or_current_uncertainty)
{
	for (unsigned i = 0; i < 15; i++) {
		TestLiveHeadCall call = live_head_setup(false);

		live_head_bad_authority(i);
		UT_ASSERT(live_head_call(&call, false, false));
		UT_ASSERT_EQ(last_ereport_elevel, ERROR);
		UT_ASSERT_EQ(call.result, InvalidXLogRecPtr);
		live_head_no_publication();
	}
}

static void
live_head_revoke_current(void)
{
	g_current_recheck_ok = false;
}

static void
live_head_revoke_vxid(void)
{
	MyProc->lxid++;
}

UT_TEST(test_live_head_rechecks_before_wal)
{
	TestLiveHeadCall call = live_head_setup(false);

	g_modifier_recheck_hook = live_head_revoke_current;
	UT_ASSERT(live_head_call(&call, false, false));
	UT_ASSERT(g_current_recheck_calls > 0);
	live_head_no_publication();
	call = live_head_setup(false);
	g_current_root_drift = true;
	UT_ASSERT(live_head_call(&call, false, false));
	live_head_no_publication();
	call = live_head_setup(false);
	g_before_current_acquire_hook = live_head_revoke_vxid;
	UT_ASSERT(live_head_call(&call, false, false));
	UT_ASSERT_EQ(last_ereport_elevel, ERROR);
	live_head_no_publication();
}

UT_TEST(test_live_head_open_failure_is_before_wal)
{
	TestLiveHeadCall call = live_head_setup(false);

	g_writer_open_ok = false;
	UT_ASSERT(live_head_call(&call, false, false));
	UT_ASSERT_EQ(g_writer_open_calls, 1);
	UT_ASSERT_EQ(last_ereport_elevel, ERROR);
	live_head_no_publication();
}

UT_TEST(test_live_head_invalid_wal_cut_panics_without_write)
{
	for (unsigned i = 0; i < 3; i++) {
		TestLiveHeadCall call = live_head_setup(false);

		live_head_emit_lsn = i == 0 ? InvalidXLogRecPtr : call.pending_lsn - (i == 2);
		UT_ASSERT(live_head_call(&call, false, false));
		UT_ASSERT_EQ(last_ereport_elevel, PANIC);
		UT_ASSERT_EQ(live_head_emit_calls, 1);
		UT_ASSERT_EQ(g_write_hdr_calls, 0);
		UT_ASSERT_EQ(call.result, InvalidXLogRecPtr);
		live_head_resources_released();
	}
}

UT_TEST(test_live_head_failed_flush_panics_without_write)
{
	for (unsigned i = 0; i < 2; i++) {
		TestLiveHeadCall call = live_head_setup(false);

		live_head_short_flush = i == 0;
		live_head_flush_error = i == 1;
		UT_ASSERT(live_head_call(&call, false, false));
		UT_ASSERT_EQ(last_ereport_elevel, PANIC);
		UT_ASSERT_EQ(g_write_hdr_calls, 0);
		UT_ASSERT_EQ(call.result, InvalidXLogRecPtr);
		live_head_resources_released();
	}
}

UT_TEST(test_live_head_post_flush_conflicts_never_return_success)
{
	for (unsigned i = 1; i <= 10; i++) {
		TestLiveHeadCall call = live_head_setup(false);

		live_head_after_flush = i;
		UT_ASSERT(live_head_call(&call, false, false));
		UT_ASSERT_EQ(last_ereport_elevel, PANIC);
		UT_ASSERT_EQ(call.result, InvalidXLogRecPtr);
		UT_ASSERT_EQ(g_write_hdr_calls, i == 4 ? 1 : 0);
		live_head_resources_released();
	}
}

UT_TEST(test_live_head_original_wait_and_cancel)
{
	TestLiveHeadCall call = live_head_setup(false);

	g_current_acquire_pending = 3;
	UT_ASSERT(!live_head_call(&call, false, false));
	UT_ASSERT_EQ(call.result, live_head_emit_lsn);
	UT_ASSERT_EQ(g_current_wait_calls, 2);
	live_head_resources_released();
	call = live_head_setup(false);
	g_current_acquire_pending = 3;
	g_process_interrupt_error = true;
	InterruptPending = true;
	UT_ASSERT(live_head_call(&call, false, false));
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(last_ereport_elevel, ERROR);
	InterruptPending = false;
	live_head_no_publication();
}

UT_TEST(test_live_head_rolled_away_binding_uses_physical_owner)
{
	TestLiveHeadCall call = live_head_setup(false);

	g_allocator_current_segment = 2;
	g_allocator_owner_ok = false;
	UT_ASSERT(!live_head_call(&call, false, false));
	UT_ASSERT_EQ(call.result, live_head_emit_lsn);
	UT_ASSERT_EQ(g_allocator_owner_calls, 0);
	assert_sealed_resident_matches_disk();
	live_head_resources_released();
	g_live_head_test_mode = false;
	g_live_head_flush_hook = NULL;
	cluster_shared_config = false;
}
