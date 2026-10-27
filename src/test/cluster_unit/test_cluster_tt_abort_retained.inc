/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_abort_retained.inc
 *      Exact top abort preserves the original retained undo head.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_tt_abort_retained.inc
 *
 * NOTES
 *      Current, allocator and physical WAL/storage are fixture boundaries.
 *      This is a top terminal transition, not local rollback completion.
 *-------------------------------------------------------------------------
 */
static TTSlot
retained_abort_seed(uint32 head_segment)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;

	reset_current_write_mock();
	seed_current_exact_active(7, 100, 5);
	disk->tt_slots[7].first_undo_block = uba_encode(head_segment, 9, 7, 3);
	enable_sealed_header_io();
	g_require_abort_flush_before_write = true;
	g_fsync_segment_calls = g_present_durable_calls = 0;
	return disk->tt_slots[7];
}

static bool
retained_abort_runtime(ClusterSemanticAdmissionToken *admission, TTSlot *out)
{
	volatile bool success = false;

	PG_TRY();
	{
		XLogRecPtr cut = cluster_tt_slot_durable_abort_exact(1, 4, 7, 100, 5, admission, out);

		UT_ASSERT_EQ(cut, g_abort_exact_lsn);
		success = true;
	}
	PG_CATCH();
	{}
	PG_END_TRY();
	return success;
}

static bool
retained_abort_redo(void)
{
	volatile bool success = false;

	PG_TRY();
	{
		cluster_tt_durable_redo_abort_slot_exact(1, 1, 4, 7, 5, 100);
		success = true;
	}
	PG_CATCH();
	{}
	PG_END_TRY();
	return success;
}

UT_TEST(test_retained_abort_runtime_preserves_original_head)
{
	for (unsigned i = 0; i < 3; i++) {
		ClusterSemanticAdmissionToken admission
			= i == 0 ? source_modifier_token() : target_modifier_token();
		UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
		TTSlot expected = retained_abort_seed(i == 0 ? 1 : 2);
		TTSlot out = { 0 };

		if (i == 2) {
			g_allocator_current_segment = 2;
			g_allocator_owner_ok = false;
		}
		expected.status = TT_SLOT_ABORTED;
		UT_ASSERT(retained_abort_runtime(&admission, &out));
		UT_ASSERT_EQ(memcmp(&out, &expected, sizeof(out)), 0);
		UT_ASSERT_EQ(memcmp(&resident->tt_slots[7], &expected, sizeof(expected)), 0);
		UT_ASSERT_EQ(memcmp(&g_last_written_slot, &expected, sizeof(expected)), 0);
		UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
		UT_ASSERT(g_abort_flush_seen);
		UT_ASSERT_EQ(g_current_acquire_calls, 2);
		UT_ASSERT_EQ(g_write_hdr_calls, 1);
		UT_ASSERT_EQ(g_current_cancel_calls, 2);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(MyProc->delayChkptFlags & DELAY_CHKPT_START, 0);
		assert_sealed_resident_matches_disk();
	}
}

static void retained_abort_store(const TTSlot *slot);

UT_TEST(test_retained_abort_redo_and_preflight_are_exact_and_idempotent)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
	TTSlot expected = retained_abort_seed(2);

	expected.status = TT_SLOT_ABORTED;
	g_require_abort_flush_before_write = false;
	UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
				 CLUSTER_TT_TERMINAL_APPLY);
	UT_ASSERT(retained_abort_redo());
	UT_ASSERT_EQ(memcmp(&disk->tt_slots[7], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_fsync_segment_calls, 1);
	UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
				 CLUSTER_TT_TERMINAL_IDEMPOTENT);
	UT_ASSERT(retained_abort_redo());
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ(g_fsync_segment_calls, 1);
	UT_ASSERT_EQ(g_present_durable_calls, 1);
	UT_ASSERT_EQ(memcmp(&disk->tt_slots[7], &expected, sizeof(expected)), 0);
	/* Replaying the older abort invalidates a later release marker, just
	 * as for nil heads; it must not destroy the retained chain. */
	expected.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	retained_abort_store(&expected);
	UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
				 CLUSTER_TT_TERMINAL_APPLY);
	UT_ASSERT(retained_abort_redo());
	expected.flags = TT_FLAGS_RESERVED;
	UT_ASSERT_EQ(memcmp(&disk->tt_slots[7], &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 2);
	UT_ASSERT_EQ(g_fsync_segment_calls, 2);
}

static void
retained_abort_store(const TTSlot *slot)
{
	((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7] = *slot;
	enable_sealed_header_io();
}

/* Each bad address must be rejected by the runtime and both recovery legs,
 * without clearing the only persistent chain head as a side effect. */
UT_TEST(test_retained_abort_rejects_bad_head_namespace)
{
	for (unsigned i = 0; i < 8; i++) {
		ClusterSemanticAdmissionToken admission = target_modifier_token();
		TTSlot before = retained_abort_seed(2);
		TTSlot out = { 0 };

		switch (i) {
		case 0:
			before.first_undo_block = uba_encode(2, 9, 8, 3);
			break;
		case 1:
			before.first_undo_block = uba_encode(257, 9, 7, 3);
			break;
		case 2:
			before.first_undo_block = uba_encode(2, 0, 7, 3);
			break;
		case 3:
			before.first_undo_block = uba_encode(2, UNDO_BLOCKS_PER_SEGMENT, 7, 3);
			break;
		case 4:
			before.first_undo_block.raw[1] |= UINT64CONST(1) << 63;
			break;
		case 5:
			before.first_undo_block = uba_encode(2, 9, 7, UINT16_MAX);
			break;
		case 6:
			before.first_undo_block.raw[0] &= ~UINT64CONST(0xffffffff);
			break;
		case 7:
			before.first_undo_block.raw[1] = TT_SLOTS_PER_SEGMENT;
			break;
		}
		retained_abort_store(&before);
		UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
					 CLUSTER_TT_TERMINAL_CORRUPT);
		UT_ASSERT(!retained_abort_runtime(&admission, &out));
		UT_ASSERT(!retained_abort_redo());
		UT_ASSERT_EQ(last_ereport_elevel, PANIC);
		UT_ASSERT_EQ(g_abort_exact_emit_calls + g_write_hdr_calls, 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before,
							sizeof(before)),
					 0);
	}
}

UT_TEST(test_retained_abort_rejects_wrong_identity_state_or_flags)
{
	for (unsigned i = 0; i < 7; i++) {
		ClusterSemanticAdmissionToken admission = source_modifier_token();
		TTSlot before = retained_abort_seed(1);
		TTSlot out = { 0 };

		switch (i) {
		case 0:
			before.xid++;
			break;
		case 1:
			before.wrap--;
			break;
		case 2:
			before.commit_scn = scn_encode(1, 42);
			break;
		case 3:
			before.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			break;
		case 4:
			before.flags = 0x80;
			break;
		case 5:
			before.status = TT_SLOT_COMMITTED;
			before.commit_scn = scn_encode(1, 42);
			break;
		case 6:
			before.status = TT_SLOT_ABORTED;
			before.commit_scn = scn_encode(1, 42);
			break;
		}
		retained_abort_store(&before);
		UT_ASSERT(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100)
				  != CLUSTER_TT_TERMINAL_APPLY);
		UT_ASSERT(!retained_abort_runtime(&admission, &out));
		UT_ASSERT(!retained_abort_redo());
		UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before,
							sizeof(before)),
					 0);
	}
}

UT_TEST(test_retained_abort_keeps_original_current_authority_checks)
{
	for (unsigned i = 0; i < 6; i++) {
		ClusterSemanticAdmissionToken admission = target_modifier_token();
		TTSlot before = retained_abort_seed(2);
		TTSlot out = { 0 };

		switch (i) {
		case 0:
			g_current_root_ok = false;
			break;
		case 1:
			g_current_acquire_ok = false;
			break;
		case 2:
			g_current_sample_ok = false;
			break;
		case 3:
			g_current_pin_ok = false;
			break;
		case 4:
			g_current_recheck_ok = false;
			break;
		case 5:
			((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7].first_undo_block
				= uba_encode(2, 10, 7, 3);
			break;
		}
		UT_ASSERT(!retained_abort_runtime(&admission, &out));
		UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before,
							sizeof(before)),
					 0);
	}
}

UT_TEST(test_retained_abort_does_not_authorize_commit_or_rebind)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot before = retained_abort_seed(2);
	TTSlot out = { 0 };
	volatile bool caught = false;

	UT_ASSERT_EQ(cluster_tt_terminal_transition_decide(&before, 4, 4, 100, 5, TT_SLOT_COMMITTED,
													   scn_encode(1, 42)),
				 CLUSTER_TT_TERMINAL_CORRUPT);
	PG_TRY();
	{
		(void)cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42), &admission,
												   &out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
	before.status = TT_SLOT_ABORTED;
	retained_abort_store(&before);
	UT_ASSERT_EQ(cluster_tt_durable_bind_preflight_exact(1, 1, 4, 7, 6, 101),
				 CLUSTER_TT_ACTIVE_CORRUPT);
	UT_ASSERT_EQ(
		cluster_tt_terminal_transition_decide(&before, 4, 4, 100, 5, TT_SLOT_ABORTED, InvalidScn),
		CLUSTER_TT_TERMINAL_CORRUPT);
}

static void
retained_abort_change_after_read(void)
{
	((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7].first_undo_block
		= uba_encode(2, 10, 7, 3);
	UT_ASSERT(cluster_undo_segment_header_seal(g_canned_block));
	g_retained_abort_after_read = NULL;
}

UT_TEST(test_retained_abort_preflight_refuses_unstable_or_missing_header)
{
	(void)retained_abort_seed(2);
	g_retained_abort_after_read = retained_abort_change_after_read;
	UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
				 CLUSTER_TT_TERMINAL_CONFLICT);
	UT_ASSERT_EQ(g_read_block_calls, 2);
	g_read_block_ok = false;
	UT_ASSERT_EQ(cluster_tt_durable_abort_preflight_exact(1, 1, 4, 7, 5, 100),
				 CLUSTER_TT_TERMINAL_CONFLICT);
	UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
}

UT_TEST(test_retained_abort_write_failure_keeps_original_head)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot before = retained_abort_seed(2);
	TTSlot out = { 0 };

	g_write_hdr_ok = false;
	UT_ASSERT(!retained_abort_runtime(&admission, &out));
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
	UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(
		memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before, sizeof(before)),
		0);
	UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7], &before,
						sizeof(before)),
				 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags & DELAY_CHKPT_START, 0);
}
