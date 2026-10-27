/*-------------------------------------------------------------------------
 * test_cluster_tt_commit_retained.inc
 *      Ordinary exact commit preserves its retained undo chain.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_tt_commit_retained.inc
 * NOTES
 *      Executes the real stage/apply owner with physical current/WAL/I/O
 *      boundaries supplied by the enclosing fixture.
 *-------------------------------------------------------------------------
 */
static TTSlot
retained_commit_seed(void)
{
	TTSlot slot = retained_abort_seed(2);

	g_require_abort_flush_before_write = false;
	return slot;
}

static bool
retained_commit_stage(ClusterSemanticAdmissionToken *admission, TTSlot *out)
{
	volatile bool success = false;

	PG_TRY();
	{
		uint8 owner = cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42),
														   admission, out);

		UT_ASSERT_EQ(owner, 1);
		success = true;
	}
	PG_CATCH();
	{}
	PG_END_TRY();
	return success;
}

UT_TEST(test_retained_commit_stages_before_flush_and_preserves_head)
{
	for (unsigned i = 0; i < 3; i++) {
		ClusterSemanticAdmissionToken admission
			= i == 0 ? source_modifier_token() : target_modifier_token();
		TTSlot before = retained_commit_seed();
		TTSlot expected = before;
		TTSlot out = { 0 };

		if (i == 2) {
			g_allocator_current_segment = 2;
			g_allocator_owner_ok = false;
		}
		expected.status = TT_SLOT_COMMITTED;
		expected.commit_scn = scn_encode(1, 42);
		if (!retained_commit_stage(&admission, &out)) {
			UT_ASSERT(false);
			return;
		}
		UT_ASSERT_EQ(memcmp(&out, &expected, sizeof(out)), 0);
		UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before,
							sizeof(before)),
					 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7], &before,
							sizeof(before)),
					 0);
		apply_after_flush((XLogRecPtr)0x5000);
		UT_ASSERT_EQ(memcmp(&g_last_written_slot, &expected, sizeof(expected)), 0);
		UT_ASSERT_EQ(g_write_hdr_calls, 1);
		UT_ASSERT(!g_write_before_flush);
		UT_ASSERT_EQ(g_current_acquire_calls, 1);
		UT_ASSERT_EQ(g_current_cancel_calls, 1);
		UT_ASSERT_EQ(g_current_unpin_calls, 1);
		UT_ASSERT_EQ(g_writer_fd_open, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		assert_sealed_resident_matches_disk();
	}
}

UT_TEST(test_retained_commit_unstage_keeps_original_abort_obligation)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot before = retained_commit_seed();
	TTSlot out = { 0 };

	if (!retained_commit_stage(&admission, &out)) {
		UT_ASSERT(false);
		return;
	}
	cluster_tt_slot_durable_commit_unstage();
	UT_ASSERT_EQ(g_reservation_release_calls, 1);
	UT_ASSERT_EQ(g_writer_fd_open, 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7], &before,
						sizeof(before)),
				 0);
	UT_ASSERT(retained_abort_runtime(&admission, &out));
	before.status = TT_SLOT_ABORTED;
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
}

UT_TEST(test_retained_commit_rejects_invalid_head_and_predecessor)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterSemanticAdmissionToken admission = target_modifier_token();
		TTSlot before = retained_commit_seed();
		TTSlot out = { 0 };

		switch (fault) {
		case 0:
			before.first_undo_block = uba_encode(257, 9, 7, 3);
			break;
		case 1:
			before.first_undo_block = uba_encode(2, 0, 7, 3);
			break;
		case 2:
			before.first_undo_block = uba_encode(2, 9, 8, 3);
			break;
		case 3:
			before.first_undo_block.raw[1] |= UINT64_C(1) << 32;
			break;
		case 4:
			before.first_undo_block = uba_encode(2, UNDO_BLOCKS_PER_SEGMENT, 7, 3);
			break;
		case 5:
			before.xid++;
			break;
		case 6:
			before.wrap--;
			break;
		case 7:
			before.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			break;
		case 8:
			before.flags = 0x80;
			break;
		case 9:
			before.commit_scn = scn_encode(1, 42);
			break;
		case 10:
			before.status = TT_SLOT_ABORTED;
			break;
		case 11:
			before.status = TT_SLOT_RECYCLABLE;
			break;
		}
		retained_abort_store(&before);
		UT_ASSERT(!retained_commit_stage(&admission, &out));
		UT_ASSERT(!cluster_tt_slot_durable_commit_staged(1, 7, 100, 5));
		UT_ASSERT_EQ(g_write_hdr_calls + g_abort_exact_emit_calls, 0);
		UT_ASSERT_EQ(g_writer_fd_open, 0);
		UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before,
							sizeof(before)),
					 0);
	}
}

UT_TEST(test_retained_commit_post_stage_head_drift_cannot_publish)
{
	ClusterSemanticAdmissionToken admission = source_modifier_token();
	TTSlot before = retained_commit_seed();
	TTSlot out = { 0 };
	volatile bool caught = false;

	if (!retained_commit_stage(&admission, &out)) {
		UT_ASSERT(false);
		return;
	}
	((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7].first_undo_block
		= uba_encode(2, 10, 7, 3);
	PG_TRY();
	{
		apply_after_flush((XLogRecPtr)0x5000);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(
		memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before, sizeof(before)),
		0);
	cluster_tt_slot_durable_commit_unstage();
}

UT_TEST(test_retained_commit_write_failure_preserves_unfinished_stage)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot before = retained_commit_seed();
	TTSlot out = { 0 };
	volatile bool caught = false;

	if (!retained_commit_stage(&admission, &out)) {
		UT_ASSERT(false);
		return;
	}
	g_write_hdr_ok = false;
	PG_TRY();
	{
		apply_after_flush((XLogRecPtr)0x5000);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(cluster_tt_slot_durable_commit_staged(1, 7, 100, 5));
	UT_ASSERT_EQ(memcmp(&((UndoSegmentHeaderData *)g_current_resident)->tt_slots[7], &before,
						sizeof(before)),
				 0);
	UT_ASSERT_EQ(
		memcmp(&((UndoSegmentHeaderData *)g_canned_block)->tt_slots[7], &before, sizeof(before)),
		0);
	cluster_tt_slot_durable_commit_unstage();
}
