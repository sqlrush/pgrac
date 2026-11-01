/* Actual native recycle redo with selected-file and finite I/O providers. */
#define UNDO_NATIVE_TT_REDO_IDENTITY_MAIN recycle_original_main
#include "test_cluster_undo_native_tt_redo_identity.c"

static bool selected_recycle(void)
{
	xl_undo_segment_recycle rec = {0};

	rec.instance = 3;
	rec.segment_id = 513;
	rec.expected_generation = 9;
	rec.old_state = SEGMENT_COMMITTED;
	rec.new_state = SEGMENT_RECYCLABLE;
	return replay_v2_record(XLOG_UNDO_SEGMENT_RECYCLE, &rec, sizeof(rec));
}

UT_TEST(recycle_holds_selected_identity_through_original_io)
{
	PGAlignedBlock before;

	selected_base(TT_SLOT_ABORTED); before = disk;
	UT_ASSERT(selected_recycle()); UT_ASSERT(io_protected);
	UT_ASSERT_EQ(acquired, 1); UT_ASSERT_EQ(released, 1); UT_ASSERT(held == NULL);
	UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(applies, 1); UT_ASSERT_EQ(skips, 0);
	UT_ASSERT_EQ(((UndoSegmentHeaderDataV2 *)disk.data)->segment_state, SEGMENT_RECYCLABLE);
	UT_ASSERT(memcmp(((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots,
		((UndoSegmentHeaderDataV2 *)before.data)->tt_slots,
		sizeof(((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots)) == 0);
	UT_ASSERT(cluster_tt_file_reference_matches_v2(disk.data, BLCKSZ, &selected));
	UT_ASSERT(memcmp(disk.data, durable.data, BLCKSZ) == 0);
}

UT_TEST(recycle_refuses_missing_and_wrong_selected_predecessor)
{
	unsigned variant;
	for (variant = 0; variant < 5; variant++) {
		PGAlignedBlock before;

		selected_base(TT_SLOT_ABORTED);
		if (variant == 0) missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) drift_on = 1;
		if (variant == 4) {
			((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots[4].original_full_xid ^= UINT64_C(1) << 32;
		}
		before = disk;
		UT_ASSERT(!selected_recycle()); UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		UT_ASSERT(memcmp(before.data, disk.data, BLCKSZ) == 0);
		UT_ASSERT(held == NULL); UT_ASSERT_EQ(released, variant == 0 ? 0 : 1);
		if (variant == 0 || variant == 3) UT_ASSERT_EQ(opens, 0);
	}
}

UT_TEST(recycle_current_checks_surround_preparation_write_and_sync)
{
	unsigned at;
	for (at = 2; at <= 7; at++) {
		selected_base(TT_SLOT_ABORTED); drift_on = at;
		UT_ASSERT(!selected_recycle()); UT_ASSERT(io_protected);
		UT_ASSERT_EQ(released, 1); UT_ASSERT(held == NULL); UT_ASSERT_EQ(closes, 1);
		UT_ASSERT_EQ(applies + skips, 0);
		UT_ASSERT_EQ(writes, at <= 4 ? 0 : 1);
	}
}

UT_TEST(recycle_error_closes_only_its_fd_and_returns_borrow_once)
{
	unsigned at;
	for (at = 1; at <= 7; at++) {
		selected_base(TT_SLOT_ABORTED); throw_on = at;
		UT_ASSERT(!selected_recycle()); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(closes, opens);
		UT_ASSERT_EQ(applies + skips, 0);
	}
}

UT_TEST(recycle_repeated_and_stale_keep_original_durability)
{
	unsigned stale;
	for (stale = 0; stale < 2; stale++) {
		selected_base(TT_SLOT_ABORTED);
		if (stale) ((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count++;
		else ((UndoSegmentHeaderDataV2 *)disk.data)->segment_state = SEGMENT_RECYCLABLE;
		UT_ASSERT(cluster_undo_segment_header_seal(disk.data)); durable = disk;
		UT_ASSERT(selected_recycle()); UT_ASSERT(io_protected); UT_ASSERT(held == NULL);
		/* The original table reapplies an equal-generation lifecycle state. */
		UT_ASSERT_EQ(writes, stale ? 0 : 1); UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(applies, stale ? 0 : 1); UT_ASSERT_EQ(skips, stale ? 1 : 0);
		UT_ASSERT_EQ(released, 1);
	}
}

UT_TEST(recycle_io_failure_preserves_effects_without_completion)
{
	unsigned variant;
	for (variant = 0; variant < 5; variant++) {
		selected_base(TT_SLOT_ABORTED);
		fail_open = variant == 0; short_read_number = variant == 1 ? 1 : 0;
		fail_write = variant == 2; fail_sync = variant == 3; corrupt_post_read = variant == 4;
		UT_ASSERT(!selected_recycle()); UT_ASSERT(held == NULL); UT_ASSERT(io_protected);
		UT_ASSERT_EQ(applies + skips, 0); UT_ASSERT_EQ(released, 1);
		UT_ASSERT_EQ(writes, variant >= 3 ? 1 : 0);
	}
}

UT_TEST(recycle_mode_is_independent_of_guc_and_rejects_tt32)
{
	selected_base(TT_SLOT_ABORTED); cluster_enabled = false;
	UT_ASSERT(selected_recycle()); UT_ASSERT(io_protected); UT_ASSERT_EQ(acquired, 1);
	selected_base(TT_SLOT_ABORTED); selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	missing = true; UT_ASSERT(!selected_recycle()); UT_ASSERT_EQ(opens, 0);
	selected_base(TT_SLOT_ABORTED);
	((UndoSegmentHeaderDataV2 *)disk.data)->tt_layout_version = 0;
	UT_ASSERT(!selected_recycle()); UT_ASSERT_EQ(writes + syncs, 0); UT_ASSERT(held == NULL);
	selected_base(TT_SLOT_ABORTED); cluster_enabled = false;
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	UT_ASSERT(selected_recycle()); UT_ASSERT_EQ(acquired + released, 0);
}

UT_TEST(recycle_cleanup_failure_keeps_original_borrow_duty)
{
	unsigned variant;
	for (variant = 0; variant < 2; variant++) {
		selected_base(TT_SLOT_ABORTED);
		fail_close = variant == 0; release_ok = variant == 0;
		UT_ASSERT(!selected_recycle()); UT_ASSERT(held != NULL); UT_ASSERT_EQ(released, 0);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
	}
}

int main(void)
{
	UT_PLAN(8);
	UT_RUN(recycle_holds_selected_identity_through_original_io);
	UT_RUN(recycle_refuses_missing_and_wrong_selected_predecessor);
	UT_RUN(recycle_current_checks_surround_preparation_write_and_sync);
	UT_RUN(recycle_error_closes_only_its_fd_and_returns_borrow_once);
	UT_RUN(recycle_repeated_and_stale_keep_original_durability);
	UT_RUN(recycle_io_failure_preserves_effects_without_completion);
	UT_RUN(recycle_mode_is_independent_of_guc_and_rejects_tt32);
	UT_RUN(recycle_cleanup_failure_keeps_original_borrow_duty);
	UT_DONE(); return ut_failed_count != 0;
}
