/* Actual INIT/REUSE redo; existing selection and finite syscall providers. */
#define UNDO_NATIVE_TT_REDO_IDENTITY_MAIN birth_original_main
#include "test_cluster_undo_native_tt_redo_identity.c"

static void
birth_source(bool reuse, PGAlignedBlock *image)
{
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, image->data));
	if (reuse) ((UndoSegmentHeaderDataV2 *)image->data)->wrap_count = 9;
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(image->data, BLCKSZ, &selected));
	UT_ASSERT(cluster_undo_segment_header_seal(image->data));
}

static bool
birth_call(bool reuse, const PGAlignedBlock *image)
{
	if (reuse) {
		struct { xl_undo_segment_reuse header; uint8 image[BLCKSZ]; } wal = {0};
		wal.header.instance = 3; wal.header.segment_id = 513;
		wal.header.old_generation = 8; wal.header.new_generation = 9;
		memcpy(wal.image, image->data, BLCKSZ);
		return replay_v2_record(XLOG_UNDO_SEGMENT_REUSE, &wal, sizeof(wal));
	} else {
		struct { xl_cluster_undo_segment_init header; uint8 image[BLCKSZ]; } wal = {0};
		wal.header.instance = 3; wal.header.segment_id = 513;
		memcpy(wal.image, image->data, BLCKSZ);
		return replay_v2_record(XLOG_UNDO_SEGMENT_INIT, &wal, sizeof(wal));
	}
}

static void
birth_reset(bool reuse, PGAlignedBlock *image)
{
	selected_base(TT_SLOT_ABORTED); birth_source(reuse, image);
	if (reuse) {
		((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count = 8;
		UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
	} else memset(disk.data, 0, BLCKSZ);
	durable = disk;
}

UT_TEST(birth_retains_selected_identity_through_original_creation_io)
{
	unsigned reuse;
	for (reuse = 0; reuse < 2; reuse++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image);
		UT_ASSERT(birth_call(reuse, &image)); UT_ASSERT(io_protected);
		UT_ASSERT_EQ(acquired, 1); UT_ASSERT_EQ(released, 1); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(extends, 1); UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(dirsyncs, 1); UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(applies, 1);
		UT_ASSERT(memcmp(disk.data, image.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(disk.data, durable.data, BLCKSZ) == 0);
	}
}

UT_TEST(birth_source_must_match_selection_before_any_file_effect)
{
	unsigned reuse, variant;
	for (reuse = 0; reuse < 2; reuse++) for (variant = 0; variant < 5; variant++) {
		PGAlignedBlock image;
		selected_base(TT_SLOT_ABORTED); birth_source(reuse, &image);
		if (variant == 0) missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) selected.storage_uuid[0]++;
		if (variant == 4) drift_on = 1;
		UT_ASSERT(!birth_call(reuse, &image));
		UT_ASSERT_EQ(mkdirs + opens + extends + writes + syncs + applies + skips, 0);
		UT_ASSERT(held == NULL); UT_ASSERT_EQ(released, variant == 0 ? 0 : 1);
	}
}

UT_TEST(birth_nonempty_predecessor_must_match_independent_selection)
{
	unsigned reuse, variant;
	for (reuse = 0; reuse < 2; reuse++) for (variant = 0; variant < 4; variant++) {
		PGAlignedBlock image, before;
		ClusterRecoveryFileIdentityV2 wrong;
		selected_base(TT_SLOT_ABORTED); birth_source(reuse, &image);
		if (reuse) ((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count = 8;
		else memcpy(disk.data, image.data, BLCKSZ);
		wrong = selected;
		if (variant < 2) memset(disk.data + CLUSTER_TT_FILE_REFERENCE_OFFSET,
			0, CLUSTER_CLOG_FILE_REFERENCE_BYTES);
		if (variant == 0) {
			wrong.creation_generation++;
			UT_ASSERT(cluster_tt_file_reference_prepare_v2(disk.data, BLCKSZ, &wrong));
		}
		if (variant == 1) {
			wrong.registration_generation++;
			UT_ASSERT(cluster_tt_file_reference_prepare_v2(disk.data, BLCKSZ, &wrong));
		}
		if (variant == 2) ((UndoSegmentHeaderDataV2 *)disk.data)->tt_layout_version = 0;
		((PageHeader)disk.data)->pd_checksum = pg_checksum_page(disk.data, 0);
		if (variant == 3) short_read_number = 1;
		before = disk;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(extends + writes + syncs + applies + skips, 0);
		UT_ASSERT(memcmp(before.data, disk.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(released, 1);
	}
}

UT_TEST(birth_current_checks_cover_truncate_write_and_directory_sync)
{
	unsigned reuse, at;
	for (reuse = 0; reuse < 2; reuse++) for (at = 2; at <= 10; at++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image); drift_on = at;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT(io_protected);
		UT_ASSERT_EQ(applies + skips, 0); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(closes, opens);
		UT_ASSERT_EQ(writes, at <= 6 ? 0 : 1);
		UT_ASSERT_EQ(extends, at <= 5 ? 0 : 1);
	}
}

UT_TEST(birth_error_returns_original_fd_and_borrow_once)
{
	unsigned reuse, at;
	for (reuse = 0; reuse < 2; reuse++) for (at = 1; at <= 10; at++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image); throw_on = at;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(closes, opens);
		UT_ASSERT_EQ(applies + skips, 0);
	}
}

UT_TEST(birth_retains_original_equal_generation_and_stale_decisions)
{
	unsigned reuse, stale;
	for (reuse = 0; reuse < 2; reuse++) for (stale = 0; stale <= reuse; stale++) {
		PGAlignedBlock image;
		selected_base(TT_SLOT_ABORTED); birth_source(reuse, &image);
		memcpy(disk.data, image.data, BLCKSZ);
		if (stale) ((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count++;
		UT_ASSERT(cluster_undo_segment_header_seal(disk.data)); durable = disk;
		UT_ASSERT(birth_call(reuse, &image)); UT_ASSERT(io_protected); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(writes, stale ? 0 : 1); UT_ASSERT_EQ(extends, stale ? 0 : 1);
		UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(released, 1);
		UT_ASSERT_EQ(applies, stale ? 0 : 1); UT_ASSERT_EQ(skips, stale ? 1 : 0);
		UT_ASSERT_EQ(dirsyncs, stale ? 0 : 1);
	}
}

UT_TEST(birth_io_failure_never_reports_completion)
{
	unsigned reuse, variant;
	for (reuse = 0; reuse < 2; reuse++) for (variant = 0; variant < 4; variant++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image);
		fail_open = variant == 0; fail_write = variant == 1;
		fail_sync = variant == 2; corrupt_post_read = variant == 3;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT(io_protected); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(applies + skips, 0);
		UT_ASSERT_EQ(closes, opens - (fail_open ? 1 : 0));
		UT_ASSERT_EQ(writes, variant >= 2 ? 1 : 0);
	}
}

UT_TEST(birth_managed_mode_ignores_guc_and_unknown_cannot_create)
{
	unsigned reuse;
	for (reuse = 0; reuse < 2; reuse++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image); cluster_enabled = false;
		UT_ASSERT(birth_call(reuse, &image)); UT_ASSERT_EQ(acquired, 1); UT_ASSERT(io_protected);
		selected_base(TT_SLOT_ABORTED); birth_source(reuse, &image);
		selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; missing = true;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT_EQ(opens + mkdirs + writes, 0);
		birth_reset(reuse, &image); cluster_enabled = false;
		selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		UT_ASSERT(birth_call(reuse, &image)); UT_ASSERT_EQ(acquired + released, 0);
	}
}

UT_TEST(birth_failed_cleanup_retains_unreturned_duty)
{
	unsigned reuse, variant;
	for (reuse = 0; reuse < 2; reuse++) for (variant = 0; variant < 2; variant++) {
		PGAlignedBlock image;
		birth_reset(reuse, &image);
		fail_close = variant == 0; release_ok = variant == 0;
		UT_ASSERT(!birth_call(reuse, &image)); UT_ASSERT(held != NULL);
		UT_ASSERT_EQ(released, 0); UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
	}
}

int main(void)
{
	UT_PLAN(9);
	UT_RUN(birth_retains_selected_identity_through_original_creation_io);
	UT_RUN(birth_source_must_match_selection_before_any_file_effect);
	UT_RUN(birth_nonempty_predecessor_must_match_independent_selection);
	UT_RUN(birth_current_checks_cover_truncate_write_and_directory_sync);
	UT_RUN(birth_error_returns_original_fd_and_borrow_once);
	UT_RUN(birth_retains_original_equal_generation_and_stale_decisions);
	UT_RUN(birth_io_failure_never_reports_completion);
	UT_RUN(birth_managed_mode_ignores_guc_and_unknown_cannot_create);
	UT_RUN(birth_failed_cleanup_retains_unreturned_duty);
	UT_DONE(); return ut_failed_count != 0;
}
