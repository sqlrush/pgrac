/* Native TT physical whole-page writer; original SMGR/cache and real files. */
#define UNDO_NATIVE_TT_READ_MAIN native_tt_read_original_main
int native_tt_read_original_main(void);
#include "test_cluster_undo_native_tt_read.c"

static void
write_successor(PGAlignedBlock *page, PGAlignedBlock *next)
{
	UndoSegmentHeaderDataV2 *header;

	*next = *page;
	header = (void *) next->data;
	header->tt_slots[0].status = TT_SLOT_COMMITTED;
	header->tt_slots[0].xid = 700;
	header->tt_slots[0].wrap = 3;
	header->tt_slots[0].original_full_xid = (UINT64CONST(2) << 32) | 700;
	header->tt_slots[0].commit_scn = 80;
	UT_ASSERT(cluster_undo_segment_header_seal(next->data));
}

static void
assert_disk(const char *path, const PGAlignedBlock *expected)
{
	PGAlignedBlock disk;
	int fd = open(path, O_RDONLY);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, disk.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(memcmp(disk.data, expected->data, BLCKSZ), 0);
}

UT_TEST(native_tt_write_preserves_full_page_and_syncs)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned before;

	reset_selected(&page, path); write_successor(&page, &next); before = fsync_calls;
	UT_ASSERT(cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
	UT_ASSERT_EQ(selected_acquires, 1); UT_ASSERT(selected_reads >= 3);
	UT_ASSERT_EQ(fsync_calls, before + 1); assert_disk(path, &next);
	finish_selected(path);
}

UT_TEST(native_tt_write_missing_control_refuses_before_open)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];

	reset_selected(&page, path); write_successor(&page, &next); selected_missing = true;
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
	UT_ASSERT_EQ(cached_fd, -1); assert_disk(path, &page); finish_selected(path);
}

UT_TEST(native_tt_write_bad_successor_never_mutates)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 3; variant++) {
		reset_selected(&page, path); write_successor(&page, &next);
		if (variant == 0) next.data[3100] ^= 1;
		if (variant == 1) ((PageHeader) next.data)->pd_checksum = 0;
		if (variant == 2) next.data[57] = 0;
		UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
		assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_tt_write_cached_fd_needs_exact_original_disk)
{
	PGAlignedBlock page, next, out;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 3; variant++) {
		reset_selected(&page, path); write_successor(&page, &next);
		UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
		selected_reads = 0;
		if (variant == 0) selected.creation_generation++;
		if (variant == 1) page.data[400] ^= 1;
		if (variant == 2) ((PageHeader) page.data)->pd_checksum = 0;
		if (variant != 0) {
			int fd = open(path, O_WRONLY);
			UT_ASSERT(fd >= 0); UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(close(fd), 0);
		}
		UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
		assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_tt_write_current_refusal_precedes_mutation)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned when;

	for (when = 1; when <= 3; when++) {
		reset_selected(&page, path); write_successor(&page, &next); selected_change_on = when;
		UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
		assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_tt_write_late_refusal_keeps_written_bytes)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];

	reset_selected(&page, path); write_successor(&page, &next); selected_change_on = 4;
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
	assert_disk(path, &next); finish_selected(path);
}

UT_TEST(native_tt_write_io_failure_keeps_original_debt)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 3; variant++) {
		reset_selected(&page, path); write_successor(&page, &next);
		pread_forced_error = variant == 0; pwrite_forced_error = variant == 1;
		fsync_fail_on_call = variant == 2 ? fsync_calls + 1 : 0;
		UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true));
		pread_forced_error = pwrite_forced_error = false; fsync_fail_on_call = 0;
		assert_disk(path, variant == 2 ? &next : &page); finish_selected(path);
	}
}

UT_TEST(native_tt_write_error_cleans_borrow_and_keeps_effects)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned when;

	for (when = 1; when <= 4; when++) {
		volatile bool caught = false;

		reset_selected(&page, path); write_successor(&page, &next); selected_throw_on = when;
		PG_TRY();
		{
			(void) cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, true);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught); assert_disk(path, when == 4 ? &next : &page); finish_selected(path);
	}
}

UT_TEST(native_tt_write_no_sync_and_guc_off_keep_selected_policy)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned before;

	reset_selected(&page, path); write_successor(&page, &next);
	cluster_enabled = false; before = fsync_calls;
	UT_ASSERT(cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, false));
	UT_ASSERT_EQ(selected_acquires, 1); UT_ASSERT_EQ(fsync_calls, before);
	assert_disk(path, &next); finish_selected(path);
}

UT_TEST(native_tt_write_arguments_data_and_legacy_are_separate)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int fd;

	reset_selected(&page, path); write_successor(&page, &next);
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 0, 1, 0, next.data, false));
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 257, 1, 0, next.data, false));
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 0, 0, next.data, false));
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, NULL, false));
	UT_ASSERT_EQ(selected_acquires, 0); assert_disk(path, &page);
	fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, 2 * BLCKSZ), 0); UT_ASSERT_EQ(close(fd), 0);
	selected_missing = true;
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 1, next.data, false));
	UT_ASSERT_EQ(selected_acquires, 1); assert_disk(path, &page);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
	UT_ASSERT(cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, false));
	UT_ASSERT_EQ(selected_acquires, 1); assert_disk(path, &next); finish_selected(path);
}

#ifndef UNDO_NATIVE_TT_WRITE_MAIN
#define UNDO_NATIVE_TT_WRITE_MAIN main
#endif
int UNDO_NATIVE_TT_WRITE_MAIN(void)
{
	char path[] = "/tmp/pgrac-native-tt-write-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(10);
	UT_RUN(native_tt_write_preserves_full_page_and_syncs);
	UT_RUN(native_tt_write_missing_control_refuses_before_open);
	UT_RUN(native_tt_write_bad_successor_never_mutates);
	UT_RUN(native_tt_write_cached_fd_needs_exact_original_disk);
	UT_RUN(native_tt_write_current_refusal_precedes_mutation);
	UT_RUN(native_tt_write_late_refusal_keeps_written_bytes);
	UT_RUN(native_tt_write_io_failure_keeps_original_debt);
	UT_RUN(native_tt_write_error_cleans_borrow_and_keeps_effects);
	UT_RUN(native_tt_write_no_sync_and_guc_off_keep_selected_policy);
	UT_RUN(native_tt_write_arguments_data_and_legacy_are_separate);
	UT_ASSERT_EQ(rmdir(publication_dir), 0);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
