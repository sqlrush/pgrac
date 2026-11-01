/* Actual noncritical header I/O and checkpoint dispatch with real files.
 * The selected control table, checkpoint recorder and ERROR schedule are finite. */
#define cluster_undo_block0_note_unsynced_header header_test_note_unsynced
#define UNDO_NATIVE_TT_HEADER_RECORDER_TEST
#define UNDO_NATIVE_TT_WRITE_MAIN native_tt_write_original_main
int native_tt_write_original_main(void);
#include "test_cluster_undo_native_tt_write.c"
#undef cluster_undo_block0_note_unsynced_header

static bool checkpoint_accept, checkpoint_throw, expect_identity = true;
static bool checkpoint_custody_ok;
static unsigned checkpoint_calls;

bool header_test_note_unsynced(uint32 segment, uint8 owner, ClusterUndoPathIntent intent)
{
	if (segment != 1 || owner != 1 || intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED)
		abort();
	if (expect_identity && selected_borrows != 1) checkpoint_custody_ok = false;
	checkpoint_calls++;
	if (checkpoint_throw) siglongjmp(*PG_exception_stack, 1);
	return checkpoint_accept;
}

/* This fixture has no recovery scope; keep the original resolver input finite. */
ClusterUndoPathIntent cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	if (owner != 1) abort();
	return CLUSTER_UNDO_PATH_RUNTIME_SHARED;
}

static void header_reset(PGAlignedBlock *page, char path[MAXPGPATH])
{
	reset_selected(page, path);
	checkpoint_accept = checkpoint_throw = false; checkpoint_calls = 0;
	checkpoint_custody_ok = true;
	expect_identity = true; pwrite_forced_error = false; fsync_fail_on_call = 0;
}

static bool header_sync(unsigned kind)
{
	if (kind == 0)
		return cluster_undo_smgr_header_unchanged_durable(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1);
	if (kind == 1)
		return cluster_undo_smgr_fsync_header(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1);
	return cluster_undo_smgr_fsync_segment_file(1, 1);
}

UT_TEST(native_header_range_reads_validate_whole_page)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH], out[40];
	unsigned offset;

	header_reset(&page, path); write_successor(&page, &next);
	remove_if_present(path); write_segment_file(path, next.data, BLCKSZ);
	for (offset = 0; offset < BLCKSZ - sizeof(out); offset += 113) {
		UT_ASSERT(cluster_undo_smgr_read_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, offset, out, sizeof(out)));
		UT_ASSERT_EQ(memcmp(out, next.data + offset, sizeof(out)), 0);
	}
	UT_ASSERT_EQ(selected_borrows, 0);
	UT_ASSERT(selected_acquires > 1); finish_selected(path);
}

UT_TEST(native_header_range_refusal_keeps_caller_bytes)
{
	PGAlignedBlock page;
	char path[MAXPGPATH], out[40], before[40];
	unsigned variant;

	for (variant = 0; variant < 6; variant++) {
		header_reset(&page, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) {
			int fd;
			page.data[8000] ^= 1; fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, 0), BLCKSZ); UT_ASSERT_EQ(close(fd), 0);
		}
		if (variant == 3) pread_forced_error = true;
		if (variant == 4) UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
		if (variant == 5) selected_change_on = 3;
		memset(out, 0x5a, sizeof(out)); memcpy(before, out, sizeof(out));
		UT_ASSERT(!cluster_undo_smgr_read_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, 128, out, sizeof(out)));
		UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0); finish_selected(path);
	}
}

UT_TEST(native_header_complete_write_keeps_original_checkpoint_duty)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant, before;

	for (variant = 0; variant < 2; variant++) {
		header_reset(&page, path); write_successor(&page, &next);
		checkpoint_accept = variant == 0; before = fsync_calls;
		UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, 0, next.data, BLCKSZ));
		UT_ASSERT_EQ(checkpoint_calls, 1); UT_ASSERT_EQ(selected_acquires, 1);
		UT_ASSERT(checkpoint_custody_ok);
		UT_ASSERT_EQ(fsync_calls, before + (variant == 1));
		assert_disk(path, &next); finish_selected(path);
	}
}

UT_TEST(native_header_partial_write_refuses_before_lookup)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 3; variant++) {
		header_reset(&page, path);
		UT_ASSERT(!cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, variant == 0 ? 0 : variant == 1 ? 128 : 1,
			page.data, variant == 0 ? 40 : variant == 1 ? 32 : BLCKSZ - 1));
		UT_ASSERT_EQ(selected_acquires, 0); UT_ASSERT_EQ(checkpoint_calls, 0);
		assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_header_write_checks_source_and_successor)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 5; variant++) {
		header_reset(&page, path); write_successor(&page, &next);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.registration_generation++;
		if (variant == 2) next.data[128] ^= 1;
		if (variant == 3) selected_change_on = 3;
		if (variant == 4) pwrite_forced_error = true;
		UT_ASSERT(!cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, 0, next.data, BLCKSZ));
		UT_ASSERT_EQ(checkpoint_calls, 0); assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_header_postwrite_refusal_preserves_effects)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 2; variant++) {
		header_reset(&page, path); write_successor(&page, &next);
		if (variant == 0) fsync_fail_on_call = fsync_calls + 1;
		if (variant == 1) { checkpoint_accept = true; selected_change_on = 4; }
		UT_ASSERT(!cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, 0, next.data, BLCKSZ));
		UT_ASSERT_EQ(checkpoint_calls, 1); assert_disk(path, &next); finish_selected(path);
	}
}

UT_TEST(native_header_sync_validates_selected_file_before_sync_or_note)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned kind, variant, before;

	for (kind = 0; kind < 3; kind++) for (variant = 0; variant < 5; variant++) {
		header_reset(&page, path); before = fsync_calls;
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.redo_branch_generation++;
		if (variant == 2) selected_change_on = 3;
		if (variant == 3) pread_forced_error = true;
		if (variant == 4) UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
		UT_ASSERT(!header_sync(kind));
		UT_ASSERT_EQ(checkpoint_calls, 0); UT_ASSERT_EQ(fsync_calls, before);
		finish_selected(path);
	}
}

UT_TEST(native_header_sync_preserves_original_record_or_samefd_fsync)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned kind, variant, before;

	for (kind = 0; kind < 3; kind++) for (variant = 0; variant < 2; variant++) {
		header_reset(&page, path); checkpoint_accept = variant == 0; before = fsync_calls;
		UT_ASSERT(header_sync(kind));
		UT_ASSERT_EQ(checkpoint_calls, kind == 0 ? 1 : 0);
		UT_ASSERT(checkpoint_custody_ok);
		UT_ASSERT_EQ(fsync_calls, before + (kind != 0 || !checkpoint_accept));
		UT_ASSERT_EQ(selected_acquires, 1); assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_header_sync_failure_keeps_file_and_owner_debt)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned kind, variant, before;

	for (kind = 0; kind < 3; kind++) for (variant = 0; variant < 2; variant++) {
		header_reset(&page, path); before = fsync_calls;
		if (variant == 0) fsync_fail_on_call = fsync_calls + 1;
		if (variant == 1) selected_change_on = 4;
		UT_ASSERT(!header_sync(kind));
		UT_ASSERT_EQ(fsync_calls, before + 1); assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(native_header_error_cleanup_keeps_bytes_and_releases_only_borrow)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned op;

	for (op = 0; op < 5; op++) {
		volatile bool caught = false;

		header_reset(&page, path); write_successor(&page, &next);
		if (op < 2) checkpoint_throw = true;
		else selected_throw_on = 4;
		PG_TRY(); {
			if (op == 0) (void) cluster_undo_smgr_write_header_bytes(
				CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next.data, BLCKSZ);
			else (void) header_sync(op == 1 ? 0 : op - 2);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); assert_disk(path, op == 0 ? &next : &page); finish_selected(path);
	}
}

UT_TEST(native_header_modes_and_argument_bounds_do_not_create_authority)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH], out[40];
	unsigned variant;

	for (variant = 0; variant < 2; variant++) {
		header_reset(&page, path); write_successor(&page, &next); cluster_enabled = false;
		if (variant == 1) { cluster_enabled = true; selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; }
		selected_missing = true;
		UT_ASSERT(!cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			1, 1, 0, next.data, BLCKSZ));
		UT_ASSERT(!header_sync(1)); UT_ASSERT_EQ(selected_acquires, 2);
		finish_selected(path);
	}
	header_reset(&page, path);
	UT_ASSERT(!cluster_undo_smgr_read_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, UINT32_MAX, out, 40));
	UT_ASSERT(!cluster_undo_smgr_read_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, NULL, 40));
	UT_ASSERT(!cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, page.data, 0));
	UT_ASSERT_EQ(selected_acquires, 0); finish_selected(path);
}

UT_TEST(native_header_legacy_range_policy_is_unchanged)
{
	PGAlignedBlock page;
	char path[MAXPGPATH], out[40];
	unsigned before;

	header_reset(&page, path); cluster_enabled = false;
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; selected_missing = true;
	expect_identity = false; checkpoint_accept = true; before = fsync_calls;
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 128, page.data, 40));
	UT_ASSERT(cluster_undo_smgr_read_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 128, out, 40));
	UT_ASSERT_EQ(memcmp(out, page.data, 40), 0); UT_ASSERT(header_sync(0));
	UT_ASSERT_EQ(fsync_calls, before); UT_ASSERT(header_sync(1));
	UT_ASSERT_EQ(selected_acquires, 0); UT_ASSERT_EQ(fsync_calls, before + 1); finish_selected(path);
}

int main(void)
{
	char path[] = "/tmp/pgrac-native-tt-header-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(12);
	UT_RUN(native_header_range_reads_validate_whole_page);
	UT_RUN(native_header_range_refusal_keeps_caller_bytes);
	UT_RUN(native_header_complete_write_keeps_original_checkpoint_duty);
	UT_RUN(native_header_partial_write_refuses_before_lookup);
	UT_RUN(native_header_write_checks_source_and_successor);
	UT_RUN(native_header_postwrite_refusal_preserves_effects);
	UT_RUN(native_header_sync_validates_selected_file_before_sync_or_note);
	UT_RUN(native_header_sync_preserves_original_record_or_samefd_fsync);
	UT_RUN(native_header_sync_failure_keeps_file_and_owner_debt);
	UT_RUN(native_header_error_cleanup_keeps_bytes_and_releases_only_borrow);
	UT_RUN(native_header_modes_and_argument_bounds_do_not_create_authority);
	UT_RUN(native_header_legacy_range_policy_is_unchanged);
	UT_ASSERT_EQ(rmdir(publication_dir), 0); UT_DONE(); return ut_failed_count ? 1 : 0;
}
