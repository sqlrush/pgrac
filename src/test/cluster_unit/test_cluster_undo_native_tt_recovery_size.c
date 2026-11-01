/* Existing-file recovery size preparation with real TT40/tmp-file I/O.
 * The selected table, original path scope and ERROR schedule are finite. */
#include "postgres.h"
#include <unistd.h>
static uint64 size_table_generation = 41, size_startup_generation = 1;
static bool size_tracking, size_truncate_fails;
static unsigned size_truncates;
static int size_test_ftruncate(int fd, off_t length);
#define ftruncate size_test_ftruncate
#define UNDO_NATIVE_TT_TABLE_GENERATION size_table_generation
#define UNDO_NATIVE_TT_STARTUP_GENERATION size_startup_generation
#define UNDO_NATIVE_TT_READ_MAIN recovery_size_original_main
int recovery_size_original_main(void);
#include "test_cluster_undo_native_tt_read.c"
#undef ftruncate

static int
size_test_ftruncate(int fd, off_t length)
{
	if (size_tracking) {
		size_truncates++;
		if (size_truncate_fails) {
			errno = EIO;
			return -1;
		}
	}
	return ftruncate(fd, length);
}

static uint64
size_on_disk(const char *path)
{
	struct stat st;

	UT_ASSERT_EQ(stat(path, &st), 0);
	return st.st_size;
}

static void
size_reset(PGAlignedBlock *page, ClusterUndoSmgrRecoveryFileV1 *file, char path[MAXPGPATH])
{
	PGAlignedBlock out;

	size_tracking = size_truncate_fails = false;
	reset_selected(page, path);
	size_table_generation = 41; size_startup_generation = 1;
	UT_ASSERT_EQ(truncate(path, BLCKSZ + 17), 0);
	UT_ASSERT(cluster_undo_smgr_recovery_probe_v1(1, 1, file, out.data));
	UT_ASSERT_EQ(memcmp(out.data, page->data, BLCKSZ), 0);
	selected_acquires = selected_reads = 0;
	product_close_calls = product_close_fail_on_call = 0;
	fsync_calls = fsync_fail_on_call = 0;
	size_truncates = 0; size_tracking = true;
}

static bool
size_prepare(const ClusterUndoSmgrRecoveryFileV1 *file, const char *base, const char *final)
{
	return cluster_undo_smgr_recovery_materialize_v1(1, 1, file, base, final);
}

UT_TEST(recovery_size_grows_original_file_without_publishing_header)
{
	PGAlignedBlock page, final, out;
	ClusterUndoSmgrRecoveryFileV1 file, saved;
	char path[MAXPGPATH], kept[4];
	int fd;

	size_reset(&page, &file, path); saved = file; final = page;
	((UndoSegmentHeaderDataV2 *)final.data)->wrap_count++;
	UT_ASSERT(cluster_undo_segment_header_seal(final.data));
	fd = open(path, O_RDWR); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, "kept", 4, BLCKSZ + 1), 4);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(size_prepare(&file, page.data, final.data));
	UT_ASSERT_EQ(size_on_disk(path), UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT_EQ(selected_acquires, 1);
	UT_ASSERT_EQ(selected_reads, 8);
	UT_ASSERT_EQ(selected_borrows, 0);
	UT_ASSERT_EQ(size_truncates, 1);
	UT_ASSERT_EQ(fsync_calls, 3);
	UT_ASSERT_EQ(product_close_calls, 3);
	UT_ASSERT_EQ(memcmp(&file, &saved, sizeof(file)), 0);
	fd = open(path, O_RDONLY); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, out.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(pread(fd, kept, 4, BLCKSZ + 1), 4);
	UT_ASSERT_EQ(memcmp(kept, "kept", 4), 0);
	UT_ASSERT_EQ(pread(fd, kept, 4, UNDO_SEGMENT_SIZE_BYTES - 4), 4);
	UT_ASSERT_EQ(memcmp(kept, "\0\0\0\0", 4), 0);
	UT_ASSERT_EQ(close(fd), 0);
	finish_selected(path);
}

UT_TEST(recovery_size_refuses_absence_and_unqualified_observations)
{
	PGAlignedBlock page, out;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned variant = 0; variant < 3; variant++) {
		size_reset(&page, &file, path);
		if (variant == 0) {
			remove_if_present(path);
			UT_ASSERT(cluster_undo_smgr_recovery_probe_v1(1, 1, &file, out.data));
			UT_ASSERT(!file.exists);
		} else if (variant == 1) file.native_selected = false;
		else file.native_table_generation = 0;
		UT_ASSERT(!size_prepare(&file, page.data, page.data));
		UT_ASSERT_EQ(size_truncates, 0);
		UT_ASSERT_EQ(fsync_calls, 0);
		if (variant == 0) UT_ASSERT(access(path, F_OK) != 0);
		else UT_ASSERT_EQ(size_on_disk(path), BLCKSZ + 17);
		finish_selected(path);
	}
}

UT_TEST(recovery_size_cannot_reselect_control_or_generations)
{
	PGAlignedBlock page;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned variant = 0; variant < 7; variant++) {
		size_reset(&page, &file, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) size_table_generation++;
		if (variant == 4) size_startup_generation++;
		if (variant == 5) file.native_file_reference[40] ^= 1;
		if (variant == 6) {
			selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
			cluster_enabled = false; selected_missing = true;
		}
		UT_ASSERT(!size_prepare(&file, page.data, page.data));
		UT_ASSERT_EQ(size_truncates, 0);
		UT_ASSERT_EQ(fsync_calls, 0);
		UT_ASSERT_EQ(size_on_disk(path), BLCKSZ + 17);
		finish_selected(path);
	}
}

UT_TEST(recovery_size_validates_both_tt40_images_before_mutation)
{
	PGAlignedBlock page, base, final;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned variant = 0; variant < 5; variant++) {
		size_reset(&page, &file, path); base = final = page;
		if (variant == 0) base.data[400] ^= 1;
		if (variant == 1) final.data[400] ^= 1;
		if (variant == 2) final.data[57] = 0;
		if (variant == 3) {
			ClusterRecoveryFileIdentityV2 other = selected;

			other.creation_generation++;
			UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, final.data));
			UT_ASSERT(cluster_tt_file_reference_prepare_v2(final.data, BLCKSZ, &other));
			UT_ASSERT(cluster_undo_segment_header_seal(final.data));
		}
		if (variant == 4) ((PageHeader)final.data)->pd_checksum = 0;
		UT_ASSERT(!size_prepare(&file, base.data, final.data));
		UT_ASSERT_EQ(size_truncates, 0);
		UT_ASSERT_EQ(size_on_disk(path), BLCKSZ + 17);
		finish_selected(path);
	}
}

UT_TEST(recovery_size_preserves_original_inode_size_and_base_checks)
{
	PGAlignedBlock page, disk;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned variant = 0; variant < 4; variant++) {
		size_reset(&page, &file, path);
		if (variant == 0) file.inode++;
		if (variant == 1) file.size++;
		if (variant >= 2) {
			int fd = open(path, O_WRONLY);

			UT_ASSERT(fd >= 0); disk = page;
			((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count++;
			if (variant == 2) UT_ASSERT(cluster_undo_segment_header_seal(disk.data));
			UT_ASSERT_EQ(pwrite(fd, disk.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(close(fd), 0);
		}
		UT_ASSERT(!size_prepare(&file, page.data, page.data));
		UT_ASSERT_EQ(size_truncates, 0);
		UT_ASSERT_EQ(size_on_disk(path), BLCKSZ + 17);
		finish_selected(path);
	}
}

UT_TEST(recovery_size_current_refusal_and_error_keep_effects_and_cleanup)
{
	PGAlignedBlock page;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned throwing = 0; throwing < 2; throwing++) {
		for (unsigned at = 1; at <= 8; at++) {
			volatile bool caught = false, ok = true;

			size_reset(&page, &file, path);
			if (throwing) selected_throw_on = at;
			else selected_change_on = at;
			PG_TRY(); { ok = size_prepare(&file, page.data, page.data); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, throwing != 0);
			if (!throwing) UT_ASSERT(!ok);
			UT_ASSERT_EQ(selected_borrows, 0);
			UT_ASSERT_EQ(size_truncates, at <= 3 ? 0 : 1);
			UT_ASSERT_EQ(size_on_disk(path), at <= 3 ? BLCKSZ + 17 : UNDO_SEGMENT_SIZE_BYTES);
			UT_ASSERT_EQ(product_close_calls, at == 1 ? 0 : at <= 6 ? 1 : at - 5);
			finish_selected(path);
		}
	}
}

UT_TEST(recovery_size_io_refusals_preserve_late_effects)
{
	PGAlignedBlock page;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	for (unsigned variant = 0; variant < 7; variant++) {
		size_reset(&page, &file, path);
		if (variant == 0) basic_open_forced_error = true;
		if (variant == 1) pread_forced_error = true;
		if (variant == 2) size_truncate_fails = true;
		if (variant == 3) fsync_fail_on_call = 1;
		if (variant == 4) product_close_fail_on_call = 1;
		if (variant == 5) fsync_fail_on_call = 2;
		if (variant == 6) fsync_fail_on_call = 3;
		UT_ASSERT(!size_prepare(&file, page.data, page.data));
		UT_ASSERT_EQ(selected_borrows, 0);
		UT_ASSERT_EQ(size_on_disk(path), variant <= 2 ? BLCKSZ + 17 : UNDO_SEGMENT_SIZE_BYTES);
		finish_selected(path);
	}
}

UT_TEST(recovery_size_legacy_branch_keeps_original_behavior)
{
	PGAlignedBlock page, out;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	size_reset(&page, &file, path);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
	UT_ASSERT(cluster_undo_smgr_recovery_probe_v1(1, 1, &file, out.data));
	UT_ASSERT(!file.native_selected);
	selected_acquires = 0;
	UT_ASSERT(size_prepare(&file, page.data, page.data));
	UT_ASSERT_EQ(selected_acquires, 0);
	UT_ASSERT_EQ(size_on_disk(path), UNDO_SEGMENT_SIZE_BYTES);
	finish_selected(path);
}

UT_TEST(recovery_size_arguments_refuse_before_acquire)
{
	PGAlignedBlock page;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	size_reset(&page, &file, path);
	UT_ASSERT(!size_prepare(NULL, page.data, page.data));
	UT_ASSERT(!size_prepare(&file, NULL, page.data));
	UT_ASSERT(!size_prepare(&file, page.data, NULL));
	UT_ASSERT(!cluster_undo_smgr_recovery_materialize_v1(0, 1, &file, page.data, page.data));
	UT_ASSERT(!cluster_undo_smgr_recovery_materialize_v1(257, 1, &file, page.data, page.data));
	UT_ASSERT(!cluster_undo_smgr_recovery_materialize_v1(1, 0, &file, page.data, page.data));
	UT_ASSERT_EQ(selected_acquires, 0);
	UT_ASSERT_EQ(size_truncates, 0);
	finish_selected(path);
}

int
main(void)
{
	char path[] = "/tmp/pgrac-tt-recovery-size-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(9);
	UT_RUN(recovery_size_grows_original_file_without_publishing_header);
	UT_RUN(recovery_size_refuses_absence_and_unqualified_observations);
	UT_RUN(recovery_size_cannot_reselect_control_or_generations);
	UT_RUN(recovery_size_validates_both_tt40_images_before_mutation);
	UT_RUN(recovery_size_preserves_original_inode_size_and_base_checks);
	UT_RUN(recovery_size_current_refusal_and_error_keep_effects_and_cleanup);
	UT_RUN(recovery_size_io_refusals_preserve_late_effects);
	UT_RUN(recovery_size_legacy_branch_keeps_original_behavior);
	UT_RUN(recovery_size_arguments_refuse_before_acquire);
	UT_ASSERT_EQ(rmdir(publication_dir), 0); UT_DONE();
	return ut_failed_count ? 1 : 0;
}
