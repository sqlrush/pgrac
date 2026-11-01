/* Original recovery file APIs with actual smgr/TT40 and temporary-file I/O.
 * Selected control, recovery path authority and ERROR scheduling are finite. */
#include "postgres.h"
static uint64 recovery_table_generation = 41, recovery_startup_generation = 1;
#define UNDO_NATIVE_TT_TABLE_GENERATION recovery_table_generation
#define UNDO_NATIVE_TT_STARTUP_GENERATION recovery_startup_generation
#define UNDO_NATIVE_TT_READ_MAIN recovery_read_original_main
int recovery_read_original_main(void);
#include "test_cluster_undo_native_tt_read.c"

static void
reset_recovery(PGAlignedBlock *page, char path[MAXPGPATH])
{
	reset_selected(page, path);
	recovery_table_generation = 41; recovery_startup_generation = 1;
	fsync_calls = 0;
	product_close_calls = product_close_fail_on_call = 0;
}

static bool
recovery_probe(ClusterUndoSmgrRecoveryFileV1 *file, char page[BLCKSZ])
{
	return cluster_undo_smgr_recovery_probe_v1(1, 1, file, page);
}

static bool
recovery_read(const ClusterUndoSmgrRecoveryFileV1 *file, uint32 block, char page[BLCKSZ])
{
	return cluster_undo_smgr_recovery_read_block_v1(1, 1, block, file, page);
}

UT_TEST(recovery_read_selected_header_and_short_tail)
{
	PGAlignedBlock page, out, expected;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];
	int fd;

	reset_recovery(&page, path);
	fd = open(path, O_RDWR); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, BLCKSZ + 17), 0);
	UT_ASSERT_EQ(pwrite(fd, "kept", 4, BLCKSZ + 1), 4);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(recovery_probe(&file, out.data));
	UT_ASSERT(file.exists && file.size == BLCKSZ + 17);
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1);
	UT_ASSERT_EQ(selected_reads, 5);
	UT_ASSERT_EQ(product_close_calls, 1);
	UT_ASSERT(recovery_read(&file, 1, out.data));
	memset(expected.data, 0, BLCKSZ); memcpy(expected.data + 1, "kept", 4);
	UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
	UT_ASSERT(recovery_read(&file, 9, out.data));
	memset(expected.data, 0, BLCKSZ);
	UT_ASSERT_EQ(memcmp(out.data, expected.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 3);
	UT_ASSERT_EQ(cached_fd, -1);
	UT_ASSERT_EQ(fsync_calls, 0);
	finish_selected(path);
}

UT_TEST(recovery_read_unknown_or_different_identity_keeps_both_outputs)
{
	PGAlignedBlock page, out, before;
	ClusterUndoSmgrRecoveryFileV1 file, saved;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 9; variant++) {
		reset_recovery(&page, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) selected.redo_branch_uuid[1]++;
		if (variant == 4) selected.storage_uuid[1]++;
		if (variant == 5) selected.database_incarnation++;
		if (variant == 6) selected.redo_timeline++;
		if (variant == 7) selected.system_identifier++;
		if (variant == 8) selected.redo_branch_generation++;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		memset(&file, 0x5b, sizeof(file)); saved = file;
		UT_ASSERT(!recovery_probe(&file, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(&file, &saved, sizeof(file)), 0);
		if (variant == 0) UT_ASSERT_EQ(product_close_calls, 0);
		finish_selected(path);
	}
}

UT_TEST(recovery_read_snapshot_cannot_reselect_identity_or_generation)
{
	PGAlignedBlock page, out, before;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 5; variant++) {
		int fd;

		reset_recovery(&page, path);
		UT_ASSERT(recovery_probe(&file, out.data));
		if (variant == 0) {
			selected.creation_generation++;
			UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, page.data));
			UT_ASSERT(cluster_tt_file_reference_prepare_v2(page.data, BLCKSZ, &selected));
			UT_ASSERT(cluster_undo_segment_header_seal(page.data));
			fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(close(fd), 0);
		}
		if (variant == 1) recovery_table_generation++;
		if (variant == 2) recovery_startup_generation++;
		if (variant == 3) {
			selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
			UT_ASSERT(recovery_probe(&file, out.data));
			selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2; cluster_enabled = true;
		}
		if (variant == 4) {
			selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
			selected_missing = true;
		}
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!recovery_read(&file, 0, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
	}
}

UT_TEST(recovery_read_requires_complete_valid_tt40_header)
{
	PGAlignedBlock page, out, before;
	ClusterUndoSmgrRecoveryFileV1 file, saved;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 5; variant++) {
		reset_recovery(&page, path);
		if (variant == 0) page.data[400] ^= 1;
		if (variant == 1) ((PageHeader) page.data)->pd_checksum = 0;
		if (variant == 2) page.data[57] = 0;
		remove_if_present(path);
		write_segment_file(path, page.data, variant == 3 ? BLCKSZ - 1 :
			variant == 4 ? UNDO_SEGMENT_SIZE_BYTES + 1 : BLCKSZ);
		memset(out.data, 0x5a, BLCKSZ); before = out;
		memset(&file, 0x5b, sizeof(file)); saved = file;
		UT_ASSERT(!recovery_probe(&file, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(memcmp(&file, &saved, sizeof(file)), 0);
		finish_selected(path);
	}
}

UT_TEST(recovery_read_missing_file_is_bound_observation_only)
{
	PGAlignedBlock page, out, zero, before;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	reset_recovery(&page, path); remove_if_present(path);
	memset(zero.data, 0, BLCKSZ);
	UT_ASSERT(recovery_probe(&file, out.data));
	UT_ASSERT(!file.exists);
	UT_ASSERT_EQ(memcmp(out.data, zero.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1);
	UT_ASSERT(recovery_read(&file, 9, out.data));
	UT_ASSERT_EQ(memcmp(out.data, zero.data, BLCKSZ), 0);
	UT_ASSERT(access(path, F_OK) != 0);
	selected.creation_generation++;
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT(!recovery_read(&file, 0, out.data));
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	UT_ASSERT(access(path, F_OK) != 0);
	finish_selected(path);
}

UT_TEST(recovery_read_current_and_error_cleanup_preserve_outputs)
{
	PGAlignedBlock page, out, before;
	ClusterUndoSmgrRecoveryFileV1 file, saved;
	char path[MAXPGPATH];
	unsigned at, throwing, reading;

	for (reading = 0; reading < 2; reading++) {
		for (throwing = 0; throwing < 2; throwing++) {
			for (at = 1; at <= 5; at++) {
				volatile bool caught = false, ok = true;

				reset_recovery(&page, path);
				if (reading) UT_ASSERT(recovery_probe(&file, out.data));
				else memset(&file, 0x5b, sizeof(file));
				saved = file;
				selected_reads = 0; product_close_calls = 0;
				if (throwing) selected_throw_on = at;
				else selected_change_on = at;
				memset(out.data, 0x5a, BLCKSZ); before = out;
				PG_TRY();
				{
					ok = reading ? recovery_read(&file, 0, out.data) : recovery_probe(&file, out.data);
				}
				PG_CATCH(); { caught = true; }
				PG_END_TRY();
				UT_ASSERT_EQ(caught, throwing != 0);
				if (!throwing) UT_ASSERT(!ok);
				UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
				UT_ASSERT_EQ(memcmp(&file, &saved, sizeof(file)), 0);
				UT_ASSERT_EQ(selected_borrows, 0);
				UT_ASSERT_EQ(cached_fd, -1);
				UT_ASSERT_EQ(product_close_calls, at == 1 ? 0 : 1);
				finish_selected(path);
			}
		}
	}
}

UT_TEST(recovery_read_io_and_original_stat_refusals)
{
	PGAlignedBlock page, out, before;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 6; variant++) {
		reset_recovery(&page, path);
		UT_ASSERT(recovery_probe(&file, out.data));
		if (variant == 0) basic_open_forced_error = true;
		if (variant == 1) pread_forced_error = true;
		if (variant == 2) product_close_fail_on_call = product_close_calls + 1;
		if (variant == 3) file.size++;
		if (variant == 4) file.inode++;
		if (variant == 5) {
			int old_fd = open(path, O_RDONLY);
			UT_ASSERT(old_fd >= 0);
			remove_if_present(path); write_segment_file(path, page.data, BLCKSZ);
			UT_ASSERT_EQ(close(old_fd), 0);
		}
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!recovery_read(&file, 0, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
	}
	product_close_fail_on_call = 0;
}

UT_TEST(recovery_read_arguments_managed_guc_off_and_legacy)
{
	PGAlignedBlock page, out;
	ClusterUndoSmgrRecoveryFileV1 file;
	char path[MAXPGPATH];

	reset_recovery(&page, path);
	UT_ASSERT(!cluster_undo_smgr_recovery_probe_v1(0, 1, &file, out.data));
	UT_ASSERT(!cluster_undo_smgr_recovery_probe_v1(257, 1, &file, out.data));
	UT_ASSERT(!cluster_undo_smgr_recovery_probe_v1(1, 0, &file, out.data));
	UT_ASSERT(!recovery_probe(NULL, out.data));
	UT_ASSERT(!recovery_probe(&file, NULL));
	UT_ASSERT(!recovery_read(NULL, 0, out.data));
	UT_ASSERT(!recovery_read(&file, UNDO_BLOCKS_PER_SEGMENT, out.data));
	UT_ASSERT_EQ(selected_acquires, 0);
	UT_ASSERT_EQ(product_close_calls, 0);
	cluster_enabled = false;
	UT_ASSERT(recovery_probe(&file, out.data));
	UT_ASSERT_EQ(selected_acquires, 1);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; selected_missing = true;
	UT_ASSERT_EQ(truncate(path, 17), 0);
	UT_ASSERT(recovery_probe(&file, out.data));
	UT_ASSERT(file.exists && file.size == 17);
	UT_ASSERT_EQ(selected_acquires, 1);
	finish_selected(path);
}

int
main(void)
{
	char path[] = "/tmp/pgrac-tt-recovery-read-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(8);
	UT_RUN(recovery_read_selected_header_and_short_tail);
	UT_RUN(recovery_read_unknown_or_different_identity_keeps_both_outputs);
	UT_RUN(recovery_read_snapshot_cannot_reselect_identity_or_generation);
	UT_RUN(recovery_read_requires_complete_valid_tt40_header);
	UT_RUN(recovery_read_missing_file_is_bound_observation_only);
	UT_RUN(recovery_read_current_and_error_cleanup_preserve_outputs);
	UT_RUN(recovery_read_io_and_original_stat_refusals);
	UT_RUN(recovery_read_arguments_managed_guc_off_and_legacy);
	UT_ASSERT_EQ(rmdir(publication_dir), 0); UT_DONE();
	return ut_failed_count ? 1 : 0;
}
