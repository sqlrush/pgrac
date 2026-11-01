/* Actual cached DATA I/O must use the selected TT40 identity on the same FD.
 * Control custody, path authorization and ERROR scheduling are finite providers;
 * the original smgr, file header and temporary-file I/O are executed. */
#define UNDO_NATIVE_TT_READ_MAIN native_data_read_original_main
int native_data_read_original_main(void);
#include "test_cluster_undo_native_tt_read.c"

static void
reset_data(PGAlignedBlock *header, PGAlignedBlock *data, char path[MAXPGPATH])
{
	int fd;

	reset_selected(header, path);
	memset(data->data, 0x37, BLCKSZ);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, data->data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	pwrite_forced_error = false;
	fsync_calls = fsync_fail_on_call = 0;
}

static bool
read_data(char out[BLCKSZ])
{
	return cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 1, out);
}

static bool
write_data(const char in[BLCKSZ], bool sync)
{
	return cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 1, in, sync);
}

static void
disk_data(const char *path, char out[BLCKSZ])
{
	int fd = open(path, O_RDONLY);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, out, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(native_data_exact_cached_read_write_and_sync)
{
	PGAlignedBlock header, data, out, changed, disk;
	char path[MAXPGPATH];
	int first_fd;

	reset_data(&header, &data, path);
	UT_ASSERT(read_data(out.data));
	UT_ASSERT_EQ(memcmp(out.data, data.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1);
	UT_ASSERT_EQ(selected_reads, 4);
	first_fd = cached_fd;
	memset(changed.data, 0x68, BLCKSZ);
	UT_ASSERT(write_data(changed.data, true));
	UT_ASSERT_EQ(cached_fd, first_fd);
	UT_ASSERT_EQ(selected_acquires, 2);
	UT_ASSERT_EQ(selected_reads, 8);
	UT_ASSERT_EQ(fsync_calls, 1);
	disk_data(path, disk.data);
	UT_ASSERT_EQ(memcmp(disk.data, changed.data, BLCKSZ), 0);
	UT_ASSERT(read_data(out.data));
	UT_ASSERT_EQ(memcmp(out.data, changed.data, BLCKSZ), 0);
	UT_ASSERT_EQ(pread(first_fd, disk.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(memcmp(disk.data, header.data, BLCKSZ), 0);
	finish_selected(path);
}

UT_TEST(native_data_unknown_or_different_identity_never_writes)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 8; variant++) {
		reset_data(&header, &data, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) selected.redo_branch_uuid[1]++;
		if (variant == 4) selected.storage_uuid[1]++;
		if (variant == 5) selected.database_incarnation++;
		if (variant == 6) selected.redo_timeline++;
		if (variant == 7) selected.system_identifier++;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!read_data(out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT(!write_data(before.data, true));
		disk_data(path, disk.data);
		UT_ASSERT_EQ(memcmp(disk.data, data.data, BLCKSZ), 0);
		UT_ASSERT_EQ(fsync_calls, 0);
		if (variant == 0) UT_ASSERT_EQ(cached_fd, -1);
		finish_selected(path);
	}
}

UT_TEST(native_data_rejects_corrupt_tt32_or_short_header)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 4; variant++) {
		int fd;

		reset_data(&header, &data, path);
		if (variant == 0) header.data[400] ^= 1;
		if (variant == 1) ((PageHeader) header.data)->pd_checksum = 0;
		if (variant == 2) header.data[57] = 0;
		fd = open(path, O_RDWR); UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, header.data, BLCKSZ, 0), BLCKSZ);
		if (variant == 3) UT_ASSERT_EQ(ftruncate(fd, BLCKSZ - 1), 0);
		UT_ASSERT_EQ(close(fd), 0);
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!read_data(out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT(!write_data(before.data, false));
		if (variant != 3) {
			disk_data(path, disk.data);
			UT_ASSERT_EQ(memcmp(disk.data, data.data, BLCKSZ), 0);
		} else {
			struct stat st;
			UT_ASSERT_EQ(stat(path, &st), 0);
			UT_ASSERT_EQ(st.st_size, BLCKSZ - 1);
		}
		finish_selected(path);
	}
}

UT_TEST(native_data_current_refusal_keeps_read_output_and_late_write_effects)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	unsigned at;

	for (at = 1; at <= 4; at++) {
		reset_data(&header, &data, path); selected_change_on = at;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!read_data(out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
		reset_data(&header, &data, path); selected_change_on = at;
		UT_ASSERT(!write_data(before.data, true));
		disk_data(path, disk.data);
		UT_ASSERT_EQ(memcmp(disk.data, at == 4 ? before.data : data.data, BLCKSZ), 0);
		UT_ASSERT_EQ(fsync_calls, at == 4 ? 1 : 0);
		finish_selected(path);
	}
}

UT_TEST(native_data_error_releases_only_operation_borrow)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	unsigned at, writing;

	for (writing = 0; writing < 2; writing++) {
		for (at = 1; at <= 4; at++) {
			volatile bool caught = false;

			reset_data(&header, &data, path); selected_throw_on = at;
			memset(out.data, 0x5a, BLCKSZ); before = out;
			PG_TRY();
			{
				if (writing) (void) write_data(before.data, true);
				else (void) read_data(out.data);
			}
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT(caught);
			UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
			UT_ASSERT_EQ(selected_borrows, 0);
			UT_ASSERT_EQ(cached_fd < 0, at == 1);
			disk_data(path, disk.data);
			UT_ASSERT_EQ(memcmp(disk.data, writing && at == 4 ? before.data : data.data,
				BLCKSZ), 0);
			finish_selected(path);
		}
	}
}

UT_TEST(native_data_io_failure_keeps_output_or_completed_write)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 5; variant++) {
		reset_data(&header, &data, path);
		if (variant == 0) basic_open_forced_error = true;
		if (variant == 1) pread_forced_error = true;
		if (variant == 2) pwrite_forced_error = true;
		if (variant == 3) fsync_fail_on_call = 1;
		if (variant == 4) UT_ASSERT_EQ(truncate(path, BLCKSZ + 7), 0);
		memset(out.data, 0x5a, BLCKSZ); before = out;
		if (variant == 0 || variant == 1 || variant == 4) {
			UT_ASSERT(!read_data(out.data));
			UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		} else {
			UT_ASSERT(!write_data(before.data, true));
			disk_data(path, disk.data);
			UT_ASSERT_EQ(memcmp(disk.data, variant == 3 ? before.data : data.data, BLCKSZ), 0);
		}
		finish_selected(path);
	}
	fsync_fail_on_call = 0; pwrite_forced_error = false;
}

UT_TEST(native_data_cached_fd_cannot_follow_reselected_file)
{
	PGAlignedBlock header, data, out, before, disk;
	char path[MAXPGPATH];
	int fd;

	reset_data(&header, &data, path);
	UT_ASSERT(read_data(out.data)); fd = cached_fd;
	selected.creation_generation++;
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, header.data));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(header.data, BLCKSZ, &selected));
	UT_ASSERT(cluster_undo_segment_header_seal(header.data));
	write_segment_file(path, header.data, UNDO_SEGMENT_SIZE_BYTES);
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT(!read_data(out.data));
	UT_ASSERT(!write_data(before.data, true));
	UT_ASSERT_EQ(cached_fd, fd);
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(pread(fd, disk.data, BLCKSZ, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(memcmp(disk.data, data.data, BLCKSZ), 0);
	finish_selected(path);
}

UT_TEST(native_data_bounds_and_managed_mode_dispatch)
{
	PGAlignedBlock header, data, out;
	char path[MAXPGPATH];
	uint32 last = UNDO_BLOCKS_PER_SEGMENT - 1;

	reset_data(&header, &data, path);
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 0, 1, 1, out.data));
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 257, 1, 1, data.data, false));
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 1, NULL));
	UT_ASSERT(!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
		UNDO_BLOCKS_PER_SEGMENT, data.data, false));
	UT_ASSERT_EQ(selected_acquires, 0);
	UT_ASSERT_EQ(cached_fd, -1);
	cluster_enabled = false;
	UT_ASSERT(cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, last, data.data, false));
	UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, last, out.data));
	UT_ASSERT_EQ(memcmp(out.data, data.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 2);
	UT_ASSERT_EQ(fsync_calls, 0);
	finish_selected(path);
}

UT_TEST(native_data_legacy_preserves_original_policy)
{
	PGAlignedBlock header, data, out;
	char path[MAXPGPATH];

	reset_data(&header, &data, path);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
	selected_missing = true;
	UT_ASSERT(read_data(out.data));
	UT_ASSERT_EQ(memcmp(out.data, data.data, BLCKSZ), 0);
	UT_ASSERT(write_data(data.data, false));
	UT_ASSERT_EQ(selected_acquires, 0);
	finish_selected(path);
}

int
main(void)
{
	char path[] = "/tmp/pgrac-native-data-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path));
	strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(9);
	UT_RUN(native_data_exact_cached_read_write_and_sync);
	UT_RUN(native_data_unknown_or_different_identity_never_writes);
	UT_RUN(native_data_rejects_corrupt_tt32_or_short_header);
	UT_RUN(native_data_current_refusal_keeps_read_output_and_late_write_effects);
	UT_RUN(native_data_error_releases_only_operation_borrow);
	UT_RUN(native_data_io_failure_keeps_output_or_completed_write);
	UT_RUN(native_data_cached_fd_cannot_follow_reselected_file);
	UT_RUN(native_data_bounds_and_managed_mode_dispatch);
	UT_RUN(native_data_legacy_preserves_original_policy);
	UT_ASSERT_EQ(rmdir(publication_dir), 0);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
