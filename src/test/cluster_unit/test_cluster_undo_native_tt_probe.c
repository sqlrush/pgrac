/* Native final-file probe uses the actual smgr and real temporary files.
 * Only selected control custody, path authority and error scheduling are finite. */
#define UNDO_NATIVE_TT_READ_MAIN native_probe_read_original_main
int native_probe_read_original_main(void);
#include "test_cluster_undo_native_tt_read.c"

static void
reset_probe(PGAlignedBlock *page, char path[MAXPGPATH])
{
	reset_selected(page, path);
	UT_ASSERT_EQ(truncate(path, UNDO_SEGMENT_SIZE_BYTES), 0);
	fsync_calls = fsync_fail_on_call = 0;
	product_close_calls = product_close_fail_on_call = 0;
}

static ClusterUndoSmgrFinalState
probe(char out[BLCKSZ])
{
	return cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, out);
}

UT_TEST(native_probe_exact_complete_page)
{
	PGAlignedBlock page, out;
	char path[MAXPGPATH];
	UndoSegmentHeaderDataV2 *header = (void *) page.data;

	reset_probe(&page, path);
	header->tt_slots[0].status = TT_SLOT_COMMITTED;
	header->tt_slots[0].xid = 700;
	header->tt_slots[0].wrap = 3;
	header->tt_slots[0].original_full_xid = (UINT64CONST(2) << 32) | 700;
	header->tt_slots[0].commit_scn = 80;
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	remove_if_present(path);
	write_segment_file(path, page.data, UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT_EQ(probe(out.data), CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1);
	UT_ASSERT_EQ(selected_reads, 5);
	UT_ASSERT_EQ(fsync_calls, 1);
	UT_ASSERT_EQ(product_close_calls, 2);
	UT_ASSERT_EQ(cached_fd, -1);
	finish_selected(path);
}

UT_TEST(native_probe_unknown_or_different_selected_identity)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 8; variant++) {
		reset_probe(&page, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) selected.redo_branch_uuid[1]++;
		if (variant == 4) selected.storage_uuid[1]++;
		if (variant == 5) selected.database_incarnation++;
		if (variant == 6) selected.redo_timeline++;
		if (variant == 7) selected.system_identifier++;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT_EQ(probe(out.data), variant == 0 ? CLUSTER_UNDO_SMGR_FINAL_IO_ERROR
												 : CLUSTER_UNDO_SMGR_FINAL_INVALID);
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(fsync_calls, 0);
		if (variant == 0) UT_ASSERT_EQ(product_close_calls, 0);
		finish_selected(path);
	}
}

UT_TEST(native_probe_corrupt_unknown_or_wrong_geometry)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 4; variant++) {
		reset_probe(&page, path);
		if (variant == 0) page.data[400] ^= 1;
		if (variant == 1) ((PageHeader) page.data)->pd_checksum = 0;
		if (variant == 2) page.data[57] = 0;
		remove_if_present(path);
		write_segment_file(path, page.data, variant == 3 ? BLCKSZ : UNDO_SEGMENT_SIZE_BYTES);
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT_EQ(probe(out.data), CLUSTER_UNDO_SMGR_FINAL_INVALID);
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(fsync_calls, 0);
		finish_selected(path);
	}
}

UT_TEST(native_probe_current_refusal_preserves_output_and_resources)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	unsigned at;

	for (at = 1; at <= 5; at++) {
		reset_probe(&page, path); selected_change_on = at;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT_EQ(probe(out.data), CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(product_close_calls, at == 1 ? 0 : at == 5 ? 2 : 1);
		UT_ASSERT_EQ(cached_fd, -1);
		finish_selected(path);
	}
}

UT_TEST(native_probe_error_closes_only_owned_fd_and_borrow)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	unsigned at;

	for (at = 1; at <= 5; at++) {
		volatile bool caught = false;
		reset_probe(&page, path); selected_throw_on = at;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		PG_TRY(); { (void) probe(out.data); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(product_close_calls, at == 1 ? 0 : at == 5 ? 2 : 1);
		UT_ASSERT_EQ(cached_fd, -1);
		finish_selected(path);
	}
}

UT_TEST(native_probe_io_refuses_and_absence_is_only_observation)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	unsigned variant;

	for (variant = 0; variant < 7; variant++) {
		reset_probe(&page, path);
		if (variant == 0) basic_open_forced_error = true;
		if (variant == 1) pread_forced_error = true;
		if (variant == 2) product_close_fail_on_call = 1;
		if (variant == 3) fsync_fail_on_call = 1;
		if (variant == 4) product_close_fail_on_call = 2;
		if (variant >= 5) remove_if_present(path);
		if (variant == 6) selected_change_on = 2;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT_EQ(probe(out.data), variant == 5 ? CLUSTER_UNDO_SMGR_FINAL_ABSENT
												 : CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(selected_acquires, 1);
		if (variant >= 5) UT_ASSERT(access(path, F_OK) != 0);
		finish_selected(path);
	}
	fsync_fail_on_call = product_close_fail_on_call = 0;
}

UT_TEST(native_probe_tuple_guards_and_managed_dispatch)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];

	reset_probe(&page, path);
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, NULL),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 0, 1, out.data),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 0, out.data),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 257, 1, out.data),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	UT_ASSERT_EQ(selected_acquires, 0);
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	cluster_enabled = false;
	UT_ASSERT_EQ(probe(out.data), CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(selected_acquires, 1);
	finish_selected(path);
}

UT_TEST(native_probe_legacy_path_does_not_acquire_managed_identity)
{
	PGAlignedBlock page, out;
	char path[MAXPGPATH];

	reset_probe(&page, path);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
	selected_missing = true;
	UT_ASSERT_EQ(probe(out.data), CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 0);
	finish_selected(path);
}

int
main(void)
{
	char path[] = "/tmp/pgrac-native-tt-probe-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path));
	strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(8);
	UT_RUN(native_probe_exact_complete_page);
	UT_RUN(native_probe_unknown_or_different_selected_identity);
	UT_RUN(native_probe_corrupt_unknown_or_wrong_geometry);
	UT_RUN(native_probe_current_refusal_preserves_output_and_resources);
	UT_RUN(native_probe_error_closes_only_owned_fd_and_borrow);
	UT_RUN(native_probe_io_refuses_and_absence_is_only_observation);
	UT_RUN(native_probe_tuple_guards_and_managed_dispatch);
	UT_RUN(native_probe_legacy_path_does_not_acquire_managed_identity);
	UT_ASSERT_EQ(rmdir(publication_dir), 0);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
