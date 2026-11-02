/* Native TT block-zero I/O: exact selected identity, private read and custody.
 * The original path resolver and control table are finite providers; actual
 * smgr/cache, TT40 framing, checksum and temporary-file reads are exercised. */
#include "postgres.h"
#include "access/clog_file.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_undo_segment_init.h"

static bool selected_missing, selected_current = true;
static unsigned selected_change_on, selected_throw_on;
static unsigned selected_borrows, selected_reads, selected_acquires;
static ClusterControlNativeFileBorrowV2 *selected_handles[8];
static bool selected_release_ok = true;
static ClusterRecoveryFileIdentityV2 selected;
static ClusterControlNativeModeV2 selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;

#define main publication_original_main
#define UNDO_PUBLICATION_NATIVE_TEST
int publication_original_main(void);
#include "test_cluster_undo_smgr_publication.c"
#undef main

bool cluster_enabled = false;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void pg_re_throw(void)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	abort();
}

#ifndef UNDO_NATIVE_TT_EXIT_TEST
void before_shmem_exit(pg_on_exit_callback function, Datum arg)
{ (void) function; (void) arg; }
#endif

#ifndef UNDO_NATIVE_TT_HEADER_RECORDER_TEST
/* These original physical-I/O cases have no resident checkpoint region.
 * If a header call is added, it must take the actual same-FD fsync fallback. */
bool cluster_undo_block0_note_unsynced_header(uint32 segment, uint8 owner,
	ClusterUndoPathIntent intent)
{ (void) segment; (void) owner; (void) intent; return false; }
#endif

ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return selected_mode; }
#ifndef UNDO_NATIVE_TT_TABLE_GENERATION
#define UNDO_NATIVE_TT_TABLE_GENERATION 41
#endif
#ifndef UNDO_NATIVE_TT_STARTUP_GENERATION
#define UNDO_NATIVE_TT_STARTUP_GENERATION 1
#endif
#ifndef UNDO_NATIVE_TT_INVENTORY_TEST
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *borrow)
{
	if (key->kind != CLUSTER_RECOVERY_FILE_UNDO_V2 || key->origin != 0 ||
		key->database_oid != 0 || key->tablespace_oid != 0 || key->object_id != 1 ||
		borrow->private_self != 0) abort();
	selected_acquires++;
	if (selected_missing) return false;
	borrow->expected = selected;
	borrow->table_generation = UNDO_NATIVE_TT_TABLE_GENERATION;
	borrow->startup_generation = UNDO_NATIVE_TT_STARTUP_GENERATION;
	borrow->private_self = (uintptr_t) borrow; borrow->private_pid = 4321;
	if (selected_borrows >= lengthof(selected_handles)) abort();
	selected_handles[selected_borrows] = borrow;
	selected_borrows++;
	return true;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < selected_borrows; i++) if (selected_handles[i] == borrow) break;
	if (i == selected_borrows || borrow->private_self != (uintptr_t) borrow) abort();
	selected_reads++;
	if (selected_reads == selected_throw_on)
		siglongjmp(*PG_exception_stack, 1);
	return selected_current && selected_reads != selected_change_on;
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < selected_borrows; i++) if (selected_handles[i] == borrow) break;
	if (i == selected_borrows || borrow->private_self != (uintptr_t) borrow) abort();
	if (!selected_release_ok) return false;
	selected_handles[i] = selected_handles[--selected_borrows];
	memset(borrow, 0, sizeof(*borrow));
	return true;
}
#endif

static void reset_selected(PGAlignedBlock *page, char path[MAXPGPATH])
{
	cluster_undo_smgr_fd_cache_reset();
	selected_borrows = selected_reads = selected_acquires = 0;
	selected_change_on = selected_throw_on = 0;
	selected_missing = false; selected_current = true;
	selected_release_ok = true;
	selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2; cluster_enabled = true;
	pread_forced_error = basic_open_forced_error = false;
	memset(&selected, 0, sizeof(selected));
	selected.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	selected.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	selected.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	selected.system_identifier = 77; selected.database_incarnation = 1;
	selected.storage_uuid[0] = 1; selected.redo_branch_uuid[0] = 2;
	selected.redo_branch_generation = 1; selected.redo_timeline = 1;
	selected.creation_generation = 3; selected.registration_generation = 4;
	selected.object_id = 1;
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, page->data));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(page->data, BLCKSZ, &selected));
	UT_ASSERT(cluster_undo_segment_header_seal(page->data));
	resolve_final(path); remove_if_present(path);
	write_segment_file(path, page->data, BLCKSZ);
}

static void finish_selected(const char *path)
{
	UT_ASSERT_EQ(selected_borrows, 0);
	cluster_undo_smgr_fd_cache_reset(); remove_if_present(path);
}

UT_TEST(native_tt_read_preserves_full_page_and_fullxid)
{
	PGAlignedBlock page, out;
	char path[MAXPGPATH];
	UndoSegmentHeaderDataV2 *header = (void *) page.data;
	reset_selected(&page, path);
	header->tt_slots[0].status = TT_SLOT_COMMITTED;
	header->tt_slots[0].xid = 700; header->tt_slots[0].wrap = 3;
	header->tt_slots[0].original_full_xid = (UINT64CONST(2) << 32) | 700;
	header->tt_slots[0].commit_scn = 80;
	UT_ASSERT(cluster_undo_segment_header_seal(page.data));
	remove_if_present(path); write_segment_file(path, page.data, BLCKSZ);
	UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	UT_ASSERT_EQ(memcmp(page.data, out.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1); UT_ASSERT(selected_reads > 0);
	finish_selected(path);
}

UT_TEST(native_tt_read_refuses_missing_control_before_open)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	reset_selected(&page, path); selected_missing = true;
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	UT_ASSERT_EQ(cached_fd, -1); UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	finish_selected(path);
}

UT_TEST(native_tt_read_refuses_changed_control_for_cached_fd)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	reset_selected(&page, path);
	UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	selected.creation_generation++;
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	finish_selected(path);
}

UT_TEST(native_tt_read_refuses_checksum_unknown_and_tt32)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	for (unsigned variant = 0; variant < 3; variant++)
	{
		reset_selected(&page, path);
		if (variant == 0) page.data[400] ^= 1;
		if (variant == 1) ((PageHeader) page.data)->pd_checksum = 0;
		if (variant == 2) page.data[57] = 0;
		remove_if_present(path); write_segment_file(path, page.data, BLCKSZ);
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
	}
}

UT_TEST(native_tt_read_keeps_output_on_io_and_current_refusal)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	for (unsigned variant = 0; variant < 4; variant++)
	{
		reset_selected(&page, path);
		if (variant == 0) pread_forced_error = true;
		if (variant == 1) selected_current = false;
		if (variant == 2) UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
		if (variant == 3) selected_change_on = 3;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
		UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
	}
}

UT_TEST(native_tt_read_error_releases_borrow_without_output)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	for (unsigned at = 1; at <= 3; at++)
	{
		volatile bool caught = false;
		reset_selected(&page, path); selected_throw_on = at;
		memset(out.data, 0x5a, BLCKSZ); before = out;
		PG_TRY();
		{
			(void) cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
		finish_selected(path);
	}
}

UT_TEST(native_tt_read_validates_arguments_before_acquiring)
{
	PGAlignedBlock page, out, before;
	char path[MAXPGPATH];
	reset_selected(&page, path);
	memset(out.data, 0x5a, BLCKSZ); before = out;
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 0, 1, 0, out.data));
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 0, 0, out.data));
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 257, 1, 0, out.data));
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, NULL));
	UT_ASSERT_EQ(selected_acquires, 0); UT_ASSERT_EQ(cached_fd, -1);
	UT_ASSERT_EQ(memcmp(out.data, before.data, BLCKSZ), 0);
	finish_selected(path);
}

UT_TEST(native_tt_read_selected_mode_applies_with_cluster_guc_off)
{
	PGAlignedBlock page, out;
	char path[MAXPGPATH];
	reset_selected(&page, path);
	cluster_enabled = false;
	UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 1);
	finish_selected(path);
}

UT_TEST(native_tt_read_legacy_path_keeps_original_bytes)
{
	PGAlignedBlock page, out;
	char path[MAXPGPATH];
	reset_selected(&page, path);
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; cluster_enabled = false;
	page.data[57] = 0; /* Legacy path does not acquire managed TT40 authority. */
	remove_if_present(path); write_segment_file(path, page.data, BLCKSZ);
	UT_ASSERT(cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, out.data));
	UT_ASSERT_EQ(memcmp(out.data, page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(selected_acquires, 0);
	finish_selected(path);
}

#ifndef UNDO_NATIVE_TT_READ_MAIN
#define UNDO_NATIVE_TT_READ_MAIN main
#endif
int UNDO_NATIVE_TT_READ_MAIN(void)
{
	char path[] = "/tmp/pgrac-native-tt-read-XXXXXX";
	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(9);
	UT_RUN(native_tt_read_preserves_full_page_and_fullxid);
	UT_RUN(native_tt_read_refuses_missing_control_before_open);
	UT_RUN(native_tt_read_refuses_changed_control_for_cached_fd);
	UT_RUN(native_tt_read_refuses_checksum_unknown_and_tt32);
	UT_RUN(native_tt_read_keeps_output_on_io_and_current_refusal);
	UT_RUN(native_tt_read_error_releases_borrow_without_output);
	UT_RUN(native_tt_read_validates_arguments_before_acquiring);
	UT_RUN(native_tt_read_selected_mode_applies_with_cluster_guc_off);
	UT_RUN(native_tt_read_legacy_path_keeps_original_bytes);
	UT_ASSERT_EQ(rmdir(publication_dir), 0);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
