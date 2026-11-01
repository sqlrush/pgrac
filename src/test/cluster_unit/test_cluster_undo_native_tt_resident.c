/* Exact resident implementation; controlled table/locks/SMGR input boundaries.
 * Removing native custody must permit the forbidden replacement in test 1. */
#include "postgres.h"
#include "access/clog_file.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#include <sys/wait.h>
#include <unistd.h>
#define UNDO_RESIDENT_NATIVE_TEST
#define main block0_original_main
int block0_original_main(void);
#include "test_cluster_undo_block0.c"
#undef main

bool cluster_enabled = true;
static ClusterRecoveryFileIdentityV2 selected;
static ClusterControlNativeModeV2 native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
static ClusterControlNativeFileBorrowV2 *active_borrows[8];
static unsigned borrow_count, acquire_count, release_count;
static bool missing, current = true, release_ok = true;

int errcode(int code)
{
	UT_ASSERT(code == ERRCODE_DATA_CORRUPTED || code == ERRCODE_INTERNAL_ERROR);
	return 0;
}

ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return native_mode; }
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *borrow)
{
	if (key->kind != CLUSTER_RECOVERY_FILE_UNDO_V2 || key->origin != 0 ||
		key->database_oid != 0 || key->tablespace_oid != 0 || key->object_id != 1 ||
		borrow->private_self != 0 || borrow_count >= lengthof(active_borrows)) abort();
	acquire_count++;
	if (missing) return false;
	borrow->expected = selected;
	borrow->table_generation = 41; borrow->startup_generation = 1;
	borrow->private_self = (uintptr_t) borrow;
	active_borrows[borrow_count++] = borrow;
	return true;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < borrow_count; i++)
		if (active_borrows[i] == borrow && borrow->private_self == (uintptr_t) borrow)
			return current;
	abort();
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < borrow_count; i++)
		if (active_borrows[i] == borrow && borrow->private_self == (uintptr_t) borrow) {
			if (!release_ok) return false;
			active_borrows[i] = active_borrows[--borrow_count];
			release_count++; memset(borrow, 0, sizeof(*borrow)); return true;
		}
	abort();
}

static ClusterUndoBlock0LogicalKey logical;
static ClusterUndoBlock0ResolvedRoot root;
static ClusterUndoBlock0AuthorityProof proof;

static void reset_native(void)
{
	if (cluster_undo_block0_backend_has_resources())
		resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, resource_release_arg);
	UT_ASSERT_EQ(borrow_count, 0);
	fresh_block0_region(2);
	cluster_enabled = true; native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	missing = false; current = release_ok = true;
	acquire_count = release_count = 0;
	logical = make_key(1, 1); root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 77, 1);
	proof = make_live_proof(1, 3);
	memset(&selected, 0, sizeof(selected));
	selected.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	selected.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	selected.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	selected.system_identifier = 77; selected.database_incarnation = 1;
	selected.storage_uuid[0] = 1; selected.redo_branch_uuid[0] = 2;
	selected.redo_branch_generation = selected.redo_timeline = 1;
	selected.creation_generation = 3; selected.registration_generation = 4;
	selected.object_id = 1;
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, smgr_image));
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(smgr_image, BLCKSZ, &selected));
	UT_ASSERT(cluster_undo_segment_header_seal(smgr_image));
}

static ClusterUndoBlock0Result admit(ClusterUndoBlock0Pin *pin, char **page)
{
	ClusterUndoBlock0FrameToken token;
	ClusterUndoBlock0Result result;
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	result = cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, pin, page);
	if (token.owned) cluster_undo_block0_frame_release(&token);
	return result;
}

UT_TEST(resident_borrow_spans_fill_pin_downgrade_and_relock)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native();
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(borrow_count, 1);
	cluster_undo_block0_pin_downgrade(&pin);
	UT_ASSERT_EQ(borrow_count, 1);
	UT_ASSERT_EQ(cluster_undo_block0_lock_content(&pin, NULL, CLUSTER_UNDO_BLOCK0_SHARED, &page), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(acquire_count, 1);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT_EQ(borrow_count, 0); UT_ASSERT_EQ(release_count, 1);
}
UT_TEST(resident_new_pin_rejects_stale_cached_file_without_io)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native(); UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_unpin(&pin); selected.creation_generation++; page = NULL;
	UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &root, NULL, CLUSTER_UNDO_BLOCK0_SHARED, &proof, &pin, &page), CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_NULL(page); UT_ASSERT_EQ(borrow_count, 0); UT_ASSERT_EQ(smgr_read_calls, 1);
}
UT_TEST(resident_missing_selection_refuses_before_fill_or_pin)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native(); missing = true;
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_NULL(page); UT_ASSERT_EQ(smgr_read_calls, 0); UT_ASSERT_EQ(borrow_count, 0);
	missing = false; UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_unpin(&pin); missing = true;
	UT_ASSERT_EQ(cluster_undo_block0_reserve(&logical, &root, &proof, &pin), CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(pin.slot, -1); UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_failed_fill_returns_borrow_and_frame)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native(); smgr_read_ok = false;
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_IO_ERROR);
	UT_ASSERT_EQ(borrow_count, 0); UT_ASSERT_EQ(acquire_count, 1); UT_ASSERT_EQ(release_count, 1);
	smgr_read_ok = true; smgr_image[3100] ^= 1;
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_relock_current_refusal_cleans_exact_reservation)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native(); UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_pin_downgrade(&pin); current = false;
	UT_ASSERT_EQ(cluster_undo_block0_lock_content(&pin, NULL, CLUSTER_UNDO_BLOCK0_EXCLUSIVE, &page), CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_NULL(page); UT_ASSERT_EQ(pin.slot, -1); UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_resource_owner_releases_locked_and_reserved_borrows)
{
	ClusterUndoBlock0Pin first, second; char *page;
	reset_native(); UT_ASSERT_EQ(admit(&first, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_pin_downgrade(&first);
	UT_ASSERT_EQ(cluster_undo_block0_pin(&logical, &root, NULL, CLUSTER_UNDO_BLOCK0_SHARED, &proof, &second, &page), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(borrow_count, 2);
	resource_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, resource_release_arg);
	UT_ASSERT_EQ(borrow_count, 0); UT_ASSERT_EQ(release_count, 2);
	UT_ASSERT(!cluster_undo_block0_backend_has_resources());
}
UT_TEST(resident_flush_rejects_different_reference_before_wal_or_write)
{
	ClusterUndoBlock0Pin pin; char *page; PGAlignedBlock successor;
	reset_native(); UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor.data, page, BLCKSZ); successor.data[3100] ^= 1;
	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0) {
		cluster_undo_block0_flush_sync(&pin, successor.data, 100, false);
		UT_ASSERT(false);
	}
	wal_error_armed = false;
	UT_ASSERT_EQ(xlog_flush_calls, 0); UT_ASSERT_EQ(smgr_write_calls, 0);
	UT_ASSERT_EQ(borrow_count, 1);
	cluster_undo_block0_unpin(&pin); UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_selected_mode_does_not_fall_back_when_guc_off)
{
	ClusterUndoBlock0Pin pin; char *page;
	reset_native(); cluster_enabled = false; missing = true;
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(smgr_read_calls, 0);
	native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_unpin(&pin); UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_content_lock_error_and_explicit_reservation_release)
{
	ClusterUndoBlock0Pin pin; char *page;
	volatile bool caught = false;
	reset_native(); UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_pin_downgrade(&pin);
	cluster_undo_block0_release_reservation(&pin);
	UT_ASSERT_EQ(borrow_count, 0); UT_ASSERT_EQ(release_count, 1);
	lwlock_throw_on_call = lwlock_acquire_calls + 2;
	PG_TRY();
	{
		(void) cluster_undo_block0_pin(&logical, &root, NULL,
			CLUSTER_UNDO_BLOCK0_SHARED, &proof, &pin, &page);
	}
	PG_CATCH(); { caught = true; lwlock_throw_on_call = 0; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(borrow_count, 0);
	UT_ASSERT_EQ(release_count, 2); UT_ASSERT_EQ(pin.slot, -1);
}
UT_TEST(resident_bad_successor_is_not_the_only_flush_guard)
{
	ClusterUndoBlock0Pin pin; char *page; PGAlignedBlock successor;
	reset_native(); UT_ASSERT_EQ(admit(&pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	memcpy(successor.data, page, BLCKSZ); current = false;
	wal_error_armed = true;
	if (sigsetjmp(wal_error_jump, 1) == 0) {
		cluster_undo_block0_flush_sync(&pin, successor.data, 100, false);
		UT_ASSERT(false);
	}
	wal_error_armed = false;
	UT_ASSERT_EQ(xlog_flush_calls, 0); UT_ASSERT_EQ(smgr_write_calls, 0);
	current = true;
	cluster_undo_block0_flush_sync(&pin, successor.data, 100, false);
	UT_ASSERT_EQ(xlog_flush_calls, 1); UT_ASSERT_EQ(smgr_write_calls, 1);
	UT_ASSERT_EQ(borrow_count, 1);
	cluster_undo_block0_unpin(&pin); UT_ASSERT_EQ(borrow_count, 0);
}
UT_TEST(resident_release_failure_keeps_duty_and_reports_panic)
{
	pid_t child;
	int status;
	reset_native(); child = fork(); UT_ASSERT(child >= 0);
	if (child == 0) {
		ClusterUndoBlock0Pin pin; char *page;
		volatile bool caught = false;
		if (admit(&pin, &page) != CLUSTER_UNDO_BLOCK0_OK) _exit(2);
		release_ok = false; wal_error_armed = true;
		if (sigsetjmp(wal_error_jump, 1) == 0)
			cluster_undo_block0_unpin(&pin);
		else caught = true;
		/* PANIC is terminal. Inspect only; do not retry local pin cleanup. */
		_exit(caught && borrow_count == 1 && release_count == 0 &&
			  cluster_undo_block0_backend_has_resources() ? 0 : 1);
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status)); UT_ASSERT_EQ(WEXITSTATUS(status), 0);
}
int main(void)
{
	UT_PLAN(11);
	UT_RUN(resident_borrow_spans_fill_pin_downgrade_and_relock);
	UT_RUN(resident_new_pin_rejects_stale_cached_file_without_io);
	UT_RUN(resident_missing_selection_refuses_before_fill_or_pin);
	UT_RUN(resident_failed_fill_returns_borrow_and_frame);
	UT_RUN(resident_relock_current_refusal_cleans_exact_reservation);
	UT_RUN(resident_resource_owner_releases_locked_and_reserved_borrows);
	UT_RUN(resident_flush_rejects_different_reference_before_wal_or_write);
	UT_RUN(resident_selected_mode_does_not_fall_back_when_guc_off);
	UT_RUN(resident_content_lock_error_and_explicit_reservation_release);
	UT_RUN(resident_bad_successor_is_not_the_only_flush_guard);
	UT_RUN(resident_release_failure_keeps_duty_and_reports_panic);
	reset_native(); cluster_undo_block0_shmem_detach(); free(block0_region);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
