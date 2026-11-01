/* Actual native rmgr/decoder/preparation with finite syscall/control providers. */
#include "postgres.h"
#include <setjmp.h>
#include "access/clog_file.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"

static void selected_redo_io(void);
static void selected_redo_jump(jmp_buf env, int value) pg_attribute_noreturn();
#define UNDO_NATIVE_TT_REDO_SELECTED_TEST
#define UNDO_NATIVE_TT_REDO_IO selected_redo_io
#define longjmp selected_redo_jump
#define main native_lifecycle_original_main
int native_lifecycle_original_main(void);
#include "test_cluster_undo_native_lifecycle.c"
#undef main
#undef longjmp

bool cluster_enabled = true;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
static ClusterControlNativeModeV2 selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
static ClusterRecoveryFileIdentityV2 selected;
static ClusterControlNativeFileBorrowV2 *held;
static unsigned acquired, current_calls, released, drift_on, throw_on;
static bool missing, release_ok = true, io_protected;

void *palloc0(Size size)
{
	void *result = calloc(1, size);

	if (result == NULL) abort();
	return result;
}
void pfree(void *pointer)
{
	free(pointer);
}
static void selected_redo_jump(jmp_buf env, int value)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, value);
	longjmp(env, value);
}
void pg_re_throw(void)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	if (expect_panic) longjmp(panic_jump, 1);
	abort();
}
static void selected_redo_io(void)
{
	if ((cluster_enabled || selected_mode != CLUSTER_NATIVE_FILES_UNKNOWN_V2) && held == NULL)
		io_protected = false;
}
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return selected_mode; }
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *fresh)
{
	UT_ASSERT_EQ(key->kind, CLUSTER_RECOVERY_FILE_UNDO_V2);
	UT_ASSERT_EQ(key->origin, 2); UT_ASSERT_EQ(key->object_id, 513);
	UT_ASSERT_EQ(key->database_oid, 0); UT_ASSERT_EQ(key->tablespace_oid, 0);
	UT_ASSERT(held == NULL); UT_ASSERT_EQ(fresh->private_self, 0);
	acquired++;
	if (missing) return false;
	fresh->expected = selected; fresh->table_generation = 41; fresh->startup_generation = 1;
	fresh->private_self = (uintptr_t) fresh; fresh->private_pid = 1234; held = fresh;
	return true;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT(held == borrow); UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	current_calls++;
	if (current_calls == throw_on) siglongjmp(*PG_exception_stack, 1);
	return current_calls != drift_on;
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	UT_ASSERT(held == borrow); UT_ASSERT_EQ(borrow->private_self, (uintptr_t)borrow);
	UT_ASSERT_EQ(closes, opens - (fail_open ? 1 : 0));
	if (!release_ok) return false;
	released++; held = NULL; memset(borrow, 0, sizeof(*borrow)); return true;
}
static void selected_base(uint8 status)
{
	native_v2_base((UINT64_C(7) << 32) | 701, status);
	held = NULL; acquired = current_calls = released = drift_on = throw_on = 0;
	missing = false; release_ok = io_protected = true;
	cluster_enabled = true; selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	memset(&selected, 0, sizeof(selected));
	selected.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	selected.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	selected.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	selected.system_identifier = 77; selected.database_incarnation = 1;
	selected.storage_uuid[0] = 1; selected.redo_branch_uuid[0] = 2;
	selected.redo_branch_generation = 1; selected.redo_timeline = 1;
	selected.creation_generation = 3; selected.registration_generation = 4;
	selected.origin = 2; selected.object_id = 513;
	/* The legacy fixture's sentinel lies inside the newly explicit reference. */
	((UndoSegmentHeaderDataV2 *)disk.data)->_reserved[7] = 0;
	UT_ASSERT(cluster_tt_file_reference_prepare_v2(disk.data, BLCKSZ, &selected));
	UT_ASSERT(cluster_undo_segment_header_seal(disk.data)); durable = disk;
}
static bool selected_bind(void)
{
	xl_undo_tt_identity_v2 record = native_v2_identity((UINT64_C(7) << 32) | 701);
	return replay_v2_record(XLOG_UNDO_TT_SLOT_BIND, &record, sizeof(record));
}

UT_TEST(selected_redo_keeps_identity_through_write_sync_readback_and_close)
{
	selected_base(TT_SLOT_UNUSED);
	UT_ASSERT(selected_bind()); UT_ASSERT(io_protected);
	UT_ASSERT_EQ(acquired, 1); UT_ASSERT_EQ(released, 1); UT_ASSERT(held == NULL);
	UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(applies, 1); UT_ASSERT_EQ(skips, 0);
	UT_ASSERT_EQ(((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots[4].original_full_xid,
		(UINT64_C(7) << 32) | 701);
	UT_ASSERT(memcmp(disk.data, durable.data, BLCKSZ) == 0);
	UT_ASSERT(cluster_tt_file_reference_matches_v2(disk.data, BLCKSZ, &selected));
}
UT_TEST(selected_redo_missing_and_mismatched_identity_refuse_before_effects)
{
	unsigned variant;
	for (variant = 0; variant < 4; variant++) {
		PGAlignedBlock before;
		selected_base(TT_SLOT_UNUSED);
		if (variant == 0) missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected.registration_generation++;
		if (variant == 3) drift_on = 1;
		before = disk;
		UT_ASSERT(!selected_bind()); UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		UT_ASSERT_EQ(memcmp(before.data, disk.data, BLCKSZ), 0);
		UT_ASSERT(held == NULL); UT_ASSERT_EQ(released, variant == 0 ? 0 : 1);
		if (variant == 0 || variant == 3) UT_ASSERT_EQ(opens, 0);
	}
}
UT_TEST(selected_redo_rechecks_each_original_io_boundary)
{
	unsigned at;
	for (at = 2; at <= 6; at++) {
		selected_base(TT_SLOT_UNUSED); drift_on = at;
		UT_ASSERT(!selected_bind()); UT_ASSERT(io_protected); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(closes, 1);
		UT_ASSERT_EQ(applies + skips, 0);
		UT_ASSERT_EQ(writes, at <= 4 ? 0 : 1);
	}
}
UT_TEST(selected_redo_error_returns_original_fd_and_borrow_once)
{
	unsigned at;
	for (at = 1; at <= 6; at++) {
		selected_base(TT_SLOT_UNUSED); throw_on = at;
		UT_ASSERT(!selected_bind()); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(released, 1); UT_ASSERT_EQ(closes, opens);
		UT_ASSERT_EQ(applies + skips, 0);
	}
}
UT_TEST(selected_redo_present_and_stale_sync_under_same_identity)
{
	unsigned stale;
	for (stale = 0; stale < 2; stale++) {
		selected_base(TT_SLOT_ACTIVE);
		if (stale) {
			((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count++;
			UT_ASSERT(cluster_undo_segment_header_seal(disk.data)); durable = disk;
		}
		UT_ASSERT(selected_bind()); UT_ASSERT(io_protected); UT_ASSERT(held == NULL);
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(applies, 0); UT_ASSERT_EQ(skips, stale); UT_ASSERT_EQ(released, 1);
	}
}
UT_TEST(selected_redo_io_failure_preserves_effects_without_completion)
{
	unsigned variant;
	for (variant = 0; variant < 5; variant++) {
		selected_base(TT_SLOT_UNUSED);
		fail_open = variant == 0; short_read_number = variant == 1 ? 1 : 0;
		fail_write = variant == 2; fail_sync = variant == 3; corrupt_post_read = variant == 4;
		UT_ASSERT(!selected_bind()); UT_ASSERT(held == NULL); UT_ASSERT(io_protected);
		UT_ASSERT_EQ(applies + skips, 0); UT_ASSERT_EQ(released, 1);
		UT_ASSERT_EQ(writes, variant >= 3 ? 1 : 0);
	}
}
UT_TEST(selected_redo_managed_mode_is_independent_of_cluster_guc)
{
	selected_base(TT_SLOT_UNUSED); cluster_enabled = false;
	UT_ASSERT(selected_bind()); UT_ASSERT(io_protected); UT_ASSERT_EQ(acquired, 1);
	selected_base(TT_SLOT_UNUSED); selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	missing = true; UT_ASSERT(!selected_bind()); UT_ASSERT_EQ(opens, 0);
	selected_base(TT_SLOT_UNUSED); cluster_enabled = false;
	selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	UT_ASSERT(selected_bind()); UT_ASSERT_EQ(acquired + released, 0);
}
UT_TEST(selected_redo_failed_cleanup_keeps_the_original_duty)
{
	unsigned variant;
	for (variant = 0; variant < 2; variant++) {
		selected_base(TT_SLOT_UNUSED);
		fail_close = variant == 0; release_ok = variant == 0;
		UT_ASSERT(!selected_bind()); UT_ASSERT(held != NULL); UT_ASSERT_EQ(released, 0);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
		/* PANIC is caught only by this finite fixture. A backend would stop;
		 * it must not return success or erase an unreturned borrow. */
	}
}
int main(void)
{
	UT_PLAN(8);
	UT_RUN(selected_redo_keeps_identity_through_write_sync_readback_and_close);
	UT_RUN(selected_redo_missing_and_mismatched_identity_refuse_before_effects);
	UT_RUN(selected_redo_rechecks_each_original_io_boundary);
	UT_RUN(selected_redo_error_returns_original_fd_and_borrow_once);
	UT_RUN(selected_redo_present_and_stale_sync_under_same_identity);
	UT_RUN(selected_redo_io_failure_preserves_effects_without_completion);
	UT_RUN(selected_redo_managed_mode_is_independent_of_cluster_guc);
	UT_RUN(selected_redo_failed_cleanup_keeps_the_original_duty);
	UT_DONE(); return ut_failed_count != 0;
}
