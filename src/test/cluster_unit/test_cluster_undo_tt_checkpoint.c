/* Complete selected TT inventory through the real native SMGR sync path.
 * Only the control table, original header-debt recorder and ERROR schedule
 * are finite providers. TT framing, cached FDs, file reads/fsync are real. */
#include "postgres.h"
#include "unit_test.h"

/* This target borrows support, not the publication test dispatcher. */
#undef UT_TEST
#define UT_TEST(name) static void pg_attribute_unused() name(void)
#define UNDO_PUBLICATION_NO_MAIN
#define UNDO_NATIVE_TT_HEADER_RECORDER_TEST
#define UNDO_NATIVE_TT_INVENTORY_TEST
#define UNDO_PUBLICATION_ERROR_PROVIDER
#define UNDO_NATIVE_TT_READ_MAIN native_tt_inventory_original_main
int native_tt_inventory_original_main(void);
#include "test_cluster_undo_native_tt_read.c"
#undef UT_TEST
#define UT_TEST(name) static void name(void)

#define INVENTORY_FILES 82
static ClusterRecoveryFileIdentityV2 inventory_files[INVENTORY_FILES];
static ClusterRecoveryFileReservationV1 inventory_reserved[1];
static ClusterControlNativeInventoryV2 inventory;
static unsigned inventory_gets, inventory_checks, inventory_destroys;
static unsigned inventory_change_on, inventory_throw_on;
static unsigned acquire_change_on, borrow_drift;
static bool inventory_missing, inventory_live;
static unsigned debt_takes;

void errfinish(const char *file, int line, const char *function)
{
	(void)file; (void)line; (void)function;
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	abort();
}

ClusterControlNativeResultV2 cluster_control_native_files_inventory_v2(
	Size budget, ClusterControlNativeInventoryV2 **fresh)
{
	UT_ASSERT(!inventory_live); UT_ASSERT_NOT_NULL(fresh); UT_ASSERT(*fresh == NULL);
	UT_ASSERT(budget >= sizeof(inventory)); inventory_gets++;
	if (inventory_missing) return CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2;
	inventory_live = true; *fresh = &inventory;
	return CLUSTER_NATIVE_FILES_OK_V2;
}

bool cluster_control_native_files_inventory_current_v2(
	const ClusterControlNativeInventoryV2 *saved)
{
	UT_ASSERT(inventory_live); UT_ASSERT(saved == &inventory); inventory_checks++;
	if (inventory_checks == inventory_throw_on) siglongjmp(*PG_exception_stack, 1);
	/* The real inventory compares the complete original info, including
	 * borrowed. A checkpoint must return its own borrow before this check. */
	return inventory_checks != inventory_change_on && selected_borrows == inventory.info.borrowed;
}

void cluster_control_native_inventory_destroy_v2(ClusterControlNativeInventoryV2 **saved)
{
	if (*saved == NULL) return;
	UT_ASSERT(inventory_live); UT_ASSERT(*saved == &inventory);
	UT_ASSERT_EQ(selected_borrows, 0);
	inventory_live = false; *saved = NULL; inventory_destroys++;
}

bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;

	UT_ASSERT(inventory_live); UT_ASSERT_EQ(borrow->private_self, 0);
	selected_acquires++;
	if (selected_missing) return false;
	for (i = 0; i < inventory.info.count; i++)
		if (key->kind == inventory_files[i].kind && key->origin == inventory_files[i].origin
			&& key->object_id == inventory_files[i].object_id) break;
	UT_ASSERT(i < inventory.info.count); UT_ASSERT_EQ(key->database_oid, 0);
	UT_ASSERT_EQ(key->tablespace_oid, 0); UT_ASSERT(key->origin == cluster_node_id);
	borrow->expected = inventory_files[i];
	borrow->table_generation = inventory.info.table_generation;
	borrow->startup_generation = inventory.info.startup_generation;
	if (selected_acquires == acquire_change_on) {
		if (borrow_drift == 2) borrow->table_generation++;
		else if (borrow_drift == 3) borrow->startup_generation++;
		else borrow->expected.creation_generation++;
	}
	borrow->private_self = (uintptr_t)borrow; borrow->private_pid = 4321;
	UT_ASSERT(selected_borrows < lengthof(selected_handles));
	selected_handles[selected_borrows++] = borrow;
	return true;
}

bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < selected_borrows; i++) if (selected_handles[i] == borrow) break;
	UT_ASSERT(i < selected_borrows); selected_reads++;
	if (selected_reads == selected_throw_on) siglongjmp(*PG_exception_stack, 1);
	return selected_reads != selected_change_on;
}

bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	unsigned i;
	for (i = 0; i < selected_borrows; i++) if (selected_handles[i] == borrow) break;
	UT_ASSERT(i < selected_borrows);
	selected_handles[i] = selected_handles[--selected_borrows];
	memset(borrow, 0, sizeof(*borrow)); return true;
}

uint32 cluster_undo_block0_take_unsynced_headers(uint32 slot, uint32 *segment, uint8 *owner)
{
	(void)slot; (void)segment; (void)owner; debt_takes++; return 0;
}
void cluster_undo_block0_return_unsynced_headers(uint32 slot, uint32 intents)
{ (void)slot; (void)intents; abort(); }

#include "storage/bufmgr.h"
#ifndef UNDO_TT_CHECKPOINT_EXTRACT
#define UNDO_TT_CHECKPOINT_EXTRACT "test_cluster_undo_tt_checkpoint.inc"
#endif
#include UNDO_TT_CHECKPOINT_EXTRACT

static void inventory_reset(unsigned count, unsigned origin)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned i;

	cluster_undo_smgr_fd_cache_reset(); cluster_node_id = origin;
	cluster_enabled = true; selected_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	selected_borrows = selected_reads = selected_acquires = 0;
	selected_missing = false; selected_change_on = selected_throw_on = 0;
	inventory_gets = inventory_checks = inventory_destroys = 0;
	inventory_change_on = inventory_throw_on = acquire_change_on = borrow_drift = 0;
	inventory_missing = inventory_live = false; debt_takes = 0;
	pread_forced_error = basic_open_forced_error = false;
	fsync_calls = fsync_fail_on_call = 0;
	memset(&inventory, 0, sizeof(inventory));
	inventory.info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	inventory.info.table_generation = 41; inventory.info.startup_generation = 1;
	inventory.info.count = count; inventory.info.reservation_count = 1;
	inventory.files = inventory_files; inventory.reservations = inventory_reserved;
	for (i = 0; i < count; i++) {
		ClusterRecoveryFileIdentityV2 *id = &inventory_files[i];
		memset(id, 0, sizeof(*id));
		id->kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
		id->format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
		id->system_identifier = 77; id->database_incarnation = 1;
		id->storage_uuid[0] = 1; id->redo_branch_uuid[0] = 2;
		id->redo_branch_generation = 1; id->redo_timeline = 1;
		id->creation_generation = 3; id->registration_generation = 4;
		id->origin = origin; id->object_id = origin * CLUSTER_UNDO_SEGS_PER_INSTANCE + i + 1;
		UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(id->object_id, origin + 1, page.data));
		UT_ASSERT(cluster_tt_file_reference_prepare_v2(page.data, BLCKSZ, id));
		UT_ASSERT(cluster_undo_segment_header_seal(page.data));
		UT_ASSERT_EQ(cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			origin + 1, id->object_id, path, sizeof(path)), 0);
		write_segment_file(path, page.data, BLCKSZ);
	}
}

static void inventory_finish(void)
{
	char path[MAXPGPATH];
	unsigned i;
	UT_ASSERT(!inventory_live); UT_ASSERT_EQ(selected_borrows, 0);
	cluster_undo_smgr_fd_cache_reset();
	for (i = 0; i < inventory.info.count; i++) {
		UT_ASSERT_EQ(cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			cluster_node_id + 1, inventory_files[i].object_id, path, sizeof(path)), 0);
		remove_if_present(path);
	}
}

static bool inventory_attempt(void)
{
	volatile bool caught = false;
	PG_TRY(); { undo_buf_sync_headers(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY(); return !caught;
}

UT_TEST(checkpoint_syncs_complete_nonresident_local_tt_inventory)
{
	unsigned origin;
	for (origin = 0; origin <= 127; origin += 127) {
		inventory_reset(80, origin);
		UT_ASSERT(inventory_attempt()); UT_ASSERT_EQ(fsync_calls, 80);
		UT_ASSERT_EQ(inventory_gets, 1); UT_ASSERT_EQ(inventory_destroys, 1);
		UT_ASSERT(selected_acquires >= 80); inventory_finish();
	}
}

UT_TEST(checkpoint_skips_other_families_foreign_and_reserved)
{
	inventory_reset(3, 0);
	inventory_files[1].kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	inventory_files[2].origin = 1;
	UT_ASSERT(inventory_attempt()); UT_ASSERT_EQ(fsync_calls, 1);
	inventory_finish();
}

UT_TEST(checkpoint_refuses_unknown_or_unavailable_inventory)
{
	unsigned variant;
	for (variant = 0; variant < 3; variant++) {
		inventory_reset(1, 0);
		if (variant == 0) inventory_missing = true;
		if (variant == 1) selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		if (variant == 2) cluster_node_id = -1;
		UT_ASSERT(!inventory_attempt()); UT_ASSERT_EQ(fsync_calls, 0);
		inventory_finish();
	}
}

UT_TEST(checkpoint_rejects_changed_identity_and_late_inventory)
{
	unsigned variant;
	for (variant = 0; variant < 10; variant++) {
		inventory_reset(1, 0);
		if (variant == 0) acquire_change_on = 1;
		if (variant == 1) inventory_change_on = 1;
		if (variant == 2) inventory_change_on = 2;
		if (variant == 3) selected_change_on = 1;
		if (variant == 4) selected_change_on = 5;
		if (variant == 5 || variant == 6) {
			acquire_change_on = 1; borrow_drift = variant - 3;
		}
		if (variant == 7) inventory_change_on = 3;
		if (variant == 8) inventory_change_on = 4;
		if (variant == 9) selected_change_on = 6;
		UT_ASSERT(!inventory_attempt());
		UT_ASSERT_EQ(inventory_destroys, 1); inventory_finish();
	}
}

UT_TEST(checkpoint_sync_failure_and_error_release_inventory_and_borrows)
{
	unsigned variant;
	for (variant = 0; variant < 5; variant++) {
		inventory_reset(1, 0);
		if (variant == 0) fsync_fail_on_call = 1;
		if (variant == 1) pread_forced_error = true;
		if (variant == 2) selected_throw_on = 2;
		if (variant == 3) inventory_throw_on = 2;
		if (variant == 4) basic_open_forced_error = true;
		UT_ASSERT(!inventory_attempt());
		UT_ASSERT_EQ(inventory_destroys, 1); inventory_finish();
	}
}

UT_TEST(checkpoint_preserves_legacy_and_creation_no_inventory_behavior)
{
	unsigned variant;
	for (variant = 0; variant < 2; variant++) {
		inventory_reset(1, 0);
		selected_mode = variant ? CLUSTER_NATIVE_FILES_CREATION_V2 : CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		cluster_enabled = variant != 0;
		UT_ASSERT(inventory_attempt()); UT_ASSERT_EQ(inventory_gets, 0);
		UT_ASSERT_EQ(fsync_calls, 0); inventory_finish();
	}
}

int main(void)
{
	char path[] = "/tmp/pgrac-tt-checkpoint-XXXXXX";
	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(6);
	UT_RUN(checkpoint_syncs_complete_nonresident_local_tt_inventory);
	UT_RUN(checkpoint_skips_other_families_foreign_and_reserved);
	UT_RUN(checkpoint_refuses_unknown_or_unavailable_inventory);
	UT_RUN(checkpoint_rejects_changed_identity_and_late_inventory);
	UT_RUN(checkpoint_sync_failure_and_error_release_inventory_and_borrows);
	UT_RUN(checkpoint_preserves_legacy_and_creation_no_inventory_behavior);
	UT_ASSERT_EQ(rmdir(publication_dir), 0); UT_DONE(); return ut_failed_count ? 1 : 0;
}
