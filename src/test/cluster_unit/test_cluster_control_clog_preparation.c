/* Actual ROOT/registry batch and shared table. Only original authority leaves
 * are controlled by the reused fixture; no live allocator is activated. */
#define TEST_CONTROL_CLOG_PREPARATION 1
#include "test_cluster_control_native_reservations.c"
#undef main

static ClusterControlNativeResultV2
prepare(uint64 first, uint64 last, ClusterControlNativeClogRangeV2 **out)
{
	return cluster_control_native_files_reserve_clog_range_locked_v2(0, first, last, out);
}
UT_TEST(full_gap_preserves_old_batches_and_creates_control_identities)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(7, 12, &r), 0);
	UT_ASSERT(r && r->count == 6 && r->first_segment == 7 && r->last_segment == 12);
	if (!r) {
		batch_end();
		return;
	}
	UT_ASSERT_EQ(r->startup_generation, 40);
	UT_ASSERT(!memcmp(&r->config, &expected_config, sizeof(expected_config)));
	UT_ASSERT_EQ(root_renames, 2);
	for (unsigned i = 0; i < 6; i++) {
		const ClusterControlNativeClogEntryV2 *v = &r->entries[i];
		UT_ASSERT(v->reserved && v->expected.kind == CLUSTER_RECOVERY_FILE_CLOG_V2);
		UT_ASSERT_EQ(v->expected.object_id, 7 + i);
		UT_ASSERT_EQ(v->expected.origin, 0);
		UT_ASSERT_EQ(v->expected.database_oid, 0);
		UT_ASSERT_EQ(v->expected.tablespace_oid, 0);
		if (i < 2) {
			UT_ASSERT(identity_same(&v->expected, &reserved[i].expected));
			UT_ASSERT(!memcmp(v->batch_uuid, reserved[i].batch_uuid, 16));
		} else {
			UT_ASSERT_EQ(v->expected.creation_generation, 40);
			UT_ASSERT_EQ(v->expected.registration_generation, 16);
			UT_ASSERT_EQ(v->expected.system_identifier, 11);
			UT_ASSERT_EQ(v->expected.database_incarnation, 12);
			UT_ASSERT_EQ(v->expected.redo_branch_generation, 13);
			UT_ASSERT_EQ(v->expected.redo_timeline, 1);
			UT_ASSERT(!memcmp(v->expected.storage_uuid, reg.storage_uuid, 16));
			UT_ASSERT(!memcmp(v->expected.redo_branch_uuid, reg.redo_branch_uuid, 16));
			UT_ASSERT(!memcmp(v->batch_uuid, r->entries[2].batch_uuid, 16));
			UT_ASSERT(memcmp(v->batch_uuid, reserved[0].batch_uuid, 16));
		}
		UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
						 &r->config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, r->startup_generation,
						 v->batch_uuid, &v->expected, &b),
					 0);
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
	}
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.reservation_count, 6);
	UT_ASSERT_EQ(info.table_generation, r->table_generation);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 7, 12), 0);
	cluster_control_native_clog_range_destroy_v2(&r);
	UT_ASSERT(r == NULL);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 7, 12), 0);
	batch_end();
}
UT_TEST(all_present_is_idempotent_and_does_not_reissue_identity)
{
	ClusterControlNativeClogRangeV2 *a = NULL, *b = NULL;
	unsigned writes;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(7, 11, &a), 0);
	writes = root_renames;
	UT_ASSERT_EQ(prepare(7, 11, &b), 0);
	if (!a || !b) {
		batch_end();
		return;
	}
	UT_ASSERT_EQ(root_renames, writes);
	UT_ASSERT_EQ(a->table_generation, b->table_generation);
	UT_ASSERT(!memcmp(a->entries, b->entries, 5 * sizeof(a->entries[0])));
	cluster_control_native_clog_range_destroy_v2(&a);
	cluster_control_native_clog_range_destroy_v2(&b);
	batch_end();
}
UT_TEST(preparation_precedes_cf_family_locks_and_output_aliases)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	unsigned reads;
	table_reset();
	owner_begin();
	reads = dependencies;
	cf_held = false;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	cf_held = true;
	batch_exclusive = false;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	batch_exclusive = true;
	for (unsigned i = 0; i < 5; i++) {
		LWLock *locks[] = { ControlFileLock, RelationMappingLock, XidGenLock, XactSLRULock,
							XactTruncationLock };
		held_family_lock = locks[i];
		UT_ASSERT(prepare(9, 12, &r) != 0);
		held_family_lock = NULL;
	}
	CritSectionCount = 1;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	CritSectionCount = 0;
	UT_ASSERT(prepare(9, 12, (void *)native_files) != 0);
	UT_ASSERT(prepare(12, 9, &r) != 0);
	UT_ASSERT(prepare(0, UINT64_MAX, &r) != 0);
	UT_ASSERT(cluster_control_native_files_reserve_clog_range_locked_v2(1, 9, 12, &r) != 0);
	UT_ASSERT_EQ(dependencies, reads);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(r == NULL);
	batch_end();
}
UT_TEST(capacity_refuses_complete_range_before_any_persistent_effect)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(0, CLUSTER_NATIVE_FILES_CAPACITY_V2, &r),
				 CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(r == NULL);
	batch_end();
}
UT_TEST(actual_owner_claim_and_anchor_refuse_without_writes)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	table_reset();
	owner_begin();
	owner_ok = false;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	owner_ok = true;
	claim_ok = false;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	claim_ok = true;
	anchor_ok = false;
	UT_ASSERT(prepare(9, 12, &r) != 0);
	anchor_ok = true;
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(r == NULL);
	batch_end();
}
UT_TEST(live_native_borrow_refuses_missing_reservation_before_mutation)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 40), 0);
	UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(&expected_config, 65536, 40,
																		reserved[0].batch_uuid,
																		&reserved[0].expected, &b),
				 0);
	UT_ASSERT_EQ(prepare(9, 12, &r), CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(r == NULL);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	batch_end();
}
UT_TEST(post_registration_failure_keeps_reservations_without_output)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileReservationV1 v;
	table_reset();
	owner_begin();
	/* Primary rename is completed by the real writer, then current/owner fails. */
	fail_rename = 0;
	rename_hook = lose_owner;
	UT_ASSERT_EQ(prepare(9, 12, &r), CLUSTER_NATIVE_FILES_REGISTRATION_DEBT_V2);
	UT_ASSERT(r == NULL);
	UT_ASSERT_EQ(root_renames, 1);
	owner_ok = true;
	UT_ASSERT(inventory_read(&s));
	/* Immutable next registry material remains even though ROOT is still old. */
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	UT_ASSERT(cluster_control_root_registry_reservation_at_v4(s, 0, &v));
	UT_ASSERT(identity_same(&v.expected, &reserved[0].expected));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	batch_end();
}
static unsigned error_rename_number;
static void
error_after_primary(void)
{
	if (root_renames < error_rename_number)
		rename_hook = error_after_primary;
	else
		config_error = true;
}
UT_TEST(error_after_persistent_primary_retains_whole_original_registration)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *s = NULL;
	volatile bool caught = false;
	table_reset();
	owner_begin();
	error_rename_number = 2;
	rename_hook = error_after_primary;
	PG_TRY();
	{
		(void)prepare(9, 12, &r);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	rename_hook = NULL;
	UT_ASSERT(caught && r == NULL);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 6);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	batch_end();
}
UT_TEST(whole_gap_above_pin_capacity_publishes_once_and_reports_cost)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	struct timespec a, b;
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	owner_begin();
	clock_gettime(CLOCK_MONOTONIC, &a);
	UT_ASSERT_EQ(prepare(7, 81, &r), 0);
	clock_gettime(CLOCK_MONOTONIC, &b);
	if (!r) {
		batch_end();
		return;
	}
	UT_ASSERT_EQ(r->count, 75);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 7, 81), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	printf(
		"# actual_control_reserve75_ns=%llu rows=%llu borrowed=%llu network_CF_unmeasured\n",
		(unsigned long long)((b.tv_sec - a.tv_sec) * UINT64_C(1000000000) + b.tv_nsec - a.tv_nsec),
		(unsigned long long)(info.count + info.reservation_count),
		(unsigned long long)info.borrowed);
	cluster_control_native_clog_range_destroy_v2(&r);
	batch_end();
}
UT_TEST(available_segments_keep_identity_and_never_gain_creator_batch)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterControlRegistryBatchV4 c = { 0 };
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	uint8 ref[128], zero[16] = { 0 };
	table_reset();
	owner_begin();
	for (unsigned i = 0; i < 2; i++) {
		receipts[i] = inventory[0];
		receipts[i].identity = reserved[i].expected;
		receipts[i].copy_count = 1;
		receipts[i].copy_index = 0;
		receipts[i].synced_bytes = 16448;
	}
	c.operation = CLUSTER_CONTROL_REGISTRY_COMPLETE_V4;
	memset(c.batch_uuid, 0x71, 16);
	c.receipts = receipts;
	c.receipt_count = 2;
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, ref), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT_EQ(prepare(7, 10, &r), 0);
	if (!r) {
		batch_end();
		return;
	}
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT(!r->entries[i].reserved);
		UT_ASSERT(identity_same(&r->entries[i].expected, &reserved[i].expected));
		UT_ASSERT(!memcmp(r->entries[i].batch_uuid, zero, 16));
		UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_CLOG_V2,
																	  0, 0, 7 + i, &b),
					 0);
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
	}
	UT_ASSERT(r->entries[2].reserved && r->entries[3].reserved);
	UT_ASSERT_EQ(r->entries[2].expected.registration_generation, 17);
	cluster_control_native_clog_range_destroy_v2(&r);
	batch_end();
}
UT_TEST(canonical_registry_budget_refuses_before_one_shot_publication)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(0, CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2 - 1, &r),
				 CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT(r == NULL);
	UT_ASSERT_EQ(root_renames, 0);
	batch_end();
}
UT_TEST(another_creator_epoch_cannot_be_reissued_or_projected)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	uint8 ref[128];
	table_reset();
	owner_begin();
	reserved[0].owner_generation++;
	reserved[1].owner_generation++;
	batch_registry.available.coverage.control_generation++;
	batch_registry.available.coverage.registration_generation++;
	root.header.file_txn_seq++;
	UT_ASSERT(cluster_recovery_registry_create_v3(selection_dir, &batch_registry, 65536, ref));
	UT_ASSERT(cluster_control_root_reservations_encode_v4(&root, &batch_registry, ref,
														  selection_bytes, 66560));
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
	UT_ASSERT(prepare(6, 8, &r) != 0);
	UT_ASSERT(r == NULL);
	UT_ASSERT_EQ(root_renames, 0);
	batch_end();
}

/* Lose only the local projection; the selected ROOT and immutable registry
 * remain on disk. This is not a server restart or a file-creation receipt. */
UT_TEST(durable_full_gap_rebuild_preserves_every_creator_identity)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlNativeFileBorrowV2 *borrows;
	ClusterControlNativeTableInfoV2 info;
	unsigned writes;

	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(7, 81, &r), 0);
	if (!r) {
		batch_end();
		return;
	}
	writes = root_renames;
	borrows = palloc0(r->count * sizeof(*borrows));
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
	UT_ASSERT(cluster_control_native_files_clog_range_v2(0, 7, 81) != 0);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &r->config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
					 r->startup_generation), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 7, 81), 0);
	for (uint64 i = 0; i < r->count; i++) {
		ClusterControlNativeFileBorrowV2 ordinary = { 0 }, zero = { 0 };
		const ClusterControlNativeClogEntryV2 *v = &r->entries[i];
		UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
						 CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 7 + i, &ordinary),
					 CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
		UT_ASSERT(!memcmp(&ordinary, &zero, sizeof(zero)));
		UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
						 &r->config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
						 r->startup_generation, v->batch_uuid, &v->expected, &borrows[i]), 0);
		UT_ASSERT(identity_same(&borrows[i].expected, &v->expected));
	}
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.borrowed, 75);
	UT_ASSERT_EQ(info.reservation_count, 75);
	UT_ASSERT_EQ(root_renames, writes);
	for (uint64 i = 0; i < r->count; i++) {
		UT_ASSERT(cluster_control_native_file_current_v2(&borrows[i]));
		UT_ASSERT(cluster_control_native_file_release_v2(&borrows[i]));
	}
	pfree(borrows);
	cluster_control_native_clog_range_destroy_v2(&r);
	batch_end();
}

UT_TEST(persistent_error_debt_rebuilds_without_retrying_registration)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileReservationV1 saved[6];
	ClusterControlNativeTableInfoV2 info;
	volatile bool caught = false;
	unsigned writes;

	table_reset();
	owner_begin();
	error_rename_number = 2;
	rename_hook = error_after_primary;
	PG_TRY();
	{
		(void)prepare(9, 12, &r);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	rename_hook = NULL;
	UT_ASSERT(caught && r == NULL);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 6);
	for (unsigned i = 0; i < 6; i++)
		UT_ASSERT(cluster_control_root_registry_reservation_at_v4(s, i, &saved[i]));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	writes = root_renames;
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 40), 0);
	for (unsigned i = 0; i < 6; i++) {
		ClusterControlNativeFileBorrowV2 b = { 0 };
		UT_ASSERT_EQ(saved[i].state, CLUSTER_RECOVERY_FILE_RESERVED_V1);
		UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
						 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 40,
						 saved[i].batch_uuid, &saved[i].expected, &b), 0);
		UT_ASSERT(identity_same(&b.expected, &saved[i].expected));
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
	}
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.reservation_count, 6);
	UT_ASSERT_EQ(info.borrowed, 0);
	UT_ASSERT_EQ(root_renames, writes);
	batch_end();
}

UT_TEST(wrap_halves_rebuild_together_before_any_creator_borrow)
{
	ClusterControlNativeClogRangeV2 *halves[2] = { NULL, NULL };
	ClusterControlNativeFileBorrowV2 borrows[2] = { 0 };
	ClusterControlNativeTableInfoV2 info;
	unsigned writes;

	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(4095, 4095, &halves[0]), 0);
	UT_ASSERT_EQ(prepare(0, 2, &halves[1]), 0);
	if (!halves[0] || !halves[1]) {
		cluster_control_native_clog_range_destroy_v2(&halves[0]);
		cluster_control_native_clog_range_destroy_v2(&halves[1]);
		batch_end();
		return;
	}
	writes = root_renames;
	UT_ASSERT_EQ(writes, 4);
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 40), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 4095, 4095), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 0, 2), 0);
	for (unsigned i = 0; i < 2; i++) {
		const ClusterControlNativeClogEntryV2 *v = &halves[i]->entries[0];
		UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
						 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 40,
						 v->batch_uuid, &v->expected, &borrows[i]), 0);
		UT_ASSERT(identity_same(&borrows[i].expected, &v->expected));
	}
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.borrowed, 2);
	UT_ASSERT_EQ(info.reservation_count, 6);
	UT_ASSERT_EQ(root_renames, writes);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT(cluster_control_native_file_release_v2(&borrows[i]));
		cluster_control_native_clog_range_destroy_v2(&halves[i]);
	}
	batch_end();
}

UT_TEST(cancelled_batch_cannot_return_through_a_fresh_projection)
{
	ClusterControlNativeClogRangeV2 *r = NULL;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterControlRegistryBatchV4 change = { 0 };
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	ClusterControlNativeTableInfoV2 info;
	uint8 ref[128];
	unsigned writes;

	table_reset();
	owner_begin();
	UT_ASSERT_EQ(prepare(9, 12, &r), 0);
	if (!r) {
		batch_end();
		return;
	}
	change.operation = CLUSTER_CONTROL_REGISTRY_CANCEL_V4;
	memcpy(change.batch_uuid, r->entries[0].batch_uuid, 16);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &change, ref), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	writes = root_renames;
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 40), 0);
	for (uint64 i = 0; i < r->count; i++) {
		const ClusterControlNativeClogEntryV2 *v = &r->entries[i];
		UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
					  &r->config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
					  r->startup_generation, v->batch_uuid, &v->expected, &b) != 0);
		UT_ASSERT(!memcmp(&b, &zero, sizeof(zero)));
	}
	UT_ASSERT(cluster_control_native_files_clog_range_v2(0, 9, 12) != 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.reservation_count, 2);
	UT_ASSERT_EQ(info.borrowed, 0);
	UT_ASSERT_EQ(root_renames, writes);
	cluster_control_native_clog_range_destroy_v2(&r);
	batch_end();
}

int
main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "--original"))
		return original_native_reservations_main(1, argv);
	UT_RUN(full_gap_preserves_old_batches_and_creates_control_identities);
	UT_RUN(all_present_is_idempotent_and_does_not_reissue_identity);
	UT_RUN(preparation_precedes_cf_family_locks_and_output_aliases);
	UT_RUN(capacity_refuses_complete_range_before_any_persistent_effect);
	UT_RUN(actual_owner_claim_and_anchor_refuse_without_writes);
	UT_RUN(live_native_borrow_refuses_missing_reservation_before_mutation);
	UT_RUN(post_registration_failure_keeps_reservations_without_output);
	UT_RUN(error_after_persistent_primary_retains_whole_original_registration);
	UT_RUN(whole_gap_above_pin_capacity_publishes_once_and_reports_cost);
	UT_RUN(available_segments_keep_identity_and_never_gain_creator_batch);
	UT_RUN(canonical_registry_budget_refuses_before_one_shot_publication);
	UT_RUN(another_creator_epoch_cannot_be_reissued_or_projected);
	UT_RUN(durable_full_gap_rebuild_preserves_every_creator_identity);
	UT_RUN(persistent_error_debt_rebuilds_without_retrying_registration);
	UT_RUN(wrap_halves_rebuild_together_before_any_creator_borrow);
	UT_RUN(cancelled_batch_cannot_return_through_a_fresh_projection);
	UT_DONE();
	return ut_failed_count != 0;
}
