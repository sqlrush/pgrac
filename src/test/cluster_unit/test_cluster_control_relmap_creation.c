/* Real ROOT/immutable registry/table; only inherited authority/receipt leaves
 * are controlled. This target does not activate CREATE DATABASE. */
#define TEST_CONTROL_CLOG_PREPARATION 1
#define TEST_CONTROL_RECOVERY_CREATE 1
#include "test_cluster_control_native_reservations.c"
#undef main

static ClusterControlNativeResultV2
reserve_target(ClusterControlNativeRelmapCreateV2 **out)
{
	return cluster_control_native_files_reserve_relmap_create_locked_v2(2000, 1663, out);
}
static void
pair_receipts(const ClusterControlNativeRelmapCreateV2 *c, ClusterRecoveryCheckpointFileV2 out[2])
{
	for (unsigned i = 0; i < 2; i++) {
		out[i] = inventory[i];
		out[i].identity = c->expected;
		out[i].synced_bytes = 2304;
		out[i].first_page = 0;
		out[i].page_count = 1;
		out[i].copy_index = i;
		out[i].copy_count = 2;
	}
}
static bool
created_selection(ClusterControlRelmapSelectionV2 **s)
{
	return cluster_control_root_relmap_selection_read_locked_v4(
			   &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, s)
		   == 0;
}
UT_TEST(independent_target_persists_before_creator_borrow)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 id;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) {
		batch_end();
		return;
	}
	UT_ASSERT_EQ(c->expected.kind, CLUSTER_RECOVERY_FILE_RELMAP_V2);
	UT_ASSERT_EQ(c->expected.database_oid, 2000);
	UT_ASSERT_EQ(c->expected.tablespace_oid, 1663);
	UT_ASSERT_EQ(c->expected.origin, 0);
	UT_ASSERT_EQ(c->expected.object_id, 0);
	UT_ASSERT_EQ(c->expected.creation_generation, 40);
	UT_ASSERT_EQ(c->expected.registration_generation, 16);
	UT_ASSERT_EQ(c->expected.system_identifier, 11);
	UT_ASSERT_EQ(c->expected.database_incarnation, 12);
	UT_ASSERT_EQ(c->expected.redo_timeline, 1);
	UT_ASSERT(!identity_same(&c->expected, &inventory[2].identity));
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(created_selection(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 3);
	UT_ASSERT(cluster_control_root_reserved_file_locked_v4(s, c->batch_uuid, &c->expected, &id));
	UT_ASSERT(identity_same(&id, &c->expected));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_RELMAP_V2,
																  2000, 1663, 0, &b),
				 CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
					 &c->control.config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
					 c->startup_generation, c->batch_uuid, &c->expected, &b),
				 0);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.table_generation, c->table_generation);
	cluster_control_native_relmap_create_destroy_v2(&c);
	UT_ASSERT(c == NULL);
	UT_ASSERT(created_selection(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 3);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	batch_end();
}
UT_TEST(complete_exact_pair_makes_only_target_available)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 id;
	struct timespec a, z;
	table_reset();
	owner_begin();
	clock_gettime(CLOCK_MONOTONIC, &a);
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) {
		batch_end();
		return;
	}
	pair_receipts(c, r);
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2), 0);
	clock_gettime(CLOCK_MONOTONIC, &z);
	UT_ASSERT_EQ(root_renames, 4);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.table_generation, c->table_generation + 1);
	UT_ASSERT_EQ(info.count, 3);
	UT_ASSERT_EQ(info.reservation_count, 2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_RELMAP_V2,
																  2000, 1663, 0, &b),
				 0);
	UT_ASSERT(identity_same(&b.expected, &c->expected));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(created_selection(&s));
	UT_ASSERT(cluster_control_root_relmap_selection_lookup_v4(s, 2000, 1663, 0, &id));
	UT_ASSERT(identity_same(&id, &c->expected));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
	UT_ASSERT_EQ(root_renames, 4);
	printf(
		"# reserve_and_complete_pair_ns=%llu table_rows=%llu CF_network_unmeasured\n",
		(unsigned long long)((z.tv_sec - a.tv_sec) * UINT64_C(1000000000) + z.tv_nsec - a.tv_nsec),
		(unsigned long long)(info.count + info.reservation_count));
	cluster_control_native_relmap_create_destroy_v2(&c);
	batch_end();
}
UT_TEST(existing_target_database_refuses_without_reissue)
{
	ClusterControlNativeRelmapCreateV2 *a = NULL, *b = NULL;
	table_reset();
	owner_begin();
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(1001, 1663, &b) != 0);
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(1001, 9999, &b) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT_EQ(reserve_target(&a), 0);
	UT_ASSERT(reserve_target(&b) != 0 && b == NULL);
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(2000, 9999, &b) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	cluster_control_native_relmap_create_destroy_v2(&a);
	batch_end();
}
UT_TEST(guards_and_shared_output_alias_refuse_before_reads)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	unsigned before;
	table_reset();
	owner_begin();
	before = dependencies;
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(0, 1663, &c) != 0);
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(2000, 0, &c) != 0);
	UT_ASSERT(cluster_control_native_files_reserve_relmap_create_locked_v2(2000, 1664, &c) != 0);
	UT_ASSERT(reserve_target((void *)native_files) != 0);
	cf_held = false;
	UT_ASSERT(reserve_target(&c) != 0);
	cf_held = true;
	batch_exclusive = false;
	UT_ASSERT(reserve_target(&c) != 0);
	batch_exclusive = true;
	for (unsigned i = 0; i < 5; i++) {
		LWLock *locks[] = { ControlFileLock, RelationMappingLock, XidGenLock, XactSLRULock,
							XactTruncationLock };
		held_family_lock = locks[i];
		UT_ASSERT(reserve_target(&c) != 0);
		held_family_lock = NULL;
	}
	CritSectionCount = 1;
	UT_ASSERT(reserve_target(&c) != 0);
	CritSectionCount = 0;
	UT_ASSERT_EQ(dependencies, before);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(c == NULL);
	batch_end();
}
UT_TEST(owner_claim_anchor_and_live_borrow_precede_reserve_mutation)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	table_reset();
	owner_begin();
	owner_ok = false;
	UT_ASSERT(reserve_target(&c) != 0);
	owner_ok = true;
	claim_ok = false;
	UT_ASSERT(reserve_target(&c) != 0);
	claim_ok = true;
	anchor_ok = false;
	UT_ASSERT(reserve_target(&c) != 0);
	anchor_ok = true;
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 1048576, 40), 0);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_RELMAP_V2,
																  0, 1664, 0, &b),
				 0);
	UT_ASSERT_EQ(reserve_target(&c), CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(c == NULL);
	batch_end();
}
UT_TEST(partial_wrong_copy_identity_and_source_never_publish)
{
	for (unsigned which = 0; which < 5; which++) {
		ClusterControlNativeRelmapCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		ClusterRecoveryCheckpointFileSourceV2 source[2];
		table_reset();
		owner_begin();
		UT_ASSERT_EQ(reserve_target(&c), 0);
		if (!c) {
			batch_end();
			return;
		}
		pair_receipts(c, r);
		if (which == 1)
			r[1].copy_index = 0;
		if (which == 2)
			r[1].identity.creation_generation++;
		if (which == 3) {
			memcpy(source, r[1].sources, sizeof(source));
			source[1].replay_end++;
			r[1].sources = source;
		}
		if (which == 4)
			r[1].synced_bytes = 2176;
		UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, which ? 2 : 1)
				  != 0);
		UT_ASSERT_EQ(root_renames, 2);
		pair_receipts(c, r);
		UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
		UT_ASSERT_EQ(root_renames, 2);
		cluster_control_native_relmap_create_destroy_v2(&c);
		batch_end();
	}
}
UT_TEST(copied_carrier_cannot_complete_or_consume_original)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL, copied;
	ClusterRecoveryCheckpointFileV2 r[2];
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) {
		batch_end();
		return;
	}
	pair_receipts(c, r);
	copied = *c;
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(&copied, r, 2) != 0);
	MyProcPid++;
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
	MyProcPid--;
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2), 0);
	cluster_control_native_relmap_create_destroy_v2(&c);
	batch_end();
}
UT_TEST(cf_reacquire_and_table_drift_do_not_reselect_target)
{
	for (unsigned which = 0; which < 2; which++) {
		ClusterControlNativeRelmapCreateV2 *c = NULL;
		ClusterControlNativeClogRangeV2 *gap = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		unsigned before;
		table_reset();
		owner_begin();
		UT_ASSERT_EQ(reserve_target(&c), 0);
		if (!c) {
			batch_end();
			return;
		}
		pair_receipts(c, r);
		if (which) {
			UT_ASSERT_EQ(cluster_control_native_files_reserve_clog_range_locked_v2(0, 9, 10, &gap),
						 0);
		} else
			cf_sequence++;
		before = root_renames;
		UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
		UT_ASSERT_EQ(root_renames, before);
		cluster_control_native_clog_range_destroy_v2(&gap);
		cluster_control_native_relmap_create_destroy_v2(&c);
		batch_end();
	}
}
UT_TEST(creator_borrow_must_retire_under_continuing_creation_protection)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlNativeFileBorrowV2 b = { 0 };
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) {
		batch_end();
		return;
	}
	pair_receipts(c, r);
	UT_ASSERT_EQ(
		cluster_control_native_file_reserved_acquire_locked_v2(
			&c->control.config, 1048576, c->startup_generation, c->batch_uuid, &c->expected, &b),
		0);
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2),
				 CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
	cluster_control_native_relmap_create_destroy_v2(&c);
	batch_end();
}
UT_TEST(reserve_late_owner_failure_preserves_original_creation_debt)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	table_reset();
	owner_begin();
	rename_hook = lose_owner;
	UT_ASSERT_EQ(reserve_target(&c), CLUSTER_NATIVE_FILES_REGISTRATION_DEBT_V2);
	UT_ASSERT(c == NULL && root_renames == 1);
	owner_ok = true;
	batch_end();
}
static unsigned fail_after_rename;
static void
creation_error(void)
{
	if (root_renames < fail_after_rename)
		rename_hook = creation_error;
	else
		config_error = true;
}
UT_TEST(complete_error_after_primary_preserves_pair_and_once_latch)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 id;
	volatile bool caught = false;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) {
		batch_end();
		return;
	}
	pair_receipts(c, r);
	fail_after_rename = 4;
	rename_hook = creation_error;
	PG_TRY();
	{
		(void)cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	rename_hook = NULL;
	UT_ASSERT(caught && c->private_attempted);
	UT_ASSERT_EQ(root_renames, 4);
	UT_ASSERT(created_selection(&s));
	UT_ASSERT(cluster_control_root_relmap_selection_lookup_v4(s, 2000, 1663, 0, &id));
	UT_ASSERT(identity_same(&id, &c->expected));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, r, 2) != 0);
	UT_ASSERT_EQ(root_renames, 4);
	cluster_control_native_relmap_create_destroy_v2(&c);
	batch_end();
}
#include "test_cluster_control_relmap_recovery.inc"
int
main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "--original"))
		return original_native_reservations_main(1, argv);
	UT_RUN(independent_target_persists_before_creator_borrow);
	UT_RUN(complete_exact_pair_makes_only_target_available);
	UT_RUN(existing_target_database_refuses_without_reissue);
	UT_RUN(guards_and_shared_output_alias_refuse_before_reads);
	UT_RUN(owner_claim_anchor_and_live_borrow_precede_reserve_mutation);
	UT_RUN(partial_wrong_copy_identity_and_source_never_publish);
	UT_RUN(copied_carrier_cannot_complete_or_consume_original);
	UT_RUN(cf_reacquire_and_table_drift_do_not_reselect_target);
	UT_RUN(creator_borrow_must_retire_under_continuing_creation_protection);
	UT_RUN(reserve_late_owner_failure_preserves_original_creation_debt);
	UT_RUN(complete_error_after_primary_preserves_pair_and_once_latch);
	UT_RUN(recovery_reserved_completes_without_open_creator);
	UT_RUN(recovery_available_requires_original_batch_and_same_pair);
	UT_RUN(recovery_wrong_record_binding_and_alias_refuse);
	UT_RUN(recovery_drift_and_partial_pair_poison_without_publication);
	UT_RUN(recovery_error_preserves_persistent_creation_debt);
	UT_RUN(recovery_cf_change_and_mapping_lock_entry_refuse);
	UT_RUN(recovery_confirmed_pair_rechecks_without_reconfirm_or_publication);
	UT_RUN(recovery_confirmed_requires_original_successful_once_confirmation);
	UT_RUN(recovery_confirmed_exact_pair_and_complete_sources_refuse_changed_or_aliased_inputs);
	UT_RUN(recovery_confirmed_context_and_selected_table_drift_poison_without_reselection);
	UT_RUN(recovery_confirmed_late_source_false_or_error_releases_only_its_local_borrow);
	UT_RUN(recovery_confirmed_failure_preserves_an_existing_native_file_borrow);
	UT_DONE();
	return ut_failed_count != 0;
}
