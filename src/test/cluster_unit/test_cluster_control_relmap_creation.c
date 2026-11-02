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
UT_TEST(pre_scan_projection_preserves_reserved_to_available_original_batch)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRecoveryFileIdentityV2 expected;
	uint8 batch[16];
	uint64 count = 0;
	table_reset(); owner_begin();
	UT_ASSERT_EQ(reserve_target(&c), 0);
	if (!c) { batch_end(); return; }
	for (unsigned phase = 0; phase < 2; ++phase) {
		UT_ASSERT(created_selection(&s));
		UT_ASSERT(cluster_control_root_relmap_creation_count_v4(s, &count));
		UT_ASSERT_EQ(count, 1);
		UT_ASSERT(cluster_control_root_relmap_creation_at_v4(s, 0, &expected, batch));
		UT_ASSERT(identity_same(&expected, &c->expected));
		UT_ASSERT(!memcmp(batch, c->batch_uuid, 16));
		cluster_control_root_relmap_selection_destroy_v4(&s);
		if (!phase) {
			pair_receipts(c, receipts);
			UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, receipts, 2), 0);
		}
	}
	cluster_control_native_relmap_create_destroy_v2(&c);
	batch_end();
}

/* Explicit creation barrier provider: the full Startup/H implementation is
 * exercised by its own original-source fixture, not invented by this leaf. */
static uint64 recovery_barrier_episode = 73;
static bool recovery_barrier_current = true;
static bool recovery_barrier_exact_root;
static ClusterControlRootReadToken recovery_barrier_before;
static bool actual_root_token(uint16, ClusterControlRootReadToken *);
bool
cluster_cold_typed_relmap_create_barrier_record_current_v2(ClusterColdTypedV1 *t,
	struct ClusterRecoveryFencePlan *f, uint64 id, uint32 p,
	const ClusterWalSourceRef *source, XLogRecPtr read, XLogRecPtr end)
{
	ClusterControlRootReadToken current;
	return recovery_barrier_current && id == recovery_barrier_episode
		&& cluster_cold_typed_relmap_create_record_current_v2(t, f, p, source, read, end)
		&& (!recovery_barrier_exact_root || (actual_root_token(1, &current)
			&& !memcmp(&current, &recovery_barrier_before, sizeof(current))));
}
bool
cluster_cold_typed_relmap_create_barrier_record_held_v2(ClusterColdTypedV1 *t,
	struct ClusterRecoveryFencePlan *f, uint64 id, uint32 p,
	const ClusterWalSourceRef *source, XLogRecPtr read, XLogRecPtr end)
{
	return recovery_barrier_current && id == recovery_barrier_episode
		&& cluster_cold_typed_relmap_create_record_held_v2(t, f, p, source, read, end);
}

bool cluster_cold_typed_relmap_create_barrier_confirmation_current_v2(
	ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint64,
	const ClusterControlRelmapRecoveryCreateV2 *, uint32,
	const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
bool
cluster_cold_typed_relmap_create_barrier_confirmation_current_v2(
	ClusterColdTypedV1 *t, struct ClusterRecoveryFencePlan *f, uint64 id,
	const ClusterControlRelmapRecoveryCreateV2 *c, uint32 p,
	const ClusterWalSourceRef *source, XLogRecPtr read, XLogRecPtr end)
{
	ClusterControlRootReadToken effective, actual;
	if (recovery_barrier_exact_root) {
		ClusterControlRootReadToken untouched = {0}, final = {0};
		/* This callback runs inside confirm, before final success/table
		 * publication. Its temporary phase is never final delta authority. */
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			c, t, f, id));
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_root_delta_v2(
			c, t, f, id, &recovery_barrier_before, &final));
		UT_ASSERT(!memcmp(&untouched, &final, sizeof(final)));
		cf_held = false;
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(
			c, t, f, id, &recovery_barrier_before, &final));
		UT_ASSERT(!memcmp(&untouched, &final, sizeof(final)));
		cf_held = true;
	}
	return recovery_barrier_current && id == recovery_barrier_episode
		&& cluster_cold_typed_relmap_create_record_current_v2(t, f, p, source, read, end)
		&& (!recovery_barrier_exact_root
			|| (cluster_control_relmap_recovery_create_root_delta_v2(
				c, t, f, id, &recovery_barrier_before, &effective)
				&& actual_root_token(1, &actual) && !memcmp(&effective, &actual, sizeof(actual))));
}
static bool
barrier_confirmed_setup(bool already_available, ClusterControlNativeRelmapCreateV2 **original,
	ClusterControlRelmapRecoveryCreateV2 **receipt, ClusterRecoveryCheckpointFileV2 files[2])
{
	table_reset(); owner_begin();
	recovery_barrier_current = true;
	if (reserve_target(original) != 0 || !*original) return false;
	recovery_pair_receipts(*original, files);
	if (already_available && cluster_control_native_files_complete_relmap_create_locked_v2(
			*original, files, 2) != 0) return false;
	recovery_enter();
	return cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
		&recovery_typed, recovery_fence, recovery_barrier_episode, 0, &recovery_source,
		0x200, 0x280, &(*original)->expected, (*original)->batch_uuid, receipt) == 0
		&& cluster_control_relmap_recovery_create_confirm_locked_v2(*receipt, files, 2) == 0;
}
UT_TEST(repeated_creation_barrier_rechecks_same_original_batch_after_fresh_cf)
{
	for (unsigned available = 0; available < 2; ++available) {
		ClusterControlNativeRelmapCreateV2 *original = NULL;
		ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
		ClusterRecoveryCheckpointFileV2 files[2];
		bool ready = barrier_confirmed_setup(available, &original, &receipt, files);
		unsigned publications = root_renames;
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&original, &receipt); return; }
		UT_ASSERT(cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
			receipt, recovery_barrier_episode, files, 2));
		cf_held = false; cf_sequence++; cf_held = true;
		UT_ASSERT(cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
			receipt, recovery_barrier_episode, files, 2));
		UT_ASSERT_EQ(root_renames, publications);
		recovery_confirmed_cleanup(&original, &receipt);
	}
}
UT_TEST(first_creation_barrier_authorization_still_refuses_fresh_cf)
{
	ClusterControlNativeRelmapCreateV2 *original = NULL;
	ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
	ClusterRecoveryCheckpointFileV2 files[2];
	table_reset(); owner_begin(); recovery_barrier_current = true;
	UT_ASSERT_EQ(reserve_target(&original), 0);
	if (!original) { batch_end(); return; }
	recovery_pair_receipts(original, files); recovery_enter();
	UT_ASSERT_EQ(cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
		&recovery_typed, recovery_fence, recovery_barrier_episode, 0, &recovery_source,
		0x200, 0x280, &original->expected, original->batch_uuid, &receipt), 0);
	cf_held = false; cf_sequence++; cf_held = true;
	UT_ASSERT(!cluster_control_relmap_recovery_create_current_v2(receipt));
	UT_ASSERT(cluster_control_relmap_recovery_create_confirm_locked_v2(receipt, files, 2) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	recovery_confirmed_cleanup(&original, &receipt);
}
UT_TEST(repeated_barrier_wrong_episode_or_source_cannot_reselect)
{
	for (unsigned which = 0; which < 3; ++which) {
		ClusterControlNativeRelmapCreateV2 *original = NULL;
		ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
		ClusterRecoveryCheckpointFileV2 files[2];
		bool ready = barrier_confirmed_setup(false, &original, &receipt, files);
		unsigned publications = root_renames;
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&original, &receipt); return; }
		cf_held = false; cf_sequence++; cf_held = true;
		if (which == 1) recovery_barrier_current = false;
		if (which == 2) files[1].image_digest[0] ^= 1;
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
			receipt, recovery_barrier_episode + (which == 0), files, 2));
		UT_ASSERT(recovery_failed != 0);
		UT_ASSERT_EQ(root_renames, publications);
		recovery_confirmed_cleanup(&original, &receipt);
	}
}

static bool
actual_root_token(uint16 thread, ClusterControlRootReadToken *token)
{
	ControlRootImage *image = calloc(1, sizeof(*image));
	uint8 *pair = malloc(133120);
	bool ok = image && pair && cluster_control_root_registry_capture_v4(
		selection_dir, root.header.storage_uuid, 11, pair, image)
		&& thread && thread <= CLUSTER_CONTROL_ROOT_RECORD_COUNT && image->present[thread - 1];
	if (ok) {
		const ClusterControlRootSnapshot *r = &image->records[thread - 1];
		memset(token, 0, sizeof(*token));
		memcpy(token->authority_uuid, r->identity.authority_uuid, 16);
		token->origin_thread_id = thread;
		token->source = 1; /* actual selected primary */
		token->lifecycle = r->lifecycle;
		token->root_lineage_seq = r->identity.root_lineage_seq;
		token->file_txn_seq = image->header.file_txn_seq;
		token->root_publish_seq = r->root_publish_seq;
		token->record_crc32c = image->record_crc32c[thread - 1];
		token->root_flags = r->root_flags;
	}
	free(pair); free(image); return ok;
}
UT_TEST(original_confirm_keeps_full_root_checks_during_own_publication)
{
	ClusterControlNativeRelmapCreateV2 *original = NULL;
	ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
	ClusterRecoveryCheckpointFileV2 files[2];
	table_reset(); owner_begin(); recovery_barrier_current = true;
	UT_ASSERT_EQ(reserve_target(&original), 0);
	if (!original) { batch_end(); return; }
	recovery_pair_receipts(original, files);
	UT_ASSERT(actual_root_token(1, &recovery_barrier_before));
	recovery_barrier_exact_root = true;
	recovery_enter();
	UT_ASSERT_EQ(cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
		&recovery_typed, recovery_fence, recovery_barrier_episode, 0, &recovery_source,
		0x200, 0x280, &original->expected, original->batch_uuid, &receipt), 0);
	UT_ASSERT(receipt != NULL);
	if (receipt)
		UT_ASSERT_EQ(cluster_control_relmap_recovery_create_confirm_locked_v2(receipt, files, 2), 0);
	recovery_barrier_exact_root = false;
	recovery_confirmed_cleanup(&original, &receipt);
}
UT_TEST(original_confirm_delta_carries_exact_root_without_rebasing)
{
	for (unsigned available = 0; available < 2; ++available) {
		ClusterControlNativeRelmapCreateV2 *original = NULL;
		ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
		ClusterRecoveryCheckpointFileV2 files[2];
		ClusterControlRootReadToken before, after, observed, untouched;
		ClusterControlNativeTableInfoV2 before_table, after_table, actual_table;
		ClusterControlRelmapReferenceV2 after_control, bad_control;
		table_reset(); owner_begin(); recovery_barrier_current = true;
		UT_ASSERT_EQ(reserve_target(&original), 0);
		if (!original) { batch_end(); return; }
		recovery_pair_receipts(original, files);
		if (available) UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(
			original, files, 2), 0);
		UT_ASSERT(actual_root_token(1, &before));
		UT_ASSERT(cluster_control_native_files_info_v2(&before_table));
		recovery_enter();
		UT_ASSERT_EQ(cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
			&recovery_typed, recovery_fence, recovery_barrier_episode, 0, &recovery_source,
			0x200, 0x280, &original->expected, original->batch_uuid, &receipt), 0);
		UT_ASSERT(receipt != NULL);
		if (!receipt) { recovery_confirmed_cleanup(&original, &receipt); return; }
		memset(&after, 0x5c, sizeof(after)); untouched = after;
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		UT_ASSERT(!memcmp(&after, &untouched, sizeof(after)));
		UT_ASSERT_EQ(cluster_control_relmap_recovery_create_confirm_locked_v2(receipt, files, 2), 0);
		UT_ASSERT(actual_root_token(1, &observed));
		UT_ASSERT(cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		UT_ASSERT(cluster_control_relmap_recovery_create_confirmed_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		UT_ASSERT(!memcmp(&after, &observed, sizeof(after)));
		UT_ASSERT_EQ(after.file_txn_seq, before.file_txn_seq + !available);
		UT_ASSERT(cluster_control_relmap_recovery_create_control_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode,
			&original->expected, original->batch_uuid,
			&before_table.control, &before_table, &after_control, &after_table));
		UT_ASSERT(cluster_control_native_files_info_v2(&actual_table));
		UT_ASSERT(!memcmp(&after_control, &actual_table.control, sizeof(after_control)));
		UT_ASSERT(!memcmp(&after_table, &actual_table, sizeof(after_table)));
		bad_control = before_table.control; bad_control.registry_reference[0] ^= 1;
		UT_ASSERT(!cluster_control_relmap_recovery_create_control_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode,
			&original->expected, original->batch_uuid,
			&bad_control, &before_table, &after_control, &after_table));
		cf_sequence++;
		UT_ASSERT(cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		before.record_crc32c ^= 1; untouched = after;
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		UT_ASSERT(!memcmp(&after, &untouched, sizeof(after)));
		before.record_crc32c ^= 1;
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode + 1, &before, &after));
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, (void *)receipt));
		cluster_control_relmap_recovery_create_fail_v2(receipt);
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_v2(receipt,
			&recovery_typed, recovery_fence, recovery_barrier_episode, &before, &after));
		recovery_confirmed_cleanup(&original, &receipt);
	}
}
UT_TEST(original_confirmation_storage_aliases_refuse_after_failure_without_io)
{
	ClusterControlNativeRelmapCreateV2 *original = NULL;
	ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
	ClusterRecoveryCheckpointFileV2 files[2];
	void *independent = NULL;
	bool ready = barrier_confirmed_setup(false, &original, &receipt, files);
	unsigned reads = dependencies, writes = root_renames;
	UT_ASSERT(ready);
	if (!ready) { recovery_confirmed_cleanup(&original, &receipt); return; }
	UT_ASSERT(cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, &independent, sizeof(independent)));
	UT_ASSERT(!cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, receipt, sizeof(independent)));
	UT_ASSERT(!cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, (char *)receipt + 1, sizeof(independent)));
	cluster_control_relmap_recovery_create_fail_v2(receipt);
	cf_held = false;
	UT_ASSERT(cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, &independent, sizeof(independent)));
	UT_ASSERT(!cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, receipt, sizeof(independent)));
	UT_ASSERT(!cluster_control_relmap_recovery_create_output_distinct_v2(
		receipt, &independent, 0));
	UT_ASSERT_EQ(dependencies, reads);
	UT_ASSERT_EQ(root_renames, writes);
	cf_held = true;
	recovery_confirmed_cleanup(&original, &receipt);
}
UT_TEST(final_confirmation_state_is_pure_and_same_episode_only)
{
	for (unsigned available = 0; available < 2; ++available) {
		ClusterControlNativeRelmapCreateV2 *original = NULL;
		ClusterControlRelmapRecoveryCreateV2 *receipt = NULL;
		ClusterRecoveryCheckpointFileV2 files[2];
		unsigned reads, writes;
		bool ready = barrier_confirmed_setup(available, &original, &receipt, files);
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&original, &receipt); return; }
		reads = dependencies; writes = root_renames;
		UT_ASSERT(cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, &recovery_typed, recovery_fence, recovery_barrier_episode));
		/* Read-only repetition uses a new CF acquisition, never first-CF
		 * authorization. The outer H entry still checks its original guards. */
		cf_sequence++;
		UT_ASSERT(cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, &recovery_typed, recovery_fence, recovery_barrier_episode));
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, NULL, recovery_fence, recovery_barrier_episode));
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, &recovery_typed, NULL, recovery_barrier_episode));
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, &recovery_typed, recovery_fence, recovery_barrier_episode + 1));
		cluster_control_relmap_recovery_create_fail_v2(receipt);
		UT_ASSERT(!cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
			receipt, &recovery_typed, recovery_fence, recovery_barrier_episode));
		UT_ASSERT_EQ(dependencies, reads);
		UT_ASSERT_EQ(root_renames, writes);
		recovery_confirmed_cleanup(&original, &receipt);
	}
}

#ifdef TEST_CONTROL_CONFIRM_MERGE
int fixture_control_creation_main(int argc, char **argv);
#define main fixture_control_creation_main
#endif
int
main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "barrier-red")) {
		UT_PLAN(7);
		UT_RUN(repeated_creation_barrier_rechecks_same_original_batch_after_fresh_cf);
		UT_RUN(first_creation_barrier_authorization_still_refuses_fresh_cf);
		UT_RUN(repeated_barrier_wrong_episode_or_source_cannot_reselect);
		UT_RUN(original_confirm_keeps_full_root_checks_during_own_publication);
		UT_RUN(original_confirm_delta_carries_exact_root_without_rebasing);
		UT_RUN(original_confirmation_storage_aliases_refuse_after_failure_without_io);
		UT_RUN(final_confirmation_state_is_pure_and_same_episode_only);
		UT_DONE();
		return ut_failed_count != 0;
	}

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
	UT_RUN(pre_scan_projection_preserves_reserved_to_available_original_batch);
	UT_RUN(repeated_creation_barrier_rechecks_same_original_batch_after_fresh_cf);
	UT_RUN(first_creation_barrier_authorization_still_refuses_fresh_cf);
	UT_RUN(repeated_barrier_wrong_episode_or_source_cannot_reselect);
	UT_RUN(original_confirmation_storage_aliases_refuse_after_failure_without_io);
	UT_RUN(original_confirm_delta_carries_exact_root_without_rebasing);
	UT_RUN(original_confirm_keeps_full_root_checks_during_own_publication);
	UT_RUN(final_confirmation_state_is_pure_and_same_episode_only);
	UT_DONE();
	return ut_failed_count != 0;
}
#ifdef TEST_CONTROL_CONFIRM_MERGE
#undef main
#endif
