/* Real ROOT/reservation/coverage publication. Original typed owner is a
 * controlled leaf here; its production frame checks are tested separately. */
#include "cluster/cluster_control_relmap_recovery.h"
#include "miscadmin.h"
static ClusterColdTypedV1 recovery_typed;
static ClusterWalSourceRef recovery_source;
static bool recovery_active, recovery_record_ok;
static bool recovery_confirmed_fail_borrowed, recovery_confirmed_error_borrowed;
static unsigned recovery_failed;
BackendType MyBackendType;
static struct ClusterRecoveryFencePlan *const recovery_fence = (void *)0x1234;
bool
RecoveryInProgress(void)
{
	return recovery_active;
}
bool
cluster_cold_typed_relmap_create_record_current_v2(ClusterColdTypedV1 *t,
												   struct ClusterRecoveryFencePlan *f, uint32 p,
												   const ClusterWalSourceRef *s, XLogRecPtr read,
												   XLogRecPtr end)
{
	if (recovery_confirmed_fail_borrowed || recovery_confirmed_error_borrowed) {
		ClusterControlNativeTableInfoV2 info;
		if (cluster_control_native_files_info_v2(&info) && info.borrowed) {
			if (recovery_confirmed_error_borrowed)
				ereport(ERROR, (errmsg("injected original source ERROR during confirmed recheck")));
			return false;
		}
	}
	return t == &recovery_typed && f == recovery_fence && p == 0 && recovery_active
		   && recovery_record_ok && !recovery_failed && read == 0x200 && end == 0x280
		   && !memcmp(s, &recovery_source, sizeof(*s));
}
bool
cluster_cold_typed_relmap_create_record_held_v2(ClusterColdTypedV1 *t,
												struct ClusterRecoveryFencePlan *f, uint32 p,
												const ClusterWalSourceRef *s, XLogRecPtr read,
												XLogRecPtr end)
{
	return cluster_cold_typed_relmap_create_record_current_v2(t, f, p, s, read, end);
}
void
cluster_cold_typed_relmap_create_failed_v2(ClusterColdTypedV1 *t)
{
	UT_ASSERT(t == &recovery_typed);
	recovery_failed++;
}
static void
recovery_pair_receipts(const ClusterControlNativeRelmapCreateV2 *old,
					   ClusterRecoveryCheckpointFileV2 r[2])
{
	pair_receipts(old, r);
	memcpy(r[1].image_digest, r[0].image_digest, 32);
}
static void
recovery_enter(void)
{
	memset(&recovery_typed, 0, sizeof(recovery_typed));
	recovery_source = sources[0].source;
	recovery_active = recovery_record_ok = true;
	recovery_confirmed_fail_borrowed = recovery_confirmed_error_borrowed = false;
	recovery_failed = 0;
	MyBackendType = B_STARTUP;
	/* Runtime owner fails here. The original persisted owner is still source0. */
	owner_ok = false;
}
static void
recovery_leave(void)
{
	recovery_active = false;
	owner_ok = true;
	MyBackendType = B_INVALID;
}
static ClusterControlNativeResultV2
recovery_begin(const ClusterControlNativeRelmapCreateV2 *old,
			   ClusterControlRelmapRecoveryCreateV2 **out)
{
	return cluster_control_relmap_recovery_create_begin_locked_v2(
		&recovery_typed, recovery_fence, 0, &recovery_source, 0x200, 0x280, &old->expected,
		old->batch_uuid, out);
}
UT_TEST(recovery_reserved_completes_without_open_creator)
{
	ClusterControlNativeRelmapCreateV2 *old = NULL;
	ClusterControlRelmapRecoveryCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 id;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&old), 0);
	if (!old) {
		batch_end();
		return;
	}
	recovery_pair_receipts(old, r);
	recovery_enter();
	UT_ASSERT_EQ(recovery_begin(old, &c), 0);
	UT_ASSERT(c && cluster_control_relmap_recovery_create_current_v2(c));
	if (c)
		UT_ASSERT_EQ(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2), 0);
	UT_ASSERT_EQ(root_renames, 4);
	UT_ASSERT_EQ(recovery_failed, 0);
	UT_ASSERT(created_selection(&s));
	UT_ASSERT(cluster_control_root_relmap_selection_lookup_v4(s, 2000, 1663, 0, &id));
	UT_ASSERT(identity_same(&old->expected, &id));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	cluster_control_relmap_recovery_create_destroy_v2(&c);
	UT_ASSERT_EQ(recovery_failed, 0);
	recovery_leave();
	cluster_control_native_relmap_create_destroy_v2(&old);
	batch_end();
}
UT_TEST(recovery_available_requires_original_batch_and_same_pair)
{
	ClusterControlNativeRelmapCreateV2 *old = NULL;
	ClusterControlRelmapRecoveryCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlNativeRelmapCreateV2 fake = { 0 };
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&old), 0);
	if (!old) {
		batch_end();
		return;
	}
	recovery_pair_receipts(old, r);
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(old, r, 2), 0);
	recovery_enter();
	UT_ASSERT_EQ(recovery_begin(old, &c), 0);
	if (c)
		UT_ASSERT_EQ(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2), 0);
	UT_ASSERT_EQ(root_renames, 4);
	cluster_control_relmap_recovery_create_destroy_v2(&c);
	recovery_enter();
	old->batch_uuid[0] ^= 1;
	UT_ASSERT(recovery_begin(old, &c) != 0 && c == NULL);
	UT_ASSERT_EQ(root_renames, 4);
	recovery_leave();
	cluster_control_native_relmap_create_destroy_v2(&old);
	batch_end();
	/* Original BOOT coverage has no creation batch: same file is insufficient. */
	table_reset();
	owner_begin();
	recovery_enter();
	fake.expected = inventory[2].identity;
	memset(fake.batch_uuid, 0x91, 16);
	UT_ASSERT(recovery_begin(&fake, &c) != 0 && c == NULL);
	UT_ASSERT_EQ(root_renames, 0);
	recovery_leave();
	batch_end();
}
UT_TEST(recovery_wrong_record_binding_and_alias_refuse)
{
	for (unsigned which = 0; which < 6; which++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		table_reset();
		owner_begin();
		UT_ASSERT_EQ(reserve_target(&old), 0);
		if (!old) {
			batch_end();
			return;
		}
		recovery_enter();
		if (which == 0)
			recovery_source.claim.identity.origin_owner_incarnation++;
		if (which == 1)
			old->batch_uuid[0] ^= 1;
		if (which == 2)
			old->expected.creation_generation++;
		if (which == 3)
			recovery_record_ok = false;
		if (which == 4)
			MyBackendType = B_BACKEND;
		if (which == 5) {
			UT_ASSERT(recovery_begin(old, (void *)&recovery_typed) != 0);
		} else
			UT_ASSERT(recovery_begin(old, &c) != 0 && !c);
		UT_ASSERT_EQ(root_renames, 2);
		recovery_leave();
		cluster_control_native_relmap_create_destroy_v2(&old);
		batch_end();
	}
}
UT_TEST(recovery_drift_and_partial_pair_poison_without_publication)
{
	for (unsigned which = 0; which < 5; which++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		table_reset();
		owner_begin();
		UT_ASSERT_EQ(reserve_target(&old), 0);
		if (!old) {
			batch_end();
			return;
		}
		recovery_pair_receipts(old, r);
		recovery_enter();
		UT_ASSERT_EQ(recovery_begin(old, &c), 0);
		if (which == 0)
			recovery_record_ok = false;
		if (which == 1)
			r[1].copy_index = 0;
		if (which == 2)
			r[1].image_digest[0] ^= 1;
		if (which == 3)
			r[1].identity.creation_generation++;
		if (c && which != 4) {
			UT_ASSERT(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2) != 0);
			recovery_record_ok = true;
			UT_ASSERT(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2) != 0);
		}
		cluster_control_relmap_recovery_create_destroy_v2(&c);
		UT_ASSERT(recovery_failed != 0);
		UT_ASSERT_EQ(root_renames, 2);
		recovery_leave();
		cluster_control_native_relmap_create_destroy_v2(&old);
		batch_end();
	}
}
UT_TEST(recovery_error_preserves_persistent_creation_debt)
{
	ClusterControlNativeRelmapCreateV2 *old = NULL;
	ClusterControlRelmapRecoveryCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	volatile bool caught = false;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(reserve_target(&old), 0);
	if (!old) {
		batch_end();
		return;
	}
	recovery_pair_receipts(old, r);
	recovery_enter();
	UT_ASSERT_EQ(recovery_begin(old, &c), 0);
	fail_after_rename = 3;
	rename_hook = creation_error;
	if (c) {
		PG_TRY();
		{
			(void)cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
	}
	config_error = false;
	rename_hook = NULL;
	UT_ASSERT(caught && recovery_failed != 0);
	UT_ASSERT_EQ(root_renames, 4);
	if (c)
		UT_ASSERT(!cluster_control_relmap_recovery_create_current_v2(c));
	cluster_control_relmap_recovery_create_destroy_v2(&c);
	recovery_leave();
	cluster_control_native_relmap_create_destroy_v2(&old);
	batch_end();
}
UT_TEST(recovery_cf_change_and_mapping_lock_entry_refuse)
{
	for (unsigned which = 0; which < 2; which++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		table_reset();
		owner_begin();
		UT_ASSERT_EQ(reserve_target(&old), 0);
		if (!old) {
			batch_end();
			return;
		}
		recovery_pair_receipts(old, r);
		recovery_enter();
		if (which == 0) {
			held_family_lock = RelationMappingLock;
			UT_ASSERT(recovery_begin(old, &c) != 0 && !c);
			held_family_lock = NULL;
		} else {
			UT_ASSERT_EQ(recovery_begin(old, &c), 0);
			cf_sequence++;
			if (c)
				UT_ASSERT(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2) != 0);
		}
		UT_ASSERT_EQ(root_renames, 2);
		cluster_control_relmap_recovery_create_destroy_v2(&c);
		recovery_leave();
		cluster_control_native_relmap_create_destroy_v2(&old);
		batch_end();
	}
}

/* Complete persisted control/table are real; typed/IR/WALR are the same finite
 * authority leaves as the original creation tests, not native qualification. */
static bool
recovery_confirmed_setup(bool available, ClusterControlNativeRelmapCreateV2 **old,
	ClusterControlRelmapRecoveryCreateV2 **c, ClusterRecoveryCheckpointFileV2 r[2])
{
	table_reset();
	owner_begin();
	if (reserve_target(old) != 0 || *old == NULL) return false;
	recovery_pair_receipts(*old, r);
	if (available && cluster_control_native_files_complete_relmap_create_locked_v2(*old, r, 2) != 0)
		return false;
	recovery_enter();
	return recovery_begin(*old, c) == 0 && *c != NULL
		&& cluster_control_relmap_recovery_create_confirm_locked_v2(*c, r, 2) == 0;
}
static void
recovery_confirmed_cleanup(ClusterControlNativeRelmapCreateV2 **old,
	ClusterControlRelmapRecoveryCreateV2 **c)
{
	cluster_control_relmap_recovery_create_destroy_v2(c);
	recovery_leave();
	cluster_control_native_relmap_create_destroy_v2(old);
	batch_end();
}
UT_TEST(recovery_confirmed_pair_rechecks_without_reconfirm_or_publication)
{
	for (unsigned available = 0; available < 2; available++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		ClusterControlNativeTableInfoV2 before, after;
		bool ready = recovery_confirmed_setup(available != 0, &old, &c, r);
		UT_ASSERT(ready);
		if (ready) {
			UT_ASSERT(cluster_control_native_files_info_v2(&before));
			for (unsigned repeat = 0; repeat < 3; repeat++)
				UT_ASSERT(cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
			UT_ASSERT(cluster_control_native_files_info_v2(&after));
			UT_ASSERT_EQ(root_renames, 4);
			UT_ASSERT_EQ(before.table_generation, after.table_generation);
			UT_ASSERT_EQ(before.borrowed, after.borrowed);
			UT_ASSERT_EQ(recovery_failed, 0);
		}
		recovery_confirmed_cleanup(&old, &c);
	}
}
UT_TEST(recovery_confirmed_requires_original_successful_once_confirmation)
{
	ClusterControlNativeRelmapCreateV2 *old = NULL;
	ClusterControlRelmapRecoveryCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	table_reset(); owner_begin();
	UT_ASSERT_EQ(reserve_target(&old), 0);
	if (!old) { batch_end(); return; }
	recovery_pair_receipts(old, r); recovery_enter();
	UT_ASSERT_EQ(recovery_begin(old, &c), 0);
	UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
	UT_ASSERT(recovery_failed != 0);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(cluster_control_relmap_recovery_create_confirm_locked_v2(c, r, 2) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	recovery_confirmed_cleanup(&old, &c);
}
UT_TEST(recovery_confirmed_exact_pair_and_complete_sources_refuse_changed_or_aliased_inputs)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2], changed[2];
		ClusterRecoveryCheckpointFileSourceV2 copy[8];
		const ClusterRecoveryCheckpointFileV2 *input = changed;
		ClusterControlNativeTableInfoV2 info;
		uint64 count = 2;
		bool ready = recovery_confirmed_setup(false, &old, &c, r);
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&old, &c); continue; }
		memcpy(changed, r, sizeof(changed));
		UT_ASSERT(r[1].source_count <= lengthof(copy));
		if (r[1].source_count > lengthof(copy)) abort();
		memcpy(copy, r[1].sources, r[1].source_count * sizeof(*copy));
		changed[1].sources = copy;
		switch (fault) {
			case 0: count = 1; break;
			case 1: count = UINT64_MAX; break;
			case 2: input = NULL; break;
			case 3: changed[1].copy_index = 0; break;
			case 4: changed[1].identity.creation_generation++; break;
			case 5: changed[1].image_digest[0] ^= 1; break;
			case 6: changed[1].source_count = UINT64_MAX; break;
			case 7: changed[1].sources = NULL; break;
			case 8: copy[0].replay_end++; break;
			case 9: changed[1].sources = (void *)changed; break;
			case 10: changed[1].sources = (void *)native_files; break;
			case 11: input = (void *)native_files; break;
			case 12: input = (void *)&recovery_typed; break;
			case 13: input = (void *)c; break;
		}
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, input, count));
		UT_ASSERT(recovery_failed != 0);
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.borrowed, 0);
		UT_ASSERT_EQ(root_renames, 4);
		recovery_confirmed_cleanup(&old, &c);
	}
}
UT_TEST(recovery_confirmed_context_and_selected_table_drift_poison_without_reselection)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		ClusterControlNativeTableInfoV2 info;
		bool ready = recovery_confirmed_setup(false, &old, &c, r);
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&old, &c); continue; }
		switch (fault) {
			case 0: recovery_record_ok = false; break;
			case 1: cf_sequence++; break;
			case 2: held_family_lock = RelationMappingLock; break;
			case 3: CritSectionCount = 1; break;
			case 4: recovery_active = false; break;
			case 5: native_files->banks[native_files->active].info.startup_generation++; break;
			case 6: native_files->banks[native_files->active].info.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; break;
		}
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		UT_ASSERT(recovery_failed != 0);
		held_family_lock = NULL; CritSectionCount = 0;
		recovery_active = recovery_record_ok = true;
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.borrowed, 0);
		UT_ASSERT_EQ(root_renames, 4);
		recovery_confirmed_cleanup(&old, &c);
	}
}
UT_TEST(recovery_confirmed_late_source_false_or_error_releases_only_its_local_borrow)
{
	for (unsigned error = 0; error < 2; error++) {
		ClusterControlNativeRelmapCreateV2 *old = NULL;
		ClusterControlRelmapRecoveryCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 r[2];
		ClusterControlNativeTableInfoV2 info;
		volatile bool caught = false, accepted = false;
		bool ready = recovery_confirmed_setup(false, &old, &c, r);
		UT_ASSERT(ready);
		if (!ready) { recovery_confirmed_cleanup(&old, &c); continue; }
		recovery_confirmed_fail_borrowed = !error;
		recovery_confirmed_error_borrowed = error != 0;
		PG_TRY(); { accepted = cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, error != 0);
		UT_ASSERT(!accepted && recovery_failed != 0);
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.borrowed, 0);
		UT_ASSERT_EQ(root_renames, 4);
		recovery_confirmed_fail_borrowed = recovery_confirmed_error_borrowed = false;
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		recovery_confirmed_cleanup(&old, &c);
	}
}
UT_TEST(recovery_confirmed_failure_preserves_an_existing_native_file_borrow)
{
	ClusterControlNativeRelmapCreateV2 *old = NULL;
	ClusterControlRelmapRecoveryCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 r[2];
	ClusterControlNativeFileBorrowV2 original = {0};
	ClusterControlNativeTableInfoV2 info;
	bool ready = recovery_confirmed_setup(false, &old, &c, r);
	UT_ASSERT(ready);
	if (ready) {
		UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
			CLUSTER_RECOVERY_FILE_RELMAP_V2, 0, GLOBALTABLESPACE_OID, 0, &original), 0);
		UT_ASSERT(cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		UT_ASSERT(cluster_control_native_file_current_v2(&original));
		r[1].image_digest[0] ^= 1;
		UT_ASSERT(!cluster_control_relmap_recovery_create_confirmed_current_v2(c, r, 2));
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.borrowed, 1);
		UT_ASSERT(cluster_control_native_file_current_v2(&original));
		UT_ASSERT(cluster_control_native_file_release_v2(&original));
		UT_ASSERT_EQ(root_renames, 4);
	}
	recovery_confirmed_cleanup(&old, &c);
}
