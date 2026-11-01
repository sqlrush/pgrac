/* Real ROOT/reservation/coverage publication. Original typed owner is a
 * controlled leaf here; its production frame checks are tested separately. */
#include "cluster/cluster_control_relmap_recovery.h"
#include "miscadmin.h"
static ClusterColdTypedV1 recovery_typed;
static ClusterWalSourceRef recovery_source;
static bool recovery_active, recovery_record_ok;
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
