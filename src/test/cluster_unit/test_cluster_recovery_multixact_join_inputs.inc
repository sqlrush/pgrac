/* Original retained cold WAL and actual native-side files.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_native_startup.h"

static void
inventory_mx_files(int fd, bool create, bool mismatch)
{
	char path[MAXPGPATH];
	PGAlignedBlock offsets = { 0 }, members = { 0 };
	uint32 value = 108, flags = MultiXactStatusForShare | (MultiXactStatusUpdate << 8);
	TransactionId xids[2] = { 4195136, 4195138 };
	memcpy(offsets.data + 42 * sizeof(uint32), &value, sizeof(value));
	memcpy(members.data + (108 / 4) * 20, &flags, sizeof(flags));
	memcpy(members.data + (108 / 4) * 20 + 4, xids, sizeof(xids));
	if (create)
		UT_ASSERT_EQ(mkdirat(fd, "native_side", 0700), 0);
	for (unsigned i = 0; i < 2; ++i) {
		const unsigned node = i ? 127 : 0;
		const char *dirs[]
			= { "", "/pg_multixact", "/pg_multixact/offsets", "/pg_multixact/members" };
		if (create) {
			for (unsigned d = 0; d < lengthof(dirs); ++d) {
				snprintf(path, sizeof(path), "native_side/origin_%u%s", node, dirs[d]);
				UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
			}
		}
		for (unsigned family = 0; family < 2; ++family) {
			int file;
			snprintf(path, sizeof(path), "native_side/origin_%u/pg_multixact/%s/0000", node,
					 family ? "members" : "offsets");
			if (create) {
				if (mismatch && node == 127) {
					memset(offsets.data + 42 * sizeof(uint32), 0, sizeof(uint32));
					flags = (MultiXactStatusUpdate << 8) | 255;
					memcpy(members.data + (108 / 4) * 20, &flags, sizeof(flags));
				}
				file = openat(fd, path, O_CREAT | O_WRONLY, 0600);
				UT_ASSERT(file >= 0);
				UT_ASSERT_EQ(write(file, family ? members.data : offsets.data, BLCKSZ), BLCKSZ);
				UT_ASSERT_EQ(close(file), 0);
			} else
				UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
		}
		if (!create)
			for (int d = lengthof(dirs) - 1; d >= 0; --d) {
				snprintf(path, sizeof(path), "native_side/origin_%u%s", node, dirs[d]);
				UT_ASSERT_EQ(unlinkat(fd, path, AT_REMOVEDIR), 0);
			}
	}
	if (!create)
		UT_ASSERT_EQ(unlinkat(fd, "native_side", AT_REMOVEDIR), 0);
}

static ClusterRecoveryMultixactJoinResult
inventory_mx_join_drain(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryRootSet *roots,
						ClusterRecoveryNativeInventoryDiag *diag, unsigned *entries)
{
	ClusterRecoveryMultixactJoinEntry out;
	for (unsigned calls = 0; calls < 100000; ++calls) {
		ClusterRecoveryMultixactJoinResult result
			= cluster_recovery_native_inventory_multixact_step(inventory, roots, 65536, &out, diag);
		if (result == CLUSTER_RECOVERY_MX_JOIN_ENTRY) {
			if (entries)
				(*entries)++;
			UT_ASSERT(out.record != NULL);
			UT_ASSERT(out.record->source.claim.identity.origin_node_id == 0
					  || out.record->source.claim.identity.origin_node_id == 127);
		} else if (result != CLUSTER_RECOVERY_MX_JOIN_MORE)
			return result;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_MX_JOIN_INVALID;
}

static ClusterRecoveryNativeInventory *
inventory_mx_join_fixture(ClusterWalInputsV1 **inputs, ClusterRecoveryRootSet **roots, int *fd,
						  char path[MAXPGPATH], bool mismatch)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;
	inventory_extra = 6;
	*inputs = inventory_fixture(false, false);
	*roots = inventory_roots(*inputs, path, fd, false);
	inventory_mx_files(*fd, true, mismatch);
	/* Finish fixture creation before capturing the namespace directory cut. */
	cluster_recovery_root_set_destroy(*roots);
	{
		const ClusterWalSourceRef *source = &cluster_wal_inputs_at_v1(*inputs, 0)->source;
		*roots = NULL;
		UT_ASSERT_EQ(cluster_recovery_root_set_begin(*fd, source->claim.identity.system_identifier,
													 source->claim.database_incarnation,
													 source->claim.identity.storage_uuid,
													 1024 * 1024, roots),
					 CLUSTER_RECOVERY_ROOTS_MORE);
	}
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(*inputs, *roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	do {
		result = cluster_recovery_root_set_step(*roots, &event);
	} while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	return inventory;
}

UT_TEST(test_cold_mx_join_visits_all_saved_creates_and_preserves_effects)
{
	for (unsigned mismatch = 0; mismatch < 2; ++mismatch) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		unsigned entries = 0, effects = 0;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeInventory *inventory
			= inventory_mx_join_fixture(&inputs, &roots, &fd, path, mismatch);
		UT_ASSERT_EQ(inventory_mx_join_drain(inventory, roots, &diag, &entries),
					 CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		UT_ASSERT_EQ(entries, 6);
		UT_ASSERT_EQ(diag.mx_compared_offsets, 2);
		UT_ASSERT_EQ(diag.mx_compared_members, 4);
		UT_ASSERT_EQ(diag.mx_physical_mismatches, mismatch ? 2 : 0);
		UT_ASSERT(diag.unowned_target_records >= 8);
		while (cluster_recovery_native_inventory_next(inventory, &effect)
			   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT)
			effects += effect.kind == CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER;
		UT_ASSERT_EQ(effects, 4);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
		UT_ASSERT_EQ(inventory_mx_join_drain(inventory, roots, &diag, NULL),
					 CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_mx_files(fd, false, false);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_cold_mx_join_final_wal_drift_and_destroy_revoke_roots)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		uint8 digest[32];
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryMultixactJoinEntry out;
		ClusterRecoveryNativeInventory *inventory
			= inventory_mx_join_fixture(&inputs, &roots, &fd, path, false);
		if (mode == 0) {
			ClusterRecoveryMultixactJoinResult result;
			do {
				result = cluster_recovery_native_inventory_multixact_step(inventory, roots, 65536,
																		  &out, &diag);
			} while (result == CLUSTER_RECOVERY_MX_JOIN_MORE);
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_MX_JOIN_ENTRY);
		} else
			UT_ASSERT_EQ(inventory_mx_join_drain(inventory, roots, &diag, NULL),
						 CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		if (mode < 2) {
			inventory_mx_change_family = false;
			inventory_mx_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
			UT_ASSERT(cluster_recovery_native_inventory_multixact_step(inventory, roots, 65536,
																	   &out, &diag)
					  > CLUSTER_RECOVERY_MX_JOIN_DRAINED);
			UT_ASSERT(out.record == NULL);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		inventory_mx_files(fd, false, false);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_cold_mx_join_refusal_poisons_inventory_and_partial_roots)
{
	for (unsigned mode = 0; mode < 4; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;
		uint8 digest[32];
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryMultixactJoinEntry out;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeInventory *inventory
			= inventory_mx_join_fixture(&inputs, &roots, &fd, path, false);
		if (mode == 0)
			cluster_recovery_root_set_invalidate(roots);
		if (mode == 3)
			UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
						 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT(cluster_recovery_native_inventory_multixact_step(
					  inventory, roots, mode == 1 ? 1 : 65536, mode == 2 ? NULL : &out, &diag)
				  > CLUSTER_RECOVERY_MX_JOIN_DRAINED);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_INVALID);
		UT_ASSERT(effect.record == NULL);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		if (mode == 3) {
			ClusterRecoveryRootsEvent event;
			ClusterRecoveryRootsResult r;
			do {
				r = cluster_recovery_root_set_step(roots, &event);
			} while (r < CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT(r > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		inventory_mx_files(fd, false, false);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}
