/* Real cold ROOT/claim/WAL and origin CLOG/SUBTRANS files.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static void
ij_files(int fd, bool create)
{
	char dir[MAXPGPATH], path[MAXPGPATH];
	if (create)
		UT_ASSERT_EQ(mkdirat(fd, "native_side", 0700), 0);
	for (unsigned i = 0; i < 2; ++i) {
		unsigned node = i ? 127 : 0;
		snprintf(dir, sizeof(dir), "native_side/origin_%u", node);
		if (create)
			UT_ASSERT_EQ(mkdirat(fd, dir, 0700), 0);
		for (unsigned parent = 0; parent < 2; ++parent) {
			snprintf(dir, sizeof(dir), "native_side/origin_%u/%s", node,
					 parent ? "pg_subtrans" : "pg_xact");
			snprintf(path, sizeof(path), "%s/%s", dir, parent ? "0040" : "0004");
			if (create) {
				PGAlignedBlock pages[2] = { 0 };
				int file;
				uint32 value = 4196000;
				UT_ASSERT_EQ(mkdirat(fd, dir, 0700), 0);
				if (parent) {
					memcpy(pages[0].data
							   + (4196001 % CLUSTER_NATIVE_SUBTRANS_PER_PAGE) * sizeof(value),
						   &value, sizeof(value));
					if (node == 127)
						value = 0;
					memcpy(pages[1].data, &value, sizeof(value));
				} else
					pages[0].data[(4195136 % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4]
						= node == 127 ? TRANSACTION_STATUS_SUB_COMMITTED
									  : TRANSACTION_STATUS_COMMITTED;
				file = openat(fd, path, O_CREAT | O_WRONLY, 0600);
				UT_ASSERT(file >= 0);
				UT_ASSERT_EQ(write(file, pages, (parent ? 2 : 1) * BLCKSZ),
							 (parent ? 2 : 1) * BLCKSZ);
				UT_ASSERT_EQ(close(file), 0);
			} else {
				UT_ASSERT_EQ(unlinkat(fd, path, 0), 0);
				UT_ASSERT_EQ(unlinkat(fd, dir, AT_REMOVEDIR), 0);
			}
		}
		snprintf(dir, sizeof(dir), "native_side/origin_%u", node);
		if (!create)
			UT_ASSERT_EQ(unlinkat(fd, dir, AT_REMOVEDIR), 0);
	}
	if (!create)
		UT_ASSERT_EQ(unlinkat(fd, "native_side", AT_REMOVEDIR), 0);
}

static ClusterRecoveryNativeInventory *
ij_fixture(ClusterWalInputsV1 **inputs, ClusterRecoveryRootSet **roots, int *fd, char *path)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;
	const ClusterWalSourceRef *source;
	inventory_extra = 13;
	*inputs = inventory_fixture(false, false);
	*roots = inventory_roots(*inputs, path, fd, false);
	ij_files(*fd, true);
	cluster_recovery_root_set_destroy(*roots);
	source = &cluster_wal_inputs_at_v1(*inputs, 0)->source;
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(*fd, source->claim.identity.system_identifier,
												 source->claim.database_incarnation,
												 source->claim.identity.storage_uuid, 1024 * 1024,
												 roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_collect_roots(*inputs, *roots, 1024 * 1024,
																 &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	do {
		result = cluster_recovery_root_set_step(*roots, &event);
	} while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	return inventory;
}

static ClusterRecoveryXactJoinResult
ij_drain(ClusterRecoveryNativeInventory *inventory, ClusterRecoveryRootSet *roots,
		 ClusterRecoveryNativeInventoryDiag *diag, unsigned *count)
{
	ClusterRecoveryXactJoinEntry entry;
	for (unsigned i = 0; i < 10000; ++i) {
		ClusterRecoveryXactJoinResult r
			= cluster_recovery_native_inventory_xact_step(inventory, roots, 65536, &entry, diag);
		if (r == CLUSTER_RECOVERY_XACT_JOIN_ENTRY) {
			if (count)
				(*count)++;
			UT_ASSERT(entry.record != NULL);
			UT_ASSERT_EQ(entry.expected_value,
						 entry.is_parent ? 4196000 : TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(entry.is_top, !entry.is_parent);
			UT_ASSERT_EQ(entry.xid, !entry.is_parent ? 4195136 : entry.ordinal ? 4196352 : 4196001);
			UT_ASSERT_EQ(entry.record->source.claim.identity.origin_node_id,
						 entry.record->cut.failed_thread - 1);
		} else if (r != CLUSTER_RECOVERY_XACT_JOIN_MORE)
			return r;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_XACT_JOIN_INVALID;
}

UT_TEST(test_cold_xact_join_keeps_every_source_and_raw_effect)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryRootSet *roots;
	char path[MAXPGPATH];
	int fd;
	unsigned count = 0, terms = 0, assignments = 0, children = 0;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeInventory *inventory = ij_fixture(&inputs, &roots, &fd, path);
	UT_ASSERT_EQ(ij_drain(inventory, roots, &diag, &count), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
	UT_ASSERT_EQ(count, 6);
	UT_ASSERT_EQ(diag.xact_clog_compared, 2);
	UT_ASSERT_EQ(diag.xact_parents_compared, 4);
	UT_ASSERT_EQ(diag.xact_physical_mismatches, 2);
	while (cluster_recovery_native_inventory_next(inventory, &effect)
		   == CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT) {
		terms += effect.kind == CLUSTER_RECOVERY_NATIVE_TERMINAL;
		assignments += effect.kind == CLUSTER_RECOVERY_NATIVE_ASSIGNMENT;
		children += effect.kind == CLUSTER_RECOVERY_NATIVE_SUBXACT;
	}
	UT_ASSERT_EQ(terms, 2);
	UT_ASSERT_EQ(assignments, 2);
	UT_ASSERT_EQ(children, 4);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT_EQ(ij_drain(inventory, roots, &diag, NULL), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	cluster_recovery_native_inventory_destroy(inventory);
	ij_files(fd, false);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_cold_xact_join_final_wal_roots_destroy_revoke_outputs)
{
	for (unsigned mode = 0; mode < 4; ++mode) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		uint8 digest[32];
		int fd;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryXactJoinEntry out;
		ClusterRecoveryNativeInventory *inventory = ij_fixture(&inputs, &roots, &fd, path);
		UT_ASSERT_EQ(ij_drain(inventory, roots, &diag, NULL), CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
		if (mode == 0) {
			inventory_assignment_drift();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
						 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		} else if (mode == 1)
			cluster_recovery_root_set_invalidate(roots);
		if (mode != 2) {
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(cluster_recovery_native_inventory_xact_step(
						  inventory, roots, mode == 3 ? 32768 : 65536, &out, &diag)
					  > CLUSTER_RECOVERY_XACT_JOIN_DRAINED);
			UT_ASSERT(out.record == NULL && out.xid == 0);
		}
		cluster_recovery_native_inventory_destroy(inventory);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		ij_files(fd, false);
		inventory_roots_done(roots, fd, path);
		cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_owned_xact_join_still_requires_final_whole_inputs)
{
	for (unsigned mode = 0; mode < 2; ++mode) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensusDiag diag;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_EFFECTS),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(diag.xact_comparisons, 2);
		if (mode == 0) {
			uint8 byte = TRANSACTION_STATUS_COMMITTED;
			int file = openat(fd, "native_side/origin_0/pg_xact/0004", O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(pwrite(file, &byte, 1, (4195136 % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4), 1);
			UT_ASSERT_EQ(close(file), 0);
		} else {
			inventory_fault = 0;
			inventory_drift();
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_REFUSED);
		owned_census_done(census, &inputs, fd, path);
	}
}
