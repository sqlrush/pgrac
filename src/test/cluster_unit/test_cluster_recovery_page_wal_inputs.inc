/* Real native PAGE WAL inputs; transport/CRC/edges use the original reader.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

#ifdef CTRC_PAGE_WAL_REFUSE_ONLY
bool
cluster_recovery_native_inventory_page_count_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 *out)
{
	(void)inventory; if (out != NULL) *out = 0; return false;
}
bool
cluster_recovery_native_inventory_page_at_v1(ClusterRecoveryNativeInventory *inventory,
	uint32 index, ClusterRecoveryPageWalV1 *out)
{
	(void)inventory; (void)index;
	if (out != NULL) memset(out, 0, sizeof(*out));
	return false;
}
#endif

UT_TEST(test_page_wal_inputs_keep_all_components_payloads_and_original_sources)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPageWalV1 value, zero = {0};
	ClusterWalInputsV1 *inputs;
	uint32 count = 99;
	inventory_extra = 18; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 2); UT_ASSERT_EQ(inventory_census_calls, 2);
	for (uint32 i = 0; i < count; i++) {
		const DecodedXLogRecord *record;
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, i, &value));
		if (value.reader == NULL) continue;
		record = value.reader->record;
		UT_ASSERT_EQ(value.source_index, i);
		UT_ASSERT_EQ(value.source.claim.identity.origin_thread_id, i ? 128 : 1);
		UT_ASSERT_EQ(value.ordinal, 2); /* checkpoint, commit, PAGE */
		UT_ASSERT_EQ(value.cut.origin_owner_incarnation,
			value.source.claim.identity.origin_owner_incarnation);
		UT_ASSERT_EQ(value.reader->system_identifier, TEST_SYSID);
		UT_ASSERT_EQ(value.reader->ReadRecPtr, record->lsn);
		UT_ASSERT_EQ(value.reader->EndRecPtr, record->next_lsn);
		UT_ASSERT_EQ(record->header.xl_rmid, RM_GENERIC_ID);
		UT_ASSERT_EQ(record->page_version_edge.entry_count, 2);
		UT_ASSERT_EQ(record->page_version_edge.result_token, 77);
		UT_ASSERT_EQ(record->page_version_edge.entries[0].before.mutation_token, 70);
		UT_ASSERT_EQ(record->page_version_edge.entries[0].page_class, RF_PAGE_CLASS_ORDINARY);
		UT_ASSERT_EQ(record->page_version_edge.entries[1].page_class,
			RF_PAGE_CLASS_REBUILDABLE_FSM);
		UT_ASSERT_EQ(record->max_block_id, 2); UT_ASSERT(!record->blocks[1].in_use);
		UT_ASSERT_EQ(record->blocks[0].rlocator.relNumber, 16386);
		UT_ASSERT_EQ(record->blocks[0].blkno, 7);
		UT_ASSERT(record->blocks[0].has_image && record->blocks[0].apply_image);
		UT_ASSERT_EQ(record->blocks[0].bimg_len, 64);
		UT_ASSERT_EQ(record->blocks[0].hole_offset, 64);
		UT_ASSERT_EQ(record->blocks[0].hole_length, BLCKSZ - 64);
		UT_ASSERT_EQ(((PageHeader)record->blocks[0].bkp_image)->pd_block_scn, 77);
		UT_ASSERT_EQ(record->blocks[2].forknum, FSM_FORKNUM);
		UT_ASSERT_EQ(record->blocks[2].data_len, 4);
		UT_ASSERT(memcmp(record->blocks[2].data, "edge", 4) == 0);
		UT_ASSERT_EQ(record->main_data_len, 4);
		UT_ASSERT(memcmp(record->main_data, "main", 4) == 0);
	}
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_native_inventory_page_at_v1(inventory, count, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 2);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_wal_inputs_do_not_reset_native_effect_cursor_or_fixed_cut)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryPageWalV1 before, after;
	ClusterWalInputsV1 *inputs;
	uint32 count = 99;
	inventory_extra = 18; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, 0, &before));
	if (before.reader != NULL) {
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag), 0);
		UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &count));
		UT_ASSERT_EQ(count, 2);
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, 0, &after));
		UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
			CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
	}
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

#ifdef CTRC_PAGE_WAL_CENSUS_REFUSE_ONLY
bool
cluster_recovery_census_page_wal_count_v1(ClusterRecoveryCensus *census, uint32 *out)
{
	(void)census; if (out != NULL) *out = 0; return false;
}
bool
cluster_recovery_census_page_wal_at_v1(ClusterRecoveryCensus *census, uint32 index,
	ClusterRecoveryPageWalV1 *out)
{
	(void)census; (void)index; if (out != NULL) memset(out, 0, sizeof(*out)); return false;
}
#endif

UT_TEST(test_page_wal_census_requires_complete_original_observation)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryPageWalV1 value, zero = {0};
	uint32 count = 99;
	char path[MAXPGPATH]; int fd;
	census = owned_census_fixture(&inputs, &fd, path, false);
	UT_ASSERT(!cluster_recovery_census_page_wal_count_v1(census, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &count));
	UT_ASSERT_EQ(count, 0);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_census_page_wal_at_v1(census, 0, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &count));
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_page_wal_census_original_scope_loss_never_rebases)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryPageWalV1 value, zero = {0};
	uint32 count = 99;
	char path[MAXPGPATH]; int fd;
	census = owned_census_fixture(&inputs, &fd, path, false);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &count));
	inputs_pin_current = false;
	UT_ASSERT(!cluster_recovery_census_page_wal_count_v1(census, &count));
	UT_ASSERT_EQ(count, 0);
	inputs_pin_current = true;
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_census_page_wal_at_v1(census, 0, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT(!cluster_recovery_census_page_wal_count_v1(census, &count));
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_page_wal_inputs_empty_is_complete_and_partial_or_failed_is_not)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPageWalV1 value, zero = {0};
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
	uint32 count = 99;
	UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(NULL, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 0);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!cluster_recovery_native_inventory_page_at_v1(inventory, 0, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	inventory->complete = false;
	UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 0);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
}

UT_TEST(test_page_wal_inputs_snapshot_changes_refuse_without_following_rebound_pointers)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryPageWalV1 value, zero = {0};
		ClusterWalInputsV1 *inputs;
		uint32 count = 99;
		uint8 digest[32];
		inventory_extra = 18; inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, 0, &value));
		if (value.reader != NULL) {
			XLogReaderState *reader = (XLogReaderState *)value.reader;
			DecodedXLogRecord *record = reader->record;
			switch (fault) {
			case 0: record->main_data[0] ^= 1; break;
			case 1: record->blocks[0].bkp_image[1] ^= 1; break;
			case 2: record->blocks[2].data[0] ^= 1; break;
			case 3: record->page_version_edge.entries[0].before.mutation_token++; break;
			case 4: record->blocks[0].bkp_image = (char *)(uintptr_t)1; break;
			case 5: record->main_data_len = UINT32_MAX; break;
			case 6: reader->record = (DecodedXLogRecord *)(uintptr_t)1; break;
			case 7: reader->ReadRecPtr++; break;
			}
			UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(inventory, &count));
			UT_ASSERT_EQ(count, 0);
			memset(&value, 0xa5, sizeof(value));
			UT_ASSERT(!cluster_recovery_native_inventory_page_at_v1(inventory, 0, &value));
			UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
			UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, digest));
		}
		cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

static void
page_wal_rewrite(void)
{
	(void)inventory_write_page(&inventory_fault_source, inventory_extra_start,
		inventory_extra_previous, true);
}

UT_TEST(test_page_wal_inputs_crc_valid_original_rewrite_refuses_both_passes)
{
	for (unsigned after = 0; after < 2; after++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterWalInputsV1 *inputs;
		uint32 count = 99;
		inventory_extra = 18; inputs = inventory_fixture(false, false);
		if (!after) inventory_between_passes = page_wal_rewrite;
		UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
			after ? CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED : CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		inventory_between_passes = NULL;
		if (after && inventory != NULL) {
			page_wal_rewrite();
			UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
				CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		}
		UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(inventory, &count));
		UT_ASSERT_EQ(count, 0);
		cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	}
	inventory_extra = 0;
}

UT_TEST(test_page_wal_inputs_charge_original_budget_and_never_export_a_prefix)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalInputsV1 *inputs;
	size_t budget;
	uint32 count = 99;
	inventory_extra = 18; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	budget = diag.owned_bytes;
	UT_ASSERT(budget > 0);
	cluster_recovery_native_inventory_destroy(inventory); inventory = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget - 1, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY);
	UT_ASSERT_NULL(inventory);
	UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 0);
	cold_census_done(&inputs);
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget, &inventory, &diag), 0);
	UT_ASSERT(cluster_recovery_native_inventory_page_count_v1(inventory, &count));
	UT_ASSERT_EQ(count, 2);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}
