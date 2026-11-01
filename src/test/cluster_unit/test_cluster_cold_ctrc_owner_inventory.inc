/* Complete original PAGE owner -> local canonical inventory.
 * PAGE/authority/FD transport remains the existing controlled leaf.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static RfPageCtrcOwnersV1
owner_inventory_run(void)
{
	return ClusterColdRecoveryCtrcPageOwners(info.cluster_tx_start, 1024 * 1024);
}

static void
owner_inventory_observe(unsigned mode)
{
	owner_round_visits = 0; owner_round_mode = mode; owner_round_waited = false;
	memset(owner_round_trace, 0, sizeof(owner_round_trace));
	startup_image_observer = owner_round_observe;
}

UT_TEST(test_startup_owner_inventory_builds_complete_actual_page_witnesses)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		RfPageCtrcOwnersV1 inventory;
		unsigned allocated, freed;
		startup_image_ready(unchanged != 0); allocated = allocations; freed = frees;
		inventory = owner_inventory_run();
		UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(inventory.count, 2); UT_ASSERT(inventory.entries != NULL);
		for (uint32 i = 0; i < inventory.count; i++) {
			const RfPageCtrcOwnerEvidenceV1 *entry = &inventory.entries[i];
			const RfPageCtrcOwnerProofV1 *proof = &entry->completed.owner;
			const uint8 *id = entry->evidence + 26;
			Size path_length = cluster_undo_tt_ctrc_get_u64_le(id + 22);
			Size id_length = 46 + path_length;
			char *path = relpathperm(proof->space.key.locator, MAIN_FORKNUM);
			UT_ASSERT_EQ(proof->detail, RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(proof->target_index, i); UT_ASSERT_EQ(proof->target_count, 2);
			UT_ASSERT_EQ(proof->owner_family, 10); UT_ASSERT_EQ(proof->rule, unchanged ? 6 : 1);
			UT_ASSERT_EQ(proof->blockno, 23 + i);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(entry->evidence), 1);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(entry->evidence + 16), 2);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(id), proof->space.key.system_identifier);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(id + 8), proof->space.key.database_incarnation);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(id + 18), CLUSTER_RECOVERY_FILE_RELATION);
			UT_ASSERT_EQ(path_length, strlen(path)); UT_ASSERT(memcmp(id + 30, path, path_length) == 0);
			pfree(path);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(id + 30 + path_length), 0);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(id + id_length), (uint64)(23 + i) * BLCKSZ);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(id + id_length + 8), BLCKSZ);
			UT_ASSERT(memcmp(id + id_length + 24, startup_image_expected.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(entry->evidence_length, 50 + id_length + BLCKSZ);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(entry->completed.completion), 1);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(entry->completed.completion + 2), proof->rule);
			UT_ASSERT(cluster_recovery_ctrc_witness_ids_in_evidence_v2(entry->completed.completion + 4,
				entry->completed.length - 4, entry->evidence, entry->evidence_length));
		}
		UT_ASSERT_EQ(startup_image_calls, 2); UT_ASSERT_EQ(prefix_steps, 2);
		pfree(inventory.entries);
		UT_ASSERT_EQ(allocations - allocated, frees - freed);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_inventory_later_target_cannot_replace_first_operation)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		RfPageCtrcOwnersV1 inventory;
		unsigned allocated, freed;
		startup_image_ready(unchanged != 0); owner_inventory_observe(1);
		allocated = allocations; freed = frees; inventory = owner_inventory_run();
		startup_image_observer = NULL;
		UT_ASSERT(inventory.detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
		UT_ASSERT_EQ(allocations - allocated, frees - freed);
		UT_ASSERT(info.cluster_tx_start->revoked && pending_storage_mask == 3);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_inventory_wait_publishes_no_prefix_and_restarts_first)
{
	RfPageCtrcOwnersV1 inventory;
	unsigned allocated, freed;
	startup_image_ready(false); owner_inventory_observe(3);
	allocated = allocations; freed = frees; inventory = owner_inventory_run();
	UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
	UT_ASSERT_EQ(allocations - allocated, frees - freed);
	UT_ASSERT(!info.cluster_tx_start->revoked && pending_storage_mask == 3);
	UT_ASSERT_EQ(owner_round_visits, 2); UT_ASSERT_EQ(prefix_steps, 2);
	owner_inventory_observe(0); ctrc_driver_waiting = false;
	inventory = owner_inventory_run(); startup_image_observer = NULL;
	UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT_EQ(inventory.count, 2);
	UT_ASSERT_EQ(owner_round_visits, 2); UT_ASSERT_EQ(owner_round_trace[0], 0);
	UT_ASSERT_EQ(owner_round_trace[1], 1); UT_ASSERT_EQ(prefix_steps, 2);
	pfree(inventory.entries); retirement_local_cleanup();
}

UT_TEST(test_startup_owner_inventory_error_frees_only_local_output_and_keeps_debt)
{
	volatile bool caught = false;
	unsigned allocated, freed;
	startup_image_ready(false); owner_inventory_observe(2);
	allocated = allocations; freed = frees;
	PG_TRY(); { (void)owner_inventory_run(); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	startup_image_observer = NULL;
	UT_ASSERT(caught && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(allocations - allocated, frees - freed);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
	UT_ASSERT_EQ(pending_storage_closes, 0); retirement_local_cleanup();
}

UT_TEST(test_startup_owner_inventory_preflights_every_target_without_partial_reads)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		RfPageCtrcOwnersV1 inventory;
		ClusterSharedFsPageV1 *storage;
		startup_image_ready(false);
		storage = info.cluster_tx_start->ctrc_page_progress->completed[1].storage;
		if (fault == 0) info.cluster_tx_start->ctrc_page_progress->completed[1].storage = NULL;
		else prefix_targets[1].history_only = true;
		inventory = owner_inventory_run();
		UT_ASSERT(inventory.detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
		UT_ASSERT_EQ(startup_image_calls, 0);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		info.cluster_tx_start->ctrc_page_progress->completed[1].storage = storage;
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_inventory_budget_and_oom_refuse_before_first_page)
{
	RfPageCtrcOwnersV1 inventory;
	startup_image_ready(false);
	inventory = ClusterColdRecoveryCtrcPageOwners(info.cluster_tx_start, 1);
	UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
	UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	{
		Size scratch = sizeof(inventory) + 2 * sizeof(RfPageCtrcOwnerProofV1)
			+ 2 * (MAXPGPATH + 92) + 2 * MAXPGPATH;
		Size complete = MAXALIGN(2 * sizeof(RfPageCtrcOwnerEvidenceV1))
			+ 2 * sizeof(ClusterColdCtrcPageCompletedV1) + scratch;
		ClusterColdCtrcPageProgressV1 *progress = info.cluster_tx_start->ctrc_page_progress;
		inventory = ClusterColdRecoveryCtrcPageOwners(info.cluster_tx_start, complete - 1);
		UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT_EQ(startup_image_calls, 0);
		progress->target_count = progress->next_target = UINT32_MAX;
		inventory = ClusterColdRecoveryCtrcPageOwners(info.cluster_tx_start, MaxAllocSize);
		progress->target_count = progress->next_target = 2;
		UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
		UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	}
	allocation_ok = false; inventory = owner_inventory_run(); allocation_ok = true;
	UT_ASSERT_EQ(inventory.detail, RF_PAGE_PROOF_DETAIL_OOM);
	UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	inventory = ClusterColdRecoveryCtrcPageOwners(NULL, 1024 * 1024);
	UT_ASSERT_EQ(inventory.count, 0); UT_ASSERT(inventory.entries == NULL);
	UT_ASSERT_EQ(startup_image_calls, 0); retirement_local_cleanup();
}
