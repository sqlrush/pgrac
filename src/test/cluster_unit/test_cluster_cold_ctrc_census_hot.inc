/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_census_hot.inc
 *     Original Startup consumes untouched and inverse local HOT inputs.
 *     Census transport and the separately tested native HOT leaf are controlled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static bool census_hot_check;
static unsigned census_hot_calls, census_hot_fault;

bool
cluster_ctrc_recovery_heap_hot_items_v1(const ClusterItlSlotData *slots,
	const ClusterCtrcRecoveryHeapItemV1 *items, Size count, BlockNumber block_number)
{
	if (!census_hot_check) return true;
	census_hot_calls++;
	UT_ASSERT(slots != NULL && items != NULL && count == 1);
	UT_ASSERT_EQ(slots[0].flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(slots[0].xid, native_ctrc_work.entries[0].key.xid);
	UT_ASSERT_EQ(block_number, observation_pages.entries[1].blockno);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	UT_ASSERT_EQ(items[0].tuple_length, 64);
	UT_ASSERT(items[0].tuple_bytes == reference_tuple_bytes[census_hot_calls % 2 == 1 ? 0 : 1]);
	if (census_hot_calls == 2) {
		if (census_hot_fault == 1) return false;
		if (census_hot_fault == 2) owner_boot++;
		if (census_hot_fault == 3) pg_re_throw();
	}
	return true;
}

static ClusterRecoveryCensusReferencesV1 *
census_hot_ready(void)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_tuples_ready();

	census_hot_check = true; census_hot_calls = census_hot_fault = 0;
	return references;
}

static void
census_hot_cleanup(void)
{
	census_hot_check = false;
	retirement_local_cleanup();
}

UT_TEST(test_startup_census_hot_checks_untouched_current_and_inverse_without_mutation)
{
	ClusterRecoveryCensusReferencesV1 *references = census_hot_ready();
	ClusterRecoveryCensusReferenceV1 saved[18];

	memcpy(saved, references->entries, sizeof(saved));
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(census_hot_calls, 2);
	UT_ASSERT(memcmp(saved, references->entries, sizeof(saved)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	census_hot_cleanup();
}

UT_TEST(test_startup_census_hot_late_refusal_current_drift_and_error_keep_debt)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		(void)census_hot_ready(); census_hot_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(census_hot_calls, 2);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		census_hot_cleanup();
	}
}

UT_TEST(test_startup_census_hot_rejects_missing_duplicate_and_gapped_items)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryCensusReferencesV1 *references = census_hot_ready();

		if (fault == 0) references->entries[17].entry.index = 2;
		if (fault == 1) {
			references->entries[18] = references->entries[17]; references->count++;
		}
		if (fault == 2) references->entries[16].version++;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live);
		census_hot_cleanup();
	}
}

UT_TEST(test_startup_census_hot_repeat_does_not_cache_topology_success)
{
	unsigned steps, opens;

	(void)census_hot_ready(); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	census_hot_calls = 0; census_hot_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(census_hot_calls, 2);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	census_hot_cleanup();
}

UT_TEST(test_startup_census_hot_zero_page_wal_still_checks_saved_topology)
{
	(void)census_hot_ready(); prefix_count = 0;
	ctrc_wait_step = reference_terminals_empty_probe; census_hot_fault = 1;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(census_hot_calls, 2);
	UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(prefix_pages, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	census_hot_cleanup();
}
