/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_wal_sources.inc
 *   Original Startup binding for complete WAL source observations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_cold_ctrc_wal_sources.inc
 *
 * NOTES
 *   PGRAC-original fixture fragment; original Start and cleanup are real,
 *   while physical census leaves use the existing controlled fixture.
 *-------------------------------------------------------------------------
 */

UT_TEST(test_ctrc_wal_sources_bind_complete_original_start_and_do_not_complete_service)
{
	ClusterRecoveryWalSourceObservationV1 value, zero = {0};
	ClusterRecoveryCtrcResultValueV1 result;
	uint32 count = 99;
	observation_two_ready(); result = window_region.checkpoint_owner.ctrc_result;
	UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcWalSourceCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, observation_manifest.source_count);
	UT_ASSERT(ClusterColdRecoveryCtrcWalSourceAt(info.cluster_tx_start, &native_ctrc_work, 0, &value));
	UT_ASSERT(memcmp(&value, &observation_wal_sources[0], sizeof(value)) == 0);
	value.observed.records++;
	UT_ASSERT(ClusterColdRecoveryCtrcWalSourceAt(info.cluster_tx_start, &native_ctrc_work, 0, &value));
	UT_ASSERT_EQ(value.observed.records, observation_manifest.wal_record_count);
	memset(&value, 0xa5, sizeof(value));
	UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceAt(info.cluster_tx_start, &native_ctrc_work, count, &value));
	UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
	UT_ASSERT_EQ(observation_scope_begins, 1); UT_ASSERT_EQ(observation_census_begins, 1);
	UT_ASSERT(memcmp(&result, &window_region.checkpoint_owner.ctrc_result, sizeof(result)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_ctrc_wal_sources_missing_count_drift_and_error_revoke_original_observation)
{
	for (volatile unsigned fault = 1; fault <= 5; fault++) {
		volatile bool caught = false;
		volatile uint32 count = 99;
		volatile ClusterRecoveryWalSourceObservationV1 value;
		ClusterRecoveryWalSourceObservationV1 zero = {0};
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		observation_wal_source_fault = fault;
		PG_TRY();
		{
			if (fault < 4)
				UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceCount(info.cluster_tx_start,
					&native_ctrc_work, (uint32 *)&count));
			else {
				count = 0; memset((void *)&value, 0xa5, sizeof(value));
				UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceAt(info.cluster_tx_start,
					&native_ctrc_work, 0, (ClusterRecoveryWalSourceObservationV1 *)&value));
			}
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3 || fault == 5); UT_ASSERT_EQ(count, 0);
		if (fault >= 4) UT_ASSERT(memcmp((const void *)&value, &zero, sizeof(value)) == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(strstr(observation_events, "DR") != NULL);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		observation_wal_source_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_wal_sources_require_exact_start_work_owner_root_and_accepted_tx)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		const struct ClusterRecoveryStartV1 *start;
		const ClusterColdCtrcWorkV1 *work;
		ClusterRecoveryWalSourceObservationV1 value, zero = {0};
		uint32 count = 99;
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		start = info.cluster_tx_start; work = &native_ctrc_work;
		if (fault == 0) start = (const struct ClusterRecoveryStartV1 *)(uintptr_t)1;
		if (fault == 1) work = (const ClusterColdCtrcWorkV1 *)&typed;
		if (fault == 2) owner_boot++;
		if (fault == 3) native_ctrc_work.entries[1].slot.xid++;
		if (fault == 4) window_region.checkpoint_owner.tx_accepted_generation = 0;
		if (fault == 5) typed.census_root.image_sha256[31]++;
		UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceCount(start, work, &count));
		UT_ASSERT_EQ(count, 0); memset(&value, 0xa5, sizeof(value));
		UT_ASSERT(!ClusterColdRecoveryCtrcWalSourceAt(start, work, 0, &value));
		UT_ASSERT(memcmp(&value, &zero, sizeof(value)) == 0);
		if (fault == 2) owner_boot--;
		release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_ctrc_wal_sources_no_page_input_is_not_a_missing_source_set)
{
	ClusterRecoveryWalSourceObservationV1 value;
	uint32 count = 99;
	observation_two_ready(); observation_page_wal_count = 0;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcWalSourceCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT(ClusterColdRecoveryCtrcWalSourceAt(info.cluster_tx_start, &native_ctrc_work, 0, &value));
	UT_ASSERT_EQ(value.observed.records, observation_manifest.wal_record_count);
	UT_ASSERT(ClusterColdRecoveryCtrcPageWalCount(info.cluster_tx_start, &native_ctrc_work, &count));
	UT_ASSERT_EQ(count, 0); ClusterReleaseColdRecoveryInputs(&info);
	observation_two_ready(); observation_wal_source_count = 0;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
	ClusterReleaseColdRecoveryInputs(&info);
}
