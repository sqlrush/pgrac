/* Actual configured sharedfs/sentinel input before original native custody.
 * The remaining native ownership/provider limits are unchanged.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
cold_storage_setup(unsigned sample)
{
	cold_native_real_scope = cold_native_real_clog = cold_native_real_storage = true;
	custody_era_offset = sample & 4 ? UINT64CONST(1) << 32 : 0;
	cold_native_setup((sample & 1) != 0, (sample & 2) != 0, false);
	test_relmap_native_storage_setup(test_root, execution_root.identity.storage_uuid);
}

static void
cold_storage_done(ClusterRelmapRetentionCensus *census)
{
	TestRelmapNativeStorageStats result;
	cluster_relmap_retention_census_destroy(census);
	result = test_relmap_native_storage_done();
	UT_ASSERT_EQ(result.live_fds, 0);
	UT_ASSERT_EQ(result.assertion_failed, 0);
	UT_ASSERT(result.sentinel_unchanged);
	cold_native_done(NULL);
	custody_era_offset = 0;
	cold_native_real_scope = cold_native_real_clog = cold_native_real_storage = false;
}

static void
cold_storage_pending_unchanged(void)
{
	ClusterRelmapV2View view = { 0 };
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.pending_generation, 8);
	UT_ASSERT_EQ(view.pending_owner.top_full_xid, custody_era_offset + 99);
	UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks + prefix_base_reads, 0);
	UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL && cold_original_id() != 0);
}

UT_TEST(test_cold_original_storage_full_native_chain_and_fd_custody)
{
	for (unsigned sample = 0; sample < 8; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out = { 0 };
		TestRelmapNativeStorageStats stats;
		bool aborted = (sample & 2) != 0;
		uint64 serial;
		cold_storage_setup(sample);
		serial = cold_original_id();
		UT_ASSERT(cold_native_execute(&census));
		UT_ASSERT(census != NULL && execution_root_fd >= 0);
		stats = test_relmap_native_storage_stats();
		UT_ASSERT_EQ(stats.calls, 1);
		UT_ASSERT_EQ(stats.reads, 2);
		UT_ASSERT_EQ(stats.live_fds, 1);
		UT_ASSERT(stats.readonly_cloexec && stats.sentinel_unchanged);
		UT_ASSERT_EQ(stats.assertion_failed, 0);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals, !aborted);
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(
			census, custody_image.identity.dbid, &out));
		UT_ASSERT(out.present && out.generation == (aborted ? 7 : 8));
		UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, aborted ? 40000 : 40001);
		UT_ASSERT_EQ(cold_original_id(), serial);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		cold_storage_done(census);
		UT_ASSERT(cold_native_clog_last.calls >= 3);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_original_storage_rejects_unqualified_sentinel_before_native)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		TestRelmapNativeStorageStats stats;
		cold_storage_setup(fault & 3);
		test_relmap_native_storage_fault(fault);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL);
		stats = test_relmap_native_storage_stats();
		UT_ASSERT_EQ(stats.calls, 1);
		UT_ASSERT_EQ(stats.live_fds, 0);
		UT_ASSERT(stats.sentinel_unchanged && stats.assertion_failed == 0);
		cold_storage_pending_unchanged();
		cold_storage_done(census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 0);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_original_storage_midread_change_and_error_preserve_pending)
{
	for (unsigned fault = 7; fault <= 9; fault++) {
		ClusterRelmapRetentionCensus *volatile census = NULL;
		volatile bool caught = false;
		TestRelmapNativeStorageStats stats;
		cold_storage_setup(0);
		test_relmap_native_storage_fault(fault);
		PG_TRY();
		{
			UT_ASSERT(!cold_native_execute((ClusterRelmapRetentionCensus **)&census));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 8);
		UT_ASSERT(census == NULL);
		stats = test_relmap_native_storage_stats();
		UT_ASSERT_EQ(stats.calls, 1);
		UT_ASSERT_EQ(stats.reads, 2);
		UT_ASSERT_EQ(stats.live_fds, 0);
		UT_ASSERT(stats.sentinel_unchanged && stats.assertion_failed == 0);
		cold_storage_pending_unchanged();
		cold_storage_done((ClusterRelmapRetentionCensus *)census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 0);
		if (ut_current_failed)
			return;
	}
}
