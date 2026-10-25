/* Original bound WAL retention pin through the cold native consumer.
 * Lower grant/provider faults are controlled; no native debt is retired.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
cold_original_pin_setup(void)
{
	ClusterWalRetentionInterval interval = { 0 };
	ClusterWalRetentionPinThreadRequest request = { 0 };
	UT_ASSERT(test_relmap_thread_pin_request(cold_original_duty.origin_thread_id,
		&cold_original_duty, &execution_root, &execution_token,
		cold_original_authority.formation, cold_original_authority.fence_need_set,
		cold_original_authority.fence_admission_set, &interval, &request));
	cold_original_authority.retention_pin = test_relmap_native_pin_setup(&request,
		&cold_original_serial, cluster_control_root_revalidate, test_relmap_authority_serial,
		PG_exception_stack);
	UT_ASSERT(cold_original_authority.retention_pin != NULL);
	UT_ASSERT_EQ(test_relmap_native_pin_stats().setup_result, CLUSTER_WAL_PIN_OK);
}

static void
cold_pin_setup(unsigned sample)
{
	cold_native_real_pin = true;
	cold_pin_postwrite_loss = cold_pin_unknown_loss = false;
	cold_storage_setup(sample);
}

static void
cold_pin_done(ClusterRelmapRetentionCensus *census)
{
	TestRelmapNativePinStats stats;
	cluster_relmap_retention_census_destroy(census);
	stats = test_relmap_native_pin_stats();
	UT_ASSERT(stats.active && stats.bound && stats.releases == 0 && stats.assertion_failed == 0);
	cold_storage_done(NULL);
	stats = test_relmap_native_pin_done();
	UT_ASSERT_EQ(stats.releases, 1);
	UT_ASSERT_EQ(stats.assertion_failed, 0);
	UT_ASSERT(!stats.active);
	cold_original_authority.retention_pin = NULL;
	cold_native_real_pin = cold_pin_postwrite_loss = cold_pin_unknown_loss = false;
}

static void
cold_pin_pending_unchanged(void)
{
	ClusterRelmapV2View view = { 0 };
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.pending_generation, 8);
	UT_ASSERT_EQ(view.pending_owner.top_full_xid, custody_era_offset + 99);
	UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
	UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
}

UT_TEST(test_cold_original_pin_full_native_chain_keeps_original_pin)
{
	for (unsigned sample = 0; sample < 8; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapCensusCommitted out = { 0 };
		ClusterWalRetentionPin *original;
		TestRelmapNativePinStats stats;
		bool aborted = (sample & 2) != 0;
		cold_pin_setup(sample);
		original = cold_original_authority.retention_pin;
		UT_ASSERT(cold_native_execute(&census));
		UT_ASSERT(census != NULL);
		UT_ASSERT(cluster_relmap_retention_census_committed_cold(
			census, custody_image.identity.dbid, &out));
		UT_ASSERT(out.present && out.generation == (aborted ? 7 : 8));
		UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, aborted ? 40000 : 40001);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals, !aborted);
		stats = test_relmap_native_pin_stats();
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT(stats.checks > 2 && stats.active && stats.bound && !stats.poisoned);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(cold_original_authority.retention_pin == original && cold_original_serial.held);
		cold_pin_done(census);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_original_pin_owner_context_refuses_before_native)
{
	ClusterWalRetentionInterval interval = { 0 };
	ClusterWalRetentionPinThreadRequest request = { 0 };
	TestRelmapNativePinStats before;
	/* This older ROOT fixture intentionally has no recovered base marker. */
	cold_storage_setup(0);
	UT_ASSERT(!(execution_root.root_flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID));
	UT_ASSERT(test_relmap_thread_pin_request(cold_original_duty.origin_thread_id,
		&cold_original_duty, &execution_root, &execution_token,
		cold_original_authority.formation, cold_original_authority.fence_need_set,
		cold_original_authority.fence_admission_set, &interval, &request));
	UT_ASSERT(test_relmap_native_pin_setup(&request, &cold_original_serial,
		cluster_control_root_revalidate, test_relmap_authority_serial, PG_exception_stack) == NULL);
	before = test_relmap_native_pin_stats();
	UT_ASSERT_EQ(before.setup_result, CLUSTER_WAL_PIN_INVALID);
	UT_ASSERT(before.acquires == 0 && before.releases == 0 && !before.active);
	cold_storage_done(NULL);
	before = test_relmap_native_pin_done();
	UT_ASSERT(before.releases == 0 && before.assertion_failed == 0);
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		TestRelmapNativePinStats stats;
		cold_pin_setup(0);
		test_relmap_native_pin_fault(fault);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL);
		UT_ASSERT_EQ(execution_root_opens + prefix_base_reads, 0);
		cold_pin_pending_unchanged();
		stats = test_relmap_native_pin_stats();
		UT_ASSERT(stats.active && stats.bound && stats.releases == 0);
		UT_ASSERT_EQ(stats.poisoned, fault == 3);
		test_relmap_native_pin_fault(0);
		UT_ASSERT_EQ(test_relmap_native_pin_revalidate(cold_original_authority.retention_pin,
			PG_exception_stack), fault == 3 ? CLUSTER_WAL_PIN_STALE : CLUSTER_WAL_PIN_OK);
		cold_pin_done(census);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_original_pin_late_owner_loss_keeps_durable_pair)
{
	for (unsigned sample = 0; sample < 4; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view = { 0 };
		TestRelmapNativePinStats stats;
		bool aborted = (sample & 2) != 0;
		cold_pin_setup(sample);
		cold_pin_postwrite_loss = true;
		cold_native_post_fault = 5;
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(!view.has_pending && view.pending_complete);
		UT_ASSERT_EQ(view.committed_generation, aborted ? 7 : 8);
		stats = test_relmap_native_pin_stats();
		UT_ASSERT(stats.active && stats.bound && stats.poisoned && stats.releases == 0);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		test_relmap_native_pin_fault(0);
		UT_ASSERT_EQ(test_relmap_native_pin_revalidate(cold_original_authority.retention_pin,
			PG_exception_stack), CLUSTER_WAL_PIN_STALE);
		cold_pin_done(census);
		if (ut_current_failed)
			return;
	}
}

UT_TEST(test_cold_original_pin_unknown_and_poison_never_release_or_retry)
{
	ClusterRelmapRetentionCensus *census = NULL;
	TestRelmapNativePinStats stats;
	cold_pin_setup(0);
	cold_pin_unknown_loss = true;
	test_relmap_native_clog_fault(1, 2);
	UT_ASSERT(!cold_native_execute(&census));
	UT_ASSERT(census == NULL);
	cold_pin_pending_unchanged();
	UT_ASSERT_EQ(prefix_base_reads, 2);
	stats = test_relmap_native_pin_stats();
	UT_ASSERT(stats.active && stats.bound && stats.poisoned && stats.releases == 0);
	test_relmap_native_pin_fault(0);
	UT_ASSERT_EQ(test_relmap_native_pin_revalidate(cold_original_authority.retention_pin,
		PG_exception_stack), CLUSTER_WAL_PIN_STALE);
	cold_pin_done(census);
	UT_ASSERT_EQ(cold_native_clog_last.calls, 2);
}
