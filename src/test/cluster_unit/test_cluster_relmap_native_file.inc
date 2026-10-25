/* Original native-file continuity through actual cold custody and pair I/O.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_cold_native_replaced_file_refuses_before_pair_write)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 8; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view = { 0 };
		custody_era_offset = sample & 4 ? UINT64CONST(1) << 32 : 0;
		cold_native_setup(sample & 1, sample & 2, false);
		test_relmap_native_clog_fault(7, 2);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL && cold_original_serial.held && cold_original_plan != NULL);
		UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.has_pending && view.pending_complete && view.committed_generation == 7);
		cold_native_done(census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 2);
		if (ut_current_failed)
			break;
	}
	custody_era_offset = 0;
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_native_replaced_file_after_pair_sync_keeps_debt)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 4; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view = { 0 };
		bool aborted = (sample & 2) != 0;
		cold_native_setup(sample & 1, aborted, false);
		test_relmap_native_clog_fault(7, 3);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL && cold_original_serial.held && cold_original_plan != NULL);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && !view.has_pending);
		UT_ASSERT_EQ(view.committed_generation, aborted ? 7 : 8);
		UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, aborted ? 40000 : 40001);
		cold_native_done(census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 3);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_native_replaced_file_permanently_invalidates_old_handle)
{
	ClusterRelmapRecoveryHandle *handle = NULL;
	ClusterRelmapV2View view = { 0 };
	ClusterRelmapRecoveryTerminalV1 out, zero = { 0 };
	cold_native_real_scope = cold_native_real_clog = true;
	cold_native_setup(false, false, false);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
	UT_ASSERT(handle != NULL);
	test_relmap_native_clog_fault(7, 2);
	UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
	test_relmap_native_clog_fault(0, 0);
	UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_recovery_terminal_read(handle, &custody_image.identity, &view, &out));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
	UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
	cluster_relmap_recovery_handle_release_v1(&handle);
	UT_ASSERT(handle == NULL);
	cold_native_done(NULL);
	UT_ASSERT_EQ(cold_native_clog_last.calls, 2);
	cold_native_real_scope = cold_native_real_clog = false;
}
