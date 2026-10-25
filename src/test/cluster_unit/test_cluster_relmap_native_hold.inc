/* Original native descriptor custody across opaque cold-handle reads.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_cold_native_handle_holds_original_fd_until_release)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 4; sample++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view = { 0 };
		ClusterRelmapRecoveryTerminalV1 terminal;
		cold_native_setup(sample & 1, sample & 2, false);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT(handle != NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(cluster_relmap_recovery_terminal_read(
			handle, &custody_image.identity, &view, &terminal));
		UT_ASSERT_EQ(terminal.status, sample & 2 ? TRANSACTION_STATUS_ABORTED
												  : TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		cluster_relmap_recovery_handle_release_v1(&handle);
		cold_native_done(NULL);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_native_failed_held_read_keeps_fd_and_original_debt_until_cleanup)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 2; sample++) {
		ClusterRelmapRecoveryHandle *handle = NULL;
		ClusterRelmapV2View view = { 0 }, after = { 0 };
		volatile bool caught = false;
		cold_native_setup(false, false, false);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &handle));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		test_relmap_native_clog_fault(sample ? 5 : 1, 2);
		PG_TRY();
		{
			UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(
				handle, &custody_image.identity, &view));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, sample != 0);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(!cluster_relmap_recovery_handle_matches_v1(handle, &custody_image.identity, &view));
		UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 2);
		UT_ASSERT(cold_original_serial.held && cold_original_plan != NULL);
		UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &after),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(after.has_pending && after.pending_complete && after.committed_generation == 7);
		cluster_relmap_recovery_handle_release_v1(&handle);
		UT_ASSERT(handle == NULL);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 0);
		cold_native_done(NULL);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}
