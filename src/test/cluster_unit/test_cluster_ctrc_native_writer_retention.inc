/*-------------------------------------------------------------------------
 * test_cluster_ctrc_native_writer_retention.inc
 *     Retain the actual FPI writer in the original recovery episode.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * NOTES
 *     Real all-replay inputs, PAGE, Generic/F4 and raw-file installer run.
 *     Installed writer/Startup/GES/WAL transport remain controlled leaves.
 *-------------------------------------------------------------------------
 */
UT_TEST(test_native_fpi_refuses_writer_outside_original_retention)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f;
		PGAlignedBlock canonical, disk;
		RfPageCtrcStepResultV1 result;
		unsigned grants, releases;

		/* The original failed-source pin still covers both original threads.
		 * The installed-writer leaf supplies another, unretained thread before
		 * scope creation, so its full value remains current throughout. */
		native_fpi_unretained_writer = true;
		native_durable_fixture(&f, source, &canonical);
		native_fpi_unretained_writer = false;
		UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&native_stamp_source));
		UT_ASSERT_EQ(ctrc_inputs_owner.writer.ref.claim.identity.origin_thread_id, 2);
		UT_ASSERT(!cluster_wal_read_pin_covers_v1(native_stamp_source.input_owner.pin, 2));
		grants = test_tx_ir_grants; releases = test_tx_ir_releases;
		result = native_durable_step(&f, &canonical);
		UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT_EQ(native_insert_flushes, 0);
		UT_ASSERT_EQ(native_install_writes, 0); UT_ASSERT_EQ(native_install_syncs, 0);
		UT_ASSERT_EQ(native_durable_calls, 0);
		UT_ASSERT_EQ(pread(f.main_fd, disk.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(canonical.data, disk.data, BLCKSZ) == 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(test_tx_ir_grants, grants); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		native_durable_finish(&f);
	}
}
