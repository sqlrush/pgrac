/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_graph_step.inc
 *     Fixed graph terminal bytes through the original native PAGE step.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_graph_step.inc
 *
 * NOTES
 *     Actual protected files, native holders, Generic/F4 and install;
 *     original unit transport/Startup/GES boundaries remain controlled.
 *-------------------------------------------------------------------------
 */


static RfPageCtrcStepResultV1
native_graph_step(CtrcReadNativeFixture *f, const PGAlignedBlock *canonical)
{
 return rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
  &native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
  canonical == NULL ? NULL : canonical->data, native_fpi_physical, 2, 1024 * 1024);
}

UT_TEST(test_native_graph_step_accepts_only_storage_checksum_normalization)
{
 {
  CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
  native_step_fixture(&f, 0);
  UT_ASSERT_EQ(pread(f.main_fd, canonical.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
  ((PageHeader)canonical.data)->pd_checksum ^= 0x7123;
  result = native_graph_step(&f, &canonical);
  UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED);
  UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_install_writes, 1); UT_ASSERT_EQ(native_install_syncs, 1);
  UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_step_finish(&f);
 }
}

UT_TEST(test_native_graph_step_refuses_terminal_bytes_lsn_and_origin_mismatch)
{
 for (unsigned fault = 0; fault < 5; fault++) {
  CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
  native_step_fixture(&f, 0);
  UT_ASSERT_EQ(pread(f.main_fd, canonical.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
  if (fault == 0) canonical.data[BLCKSZ - 1]++;
  if (fault == 1) PageSetLSNPreserveOrigin(canonical.data, PageGetLSN(canonical.data) + 8);
  if (fault == 2) UT_ASSERT(PageSetLSNOrigin(canonical.data, 2));
  if (fault == 3) ((PageHeader)canonical.data)->pd_block_scn++;
  result = native_graph_step(&f, fault == 4 ? NULL : &canonical);
  UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
  UT_ASSERT_EQ(native_fpi_tokens, 0); UT_ASSERT_EQ(native_insert_records, 0);
  UT_ASSERT_EQ(native_install_writes + native_install_syncs, 0);
  UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_step_finish(&f);
 }
}

UT_TEST(test_native_graph_step_fixed_input_cannot_change_during_wal_preparation)
{
 CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
 native_step_fixture(&f, 0);
 UT_ASSERT_EQ(pread(f.main_fd, canonical.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
 native_graph_changed_input = &canonical; native_step_fault = 12;
 result = native_graph_step(&f, &canonical);
 UT_ASSERT_EQ(result.phase, RF_PAGE_CTRC_STEP_REFUSED);
 UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT_EQ(native_install_writes, 0);
 UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
 native_graph_changed_input = NULL; native_step_finish(&f);
}

UT_TEST(test_native_graph_step_error_keeps_original_inserted_debt)
{
 CtrcReadNativeFixture f; PGAlignedBlock canonical; volatile bool caught = false;
 native_step_fixture(&f, 0);
 UT_ASSERT_EQ(pread(f.main_fd, canonical.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
 native_install_fault = 9; native_install_at = 11;
 PG_TRY(); { (void)native_graph_step(&f, &canonical); }
 PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught); UT_ASSERT_EQ(native_insert_records, 1);
 UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
 UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
 UT_ASSERT_EQ(native_step_allocation_depth, 0); native_step_finish(&f);
}
