/* Successful native F4 steps retain only the original raw storage custody.
 * Native owners/PAGE/F4/readback execute; raw FD transport is a unit leaf.
 * Author: SqlRush <sqlrush@gmail.com> */
static RfPageCtrcHeldStepV1
native_held_step(CtrcReadNativeFixture *f, const PGAlignedBlock *canonical)
{
	return rf_page_recovery_ctrc_replays_itl_graph_hold_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		canonical == NULL ? NULL : canonical->data, native_fpi_physical, 2, 1024 * 1024);
}

UT_TEST(test_native_held_fd_retains_same_durable_storage)
{
	for (unsigned source = 0; source < 2; source++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcHeldStepV1 held;
		unsigned releases;
		native_durable_fixture(&f, source, &canonical); releases = test_tx_ir_releases;
		held = native_held_step(&f, &canonical);
		UT_ASSERT_EQ(held.result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
		UT_ASSERT(held.storage == (void *)&ctrc_page_fd_live);
		UT_ASSERT(cluster_shared_fs_page_current_v1(held.storage));
		UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 0);
		UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_install_syncs, 1);
		UT_ASSERT_EQ(ctrc_read_native_reads, 20); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		UT_ASSERT(cluster_recovery_stamp_fpi_durable_current_v1(&held.result.inserted.window,
			held.result.inserted.record_start, held.result.inserted.record_end));
		UT_ASSERT(cluster_shared_fs_page_close_v1(held.storage));
		UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		native_durable_retained(); native_durable_finish(&f);
	}
}

UT_TEST(test_native_held_fd_uses_long_lived_context_and_restores_caller)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcHeldStepV1 held;
	MemoryContextData local = { .type = T_AllocSetContext };
	MemoryContext original = CurrentMemoryContext;

	native_durable_fixture(&f, 0, &canonical); CurrentMemoryContext = &local;
	ctrc_page_fd_context = NULL;
	held = native_held_step(&f, &canonical);
	UT_ASSERT(CurrentMemoryContext == &local);
	UT_ASSERT(ctrc_page_fd_context == TopMemoryContext);
	CurrentMemoryContext = original;
	UT_ASSERT_EQ(held.result.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE);
	UT_ASSERT(cluster_shared_fs_page_current_v1(held.storage));
	UT_ASSERT(cluster_shared_fs_page_close_v1(held.storage)); native_durable_finish(&f);
}

UT_TEST(test_native_held_fd_preserves_four_older_close_contracts)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcStepResultV1 result;
		native_durable_fixture(&f, 0, &canonical);
		if (mode == 0) result = native_step(&f, 2, 1024 * 1024);
		else if (mode == 1) result = rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(
			tx_inputs_start, &ctrc_inputs_work, &native_stamp_source, f.page.page.authority,
			&f.space, 7, &f.page.target.expected_result, canonical.data, native_fpi_physical, 2, 1024 * 1024);
		else if (mode == 2) result = native_durable_step(&f, &canonical);
		else result = native_unchanged_step(&f, &canonical, 1024 * 1024);
		UT_ASSERT_EQ(result.phase, mode < 2 ? RF_PAGE_CTRC_STEP_TARGET_SYNCED : RF_PAGE_CTRC_STEP_FPI_DURABLE);
		UT_ASSERT_EQ(ctrc_page_fd_opens, 1); UT_ASSERT_EQ(ctrc_page_fd_closes, 1);
		UT_ASSERT(!ctrc_page_fd_live); native_durable_finish(&f);
	}
}

UT_TEST(test_native_held_fd_does_not_transfer_other_phases)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcHeldStepV1 held;
		if (fault == 0) native_unchanged_fixture(&f, 0, &canonical);
		else native_durable_fixture(&f, 0, &canonical);
		if (fault == 1) ctrc_page_fd_fault = 1;
		if (fault == 3) native_insert_fault = 6; /* Original PAGE gap blocker. */
		held = native_held_step(&f, fault == 2 ? NULL : &canonical);
		UT_ASSERT(held.storage == NULL);
		UT_ASSERT_EQ(held.result.phase, fault == 0 ? RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED
			: fault == 3 ? RF_PAGE_CTRC_STEP_WAIT : RF_PAGE_CTRC_STEP_REFUSED);
		UT_ASSERT(!ctrc_page_fd_live); UT_ASSERT_EQ(native_insert_records, 0);
		native_durable_finish(&f);
	}
}

UT_TEST(test_native_held_fd_late_refusal_closes_and_keeps_debt)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; RfPageCtrcHeldStepV1 held;
	native_durable_fixture(&f, 0, &canonical); native_durable_fault = 7;
	held = native_held_step(&f, &canonical);
	UT_ASSERT_EQ(held.result.phase, RF_PAGE_CTRC_STEP_REFUSED); UT_ASSERT(held.storage == NULL);
	UT_ASSERT_EQ(ctrc_page_fd_closes, 1); UT_ASSERT(!ctrc_page_fd_live);
	UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
	native_durable_retained(); native_durable_finish(&f);
}

UT_TEST(test_native_held_fd_error_closes_and_keeps_debt)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; volatile bool caught = false;
	native_durable_fixture(&f, 0, &canonical); native_durable_fault = 102;
	PG_TRY(); { (void)native_held_step(&f, &canonical); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(ctrc_page_fd_closes, 1); UT_ASSERT(!ctrc_page_fd_live);
	UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
	native_durable_retained(); native_durable_finish(&f);
}

UT_TEST(test_native_held_fd_open_error_restores_caller_context)
{
	CtrcReadNativeFixture f; PGAlignedBlock canonical; volatile bool caught = false;
	MemoryContextData local = { .type = T_AllocSetContext };
	MemoryContext original = CurrentMemoryContext;
	native_durable_fixture(&f, 0, &canonical); ctrc_page_fd_fault = 8;
	CurrentMemoryContext = &local; ctrc_page_fd_context = NULL;
	PG_TRY(); { (void)native_held_step(&f, &canonical); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(CurrentMemoryContext == &local);
	UT_ASSERT(ctrc_page_fd_context == TopMemoryContext); CurrentMemoryContext = original;
	UT_ASSERT(!ctrc_page_fd_live); UT_ASSERT_EQ(ctrc_page_fd_closes, 0);
	UT_ASSERT_EQ(native_insert_records, 0); native_durable_finish(&f);
}
