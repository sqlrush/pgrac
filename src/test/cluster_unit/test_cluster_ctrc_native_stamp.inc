/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_stamp.inc
 *     Original native CTRC holders and PAGE feed the existing F4 window.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_stamp.inc
 *
 * NOTES
 *     Actual native owners/PAGE, canonical TX, descriptor and stamp code;
 *     installed-writer observation is a controlled leaf, not native WAL.
 *
 *-------------------------------------------------------------------------
 */
static bool native_stamp_installed;
static unsigned native_stamp_fault;
static ClusterRecoveryCtrcReplaySetSourceV1 native_stamp_source;
static GenericXLogRecoveryImageDescriptorV1 native_stamp_image;
static CtrcReadNativeFixture *native_stamp_live;

static void *
native_stamp_alloc(Size size)
{
	UT_ASSERT(native_stamp_allocation == NULL);
	if (native_stamp_alloc_error) pg_re_throw();
	native_stamp_allocs++;
	native_stamp_allocation = palloc(size);
	return native_stamp_allocation;
}

static void
native_stamp_free(void *pointer)
{
	UT_ASSERT(pointer != NULL && pointer == native_stamp_allocation);
	native_stamp_frees++; native_stamp_allocation = NULL; pfree(pointer);
}

bool
ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation)
{
	*writer = ctrc_inputs_owner.writer;
	memcpy(operation_uuid, ctrc_inputs_owner.operation_uuid, 16);
	*generation = ctrc_inputs_owner.install_generation;
	switch (native_stamp_fault) {
	case 1: writer->epoch++; break;
	case 2: operation_uuid[15]++; break;
	case 3: (*generation)++; break;
	case 4: ctrc_inputs_owner.writer.epoch++; break;
	case 5: native_fpi_region.checkpoint_owner.tx_accepted_generation++; break;
	case 6: native_stamp_image.result_token++; break;
	case 7: native_stamp_source.ir_request_id++; break;
	case 8: native_stamp_live->page.target.expected_result.mutation_token++; break;
	case 9: ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++; break;
	case 10: cluster_wal_inputs_release_v1(&native_stamp_live->page.replay.inputs); break;
	case 11: pg_re_throw(); break;
	}
	return native_stamp_installed;
}

static void
native_stamp_fixture(CtrcReadNativeFixture *f, uint32 index)
{
	RfPageCtrcFpiV1 prepared;
	native_fpi_complete_writer = true;
	native_fpi_fixture(f, 8, true);
	native_fpi_complete_writer = false;
	prepared = native_fpi_prepare(f, 2, 1024 * 1024);
	UT_ASSERT(prepared.ready && prepared.rewritten);
	/* Hash outside PAGE; retain the same original IR/WALR and immutable frame. */
	rf_page_authority_ctrc_replays_destroy_v1(&f->page.page.authority);
	UT_ASSERT(GenericXLogRecoveryImageDescribe(&prepared.frame, &native_stamp_image));
	f->page.page = ctrc_page_build(&f->page);
	UT_ASSERT_EQ(f->page.page.verdict, RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f->page.page.authority), 0);
	native_stamp_source = cluster_wal_inputs_ctrc_replays_source_v1(f->page.replay.inputs, index);
	UT_ASSERT(native_stamp_source.ir_request_id != 0);
	native_stamp_installed = true; native_stamp_fault = 0; native_stamp_live = f;
	UT_ASSERT(native_stamp_allocation == NULL);
	native_stamp_allocs = native_stamp_frees = 0; native_stamp_alloc_error = false;
}

static ClusterRecoveryStampHandleV1
native_stamp_prepare(CtrcReadNativeFixture *f)
{
	return cluster_recovery_stamp_prepare_ctrc_fpi_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &f->page.target.page_identity, &native_stamp_image, 2, 1024 * 1024);
}

static void
native_stamp_empty(CtrcReadNativeFixture *f)
{
	ClusterRecoveryStampHandleV1 handle = native_stamp_prepare(f);
	static const ClusterRecoveryStampHandleV1 zero;
	UT_ASSERT(memcmp(&handle, &zero, sizeof(handle)) == 0);
	for (unsigned i = 1; i <= CLUSTER_RECOVERY_STAMP_SLOTS; i++)
		UT_ASSERT_EQ(native_fpi_region.thread_replay[i].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
}

UT_TEST(test_native_stamp_reference_uses_original_grant_and_page)
{
	for (uint32 i = 0; i < 2; i++) {
		CtrcReadNativeFixture f; RfPageCtrcReferenceV1 reference; RfPageGuardPreflightV1 page;
		unsigned grants, releases, cf, reads;
		native_stamp_fixture(&f, i);
		grants = test_tx_ir_grants; releases = test_tx_ir_releases; cf = test_cf_lock_calls;
		reads = ctrc_read_native_reads;
		reference = rf_page_authority_ctrc_replays_reference_nowait_v1(f.page.page.authority,
			&native_stamp_source, &f.page.target.page_identity, &f.page.target.expected_result);
		UT_ASSERT(reference.ready);
		UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &page));
		UT_ASSERT_EQ(reference.reference.source_binding_cookie, native_stamp_source.ir_request_id);
		UT_ASSERT_EQ(reference.reference.protection_fingerprint, page.identity_fingerprint);
		UT_ASSERT_EQ(reference.reference.partition, page.partition);
		UT_ASSERT_EQ(reference.reference.reserved_zero, 0);
		UT_ASSERT_EQ(test_tx_ir_grants, grants); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(ctrc_read_native_reads, reads);
		native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_reference_refuses_changed_source_target_or_custody)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcReferenceV1 value;
		RfPageIdentityV1 identity; RfPageVersionV1 before;
		native_stamp_fixture(&f, 0); identity = f.page.target.page_identity; before = f.page.target.expected_result;
		switch (fault) {
		case 0: native_stamp_source.ir_request_id++; break;
		case 1: native_stamp_source.work_generation++; break;
		case 2: native_stamp_source.source_index++; break;
		case 3: native_stamp_source.root.file_txn_seq++; break;
		case 4: native_stamp_source.input_owner.generation++; break;
		case 5: native_stamp_source.selected.source.claim.identity.origin_owner_incarnation++; break;
		case 6: native_stamp_source.scope = NULL; break;
		case 7: identity.blockno++; break;
		case 8: before.mutation_token++; break;
		case 9: before.segment_incarnation[0]++; break;
		case 10: rf_page_authority_ctrc_replays_destroy_v1(&f.page.page.authority); break;
		case 11: cluster_wal_inputs_release_v1(&f.page.replay.inputs); break;
		}
		value = rf_page_authority_ctrc_replays_reference_nowait_v1(f.page.page.authority,
			&native_stamp_source, &identity, &before);
		UT_ASSERT(!value.ready); UT_ASSERT_EQ(value.reference.source_binding_cookie, 0);
		UT_ASSERT_EQ(value.reference.protection_fingerprint, 0); native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_admits_each_original_source_without_rr)
{
	for (uint32 i = 0; i < 2; i++) {
		CtrcReadNativeFixture f; ClusterRecoveryStampHandleV1 handle; ClusterRecoveryStampFpiV1 fpi;
		ClusterRecoveryStampCaptureV1 capture; unsigned grants, releases;
		native_stamp_fixture(&f, i); grants = test_tx_ir_grants; releases = test_tx_ir_releases;
		handle = native_stamp_prepare(&f); UT_ASSERT(handle.key.window_seq != 0);
		UT_ASSERT_EQ(handle.key.target_thread, native_stamp_source.selected.source.claim.identity.origin_thread_id);
		UT_ASSERT_EQ(handle.kind, CLUSTER_RECOVERY_STAMP_FPI); UT_ASSERT_EQ(handle.payload_length, sizeof(fpi));
		UT_ASSERT(memcmp(&handle.owner.writer, &ctrc_inputs_owner.writer, sizeof(handle.owner.writer)) == 0);
		UT_ASSERT(memcmp(&handle.owner.target, &native_stamp_source.selected.source.claim, sizeof(handle.owner.target)) == 0);
		UT_ASSERT_EQ(handle.owner.executor_pid, MyProcPid); UT_ASSERT_EQ(handle.owner.executor_boot, ctrc_inputs_owner.owner_boot);
		UT_ASSERT_EQ(handle.owner.recovery_serial, tx_inputs_serial);
		memcpy(&fpi, handle.payload, sizeof(fpi));
		UT_ASSERT(memcmp(&fpi.image, &native_stamp_image, sizeof(fpi.image)) == 0);
		UT_ASSERT_EQ(fpi.protection.source_binding_cookie, native_stamp_source.ir_request_id);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[handle.key.target_thread].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture)); UT_ASSERT_EQ(capture.count, 1);
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
		UT_ASSERT(cluster_recovery_stamp_cancel_v1(&handle));
		UT_ASSERT_EQ(test_tx_ir_grants, grants); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_refuses_unaccepted_or_wrong_original_owner)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		CtrcReadNativeFixture f;
		native_stamp_fixture(&f, 1);
		switch (fault) {
		case 0: native_fpi_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 1: native_fpi_region.checkpoint_owner.tx_result.unresolved_count++; break;
		case 2: native_stamp_installed = false; break;
		case 3: ctrc_inputs_work.generation++; break;
		case 4: ctrc_inputs_owner.owner_pid++; break;
		case 5: ShutdownRequestPending = true; break;
		case 6: CritSectionCount = 1; break;
		case 7: MyBackendType = B_CHECKPOINTER; break;
		case 8: IsUnderPostmaster = false; break;
		case 9: inputs_native_recovery = true; break;
		case 10: ctrc_inputs_owner.install_generation++; break;
		case 11: ctrc_inputs_work.counts.subject_count++; break;
		case 12: ctrc_inputs_work.counts.subject_count = UINT32_MAX; break;
		case 13: {
			ClusterRecoveryStampHandleV1 refused = cluster_recovery_stamp_prepare_ctrc_fpi_v1(
				tx_inputs_start, &ctrc_inputs_work, &native_stamp_source, f.page.page.authority,
				&f.page.target.page_identity, &native_stamp_image, 2, 1);
			UT_ASSERT_EQ(refused.key.window_seq, 0); UT_ASSERT_EQ(native_stamp_allocs, 0);
			native_fpi_finish(&f); continue;
		}
		}
		native_stamp_empty(&f);
		ShutdownRequestPending = false; CritSectionCount = 0; MyBackendType = B_STARTUP;
		IsUnderPostmaster = true; inputs_native_recovery = false;
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_requires_exact_canonical_descriptor)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		CtrcReadNativeFixture f;
		native_stamp_fixture(&f, 0);
		switch (fault) {
		case 0: native_stamp_image.owner = 0; break;
		case 1: native_stamp_image.fork_number = FSM_FORKNUM; break;
		case 2: native_stamp_image.block_number++; break;
		case 3: native_stamp_image.result_token = 0; break;
		case 4: native_stamp_image.result_token = native_stamp_image.version.before.mutation_token; break;
		case 5: native_stamp_image.version.before.mutation_token++; break;
		case 6: native_stamp_image.version.result_incarnation[0]++; break;
		case 7: native_stamp_image.space_identity[0]++; break;
		case 8: memset(native_stamp_image.after_sha256, 0, 32); break;
		case 9: native_stamp_image.version.before_kind = 0; break;
		}
		native_stamp_empty(&f); native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_rechecks_inputs_after_installed_writer_callback)
{
	for (unsigned fault = 1; fault <= 12; fault++) {
		CtrcReadNativeFixture f; volatile bool caught = false; bool prior_failed = ut_current_failed;
		native_stamp_fixture(&f, 1);
		if (fault == 12) native_stamp_alloc_error = true;
		else native_stamp_fault = fault;
		PG_TRY(); { native_stamp_empty(&f); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		if (!prior_failed && ut_current_failed) printf("# installed-writer drift fault=%u\n", fault);
		UT_ASSERT_EQ(caught, fault >= 11);
		if (fault != 10) UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_fpi_finish(&f);
	}
}

UT_TEST(test_native_stamp_failed_window_keeps_original_debt_and_owners)
{
	CtrcReadNativeFixture f; ClusterRecoveryStampHandleV1 handle, next;
	ClusterRecoveryStampFpiV1 fpi; ClusterRecoveryStampCaptureV1 capture;
	native_stamp_fixture(&f, 0); handle = native_stamp_prepare(&f); UT_ASSERT(handle.key.window_seq != 0);
	memcpy(&fpi, handle.payload, sizeof(fpi));
	UT_ASSERT(!cluster_recovery_stamp_prepare_v1(&handle.owner, CLUSTER_RECOVERY_STAMP_FPI,
		(const uint8 *)&fpi, sizeof(fpi), &next));
	UT_ASSERT(cluster_recovery_stamp_fail_v1(&handle)); next = native_stamp_prepare(&f);
	UT_ASSERT_EQ(next.key.window_seq, 0);
	UT_ASSERT(native_fpi_region.thread_replay[handle.key.target_thread].stamp.failed);
	UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
	UT_ASSERT(cluster_recovery_stamp_poll_v1(&capture) != CLUSTER_RECOVERY_STAMP_COMPLETE);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_fpi_finish(&f);
}

UT_TEST(test_native_stamp_uses_unchanged_critical_insertion_transitions)
{
	CtrcReadNativeFixture f; ClusterRecoveryStampHandleV1 handle, fixed;
	XLogRecPtr start, end;
	native_stamp_fixture(&f, 1); handle = fixed = native_stamp_prepare(&f); UT_ASSERT(handle.key.window_seq != 0);
	native_stamp_source.ir_request_id++; native_stamp_image.after_sha256[0]++;
	UT_ASSERT(memcmp(&handle, &fixed, sizeof(handle)) == 0);
	UT_ASSERT(!cluster_recovery_stamp_inserting_v1(&handle));
	start = handle.owner.writer.startup_first_lsn + 8; end = start + 64;
	CritSectionCount++;
	UT_ASSERT(cluster_recovery_stamp_inserting_v1(&handle));
	UT_ASSERT(cluster_recovery_stamp_inserted_v1(&handle, start, end));
	CritSectionCount--;
	UT_ASSERT(cluster_recovery_stamp_fpi_inserted_current_v1(&handle, start, end));
	UT_ASSERT(!cluster_recovery_stamp_cancel_v1(&handle));
	UT_ASSERT_EQ(native_fpi_region.thread_replay[handle.key.target_thread].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_fpi_finish(&f);
}
