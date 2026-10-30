/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_terminal.inc
 *     Original native CLOG terminal read before retained pair publication.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_terminal.inc
 * NOTES
 *     Native status, SLRU and pair files execute; locks, directory authority,
 *     transaction commit record and WAL transport remain controlled.
 *-------------------------------------------------------------------------
 */

static XidNativeClogTerminalFault retained_native_fault;
static bool retained_native_sync_drift;
static unsigned retained_native_successful_syncs;

static void
retained_native_drift(void)
{
	UT_ASSERT(retention_truncation_held && !held && !retention_held);
	if (retained_native_sync_drift) {
		retained_native_sync_drift = false;
		caller_boot++;
	}
}

static bool
retained_native_clog_sync(TransactionId xid, ClusterClogNativeTerminalV1 *out)
{
	bool caught = false;
	bool ok;

	UT_ASSERT(retention_truncation_held && !held && !retention_held && !retention_mapping_held);
	ok = test_xid_native_clog_terminal(xid, retained_native_fault, retained_native_drift, out, &caught);
	if (caught) {
		/* The isolated CLOG fixture models elog's holdoff reset for its own
		 * locks. Deliver the same ERROR to the original outer native reader. */
		InterruptHoldoffCount = 0;
		PG_RE_THROW();
	}
	return ok;
}

static bool
retained_native_terminal_read(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	bool ok = retention_original_native_terminal_sync(sample, out);

	if (ok && out->status == TRANSACTION_STATUS_COMMITTED) retained_native_successful_syncs++;
	UT_ASSERT(!retention_truncation_held && !held && !retention_held);
	return ok;
}

static void
retained_native_before_pair_write(void)
{
	XidNativeClogTerminalState state;
	TransactionId top = (TransactionId)cluster_relmap_v2_arms[0].sample.owner.top_full_xid;

	test_xid_native_clog_terminal_state(top, &state);
	UT_ASSERT(retained_native_successful_syncs != 0 && state.calls != 0);
	UT_ASSERT(state.file_syncs != 0 && state.directory_syncs != 0);
	UT_ASSERT_EQ(state.disk_status, TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(!state.dirty && !state.slru_held && !state.truncation_held);
	UT_ASSERT(!retention_truncation_held && !held && !retention_held);
}

static void
retained_native_pair_start(bool rollover)
{
	retained_pair_start(rollover);
	test_xid_native_clog_use_local_directory();
	retention_clog_terminal_sync = retained_native_clog_sync;
	stage_pair_terminal_reader = retained_native_terminal_read;
	stage_pair_terminal_write_observer = retained_native_before_pair_write;
	variables.oldestClogXid = FirstNormalTransactionId;
	retained_native_fault = XID_CLOG_TERMINAL_VALID;
	retained_native_sync_drift = false;
	retained_native_successful_syncs = 0;
}

static void
retained_native_pair_arm(void)
{
	UT_ASSERT(cluster_relmap_producer_arm_v2(true, &stage_updates));
	UT_ASSERT(cluster_relmap_producer_arm_v2(false, &stage_updates));
	stage_pair_finishing = true;
	retained_pair_expect(0, true, false); retained_pair_expect(1, true, false);
}

static void
retained_native_pair_kept(const ClusterRelmapRetentionPin *pins, TestRelmapPhysicalStats before)
{
	TestRelmapPhysicalStats after = test_relmap_physical_stats();
	XidNativeClogTerminalState native_before, native_after;
	TransactionId top = (TransactionId)cluster_relmap_v2_arms[0].sample.owner.top_full_xid;

	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, 3 * sizeof(*pins)), 0);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT(!retention_truncation_held && !held && !retention_held && !retention_mapping_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	retained_pair_expect(0, true, false); retained_pair_expect(1, true, false);
	test_xid_native_clog_terminal_state(top, &native_before);
	UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
	UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
	after = test_relmap_physical_stats();
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	test_xid_native_clog_terminal_state(top, &native_after);
	UT_ASSERT_EQ(memcmp(&native_before, &native_after, sizeof(native_before)), 0);
}

static void
retained_native_pair_stop(void)
{
	UT_ASSERT(!retention_truncation_held);
	retention_clog_terminal_sync = NULL;
	stage_pair_terminal_reader = NULL;
	stage_pair_terminal_write_observer = NULL;
	retained_pair_stop();
}

UT_TEST(retained_native_terminal_commits_both_pairs_after_original_clog)
{
	for (unsigned rollover = 0; rollover < 2; rollover++) {
		TransactionStateData child = { 0 };
		ClusterRelmapRetentionPin pins[3];
		XidNativeClogTerminalState before, after;
		TransactionId raw_child;
		uint64 top;

		retained_native_pair_start(rollover != 0);
		retained_native_pair_arm();
		top = cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
		child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
		child.parent = &TopTransactionStateData; child.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
		CurrentTransactionState = &child;
		raw_child = XidFromFullTransactionId(xact_source_current());
		UT_ASSERT_EQ(raw_child, (TransactionId)top + CLUSTER_XID_STRIDE);
		CurrentTransactionState = &TopTransactionStateData;
		test_xid_native_clog_set_status((TransactionId)top, 1, &raw_child, TRANSACTION_STATUS_COMMITTED);
		TopTransactionStateData.state = TRANS_COMMIT;
		test_xid_native_clog_terminal_state((TransactionId)top, &before);
		UT_ASSERT(before.file_present && before.dirty);
		UT_ASSERT_EQ(before.disk_status, TRANSACTION_STATUS_IN_PROGRESS);
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		UT_ASSERT(cluster_relmap_producer_finish_v2(true));
		test_xid_native_clog_terminal_state((TransactionId)top, &after);
		UT_ASSERT_EQ(after.calls, 2);
		UT_ASSERT_EQ(after.disk_status, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT(after.file_syncs > before.file_syncs && after.directory_syncs > before.directory_syncs);
		UT_ASSERT(after.wal_flushes > before.wal_flushes && after.writes > before.writes);
		UT_ASSERT(!after.dirty && !after.truncation_held && !after.slru_held && !after.caught);
		UT_ASSERT_EQ(after.transient_fds, 0);
		test_xid_native_clog_terminal_state(raw_child, &after);
		UT_ASSERT_EQ(after.disk_status, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
		retained_pair_expect(0, false, true); retained_pair_expect(1, false, true);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		retained_native_pair_stop();
	}
}

UT_TEST(retained_native_terminal_refuses_uncommitted_and_aborted)
{
	for (unsigned aborted = 0; aborted < 2; aborted++) {
		XidNativeClogTerminalState state;
		TestRelmapPhysicalStats before;
		ClusterRelmapRetentionPin pins[3];
		uint64 top;

		retained_native_pair_start(false); retained_native_pair_arm();
		top = cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
		test_xid_native_clog_set_status((TransactionId)top, 0, NULL,
			aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_IN_PROGRESS);
		TopTransactionStateData.state = aborted ? TRANS_ABORT : TRANS_INPROGRESS;
		before = test_relmap_physical_stats();
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		retained_native_pair_kept(pins, before);
		test_xid_native_clog_terminal_state((TransactionId)top, &state);
		UT_ASSERT_EQ(state.calls, 1);
		UT_ASSERT_EQ(state.disk_status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_IN_PROGRESS);
		retained_native_pair_stop();
	}
}

UT_TEST(retained_native_terminal_io_failure_and_error_keep_custody)
{
	for (unsigned fault = XID_CLOG_TERMINAL_FILE_SYNC; fault <= XID_CLOG_TERMINAL_OPEN_ERROR; fault++) {
		XidNativeClogTerminalState state;
		TestRelmapPhysicalStats before;
		ClusterRelmapRetentionPin pins[3];
		volatile bool caught = false;
		bool result = false;
		uint64 top;

		retained_native_pair_start(false); retained_native_pair_arm();
		top = cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
		test_xid_native_clog_set_status((TransactionId)top, 0, NULL, TRANSACTION_STATUS_COMMITTED);
		TopTransactionStateData.state = TRANS_COMMIT;
		retained_native_fault = fault;
		before = test_relmap_physical_stats();
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		retention_error_expected = true;
		PG_TRY(); { result = cluster_relmap_producer_finish_v2(true); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		retention_error_expected = false;
		UT_ASSERT(!result);
		UT_ASSERT_EQ(caught, fault == XID_CLOG_TERMINAL_OPEN_ERROR);
		retained_native_pair_kept(pins, before);
		test_xid_native_clog_terminal_state((TransactionId)top, &state);
		UT_ASSERT_EQ(state.calls, 1); UT_ASSERT_EQ(state.caught, caught);
		UT_ASSERT(!state.slru_held && !state.truncation_held);
		UT_ASSERT_EQ(state.transient_fds, 0); UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		retained_native_pair_stop();
	}
}

UT_TEST(retained_native_terminal_owner_change_after_sync_keeps_pairs)
{
	TestRelmapPhysicalStats before;
	ClusterRelmapRetentionPin pins[3];
	XidNativeClogTerminalState state;
	uint64 top, boot;

	retained_native_pair_start(true); retained_native_pair_arm();
	top = cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
	test_xid_native_clog_set_status((TransactionId)top, 0, NULL, TRANSACTION_STATUS_COMMITTED);
	TopTransactionStateData.state = TRANS_COMMIT;
	before = test_relmap_physical_stats(); boot = caller_boot;
	memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
	retained_native_sync_drift = true;
	UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
	UT_ASSERT_EQ(caller_boot, boot + 1);
	retained_native_pair_kept(pins, before);
	test_xid_native_clog_terminal_state((TransactionId)top, &state);
	UT_ASSERT_EQ(state.calls, 1); UT_ASSERT_EQ(state.disk_status, TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(state.file_syncs != 0 && state.directory_syncs != 0);
	retained_native_pair_stop();
}

UT_TEST(retained_native_terminal_bounds_refuse_before_clog_or_pair_io)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TestRelmapPhysicalStats before;
		ClusterRelmapRetentionPin pins[3];
		XidNativeClogTerminalState state;
		uint64 top;

		retained_native_pair_start(false); retained_native_pair_arm();
		top = cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
		test_xid_native_clog_set_status((TransactionId)top, 0, NULL, TRANSACTION_STATUS_COMMITTED);
		TopTransactionStateData.state = TRANS_COMMIT;
		if (fault == 0) variables.clusterIssuedState = 2;
		if (fault == 1) variables.oldestClogXid = (TransactionId)top - UINT32_C(0x80000000);
		if (fault == 2) variables.nextXid = variables.clusterLastIssuedXid;
		before = test_relmap_physical_stats();
		memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		retained_native_pair_kept(pins, before);
		test_xid_native_clog_terminal_state((TransactionId)top, &state);
		UT_ASSERT_EQ(state.calls, 0); UT_ASSERT(state.dirty);
		UT_ASSERT_EQ(state.disk_status, TRANSACTION_STATUS_IN_PROGRESS);
		retained_native_pair_stop();
	}
}
