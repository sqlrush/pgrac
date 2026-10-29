/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_native_clog.inc
 *     Original retained allocation through native FULL CLOG initialization.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_native_clog.inc
 * NOTES
 *     Native CLOG/SLRU execute; COMPLETE, locks, lease and WAL transport are
 *     controlled. Initialization is not era/lifetime/completion authority.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static void
retention_full_begin(uint64 next, bool fail)
{
	retention_fixture(next, 0);
	memset(&full_clog_fixture, 0, sizeof(full_clog_fixture));
	full_clog_fixture.active = true;
	reservation = true;
	floor_full = 0;
	limit_full = UINT64_MAX;
	cluster_shared_catalog = cluster_xid_striping = true;
	test_xid_native_clog_start(fail);
	native_clog_enabled = true;
}

UT_TEST(retained_allocator_native_full_gap_and_subxact_wrap)
{
	static const uint64 next[] = {UINT64_C(0x500007ffe), UINT64_C(0x7fffffffe)};
	static const uint64 issued[] = {UINT64_C(0x500008004), UINT64_C(0x800000004)};
	static const uint64 birth[] = {UINT64_C(0x500008000), UINT64_C(0x800000003)};
	static const unsigned calls[] = {7, 4};

	for (unsigned i = 0; i < lengthof(next); ++i) {
		XidNativeClogState state;
		XidNativeClogReplayState replay;

		retention_full_begin(next[i], false);
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(i != 0, 0)), issued[i]);
		test_xid_native_clog_state(&state);
		native_clog_wal(&state, calls[i], birth[i]);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), issued[i] + 1);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, issued[i]);
		UT_ASSERT_EQ(retention_final_observations, 1);
		UT_ASSERT_EQ(retention_acquires, 2);
		if (i == 0) {
			UT_ASSERT_EQ(proc.xid, (uint32)issued[i]);
			UT_ASSERT_EQ(visible_xids[0], (uint32)issued[i]);
		} else {
			UT_ASSERT_EQ(proc.xid | visible_xids[0], 0);
			UT_ASSERT_EQ(subxids[0].count, 1);
			UT_ASSERT_EQ(proc.subxids.xids[0], 4);
		}
		UT_ASSERT(!held && !retention_held && !state.xid_held && !state.slru_held);
		/* Replay the actually captured FULL bytes through the original redo
		 * and independent file read, never a manufactured birth payload. */
		if (state.length == sizeof(xl_clog_zeropage_full)) {
			test_xid_native_clog_replay(XID_CLOG_REPLAY_VALID, &replay);
			UT_ASSERT(replay.io_ok && !replay.caught && replay.target_zero
				&& replay.neighbor_sentinel && !replay.target_sentinel);
			UT_ASSERT_EQ(replay.writes, 1);
			UT_ASSERT_EQ(replay.inserts, 0);
		}
		native_clog_end();
	}
}

UT_TEST(retained_allocator_initial_and_recomputed_refusal_precede_native_clog)
{
	for (unsigned rederived = 0; rederived < 2; ++rederived) {
		XidNativeClogState state;

		retention_full_begin(2052, false);
		if (rederived) {
			variables.xidVacLimit = 2052;
			variables.xidWarnLimit = 4096;
			retention_recompute = true;
			retention_fault = 4;
		} else
			relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		test_xid_native_clog_state(&state);
		UT_ASSERT_EQ(state.calls | state.begins | state.inserts | state.dirty_pages, 0);
		UT_ASSERT_EQ(extensions | native_publish_count | proc.xid | visible_xids[0], 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, 2051);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), rederived ? 2084 : 2052);
		UT_ASSERT_EQ(retention_acquires, rederived ? 2 : 1);
		native_clog_end();
	}
}

UT_TEST(retained_allocator_late_refusal_preserves_native_full_effect_without_issuance)
{
	XidNativeClogState state;
	const uint64 next = UINT64_C(0x500007ffe);

	retention_full_begin(next, false);
	retention_fault = 2;
	(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	test_xid_native_clog_state(&state);
	native_clog_wal(&state, 7, UINT64_C(0x500008000));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, next - 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), next);
	UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0] | retention_final_observations, 0);
	UT_ASSERT(!held && !retention_held && !state.xid_held && !state.slru_held);
	native_clog_end();
}

UT_TEST(retained_allocator_native_wal_error_keeps_dirty_duty_before_publication)
{
	XidNativeClogState state;
	const uint64 next = UINT64_C(0x500007ffe);
	volatile bool caught = false;

	retention_full_begin(next, true);
	if (setjmp(full_clog_fixture.error) == 0)
		(void)cluster_relmap_retention_allocate_xid_v2(false,
			retention_sysid, retention_dbinc, retention_origin);
	else
		caught = true;
	test_xid_native_clog_state(&state);
	UT_ASSERT(caught);
	native_clog_wal(&state, 3, UINT64_C(0x500008000));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), next);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, next - 1);
	UT_ASSERT_EQ(proc.xid | visible_xids[0] | subxids[0].count | retention_final_observations, 0);
	UT_ASSERT_EQ(retention_acquires, 1);
	UT_ASSERT(held && !retention_held && state.xid_held && state.slru_held);
	/* Original native ERROR cleanup only; do not retry or retire the debt. */
	native_clog_end();
	UT_ASSERT_EQ(InterruptHoldoffCount | lock_errors | native_publish_order_errors, 0);
}

static void
retention_full_run(void)
{
	UT_RUN(retained_allocator_native_full_gap_and_subxact_wrap);
	UT_RUN(retained_allocator_initial_and_recomputed_refusal_precede_native_clog);
	UT_RUN(retained_allocator_late_refusal_preserves_native_full_effect_without_issuance);
	UT_RUN(retained_allocator_native_wal_error_keeps_dirty_duty_before_publication);
}
