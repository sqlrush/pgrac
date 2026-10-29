/*-------------------------------------------------------------------------
 *
 * test_cluster_clog_full_consumer.inc
 *    FULL birth consumers use original native CLOG/SLRU file I/O.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_clog_full_consumer.inc
 * NOTES
 *    Native files execute under original controlled authority and locks.
 *    A correct physical ZERO is not a generation-lifetime certificate.
 *-------------------------------------------------------------------------
 */

static void
full_consumer_cold_scope(void)
{
	fixture_origin = 1;
	InRecovery = authority = true;
	c0_authority = false;
}

UT_TEST(native_full_clog_consumer_preserves_original_page_and_neighbors)
{
	const uint32 pages[] = { 97, 0, 131071 };
	const uint64 epochs[] = { 7, UINT32_MAX, 0 };

	full_consumer_cold_scope();
	for (unsigned i = 0; i < lengthof(pages); i++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(false, pages[i]);
		xl_clog_zeropage_full birth = { pages[i], 0,
			(epochs[i] << 32) | (pages[i] == 0 ? 3 : pages[i] * UINT64_C(32768)) };
		unsigned before_writes = writes, before_syncs = syncs, before_dirsyncs = dirsyncs;
		bool applied;

		op.normalized_info = CLOG_ZEROPAGE_FULL;
		if (i == 0) {
			fixture_zero(96);
			fixture_bit(96 * CLOG_XACTS_PER_PAGE + 7, TRANSACTION_STATUS_COMMITTED);
		}
		/* The legacy entry cannot discard the original FULL payload. */
		UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT_EQ(writes, before_writes);
		applied = cluster_clog_native_recovery_full_record_apply_v1(
			1, &op, (const uint8 *)&birth, sizeof(birth));
		UT_ASSERT(applied);
		UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(
			1, &op, (const uint8 *)&birth, sizeof(birth)));
		if (applied) {
			UT_ASSERT(fixture_exists(pages[i]));
			UT_ASSERT_EQ(fixture_status((TransactionId)birth.first_full_xid),
						 TRANSACTION_STATUS_IN_PROGRESS);
			if (i == 0)
				UT_ASSERT_EQ(fixture_status(96 * CLOG_XACTS_PER_PAGE + 7),
							 TRANSACTION_STATUS_COMMITTED);
		}
		UT_ASSERT(writes > before_writes && syncs > before_syncs && dirsyncs > before_dirsyncs);
		UT_ASSERT_EQ(birth.first_full_xid,
			(epochs[i] << 32) | (pages[i] == 0 ? 3 : pages[i] * UINT64_C(32768)));
		/* FULL verification may retain real resident pages until teardown. */
		clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
		fixture_idle();
	}
}

UT_TEST(native_full_clog_consumer_refuses_shape_and_scope_before_io)
{
	full_consumer_cold_scope();
	for (unsigned fault = 0; fault < 18; fault++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 97);
		xl_clog_zeropage_full birth = { 97, 0, (UINT64_C(7) << 32) | UINT64_C(3178496) };
		const uint8 *bytes = (const uint8 *)&birth;
		uint32 length = sizeof(birth);
		unsigned before_writes = writes, before_reads = reads, before_syncs = syncs;
		int origin = 1;

		op.normalized_info = CLOG_ZEROPAGE_FULL;
		if (fault == 0) bytes = NULL;
		if (fault == 1) length--;
		if (fault == 2) length++;
		if (fault == 3) birth.reserved_zero = 1;
		if (fault == 4) birth.pageno = -1;
		if (fault == 5) birth.first_full_xid = UINT64_C(7) << 32;
		if (fault == 6) birth.first_full_xid = (UINT64_C(7) << 32) | 1;
		if (fault == 7) birth.first_full_xid = (UINT64_C(7) << 32) | 2;
		if (fault == 8) birth.first_full_xid++;
		if (fault == 9) op.page_number++;
		if (fault == 10) op.oldest_xid = 3;
		if (fault == 11) op.oldest_database = 5;
		if (fault == 12) op.normalized_info = CLOG_ZEROPAGE;
		if (fault == 13) op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
		if (fault == 14) origin = 0;
		if (fault == 15) origin = 2;
		if (fault == 16) authority = false;
		if (fault == 17) InRecovery = false;
		UT_ASSERT(!cluster_clog_native_recovery_full_record_apply_v1(origin, &op, bytes, length));
		UT_ASSERT(!cluster_clog_native_recovery_full_record_verify_v1(origin, &op, bytes, length));
		UT_ASSERT_EQ(writes, before_writes);
		UT_ASSERT_EQ(reads, before_reads);
		UT_ASSERT_EQ(syncs, before_syncs);
		authority = InRecovery = true;
		fixture_idle();
	}
}

UT_TEST(native_full_clog_consumer_keeps_late_failure_incomplete)
{
	full_consumer_cold_scope();
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 160);
		xl_clog_zeropage_full birth = { 160, 0, (UINT64_C(8) << 32) | UINT64_C(5242880) };
		volatile bool caught = false;
		bool result = false;

		op.normalized_info = CLOG_ZEROPAGE_FULL;
		fail_open = fault == 0;
		fail_sync = fault == 1;
		fail_dirsync = fault == 2;
		revoke_after_sync = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_full_record_apply_v1(
				1, &op, (const uint8 *)&birth, sizeof(birth));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		UT_ASSERT_EQ(caught, fault == 0 || fault == 2);
		expect_error = fail_open = fail_sync = fail_dirsync = revoke_after_sync = false;
		authority = true;
		fixture_idle();
		/* No successful retry, rollback or completion is inferred. */
	}
}
