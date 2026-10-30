/*-------------------------------------------------------------------------
 * test_cluster_clog_terminal_residency.inc
 *     Terminal replay reloads targets while preserving untouched FULL input.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_terminal_residency.inc
 * NOTES
 *     Original CLOG/SLRU/files execute; authority, locks and faults are fixtures.
 *     Resident input is not era or continuous physical lifetime authority.
 *-------------------------------------------------------------------------
 */
static uint64
terminal_residency_metadata(int page)
{
	SlruShared shared = ClogRecoveryCtl[1].shared;
	uint64 birth = 0;

	LWLockAcquire(shared->ControlLock, LW_SHARED);
	for (int slot = 0; slot < shared->num_slots; slot++)
		if (shared->page_status[slot] == SLRU_PAGE_VALID
			&& shared->page_number[slot] == page)
			birth = shared->page_initial_full_xid[slot];
	LWLockRelease(shared->ControlLock);
	return birth;
}

UT_TEST(terminal_residency_keeps_only_untouched_native_full_input)
{
	TransactionId child = 101;
	unsigned before_read;

	foreign_birth_begin();
	UT_ASSERT(full_residency_apply(0, 11));
	UT_ASSERT(full_residency_apply(1, 12));
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 1, &child,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(fixture_status(child), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
	UT_ASSERT_EQ(full_residency_birth(1), full_residency_value(1, 12));
	before_read = reads;
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 1, &child,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(reads > before_read);
	UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
	UT_ASSERT_EQ(full_residency_birth(1), full_residency_value(1, 12));
	foreign_birth_end();
}

UT_TEST(terminal_residency_never_hides_changed_physical_terminal_bits)
{
	foreign_birth_begin();
	UT_ASSERT(full_residency_apply(0, 11));
	UT_ASSERT(full_residency_apply(1, 12));
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(full_residency_birth(1), full_residency_value(1, 12));
	fixture_bit(100, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(!cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
	UT_ASSERT_EQ(full_residency_birth(1), 0);
	fixture_idle();
	foreign_birth_end();
}

UT_TEST(terminal_residency_complete_tree_does_not_restore_target_birth)
{
	TransactionId child = CLOG_XACTS_PER_PAGE + 17;

	foreign_birth_begin();
	UT_ASSERT(full_residency_apply(0, 11));
	UT_ASSERT(full_residency_apply(1, 12));
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 1, &child,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(fixture_status(child), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
	UT_ASSERT_EQ(terminal_residency_metadata(1), 0);
	fixture_idle();
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 1, &child,
		TRANSACTION_STATUS_COMMITTED));
	fixture_idle();
	fixture_zero(0);
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 120, 0, NULL,
		TRANSACTION_STATUS_ABORTED));
	UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
	UT_ASSERT_EQ(fixture_status(120), TRANSACTION_STATUS_ABORTED);
	foreign_birth_end();
}

UT_TEST(terminal_residency_preconditions_and_entered_failures_keep_polarity)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		volatile bool caught = false, result = false;
		unsigned before_io;

		foreign_birth_begin();
		UT_ASSERT(full_residency_apply(0, 11));
		UT_ASSERT(full_residency_apply(1, 12));
		relmap_base_reset(1);
		UT_ASSERT(resident_birth_read(&base, &held));
		InRecovery = true;
		before_io = reads + syncs + dirsyncs;
		authority = false;
		UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
			TRANSACTION_STATUS_COMMITTED));
		authority = true;
		UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, -1, NULL,
			TRANSACTION_STATUS_COMMITTED));
		UT_ASSERT_EQ(reads + syncs + dirsyncs, before_io);
		UT_ASSERT(held != NULL && !held->invalidated);
		UT_ASSERT_EQ(full_residency_birth(0), full_residency_value(0, 11));
		UT_ASSERT_EQ(full_residency_birth(1), full_residency_value(1, 12));
		fail_sync = fault == 0; fail_dirsync = fault == 1;
		revoke_after_sync = fault == 2; fail_read = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
				TRANSACTION_STATUS_COMMITTED);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		expect_error = fail_sync = fail_dirsync = revoke_after_sync = fail_read = false;
		UT_ASSERT_EQ(terminal_residency_metadata(0), 0);
		UT_ASSERT_EQ(terminal_residency_metadata(1), 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		UT_ASSERT(held != NULL && held->invalidated);
		UT_ASSERT_EQ(transient_fds, 1);
		relmap_mutation_refuses(&base, &held);
		foreign_birth_end();
	}
}
