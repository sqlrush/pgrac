/*-------------------------------------------------------------------------
 * test_cluster_clog_full_residency.inc
 *     Foreign FULL replay preserves unrelated actual native resident pages.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_full_residency.inc
 * NOTES
 *     Original CLOG/SLRU/files execute; authority, locks and faults are fixtures.
 *     Actual eviction loses birth input; residency is not lifetime authority.
 *-------------------------------------------------------------------------
 */
static uint64
full_residency_value(int page, uint32 epoch)
{
	return ((uint64)epoch << 32) | (page == 0 ? 3 : (uint32)page * CLOG_XACTS_PER_PAGE);
}

static bool
full_residency_apply(int page, uint32 epoch)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {page, 0, full_residency_value(page, epoch)};

	op.page_number = page;
	return cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth));
}

static uint64
full_residency_birth(int page)
{
	PGAlignedBlock zero = {0};

	return clog_relmap_resident_birth(&ClogRecoveryCtl[1], page, (const uint8 *)zero.data);
}

UT_TEST(full_residency_second_apply_preserves_first_native_page)
{
	foreign_birth_begin();
	UT_ASSERT(full_residency_apply(0, 11));
	UT_ASSERT(full_residency_apply(1, 12));
	UT_ASSERT_EQ(full_residency_birth(0), (UINT64_C(11) << 32) | 3);
	UT_ASSERT_EQ(full_residency_birth(1), (UINT64_C(12) << 32) | CLOG_XACTS_PER_PAGE);
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
	UT_ASSERT_EQ(fixture_status(CLOG_XACTS_PER_PAGE + 17), TRANSACTION_STATUS_IN_PROGRESS);
	foreign_birth_end();
}

UT_TEST(full_residency_target_replacement_and_verify_do_not_borrow_birth)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full historical = {0, 0, (UINT64_C(99) << 32) | 3};
	unsigned before_read;

	foreign_birth_begin();
	UT_ASSERT(full_residency_apply(0, 11));
	UT_ASSERT(full_residency_apply(1, 12));
	UT_ASSERT(full_residency_apply(0, 13));
	UT_ASSERT_EQ(full_residency_birth(0), (UINT64_C(13) << 32) | 3);
	UT_ASSERT_EQ(full_residency_birth(1), (UINT64_C(12) << 32) | CLOG_XACTS_PER_PAGE);
	before_read = reads;
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&historical, sizeof(historical)));
	UT_ASSERT(reads > before_read);
	UT_ASSERT_EQ(full_residency_birth(0), 0);
	UT_ASSERT_EQ(full_residency_birth(1), (UINT64_C(12) << 32) | CLOG_XACTS_PER_PAGE);
	/* A successful cached zero must not conceal a later physical change. */
	UT_ASSERT(full_residency_apply(0, 13));
	fixture_bit(100, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(!cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&historical, sizeof(historical)));
	UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(full_residency_birth(0), 0);
	UT_ASSERT_EQ(full_residency_birth(1), 0);
	foreign_birth_end();
}

UT_TEST(full_residency_actual_capacity_eviction_stays_unknown_after_reload)
{
	SlruCtl ctl = &ClogRecoveryCtl[1];
	int capacity = ctl->shared->num_slots, present = 0, missing = -1, slot;

	foreign_birth_begin();
	for (int page = 0; page <= capacity; page++)
		UT_ASSERT(full_residency_apply(page, 11));
	for (int page = 0; page <= capacity; page++) {
		uint64 birth = full_residency_birth(page);

		if (birth == 0)
			missing = page;
		else {
			present++;
			UT_ASSERT_EQ(birth, full_residency_value(page, 11));
		}
	}
	UT_ASSERT_EQ(present, capacity);
	UT_ASSERT(missing >= 0);
	if (missing >= 0) {
		LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
		slot = SimpleLruReadPage(ctl, missing, true, InvalidTransactionId);
		UT_ASSERT_EQ(ctl->shared->page_initial_full_xid[slot], 0);
		LWLockRelease(ctl->shared->ControlLock);
		UT_ASSERT_EQ(full_residency_birth(missing), 0);
	}
	foreign_birth_end();
}

UT_TEST(full_residency_failure_and_legacy_keep_original_complete_invalidation)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		volatile bool caught = false, result = false;

		foreign_birth_begin();
		UT_ASSERT(full_residency_apply(0, 11));
		UT_ASSERT(full_residency_apply(1, 12));
		relmap_base_reset(1);
		UT_ASSERT(resident_birth_read(&base, &held));
		InRecovery = true;
		fail_sync = fault == 0; fail_dirsync = fault == 1;
		revoke_after_sync = fault == 2; fail_read = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			result = full_residency_apply(2, 13);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		expect_error = fail_sync = fail_dirsync = revoke_after_sync = fail_read = false;
		UT_ASSERT_EQ(full_residency_birth(0), 0);
		UT_ASSERT_EQ(full_residency_birth(1), 0);
		UT_ASSERT_EQ(full_residency_birth(2), 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		UT_ASSERT(held != NULL && held->invalidated);
		UT_ASSERT_EQ(transient_fds, 1);
		relmap_mutation_refuses(&base, &held);
		foreign_birth_end();
	}
	for (unsigned truncate = 0; truncate < 2; truncate++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(truncate, truncate ? 32 : 2);

		foreign_birth_begin();
		UT_ASSERT(full_residency_apply(0, 11));
		UT_ASSERT(full_residency_apply(1, 12));
		UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT_EQ(full_residency_birth(0), 0);
		UT_ASSERT_EQ(full_residency_birth(1), 0);
		foreign_birth_end();
	}
}
