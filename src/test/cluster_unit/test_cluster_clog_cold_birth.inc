/*-------------------------------------------------------------------------
 * test_cluster_clog_cold_birth.inc
 *     Original own cold FULL apply supplies native resident birth input.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_cold_birth.inc
 * NOTES
 *     Original CLOG/SLRU/files execute; locks, WAL and authority are fixtures.
 *     This is initialization input, not era, lifetime or COMPLETE authority.
 *-------------------------------------------------------------------------
 */
static ClusterSideProjectionOperationV1
cold_birth_op(void)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 0);
	op.normalized_info = CLOG_ZEROPAGE_FULL;
	return op;
}

static uint64
cold_birth_value(void)
{
	return (UINT64_C(9) << 32) | FirstNormalTransactionId;
}

static uint64
cold_birth_resident(void)
{
	PGAlignedBlock zero = {0};
	return clog_relmap_resident_birth(XactCtl, 0, (const uint8 *)zero.data);
}

UT_TEST(cold_birth_apply_feeds_original_held_reader)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {0, 0, cold_birth_value()};
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	int other;

	own_full_begin();
	clog_recovery_cache_discard(XactCtl);
	fixture_zero(1);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	other = SimpleLruReadPage(XactCtl, 1, true, CLOG_XACTS_PER_PAGE + 17);
	UT_ASSERT(clog_recovery_status(XactCtl, other, CLOG_XACTS_PER_PAGE + 17,
		TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
	XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE] = UINT64_C(0x1234);
	LWLockRelease(XactSLRULock);
	UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(cold_birth_resident(), birth.first_full_xid);
	UT_ASSERT(XactCtl->shared->page_dirty[other]);
	UT_ASSERT_EQ(XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE], UINT64_C(0x1234));
	relmap_base_reset(1);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, birth.first_full_xid);
	UT_ASSERT_NE(base.resident_first_full_xid >> 32, base.queried_full_xid >> 32);
	cluster_clog_relmap_base_release_v1(&held);
	own_full_end();
}

UT_TEST(cold_birth_verification_never_creates_or_replaces_input)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {0, 0, cold_birth_value()};
	uint64 generation;

	own_full_begin();
	clog_recovery_cache_discard(XactCtl);
	fixture_zero(0);
	generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(cold_birth_resident(), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation), generation);
	UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(cold_birth_resident(), birth.first_full_xid);
	birth.first_full_xid += UINT64_C(1) << 32;
	generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
	/* Equal zero bytes do not let a historical verifier replace native birth. */
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(cold_birth_resident(), cold_birth_value());
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation), generation);
	clog_recovery_cache_discard(XactCtl);
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(cold_birth_resident(), 0);
	own_full_end();
}

UT_TEST(cold_birth_late_failures_keep_unknown_and_held_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterSideProjectionOperationV1 op = cold_birth_op();
		xl_clog_zeropage_full birth = {0, 0, cold_birth_value()};
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		volatile bool caught = false, result = false;

		own_full_begin();
		clog_recovery_cache_discard(XactCtl);
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		InRecovery = true;
		fail_sync = fault == 0;
		fail_dirsync = fault == 1;
		revoke_after_sync = fault == 2;
		fail_read = fault == 3;
		expect_error = true;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_full_record_apply_v1(1, &op,
				(const uint8 *)&birth, sizeof(birth));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught || !result);
		expect_error = fail_sync = fail_dirsync = revoke_after_sync = fail_read = false;
		UT_ASSERT_EQ(cold_birth_resident(), 0);
		UT_ASSERT(held != NULL && held->invalidated);
		UT_ASSERT_EQ(transient_fds, 1);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		relmap_mutation_refuses(&base, &held);
		own_full_end();
	}
}

static unsigned cold_birth_control_calls;
static bool cold_birth_lock_error;
static void
cold_birth_final_lock_fault(LWLock *lock)
{
	if (lock != XactCtl->shared->ControlLock || ++cold_birth_control_calls != 4)
		return;
	if (cold_birth_lock_error)
		elog(ERROR, "test cold FULL resident publication lock");
	authority = false;
}

UT_TEST(cold_birth_final_lock_error_keeps_original_cleanup)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterSideProjectionOperationV1 op = cold_birth_op();
		xl_clog_zeropage_full birth = {0, 0, cold_birth_value()};
		volatile bool caught = false, result = false;

		own_full_begin();
		clog_recovery_cache_discard(XactCtl);
		cold_birth_control_calls = 0;
		cold_birth_lock_error = fault == 0;
		fixture_native_lock_hook = cold_birth_final_lock_fault;
		expect_error = true;
		PG_TRY();
		{
			result = cluster_clog_native_recovery_full_record_apply_v1(1, &op,
				(const uint8 *)&birth, sizeof(birth));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		fixture_native_lock_hook = NULL;
		expect_error = false;
		UT_ASSERT_EQ(cold_birth_control_calls, 4);
		UT_ASSERT_EQ(caught, fault == 0);
		UT_ASSERT(caught || !result);
		UT_ASSERT(!LWLockHeldByMe(XactCtl->shared->ControlLock));
		UT_ASSERT(!LWLockHeldByMe(&ClogRecoveryLocks[3].lock));
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		UT_ASSERT_EQ(cold_birth_resident(), 0);
		/* The durable zero page is retained, with no retry or inferred birth. */
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		own_full_end();
	}
}
