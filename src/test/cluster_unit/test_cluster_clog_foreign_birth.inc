/*-------------------------------------------------------------------------
 * test_cluster_clog_foreign_birth.inc
 *     Foreign native FULL apply retains only its own verified resident input.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_foreign_birth.inc
 * NOTES
 *     Original CLOG/SLRU/files execute; episode, locks and WAL are fixtures.
 *     Birth input does not certify era, physical lifetime or COMPLETE.
 *-------------------------------------------------------------------------
 */
static uint64
foreign_birth_resident(void)
{
	PGAlignedBlock zero = {0};
	return clog_relmap_resident_birth(&ClogRecoveryCtl[1], 0,
		(const uint8 *)zero.data);
}

static void
foreign_birth_begin(void)
{
	fixture_idle();
	relmap_base_reset(1);
	cluster_node_id = 0;
	InRecovery = true;
}

static void
foreign_birth_end(void)
{
	clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
	fixture_native_lock_hook = NULL;
	relmap_base_recheck_hook = NULL;
	expect_error = fail_read = fail_sync = fail_dirsync = revoke_after_sync = false;
	authority = InRecovery = true;
	fixture_idle();
}

UT_TEST(foreign_birth_apply_uses_matching_controller_not_local_birth)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {0, 0, (UINT64_C(11) << 32) | 3};
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;

	resident_birth_prepare(true);
	foreign_birth_begin();
	UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(foreign_birth_resident(), birth.first_full_xid);
	UT_ASSERT_EQ(clog_relmap_resident_birth(XactCtl, 0,
		(const uint8 *)(PGAlignedBlock){0}.data), resident_birth_full());
	relmap_base_reset(1);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, birth.first_full_xid);
	UT_ASSERT_NE(base.resident_first_full_xid, base.queried_full_xid);
	UT_ASSERT(!base.native_next_observed && !base.truncation_horizon_observed);
	UT_ASSERT_EQ(base.next_full_xid, 0);
	UT_ASSERT_EQ(base.oldest_clog_xid, 0);
	cluster_clog_relmap_base_release_v1(&held);
	foreign_birth_end();
}

UT_TEST(foreign_birth_verify_legacy_and_reload_never_restore_input)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {0, 0, (UINT64_C(11) << 32) | 3};
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;

	foreign_birth_begin(); fixture_zero(0);
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT_EQ(foreign_birth_resident(), 0);
	UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	relmap_base_reset(1);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(base.resident_birth_observed);
	/* No mutation sequence or native bytes change: only original cache loss. */
	clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
	LWLockAcquire(ClogRecoveryCtl[1].shared->ControlLock, LW_EXCLUSIVE);
	(void)SimpleLruReadPage(&ClogRecoveryCtl[1], 0, true, 100);
	LWLockRelease(ClogRecoveryCtl[1].shared->ControlLock);
	UT_ASSERT_EQ(foreign_birth_resident(), 0);
	UT_ASSERT(!resident_birth_read(&base, &held));
	UT_ASSERT_EQ(base.queried_full_xid, 0);
	UT_ASSERT(held != NULL && held->failed);
	UT_ASSERT_EQ(transient_fds, 1);
	cluster_clog_relmap_base_release_v1(&held);
	relmap_base_reset(1);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	cluster_clog_relmap_base_release_v1(&held);
	InRecovery = true;
	op.normalized_info = CLOG_ZEROPAGE;
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	UT_ASSERT_EQ(foreign_birth_resident(), 0);
	foreign_birth_end();
}

static unsigned foreign_birth_root_calls;
static void
foreign_birth_drop_at_final_root(void)
{
	if (++foreign_birth_root_calls == 2)
		clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
}

UT_TEST(foreign_birth_final_root_loss_refuses_without_rebasing)
{
	ClusterSideProjectionOperationV1 op = cold_birth_op();
	xl_clog_zeropage_full birth = {0, 0, (UINT64_C(11) << 32) | 3};
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;

	foreign_birth_begin();
	UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	relmap_base_reset(1);
	foreign_birth_root_calls = 0;
	relmap_base_recheck_hook = foreign_birth_drop_at_final_root;
	UT_ASSERT(!resident_birth_read(&base, &held));
	UT_ASSERT_EQ(base.queried_full_xid, 0);
	UT_ASSERT(held == NULL && !authority);
	UT_ASSERT_EQ(transient_fds, 0);
	cluster_clog_relmap_base_release_v1(&held);
	foreign_birth_end();
}

UT_TEST(foreign_birth_native_failures_keep_unknown_and_original_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterSideProjectionOperationV1 op = cold_birth_op();
		xl_clog_zeropage_full birth = {0, 0, (UINT64_C(11) << 32) | 3};
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		volatile bool caught = false, result = false;

		foreign_birth_begin();
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		InRecovery = true;
		fail_sync = fault == 0; fail_dirsync = fault == 1;
		revoke_after_sync = fault == 2; fail_read = fault == 3;
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
		UT_ASSERT_EQ(foreign_birth_resident(), 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT(held != NULL && held->invalidated);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		UT_ASSERT_EQ(transient_fds, 1);
		relmap_mutation_refuses(&base, &held);
		foreign_birth_end();
	}
}

static unsigned foreign_birth_control_calls;
static bool foreign_birth_lock_error;
static void
foreign_birth_final_lock_fault(LWLock *lock)
{
	/* discard/write/write-reacquire, discard/read/read-reacquire, save. */
	if (lock != ClogRecoveryCtl[1].shared->ControlLock
		|| ++foreign_birth_control_calls != 7)
		return;
	if (foreign_birth_lock_error)
		elog(ERROR, "test foreign FULL resident publication lock");
	authority = false;
}

UT_TEST(foreign_birth_final_publication_rechecks_authority_and_error_cleanup)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterSideProjectionOperationV1 op = cold_birth_op();
		xl_clog_zeropage_full birth = {0, 0, (UINT64_C(11) << 32) | 3};
		volatile bool caught = false, result = false;

		foreign_birth_begin();
		foreign_birth_control_calls = 0;
		foreign_birth_lock_error = fault == 0;
		fixture_native_lock_hook = foreign_birth_final_lock_fault;
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
		UT_ASSERT_EQ(caught, fault == 0);
		UT_ASSERT(caught || !result);
		UT_ASSERT_EQ(foreign_birth_resident(), 0);
		UT_ASSERT(!LWLockHeldByMe(ClogRecoveryCtl[1].shared->ControlLock));
		UT_ASSERT(!LWLockHeldByMe(&ClogRecoveryLocks[3].lock));
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		/* Durable zero bytes stay; refusal does not undo or retry storage. */
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		foreign_birth_end();
	}
}
