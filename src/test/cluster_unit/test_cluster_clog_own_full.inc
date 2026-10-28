/*-------------------------------------------------------------------------
 * test_cluster_clog_own_full.inc
 *     Own-origin FULL cold replay through the original native controller.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_own_full.inc
 * NOTES
 *     Native CLOG/SLRU files execute; authority, locks and faults are fixtures.
 *     No page-generation lifetime or joint completion certification.
 *-------------------------------------------------------------------------
 */
static void
own_full_route(int origin)
{
	char path[MAXPGPATH];
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_%d/pg_xact", fixture_root, origin);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
}

static void
own_full_begin(void)
{
	fixture_idle();
	cluster_node_id = fixture_origin = 1;
	/* The existing lock-fault provider also revokes at the local control lock. */
	c0_origin = 0;
	InRecovery = authority = cluster_shared_config = true;
	c0_authority = false;
	relmap_base_interrupts = true;
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	MyAuxProcType = StartupProcess;
	for (int i = 0; i < XactCtl->shared->num_slots; i++) {
		XactCtl->shared->page_status[i] = SLRU_PAGE_EMPTY;
		XactCtl->shared->page_dirty[i] = false;
	}
	own_full_route(1);
}

static void
own_full_end(void)
{
	fixture_idle();
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	cluster_node_id = 0;
	fixture_origin = c0_origin = 1;
	InRecovery = authority = cluster_shared_config = true;
	c0_authority = relmap_base_interrupts = false;
	MyAuxProcType = StartupProcess;
	own_full_route(0);
}

UT_TEST(own_full_cold_replay_preserves_native_cache_and_reads_disk)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 224);
	xl_clog_zeropage_full birth = { 224, 0, (UINT64_C(7) << 32) | UINT64_C(7340032) };
	char path[MAXPGPATH];
	int other;
	unsigned before_writes, before_syncs, before_dirsyncs;
	bool applied;

	own_full_begin();
	fixture_path(224, path);
	if (fixture_exists(224)) UT_ASSERT_EQ(unlink(path), 0);
	fixture_zero(1);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	other = SimpleLruReadPage(XactCtl, 1, true, CLOG_XACTS_PER_PAGE + 17);
	UT_ASSERT(clog_recovery_status(XactCtl, other, CLOG_XACTS_PER_PAGE + 17,
		TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
	XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE] = UINT64_C(0x1234);
	LWLockRelease(XactSLRULock);
	op.normalized_info = CLOG_ZEROPAGE_FULL;
	before_writes = writes; before_syncs = syncs; before_dirsyncs = dirsyncs;
	applied = cluster_clog_native_recovery_full_record_apply_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth));
	UT_ASSERT(applied);
	UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
		(const uint8 *)&birth, sizeof(birth)));
	UT_ASSERT(writes > before_writes && syncs > before_syncs && dirsyncs > before_dirsyncs);
	UT_ASSERT_EQ(XactCtl->shared->page_status[other], SLRU_PAGE_VALID);
	UT_ASSERT(XactCtl->shared->page_dirty[other]);
	UT_ASSERT_EQ(XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE], UINT64_C(0x1234));
	UT_ASSERT_EQ(fixture_status(CLOG_XACTS_PER_PAGE + 17), TRANSACTION_STATUS_IN_PROGRESS);
	if (applied) {
		UT_ASSERT_EQ(fixture_status((TransactionId)birth.first_full_xid),
			TRANSACTION_STATUS_IN_PROGRESS);
		/* The unchanged zero cache must not hide nonzero durable bytes. */
		fixture_bit((TransactionId)birth.first_full_xid + 7, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT(!cluster_clog_native_recovery_full_record_verify_v1(1, &op,
			(const uint8 *)&birth, sizeof(birth)));
	}
	own_full_end();
}

UT_TEST(own_full_cold_replay_requires_exact_cold_role_shape_and_route)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 160);
		xl_clog_zeropage_full birth = {160, 0, (UINT64_C(8) << 32) | UINT64_C(5242880)};
		uint32 length = sizeof(birth);
		unsigned before_writes, before_syncs, before_reads;
		uint64 generation;

		own_full_begin();
		op.normalized_info = CLOG_ZEROPAGE_FULL;
		if (fault == 0) { authority = false; c0_authority = true; }
		if (fault == 1) InRecovery = false;
		if (fault == 2) MyAuxProcType = CheckpointerProcess;
		if (fault == 3) birth.reserved_zero = 1;
		if (fault == 4) length--;
		if (fault == 5) birth.first_full_xid++;
		if (fault == 6) op.normalized_info = CLOG_ZEROPAGE;
		if (fault == 7) op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
		if (fault == 8) own_full_route(0);
		if (fault == 9) cluster_shared_config = false;
		before_writes = writes; before_syncs = syncs; before_reads = reads;
		generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
		UT_ASSERT(!cluster_clog_native_recovery_full_record_apply_v1(1, &op,
			(const uint8 *)&birth, length));
		UT_ASSERT(!cluster_clog_native_recovery_full_record_verify_v1(1, &op,
			(const uint8 *)&birth, length));
		UT_ASSERT_EQ(writes, before_writes);
		UT_ASSERT_EQ(syncs, before_syncs);
		UT_ASSERT_EQ(reads, before_reads);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation), generation);
		own_full_end();
	}
	own_full_begin();
	{
		ClusterSideProjectionOperationV1 zero = fixture_clog_record(false, 0);
		ClusterSideProjectionOperationV1 truncate = fixture_clog_record(true, 32);
		unsigned before = writes + deleted;
		UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &zero));
		UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &zero));
		UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, &truncate));
		UT_ASSERT(!cluster_clog_native_recovery_record_verify_v1(1, &truncate));
		UT_ASSERT_EQ(writes + deleted, before);
	}
	own_full_end();
}

UT_TEST(own_full_cold_replay_errors_keep_original_cache_and_cleanup)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 160);
		xl_clog_zeropage_full birth = {160, 0, (UINT64_C(8) << 32) | UINT64_C(5242880)};
		volatile bool caught = false, result = false;
		int other;

		own_full_begin();
		fixture_zero(160); fixture_zero(1);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		other = SimpleLruReadPage(XactCtl, 1, true, CLOG_XACTS_PER_PAGE + 17);
		UT_ASSERT(clog_recovery_status(XactCtl, other, CLOG_XACTS_PER_PAGE + 17,
			TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
		LWLockRelease(XactSLRULock);
		op.normalized_info = CLOG_ZEROPAGE_FULL;
		fail_open = fault == 0; fail_sync = fault == 1; fail_dirsync = fault == 2;
		revoke_on_lock = fault == 3; revoke_after_sync = fault == 4; fail_read = fault == 5;
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
		UT_ASSERT_EQ(caught, fault == 0 || fault == 2);
		expect_error = fail_open = fail_sync = fail_dirsync = revoke_on_lock = false;
		revoke_after_sync = fail_read = false;
		UT_ASSERT_EQ(XactCtl->shared->page_status[other], SLRU_PAGE_VALID);
		UT_ASSERT(XactCtl->shared->page_dirty[other]);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), 0);
		/* No successful retry or completion follows the entered failure. */
		own_full_end();
	}
}

UT_TEST(own_full_cold_replay_revokes_equal_byte_held_observation)
{
	ClusterSideProjectionOperationV1 op = fixture_clog_record(false, 0);
	xl_clog_zeropage_full birth = {0, 0, (UINT64_C(7) << 32) | 3};
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation;
	bool opened;

	own_full_begin();
	op.normalized_info = CLOG_ZEROPAGE_FULL;
	opened = relmap_mutation_open(1, TRANSACTION_STATUS_IN_PROGRESS, &base, &held);
	UT_ASSERT(opened);
	if (opened) {
		generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
		InRecovery = true;
		UT_ASSERT(cluster_clog_native_recovery_full_record_verify_v1(1, &op,
			(const uint8 *)&birth, sizeof(birth)));
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation), generation);
		UT_ASSERT(!held->invalidated);
		UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
			(const uint8 *)&birth, sizeof(birth)));
		UT_ASSERT(held->invalidated);
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		relmap_mutation_refuses(&base, &held);
	}
	own_full_end();
}
