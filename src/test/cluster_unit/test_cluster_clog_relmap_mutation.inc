/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_mutation.inc
 *     Same-process native mutations revoke held CLOG observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_mutation.inc
 * NOTES
 *     Original CLOG/SLRU and file I/O execute. Original episode, locks and
 *     fault delivery are fixtures; no cross-process lifetime certification.
 *-------------------------------------------------------------------------
 */
static bool
relmap_mutation_open(int origin, XidStatus status, ClusterRelmapClogBaseV1 *base,
	ClusterRelmapClogBaseHandleV1 **held)
{
	relmap_base_reset(origin);
	fixture_zero(0);
	fixture_bit(100, status);
	if (origin == 0) {
		clog_recovery_cache_discard(XactCtl);
	}
	return cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), base, held);
}

static void
relmap_mutation_refuses(ClusterRelmapClogBaseV1 *base,
	ClusterRelmapClogBaseHandleV1 **held)
{
	unsigned io;
	InRecovery = false;
	authority = true;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), base, held));
	UT_ASSERT_EQ(base->queried_full_xid, 0);
	UT_ASSERT(!authority);
	UT_ASSERT_EQ(transient_fds, 1);
	io = reads + syncs + dirsyncs;
	authority = true;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), base, held));
	UT_ASSERT_EQ(reads + syncs + dirsyncs, io);
	cluster_clog_relmap_base_release_v1(held);
	fixture_idle();
}

UT_TEST(relmap_native_terminal_aba_does_not_revive_held_observation)
{
	for (int status = TRANSACTION_STATUS_COMMITTED; status <= TRANSACTION_STATUS_ABORTED;
		 ++status) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		ClusterSideProjectionOperationV1 zero = {0};
		UT_ASSERT(relmap_mutation_open(1, status, &base, &held));
		if (held == NULL) return;
		zero.kind = CLUSTER_SIDE_PROJECTION_CLOG;
		zero.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
		zero.normalized_info = CLOG_ZEROPAGE;
		InRecovery = true;
		UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &zero));
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, status));
		UT_ASSERT_EQ(fixture_status(100), status);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_native_record_mutations_invalidate_even_unchanged_bytes)
{
	for (unsigned kind = 0; kind < 3; ++kind) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		ClusterSideProjectionOperationV1 op = {0};
		xl_clog_zeropage_full birth = {0, 0, (UINT64_C(7) << 32) | 3};
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		if (held == NULL) return;
		op.kind = CLUSTER_SIDE_PROJECTION_CLOG;
		op.action = kind == 2 ? CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE
			: CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
		op.normalized_info = kind == 2 ? CLOG_TRUNCATE
			: kind == 1 ? CLOG_ZEROPAGE_FULL : CLOG_ZEROPAGE;
		if (kind == 2) { op.oldest_xid = 3; op.oldest_database = 11; }
		InRecovery = true;
		if (kind == 1)
			UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
				(const uint8 *)&birth, sizeof(birth)));
		else
			UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		/* Drop the new clean FULL input before checking equal-byte mutation:
		 * the original held observation must still remain invalidated. */
		if (kind == 1)
			clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_local_native_status_zero_and_redo_invalidate)
{
	for (unsigned route = 0; route < 4; ++route) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		XLogReaderState reader = {0};
		DecodedXLogRecord decoded = {0};
		xl_clog_zeropage_full birth = {0, 0, (UINT64_C(7) << 32) | 3};
		int page = 0;
		UT_ASSERT(relmap_mutation_open(0, route == 0 ? TRANSACTION_STATUS_ABORTED
			: TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		if (held == NULL) return;
		if (route == 0)
			ClusterClogAdoptNativeStatus(100, TRANSACTION_STATUS_ABORTED);
		else if (route == 1) {
			LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
			SimpleLruWritePage(XactCtl, ZeroCLOGPage(0, false));
			LWLockRelease(XactSLRULock);
		} else {
			reader.record = &decoded;
			decoded.main_data = route == 2 ? (char *)&page : (char *)&birth;
			decoded.main_data_len = route == 2 ? sizeof(page) : sizeof(birth);
			decoded.max_block_id = -1;
			decoded.header.xl_info = route == 2 ? CLOG_ZEROPAGE : CLOG_ZEROPAGE_FULL;
			clog_redo(&reader);
		}
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_verify_refusal_other_origin_and_release_preserve_custody)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL, *second = NULL;
	ClusterSideProjectionOperationV1 zero = {0};
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_ABORTED, &base, &held));
	if (held == NULL) return;
	fixture_origin = 1; fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
	InRecovery = true;
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, -1, NULL,
		TRANSACTION_STATUS_COMMITTED));
	zero.kind = CLUSTER_SIDE_PROJECTION_CLOG;
	zero.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	zero.normalized_info = CLOG_ZEROPAGE;
	UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &zero));
	InRecovery = false;
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &second));
	UT_ASSERT_EQ(transient_fds, 2);
	cluster_clog_relmap_base_release_v1(&held);
	InRecovery = true;
	UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, NULL));
	InRecovery = false;
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &second));
	InRecovery = true;
	UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
		TRANSACTION_STATUS_COMMITTED));
	relmap_mutation_refuses(&base, &second);
}

UT_TEST(relmap_failed_native_mutation_cannot_revalidate_old_bytes)
{
	for (unsigned failure = 0; failure < 2; ++failure) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		volatile bool caught = false, ok = true;
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
		if (held == NULL) return;
		InRecovery = true;
		/* Exercise terminal cleanup with elog's real holdoff reset. */
		relmap_base_interrupts = true;
		if (failure == 0) directory_sync_error = EIO;
		else fail_dirsync = expect_error = true;
		PG_TRY();
		{
			ok = cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL,
				TRANSACTION_STATUS_COMMITTED);
		}
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(failure == 0 ? !ok : caught);
		directory_sync_error = 0; fail_dirsync = expect_error = false;
		relmap_base_interrupts = true;
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		relmap_mutation_refuses(&base, &held);
	}
}

static unsigned relmap_mutation_recheck_target;
static void
relmap_mutation_during_root(void)
{
	if (relmap_base_rechecks != relmap_mutation_recheck_target)
		return;
	relmap_base_recheck_hook = NULL;
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	SimpleLruWritePage(XactCtl, ZeroCLOGPage(0, false));
	LWLockRelease(XactSLRULock);
}

UT_TEST(relmap_native_mutation_during_first_or_final_root_refuses)
{
	for (unsigned existing = 0; existing < 2; ++existing) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		relmap_base_reset(0); fixture_zero(0);
		clog_recovery_cache_discard(XactCtl);
		if (existing)
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
		relmap_mutation_recheck_target = relmap_base_rechecks + 2;
		relmap_base_recheck_hook = relmap_mutation_during_root;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT(relmap_base_recheck_hook == NULL);
		UT_ASSERT(!authority);
		UT_ASSERT_EQ(base.queried_full_xid, 0);
		UT_ASSERT_EQ(transient_fds, existing);
		cluster_clog_relmap_base_release_v1(&held);
		fixture_idle();
	}
}
