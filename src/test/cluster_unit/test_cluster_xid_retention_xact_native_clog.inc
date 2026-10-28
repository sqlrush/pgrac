/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_retention_xact_native_clog.inc
 *    Original transaction-stack retention with native FULL CLOG.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_retention_xact_native_clog.inc
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */

static FullTransactionId
xact_retained_full_native(bool child)
{
	UT_ASSERT(!held && !retention_held);
	return cluster_relmap_retention_allocate_full_clog_xid_v2(child,
		retention_sysid, retention_dbinc, retention_origin);
}

static void
xact_retained_native_start(uint64 first, unsigned origin)
{
	xact_caller_fixture(first, origin);
	memset(&full_clog_fixture, 0, sizeof(full_clog_fixture));
	full_clog_fixture.active = true;
	test_xid_native_clog_start(origin);
	full_clog_native_extend = retained_native_extend;
	xact_native_allocator = xact_retained_full_native;
}

static void
xact_retained_native_stop(void)
{
	xact_native_allocator = NULL;
	retained_native_stop();
}

UT_TEST(original_xact_full_native_retains_top_owner_through_child_publication)
{
	uint64 first = UINT64_C(0x700008000);
	ClusterRelmapOwnerSampleV2 sample, repeated;
	ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];
	TransactionStateData middle = {0}, child = {0};
	TestXidNativeClogState native;

	xact_retained_native_start(first, 0);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(sample.owner.top_full_xid, first);
	UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), first);
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
	memcpy(pins, relmap_runtime_shmem->directories[0].pins, sizeof(pins));
	middle.state = child.state = TRANS_INPROGRESS;
	middle.nestingLevel = 2; child.nestingLevel = 3;
	middle.parent = &TopTransactionStateData; child.parent = &middle;
	middle.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	child.curTransactionOwner = (ResourceOwner)(uintptr_t)13;
	CurrentTransactionState = &child;
	xact_wal_level = WAL_LEVEL_LOGICAL;
	AssignTransactionId(&child);
	UT_ASSERT_EQ(U64FromFullTransactionId(middle.fullTransactionId), UINT64_C(0x700008001));
	UT_ASSERT_EQ(U64FromFullTransactionId(child.fullTransactionId), UINT64_C(0x700008002));
	UT_ASSERT_EQ(xact_links, 2);
	UT_ASSERT_EQ(xact_child[0], 32769); UT_ASSERT_EQ(xact_parent[0], 32768);
	UT_ASSERT_EQ(xact_child[1], 32770); UT_ASSERT_EQ(xact_parent[1], 32769);
	UT_ASSERT_EQ(xact_predicates, 1); UT_ASSERT_EQ(xact_locks, 3);
	for (unsigned i = 0; i < 3; i++)
		UT_ASSERT(xact_lock_owner[i] == (ResourceOwner)(uintptr_t)(11 + i));
	UT_ASSERT(xact_source_resource_owner == (ResourceOwner)(uintptr_t)99);
	UT_ASSERT_EQ(xact_wal_records, 1);
	UT_ASSERT(TopTransactionStateData.didLogXid);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&repeated));
	UT_ASSERT_EQ(memcmp(&sample, &repeated, sizeof(sample)), 0);
	UT_ASSERT_EQ(caller_assignments, 3);
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[0].pins, sizeof(pins)), 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, UINT64_C(0x700008002));
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), first);
	test_xid_native_clog_state(1, &native);
	UT_ASSERT_EQ(native.calls, 3); UT_ASSERT_EQ(native.inserts, 1);
	UT_ASSERT_EQ(native.birth.first_full_xid, first);
	UT_ASSERT_EQ(native.birth.pageno, 1); UT_ASSERT_EQ(native.birth.reserved_zero, 0);
	UT_ASSERT_EQ(native.length, sizeof(native.birth)); UT_ASSERT_EQ(native.info, CLOG_ZEROPAGE_FULL);
	UT_ASSERT(native.page_zero && native.dirty);
	UT_ASSERT_EQ(caller_owner_lock_errors + lock_errors + native_publish_order_errors, 0);
	UT_ASSERT(test_xid_native_clog_writeback(1));
	xact_retained_native_stop();
}

UT_TEST(original_xact_full_native_recursion_keeps_actual_epoch_and_striped_gap)
{
	ClusterRelmapOwnerSampleV2 sample;
	TransactionStateData child = {0};
	TestXidNativeClogState native;

	xact_retained_native_start(UINT64_C(0x7fffffffe), 4);
	cluster_shared_catalog = cluster_xid_striping = true;
	reservation = true; limit_full = UINT64_MAX;
	child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
	child.parent = &TopTransactionStateData;
	child.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	CurrentTransactionState = &child;
	AssignTransactionId(&child);
	UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), UINT64_C(0x800000004));
	UT_ASSERT_EQ(U64FromFullTransactionId(child.fullTransactionId), UINT64_C(0x800000014));
	UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), UINT64_C(0x800000004));
	UT_ASSERT_EQ(xact_links, 1); UT_ASSERT_EQ(xact_child[0], 20); UT_ASSERT_EQ(xact_parent[0], 4);
	UT_ASSERT_EQ(proc.xid, 4); UT_ASSERT_EQ(proc.subxids.xids[0], 20);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), UINT64_C(0x800000004));
	UT_ASSERT_EQ(caller_assignments, 2);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(sample.owner.top_full_xid, UINT64_C(0x800000004));
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].pins[0].key.top_full_xid, UINT64_C(0x800000004));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x800000014));
	test_xid_native_clog_state(0, &native);
	UT_ASSERT_EQ(native.calls, 20); UT_ASSERT_EQ(native.inserts, 1);
	UT_ASSERT_EQ(native.birth.first_full_xid, UINT64_C(0x800000003));
	UT_ASSERT_EQ(native.birth.pageno, 0); UT_ASSERT(native.page_zero && native.dirty);
	UT_ASSERT_EQ(caller_owner_lock_errors + lock_errors + native_publish_order_errors, 0);
	UT_ASSERT(test_xid_native_clog_writeback(0));
	xact_retained_native_stop();
}

UT_TEST(original_xact_full_native_refusals_keep_top_pin_and_completed_initialization)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];
	TransactionStateData child = {0};
	TestXidNativeClogState native;
	volatile bool caught = false;

	xact_retained_native_start(UINT64_C(0x700008000), 0);
	xact_leaving = true;
	xact_capture_error(ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS);
	test_xid_native_clog_state(1, &native);
	UT_ASSERT_EQ(native.calls, 0); UT_ASSERT_EQ(caller_assignments, 0);
	xact_leaving = false;
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
	memcpy(pins, relmap_runtime_shmem->directories[0].pins, sizeof(pins));
	child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
	child.parent = &TopTransactionStateData;
	child.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	CurrentTransactionState = &child;
	/* Original late guard fault after successful native FULL extension. */
	extensions = 0; retention_fault = 2;
	retention_error_expected = true;
	PG_TRY();
	{
		AssignTransactionId(&child);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	retention_error_expected = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(retention_error_code, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), UINT64_C(0x700008000));
	UT_ASSERT_EQ(U64FromFullTransactionId(child.fullTransactionId), 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x700008001));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, UINT64_C(0x700008000));
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[0].pins, sizeof(pins)), 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	UT_ASSERT_EQ(xact_links, 0); UT_ASSERT_EQ(xact_locks, 1); UT_ASSERT_EQ(caller_assignments, 1);
	UT_ASSERT_EQ(subxids[0].count, 0);
	test_xid_native_clog_state(1, &native);
	UT_ASSERT_EQ(native.calls, 2); UT_ASSERT_EQ(native.inserts, 1);
	UT_ASSERT_EQ(native.birth.first_full_xid, UINT64_C(0x700008000));
	UT_ASSERT(native.page_zero && native.dirty);
	UT_ASSERT(!retention_held);
	/* Only original outer resource-owner ERROR cleanup is controlled. */
	if (held) { HOLD_INTERRUPTS(); LWLockRelease(XidGenLock); }
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(caller_owner_lock_errors + lock_errors + native_publish_order_errors, 0);
	xact_retained_native_stop();
}
