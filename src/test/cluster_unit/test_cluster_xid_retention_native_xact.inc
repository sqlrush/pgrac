/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_native_xact.inc
 *     Original transaction stack, retained allocator and native CLOG.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_native_xact.inc
 * NOTES
 *     Admission, qualified directories, locks and WAL insertion are controlled.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
static void
retained_xact_extend(FullTransactionId candidate)
{
	UT_ASSERT(held && !retention_held);
	if (!test_xid_native_clog_extend(candidate))
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("controlled native CLOG WAL failure")));
	if (retained_clog_invalidate)
		relmap_runtime_shmem->directories[retention_origin].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
}

static void
retained_xact_start(uint64 first, bool striped, bool fail)
{
	xact_caller_fixture(first, 4);
	xact_retained_entry_test = true;
	retention_full_clog_leaf = retained_xact_extend;
	retained_clog_invalidate = false;
	cluster_shared_catalog = cluster_xid_striping = striped;
	reservation = true;
	floor_full = 0;
	limit_full = UINT64_MAX;
	test_xid_native_clog_start(fail, cluster_node_id);
}

static FullTransactionId
retained_xact_top(void)
{
	return xact_source_retained(retention_sysid, retention_dbinc, retention_origin);
}

static void
retained_xact_error(bool child, unsigned expected)
{
	volatile bool caught = false;

	retention_error_expected = true;
	retention_error_code = 0;
	PG_TRY();
	{
		if (child)
			(void)xact_source_current();
		else
			(void)retained_xact_top();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(retention_error_code, expected);
	UT_ASSERT(!retention_held);
	/* Only original outer native-lock ERROR cleanup is controlled here. */
	if (held) {
		HOLD_INTERRUPTS();
		LWLockRelease(XidGenLock);
	}
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	retention_error_expected = false;
}

UT_TEST(retained_xact_top_and_children_use_actual_native_birth)
{
	const uint64 top = UINT64_C(0x500008004);
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionPin pins[2];
	TransactionStateData middle = { 0 }, child = { 0 };
	XidNativeClogState state;

	retained_xact_start(UINT64_C(0x500007ffe), true, false);
	UT_ASSERT_EQ(U64FromFullTransactionId(retained_xact_top()), top);
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 7, UINT64_C(0x500008000));
	UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), top);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(sample.owner.top_full_xid, top);
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
	memcpy(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins));
	middle.state = child.state = TRANS_INPROGRESS;
	middle.nestingLevel = 2; child.nestingLevel = 3;
	middle.parent = &TopTransactionStateData; child.parent = &middle;
	middle.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	child.curTransactionOwner = (ResourceOwner)(uintptr_t)13;
	CurrentTransactionState = &child;
	xact_wal_level = WAL_LEVEL_LOGICAL;
	UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_current()), top + 32);
	UT_ASSERT_EQ(U64FromFullTransactionId(middle.fullTransactionId), top + 16);
	UT_ASSERT_EQ(xact_links, 2);
	UT_ASSERT_EQ(xact_child[0], (uint32)(top + 16));
	UT_ASSERT_EQ(xact_parent[0], (uint32)top);
	UT_ASSERT_EQ(xact_child[1], (uint32)(top + 32));
	UT_ASSERT_EQ(xact_parent[1], (uint32)(top + 16));
	UT_ASSERT_EQ(xact_predicates, 1);
	UT_ASSERT_EQ(xact_locks, 3);
	UT_ASSERT_EQ(xact_wal_records, 1);
	for (unsigned i = 0; i < 3; i++)
		UT_ASSERT(xact_lock_owner[i] == (ResourceOwner)(uintptr_t)(11 + i));
	UT_ASSERT_EQ(U64FromFullTransactionId(retained_xact_top()), top);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, top + 32);
	UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[4].pins, sizeof(pins)), 0);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
	UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors, 0);
	retained_clog_stop();
}

UT_TEST(retained_xact_wrap_and_ordinary_identity_are_distinct)
{
	ClusterRelmapRetentionPin pin;
	XidNativeClogState state;
	VariableCacheData native;

	retained_xact_start(UINT64_C(0x7fffffffe), true, false);
	retained_clog_pin(UINT64_C(0x7fffffffd));
	pin = relmap_runtime_shmem->directories[4].pins[0];
	UT_ASSERT_EQ(U64FromFullTransactionId(retained_xact_top()), UINT64_C(0x800000004));
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 4, UINT64_C(0x800000003));
	retention_dbinc++;
	retained_xact_error(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&pin, relmap_runtime_shmem->directories[4].pins, sizeof(pin)), 0);
	retained_clog_stop();

	retained_xact_start(2052, false, false);
	UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_top()), 2052);
	native = variables;
	retained_xact_error(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&native, &variables, sizeof(native)), 0);
	UT_ASSERT_EQ(full_clog_fixture.count, 0);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, 2051);
	retained_clog_stop();
}

UT_TEST(retained_xact_identity_and_admission_refuse_before_effects)
{
	for (unsigned which = 0; which < 9; which++) {
		ClusterRelmapRetentionDirectory saved;
		VariableCacheData native;
		unsigned expected = ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE;

		retained_xact_start(UINT64_C(0x500008000), false, false);
		saved = relmap_runtime_shmem->directories[4];
		native = variables;
		switch (which) {
		case 0: retention_sysid = 0; break;
		case 1: retention_dbinc = 0; break;
		case 2: retention_origin = 3; break;
		case 3: TopTransactionStateData.state = TRANS_COMMIT; break;
		case 4: cluster_enabled = false; break;
		case 5: xact_leaving = true; expected = ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS; break;
		case 6: xact_removed = true; expected = ERRCODE_CLUSTER_NODE_REMOVED_FENCED; break;
		case 7: xact_join = CLUSTER_JOIN_GATE_BLOCK_53R60; expected = ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS; break;
		case 8: xact_join = CLUSTER_JOIN_GATE_BLOCK_53R61; expected = ERRCODE_CLUSTER_JOIN_REJECTED_STALE; break;
		}
		retained_xact_error(false, expected);
		UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4], sizeof(saved)), 0);
		UT_ASSERT_EQ(memcmp(&native, &variables, sizeof(native)), 0);
		UT_ASSERT_EQ(full_clog_fixture.count | full_clog_fixture.raw_calls | native_publish_count, 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_xact_guard_and_native_error_never_retry)
{
	for (unsigned which = 0; which < 4; which++) {
		ClusterRelmapRetentionPin pin;
		TransactionStateData child = { 0 };
		XidNativeClogState first, again;
		unsigned expected = which == 2 ? ERRCODE_DISK_FULL : ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE;

		retained_xact_start(UINT64_C(0x500008000), false, which == 2);
		retained_clog_pin(which == 1 ? UINT64_C(0x480008000) : UINT64_C(0x500007fff));
		pin = relmap_runtime_shmem->directories[4].pins[0];
		if (which == 0) relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		retained_clog_invalidate = which == 3;
		retained_xact_error(false, expected);
		test_xid_native_clog_state(&first);
		UT_ASSERT_EQ(native_publish_count, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), 0);
		retained_clog_invalidate = false;
		relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_COMPLETE;
		retained_xact_error(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
		child.parent = &TopTransactionStateData;
		CurrentTransactionState = &child;
		retained_xact_error(true, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		test_xid_native_clog_state(&again);
		UT_ASSERT_EQ(first.calls, again.calls);
		UT_ASSERT_EQ(first.inserts, again.inserts);
		UT_ASSERT_EQ(memcmp(&pin, relmap_runtime_shmem->directories[4].pins, sizeof(pin)), 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_xact_postpublication_error_keeps_top_and_pins)
{
	ClusterRelmapRetentionPin pin;
	TransactionStateData child = { 0 };
	XidNativeClogState first, again;

	retained_xact_start(UINT64_C(0x500008000), false, false);
	retained_clog_pin(UINT64_C(0x500007fff));
	pin = relmap_runtime_shmem->directories[4].pins[0];
	xact_predicate_error = true;
	retained_xact_error(false, ERRCODE_DISK_FULL);
	UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), UINT64_C(0x500008000));
	UT_ASSERT_EQ(native_publish_count, 1);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500008000));
	test_xid_native_clog_state(&first);
	xact_predicate_error = false;
	retained_xact_error(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
	child.parent = &TopTransactionStateData;
	CurrentTransactionState = &child;
	retained_xact_error(true, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	test_xid_native_clog_state(&again);
	UT_ASSERT_EQ(first.calls, again.calls);
	UT_ASSERT_EQ(memcmp(&pin, relmap_runtime_shmem->directories[4].pins, sizeof(pin)), 0);
	retained_clog_stop();
}

UT_TEST(retained_xact_reset_does_not_leak_to_next_transaction)
{
	retained_xact_start(UINT64_C(0x500008000), false, false);
	UT_ASSERT_EQ(U64FromFullTransactionId(retained_xact_top()), UINT64_C(0x500008000));
	/* Execute the exact helper used by original StartTransaction. The rest of
	 * transaction resource teardown/startup is outside this focused fixture. */
	xact_source_reset();
	XactTopFullTransactionId = TopTransactionStateData.fullTransactionId = InvalidFullTransactionId;
	relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_top()), UINT64_C(0x500008001));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500008000));
	UT_ASSERT_EQ(full_clog_fixture.count, 1);
	UT_ASSERT(full_clog_fixture.raw_calls > 0);
	retained_clog_stop();
}
