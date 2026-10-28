/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_retention_full_clog.inc
 *    Compose original retention publication with native FULL CLOG inputs.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_retention_full_clog.inc
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */

static void
retention_full_fixture(uint64 candidate, unsigned origin)
{
	retention_fixture(candidate, origin);
	cluster_node_id = origin;
	UT_ASSERT_EQ(GetSystemIdentifier(), retention_sysid);
	memset(&full_clog_fixture, 0, sizeof(full_clog_fixture));
	full_clog_fixture.active = true;
}

static FullTransactionId
retention_full_call(bool child, unsigned expected_error)
{
	FullTransactionId result = InvalidFullTransactionId;
	volatile bool caught = false;

	retention_error_expected = expected_error != 0;
	PG_TRY();
	{
		result = cluster_relmap_retention_allocate_full_clog_xid_v2(child,
			retention_sysid, retention_dbinc, retention_origin);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, expected_error != 0);
	UT_ASSERT_EQ(retention_error_code, expected_error);
	UT_ASSERT(!retention_held);
	if (held) {
		UT_ASSERT(caught);
		HOLD_INTERRUPTS(); /* Only the original native caller ERROR cleanup. */
		LWLockRelease(XidGenLock);
	}
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(lock_errors + native_publish_order_errors, 0);
	retention_error_expected = false;
	return result;
}

UT_TEST(retained_full_clog_publishes_original_top_and_child_after_extension)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		uint64 first = ((variant / 2 ? UINT64CONST(7) : 0) << 32) + 2052;
		unsigned origin = variant % 2 ? CLUSTER_MAX_NODES - 1 : 0;

		retention_full_fixture(first, origin);
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(false, 0)), first);
		UT_ASSERT_EQ(full_clog_fixture.count, 1);
		UT_ASSERT_EQ(full_clog_fixture.full[0], first);
		UT_ASSERT_EQ(full_clog_fixture.early_publication, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), first);
		UT_ASSERT_EQ(proc.xid, 2052);
		UT_ASSERT_EQ(visible_xids[0], 2052);
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(true, 0)), first + 1);
		UT_ASSERT_EQ(full_clog_fixture.count, 2);
		UT_ASSERT_EQ(full_clog_fixture.full[1], first + 1);
		UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
		UT_ASSERT_EQ(proc.subxids.xids[0], 2053);
		UT_ASSERT_EQ(subxids[0].count, 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first + 2);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), first + 1);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[origin].last_issued, first + 1);
		UT_ASSERT_EQ(retention_final_observations, 2);
		UT_ASSERT_EQ(retention_acquires, 4);
	}
	full_clog_fixture.active = false;
}

UT_TEST(retained_full_clog_gap_preserves_page_and_epoch_candidates)
{
	static const uint64 starts[] = {UINT64_C(0x500007ffe), UINT64_C(0x7fffffffe)};
	static const uint64 issued[] = {UINT64_C(0x500008004), UINT64_C(0x800000004)};
	static const unsigned counts[] = {7, 4};

	for (unsigned variant = 0; variant < lengthof(starts); variant++) {
		FullTransactionId next = FullTransactionIdFromU64(starts[variant]);

		retention_full_fixture(starts[variant], 4);
		cluster_shared_catalog = cluster_xid_striping = true;
		reservation = true;
		limit_full = UINT64_MAX;
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(variant != 0, 0)), issued[variant]);
		UT_ASSERT_EQ(full_clog_fixture.count, counts[variant]);
		for (unsigned i = 0; i < counts[variant]; i++) {
			UT_ASSERT_EQ(full_clog_fixture.full[i], U64FromFullTransactionId(next));
			FullTransactionIdAdvance(&next);
		}
		UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
		UT_ASSERT_EQ(extensions, counts[variant] * 3);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), issued[variant] + 1);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, issued[variant]);
		UT_ASSERT_EQ(retention_final_observations, 1);
	}
	full_clog_fixture.active = false;
}

UT_TEST(retained_full_clog_rejects_another_native_origin_before_extension)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned variant = 0; variant < 3; variant++) {
			uint64 first = (UINT64CONST(7) << 32) + 2052;
			ClusterRelmapRetentionDirectory before;

			retention_full_fixture(first, origin);
			relmap_runtime_shmem->directories[origin].pins[0].occupied = true;
			relmap_runtime_shmem->directories[origin].pins[0].key.top_full_xid = first - 1;
			before = relmap_runtime_shmem->directories[origin];
			cluster_node_id = variant == 0 ? -1 : variant == 1 ? CLUSTER_MAX_NODES
				: (origin + 1) % CLUSTER_MAX_NODES;
			(void)retention_full_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
			UT_ASSERT_EQ(retention_acquires | extensions | full_clog_fixture.count
				| native_publish_count | proc.xid | visible_xids[0], 0);
			UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
			UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), 0);
			UT_ASSERT_EQ(memcmp(&relmap_runtime_shmem->directories[origin], &before, sizeof(before)), 0);
		}
	full_clog_fixture.active = false;
}

UT_TEST(retained_full_clog_rejects_another_native_system_before_extension)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		uint64 first = (UINT64CONST(7) << 32) + 2052;
		ClusterRelmapRetentionDirectory before;

		retention_full_fixture(first, 0);
		if (variant == 0) allocation_system_identifier = 0;
		if (variant == 1) allocation_system_identifier++;
		if (variant == 2) {
			retention_sysid++;
			relmap_runtime_shmem->directories[0].system_identifier = retention_sysid;
		}
		relmap_runtime_shmem->directories[0].pins[0].occupied = true;
		relmap_runtime_shmem->directories[0].pins[0].key.top_full_xid = first - 1;
		before = relmap_runtime_shmem->directories[0];
		(void)retention_full_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(retention_acquires | extensions | full_clog_fixture.count
			| native_publish_count | proc.xid | visible_xids[0], 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), 0);
		UT_ASSERT_EQ(memcmp(&relmap_runtime_shmem->directories[0], &before, sizeof(before)), 0);
	}
	full_clog_fixture.active = false;
}

UT_TEST(retained_full_clog_refuses_before_native_extension_and_keeps_pins)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		uint64 first = (UINT64CONST(7) << 32) + 2052;
		ClusterRelmapRetentionDirectory *directory;

		retention_full_fixture(first, 0);
		directory = &relmap_runtime_shmem->directories[0];
		directory->pins[0].occupied = true;
		directory->pins[0].key.top_full_xid = first - 1;
		if (variant == 0) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (variant == 1) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (variant == 2) retention_dbinc++;
		if (variant == 3) Mode = BootstrapProcessing;
		if (variant == 4) directory->pins[0].key.top_full_xid = first - UINT64CONST(0x80000000);
		(void)retention_full_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(full_clog_fixture.count | full_clog_fixture.raw_calls | extensions, 0);
		UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0], 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
		UT_ASSERT_EQ(directory->last_issued, first - 1);
		UT_ASSERT(directory->pins[0].occupied);
	}
	Mode = NormalProcessing;
	full_clog_fixture.active = false;
}

UT_TEST(retained_full_clog_late_refusal_and_slru_error_never_publish)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		uint64 first = (UINT64CONST(7) << 32) + UINT64CONST(0x80000804);
		ClusterRelmapRetentionDirectory *directory;

		retention_full_fixture(first, 0);
		directory = &relmap_runtime_shmem->directories[0];
		directory->pins[1].occupied = true;
		directory->pins[1].key.top_full_xid = first - 1;
		retention_fault = fault;
		(void)retention_full_call(false, fault == 3 ? ERRCODE_DISK_FULL
			: ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(full_clog_fixture.count, 1);
		UT_ASSERT_EQ(full_clog_fixture.full[0], first);
		UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
		UT_ASSERT_EQ(extensions, fault == 3 ? 1 : 3);
		UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0] | retention_final_observations, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), 0);
		UT_ASSERT_EQ(directory->last_issued, first - 1);
		UT_ASSERT(directory->pins[1].occupied);
	}
	full_clog_fixture.active = false;
}
