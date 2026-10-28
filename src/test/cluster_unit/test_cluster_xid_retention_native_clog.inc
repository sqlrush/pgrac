/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_retention_native_clog.inc
 *    Retained allocation through original native FULL CLOG and SLRU.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_retention_native_clog.inc
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_xid_native_clog.h"

static void
retained_native_extend(FullTransactionId full)
{
	/* Namespace-separated lock fixtures mirror the one original XidGenLock. */
	UT_ASSERT(held && !retention_held);
	test_xid_native_clog_extend(full);
	UT_ASSERT(held && !retention_held);
}

static void
retained_native_start(uint64 first, unsigned origin)
{
	retention_full_fixture(first, origin);
	test_xid_native_clog_start(origin);
	full_clog_native_extend = retained_native_extend;
}

static void
retained_native_stop(void)
{
	full_clog_native_extend = NULL;
	test_xid_native_clog_stop();
	full_clog_fixture.active = false;
}

UT_TEST(retained_native_clog_initializes_before_original_top_and_child_publication)
{
	uint64 first = (UINT64CONST(7) << 32) | 32768;
	TestXidNativeClogState state;

	retained_native_start(first, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(false, 0)), first);
	test_xid_native_clog_state(1, &state);
	UT_ASSERT_EQ(state.calls, 1);
	UT_ASSERT_EQ(state.inserts, 1);
	UT_ASSERT_EQ(state.info, CLOG_ZEROPAGE_FULL);
	UT_ASSERT_EQ(state.length, sizeof(state.birth));
	UT_ASSERT_EQ(state.birth.first_full_xid, first);
	UT_ASSERT_EQ(state.birth.pageno, 1);
	UT_ASSERT_EQ(state.birth.reserved_zero, 0);
	UT_ASSERT(state.dirty && state.page_zero);
	UT_ASSERT_EQ(state.writes, 0);
	UT_ASSERT_EQ(full_clog_fixture.early_publication, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), first);
	UT_ASSERT_EQ(proc.xid, 32768);
	UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(true, 0)), first + 1);
	test_xid_native_clog_state(1, &state);
	UT_ASSERT_EQ(state.calls, 2);
	UT_ASSERT_EQ(state.inserts, 1);
	UT_ASSERT(state.page_zero && state.dirty);
	UT_ASSERT_EQ(proc.subxids.xids[0], 32769);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, first + 1);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first + 2);
	UT_ASSERT_EQ(retention_final_observations, 2);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
	UT_ASSERT(test_xid_native_clog_writeback(1));
	retained_native_stop();
}

UT_TEST(retained_native_clog_gap_preserves_full_page_birth_across_epoch)
{
	static const uint64 starts[] = {UINT64_C(0x500007ffe), UINT64_C(0x7fffffffe)};
	static const uint64 issued[] = {UINT64_C(0x500008004), UINT64_C(0x800000004)};
	static const uint64 births[] = {UINT64_C(0x500008000), UINT64_C(0x800000003)};
	static const unsigned calls[] = {7, 4};

	for (unsigned variant = 0; variant < lengthof(starts); variant++) {
		int page = variant == 0 ? 1 : 0;
		TestXidNativeClogState state;

		retained_native_start(starts[variant], 4);
		cluster_shared_catalog = cluster_xid_striping = true;
		reservation = true;
		limit_full = UINT64_MAX;
		UT_ASSERT_EQ(U64FromFullTransactionId(retention_full_call(variant != 0, 0)), issued[variant]);
		test_xid_native_clog_state(page, &state);
		UT_ASSERT_EQ(state.calls, calls[variant]);
		UT_ASSERT_EQ(state.inserts, 1);
		UT_ASSERT_EQ(state.info, CLOG_ZEROPAGE_FULL);
		UT_ASSERT_EQ(state.length, sizeof(state.birth));
		UT_ASSERT_EQ(state.birth.first_full_xid, births[variant]);
		UT_ASSERT_EQ(state.birth.pageno, page);
		UT_ASSERT_EQ(state.birth.reserved_zero, 0);
		UT_ASSERT(state.page_zero && state.dirty);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, issued[variant]);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), issued[variant] + 1);
		UT_ASSERT_EQ(retention_final_observations, 1);
		UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
		UT_ASSERT(test_xid_native_clog_writeback(page));
		retained_native_stop();
	}
}

UT_TEST(retained_native_clog_refusal_preserves_actual_initialization_and_pins)
{
	for (unsigned late = 0; late < 2; late++) {
		uint64 first = (UINT64CONST(7) << 32) | 32768;
		TestXidNativeClogState state;
		ClusterRelmapRetentionPin pin;

		retained_native_start(first, 0);
		relmap_runtime_shmem->directories[0].pins[1].occupied = true;
		relmap_runtime_shmem->directories[0].pins[1].key.top_full_xid = first - 1;
		pin = relmap_runtime_shmem->directories[0].pins[1];
		if (late) retention_fault = 2;
		else relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		(void)retention_full_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		test_xid_native_clog_state(1, &state);
		UT_ASSERT_EQ(state.calls, late);
		UT_ASSERT_EQ(state.inserts, late);
		UT_ASSERT_EQ(state.writes, 0);
		UT_ASSERT_EQ(state.dirty, late != 0);
		if (late) {
			UT_ASSERT(state.page_zero);
			UT_ASSERT_EQ(state.birth.first_full_xid, first);
		}
		UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0] | retention_final_observations, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entries[0].cluster_top_full_xid_atomic), 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, first - 1);
		UT_ASSERT_EQ(memcmp(&relmap_runtime_shmem->directories[0].pins[1], &pin, sizeof(pin)), 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		/* No second initialization, successful retry or rollback follows. */
		retained_native_stop();
	}
}
