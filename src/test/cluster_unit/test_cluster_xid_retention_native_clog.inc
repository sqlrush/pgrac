/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_native_clog.inc
 *     Compose retained allocation with original native CLOG initialization.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_native_clog.inc
 * NOTES
 *     Original runtime, allocator, CLOG, SLRU and redo bodies execute.
 *     Qualified directories, authority, locks and WAL insertion are controlled.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_xid_native_clog.h"

static bool retained_clog_invalidate;

static void
retained_clog_extend(FullTransactionId candidate)
{
	UT_ASSERT(held && !retention_held);
	UT_ASSERT_EQ(native_publish_count, 0);
	UT_ASSERT_EQ(proc.xid | visible_xids[0] | subxids[0].count, 0);
	if (!test_xid_native_clog_extend(candidate))
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("controlled native CLOG WAL failure")));
	if (retained_clog_invalidate)
		relmap_runtime_shmem->directories[retention_origin].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
}

static void
retained_clog_start(uint64 first, bool striped, bool fail)
{
	caller_fixture(first, 4);
	retention_full_clog_leaf = retained_clog_extend;
	retained_clog_invalidate = false;
	cluster_shared_catalog = cluster_xid_striping = striped;
	reservation = true;
	floor_full = 0;
	limit_full = UINT64_MAX;
	test_xid_native_clog_start(fail, cluster_node_id);
}

static void
retained_clog_stop(void)
{
	retention_full_clog_leaf = NULL;
	test_xid_native_clog_stop();
	full_clog_fixture.active = false;
}

/* A pre-existing protected obligation; no terminal or TT receipt is supplied. */
static void
retained_clog_pin(uint64 full_xid)
{
	ClusterRelmapRetentionPin *pin = &relmap_runtime_shmem->directories[retention_origin].pins[0];

	memset(pin, 0, sizeof(*pin));
	pin->key.identity = caller_namespace(true);
	pin->key.generation = 7;
	pin->key.origin = retention_origin;
	pin->key.top_full_xid = full_xid;
	pin->key.membership_epoch = 43;
	pin->key.admitted_incarnation = 31;
	pin->key.boot_incarnation = 17;
	pin->occupied = pin->may_be_staged = true;
	pin->wal_end = 8192;
}

static void
retained_clog_birth(const XidNativeClogState *state, unsigned calls, uint64 first)
{
	UT_ASSERT_EQ(state->calls, calls);
	UT_ASSERT_EQ(state->begins, 1);
	UT_ASSERT_EQ(state->inserts, 1);
	UT_ASSERT_EQ(state->length, sizeof(xl_clog_zeropage_full));
	UT_ASSERT_EQ(state->info, CLOG_ZEROPAGE_FULL);
	UT_ASSERT_EQ(state->bytes.first_full_xid, first);
	UT_ASSERT_EQ(state->bytes.pageno, (uint32)first / (BLCKSZ * 4));
	UT_ASSERT_EQ(state->bytes.reserved_zero, 0);
	UT_ASSERT_EQ(state->dirty_pages, 1);
	UT_ASSERT(state->page_zero);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication, 0);
}

UT_TEST(retained_native_birth_keeps_owner_pins_and_actual_top)
{
	ClusterRelmapOwnerSampleV2 sample;
	XidNativeClogState state;
	ClusterRelmapRetentionDirectory *directory;
	bool captured;

	retained_clog_start(UINT64_C(0x500007ffe), true, false);
	captured = cluster_relmap_owner_capture_v2(&sample);
	UT_ASSERT(captured);
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 7, UINT64_C(0x500008000));
	UT_ASSERT_EQ(sample.owner.top_full_xid, UINT64_C(0x500008004));
	UT_ASSERT_EQ(sample.owner.node, 4);
	UT_ASSERT_EQ(proc.xid, 32772);
	UT_ASSERT_EQ(visible_xids[0], 32772);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008005));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), UINT64_C(0x500008004));
	if (captured) {
		UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
	}
	directory = &relmap_runtime_shmem->directories[4];
	UT_ASSERT(directory->pins[0].occupied && directory->pins[1].occupied);
	UT_ASSERT_EQ(directory->pins[0].key.top_full_xid, UINT64_C(0x500008004));
	UT_ASSERT_EQ(directory->pins[1].key.top_full_xid, UINT64_C(0x500008004));
	UT_ASSERT_EQ(directory->last_issued, UINT64_C(0x500008004));
	UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors, 0);
	UT_ASSERT(!held && !retention_held && !state.xid_held && !state.slru_held);
	retained_clog_stop();
}

UT_TEST(retained_native_birth_keeps_subxid_epoch_and_replays_captured_bytes)
{
	XidNativeClogState state;
	XidNativeClogReplayState replay;
	ClusterRelmapRetentionPin saved;

	retained_clog_start(UINT64_C(0x7fffffffe), true, false);
	retained_clog_pin(UINT64_C(0x7fffffffd));
	saved = relmap_runtime_shmem->directories[4].pins[0];
	UT_ASSERT_EQ(U64FromFullTransactionId(retention_call(true, 0)), UINT64_C(0x800000004));
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 4, UINT64_C(0x800000003));
	UT_ASSERT_EQ(proc.xid | visible_xids[0], 0);
	UT_ASSERT_EQ(proc.subxids.xids[0], 4);
	UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x800000004));
	UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4].pins[0], sizeof(saved)), 0);
	if (state.inserts == 1) {
		test_xid_native_clog_replay(XID_CLOG_REPLAY_VALID, &replay);
		UT_ASSERT(replay.io_ok && !replay.file_existed && !replay.caught);
		UT_ASSERT_EQ(replay.writes, 1);
		UT_ASSERT_EQ(replay.inserts, 0);
		UT_ASSERT_EQ(replay.file_size, 2 * BLCKSZ);
		UT_ASSERT(replay.target_zero && replay.neighbor_sentinel);
	}
	UT_ASSERT(!held && !retention_held && !state.xid_held && !state.slru_held);
	retained_clog_stop();
}

UT_TEST(retained_native_refusals_precede_clog_and_publication)
{
	for (unsigned which = 0; which < 3; which++) {
		uint64 first = which == 1 ? UINT64_C(0x580008000) : UINT64_C(0x500008000);
		XidNativeClogState state;
		ClusterRelmapRetentionDirectory saved;

		retained_clog_start(first, false, false);
		retained_clog_pin(which == 1 ? UINT64_C(0x500008000) : first - 1);
		if (which == 0) relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 2) cluster_node_id = 5;
		saved = relmap_runtime_shmem->directories[4];
		(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
		test_xid_native_clog_state(&state);
		UT_ASSERT_EQ(state.calls | state.inserts | state.dirty_pages | extensions | native_publish_count, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), first);
		UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4], sizeof(saved)), 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_native_lease_wait_rederives_before_clog)
{
	for (unsigned fail = 0; fail < 2; fail++) {
		XidNativeClogState state;
		ClusterRelmapRetentionPin saved;

		retained_clog_start(2052, true, false);
		retained_clog_pin(2051);
		saved = relmap_runtime_shmem->directories[4].pins[0];
		reservation = false;
		advance_while_unlocked = true;
		retention_recompute = fail != 0;
		retention_fault = fail ? 4 : 0;
		(void)retention_call(false, fail ? ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE : 0);
		test_xid_native_clog_state(&state);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(state.calls, fail ? 0 : 1);
		UT_ASSERT_EQ(state.inserts | state.dirty_pages, 0);
		UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), fail ? 2084 : 2085);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, fail ? 2051 : 2084);
		UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4].pins[0], sizeof(saved)), 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_native_late_guard_refusal_keeps_born_page_and_pins)
{
	XidNativeClogState state;
	ClusterRelmapRetentionPin saved;
	retained_clog_start(UINT64_C(0x500008000), false, false);
	retained_clog_pin(UINT64_C(0x500007fff));
	saved = relmap_runtime_shmem->directories[4].pins[0];
	retained_clog_invalidate = true;
	(void)retention_call(false, ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 1, UINT64_C(0x500008000));
	UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0] | subxids[0].count, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008000));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500007fff));
	UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4].pins[0], sizeof(saved)), 0);
	UT_ASSERT(!state.xid_held && !state.slru_held);
	retained_clog_stop();
}

UT_TEST(retained_native_wal_error_keeps_unpublished_issuance_and_pins)
{
	XidNativeClogState state;
	ClusterRelmapRetentionPin saved;
	retained_clog_start(UINT64_C(0x500008000), false, true);
	retained_clog_pin(UINT64_C(0x500007fff));
	saved = relmap_runtime_shmem->directories[4].pins[0];
	(void)retention_call(false, ERRCODE_DISK_FULL);
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 1, UINT64_C(0x500008000));
	UT_ASSERT_EQ(native_publish_count | proc.xid | visible_xids[0] | subxids[0].count, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008000));
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500007fff));
	UT_ASSERT_EQ(memcmp(&saved, &relmap_runtime_shmem->directories[4].pins[0], sizeof(saved)), 0);
	UT_ASSERT(state.xid_held && state.slru_held);
	retained_clog_stop();
}
