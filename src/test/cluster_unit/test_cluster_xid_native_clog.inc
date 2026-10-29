/*-------------------------------------------------------------------------
 * test_cluster_xid_native_clog.inc
 *     Join native allocator candidates with actual FULL page WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_native_clog.inc
 * NOTES
 *     Original allocator and CLOG/SLRU bodies execute. Locks, lease and WAL
 *     insertion are controlled; no lifetime or completion authority.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_xid_native_clog.h"
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool native_clog_enabled;

void
ExtendCLOGFull(FullTransactionId candidate)
{
#ifdef CLUSTER_XID_RETENTION_TEST
	/* Preserve the original retention cases' controlled extension provider.
	 * Only the new explicit native composition enables actual CLOG effects. */
	if (!full_clog_fixture.active) {
		extended();
		return;
	}
#endif
	xid_fixture_extend_full(candidate);
	if (native_clog_enabled) {
		UT_ASSERT(held && reservation && full_clog_fixture.early_publication == 0);
		if (!test_xid_native_clog_extend(candidate))
			longjmp(full_clog_fixture.error, 1);
	}
}

static void
native_clog_begin(uint64 next, bool fail)
{
	full_clog_reset(next);
	test_xid_native_clog_start(fail);
	native_clog_enabled = true;
}
static void
native_clog_end(void)
{
	native_clog_enabled = false;
	test_xid_native_clog_stop();
	if (held) LWLockRelease(XidGenLock);
	full_clog_fixture.active = false;
}
static void
native_clog_wal(const XidNativeClogState *state, unsigned calls, uint64 first)
{
	UT_ASSERT_EQ(state->calls, calls);
	UT_ASSERT_EQ(state->begins, 1); UT_ASSERT_EQ(state->inserts, 1);
	UT_ASSERT_EQ(state->length, sizeof(xl_clog_zeropage_full));
	UT_ASSERT_EQ(state->info, CLOG_ZEROPAGE_FULL);
	UT_ASSERT_EQ(state->bytes.first_full_xid, first);
	UT_ASSERT_EQ(state->bytes.pageno, (uint32)first / (BLCKSZ * 4));
	UT_ASSERT_EQ(state->bytes.reserved_zero, 0);
	UT_ASSERT_EQ(state->dirty_pages, 1); UT_ASSERT(state->page_zero);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls | full_clog_fixture.early_publication | lock_errors, 0);
}

UT_TEST(native_allocator_full_birth_uses_gap_page_and_original_candidate)
{
	XidNativeClogState state;
	native_clog_begin(UINT64_C(0x500007ffe), false);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)), UINT64_C(0x500008004));
	test_xid_native_clog_state(&state);
	native_clog_wal(&state, 7, UINT64_C(0x500008000));
	UT_ASSERT_EQ(proc.xid, 32772); UT_ASSERT_EQ(visible_xids[0], 32772);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008005));
	UT_ASSERT(!held && !state.xid_held && !state.slru_held);
	native_clog_end();
}
UT_TEST(native_allocator_full_birth_keeps_epoch_through_subxact_wrap)
{
	XidNativeClogState state;
	native_clog_begin(UINT64_C(0x7fffffffe), false);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(true)), UINT64_C(0x800000004));
	test_xid_native_clog_state(&state);
	native_clog_wal(&state, 4, UINT64_C(0x800000003));
	UT_ASSERT_EQ(proc.xid | visible_xids[0], 0); UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(proc.subxids.xids[0], 4);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x800000005));
	UT_ASSERT(!held && !state.xid_held && !state.slru_held);
	native_clog_end();
}
UT_TEST(native_allocator_full_birth_rederives_before_any_native_call)
{
	XidNativeClogState state;
	native_clog_begin(2052, false);
	reservation = false; advance_while_unlocked = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)), 2084);
	test_xid_native_clog_state(&state);
	UT_ASSERT_EQ(waits, 1); UT_ASSERT_EQ(state.calls, 1);
	UT_ASSERT_EQ(state.begins | state.inserts | state.dirty_pages, 0);
	UT_ASSERT_EQ(full_clog_fixture.full[0], 2084); UT_ASSERT_EQ(visible_xids[0], 2084);
	UT_ASSERT_EQ(early_extensions | lock_errors, 0);
	UT_ASSERT(!held && !state.xid_held && !state.slru_held);
	native_clog_end();
}
UT_TEST(native_allocator_full_birth_wal_error_keeps_native_duty_unpublished)
{
	XidNativeClogState state;
	native_clog_begin(UINT64_C(0x500007ffe), true);
	UT_ASSERT(full_clog_call_caught());
	test_xid_native_clog_state(&state);
	native_clog_wal(&state, 3, UINT64_C(0x500008000));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500007ffe));
	UT_ASSERT_EQ(proc.xid | visible_xids[0] | subxids[0].count, 0);
	UT_ASSERT(held && state.xid_held && state.slru_held);
	native_clog_end();
}

UT_TEST(native_allocator_captured_gap_wal_replays_original_page)
{
	XidNativeClogState before, after;
	XidNativeClogReplayState replay;

	native_clog_begin(UINT64_C(0x500007ffe), false);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)), UINT64_C(0x500008004));
	test_xid_native_clog_state(&before);
	native_clog_wal(&before, 7, UINT64_C(0x500008000));
	test_xid_native_clog_replay(XID_CLOG_REPLAY_VALID, &replay);
	UT_ASSERT(replay.io_ok && !replay.file_existed && !replay.caught);
	UT_ASSERT_EQ(replay.writes, 1); UT_ASSERT_EQ(replay.inserts, 0);
	UT_ASSERT_EQ(replay.file_size, 2 * BLCKSZ);
	UT_ASSERT(replay.target_zero && !replay.target_sentinel && replay.neighbor_sentinel);
	test_xid_native_clog_state(&after);
	UT_ASSERT_EQ(memcmp(&before.bytes, &after.bytes, sizeof(before.bytes)), 0);
	UT_ASSERT_EQ(after.inserts, 1); UT_ASSERT_EQ(after.dirty_pages, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008005));
	UT_ASSERT_EQ(proc.xid, 32772); UT_ASSERT_EQ(visible_xids[0], 32772);
	UT_ASSERT(!held && !after.xid_held && !after.slru_held);
	native_clog_end();
}
UT_TEST(native_allocator_captured_wrap_wal_replays_page_zero)
{
	XidNativeClogState before, after;
	XidNativeClogReplayState replay;

	native_clog_begin(UINT64_C(0x7fffffffe), false);
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(true)), UINT64_C(0x800000004));
	test_xid_native_clog_state(&before);
	native_clog_wal(&before, 4, UINT64_C(0x800000003));
	test_xid_native_clog_replay(XID_CLOG_REPLAY_VALID, &replay);
	UT_ASSERT(replay.io_ok && !replay.file_existed && !replay.caught);
	UT_ASSERT_EQ(replay.writes, 1); UT_ASSERT_EQ(replay.inserts, 0);
	UT_ASSERT_EQ(replay.file_size, 2 * BLCKSZ);
	UT_ASSERT(replay.target_zero && !replay.target_sentinel && replay.neighbor_sentinel);
	test_xid_native_clog_state(&after);
	UT_ASSERT_EQ(memcmp(&before.bytes, &after.bytes, sizeof(before.bytes)), 0);
	UT_ASSERT_EQ(after.inserts, 1); UT_ASSERT_EQ(after.dirty_pages, 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x800000005));
	UT_ASSERT_EQ(proc.xid | visible_xids[0], 0); UT_ASSERT_EQ(subxids[0].count, 1);
	UT_ASSERT_EQ(proc.subxids.xids[0], 4);
	UT_ASSERT(!held && !after.xid_held && !after.slru_held);
	native_clog_end();
}
UT_TEST(native_allocator_captured_wal_refuses_malformed_redo_before_write)
{
	for (unsigned fault = XID_CLOG_REPLAY_RESERVED; fault <= XID_CLOG_REPLAY_BLOCKREF; ++fault) {
		XidNativeClogState before, after;
		XidNativeClogReplayState replay;

		native_clog_begin(UINT64_C(0x500007ffe), false);
		UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionIdFullCLOG(false)), UINT64_C(0x500008004));
		test_xid_native_clog_state(&before);
		native_clog_wal(&before, 7, UINT64_C(0x500008000));
		test_xid_native_clog_replay((XidNativeClogReplayFault)fault, &replay);
		UT_ASSERT(replay.io_ok && !replay.file_existed && replay.caught);
		UT_ASSERT_EQ(replay.writes | replay.inserts, 0);
		UT_ASSERT_EQ(replay.file_size, 2 * BLCKSZ);
		UT_ASSERT(!replay.target_zero && replay.target_sentinel && replay.neighbor_sentinel);
		test_xid_native_clog_state(&after);
		UT_ASSERT_EQ(memcmp(&before.bytes, &after.bytes, sizeof(before.bytes)), 0);
		UT_ASSERT_EQ(after.inserts, 1); UT_ASSERT_EQ(after.dirty_pages, 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), UINT64_C(0x500008005));
		UT_ASSERT_EQ(proc.xid, 32772); UT_ASSERT_EQ(visible_xids[0], 32772);
		UT_ASSERT(!held && !after.xid_held && !after.slru_held);
		native_clog_end();
	}
}
