/* Actual recursive current-MX admission/history/decision helpers; transport,
 * storage and physical native recursion remain controlled external leaves.
 * No native recursive/SQL/TT40 execution claim.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_WALKER_FIXTURE
#include "test_cluster_mx_history_hot_caller.c"
#undef main
#undef HISTORY_WALKER_FIXTURE
void *
palloc0(Size size)
{
	void *result = MemoryContextAlloc(CurrentMemoryContext, size);

	memset(result, 0, size);
	return result;
}
static Snapshot
recursive_setup(bool updated)
{
	Snapshot s = updated ? read_setup(false) : auth_setup(CLUSTER_TX_IN_PROGRESS, false);
	caller_enters = caller_leaves = caller_inflight = 0; caller_closed = false;
	finish_outcome = CLUSTER_TX_IN_PROGRESS;
	exclusive = true; bridge_lock_override = auth_lock;
	mx_history_recursive_open(s); return s;
}
static int
recursive_call(bool history, LockTupleMode mode, MxHeapReadout *out)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_recursive_authorize(&relation, 1, &tuple, history, 145, mode, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static void
recursive_clean(Snapshot s)
{
	MxAuthorizeState state;

	mx_history_recursive_close(); UT_ASSERT_EQ(caller_enters, 1);
	UT_ASSERT_EQ(caller_leaves, 1); UT_ASSERT_EQ(caller_inflight, 0);
	mx_history_authorize_state(&state);
	if (state.operation_id != 0) auth_finished();
	else UT_ASSERT(!state.active);
	auth_clean(s);
}
UT_TEST(recursive_history_hop_returns_full_B_and_normalized_receipts)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT(out.follow_updated_chain && out.successor.valid);
	UT_ASSERT_EQ(out.authoritative_updater, 96); UT_ASSERT_EQ(out.retained.normalized_count, 2);
	UT_ASSERT_EQ(out.retained.proof_count, 1); /* One proved remote updater; requester is added separately. */ UT_ASSERT(content_locked);
	UT_ASSERT_EQ(caller_inflight, 1); UT_ASSERT_EQ(caller_leaves, 0);
	recursive_clean(s);
}
UT_TEST(recursive_two_root_calls_share_one_snapshot_and_admission)
{
	Snapshot s = recursive_setup(false); MxHeapReadout out; uint64 first;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT(!out.follow_updated_chain);
	first = out.retained.operation_id; UT_ASSERT(first != 0);
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
	UT_ASSERT(out.retained.operation_id != 0 && out.retained.operation_id != first);
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 0); UT_ASSERT_EQ(caller_inflight, 1);
	recursive_clean(s);
}
UT_TEST(recursive_TX_restart_keeps_scope_and_discards_old_decision)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out; MxAuthorizeState before, after;
	UT_ASSERT_EQ(recursive_call(true, LockTupleExclusive, &out), 1);
	UT_ASSERT(!out.successor.valid); UT_ASSERT_EQ(out.retained.proof_count, 0);
	mx_history_authorize_state(&before); UT_ASSERT(before.active && !before.finished);
	finish_outcome = CLUSTER_TX_COMMITTED;
	UT_ASSERT_EQ(recursive_call(true, LockTupleExclusive, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Updated); UT_ASSERT(out.successor.valid);
	mx_history_authorize_state(&after); UT_ASSERT_EQ(after.operation_id, before.operation_id);
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_inflight, 1);
	recursive_clean(s);
}
UT_TEST(recursive_closed_scope_never_reenters_or_dispatches)
{
	Snapshot s = recursive_setup(false); MxHeapReadout out;
	pg_atomic_write_u32(&history_gate.transition_closed, 1);
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(caller_enters, 1);
	recursive_clean(s);
}
UT_TEST(recursive_legacy_guard_keeps_raw_members_unreachable)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out;
	UT_ASSERT_EQ(recursive_call(false, LockTupleKeyShare, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(auth_describes, 0); recursive_clean(s);
}
UT_TEST(recursive_incomplete_descriptor_cannot_select_legacy)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out;
	auth_describe_fault = 4;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT(!out.successor.valid); UT_ASSERT_EQ(out.retained.proof_count, 0);
	recursive_clean(s);
}
UT_TEST(recursive_ERROR_cleans_scope_once)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out; Snapshot actual; SCN floor; const char *reason;
	auth_throw_describe = true;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), -1);
	mx_history_recursive_close(); UT_ASSERT_EQ(caller_inflight, 0);
	UT_ASSERT(!cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
	mx_history_recursive_close(); UT_ASSERT_EQ(caller_leaves, 1); auth_clean(s);
}
UT_TEST(recursive_edge_keeps_complete_B_and_is_consumed_once)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out;
	ClusterHeapSuccessorProof next; volatile bool rejected = false;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
	PG_TRY(); {
		UT_ASSERT(mx_history_recursive_next(&tuple, &next));
		UT_ASSERT_EQ(next.updater_xid, 96); UT_ASSERT_EQ(next.origin_node_id, 0);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&next.tid), 3);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&next.tid), 2);
		UT_ASSERT(memcmp(&next, &out.successor, sizeof(next)) == 0);
	}
	PG_CATCH(); { rejected = true; }
	PG_END_TRY(); UT_ASSERT(!rejected); rejected = false;
	PG_TRY(); { (void)mx_history_recursive_next(&tuple, &next); }
	PG_CATCH(); { rejected = true; }
	PG_END_TRY(); UT_ASSERT(rejected); UT_ASSERT(!next.valid);
	recursive_clean(s);
}
UT_TEST(recursive_edge_never_reduces_mixed_B_to_a_raw_updater)
{
	for (unsigned fault = 1; fault <= 7; ++fault) {
		Snapshot s = recursive_setup(true); MxHeapReadout out;
		ClusterHeapSuccessorProof next; volatile bool rejected = false;
		UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
		mx_history_lock_successor_fault(fault);
		PG_TRY(); { (void)mx_history_recursive_next(&tuple, &next); }
		PG_CATCH(); { rejected = true; }
		PG_END_TRY(); UT_ASSERT(rejected); UT_ASSERT(!next.valid);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		recursive_clean(s);
	}
}
UT_TEST(recursive_physical_miss_cannot_discard_an_expected_proof)
{
	Snapshot s = recursive_setup(true); MxHeapReadout out; volatile bool rejected = false;
	UT_ASSERT_EQ(recursive_call(true, LockTupleKeyShare, &out), 2);
	PG_TRY(); { mx_history_recursive_missing(&out.successor); }
	PG_CATCH(); { rejected = true; }
	PG_END_TRY(); UT_ASSERT(rejected);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	recursive_clean(s);
}
UT_TEST(recursive_header_matches_LOCK_UPDATED_and_retains_forward_link)
{
	Snapshot s = recursive_setup(true);
	uint8 bytes[SizeofHeapTupleHeader] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	uint8 expected[SizeofHeapTupleHeader] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	HeapTupleHeader plan = (HeapTupleHeader)expected;
	bool planned;

	tuple.t_data->t_infomask2 |= HEAP_HOT_UPDATED;
	tuple.t_data->t_itl_slot_idx = 2;
	memcpy(expected, tuple.t_data, sizeof(expected));
	HeapTupleHeaderSetXmax(plan, 193);
	plan->t_infomask &= ~HEAP_XMAX_BITS;
	plan->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	plan->t_infomask |= HEAP_XMAX_IS_MULTI | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
	planned = mx_history_recursive_header(&tuple, 193,
		HEAP_XMAX_IS_MULTI | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY, 0, bytes);
	UT_ASSERT(planned);
	if (planned) {
	UT_ASSERT(memcmp(bytes, expected, sizeof(bytes)) == 0);
	UT_ASSERT(HeapTupleHeaderIsHotUpdated((HeapTupleHeader)bytes));
	UT_ASSERT_EQ(((HeapTupleHeader)bytes)->t_itl_slot_idx, 2);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&((HeapTupleHeader)bytes)->t_ctid), 2);
	}
	/* Unsupported legacy MOVED flags cannot make writer and redo diverge. */
	tuple.t_data->t_infomask |= HEAP_MOVED_OFF;
	UT_ASSERT(!mx_history_recursive_header(&tuple, 193, HEAP_XMAX_IS_MULTI, 0, bytes));
	recursive_clean(s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(11);
	UT_RUN(recursive_history_hop_returns_full_B_and_normalized_receipts);
	UT_RUN(recursive_two_root_calls_share_one_snapshot_and_admission);
	UT_RUN(recursive_TX_restart_keeps_scope_and_discards_old_decision);
	UT_RUN(recursive_closed_scope_never_reenters_or_dispatches);
	UT_RUN(recursive_legacy_guard_keeps_raw_members_unreachable);
	UT_RUN(recursive_incomplete_descriptor_cannot_select_legacy);
	UT_RUN(recursive_ERROR_cleans_scope_once);
	UT_RUN(recursive_edge_keeps_complete_B_and_is_consumed_once);
	UT_RUN(recursive_edge_never_reduces_mixed_B_to_a_raw_updater);
	UT_RUN(recursive_physical_miss_cannot_discard_an_expected_proof);
	UT_RUN(recursive_header_matches_LOCK_UPDATED_and_retains_forward_link);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
