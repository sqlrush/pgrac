/* Typed history -> original tuple-lock failure channel. Actual heap bodies
 * and identity checks, controlled storage/transport and admission leaves.
 * No native root/recursive lock execution or SQL claim.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_WALKER_FIXTURE
#include "test_cluster_mx_history_hot_caller.c"
#undef main
#undef HISTORY_WALKER_FIXTURE

static Snapshot
lock_setup(ClusterTxOutcome outcome)
{
	Snapshot s = read_setup(false);
	finish_outcome = outcome;
	exclusive = true; bridge_lock_override = auth_lock;
	return s;
}
static int
lock_produce(Snapshot actual, LockWaitPolicy wait, MxHeapReadout *out)
{
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	volatile int result = -1;
	ctx.follow_updates = false; ctx.lock_mode = LockTupleExclusive;
	ctx.wait_policy = wait; error_code = 0;
	PG_TRY(); { result = mx_history_authorize_caller(&relation, 1, &tuple, actual, &ctx, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static int
lock_failure(TM_Result result, bool handled, TM_FailureData *failure, ClusterHeapSuccessorProof *next)
{
	volatile int ok = -1; error_code = 0;
	PG_TRY(); { mx_history_lock_failure(&tuple, result, handled, failure, next); ok = 1; }
	PG_CATCH(); { ok = -1; }
	PG_END_TRY(); return ok;
}
static void
failure_seed(TM_FailureData *failure, ClusterHeapSuccessorProof *next)
{
	memset(failure, 0xa5, sizeof(*failure)); failure->traversed = true;
	memset(next, 0xa5, sizeof(*next));
}
static void
failure_empty(const TM_FailureData *failure, const ClusterHeapSuccessorProof *next)
{
	ClusterHeapSuccessorProof zero = {0};
	UT_ASSERT(!ItemPointerIsValid(&failure->ctid));
	UT_ASSERT_EQ(failure->xmax, InvalidTransactionId);
	UT_ASSERT_EQ(failure->cmax, InvalidCommandId);
	UT_ASSERT(failure->traversed);
	UT_ASSERT(memcmp(next, &zero, sizeof(zero)) == 0);
}
UT_TEST(lock_updated_hands_exact_history_successor_to_native_channel)
{
	Snapshot s = lock_setup(CLUSTER_TX_COMMITTED); MxHeapReadout out;
	TM_FailureData failure; ClusterHeapSuccessorProof next;
	UT_ASSERT_EQ(lock_produce(s, LockWaitBlock, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Updated); UT_ASSERT(out.successor.valid);
	failure_seed(&failure, &next);
	UT_ASSERT_EQ(lock_failure(out.result, true, &failure, &next), 1);
	UT_ASSERT_EQ(failure.xmax, 96); UT_ASSERT_EQ(failure.cmax, InvalidCommandId);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&failure.ctid), 3);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&failure.ctid), 2); UT_ASSERT(failure.traversed);
	UT_ASSERT(next.valid); UT_ASSERT_EQ(next.updater_xid, 96);
	UT_ASSERT_EQ(next.origin_node_id, 0); UT_ASSERT_EQ(next.cluster_epoch, 7);
	UT_ASSERT(ItemPointerEquals(&failure.ctid, &next.tid));
	UT_ASSERT(memcmp(&next, &out.successor, sizeof(next)) == 0);
	UT_ASSERT_EQ(auth_describes, 1); UT_ASSERT_EQ(finish_reads, 3);
	read_balanced(); auth_finished(); auth_clean(s);
}
UT_TEST(lock_updated_missing_or_mixed_successor_never_exports_raw_fallback)
{
	for (unsigned fault = 1; fault <= 7; ++fault) {
		Snapshot s = lock_setup(CLUSTER_TX_COMMITTED); MxHeapReadout out;
		TM_FailureData failure; ClusterHeapSuccessorProof next;
		UT_ASSERT_EQ(lock_produce(s, LockWaitBlock, &out), 2);
		mx_history_lock_successor_fault(fault);
		if (fault == 6) ItemPointerSet(&tuple.t_data->t_ctid, 3, 1);
		failure_seed(&failure, &next);
		UT_ASSERT_EQ(lock_failure(TM_Updated, true, &failure, &next), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		failure_empty(&failure, &next); read_balanced(); auth_finished(); auth_clean(s);
	}
}
UT_TEST(lock_updated_changed_root_edge_rejects_previous_proof)
{
	Snapshot s = lock_setup(CLUSTER_TX_COMMITTED); MxHeapReadout out;
	TM_FailureData failure; ClusterHeapSuccessorProof next;
	UT_ASSERT_EQ(lock_produce(s, LockWaitBlock, &out), 2);
	ItemPointerSet(&tuple.t_data->t_ctid, 3, 1); failure_seed(&failure, &next);
	UT_ASSERT_EQ(lock_failure(TM_Updated, true, &failure, &next), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	failure_empty(&failure, &next); auth_clean(s);
}
UT_TEST(lock_updated_requires_B_even_without_output_pointer)
{
	for (unsigned missing = 0; missing < 2; ++missing) {
		Snapshot s = lock_setup(CLUSTER_TX_COMMITTED); MxHeapReadout out;
		TM_FailureData failure; ClusterHeapSuccessorProof next;
		UT_ASSERT_EQ(lock_produce(s, LockWaitBlock, &out), 2);
		if (missing) mx_history_lock_successor_fault(1);
		failure_seed(&failure, &next);
		UT_ASSERT_EQ(lock_failure(TM_Updated, true, &failure, NULL), missing ? -1 : 1);
		if (missing) {
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(failure.xmax, InvalidTransactionId);
		} else UT_ASSERT_EQ(failure.xmax, 96);
		auth_clean(s);
	}
}
UT_TEST(lock_deleted_keeps_typed_updater_without_inventing_successor)
{
	Snapshot s = lock_setup(CLUSTER_TX_COMMITTED); MxHeapReadout out;
	TM_FailureData failure; ClusterHeapSuccessorProof next;
	ItemPointerSet(&tuple.t_data->t_ctid, 3, 1);
	UT_ASSERT_EQ(lock_produce(s, LockWaitBlock, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Deleted); UT_ASSERT(!out.successor.valid);
	failure_seed(&failure, &next); memset(&next, 0, sizeof(next));
	UT_ASSERT_EQ(lock_failure(out.result, true, &failure, &next), 1);
	UT_ASSERT_EQ(failure.xmax, 96); UT_ASSERT_EQ(failure.cmax, InvalidCommandId);
	UT_ASSERT(ItemPointerEquals(&failure.ctid, &tuple.t_self)); UT_ASSERT(!next.valid);
	UT_ASSERT(failure.traversed); auth_clean(s);
}
UT_TEST(lock_would_block_preserves_original_native_failure_polarity)
{
	Snapshot s = lock_setup(CLUSTER_TX_IN_PROGRESS); MxHeapReadout out;
	TM_FailureData failure; ClusterHeapSuccessorProof next;
	UT_ASSERT_EQ(lock_produce(s, LockWaitSkip, &out), 2);
	UT_ASSERT_EQ(out.result, TM_WouldBlock); UT_ASSERT_EQ(auth_waits, 0);
	failure_seed(&failure, &next); memset(&next, 0, sizeof(next));
	UT_ASSERT_EQ(lock_failure(out.result, true, &failure, &next), 1);
	UT_ASSERT_EQ(failure.cmax, InvalidCommandId); UT_ASSERT(failure.traversed);
	UT_ASSERT(!next.valid); read_balanced(); auth_finished(); auth_clean(s);
}
UT_TEST(lock_unhandled_current_MX_never_reads_local_raw_members)
{
	Snapshot s = lock_setup(CLUSTER_TX_COMMITTED);
	TM_FailureData failure; ClusterHeapSuccessorProof next;
	failure_seed(&failure, &next); memset(&next, 0, sizeof(next));
	UT_ASSERT_EQ(lock_failure(TM_Updated, false, &failure, &next), 1);
	UT_ASSERT_EQ(failure.xmax, InvalidTransactionId); UT_ASSERT(!next.valid);
	UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(failure.cmax, InvalidCommandId);
	auth_clean(s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(7);
	UT_RUN(lock_updated_hands_exact_history_successor_to_native_channel);
	UT_RUN(lock_updated_missing_or_mixed_successor_never_exports_raw_fallback);
	UT_RUN(lock_updated_changed_root_edge_rejects_previous_proof);
	UT_RUN(lock_updated_requires_B_even_without_output_pointer);
	UT_RUN(lock_deleted_keeps_typed_updater_without_inventing_successor);
	UT_RUN(lock_would_block_preserves_original_native_failure_polarity);
	UT_RUN(lock_unhandled_current_MX_never_reads_local_raw_members);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
