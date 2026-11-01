/* Actual dormant FULL companion current-MX consumer; visibility, admission,
 * CR transport and descriptor/history storage leaves are controlled.
 * No fabricated FULL page or native SQL/GCS claim.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_FULL_FIXTURE
#define TransactionIdIsCurrentTransactionId full_fixture_forbidden_own_xid
#include "test_cluster_mx_history_native_walk.c"
#undef main
#undef TransactionIdIsCurrentTransactionId
#undef HISTORY_FULL_FIXTURE
#include "cluster/cluster_gcs_block.h"
#include "../../backend/access/heap/heapam_r4_private.h"
bool TransactionIdIsCurrentTransactionId(TransactionId xid);

static unsigned full_cr_calls;
static int full_cr_mode;
bool cluster_trace_normal_stop = false;
bool TransactionIdIsCurrentTransactionId(TransactionId xid) { return false; }
bool
cluster_heap_tuple_satisfies_visibility_waitable(HeapTuple t, Snapshot s, Buffer b,
	bool *wait, ClusterTxLocator *locator)
{
	*wait = false; memset(locator, 0, sizeof(*locator));
	return HeapTupleSatisfiesVisibility(t, s, b);
}
ClusterCrBuildResult
cluster_gcs_block_cr_fetch_projection(BufferTag tag, SCN scn, char dst[BLCKSZ],
	ClusterR4ProjectionScratch *projection, ClusterCrBuildReason *reason)
{
	UT_ASSERT(!content_locked); UT_ASSERT_EQ(tag.blockNum, 3);
	UT_ASSERT_EQ(scn, walk_snapshot->read_scn); full_cr_calls++;
	UT_ASSERT(full_cr_mode != 0); /* Ordinary live tests must not enter CR. */
	memset(projection, 0xa5, sizeof(*projection));
	if (full_cr_mode == 3)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("CR cancel")));
	*reason = full_cr_mode == 1 ? CLUSTER_CR_BUILD_TARGET_DISABLED : CLUSTER_CR_BUILD_PROTOCOL;
	return full_cr_mode == 1 ? CLUSTER_CR_BUILD_RETRYABLE : CLUSTER_CR_BUILD_FAIL_CLOSED;
}
/* Buffer tag and diagnostics are leaf projections, not authority. Scratch
 * visibility is deliberately forbidden: no positive FULL result is supplied. */
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1); *locator = relation.rd_locator;
	*fork = MAIN_FORKNUM; *block = 3;
}
bool HeapTupleSatisfiesMVCCScratch(HeapTuple t, Snapshot s,
	const ClusterR4HotScratchTestContext *context) { abort(); }
void cluster_catalog_stats_vis_unknown_inc(void) { abort(); }
const char *cluster_cr_build_reason_name(ClusterCrBuildReason reason) { return "controlled"; }
void cluster_ctrc_test_barrier_wait(ClusterCtrcTestBarrierPhase phase) { }
bool
cluster_heap_r4_trace_format(const ClusterR4ScratchTrace *trace, char *out, Size size)
{
	if (size) out[0] = 0;
	return true;
}
static Snapshot
full_setup(bool scur)
{
	Snapshot s = walk_setup(scur);
	/* CR eligibility uses the mirror; actual current-MX proof scans exact ITL
	 * roots, independently of this stale/unallocated mirror. */
	tuple.t_data->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	hot_candidate().t_data->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	full_cr_calls = 0; full_cr_mode = 0; return s;
}
static int
full_call(Snapshot actual, bool first, bool *dead, ItemPointer tid, MxFullReadout *out)
{
	volatile int result = -1; error_code = 0; walk_snapshot = actual;
	PG_TRY(); { result = mx_history_full_companion(&relation, 1, actual, tid, first, dead, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
UT_TEST(full_live_history_proof_precedes_visibility_and_owned_copy)
{
	for (unsigned scur = 0; scur < 2; ++scur) {
		Snapshot s = full_setup(scur); ItemPointerData tid = tuple.t_self; MxFullReadout out;
		bool dead = true;
		UT_ASSERT_EQ(full_call(s, true, &dead, &tid, &out), 3);
		if (out.kind != 3) { auth_clean(s); return; }
		UT_ASSERT_EQ(out.kind, 3); UT_ASSERT(out.owned); UT_ASSERT(!dead);
		UT_ASSERT(!out.cr_full_page && !out.cr_private_projection); UT_ASSERT(out.projection_empty);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&tid), 2); UT_ASSERT(ItemPointerEquals(&tid, &out.tid));
		UT_ASSERT_EQ(out.xmin, 96); HeapTupleHeaderSetXmin(hot_candidate().t_data, 112);
		UT_ASSERT_EQ(mx_history_full_owned_xmin(), 96);
		UT_ASSERT_EQ(walk_visible[1], 1); UT_ASSERT_EQ(walk_visible[2], 1);
		UT_ASSERT_EQ(walk_conflicts, 2); UT_ASSERT_EQ(walk_predicates, 1);
		UT_ASSERT_EQ(auth_describes, 1); UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(full_cr_calls, 0);
		printf("# operation-state-bytes=%zu\n", out.operation_bytes);
		walk_balanced(false); auth_clean(s);
	}
}
UT_TEST(full_history_restarts_original_root_same_snapshot)
{
	Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
	auth_descriptor_fault = 1;
	UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), 3);
	UT_ASSERT_EQ(auth_describes, 2); UT_ASSERT_EQ(walk_visible[1], 2);
	UT_ASSERT_EQ(walk_visible[2], 1); UT_ASSERT_EQ(caller_enters, 2);
	UT_ASSERT_EQ(caller_leaves, 2); UT_ASSERT_EQ(caller_inflight, 0);
	UT_ASSERT_EQ(remembered, 1); UT_ASSERT_EQ(full_cr_calls, 0); auth_clean(s);
}
UT_TEST(full_history_successor_ABA_fails_before_visibility)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
		walk_late_aba = fault;
		UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(out.kind, 0); UT_ASSERT(!out.owned);
		UT_ASSERT_EQ(walk_visible[2], 0); UT_ASSERT_EQ(walk_predicates, 0);
		read_balanced(); auth_clean(s);
	}
}
UT_TEST(full_history_rejects_unretained_scope_or_closed_admission)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = full_setup(true), input = s; SnapshotData forged = *s;
		ItemPointerData tid = tuple.t_self; MxFullReadout out;
		if (fault == 0) input = &forged;
		if (fault == 1) s->read_epoch++;
		if (fault == 2) caller_closed = true;
		UT_ASSERT_EQ(full_call(input, true, NULL, &tid, &out), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(walk_visible[2], 0);
		UT_ASSERT_EQ(caller_inflight, 0); auth_clean(s);
	}
}
UT_TEST(full_history_ERROR_restores_outer_evaluator_and_admission)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		Snapshot s = full_setup(true), actual; SCN floor; const char *reason;
		ClusterSnapshotReadScopeV1 outer; ItemPointerData tid = tuple.t_self; MxFullReadout out;
		cluster_snapshot_read_enter_v1(&outer, s); auth_throw_describe = fault == 0;
		if (fault == 1) { finish_pending = true; history_wait_fault = 1; }
		UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), -1);
		read_balanced(); UT_ASSERT_EQ(walk_visible[2], 0);
		UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
		UT_ASSERT(actual == s); cluster_snapshot_read_exit_v1(&outer); auth_clean(s);
	}
}
UT_TEST(full_history_rejects_noncommitted_or_discarded_updater)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
		if (fault < 2) finish_outcome = fault ? CLUSTER_TX_ABORTED : CLUSTER_TX_IN_PROGRESS;
		if (fault == 2) { finish_native_top_records = false; finish_all_discarded = true; }
		UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(walk_visible[2], 0); UT_ASSERT_EQ(auth_waits, 0);
		read_balanced(); auth_clean(s);
	}
}
UT_TEST(full_history_preserves_skip_and_not_found_contract)
{
	for (unsigned missing = 0; missing < 2; ++missing) {
		Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
		bool dead = true; walk_next_visible = !missing;
		UT_ASSERT_EQ(full_call(s, false, &dead, &tid, &out), missing ? 0 : 3);
		UT_ASSERT_EQ(walk_visible[1], 0); UT_ASSERT_EQ(walk_visible[2], 1); UT_ASSERT(!dead);
		UT_ASSERT_EQ(walk_predicates, missing ? 0 : 1);
		if (missing) { UT_ASSERT(ItemPointerEquals(&tid, &tuple.t_self)); UT_ASSERT(!out.owned); }
		walk_balanced(false); auth_clean(s);
	}
}
UT_TEST(full_original_CR_dormant_relocks_then_uses_history)
{
	Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
	HeapTupleHeaderSetXmin(tuple.t_data, 64);
	tuple.t_data->t_itl_slot_idx = 0; full_cr_mode = 1;
	UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), 3);
	UT_ASSERT_EQ(full_cr_calls, 1); UT_ASSERT_EQ(auth_describes, 1);
	UT_ASSERT(!out.cr_full_page && !out.cr_private_projection); UT_ASSERT(out.projection_empty);
	UT_ASSERT(out.owned); UT_ASSERT_EQ(walk_visible[2], 1);
	walk_balanced(false); auth_clean(s);
}
UT_TEST(full_original_CR_UNKNOWN_or_ERROR_does_not_fall_back)
{
	for (unsigned fault = 2; fault <= 3; ++fault) {
		Snapshot s = full_setup(true); ItemPointerData tid = tuple.t_self; MxFullReadout out;
		HeapTupleHeaderSetXmin(tuple.t_data, 64);
		tuple.t_data->t_itl_slot_idx = 0; full_cr_mode = fault;
		UT_ASSERT_EQ(full_call(s, true, NULL, &tid, &out), -1);
		UT_ASSERT_EQ(error_code, fault == 2 ? ERRCODE_CLUSTER_TT_STATUS_UNKNOWN : ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(full_cr_calls, 1); UT_ASSERT_EQ(auth_describes, 0);
		UT_ASSERT_EQ(walk_visible[1], 0); UT_ASSERT_EQ(walk_visible[2], 0);
		UT_ASSERT(content_locked && !exclusive); UT_ASSERT_EQ(caller_inflight, 0);
		UT_ASSERT(!out.cr_full_page && !out.cr_private_projection); UT_ASSERT(out.projection_empty); auth_clean(s);
	}
}
int main(void)
{
	history_fixture_init(); UT_PLAN(9);
	UT_RUN(full_live_history_proof_precedes_visibility_and_owned_copy);
	UT_RUN(full_history_restarts_original_root_same_snapshot);
	UT_RUN(full_history_successor_ABA_fails_before_visibility);
	UT_RUN(full_history_rejects_unretained_scope_or_closed_admission);
	UT_RUN(full_history_ERROR_restores_outer_evaluator_and_admission);
	UT_RUN(full_history_rejects_noncommitted_or_discarded_updater);
	UT_RUN(full_history_preserves_skip_and_not_found_contract);
	UT_RUN(full_original_CR_dormant_relocks_then_uses_history);
	UT_RUN(full_original_CR_UNKNOWN_or_ERROR_does_not_fall_back);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
