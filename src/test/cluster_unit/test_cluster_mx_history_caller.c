/* Caller Snapshot selection and scoped admission around real writer history.
 * Admission acquisition/release outcomes and transport/storage remain leaves;
 * native Snapshot membership, recheck policy, collector and writer are real.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_CALLER_FIXTURE
#include "test_cluster_mx_history_authorize.c"
#undef main
#undef HISTORY_CALLER_FIXTURE

static unsigned caller_enters, caller_leaves, caller_inflight;
static bool caller_closed;
ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
	ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	caller_enters++;
	memset(token, 0, sizeof(*token));
	if (caller_closed) return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	*token = admission; caller_inflight++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token->entered); UT_ASSERT_EQ(caller_inflight, 1);
	UT_ASSERT_EQ(token->record_generation, admission.record_generation);
	caller_leaves++; caller_inflight--;
	memset(token, 0, sizeof(*token));
}
static Snapshot
caller_setup(void)
{
	Snapshot s = auth_setup(CLUSTER_TX_IN_PROGRESS, false);
	caller_enters = caller_leaves = caller_inflight = 0; caller_closed = false;
	return s;
}
static int
caller_call(Snapshot actual, ClusterCurrentMxRequestContext *ctx, MxHeapReadout *out)
{
	volatile int result = -1;
	error_code = 0;
	PG_TRY(); { result = mx_history_authorize_caller(&relation, 1, &tuple, actual, ctx, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY();
	return result;
}
static void
caller_balanced(void)
{
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 1);
	UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(remembered, 1);
}
UT_TEST(caller_explicit_registered_snapshot_reaches_writer)
{
	Snapshot s = caller_setup(); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT_EQ(out.retained.proof_count, 1);
	UT_ASSERT_EQ(auth_describes, 1); caller_balanced(); auth_finished(); auth_clean(s);
}
UT_TEST(caller_implicit_snapshot_is_actual_active_snapshot)
{
	Snapshot s = caller_setup(); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	PushActiveSnapshot(s); UT_ASSERT(GetActiveSnapshot() == s);
	UT_ASSERT_EQ(caller_call(NULL, &ctx, &out), 2);
	UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT(GetActiveSnapshot() == s);
	caller_balanced(); PopActiveSnapshot(); auth_clean(s);
}
UT_TEST(caller_registered_list_is_not_an_implicit_snapshot)
{
	Snapshot s = caller_setup(); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT_EQ(caller_call(NULL, &ctx, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(caller_enters, 0); UT_ASSERT_EQ(caller_leaves, 0);
	UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(finish_requests, 0);
	bridge_no_authority(&out); auth_clean(s);
}
UT_TEST(caller_invalid_snapshot_cannot_acquire_authority)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		Snapshot s = caller_setup(); SnapshotData forged = *s; MxHeapReadout out;
		Snapshot input = s; ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		if (fault == 0) { forged.active_count = forged.regd_count = 999; input = &forged; }
		if (fault == 1) s->snapshot_type = SNAPSHOT_DIRTY;
		if (fault == 2) s->read_epoch++;
		if (fault == 3) s->cluster_source = SNAPSHOT_SOURCE_LOCAL;
		UT_ASSERT_EQ(caller_call(input, &ctx, &out), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(caller_enters, 0); UT_ASSERT_EQ(auth_describes, 0);
		UT_ASSERT_EQ(finish_requests, 0); bridge_no_authority(&out); auth_clean(s);
	}
}
UT_TEST(caller_closed_admission_never_dispatches)
{
	Snapshot s = caller_setup(); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	caller_closed = true;
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 0);
	UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(auth_describes, 0);
	bridge_no_authority(&out); auth_clean(s);
}
UT_TEST(caller_ERROR_restores_original_evaluation_scope)
{
	for (unsigned failure = 0; failure < 3; ++failure) {
		Snapshot s = caller_setup(); MxHeapReadout out;
		ClusterSnapshotReadScopeV1 outer; Snapshot actual; SCN floor; const char *reason;
		ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
		cluster_snapshot_read_enter_v1(&outer, s);
		ctx.lock_mode = LockTupleExclusive;
		auth_throw_describe = failure == 0; auth_throw_wait = failure == 2;
		if (failure == 1) { finish_pending = true; history_wait_fault = 1; }
		UT_ASSERT_EQ(caller_call(s, &ctx, &out), -1);
		caller_balanced(); bridge_no_authority(&out); auth_finished();
		UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
		UT_ASSERT(actual == s); cluster_snapshot_read_exit_v1(&outer); auth_clean(s);
	}
}
UT_TEST(caller_restart_releases_admission_and_keeps_operation)
{
	Snapshot s = caller_setup(); MxHeapReadout out; MxAuthorizeState before, after;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	ctx.lock_mode = LockTupleExclusive;
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), 1);
	caller_balanced(); mx_history_authorize_state(&before);
	UT_ASSERT(before.active && !before.finished); UT_ASSERT_EQ(before.restarts, 1);
	finish_outcome = CLUSTER_TX_COMMITTED;
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), 2);
	mx_history_authorize_state(&after); UT_ASSERT_EQ(after.operation_id, before.operation_id);
	UT_ASSERT_EQ(caller_enters, 2); UT_ASSERT_EQ(caller_leaves, 2);
	UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(out.result, TM_Ok); auth_finished(); auth_clean(s);
}
UT_TEST(caller_follow_preserves_operation_but_releases_scope)
{
	Snapshot s = caller_setup(); MxHeapReadout out; MxAuthorizeState state;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	PageHeader ph = (PageHeader)BufferBlocks; ItemId lp = &ph->pd_linp[1];
	HeapTupleHeader candidate;
	ph->pd_lower += sizeof(ItemIdData); ph->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(lp, ph->pd_upper, SizeofHeapTupleHeader);
	candidate = (HeapTupleHeader)PageGetItem(BufferBlocks, lp);
	memset(candidate, 0, SizeofHeapTupleHeader); candidate->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(candidate, 96); ItemPointerSet(&tuple.t_data->t_ctid, 3, 2);
	tuple.t_data->t_infomask &= ~HEAP_XMAX_LOCK_ONLY;
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	finish_top = 96; finish_native_top_records = true;
	ClusterPageGetItlSlots(BufferBlocks)[0].xid = 96;
	ClusterPageGetItlSlots(BufferBlocks)[1].xid = 96;
	ClusterPageGetItlSlots(BufferBlocks)[2] = (ClusterItlSlotData){ .xid = 96,
		.flags = ITL_FLAG_ACTIVE, .wrap = 1, .undo_segment_head = uba_encode(1, 6, 3, 0), .write_scn = 79 };
	finish_successor_in_graph = finish_same_page_successor = true;
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), 2);
	caller_balanced(); mx_history_authorize_state(&state);
	UT_ASSERT(state.active && !state.finished);
	UT_ASSERT(out.follow_updated_chain && out.successor.valid);
	auth_clean(s);
}
static void
caller_closing_lock(Buffer buffer, int mode)
{
	auth_lock(buffer, mode);
	if (mode == BUFFER_LOCK_EXCLUSIVE && bridge_relocks == 2)
		pg_atomic_write_u32(&history_gate.transition_closed, 1);
}
UT_TEST(caller_requalification_cannot_export_closed_scope)
{
	Snapshot s = caller_setup(); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(false);
	bridge_lock_override = caller_closing_lock;
	UT_ASSERT_EQ(caller_call(s, &ctx, &out), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
	caller_balanced(); bridge_no_authority(&out); auth_finished();
	auth_clean(s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(9);
	UT_RUN(caller_explicit_registered_snapshot_reaches_writer);
	UT_RUN(caller_implicit_snapshot_is_actual_active_snapshot);
	UT_RUN(caller_registered_list_is_not_an_implicit_snapshot);
	UT_RUN(caller_invalid_snapshot_cannot_acquire_authority);
	UT_RUN(caller_closed_admission_never_dispatches);
	UT_RUN(caller_ERROR_restores_original_evaluation_scope);
	UT_RUN(caller_restart_releases_admission_and_keeps_operation);
	UT_RUN(caller_follow_preserves_operation_but_releases_scope);
	UT_RUN(caller_requalification_cannot_export_closed_scope);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
