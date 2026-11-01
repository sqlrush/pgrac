/* Native tuple-lock scope and root requalification helpers. Snapshot, collector,
 * typed result, operation lifecycle and full fingerprint checks execute. The
 * unlocked follow work and admission acquisition are controlled leaves; this
 * does not execute the whole native root, recursive lock, TableAM or SQL.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_WALKER_FIXTURE
#include "test_cluster_mx_history_hot_caller.c"
#undef main
#undef HISTORY_WALKER_FIXTURE

static Snapshot
window_setup(void)
{
	Snapshot s = read_setup(false);
	finish_outcome = CLUSTER_TX_IN_PROGRESS;
	exclusive = true; bridge_lock_override = auth_lock;
	return s;
}
static int
window_open(Snapshot actual, MxHeapReadout *out)
{
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_lock_window_open(&relation, 1, &tuple, actual, &ctx, out); }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static int
window_check(bool after)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); {
		if (after) result = mx_history_lock_window_after(1, &tuple) ? 1 : 0;
		else { mx_history_lock_window_check(); result = 1; }
	}
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static void
window_close(Snapshot s)
{
	mx_history_lock_window_close();
	UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(caller_enters, caller_leaves);
	auth_finished(); auth_clean(s);
}
UT_TEST(window_keeps_original_authority_until_native_return)
{
	Snapshot s = window_setup(), actual; MxHeapReadout out; SCN floor; const char *reason;
	UT_ASSERT_EQ(window_open(s, &out), 2);
	UT_ASSERT(out.follow_updated_chain && out.successor.valid);
	UT_ASSERT_EQ(out.authoritative_updater, 96);
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_inflight, 1); UT_ASSERT_EQ(caller_leaves, 0);
	UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason)); UT_ASSERT(actual == s);
	mx_history_lock_window_before(1, &tuple);
	auth_lock(1, BUFFER_LOCK_UNLOCK); /* Controlled native follow work. */
	UT_ASSERT_EQ(caller_inflight, 1); auth_lock(1, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT_EQ(window_check(true), 1); UT_ASSERT_EQ(window_check(false), 1);
	UT_ASSERT_EQ(remembered, 1); window_close(s);
}
UT_TEST(window_rechecks_entire_root_not_only_xmax_and_ctid)
{
	for (unsigned fault = 0; fault < 8; ++fault) {
		Snapshot s = window_setup(); MxHeapReadout out; MxAuthorizeState before, after;
		UT_ASSERT_EQ(window_open(s, &out), 2); mx_history_authorize_state(&before);
		mx_history_lock_window_before(1, &tuple); auth_lock(1, BUFFER_LOCK_UNLOCK);
		switch (fault) {
		case 0: tuple.t_data->t_infomask2++; break;
		case 1: HeapTupleHeaderSetXmin(tuple.t_data, 65); break;
		case 2: PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(4321)); break;
		case 3: pcm_source.generation++; break;
		case 4: ClusterPageGetItlSlots(BufferBlocks)[7].write_scn++; break;
		case 5: ItemIdSetNormal(&((PageHeader)BufferBlocks)->pd_linp[0],
			ItemIdGetOffset(&((PageHeader)BufferBlocks)->pd_linp[0]), SizeofHeapTupleHeader + 1); break;
		case 6: ItemPointerSetOffsetNumber(&tuple.t_self, 2); break;
		case 7: tuple.t_data->t_hoff++; break;
		}
		auth_lock(1, BUFFER_LOCK_EXCLUSIVE);
		UT_ASSERT_EQ(window_check(true), 0); mx_history_authorize_state(&after);
		UT_ASSERT_EQ(after.operation_id, before.operation_id); UT_ASSERT(after.active && !after.finished);
		UT_ASSERT_EQ(after.restarts, before.restarts + 1); UT_ASSERT(content_locked);
		window_close(s);
	}
}
UT_TEST(window_snapshot_owner_and_admission_cannot_drift_after_follow)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		Snapshot s = window_setup(); MxHeapReadout out;
		UT_ASSERT_EQ(window_open(s, &out), 2); mx_history_lock_window_before(1, &tuple);
		auth_lock(1, BUFFER_LOCK_UNLOCK);
		if (fault == 0) s->read_epoch++;
		if (fault == 1) s->read_scn++;
		if (fault == 2) CurrentResourceOwner = (ResourceOwner)2;
		if (fault == 3) pg_atomic_write_u32(&history_gate.transition_closed, 1);
		auth_lock(1, BUFFER_LOCK_EXCLUSIVE);
		UT_ASSERT_EQ(window_check(true), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		CurrentResourceOwner = (ResourceOwner)1; window_close(s);
	}
}
UT_TEST(window_checks_scope_again_before_receipt_mutation)
{
	Snapshot s = window_setup(); MxHeapReadout out;
	UT_ASSERT_EQ(window_open(s, &out), 2);
	pg_atomic_write_u32(&history_gate.transition_closed, 1);
	UT_ASSERT_EQ(window_check(false), -1);
	UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT); window_close(s);
}
UT_TEST(window_rejects_missing_or_mixed_follow_proof_before_unlock)
{
	for (unsigned fault = 1; fault <= 7; ++fault) {
		Snapshot s = window_setup(); MxHeapReadout out; volatile bool rejected = false;
		UT_ASSERT_EQ(window_open(s, &out), 2); mx_history_lock_successor_fault(fault);
		PG_TRY(); { mx_history_lock_window_before(1, &tuple); }
		PG_CATCH(); { rejected = true; }
		PG_END_TRY(); UT_ASSERT(rejected); UT_ASSERT(content_locked);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT); window_close(s);
	}
}
UT_TEST(window_ERROR_restores_outer_evaluation_and_releases_once)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		Snapshot s = window_setup(), actual; MxHeapReadout out;
		ClusterSnapshotReadScopeV1 outer; SCN floor; const char *reason;
		cluster_snapshot_read_enter_v1(&outer, s); auth_throw_describe = fault == 0;
		if (fault == 1) { finish_pending = true; history_wait_fault = 1; }
		UT_ASSERT_EQ(window_open(s, &out), -1);
		mx_history_lock_window_close();
		UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 1);
		UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason)); UT_ASSERT(actual == s);
		cluster_snapshot_read_exit_v1(&outer); window_close(s);
	}
}
UT_TEST(window_implicit_snapshot_is_active_and_reference_stays_callers)
{
	Snapshot s = window_setup(); MxHeapReadout out;
	PushActiveSnapshot(s); UT_ASSERT_EQ(window_open(NULL, &out), 2);
	UT_ASSERT_EQ(caller_inflight, 1); UT_ASSERT(GetActiveSnapshot() == s);
	mx_history_lock_window_close(); UT_ASSERT(GetActiveSnapshot() == s);
	PopActiveSnapshot(); window_close(s);
}
UT_TEST(window_closed_or_unretained_never_dispatches)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		Snapshot s = window_setup(); MxHeapReadout out;
		if (fault == 1) caller_closed = true;
		UT_ASSERT_EQ(window_open(fault ? s : NULL, &out), -1);
		UT_ASSERT_EQ(auth_describes, 0); UT_ASSERT_EQ(caller_inflight, 0);
		mx_history_lock_window_close(); auth_clean(s);
	}
}
int main(void)
{
	history_fixture_init(); UT_PLAN(8);
	UT_RUN(window_keeps_original_authority_until_native_return);
	UT_RUN(window_rechecks_entire_root_not_only_xmax_and_ctid);
	UT_RUN(window_snapshot_owner_and_admission_cannot_drift_after_follow);
	UT_RUN(window_checks_scope_again_before_receipt_mutation);
	UT_RUN(window_rejects_missing_or_mixed_follow_proof_before_unlock);
	UT_RUN(window_ERROR_restores_outer_evaluation_and_releases_once);
	UT_RUN(window_implicit_snapshot_is_active_and_reference_stays_callers);
	UT_RUN(window_closed_or_unretained_never_dispatches);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
