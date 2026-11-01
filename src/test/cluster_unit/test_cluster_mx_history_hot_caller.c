/* Actual scoped HOT front doors and native successor matchers. Admission
 * enter/leave outcomes and descriptor/history storage/transport are controlled;
 * Snapshot membership, retention, recheck, collector and typed readers are real.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_HOT_CALLER_FIXTURE
#include "test_cluster_mx_history_hot.c"
#undef main
#undef HISTORY_HOT_CALLER_FIXTURE

static unsigned caller_enters, caller_leaves, caller_inflight;
static bool caller_closed;
ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
	ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	caller_enters++; memset(token, 0, sizeof(*token));
	if (caller_closed) return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	*token = admission; caller_inflight++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token->entered); UT_ASSERT_EQ(caller_inflight, 1);
	UT_ASSERT_EQ(token->record_generation, admission.record_generation);
	caller_leaves++; caller_inflight--; memset(token, 0, sizeof(*token));
}
static Snapshot
read_setup(bool scur)
{
	Snapshot s = hot_setup(CLUSTER_TX_COMMITTED, scur);
	caller_enters = caller_leaves = caller_inflight = 0; caller_closed = false;
	return s;
}
static int
read_call(Snapshot actual, bool full, MxHotReadout *out)
{
	volatile int result = -1; error_code = 0;
	PG_TRY(); { result = mx_history_hot_caller(&relation, 1, &tuple, actual, full, out) ? 1 : 0; }
	PG_CATCH(); { result = -1; }
	PG_END_TRY(); return result;
}
static void
read_balanced(void)
{
	UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 1);
	UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(remembered, 1);
}
UT_TEST(read_caller_registered_scope_reaches_both_native_consumers)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned scur = 0; scur < 2; ++scur) {
			Snapshot s = read_setup(scur); MxHotReadout out; HeapTupleData next = hot_candidate();
			finish_pending = true;
			UT_ASSERT_EQ(read_call(s, full, &out), 1);
			UT_ASSERT_EQ(out.updater, 96); UT_ASSERT(!out.restart && out.successor.valid);
			UT_ASSERT(mx_history_hot_matches(1, &next, full));
			UT_ASSERT(content_locked && !exclusive); UT_ASSERT_EQ(hot_promotions, 0);
			UT_ASSERT_EQ(auth_describes, 1); UT_ASSERT_EQ(auth_waits, 0);
			UT_ASSERT_EQ(finish_reads, 3); read_balanced(); auth_finished(); auth_clean(s);
		}
}
UT_TEST(read_caller_implicit_scope_is_only_current_active_snapshot)
{
	for (unsigned full = 0; full < 2; ++full) {
		Snapshot s = read_setup(true); MxHotReadout out;
		PushActiveSnapshot(s);
		UT_ASSERT_EQ(read_call(NULL, full, &out), 1);
		UT_ASSERT(GetActiveSnapshot() == s); UT_ASSERT(out.successor.valid);
		read_balanced(); PopActiveSnapshot(); auth_clean(s);
		s = read_setup(true); UT_ASSERT(!ActiveSnapshotSet());
		UT_ASSERT_EQ(read_call(NULL, full, &out), -1); hot_empty(&out);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(caller_enters, 0); UT_ASSERT_EQ(auth_describes, 0); auth_clean(s);
	}
}
UT_TEST(read_caller_invalid_snapshot_never_dispatches)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 6; ++fault) {
			Snapshot s = read_setup(true), input = s; SnapshotData forged = *s; MxHotReadout out;
			if (fault == 0) { forged.active_count = forged.regd_count = 999; input = &forged; }
			if (fault == 1) s->snapshot_type = SNAPSHOT_DIRTY;
			if (fault == 2) s->read_epoch++;
			if (fault == 3) s->cluster_source = SNAPSHOT_SOURCE_LOCAL;
			if (fault == 4) CurrentResourceOwner = NULL;
			if (fault == 5) CritSectionCount = 1;
			UT_ASSERT_EQ(read_call(input, full, &out), -1); hot_empty(&out); UT_ASSERT(!out.restart);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			UT_ASSERT_EQ(caller_enters, 0); UT_ASSERT_EQ(auth_describes, 0);
			UT_ASSERT_EQ(finish_requests, 0); CurrentResourceOwner = (ResourceOwner)1;
			CritSectionCount = 0; auth_clean(s);
		}
}
UT_TEST(read_caller_closed_admission_has_no_authority_or_debt)
{
	for (unsigned full = 0; full < 2; ++full) {
		Snapshot s = read_setup(true); MxHotReadout out; caller_closed = true;
		UT_ASSERT_EQ(read_call(s, full, &out), -1); hot_empty(&out);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(caller_enters, 1); UT_ASSERT_EQ(caller_leaves, 0);
		UT_ASSERT_EQ(caller_inflight, 0); UT_ASSERT_EQ(auth_describes, 0); auth_clean(s);
	}
}
UT_TEST(read_caller_ERROR_restores_outer_scope_and_clears_successor)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 2; ++fault) {
			Snapshot s = read_setup(true); MxHotReadout out;
			ClusterSnapshotReadScopeV1 outer; Snapshot actual; SCN floor; const char *reason;
			cluster_snapshot_read_enter_v1(&outer, s);
			auth_throw_describe = fault == 0;
			if (fault == 1) { finish_pending = true; history_wait_fault = 1; }
			UT_ASSERT_EQ(read_call(s, full, &out), -1); hot_empty(&out); UT_ASSERT(!out.restart);
			read_balanced(); auth_finished();
			UT_ASSERT(cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
			UT_ASSERT(actual == s); cluster_snapshot_read_exit_v1(&outer); auth_clean(s);
		}
}
UT_TEST(read_caller_restart_keeps_operation_and_releases_per_attempt_scope)
{
	for (unsigned full = 0; full < 2; ++full) {
		Snapshot s = read_setup(true); MxHotReadout out; MxAuthorizeState before, after;
		auth_descriptor_fault = 1;
		UT_ASSERT_EQ(read_call(s, full, &out), 0); UT_ASSERT(out.restart); hot_empty(&out);
		read_balanced(); mx_history_authorize_state(&before);
		UT_ASSERT(before.active && !before.finished); UT_ASSERT_EQ(before.restarts, 1);
		UT_ASSERT_EQ(read_call(s, full, &out), 1); UT_ASSERT(out.successor.valid);
		mx_history_authorize_state(&after); UT_ASSERT_EQ(before.operation_id, after.operation_id);
		UT_ASSERT_EQ(caller_enters, 2); UT_ASSERT_EQ(caller_leaves, 2); UT_ASSERT_EQ(caller_inflight, 0);
		UT_ASSERT_EQ(remembered, 1); auth_finished(); auth_clean(s);
	}
}
UT_TEST(read_caller_requalification_does_not_export_stale_scope)
{
	for (unsigned full = 0; full < 2; ++full)
		for (unsigned fault = 0; fault < 3; ++fault) {
			Snapshot s = read_setup(true); MxHotReadout out;
			if (fault == 0) hot_fault = 4;
			if (fault == 1) hot_fault = 5;
			if (fault == 2) s->read_scn = InvalidScn;
			UT_ASSERT_EQ(read_call(s, full, &out), -1); hot_empty(&out); UT_ASSERT(!out.restart);
			UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
			if (fault < 2) read_balanced(); else UT_ASSERT_EQ(caller_enters, 0);
			CurrentResourceOwner = (ResourceOwner)1;
			if (fault < 2) auth_finished();
			else {
				MxAuthorizeState state; mx_history_authorize_state(&state);
				UT_ASSERT_EQ(state.operation_id, 0); UT_ASSERT(!state.active);
			}
			auth_clean(s);
		}
}
UT_TEST(read_caller_share_bracket_is_never_promoted_or_replaced)
{
	for (unsigned full = 0; full < 2; ++full) {
		Snapshot s = read_setup(true); MxHotReadout out; exclusive = true;
		UT_ASSERT_EQ(read_call(s, full, &out), -1); hot_empty(&out);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		UT_ASSERT_EQ(hot_promotions, 0); UT_ASSERT_EQ(auth_describes, 0);
		read_balanced(); auth_clean(s);
	}
}
#ifdef HISTORY_WALKER_FIXTURE
int history_walker_unused_caller_main(void);
#define main history_walker_unused_caller_main
#endif
int main(void)
{
	history_fixture_init(); UT_PLAN(8);
	UT_RUN(read_caller_registered_scope_reaches_both_native_consumers);
	UT_RUN(read_caller_implicit_scope_is_only_current_active_snapshot);
	UT_RUN(read_caller_invalid_snapshot_never_dispatches);
	UT_RUN(read_caller_closed_admission_has_no_authority_or_debt);
	UT_RUN(read_caller_ERROR_restores_outer_scope_and_clears_successor);
	UT_RUN(read_caller_restart_keeps_operation_and_releases_per_attempt_scope);
	UT_RUN(read_caller_requalification_does_not_export_stale_scope);
	UT_RUN(read_caller_share_bracket_is_never_promoted_or_replaced);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
