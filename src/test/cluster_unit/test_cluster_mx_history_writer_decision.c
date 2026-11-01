/* Actual retained collector/B assembler and pure operation-aware decision.
 * Descriptor/history/PCM/source/admission remain controlled external leaves.
 * Native writer acceptance is intentionally RED until receipt/follow wiring.
 * Portions Copyright (c) 2026, pgrac contributors */
#define HISTORY_HOT_CALLER_FIXTURE
#include "test_cluster_mx_history_hot.c"
#undef main

static Snapshot
writer_decision_collect(ClusterTxOutcome outcome, bool own, ClusterMxHistoryCollector **c,
	ClusterMxHistoryCapture *capture, ClusterMxHistorySuccessorProof *b)
{
	Snapshot s = hot_child_setup(false);
	ClusterMxHistorySuccessorChallenge challenge = typed_hot_challenge();
	uint32 capabilities[2];
	finish_outcome = outcome; exclusive = true; bridge_lock_override = auth_lock;
	if (own) {
		cluster_node_id = 0;
		cluster_xid_stripe_latch_runtime(true, 0, FullTransactionIdFromEpochAndXid(1, 3));
		top_source = FullTransactionIdFromEpochAndXid(1, 80);
		canonical_source = (ClusterCanonicalTxnBinding){
			1, 0, 80, 3, 0, 1, CLUSTER_CANONICAL_TXN_PUBLISHED, 0, 123 };
		publication_source = CLUSTER_CANONICAL_TXN_PUBLISHED;
	}
	UT_ASSERT(typed_hot_begin(s, c));
	UT_ASSERT(cluster_mx_history_capture_read(*c, capture));
	UT_ASSERT_EQ(finish_ready(*c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(typed_hot_finish(*c, &challenge, b, capabilities), CMXH_COMPLETE);
	UT_ASSERT_EQ(capabilities[0], 21); UT_ASSERT_EQ(b->requester_capability_generation, 55);
	UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 2);
	return s;
}
static ClusterCurrentMxDecision
writer_decide(const ClusterMxHistoryCapture *capture, const ClusterMxHistorySuccessorProof *b,
	const ClusterCurrentMxRequestContext *ctx, ClusterTTStatusKey *wait,
	ClusterCurrentMxDecisionTrace *trace)
{
	ClusterMxHistorySuccessorChallenge challenge = typed_hot_challenge();
	return cluster_multixact_current_decide_operations_with_successor(members, finish_keys,
		finish_proofs, 2, ctx, &capture->requester_owner, &challenge.target, &b->key, &b->member,
		wait, trace);
}
UT_TEST(writer_typed_decision_preserves_outcome_action_and_wait_policy)
{
	const ClusterTxOutcome states[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < 3; ++state) {
		ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
		ClusterMxHistorySuccessorProof b;
		Snapshot s = writer_decision_collect(states[state], false, &c, &capture, &b);
		for (unsigned action = 0; action < 3; ++action)
			for (unsigned conflict = 0; conflict < 2; ++conflict)
				for (unsigned follow = 0; follow < 2; ++follow)
					for (unsigned policy = 0; policy < 3; ++policy)
						for (unsigned wait_for = 0; wait_for < 2; ++wait_for) {
			ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
			ClusterTTStatusKey wait = { 0 }, zero = { 0 }; ClusterCurrentMxDecisionTrace trace;
			ClusterCurrentMxDecision expected;
			bool conflicts = action != 0 || conflict;
			ctx.follow_updates = follow; ctx.wait_for_conflict = wait_for;
			ctx.wait_policy = policy;
			if (action == 0) {
				ctx.action = CCM_ACTION_LOCK;
				ctx.lock_mode = conflict ? LockTupleExclusive : LockTupleKeyShare;
				ctx.desired_status = conflict ? MultiXactStatusForUpdate : MultiXactStatusForKeyShare;
			} else if (action == 1) {
				ctx.action = CCM_ACTION_UPDATE;
				ctx.lock_mode = conflict ? LockTupleExclusive : LockTupleNoKeyExclusive;
				ctx.desired_status = conflict ? MultiXactStatusUpdate : MultiXactStatusNoKeyUpdate;
			} else {
				ctx.action = CCM_ACTION_DELETE; ctx.lock_mode = LockTupleExclusive;
				ctx.desired_status = MultiXactStatusUpdate;
			}
			if (state == 2) expected = CMDL_CONTINUE;
			else if (state == 1) expected = conflicts || follow ? CMDL_UPDATED : CMDL_CONTINUE;
			else if (!conflicts) expected = follow ? CMDL_FOLLOW_UPDATED : CMDL_CONTINUE;
			else if (action != 0 && !wait_for) expected = CMDL_BEING_MODIFIED;
			else if (action == 0 && policy == LockWaitSkip) expected = CMDL_WOULD_BLOCK;
			else if (action == 0 && policy == LockWaitError) expected = CMDL_LOCK_NOT_AVAILABLE;
			else expected = CMDL_WAIT_MEMBER;
			UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, &trace), expected);
			if (expected == CMDL_WAIT_MEMBER) {
				UT_ASSERT_EQ(wait.local_xid, 80); UT_ASSERT_EQ(wait.tt_slot_id, 4);
			} else UT_ASSERT(memcmp(&wait, &zero, sizeof(wait)) == 0);
		}
		UT_ASSERT_EQ(finish_reads, 3); UT_ASSERT_EQ(remembered, 2); clean(c, s);
	}
}
UT_TEST(writer_typed_decision_requires_exact_owner_before_SELF_or_precheck)
{
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	Snapshot s = writer_decision_collect(CLUSTER_TX_IN_PROGRESS, true, &c, &capture, &b);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	ClusterTTStatusKey wait;
	ctx.top_xid = 80; ctx.current_member_xid = 96; ctx.tuple_cmax = ctx.curcid;
	UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, NULL), CMDL_SELF_MODIFIED);
	ctx.tuple_cmax--;
	UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, NULL), CMDL_INVISIBLE);
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterMxHistoryCapture bad = capture;
		if (fault == 0) bad.requester_owner.top_fxid = FullTransactionIdFromEpochAndXid(2, 80);
		if (fault == 1) bad.requester_owner.segment_generation++;
		if (fault == 2) bad.requester_owner.slot_wrap++;
		if (fault == 3) memset(&bad.requester_owner, 0, sizeof(bad.requester_owner));
		UT_ASSERT_EQ(writer_decide(&bad, &b, &ctx, &wait, NULL), CMDL_UNKNOWN);
	}
	ctx.precheck_result = TM_SelfModified; b.member.base.state = CCM_UNKNOWN;
	UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, NULL), CMDL_UNKNOWN);
	UT_ASSERT_EQ(finish_proofs[0].base.state, CCM_ACTIVE); clean(c, s);
}
UT_TEST(writer_typed_decision_rejects_mixed_A_B_and_target)
{
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	Snapshot s = writer_decision_collect(CLUSTER_TX_COMMITTED, false, &c, &capture, &b);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	ClusterMxHistorySuccessorChallenge challenge = typed_hot_challenge();
	for (unsigned fault = 0; fault < 21; ++fault) {
		ClusterMxHistorySuccessorProof bad = b;
		ClusterCurrentMemberOperationProof a[2]; ClusterSavepointOperationKey keys[2];
		ClusterUndoRecordTarget target = challenge.target;
		ClusterTTStatusKey wait, zero = { 0 }; ClusterCurrentMxDecisionTrace trace;
		memcpy(a, finish_proofs, sizeof(a)); memcpy(keys, finish_keys, sizeof(keys));
		if (fault == 0) bad.member.top_fxid = FullTransactionIdFromEpochAndXid(2, 80);
		if (fault == 1) bad.member.operation_fxid = FullTransactionIdFromEpochAndXid(2, 96);
		if (fault == 2) bad.member.base.key.segment_generation++;
		if (fault == 3) bad.member.base.key.slot_wrap++;
		if (fault == 4) bad.member.base.key.local_xid = 96;
		if (fault == 5) bad.member.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (fault == 6) bad.member.base.commit_scn++;
		if (fault == 7) bad.member.base.state = CCM_ABORTED;
		if (fault == 8) bad.member.base.member_ordinal++;
		if (fault == 9) a[1].base.state = CCM_UNKNOWN;
		if (fault == 10) a[0].effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (fault == 11) bad.key.target.offnum++;
		if (fault == 12) target.blockno++;
		if (fault == 13) { target.locator.relNumber++; bad.key.target = target; }
		if (fault == 14) { target.forknum = VISIBILITYMAP_FORKNUM; bad.key.target = target; }
		if (fault == 15) { target.offnum = keys[0].target.offnum; bad.key.target = target; }
		if (fault == 16) bad.member.flags = 0;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_multixact_current_decide_operations_with_successor(members, keys,
			a, 2, &ctx, &capture.requester_owner, fault == 17 ? NULL : &target,
			fault == 18 ? NULL : &bad.key, fault == 19 ? NULL : &bad.member,
			&wait, &trace), fault == 20 ? CMDL_UPDATED : CMDL_UNKNOWN);
		UT_ASSERT(memcmp(&wait, &zero, sizeof(wait)) == 0);
		if (fault != 20) UT_ASSERT(trace.unknown_reason != CMX_UNKNOWN_NONE);
	}
	clean(c, s);
}
UT_TEST(writer_original_raw_gate_cannot_accept_a_typed_substitute)
{
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b;
	Snapshot s = writer_decision_collect(CLUSTER_TX_COMMITTED, false, &c, &capture, &b);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true); ClusterTTStatusKey wait;
	UT_ASSERT_EQ(cluster_multixact_current_decide_operations(members, finish_keys, finish_proofs,
		2, &ctx, &capture.requester_owner, NULL, NULL, &wait, NULL), CMDL_UNKNOWN);
	UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, NULL), CMDL_UPDATED);
	clean(c, s);
}
UT_TEST(writer_decision_keeps_original_A_for_receipts)
{
	ClusterMxHistoryCollector *c = NULL; ClusterMxHistoryCapture capture;
	ClusterMxHistorySuccessorProof b; MxReceiptReadout retained;
	Snapshot s = writer_decision_collect(CLUSTER_TX_IN_PROGRESS, false, &c, &capture, &b);
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true); ClusterTTStatusKey wait;
	uint32 capabilities[2] = { 21, 34 };
	UT_ASSERT_EQ(writer_decide(&capture, &b, &ctx, &wait, NULL), CMDL_FOLLOW_UPDATED);
	UT_ASSERT(mx_receipt_retain(&mxkey, members, finish_keys, finish_proofs, capabilities, 2,
		145, MultiXactStatusForKeyShare, 900, 4, &retained));
	UT_ASSERT_EQ(retained.proof_count, 1); UT_ASSERT_EQ(retained.capabilities[0], 21);
	UT_ASSERT_EQ(retained.proofs[0].member_xid, 96); UT_ASSERT_EQ(retained.proofs[0].key.local_xid, 80);
	UT_ASSERT_EQ(b.requester_capability_generation, 55); UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_reads, 3); clean(c, s);
}
UT_TEST(native_writer_child_canonical_successor_acceptance)
{
	Snapshot s = hot_child_setup(false); MxHeapReadout out;
	ClusterCurrentMxRequestContext ctx = heap_consumer_context(true);
	int result;
	exclusive = true; bridge_lock_override = auth_lock; finish_outcome = CLUSTER_TX_IN_PROGRESS;
	result = auth_call(s, &admission, &ctx, &out);
	printf("# native writer child96/top80 result=%d error=%d history_requests=%u reads=%u\n",
		result, error_code, finish_requests, finish_reads);
	UT_ASSERT_EQ(result, 2);
	if (result == 2) {
		UT_ASSERT(out.follow_updated_chain && out.successor.valid);
		UT_ASSERT_EQ(out.result, TM_Ok); UT_ASSERT_EQ(out.retained.proof_count, 1);
		UT_ASSERT_EQ(finish_reads, 3);
	}
	auth_clean(s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(6);
	UT_RUN(writer_typed_decision_preserves_outcome_action_and_wait_policy);
	UT_RUN(writer_typed_decision_requires_exact_owner_before_SELF_or_precheck);
	UT_RUN(writer_typed_decision_rejects_mixed_A_B_and_target);
	UT_RUN(writer_original_raw_gate_cannot_accept_a_typed_substitute);
	UT_RUN(writer_decision_keeps_original_A_for_receipts);
	UT_RUN(native_writer_child_canonical_successor_acceptance);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
