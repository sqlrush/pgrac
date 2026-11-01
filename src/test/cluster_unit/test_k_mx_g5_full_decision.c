/* Whole-operation pure decisions from literal G2 fixtures; no live authority. */
int k_full_decision_recompose_main(void);
#define K_MX_FULL_RECOMPOSE_MAIN k_full_decision_recompose_main
#include "test_k_mx_g5_full_recompose.c"
#include "cluster/cluster_undo_format.h"

static ClusterCurrentMxRequestContext
k_context(void)
{
	ClusterCurrentMxRequestContext ctx = {0};
	ctx.mxkey.origin_node_id = 2; ctx.mxkey.multixact_id = 1000; ctx.mxkey.cluster_epoch = 9;
	ctx.top_xid = ctx.current_member_xid = 7; ctx.curcid = ctx.tuple_cmax = 5;
	ctx.precheck_result = TM_Ok; ctx.desired_status = MultiXactStatusForUpdate;
	ctx.lock_mode = LockTupleExclusive; ctx.wait_policy = LockWaitBlock;
	ctx.action = CCM_ACTION_LOCK; ctx.tuple_shape = CCM_SHAPE_LOCK_ONLY;
	ctx.wait_for_conflict = 1; ctx.updater_origin_node_id = 2;
	return ctx;
}
static ClusterMxFullMemberProof *
k_proof_at(ClusterMxCompositionReceive *op, uint32 ordinal)
{
	uint32 n; const ClusterMxCompositionProofPage *page = cluster_mx_composition_proofs(op, &n);
	while (page && ordinal >= page->first + page->count) page = page->next;
	if (!page || ordinal < page->first) abort();
	return (ClusterMxFullMemberProof *)&page->proofs[ordinal - page->first];
}
static ClusterCurrentMxMemberDesc *
k_member_at(ClusterMxCompositionReceive *op, uint32 ordinal)
{
	uint32 n; const ClusterMxCompositionPage *page = cluster_mx_composition_pages(op, &n);
	while (page && ordinal >= page->first + page->count) page = page->next;
	if (!page || ordinal < page->first) abort();
	return (ClusterCurrentMxMemberDesc *)&page->members[ordinal - page->first];
}
static void
k_successor(uint32 ordinal, ClusterMxFullUpdaterChallenge *challenge,
			ClusterMxFullUpdaterProof *proof)
{
	uint32 segment = 2 * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	ClusterCurrentUpdaterChallenge *c = &challenge->challenge;
	memset(challenge, 0, sizeof(*challenge)); memset(proof, 0, sizeof(*proof));
	challenge->member_ordinal = proof->member_ordinal = ordinal;
	c->member_ordinal = ordinal % 7; c->updater_xid = 100 + ordinal;
	c->candidate_next_xmin_alias.origin_node_id = 2;
	c->candidate_next_xmin_alias.undo_record_segment_id = segment;
	c->candidate_next_xmin_alias.tt_slot_id = 1;
	c->candidate_next_xmin_alias.cluster_epoch = 9;
	c->candidate_next_xmin_alias.local_xid = c->updater_xid;
	c->candidate_next_xmin_locator.uba = uba_encode(segment, 2, 0, 0);
	c->candidate_next_xmin_locator.xid = c->updater_xid;
	c->candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;
	c->candidate_next_xmin_locator.itl_kind = ITL_FLAG_ACTIVE;
	c->candidate_next_xmin_locator.itl_slot_index = 0;
	proof->proof.mxkey = k_context().mxkey;
	proof->proof.candidate_next_xmin_alias = c->candidate_next_xmin_alias;
	proof->proof.candidate_next_xmin_locator = c->candidate_next_xmin_locator;
	proof->proof.candidate_next_xmin_locator.tt_wrap = 3;
	proof->proof.updater_xid = c->updater_xid;
	proof->proof.member_ordinal = c->member_ordinal; proof->proof.verdict = CUCP_MATCH;
}
static void
k_no_wait(const ClusterTTStatusKey *wait)
{
	static const ClusterTTStatusKey zero;
	UT_ASSERT_EQ(memcmp(wait, &zero, sizeof(zero)), 0);
}

UT_TEST(whole_terminal_decisions_run_through_65537)
{
	const uint32 sizes[] = {2, 256, 257, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(sizes[s], false, false, false, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context();
		ClusterTTStatusKey wait; ClusterCurrentMxDecisionTrace trace;
		memset(&wait, 0xa5, sizeof(wait)); memset(&trace, 0xa5, sizeof(trace));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, &trace), CMDL_CONTINUE);
		k_no_wait(&wait); UT_ASSERT_EQ(trace.unknown_reason, CMX_UNKNOWN_NONE);
		UT_ASSERT_EQ(trace.member_ordinal, -1);
		cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(committed_tail_updater_is_deleted_without_prefix_success)
{
	const uint32 sizes[] = {257, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(sizes[s], false, false, true, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		ctx.tuple_shape = CCM_SHAPE_DELETED;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_DELETED);
		k_no_wait(&wait); cluster_mx_composition_finish(&op); check_discarded(op);
	}
	/* A terminal updater precedes conflicts from other members in the
	 * original whole-list rule. Populate this literal state before deciding. */
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, mode != 3, false, true, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		ClusterCurrentMemberProof *q = &k_proof_at(op, 256)->proof;
		ClusterMxFullUpdaterChallenge c; ClusterMxFullUpdaterProof p;
		const ClusterCurrentMxDecision expected[] = {CMDL_DELETED, CMDL_UPDATED,
			CMDL_WAIT_MEMBER, CMDL_CONTINUE};
		memset(&q->key, 0, sizeof(q->key));
		memset(q->origin_fullxid_v2, 0, sizeof(q->origin_fullxid_v2));
		ClusterCurrentMemberProofSetCtrcGrant(q, 0);
		q->state = mode < 2 ? CCM_COMMITTED : CCM_ABORTED;
		q->commit_scn = mode < 2 ? 1256 : InvalidScn;
		ctx.tuple_shape = mode == 1 ? CCM_SHAPE_UPDATED : CCM_SHAPE_DELETED;
		k_successor(256, &c, &p); memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, mode == 1 ? &c : NULL,
			mode == 1 ? &p : NULL, &wait, NULL), expected[mode]);
		if (mode == 2) UT_ASSERT_EQ(wait.local_xid, 100); else k_no_wait(&wait);
		cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(unknown_tail_wins_over_wait_precheck_and_updater)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(65537, true, false, mode == 2, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context();
		ClusterTTStatusKey wait; ClusterCurrentMxDecisionTrace trace;
		k_proof_at(op, 65536)->proof.state = CCM_UNKNOWN;
		if (mode == 1) ctx.precheck_result = TM_Invisible;
		if (mode == 2) ctx.tuple_shape = CCM_SHAPE_DELETED;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, &trace), CMDL_UNKNOWN);
		k_no_wait(&wait); UT_ASSERT_EQ(trace.member_ordinal, 65536);
		cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(global_wait_key_can_come_from_member_65536)
{
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(65537, true, false, false, true, &stats);
	ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
	ClusterMxFullMemberProof *last = k_proof_at(op, 65536);
	last->origin_node_id = last->proof.key.origin_node_id = 1;
	UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait.origin_node_id, 1); UT_ASSERT_EQ(wait.local_xid, 65636);
	UT_ASSERT_EQ(wait.undo_segment_id, 1); UT_ASSERT_EQ(wait.tt_slot_id, 1);
	UT_ASSERT_EQ(wait._reserved, 0); UT_ASSERT_EQ(wait._reserved2, 0);
	cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(global_wait_key_keeps_original_slot_and_segment_ties)
{
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(257, true, false, false, true, &stats);
	ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
	const uint32 ordinals[] = {7, 100, 256};
	for (unsigned i = 0; i < lengthof(ordinals); i++) {
		ClusterMxFullMemberProof *q = k_proof_at(op, ordinals[i]);
		q->origin_node_id = q->proof.key.origin_node_id = 1;
		q->proof.key.local_xid = 77;
		q->proof.key.tt_slot_id = i == 0 ? 7 : 3;
		q->proof.key.undo_segment_id = i == 2 ? 1 : 2;
	}
	UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait.origin_node_id, 1); UT_ASSERT_EQ(wait.local_xid, 77);
	UT_ASSERT_EQ(wait.tt_slot_id, 3); UT_ASSERT_EQ(wait.undo_segment_id, 1);
	cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(whole_set_wait_policies_preserve_zero_outputs)
{
	const ClusterCurrentMxDecision expected[] = {CMDL_BEING_MODIFIED, CMDL_WOULD_BLOCK,
		CMDL_LOCK_NOT_AVAILABLE, CMDL_WAIT_MEMBER};
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(257, true, false, false, true, &stats);
	for (unsigned mode = 0; mode < lengthof(expected); mode++) {
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		if (mode == 0) {
			ctx.action = CCM_ACTION_UPDATE; ctx.desired_status = MultiXactStatusUpdate;
			ctx.wait_for_conflict = 0;
		} else ctx.wait_policy = mode == 1 ? LockWaitSkip : mode == 2 ? LockWaitError : LockWaitBlock;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), expected[mode]);
		if (mode == 3) UT_ASSERT_EQ(wait.local_xid, 100); else k_no_wait(&wait);
	}
	cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(full_updater_provenance_uses_global_and_local_positions)
{
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(65537, false, false, true, true, &stats);
	ClusterCurrentMxRequestContext ctx = k_context();
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterMxFullUpdaterChallenge c; ClusterMxFullUpdaterProof p;
		ClusterTTStatusKey wait; ClusterCurrentMxDecisionTrace trace;
		k_successor(65536, &c, &p);
		switch (fault) {
		case 1: c.member_ordinal = 0; break;
		case 2: p.member_ordinal = 0; break;
		case 3: c.challenge.member_ordinal++; break;
		case 4: p.proof.member_ordinal++; break;
		case 5: p.proof.mxkey.multixact_id++; break;
		case 6: p.proof.candidate_next_xmin_locator.xid++; break;
		}
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, &c, &p, &wait, &trace),
			fault == 0 ? CMDL_UPDATED : CMDL_UNKNOWN);
		k_no_wait(&wait);
		if (fault) UT_ASSERT_EQ(trace.member_ordinal, 65536);
	}
	cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(compatible_active_follow_waits_for_other_locker_first)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input_status(257, true, false, true, true,
			&stats, MultiXactStatusNoKeyUpdate);
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		ClusterMxFullUpdaterChallenge c; ClusterMxFullUpdaterProof p;
		ctx.tuple_shape = CCM_SHAPE_UPDATED; ctx.follow_updates = 1;
		ctx.desired_status = MultiXactStatusForKeyShare; ctx.lock_mode = LockTupleKeyShare;
		k_successor(256, &c, &p);
		if (mode == 1) {
			k_member_at(op, 0)->member_status = MultiXactStatusForUpdate;
			k_proof_at(op, 0)->proof.member_status = MultiXactStatusForUpdate;
		}
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, mode == 0 ? &c : NULL,
			mode == 0 ? &p : NULL, &wait, NULL),
			mode == 0 ? CMDL_FOLLOW_UPDATED : mode == 1 ? CMDL_WAIT_MEMBER : CMDL_UNKNOWN);
		if (mode == 1) UT_ASSERT_EQ(wait.local_xid, 100); else k_no_wait(&wait);
		cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(global_tuple_shape_and_precheck_are_not_per_frame_defaults)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, false, false, false, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		if (mode == 0) ctx.tuple_shape = CCM_SHAPE_UPDATED;
		if (mode == 1) ctx.precheck_result = TM_Invisible;
		if (mode == 2) ctx.precheck_result = TM_SelfModified;
		if (mode == 3) ctx.follow_updates = 2;
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL),
			mode == 1 ? CMDL_INVISIBLE : mode == 2 ? CMDL_SELF_MODIFIED : CMDL_UNKNOWN);
		k_no_wait(&wait); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(missing_final_proof_never_returns_a_prefix_decision)
{
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(257, true, false, false, false, &stats);
	ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
	memset(&wait, 0xa5, sizeof(wait));
	UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_UNKNOWN);
	k_no_wait(&wait); cluster_mx_composition_finish(&op); check_discarded(op);
}
UT_TEST(member_epoch_and_request_context_cannot_be_masked)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterMxCompositionReceiveStats stats = {0};
		ClusterMxCompositionReceive *op = k_plan_input(257, true, false, mode == 2, true, &stats);
		ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
		if (mode == 0) ctx.mxkey.cluster_epoch = 8;
		if (mode == 1) ctx.mxkey.multixact_id = 0;
		/* A supplied updater cannot be hidden by projecting every member as a locker. */
		memset(&wait, 0xa5, sizeof(wait));
		UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_UNKNOWN);
		k_no_wait(&wait); cluster_mx_composition_finish(&op); check_discarded(op);
	}
}
UT_TEST(raw_foreign_proof_cannot_manufacture_self)
{
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = k_plan_input(257, true, false, false, true, &stats);
	ClusterCurrentMxRequestContext ctx = k_context(); ClusterTTStatusKey wait;
	k_proof_at(op, 256)->proof.state = CCM_SELF;
	ctx.top_xid = ctx.current_member_xid = 356;
	memset(&wait, 0xa5, sizeof(wait));
	UT_ASSERT_EQ(cluster_mx_composition_decide(op, &ctx, NULL, NULL, &wait, NULL), CMDL_UNKNOWN);
	k_no_wait(&wait); cluster_mx_composition_finish(&op); check_discarded(op);
}
int main(void)
{
	UT_PLAN(12);
	UT_RUN(whole_terminal_decisions_run_through_65537);
	UT_RUN(committed_tail_updater_is_deleted_without_prefix_success);
	UT_RUN(unknown_tail_wins_over_wait_precheck_and_updater);
	UT_RUN(global_wait_key_can_come_from_member_65536);
	UT_RUN(global_wait_key_keeps_original_slot_and_segment_ties);
	UT_RUN(whole_set_wait_policies_preserve_zero_outputs);
	UT_RUN(full_updater_provenance_uses_global_and_local_positions);
	UT_RUN(compatible_active_follow_waits_for_other_locker_first);
	UT_RUN(global_tuple_shape_and_precheck_are_not_per_frame_defaults);
	UT_RUN(missing_final_proof_never_returns_a_prefix_decision);
	UT_RUN(member_epoch_and_request_context_cannot_be_masked);
	UT_RUN(raw_foreign_proof_cannot_manufacture_self);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
