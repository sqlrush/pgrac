/* Exact-operation consumers over the existing decision/composition core.
 * Private proof values are explicit inputs, not native TT40 witnesses.
 * Portions Copyright (c) 2026, pgrac contributors */
int consumer_unused_mx_main(void);
#define main consumer_unused_mx_main
#include "test_cluster_multixact_current.c"
#undef main
#include "cluster/cluster_savepoint_reader.h"
#include "cluster/cluster_gcs_block.h"

typedef struct ConsumerFixture {
	ClusterCurrentMxKey mx;
	ClusterCurrentMxMemberDesc members[3];
	ClusterSavepointOperationKey keys[3];
	ClusterCurrentMemberOperationProof proofs[3];
	ClusterCurrentMxRequestContext ctx;
	ClusterR4CrOwnerChallenge owner;
} ConsumerFixture;
static void
consumer_setup(ConsumerFixture *f, uint16 n)
{
	cluster_node_id = 3;
	memset(f, 0, sizeof(*f));
	f->mx = test_mxkey();
	for (uint16 i = 0; i < n; ++i) {
		ClusterCurrentMemberOperationProof *p = &f->proofs[i];
		test_member(&f->members[i], 100 + i * 3, MultiXactStatusForShare);
		f->keys[i] = (ClusterSavepointOperationKey){
			.locator = { .uba = uba_encode(1281, 7 + i, 2, 0),
						 .xid = 97,
						 .tt_wrap = 3,
						 .itl_kind = ITL_FLAG_ACTIVE,
						 .itl_slot_index = 0 },
			.target
			= { .locator = { 1663, 42, 123 }, .forknum = MAIN_FORKNUM, .blockno = 17, .offnum = 7 },
			.top_fxid = FullTransactionIdFromEpochAndXid(2, 97),
			.operation_fxid = FullTransactionIdFromEpochAndXid(2, 100 + i * 3)
		};
		p->base = (ClusterCurrentMemberProof){ .key = { .origin_node_id = 5,
														.undo_segment_id = 1281,
														.tt_slot_id = 3,
														.cluster_epoch = 9,
														.local_xid = 97,
														.segment_generation = 7,
														.slot_wrap = 3,
														.binding_version = 1 },
											   .member_xid = f->members[i].xid,
											   .member_ordinal = i,
											   .member_status = MultiXactStatusForShare,
											   .state = CCM_ACTIVE };
		ClusterCurrentMemberProofSetCtrcGrant(&p->base, 17);
		p->operation_uba = f->keys[i].locator.uba;
		p->top_fxid = f->keys[i].top_fxid;
		p->operation_fxid = f->keys[i].operation_fxid;
		p->effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
		p->format_version = 2;
		p->flags = 1;
	}
	test_context(&f->ctx, &f->mx, CCM_ACTION_LOCK, MultiXactStatusForUpdate, LockTupleExclusive);
}
static void
consumer_discard(ConsumerFixture *f, uint16 i)
{
	f->proofs[i].effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
	ClusterCurrentMemberProofSetCtrcGrant(&f->proofs[i].base, 0);
}
static ClusterCurrentMxDecision
consumer_decide(ConsumerFixture *f, uint16 n, ClusterTTStatusKey *wait)
{
	return cluster_multixact_current_decide_operations(f->members, f->keys, f->proofs, n, &f->ctx,
													   &f->owner, NULL, NULL, wait, NULL);
}
UT_TEST(consumer_discarded_active_does_not_conflict_or_rewrite_top)
{
	ConsumerFixture f;
	ClusterCurrentMemberOperationProof before;
	ClusterTTStatusKey wait = { 0 };
	consumer_setup(&f, 1);
	consumer_discard(&f, 0);
	before = f.proofs[0];
	UT_ASSERT_EQ(cluster_multixact_current_validate_operation_proof_set(&f.mx, f.members, f.keys,
																		f.proofs, 1),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(consumer_decide(&f, 1, &wait), CMDL_CONTINUE);
	UT_ASSERT_EQ(memcmp(&before, &f.proofs[0], sizeof(before)), 0);
	UT_ASSERT_EQ(wait.local_xid, InvalidTransactionId);
}
UT_TEST(consumer_live_child_waits_on_canonical_top)
{
	ConsumerFixture f;
	ClusterTTStatusKey wait = { 0 };
	consumer_setup(&f, 1);
	UT_ASSERT_EQ(consumer_decide(&f, 1, &wait), CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait.local_xid, 97);
	UT_ASSERT_EQ(wait.undo_segment_id, 1281);
	f.ctx.wait_policy = LockWaitSkip;
	UT_ASSERT_EQ(consumer_decide(&f, 1, &wait), CMDL_WOULD_BLOCK);
	f.ctx.wait_policy = LockWaitError;
	UT_ASSERT_EQ(consumer_decide(&f, 1, &wait), CMDL_LOCK_NOT_AVAILABLE);
}
UT_TEST(consumer_unknown_member_poison_is_whole_set)
{
	ConsumerFixture f;
	consumer_setup(&f, 2);
	consumer_discard(&f, 0);
	f.proofs[1].base.state = CCM_UNKNOWN;
	UT_ASSERT_EQ(consumer_decide(&f, 2, NULL), CMDL_UNKNOWN);
}
UT_TEST(consumer_exact_mismatch_never_becomes_discarded_success)
{
	for (unsigned fault = 0; fault < 15; ++fault) {
		ConsumerFixture f;
		consumer_setup(&f, 2);
		consumer_discard(&f, 0);
		switch (fault) {
		case 0:
			f.proofs[0].base.member_ordinal++;
			break;
		case 1:
			f.proofs[0].base.member_status++;
			break;
		case 2:
			f.proofs[0].base.key.local_xid++;
			break;
		case 3:
			f.proofs[0].operation_fxid = FullTransactionIdFromEpochAndXid(3, 100);
			break;
		case 4:
			f.proofs[0].base.key.cluster_epoch++;
			break;
		case 5:
			f.proofs[0].base.key.slot_wrap++;
			break;
		case 6:
			f.proofs[0].base.key.segment_generation = UINT32_MAX;
			break;
		case 7:
			f.proofs[0].base.key.binding_version = 0;
			break;
		case 8:
			f.proofs[0].effect_state = 0;
			break;
		case 9:
			f.proofs[0].reserved = 1;
			break;
		case 10:
			f.proofs[0].operation_uba = f.keys[1].locator.uba;
			break;
		case 11:
			f.keys[1].target.offnum++;
			break;
		case 12:
			ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 17);
			break;
		case 13:
			f.proofs[0].base.state = CCM_SELF;
			break;
		case 14:
			f.keys[0].locator.itl_slot_index = 8;
			break;
		}
		UT_ASSERT_EQ(consumer_decide(&f, 2, NULL), CMDL_UNKNOWN);
	}
}
UT_TEST(consumer_live_grant_required_even_for_compatible_lock)
{
	ConsumerFixture f;
	consumer_setup(&f, 1);
	f.ctx.lock_mode = LockTupleKeyShare;
	f.ctx.desired_status = MultiXactStatusForKeyShare;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_CONTINUE);
	ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0);
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_UNKNOWN);
}
static void
consumer_owner(ConsumerFixture *f)
{
	cluster_node_id = 5;
	f->ctx.top_xid = 97;
	f->ctx.current_member_xid = 100;
	f->owner = (ClusterR4CrOwnerChallenge){ .top_fxid = f->keys[0].top_fxid,
											.active_lsn = 10,
											.segment_id = 1281,
											.segment_generation = 7,
											.top_xid = 97,
											.slot_offset = 2,
											.slot_wrap = 3,
											.origin_instance = 6,
											.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,
											.flags = CLUSTER_R4_CR_OWNER_PRESENT };
}
UT_TEST(consumer_only_exact_owner_projects_self_and_applies_cid)
{
	ConsumerFixture f;
	consumer_setup(&f, 1);
	consumer_owner(&f);
	f.members[0].member_status = f.proofs[0].base.member_status = MultiXactStatusUpdate;
	f.ctx.action = CCM_ACTION_UPDATE;
	f.ctx.desired_status = MultiXactStatusUpdate;
	f.ctx.tuple_shape = CCM_SHAPE_DELETED;
	f.ctx.curcid = 5;
	f.ctx.tuple_cmax = 5;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_SELF_MODIFIED);
	f.ctx.tuple_cmax = 4;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_INVISIBLE);
	UT_ASSERT_EQ(f.proofs[0].base.state, CCM_ACTIVE);
}
UT_TEST(consumer_raw_owner_collision_without_full_binding_fails_closed)
{
	for (unsigned fault = 0; fault < 8; ++fault) {
		ConsumerFixture f;
		consumer_setup(&f, 1);
		consumer_owner(&f);
		switch (fault) {
		case 0:
			f.owner.top_fxid = FullTransactionIdFromEpochAndXid(3, 97);
			break;
		case 1:
			f.owner.segment_generation++;
			break;
		case 2:
			f.owner.slot_wrap++;
			break;
		case 3:
			f.owner.slot_offset++;
			break;
		case 4:
			f.owner.segment_id++;
			break;
		case 5:
			memset(&f.owner, 0, sizeof(f.owner));
			break;
		case 6:
			f.owner.origin_instance--;
			break;
		case 7:
			cluster_node_id = 3;
			break;
		}
		UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_UNKNOWN);
	}
}
UT_TEST(consumer_discarded_self_does_not_trigger_self_modified)
{
	ConsumerFixture f;
	consumer_setup(&f, 1);
	consumer_owner(&f);
	consumer_discard(&f, 0);
	f.members[0].member_status = f.proofs[0].base.member_status = MultiXactStatusUpdate;
	f.ctx.action = CCM_ACTION_UPDATE;
	f.ctx.desired_status = MultiXactStatusUpdate;
	f.ctx.tuple_shape = CCM_SHAPE_DELETED;
	f.ctx.curcid = f.ctx.tuple_cmax = 5;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_CONTINUE);
}
UT_TEST(consumer_recompose_preserves_raw_survivors_and_drops_only_effect)
{
	ConsumerFixture f;
	MultiXactMember out[4] = { 0 };
	uint16 n = 99;
	consumer_setup(&f, 3);
	consumer_discard(&f, 1);
	f.proofs[2].base.state = CCM_COMMITTED;
	f.proofs[2].base.commit_scn = 500;
	ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[2].base, 0);
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, f.members, f.keys, f.proofs,
																3, 200, MultiXactStatusForKeyShare,
																out, 4, &n),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(n, 2);
	UT_ASSERT_EQ(out[0].xid, 100);
	UT_ASSERT_EQ(out[1].xid, 200);
	UT_ASSERT_EQ(f.proofs[1].base.state, CCM_ACTIVE);
}
UT_TEST(consumer_committed_updater_live_denied_discarded_allowed)
{
	ConsumerFixture f;
	MultiXactMember out[2] = { 0 };
	uint16 n;
	consumer_setup(&f, 1);
	f.members[0].member_status = f.proofs[0].base.member_status = MultiXactStatusUpdate;
	f.proofs[0].base.state = CCM_COMMITTED;
	f.proofs[0].base.commit_scn = 500;
	ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0);
	f.ctx.tuple_shape = CCM_SHAPE_DELETED;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_DELETED);
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, f.members, f.keys, f.proofs,
																1, 200, MultiXactStatusForUpdate,
																out, 2, &n),
				 CMX_RECOMPOSE_DENIED);
	consumer_discard(&f, 0);
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_CONTINUE);
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, f.members, f.keys, f.proofs,
																1, 200, MultiXactStatusForUpdate,
																out, 2, &n),
				 CMX_RECOMPOSE_OK);
}
UT_TEST(consumer_v1_and_exact_mix_keep_original_top_only_contract)
{
	ConsumerFixture f;
	consumer_setup(&f, 2);
	consumer_discard(&f, 0);
	memset(&f.keys[1], 0, sizeof(f.keys[1]));
	memset(&f.proofs[1], 0, sizeof(f.proofs[1]));
	test_proof(&f.proofs[1].base, &f.members[1], 1, CCM_ABORTED, 5, 3);
	f.proofs[1].format_version = 2;
	UT_ASSERT_EQ(consumer_decide(&f, 2, NULL), CMDL_CONTINUE);
	f.proofs[1].operation_fxid = FullTransactionIdFromEpochAndXid(2, 103);
	UT_ASSERT_EQ(consumer_decide(&f, 2, NULL), CMDL_UNKNOWN);
}
UT_TEST(consumer_failure_clears_complete_recompose_output)
{
	ConsumerFixture f;
	MultiXactMember out[3], zero[3] = { 0 };
	uint16 n = 99;
	consumer_setup(&f, 2);
	f.proofs[1].base.member_xid++;
	memset(out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, f.members, f.keys, f.proofs,
																2, 200, MultiXactStatusForShare,
																out, 3, &n),
				 CMX_RECOMPOSE_UNKNOWN);
	UT_ASSERT_EQ(n, 0);
	UT_ASSERT_EQ(memcmp(out, zero, sizeof(out)), 0);
}

UT_TEST(consumer_fullxid_order_survives_raw_wrap)
{
	ConsumerFixture f;
	ClusterTTStatusKey wait = { 0 };
	consumer_setup(&f, 1);
	f.members[0].xid = f.proofs[0].base.member_xid = 94;
	f.keys[0].operation_fxid = f.proofs[0].operation_fxid = FullTransactionIdFromEpochAndXid(3, 94);
	UT_ASSERT_EQ(consumer_decide(&f, 1, &wait), CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait.local_xid, 97);
}
UT_TEST(consumer_complete_cap_and_no_partial_capacity_success)
{
	ConsumerFixture f;
	static ClusterCurrentMxMemberDesc members[256];
	static ClusterSavepointOperationKey keys[256];
	static ClusterCurrentMemberOperationProof proofs[256];
	static MultiXactMember normalized[256];
	MultiXactMember canary = { 999, MultiXactStatusForShare };
	uint16 count;
	consumer_setup(&f, 1);
	for (uint16 i = 0; i < 256; ++i) {
		members[i] = f.members[0];
		members[i].xid = 100 + i * 3;
		keys[i] = f.keys[0];
		keys[i].operation_fxid = FullTransactionIdFromEpochAndXid(2, members[i].xid);
		proofs[i] = f.proofs[0];
		proofs[i].base.member_ordinal = i;
		proofs[i].base.member_xid = members[i].xid;
		proofs[i].operation_fxid = keys[i].operation_fxid;
	}
	UT_ASSERT_EQ(
		cluster_multixact_current_validate_operation_proof_set(&f.mx, members, keys, proofs, 256),
		CMX_RESOLVE_OK);
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, members, keys, proofs, 256,
																999, MultiXactStatusForShare,
																normalized, 256, &count),
				 CMX_RECOMPOSE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(normalized[0].xid, 0);
	for (uint16 i = 0; i < 256; ++i) {
		proofs[i].effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		ClusterCurrentMemberProofSetCtrcGrant(&proofs[i].base, 0);
	}
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, members, keys, proofs, 256,
																999, MultiXactStatusForShare,
																normalized, 256, &count),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(normalized[0].xid, 999);
	UT_ASSERT_EQ(
		cluster_multixact_current_validate_operation_proof_set(&f.mx, members, keys, proofs, 257),
		CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(cluster_multixact_current_recompose_operations(&f.mx, members, keys, proofs, 256,
																999, MultiXactStatusForShare,
																&canary, 257, &count),
				 CMX_RECOMPOSE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(canary.xid, 999);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(
		cluster_multixact_current_validate_operation_proof_set(&f.mx, members, NULL, proofs, 1),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(
		cluster_multixact_current_validate_operation_proof_set(&f.mx, members, keys, NULL, 1),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(
		cluster_multixact_current_validate_operation_proof_set(&f.mx, members, keys, proofs, 0),
		CMX_RESOLVE_UNKNOWN);
}

UT_TEST(consumer_exact_successor_proof_required_for_live_updater)
{
	ConsumerFixture f;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater = { 0 };
	consumer_setup(&f, 1);
	f.members[0].member_status = f.proofs[0].base.member_status = MultiXactStatusNoKeyUpdate;
	f.ctx.lock_mode = LockTupleKeyShare;
	f.ctx.desired_status = MultiXactStatusForKeyShare;
	f.ctx.tuple_shape = CCM_SHAPE_UPDATED;
	f.ctx.follow_updates = 1;
	f.ctx.updater_origin_node_id = 5;
	test_updater_challenge(&challenge, 5, 100, 3);
	updater.mxkey = f.mx;
	updater.candidate_next_xmin_alias = challenge.candidate_next_xmin_alias;
	updater.candidate_next_xmin_locator = challenge.candidate_next_xmin_locator;
	updater.candidate_next_xmin_locator.tt_wrap = 3;
	updater.updater_xid = 100;
	updater.verdict = CUCP_MATCH;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_UNKNOWN);
	UT_ASSERT_EQ(cluster_multixact_current_decide_operations(f.members, f.keys, f.proofs, 1, &f.ctx,
															 &f.owner, &challenge, &updater, NULL,
															 NULL),
				 CMDL_FOLLOW_UPDATED);
	f.proofs[0].base.state = CCM_COMMITTED;
	f.proofs[0].base.commit_scn = 500;
	ClusterCurrentMemberProofSetCtrcGrant(&f.proofs[0].base, 0);
	UT_ASSERT_EQ(cluster_multixact_current_decide_operations(f.members, f.keys, f.proofs, 1, &f.ctx,
															 &f.owner, &challenge, &updater, NULL,
															 NULL),
				 CMDL_UPDATED);
	updater.updater_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_decide_operations(f.members, f.keys, f.proofs, 1, &f.ctx,
															 &f.owner, &challenge, &updater, NULL,
															 NULL),
				 CMDL_UNKNOWN);
	consumer_discard(&f, 0);
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_CONTINUE);
}
UT_TEST(consumer_v1_exact_canonical_owner_projects_self)
{
	ConsumerFixture f;
	consumer_setup(&f, 1);
	consumer_owner(&f);
	memset(&f.keys[0], 0, sizeof(f.keys[0]));
	f.members[0].xid = f.proofs[0].base.member_xid = 97;
	f.proofs[0].operation_uba = (UBA){ 0 };
	f.proofs[0].top_fxid = f.proofs[0].operation_fxid = FullTransactionIdFromU64(0);
	f.proofs[0].effect_state = 0;
	f.proofs[0].flags = 0;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_CONTINUE);
	/* Same physical slot but a different canonical raw top must not use
	 * the current child raw xid as a shortcut to SELF. */
	f.members[0].xid = f.proofs[0].base.member_xid = f.proofs[0].base.key.local_xid = 100;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_UNKNOWN);
	f.members[0].xid = f.proofs[0].base.member_xid = f.proofs[0].base.key.local_xid = 97;
	f.owner.segment_generation++;
	UT_ASSERT_EQ(consumer_decide(&f, 1, NULL), CMDL_UNKNOWN);
}
int
main(void)
{
	UT_PLAN(16);
	UT_RUN(consumer_exact_successor_proof_required_for_live_updater);
	UT_RUN(consumer_v1_exact_canonical_owner_projects_self);
	UT_RUN(consumer_fullxid_order_survives_raw_wrap);
	UT_RUN(consumer_complete_cap_and_no_partial_capacity_success);
	UT_RUN(consumer_discarded_active_does_not_conflict_or_rewrite_top);
	UT_RUN(consumer_live_child_waits_on_canonical_top);
	UT_RUN(consumer_unknown_member_poison_is_whole_set);
	UT_RUN(consumer_exact_mismatch_never_becomes_discarded_success);
	UT_RUN(consumer_live_grant_required_even_for_compatible_lock);
	UT_RUN(consumer_only_exact_owner_projects_self_and_applies_cid);
	UT_RUN(consumer_raw_owner_collision_without_full_binding_fails_closed);
	UT_RUN(consumer_discarded_self_does_not_trigger_self_modified);
	UT_RUN(consumer_recompose_preserves_raw_survivors_and_drops_only_effect);
	UT_RUN(consumer_committed_updater_live_denied_discarded_allowed);
	UT_RUN(consumer_v1_and_exact_mix_keep_original_top_only_contract);
	UT_RUN(consumer_failure_clears_complete_recompose_output);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
