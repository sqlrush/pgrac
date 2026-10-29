/*-------------------------------------------------------------------------
 * test_cluster_resource_x_master_quiet.c
 *   Exercise original master quiet consumption and physical reverse owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_resource_x_master_quiet.c
 * NOTES
 *   Codec, admission, master, transport and LMS bodies are real. Original
 *   requester quiet evidence is explicit authenticated peer input here.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_resource_x_master_consumed_fixture.inc"

#define MQ_QUIET UINT8_C(12)
#define MQ_RETIRED UINT8_C(13)

static bool
mq_start(PublishFixture *f, ResourceXDecodedFrame *quiet)
{
	if (!master_done_start(f, quiet)) return false;
	quiet->common.outcome = MQ_QUIET;
	quiet->body.acquire_completion.native_generation = quiet->body.acquire_completion.decision.episode.base_generation;
	return master_done_round_trip(quiet);
}

static bool
mq_consumed(PublishFixture *f, ResourceXDecodedFrame *quiet)
{
	if (!mq_start(f, quiet)) return false;
	master_done_send(f);
	UT_ASSERT_EQ(master_done_receive(quiet), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f->state->deferred_settlement.present, 3);
	return f->state->deferred_settlement.present == 3;
}

static void
mq_directory(PublishFixture *f)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ResourceXHolderGrantProvenance empty_holder;
	struct StopPcmEntryLayout *directory = (void *)fake_pcm_entries.data[0];
	int node = f->request.common.logical_assertion.requester_node;
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->master_state), PCM_STATE_N);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->s_holders_bitmap), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&directory->pi_holders_bitmap), 0);
	UT_ASSERT(memcmp(&f->state->holder_grants[node], &empty_holder, sizeof(empty_holder)) == 0);
	UT_ASSERT(memcmp(&f->state->requests[node], &empty_request, sizeof(empty_request)) == 0);
	UT_ASSERT_EQ(f->state->bootstrap_priority[node].state, RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY);
	UT_ASSERT_EQ(f->state->bootstrap_receipts[node].highest_attempt_floor, f->request.common.assertion_sequence);
	UT_ASSERT_EQ(f->state->authority_generation, f->plan.proposed_authority_generation);
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT_EQ(master_durable_admission_depth, 0);
}

UT_TEST(test_master_quiet_consumes_only_original_request_without_S_grant)
{
	PublishFixture f; ResourceXDecodedFrame quiet, ack;
	if (!mq_consumed(&f, &quiet)) return;
	mq_directory(&f); ack = consumed_decode(&f);
	UT_ASSERT_EQ(f.state->grant_intent.payload[0], 8);
	UT_ASSERT_EQ(ack.common.outcome, MQ_RETIRED);
	UT_ASSERT_EQ(ack.common.action_node, cluster_node_id);
	UT_ASSERT(memcmp(&ack.body.acquire_completion, &quiet.body.acquire_completion, sizeof(quiet.body.acquire_completion)) == 0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_MASTER_GRANT);
}
UT_TEST(test_master_quiet_early_receipt_waits_original_GRANT_callback)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ResourceXIntentSlot grant;
	if (!mq_start(&f, &quiet)) return;
	grant = master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&quiet), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&grant, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3); mq_directory(&f);
}
UT_TEST(test_master_quiet_duplicate_retains_one_exact_reverse_notice)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ClusterPcmResourceXMasterState before;
	if (!mq_consumed(&f, &quiet)) return;
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&quiet), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_master_quiet_NOT_ADMITTED_keeps_full_body_and_independent_receipt)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ClusterPcmResourceXDeferredSettlement receipt;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; ResourceXIntentSlot slot;
	if (!mq_consumed(&f, &quiet)) return;
	receipt=f.state->deferred_settlement; slot=f.state->grant_intent.slot;
	memcpy(bytes,f.state->grant_intent.payload,sizeof(bytes));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&slot,++fake_pcm_clock_us),RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT(memcmp(bytes,f.state->grant_intent.payload,sizeof(bytes))==0);
	UT_ASSERT(memcmp(&receipt,&f.state->deferred_settlement,sizeof(receipt))==0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state,RESOURCE_X_INTENT_SLOT_ARMED);
}
UT_TEST(test_master_quiet_callback_failure_rearms_and_late_callback_is_stale)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ResourceXIntentSlot a,b;
	if (!mq_consumed(&f,&quiet)) return;
	a=master_done_stage(&f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a,false,++fake_pcm_clock_us));
	b=master_done_stage(&f); UT_ASSERT(b.send_episode>a.send_episode);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a,true,++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present,3);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&b,true,++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present,0); mq_directory(&f);
}
UT_TEST(test_master_quiet_generic_completion_cannot_drop_reverse_receipt)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ResourceXIntentSlot slot; ClusterPcmResourceXMasterState before;
	if (!mq_consumed(&f,&quiet)) return;
	slot=master_done_stage(&f); before=*f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&slot));
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
}
UT_TEST(test_master_quiet_actual_LMS_owns_reverse_send_and_receipt_until_callback)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ResourceXIntentSlot offered,staged;
	if (!mq_consumed(&f,&quiet)) return;
	consumed_ring_reset(); offered=f.state->grant_intent.slot;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0,&offered,61,9999999),CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count,1);
	memcpy(&staged,consumed_ring_shared.rings[0].ring[0].payload,sizeof(staged));
	UT_ASSERT(staged.send_episode!=0); UT_ASSERT_EQ(staged.state,RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present,3);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged,true,++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present,0); mq_directory(&f);
}
UT_TEST(test_master_quiet_actual_LMS_full_preserves_original_debt)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ClusterPcmResourceXMasterState before;
	if (!mq_consumed(&f,&quiet)) return;
	consumed_ring_reset(); consumed_ring_shared.rings[0].count=PGRAC_LMS_OUTBOUND_CAPACITY; before=*f.state;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0,&f.state->grant_intent.slot,61,9999999),CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
}
UT_TEST(test_master_quiet_wrong_full_identity_does_not_consume_canonical_A)
{
	for(int n=0;n<8;n++) {
		PublishFixture f; ResourceXDecodedFrame quiet;
		if (!mq_start(&f,&quiet)) return;
		master_done_send(&f);
		if(n==0)quiet.body.acquire_completion.decision.episode.backend_generation++;
		if(n==1)quiet.body.acquire_completion.decision.episode.reservation_token++;
		if(n==2)quiet.body.acquire_completion.decision.resource_binding++;
		if(n==3)quiet.body.acquire_completion.decision.master_ingress_generation++;
		if(n==4)quiet.body.acquire_completion.decision.requester_ingress_generation++;
		if(n==5)quiet.body.acquire_completion.decision_crc32c++;
		if(n==6)quiet.common.authority_generation++;
		if(n==7)quiet.body.acquire_completion.native_generation++;
		master_done_refuse(&f,&quiet);
	}
}
UT_TEST(test_master_quiet_wrong_sender_direction_or_live_domain_refused)
{
	PublishFixture f; ResourceXDecodedFrame quiet,ack; ClusterPcmResourceXMasterState before;
	if (!mq_start(&f,&quiet)) return;
	master_done_send(&f); before=*f.state;
	UT_ASSERT(master_done_direct(&quiet,2,61,61,77)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(master_done_direct(&quiet,1,62,61,77)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(master_done_direct(&quiet,1,61,62,77)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(master_done_direct(&quiet,1,61,61,78)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
	UT_ASSERT_EQ(master_done_receive(&quiet),RESOURCE_X_APPLY_APPLIED);
	ack=consumed_decode(&f); before=*f.state;
	UT_ASSERT_EQ(master_done_receive(&ack),RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
}
UT_TEST(test_master_quiet_changed_independent_receipt_blocks_callback)
{
	PublishFixture f; ResourceXDecodedFrame quiet; ResourceXIntentSlot slot; ClusterPcmResourceXMasterState before;
	if (!mq_consumed(&f,&quiet)) return;
	slot=master_done_stage(&f); f.state->deferred_settlement.payload[260]^=1; before=*f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot,true,++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
}
UT_TEST(test_master_quiet_old_callback_and_receipt_cannot_clear_successor)
{
	PublishFixture f; ResourceXDecodedFrame quiet,next,ack; ResourceXBootstrapWork work;
	ResourceXIntentSlot old; ClusterPcmResourceXMasterState before;
	if (!mq_consumed(&f,&quiet)) return;
	old=master_done_stage(&f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old,true,++fake_pcm_clock_us));
	next=acquire_frame(2,RESOURCE_X_ACQUIRE_FIRST_FILL,PCM_STATE_N,PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next),RESOURCE_X_APPLY_APPLIED); work=bootstrap_next_work();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work,&ack),RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work,NULL),RESOURCE_X_APPLY_APPLIED);
	before=*f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old,true,++fake_pcm_clock_us));
	UT_ASSERT(master_done_receive(&quiet)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before,f.state,sizeof(before))==0);
}
UT_TEST(test_master_quiet_codec_exact_versions_length_and_legacy_rejection)
{
	for(int ack=0;ack<2;ack++) {
		PublishFixture f; ResourceXDecodedFrame quiet,out; ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES+1]; uint16 len=0;
		if(!mq_start(&f,&quiet))return;
		if(ack){quiet.common.outcome=MQ_RETIRED;quiet.common.action_node=0;}
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,&quiet,bytes,sizeof(bytes),&len,&reject));
		UT_ASSERT_EQ(len,RESOURCE_X_COMPLETE_V5_BYTES); UT_ASSERT_EQ(bytes[0],ack?8:7);
		UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len,&out,&reject));
		for(int n=0;n<len;n++)UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,n,&out,&reject));
		UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len+1,&out,&reject));
		bytes[0]=5; UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len,&out,&reject));
		bytes[0]=6; UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len,&out,&reject));
	}
}
UT_TEST(test_master_quiet_codec_refuses_installed_generation_and_wrong_role)
{
	for(int n=0;n<6;n++) {
		PublishFixture f; ResourceXDecodedFrame quiet; ResourceXWireReject reject;
		uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];uint16 len=0;
		if(!mq_start(&f,&quiet))return;
		if(n==0)quiet.body.acquire_completion.native_generation++;
		if(n==1)quiet.body.acquire_completion.write_io_generation|=1;
		if(n==2)quiet.common.action_node=0;
		if(n==3){quiet.common.outcome=MQ_RETIRED;quiet.common.action_node=1;}
		if(n==4)quiet.body.acquire_completion.decision.source_node=2;
		if(n==5){quiet.has_acquire_decision=false;quiet.kind=RESOURCE_X_WIRE_INSTALL_SETTLEMENT;}
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,&quiet,bytes,sizeof(bytes),&len,&reject));
	}
}
UT_TEST(test_master_quiet_rebind_keeps_E_A_I_and_quiet_version)
{
	PublishFixture f; ResourceXDecodedFrame quiet,out; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];uint16 len=0;
	if(!mq_start(&f,&quiet))return;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,&quiet,bytes,sizeof(bytes),&len,&reject));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len,91,&reject));
	UT_ASSERT_EQ(bytes[0],7);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,bytes,len,&out,&reject));
	UT_ASSERT_EQ(out.common.sender_connection_generation,91);
	UT_ASSERT(memcmp(&out.body.acquire_completion,&quiet.body.acquire_completion,sizeof(quiet.body.acquire_completion))==0);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_master_quiet_consumes_only_original_request_without_S_grant);
	UT_RUN(test_master_quiet_early_receipt_waits_original_GRANT_callback);
	UT_RUN(test_master_quiet_duplicate_retains_one_exact_reverse_notice);
	UT_RUN(test_master_quiet_NOT_ADMITTED_keeps_full_body_and_independent_receipt);
	UT_RUN(test_master_quiet_callback_failure_rearms_and_late_callback_is_stale);
	UT_RUN(test_master_quiet_generic_completion_cannot_drop_reverse_receipt);
	UT_RUN(test_master_quiet_actual_LMS_owns_reverse_send_and_receipt_until_callback);
	UT_RUN(test_master_quiet_actual_LMS_full_preserves_original_debt);
	UT_RUN(test_master_quiet_wrong_full_identity_does_not_consume_canonical_A);
	UT_RUN(test_master_quiet_wrong_sender_direction_or_live_domain_refused);
	UT_RUN(test_master_quiet_changed_independent_receipt_blocks_callback);
	UT_RUN(test_master_quiet_old_callback_and_receipt_cannot_clear_successor);
	UT_RUN(test_master_quiet_codec_exact_versions_length_and_legacy_rejection);
	UT_RUN(test_master_quiet_codec_refuses_installed_generation_and_wrong_role);
	UT_RUN(test_master_quiet_rebind_keeps_E_A_I_and_quiet_version);
	UT_DONE(); return ut_failed_count?1:0;
}
