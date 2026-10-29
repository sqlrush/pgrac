/*-------------------------------------------------------------------------
 * test_cluster_native_unarmed_quiet.c
 *   Exercise exact cancellation without a published disk image receipt.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_unarmed_quiet.c
 * NOTES
 *   Actual native, IO, codec, PCM and LMS bodies run. Process, storage and
 *   error-event leaves are controlled; no usable image is synthesized.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_quiet_notice_fixture.inc"

#define UA_SEALING UINT8_C(13)
#define UA_QUIET UINT8_C(14)
#define UA_CONSUMED UINT8_C(15)
#define UA_OUTCOME UINT8_C(14)
#define UA_RETIRED UINT8_C(15)
static unsigned ua_abort_calls, ua_aborts, ua_wakes;
static bool ua_seal_error, ua_before_abort_error, ua_after_abort_error, ua_publish_error;
static bool ua_seal_refused;

static void
ua_broadcast(ConditionVariable *cv)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, UA_SEALING);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0),0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0),0);
	UT_ASSERT_EQ(request_buf.pcm_state,PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type,BUF_TYPE_CURRENT);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner,1));
	ua_wakes++; ConditionVariableBroadcast(cv);
}
#define TerminateBufferIO ua_actual_terminate
#define ConditionVariableBroadcast ua_broadcast
#define LockBufHdr stage_header_lock
#include "test_cluster_native_disk_terminate.inc"
#undef LockBufHdr
#undef ConditionVariableBroadcast
#undef TerminateBufferIO
#define GetBufferDescriptor(id) quiet_descriptor(id)
#define relpathperm(locator,forknum) quiet_wrong_output()
#define pfree(pointer) quiet_wrong_free(pointer)
#define LockBufHdr stage_header_lock
#define TerminateBufferIO ua_actual_terminate
#define AbortBufferIO ua_actual_abort
#include "test_cluster_native_disk_abort.inc"
#undef AbortBufferIO
#undef TerminateBufferIO
#undef LockBufHdr
#undef pfree
#undef relpathperm
#undef GetBufferDescriptor

#ifndef UNARMED_QUIET_RED
static void
ua_abort(Buffer buffer)
{
	ua_abort_calls++;
	if(ua_before_abort_error) pg_re_throw();
	ua_actual_abort(buffer); ua_aborts++;
	if(ua_after_abort_error) pg_re_throw();
}
static ResourceXApplyResult
ua_seal(const ResourceXDeliveryClaim *execution,const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt,const ClusterPcmOwnSnapshot *native)
{
	ResourceXApplyResult result;
	if(ua_seal_refused)return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	result=cluster_pcm_lock_resource_x_requester_acquire_unarmed_seal_exact(execution,episode,receipt,native);
	if(ua_seal_error&&result==RESOURCE_X_APPLY_APPLIED)pg_re_throw();
	return result;
}
static ResourceXApplyResult
ua_publish(const ResourceXDeliveryClaim *execution,const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt,const ClusterPcmOwnSnapshot *native)
{
	ResourceXApplyResult result=cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact(execution,episode,receipt,native);
	if(ua_publish_error&&result==RESOURCE_X_APPLY_APPLIED)pg_re_throw();
	return result;
}
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define AbortBufferIO ua_abort
#define cluster_pcm_lock_resource_x_requester_acquire_unarmed_seal_exact ua_seal
#define cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact ua_publish
#include "test_cluster_native_unarmed_quiet.inc"
#undef cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact
#undef cluster_pcm_lock_resource_x_requester_acquire_unarmed_seal_exact
#undef AbortBufferIO
#undef GetPrivateRefCount
#undef LockBufHdr
#endif

static bool
ua_start(EvidenceFixture *f,unsigned cut)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];uint16 len;ResourceXWireReject reject;
	unsigned char *memory=(void *)fake_pcm_header.data;Size offset=offsetof(ClusterPcmResourceXMasterState,requester_join.grant_payload),found=0;
	if(!complete_start(f))return false;
	ua_abort_calls=ua_aborts=ua_wakes=0;
	ua_seal_error=ua_before_abort_error=ua_after_abort_error=ua_publish_error=ua_seal_refused=false;
	quiet_retires=0;quiet_retire_refused=false;quiet_on_content=NULL;
	armed_capability=prepared_claim;
	if(cut==1&&disk_run()!=RESOURCE_X_APPLY_APPLIED)return false;
	if(cut==2&&stage_disk_run()!=RESOURCE_X_APPLY_APPLIED)return false;
	if(cut==3)memset(disk_shared.data,0xA7,sizeof(disk_shared));
	if(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,&input_grant,bytes,sizeof(bytes),&len,&reject))return false;
	for(Size n=offset%sizeof(uint64);n+len<=fake_pcm_header_requested_size;n+=sizeof(uint64))
		if(memcmp(memory+n,bytes,len)==0){qn_state=(void *)(memory+n-offset);found++;}
	UT_ASSERT_EQ(found,1);if(found!=1)return false;
	qn_intent=&qn_state->requester_settlement_intent;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,4);
	return true;
}
static ResourceXApplyResult
ua_run(void)
{
#ifdef UNARMED_QUIET_RED
	return cluster_bufmgr_pcm_own_acquire_quiet_disk_exact(&request_buf,&request_base,&prepared_episode,&armed_capability);
#else
	return cluster_bufmgr_pcm_own_acquire_quiet_unarmed_disk_exact(&request_buf,&request_base,&prepared_episode,&armed_capability);
#endif
}
static bool
ua_catches(void)
{
	volatile bool caught=false;
	PG_TRY();{(void)ua_run();}PG_CATCH();{caught=true;}PG_END_TRY();return caught;
}
static void
ua_done(unsigned reads)
{
	const ResourceXAcquireReadReceipt *r=&request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,UA_QUIET);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0),request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0),0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0),0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state)&(BM_VALID|BM_IO_IN_PROGRESS|BM_IO_ERROR),BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner,1));
	UT_ASSERT_EQ(r->authority_generation,input_grant.common.authority_generation);
	UT_ASSERT_EQ(r->decision_crc32c,input_grant.common.semantic_crc32c);
	UT_ASSERT_EQ(r->page_lsn,0);UT_ASSERT_EQ(r->page_scn,0);UT_ASSERT_EQ(r->page_crc32c,0);
	UT_ASSERT_EQ(ua_aborts,1);UT_ASSERT_EQ(ua_wakes,1);UT_ASSERT_EQ(quiet_retires,1);
	UT_ASSERT_EQ(complete_commits,0);UT_ASSERT_EQ(complete_io_calls,0);UT_ASSERT_EQ(disk_reads,reads);complete_idle();
}
static void
ua_refused(void)
{
	ClusterPcmOwnEntry own=request_own;ClusterPcmResourceXBootstrapRound round=request_entry()->resource_x_bootstrap_round;
	uint32 state=pg_atomic_read_u32(&request_buf.state);unsigned calls=ua_abort_calls;
	ResourceXApplyResult result=ua_run();UT_ASSERT(result!=RESOURCE_X_APPLY_APPLIED&&result!=RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&own,&request_own,sizeof(own))==0);UT_ASSERT(memcmp(&round,&request_entry()->resource_x_bootstrap_round,sizeof(round))==0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state),state);UT_ASSERT_EQ(ua_abort_calls,calls);complete_idle();
}
static ResourceXDecodedFrame
ua_frame(bool ack)
{
	ResourceXDecodedFrame frame=done_frame();
	frame.common.outcome=ack?UA_RETIRED:UA_OUTCOME;
	frame.common.action_node=ack?frame.body.acquire_completion.decision.master_node:cluster_node_id;
	frame.common.sender_connection_generation=71;
	frame.body.acquire_completion.native_generation=prepared_episode.base_generation;
	return frame;
}
static ResourceXApplyResult
ua_ack(void)
{
	ResourceXDecodedFrame ack=ua_frame(true);
	if(!qn_wire(&ack))return RESOURCE_X_APPLY_INVALID;
	return settle_gcs(&ack,NULL);
}

UT_TEST(test_unarmed_unread_private_and_shared_bytes_are_never_published)
{
	for(unsigned cut=0;cut<4;cut++){
		EvidenceFixture f;UT_ASSERT(ua_start(&f,cut));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(cut==1||cut==2?1:0);
	}
}
UT_TEST(test_unarmed_exact_duplicate_never_repeats_abort_or_wakeup)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);
	for(int n=0;n<5;n++)UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_DUPLICATE);ua_done(0);
}
UT_TEST(test_unarmed_seal_precedes_native_release_and_survives_ERROR)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));ua_seal_error=true;UT_ASSERT(ua_catches());ua_seal_error=false;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,UA_SEALING);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0),PCM_OWN_FLAG_GRANT_PENDING);UT_ASSERT_EQ(ua_abort_calls,0);
	UT_ASSERT(disk_run()!=RESOURCE_X_APPLY_APPLIED);UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(0);
}
UT_TEST(test_unarmed_seal_refusal_does_not_drop_native_or_IO)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));ua_seal_refused=true;ua_refused();
}
UT_TEST(test_unarmed_ERROR_after_GP_retirement_keeps_own_IO_until_resumed)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));ua_before_abort_error=true;UT_ASSERT(ua_catches());ua_before_abort_error=false;
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0),0);UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0),0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner,1));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(0);
}
UT_TEST(test_unarmed_ERROR_after_native_IO_does_not_repeat_native_abort)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));ua_after_abort_error=true;UT_ASSERT(ua_catches());ua_after_abort_error=false;
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner,1));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(0);
}
UT_TEST(test_unarmed_ERROR_after_shared_quiet_replays_only_original_receipt)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));ua_publish_error=true;UT_ASSERT(ua_catches());ua_publish_error=false;
	UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_DUPLICATE);ua_done(0);
}
UT_TEST(test_unarmed_cancel_and_admission_loss_cannot_erase_local_cleanup)
{
	for(int n=0;n<3;n++){
		EvidenceFixture f;ResourceXGateSnapshot gate;UT_ASSERT(ua_start(&f,0));
		if(n==0)disk_cancel();if(n==1)request_admission.entered=false;
		if(n==2){UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate),RESOURCE_X_APPLY_APPLIED);}
		UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(0);
	}
}
UT_TEST(test_unarmed_foreign_or_future_E_and_authority_preserve_all_debt)
{
	for(int n=0;n<9;n++){
		EvidenceFixture f;UT_ASSERT(ua_start(&f,0));
		if(n==0)armed_capability.executor_sequence++;
		if(n==1)armed_capability.observation.entry_binding_generation++;
		if(n==2)armed_capability.observation.request.assertion_sequence++;
		if(n==3)prepared_episode.backend_generation++;
		if(n==4)prepared_episode.reservation_token++;
		if(n==5)armed_capability.observation.r4_record_generation++;
		if(n==6)request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor=2;
		if(n==7)request_entry()->resource_x_bootstrap_round.failed_attempt_floor=1;
		if(n==8)qn_state->requester_join.grant_payload[64]^=1; /* retained A must decode exactly */
		ua_refused();
	}
}
UT_TEST(test_unarmed_original_native_write_IO_hold_and_shape_required)
{
	for(int n=0;n<7;n++){
		EvidenceFixture f;UT_ASSERT(ua_start(&f,0));
		if(n==0)pg_atomic_write_u64(&request_own.generation,99);
		if(n==1)pg_atomic_write_u64(&request_own.reservation_token,99);
		if(n==2)pg_atomic_write_u64(&request_own.delivery_attempt,99);
		if(n==3)pg_atomic_write_u64(&request_own.write_io_generation,1);
		if(n==4)pg_atomic_write_u32(&request_buf.state,pg_atomic_read_u32(&request_buf.state)|BM_VALID);
		if(n==5)request_buf.pcm_state=PCM_STATE_S;
		if(n==6)request_buf.buffer_type=BUF_TYPE_PI;
		ua_refused();
	}
}
UT_TEST(test_unarmed_original_process_pin_and_RO_IO_required)
{
	for(int n=0;n<5;n++){
		EvidenceFixture f;PGPROC *proc;ResourceOwner owner;int pid;uint64 generation;UT_ASSERT(ua_start(&f,0));proc=MyProc;owner=CurrentResourceOwner;pid=MyProcPid;generation=MyProc->cluster_grd_generation;
		if(n==0)MyProcPid++;
		if(n==1)CurrentResourceOwner=ResourceOwnerCreate(NULL,"other-unarmed");
		if(n==2)ResourceOwnerForgetBufferIO(owner,1);
		if(n==3)MyProc->cluster_grd_generation++;
		if(n==4)request_pins=0;
		ua_refused();MyProc=proc;MyProc->cluster_grd_generation=generation;MyProcPid=pid;CurrentResourceOwner=owner;
	}
}
UT_TEST(test_unarmed_missing_GP_retirement_keeps_sealed_owner_and_IO)
{
	EvidenceFixture f;UT_ASSERT(ua_start(&f,0));quiet_retire_refused=true;
	UT_ASSERT(ua_run()!=RESOURCE_X_APPLY_APPLIED);UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,UA_SEALING);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner,1));UT_ASSERT_EQ(ua_abort_calls,0);
	quiet_retire_refused=false;UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);ua_done(0);
}
UT_TEST(test_unarmed_installed_C_or_verified_ARMED_is_not_reclassified_no_image)
{
	for(int n=0;n<2;n++){
		EvidenceFixture f;UT_ASSERT(quiet_start(&f,n));armed_capability=prepared_claim;ua_refused();
	}
}
UT_TEST(test_unarmed_quiet_uses_existing_LMS_owner_and_explicit_no_image_format)
{
	EvidenceFixture f;ResourceXIntentSlot slot;ResourceXDecodedFrame frame;UT_ASSERT(ua_start(&f,0));
	UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);UT_ASSERT(qn_probe(&slot));UT_ASSERT_EQ(qn_intent->payload[0],9);
	frame=ua_frame(false);UT_ASSERT(qn_wire(&frame));UT_ASSERT_EQ(frame.body.acquire_completion.page_scn,0);
	UT_ASSERT_EQ(slot.body.owner_kind,RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);ua_done(0);
}
UT_TEST(test_unarmed_exact_RETIRED_before_callback_waits_then_retires_original)
{
	EvidenceFixture f;ResourceXIntentSlot slot;UT_ASSERT(ua_start(&f,0));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);
	slot=qn_stage();UT_ASSERT_EQ(ua_ack(),RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,UA_CONSUMED);
	UT_ASSERT_EQ(qn_intent->slot.state,RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot,true,++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent,0);UT_ASSERT_EQ(ua_aborts,1);
}
UT_TEST(test_unarmed_callback_then_ACK_retires_without_touching_successor_native)
{
	EvidenceFixture f;ResourceXIntentSlot slot;ClusterPcmOwnEntry own;uint32 state;
	UT_ASSERT(ua_start(&f,0));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);disk_turnover();own=request_own;state=pg_atomic_read_u32(&request_buf.state);
	slot=qn_stage();UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot,true,++fake_pcm_clock_us));
	UT_ASSERT_EQ(ua_ack(),RESOURCE_X_APPLY_APPLIED);UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent,0);
	UT_ASSERT(memcmp(&own,&request_own,sizeof(own))==0);UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state),state);
}
UT_TEST(test_unarmed_normal_verified_quiet_ACK_cannot_retire_no_image_receipt)
{
	EvidenceFixture f;ResourceXIntentSlot slot;ResourceXDecodedFrame ack;UT_ASSERT(ua_start(&f,0));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(qn_probe(&slot));ack=ua_frame(true);ack.common.outcome=QN_RETIRED;
	UT_ASSERT(!qn_wire(&ack));UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,UA_QUIET);
}
UT_TEST(test_unarmed_full_no_image_receipt_rejects_fabricated_byte_evidence)
{
	for(int n=0;n<3;n++){
		EvidenceFixture f;ResourceXIntentSlot slot;UT_ASSERT(ua_start(&f,0));UT_ASSERT_EQ(ua_run(),RESOURCE_X_APPLY_APPLIED);
		if(n==0)request_entry()->resource_x_bootstrap_round.acquire_read_receipt.page_lsn=1;
		if(n==1)request_entry()->resource_x_bootstrap_round.acquire_read_receipt.page_scn=93;
		if(n==2)request_entry()->resource_x_bootstrap_round.acquire_read_receipt.page_crc32c=1;
		UT_ASSERT(!qn_probe(&slot));UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent,1);
	}
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_unarmed_unread_private_and_shared_bytes_are_never_published);
	UT_RUN(test_unarmed_exact_duplicate_never_repeats_abort_or_wakeup);
	UT_RUN(test_unarmed_seal_precedes_native_release_and_survives_ERROR);
	UT_RUN(test_unarmed_seal_refusal_does_not_drop_native_or_IO);
	UT_RUN(test_unarmed_ERROR_after_GP_retirement_keeps_own_IO_until_resumed);
	UT_RUN(test_unarmed_ERROR_after_native_IO_does_not_repeat_native_abort);
	UT_RUN(test_unarmed_ERROR_after_shared_quiet_replays_only_original_receipt);
	UT_RUN(test_unarmed_cancel_and_admission_loss_cannot_erase_local_cleanup);
	UT_RUN(test_unarmed_foreign_or_future_E_and_authority_preserve_all_debt);
	UT_RUN(test_unarmed_original_native_write_IO_hold_and_shape_required);
	UT_RUN(test_unarmed_original_process_pin_and_RO_IO_required);
	UT_RUN(test_unarmed_missing_GP_retirement_keeps_sealed_owner_and_IO);
	UT_RUN(test_unarmed_installed_C_or_verified_ARMED_is_not_reclassified_no_image);
	UT_RUN(test_unarmed_quiet_uses_existing_LMS_owner_and_explicit_no_image_format);
	UT_RUN(test_unarmed_exact_RETIRED_before_callback_waits_then_retires_original);
	UT_RUN(test_unarmed_callback_then_ACK_retires_without_touching_successor_native);
	UT_RUN(test_unarmed_normal_verified_quiet_ACK_cannot_retire_no_image_receipt);
	UT_RUN(test_unarmed_full_no_image_receipt_rejects_fabricated_byte_evidence);
	UT_DONE();return ut_failed_count?1:0;
}
