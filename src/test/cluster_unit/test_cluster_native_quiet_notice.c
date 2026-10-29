/*-------------------------------------------------------------------------
 * test_cluster_native_quiet_notice.c
 *   Exercise retained original quiet evidence through actual send/ACK owners.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_quiet_notice.c
 * NOTES
 *   Native cleanup, codec, PCM, LMS admission and receiver bodies are real.
 *   Peer ACK input and local process/event leaves are controlled.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_quiet_fixture.inc"

#define QN_QUIET UINT8_C(12)
#define QN_RETIRED UINT8_C(13)
#define QN_CONSUMED UINT8_C(12)
static ClusterPcmResourceXMasterState *qn_state;
static ClusterPcmResourceXReadIntent *qn_intent;

static bool
qn_start(EvidenceFixture *f)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 len; ResourceXWireReject reject;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload), found = 0;
	if (!quiet_start(f, 0) || quiet_run() != RESOURCE_X_APPLY_APPLIED) return false;
	quiet_done();
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
		bytes, sizeof(bytes), &len, &reject)) return false;
	for (Size n = offset % sizeof(uint64); n + len <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, bytes, len) == 0) { qn_state = (void *)(memory + n - offset); found++; }
	UT_ASSERT_EQ(found, 1);
	if (found != 1) return false;
	qn_intent = &qn_state->requester_settlement_intent;
	return true;
}

static bool
qn_probe(ResourceXIntentSlot *slot)
{
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
	fake_pcm_clock_us += 1000000;
	return cluster_pcm_lock_resource_x_ready_intent_probe_exact(&input_grant.common.logical_assertion.resource,
		&cursor, slot) == RESOURCE_X_INTENT_PROBE_FOUND;
}

static bool
qn_global_probe(ResourceXIntentSlot *slot)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint32 examined; ResourceXAcquisitionRef delivery;
	ResourceXIntentProbeResult result;
	fake_pcm_clock_us += 1000000;
	for (int n = 0; n < 8; n++) {
		result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(4, slot, bytes, sizeof(bytes), &examined, &delivery);
		if (result == RESOURCE_X_INTENT_PROBE_FOUND) return true;
		if (result == RESOURCE_X_INTENT_PROBE_CORRUPT) break;
	}
	return false;
}

static ResourceXDecodedFrame
qn_frame(bool ack)
{
	ResourceXDecodedFrame frame = done_frame();
	frame.common.outcome = ack ? QN_RETIRED : QN_QUIET;
	frame.common.action_node = ack ? frame.body.acquire_completion.decision.master_node : cluster_node_id;
	frame.common.sender_connection_generation = 71;
	frame.body.acquire_completion.native_generation = prepared_episode.base_generation;
	return frame;
}

static bool
qn_wire(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len; ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, frame, bytes, sizeof(bytes), &len, &reject)
		&& cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, len, frame, &reject);
}

static ResourceXApplyResult
qn_ack(void)
{
	ResourceXDecodedFrame frame = qn_frame(true);
	if (!qn_wire(&frame)) return RESOURCE_X_APPLY_INVALID;
	return settle_gcs(&frame, NULL);
}

static ResourceXIntentSlot
qn_stage(void)
{
	ResourceXIntentSlot out = {0}, original = {0};
	UT_ASSERT(qn_probe(&original));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&original, ++fake_pcm_clock_us, &out), RESOURCE_X_INTENT_STAGED);
	return out;
}

static void
qn_retired(void)
{
	static const ResourceXAcquireReadReceipt empty_receipt;
	static const ClusterPcmResourceXReadIntent empty_intent;
	ClusterPcmResourceXBootstrapRound *round = &request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(round->has_acquire_intent, 0); UT_ASSERT_EQ(round->delivery_executor_purpose, 0);
	UT_ASSERT_EQ(round->delivery_target.buffer_id_plus_one, 0);
	UT_ASSERT_EQ(round->highest_attempt_floor, prepared_claim.observation.request.assertion_sequence);
	UT_ASSERT_EQ(round->cancelled_attempt_floor, round->highest_attempt_floor);
	UT_ASSERT(memcmp(&round->acquire_read_receipt, &empty_receipt, sizeof(empty_receipt)) == 0);
	UT_ASSERT(memcmp(qn_intent, &empty_intent, sizeof(empty_intent)) == 0);
	UT_ASSERT_EQ(quiet_aborts, 1); UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(disk_reads, 1); complete_idle();
}

UT_TEST(test_quiet_notice_original_passive_probe_arms_only_original_full_E_A_I)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame frame;
	UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(qn_intent->payload[0], 7);
	frame = qn_frame(false); UT_ASSERT(qn_wire(&frame));
	UT_ASSERT_EQ(slot.destination_node, frame.body.acquire_completion.decision.master_node);
	UT_ASSERT_EQ(slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(slot.logical_generation, frame.common.assertion_sequence);
	UT_ASSERT_EQ(slot.authority_generation, frame.common.authority_generation);
}
UT_TEST(test_quiet_notice_original_global_LMS_scan_finds_debt_without_backend)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	PGPROC *proc; int pid; ResourceOwner owner;
	UT_ASSERT(qn_start(&f)); proc = MyProc; pid = MyProcPid; owner = CurrentResourceOwner;
	MyProc = NULL; MyProcPid = 0; CurrentResourceOwner = NULL;
	UT_ASSERT(qn_global_probe(&slot)); UT_ASSERT_EQ(slot.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(quiet_aborts, 1); UT_ASSERT_EQ(disk_reads, 1);
	MyProc = proc; MyProcPid = pid; CurrentResourceOwner = owner;
}
UT_TEST(test_quiet_notice_transport_completion_does_not_consume_receipt)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(qn_start(&f)); slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 11);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}
UT_TEST(test_quiet_notice_callback_before_authenticated_ACK_retires_original)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(qn_start(&f)); slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(qn_ack(), RESOURCE_X_APPLY_APPLIED); qn_retired();
}
UT_TEST(test_quiet_notice_ACK_before_callback_keeps_STAGED_then_retires)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(qn_start(&f)); slot = qn_stage();
	UT_ASSERT_EQ(qn_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, QN_CONSUMED);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us)); qn_retired();
}
UT_TEST(test_quiet_notice_exact_ACK_retires_unsent_ARMED_without_faking_send)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(qn_ack(), RESOURCE_X_APPLY_APPLIED); qn_retired();
}
UT_TEST(test_quiet_notice_failed_callback_after_ACK_is_quiescent_not_resubmitted)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(qn_start(&f)); slot = qn_stage(); UT_ASSERT_EQ(qn_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, false, ++fake_pcm_clock_us)); qn_retired();
}
UT_TEST(test_quiet_notice_NOT_ADMITTED_preserves_exact_send_and_receipt)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmResourceXReadIntent before;
	UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot)); before = *qn_intent;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&slot, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT(memcmp(before.payload, qn_intent->payload, sizeof(before.payload)) == 0);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 11);
}
UT_TEST(test_quiet_notice_failure_rearms_same_body_and_late_callback_cannot_clear)
{
	EvidenceFixture f; ResourceXIntentSlot a,b; uint8 bytes[sizeof(qn_intent->payload)];
	UT_ASSERT(qn_start(&f)); a = qn_stage(); memcpy(bytes, qn_intent->payload, sizeof(bytes));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, false, ++fake_pcm_clock_us));
	b = qn_stage(); UT_ASSERT(b.send_episode > a.send_episode);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(bytes, qn_intent->payload, sizeof(bytes)) == 0);
}
UT_TEST(test_quiet_notice_generic_completion_cannot_discard_transport_owner)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmResourceXReadIntent before;
	UT_ASSERT(qn_start(&f)); slot = qn_stage(); before = *qn_intent;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&slot));
	UT_ASSERT(memcmp(&before, qn_intent, sizeof(before)) == 0);
}
UT_TEST(test_quiet_notice_same_tag_successor_native_is_not_consulted)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmOwnEntry own; uint32 state;
	UT_ASSERT(qn_start(&f)); disk_turnover(); own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT(qn_probe(&slot)); slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(qn_ack(), RESOURCE_X_APPLY_APPLIED); qn_retired();
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0); UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
}
UT_TEST(test_quiet_notice_late_ACK_after_original_retire_cannot_touch_new_round)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame ack; ClusterPcmResourceXBootstrapRound before;
	UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot)); ack = qn_frame(true); UT_ASSERT(qn_wire(&ack));
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED); qn_retired();
	request_entry()->resource_x_bootstrap_round.highest_attempt_floor++; before = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
}
UT_TEST(test_quiet_notice_wrong_ACK_full_identity_preserves_original)
{
	for (int n=0;n<8;n++) {
		EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame ack; ClusterPcmResourceXBootstrapRound before;
		UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot)); ack=qn_frame(true);
		if(n==0) ack.body.acquire_completion.decision.episode.backend_generation++;
		if(n==1) ack.body.acquire_completion.decision.episode.reservation_token++;
		if(n==2) ack.body.acquire_completion.decision.resource_binding++;
		if(n==3) ack.body.acquire_completion.page_crc32c++;
		if(n==4) ack.body.acquire_completion.decision_crc32c++;
		if(n==5) ack.common.assertion_sequence++;
		if(n==6) ack.body.acquire_completion.decision.master_ingress_generation++;
		if(n==7) ack.body.acquire_completion.decision.requester_ingress_generation++;
		before=request_entry()->resource_x_bootstrap_round;
		if(qn_wire(&ack)) UT_ASSERT(settle_receive(&ack)!=RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before,&request_entry()->resource_x_bootstrap_round,sizeof(before))==0);
	}
}
UT_TEST(test_quiet_notice_normal_C_consumed_frame_never_retires_quiet)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame ack;
	UT_ASSERT(qn_start(&f)); UT_ASSERT(qn_probe(&slot)); ack=settle_frame();
	UT_ASSERT(settle_receive(&ack)!=RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,11);
}
UT_TEST(test_quiet_notice_corrupt_receipt_or_foreign_intent_is_not_overwritten)
{
	for(int n=0;n<3;n++) {
		EvidenceFixture f; ResourceXIntentSlot slot; ClusterPcmResourceXReadIntent before;
		UT_ASSERT(qn_start(&f));
		if(n==0) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.decision_crc32c++;
		if(n==1) qn_intent->slot.state=99;
		if(n==2) qn_intent->slot.logical_generation=99;
		before=*qn_intent; UT_ASSERT(!qn_probe(&slot)); UT_ASSERT(memcmp(&before,qn_intent,sizeof(before))==0);
	}
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_quiet_notice_original_passive_probe_arms_only_original_full_E_A_I);
	UT_RUN(test_quiet_notice_original_global_LMS_scan_finds_debt_without_backend);
	UT_RUN(test_quiet_notice_transport_completion_does_not_consume_receipt);
	UT_RUN(test_quiet_notice_callback_before_authenticated_ACK_retires_original);
	UT_RUN(test_quiet_notice_ACK_before_callback_keeps_STAGED_then_retires);
	UT_RUN(test_quiet_notice_exact_ACK_retires_unsent_ARMED_without_faking_send);
	UT_RUN(test_quiet_notice_failed_callback_after_ACK_is_quiescent_not_resubmitted);
	UT_RUN(test_quiet_notice_NOT_ADMITTED_preserves_exact_send_and_receipt);
	UT_RUN(test_quiet_notice_failure_rearms_same_body_and_late_callback_cannot_clear);
	UT_RUN(test_quiet_notice_generic_completion_cannot_discard_transport_owner);
	UT_RUN(test_quiet_notice_same_tag_successor_native_is_not_consulted);
	UT_RUN(test_quiet_notice_late_ACK_after_original_retire_cannot_touch_new_round);
	UT_RUN(test_quiet_notice_wrong_ACK_full_identity_preserves_original);
	UT_RUN(test_quiet_notice_normal_C_consumed_frame_never_retires_quiet);
	UT_RUN(test_quiet_notice_corrupt_receipt_or_foreign_intent_is_not_overwritten);
	UT_DONE(); return ut_failed_count==0 ? 0 : 1;
}
