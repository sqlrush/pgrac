/* Actual native C/DONE, codec, PCM and authenticated requester ingress.
 * The reverse frame is explicit authenticated peer input, not a live peer.
 * RED uses the original evidence receiver/DONE replay as absent seams. */
#define gcs_block_resource_x_gate_session_snapshot_result settle_original_session
#define gcs_block_resource_x_gate_session_recheck_result settle_original_recheck
#include "test_cluster_native_disk_done_fixture.inc"
#undef gcs_block_resource_x_gate_session_recheck_result
#undef gcs_block_resource_x_gate_session_snapshot_result

static PcmXSessionAuthResult
gcs_block_resource_x_gate_session_snapshot_result(const BufferTag *tag,
	ResourceXGateSnapshot *gate, int32 *master, uint64 *session)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (!cluster_pcm_lock_resource_x_gate_snapshot(gate) || gate->phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	*master = cluster_gcs_lookup_master(*tag);
	*session = input_grant.common.master_session_incarnation;
	return PCM_X_SESSION_AUTH_OK;
}

/* Legacy cleanup is unrelated to typed normal completion. */
static ResourceXApplyResult
settle_unrelated_cleanup(const ResourceXDecodedFrame *frame, int32 source, uint32 ingress,
	int32 master, uint32 master_ingress, uint64 r4, ResourceXRequesterJoinSnapshot *out, void *unused)
{
	(void)frame; (void)source; (void)ingress; (void)master; (void)master_ingress;
	(void)r4; (void)out; (void)unused;
	UT_ASSERT(false);
	return RESOURCE_X_APPLY_INVALID;
}
#define gcs_block_resource_x_cleanup_frame_exact settle_unrelated_cleanup
#include "test_cluster_native_read_settle_ingress.inc"
#undef gcs_block_resource_x_cleanup_frame_exact

#define SETTLE_CONSUMED UINT8_C(8)
#define SETTLE_RETIRED UINT8_C(9)

static bool
settle_wire(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	ResourceXWireReject reject;
	bool ok = done_encode(frame, bytes);
	if (ok)
		ok = cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, sizeof(bytes), frame, &reject);
	UT_ASSERT(ok);
	return ok;
}

static ResourceXDecodedFrame
settle_frame(void)
{
	ResourceXDecodedFrame frame = done_frame();
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	frame.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
	frame.common.sender_connection_generation = 71;
	(void)settle_wire(&frame);
	return frame;
}

static ResourceXApplyResult
settle_receive_exact(const ResourceXDecodedFrame *frame, int32 source, uint32 ingress,
	int32 master, uint32 master_ingress, uint64 r4)
{
#ifdef READ_SETTLE_RED
	ResourceXRequesterJoinSnapshot out;
	return cluster_pcm_lock_resource_x_requester_join_current_exact(frame, source,
		ingress, master, master_ingress, r4, &out);
#else
	return cluster_pcm_lock_resource_x_requester_acquire_read_consumed_exact(frame,
		source, ingress, master, master_ingress, r4);
#endif
}

static ResourceXApplyResult
settle_receive(const ResourceXDecodedFrame *frame)
{
	return settle_receive_exact(frame, 0, 61, 0, 61, 77);
}

static ResourceXApplyResult
settle_run(void)
{
#ifdef READ_SETTLE_RED
	return done_run();
#else
	return cluster_bufmgr_pcm_own_acquire_settle_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim);
#endif
}

static bool
settle_start(EvidenceFixture *f, ResourceXDecodedFrame *frame)
{
	ready_peer_ok = true;
	ready_connection_generation = 61;
	bootstrap_ingress_generation = 0;
	if (!done_start(f)) return false;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	*frame = settle_frame();
	return true;
}

static ResourceXIntentSlot
settle_stage(void)
{
	ResourceXIntentSlot staged = {0};
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&done_intent->slot, ++fake_pcm_clock_us, &staged), RESOURCE_X_INTENT_STAGED);
	return staged;
}

static ResourceXApplyResult
settle_gcs(const ResourceXDecodedFrame *frame, ClusterICEnvelope *override)
{
	ResourceXRequesterJoinSnapshot out;
	ClusterICEnvelope env = {0};
	ResourceXApplyResult result;
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = RESOURCE_X_COMPLETE_V5_BYTES;
	memset(&out, 0, sizeof(out));
	result = gcs_block_resource_x_requester_join_ingress(
		override != NULL ? override : &env, frame, 61, &out);
	UT_ASSERT_EQ(out.flags & RESOURCE_X_REQUESTER_JOIN_READY, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
	return result;
}

static void
settle_once(uint8 purpose)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, purpose);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(done_state->requester_join.install_succeeded, 0);
	UT_ASSERT_EQ(done_state->requester_join.settled, 0);
	UT_ASSERT_EQ(done_state->requester_join.t_install_us, 0);
	complete_idle();
}

UT_TEST(test_consumed_ACK_retains_full_original_A_I_and_physical_notice)
{
	EvidenceFixture f; ResourceXDecodedFrame ack;
	ResourceXAcquireReadReceipt receipt; ClusterPcmResourceXReadIntent before;
	uint8 a[RESOURCE_X_DECISION_V4_BYTES]; unsigned wakes;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	receipt = request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	before = *done_intent; memcpy(a, done_state->requester_join.grant_payload, sizeof(a));
	wakes = fake_cv_broadcast_count;
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(fake_cv_broadcast_count, wakes + 1);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	UT_ASSERT(memcmp(a, done_state->requester_join.grant_payload, sizeof(a)) == 0);
	UT_ASSERT(memcmp(&receipt, &request_entry()->resource_x_bootstrap_round.acquire_read_receipt, sizeof(receipt)) == 0);
	settle_once(SETTLE_CONSUMED);
}
UT_TEST(test_consumed_quiet_ARMED_notice_retires_only_after_native_S_recheck)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot empty = {0};
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	memcpy(bytes, done_intent->payload, sizeof(bytes));
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&empty, &done_intent->slot, sizeof(empty)) == 0);
	UT_ASSERT(memcmp(bytes, done_intent->payload, sizeof(bytes)) == 0);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_no_master_receipt_is_local_wait_without_erasure)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } before = *done_intent;
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
}
UT_TEST(test_consumed_STAGED_waits_for_real_original_success_callback)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot staged;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } staged = settle_stage();
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&staged, &done_intent->slot, sizeof(staged)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_callback_before_ACK_does_not_substitute_for_master_consumption)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot staged;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } staged = settle_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_failed_physical_callback_keeps_body_until_exact_local_retirement)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot staged;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } staged = settle_stage();
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(done_intent->slot.send_episode, 1);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_exact_duplicates_never_rearm_notify_repeat_C_or_bump)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXBootstrapRound before;
	unsigned wakes;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED); wakes = fake_cv_broadcast_count;
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(fake_cv_broadcast_count, wakes);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	before = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_stale_callback_cannot_quiesce_the_original_STAGED_owner)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot staged, stale;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } staged = settle_stage();
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	stale = staged; stale.send_episode++;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&stale, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
}
UT_TEST(test_consumed_before_original_notice_cannot_forge_a_master_receipt)
{
	EvidenceFixture f; ResourceXDecodedFrame ack;
	if (!done_start(&f)) { UT_ASSERT(false); return; } ack = settle_frame();
	UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
	settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE);
}
UT_TEST(test_consumed_changed_complete_E_is_refused_even_with_valid_wire_CRC)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) ack.body.acquire_completion.decision.episode.backend_generation++;
		if (n == 1) ack.body.acquire_completion.decision.episode.requester_incarnation++;
		if (n == 2) ack.body.acquire_completion.decision.episode.reservation_token++;
		if (n == 3) { ack.body.acquire_completion.decision.episode.base_generation++;
			ack.body.acquire_completion.native_generation++; }
		if (n == 4) ack.body.acquire_completion.decision.episode.backend_id++;
		if (n == 5) ack.common.assertion_sequence++;
		if (n == 6) ack.common.logical_assertion.resource.blockNum++;
		if (!settle_wire(&ack)) continue;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_changed_complete_A_is_not_reduced_to_authority_or_CRC)
{
	for (int n = 0; n < 7; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) ack.common.authority_generation++;
		if (n == 1) ack.body.acquire_completion.decision.resource_binding++;
		if (n == 2) ack.body.acquire_completion.decision.enqueue_order++;
		if (n == 3) ack.body.acquire_completion.decision.disk_watermark_scn++;
		if (n == 4) ack.body.acquire_completion.decision.requester_ingress_generation++;
		if (n == 5) ack.body.acquire_completion.decision.master_ingress_generation++;
		if (n == 6) ack.common.master_session_incarnation++;
		if (!settle_wire(&ack)) continue;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_changed_actual_I_is_not_accepted_as_a_master_receipt)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) ack.body.acquire_completion.write_io_generation += 2;
		if (n == 1) ack.body.acquire_completion.page_lsn++;
		if (n == 2) ack.body.acquire_completion.page_scn++;
		if (n == 3) ack.body.acquire_completion.page_crc32c++;
		if (n == 4) ack.body.acquire_completion.decision_crc32c++;
		if (!settle_wire(&ack)) continue;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_noncanonical_decoded_flags_and_CRC_do_not_repair_input)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) ack.common.semantic_crc32c ^= 1;
		if (n == 1) ack.has_acquire_intent = true;
		if (n == 2) ack.blocked_has_remote_proof = true;
		if (n == 3) ack.payload_bytes--;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_source_master_two_local_connections_and_R4_are_independent)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT(settle_receive_exact(&ack, n == 0 ? 2 : 0, n == 1 ? 62 : 61,
			n == 2 ? 2 : 0, n == 3 ? 62 : 61, n == 4 ? 78 : 77) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_peer_boot_slot_epoch_and_freshness_turnover_leave_NOTICE)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) auth_peer_boot++;
		if (n == 1) auth_slot_valid = false;
		if (n == 2) auth_epoch++;
		if (n == 3) auth_fresh = false;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_cancel_failure_or_domain_drift_never_erases_existing_debt)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } before = *done_intent;
		if (n == 0) request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
		if (n == 1) request_entry()->resource_x_bootstrap_round.failed_attempt_floor = 1;
		if (n == 2) request_entry()->resource_x_bootstrap_round.r4_record_generation++;
		if (n == 3) request_entry()->resource_x_bootstrap_round.current_master_node = 2;
		if (n == 4) request_entry()->binding_generation++;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	}
}
UT_TEST(test_consumed_actual_GCS_TARGET_receiver_authenticates_reverse_notification)
{
	EvidenceFixture f; ResourceXDecodedFrame ack;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(settle_gcs(&ack, NULL), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_GCS_wrong_message_route_epoch_or_length_is_rejected)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterICEnvelope env = {0};
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0;
		env.dest_node_id = 1; env.epoch = 17; env.payload_length = 264;
		if (n == 0) env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT;
		if (n == 1) env.source_node_id = 2;
		if (n == 2) env.dest_node_id = 2;
		if (n == 3) env.epoch++;
		if (n == 4) env.payload_length--;
		UT_ASSERT(settle_gcs(&ack, &env) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_GCS_admission_peer_or_master_drift_does_not_skip_guards)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		if (n == 0) master_durable_admission = false;
		if (n == 1) ready_peer_ok = false;
		if (n == 2) fake_gcs_master_node = 2;
		if (n == 3) bootstrap_ingress_generation = 62;
		UT_ASSERT(settle_gcs(&ack, NULL) != RESOURCE_X_APPLY_APPLIED);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_native_process_owner_pin_recheck_precedes_retirement)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED); before = *done_intent;
		if (n == 0) MyProc->pid++;
		if (n == 1) MyProc->cluster_grd_generation++;
		if (n == 2) request_pins = 0;
		if (n == 3) CurrentResourceOwner = NULL;
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
	}
}
UT_TEST(test_consumed_native_generation_tag_token_and_mode_turnover_preserve_receipt)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) pg_atomic_write_u64(&request_own.generation, request_base.generation + 2);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, prepared_episode.reservation_token + 1);
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) request_buf.pcm_state = PCM_STATE_X;
		if (n == 4) request_buf.buffer_type = BUF_TYPE_PI;
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
	}
}
UT_TEST(test_consumed_IO_error_write_turnover_and_later_hold_cannot_be_retired)
{
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BM_IO_IN_PROGRESS);
		if (n == 1) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BM_IO_ERROR);
		if (n == 2) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 2);
		if (n == 4) pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
		if (n == 5) ResourceOwnerRememberBufferIO(original_io_owner, 1);
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
	}
}
UT_TEST(test_consumed_retained_payload_tail_or_exact_handle_corruption_is_not_forgotten)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		if (n == 0) done_intent->payload[240] ^= 1;
		if (n == 1) done_intent->payload[RESOURCE_X_COMPLETE_V5_BYTES] = 1;
		if (n == 2) done_intent->slot.body.reserved = 1;
		if (n == 3) done_intent->slot.authority_generation++;
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
	}
}
UT_TEST(test_consumed_empty_descriptor_residue_is_fail_closed_not_quiescent)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot staged;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; } staged = settle_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	done_intent->slot.send_episode = 1;
	UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
}
UT_TEST(test_consumed_original_completed_replay_accepts_ACK_without_regressing_or_reinstalling)
{
	EvidenceFixture f; ResourceXDecodedFrame ack;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	settle_once(SETTLE_CONSUMED);
}
UT_TEST(test_consumed_settled_native_S_evidence_remains_in_original_full_owner)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXDeliveryClaim current;
	ResourceXAcquireReadReceipt receipt;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_owner_exact(&execution_claim,
		&prepared_episode, &current, &receipt), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(current.purpose, SETTLE_RETIRED);
	UT_ASSERT_EQ(receipt.authority_generation, input_grant.common.authority_generation);
	UT_ASSERT_EQ(receipt.page_crc32c, ack.body.acquire_completion.page_crc32c);
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_DUPLICATE);
	settle_once(SETTLE_RETIRED);
}
UT_TEST(test_consumed_replay_never_releases_a_later_hold_even_with_same_sequence)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		if (n == 1) UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
		pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
		UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), execution_claim.executor_sequence);
	}
}
UT_TEST(test_consumed_ERROR_during_native_recheck_preserves_receipt_for_original_executor)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; volatile bool caught = false;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	stage_header_hook = stage_raise;
	PG_TRY(); { (void)settle_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, SETTLE_CONSUMED);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}

UT_TEST(test_consumed_GCS_reverse_receipt_cannot_leave_legacy_READY_in_output)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXRequesterJoinSnapshot out, empty = {0};
	ClusterICEnvelope env = {0};
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0;
	env.dest_node_id = 1; env.epoch = 17; env.payload_length = 264;
	memset(&out, 0xA5, sizeof(out));
	UT_ASSERT_EQ(gcs_block_resource_x_requester_join_ingress(&env, &ack, 61, &out), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&out, &empty, sizeof(out)) == 0);
	settle_once(SETTLE_CONSUMED);
}

static ResourceXDecodedFrame settle_postcheck_ack;
static void
settle_ack_between_native_observations(void)
{
	UT_ASSERT_EQ(settle_receive(&settle_postcheck_ack), RESOURCE_X_APPLY_APPLIED);
}
static void
settle_ack_on_next_header(void)
{
	stage_header_hook = settle_ack_between_native_observations;
}
UT_TEST(test_consumed_ACK_between_DONE_arm_and_postcheck_is_not_a_false_failure)
{
	EvidenceFixture f;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	settle_postcheck_ack = settle_frame();
	stage_header_hook = settle_ack_on_next_header;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_CONSUMED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}

static void
settle_damage_physical(unsigned field)
{
	if (field >= 6)
		(void)settle_stage();
	if (field == 0) done_intent->slot.first_armed_us = 0;
	if (field == 1) done_intent->slot.first_armed_us = UINT64_MAX;
	if (field == 2) done_intent->slot.last_attempt_us = UINT64_MAX;
	if (field == 3) done_intent->slot.last_attempt_us = done_intent->slot.first_armed_us - 1;
	if (field == 4) done_intent->slot.last_send_us = 1;
	if (field == 5) done_intent->slot.send_episode = 1;
	if (field == 6) done_intent->slot.send_episode = 0;
	if (field == 7) done_intent->slot.last_attempt_us = 0;
}
UT_TEST(test_consumed_malformed_physical_descriptor_cannot_be_acknowledged)
{
	for (unsigned n = 0; n < 8; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		settle_damage_physical(n); before = *done_intent;
		UT_ASSERT(settle_receive(&ack) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
		settle_once(RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_consumed_malformed_physical_descriptor_is_retained_after_master_ACK)
{
	for (unsigned n = 0; n < 8; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
		if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
		settle_damage_physical(n); before = *done_intent;
		UT_ASSERT(settle_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
		settle_once(SETTLE_CONSUMED);
	}
}
UT_TEST(test_consumed_real_NOT_ADMITTED_bookkeeping_remains_a_quiet_original_owner)
{
	EvidenceFixture f; ResourceXDecodedFrame ack; ResourceXIntentSlot original;
	if (!settle_start(&f, &ack)) { UT_ASSERT(false); return; }
	original = done_intent->slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
		&original, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT(done_intent->slot.last_attempt_us >= done_intent->slot.first_armed_us);
	UT_ASSERT_EQ(done_intent->slot.send_episode, 0);
	UT_ASSERT_EQ(settle_receive(&ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(settle_run(), RESOURCE_X_APPLY_APPLIED);
	settle_once(SETTLE_RETIRED);
}

int
main(void)
{
	UT_PLAN(33);
	UT_RUN(test_consumed_ACK_retains_full_original_A_I_and_physical_notice);
	UT_RUN(test_consumed_quiet_ARMED_notice_retires_only_after_native_S_recheck);
	UT_RUN(test_consumed_no_master_receipt_is_local_wait_without_erasure);
	UT_RUN(test_consumed_STAGED_waits_for_real_original_success_callback);
	UT_RUN(test_consumed_callback_before_ACK_does_not_substitute_for_master_consumption);
	UT_RUN(test_consumed_failed_physical_callback_keeps_body_until_exact_local_retirement);
	UT_RUN(test_consumed_exact_duplicates_never_rearm_notify_repeat_C_or_bump);
	UT_RUN(test_consumed_stale_callback_cannot_quiesce_the_original_STAGED_owner);
	UT_RUN(test_consumed_before_original_notice_cannot_forge_a_master_receipt);
	UT_RUN(test_consumed_changed_complete_E_is_refused_even_with_valid_wire_CRC);
	UT_RUN(test_consumed_changed_complete_A_is_not_reduced_to_authority_or_CRC);
	UT_RUN(test_consumed_changed_actual_I_is_not_accepted_as_a_master_receipt);
	UT_RUN(test_consumed_noncanonical_decoded_flags_and_CRC_do_not_repair_input);
	UT_RUN(test_consumed_source_master_two_local_connections_and_R4_are_independent);
	UT_RUN(test_consumed_peer_boot_slot_epoch_and_freshness_turnover_leave_NOTICE);
	UT_RUN(test_consumed_cancel_failure_or_domain_drift_never_erases_existing_debt);
	UT_RUN(test_consumed_actual_GCS_TARGET_receiver_authenticates_reverse_notification);
	UT_RUN(test_consumed_GCS_wrong_message_route_epoch_or_length_is_rejected);
	UT_RUN(test_consumed_GCS_admission_peer_or_master_drift_does_not_skip_guards);
	UT_RUN(test_consumed_native_process_owner_pin_recheck_precedes_retirement);
	UT_RUN(test_consumed_native_generation_tag_token_and_mode_turnover_preserve_receipt);
	UT_RUN(test_consumed_IO_error_write_turnover_and_later_hold_cannot_be_retired);
	UT_RUN(test_consumed_retained_payload_tail_or_exact_handle_corruption_is_not_forgotten);
	UT_RUN(test_consumed_empty_descriptor_residue_is_fail_closed_not_quiescent);
	UT_RUN(test_consumed_original_completed_replay_accepts_ACK_without_regressing_or_reinstalling);
	UT_RUN(test_consumed_settled_native_S_evidence_remains_in_original_full_owner);
	UT_RUN(test_consumed_replay_never_releases_a_later_hold_even_with_same_sequence);
	UT_RUN(test_consumed_ERROR_during_native_recheck_preserves_receipt_for_original_executor);
	UT_RUN(test_consumed_GCS_reverse_receipt_cannot_leave_legacy_READY_in_output);
	UT_RUN(test_consumed_ACK_between_DONE_arm_and_postcheck_is_not_a_false_failure);
	UT_RUN(test_consumed_malformed_physical_descriptor_cannot_be_acknowledged);
	UT_RUN(test_consumed_malformed_physical_descriptor_is_retained_after_master_ACK);
	UT_RUN(test_consumed_real_NOT_ADMITTED_bookkeeping_remains_a_quiet_original_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
