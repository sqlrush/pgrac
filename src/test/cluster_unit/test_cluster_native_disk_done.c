/*-------------------------------------------------------------------------
 * test_cluster_native_disk_done.c
 *    Retain completed DISK/S evidence in the original asynchronous DONE.
 *
 * Real PCM/codec/native/IO bodies run. Only the explicit absent producer
 * seam is bridged to the original completion replay for the RED build.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_complete_fixture.inc"
#include "test_cluster_resource_x_decision_owner_layout.inc"
#include "test_cluster_native_disk_done_candidate.inc"
/* The inherited PCM fixture has an unrelated enqueue leaf. Keep it intact;
 * this name selects the entire actual LMS admission/staging body below. */
ClusterLmsEnqueueResult done_actual_lms_enqueue(int worker, const ResourceXIntentSlot *intent,
	uint32 connection, uint64 deadline);
#define cluster_lms_outbound_enqueue_resource_x_intent done_actual_lms_enqueue
#include "test_cluster_native_disk_done_lms.inc"
#undef cluster_lms_outbound_enqueue_resource_x_intent
#ifndef DISK_DONE_RED
#define LockBufHdr stage_header_lock
#include "test_cluster_native_disk_done.inc"
#undef LockBufHdr
#endif

static ClusterPcmResourceXMasterState *done_state;
static ClusterPcmResourceXControlIntent *done_intent;

static ResourceXApplyResult
done_run(void)
{
#ifdef DISK_DONE_RED
	return complete_run();
#else
	return cluster_bufmgr_pcm_own_acquire_done_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &execution_claim);
#endif
}

/* Locate the actual original owner through its complete retained A bytes,
 * using the extracted product layout, never a guessed test-only offset. */
static bool
done_start_watermark(EvidenceFixture *f, uint64 watermark)
{
	uint8 encoded[RESOURCE_X_DECISION_V4_BYTES];
	uint16 bytes = 0;
	ResourceXWireReject reject;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload);
	Size location = 0, matches = 0;

	if (!complete_start(f)
		|| (watermark != 90 && !disk_start_watermark(f, 0, watermark))
		|| complete_run() != RESOURCE_X_APPLY_APPLIED)
		return false;
	complete_finished();
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
		encoded, sizeof(encoded), &bytes, &reject) || bytes != sizeof(encoded))
		return false;
	for (Size n = offset % sizeof(uint64); n + bytes <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, encoded, bytes) == 0) {
			location = n;
			matches++;
		}
	UT_ASSERT_EQ(matches, 1);
	if (matches != 1 || location < offset)
		return false;
	done_state = (ClusterPcmResourceXMasterState *)(memory + location - offset);
	UT_ASSERT((unsigned char *)(done_state + 1) <= memory + fake_pcm_header_requested_size);
	done_intent = &done_state->requester_settlement_intent;
	return true;
}

static bool
done_start(EvidenceFixture *f)
{
	return done_start_watermark(f, 90);
}

static ResourceXDecodedFrame
done_frame(void)
{
	ResourceXDecodedFrame frame;
	const ResourceXAcquireReadReceipt *r = &request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	ResourceXDecodedAcquireCompletion *c;
	memset(&frame, 0, sizeof(frame));
	frame.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	frame.payload_bytes = RESOURCE_X_COMPLETE_V5_BYTES;
	frame.has_acquire_decision = true;
	frame.common = input_grant.common;
	frame.common.action_node = cluster_node_id;
	frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	frame.common.semantic_crc32c = 0;
	c = &frame.body.acquire_completion;
	c->decision = input_grant.body.acquire_decision;
	c->native_generation = prepared_episode.base_generation + 1;
	c->write_io_generation = r->write_io_generation;
	c->page_lsn = r->page_lsn;
	c->page_scn = r->page_scn;
	c->page_crc32c = r->page_crc32c;
	c->decision_crc32c = r->decision_crc32c;
	return frame;
}

static bool
done_encode(const ResourceXDecodedFrame *frame, uint8 *bytes)
{
	uint16 len = 0;
	ResourceXWireReject reject;
	bool ok = cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, RESOURCE_X_COMPLETE_V5_BYTES, &len, &reject);
	UT_ASSERT(ok);
	if (ok)
		UT_ASSERT_EQ(len, RESOURCE_X_COMPLETE_V5_BYTES);
	return ok;
}

static void
done_debt(void)
{
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
	UT_ASSERT_EQ(done_state->requester_join.t_install_us, 0);
	UT_ASSERT_EQ(done_state->requester_join.install_succeeded, 0);
	UT_ASSERT_EQ(done_state->requester_join.settled, 0);
	UT_ASSERT_EQ(complete_commits, 1);
	UT_ASSERT_EQ(complete_io_calls, 1);
	UT_ASSERT_EQ(complete_broadcasts, 1);
	UT_ASSERT_EQ(disk_reads, 1);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	complete_idle();
}

UT_TEST(test_done_actual_C_arms_original_owner_without_semantic_retirement)
{
	EvidenceFixture f;
	ResourceXIntentSlot probed;
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(done_intent->slot.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(done_intent->slot.payload_bytes, 264);
	UT_ASSERT_EQ(done_intent->slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(done_intent->slot.destination_node, input_grant.body.acquire_decision.master_node);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(&request_base.tag,
		&cursor, &probed), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT(memcmp(&probed, &done_intent->slot, sizeof(probed)) == 0);
	done_debt();
}
UT_TEST(test_done_wire_retains_full_E_A_and_actual_I)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, decoded;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	ResourceXWireReject reject;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	UT_ASSERT_EQ(bytes[0], RESOURCE_X_COMPLETION_WIRE_VERSION);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), &decoded, &reject));
	UT_ASSERT(memcmp(&frame.body.acquire_completion, &decoded.body.acquire_completion,
		sizeof(frame.body.acquire_completion)) == 0);
	UT_ASSERT_EQ(decoded.common.target_mode, PCM_STATE_S);
	UT_ASSERT_EQ(decoded.common.authority_generation, input_grant.common.authority_generation);
}
UT_TEST(test_done_original_retained_bytes_are_canonical)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(bytes, done_intent->payload, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(done_intent->slot.logical_generation, execution_claim.observation.request.assertion_sequence);
	UT_ASSERT_EQ(done_intent->slot.authority_generation, input_grant.common.authority_generation);
}
UT_TEST(test_done_duplicate_before_send_never_rearms_or_repeats_C)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	before = *done_intent;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	done_debt();
}
UT_TEST(test_done_NOT_ADMITTED_keeps_exact_original_payload_and_responsibility)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	ResourceXIntentSlot slot;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	before = *done_intent; slot = before.slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&slot, 201),
		RESOURCE_X_INTENT_NOT_ADMITTED);
	before.slot.last_attempt_us = 201;
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	done_debt();
}
UT_TEST(test_done_real_stage_and_transport_callback_keep_semantic_C)
{
	EvidenceFixture f;
	ResourceXIntentSlot slot, staged_slot;
	uint8 payload[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	slot = done_intent->slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&slot,
		&staged_slot, payload, sizeof(payload)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&slot, 202,
		&staged_slot), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged_slot, true, 203));
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(memcmp(payload, done_intent->payload, sizeof(payload)) == 0);
	done_debt();
}
UT_TEST(test_done_after_sent_does_not_offer_a_second_notice)
{
	EvidenceFixture f;
	ResourceXIntentSlot staged_slot;
	ClusterPcmResourceXBootstrapRound round;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&done_intent->slot,
		204, &staged_slot), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged_slot, true, 205));
	round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	done_debt();
}
UT_TEST(test_done_failed_real_transport_rearms_same_notice_only)
{
	EvidenceFixture f;
	ResourceXIntentSlot staged_slot;
	uint8 before[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	memcpy(before, done_intent->payload, sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&done_intent->slot,
		206, &staged_slot), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged_slot, false, 207));
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(done_intent->slot.send_episode, 1);
	UT_ASSERT(memcmp(before, done_intent->payload, sizeof(before)) == 0);
	done_debt();
}
UT_TEST(test_done_occupied_original_container_is_local_wait_without_eviction)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	ClusterPcmResourceXBootstrapRound round;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	memset(done_intent, 0x3B, sizeof(*done_intent));
	done_intent->slot.state = RESOURCE_X_INTENT_SLOT_ARMED;
	before = *done_intent; round = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	memset(done_intent, 0, sizeof(*done_intent));
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_done_before_actual_C_never_arms_or_completes_IO)
{
	EvidenceFixture f;
	UT_ASSERT(complete_start(&f));
	UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
}
UT_TEST(test_done_does_not_release_a_still_held_completion_fence)
{
	EvidenceFixture f;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
	UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), execution_claim.executor_sequence);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
}
UT_TEST(test_done_IO_and_invalid_native_states_cannot_be_reported_complete)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BM_IO_IN_PROGRESS);
		if (n == 1) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID | BM_VALID | BM_IO_ERROR);
		if (n == 2) pg_atomic_write_u32(&request_buf.state, BM_TAG_VALID);
		if (n == 3) ResourceOwnerRememberBufferIO(original_io_owner, 1);
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_done_write_IO_turnover_rejects_without_erasing_C)
{
	for (int n = 1; n < 3; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		pg_atomic_write_u64(&request_own.write_io_generation, n);
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE);
	}
}
UT_TEST(test_done_changed_full_E_or_executor_is_refused)
{
	for (int n = 0; n < 8; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) prepared_episode.backend_generation++;
		if (n == 1) prepared_episode.requester_incarnation++;
		if (n == 2) prepared_episode.reservation_token++;
		if (n == 3) prepared_episode.base_generation++;
		if (n == 4) execution_claim.executor_sequence++;
		if (n == 5) execution_claim.observation.entry_binding_generation++;
		if (n == 6) execution_claim.observation.request.assertion_sequence++;
		if (n == 7) execution_claim.reserved[0] = 1;
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_done_actual_process_and_current_domain_turnover_are_refused)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) MyProc->cluster_grd_generation++;
		if (n == 1) MyProc->pid++;
		if (n == 2) fake_gcs_master_node = 2;
		if (n == 3) request_entry()->resource_x_bootstrap_round.r4_record_generation++;
		if (n == 4) request_entry()->binding_generation++;
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_done_native_generation_tag_and_token_turnover_are_refused)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) pg_atomic_write_u64(&request_own.generation, request_base.generation + 2);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, prepared_episode.reservation_token + 1);
		if (n == 2) request_buf.tag.blockNum++;
		if (n == 3) request_buf.pcm_state = PCM_STATE_X;
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_done_wire_old_partial_version_reserved_CRC_are_not_repaired)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES], bad[sizeof(bytes)];
	if (!done_start(&f)) { UT_ASSERT(false); return; } frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	for (int n = 0; n < 5; n++) {
		memcpy(bad, bytes, sizeof(bad));
		if (n == 0) bad[0] = RESOURCE_X_DECISION_WIRE_VERSION;
		if (n == 1) bad[153] = 1;
		if (n == 2) bad[214] = 1;
		if (n == 3) bad[240] ^= 1;
		if (n < 3) test_reseal(bad, sizeof(bad));
		UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bad, sizeof(bad) - (n == 4 ? 1 : 0), &decoded, &reject));
		UT_ASSERT_EQ(reject, n == 0 ? RESOURCE_X_WIRE_REJECT_VERSION
			: n < 3 ? RESOURCE_X_WIRE_REJECT_RESERVED : n == 3 ? RESOURCE_X_WIRE_REJECT_CRC
			: RESOURCE_X_WIRE_REJECT_LEGACY_LENGTH);
		UT_ASSERT(!cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bad, sizeof(bad) - (n == 4 ? 1 : 0), 81, &reject));
	}
}
UT_TEST(test_done_wire_has_no_alias_to_other_modes_sources_or_holders)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, bad;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	uint16 len;
	if (!done_start(&f)) { UT_ASSERT(false); return; } frame = done_frame();
	for (int n = 0; n < 10; n++) {
		bad = frame;
		if (n == 0) bad.common.target_mode = PCM_STATE_X;
		if (n == 1) bad.common.outcome = RESOURCE_X_OUTCOME_RETRY;
		if (n == 2) bad.common.action_node = 3;
		if (n == 3) bad.body.acquire_completion.decision.episode.path = RESOURCE_X_ACQUIRE_RESIDENT;
		if (n == 4) bad.body.acquire_completion.decision.source_kind = RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT;
		if (n == 5) bad.body.acquire_completion.decision.holders[17].grant_generation = 3;
		if (n == 6) bad.body.acquire_completion.decision.pi_holders_bitmap = 1;
		if (n == 7) bad.body.acquire_completion.native_generation++;
		if (n == 8) bad.body.acquire_completion.write_io_generation = 1;
		if (n == 9) bad.body.acquire_completion.decision.episode.base_generation = UINT64_MAX - 1;
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&bad, bytes, sizeof(bytes), &len, &reject));
	}
}
UT_TEST(test_done_wire_sender_rebind_preserves_all_semantic_bytes)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES], before[sizeof(bytes)];
	if (!done_start(&f)) { UT_ASSERT(false); return; } frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), 81, &reject));
	UT_ASSERT(memcmp(before + 96, bytes + 96, sizeof(bytes) - 96) == 0);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), &decoded, &reject));
	UT_ASSERT_EQ(decoded.common.sender_connection_generation, 81);
	UT_ASSERT_EQ(decoded.body.acquire_completion.decision.master_ingress_generation,
		input_grant.body.acquire_decision.master_ingress_generation);
}
UT_TEST(test_done_late_transport_callback_cannot_clear_a_different_offer)
{
	EvidenceFixture f;
	ResourceXIntentSlot staged_slot, stale;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&done_intent->slot,
		208, &staged_slot), RESOURCE_X_INTENT_STAGED);
	stale = staged_slot; stale.send_episode++;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&stale, true, 209));
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	done_debt();
}
UT_TEST(test_done_shared_receipt_corruption_cannot_be_sent)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.authority_generation++;
		if (n == 1) request_entry()->resource_x_bootstrap_round.acquire_read_receipt.decision_crc32c++;
		UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
}
UT_TEST(test_done_cancel_floor_never_drops_existing_completion_debt)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED); before = *done_intent;
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
	UT_ASSERT(done_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	done_debt();
}
UT_TEST(test_done_legacy_X_install_settlement_cannot_consume_typed_S_notice)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame;
	ResourceXMasterSnapshot snapshot;
	if (!done_start(&f)) { UT_ASSERT(false); return; } frame = done_frame();
	UT_ASSERT(cluster_pcm_lock_resource_x_install_settlement_exact(&frame, cluster_node_id,
		&snapshot) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(done_state->requester_join.settled, 0);
}
UT_TEST(test_done_original_completion_replay_preserves_notice_and_never_repeats_C)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED); before = *done_intent;
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
	done_debt();
}

UT_TEST(test_done_ERROR_before_and_after_arm_keeps_the_exact_original_owner)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f;
		volatile bool caught = false;
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		if (n == 0) stage_header_hook = stage_raise;
		else {
			fake_pcm_clock_hook = stage_header_error;
			fake_pcm_clock_hook_countdown = 1;
		}
		PG_TRY();
		{
			(void)done_run();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(done_intent->slot.state, n == 0 ? RESOURCE_X_INTENT_SLOT_EMPTY : RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			n == 0 ? RESOURCE_X_DELIVERY_ACQUIRE_READ_COMPLETE : RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
		UT_ASSERT_EQ(done_run(), n == 0 ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_DUPLICATE);
		done_debt();
	}
}
UT_TEST(test_done_existing_family_claims_exact_length_before_legacy_fallback)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, sizeof(bytes)));
	UT_ASSERT(!gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_ASSERT_X, sizeof(bytes)));
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_ASSERT_X, bytes, sizeof(bytes), &decoded, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_TYPE_KIND);
}
UT_TEST(test_done_CRC_valid_altered_full_E_A_I_is_not_a_sendable_owner)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		ResourceXIntentSlot expected, observed;
		uint8 payload[RESOURCE_X_COMPLETE_V5_BYTES];
		if (!done_start(&f)) { UT_ASSERT(false); return; }
		UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
		expected = done_intent->slot;
		done_intent->payload[n == 0 ? 104 : n == 1 ? 168 : 256] ^= 1;
		test_reseal(done_intent->payload, RESOURCE_X_COMPLETE_V5_BYTES);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&expected,
			&observed, payload, sizeof(payload)), RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(done_intent->slot.send_episode, 0);
	}
}
UT_TEST(test_done_notice_replay_cannot_release_a_subsequent_native_hold)
{
	EvidenceFixture f;
	ClusterPcmResourceXControlIntent before;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	before = *done_intent;
	pg_atomic_write_u64(&request_own.delivery_attempt, execution_claim.executor_sequence);
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), execution_claim.executor_sequence);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&before, done_intent, sizeof(before)) == 0);
}

UT_TEST(test_done_valid_cross_node_Lamport_time_survives_actual_C_and_codec)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame, decoded;
	ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start_watermark(&f, scn_encode(7, 90))) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(gcs_block_lost_write_verdict(scn_encode(7, 90), 93), GCS_LOST_WRITE_PASS);
	frame = done_frame();
	if (!done_encode(&frame, bytes)) return;
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), &decoded, &reject));
	UT_ASSERT_EQ(decoded.body.acquire_completion.decision.disk_watermark_scn, scn_encode(7, 90));
	UT_ASSERT_EQ(decoded.body.acquire_completion.page_scn, 93);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	done_debt();
}
UT_TEST(test_done_higher_node_bits_do_not_make_stale_time_or_absent_page_current)
{
	EvidenceFixture f;
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject;
	uint16 length;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	for (int n = 0; n < 2; n++) {
		frame = done_frame();
		frame.body.acquire_completion.page_scn = n == 0 ? scn_encode(7, 89) : InvalidScn;
		UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&frame, bytes, sizeof(bytes), &length, &reject));
	}
}
UT_TEST(test_done_real_LMS_enqueue_and_payload_guard_bind_the_original_owner)
{
	EvidenceFixture f;
	ResourceXIntentSlot offered, staged_slot, observed;
	ClusterLmsEnqueueResult admitted;
	ClusterLmsOutboundSlot *ring_slot;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	offered = done_intent->slot;
	UT_ASSERT(lms_outbound_resource_x_intent_valid(&offered));
	admitted = done_actual_lms_enqueue(0, &offered, 51, 9999);
	UT_ASSERT_EQ(admitted, CLUSTER_LMS_ENQUEUE_ADMITTED);
	if (admitted != CLUSTER_LMS_ENQUEUE_ADMITTED)
		return;
	UT_ASSERT_EQ(request_shared.rings[0].count, 1);
	ring_slot = &request_shared.rings[0].ring[0];
	UT_ASSERT_EQ(ring_slot->msg_type, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	UT_ASSERT_EQ(ring_slot->kind, CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT);
	memcpy(&staged_slot, ring_slot->payload, sizeof(staged_slot));
	UT_ASSERT_EQ(staged_slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(staged_slot.send_episode, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&staged_slot,
		&observed, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&observed, bytes));
	observed.destination_node ^= 1;
	UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&observed, bytes));
	observed = staged_slot;
	observed.body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_STATUS;
	UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&observed, bytes));
	cluster_lms_outbound_resource_x_send_complete(&staged_slot, true);
	UT_ASSERT_EQ(done_intent->slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_DUPLICATE);
	done_debt();
}
UT_TEST(test_done_real_LMS_gate_keeps_legacy_length_and_role_guards)
{
	EvidenceFixture f;
	ResourceXIntentSlot slot;
	if (!done_start(&f)) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
	slot = done_intent->slot;
	slot.payload_bytes = RESOURCE_X_SHORT_V1_BYTES;
	UT_ASSERT(lms_outbound_resource_x_intent_valid(&slot));
	slot.payload_bytes = RESOURCE_X_COMPLETE_V5_BYTES - 1;
	UT_ASSERT(!lms_outbound_resource_x_intent_valid(&slot));
	for (int n = 0; n < 6; n++) {
		slot = done_intent->slot;
		if (n == 0) slot.body.owner_generation++;
		if (n == 1) slot.body.owner_index++;
		if (n == 2) slot.body.reserved++;
		if (n == 3) slot.body.owner_node ^= 1;
		if (n == 4) slot.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
		if (n == 5) slot.state = RESOURCE_X_INTENT_SLOT_STAGED;
		UT_ASSERT(!lms_outbound_resource_x_intent_valid(&slot));
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(32);
	UT_RUN(test_done_actual_C_arms_original_owner_without_semantic_retirement);
	UT_RUN(test_done_wire_retains_full_E_A_and_actual_I);
	UT_RUN(test_done_original_retained_bytes_are_canonical);
	UT_RUN(test_done_duplicate_before_send_never_rearms_or_repeats_C);
	UT_RUN(test_done_NOT_ADMITTED_keeps_exact_original_payload_and_responsibility);
	UT_RUN(test_done_real_stage_and_transport_callback_keep_semantic_C);
	UT_RUN(test_done_after_sent_does_not_offer_a_second_notice);
	UT_RUN(test_done_failed_real_transport_rearms_same_notice_only);
	UT_RUN(test_done_occupied_original_container_is_local_wait_without_eviction);
	UT_RUN(test_done_before_actual_C_never_arms_or_completes_IO);
	UT_RUN(test_done_does_not_release_a_still_held_completion_fence);
	UT_RUN(test_done_IO_and_invalid_native_states_cannot_be_reported_complete);
	UT_RUN(test_done_write_IO_turnover_rejects_without_erasing_C);
	UT_RUN(test_done_changed_full_E_or_executor_is_refused);
	UT_RUN(test_done_actual_process_and_current_domain_turnover_are_refused);
	UT_RUN(test_done_native_generation_tag_and_token_turnover_are_refused);
	UT_RUN(test_done_wire_old_partial_version_reserved_CRC_are_not_repaired);
	UT_RUN(test_done_wire_has_no_alias_to_other_modes_sources_or_holders);
	UT_RUN(test_done_wire_sender_rebind_preserves_all_semantic_bytes);
	UT_RUN(test_done_late_transport_callback_cannot_clear_a_different_offer);
	UT_RUN(test_done_shared_receipt_corruption_cannot_be_sent);
	UT_RUN(test_done_cancel_floor_never_drops_existing_completion_debt);
	UT_RUN(test_done_legacy_X_install_settlement_cannot_consume_typed_S_notice);
	UT_RUN(test_done_original_completion_replay_preserves_notice_and_never_repeats_C);
	UT_RUN(test_done_ERROR_before_and_after_arm_keeps_the_exact_original_owner);
	UT_RUN(test_done_existing_family_claims_exact_length_before_legacy_fallback);
	UT_RUN(test_done_CRC_valid_altered_full_E_A_I_is_not_a_sendable_owner);
	UT_RUN(test_done_notice_replay_cannot_release_a_subsequent_native_hold);
	UT_RUN(test_done_valid_cross_node_Lamport_time_survives_actual_C_and_codec);
	UT_RUN(test_done_higher_node_bits_do_not_make_stale_time_or_absent_page_current);
	UT_RUN(test_done_real_LMS_enqueue_and_payload_guard_bind_the_original_owner);
	UT_RUN(test_done_real_LMS_gate_keeps_legacy_length_and_role_guards);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
