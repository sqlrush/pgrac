/* Real master consume/ready/stage/callback owners. Native C is authenticated
 * peer input, not a live requester or native/network qualification. */
#include "test_cluster_resource_x_master_done_fixture.inc"
#include "test_cluster_resource_x_consumed_ring.inc"
ClusterLmsEnqueueResult consumed_actual_lms_enqueue(int worker, const ResourceXIntentSlot *intent,
	uint32 connection, uint64 deadline);
#define cluster_lms_outbound_enqueue_resource_x_intent consumed_actual_lms_enqueue
#include "test_cluster_resource_x_consumed_lms.inc"
#undef cluster_lms_outbound_enqueue_resource_x_intent

static ClusterLmsOutboundSharedState consumed_ring_shared;
static LWLock consumed_ring_locks[CLUSTER_LMS_MAX_WORKERS];

static void
consumed_ring_reset(void)
{
	memset(&consumed_ring_shared, 0, sizeof(consumed_ring_shared));
	cluster_lms_outbound_shared = &consumed_ring_shared;
	cluster_lms_outbound_rings = consumed_ring_shared.rings;
	pg_atomic_init_u64(&consumed_ring_shared.resource_x_transport_mutation_sequence, 1);
	for (int n = 0; n < CLUSTER_LMS_MAX_WORKERS; n++)
		cluster_lms_outbound_locks[n] = &consumed_ring_locks[n];
}

static bool
consumed_start(PublishFixture *f, ResourceXDecodedFrame *done)
{
	if (!master_done_start(f, done)) return false;
	master_done_send(f);
	UT_ASSERT_EQ(master_done_receive(done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f->state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(f->state->deferred_settlement.present, 3);
	return f->state->grant_intent.slot.state == RESOURCE_X_INTENT_SLOT_ARMED
		&& f->state->deferred_settlement.present == 3;
}

static ResourceXDecodedFrame
consumed_decode(PublishFixture *f)
{
	ResourceXDecodedFrame ack = {0}; ResourceXWireReject reject;
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		f->state->grant_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &ack, &reject));
	return ack;
}

static bool
consumed_rewrite(PublishFixture *f, ResourceXDecodedFrame *ack)
{
	uint16 len = 0; ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		ack, f->state->grant_intent.payload, RESOURCE_X_COMPLETE_V5_BYTES, &len, &reject);
}

static void
consumed_send_refused(PublishFixture *f)
{
	ResourceXIntentSlot out; uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	ClusterPcmResourceXMasterState before = *f->state;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(
		&f->state->grant_intent.slot, &out, bytes, sizeof(bytes)) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f->state, sizeof(before)) == 0);
}

UT_TEST(test_consumed_full_e_a_i_in_existing_done_family)
{
	PublishFixture f; ResourceXDecodedFrame done, ack;
	if (!consumed_start(&f, &done)) return;
	ack = consumed_decode(&f);
	UT_ASSERT_EQ(f.state->grant_intent.payload[0], 6);
	UT_ASSERT_EQ(ack.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(ack.common.outcome, RESOURCE_X_OUTCOME_DUPLICATE);
	UT_ASSERT_EQ(ack.common.action_node, cluster_node_id);
	UT_ASSERT(memcmp(&ack.body.acquire_completion, &done.body.acquire_completion,
		sizeof(done.body.acquire_completion)) == 0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_MASTER_GRANT);
	UT_ASSERT_EQ(f.state->grant_intent.slot.destination_node, 1);
	UT_ASSERT_EQ(f.state->grant_intent.slot.body.owner_generation, done.common.authority_generation);
}
UT_TEST(test_consumed_early_done_only_arms_after_real_grant_callback)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot grant;
	if (!master_done_start(&f, &done)) return;
	grant = master_done_stage(&f);
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 2);
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&grant, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(cluster_gcs_block_resource_x_bootstrap_tick(&f.work), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	UT_ASSERT_EQ(f.state->grant_intent.slot.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
}
UT_TEST(test_consumed_not_admitted_keeps_exact_receipt_and_payload)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXDeferredSettlement receipt;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; ResourceXIntentSlot slot;
	if (!consumed_start(&f, &done)) return;
	receipt = f.state->deferred_settlement; slot = f.state->grant_intent.slot;
	memcpy(bytes, f.state->grant_intent.payload, sizeof(bytes));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
		&slot, ++fake_pcm_clock_us), RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT(memcmp(bytes, f.state->grant_intent.payload, sizeof(bytes)) == 0);
	UT_ASSERT(memcmp(&receipt, &f.state->deferred_settlement, sizeof(receipt)) == 0);
}
UT_TEST(test_consumed_failure_rearms_same_body_and_rejects_old_callback)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot a, b;
	ClusterPcmResourceXDeferredSettlement receipt;
	if (!consumed_start(&f, &done)) return;
	receipt = f.state->deferred_settlement; a = master_done_stage(&f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT(memcmp(&receipt, &f.state->deferred_settlement, sizeof(receipt)) == 0);
	b = master_done_stage(&f);
	UT_ASSERT(b.send_episode > a.send_episode);
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&a, true, ++fake_pcm_clock_us));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&b, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
}
UT_TEST(test_consumed_generic_semantic_complete_cannot_discharge_transport_debt)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	slot = master_done_stage(&f); before = *f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&slot));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_consumed_original_ready_probe_finds_notification)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot out;
	uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 1;
	if (!consumed_start(&f, &done)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
		&done.common.logical_assertion.resource, &cursor, &out), RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(out.kind, RESOURCE_X_WIRE_INSTALL_SETTLEMENT);
	UT_ASSERT_EQ(out.payload_bytes, RESOURCE_X_COMPLETE_V5_BYTES);
}
UT_TEST(test_consumed_duplicate_does_not_replace_armed_or_staged_owner)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	for (int n = 0; n < 2; n++) {
		if (n) (void)master_done_stage(&f);
		before = *f.state;
		UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}
UT_TEST(test_consumed_actual_lms_shape_and_payload_direction)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	if (!consumed_start(&f, &done)) return;
	slot = f.state->grant_intent.slot;
	UT_ASSERT(lms_outbound_resource_x_intent_valid(&slot));
	UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&slot, f.state->grant_intent.payload));
	UT_ASSERT_EQ(lms_outbound_resource_x_intent_msg_type(&slot), RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	slot.destination_node = 2;
	UT_ASSERT(!lms_outbound_resource_x_intent_valid(&slot));
	UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&slot, f.state->grant_intent.payload));
	slot = f.state->grant_intent.slot; slot.body.owner_kind = RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT;
	UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&slot, f.state->grant_intent.payload));
}
UT_TEST(test_consumed_valid_crc_payload_change_cannot_replace_consumed_i)
{
	for (int n = 0; n < 7; n++) {
		PublishFixture f; ResourceXDecodedFrame done, ack;
		if (!consumed_start(&f, &done)) continue;
		ack = consumed_decode(&f);
		if (n == 0) ack.body.acquire_completion.page_crc32c++;
		if (n == 1) ack.body.acquire_completion.page_lsn++;
		if (n == 2) ack.body.acquire_completion.page_scn++;
		if (n == 3) ack.body.acquire_completion.write_io_generation += 2;
		if (n == 4) ack.body.acquire_completion.decision.episode.reservation_token++;
		if (n == 5) ack.body.acquire_completion.decision.enqueue_order++;
		if (n == 6) ack.body.acquire_completion.decision_crc32c++;
		UT_ASSERT(consumed_rewrite(&f, &ack)); consumed_send_refused(&f);
	}
}
UT_TEST(test_consumed_unknown_or_mismatched_retained_receipt_blocks_send)
{
	for (int n = 0; n < 4; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!consumed_start(&f, &done)) continue;
		if (n == 0) f.state->deferred_settlement.present = 0;
		if (n == 1) f.state->deferred_settlement.present = 2;
		if (n == 2) f.state->deferred_settlement.authenticated_sender = 2;
		if (n == 3) f.state->deferred_settlement.payload[260] ^= 1;
		consumed_send_refused(&f);
	}
}
UT_TEST(test_consumed_master_session_binding_and_admission_are_rechecked)
{
	for (int n = 0; n < 4; n++) {
		PublishFixture f; ResourceXDecodedFrame done;
		if (!consumed_start(&f, &done)) continue;
		if (n == 0) ut_master_session++;
		if (n == 1) f.state->binding_generation++;
		if (n == 2) {
			ResourceXGateSnapshot gate;
			UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
		}
		if (n == 3) f.state->bootstrap_receipts[1].highest_attempt_floor--;
		consumed_send_refused(&f);
	}
}
UT_TEST(test_consumed_physical_callback_clears_only_matching_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot, wrong;
	ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	slot = master_done_stage(&f); wrong = slot; wrong.send_episode++; before = *f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&wrong, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
	UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	before = *f.state;
	UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_consumed_pending_notification_preserves_fifo_and_blocks_publication)
{
	PublishFixture f; ResourceXDecodedFrame done, next, ack; ResourceXBootstrapWork work;
	ClusterPcmResourceXGrantIntent debt;
	if (!consumed_start(&f, &done)) return;
	debt = f.state->grant_intent;
	next = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	/* Observe the real retained NEXT owner; the LMS correctly withholds
	 * bootstrap work until the predecessor notification is handed off. */
	memset(&work, 0, sizeof(work));
	work.request = f.state->bootstrap_priority[2].request;
	work.acquire_intent = f.state->bootstrap_priority[2].acquire_intent;
	work.has_acquire_intent = true;
	work.binding_generation = f.state->binding_generation;
	work.enqueue_order = f.state->bootstrap_priority[2].enqueue_order;
	work.r4_record_generation = f.state->bootstrap_priority[2].r4_record_generation;
	work.authenticated_ingress_connection_generation = f.state->bootstrap_priority[2].authenticated_ingress_connection_generation;
	work.master_sender_connection_generation = f.state->bootstrap_priority[2].master_sender_connection_generation;
	UT_ASSERT_EQ(work.request.logical_assertion.requester_node, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work, NULL) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&debt, &f.state->grant_intent, sizeof(debt)) == 0);
	master_done_send(&f);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work, NULL), RESOURCE_X_APPLY_APPLIED);
}
UT_TEST(test_consumed_old_callback_cannot_clear_successor_grant)
{
	PublishFixture f; ResourceXDecodedFrame done, next, ack; ResourceXBootstrapWork work;
	ResourceXIntentSlot old; ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	old = master_done_stage(&f);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, ++fake_pcm_clock_us));
	next = acquire_frame(2, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT_EQ(acquire_deliver(&next), RESOURCE_X_APPLY_APPLIED);
	work = bootstrap_next_work();
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_resume_exact(&work, &ack), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_decision_publish_exact(&work, NULL), RESOURCE_X_APPLY_APPLIED);
	before = *f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_consumed_reverse_notice_is_never_requester_done_even_when_colocated)
{
	PublishFixture f; ResourceXDecodedFrame done, ack; ClusterPcmResourceXMasterState before;
	if (!publish_start(0, PCM_STATE_S, &f)) return;
	cluster_pcm_lock_resource_x_bootstrap_forget_exact(&f.work);
	f.request = acquire_frame(0, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	UT_ASSERT(acquire_canonical_start(&f.request, &f.work));
	f.plan = decision_plan_read(&f.work); publish_apply(&f); done = master_done_frame(&f);
	master_done_send(&f); UT_ASSERT_EQ(master_done_receive(&done), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	if (f.state->deferred_settlement.present != 3) return;
	ack = consumed_decode(&f); before = *f.state;
	UT_ASSERT_EQ(master_done_direct(&ack, 0, 61, 61, 77), RESOURCE_X_APPLY_INVALID);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_consumed_codec_has_strict_version_outcome_pair)
{
	PublishFixture f; ResourceXDecodedFrame done, ack, out; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len = 0;
	if (!master_done_start(&f, &done)) return;
	ack = done; ack.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE; ack.common.action_node = 0;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		&ack, bytes, sizeof(bytes), &len, &reject));
	if (len != sizeof(bytes)) return;
	UT_ASSERT_EQ(bytes[0], 6);
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, len, &out, &reject));
	bytes[0] = 5;
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, len, &out, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_VERSION);
}
UT_TEST(test_consumed_codec_refuses_requester_direction_and_truncation)
{
	PublishFixture f; ResourceXDecodedFrame done, ack, out; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES]; uint16 len = 0;
	if (!master_done_start(&f, &done)) return;
	ack = done; ack.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		&ack, bytes, sizeof(bytes), &len, &reject));
	ack.common.action_node = 0;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		&ack, bytes, sizeof(bytes), &len, &reject));
	UT_ASSERT(!cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, sizeof(bytes)-1, &out, &reject));
	UT_ASSERT(!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
		&ack, bytes, sizeof(bytes), &len, &reject));
}
UT_TEST(test_consumed_actual_lms_enqueue_carries_acquired_physical_episode)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot offered, staged;
	if (!consumed_start(&f, &done)) return;
	consumed_ring_reset(); offered = f.state->grant_intent.slot;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &offered, 61, 9999999), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, 1);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].ring[0].msg_type, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE);
	memcpy(&staged, consumed_ring_shared.rings[0].ring[0].payload, sizeof(staged));
	UT_ASSERT_EQ(staged.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(staged.send_episode != 0);
	UT_ASSERT(memcmp(&staged, &f.state->grant_intent.slot, sizeof(staged)) == 0);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 0);
}
UT_TEST(test_consumed_actual_lms_full_does_not_drop_or_stage_original_debt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	consumed_ring_reset(); consumed_ring_shared.rings[0].count = PGRAC_LMS_OUTBOUND_CAPACITY;
	before = *f.state;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &f.state->grant_intent.slot, 61, 9999999), CLUSTER_LMS_ENQUEUE_FULL);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, PGRAC_LMS_OUTBOUND_CAPACITY);
}
UT_TEST(test_consumed_actual_lms_duplicate_offer_has_one_ring_owner)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot offered;
	if (!consumed_start(&f, &done)) return;
	consumed_ring_reset(); offered = f.state->grant_intent.slot;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &offered, 61, 9999999), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &offered, 61, 9999999), CLUSTER_LMS_ENQUEUE_INVALID);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, 1);
	UT_ASSERT_EQ(f.state->deferred_settlement.present, 3);
}
UT_TEST(test_consumed_actual_lms_missing_clock_keeps_owner_and_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	consumed_ring_reset(); before = *f.state; fake_pcm_clock_us = 0;
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &f.state->grant_intent.slot, 61, 9999999), CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, 0);
}
UT_TEST(test_consumed_actual_lms_exhausted_mutation_sequence_retains_debt)
{
	PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	consumed_ring_reset(); before = *f.state;
	pg_atomic_write_u64(&consumed_ring_shared.resource_x_transport_mutation_sequence, UINT64_MAX);
	UT_ASSERT_EQ(consumed_actual_lms_enqueue(0, &f.state->grant_intent.slot, 61, 9999999), CLUSTER_LMS_ENQUEUE_UNAVAILABLE);
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(consumed_ring_shared.rings[0].count, 0);
}
UT_TEST(test_consumed_callback_cannot_consume_changed_independent_receipt)
{
	PublishFixture f; ResourceXDecodedFrame done; ResourceXIntentSlot slot;
	ClusterPcmResourceXMasterState before;
	if (!consumed_start(&f, &done)) return;
	slot = master_done_stage(&f);
	f.state->deferred_settlement.payload[260] ^= 1; before = *f.state;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
}
UT_TEST(test_consumed_wire_rebind_preserves_full_a_i_and_reverse_format)
{
	PublishFixture f; ResourceXDecodedFrame done, ack; ResourceXWireReject reject;
	uint8 bytes[RESOURCE_X_COMPLETE_V5_BYTES];
	if (!consumed_start(&f, &done)) return;
	memcpy(bytes, f.state->grant_intent.payload, sizeof(bytes));
	UT_ASSERT(cluster_resource_x_wire_rebind_sender_generation(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), 123, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		bytes, sizeof(bytes), &ack, &reject));
	UT_ASSERT_EQ(ack.common.sender_connection_generation, 123);
	UT_ASSERT_EQ(ack.common.outcome, RESOURCE_X_OUTCOME_DUPLICATE);
	UT_ASSERT_EQ(bytes[0], 6);
	UT_ASSERT(memcmp(&ack.body.acquire_completion, &done.body.acquire_completion,
		sizeof(done.body.acquire_completion)) == 0);
}

UT_TEST(test_consumed_duplicate_cannot_certify_corrupted_notification_handle)
{
	for (int n = 0; n < 3; n++) {
		PublishFixture f; ResourceXDecodedFrame done; ClusterPcmResourceXMasterState before;
		if (!consumed_start(&f, &done)) continue;
		if (n == 0) f.state->grant_intent.slot.body.owner_generation++;
		if (n == 1) f.state->grant_intent.slot.body.owner_node = 2;
		if (n == 2) f.state->grant_intent.slot.body.owner_index = 1;
		before = *f.state;
		UT_ASSERT(master_done_receive(&done) != RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before, f.state, sizeof(before)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(25);
	UT_RUN(test_consumed_full_e_a_i_in_existing_done_family);
	UT_RUN(test_consumed_early_done_only_arms_after_real_grant_callback);
	UT_RUN(test_consumed_not_admitted_keeps_exact_receipt_and_payload);
	UT_RUN(test_consumed_failure_rearms_same_body_and_rejects_old_callback);
	UT_RUN(test_consumed_generic_semantic_complete_cannot_discharge_transport_debt);
	UT_RUN(test_consumed_original_ready_probe_finds_notification);
	UT_RUN(test_consumed_duplicate_does_not_replace_armed_or_staged_owner);
	UT_RUN(test_consumed_actual_lms_shape_and_payload_direction);
	UT_RUN(test_consumed_valid_crc_payload_change_cannot_replace_consumed_i);
	UT_RUN(test_consumed_unknown_or_mismatched_retained_receipt_blocks_send);
	UT_RUN(test_consumed_master_session_binding_and_admission_are_rechecked);
	UT_RUN(test_consumed_physical_callback_clears_only_matching_receipt);
	UT_RUN(test_consumed_pending_notification_preserves_fifo_and_blocks_publication);
	UT_RUN(test_consumed_old_callback_cannot_clear_successor_grant);
	UT_RUN(test_consumed_reverse_notice_is_never_requester_done_even_when_colocated);
	UT_RUN(test_consumed_codec_has_strict_version_outcome_pair);
	UT_RUN(test_consumed_codec_refuses_requester_direction_and_truncation);
	UT_RUN(test_consumed_actual_lms_enqueue_carries_acquired_physical_episode);
	UT_RUN(test_consumed_actual_lms_full_does_not_drop_or_stage_original_debt);
	UT_RUN(test_consumed_actual_lms_duplicate_offer_has_one_ring_owner);
	UT_RUN(test_consumed_actual_lms_missing_clock_keeps_owner_and_receipt);
	UT_RUN(test_consumed_actual_lms_exhausted_mutation_sequence_retains_debt);
	UT_RUN(test_consumed_callback_cannot_consume_changed_independent_receipt);
	UT_RUN(test_consumed_wire_rebind_preserves_full_a_i_and_reverse_format);
	UT_RUN(test_consumed_duplicate_cannot_certify_corrupted_notification_handle);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
