/*-------------------------------------------------------------------------
 * test_cluster_source_read_receipts.c
 *    Retain complete source receipts in the original requester owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_source_read_receipts.c
 * NOTES
 *    Original requester owners, codec, LMS and authenticated ACK ingress are
 *    real. Native completion/quiet snapshots are explicit adapter inputs;
 *    these cases do not qualify image installation or master/source retirement.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_read_settle_fixture.inc"

static ClusterPcmResourceXMasterState *sr_state;
static ResourceXAcquireReadReceipt sr_receipt;
static ClusterPcmOwnSnapshot sr_native;

static bool
sr_wire(ResourceXDecodedFrame *frame)
{
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		frame, bytes, sizeof(bytes), &len, &reject)
		&& cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			bytes, len, frame, &reject);
}

static bool
sr_start(EvidenceFixture *f, int profile)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	uint16 len = 0;
	ResourceXWireReject reject;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload), matches = 0;
	const ResourceXDecodedImageEnvelope *image;
	if (!disk_start(f, profile)) return false;
	/* The next input comes from M, not the source peer used above. */
	auth_peer_boot = 31;
	BufferBlocks = disk_shared.data;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
		bytes, sizeof(bytes), &len, &reject)) return false;
	for (Size n = offset % sizeof(uint64); n + len <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, bytes, len) == 0) {
			sr_state = (void *)(memory + n - offset); matches++;
		}
	UT_ASSERT_EQ(matches, 1); if (matches != 1) return false;
	image = &input_image.body.acquire_image.original_image;
	memset(&sr_receipt, 0, sizeof(sr_receipt));
	sr_receipt.authority_generation = input_grant.common.authority_generation;
	sr_receipt.decision_crc32c = input_grant.common.semantic_crc32c;
	sr_receipt.page_lsn = PageGetLSN((Page)image->page_bytes);
	sr_receipt.page_scn = ((PageHeader)image->page_bytes)->pd_block_scn;
	sr_receipt.page_crc32c = image->page_checksum;
	sr_native = request_base;
	sr_native.reservation_token = prepared_episode.reservation_token;
	sr_native.flags = PCM_OWN_FLAG_GRANT_PENDING;
	sr_native.semantic_buf_state = BM_TAG_VALID | BM_IO_IN_PROGRESS;
	return true;
}

static bool
sr_arm(void)
{
	ResourceXApplyResult result = cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact(
		&execution_claim, &prepared_episode, &sr_receipt);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	return result == RESOURCE_X_APPLY_APPLIED;
}

static bool
sr_complete(bool handoff)
{
	sr_native.generation++;
	sr_native.flags = 0;
	sr_native.pcm_state = PCM_STATE_S;
	sr_native.buffer_type = BUF_TYPE_SCUR;
	sr_native.semantic_buf_state = BM_TAG_VALID | BM_VALID;
	if (cluster_pcm_lock_resource_x_requester_acquire_read_complete_exact(&execution_claim,
		&prepared_episode, &sr_receipt, &sr_native) != RESOURCE_X_APPLY_APPLIED) return false;
	return (handoff ? cluster_pcm_lock_resource_x_requester_acquire_read_handoff_exact
		: cluster_pcm_lock_resource_x_requester_acquire_read_notice_exact)(
		&execution_claim, &prepared_episode, &sr_receipt, &sr_native) == RESOURCE_X_APPLY_APPLIED;
}

static ResourceXDecodedFrame
sr_frame(void)
{
	ResourceXDecodedFrame frame = {0};
	ResourceXIntentSlot *slot = &sr_state->requester_settlement_intent.slot;
	ResourceXWireReject reject;
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		sr_state->requester_settlement_intent.payload, slot->payload_bytes, &frame, &reject));
	return frame;
}

static ResourceXApplyResult
sr_ack(ResourceXDecodedFrame frame, bool ingress)
{
	ResourceXRequesterJoinSnapshot out;
	ClusterICEnvelope env = {0};
	frame.common.action_node = frame.body.acquire_completion.decision.master_node;
	frame.common.outcome = frame.common.outcome == RESOURCE_X_OUTCOME_QUIET
		? RESOURCE_X_OUTCOME_QUIET_RETIRED : frame.common.outcome == RESOURCE_X_OUTCOME_QUIET_NO_IMAGE
			? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED : RESOURCE_X_OUTCOME_DUPLICATE;
	frame.common.sender_connection_generation = 71;
	if (!sr_wire(&frame)) return RESOURCE_X_APPLY_INVALID;
	if (!ingress) return cluster_pcm_lock_resource_x_requester_acquire_read_consumed_exact(
		&frame, 0, 61, 0, 61, 77);
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
	return gcs_block_resource_x_requester_join_ingress(&env, &frame, 61, &out);
}

static bool
sr_stage(ResourceXIntentSlot *out)
{
	ResourceXIntentResult result = cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&sr_state->requester_settlement_intent.slot, ++fake_pcm_clock_us, out);
	UT_ASSERT_EQ(result, RESOURCE_X_INTENT_STAGED);
	return result == RESOURCE_X_INTENT_STAGED;
}

static bool
sr_quiet(bool unarmed)
{
	ResourceXApplyResult result;
	if (unarmed) {
		sr_receipt.page_lsn = sr_receipt.page_scn = sr_receipt.page_crc32c = 0;
		result = cluster_pcm_lock_resource_x_requester_acquire_unarmed_seal_exact(
			&execution_claim, &prepared_episode, &sr_receipt, &sr_native);
	} else result = cluster_pcm_lock_resource_x_requester_acquire_read_seal_exact(
		&execution_claim, &prepared_episode, &sr_receipt, &sr_native);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result != RESOURCE_X_APPLY_APPLIED) return false;
	sr_native.flags = 0;
	sr_native.semantic_buf_state = BM_TAG_VALID | BM_IO_ERROR;
	result = cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact(
		&execution_claim, &prepared_episode, &sr_receipt, &sr_native);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		ResourceXIntentSlot slot; uint32 cursor = RESOURCE_X_PROTOCOL_NODE_LIMIT + 3;
		result = cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&input_grant.common.logical_assertion.resource, &cursor, &slot)
			== RESOURCE_X_INTENT_PROBE_FOUND ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_BAD_STATE;
		UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	}
	return result == RESOURCE_X_APPLY_APPLIED;
}

UT_TEST(test_source_read_arm_binds_S_and_X_original_image)
{
	for (int p = 2; p <= 3; p++) {
		EvidenceFixture f; ResourceXDeliveryClaim current; ResourceXAcquireReadReceipt receipt;
		UT_ASSERT(sr_start(&f, p)); if (!sr_arm()) continue;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_owner_exact(
			&execution_claim, &prepared_episode, &current, &receipt), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(current.purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED);
		UT_ASSERT(memcmp(&sr_receipt, &receipt, sizeof(receipt)) == 0);
		UT_ASSERT_EQ(disk_reads, 0);
	}
}
UT_TEST(test_source_read_changed_I_is_refused_before_arming)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ClusterPcmResourceXBootstrapRound before;
		UT_ASSERT(sr_start(&f, 2)); before = request_entry()->resource_x_bootstrap_round;
		if (n == 0) sr_receipt.page_lsn++;
		else if (n == 1) sr_receipt.page_scn++;
		else if (n == 2) sr_receipt.page_crc32c++;
		else if (n == 3) sr_receipt.decision_crc32c++;
		else sr_receipt.authority_generation++;
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact(
			&execution_claim, &prepared_episode, &sr_receipt) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	}
}
UT_TEST(test_source_read_completion_and_notice_keep_all_of_A)
{
	for (int p = 2; p <= 3; p++) {
		EvidenceFixture f; ResourceXDecodedFrame notice;
		UT_ASSERT(sr_start(&f, p)); if (!sr_arm()) continue;
		UT_ASSERT(sr_complete(true)); notice = sr_frame();
		UT_ASSERT_EQ(notice.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
		UT_ASSERT_EQ(notice.body.acquire_completion.decision.holders[2].boot_incarnation,
			input_grant.body.acquire_decision.holders[2].boot_incarnation);
		UT_ASSERT_EQ(notice.body.acquire_completion.decision.source_service_generation, 55);
		UT_ASSERT_EQ(notice.body.acquire_completion.page_crc32c, sr_receipt.page_crc32c);
	}
}
UT_TEST(test_source_read_ACK_then_callback_settles_exactly_once)
{
	EvidenceFixture f; ResourceXIntentSlot staged; ResourceXDecodedFrame notice;
	UT_ASSERT(sr_start(&f, 3)); if (!sr_arm()) return;
	UT_ASSERT(sr_complete(true)); notice = sr_frame(); if (!sr_stage(&staged)) return;
	UT_ASSERT_EQ(sr_ack(notice, true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_CONSUMED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(sr_ack(notice, true), RESOURCE_X_APPLY_DUPLICATE);
}
UT_TEST(test_source_read_callback_before_ACK_keeps_logical_debt)
{
	EvidenceFixture f; ResourceXIntentSlot staged; ResourceXDecodedFrame notice;
	UT_ASSERT(sr_start(&f, 2)); if (!sr_arm()) return;
	UT_ASSERT(sr_complete(true)); notice = sr_frame(); if (!sr_stage(&staged)) return;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT_EQ(sr_ack(notice, false), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
}
UT_TEST(test_source_read_failed_callback_after_ACK_does_not_reoffer)
{
	EvidenceFixture f; ResourceXIntentSlot staged; ResourceXDecodedFrame notice;
	UT_ASSERT(sr_start(&f, 2)); if (!sr_arm()) return;
	UT_ASSERT(sr_complete(true)); notice = sr_frame(); if (!sr_stage(&staged)) return;
	UT_ASSERT_EQ(sr_ack(notice, false), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
}
UT_TEST(test_source_read_NOT_ADMITTED_keeps_exact_original_offer)
{
	EvidenceFixture f; ResourceXIntentSlot before;
	UT_ASSERT(sr_start(&f, 2)); if (!sr_arm()) return;
	UT_ASSERT(sr_complete(true)); before = sr_state->requester_settlement_intent.slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&before, ++fake_pcm_clock_us),
		RESOURCE_X_INTENT_NOT_ADMITTED);
	UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_ARMED);
	UT_ASSERT_EQ(sr_state->requester_settlement_intent.slot.logical_generation, before.logical_generation);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.episode, 1);
}
UT_TEST(test_source_read_ACK_cannot_change_holder_or_I)
{
	for (int n = 0; n < 5; n++) {
		EvidenceFixture f; ResourceXDecodedFrame notice; ClusterPcmResourceXBootstrapRound before;
		UT_ASSERT(sr_start(&f, 2)); if (!sr_arm()) continue;
		UT_ASSERT(sr_complete(true)); notice = sr_frame(); before = request_entry()->resource_x_bootstrap_round;
		if (n == 0) notice.body.acquire_completion.decision.holders[2].boot_incarnation++;
		else if (n == 1) notice.body.acquire_completion.decision.holders[2].grant_generation++;
		else if (n == 2) notice.body.acquire_completion.page_lsn++;
		else if (n == 3) notice.body.acquire_completion.decision.episode.reservation_token++;
		else notice.body.acquire_completion.decision.source_service_generation++;
		UT_ASSERT(sr_ack(notice, false) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &request_entry()->resource_x_bootstrap_round, sizeof(before)) == 0);
	}
}
UT_TEST(test_source_read_verified_quiet_waits_for_exact_ACK_and_callback)
{
	EvidenceFixture f; ResourceXIntentSlot staged; ResourceXDecodedFrame notice;
	UT_ASSERT(sr_start(&f, 3)); if (!sr_arm() || !sr_quiet(false)) return;
	notice = sr_frame(); UT_ASSERT_EQ(notice.common.outcome, RESOURCE_X_OUTCOME_QUIET);
	if (!sr_stage(&staged)) return;
	UT_ASSERT_EQ(sr_ack(notice, true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 1);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT(request_entry()->resource_x_bootstrap_round.highest_attempt_floor > 0);
}
UT_TEST(test_source_read_unarmed_quiet_never_claims_image_or_native_C)
{
	for (int p = 2; p <= 3; p++) {
		EvidenceFixture f; ResourceXDecodedFrame notice;
		UT_ASSERT(sr_start(&f, p)); if (!sr_quiet(true)) continue;
		notice = sr_frame(); UT_ASSERT_EQ(notice.common.outcome, RESOURCE_X_OUTCOME_QUIET_NO_IMAGE);
		UT_ASSERT_EQ(notice.body.acquire_completion.native_generation, prepared_episode.base_generation);
		UT_ASSERT_EQ(notice.body.acquire_completion.page_lsn, 0);
		UT_ASSERT_EQ(sr_ack(notice, false), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
		UT_ASSERT_EQ(disk_reads, 0);
	}
}

/* These are the actual LMS payload and container validators, not a leaf
 * substituted for transport admission. The disk branch protects the existing
 * quiet ABI while the source branch uses the original full receipt owner. */
UT_TEST(test_source_read_actual_LMS_accepts_all_requester_receipt_shapes)
{
	for (int source = 0; source < 2; source++) for (int kind = 0; kind < 3; kind++) {
		EvidenceFixture f; ResourceXDecodedFrame frame; ResourceXIntentSlot slot;
		uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 len; ResourceXWireReject reject;
		if (source) {
			UT_ASSERT(sr_start(&f, 2)); if (!sr_arm() || !sr_complete(true)) { UT_ASSERT(false); continue; }
			frame = sr_frame(); slot = sr_state->requester_settlement_intent.slot;
		} else {
			UT_ASSERT(done_start(&f)); UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
			frame = done_frame(); slot = done_intent->slot;
		}
		if (kind) {
			frame.common.outcome = kind == 1 ? RESOURCE_X_OUTCOME_QUIET : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE;
			frame.body.acquire_completion.native_generation = prepared_episode.base_generation;
			if (kind == 2) frame.body.acquire_completion.page_lsn = frame.body.acquire_completion.page_scn
				= frame.body.acquire_completion.page_crc32c = 0;
		}
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&frame, bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT(lms_outbound_resource_x_intent_valid(&slot));
		UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&slot, bytes));
		frame.common.outcome = kind == 0 ? RESOURCE_X_OUTCOME_DUPLICATE
			: kind == 1 ? RESOURCE_X_OUTCOME_QUIET_RETIRED : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED;
		frame.common.action_node = frame.body.acquire_completion.decision.master_node;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&frame, bytes, sizeof(bytes), &len, &reject));
		UT_ASSERT(!lms_outbound_resource_x_intent_payload_valid(&slot, bytes));
	}
}
UT_TEST(test_source_read_LMS_preserves_master_disk_only_and_reverse_direction)
{
	for (int kind = 0; kind < 3; kind++) {
		EvidenceFixture f; ResourceXDecodedFrame frame; ResourceXIntentSlot slot;
		uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 len; ResourceXWireReject reject;
		UT_ASSERT(done_start(&f)); UT_ASSERT_EQ(done_run(), RESOURCE_X_APPLY_APPLIED);
		frame = done_frame(); slot = done_intent->slot;
		frame.common.action_node = 0;
		frame.common.outcome = kind == 0 ? RESOURCE_X_OUTCOME_DUPLICATE
			: kind == 1 ? RESOURCE_X_OUTCOME_QUIET_RETIRED : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED;
		if (kind) frame.body.acquire_completion.native_generation = prepared_episode.base_generation;
		if (kind == 2) frame.body.acquire_completion.page_lsn = frame.body.acquire_completion.page_scn
			= frame.body.acquire_completion.page_crc32c = 0;
		UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
			&frame, bytes, sizeof(bytes), &len, &reject));
		slot.destination_node = 1; slot.body.owner_node = 0;
		slot.body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;
		slot.body.owner_generation = slot.authority_generation; cluster_node_id = 0;
		UT_ASSERT(lms_outbound_resource_x_intent_valid(&slot));
		UT_ASSERT(lms_outbound_resource_x_intent_payload_valid(&slot, bytes));
		slot.payload_bytes = RESOURCE_X_SOURCE_COMPLETE_V11_BYTES;
		UT_ASSERT(!lms_outbound_resource_x_intent_valid(&slot)); cluster_node_id = 1;
	}
}
UT_TEST(test_source_read_ACK_domain_changes_leave_exact_debt)
{
	for (int n = 0; n < 8; n++) {
		EvidenceFixture f; ResourceXDecodedFrame ack; ClusterPcmResourceXReadIntent before;
		int source = 0, master = 0; uint32 ingress = 61, master_ingress = 61; uint64 r4 = 77;
		UT_ASSERT(sr_start(&f, 3)); if (!sr_arm() || !sr_complete(true)) { UT_ASSERT(false); continue; }
		ack = sr_frame(); before = sr_state->requester_settlement_intent;
		ack.common.action_node = 0; ack.common.outcome = RESOURCE_X_OUTCOME_DUPLICATE;
		if (n == 0) source = 2; else if (n == 1) master = 2;
		else if (n == 2) ingress++; else if (n == 3) master_ingress++; else if (n == 4) r4++;
		else if (n == 5) ack.common.resource_formation++;
		else if (n == 6) ack.common.master_session_incarnation++;
		else ack.body.acquire_completion.decision.episode.reservation_token++;
		UT_ASSERT(sr_wire(&ack));
		UT_ASSERT(cluster_pcm_lock_resource_x_requester_acquire_read_consumed_exact(
			&ack, source, ingress, master, master_ingress, r4) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &sr_state->requester_settlement_intent, sizeof(before)) == 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
			RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	}
}
UT_TEST(test_source_read_snapshot_capacity_is_exact_and_no_other_owner_grows)
{
	EvidenceFixture f; ResourceXIntentSlot slot, before; uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	UT_ASSERT(sr_start(&f, 2)); if (!sr_arm() || !sr_complete(true)) { UT_ASSERT(false); return; }
	before = sr_state->requester_settlement_intent.slot; memset(&slot, 0x3B, sizeof(slot));
	memset(bytes, 0x3B, sizeof(bytes));
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&before, &slot, bytes,
		sizeof(bytes) - 1) != RESOURCE_X_APPLY_APPLIED);
	for (unsigned n = 0; n < sizeof(bytes); n++) UT_ASSERT_EQ(bytes[n], 0x3B);
	UT_ASSERT(memcmp(&before, &sr_state->requester_settlement_intent.slot, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&before, &slot,
		bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(bytes, sr_state->requester_settlement_intent.payload, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(sizeof(ClusterPcmResourceXControlIntent), 408);
	UT_ASSERT_EQ(sizeof(sr_state->deferred_settlement.payload), RESOURCE_X_COMPLETE_V5_BYTES);
}

UT_TEST(test_source_read_global_probe_and_real_LMS_handoff_keep_full_receipt)
{
	EvidenceFixture f; ResourceXIntentSlot offered = {0}, staged;
	ResourceXAcquisitionRef delivery; ResourceXIntentProbeResult found = RESOURCE_X_INTENT_PROBE_IDLE;
	uint32 examined; uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES];
	UT_ASSERT(sr_start(&f, 3)); if (!sr_arm() || !sr_complete(true)) { UT_ASSERT(false); return; }
	UT_ASSERT(gcs_block_resource_x_payload_candidate(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, sizeof(bytes)));
	for (int n = 0; n < 8 && found != RESOURCE_X_INTENT_PROBE_FOUND; n++) {
		found = cluster_pcm_lock_resource_x_outbound_work_probe_exact(4, &offered,
			bytes, sizeof(bytes), &examined, &delivery);
		if (found == RESOURCE_X_INTENT_PROBE_CORRUPT) break;
	}
	UT_ASSERT_EQ(found, RESOURCE_X_INTENT_PROBE_FOUND);
	if (found != RESOURCE_X_INTENT_PROBE_FOUND) return;
	UT_ASSERT_EQ(offered.payload_bytes, sizeof(bytes));
	UT_ASSERT(memcmp(bytes, sr_state->requester_settlement_intent.payload, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(done_actual_lms_enqueue(0, &offered, 51, 9999), CLUSTER_LMS_ENQUEUE_ADMITTED);
	UT_ASSERT_EQ(request_shared.rings[0].count, 1);
	memcpy(&staged, request_shared.rings[0].ring[0].payload, sizeof(staged));
	UT_ASSERT_EQ(staged.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT_EQ(staged.payload_bytes, sizeof(bytes));
	UT_ASSERT_EQ(sr_ack(sr_frame(), true), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_source_read_arm_binds_S_and_X_original_image);
	UT_RUN(test_source_read_changed_I_is_refused_before_arming);
	UT_RUN(test_source_read_completion_and_notice_keep_all_of_A);
	UT_RUN(test_source_read_ACK_then_callback_settles_exactly_once);
	UT_RUN(test_source_read_callback_before_ACK_keeps_logical_debt);
	UT_RUN(test_source_read_failed_callback_after_ACK_does_not_reoffer);
	UT_RUN(test_source_read_NOT_ADMITTED_keeps_exact_original_offer);
	UT_RUN(test_source_read_ACK_cannot_change_holder_or_I);
	UT_RUN(test_source_read_verified_quiet_waits_for_exact_ACK_and_callback);
	UT_RUN(test_source_read_unarmed_quiet_never_claims_image_or_native_C);
	UT_RUN(test_source_read_actual_LMS_accepts_all_requester_receipt_shapes);
	UT_RUN(test_source_read_LMS_preserves_master_disk_only_and_reverse_direction);
	UT_RUN(test_source_read_ACK_domain_changes_leave_exact_debt);
	UT_RUN(test_source_read_snapshot_capacity_is_exact_and_no_other_owner_grows);
	UT_RUN(test_source_read_global_probe_and_real_LMS_handoff_keep_full_receipt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
