/*-------------------------------------------------------------------------
 * test_cluster_resource_x_decision_send.c
 *    Complete conditional frames in the original master transport owners.
 *
 * The real PCM object, codec, scheduler and callback implementations run.
 * Only the future conditional publisher is replaced by explicit typed frame
 * injection into the original containers, located by an actual old intent.
 * This does not qualify conditional authority, holder witnesses or native C.
 *-------------------------------------------------------------------------
 */
int pcm_decision_fixture_main(void);
#define main pcm_decision_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main
#include "test_cluster_resource_x_decision_owner_layout.inc"

typedef struct DecisionSendFixture {
	ClusterPcmResourceXMasterState *state;
	ResourceXIntentSlot *slot;
	uint8 *payload;
	ResourceXDecodedFrame frame;
	ResourceXIntentSlot expected;
	uint8 bytes[RESOURCE_X_DECISION_V3_BYTES];
	uint32 owner_index;
} DecisionSendFixture;

static bool
decision_send_encode(DecisionSendFixture *f)
{
	ResourceXWireReject reject;
	uint16 len = 0;
	bool ok = cluster_resource_x_wire_encode(f->frame.kind == RESOURCE_X_WIRE_BLOCK_TO_N
		? RESOURCE_X_MSG_BLOCK_TO_N : RESOURCE_X_MSG_IMAGE_OR_GRANT, &f->frame,
		f->bytes, sizeof(f->bytes), &len, &reject);
	UT_ASSERT(ok);
	UT_ASSERT_EQ(len, RESOURCE_X_DECISION_V3_BYTES);
	return ok && len == RESOURCE_X_DECISION_V3_BYTES;
}

static bool
decision_send_start(bool block, DecisionSendFixture *f)
{
	ResourceXDecodedFrame assertion, request;
	ResourceXMasterSnapshot snapshot;
	ResourceXDurableProof durable = {0};
	ResourceXIntentSlot original;
	ResourceXIntentBodyHandle body;
	ResourceXDecodedAcquireDecision *a;
	uint8 old_payload[RESOURCE_X_PROOF_V1_BYTES];
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset, matches = 0, location = 0;
	BufferTag tag = make_tag(850);

	memset(f, 0, sizeof(*f));
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	MyBackendType = B_LMS;
	ut_master_session = 31;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17), RESOURCE_X_APPLY_APPLIED);
	if (block)
		UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 3));
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2, &snapshot),
		RESOURCE_X_APPLY_APPLIED);
	if (block) {
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
			&assertion.common.logical_assertion, 3, &original, old_payload, sizeof(old_payload)),
			RESOURCE_X_APPLY_APPLIED);
		f->owner_index = 3;
		offset = offsetof(ClusterPcmResourceXMasterState, block_intents[3].slot);
	} else {
		durable.assertion = assertion.common.logical_assertion;
		durable.base_authority_generation = 1;
		durable.resource_formation = 17;
		durable.master_session_incarnation = 31;
		durable.assertion_sequence = 41;
		durable.requester_target_generation = 41;
		durable.page_scn_lsn = 82;
		durable.page_checksum = UINT32_C(0x12345678);
		durable.source_proof_crc32c = UINT32_C(0x87654321);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable, &snapshot),
			RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
			&durable.assertion, &original, old_payload, sizeof(old_payload)), RESOURCE_X_APPLY_APPLIED);
		f->owner_index = RESOURCE_X_PROTOCOL_NODE_LIMIT + 1;
		offset = offsetof(ClusterPcmResourceXMasterState, grant_intent.slot);
	}
	/* The real descriptor identifies the container. The generated original
	 * layout is used on both parent and candidate; no fabricated offsets. */
	for (Size n = 0; n + sizeof(original) <= fake_pcm_header_requested_size; n += 8)
		if (memcmp(memory + n, &original, sizeof(original)) == 0) {
			matches++;
			location = n;
		}
	UT_ASSERT_EQ(matches, 1);
	if (matches != 1 || location < offset) return false;
	f->state = (ClusterPcmResourceXMasterState *)(memory + location - offset);
	UT_ASSERT((unsigned char *)(f->state + 1) <= memory + fake_pcm_header_requested_size);
	f->slot = block ? &f->state->block_intents[3].slot : &f->state->grant_intent.slot;
	f->payload = block ? f->state->block_intents[3].payload : f->state->grant_intent.payload;
	UT_ASSERT((block ? sizeof(f->state->block_intents[3].payload)
		: sizeof(f->state->grant_intent.payload)) >= RESOURCE_X_DECISION_V3_BYTES);
	if ((block ? sizeof(f->state->block_intents[3].payload)
		: sizeof(f->state->grant_intent.payload)) < RESOURCE_X_DECISION_V3_BYTES) return false;

	request = make_resource_x_acquire_request(tag, 2, RESOURCE_X_ACQUIRE_FIRST_FILL,
		PCM_STATE_N, PCM_STATE_X);
	f->frame.kind = block ? RESOURCE_X_WIRE_BLOCK_TO_N : RESOURCE_X_WIRE_AUTHORITY_GRANT;
	f->frame.payload_bytes = RESOURCE_X_DECISION_V3_BYTES;
	f->frame.has_acquire_decision = true;
	f->frame.common = assertion.common;
	f->frame.common.authority_generation = 2;
	f->frame.common.ordered_lane = 0;
	f->frame.common.action_node = block ? 3 : 2;
	f->frame.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	a = &f->frame.body.acquire_decision;
	a->episode = request.body.acquire_intent;
	a->episode.cluster_epoch = 17;
	a->episode.r4_generation = 17;
	a->resource_binding = f->state->binding_generation;
	a->enqueue_order = f->state->requests[2].enqueue_order;
	a->master_node = 0;
	a->source_node = block ? 3 : -1;
	a->source_kind = block ? RESOURCE_X_ACQUIRE_SOURCE_REMOTE_CURRENT : RESOURCE_X_ACQUIRE_SOURCE_DISK;
	a->source_service_generation = block ? 2 : 0;
	a->holders_bitmap = a->required_holders_bitmap = block ? 8 : 0;
	a->directory_mode = block ? PCM_STATE_X : PCM_STATE_N;
	a->master_ingress_generation = 61;
	a->requester_ingress_generation = 71;
	if (block) a->holders[3] = (ResourceXDecodedAcquireHolder){103, 1};
	if (!decision_send_encode(f)) return false;
	body = original.body;
	memset(f->slot, 0, sizeof(*f->slot));
	memcpy(f->payload, f->bytes, sizeof(f->bytes));
	UT_ASSERT(cluster_pcm_lock_resource_x_intent_arm_exact(f->slot, &body, 41, 2,
		100000, block ? 3 : 2, RESOURCE_X_DECISION_V3_BYTES, f->frame.kind));
	f->expected = *f->slot;
	fake_pcm_clock_us = 100001;
	return f->slot->state == RESOURCE_X_INTENT_SLOT_ARMED;
}

static void
decision_send_snapshot(DecisionSendFixture *f)
{
	ResourceXIntentSlot slot;
	uint8 bytes[RESOURCE_X_DECISION_V3_BYTES];
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f->expected,
		&slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&slot, f->slot, sizeof(slot)) == 0);
	UT_ASSERT(memcmp(bytes, f->bytes, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_decision_send_exact_container_capacity)
{
	UT_ASSERT_EQ(sizeof(((ClusterPcmResourceXMasterState *)0)->block_intents[0].payload), 736);
	UT_ASSERT_EQ(sizeof(((ClusterPcmResourceXMasterState *)0)->grant_intent.payload), 736);
	/* Unrelated completion/control owners retain their original capacity. */
	UT_ASSERT_EQ(sizeof(ClusterPcmResourceXControlIntent), 408);
	UT_ASSERT_EQ(sizeof(ClusterPcmResourceXSourceSettlement), 432);
	UT_ASSERT_EQ(sizeof(ClusterPcmResourceXRequesterJoin), 13680);
	UT_ASSERT_EQ(sizeof(ClusterPcmResourceXMasterState), 83016);
	reset_fake_pcm_runtime(24);
	printf("# master_state_bytes=%zu header24_requested=%zu fixture_capacity=%zu\n",
		sizeof(ClusterPcmResourceXMasterState), fake_pcm_header_requested_size, sizeof(fake_pcm_header.data));
}

UT_TEST(test_decision_send_arm_accepts_only_the_two_existing_families)
{
	ResourceXIntentBodyHandle body = {0};
	ResourceXIntentSlot slot;
	body.assertion.resource = make_tag(850);
	body.assertion.requester_node = 2;
	body.owner_generation = 41;
	body.owner_node = 0;
	body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_BLOCK;
	body.owner_index = 3;
	for (int kind = RESOURCE_X_WIRE_KIND_MIN; kind <= RESOURCE_X_WIRE_KIND_MAX; kind++) {
		bool allowed = kind == RESOURCE_X_WIRE_AUTHORITY_GRANT || kind == RESOURCE_X_WIRE_BLOCK_TO_N;
		bool grant = kind == RESOURCE_X_WIRE_AUTHORITY_GRANT;
		body.owner_kind = grant ? RESOURCE_X_INTENT_OWNER_MASTER_GRANT : RESOURCE_X_INTENT_OWNER_MASTER_BLOCK;
		body.owner_index = grant ? 0 : 3;
		body.owner_generation = grant ? 2 : 41;
		memset(&slot, 0, sizeof(slot));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_intent_arm_exact(&slot, &body, 41, 2, 100000,
			grant ? 2 : 3, RESOURCE_X_DECISION_V3_BYTES, kind), allowed);
	}
	for (int bad = 0; bad < 4; bad++) {
		body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;
		body.owner_index = 0;
		body.owner_generation = 2;
		memset(&slot, 0, sizeof(slot));
		if (bad == 0) body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_STATUS;
		if (bad == 1) body.owner_generation = 41;
		if (bad == 2) body.owner_index = 3;
		UT_ASSERT(!cluster_pcm_lock_resource_x_intent_arm_exact(&slot, &body, 41, 2, 100000,
			bad == 3 ? 3 : 2, 736, RESOURCE_X_WIRE_AUTHORITY_GRANT));
	}
	body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_BLOCK;
	body.owner_index = 3;
	body.owner_generation = 41;
	for (int len = 313; len <= 737; len++) {
		if (len == 736) continue;
		memset(&slot, 0, sizeof(slot));
		UT_ASSERT(!cluster_pcm_lock_resource_x_intent_arm_exact(&slot, &body, 41, 2, 100000,
			3, len, RESOURCE_X_WIRE_BLOCK_TO_N));
	}
}

UT_TEST(test_decision_send_grant_retains_all736_bytes)
{
	DecisionSendFixture f;
	if (decision_send_start(false, &f)) decision_send_snapshot(&f);
}

UT_TEST(test_decision_send_block_retains_all736_bytes)
{
	DecisionSendFixture f;
	if (decision_send_start(true, &f)) decision_send_snapshot(&f);
}

UT_TEST(test_decision_send_short_output_preserves_bytes_and_owner)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot slot;
		uint8 bytes[736], before[736];
		if (!decision_send_start(block, &f)) continue;
		memset(bytes, 0xa5, sizeof(bytes)); memcpy(before, bytes, sizeof(bytes));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected,
			&slot, bytes, 735), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
		UT_ASSERT(memcmp(f.slot, &f.expected, sizeof(f.expected)) == 0);
	}
}

UT_TEST(test_decision_send_ready_probe_uses_original_owner_indices)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot slot;
		uint32 cursor;
		if (!decision_send_start(block, &f)) continue;
		cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&f.expected.body.assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_FOUND);
		UT_ASSERT_EQ(cursor, f.owner_index + 1);
		UT_ASSERT(memcmp(&slot, &f.expected, sizeof(slot)) == 0);
	}
}

UT_TEST(test_decision_send_global_probe_copies_full_body_with_bounded_visits)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot slot;
		uint8 bytes[736];
		uint32 examined = 0;
		ResourceXIntentProbeResult r = RESOURCE_X_INTENT_PROBE_IDLE;
		if (!decision_send_start(block, &f)) continue;
		for (int n = 0; n < 30 && r != RESOURCE_X_INTENT_PROBE_FOUND; n++) {
			r = cluster_pcm_lock_resource_x_outbound_intent_probe_exact(1, &slot, bytes,
				sizeof(bytes), &examined);
			UT_ASSERT(examined <= 1);
		}
		UT_ASSERT_EQ(r, RESOURCE_X_INTENT_PROBE_FOUND);
		UT_ASSERT(memcmp(bytes, f.bytes, sizeof(bytes)) == 0);
		UT_ASSERT(memcmp(&slot, &f.expected, sizeof(slot)) == 0);
	}
}

UT_TEST(test_decision_send_not_admitted_keeps_exact_continuation)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		if (!decision_send_start(block, &f)) continue;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
			&f.expected, 100002), RESOURCE_X_INTENT_NOT_ADMITTED);
		UT_ASSERT_EQ(f.slot->state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(f.slot->last_attempt_us, 100002);
		decision_send_snapshot(&f);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected,
			100003), RESOURCE_X_INTENT_NOT_DUE);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected,
			600002), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(memcmp(f.payload, f.bytes, sizeof(f.bytes)) == 0);
	}
}

UT_TEST(test_decision_send_one_transport_stage_not_two_offers)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot staged;
		if (!decision_send_start(block, &f)) continue;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&f.expected, 100002, &staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT_EQ(staged.send_episode, 1);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected,
			100003), RESOURCE_X_INTENT_STALE);
		UT_ASSERT_EQ(f.slot->send_episode, 1);
		decision_send_snapshot(&f);
	}
}

UT_TEST(test_decision_send_transport_success_retires_slot_not_semantic_request)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot staged, empty = {0};
		ClusterPcmResourceXMasterRequest canonical;
		if (!decision_send_start(block, &f)) continue;
		canonical = f.state->requests[2];
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&f.expected, 100002, &staged), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100003));
		UT_ASSERT(memcmp(f.slot, &empty, sizeof(empty)) == 0);
		UT_ASSERT(memcmp(f.payload, f.bytes, sizeof(f.bytes)) == 0);
		UT_ASSERT(memcmp(&canonical, &f.state->requests[2], sizeof(canonical)) == 0);
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100004));
	}
}

UT_TEST(test_decision_send_failed_transport_rearms_same_episode)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot first, second, before;
		if (!decision_send_start(block, &f)) continue;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&f.expected, 100002, &first), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&first, false, 100003));
		UT_ASSERT_EQ(f.slot->state, RESOURCE_X_INTENT_SLOT_ARMED);
		UT_ASSERT_EQ(f.slot->first_armed_us, 100000);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&f.expected, 600003, &second), RESOURCE_X_INTENT_STAGED);
		UT_ASSERT_EQ(second.send_episode, 2);
		before = *f.slot;
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&first, true, 600004));
		UT_ASSERT(memcmp(f.slot, &before, sizeof(before)) == 0);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&second, true, 600005));
	}
}

UT_TEST(test_decision_send_old_callback_cannot_consume_new_logical_owner)
{
	DecisionSendFixture f;
	ResourceXIntentSlot old, current;
	if (!decision_send_start(false, &f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f.expected, 100002, &old), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, 100003));
	f.frame.common.assertion_sequence++;
	f.frame.common.authority_generation++;
	if (!decision_send_encode(&f)) return;
	memcpy(f.payload, f.bytes, sizeof(f.bytes));
	old.body.owner_generation++;
	UT_ASSERT(cluster_pcm_lock_resource_x_intent_arm_exact(f.slot, &old.body, 42, 3, 100000,
		2, 736, RESOURCE_X_WIRE_AUTHORITY_GRANT));
	f.expected = *f.slot;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&f.expected, 100004, &current), RESOURCE_X_INTENT_STAGED);
	old.body.owner_generation--;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&old, true, 100005));
	UT_ASSERT(memcmp(f.slot, &current, sizeof(current)) == 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&current, true, 100006));
}

UT_TEST(test_decision_send_corrupt_frame_is_not_offered_or_discarded)
{
	for (int block = 0; block <= 1; block++) {
		DecisionSendFixture f;
		ResourceXIntentSlot slot;
		uint8 out[736], before[736];
		uint32 cursor = 0;
		if (!decision_send_start(block, &f)) continue;
		f.payload[735] ^= 1;
		memset(out, 0xa5, sizeof(out)); memcpy(before, out, sizeof(out));
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected,
			&slot, out, sizeof(out)), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(out, before, sizeof(out)) == 0);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&f.expected.body.assertion.resource, &cursor, &slot), RESOURCE_X_INTENT_PROBE_CORRUPT);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected, 100002),
			RESOURCE_X_INTENT_STALE);
		UT_ASSERT(memcmp(f.slot, &f.expected, sizeof(f.expected)) == 0);
	}
}

UT_TEST(test_decision_send_valid_crc_cannot_rebind_original_owner)
{
	for (int block = 0; block <= 1; block++)
		for (int field = 0; field < 8; field++) {
			DecisionSendFixture f;
			ResourceXIntentSlot slot;
			uint8 bytes[736];
			if (!decision_send_start(block, &f)) continue;
			switch (field) {
			case 0: f.frame.common.logical_assertion.resource.blockNum++; break;
			case 1: f.frame.common.logical_assertion.requester_node = 1;
				if (!block) f.frame.common.action_node = 1; break;
			case 2: f.frame.common.assertion_sequence++; break;
			case 3: f.frame.common.authority_generation++;
				if (block) f.frame.body.acquire_decision.source_service_generation++; break;
			case 4: if (block) { f.frame.common.action_node = 1;
				f.frame.body.acquire_decision.holders_bitmap |= 2;
				f.frame.body.acquire_decision.required_holders_bitmap |= 2;
				f.frame.body.acquire_decision.holders[1] = (ResourceXDecodedAcquireHolder){101, 1};
				f.frame.body.acquire_decision.directory_mode = PCM_STATE_S;
			} else f.frame.body.acquire_decision.master_node = 1; break;
			case 5: f.frame.body.acquire_decision.resource_binding++; break;
			case 6: f.frame.body.acquire_decision.master_node = 1; break;
			case 7: f.frame.common.sender_connection_generation = 81; break;
			}
			if (!decision_send_encode(&f)) continue;
			memcpy(f.payload, f.bytes, sizeof(f.bytes));
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected,
				&slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_STALE);
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected, 100002),
				RESOURCE_X_INTENT_STALE);
			UT_ASSERT(memcmp(f.slot, &f.expected, sizeof(f.expected)) == 0);
		}
}

UT_TEST(test_decision_send_slot_owner_shape_cannot_alias_other_container)
{
	for (int block = 0; block <= 1; block++)
		for (int field = 0; field < 4; field++) {
			DecisionSendFixture f;
			ResourceXIntentSlot slot;
			uint8 bytes[736];
			if (!decision_send_start(block, &f)) continue;
			if (field == 0) f.slot->body.owner_node++;
			if (field == 1) f.slot->body.owner_generation++;
			if (field == 2) f.slot->body.owner_index = 31;
			if (field == 3) f.slot->kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
			f.expected = *f.slot;
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected,
				&slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_STALE);
		}
}

UT_TEST(test_decision_send_staged_corruption_keeps_transport_responsibility)
{
	DecisionSendFixture f;
	ResourceXIntentSlot staged;
	if (!decision_send_start(false, &f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(&f.expected,
		100002, &staged), RESOURCE_X_INTENT_STAGED);
	f.payload[735] ^= 1;
	UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100003));
	UT_ASSERT(memcmp(f.slot, &staged, sizeof(staged)) == 0);
	f.payload[735] ^= 1;
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&staged, true, 100004));
}

UT_TEST(test_decision_send_pi_rebuild_fence_preserves_new_intent)
{
	DecisionSendFixture f;
	ResourceXIntentSlot slot;
	uint8 bytes[736];
	if (!decision_send_start(false, &f)) return;
	pi_service_blocked = true;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(&f.expected,
		&slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(&f.expected, 100002),
		RESOURCE_X_INTENT_STALE);
	UT_ASSERT(memcmp(f.slot, &f.expected, sizeof(f.expected)) == 0);
	pi_service_blocked = false;
	decision_send_snapshot(&f);
}

UT_TEST(test_decision_send_legacy_native_consumers_still_reject_conditional_frame)
{
	DecisionSendFixture f;
	ResourceXIntentSlot slot;
	ResourceXRequesterJoinSnapshot snapshot;
	uint8 bytes[736];
	if (!decision_send_start(false, &f)) return;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
		&f.expected.body.assertion, &slot, bytes, sizeof(bytes)), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(&f.frame, 0, &snapshot),
		RESOURCE_X_APPLY_INVALID);
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_decision_send_exact_container_capacity);
	UT_RUN(test_decision_send_arm_accepts_only_the_two_existing_families);
	UT_RUN(test_decision_send_grant_retains_all736_bytes);
	UT_RUN(test_decision_send_block_retains_all736_bytes);
	UT_RUN(test_decision_send_short_output_preserves_bytes_and_owner);
	UT_RUN(test_decision_send_ready_probe_uses_original_owner_indices);
	UT_RUN(test_decision_send_global_probe_copies_full_body_with_bounded_visits);
	UT_RUN(test_decision_send_not_admitted_keeps_exact_continuation);
	UT_RUN(test_decision_send_one_transport_stage_not_two_offers);
	UT_RUN(test_decision_send_transport_success_retires_slot_not_semantic_request);
	UT_RUN(test_decision_send_failed_transport_rearms_same_episode);
	UT_RUN(test_decision_send_old_callback_cannot_consume_new_logical_owner);
	UT_RUN(test_decision_send_corrupt_frame_is_not_offered_or_discarded);
	UT_RUN(test_decision_send_valid_crc_cannot_rebind_original_owner);
	UT_RUN(test_decision_send_slot_owner_shape_cannot_alias_other_container);
	UT_RUN(test_decision_send_staged_corruption_keeps_transport_responsibility);
	UT_RUN(test_decision_send_pi_rebuild_fence_preserves_new_intent);
	UT_RUN(test_decision_send_legacy_native_consumers_still_reject_conditional_frame);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
